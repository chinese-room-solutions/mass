package modules

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/KernelPryanic/ctxerr"
	massmodule "github.com/chinese-room-solutions/mass-module"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/hashicorp/go-hclog"
	gomodule "github.com/hashicorp/go-plugin"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

// ModuleResolverInterface resolves a module name+version to a command string,
// installing it from a registry if needed.
type ModuleResolverInterface interface {
	Resolve(ctx context.Context, name, version string) (string, error)
}

// LoadedModule holds a running module's state.
type LoadedModule struct {
	Name   string
	Info   *massmodule.ModuleInfo
	client *gomodule.Client
	module massmodule.ModuleInterface
}

// Module returns the underlying gRPC module client.
func (lm *LoadedModule) Module() massmodule.ModuleInterface {
	return lm.module
}

// Compile-time check: Manager implements ModuleRuntimeInterface.
var _ ModuleRuntimeInterface = (*Manager)(nil)

// Manager discovers, launches, and manages modules via bare processes (go-plugin).
type Manager struct {
	logger      zerolog.Logger
	modules     []*LoadedModule
	installer   ModuleResolverInterface // nil when no registry is configured
	extraEnv    []string                // additional env vars for module processes
	logCallback func(name, line string)
}

// NewManager creates a new module Manager.
func NewManager(logger zerolog.Logger) *Manager {
	return &Manager{logger: logger}
}

// SetExtraEnv sets additional environment variables for module subprocesses.
func (m *Manager) SetExtraEnv(env []string) {
	m.extraEnv = env
}

// SetLogCallback sets a callback that is invoked for each log line written
// to a module's stderr. The callback receives the module name and the line.
func (m *Manager) SetLogCallback(fn func(name, line string)) {
	m.logCallback = fn
}

// SetInstaller configures a module installer for registry-based module resolution.
func (m *Manager) SetInstaller(inst ModuleResolverInterface) {
	m.installer = inst
}

// LoadModule launches a module process and queries its metadata.
// The module is identified by its Command field (e.g. "./binary" or
// "python main.py"). If Command is empty but Source is set, the
// command is resolved from the registry.
func (m *Manager) LoadModule(ctx context.Context, conf config.ModuleConfig) error {
	errCtx := map[string]any{"module": conf.Name}
	cmdStr := conf.Command

	// If no command but source is specified, resolve from registry.
	if cmdStr == "" && conf.Source != "" {
		if m.installer == nil {
			return ctxerr.With(fmt.Errorf("module specifies source %q but no registry is configured", conf.Source), errCtx)
		}
		resolved, err := m.installer.Resolve(ctx, conf.Source, conf.Version)
		if err != nil {
			return ctxerr.With(fmt.Errorf("resolving module from registry: %w", err), map[string]any{"module": conf.Name, "source": conf.Source, "version": conf.Version})
		}
		cmdStr = resolved
	}

	if cmdStr == "" {
		return ctxerr.With(fmt.Errorf("module has no command configured"), errCtx)
	}

	// Split command string into executable + args (handles quoted paths with spaces).
	parts := config.SplitCommand(cmdStr)

	// Resolve the executable (first element) to an absolute path so
	// exec.Command finds it on all platforms.
	executable, err := resolveExecutable(parts[0])
	if err != nil {
		return ctxerr.With(fmt.Errorf("resolving executable: %w", err), errCtx)
	}

	pluginMap := map[string]gomodule.Plugin{
		massmodule.ModuleName: &massmodule.ModuleGRPCPlugin{},
	}

	var client *gomodule.Client

	if conf.Debug {
		// Debug mode: connect to an already-running module process via .reattach.json.
		// The module must be started separately with MASS_MODULE_DEBUG=1.
		reattachPath := filepath.Join(filepath.Dir(executable), ".reattach.json")
		rc, err := loadReattachConfig(reattachPath)
		if err != nil {
			return ctxerr.With(fmt.Errorf("loading debug reattach config: %w", err), errCtx)
		}
		m.logger.Info().
			Str("module", conf.Name).
			Str("addr", rc.Addr.String()).
			Int("pid", rc.Pid).
			Msg("debug mode: reattaching to running module")

		client = gomodule.NewClient(&gomodule.ClientConfig{
			HandshakeConfig:  massmodule.Handshake,
			Plugins:          pluginMap,
			Reattach:         rc,
			AllowedProtocols: []gomodule.Protocol{gomodule.ProtocolGRPC},
			GRPCDialOptions: []grpc.DialOption{
				grpc.WithDefaultCallOptions(
					grpc.MaxCallRecvMsgSize(massmodule.MaxGRPCMessageSize),
					grpc.MaxCallSendMsgSize(massmodule.MaxGRPCMessageSize),
				),
			},
		})
	} else {
		// Build args: remaining command elements + optional config path.
		args := append([]string{}, parts[1:]...)
		if conf.Config != "" {
			configPath, err := filepath.Abs(conf.Config)
			if err != nil {
				return ctxerr.With(fmt.Errorf("resolving config path: %w", err), errCtx)
			}
			args = append(args, configPath)
		}

		cmd := exec.Command(executable, args...)
		hideConsole(cmd)

		// Ensure the module subprocess can find shared libraries (MinGW runtime
		// DLLs on Windows, ggml DLLs, etc.) by adding the directory of the
		// current executable and the module executable to PATH.
		cmd.Env = appendLibraryPaths(append(os.Environ(), m.extraEnv...), executable)

		lw := &logWriter{name: conf.Name, onLog: m.logCallback}
		client = gomodule.NewClient(&gomodule.ClientConfig{
			HandshakeConfig:  massmodule.Handshake,
			Plugins:          pluginMap,
			Cmd:              cmd,
			AllowedProtocols: []gomodule.Protocol{gomodule.ProtocolGRPC},
			Stderr:           lw,
			SyncStderr:       lw,
			Logger: hclog.New(&hclog.LoggerOptions{
				Name:   "module",
				Level:  hclog.Error,
				Output: io.Discard,
			}),
			GRPCDialOptions: []grpc.DialOption{
				grpc.WithDefaultCallOptions(
					grpc.MaxCallRecvMsgSize(massmodule.MaxGRPCMessageSize),
					grpc.MaxCallSendMsgSize(massmodule.MaxGRPCMessageSize),
				),
			},
		})
	}

	rpcClient, err := client.Client()
	if err != nil {
		client.Kill()
		return ctxerr.With(fmt.Errorf("connecting to module: %w", err), errCtx)
	}

	raw, err := rpcClient.Dispense(massmodule.ModuleName)
	if err != nil {
		client.Kill()
		return ctxerr.With(fmt.Errorf("dispensing module: %w", err), errCtx)
	}

	mod := raw.(massmodule.ModuleInterface)
	info, err := mod.GetInfo()
	if err != nil {
		client.Kill()
		return ctxerr.With(fmt.Errorf("getting info from module: %w", err), errCtx)
	}

	m.modules = append(m.modules, &LoadedModule{
		Name:   conf.Name,
		Info:   info,
		client: client,
		module: mod,
	})

	m.logger.Info().
		Str("module", info.Name).
		Str("version", info.Version).
		Int("models", len(info.Models)).
		Msg("module loaded")

	return nil
}

// Modules returns all loaded modules.
func (m *Manager) Modules() []*LoadedModule {
	return m.modules
}

// GetModule returns a loaded module by name, or nil if not found.
func (m *Manager) GetModule(name string) *LoadedModule {
	for _, mod := range m.modules {
		if mod.Name == name {
			return mod
		}
	}
	return nil
}

// UnloadModule stops and removes a single module by name.
func (m *Manager) UnloadModule(name string) error {
	for i, mod := range m.modules {
		if mod.Name == name {
			m.logger.Info().Str("module", name).Msg("unloading module")
			killAndWait(mod.client)
			m.modules = append(m.modules[:i], m.modules[i+1:]...)
			return nil
		}
	}
	return ctxerr.With(fmt.Errorf("module %q not found", name), map[string]any{"module": name})
}

// Shutdown kills all module subprocesses.
func (m *Manager) Shutdown() {
	for _, mod := range m.modules {
		m.logger.Info().Str("module", mod.Name).Msg("stopping module")
		killAndWait(mod.client)
	}
}

// killAndWait kills a go-plugin client and waits for the process to fully exit.
// On Windows, file locks on the binary persist until the process handle is released.
func killAndWait(c *gomodule.Client) {
	c.Kill()
	// go-plugin's Kill() sends the signal but the process may not have exited yet.
	// Poll Exited() to ensure the OS has released all file handles.
	for range 20 { // up to ~2s
		if c.Exited() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type logWriter struct {
	name  string
	onLog func(name, line string)
}

func (w *logWriter) Write(p []byte) (int, error) {
	// SyncStderr delivers raw byte chunks that may contain multiple lines.
	for _, line := range strings.Split(string(p), "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}
		// Write the raw line to host stderr as-is (preserves module formatting).
		fmt.Fprintln(os.Stderr, line)
		if w.onLog != nil {
			w.onLog(w.name, line)
		}
	}
	return len(p), nil
}

// appendLibraryPaths adds the directories of the current executable and the
// module binary to the PATH environment variable so that shared libraries
// (MinGW runtime DLLs, ggml DLLs) can be found by module subprocesses.
func appendLibraryPaths(environ []string, moduleBinary string) []string {
	extra := []string{}

	// Directory of the current (host) executable — contains runtime DLLs.
	if self, err := os.Executable(); err == nil {
		extra = append(extra, filepath.Dir(self))
	}

	// Directory of the module binary itself.
	if dir := filepath.Dir(moduleBinary); dir != "" {
		extra = append(extra, dir)
	}

	if len(extra) == 0 {
		return environ
	}

	sep := string(os.PathListSeparator)
	addition := ""
	for _, p := range extra {
		if addition != "" {
			addition += sep
		}
		addition += p
	}

	for i, env := range environ {
		// Case-insensitive match for PATH on Windows.
		if len(env) >= 5 && (env[:5] == "PATH=" || env[:5] == "Path=" || env[:5] == "path=") {
			environ[i] = env + sep + addition
			return environ
		}
	}
	// PATH not found — add it.
	return append(environ, "PATH="+addition)
}

// loadReattachConfig reads a .reattach.json file written by a module running
// in debug mode and returns a go-plugin ReattachConfig for connecting to it.
func loadReattachConfig(path string) (*gomodule.ReattachConfig, error) {
	errCtx := map[string]any{"path": path}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("reading reattach file: %w", err), errCtx)
	}
	var raw struct {
		Protocol        string `json:"protocol"`
		ProtocolVersion int    `json:"protocol_version"`
		AddrNetwork     string `json:"addr_network"`
		Addr            string `json:"addr"`
		Pid             int    `json:"pid"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, ctxerr.With(fmt.Errorf("parsing reattach file: %w", err), errCtx)
	}
	addr, err := net.ResolveTCPAddr(raw.AddrNetwork, raw.Addr)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("resolving address: %w", err), map[string]any{"network": raw.AddrNetwork, "addr": raw.Addr})
	}
	return &gomodule.ReattachConfig{
		Protocol:        gomodule.Protocol(raw.Protocol),
		ProtocolVersion: raw.ProtocolVersion,
		Addr:            addr,
		Pid:             raw.Pid,
	}, nil
}

// resolveExecutable converts a potentially relative binary path to an absolute
// path, appending ".exe" on Windows if the file doesn't exist without it.
func resolveExecutable(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(abs); err == nil {
		return abs, nil
	}
	if runtime.GOOS == "windows" && filepath.Ext(abs) == "" {
		withExe := abs + ".exe"
		if _, err := os.Stat(withExe); err == nil {
			return withExe, nil
		}
	}
	return abs, nil
}
