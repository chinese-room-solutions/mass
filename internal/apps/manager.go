package apps

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
	massapp "github.com/chinese-room-solutions/mass-sdk/app"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/hashicorp/go-hclog"
	gomodule "github.com/hashicorp/go-plugin"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

// AppResolverInterface resolves an app name+version to a command string,
// installing it from a registry if needed.
type AppResolverInterface interface {
	Resolve(ctx context.Context, name, version string) (string, error)
}

// LoadedApp holds a running app's state.
type LoadedApp struct {
	Name   string
	Info   *massapp.AppInfo
	client *gomodule.Client
	app    massapp.AppInterface
}

// App returns the underlying gRPC app client.
func (lm *LoadedApp) App() massapp.AppInterface {
	return lm.app
}

// Compile-time check: Manager implements AppRuntimeInterface.
var _ AppRuntimeInterface = (*Manager)(nil)

// Manager discovers, launches, and manages apps via bare processes (go-plugin).
type Manager struct {
	logger      zerolog.Logger
	apps        []*LoadedApp
	installer   AppResolverInterface // nil when no registry is configured
	extraEnv    []string             // additional env vars for app processes
	logCallback func(name, line string)
}

// NewManager creates a new app Manager.
func NewManager(logger zerolog.Logger) *Manager {
	return &Manager{logger: logger}
}

// SetExtraEnv sets additional environment variables for app subprocesses.
func (m *Manager) SetExtraEnv(env []string) {
	m.extraEnv = env
}

// SetLogCallback sets a callback that is invoked for each log line written
// to an app's stderr. The callback receives the app name and the line.
func (m *Manager) SetLogCallback(fn func(name, line string)) {
	m.logCallback = fn
}

// SetInstaller configures an app installer for registry-based app resolution.
func (m *Manager) SetInstaller(inst AppResolverInterface) {
	m.installer = inst
}

// LoadApp launches an app process and queries its metadata.
// The app is identified by its Command field (e.g. "./binary" or
// "python main.py"). If Command is empty but Source is set, the
// command is resolved from the registry.
func (m *Manager) LoadApp(ctx context.Context, conf config.AppConfig) error {
	errCtx := map[string]any{"app": conf.Name}
	cmdStr := conf.Command

	// If no command but source is specified, resolve from registry.
	if cmdStr == "" && conf.Source != "" {
		if m.installer == nil {
			return ctxerr.With(fmt.Errorf("app specifies source %q but no registry is configured", conf.Source), errCtx)
		}
		resolved, err := m.installer.Resolve(ctx, conf.Source, conf.Version)
		if err != nil {
			return ctxerr.With(fmt.Errorf("resolving app from registry: %w", err), map[string]any{"app": conf.Name, "source": conf.Source, "version": conf.Version})
		}
		cmdStr = resolved
	}

	if cmdStr == "" {
		return ctxerr.With(fmt.Errorf("app has no command configured"), errCtx)
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
		massapp.AppName: &massapp.AppGRPCPlugin{},
	}

	var client *gomodule.Client

	if conf.Debug {
		// Debug mode: connect to an already-running app process via .reattach.json.
		// The app must be started separately with MASS_APP_DEBUG=1.
		reattachPath := filepath.Join(filepath.Dir(executable), ".reattach.json")
		rc, err := loadReattachConfig(reattachPath)
		if err != nil {
			return ctxerr.With(fmt.Errorf("loading debug reattach config: %w", err), errCtx)
		}
		m.logger.Info().
			Str("app", conf.Name).
			Str("addr", rc.Addr.String()).
			Int("pid", rc.Pid).
			Msg("debug mode: reattaching to running app")

		client = gomodule.NewClient(&gomodule.ClientConfig{
			HandshakeConfig:  massapp.Handshake,
			Plugins:          pluginMap,
			Reattach:         rc,
			AllowedProtocols: []gomodule.Protocol{gomodule.ProtocolGRPC},
			GRPCDialOptions: []grpc.DialOption{
				grpc.WithDefaultCallOptions(
					grpc.MaxCallRecvMsgSize(massapp.MaxGRPCMessageSize),
					grpc.MaxCallSendMsgSize(massapp.MaxGRPCMessageSize),
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

		// Ensure the app subprocess can find shared libraries (MinGW runtime
		// DLLs on Windows, ggml DLLs, etc.) by adding the directory of the
		// current executable and the app executable to PATH.
		cmd.Env = appendLibraryPaths(append(os.Environ(), m.extraEnv...), executable)

		lw := &logWriter{name: conf.Name, onLog: m.logCallback}
		client = gomodule.NewClient(&gomodule.ClientConfig{
			HandshakeConfig:  massapp.Handshake,
			Plugins:          pluginMap,
			Cmd:              cmd,
			AllowedProtocols: []gomodule.Protocol{gomodule.ProtocolGRPC},
			Stderr:           lw,
			SyncStderr:       lw,
			Logger: hclog.New(&hclog.LoggerOptions{
				Name:   "app",
				Level:  hclog.Error,
				Output: io.Discard,
			}),
			GRPCDialOptions: []grpc.DialOption{
				grpc.WithDefaultCallOptions(
					grpc.MaxCallRecvMsgSize(massapp.MaxGRPCMessageSize),
					grpc.MaxCallSendMsgSize(massapp.MaxGRPCMessageSize),
				),
			},
		})
	}

	rpcClient, err := client.Client()
	if err != nil {
		client.Kill()
		return ctxerr.With(fmt.Errorf("connecting to app: %w", err), errCtx)
	}

	raw, err := rpcClient.Dispense(massapp.AppName)
	if err != nil {
		client.Kill()
		return ctxerr.With(fmt.Errorf("dispensing app: %w", err), errCtx)
	}

	app := raw.(massapp.AppInterface)
	info, err := app.GetInfo()
	if err != nil {
		client.Kill()
		return ctxerr.With(fmt.Errorf("getting info from app: %w", err), errCtx)
	}

	m.apps = append(m.apps, &LoadedApp{
		Name:   conf.Name,
		Info:   info,
		client: client,
		app:    app,
	})

	m.logger.Info().
		Str("app", info.Name).
		Str("version", info.Version).
		Int("models", len(info.Models)).
		Msg("app loaded")

	return nil
}

// GetApp returns a loaded app by name, or nil if not found.
func (m *Manager) GetApp(name string) *LoadedApp {
	for _, a := range m.apps {
		if a.Name == name {
			return a
		}
	}
	return nil
}

// Shutdown kills all app subprocesses.
func (m *Manager) Shutdown() {
	for _, a := range m.apps {
		m.logger.Info().Str("app", a.Name).Msg("stopping app")
		killAndWait(a.client)
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
		// Write the raw line to host stderr as-is (preserves app formatting).
		fmt.Fprintln(os.Stderr, line)
		if w.onLog != nil {
			w.onLog(w.name, line)
		}
	}
	return len(p), nil
}

// appendLibraryPaths adds the directories of the current executable and the
// app binary to the PATH environment variable so that shared libraries
// (MinGW runtime DLLs, ggml DLLs) can be found by app subprocesses.
func appendLibraryPaths(environ []string, appBinary string) []string {
	extra := []string{}

	// Directory of the current (host) executable — contains runtime DLLs.
	if self, err := os.Executable(); err == nil {
		extra = append(extra, filepath.Dir(self))
	}

	// Directory of the app binary itself.
	if dir := filepath.Dir(appBinary); dir != "" {
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

// loadReattachConfig reads a .reattach.json file written by an app running
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
