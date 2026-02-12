package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/KernelPryanic/ctxerr"
	pkgllm "github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/rs/zerolog"
	"gopkg.in/yaml.v3"
)

// DefaultListenAddr is the default HTTP listen address for the web UI.
const DefaultListenAddr = ":3455"

// LaunchMode controls how a module starts.
type LaunchMode string

const (
	LaunchModeManual   LaunchMode = "manual"    // User must click Start
	LaunchModeOnDemand LaunchMode = "on_demand" // Start on first request, stop after idle timeout
)

// DefaultModelIdleTimeout is the default idle timeout for dynamically loaded models.
const DefaultModelIdleTimeout = 2 * time.Minute

// DefaultModuleIdleTimeout is the default idle timeout for on-demand modules.
const DefaultModuleIdleTimeout = 5 * time.Second

// DefaultResultTTL is the default TTL for cached inference results.
const DefaultResultTTL = 24 * time.Hour

var (
	ErrModelPathEmpty  = pkgllm.ErrModelPathEmpty
	ErrModelNotFound   = errors.New("model not found")
	ErrLogLevelUnknown = errors.New("unsupported log level")
)

// LogLevel wraps zerolog.Level with YAML/text unmarshalling.
type LogLevel zerolog.Level

func (v LogLevel) MarshalText() ([]byte, error) {
	switch zerolog.Level(v) {
	case zerolog.TraceLevel:
		return []byte("trace"), nil
	case zerolog.DebugLevel:
		return []byte("debug"), nil
	case zerolog.InfoLevel:
		return []byte("info"), nil
	case zerolog.WarnLevel:
		return []byte("warn"), nil
	case zerolog.ErrorLevel:
		return []byte("error"), nil
	case zerolog.FatalLevel:
		return []byte("fatal"), nil
	case zerolog.PanicLevel:
		return []byte("panic"), nil
	case zerolog.Disabled:
		return []byte("disabled"), nil
	default:
		return []byte("info"), nil
	}
}

func (v *LogLevel) UnmarshalText(t []byte) error {
	switch string(t) {
	case "trace":
		*v = LogLevel(zerolog.TraceLevel)
	case "debug":
		*v = LogLevel(zerolog.DebugLevel)
	case "info":
		*v = LogLevel(zerolog.InfoLevel)
	case "warn":
		*v = LogLevel(zerolog.WarnLevel)
	case "error":
		*v = LogLevel(zerolog.ErrorLevel)
	case "fatal":
		*v = LogLevel(zerolog.FatalLevel)
	case "panic":
		*v = LogLevel(zerolog.PanicLevel)
	case "disabled":
		*v = LogLevel(zerolog.Disabled)
	default:
		return ErrLogLevelUnknown
	}
	return nil
}

// LoggerConfig holds logging settings.
type LoggerConfig struct {
	Level         LogLevel `yaml:"level"`
	ConsoleWriter bool     `yaml:"console_writer"`
}

// Config is the unified application configuration.
type Config struct {
	ListenAddr        string `yaml:"listen_addr" json:"listen_addr"`
	AuthToken         string `yaml:"auth_token,omitempty" json:"-"` // Legacy: read from YAML for migration only, never serialized
	DataDir           string `yaml:"data_dir,omitempty" json:"data_dir,omitempty"`
	Theme             string `yaml:"theme,omitempty" json:"theme,omitempty"`       // "dark" or "light", default "dark"
	DevMode           bool   `yaml:"dev_mode,omitempty" json:"dev_mode,omitempty"` // Enables developer tools
	RegistryURL       string `yaml:"registry_url,omitempty" json:"registry_url,omitempty"`
	ModelIdleTimeout  string `yaml:"model_idle_timeout,omitempty" json:"model_idle_timeout,omitempty"`   // Idle timeout before evicting dynamic models (e.g. "5m")
	ModuleIdleTimeout string `yaml:"module_idle_timeout,omitempty" json:"module_idle_timeout,omitempty"` // Idle timeout before stopping on-demand modules (e.g. "5s")
	ResultTTL         string `yaml:"result_ttl,omitempty" json:"result_ttl,omitempty"`                   // How long to keep inference results for caching (e.g. "24h")

	Logger LoggerConfig `yaml:"logger,omitempty" json:"logger,omitempty"`
	TLS    TLSConfig    `yaml:"tls,omitempty" json:"tls,omitempty"`

	// Modules are persisted separately in modules.yml.
	Modules []ModuleConfig `yaml:"-" json:"-"`
}

// TLSConfig holds TLS settings for MASS server and agent communication.
type TLSConfig struct {
	// Enabled activates TLS. When false, the server uses plaintext h2c.
	Enabled bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	// CertFile is the path to a PEM file containing the certificate and private key.
	CertFile string `yaml:"cert_file,omitempty" json:"cert_file,omitempty"`
}

// EffectiveListenAddr returns ListenAddr if set, otherwise DefaultListenAddr.
func (c *Config) EffectiveListenAddr() string {
	if c.ListenAddr != "" {
		return c.ListenAddr
	}
	return DefaultListenAddr
}

// EffectiveModelIdleTimeout returns the idle timeout for dynamic model eviction,
// defaulting to DefaultModelIdleTimeout if empty or invalid.
func (c *Config) EffectiveModelIdleTimeout() time.Duration {
	if c.ModelIdleTimeout == "" {
		return DefaultModelIdleTimeout
	}
	d, err := time.ParseDuration(c.ModelIdleTimeout)
	if err != nil {
		return DefaultModelIdleTimeout
	}
	return d
}

// EffectiveModuleIdleTimeout returns the idle timeout for on-demand module shutdown,
// defaulting to DefaultModuleIdleTimeout if empty or invalid.
func (c *Config) EffectiveModuleIdleTimeout() time.Duration {
	if c.ModuleIdleTimeout == "" {
		return DefaultModuleIdleTimeout
	}
	d, err := time.ParseDuration(c.ModuleIdleTimeout)
	if err != nil {
		return DefaultModuleIdleTimeout
	}
	return d
}

// EffectiveResultTTL returns the configured result TTL duration,
// defaulting to DefaultResultTTL if empty or invalid.
func (c *Config) EffectiveResultTTL() time.Duration {
	if c.ResultTTL == "" {
		return DefaultResultTTL
	}
	d, err := time.ParseDuration(c.ResultTTL)
	if err != nil {
		return DefaultResultTTL
	}
	return d
}

// EffectiveDataDir returns the configured DataDir or the platform default.
func (c *Config) EffectiveDataDir() (string, error) {
	if c.DataDir != "" {
		return c.DataDir, nil
	}
	return DefaultDataDir()
}

// FindModule returns the ModuleConfig with the given name, or nil.
func (c *Config) FindModule(name string) *ModuleConfig {
	for i := range c.Modules {
		if c.Modules[i].Name == name {
			return &c.Modules[i]
		}
	}
	return nil
}

// RemoveModule removes a module by name. Returns true if found and removed.
func (c *Config) RemoveModule(name string) bool {
	for i := range c.Modules {
		if c.Modules[i].Name == name {
			c.Modules = append(c.Modules[:i], c.Modules[i+1:]...)
			return true
		}
	}
	return false
}

// ModuleConfig describes a module known to MASS (unified superset).
type ModuleConfig struct {
	Name       string     `yaml:"name" json:"name"`
	Command    string     `yaml:"command" json:"command"`                   // Command to execute, e.g. "./module" or "python main.py"
	Config     string     `yaml:"config,omitempty" json:"config,omitempty"` // Config file path passed to module as extra arg
	Source     string     `yaml:"source,omitempty" json:"source,omitempty"` // "local", "url", "github:owner/repo", or registry name
	Version    string     `yaml:"version,omitempty" json:"version,omitempty"`
	Debug      bool       `yaml:"debug,omitempty" json:"debug,omitempty"`             // Connect to already-running module via .reattach.json
	AutoStart  bool       `yaml:"auto_start,omitempty" json:"auto_start,omitempty"`   // Start subprocess when MASS launches
	LaunchMode LaunchMode `yaml:"launch_mode,omitempty" json:"launch_mode,omitempty"` // manual or on_demand
}

// EffectiveLaunchMode returns the configured launch mode, defaulting to on_demand.
func (mc *ModuleConfig) EffectiveLaunchMode() LaunchMode {
	if mc.LaunchMode == "" {
		return LaunchModeOnDemand
	}
	return mc.LaunchMode
}

// Default returns a Config with sensible defaults.
func Default() *Config {
	return &Config{
		Logger: LoggerConfig{
			Level:         LogLevel(zerolog.DebugLevel),
			ConsoleWriter: true,
		},
		Modules: []ModuleConfig{},
	}
}

// DefaultPath returns the default config file path under os.UserConfigDir().
func DefaultPath() (string, error) {
	cfgDir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("getting user config dir: %w", err)
	}
	massDir := filepath.Join(cfgDir, "mass")
	if err := os.MkdirAll(massDir, 0755); err != nil {
		return "", fmt.Errorf("creating mass config dir: %w", err)
	}
	return filepath.Join(massDir, "config.yml"), nil
}

// ModulesPath returns the modules.yml path for a given config.yml path.
func ModulesPath(configPath string) string {
	return filepath.Join(filepath.Dir(configPath), "modules.yml")
}

// LogsDir returns the platform-appropriate logs directory.
//
//	Windows: {configDir}/logs  (e.g. %APPDATA%/mass/logs)
//	macOS:   ~/Library/Logs/mass
//	Linux:   $XDG_STATE_HOME/mass/logs or ~/.local/state/mass/logs
func LogsDir(configPath string) string {
	switch runtime.GOOS {
	case "darwin":
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, "Library", "Logs", "mass")
		}
	case "linux":
		if xdg := os.Getenv("XDG_STATE_HOME"); xdg != "" {
			return filepath.Join(xdg, "mass", "logs")
		}
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, ".local", "state", "mass", "logs")
		}
	}
	// Windows and fallback: logs next to config.
	return filepath.Join(filepath.Dir(configPath), "logs")
}

// Load reads the configuration from the specified YAML file and module
// settings from the sibling modules.yml file. Returns Default() if neither
// file exists. Also handles migration from legacy gui-config.json.
// The returned firstRun flag is true when no config file existed on disk.
func Load(path string) (cfg *Config, firstRun bool, err error) {
	errCtx := map[string]any{"path": path}
	cfg = Default()

	data, readErr := os.ReadFile(path)
	if readErr != nil {
		if !os.IsNotExist(readErr) {
			return nil, false, ctxerr.With(fmt.Errorf("reading config: %w", readErr), errCtx)
		}
		firstRun = true
		// Try legacy gui-config.json migration.
		cfg = migrateFromLegacy(filepath.Dir(path), cfg)
	} else {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, false, ctxerr.With(fmt.Errorf("parsing config: %w", err), errCtx)
		}
	}

	// Load modules from dedicated file.
	modulesPath := ModulesPath(path)
	pdata, readErr := os.ReadFile(modulesPath)
	if readErr == nil {
		// Intermediate type to handle migration from legacy fields.
		type moduleWithLegacy struct {
			ModuleConfig `yaml:",inline"`
			Binary       string `yaml:"binary"`
		}
		var raw []moduleWithLegacy
		if err := yaml.Unmarshal(pdata, &raw); err != nil {
			return nil, false, ctxerr.With(fmt.Errorf("parsing modules config: %w", err), map[string]any{
				"modules_path": modulesPath,
			})
		}
		cfg.Modules = make([]ModuleConfig, len(raw))
		for i, r := range raw {
			cfg.Modules[i] = r.ModuleConfig
			// Migrate legacy "binary" field to "command".
			if cfg.Modules[i].Command == "" && r.Binary != "" {
				cfg.Modules[i].Command = r.Binary
			}
			// Migrate legacy launch_mode "auto" → AutoStart + manual.
			if cfg.Modules[i].LaunchMode == "auto" {
				cfg.Modules[i].AutoStart = true
				cfg.Modules[i].LaunchMode = LaunchModeManual
			}
		}
	}

	return cfg, firstRun, nil
}

// migrateFromLegacy reads the old gui-config.json and extracts settings.
func migrateFromLegacy(dir string, cfg *Config) *Config {
	legacyPath := filepath.Join(dir, "gui-config.json")
	data, err := os.ReadFile(legacyPath)
	if err != nil {
		return cfg
	}

	var legacy struct {
		ListenAddr string         `json:"listen_addr"`
		AuthToken  string         `json:"auth_token"`
		DataDir    string         `json:"data_dir"`
		Theme      string         `json:"theme"`
		DevMode    bool           `json:"dev_mode"`
		Plugins    []ModuleConfig `json:"plugins"`
	}
	if err := json.Unmarshal(data, &legacy); err != nil {
		return cfg
	}

	cfg.ListenAddr = legacy.ListenAddr
	cfg.AuthToken = legacy.AuthToken
	cfg.DataDir = legacy.DataDir
	cfg.Theme = legacy.Theme
	cfg.DevMode = legacy.DevMode
	cfg.Modules = legacy.Plugins

	return cfg
}

// Save writes the configuration to config.yml and module settings
// to a sibling modules.yml file.
func Save(cfg *Config, path string) error {
	errCtx := map[string]any{"path": path}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return ctxerr.With(fmt.Errorf("marshalling config: %w", err), errCtx)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return ctxerr.With(fmt.Errorf("writing config: %w", err), errCtx)
	}

	modulesPath := ModulesPath(path)
	pdata, err := yaml.Marshal(cfg.Modules)
	if err != nil {
		return ctxerr.With(fmt.Errorf("marshalling modules config: %w", err), map[string]any{
			"modules_path": modulesPath,
		})
	}
	if err := os.WriteFile(modulesPath, pdata, 0644); err != nil {
		return ctxerr.With(fmt.Errorf("writing modules config: %w", err), map[string]any{
			"modules_path": modulesPath,
		})
	}
	return nil
}

// DefaultDataDir returns the platform-appropriate default data directory.
//
//	Windows: %LOCALAPPDATA%/mass
//	Linux:   $XDG_DATA_HOME/mass or ~/.local/share/mass
//	macOS:   ~/Library/Application Support/mass
func DefaultDataDir() (string, error) {
	switch runtime.GOOS {
	case "windows":
		dir, err := os.UserCacheDir() // %LOCALAPPDATA%
		if err != nil {
			return "", fmt.Errorf("getting user cache dir: %w", err)
		}
		return filepath.Join(dir, "mass"), nil
	case "darwin":
		dir, err := os.UserConfigDir() // ~/Library/Application Support
		if err != nil {
			return "", fmt.Errorf("getting user config dir: %w", err)
		}
		return filepath.Join(dir, "mass"), nil
	default:
		if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
			return filepath.Join(xdg, "mass"), nil
		}
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("getting user home dir: %w", err)
		}
		return filepath.Join(home, ".local", "share", "mass"), nil
	}
}

// ModuleInstallDir returns the directory where module binaries are installed.
func ModuleInstallDir(dataDir string) string {
	return filepath.Join(dataDir, "modules")
}

// ModuleDir returns the base directory for a specific module (contains version subdirs).
func ModuleDir(dataDir, moduleName string) string {
	return filepath.Join(dataDir, "modules", moduleName)
}

// ModuleVersionDir returns the install directory for a specific module version.
func ModuleVersionDir(dataDir, moduleName, version string) string {
	return filepath.Join(dataDir, "modules", moduleName, version)
}

// ModelsDir returns the centralized models directory: {dataDir}/models/.
func ModelsDir(dataDir string) string {
	return filepath.Join(dataDir, "models")
}

// ModuleModelsDir returns the models directory for a specific module.
func ModuleModelsDir(dataDir, moduleName string) string {
	return filepath.Join(dataDir, "models", moduleName)
}

// --- Command string helpers ---
//
// Command strings may contain paths with spaces. Such paths are stored
// in double-quoted form (e.g. `"C:\Program Files\mod\mod.exe" --flag`).
// SplitCommand splits a command string into tokens respecting double quotes.
// QuotePath wraps a path in double quotes if it contains spaces.

// SplitCommand splits a command string into tokens, respecting double-quoted
// segments. For example:
//
//	`"C:\path with spaces\bin.exe" --flag arg` → ["C:\path with spaces\bin.exe", "--flag", "arg"]
//	`simple.exe arg1 arg2`                     → ["simple.exe", "arg1", "arg2"]
func SplitCommand(command string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false

	for i := 0; i < len(command); i++ {
		ch := command[i]
		switch {
		case ch == '"':
			inQuote = !inQuote
		case ch == ' ' && !inQuote:
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

// QuotePath returns the path wrapped in double quotes if it contains spaces,
// otherwise returns it unchanged.
func QuotePath(path string) string {
	if strings.Contains(path, " ") {
		return `"` + path + `"`
	}
	return path
}

// ExpandVars replaces ${KEY} placeholders in s using the provided vars map.
func ExpandVars(s string, vars map[string]string) string {
	for k, v := range vars {
		s = strings.ReplaceAll(s, "${"+k+"}", v)
	}
	return s
}

// ExpandCommandVars replaces ${KEY} placeholders in a command string, quoting
// any resulting tokens that contain spaces so SplitCommand can parse them.
func ExpandCommandVars(command string, vars map[string]string) string {
	parts := SplitCommand(command)
	for i, p := range parts {
		expanded := ExpandVars(p, vars)
		if expanded != p {
			parts[i] = QuotePath(expanded)
		}
	}
	return strings.Join(parts, " ")
}

// --- Model configs (canonical definitions in pkg/llm, re-exported here) ---

type ChatModelConfig = pkgllm.ChatModelConfig
type EmbeddingModelConfig = pkgllm.EmbeddingModelConfig
type PlacementConfig = pkgllm.PlacementConfig
