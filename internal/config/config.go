package config

import (
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

// LaunchMode controls how an app starts.
type LaunchMode string

const (
	LaunchModeManual   LaunchMode = "manual"    // User must click Start
	LaunchModeOnDemand LaunchMode = "on_demand" // Start on first request, stop after idle timeout
)

// DefaultModelIdleTimeout is the default idle timeout for dynamically loaded models.
const DefaultModelIdleTimeout = 2 * time.Minute

// DefaultAppIdleTimeout is the default idle timeout for on-demand apps.
const DefaultAppIdleTimeout = 5 * time.Second

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
	ListenAddr       string `yaml:"listen_addr" json:"listen_addr"`
	AuthToken        string `yaml:"auth_token,omitempty" json:"-" secret:"true"` // Legacy: read from YAML for migration only, never serialized
	DataDir          string `yaml:"data_dir,omitempty" json:"data_dir,omitempty"`
	Theme            string `yaml:"theme,omitempty" json:"theme,omitempty"`       // "dark" or "light", default "dark"
	DevMode          bool   `yaml:"dev_mode,omitempty" json:"dev_mode,omitempty"` // Enables developer tools
	RegistryURL      string `yaml:"registry_url,omitempty" json:"registry_url,omitempty"`
	ModelIdleTimeout string `yaml:"model_idle_timeout,omitempty" json:"model_idle_timeout,omitempty"` // Idle timeout before evicting dynamic models (e.g. "5m")
	AppIdleTimeout   string `yaml:"app_idle_timeout,omitempty" json:"app_idle_timeout,omitempty"`     // Idle timeout before stopping on-demand apps (e.g. "5s")
	ResultTTL        string `yaml:"result_ttl,omitempty" json:"result_ttl,omitempty"`                 // How long to keep inference results for caching (e.g. "24h")

	Logger LoggerConfig `yaml:"logger,omitempty" json:"logger,omitempty"`
	TLS    TLSConfig    `yaml:"tls,omitempty" json:"tls,omitempty"`

	// Apps are persisted separately in apps.yml.
	Apps []AppConfig `yaml:"-" json:"-"`
}

// TLSConfig holds TLS settings for MASS server and worker communication.
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

// EffectiveAppIdleTimeout returns the idle timeout for on-demand app shutdown,
// defaulting to DefaultAppIdleTimeout if empty or invalid.
func (c *Config) EffectiveAppIdleTimeout() time.Duration {
	if c.AppIdleTimeout == "" {
		return DefaultAppIdleTimeout
	}
	d, err := time.ParseDuration(c.AppIdleTimeout)
	if err != nil {
		return DefaultAppIdleTimeout
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

// FindApp returns the AppConfig with the given name, or nil.
func (c *Config) FindApp(name string) *AppConfig {
	for i := range c.Apps {
		if c.Apps[i].Name == name {
			return &c.Apps[i]
		}
	}
	return nil
}

// RemoveApp removes an app by name. Returns true if found and removed.
func (c *Config) RemoveApp(name string) bool {
	for i := range c.Apps {
		if c.Apps[i].Name == name {
			c.Apps = append(c.Apps[:i], c.Apps[i+1:]...)
			return true
		}
	}
	return false
}

// AppConfig describes an app known to MASS (unified superset).
type AppConfig struct {
	Name       string     `yaml:"name" json:"name"`
	Command    string     `yaml:"command" json:"command"`                   // Command to execute, e.g. "./app" or "python main.py"
	Config     string     `yaml:"config,omitempty" json:"config,omitempty"` // Config file path passed to app as extra arg
	Source     string     `yaml:"source,omitempty" json:"source,omitempty"` // "local", "url", "github:owner/repo", or registry name
	Version    string     `yaml:"version,omitempty" json:"version,omitempty"`
	Debug      bool       `yaml:"debug,omitempty" json:"debug,omitempty"`             // Connect to already-running app via .reattach.json
	AutoStart  bool       `yaml:"auto_start,omitempty" json:"auto_start,omitempty"`   // Start subprocess when MASS launches
	LaunchMode LaunchMode `yaml:"launch_mode,omitempty" json:"launch_mode,omitempty"` // manual or on_demand
}

// EffectiveLaunchMode returns the configured launch mode, defaulting to on_demand.
func (mc *AppConfig) EffectiveLaunchMode() LaunchMode {
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
		Apps: []AppConfig{},
	}
}

// File names within the MASS config directory.
const (
	ConfigFile = "config.yml"
	AppsFile   = "apps.yml"
)

// DefaultDir returns the platform-appropriate MASS config directory,
// creating it if needed (e.g. %APPDATA%/mass on Windows, ~/.config/mass
// on Linux). Files like ConfigFile and AppsFile live inside it.
func DefaultDir() (string, error) {
	cfgDir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("getting user config dir: %w", err)
	}
	massDir := filepath.Join(cfgDir, "mass")
	if err := os.MkdirAll(massDir, 0755); err != nil {
		return "", fmt.Errorf("creating mass config dir: %w", err)
	}
	return massDir, nil
}

// LogsDir returns the platform-appropriate logs directory. configDir is the
// MASS config directory (used as the fallback root on Windows/unknown).
//
//	Windows: {configDir}/logs  (e.g. %APPDATA%/mass/logs)
//	macOS:   ~/Library/Logs/mass
//	Linux:   $XDG_STATE_HOME/mass/logs or ~/.local/state/mass/logs
func LogsDir(configDir string) string {
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
	return filepath.Join(configDir, "logs")
}

// Load reads ConfigFile and AppsFile from the given config directory.
// Returns Default() (with overlay from any existing files) and firstRun=true
// when no ConfigFile exists on disk.
func Load(configDir string) (cfg *Config, firstRun bool, err error) {
	cfgPath := filepath.Join(configDir, ConfigFile)
	errCtx := map[string]any{"path": cfgPath}
	cfg = Default()

	data, readErr := os.ReadFile(cfgPath)
	if readErr != nil {
		if !os.IsNotExist(readErr) {
			return nil, false, ctxerr.With(fmt.Errorf("reading config: %w", readErr), errCtx)
		}
		firstRun = true
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, false, ctxerr.With(fmt.Errorf("parsing config: %w", err), errCtx)
	}

	appsPath := filepath.Join(configDir, AppsFile)
	pdata, readErr := os.ReadFile(appsPath)
	if readErr == nil {
		if err := yaml.Unmarshal(pdata, &cfg.Apps); err != nil {
			return nil, false, ctxerr.With(fmt.Errorf("parsing apps config: %w", err), map[string]any{
				"apps_path": appsPath,
			})
		}
	}

	return cfg, firstRun, nil
}

// Save writes ConfigFile and AppsFile to the given config directory.
func Save(cfg *Config, configDir string) error {
	cfgPath := filepath.Join(configDir, ConfigFile)
	errCtx := map[string]any{"path": cfgPath}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return ctxerr.With(fmt.Errorf("marshalling config: %w", err), errCtx)
	}
	if err := os.WriteFile(cfgPath, data, 0644); err != nil {
		return ctxerr.With(fmt.Errorf("writing config: %w", err), errCtx)
	}

	appsPath := filepath.Join(configDir, AppsFile)
	pdata, err := yaml.Marshal(cfg.Apps)
	if err != nil {
		return ctxerr.With(fmt.Errorf("marshalling apps config: %w", err), map[string]any{
			"apps_path": appsPath,
		})
	}
	if err := os.WriteFile(appsPath, pdata, 0644); err != nil {
		return ctxerr.With(fmt.Errorf("writing apps config: %w", err), map[string]any{
			"apps_path": appsPath,
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

// AppInstallDir returns the directory where app binaries are installed.
func AppInstallDir(dataDir string) string {
	return filepath.Join(dataDir, "apps")
}

// AppDir returns the base directory for a specific app (contains version subdirs).
func AppDir(dataDir, appName string) string {
	return filepath.Join(dataDir, "apps", appName)
}

// AppVersionDir returns the install directory for a specific app version.
func AppVersionDir(dataDir, appName, version string) string {
	return filepath.Join(dataDir, "apps", appName, version)
}

// ModelsDir returns the centralized models directory: {dataDir}/models/.
func ModelsDir(dataDir string) string {
	return filepath.Join(dataDir, "models")
}

// --- Command string helpers ---
//
// Command strings may contain paths with spaces. Such paths are stored
// in double-quoted form (e.g. `"C:\Program Files\app\app.exe" --flag`).
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

type LlamaChatConfig = pkgllm.LlamaChatConfig
type LlamaEmbeddingConfig = pkgllm.LlamaEmbeddingConfig
type PlacementConfig = pkgllm.PlacementConfig

type ModelConfigInterface = pkgllm.ModelConfigInterface
type ChatModelConfigInterface = pkgllm.ChatModelConfigInterface
type EmbeddingModelConfigInterface = pkgllm.EmbeddingModelConfigInterface
