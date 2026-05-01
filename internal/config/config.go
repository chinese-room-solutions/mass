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
	"github.com/rs/zerolog"
	"gopkg.in/yaml.v3"
)

// DefaultListenAddr is the default HTTP listen address for the web UI.
const DefaultListenAddr = ":3455"

// DefaultResultTTL is the default TTL for cached job results.
const DefaultResultTTL = 24 * time.Hour

// DefaultIdleEvictionTTL is the default time a loaded model can sit
// idle on a worker before MASS evicts it. Short enough that a forgotten
// model frees its slot quickly; long enough that back-to-back chats
// don't pay reload cost.
const DefaultIdleEvictionTTL = 30 * time.Second

// ErrLogLevelUnknown is returned when an unrecognized log level string is parsed.
var ErrLogLevelUnknown = errors.New("unsupported log level")

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
	ListenAddr      string `yaml:"listen_addr" json:"listen_addr"`
	AuthToken       string `yaml:"auth_token,omitempty" json:"-" secret:"true"` // Legacy: read from YAML for migration only, never serialized
	DataDir         string `yaml:"data_dir,omitempty" json:"data_dir,omitempty"`
	Theme           string `yaml:"theme,omitempty" json:"theme,omitempty"`       // "dark" or "light", default "dark"
	DevMode         bool   `yaml:"dev_mode,omitempty" json:"dev_mode,omitempty"` // Enables developer tools
	RegistryURL     string `yaml:"registry_url,omitempty" json:"registry_url,omitempty"`
	ResultTTL       string `yaml:"result_ttl,omitempty" json:"result_ttl,omitempty"`               // How long to keep job results (e.g. "24h")
	IdleEvictionTTL string `yaml:"idle_eviction_ttl,omitempty" json:"idle_eviction_ttl,omitempty"` // How long a loaded model can sit idle before eviction (e.g. "10s")

	Logger LoggerConfig `yaml:"logger,omitempty" json:"logger,omitempty"`
	TLS    TLSConfig    `yaml:"tls,omitempty" json:"tls,omitempty"`
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

// EffectiveIdleEvictionTTL returns the configured idle-eviction TTL
// duration, defaulting to DefaultIdleEvictionTTL if empty or invalid.
func (c *Config) EffectiveIdleEvictionTTL() time.Duration {
	if c.IdleEvictionTTL == "" {
		return DefaultIdleEvictionTTL
	}
	d, err := time.ParseDuration(c.IdleEvictionTTL)
	if err != nil {
		return DefaultIdleEvictionTTL
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

// Default returns a Config with sensible defaults.
func Default() *Config {
	return &Config{
		Logger: LoggerConfig{
			Level:         LogLevel(zerolog.DebugLevel),
			ConsoleWriter: true,
		},
	}
}

// ConfigFile is the YAML config file name within the MASS config directory.
const ConfigFile = "config.yml"

// DefaultDir returns the platform-appropriate MASS config directory,
// creating it if needed (e.g. %APPDATA%/mass on Windows, ~/.config/mass
// on Linux). [ConfigFile] lives inside it.
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

// Load reads ConfigFile from the given config directory. Returns Default()
// (overlaid with disk content) and firstRun=true when no file exists yet.
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

	return cfg, firstRun, nil
}

// Save writes ConfigFile to the given config directory.
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

// ModelsDir returns the centralized models root: {dataDir}/models/.
// Layout under it is one subdirectory per format ({dataDir}/models/gguf/,
// {dataDir}/models/onnx/, …), each holding flat canonical-name files. The
// gateway is given the root (via InitRequest.models_dir) and walks its
// own format subdir(s).
func ModelsDir(dataDir string) string {
	return filepath.Join(dataDir, "models")
}

// FormatModelsDir returns the directory holding all files for one model
// format: {dataDir}/models/{format}/. Multiple runtimes that handle the
// same format share this directory.
func FormatModelsDir(dataDir, format string) string {
	return filepath.Join(ModelsDir(dataDir), format)
}

// RuntimesDir returns the directory where installed runtime gateway packages
// live: {dataDir}/runtimes/.
func RuntimesDir(dataDir string) string {
	return filepath.Join(dataDir, "runtimes")
}

// RuntimeDir returns the install directory for a specific runtime kind.
// The gateway's persistent state (e.g. catalogue files) lives inside
// this dir; uninstall preserves state files (see [Manager.Uninstall]).
func RuntimeDir(dataDir, runtimeName string) string {
	return filepath.Join(RuntimesDir(dataDir), runtimeName)
}

// --- Command string helpers ---
//
// Command strings may contain paths with spaces. Such paths are stored
// in double-quoted form (e.g. `"C:\Program Files\app\app.exe" --flag`).
// SplitCommand splits a command string into tokens respecting double quotes.
// QuotePath wraps a path in double quotes if it contains spaces.

// SplitCommand splits a command string into tokens, respecting double-quoted
// segments.
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
