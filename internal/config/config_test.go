package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLlamaChatConfigValidate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := LlamaChatConfig{
			Path:        "./models/test.gguf",
			ContextSize: 2048,
			MaxTokens:   1024,
		}
		require.NoError(t, cfg.Validate())
	})

	t.Run("empty path", func(t *testing.T) {
		cfg := LlamaChatConfig{
			ContextSize: 2048,
		}
		require.ErrorIs(t, cfg.Validate(), ErrModelPathEmpty)
	})
}

func TestLlamaEmbeddingConfigValidate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := LlamaEmbeddingConfig{
			Path:        "./models/test-embed.gguf",
			ContextSize: 2048,
		}
		require.NoError(t, cfg.Validate())
	})

	t.Run("empty path", func(t *testing.T) {
		cfg := LlamaEmbeddingConfig{
			ContextSize: 2048,
		}
		require.ErrorIs(t, cfg.Validate(), ErrModelPathEmpty)
	})
}

func TestEffectiveLaunchMode(t *testing.T) {
	tests := []struct {
		name string
		mode LaunchMode
		want LaunchMode
	}{
		{"empty defaults to on_demand", "", LaunchModeOnDemand},
		{"manual", LaunchModeManual, LaunchModeManual},
		{"on_demand", LaunchModeOnDemand, LaunchModeOnDemand},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := AppConfig{LaunchMode: tt.mode}
			require.Equal(t, tt.want, mc.EffectiveLaunchMode())
		})
	}
}

func TestEffectiveModelIdleTimeout(t *testing.T) {
	tests := []struct {
		name    string
		timeout string
		want    time.Duration
	}{
		{"empty defaults to 2m", "", DefaultModelIdleTimeout},
		{"valid 10m", "10m", 10 * time.Minute},
		{"valid 30s", "30s", 30 * time.Second},
		{"valid 1h", "1h", time.Hour},
		{"invalid falls back to default", "notaduration", DefaultModelIdleTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{ModelIdleTimeout: tt.timeout}
			require.Equal(t, tt.want, cfg.EffectiveModelIdleTimeout())
		})
	}
}

func TestEffectiveAppIdleTimeout(t *testing.T) {
	tests := []struct {
		name    string
		timeout string
		want    time.Duration
	}{
		{"empty defaults to 5s", "", DefaultAppIdleTimeout},
		{"valid 10s", "10s", 10 * time.Second},
		{"valid 1m", "1m", time.Minute},
		{"invalid falls back to default", "notaduration", DefaultAppIdleTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{AppIdleTimeout: tt.timeout}
			require.Equal(t, tt.want, cfg.EffectiveAppIdleTimeout())
		})
	}
}

func TestLoadApps(t *testing.T) {
	tests := []struct {
		name          string
		yaml          string
		wantAutoStart bool
		wantMode      LaunchMode
	}{
		{
			"auto_start true",
			"- name: test\n  command: ./test\n  auto_start: true\n",
			true, LaunchModeOnDemand,
		},
		{
			"auto_start false",
			"- name: test\n  command: ./test\n  auto_start: false\n",
			false, LaunchModeOnDemand,
		},
		{
			"no auto_start defaults to false",
			"- name: test\n  command: ./test\n",
			false, LaunchModeOnDemand,
		},
		{
			"on_demand with auto_start",
			"- name: test\n  command: ./test\n  launch_mode: on_demand\n  auto_start: true\n",
			true, LaunchModeOnDemand,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, ConfigFile), []byte("listen_addr: ':3455'\n"), 0644))
			require.NoError(t, os.WriteFile(filepath.Join(dir, AppsFile), []byte(tt.yaml), 0644))

			cfg, _, err := Load(dir)
			require.NoError(t, err)
			require.Len(t, cfg.Apps, 1)
			require.Equal(t, tt.wantAutoStart, cfg.Apps[0].AutoStart)
			require.Equal(t, tt.wantMode, cfg.Apps[0].EffectiveLaunchMode())
		})
	}
}
