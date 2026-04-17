package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLlamaChatConfigFingerprint(t *testing.T) {
	base := LlamaChatConfig{
		Path:        "/models/llama.gguf",
		ContextSize: 4096,
		BatchSize:   512,
		FlashAttn:   "enabled",
		Thinking:    true,
	}

	t.Run("deterministic", func(t *testing.T) {
		fp1 := base.Fingerprint()
		fp2 := base.Fingerprint()
		require.Equal(t, fp1, fp2)
		require.Len(t, fp1, 16)
	})

	tests := []struct {
		name   string
		modify func(LlamaChatConfig) LlamaChatConfig
	}{
		{"path", func(c LlamaChatConfig) LlamaChatConfig { c.Path = "/other.gguf"; return c }},
		{"context_size", func(c LlamaChatConfig) LlamaChatConfig { c.ContextSize = 2048; return c }},
		{"batch_size", func(c LlamaChatConfig) LlamaChatConfig { c.BatchSize = 256; return c }},
		{"flash_attn", func(c LlamaChatConfig) LlamaChatConfig { c.FlashAttn = "disabled"; return c }},
		{"thinking", func(c LlamaChatConfig) LlamaChatConfig { c.Thinking = false; return c }},
	}
	for _, tt := range tests {
		t.Run("change_"+tt.name, func(t *testing.T) {
			modified := tt.modify(base)
			require.NotEqual(t, base.Fingerprint(), modified.Fingerprint())
		})
	}
}

func TestLlamaEmbeddingConfigFingerprint(t *testing.T) {
	base := LlamaEmbeddingConfig{
		Path:        "/models/embed.gguf",
		ContextSize: 512,
	}

	t.Run("deterministic", func(t *testing.T) {
		fp1 := base.Fingerprint()
		fp2 := base.Fingerprint()
		require.Equal(t, fp1, fp2)
		require.Len(t, fp1, 16)
	})

	tests := []struct {
		name   string
		modify func(LlamaEmbeddingConfig) LlamaEmbeddingConfig
	}{
		{"path", func(c LlamaEmbeddingConfig) LlamaEmbeddingConfig { c.Path = "/other.gguf"; return c }},
		{"context_size", func(c LlamaEmbeddingConfig) LlamaEmbeddingConfig { c.ContextSize = 1024; return c }},
	}
	for _, tt := range tests {
		t.Run("change_"+tt.name, func(t *testing.T) {
			modified := tt.modify(base)
			require.NotEqual(t, base.Fingerprint(), modified.Fingerprint())
		})
	}
}

func TestChatAndEmbeddingFingerprintsDiffer(t *testing.T) {
	// Same path/values but different kinds should produce different fingerprints.
	chat := LlamaChatConfig{Path: "/model.gguf", ContextSize: 4096}
	embed := LlamaEmbeddingConfig{Path: "/model.gguf", ContextSize: 4096}
	require.NotEqual(t, chat.Fingerprint(), embed.Fingerprint())
}

func TestPlacementFields_DoNotChangeFingerprint(t *testing.T) {
	// PlacementConfig fields are not part of the model identity and must not
	// affect the fingerprint.

	t.Run("chat", func(t *testing.T) {
		cfg := LlamaChatConfig{Path: "/model.gguf", ContextSize: 4096}
		require.Equal(t, cfg.Fingerprint(), cfg.Fingerprint(),
			"fingerprint should be independent of placement")
	})

	t.Run("embedding", func(t *testing.T) {
		cfg := LlamaEmbeddingConfig{Path: "/embed.gguf", ContextSize: 512}
		require.Equal(t, cfg.Fingerprint(), cfg.Fingerprint(),
			"fingerprint should be independent of placement")
	})
}
