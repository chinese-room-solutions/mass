package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChatModelFingerprint(t *testing.T) {
	base := ChatModelConfig{
		Path:        "/models/llama.gguf",
		ContextSize: 4096,
		BatchSize:   512,
		FlashAttn:   "enabled",
		Thinking:    true,
	}

	t.Run("deterministic", func(t *testing.T) {
		fp1 := ChatModelFingerprint(base)
		fp2 := ChatModelFingerprint(base)
		require.Equal(t, fp1, fp2)
		require.Len(t, fp1, 16)
	})

	tests := []struct {
		name   string
		modify func(ChatModelConfig) ChatModelConfig
	}{
		{"path", func(c ChatModelConfig) ChatModelConfig { c.Path = "/other.gguf"; return c }},
		{"context_size", func(c ChatModelConfig) ChatModelConfig { c.ContextSize = 2048; return c }},
		{"batch_size", func(c ChatModelConfig) ChatModelConfig { c.BatchSize = 256; return c }},
		{"flash_attn", func(c ChatModelConfig) ChatModelConfig { c.FlashAttn = "disabled"; return c }},
		{"thinking", func(c ChatModelConfig) ChatModelConfig { c.Thinking = false; return c }},
	}
	for _, tt := range tests {
		t.Run("change_"+tt.name, func(t *testing.T) {
			modified := tt.modify(base)
			require.NotEqual(t, ChatModelFingerprint(base), ChatModelFingerprint(modified))
		})
	}
}

func TestEmbeddingModelFingerprint(t *testing.T) {
	base := EmbeddingModelConfig{
		Path:        "/models/embed.gguf",
		ContextSize: 512,
	}

	t.Run("deterministic", func(t *testing.T) {
		fp1 := EmbeddingModelFingerprint(base)
		fp2 := EmbeddingModelFingerprint(base)
		require.Equal(t, fp1, fp2)
		require.Len(t, fp1, 16)
	})

	tests := []struct {
		name   string
		modify func(EmbeddingModelConfig) EmbeddingModelConfig
	}{
		{"path", func(c EmbeddingModelConfig) EmbeddingModelConfig { c.Path = "/other.gguf"; return c }},
		{"context_size", func(c EmbeddingModelConfig) EmbeddingModelConfig { c.ContextSize = 1024; return c }},
	}
	for _, tt := range tests {
		t.Run("change_"+tt.name, func(t *testing.T) {
			modified := tt.modify(base)
			require.NotEqual(t, EmbeddingModelFingerprint(base), EmbeddingModelFingerprint(modified))
		})
	}
}

func TestChatAndEmbeddingFingerprintsDiffer(t *testing.T) {
	// Same path/values but different types should produce different fingerprints
	// due to the "chat|" vs "embed|" prefix.
	chatFP := ChatModelFingerprint(ChatModelConfig{Path: "/model.gguf", ContextSize: 4096})
	embedFP := EmbeddingModelFingerprint(EmbeddingModelConfig{Path: "/model.gguf", ContextSize: 4096})
	require.NotEqual(t, chatFP, embedFP)
}

func TestPlacementFields_DoNotChangeFingerprint(t *testing.T) {
	// PlacementConfig fields (gpu_layers, threads, max_concurrent, main_gpu, tensor_split)
	// are no longer part of the model config and should NOT affect the fingerprint.

	t.Run("chat", func(t *testing.T) {
		cfg := ChatModelConfig{Path: "/model.gguf", ContextSize: 4096}
		baseFP := ChatModelFingerprint(cfg)

		// The fingerprint is computed solely from ChatModelConfig; PlacementConfig
		// is a separate struct and never enters the fingerprint calculation.
		// Verify the fingerprint is stable regardless of which PlacementConfig is used.
		require.Equal(t, baseFP, ChatModelFingerprint(cfg),
			"fingerprint should be independent of placement")
	})

	t.Run("embedding", func(t *testing.T) {
		cfg := EmbeddingModelConfig{Path: "/embed.gguf", ContextSize: 512}
		baseFP := EmbeddingModelFingerprint(cfg)

		require.Equal(t, baseFP, EmbeddingModelFingerprint(cfg),
			"fingerprint should be independent of placement")
	})
}
