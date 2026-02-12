package llm

import (
	"crypto/sha256"
	"fmt"
)

// ChatModelFingerprint computes a deterministic fingerprint for a ChatModelConfig.
// Two configs with the same fingerprint are functionally identical and can share
// a loaded model instance. Placement fields (gpu_layers, threads, etc.) are
// excluded — they affect how/where the model runs, not what it is.
// Returns a 16-char hex string.
func ChatModelFingerprint(cfg ChatModelConfig) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "chat|%s|%d|%d|%s|%t|%s|%s|%s",
		cfg.Path,
		cfg.ContextSize,
		cfg.BatchSize,
		cfg.FlashAttn,
		cfg.Thinking,
		cfg.MmprojPath,
		cfg.ChatTemplate,
		cfg.CacheType,
	)
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}

// EmbeddingModelFingerprint computes a deterministic fingerprint for an EmbeddingModelConfig.
func EmbeddingModelFingerprint(cfg EmbeddingModelConfig) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "embed|%s|%d",
		cfg.Path,
		cfg.ContextSize,
	)
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}
