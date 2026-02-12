package config

import pkgllm "github.com/chinese-room-solutions/mass/pkg/llm"

// Re-export fingerprint functions from pkg/llm.
var (
	ChatModelFingerprint      = pkgllm.ChatModelFingerprint
	EmbeddingModelFingerprint = pkgllm.EmbeddingModelFingerprint
)
