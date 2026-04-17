package llm

import (
	"crypto/sha256"
	"errors"
	"fmt"
)

// ErrModelPathEmpty is returned when a model config has an empty path.
var ErrModelPathEmpty = errors.New("model path is empty")

// DefaultContextSize is the value substituted for ContextSize=0 ("auto") at
// the configuration boundary. Picked to match the worker's internal default
// so the worker, the scheduler heuristic, and the loaded-model UI panel all
// agree on the same number.
const DefaultContextSize int32 = 4096

// ModelConfigInterface is the common contract for runtime-specific configs
// (LlamaChatConfig, etc.). Implementations must be value types so
// fingerprinting and pool lookups stay copy-safe.
type ModelConfigInterface interface {
	// Runtime returns the inference runtime name, e.g. "llama" or "ort".
	Runtime() string
	// Kind returns the model kind this config is valid for.
	Kind() ModelKind
	// Fingerprint returns a 16-char hex SHA-256 over the identity fields.
	// Two configs with the same fingerprint share a loaded instance.
	Fingerprint() string
	// Validate checks that the config has all required fields.
	Validate() error
}

// ChatModelConfigInterface marks configs valid for chat inference.
// The unexported isChatConfig method is the compile-time discriminator.
type ChatModelConfigInterface interface {
	ModelConfigInterface
	isChatConfig()
}

// EmbeddingModelConfigInterface marks configs valid for embedding extraction.
type EmbeddingModelConfigInterface interface {
	ModelConfigInterface
	isEmbeddingConfig()
}

// PlacementConfig describes how a model is placed on compute devices.
// These fields are decided by the scheduler and do NOT affect the fingerprint.
// Users may override them; if left at zero values the scheduler auto-calculates.
type PlacementConfig struct {
	GpuLayers     int32  `yaml:"gpu_layers"`
	MainGPU       string `yaml:"main_gpu"`
	TensorSplit   string `yaml:"tensor_split"`
	Threads       int32  `yaml:"threads"`
	MaxConcurrent int32  `yaml:"max_concurrent"`
}

// --- llama runtime ---

// LlamaChatConfig describes the identity configuration for a llama.cpp chat
// model. These fields determine the model's fingerprint — two configs with
// identical identity fields share a loaded instance regardless of placement.
type LlamaChatConfig struct {
	Path         string `yaml:"path"`
	ContextSize  int32  `yaml:"context_size"`
	BatchSize    int32  `yaml:"batch_size"`
	MaxTokens    int32  `yaml:"max_tokens"`
	FlashAttn    string `yaml:"flash_attn"`
	Thinking     bool   `yaml:"thinking"`
	MmprojPath   string `yaml:"mmproj_path"`
	ChatTemplate string `yaml:"chat_template"`
	CacheType    string `yaml:"cache_type"`
}

func (c LlamaChatConfig) Runtime() string { return "llama" }
func (c LlamaChatConfig) Kind() ModelKind { return ModelKindChat }
func (LlamaChatConfig) isChatConfig()     {}

func (c LlamaChatConfig) Validate() error {
	if c.Path == "" {
		return ErrModelPathEmpty
	}
	return nil
}

// WithDefaults returns a copy with sentinel zeroes resolved to their
// concrete defaults. Callers must invoke this at every config entry
// boundary (UI handlers, RPC unpackers, YAML loaders) before fingerprinting
// or dispatch — guarantees the scheduler heuristic, the worker, and any
// fingerprint-keyed cache all see the same effective values.
func (c LlamaChatConfig) WithDefaults() LlamaChatConfig {
	if c.ContextSize == 0 {
		c.ContextSize = DefaultContextSize
	}
	return c
}

func (c LlamaChatConfig) Fingerprint() string {
	c = c.WithDefaults()
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "chat|llama|%s|%d|%d|%s|%t|%s|%s|%s",
		c.Path,
		c.ContextSize,
		c.BatchSize,
		c.FlashAttn,
		c.Thinking,
		c.MmprojPath,
		c.ChatTemplate,
		c.CacheType,
	)
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}

// LlamaEmbeddingConfig describes the identity configuration for a llama.cpp
// embedding model.
type LlamaEmbeddingConfig struct {
	Path        string `yaml:"path"`
	ContextSize int32  `yaml:"context_size"`
}

func (c LlamaEmbeddingConfig) Runtime() string  { return "llama" }
func (c LlamaEmbeddingConfig) Kind() ModelKind  { return ModelKindEmbedding }
func (LlamaEmbeddingConfig) isEmbeddingConfig() {}

func (c LlamaEmbeddingConfig) Validate() error {
	if c.Path == "" {
		return ErrModelPathEmpty
	}
	return nil
}

// WithDefaults — see LlamaChatConfig.WithDefaults.
func (c LlamaEmbeddingConfig) WithDefaults() LlamaEmbeddingConfig {
	if c.ContextSize == 0 {
		c.ContextSize = DefaultContextSize
	}
	return c
}

func (c LlamaEmbeddingConfig) Fingerprint() string {
	c = c.WithDefaults()
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "embed|llama|%s|%d",
		c.Path,
		c.ContextSize,
	)
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}

// Compile-time interface checks.
var (
	_ ChatModelConfigInterface      = LlamaChatConfig{}
	_ EmbeddingModelConfigInterface = LlamaEmbeddingConfig{}
)
