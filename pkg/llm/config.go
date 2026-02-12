package llm

import "errors"

// ErrModelPathEmpty is returned when a model config has an empty path.
var ErrModelPathEmpty = errors.New("model path is empty")

// ChatModelConfig describes the identity configuration for a chat model.
// These fields determine the model's fingerprint — two configs with identical
// identity fields share a loaded instance regardless of placement.
type ChatModelConfig struct {
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

// Validate checks that the config has all required fields.
func (c ChatModelConfig) Validate() error {
	if c.Path == "" {
		return ErrModelPathEmpty
	}
	return nil
}

// EmbeddingModelConfig describes the identity configuration for an embedding model.
// These fields determine the model's fingerprint.
type EmbeddingModelConfig struct {
	Path        string `yaml:"path"`
	ContextSize int32  `yaml:"context_size"`
}

// Validate checks that the config has all required fields.
func (c EmbeddingModelConfig) Validate() error {
	if c.Path == "" {
		return ErrModelPathEmpty
	}
	return nil
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
