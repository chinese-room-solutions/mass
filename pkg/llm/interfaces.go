// Package llm defines the public interfaces and types for model inference.
// These types are shared between MASS and external consumers (e.g., mass-worker).
// Runtime implementations live in the worker that uses them (e.g.
// mass-worker-llama's internal/llama package for llama.cpp).
package llm

import (
	"context"

	"github.com/rs/zerolog"
)

// ChatModelInterface represents a loaded chat model with lifecycle management.
// Implementations provide inference via PredictorInterface and cleanup via Close.
type ChatModelInterface interface {
	Pool() PredictorInterface
	// PoolSize is the number of concurrent inference slots the worker
	// actually allocated. May be smaller than the requested max_concurrent
	// when the worker capped the pool because VRAM ran out mid-init.
	// Returns 0 when the loader didn't report a value (treat as unknown).
	PoolSize() int32
	Close()
}

// EmbeddingModelInterface represents a loaded embedding model with lifecycle management.
// Implementations provide inference via EmbedderInterface and cleanup via Close.
type EmbeddingModelInterface interface {
	Pool() EmbedderInterface
	PoolSize() int32
	Close()
}

// ModelLoaderInterface abstracts the inference runtime (e.g. llama.cpp, vLLM,
// ONNX). Each runtime implements the verbs it supports; the generic config
// interfaces let the scheduler dispatch by kind without naming a specific
// runtime.
type ModelLoaderInterface interface {
	// LoadChatModel loads a chat/completion model. Identity config determines
	// what model to load; placement config determines how it is placed on devices.
	LoadChatModel(logger zerolog.Logger, name string, cfg ChatModelConfigInterface, placement PlacementConfig) (ChatModelInterface, error)

	// LoadEmbeddingModel loads an embedding model.
	LoadEmbeddingModel(logger zerolog.Logger, name string, cfg EmbeddingModelConfigInterface, placement PlacementConfig) (EmbeddingModelInterface, error)
}

// PredictorInterface submits completion requests through a concurrency-limited pool.
type PredictorInterface interface {
	Submit(ctx context.Context, req CompletionRequest) CompletionResult
	SubmitStream(ctx context.Context, req CompletionRequest) (<-chan CompletionDelta, <-chan error)
	Tokenize(ctx context.Context, text string) ([]int32, error)
	Name() string
}

// EmbedderInterface submits embedding requests through a concurrency-limited pool.
type EmbedderInterface interface {
	Embed(ctx context.Context, text string) EmbeddingResult
	EmbedBatch(ctx context.Context, texts []string) BatchEmbeddingResult
	Name() string
}
