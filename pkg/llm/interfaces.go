// Package llm defines the public interfaces and types for model inference.
// These types are shared between MASS and external consumers (e.g., mass-agent).
// Implementations live in internal/llm.
package llm

import (
	"context"

	"github.com/rs/zerolog"
)

// ChatModelInterface represents a loaded chat model with lifecycle management.
// Implementations provide inference via PredictorInterface and cleanup via Close.
type ChatModelInterface interface {
	Pool() PredictorInterface
	Close()
}

// EmbeddingModelInterface represents a loaded embedding model with lifecycle management.
// Implementations provide inference via EmbedderInterface and cleanup via Close.
type EmbeddingModelInterface interface {
	Pool() EmbedderInterface
	Close()
}

// ModelLoaderInterface abstracts the inference runtime (e.g., llama.cpp, ONNX, vLLM).
// The default implementation loads GGUF models via llama.cpp.
type ModelLoaderInterface interface {
	// LoadChatModel loads a chat/completion model. Identity config determines what
	// model to load; placement config determines how it is placed on devices.
	LoadChatModel(logger zerolog.Logger, name string, cfg ChatModelConfig, placement PlacementConfig) (ChatModelInterface, error)

	// LoadEmbeddingModel loads an embedding model.
	LoadEmbeddingModel(logger zerolog.Logger, name string, cfg EmbeddingModelConfig, placement PlacementConfig) (EmbeddingModelInterface, error)
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
