package llm

import (
	pkgllm "github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/rs/zerolog"
)

// Re-export public interfaces and config types from pkg/llm so that existing
// internal code keeps compiling without import changes.
type ChatModelInterface = pkgllm.ChatModelInterface
type EmbeddingModelInterface = pkgllm.EmbeddingModelInterface
type ModelLoaderInterface = pkgllm.ModelLoaderInterface
type PredictorInterface = pkgllm.PredictorInterface
type EmbedderInterface = pkgllm.EmbedderInterface
type ChatModelConfig = pkgllm.ChatModelConfig
type EmbeddingModelConfig = pkgllm.EmbeddingModelConfig
type PlacementConfig = pkgllm.PlacementConfig
type CompletionRequest = pkgllm.CompletionRequest
type CompletionResult = pkgllm.CompletionResult
type CompletionDelta = pkgllm.CompletionDelta
type CompletionUsage = pkgllm.CompletionUsage
type ChatMessage = pkgllm.ChatMessage
type ContentPart = pkgllm.ContentPart
type ContentType = pkgllm.ContentType
type EmbeddingResult = pkgllm.EmbeddingResult
type BatchEmbeddingResult = pkgllm.BatchEmbeddingResult

// Re-export content type constants.
const (
	ContentText  = pkgllm.ContentText
	ContentImage = pkgllm.ContentImage
	ContentAudio = pkgllm.ContentAudio
	ContentFile  = pkgllm.ContentFile
)

// Compile-time check: LlamaLoader implements ModelLoaderInterface.
var _ ModelLoaderInterface = (*LlamaLoader)(nil)

// LlamaLoader loads GGUF models via llama.cpp (llama-go).
type LlamaLoader struct{}

// NewLlamaLoader creates a new LlamaLoader.
func NewLlamaLoader() *LlamaLoader {
	return &LlamaLoader{}
}

func (l *LlamaLoader) LoadChatModel(logger zerolog.Logger, name string, cfg ChatModelConfig, placement PlacementConfig) (ChatModelInterface, error) {
	return NewModel(logger, name, cfg, placement)
}

func (l *LlamaLoader) LoadEmbeddingModel(logger zerolog.Logger, name string, cfg EmbeddingModelConfig, placement PlacementConfig) (EmbeddingModelInterface, error) {
	return NewEmbeddingModel(logger, name, cfg, placement)
}
