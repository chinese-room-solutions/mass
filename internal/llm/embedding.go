package llm

import (
	"context"
	"fmt"

	"github.com/KernelPryanic/ctxerr"
	"github.com/rs/zerolog"
	llama "github.com/tcpipuk/llama-go"
)

// EmbeddingModel wraps a tcpipuk/llama-go model for embedding extraction.
type EmbeddingModel struct {
	name    string
	model   *llama.Model
	pool    *EmbeddingPool
	threads int
	ctxSize int
	logger  zerolog.Logger
}

// NewEmbeddingModel loads a GGUF model and creates a worker pool for embeddings.
func NewEmbeddingModel(logger zerolog.Logger, name string, cfg EmbeddingModelConfig, placement PlacementConfig) (*EmbeddingModel, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	threads := int(placement.Threads)
	if threads <= 0 {
		threads = effectiveCPUs()
	}
	contextSize := int(cfg.ContextSize)
	if contextSize <= 0 {
		contextSize = 2048
	}
	maxConcurrent := int(placement.MaxConcurrent)

	errCtx := map[string]any{"name": name, "path": cfg.Path}

	initLlamaLogging(logger)

	// gpu_layers convention:
	//   0 or unset = auto (all layers on GPU, llama.cpp -1)
	//  -1          = CPU only (no GPU offload, llama.cpp 0)
	//   N > 0      = offload exactly N layers to GPU
	gpuLayers := int(placement.GpuLayers)
	switch gpuLayers {
	case 0:
		gpuLayers = -1 // auto: offload all layers to GPU
	case -1:
		gpuLayers = 0 // CPU only
	}

	modelOpts := []llama.ModelOption{
		llama.WithGPULayers(gpuLayers),
		llama.WithMMap(true),
	}
	if placement.MainGPU != "" {
		modelOpts = append(modelOpts, llama.WithMainGPU(placement.MainGPU))
	}
	if placement.TensorSplit != "" {
		modelOpts = append(modelOpts, llama.WithTensorSplit(placement.TensorSplit))
	}

	lm, err := llama.LoadModel(cfg.Path, modelOpts...)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("loading embedding model: %w", err), errCtx)
	}

	m := &EmbeddingModel{
		name:    name,
		model:   lm,
		threads: threads,
		ctxSize: contextSize,
		logger:  logger.With().Str("embedding_model", name).Logger(),
	}
	m.pool = newEmbeddingPool(maxConcurrent, m)

	m.logger.Info().
		Str("path", cfg.Path).
		Int("context_size", contextSize).
		Int("threads", threads).
		Int("max_concurrent", maxConcurrent).
		Int("gpu_layers", gpuLayers).
		Msg("embedding model loaded")

	return m, nil
}

// Pool returns the model's worker pool which implements EmbedderInterface.
func (m *EmbeddingModel) Pool() EmbedderInterface {
	return m.pool
}

// embed creates a new context with embeddings enabled and returns the embedding vector.
func (m *EmbeddingModel) embed(_ context.Context, text string) ([]float32, error) {
	ctxOpts := []llama.ContextOption{
		llama.WithThreads(m.threads),
		llama.WithContext(m.ctxSize),
		llama.WithEmbeddings(),
	}

	lctx, err := m.model.NewContext(ctxOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating embedding context: %w", err)
	}
	defer lctx.Close() //nolint:errcheck // best-effort cleanup

	embedding, err := lctx.GetEmbeddings(text)
	if err != nil {
		return nil, fmt.Errorf("getting embeddings: %w", err)
	}

	return embedding, nil
}

// embedBatch creates a new context with embeddings enabled and returns embedding vectors for all inputs.
func (m *EmbeddingModel) embedBatch(_ context.Context, texts []string) ([][]float32, error) {
	ctxOpts := []llama.ContextOption{
		llama.WithThreads(m.threads),
		llama.WithContext(m.ctxSize),
		llama.WithEmbeddings(),
	}

	lctx, err := m.model.NewContext(ctxOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating embedding context: %w", err)
	}
	defer lctx.Close() //nolint:errcheck // best-effort cleanup

	embeddings, err := lctx.GetEmbeddingsBatch(texts)
	if err != nil {
		return nil, fmt.Errorf("getting embeddings batch: %w", err)
	}

	return embeddings, nil
}

// Close drains the pool and frees the underlying model.
func (m *EmbeddingModel) Close() {
	m.pool.close()
	if err := m.model.Close(); err != nil {
		m.logger.Warn().Err(err).Msg("error closing embedding model")
	}
	m.logger.Info().Msg("embedding model closed")
}
