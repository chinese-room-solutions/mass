package llm

import (
	"context"
	"fmt"

	llama "github.com/tcpipuk/llama-go"
)

// Types CompletionRequest, CompletionResult, CompletionDelta, CompletionUsage,
// ChatMessage, ContentPart, ContentType, PredictorInterface, EmbedderInterface,
// EmbeddingResult, BatchEmbeddingResult and content constants are defined in
// pkg/llm and re-exported via runtime.go.

type worker struct {
	ctx    *llama.Context
	vision *llama.VisionContext // nil for text-only models
}

// Pool holds pre-created persistent contexts and distributes them to callers.
type Pool struct {
	workers []*worker
	acquire chan *worker
	model   *Model
}

func newPool(size int, model *Model, ctxOpts []llama.ContextOption) (*Pool, error) {
	if size <= 0 {
		size = 1
	}
	p := &Pool{
		workers: make([]*worker, size),
		acquire: make(chan *worker, size),
		model:   model,
	}
	for i := range size {
		lctx, err := model.model.NewContext(ctxOpts...)
		if err != nil {
			// close already-created contexts before returning
			for j := 0; j < i; j++ {
				if p.workers[j].vision != nil {
					_ = p.workers[j].vision.Close()
				}
				_ = p.workers[j].ctx.Close()
			}
			return nil, fmt.Errorf("creating context %d: %w", i, err)
		}
		w := &worker{ctx: lctx}

		// Create per-worker VisionContext when mmproj is configured.
		// VisionContext is NOT thread-safe, so each worker needs its own.
		if model.mmprojPath != "" {
			vision, vErr := model.model.NewVisionContext(model.mmprojPath,
				llama.WithVisionFlashAttn(model.flashAttn))
			if vErr != nil {
				_ = lctx.Close()
				for j := 0; j < i; j++ {
					if p.workers[j].vision != nil {
						_ = p.workers[j].vision.Close()
					}
					_ = p.workers[j].ctx.Close()
				}
				return nil, fmt.Errorf("creating vision context %d: %w", i, vErr)
			}
			w.vision = vision
		}

		p.workers[i] = w
		p.acquire <- w
	}
	return p, nil
}

// Submit acquires a worker context and runs inference, then returns the context to the pool.
func (p *Pool) Submit(ctx context.Context, req CompletionRequest) CompletionResult {
	var w *worker
	select {
	case w = <-p.acquire:
	case <-ctx.Done():
		return CompletionResult{Error: ctx.Err()}
	}
	defer func() { p.acquire <- w }()

	result, err := p.model.predict(ctx, w.ctx, w.vision, req)
	result.Error = err
	return result
}

// SubmitStream acquires a worker context and runs streaming inference.
// Returns a channel of deltas and a channel for the final error.
// The delta channel is closed when generation is complete.
func (p *Pool) SubmitStream(ctx context.Context, req CompletionRequest) (<-chan CompletionDelta, <-chan error) {
	deltaCh := make(chan CompletionDelta)
	errCh := make(chan error, 1)

	go func() {
		defer close(deltaCh)

		var w *worker
		select {
		case w = <-p.acquire:
		case <-ctx.Done():
			errCh <- ctx.Err()
			return
		}
		defer func() { p.acquire <- w }()

		streamErr := p.model.predictStream(ctx, w.ctx, w.vision, req, deltaCh)
		if streamErr != nil {
			errCh <- streamErr
		}
	}()

	return deltaCh, errCh
}

// Tokenize converts text to token IDs. Acquires a worker context for the operation.
func (p *Pool) Tokenize(ctx context.Context, text string) ([]int32, error) {
	var w *worker
	select {
	case w = <-p.acquire:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { p.acquire <- w }()

	return w.ctx.Tokenize(text)
}

// Name returns the model name.
func (p *Pool) Name() string {
	return p.model.name
}

// close drains all workers and closes their contexts (including vision).
func (p *Pool) close() {
	for range len(p.workers) {
		w := <-p.acquire
		if w.vision != nil {
			_ = w.vision.Close()
		}
		_ = w.ctx.Close()
	}
}

// EmbeddingPool limits concurrent access to an EmbeddingModel using a semaphore.
type EmbeddingPool struct {
	sem   chan struct{}
	model *EmbeddingModel
}

func newEmbeddingPool(size int, model *EmbeddingModel) *EmbeddingPool {
	if size <= 0 {
		size = 1
	}
	return &EmbeddingPool{
		sem:   make(chan struct{}, size),
		model: model,
	}
}

// Embed sends a single embedding request through the pool's semaphore.
func (p *EmbeddingPool) Embed(ctx context.Context, text string) EmbeddingResult {
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-ctx.Done():
		return EmbeddingResult{Error: ctx.Err()}
	}

	embedding, err := p.model.embed(ctx, text)
	return EmbeddingResult{Embedding: embedding, Error: err}
}

// EmbedBatch sends a batch embedding request through the pool's semaphore.
func (p *EmbeddingPool) EmbedBatch(ctx context.Context, texts []string) BatchEmbeddingResult {
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-ctx.Done():
		return BatchEmbeddingResult{Error: ctx.Err()}
	}

	embeddings, err := p.model.embedBatch(ctx, texts)
	return BatchEmbeddingResult{Embeddings: embeddings, Error: err}
}

// Name returns the model name.
func (p *EmbeddingPool) Name() string {
	return p.model.name
}

// close waits for all in-flight requests to finish.
func (p *EmbeddingPool) close() {
	for i := 0; i < cap(p.sem); i++ {
		p.sem <- struct{}{}
	}
}
