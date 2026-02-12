package scheduler

import (
	"fmt"

	"connectrpc.com/connect"
	"github.com/KernelPryanic/ctxerr"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/llm"
	"github.com/chinese-room-solutions/mass/internal/server"
	"github.com/chinese-room-solutions/mass/rpc"
)

// Compile-time check: modelResolver implements ModelResolverInterface.
var _ server.ModelResolverInterface = (*modelResolver)(nil)

// loadModelFunc loads a model through the full scheduler pipeline (device
// selection, placement computation, agent dispatch). It mirrors the LoadModel
// path so that on-demand loads are treated identically to manual ones.
type loadModelFunc func(modelType string, chatCfg *config.ChatModelConfig, embedCfg *config.EmbeddingModelConfig, userPlacement config.PlacementConfig, source string) (string, error)

// modelResolver resolves requests to loaded model instances via the
// model pool. Supports both new model_config-based and legacy name-based lookups.
// It tracks fingerprints acquired via GetOrLoad* so they can be released after
// the request completes.
type modelResolver struct {
	pool      *modelPool
	source    string        // who is making the request: "direct", "module:<name>"
	modelsDir string        // base models directory for resolving model IDs
	loadModel loadModelFunc // full scheduler load path; nil falls back to pool-direct
	acquired  []string      // fingerprints acquired during this request
}

// ReleaseAll releases all fingerprints acquired during resolution.
func (r *modelResolver) ReleaseAll() {
	for _, fp := range r.acquired {
		r.pool.Release(fp)
	}
	r.acquired = nil
}

func (r *modelResolver) ResolveChat(req *rpc.ChatCompletionRequest) (llm.PredictorInterface, string, error) {
	if mc := req.ModelConfig; mc != nil {
		if lc := mc.GetLlama(); lc != nil {
			cfg, placement := llamaChatToChatConfig(lc, r.modelsDir)
			if err := cfg.Validate(); err != nil {
				return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model_config: %w", err))
			}
			p, fp, err := r.getOrLoadChat(cfg, placement)
			if err != nil {
				return nil, "", connect.NewError(connect.CodeInternal, err)
			}
			r.acquired = append(r.acquired, fp)
			return p, fp, nil
		}
		return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model_config: unsupported backend"))
	}

	if req.Model != "" {
		// Fast path: model already loaded and registered by name.
		if p, fp, ok := r.pool.LookupChatByName(req.Model); ok {
			return p, fp, nil
		}
		// On-demand load: resolve model path and load with defaults.
		if r.modelsDir != "" {
			if path, err := config.ResolveModelPath(req.Model, r.modelsDir); err == nil {
				cfg := config.ChatModelConfig{Path: path}
				mmprojPath := config.DetectMmproj(path)
				if mmprojPath != "" {
					cfg.MmprojPath = mmprojPath
				}
				p, fp, err := r.getOrLoadChat(cfg, config.PlacementConfig{})
				if err != nil {
					return nil, "", connect.NewError(connect.CodeInternal, fmt.Errorf("loading model %s: %w", req.Model, err))
				}
				r.acquired = append(r.acquired, fp)
				return p, fp, nil
			}
		}
		return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model: unknown model %s", req.Model))
	}

	return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model: either model or model_config is required"))
}

func (r *modelResolver) ResolveEmbedding(req *rpc.EmbeddingRequest) (llm.EmbedderInterface, string, error) {
	if mc := req.ModelConfig; mc != nil {
		if lc := mc.GetLlama(); lc != nil {
			cfg, placement := llamaEmbeddingToEmbeddingConfig(lc, r.modelsDir)
			if err := cfg.Validate(); err != nil {
				return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model_config: %w", err))
			}
			e, fp, err := r.getOrLoadEmbedding(cfg, placement)
			if err != nil {
				return nil, "", connect.NewError(connect.CodeInternal, err)
			}
			r.acquired = append(r.acquired, fp)
			return e, fp, nil
		}
		return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model_config: unsupported backend"))
	}

	if req.Model != "" {
		if e, fp, ok := r.pool.LookupEmbeddingByName(req.Model); ok {
			return e, fp, nil
		}
		if r.modelsDir != "" {
			if path, err := config.ResolveModelPath(req.Model, r.modelsDir); err == nil {
				cfg := config.EmbeddingModelConfig{Path: path}
				e, fp, err := r.getOrLoadEmbedding(cfg, config.PlacementConfig{})
				if err != nil {
					return nil, "", connect.NewError(connect.CodeInternal, fmt.Errorf("loading model %s: %w", req.Model, err))
				}
				r.acquired = append(r.acquired, fp)
				return e, fp, nil
			}
		}
		return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model: unknown model %s", req.Model))
	}

	return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model: either model or model_config is required"))
}

func (r *modelResolver) ResolveBatchEmbedding(req *rpc.BatchEmbeddingRequest) (llm.EmbedderInterface, string, error) {
	if mc := req.ModelConfig; mc != nil {
		if lc := mc.GetLlama(); lc != nil {
			cfg, placement := llamaEmbeddingToEmbeddingConfig(lc, r.modelsDir)
			if err := cfg.Validate(); err != nil {
				return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model_config: %w", err))
			}
			e, fp, err := r.getOrLoadEmbedding(cfg, placement)
			if err != nil {
				return nil, "", connect.NewError(connect.CodeInternal, err)
			}
			r.acquired = append(r.acquired, fp)
			return e, fp, nil
		}
		return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model_config: unsupported backend"))
	}

	if req.Model != "" {
		if e, fp, ok := r.pool.LookupEmbeddingByName(req.Model); ok {
			return e, fp, nil
		}
		if r.modelsDir != "" {
			if path, err := config.ResolveModelPath(req.Model, r.modelsDir); err == nil {
				cfg := config.EmbeddingModelConfig{Path: path}
				e, fp, err := r.getOrLoadEmbedding(cfg, config.PlacementConfig{})
				if err != nil {
					return nil, "", connect.NewError(connect.CodeInternal, fmt.Errorf("loading model %s: %w", req.Model, err))
				}
				r.acquired = append(r.acquired, fp)
				return e, fp, nil
			}
		}
		return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model: unknown model %s", req.Model))
	}

	return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model: either model or model_config is required"))
}

func (r *modelResolver) ResolveTokenize(req *rpc.TokenizeRequest) (llm.PredictorInterface, string, error) {
	if mc := req.ModelConfig; mc != nil {
		if lc := mc.GetLlama(); lc != nil {
			cfg, placement := llamaChatToChatConfig(lc, r.modelsDir)
			if err := cfg.Validate(); err != nil {
				return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model_config: %w", err))
			}
			p, fp, err := r.getOrLoadChat(cfg, placement)
			if err != nil {
				return nil, "", connect.NewError(connect.CodeInternal, err)
			}
			r.acquired = append(r.acquired, fp)
			return p, fp, nil
		}
		return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model_config: unsupported backend"))
	}

	if req.Model != "" {
		if p, fp, ok := r.pool.LookupChatByName(req.Model); ok {
			return p, fp, nil
		}
		if r.modelsDir != "" {
			if path, err := config.ResolveModelPath(req.Model, r.modelsDir); err == nil {
				cfg := config.ChatModelConfig{Path: path}
				mmprojPath := config.DetectMmproj(path)
				if mmprojPath != "" {
					cfg.MmprojPath = mmprojPath
				}
				p, fp, err := r.getOrLoadChat(cfg, config.PlacementConfig{})
				if err != nil {
					return nil, "", connect.NewError(connect.CodeInternal, fmt.Errorf("loading model %s: %w", req.Model, err))
				}
				r.acquired = append(r.acquired, fp)
				return p, fp, nil
			}
		}
		return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model: unknown model %s", req.Model))
	}

	return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model: either model or model_config is required"))
}

// getOrLoadChat returns a chat predictor, using the full scheduler pipeline if
// available. If the model is already in the pool it is reused directly;
// otherwise loadModel is called (same path as the UI "Load Model" button).
func (r *modelResolver) getOrLoadChat(cfg config.ChatModelConfig, placement config.PlacementConfig) (llm.PredictorInterface, string, error) {
	fp := config.ChatModelFingerprint(cfg)

	// Fast path: model already loaded — reuse it.
	if p, fp, ok := r.pool.AcquireChat(fp); ok {
		return p, fp, nil
	}

	// Full scheduler load path (device selection, placement, agent dispatch).
	if r.loadModel != nil {
		fp, err := r.loadModel("chat", &cfg, nil, placement, r.source)
		if err != nil {
			return nil, "", err
		}
		if p, fp, ok := r.pool.AcquireChat(fp); ok {
			return p, fp, nil
		}
		return nil, "", ctxerr.With(fmt.Errorf("model loaded but not found in pool"), map[string]any{"fingerprint": fp})
	}

	// Fallback: direct pool load (no scheduler, e.g. tests or queue-less mode).
	return r.pool.GetOrLoadChat(cfg, placement, r.source)
}

// getOrLoadEmbedding returns an embedder, using the full scheduler pipeline if
// available. Same logic as getOrLoadChat but for embedding models.
func (r *modelResolver) getOrLoadEmbedding(cfg config.EmbeddingModelConfig, placement config.PlacementConfig) (llm.EmbedderInterface, string, error) {
	fp := config.EmbeddingModelFingerprint(cfg)

	// Fast path: model already loaded — reuse it.
	if e, fp, ok := r.pool.AcquireEmbedding(fp); ok {
		return e, fp, nil
	}

	// Full scheduler load path.
	if r.loadModel != nil {
		fp, err := r.loadModel("embedding", nil, &cfg, placement, r.source)
		if err != nil {
			return nil, "", err
		}
		if e, fp, ok := r.pool.AcquireEmbedding(fp); ok {
			return e, fp, nil
		}
		return nil, "", ctxerr.With(fmt.Errorf("model loaded but not found in pool"), map[string]any{"fingerprint": fp})
	}

	// Fallback: direct pool load.
	return r.pool.GetOrLoadEmbedding(cfg, placement, r.source)
}

// llamaChatToChatConfig converts a proto LlamaChatConfig to identity and placement configs.
func llamaChatToChatConfig(lc *rpc.LlamaChatConfig, modelsDir string) (config.ChatModelConfig, config.PlacementConfig) {
	path := lc.Model
	if resolved, err := config.ResolveModelPath(path, modelsDir); err == nil {
		path = resolved
	}
	mmprojPath := lc.Mmproj
	if mmprojPath != "" {
		if resolved, err := config.ResolveModelPath(mmprojPath, modelsDir); err == nil {
			mmprojPath = resolved
		}
	} else {
		// Auto-detect mmproj in the model's directory.
		mmprojPath = config.DetectMmproj(path)
	}
	cfg := config.ChatModelConfig{
		Path:         path,
		ContextSize:  lc.ContextSize,
		BatchSize:    lc.BatchSize,
		FlashAttn:    lc.FlashAttn,
		Thinking:     lc.Thinking,
		MmprojPath:   mmprojPath,
		ChatTemplate: lc.ChatTemplate,
		CacheType:    lc.CacheType,
	}
	return cfg, llamaPlacement(lc.GpuLayers, lc.Threads, lc.MaxConcurrent, lc.MainGpu, lc.TensorSplit)
}

// llamaEmbeddingToEmbeddingConfig converts a proto LlamaEmbeddingConfig to identity and placement configs.
func llamaEmbeddingToEmbeddingConfig(lc *rpc.LlamaEmbeddingConfig, modelsDir string) (config.EmbeddingModelConfig, config.PlacementConfig) {
	path := lc.Model
	if resolved, err := config.ResolveModelPath(path, modelsDir); err == nil {
		path = resolved
	}
	cfg := config.EmbeddingModelConfig{
		Path:        path,
		ContextSize: lc.ContextSize,
	}
	return cfg, llamaPlacement(lc.GpuLayers, lc.Threads, lc.MaxConcurrent, lc.MainGpu, lc.TensorSplit)
}

// llamaPlacement constructs a PlacementConfig from the common placement fields
// shared by LlamaChatConfig and LlamaEmbeddingConfig proto messages.
func llamaPlacement(gpuLayers, threads, maxConcurrent int32, mainGPU, tensorSplit string) config.PlacementConfig {
	return config.PlacementConfig{
		GpuLayers:     gpuLayers,
		Threads:       threads,
		MaxConcurrent: maxConcurrent,
		MainGPU:       mainGPU,
		TensorSplit:   tensorSplit,
	}
}
