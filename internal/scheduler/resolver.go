package scheduler

import (
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"github.com/KernelPryanic/ctxerr"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/server"
	"github.com/chinese-room-solutions/mass/pkg/llm"
)

// Compile-time check: modelResolver implements ModelResolverInterface.
var _ server.ModelResolverInterface = (*modelResolver)(nil)

// loadModelFunc loads a model through the full scheduler pipeline (device
// selection, placement computation, agent dispatch). It mirrors the LoadModel
// path so that on-demand loads are treated identically to manual ones.
type loadModelFunc func(cfg config.ModelConfigInterface, userPlacement config.PlacementConfig, source string, mode InstanceMode) (string, error)

// modelResolver resolves requests to loaded model instances via the
// model pool. Supports both new model_config-based and legacy name-based lookups.
// It tracks fingerprints acquired via GetOrLoad* so they can be released after
// the request completes.
type modelResolver struct {
	pool      *modelPool
	source    string        // who is making the request: "direct", "app:<name>"
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
	return r.dispatchChat(req.ModelConfig)
}

func (r *modelResolver) ResolveEmbedding(req *rpc.EmbeddingRequest) (llm.EmbedderInterface, string, error) {
	return r.dispatchEmbedding(req.ModelConfig)
}

func (r *modelResolver) ResolveBatchEmbedding(req *rpc.BatchEmbeddingRequest) (llm.EmbedderInterface, string, error) {
	return r.dispatchEmbedding(req.ModelConfig)
}

// ResolveTokenize routes via the chat auto-load path when model_config is set;
// otherwise it falls back to a loaded-only lookup against the legacy `model`
// string for callers that pre-date the model_config field.
func (r *modelResolver) ResolveTokenize(req *rpc.TokenizeRequest) (llm.PredictorInterface, string, error) {
	if req.ModelConfig != nil {
		return r.dispatchChat(req.ModelConfig)
	}
	if req.Model == "" {
		return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model_config or model is required"))
	}
	resolved := req.Model
	if abs, err := config.ResolveModelPath(req.Model, r.modelsDir); err == nil {
		resolved = abs
	}
	for _, inst := range r.pool.LoadedSnapshot() {
		if inst.ChatConfig != nil && inst.ChatConfig.Path == resolved {
			if p, fp, ok := r.pool.AcquireChat(inst.Fingerprint); ok {
				r.acquired = append(r.acquired, fp)
				return p, fp, nil
			}
		}
	}
	return nil, "", connect.NewError(connect.CodeFailedPrecondition,
		fmt.Errorf("model %q is not loaded; pass model_config to auto-load, or call LoadModel first", req.Model))
}

// dispatchChat is the shared resolver path for any chat-kind request
// (ChatCompletion, Tokenize). model_config is required — there is no
// name-only shortcut. Apps that want to address a loaded instance should
// call ListLoadedModels and echo the returned config verbatim.
func (r *modelResolver) dispatchChat(mc *rpc.ChatModelConfig) (llm.PredictorInterface, string, error) {
	if mc == nil {
		return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model_config is required"))
	}
	cfg, placement, err := ChatConfigFromProto(mc, r.modelsDir)
	if err != nil {
		return nil, "", connect.NewError(connect.CodeInvalidArgument, err)
	}
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

// dispatchEmbedding is the shared resolver path for any embedding-kind
// request (Embedding, BatchEmbedding). model_config is required.
func (r *modelResolver) dispatchEmbedding(mc *rpc.EmbeddingModelConfig) (llm.EmbedderInterface, string, error) {
	if mc == nil {
		return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("model_config is required"))
	}
	cfg, placement, err := EmbeddingConfigFromProto(mc, r.modelsDir)
	if err != nil {
		return nil, "", connect.NewError(connect.CodeInvalidArgument, err)
	}
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

// getOrLoadChat returns a chat predictor, using the full scheduler pipeline if
// available. If the model is already in the pool it is reused directly;
// otherwise loadModel is called (same path as the UI "Load Model" button).
func (r *modelResolver) getOrLoadChat(cfg config.LlamaChatConfig, placement config.PlacementConfig) (llm.PredictorInterface, string, error) {
	fp := cfg.Fingerprint()

	// Fast path: model already loaded — reuse it.
	if p, fp, ok := r.pool.AcquireChat(fp); ok {
		return p, fp, nil
	}

	// Full scheduler load path (device selection, placement, agent dispatch).
	// Inference autoload → ModeDynamic so the idle sweep can reclaim the slot.
	if r.loadModel != nil {
		fp, err := r.loadModel(cfg, placement, r.source, ModeDynamic)
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
func (r *modelResolver) getOrLoadEmbedding(cfg config.LlamaEmbeddingConfig, placement config.PlacementConfig) (llm.EmbedderInterface, string, error) {
	fp := cfg.Fingerprint()

	// Fast path: model already loaded — reuse it.
	if e, fp, ok := r.pool.AcquireEmbedding(fp); ok {
		return e, fp, nil
	}

	// Full scheduler load path. Inference autoload → ModeDynamic.
	if r.loadModel != nil {
		fp, err := r.loadModel(cfg, placement, r.source, ModeDynamic)
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

// ChatConfigFromProto unpacks the proto ChatModelConfig envelope into the
// scheduler's identity + placement pair. Exported because the web layer
// also needs to materialize configs from proto requests (LoadModel RPC).
// Returns an error if the oneof variant is not recognized.
func ChatConfigFromProto(mc *rpc.ChatModelConfig, modelsDir string) (config.LlamaChatConfig, config.PlacementConfig, error) {
	if lc := mc.GetLlama(); lc != nil {
		cfg, placement := llamaChatToChatConfig(lc, modelsDir)
		return cfg, placement, nil
	}
	return config.LlamaChatConfig{}, config.PlacementConfig{}, fmt.Errorf("model_config: unsupported runtime")
}

// EmbeddingConfigFromProto is the embedding counterpart to ChatConfigFromProto.
func EmbeddingConfigFromProto(mc *rpc.EmbeddingModelConfig, modelsDir string) (config.LlamaEmbeddingConfig, config.PlacementConfig, error) {
	if lc := mc.GetLlama(); lc != nil {
		cfg, placement := llamaEmbeddingToEmbeddingConfig(lc, modelsDir)
		return cfg, placement, nil
	}
	return config.LlamaEmbeddingConfig{}, config.PlacementConfig{}, fmt.Errorf("model_config: unsupported runtime")
}

// llamaChatToChatConfig converts a proto LlamaChatConfig to identity and placement configs.
func llamaChatToChatConfig(lc *rpc.LlamaChatConfig, modelsDir string) (config.LlamaChatConfig, config.PlacementConfig) {
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
	cfg := config.LlamaChatConfig{
		Path:         path,
		ContextSize:  lc.GetContextSize(),
		BatchSize:    lc.GetBatchSize(),
		FlashAttn:    flashAttnProtoToString(lc),
		Thinking:     lc.Thinking,
		MmprojPath:   mmprojPath,
		ChatTemplate: lc.ChatTemplate,
		CacheType:    cacheTypeProtoToString(lc.CacheType),
	}.WithDefaults()
	return cfg, llamaPlacement(lc.GetGpuLayers(), lc.GetThreads(), lc.GetMaxConcurrent(), lc.MainGpu, lc.TensorSplit)
}

// llamaEmbeddingToEmbeddingConfig converts a proto LlamaEmbeddingConfig to identity and placement configs.
func llamaEmbeddingToEmbeddingConfig(lc *rpc.LlamaEmbeddingConfig, modelsDir string) (config.LlamaEmbeddingConfig, config.PlacementConfig) {
	path := lc.Model
	if resolved, err := config.ResolveModelPath(path, modelsDir); err == nil {
		path = resolved
	}
	cfg := config.LlamaEmbeddingConfig{
		Path:        path,
		ContextSize: lc.GetContextSize(),
	}.WithDefaults()
	return cfg, llamaPlacement(lc.GetGpuLayers(), lc.GetThreads(), lc.GetMaxConcurrent(), lc.MainGpu, lc.TensorSplit)
}

// llamaPlacement constructs a PlacementConfig from the common placement fields
// shared by LlamaChatConfig and LlamaEmbeddingConfig proto messages. tensorSplit
// is serialized to the canonical CSV string used by the internal config so
// fingerprints stay stable across the wire→internal boundary.
func llamaPlacement(gpuLayers, threads, maxConcurrent int32, mainGPU string, tensorSplit []float32) config.PlacementConfig {
	return config.PlacementConfig{
		GpuLayers:     gpuLayers,
		Threads:       threads,
		MaxConcurrent: maxConcurrent,
		MainGPU:       mainGPU,
		TensorSplit:   tensorSplitToString(tensorSplit),
	}
}

// flashAttnProtoToString maps the optional bool to the internal string form
// ("enabled"/"disabled"/""). Unset = "" (auto).
func flashAttnProtoToString(lc *rpc.LlamaChatConfig) string {
	if lc.FlashAttn == nil {
		return ""
	}
	if *lc.FlashAttn {
		return "enabled"
	}
	return "disabled"
}

// cacheTypeProtoToString maps the CacheType enum to the internal string form
// used for fingerprinting and llama.cpp config.
func cacheTypeProtoToString(t rpc.CacheType) string {
	switch t {
	case rpc.CacheType_CACHE_TYPE_F16:
		return "f16"
	case rpc.CacheType_CACHE_TYPE_Q8_0:
		return "q8_0"
	case rpc.CacheType_CACHE_TYPE_Q4_0:
		return "q4_0"
	default:
		return ""
	}
}

// tensorSplitToString serializes a tensor-split ratio slice into the
// "x,y,z" CSV form the internal config and llama.cpp use.
func tensorSplitToString(s []float32) string {
	if len(s) == 0 {
		return ""
	}
	parts := make([]string, len(s))
	for i, v := range s {
		parts[i] = fmt.Sprintf("%g", v)
	}
	return strings.Join(parts, ",")
}
