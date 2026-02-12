package llm

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/KernelPryanic/ctxerr"
	"github.com/rs/zerolog"
	llama "github.com/tcpipuk/llama-go"
)

// detectGPU returns true if an NVIDIA GPU with CUDA appears to be available.
// On Linux it checks for libcuda.so in WSL2 and native driver locations.
// On Windows it checks for nvcuda.dll in the system directory.
func detectGPU() bool {
	paths := []string{
		// Linux / WSL2
		"/usr/lib/wsl/lib/libcuda.so",
		"/usr/lib/wsl/lib/libcuda.so.1",
		"/usr/lib/x86_64-linux-gnu/libcuda.so.1",
		"/usr/local/cuda/lib64/libcuda.so",
	}
	if runtime.GOOS == "windows" {
		sys32 := os.Getenv("SystemRoot") + `\System32`
		paths = append(paths, sys32+`\nvcuda.dll`)
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	return false
}

// effectiveCPUs returns the number of CPUs actually available to this process.
// In containerized environments (Docker, RunPod, k8s) the cgroup CPU quota is
// often much lower than runtime.NumCPU(), which reports the host's total cores.
func effectiveCPUs() int {
	n := runtime.NumCPU()

	// cgroup v2
	if data, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		parts := strings.Fields(string(data))
		if len(parts) == 2 && parts[0] != "max" {
			quota, err1 := strconv.Atoi(parts[0])
			period, err2 := strconv.Atoi(parts[1])
			if err1 == nil && err2 == nil && period > 0 {
				cpus := int(math.Ceil(float64(quota) / float64(period)))
				if cpus > 0 && cpus < n {
					return cpus
				}
			}
		}
	}

	// cgroup v1
	quota, errQ := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
	period, errP := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
	if errQ == nil && errP == nil {
		q, err1 := strconv.Atoi(strings.TrimSpace(string(quota)))
		p, err2 := strconv.Atoi(strings.TrimSpace(string(period)))
		if err1 == nil && err2 == nil && q > 0 && p > 0 {
			cpus := int(math.Ceil(float64(q) / float64(p)))
			if cpus > 0 && cpus < n {
				return cpus
			}
		}
	}

	return n
}

// Model wraps a tcpipuk/llama-go model with a concurrency-limited pool.
type Model struct {
	name         string
	model        *llama.Model
	pool         *Pool
	maxTokens    int
	threads      int
	ctxSize      int
	thinking     bool
	mmprojPath   string // Vision projector path (empty = text-only model)
	chatTemplate string // Override chat template (empty = use model's GGUF template)
	flashAttn    string // Flash attention mode forwarded to vision context
	logger       zerolog.Logger
}

// NewModel loads a GGUF model and creates a worker pool for it.
func NewModel(logger zerolog.Logger, name string, cfg ChatModelConfig, placement PlacementConfig) (*Model, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	gpu := detectGPU()

	threads := int(placement.Threads)
	if threads <= 0 {
		threads = effectiveCPUs()
	}
	maxTokens := int(cfg.MaxTokens)
	if maxTokens <= 0 {
		maxTokens = 1024
	}
	contextSize := int(cfg.ContextSize)
	if contextSize <= 0 {
		contextSize = 0 // 0 = use model's native maximum
	}
	batchSize := int(cfg.BatchSize)
	if batchSize <= 0 {
		// GPU can process large batches efficiently; CPU benefits from smaller batches.
		if gpu {
			batchSize = 2048
		} else {
			batchSize = 512
		}
	}
	flashAttn := cfg.FlashAttn
	if flashAttn == "" {
		// Flash attention requires GPU; disable on CPU-only builds.
		if gpu {
			flashAttn = "enabled"
		} else {
			flashAttn = "disabled"
		}
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
		llama.WithMLock(),
	}
	if placement.MainGPU != "" {
		modelOpts = append(modelOpts, llama.WithMainGPU(placement.MainGPU))
	}
	if placement.TensorSplit != "" {
		modelOpts = append(modelOpts, llama.WithTensorSplit(placement.TensorSplit))
	}

	lm, err := llama.LoadModel(cfg.Path, modelOpts...)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("loading model: %w", err), errCtx)
	}

	m := &Model{
		name:         name,
		model:        lm,
		maxTokens:    maxTokens,
		threads:      threads,
		ctxSize:      contextSize,
		thinking:     cfg.Thinking,
		mmprojPath:   cfg.MmprojPath,
		chatTemplate: cfg.ChatTemplate,
		flashAttn:    flashAttn,
		logger:       logger.With().Str("model", name).Logger(),
	}

	ctxOpts := m.buildContextOpts(batchSize, flashAttn, cfg.CacheType)
	pool, err := newPool(maxConcurrent, m, ctxOpts)
	if err != nil {
		if closeErr := lm.Close(); closeErr != nil {
			logger.Warn().Err(closeErr).Msg("closing model after pool creation failure")
		}
		return nil, ctxerr.With(fmt.Errorf("creating pool: %w", err), errCtx)
	}
	m.pool = pool

	logEvent := m.logger.Info().
		Str("path", cfg.Path).
		Int("context_size", contextSize).
		Int("batch_size", batchSize).
		Str("flash_attn", flashAttn).
		Int("threads", threads).
		Int("max_concurrent", maxConcurrent).
		Int("max_tokens", maxTokens).
		Int("gpu_layers", gpuLayers)
	if cfg.MmprojPath != "" {
		logEvent = logEvent.Str("mmproj", cfg.MmprojPath)
	}
	if cfg.ChatTemplate != "" {
		logEvent = logEvent.Str("chat_template", cfg.ChatTemplate)
	}
	if cfg.CacheType != "" {
		logEvent = logEvent.Str("cache_type", cfg.CacheType)
	}
	logEvent.Msg("model loaded")

	return m, nil
}

// buildContextOpts constructs the slice of ContextOptions used when creating contexts.
func (m *Model) buildContextOpts(batchSize int, flashAttn, cacheType string) []llama.ContextOption {
	opts := []llama.ContextOption{
		llama.WithThreads(m.threads),
		llama.WithBatch(batchSize),
		llama.WithFlashAttn(flashAttn),
	}
	if m.ctxSize != 0 {
		opts = append(opts, llama.WithContext(m.ctxSize))
	}
	// Vision models require f16 KV cache — quantized (q8_0/q4_0) KV caches
	// corrupt image embeddings that are injected directly as float vectors.
	// Quantized KV cache types (q8_0/q4_0) require flash attention (GPU-only).
	// llama-go defaults to q8_0, so we must explicitly set f16 when flash
	// attention is disabled to avoid "V cache quantization requires flash_attn".
	if m.mmprojPath != "" {
		opts = append(opts, llama.WithKVCacheType("f16"))
	} else if cacheType != "" {
		opts = append(opts, llama.WithKVCacheType(cacheType))
	} else if flashAttn == "disabled" {
		opts = append(opts, llama.WithKVCacheType("f16"))
	}
	return opts
}

// Pool returns the model's worker pool which implements PredictorInterface.
func (m *Model) Pool() PredictorInterface {
	return m.pool
}

// buildChatArgs converts a CompletionRequest into llama-go messages and options.
// Returns an error if any content part uses a type unsupported by this engine.
func (m *Model) buildChatArgs(req CompletionRequest, vision *llama.VisionContext) ([]llama.ChatMessage, llama.ChatOptions, error) {
	messages := make([]llama.ChatMessage, len(req.Messages))
	for i, msg := range req.Messages {
		lm := llama.ChatMessage{
			Role:    msg.Role,
			Content: msg.Content,
		}
		if len(msg.Parts) > 0 {
			lm.Parts = make([]llama.ContentPart, len(msg.Parts))
			for j, p := range msg.Parts {
				switch p.Type {
				case ContentText, ContentImage, ContentAudio:
					// Supported by llama.cpp/mtmd
				default:
					return nil, llama.ChatOptions{}, fmt.Errorf(
						"unsupported content type %q in message %d part %d", p.Type, i, j)
				}
				lm.Parts[j] = llama.ContentPart{
					Type: llama.ContentType(p.Type),
					Text: p.Text,
					Data: p.Data,
				}
			}
		}
		messages[i] = lm
	}

	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = m.maxTokens
	}

	thinking := m.thinking || req.EnableThinking

	opts := llama.ChatOptions{
		MaxTokens:       llama.Int(maxTokens),
		StopWords:       req.Stop,
		EnableThinking:  llama.Bool(thinking),
		ReasoningFormat: llama.ReasoningFormatAuto,
		VisionContext:   vision,
		ChatTemplate:    m.chatTemplate,
	}
	// Only override llama-go defaults when the caller explicitly set a value.
	// Proto3 zero values (0.0, 0) are indistinguishable from "not set", so we
	// leave the llama-go defaults (temp=0.8, top_p=0.95, top_k=40, min_p=0.05)
	// in place when all sampling params are zero — this matches the CLI behavior.
	if req.Temperature != 0 {
		opts.Temperature = llama.Float32(req.Temperature)
	}
	if req.TopP != 0 {
		opts.TopP = llama.Float32(req.TopP)
	}
	if req.TopK != 0 {
		opts.TopK = llama.Int(req.TopK)
	}
	if req.Seed != 0 {
		opts.Seed = llama.Int(req.Seed)
	}
	if req.MinP != 0 {
		opts.MinP = llama.Float32(req.MinP)
	}
	if req.RepeatPenalty != 0 {
		opts.RepeatPenalty = llama.Float32(req.RepeatPenalty)
	}
	if req.FrequencyPenalty != 0 {
		opts.FrequencyPenalty = llama.Float32(req.FrequencyPenalty)
	}
	if req.PresencePenalty != 0 {
		opts.PresencePenalty = llama.Float32(req.PresencePenalty)
	}

	return messages, opts, nil
}

// predict runs chat completion on a persistent pre-created context.
// Called by pool workers which each hold their own context.
func (m *Model) predict(ctx context.Context, lctx *llama.Context, vision *llama.VisionContext, req CompletionRequest) (CompletionResult, error) {
	messages, opts, err := m.buildChatArgs(req, vision)
	if err != nil {
		return CompletionResult{}, err
	}

	m.logger.Debug().
		Int("num_messages", len(messages)).
		Str("system_prompt_prefix", truncate(messages[0].Content, 100)).
		Str("user_prompt_prefix", truncate(messages[len(messages)-1].Content, 100)).
		Msg("predict start")

	response, err := lctx.Chat(ctx, messages, opts)
	if err != nil {
		return CompletionResult{}, fmt.Errorf("running chat completion: %w", err)
	}

	m.logger.Debug().
		Int("content_len", len(response.Content)).
		Int("reasoning_len", len(response.ReasoningContent)).
		Str("content_prefix", truncate(response.Content, 200)).
		Msg("predict done")

	return CompletionResult{
		Text:             response.Content,
		ReasoningContent: response.ReasoningContent,
		PromptTokens:     response.Usage.PromptTokens,
		CompletionTokens: response.Usage.CompletionTokens,
		TokensPerSecond:  response.Usage.TokensPerSecond,
	}, nil
}

// predictStream runs streaming chat completion, sending deltas to the provided channel.
// The caller is responsible for closing deltaCh after this returns.
func (m *Model) predictStream(ctx context.Context, lctx *llama.Context, vision *llama.VisionContext, req CompletionRequest, deltaCh chan<- CompletionDelta) error {
	messages, opts, err := m.buildChatArgs(req, vision)
	if err != nil {
		return err
	}

	m.logger.Debug().
		Int("num_messages", len(messages)).
		Msg("predict stream start")

	llamaDeltaCh, llamaErrCh := lctx.ChatStream(ctx, messages, opts)

	for delta := range llamaDeltaCh {
		select {
		case deltaCh <- CompletionDelta{
			Content:          delta.Content,
			ReasoningContent: delta.ReasoningContent,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Check for error from the stream.
	select {
	case err := <-llamaErrCh:
		if err != nil {
			return fmt.Errorf("streaming chat completion: %w", err)
		}
	default:
	}

	// Send a final delta with usage info from llama.cpp perf counters.
	usage := lctx.LastUsage()
	if usage.TotalTokens > 0 {
		select {
		case deltaCh <- CompletionDelta{
			Usage: &CompletionUsage{
				PromptTokens:     usage.PromptTokens,
				CompletionTokens: usage.CompletionTokens,
				TotalTokens:      usage.TotalTokens,
				TokensPerSecond:  usage.TokensPerSecond,
			},
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	m.logger.Debug().Msg("predict stream done")
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// Close drains the pool, frees vision contexts, and frees the underlying model.
func (m *Model) Close() {
	m.pool.close()
	if err := m.model.Close(); err != nil {
		m.logger.Warn().Err(err).Msg("error closing model")
	}
	m.logger.Info().Msg("model closed")
}
