package worker

import (
	"context"
	"fmt"
	"strings"

	"github.com/KernelPryanic/ctxerr"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/pkg/llm"
)

// --- Remote Chat Model ---

// remoteChatModel implements llm.ChatModelInterface by proxying to a remote worker via stream.
type remoteChatModel struct {
	fingerprint string
	worker      *StreamWorker
	pool        *remoteChatPool
	poolSize    int32
}

func newRemoteChatModel(fingerprint, name string, worker *StreamWorker, poolSize int32) *remoteChatModel {
	m := &remoteChatModel{
		fingerprint: fingerprint,
		worker:      worker,
		poolSize:    poolSize,
	}
	m.pool = &remoteChatPool{fingerprint: fingerprint, worker: worker, name: name}
	return m
}

func (m *remoteChatModel) Pool() llm.PredictorInterface { return m.pool }
func (m *remoteChatModel) PoolSize() int32              { return m.poolSize }

func (m *remoteChatModel) Close() {
	if _, err := m.worker.sendJob(&workerpb.HubMessage{
		Msg: &workerpb.HubMessage_UnloadModel{UnloadModel: &workerpb.HubUnloadModel{
			Fingerprint: m.fingerprint,
		}},
	}); err != nil {
		m.worker.logger.Warn().Err(err).Str("fingerprint", m.fingerprint).Msg("unloading remote chat model")
	}
}

// remoteChatPool implements llm.PredictorInterface by forwarding via the worker stream.
type remoteChatPool struct {
	fingerprint string
	worker      *StreamWorker
	name        string
}

func (p *remoteChatPool) Submit(ctx context.Context, req llm.CompletionRequest) llm.CompletionResult {
	protoReq := completionRequestToProto(req)
	result, err := p.worker.sendJob(&workerpb.HubMessage{
		Msg: &workerpb.HubMessage_ChatCompletion{ChatCompletion: &workerpb.HubChatCompletion{
			Fingerprint: p.fingerprint,
			Request:     protoReq,
		}},
	})
	if err != nil {
		return llm.CompletionResult{Error: ctxerr.With(fmt.Errorf("remote chat completion: %w", err), map[string]any{"fingerprint": p.fingerprint, "worker_id": p.worker.id})}
	}
	msg := result.GetChatCompletion()
	if msg == nil {
		return llm.CompletionResult{Error: fmt.Errorf("unexpected result type for chat completion")}
	}
	return llm.CompletionResult{
		Text:             msg.Message.GetContent(),
		ReasoningContent: msg.ReasoningContent,
		PromptTokens:     int(msg.Usage.GetPromptTokens()),
		CompletionTokens: int(msg.Usage.GetCompletionTokens()),
		TokensPerSecond:  msg.GetTokensPerSecond(),
	}
}

func (p *remoteChatPool) SubmitStream(ctx context.Context, req llm.CompletionRequest) (<-chan llm.CompletionDelta, <-chan error) {
	deltaCh := make(chan llm.CompletionDelta, 1)
	errCh := make(chan error, 1)

	go func() {
		defer close(deltaCh)
		// Unary fallback: send full response as single delta.
		result := p.Submit(ctx, req)
		if result.Error != nil {
			errCh <- result.Error
			return
		}
		deltaCh <- llm.CompletionDelta{
			Content:          result.Text,
			ReasoningContent: result.ReasoningContent,
			Usage: &llm.CompletionUsage{
				PromptTokens:     result.PromptTokens,
				CompletionTokens: result.CompletionTokens,
				TotalTokens:      result.PromptTokens + result.CompletionTokens,
				TokensPerSecond:  result.TokensPerSecond,
			},
		}
	}()

	return deltaCh, errCh
}

func (p *remoteChatPool) Tokenize(ctx context.Context, text string) ([]int32, error) {
	result, err := p.worker.sendJob(&workerpb.HubMessage{
		Msg: &workerpb.HubMessage_Tokenize{Tokenize: &workerpb.HubTokenize{
			Fingerprint: p.fingerprint,
			Request:     &rpc.TokenizeRequest{Text: text},
		}},
	})
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("remote tokenize: %w", err), map[string]any{"fingerprint": p.fingerprint, "worker_id": p.worker.id})
	}
	tok := result.GetTokenize()
	if tok == nil {
		return nil, fmt.Errorf("unexpected result type for tokenize")
	}
	return tok.Tokens, nil
}

func (p *remoteChatPool) Name() string { return p.name }

// --- Remote Embedding Model ---

type remoteEmbeddingModel struct {
	fingerprint string
	worker      *StreamWorker
	pool        *remoteEmbeddingPool
	poolSize    int32
}

func newRemoteEmbeddingModel(fingerprint, name string, worker *StreamWorker, poolSize int32) *remoteEmbeddingModel {
	m := &remoteEmbeddingModel{
		fingerprint: fingerprint,
		worker:      worker,
		poolSize:    poolSize,
	}
	m.pool = &remoteEmbeddingPool{fingerprint: fingerprint, worker: worker, name: name}
	return m
}

func (m *remoteEmbeddingModel) Pool() llm.EmbedderInterface { return m.pool }
func (m *remoteEmbeddingModel) PoolSize() int32             { return m.poolSize }

func (m *remoteEmbeddingModel) Close() {
	if _, err := m.worker.sendJob(&workerpb.HubMessage{
		Msg: &workerpb.HubMessage_UnloadModel{UnloadModel: &workerpb.HubUnloadModel{
			Fingerprint: m.fingerprint,
		}},
	}); err != nil {
		m.worker.logger.Warn().Err(err).Str("fingerprint", m.fingerprint).Msg("unloading remote embedding model")
	}
}

type remoteEmbeddingPool struct {
	fingerprint string
	worker      *StreamWorker
	name        string
}

func (p *remoteEmbeddingPool) Embed(ctx context.Context, text string) llm.EmbeddingResult {
	result, err := p.worker.sendJob(&workerpb.HubMessage{
		Msg: &workerpb.HubMessage_Embedding{Embedding: &workerpb.HubEmbedding{
			Fingerprint: p.fingerprint,
			Request:     &rpc.EmbeddingRequest{Input: text},
		}},
	})
	if err != nil {
		return llm.EmbeddingResult{Error: ctxerr.With(fmt.Errorf("remote embedding: %w", err), map[string]any{"fingerprint": p.fingerprint, "worker_id": p.worker.id})}
	}
	emb := result.GetEmbedding()
	if emb == nil {
		return llm.EmbeddingResult{Error: fmt.Errorf("unexpected result type for embedding")}
	}
	return llm.EmbeddingResult{Embedding: emb.Embedding}
}

func (p *remoteEmbeddingPool) EmbedBatch(ctx context.Context, texts []string) llm.BatchEmbeddingResult {
	result, err := p.worker.sendJob(&workerpb.HubMessage{
		Msg: &workerpb.HubMessage_BatchEmbedding{BatchEmbedding: &workerpb.HubBatchEmbedding{
			Fingerprint: p.fingerprint,
			Request:     &rpc.BatchEmbeddingRequest{Inputs: texts},
		}},
	})
	if err != nil {
		return llm.BatchEmbeddingResult{Error: ctxerr.With(fmt.Errorf("remote batch embedding: %w", err), map[string]any{"fingerprint": p.fingerprint, "worker_id": p.worker.id})}
	}
	batch := result.GetBatchEmbedding()
	if batch == nil {
		return llm.BatchEmbeddingResult{Error: fmt.Errorf("unexpected result type for batch embedding")}
	}
	embeddings := make([][]float32, len(batch.Embeddings))
	for i, e := range batch.Embeddings {
		embeddings[i] = e.Embedding
	}
	return llm.BatchEmbeddingResult{Embeddings: embeddings}
}

func (p *remoteEmbeddingPool) Name() string { return p.name }

// adaptChatConfigForPlacement clears identity fields that are incompatible
// with the target placement. Quantized KV cache types (q8_0, q4_0) require
// flash_attn which is GPU-only — clear both for CPU-only placement.
func adaptChatConfigForPlacement(cfg *llm.LlamaChatConfig, placement llm.PlacementConfig) {
	if placement.GpuLayers == 0 {
		// CPU-only: quantized KV cache requires flash_attn (GPU-only).
		if cfg.CacheType != "" && cfg.CacheType != "f16" {
			cfg.CacheType = ""
		}
		cfg.FlashAttn = ""
	}
}

// --- Proto converters ---

// chatConfigToProto serializes the wire-side fields of a chat config. Model
// and mmproj file paths are NOT carried here — see [buildLocalModelFile] and
// the `files` field on [workerpb.HubLoadChatModel].
func chatConfigToProto(cfg llm.LlamaChatConfig, placement llm.PlacementConfig) *rpc.LlamaChatConfig {
	out := &rpc.LlamaChatConfig{
		ContextSize:   protoOptInt32(cfg.ContextSize),
		BatchSize:     protoOptInt32(cfg.BatchSize),
		FlashAttn:     flashAttnStringToProto(cfg.FlashAttn),
		Thinking:      cfg.Thinking,
		ChatTemplate:  cfg.ChatTemplate,
		CacheType:     cacheTypeStringToProto(cfg.CacheType),
		GpuLayers:     protoOptInt32(placement.GpuLayers),
		Threads:       protoOptInt32(placement.Threads),
		MaxConcurrent: protoOptInt32(placement.MaxConcurrent),
		MainGpu:       placement.MainGPU,
		TensorSplit:   tensorSplitStringToProto(placement.TensorSplit),
	}
	return out
}

// embeddingConfigToProto serializes the wire-side fields of an embedding
// config. Model file path is NOT carried here — see [buildLocalModelFile].
func embeddingConfigToProto(cfg llm.LlamaEmbeddingConfig, placement llm.PlacementConfig) *rpc.LlamaEmbeddingConfig {
	return &rpc.LlamaEmbeddingConfig{
		ContextSize:   protoOptInt32(cfg.ContextSize),
		GpuLayers:     protoOptInt32(placement.GpuLayers),
		Threads:       protoOptInt32(placement.Threads),
		MaxConcurrent: protoOptInt32(placement.MaxConcurrent),
		MainGpu:       placement.MainGPU,
		TensorSplit:   tensorSplitStringToProto(placement.TensorSplit),
	}
}

func completionRequestToProto(req llm.CompletionRequest) *rpc.ChatCompletionRequest {
	messages := make([]*rpc.ChatMessage, len(req.Messages))
	for i, m := range req.Messages {
		msg := &rpc.ChatMessage{Role: roleStringToProto(m.Role), Content: m.Content}
		if len(m.Parts) > 0 {
			msg.Parts = make([]*rpc.ContentPart, len(m.Parts))
			for j, p := range m.Parts {
				msg.Parts[j] = contentPartToProto(p)
			}
		}
		messages[i] = msg
	}
	return &rpc.ChatCompletionRequest{
		Messages: messages,
		Sampling: &rpc.SamplingParams{
			MaxTokens:        protoOptInt32(int32(req.MaxTokens)),
			Temperature:      req.Temperature,
			TopP:             req.TopP,
			TopK:             int32(req.TopK),
			Seed:             protoOptInt32(int32(req.Seed)),
			Stop:             req.Stop,
			MinP:             req.MinP,
			RepeatPenalty:    req.RepeatPenalty,
			FrequencyPenalty: req.FrequencyPenalty,
			PresencePenalty:  req.PresencePenalty,
			EnableThinking:   req.EnableThinking,
		},
	}
}

// contentPartToProto wraps an internal multipart entry as the matching
// ContentPart oneof variant.
func contentPartToProto(p llm.ContentPart) *rpc.ContentPart {
	switch p.Type {
	case llm.ContentText:
		return &rpc.ContentPart{Content: &rpc.ContentPart_Text{Text: p.Text}}
	case llm.ContentImage:
		return &rpc.ContentPart{Content: &rpc.ContentPart_Image{Image: &rpc.ImageContent{Data: p.Data}}}
	case llm.ContentAudio:
		return &rpc.ContentPart{Content: &rpc.ContentPart_Audio{Audio: &rpc.AudioContent{Data: p.Data}}}
	case llm.ContentFile:
		return &rpc.ContentPart{Content: &rpc.ContentPart_File{File: &rpc.FileContent{Data: p.Data, Filename: p.Filename}}}
	default:
		return &rpc.ContentPart{}
	}
}

// protoOptInt32 returns a pointer to v, used for proto3 `optional int32`
// fields where 0 carries meaning (and so the unset/zero distinction matters).
// We treat 0 as "set to zero" — internal config has no separate "unset"
// state, and the wire protocol can tell zero apart from unset only because
// the field is optional.
func protoOptInt32(v int32) *int32 { return &v }

// roleStringToProto maps the internal role string to the proto Role enum.
// Mirror of roleToString in internal/server.
func roleStringToProto(s string) rpc.Role {
	switch s {
	case "system":
		return rpc.Role_ROLE_SYSTEM
	case "user":
		return rpc.Role_ROLE_USER
	case "assistant":
		return rpc.Role_ROLE_ASSISTANT
	case "tool":
		return rpc.Role_ROLE_TOOL
	default:
		return rpc.Role_ROLE_UNSPECIFIED
	}
}

// flashAttnStringToProto maps the internal "enabled"/"disabled"/"" form
// to the proto3 optional bool (nil = auto).
func flashAttnStringToProto(s string) *bool {
	switch s {
	case "enabled":
		t := true
		return &t
	case "disabled":
		f := false
		return &f
	default:
		return nil
	}
}

// cacheTypeStringToProto maps the internal cache-type string to the
// CacheType enum.
func cacheTypeStringToProto(s string) rpc.CacheType {
	switch s {
	case "f16":
		return rpc.CacheType_CACHE_TYPE_F16
	case "q8_0":
		return rpc.CacheType_CACHE_TYPE_Q8_0
	case "q4_0":
		return rpc.CacheType_CACHE_TYPE_Q4_0
	default:
		return rpc.CacheType_CACHE_TYPE_UNSPECIFIED
	}
}

// tensorSplitStringToProto parses the canonical "x,y,z" CSV form into a
// float slice. Returns nil for empty input.
func tensorSplitStringToProto(s string) []float32 {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]float32, 0, len(parts))
	for _, p := range parts {
		var v float32
		if _, err := fmt.Sscanf(strings.TrimSpace(p), "%g", &v); err == nil {
			out = append(out, v)
		}
	}
	return out
}
