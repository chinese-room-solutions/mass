package agent

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/KernelPryanic/ctxerr"
	"github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/chinese-room-solutions/mass/rpc"
	agentpb "github.com/chinese-room-solutions/mass/rpc/agent"
)

// --- Remote Chat Model ---

// remoteChatModel implements llm.ChatModelInterface by proxying to a remote agent via stream.
type remoteChatModel struct {
	fingerprint string
	agent       *StreamAgent
	pool        *remoteChatPool
}

func newRemoteChatModel(fingerprint, name string, agent *StreamAgent) *remoteChatModel {
	m := &remoteChatModel{
		fingerprint: fingerprint,
		agent:       agent,
	}
	m.pool = &remoteChatPool{fingerprint: fingerprint, agent: agent, name: name}
	return m
}

func (m *remoteChatModel) Pool() llm.PredictorInterface { return m.pool }

func (m *remoteChatModel) Close() {
	_, _ = m.agent.sendJob(&agentpb.HubMessage{
		Msg: &agentpb.HubMessage_UnloadModel{UnloadModel: &agentpb.HubUnloadModel{
			Fingerprint: m.fingerprint,
		}},
	})
}

// remoteChatPool implements llm.PredictorInterface by forwarding via the agent stream.
type remoteChatPool struct {
	fingerprint string
	agent       *StreamAgent
	name        string
}

func (p *remoteChatPool) Submit(ctx context.Context, req llm.CompletionRequest) llm.CompletionResult {
	protoReq := completionRequestToProto(req)
	result, err := p.agent.sendJob(&agentpb.HubMessage{
		Msg: &agentpb.HubMessage_ChatCompletion{ChatCompletion: &agentpb.HubChatCompletion{
			Fingerprint: p.fingerprint,
			Request:     protoReq,
		}},
	})
	if err != nil {
		return llm.CompletionResult{Error: ctxerr.With(fmt.Errorf("remote chat completion: %w", err), map[string]any{"fingerprint": p.fingerprint, "agent_id": p.agent.id})}
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
		TokensPerSecond:  msg.Usage.GetTokensPerSecond(),
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
	result, err := p.agent.sendJob(&agentpb.HubMessage{
		Msg: &agentpb.HubMessage_Tokenize{Tokenize: &agentpb.HubTokenize{
			Fingerprint: p.fingerprint,
			Request:     &rpc.TokenizeRequest{Text: text},
		}},
	})
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("remote tokenize: %w", err), map[string]any{"fingerprint": p.fingerprint, "agent_id": p.agent.id})
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
	agent       *StreamAgent
	pool        *remoteEmbeddingPool
}

func newRemoteEmbeddingModel(fingerprint, name string, agent *StreamAgent) *remoteEmbeddingModel {
	m := &remoteEmbeddingModel{
		fingerprint: fingerprint,
		agent:       agent,
	}
	m.pool = &remoteEmbeddingPool{fingerprint: fingerprint, agent: agent, name: name}
	return m
}

func (m *remoteEmbeddingModel) Pool() llm.EmbedderInterface { return m.pool }

func (m *remoteEmbeddingModel) Close() {
	_, _ = m.agent.sendJob(&agentpb.HubMessage{
		Msg: &agentpb.HubMessage_UnloadModel{UnloadModel: &agentpb.HubUnloadModel{
			Fingerprint: m.fingerprint,
		}},
	})
}

type remoteEmbeddingPool struct {
	fingerprint string
	agent       *StreamAgent
	name        string
}

func (p *remoteEmbeddingPool) Embed(ctx context.Context, text string) llm.EmbeddingResult {
	result, err := p.agent.sendJob(&agentpb.HubMessage{
		Msg: &agentpb.HubMessage_Embedding{Embedding: &agentpb.HubEmbedding{
			Fingerprint: p.fingerprint,
			Request:     &rpc.EmbeddingRequest{Input: text},
		}},
	})
	if err != nil {
		return llm.EmbeddingResult{Error: ctxerr.With(fmt.Errorf("remote embedding: %w", err), map[string]any{"fingerprint": p.fingerprint, "agent_id": p.agent.id})}
	}
	emb := result.GetEmbedding()
	if emb == nil {
		return llm.EmbeddingResult{Error: fmt.Errorf("unexpected result type for embedding")}
	}
	return llm.EmbeddingResult{Embedding: emb.Embedding}
}

func (p *remoteEmbeddingPool) EmbedBatch(ctx context.Context, texts []string) llm.BatchEmbeddingResult {
	result, err := p.agent.sendJob(&agentpb.HubMessage{
		Msg: &agentpb.HubMessage_BatchEmbedding{BatchEmbedding: &agentpb.HubBatchEmbedding{
			Fingerprint: p.fingerprint,
			Request:     &rpc.BatchEmbeddingRequest{Inputs: texts},
		}},
	})
	if err != nil {
		return llm.BatchEmbeddingResult{Error: ctxerr.With(fmt.Errorf("remote batch embedding: %w", err), map[string]any{"fingerprint": p.fingerprint, "agent_id": p.agent.id})}
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

// --- Path helpers ---

// toRelativeModelPath converts an absolute model path to a relative path
// (e.g. "publisher/model/variant.gguf") for sending to remote agents.
// If the path is already relative or can't be relativized, it's returned as-is.
func toRelativeModelPath(absPath, modelsDir string) string {
	if modelsDir == "" || absPath == "" {
		return absPath
	}
	rel, err := filepath.Rel(modelsDir, absPath)
	if err != nil || strings.HasPrefix(rel, "..") {
		return absPath
	}
	return filepath.ToSlash(rel)
}

// adaptChatConfigForPlacement clears identity fields that are incompatible
// with the target placement. Quantized KV cache types (q8_0, q4_0) require
// flash_attn which is GPU-only — clear both for CPU-only placement.
func adaptChatConfigForPlacement(cfg *llm.ChatModelConfig, placement llm.PlacementConfig) {
	if placement.GpuLayers == 0 {
		// CPU-only: quantized KV cache requires flash_attn (GPU-only).
		if cfg.CacheType != "" && cfg.CacheType != "f16" {
			cfg.CacheType = ""
		}
		cfg.FlashAttn = ""
	}
}

// --- Proto converters ---

func chatConfigToProto(cfg llm.ChatModelConfig, placement llm.PlacementConfig) *rpc.LlamaChatConfig {
	return &rpc.LlamaChatConfig{
		Model:         cfg.Path,
		ContextSize:   cfg.ContextSize,
		BatchSize:     cfg.BatchSize,
		FlashAttn:     cfg.FlashAttn,
		Thinking:      cfg.Thinking,
		Mmproj:        cfg.MmprojPath,
		ChatTemplate:  cfg.ChatTemplate,
		CacheType:     cfg.CacheType,
		GpuLayers:     placement.GpuLayers,
		Threads:       placement.Threads,
		MaxConcurrent: placement.MaxConcurrent,
		MainGpu:       placement.MainGPU,
		TensorSplit:   placement.TensorSplit,
	}
}

func embeddingConfigToProto(cfg llm.EmbeddingModelConfig, placement llm.PlacementConfig) *rpc.LlamaEmbeddingConfig {
	return &rpc.LlamaEmbeddingConfig{
		Model:         cfg.Path,
		ContextSize:   cfg.ContextSize,
		GpuLayers:     placement.GpuLayers,
		Threads:       placement.Threads,
		MaxConcurrent: placement.MaxConcurrent,
		MainGpu:       placement.MainGPU,
		TensorSplit:   placement.TensorSplit,
	}
}

func completionRequestToProto(req llm.CompletionRequest) *rpc.ChatCompletionRequest {
	messages := make([]*rpc.ChatMessage, len(req.Messages))
	for i, m := range req.Messages {
		msg := &rpc.ChatMessage{Role: m.Role, Content: m.Content}
		if len(m.Parts) > 0 {
			msg.Parts = make([]*rpc.ContentPart, len(m.Parts))
			for j, p := range m.Parts {
				msg.Parts[j] = &rpc.ContentPart{
					Type: string(p.Type), Text: p.Text, Data: p.Data, Filename: p.Filename,
				}
			}
		}
		messages[i] = msg
	}
	return &rpc.ChatCompletionRequest{
		Messages:         messages,
		MaxTokens:        int32(req.MaxTokens),
		Temperature:      req.Temperature,
		TopP:             req.TopP,
		TopK:             int32(req.TopK),
		Seed:             int32(req.Seed),
		Stop:             req.Stop,
		MinP:             req.MinP,
		RepeatPenalty:    req.RepeatPenalty,
		FrequencyPenalty: req.FrequencyPenalty,
		PresencePenalty:  req.PresencePenalty,
		EnableThinking:   req.EnableThinking,
	}
}
