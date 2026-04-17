package server

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"github.com/KernelPryanic/ctxerr"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/chinese-room-solutions/mass-proto/gen/go/rpcconnect"
	"github.com/chinese-room-solutions/mass/internal/metrics"
	"github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/chinese-room-solutions/mass/pkg/workerpool"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

// ModelResolverInterface resolves a request to a loaded model instance.
// Implemented by the scheduler. Returns the model handle plus its
// fingerprint (the loaded instance's identity, which the response carries
// back so callers can correlate which instance handled them).
type ModelResolverInterface interface {
	ResolveChat(req *rpc.ChatCompletionRequest) (llm.PredictorInterface, string, error)
	ResolveEmbedding(req *rpc.EmbeddingRequest) (llm.EmbedderInterface, string, error)
	ResolveBatchEmbedding(req *rpc.BatchEmbeddingRequest) (llm.EmbedderInterface, string, error)
	ResolveTokenize(req *rpc.TokenizeRequest) (llm.PredictorInterface, string, error)
}

// Compile-time check: Server implements rpcconnect.MassHandler.
var _ rpcconnect.MassHandler = (*Server)(nil)

// Server implements the mass.Mass ConnectRPC service.
type Server struct {
	rpcconnect.UnimplementedMassHandler
	logger   zerolog.Logger
	resolver ModelResolverInterface
}

// NewServer creates a new Server backed by a model resolver.
func NewServer(logger zerolog.Logger, resolver ModelResolverInterface) *Server {
	return &Server{
		logger:   logger,
		resolver: resolver,
	}
}

// textFileExtensions lists extensions treated as readable text when sent as "file" parts.
var textFileExtensions = map[string]bool{
	".txt": true, ".md": true, ".csv": true, ".tsv": true,
	".json": true, ".xml": true, ".yaml": true, ".yml": true, ".toml": true,
	".html": true, ".css": true, ".js": true, ".ts": true, ".jsx": true, ".tsx": true,
	".go": true, ".py": true, ".rs": true, ".c": true, ".cpp": true, ".h": true,
	".java": true, ".rb": true, ".sh": true, ".bash": true, ".ps1": true,
	".sql": true, ".proto": true, ".ini": true, ".cfg": true, ".conf": true,
	".log": true, ".env": true, ".gitignore": true, ".dockerfile": true,
}

// roleToString maps the proto Role enum to the lowercase strings used by
// downstream chat templates (matches OpenAI's role names).
func roleToString(r rpc.Role) string {
	switch r {
	case rpc.Role_ROLE_SYSTEM:
		return "system"
	case rpc.Role_ROLE_USER:
		return "user"
	case rpc.Role_ROLE_ASSISTANT:
		return "assistant"
	case rpc.Role_ROLE_TOOL:
		return "tool"
	default:
		return ""
	}
}

// protoToContentPart converts a proto ContentPart to an internal llm.ContentPart.
// For file parts with recognized text extensions, the data is extracted as UTF-8
// and converted to a text part so inference engines don't need file handling.
func protoToContentPart(p *rpc.ContentPart) llm.ContentPart {
	switch c := p.Content.(type) {
	case *rpc.ContentPart_Text:
		return llm.ContentPart{Type: llm.ContentText, Text: c.Text}
	case *rpc.ContentPart_Image:
		return llm.ContentPart{Type: llm.ContentImage, Data: c.Image.GetData()}
	case *rpc.ContentPart_Audio:
		return llm.ContentPart{Type: llm.ContentAudio, Data: c.Audio.GetData()}
	case *rpc.ContentPart_File:
		f := c.File
		ext := strings.ToLower(filepath.Ext(f.GetFilename()))
		if len(f.GetData()) > 0 && textFileExtensions[ext] && utf8.Valid(f.GetData()) {
			label := f.GetFilename()
			if label == "" {
				label = "file"
			}
			return llm.ContentPart{
				Type:     llm.ContentText,
				Text:     fmt.Sprintf("[%s]\n%s", label, string(f.GetData())),
				Filename: f.GetFilename(),
			}
		}
		return llm.ContentPart{Type: llm.ContentFile, Data: f.GetData(), Filename: f.GetFilename()}
	default:
		return llm.ContentPart{}
	}
}

// ProtoToMessages converts proto ChatMessage slices to internal llm.ChatMessage slices,
// including multimodal Parts (images, audio, files).
func ProtoToMessages(msgs []*rpc.ChatMessage) []llm.ChatMessage {
	messages := make([]llm.ChatMessage, len(msgs))
	for i, msg := range msgs {
		lm := llm.ChatMessage{
			Role:    roleToString(msg.Role),
			Content: msg.Content,
		}
		if len(msg.Parts) > 0 {
			lm.Parts = make([]llm.ContentPart, 0, len(msg.Parts))
			for _, p := range msg.Parts {
				lm.Parts = append(lm.Parts, protoToContentPart(p))
			}
		}
		messages[i] = lm
	}
	return messages
}

// samplingToCompletion converts the shared SamplingParams proto to internal
// completion-request fields. Nil sampling = all defaults.
func samplingToCompletion(s *rpc.SamplingParams) llm.CompletionRequest {
	if s == nil {
		return llm.CompletionRequest{}
	}
	return llm.CompletionRequest{
		MaxTokens:        int(s.GetMaxTokens()),
		Temperature:      s.Temperature,
		TopP:             s.TopP,
		TopK:             int(s.TopK),
		Seed:             int(s.GetSeed()),
		Stop:             s.Stop,
		MinP:             s.MinP,
		RepeatPenalty:    s.RepeatPenalty,
		FrequencyPenalty: s.FrequencyPenalty,
		PresencePenalty:  s.PresencePenalty,
		EnableThinking:   s.EnableThinking,
	}
}

// RPCToCompletionRequest converts RPC messages and request params to an llm.CompletionRequest.
func RPCToCompletionRequest(messages []llm.ChatMessage, req *rpc.ChatCompletionRequest) llm.CompletionRequest {
	cr := samplingToCompletion(req.Sampling)
	cr.Messages = messages
	return cr
}

// doCompletion returns the response plus the fingerprint of the loaded
// instance that served it. Callers attach the fingerprint to the
// X-Mass-Fingerprint response header — see the proto's stability comment
// for the rationale (kept out of OpenAI-shaped JSON bodies).
func (s *Server) doCompletion(
	ctx context.Context, req *rpc.ChatCompletionRequest,
) (*rpc.ChatCompletionResponse, string, error) {
	model, fingerprint, err := s.resolver.ResolveChat(req)
	if err != nil {
		return nil, "", err
	}

	if len(req.Messages) == 0 {
		return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("messages: at least one message is required"))
	}

	messages := ProtoToMessages(req.Messages)
	modelID := req.ModelConfig.GetLlama().GetModel()

	metrics.ActiveRequestsInc(modelID)
	start := time.Now()
	result := model.Submit(ctx, RPCToCompletionRequest(messages, req))
	metrics.ActiveRequestsDec(modelID)
	metrics.InferenceDuration(modelID, time.Since(start).Seconds())

	if result.Error != nil {
		return nil, fingerprint, connect.NewError(connect.CodeInternal,
			ctxerr.With(fmt.Errorf("inference failed for model %q: %w", modelID, result.Error), map[string]any{"model": modelID, "fingerprint": fingerprint}),
		)
	}

	return &rpc.ChatCompletionResponse{
		Id:    uuid.NewString(),
		Model: modelID,
		Message: &rpc.ChatMessage{
			Role:    rpc.Role_ROLE_ASSISTANT,
			Content: result.Text,
		},
		FinishReason:     rpc.FinishReason_FINISH_REASON_STOP,
		ReasoningContent: result.ReasoningContent,
		Usage: &rpc.Usage{
			PromptTokens:     int32(result.PromptTokens),
			CompletionTokens: int32(result.CompletionTokens),
			TotalTokens:      int32(result.PromptTokens + result.CompletionTokens),
		},
		TokensPerSecond: result.TokensPerSecond,
	}, fingerprint, nil
}

// fingerprintHeader is the response header name used to carry the loaded
// instance's identity. Sent on every successful inference response.
const fingerprintHeader = "X-Mass-Fingerprint"

// withFingerprint wraps a response and stamps the fingerprint header.
// Centralizes the convention so all inference RPCs do the same thing.
func withFingerprint[T any](resp *T, fingerprint string) *connect.Response[T] {
	r := connect.NewResponse(resp)
	if fingerprint != "" {
		r.Header().Set(fingerprintHeader, fingerprint)
	}
	return r
}

// ChatCompletion handles a single chat completion request.
func (s *Server) ChatCompletion(
	ctx context.Context, req *connect.Request[rpc.ChatCompletionRequest],
) (*connect.Response[rpc.ChatCompletionResponse], error) {
	s.logger.Trace().Str("method", "ChatCompletion").Int("messages", len(req.Msg.Messages)).Msg("handling request")
	resp, fingerprint, err := s.doCompletion(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return withFingerprint(resp, fingerprint), nil
}

// ChatCompletionStream handles a streaming chat completion. Each emitted
// chunk carries an incremental delta; the final chunk has finish_reason set
// and (when available) usage stats.
func (s *Server) ChatCompletionStream(
	ctx context.Context,
	req *connect.Request[rpc.ChatCompletionRequest],
	stream *connect.ServerStream[rpc.ChatCompletionChunk],
) error {
	s.logger.Trace().Str("method", "ChatCompletionStream").Int("messages", len(req.Msg.Messages)).Msg("handling request")

	model, fingerprint, err := s.resolver.ResolveChat(req.Msg)
	if err != nil {
		return err
	}
	if len(req.Msg.Messages) == 0 {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("messages: at least one message is required"))
	}

	if fingerprint != "" {
		stream.ResponseHeader().Set(fingerprintHeader, fingerprint)
	}

	messages := ProtoToMessages(req.Msg.Messages)
	completionReq := RPCToCompletionRequest(messages, req.Msg)
	modelID := req.Msg.ModelConfig.GetLlama().GetModel()
	requestID := uuid.NewString()

	deltaCh, errCh := model.SubmitStream(ctx, completionReq)

	roleSent := false
	var finalUsage *llm.CompletionUsage
	for delta := range deltaCh {
		if delta.Usage != nil {
			finalUsage = delta.Usage
			continue
		}
		chunk := &rpc.ChatCompletionChunk{
			Id:    requestID,
			Model: modelID,
			Delta: &rpc.ChatMessageDelta{
				Content:          delta.Content,
				ReasoningContent: delta.ReasoningContent,
			},
		}
		if !roleSent {
			chunk.Delta.Role = rpc.Role_ROLE_ASSISTANT
			roleSent = true
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
	}

	select {
	case sErr := <-errCh:
		if sErr != nil {
			return connect.NewError(connect.CodeInternal,
				ctxerr.With(fmt.Errorf("stream failed: %w", sErr), map[string]any{"model": modelID, "fingerprint": fingerprint}),
			)
		}
	default:
	}

	final := &rpc.ChatCompletionChunk{
		Id:           requestID,
		Model:        modelID,
		FinishReason: rpc.FinishReason_FINISH_REASON_STOP,
	}
	if finalUsage != nil {
		final.Usage = &rpc.Usage{
			PromptTokens:     int32(finalUsage.PromptTokens),
			CompletionTokens: int32(finalUsage.CompletionTokens),
			TotalTokens:      int32(finalUsage.TotalTokens),
		}
		final.TokensPerSecond = finalUsage.TokensPerSecond
	}
	return stream.Send(final)
}

// BatchChatCompletion runs N chat items against one loaded model in
// parallel. **Parallel fan-out, not llama.cpp multi-sequence batching** —
// each item is a separate llama_decode gated by the model's predictor pool
// (capped at max_concurrent). Wall-clock ≈ `ceil(N/max_concurrent) ×
// per_item`, plus one HTTP round-trip instead of N. Real multi-sequence
// batching was tried and regressed throughput.
//
// Homogeneity is structural: one model_config + per-prompt items. No
// per-item model, same as [BatchEmbeddingRequest].
func (s *Server) BatchChatCompletion(
	ctx context.Context, req *connect.Request[rpc.BatchChatCompletionRequest],
) (*connect.Response[rpc.BatchChatCompletionResponse], error) {
	s.logger.Trace().Str("method", "BatchChatCompletion").Int("items", len(req.Msg.Items)).Msg("handling request")
	if len(req.Msg.Items) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("items: at least one item is required"))
	}

	responses := make([]*rpc.ChatCompletionResponse, len(req.Msg.Items))
	fingerprints := make([]string, len(req.Msg.Items))
	errs := make([]error, len(req.Msg.Items))

	wp := workerpool.New(len(req.Msg.Items))
	for i, item := range req.Msg.Items {
		inner := batchItemToChatRequest(req.Msg.ModelConfig, item)
		if err := wp.Do(ctx, func(ctx context.Context) {
			responses[i], fingerprints[i], errs[i] = s.doCompletion(ctx, inner)
		}); err != nil {
			// ctx was cancelled before the slot opened; record per-item.
			errs[i] = err
		}
	}
	wp.Close()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	// All items share one model_config (proto-enforced), so every item's
	// fingerprint is the same — surface the first non-empty one as the
	// batch-level fingerprint.
	var fp string
	for _, f := range fingerprints {
		if f != "" {
			fp = f
			break
		}
	}
	return withFingerprint(&rpc.BatchChatCompletionResponse{
		Responses: responses,
	}, fp), nil
}

// batchItemToChatRequest composes a per-item ChatCompletionRequest from
// the batch's shared model_config and the item's per-prompt knobs.
func batchItemToChatRequest(mc *rpc.ChatModelConfig, item *rpc.BatchChatCompletionItem) *rpc.ChatCompletionRequest {
	return &rpc.ChatCompletionRequest{
		ModelConfig: mc,
		Messages:    item.Messages,
		Sampling:    item.Sampling,
	}
}

// Tokenize converts text to token IDs using the specified model's tokenizer.
func (s *Server) Tokenize(
	ctx context.Context, req *connect.Request[rpc.TokenizeRequest],
) (*connect.Response[rpc.TokenizeResponse], error) {
	s.logger.Trace().Str("method", "Tokenize").Msg("handling request")
	model, fingerprint, err := s.resolver.ResolveTokenize(req.Msg)
	if err != nil {
		return nil, err
	}

	tokens, err := model.Tokenize(ctx, req.Msg.Text)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			ctxerr.With(fmt.Errorf("tokenize failed: %w", err), map[string]any{"fingerprint": fingerprint}),
		)
	}

	return connect.NewResponse(&rpc.TokenizeResponse{Tokens: tokens}), nil
}

// Embedding handles a single embedding request.
func (s *Server) Embedding(
	ctx context.Context, req *connect.Request[rpc.EmbeddingRequest],
) (*connect.Response[rpc.EmbeddingResponse], error) {
	s.logger.Trace().Str("method", "Embedding").Msg("handling request")
	model, fingerprint, err := s.resolver.ResolveEmbedding(req.Msg)
	if err != nil {
		return nil, err
	}

	if req.Msg.Input == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("input: input text is required"))
	}

	modelID := req.Msg.ModelConfig.GetLlama().GetModel()
	metrics.ActiveRequestsInc(modelID)
	start := time.Now()
	result := model.Embed(ctx, req.Msg.Input)
	metrics.ActiveRequestsDec(modelID)
	metrics.InferenceDuration(modelID, time.Since(start).Seconds())

	if result.Error != nil {
		return nil, connect.NewError(connect.CodeInternal,
			ctxerr.With(fmt.Errorf("embedding failed: %w", result.Error), map[string]any{"model": modelID, "fingerprint": fingerprint}),
		)
	}

	return withFingerprint(&rpc.EmbeddingResponse{
		Id:        uuid.NewString(),
		Model:     modelID,
		Embedding: result.Embedding,
	}, fingerprint), nil
}

// BatchEmbedding embeds N inputs in **one** llama.cpp forward pass — real
// multi-sequence batching, unlike [Server.BatchChatCompletion]. Wall-clock
// ≈ one pass over the longest input (padded), not N × per-item. Proto
// enforces one (Model, model_config) per batch, so no homogeneity check.
func (s *Server) BatchEmbedding(
	ctx context.Context, req *connect.Request[rpc.BatchEmbeddingRequest],
) (*connect.Response[rpc.BatchEmbeddingResponse], error) {
	s.logger.Trace().Str("method", "BatchEmbedding").Int("inputs", len(req.Msg.Inputs)).Msg("handling request")
	model, fingerprint, err := s.resolver.ResolveBatchEmbedding(req.Msg)
	if err != nil {
		return nil, err
	}

	if len(req.Msg.Inputs) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("inputs: at least one input is required"))
	}

	modelID := req.Msg.ModelConfig.GetLlama().GetModel()
	metrics.ActiveRequestsInc(modelID)
	start := time.Now()
	result := model.EmbedBatch(ctx, req.Msg.Inputs)
	metrics.ActiveRequestsDec(modelID)
	metrics.InferenceDuration(modelID, time.Since(start).Seconds())

	if result.Error != nil {
		return nil, connect.NewError(connect.CodeInternal,
			ctxerr.With(fmt.Errorf("batch embedding failed: %w", result.Error), map[string]any{"model": modelID, "fingerprint": fingerprint, "inputs": len(req.Msg.Inputs)}),
		)
	}

	embeddings := make([]*rpc.EmbeddingData, len(result.Embeddings))
	for i, emb := range result.Embeddings {
		embeddings[i] = &rpc.EmbeddingData{
			Index:     int32(i),
			Embedding: emb,
		}
	}

	return withFingerprint(&rpc.BatchEmbeddingResponse{
		Id:         uuid.NewString(),
		Model:      modelID,
		Embeddings: embeddings,
	}, fingerprint), nil
}

// --- Execute methods for queue processor ---
// These accept raw proto bytes, unmarshal, run inference, and return serialized response bytes.

// ExecuteChatCompletion processes a serialized ChatCompletionRequest and
// returns serialized response bytes. The fingerprint of the serving
// instance is discarded here — async queue results carry only the body;
// callers needing the fingerprint should use the synchronous RPC where
// it's exposed via the X-Mass-Fingerprint response header.
func (s *Server) ExecuteChatCompletion(ctx context.Context, payload []byte) ([]byte, error) {
	req := &rpc.ChatCompletionRequest{}
	if err := proto.Unmarshal(payload, req); err != nil {
		return nil, fmt.Errorf("unmarshalling chat request: %w", err)
	}
	resp, _, err := s.doCompletion(ctx, req)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(resp)
}

// ExecuteBatchChatCompletion processes a serialized BatchChatCompletionRequest.
func (s *Server) ExecuteBatchChatCompletion(ctx context.Context, payload []byte) ([]byte, error) {
	req := &rpc.BatchChatCompletionRequest{}
	if err := proto.Unmarshal(payload, req); err != nil {
		return nil, fmt.Errorf("unmarshalling batch chat request: %w", err)
	}
	resp, err := s.BatchChatCompletion(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return proto.Marshal(resp.Msg)
}

// ExecuteEmbedding processes a serialized EmbeddingRequest.
func (s *Server) ExecuteEmbedding(ctx context.Context, payload []byte) ([]byte, error) {
	req := &rpc.EmbeddingRequest{}
	if err := proto.Unmarshal(payload, req); err != nil {
		return nil, fmt.Errorf("unmarshalling embedding request: %w", err)
	}
	resp, err := s.Embedding(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return proto.Marshal(resp.Msg)
}

// ExecuteBatchEmbedding processes a serialized BatchEmbeddingRequest.
func (s *Server) ExecuteBatchEmbedding(ctx context.Context, payload []byte) ([]byte, error) {
	req := &rpc.BatchEmbeddingRequest{}
	if err := proto.Unmarshal(payload, req); err != nil {
		return nil, fmt.Errorf("unmarshalling batch embedding request: %w", err)
	}
	resp, err := s.BatchEmbedding(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return proto.Marshal(resp.Msg)
}

// ExecuteTokenize processes a serialized TokenizeRequest.
func (s *Server) ExecuteTokenize(ctx context.Context, payload []byte) ([]byte, error) {
	req := &rpc.TokenizeRequest{}
	if err := proto.Unmarshal(payload, req); err != nil {
		return nil, fmt.Errorf("unmarshalling tokenize request: %w", err)
	}
	resp, err := s.Tokenize(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return proto.Marshal(resp.Msg)
}
