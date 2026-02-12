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
	"github.com/chinese-room-solutions/mass/internal/llm"
	"github.com/chinese-room-solutions/mass/internal/metrics"
	"github.com/chinese-room-solutions/mass/pkg/workerpool"
	"github.com/chinese-room-solutions/mass/rpc"
	"github.com/chinese-room-solutions/mass/rpc/rpcconnect"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

// ModelResolverInterface resolves a request to a loaded model instance.
// Implemented by the scheduler to support both legacy name-based lookups and
// dynamic model_config-based loading.
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

// protoContentType maps the proto type string to the internal ContentType.
var protoContentType = map[string]llm.ContentType{
	"text":  llm.ContentText,
	"image": llm.ContentImage,
	"audio": llm.ContentAudio,
	"file":  llm.ContentFile,
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

// protoToContentPart converts a proto ContentPart to an internal llm.ContentPart.
// For FILE parts with recognized text extensions, the data is extracted as UTF-8
// and converted to a TEXT part so inference engines don't need file handling.
func protoToContentPart(p *rpc.ContentPart) llm.ContentPart {
	ct := protoContentType[p.Type]

	if ct == llm.ContentFile && len(p.Data) > 0 {
		ext := strings.ToLower(filepath.Ext(p.Filename))
		if textFileExtensions[ext] && utf8.Valid(p.Data) {
			label := p.Filename
			if label == "" {
				label = "file"
			}
			return llm.ContentPart{
				Type:     llm.ContentText,
				Text:     fmt.Sprintf("[%s]\n%s", label, string(p.Data)),
				Filename: p.Filename,
			}
		}
		// Non-text file: pass data through for engines that may support it.
		return llm.ContentPart{
			Type:     llm.ContentFile,
			Data:     p.Data,
			Filename: p.Filename,
		}
	}

	return llm.ContentPart{
		Type:     ct,
		Text:     p.Text,
		Data:     p.Data,
		Filename: p.Filename,
	}
}

// ProtoToMessages converts proto ChatMessage slices to internal llm.ChatMessage slices,
// including multimodal Parts (images, audio, files).
func ProtoToMessages(msgs []*rpc.ChatMessage) []llm.ChatMessage {
	messages := make([]llm.ChatMessage, len(msgs))
	for i, msg := range msgs {
		lm := llm.ChatMessage{
			Role:    msg.Role,
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

// RPCToCompletionRequest converts RPC messages and request params to an llm.CompletionRequest.
func RPCToCompletionRequest(messages []llm.ChatMessage, req *rpc.ChatCompletionRequest) llm.CompletionRequest {
	return llm.CompletionRequest{
		Messages:         messages,
		MaxTokens:        int(req.MaxTokens),
		Temperature:      req.Temperature,
		TopP:             req.TopP,
		TopK:             int(req.TopK),
		Seed:             int(req.Seed),
		Stop:             req.Stop,
		MinP:             req.MinP,
		RepeatPenalty:    req.RepeatPenalty,
		FrequencyPenalty: req.FrequencyPenalty,
		PresencePenalty:  req.PresencePenalty,
		EnableThinking:   req.EnableThinking,
	}
}

func (s *Server) doCompletion(
	ctx context.Context, req *rpc.ChatCompletionRequest,
) (*rpc.ChatCompletionResponse, error) {
	model, modelName, err := s.resolver.ResolveChat(req)
	if err != nil {
		return nil, err
	}

	if len(req.Messages) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("messages: at least one message is required"))
	}

	messages := ProtoToMessages(req.Messages)

	metrics.ActiveRequestsInc(modelName)
	start := time.Now()
	result := model.Submit(ctx, RPCToCompletionRequest(messages, req))
	metrics.ActiveRequestsDec(modelName)
	metrics.InferenceDuration(modelName, time.Since(start).Seconds())

	if result.Error != nil {
		return nil, connect.NewError(connect.CodeInternal,
			ctxerr.With(fmt.Errorf("inference failed for model %q: %w", modelName, result.Error), map[string]any{"model": modelName}),
		)
	}

	return &rpc.ChatCompletionResponse{
		Id:    uuid.NewString(),
		Model: modelName,
		Message: &rpc.ChatMessage{
			Role:    "assistant",
			Content: result.Text,
		},
		FinishReason:     "stop",
		ReasoningContent: result.ReasoningContent,
		Usage: &rpc.Usage{
			PromptTokens:     int32(result.PromptTokens),
			CompletionTokens: int32(result.CompletionTokens),
			TotalTokens:      int32(result.PromptTokens + result.CompletionTokens),
			TokensPerSecond:  result.TokensPerSecond,
		},
	}, nil
}

// ChatCompletion handles a single chat completion request.
func (s *Server) ChatCompletion(
	ctx context.Context, req *connect.Request[rpc.ChatCompletionRequest],
) (*connect.Response[rpc.ChatCompletionResponse], error) {
	s.logger.Trace().Str("method", "ChatCompletion").Str("model", req.Msg.Model).Int("messages", len(req.Msg.Messages)).Msg("handling request")
	resp, err := s.doCompletion(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

// BatchChatCompletion handles multiple chat completion requests concurrently.
// All requests run in parallel — LoadModel uses singleflight to deduplicate
// concurrent loads of the same fingerprint, and the pool's worker semaphore
// limits actual inference concurrency to max_concurrent.
func (s *Server) BatchChatCompletion(
	ctx context.Context, req *connect.Request[rpc.BatchChatCompletionRequest],
) (*connect.Response[rpc.BatchChatCompletionResponse], error) {
	s.logger.Trace().Str("method", "BatchChatCompletion").Int("requests", len(req.Msg.Requests)).Msg("handling request")
	if len(req.Msg.Requests) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("requests: at least one request is required"))
	}

	responses := make([]*rpc.ChatCompletionResponse, len(req.Msg.Requests))
	errs := make([]error, len(req.Msg.Requests))

	wp := workerpool.New(len(req.Msg.Requests))
	for i, r := range req.Msg.Requests {
		_ = wp.Do(ctx, func(ctx context.Context) {
			responses[i], errs[i] = s.doCompletion(ctx, r)
		})
	}
	wp.Close()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	return connect.NewResponse(&rpc.BatchChatCompletionResponse{
		Responses: responses,
	}), nil
}

// Tokenize converts text to token IDs using the specified model's tokenizer.
func (s *Server) Tokenize(
	ctx context.Context, req *connect.Request[rpc.TokenizeRequest],
) (*connect.Response[rpc.TokenizeResponse], error) {
	s.logger.Trace().Str("method", "Tokenize").Str("model", req.Msg.Model).Msg("handling request")
	model, modelName, err := s.resolver.ResolveTokenize(req.Msg)
	if err != nil {
		return nil, err
	}

	tokens, err := model.Tokenize(ctx, req.Msg.Text)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			ctxerr.With(fmt.Errorf("tokenize failed for model %q: %w", modelName, err), map[string]any{"model": modelName}),
		)
	}

	return connect.NewResponse(&rpc.TokenizeResponse{Tokens: tokens}), nil
}

// Embedding handles a single embedding request.
func (s *Server) Embedding(
	ctx context.Context, req *connect.Request[rpc.EmbeddingRequest],
) (*connect.Response[rpc.EmbeddingResponse], error) {
	s.logger.Trace().Str("method", "Embedding").Msg("handling request")
	model, modelName, err := s.resolver.ResolveEmbedding(req.Msg)
	if err != nil {
		return nil, err
	}

	if req.Msg.Input == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("input: input text is required"))
	}

	metrics.ActiveRequestsInc(modelName)
	start := time.Now()
	result := model.Embed(ctx, req.Msg.Input)
	metrics.ActiveRequestsDec(modelName)
	metrics.InferenceDuration(modelName, time.Since(start).Seconds())

	if result.Error != nil {
		return nil, connect.NewError(connect.CodeInternal,
			ctxerr.With(fmt.Errorf("embedding failed: %w", result.Error), map[string]any{"model": modelName}),
		)
	}

	return connect.NewResponse(&rpc.EmbeddingResponse{
		Id:        uuid.NewString(),
		Model:     modelName,
		Embedding: result.Embedding,
	}), nil
}

// BatchEmbedding handles a batch embedding request.
func (s *Server) BatchEmbedding(
	ctx context.Context, req *connect.Request[rpc.BatchEmbeddingRequest],
) (*connect.Response[rpc.BatchEmbeddingResponse], error) {
	s.logger.Trace().Str("method", "BatchEmbedding").Int("inputs", len(req.Msg.Inputs)).Msg("handling request")
	model, modelName, err := s.resolver.ResolveBatchEmbedding(req.Msg)
	if err != nil {
		return nil, err
	}

	if len(req.Msg.Inputs) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("inputs: at least one input is required"))
	}

	metrics.ActiveRequestsInc(modelName)
	start := time.Now()
	result := model.EmbedBatch(ctx, req.Msg.Inputs)
	metrics.ActiveRequestsDec(modelName)
	metrics.InferenceDuration(modelName, time.Since(start).Seconds())

	if result.Error != nil {
		return nil, connect.NewError(connect.CodeInternal,
			ctxerr.With(fmt.Errorf("batch embedding failed: %w", result.Error), map[string]any{"model": modelName, "inputs": len(req.Msg.Inputs)}),
		)
	}

	embeddings := make([]*rpc.EmbeddingData, len(result.Embeddings))
	for i, emb := range result.Embeddings {
		embeddings[i] = &rpc.EmbeddingData{
			Index:     int32(i),
			Embedding: emb,
		}
	}

	return connect.NewResponse(&rpc.BatchEmbeddingResponse{
		Id:         uuid.NewString(),
		Model:      modelName,
		Embeddings: embeddings,
	}), nil
}

// --- Execute methods for queue processor ---
// These accept raw proto bytes, unmarshal, run inference, and return serialized response bytes.

// ExecuteChatCompletion processes a serialized ChatCompletionRequest and returns serialized response bytes.
func (s *Server) ExecuteChatCompletion(ctx context.Context, payload []byte) ([]byte, error) {
	req := &rpc.ChatCompletionRequest{}
	if err := proto.Unmarshal(payload, req); err != nil {
		return nil, fmt.Errorf("unmarshalling chat request: %w", err)
	}
	resp, err := s.doCompletion(ctx, req)
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

// SubmitRequest and GetResult are queue management RPCs handled at the scheduler level.
// These stubs satisfy the ConnectRPC interface but should never be called directly on Server.

func (s *Server) SubmitRequest(context.Context, *connect.Request[rpc.SubmitRequestRequest]) (*connect.Response[rpc.SubmitRequestResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("SubmitRequest is handled by the scheduler"))
}

func (s *Server) GetResult(context.Context, *connect.Request[rpc.GetResultRequest]) (*connect.Response[rpc.GetResultResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("GetResult is handled by the scheduler"))
}
