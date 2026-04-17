package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type stubPredictor struct {
	name     string
	response string
}

func (s *stubPredictor) Submit(_ context.Context, req llm.CompletionRequest) llm.CompletionResult {
	return llm.CompletionResult{Text: s.response}
}

func (s *stubPredictor) Tokenize(_ context.Context, text string) ([]int32, error) {
	tokens := make([]int32, len(text))
	for i, c := range text {
		tokens[i] = c
	}
	return tokens, nil
}

func (s *stubPredictor) SubmitStream(_ context.Context, req llm.CompletionRequest) (<-chan llm.CompletionDelta, <-chan error) {
	deltaCh := make(chan llm.CompletionDelta, 1)
	errCh := make(chan error, 1)
	deltaCh <- llm.CompletionDelta{Content: s.response}
	close(deltaCh)
	return deltaCh, errCh
}

func (s *stubPredictor) Name() string { return s.name }

type stubEmbedder struct {
	name      string
	embedding []float32
}

func (s *stubEmbedder) Embed(_ context.Context, text string) llm.EmbeddingResult {
	return llm.EmbeddingResult{Embedding: s.embedding}
}

func (s *stubEmbedder) EmbedBatch(_ context.Context, texts []string) llm.BatchEmbeddingResult {
	embeddings := make([][]float32, len(texts))
	for i := range texts {
		embeddings[i] = s.embedding
	}
	return llm.BatchEmbeddingResult{Embeddings: embeddings}
}

func (s *stubEmbedder) Name() string { return s.name }

// slowPredictor is a stubPredictor that sleeps during Submit to allow
// concurrency measurement via atomic counters.
type slowPredictor struct {
	name       string
	response   string
	delay      time.Duration
	concurrent atomic.Int32
	maxConc    atomic.Int32
}

func (s *slowPredictor) Submit(_ context.Context, _ llm.CompletionRequest) llm.CompletionResult {
	cur := s.concurrent.Add(1)
	defer s.concurrent.Add(-1)
	// Update max concurrent high-water mark.
	for {
		old := s.maxConc.Load()
		if cur <= old || s.maxConc.CompareAndSwap(old, cur) {
			break
		}
	}
	time.Sleep(s.delay)
	return llm.CompletionResult{Text: s.response}
}

func (s *slowPredictor) Tokenize(_ context.Context, text string) ([]int32, error) {
	return nil, nil
}

func (s *slowPredictor) SubmitStream(_ context.Context, _ llm.CompletionRequest) (<-chan llm.CompletionDelta, <-chan error) {
	return nil, nil
}

func (s *slowPredictor) Name() string { return s.name }

// stubResolver implements ModelResolverInterface for testing.
type stubResolver struct {
	chatModels map[string]llm.PredictorInterface
	embedder   llm.EmbedderInterface
}

// chatModelName extracts the model name from a chat ModelConfig for stub
// lookup. The runtime resolver computes a fingerprint; the stub keys by
// the user-visible model field for readability.
func chatModelName(mc *rpc.ChatModelConfig) string {
	if mc == nil {
		return ""
	}
	return mc.GetLlama().GetModel()
}

func (r *stubResolver) ResolveChat(req *rpc.ChatCompletionRequest) (llm.PredictorInterface, string, error) {
	name := chatModelName(req.ModelConfig)
	if m, ok := r.chatModels[name]; ok {
		return m, name, nil
	}
	return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown model %s", name))
}

func (r *stubResolver) ResolveEmbedding(req *rpc.EmbeddingRequest) (llm.EmbedderInterface, string, error) {
	if r.embedder == nil {
		return nil, "", connect.NewError(connect.CodeUnimplemented, fmt.Errorf("embedding model is not loaded"))
	}
	return r.embedder, r.embedder.Name(), nil
}

func (r *stubResolver) ResolveBatchEmbedding(req *rpc.BatchEmbeddingRequest) (llm.EmbedderInterface, string, error) {
	if r.embedder == nil {
		return nil, "", connect.NewError(connect.CodeUnimplemented, fmt.Errorf("embedding model is not loaded"))
	}
	return r.embedder, r.embedder.Name(), nil
}

func (r *stubResolver) ResolveTokenize(req *rpc.TokenizeRequest) (llm.PredictorInterface, string, error) {
	if m, ok := r.chatModels[req.Model]; ok {
		return m, req.Model, nil
	}
	return nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown model %s", req.Model))
}

// chatCfg is a small builder that wraps a model name into the
// model_config envelope that all chat-kind requests now require.
func chatCfg(name string) *rpc.ChatModelConfig {
	return &rpc.ChatModelConfig{
		Config: &rpc.ChatModelConfig_Llama{
			Llama: &rpc.LlamaChatConfig{Model: name},
		},
	}
}

// embedCfg mirrors chatCfg for embedding requests. Returns a config naming
// the test fixture model so the stub resolver can find it.
func embedCfg() *rpc.EmbeddingModelConfig {
	return &rpc.EmbeddingModelConfig{
		Config: &rpc.EmbeddingModelConfig_Llama{
			Llama: &rpc.LlamaEmbeddingConfig{Model: "embedding"},
		},
	}
}

// ptr returns a pointer to v — handy for proto3 `optional` scalar fields.
func ptr[T any](v T) *T { return &v }

func newTestServer(t *testing.T) *Server {
	t.Helper()
	resolver := &stubResolver{
		chatModels: map[string]llm.PredictorInterface{
			"advert": &stubPredictor{name: "advert", response: "advert response"},
			"resume": &stubPredictor{name: "resume", response: "resume response"},
		},
		embedder: &stubEmbedder{name: "embedding", embedding: []float32{0.1, 0.2, 0.3}},
	}
	return NewServer(zerolog.Nop(), resolver)
}

func TestChatCompletion(t *testing.T) {
	s := newTestServer(t)

	t.Run("valid request", func(t *testing.T) {
		resp, err := s.ChatCompletion(context.Background(), connect.NewRequest(&rpc.ChatCompletionRequest{
			ModelConfig: chatCfg("advert"),
			Messages: []*rpc.ChatMessage{
				{Role: rpc.Role_ROLE_USER, Content: "Hello"},
			},
			Sampling: &rpc.SamplingParams{MaxTokens: ptr[int32](100)},
		}))
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, "advert", resp.Msg.Model)
		require.Equal(t, rpc.Role_ROLE_ASSISTANT, resp.Msg.Message.Role)
		require.Equal(t, "advert response", resp.Msg.Message.Content)
		require.Equal(t, rpc.FinishReason_FINISH_REASON_STOP, resp.Msg.FinishReason)
		require.NotEmpty(t, resp.Msg.Id)
	})

	t.Run("unknown model", func(t *testing.T) {
		_, err := s.ChatCompletion(context.Background(), connect.NewRequest(&rpc.ChatCompletionRequest{
			ModelConfig: chatCfg("nonexistent"),
			Messages: []*rpc.ChatMessage{
				{Role: rpc.Role_ROLE_USER, Content: "Hello"},
			},
		}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown model")
	})

	t.Run("empty messages", func(t *testing.T) {
		_, err := s.ChatCompletion(context.Background(), connect.NewRequest(&rpc.ChatCompletionRequest{
			ModelConfig: chatCfg("advert"),
			Messages:    nil,
		}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one message")
	})
}

func TestBatchChatCompletion(t *testing.T) {
	s := newTestServer(t)

	t.Run("multiple items", func(t *testing.T) {
		resp, err := s.BatchChatCompletion(context.Background(), connect.NewRequest(&rpc.BatchChatCompletionRequest{
			ModelConfig: chatCfg("advert"),
			Items: []*rpc.BatchChatCompletionItem{
				{Messages: []*rpc.ChatMessage{{Role: rpc.Role_ROLE_USER, Content: "First"}}},
				{Messages: []*rpc.ChatMessage{{Role: rpc.Role_ROLE_USER, Content: "Second"}}},
			},
		}))
		require.NoError(t, err)
		require.Len(t, resp.Msg.Responses, 2)
		require.Equal(t, "advert", resp.Msg.Responses[0].Model)
		require.Equal(t, "advert response", resp.Msg.Responses[0].Message.Content)
		require.Equal(t, "advert", resp.Msg.Responses[1].Model)
		require.Equal(t, "advert response", resp.Msg.Responses[1].Message.Content)
	})

	t.Run("empty items", func(t *testing.T) {
		_, err := s.BatchChatCompletion(context.Background(), connect.NewRequest(&rpc.BatchChatCompletionRequest{
			ModelConfig: chatCfg("advert"),
			Items:       nil,
		}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one item")
	})

	t.Run("invalid model", func(t *testing.T) {
		_, err := s.BatchChatCompletion(context.Background(), connect.NewRequest(&rpc.BatchChatCompletionRequest{
			ModelConfig: chatCfg("bad"),
			Items: []*rpc.BatchChatCompletionItem{
				{Messages: []*rpc.ChatMessage{{Role: rpc.Role_ROLE_USER, Content: "OK"}}},
				{Messages: []*rpc.ChatMessage{{Role: rpc.Role_ROLE_USER, Content: "Fail"}}},
			},
		}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown model")
	})
}

func TestBatchChatCompletion_Concurrent(t *testing.T) {
	pred := &slowPredictor{name: "slow", response: "done", delay: 50 * time.Millisecond}
	resolver := &stubResolver{
		chatModels: map[string]llm.PredictorInterface{"slow": pred},
	}
	s := NewServer(zerolog.Nop(), resolver)

	items := make([]*rpc.BatchChatCompletionItem, 3)
	for i := range items {
		items[i] = &rpc.BatchChatCompletionItem{
			Messages: []*rpc.ChatMessage{{Role: rpc.Role_ROLE_USER, Content: fmt.Sprintf("msg-%d", i)}},
		}
	}

	resp, err := s.BatchChatCompletion(context.Background(), connect.NewRequest(&rpc.BatchChatCompletionRequest{
		ModelConfig: chatCfg("slow"),
		Items:       items,
	}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Responses, 3)

	for i, r := range resp.Msg.Responses {
		require.Equal(t, "slow", r.Model, "response %d model", i)
		require.Equal(t, "done", r.Message.Content, "response %d content", i)
	}

	// All 3 should have run concurrently — max concurrent must exceed 1.
	require.Greater(t, pred.maxConc.Load(), int32(1), "items should execute concurrently")
}

func TestBatchChatCompletion_SingleItemBatch(t *testing.T) {
	s := newTestServer(t)

	resp, err := s.BatchChatCompletion(context.Background(), connect.NewRequest(&rpc.BatchChatCompletionRequest{
		ModelConfig: chatCfg("advert"),
		Items: []*rpc.BatchChatCompletionItem{
			{Messages: []*rpc.ChatMessage{{Role: rpc.Role_ROLE_USER, Content: "solo"}}},
		},
	}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Responses, 1)
	require.Equal(t, "advert", resp.Msg.Responses[0].Model)
	require.Equal(t, "advert response", resp.Msg.Responses[0].Message.Content)
	require.Equal(t, rpc.FinishReason_FINISH_REASON_STOP, resp.Msg.Responses[0].FinishReason)
	require.NotEmpty(t, resp.Msg.Responses[0].Id)
}

func TestBatchChatCompletion_ErrorInOneItem(t *testing.T) {
	s := newTestServer(t)

	_, err := s.BatchChatCompletion(context.Background(), connect.NewRequest(&rpc.BatchChatCompletionRequest{
		ModelConfig: chatCfg("advert"),
		Items: []*rpc.BatchChatCompletionItem{
			{Messages: []*rpc.ChatMessage{{Role: rpc.Role_ROLE_USER, Content: "good"}}},
			{Messages: nil}, // empty messages → error
			{Messages: []*rpc.ChatMessage{{Role: rpc.Role_ROLE_USER, Content: "also good"}}},
		},
	}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "at least one message")
}

func TestEmbedding(t *testing.T) {
	s := newTestServer(t)

	t.Run("valid request", func(t *testing.T) {
		resp, err := s.Embedding(context.Background(), connect.NewRequest(&rpc.EmbeddingRequest{
			ModelConfig: embedCfg(),
			Input:       "Hello world",
		}))
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, "embedding", resp.Msg.Model)
		require.Equal(t, []float32{0.1, 0.2, 0.3}, resp.Msg.Embedding)
		require.NotEmpty(t, resp.Msg.Id)
	})

	t.Run("not loaded", func(t *testing.T) {
		s := NewServer(zerolog.Nop(), &stubResolver{})
		_, err := s.Embedding(context.Background(), connect.NewRequest(&rpc.EmbeddingRequest{
			ModelConfig: embedCfg(),
			Input:       "Hello",
		}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not loaded")
	})

	t.Run("empty input", func(t *testing.T) {
		_, err := s.Embedding(context.Background(), connect.NewRequest(&rpc.EmbeddingRequest{
			ModelConfig: embedCfg(),
			Input:       "",
		}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "input text is required")
	})
}

func TestBatchEmbedding(t *testing.T) {
	s := newTestServer(t)

	t.Run("valid request", func(t *testing.T) {
		resp, err := s.BatchEmbedding(context.Background(), connect.NewRequest(&rpc.BatchEmbeddingRequest{
			ModelConfig: embedCfg(),
			Inputs:      []string{"first", "second", "third"},
		}))
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, "embedding", resp.Msg.Model)
		require.Len(t, resp.Msg.Embeddings, 3)
		for i, emb := range resp.Msg.Embeddings {
			require.Equal(t, int32(i), emb.Index)
			require.Equal(t, []float32{0.1, 0.2, 0.3}, emb.Embedding)
		}
		require.NotEmpty(t, resp.Msg.Id)
	})

	t.Run("not loaded", func(t *testing.T) {
		s := NewServer(zerolog.Nop(), &stubResolver{})
		_, err := s.BatchEmbedding(context.Background(), connect.NewRequest(&rpc.BatchEmbeddingRequest{
			ModelConfig: embedCfg(),
			Inputs:      []string{"hello"},
		}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not loaded")
	})

	t.Run("empty inputs", func(t *testing.T) {
		_, err := s.BatchEmbedding(context.Background(), connect.NewRequest(&rpc.BatchEmbeddingRequest{
			ModelConfig: embedCfg(),
			Inputs:      nil,
		}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one input")
	})
}
