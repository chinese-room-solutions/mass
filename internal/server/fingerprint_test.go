package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/stretchr/testify/require"
)

// TestWithFingerprint covers the helper directly, including the empty-string
// branch (no header set) so future refactors can't silently start emitting
// blank fingerprint headers downstream callers might key on.
func TestWithFingerprint(t *testing.T) {
	t.Run("non-empty stamps header", func(t *testing.T) {
		resp := withFingerprint(&rpc.ChatCompletionResponse{Id: "x"}, "abc123")
		require.Equal(t, "abc123", resp.Header().Get(fingerprintHeader))
		require.Equal(t, "X-Mass-Fingerprint", fingerprintHeader)
	})

	t.Run("empty fingerprint omits header", func(t *testing.T) {
		resp := withFingerprint(&rpc.ChatCompletionResponse{Id: "x"}, "")
		require.Empty(t, resp.Header().Get(fingerprintHeader))
		require.Empty(t, resp.Header().Values(fingerprintHeader),
			"empty fingerprint must not set the header at all")
	})
}

// TestChatCompletionSetsFingerprintHeader exercises the end-to-end
// contract: every successful chat completion must echo the resolver's
// fingerprint in the X-Mass-Fingerprint header so callers can pin
// follow-up requests to the same loaded instance.
func TestChatCompletionSetsFingerprintHeader(t *testing.T) {
	s := newTestServer(t)
	maxTokens := int32(1)
	resp, err := s.ChatCompletion(context.Background(), connect.NewRequest(&rpc.ChatCompletionRequest{
		ModelConfig: chatCfg("advert"),
		Messages:    []*rpc.ChatMessage{{Role: rpc.Role_ROLE_USER, Content: "hi"}},
		Sampling:    &rpc.SamplingParams{MaxTokens: &maxTokens},
	}))
	require.NoError(t, err)
	// stubResolver returns the model name as the fingerprint.
	require.Equal(t, "advert", resp.Header().Get(fingerprintHeader))
}

// TestBatchChatCompletionSetsFingerprintHeader verifies the batch-level
// fingerprint surfacing logic — when every item shares the same loaded
// model, the batch response must carry that one fingerprint.
func TestBatchChatCompletionSetsFingerprintHeader(t *testing.T) {
	s := newTestServer(t)
	resp, err := s.BatchChatCompletion(context.Background(), connect.NewRequest(&rpc.BatchChatCompletionRequest{
		ModelConfig: chatCfg("advert"),
		Items: []*rpc.BatchChatCompletionItem{
			{Messages: []*rpc.ChatMessage{{Role: rpc.Role_ROLE_USER, Content: "1"}}},
			{Messages: []*rpc.ChatMessage{{Role: rpc.Role_ROLE_USER, Content: "2"}}},
		},
	}))
	require.NoError(t, err)
	require.Equal(t, "advert", resp.Header().Get(fingerprintHeader))
}

// TestEmbeddingSetsFingerprintHeader confirms the same convention for the
// embedding-side response wrapper.
func TestEmbeddingSetsFingerprintHeader(t *testing.T) {
	s := newTestServer(t)
	resp, err := s.Embedding(context.Background(), connect.NewRequest(&rpc.EmbeddingRequest{
		ModelConfig: embedCfg(),
		Input:       "hello",
	}))
	require.NoError(t, err)
	require.Equal(t, "embedding", resp.Header().Get(fingerprintHeader))
}

// TestBatchEmbeddingSetsFingerprintHeader covers the batch embedding path.
func TestBatchEmbeddingSetsFingerprintHeader(t *testing.T) {
	s := newTestServer(t)
	resp, err := s.BatchEmbedding(context.Background(), connect.NewRequest(&rpc.BatchEmbeddingRequest{
		ModelConfig: embedCfg(),
		Inputs:      []string{"a", "b"},
	}))
	require.NoError(t, err)
	require.Equal(t, "embedding", resp.Header().Get(fingerprintHeader))
}
