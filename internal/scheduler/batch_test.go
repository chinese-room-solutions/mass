package scheduler

import (
	"testing"

	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestBatchSize(t *testing.T) {
	chatBatch3, err := proto.Marshal(&rpc.BatchChatCompletionRequest{
		ModelConfig: &rpc.ChatModelConfig{
			Config: &rpc.ChatModelConfig_Llama{Llama: &rpc.LlamaChatConfig{Model: "m"}},
		},
		Items: []*rpc.BatchChatCompletionItem{{}, {}, {}},
	})
	require.NoError(t, err)

	chatBatchEmpty, err := proto.Marshal(&rpc.BatchChatCompletionRequest{})
	require.NoError(t, err)

	embedBatch5, err := proto.Marshal(&rpc.BatchEmbeddingRequest{
		Inputs: []string{"a", "b", "c", "d", "e"},
	})
	require.NoError(t, err)

	embedBatchEmpty, err := proto.Marshal(&rpc.BatchEmbeddingRequest{})
	require.NoError(t, err)

	tests := []struct {
		name    string
		reqType queue.RequestType
		payload []byte
		want    int
	}{
		{"single chat", queue.RequestTypeChatCompletion, []byte("anything"), 1},
		{"single embedding", queue.RequestTypeEmbedding, []byte("anything"), 1},
		{"tokenize", queue.RequestTypeTokenize, []byte("anything"), 1},
		{"chat batch of 3", queue.RequestTypeBatchChatCompletion, chatBatch3, 3},
		{"empty chat batch coerces to 1", queue.RequestTypeBatchChatCompletion, chatBatchEmpty, 1},
		{"malformed chat batch coerces to 1", queue.RequestTypeBatchChatCompletion, []byte{0xff, 0xff, 0xff}, 1},
		{"embedding batch of 5", queue.RequestTypeBatchEmbedding, embedBatch5, 5},
		{"empty embedding batch coerces to 1", queue.RequestTypeBatchEmbedding, embedBatchEmpty, 1},
		{"malformed embedding batch coerces to 1", queue.RequestTypeBatchEmbedding, []byte{0xff, 0xff, 0xff}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, batchSize(tt.reqType, tt.payload))
		})
	}
}

func TestIsEmbeddingBatch(t *testing.T) {
	require.True(t, isEmbeddingBatch(queue.RequestTypeBatchEmbedding))
	require.False(t, isEmbeddingBatch(queue.RequestTypeBatchChatCompletion))
	require.False(t, isEmbeddingBatch(queue.RequestTypeChatCompletion))
	require.False(t, isEmbeddingBatch(queue.RequestTypeEmbedding))
	require.False(t, isEmbeddingBatch(queue.RequestTypeTokenize))
}
