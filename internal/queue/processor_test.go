package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/rpc"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestProcessor_ProcessesAndStoresResult(t *testing.T) {
	db := setupTestDB(t)
	q := New(db)
	results := NewResultStore(db)
	ctx := context.Background()

	// Submit a request.
	req := &rpc.ChatCompletionRequest{Model: "test", Messages: []*rpc.ChatMessage{{Role: "user", Content: "hi"}}}
	sub, err := q.SubmitChatCompletion(ctx, req, PriorityMedium)
	require.NoError(t, err)

	// Create the result entry (as the server would).
	err = results.Create(sub.ID, sub.RequestHash)
	require.NoError(t, err)

	// Build a mock response.
	mockResp := &rpc.ChatCompletionResponse{
		Id:    "resp-1",
		Model: "test",
		Message: &rpc.ChatMessage{
			Role:    "assistant",
			Content: "hello back",
		},
	}
	respBytes, _ := proto.Marshal(mockResp)

	// Create processor with a stub execute function.
	proc := NewProcessor(ProcessorOpts{
		Queue:   q,
		Results: results,
		ExecuteFn: func(ctx context.Context, env Envelope) ([]byte, error) {
			require.Equal(t, RequestTypeChatCompletion, env.Type)
			return respBytes, nil
		},
		Logger:       zerolog.Nop(),
		PollInterval: 10 * time.Millisecond,
		ResultTTL:    24 * time.Hour,
	})

	// Run the processor briefly.
	procCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	go proc.Run(procCtx)

	// Wait for the result.
	r, err := results.WaitForResult(sub.ID, 10*time.Millisecond, procCtx.Done())
	require.NoError(t, err)
	require.Equal(t, ResultStatusDone, r.Status)
	require.Equal(t, respBytes, r.Body)
}

func TestProcessor_HandlesError(t *testing.T) {
	db := setupTestDB(t)
	q := New(db)
	results := NewResultStore(db)
	ctx := context.Background()

	sub, err := q.SubmitEmbedding(ctx, &rpc.EmbeddingRequest{Model: "test", Input: "hello"}, PriorityMedium)
	require.NoError(t, err)
	err = results.Create(sub.ID, sub.RequestHash)
	require.NoError(t, err)

	proc := NewProcessor(ProcessorOpts{
		Queue:   q,
		Results: results,
		ExecuteFn: func(ctx context.Context, env Envelope) ([]byte, error) {
			return nil, fmt.Errorf("model not found")
		},
		Logger:       zerolog.Nop(),
		PollInterval: 10 * time.Millisecond,
		ResultTTL:    24 * time.Hour,
	})

	procCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	go proc.Run(procCtx)

	r, err := results.WaitForResult(sub.ID, 10*time.Millisecond, procCtx.Done())
	require.NoError(t, err)
	require.Equal(t, ResultStatusError, r.Status)
	require.Contains(t, r.Error, "model not found")
}

func TestProcessor_EmptyQueue(t *testing.T) {
	db := setupTestDB(t)
	q := New(db)
	results := NewResultStore(db)

	proc := NewProcessor(ProcessorOpts{
		Queue:   q,
		Results: results,
		ExecuteFn: func(ctx context.Context, env Envelope) ([]byte, error) {
			t.Fatal("should not be called on empty queue")
			return nil, nil
		},
		Logger:       zerolog.Nop(),
		PollInterval: 10 * time.Millisecond,
		ResultTTL:    24 * time.Hour,
	})

	// Run briefly — should not panic or call execute.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	proc.Run(ctx)
}
