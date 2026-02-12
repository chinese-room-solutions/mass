package llm

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mockPredictor implements PredictorInterface for testing.
type mockPredictor struct {
	name      string
	delay     time.Duration
	response  string
	callCount atomic.Int32
}

func (m *mockPredictor) Submit(ctx context.Context, req CompletionRequest) CompletionResult {
	m.callCount.Add(1)
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return CompletionResult{Error: ctx.Err()}
		}
	}
	return CompletionResult{Text: m.response}
}

func (m *mockPredictor) SubmitStream(ctx context.Context, req CompletionRequest) (<-chan CompletionDelta, <-chan error) {
	deltaCh := make(chan CompletionDelta)
	errCh := make(chan error, 1)
	go func() {
		defer close(deltaCh)
		result := m.Submit(ctx, req)
		if result.Error != nil {
			errCh <- result.Error
			return
		}
		deltaCh <- CompletionDelta{Content: result.Text}
	}()
	return deltaCh, errCh
}

func (m *mockPredictor) Tokenize(context.Context, string) ([]int32, error) {
	return nil, nil
}

func (m *mockPredictor) Name() string {
	return m.name
}

func TestMockPredictor(t *testing.T) {
	m := &mockPredictor{name: "test", response: "hello"}
	req := CompletionRequest{
		Messages: []ChatMessage{{Role: "user", Content: "hi"}},
	}
	result := m.Submit(context.Background(), req)
	require.NoError(t, result.Error)
	require.Equal(t, "hello", result.Text)
	require.Equal(t, int32(1), m.callCount.Load())
}

func TestMockPredictorContextCancel(t *testing.T) {
	m := &mockPredictor{name: "test", delay: time.Second, response: "hello"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := CompletionRequest{
		Messages: []ChatMessage{{Role: "user", Content: "hi"}},
	}
	result := m.Submit(ctx, req)
	require.Error(t, result.Error)
	require.ErrorIs(t, result.Error, context.Canceled)
}

func TestMockPredictorConcurrency(t *testing.T) {
	m := &mockPredictor{name: "test", delay: 10 * time.Millisecond, response: "ok"}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := CompletionRequest{
				Messages: []ChatMessage{{Role: "user", Content: "test"}},
			}
			result := m.Submit(context.Background(), req)
			require.NoError(t, result.Error)
			require.Equal(t, "ok", result.Text)
		}()
	}
	wg.Wait()
	require.Equal(t, int32(10), m.callCount.Load())
}
