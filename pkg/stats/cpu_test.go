package stats

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A sample that fails returns immediately — gopsutil bails out before its
// own interval sleep when the first counter read fails — so only the
// sampler's own pacing keeps the loop off a core. Bound the call count over
// a fixed window: unpaced, this loop managed millions of iterations a second.
func TestSampleCPU_FailedSampleDoesNotSpin(t *testing.T) {
	const interval = 20 * time.Millisecond
	const window = 10 * interval

	tests := []struct {
		name   string
		sample sampleFunc
	}{
		{
			name: "error",
			sample: func(context.Context, time.Duration, bool) ([]float64, error) {
				return nil, errors.New("reading CPU times")
			},
		},
		{
			name: "empty result",
			sample: func(context.Context, time.Duration, bool) ([]float64, error) {
				return nil, nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int64
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			exited := make(chan struct{})
			go func() {
				defer close(exited)
				sampleCPU(ctx, interval, func(ctx context.Context, d time.Duration, percpu bool) ([]float64, error) {
					calls.Add(1)
					return tt.sample(ctx, d, percpu)
				})
			}()

			time.Sleep(window)
			cancel()
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				t.Fatal("sampler did not exit on context cancellation")
			}

			n := calls.Load()
			require.Positive(t, n, "sampler must keep retrying after a failure")
			require.LessOrEqual(t, n, int64(3*window/interval),
				"retries must be paced by the sampling interval, not spun")
		})
	}
}

func TestSampleCPU_StoresSampleAndExitsOnCancel(t *testing.T) {
	const interval = 5 * time.Millisecond
	cpuUtilPct.Store(0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sampled := make(chan struct{}, 1)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		sampleCPU(ctx, interval, func(_ context.Context, d time.Duration, _ bool) ([]float64, error) {
			time.Sleep(d) // real samples block for the interval
			select {
			case sampled <- struct{}{}:
			default:
			}
			return []float64{42.5}, nil
		})
	}()

	<-sampled
	cancel()
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("sampler did not exit on context cancellation")
	}
	require.InDelta(t, 42.5, float64(cpuUtilPct.Load())/100, 0.001)
}
