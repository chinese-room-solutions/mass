package workerpool_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/pkg/workerpool"
	"github.com/stretchr/testify/require"
)

func TestNew_PanicsOnZeroSize(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{"zero", 0},
		{"negative", -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Panics(t, func() { workerpool.New(tt.size) })
		})
	}
}

func TestDo_ExecutesConcurrently(t *testing.T) {
	const poolSize = 3
	wp := workerpool.New(poolSize)
	defer wp.Close()

	var running atomic.Int32
	var maxRunning atomic.Int32

	ctx := context.Background()
	for range 10 {
		err := wp.Do(ctx, func(_ context.Context) {
			cur := running.Add(1)
			// Track max concurrency observed.
			for {
				old := maxRunning.Load()
				if cur <= old || maxRunning.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			running.Add(-1)
		})
		require.NoError(t, err)
	}

	wp.Wait()
	require.Equal(t, int32(0), running.Load())
	require.LessOrEqual(t, maxRunning.Load(), int32(poolSize))
}

func TestDo_RespectsContextCancellation(t *testing.T) {
	wp := workerpool.New(1)
	defer wp.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Fill the pool.
	started := make(chan struct{})
	err := wp.Do(ctx, func(_ context.Context) {
		close(started)
		time.Sleep(100 * time.Millisecond)
	})
	require.NoError(t, err)

	<-started
	cancel()

	// Next submission should fail because ctx is cancelled.
	err = wp.Do(ctx, func(_ context.Context) {})
	require.ErrorIs(t, err, context.Canceled)
}

func TestDo_AlreadyCancelledContext(t *testing.T) {
	wp := workerpool.New(1)
	defer wp.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := wp.Do(ctx, func(_ context.Context) {
		t.Fatal("should not execute")
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestWait_BlocksUntilDone(t *testing.T) {
	wp := workerpool.New(2)

	var count atomic.Int32
	ctx := context.Background()

	for range 5 {
		err := wp.Do(ctx, func(_ context.Context) {
			time.Sleep(5 * time.Millisecond)
			count.Add(1)
		})
		require.NoError(t, err)
	}

	wp.Wait()
	require.Equal(t, int32(5), count.Load())
	wp.Close()
}

func TestDo_SingleWorker(t *testing.T) {
	wp := workerpool.New(1)
	defer wp.Close()

	var seq []int
	ctx := context.Background()

	for i := range 3 {
		err := wp.Do(ctx, func(_ context.Context) {
			seq = append(seq, i)
		})
		require.NoError(t, err)
	}

	wp.Wait()
	require.Len(t, seq, 3)
}
