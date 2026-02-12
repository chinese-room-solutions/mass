package workerpool

import (
	"context"
	"sync"
)

// WorkerPool limits concurrent goroutine execution using a semaphore channel.
type WorkerPool struct {
	sem chan struct{}
	wg  sync.WaitGroup
}

// New creates a WorkerPool that allows at most size concurrent goroutines.
func New(size int) *WorkerPool {
	if size <= 0 {
		panic("worker pool size must be positive")
	}
	return &WorkerPool{sem: make(chan struct{}, size)}
}

// Do submits fn for execution, blocking until a slot is available or ctx is cancelled.
// The caller is responsible for handling errors inside fn (e.g. logging, storing results).
func (wp *WorkerPool) Do(ctx context.Context, fn func(ctx context.Context)) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	select {
	case wp.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
		defer func() { <-wp.sem }()
		fn(ctx)
	}()

	return nil
}

// Wait blocks until all submitted goroutines have completed.
func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}

// Close waits for all goroutines to finish and then releases resources.
func (wp *WorkerPool) Close() {
	wp.Wait()
	close(wp.sem)
}
