# pkg/workerpool

Minimal semaphore-backed goroutine pool with context cancellation.

Used where MASS needs to cap parallelism on a burst of independent
tasks (inference submissions, downloads, device benchmarks) without
reaching for a heavier scheduler.

## Semantics

- `New(size)` — construct a pool with `size` concurrent slots. Panics
  if `size <= 0`.
- `Do(ctx, fn)` — blocks until a slot is available or `ctx` is
  cancelled, then runs `fn(ctx)` in a goroutine. Returns `ctx.Err()` on
  cancellation; error handling inside `fn` is the caller's problem.
- `Wait()` — wait for all in-flight tasks to complete.
- `Close()` — `Wait()` then release the semaphore channel.

## Usage

```go
wp := workerpool.New(runtime.NumCPU())
defer wp.Close()

for _, job := range jobs {
    job := job
    if err := wp.Do(ctx, func(ctx context.Context) {
        process(ctx, job)
    }); err != nil {
        return err // ctx cancelled
    }
}
wp.Wait()
```

## What this is not

- Not an `errgroup`: errors must be collected by the caller (channel,
  shared map, etc.). Keep that out of the pool so the pool stays dumb.
- Not a priority queue: slots are handed out in arrival order.
- Not re-usable after `Close()`.
