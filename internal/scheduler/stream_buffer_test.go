package scheduler

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Append assigns monotonic seqs and Attach with resumeSeq=0 replays
// every appended chunk in order, then closes on terminal frame.
func TestJobBuffer_AppendReplaysInOrder(t *testing.T) {
	buf := newJobBuffer()
	buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeChunk, Chunk: []byte("a")})
	buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeChunk, Chunk: []byte("b")})
	buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeCompleted, Final: []byte("done")})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := buf.Attach(ctx, 0)
	got := drainSeq(t, ch)
	require.Len(t, got, 3)
	require.Equal(t, uint64(0), got[0].Seq)
	require.Equal(t, []byte("a"), got[0].Chunk.Chunk)
	require.Equal(t, uint64(1), got[1].Seq)
	require.Equal(t, []byte("b"), got[1].Chunk.Chunk)
	require.Equal(t, uint64(2), got[2].Seq)
	require.Equal(t, worker.JobChunkTypeCompleted, got[2].Chunk.Type)
}

// Attach with resumeSeq > 0 starts after the requested seq.
func TestJobBuffer_AttachResumeSkipsReplayed(t *testing.T) {
	buf := newJobBuffer()
	for i := range 5 {
		buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeChunk, Chunk: []byte{byte('a' + i)}})
	}
	buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeCompleted})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := buf.Attach(ctx, 3)
	got := drainSeq(t, ch)
	require.Len(t, got, 3) // seqs 3, 4, 5
	require.Equal(t, uint64(3), got[0].Seq)
	require.Equal(t, []byte("d"), got[0].Chunk.Chunk)
	require.Equal(t, uint64(5), got[2].Seq)
	require.Equal(t, worker.JobChunkTypeCompleted, got[2].Chunk.Type)
}

// Drop-oldest is applied when the byte budget is exceeded.
func TestJobBuffer_DropOldestOnByteCap(t *testing.T) {
	buf := newJobBuffer()
	const chunkSize = 64 << 10 // 64 KiB
	payload := make([]byte, chunkSize)
	// Push enough to overshoot the byte cap by a healthy margin so we can
	// assert against actual drops rather than racing the boundary.
	totalChunks := (streamBufferMaxBytes / chunkSize) + 50
	for range totalChunks {
		buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeChunk, Chunk: payload})
	}
	require.LessOrEqual(t, buf.bytes, streamBufferMaxBytes, "bytes must stay under cap")
	require.Greater(t, buf.floor, uint64(0), "floor should advance past the dropped chunks")
}

// A consumer attaching with resumeSeq below the buffer floor receives a
// synthetic error chunk so the gateway sees the loss instead of silently
// skipping past dropped frames.
func TestJobBuffer_AttachBelowFloorYieldsError(t *testing.T) {
	buf := newJobBuffer()
	const chunkSize = 64 << 10
	payload := make([]byte, chunkSize)
	for range (streamBufferMaxBytes / chunkSize) + 10 {
		buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeChunk, Chunk: payload})
	}
	require.Greater(t, buf.floor, uint64(0))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := buf.Attach(ctx, 0) // way below floor
	got := drainSeq(t, ch)
	require.Len(t, got, 1)
	require.Equal(t, worker.JobChunkTypeError, got[0].Chunk.Type)
	require.True(t, strings.Contains(got[0].Chunk.ErrText, "overflow"), "error should mention overflow, got %q", got[0].Chunk.ErrText)
}

// Attach before any frames are appended waits for new chunks and a
// terminal frame, then closes.
func TestJobBuffer_AttachLivePump(t *testing.T) {
	buf := newJobBuffer()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch := buf.Attach(ctx, 0)

	go func() {
		buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeChunk, Chunk: []byte("x")})
		buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeCompleted, Final: []byte("done")})
	}()

	got := drainSeq(t, ch)
	require.Len(t, got, 2)
	require.Equal(t, []byte("x"), got[0].Chunk.Chunk)
	require.Equal(t, worker.JobChunkTypeCompleted, got[1].Chunk.Type)
}

// sweepReplayBuffers drops only post-terminal buffers older than ttl;
// inflight (non-terminal) buffers are left alone.
func TestScheduler_SweepReplayBuffers(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sweep.db")
	st, err := store.Open(store.DialectSQLite, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	pool := queue.NewPool(st.DB(), st.Dialect())
	results := queue.NewResultStore(st.DB(), st.Dialect())
	cfg := &config.Config{}
	s := New(cfg, zerolog.Nop(), worker.NewFleet())
	s.InitQueue(pool, results, st)

	terminalOld := newJobBuffer()
	terminalOld.Append(&worker.JobChunk{Type: worker.JobChunkTypeCompleted, Final: []byte("a")})
	// Backdate the terminal stamp.
	terminalOld.mu.Lock()
	terminalOld.terminalAt = time.Now().Add(-time.Minute)
	terminalOld.mu.Unlock()

	terminalFresh := newJobBuffer()
	terminalFresh.Append(&worker.JobChunk{Type: worker.JobChunkTypeCompleted, Final: []byte("b")})

	inflight := newJobBuffer()
	inflight.Append(&worker.JobChunk{Type: worker.JobChunkTypeChunk, Chunk: []byte("c")})

	s.jobBuffers["old"] = terminalOld
	s.jobBuffers["fresh"] = terminalFresh
	s.jobBuffers["inflight"] = inflight

	s.sweepReplayBuffers(30 * time.Second)
	s.jobsMu.Lock()
	_, oldStillThere := s.jobBuffers["old"]
	_, freshStillThere := s.jobBuffers["fresh"]
	_, inflightStillThere := s.jobBuffers["inflight"]
	s.jobsMu.Unlock()

	require.False(t, oldStillThere, "old terminal buffer should have been swept")
	require.True(t, freshStillThere, "fresh terminal buffer should survive within TTL")
	require.True(t, inflightStillThere, "inflight buffer should never be swept")
}

// A consumer attached to a just-terminal buffer must receive all
// buffered chunks plus the terminal frame, and the channel must close
// cleanly — even when sweepReplayBuffers fires concurrently and
// removes the buffer from the scheduler's map. The buffer object lives
// as long as a reference is held; sweep deletes the MAP entry, not the
// underlying buffer. A refactor that closes/zeros the buffer in sweep
// would silently break gateway resume-after-brief-disconnect and would
// fire under -race.
//
// Test shape:
//  1. Stage a buffer with N chunks + terminal frame and put it in
//     s.jobBuffers.
//  2. Attach a consumer (resumeSeq=0). The Attach goroutine starts
//     reading buffered chunks into the output channel.
//  3. Concurrently call sweepReplayBuffers with a TTL short enough to
//     evict — racing with the consumer.
//  4. Assert: consumer receives every chunk + terminal in order,
//     channel closes cleanly. After both finish, a fresh StreamChunks
//     for the same RequestID returns ErrUnknown — sweep took effect.
func TestSweepReplayBuffers_DoesNotRaceLiveAttachConsumer(t *testing.T) {
	s, _ := newTestScheduler(t)

	const requestID = "rid-race"
	buf := newJobBuffer()
	for i := range 8 {
		buf.Append(&worker.JobChunk{
			Type:  worker.JobChunkTypeChunk,
			Chunk: []byte{byte('a' + i)},
		})
	}
	buf.Append(&worker.JobChunk{
		Type:  worker.JobChunkTypeCompleted,
		Final: []byte("ok"),
	})
	// Backdate the terminal stamp far enough that any non-zero TTL
	// will evict — sweep's predicate is terminalAt.Before(cutoff).
	buf.mu.Lock()
	buf.terminalAt = time.Now().Add(-time.Minute)
	buf.mu.Unlock()

	s.jobsMu.Lock()
	s.jobBuffers[requestID] = buf
	s.jobsMu.Unlock()

	// Consumer attaches and races with sweep.
	consumerCh := buf.Attach(t.Context(), 0)

	// Sweep runs on its own goroutine — it must not panic and must not
	// invalidate the in-flight Attach's view of the buffer.
	sweepDone := make(chan struct{})
	go func() {
		s.sweepReplayBuffers(time.Second)
		close(sweepDone)
	}()

	// Drain the consumer.
	chunks := drainSeq(t, consumerCh)

	<-sweepDone

	// Every chunk + terminal must have arrived.
	require.Len(t, chunks, 9,
		"consumer must receive every appended chunk plus the terminal frame")
	for i := range 8 {
		require.Equal(t, worker.JobChunkTypeChunk, chunks[i].Chunk.Type,
			"chunk %d type", i)
		require.Equal(t, []byte{byte('a' + i)}, chunks[i].Chunk.Chunk,
			"chunk %d payload", i)
	}
	require.Equal(t, worker.JobChunkTypeCompleted, chunks[8].Chunk.Type,
		"final chunk must be the terminal frame")

	// After the race resolves, sweep must have actually swept — a
	// fresh StreamChunks for the same RequestID returns ErrUnknown.
	_, err := s.StreamChunks(context.Background(), requestID, 0)
	require.Error(t, err, "post-sweep StreamChunks must error: buffer is gone")
}

// The age reaper: a non-terminal buffer older than jobBufferMaxAge is
// reaped — dropped from the map, its result row failed, and a terminal
// error chunk appended so already-attached consumers close instead of
// pumping forever. Fresh non-terminal buffers and their result rows are
// untouched.
func TestScheduler_SweepReplayBuffers_ReapsExpiredNonTerminal(t *testing.T) {
	s, _ := newTestScheduler(t)

	require.NoError(t, s.results.Create("expired"))
	require.NoError(t, s.results.Create("live"))

	expired := newJobBuffer()
	expired.Append(&worker.JobChunk{Type: worker.JobChunkTypeChunk, Chunk: []byte("x")})
	// Backdate creation past the age cap.
	expired.createdAt = time.Now().Add(-jobBufferMaxAge - time.Minute)

	live := newJobBuffer()
	live.Append(&worker.JobChunk{Type: worker.JobChunkTypeChunk, Chunk: []byte("y")})

	s.jobsMu.Lock()
	s.jobBuffers["expired"] = expired
	s.jobBuffers["live"] = live
	s.jobsMu.Unlock()

	// Attach before the sweep: the consumer must observe the synthetic
	// terminal error frame and close.
	consumerCh := expired.Attach(t.Context(), 0)

	s.sweepReplayBuffers(30 * time.Second)

	s.jobsMu.Lock()
	_, expiredStillThere := s.jobBuffers["expired"]
	_, liveStillThere := s.jobBuffers["live"]
	s.jobsMu.Unlock()
	require.False(t, expiredStillThere, "expired non-terminal buffer must be reaped")
	require.True(t, liveStillThere, "fresh non-terminal buffer must survive")

	got := drainSeq(t, consumerCh)
	require.NotEmpty(t, got, "consumer must receive the terminal error frame")
	last := got[len(got)-1]
	require.Equal(t, worker.JobChunkTypeError, last.Chunk.Type)
	require.Contains(t, last.Chunk.ErrText, "reaped")

	res, err := s.results.Get("expired")
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusError, res.Status, "expired job's result must be failed")
	require.Contains(t, res.Error, "reaped")

	liveRes, err := s.results.Get("live")
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusPending, liveRes.Status, "live job's result must stay pending")
}

func drainSeq(t *testing.T, ch <-chan SequencedChunk) []SequencedChunk {
	t.Helper()
	var out []SequencedChunk
	timeout := time.After(2 * time.Second)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, c)
		case <-timeout:
			t.Fatalf("drainSeq: timed out after %d chunks", len(out))
		}
	}
}
