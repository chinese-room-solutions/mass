package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/stretchr/testify/require"
)

// SetWorkerEnabledFn must actually flow into the dispatcher's gating
// decisions: a worker that the operator marked disabled must drop out
// of pickWorkerQueue's candidate set, even when otherwise schedulable.
func TestSetWorkerEnabledFn_DropsCandidate(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Flops: 100, BenchedAt: time.Now(),
	}))
	w := worker.NewFakeStreamWorker("w1", runtimeName, gpu1(), time.Now())
	w.SetFakeCapacity(4)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	// Baseline: worker is enabled, picked.
	target, _ := s.pickWorkerQueue(queue.Envelope{
		RuntimeName: runtimeName, ModelID: modelID, Cost: float64(100),
	})
	require.NotNil(t, target)
	require.Equal(t, "worker|w1", target.name)

	// Operator disables every worker.
	s.SetWorkerEnabledFn(func(string) bool { return false })

	target, _ = s.pickWorkerQueue(queue.Envelope{
		RuntimeName: runtimeName, ModelID: modelID, Cost: float64(100),
	})
	require.Nil(t, target, "operator-disabled worker must drop out of candidates")

	// Re-enable lifts the gate.
	s.SetWorkerEnabledFn(func(string) bool { return true })
	target, _ = s.pickWorkerQueue(queue.Envelope{
		RuntimeName: runtimeName, ModelID: modelID, Cost: float64(100),
	})
	require.NotNil(t, target, "re-enabling must restore candidacy")
}

// WorkersWithModel filters online workers of runtimeName to those that
// report modelID resident. Empty modelID short-circuits to "all workers
// of the runtime". Non-matching workers (wrong runtime, model absent)
// drop out.
func TestWorkersWithModel(t *testing.T) {
	const (
		runtimeName = "llama-cpp"
		other       = "other-runtime"
		modelID     = "m-1"
	)
	s, _ := newTestScheduler(t)
	// w1: matching runtime, has the model
	w1 := worker.NewFakeStreamWorker("w1", runtimeName, gpu1(), time.Now())
	w1.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	require.NoError(t, s.workers.Register(w1))
	// w2: matching runtime, model absent
	w2 := worker.NewFakeStreamWorker("w2", runtimeName, gpu1(), time.Now())
	require.NoError(t, s.workers.Register(w2))
	// w3: wrong runtime, has the model — should still drop (runtime filter
	// runs first inside WorkersForRuntime)
	w3 := worker.NewFakeStreamWorker("w3", other, gpu1(), time.Now())
	w3.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	require.NoError(t, s.workers.Register(w3))

	got := s.WorkersWithModel(runtimeName, modelID)
	require.Len(t, got, 1, "only w1 matches (right runtime + model resident)")
	require.Equal(t, "w1", got[0].ID())

	// Empty modelID returns all workers of the runtime, regardless of
	// what's loaded.
	got = s.WorkersWithModel(runtimeName, "")
	require.Len(t, got, 2, "empty modelID returns all workers of the runtime")
}

// StreamChunks must error on unknown requestID (Submit was never called
// or the buffer was already swept) and must attach to the per-job ring
// buffer for known requests.
func TestStreamChunks_UnknownAndKnown(t *testing.T) {
	s, _ := newTestScheduler(t)
	ctx := context.Background()

	_, err := s.StreamChunks(ctx, "no-such-id", 0)
	require.Error(t, err, "unknown request_id must error")

	// Seed a buffer directly so we can attach without driving Submit.
	const reqID = "req-x"
	buf := newJobBuffer()
	s.jobsMu.Lock()
	s.jobBuffers[reqID] = buf
	s.jobsMu.Unlock()

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := s.StreamChunks(streamCtx, reqID, 0)
	require.NoError(t, err)
	require.NotNil(t, ch, "known request must yield a chunk channel")

	// Push one chunk and a terminal frame; the consumer must see both.
	buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeChunk, Chunk: []byte("hi")})
	buf.Append(&worker.JobChunk{Type: worker.JobChunkTypeCompleted, Final: []byte("done")})

	got := drainChunks(t, ch, 2)
	require.Len(t, got, 2)
	require.Equal(t, worker.JobChunkTypeChunk, got[0].Chunk.Type)
	require.Equal(t, worker.JobChunkTypeCompleted, got[1].Chunk.Type)
}

// dropJob deletes the per-request buffer. After dropJob, StreamChunks
// for the same id must return ErrUnknown shape. Idempotent: a second
// dropJob on the same id is a no-op.
func TestDropJob(t *testing.T) {
	s, _ := newTestScheduler(t)
	const reqID = "req-drop"
	s.jobsMu.Lock()
	s.jobBuffers[reqID] = newJobBuffer()
	s.jobsMu.Unlock()

	_, err := s.StreamChunks(context.Background(), reqID, 0)
	require.NoError(t, err)

	s.dropJob(reqID)
	_, err = s.StreamChunks(context.Background(), reqID, 0)
	require.Error(t, err, "after dropJob, request_id must be unknown")

	// Second drop is a no-op (no panic on missing key).
	s.dropJob(reqID)
}

// EvictModel calls UnloadModel on every worker that reports modelID
// resident — or just the workerID-scoped one when workerID != "".
func TestEvictModel(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"

	t.Run("evicts every resident worker when workerID is empty", func(t *testing.T) {
		s, _ := newTestScheduler(t)
		w1 := newEvictFakeWorker(t, s, "w1", modelID, true)
		w2 := newEvictFakeWorker(t, s, "w2", modelID, true)
		// w3 has the runtime but not the model — must not be touched.
		w3 := newEvictFakeWorker(t, s, "w3", modelID, false)

		go ackUnload(t, w1)
		go ackUnload(t, w2)
		n, err := s.EvictModel(runtimeName, modelID, "")
		require.NoError(t, err)
		require.Equal(t, 2, n, "two resident workers must both unload")
		require.Equal(t, int32(1), w1.unloadCalls.Load())
		require.Equal(t, int32(1), w2.unloadCalls.Load())
		require.Equal(t, int32(0), w3.unloadCalls.Load(), "non-resident must not be touched")
	})

	t.Run("scopes to one worker when workerID is set", func(t *testing.T) {
		s, _ := newTestScheduler(t)
		w1 := newEvictFakeWorker(t, s, "w1", modelID, true)
		w2 := newEvictFakeWorker(t, s, "w2", modelID, true)

		go ackUnload(t, w2)
		n, err := s.EvictModel(runtimeName, modelID, "w2")
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.Equal(t, int32(0), w1.unloadCalls.Load(), "non-targeted worker must not be touched")
		require.Equal(t, int32(1), w2.unloadCalls.Load())
	})

	t.Run("returns 0 when nobody has the model", func(t *testing.T) {
		s, _ := newTestScheduler(t)
		_ = newEvictFakeWorker(t, s, "w1", modelID, false)
		n, err := s.EvictModel(runtimeName, modelID, "")
		require.NoError(t, err)
		require.Equal(t, 0, n)
	})
}

// CleanupResults clears terminal rows past the TTL and leaves live rows
// alone regardless of age. Uses a real result store so we exercise the
// SQL path.
func TestCleanupResults(t *testing.T) {
	s, st := newTestScheduler(t)

	// One terminal row and one still-pending row, both older than the
	// TTL. Only the terminal one may be pruned — a live job keeps its
	// result row no matter how long it has been queued.
	require.NoError(t, s.results.Create("old-done"))
	require.NoError(t, s.results.Complete("old-done", []byte("body")))
	require.NoError(t, s.results.Create("old-pending"))

	// Backdate both rows past the default 24h TTL. Goes around the
	// ResultStore API (which has no backdating knob) because this is a
	// test-only contrivance.
	past := time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano)
	_, err := st.DB().Exec(
		`UPDATE queue_results SET created_at = ? WHERE id IN (?, ?)`,
		past, "old-done", "old-pending",
	)
	require.NoError(t, err)
	_, err = st.DB().Exec(
		`UPDATE queue_results SET completed_at = ? WHERE id = ?`,
		past, "old-done",
	)
	require.NoError(t, err)

	n, err := s.CleanupResults(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), n, "exactly the terminal backdated row must be deleted")

	// The live row survives regardless of age.
	res, err := s.results.Get("old-pending")
	require.NoError(t, err)
	require.NotNil(t, res, "live pending row must survive TTL cleanup")
	old, err := s.results.Get("old-done")
	require.NoError(t, err)
	require.Nil(t, old, "stale terminal row must be gone")
}

// evictIdleOnce sweeps loaded models with Active=0 and IdleSince older
// than the cutoff, unloading them. Models with active jobs OR fresh
// IdleSince stay loaded.
func TestEvictIdleOnce(t *testing.T) {
	s, _ := newTestScheduler(t)

	// w-stale: one model idle for 2 minutes — must be unloaded.
	// w-busy: model has Active>0 — must NOT be unloaded.
	// w-fresh: model IdleSince is recent — must NOT be unloaded.
	stale := newEvictFakeWorker(t, s, "w-stale", "m-stale", true)
	stale.w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: "m-stale", PoolSize: 1, Active: 0,
		IdleSince: time.Now().Add(-2 * time.Minute),
	}})
	busy := newEvictFakeWorker(t, s, "w-busy", "m-busy", true)
	busy.w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: "m-busy", PoolSize: 1, Active: 1,
		IdleSince: time.Now().Add(-2 * time.Minute), // ignored due to Active>0
	}})
	fresh := newEvictFakeWorker(t, s, "w-fresh", "m-fresh", true)
	fresh.w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: "m-fresh", PoolSize: 1, Active: 0,
		IdleSince: time.Now().Add(-1 * time.Second), // newer than cutoff
	}})

	go ackUnload(t, stale)

	// TTL = 1 minute. Cutoff = now - 1min. m-stale is 2 min old → evict.
	s.evictIdleOnce(time.Minute)

	require.Equal(t, int32(1), stale.unloadCalls.Load(), "stale model must be evicted")
	require.Equal(t, int32(0), busy.unloadCalls.Load(), "active model must NOT be evicted")
	require.Equal(t, int32(0), fresh.unloadCalls.Load(), "fresh model must NOT be evicted")
}

// --- helpers ---

// drainChunks blocks until n chunks arrive on ch or 1s elapses.
func drainChunks(t *testing.T, ch <-chan SequencedChunk, n int) []SequencedChunk {
	t.Helper()
	out := make([]SequencedChunk, 0, n)
	deadline := time.After(1 * time.Second)
	for len(out) < n {
		select {
		case c, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, c)
		case <-deadline:
			t.Fatalf("drainChunks: only got %d/%d chunks within 1s", len(out), n)
		}
	}
	return out
}

// evictFakeWorker bundles a *StreamWorker with hooks for the eviction
// tests: counts UnloadModel calls, captures the job_id so the test can
// deliver an ack.
type evictFakeWorker struct {
	w             *worker.StreamWorker
	unloadCalls   atomic.Int32
	unloadJobID   chan string
	unloadAttempt sync.Once
}

// All callers run with runtimeName="llama-cpp" today. Hard-coded here
// so the helper signature stays minimal; if a future test needs a
// second runtime, lift the param back.
func newEvictFakeWorker(t *testing.T, s *Scheduler, id, modelID string, residentMatch bool) *evictFakeWorker {
	t.Helper()
	fw := &evictFakeWorker{unloadJobID: make(chan string, 1)}
	fw.w = worker.NewFakeStreamWorker(id, "llama-cpp", gpu1(), time.Now())
	fw.w.SetFakeCapacity(4)
	if residentMatch {
		fw.w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	}
	fw.w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if um := msg.GetUnloadModel(); um != nil {
			fw.unloadCalls.Add(1)
			fw.unloadAttempt.Do(func() {
				fw.unloadJobID <- um.GetJobId()
			})
		}
		return nil
	})
	require.NoError(t, s.workers.Register(fw.w))
	s.OnWorkerConnected(fw.w)
	return fw
}

// ackUnload delivers a success ack for the first UnloadModel call. Run
// on a goroutine so EvictModel doesn't block waiting for it.
func ackUnload(t *testing.T, fw *evictFakeWorker) {
	t.Helper()
	select {
	case jobID := <-fw.unloadJobID:
		fw.w.DeliverUnloadResult(jobID, "")
	case <-time.After(2 * time.Second):
		t.Errorf("ackUnload: no UnloadModel arrived for %s within 2s", fw.w.ID())
	}
}
