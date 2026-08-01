package scheduler

import (
	"context"
	"testing"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/stretchr/testify/require"
)

// stageRunningJob registers a fake worker, submits one job, dispatches it to
// in-flight, and returns the request_id plus a channel that receives the
// worker job_id of any HubCancelJob the worker is sent. The job stays running
// (no terminal frame) until the caller decides what to do.
func stageRunningJob(t *testing.T, s *Scheduler, st *store.Store) (requestID string, cancels <-chan string, w *worker.StreamWorker) {
	t.Helper()
	const runtimeName, modelID = "llama-cpp", "m-1"
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	cancelCh := make(chan string, 4)
	w = worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(4)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if cj := msg.GetCancelJob(); cj != nil {
			select {
			case cancelCh <- cj.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	rid, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName, ModelID: modelID, Payload: []byte("p"),
		Cost: 100, CostAxis: "q4k_matvec",
	})
	require.NoError(t, err)
	s.dispatchPass(context.Background())
	waitDispatchIdle(t, s)
	return rid, cancelCh, w
}

func TestGetResult(t *testing.T) {
	s, _ := newTestScheduler(t)

	t.Run("unknown id returns ErrNoResult", func(t *testing.T) {
		_, err := s.GetResult("nope")
		require.ErrorIs(t, err, ErrNoResult)
	})

	t.Run("pending after create", func(t *testing.T) {
		require.NoError(t, s.results.Create("rid-pending"))
		r, err := s.GetResult("rid-pending")
		require.NoError(t, err)
		require.Equal(t, queue.ResultStatusPending, r.Status)
		require.Empty(t, r.Body)
	})

	t.Run("done carries body", func(t *testing.T) {
		require.NoError(t, s.results.Create("rid-done"))
		require.NoError(t, s.results.Complete("rid-done", []byte("the-answer")))
		r, err := s.GetResult("rid-done")
		require.NoError(t, err)
		require.Equal(t, queue.ResultStatusDone, r.Status)
		require.Equal(t, []byte("the-answer"), r.Body)
	})

	t.Run("error carries message", func(t *testing.T) {
		require.NoError(t, s.results.Create("rid-err"))
		require.NoError(t, s.results.Fail("rid-err", "boom"))
		r, err := s.GetResult("rid-err")
		require.NoError(t, err)
		require.Equal(t, queue.ResultStatusError, r.Status)
		require.Equal(t, "boom", r.Error)
	})
}

func TestCancelByRequestID(t *testing.T) {
	t.Run("pending row cancelled via global scan", func(t *testing.T) {
		s, _ := newTestScheduler(t)
		ctx := context.Background()
		const rid = "rid-cancel-pending"

		require.NoError(t, s.results.Create(rid))
		_, err := s.globalQ.Submit(ctx, queue.Envelope{
			Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "m",
			RequestID: rid, Payload: []byte("p"),
		})
		require.NoError(t, err)

		require.NoError(t, s.CancelByRequestID(ctx, rid))

		// Row gone from global, result marked error ("cancelled by operator").
		msgs, err := s.globalQ.Peek(ctx, queuePeekLimit)
		require.NoError(t, err)
		require.Empty(t, msgs)
		r, err := s.GetResult(rid)
		require.NoError(t, err)
		require.Equal(t, queue.ResultStatusError, r.Status)
	})

	t.Run("pending cancel completes despite a cancelled caller context", func(t *testing.T) {
		s, _ := newTestScheduler(t)
		const rid = "rid-cancel-deadctx"

		require.NoError(t, s.results.Create(rid))
		_, err := s.globalQ.Submit(context.Background(), queue.Envelope{
			Priority: queue.PriorityMedium, RuntimeName: "llama-cpp", ModelID: "m",
			RequestID: rid, Payload: []byte("p"),
		})
		require.NoError(t, err)

		// The gateway's DELETE handler context can expire mid-cancel (e.g. a
		// cold-start load makes the path slow). An already-cancelled caller
		// context must NOT strand the pending row — the cancel detaches with a
		// fresh context.
		dead, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, s.CancelByRequestID(dead, rid))

		msgs, err := s.globalQ.Peek(context.Background(), queuePeekLimit)
		require.NoError(t, err)
		require.Empty(t, msgs, "pending row must be deleted even with a dead caller context")
		r, err := s.GetResult(rid)
		require.NoError(t, err)
		require.Equal(t, queue.ResultStatusError, r.Status)
	})

	t.Run("no live job returns ErrNotInflight", func(t *testing.T) {
		s, _ := newTestScheduler(t)
		// Nothing pending, nothing inflight: pending scan misses, running
		// path reports not-inflight.
		err := s.CancelByRequestID(context.Background(), "ghost")
		require.ErrorIs(t, err, ErrNotInflight)
	})

	t.Run("completed job is not cancellable", func(t *testing.T) {
		s, _ := newTestScheduler(t)
		const rid = "rid-already-done"
		require.NoError(t, s.results.Create(rid))
		require.NoError(t, s.results.Complete(rid, []byte("done")))
		// No pending row, not inflight → ErrNotInflight.
		err := s.CancelByRequestID(context.Background(), rid)
		require.ErrorIs(t, err, ErrNotInflight)
	})

	t.Run("running job cancelled via inflight path", func(t *testing.T) {
		s, st := newTestScheduler(t)
		rid, cancels, _ := stageRunningJob(t, s, st)

		// The job is now in flight (dispatched, no terminal frame). It's no
		// longer a pending global row, so CancelByRequestID must fall through
		// to the running path and send the worker a HubCancelJob.
		require.NoError(t, s.CancelByRequestID(context.Background(), rid))

		select {
		case <-cancels:
			// HubCancelJob reached the worker — the running path fired.
		case <-time.After(2 * time.Second):
			t.Fatal("expected a HubCancelJob to be sent to the worker")
		}
	})

	t.Run("cancel during dispatch window is recorded, not lost", func(t *testing.T) {
		s, _ := newTestScheduler(t)
		ctx := context.Background()
		const rid = "rid-mid-dispatch"

		// Simulate the lease→LoadModel window: the row left the pending queue
		// (it's leased) but startInflight hasn't run yet. No pending row, no
		// inflight record.
		s.markDispatching(rid)

		// Cancel must NOT 404 here: it records intent and returns nil, instead
		// of falling through to the running path and reporting ErrNotInflight.
		require.NoError(t, s.CancelByRequestID(ctx, rid))

		// The recorded intent makes the subsequent promotion abort: startInflight
		// returns false (job never reaches AssignJob).
		require.False(t, s.startInflight(workerQueueName("w1"), rid, "m", "llama-cpp", "axis", 1.0, 0),
			"a cancel recorded mid-dispatch must abort the inflight promotion")

		// No inflight record was created.
		require.False(t, s.isInflightCancelled(rid))
		s.inflightMu.Lock()
		_, tracked := s.inflightByRequest[rid]
		s.inflightMu.Unlock()
		require.False(t, tracked, "aborted dispatch must not leave an inflight record")
	})

	t.Run("dispatch window with no cancel promotes normally", func(t *testing.T) {
		s, _ := newTestScheduler(t)
		const rid = "rid-clean-dispatch"

		s.markDispatching(rid)
		// No cancel: promotion succeeds and the marker is consumed.
		require.True(t, s.startInflight(workerQueueName("w1"), rid, "m", "llama-cpp", "axis", 1.0, 0))
		s.inflightMu.Lock()
		_, tracked := s.inflightByRequest[rid]
		_, stillDispatching := s.dispatchingByRequest[rid]
		s.inflightMu.Unlock()
		require.True(t, tracked, "clean dispatch must record an inflight entry")
		require.False(t, stillDispatching, "startInflight must consume the dispatch marker")
	})
}

// A running job whose worker disconnects loses its processing status: the
// disconnect drain releases the global anchor for redistribution and reverts
// the result row to pending, so async pollers see "queued again", not a
// stuck "processing" with no worker behind it.
func TestWorkerDisconnect_RevertsResultToPending(t *testing.T) {
	s, st := newTestScheduler(t)
	rid, _, w := stageRunningJob(t, s, st)

	r, err := s.GetResult(rid)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusProcessing, r.Status)

	w.SetOffline()
	s.OnWorkerDisconnected("w1")

	r, err = s.GetResult(rid)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusPending, r.Status,
		"disconnect drain must revert an in-flight job's result to pending")
}

// The gateway's ?wait=1 path drains StreamChunks to the terminal frame and
// immediately reads the durable result, so the pump must store the result
// BEFORE publishing the terminal chunk. Regression: the store used to happen
// after the publish (with the throughput-correction DB upsert in between),
// and wait-callers routinely read a still-processing row for a completed job.
func TestAsyncWait_ResultDurableBeforeTerminalChunk(t *testing.T) {
	const runtimeName, modelID = "llama-cpp", "m-1"
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	assigns := make(chan string, 1)
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(4)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if aj := msg.GetAssignJob(); aj != nil {
			select {
			case assigns <- aj.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	rid, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName, ModelID: modelID, Payload: []byte("p"),
		Cost: 100, CostAxis: "q4k_matvec",
	})
	require.NoError(t, err)
	s.dispatchPass(context.Background())

	var workerJobID string
	select {
	case workerJobID = <-assigns:
	case <-time.After(2 * time.Second):
		t.Fatal("job never reached AssignJob")
	}

	// Attach before the terminal frame lands, like the gateway's wait drain.
	ch, err := s.StreamChunks(context.Background(), rid, 0)
	require.NoError(t, err)

	w.DeliverJobChunk(workerJobID, &worker.JobChunk{
		Type: worker.JobChunkTypeCompleted, Final: []byte("the-answer"),
	})

	for range ch { //nolint:revive // drain to terminal, like handleJobResult's wait
	}
	r, err := s.GetResult(rid)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusDone, r.Status,
		"result must be durable the moment the terminal chunk is observable")
	require.Equal(t, []byte("the-answer"), r.Body)
}

// End-to-end: a submitted job flows through dispatch to a worker, completes,
// and its result lands in the durable store retrievable via GetResult — the
// full path the async API depends on.
func TestAsyncEndToEnd_SubmitDispatchCompleteGetResult(t *testing.T) {
	const runtimeName, modelID = "llama-cpp", "m-1"
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	assigns := make(chan string, 1)
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(4)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if aj := msg.GetAssignJob(); aj != nil {
			select {
			case assigns <- aj.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	rid, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName, ModelID: modelID, Payload: []byte("p"),
		Cost: 100, CostAxis: "q4k_matvec",
	})
	require.NoError(t, err)

	// Before dispatch the result is pending.
	r, err := s.GetResult(rid)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusPending, r.Status)

	s.dispatchPass(context.Background())

	var workerJobID string
	select {
	case workerJobID = <-assigns:
	case <-time.After(2 * time.Second):
		t.Fatal("job never reached AssignJob")
	}

	// In flight, no terminal frame yet: async pollers see processing.
	// AssignJob fires after the processing mark, so no polling needed.
	r, err = s.GetResult(rid)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusProcessing, r.Status)

	// Worker finishes the job.
	w.DeliverJobChunk(workerJobID, &worker.JobChunk{
		Type: worker.JobChunkTypeCompleted, Final: []byte("the-answer"),
	})

	// The pump persists the terminal frame; GetResult surfaces it as Done.
	deadline := time.Now().Add(3 * time.Second)
	for {
		r, err := s.GetResult(rid)
		require.NoError(t, err)
		if r.Status == queue.ResultStatusDone {
			require.Equal(t, []byte("the-answer"), r.Body)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("result never reached Done (last status %q)", r.Status)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
