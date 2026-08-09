package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/stretchr/testify/require"
)

// benchHarness wires a strict scheduler, one online worker, and a fake
// gateway so a test can drive the orchestrator end to end: it observes
// the HubModelBenchmark frames the worker receives and answers each one.
type benchHarness struct {
	t       *testing.T
	s       *Scheduler
	st      *store.Store
	w       *worker.StreamWorker
	cancel  context.CancelFunc
	sent    chan *workerpb.HubModelBenchmark
	unloads chan string

	mu       sync.Mutex
	authored int
	authErr  error
}

const (
	benchRuntime = "llama-cpp"
	benchModelID = "group/a.gguf"
	testModelKey = "gguf/group/a.gguf"
	benchDevSet  = "gpu:0"
)

func newBenchHarness(t *testing.T) *benchHarness {
	t.Helper()
	s, st := newStrictTestScheduler(t)
	h := &benchHarness{
		t:       t,
		s:       s,
		st:      st,
		sent:    make(chan *workerpb.HubModelBenchmark, 8),
		unloads: make(chan string, 8),
	}

	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Flops: 100, BenchedAt: time.Now(),
	}))
	h.w = worker.NewFakeStreamWorker("w1", benchRuntime, gpu1(), time.Now())
	h.w.SetFakeCapacity(4)
	h.w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if mb := msg.GetModelBenchmark(); mb != nil {
			h.sent <- mb
		}
		if um := msg.GetUnloadModel(); um != nil {
			h.w.DeliverUnloadResult(um.GetJobId(), "")
			h.unloads <- um.GetModelId()
		}
		return nil
	})

	s.SetBenchProviders(
		func(context.Context, string) ([]BenchModel, error) {
			return []BenchModel{{ID: benchModelID, Key: testModelKey}}, nil
		},
		func(context.Context, string, string) (BenchPayload, error) {
			h.mu.Lock()
			h.authored++
			err := h.authErr
			h.mu.Unlock()
			if err != nil {
				return BenchPayload{}, err
			}
			return BenchPayload{
				Payload:   []byte("bench-job"),
				LoadHints: []byte("hints"),
				Cost:      100,
				Files:     []*workerpb.ModelFile{benchModelFile(testModelKey, 1024)},
			}, nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	t.Cleanup(cancel)
	s.StartBenchOrchestrator(ctx)
	return h
}

// connect registers the worker and fires the connect trigger.
func (h *benchHarness) connect() {
	h.t.Helper()
	require.NoError(h.t, h.s.workers.Register(h.w))
	h.s.OnWorkerConnected(h.w)
}

// awaitBench waits for the next HubModelBenchmark to reach the worker.
func (h *benchHarness) awaitBench() *workerpb.HubModelBenchmark {
	h.t.Helper()
	select {
	case mb := <-h.sent:
		return mb
	case <-time.After(3 * time.Second):
		h.t.Fatal("no HubModelBenchmark reached the worker within 3s")
		return nil
	}
}

// answerOK replies with a successful measurement for the harness model.
func (h *benchHarness) answerOK(elapsed, graph float64, base, perSlot int64) {
	h.w.DeliverModelBenchResult(testModelKey, worker.ModelBenchmarkResult{
		ElapsedSecs: elapsed, GraphSecs: graph, BaseBytes: base, PerSlotBytes: perSlot,
	}, nil)
}

// answerFail replies with a classified failure.
func (h *benchHarness) answerFail(modelKey string, kind error, msg string) {
	h.w.DeliverModelBenchResult(modelKey, worker.ModelBenchmarkResult{}, fmt.Errorf("%w: %s", kind, msg))
}

// awaitRow polls until the store holds a row for the triple.
func (h *benchHarness) awaitRow() store.ModelBenchmarkRow {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		row, err := h.st.GetModelBenchmark("w1", benchDevSet, testModelKey)
		if err == nil {
			return row
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatal("no model benchmark row was written within 3s")
	return store.ModelBenchmarkRow{}
}

// A worker connecting owes a measurement for every catalogue model it has
// no row for. The row MASS writes is cost/elapsed plus the worker's own
// figures, and the scheduler picks it up immediately.
func TestBenchOrchestrator_WorkerConnectTrigger(t *testing.T) {
	h := newBenchHarness(t)
	h.connect()

	mb := h.awaitBench()
	require.Equal(t, testModelKey, mb.GetModelId(), "the bench is keyed by the model's store key")
	require.Equal(t, []byte("bench-job"), mb.GetPayload(), "gateway payload ships verbatim")
	require.Equal(t, []byte("hints"), mb.GetLoadHints(), "gateway load hints ship verbatim")
	require.InDelta(t, 100.0, mb.GetCost(), 1e-9)
	require.Len(t, mb.GetFiles(), 1, "the gateway's artifact set ships verbatim")
	require.Equal(t, testModelKey, mb.GetFiles()[0].GetFilename())

	h.answerOK(2, 0.25, 5<<30, 1<<30)

	row := h.awaitRow()
	require.Empty(t, row.Error)
	require.InDelta(t, 50.0, row.UnitsPerSec, 1e-9, "units_per_sec is cost / elapsed_secs")
	require.InDelta(t, 0.25, row.GraphSecs, 1e-9)
	require.Equal(t, int64(5<<30), row.BaseBytes)
	require.Equal(t, int64(1<<30), row.PerSlotBytes)
}

// A model whose row already exists is not re-benched on the next
// connect: measurements survive restarts, and re-measuring would take
// the worker offline for nothing.
func TestBenchOrchestrator_SkipsModelsWithRows(t *testing.T) {
	h := newBenchHarness(t)
	seedModelBench(t, h.st, "w1", benchDevSet, testModelKey, 50)
	h.connect()

	select {
	case mb := <-h.sent:
		t.Fatalf("a model with a valid row must not be re-benched, got %q", mb.GetModelId())
	case <-time.After(300 * time.Millisecond):
	}
}

// An incapable verdict is permanent: the worker says the device set
// can't run the model, MASS records it and never retries on its own.
func TestBenchOrchestrator_IncapableIsRecordedWithoutRetry(t *testing.T) {
	h := newBenchHarness(t)
	h.connect()

	h.awaitBench()
	h.answerFail(testModelKey, worker.ErrBenchIncapable, "model larger than device")

	row := h.awaitRow()
	require.Contains(t, row.Error, "model larger than device")
	require.Zero(t, row.UnitsPerSec, "an incapable row carries no measurements")

	h.mu.Lock()
	authored := h.authored
	h.mu.Unlock()
	require.Equal(t, 1, authored, "an incapable verdict must not be retried")

	select {
	case <-h.sent:
		t.Fatal("incapable must not be re-benched")
	case <-time.After(300 * time.Millisecond):
	}

	// And it takes the model out of placement entirely.
	_, ok := h.s.lookupModelBenchmark("w1", benchDevSet, testModelKey)
	require.False(t, ok)
}

// A transient failure is retried with backoff, and the last error is
// persisted as incapable once the attempt budget is spent — otherwise
// jobs would wait forever on a bench that keeps failing.
func TestBenchOrchestrator_TransientRetriesThenPersists(t *testing.T) {
	h := newBenchHarness(t)
	// Shrink the backoff so the test doesn't wait minutes for it.
	restore := benchBackoff
	benchBackoff = [...]time.Duration{time.Millisecond, 2 * time.Millisecond}
	t.Cleanup(func() { benchBackoff = restore })

	h.connect()
	for i := range benchMaxAttempts {
		h.awaitBench()
		h.answerFail(testModelKey, worker.ErrBenchTransient, fmt.Sprintf("crash %d", i+1))
	}

	row := h.awaitRow()
	require.Contains(t, row.Error, "crash 3", "the last error is what gets persisted")
	require.Contains(t, row.Error, fmt.Sprintf("%d benchmark attempts failed", benchMaxAttempts))

	select {
	case <-h.sent:
		t.Fatal("the attempt budget is spent; no further benchmarks")
	case <-time.After(300 * time.Millisecond):
	}
}

// A gateway that doesn't know the model yet (a download still landing)
// is retried without spending an attempt — nothing about the worker has
// been learned.
func TestBenchOrchestrator_ModelNotReadyRetriesForFree(t *testing.T) {
	h := newBenchHarness(t)
	restore := benchBackoff
	benchBackoff = [...]time.Duration{time.Millisecond, 2 * time.Millisecond}
	t.Cleanup(func() { benchBackoff = restore })

	h.mu.Lock()
	h.authErr = errors.Join(ErrBenchModelGone, errors.New("not found"))
	h.mu.Unlock()
	h.connect()

	// Let it spin through several free retries, then let it succeed.
	require.Eventually(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.authored >= benchMaxAttempts+2
	}, 3*time.Second, 5*time.Millisecond, "a not-ready model must keep retrying")

	_, err := h.st.GetModelBenchmark("w1", benchDevSet, testModelKey)
	require.Error(t, err, "a not-ready model must not be recorded incapable")

	h.mu.Lock()
	h.authErr = nil
	h.mu.Unlock()
	h.awaitBench()
	h.answerOK(2, 0.25, 0, 0)
	require.Empty(t, h.awaitRow().Error)
}

// The bench owns the worker while it runs: nothing new dispatches to it,
// and the queue drains again the moment the result lands.
func TestBenchOrchestrator_DispatchGateExclusivity(t *testing.T) {
	ctx := context.Background()
	h := newBenchHarness(t)

	assigns := make(chan string, 4)
	h.w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if mb := msg.GetModelBenchmark(); mb != nil {
			h.sent <- mb
		}
		if aj := msg.GetAssignJob(); aj != nil {
			assigns <- aj.GetJobId()
		}
		return nil
	})
	h.connect()
	h.awaitBench()

	// A job that is otherwise perfectly placeable arrives mid-bench.
	seedModelBench(t, h.st, "w1", benchDevSet, "gguf/group/other.gguf", 100)
	h.s.InvalidateModelBenchmarks("")
	h.w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: "m-other", PoolSize: 1, DeviceIDs: []string{"gpu:0"}}})
	_, err := h.s.Submit(ctx, SubmitRequest{
		RuntimeName: benchRuntime,
		ModelID:     "m-other",
		Payload:     []byte("p"),
		Cost:        100,
		Files:       []*workerpb.ModelFile{benchModelFile("gguf/group/other.gguf", 0)},
	})
	require.NoError(t, err)

	require.True(t, h.s.benchGateHeld("w1"), "the bench must hold the worker's dispatch gate")
	h.s.dispatchPass(ctx)
	select {
	case jobID := <-assigns:
		t.Fatalf("nothing may dispatch to a benching worker, got %s", jobID)
	case <-time.After(300 * time.Millisecond):
	}

	// The result frame arrives after the worker has unloaded, so the
	// device is free and dispatch may resume.
	h.answerOK(2, 0.25, 0, 0)
	h.awaitRow()
	require.Eventually(t, func() bool { return !h.s.benchGateHeld("w1") },
		2*time.Second, 5*time.Millisecond, "the gate must lift on the result")

	h.s.dispatchPass(ctx)
	select {
	case <-assigns:
	case <-time.After(3 * time.Second):
		t.Fatal("dispatch did not resume after the benchmark")
	}
}

// Before measuring, idle residents are cleared off the target device set
// — an allocation failure beside another model is contention, not a
// capability verdict.
func TestBenchOrchestrator_ClearsIdleResidentsFirst(t *testing.T) {
	h := newBenchHarness(t)
	h.w.SetFakeLoadedModels([]worker.LoadedModelStatus{
		{ModelID: "m-resident", PoolSize: 1, DeviceIDs: []string{"gpu:0"}},
	})
	h.connect()

	select {
	case evicted := <-h.unloads:
		require.Equal(t, "m-resident", evicted)
	case <-time.After(3 * time.Second):
		t.Fatal("the resident was not evicted before the benchmark")
	}
	h.awaitBench()
}

// A manual re-bench is the only way past an incapable verdict: it wipes
// the row and queues a fresh measurement.
func TestBenchOrchestrator_ManualRebenchClearsIncapable(t *testing.T) {
	h := newBenchHarness(t)
	h.connect()
	h.awaitBench()
	h.answerFail(testModelKey, worker.ErrBenchIncapable, "out of memory")
	require.NotEmpty(t, h.awaitRow().Error)

	require.NoError(t, h.s.RebenchModel(benchRuntime, testModelKey))

	_, err := h.st.GetModelBenchmark("w1", benchDevSet, testModelKey)
	require.ErrorContains(t, err, "no rows", "the incapable row must be wiped")

	h.awaitBench()
	h.answerOK(4, 0.5, 0, 0)
	row := h.awaitRow()
	require.Empty(t, row.Error)
	require.InDelta(t, 25.0, row.UnitsPerSec, 1e-9)
}

// Removing a model forgets its measurements everywhere and tells the
// fleet to drop the files.
func TestBenchOrchestrator_ModelRemovalCleansUp(t *testing.T) {
	h := newBenchHarness(t)
	deletes := make(chan []string, 4)
	h.w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if d := msg.GetDeleteCacheFiles(); d != nil {
			deletes <- d.GetFilenames()
		}
		return nil
	})
	require.NoError(t, h.s.workers.Register(h.w))
	seedModelBench(t, h.st, "w1", benchDevSet, testModelKey, 50)

	h.s.OnModelRemoved([]string{testModelKey})

	_, err := h.st.GetModelBenchmark("w1", benchDevSet, testModelKey)
	require.Error(t, err, "the model's rows must be gone")
	select {
	case files := <-deletes:
		require.Equal(t, []string{testModelKey}, files)
	case <-time.After(2 * time.Second):
		t.Fatal("no cache-file deletion reached the worker")
	}
}

// Revoking a worker forgets every measurement recorded against it; its
// id is never coming back.
func TestBenchOrchestrator_WorkerRemovalCleansUp(t *testing.T) {
	h := newBenchHarness(t)
	seedModelBench(t, h.st, "w1", benchDevSet, testModelKey, 50)
	seedModelBench(t, h.st, "w2", benchDevSet, testModelKey, 50)

	h.s.OnWorkerRemoved("w1")

	_, err := h.st.GetModelBenchmark("w1", benchDevSet, testModelKey)
	require.Error(t, err, "the revoked worker's rows must be gone")
	_, err = h.st.GetModelBenchmark("w2", benchDevSet, testModelKey)
	require.NoError(t, err, "a peer's rows must be untouched")
}

// A worker that disconnects mid-bench leaves no row, so the reconnect
// sweep starts the measurement over.
func TestBenchOrchestrator_DisconnectMidBenchLeavesNoRow(t *testing.T) {
	h := newBenchHarness(t)
	h.connect()
	h.awaitBench()

	h.w.SetOffline()
	h.s.OnWorkerDisconnected("w1")

	_, err := h.st.GetModelBenchmark("w1", benchDevSet, testModelKey)
	require.Error(t, err, "an interrupted bench must not record a verdict")
	require.False(t, h.s.benchGateHeld("w1"), "the gate must not outlive the worker")
}

// A worker with every device disabled is skipped: it answers "no devices
// enabled" transiently, which would burn three attempts and then record
// a verdict the operator's next toggle invalidates.
func TestBenchOrchestrator_SkipsWorkerWithEmptyWhitelist(t *testing.T) {
	h := newBenchHarness(t)
	h.s.SetDeviceEnabledFn(func(string, string) bool { return false })
	require.NoError(t, h.s.workers.Register(h.w))
	h.s.bench.sweepWorker(h.w)

	select {
	case <-h.sent:
		t.Fatal("a worker with no enabled devices must not be benched")
	case <-time.After(300 * time.Millisecond):
	}
}

// The download-completion trigger benches the finished model across the
// fleet, so the first job for it doesn't pay for the measurement.
func TestBenchOrchestrator_DownloadCompletionTrigger(t *testing.T) {
	h := newBenchHarness(t)
	// Register without the connect sweep so the download is the only
	// thing that can queue a bench.
	require.NoError(t, h.s.workers.Register(h.w))

	h.s.OnModelDownloaded(benchRuntime, testModelKey)

	mb := h.awaitBench()
	require.Equal(t, testModelKey, mb.GetModelId())
	h.answerOK(2, 0.25, 0, 0)
	require.Empty(t, h.awaitRow().Error)
}

// A device-whitelist change re-measures the NEW predicted set and leaves
// the old set's row alone — toggling back must reuse it, not re-bench.
func TestBenchOrchestrator_DeviceToggleBenchesNewSet(t *testing.T) {
	h := newBenchHarness(t)
	h.w = worker.NewFakeStreamWorker("w1", benchRuntime, []stats.Device{
		{ID: "gpu:0", Type: stats.DeviceTypeGPU},
		{ID: "gpu:1", Type: stats.DeviceTypeGPU},
	}, time.Now())
	h.w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if mb := msg.GetModelBenchmark(); mb != nil {
			h.sent <- mb
		}
		return nil
	})
	require.NoError(t, h.st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:1", DeviceName: "gpu:1",
		Flops: 100, BenchedAt: time.Now(),
	}))
	// Both GPUs already measured as one set; nothing is owed.
	seedModelBench(t, h.st, "w1", "gpu:0,gpu:1", testModelKey, 200)
	require.NoError(t, h.s.workers.Register(h.w))
	h.s.OnWorkerConnected(h.w)
	select {
	case <-h.sent:
		t.Fatal("the current set already has a row")
	case <-time.After(200 * time.Millisecond):
	}

	// Disabling gpu:1 changes the predicted set, which has no row.
	h.s.SetDeviceEnabledFn(func(_, devID string) bool { return devID != "gpu:1" })
	h.s.OnWorkerDevicesChanged("w1")

	h.awaitBench()
	h.answerOK(5, 0.5, 0, 0)

	row := h.awaitRow()
	require.InDelta(t, 20.0, row.UnitsPerSec, 1e-9)
	old, err := h.st.GetModelBenchmark("w1", "gpu:0,gpu:1", testModelKey)
	require.NoError(t, err, "the previous set's row must be kept")
	require.InDelta(t, 200.0, old.UnitsPerSec, 1e-9)
}
