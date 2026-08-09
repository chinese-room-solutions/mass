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
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/stretchr/testify/require"
)

// drainOneWorkerQueue must drain at least 1 row when the worker's
// AvailableCapacity is 0, because that's the steady state until
// dispatchEnvelope's LoadModel materialises the context pool. Without
// this allowance the worker queue grows indefinitely and load-on-demand
// never fires — the regression that mass@fcd5ab7 fixed.
//
// This test also locks the load-on-demand artifact contract: the envelope's
// Files + LoadHints must reach the worker on the LoadModel call, and the
// LoadModel must fire *before* AssignJob.
func TestDrainOneWorkerQueue_PopsWhenCapacityZero(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	// No model loaded → capacity=0. Sender records every HubMessage so
	// the test can assert LoadModel fired with the envelope's artifacts.
	w := worker.NewFakeStreamWorker("w1", runtimeName, gpu1(), time.Now())
	w.SetFakeCapacity(0)

	var (
		mu     sync.Mutex
		seen   []string
		gotLM  *workerpb.HubLoadModel
		loadID = make(chan string, 1)
	)
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		mu.Lock()
		defer mu.Unlock()
		if lm := msg.GetLoadModel(); lm != nil {
			seen = append(seen, "LoadModel:"+lm.GetModelId())
			gotLM = lm
			select {
			case loadID <- lm.GetJobId():
			default:
			}
		}
		if aj := msg.GetAssignJob(); aj != nil {
			seen = append(seen, "AssignJob:"+aj.GetModelId())
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	wantFiles := []*workerpb.ModelFile{
		{Url: "http://x/y.gguf", Filename: "y.gguf", SizeBytes: 1024},
	}
	wantHints := []byte("opaque-hints-blob")

	_, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "q4k_matvec",
		Files:     wantFiles,
		LoadHints: wantHints,
		Source:    "test",
	})
	require.NoError(t, err)

	// The drain goroutine blocks awaiting the LoadModel ack — deliver it
	// once observed, then wait for the drain to finish.
	s.dispatchPass(context.Background())

	var jobID string
	select {
	case jobID = <-loadID:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch did not call LoadModel within 2s — load-on-demand path broken")
	}
	w.DeliverLoadResult(jobID, worker.LoadResult{PoolSize: 1}, "")
	waitDispatchIdle(t, s)

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(seen), 1, "at least LoadModel must have fired")
	require.Equal(t, "LoadModel:"+modelID, seen[0],
		"first wire message must be LoadModel, not AssignJob — load-on-demand precedes assign")
	require.NotNil(t, gotLM, "LoadModel HubMessage must have been captured")
	require.Equal(t, modelID, gotLM.GetModelId(), "LoadModel must carry envelope ModelID")
	require.Len(t, gotLM.GetFiles(), 1, "LoadModel must carry envelope Files")
	require.Equal(t, wantFiles[0].Url, gotLM.GetFiles()[0].GetUrl())
	require.Equal(t, wantFiles[0].SizeBytes, gotLM.GetFiles()[0].GetSizeBytes())
	require.Equal(t, wantHints, gotLM.GetLoadHints(),
		"LoadModel must carry envelope LoadHints verbatim")
}

// pickWorkerQueue must charge load_latency only when the worker isn't
// effectively-warm for env.ModelID. Two workers, same power, same
// empty tail: the one with the matching tail_model_id wins because
// it pays zero load cost; the other has to ship the file bytes.
func TestPickWorkerQueue_TailModelSuppressesLoadCost(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, st := newTestScheduler(t)
	for _, wID := range []string{"w-warm", "w-cold"} {
		require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
			MemoryGBs: 1.0, LoadGBs: 1.0, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
		}))
	}
	// Both workers have capacity and equal compute. The differentiator
	// must be the load cost.
	// The envelope's files carry no URL (local-path shipping), so both
	// workers must be loopback to stay placement candidates.
	warm := worker.NewFakeStreamWorker("w-warm", runtimeName, gpu1(), time.Now())
	warm.SetFakeCapacity(4)
	warm.SetFakeLoopback(true)
	require.NoError(t, s.workers.Register(warm))
	s.OnWorkerConnected(warm)
	cold := worker.NewFakeStreamWorker("w-cold", runtimeName, gpu1(), time.Now())
	cold.SetFakeCapacity(4)
	cold.SetFakeLoopback(true)
	require.NoError(t, s.workers.Register(cold))
	s.OnWorkerConnected(cold)

	// Pre-seed w-warm's tail_model_id so scoring sees it as
	// effectively-warm for modelID. tail_seconds = 1.0s — non-zero so
	// the "empty queue → check residency" branch is bypassed and the
	// tail_model branch fires.
	s.creditTail("worker|w-warm", 1.0, modelID)

	// Envelope carries 10 GB at 1 GB/s memory throughput, so a cold
	// worker would pay 10s of load_latency — strictly worse than
	// w-warm's seeded 1.0s tail.
	target, _ := s.pickWorkerQueue(queue.Envelope{
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Cost:        float64(100), CostAxis: "q4k_matvec",
		Files: []*workerpb.ModelFile{{SizeBytes: 10_000_000_000}},
	})
	require.NotNil(t, target)
	require.Equal(t, "worker|w-warm", target.name,
		"tail_model_id match must suppress load cost; warm (1.0s tail) wins over cold (10s load)")
}

// pumpWorkerChunks must NOT fail the result when the worker goes
// offline mid-job. The reaper (OnWorkerDisconnected) is what releases
// the global anchor; if pump also wrote a failure result, the
// re-dispatched run would have nowhere to land its completion. This is
// the disconnect-tolerant pump path introduced in mass@3b40e9f.
func TestPumpWorkerChunks_LeavesResultPendingOnDisconnect(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	// Sender that records the AssignJob and immediately responds
	// success at the bidi-stream level. We deliver no chunk back — the
	// pump's workerCh stays open until we close it from the fake.
	var assigned atomic.Bool
	jobIDCh := make(chan string, 1)
	w := worker.NewFakeStreamWorker("w1", runtimeName, gpu1(), time.Now())
	w.SetFakeCapacity(4)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if aj := msg.GetAssignJob(); aj != nil {
			assigned.Store(true)
			select {
			case jobIDCh <- aj.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	requestID, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "q4k_matvec",
	})
	require.NoError(t, err)

	// Drive dispatch to AssignJob.
	s.dispatchPass(context.Background())
	pollUntilEqual(t, 1, func() int {
		if assigned.Load() {
			return 1
		}
		return 0
	}, "AssignJob must fire")
	jobID := <-jobIDCh

	// SetOffline flips online=false AND closes every in-flight job
	// channel without a terminal frame — exactly the disconnect-mid-job
	// shape pumpWorkerChunks must tolerate. Per the new contract, it
	// must NOT write a failure result; it should leave the entry
	// pending for redistribution.
	_ = jobID // job is referenced via internal channel; SetOffline closes them all
	w.SetOffline()

	// Give the pump goroutine a moment to react. The result entry must
	// not transition to error, and the pump must leave the processing
	// status alone — the processing→pending revert belongs to the
	// disconnect drain (OnWorkerDisconnected), which hasn't run here.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		res, err := s.results.Get(requestID)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.NotEqual(t, queue.ResultStatusError, res.Status,
			"pump must not fail result on worker disconnect — OnWorkerDisconnected will redistribute")
		time.Sleep(50 * time.Millisecond)
	}
	res, err := s.results.Get(requestID)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusProcessing, res.Status,
		"after pump exits on disconnect, the result stays processing until the disconnect drain reverts it")
}

// A job that was cancelled and then loses its worker (e.g. the worker
// crashed before observing the cancel) must NOT be redistributed — the
// operator asked it to stop. The pump finalizes it as cancelled instead
// of leaving it pending for re-placement.
func TestPumpWorkerChunks_CancelledThenDisconnect_NotRedistributed(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	var assigned atomic.Bool
	jobIDCh := make(chan string, 1)
	w := worker.NewFakeStreamWorker("w1", runtimeName, gpu1(), time.Now())
	w.SetFakeCapacity(4)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if aj := msg.GetAssignJob(); aj != nil {
			assigned.Store(true)
			select {
			case jobIDCh <- aj.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	requestID, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "q4k_matvec",
	})
	require.NoError(t, err)

	s.dispatchPass(context.Background())
	pollUntilEqual(t, 1, func() int {
		if assigned.Load() {
			return 1
		}
		return 0
	}, "AssignJob must fire")
	<-jobIDCh

	// Operator cancels the running job, then the worker drops without ever
	// emitting a terminal frame (the crash case).
	require.NoError(t, s.CancelRunningJob(context.Background(), requestID))
	require.True(t, s.isInflightCancelled(requestID))
	w.SetOffline()

	// The result must settle to error ("cancelled by operator"), never stay
	// pending (which would mean it's queued for redistribution).
	pollUntilEqual(t, 1, func() int {
		res, err := s.results.Get(requestID)
		if err != nil || res == nil {
			return 0
		}
		if res.Status == queue.ResultStatusError {
			return 1
		}
		return 0
	}, "cancelled job must finalize as error, not await redistribution")

	res, err := s.results.Get(requestID)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusError, res.Status)
	require.Equal(t, "cancelled by operator", res.Error)
}

// dispatchEnvelope must NOT load a new model when the predicted device
// set overlaps with an active resident — the job's lease is released
// for retry, no LoadModel/UnloadModel fires. This is the bug from
// project_device_set_exclusion: previously a worker would happily hold
// two models on the same GPUs, halving throughput silently.
func TestDispatchEnvelope_OverlapWithActiveResidentDefersLoad(t *testing.T) {
	const runtimeName = "llama-cpp"
	const residentModelID = "m-resident"
	const newModelID = "m-new"

	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	// Resident model occupies gpu:0 AND is actively running a job
	// (Active=1). The new envelope targets the same gpu:0. The gate
	// must defer the load.
	var loadCalls, unloadCalls atomic.Int32
	w := worker.NewFakeStreamWorker("w1", runtimeName, gpu1(), time.Now())
	w.SetFakeCapacity(0) // pre-load capacity
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: residentModelID, PoolSize: 1, Active: 1,
		DeviceIDs: []string{"gpu:0"},
	}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if msg.GetLoadModel() != nil {
			loadCalls.Add(1)
		}
		if msg.GetUnloadModel() != nil {
			unloadCalls.Add(1)
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	_, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     newModelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "q4k_matvec",
		Files:     []*workerpb.ModelFile{{Url: "http://x/y.gguf", Filename: "y.gguf", SizeBytes: 1024}},
		LoadHints: []byte("h"),
		Source:    "test",
	})
	require.NoError(t, err)

	// Drive one dispatch tick and wait for its drain goroutine — the gate
	// bounces the row back onto the queue before the drain exits.
	s.dispatchPass(context.Background())
	waitDispatchIdle(t, s)

	require.Equal(t, int32(0), loadCalls.Load(),
		"LoadModel must NOT fire while an overlapping resident is active")
	require.Equal(t, int32(0), unloadCalls.Load(),
		"UnloadModel must NOT fire — gate only evicts when the resident is idle")
}

// When every overlapping resident is idle (Active=0), the gate evicts
// them first and then proceeds with the load. This is the natural
// "queue processing as it goes" flow: a slot frees, the next queued
// model takes its place on the GPUs.
func TestDispatchEnvelope_OverlapWithIdleResidentEvictsThenLoads(t *testing.T) {
	const runtimeName = "llama-cpp"
	const residentModelID = "m-resident"
	const newModelID = "m-new"

	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	var (
		mu          sync.Mutex
		seenSeq     []string
		unloadJobID = make(chan string, 1)
		loadJobID   = make(chan string, 1)
	)
	w := worker.NewFakeStreamWorker("w1", runtimeName, gpu1(), time.Now())
	w.SetFakeCapacity(0)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: residentModelID, PoolSize: 1, Active: 0,
		IdleSince: time.Now(),
		DeviceIDs: []string{"gpu:0"},
	}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		mu.Lock()
		defer mu.Unlock()
		if um := msg.GetUnloadModel(); um != nil {
			seenSeq = append(seenSeq, "Unload:"+um.GetModelId())
			select {
			case unloadJobID <- um.GetJobId():
			default:
			}
		}
		if lm := msg.GetLoadModel(); lm != nil {
			seenSeq = append(seenSeq, "Load:"+lm.GetModelId())
			select {
			case loadJobID <- lm.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	_, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     newModelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "q4k_matvec",
		Files:     []*workerpb.ModelFile{{Url: "http://x/y.gguf", Filename: "y.gguf", SizeBytes: 1024}},
		LoadHints: []byte("h"),
		Source:    "test",
	})
	require.NoError(t, err)

	// The drain goroutine blocks on the Unload + Load acks — feed them
	// as they arrive.
	s.dispatchPass(context.Background())

	// Ack the eviction first.
	select {
	case jobID := <-unloadJobID:
		w.DeliverUnloadResult(jobID, "")
	case <-time.After(2 * time.Second):
		t.Fatal("expected UnloadModel call within 2s")
	}
	// Ack the load.
	select {
	case jobID := <-loadJobID:
		w.DeliverLoadResult(jobID, worker.LoadResult{PoolSize: 1}, "")
	case <-time.After(2 * time.Second):
		t.Fatal("expected LoadModel call within 2s")
	}
	waitDispatchIdle(t, s)

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(seenSeq), 2, "must observe at least Unload then Load")
	require.Equal(t, "Unload:"+residentModelID, seenSeq[0],
		"eviction of the overlapping resident must precede the new load")
	require.Equal(t, "Load:"+newModelID, seenSeq[1],
		"new model load must follow the eviction")
}

// Regression for the bug spotted in production: two submits for
// different models with overlapping predicted device sets arriving
// before any heartbeat updates LoadedModelStatus.Active. After the
// first dispatch puts modelA on the worker, the heartbeat-driven
// Active still reads 0 — so when the second envelope is dispatched
// (and modelA's pump goroutine is still streaming chunks), the gate
// would see "overlap with idle resident" and evict modelA to make
// room for modelB. The fix: MASS's own inflight tracker is the
// authoritative activity signal for jobs it just dispatched. Without
// it, both jobs end up "running" while only one model stays loaded.
func TestDispatchEnvelope_TwoOverlappingSubmitsDoNotCoLocate(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelA = "m-a"
	const modelB = "m-b"

	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	var (
		mu          sync.Mutex
		loadSeq     []string
		unloadCalls atomic.Int32
		assignCalls atomic.Int32
		loadJobID   = make(chan string, 4)
	)
	w := worker.NewFakeStreamWorker("w1", runtimeName, gpu1(), time.Now())
	// Capacity 0 → drainOneWorkerQueue peeks 1 row per pass, so each
	// dispatchPass dispatches exactly one envelope. That matches the
	// production sequence: first pass loads + assigns modelA, the
	// worker's pump goroutine is still streaming, and the *next* pass
	// reaches modelB — at which point Active for modelA is still 0
	// (no heartbeat between passes).
	w.SetFakeCapacity(0)
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		mu.Lock()
		defer mu.Unlock()
		if lm := msg.GetLoadModel(); lm != nil {
			loadSeq = append(loadSeq, lm.GetModelId())
			select {
			case loadJobID <- lm.GetJobId():
			default:
			}
		}
		if msg.GetUnloadModel() != nil {
			unloadCalls.Add(1)
		}
		if msg.GetAssignJob() != nil {
			assignCalls.Add(1)
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	files := []*workerpb.ModelFile{{Url: "http://x/y.gguf", Filename: "y.gguf", SizeBytes: 1024}}
	_, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName, ModelID: modelA, Payload: []byte("p"),
		Cost: 100, CostAxis: "q4k_matvec", Files: files, LoadHints: []byte("h"),
	})
	require.NoError(t, err)
	_, err = s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName, ModelID: modelB, Payload: []byte("p"),
		Cost: 100, CostAxis: "q4k_matvec", Files: files, LoadHints: []byte("h"),
	})
	require.NoError(t, err)

	// First pass: loads modelA. The pump goroutine is still alive after
	// the drain finishes — we never deliver a terminal frame.
	s.dispatchPass(context.Background())
	select {
	case jobID := <-loadJobID:
		w.DeliverLoadResult(jobID, worker.LoadResult{PoolSize: 1}, "")
	case <-time.After(2 * time.Second):
		t.Fatal("expected first LoadModel within 2s")
	}
	waitDispatchIdle(t, s)

	// Reflect post-load state: modelA is resident, Active stays 0
	// because no heartbeat has arrived between dispatch passes. This
	// is the production timing window.
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: modelA, PoolSize: 1, Active: 0,
		DeviceIDs: []string{"gpu:0"},
	}})

	// Second pass: dispatches modelB. The gate must see modelA as
	// still active (via the inflight tracker, not Active) and bounce
	// the row for retry. No UnloadModel, no second LoadModel.
	s.dispatchPass(context.Background())
	waitDispatchIdle(t, s)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{modelA}, loadSeq,
		"only modelA should have loaded; modelB must be bounced for retry")
	require.Equal(t, int32(0), unloadCalls.Load(),
		"the gate must NOT evict modelA — MASS-side inflight signals it's busy")
	require.Equal(t, int32(1), assignCalls.Load(),
		"only modelA should have been assigned")

	// modelB row remains on the worker queue head.
	s.queueMu.RLock()
	wq := s.devQueues["worker|w1"]
	s.queueMu.RUnlock()
	require.Equal(t, 1, mustDepth(t, wq, context.Background()),
		"bounced row must remain on the worker queue for retry")
}

// A worker that already holds the target model is unaffected by the
// gate — no evict, no load, just AssignJob. Same-model-resident is the
// hot path the gate must not regress.
func TestDispatchEnvelope_AlreadyResidentSkipsGate(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	var (
		loadCalls, unloadCalls atomic.Int32
		assignCalls            atomic.Int32
	)
	w := worker.NewFakeStreamWorker("w1", runtimeName, gpu1(), time.Now())
	w.SetFakeCapacity(4)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: modelID, PoolSize: 1,
		DeviceIDs: []string{"gpu:0"},
	}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if msg.GetLoadModel() != nil {
			loadCalls.Add(1)
		}
		if msg.GetUnloadModel() != nil {
			unloadCalls.Add(1)
		}
		if msg.GetAssignJob() != nil {
			assignCalls.Add(1)
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	_, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "q4k_matvec",
	})
	require.NoError(t, err)
	s.dispatchPass(context.Background())

	pollUntilEqual(t, 1, func() int { return int(assignCalls.Load()) },
		"AssignJob must fire for already-resident model")
	require.Equal(t, int32(0), loadCalls.Load(),
		"LoadModel must not fire for already-resident model")
	require.Equal(t, int32(0), unloadCalls.Load(),
		"UnloadModel must not fire for already-resident model")
}

// Resident model holds {gpu:0, gpu:1} but the operator has since
// disabled gpu:1. The next job for that model must re-load it onto the
// remaining enabled set ({gpu:0}) — not run on stale placement. The
// resident is idle, so the gate evicts it and falls into the normal
// cold-load path.
func TestDispatchEnvelope_StaleResidentIdleEvictsThenReloads(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"

	s, st := newTestScheduler(t)
	for _, devID := range []string{"gpu:0", "gpu:1"} {
		require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: "w1", DeviceID: devID, DeviceName: devID,
			MemoryGBs: 25, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
		}))
	}
	// Operator disabled gpu:1 *after* the model was loaded.
	s.SetDeviceEnabledFn(func(_, devID string) bool { return devID != "gpu:1" })

	var (
		mu          sync.Mutex
		seenSeq     []string
		unloadJobID = make(chan string, 1)
		loadJobID   = make(chan string, 1)
	)
	w := worker.NewFakeStreamWorker("w1", runtimeName, gpu2(), time.Now())
	// Resident is idle (Active=0) and has one free slot in its pool —
	// pickWorkerQueue excludes residents at capacity 0, so the picker
	// needs at least one slot to even reach the dispatch gate.
	w.SetFakeCapacity(1)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: modelID, PoolSize: 1, Active: 0,
		IdleSince: time.Now(),
		DeviceIDs: []string{"gpu:0", "gpu:1"},
	}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		mu.Lock()
		defer mu.Unlock()
		if um := msg.GetUnloadModel(); um != nil {
			seenSeq = append(seenSeq, "Unload:"+um.GetModelId())
			select {
			case unloadJobID <- um.GetJobId():
			default:
			}
		}
		if lm := msg.GetLoadModel(); lm != nil {
			seenSeq = append(seenSeq, "Load:"+lm.GetModelId())
			select {
			case loadJobID <- lm.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	_, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "q4k_matvec",
		Files:     []*workerpb.ModelFile{{Url: "http://x/y.gguf", Filename: "y.gguf", SizeBytes: 1024}},
		LoadHints: []byte("h"),
		Source:    "test",
	})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		s.dispatchPass(context.Background())
		close(done)
	}()

	select {
	case jobID := <-unloadJobID:
		// Mirror the real worker: the model is no longer resident after
		// the unload acknowledges. Without this the cold-load path below
		// short-circuits because workerHasModel still reports true and
		// no LoadModel ever fires.
		w.SetFakeLoadedModels(nil)
		w.DeliverUnloadResult(jobID, "")
	case <-time.After(2 * time.Second):
		t.Fatal("expected UnloadModel for stale-placement resident within 2s")
	}
	select {
	case jobID := <-loadJobID:
		w.DeliverLoadResult(jobID, worker.LoadResult{PoolSize: 1}, "")
	case <-time.After(2 * time.Second):
		t.Fatal("expected LoadModel (re-place onto new device set) within 2s")
	}
	<-done

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(seenSeq), 2, "must observe at least Unload then Load")
	require.Equal(t, "Unload:"+modelID, seenSeq[0],
		"stale-placement resident must be unloaded first")
	require.Equal(t, "Load:"+modelID, seenSeq[1],
		"same model must be re-loaded onto the new device set")
}

// Same setup as above, but the resident is still serving traffic
// (Active=1). The gate must NOT yank it — bounce the job for retry
// instead so the resident drains naturally, then the next tick can
// evict + reload onto the new device set.
func TestDispatchEnvelope_StaleResidentActiveBounces(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"

	s, st := newTestScheduler(t)
	for _, devID := range []string{"gpu:0", "gpu:1"} {
		require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: "w1", DeviceID: devID, DeviceName: devID,
			MemoryGBs: 25, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
		}))
	}
	s.SetDeviceEnabledFn(func(_, devID string) bool { return devID != "gpu:1" })

	var loadCalls, unloadCalls atomic.Int32
	w := worker.NewFakeStreamWorker("w1", runtimeName, gpu2(), time.Now())
	// Pool has free slots (3 of 4) but one job is active. The picker
	// picks this worker, and the gate must bounce because evicting an
	// actively-serving resident would yank the running job.
	w.SetFakeCapacity(3)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: modelID, PoolSize: 4, Active: 1,
		DeviceIDs: []string{"gpu:0", "gpu:1"},
	}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if msg.GetLoadModel() != nil {
			loadCalls.Add(1)
		}
		if msg.GetUnloadModel() != nil {
			unloadCalls.Add(1)
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	_, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "q4k_matvec",
		Files:     []*workerpb.ModelFile{{Url: "http://x/y.gguf", Filename: "y.gguf", SizeBytes: 1024}},
		LoadHints: []byte("h"),
		Source:    "test",
	})
	require.NoError(t, err)

	s.dispatchPass(context.Background())
	waitDispatchIdle(t, s)

	require.Equal(t, int32(0), unloadCalls.Load(),
		"UnloadModel must NOT fire while the stale resident is active")
	require.Equal(t, int32(0), loadCalls.Load(),
		"LoadModel must NOT fire — stale resident is still serving traffic")
}

// residentsBlockingLoad is the pure predicate shared by the picker, the
// load-cost predictor, and the dispatch gate. It returns every resident
// that must be evicted before a fresh LoadModel(target) onto predicted
// can proceed. Two regimes share one table: stale-self (target's own
// placement no longer fits) and other-overlap (a different model's
// placement intersects the predicted set). Empty DeviceIDs is treated
// asymmetrically — see the function docstring.
func TestResidentsBlockingLoad(t *testing.T) {
	tests := []struct {
		name      string
		target    string
		loaded    []worker.LoadedModelStatus
		predicted []string
		wantIDs   []string
	}{
		{
			name:      "no residents → nothing blocks",
			target:    "m",
			loaded:    nil,
			predicted: []string{"gpu:0"},
		},
		{
			name:      "empty predicted → nothing blocks",
			target:    "m",
			loaded:    []worker.LoadedModelStatus{{ModelID: "a", DeviceIDs: []string{"gpu:0"}}},
			predicted: nil,
		},
		{
			name:      "other model on disjoint devices → does not block",
			target:    "m",
			loaded:    []worker.LoadedModelStatus{{ModelID: "a", DeviceIDs: []string{"cpu:0"}}},
			predicted: []string{"gpu:0", "gpu:1"},
		},
		{
			name:      "other model on exactly the predicted set → blocks",
			target:    "m",
			loaded:    []worker.LoadedModelStatus{{ModelID: "a", DeviceIDs: []string{"gpu:0", "gpu:1"}}},
			predicted: []string{"gpu:0", "gpu:1"},
			wantIDs:   []string{"a"},
		},
		{
			name:      "other model partially overlapping → blocks",
			target:    "m",
			loaded:    []worker.LoadedModelStatus{{ModelID: "a", DeviceIDs: []string{"gpu:0"}}},
			predicted: []string{"gpu:0", "gpu:1"},
			wantIDs:   []string{"a"},
		},
		{
			name:   "multiple other models — only the overlapping ones block",
			target: "m",
			loaded: []worker.LoadedModelStatus{
				{ModelID: "a", DeviceIDs: []string{"gpu:0"}},
				{ModelID: "b", DeviceIDs: []string{"gpu:1"}},
				{ModelID: "c", DeviceIDs: []string{"cpu:0"}},
			},
			predicted: []string{"gpu:0", "gpu:1"},
			wantIDs:   []string{"a", "b"},
		},
		{
			name:      "other model with empty DeviceIDs → conservatively blocks",
			target:    "m",
			loaded:    []worker.LoadedModelStatus{{ModelID: "a", DeviceIDs: nil}},
			predicted: []string{"gpu:0"},
			wantIDs:   []string{"a"},
		},
		{
			name:      "target resident fitting → does not block",
			target:    "a",
			loaded:    []worker.LoadedModelStatus{{ModelID: "a", DeviceIDs: []string{"gpu:0"}}},
			predicted: []string{"gpu:0"},
		},
		{
			name:      "target resident as strict subset of predicted → does not block",
			target:    "a",
			loaded:    []worker.LoadedModelStatus{{ModelID: "a", DeviceIDs: []string{"gpu:0"}}},
			predicted: []string{"gpu:0", "gpu:1"},
		},
		{
			name:      "target resident, one of its devices disabled → blocks (stale)",
			target:    "a",
			loaded:    []worker.LoadedModelStatus{{ModelID: "a", DeviceIDs: []string{"gpu:0", "gpu:1"}}},
			predicted: []string{"gpu:0"},
			wantIDs:   []string{"a"},
		},
		{
			name:      "target resident, every device disabled → blocks (stale)",
			target:    "a",
			loaded:    []worker.LoadedModelStatus{{ModelID: "a", DeviceIDs: []string{"gpu:0"}}},
			predicted: []string{"cpu:0"},
			wantIDs:   []string{"a"},
		},
		{
			name:      "target resident with empty DeviceIDs → does NOT block (placement unknown)",
			target:    "a",
			loaded:    []worker.LoadedModelStatus{{ModelID: "a", DeviceIDs: nil}},
			predicted: []string{"gpu:0"},
		},
		{
			name:   "mixed: stale target + other-overlap → both surface",
			target: "a",
			loaded: []worker.LoadedModelStatus{
				{ModelID: "a", DeviceIDs: []string{"gpu:0", "gpu:1"}},
				{ModelID: "b", DeviceIDs: []string{"gpu:0"}},
			},
			predicted: []string{"gpu:0"},
			wantIDs:   []string{"a", "b"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := worker.NewFakeStreamWorker("w1", "llama-cpp", gpu1(), time.Now())
			w.SetFakeLoadedModels(tt.loaded)
			got := residentsBlockingLoad(w, tt.target, tt.predicted)
			gotIDs := make([]string, 0, len(got))
			for _, lm := range got {
				gotIDs = append(gotIDs, lm.ModelID)
			}
			require.ElementsMatch(t, tt.wantIDs, gotIDs)
		})
	}
}

// predictDeviceSet must mirror the C++ worker's allowed_load_devices()
// rule: every enabled GPU, or the CPU when no GPU is enabled.
func TestPredictDeviceSet(t *testing.T) {
	tests := []struct {
		name     string
		devices  []stats.Device
		disabled map[string]bool
		want     []string
	}{
		{
			name:    "single GPU enabled",
			devices: []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}},
			want:    []string{"gpu:0"},
		},
		{
			name: "two GPUs enabled → both",
			devices: []stats.Device{
				{ID: "gpu:0", Type: stats.DeviceTypeGPU},
				{ID: "gpu:1", Type: stats.DeviceTypeGPU},
			},
			want: []string{"gpu:0", "gpu:1"},
		},
		{
			name: "GPU + CPU enabled → GPU only (CPU never picked when a GPU is in)",
			devices: []stats.Device{
				{ID: "gpu:0", Type: stats.DeviceTypeGPU},
				{ID: "cpu:0", Type: stats.DeviceTypeCPU},
			},
			want: []string{"gpu:0"},
		},
		{
			name: "CPU only when every GPU disabled",
			devices: []stats.Device{
				{ID: "gpu:0", Type: stats.DeviceTypeGPU},
				{ID: "cpu:0", Type: stats.DeviceTypeCPU},
			},
			disabled: map[string]bool{"gpu:0": true},
			want:     []string{"cpu:0"},
		},
		{
			name:    "no devices → empty",
			devices: nil,
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestScheduler(t)
			if len(tt.disabled) > 0 {
				disabled := tt.disabled
				s.SetDeviceEnabledFn(func(_, devID string) bool {
					return !disabled[devID]
				})
			}
			w := worker.NewFakeStreamWorker("w1", "llama-cpp", tt.devices, time.Now())
			got := s.predictDeviceSet(w)
			require.Equal(t, tt.want, got)
		})
	}
}

// End-to-end with operator-disabled GPU: a worker with GPU+CPU has the
// GPU disabled in the operator UI. The C++ worker's allowed_load_devices
// rule falls back to CPU-only; predictDeviceSet must mirror that, and a
// resident model already on the CPU set must block a second submit that
// also routes to CPU (the gate's overlap detection sees the resident
// CPU set as overlapping with the predicted CPU set). Closes the loop
// between the operator's enable toggle and the gate.
func TestDispatchEnvelope_DisabledGPUFallsThroughToCPUOverlap(t *testing.T) {
	const runtimeName = "llama-cpp"
	const residentModelID = "m-resident"
	const newModelID = "m-new"

	s, st := newTestScheduler(t)
	// Both GPU and CPU benched — score-wise either would be eligible if
	// enabled. The operator-enable flag is what steers placement.
	for _, devID := range []string{"gpu:0", "cpu:0"} {
		require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: "w1", DeviceID: devID, DeviceName: devID,
			MemoryGBs: 10, LoadGBs: 10, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
		}))
	}
	// Operator: GPU disabled, CPU enabled.
	s.SetDeviceEnabledFn(func(_, devID string) bool {
		return devID != "gpu:0"
	})

	var loadCalls atomic.Int32
	w := worker.NewFakeStreamWorker("w1", runtimeName, []stats.Device{
		{ID: "gpu:0", Type: stats.DeviceTypeGPU},
		{ID: "cpu:0", Type: stats.DeviceTypeCPU},
	}, time.Now())
	w.SetFakeCapacity(0)
	// Resident on the CPU — same set predictDeviceSet will return for
	// a fresh load given the disabled GPU.
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: residentModelID, PoolSize: 1, Active: 1,
		DeviceIDs: []string{"cpu:0"},
	}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if msg.GetLoadModel() != nil {
			loadCalls.Add(1)
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	// predictDeviceSet should return [cpu:0] — GPU disabled, only CPU
	// available.
	require.Equal(t, []string{"cpu:0"}, s.predictDeviceSet(w),
		"disabled GPU must fall through to CPU")

	_, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     newModelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "q4k_matvec",
		Files:     []*workerpb.ModelFile{{Url: "http://x/y.gguf", Filename: "y.gguf", SizeBytes: 1024}},
		LoadHints: []byte("h"),
	})
	require.NoError(t, err)
	s.dispatchPass(context.Background())
	waitDispatchIdle(t, s)

	require.Equal(t, int32(0), loadCalls.Load(),
		"gate must block load: predicted set [cpu:0] overlaps active resident on [cpu:0]")
}

// dispatchEnvelope must record COMPUTE-ONLY seconds (and the axis the
// throughput lookup actually used) on the inflight record. The envelope's
// QueuedSeconds carries the load-switch latency priced at placement, but
// the load completes before the inflight clock starts — carrying it over
// (the old behaviour) overstated the worker's busy-time in scoring.
func TestDispatchEnvelope_InflightSecondsComputeOnly(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, st := newTestScheduler(t)
	// 100 GFLOPS compute, 25 GB/s load: Cost=100 → taskSec = 1.0 s;
	// a 25 GB artifact → loadLat = 1.0 s priced into QueuedSeconds.
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 100, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	// The envelope names an axis no device benched — the used-axis
	// fallback must also be what the inflight record carries.
	s.SetRuntimeDefaultAxisFn(func(string) string { return "q4k_matvec" })

	w := worker.NewFakeStreamWorker("w1", runtimeName, gpu1(), time.Now())
	w.SetFakeCapacity(0)
	loadID := make(chan string, 1)
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if lm := msg.GetLoadModel(); lm != nil {
			select {
			case loadID <- lm.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	rid, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "axis_nobody_benched",
		Files: []*workerpb.ModelFile{
			{Url: "http://x/y.gguf", Filename: "y.gguf", SizeBytes: 25_000_000_000},
		},
	})
	require.NoError(t, err)

	s.dispatchPass(context.Background())
	var jobID string
	select {
	case jobID = <-loadID:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch did not call LoadModel within 2s")
	}
	w.DeliverLoadResult(jobID, worker.LoadResult{PoolSize: 1}, "")
	waitDispatchIdle(t, s)

	s.inflightMu.Lock()
	rec, ok := s.inflightByRequest[rid]
	inflightSec := s.inflightSeconds[workerQueueName("w1")]
	s.inflightMu.Unlock()
	require.True(t, ok, "job must be inflight after dispatch")
	require.InDelta(t, 1.0, rec.seconds, 1e-9,
		"inflight seconds must be Cost/throughput only — no load latency")
	require.InDelta(t, 1.0, inflightSec, 1e-9,
		"queue inflight sum must match the compute-only prediction")
	require.Equal(t, "q4k_matvec", rec.axis,
		"record must carry the axis the prediction divided by, not the unbenched request axis")
}
