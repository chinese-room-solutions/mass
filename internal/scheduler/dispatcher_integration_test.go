package scheduler

import (
	"bytes"
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Conservation test for tail_seconds + inflight bookkeeping across
// many jobs and many dispatch ticks. Subsumes TestDispatcher_InflightLifecycle
// + TestDispatcher_TailSecondsLifecycle + TestDispatcher_ConcurrentJobsCompleteAcrossTwoWorkers:
// if conservation holds, every per-job credit/debit was correct, every
// job reached Done, and inflight cleared cleanly.
//
// Setup: two workers (mismatched power), single resident model, 30
// jobs submitted with varying weights. Sender records every AssignJob
// jobID; jobs are terminated in a deterministically-permuted order
// (seeded RNG) to exercise out-of-order completion. The store is
// wrapped to count every AddTailSeconds / AddTailSecondsAndSetModel
// call and the running sum.
//
// Final invariants:
//   - sum_of_credit == sum_of_debit (within float epsilon)
//   - Every per-queue tail row at the end is exactly 0
//   - inflightSeconds map has no entries
//   - inflightByRequest map is empty
//   - Every submitted RequestID has result status Done
//   - tail never went negative at any observed point
func TestDispatcher_TailAndInflightConservation_AcrossManyTicks(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	const total = 30

	s, st := newTestScheduler(t)

	// Wrap the store so we can count tail mutations and watch for
	// negative values. The wrapper forwards every other call to st.
	tracker := &tailTracker{StateStoreInterface: st}
	s.store = tracker

	// Two workers with different power so the dispatcher does
	// non-trivial placement across them. Model resident on both so
	// load-on-demand doesn't get involved (separately tested).
	workerIDs := []string{"w-fast", "w-slow"}
	gflopsByID := map[string]float64{"w-fast": 500, "w-slow": 100}
	type wInfo struct {
		w      *worker.StreamWorker
		assign chan string
	}
	infos := make(map[string]*wInfo)
	for _, wID := range workerIDs {
		require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
			Throughput: map[string]float64{"q4k_matvec": gflopsByID[wID]}, BenchedAt: time.Now(),
		}))
		info := &wInfo{assign: make(chan string, total)}
		w := worker.NewFakeStreamWorker(wID, runtimeName,
			[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
		w.SetFakeCapacity(8)
		w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 8}})
		w.SetFakeSender(func(msg *workerpb.HubMessage) error {
			if aj := msg.GetAssignJob(); aj != nil {
				select {
				case info.assign <- aj.GetJobId():
				default:
				}
			}
			return nil
		})
		require.NoError(t, s.workers.Register(w))
		s.OnWorkerConnected(w)
		info.w = w
		infos[wID] = info
	}

	// Submit 30 jobs with varying weights to exercise non-uniform
	// QueuedSeconds values. Deterministic so re-runs are reproducible.
	rng := rand.New(rand.NewSource(42))
	requestIDs := make([]string, 0, total)
	for i := range total {
		cost := float64(100 + rng.Intn(400)) // 100..499
		rid, err := s.Submit(context.Background(), SubmitRequest{
			RuntimeName: runtimeName,
			ModelID:     modelID,
			Payload:     []byte("p"),
			Cost:        cost,
			CostAxis:    "q4k_matvec",
		})
		require.NoError(t, err, "submit %d", i)
		requestIDs = append(requestIDs, rid)
	}

	// Drive dispatch in rounds, completing each round's jobs before the
	// next. The dispatcher respects pool_size (8 per worker), so 30 jobs
	// can't all be in flight at once — they flow through as slots free on
	// completion. Each round: run a pass, collect the newly-assigned jobs,
	// then deliver terminal frames (out of order, deterministic seed, to
	// exercise pumps completing in arbitrary order). Loop until every job
	// has been assigned and completed.
	collected := map[string][]string{}
	deadline := time.Now().Add(5 * time.Second)
	for sumLens(collected) < total && time.Now().Before(deadline) {
		s.dispatchPass(context.Background())

		// Collect this round's AssignJob ids across both workers.
		round := []struct{ wID, jobID string }{}
		drained := true
		for drained {
			drained = false
			for wID, info := range infos {
				select {
				case jobID := <-info.assign:
					collected[wID] = append(collected[wID], jobID)
					round = append(round, struct{ wID, jobID string }{wID, jobID})
					drained = true
				default:
				}
			}
		}

		// Complete this round's jobs out of order so slots free up for the
		// next pass.
		rng.Shuffle(len(round), func(i, j int) { round[i], round[j] = round[j], round[i] })
		for _, term := range round {
			infos[term.wID].w.DeliverJobChunk(term.jobID, &worker.JobChunk{
				Type:  worker.JobChunkTypeCompleted,
				Final: []byte("ok"),
			})
		}
		// Let the pump goroutines drain the terminal frames (finishInflight)
		// before the next pass reads capacity.
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, total, sumLens(collected),
		"every submitted job must reach AssignJob across the worker pool")
	// Both workers must receive at least one job — guards against
	// pathological all-on-one-worker placement.
	require.NotEmpty(t, collected["w-fast"], "fast worker must receive ≥1 job")
	require.NotEmpty(t, collected["w-slow"], "slow worker must receive ≥1 job")

	// Wait for every result to reach Done. Pump goroutines run
	// asynchronously.
	for _, rid := range requestIDs {
		done := false
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			res, err := s.results.Get(rid)
			require.NoError(t, err)
			if res != nil && res.Status == queue.ResultStatusDone {
				done = true
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		require.True(t, done, "request %s never reached Done", rid)
	}

	// --- Conservation invariants ---

	tracker.mu.Lock()
	credit := tracker.credit
	debit := tracker.debit
	minObserved := tracker.minObserved
	tracker.mu.Unlock()
	require.InDelta(t, credit, debit, 1e-9,
		"sum_of_credit == sum_of_debit (credit=%v debit=%v)", credit, debit)
	require.GreaterOrEqual(t, minObserved, -1e-9,
		"tail never goes negative mid-flight (min observed: %v)", minObserved)

	// Every per-queue tail row must be exactly 0 at the end.
	for _, wID := range workerIDs {
		row, err := st.GetWorkerQueueState("worker|" + wID)
		require.NoError(t, err)
		require.InDelta(t, 0.0, row.TailSeconds, 1e-9,
			"final tail for %s must be 0", wID)
	}

	// Inflight maps must be empty (no leftover keys).
	s.inflightMu.Lock()
	require.Empty(t, s.inflightSeconds,
		"inflightSeconds map must be empty after all jobs terminate")
	require.Empty(t, s.inflightByRequest,
		"inflightByRequest map must be empty after all jobs terminate")
	s.inflightMu.Unlock()
}

// A burst of more jobs than the worker's pool_size must not dispatch past
// the pool before any job completes. The worker's heartbeat
// AvailableCapacity lags real-time dispatch, so without netting MASS's own
// in-flight count the dispatcher would re-use the same free-slot count
// across kick()-triggered passes and over-dispatch (the "3 running in a
// pool of 2" symptom). No terminal frames are delivered, so capacity only
// frees if MASS accounts for its own dispatches.
func TestDispatcher_BurstRespectsPoolSize(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	const poolSize = 2
	const burst = 6

	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	assign := make(chan string, burst)
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(poolSize)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: poolSize}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if aj := msg.GetAssignJob(); aj != nil {
			select {
			case assign <- aj.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	for range burst {
		_, err := s.Submit(context.Background(), SubmitRequest{
			RuntimeName: runtimeName, ModelID: modelID,
			Payload: []byte("p"), Cost: 100, CostAxis: "q4k_matvec",
		})
		require.NoError(t, err)
	}

	// Several passes — the fake worker never decrements its advertised
	// capacity, so only MASS's own inflight accounting can hold the line.
	for range 5 {
		s.dispatchPass(context.Background())
		waitDispatchIdle(t, s)
	}

	dispatched := 0
	for {
		select {
		case <-assign:
			dispatched++
			continue
		default:
		}
		break
	}
	require.Equal(t, poolSize, dispatched,
		"a burst must dispatch exactly pool_size jobs before any completion, not more")

	// The rest stay queued on the worker, ready to dispatch as slots free.
	s.inflightMu.Lock()
	inflight := len(s.inflightByRequest)
	s.inflightMu.Unlock()
	require.Equal(t, poolSize, inflight, "inflight count must equal pool_size")
}

// Once a heartbeat reports the running jobs, MASS must not debit them a
// second time. The worker's available_capacity is already net of its
// active jobs, so subtracting the full in-flight count on top of it made a
// pool of N dispatch at ~N/2 and stalled the queue until in-flight hit 0.
func TestDispatcher_SyncedHeartbeatDoesNotDoubleCountInflight(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	const poolSize = 2

	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	assign := make(chan string, 8)
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(poolSize)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: poolSize}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if aj := msg.GetAssignJob(); aj != nil {
			select {
			case assign <- aj.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	for range poolSize + 1 {
		_, err := s.Submit(context.Background(), SubmitRequest{
			RuntimeName: runtimeName, ModelID: modelID,
			Payload: []byte("p"), Cost: 100, CostAxis: "q4k_matvec",
		})
		require.NoError(t, err)
	}

	drive := func() []string {
		var got []string
		for range 5 {
			s.dispatchPass(context.Background())
			waitDispatchIdle(t, s)
			for {
				select {
				case jobID := <-assign:
					got = append(got, jobID)
					continue
				default:
				}
				break
			}
		}
		return got
	}

	running := drive()
	require.Len(t, running, poolSize, "the pool must fill and stop")

	// The next heartbeat catches up: the worker reports both jobs active
	// and no free slot. Still saturated — nothing more may dispatch.
	w.SetFakeCapacity(0)
	w.SetFakeActiveJobs(poolSize)
	require.Empty(t, drive(), "a saturated worker must not receive more jobs")

	// One job finishes and the heartbeat reports the freed slot alongside
	// the one job still running. The queued job must go out now: the
	// remaining job is already accounted for in available_capacity.
	w.DeliverJobChunk(running[0], &worker.JobChunk{
		Type: worker.JobChunkTypeCompleted, Final: []byte("ok"),
	})
	pollUntilEqual(t, poolSize-1, func() int {
		return s.inflightCountForWorker("w1")
	}, "the completed job must clear from the inflight map")
	w.SetFakeCapacity(1)
	w.SetFakeActiveJobs(poolSize - 1)

	require.Len(t, drive(), 1, "the freed slot must dispatch the queued job")
}

// A terminal frame must wake the dispatcher. Deleting the job's rows
// doesn't signal the queue pool, so the job queued behind it used to wait
// out the loop's 2s ticker — dead time between every pair of jobs on a
// pool_size=1 worker. Driven through the real loop (Start), since the
// gap is in the loop's wake-up sources.
func TestDispatcher_JobTerminalWakesDispatcher(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"

	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	assign := make(chan string, 2)
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(1)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if aj := msg.GetAssignJob(); aj != nil {
			select {
			case assign <- aj.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.Start(ctx)

	for range 2 {
		_, err := s.Submit(ctx, SubmitRequest{
			RuntimeName: runtimeName, ModelID: modelID,
			Payload: []byte("p"), Cost: 100, CostAxis: "q4k_matvec",
		})
		require.NoError(t, err)
	}

	var first string
	select {
	case first = <-assign:
	case <-time.After(2 * time.Second):
		t.Fatal("the first job never reached the worker")
	}

	// The single slot is taken: the second job waits on the worker queue.
	select {
	case <-assign:
		t.Fatal("a pool of 1 must not run two jobs at once")
	case <-time.After(250 * time.Millisecond):
	}

	// Finish the first job and touch nothing else — no kick, no
	// dispatchPass. The second assign has to land well inside the
	// ticker interval.
	w.DeliverJobChunk(first, &worker.JobChunk{
		Type: worker.JobChunkTypeCompleted, Final: []byte("ok"),
	})
	select {
	case <-assign:
	case <-time.After(time.Second):
		t.Fatal("the freed slot waited for the dispatcher ticker instead of the completion")
	}
}

// tailTracker wraps a StateStoreInterface and counts every tail
// credit/debit, plus tracks the running sum so a negative observation
// becomes visible to the test. Every other call — including
// SetTailSeconds, which replaces rather than deltas — passes through
// the embedded interface unchanged.
type tailTracker struct {
	StateStoreInterface

	mu          sync.Mutex
	credit      float64 // sum of positive deltas
	debit       float64 // sum of |negative deltas|
	running     map[string]float64
	minObserved float64
}

func (t *tailTracker) record(queueName string, delta float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if delta >= 0 {
		t.credit += delta
	} else {
		t.debit += -delta
	}
	if t.running == nil {
		t.running = map[string]float64{}
	}
	t.running[queueName] += delta
	if t.running[queueName] < t.minObserved {
		t.minObserved = t.running[queueName]
	}
}

func (t *tailTracker) AddTailSeconds(queueName string, delta float64) error {
	t.record(queueName, delta)
	return t.StateStoreInterface.AddTailSeconds(queueName, delta)
}

func (t *tailTracker) AddTailSecondsAndSetModel(queueName string, delta float64, newModelID string) error {
	t.record(queueName, delta)
	return t.StateStoreInterface.AddTailSecondsAndSetModel(queueName, delta, newModelID)
}

// sumLens totals the lengths of every slice value in m.
func sumLens(m map[string][]string) int {
	n := 0
	for _, v := range m {
		n += len(v)
	}
	return n
}

// Worker disconnect mid-burst: jobs that were inflight or queued on
// the lost worker must redistribute onto the surviving peer and reach
// Done. The contract has three halves: pump (must NOT fail the result
// on disconnect), OnWorkerDisconnected (must release queued anchors
// and clear inflight bookkeeping), and drainGlobal+dispatch (must
// re-place orphaned anchors onto the peer). Each is pinned by a
// narrower test; this one verifies they compose into the operator-
// visible "no job is lost" property.
func TestDispatcher_WorkerDisconnectMidBurst_RedistributesAndCompletes(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	const total = 8

	s, st := newTestScheduler(t)

	// Two workers with the model resident. capacity=4 each, so the
	// 8-job burst fully dispatches: 4 inflight on each worker.
	type wInfo struct {
		w      *worker.StreamWorker
		assign chan string
	}
	infos := map[string]*wInfo{}
	for _, wID := range []string{"w-lost", "w-survivor"} {
		require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
			Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
		}))
		info := &wInfo{assign: make(chan string, total)}
		w := worker.NewFakeStreamWorker(wID, runtimeName,
			[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
		w.SetFakeCapacity(4)
		w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 4}})
		w.SetFakeSender(func(msg *workerpb.HubMessage) error {
			if aj := msg.GetAssignJob(); aj != nil {
				select {
				case info.assign <- aj.GetJobId():
				default:
				}
			}
			return nil
		})
		require.NoError(t, s.workers.Register(w))
		s.OnWorkerConnected(w)
		info.w = w
		infos[wID] = info
	}

	requestIDs := make([]string, 0, total)
	for range total {
		rid, err := s.Submit(context.Background(), SubmitRequest{
			RuntimeName: runtimeName,
			ModelID:     modelID,
			Payload:     []byte("p"),
			Cost:        100, CostAxis: "q4k_matvec",
		})
		require.NoError(t, err)
		requestIDs = append(requestIDs, rid)
	}

	// Drive dispatch until the whole burst has been assigned — each pass
	// takes up to AvailableCapacity per worker, and the per-queue drains
	// complete asynchronously, so keep passing until the counts converge.
	collected := map[string][]string{}
	deadline := time.Now().Add(2 * time.Second)
	for sumLens(collected) < total && time.Now().Before(deadline) {
		s.dispatchPass(context.Background())
		for wID, info := range infos {
			select {
			case jobID := <-info.assign:
				collected[wID] = append(collected[wID], jobID)
			default:
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	require.Equal(t, total, sumLens(collected),
		"every submitted job must reach AssignJob")
	lostBefore := len(collected["w-lost"])
	survivorBefore := len(collected["w-survivor"])
	require.Positive(t, lostBefore, "w-lost must receive ≥1 job before disconnect")
	require.Positive(t, survivorBefore, "w-survivor must receive ≥1 job before disconnect")

	// Disconnect w-lost mid-burst. SetOffline closes every in-flight
	// job channel without a terminal frame — exactly the production
	// shape. OnWorkerDisconnected reaps the worker queue back to global.
	infos["w-lost"].w.SetOffline()
	s.OnWorkerDisconnected("w-lost")

	// Inflight bookkeeping for w-lost must clear via the pump
	// goroutines exiting on the closed channel. Bounded-poll.
	pollUntilEqual(t, 0, func() int {
		return int(s.getInflightSeconds("worker|w-lost"))
	}, "inflight_seconds for the lost worker must clear after disconnect")

	// w-survivor is fully booked with its original jobs; complete them
	// to free capacity for the redistributed orphans. This mirrors
	// reality: the survivor finishes its own work, and the dispatcher
	// places the orphans on freed slots over subsequent ticks.
	originalSurvivor := append([]string(nil), collected["w-survivor"]...)
	for _, jobID := range originalSurvivor {
		infos["w-survivor"].w.DeliverJobChunk(jobID, &worker.JobChunk{
			Type:  worker.JobChunkTypeCompleted,
			Final: []byte("ok"),
		})
	}

	// drainGlobal places the orphaned anchors onto the survivor as
	// capacity frees. Drive dispatch passes until every job has been
	// (re-)assigned.
	driveDeadline := time.Now().Add(5 * time.Second)
	for sumLens(collected) < survivorBefore+lostBefore+survivorBefore &&
		time.Now().Before(driveDeadline) {
		s.dispatchPass(context.Background())
		for {
			select {
			case jobID := <-infos["w-survivor"].assign:
				collected["w-survivor"] = append(collected["w-survivor"], jobID)
				continue
			default:
			}
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// After redistribution, w-survivor's collected jobs are the
	// originals plus the redistributed ones from w-lost.
	redistributed := len(collected["w-survivor"]) - survivorBefore
	require.Equal(t, lostBefore, redistributed,
		"all %d jobs from w-lost must reappear as AssignJobs on w-survivor (got %d redistributed)",
		lostBefore, redistributed)

	// Deliver Completed frames for the newly-assigned redistributed
	// jobs. The originals were already finished above.
	for _, jobID := range collected["w-survivor"][survivorBefore:] {
		infos["w-survivor"].w.DeliverJobChunk(jobID, &worker.JobChunk{
			Type:  worker.JobChunkTypeCompleted,
			Final: []byte("ok"),
		})
	}

	// Every original RequestID must reach Done. None should be Error
	// (would mean the pump or reaper wrongly wrote a failure result).
	for _, rid := range requestIDs {
		done := false
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			res, err := s.results.Get(rid)
			require.NoError(t, err)
			require.NotEqual(t, queue.ResultStatusError, res.Status,
				"result for %s must NOT transition to Error — disconnect should redistribute, not fail", rid)
			if res.Status == queue.ResultStatusDone {
				done = true
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		require.True(t, done, "request %s never reached Done after redistribution", rid)
	}

	// Bookkeeping must be empty on both workers' queue names.
	s.inflightMu.Lock()
	require.Empty(t, s.inflightSeconds,
		"inflightSeconds map must be empty after redistribution completes")
	require.Empty(t, s.inflightByRequest,
		"inflightByRequest map must be empty after redistribution completes")
	s.inflightMu.Unlock()
}

// Idle eviction races a Submit for the just-evicted model. Setup: a
// worker has ModelA loaded with Active=0 and IdleSince older than TTL.
// In one goroutine, evictIdleOnce fires UnloadModel (sender blocks
// until the test releases it). Concurrently, a fresh Submit for ModelA
// races through inline placeOnWorkerQueue. After releasing the unload
// + driving dispatch + delivering Completed, the test asserts the
// race didn't leak bookkeeping (inflight maps empty, tail at zero,
// result Done).
func TestEvictIdleOnce_RacesSubmitForSameModel_NoBookkeepingLeak(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-a"

	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	var (
		assignCh      = make(chan string, 4)
		unloadJobID   = make(chan string, 1)
		releaseUnload = make(chan struct{})
		loadJobID     = make(chan string, 1)
	)
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(4)
	// Resident, idle past TTL — this is what evictIdleOnce hunts.
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: modelID, PoolSize: 4, Active: 0,
		IdleSince: time.Now().Add(-2 * time.Minute),
		DeviceIDs: []string{"gpu:0"},
	}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if aj := msg.GetAssignJob(); aj != nil {
			select {
			case assignCh <- aj.GetJobId():
			default:
			}
		}
		if um := msg.GetUnloadModel(); um != nil {
			select {
			case unloadJobID <- um.GetJobId():
			default:
			}
			// Block until the test releases — simulates UnloadModel
			// taking time on a real worker.
			<-releaseUnload
		}
		if lm := msg.GetLoadModel(); lm != nil {
			select {
			case loadJobID <- lm.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	// Goroutine A: idle eviction. Will block at sender's UnloadModel
	// until releaseUnload closes.
	evictDone := make(chan struct{})
	go func() {
		s.evictIdleOnce(time.Minute)
		close(evictDone)
	}()

	// Wait until the unload has been issued — that's the race window
	// where the worker reports ModelA still resident but it's about to
	// go away.
	var unloadID string
	select {
	case unloadID = <-unloadJobID:
	case <-time.After(2 * time.Second):
		t.Fatal("evictIdleOnce did not call UnloadModel within 2s")
	}

	// Goroutine B: fresh Submit for the same model. Lands while evict
	// is mid-flight on the wire.
	requestID, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "q4k_matvec",
		Files:     []*workerpb.ModelFile{{Url: "http://x/y.gguf", Filename: "y.gguf", SizeBytes: 1024}},
		LoadHints: []byte("h"),
	})
	require.NoError(t, err)

	// Release the unload so EvictModel returns and the worker reports
	// ModelA as gone. The next dispatch tick must observe the
	// not-resident state and load-on-demand for the new Submit.
	w.DeliverUnloadResult(unloadID, "")
	close(releaseUnload)
	<-evictDone

	// After evict, simulate the worker's heartbeat dropping ModelA.
	w.SetFakeLoadedModels(nil)

	// Drive dispatch. May need a load-on-demand (worker now
	// non-resident), then AssignJob.
	dispatchDone := make(chan struct{})
	go func() {
		s.dispatchPass(context.Background())
		close(dispatchDone)
	}()

	// Ack the load-on-demand if it fires (the gate may evict zero
	// overlaps since nothing's resident, then loads).
	select {
	case lid := <-loadJobID:
		// Mark the worker as holding the freshly-loaded model with
		// Active>0 so the heartbeat-driven gate stays consistent if
		// other passes fire.
		w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
			ModelID: modelID, PoolSize: 4, Active: 1,
			DeviceIDs: []string{"gpu:0"},
		}})
		w.DeliverLoadResult(lid, worker.LoadResult{PoolSize: 4}, "")
	case <-time.After(2 * time.Second):
		t.Fatal("expected LoadModel after the racing Submit")
	}

	// Now AssignJob will fire — collect its jobID and deliver
	// Completed.
	var jobID string
	select {
	case jobID = <-assignCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expected AssignJob after LoadModel")
	}
	<-dispatchDone

	w.DeliverJobChunk(jobID, &worker.JobChunk{
		Type:  worker.JobChunkTypeCompleted,
		Final: []byte("ok"),
	})

	// Bounded-poll for the result.
	done := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		res, err := s.results.Get(requestID)
		require.NoError(t, err)
		if res != nil && res.Status == queue.ResultStatusDone {
			done = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	require.True(t, done, "racing Submit must reach Done despite the eviction race")

	// Bookkeeping invariants after the race resolves.
	s.inflightMu.Lock()
	require.Empty(t, s.inflightSeconds,
		"inflightSeconds map must be empty after the race resolves")
	require.Empty(t, s.inflightByRequest,
		"inflightByRequest map must be empty after the race resolves")
	s.inflightMu.Unlock()

	row, err := st.GetWorkerQueueState("worker|w1")
	require.NoError(t, err)
	require.InDelta(t, 0.0, row.TailSeconds, 1e-9,
		"tail must end at 0 — credit-debit balance held across the race")
}

// AssignJob failures (worker offline, send error) must release the
// inflight slot — otherwise the failed-to-dispatch job leaks weight
// onto its worker forever.
func TestDispatcher_AssignJobFailureReleasesInflight(t *testing.T) {
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	w := worker.NewFakeStreamWorker("w1", "llama-cpp",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(4)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: "m-1", PoolSize: 1}})

	// Sender that always errors — AssignJob will fail at the wire.
	w.SetFakeSender(func(*workerpb.HubMessage) error {
		return errFakeSenderClosed
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	_, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: "llama-cpp",
		ModelID:     "m-1",
		Payload:     []byte("p"),
		Cost:        750, CostAxis: "q4k_matvec",
	})
	require.NoError(t, err)

	s.dispatchPass(context.Background())
	waitDispatchIdle(t, s)

	// Even with the dispatch failure, inflight must be back to 0 —
	// dispatchEnvelope's error path explicitly calls finishInflight.
	require.InDelta(t, 0.0, s.getInflightSeconds("worker|w1"), 0.001,
		"AssignJob failure must not leak inflight seconds")
}

// Work stealing rebalances queue depth across runtime-matched workers
// when an idle peer can serve a job. Three cases cover the policy:
// successful steal above threshold, no-steal below threshold, no-steal
// when the would-be stealer can't serve the model.
func TestAttemptSteals_Rebalances(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"

	// fillQueue plants n envelopes directly on q (bypassing the
	// dispatcher) so tests can stage depth-based scenarios.
	fillQueue := func(t *testing.T, q queue.QueueInterface, n int) {
		t.Helper()
		for range n {
			_, err := q.Submit(context.Background(), queue.Envelope{
				Priority:    queue.PriorityMedium,
				RuntimeName: runtimeName,
				ModelID:     modelID,
				Payload:     []byte("p"),
				Cost:        100,
				CostAxis:    "q4k_matvec",
			})
			require.NoError(t, err)
		}
	}

	tests := []struct {
		name             string
		busyDepth        int
		idleHasModel     bool
		wantStealOccured bool
	}{
		{
			// busyDepth must exceed stealThreshold (2) for a steal to
			// fire; the idle worker must have the model resident so
			// the row's model_id check passes.
			name:             "above threshold + affined idle steals one row",
			busyDepth:        4,
			idleHasModel:     true,
			wantStealOccured: true,
		},
		{
			name:             "below threshold leaves queues alone",
			busyDepth:        2, // == stealThreshold, not >
			idleHasModel:     true,
			wantStealOccured: false,
		},
		{
			// Residency is no longer a hard gate. An idle worker without
			// the model still steals — dispatchEnvelope will load it on
			// demand at pop time using the envelope's inline artifacts.
			name:             "idle without model still steals (load-on-demand at pop)",
			busyDepth:        4,
			idleHasModel:     false,
			wantStealOccured: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, st := newTestScheduler(t)
			for _, wID := range []string{"busy", "idle"} {
				require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
					WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
					Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
				}))
			}

			busy := worker.NewFakeStreamWorker("busy", runtimeName,
				[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
			busy.SetFakeCapacity(4)
			busy.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})

			idle := worker.NewFakeStreamWorker("idle", runtimeName,
				[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
			idle.SetFakeCapacity(4)
			if tt.idleHasModel {
				idle.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
			}

			require.NoError(t, s.workers.Register(busy))
			require.NoError(t, s.workers.Register(idle))
			s.OnWorkerConnected(busy)
			s.OnWorkerConnected(idle)

			s.queueMu.RLock()
			busyQ := s.devQueues["worker|busy"]
			idleQ := s.devQueues["worker|idle"]
			s.queueMu.RUnlock()
			require.NotNil(t, busyQ)
			require.NotNil(t, idleQ)

			fillQueue(t, busyQ, tt.busyDepth)
			ctx := context.Background()
			require.Equal(t, tt.busyDepth, mustDepth(t, busyQ, ctx))
			require.Equal(t, 0, mustDepth(t, idleQ, ctx))

			s.attemptSteals(ctx)

			busyAfter := mustDepth(t, busyQ, ctx)
			idleAfter := mustDepth(t, idleQ, ctx)
			if tt.wantStealOccured {
				require.Equal(t, tt.busyDepth-1, busyAfter, "busy queue should shrink by 1")
				require.Equal(t, 1, idleAfter, "idle queue should gain 1")
			} else {
				require.Equal(t, tt.busyDepth, busyAfter, "busy queue should be unchanged")
				require.Equal(t, 0, idleAfter, "idle queue should be unchanged")
			}
		})
	}
}

// Stealing must rebalance correctly when multiple idle workers contend
// over the same deep peer. Three idle workers and one busy worker
// (depth=4): each idle should steal one row, leaving the busy with
// depth=1. The single peer can't lose more than one row to any single
// idle (Peek+MoveTo claims one); the multi-idle iteration plays out
// across all three.
func TestAttemptSteals_MultipleIdlesContendForSamePeer(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"

	s, st := newTestScheduler(t)
	for _, wID := range []string{"busy", "idle1", "idle2", "idle3"} {
		require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
			Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
		}))
	}

	busy := worker.NewFakeStreamWorker("busy", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	busy.SetFakeCapacity(4)
	busy.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	require.NoError(t, s.workers.Register(busy))
	s.OnWorkerConnected(busy)

	for _, idleID := range []string{"idle1", "idle2", "idle3"} {
		w := worker.NewFakeStreamWorker(idleID, runtimeName,
			[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
		w.SetFakeCapacity(4)
		w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
		require.NoError(t, s.workers.Register(w))
		s.OnWorkerConnected(w)
	}

	s.queueMu.RLock()
	busyQ := s.devQueues["worker|busy"]
	idleQs := map[string]queue.QueueInterface{
		"idle1": s.devQueues["worker|idle1"],
		"idle2": s.devQueues["worker|idle2"],
		"idle3": s.devQueues["worker|idle3"],
	}
	s.queueMu.RUnlock()

	for range 4 {
		_, err := busyQ.Submit(context.Background(), queue.Envelope{
			Priority:    queue.PriorityMedium,
			RuntimeName: runtimeName,
			ModelID:     modelID,
			Payload:     []byte("p"),
			Cost:        100,
			CostAxis:    "q4k_matvec",
		})
		require.NoError(t, err)
	}

	ctx := context.Background()
	s.attemptSteals(ctx)

	// Each idle steals exactly one row → busy drops from 4 to 1.
	require.Equal(t, 1, mustDepth(t, busyQ, ctx), "busy should lose 3 rows to 3 idles")
	total := 0
	for _, q := range idleQs {
		total += mustDepth(t, q, ctx)
	}
	require.Equal(t, 3, total, "exactly one row should land on each idle")
}

// Stealing skips workers whose AvailableCapacity is 0 AND who are
// resident on the model — that's a saturated worker, not an idle one.
// A non-resident peer reports cap=0 pre-load but is still a valid steal
// destination (matches pickWorkerQueue's residency-aware capacity gate).
// This test pins the "cap=0 + non-resident is still idle for stealing"
// behavior.
func TestAttemptSteals_IdleSkippedWhenCapacityZero(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"

	s, st := newTestScheduler(t)
	for _, wID := range []string{"busy", "saturated"} {
		require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
			Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
		}))
	}

	busy := worker.NewFakeStreamWorker("busy", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	busy.SetFakeCapacity(4)
	busy.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})

	// Saturated idle peer: resident, capacity=0 → stealing must skip it.
	saturated := worker.NewFakeStreamWorker("saturated", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	saturated.SetFakeCapacity(0)
	saturated.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})

	require.NoError(t, s.workers.Register(busy))
	require.NoError(t, s.workers.Register(saturated))
	s.OnWorkerConnected(busy)
	s.OnWorkerConnected(saturated)

	s.queueMu.RLock()
	busyQ := s.devQueues["worker|busy"]
	satQ := s.devQueues["worker|saturated"]
	s.queueMu.RUnlock()

	for range 4 {
		_, err := busyQ.Submit(context.Background(), queue.Envelope{
			Priority:    queue.PriorityMedium,
			RuntimeName: runtimeName,
			ModelID:     modelID,
			Payload:     []byte("p"),
			Cost:        100,
			CostAxis:    "q4k_matvec",
		})
		require.NoError(t, err)
	}

	ctx := context.Background()
	s.attemptSteals(ctx)

	require.Equal(t, 4, mustDepth(t, busyQ, ctx),
		"saturated worker must not steal — its capacity is real saturation, not pre-load idle")
	require.Equal(t, 0, mustDepth(t, satQ, ctx),
		"saturated worker's queue must stay empty")
}

// Stealing transfers tail_seconds bookkeeping atomically with the row
// move: peer's tail debits by the stolen envelope's QueuedSeconds and
// the idle's tail credits by the same amount. Without this, scoring
// after a steal would over-count peer's load and under-count idle's.
func TestAttemptSteals_TransfersTailBookkeeping(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"

	s, st := newTestScheduler(t)
	for _, wID := range []string{"busy", "idle"} {
		require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
			Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
		}))
	}

	busy := worker.NewFakeStreamWorker("busy", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	busy.SetFakeCapacity(4)
	busy.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})

	idle := worker.NewFakeStreamWorker("idle", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	idle.SetFakeCapacity(4)
	idle.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})

	require.NoError(t, s.workers.Register(busy))
	require.NoError(t, s.workers.Register(idle))
	s.OnWorkerConnected(busy)
	s.OnWorkerConnected(idle)

	s.queueMu.RLock()
	busyQ := s.devQueues["worker|busy"]
	s.queueMu.RUnlock()

	const stolenQSec = 4.2
	for range 4 {
		_, err := busyQ.Submit(context.Background(), queue.Envelope{
			Priority:      queue.PriorityMedium,
			RuntimeName:   runtimeName,
			ModelID:       modelID,
			Payload:       []byte("p"),
			Cost:          100,
			CostAxis:      "q4k_matvec",
			QueuedSeconds: stolenQSec,
		})
		require.NoError(t, err)
	}
	// Credit busy's tail manually since we bypassed Submit's accounting.
	s.creditTail("worker|busy", 4*stolenQSec, modelID)

	pre, err := st.GetWorkerQueueState("worker|busy")
	require.NoError(t, err)
	require.InDelta(t, 4*stolenQSec, pre.TailSeconds, 0.001)

	ctx := context.Background()
	s.attemptSteals(ctx)

	post, err := st.GetWorkerQueueState("worker|busy")
	require.NoError(t, err)
	require.InDelta(t, 3*stolenQSec, post.TailSeconds, 0.001,
		"busy tail must shrink by exactly one stolen row's QueuedSeconds")

	idleRow, err := st.GetWorkerQueueState("worker|idle")
	require.NoError(t, err)
	require.InDelta(t, stolenQSec, idleRow.TailSeconds, 0.001,
		"idle tail must grow by exactly one stolen row's QueuedSeconds")
	require.Equal(t, modelID, idleRow.TailModelID,
		"idle's tail_model_id must be set to the stolen envelope's ModelID")
}

// Work stealing must respect the same eligibility predicates the picker
// scores with: an idle worker that cannot fetch the envelope's files
// (URL-less local-path artifact on a non-loopback worker) or has no
// benched throughput axis for it must not steal — the dispatch would be
// guaranteed to fail and burn the envelope's load-attempt budget.
func TestAttemptSteals_GatedOnWorkerEligibility(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	localFiles := []*workerpb.ModelFile{{LocalPath: "/data/models/m.gguf", SizeBytes: 1}}

	tests := []struct {
		name         string
		idleLoopback bool
		costAxis     string
		files        []*workerpb.ModelFile
		wantSteal    bool
	}{
		{
			name:         "non-loopback idle cannot steal a local-path artifact",
			idleLoopback: false,
			costAxis:     "q4k_matvec",
			files:        localFiles,
			wantSteal:    false,
		},
		{
			name:         "loopback idle steals the same envelope",
			idleLoopback: true,
			costAxis:     "q4k_matvec",
			files:        localFiles,
			wantSteal:    true,
		},
		{
			name:         "unbenched cost axis blocks the steal",
			idleLoopback: true,
			costAxis:     "exotic_axis",
			wantSteal:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, st := newTestScheduler(t)
			for _, wID := range []string{"busy", "idle"} {
				require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
					WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
					Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
				}))
			}

			busy := worker.NewFakeStreamWorker("busy", runtimeName,
				[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
			busy.SetFakeCapacity(4)
			busy.SetFakeLoopback(true)
			busy.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})

			idle := worker.NewFakeStreamWorker("idle", runtimeName,
				[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
			idle.SetFakeCapacity(4)
			idle.SetFakeLoopback(tt.idleLoopback)
			idle.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})

			require.NoError(t, s.workers.Register(busy))
			require.NoError(t, s.workers.Register(idle))
			s.OnWorkerConnected(busy)
			s.OnWorkerConnected(idle)

			s.queueMu.RLock()
			busyQ := s.devQueues["worker|busy"]
			idleQ := s.devQueues["worker|idle"]
			s.queueMu.RUnlock()

			for range 4 {
				_, err := busyQ.Submit(context.Background(), queue.Envelope{
					Priority:    queue.PriorityMedium,
					RuntimeName: runtimeName,
					ModelID:     modelID,
					Payload:     []byte("p"),
					Cost:        100,
					CostAxis:    tt.costAxis,
					Files:       tt.files,
				})
				require.NoError(t, err)
			}

			ctx := context.Background()
			s.attemptSteals(ctx)

			if tt.wantSteal {
				require.Equal(t, 3, mustDepth(t, busyQ, ctx), "eligible idle must steal one row")
				require.Equal(t, 1, mustDepth(t, idleQ, ctx), "stolen row must land on the idle queue")
			} else {
				require.Equal(t, 4, mustDepth(t, busyQ, ctx), "ineligible idle must not steal")
				require.Equal(t, 0, mustDepth(t, idleQ, ctx), "ineligible idle queue must stay empty")
			}
		})
	}
}

// Stealing across runtimes must NOT cross runtime boundaries. A busy
// llama-cpp worker can't have a row stolen by an idle ort worker even
// when both are operator-enabled and benched: dispatchEnvelope would
// fail because the worker doesn't speak the same runtime.
func TestAttemptSteals_DoesNotCrossRuntimes(t *testing.T) {
	s, st := newTestScheduler(t)
	for _, wID := range []string{"busy", "idle"} {
		require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
			Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
		}))
	}

	busy := worker.NewFakeStreamWorker("busy", "llama-cpp",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	busy.SetFakeCapacity(4)
	busy.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: "m-1", PoolSize: 1}})

	// Different runtime — must be isolated from busy's queue.
	idle := worker.NewFakeStreamWorker("idle", "other-runtime",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	idle.SetFakeCapacity(4)

	require.NoError(t, s.workers.Register(busy))
	require.NoError(t, s.workers.Register(idle))
	s.OnWorkerConnected(busy)
	s.OnWorkerConnected(idle)

	s.queueMu.RLock()
	busyQ := s.devQueues["worker|busy"]
	idleQ := s.devQueues["worker|idle"]
	s.queueMu.RUnlock()

	for range 4 {
		_, err := busyQ.Submit(context.Background(), queue.Envelope{
			Priority:    queue.PriorityMedium,
			RuntimeName: "llama-cpp",
			ModelID:     "m-1",
			Payload:     []byte("p"),
			Cost:        100,
			CostAxis:    "q4k_matvec",
		})
		require.NoError(t, err)
	}

	ctx := context.Background()
	s.attemptSteals(ctx)

	require.Equal(t, 4, mustDepth(t, busyQ, ctx),
		"work-stealing must be runtime-scoped — other-runtime idle cannot take llama-cpp rows")
	require.Equal(t, 0, mustDepth(t, idleQ, ctx),
		"other-runtime idle queue must stay empty")
}

// Three workers with equal power: sequential Submits should distribute
// using the in-flight + tail scoring. The first submit lands on w1
// (insertion order), the second on w2 (w1 now has positive inflight),
// the third on w3. Locks in that Submit's inline placement reads the
// updated load between calls.
func TestSubmit_DistributesAcrossEqualWorkers(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, st := newTestScheduler(t)

	workerIDs := []string{"w1", "w2", "w3"}
	senders := make(map[string]*sync.Mutex)
	captured := make(map[string]*string)
	release := make(chan struct{})

	for _, wID := range workerIDs {
		require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
			Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
		}))
		w := worker.NewFakeStreamWorker(wID, runtimeName,
			[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
		w.SetFakeCapacity(4)
		w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
		var mu sync.Mutex
		var cap string
		senders[wID] = &mu
		captured[wID] = &cap
		// Block AssignJob so the dispatched envelope stays "in flight"
		// and subsequent Submit calls score against a worker with a real
		// inflight contribution.
		w.SetFakeSender(func(msg *workerpb.HubMessage) error {
			if aj := msg.GetAssignJob(); aj != nil {
				mu.Lock()
				cap = aj.GetJobId()
				mu.Unlock()
				<-release
			}
			return nil
		})
		require.NoError(t, s.workers.Register(w))
		s.OnWorkerConnected(w)
	}

	for i := range 3 {
		_, err := s.Submit(context.Background(), SubmitRequest{
			RuntimeName: runtimeName,
			ModelID:     modelID,
			Payload:     []byte("p"),
			Cost:        100, CostAxis: "q4k_matvec",
		})
		require.NoError(t, err, "submit %d", i)

		// Drive a dispatch pass on a goroutine — it'll block at AssignJob.
		done := make(chan struct{})
		go func() {
			s.dispatchPass(context.Background())
			close(done)
		}()
		// Wait until SOME worker has captured the AssignJob.
		deadline := time.After(2 * time.Second)
	wait:
		for {
			for _, wID := range workerIDs {
				senders[wID].Lock()
				v := *captured[wID]
				senders[wID].Unlock()
				if v != "" {
					// Clear so the next iteration measures the next dispatch.
					senders[wID].Lock()
					*captured[wID] = ""
					senders[wID].Unlock()
					break wait
				}
			}
			select {
			case <-deadline:
				t.Fatalf("submit %d never reached AssignJob", i)
			default:
				time.Sleep(5 * time.Millisecond)
			}
		}
		_ = done // dispatch goroutine stays blocked on release; that's fine
	}

	// All three workers must be carrying exactly one inflight envelope.
	for _, wID := range workerIDs {
		require.InDelta(t, 1.0, s.getInflightSeconds("worker|"+wID), 0.001,
			"each worker must have exactly one inflight envelope after distributed Submit")
	}

	close(release)
}

// Submit pre-flight refuses when no online worker matches the runtime;
// after a worker registers, subsequent Submit calls succeed. Locks in
// that the preflight check reads the live fleet — not a cached
// snapshot — and that no envelope leaks into the global queue during
// the no-worker window.
func TestSubmit_RecoversAfterWorkerLateConnects(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, _ := newTestScheduler(t)

	_, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "q4k_matvec",
	})
	require.ErrorIs(t, err, ErrNoWorker)

	st := s.store.(*store.Store) // backing store for the live scheduler
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(4)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	rid, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "q4k_matvec",
	})
	require.NoError(t, err)
	require.NotEmpty(t, rid)
}

// drainGlobal must place rows that were Submit'd before any worker was
// available. The plot: connect no workers → Submit fails (covered
// above); but if a row exists on global with no schedulable worker
// (stale row from a crash, or a worker that disconnected between
// pre-flight and inline placement), drainGlobal must pick it up once a
// worker materialises. Drives the dispatcher loop's retry semantics.
func TestDrainGlobal_PlacesOrphanRowAfterWorkerConnects(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, st := newTestScheduler(t)

	// Plant an unplaced row directly on global, bypassing Submit's
	// preflight (which would fail with no workers).
	s.queueMu.RLock()
	globalQ := s.globalQ
	s.queueMu.RUnlock()
	require.NotNil(t, globalQ)

	res, err := globalQ.Submit(context.Background(), queue.Envelope{
		Priority:    queue.PriorityMedium,
		Cost:        100,
		CostAxis:    "q4k_matvec",
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.ID)

	// Initially: drainGlobal has nothing to place onto.
	s.drainGlobal(context.Background())
	require.Equal(t, 1, mustDepth(t, globalQ, context.Background()),
		"row stays on global with no candidate worker")

	// Add a worker; drainGlobal must place the row onto its queue.
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(4)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	s.drainGlobal(context.Background())

	s.queueMu.RLock()
	wq := s.devQueues["worker|w1"]
	s.queueMu.RUnlock()
	require.Equal(t, 1, mustDepth(t, wq, context.Background()),
		"drainGlobal must place the orphan row onto the newly-online worker")
}

// Worker exists but is unbenched → drainGlobal must NOT place
// (workerIsSchedulable filters it out). After a benchmark lands, the
// queue materialises and drainGlobal can place. Covers the bench-
// required gate's interaction with retry.
func TestDrainGlobal_WaitsForBenchmark(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, st := newTestScheduler(t)

	// Unbenched worker — no device queue is materialised.
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(4)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	s.queueMu.RLock()
	globalQ := s.globalQ
	_, hasQ := s.devQueues["worker|w1"]
	s.queueMu.RUnlock()
	require.False(t, hasQ, "unbenched worker must have no device queue")

	_, err := globalQ.Submit(context.Background(), queue.Envelope{
		Priority:    queue.PriorityMedium,
		Cost:        100,
		CostAxis:    "q4k_matvec",
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
	})
	require.NoError(t, err)
	s.drainGlobal(context.Background())
	require.Equal(t, 1, mustDepth(t, globalQ, context.Background()),
		"unbenched worker can't pick up rows")

	// Land a benchmark + replay OnWorkerConnected to materialise the
	// queue.
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	s.InvalidateBench("w1", "gpu:0")
	s.OnWorkerConnected(w)

	s.drainGlobal(context.Background())

	s.queueMu.RLock()
	wq := s.devQueues["worker|w1"]
	s.queueMu.RUnlock()
	require.NotNil(t, wq, "benched worker must have a device queue")
	require.Equal(t, 1, mustDepth(t, wq, context.Background()),
		"drainGlobal must place once benchmark lands")
}

// leaseAndDispatch's race-loser path: a row is already leased (e.g. a
// concurrent dispatcher tick stole the lease). LeaseByID returns nil
// and leaseAndDispatch returns cleanly — no inflight credit, no debit,
// no AssignJob.
func TestLeaseAndDispatch_RaceLoserSkipsCleanly(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(4)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	var assignCalls atomic.Int32
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if msg.GetAssignJob() != nil {
			assignCalls.Add(1)
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	s.queueMu.RLock()
	wq := s.devQueues["worker|w1"]
	s.queueMu.RUnlock()

	res, err := wq.Submit(context.Background(), queue.Envelope{
		Priority:    queue.PriorityMedium,
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Payload:     []byte("p"),
	})
	require.NoError(t, err)

	// Steal the lease before leaseAndDispatch tries.
	leased, err := wq.LeaseByID(context.Background(), queue.MessageID(res.ID), time.Minute)
	require.NoError(t, err)
	require.NotNil(t, leased)

	// Hand a stale Peek-shaped Message to leaseAndDispatch — it should
	// see leased==nil from LeaseByID and bail.
	stale := &queue.Message{ID: queue.MessageID(res.ID), Body: leased.Body}
	s.leaseAndDispatch(context.Background(), w, wq, "worker|w1", stale)

	require.Equal(t, int32(0), assignCalls.Load(),
		"race-loser must NOT call AssignJob")
	require.InDelta(t, 0.0, s.getInflightSeconds("worker|w1"), 0.001,
		"race-loser must NOT credit inflight")
}

// An unparseable envelope (corrupt Body) must be deleted from the
// worker queue and not panic / leak inflight / fire AssignJob.
func TestLeaseAndDispatch_UnparseableEnvelopeIsDropped(t *testing.T) {
	const runtimeName = "llama-cpp"
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(4)
	var assignCalls atomic.Int32
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if msg.GetAssignJob() != nil {
			assignCalls.Add(1)
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	// Plant a row with a body the envelope unmarshaler can't parse.
	s.queueMu.RLock()
	wq := s.devQueues["worker|w1"]
	s.queueMu.RUnlock()
	// The underlying Submit takes an Envelope — to inject garbage we
	// use the Pool's raw queue Submit with a malformed Envelope marshal.
	// Easiest path: use the queue's low-level interface via the typed
	// Message struct — leaseAndDispatch expects msg.Body to deserialize.
	// We submit a valid envelope first, then leaseAndDispatch with a
	// hand-crafted bogus body.
	res, err := wq.Submit(context.Background(), queue.Envelope{
		Priority:    queue.PriorityMedium,
		RuntimeName: runtimeName,
		Payload:     []byte("p"),
	})
	require.NoError(t, err)
	bogus := &queue.Message{ID: queue.MessageID(res.ID), Body: []byte{0xff, 0xff, 0xff}}
	s.leaseAndDispatch(context.Background(), w, wq, "worker|w1", bogus)

	require.Equal(t, int32(0), assignCalls.Load(),
		"unparseable envelope must NOT reach AssignJob")
	require.Equal(t, 0, mustDepth(t, wq, context.Background()),
		"unparseable envelope row must be deleted from the queue")
	require.InDelta(t, 0.0, s.getInflightSeconds("worker|w1"), 0.001,
		"unparseable envelope must NOT credit inflight")
}

// Device-set gate: EvictModel failure must release the lease so the
// next tick retries — the gate must not load a model on top of a
// resident it couldn't evict.
func TestDispatchEnvelope_OverlapEvictFailureReleasesLease(t *testing.T) {
	const runtimeName = "llama-cpp"
	const residentModelID = "m-resident"
	const newModelID = "m-new"
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	var loadCalls, unloadCalls atomic.Int32
	unloadJobID := make(chan string, 1)
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(0)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: residentModelID, PoolSize: 1, Active: 0,
		IdleSince: time.Now(),
		DeviceIDs: []string{"gpu:0"},
	}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if um := msg.GetUnloadModel(); um != nil {
			unloadCalls.Add(1)
			select {
			case unloadJobID <- um.GetJobId():
			default:
			}
		}
		if msg.GetLoadModel() != nil {
			loadCalls.Add(1)
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
	})
	require.NoError(t, err)

	// Run dispatch on a goroutine; ack UnloadModel with an error so
	// the gate's evict step fails.
	done := make(chan struct{})
	go func() {
		s.dispatchPass(context.Background())
		close(done)
	}()
	select {
	case jobID := <-unloadJobID:
		w.DeliverUnloadResult(jobID, "synthetic unload failure")
	case <-time.After(2 * time.Second):
		t.Fatal("expected UnloadModel call within 2s")
	}
	<-done
	waitDispatchIdle(t, s)

	require.Equal(t, int32(1), unloadCalls.Load(),
		"evict must be attempted")
	require.Equal(t, int32(0), loadCalls.Load(),
		"failed evict must NOT proceed to LoadModel")

	// Row must come back on the worker queue for retry — after the bounce
	// delay, not instantly.
	s.queueMu.RLock()
	wq := s.devQueues["worker|w1"]
	s.queueMu.RUnlock()
	require.Eventually(t, func() bool {
		return mustDepth(t, wq, context.Background()) == 1
	}, 5*time.Second, 10*time.Millisecond,
		"row must remain on the worker queue for retry")
}

// substringCounter counts log lines containing want — how a test observes
// events the scheduler only reports through its logger.
type substringCounter struct {
	want  string
	count *atomic.Int64
}

func (c *substringCounter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte(c.want)) {
		c.count.Add(1)
	}
	return len(p), nil
}

// The device-set gate bounces a job while a resident model is still serving
// traffic. Regression: the bounce released the lease into the past and fired
// two wake-ups (the queue signal inside ReleaseLease, plus kick), so a
// resident that stayed active spun drain -> lease -> bounce as fast as SQLite
// could serve it, for as long as it kept serving. The bounce is still
// uncapped and still charges nothing against Envelope.Attempts — a busy
// blocker must not fail the job — it just has to be paced.
func TestDispatchEnvelope_ActiveBlockerBounceIsPaced(t *testing.T) {
	const runtimeName = "llama-cpp"
	const residentModelID = "m-resident"
	const newModelID = "m-new"

	s, st := newTestScheduler(t)
	var bounces, loadCalls atomic.Int64
	s.logger = zerolog.New(&substringCounter{want: "resident blocks load", count: &bounces}).
		Level(zerolog.DebugLevel)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	// Resident model holds gpu:0 and keeps serving (Active=1) for the whole
	// test, while the worker still reports a free slot — nothing but the
	// gate holds the new job back.
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeCapacity(1)
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: residentModelID, PoolSize: 2, Active: 1,
		DeviceIDs: []string{"gpu:0"},
	}})
	w.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if msg.GetLoadModel() != nil {
			loadCalls.Add(1)
		}
		return nil
	})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	_, err := s.Submit(ctx, SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     newModelID,
		Payload:     []byte("p"),
		Cost:        100, CostAxis: "q4k_matvec",
		Files:     []*workerpb.ModelFile{{Url: "http://x/y.gguf", Filename: "y.gguf", SizeBytes: 1024}},
		LoadHints: []byte("h"),
	})
	require.NoError(t, err)

	const window = time.Second
	time.Sleep(window)
	cancel()
	waitDispatchIdle(t, s)

	require.Zero(t, loadCalls.Load(), "the gate must not load past an active blocker")
	n := bounces.Load()
	require.Positive(t, n, "the blocked row must keep retrying")
	// One bounce per pendingRetryInterval, plus the odd extra pass from the
	// loop's slow ticker. Pre-fix this reached into the thousands.
	require.LessOrEqual(t, n, int64(4*window/pendingRetryInterval),
		"a blocked row must not re-enter dispatch faster than the bounce delay")
}

// One worker's cold model load must not stall dispatch to the rest of
// the fleet. Regression: dispatchEnvelope used to run on the single
// dispatcher goroutine and block for the entire LoadModel round-trip
// (minutes for a multi-GB model) — while one worker loaded, no other
// worker queue drained. Driven through the REAL dispatch loop (Start),
// not dispatchPass, because the bug lives in the loop's threading.
func TestDispatch_ColdLoadDoesNotStallOtherWorkers(t *testing.T) {
	s, st := newTestScheduler(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// w-cold (runtime rt-cold): nothing resident, so its dispatch parks
	// inside LoadModel until the test delivers the ack.
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w-cold", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	loadID := make(chan string, 1)
	coldAssigns := make(chan string, 1)
	wCold := worker.NewFakeStreamWorker("w-cold", "rt-cold",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	wCold.SetFakeCapacity(0)
	wCold.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if lm := msg.GetLoadModel(); lm != nil {
			select {
			case loadID <- lm.GetJobId():
			default:
			}
		}
		if aj := msg.GetAssignJob(); aj != nil {
			select {
			case coldAssigns <- aj.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(wCold))
	s.OnWorkerConnected(wCold)

	// w-warm (runtime rt-warm): model resident, assigns immediately.
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w-warm", DeviceID: "gpu:0", DeviceName: "gpu:0",
		MemoryGBs: 25, LoadGBs: 25, Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	warmAssigns := make(chan string, 1)
	wWarm := worker.NewFakeStreamWorker("w-warm", "rt-warm",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	wWarm.SetFakeCapacity(4)
	wWarm.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: "m-warm", PoolSize: 4}})
	wWarm.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if aj := msg.GetAssignJob(); aj != nil {
			select {
			case warmAssigns <- aj.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, s.workers.Register(wWarm))
	s.OnWorkerConnected(wWarm)

	s.Start(ctx)

	// Submit the cold job first and wait for its LoadModel to be issued —
	// from here the cold worker's drain goroutine is parked awaiting the ack.
	_, err := s.Submit(ctx, SubmitRequest{
		RuntimeName: "rt-cold", ModelID: "m-cold", Payload: []byte("p"),
		Cost: 100, CostAxis: "q4k_matvec",
		Files: []*workerpb.ModelFile{{Url: "http://x/y.gguf", Filename: "y.gguf", SizeBytes: 1024}},
	})
	require.NoError(t, err)
	var coldLoadJobID string
	select {
	case coldLoadJobID = <-loadID:
	case <-time.After(2 * time.Second):
		t.Fatal("cold worker never received LoadModel")
	}

	// The warm worker's job must dispatch while the cold load is still
	// in flight — the single-goroutine dispatcher would sit inside
	// LoadModel here and never assign it.
	_, err = s.Submit(ctx, SubmitRequest{
		RuntimeName: "rt-warm", ModelID: "m-warm", Payload: []byte("p"),
		Cost: 100, CostAxis: "q4k_matvec",
	})
	require.NoError(t, err)
	select {
	case <-warmAssigns:
	case <-time.After(2 * time.Second):
		t.Fatal("warm worker starved while a peer's cold load was in flight")
	}

	// Unblock the cold load; its own job must still complete dispatch.
	wCold.DeliverLoadResult(coldLoadJobID, worker.LoadResult{PoolSize: 1}, "")
	select {
	case <-coldAssigns:
	case <-time.After(2 * time.Second):
		t.Fatal("cold worker's job never reached AssignJob after the load ack")
	}
}

// A job that was queued when MASS went down must run when MASS comes
// back. The queue rows are durable but the replay buffers are not, so the
// recovered dispatch has to rebuild the buffer from the durable result
// instead of failing the job for not having one.
func TestDispatch_QueuedJobSurvivesRestart(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	ctx := context.Background()

	first, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))
	devices := []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}

	// Lifetime 1: submit + place. Nothing drives the dispatch loop here,
	// so the row is still sitting pending on the worker queue when the
	// process "dies".
	w1 := worker.NewFakeStreamWorker("w1", runtimeName, devices, time.Now())
	w1.SetFakeCapacity(4)
	w1.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	require.NoError(t, first.workers.Register(w1))
	first.OnWorkerConnected(w1)

	rid, err := first.Submit(ctx, SubmitRequest{
		RuntimeName: runtimeName, ModelID: modelID, Payload: []byte("p"),
		Cost: 100, CostAxis: "q4k_matvec",
	})
	require.NoError(t, err)
	first.queueMu.RLock()
	wq := first.devQueues["worker|w1"]
	first.queueMu.RUnlock()
	require.Equal(t, 1, mustDepth(t, wq, ctx), "job must be queued, not dispatched")

	// Lifetime 2: a fresh scheduler over the same database. No resubmit —
	// everything it knows comes from the durable rows.
	second := New(&config.Config{}, zerolog.Nop(), worker.NewFleet())
	second.InitQueue(queue.NewPool(st.DB(), st.Dialect()), queue.NewResultStore(st.DB(), st.Dialect()), st)
	assigns := make(chan string, 1)
	w2 := worker.NewFakeStreamWorker("w1", runtimeName, devices, time.Now())
	w2.SetFakeCapacity(4)
	w2.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	w2.SetFakeSender(func(msg *workerpb.HubMessage) error {
		if aj := msg.GetAssignJob(); aj != nil {
			select {
			case assigns <- aj.GetJobId():
			default:
			}
		}
		return nil
	})
	require.NoError(t, second.workers.Register(w2))
	second.OnWorkerConnected(w2)

	second.reapAbandonedAtStartup(ctx)
	second.recoverPersistedQueues()
	second.dispatchPass(ctx)

	var jobID string
	select {
	case jobID = <-assigns:
	case <-time.After(2 * time.Second):
		t.Fatal("recovered job never reached AssignJob")
	}
	w2.DeliverJobChunk(jobID, &worker.JobChunk{
		Type: worker.JobChunkTypeCompleted, Final: []byte("recovered-ok"),
	})

	deadline := time.Now().Add(2 * time.Second)
	for {
		res, err := second.results.Get(rid)
		require.NoError(t, err)
		require.NotNil(t, res)
		if res.Status == queue.ResultStatusDone {
			require.Equal(t, []byte("recovered-ok"), res.Body)
			require.Empty(t, res.Error)
			break
		}
		require.NotEqual(t, queue.ResultStatusError, res.Status,
			"recovered job must not be failed: %s", res.Error)
		if time.Now().After(deadline) {
			t.Fatalf("recovered job never reached Done (status %s)", res.Status)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// The other two halves of the missing-buffer branch: a job whose result
// is already terminal keeps that outcome (a restart must not stomp
// "cancelled by operator" with a dispatch failure), and a job with no
// result row left is failed as before. Neither reaches the worker, and
// both drop their queue rows.
func TestDispatch_MissingReplayBuffer_TerminalAndUnknownResults(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"

	tests := []struct {
		name string
		// existingError, when set, is the terminal error already recorded
		// for the request. Empty means no result row exists at all.
		existingError string
	}{
		{name: "terminal result is preserved", existingError: "cancelled by operator"},
		{name: "no result row at all"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
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

			const requestID = "rid-orphan"
			if tt.existingError != "" {
				require.NoError(t, s.results.Create(requestID))
				require.NoError(t, s.results.Fail(requestID, tt.existingError))
			}
			s.queueMu.RLock()
			wq := s.devQueues["worker|w1"]
			s.queueMu.RUnlock()
			placeOnWorkerQueueForTest(t, s, wq, queue.Envelope{
				Priority: queue.PriorityMedium, Cost: 100, CostAxis: "q4k_matvec",
				RuntimeName: runtimeName, ModelID: modelID,
				RequestID: requestID, Payload: []byte("p"),
			})

			s.dispatchPass(ctx)
			waitDispatchIdle(t, s)

			require.Empty(t, assigns, "the job must never reach the worker")
			res, err := s.results.Get(requestID)
			require.NoError(t, err)
			if tt.existingError == "" {
				require.Nil(t, res, "nothing to record a failure against")
			} else {
				require.NotNil(t, res)
				require.Equal(t, queue.ResultStatusError, res.Status)
				require.Equal(t, tt.existingError, res.Error,
					"the recorded outcome must survive the dispatch attempt")
			}

			rows, err := wq.PeekAll(ctx, 10)
			require.NoError(t, err)
			require.Empty(t, rows, "worker row must be dropped")
			gRows, err := s.globalQ.PeekAll(ctx, 10)
			require.NoError(t, err)
			require.Empty(t, gRows, "global anchor must be dropped")
		})
	}
}

func mustDepth(t *testing.T, q queue.QueueInterface, ctx context.Context) int {
	t.Helper()
	d, err := q.Depth(ctx)
	require.NoError(t, err)
	return d
}

// errFakeSenderClosed is a stable sentinel for tests that inject a
// sender failure. Real workers surface offline via ErrWorkerOffline;
// this test cares only that *some* error propagates, not which.
var errFakeSenderClosed = &fakeErr{"fake sender: closed"}

type fakeErr struct{ msg string }

func (e *fakeErr) Error() string { return e.msg }

// pollUntilEqual waits up to 2s for f() to return want, failing the
// test on timeout with msg.
func pollUntilEqual(t *testing.T, want int, f func() int, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s: got %d, want %d after 2s", msg, f(), want)
}
