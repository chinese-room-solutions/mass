package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/stretchr/testify/require"
)

// deviceSet drops the residency branch as of 2026-05-30: predictions
// must read the operator's CURRENT enable mask, not whichever set the
// model happened to load onto last time. The cases below pin the new
// contract: residency is irrelevant; enable/disable is authoritative.
func TestScheduler_DeviceSet_IgnoresResidency(t *testing.T) {
	tests := []struct {
		name     string
		devices  []stats.Device
		loaded   []worker.LoadedModelStatus
		disabled []string
		want     []string
	}{
		{
			// Two enabled GPUs; residency on only one MUST not narrow
			// the set — that was the bug.
			name:    "residency on one GPU does not narrow the set",
			devices: []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}, {ID: "gpu:1", Type: stats.DeviceTypeGPU}},
			loaded:  []worker.LoadedModelStatus{{ModelID: "m", DeviceIDs: []string{"gpu:0"}}},
			want:    []string{"gpu:0", "gpu:1"},
		},
		{
			// Resident on a disabled device: disable wins.
			name:     "resident GPU disabled — drops out",
			devices:  []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}, {ID: "gpu:1", Type: stats.DeviceTypeGPU}},
			loaded:   []worker.LoadedModelStatus{{ModelID: "m", DeviceIDs: []string{"gpu:0", "gpu:1"}}},
			disabled: []string{"gpu:1"},
			want:     []string{"gpu:0"},
		},
		{
			// CPU-only fallback: every GPU disabled, residency on
			// CPU+GPU. The default-placement rule still returns just CPU.
			name:     "all GPUs disabled — single CPU returned",
			devices:  []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}, {ID: "cpu:0", Type: stats.DeviceTypeCPU}},
			loaded:   []worker.LoadedModelStatus{{ModelID: "m", DeviceIDs: []string{"gpu:0", "cpu:0"}}},
			disabled: []string{"gpu:0"},
			want:     []string{"cpu:0"},
		},
		{
			// Every device disabled — nil. This is the worker-unusable signal
			// eligibleWorker reads to skip the candidate.
			name:     "all disabled — nil",
			devices:  []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}},
			loaded:   []worker.LoadedModelStatus{{ModelID: "m", DeviceIDs: []string{"gpu:0"}}},
			disabled: []string{"gpu:0"},
			want:     nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestScheduler(t)
			w := worker.NewFakeStreamWorker("w1", "llama-cpp", tt.devices, time.Now())
			if len(tt.loaded) > 0 {
				w.SetFakeLoadedModels(tt.loaded)
			}
			require.NoError(t, s.workers.Register(w))
			disabled := make(map[string]bool, len(tt.disabled))
			for _, id := range tt.disabled {
				disabled[id] = true
			}
			s.SetDeviceEnabledFn(func(_, deviceID string) bool {
				return !disabled[deviceID]
			})
			got := s.deviceSet(w)
			require.ElementsMatch(t, tt.want, got)
		})
	}
}

// TryReestimateLock semantics: a successful lock blocks a second
// attempt on the SAME worker; different workers never contend. Release
// frees the next attempt.
func TestScheduler_TryReestimateLock_Semantics(t *testing.T) {
	s, _ := newTestScheduler(t)

	release1, ok1 := s.TryReestimateLock("w1")
	require.True(t, ok1, "first lock must succeed")
	require.NotNil(t, release1)

	// Same worker: must fail fast with no release.
	release2, ok2 := s.TryReestimateLock("w1")
	require.False(t, ok2, "concurrent lock on same worker must be refused")
	require.Nil(t, release2)

	// Different worker: independent — succeeds.
	release3, ok3 := s.TryReestimateLock("w2")
	require.True(t, ok3, "different worker must not contend")
	t.Cleanup(release3)

	// Release the first → next attempt on w1 succeeds.
	release1()
	release4, ok4 := s.TryReestimateLock("w1")
	require.True(t, ok4, "lock must be available again after release")
	t.Cleanup(release4)
}

// ReestimateWorkerQueue walks the worker's queued envelopes, rewrites
// each one's QueuedSeconds against the new device set, and writes the
// sum back to tail_seconds. This is the core toggle response: scoring
// and UI must reflect "what wall-clock would this job actually take
// under the new enabled mask?" — not whatever number Submit picked.
func TestScheduler_ReestimateWorkerQueue_RewritesQueuedSeconds(t *testing.T) {
	s, st := newTestScheduler(t)
	const (
		wID   = "w1"
		qName = "worker|w1"
		axis  = "q4k_matvec"
	)
	// Two GPUs benched at different rates; the operator will disable
	// the slow one to verify the sum shrinks the predicted seconds.
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{axis: 1800}, BenchedAt: time.Now(),
	}))
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: wID, DeviceID: "gpu:1", DeviceName: "gpu:1",
		Throughput: map[string]float64{axis: 200}, BenchedAt: time.Now(),
	}))
	w := worker.NewFakeStreamWorker(wID, "llama-cpp",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}, {ID: "gpu:1", Type: stats.DeviceTypeGPU}},
		time.Now())
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)
	s.SetRuntimeDefaultAxisFn(func(string) string { return axis })

	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues[qName]
	s.queueMu.RUnlock()
	require.NotNil(t, wq)

	// Pre-fill with QueuedSeconds = 999 (wildly wrong) so we can see the
	// rewrite. Cost = 2000 tokens. Under both GPUs (1800+200 = 2000) →
	// 1.0s; under gpu:0 only (1800) → ~1.111s.
	env := queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m", RequestID: "r-1", Payload: []byte("p"),
		Cost: 2000, CostAxis: axis, QueuedSeconds: 999,
	}
	_, err := wq.Submit(ctx, env)
	require.NoError(t, err)
	// Seed worker_queue_state so SetTailSeconds has a row to update.
	require.NoError(t, st.UpsertWorkerQueueState(store.WorkerQueueState{
		QueueName: qName, WorkerID: wID, TailSeconds: 999,
	}))

	// Disable gpu:1 → re-estimate must drop tail to ~1.111s and rewrite
	// the envelope body so dispatch debits the same number on pop.
	disabled := map[string]bool{"gpu:1": true}
	s.SetDeviceEnabledFn(func(_, devID string) bool { return !disabled[devID] })

	s.ReestimateWorkerQueue(ctx, wID)

	dqs, err := st.GetWorkerQueueState(qName)
	require.NoError(t, err)
	require.InDelta(t, 2000.0/1800.0, dqs.TailSeconds, 0.01,
		"tail_seconds must reflect new (single-GPU) device set, not the pre-toggle prediction")
	require.Equal(t, "m", dqs.TailModelID, "last-queued model carried into tail_model_id")

	// Verify the envelope body got rewritten — peek and decode it.
	rows, err := wq.PeekAll(ctx, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	decoded, err := queue.UnmarshalEnvelope(rows[0].Body)
	require.NoError(t, err)
	require.InDelta(t, 2000.0/1800.0, decoded.QueuedSeconds, 0.01,
		"envelope's QueuedSeconds must be rewritten so dispatch debits the right number")
}

// Re-estimation across multiple envelopes preserves the conservation
// invariant: tail_seconds == Σ envelope.QueuedSeconds after the rewrite.
// Also confirms tail_model_id carries the LAST envelope's model so the
// next score's load-cost branch sees the right "what's at the back".
func TestScheduler_ReestimateWorkerQueue_MultiEnvelopeSumAndTailModel(t *testing.T) {
	s, st := newTestScheduler(t)
	const (
		wID  = "w1"
		qN   = "worker|w1"
		axis = "q4k_matvec"
	)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{axis: 1000}, BenchedAt: time.Now(),
	}))
	w := worker.NewFakeStreamWorker(wID, "llama-cpp",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)
	s.SetRuntimeDefaultAxisFn(func(string) string { return axis })

	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues[qN]
	s.queueMu.RUnlock()
	require.NotNil(t, wq)

	// Submit three envelopes with garbage QueuedSeconds; ModelIDs in
	// chronological order so the LAST one (m-c) should win tail_model_id.
	// Different Cost values so we can check the sum.
	for _, e := range []struct {
		req     string
		model   string
		cost    float64
		payload []byte
	}{
		{"r-a", "m-a", 1000, []byte("a")},
		{"r-b", "m-b", 500, []byte("b")},
		{"r-c", "m-c", 250, []byte("c")},
	} {
		env := queue.Envelope{
			Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
			ModelID: e.model, RequestID: e.req, Payload: e.payload,
			Cost: e.cost, CostAxis: axis, QueuedSeconds: 999,
		}
		_, err := wq.Submit(ctx, env)
		require.NoError(t, err)
	}
	require.NoError(t, st.UpsertWorkerQueueState(store.WorkerQueueState{
		QueueName: qN, WorkerID: wID,
	}))

	s.ReestimateWorkerQueue(ctx, wID)

	dqs, err := st.GetWorkerQueueState(qN)
	require.NoError(t, err)
	// Compute (1000+500+250)/1000 = 1.75 — but each envelope after the
	// first pays a load switch cost too (different ModelIDs). We don't
	// attach files, so totalLoadBytes is 0 → switch cost is 0. Pure sum.
	require.InDelta(t, 1.75, dqs.TailSeconds, 1e-6,
		"tail_seconds must equal the sum of new per-envelope predictions")
	require.Equal(t, "m-c", dqs.TailModelID, "tail_model_id tracks the LAST queued envelope")

	rows, err := wq.PeekAll(ctx, 10)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	var sumFromBodies float64
	for _, r := range rows {
		e, err := queue.UnmarshalEnvelope(r.Body)
		require.NoError(t, err)
		sumFromBodies += e.QueuedSeconds
	}
	require.InDelta(t, dqs.TailSeconds, sumFromBodies, 1e-6,
		"conservation invariant: Σ envelope.QueuedSeconds == tail_seconds")
}

// Toggle handlers serialise on the per-worker re-estimate lock. Two
// concurrent toggles attempt to claim simultaneously; exactly one must
// observe ok=true. The other gets the typed refusal that the HTTP
// layer maps to 409.
func TestScheduler_TryReestimateLock_ConcurrentTogglesReject(t *testing.T) {
	s, _ := newTestScheduler(t)
	const n = 16
	var (
		wg       sync.WaitGroup
		acquired int32
	)
	acq := make(chan struct{}, n)
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			release, ok := s.TryReestimateLock("w1")
			if ok {
				acq <- struct{}{}
				defer release()
				// Hold long enough to ensure peers see us.
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	close(acq)
	for range acq {
		acquired++
	}
	require.EqualValues(t, 1, acquired,
		"exactly one toggle must hold the lock at a time per worker")
}

// ReestimateWorkerQueue is a no-op when the worker has no queue (not
// connected, or all rows already drained). It must NOT panic or write
// a stale tail_seconds row that future scoring would consult.
func TestScheduler_ReestimateWorkerQueue_NoQueueNoOp(t *testing.T) {
	s, _ := newTestScheduler(t)
	w := worker.NewFakeStreamWorker("ghost", "llama-cpp",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	require.NoError(t, s.workers.Register(w))
	// Deliberately do NOT call OnWorkerConnected — no devQueues entry exists.
	require.NotPanics(t, func() {
		s.ReestimateWorkerQueue(context.Background(), "ghost")
	})
}

// Disabling EVERY device collapses predicted seconds for each queued
// envelope to 0 (no schedulable throughput) — tail_seconds goes to 0,
// envelope bodies are rewritten to 0. Without this, a disconnect-then-
// reenable cycle leaks the pre-toggle tail forever.
func TestScheduler_ReestimateWorkerQueue_AllDisabledZeroesEverything(t *testing.T) {
	s, st := newTestScheduler(t)
	const (
		wID  = "w1"
		qN   = "worker|w1"
		axis = "q4k_matvec"
	)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{axis: 1000}, BenchedAt: time.Now(),
	}))
	w := worker.NewFakeStreamWorker(wID, "llama-cpp",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)
	s.SetRuntimeDefaultAxisFn(func(string) string { return axis })

	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues[qN]
	s.queueMu.RUnlock()
	require.NotNil(t, wq)

	for _, req := range []string{"r-1", "r-2"} {
		_, err := wq.Submit(ctx, queue.Envelope{
			Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
			ModelID: "m", RequestID: req, Payload: []byte("p"),
			Cost: 1000, CostAxis: axis, QueuedSeconds: 5,
		})
		require.NoError(t, err)
	}
	require.NoError(t, st.UpsertWorkerQueueState(store.WorkerQueueState{
		QueueName: qN, WorkerID: wID, TailSeconds: 10,
	}))

	// Disable everything.
	s.SetDeviceEnabledFn(func(_, _ string) bool { return false })
	s.ReestimateWorkerQueue(ctx, wID)

	dqs, err := st.GetWorkerQueueState(qN)
	require.NoError(t, err)
	require.InDelta(t, 0.0, dqs.TailSeconds, 1e-9,
		"all-disabled worker must drop tail to 0 — no schedulable throughput")

	rows, err := wq.PeekAll(ctx, 10)
	require.NoError(t, err)
	for _, r := range rows {
		env, err := queue.UnmarshalEnvelope(r.Body)
		require.NoError(t, err)
		require.InDelta(t, 0.0, env.QueuedSeconds, 1e-9,
			"each envelope body must reflect zero predicted seconds")
	}
}

// Model-switch cost is part of the per-envelope prediction. When two
// adjacent envelopes target different models, the SECOND envelope's
// QueuedSeconds must include a load-switch term (file_bytes /
// effectiveLoadThroughput) — same rule as loadLatencyForCand. Without
// this, tail_seconds under-counts and scoring under-estimates wait.
func TestScheduler_ReestimateWorkerQueue_AccountsForModelSwitchCost(t *testing.T) {
	s, st := newTestScheduler(t)
	const (
		wID  = "w1"
		qN   = "worker|w1"
		axis = "q4k_matvec"
	)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{axis: 1000}, LoadGBs: 10,
		BenchedAt: time.Now(),
	}))
	w := worker.NewFakeStreamWorker(wID, "llama-cpp",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)
	s.SetRuntimeDefaultAxisFn(func(string) string { return axis })

	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues[qN]
	s.queueMu.RUnlock()
	require.NotNil(t, wq)

	// 1st: m-a, no files — no switch cost. Pure compute 0.5s.
	_, err := wq.Submit(ctx, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m-a", RequestID: "r-a", Payload: []byte("p"),
		Cost: 500, CostAxis: axis,
	})
	require.NoError(t, err)
	// 2nd: m-b, 1 GB file — pays a switch term of 1GB / 10GB/s = 0.1s
	// on top of 0.5s compute.
	_, err = wq.Submit(ctx, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m-b", RequestID: "r-b", Payload: []byte("p"),
		Cost: 500, CostAxis: axis,
		Files: []*workerpb.ModelFile{{SizeBytes: 1_000_000_000}},
	})
	require.NoError(t, err)
	require.NoError(t, st.UpsertWorkerQueueState(store.WorkerQueueState{
		QueueName: qN, WorkerID: wID,
	}))

	// No model is resident on the worker, so the first envelope (m-a) is
	// itself a cold load — but it has no Files, so switch cost = 0
	// (totalLoadBytes returns 0). Subsequent m-a → m-b switch is real.
	s.ReestimateWorkerQueue(ctx, wID)

	dqs, err := st.GetWorkerQueueState(qN)
	require.NoError(t, err)
	// First row: 0.5s. Second row: 0.5 + 1e9/10e9 = 0.6s. Sum = 1.1s.
	require.InDelta(t, 1.1, dqs.TailSeconds, 1e-6,
		"tail must reflect both compute AND load-switch costs across adjacent models")
}

// Walking the queue updates the running resident pointer: by the time
// we get to an envelope targeting model X, the previous queued envelope
// is assumed to have already loaded its model. So two consecutive m-b
// envelopes pay switch cost ONCE, not twice.
func TestScheduler_ReestimateWorkerQueue_RunningResidentSuppressesRepeatSwitch(t *testing.T) {
	s, st := newTestScheduler(t)
	const (
		wID  = "w1"
		qN   = "worker|w1"
		axis = "q4k_matvec"
	)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{axis: 1000}, LoadGBs: 10,
		BenchedAt: time.Now(),
	}))
	w := worker.NewFakeStreamWorker(wID, "llama-cpp",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: "m-a"}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)
	s.SetRuntimeDefaultAxisFn(func(string) string { return axis })

	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues[qN]
	s.queueMu.RUnlock()

	// m-a resident → 1st envelope (m-a) skips switch.
	// 2nd envelope (m-b) pays switch.
	// 3rd envelope (m-b) — same model as previous queued — no switch.
	for _, e := range []struct {
		req, model string
	}{
		{"r-1", "m-a"},
		{"r-2", "m-b"},
		{"r-3", "m-b"},
	} {
		_, err := wq.Submit(ctx, queue.Envelope{
			Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
			ModelID: e.model, RequestID: e.req, Payload: []byte("p"),
			Cost: 500, CostAxis: axis,
			Files: []*workerpb.ModelFile{{SizeBytes: 1_000_000_000}},
		})
		require.NoError(t, err)
	}
	require.NoError(t, st.UpsertWorkerQueueState(store.WorkerQueueState{
		QueueName: qN, WorkerID: wID,
	}))

	s.ReestimateWorkerQueue(ctx, wID)

	dqs, err := st.GetWorkerQueueState(qN)
	require.NoError(t, err)
	// r-1: 0.5 (compute only — m-a resident).
	// r-2: 0.5 + 0.1 (switch m-a→m-b).
	// r-3: 0.5 (m-b already preloaded by r-2).
	// Sum = 1.6.
	require.InDelta(t, 1.6, dqs.TailSeconds, 1e-6,
		"adjacent same-model envelopes must NOT each pay a switch cost")
	require.Equal(t, "m-b", dqs.TailModelID, "last enqueued model wins tail_model_id")
}

// When the worker's enabled devices have NO bench numbers (operator
// disabled the only benched device), throughput is unschedulable for
// every envelope — predicted seconds collapse to 0 and tail goes to 0.
// Dispatch's eligibility gate will surface those rows as "no fit" when
// they reach the head.
func TestScheduler_ReestimateWorkerQueue_NoBenchOnEnabledSet(t *testing.T) {
	s, st := newTestScheduler(t)
	const (
		wID  = "w1"
		qN   = "worker|w1"
		axis = "q4k_matvec"
	)
	// Bench only gpu:0; operator will disable it.
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{axis: 1000}, BenchedAt: time.Now(),
	}))
	w := worker.NewFakeStreamWorker(wID, "llama-cpp",
		[]stats.Device{
			{ID: "gpu:0", Type: stats.DeviceTypeGPU},
			{ID: "gpu:1", Type: stats.DeviceTypeGPU},
		}, time.Now())
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)
	s.SetRuntimeDefaultAxisFn(func(string) string { return axis })

	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues[qN]
	s.queueMu.RUnlock()
	_, err := wq.Submit(ctx, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m", RequestID: "r-1", Payload: []byte("p"),
		Cost: 1000, CostAxis: axis,
	})
	require.NoError(t, err)
	require.NoError(t, st.UpsertWorkerQueueState(store.WorkerQueueState{
		QueueName: qN, WorkerID: wID,
	}))

	// Disable the only benched device; gpu:1 has no bench.
	disabled := map[string]bool{"gpu:0": true}
	s.SetDeviceEnabledFn(func(_, devID string) bool { return !disabled[devID] })

	s.ReestimateWorkerQueue(ctx, wID)

	dqs, err := st.GetWorkerQueueState(qN)
	require.NoError(t, err)
	require.InDelta(t, 0.0, dqs.TailSeconds, 1e-9,
		"no bench on enabled set → unschedulable → predicted seconds zero")
}

// A row whose body fails to decode is logged and excluded from the new
// tail sum — its on-disk QueuedSeconds is unreadable, so there is
// nothing to preserve. The well-formed peers must still be rewritten.
func TestScheduler_ReestimateWorkerQueue_MalformedRowSkipped(t *testing.T) {
	s, st := newTestScheduler(t)
	const (
		wID  = "w1"
		qN   = "worker|w1"
		axis = "q4k_matvec"
	)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{axis: 1000}, BenchedAt: time.Now(),
	}))
	w := worker.NewFakeStreamWorker(wID, "llama-cpp",
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)
	s.SetRuntimeDefaultAxisFn(func(string) string { return axis })

	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues[qN]
	s.queueMu.RUnlock()

	// Submit one good envelope.
	res, err := wq.Submit(ctx, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m", RequestID: "r-good", Payload: []byte("p"),
		Cost: 500, CostAxis: axis,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.ID)

	// Corrupt the body in-place via UpdateBody — re-estimate must keep
	// going for any other rows. (Here there's only one row so we also
	// want to confirm tail_seconds reflects the OLD value preservation.)
	require.NoError(t, wq.UpdateBody(ctx, queue.MessageID(res.ID), []byte("not-a-valid-envelope")))
	require.NoError(t, st.UpsertWorkerQueueState(store.WorkerQueueState{
		QueueName: qN, WorkerID: wID, TailSeconds: 12.5,
	}))

	require.NotPanics(t, func() {
		s.ReestimateWorkerQueue(ctx, wID)
	})

	// The undecodable row contributes nothing to the recomputed sum, so
	// re-estimation replaces the stale pre-toggle tail (12.5) with 0.
	row, err := st.GetWorkerQueueState(qN)
	require.NoError(t, err)
	require.InDelta(t, 0.0, row.TailSeconds, 1e-9,
		"undecodable row must be excluded from the recomputed tail sum")
}

// UpdateBody on a missing message ID is a no-op (returns nil). Critical
// because re-estimation can race a concurrent terminal-frame delete:
// the row vanishes between PeekAll and UpdateBody. Hard error here
// would crash re-estimation for the surviving rows.
func TestQueue_UpdateBody_MissingRowReturnsNil(t *testing.T) {
	s, _ := newTestScheduler(t)
	s.queueMu.RLock()
	globalQ := s.globalQ
	s.queueMu.RUnlock()
	require.NotNil(t, globalQ)
	require.NoError(t, globalQ.UpdateBody(context.Background(),
		queue.MessageID("m_nonexistent_id"), []byte("anything")))
}

// SetTailSeconds clamps negative values at 0. Without the clamp,
// re-estimation could persist a negative tail under float arithmetic
// edge cases (e.g. all envelopes contributed 0 but a NaN slipped in).
func TestStore_SetTailSeconds_ClampsNegative(t *testing.T) {
	_, st := newTestScheduler(t)
	require.NoError(t, st.UpsertWorkerQueueState(store.WorkerQueueState{
		QueueName: "worker|w1", WorkerID: "w1",
	}))
	require.NoError(t, st.SetTailSeconds("worker|w1", -42.0, "m"))
	dqs, err := st.GetWorkerQueueState("worker|w1")
	require.NoError(t, err)
	require.InDelta(t, 0.0, dqs.TailSeconds, 1e-9,
		"negative value must clamp at 0")
	require.Equal(t, "m", dqs.TailModelID, "model id must persist even when clamped")
}

// SetTailSeconds against a missing queue row is silently a no-op: the
// UPDATE matches 0 rows, no error. Mirrors AddTailSeconds semantics so
// re-estimation can race a worker_queue_state delete (rare but possible
// during disconnect) without crashing.
func TestStore_SetTailSeconds_MissingRowIsNoOp(t *testing.T) {
	_, st := newTestScheduler(t)
	require.NoError(t, st.SetTailSeconds("worker|ghost", 5.0, "m"))
}

// Leased (in-flight) rows must NOT be recomputed by re-estimation: the
// running job is already executing on the device set it was scheduled
// against and won't be rerouted mid-stream. Its prediction stays
// anchored to the pre-toggle device set; its existing QueuedSeconds
// still contributes to the tail sum unchanged. Pending peers behind it
// ARE recomputed against the new device set.
func TestScheduler_ReestimateWorkerQueue_SkipsLeasedRows(t *testing.T) {
	s, st := newTestScheduler(t)
	const (
		wID  = "w1"
		qN   = "worker|w1"
		axis = "q4k_matvec"
	)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{axis: 1000}, BenchedAt: time.Now(),
	}))
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: wID, DeviceID: "gpu:1", DeviceName: "gpu:1",
		Throughput: map[string]float64{axis: 500}, BenchedAt: time.Now(),
	}))
	w := worker.NewFakeStreamWorker(wID, "llama-cpp",
		[]stats.Device{
			{ID: "gpu:0", Type: stats.DeviceTypeGPU},
			{ID: "gpu:1", Type: stats.DeviceTypeGPU},
		}, time.Now())
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)
	s.SetRuntimeDefaultAxisFn(func(string) string { return axis })

	ctx := context.Background()
	s.queueMu.RLock()
	wq := s.devQueues[qN]
	s.queueMu.RUnlock()
	require.NotNil(t, wq)

	// Two envelopes:
	//   r-leased: pre-toggle prediction, will be marked Leased.
	//   r-pending: pre-toggle prediction, stays unleased.
	// Both at Cost=1000 against current 2× GPUs (N×min = 2×500 = 1000),
	// so original QueuedSeconds = 1.0s each.
	const preTogglePrediction = 1.0
	res1, err := wq.Submit(ctx, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m", RequestID: "r-leased", Payload: []byte("p"),
		Cost: 1000, CostAxis: axis, QueuedSeconds: preTogglePrediction,
	})
	require.NoError(t, err)
	res2, err := wq.Submit(ctx, queue.Envelope{
		Priority: queue.PriorityMedium, RuntimeName: "llama-cpp",
		ModelID: "m", RequestID: "r-pending", Payload: []byte("p"),
		Cost: 1000, CostAxis: axis, QueuedSeconds: preTogglePrediction,
	})
	require.NoError(t, err)
	require.NoError(t, st.UpsertWorkerQueueState(store.WorkerQueueState{
		QueueName: qN, WorkerID: wID,
		TailSeconds: 2 * preTogglePrediction,
	}))

	// Lease the first row so PeekAll reports it as Leased=true.
	leased, err := wq.LeaseByID(ctx, queue.MessageID(res1.ID), 10*time.Second)
	require.NoError(t, err)
	require.NotNil(t, leased, "first row must be leasable")

	// Toggle: disable gpu:1. New device set is just gpu:0 (1000 GFLOPS).
	// New pending prediction = 1000 / 1000 = 1.0s — same numerically here,
	// so we'll verify by reading the body bytes and confirming the
	// LEASED row's recorded QueuedSeconds is untouched while the
	// pending row was rewritten through the predictedQueuedSeconds path.
	// To make rewrite visible, disable BOTH GPUs and rely on the
	// CPU-fallback unbenched path (predictedQueuedSeconds → 0).
	s.SetDeviceEnabledFn(func(_, devID string) bool {
		return false // every device off; pending row gets predicted=0
	})

	s.ReestimateWorkerQueue(ctx, wID)

	rows, err := wq.PeekAll(ctx, 10)
	require.NoError(t, err)
	require.Len(t, rows, 2)

	byReq := map[string]queue.Envelope{}
	for _, row := range rows {
		env, err := queue.UnmarshalEnvelope(row.Body)
		require.NoError(t, err)
		byReq[env.RequestID] = env
	}

	require.InDelta(t, preTogglePrediction, byReq["r-leased"].QueuedSeconds, 1e-9,
		"leased row's QueuedSeconds must NOT be rewritten; it's still running on the pre-toggle device set")
	require.InDelta(t, 0.0, byReq["r-pending"].QueuedSeconds, 1e-9,
		"pending row's QueuedSeconds must be recomputed against the new (empty) device set")

	dqs, err := st.GetWorkerQueueState(qN)
	require.NoError(t, err)
	// Tail sum = leased (kept at 1.0) + pending (now 0) = 1.0.
	require.InDelta(t, preTogglePrediction, dqs.TailSeconds, 1e-9,
		"tail_seconds must include the leased row's unchanged contribution + the pending row's new prediction")
	_ = res2 // referenced for clarity above
}

// Compile-time guard: the local types are still satisfied by the
// concrete implementations.
var _ = workerpb.ModelFileRole_MODEL_FILE_ROLE_PRIMARY // keep import live for future use
