package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/stretchr/testify/require"
)

// pickWorkerQueue minimises expected completion time: inflight_seconds
// + tail_seconds + load_latency + the job's own compute seconds on the
// candidate. No hard residency filter — a non-resident worker pays
// load_latency = file_bytes/memory_throughput. Each subtest isolates one
// axis: power, in-flight, tail, capacity gate, aggregation across multiple
// GPUs, GPU-vs-CPU choice.
func TestPickWorkerQueue_Scoring(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"

	type workerSpec struct {
		id         string
		devices    []stats.Device
		gflops     map[string]float64 // device_id → benched ComputeGFlops; 0 means "no bench seeded"
		capacity   int
		hasModel   bool
		dropFromHB bool // when true, leave the worker unregistered (used for "no candidates" sanity)
	}
	tests := []struct {
		name      string
		workers   []workerSpec
		envModel  string
		envCost   float64
		inflight  map[string]float64
		tail      map[string]float64
		wantQueue string // empty when expecting nil
	}{
		{
			name: "two idle queues with equal power tie; insertion order wins",
			workers: []workerSpec{
				{id: "w1", devices: gpu1(), gflops: map[string]float64{"gpu:0": 100}, capacity: 1},
				{id: "w2", devices: gpu1(), gflops: map[string]float64{"gpu:0": 100}, capacity: 4},
			},
			wantQueue: "worker|w1",
		},
		{
			// Same-difficulty envelope dispatched to each worker yields a
			// power-proportional in-flight seconds value: w1 at 100 GFLOPS
			// records 1.0s for the same task w2 at 400 GFLOPS records as
			// 0.25s. Scoring is in seconds, so w2 wins.
			name: "higher GFLOPS wins when both queues carry the same task",
			workers: []workerSpec{
				{id: "w1", devices: gpu1(), gflops: map[string]float64{"gpu:0": 100}, capacity: 4},
				{id: "w2", devices: gpu1(), gflops: map[string]float64{"gpu:0": 400}, capacity: 4},
			},
			inflight:  map[string]float64{"worker|w1": 1.0, "worker|w2": 0.25},
			wantQueue: "worker|w2",
		},
		{
			name: "in-flight load shifts placement to idle peer",
			workers: []workerSpec{
				{id: "w1", devices: gpu1(), gflops: map[string]float64{"gpu:0": 100}, capacity: 4},
				{id: "w2", devices: gpu1(), gflops: map[string]float64{"gpu:0": 100}, capacity: 4},
			},
			inflight:  map[string]float64{"worker|w1": 50.0},
			wantQueue: "worker|w2",
		},
		{
			name: "tail_seconds shifts placement to lighter queue",
			workers: []workerSpec{
				{id: "w1", devices: gpu1(), gflops: map[string]float64{"gpu:0": 100}, capacity: 4},
				{id: "w2", devices: gpu1(), gflops: map[string]float64{"gpu:0": 100}, capacity: 4},
			},
			tail:      map[string]float64{"worker|w1": 80.0},
			wantQueue: "worker|w2",
		},
		{
			// Capacity gate is residency-aware: a resident worker at
			// capacity 0 is saturated and excluded. The dispatch will
			// load-on-demand on the non-resident peer instead.
			name: "saturated resident worker is excluded",
			workers: []workerSpec{
				{id: "w1", devices: gpu1(), gflops: map[string]float64{"gpu:0": 100}, capacity: 0, hasModel: true},
				{id: "w2", devices: gpu1(), gflops: map[string]float64{"gpu:0": 100}, capacity: 1},
			},
			envModel:  modelID,
			wantQueue: "worker|w2",
		},
		{
			// A non-resident worker reports capacity=0 until LoadModel
			// materialises its context pool. That's NOT a saturated
			// signal — placement must be allowed so dispatchEnvelope
			// can load on demand. With no envelope ModelID, residency
			// is irrelevant and capacity stays a hard gate.
			name: "non-resident zero-capacity worker is still a candidate",
			workers: []workerSpec{
				{id: "w1", devices: gpu1(), gflops: map[string]float64{"gpu:0": 100}, capacity: 0},
			},
			envModel:  modelID,
			wantQueue: "worker|w1",
		},
		{
			// Residency is no longer a hard gate. Without env.Files,
			// load_latency is 0 regardless of residency — the non-
			// resident worker is just as cheap to dispatch to. Both
			// scores tie at 0; insertion order picks w1.
			name: "non-resident worker is still a candidate (no files attached)",
			workers: []workerSpec{
				{id: "w1", devices: gpu1(), gflops: map[string]float64{"gpu:0": 100}, capacity: 1, hasModel: true},
				{id: "w2", devices: gpu1(), gflops: map[string]float64{"gpu:0": 800}, capacity: 8},
			},
			envModel:  modelID,
			wantQueue: "worker|w1",
		},
		{
			// effectivePower sums across enabled, benched GPUs. Two
			// modest GPUs together outscore one big one.
			name: "GPU GFLOPS sum across multiple devices",
			workers: []workerSpec{
				{
					id:       "w1",
					devices:  []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}, {ID: "gpu:1", Type: stats.DeviceTypeGPU}},
					gflops:   map[string]float64{"gpu:0": 300, "gpu:1": 300},
					capacity: 4,
				},
				{id: "w2", devices: gpu1(), gflops: map[string]float64{"gpu:0": 500}, capacity: 4},
			},
			inflight:  map[string]float64{"worker|w1": 1.0, "worker|w2": 1.0},
			envCost:   600,         // task time 1.0s on w1 vs 1.2s on w2 breaks the tie
			wantQueue: "worker|w1", // 600 GF total > 500 GF
		},
		{
			// The score includes the job's own compute time, so on an
			// idle fleet the faster worker wins instead of tying at 0
			// and falling to insertion order.
			name: "idle fleet: faster worker wins via task time",
			workers: []workerSpec{
				{id: "w1", devices: gpu1(), gflops: map[string]float64{"gpu:0": 100}, capacity: 4},
				{id: "w2", devices: gpu1(), gflops: map[string]float64{"gpu:0": 800}, capacity: 4},
			},
			envCost:   400,
			wantQueue: "worker|w2",
		},
		{
			// A worker with no enabled GPU but a benched CPU is
			// schedulable; effectivePower returns the CPU number.
			name: "CPU-only worker is schedulable when benched",
			workers: []workerSpec{
				{
					id:       "w1",
					devices:  []stats.Device{{ID: "cpu:0", Type: stats.DeviceTypeCPU}},
					gflops:   map[string]float64{"cpu:0": 80},
					capacity: 2,
				},
			},
			wantQueue: "worker|w1",
		},
		{
			// A worker whose only enabled device has no bench is
			// excluded from scoring (effectivePower returns 0).
			name: "unbenched worker is excluded",
			workers: []workerSpec{
				{id: "w1", devices: gpu1(), gflops: nil /* no bench */, capacity: 4},
				{id: "w2", devices: gpu1(), gflops: map[string]float64{"gpu:0": 100}, capacity: 4},
			},
			wantQueue: "worker|w2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, st := newTestScheduler(t)
			for _, spec := range tt.workers {
				if spec.dropFromHB {
					continue
				}
				for devID, gf := range spec.gflops {
					require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
						WorkerID: spec.id, DeviceID: devID, DeviceName: devID,
						Throughput: map[string]float64{"q4k_matvec": gf}, BenchedAt: time.Now(),
					}))
				}
				w := worker.NewFakeStreamWorker(spec.id, runtimeName, spec.devices, time.Now())
				w.SetFakeCapacity(spec.capacity)
				if spec.hasModel {
					w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
				}
				require.NoError(t, s.workers.Register(w))
				s.OnWorkerConnected(w)
			}

			// Seed in-flight + tail through the scheduler's public
			// mutator paths so tests exercise the same code the
			// dispatcher does.
			for qname, sec := range tt.inflight {
				s.startInflight(qname, "synthetic-"+qname, "", "", "", sec, 0)
			}
			for qname, sec := range tt.tail {
				s.creditTail(qname, sec, "")
			}

			env := queue.Envelope{
				RuntimeName: runtimeName,
				ModelID:     tt.envModel,
				Cost:        tt.envCost, CostAxis: "q4k_matvec",
			}
			target, _ := s.pickWorkerQueue(env)
			if tt.wantQueue == "" {
				require.Nil(t, target, "expected no target")
				return
			}
			require.NotNil(t, target, "expected a target")
			require.Equal(t, tt.wantQueue, target.name)
		})
	}
}

// effectiveThroughput predicts compute rate across the worker's enabled
// device set using N × min(rates) — llama.cpp tensor-split layers
// synchronise across all participating devices, so wall-clock is gated
// by the slowest. Homogeneous pairs collapse to "sum"; heterogeneous
// pairs honestly reflect slowest-link gating. Disabled / unbenched
// devices don't contribute to either N or min.
func TestEffectiveThroughput(t *testing.T) {
	const runtimeName = "llama-cpp"
	const axis = "q4k_matvec"
	tests := []struct {
		name      string
		devices   []stats.Device
		bench     map[string]float64 // device_id → axis throughput; missing = no bench row
		disabled  map[string]bool    // device_id → disabled
		wantPower float64
	}{
		{
			name:      "single GPU benched",
			devices:   gpu1(),
			bench:     map[string]float64{"gpu:0": 250},
			wantPower: 250,
		},
		{
			// Two GPUs, mildly heterogeneous. N=2, min=300 → 600.
			// (Old "sum" model would have said 700.)
			name:      "two GPUs gated by slowest",
			devices:   []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}, {ID: "gpu:1", Type: stats.DeviceTypeGPU}},
			bench:     map[string]float64{"gpu:0": 300, "gpu:1": 400},
			wantPower: 600,
		},
		{
			// Homogeneous pair: N × min == sum. Verifies the formula
			// collapses cleanly for the case operators most often expect.
			name:      "two homogeneous GPUs equal sum",
			devices:   []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}, {ID: "gpu:1", Type: stats.DeviceTypeGPU}},
			bench:     map[string]float64{"gpu:0": 400, "gpu:1": 400},
			wantPower: 800,
		},
		{
			// Wildly heterogeneous (1800 + 50): adding a weak GPU
			// gates everything to 2×50 = 100, not 1850. This is the
			// "enabling slow GPU made jobs slower in practice" case.
			name:      "heterogeneous pair gated by slow device",
			devices:   []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}, {ID: "gpu:1", Type: stats.DeviceTypeGPU}},
			bench:     map[string]float64{"gpu:0": 1800, "gpu:1": 50},
			wantPower: 100,
		},
		{
			name:      "CPU only",
			devices:   []stats.Device{{ID: "cpu:0", Type: stats.DeviceTypeCPU}},
			bench:     map[string]float64{"cpu:0": 80},
			wantPower: 80,
		},
		{
			name: "GPU + CPU prefers GPU sum",
			devices: []stats.Device{
				{ID: "gpu:0", Type: stats.DeviceTypeGPU},
				{ID: "cpu:0", Type: stats.DeviceTypeCPU},
			},
			bench:     map[string]float64{"gpu:0": 500, "cpu:0": 80},
			wantPower: 500,
		},
		{
			name: "CPU wins when GPUs disabled",
			devices: []stats.Device{
				{ID: "gpu:0", Type: stats.DeviceTypeGPU},
				{ID: "cpu:0", Type: stats.DeviceTypeCPU},
			},
			bench:     map[string]float64{"gpu:0": 500, "cpu:0": 80},
			disabled:  map[string]bool{"gpu:0": true},
			wantPower: 80,
		},
		{
			name: "unbenched GPU doesn't contribute",
			devices: []stats.Device{
				{ID: "gpu:0", Type: stats.DeviceTypeGPU},
				{ID: "gpu:1", Type: stats.DeviceTypeGPU},
			},
			bench:     map[string]float64{"gpu:0": 250}, // gpu:1 unbenched
			wantPower: 250,
		},
		{
			name:      "no bench data at all",
			devices:   gpu1(),
			bench:     nil,
			wantPower: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, st := newTestScheduler(t)
			if len(tt.disabled) > 0 {
				disabled := tt.disabled
				s.SetDeviceEnabledFn(func(_, devID string) bool {
					return !disabled[devID]
				})
			}
			for devID, gf := range tt.bench {
				require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
					WorkerID: "w1", DeviceID: devID, DeviceName: devID,
					Throughput: map[string]float64{"q4k_matvec": gf}, BenchedAt: time.Now(),
				}))
			}
			w := worker.NewFakeStreamWorker("w1", runtimeName, tt.devices, time.Now())
			got, usedAxis, ok := s.effectiveThroughput(w, axis, axis)
			if ok {
				require.Equal(t, axis, usedAxis, "exact-match axis must be the one used")
			}
			if tt.wantPower == 0 {
				require.False(t, ok, "no benched device should yield ok=false")
				require.InDelta(t, 0.0, got, 1e-9)
				return
			}
			require.True(t, ok)
			require.InDelta(t, tt.wantPower, got, 1e-9)
		})
	}
}

// effectiveThroughput predicts compute rate using N × min(rates) over
// the worker's currently-enabled, benched device set. Residency is
// intentionally NOT consulted; the operator's enable mask is the source
// of truth. The "slowest device gates the layer-step" model means a
// fast+slow pair predicts honestly less, not optimistically more.
func TestEffectiveThroughput_EnabledSet(t *testing.T) {
	const (
		runtimeName = "llama-cpp"
		modelID     = "qwen3-5-4b/Qwen3.5-4B-UD-Q4_K_XL.gguf#abc123"
	)
	tests := []struct {
		name      string
		devices   []stats.Device
		bench     map[string]float64
		loaded    []worker.LoadedModelStatus // residency: must not influence the result
		disabled  []string
		wantPower float64
	}{
		{
			// Two enabled GPUs, 1800+50 wildly heterogeneous: N×min =
			// 2 × 50 = 100. Residency on one GPU MUST NOT change this —
			// the operator's enable mask is the truth source.
			name: "heterogeneous GPUs gated by slowest, residency irrelevant",
			devices: []stats.Device{
				{ID: "gpu:0", Type: stats.DeviceTypeGPU},
				{ID: "gpu:1", Type: stats.DeviceTypeGPU},
			},
			bench: map[string]float64{"gpu:0": 1800, "gpu:1": 50},
			loaded: []worker.LoadedModelStatus{{
				ModelID:   modelID,
				DeviceIDs: []string{"gpu:0"},
			}},
			wantPower: 100,
		},
		{
			// GPU preferred over CPU when any GPU enabled. CPU drops
			// out of the device set entirely, so N=1 and the GPU's
			// rate stands alone — no slowest-link gating.
			name: "GPU enabled — CPU drops out even when resident on it",
			devices: []stats.Device{
				{ID: "gpu:0", Type: stats.DeviceTypeGPU},
				{ID: "cpu:0", Type: stats.DeviceTypeCPU},
			},
			bench: map[string]float64{"gpu:0": 1800, "cpu:0": 90},
			loaded: []worker.LoadedModelStatus{{
				ModelID:   modelID,
				DeviceIDs: []string{"gpu:0", "cpu:0"},
			}},
			wantPower: 1800,
		},
		{
			// Every GPU disabled → CPU fallback contributes alone.
			name:      "all GPUs disabled falls back to CPU bench",
			devices:   []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}, {ID: "cpu:0", Type: stats.DeviceTypeCPU}},
			bench:     map[string]float64{"gpu:0": 1800, "cpu:0": 90},
			disabled:  []string{"gpu:0"},
			wantPower: 90,
		},
		{
			// Disabling the slow GPU restores the fast GPU's full rate.
			// This is the operator workflow: "I observed slowdown, I
			// disable the weak card, my prediction recovers."
			name:      "disabling slow GPU restores fast GPU's full rate",
			devices:   []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}, {ID: "gpu:1", Type: stats.DeviceTypeGPU}},
			bench:     map[string]float64{"gpu:0": 1800, "gpu:1": 50},
			disabled:  []string{"gpu:1"},
			wantPower: 1800,
		},
		{
			// An enabled-but-unbenched device is treated as "not yet
			// measurable" — skipped from both N and min so the worker
			// stays schedulable on the benched subset.
			name:      "unbenched enabled device is skipped, not zeroed",
			devices:   []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}, {ID: "gpu:1", Type: stats.DeviceTypeGPU}},
			bench:     map[string]float64{"gpu:0": 1800},
			wantPower: 1800,
		},
		{
			// No enabled device has a bench row — unschedulable.
			name:      "no benched devices in enabled set returns 0",
			devices:   []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}},
			bench:     nil,
			wantPower: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, st := newTestScheduler(t)
			for devID, gf := range tt.bench {
				require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
					WorkerID: "w1", DeviceID: devID, DeviceName: devID,
					Throughput: map[string]float64{"q4k_matvec": gf}, BenchedAt: time.Now(),
				}))
			}
			w := worker.NewFakeStreamWorker("w1", runtimeName, tt.devices, time.Now())
			if tt.loaded != nil {
				w.SetFakeLoadedModels(tt.loaded)
			}
			require.NoError(t, s.workers.Register(w))
			disabled := make(map[string]bool, len(tt.disabled))
			for _, devID := range tt.disabled {
				disabled[devID] = true
			}
			s.SetDeviceEnabledFn(func(workerID, deviceID string) bool {
				return !disabled[deviceID]
			})
			got, _, ok := s.effectiveThroughput(w, "q4k_matvec", "q4k_matvec")
			if tt.wantPower == 0 {
				require.False(t, ok)
				require.InDelta(t, 0.0, got, 1e-9)
				return
			}
			require.True(t, ok)
			require.InDelta(t, tt.wantPower, got, 1e-9)
		})
	}
}

// pickWorkerQueue's queuedSeconds for a fast+slow GPU pair reflects
// the slowest-device gating that llama.cpp tensor-split actually
// delivers: 2 × min(1800, 50) = 100 GFLOPS effective. The earlier
// "sum" model said 1850 GFLOPS and would lie by 18× — operators saw
// jobs run slower in practice after enabling the weak GPU, matching
// the N×min prediction, not sum.
func TestPickWorkerQueue_HeterogeneousPairGatedBySlowest(t *testing.T) {
	const (
		runtimeName = "llama-cpp"
		modelID     = "m-split"
	)
	s, st := newTestScheduler(t)

	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 1800}, BenchedAt: time.Now(),
	}))
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:1", DeviceName: "gpu:1",
		Throughput: map[string]float64{"q4k_matvec": 50}, BenchedAt: time.Now(),
	}))
	w1 := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{
			{ID: "gpu:0", Type: stats.DeviceTypeGPU},
			{ID: "gpu:1", Type: stats.DeviceTypeGPU},
		}, time.Now())
	w1.SetFakeCapacity(4)
	w1.SetFakeLoadedModels([]worker.LoadedModelStatus{{
		ModelID: modelID, PoolSize: 1,
		DeviceIDs: []string{"gpu:0", "gpu:1"},
	}})
	require.NoError(t, s.workers.Register(w1))
	s.OnWorkerConnected(w1)

	target, qSec := s.pickWorkerQueue(queue.Envelope{
		RuntimeName: runtimeName,
		ModelID:     modelID,
		Cost:        float64(100), CostAxis: "q4k_matvec",
	})
	require.NotNil(t, target)
	require.Equal(t, "worker|w1", target.name)
	// 100 / (2 × 50) = 1.0s. The "sum" rule would have said 0.054s
	// — an 18× under-estimate that bit operators in practice.
	require.InDelta(t, 1.0, qSec, 0.001,
		"split-model queued_seconds must honour N × min(rates), not sum")
}

// Saturated GPU worker falls through to the CPU worker. The GPU
// worker is resident on the model AND has capacity=0 → the
// residency-aware capacity gate in pickWorkerQueue excludes it.
// The CPU worker (also resident, capacity>0) takes the placement.
// Exercises the cross-device fallback path that the existing scoring
// tests don't reach: every other Test*Scoring case uses single-device
// workers or pure GPU pairs.
func TestSubmit_SaturatedGPUFallsThroughToCPUWorker(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, st := newTestScheduler(t)

	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "gpu-worker", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 500}, BenchedAt: time.Now(),
	}))
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "cpu-worker", DeviceID: "cpu:0", DeviceName: "cpu:0",
		Throughput: map[string]float64{"q4k_matvec": 80}, BenchedAt: time.Now(),
	}))

	// GPU resident + capacity 0 = saturated → must be skipped.
	gpuW := worker.NewFakeStreamWorker("gpu-worker", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	gpuW.SetFakeCapacity(0)
	gpuW.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
	// CPU resident + capacity 4 = takes the load instead.
	cpuW := worker.NewFakeStreamWorker("cpu-worker", runtimeName,
		[]stats.Device{{ID: "cpu:0", Type: stats.DeviceTypeCPU}}, time.Now())
	cpuW.SetFakeCapacity(4)
	cpuW.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 4}})

	require.NoError(t, s.workers.Register(gpuW))
	require.NoError(t, s.workers.Register(cpuW))
	s.OnWorkerConnected(gpuW)
	s.OnWorkerConnected(cpuW)

	_, err := s.Submit(context.Background(), SubmitRequest{
		RuntimeName: runtimeName, ModelID: modelID,
		Payload: []byte("p"), Cost: 100, CostAxis: "q4k_matvec",
	})
	require.NoError(t, err)

	gpuRow, err := st.GetWorkerQueueState("worker|gpu-worker")
	require.NoError(t, err)
	cpuRow, err := st.GetWorkerQueueState("worker|cpu-worker")
	require.NoError(t, err)
	require.InDelta(t, 0.0, gpuRow.TailSeconds, 0.001,
		"saturated GPU worker must be skipped even when it would score lower")
	require.InDelta(t, 100.0/80.0, cpuRow.TailSeconds, 0.001,
		"CPU worker must absorb the placement when the GPU is saturated")
}

// A warm worker with a free slot stays eligible once its heartbeat
// reflects the job MASS has running on it. Debiting that job against a
// capacity number that already excludes it made the picker call a warm
// worker saturated and push the job to a cold peer, forcing a needless
// model load.
func TestPickWorkerQueue_SyncedHeartbeatKeepsWarmWorkerEligible(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"
	s, st := newTestScheduler(t)

	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "warm", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 500}, BenchedAt: time.Now(),
	}))
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "cold", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 80}, BenchedAt: time.Now(),
	}))

	// Pool of 2, one job running and reported by the heartbeat: one slot
	// really is free.
	warm := worker.NewFakeStreamWorker("warm", runtimeName, gpu1(), time.Now())
	warm.SetFakeCapacity(1)
	warm.SetFakeActiveJobs(1)
	warm.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 2, Active: 1}})
	cold := worker.NewFakeStreamWorker("cold", runtimeName, gpu1(), time.Now())
	cold.SetFakeCapacity(4)
	require.NoError(t, s.workers.Register(warm))
	require.NoError(t, s.workers.Register(cold))
	s.OnWorkerConnected(warm)
	s.OnWorkerConnected(cold)

	// The same job, from MASS's side.
	s.inflightMu.Lock()
	s.inflightByRequest["req-running"] = inflightRecord{
		queueName: "worker|warm", workerID: "warm", modelID: modelID,
	}
	s.inflightMu.Unlock()

	target, _ := s.pickWorkerQueue(queue.Envelope{
		RuntimeName: runtimeName, ModelID: modelID,
		Cost: 100, CostAxis: "q4k_matvec",
	})
	require.NotNil(t, target)
	require.Equal(t, "worker|warm", target.name,
		"a warm worker with a heartbeat-confirmed free slot must keep the placement")
}

// startInflight and finishInflight must round-trip cleanly: adding a
// seconds estimate and then removing it leaves the per-queue counter at
// the previous value. Critical because every dispatched job goes through
// this pair exactly once.
func TestInflightTracking_RoundTrip(t *testing.T) {
	s, _ := newTestScheduler(t)
	const queueName = "worker|w1"

	s.startInflight(queueName, "req-1", "", "", "", 1.0, 0)
	s.startInflight(queueName, "req-2", "", "", "", 2.0, 0)
	require.InDelta(t, 3.0, s.getInflightSeconds(queueName), 0.001)

	s.finishInflight("req-1")
	require.InDelta(t, 2.0, s.getInflightSeconds(queueName), 0.001)

	s.finishInflight("req-2")
	require.InDelta(t, 0.0, s.getInflightSeconds(queueName), 0.001)

	// Double-finish must be a no-op, not panic or underflow.
	s.finishInflight("req-1")
	require.InDelta(t, 0.0, s.getInflightSeconds(queueName), 0.001)
}

// InvalidateBench drops the cached row so the next getBenchmark call
// re-reads from the store. Verified by writing a row, priming the
// cache, mutating the row, and asserting the post-invalidation lookup
// sees the new value.
func TestBenchCache_Invalidate(t *testing.T) {
	s, st := newTestScheduler(t)
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
	}))

	row, ok := s.getBenchmark("w1", "gpu:0")
	require.True(t, ok)
	require.Equal(t, 100.0, row.Throughput["q4k_matvec"])

	// Update behind the cache; cached lookup must still return 100.
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 500}, BenchedAt: time.Now(),
	}))
	row, ok = s.getBenchmark("w1", "gpu:0")
	require.True(t, ok)
	require.Equal(t, 100.0, row.Throughput["q4k_matvec"], "stale cache should hold")

	s.InvalidateBench("w1", "gpu:0")
	row, ok = s.getBenchmark("w1", "gpu:0")
	require.True(t, ok)
	require.Equal(t, 500.0, row.Throughput["q4k_matvec"], "post-invalidate lookup should see fresh row")
}

func gpu1() []stats.Device {
	return []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}
}
