package scheduler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/stretchr/testify/require"
)

// benchModelKey picks the identity a bench row is written under: the
// PRIMARY artifact's store key, falling back to the first artifact that
// carries one. Getting this wrong silently unschedules every job for the
// model, so each shape is pinned.
func TestBenchModelKey(t *testing.T) {
	primary := &workerpb.ModelFile{Filename: "gguf/g/a.gguf", Role: workerpb.ModelFileRole_MODEL_FILE_ROLE_PRIMARY}
	mmproj := &workerpb.ModelFile{Filename: "gguf/g/mmproj.gguf", Role: workerpb.ModelFileRole_MODEL_FILE_ROLE_MMPROJ}
	untagged := &workerpb.ModelFile{Filename: "gguf/g/b.gguf"}

	tests := []struct {
		name  string
		files []*workerpb.ModelFile
		want  string
	}{
		{name: "no files", files: nil, want: ""},
		{name: "primary only", files: []*workerpb.ModelFile{primary}, want: "gguf/g/a.gguf"},
		{name: "primary after companion", files: []*workerpb.ModelFile{mmproj, primary}, want: "gguf/g/a.gguf"},
		{name: "untagged falls back to first", files: []*workerpb.ModelFile{untagged, mmproj}, want: "gguf/g/b.gguf"},
		{name: "nil and empty entries skipped", files: []*workerpb.ModelFile{nil, {}, untagged}, want: "gguf/g/b.gguf"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, benchModelKey(tt.files))
		})
	}
}

// Candidacy is exactly "a usable row for this worker's CURRENT predicted
// device set, measured against the model's current file". Every way that
// can fail must drop the worker from placement — a job dispatched
// against a missing, incapable, or stale measurement is a job MASS
// cannot honestly price.
func TestModelBenchmark_Candidacy(t *testing.T) {
	const (
		runtimeName = "llama-cpp"
		modelKey    = "gguf/g/a.gguf"
	)
	tests := []struct {
		name string
		// seed records the row (if any) the store holds.
		seed     func(st *store.Store)
		disabled map[string]bool
		wantOK   bool
		wantRate float64
	}{
		{
			name: "usable row on the predicted set is a candidate",
			seed: func(st *store.Store) {
				seedModelBench(t, st, "w1", "gpu:0", modelKey, 250)
			},
			wantOK:   true,
			wantRate: 250,
		},
		{
			name:   "no row at all: not a candidate",
			seed:   func(*store.Store) {},
			wantOK: false,
		},
		{
			name: "incapable row: not a candidate",
			seed: func(st *store.Store) {
				require.NoError(t, st.SaveModelBenchmarkError(store.ModelBenchmarkRow{
					WorkerID: "w1", DeviceSet: "gpu:0", ModelID: modelKey,
					Error: "out of memory",
				}))
			},
			wantOK: false,
		},
		{
			name: "row measured with zero throughput: not a candidate",
			seed: func(st *store.Store) {
				seedModelBench(t, st, "w1", "gpu:0", modelKey, 0)
			},
			wantOK: false,
		},
		{
			name: "row for another device set: not a candidate",
			seed: func(st *store.Store) {
				seedModelBench(t, st, "w1", "cpu:0", modelKey, 250)
			},
			wantOK: false,
		},
		{
			name: "row kept for a set the operator re-enables becomes a candidate again",
			seed: func(st *store.Store) {
				seedModelBench(t, st, "w1", "cpu:0", modelKey, 90)
			},
			disabled: map[string]bool{"gpu:0": true},
			wantOK:   true,
			wantRate: 90,
		},
		{
			name: "every device disabled: not a candidate",
			seed: func(st *store.Store) {
				seedModelBench(t, st, "w1", "gpu:0", modelKey, 250)
			},
			disabled: map[string]bool{"gpu:0": true, "cpu:0": true},
			wantOK:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, st := newStrictTestScheduler(t)
			tt.seed(st)
			if len(tt.disabled) > 0 {
				disabled := tt.disabled
				s.SetDeviceEnabledFn(func(_, devID string) bool { return !disabled[devID] })
			}
			w := worker.NewFakeStreamWorker("w1", runtimeName, []stats.Device{
				{ID: "gpu:0", Type: stats.DeviceTypeGPU},
				{ID: "cpu:0", Type: stats.DeviceTypeCPU},
			}, time.Now())
			require.NoError(t, s.workers.Register(w))

			env := queue.Envelope{
				RuntimeName: runtimeName,
				ModelID:     "m-1",
				Files:       []*workerpb.ModelFile{benchModelFile(modelKey, 0)},
			}
			row, ok := s.modelBenchmark(w, env)
			require.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				require.InDelta(t, tt.wantRate, row.UnitsPerSec, 1e-9)
			}
		})
	}
}

// A row is only valid for the file it measured. Once the model's bytes
// change on disk the numbers describe different weights, so the row must
// read as absent until a re-bench replaces it.
func TestModelBenchmark_StaleModelFile(t *testing.T) {
	const (
		runtimeName = "llama-cpp"
		modelKey    = "gguf/g/a.gguf"
	)
	s, st := newStrictTestScheduler(t)

	modelsDir := t.TempDir()
	path := filepath.Join(modelsDir, filepath.FromSlash(modelKey))
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("weights"), 0o644))
	info, err := os.Stat(path)
	require.NoError(t, err)
	s.SetModelsDir(modelsDir)

	require.NoError(t, st.SaveModelBenchmark(store.ModelBenchmarkRow{
		WorkerID: "w1", DeviceSet: "gpu:0", ModelID: modelKey,
		UnitsPerSec: 250, GraphSecs: 0.1,
		ModelSize: info.Size(), ModelMTime: info.ModTime().Unix(),
	}))

	w := worker.NewFakeStreamWorker("w1", runtimeName, gpu1(), time.Now())
	require.NoError(t, s.workers.Register(w))
	env := queue.Envelope{
		RuntimeName: runtimeName,
		Files:       []*workerpb.ModelFile{benchModelFile(modelKey, 0)},
	}

	_, ok := s.modelBenchmark(w, env)
	require.True(t, ok, "matching size + mtime must stay a candidate")

	// Rewrite the file with different bytes and a later mtime.
	require.NoError(t, os.WriteFile(path, []byte("different weights"), 0o644))
	require.NoError(t, os.Chtimes(path, time.Now().Add(time.Hour), time.Now().Add(time.Hour)))
	s.InvalidateModelBenchmarks("")
	_, ok = s.modelBenchmark(w, env)
	require.False(t, ok, "a changed model file invalidates the measurement")

	// A file MASS can't stat at all is treated the same way.
	require.NoError(t, os.Remove(path))
	s.InvalidateModelBenchmarks("")
	_, ok = s.modelBenchmark(w, env)
	require.False(t, ok, "an unreadable model file invalidates the measurement")
}

// Estimates come straight from the row: seconds = Cost / units_per_sec,
// with no correction factor. The same job on the same worker must always
// price the same.
func TestPredictedQueuedSeconds_FromRow(t *testing.T) {
	const (
		runtimeName = "llama-cpp"
		modelKey    = "gguf/g/a.gguf"
	)
	tests := []struct {
		name        string
		unitsPerSec float64
		cost        float64
		want        float64
	}{
		{name: "cost equals rate: one second", unitsPerSec: 100, cost: 100, want: 1},
		{name: "faster measurement: less time", unitsPerSec: 800, cost: 400, want: 0.5},
		{name: "slow measurement: more time", unitsPerSec: 50, cost: 400, want: 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, st := newStrictTestScheduler(t)
			seedModelBench(t, st, "w1", "gpu:0", modelKey, tt.unitsPerSec)
			w := worker.NewFakeStreamWorker("w1", runtimeName, gpu1(), time.Now())
			require.NoError(t, s.workers.Register(w))

			env := queue.Envelope{
				RuntimeName: runtimeName,
				ModelID:     "m-1",
				Cost:        tt.cost,
				Files:       []*workerpb.ModelFile{benchModelFile(modelKey, 0)},
			}
			// residentID == env.ModelID so no load-switch term muddies
			// the compute math.
			require.InDelta(t, tt.want, s.predictedQueuedSeconds(w, env, 1e9, "m-1"), 1e-9)
		})
	}
}

// The pool size is the latency budget divided by the measured decode
// time, clamped to the configured cap and then bounded by what the
// worker's free memory can hold at the measured per-slot cost.
func TestPlannedPoolSize(t *testing.T) {
	const gb = int64(1024 * 1024 * 1024)
	tests := []struct {
		name         string
		budget       float64
		slotsCap     int
		graphSecs    float64
		baseBytes    int64
		perSlotBytes int64
		deviceMB     int
		usedMB       int
		headroomPct  int32
		want         int
	}{
		{
			// 2.5s budget / 0.1s decode = 25 slots, capped at 16. No
			// memory dimension measured, so nothing bounds it further.
			name:      "budget beats cap: clamped to cap",
			graphSecs: 0.1,
			want:      16,
		},
		{
			// 2.5 / 0.5 = 5 slots, comfortably under the cap.
			name:      "budget below cap: budget wins",
			graphSecs: 0.5,
			want:      5,
		},
		{
			// A decode slower than the whole budget still gets one slot
			// — a zero-slot pool can serve nothing.
			name:      "decode slower than budget: floor of one",
			graphSecs: 10,
			want:      1,
		},
		{
			// No decode time measured: fall back to the cap and let
			// memory decide.
			name:      "no decode time: cap",
			graphSecs: 0,
			want:      16,
		},
		{
			// 24 GB free, base 5 GB (pool of 1), 2 GB per extra slot,
			// 75% headroom: (24-5) * 0.75 = 14.25 GB → 7 extra slots →
			// 8 total, below the budget's 16.
			name:         "memory bound below the budget",
			graphSecs:    0.1,
			baseBytes:    5 * gb,
			perSlotBytes: 2 * gb,
			deviceMB:     24 * 1024,
			headroomPct:  75,
			want:         8,
		},
		{
			// Free memory barely covers the base: no extra slot fits.
			name:         "no headroom for extra slots: one",
			graphSecs:    0.1,
			baseBytes:    23 * gb,
			perSlotBytes: 2 * gb,
			deviceMB:     24 * 1024,
			headroomPct:  75,
			want:         1,
		},
		{
			// Memory allows far more than the budget asks for; the
			// budget stays the binding rule.
			name:         "budget bound below memory",
			graphSecs:    1.25,
			baseBytes:    1 * gb,
			perSlotBytes: 1024 * 1024,
			deviceMB:     24 * 1024,
			headroomPct:  100,
			want:         2,
		},
		{
			// Operator config overrides both defaults.
			name:      "configured budget and cap are honoured",
			budget:    1.0,
			slotsCap:  3,
			graphSecs: 0.1,
			want:      3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newStrictTestScheduler(t)
			s.cfg.BenchBudgetSeconds = tt.budget
			s.cfg.BenchSlotsCap = tt.slotsCap

			var devices []stats.Device
			var devStats []stats.DeviceStats
			if tt.deviceMB > 0 {
				devices = []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: tt.deviceMB}}
				devStats = []stats.DeviceStats{{DeviceID: "gpu:0", UsedMemoryMB: tt.usedMB, TotalMemoryMB: tt.deviceMB}}
			} else {
				devices = gpu1()
			}
			w := worker.NewFakeStreamWorker("w1", "llama-cpp", devices, time.Now())
			if len(devStats) > 0 {
				w.SetFakeDeviceStats(devStats)
			}
			require.NoError(t, s.workers.Register(w))

			got := s.plannedPoolSize(w,
				queue.Envelope{HeadroomPct: tt.headroomPct},
				store.ModelBenchmarkRow{
					GraphSecs:    tt.graphSecs,
					BaseBytes:    tt.baseBytes,
					PerSlotBytes: tt.perSlotBytes,
				})
			require.Equal(t, tt.want, got)
		})
	}
}

// A job whose model has no usable row anywhere waits on the global queue
// while any bench is still owed, and fails with the recorded verdict
// once every eligible worker has concluded it can't run the model.
func TestDrainGlobal_WaitsThenFailsWhenEveryWorkerConcluded(t *testing.T) {
	const (
		runtimeName = "llama-cpp"
		modelKey    = "gguf/g/a.gguf"
	)
	ctx := context.Background()
	s, st := newStrictTestScheduler(t)

	for _, id := range []string{"w1", "w2"} {
		require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: id, DeviceID: "gpu:0", DeviceName: "gpu:0",
			Flops: 100, BenchedAt: time.Now(),
		}))
		w := worker.NewFakeStreamWorker(id, runtimeName, gpu1(), time.Now())
		w.SetFakeCapacity(4)
		require.NoError(t, s.workers.Register(w))
		s.OnWorkerConnected(w)
	}

	rid, err := s.Submit(ctx, SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     "m-1",
		Payload:     []byte("p"),
		Cost:        100,
		Files:       []*workerpb.ModelFile{benchModelFile(modelKey, 0)},
	})
	require.NoError(t, err)

	// Nothing benched yet: the row waits.
	require.True(t, s.drainGlobal(ctx), "job must stay pending while a bench is owed")
	res, err := s.GetResult(rid)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusPending, res.Status)

	// One worker concludes incapable, the other is still benching.
	require.NoError(t, st.SaveModelBenchmarkError(store.ModelBenchmarkRow{
		WorkerID: "w1", DeviceSet: "gpu:0", ModelID: modelKey,
		Error: "model larger than device",
	}))
	s.InvalidateModelBenchmarks("")
	require.True(t, s.drainGlobal(ctx), "one pending bench still holds the job")
	res, err = s.GetResult(rid)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusPending, res.Status)

	// Both concluded incapable: the job fails with the recorded verdict.
	require.NoError(t, st.SaveModelBenchmarkError(store.ModelBenchmarkRow{
		WorkerID: "w2", DeviceSet: "gpu:0", ModelID: modelKey,
		Error: "model larger than device",
	}))
	s.InvalidateModelBenchmarks("")
	require.False(t, s.drainGlobal(ctx), "nothing is owed any more; the row must not linger")

	res, err = s.GetResult(rid)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusError, res.Status)
	require.Contains(t, res.Error, "model larger than device")

	depth, err := s.globalQ.Depth(ctx)
	require.NoError(t, err)
	require.Zero(t, depth, "the failed job's global row must be gone")
}

// A usable row anywhere keeps the job waiting even when it can't be
// placed right now — the worker is merely busy or short on memory, and
// waiting is the correct answer.
func TestDrainGlobal_UsableRowKeepsJobWaiting(t *testing.T) {
	const (
		runtimeName = "llama-cpp"
		modelKey    = "gguf/g/a.gguf"
	)
	ctx := context.Background()
	s, st := newStrictTestScheduler(t)

	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Flops: 100, BenchedAt: time.Now(),
	}))
	// A usable row, but the load doesn't fit the worker's free memory.
	seedModelBenchMem(t, st, "w1", "gpu:0", modelKey, 100, 64*1024*1024*1024, 0)

	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: 8 * 1024}}, time.Now())
	w.SetFakeDeviceStats([]stats.DeviceStats{{DeviceID: "gpu:0", UsedMemoryMB: 0, TotalMemoryMB: 8 * 1024}})
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	rid, err := s.Submit(ctx, SubmitRequest{
		RuntimeName: runtimeName,
		ModelID:     "m-1",
		Payload:     []byte("p"),
		Cost:        100,
		Files:       []*workerpb.ModelFile{benchModelFile(modelKey, 0)},
	})
	require.NoError(t, err)

	require.True(t, s.drainGlobal(ctx), "a usable row means the job waits, not fails")
	res, err := s.GetResult(rid)
	require.NoError(t, err)
	require.Equal(t, queue.ResultStatusPending, res.Status)
}

// Every load MASS emits pins the context pool: the worker disables its
// own VRAM headroom gate when it sees a pinned size, so a 0 here would
// silently hand the load back to unbounded growth. The number must be
// the same one plannedPoolSize derived from the model's row, under both
// the latency-budget rule and the memory bound.
func TestDispatchEnvelope_LoadCarriesPinnedPoolSize(t *testing.T) {
	const (
		runtimeName = "llama-cpp"
		modelID     = "m-1"
		modelKey    = "gguf/g/a.gguf"
		gb          = int64(1024 * 1024 * 1024)
	)
	tests := []struct {
		name         string
		graphSecs    float64
		baseBytes    int64
		perSlotBytes int64
		deviceMB     int
		headroomPct  int32
		want         int32
	}{
		{
			// 2.5s budget / 0.5s decode = 5 slots; the row records no
			// memory dimension, so nothing trims it.
			name:      "budget-bound pool",
			graphSecs: 0.5,
			deviceMB:  24 * 1024,
			want:      5,
		},
		{
			// 2.5 / 0.1 = 25 slots, capped at 16 — but 24 GB free, a
			// 5 GB base and 2 GB per slot at 75% headroom only leaves
			// room for 7 more, so 8 wins.
			name:         "memory-bound pool",
			graphSecs:    0.1,
			baseBytes:    5 * gb,
			perSlotBytes: 2 * gb,
			deviceMB:     24 * 1024,
			headroomPct:  75,
			want:         8,
		},
		{
			// A model whose single decode outlasts the whole budget on
			// hardware with no room to grow still gets one slot — never
			// zero, which would mean "grow until the watermark".
			name:         "starved pool floors at one, never zero",
			graphSecs:    10,
			baseBytes:    23 * gb,
			perSlotBytes: 2 * gb,
			deviceMB:     24 * 1024,
			headroomPct:  75,
			want:         1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, st := newStrictTestScheduler(t)
			require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
				WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
				Flops: 100, BenchedAt: time.Now(),
			}))
			require.NoError(t, st.SaveModelBenchmark(store.ModelBenchmarkRow{
				WorkerID: "w1", DeviceSet: "gpu:0", ModelID: modelKey,
				UnitsPerSec:  100,
				GraphSecs:    tt.graphSecs,
				BaseBytes:    tt.baseBytes,
				PerSlotBytes: tt.perSlotBytes,
			}))

			devices := []stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: tt.deviceMB}}
			w := worker.NewFakeStreamWorker("w1", runtimeName, devices, time.Now())
			w.SetFakeDeviceStats([]stats.DeviceStats{{DeviceID: "gpu:0", UsedMemoryMB: 0, TotalMemoryMB: tt.deviceMB}})
			w.SetFakeCapacity(0) // nothing resident: the dispatch must load
			loads := make(chan *workerpb.HubLoadModel, 1)
			w.SetFakeSender(func(msg *workerpb.HubMessage) error {
				if lm := msg.GetLoadModel(); lm != nil {
					select {
					case loads <- lm:
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
				Cost:        100,
				HeadroomPct: tt.headroomPct,
				Files:       []*workerpb.ModelFile{benchModelFile(modelKey, 1024)},
			})
			require.NoError(t, err)
			s.dispatchPass(context.Background())

			select {
			case lm := <-loads:
				require.Positive(t, lm.GetMaxConcurrent(),
					"a load must never leave the pool size unset: 0 means unbounded growth")
				require.Equal(t, tt.want, lm.GetMaxConcurrent())
			case <-time.After(3 * time.Second):
				t.Fatal("dispatch did not emit a load within 3s")
			}
		})
	}
}
