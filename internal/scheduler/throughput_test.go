package scheduler

import (
	"testing"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/stretchr/testify/require"
)

// gpu1cpu1 hosts one GPU + one CPU so tests can exercise the
// max(gpu_sum, cpu) branch of effectiveLoadThroughput.
func gpu1cpu1() []stats.Device {
	return []stats.Device{
		{ID: "gpu:0", Type: stats.DeviceTypeGPU},
		{ID: "cpu:0", Type: stats.DeviceTypeCPU},
	}
}

// gpu2 hosts two GPUs so tests can exercise the gpu_sum branch + the
// split-resident min branch of effectiveLoadThroughput.
func gpu2() []stats.Device {
	return []stats.Device{
		{ID: "gpu:0", Type: stats.DeviceTypeGPU},
		{ID: "gpu:1", Type: stats.DeviceTypeGPU},
	}
}

// effectiveLoadThroughput's contract: sum LoadGBs across the device
// set the worker will use for the job. The table covers each route
// through deviceSet (resident DeviceIDs, TargetDeviceIDs, default) and
// the bench-availability edge cases.
func TestEffectiveLoadThroughput_Branches(t *testing.T) {
	const wID = "w1"

	tests := []struct {
		name     string
		devices  []stats.Device
		bench    []store.BenchmarkRow // by device id
		loaded   []worker.LoadedModelStatus
		disabled []string // device ids the operator turned off
		wantGBs  float64  // wantGBs * 1e9 == expected return
	}{
		{
			// Default placement: GPUs are preferred over CPU when any
			// GPU is enabled — mirrors the C++ worker's
			// allowed_load_devices() rule. Only the GPU contributes.
			name:    "default placement prefers GPUs over CPU",
			devices: gpu1cpu1(),
			bench: []store.BenchmarkRow{
				{WorkerID: wID, DeviceID: "gpu:0", LoadGBs: 20},
				{WorkerID: wID, DeviceID: "cpu:0", LoadGBs: 5},
			},
			wantGBs: 20,
		},
		{
			// Every GPU disabled → default placement falls back to CPU,
			// matching what the worker would actually use.
			name:     "all GPUs disabled falls back to CPU",
			devices:  gpu1cpu1(),
			disabled: []string{"gpu:0"},
			bench: []store.BenchmarkRow{
				{WorkerID: wID, DeviceID: "gpu:0", LoadGBs: 2},
				{WorkerID: wID, DeviceID: "cpu:0", LoadGBs: 7},
			},
			wantGBs: 7,
		},
		{
			// Two GPUs both enabled — sum across the placement set.
			name:    "default sums both GPUs",
			devices: gpu2(),
			bench: []store.BenchmarkRow{
				{WorkerID: wID, DeviceID: "gpu:0", LoadGBs: 10},
				{WorkerID: wID, DeviceID: "gpu:1", LoadGBs: 15},
			},
			wantGBs: 25,
		},
		{
			// Disabled device drops out of the default set.
			name:     "disabled GPU excluded from default sum",
			devices:  gpu2(),
			disabled: []string{"gpu:1"},
			bench: []store.BenchmarkRow{
				{WorkerID: wID, DeviceID: "gpu:0", LoadGBs: 10},
				{WorkerID: wID, DeviceID: "gpu:1", LoadGBs: 15},
			},
			wantGBs: 10,
		},
		{
			// Residency must NOT influence load-throughput scoring: the
			// resident set is a historical artefact, while load latency
			// is paid against the *current* enabled devices. A model
			// loaded onto one GPU does not make the second-GPU bench
			// number invisible to scoring once both are enabled.
			name:    "residency ignored — sums across enabled devices",
			devices: gpu2(),
			bench: []store.BenchmarkRow{
				{WorkerID: wID, DeviceID: "gpu:0", LoadGBs: 10},
				{WorkerID: wID, DeviceID: "gpu:1", LoadGBs: 15},
			},
			loaded:  []worker.LoadedModelStatus{{ModelID: "m-1", DeviceIDs: []string{"gpu:0"}}},
			wantGBs: 25,
		},
		{
			// Disabled device drops out of the sum even when the model
			// is resident on it. The operator's toggle is authoritative
			// over what's loaded — a subsequent load would skip the
			// disabled device anyway.
			name:     "disabled device drops out even if resident",
			devices:  gpu2(),
			disabled: []string{"gpu:1"},
			bench: []store.BenchmarkRow{
				{WorkerID: wID, DeviceID: "gpu:0", LoadGBs: 10},
				{WorkerID: wID, DeviceID: "gpu:1", LoadGBs: 15},
			},
			loaded:  []worker.LoadedModelStatus{{ModelID: "m-1", DeviceIDs: []string{"gpu:0", "gpu:1"}}},
			wantGBs: 10,
		},
		{
			// No bench rows at all — returns 0. loadLatencyForCand
			// reads this as "no penalty" so scheduling doesn't break.
			name:    "no benches → 0",
			devices: gpu1cpu1(),
			wantGBs: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, st := newTestScheduler(t)
			for _, b := range tt.bench {
				if b.Throughput == nil {
					b.Throughput = map[string]float64{"q4k_matvec": 100}
				}
				b.BenchedAt = time.Now()
				require.NoError(t, st.SaveBenchmark(b))
			}
			w := worker.NewFakeStreamWorker(wID, "llama-cpp", tt.devices, time.Now())
			if len(tt.loaded) > 0 {
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

			got := s.effectiveLoadThroughput(w)
			require.InDelta(t, tt.wantGBs*1e9, got, 1e-3)
		})
	}
}

// loadLatencyForCand's job is to skip the load penalty whenever the
// worker is effectively-warm. Each branch in the warm rule is
// load-bearing — a missed branch silently double-charges or under-
// charges a candidate, which biases placement.
func TestLoadLatencyForCand_Branches(t *testing.T) {
	s, _ := newTestScheduler(t)
	w := worker.NewFakeStreamWorker("w1", "llama-cpp", gpu1(), time.Now())
	const queueName = "worker|w1"
	const modelID = "m-1"
	envWithModel := queue.Envelope{ModelID: modelID, Files: []*workerpb.ModelFile{{SizeBytes: 1_000_000_000}}}

	tests := []struct {
		name         string
		env          queue.Envelope
		tailSec      float64
		loadBytes    int64
		loadBytesSec float64
		setup        func()
		want         float64
	}{
		{
			// No model id on the envelope — nothing to load. Embed jobs
			// look like this when the gateway omits model id.
			name:         "empty modelID short-circuits",
			env:          queue.Envelope{ModelID: ""},
			loadBytes:    1_000_000_000,
			loadBytesSec: 1e9,
			want:         0,
		},
		{
			// Zero load_bytes — Files slice was empty or every entry
			// had SizeBytes=0. Nothing to upload.
			name:         "zero load_bytes short-circuits",
			env:          envWithModel,
			loadBytes:    0,
			loadBytesSec: 1e9,
			want:         0,
		},
		{
			// Tail > 0 and tail_model_id matches: the queue's last
			// scheduled model is already this one, so by the time we
			// get there it'll be resident. Zero load cost.
			name:         "tail model matches — warm",
			env:          envWithModel,
			tailSec:      1.0,
			loadBytes:    1_000_000_000,
			loadBytesSec: 1e9,
			setup:        func() { s.creditTail(queueName, 1.0, modelID) },
			want:         0,
		},
		{
			// Empty tail, worker already has the model resident — also warm.
			name:         "resident on idle worker — warm",
			env:          envWithModel,
			loadBytes:    1_000_000_000,
			loadBytesSec: 1e9,
			setup: func() {
				w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, DeviceIDs: []string{"gpu:0"}}})
			},
			want: 0,
		},
		{
			// Cold path with throughput=0. Worker hasn't benched yet
			// (load_gbs == 0), so we can't price the load — return 0
			// rather than block placement entirely.
			name:         "cold but no bench yet — 0 penalty",
			env:          envWithModel,
			loadBytes:    1_000_000_000,
			loadBytesSec: 0,
			want:         0,
		},
		{
			// Cold path with a real throughput — bytes / throughput
			// in seconds. 10 GB at 5 GB/s = 2s.
			name:         "cold path with bench → bytes / throughput",
			env:          envWithModel,
			loadBytes:    10_000_000_000,
			loadBytesSec: 5e9,
			want:         2.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset worker + queue state between sub-tests so prior
			// setups don't leak through. The tail mirror entry is reset
			// directly: production has no "hard reset" mutation (entries
			// are created on connect and dropped on disconnect), and the
			// creditTail in a setup below needs an existing entry to
			// land on — same as it needs the store row upserted here.
			w.SetFakeLoadedModels(nil)
			require.NoError(t, s.store.UpsertWorkerQueueState(store.WorkerQueueState{
				QueueName: queueName, WorkerID: "w1",
			}))
			s.tailMu.Lock()
			s.tails[queueName] = tailState{}
			s.tailMu.Unlock()
			if tt.setup != nil {
				tt.setup()
			}
			got := loadLatencyForCand(w, queueName, tt.env, tt.tailSec, tt.loadBytes, tt.loadBytesSec, s)
			require.InDelta(t, tt.want, got, 1e-9)
		})
	}
}

// runtimeDefaultAxis is the bridge between MASS's scheduler and each
// gateway's declared cost axis. Three states matter: unset (no
// gateway plumbed), set with empty result (unknown runtime), and set
// with a real axis name.
func TestRuntimeDefaultAxis(t *testing.T) {
	tests := []struct {
		name string
		fn   RuntimeDefaultAxisFn
		want string
	}{
		{
			// Default state — no runtimes registered yet. tailModel
			// callers must accept an empty string; effectiveThroughput
			// treats it as "no fallback axis."
			name: "unset returns empty",
			fn:   nil,
			want: "",
		},
		{
			// Function plumbed but the named runtime isn't installed.
			// Same scheduling consequence as "unset" for this runtime.
			name: "unknown runtime → empty",
			fn: func(rn string) string {
				if rn == "llama-cpp" {
					return "q4k_matvec"
				}
				return ""
			},
			want: "",
		},
		{
			// Function returns a real axis — the happy path.
			name: "known runtime → axis name",
			fn: func(rn string) string {
				return "q4k_matvec"
			},
			want: "q4k_matvec",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestScheduler(t)
			s.SetRuntimeDefaultAxisFn(tt.fn)
			require.Equal(t, tt.want, s.runtimeDefaultAxis("other-runtime"))
		})
	}
}

// tailModel must survive the "no row yet" case — when a worker is
// freshly registered and no envelope has hit its queue. Without this
// path, effectivelyWarm scoring would NPE on the first dispatch.
func TestTailModel_AbsentQueueReturnsEmpty(t *testing.T) {
	s, _ := newTestScheduler(t)
	require.Equal(t, "", s.tailModel("worker|never-registered"))
}
