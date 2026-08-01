package scheduler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/stretchr/testify/require"
)

// Submit's preflight surfaces no-worker errors synchronously so a gateway
// sees admission failures on the RPC return rather than via a stuck
// queue row. Model residency is no longer preflight-checked: the
// dispatcher loads on demand at pop time using the envelope's inline
// files + load_hints, so a non-resident model is a valid Submit.
func TestSubmit_PreflightErrors(t *testing.T) {
	const runtimeName = "llama-cpp"
	const modelID = "m-1"

	type workerSpec struct {
		runtime  string
		hasModel bool
	}
	tests := []struct {
		name     string
		workers  []workerSpec
		envModel string
		wantErr  error
	}{
		{
			name:     "no worker for runtime returns ErrNoWorker",
			workers:  nil,
			envModel: modelID,
			wantErr:  ErrNoWorker,
		},
		{
			name:     "worker exists but wrong runtime returns ErrNoWorker",
			workers:  []workerSpec{{runtime: "other-runtime", hasModel: true}},
			envModel: modelID,
			wantErr:  ErrNoWorker,
		},
		{
			name:     "non-resident model succeeds — Submit no longer preflight-checks residency",
			workers:  []workerSpec{{runtime: runtimeName, hasModel: false}},
			envModel: modelID,
			wantErr:  nil,
		},
		{
			name:     "model resident on at least one worker succeeds",
			workers:  []workerSpec{{runtime: runtimeName, hasModel: true}},
			envModel: modelID,
			wantErr:  nil,
		},
		{
			name:     "empty modelID succeeds (no residency lookup involved)",
			workers:  []workerSpec{{runtime: runtimeName, hasModel: false}},
			envModel: "",
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, st := newTestScheduler(t)
			for i, ws := range tt.workers {
				wID := "w" + string(rune('1'+i))
				require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
					WorkerID: wID, DeviceID: "gpu:0", DeviceName: "gpu:0",
					Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
				}))
				w := worker.NewFakeStreamWorker(wID, ws.runtime,
					[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
				w.SetFakeCapacity(4)
				if ws.hasModel {
					w.SetFakeLoadedModels([]worker.LoadedModelStatus{{ModelID: modelID, PoolSize: 1}})
				}
				require.NoError(t, s.workers.Register(w))
				s.OnWorkerConnected(w)
			}

			_, err := s.Submit(context.Background(), SubmitRequest{
				RuntimeName: runtimeName,
				ModelID:     tt.envModel,
				Payload:     []byte("p"),
				Cost:        100, CostAxis: "q4k_matvec",
			})
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.True(t, errors.Is(err, tt.wantErr),
				"got %v, want errors.Is(%v)", err, tt.wantErr)
		})
	}
}

// Submit rejects gateway-supplied identity fields longer than the envelope
// wire format's 255-byte cap with ErrFieldTooLong — the alternative
// (Marshal-time truncation) silently corrupts identity. The check runs
// before preflight, so no worker fleet is needed. The resolved source
// ("gateway:" + runtime_name when Source is empty) counts against the cap.
func TestSubmit_RejectsOversizeIdentityFields(t *testing.T) {
	oversize := strings.Repeat("a", 256)
	tests := []struct {
		name string
		req  SubmitRequest
	}{
		{
			name: "256-byte model_id",
			req:  SubmitRequest{RuntimeName: "llama-cpp", ModelID: oversize, Cost: 1, CostAxis: "ax"},
		},
		{
			name: "256-byte runtime_name",
			req:  SubmitRequest{RuntimeName: oversize, ModelID: "m", Cost: 1, CostAxis: "ax"},
		},
		{
			name: "256-byte cost_axis",
			req:  SubmitRequest{RuntimeName: "llama-cpp", ModelID: "m", Cost: 1, CostAxis: oversize},
		},
		{
			name: "256-byte source",
			req:  SubmitRequest{RuntimeName: "llama-cpp", ModelID: "m", Cost: 1, CostAxis: "ax", Source: oversize},
		},
		{
			name: "resolved default source over the cap",
			req:  SubmitRequest{RuntimeName: strings.Repeat("a", 250), ModelID: "m", Cost: 1, CostAxis: "ax"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestScheduler(t)
			_, err := s.Submit(context.Background(), tt.req)
			require.ErrorIs(t, err, ErrFieldTooLong)
		})
	}
}

// Submit stamps the request's priority onto the persisted envelope so the
// worker queue's priority-ordered dequeue honours it. Submitting with no
// worker leaves the row on global, where we can read the envelope back.
func TestSubmit_StampsPriorityOnEnvelope(t *testing.T) {
	const runtimeName = "llama-cpp"
	tests := []struct {
		name string
		prio queue.Priority
	}{
		{"low", queue.PriorityLow},
		{"medium", queue.PriorityMedium},
		{"high", queue.PriorityHigh},
		{"critical", queue.PriorityCritical},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestScheduler(t)
			// One worker so preflight passes; no benchmark so it isn't
			// schedulable and the row stays unleased on global for us to read.
			w := worker.NewFakeStreamWorker("w1", runtimeName,
				[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
			require.NoError(t, s.workers.Register(w))

			_, err := s.Submit(context.Background(), SubmitRequest{
				RuntimeName: runtimeName,
				ModelID:     "m-1",
				Payload:     []byte("p"),
				Cost:        100, CostAxis: "q4k_matvec",
				Priority: tt.prio,
			})
			require.NoError(t, err)

			msgs, err := s.globalQ.Peek(context.Background(), 4)
			require.NoError(t, err)
			require.Len(t, msgs, 1)
			env, err := queue.UnmarshalEnvelope(msgs[0].Body)
			require.NoError(t, err)
			require.Equal(t, tt.prio, env.Priority)
		})
	}
}

// drainGlobal reports pending=true when a peeked row has no eligible
// target (so it stays on global), and false once it places. The dispatch
// loop uses that signal to retry on the short interval instead of the slow
// ticker — the cadence improvement hinges on this boolean.
func TestDrainGlobal_PendingSignal(t *testing.T) {
	const runtimeName = "llama-cpp"

	t.Run("unplaceable row -> pending true", func(t *testing.T) {
		s, _ := newTestScheduler(t)
		// Registered worker (preflight passes) but NOT benched, so
		// pickWorkerQueue finds no candidate and the row stays on global.
		w := worker.NewFakeStreamWorker("w1", runtimeName,
			[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
		require.NoError(t, s.workers.Register(w))

		_, err := s.Submit(context.Background(), SubmitRequest{
			RuntimeName: runtimeName, ModelID: "m-1", Payload: []byte("p"),
			Cost: 100, CostAxis: "q4k_matvec",
		})
		require.NoError(t, err)
		require.True(t, s.drainGlobal(context.Background()),
			"a row no worker can take must report pending")
	})

	t.Run("placeable row -> pending false", func(t *testing.T) {
		s, st := newTestScheduler(t)
		require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
			WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
			Throughput: map[string]float64{"q4k_matvec": 100}, BenchedAt: time.Now(),
		}))
		w := worker.NewFakeStreamWorker("w1", runtimeName,
			[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
		w.SetFakeCapacity(4)
		require.NoError(t, s.workers.Register(w))
		s.OnWorkerConnected(w)

		// Submit places inline (Submit calls placeOnWorkerQueue), so by the
		// time drainGlobal runs the global row is leased and not re-peeked.
		_, err := s.Submit(context.Background(), SubmitRequest{
			RuntimeName: runtimeName, ModelID: "m-1", Payload: []byte("p"),
			Cost: 100, CostAxis: "q4k_matvec",
		})
		require.NoError(t, err)
		require.False(t, s.drainGlobal(context.Background()),
			"a placed row leaves nothing unplaceable on global")
	})
}

// Submit rejects the throughput-contract fields when the gateway
// omits them. Cost > 0 and a non-empty CostAxis are mandatory so MASS
// can score the envelope against worker throughputs.
func TestSubmit_RejectsInvalidThroughputContract(t *testing.T) {
	const runtimeName = "llama-cpp"
	tests := []struct {
		name    string
		cost    float64
		axis    string
		wantErr error
	}{
		{name: "zero cost", cost: 0, axis: "q4k_matvec", wantErr: ErrInvalidCost},
		{name: "negative cost", cost: -1, axis: "q4k_matvec", wantErr: ErrInvalidCost},
		{name: "empty axis", cost: 100, axis: "", wantErr: ErrInvalidCostAxis},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestScheduler(t)
			_, err := s.Submit(context.Background(), SubmitRequest{
				RuntimeName: runtimeName,
				ModelID:     "m-1",
				Payload:     []byte("p"),
				Cost:        tt.cost,
				CostAxis:    tt.axis,
			})
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// effectiveThroughput falls back to the runtime's default axis when the
// worker hasn't benched the requested axis. Verifies the bridge that
// lets MASS still place jobs on partially-benched workers.
func TestEffectiveThroughput_FallsBackToDefaultAxis(t *testing.T) {
	const runtimeName = "llama-cpp"
	s, st := newTestScheduler(t)

	// Worker benched on q4k_matvec only.
	require.NoError(t, st.SaveBenchmark(store.BenchmarkRow{
		WorkerID: "w1", DeviceID: "gpu:0", DeviceName: "gpu:0",
		Throughput: map[string]float64{"q4k_matvec": 250},
		BenchedAt:  time.Now(),
	}))
	w := worker.NewFakeStreamWorker("w1", runtimeName,
		[]stats.Device{{ID: "gpu:0", Type: stats.DeviceTypeGPU}}, time.Now())
	require.NoError(t, s.workers.Register(w))
	s.OnWorkerConnected(w)

	// Request a different axis; expect fallback to q4k_matvec — and the
	// returned usedAxis must name the fallback, since callers key
	// correction samples by it.
	got, usedAxis, ok := s.effectiveThroughput(w, "f16_matmul", "q4k_matvec")
	require.True(t, ok, "fallback to default axis should succeed")
	require.Equal(t, "q4k_matvec", usedAxis)
	require.InDelta(t, 250.0, got, 1e-9)

	// And confirms exact-match reports the requested axis as used.
	got, usedAxis, ok = s.effectiveThroughput(w, "q4k_matvec", "q4k_matvec")
	require.True(t, ok)
	require.Equal(t, "q4k_matvec", usedAxis)
	require.InDelta(t, 250.0, got, 1e-9)

	// And when neither the requested axis nor the default is benched,
	// the candidate is unschedulable.
	got, _, ok = s.effectiveThroughput(w, "f16_matmul", "denoise_step_per_sec")
	require.False(t, ok)
	require.InDelta(t, 0.0, got, 1e-9)
}
