package web

import (
	"net/http"
	"net/http/httptest"
	"testing"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

const benchModelKey = "gguf/qwen/qwen3.gguf"

// initSchedulerQueue wires the scheduler's queue subsystem, which the
// re-bench path needs (its store slice is set there).
func initSchedulerQueue(t *testing.T, h *Handler) {
	t.Helper()
	st, ok := h.store.(*store.Store)
	require.True(t, ok)
	h.orch.InitQueue(queue.NewPool(st.DB(), st.Dialect()), queue.NewResultStore(st.DB(), st.Dialect()), st)
}

func measuredRow(workerID, deviceSet string) store.ModelBenchmarkRow {
	return store.ModelBenchmarkRow{
		WorkerID: workerID, DeviceSet: deviceSet, ModelID: benchModelKey,
		UnitsPerSec: 42.5, GraphSecs: 0.031, BaseBytes: 5 << 30, PerSlotBytes: 128 << 20,
		ModelSize: 4096, ModelMTime: 1_700_000_000,
	}
}

func TestBuildModelBenchRows(t *testing.T) {
	tests := []struct {
		name     string
		rows     []store.ModelBenchmarkRow
		names    map[string]string
		benching []string
		want     []templates.ModelBenchRowView
	}{
		{
			name:  "usable row carries its measurements",
			rows:  []store.ModelBenchmarkRow{measuredRow("w1", "gpu:0")},
			names: map[string]string{"w1": "host-a"},
			want: []templates.ModelBenchRowView{{
				WorkerName: "host-a", DeviceSet: "gpu:0", State: templates.ModelBenchMeasured,
				UnitsPerSec: 42.5, GraphSecs: 0.031, BaseBytes: 5 << 30, PerSlotBytes: 128 << 20,
			}},
		},
		{
			name: "incapable row carries the reason and no numbers",
			rows: []store.ModelBenchmarkRow{{
				WorkerID: "w1", DeviceSet: "gpu:0", ModelID: benchModelKey, Error: "out of memory",
			}},
			names: map[string]string{"w1": "host-a"},
			want: []templates.ModelBenchRowView{{
				WorkerName: "host-a", DeviceSet: "gpu:0",
				State: templates.ModelBenchIncapable, Error: "out of memory",
			}},
		},
		{
			name:     "in-flight worker replaces its own stored row",
			rows:     []store.ModelBenchmarkRow{measuredRow("w1", "gpu:0"), measuredRow("w2", "gpu:0")},
			names:    map[string]string{"w1": "host-a", "w2": "host-b"},
			benching: []string{"w1"},
			want: []templates.ModelBenchRowView{
				{WorkerName: "host-a", State: templates.ModelBenchRunning},
				{
					WorkerName: "host-b", DeviceSet: "gpu:0", State: templates.ModelBenchMeasured,
					UnitsPerSec: 42.5, GraphSecs: 0.031, BaseBytes: 5 << 30, PerSlotBytes: 128 << 20,
				},
			},
		},
		{
			name:  "unknown worker falls back to its id",
			rows:  []store.ModelBenchmarkRow{measuredRow("w-gone", "cpu:0")},
			names: map[string]string{},
			want: []templates.ModelBenchRowView{{
				WorkerName: "w-gone", DeviceSet: "cpu:0", State: templates.ModelBenchMeasured,
				UnitsPerSec: 42.5, GraphSecs: 0.031, BaseBytes: 5 << 30, PerSlotBytes: 128 << 20,
			}},
		},
		{
			name:  "rows sort by worker then device set",
			rows:  []store.ModelBenchmarkRow{measuredRow("w2", "gpu:1"), measuredRow("w2", "gpu:0"), measuredRow("w1", "gpu:0")},
			names: map[string]string{"w1": "host-a", "w2": "host-b"},
			want: []templates.ModelBenchRowView{
				{WorkerName: "host-a", DeviceSet: "gpu:0"},
				{WorkerName: "host-b", DeviceSet: "gpu:0"},
				{WorkerName: "host-b", DeviceSet: "gpu:1"},
			},
		},
		{
			name: "nothing concluded and nothing running renders no rows",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildModelBenchRows(tt.rows, tt.names, tt.benching)
			require.Len(t, got, len(tt.want))
			for i, want := range tt.want {
				require.Equal(t, want.WorkerName, got[i].WorkerName)
				require.Equal(t, want.DeviceSet, got[i].DeviceSet)
				if want.State != "" {
					require.Equal(t, want.State, got[i].State)
					require.Equal(t, want.Error, got[i].Error)
					require.InDelta(t, want.UnitsPerSec, got[i].UnitsPerSec, 0.001)
					require.InDelta(t, want.GraphSecs, got[i].GraphSecs, 0.001)
					require.Equal(t, want.BaseBytes, got[i].BaseBytes)
					require.Equal(t, want.PerSlotBytes, got[i].PerSlotBytes)
				}
			}
		})
	}
}

// The view reads the store's verdicts and names the workers from the fleet.
// Its worker count is the fleet the model could run on — online workers of
// its own runtime, whether or not they have a verdict yet.
func TestModelBenchView_ReadsStoredVerdicts(t *testing.T) {
	h := newTestHandler(t)
	seedStreamWorker(t, h, "w1", "host-a",
		[]*workerpb.WorkerDevice{gpuDevice("gpu:0", "Test GPU", 8192)}, nil)
	seedStreamWorker(t, h, "w2", "host-b",
		[]*workerpb.WorkerDevice{gpuDevice("gpu:0", "Test GPU", 8192)}, nil)
	other := worker.NewStreamWorker("w3",
		&workerpb.WorkerRegister{Name: "host-c", RuntimeName: "other-runtime"},
		nil, "", "", true, zerolog.Nop())
	require.NoError(t, h.workers.Register(other))
	require.NoError(t, h.store.SaveModelBenchmark(measuredRow("w1", "gpu:0")))

	view := h.modelBenchView("llama-cpp", benchModelKey)
	require.Equal(t, "llama-cpp", view.RuntimeName)
	require.Equal(t, benchModelKey, view.ModelKey)
	require.Equal(t, 2, view.ConnectedWorkers)
	require.Len(t, view.Rows, 1)
	require.Equal(t, "host-a", view.Rows[0].WorkerName)
	require.Equal(t, templates.ModelBenchMeasured, view.Rows[0].State)

	html := templates.RenderModelBenchPanel(view)
	require.Contains(t, html, "42.5 units/s")
	require.Contains(t, html, "benched on 1/2 workers")
}

func TestHandleModelBenchStatus_UnresolvableModelRendersNothing(t *testing.T) {
	h := newTestHandler(t)
	tests := []struct {
		name  string
		query string
	}{
		{name: "no params", query: ""},
		{name: "no id", query: "?runtime=llama-cpp"},
		// No gateway is running in this handler, so the catalogue can't
		// resolve the id — the card stays away rather than showing rows
		// keyed by a guess.
		{name: "unknown model", query: "?runtime=llama-cpp&id=does-not-exist.gguf"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.handleModelBenchStatus(w, httptest.NewRequest(http.MethodGet, "/api/models/benchmarks"+tt.query, nil))
			require.Equal(t, http.StatusOK, w.Code)
			require.Empty(t, w.Body.String())
		})
	}
}

// Re-bench clears every verdict — the incapable ones included, since that
// is the only way back from one — and answers with the reset card.
func TestHandleModelRebench_ClearsVerdicts(t *testing.T) {
	h := newTestHandler(t)
	initSchedulerQueue(t, h)
	seedStreamWorker(t, h, "w1", "host-a",
		[]*workerpb.WorkerDevice{gpuDevice("gpu:0", "Test GPU", 8192)}, nil)
	require.NoError(t, h.store.SaveModelBenchmarkError(store.ModelBenchmarkRow{
		WorkerID: "w1", DeviceSet: "gpu:0", ModelID: benchModelKey, Error: "out of memory",
	}))

	w := httptest.NewRecorder()
	h.handleModelRebench(w, httptest.NewRequest(http.MethodPost,
		"/api/models/rebench?runtime=llama-cpp&key="+benchModelKey, nil))

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "model-bench-card")
	require.NotContains(t, w.Body.String(), "out of memory")

	rows, err := h.store.ListModelBenchmarksByModel(benchModelKey)
	require.NoError(t, err)
	require.Empty(t, rows, "re-bench wipes the incapable verdict")
}

func TestHandleModelRebench_RejectsIncompleteRequests(t *testing.T) {
	h := newTestHandler(t)
	initSchedulerQueue(t, h)
	tests := []struct {
		name  string
		query string
	}{
		{name: "no params", query: ""},
		{name: "no key", query: "?runtime=llama-cpp"},
		{name: "no runtime", query: "?key=" + benchModelKey},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.handleModelRebench(w, httptest.NewRequest(http.MethodPost, "/api/models/rebench"+tt.query, nil))
			require.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}
