package web

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/stretchr/testify/require"
)

func TestBuildQueueSectionViews(t *testing.T) {
	sections := []scheduler.QueueSection{
		{
			Name:         "global",
			DepthSeconds: 1.5,
			Rows: []scheduler.QueueRow{
				{MsgID: "m1", ModelID: "a.gguf", Inflight: false},
				{MsgID: "m2", ModelID: "b.gguf", Inflight: false},
			},
		},
		{
			Name:         "worker|llama-host",
			WorkerID:     "llama-host",
			DepthSeconds: 4.0,
			Rows: []scheduler.QueueRow{
				{MsgID: "m3", ModelID: "c.gguf", Inflight: true},  // running
				{MsgID: "m4", ModelID: "d.gguf", Inflight: false}, // pending
			},
		},
	}

	views := buildQueueSectionViews(sections)
	require.Len(t, views, 2)

	global := views[0]
	require.Equal(t, "Global queue", global.Title, "global gets a friendly title")
	require.Equal(t, 2, global.RowCount)
	require.Equal(t, 2, global.PendingCount)
	require.Equal(t, 0, global.RunningCount)

	wq := views[1]
	require.Equal(t, "llama-host", wq.Title, "worker queue titled by worker id")
	require.Equal(t, "llama-host", wq.WorkerID)
	require.Equal(t, 2, wq.RowCount)
	require.Equal(t, 1, wq.PendingCount)
	require.Equal(t, 1, wq.RunningCount)
	require.InDelta(t, 4.0, wq.DepthSeconds, 1e-9)

	// Per-row Inflight maps to Running on the view.
	running := map[string]bool{}
	for _, r := range wq.Rows {
		running[r.MsgID] = r.Running
	}
	require.True(t, running["m3"])
	require.False(t, running["m4"])
}

func TestBuildQueueSectionViews_Empty(t *testing.T) {
	require.Empty(t, buildQueueSectionViews(nil))
}

// A running benchmark is rendered as its own exclusive unit inside the
// worker's section; an idle fleet renders none of it.
func TestQueueSections_RenderRunningBench(t *testing.T) {
	section := scheduler.QueueSection{
		Name:     "worker|llama-host",
		WorkerID: "llama-host",
		Rows:     []scheduler.QueueRow{{MsgID: "m1", ModelID: "a.gguf", Inflight: true}},
	}
	benching := section
	benching.Bench = &scheduler.RunningBench{ModelKey: "gguf/qwen/qwen3.gguf", RuntimeName: "llama-cpp"}

	tests := []struct {
		name        string
		section     scheduler.QueueSection
		wantBench   bool
		wantContain []string
		wantAbsent  []string
	}{
		{
			name:       "idle worker shows no bench unit",
			section:    section,
			wantAbsent: []string{"benchmark", "exclusive", "speedometer2"},
		},
		{
			name:      "benching worker shows the model, the worker and its exclusivity",
			section:   benching,
			wantBench: true,
			wantContain: []string{
				"gguf/qwen/qwen3.gguf", "llama-host", "speedometer2",
				">benchmark<", "exclusive", "· benchmarking",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			views := buildQueueSectionViews([]scheduler.QueueSection{tt.section})
			require.Len(t, views, 1)
			if tt.wantBench {
				require.NotNil(t, views[0].Bench)
				require.Equal(t, "llama-host", views[0].Bench.WorkerID)
			} else {
				require.Nil(t, views[0].Bench)
			}

			var buf bytes.Buffer
			require.NoError(t, templates.QueueSections(views).Render(t.Context(), &buf))
			for _, want := range tt.wantContain {
				require.Contains(t, buf.String(), want)
			}
			for _, absent := range tt.wantAbsent {
				require.NotContains(t, buf.String(), absent)
			}
		})
	}
}

func TestHandleQueueCancel_MissingParamsIs400(t *testing.T) {
	h := newTestHandler(t) // bare scheduler wired → reaches the param check
	tests := []struct {
		name  string
		query string
	}{
		{"both missing", ""},
		{"missing msgID", "?queue=global"},
		{"missing queue", "?msgID=m1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/api/queue/cancel"+tt.query, nil)
			w := httptest.NewRecorder()
			h.handleQueueCancel(w, r)
			require.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestHandleQueueCancelRunning_MissingParamIs400(t *testing.T) {
	h := newTestHandler(t)
	r := httptest.NewRequest(http.MethodPost, "/api/queue/cancel-running", nil)
	w := httptest.NewRecorder()
	h.handleQueueCancelRunning(w, r)
	require.Equal(t, http.StatusBadRequest, w.Code)
}
