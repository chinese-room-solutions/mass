package web

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// seedStreamWorker builds a real *worker.StreamWorker (the concrete type both
// view builders require — schedulerInstances type-asserts to it), applies a
// heartbeat so it carries device stats + loaded models, and registers it in
// the handler's fleet. Returns the worker for further assertions.
func seedStreamWorker(t *testing.T, h *Handler, id, name string, devices []*workerpb.WorkerDevice, loaded []*workerpb.LoadedModelStatus) *worker.StreamWorker {
	t.Helper()

	reg := &workerpb.WorkerRegister{
		Name:        name,
		RuntimeName: "llama-cpp",
		Devices:     devices,
	}
	sw := worker.NewStreamWorker(id, reg, nil, "", "", true, zerolog.Nop())

	dstats := make([]*workerpb.WorkerDeviceStats, len(devices))
	for i, d := range devices {
		dstats[i] = &workerpb.WorkerDeviceStats{
			DeviceId:      d.Id,
			UsedMemoryMb:  1024,
			TotalMemoryMb: d.TotalMemoryMb,
		}
	}
	sw.ApplyHeartbeat(&workerpb.WorkerHeartbeat{
		DeviceStats:       dstats,
		LoadedModels:      loaded,
		ActiveJobs:        1,
		AvailableCapacity: 3,
	})

	require.NoError(t, h.workers.Register(sw))
	return sw
}

func gpuDevice(id, name string, memMB int32) *workerpb.WorkerDevice {
	return &workerpb.WorkerDevice{
		Id:            id,
		Name:          name,
		Type:          workerpb.WorkerDeviceType_WORKER_DEVICE_TYPE_GPU,
		TotalMemoryMb: memMB,
	}
}

func TestBuildWorkerViews_RendersDevicesAndBench(t *testing.T) {
	h := newTestHandler(t)
	devices := []*workerpb.WorkerDevice{
		gpuDevice("gpu:0", "NVIDIA RTX 4090", 24576),
		gpuDevice("gpu:1", "NVIDIA RTX 4090", 24576),
	}
	seedStreamWorker(t, h, "llama-host-a", "host-a", devices, nil)

	// Seed a benchmark row for one device so the bench branch is exercised.
	require.NoError(t, h.store.SaveBenchmark(store.BenchmarkRow{
		WorkerID:   "llama-host-a",
		DeviceID:   "gpu:0",
		DeviceName: "NVIDIA RTX 4090",
		MemoryGBs:  900.0,
		LoadGBs:    50.0,
		Flops:      1234.5,
		BenchedAt:  time.Unix(1_700_000_000, 0),
	}))

	views := h.buildWorkerViews()
	require.Len(t, views, 1)
	v := views[0]
	require.Equal(t, "llama-host-a", v.ID)
	require.Equal(t, "host-a", v.Name)
	require.Equal(t, "llama-cpp", v.RuntimeName)
	require.True(t, v.Online)
	require.True(t, v.Enabled, "no disabled rows → derived-enabled")
	require.Len(t, v.Devices, 2)
	require.Equal(t, 1, v.ActiveJobs)

	// The benched device carries its compute number; the other doesn't.
	byID := map[string]bool{}
	for _, d := range v.Devices {
		byID[d.DeviceID] = d.HasBenchmark
	}
	require.True(t, byID["gpu:0"], "gpu:0 has a bench row")
	require.False(t, byID["gpu:1"], "gpu:1 has none")
}

func TestBuildWorkerViews_DisabledDeviceFlipsEnabled(t *testing.T) {
	h := newTestHandler(t)
	devices := []*workerpb.WorkerDevice{gpuDevice("gpu:0", "GPU0", 8192)}
	seedStreamWorker(t, h, "w-solo", "solo", devices, nil)

	// Disable the only device → worker derives to not-enabled.
	require.NoError(t, h.store.SetWorkerDeviceEnabled("w-solo", "gpu:0", false))

	views := h.buildWorkerViews()
	require.Len(t, views, 1)
	require.False(t, views[0].Enabled, "all devices disabled → worker disabled")
	require.False(t, views[0].Devices[0].Enabled)
}

func TestHandleWorkersList_RendersWorker(t *testing.T) {
	h := newTestHandler(t)
	seedStreamWorker(t, h, "llama-render-host", "render-host",
		[]*workerpb.WorkerDevice{gpuDevice("gpu:0", "Test GPU", 8192)}, nil)

	r := httptest.NewRequest(http.MethodGet, "/api/workers/list", nil)
	w := httptest.NewRecorder()
	h.handleWorkersList(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Header().Get("Content-Type"), "text/html")
	body := w.Body.String()
	require.Contains(t, body, "render-host")
	require.Contains(t, body, "Test GPU")
}

func TestSchedulerInstances_OnePerLoadedModel(t *testing.T) {
	h := newTestHandler(t)
	loaded := []*workerpb.LoadedModelStatus{
		{ModelId: "qwen3.gguf#abc123", PoolSize: 2, Active: 1, DeviceIds: []string{"gpu:0"}},
		{ModelId: "llama.gguf#def456", PoolSize: 1, Active: 0, DeviceIds: []string{"gpu:1"}},
	}
	seedStreamWorker(t, h, "llama-sched-host", "sched-host",
		[]*workerpb.WorkerDevice{gpuDevice("gpu:0", "G0", 8192), gpuDevice("gpu:1", "G1", 8192)}, loaded)

	insts := h.schedulerInstances()
	require.Len(t, insts, 2, "one instance per loaded model")

	byModel := map[string]bool{}
	for _, in := range insts {
		byModel[in.ModelID] = true
		require.Equal(t, "llama-sched-host", in.WorkerID)
		require.Equal(t, "llama-cpp", in.RuntimeName)
	}
	require.True(t, byModel["qwen3.gguf#abc123"])
	require.True(t, byModel["llama.gguf#def456"])

	// Status derives from Active: model 1 (Active=1) is Active, model 2 Idle.
	for _, in := range insts {
		if in.ModelID == "qwen3.gguf#abc123" {
			require.Equal(t, "Active", in.Status)
			require.Equal(t, []string{"gpu:0"}, in.DeviceIDs)
		} else {
			require.Equal(t, "Idle", in.Status)
		}
	}
}

func TestSchedulerInstances_SplitsModelIDFingerprint(t *testing.T) {
	h := newTestHandler(t)
	loaded := []*workerpb.LoadedModelStatus{
		{ModelId: "model.gguf#deadbeef", PoolSize: 1, Active: 0, DeviceIds: []string{"gpu:0"}},
	}
	seedStreamWorker(t, h, "w-fp", "fp",
		[]*workerpb.WorkerDevice{gpuDevice("gpu:0", "G0", 8192)}, loaded)

	insts := h.schedulerInstances()
	require.Len(t, insts, 1)
	require.Equal(t, "model.gguf", insts[0].Filename)
	require.Equal(t, "deadbeef", insts[0].Fingerprint)
}

func TestHandleSchedulerList_RendersInstances(t *testing.T) {
	h := newTestHandler(t)
	loaded := []*workerpb.LoadedModelStatus{
		{ModelId: "shown-model.gguf#aa11", PoolSize: 1, Active: 1, DeviceIds: []string{"gpu:0"}},
	}
	seedStreamWorker(t, h, "w-list", "list-host",
		[]*workerpb.WorkerDevice{gpuDevice("gpu:0", "G0", 8192)}, loaded)

	r := httptest.NewRequest(http.MethodGet, "/api/scheduler/list", nil)
	w := httptest.NewRecorder()
	h.handleSchedulerList(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "shown-model.gguf")
}

func TestInstanceDeviceIDs_PrefersReportedSet(t *testing.T) {
	h := newTestHandler(t)
	sw := seedStreamWorker(t, h, "w-dev", "dev",
		[]*workerpb.WorkerDevice{gpuDevice("gpu:0", "G0", 8192), gpuDevice("gpu:1", "G1", 8192)}, nil)

	// When the loaded model reports a device set, it wins verbatim.
	got := h.instanceDeviceIDs(sw, worker.LoadedModelStatus{
		ModelID:   "m",
		DeviceIDs: []string{"gpu:1"},
	})
	require.Equal(t, []string{"gpu:1"}, got)

	// When it doesn't, fall back to every enabled GPU.
	got = h.instanceDeviceIDs(sw, worker.LoadedModelStatus{ModelID: "m"})
	require.ElementsMatch(t, []string{"gpu:0", "gpu:1"}, got)
}
