package worker

import (
	"testing"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// stubSender collects sent messages for testing.
type stubSender struct {
	sent    []*workerpb.HubMessage
	handler func(*workerpb.HubMessage) *workerpb.WorkerJobResult
}

func (s *stubSender) Send(msg *workerpb.HubMessage) error {
	s.sent = append(s.sent, msg)
	// Simulate async: deliver result immediately if handler is set.
	if s.handler != nil {
		go func() {
			// Find the stream worker that owns this sender — test code delivers directly.
		}()
	}
	return nil
}

func newTestStreamWorker() *StreamWorker {
	sender := &stubSender{}
	return NewStreamWorker(&workerpb.WorkerRegister{
		Id:   "test-worker",
		Name: "Test Worker",
		Devices: []*workerpb.WorkerDevice{
			{Id: "cpu:0", Name: "Test CPU", Type: workerpb.WorkerDeviceType_WORKER_DEVICE_TYPE_CPU, TotalMemoryMb: 16384},
			{Id: "gpu:0", Name: "Test GPU", Type: workerpb.WorkerDeviceType_WORKER_DEVICE_TYPE_GPU, TotalMemoryMb: 8192},
		},
	}, sender, "http://mass:3455", "/models", false, zerolog.Nop())
}

func TestStreamWorker_BasicInfo(t *testing.T) {
	ag := newTestStreamWorker()
	require.Equal(t, "test-worker", ag.ID())
	require.Equal(t, "Test Worker", ag.Name())
	require.True(t, ag.Status().Online)
	require.Len(t, ag.Devices(), 2)
	require.Equal(t, "cpu:0", ag.Devices()[0].ID)
	require.Equal(t, "gpu:0", ag.Devices()[1].ID)
}

func TestStreamWorker_SendJob_And_Deliver(t *testing.T) {
	ag := newTestStreamWorker()

	// Start sendJob in a goroutine (it blocks).
	resultCh := make(chan *workerpb.WorkerJobResult, 1)
	errCh := make(chan error, 1)
	go func() {
		r, err := ag.sendJob(&workerpb.HubMessage{
			Msg: &workerpb.HubMessage_Benchmark{Benchmark: &workerpb.HubBenchmark{DeviceId: "cpu:0"}},
		})
		resultCh <- r
		errCh <- err
	}()

	// Wait for the job to be registered, then find its ID and deliver a result.
	var jobID string
	require.Eventually(t, func() bool {
		ag.pendingMu.Lock()
		defer ag.pendingMu.Unlock()
		for id := range ag.pending {
			jobID = id
			return true
		}
		return false
	}, 1e9, 1e6) // 1s timeout, 1ms poll

	ag.DeliverResult(&workerpb.WorkerJobResult{
		JobId: jobID,
		Result: &workerpb.WorkerJobResult_Benchmark{
			Benchmark: &workerpb.WorkerBenchmarkResponse{
				Results: []*workerpb.WorkerBenchmarkResult{
					{DeviceId: "cpu:0", DeviceName: "Test CPU", MemoryGbs: 50, ComputeGflops: 10},
				},
			},
		},
	})

	result := <-resultCh
	err := <-errCh
	require.NoError(t, err)
	require.NotNil(t, result.GetBenchmark())
	require.Equal(t, 50.0, result.GetBenchmark().Results[0].MemoryGbs)
}

func TestStreamWorker_SetOffline_ClosePending(t *testing.T) {
	ag := newTestStreamWorker()

	// Start a sendJob that will be cancelled by SetOffline.
	errCh := make(chan error, 1)
	go func() {
		_, err := ag.sendJob(&workerpb.HubMessage{
			Msg: &workerpb.HubMessage_Benchmark{Benchmark: &workerpb.HubBenchmark{}},
		})
		errCh <- err
	}()

	// Wait for pending job to exist.
	require.Eventually(t, func() bool {
		ag.pendingMu.Lock()
		defer ag.pendingMu.Unlock()
		return len(ag.pending) > 0
	}, 1e9, 1e6)

	ag.SetOffline()
	require.False(t, ag.Status().Online)

	err := <-errCh
	require.Error(t, err)
	require.Contains(t, err.Error(), "disconnected")
}

func TestStreamWorker_LoadChatModel_Via_SendJob(t *testing.T) {
	ag := newTestStreamWorker()

	// LoadChatModel sends a job and blocks. We deliver the result async.
	modelCh := make(chan llm.ChatModelInterface, 1)
	errCh := make(chan error, 1)
	go func() {
		m, err := ag.LoadChatModel(zerolog.Nop(), "test-model", llm.LlamaChatConfig{Path: "/test.gguf"}, llm.PlacementConfig{})
		modelCh <- m
		errCh <- err
	}()

	// Find and deliver the result.
	var jobID string
	require.Eventually(t, func() bool {
		ag.pendingMu.Lock()
		defer ag.pendingMu.Unlock()
		for id := range ag.pending {
			jobID = id
			return true
		}
		return false
	}, 1e9, 1e6)

	ag.DeliverResult(&workerpb.WorkerJobResult{
		JobId: jobID,
		Result: &workerpb.WorkerJobResult_LoadModel{
			LoadModel: &workerpb.WorkerLoadModelResult{Fingerprint: "fp-abc123"},
		},
	})

	model := <-modelCh
	err := <-errCh
	require.NoError(t, err)
	require.NotNil(t, model)
	require.Equal(t, "test-model", model.Pool().Name())
}

func TestFleet_StreamWorker_Lifecycle(t *testing.T) {
	reg := NewFleet()
	local := &fakeWorker{id: "local", name: "Local", online: true}
	require.NoError(t, reg.Register(local))

	ag := newTestStreamWorker()
	require.NoError(t, reg.Register(ag))
	require.Len(t, reg.All(), 2)

	// Deregister on disconnect.
	ag.SetOffline()
	require.NoError(t, reg.Deregister(ag.ID()))
	require.Len(t, reg.All(), 1)
}

func TestStreamWorker_DeliverResult_UnknownJob(t *testing.T) {
	ag := newTestStreamWorker()
	// Should not panic.
	ag.DeliverResult(&workerpb.WorkerJobResult{JobId: "nonexistent"})
}

func TestStreamWorker_SendJob_Error(t *testing.T) {
	ag := newTestStreamWorker()

	errCh := make(chan error, 1)
	go func() {
		_, err := ag.sendJob(&workerpb.HubMessage{
			Msg: &workerpb.HubMessage_ChatCompletion{ChatCompletion: &workerpb.HubChatCompletion{
				Fingerprint: "fp-test",
			}},
		})
		errCh <- err
	}()

	var jobID string
	require.Eventually(t, func() bool {
		ag.pendingMu.Lock()
		defer ag.pendingMu.Unlock()
		for id := range ag.pending {
			jobID = id
			return true
		}
		return false
	}, 1e9, 1e6)

	ag.DeliverResult(&workerpb.WorkerJobResult{
		JobId: jobID,
		Result: &workerpb.WorkerJobResult_Error{
			Error: &workerpb.WorkerJobError{Message: "model not loaded"},
		},
	})

	err := <-errCh
	require.Error(t, err)
	require.Contains(t, err.Error(), "model not loaded")
}

func TestRelativeModelPath(t *testing.T) {
	tests := []struct {
		name      string
		absPath   string
		modelsDir string
		want      string
	}{
		{"relative from models dir", "/data/models/publisher/repo/model.gguf", "/data/models", "publisher/repo/model.gguf"},
		{"empty models dir", "/data/models/model.gguf", "", "/data/models/model.gguf"},
		{"already relative", "publisher/repo/model.gguf", "/data/models", "publisher/repo/model.gguf"},
		{"outside models dir", "/other/path/model.gguf", "/data/models", "/other/path/model.gguf"},
		{"empty path", "", "/data/models", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := relativeModelPath(tt.absPath, tt.modelsDir)
			require.Equal(t, tt.want, got)
		})
	}
}
