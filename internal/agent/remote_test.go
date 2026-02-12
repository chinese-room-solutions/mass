package agent

import (
	"testing"

	"github.com/chinese-room-solutions/mass/pkg/llm"
	agentpb "github.com/chinese-room-solutions/mass/rpc/agent"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// stubSender collects sent messages for testing.
type stubSender struct {
	sent    []*agentpb.HubMessage
	handler func(*agentpb.HubMessage) *agentpb.AgentJobResult
}

func (s *stubSender) Send(msg *agentpb.HubMessage) error {
	s.sent = append(s.sent, msg)
	// Simulate async: deliver result immediately if handler is set.
	if s.handler != nil {
		go func() {
			// Find the stream agent that owns this sender — test code delivers directly.
		}()
	}
	return nil
}

func newTestStreamAgent() *StreamAgent {
	sender := &stubSender{}
	return NewStreamAgent(&agentpb.AgentRegister{
		Id:   "test-agent",
		Name: "Test Agent",
		Devices: []*agentpb.AgentDevice{
			{Id: "cpu:0", Name: "Test CPU", Type: "CPU", TotalMemoryMb: 16384},
			{Id: "gpu:0", Name: "Test GPU", Type: "GPU", TotalMemoryMb: 8192},
		},
	}, sender, "http://mass:3455", "/models", zerolog.Nop())
}

func TestStreamAgent_BasicInfo(t *testing.T) {
	ag := newTestStreamAgent()
	require.Equal(t, "test-agent", ag.ID())
	require.Equal(t, "Test Agent", ag.Name())
	require.True(t, ag.Status().Online)
	require.Len(t, ag.Devices(), 2)
	require.Equal(t, "cpu:0", ag.Devices()[0].ID)
	require.Equal(t, "gpu:0", ag.Devices()[1].ID)
}

func TestStreamAgent_SendJob_And_Deliver(t *testing.T) {
	ag := newTestStreamAgent()

	// Start sendJob in a goroutine (it blocks).
	resultCh := make(chan *agentpb.AgentJobResult, 1)
	errCh := make(chan error, 1)
	go func() {
		r, err := ag.sendJob(&agentpb.HubMessage{
			Msg: &agentpb.HubMessage_Benchmark{Benchmark: &agentpb.HubBenchmark{DeviceId: "cpu:0"}},
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

	ag.DeliverResult(&agentpb.AgentJobResult{
		JobId: jobID,
		Result: &agentpb.AgentJobResult_Benchmark{
			Benchmark: &agentpb.AgentBenchmarkResponse{
				Results: []*agentpb.AgentBenchmarkResult{
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

func TestStreamAgent_SetOffline_ClosePending(t *testing.T) {
	ag := newTestStreamAgent()

	// Start a sendJob that will be cancelled by SetOffline.
	errCh := make(chan error, 1)
	go func() {
		_, err := ag.sendJob(&agentpb.HubMessage{
			Msg: &agentpb.HubMessage_Benchmark{Benchmark: &agentpb.HubBenchmark{}},
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

func TestStreamAgent_LoadChatModel_Via_SendJob(t *testing.T) {
	ag := newTestStreamAgent()

	// LoadChatModel sends a job and blocks. We deliver the result async.
	modelCh := make(chan llm.ChatModelInterface, 1)
	errCh := make(chan error, 1)
	go func() {
		m, err := ag.LoadChatModel(zerolog.Nop(), "test-model", llm.ChatModelConfig{Path: "/test.gguf"}, llm.PlacementConfig{})
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

	ag.DeliverResult(&agentpb.AgentJobResult{
		JobId: jobID,
		Result: &agentpb.AgentJobResult_LoadModel{
			LoadModel: &agentpb.AgentLoadModelResult{Fingerprint: "fp-abc123"},
		},
	})

	model := <-modelCh
	err := <-errCh
	require.NoError(t, err)
	require.NotNil(t, model)
	require.Equal(t, "test-model", model.Pool().Name())
}

func TestRegistry_StreamAgent_Lifecycle(t *testing.T) {
	reg := NewRegistry()
	local := &fakeAgent{id: "local", name: "Local", online: true}
	require.NoError(t, reg.Register(local))

	ag := newTestStreamAgent()
	require.NoError(t, reg.Register(ag))
	require.Len(t, reg.All(), 2)

	// Deregister on disconnect.
	ag.SetOffline()
	require.NoError(t, reg.Deregister(ag.ID()))
	require.Len(t, reg.All(), 1)
}

func TestStreamAgent_DeliverResult_UnknownJob(t *testing.T) {
	ag := newTestStreamAgent()
	// Should not panic.
	ag.DeliverResult(&agentpb.AgentJobResult{JobId: "nonexistent"})
}

func TestStreamAgent_SendJob_Error(t *testing.T) {
	ag := newTestStreamAgent()

	errCh := make(chan error, 1)
	go func() {
		_, err := ag.sendJob(&agentpb.HubMessage{
			Msg: &agentpb.HubMessage_ChatCompletion{ChatCompletion: &agentpb.HubChatCompletion{
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

	ag.DeliverResult(&agentpb.AgentJobResult{
		JobId: jobID,
		Result: &agentpb.AgentJobResult_Error{
			Error: &agentpb.AgentJobError{Message: "model not loaded"},
		},
	})

	err := <-errCh
	require.Error(t, err)
	require.Contains(t, err.Error(), "model not loaded")
}

func TestToRelativeModelPath(t *testing.T) {
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
			got := toRelativeModelPath(tt.absPath, tt.modelsDir)
			require.Equal(t, tt.want, got)
		})
	}
}
