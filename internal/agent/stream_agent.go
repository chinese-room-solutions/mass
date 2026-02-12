package agent

import (
	"fmt"
	"sync"
	"time"

	"github.com/KernelPryanic/ctxerr"
	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/chinese-room-solutions/mass/pkg/llm"
	agentpb "github.com/chinese-room-solutions/mass/rpc/agent"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// Compile-time check.
var _ AgentInterface = (*StreamAgent)(nil)

// jobSender is the interface for sending messages to the agent via the bidi stream.
// Abstracted so we can test without a real stream.
type jobSender interface {
	Send(msg *agentpb.HubMessage) error
}

// StreamAgent implements AgentInterface for remote agents connected via
// a bidirectional stream. MASS pushes jobs, agent pushes results.
type StreamAgent struct {
	mu        sync.RWMutex
	id        string
	name      string
	devices   []bench.Device
	online    bool
	lastSeen  time.Time
	massURL   string
	modelsDir string

	sendMu sync.Mutex
	sender jobSender

	pendingMu sync.Mutex
	pending   map[string]chan *agentpb.AgentJobResult

	deviceStats []bench.DeviceStats // latest stats from heartbeat

	cancelFn func() // cancels the hub's stream context
	logger   zerolog.Logger
}

// NewStreamAgent creates a stream agent from registration info.
func NewStreamAgent(reg *agentpb.AgentRegister, sender jobSender, massURL, modelsDir string, logger zerolog.Logger) *StreamAgent {
	devices := make([]bench.Device, len(reg.Devices))
	for i, d := range reg.Devices {
		devices[i] = bench.Device{
			ID:            d.Id,
			Name:          d.Name,
			Type:          d.Type,
			TotalMemoryMB: int(d.TotalMemoryMb),
		}
	}
	return &StreamAgent{
		id:        reg.Id,
		name:      reg.Name,
		devices:   devices,
		online:    true,
		lastSeen:  time.Now(),
		massURL:   massURL,
		modelsDir: modelsDir,
		sender:    sender,
		pending:   make(map[string]chan *agentpb.AgentJobResult),
		logger:    logger.With().Str("agent", reg.Id).Logger(),
	}
}

func (a *StreamAgent) ID() string   { return a.id }
func (a *StreamAgent) Name() string { return a.name }

func (a *StreamAgent) Status() AgentStatus {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return AgentStatus{Online: a.online, LastSeen: a.lastSeen}
}

func (a *StreamAgent) Stats() []bench.DeviceStats {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]bench.DeviceStats, len(a.deviceStats))
	copy(out, a.deviceStats)
	return out
}

func (a *StreamAgent) Devices() []bench.Device {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]bench.Device, len(a.devices))
	copy(out, a.devices)
	return out
}

// sendJob sends a job to the agent and blocks until the result arrives.
func (a *StreamAgent) sendJob(msg *agentpb.HubMessage) (*agentpb.AgentJobResult, error) {
	jobID := uuid.NewString()
	msg.JobId = jobID

	ch := make(chan *agentpb.AgentJobResult, 1)
	a.pendingMu.Lock()
	a.pending[jobID] = ch
	a.pendingMu.Unlock()

	defer func() {
		a.pendingMu.Lock()
		delete(a.pending, jobID)
		a.pendingMu.Unlock()
	}()

	a.sendMu.Lock()
	err := a.sender.Send(msg)
	a.sendMu.Unlock()
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("sending job to agent %s: %w", a.id, err), map[string]any{"agent_id": a.id, "job_id": jobID})
	}

	result, ok := <-ch
	if !ok {
		return nil, ctxerr.With(fmt.Errorf("agent %s disconnected while waiting for job %s", a.id, jobID), map[string]any{"agent_id": a.id, "job_id": jobID})
	}
	if errMsg := result.GetError(); errMsg != nil {
		return nil, ctxerr.With(fmt.Errorf("agent %s job error: %s", a.id, errMsg.Message), map[string]any{"agent_id": a.id, "job_id": jobID})
	}
	return result, nil
}

// DeliverResult routes a job result to the waiting caller.
func (a *StreamAgent) DeliverResult(result *agentpb.AgentJobResult) {
	a.pendingMu.Lock()
	ch, ok := a.pending[result.JobId]
	a.pendingMu.Unlock()
	if ok {
		ch <- result
	} else {
		a.logger.Warn().Str("job_id", result.JobId).Msg("received result for unknown job")
	}
}

// SetCancelFn sets the function to call to cancel the hub's stream context.
func (a *StreamAgent) SetCancelFn(fn func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cancelFn = fn
}

// SetOffline marks the agent as offline, cancels the stream, and closes all pending job channels.
func (a *StreamAgent) SetOffline() {
	a.mu.Lock()
	a.online = false
	cancel := a.cancelFn
	a.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	a.pendingMu.Lock()
	for id, ch := range a.pending {
		close(ch)
		delete(a.pending, id)
	}
	a.pendingMu.Unlock()
}

// Disconnect cancels the stream and marks offline. Called when kicked from UI.
func (a *StreamAgent) Disconnect() {
	a.SetOffline()
}

// --- AgentInterface: BencherInterface ---

func (a *StreamAgent) Bench(deviceID string) (bench.Result, error) {
	result, err := a.sendJob(&agentpb.HubMessage{
		Msg: &agentpb.HubMessage_Benchmark{Benchmark: &agentpb.HubBenchmark{DeviceId: deviceID}},
	})
	if err != nil {
		return bench.Result{}, err
	}
	br := result.GetBenchmark()
	if br == nil || len(br.Results) == 0 {
		return bench.Result{}, fmt.Errorf("no benchmark results returned")
	}
	r := br.Results[0]
	return bench.Result{
		DeviceID:      r.DeviceId,
		DeviceName:    r.DeviceName,
		MemoryGBs:     r.MemoryGbs,
		ComputeGFlops: r.ComputeGflops,
		BenchedAt:     time.Now(),
	}, nil
}

// --- AgentInterface: ModelLoaderInterface ---

func (a *StreamAgent) LoadChatModel(logger zerolog.Logger, name string, cfg llm.ChatModelConfig, placement llm.PlacementConfig) (llm.ChatModelInterface, error) {
	// Convert absolute path to relative model ID for the remote agent.
	// The agent resolves it against its own models directory.
	remoteCfg := cfg
	remoteCfg.Path = toRelativeModelPath(cfg.Path, a.modelsDir)
	adaptChatConfigForPlacement(&remoteCfg, placement)
	result, err := a.sendJob(&agentpb.HubMessage{
		Msg: &agentpb.HubMessage_LoadChatModel{LoadChatModel: &agentpb.HubLoadChatModel{
			Config:  chatConfigToProto(remoteCfg, placement),
			MassUrl: a.massURL,
		}},
	})
	if err != nil {
		return nil, err
	}
	fp := result.GetLoadModel().GetFingerprint()
	return newRemoteChatModel(fp, name, a), nil
}

func (a *StreamAgent) LoadEmbeddingModel(logger zerolog.Logger, name string, cfg llm.EmbeddingModelConfig, placement llm.PlacementConfig) (llm.EmbeddingModelInterface, error) {
	remoteCfg := cfg
	remoteCfg.Path = toRelativeModelPath(cfg.Path, a.modelsDir)
	result, err := a.sendJob(&agentpb.HubMessage{
		Msg: &agentpb.HubMessage_LoadEmbeddingModel{LoadEmbeddingModel: &agentpb.HubLoadEmbeddingModel{
			Config:  embeddingConfigToProto(remoteCfg, placement),
			MassUrl: a.massURL,
		}},
	})
	if err != nil {
		return nil, err
	}
	fp := result.GetLoadModel().GetFingerprint()
	return newRemoteEmbeddingModel(fp, name, a), nil
}
