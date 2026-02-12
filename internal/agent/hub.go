package agent

import (
	"context"
	"fmt"
	"io"

	"github.com/KernelPryanic/ctxerr"

	"connectrpc.com/connect"
	"github.com/chinese-room-solutions/mass/pkg/bench"
	agentpb "github.com/chinese-room-solutions/mass/rpc/agent"
	"github.com/chinese-room-solutions/mass/rpc/agent/agentconnect"
	"github.com/rs/zerolog"
)

// Compile-time check.
var _ agentconnect.AgentHubHandler = (*Hub)(nil)

// Hub implements the AgentHub ConnectRPC service on the MASS side.
// Agents connect as clients; the hub manages their lifecycle.
type Hub struct {
	agentconnect.UnimplementedAgentHubHandler

	registry  *Registry
	massURL   string
	modelsDir string
	logger    zerolog.Logger
}

// NewHub creates a new AgentHub service.
func NewHub(registry *Registry, massURL, modelsDir string, logger zerolog.Logger) *Hub {
	return &Hub{
		registry:  registry,
		massURL:   massURL,
		modelsDir: modelsDir,
		logger:    logger.With().Str("component", "agent_hub").Logger(),
	}
}

// Connect handles a bidirectional stream from an agent.
func (h *Hub) Connect(ctx context.Context, stream *connect.BidiStream[agentpb.AgentMessage, agentpb.HubMessage]) error {
	// First message must be Register.
	firstMsg, err := stream.Receive()
	if err != nil {
		return fmt.Errorf("reading registration: %w", err)
	}
	reg := firstMsg.GetRegister()
	if reg == nil {
		return fmt.Errorf("first message must be Register, got %T", firstMsg.Msg)
	}

	// Create a cancellable context so the stream can be killed from the UI.
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	// Create the stream agent.
	sender := &bidiSender{stream: stream}
	agent := NewStreamAgent(reg, sender, h.massURL, h.modelsDir, h.logger)
	agent.SetCancelFn(streamCancel)

	// Register in the registry.
	if err := h.registry.Register(agent); err != nil {
		return ctxerr.With(fmt.Errorf("registering agent %s: %w", reg.Id, err), map[string]any{"agent_id": reg.Id, "agent_name": reg.Name})
	}
	h.logger.Info().Str("agent", reg.Id).Str("name", reg.Name).Int("devices", len(reg.Devices)).Msg("agent connected")

	// Ensure cleanup on disconnect.
	defer func() {
		agent.SetOffline()
		_ = h.registry.Deregister(agent.ID())
		h.logger.Info().Str("agent", reg.Id).Msg("agent disconnected")
	}()

	_ = streamCtx // used by cancel propagation

	// Receive loop: route job results and heartbeats.
	for {
		msg, err := stream.Receive()
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return nil
			}
			return ctxerr.With(fmt.Errorf("receiving from agent %s: %w", reg.Id, err), map[string]any{"agent_id": reg.Id})
		}

		switch m := msg.Msg.(type) {
		case *agentpb.AgentMessage_JobResult:
			agent.DeliverResult(m.JobResult)
		case *agentpb.AgentMessage_Heartbeat:
			hb := m.Heartbeat
			agent.mu.Lock()
			agent.online = true
			if len(hb.DeviceStats) > 0 {
				stats := make([]bench.DeviceStats, len(hb.DeviceStats))
				for i, ds := range hb.DeviceStats {
					stats[i] = bench.DeviceStats{
						DeviceID:       ds.DeviceId,
						UsedMemoryMB:   int(ds.UsedMemoryMb),
						TotalMemoryMB:  int(ds.TotalMemoryMb),
						UtilizationPct: ds.UtilizationPct,
					}
				}
				agent.deviceStats = stats
			}
			agent.mu.Unlock()
			h.registry.NotifyUpdate(agent.ID())
		case *agentpb.AgentMessage_Register:
			h.logger.Warn().Str("agent", reg.Id).Msg("duplicate Register message ignored")
		}
	}
}

// bidiSender wraps a BidiStream to implement the jobSender interface.
type bidiSender struct {
	stream *connect.BidiStream[agentpb.AgentMessage, agentpb.HubMessage]
}

func (s *bidiSender) Send(msg *agentpb.HubMessage) error {
	return s.stream.Send(msg)
}
