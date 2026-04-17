package worker

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/KernelPryanic/ctxerr"

	"connectrpc.com/connect"
	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/chinese-room-solutions/mass-proto/gen/go/worker/workerconnect"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/rs/zerolog"
)

// Compile-time check.
var _ workerconnect.WorkerHubHandler = (*Hub)(nil)

// CanonicalSetFn returns the set of model file relpaths MASS still considers
// live. Anything a worker reports outside this set is reaped on the next
// reconcile. Computed by MASS on demand — typically a directory walk of
// `Config.modelsDir`.
type CanonicalSetFn func() map[string]struct{}

// Hub implements the WorkerHub ConnectRPC service on the MASS side.
// Workers connect as clients; the hub manages their lifecycle.
type Hub struct {
	workerconnect.UnimplementedWorkerHubHandler

	fleet     *Fleet
	massURL   string
	modelsDir string
	canonical CanonicalSetFn
	logger    zerolog.Logger
}

// NewHub creates a new WorkerHub service. canonical may be nil during early
// init; the hub then skips cache reconciliation until [Hub.SetCanonicalFn]
// is called.
func NewHub(fleet *Fleet, massURL, modelsDir string, canonical CanonicalSetFn, logger zerolog.Logger) *Hub {
	return &Hub{
		fleet:     fleet,
		massURL:   massURL,
		modelsDir: modelsDir,
		canonical: canonical,
		logger:    logger.With().Str("component", "worker_hub").Logger(),
	}
}

// SetCanonicalFn wires the canonical-set provider after construction. Used
// when the scheduler that knows the models dir is built after the hub.
func (h *Hub) SetCanonicalFn(fn CanonicalSetFn) {
	h.canonical = fn
}

// Connect handles a bidirectional stream from a worker.
func (h *Hub) Connect(ctx context.Context, stream *connect.BidiStream[workerpb.WorkerMessage, workerpb.HubMessage]) error {
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

	// Create the stream worker.
	sender := &bidiSender{stream: stream}
	loopback := isLoopbackPeer(stream.Peer().Addr)
	worker := NewStreamWorker(reg, sender, h.massURL, h.modelsDir, loopback, h.logger)
	worker.SetCancelFn(streamCancel)

	// Add the worker to the fleet.
	if err := h.fleet.Register(worker); err != nil {
		return ctxerr.With(fmt.Errorf("registering worker %s: %w", reg.Id, err), map[string]any{"worker_id": reg.Id, "worker_name": reg.Name})
	}
	h.logger.Info().Str("worker", reg.Id).Str("name", reg.Name).Int("devices", len(reg.Devices)).Msg("worker connected")

	// Ensure cleanup on disconnect.
	defer func() {
		worker.SetOffline()
		if err := h.fleet.Deregister(worker.ID()); err != nil {
			h.logger.Warn().Err(err).Str("worker", worker.ID()).Msg("deregistering worker on disconnect")
		}
		h.logger.Info().Str("worker", reg.Id).Msg("worker disconnected")
	}()

	_ = streamCtx // used by cancel propagation

	// Receive loop: route job results and heartbeats.
	for {
		msg, err := stream.Receive()
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return nil
			}
			return ctxerr.With(fmt.Errorf("receiving from worker %s: %w", reg.Id, err), map[string]any{"worker_id": reg.Id})
		}

		switch m := msg.Msg.(type) {
		case *workerpb.WorkerMessage_JobResult:
			worker.DeliverResult(m.JobResult)
		case *workerpb.WorkerMessage_Heartbeat:
			hb := m.Heartbeat
			worker.mu.Lock()
			worker.online = true
			if len(hb.DeviceStats) > 0 {
				dstats := make([]stats.DeviceStats, len(hb.DeviceStats))
				for i, ds := range hb.DeviceStats {
					dstats[i] = stats.DeviceStats{
						DeviceID:       ds.DeviceId,
						UsedMemoryMB:   int(ds.UsedMemoryMb),
						TotalMemoryMB:  int(ds.TotalMemoryMb),
						UtilizationPct: ds.UtilizationPct,
					}
				}
				worker.deviceStats = dstats
			}
			// cache_files is authoritative for the worker — replace, never merge.
			worker.cacheFiles = append(worker.cacheFiles[:0:0], hb.CacheFiles...)
			worker.mu.Unlock()
			h.fleet.NotifyUpdate(worker.ID())

			// Reconcile the worker's cache against MASS's canonical set on
			// every heartbeat. Cheap (map lookups), and gets a freshly
			// reconnected worker caught up within one heartbeat (~10s).
			if h.canonical != nil && len(hb.CacheFiles) > 0 {
				if _, err := worker.Reconcile(h.canonical()); err != nil {
					h.logger.Warn().Err(err).Str("worker", worker.ID()).Msg("cache reconcile failed")
				}
			}
		case *workerpb.WorkerMessage_Register:
			h.logger.Warn().Str("worker", reg.Id).Msg("duplicate Register message ignored")
		}
	}
}

// bidiSender wraps a BidiStream to implement the jobSenderInterface.
type bidiSender struct {
	stream *connect.BidiStream[workerpb.WorkerMessage, workerpb.HubMessage]
}

func (s *bidiSender) Send(msg *workerpb.HubMessage) error {
	return s.stream.Send(msg)
}

// isLoopbackPeer reports whether addr (as reported by [connect.Peer.Addr]) is
// a loopback host. Empty addr is treated as non-loopback so an unknown peer
// never gets the in-place file shortcut.
func isLoopbackPeer(addr string) bool {
	if addr == "" {
		return false
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return host == "localhost"
}
