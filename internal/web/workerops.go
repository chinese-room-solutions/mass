package web

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/chinese-room-solutions/mass/internal/audit"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
)

// Worker-fleet operations shared by the dashboard's /api/workers handlers and
// the public mass.v1.Mass Connect API. Each owns its audit-log call and
// returns sentinel errors (ops.go) so transports map their own status codes.

// WorkerInfo is the transport-neutral view of one worker plus its devices.
// The HTMX WorkerView and the Connect Worker message both map from it.
type WorkerInfo struct {
	ID          string
	Name        string
	RuntimeName string
	Version     string // worker's own semver (required at handshake)
	Compatible  string // runtime-version range it decodes (required at handshake)
	Online      bool
	Enabled     bool // operator toggle: any device on this worker enabled
	ActiveJobs  int
	Devices     []DeviceInfo
	// BenchingModel is the store key of the model being measured on this
	// worker right now, "" when it isn't benching. A benching worker takes
	// no jobs until the measurement finishes.
	BenchingModel string
}

// DeviceInfo is the transport-neutral view of one worker device.
type DeviceInfo struct {
	DeviceID       string
	DeviceName     string
	Type           string // "CPU" / "GPU"
	Enabled        bool
	TotalMemoryMB  int
	UsedMemoryMB   int
	UtilizationPct float64
	HasUtilization bool
	HasStats       bool
	HasBenchmark   bool
	MemoryGBs      float64
	LoadGBs        float64
	ComputeGFlops  float64 // device matmul throughput, display only
}

// workerInfos assembles per-worker neutral views from the live fleet plus any
// stored benchmark rows, in fleet enumeration order. buildWorkerViews maps
// this onto the template shape.
func (h *Handler) workerInfos() []WorkerInfo {
	if h.workers == nil {
		return nil
	}
	all := h.workers.All()
	out := make([]WorkerInfo, 0, len(all))
	for _, wkr := range all {
		status := wkr.Status()

		statsMap := make(map[string]stats.DeviceStats)
		for _, s := range wkr.Stats() {
			statsMap[s.DeviceID] = s
		}

		// Snapshot the operator's per-device toggle state once per worker —
		// absent rows mean "enabled" (sane default for newly-connected workers
		// without any persisted intent).
		enabledByDev := map[string]bool{}
		if h.store != nil {
			if rows, err := h.store.ListWorkerDevicesEnabled(wkr.ID()); err == nil {
				for _, r := range rows {
					enabledByDev[r.DeviceID] = r.Enabled
				}
			}
		}

		var devices []DeviceInfo
		anyEnabled := false
		for _, dev := range safeDevices(wkr) {
			devEnabled := true
			if v, ok := enabledByDev[dev.ID]; ok {
				devEnabled = v
			}
			if devEnabled {
				anyEnabled = true
			}
			di := DeviceInfo{
				DeviceID:      dev.ID,
				DeviceName:    dev.Name,
				Type:          string(dev.Type),
				Enabled:       devEnabled,
				TotalMemoryMB: dev.TotalMemoryMB,
			}
			if h.store != nil {
				if row, err := h.store.GetBenchmark(wkr.ID(), dev.ID); err == nil {
					di.MemoryGBs = row.MemoryGBs
					di.LoadGBs = row.LoadGBs
					di.ComputeGFlops = row.Flops
					di.HasBenchmark = true
				}
			}
			if s, ok := statsMap[dev.ID]; ok {
				if s.TotalMemoryMB > 0 {
					di.UsedMemoryMB = s.UsedMemoryMB
					di.TotalMemoryMB = s.TotalMemoryMB
					di.HasStats = true
				}
				di.UtilizationPct = s.UtilizationPct
				di.HasUtilization = true
			}
			devices = append(devices, di)
		}
		// "Worker enabled" is derived: any non-disabled device counts. A worker
		// with no devices yet (race before first heartbeat) is treated enabled.
		workerEnabled := anyEnabled || len(devices) == 0

		benching := ""
		if h.orch != nil {
			benching = h.orch.BenchInFlight(wkr.ID())
		}

		out = append(out, WorkerInfo{
			ID:            wkr.ID(),
			Name:          wkr.Name(),
			RuntimeName:   wkr.RuntimeName(),
			Version:       wkr.Version(),
			Compatible:    wkr.Compatible(),
			Online:        status.Online,
			Enabled:       workerEnabled,
			ActiveJobs:    wkr.ActiveJobs(),
			Devices:       devices,
			BenchingModel: benching,
		})
	}
	return out
}

// setWorkerDeviceEnabled flips one (worker, device) enable flag to an explicit
// value: persists it, pushes the new whitelist to the worker, drains/re-scores
// the scheduler, and broadcasts a Workers-tab change. The dashboard toggle
// handler reads the current value and passes its inverse.
func (h *Handler) setWorkerDeviceEnabled(ctx context.Context, workerID, deviceID string, enabled bool, actor string) error {
	if h.workers == nil || h.store == nil {
		return fmt.Errorf("%w: no worker fleet", ErrOpUnavailable)
	}
	if workerID == "" || deviceID == "" {
		return fmt.Errorf("%w: worker id and device id required", ErrOpInvalid)
	}
	wkr := h.workers.Get(workerID)
	if wkr == nil {
		return fmt.Errorf("%w: worker %s", ErrOpNotFound, workerID)
	}
	release, ok := h.tryReestimateLock(workerID)
	if !ok {
		return fmt.Errorf("%w: worker re-estimating queued jobs after a recent toggle", ErrOpBusy)
	}
	defer release()

	if err := h.store.SetWorkerDeviceEnabled(workerID, deviceID, enabled); err != nil {
		return fmt.Errorf("persisting toggle: %w", err)
	}
	action := "worker.device_enabled"
	if !enabled {
		action = "worker.device_disabled"
	}
	audit.Log(h.logger, action, workerID, audit.OutcomeOK).
		Str("actor", actor).Str("device", deviceID).Msg("")

	h.applyDeviceToggle(ctx, wkr, workerID)
	return nil
}

// setWorkerEnabled flips every device on the worker to an explicit value in one
// transaction (CPU included in the bulk set): persists, pushes the whitelist,
// drains/re-scores the scheduler, and broadcasts. The dashboard toggle handler
// derives the current "any enabled" state and passes its inverse.
func (h *Handler) setWorkerEnabled(ctx context.Context, workerID string, enabled bool, actor string) error {
	if h.workers == nil || h.store == nil {
		return fmt.Errorf("%w: no worker fleet", ErrOpUnavailable)
	}
	if workerID == "" {
		return fmt.Errorf("%w: worker id required", ErrOpInvalid)
	}
	wkr := h.workers.Get(workerID)
	if wkr == nil {
		return fmt.Errorf("%w: worker %s", ErrOpNotFound, workerID)
	}
	release, ok := h.tryReestimateLock(workerID)
	if !ok {
		return fmt.Errorf("%w: worker re-estimating queued jobs after a recent toggle", ErrOpBusy)
	}
	defer release()

	devices := safeDevices(wkr)
	if len(devices) == 0 {
		return nil
	}
	toUpdate := make([]string, 0, len(devices))
	for _, d := range devices {
		toUpdate = append(toUpdate, d.ID)
	}
	if err := h.store.SetWorkerDevicesEnabledBulk(workerID, toUpdate, enabled); err != nil {
		return fmt.Errorf("persisting toggle: %w", err)
	}
	action := "worker.enabled"
	if !enabled {
		action = "worker.disabled"
	}
	audit.Log(h.logger, action, workerID, audit.OutcomeOK).
		Str("actor", actor).Int("devices", len(toUpdate)).Msg("")

	h.applyDeviceToggle(ctx, wkr, workerID)
	return nil
}

// applyDeviceToggle pushes the recomputed enabled-device whitelist to the
// worker, wakes the scheduler to drain/re-score its queue against the new
// device set, and broadcasts a Workers-tab change. Shared by both toggles.
//
// Enabling refreshes the persisted device list (OnWorkerConnected is
// idempotent); OnWorkerDevicesChanged then drains pending rows whose worker is
// no longer schedulable and kicks the dispatcher so peers pick them up.
// Disabling the last device cascades into a full drain back to global so jobs
// aren't stranded. Re-estimation rewrites the surviving rows' per-envelope
// QueuedSeconds against the new device set.
func (h *Handler) applyDeviceToggle(ctx context.Context, wkr worker.WorkerInterface, workerID string) {
	if err := h.pushEnabledDevices(wkr); err != nil {
		h.logger.Warn().Err(err).Str("worker", workerID).Msg("pushing enabled devices to worker")
	}
	if h.orch != nil {
		if sw, ok := wkr.(*worker.StreamWorker); ok {
			h.orch.OnWorkerConnected(sw)
			h.orch.OnWorkerDevicesChanged(workerID)
			h.orch.ReestimateWorkerQueue(ctx, workerID)
		}
	}
	h.workersBroker.Broadcast(WorkersEvent{Kind: WorkersEventChange})
}

// deviceEnabled reads the current persisted enable state for one (worker,
// device) pair, defaulting to enabled when no row exists. The dashboard toggle
// handler uses it to compute the inverse to pass to setWorkerDeviceEnabled.
func (h *Handler) deviceEnabled(workerID, deviceID string) bool {
	if h.store == nil {
		return true
	}
	row, err := h.store.GetWorkerDeviceEnabled(workerID, deviceID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			h.logger.Warn().Err(err).Str("worker", workerID).Str("device", deviceID).Msg("reading device enabled state")
		}
		return true
	}
	return row.Enabled
}

// workerAnyEnabled reports whether any device on the worker is currently
// enabled (absent row = enabled). The dashboard toggle handler passes its
// inverse to setWorkerEnabled so a partially-enabled worker toggles off.
func (h *Handler) workerAnyEnabled(workerID string, devices []stats.Device) bool {
	enabledByDev := map[string]bool{}
	if h.store != nil {
		rows, err := h.store.ListWorkerDevicesEnabled(workerID)
		if err != nil {
			h.logger.Warn().Err(err).Str("worker", workerID).Msg("listing device enabled state")
		}
		for _, r := range rows {
			enabledByDev[r.DeviceID] = r.Enabled
		}
	}
	for _, d := range devices {
		if v, ok := enabledByDev[d.ID]; !ok || v {
			return true
		}
	}
	return false
}

// benchmarkWorkers benches the targeted workers/devices (empty target = every
// online worker / every device), persists results, and broadcasts a Workers-tab
// change so every browser refreshes. Returns one result per benched device.
func (h *Handler) benchmarkWorkers(workerIDs, deviceIDs []string) []benchResult {
	if h.workers == nil {
		return nil
	}
	wantWorkers := sliceSet(workerIDs)
	wantDevices := sliceSet(deviceIDs)

	var (
		mu      sync.Mutex
		results []benchResult
		wg      sync.WaitGroup
	)
	for _, wkr := range h.workers.All() {
		if !wkr.Status().Online {
			continue
		}
		if len(wantWorkers) > 0 && !wantWorkers[wkr.ID()] {
			continue
		}
		wg.Add(1)
		go func(wkr worker.WorkerInterface) {
			defer wg.Done()
			h.benchmarkWorker(wkr, wantDevices, &mu, &results)
		}(wkr)
	}
	wg.Wait()

	if h.workersBroker != nil {
		h.workersBroker.Broadcast(WorkersEvent{Kind: WorkersEventChange})
	}
	return results
}

// mintJoinToken creates a worker-enrollment join token valid for ttl (0 selects
// the server default). Returns the plaintext (shown once) and the unix expiry.
// Operator-authenticated at the transport layer — this op must never be reached
// by an auth-exempt path.
func (h *Handler) mintJoinToken(ttl time.Duration, actor string) (token string, expiresAt int64, err error) {
	if h.enroller == nil {
		return "", 0, fmt.Errorf("%w: enroller", ErrOpUnavailable)
	}
	token, expiresAt, err = h.enroller.MintJoinToken(ttl)
	if err != nil {
		return "", 0, fmt.Errorf("minting join token: %w", err)
	}
	audit.Log(h.logger, "worker.join_token_created", "", audit.OutcomeOK).
		Str("actor", actor).Int64("expires_at", expiresAt).Msg("")
	return token, expiresAt, nil
}

// revokeWorker deletes an enrolled worker's persisted credential, so its next
// steady-state connect is rejected as unknown. A currently-connected worker is
// not torn down mid-stream; revocation takes effect on its next reconnect.
// Returns ErrOpNotFound when no such worker row exists.
func (h *Handler) revokeWorker(workerID, actor string) error {
	if h.store == nil {
		return fmt.Errorf("%w: store", ErrOpUnavailable)
	}
	if workerID == "" {
		return fmt.Errorf("%w: worker id required", ErrOpInvalid)
	}
	deleted, err := h.store.DeleteWorker(workerID)
	if err != nil {
		return fmt.Errorf("revoking worker: %w", err)
	}
	if !deleted {
		return fmt.Errorf("%w: worker %s", ErrOpNotFound, workerID)
	}
	// A revoked worker is never coming back under this id, so its
	// measurements are dead weight.
	if h.orch != nil {
		h.orch.OnWorkerRemoved(workerID)
	}
	audit.Log(h.logger, "worker.revoked", workerID, audit.OutcomeOK).
		Str("actor", actor).Msg("")
	return nil
}

// sliceSet builds a non-empty set from a slice, dropping empties.
func sliceSet(ss []string) map[string]bool {
	out := map[string]bool{}
	for _, s := range ss {
		if s != "" {
			out[s] = true
		}
	}
	return out
}
