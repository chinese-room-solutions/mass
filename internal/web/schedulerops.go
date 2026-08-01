package web

import (
	"fmt"
	"sort"
	"strings"

	"github.com/chinese-room-solutions/mass/internal/audit"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
)

// Scheduler (loaded-instance) operations shared by the dashboard's Scheduler
// tab and the public mass.v1.Mass Connect API. instanceInfos is read-only;
// evictInstance owns its audit-log call.

// InstanceInfo is the transport-neutral view of one (worker, loaded-model)
// pair. The HTMX SchedulerInstanceView and the Connect Instance message both
// map from it.
type InstanceInfo struct {
	Key         string
	ModelID     string
	Filename    string
	Fingerprint string
	WorkerID    string
	WorkerName  string
	RuntimeName string
	DeviceIDs   []string
	Source      string
	Mode        string
	Status      string
	PoolSize    int
	Active      int
}

// instanceInfos builds the neutral loaded-instance views from the live fleet.
// One instance per (worker, loaded-model) pair, sorted by key.
func (h *Handler) instanceInfos() []InstanceInfo {
	if h.workers == nil {
		return nil
	}
	var out []InstanceInfo
	for _, w := range h.workers.All() {
		sw, ok := w.(*worker.StreamWorker)
		if !ok {
			continue
		}
		for _, lm := range sw.LoadedModels() {
			status := "Active"
			if lm.Active == 0 {
				status = "Idle"
			}
			filename, fingerprint := splitModelID(lm.ModelID)
			source := lm.Source
			if source == "" {
				source = "direct"
			}
			out = append(out, InstanceInfo{
				Key:         sw.ID() + ":" + lm.ModelID,
				ModelID:     lm.ModelID,
				Filename:    filename,
				Fingerprint: fingerprint,
				WorkerID:    sw.ID(),
				WorkerName:  sw.Name(),
				RuntimeName: sw.RuntimeName(),
				DeviceIDs:   h.instanceDeviceIDs(sw, lm),
				Source:      source,
				Mode:        "dynamic", // gateway-driven loads are dynamic by definition
				PoolSize:    lm.PoolSize,
				Active:      lm.Active,
				Status:      status,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// instanceDeviceIDs returns the canonical device IDs a loaded model occupies.
// Prefers the worker-reported set (populated post-stage-2 when the gateway
// emits LoadedModelStatus.DeviceIDs). For workers that haven't been upgraded
// yet, falls back to "every enabled GPU" — that mirrors llama.cpp's
// mparams.devices whitelist (the C++ worker only includes the CPU when every
// GPU is disabled, so this matches the actual placement set in practice).
func (h *Handler) instanceDeviceIDs(sw *worker.StreamWorker, lm worker.LoadedModelStatus) []string {
	if len(lm.DeviceIDs) > 0 {
		return lm.DeviceIDs
	}
	devs := sw.Devices()
	if len(devs) == 0 {
		return nil
	}
	enabledByDev := map[string]bool{}
	if h.store != nil {
		if rows, err := h.store.ListWorkerDevicesEnabled(sw.ID()); err == nil {
			for _, r := range rows {
				enabledByDev[r.DeviceID] = r.Enabled
			}
		}
	}
	enabled := func(id string) bool {
		v, ok := enabledByDev[id]
		return !ok || v // absent row = enabled by default
	}
	var gpus []string
	for _, d := range devs {
		if d.Type == stats.DeviceTypeGPU && enabled(d.ID) {
			gpus = append(gpus, d.ID)
		}
	}
	if len(gpus) > 0 {
		return gpus
	}
	for _, d := range devs {
		if d.Type == stats.DeviceTypeCPU && enabled(d.ID) {
			return []string{d.ID}
		}
	}
	return nil
}

// splitModelID splits a gateway-built model ID at its trailing "#<fingerprint>"
// suffix, returning the prefix as filename and the fingerprint separately.
// Shape is gateway-defined and opaque to MASS; today llama-cpp emits
// "<filename>#<sha-prefix>". Returns the full ID as filename when no '#'.
func splitModelID(id string) (filename, fingerprint string) {
	if i := strings.LastIndexByte(id, '#'); i >= 0 {
		return id[:i], id[i+1:]
	}
	return id, ""
}

// evictInstance unloads modelID from the worker's pool. Fires an immediate
// Scheduler-tab refresh so the operator sees the card vanish without waiting
// for the next heartbeat diff. Returns ErrOpNotFound when no worker matches.
func (h *Handler) evictInstance(workerID, modelID, actor string) error {
	if h.workers == nil {
		return fmt.Errorf("%w: no worker fleet", ErrOpUnavailable)
	}
	if workerID == "" || modelID == "" {
		return fmt.Errorf("%w: worker and model are required", ErrOpInvalid)
	}
	for _, w := range h.workers.All() {
		sw, ok := w.(*worker.StreamWorker)
		if !ok || sw.ID() != workerID {
			continue
		}
		if err := sw.UnloadModel(modelID); err != nil {
			return err
		}
		audit.Log(h.logger, "scheduler.evicted", modelID, audit.OutcomeOK).
			Str("actor", actor).Str("worker", workerID).Msg("")
		h.workers.NotifyLoadedChanged(sw.ID())
		return nil
	}
	return fmt.Errorf("%w: worker %s", ErrOpNotFound, workerID)
}
