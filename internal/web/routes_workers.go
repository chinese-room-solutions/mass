package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"

	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/starfederation/datastar-go/datastar"
)

// patchWorkersList patches the workers list and restores open/closed state.
func (h *Handler) patchWorkersList(sse *datastar.ServerSentEventGenerator, views []templates.WorkerView) {
	mustSSE(
		// Snapshot open state, patch, restore — all via ExecuteScript to avoid races.
		sse.ExecuteScript(`(function(){var s={};document.querySelectorAll('#workers-list .worker-card').forEach(function(c){if(c.open)s[c.id]=true});window.__workerOpenSnap=s})()`))
	mustSSE(sse.PatchElements(
		templates.RenderAgentsListInner(views),
		datastar.WithSelector("#workers-list"),
		datastar.WithMode(datastar.ElementPatchModeInner),
	))
	mustSSE(sse.ExecuteScript(`(function(){var s=window.__workerOpenSnap||{};for(var k in s)if(s[k]){var c=document.getElementById(k);if(c)c.open=true}if(window.__reapplyWorkersFilter)window.__reapplyWorkersFilter()})()`))
}

// patchWorkerStats updates gauge values via JS without replacing the DOM.
// This avoids the open/closed state problem entirely.
func (h *Handler) patchWorkerStats(sse *datastar.ServerSentEventGenerator) {
	const circumference = 2 * math.Pi * 28 // must match writeGauge radius

	var sb strings.Builder
	sb.WriteString(`(function(){var u=function(id,pct,sub,color){`)
	sb.WriteString(`var g=document.getElementById('gauge-'+id);if(!g)return;`)
	sb.WriteString(`var c=g.querySelectorAll('circle')[1];if(c){c.setAttribute('stroke-dashoffset',`)
	fmt.Fprintf(&sb, `(%.4f*(1-pct/100)).toFixed(2));`, circumference)
	sb.WriteString(`c.setAttribute('stroke',color)}`)
	sb.WriteString(`var t=g.querySelector('text');if(t)t.textContent=Math.round(pct)+'%';`)
	sb.WriteString(`var p=g.parentNode;if(p){var spans=p.querySelectorAll('span');`)
	sb.WriteString(`if(spans.length>=2)spans[1].textContent=sub}`)
	sb.WriteString(`};`)

	for _, ag := range h.workers.All() {
		statsMap := make(map[string]stats.DeviceStats)
		for _, s := range ag.Stats() {
			statsMap[s.DeviceID] = s
		}
		for _, dev := range safeDevices(ag) {
			s, ok := statsMap[dev.ID]
			if !ok {
				continue
			}
			scopedID := ag.ID() + "_" + dev.ID
			// Memory gauge.
			if s.TotalMemoryMB > 0 {
				pct := float64(s.UsedMemoryMB) / float64(s.TotalMemoryMB) * 100
				if pct > 100 {
					pct = 100
				}
				sub := fmt.Sprintf("%s / %s", templates.FormatMemMB(s.UsedMemoryMB), templates.FormatMemMB(s.TotalMemoryMB))
				fmt.Fprintf(&sb, `u('%s-mem',%.1f,'%s','%s');`, scopedID, pct, sub, templates.BarColor(pct))
			}
			// Utilization gauge.
			pct := s.UtilizationPct
			if pct > 100 {
				pct = 100
			}
			fmt.Fprintf(&sb, `u('%s-util',%.1f,'%.0f%%','%s');`, scopedID, pct, pct, templates.BarColor(pct))
		}
	}

	// Also update CPU utilization from local worker stats.
	sb.WriteString(`})()`)
	mustSSE(sse.ExecuteScript(sb.String()))
}

// replayWorkers pushes the current workers list to an SSE client.
func (h *Handler) replayWorkers(sse *datastar.ServerSentEventGenerator) {
	h.patchWorkersList(sse, h.buildWorkerViews())
}

// handleListWorkers returns the workers list with current device/benchmark data.
func (h *Handler) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	h.patchWorkersList(sse, h.buildWorkerViews())
}

// benchmarkTarget specifies which workers/devices to benchmark.
type benchmarkTarget struct {
	WorkerIDs map[string]bool // empty = all online workers
	DeviceIDs map[string]bool // empty = all devices on selected workers
}

func (t benchmarkTarget) matchWorker(id string) bool {
	return len(t.WorkerIDs) == 0 || t.WorkerIDs[id]
}

func (t benchmarkTarget) matchDevice(id string) bool {
	return len(t.DeviceIDs) == 0 || t.DeviceIDs[id]
}

// benchmarkResult is one device's benchmark outcome for JSON responses.
type benchmarkResult struct {
	WorkerID      string  `json:"worker_id"`
	DeviceID      string  `json:"device_id"`
	DeviceName    string  `json:"device_name"`
	MemoryGBs     float64 `json:"memory_gbs"`
	ComputeGFlops float64 `json:"compute_gflops"`
	Error         string  `json:"error,omitempty"`
}

// benchWorker runs benchmarks for a single worker and returns results.
// Devices within a single worker run sequentially (shared hardware).
func (h *Handler) benchWorker(ag worker.WorkerInterface, target benchmarkTarget) []benchmarkResult {
	var out []benchmarkResult

	workerID := ag.ID()
	devices := safeDevices(ag)
	for _, dev := range devices {
		if len(target.DeviceIDs) > 0 && !target.DeviceIDs[dev.ID] {
			continue
		}
		h.logger.Info().Str("agent", workerID).Str("device", dev.ID).Msg("starting benchmark")
		res, err := ag.Bench(dev.ID)
		if err != nil {
			h.logger.Error().Err(err).Str("agent", workerID).Str("device", dev.ID).Msg("benchmark failed")
			out = append(out, benchmarkResult{WorkerID: workerID, DeviceID: dev.ID, Error: err.Error()})
			continue
		}
		h.saveBenchResults(workerID, []bench.Result{res})
		out = append(out, benchmarkResult{
			WorkerID:      workerID,
			DeviceID:      res.DeviceID,
			DeviceName:    res.DeviceName,
			MemoryGBs:     res.MemoryGBs,
			ComputeGFlops: res.ComputeGFlops,
		})
	}

	return out
}

// runBenchmarks executes benchmarks on the targeted workers/devices and returns results.
// Workers run in parallel (independent machines); devices within a worker run sequentially.
func (h *Handler) runBenchmarks(target benchmarkTarget) []benchmarkResult {
	var work []worker.WorkerInterface
	for _, ag := range h.workers.All() {
		if !ag.Status().Online || !target.matchWorker(ag.ID()) {
			continue
		}
		work = append(work, ag)
	}

	if len(work) == 0 {
		return nil
	}

	// Single worker — no goroutine overhead.
	if len(work) == 1 {
		return h.benchWorker(work[0], target)
	}

	// Multiple workers — run in parallel.
	ch := make(chan []benchmarkResult, len(work))
	for _, ag := range work {
		go func(ag worker.WorkerInterface) {
			ch <- h.benchWorker(ag, target)
		}(ag)
	}

	var out []benchmarkResult
	for range work {
		out = append(out, (<-ch)...)
	}
	return out
}

// handleBenchmarkSSE is the Datastar SSE endpoint for UI benchmark triggers.
// Reads benchWorkerIds / benchDeviceIds from Datastar signals.
func (h *Handler) handleBenchmarkSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	workerIds := r.URL.Query().Get("workerIds")
	deviceIds := r.URL.Query().Get("deviceIds")

	target := benchmarkTarget{
		WorkerIDs: parseCSV(workerIds),
		DeviceIDs: parseCSV(deviceIds),
	}

	// Show loading spinners on targeted devices' benchmark text via JS.
	for _, ag := range h.workers.All() {
		if !ag.Status().Online || !target.matchWorker(ag.ID()) {
			continue
		}
		for _, dev := range safeDevices(ag) {
			if target.matchDevice(dev.ID) {
				scopedID := ag.ID() + "_" + dev.ID
				spin := `<sl-spinner style="font-size:0.75rem;--track-width:2px"></sl-spinner>`
				mustSSE(sse.ExecuteScript(fmt.Sprintf(
					`(function(){var s='%s';`+
						`['bench-bw-%s','bench-comp-%s'].forEach(function(id){`+
						`var e=document.getElementById(id);if(e)e.innerHTML=s})})()`,
					spin, scopedID, scopedID,
				)))
			}
		}
	}

	h.runBenchmarks(target)

	// Broadcast a full agent change so all SSE clients get updated results.
	h.broker.Broadcast(SSEEvent{Type: EventTypeWorkerChange})
}

// handleBenchmarkAPI is the JSON API endpoint for programmatic benchmark triggers.
// POST /api/v1/benchmark
// Body: {"agent_ids": ["id1", ...], "device_ids": ["id1", ...]}
// Both fields are optional; empty/omitted means "all".
func (h *Handler) handleBenchmarkAPI(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkerIDs []string `json:"agent_ids"`
		DeviceIDs []string `json:"device_ids"`
	}
	if r.Body != nil {
		// Decode failure means caller sent malformed/empty body — proceed with
		// zero values (run benchmarks for all workers/devices).
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			h.logger.Debug().Err(err).Msg("decoding benchmark API request body")
		}
	}

	target := benchmarkTarget{
		WorkerIDs: sliceToSet(req.WorkerIDs),
		DeviceIDs: sliceToSet(req.DeviceIDs),
	}

	results := h.runBenchmarks(target)

	// Broadcast agent change so any open SSE connections refresh the UI.
	h.broker.Broadcast(SSEEvent{Type: EventTypeWorkerChange})

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"results": results,
	}); err != nil {
		h.logger.Error().Err(err).Msg("encoding benchmark response")
	}
}

// parseCSV splits a comma-separated string into a set of non-empty trimmed values.
func parseCSV(s string) map[string]bool {
	m := make(map[string]bool)
	for _, v := range strings.Split(s, ",") {
		v = strings.TrimSpace(v)
		if v != "" {
			m[v] = true
		}
	}
	return m
}

// sliceToSet converts a string slice to a set.
func sliceToSet(ss []string) map[string]bool {
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		s = strings.TrimSpace(s)
		if s != "" {
			m[s] = true
		}
	}
	return m
}

// handleToggleWorkerScheduling toggles all device queues for a worker.
// POST /api/workers/toggle?worker=<workerID>
func (h *Handler) handleToggleWorkerScheduling(w http.ResponseWriter, r *http.Request) {
	workerID := r.URL.Query().Get("worker")
	if workerID == "" {
		http.Error(w, "worker parameter required", http.StatusBadRequest)
		return
	}

	// Find all device queues for this worker and determine current state.
	states, err := h.store.ListDeviceQueueStates()
	if err != nil {
		h.logger.Error().Err(err).Msg("listing device queue states")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	var workerQueues []store.DeviceQueueState
	allDisabled := true
	for _, st := range states {
		if st.WorkerID == workerID {
			workerQueues = append(workerQueues, st)
			if st.Enabled {
				allDisabled = false
			}
		}
	}

	if len(workerQueues) == 0 {
		http.Error(w, "no device queues found for worker", http.StatusNotFound)
		return
	}

	// If all disabled → enable all; otherwise → disable all.
	newEnabled := allDisabled
	totalDrained := 0
	for _, st := range workerQueues {
		drained, err := h.orch.SetDeviceQueueEnabled(r.Context(), st.QueueName, newEnabled)
		if err != nil {
			h.logger.Error().Err(err).Str("queue", st.QueueName).Msg("toggling device in worker toggle")
			continue
		}
		totalDrained += drained
	}

	action := "enabled"
	if !newEnabled {
		action = "disabled"
	}
	h.logger.Info().Str("worker", workerID).Str("action", action).Int("drained", totalDrained).Msg("worker scheduling toggled via UI")

	h.broker.Broadcast(SSEEvent{Type: EventTypeWorkerChange})
	w.WriteHeader(http.StatusNoContent)
}

// handleToggleDeviceScheduling toggles a device queue's enabled state.
// POST /api/workers/devices/toggle?queue=<queueName>
func (h *Handler) handleToggleDeviceScheduling(w http.ResponseWriter, r *http.Request) {
	queueName := r.URL.Query().Get("queue")
	if queueName == "" {
		http.Error(w, "queue parameter required", http.StatusBadRequest)
		return
	}

	// Read current state.
	st, err := h.store.GetDeviceQueueState(queueName)
	if err != nil {
		h.logger.Error().Err(err).Str("queue", queueName).Msg("reading device queue state")
		http.Error(w, "queue not found", http.StatusNotFound)
		return
	}

	newEnabled := !st.Enabled
	drained, err := h.orch.SetDeviceQueueEnabled(r.Context(), queueName, newEnabled)
	if err != nil {
		h.logger.Error().Err(err).Str("queue", queueName).Msg("toggling device scheduling")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	action := "enabled"
	if !newEnabled {
		action = "disabled"
	}
	h.logger.Info().Str("queue", queueName).Str("action", action).Int("drained", drained).Msg("device scheduling toggled via UI")

	h.broker.Broadcast(SSEEvent{Type: EventTypeWorkerChange})
	w.WriteHeader(http.StatusNoContent)
}

// buildWorkerViews assembles agent views from the fleet + stored benchmarks.
func (h *Handler) buildWorkerViews() []templates.WorkerView {
	workers := h.workers.All()

	// Pre-fetch all device queue states for enabled/disabled lookup.
	queueStates, _ := h.store.ListDeviceQueueStates()
	stateByQueue := make(map[string]store.DeviceQueueState, len(queueStates))
	for _, s := range queueStates {
		stateByQueue[s.QueueName] = s
	}

	views := make([]templates.WorkerView, 0, len(workers))
	for _, ag := range workers {
		status := ag.Status()

		// Build stats map from live agent stats.
		statsMap := make(map[string]stats.DeviceStats)
		for _, s := range ag.Stats() {
			statsMap[s.DeviceID] = s
		}

		var devices []templates.ComputeView
		for _, dev := range safeDevices(ag) {
			queueName := scheduler.DeviceQueueName(ag.ID(), dev.ID)
			cv := templates.ComputeView{
				DeviceID:   dev.ID,
				DeviceName: dev.Name,
				Type:       dev.Type,
				MemoryMB:   dev.TotalMemoryMB,
				Enabled:    true,
				QueueName:  queueName,
			}
			if st, ok := stateByQueue[queueName]; ok {
				cv.Enabled = st.Enabled
			}
			if row, err := h.store.GetBenchmark(ag.ID(), dev.ID); err == nil {
				cv.MemoryGBs = row.MemoryGBs
				cv.ComputeGFlops = row.ComputeGFlops
				cv.HasBenchmark = true
			}
			if s, ok := statsMap[dev.ID]; ok {
				if s.TotalMemoryMB > 0 {
					cv.UsedMemoryMB = s.UsedMemoryMB
					cv.MemoryMB = s.TotalMemoryMB
					cv.HasStats = true
				}
				cv.UtilizationPct = s.UtilizationPct
				cv.HasUtilization = true // always show bar when stats are available
			}
			devices = append(devices, cv)
		}
		allDisabled := len(devices) > 0
		for _, d := range devices {
			if d.Enabled {
				allDisabled = false
				break
			}
		}
		views = append(views, templates.WorkerView{
			ID:          ag.ID(),
			Name:        ag.Name(),
			Online:      status.Online,
			AllDisabled: allDisabled,
			Devices:     devices,
		})
	}
	return views
}

// saveBenchResults persists benchmark results to the store.
func (h *Handler) saveBenchResults(workerID string, results []bench.Result) {
	for _, res := range results {
		h.logger.Info().
			Str("agent", workerID).
			Str("device", res.DeviceID).
			Float64("memory_gbs", res.MemoryGBs).
			Float64("compute_gflops", res.ComputeGFlops).
			Msg("benchmark result")

		if err := h.store.SaveBenchmark(store.BenchmarkRow{
			WorkerID:      workerID,
			DeviceID:      res.DeviceID,
			DeviceName:    res.DeviceName,
			MemoryGBs:     res.MemoryGBs,
			ComputeGFlops: res.ComputeGFlops,
			BenchedAt:     res.BenchedAt,
		}); err != nil {
			h.logger.Error().Err(err).Str("agent", workerID).Str("device", res.DeviceID).Msg("saving benchmark result")
		}
	}
}

// autoBenchmarkWorker benchmarks any devices on the given agent that don't
// already have stored benchmark data. Runs synchronously — callers should
// invoke in a goroutine to avoid blocking.
func (h *Handler) autoBenchmarkWorker(workerID string) {
	ag := h.workers.Get(workerID)
	if ag == nil || !ag.Status().Online {
		return
	}

	// Collect devices that lack benchmark data.
	var missing []string
	for _, dev := range safeDevices(ag) {
		has, err := h.store.HasBenchmark(workerID, dev.ID)
		if err != nil {
			h.logger.Error().Err(err).Str("device", dev.ID).Msg("checking benchmark existence")
			continue
		}
		if !has {
			missing = append(missing, dev.ID)
		}
	}
	if len(missing) == 0 {
		return
	}

	h.logger.Info().Str("agent", workerID).Strs("devices", missing).Msg("auto-benchmarking unbenchmarked devices")

	target := benchmarkTarget{
		WorkerIDs: map[string]bool{workerID: true},
		DeviceIDs: sliceToSet(missing),
	}
	h.runBenchmarks(target)

	// Notify UI so benchmark values appear.
	h.broker.Broadcast(SSEEvent{Type: EventTypeWorkerChange})
}

// safeDevices calls Devices() with panic recovery.
func safeDevices(ag interface{ Devices() []stats.Device }) (devices []stats.Device) {
	defer func() {
		if r := recover(); r != nil {
			devices = nil
		}
	}()
	return ag.Devices()
}
