package web

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KernelPryanic/ctxerr"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/runtimes"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/starfederation/datastar-go/datastar"
	"golang.org/x/crypto/bcrypt"
)

// --- Lazy tab fragments --------------------------------------------------
// The dashboard ships an empty shell so initial render is O(1). Each tab
// requests its own data on first activation via the endpoints below.

func (h *Handler) handleSchedulerList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := templates.SchedulerInstanceList(h.schedulerInstances()).Render(r.Context(), w); err != nil {
		h.logger.Warn().Err(err).Msg("rendering scheduler list")
	}
}

func (h *Handler) handleWorkersList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := io.WriteString(w, templates.RenderWorkersList(h.buildWorkerViews())); err != nil {
		h.logger.Warn().Err(err).Msg("rendering workers list")
	}
}

// --- File browser (Settings + Install dialogs) ---------------------------

type browseRoot struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

type browseEntry struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	IsDir bool   `json:"is_dir"`
}

// handleBrowseRoots returns the filesystem roots (drives on Windows, "/"
// elsewhere) the file browser starts from when no directory is selected.
func (h *Handler) handleBrowseRoots(w http.ResponseWriter, r *http.Request) {
	_ = r
	roots := listRoots()
	out := make([]browseRoot, len(roots))
	for i, root := range roots {
		out[i] = browseRoot{Name: root, Path: root}
	}
	h.writeJSON(w, http.StatusOK, out)
}

// handleBrowseFiles lists the contents of dir, optionally filtered by ext.
// ext is a comma-separated list of extensions (".mass" for the runtime
// installer, empty for Browse Local — server-side filtering is the
// caller's choice). Returns a parent ".." entry when not at the filesystem
// root. Symlinks are skipped to avoid loops. Hidden files are listed
// (operators can hide them in the UI if needed).
func (h *Handler) handleBrowseFiles(w http.ResponseWriter, r *http.Request) {
	dir := r.URL.Query().Get("dir")
	rawExt := strings.ToLower(r.URL.Query().Get("ext"))
	var exts []string
	for _, e := range strings.Split(rawExt, ",") {
		if e = strings.TrimSpace(e); e != "" {
			exts = append(exts, e)
		}
	}

	if dir == "" {
		if h.cfg != nil && h.cfg.DataDir != "" {
			dir = h.cfg.DataDir
		} else if home, err := os.UserHomeDir(); err == nil {
			dir = home
		} else {
			dir = "."
		}
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "resolving path: "+err.Error())
		return
	}
	dir = absDir

	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, err.Error())
		return
	}

	var entries []browseEntry
	if parent := filepath.Dir(dir); parent != dir {
		entries = append(entries, browseEntry{Name: "..", Path: parent, IsDir: true})
	}
	for _, d := range dirEntries {
		if d.Type()&fs.ModeSymlink != 0 {
			continue
		}
		name := d.Name()
		path := filepath.Join(dir, name)
		if d.IsDir() {
			entries = append(entries, browseEntry{Name: name, Path: path, IsDir: true})
		} else if matchesExt(name, exts) {
			entries = append(entries, browseEntry{Name: name, Path: path, IsDir: false})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir != entries[j].IsDir {
			return entries[i].IsDir
		}
		return entries[i].Name < entries[j].Name
	})
	h.writeJSON(w, http.StatusOK, entries)
}

// matchesExt reports whether a filename ends in any of exts (already
// lowercased, leading-dot included). An empty exts slice matches anything.
func matchesExt(name string, exts []string) bool {
	if len(exts) == 0 {
		return true
	}
	lower := strings.ToLower(name)
	for _, e := range exts {
		if strings.HasSuffix(lower, e) {
			return true
		}
	}
	return false
}

// --- Runtime auto-start toggle -------------------------------------------

func (h *Handler) handleRuntimeAutoStartToggle(w http.ResponseWriter, r *http.Request) {
	if h.runtimes == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "runtimes manager not available")
		return
	}
	kind := r.PathValue("kind")
	mf, err := h.runtimes.Get(kind)
	if err != nil {
		if errors.Is(err, runtimes.ErrRuntimeNotFound) {
			h.writeJSONErrorMsg(w, http.StatusNotFound, err.Error())
			return
		}
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, err.Error())
		return
	}
	target := !mf.AutoStart
	if err := h.runtimes.SetAutoStart(kind, target); err != nil {
		h.logger.Warn().Err(err).Str("runtime_name", kind).Msg("toggling auto-start")
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, err.Error())
		return
	}
	// SSE-patch the toggle button + sidebar dot in place.
	sse := datastar.NewSSE(w, r)
	if err := sse.PatchElements(templates.RenderRuntimeAutoStartButton(kind, target)); err != nil {
		h.logger.Debug().Err(err).Msg("SSE patch auto-start button")
	}
	if err := sse.PatchElements(templates.RenderRuntimeSidebarDot(kind, h.runtimes.IsRunning(kind), target)); err != nil {
		h.logger.Debug().Err(err).Msg("SSE patch sidebar dot")
	}
}

// --- Runtime live logs SSE -----------------------------------------------

// handleRuntimeLogsSSE streams gateway log lines for runtimeName as SSE.
// Filters the SystemLogBuffer for lines tagged with the runtime_name field
// (the hclog→zerolog adapter in runtimes/gateway.go applies this tag).
func (h *Handler) handleRuntimeLogsSSE(w http.ResponseWriter, r *http.Request) {
	kind := r.PathValue("kind")
	if h.sysLog == nil {
		http.Error(w, "log buffer unavailable", http.StatusServiceUnavailable)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	matchTag := `"runtime_name":"` + kind + `"`
	matchAlt := `runtime_name=` + kind
	matches := func(line string) bool {
		// Console-formatted lines wrap field names and values with ANSI
		// color escapes; strip them before substring matching so "key=value"
		// lookups still work. JSON file lines have no escapes — the same
		// pass is a no-op there.
		plain := stripANSI(line)
		return strings.Contains(plain, matchTag) || strings.Contains(plain, matchAlt)
	}

	// Replay buffered tail (last 200 matching lines).
	hist := h.sysLog.Lines()
	pickTail := make([]string, 0, 200)
	for _, line := range hist {
		if matches(line) {
			pickTail = append(pickTail, line)
			if len(pickTail) > 200 {
				pickTail = pickTail[1:]
			}
		}
	}
	for _, line := range pickTail {
		writeLogEvent(w, line)
	}
	flusher.Flush()

	// Subscribe for live appends.
	ch := h.sysLog.Subscribe()
	defer h.sysLog.Unsubscribe(ch)

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case line, ok := <-ch:
			if !ok {
				return
			}
			if !matches(line) {
				continue
			}
			writeLogEvent(w, line)
			flusher.Flush()
		case <-heartbeat.C:
			_, _ = io.WriteString(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

// ansiSGRRe matches ANSI SGR (color) escape sequences as written by
// zerolog.ConsoleWriter. Used to strip colors before substring filtering.
var ansiSGRRe = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// stripANSI returns s with ANSI SGR escape sequences removed.
func stripANSI(s string) string { return ansiSGRRe.ReplaceAllString(s, "") }

// writeLogEvent serializes one rendered log line as an SSE "log" event.
// Each event payload is a single-line string (newlines escaped) so the
// browser sees one frame per line.
func writeLogEvent(w io.Writer, raw string) {
	rendered := templates.RenderLogLine(raw)
	rendered = strings.ReplaceAll(rendered, "\r", "")
	rendered = strings.ReplaceAll(rendered, "\n", " ")
	_, _ = fmt.Fprintf(w, "event: log\ndata: %s\n\n", rendered)
}

// --- Scheduler tab --------------------------------------------------------

func (h *Handler) handleSchedulerDetail(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if key == "" {
		return
	}
	for _, item := range h.schedulerInstances() {
		if item.Key != key {
			continue
		}
		view := templates.SchedulerInstancePropsView{
			Key:         item.Key,
			ModelID:     item.ModelID,
			Filename:    item.Filename,
			Fingerprint: item.Fingerprint,
			WorkerID:    item.WorkerID,
			WorkerName:  item.WorkerName,
			RuntimeName: item.RuntimeName,
			DeviceID:    item.DeviceID,
			Source:      item.Source,
			Mode:        item.Mode,
			Status:      item.Status,
			PoolSize:    item.PoolSize,
			Active:      item.Active,
		}
		if err := templates.SchedulerInstancePropsPanel(view).Render(r.Context(), w); err != nil {
			h.logger.Warn().Err(err).Msg("rendering scheduler detail")
		}
		return
	}
}

func (h *Handler) handleSchedulerEvict(w http.ResponseWriter, r *http.Request) {
	if h.orch == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "scheduler not available")
		return
	}
	workerID := r.URL.Query().Get("worker")
	modelID := r.URL.Query().Get("model")
	if workerID == "" || modelID == "" {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "worker and model are required")
		return
	}
	for _, w2 := range h.workers.All() {
		sw, ok := w2.(*worker.StreamWorker)
		if !ok || sw.ID() != workerID {
			continue
		}
		if err := sw.UnloadModel(modelID); err != nil {
			h.writeJSONErrorMsg(w, http.StatusInternalServerError, err.Error())
			return
		}
		// Push a Scheduler-tab refresh straight away — the next heartbeat
		// would also wake subscribers via the loaded-set diff, but that's
		// seconds away and the operator clicked the button just now.
		h.workers.NotifyLoadedChanged(sw.ID())
		w.WriteHeader(http.StatusNoContent)
		return
	}
	h.writeJSONErrorMsg(w, http.StatusNotFound, "worker not found")
}

// --- Scheduler tab live updates ------------------------------------------

// handleSchedulerEventsSSE pushes a single "change" event whenever the
// loaded-model set on any worker actually moves (heartbeat-detected diff,
// or an explicit Load/Unload notify). JS reacts by re-fetching
// /api/scheduler/list. Mirrors the Workers tab's stats/change pattern but
// without the periodic stats tick — Scheduler-tab values only move when
// load/active counters change, so polling on a timer is wasted work.
func (h *Handler) handleSchedulerEventsSSE(w http.ResponseWriter, r *http.Request) {
	if h.schedulerBroker == nil || h.workers == nil {
		http.Error(w, "scheduler SSE unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ch := h.schedulerBroker.Subscribe()
	defer h.schedulerBroker.Unsubscribe(ch)

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case _, ok := <-ch:
			if !ok {
				return
			}
			if _, err := fmt.Fprintf(w, "event: change\ndata: \n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// --- Settings auto-save ---------------------------------------------------

// settingsPostBody mirrors the Datastar signals the Settings tab binds to.
type settingsPostBody struct {
	ListenAddr      string `json:"listenAddr"`
	DataDir         string `json:"dataDir"`
	ResultTTL       string `json:"resultTTL"`
	IdleEvictionTTL string `json:"idleEvictionTTL"`
	RegistryURL     string `json:"registryURL"`
	LogLevel        string `json:"logLevel"`
	AuthToken       string `json:"authToken"`
	TLSEnabled      bool   `json:"tlsEnabled"`
	TLSCertFile     string `json:"tlsCertFile"`
	DevMode         bool   `json:"devMode"`
	Theme           string `json:"theme"`
}

func (h *Handler) handleSettingsSave(w http.ResponseWriter, r *http.Request) {
	var body settingsPostBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "bad body: "+err.Error())
		return
	}
	h.cfg.ListenAddr = strings.TrimSpace(body.ListenAddr)
	h.cfg.DataDir = strings.TrimSpace(body.DataDir)
	h.cfg.ResultTTL = strings.TrimSpace(body.ResultTTL)
	h.cfg.IdleEvictionTTL = strings.TrimSpace(body.IdleEvictionTTL)
	h.cfg.RegistryURL = strings.TrimSpace(body.RegistryURL)
	h.cfg.DevMode = body.DevMode
	h.cfg.TLS.Enabled = body.TLSEnabled
	h.cfg.TLS.CertFile = strings.TrimSpace(body.TLSCertFile)

	if body.LogLevel != "" {
		var lvl config.LogLevel
		if err := lvl.UnmarshalText([]byte(body.LogLevel)); err == nil {
			h.cfg.Logger.Level = lvl
		}
	}

	// Auth token: empty = unchanged unless the user explicitly cleared an
	// edited value (handled by the Edited signal client-side, which sends
	// empty only on intentional clear). We treat any literal empty as
	// "clear"; bullet placeholder is never POSTed because the client wipes
	// the value on focus.
	if body.AuthToken != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(body.AuthToken), bcrypt.DefaultCost)
		if err != nil {
			h.writeJSONErrorMsg(w, http.StatusInternalServerError, "hashing auth token: "+err.Error())
			return
		}
		if err := h.store.SetSetting("auth_token", string(hash)); err != nil {
			h.writeJSONErrorMsg(w, http.StatusInternalServerError, "storing auth token: "+err.Error())
			return
		}
		h.SetAuthHash(hash)
	}

	if h.saveFn != nil {
		h.saveFn()
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- System log sync (focus-regain) --------------------------------------

func (h *Handler) handleSyncLogs(w http.ResponseWriter, r *http.Request) {
	type syncResponse struct {
		SysLog string `json:"sysLog"`
	}
	var resp syncResponse
	if h.sysLog != nil {
		var sb strings.Builder
		for _, line := range h.sysLog.Lines() {
			sb.WriteString(templates.RenderSystemLogLine(line))
		}
		resp.SysLog = sb.String()
	}
	h.writeJSON(w, http.StatusOK, resp)
}

// --- System log SSE (live tail in Settings tab) --------------------------

func (h *Handler) handleSysLogsSSE(w http.ResponseWriter, r *http.Request) {
	if h.sysLog == nil {
		http.Error(w, "log buffer unavailable", http.StatusServiceUnavailable)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	for _, line := range h.sysLog.Lines() {
		writeLogEvent(w, line)
	}
	flusher.Flush()

	ch := h.sysLog.Subscribe()
	defer h.sysLog.Unsubscribe(ch)
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case line, ok := <-ch:
			if !ok {
				return
			}
			writeLogEvent(w, line)
			flusher.Flush()
		case <-heartbeat.C:
			_, _ = io.WriteString(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

// --- Workers tab live updates + benchmark dispatch -----------------------

// statsEvent is the JSON payload of a "stats" SSE frame.
type statsEvent struct {
	Workers []workerStatsView `json:"workers"`
}

type workerStatsView struct {
	WorkerID string             `json:"worker_id"`
	Online   bool               `json:"online"`
	Stats    []deviceStatsEntry `json:"stats"`
}

type deviceStatsEntry struct {
	DeviceID       string  `json:"device_id"`
	UsedMemoryMB   int     `json:"used_mb"`
	TotalMemoryMB  int     `json:"total_mb"`
	UtilizationPct float64 `json:"util_pct"`
}

// handleWorkersEventsSSE pushes live updates to the Workers tab. Two event
// kinds are emitted: "stats" every 2s (gauge values; JS updates rings in
// place) and "change" whenever the fleet shape changes (list refetch).
func (h *Handler) handleWorkersEventsSSE(w http.ResponseWriter, r *http.Request) {
	if h.workersBroker == nil || h.workers == nil {
		http.Error(w, "workers SSE unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ch := h.workersBroker.Subscribe()
	defer h.workersBroker.Unsubscribe(ch)

	stats := time.NewTicker(2 * time.Second)
	defer stats.Stop()

	// Initial stats frame so the browser doesn't wait up to 2s for first
	// gauge values. No initial "change" — the list HTML was just fetched
	// via /api/workers/list, refetching now would collapse open cards.
	h.sendWorkersStats(w, flusher)

	for {
		select {
		case <-r.Context().Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			switch evt.Kind {
			case WorkersEventStats:
				h.sendWorkersStats(w, flusher)
			case WorkersEventChange:
				h.sendWorkersEvent(w, flusher, "change", "")
			}
		case <-stats.C:
			h.sendWorkersStats(w, flusher)
		}
	}
}

func (h *Handler) sendWorkersStats(w io.Writer, f http.Flusher) {
	all := h.workers.All()
	out := statsEvent{Workers: make([]workerStatsView, 0, len(all))}
	for _, wkr := range all {
		stat := wkr.Stats()
		entries := make([]deviceStatsEntry, len(stat))
		for i, s := range stat {
			entries[i] = deviceStatsEntry{
				DeviceID:       s.DeviceID,
				UsedMemoryMB:   s.UsedMemoryMB,
				TotalMemoryMB:  s.TotalMemoryMB,
				UtilizationPct: s.UtilizationPct,
			}
		}
		out.Workers = append(out.Workers, workerStatsView{
			WorkerID: wkr.ID(),
			Online:   wkr.Status().Online,
			Stats:    entries,
		})
	}
	body, err := json.Marshal(out)
	if err != nil {
		h.logger.Warn().Err(err).Msg("encoding workers stats event")
		return
	}
	h.sendWorkersEvent(w, f, "stats", string(body))
}

func (h *Handler) sendWorkersEvent(w io.Writer, f http.Flusher, kind, data string) {
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", kind, data); err != nil {
		return
	}
	f.Flush()
}

// benchResult is one device's benchmark outcome, returned in the
// /api/workers/benchmark JSON response.
type benchResult struct {
	WorkerID      string  `json:"worker_id"`
	DeviceID      string  `json:"device_id"`
	DeviceName    string  `json:"device_name"`
	MemoryGBs     float64 `json:"memory_gbs"`
	ComputeGFlops float64 `json:"compute_gflops"`
	Error         string  `json:"error,omitempty"`
}

// handleWorkersBenchmark runs a benchmark on the targeted workers/devices,
// persists results, and broadcasts a "change" event so every connected
// browser refreshes its Workers tab.
//
// Query params:
//   - workerIds: comma-separated worker IDs; empty = every online worker.
//   - deviceIds: comma-separated device IDs; empty = every device on the
//     selected workers.
func (h *Handler) handleWorkersBenchmark(w http.ResponseWriter, r *http.Request) {
	if h.workers == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "no worker fleet")
		return
	}
	wantWorkers := parseCSVSet(r.URL.Query().Get("workerIds"))
	wantDevices := parseCSVSet(r.URL.Query().Get("deviceIds"))

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

	h.workersBroker.Broadcast(WorkersEvent{Kind: WorkersEventChange})

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"results": results}); err != nil {
		h.logger.Warn().Err(err).Msg("encoding benchmark response")
	}
}

// autoBenchmarkWorker benches any devices on wkr that don't yet have a
// stored benchmark row. Intended to run in a goroutine off the fleet-add
// callback — every newly-connected worker gets benched once on first sight.
// Skipping already-benched devices avoids re-benching on every reconnect:
// numbers don't meaningfully change between worker restarts.
func (h *Handler) autoBenchmarkWorker(workerID string) {
	if h.workers == nil || h.store == nil {
		return
	}
	wkr := h.workers.Get(workerID)
	if wkr == nil || !wkr.Status().Online {
		return
	}
	missing := map[string]bool{}
	for _, d := range safeDevices(wkr) {
		has, err := h.store.HasBenchmark(workerID, d.ID)
		if err != nil {
			h.logger.Warn().Err(err).Str("worker", workerID).Str("device", d.ID).Msg("checking benchmark existence")
			continue
		}
		if !has {
			missing[d.ID] = true
		}
	}
	if len(missing) == 0 {
		return
	}
	h.logger.Info().Str("worker", workerID).Int("devices", len(missing)).Msg("auto-benchmarking devices missing stored results")

	var (
		mu      sync.Mutex
		results []benchResult
	)
	h.benchmarkWorker(wkr, missing, &mu, &results)
	if h.workersBroker != nil {
		h.workersBroker.Broadcast(WorkersEvent{Kind: WorkersEventChange})
	}
}

// benchmarkWorker benches every targeted device on wkr sequentially (devices
// share the host). Persists each result; failures are recorded as rows with
// a populated Error field.
func (h *Handler) benchmarkWorker(wkr worker.WorkerInterface, wantDevices map[string]bool, mu *sync.Mutex, out *[]benchResult) {
	for _, dev := range safeDevices(wkr) {
		if len(wantDevices) > 0 && !wantDevices[dev.ID] {
			continue
		}
		h.logger.Info().Str("worker", wkr.ID()).Str("device", dev.ID).Msg("benchmark start")
		res, err := wkr.Bench(dev.ID)
		if err != nil {
			h.logger.Warn().Err(err).Str("worker", wkr.ID()).Str("device", dev.ID).Msg("benchmark failed")
			mu.Lock()
			*out = append(*out, benchResult{
				WorkerID: wkr.ID(),
				DeviceID: dev.ID,
				Error:    err.Error(),
			})
			mu.Unlock()
			continue
		}
		if err := h.store.SaveBenchmark(store.BenchmarkRow{
			WorkerID:      wkr.ID(),
			DeviceID:      res.DeviceID,
			DeviceName:    res.DeviceName,
			MemoryGBs:     res.MemoryGBs,
			ComputeGFlops: res.ComputeGFlops,
			BenchedAt:     res.BenchedAt,
		}); err != nil {
			h.logger.Warn().Err(err).Str("worker", wkr.ID()).Str("device", dev.ID).Msg("saving benchmark")
		}
		mu.Lock()
		*out = append(*out, benchResult{
			WorkerID:      wkr.ID(),
			DeviceID:      res.DeviceID,
			DeviceName:    res.DeviceName,
			MemoryGBs:     res.MemoryGBs,
			ComputeGFlops: res.ComputeGFlops,
		})
		mu.Unlock()
	}
}

// handleToggleDevice flips the operator-controlled enable flag for one
// (worker, device) pair, persists it, pushes the new whitelist to the
// worker, and broadcasts a "change" event so every browser refreshes.
//
// Disabling a device only affects future model loads — already-loaded
// models stay where they are. If a model can't fit on the remaining
// enabled devices it will fail to load with a normal error.
func (h *Handler) handleToggleDevice(w http.ResponseWriter, r *http.Request) {
	if h.workers == nil || h.store == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "no worker fleet")
		return
	}
	workerID := r.PathValue("id")
	deviceID := r.PathValue("devID")
	if workerID == "" || deviceID == "" {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "worker id and device id required")
		return
	}
	wkr := h.workers.Get(workerID)
	if wkr == nil {
		h.writeJSONErrorMsg(w, http.StatusNotFound, "worker not found")
		return
	}

	current := true
	if row, err := h.store.GetWorkerDeviceEnabled(workerID, deviceID); err == nil {
		current = row.Enabled
	} else if !errors.Is(err, sql.ErrNoRows) {
		h.logger.Warn().Err(err).Str("worker", workerID).Str("device", deviceID).Msg("reading device enabled state")
	}
	next := !current
	if err := h.store.SetWorkerDeviceEnabled(workerID, deviceID, next); err != nil {
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, "persisting toggle: "+err.Error())
		return
	}

	if err := h.pushEnabledDevices(wkr); err != nil {
		h.logger.Warn().Err(err).Str("worker", workerID).Msg("pushing enabled devices to worker")
	}
	h.workersBroker.Broadcast(WorkersEvent{Kind: WorkersEventChange})
	w.WriteHeader(http.StatusNoContent)
}

// handleToggleWorker flips every device on the worker in one transaction.
// "Worker enabled" is derived: any device enabled = worker enabled. If
// the worker is currently fully or partially enabled, the toggle disables
// every device; otherwise it enables every non-CPU device. CPU stays
// always-enabled regardless.
func (h *Handler) handleToggleWorker(w http.ResponseWriter, r *http.Request) {
	if h.workers == nil || h.store == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "no worker fleet")
		return
	}
	workerID := r.PathValue("id")
	if workerID == "" {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "worker id required")
		return
	}
	wkr := h.workers.Get(workerID)
	if wkr == nil {
		h.writeJSONErrorMsg(w, http.StatusNotFound, "worker not found")
		return
	}

	devices := safeDevices(wkr)
	if len(devices) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	enabledByDev := map[string]bool{}
	rows, err := h.store.ListWorkerDevicesEnabled(workerID)
	if err != nil {
		h.logger.Warn().Err(err).Str("worker", workerID).Msg("listing device enabled state")
	}
	for _, r := range rows {
		enabledByDev[r.DeviceID] = r.Enabled
	}
	anyEnabled := false
	for _, d := range devices {
		v, ok := enabledByDev[d.ID]
		if !ok || v {
			anyEnabled = true
			break
		}
	}

	next := !anyEnabled
	toUpdate := make([]string, 0, len(devices))
	for _, d := range devices {
		toUpdate = append(toUpdate, d.ID)
	}
	if err := h.store.SetWorkerDevicesEnabledBulk(workerID, toUpdate, next); err != nil {
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, "persisting toggle: "+err.Error())
		return
	}

	if err := h.pushEnabledDevices(wkr); err != nil {
		h.logger.Warn().Err(err).Str("worker", workerID).Msg("pushing enabled devices to worker")
	}
	h.workersBroker.Broadcast(WorkersEvent{Kind: WorkersEventChange})
	w.WriteHeader(http.StatusNoContent)
}

// pushEnabledDevices computes the current enabled-device whitelist for
// wkr and sends it to the worker via the bidi stream. Used by both
// toggle handlers and the post-Register handshake (via main.go's
// EnabledDevicesProvider closure).
func (h *Handler) pushEnabledDevices(wkr worker.WorkerInterface) error {
	sw, ok := wkr.(*worker.StreamWorker)
	if !ok {
		return nil
	}
	devices := safeDevices(wkr)
	enabled := h.computeEnabledDeviceIDs(wkr.ID(), devices)
	return sw.SetEnabledDevices(enabled)
}

// computeEnabledDeviceIDs returns the device IDs the operator has not
// disabled. Absent persisted rows mean "enabled" (sane default for newly-
// connected workers without any toggle history).
func (h *Handler) computeEnabledDeviceIDs(workerID string, devices []stats.Device) []string {
	enabledByDev := map[string]bool{}
	if rows, err := h.store.ListWorkerDevicesEnabled(workerID); err == nil {
		for _, r := range rows {
			enabledByDev[r.DeviceID] = r.Enabled
		}
	}
	out := make([]string, 0, len(devices))
	for _, d := range devices {
		v, ok := enabledByDev[d.ID]
		if !ok || v {
			out = append(out, d.ID)
		}
	}
	return out
}

// parseCSVSet splits a comma-separated query value into a non-empty set.
func parseCSVSet(s string) map[string]bool {
	out := map[string]bool{}
	for _, v := range strings.Split(s, ",") {
		v = strings.TrimSpace(v)
		if v != "" {
			out[v] = true
		}
	}
	return out
}

// --- helpers -------------------------------------------------------------

// renderRuntimeRow re-renders a single runtime row's actions area. Helper
// retained for future SSE patches; not currently called from this file.
func renderRuntimeRow(kind string, running bool) string {
	return templates.RenderRuntimeRowActions(kind, running)
}

// _ keeps ctxerr referenced in the package even when other files come/go.
var _ = ctxerr.With
var _ = html.EscapeString
var _ = context.Background
var _ = renderRuntimeRow
