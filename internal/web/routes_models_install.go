package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chinese-room-solutions/mass-sdk/huggingface"
	"github.com/chinese-room-solutions/mass/internal/downloads"
	"github.com/chinese-room-solutions/mass/internal/runtimes"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// hfPageSize matches the pre-refactor "Show More" page size.
const hfPageSize = 5

// errNoRunningGateway is returned when no runtime gateway is up. Install
// surfaces (HF search, Browse Local) refuse to operate in that state.
var errNoRunningGateway = errors.New("no runtime gateway is running - start one in the Runtimes tab")

// errRuntimeNotRunning is returned when the operator-selected runtime kind
// isn't currently running.
var errRuntimeNotRunning = errors.New("selected runtime is not running")

// requireRunningGateway returns the running gateway matching kind (or any,
// when kind is empty). Used by HF search routes to enforce that at least
// one runtime is up before reaching out to the registry.
func (h *Handler) requireRunningGateway(kind string) error {
	if h.runtimes == nil {
		return errNoRunningGateway
	}
	running := h.runtimes.RunningGateways()
	if len(running) == 0 {
		return errNoRunningGateway
	}
	if kind == "" {
		return nil
	}
	for _, g := range running {
		if g.RuntimeName() == kind {
			return nil
		}
	}
	return errRuntimeNotRunning
}

// installRequest is the JSON body for POST /api/models/install.
type installRequest struct {
	Source      string `json:"source"` // "huggingface"
	RepoID      string `json:"repo_id"`
	Filename    string `json:"filename"`
	RuntimeName string `json:"runtime_name"` // optional; defaults to first running gateway
	Name        string `json:"name"`         // operator-typed model display label, required
}

// installResponse echoes the planned files so the UI can render rows
// immediately without waiting for the first SSE progress frame.
type installResponse struct {
	Files []installResponseFile `json:"files"`
}

type installResponseFile struct {
	RelPath   string `json:"rel_path"`
	SizeBytes int64  `json:"size_bytes"`
	GroupKey  string `json:"group_key"`
}

// handleInstallModel is the entry point for the Install dialog's per-file
// download buttons. It asks the chosen gateway to plan the file set
// (primary + companions) and queues each entry on the downloads.Manager.
func (h *Handler) handleInstallModel(w http.ResponseWriter, r *http.Request) {
	if h.downloads == nil || h.runtimes == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "downloads not available")
		return
	}
	var req installRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "bad body: "+err.Error())
		return
	}
	if req.Source == "" {
		req.Source = "huggingface"
	}
	if req.RepoID == "" || req.Filename == "" {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "repo_id and filename are required")
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "name is required")
		return
	}

	gw, err := h.pickGatewayForInstall(req.RuntimeName)
	if err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, err.Error())
		return
	}

	plan, err := gw.PlanModelFiles(r.Context(), req.Source, req.RepoID, req.Filename, strings.TrimSpace(req.Name))
	if err != nil {
		h.logger.Warn().Err(err).Str("repo_id", req.RepoID).Str("filename", req.Filename).Msg("plan model files")
		h.writeJSONErrorMsg(w, http.StatusBadGateway, "plan failed: "+err.Error())
		return
	}
	if len(plan) == 0 {
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, "gateway returned empty plan")
		return
	}

	groupKey := gw.RuntimeName() + ":" + req.RepoID + ":" + req.Filename

	out := installResponse{Files: make([]installResponseFile, 0, len(plan))}
	for _, f := range plan {
		// f.RelPath is the gateway-emitted canonical path under
		// models_dir, already rooted at the shared format directory.
		// f.GroupLabel is the operator-typed Name shared across the
		// bundle so all in-flight rows cluster.
		spec := downloads.Job{
			RelPath:     f.GetRelPath(),
			URL:         f.GetUrl(),
			Source:      req.Source,
			RepoID:      req.RepoID,
			RuntimeName: gw.RuntimeName(),
			GroupKey:    groupKey,
			GroupName:   f.GetGroupLabel(),
			Filename:    filepath.Base(f.GetRelPath()),
			Status:      store.DownloadStatusActive,
			Total:       f.GetSizeBytes(),
		}
		if err := h.downloads.Start(spec); err != nil {
			if errors.Is(err, downloads.ErrAlreadyExists) || errors.Is(err, downloads.ErrAlreadyDone) {
				continue // companion already in flight or already on disk — fine
			}
			h.logger.Warn().Err(err).Str("rel_path", spec.RelPath).Msg("starting download")
			h.writeJSONErrorMsg(w, http.StatusInternalServerError, err.Error())
			return
		}
		out.Files = append(out.Files, installResponseFile{
			RelPath:   spec.RelPath,
			SizeBytes: spec.Total,
			GroupKey:  groupKey,
		})
	}
	h.writeJSON(w, http.StatusOK, out)
}

// pickGatewayForInstall picks the running gateway that should plan
// the install. When kindHint is set the matching gateway is
// required; otherwise the first running gateway wins. The gateway
// itself rejects unrecognised inputs at PlanModelFiles time, so MASS
// holds no filename or extension knowledge here.
func (h *Handler) pickGatewayForInstall(kindHint string) (*runtimes.LoadedGateway, error) {
	running := h.runtimes.RunningGateways()
	if len(running) == 0 {
		return nil, errNoRunningGateway
	}
	if kindHint != "" {
		for _, gw := range running {
			if gw.RuntimeName() == kindHint {
				return gw, nil
			}
		}
		return nil, fmt.Errorf("runtime %q is not running", kindHint)
	}
	return running[0], nil
}

// downloadActionRequest is the JSON body for pause/resume/cancel.
type downloadActionRequest struct {
	RelPath string `json:"rel_path"`
}

func (h *Handler) handleDownloadPause(w http.ResponseWriter, r *http.Request) {
	if h.downloads == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "downloads not available")
		return
	}
	var req downloadActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "bad body")
		return
	}
	if err := h.downloads.Pause(req.RelPath); err != nil {
		if errors.Is(err, downloads.ErrNotFound) {
			h.writeJSONErrorMsg(w, http.StatusNotFound, err.Error())
			return
		}
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleDownloadResume(w http.ResponseWriter, r *http.Request) {
	if h.downloads == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "downloads not available")
		return
	}
	var req downloadActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "bad body")
		return
	}
	if err := h.downloads.Resume(req.RelPath); err != nil {
		switch {
		case errors.Is(err, downloads.ErrNotFound):
			h.writeJSONErrorMsg(w, http.StatusNotFound, err.Error())
		case errors.Is(err, downloads.ErrNotPaused):
			h.writeJSONErrorMsg(w, http.StatusBadRequest, err.Error())
		default:
			h.writeJSONErrorMsg(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleDownloadCancel(w http.ResponseWriter, r *http.Request) {
	if h.downloads == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "downloads not available")
		return
	}
	var req downloadActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "bad body")
		return
	}
	if err := h.downloads.Cancel(req.RelPath); err != nil {
		if errors.Is(err, downloads.ErrNotFound) {
			h.writeJSONErrorMsg(w, http.StatusNotFound, err.Error())
			return
		}
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleImportLocalModel copies the file the operator picked plus any
// auto-discovered companions (e.g. a sibling vision projector) into
// MASS's models store. The runtime owns the discovery + canonical-name
// computation: MASS calls PlanLocalImport with the picked path, gets
// back a list of [DownloadFile] entries (each with absolute file://
// URL + canonical relPath under models_dir), and copies bytes for
// every one. MASS does not parse the file or know what counts as a
// "variant" / "companion". The Name field is required and becomes the
// group label + bundle key linking every file in this install.
type importRequest struct {
	Path        string `json:"path"`
	RuntimeName string `json:"runtime_name"`
	Name        string `json:"name"`
}

type importResponseFile struct {
	RelPath string `json:"rel_path"`
}

func (h *Handler) handleImportLocalModel(w http.ResponseWriter, r *http.Request) {
	if h.downloads == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "downloads not available")
		return
	}
	var req importRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "bad body")
		return
	}
	if strings.TrimSpace(req.Path) == "" {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "path is required")
		return
	}
	if strings.TrimSpace(req.RuntimeName) == "" {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "runtime_name is required")
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "name is required")
		return
	}
	gw, err := h.runtimes.LoadedGatewayFor(req.RuntimeName)
	if err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "runtime not running: "+req.RuntimeName)
		return
	}
	files, err := gw.PlanLocalImport(r.Context(), req.Path, strings.TrimSpace(req.Name))
	if err != nil {
		// Surface gRPC AlreadyExists (destination filename already
		// occupied on disk) as HTTP 409 with the gateway's own
		// message — strips the rpc-error wrapping the operator
		// doesn't care about.
		if st, ok := status.FromError(err); ok && st.Code() == codes.AlreadyExists {
			h.writeJSONErrorMsg(w, http.StatusConflict, st.Message())
			return
		}
		h.writeJSONErrorMsg(w, http.StatusBadRequest, err.Error())
		return
	}
	out := make([]importResponseFile, 0, len(files))
	for _, f := range files {
		// Each DownloadFile carries the gateway-assigned relPath plus
		// a file:// URL pointing at the source on disk. Strip the
		// scheme so ImportLocal sees a plain path.
		srcPath := strings.TrimPrefix(f.GetUrl(), "file://")
		rel, err := h.downloads.ImportLocal(srcPath, f.GetRelPath(), req.RuntimeName, downloads.LocalImportLabels{
			GroupName: f.GetGroupLabel(),
			Filename:  filepath.Base(f.GetRelPath()),
		})
		if err != nil {
			switch {
			case errors.Is(err, downloads.ErrAlreadyExists):
				h.writeJSONErrorMsg(w, http.StatusConflict, "import already in progress: "+filepath.Base(srcPath))
			case errors.Is(err, downloads.ErrAlreadyDone):
				h.writeJSONErrorMsg(w, http.StatusConflict, "model already exists at destination: "+f.GetRelPath())
			default:
				h.writeJSONErrorMsg(w, http.StatusBadRequest, err.Error())
			}
			return
		}
		out = append(out, importResponseFile{RelPath: rel})
	}
	h.writeJSON(w, http.StatusOK, map[string]any{"files": out})
}

// handleListModelNames returns the distinct set of model names
// already installed across every running gateway, sorted case-
// insensitively. Powers the install dialog's name-field autocomplete.
func (h *Handler) handleListGroupNames(w http.ResponseWriter, r *http.Request) {
	if h.runtimes == nil {
		h.writeJSON(w, http.StatusOK, []string{})
		return
	}
	seen := map[string]struct{}{}
	for _, gw := range h.runtimes.RunningGateways() {
		groups, err := gw.ListGroups(r.Context())
		if err != nil {
			h.logger.Debug().Err(err).Str("runtime", gw.RuntimeName()).Msg("listing groups for autocomplete")
			continue
		}
		for _, g := range groups {
			if name := strings.TrimSpace(g.GetDisplayName()); name != "" {
				seen[name] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i]) < strings.ToLower(out[j])
	})
	h.writeJSON(w, http.StatusOK, out)
}

// renameGroupRequest is the JSON body for POST /api/groups/rename.
type renameGroupRequest struct {
	RuntimeName string `json:"runtime_name"`
	ID          string `json:"id"`       // Group.id (slug) the gateway returned in ListGroups
	NewName     string `json:"new_name"` // operator-typed replacement
}

func (h *Handler) handleRenameGroup(w http.ResponseWriter, r *http.Request) {
	var req renameGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "bad body")
		return
	}
	if strings.TrimSpace(req.RuntimeName) == "" || strings.TrimSpace(req.ID) == "" || strings.TrimSpace(req.NewName) == "" {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "runtime_name, id and new_name are required")
		return
	}
	gw, err := h.runtimes.LoadedGatewayFor(req.RuntimeName)
	if err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "runtime not running: "+req.RuntimeName)
		return
	}
	if err := gw.RenameGroup(r.Context(), req.ID, strings.TrimSpace(req.NewName)); err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.NotFound:
				h.writeJSONErrorMsg(w, http.StatusNotFound, st.Message())
				return
			case codes.AlreadyExists:
				h.writeJSONErrorMsg(w, http.StatusConflict, st.Message())
				return
			}
		}
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Wake the Models SSE renderer so the card re-renders with the new
	// slug + display name. Without this the optimistic JS update keeps
	// the stale id and a follow-up rename targets the wrong group.
	h.runtimes.FireStateChange(req.RuntimeName)
	w.WriteHeader(http.StatusNoContent)
}

// --- HF search ------------------------------------------------------------

// hfSearchState holds server-side pagination state for one HF search.
type hfSearchState struct {
	query      string
	shownIDs   []string
	nextCursor string
	hasMore    bool
}

// hfSearch keys per-session search state by search query. Single-process
// state is fine — every dashboard hit terminates at the same MASS instance.
var (
	hfSearchMu     sync.Mutex
	hfSearchState_ = map[string]*hfSearchState{}
)

type hfSearchRequest struct {
	Query       string `json:"query"`
	RuntimeName string `json:"runtime_name"`
}

// handleSearchHF runs a fresh HF search and returns rendered result rows.
func (h *Handler) handleSearchHF(w http.ResponseWriter, r *http.Request) {
	var req hfSearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "bad body")
		return
	}
	q := strings.TrimSpace(req.Query)
	if q == "" {
		h.writeHTML(w, templates.HFSearchEmpty())
		return
	}

	if err := h.requireRunningGateway(req.RuntimeName); err != nil {
		h.writeHTML(w, templates.HFSearchError(err.Error()))
		return
	}

	res, err := huggingface.Search(r.Context(), q, huggingface.SearchOptions{
		Limit: hfPageSize,
	})
	if err != nil {
		h.logger.Warn().Err(err).Str("query", q).Msg("HF search failed")
		h.writeHTML(w, templates.HFSearchError(err.Error()))
		return
	}

	state := &hfSearchState{query: q, nextCursor: res.NextCursor, hasMore: res.HasMore}
	for _, m := range res.Models {
		state.shownIDs = append(state.shownIDs, m.RepoID)
	}
	hfSearchMu.Lock()
	hfSearchState_[q] = state
	hfSearchMu.Unlock()

	h.writeHTML(w, templates.HFSearchResults(res.Models, q, res.HasMore))
}

type hfMoreRequest struct {
	Query       string `json:"query"`
	RuntimeName string `json:"runtime_name"`
}

func (h *Handler) handleSearchHFMore(w http.ResponseWriter, r *http.Request) {
	var req hfMoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "bad body")
		return
	}
	q := strings.TrimSpace(req.Query)
	if q == "" {
		h.writeHTML(w, "")
		return
	}
	hfSearchMu.Lock()
	state := hfSearchState_[q]
	hfSearchMu.Unlock()
	if state == nil || !state.hasMore {
		h.writeHTML(w, "")
		return
	}

	if err := h.requireRunningGateway(req.RuntimeName); err != nil {
		h.writeHTML(w, templates.HFSearchError(err.Error()))
		return
	}

	res, err := huggingface.Search(r.Context(), q, huggingface.SearchOptions{
		Limit:      hfPageSize,
		Cursor:     state.nextCursor,
		ExcludeIDs: state.shownIDs,
	})
	if err != nil {
		h.writeHTML(w, templates.HFSearchError(err.Error()))
		return
	}
	for _, m := range res.Models {
		state.shownIDs = append(state.shownIDs, m.RepoID)
	}
	state.nextCursor = res.NextCursor
	state.hasMore = res.HasMore
	hfSearchMu.Lock()
	hfSearchState_[q] = state
	hfSearchMu.Unlock()

	h.writeHTML(w, templates.HFSearchAppendRows(res.Models, q, res.HasMore))
}

// --- Downloads SSE --------------------------------------------------------

func (h *Handler) handleDownloadsEventsSSE(w http.ResponseWriter, r *http.Request) {
	if h.downloads == nil {
		http.Error(w, "downloads SSE unavailable", http.StatusServiceUnavailable)
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
	// Initial snapshot so a freshly-opened tab sees current rows immediately.
	snapshot := h.downloads.List()
	for _, j := range snapshot {
		writeDownloadEvent(w, downloads.Event{
			RelPath:    j.RelPath,
			GroupKey:   j.GroupKey,
			Status:     j.Status,
			Downloaded: j.Downloaded,
			Total:      j.Total,
			ErrorMsg:   j.ErrorMsg,
		})
	}
	flusher.Flush()

	ch := h.downloads.Subscribe()
	defer h.downloads.Unsubscribe(ch)
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			writeDownloadEvent(w, evt)
			flusher.Flush()
		case <-heartbeat.C:
			_, _ = io.WriteString(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

func writeDownloadEvent(w io.Writer, evt downloads.Event) {
	body, err := json.Marshal(evt)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.Status, body)
}

// writeHTML is a small helper for template-rendered endpoints. Always
// writes 200 OK — these endpoints surface errors as inline HTML rather
// than HTTP error codes (the dashboard renders the response body as-is).
func (h *Handler) writeHTML(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := io.WriteString(w, body); err != nil {
		h.logger.Debug().Err(err).Msg("writing html response")
	}
}
