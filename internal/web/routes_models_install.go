package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/chinese-room-solutions/mass/internal/audit"
	"github.com/chinese-room-solutions/mass/internal/downloads"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	audit.Log(h.logger, "download.paused", req.RelPath, audit.OutcomeOK).
		Str("actor", actorFromRequest(r)).Msg("")
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
	audit.Log(h.logger, "download.resumed", req.RelPath, audit.OutcomeOK).
		Str("actor", actorFromRequest(r)).Msg("")
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
	audit.Log(h.logger, "download.cancelled", req.RelPath, audit.OutcomeOK).
		Str("actor", actorFromRequest(r)).Msg("")
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
	var req importRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "bad body")
		return
	}
	rels, err := h.importLocalModel(r.Context(), req.RuntimeName, req.Path, req.Name, actorFromRequest(r))
	if err != nil {
		status, msg := modelOpHTTPStatus(err)
		h.writeJSONErrorMsg(w, status, msg)
		return
	}
	out := make([]importResponseFile, 0, len(rels))
	for _, rel := range rels {
		out = append(out, importResponseFile{RelPath: rel})
	}
	h.writeJSON(w, http.StatusOK, map[string]any{"files": out})
}

// deleteModelRequest is the JSON body for the dashboard delete adapter.
type deleteModelRequest struct {
	RuntimeName string `json:"runtime_name"`
	ID          string `json:"id"`
}

// handleDeleteModel is the dashboard adapter over the shared delete core: the
// runtime decides which files make up the model, MASS removes them. Replaces
// the old browser-direct DELETE to the gateway proxy so byte removal stays a
// MASS operation.
func (h *Handler) handleDeleteModel(w http.ResponseWriter, r *http.Request) {
	var req deleteModelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "bad body")
		return
	}
	if _, err := h.deleteModel(r.Context(), req.RuntimeName, req.ID, actorFromRequest(r)); err != nil {
		status, msg := modelOpHTTPStatus(err)
		h.writeJSONErrorMsg(w, status, msg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// modelOpHTTPStatus maps a modelops sentinel error to an HTTP status + message
// for the dashboard /api/* adapters.
func modelOpHTTPStatus(err error) (int, string) {
	switch {
	case errors.Is(err, ErrModelOpConflict), errors.Is(err, ErrModelOpBusy):
		return http.StatusConflict, err.Error()
	case errors.Is(err, ErrModelOpInvalid), errors.Is(err, ErrModelOpRuntimeDown):
		return http.StatusBadRequest, err.Error()
	case errors.Is(err, ErrModelOpUnavailable):
		return http.StatusServiceUnavailable, err.Error()
	default:
		return http.StatusInternalServerError, err.Error()
	}
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
	audit.Log(h.logger, "group.renamed", req.ID, audit.OutcomeOK).
		Str("actor", actorFromRequest(r)).
		Str("runtime", req.RuntimeName).
		Str("new_name", req.NewName).Msg("")
	// Wake the Models SSE renderer so the card re-renders with the new
	// slug + display name. Without this the optimistic JS update keeps
	// the stale id and a follow-up rename targets the wrong group.
	h.runtimes.FireStateChange(req.RuntimeName)
	w.WriteHeader(http.StatusNoContent)
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
	heartbeat := newHeartbeatTicker()
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
