package web

import (
	"errors"
	"net/http"

	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
)

// handleQueueList renders the Queue tab body — one section per queue
// (global + each active worker queue) with peekable unleased rows.
func (h *Handler) handleQueueList(w http.ResponseWriter, r *http.Request) {
	sections, err := h.queueSnapshot(r.Context())
	if err != nil {
		if errors.Is(err, ErrOpUnavailable) {
			http.Error(w, "scheduler not available", http.StatusServiceUnavailable)
			return
		}
		h.logger.Warn().Err(err).Msg("queue snapshot")
		http.Error(w, "queue snapshot failed", http.StatusInternalServerError)
		return
	}
	views := buildQueueSectionViews(sections)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := templates.QueueSections(views).Render(r.Context(), w); err != nil {
		h.logger.Warn().Err(err).Msg("rendering queue list")
	}
}

// handleQueueEventsSSE pushes a "change" event whenever queue rows appear or
// disappear. JS reacts by re-fetching /api/queue/list. Mirrors the
// Scheduler-tab SSE pattern exactly.
func (h *Handler) handleQueueEventsSSE(w http.ResponseWriter, r *http.Request) {
	streamChangeEvents(w, r, h.queueBroker, "queue SSE unavailable")
}

// Queue tab mutations take their parameters as query-string args to keep
// the wiring symmetric with the Scheduler tab's evict button — Datastar
// `@post('/path?k=v')` doesn't ship a usable JSON body by default
// (the `body` option isn't part of the call signature), so query strings
// are the simplest path that works.

func (h *Handler) handleQueueCancel(w http.ResponseWriter, r *http.Request) {
	queueName := r.URL.Query().Get("queue")
	msgID := r.URL.Query().Get("msgID")
	err := h.cancelQueuedJob(r.Context(), queueName, msgID, actorFromRequest(r))
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, ErrOpUnavailable):
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "scheduler not available")
	case errors.Is(err, ErrOpInvalid):
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "queue and msgID are required")
	case errors.Is(err, scheduler.ErrRowInFlight):
		h.writeJSONErrorMsg(w, http.StatusConflict, "row is in flight; cannot cancel")
	case errors.Is(err, scheduler.ErrUnknownQueue):
		h.writeJSONErrorMsg(w, http.StatusNotFound, "queue not found")
	default:
		h.logger.Warn().Err(err).Str("queue", queueName).Str("msg_id", msgID).Msg("cancel queued row")
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, "cancel failed")
	}
}

// handleQueueCancelRunning fires HubCancelJob at the worker that's
// currently running requestID. The actual terminal frame arrives async;
// the immediate response is whether the cancel was *accepted*, not
// completed.
func (h *Handler) handleQueueCancelRunning(w http.ResponseWriter, r *http.Request) {
	requestID := r.URL.Query().Get("requestID")
	err := h.cancelRunningJob(r.Context(), requestID, actorFromRequest(r))
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, ErrOpUnavailable):
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "scheduler not available")
	case errors.Is(err, ErrOpInvalid):
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "requestID is required")
	case errors.Is(err, scheduler.ErrNotInflight):
		h.writeJSONErrorMsg(w, http.StatusNotFound, "request not in flight")
	case errors.Is(err, scheduler.ErrWorkerGone):
		h.writeJSONErrorMsg(w, http.StatusGone, "worker disconnected")
	default:
		h.logger.Warn().Err(err).Str("request_id", requestID).Msg("cancel running job")
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, "cancel failed")
	}
}

// handleQueueEvict moves a worker-queue row back to the global queue so
// the dispatcher re-places it. Same request shape as cancel.
func (h *Handler) handleQueueEvict(w http.ResponseWriter, r *http.Request) {
	queueName := r.URL.Query().Get("queue")
	msgID := r.URL.Query().Get("msgID")
	err := h.evictQueuedJob(r.Context(), queueName, msgID, actorFromRequest(r))
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, ErrOpUnavailable):
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "scheduler not available")
	case errors.Is(err, ErrOpInvalid):
		h.writeJSONErrorMsg(w, http.StatusBadRequest, "queue and msgID are required")
	case errors.Is(err, scheduler.ErrRowInFlight):
		h.writeJSONErrorMsg(w, http.StatusConflict, "row is in flight; cannot evict")
	case errors.Is(err, scheduler.ErrEvictGlobalRow):
		h.writeJSONErrorMsg(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, scheduler.ErrUnknownQueue):
		h.writeJSONErrorMsg(w, http.StatusNotFound, "queue not found")
	default:
		h.logger.Warn().Err(err).Str("queue", queueName).Str("msg_id", msgID).Msg("evict queued row")
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, "evict failed")
	}
}

// buildQueueSectionViews translates [scheduler.QueueSection] (the data
// shape) into [templates.QueueSectionView] (the render shape). Adds the
// section title + per-row display strings the template doesn't synthesize.
func buildQueueSectionViews(sections []scheduler.QueueSection) []templates.QueueSectionView {
	out := make([]templates.QueueSectionView, 0, len(sections))
	for _, sec := range sections {
		pending, running := 0, 0
		for _, r := range sec.Rows {
			if r.Inflight {
				running++
			} else {
				pending++
			}
		}
		view := templates.QueueSectionView{
			QueueName:    sec.Name,
			WorkerID:     sec.WorkerID,
			RowCount:     len(sec.Rows),
			PendingCount: pending,
			RunningCount: running,
			DepthSeconds: sec.DepthSeconds,
		}
		if sec.Name == "global" {
			view.Title = "Global queue"
		} else if sec.WorkerID != "" {
			view.Title = sec.WorkerID
		} else {
			view.Title = sec.Name
		}
		view.Rows = make([]templates.QueueRowView, 0, len(sec.Rows))
		for _, r := range sec.Rows {
			view.Rows = append(view.Rows, templates.QueueRowView{
				MsgID:         r.MsgID,
				RequestID:     r.RequestID,
				RuntimeName:   r.RuntimeName,
				ModelID:       r.ModelID,
				Source:        r.Source,
				Priority:      r.Priority,
				QueuedSeconds: r.QueuedSeconds,
				PayloadBytes:  r.PayloadBytes,
				Running:       r.Inflight,
			})
		}
		out = append(out, view)
	}
	return out
}
