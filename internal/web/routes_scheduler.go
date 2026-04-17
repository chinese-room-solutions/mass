package web

import (
	"bytes"
	"context"
	"net/http"
	"strings"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/chinese-room-solutions/mass/pkg/llm"
	"github.com/starfederation/datastar-go/datastar"
)

func (h *Handler) handleSchedulerEvict(w http.ResponseWriter, r *http.Request) {
	fp := r.URL.Query().Get("fp")
	sse := datastar.NewSSE(w, r)
	if fp == "" {
		return
	}
	if !h.orch.PoolEvict(fp) {
		h.logger.Warn().Str("fingerprint", fp).Msg("evict request for unknown or loading instance")
		return
	}
	mustSSE(
		// Clear the properties panel and deselect — the pool change callback
		// will also push an updated instance list to all SSE clients.
		sse.PatchElements(
			`<div id="scheduler-props-content"></div>`,
			datastar.WithSelector("#scheduler-props-content"),
			datastar.WithMode(datastar.ElementPatchModeOuter),
		))
	mustSSE(sse.PatchSignals([]byte(`{"selectedSchedulerFp":""}`)))
}

func (h *Handler) handleSchedulerInstanceInfo(w http.ResponseWriter, r *http.Request) {
	fp := r.URL.Query().Get("fp")
	sse := datastar.NewSSE(w, r)

	if fp == "" {
		mustSSE(sse.PatchElements(`<div id="scheduler-props-content"></div>`,
			datastar.WithSelector("#scheduler-props-content"),
			datastar.WithMode(datastar.ElementPatchModeOuter),
		))
		return
	}

	inst, ok := h.orch.PoolSnapshotInstance(fp)
	if !ok {
		mustSSE(sse.PatchElements(`<div id="scheduler-props-content"><p class="text-neutral-500 text-sm p-4">Instance not found (may have been evicted).</p></div>`,
			datastar.WithSelector("#scheduler-props-content"),
			datastar.WithMode(datastar.ElementPatchModeOuter),
		))
		return
	}

	view := instanceInfoToPropsView(inst, h.modelsDir())
	var buf bytes.Buffer
	if err := templates.SchedulerInstancePropsPanel(view).Render(r.Context(), &buf); err != nil {
		h.logger.Error().Err(err).Msg("failed to render scheduler instance props")
		return
	}
	mustSSE(sse.PatchElements(buf.String(),
		datastar.WithSelector("#scheduler-props-content"),
		datastar.WithMode(datastar.ElementPatchModeOuter),
	))
}

func (h *Handler) handleSchedulerInstances(w http.ResponseWriter, r *http.Request) {
	html := h.renderSchedulerContent(r.Context())
	sse := datastar.NewSSE(w, r)
	mustSSE(sse.PatchElements(html,
		datastar.WithSelector("#scheduler-content"),
		datastar.WithMode(datastar.ElementPatchModeOuter),
	))
}

// pushSchedulerContent renders and pushes the scheduler instance list via an existing SSE connection.
func (h *Handler) pushSchedulerContent(sse *datastar.ServerSentEventGenerator) {
	html := h.renderSchedulerContent(sse.Context())
	mustSSE(sse.PatchElements(html,
		datastar.WithSelector("#scheduler-content"),
		datastar.WithMode(datastar.ElementPatchModeOuter),
	))
}

// pushSchedulerInstanceStatus renders and pushes only the status elements
// (dot, badge, active reqs) for a single instance via an existing SSE connection.
// Also updates the properties panel if it is showing this instance.
func (h *Handler) pushSchedulerInstanceStatus(sse *datastar.ServerSentEventGenerator, fp string) {
	inst, ok := h.orch.PoolSnapshotInstance(fp)
	if !ok {
		return
	}

	status := "Idle"
	if inst.Loading {
		status = "Loading"
	} else if inst.Active {
		status = "Active"
	}

	ctx := sse.Context()
	var buf bytes.Buffer

	// Update list row elements.
	if err := templates.SchedulerInstanceDot(fp, status).Render(ctx, &buf); err == nil {
		mustSSE(sse.PatchElements(buf.String(),
			datastar.WithSelector("#sched-dot-"+fp),
			datastar.WithMode(datastar.ElementPatchModeOuter),
		))
	}

	buf.Reset()
	if err := templates.SchedulerInstanceStatusBadge(fp, status).Render(ctx, &buf); err == nil {
		mustSSE(sse.PatchElements(buf.String(),
			datastar.WithSelector("#sched-status-"+fp),
			datastar.WithMode(datastar.ElementPatchModeOuter),
		))
	}

	buf.Reset()
	if err := templates.SchedulerInstanceReqs(fp, inst.ActiveReqs).Render(ctx, &buf); err == nil {
		mustSSE(sse.PatchElements(buf.String(),
			datastar.WithSelector("#sched-reqs-"+fp),
			datastar.WithMode(datastar.ElementPatchModeOuter),
		))
	}

	// Update properties panel elements (only patched if the panel is showing this instance).
	buf.Reset()
	if err := templates.SchedulerPropsStatus(fp, status).Render(ctx, &buf); err == nil {
		mustSSE(sse.PatchElements(buf.String(),
			datastar.WithSelector("#sched-props-status-"+fp),
			datastar.WithMode(datastar.ElementPatchModeOuter),
		))
	}

	buf.Reset()
	if err := templates.SchedulerPropsReqs(fp, inst.ActiveReqs).Render(ctx, &buf); err == nil {
		mustSSE(sse.PatchElements(buf.String(),
			datastar.WithSelector("#sched-props-reqs-"+fp),
			datastar.WithMode(datastar.ElementPatchModeOuter),
		))
	}
}

// closeSchedulerPropsIfGone clears the properties panel selection if the
// currently selected instance no longer exists in the pool after a list change.
func (h *Handler) closeSchedulerPropsIfGone(sse *datastar.ServerSentEventGenerator) {
	mustSSE(sse.PatchElements(
		`<div id="scheduler-props-content"></div>`,
		datastar.WithSelector("#scheduler-props-content"),
		datastar.WithMode(datastar.ElementPatchModeOuter),
	))
	mustSSE(sse.PatchSignals([]byte(`{"selectedSchedulerFp":""}`)))
}

// renderSchedulerContent renders the scheduler instance list to an HTML string.
func (h *Handler) renderSchedulerContent(ctx context.Context) string {
	views := snapshotToViews(h.orch.PoolSnapshot(), h.modelsDir())
	var buf bytes.Buffer
	if err := templates.SchedulerInstanceList(views).Render(ctx, &buf); err != nil {
		h.logger.Error().Err(err).Msg("failed to render scheduler instances template")
		return ""
	}
	return buf.String()
}

func instanceInfoToPropsView(inst scheduler.ModelInstanceInfo, modelsDir string) templates.SchedulerInstancePropsView {
	typ := "Chat"
	if inst.Type == llm.ModelKindEmbedding {
		typ = "Embedding"
	}
	status := "Idle"
	if inst.Loading {
		status = "Loading"
	} else if inst.Active {
		status = "Active"
	}
	source := inst.Source
	if strings.HasPrefix(source, "app:") {
		source = "app: " + source[7:]
	}
	cfg := make([]templates.SchedulerConfigEntry, len(inst.Config))
	for i, e := range inst.Config {
		cfg[i] = templates.SchedulerConfigEntry{Key: e.Key, Value: e.Value}
	}
	return templates.SchedulerInstancePropsView{
		Fingerprint: inst.Fingerprint,
		Filename:    config.ModelIDFromPath(inst.Path, modelsDir),
		Path:        inst.Path,
		Type:        typ,
		Source:      source,
		Mode:        inst.Mode.String(),
		WorkerID:    inst.WorkerID,
		WorkerName:  inst.WorkerName,
		DeviceIDs:   inst.DeviceIDs,
		Status:      status,
		ActiveReqs:  inst.ActiveReqs,
		Config:      cfg,
	}
}

func snapshotToViews(snapshot []scheduler.ModelInstanceInfo, modelsDir string) []templates.SchedulerInstanceView {
	views := make([]templates.SchedulerInstanceView, len(snapshot))
	for i, inst := range snapshot {
		typ := "Chat"
		if inst.Type == llm.ModelKindEmbedding {
			typ = "Embedding"
		}

		status := "Idle"
		if inst.Loading {
			status = "Loading"
		} else if inst.Active {
			status = "Active"
		}

		source := inst.Source
		if strings.HasPrefix(source, "app:") {
			source = "app: " + strings.TrimPrefix(source, "app:")
		}

		views[i] = templates.SchedulerInstanceView{
			Fingerprint: inst.Fingerprint,
			Path:        inst.Path,
			Filename:    config.ModelIDFromPath(inst.Path, modelsDir),
			Type:        typ,
			Source:      source,
			Mode:        inst.Mode.String(),
			WorkerID:    inst.WorkerID,
			WorkerName:  inst.WorkerName,
			DeviceIDs:   inst.DeviceIDs,
			ActiveReqs:  inst.ActiveReqs,
			Status:      status,
		}
	}
	return views
}
