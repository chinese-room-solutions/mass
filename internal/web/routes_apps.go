package web

import (
	"net/http"

	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/starfederation/datastar-go/datastar"
)

// handleAppRenderLogs renders the live log view for an app into #app-content.
func (h *Handler) handleAppRenderLogs(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	sse := datastar.NewSSE(w, r)
	history := h.orch.GetLogHistory(name)
	patchAppContent(sse, templates.RenderLogView(name, history))
}
