package web

import (
	"fmt"
	"html"
	"net/http"

	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/starfederation/datastar-go/datastar"
)

// handleModuleProxy proxies HTTP requests to a module's built-in HTTP handler
// via the go-plugin gRPC channel. No ports needed — MASS is the reverse proxy.
//
// Route: /modules/{name}/{path...}
func (h *Handler) handleModuleProxy(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	subpath := "/" + r.PathValue("path")

	mp := h.orch.GetModule(name)
	if mp == nil {
		proxyErrorPage(w, r, "module not found: "+name, http.StatusNotFound)
		return
	}

	// Auto-start stopped modules on first request.
	if mp.State == scheduler.StateStopped {
		if err := h.orch.EnsureRunning(r.Context(), name); err != nil {
			proxyErrorPage(w, r, "failed to start module: "+err.Error(), http.StatusInternalServerError)
			return
		}
		h.orch.ArmIdleTimer(name)
		mp = h.orch.GetModule(name)
		if mp == nil {
			proxyErrorPage(w, r, "module disappeared after start", http.StatusInternalServerError)
			return
		}
	}

	mgr := mp.Runtime()
	if mgr == nil {
		proxyErrorPage(w, r, "module process not running", http.StatusServiceUnavailable)
		return
	}

	loaded := mgr.GetModule(name)
	if loaded == nil {
		proxyErrorPage(w, r, "module not loaded", http.StatusServiceUnavailable)
		return
	}

	handler := loaded.Module().HTTPHandler()
	if handler == nil {
		proxyErrorPage(w, r, "module has no UI", http.StatusNotFound)
		return
	}

	// Rewrite the request path to the module's local path (strip /modules/{name} prefix).
	proxyReq := r.Clone(r.Context())
	proxyReq.URL.Path = subpath
	proxyReq.RequestURI = subpath
	if r.URL.RawQuery != "" {
		proxyReq.RequestURI = subpath + "?" + r.URL.RawQuery
	}

	// The module's HTTPHandler on the host side (moduleGRPCClient) will
	// forward this over gRPC to the module subprocess.
	handler.ServeHTTP(w, proxyReq)
}

// handleModuleRenderLogs renders the log view for a module.
// This stays as a Datastar SSE endpoint since it patches into MASS's own shell.
func (h *Handler) handleModuleRenderLogs(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	sse := datastar.NewSSE(w, r)
	history := h.orch.GetLogHistory(name)
	patchContent(sse, templates.RenderLogView(name, history))
}

// proxyErrorPage writes a themed HTML error page suitable for rendering inside an iframe.
// proxyErrorPage writes a themed HTML error page for rendering inside an iframe.
// Colors match --mass-bg-base and --sl-color-danger from the MASS theme.
func proxyErrorPage(w http.ResponseWriter, r *http.Request, msg string, code int) {
	theme := r.URL.Query().Get("theme")
	if theme == "" {
		theme = "dark"
	}
	// Use the same palette as the MASS theme variables.
	bg := "#171616"    // dark --mass-bg-base
	fg := "#f87171"    // dark danger text
	muted := "#9a9a9a" // dark --mass-text-muted
	if theme == "light" {
		bg = "#f7f5f2"    // light --mass-bg-base
		fg = "#b91c1c"    // light danger text
		muted = "#4b5563" // light --mass-text-muted
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	_, _ = fmt.Fprintf(w, `<!doctype html><html><body style="margin:0;padding:24px;background:%s;font-family:system-ui,sans-serif">
<p style="color:%s;font-size:14px;line-height:1.6;max-width:720px">%s</p>
<p style="color:%s;font-size:12px;margin-top:12px">Try restarting the module.</p>
</body></html>`, bg, fg, html.EscapeString(msg), muted)
}

// patchContent patches #module-content with inner mode via Datastar SSE.
func patchContent(sse *datastar.ServerSentEventGenerator, htmlStr string) {
	_ = sse.PatchElements(htmlStr,
		datastar.WithSelector("#module-content"),
		datastar.WithMode(datastar.ElementPatchModeInner),
	)
}

// patchInstallError resets the installing state and shows an error inside the install dialog.
func patchInstallError(sse *datastar.ServerSentEventGenerator, msg string) {
	_ = sse.PatchSignals([]byte(`{"installing":false}`))
	_ = sse.PatchElements(
		fmt.Sprintf(`<div id="install-error"><sl-alert variant="danger" open>%s</sl-alert></div>`, html.EscapeString(msg)),
		datastar.WithSelector("#install-error"),
		datastar.WithMode(datastar.ElementPatchModeOuter),
	)
}
