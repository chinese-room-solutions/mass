package web

import (
	"encoding/json"
	"errors"
	"fmt"
	htmlpkg "html"
	"net/http"
	"strings"

	"github.com/chinese-room-solutions/mass/internal/runtimes"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/starfederation/datastar-go/datastar"
)

// runtimeView is the JSON shape returned by /api/runtimes endpoints.
type runtimeView struct {
	RuntimeName string `json:"runtime_name"`
	Version     string `json:"version"`
	DisplayName string `json:"display_name"`
	Description string `json:"description,omitempty"`
	Running     bool   `json:"running"`
}

func (h *Handler) handleListRuntimes(w http.ResponseWriter, r *http.Request) {
	_ = r
	if h.runtimes == nil {
		h.writeJSON(w, http.StatusOK, []runtimeView{})
		return
	}
	mfs := h.runtimes.List()
	out := make([]runtimeView, len(mfs))
	for i, mf := range mfs {
		out[i] = runtimeView{
			RuntimeName: mf.RuntimeName,
			Version:     mf.Version,
			DisplayName: mf.DisplayName,
			Description: mf.Description,
			Running:     h.runtimes.IsRunning(mf.RuntimeName),
		}
	}
	h.writeJSON(w, http.StatusOK, out)
}

// handleInstallRuntime is Datastar-driven: it reads the dialog signals
// from the request, installs the .mass package, and on success patches the
// runtime list + closes the dialog by clearing signals. Errors render
// inline into #install-error so the user stays in the dialog.
//
// Backward-compat: when the request body looks like JSON ({...}) or a JSON
// content type with package_path, it falls through to the old shape so
// scripts/tests can still drive it.
func (h *Handler) handleInstallRuntime(w http.ResponseWriter, r *http.Request) {
	if h.runtimes == nil {
		patchInstallError(w, r, "Runtimes manager not available")
		return
	}

	var signals struct {
		PackagePath string `json:"packagePath"`
	}
	if err := datastar.ReadSignals(r, &signals); err != nil {
		patchInstallError(w, r, "Invalid request: "+err.Error())
		return
	}
	pkgPath := strings.TrimSpace(signals.PackagePath)
	if pkgPath == "" {
		patchInstallError(w, r, "Package path is required")
		return
	}

	if _, err := h.runtimes.InstallFromPath(pkgPath); err != nil {
		msg := err.Error()
		switch {
		case errors.Is(err, runtimes.ErrRuntimeAlreadyInstalled):
			msg = "This runtime is already installed."
		case errors.Is(err, runtimes.ErrManifestMissing):
			msg = "Package is missing runtime.yml."
		case errors.Is(err, runtimes.ErrBinaryMissing):
			msg = "Package is missing the gateway binary."
		default:
			h.logger.Warn().Err(err).Msg("installing runtime")
		}
		patchInstallError(w, r, msg)
		return
	}

	sse := datastar.NewSSE(w, r)
	// Refresh sidebar list.
	if err := sse.PatchElements(
		templates.RenderRuntimeList(h.runtimeViews(), ""),
		datastar.WithSelector("#runtime-list"),
		datastar.WithMode(datastar.ElementPatchModeOuter),
	); err != nil {
		h.logger.Debug().Err(err).Msg("SSE patch runtime-list")
	}
	// Clear error + close dialog + reset path.
	if err := sse.PatchElements(`<div id="install-error"></div>`); err != nil {
		h.logger.Debug().Err(err).Msg("SSE clear install-error")
	}
	if b, err := json.Marshal(map[string]any{
		"installRuntimeOpen": false,
		"installing":         false,
		"packagePath":        "",
	}); err == nil {
		if err := sse.PatchSignals(b); err != nil {
			h.logger.Debug().Err(err).Msg("SSE patch signals after install")
		}
	}
}

// patchInstallError writes the install dialog's #install-error region with a
// red alert and re-enables the Install button by clearing $installing.
func patchInstallError(w http.ResponseWriter, r *http.Request, msg string) {
	sse := datastar.NewSSE(w, r)
	html := fmt.Sprintf(`<div id="install-error"><sl-alert variant="danger" open>%s</sl-alert></div>`, escapeHTML(msg))
	if err := sse.PatchElements(html); err != nil {
		// Best-effort: client will retry; nothing we can do here.
		_ = err
	}
	if b, jerr := json.Marshal(map[string]any{"installing": false}); jerr == nil {
		_ = sse.PatchSignals(b)
	}
}

// escapeHTML is a tiny local wrapper over html.EscapeString to keep imports
// tidy in this file.
func escapeHTML(s string) string { return htmlpkg.EscapeString(s) }

func (h *Handler) handleUninstallRuntime(w http.ResponseWriter, r *http.Request) {
	if h.runtimes == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "runtimes manager not available")
		return
	}
	kind := r.PathValue("kind")
	if err := h.runtimes.Uninstall(kind); err != nil {
		h.logger.Warn().Err(err).Str("runtime_name", kind).Msg("uninstalling runtime")
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Refresh the sidebar list so the row vanishes; clear $activeRuntime
	// so the right pane returns to the welcome state.
	sse := datastar.NewSSE(w, r)
	if err := sse.PatchElements(
		templates.RenderRuntimeList(h.runtimeViews(), ""),
		datastar.WithSelector("#runtime-list"),
		datastar.WithMode(datastar.ElementPatchModeOuter),
	); err != nil {
		h.logger.Debug().Err(err).Msg("SSE patch runtime-list after uninstall")
	}
	if b, err := json.Marshal(map[string]any{"activeRuntime": ""}); err == nil {
		_ = sse.PatchSignals(b)
	}
}

func (h *Handler) handleStartRuntime(w http.ResponseWriter, r *http.Request) {
	if h.runtimes == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "runtimes manager not available")
		return
	}
	kind := r.PathValue("kind")
	if _, err := h.runtimes.Start(r.Context(), kind); err != nil {
		if errors.Is(err, runtimes.ErrRuntimeNotFound) {
			h.writeJSONErrorMsg(w, http.StatusNotFound, err.Error())
			return
		}
		h.logger.Warn().Err(err).Str("runtime_name", kind).Msg("starting runtime")
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.patchRuntimeRowState(w, r, kind)
}

func (h *Handler) handleStopRuntime(w http.ResponseWriter, r *http.Request) {
	if h.runtimes == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "runtimes manager not available")
		return
	}
	kind := r.PathValue("kind")
	if err := h.runtimes.Stop(kind); err != nil && !errors.Is(err, runtimes.ErrRuntimeNotRunning) {
		h.logger.Warn().Err(err).Str("runtime_name", kind).Msg("stopping runtime")
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.patchRuntimeRowState(w, r, kind)
}

// patchRuntimeRowState writes SSE PatchElements that swap a single row's
// Start/Stop button + sidebar status dot to reflect the post-action state.
// Used by both Start and Stop so the same row updates inline without a
// full sidebar re-render.
func (h *Handler) patchRuntimeRowState(w http.ResponseWriter, r *http.Request, kind string) {
	mf, err := h.runtimes.Get(kind)
	autoStart := false
	if err == nil {
		autoStart = mf.AutoStart
	}
	running := h.runtimes.IsRunning(kind)
	sse := datastar.NewSSE(w, r)
	if err := sse.PatchElements(templates.RenderRuntimeRowActions(kind, running)); err != nil {
		h.logger.Debug().Err(err).Msg("SSE patch runtime row actions")
	}
	if err := sse.PatchElements(templates.RenderRuntimeSidebarDot(kind, running, autoStart)); err != nil {
		h.logger.Debug().Err(err).Msg("SSE patch runtime sidebar dot")
	}
}

// --- Small JSON helpers used by the runtime routes ---

func (h *Handler) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		h.logger.Debug().Err(err).Msg("encoding json response")
	}
}

func (h *Handler) writeJSONErrorMsg(w http.ResponseWriter, status int, msg string) {
	h.writeJSON(w, status, map[string]string{"error": msg})
}
