package web

import (
	"context"
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
	infos := h.listRuntimeInfos()
	out := make([]runtimeView, len(infos))
	for i, ri := range infos {
		out[i] = runtimeView{
			RuntimeName: ri.RuntimeName,
			Version:     ri.Version,
			DisplayName: ri.DisplayName,
			Description: ri.Description,
			Running:     ri.Running,
		}
	}
	h.writeJSON(w, http.StatusOK, out)
}

// handleInstallRuntime is Datastar-driven: it reads the dialog signals
// from the request, installs the .mass package, and on success patches the
// runtime list + closes the dialog by clearing signals. Errors render
// inline into #install-error so the user stays in the dialog.
func (h *Handler) handleInstallRuntime(w http.ResponseWriter, r *http.Request) {
	if h.runtimes == nil {
		h.patchInstallError(w, r, "Runtimes manager not available")
		return
	}

	var signals struct {
		PackagePath      string `json:"packagePath"`
		AddWorkerRuntime string `json:"addWorkerRuntime"`
	}
	if err := datastar.ReadSignals(r, &signals); err != nil {
		h.patchInstallError(w, r, "Invalid request: "+err.Error())
		return
	}
	pkgPath := strings.TrimSpace(signals.PackagePath)
	if pkgPath == "" {
		h.patchInstallError(w, r, "Package path is required")
		return
	}

	if _, err := h.installRuntime(pkgPath, actorFromRequest(r)); err != nil {
		msg := err.Error()
		switch {
		case errors.Is(err, runtimes.ErrRuntimeAlreadyInstalled):
			msg = "This runtime is already installed."
		case errors.Is(err, runtimes.ErrManifestMissing):
			msg = "Package is missing runtime.yml."
		case errors.Is(err, runtimes.ErrBinaryMissing):
			msg = "Package is missing the gateway binary."
		}
		h.patchInstallError(w, r, msg)
		return
	}

	sse := datastar.NewSSE(w, r)
	views := h.runtimeViews()
	// Refresh sidebar list + welcome placard. Both patched by element id
	// (the rendered HTML carries id="runtime-list" / "runtime-welcome-content")
	// — the same id-based path the row Start/Stop patches use. Selector-scoped
	// patches are avoided on purpose; they don't apply on the pinned frontend.
	if err := sse.PatchElements(templates.RenderRuntimeList(views, "")); err != nil {
		h.logger.Debug().Err(err).Msg("sse patch runtime-list")
	}
	if err := sse.PatchElements(templates.RenderWelcomeState(len(views) == 0)); err != nil {
		h.logger.Debug().Err(err).Msg("sse patch runtime-welcome")
	}
	h.patchAddWorkerPicker(sse, views, signals.AddWorkerRuntime)
	// Clear error + close dialog + reset path.
	if err := sse.PatchElements(`<div id="install-error"></div>`); err != nil {
		h.logger.Debug().Err(err).Msg("sse clear install-error")
	}
	if b, err := json.Marshal(map[string]any{
		"installLocalOpen": false,
		"installing":       false,
		"packagePath":      "",
	}); err == nil {
		if err := sse.PatchSignals(b); err != nil {
			h.logger.Debug().Err(err).Msg("sse patch signals after install")
		}
	}
}

// patchAddWorkerPicker re-renders the Add-worker dialog's runtime picker to
// match the current runtime set, updates $hasRuntimes (which gates the Workers
// tab's "Add worker" button), and corrects $addWorkerRuntime when the client's
// selection is empty or names a runtime that's no longer installed. A valid
// existing selection is left untouched. Called by every handler that changes
// the installed-runtime set. current is the client's $addWorkerRuntime signal,
// read before the SSE generator was created.
func (h *Handler) patchAddWorkerPicker(sse *datastar.ServerSentEventGenerator, views []templates.RuntimeViewData, current string) {
	if err := sse.PatchElements(templates.RenderAddWorkerRuntimePicker(views)); err != nil {
		h.logger.Debug().Err(err).Msg("sse patch add-worker-runtime-picker")
	}
	signals := map[string]any{"hasRuntimes": len(views) > 0}
	if corrected := templates.CorrectAddWorkerRuntime(views, current); corrected != current {
		signals["addWorkerRuntime"] = corrected
	}
	if b, err := json.Marshal(signals); err == nil {
		if err := sse.PatchSignals(b); err != nil {
			h.logger.Debug().Err(err).Msg("sse patch add-worker signals")
		}
	}
}

// registrySignals is the runtime registry dialog's client state: the search
// filter plus the list's current window.
type registrySignals struct {
	Query string `json:"registryQuery"`
	Limit int    `json:"registryLimit"`
}

// withDefaults returns the signals with the window defaulted to one page.
func (s registrySignals) withDefaults() registrySignals {
	if s.Limit <= 0 {
		s.Limit = templates.RegistryPageSize
	}
	return s
}

// readRegistrySignals reads the dialog's signals. Must be called before the
// SSE generator is created (NewSSE consumes the request body).
func (h *Handler) readRegistrySignals(r *http.Request) registrySignals {
	var sig registrySignals
	if err := datastar.ReadSignals(r, &sig); err != nil {
		h.logger.Debug().Err(err).Msg("reading registry dialog signals")
	}
	return sig.withDefaults()
}

// patchRegistryAvailable fetches the registry (cache-tolerant) and re-renders
// the dialog's available list at the current window, or an inline error alert
// when the registry is unreachable.
func (h *Handler) patchRegistryAvailable(ctx context.Context, sse *datastar.ServerSentEventGenerator, sig registrySignals) {
	filtered := strings.TrimSpace(sig.Query) != ""
	res, err := h.searchPackages(ctx, string(registryRuntimeKind), sig.Query, "")
	if err != nil {
		if perr := sse.PatchElements(templates.RenderRegistryAvailable(nil, false, filtered, "Registry unavailable: "+err.Error(), 0)); perr != nil {
			h.logger.Debug().Err(perr).Msg("sse patch registry-available error")
		}
		return
	}
	views, next := windowRows(h.registryPackageViews(res.Packages), sig.Limit, templates.RegistryPageSize)
	if perr := sse.PatchElements(templates.RenderRegistryAvailable(views, res.Stale, filtered, "", next)); perr != nil {
		h.logger.Debug().Err(perr).Msg("sse patch registry-available")
	}
}

// handleRegistryAvailable streams the registry's available-runtimes list into
// #registry-available. It fetches the index (respecting request cancellation),
// keeps only runtime packages, marks those already installed, and renders the
// current window — or an inline error alert when the registry is unreachable.
func (h *Handler) handleRegistryAvailable(w http.ResponseWriter, r *http.Request) {
	sig := h.readRegistrySignals(r)
	h.patchRegistryAvailable(r.Context(), datastar.NewSSE(w, r), sig)
}

// registryRuntimeKind is the package kind the runtimes tab lists.
const registryRuntimeKind = "runtime"

// registryPackageViews maps neutral registry package views onto the template
// shape, folding each package's versions into a single "newest listed" version
// and marking whether it's already installed. Only runtime packages appear on
// the runtimes tab.
func (h *Handler) registryPackageViews(pkgs []PackageView) []templates.RegistryPackageView {
	installedVersion := make(map[string]string)
	installed := make(map[string]bool)
	for _, ri := range h.listRuntimeInfos() {
		installed[ri.RuntimeName] = true
		installedVersion[ri.RuntimeName] = ri.Version
	}
	fleet := h.fleetPairings()
	// Cache-only: the list this renders was just fetched, so the cache is
	// warm; an unreadable one simply drops the pre-upgrade flag.
	idx, err := h.cachedIndex()
	if err != nil {
		h.logger.Debug().Err(err).Msg("no cached registry index for the pre-upgrade fleet flag")
	}
	out := make([]templates.RegistryPackageView, 0, len(pkgs))
	for _, p := range pkgs {
		if p.Kind != registryRuntimeKind {
			continue
		}
		// Show the version an install would actually fetch — the newest one
		// this server's platform and version resolve to. With none, fall back
		// to the newest listed so the row still names the package, marked not
		// installable.
		version, installable := p.Installable, p.Installable != ""
		if !installable {
			version, _ = newestListedVersion(p.Versions)
		}
		// Pre-upgrade fleet flag: only for an installed runtime whose newest
		// listed version is strictly newer than what's on disk. Count the
		// connected workers whose index row excludes that new version — the
		// ones the upgrade would strand at Register.
		incompatible := 0
		if idx != nil && installed[p.RuntimeName] && version != "" && isNewerVersion(version, installedVersion[p.RuntimeName]) {
			incompatible = countIncompatibleWorkers(idx, fleet, p.RuntimeName, version)
		}
		out = append(out, templates.RegistryPackageView{
			Name:                p.Name,
			DisplayName:         p.DisplayName,
			Description:         p.Description,
			Version:             version,
			RuntimeName:         p.RuntimeName,
			Installable:         installable,
			Installed:           installed[p.RuntimeName],
			IncompatibleWorkers: incompatible,
		})
	}
	return out
}

// handleRegistryInstall installs a runtime from the registry by package name
// (newest resolvable version), then refreshes the sidebar list, the welcome
// placard, and the available list so the row flips to "Installed".
func (h *Handler) handleRegistryInstall(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	// Read the dialog signals before creating the SSE generator so the
	// re-patched list preserves the active filter and window — NewSSE
	// consumes the body, and it can only be read once.
	var signals struct {
		registrySignals
		AddWorkerRuntime string `json:"addWorkerRuntime"`
	}
	if err := datastar.ReadSignals(r, &signals); err != nil {
		h.logger.Debug().Err(err).Msg("reading registry install signals")
	}
	sig := signals.withDefaults()
	sse := datastar.NewSSE(w, r)
	if _, err := h.installRuntimeFromRegistry(r.Context(), name, "", actorFromRequest(r)); err != nil {
		msg := err.Error()
		if errors.Is(err, runtimes.ErrRuntimeAlreadyInstalled) {
			msg = "This runtime is already installed."
		}
		if perr := sse.PatchElements(fmt.Sprintf(
			`<sl-alert variant="danger" open closable id="registry-install-alert">%s</sl-alert>`,
			escapeHTML(msg))); perr != nil {
			h.logger.Debug().Err(perr).Msg("sse patch registry install error")
		}
		// Clear the per-row loading signal so the failed row stops spinning and
		// every Install button re-enables — without this a failure disables them
		// all forever.
		h.clearRegistryInstalling(sse)
		return
	}
	views := h.runtimeViews()
	if err := sse.PatchElements(templates.RenderRuntimeList(views, "")); err != nil {
		h.logger.Debug().Err(err).Msg("sse patch runtime-list after registry install")
	}
	if err := sse.PatchElements(templates.RenderWelcomeState(len(views) == 0)); err != nil {
		h.logger.Debug().Err(err).Msg("sse patch runtime-welcome after registry install")
	}
	h.patchAddWorkerPicker(sse, views, signals.AddWorkerRuntime)
	// Re-fetch the registry list so the just-installed row flips to "Installed".
	// A fetch failure inside is non-fatal — the install already succeeded.
	h.patchRegistryAvailable(r.Context(), sse, sig)
	// Clear the per-row loading signal so the button stops spinning and the
	// Install buttons re-enable. The re-patched list above already flipped the
	// installed row to "Installed".
	h.clearRegistryInstalling(sse)
}

// clearRegistryInstalling patches $registryInstalling back to "" over the SSE
// stream, ending the per-row install spinner and re-enabling every Install
// button. Called on both the success and error paths of a registry install.
func (h *Handler) clearRegistryInstalling(sse *datastar.ServerSentEventGenerator) {
	b, err := json.Marshal(map[string]any{"registryInstalling": ""})
	if err != nil {
		return
	}
	if err := sse.PatchSignals(b); err != nil {
		h.logger.Debug().Err(err).Msg("sse clear registryInstalling")
	}
}

// patchInstallError writes the install dialog's #install-error region with a
// red alert and re-enables the Install button by clearing $installing.
func (h *Handler) patchInstallError(w http.ResponseWriter, r *http.Request, msg string) {
	sse := datastar.NewSSE(w, r)
	html := fmt.Sprintf(`<div id="install-error"><sl-alert variant="danger" open>%s</sl-alert></div>`, escapeHTML(msg))
	if err := sse.PatchElements(html); err != nil {
		h.logger.Debug().Err(err).Msg("sse patch install error")
	}
	if b, jerr := json.Marshal(map[string]any{"installing": false}); jerr == nil {
		if err := sse.PatchSignals(b); err != nil {
			h.logger.Debug().Err(err).Msg("sse patch signals after install error")
		}
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
	// Read the signals before creating the SSE generator — NewSSE consumes
	// the request body, so a read after it comes back empty. The registry
	// dialog's signals ride along so a Remove clicked inside it re-renders
	// the list at the same filter and window.
	var signals struct {
		registrySignals
		AddWorkerRuntime string `json:"addWorkerRuntime"`
	}
	if err := datastar.ReadSignals(r, &signals); err != nil {
		h.logger.Debug().Err(err).Msg("reading uninstall signals")
	}
	sig := signals.withDefaults()
	if err := h.uninstallRuntime(kind, actorFromRequest(r)); err != nil {
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, "uninstall failed")
		return
	}
	// Refresh the sidebar list so the row vanishes; clear $activeRuntime
	// so the right pane returns to the welcome state.
	sse := datastar.NewSSE(w, r)
	views := h.runtimeViews()
	// Patched by element id (see handleInstallRuntime). Re-rendering the
	// welcome state also flips the placard from "Select a runtime" back to
	// "No runtimes installed" when the last runtime is removed.
	if err := sse.PatchElements(templates.RenderRuntimeList(views, "")); err != nil {
		h.logger.Debug().Err(err).Msg("sse patch runtime-list after uninstall")
	}
	if err := sse.PatchElements(templates.RenderWelcomeState(len(views) == 0)); err != nil {
		h.logger.Debug().Err(err).Msg("sse patch runtime-welcome after uninstall")
	}
	h.patchAddWorkerPicker(sse, views, signals.AddWorkerRuntime)
	// Refresh the registry dialog list so a Remove clicked there flips the
	// row back to Install. Cheap no-op visually when the dialog is closed.
	h.patchRegistryAvailable(r.Context(), sse, sig)
	if b, err := json.Marshal(map[string]any{"activeRuntime": ""}); err == nil {
		if err := sse.PatchSignals(b); err != nil {
			h.logger.Debug().Err(err).Msg("sse patch signals after uninstall")
		}
	}
}

func (h *Handler) handleStartRuntime(w http.ResponseWriter, r *http.Request) {
	if h.runtimes == nil {
		h.writeJSONErrorMsg(w, http.StatusServiceUnavailable, "runtimes manager not available")
		return
	}
	kind := r.PathValue("kind")
	if err := h.startRuntime(r.Context(), kind, actorFromRequest(r)); err != nil {
		if errors.Is(err, runtimes.ErrRuntimeNotFound) {
			h.writeJSONErrorMsg(w, http.StatusNotFound, err.Error())
			return
		}
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, "start failed")
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
	if err := h.stopRuntime(kind, actorFromRequest(r)); err != nil {
		h.writeJSONErrorMsg(w, http.StatusInternalServerError, "stop failed")
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
		h.logger.Debug().Err(err).Msg("sse patch runtime row actions")
	}
	if err := sse.PatchElements(templates.RenderRuntimeSidebarDot(kind, running, autoStart)); err != nil {
		h.logger.Debug().Err(err).Msg("sse patch runtime sidebar dot")
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
