package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/chinese-room-solutions/mass-sdk/uikit"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/starfederation/datastar-go/datastar"
)

// Themes dialog: browse the registry's theme packages and manage the installed
// ones. Installed themes come from the live uikit registry, so the dialog is
// useful with no registry connectivity at all — a fetch failure only degrades
// the "Available" section to a note.

// handleThemeRegistry streams the theme manage/browse list into #theme-manager,
// honoring the dialog's search query and per-section windows.
func (h *Handler) handleThemeRegistry(w http.ResponseWriter, r *http.Request) {
	// Read the dialog's signals before creating the SSE generator — NewSSE
	// consumes the request body, so a read after it comes back empty.
	sig := h.readThemeSignals(r)
	h.patchThemeManager(r.Context(), datastar.NewSSE(w, r), sig)
}

// handleThemeInstall installs a theme package from the registry, then refreshes
// the header's theme picker, the inlined theme CSS, and the dialog list so the
// new theme is pickable without a reload.
func (h *Handler) handleThemeInstall(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	sig := h.readThemeSignals(r)
	sse := datastar.NewSSE(w, r)

	if err := h.installThemeFromRegistry(r.Context(), name, actorFromRequest(r)); err != nil {
		h.patchThemeAlert(sse, "Install failed: "+err.Error())
		h.clearThemeBusy(sse)
		return
	}
	h.patchThemeAlert(sse, "")
	h.patchThemePicker(sse)
	h.patchThemeManager(r.Context(), sse, sig)
	h.clearThemeBusy(sse)
}

// handleThemeRemove uninstalls a pluggable theme. When the removed theme was
// the active one the page would be left styling itself with CSS that no longer
// exists, so it falls back to the default built-in first.
func (h *Handler) handleThemeRemove(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sig := h.readThemeSignals(r)
	sse := datastar.NewSSE(w, r)

	if err := h.removeInstalledTheme(id, actorFromRequest(r)); err != nil {
		msg := err.Error()
		switch {
		case errors.Is(err, uikit.ErrThemeBuiltin):
			msg = "Built-in themes cannot be removed."
		case errors.Is(err, uikit.ErrThemeNotInstalled):
			msg = "That theme is not installed."
		}
		h.patchThemeAlert(sse, msg)
		h.clearThemeBusy(sse)
		return
	}
	if h.cfg != nil && h.cfg.Theme == id {
		if info, ok := uikit.LookupTheme(string(uikit.ThemeDark)); ok {
			h.applyTheme(sse, info)
		}
	}
	h.patchThemeAlert(sse, "")
	h.patchThemePicker(sse)
	h.patchThemeManager(r.Context(), sse, sig)
	h.clearThemeBusy(sse)
}

// themeSignals is the themes dialog's client state: the search filter plus how
// many rows each section is currently windowed to.
type themeSignals struct {
	Query          string `json:"themeQuery"`
	InstalledLimit int    `json:"themeInstalledLimit"`
	AvailableLimit int    `json:"themeAvailableLimit"`
}

// readThemeSignals reads the dialog's signals, defaulting each window to one
// page. Must be called before the SSE generator is created (NewSSE consumes the
// request body).
func (h *Handler) readThemeSignals(r *http.Request) themeSignals {
	var sig themeSignals
	if err := datastar.ReadSignals(r, &sig); err != nil {
		h.logger.Debug().Err(err).Msg("reading theme dialog signals")
	}
	if sig.InstalledLimit <= 0 {
		sig.InstalledLimit = templates.ThemePageSize
	}
	if sig.AvailableLimit <= 0 {
		sig.AvailableLimit = templates.ThemePageSize
	}
	return sig
}

// patchThemeManager re-renders the dialog body: the installed list (always,
// from the live uikit registry) plus the available list, or a note in its place
// when the registry can't be reached. Each section renders only its current
// window; the rest sits behind its "Show More" row.
func (h *Handler) patchThemeManager(ctx context.Context, sse *datastar.ServerSentEventGenerator, sig themeSignals) {
	available, stale, err := h.availableThemes(ctx, sig.Query)
	errMsg := ""
	if err != nil {
		errMsg = "Registry unavailable — installed themes still work."
	}
	installed, installedNext := windowRows(h.installedThemes(sig.Query), sig.InstalledLimit, templates.ThemePageSize)
	available, availableNext := windowRows(available, sig.AvailableLimit, templates.ThemePageSize)
	view := templates.ThemeManagerView{
		Installed:     themeInstalledViews(installed),
		Available:     themeAvailableViews(available),
		InstalledNext: installedNext,
		AvailableNext: availableNext,
		Stale:         stale,
		Filtered:      sig.Query != "",
		ErrMsg:        errMsg,
	}
	if perr := sse.PatchElements(templates.RenderThemeManager(view)); perr != nil {
		h.logger.Debug().Err(perr).Msg("sse patch theme-manager")
	}
}

// patchThemePicker re-renders the header's theme menu and the inlined
// pluggable-theme CSS so a just-installed theme is immediately selectable and
// actually styled — both are rendered server-side once per page load, so
// without this they would only catch up on a reload. The menu must be
// replaced, not morphed: morphing sl-menu's children leaves the component in
// a state where it stops emitting sl-select, deadening the whole picker.
func (h *Handler) patchThemePicker(sse *datastar.ServerSentEventGenerator) {
	if err := sse.PatchElements(templates.RenderThemeMenu(), datastar.WithModeReplace()); err != nil {
		h.logger.Debug().Err(err).Msg("sse patch theme-menu")
	}
	if err := sse.PatchElements(templates.RenderThemesStyle()); err != nil {
		h.logger.Debug().Err(err).Msg("sse patch themes-style")
	}
}

// patchThemeAlert writes (or clears, when msg is empty) the dialog's inline
// error region so the operator stays in the dialog.
func (h *Handler) patchThemeAlert(sse *datastar.ServerSentEventGenerator, msg string) {
	html := `<div id="theme-alert"></div>`
	if msg != "" {
		html = fmt.Sprintf(
			`<div id="theme-alert"><sl-alert variant="danger" open closable>%s</sl-alert></div>`,
			escapeHTML(msg))
	}
	if err := sse.PatchElements(html); err != nil {
		h.logger.Debug().Err(err).Msg("sse patch theme-alert")
	}
}

// clearThemeBusy patches $themeBusy back to "", ending the per-row spinner and
// re-enabling every Install/Remove button. Called on both the success and error
// paths so a failure can't leave the dialog stuck.
func (h *Handler) clearThemeBusy(sse *datastar.ServerSentEventGenerator) {
	b, err := json.Marshal(map[string]any{"themeBusy": ""})
	if err != nil {
		return
	}
	if err := sse.PatchSignals(b); err != nil {
		h.logger.Debug().Err(err).Msg("sse clear themeBusy")
	}
}

// windowRows cuts a list down to its current window and returns the window a
// "Show More" click should ask for next — 0 when the whole list already fits,
// which is what makes the row disappear at the boundary. pageSize is the
// list's widening step.
func windowRows[T any](rows []T, limit, pageSize int) ([]T, int) {
	if len(rows) <= limit {
		return rows, 0
	}
	return rows[:limit], limit + pageSize
}

// themeInstalledViews maps the neutral installed-theme views onto the template
// shape.
func themeInstalledViews(themes []InstalledTheme) []templates.InstalledThemeView {
	out := make([]templates.InstalledThemeView, len(themes))
	for i, t := range themes {
		out[i] = templates.InstalledThemeView{ID: t.ID, Label: t.Label, Base: t.Base, Builtin: t.Builtin}
	}
	return out
}

// themeAvailableViews maps the neutral registry-theme views onto the template
// shape.
func themeAvailableViews(themes []AvailableTheme) []templates.AvailableThemeView {
	out := make([]templates.AvailableThemeView, len(themes))
	for i, t := range themes {
		out[i] = templates.AvailableThemeView{
			Name:        t.Name,
			Label:       t.Label,
			Description: t.Description,
			Version:     t.Version,
		}
	}
	return out
}
