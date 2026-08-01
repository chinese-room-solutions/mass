// Package templates contains the templ-generated views and a few small Go
// helpers shared by them.
package templates

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"

	"github.com/a-h/templ"
	"github.com/chinese-room-solutions/mass-sdk/uikit"
)

// PropEntry is one row in a properties panel. Shared by the Scheduler
// instance-props view (and historically by the Models tab props panel,
// before models moved to per-runtime ownership).
type PropEntry struct {
	Key   string
	Value string
}

// pluralS returns "s" when n != 1. Tiny helper used by templ files for
// English plural suffixes.
func pluralS(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// modelsTabStyles applies once per page: rotates the <details> chevron
// when the group expands. The model-list rows themselves come from each
// runtime gateway's HTML fragment, which uses these classes verbatim.
const modelsTabStyles = `<style>
details.group-card[open] > summary sl-icon[name="chevron-right"] {
  transform: rotate(90deg);
}
details.hf-files[open] > summary sl-icon.hf-files-chev {
  transform: rotate(90deg);
}
</style>`

var (
	//go:embed scripts/file_browser.js
	fileBrowserJS string

	//go:embed scripts/shell.js
	shellScriptsJS string

	//go:embed scripts/settings_autosave.js
	settingsAutoSaveJS string

	//go:embed scripts/scheduler.js
	schedulerJS string

	//go:embed scripts/models.js
	modelsTabJS string

	//go:embed scripts/runtimes.js
	runtimesJS string

	//go:embed scripts/install.js
	installJS string
)

// Wrapped script blobs used by the templates.
var (
	fileBrowserScript      = "<script>" + fileBrowserJS + "</script>"
	shellScripts           = "<script>" + shellScriptsJS + "</script>"
	settingsAutoSaveScript = "<script>" + settingsAutoSaveJS + "</script>"
	schedulerScript        = "<script>" + schedulerJS + "</script>"
	modelsTabScript        = "<script>" + modelsTabJS + "</script>"
	runtimesScript         = "<script>" + runtimesJS + "</script>"
	installScript          = "<script>" + installJS + "</script>"
	alertScript            = "<script>" + uikit.AlertJS + "</script>"
)

// DashboardData holds all data needed to render the main dashboard shell.
// Models, Scheduler and Workers are fetched lazily by the browser on first
// tab activation — the shell ships their bodies as a spinner placeholder.
type DashboardData struct {
	Runtimes      []RuntimeViewData
	ActiveRuntime string
	ListenAddr    string
	// DataDir is the raw configured value, bound to the Settings input — empty
	// means "platform default". EffectiveDataDir is what MASS actually resolved
	// it to at startup, shown in the About section.
	DataDir          string
	EffectiveDataDir string
	AuthTokenSet     bool
	LogLevel         string // zerolog level name (e.g. "debug", "info")
	Theme            string // "dark" or "light"
	DevMode          bool
	Version          string // running build's version (shown in About)
	ConfigFile       string // path to the loaded config.yml (shown in About)
	LogsDir          string // path to logs directory (shown in About)
	TLSEnabled       bool
	TLSCertFile      string
	ResultTTL        string // job result cache TTL (e.g. "24h")
	IdleEvictionTTL  string // loaded-model idle eviction TTL (e.g. "10s")
	LoadAttempts     int    // total attempts before failing a job (1 = no retry)
	RegistryURL      string // future: package registry
}

// RegistryPackageView holds template data for one available registry runtime.
// Version is the newest listed version; Installable is whether an artifact
// exists for this server's platform; Installed is whether it's already on disk.
type RegistryPackageView struct {
	Name        string
	DisplayName string
	Description string
	Version     string
	// RuntimeName is the installed runtime's kind (the DELETE /api/runtimes/
	// path key) — what the row's Remove button hands the confirm dialog.
	RuntimeName string
	Installable bool
	Installed   bool
	// IncompatibleWorkers is the count of connected workers whose compatible
	// range excludes this listed version — the fleet a runtime upgrade to it
	// would strand. Non-zero only for an installed runtime with a newer listed
	// version. Rendered as a pre-upgrade warning on the row.
	IncompatibleWorkers int
}

// registryTitle prefers the display name, falling back to the package name.
func registryTitle(p RegistryPackageView) string {
	if p.DisplayName != "" {
		return p.DisplayName
	}
	return p.Name
}

// incompatibleWorkersLabel is the pre-upgrade warning text: how many connected
// workers this version would strand at Register.
func incompatibleWorkersLabel(n int) string {
	if n == 1 {
		return "1 connected worker incompatible"
	}
	return fmt.Sprintf("%d connected workers incompatible", n)
}

// registryInstallDisabled returns the data-attr:disabled value for a row's
// Install button: disabled when already installed or no artifact for the
// platform.
func registryInstallDisabled(p RegistryPackageView) string {
	if p.Installed || !p.Installable {
		return "true"
	}
	return "false"
}

// registryInstallDisabledExpr is the data-attr:disabled Datastar expression for
// a row's Install button. It disables when the package can't be installed
// (already installed / no artifact) or when any registry install is in flight
// (registryInstalling is non-empty), so a click doesn't fire a second install
// while one is downloading.
func registryInstallDisabledExpr(p RegistryPackageView) string {
	if registryInstallDisabled(p) == "true" {
		return "true"
	}
	return "$registryInstalling !== ''"
}

// registryInstallLoadingExpr is the data-attr:loading expression: true while
// this specific package is the one installing, so only its button spins.
func registryInstallLoadingExpr(p RegistryPackageView) string {
	return fmt.Sprintf("$registryInstalling === %s", jsStringLiteral(p.Name))
}

// registryInstallClickExpr marks this package as installing, then posts the
// install. The SSE handler clears $registryInstalling when it finishes.
func registryInstallClickExpr(p RegistryPackageView) string {
	return fmt.Sprintf("$registryInstalling = %s; @post('/api/runtimes/registry/install/%s')",
		jsStringLiteral(p.Name), p.Name)
}

// registryRemoveClickExpr routes an installed row's Remove through the same
// confirm dialog the sidebar uses — uninstalling a runtime deletes its binary,
// so it stays confirm-gated no matter the entry point.
func registryRemoveClickExpr(p RegistryPackageView) string {
	return fmt.Sprintf("$confirmUninstall = %s; $confirmOpen = true", jsStringLiteral(p.RuntimeName))
}

// RegistryPageSize is how many rows the runtime registry dialog renders per
// window — the same visible-row budget as the themes dialog and the gateway's
// Install Model panel.
const RegistryPageSize = 5

// registryMoreClickExpr widens the list window by a page, then re-fetches. The
// server reads the signal back and renders the wider window.
func registryMoreClickExpr(next int) string {
	return fmt.Sprintf("$registryLimit = %d; @get('/api/runtimes/registry')", next)
}

// registryReloadExpr rewinds the window to the first page and re-fetches.
// Opening the dialog and every fresh search start from the top.
func registryReloadExpr() string {
	return fmt.Sprintf("$registryLimit = %d; @get('/api/runtimes/registry')", RegistryPageSize)
}

// jsStringLiteral renders s as a single-quoted JS string literal, escaping the
// characters that would break out of the quotes in a Datastar attr expression.
func jsStringLiteral(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	return "'" + r.Replace(s) + "'"
}

// RenderRegistryAvailable renders the #registry-available list for SSE patches.
// filtered is true when the list is the result of a non-empty search query, so
// an empty list distinguishes "no match" from "empty registry". next is the
// window a "Show More" click should ask for, 0 when the list already fits.
func RenderRegistryAvailable(pkgs []RegistryPackageView, stale, filtered bool, errMsg string, next int) string {
	return renderToString(registryAvailable(pkgs, stale, filtered, errMsg, next))
}

// InstalledThemeView is one row of the themes dialog's "Installed" section.
// Built-ins can't be removed, so their row shows a marker instead of a button.
type InstalledThemeView struct {
	ID      string
	Label   string
	Base    string // "dark" | "light"
	Builtin bool
}

// AvailableThemeView is one row of the themes dialog's "Available" section: a
// registry theme package that isn't installed yet, at the version an install
// would pick.
type AvailableThemeView struct {
	Name        string // registry package name, the install endpoint's key
	Label       string
	Description string
	Version     string
}

// ThemeManagerView is the whole themes-dialog body. Installed always renders
// (it comes from the live uikit registry); ErrMsg replaces the Available
// section when the package registry can't be reached. Filtered is true when a
// non-empty search produced the result, so an empty section reads as "no match"
// rather than "nothing there".
// InstalledNext / AvailableNext carry the window a section's "Show More" click
// should ask for, or 0 when that section is already showing everything.
type ThemeManagerView struct {
	Installed     []InstalledThemeView
	Available     []AvailableThemeView
	InstalledNext int
	AvailableNext int
	Stale         bool
	Filtered      bool
	ErrMsg        string
}

// ThemePageSize is how many rows a themes-dialog section renders per window.
// Both sections start at one page, so the dialog opens showing up to ten rows —
// the same visible-row budget as the Install Model panel.
const ThemePageSize = 5

// Themes-dialog layout. The body is two content-sized grid rows that split the
// panel evenly only when both overflow: each row grows to its own content, any
// space one section doesn't fill goes to the other, and the panel stops at the
// ceiling with the overflowing section(s) scrolling internally. max-content
// keeps a short list from padding the dialog out to the ceiling, and
// align-content:start stops the tracks stretching to fill it.
const (
	themeManagerStyle = "display:grid;grid-template-rows:minmax(0,auto) minmax(0,auto);" +
		"align-content:start;gap:1rem;height:60vh;max-height:max-content"
	themeSectionStyle = "display:flex;flex-direction:column;min-height:0;gap:0.5rem"
	themeRowsStyle    = "min-height:0;overflow-y:auto"
)

// themeBusyDisabledExpr disables a theme row's button while any theme install
// or removal is in flight, so a second click can't race the first.
func themeBusyDisabledExpr() string { return "$themeBusy !== ''" }

// themeBusyLoadingExpr is the data-attr:loading expression: true only while
// this specific row is the one working, so only its button spins.
func themeBusyLoadingExpr(key string) string {
	return fmt.Sprintf("$themeBusy === %s", jsStringLiteral(key))
}

// themeInstallClickExpr marks the package as busy, then posts the install. The
// SSE handler clears $themeBusy when it finishes.
func themeInstallClickExpr(name string) string {
	return fmt.Sprintf("$themeBusy = %s; @post('/api/themes/install/%s')", jsStringLiteral(name), name)
}

// themeRemoveClickExpr marks the theme as busy, then posts the removal. The
// button sits inside the row's activate click-target, so the event must not
// bubble — otherwise removing a theme would also activate it.
func themeRemoveClickExpr(id string) string {
	return fmt.Sprintf("evt.stopPropagation(); $themeBusy = %s; @post('/api/themes/remove/%s')", jsStringLiteral(id), id)
}

// themeActivateClickExpr activates an installed theme from its dialog row via
// the same signal+post contract as the header palette. Guarded on $themeBusy so
// a row click can't re-apply a theme mid-removal.
func themeActivateClickExpr(id string) string {
	return fmt.Sprintf("if ($themeBusy === '') { $theme = %s; @post('/internal/settings/theme') }", jsStringLiteral(id))
}

// themeActiveShowExpr shows a row's active-check only while that theme is the
// applied one — reactive against $theme, so it follows palette switches too.
func themeActiveShowExpr(id string) string {
	return fmt.Sprintf("$theme === %s", jsStringLiteral(id))
}

// themeMoreClickExpr widens one section's window by a page, then re-fetches the
// dialog body. The server reads the signal back and renders the wider window.
func themeMoreClickExpr(signal string, next int) string {
	return fmt.Sprintf("$%s = %d; @get('/api/themes/registry')", signal, next)
}

// themeReloadExpr rewinds both windows to the first page and re-fetches. Opening
// the dialog and every fresh search start from the top.
func themeReloadExpr() string {
	return fmt.Sprintf("$themeInstalledLimit = %d; $themeAvailableLimit = %d; @get('/api/themes/registry')",
		ThemePageSize, ThemePageSize)
}

// RenderThemeManager renders the #theme-manager dialog body for SSE patches.
func RenderThemeManager(view ThemeManagerView) string {
	return renderToString(themeManager(view))
}

// RenderThemeMenu renders the header's #theme-menu for SSE patches, so an
// installed or removed theme appears in (or leaves) the picker without a
// reload.
func RenderThemeMenu() string {
	return renderToString(themeMenu())
}

// RenderThemesStyle renders the #mass-themes <style> element for SSE patches,
// so a just-installed theme's CSS is in the page before it can be picked.
func RenderThemesStyle() string {
	return themesStyle()
}

// firstRuntimeName returns the first runtime's name, or "" when none are
// installed. Seeds the Add-worker dialog's runtime selection.
func firstRuntimeName(rs []RuntimeViewData) string {
	if len(rs) == 0 {
		return ""
	}
	return rs[0].RuntimeName
}

// RuntimeViewData holds template data for one installed runtime gateway.
type RuntimeViewData struct {
	RuntimeName string
	DisplayName string
	Version     string
	Description string
	Running     bool
	AutoStart   bool
}

// dashboardSignals returns the JSON data-signals string for the dashboard.
func dashboardSignals(data DashboardData) string {
	resolved := uikit.ParseTheme(data.Theme)
	base, _ := uikit.LookupTheme(string(resolved))
	signals := map[string]any{
		"activeTab":          "runtimes",
		"activeRuntime":      data.ActiveRuntime,
		"sidebarCollapsed":   false,
		"runtimeSearch":      "",
		"installRuntimeOpen": false,
		"installLocalOpen":   false,
		"registryQuery":      "",
		"registryInstalling": "",
		// Themes dialog: open state, its search filter, the package name /
		// theme id of the row currently installing or being removed ("" = idle),
		// and how many rows each section currently windows to.
		"browseThemesOpen":    false,
		"themeQuery":          "",
		"themeBusy":           "",
		"themeInstalledLimit": ThemePageSize,
		"themeAvailableLimit": ThemePageSize,
		"confirmOpen":         false,
		"confirmUninstall":    "",
		"packagePath":         "",
		"installing":          false,
		// Runtimes-tab signals other than the dialog state.
		"listenAddr": data.ListenAddr,
		"dataDir":    data.DataDir,
		"authToken": func() string {
			if data.AuthTokenSet {
				return "••••••••"
			}
			return ""
		}(),
		"authTokenSet":              data.AuthTokenSet,
		"authTokenEdited":           false,
		"theme":                     string(resolved),
		"themeBase":                 string(base.Base),
		"devMode":                   data.DevMode,
		"resultTTL":                 data.ResultTTL,
		"idleEvictionTTL":           data.IdleEvictionTTL,
		"loadAttempts":              data.LoadAttempts,
		"registryURL":               data.RegistryURL,
		"logLevel":                  data.LogLevel,
		"tlsEnabled":                data.TLSEnabled,
		"tlsCertFile":               data.TLSCertFile,
		"modelsFilter":              "",
		"selectedModelID":           "",
		"selectedModelRuntime":      "",
		"confirmDeleteModelID":      "",
		"confirmDeleteModelKind":    "",
		"confirmDeleteModelOpen":    false,
		"confirmDeleteGroupOpen":    false,
		"confirmDeleteGroupPayload": "",
		"confirmDeleteGroupLabel":   "",
		"confirmDeleteGroupCount":   0,
		"selectedSchedulerKey":      "",
		// Gates the Workers tab's "Add worker" button — adding a worker needs a
		// runtime to attach it to. Re-patched over SSE on every runtime install
		// and uninstall, so the button follows without a reload.
		"hasRuntimes":      len(data.Runtimes) > 0,
		"addWorkerOpen":    false,
		"addWorkerRuntime": firstRuntimeName(data.Runtimes),
		// Add-worker dialog: a join token is minted server-side when the dialog
		// opens and patched into these signals. Empty until then.
		"addWorkerToken":        "",
		"addWorkerTokenExpiry":  "",
		"addWorkerTokenError":   "",
		"addWorkerAuthDisabled": false,
		// Add-worker dialog: worker package + backend, populated server-side on
		// dialog open and on runtime/worker change. Empty ⇒ the setup script
		// auto-selects (a lone package / the "Auto" backend).
		"addWorkerWorker":  "",
		"addWorkerBackend": "",
		// Add-worker dialog: the address the worker machines will use to reach this
		// MASS. Prefilled from the browser origin on first dialog open (a
		// user-edited value survives reopen). All generated commands derive their
		// base from this, so a host-local admin can substitute the LAN/DNS address.
		"addWorkerMassURL": "",
		// Add-worker dialog: which copy row last confirmed a copy (1-4, 0 = none).
		// A click sets it and resets after a short delay so the button flips to a
		// success check transiently.
		"addWorkerCopied": 0,
	}
	b, err := json.Marshal(signals)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// htmlThemeClass returns the <html> class string for the given theme: the
// registry's base + overlay classes, plus "dark" for the Tailwind dark variant
// on any dark-based theme and "mass-theme-custom" for pluggable (non-built-in)
// themes so the generic utility-override block in input.tw.css applies.
func htmlThemeClass(theme string) string {
	resolved := uikit.ParseTheme(theme)
	info, _ := uikit.LookupTheme(string(resolved))
	class := resolved.HTMLClass()
	if info.Base == uikit.ThemeDark {
		class += " dark"
	}
	if resolved != uikit.ThemeDark && resolved != uikit.ThemeLight {
		class += " mass-theme-custom"
	}
	return class
}

// bodyThemeClass returns the <body> class string for the given theme, keyed on
// the resolved theme's base (a light-based pluggable theme gets light body
// classes, everything else dark).
func bodyThemeClass(theme string) string {
	info, _ := uikit.LookupTheme(string(uikit.ParseTheme(theme)))
	if info.Base == uikit.ThemeLight {
		return "bg-neutral-100 text-neutral-900"
	}
	return "bg-neutral-950 text-neutral-100"
}

// massThemesScript returns the inline <script> that publishes the theme
// registry to the page as window.__massThemes (read by shell.js applyTheme).
// uikit.ThemesJSON is a compact JSON object and cannot contain '<', so it is
// safe to inline into a script element.
func massThemesScript() string {
	return "<script>window.__massThemes = " + uikit.ThemesJSON() + ";</script>"
}

// themesStyle returns the inline <style> carrying the pluggable-theme CSS.
// Rendered whole via templ.Raw because templ treats <style> content as raw text
// (expressions inside it are not evaluated). The SDK loader guarantees the CSS
// contains no '<', so it cannot break out. The element is always emitted, even
// with no themes loaded: it is the patch target the themes dialog re-renders
// into after an install, and a patch needs something to match by id.
func themesStyle() string {
	return `<style id="mass-themes">` + uikit.ThemesCSS() + "</style>"
}

// displayPath normalises a filesystem path for display in the UI. Windows
// data dirs come through with backslashes, which read as escape sequences
// in monospace contexts and clash with the rest-of-codebase forward-slash
// convention. We render with forward slashes everywhere; the OS still
// accepts both forms when the operator copies the path.
func displayPath(p string) string {
	return strings.ReplaceAll(p, `\`, `/`)
}

// initial returns the first letter of name, uppercased. Used for letter
// avatars when no icon is supplied.
func initial(name string) string {
	runes := []rune(name)
	if len(runes) == 0 {
		return "?"
	}
	return strings.ToUpper(string(runes[0]))
}

// runtimeStatusDotClass returns Tailwind classes for a runtime row's status
// dot. Running gateways are green; stopped ones are neutral; auto-start
// runtimes that aren't yet up render blue (they will start on demand).
func runtimeStatusDotClass(running, autoStart bool) string {
	base := "w-2.5 h-2.5 rounded-full flex-shrink-0"
	switch {
	case running:
		return base + " bg-green-400"
	case autoStart:
		return base + " bg-blue-400"
	default:
		return base + " bg-neutral-600"
	}
}

// renderToString renders a templ component to a string. Rendering to an
// in-memory buffer only fails if the component logic itself errors, which is
// an invariant violation for these process-built views — so panic rather
// than swallow.
func renderToString(c templ.Component) string {
	var buf bytes.Buffer
	if err := c.Render(context.Background(), &buf); err != nil {
		panic(fmt.Errorf("rendering component: %w", err))
	}
	return buf.String()
}

// RenderRuntimeRowActions returns the HTML for the Start/Stop button area
// keyed by runtime kind. Patched independently via SSE.
func RenderRuntimeRowActions(kind string, running bool) string {
	return fmt.Sprintf(`<span id="runtime-actions-%s">`, html.EscapeString(kind)) +
		renderToString(runtimeRowActions(kind, running)) + `</span>`
}

// RenderRuntimeAutoStartButton renders just the auto-start lightning toggle
// for a runtime row. Patched after the toggle endpoint flips the flag.
func RenderRuntimeAutoStartButton(kind string, autoStart bool) string {
	return fmt.Sprintf(`<span id="runtime-autostart-%s">`, html.EscapeString(kind)) +
		renderToString(runtimeAutoStartButton(kind, autoStart)) + `</span>`
}

// RenderRuntimeSidebarDot returns the sidebar status-dot for a runtime row.
func RenderRuntimeSidebarDot(kind string, running, autoStart bool) string {
	return fmt.Sprintf(
		`<span id="sidebar-dot-%s" class="%s absolute -bottom-0.5 -right-0.5 border-2 border-neutral-900"></span>`,
		html.EscapeString(kind), runtimeStatusDotClass(running, autoStart),
	)
}

// RenderRuntimeList returns the full HTML of #runtime-list for SSE updates
// after install or uninstall.
func RenderRuntimeList(rs []RuntimeViewData, active string) string {
	return `<div id="runtime-list" class="flex-1 overflow-y-auto py-1">` +
		renderToString(runtimeListItems(rs, active)) + `</div>`
}

// RenderAddWorkerRuntimePicker returns the #add-worker-runtime-picker fragment
// for SSE updates after the installed-runtime set changes.
func RenderAddWorkerRuntimePicker(rs []RuntimeViewData) string {
	return renderToString(addWorkerRuntimePicker(rs))
}

// WorkerOptionView is one worker package the operator can pick in the Add-worker
// dialog: its package name (the select value), human display name, and the
// distinct backends its resolvable versions advertise (a union across platforms).
type WorkerOptionView struct {
	Name        string
	DisplayName string
	Backends    []string
}

// RenderAddWorkerWorkerPicker returns the #add-worker-worker-picker fragment: a
// worker <select> populated from opts, always bound to $addWorkerWorker (the
// server pins that signal to a package name even for a lone package so the
// command carries an explicit &worker=). With none the fragment is empty.
// loadFailed renders a muted note instead of a select when the registry could
// not be reached.
func RenderAddWorkerWorkerPicker(opts []WorkerOptionView, loadFailed bool) string {
	return renderToString(addWorkerWorkerPicker(opts, loadFailed))
}

// RenderAddWorkerBackendPicker returns the #add-worker-backend-picker fragment: a
// backend <select> ("Auto" + each backend) for the selected worker package, or
// an empty container when the package has fewer than two backends (no choice to
// make). backends is the chosen package's backend union.
func RenderAddWorkerBackendPicker(backends []string) string {
	return renderToString(addWorkerBackendPicker(backends))
}

// AddWorkerSelection resolves what $addWorkerWorker and $addWorkerBackend should
// hold after options are (re)loaded for a runtime. worker is "" only when there
// are no options; otherwise it is the retained currentWorker when that still
// names a listed package, else the first package name — every package, including
// a lone one, is pinned explicitly so the command carries &worker=. backend is
// kept when currentBackend is still one of the selected package's backends, else
// reset to "".
func AddWorkerSelection(opts []WorkerOptionView, currentWorker, currentBackend string) (worker, backend string) {
	if len(opts) == 0 {
		return "", ""
	}
	worker = opts[0].Name
	for _, o := range opts {
		if o.Name == currentWorker {
			worker = currentWorker
			break
		}
	}
	backend = ""
	for _, o := range opts {
		if o.Name == worker {
			for _, b := range o.Backends {
				if b == currentBackend {
					backend = currentBackend
				}
			}
		}
	}
	return worker, backend
}

// BackendsForWorker returns the backend union of the option named worker, or nil
// when worker is empty or not listed. Callers pass it to
// RenderAddWorkerBackendPicker so the backend select follows the worker choice.
func BackendsForWorker(opts []WorkerOptionView, worker string) []string {
	if worker == "" {
		return nil
	}
	for _, o := range opts {
		if o.Name == worker {
			return o.Backends
		}
	}
	return nil
}

// CorrectAddWorkerRuntime returns the value $addWorkerRuntime should hold given
// the current runtime set and the client's current selection: the existing
// selection when it still names an installed runtime, otherwise the first
// runtime's name ("" when none are installed). Callers patch the signal only
// when this differs from current, leaving a valid selection untouched.
func CorrectAddWorkerRuntime(rs []RuntimeViewData, current string) string {
	for _, r := range rs {
		if r.RuntimeName == current {
			return current
		}
	}
	return firstRuntimeName(rs)
}

// RenderWelcomeState returns the Runtimes-tab right-pane welcome state.
func RenderWelcomeState(empty bool) string {
	heading := "Select a runtime"
	if empty {
		heading = "No runtimes installed"
	}
	subtext := "Install a runtime gateway from the registry, or a local .mass package."
	// Loud primary CTA only in the true empty state; once runtimes exist the
	// pane is just a "pick one" prompt, so the install button drops to default.
	installVariant := "default"
	if empty {
		installVariant = "primary"
	}
	return `<div id="runtime-welcome-content" class="flex flex-col items-center justify-center h-full text-center">` +
		`<h2 class="text-lg font-semibold mb-2">` + heading + `</h2>` +
		`<p class="text-neutral-400 text-sm mb-4">` + subtext + `</p>` +
		`<div class="flex items-center gap-2">` +
		`<sl-button variant="` + installVariant + `" size="small" data-on:click="$installRuntimeOpen = true">` +
		`<sl-icon slot="prefix" name="cloud-download"></sl-icon>Install Runtime</sl-button>` +
		`<sl-button variant="default" size="small" data-on:click="$installLocalOpen = true">` +
		`<sl-icon slot="prefix" name="folder2-open"></sl-icon>Browse Local</sl-button>` +
		`</div></div>`
}

// --- Models tab text helpers ---------------------------------------------
// View construction (modelstore.Entry → []ModelGroupView) lives in handler.go
// to avoid pulling modelstore into the templates package. The format/group
// helpers below are pure text and shared by both layers.

// --- Logs (gateway + system) ---------------------------------------------

// ansiRe matches ANSI SGR escape sequences: ESC [ <params> m
var ansiRe = regexp.MustCompile(`\x1b\[([0-9;]*)m`)

// urlRe matches http:// and https:// URLs in text.
var urlRe = regexp.MustCompile(`https?://[^\s<>"` + "`" + `]+`)

// logLevelColor returns a CSS color for a console-formatted log line.
func logLevelColor(line string) string {
	idx := strings.IndexByte(line, ' ')
	if idx > 0 && idx+4 <= len(line) {
		token := line[idx+1:]
		if len(token) >= 3 {
			switch token[:3] {
			case "DBG", "TRC":
				return "var(--mass-log-dbg)"
			case "INF":
				return "var(--mass-log-inf)"
			case "WRN":
				return "var(--mass-log-wrn)"
			case "ERR", "FTL":
				return "var(--mass-log-err)"
			}
		}
	}
	return "var(--mass-log-inf)"
}

func renderStructuredLogLine(raw string) string {
	esc := html.EscapeString
	sp1 := strings.IndexByte(raw, ' ')
	if sp1 < 0 {
		return `<p class="whitespace-pre-wrap leading-tight" style="color:var(--mass-log-inf)">` + esc(raw) + `</p>`
	}
	rest := raw[sp1+1:]
	sp2 := strings.IndexByte(rest, ' ')
	var level, after string
	if sp2 >= 0 {
		level = rest[:sp2]
		after = rest[sp2+1:]
	} else {
		level = rest
	}
	levelColor := logLevelColor(raw)
	knownLevel := false
	switch level {
	case "DBG", "TRC", "INF", "WRN", "ERR", "FTL":
		knownLevel = true
	}
	if !knownLevel {
		return `<p class="whitespace-pre-wrap leading-tight" style="color:` + levelColor + `">` + esc(raw) + `</p>`
	}
	timestamp := raw[:sp1]
	parts := strings.Split(after, " ")
	msgEnd := len(parts)
	for i := len(parts) - 1; i >= 0; i-- {
		if strings.Contains(parts[i], "=") {
			msgEnd = i
		} else {
			break
		}
	}
	msg := strings.Join(parts[:msgEnd], " ")
	kvParts := parts[msgEnd:]

	var b strings.Builder
	b.WriteString(`<p class="whitespace-pre-wrap leading-tight">`)
	fmt.Fprintf(&b, `<span style="color:var(--mass-log-time)">%s</span> `, esc(timestamp))
	fmt.Fprintf(&b, `<span style="color:%s;font-weight:bold">%s</span>`, levelColor, esc(level))
	if msg != "" {
		fmt.Fprintf(&b, ` <span style="color:var(--mass-log-msg)">%s</span>`, esc(msg))
	}
	for _, kv := range kvParts {
		eqIdx := strings.IndexByte(kv, '=')
		if eqIdx > 0 {
			key := kv[:eqIdx]
			val := kv[eqIdx+1:]
			fmt.Fprintf(&b, ` <span style="color:var(--mass-log-key)">%s</span>=<span style="color:var(--mass-log-val)">%s</span>`, esc(key), esc(val))
		} else {
			fmt.Fprintf(&b, ` %s`, esc(kv))
		}
	}
	b.WriteString(`</p>`)
	return b.String()
}

func linkifyURLs(htmlStr string) string {
	return urlRe.ReplaceAllStringFunc(htmlStr, func(u string) string {
		return fmt.Sprintf(`<a href="%s" target="_blank" class="underline hover:text-white">%s</a>`,
			html.EscapeString(u), html.EscapeString(u))
	})
}

// RenderLogLine converts a raw log line into a styled HTML <p>.
func RenderLogLine(raw string) string {
	plain := ansiRe.ReplaceAllString(raw, "")
	return linkifyURLs(renderStructuredLogLine(plain))
}

// RenderSystemLogLine renders one MASS system log line.
func RenderSystemLogLine(raw string) string {
	return RenderLogLine(raw)
}

// RenderRuntimeLogView returns the live-logs panel for a runtime kind.
// History is the buffered tail to pre-render.
func RenderRuntimeLogView(kind string, history []string) string {
	esc := html.EscapeString
	var entries string
	if len(history) > 0 {
		var b strings.Builder
		for _, line := range history {
			b.WriteString(RenderLogLine(line))
		}
		entries = b.String()
	} else {
		entries = `<p id="log-placeholder" class="text-neutral-500">Waiting for log output...</p>`
	}
	return fmt.Sprintf(`<div class="space-y-3 p-4">
<div class="flex items-center gap-2 text-sm font-medium text-neutral-300">
	<sl-icon name="terminal"></sl-icon>
	<span>%s — Live Logs</span>
</div>
<div id="log-entries" class="font-mono text-xs rounded-lg p-4 min-h-64 max-h-[calc(100vh-12rem)] overflow-y-auto space-y-px"
		style="background:var(--mass-bg-panel);border:1px solid var(--mass-border)">
	%s
</div>
<script>
(function(){var el=document.getElementById('log-entries');if(el)el.scrollTop=el.scrollHeight})();
</script>
</div>`, esc(kind), entries)
}

// formatGFlops renders a Q4_K matvec throughput number for an operator-
// facing chip. Big numbers fold into terraflops (one decimal place) so
// "1240 GF" displays as "1.2 TF"; smaller numbers stay GFLOPS-rounded.
func formatGFlops(gf float64) string {
	if gf >= 1000 {
		return fmt.Sprintf("%.1f TF", gf/1000)
	}
	return fmt.Sprintf("%.0f GF", gf)
}

// mismatchedGPURatio is the max/min compute_gflops threshold beyond which
// we surface the mismatched-GPU advisory. 2× fires on any meaningfully
// uneven pair — the discrete + iGPU case (1.8 TF vs ~50 GF) is ~36×, but
// even a moderate 3090 + 3060 pairing (~2×) is enough lockstep tax that
// the operator should know. Operators can still use the split
// deliberately — the advisory is informative, not coercive.
const mismatchedGPURatio = 2.0

// mismatchedGPUAdvisory returns an empty string when the worker's
// enabled-and-benched GPU set is uniform enough, or a one-sentence
// advisory string when a wide GFLOPS gap means tensor-splitting across
// the set would bottleneck on the slowest GPU. Caller renders the
// returned text inside an sl-alert.
//
// Background in [memory/project_mismatched_gpu_split.md]: llama.cpp's
// per-layer split runs lockstep, so the fastest GPU stalls on every
// layer waiting for the slowest. A 36× capability gap (discrete +
// integrated) collapses to roughly the integrated GPU's pace.
func mismatchedGPUAdvisory(devices []ComputeView) string {
	var minGF, maxGF float64
	var slowName, fastName string
	for _, d := range devices {
		if d.Type != "GPU" || !d.Enabled || !d.HasBenchmark || d.ComputeGFlops <= 0 {
			continue
		}
		if minGF == 0 || d.ComputeGFlops < minGF {
			minGF = d.ComputeGFlops
			slowName = d.DeviceName
		}
		if d.ComputeGFlops > maxGF {
			maxGF = d.ComputeGFlops
			fastName = d.DeviceName
		}
	}
	if minGF == 0 || maxGF == 0 || slowName == fastName {
		return "" // need at least two distinct enabled+benched GPUs
	}
	if maxGF/minGF < mismatchedGPURatio {
		return ""
	}
	return fmt.Sprintf(
		"Mismatched GPUs enabled (%s at %s vs %s at %s). "+
			"Runtimes that tensor-split across these devices will be bounded by the slower one. "+
			"Disable the slower GPU below if that's not what you want.",
		html.EscapeString(fastName), formatGFlops(maxGF),
		html.EscapeString(slowName), formatGFlops(minGF),
	)
}

// --- Workers tab view models ----------------------------------------------

// WorkerView is the per-worker render shape for the Workers tab.
type WorkerView struct {
	ID          string
	Name        string
	RuntimeName string
	Version     string // worker's own semver (required at handshake)
	Online      bool
	Enabled     bool // operator toggle: any device on this worker enabled
	Devices     []ComputeView
	ActiveJobs  int
}

// ComputeView is a per-device row inside a worker card.
type ComputeView struct {
	DeviceID       string
	DeviceName     string
	Type           string  // "CPU" / "GPU"
	Enabled        bool    // operator toggle: device allowed for new model loads
	MemoryMB       int     // total RAM/VRAM in MB
	UsedMemoryMB   int     // currently used
	UtilizationPct float64 // 0-100
	HasUtilization bool
	HasStats       bool
	MemoryGBs      float64 // benchmarked in-device memory bandwidth (STREAM)
	LoadGBs        float64 // benchmarked host→device upload throughput
	ComputeGFlops  float64 // benchmarked Q4_K matmul throughput
	HasBenchmark   bool
}

// RenderWorkersList returns the inner HTML of #workers-list (no wrapper).
func RenderWorkersList(workers []WorkerView) string {
	if len(workers) == 0 {
		return `<div class="flex flex-col items-center justify-center py-16 text-center">` +
			`<sl-icon name="pc-display-horizontal" style="font-size:2rem;color:var(--sl-color-neutral-500)" class="mb-3"></sl-icon>` +
			`<p class="text-sm" style="color:var(--mass-text-muted)">No workers connected.</p>` +
			`<p class="text-xs mt-1" style="color:var(--mass-text-faint)">Start a <span class="font-mono">mass-worker-*</span> binary that points at this MASS instance.</p>` +
			`</div>`
	}
	var b strings.Builder
	b.WriteString(`<style>.worker-stats-row{display:grid;font-family:var(--sl-font-mono);grid-template-columns:10ch 6rem 12ch 6rem;align-items:center;gap:0 2rem}.worker-stats-row .bench-val{white-space:nowrap}.worker-stats-row>div.text-xs{text-align:center}</style>`)
	for _, w := range workers {
		statusColor := "var(--mass-success)"
		statusIcon := "circle-fill"
		statusText := "Online"
		if !w.Online {
			statusColor = "var(--sl-color-neutral-400)"
			statusIcon = "circle"
			statusText = "Offline"
		}
		idSafe := html.EscapeString(w.ID)
		filterText := strings.ToLower(w.Name + " " + statusText + " " + w.RuntimeName)
		for _, d := range w.Devices {
			filterText += " " + strings.ToLower(d.DeviceName+" "+d.Type)
		}
		fmt.Fprintf(&b, `<sl-details class="worker-card" id="worker-card-%s" data-filter-text="%s">`,
			idSafe, html.EscapeString(filterText))
		b.WriteString(`<div slot="summary" class="flex items-center gap-3 w-full">`)
		fmt.Fprintf(&b, `<sl-tooltip content="%s"><sl-icon name="%s" style="font-size:0.5rem;color:%s"></sl-icon></sl-tooltip>`,
			statusText, statusIcon, statusColor)
		fmt.Fprintf(&b, `<span class="text-sm font-medium" style="color:var(--mass-text)">%s</span>`, html.EscapeString(w.Name))
		fmt.Fprintf(&b, `<sl-badge variant="primary" pill style="font-size:0.65rem">%s</sl-badge>`, html.EscapeString(w.RuntimeName))
		if w.Version != "" {
			fmt.Fprintf(&b, `<span class="text-xs" style="color:var(--mass-text-faint)">v%s</span>`, html.EscapeString(w.Version))
		}
		// Aggregate GFLOPS for the operator-visible "what does this worker
		// bring" summary. GPU devices sum (tensor split runs them as one
		// lockstep unit, but the throughput numbers add — bench measures
		// achievable Q4_K matvec on each in isolation). CPU stays separate
		// because the worker reserves CPU as a fallback used only when
		// every GPU is disabled.
		var gpuGF, cpuGF float64
		var gpuBenched, cpuBenched bool
		for _, d := range w.Devices {
			if !d.Enabled || !d.HasBenchmark {
				continue
			}
			switch d.Type {
			case "GPU":
				gpuGF += d.ComputeGFlops
				gpuBenched = true
			case "CPU":
				cpuGF = d.ComputeGFlops // worker reports one CPU device
				cpuBenched = true
			}
		}
		// Two-row chip: enabled-out-of-total devices (+ in-flight count
		// when busy) on top, bench aggregates below. mx-auto centers
		// the block between the title (left) and the toggle / Bench-All
		// controls (right); items-start keeps the two lines left-aligned
		// with each other so they read as a list.
		enabledDevices := 0
		for _, d := range w.Devices {
			if d.Enabled {
				enabledDevices++
			}
		}
		fmt.Fprintf(&b, `<div class="text-xs text-neutral-500 mx-auto flex flex-col items-start leading-tight">`)
		if w.ActiveJobs > 0 {
			fmt.Fprintf(&b, `<span>%d/%d device(s) · %d running</span>`,
				enabledDevices, len(w.Devices), w.ActiveJobs)
		} else {
			fmt.Fprintf(&b, `<span>%d/%d device(s)</span>`,
				enabledDevices, len(w.Devices))
		}
		if gpuBenched || cpuBenched {
			b.WriteString(`<span>`)
			first := true
			if gpuBenched {
				fmt.Fprintf(&b, `GPU %s`, formatGFlops(gpuGF))
				first = false
			}
			if cpuBenched {
				if !first {
					b.WriteString(` · `)
				}
				fmt.Fprintf(&b, `CPU %s`, formatGFlops(cpuGF))
			}
			b.WriteString(`</span>`)
		}
		b.WriteString(`</div>`)
		if len(w.Devices) > 0 {
			workerToggleIcon := "toggle2-on"
			workerToggleColor := "var(--mass-success)"
			workerToggleTip := "Disable all devices on this worker"
			if !w.Enabled {
				workerToggleIcon = "toggle2-off"
				workerToggleColor = "var(--sl-color-neutral-500)"
				workerToggleTip = "Enable all devices on this worker"
			}
			// Encoding the icon name into the id forces a full element
			// swap on every flip; Shoelace caches the SVG and in-place
			// `name` mutations sometimes fail to refresh.
			fmt.Fprintf(&b,
				`<sl-tooltip content="%s"><sl-icon-button id="workertog-%s-%s" name="%s" style="font-size:1.2rem;color:%s" `+
					`onclick="event.stopPropagation();fetch('/api/workers/%s/toggle',{method:'POST'}).then(window.__massRefetchWorkers).catch(function(){})"></sl-icon-button></sl-tooltip>`,
				workerToggleTip, idSafe, workerToggleIcon, workerToggleIcon, workerToggleColor,
				url.QueryEscape(w.ID))
		}
		if w.Online && len(w.Devices) > 0 {
			fmt.Fprintf(&b, `<sl-button size="small" variant="text" id="bench-all-%s" `+
				`onclick="event.stopPropagation()" `+
				`data-on:click="@post('/api/workers/benchmark?workerIds=%s')">`,
				idSafe, url.QueryEscape(w.ID))
			b.WriteString(`<sl-icon slot="prefix" name="speedometer2"></sl-icon>Bench All</sl-button>`)
		}
		b.WriteString(`</div>`)
		if len(w.Devices) == 0 {
			b.WriteString(`<p class="text-xs text-neutral-500 py-2">No compute devices reported.</p>`)
		}
		// Mismatched-GPU advisory. Only fires when >=2 enabled-and-benched
		// GPUs differ by a wide margin in measured Q4_K matvec throughput.
		// llama.cpp tensor-splits across every enabled GPU regardless of
		// capability and the split runs in lockstep, so the discrete-GPU
		// throughput collapses to the slowest device's pace. Operator may
		// still *want* this (fit a model that doesn't fit on one GPU
		// alone), so we inform, not refuse.
		if msg := mismatchedGPUAdvisory(w.Devices); msg != "" {
			fmt.Fprintf(&b, `<sl-alert variant="warning" open class="mb-2" style="--sl-spacing-large:0.6rem">`+
				`<sl-icon slot="icon" name="exclamation-triangle"></sl-icon>`+
				`<span class="text-xs">%s</span></sl-alert>`, msg)
		}
		for _, d := range w.Devices {
			icon := "cpu"
			iconColor := "var(--mass-accent)"
			badgeVariant := "primary"
			if d.Type == "GPU" {
				icon = "gpu-card"
				iconColor = "var(--mass-success)"
				badgeVariant = "success"
			}
			scopedID := idSafe + "_" + html.EscapeString(d.DeviceID)
			border := "border-neutral-800"
			if !d.Enabled {
				border = "border-yellow-700/60"
			}
			fmt.Fprintf(&b, `<div class="relative bg-neutral-900 rounded-lg border %s p-3 mb-2 space-y-2">`, border)
			fmt.Fprintf(&b, `<span class="absolute bottom-1.5 right-2 text-xs text-neutral-500 font-mono pointer-events-none">%s</span>`, html.EscapeString(d.DeviceID))

			b.WriteString(`<div class="flex items-center gap-3">`)
			fmt.Fprintf(&b, `<sl-icon name="%s" style="font-size:1.1rem;color:%s"></sl-icon>`, icon, iconColor)
			fmt.Fprintf(&b, `<span class="text-sm font-medium text-white">%s</span>`, html.EscapeString(d.DeviceName))
			fmt.Fprintf(&b, `<sl-badge variant="%s" pill>%s</sl-badge>`, badgeVariant, html.EscapeString(d.Type))
			b.WriteString(`<span class="ml-auto"></span>`)
			devToggleIcon := "toggle2-on"
			devToggleColor := "var(--mass-success)"
			devToggleTip := "Disable this device for new model loads"
			if !d.Enabled {
				devToggleIcon = "toggle2-off"
				devToggleColor = "var(--sl-color-neutral-500)"
				devToggleTip = "Enable this device for new model loads"
			}
			// Inline fetch — see the worker-toggle comment above.
			fmt.Fprintf(&b,
				`<sl-tooltip content="%s"><sl-icon-button id="devtog-%s-%s" name="%s" style="font-size:1.2rem;color:%s" `+
					`onclick="event.stopPropagation();fetch('/api/workers/%s/devices/%s/toggle',{method:'POST'}).then(window.__massRefetchWorkers).catch(function(){})"></sl-icon-button></sl-tooltip>`,
				devToggleTip, scopedID, devToggleIcon, devToggleIcon, devToggleColor,
				url.QueryEscape(w.ID), url.QueryEscape(d.DeviceID))
			fmt.Fprintf(&b, `<sl-button size="small" variant="text" data-on:click="@post('/api/workers/benchmark?workerIds=%s&deviceIds=%s')">`,
				url.QueryEscape(w.ID), url.QueryEscape(d.DeviceID))
			b.WriteString(`<sl-icon slot="prefix" name="speedometer2"></sl-icon>Bench</sl-button>`)
			b.WriteString(`</div>`)

			hasGauges := d.HasStats || d.HasUtilization
			if hasGauges || d.HasBenchmark {
				b.WriteString(`<div class="worker-stats-row">`)

				memTip := "RAM throughput (STREAM, in-device)"
				if d.Type == "GPU" {
					memTip = "VRAM throughput (STREAM, in-device)"
				}
				b.WriteString(`<div class="text-xs">`)
				fmt.Fprintf(&b, `<sl-tooltip content="%s"><span class="text-neutral-500" style="cursor:help">Memory</span></sl-tooltip>`, memTip)
				fmt.Fprintf(&b, `<div class="text-neutral-200 font-mono mt-0.5 bench-val" id="bench-bw-%s">`, scopedID)
				if d.HasBenchmark {
					b.WriteString(formatMemoryBW(d.MemoryGBs))
				} else {
					b.WriteString(`<span class="text-neutral-500">—</span>`)
				}
				b.WriteString(`</div>`)
				// Load throughput (host→device upload) — drives the
				// scheduler's switch-cost predictor. Shown as a faint
				// second line so the operator can compare it to Memory.
				if d.HasBenchmark && d.LoadGBs > 0 {
					loadTip := "Host→device upload throughput (model load)"
					fmt.Fprintf(&b, `<sl-tooltip content="%s"><div class="text-neutral-400 font-mono mt-0.5 bench-val" id="bench-load-%s" style="font-size:0.65rem;opacity:0.75;cursor:help">load %s</div></sl-tooltip>`,
						loadTip, scopedID, formatMemoryBW(d.LoadGBs))
				}
				b.WriteString(`</div>`)

				memLabel := "RAM"
				if d.Type == "GPU" {
					memLabel = "VRAM"
				}
				if d.HasStats && d.MemoryMB > 0 {
					pct := float64(d.UsedMemoryMB) / float64(d.MemoryMB) * 100
					if pct > 100 {
						pct = 100
					}
					writeGauge(&b, scopedID+"-mem", memLabel, pct,
						fmt.Sprintf("%s / %s", FormatMemMB(d.UsedMemoryMB), FormatMemMB(d.MemoryMB)))
				} else if d.MemoryMB > 0 {
					fmt.Fprintf(&b, `<div class="text-xs"><span class="text-neutral-500">%s</span><div class="text-neutral-200 font-mono mt-0.5">%s</div></div>`, memLabel, FormatMemMB(d.MemoryMB))
				} else {
					b.WriteString(`<div></div>`)
				}

				b.WriteString(`<div class="text-xs">`)
				b.WriteString(`<sl-tooltip content="Q4_K matmul throughput"><span class="text-neutral-500" style="cursor:help">Compute</span></sl-tooltip>`)
				fmt.Fprintf(&b, `<div class="text-neutral-200 font-mono mt-0.5 bench-val" id="bench-comp-%s">`, scopedID)
				if d.HasBenchmark {
					b.WriteString(formatFlops(d.ComputeGFlops))
				} else {
					b.WriteString(`<span class="text-neutral-500">—</span>`)
				}
				b.WriteString(`</div></div>`)

				if d.HasUtilization {
					computeLabel := "CPU"
					if d.Type == "GPU" {
						computeLabel = "GPU"
					}
					pct := d.UtilizationPct
					if pct > 100 {
						pct = 100
					}
					writeGauge(&b, scopedID+"-util", computeLabel, pct, fmt.Sprintf("%.0f%%", pct))
				} else {
					b.WriteString(`<div></div>`)
				}

				b.WriteString(`</div>`)
			} else {
				b.WriteString(`<div class="text-xs"><span class="text-neutral-500 italic">No benchmark data — click Bench</span></div>`)
			}

			b.WriteString(`</div>`)
		}
		b.WriteString(`</sl-details>`)
	}
	return b.String()
}

// writeGauge renders an SVG ring gauge with a label, percentage, and subtitle.
func writeGauge(b *strings.Builder, id, label string, pct float64, subtitle string) {
	const r = 28
	circumference := 2 * 3.14159265 * r
	offset := circumference * (1 - pct/100)
	color := BarColor(pct)
	b.WriteString(`<div class="flex flex-col items-center" style="width:58px">`)
	fmt.Fprintf(b, `<svg width="58" height="58" viewBox="0 0 66 66" class="gauge-ring" id="gauge-%s" data-gauge-pct="%.1f">`, html.EscapeString(id), pct)
	b.WriteString(`<circle cx="33" cy="33" r="28" fill="none" stroke="var(--mass-border)" stroke-width="5" opacity="0.6"/>`)
	fmt.Fprintf(b, `<circle cx="33" cy="33" r="28" fill="none" stroke="%s" stroke-width="5" stroke-linecap="round" stroke-dasharray="%.2f" stroke-dashoffset="%.2f" transform="rotate(-90 33 33)" style="transition:stroke-dashoffset .8s ease,stroke .4s ease"/>`, color, circumference, offset)
	fmt.Fprintf(b, `<text x="33" y="33" text-anchor="middle" dominant-baseline="central" fill="var(--mass-text)" font-size="12" font-family="monospace" font-weight="600">%.0f%%</text>`, pct)
	b.WriteString(`</svg>`)
	fmt.Fprintf(b, `<span class="text-xs text-neutral-500 mt-0.5">%s</span>`, label)
	fmt.Fprintf(b, `<span class="text-xs text-neutral-500 font-mono" style="font-size:0.65rem;white-space:nowrap">%s</span>`, subtitle)
	b.WriteString(`</div>`)
}

// BarColor returns a CSS color transitioning success → warning → danger over
// 0–100%, blended from the theme tokens so gauges follow the active theme.
// Mirrored by barColor in scripts/shell.js for in-place SSE gauge updates.
func BarColor(pct float64) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	if pct <= 50 {
		return fmt.Sprintf("color-mix(in srgb, var(--mass-warning) %.0f%%, var(--mass-success))", pct*2)
	}
	return fmt.Sprintf("color-mix(in srgb, var(--mass-danger) %.0f%%, var(--mass-warning))", (pct-50)*2)
}

// FormatMemMB formats a memory value in MB into a human-readable string.
func FormatMemMB(mb int) string {
	if mb >= 1024 {
		return fmt.Sprintf("%.1f GB", float64(mb)/1024)
	}
	return fmt.Sprintf("%d MB", mb)
}

func formatFlops(gflops float64) string {
	switch {
	case gflops >= 1000:
		return fmt.Sprintf("%.1f TFLOPS", gflops/1000)
	case gflops >= 1:
		return fmt.Sprintf("%.1f GFLOPS", gflops)
	case gflops >= 0.001:
		return fmt.Sprintf("%.1f MFLOPS", gflops*1000)
	default:
		return fmt.Sprintf("%.1f KFLOPS", gflops*1e6)
	}
}

func formatMemoryBW(gbs float64) string {
	switch {
	case gbs >= 1000:
		return fmt.Sprintf("%.1f TB/s", gbs/1000)
	case gbs >= 1:
		return fmt.Sprintf("%.1f GB/s", gbs)
	case gbs >= 0.001:
		return fmt.Sprintf("%.1f MB/s", gbs*1000)
	default:
		return fmt.Sprintf("%.1f KB/s", gbs*1e6)
	}
}
