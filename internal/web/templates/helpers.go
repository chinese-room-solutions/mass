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

	//go:embed scripts/settings_browse.js
	settingsBrowseJS string

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
	settingsBrowseScript   = "<script>" + settingsBrowseJS + "</script>"
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
	Runtimes        []RuntimeViewData
	ActiveRuntime   string
	ListenAddr      string
	DataDir         string
	AuthTokenSet    bool
	LogLevel        string // zerolog level name (e.g. "debug", "info")
	Theme           string // "dark" or "light"
	DevMode         bool
	ConfigDir       string // directory containing config.yml (shown on Settings tab)
	LogsDir         string // path to logs directory (shown on Settings tab)
	TLSEnabled      bool
	TLSCertFile     string
	ResultTTL       string // job result cache TTL (e.g. "24h")
	IdleEvictionTTL string // loaded-model idle eviction TTL (e.g. "10s")
	RegistryURL     string // future: package registry
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
	theme := data.Theme
	if theme == "" {
		theme = "dark"
	}
	signals := map[string]any{
		"activeTab":          "runtimes",
		"activeRuntime":      data.ActiveRuntime,
		"sidebarCollapsed":   false,
		"runtimeSearch":      "",
		"installRuntimeOpen": false,
		"confirmOpen":        false,
		"confirmUninstall":   "",
		"packagePath":        "",
		"installing":         false,
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
		"theme":                     theme,
		"devMode":                   data.DevMode,
		"resultTTL":                 data.ResultTTL,
		"idleEvictionTTL":           data.IdleEvictionTTL,
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
	}
	b, err := json.Marshal(signals)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// htmlThemeClass returns the <html> class string for the given theme.
func htmlThemeClass(theme string) string {
	if theme == "light" {
		return "sl-theme-light"
	}
	return "sl-theme-dark dark"
}

// bodyThemeClass returns the <body> class string for the given theme.
func bodyThemeClass(theme string) string {
	if theme == "light" {
		return "bg-neutral-100 text-neutral-900"
	}
	return "bg-neutral-950 text-neutral-100"
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

// RenderRuntimeRowActions returns the HTML for the Start/Stop button area
// keyed by runtime kind. Patched independently via SSE.
func RenderRuntimeRowActions(kind string, running bool) string {
	var buf bytes.Buffer
	_ = runtimeRowActions(kind, running).Render(context.Background(), &buf)
	return fmt.Sprintf(`<span id="runtime-actions-%s">`, html.EscapeString(kind)) + buf.String() + `</span>`
}

// RenderRuntimeAutoStartButton renders just the auto-start lightning toggle
// for a runtime row. Patched after the toggle endpoint flips the flag.
func RenderRuntimeAutoStartButton(kind string, autoStart bool) string {
	var buf bytes.Buffer
	_ = runtimeAutoStartButton(kind, autoStart).Render(context.Background(), &buf)
	return fmt.Sprintf(`<span id="runtime-autostart-%s">`, html.EscapeString(kind)) + buf.String() + `</span>`
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
	var buf bytes.Buffer
	_ = runtimeListItems(rs, active).Render(context.Background(), &buf)
	return `<div id="runtime-list" class="flex-1 overflow-y-auto py-1">` + buf.String() + `</div>`
}

// RenderWelcomeState returns the Runtimes-tab right-pane welcome state.
func RenderWelcomeState(empty bool) string {
	heading := "Select a runtime"
	subtext := "Choose a runtime from the sidebar, or install a new one."
	if empty {
		heading = "No runtimes installed"
		subtext = "Install a runtime gateway package (.mass) to get started."
	}
	return `<div class="flex flex-col items-center justify-center h-64 text-center">` +
		`<h2 class="text-lg font-semibold mb-2">` + heading + `</h2>` +
		`<p class="text-neutral-400 text-sm mb-4">` + subtext + `</p>` +
		`<sl-button variant="primary" size="small" data-on:click="$installRuntimeOpen = true">` +
		`<sl-icon slot="prefix" name="plus-lg"></sl-icon>Install Runtime</sl-button></div>`
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

// --- Workers tab view models ----------------------------------------------

// WorkerView is the per-worker render shape for the Workers tab.
type WorkerView struct {
	ID          string
	Name        string
	RuntimeName string
	Online      bool
	Enabled     bool // operator toggle: any device on this worker enabled
	Devices     []ComputeView
	ActiveJobs  int
	Capacity    int
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
	MemoryGBs      float64 // benchmarked memory bandwidth
	ComputeGFlops  float64 // benchmarked Q4_K matmul throughput
	HasBenchmark   bool
}

// RenderWorkersList returns the inner HTML of #workers-list (no wrapper).
func RenderWorkersList(workers []WorkerView) string {
	var b strings.Builder
	b.WriteString(`<style>.worker-stats-row{display:grid;font-family:var(--sl-font-mono);grid-template-columns:10ch 6rem 12ch 6rem;align-items:center;gap:0 2rem}.worker-stats-row .bench-val{white-space:nowrap}.worker-stats-row>div.text-xs{text-align:center}</style>`)
	if len(workers) == 0 {
		b.WriteString(`<div class="flex flex-col items-center justify-center py-16 text-center">` +
			`<sl-icon name="pc-display-horizontal" style="font-size:2rem;color:var(--sl-color-neutral-500)" class="mb-3"></sl-icon>` +
			`<p class="text-sm" style="color:var(--mass-text-muted)">No workers connected.</p>` +
			`<p class="text-xs mt-1" style="color:var(--mass-text-faint)">Start a <span class="font-mono">mass-worker-*</span> binary that points at this MASS instance.</p>` +
			`</div>`)
		return b.String()
	}
	for _, w := range workers {
		statusColor := "var(--sl-color-success-400)"
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
		fmt.Fprintf(&b, `<span class="text-xs text-neutral-500 ml-auto mr-2">%d device(s) · %d/%d active</span>`,
			len(w.Devices), w.ActiveJobs, w.ActiveJobs+w.Capacity)
		if len(w.Devices) > 0 {
			workerToggleIcon := "toggle2-on"
			workerToggleColor := "var(--sl-color-success-500)"
			workerToggleTip := "Disable all devices on this worker"
			if !w.Enabled {
				workerToggleIcon = "toggle2-off"
				workerToggleColor = "var(--sl-color-neutral-500)"
				workerToggleTip = "Enable all devices on this worker"
			}
			// Inline fetch — mirrors the pre-refactor commit's pattern.
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
		for _, d := range w.Devices {
			icon := "cpu"
			iconColor := "var(--mass-blue)"
			badgeVariant := "primary"
			if d.Type == "GPU" {
				icon = "gpu-card"
				iconColor = "var(--mass-green)"
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
			devToggleColor := "var(--sl-color-success-500)"
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

				memTip := "RAM throughput"
				if d.Type == "GPU" {
					memTip = "VRAM throughput"
				}
				b.WriteString(`<div class="text-xs">`)
				fmt.Fprintf(&b, `<sl-tooltip content="%s"><span class="text-neutral-500" style="cursor:help">Memory</span></sl-tooltip>`, memTip)
				fmt.Fprintf(&b, `<div class="text-neutral-200 font-mono mt-0.5 bench-val" id="bench-bw-%s">`, scopedID)
				if d.HasBenchmark {
					b.WriteString(formatMemoryBW(d.MemoryGBs))
				} else {
					b.WriteString(`<span class="text-neutral-500">—</span>`)
				}
				b.WriteString(`</div></div>`)

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
	b.WriteString(`<circle cx="33" cy="33" r="28" fill="none" stroke="var(--sl-color-neutral-300)" stroke-width="5" opacity="0.25"/>`)
	fmt.Fprintf(b, `<circle cx="33" cy="33" r="28" fill="none" stroke="%s" stroke-width="5" stroke-linecap="round" stroke-dasharray="%.2f" stroke-dashoffset="%.2f" transform="rotate(-90 33 33)" style="transition:stroke-dashoffset .8s ease,stroke .4s ease"/>`, color, circumference, offset)
	fmt.Fprintf(b, `<text x="33" y="33" text-anchor="middle" dominant-baseline="central" fill="var(--mass-text)" font-size="12" font-family="monospace" font-weight="600">%.0f%%</text>`, pct)
	b.WriteString(`</svg>`)
	fmt.Fprintf(b, `<span class="text-xs text-neutral-500 mt-0.5">%s</span>`, label)
	fmt.Fprintf(b, `<span class="text-xs text-neutral-500 font-mono" style="font-size:0.65rem;white-space:nowrap">%s</span>`, subtitle)
	b.WriteString(`</div>`)
}

// BarColor returns an HSL color string transitioning green → red over 0–100%.
func BarColor(pct float64) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	hue := 120 * (1 - pct/100)
	return fmt.Sprintf("hsl(%.0f,70%%,45%%)", hue)
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

// jsStringEscape escapes for embedding in a JS single-quoted string literal.
func jsStringEscape(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`, "\n", `\n`, "\r", `\r`)
	return r.Replace(s)
}
