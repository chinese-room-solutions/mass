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

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
)

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
)

// fileBrowserScript is the shared file browser logic used by both the module
// file picker and the settings directory picker. Must load before shellScripts.
var fileBrowserScript string

// shellScripts contains the inline JS for the main shell:
// module search filter, auto-save debounce, and resizable left panel.
var shellScripts string

// settingsAutoSaveScript adds debounced auto-save for settings inputs.
// Datastar's data-on:sl-change doesn't work with Shoelace custom events,
// so we use a global event listener (same pattern as module config auto-save).
var settingsAutoSaveScript string

// settingsBrowseScript is the inline JS for the directory browser dialog on the Settings page.
var settingsBrowseScript string

// schedulerScript is the inline JS for the scheduler tab: spacer sync, deselect,
// and load-model dialog logic.
var schedulerScript string

// modelsTabScript is the inline JS for the models tab: spacer sync, download
// row management, progress tracking, and model loading/import.
var modelsTabScript string

func init() {
	fileBrowserScript = "<script>" + fileBrowserJS + "</script>"
	shellScripts = "<script>" + shellScriptsJS + "</script>"
	settingsAutoSaveScript = "<script>" + settingsAutoSaveJS + "</script>"
	settingsBrowseScript = "<script>" + settingsBrowseJS + "</script>"
	schedulerScript = "<script>" + schedulerJS + "</script>"
	modelsTabScript = "<script>" + modelsTabJS + "</script>"
}

// DashboardData holds all data needed to render the main dashboard shell.
type DashboardData struct {
	Modules           []ModuleViewData
	ActiveModule      string
	ListenAddr        string
	DataDir           string
	AuthTokenSet      bool
	ModelIdleTimeout  string // idle timeout for dynamic model eviction (e.g. "5m")
	ModuleIdleTimeout string // idle timeout for on-demand module shutdown (e.g. "5s")
	LogLevel          string // zerolog level name (e.g. "debug", "info")
	Theme             string // "dark" or "light"
	DevMode           bool
	ConfigDir         string // directory containing config.yml (shown on Settings tab)
	LogsDir           string // path to logs directory (shown on Settings tab)
	AgentsHTML        string // pre-rendered agents list for initial page load
	TLSEnabled        bool
	TLSCertFile       string
}

// ModuleViewData holds template data for a single module.
type ModuleViewData struct {
	Name       string
	State      scheduler.ModuleState
	Version    string
	Error      error
	LaunchMode config.LaunchMode
	AutoStart  bool
	Debug      bool
	HasIcon    bool
}

// ModelGroupView holds template data for a model group in the Models tab.
type ModelGroupView struct {
	BaseName    string
	ModelType   string // "Chat", "Embedding"
	HasVision   bool   // true if any variant has a sibling mmproj
	HasThinking bool   // true if any variant supports thinking/reasoning
	Variants    []ModelVariantView
}

// ModelVariantView holds template data for a single model file.
type ModelVariantView struct {
	Filename      string
	DisplayName   string // e.g. "unsloth/Qwen3.5-4B-UD-Q4_K_XL.gguf"
	Path          string // absolute path (for delete action)
	Quantization  string
	SizeFormatted string // e.g. "4.2 GB"
	IsMmproj      bool   // true for vision projector files
}

// SchedulerInstanceView holds template data for a single loaded model instance.
type SchedulerInstanceView struct {
	Fingerprint string
	Path        string
	Filename    string   // base filename extracted from Path
	Type        string   // "Chat" or "Embedding"
	Source      string   // caller identity: "direct", "module: <name>", or custom value
	Mode        string   // "dynamic" or "static"
	AgentID     string   // ID of the agent running this model
	AgentName   string   // human-readable name of the agent
	DeviceIDs   []string // device(s) the model is loaded on
	ActiveReqs  int64
	Status      string // "Active", "Idle", or "Loading"
}

// SchedulerInstancePropsView holds template data for the scheduler instance properties panel.
type SchedulerInstancePropsView struct {
	Fingerprint string
	Filename    string
	Path        string
	Type        string // "Chat" or "Embedding"
	Source      string
	Mode        string   // "dynamic" or "static"
	AgentID     string   // ID of the agent running this model
	AgentName   string   // human-readable name of the agent
	DeviceIDs   []string // device(s) the model is loaded on
	Status      string
	ActiveReqs  int64
	Config      []SchedulerConfigEntry
}

// SchedulerConfigEntry is a key-value pair for display in the properties panel.
type SchedulerConfigEntry struct {
	Key   string
	Value string
}

// badgeClass returns the CSS class for a model type badge.
func badgeClass(modelType string) string {
	switch strings.ToLower(modelType) {
	case "chat":
		return "mass-badge mass-badge-chat"
	case "embedding":
		return "mass-badge mass-badge-embedding"
	case "mmproj":
		return "mass-badge mass-badge-mmproj"
	default:
		return "mass-badge"
	}
}

// dashboardSignals returns the JSON data-signals string for the dashboard.
func dashboardSignals(data DashboardData) string {
	theme := data.Theme
	if theme == "" {
		theme = "dark"
	}
	signals := map[string]any{
		"activeTab":        "modules",
		"activeModule":     data.ActiveModule,
		"moduleView":       "ui",
		"sidebarCollapsed": false,
		"moduleSearch":     "",
		"addModuleOpen":    false,
		"confirmOpen":      false,
		"confirmUninstall": "",
		"packagePath":      "",
		"installing":       false,
		"listenAddr":       data.ListenAddr,
		"dataDir":          data.DataDir,
		"authToken": func() string {
			if data.AuthTokenSet {
				return "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022"
			}
			return ""
		}(),
		"authTokenSet":           data.AuthTokenSet,
		"authTokenEdited":        false,
		"theme":                  theme,
		"devMode":                data.DevMode,
		"modelsHfQuery":          "",
		"modelsFilter":           "",
		"selectedModelPath":      "",
		"modelsInstallOpen":      false,
		"confirmDeleteModelPath": "",
		"confirmDeleteModelName": "",
		"selectedSchedulerFp":    "",
		"modelIdleTimeout":       data.ModelIdleTimeout,
		"moduleIdleTimeout":      data.ModuleIdleTimeout,
		"logLevel":               data.LogLevel,
		"tlsEnabled":             data.TLSEnabled,
		"tlsCertFile":            data.TLSCertFile,
	}
	// Per-module debug signals.
	for _, p := range data.Modules {
		signals["debug_"+p.Name] = p.Debug
	}
	b, err := json.Marshal(signals)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// HtmlThemeClass returns the <html> class string for the given theme.
func HtmlThemeClass(theme string) string {
	if theme == "light" {
		return "sl-theme-light"
	}
	return "sl-theme-dark dark"
}

// htmlThemeClass is the unexported alias used by templ templates.
func htmlThemeClass(theme string) string { return HtmlThemeClass(theme) }

// BodyThemeClass returns the <body> class string for the given theme.
func BodyThemeClass(theme string) string {
	if theme == "light" {
		return "bg-neutral-100 text-neutral-900"
	}
	return "bg-neutral-950 text-neutral-100"
}

// bodyThemeClass is the unexported alias used by templ templates.
func bodyThemeClass(theme string) string { return BodyThemeClass(theme) }

// initial returns the first letter of name, uppercased.
func initial(name string) string {
	runes := []rune(name)
	if len(runes) == 0 {
		return "?"
	}
	return strings.ToUpper(string(runes[0]))
}

// statusDotClass returns Tailwind classes for the status dot.
func statusDotClass(state scheduler.ModuleState, launchMode config.LaunchMode) string {
	base := "w-2.5 h-2.5 rounded-full flex-shrink-0"
	switch state {
	case scheduler.StateRunning:
		return base + " bg-green-400"
	case scheduler.StateStarting, scheduler.StateStopping:
		return base + " bg-yellow-400 animate-pulse"
	case scheduler.StateError:
		return base + " bg-red-400"
	default:
		if launchMode == config.LaunchModeOnDemand {
			return base + " bg-blue-400"
		}
		return base + " bg-neutral-600"
	}
}

// statusBadgeVariant returns the Shoelace badge variant for a module state.
func statusBadgeVariant(state scheduler.ModuleState) string {
	switch state {
	case scheduler.StateRunning:
		return "success"
	case scheduler.StateStarting, scheduler.StateStopping:
		return "warning"
	case scheduler.StateError:
		return "danger"
	default:
		return "neutral"
	}
}

// RenderError returns an error alert HTML fragment.
func RenderError(msg string) string {
	return fmt.Sprintf(
		`<div id="error-display"><sl-alert variant="danger" open>%s</sl-alert></div>`,
		html.EscapeString(msg),
	)
}

// RenderModuleStatus returns HTML for the module status area (used by SSE updates).
func RenderModuleStatus(name string, state scheduler.ModuleState, err error, progress string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `<div id="module-status-%s" class="flex items-center gap-2">`, html.EscapeString(name))
	fmt.Fprintf(&b, `<sl-badge variant="%s" pill>%s</sl-badge>`,
		statusBadgeVariant(state), html.EscapeString(state.String()))
	if progress != "" {
		fmt.Fprintf(&b, `<span class="text-sm text-neutral-400">%s</span>`, html.EscapeString(progress))
	}
	if err != nil {
		fmt.Fprintf(&b, `<span class="text-sm text-red-400">%s</span>`, html.EscapeString(err.Error()))
	}
	b.WriteString(`</div>`)
	return b.String()
}

// RenderSidebarDot returns HTML for the status dot (SSE update).
func RenderSidebarDot(name string, state scheduler.ModuleState, launchMode config.LaunchMode) string {
	return fmt.Sprintf(
		`<span id="sidebar-dot-%s" class="%s absolute -bottom-0.5 -right-0.5 border border-neutral-900"></span>`,
		html.EscapeString(name), statusDotClass(state, launchMode))
}

// launchModeIcon returns the Bootstrap icon name for a launch mode.
func launchModeIcon(mode config.LaunchMode) string {
	switch mode {
	case config.LaunchModeOnDemand:
		return "activity"
	default:
		return "hand-index"
	}
}

// launchModeIconColor returns a CSS color style for the launch mode icon.
func launchModeIconColor(mode config.LaunchMode) string {
	switch mode {
	case config.LaunchModeOnDemand:
		return "opacity:1;color:var(--sl-color-primary-400)"
	default:
		return ""
	}
}

// launchModeTooltip returns the tooltip text for a launch mode icon.
func launchModeTooltip(mode config.LaunchMode) string {
	switch mode {
	case config.LaunchModeOnDemand:
		return "On Demand"
	default:
		return "Manual"
	}
}

// RenderLaunchModeDropdown returns the launch mode dropdown HTML for a module.
func RenderLaunchModeDropdown(name string, mode config.LaunchMode) string {
	var buf bytes.Buffer
	_ = launchModeDropdown(name, mode).Render(context.Background(), &buf)
	return buf.String()
}

// RenderModuleActions returns the Start/Stop icon button area with a stable ID
// so it can be patched independently via SSE status events.
// Delegates to the moduleActionIcons templ component as the single source of truth.
func RenderModuleActions(name string, state scheduler.ModuleState) string {
	var buf bytes.Buffer
	_ = moduleActionIcons(name, state).Render(context.Background(), &buf)
	return fmt.Sprintf(`<div id="module-actions-%s" class="flex items-center justify-center" style="min-width:2rem">`,
		html.EscapeString(name)) + buf.String() + `</div>`
}

// RenderWelcomeState returns the welcome/empty state HTML for #module-content.
// When empty is true, it shows "No modules configured"; otherwise "Select a module".
func RenderWelcomeState(empty bool) string {
	heading := "Select a module"
	subtext := "Choose a module from the sidebar, or install a new one."
	if empty {
		heading = "No modules configured"
		subtext = "Add a module to get started."
	}
	return `<div class="flex flex-col items-center justify-center h-64 text-center">` +
		`<h2 class="text-lg font-semibold mb-2">` + heading + `</h2>` +
		`<p class="text-neutral-400 text-sm mb-4">` + subtext + `</p>` +
		`<sl-button variant="primary" size="small" data-on:click="$addModuleOpen = true">` +
		`<sl-icon slot="prefix" name="plus-lg"></sl-icon>Install New Module</sl-button></div>`
}

// RenderRestartNotice returns the yellow restart-needed banner for pe-restart-notice.
func RenderRestartNotice() string {
	return `<div id="pe-restart-notice">` +
		`<sl-alert variant="warning" open style="--sl-spacing-large:0.5rem;font-size:0.8rem">` +
		`<sl-icon slot="icon" name="exclamation-triangle" style="font-size:1rem"></sl-icon>` +
		`Configuration saved. Restart the module to apply changes.` +
		`</sl-alert></div>`
}

// RenderRestartNoticeClear returns an empty pe-restart-notice div to clear the banner.
func RenderRestartNoticeClear() string {
	return `<div id="pe-restart-notice"></div>`
}

// pluralS returns "s" if n != 1, for pluralization.
func pluralS(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// ansiRe matches ANSI SGR escape sequences: ESC [ <params> m
var ansiRe = regexp.MustCompile(`\x1b\[([0-9;]*)m`)

// urlRe matches http:// and https:// URLs in text.
var urlRe = regexp.MustCompile(`https?://[^\s<>"` + "`" + `]+`)

// logLevelColor returns a CSS color for a plain-text log line based on the
// level token (INF, DBG, WRN, ERR, FTL, TRC) found after the timestamp.
func logLevelColor(line string) string {
	// Fast scan: level token appears right after the timestamp, separated by a space.
	// Format: "2026-03-10T00:54:16+01:00 INF message..."
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
	return "var(--mass-log-inf)" // default green
}

// renderStructuredLogLine renders a plain-text structured log line
// (timestamp LEVEL message key=value...) with colored parts:
// - timestamp: dim gray
// - level token: colored by severity
// - message: white
// - key=value pairs: dim cyan keys, neutral values
func renderStructuredLogLine(raw string) string {
	esc := html.EscapeString

	// Try to parse: "timestamp LEVEL message key=value..."
	// Find first space (after timestamp).
	sp1 := strings.IndexByte(raw, ' ')
	if sp1 < 0 {
		return `<p class="whitespace-pre-wrap leading-tight" style="color:var(--mass-log-inf)">` + esc(raw) + `</p>`
	}

	// Check if next token is a known level.
	rest := raw[sp1+1:]
	sp2 := strings.IndexByte(rest, ' ')
	var level, after string
	if sp2 >= 0 {
		level = rest[:sp2]
		after = rest[sp2+1:]
	} else {
		level = rest
		after = ""
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

	// Split "after" into message and key=value pairs.
	// Key=value pairs are tokens containing '=' with no spaces in the key.
	// Scan from the end to find where key=value pairs start.
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
	// Timestamp in dim gray.
	fmt.Fprintf(&b, `<span style="color:var(--mass-log-time)">%s</span> `, esc(timestamp))
	// Level token in its color.
	fmt.Fprintf(&b, `<span style="color:%s;font-weight:bold">%s</span>`, levelColor, esc(level))
	// Message.
	if msg != "" {
		fmt.Fprintf(&b, ` <span style="color:var(--mass-log-msg)">%s</span>`, esc(msg))
	}
	// Key=value pairs with dim styling.
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

// linkifyURLs replaces http/https URLs in HTML text content with clickable <a> tags.
func linkifyURLs(htmlStr string) string {
	return urlRe.ReplaceAllStringFunc(htmlStr, func(u string) string {
		return fmt.Sprintf(`<a href="%s" target="_blank" class="underline hover:text-white">%s</a>`, u, u)
	})
}

// RenderLogLine converts a raw log line into an HTML <p> element with
// consistent colored parts. ANSI escape codes are stripped first so that
// all lines go through the same structured renderer.
func RenderLogLine(raw string) string {
	// Strip ANSI escape codes so all lines render with the same style.
	plain := ansiRe.ReplaceAllString(raw, "")
	return linkifyURLs(renderStructuredLogLine(plain))
}

// RenderLogView returns the HTML for the live log stream view.
// The caller patches this into #module-content using inner mode.
// If history is non-empty, the buffered lines are pre-rendered instead of
// the "Waiting for log output..." placeholder.
func RenderLogView(name string, history []string) string {
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
	return fmt.Sprintf(`<div class="space-y-3">
<div class="flex items-center gap-2 text-sm font-medium text-neutral-300">
  <sl-icon name="terminal"></sl-icon>
  <span>%s — Live Logs</span>
</div>
<div id="log-entries" class="font-mono text-xs bg-neutral-900 rounded-lg p-4 border border-neutral-800 min-h-64 max-h-[70vh] overflow-y-auto space-y-px">
  %s
</div>
<script>
(function(){var el=document.getElementById('log-entries');if(el)el.scrollTop=el.scrollHeight;
var obs=new MutationObserver(function(){
  var ph=document.getElementById('log-placeholder');if(ph)ph.remove();
  el.scrollTop=el.scrollHeight});
obs.observe(el,{childList:true})})();
</script>
</div>`, esc(name), entries)
}

// RenderSystemLogLine renders a MASS system log line (console-formatted with ANSI codes)
// into an HTML <p> element. Uses the same ANSI parser as module logs.
func RenderSystemLogLine(raw string) string {
	return RenderLogLine(raw)
}

// RenderModuleList returns the outer HTML of #module-list for SSE updates
// after module discovery or removal.
// Delegates to the moduleListItems templ component as the single source of truth.
func RenderModuleList(modules []ModuleViewData, activeModule string) string {
	var buf bytes.Buffer
	_ = moduleListItems(modules, activeModule).Render(context.Background(), &buf)
	return `<div id="module-list" class="flex-1 overflow-y-auto py-1">` + buf.String() + `</div>`
}

// AgentView holds data for rendering an agent row in the Agents tab.
type AgentView struct {
	ID          string
	Name        string
	Online      bool
	AllDisabled bool // true when every device on this agent is disabled
	Devices     []ComputeView
}

// RenderAgentsList returns the full HTML for the agents list area (with wrapper div).
func RenderAgentsList(agents []AgentView) string {
	return `<div id="agents-list" class="space-y-3">` + RenderAgentsListInner(agents) + `</div>`
}

// RenderAgentsListInner returns just the inner HTML of the agents list (no wrapper).
func RenderAgentsListInner(agents []AgentView) string {
	var b strings.Builder
	b.WriteString(`<style>.agent-stats-row{display:grid;font-family:var(--sl-font-mono);grid-template-columns:10ch 6rem 12ch 6rem;align-items:center;gap:0 2rem}.agent-stats-row .bench-val{white-space:nowrap}.agent-stats-row>div.text-xs{text-align:center}</style>`)
	if len(agents) == 0 {
		b.WriteString(`<p class="text-xs text-neutral-500 text-center py-8">No agents registered.</p>`)
	}
	for _, ag := range agents {
		statusColor := "var(--sl-color-success-400)"
		statusIcon := "circle-fill"
		statusText := "Online"
		if !ag.Online {
			statusColor = "var(--sl-color-neutral-400)"
			statusIcon = "circle"
			statusText = "Offline"
		}

		agentIDSafe := html.EscapeString(ag.ID)
		// Build filter text: agent name + status + device names/types.
		filterText := strings.ToLower(ag.Name + " " + statusText)
		for _, d := range ag.Devices {
			filterText += " " + strings.ToLower(d.DeviceName+" "+d.Type)
		}
		agentCardStyle := ""
		if ag.AllDisabled {
			agentCardStyle = ` style="--mass-border:rgba(133,77,14,0.5)"`
		}
		fmt.Fprintf(&b, `<sl-details class="agent-card" id="agent-card-%s" data-filter-text="%s"%s>`,
			agentIDSafe, html.EscapeString(filterText), agentCardStyle)
		b.WriteString(`<div slot="summary" class="flex items-center gap-3 w-full">`)
		fmt.Fprintf(&b, `<sl-tooltip content="%s"><sl-icon name="%s" style="font-size:0.5rem;color:%s"></sl-icon></sl-tooltip>`, statusText, statusIcon, statusColor)
		fmt.Fprintf(&b, `<span class="text-sm font-medium" style="color:var(--mass-text)">%s</span>`, html.EscapeString(ag.Name))
		fmt.Fprintf(&b, `<span class="text-xs text-neutral-500 ml-auto mr-2">%d device(s)</span>`, len(ag.Devices))
		if ag.Online && len(ag.Devices) > 0 {
			agentID := html.EscapeString(ag.ID)
			agToggleIcon := "toggle2-on"
			agToggleColor := "var(--sl-color-success-500)"
			agToggleTip := "Pause all devices"
			if ag.AllDisabled {
				agToggleIcon = "toggle2-off"
				agToggleColor = "var(--sl-color-neutral-500)"
				agToggleTip = "Resume all devices"
			}
			fmt.Fprintf(&b, `<sl-tooltip content="%s"><sl-icon-button name="%s" style="font-size:1.2rem;color:%s" `+
				`onclick="event.stopPropagation();fetch('/api/agents/toggle?agent=%s',{method:'POST'})"></sl-icon-button></sl-tooltip>`,
				agToggleTip, agToggleIcon, agToggleColor, url.QueryEscape(agentID))
			fmt.Fprintf(&b, `<sl-button size="small" variant="text" id="bench-all-%s" `+
				`onclick="event.stopPropagation()" `+
				`data-on:click="@post('/api/agents/benchmark?agentIds=%s')">`,
				agentID, agentID)
			b.WriteString(`<sl-icon slot="prefix" name="speedometer2"></sl-icon>Bench All</sl-button>`)
		}
		b.WriteString(`</div>`)

		// Expanded content: device cards
		if len(ag.Devices) == 0 {
			b.WriteString(`<p class="text-xs text-neutral-500 py-2">No compute devices detected.</p>`)
		}
		for _, d := range ag.Devices {
			icon := "cpu"
			iconColor := "var(--mass-blue)"
			badgeVariant := "primary"
			if d.Type == "GPU" {
				icon = "gpu-card"
				iconColor = "var(--mass-green)"
				badgeVariant = "success"
			}
			// Prefix device IDs with agent ID to ensure uniqueness across agents.
			scopedID := agentIDSafe + "_" + html.EscapeString(d.DeviceID)
			devIDSafe := scopedID
			cardBorder := "border-neutral-800"
			if !d.Enabled {
				cardBorder = "border-yellow-900/50"
			}
			fmt.Fprintf(&b, `<div class="bg-neutral-900 rounded-lg border %s p-3 mb-2 space-y-2">`, cardBorder)

			// Header row: icon + name + badge + toggle + bench button.
			b.WriteString(`<div class="flex items-center gap-3">`)
			fmt.Fprintf(&b, `<sl-icon name="%s" style="font-size:1.1rem;color:%s"></sl-icon>`, icon, iconColor)
			fmt.Fprintf(&b, `<span class="text-sm font-medium text-white">%s</span>`, html.EscapeString(d.DeviceName))
			fmt.Fprintf(&b, `<sl-badge variant="%s" pill>%s</sl-badge>`, badgeVariant, html.EscapeString(d.Type))
			b.WriteString(`<span class="ml-auto"></span>`)
			devToggleIcon := "toggle2-on"
			devToggleColor := "var(--sl-color-success-500)"
			devToggleTip := "Pause scheduling"
			if !d.Enabled {
				devToggleIcon = "toggle2-off"
				devToggleColor = "var(--sl-color-neutral-500)"
				devToggleTip = "Resume scheduling"
			}
			fmt.Fprintf(&b, `<sl-tooltip content="%s"><sl-icon-button name="%s" style="font-size:1.2rem;color:%s" `+
				`onclick="event.stopPropagation();fetch('/api/agents/devices/toggle?queue=%s',{method:'POST'})"></sl-icon-button></sl-tooltip>`,
				devToggleTip, devToggleIcon, devToggleColor, url.QueryEscape(d.QueueName))
			fmt.Fprintf(&b, `<sl-button size="small" variant="text" data-on:click="@post('/api/agents/benchmark?agentIds=%s&deviceIds=%s')">`,
				html.EscapeString(ag.ID), html.EscapeString(d.DeviceID))
			b.WriteString(`<sl-icon slot="prefix" name="speedometer2"></sl-icon>Bench</sl-button>`)
			b.WriteString(`</div>`)

			// Stats row: Bandwidth, RAM gauge, Compute, CPU/GPU gauge.
			if d.Loading {
				b.WriteString(`<div class="flex items-center justify-center gap-2 text-xs">`)
				b.WriteString(`<sl-spinner style="font-size:0.875rem;--track-width:2px"></sl-spinner>`)
				b.WriteString(`<span class="text-neutral-500">Benchmarking...</span>`)
				b.WriteString(`</div>`)
			} else {
				hasGauges := d.HasStats || d.HasUtilization
				if hasGauges || d.HasBenchmark {
					b.WriteString(`<div class="agent-stats-row">`)

					// Memory bandwidth.
					memTip := "RAM throughput (ggml device-local add)"
					if d.Type == "GPU" {
						memTip = "VRAM throughput (ggml device-local add)"
					}
					b.WriteString(`<div class="text-xs">`)
					fmt.Fprintf(&b, `<sl-tooltip content="%s"><span class="text-neutral-500" style="cursor:help">Memory</span></sl-tooltip>`, memTip)
					fmt.Fprintf(&b, `<div class="text-neutral-200 font-mono mt-0.5 bench-val" id="bench-bw-%s">`, devIDSafe)
					if d.HasBenchmark {
						b.WriteString(formatMemoryBW(d.MemoryGBs))
					} else {
						b.WriteString(`<span class="text-neutral-500">—</span>`)
					}
					b.WriteString(`</div></div>`)

					// Memory gauge (always emit a grid item for alignment).
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

					// Compute (Q4_K matmul throughput).
					b.WriteString(`<div class="text-xs">`)
					b.WriteString(`<sl-tooltip content="Q4_K quantized matmul throughput (comparable across devices)"><span class="text-neutral-500" style="cursor:help">Compute</span></sl-tooltip>`)
					fmt.Fprintf(&b, `<div class="text-neutral-200 font-mono mt-0.5 bench-val" id="bench-comp-%s">`, devIDSafe)
					if d.HasBenchmark {
						b.WriteString(formatFlops(d.ComputeGFlops))
					} else {
						b.WriteString(`<span class="text-neutral-500">—</span>`)
					}
					b.WriteString(`</div></div>`)

					// Utilization gauge (always emit a grid item for alignment).
					if d.HasUtilization {
						computeLabel := "CPU"
						if d.Type == "GPU" {
							computeLabel = "GPU"
						}
						pct := d.UtilizationPct
						if pct > 100 {
							pct = 100
						}
						writeGauge(&b, scopedID+"-util", computeLabel, pct,
							fmt.Sprintf("%.0f%%", pct))
					} else {
						b.WriteString(`<div></div>`)
					}

					b.WriteString(`</div>`)
				} else {
					b.WriteString(`<div class="text-xs"><span class="text-neutral-500 italic">No benchmark data — click Bench</span></div>`)
				}
			}

			b.WriteString(`</div>`)
		}
		b.WriteString(`</sl-details>`)
	}
	return b.String()
}

// ComputeView holds data for rendering a device card in the Compute tab.
type ComputeView struct {
	DeviceID       string
	DeviceName     string
	Type           string  // "CPU" or "GPU"
	MemoryMB       int     // total RAM/VRAM in MB (0 = unknown)
	UsedMemoryMB   int     // currently used RAM/VRAM in MB (0 = unknown)
	UtilizationPct float64 // 0-100, compute utilization
	HasUtilization bool    // true = show compute utilization bar
	MemoryGBs      float64
	ComputeGFlops  float64 // Q4_K matmul GFLOPS (comparable across CPU/GPU)
	HasBenchmark   bool
	HasStats       bool   // true = show memory utilization bar
	Loading        bool   // true = show spinner instead of benchmark values
	Enabled        bool   // true = participates in scheduling
	QueueName      string // device queue name for toggle URL
}

// writeGauge renders an SVG ring gauge with a label, percentage, and subtitle.
// The ring uses stroke-dashoffset for the fill arc. Animation is handled by JS
// after DOM patching (see patchAgentsList ExecuteScript).
func writeGauge(b *strings.Builder, id, label string, pct float64, subtitle string) {
	// SVG ring parameters: radius=28, stroke=5, viewBox 66x66 centered at 33,33.
	const r = 28
	circumference := 2 * 3.14159265 * r // ~175.93
	offset := circumference * (1 - pct/100)
	color := BarColor(pct)

	b.WriteString(`<div class="flex flex-col items-center" style="width:58px">`)
	fmt.Fprintf(b, `<svg width="58" height="58" viewBox="0 0 66 66" class="gauge-ring" id="gauge-%s" data-gauge-pct="%.1f">`, html.EscapeString(id), pct)
	// Track ring (theme-aware).
	b.WriteString(`<circle cx="33" cy="33" r="28" fill="none" stroke="var(--sl-color-neutral-300)" stroke-width="5" opacity="0.25"/>`)
	// Value ring (rotated -90deg so 0% starts at top).
	fmt.Fprintf(b, `<circle cx="33" cy="33" r="28" fill="none" stroke="%s" stroke-width="5" stroke-linecap="round" stroke-dasharray="%.2f" stroke-dashoffset="%.2f" transform="rotate(-90 33 33)" style="transition:stroke-dashoffset .8s ease,stroke .4s ease"/>`, color, circumference, offset)
	// Center text (theme-aware).
	fmt.Fprintf(b, `<text x="33" y="33" text-anchor="middle" dominant-baseline="central" fill="var(--mass-text)" font-size="12" font-family="monospace" font-weight="600">%.0f%%</text>`, pct)
	b.WriteString(`</svg>`)
	fmt.Fprintf(b, `<span class="text-xs text-neutral-500 mt-0.5">%s</span>`, label)
	fmt.Fprintf(b, `<span class="text-xs text-neutral-500 font-mono" style="font-size:0.65rem;white-space:nowrap">%s</span>`, subtitle)
	b.WriteString(`</div>`)
}

// BarColor returns an HSL color string that transitions from green to red.
// 0% = green (hsl(120,70%,45%)), 100% = red (hsl(0,70%,50%)).
func BarColor(pct float64) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	hue := 120 * (1 - pct/100) // 120 (green) → 0 (red)
	return fmt.Sprintf("hsl(%.0f,70%%,45%%)", hue)
}

// FormatMemMB formats a memory value in MB into a human-readable string.
func FormatMemMB(mb int) string {
	if mb >= 1024 {
		return fmt.Sprintf("%.1f GB", float64(mb)/1024)
	}
	return fmt.Sprintf("%d MB", mb)
}

// formatFlops formats GFLOPS into a human-readable string with appropriate unit.
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

// formatMemoryBW formats GB/s into a human-readable string with appropriate unit.
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
