package web

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/starfederation/datastar-go/datastar"
)

// handleSSEEvents is the main SSE endpoint that pushes live updates to the browser.
func (h *Handler) handleSSEEvents(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	ch := h.broker.Subscribe()
	defer h.broker.Unsubscribe(ch)

	// Replay-on-connect: browsers may drop the SSE connection when the tab
	// is backgrounded. Events broadcast during the gap are lost (clients=0).
	// Each replay method sends the current state so the UI catches up.
	h.replayModuleStates(sse)
	h.replaySystemLogs(sse)
	h.pushSchedulerContent(sse)
	h.replayAgents(sse)

	for {
		select {
		case <-sse.Context().Done():
			return
		case evt := <-ch:
			switch evt.Type {
			case EventTypeStatus:
				if err := sse.PatchElements(templates.RenderModuleStatus(evt.ModuleName, evt.State, evt.Error, "")); err != nil {
					h.logger.Debug().Err(err).Str("module", evt.ModuleName).Msg("SSE patch module-status (expected if no module selected)")
				}
				launchMode := config.LaunchModeManual
				if pc := h.cfg.FindModule(evt.ModuleName); pc != nil {
					launchMode = pc.EffectiveLaunchMode()
				}
				if err := sse.PatchElements(templates.RenderSidebarDot(evt.ModuleName, evt.State, launchMode),
					datastar.WithSelector("#sidebar-dot-"+evt.ModuleName)); err != nil {
					h.logger.Warn().Err(err).Str("module", evt.ModuleName).Msg("SSE patch sidebar-dot failed")
				}
				if err := sse.PatchElements(templates.RenderModuleActions(evt.ModuleName, evt.State),
					datastar.WithSelector("#module-actions-"+evt.ModuleName)); err != nil {
					h.logger.Warn().Err(err).Str("module", evt.ModuleName).Msg("SSE patch module-actions failed")
				}
				if evt.State != scheduler.StateRunning {
					_ = sse.PatchElements(templates.RenderRestartNoticeClear())
				}
			case EventTypeProgress:
				_ = sse.PatchElements(templates.RenderModuleStatus(evt.ModuleName, scheduler.StateStarting, nil, evt.Progress))
			case EventTypeLog:
				line := templates.RenderLogLine(evt.LogLine)
				_ = sse.ExecuteScript(appendLogLineScript(line))
			case EventTypeSystemLog:
				rendered := templates.RenderSystemLogLine(evt.SysLogLine)
				_ = sse.ExecuteScript(appendSysLogLineScript(rendered))
			case EventTypePoolChange:
				if evt.PoolChange == scheduler.PoolChangeList {
					h.pushSchedulerContent(sse)
					h.closeSchedulerPropsIfGone(sse)
				} else {
					h.pushSchedulerInstanceStatus(sse, evt.Fingerprint)
				}
			case EventTypeDownload:
				jsFile := jsStringEscape(evt.DlFilename)
				if evt.DlStart {
					jsGroup := jsStringEscape(evt.DlGroupName)
					_ = sse.ExecuteScript(fmt.Sprintf(`window.__massModelDlStart('%s','%s')`, jsFile, jsGroup))
				} else if evt.DlPaused {
					_ = sse.ExecuteScript(fmt.Sprintf(`window.__massModelDlPaused('%s')`, jsFile))
				} else if evt.DlCancelled {
					_ = sse.ExecuteScript(fmt.Sprintf(`window.__massModelDlCancel('%s')`, jsFile))
					_ = sse.ExecuteScript(fmt.Sprintf(`window.__hfDlCancel('%s')`, jsFile))
				} else if evt.Error != nil {
					jsErr := jsStringEscape(evt.Error.Error())
					_ = sse.ExecuteScript(fmt.Sprintf(`window.__hfDlErr('%s','%s')`, jsFile, jsErr))
					_ = sse.ExecuteScript(fmt.Sprintf(`window.__massModelDlErr('%s','%s')`, jsFile, jsErr))
				} else if evt.DlDone {
					_ = sse.ExecuteScript(fmt.Sprintf(`window.__hfDlDone('%s')`, jsFile))
					_ = sse.ExecuteScript(fmt.Sprintf(`window.__massModelDlDone('%s')`, jsFile))
					jsPath := jsStringEscape(evt.DlPath)
					_ = sse.ExecuteScript(fmt.Sprintf(
						`(function(){var el=document.querySelector('[data-bind="modelPath"]');`+
							`if(el){el.value='%s';el.dispatchEvent(new Event('input',{bubbles:true}))}})()`,
						jsPath))
					_ = sse.ExecuteScript(`(function(){var b=document.getElementById('mass-models-refresh-btn');if(b)b.click()})()`)
				} else {
					pct := 0
					if evt.DlTotal > 0 {
						pct = int(100 * evt.DlDownloaded / evt.DlTotal)
					}
					_ = sse.ExecuteScript(fmt.Sprintf(`window.__hfDlProgress('%s',%d)`, jsFile, pct))
					_ = sse.ExecuteScript(fmt.Sprintf(`window.__massModelDlProgress('%s',%d,%d,%d)`, jsFile, pct, evt.DlDownloaded, evt.DlTotal))
				}
			case EventTypeAgentChange:
				h.patchAgentsList(sse, h.buildAgentViews())
			case EventTypeAgentStats:
				h.patchAgentStats(sse)
			}
		}
	}
}

// parseDownloadLine detects structured download progress lines from module
// stderr and converts them into EventTypeDownload events.
func parseDownloadLine(moduleName, line string) (SSEEvent, bool) {
	if strings.HasPrefix(line, "MASS_DL_DONE:") {
		parts := strings.SplitN(line[len("MASS_DL_DONE:"):], ":", 2)
		if len(parts) == 2 {
			return SSEEvent{
				Type: EventTypeDownload, ModuleName: moduleName,
				DlFilename: parts[0], DlDone: true, DlPath: parts[1],
			}, true
		}
	}
	if strings.HasPrefix(line, "MASS_DL_ERR:") {
		parts := strings.SplitN(line[len("MASS_DL_ERR:"):], ":", 2)
		if len(parts) == 2 {
			return SSEEvent{
				Type: EventTypeDownload, ModuleName: moduleName,
				DlFilename: parts[0], Error: errors.New(parts[1]),
			}, true
		}
	}
	if strings.HasPrefix(line, "MASS_DL:") {
		parts := strings.SplitN(line[len("MASS_DL:"):], ":", 3)
		if len(parts) == 3 {
			dl, _ := strconv.ParseInt(parts[1], 10, 64)
			tot, _ := strconv.ParseInt(parts[2], 10, 64)
			return SSEEvent{
				Type: EventTypeDownload, ModuleName: moduleName,
				DlFilename: parts[0], DlDownloaded: dl, DlTotal: tot,
			}, true
		}
	}
	return SSEEvent{}, false
}

// replayModuleStates sends the current sidebar dot and action buttons for all
// modules so the client catches up after an SSE reconnect.
func (h *Handler) replayModuleStates(sse *datastar.ServerSentEventGenerator) {
	allModules := h.orch.GetAllModules()
	for name, mp := range allModules {
		launchMode := config.LaunchModeManual
		if pc := h.cfg.FindModule(name); pc != nil {
			launchMode = pc.EffectiveLaunchMode()
		}
		_ = sse.PatchElements(templates.RenderSidebarDot(name, mp.State, launchMode),
			datastar.WithSelector("#sidebar-dot-"+name))
		_ = sse.PatchElements(templates.RenderModuleActions(name, mp.State),
			datastar.WithSelector("#module-actions-"+name))
	}
}

// handleSyncLogs returns the current system logs and (optionally) the active
// module's live logs as pre-rendered HTML so the browser can catch up after
// regaining focus.
func (h *Handler) handleSyncLogs(w http.ResponseWriter, r *http.Request) {
	type syncResponse struct {
		SysLog    string `json:"sysLog"`
		ModuleLog string `json:"moduleLog,omitempty"`
	}

	var resp syncResponse

	if h.sysLog != nil {
		var sb strings.Builder
		for _, line := range h.sysLog.Lines() {
			sb.WriteString(templates.RenderSystemLogLine(line))
		}
		resp.SysLog = sb.String()
	}

	if name := r.URL.Query().Get("module"); name != "" {
		lines := h.orch.GetLogHistory(name)
		if len(lines) > 0 {
			var sb strings.Builder
			for _, line := range lines {
				sb.WriteString(templates.RenderLogLine(line))
			}
			resp.ModuleLog = sb.String()
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

// replaySystemLogs clears the system log container and replays the most recent
// lines so the client catches up after an SSE reconnect.
func (h *Handler) replaySystemLogs(sse *datastar.ServerSentEventGenerator) {
	if h.sysLog == nil {
		return
	}
	lines := h.sysLog.Lines()
	_ = sse.ExecuteScript("(function(){var el=document.getElementById('syslog-entries');if(el)el.innerHTML=''})()")
	for _, line := range lines {
		rendered := templates.RenderSystemLogLine(line)
		_ = sse.ExecuteScript(appendSysLogLineScript(rendered))
	}
}

// jsStringEscape escapes a string for safe embedding in a JS single-quoted string literal.
func jsStringEscape(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`, "\n", `\n`, "\r", `\r`)
	return r.Replace(s)
}

// appendLogLineScript returns a JS snippet that appends a log line to #log-entries.
func appendLogLineScript(html string) string {
	return "(function(){var el=document.getElementById('log-entries');" +
		"if(!el)return;" +
		"var ph=document.getElementById('log-placeholder');if(ph)ph.remove();" +
		"el.insertAdjacentHTML('beforeend','" + jsStringEscape(html) + "');" +
		"el.scrollTop=el.scrollHeight})()"
}

// appendSysLogLineScript returns a JS snippet that appends a system log line
// to #syslog-entries. Caps at 500 DOM children to prevent unbounded growth.
func appendSysLogLineScript(html string) string {
	return "(function(){var el=document.getElementById('syslog-entries');" +
		"if(!el)return;" +
		"var ph=document.getElementById('syslog-placeholder');if(ph)ph.remove();" +
		"el.insertAdjacentHTML('beforeend','" + jsStringEscape(html) + "');" +
		"while(el.children.length>500)el.removeChild(el.firstChild);" +
		"el.scrollTop=el.scrollHeight})()"
}
