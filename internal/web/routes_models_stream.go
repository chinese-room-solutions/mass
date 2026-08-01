package web

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"sort"
	"strings"
	"sync"

	gatewaypb "github.com/chinese-room-solutions/mass-proto/gen/go/gateway"
	"github.com/chinese-room-solutions/mass-sdk/format"
	"github.com/chinese-room-solutions/mass/internal/downloads"
	"github.com/chinese-room-solutions/mass/internal/runtimes"
	"github.com/rs/zerolog"
)

// handleModelsStreamSSE feeds the Models tab a complete, fully-grouped view
// of every recognised model on disk.
//
// Pipeline per connection:
//
//  1. On open, MASS calls ListModels on each running gateway (the gateway
//     owns parsing + grouping). The responses are merged: groups with the
//     same (owner, model) across runtimes coalesce, and variants with the
//     same canonical filename across runtimes dedup so files in shared
//     format directories don't render twice.
//  2. The aggregated list is rendered as one HTML fragment and pushed to
//     the browser as a single Datastar `inner`-mode patch on #models-list.
//  3. Re-runs on runtime install / state change so the view stays current
//     without polling.
//
// Cache-warm ListModels is fast enough that one-shot grouped renders beat
// the cost of a per-row SSE multiplex.
func (h *Handler) handleModelsStreamSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	rdr := newModelsRenderer(h.runtimes, h.downloads, w, flusher, h.logger)

	// Re-render on runtime install / state changes. The unsubscribe funcs
	// run on connection close so subscribers don't accumulate.
	render := func() { rdr.render(ctx) }
	render()
	if h.runtimes != nil {
		stopInstall := h.runtimes.AddOnInstallChange(func(_ []string) { render() })
		stopState := h.runtimes.AddOnStateChange(func(_ string) { render() })
		defer stopInstall()
		defer stopState()
	}
	if h.downloads != nil {
		// A finished download adds a file to disk; re-render so it appears
		// in its group without the operator having to refresh.
		stopDl := h.subscribeDownloadsForRender(ctx, func() { render() })
		defer stopDl()
	}

	heartbeat := newHeartbeatTicker()
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			rdr.writeMu.Lock()
			_, err := fmt.Fprintf(w, ": ping\n\n")
			if err == nil {
				flusher.Flush()
			}
			rdr.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// modelsRenderer holds the per-connection state for one Models stream.
//
// We render the catalogue with per-group Datastar patches: each group's
// `<details>` carries a stable id and an outer-mode patch is emitted only
// when that group's HTML changes. Groups that disappear get a removal
// patch; new ones land via an append-mode patch on the parent container.
//
// Empty/loading states still go through one inner-mode patch on
// #models-list — they replace the entire scaffolding, which is what we
// want so the next non-empty render starts from a known-good shell.
type modelsRenderer struct {
	runtimes  *runtimes.Manager
	downloads *downloads.Manager
	w         http.ResponseWriter
	flusher   http.Flusher
	logger    zerolog.Logger
	writeMu   sync.Mutex
	// lastGroupHTML maps group_id → last-emitted HTML for that group.
	// Empty when the previous render was an empty/loading state, signalling
	// the next render needs to (re)install the scaffolding container.
	lastGroupHTML map[string]string
	// lastEmpty is the empty/loading body if the previous render was one;
	// "" otherwise. Used to skip identical empty-state re-renders.
	lastEmpty string
}

func newModelsRenderer(rm *runtimes.Manager, dl *downloads.Manager, w http.ResponseWriter, flusher http.Flusher, logger zerolog.Logger) *modelsRenderer {
	return &modelsRenderer{
		runtimes:  rm,
		downloads: dl,
		w:         w,
		flusher:   flusher,
		logger:    logger.With().Str("component", "models-renderer").Logger(),
	}
}

// render fans out ListModels to each running gateway, merges the results,
// and pushes the smallest set of Datastar patches that brings the browser
// in line. Returns silently on errors — the previous frame stays in the
// browser.
//
// When the running set is non-empty but ListModels could be slow
// (cold-cache first start), the renderer emits a loading placeholder
// before fetching so the operator sees a spinner instead of stale "no
// runtime" copy while the gateway parses.
func (r *modelsRenderer) render(ctx context.Context) {
	var running []*runtimes.LoadedGateway
	if r.runtimes != nil {
		running = r.runtimes.RunningGateways()
	}
	if len(running) > 0 && len(r.lastGroupHTML) == 0 {
		// First render after an empty state — show a spinner while we wait
		// on ListModels. The next emit will replace it with the real catalogue.
		r.emitEmpty(modelsLoadingHTML)
	}
	groups, pills, order := r.aggregate(ctx, running)
	if len(running) == 0 {
		r.emitEmpty(modelsEmptyNoRuntimeHTML)
		return
	}
	if len(order) == 0 {
		// Suppress the "no models" placeholder while files are arriving via
		// the downloads sibling container — otherwise the operator sees a
		// "no models" message stacked above their in-flight progress rows.
		if r.downloads != nil && len(r.downloads.List()) > 0 {
			r.emitEmpty(`<div class="space-y-1" id="models-list-inner"></div>`)
			return
		}
		r.emitEmpty(modelsEmptyNoModelsHTML)
		return
	}
	r.emitGroups(groups, pills, order)
}

// emitEmpty replaces #models-list with body and clears per-group memory
// so the next non-empty render reinstalls the scaffolding container.
func (r *modelsRenderer) emitEmpty(body string) {
	if body == r.lastEmpty && len(r.lastGroupHTML) == 0 {
		return
	}
	r.lastEmpty = body
	r.lastGroupHTML = nil
	r.writePatch("#models-list", "inner", body)
}

// emitGroups diffs the new catalogue against the per-connection memory
// and emits the minimum set of Datastar patches to reconcile the browser.
//
//   - Previous render was an empty state → install scaffolding container
//     once via inner-mode patch on #models-list, then emit one outer-mode
//     patch per group.
//   - Otherwise → for each group whose HTML changed, emit an outer-mode
//     patch keyed by #group-card-<dom-id>; for groups that disappeared,
//     emit a remove patch. New groups land via append-mode on
//     #models-list-inner so the in-place container reuses the existing
//     siblings.
func (r *modelsRenderer) emitGroups(groups map[string]*gatewaypb.Group, pills map[string][]string, order []string) {
	freshScaffold := len(r.lastGroupHTML) == 0
	if freshScaffold {
		r.writePatch("#models-list", "inner", `<div class="space-y-1" id="models-list-inner"></div>`)
		r.lastEmpty = ""
	}
	newHTML := make(map[string]string, len(order))
	for _, id := range order {
		newHTML[id] = renderGroupHTML(groups[id], pills[id])
	}
	// Removals: anything in lastGroupHTML missing from new set.
	for id := range r.lastGroupHTML {
		if _, ok := newHTML[id]; ok {
			continue
		}
		r.writePatch("#group-card-"+groupDOMID(id), "remove", "")
	}
	// Additions / updates in display-order so freshly-arriving groups land
	// in the right slot. Append-mode for new ones, outer-mode for changed.
	for _, id := range order {
		html := newHTML[id]
		prev, existed := r.lastGroupHTML[id]
		switch {
		case freshScaffold || !existed:
			r.writePatch("#models-list-inner", "append", html)
		case prev != html:
			r.writePatch("#group-card-"+groupDOMID(id), "outer", html)
		}
	}
	r.lastGroupHTML = newHTML
}

// writePatch writes one datastar-patch-elements SSE frame. Mode "remove"
// uses the no-elements form; everything else carries an elements payload.
func (r *modelsRenderer) writePatch(selector, mode, body string) {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	var err error
	if mode == "remove" {
		_, err = fmt.Fprintf(r.w,
			"event: datastar-patch-elements\ndata: selector %s\ndata: mode remove\n\n",
			selector)
	} else {
		_, err = fmt.Fprintf(r.w,
			"event: datastar-patch-elements\ndata: selector %s\ndata: mode %s\ndata: elements %s\n\n",
			selector, mode, body)
	}
	if err == nil {
		r.flusher.Flush()
	}
}

// aggregate fans out ListGroups to each running gateway and merges the
// per-runtime catalogues into one (groups, runtime-pills, display-order)
// tuple keyed by group id.
func (r *modelsRenderer) aggregate(ctx context.Context, running []*runtimes.LoadedGateway) (
	groups map[string]*gatewaypb.Group,
	pills map[string][]string,
	order []string,
) {
	type acc struct {
		group    *gatewaypb.Group
		runtimes []string
		modelID  map[string]bool
	}
	accs := map[string]*acc{}
	for _, gw := range running {
		gws, err := gw.ListGroups(ctx)
		if err != nil {
			r.logger.Warn().Err(err).Str("runtime", gw.RuntimeName()).Msg("listing model groups from gateway")
			continue
		}
		for _, g := range gws {
			key := g.GetId()
			a, ok := accs[key]
			if !ok {
				a = &acc{group: g, modelID: map[string]bool{}}
				accs[key] = a
				order = append(order, key)
				for _, m := range g.GetModels() {
					a.modelID[m.GetId()] = true
				}
				a.runtimes = append(a.runtimes, gw.RuntimeName())
				continue
			}
			a.runtimes = appendUnique(a.runtimes, gw.RuntimeName())
			// Coalesce capabilities + dedup child models by id (same
			// canonical filename across runtimes is the same file on disk).
			if a.group.GetCapabilities() == nil {
				a.group.Capabilities = &gatewaypb.Capabilities{}
			}
			if g.GetCapabilities().GetVision() {
				a.group.Capabilities.Vision = true
			}
			if g.GetCapabilities().GetAudio() {
				a.group.Capabilities.Audio = true
			}
			if g.GetCapabilities().GetThinking() {
				a.group.Capabilities.Thinking = true
			}
			for _, m := range g.GetModels() {
				if a.modelID[m.GetId()] {
					continue
				}
				a.modelID[m.GetId()] = true
				a.group.Models = append(a.group.Models, m)
			}
		}
	}
	sort.Slice(order, func(i, j int) bool {
		return strings.ToLower(accs[order[i]].group.GetDisplayName()) <
			strings.ToLower(accs[order[j]].group.GetDisplayName())
	})
	groups = make(map[string]*gatewaypb.Group, len(accs))
	pills = make(map[string][]string, len(accs))
	for id, a := range accs {
		groups[id] = a.group
		pills[id] = a.runtimes
	}
	return groups, pills, order
}

// renderGroupHTML returns the HTML for one group — used by per-group
// outer-mode patches and append-mode patches alike.
func renderGroupHTML(g *gatewaypb.Group, runtimePills []string) string {
	var b strings.Builder
	writeModelGroup(&b, g, runtimePills)
	return strings.ReplaceAll(b.String(), "\n", "")
}

// subscribeDownloadsForRender invokes onChange whenever a download starts,
// completes, or terminates. Starts trigger the re-render so the
// "no models" placeholder gives way to a clean container before the JS
// injects its progress rows; completes and terminations refresh the list
// once a file has landed (or to clear the suppressed-empty state).
// Returns a stop function the caller defers.
func (h *Handler) subscribeDownloadsForRender(ctx context.Context, onChange func()) func() {
	ch := h.downloads.Subscribe()
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				h.downloads.Unsubscribe(ch)
				return
			case <-stop:
				h.downloads.Unsubscribe(ch)
				return
			case evt, ok := <-ch:
				if !ok {
					return
				}
				switch evt.Status {
				case "started", "done", "cancelled", "error":
					onChange()
				}
			}
		}
	}()
	return func() { close(stop) }
}

func appendUnique(slice []string, val string) []string {
	for _, s := range slice {
		if s == val {
			return slice
		}
	}
	return append(slice, val)
}

const modelsEmptyNoRuntimeHTML = `<div class="flex flex-col items-center justify-center py-16 text-center">` +
	`<sl-icon name="box-seam" style="font-size:2rem;color:var(--sl-color-warning-500)" class="mb-3"></sl-icon>` +
	`<p class="text-sm" style="color:var(--mass-text-muted)">No runtime gateway is running.</p>` +
	`<p class="text-xs mt-1" style="color:var(--mass-text-faint)">Start one in the <span class="font-medium">Runtimes</span> tab to see installed models.</p>` +
	`</div>`

const modelsEmptyNoModelsHTML = `<div class="flex flex-col items-center justify-center py-16 text-center">` +
	`<sl-icon name="box-seam" style="font-size:2rem;color:var(--sl-color-neutral-500)" class="mb-3"></sl-icon>` +
	`<p class="text-sm" style="color:var(--mass-text-muted)">No models installed.</p>` +
	`<p class="text-xs mt-1" style="color:var(--mass-text-faint)">Use Install Model or Browse Local below.</p>` +
	`</div>`

// modelsLoadingHTML is the spinner placeholder shown while a running
// gateway is parsing model files (cold-cache first start). It replaces
// the previous "no runtime" or final-render frame so the operator sees
// progress instead of stale copy.
const modelsLoadingHTML = `<div class="flex flex-col items-center justify-center py-16 text-center">` +
	`<sl-spinner style="font-size:1.25rem;--track-width:2px"></sl-spinner>` +
	`</div>`

// writeModelGroup renders one `<details>` card for one Group on the
// Models tab. runtimePills lists every runtime that recognised at
// least one model in the group.
func writeModelGroup(b *strings.Builder, g *gatewaypb.Group, runtimePills []string) {
	esc := html.EscapeString
	filterParts := []string{g.GetDisplayName()}
	for _, mt := range g.GetModelTypes() {
		if label := modelTypeLabel(mt); label != "" {
			filterParts = append(filterParts, label)
		}
	}
	for _, rt := range runtimePills {
		filterParts = append(filterParts, rt, prettyRuntimeName(rt))
	}
	filterText := strings.ToLower(strings.Join(filterParts, " "))
	fmt.Fprintf(b, `<details id="group-card-%s" class="group-card bg-neutral-800/60 rounded-lg border border-neutral-700/50 overflow-hidden" data-filter-text="%s">`,
		groupDOMID(g.GetId()), esc(filterText))
	b.WriteString(`<summary class="flex items-center gap-3 w-full px-4 py-3 cursor-pointer select-none hover:bg-neutral-700/40 list-none [&::-webkit-details-marker]:hidden">`)
	b.WriteString(`<sl-icon name="chevron-right" class="text-neutral-400" style="font-size:0.75rem;transition:transform 0.2s"></sl-icon>`)
	primaryRuntime := ""
	if len(runtimePills) > 0 {
		primaryRuntime = runtimePills[0]
	}
	// Group name renders inline-editable: double-click swaps the
	// span for an input via window.__massBeginRenameGroup. Enter
	// commits via /api/groups/rename; Escape cancels. Single click
	// on the name doesn't toggle the <details> — the operator gets
	// to the rows by clicking the chevron or empty space, not the
	// editable label.
	fmt.Fprintf(b,
		`<span class="group-name text-sm font-medium text-white" data-group-id="%s" data-group-runtime="%s" data-group-name="%s" onclick="event.preventDefault(); event.stopPropagation();" ondblclick="event.preventDefault(); event.stopPropagation(); window.__massBeginRenameGroup(this);">%s</span>`,
		esc(g.GetId()), esc(primaryRuntime), esc(g.GetDisplayName()), esc(g.GetDisplayName()))
	for _, rt := range runtimePills {
		fmt.Fprintf(b, `<sl-tooltip content="Runtime"><span class="mass-badge font-mono text-[10px] rounded px-1.5 py-0.5" style="white-space:nowrap">%s</span></sl-tooltip>`,
			esc(prettyRuntimeName(rt)))
	}
	for _, mt := range g.GetModelTypes() {
		if label := modelTypeLabel(mt); label != "" {
			fmt.Fprintf(b, `<span class="mass-badge-alt font-mono text-xs font-bold rounded px-1.5 py-0.5">%s</span>`,
				esc(label))
		}
	}
	writeCapabilityIcons(b, g.GetCapabilities(), capIconStyleGroup)
	n := len(g.GetModels())
	pluralS := ""
	if n != 1 {
		pluralS = "s"
	}
	fmt.Fprintf(b, `<span class="text-xs text-neutral-400 ml-auto">%d model%s</span>`, n, pluralS)
	// Group-level delete: bulk-removes every model file in the group
	// via the same per-model runtime DELETE endpoint, looped client-side.
	groupPayload := groupDeletePayloadJSON(g, primaryRuntime)
	// Datastar reads the data-on:click attribute as JS source. The payload
	// is JSON (contains double-quotes), so it goes inside JS single-quotes
	// AND gets HTML-attribute-escaped so its quotes don't terminate the
	// attribute value early.
	groupClick := fmt.Sprintf(
		`$confirmDeleteGroupPayload='%s'; $confirmDeleteGroupLabel='%s'; $confirmDeleteGroupCount=%d; $confirmDeleteGroupOpen=true`,
		format.JSEscape(groupPayload), format.JSEscape(g.GetDisplayName()), n)
	fmt.Fprintf(b,
		`<div onclick="event.stopPropagation()"><sl-tooltip content="Delete entire group"><sl-icon-button name="trash3" style="font-size:0.85rem;color:var(--sl-color-danger-400)" data-on:click="%s"></sl-icon-button></sl-tooltip></div>`,
		html.EscapeString(groupClick))
	b.WriteString(`</summary>`)
	b.WriteString(`<div class="space-y-px border-t border-neutral-700/50 px-2 py-1 overflow-y-auto" style="max-height:50vh">`)
	for _, m := range g.GetModels() {
		writeModelRow(b, m, primaryRuntime)
	}
	b.WriteString(`</div></details>`)
}

// groupDeletePayloadJSON serialises a group's child models into the
// JSON `[{kind, id}, ...]` list the group-delete handler consumes.
// Children inherit the group's primary runtime — that's the same
// runtime the per-row trash icon uses.
func groupDeletePayloadJSON(g *gatewaypb.Group, runtime string) string {
	type entry struct {
		Kind string `json:"kind"`
		ID   string `json:"id"`
	}
	out := make([]entry, 0, len(g.GetModels()))
	for _, m := range g.GetModels() {
		out = append(out, entry{Kind: runtime, ID: m.GetId()})
	}
	buf, err := json.Marshal(out)
	if err != nil {
		return "[]"
	}
	return string(buf)
}

// writeModelRow renders one model file row inside a group card.
// primaryRuntime is set as $selectedModelRuntime on click so the
// Detail panel knows which gateway to query for the model's full
// info. Per-row capability icons reflect the file's own caps —
// dimmer than the group header's union.
func writeModelRow(b *strings.Builder, m *gatewaypb.Model, primaryRuntime string) {
	esc := html.EscapeString
	id := m.GetId()
	idJS := format.JSEscape(id)
	displayName := m.GetDisplayName()
	if displayName == "" {
		displayName = id
	}
	rtJS := format.JSEscape(primaryRuntime)

	fmt.Fprintf(b, `<div class="model-row flex items-center gap-2 px-3 py-2 hover:bg-neutral-700/40 rounded cursor-pointer" data-row-id="%s" data-attr:class="$selectedModelID==='%s' ? 'model-row flex items-center gap-2 px-3 py-2 hover:bg-neutral-700/40 rounded cursor-pointer border border-blue-500/60' : 'model-row flex items-center gap-2 px-3 py-2 hover:bg-neutral-700/40 rounded cursor-pointer'" data-on:click="$selectedModelID='%s'; $selectedModelRuntime='%s'">`,
		esc(id), idJS, idJS, rtJS)
	if badge := m.GetBadgeText(); badge != "" {
		fmt.Fprintf(b, `<span class="mass-badge-alt font-mono text-xs font-bold rounded px-1.5 py-0.5" style="min-width:4.5rem;text-align:center">%s</span>`,
			esc(badge))
	} else {
		b.WriteString(`<span style="min-width:4.5rem;flex-shrink:0"></span>`)
	}
	fmt.Fprintf(b, `<span class="text-xs text-neutral-200 truncate flex-1" title="%s">%s</span>`,
		esc(displayName), esc(displayName))
	writeCapabilityIcons(b, m.GetCapabilities(), capIconStyleRow)
	fmt.Fprintf(b, `<span class="text-xs text-neutral-400 flex-shrink-0" style="width:5rem;text-align:right">%s</span>`,
		format.Bytes(m.GetSizeBytes()))
	fmt.Fprintf(b, `<div onclick="event.stopPropagation()"><sl-tooltip content="Delete model"><sl-icon-button name="trash3" style="font-size:0.85rem;color:var(--sl-color-danger-400)" data-on:click="$confirmDeleteModelID='%s'; $confirmDeleteModelKind='%s'; $confirmDeleteModelOpen=true"></sl-icon-button></sl-tooltip></div></div>`,
		idJS, rtJS)
}

// modelTypeLabel returns the display string MASS shows for one
// ModelTypeEntry. Gateway-supplied label wins when set; otherwise we
// fall back to a canonical title-cased name for the typed kind.
// Returns empty string for unspecified entries (MASS skips them).
func modelTypeLabel(mt *gatewaypb.ModelTypeEntry) string {
	if mt == nil {
		return ""
	}
	if l := mt.GetLabel(); l != "" {
		return l
	}
	switch mt.GetKind() {
	case gatewaypb.ModelTypeKind_MODEL_TYPE_CHAT:
		return "Chat"
	case gatewaypb.ModelTypeKind_MODEL_TYPE_EMBEDDING:
		return "Embedding"
	case gatewaypb.ModelTypeKind_MODEL_TYPE_RERANK:
		return "Rerank"
	case gatewaypb.ModelTypeKind_MODEL_TYPE_IMAGE_GEN:
		return "Image Gen"
	case gatewaypb.ModelTypeKind_MODEL_TYPE_SPEECH_TO_TEXT:
		return "Speech-to-Text"
	case gatewaypb.ModelTypeKind_MODEL_TYPE_TEXT_TO_SPEECH:
		return "Text-to-Speech"
	default:
		return ""
	}
}

func prettyRuntimeName(name string) string {
	if name == "llama-cpp" {
		return "llama.cpp"
	}
	return name
}

// capIconStyle picks the visual weight for capability icons. Group
// headers carry the authoritative summary (bolder); per-row icons
// reflect the file's own caps (dimmer + smaller) so they don't
// compete with the group's badges.
type capIconStyle int

const (
	capIconStyleGroup capIconStyle = iota
	capIconStyleRow
)

// writeCapabilityIcons emits the lightbulb / eye / mic tooltipped
// icons for whichever capabilities are set. Style controls the
// visual weight (group header vs. per-row).
func writeCapabilityIcons(b *strings.Builder, caps *gatewaypb.Capabilities, style capIconStyle) {
	if caps == nil {
		return
	}
	var sizeStyle string
	switch style {
	case capIconStyleRow:
		sizeStyle = "font-size:0.8rem"
	default:
		sizeStyle = "font-size:0.85rem"
	}
	if caps.GetThinking() {
		fmt.Fprintf(b, `<sl-tooltip content="Thinking / reasoning capable"><sl-icon name="lightbulb" style="%s;color:var(--sl-color-warning-400)"></sl-icon></sl-tooltip>`, sizeStyle)
	}
	if caps.GetVision() {
		fmt.Fprintf(b, `<sl-tooltip content="Vision capable"><sl-icon name="eye" style="%s;color:var(--sl-color-primary-400)"></sl-icon></sl-tooltip>`, sizeStyle)
	}
	if caps.GetAudio() {
		fmt.Fprintf(b, `<sl-tooltip content="Audio capable"><sl-icon name="mic" style="%s;color:var(--sl-color-primary-400)"></sl-icon></sl-tooltip>`, sizeStyle)
	}
}

// groupDOMID turns an opaque gateway-supplied group id into a short
// DOM-safe token for use as an HTML id attribute. Stable across renders,
// collision-resistant, and free of characters that would break CSS
// selectors. SHA-1 truncated to 12 hex chars — not cryptographic.
func groupDOMID(s string) string {
	sum := sha1.Sum([]byte(s))
	return hex.EncodeToString(sum[:6])
}
