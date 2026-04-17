package web

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	sdkhf "github.com/chinese-room-solutions/mass-sdk/huggingface"
	"github.com/chinese-room-solutions/mass-sdk/uikit"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/huggingface"
	"github.com/chinese-room-solutions/mass/internal/model"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/web/templates"
	"github.com/starfederation/datastar-go/datastar"
)

const hfPageSize = 5

// hfSearchState holds server-side pagination state for a HF search.
type hfSearchState struct {
	query      string
	buffer     []uikit.HFResultModel // fetched but not yet shown
	shownIDs   []string              // all IDs shown (for ExcludeIDs)
	nextCursor string                // opaque cursor from HF Link header
	hasMore    bool
}

// modelsDir returns the effective centralized models directory.
func (h *Handler) modelsDir() string {
	dataDir, err := h.cfg.EffectiveDataDir()
	if err != nil {
		return ""
	}
	return config.ModelsDir(dataDir)
}

// handleListModels scans the centralized models directory and renders the local models list.
// Supports an optional ?filter= query parameter for searching.
func (h *Handler) handleListModels(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	dir := h.modelsDir()
	if dir == "" {
		mustSSE(sse.PatchElements(
			`<div id="models-content"><p class="text-neutral-500 text-sm">Data directory not configured.</p></div>`,
			datastar.WithSelector("#models-content"),
			datastar.WithMode(datastar.ElementPatchModeOuter),
		))
		return
	}

	// Ensure the models directory exists.
	if err := os.MkdirAll(dir, 0755); err != nil {
		h.logger.Warn().Err(err).Str("dir", dir).Msg("failed to create models directory")
	}

	models, err := ScanModels(dir)
	if err != nil {
		mustSSE(sse.PatchElements(
			templates.RenderError("Failed to scan models: "+err.Error()),
			datastar.WithSelector("#models-content"),
			datastar.WithMode(datastar.ElementPatchModeInner),
		))
		return
	}

	// Apply filter if provided.
	filter := strings.TrimSpace(r.URL.Query().Get("filter"))
	if filter != "" {
		terms := strings.Fields(strings.ToLower(filter))
		var filtered []LocalModelInfo
		for _, m := range models {
			haystack := strings.ToLower(m.Filename + " " + m.RelPath + " " + string(m.ModelType))
			match := true
			for _, t := range terms {
				if !strings.Contains(haystack, t) {
					match = false
					break
				}
			}
			if match {
				filtered = append(filtered, m)
			}
		}
		models = filtered
	}

	groups := GroupModels(models)
	viewGroups := make([]templates.ModelGroupView, len(groups))
	for i, g := range groups {
		variants := make([]templates.ModelVariantView, len(g.Variants))
		for j, v := range g.Variants {
			// Display as "publisher/filename" when under a subdirectory.
			displayName := v.Filename
			if parts := strings.SplitN(v.SubDir, "/", 2); len(parts) >= 1 && parts[0] != "" {
				displayName = parts[0] + "/" + v.Filename
			}
			variants[j] = templates.ModelVariantView{
				Filename:      v.Filename,
				DisplayName:   displayName,
				Path:          v.Path,
				Quantization:  v.Quantization,
				SizeFormatted: uikit.FormatBytes(v.SizeBytes),
				IsMmproj:      v.ModelType == ModelFileKindMmproj,
			}
		}
		hasVision := false
		hasThinking := false
		for _, v := range g.Variants {
			if v.HasVision {
				hasVision = true
			}
			if v.HasThinking {
				hasThinking = true
			}
		}
		viewGroups[i] = templates.ModelGroupView{
			BaseName:    g.BaseName,
			ModelType:   g.ModelType.Title(),
			HasVision:   hasVision,
			HasThinking: hasThinking,
			Variants:    variants,
		}
	}

	// Render templ component to HTML string.
	var buf bytes.Buffer
	if err := templates.ModelsLocalList(viewGroups).Render(r.Context(), &buf); err != nil {
		h.logger.Error().Err(err).Msg("failed to render models list template")
		return
	}

	if err := sse.PatchElements(buf.String(),
		datastar.WithSelector("#models-content"),
		datastar.WithMode(datastar.ElementPatchModeOuter),
	); err != nil {
		return
	}

	// Replay active/paused downloads so they appear as downloading rows
	// in the freshly rendered model list.
	h.replayDownloads(sse)
}

// handleModelInfo reads GGUF metadata from a model file and renders the properties panel.
func (h *Handler) handleModelInfo(w http.ResponseWriter, r *http.Request) {
	modelPath := r.URL.Query().Get("path")
	sse := datastar.NewSSE(w, r)

	if modelPath == "" {
		mustSSE(sse.PatchElements(`<div id="models-props-content"><p class="text-neutral-500 text-sm">No model selected.</p></div>`,
			datastar.WithSelector("#models-props-content"),
			datastar.WithMode(datastar.ElementPatchModeOuter),
		))
		return
	}

	// Security: ensure the path is inside the models directory.
	dir := h.modelsDir()
	absPath, ok := isPathUnder(modelPath, dir)
	if !ok {
		mustSSE(sse.PatchElements(`<div id="models-props-content"><p class="text-red-400 text-sm">Invalid model path.</p></div>`,
			datastar.WithSelector("#models-props-content"),
			datastar.WithMode(datastar.ElementPatchModeOuter),
		))
		return
	}

	info, err := ReadGGUFModelInfo(absPath)
	if err != nil {
		h.logger.Error().Err(err).Str("path", absPath).Msg("failed to read GGUF metadata")
		mustSSE(sse.PatchElements(fmt.Sprintf(`<div id="models-props-content"><p class="text-red-400 text-sm">Failed to read model metadata: %s</p></div>`,
			strings.ReplaceAll(err.Error(), `"`, `&quot;`)),
			datastar.WithSelector("#models-props-content"),
			datastar.WithMode(datastar.ElementPatchModeOuter),
		))
		return
	}

	// Derive repo ID from the directory structure (publisher/repo).
	repoID := ""
	if rel, err := filepath.Rel(dir, absPath); err == nil {
		rel = filepath.ToSlash(rel)
		// RelPath is "publisher/repo/file.gguf" — repo ID is the first two segments.
		parts := strings.SplitN(rel, "/", 3)
		if len(parts) >= 3 {
			repoID = parts[0] + "/" + parts[1]
		}
	}

	// Infer model type from filename/path.
	relPath := ""
	if rel, relErr := filepath.Rel(dir, absPath); relErr == nil {
		relPath = rel
	}
	modelType := string(inferModelType(info.Filename, relPath))

	// Detect vision projector once.
	var mmprojPath, mmprojRel string
	if modelType != "mmproj" {
		mmprojPath = config.DetectMmproj(info.Path)
		if mmprojPath != "" {
			if r, err := filepath.Rel(dir, mmprojPath); err == nil {
				mmprojRel = filepath.ToSlash(r)
			} else {
				mmprojRel = filepath.Base(mmprojPath)
			}
		}
	}

	// Build display-ready view.
	view := templates.ModelPropsView{
		Name:         info.Name,
		RepoID:       repoID,
		Filename:     info.Filename,
		Path:         info.Path,
		ModelType:    modelType,
		Architecture: info.Architecture,
		QuantType:    info.QuantType,
		FileSize:     info.FileSizeStr,
		IsMmproj:     modelType == "mmproj",
		HasVision:    mmprojPath != "",
		HasThinking:  detectThinkingSupport(info.ChatTemplate),
		MmprojPath:   mmprojPath,
		MmprojRel:    mmprojRel,
	}
	if view.Name == "" {
		view.Name = info.Filename
	}
	if info.ContextLength > 0 {
		view.ContextLength = formatUint(info.ContextLength)
	}
	if info.EmbeddingLength > 0 {
		view.EmbeddingLength = formatUint(info.EmbeddingLength)
	}
	if info.BlockCount > 0 {
		view.BlockCount = formatUint(info.BlockCount)
	}
	if info.HeadCount > 0 {
		view.HeadCount = formatUint(info.HeadCount)
	}
	if info.HeadCountKV > 0 {
		view.HeadCountKV = formatUint(info.HeadCountKV)
	}
	if info.VocabSize > 0 {
		view.VocabSize = formatUint(uint64(info.VocabSize))
	}
	view.TokenizerModel = info.TokenizerModel
	view.ChatTemplate = detectChatTemplateName(info.ChatTemplate)

	var buf bytes.Buffer
	if err := templates.ModelPropsPanel(view).Render(r.Context(), &buf); err != nil {
		h.logger.Error().Err(err).Msg("failed to render model props template")
		return
	}
	mustSSE(sse.PatchElements(buf.String(),
		datastar.WithSelector("#models-props-content"),
		datastar.WithMode(datastar.ElementPatchModeOuter),
	))
}

// detectChatTemplateName identifies a chat template name from the raw Jinja
// template string stored in GGUF metadata. Returns a human-readable name
// (e.g. "chatml", "vicuna", "mistral") or empty string if no template.
func detectChatTemplateName(raw string) string {
	if raw == "" {
		return ""
	}
	// Match patterns used by llama.cpp's llama_chat_apply_template_internal.
	switch {
	case strings.Contains(raw, "<|im_start|>"):
		return "chatml"
	case strings.Contains(raw, "[INST]"):
		if strings.Contains(raw, "[AVAILABLE_TOOLS]") {
			return "mistral-v3-tekken"
		}
		if strings.Contains(raw, "<<SYS>>") {
			return "llama2"
		}
		return "mistral"
	case strings.Contains(raw, "USER:"):
		return "vicuna"
	case strings.Contains(raw, "<start_of_turn>"):
		return "gemma"
	case strings.Contains(raw, "<|start_header_id|>"):
		return "llama3"
	case strings.Contains(raw, "<|user|>"):
		return "zephyr"
	case strings.Contains(raw, "### Instruction:"):
		return "alpaca"
	case strings.Contains(raw, "<|START_OF_TURN_TOKEN|>"):
		return "command-r"
	case strings.Contains(raw, "<|role_start|>"):
		return "deepseek"
	default:
		return "custom"
	}
}

// formatUint formats a uint64 with comma separators.
func formatUint(n uint64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var result []byte
	for i, d := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(d))
	}
	return string(result)
}

// handleDeleteModel deletes a model file from the centralized models directory.
func (h *Handler) handleDeleteModel(w http.ResponseWriter, r *http.Request) {
	modelPath := r.URL.Query().Get("path")
	sse := datastar.NewSSE(w, r)

	if modelPath == "" {
		mustSSE(sse.PatchElements(templates.RenderError("Missing model path"),
			datastar.WithSelector("#models-content"),
			datastar.WithMode(datastar.ElementPatchModeInner),
		))
		return
	}

	// Resolve to an absolute path before delegating, so the rest of this
	// handler (which manipulates the DOM by absolute path) works.
	absPath, ok := isPathUnder(modelPath, h.modelsDir())
	if !ok {
		mustSSE(sse.PatchElements(templates.RenderError("Invalid model path"),
			datastar.WithSelector("#models-content"),
			datastar.WithMode(datastar.ElementPatchModeInner),
		))
		return
	}

	if err := h.deleteModelFile(absPath); err != nil {
		msg := "Failed to delete: " + err.Error()
		if errors.Is(err, errModelNotFound) {
			msg = "Model not found"
		} else if errors.Is(err, errInvalidModelPath) {
			msg = "Invalid model path"
		}
		mustSSE(sse.PatchElements(templates.RenderError(msg),
			datastar.WithSelector("#models-content"),
			datastar.WithMode(datastar.ElementPatchModeInner),
		))
		return
	}

	// Restore the "Get" button in HF search results so the file no longer shows as downloaded.
	filename := filepath.Base(absPath)
	jsFilename := jsStringEscape(filename)
	mustSSE(sse.ExecuteScript(fmt.Sprintf(
		`(function(){if(!window.__hfDlID)return;var id=window.__hfDlID('%s');`+
			`function restore(el){`+
			`var repo=el.getAttribute('data-repo'),file=el.getAttribute('data-file');`+
			`if(!repo||!file)return;`+
			`el.innerHTML='<sl-button size="small" variant="primary" onclick="window.__hfDownload(\''+repo+'\',\''+file+'\')">'`+
			`+'<sl-icon slot="prefix" name="download"></sl-icon>Get</sl-button>';`+
			`}`+
			`document.querySelectorAll('[id="'+id+'"]').forEach(restore);`+
			`document.querySelectorAll('template').forEach(function(t){`+
			`var e=t.content.querySelector('[id="'+id+'"]');if(e)restore(e);`+
			`});})()`, jsFilename)))

	// Remove the deleted variant from the DOM without re-rendering (preserves <details> open state).
	jsPath := jsStringEscape(absPath)
	mustSSE(sse.ExecuteScript(fmt.Sprintf(
		`(function(){`+
			`var p='%s';`+
			`var row=null;`+
			`document.querySelectorAll('[data-model-path]').forEach(function(el){`+
			`if(el.getAttribute('data-model-path')===p)row=el;`+
			`});`+
			`if(!row)return;`+
			`var group=row.closest('details.model-group');`+
			`row.remove();`+
			`if(group){`+
			`var remaining=group.querySelectorAll('[data-model-path]').length;`+
			`if(remaining===0){group.remove();}else{`+
			`var countEl=group.querySelector('summary .text-xs.text-neutral-400');`+
			`if(countEl)countEl.textContent=remaining+' variant'+(remaining===1?'':'s');`+
			`}}`+
			`var content=document.getElementById('models-content');`+
			`if(content&&content.querySelectorAll('details.model-group').length===0){`+
			`content.innerHTML='<p class=\"text-neutral-500 text-sm py-4\">No models found. Click \\\"Install New Model\\\" to download from Hugging Face.</p>';`+
			`}`+
			`})()`,
		jsPath)))
}

// handleSearchHF performs a HuggingFace search and renders results.
func (h *Handler) handleSearchHF(w http.ResponseWriter, r *http.Request) {
	var signals struct {
		ModelsHfQuery string `json:"modelsHfQuery"`
	}
	if err := decodeJSON(r, &signals); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	sse := datastar.NewSSE(w, r)

	query := strings.TrimSpace(signals.ModelsHfQuery)
	if query == "" {
		mustSSE(
			// Clear search results when query is empty.
			sse.PatchElements(
				`<div id="models-hf-results"></div>`,
				datastar.WithSelector("#models-hf-results"),
				datastar.WithMode(datastar.ElementPatchModeOuter),
			))
		return
	}

	result, err := sdkhf.Search(r.Context(), query, sdkhf.SearchOptions{
		FileExts: []string{".gguf"},
		Limit:    hfPageSize,
	})
	if err != nil {
		mustSSE(sse.PatchElements(
			uikit.RenderHFStatus("Search failed: "+err.Error(), true),
			datastar.WithSelector("#models-hf-results"),
			datastar.WithMode(datastar.ElementPatchModeInner),
		))
		return
	}

	uiModels := convertHFModels(result.Models)

	// Store server-side pagination state.
	shownIDs := make([]string, 0, len(result.Models))
	for _, m := range result.Models {
		shownIDs = append(shownIDs, m.RepoID)
	}
	h.hfMu.Lock()
	h.hfState = &hfSearchState{
		query:      query,
		shownIDs:   shownIDs,
		nextCursor: result.NextCursor,
		hasMore:    result.HasMore,
	}
	h.hfMu.Unlock()

	downloaded := downloadedFilesMap(h.modelsDir())

	// __mass__ is used as the app name for Show More URL routing.
	htmlResult := uikit.RenderHFResults("__mass__", uiModels, uikit.HFResultsOpts{
		HasMore:         result.HasMore,
		DownloadedFiles: downloaded,
		MoreURL:         "/api/v1/models/search/more",
	})
	mustSSE(sse.PatchElements(htmlResult,
		datastar.WithSelector("#models-hf-results"),
		datastar.WithMode(datastar.ElementPatchModeInner),
	))
}

// handleSearchHFMore continues pagination for HF search results.
func (h *Handler) handleSearchHFMore(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	h.hfMu.Lock()
	state := h.hfState
	h.hfMu.Unlock()

	if state == nil {
		mustSSE(sse.PatchElements(
			uikit.RenderHFStatus("No active search.", true),
			datastar.WithSelector("#models-hf-results"),
			datastar.WithMode(datastar.ElementPatchModeInner),
		))
		return
	}

	h.hfMu.Lock()
	// Fetch more from HF if buffer is low.
	if len(state.buffer) < hfPageSize && state.hasMore {
		// Release lock during network call, then re-acquire.
		query := state.query
		cursor := state.nextCursor
		excludeIDs := make([]string, len(state.shownIDs))
		copy(excludeIDs, state.shownIDs)
		h.hfMu.Unlock()

		result, err := sdkhf.Search(r.Context(), query, sdkhf.SearchOptions{
			FileExts:   []string{".gguf"},
			Limit:      hfPageSize,
			Cursor:     cursor,
			ExcludeIDs: excludeIDs,
		})
		if err != nil {
			mustSSE(sse.PatchElements(
				uikit.RenderHFStatus("Search failed: "+err.Error(), true),
				datastar.WithSelector("#models-hf-results"),
				datastar.WithMode(datastar.ElementPatchModeInner),
			))
			return
		}

		h.hfMu.Lock()
		state.buffer = append(state.buffer, convertHFModels(result.Models)...)
		state.nextCursor = result.NextCursor
		state.hasMore = result.HasMore
		for _, m := range result.Models {
			state.shownIDs = append(state.shownIDs, m.RepoID)
		}
	}

	if len(state.buffer) == 0 {
		h.hfMu.Unlock()
		mustSSE(sse.PatchElements(
			uikit.RenderHFFooter("__mass__", false, "/api/v1/models/search/more"),
		))
		return
	}

	// Take up to hfPageSize from buffer.
	take := hfPageSize
	if take > len(state.buffer) {
		take = len(state.buffer)
	}
	page := make([]uikit.HFResultModel, take)
	copy(page, state.buffer[:take])
	state.buffer = state.buffer[take:]
	hasMore := len(state.buffer) > 0 || state.hasMore
	h.hfMu.Unlock()

	downloaded := downloadedFilesMap(h.modelsDir())
	rowsHTML := uikit.RenderHFResultRows(page, downloaded)
	footerHTML := uikit.RenderHFFooter("__mass__", hasMore, "/api/v1/models/search/more")
	mustSSE(sse.PatchElements(rowsHTML,
		datastar.WithSelector("#pe-hf-list"),
		datastar.WithMode(datastar.ElementPatchModeAppend),
	))
	mustSSE(sse.PatchElements(footerHTML))
}

// handleDownloadModel starts downloading a model file from HuggingFace (POST /api/models/download).
func (h *Handler) handleDownloadModel(w http.ResponseWriter, r *http.Request) {
	var signals struct {
		DlRepoID   string `json:"dlRepoID"`
		DlFilename string `json:"dlFilename"`
	}
	if err := decodeJSON(r, &signals); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if signals.DlRepoID == "" || signals.DlFilename == "" {
		sse := datastar.NewSSE(w, r)
		mustSSE(sse.PatchElements(templates.RenderError("Missing download parameters")))
		return
	}

	if err := h.startModelDownload(signals.DlRepoID, signals.DlFilename); err != nil {
		sse := datastar.NewSSE(w, r)
		mustSSE(sse.PatchElements(templates.RenderError(err.Error())))
		return
	}

	w.WriteHeader(http.StatusOK)
}

// startModelDownload launches an async download goroutine for the given HF model file.
// Progress is broadcast via SSE events. Supports pause/resume/cancel via the download state map.
func (h *Handler) startModelDownload(repoID, filename string) error {
	destDir := h.modelsDir()
	if destDir == "" {
		return fmt.Errorf("data directory not configured")
	}

	// Check for existing active download.
	if ds := h.getDownload(filename); ds != nil {
		return fmt.Errorf("download already in progress for %s", filename)
	}

	// Compute the group display name using the same logic as local model grouping.
	baseName := stripQuant(filename)
	groupName := uikit.FormatModelName(sdkhf.SanitizeRepoID(repoID) + "/" + baseName)

	ctx, cancel := context.WithCancel(context.Background())
	ds := h.addDownload(repoID, filename, cancel)
	ds.GroupName = groupName

	// Persist to DB so the download survives a restart.
	if h.store != nil {
		if err := h.store.UpsertDownload(model.Download{
			Filename:  filename,
			RepoID:    repoID,
			GroupName: groupName,
			Status:    "active",
		}); err != nil {
			h.logger.Warn().Err(err).Str("file", filename).Msg("failed to persist download state")
		}
	}

	h.logger.Info().
		Str("repo", repoID).
		Str("file", filename).
		Str("dest", destDir).
		Msg("starting model download")

	// Broadcast start event so the UI inserts a downloading row.
	h.broker.Broadcast(SSEEvent{
		Type:        EventTypeDownload,
		AppName:     "__mass__",
		DlFilename:  filename,
		DlRepoID:    repoID,
		DlGroupName: groupName,
		DlStart:     true,
	})

	go h.runDownload(ctx, ds, destDir)

	return nil
}

// runDownload executes the actual download. Separated so resume can call it too.
func (h *Handler) runDownload(ctx context.Context, ds *downloadState, destDir string) {
	var lastPct int64 = -1
	localPath, err := huggingface.Download(
		ctx,
		ds.RepoID, ds.Filename, destDir,
		func(downloaded, total int64) {
			now := time.Now()
			ds.mu.Lock()
			ds.Downloaded = downloaded
			ds.Total = total
			shouldPersist := h.store != nil && now.Sub(ds.lastPersist) >= 5*time.Second
			if shouldPersist {
				ds.lastPersist = now
			}
			ds.mu.Unlock()

			if shouldPersist {
				if err := h.store.UpdateProgress(ds.Filename, downloaded, total); err != nil {
					h.logger.Warn().Err(err).Str("file", ds.Filename).Msg("failed to persist download progress")
				}
			}

			if total <= 0 {
				return
			}
			pct := 100 * downloaded / total
			if pct != lastPct {
				lastPct = pct
				h.broker.Broadcast(SSEEvent{
					Type:         EventTypeDownload,
					AppName:      "__mass__",
					DlFilename:   ds.Filename,
					DlDownloaded: downloaded,
					DlTotal:      total,
				})
			}
		},
	)
	if err != nil {
		if ctx.Err() != nil {
			// Context was cancelled (pause or cancel). Handlers already dealt with SSE.
			return
		}
		h.logger.Error().Err(err).Str("file", ds.Filename).Msg("model download failed")
		h.removeDownload(ds.Filename)
		if h.store != nil {
			if delErr := h.store.DeleteDownload(ds.Filename); delErr != nil {
				h.logger.Warn().Err(delErr).Str("file", ds.Filename).Msg("failed to delete download record")
			}
		}
		h.broker.Broadcast(SSEEvent{
			Type:       EventTypeDownload,
			AppName:    "__mass__",
			DlFilename: ds.Filename,
			Error:      err,
		})
		return
	}
	h.logger.Info().Str("file", ds.Filename).Str("path", localPath).Msg("model download complete")

	h.removeDownload(ds.Filename)
	if h.store != nil {
		if delErr := h.store.DeleteDownload(ds.Filename); delErr != nil {
			h.logger.Warn().Err(delErr).Str("file", ds.Filename).Msg("failed to delete download record")
		}
	}
	h.broker.Broadcast(SSEEvent{
		Type:       EventTypeDownload,
		AppName:    "__mass__",
		DlFilename: ds.Filename,
		DlDone:     true,
		DlPath:     localPath,
	})
}

// errDownloadNotFound is returned when no active download exists for the
// given filename. errDownloadNotPaused is returned by resumeDownload when
// the download is currently active.
var (
	errDownloadNotFound  = errors.New("no active download")
	errDownloadNotPaused = errors.New("download is not paused")
)

// pauseDownload halts the current download by cancelling its context;
// the partial temp file is preserved on disk for later resume.
func (h *Handler) pauseDownload(filename string) error {
	ds := h.getDownload(filename)
	if ds == nil {
		return errDownloadNotFound
	}
	ds.mu.Lock()
	ds.cancelFn()
	ds.Paused = true
	downloaded := ds.Downloaded
	total := ds.Total
	ds.mu.Unlock()

	if h.store != nil {
		if err := h.store.SetStatus(filename, "paused"); err != nil {
			h.logger.Warn().Err(err).Str("file", filename).Msg("failed to set download status")
		}
		if err := h.store.UpdateProgress(filename, downloaded, total); err != nil {
			h.logger.Warn().Err(err).Str("file", filename).Msg("failed to update download progress")
		}
	}
	h.broker.Broadcast(SSEEvent{
		Type:         EventTypeDownload,
		AppName:      "__mass__",
		DlFilename:   filename,
		DlPaused:     true,
		DlDownloaded: downloaded,
		DlTotal:      total,
	})
	return nil
}

// resumeDownload restarts a paused download from its partial temp file.
func (h *Handler) resumeDownload(filename string) error {
	ds := h.getDownload(filename)
	if ds == nil {
		return errDownloadNotFound
	}
	ds.mu.Lock()
	if !ds.Paused {
		ds.mu.Unlock()
		return errDownloadNotPaused
	}
	ctx, cancel := context.WithCancel(context.Background())
	ds.cancelFn = cancel
	ds.Paused = false
	ds.mu.Unlock()

	if h.store != nil {
		if err := h.store.SetStatus(filename, "active"); err != nil {
			h.logger.Warn().Err(err).Str("file", filename).Msg("failed to set download status")
		}
	}
	destDir := h.modelsDir()
	go h.runDownload(ctx, ds, destDir)
	return nil
}

// cancelDownload aborts a download and removes its temp file.
func (h *Handler) cancelDownload(filename string) error {
	ds := h.getDownload(filename)
	if ds == nil {
		return errDownloadNotFound
	}
	ds.mu.Lock()
	ds.cancelFn()
	ds.mu.Unlock()

	h.removeDownload(filename)
	if h.store != nil {
		if err := h.store.DeleteDownload(filename); err != nil {
			h.logger.Warn().Err(err).Str("file", filename).Msg("failed to delete download record")
		}
	}
	destDir := h.modelsDir()
	if destDir != "" {
		if err := os.Remove(huggingface.TempFilePath(ds.RepoID, filename, destDir)); err != nil && !os.IsNotExist(err) {
			h.logger.Warn().Err(err).Str("file", filename).Msg("failed to remove temp download file")
		}
	}
	h.broker.Broadcast(SSEEvent{
		Type:        EventTypeDownload,
		AppName:     "__mass__",
		DlFilename:  filename,
		DlCancelled: true,
	})
	return nil
}

// handleDownloadPause is the UI wrapper for pauseDownload.
func (h *Handler) handleDownloadPause(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Filename string `json:"filename"`
	}
	if err := decodeJSON(r, &req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := h.pauseDownload(req.Filename); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errDownloadNotFound) {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleDownloadResume is the UI wrapper for resumeDownload.
func (h *Handler) handleDownloadResume(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Filename string `json:"filename"`
	}
	if err := decodeJSON(r, &req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := h.resumeDownload(req.Filename); err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, errDownloadNotFound):
			status = http.StatusNotFound
		case errors.Is(err, errDownloadNotPaused):
			status = http.StatusBadRequest
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleDownloadCancel is the UI wrapper for cancelDownload.
func (h *Handler) handleDownloadCancel(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Filename string `json:"filename"`
	}
	if err := decodeJSON(r, &req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := h.cancelDownload(req.Filename); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errDownloadNotFound) {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// errAlreadyExists is returned by importModelFile when a destination
// model with the same filename already exists.
var errAlreadyExists = errors.New("model already exists")

// importModelFile validates the source path, registers an in-progress
// download record (so the UI's progress channel can mirror the copy), and
// kicks off the asynchronous file copy. Returns the resulting model ID
// (e.g. "local/<filename>") immediately; progress events flow through the
// /internal/events SSE stream.
func (h *Handler) importModelFile(srcPath string) (string, error) {
	destDir := h.modelsDir()
	if destDir == "" {
		return "", errors.New("data directory not configured")
	}
	srcInfo, err := os.Stat(srcPath)
	if err != nil {
		return "", errModelNotFound
	}
	if srcInfo.IsDir() {
		return "", errInvalidModelPath
	}

	filename := filepath.Base(srcPath)
	localDir := filepath.Join(destDir, "local")
	destPath := filepath.Join(localDir, filename)
	if _, err := os.Stat(destPath); err == nil {
		return "", errAlreadyExists
	}
	if ds := h.getDownload(filename); ds != nil {
		return "", errAlreadyExists
	}

	groupName := uikit.FormatModelName(stripQuant(filename))
	ctx, cancel := context.WithCancel(context.Background())
	ds := h.addDownload("local", filename, cancel)
	ds.GroupName = groupName
	ds.Total = srcInfo.Size()

	h.logger.Info().Str("src", srcPath).Str("dest", destPath).Msg("starting model import")
	h.broker.Broadcast(SSEEvent{
		Type:        EventTypeDownload,
		AppName:     "__mass__",
		DlFilename:  filename,
		DlRepoID:    "local",
		DlGroupName: groupName,
		DlStart:     true,
	})
	go h.runImport(ctx, ds, srcPath, localDir, destPath)
	return "local/" + strings.TrimSuffix(filename, filepath.Ext(filename)), nil
}

// handleImportModel is the UI wrapper for importModelFile.
func (h *Handler) handleImportModel(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string `json:"path"`
	}
	if err := decodeJSON(r, &req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Path == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "path is required"})
		return
	}
	if _, err := h.importModelFile(req.Path); err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, errModelNotFound):
			status = http.StatusBadRequest
		case errors.Is(err, errInvalidModelPath):
			status = http.StatusBadRequest
		case errors.Is(err, errAlreadyExists):
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"filename": filepath.Base(req.Path)})
}

// runImport copies a local file into the models directory with progress updates.
func (h *Handler) runImport(ctx context.Context, ds *downloadState, srcPath, localDir, destPath string) {
	// Ensure local/ subdirectory exists.
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		h.logger.Error().Err(err).Str("dir", localDir).Msg("failed to create local models dir")
		h.removeDownload(ds.Filename)
		h.broker.Broadcast(SSEEvent{
			Type:       EventTypeDownload,
			AppName:    "__mass__",
			DlFilename: ds.Filename,
			Error:      err,
		})
		return
	}

	src, err := os.Open(srcPath)
	if err != nil {
		h.logger.Error().Err(err).Str("file", srcPath).Msg("failed to open source file")
		h.removeDownload(ds.Filename)
		h.broker.Broadcast(SSEEvent{
			Type:       EventTypeDownload,
			AppName:    "__mass__",
			DlFilename: ds.Filename,
			Error:      err,
		})
		return
	}
	defer func() {
		if cErr := src.Close(); cErr != nil {
			h.logger.Warn().Err(cErr).Str("file", ds.Filename).Msg("closing import source")
		}
	}()

	// Write to temp file then rename for atomicity.
	tmpPath := destPath + ".importing"
	dst, err := os.Create(tmpPath)
	if err != nil {
		h.logger.Error().Err(err).Str("file", tmpPath).Msg("failed to create temp import file")
		h.removeDownload(ds.Filename)
		h.broker.Broadcast(SSEEvent{
			Type:       EventTypeDownload,
			AppName:    "__mass__",
			DlFilename: ds.Filename,
			Error:      err,
		})
		return
	}

	total := ds.Total
	var copied int64
	var lastPct int64 = -1
	buf := make([]byte, 256*1024) // 256KB buffer

	for {
		// Check for cancellation.
		select {
		case <-ctx.Done():
			if cErr := dst.Close(); cErr != nil {
				h.logger.Warn().Err(cErr).Str("file", tmpPath).Msg("closing import temp on cancel")
			}
			if rmErr := os.Remove(tmpPath); rmErr != nil {
				h.logger.Warn().Err(rmErr).Str("file", tmpPath).Msg("removing import temp on cancel")
			}
			h.removeDownload(ds.Filename)
			h.broker.Broadcast(SSEEvent{
				Type:        EventTypeDownload,
				AppName:     "__mass__",
				DlFilename:  ds.Filename,
				DlCancelled: true,
			})
			return
		default:
		}

		n, readErr := src.Read(buf)
		if n > 0 {
			if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
				if cErr := dst.Close(); cErr != nil {
					h.logger.Warn().Err(cErr).Str("file", tmpPath).Msg("closing import temp after write failure")
				}
				if rmErr := os.Remove(tmpPath); rmErr != nil {
					h.logger.Warn().Err(rmErr).Str("file", tmpPath).Msg("removing import temp after write failure")
				}
				h.logger.Error().Err(writeErr).Str("file", ds.Filename).Msg("import write failed")
				h.removeDownload(ds.Filename)
				h.broker.Broadcast(SSEEvent{
					Type:       EventTypeDownload,
					AppName:    "__mass__",
					DlFilename: ds.Filename,
					Error:      writeErr,
				})
				return
			}
			copied += int64(n)

			ds.mu.Lock()
			ds.Downloaded = copied
			ds.mu.Unlock()

			if total > 0 {
				pct := 100 * copied / total
				if pct != lastPct {
					lastPct = pct
					h.broker.Broadcast(SSEEvent{
						Type:         EventTypeDownload,
						AppName:      "__mass__",
						DlFilename:   ds.Filename,
						DlDownloaded: copied,
						DlTotal:      total,
					})
				}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			if cErr := dst.Close(); cErr != nil {
				h.logger.Warn().Err(cErr).Str("file", tmpPath).Msg("closing import temp after read failure")
			}
			if rmErr := os.Remove(tmpPath); rmErr != nil {
				h.logger.Warn().Err(rmErr).Str("file", tmpPath).Msg("removing import temp after read failure")
			}
			h.logger.Error().Err(readErr).Str("file", ds.Filename).Msg("import read failed")
			h.removeDownload(ds.Filename)
			h.broker.Broadcast(SSEEvent{
				Type:       EventTypeDownload,
				AppName:    "__mass__",
				DlFilename: ds.Filename,
				Error:      readErr,
			})
			return
		}
	}

	if err := dst.Close(); err != nil {
		if rmErr := os.Remove(tmpPath); rmErr != nil {
			h.logger.Warn().Err(rmErr).Str("file", tmpPath).Msg("removing import temp after close failure")
		}
		h.logger.Error().Err(err).Str("file", ds.Filename).Msg("import close failed")
		h.removeDownload(ds.Filename)
		h.broker.Broadcast(SSEEvent{
			Type:       EventTypeDownload,
			AppName:    "__mass__",
			DlFilename: ds.Filename,
			Error:      err,
		})
		return
	}

	// Atomic rename.
	if err := os.Rename(tmpPath, destPath); err != nil {
		if rmErr := os.Remove(tmpPath); rmErr != nil {
			h.logger.Warn().Err(rmErr).Str("file", tmpPath).Msg("removing import temp after rename failure")
		}
		h.logger.Error().Err(err).Str("file", ds.Filename).Msg("import rename failed")
		h.removeDownload(ds.Filename)
		h.broker.Broadcast(SSEEvent{
			Type:       EventTypeDownload,
			AppName:    "__mass__",
			DlFilename: ds.Filename,
			Error:      err,
		})
		return
	}

	h.logger.Info().Str("file", ds.Filename).Str("path", destPath).Msg("model import complete")
	h.removeDownload(ds.Filename)
	h.broker.Broadcast(SSEEvent{
		Type:       EventTypeDownload,
		AppName:    "__mass__",
		DlFilename: ds.Filename,
		DlDone:     true,
		DlPath:     destPath,
	})
}

// convertHFModels converts huggingface.Model to uikit.HFResultModel.
func convertHFModels(models []sdkhf.Model) []uikit.HFResultModel {
	out := make([]uikit.HFResultModel, len(models))
	for i, m := range models {
		out[i] = uikit.HFResultModel{
			RepoID:      m.RepoID,
			Description: m.Description,
			Downloads:   m.Downloads,
			Likes:       m.Likes,
			Params:      m.Params,
			PipelineTag: m.PipelineTag,
			AvatarURL:   m.AvatarURL,
		}
		for _, f := range m.Files {
			out[i].Files = append(out[i].Files, uikit.HFResultFile{
				Filename:  f.Filename,
				SizeBytes: f.SizeBytes,
			})
		}
	}
	return out
}

// handleLoadModel loads a model into the pool from the UI.
func (h *Handler) handleLoadModel(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path          string `json:"path"`
		Type          string `json:"type"` // "chat" or "embedding"
		ContextSize   int32  `json:"contextSize,omitempty"`
		BatchSize     int32  `json:"batchSize,omitempty"`
		GpuLayers     int32  `json:"gpuLayers,omitempty"`
		Threads       int32  `json:"threads,omitempty"`
		MaxConcurrent int32  `json:"maxConcurrent,omitempty"`
		MaxTokens     int32  `json:"maxTokens,omitempty"`
		FlashAttn     string `json:"flashAttn,omitempty"`
		MainGPU       string `json:"mainGpu,omitempty"`
		TensorSplit   string `json:"tensorSplit,omitempty"`
		Thinking      bool   `json:"thinking,omitempty"`
		MmprojPath    string `json:"mmprojPath,omitempty"`
		ChatTemplate  string `json:"chatTemplate,omitempty"`
		CacheType     string `json:"cacheType,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Path == "" || req.Type == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "path and type are required"})
		return
	}

	// Resolve the path. Accepts absolute paths, paths inside the models directory,
	// or model IDs like "publisher/repo/variant".
	absPath := req.Path
	if resolved, err := config.ResolveModelPath(req.Path, h.modelsDir()); err == nil {
		absPath = resolved
	}

	var fp string
	var err error
	switch req.Type {
	case "chat":
		mmprojPath := req.MmprojPath
		if mmprojPath != "" {
			if resolved, resolveErr := config.ResolveModelPath(mmprojPath, h.modelsDir()); resolveErr == nil {
				mmprojPath = resolved
			}
		} else {
			// Auto-detect vision projector in the model's directory.
			mmprojPath = config.DetectMmproj(absPath)
		}
		cfg := config.LlamaChatConfig{
			Path:         absPath,
			ContextSize:  req.ContextSize,
			BatchSize:    req.BatchSize,
			MaxTokens:    req.MaxTokens,
			FlashAttn:    req.FlashAttn,
			Thinking:     req.Thinking,
			MmprojPath:   mmprojPath,
			ChatTemplate: req.ChatTemplate,
			CacheType:    req.CacheType,
		}.WithDefaults()
		placement := config.PlacementConfig{
			GpuLayers:     req.GpuLayers,
			Threads:       req.Threads,
			MaxConcurrent: req.MaxConcurrent,
			MainGPU:       req.MainGPU,
			TensorSplit:   req.TensorSplit,
		}
		fp, err = h.orch.LoadModel(cfg, placement, "direct", scheduler.ModeStatic)
	case "embedding":
		cfg := config.LlamaEmbeddingConfig{
			Path:        absPath,
			ContextSize: req.ContextSize,
		}.WithDefaults()
		placement := config.PlacementConfig{
			GpuLayers:     req.GpuLayers,
			Threads:       req.Threads,
			MaxConcurrent: req.MaxConcurrent,
			MainGPU:       req.MainGPU,
			TensorSplit:   req.TensorSplit,
		}
		fp, err = h.orch.LoadModel(cfg, placement, "direct", scheduler.ModeStatic)
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "type must be 'chat' or 'embedding'"})
		return
	}

	if err != nil {
		h.logger.Error().Err(err).Str("path", absPath).Str("type", req.Type).Msg("failed to load model")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	h.logger.Info().Str("path", absPath).Str("type", req.Type).Str("fingerprint", fp).Msg("model loaded from UI")
	writeJSON(w, http.StatusOK, map[string]string{"fingerprint": fp})
}

// handleModelsSelect returns HTML for a model selection overlay filtered by type.
func (h *Handler) handleModelsSelect(w http.ResponseWriter, r *http.Request) {
	filterType := strings.ToLower(r.URL.Query().Get("type")) // "chat", "embedding", or "" for all

	dir := h.modelsDir()
	var models []LocalModelInfo
	if dir != "" {
		var err error
		models, err = ScanModels(dir)
		if err != nil {
			models = nil
		}
	}

	// Always exclude mmproj files from the select dialog — they're
	// vision projector dependencies, not standalone models.
	// Also filter by type if requested.
	{
		var filtered []LocalModelInfo
		for _, m := range models {
			if m.ModelType == ModelFileKindMmproj {
				continue
			}
			if filterType != "" && string(m.ModelType) != filterType {
				continue
			}
			filtered = append(filtered, m)
		}
		models = filtered
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	mustHTTPWrite(w, []byte(RenderModelSelectOverlay(models, filterType)))
}

// RenderModelSelectOverlay renders HTML for a model selection overlay.
// It shows local models of the given type and lets users pick one.
// Includes a search input for client-side filtering and caps the list at ~5 visible rows.
func RenderModelSelectOverlay(models []LocalModelInfo, filterType string) string {
	var b strings.Builder

	// Search input for filtering (JS wired in shellScripts __massModelSelect).
	if len(models) > 0 {
		b.WriteString(`<sl-input id="mass-model-select-search" size="small" clearable placeholder="Filter models..." autocomplete="off" style="margin-bottom:0.4rem"></sl-input>`)
	}

	b.WriteString(`<div id="mass-model-select-entries" class="overflow-y-auto" style="max-height:180px">`)
	if len(models) == 0 {
		b.WriteString(`<p class="text-neutral-500 text-sm px-3 py-4">No `)
		if filterType != "" {
			b.WriteString(strings.ToLower(filterType) + " ")
		}
		b.WriteString(`models found. Download models from the Models tab first.</p>`)
	} else {
		for _, m := range models {
			quant := m.Quantization
			// Display name: publisher/filename for models in subdirs, just filename otherwise.
			displayName := m.Filename
			if parts := strings.SplitN(m.SubDir, "/", 2); len(parts) >= 1 && parts[0] != "" {
				displayName = parts[0] + "/" + m.Filename
			}
			fmt.Fprintf(&b, `<div class="flex items-center gap-2 px-3 py-2 hover:bg-neutral-700/40 rounded cursor-pointer" data-filename="%s" data-model-type="%s" data-model-id="%s" `,
				html.EscapeString(displayName), m.ModelType, html.EscapeString(m.ModelID))
			fmt.Fprintf(&b, `onclick="window.__massSelectModel('%s','%s')"`, jsStringEscape(m.ModelID), m.ModelType)
			b.WriteString(`>`)
			if quant != "" {
				fmt.Fprintf(&b, `<span class="mass-badge-alt font-mono text-xs font-bold rounded px-1.5 py-0.5" style="min-width:4.5rem;text-align:center">%s</span>`, quant)
			} else {
				b.WriteString(`<span style="min-width:4.5rem;flex-shrink:0"></span>`)
			}
			badgeCls := "mass-badge"
			switch m.ModelType {
			case ModelFileKindChat:
				badgeCls = "mass-badge mass-badge-chat"
			case ModelFileKindEmbedding:
				badgeCls = "mass-badge mass-badge-embedding"
			case ModelFileKindMmproj:
				badgeCls = "mass-badge mass-badge-mmproj"
			}
			fmt.Fprintf(&b, `<span class="%s font-mono text-xs font-bold rounded px-1.5 py-0.5">%s</span>`, badgeCls, m.ModelType.Title())
			if m.HasVision {
				b.WriteString(`<sl-tooltip content="Vision capable"><sl-icon name="eye" style="font-size:0.85rem;color:var(--sl-color-primary-400)"></sl-icon></sl-tooltip>`)
			}
			if m.HasThinking {
				b.WriteString(`<sl-tooltip content="Thinking capable"><sl-icon name="lightbulb" style="font-size:0.85rem;color:var(--sl-color-warning-400)"></sl-icon></sl-tooltip>`)
			}
			fmt.Fprintf(&b, `<span class="text-xs text-neutral-300 truncate flex-1" title="%s">%s</span>`,
				html.EscapeString(displayName), html.EscapeString(displayName))
			fmt.Fprintf(&b, `<span class="text-xs text-neutral-500 flex-shrink-0" style="width:5rem;text-align:right">%s</span>`,
				uikit.FormatBytes(m.SizeBytes))
			b.WriteString(`</div>`)
		}
	}
	b.WriteString(`</div>`)
	return b.String()
}
