package web

import (
	"net/http"
	"strings"

	"github.com/chinese-room-solutions/mass-sdk/uikit"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
)

// --- JSON response types for /api/v1/models ---

// APIModel is the JSON shape for a model in the list and detail endpoints.
type APIModel struct {
	ID           string           `json:"id"`
	Type         string           `json:"type"`
	SizeBytes    int64            `json:"size_bytes"`
	Size         string           `json:"size"`
	Quantization string           `json:"quantization,omitempty"`
	Capabilities *APICapabilities `json:"capabilities,omitempty"`

	// Detail fields (only populated by the detail endpoint).
	Architecture    string `json:"architecture,omitempty"`
	Name            string `json:"name,omitempty"`
	ContextLength   uint64 `json:"context_length,omitempty"`
	EmbeddingLength uint64 `json:"embedding_length,omitempty"`
	BlockCount      uint64 `json:"block_count,omitempty"`
	HeadCount       uint64 `json:"head_count,omitempty"`
	HeadCountKV     uint64 `json:"head_count_kv,omitempty"`
	VocabSize       int    `json:"vocab_size,omitempty"`
	TokenizerModel  string `json:"tokenizer_model,omitempty"`
	ChatTemplate    string `json:"chat_template,omitempty"`
	QuantType       string `json:"quant_type,omitempty"`

	// Runtime status (only included when ?status=true).
	Status *APIModelStatus `json:"status,omitempty"`
}

// APICapabilities describes what features a model supports.
type APICapabilities struct {
	Vision   bool `json:"vision"`
	Thinking bool `json:"thinking"`
}

// APIModelStatus holds runtime state from the scheduler model pool.
type APIModelStatus struct {
	Loaded     bool   `json:"loaded"`
	ActiveReqs int64  `json:"active_reqs,omitempty"`
	Mode       string `json:"mode,omitempty"`
	Source     string `json:"source,omitempty"`
}

// handleAPIListModels handles GET /api/v1/models.
// Query params: type (chat|embedding), search (free text), status (true to include runtime state).
func (h *Handler) handleAPIListModels(w http.ResponseWriter, r *http.Request) {
	dir := h.modelsDir()
	if dir == "" {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "data directory not configured"})
		return
	}

	// If ?id= is provided, delegate to the detail handler.
	if id := r.URL.Query().Get("id"); id != "" {
		h.handleAPIGetModel(w, r, id)
		return
	}

	all, err := ScanModels(dir)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "scanning models: " + err.Error()})
		return
	}

	// Filter out mmproj files (they are dependencies, not standalone models).
	var models []LocalModelInfo
	for _, m := range all {
		if m.ModelType != ModelFileKindMmproj {
			models = append(models, m)
		}
	}

	// Apply ?type= filter.
	if typeFilter := r.URL.Query().Get("type"); typeFilter != "" {
		typeFilter = strings.ToLower(typeFilter)
		var filtered []LocalModelInfo
		for _, m := range models {
			if string(m.ModelType) == typeFilter {
				filtered = append(filtered, m)
			}
		}
		models = filtered
	}

	// Apply ?search= filter.
	if search := r.URL.Query().Get("search"); search != "" {
		terms := strings.Fields(strings.ToLower(search))
		var filtered []LocalModelInfo
		for _, m := range models {
			haystack := strings.ToLower(m.ModelID + " " + m.Filename + " " + string(m.ModelType))
			match := true
			for _, term := range terms {
				if !strings.Contains(haystack, term) {
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

	// Build status map if requested.
	wantStatus := r.URL.Query().Get("status") == "true"
	var statusByPath map[string]scheduler.ModelInstanceInfo
	if wantStatus {
		statusByPath = loadedModelsByPath(h.orch.PoolSnapshot())
	}

	// Build response.
	result := make([]APIModel, 0, len(models))
	for _, m := range models {
		item := APIModel{
			ID:           m.ModelID,
			Type:         string(m.ModelType),
			SizeBytes:    m.SizeBytes,
			Size:         uikit.FormatBytes(m.SizeBytes),
			Quantization: m.Quantization,
			Capabilities: &APICapabilities{
				Vision: m.HasVision,
			},
		}
		if wantStatus {
			item.Status = buildModelStatus(m.Path, statusByPath)
		}
		result = append(result, item)
	}

	writeJSON(w, http.StatusOK, result)
}

// handleAPIGetModel handles the detail view when ?id= is provided.
func (h *Handler) handleAPIGetModel(w http.ResponseWriter, r *http.Request, id string) {
	dir := h.modelsDir()

	absPath, err := config.ResolveModelPath(id, dir)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "model not found"})
		return
	}

	info, err := ReadGGUFModelInfo(absPath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "reading model metadata: " + err.Error()})
		return
	}

	relPath, _ := strings.CutPrefix(strings.ReplaceAll(absPath, "\\", "/"), strings.ReplaceAll(dir, "\\", "/")+"/")
	modelType := string(inferModelType(info.Filename, relPath))
	modelID := strings.TrimSuffix(relPath, ".gguf")

	hasVision := config.DetectMmproj(absPath) != ""
	hasThinking := detectThinkingSupport(info.ChatTemplate)

	result := APIModel{
		ID:   modelID,
		Type: modelType,
		Capabilities: &APICapabilities{
			Vision:   hasVision,
			Thinking: hasThinking,
		},
		SizeBytes:       info.FileSize,
		Size:            info.FileSizeStr,
		Architecture:    info.Architecture,
		Name:            info.Name,
		ContextLength:   info.ContextLength,
		EmbeddingLength: info.EmbeddingLength,
		BlockCount:      info.BlockCount,
		HeadCount:       info.HeadCount,
		HeadCountKV:     info.HeadCountKV,
		VocabSize:       info.VocabSize,
		TokenizerModel:  info.TokenizerModel,
		ChatTemplate:    detectChatTemplateName(info.ChatTemplate),
		QuantType:       info.QuantType,
	}

	if result.Quantization == "" {
		result.Quantization = uikit.ExtractQuant(info.Filename)
	}

	if r.URL.Query().Get("status") == "true" {
		statusByPath := loadedModelsByPath(h.orch.PoolSnapshot())
		result.Status = buildModelStatus(absPath, statusByPath)
	}

	writeJSON(w, http.StatusOK, result)
}

// loadedModelsByPath builds a lookup map from absolute model path to pool instance info.
func loadedModelsByPath(snapshot []scheduler.ModelInstanceInfo) map[string]scheduler.ModelInstanceInfo {
	m := make(map[string]scheduler.ModelInstanceInfo, len(snapshot))
	for _, inst := range snapshot {
		// Normalize path separators for consistent matching on Windows.
		normalized := strings.ReplaceAll(inst.Path, "\\", "/")
		if _, exists := m[normalized]; !exists {
			m[normalized] = inst
		}
	}
	return m
}

// buildModelStatus creates an APIModelStatus from the pool snapshot, if the model is loaded.
func buildModelStatus(absPath string, statusByPath map[string]scheduler.ModelInstanceInfo) *APIModelStatus {
	normalized := strings.ReplaceAll(absPath, "\\", "/")
	inst, loaded := statusByPath[normalized]
	if !loaded {
		return &APIModelStatus{Loaded: false}
	}
	return &APIModelStatus{
		Loaded:     true,
		ActiveReqs: inst.ActiveReqs,
		Mode:       inst.Mode.String(),
		Source:     inst.Source,
	}
}

// detectThinkingSupport checks whether a model's raw chat template contains
// thinking/reasoning token patterns (e.g. <think>, {%- if enable_thinking -%}).
func detectThinkingSupport(rawChatTemplate string) bool {
	if rawChatTemplate == "" {
		return false
	}
	t := strings.ToLower(rawChatTemplate)
	return strings.Contains(t, "<think>") ||
		strings.Contains(t, "enable_thinking") ||
		strings.Contains(t, "reasoning_content")
}
