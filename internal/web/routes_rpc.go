package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	rpc "github.com/chinese-room-solutions/mass-proto/gen/go"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
)

// kindFromProto maps the proto ModelKind enum to the internal string
// representation used by ScanModels and the model pool. UNSPECIFIED maps to
// "" so callers can use it as "match all".
func kindFromProto(k rpc.ModelKind) string {
	switch k {
	case rpc.ModelKind_MODEL_KIND_CHAT:
		return "chat"
	case rpc.ModelKind_MODEL_KIND_EMBEDDING:
		return "embedding"
	default:
		return ""
	}
}

// kindToProto is the inverse of kindFromProto.
func kindToProto(s string) rpc.ModelKind {
	switch s {
	case "chat":
		return rpc.ModelKind_MODEL_KIND_CHAT
	case "embedding":
		return rpc.ModelKind_MODEL_KIND_EMBEDDING
	default:
		return rpc.ModelKind_MODEL_KIND_UNSPECIFIED
	}
}

// loadModeToProto maps the scheduler's InstanceMode to the proto LoadMode enum.
func loadModeToProto(m scheduler.InstanceMode) rpc.LoadMode {
	switch m {
	case scheduler.ModeDynamic:
		return rpc.LoadMode_LOAD_MODE_DYNAMIC
	case scheduler.ModeStatic:
		return rpc.LoadMode_LOAD_MODE_STATIC
	default:
		return rpc.LoadMode_LOAD_MODE_UNSPECIFIED
	}
}

// rejectAppSource blocks app-source requests from destructive ops. Until
// apps get manifest-declared permissions, only `direct` (user-token)
// callers can pause/drain device queues. Returns true if rejected
// (caller should return).
func rejectAppSource(w http.ResponseWriter, r *http.Request) bool {
	if strings.HasPrefix(r.Header.Get("X-Mass-Source"), "app:") {
		writeJSON(w, http.StatusForbidden, map[string]string{
			"error": "this RPC is not available to app callers; use the user API token",
		})
		return true
	}
	return false
}

// handleRPCListModels implements mass.v1.Mass/ListModels.
func (h *Handler) handleRPCListModels(w http.ResponseWriter, r *http.Request) {
	var req rpc.ListModelsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	dir := h.modelsDir()
	if dir == "" {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "data directory not configured"})
		return
	}

	all, err := ScanModels(dir)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Filter out mmproj files.
	var models []LocalModelInfo
	kindFilter := kindFromProto(req.Kind)
	for _, m := range all {
		if m.ModelType == ModelFileKindMmproj {
			continue
		}
		if kindFilter != "" && string(m.ModelType) != kindFilter {
			continue
		}
		if req.Search != "" {
			terms := strings.Fields(strings.ToLower(req.Search))
			haystack := strings.ToLower(m.ModelID + " " + m.Filename + " " + string(m.ModelType))
			match := true
			for _, term := range terms {
				if !strings.Contains(haystack, term) {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}
		models = append(models, m)
	}

	var statusByPath map[string]scheduler.ModelInstanceInfo
	if req.IncludeStatus {
		statusByPath = loadedModelsByPath(h.orch.PoolSnapshot())
	}

	result := make([]*rpc.ModelInfo, 0, len(models))
	for _, m := range models {
		info := &rpc.ModelInfo{
			Id:        m.ModelID,
			Kind:      kindToProto(string(m.ModelType)),
			SizeBytes: m.SizeBytes,
			Capabilities: &rpc.ModelCapabilities{
				Vision:   m.HasVision,
				Thinking: m.HasThinking,
			},
			Quantization: m.Quantization,
		}
		if req.IncludeStatus && statusByPath != nil {
			info.Status = buildRPCModelStatus(m.Path, statusByPath)
		}
		result = append(result, info)
	}

	writeJSON(w, http.StatusOK, &rpc.ListModelsResponse{Models: result})
}

// handleRPCListLoadedModels implements mass.v1.Mass/ListLoadedModels.
// Returns the exact identity config for each loaded instance so callers
// can echo it back verbatim in inference requests and hit the same pool
// entry rather than triggering a second load with subtly different defaults.
func (h *Handler) handleRPCListLoadedModels(w http.ResponseWriter, r *http.Request) {
	var req rpc.ListLoadedModelsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	all := h.orch.LoadedSnapshot()
	out := make([]*rpc.LoadedModel, 0, len(all))
	kindFilter := req.Kind
	for _, inst := range all {
		instKind := kindToProto(string(inst.Type))
		if kindFilter != rpc.ModelKind_MODEL_KIND_UNSPECIFIED && kindFilter != instKind {
			continue
		}
		lm := &rpc.LoadedModel{
			Fingerprint:    inst.Fingerprint,
			Kind:           instKind,
			Source:         inst.Source,
			ActiveRequests: inst.ActiveReqs,
			WorkerId:       inst.WorkerID,
			WorkerName:     inst.WorkerName,
			DeviceIds:      inst.DeviceIDs,
		}
		switch {
		case inst.ChatConfig != nil:
			lm.Config = &rpc.LoadedModel_LlamaChat{LlamaChat: chatConfigToProto(*inst.ChatConfig, inst.Placement)}
		case inst.EmbeddingConfig != nil:
			lm.Config = &rpc.LoadedModel_LlamaEmbedding{LlamaEmbedding: embeddingConfigToProto(*inst.EmbeddingConfig, inst.Placement)}
		}
		out = append(out, lm)
	}

	writeJSON(w, http.StatusOK, &rpc.ListLoadedModelsResponse{Models: out})
}

// chatConfigToProto serializes a chat identity+placement pair into the
// proto LlamaChatConfig that callers should echo back in their inference
// requests' model_config field.
func chatConfigToProto(c config.LlamaChatConfig, p config.PlacementConfig) *rpc.LlamaChatConfig {
	return &rpc.LlamaChatConfig{
		Model:         c.Path,
		ContextSize:   protoInt32(c.ContextSize),
		BatchSize:     protoInt32(c.BatchSize),
		FlashAttn:     flashAttnToProto(c.FlashAttn),
		Thinking:      c.Thinking,
		Mmproj:        c.MmprojPath,
		ChatTemplate:  c.ChatTemplate,
		CacheType:     cacheTypeToProto(c.CacheType),
		GpuLayers:     protoInt32(p.GpuLayers),
		Threads:       protoInt32(p.Threads),
		MaxConcurrent: protoInt32(p.MaxConcurrent),
		MainGpu:       p.MainGPU,
		TensorSplit:   tensorSplitToProto(p.TensorSplit),
	}
}

// embeddingConfigToProto is the embedding counterpart to chatConfigToProto.
func embeddingConfigToProto(c config.LlamaEmbeddingConfig, p config.PlacementConfig) *rpc.LlamaEmbeddingConfig {
	return &rpc.LlamaEmbeddingConfig{
		Model:         c.Path,
		ContextSize:   protoInt32(c.ContextSize),
		GpuLayers:     protoInt32(p.GpuLayers),
		Threads:       protoInt32(p.Threads),
		MaxConcurrent: protoInt32(p.MaxConcurrent),
		MainGpu:       p.MainGPU,
		TensorSplit:   tensorSplitToProto(p.TensorSplit),
	}
}

// protoInt32 returns &v for proto3 `optional int32` fields.
func protoInt32(v int32) *int32 { return &v }

// flashAttnToProto maps the internal "enabled"/"disabled"/"" tri-state to
// proto3 optional bool (nil = auto).
func flashAttnToProto(s string) *bool {
	switch s {
	case "enabled":
		t := true
		return &t
	case "disabled":
		f := false
		return &f
	default:
		return nil
	}
}

// cacheTypeToProto maps the internal cache-type string to the CacheType enum.
func cacheTypeToProto(s string) rpc.CacheType {
	switch s {
	case "f16":
		return rpc.CacheType_CACHE_TYPE_F16
	case "q8_0":
		return rpc.CacheType_CACHE_TYPE_Q8_0
	case "q4_0":
		return rpc.CacheType_CACHE_TYPE_Q4_0
	default:
		return rpc.CacheType_CACHE_TYPE_UNSPECIFIED
	}
}

// tensorSplitToProto parses the internal CSV form into a float slice.
func tensorSplitToProto(s string) []float32 {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]float32, 0, len(parts))
	for _, p := range parts {
		var v float32
		if _, err := fmt.Sscanf(strings.TrimSpace(p), "%g", &v); err == nil {
			out = append(out, v)
		}
	}
	return out
}

// handleRPCLoadModel implements mass.v1.Mass/LoadModel. The request carries
// the same identity config inference requests do (chat or embedding oneof),
// so there's exactly one way to spell "this model with these settings"
// across the API.
func (h *Handler) handleRPCLoadModel(w http.ResponseWriter, r *http.Request) {
	var req rpc.LoadModelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	var (
		fp      string
		err     error
		kind    string
		absPath string
	)
	switch cfgOneof := req.Config.(type) {
	case *rpc.LoadModelRequest_Chat:
		cfg, placement, convErr := scheduler.ChatConfigFromProto(cfgOneof.Chat, h.modelsDir())
		if convErr != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": convErr.Error()})
			return
		}
		absPath, kind = cfg.Path, "chat"
		fp, err = h.orch.LoadModel(cfg, placement, "direct", scheduler.ModeStatic)
	case *rpc.LoadModelRequest_Embedding:
		cfg, placement, convErr := scheduler.EmbeddingConfigFromProto(cfgOneof.Embedding, h.modelsDir())
		if convErr != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": convErr.Error()})
			return
		}
		absPath, kind = cfg.Path, "embedding"
		fp, err = h.orch.LoadModel(cfg, placement, "direct", scheduler.ModeStatic)
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "config oneof is required (chat or embedding)"})
		return
	}

	if err != nil {
		h.logger.Error().Err(err).Str("path", absPath).Str("type", kind).Msg("failed to load model")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	h.logger.Info().Str("path", absPath).Str("type", kind).Str("fingerprint", fp).Msg("model loaded via RPC")
	writeJSON(w, http.StatusOK, &rpc.LoadModelResponse{Fingerprint: fp})
}

// handleRPCDownloadModel implements mass.v1.Mass/DownloadModel.
// Starts an async HuggingFace model download. Progress is broadcast via SSE.
func (h *Handler) handleRPCDownloadModel(w http.ResponseWriter, r *http.Request) {
	var req rpc.DownloadModelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	if req.RepoId == "" || req.Filename == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "repo_id and filename are required"})
		return
	}

	if err := h.startModelDownload(req.RepoId, req.Filename); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, &rpc.DownloadModelResponse{})
}

// handleRPCRunBenchmark implements mass.v1.Mass/RunBenchmark.
func (h *Handler) handleRPCRunBenchmark(w http.ResponseWriter, r *http.Request) {
	var req rpc.RunBenchmarkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	target := benchmarkTarget{
		WorkerIDs: sliceToSet(req.WorkerIds),
		DeviceIDs: sliceToSet(req.DeviceIds),
	}

	results := h.runBenchmarks(target)
	h.broker.Broadcast(SSEEvent{Type: EventTypeWorkerChange})

	var pbResults []*rpc.BenchmarkResult
	for _, br := range results {
		pbResults = append(pbResults, &rpc.BenchmarkResult{
			WorkerId:      br.WorkerID,
			DeviceId:      br.DeviceID,
			DeviceName:    br.DeviceName,
			MemoryGbs:     br.MemoryGBs,
			ComputeGflops: br.ComputeGFlops,
			Error:         br.Error,
		})
	}

	writeJSON(w, http.StatusOK, &rpc.RunBenchmarkResponse{Results: pbResults})
}

// handleRPCSetDeviceEnabled implements mass.v1.Mass/SetDeviceEnabled.
func (h *Handler) handleRPCSetDeviceEnabled(w http.ResponseWriter, r *http.Request) {
	if rejectAppSource(w, r) {
		return
	}
	var req rpc.SetDeviceEnabledRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	if req.QueueName == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "queue_name is required"})
		return
	}

	drained, err := h.orch.SetDeviceQueueEnabled(r.Context(), req.QueueName, req.Enabled)
	if err != nil {
		h.logger.Error().Err(err).Str("queue", req.QueueName).Bool("enabled", req.Enabled).Msg("failed to set device enabled")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	h.broker.Broadcast(SSEEvent{Type: EventTypeWorkerChange})
	writeJSON(w, http.StatusOK, &rpc.SetDeviceEnabledResponse{DrainedTasks: int32(drained)})
}

// buildRPCModelStatus converts scheduler pool info to proto ModelStatus.
func buildRPCModelStatus(path string, statusByPath map[string]scheduler.ModelInstanceInfo) *rpc.ModelStatus {
	info, ok := statusByPath[path]
	if !ok {
		return nil
	}
	return &rpc.ModelStatus{
		Loaded:         true,
		ActiveRequests: info.ActiveReqs,
		Mode:           loadModeToProto(info.Mode),
		Source:         info.Source,
	}
}

// errModelNotFound and errInvalidModelPath are sentinel errors deleteModelFile
// returns so the UI handler can map them to 4xx responses without stringifying.
var (
	errModelNotFound    = errors.New("model not found")
	errInvalidModelPath = errors.New("invalid model path (outside models directory)")
)

// deleteModelFile removes a model file by ID (path or HF-style ID) and
// invalidates its GGUF cache. It also cleans up any newly-empty parent
// directories under the models root.
func (h *Handler) deleteModelFile(id string) error {
	dir := h.modelsDir()
	if dir == "" {
		return errors.New("data directory not configured")
	}
	resolved, err := config.ResolveModelPath(id, dir)
	if err != nil {
		return errModelNotFound
	}
	absPath, ok := isPathUnder(resolved, dir)
	if !ok {
		return errInvalidModelPath
	}
	if err := os.Remove(absPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errModelNotFound
		}
		return err
	}
	InvalidateGGUFCache(absPath)

	// Clean up empty parent directories up to the models root.
	parent := filepath.Dir(absPath)
	for parent != dir && parent != filepath.Dir(parent) {
		entries, dirErr := os.ReadDir(parent)
		if dirErr != nil || len(entries) > 0 {
			break
		}
		if rmErr := os.Remove(parent); rmErr != nil {
			h.logger.Warn().Err(rmErr).Str("dir", parent).Msg("failed to remove empty parent directory")
			break
		}
		parent = filepath.Dir(parent)
	}
	h.logger.Info().Str("path", absPath).Msg("model deleted")
	return nil
}
