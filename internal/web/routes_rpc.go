package web

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/rpc"
)

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
	for _, m := range all {
		if m.ModelType == "Mmproj" {
			continue
		}
		if req.Type != "" && strings.ToLower(m.ModelType) != req.Type {
			continue
		}
		if req.Search != "" {
			terms := strings.Fields(strings.ToLower(req.Search))
			haystack := strings.ToLower(m.ModelID + " " + m.Filename + " " + m.ModelType)
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
			Type:      strings.ToLower(m.ModelType),
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

// handleRPCLoadModel implements mass.v1.Mass/LoadModel.
func (h *Handler) handleRPCLoadModel(w http.ResponseWriter, r *http.Request) {
	var req rpc.LoadModelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	if req.Path == "" || req.Type == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "path and type are required"})
		return
	}

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
			mmprojPath = config.DetectMmproj(absPath)
		}
		cfg := config.ChatModelConfig{
			Path:         absPath,
			ContextSize:  req.ContextSize,
			BatchSize:    req.BatchSize,
			MaxTokens:    req.MaxTokens,
			FlashAttn:    req.FlashAttn,
			Thinking:     req.Thinking,
			MmprojPath:   mmprojPath,
			ChatTemplate: req.ChatTemplate,
			CacheType:    req.CacheType,
		}
		placement := config.PlacementConfig{
			GpuLayers:     req.GpuLayers,
			Threads:       req.Threads,
			MaxConcurrent: req.MaxConcurrent,
			MainGPU:       req.MainGpu,
			TensorSplit:   req.TensorSplit,
		}
		fp, err = h.orch.LoadModel("chat", &cfg, nil, placement, "direct")
	case "embedding":
		cfg := config.EmbeddingModelConfig{
			Path:        absPath,
			ContextSize: req.ContextSize,
		}
		placement := config.PlacementConfig{
			GpuLayers:     req.GpuLayers,
			Threads:       req.Threads,
			MaxConcurrent: req.MaxConcurrent,
			MainGPU:       req.MainGpu,
			TensorSplit:   req.TensorSplit,
		}
		fp, err = h.orch.LoadModel("embedding", nil, &cfg, placement, "direct")
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "type must be 'chat' or 'embedding'"})
		return
	}

	if err != nil {
		h.logger.Error().Err(err).Str("path", absPath).Str("type", req.Type).Msg("failed to load model")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	h.logger.Info().Str("path", absPath).Str("type", req.Type).Str("fingerprint", fp).Msg("model loaded via RPC")
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
		AgentIDs:  sliceToSet(req.AgentIds),
		DeviceIDs: sliceToSet(req.DeviceIds),
	}

	results := h.runBenchmarks(target)
	h.broker.Broadcast(SSEEvent{Type: EventTypeAgentChange})

	var pbResults []*rpc.BenchmarkResult
	for _, br := range results {
		pbResults = append(pbResults, &rpc.BenchmarkResult{
			AgentId:       br.AgentID,
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

	h.broker.Broadcast(SSEEvent{Type: EventTypeAgentChange})
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
		Mode:           info.Mode.String(),
		Source:         info.Source,
	}
}
