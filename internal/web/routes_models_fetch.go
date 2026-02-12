package web

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/chinese-room-solutions/mass/internal/config"
)

// handleFetchModel serves model files to remote agents.
// GET /api/models/fetch/{path...}
// The path is relative to the models directory.
// Supports range requests for resumable downloads.
func (h *Handler) handleFetchModel(w http.ResponseWriter, r *http.Request) {
	relPath := r.PathValue("path")
	if relPath == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}

	// Sanitize: prevent directory traversal.
	relPath = filepath.Clean(relPath)
	if strings.Contains(relPath, "..") {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}

	dataDir, err := h.cfg.EffectiveDataDir()
	if err != nil {
		http.Error(w, "data dir not configured", http.StatusInternalServerError)
		return
	}

	absPath := filepath.Join(config.ModelsDir(dataDir), relPath)

	// Verify the file exists and is within the models directory.
	modelsDir := config.ModelsDir(dataDir)
	if !strings.HasPrefix(absPath, modelsDir) {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}

	f, err := os.Open(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "model not found", http.StatusNotFound)
		} else {
			http.Error(w, "error reading model", http.StatusInternalServerError)
		}
		return
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		http.Error(w, "error reading model", http.StatusInternalServerError)
		return
	}

	// ServeContent handles range requests, caching headers, and Content-Type.
	http.ServeContent(w, r, filepath.Base(absPath), stat.ModTime(), f)
}
