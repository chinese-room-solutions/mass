package config

import (
	"os"
	"path/filepath"
	"strings"
)

// ResolveModelPath resolves a model reference to an absolute path.
// Accepts an absolute existing path, or a "publisher/repo/variant" model
// ID resolved under modelsDir (with ".gguf" appended if needed).
// Returns ErrModelNotFound if neither matches.
func ResolveModelPath(modelRef string, modelsDir string) (string, error) {
	if modelRef == "" {
		return "", ErrModelPathEmpty
	}

	// Absolute path — check if file exists.
	if filepath.IsAbs(modelRef) {
		if _, err := os.Stat(modelRef); err == nil {
			return modelRef, nil
		}
		// Also try with .gguf appended.
		if !strings.HasSuffix(strings.ToLower(modelRef), ".gguf") {
			withExt := modelRef + ".gguf"
			if _, err := os.Stat(withExt); err == nil {
				return withExt, nil
			}
		}
		return modelRef, nil // return as-is for absolute paths even if not found (loader will report error)
	}

	if modelsDir == "" {
		return "", ErrModelNotFound
	}

	// Model ID — resolve relative to modelsDir.
	// Try exact match first (with extension).
	candidate := filepath.Join(modelsDir, filepath.FromSlash(modelRef))
	if _, err := os.Stat(candidate); err == nil {
		return candidate, nil
	}

	// Try with .gguf extension.
	if !strings.HasSuffix(strings.ToLower(modelRef), ".gguf") {
		withExt := candidate + ".gguf"
		if _, err := os.Stat(withExt); err == nil {
			return withExt, nil
		}
	}

	return "", ErrModelNotFound
}

// DetectMmproj scans the model's directory for a vision projector GGUF
// (mmproj-*.gguf or *mmproj*.gguf). Returns the absolute path or "".
func DetectMmproj(modelPath string) string {
	dir := filepath.Dir(modelPath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := strings.ToLower(e.Name())
		if strings.Contains(name, "mmproj") && strings.HasSuffix(name, ".gguf") {
			return filepath.Join(dir, e.Name())
		}
	}
	return ""
}

// ModelIDFromPath derives a model ID from an absolute path relative to
// the models directory. Returns the relative path without extension,
// or the filename without extension if the path is not under modelsDir.
func ModelIDFromPath(absPath string, modelsDir string) string {
	if modelsDir != "" {
		rel, err := filepath.Rel(modelsDir, absPath)
		if err == nil && !strings.HasPrefix(rel, "..") {
			rel = filepath.ToSlash(rel)
			return strings.TrimSuffix(rel, filepath.Ext(rel))
		}
	}
	base := filepath.Base(absPath)
	return strings.TrimSuffix(base, filepath.Ext(base))
}
