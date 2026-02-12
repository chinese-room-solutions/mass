package web

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"
)

// MigrateModelDirs converts old "owner--repo" flat directories to the
// new "owner/repo" two-level structure. It is safe to call on every
// startup — it only acts when legacy directories exist.
func MigrateModelDirs(modelsDir string, logger zerolog.Logger) {
	if modelsDir == "" {
		return
	}
	entries, err := os.ReadDir(modelsDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		idx := strings.Index(name, "--")
		if idx <= 0 || idx == len(name)-2 {
			continue // not a legacy dir
		}

		owner := name[:idx]
		repo := name[idx+2:]
		oldDir := filepath.Join(modelsDir, name)
		newDir := filepath.Join(modelsDir, owner, repo)

		// Create the new two-level structure.
		if err := os.MkdirAll(newDir, 0755); err != nil {
			logger.Warn().Err(err).Str("dir", name).Msg("model migration: failed to create target dir")
			continue
		}

		// Move all files from old to new.
		files, err := os.ReadDir(oldDir)
		if err != nil {
			logger.Warn().Err(err).Str("dir", name).Msg("model migration: failed to read old dir")
			continue
		}

		allMoved := true
		for _, f := range files {
			src := filepath.Join(oldDir, f.Name())
			dst := filepath.Join(newDir, f.Name())
			if err := os.Rename(src, dst); err != nil {
				logger.Warn().Err(err).Str("file", f.Name()).Msg("model migration: failed to move file")
				allMoved = false
			}
		}

		if allMoved {
			_ = os.Remove(oldDir)
			logger.Info().Str("from", name).Str("to", owner+"/"+repo).Msg("model migration: migrated directory")
		}
	}
}
