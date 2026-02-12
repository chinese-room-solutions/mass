package huggingface

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTempFilePath(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name     string
		repoID   string
		filename string
		wantDir  string // expected directory component relative to dir
		wantBase string // expected base filename
	}{
		{
			name:     "standard repo",
			repoID:   "unsloth/Qwen3.5-4B-GGUF",
			filename: "Qwen3.5-4B-UD-IQ2_M.gguf",
			wantDir:  "unsloth/Qwen3.5-4B-GGUF",
			wantBase: ".downloading-Qwen3.5-4B-UD-IQ2_M.gguf",
		},
		{
			name:     "repo with dots",
			repoID:   "TheBloke/Llama-2-7B-GGUF",
			filename: "llama-2-7b.Q4_K_M.gguf",
			wantDir:  "TheBloke/Llama-2-7B-GGUF",
			wantBase: ".downloading-llama-2-7b.Q4_K_M.gguf",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TempFilePath(tt.repoID, tt.filename, dir)
			wantFull := filepath.Join(dir, tt.wantDir, tt.wantBase)
			require.Equal(t, wantFull, got)
		})
	}
}

func TestTempFilePath_MatchesDownloadDest(t *testing.T) {
	dir := t.TempDir()
	repoID := "unsloth/Qwen3.5-4B-GGUF"
	filename := "model.gguf"

	// The temp file should be in the same directory as the final download dest.
	tempPath := TempFilePath(repoID, filename, dir)
	destPath := filepath.Join(dir, SanitizeRepoID(repoID), filename)

	require.Equal(t, filepath.Dir(destPath), filepath.Dir(tempPath),
		"temp file and dest file should be in the same directory")
	require.Equal(t, ".downloading-"+filename, filepath.Base(tempPath))
}
