package downloads

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// RemoveLocal removes a store entry whether it names a single file or a whole
// directory subtree (directory-shaped models), and never escapes modelsDir.
func TestManager_RemoveLocal(t *testing.T) {
	modelsDir := filepath.Join(t.TempDir(), "models")
	m := NewManager(fakeDownloadStore{}, modelsDir, zerolog.Nop())

	// A single-file model.
	filePath := filepath.Join(modelsDir, "gguf", "model.gguf")
	require.NoError(t, os.MkdirAll(filepath.Dir(filePath), 0o755))
	require.NoError(t, os.WriteFile(filePath, []byte("weights"), 0o644))

	// A directory-shaped model with nested files.
	dirModel := filepath.Join(modelsDir, "onnx", "whisper")
	require.NoError(t, os.MkdirAll(filepath.Join(dirModel, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dirModel, "model.onnx"), []byte("m"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirModel, "sub", "tokens.txt"), []byte("t"), 0o644))

	t.Run("removes a single file", func(t *testing.T) {
		require.NoError(t, m.RemoveLocal([]string{"gguf/model.gguf"}))
		_, err := os.Stat(filePath)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("removes a directory subtree wholesale", func(t *testing.T) {
		require.NoError(t, m.RemoveLocal([]string{"onnx/whisper"}))
		_, err := os.Stat(dirModel)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("missing entry is not an error", func(t *testing.T) {
		require.NoError(t, m.RemoveLocal([]string{"gguf/gone.gguf"}))
	})

	t.Run("traversal is still rejected before any removal", func(t *testing.T) {
		outside := filepath.Join(t.TempDir(), "victim")
		require.NoError(t, os.WriteFile(outside, []byte("keep"), 0o644))
		rel, err := filepath.Rel(modelsDir, outside)
		require.NoError(t, err)
		err = m.RemoveLocal([]string{filepath.ToSlash(rel)})
		require.ErrorIs(t, err, ErrInvalidRelPath)
		_, statErr := os.Stat(outside)
		require.NoError(t, statErr, "traversal target must survive")
	})
}
