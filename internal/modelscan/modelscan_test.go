package modelscan

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))
}

func TestScanner_Set(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "gguf", "a.gguf"))
	writeFile(t, filepath.Join(dir, "onnx", "whisper", "model.onnx"))
	writeFile(t, filepath.Join(dir, "onnx", "whisper", "sub", "tokens.txt"))
	// In-flight download temp files must be excluded.
	writeFile(t, filepath.Join(dir, "gguf", ".downloading-b.gguf"))

	s := New(dir, time.Minute, zerolog.Nop())
	got := s.Set()

	want := map[string]struct{}{
		"gguf/a.gguf":                 {},
		"onnx/whisper/model.onnx":     {},
		"onnx/whisper/sub/tokens.txt": {},
	}
	require.Equal(t, want, got)
}

// A missing models dir (nothing installed yet) is an empty set, not an error.
func TestScanner_MissingDirEmpty(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "nope"), time.Minute, zerolog.Nop())
	require.Empty(t, s.Set())
}

// SAFETY: an unreadable store must yield an empty set so reconcile skips.
// A regular file standing in for the models dir makes WalkDir error.
func TestScanner_WalkErrorEmpty(t *testing.T) {
	f := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(f, []byte("x"), 0o644))
	s := New(f, time.Minute, zerolog.Nop())
	require.Empty(t, s.Set())
}

// The set is memoized for the TTL: a file added after the first walk is not
// seen until the TTL lapses or Invalidate is called.
func TestScanner_TTLMemoAndInvalidate(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "gguf", "a.gguf"))

	s := New(dir, time.Hour, zerolog.Nop())
	require.Len(t, s.Set(), 1)

	writeFile(t, filepath.Join(dir, "gguf", "b.gguf"))
	require.Len(t, s.Set(), 1, "TTL should serve the memoized set")

	s.Invalidate()
	require.Len(t, s.Set(), 2, "Invalidate should force a re-walk")
}
