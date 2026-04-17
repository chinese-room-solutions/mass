package web

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeStub(t *testing.T, root, rel string) {
	t.Helper()
	abs := filepath.Join(root, filepath.FromSlash(rel))
	require.NoError(t, os.MkdirAll(filepath.Dir(abs), 0o755))
	require.NoError(t, os.WriteFile(abs, []byte("x"), 0o600))
}

func TestCanonicalModelFiles(t *testing.T) {
	dir := t.TempDir()
	writeStub(t, dir, "publisher/repo/model.gguf")
	writeStub(t, dir, "publisher/repo/mmproj.gguf")
	writeStub(t, dir, "other/embed.gguf")
	// Excluded: non-GGUF, in-progress download.
	writeStub(t, dir, "publisher/repo/README.md")
	writeStub(t, dir, "publisher/repo/.downloading-temp.gguf")

	got := CanonicalModelFiles(dir)
	require.ElementsMatch(t,
		[]string{"publisher/repo/model.gguf", "publisher/repo/mmproj.gguf", "other/embed.gguf"},
		keys(got),
	)
}

func TestCanonicalModelFiles_EmptyDir(t *testing.T) {
	require.Empty(t, CanonicalModelFiles(""))
	require.Empty(t, CanonicalModelFiles(filepath.Join(t.TempDir(), "missing")))
}

func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
