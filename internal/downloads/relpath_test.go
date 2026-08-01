package downloads

import (
	"path/filepath"
	"testing"

	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// ValidateRelPath is the traversal gate for every gateway-supplied
// models-dir-relative path. A miss here means a hostile gateway can read,
// overwrite, or delete arbitrary host files via DownloadFiles / ImportLocal /
// RemoveLocal.
func TestValidateRelPath(t *testing.T) {
	tests := []struct {
		name string
		rel  string
		ok   bool
	}{
		{name: "canonical", rel: "gguf/owner-model-Q4_K_M.gguf", ok: true},
		{name: "single segment", rel: "model.gguf", ok: true},
		{name: "nested", rel: "a/b/c.bin", ok: true},
		{name: "empty", rel: "", ok: false},
		{name: "whitespace only", rel: "   ", ok: false},
		{name: "parent escape", rel: "../evil", ok: false},
		{name: "nested parent escape", rel: "gguf/../../evil", ok: false},
		{name: "bare dotdot", rel: "..", ok: false},
		{name: "dot segment", rel: "./gguf/m.gguf", ok: false},
		{name: "absolute posix", rel: "/etc/passwd", ok: false},
		{name: "absolute windows", rel: `C:\Windows\system32\evil`, ok: false},
		{name: "drive-relative windows", rel: "C:evil", ok: false},
		{name: "backslash traversal", rel: `..\..\evil`, ok: false},
		{name: "mixed separators", rel: `gguf\..\..\evil`, ok: false},
		{name: "double slash", rel: "gguf//m.gguf", ok: false},
		{name: "trailing slash", rel: "gguf/m.gguf/", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRelPath(tt.rel)
			if tt.ok {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrInvalidRelPath)
			}
		})
	}
}

// The manager itself must refuse hostile rel paths on every entry point,
// regardless of what the RPC layer already checked.
func TestManager_RejectsTraversalRelPath(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(fakeDownloadStore{}, filepath.Join(dir, "models"), zerolog.Nop())

	t.Run("Start", func(t *testing.T) {
		err := m.Start(Job{RelPath: "../evil.gguf", URL: "http://x/y"})
		require.ErrorIs(t, err, ErrInvalidRelPath)
	})
	t.Run("ImportLocal", func(t *testing.T) {
		_, err := m.ImportLocal(filepath.Join(dir, "src.gguf"), "../evil.gguf", "llama-cpp", LocalImportLabels{})
		require.ErrorIs(t, err, ErrInvalidRelPath)
	})
	t.Run("RemoveLocal", func(t *testing.T) {
		err := m.RemoveLocal([]string{"../evil.gguf"})
		require.ErrorIs(t, err, ErrInvalidRelPath)
	})
}

// fakeDownloadStore satisfies store.DownloadStoreInterface without a DB.
// Every method is a no-op: the traversal tests must fail before persistence.
type fakeDownloadStore struct{}

func (fakeDownloadStore) UpsertDownload(store.DownloadRow) error            { return nil }
func (fakeDownloadStore) UpdateDownloadProgress(string, int64, int64) error { return nil }
func (fakeDownloadStore) SetDownloadStatus(string, string, string) error    { return nil }
func (fakeDownloadStore) DeleteDownload(string) error                       { return nil }
func (fakeDownloadStore) GetDownload(string) (store.DownloadRow, error) {
	return store.DownloadRow{}, nil
}
func (fakeDownloadStore) ListDownloads() ([]store.DownloadRow, error) { return nil, nil }
