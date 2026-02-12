package storetest

import (
	"path/filepath"
	"testing"

	"github.com/chinese-room-solutions/mass/internal/store"
)

func TestSQLiteStore_Contract(t *testing.T) {
	RunAll(t, func(t *testing.T) store.StoreInterface {
		t.Helper()
		dbPath := filepath.Join(t.TempDir(), "test.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}
