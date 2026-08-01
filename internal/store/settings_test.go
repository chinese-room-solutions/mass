package store

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(DialectSQLite, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSettings(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, s *Store)
	}{
		{
			name: "get missing key returns empty",
			run: func(t *testing.T, s *Store) {
				t.Helper()
				val, err := s.GetSetting("nonexistent")
				require.NoError(t, err)
				require.Equal(t, "", val)
			},
		},
		{
			name: "set and get",
			run: func(t *testing.T, s *Store) {
				t.Helper()
				require.NoError(t, s.SetSetting("key1", "value1"))

				val, err := s.GetSetting("key1")
				require.NoError(t, err)
				require.Equal(t, "value1", val)
			},
		},
		{
			name: "upsert overwrites",
			run: func(t *testing.T, s *Store) {
				t.Helper()
				require.NoError(t, s.SetSetting("key1", "v1"))
				require.NoError(t, s.SetSetting("key1", "v2"))

				val, err := s.GetSetting("key1")
				require.NoError(t, err)
				require.Equal(t, "v2", val)
			},
		},
		{
			name: "delete existing",
			run: func(t *testing.T, s *Store) {
				t.Helper()
				require.NoError(t, s.SetSetting("key1", "value1"))
				require.NoError(t, s.DeleteSetting("key1"))

				val, err := s.GetSetting("key1")
				require.NoError(t, err)
				require.Equal(t, "", val)
			},
		},
		{
			name: "delete nonexistent is no-op",
			run: func(t *testing.T, s *Store) {
				t.Helper()
				require.NoError(t, s.DeleteSetting("nope"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestDB(t)
			tt.run(t, s)
		})
	}
}
