package store

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkers(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, s *Store)
	}{
		{
			name: "get missing returns ErrNoRows",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				_, err := s.GetWorker("nope")
				require.ErrorIs(t, err, sql.ErrNoRows)
			},
		},
		{
			name: "insert then get round-trips",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.InsertWorker(WorkerRow{
					WorkerID: "w1", Name: "alpha", SecretHash: "hash", CreatedAt: 1000,
				}))

				got, err := s.GetWorker("w1")
				require.NoError(t, err)
				require.Equal(t, "w1", got.WorkerID)
				require.Equal(t, "alpha", got.Name)
				require.Equal(t, "hash", got.SecretHash)
				require.Equal(t, int64(1000), got.CreatedAt)
			},
		},
		{
			name: "list returns newest first",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.InsertWorker(WorkerRow{WorkerID: "old", SecretHash: "h", CreatedAt: 100}))
				require.NoError(t, s.InsertWorker(WorkerRow{WorkerID: "new", SecretHash: "h", CreatedAt: 200}))

				rows, err := s.ListWorkers()
				require.NoError(t, err)
				require.Len(t, rows, 2)
				require.Equal(t, "new", rows[0].WorkerID)
				require.Equal(t, "old", rows[1].WorkerID)
			},
		},
		{
			name: "delete revokes and reports deletion",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.InsertWorker(WorkerRow{WorkerID: "w1", SecretHash: "h", CreatedAt: 1}))

				deleted, err := s.DeleteWorker("w1")
				require.NoError(t, err)
				require.True(t, deleted)

				_, err = s.GetWorker("w1")
				require.ErrorIs(t, err, sql.ErrNoRows)
			},
		},
		{
			name: "delete unknown reports no deletion",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				deleted, err := s.DeleteWorker("ghost")
				require.NoError(t, err)
				require.False(t, deleted)
			},
		},
		{
			name: "empty store lists nothing",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				rows, err := s.ListWorkers()
				require.NoError(t, err)
				require.Empty(t, rows)
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
