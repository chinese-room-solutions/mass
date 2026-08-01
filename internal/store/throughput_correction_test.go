package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestThroughputCorrections(t *testing.T) {
	row := ThroughputCorrection{WorkerID: "w1", Axis: "q4k_matvec", Factor: 1.8, Samples: 7}

	tests := []struct {
		name string
		run  func(t *testing.T, s *Store)
	}{
		{
			name: "list on empty returns nothing",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				rows, err := s.ListThroughputCorrections()
				require.NoError(t, err)
				require.Empty(t, rows)
			},
		},
		{
			name: "upsert and list round-trip",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.UpsertThroughputCorrection(row))

				rows, err := s.ListThroughputCorrections()
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.Equal(t, "w1", rows[0].WorkerID)
				require.Equal(t, "q4k_matvec", rows[0].Axis)
				require.InDelta(t, 1.8, rows[0].Factor, 1e-9)
				require.Equal(t, 7, rows[0].Samples)
				require.WithinDuration(t, time.Now(), rows[0].UpdatedAt, time.Minute)
			},
		},
		{
			name: "upsert replaces on conflict",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.UpsertThroughputCorrection(row))

				updated := row
				updated.Factor = 2.2
				updated.Samples = 8
				require.NoError(t, s.UpsertThroughputCorrection(updated))

				rows, err := s.ListThroughputCorrections()
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.InDelta(t, 2.2, rows[0].Factor, 1e-9)
				require.Equal(t, 8, rows[0].Samples)
			},
		},
		{
			name: "axes are separate rows",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.UpsertThroughputCorrection(row))
				other := row
				other.Axis = "f16_matvec"
				other.Factor = 0.5
				require.NoError(t, s.UpsertThroughputCorrection(other))

				rows, err := s.ListThroughputCorrections()
				require.NoError(t, err)
				require.Len(t, rows, 2)
			},
		},
		{
			name: "delete removes only the worker's rows",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.UpsertThroughputCorrection(row))
				other := row
				other.WorkerID = "w2"
				require.NoError(t, s.UpsertThroughputCorrection(other))

				require.NoError(t, s.DeleteThroughputCorrections("w1"))

				rows, err := s.ListThroughputCorrections()
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.Equal(t, "w2", rows[0].WorkerID)
			},
		},
		{
			name: "delete nonexistent is no-op",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.DeleteThroughputCorrections("nope"))
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
