package store

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBenchmarks(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)

	row := BenchmarkRow{
		WorkerID:   "local",
		DeviceID:   "cpu:0",
		DeviceName: "12-core x86_64/linux",
		MemoryGBs:  25.5,
		LoadGBs:    8.4,
		Flops:      42.3,
		BenchedAt:  now,
	}

	tests := []struct {
		name string
		run  func(t *testing.T, s *Store)
	}{
		{
			name: "get missing returns ErrNoRows",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				_, err := s.GetBenchmark("local", "cpu:0")
				require.ErrorIs(t, err, sql.ErrNoRows)
			},
		},
		{
			name: "has returns false for missing",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				ok, err := s.HasBenchmark("local", "cpu:0")
				require.NoError(t, err)
				require.False(t, ok)
			},
		},
		{
			name: "save and get",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SaveBenchmark(row))

				got, err := s.GetBenchmark("local", "cpu:0")
				require.NoError(t, err)
				require.Equal(t, row.WorkerID, got.WorkerID)
				require.Equal(t, row.DeviceID, got.DeviceID)
				require.Equal(t, row.DeviceName, got.DeviceName)
				require.InDelta(t, row.MemoryGBs, got.MemoryGBs, 0.01)
				require.InDelta(t, row.LoadGBs, got.LoadGBs, 0.01)
				require.InDelta(t, row.Flops, got.Flops, 0.01)
			},
		},
		{
			name: "save upserts on conflict",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SaveBenchmark(row))

				updated := row
				updated.MemoryGBs = 30.0
				updated.LoadGBs = 10.0
				updated.Flops = 50.0
				require.NoError(t, s.SaveBenchmark(updated))

				got, err := s.GetBenchmark("local", "cpu:0")
				require.NoError(t, err)
				require.InDelta(t, 30.0, got.MemoryGBs, 0.01)
				require.InDelta(t, 10.0, got.LoadGBs, 0.01)
				require.InDelta(t, 50.0, got.Flops, 0.01)
			},
		},
		{
			name: "has returns true after save",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SaveBenchmark(row))

				ok, err := s.HasBenchmark("local", "cpu:0")
				require.NoError(t, err)
				require.True(t, ok)
			},
		},
		{
			name: "same device different agents are separate",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SaveBenchmark(row))

				remoteRow := BenchmarkRow{
					WorkerID:   "remote-1",
					DeviceID:   "cpu:0",
					DeviceName: "8-core arm64/linux",
					MemoryGBs:  15.0,
					Flops:      20.0,
					BenchedAt:  now,
				}
				require.NoError(t, s.SaveBenchmark(remoteRow))

				local, err := s.GetBenchmark("local", "cpu:0")
				require.NoError(t, err)
				require.InDelta(t, 25.5, local.MemoryGBs, 0.01)

				remote, err := s.GetBenchmark("remote-1", "cpu:0")
				require.NoError(t, err)
				require.InDelta(t, 15.0, remote.MemoryGBs, 0.01)
			},
		},
		{
			name: "list returns all rows",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SaveBenchmark(row))

				row2 := BenchmarkRow{
					WorkerID:   "local",
					DeviceID:   "gpu:0",
					DeviceName: "NVIDIA RTX 4090",
					MemoryGBs:  1008.0,
					Flops:      330.0,
					BenchedAt:  now,
				}
				require.NoError(t, s.SaveBenchmark(row2))

				rows, err := s.ListBenchmarks()
				require.NoError(t, err)
				require.Len(t, rows, 2)
				require.Equal(t, "cpu:0", rows[0].DeviceID)
				require.Equal(t, "gpu:0", rows[1].DeviceID)
			},
		},
		{
			name: "delete removes row",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SaveBenchmark(row))
				require.NoError(t, s.DeleteBenchmark("local", "cpu:0"))

				ok, err := s.HasBenchmark("local", "cpu:0")
				require.NoError(t, err)
				require.False(t, ok)
			},
		},
		{
			name: "delete nonexistent is no-op",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.DeleteBenchmark("local", "nope"))
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
