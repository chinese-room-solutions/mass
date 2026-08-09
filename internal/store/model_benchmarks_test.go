package store

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func modelBenchRow() ModelBenchmarkRow {
	return ModelBenchmarkRow{
		WorkerID:     "w1",
		DeviceSet:    "gpu:0",
		ModelID:      "m1",
		UnitsPerSec:  120.5,
		GraphSecs:    0.25,
		BaseBytes:    4 << 30,
		PerSlotBytes: 256 << 20,
		ModelSize:    3 << 30,
		ModelMTime:   1700000000,
	}
}

func TestModelBenchmarks(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, s *Store)
	}{
		{
			name: "get missing row returns ErrNoRows",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				_, err := s.GetModelBenchmark("w1", "gpu:0", "m1")
				require.ErrorIs(t, err, sql.ErrNoRows)
			},
		},
		{
			name: "save and get round-trips every field",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				row := modelBenchRow()
				require.NoError(t, s.SaveModelBenchmark(row))

				got, err := s.GetModelBenchmark("w1", "gpu:0", "m1")
				require.NoError(t, err)
				require.Equal(t, row.WorkerID, got.WorkerID)
				require.Equal(t, row.DeviceSet, got.DeviceSet)
				require.Equal(t, row.ModelID, got.ModelID)
				require.InDelta(t, row.UnitsPerSec, got.UnitsPerSec, 1e-9)
				require.InDelta(t, row.GraphSecs, got.GraphSecs, 1e-9)
				require.Equal(t, row.BaseBytes, got.BaseBytes)
				require.Equal(t, row.PerSlotBytes, got.PerSlotBytes)
				require.Equal(t, row.ModelSize, got.ModelSize)
				require.Equal(t, row.ModelMTime, got.ModelMTime)
				require.Empty(t, got.Error)
				require.InDelta(t, time.Now().Unix(), got.CreatedAt, 60)
				require.Equal(t, got.CreatedAt, got.UpdatedAt)
			},
		},
		{
			name: "save twice updates measurements and keeps created_at",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SaveModelBenchmark(modelBenchRow()))
				first, err := s.GetModelBenchmark("w1", "gpu:0", "m1")
				require.NoError(t, err)

				updated := modelBenchRow()
				updated.UnitsPerSec = 200
				updated.GraphSecs = 0.5
				updated.ModelMTime = 1800000000
				require.NoError(t, s.SaveModelBenchmark(updated))

				got, err := s.GetModelBenchmark("w1", "gpu:0", "m1")
				require.NoError(t, err)
				require.InDelta(t, 200.0, got.UnitsPerSec, 1e-9)
				require.InDelta(t, 0.5, got.GraphSecs, 1e-9)
				require.Equal(t, int64(1800000000), got.ModelMTime)
				require.Equal(t, first.CreatedAt, got.CreatedAt)
			},
		},
		{
			name: "incapable verdict overwrites a good row and zeroes measurements",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SaveModelBenchmark(modelBenchRow()))

				bad := modelBenchRow()
				bad.Error = "allocation failed"
				require.NoError(t, s.SaveModelBenchmarkError(bad))

				got, err := s.GetModelBenchmark("w1", "gpu:0", "m1")
				require.NoError(t, err)
				require.Equal(t, "allocation failed", got.Error)
				require.Zero(t, got.UnitsPerSec)
				require.Zero(t, got.GraphSecs)
				require.Zero(t, got.BaseBytes)
				require.Zero(t, got.PerSlotBytes)
				require.Equal(t, modelBenchRow().ModelSize, got.ModelSize)
			},
		},
		{
			name: "successful measurement clears a previous error",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				bad := modelBenchRow()
				bad.Error = "out of memory"
				require.NoError(t, s.SaveModelBenchmarkError(bad))

				require.NoError(t, s.SaveModelBenchmark(modelBenchRow()))

				got, err := s.GetModelBenchmark("w1", "gpu:0", "m1")
				require.NoError(t, err)
				require.Empty(t, got.Error)
				require.InDelta(t, 120.5, got.UnitsPerSec, 1e-9)
			},
		},
		{
			name: "incapable verdict rejects an empty error",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.Error(t, s.SaveModelBenchmarkError(modelBenchRow()))
			},
		},
		{
			name: "device sets and workers are separate rows",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SaveModelBenchmark(modelBenchRow()))

				otherSet := modelBenchRow()
				otherSet.DeviceSet = "cpu"
				require.NoError(t, s.SaveModelBenchmark(otherSet))

				otherWorker := modelBenchRow()
				otherWorker.WorkerID = "w2"
				require.NoError(t, s.SaveModelBenchmark(otherWorker))

				rows, err := s.ListModelBenchmarksByModel("m1")
				require.NoError(t, err)
				require.Len(t, rows, 3)
				require.Equal(t, "w1", rows[0].WorkerID)
				require.Equal(t, "cpu", rows[0].DeviceSet)
				require.Equal(t, "w2", rows[2].WorkerID)
			},
		},
		{
			name: "list by model ignores other models",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SaveModelBenchmark(modelBenchRow()))
				other := modelBenchRow()
				other.ModelID = "m2"
				require.NoError(t, s.SaveModelBenchmark(other))

				rows, err := s.ListModelBenchmarksByModel("m1")
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.Equal(t, "m1", rows[0].ModelID)
			},
		},
		{
			name: "list on empty returns nothing",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				rows, err := s.ListModelBenchmarksByModel("m1")
				require.NoError(t, err)
				require.Empty(t, rows)
			},
		},
		{
			name: "delete by model removes only that model's rows",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SaveModelBenchmark(modelBenchRow()))
				other := modelBenchRow()
				other.ModelID = "m2"
				require.NoError(t, s.SaveModelBenchmark(other))

				require.NoError(t, s.DeleteModelBenchmarksByModel("m1"))

				_, err := s.GetModelBenchmark("w1", "gpu:0", "m1")
				require.ErrorIs(t, err, sql.ErrNoRows)
				rows, err := s.ListModelBenchmarksByModel("m2")
				require.NoError(t, err)
				require.Len(t, rows, 1)
			},
		},
		{
			name: "delete by worker removes only that worker's rows",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SaveModelBenchmark(modelBenchRow()))
				other := modelBenchRow()
				other.WorkerID = "w2"
				require.NoError(t, s.SaveModelBenchmark(other))

				require.NoError(t, s.DeleteModelBenchmarksByWorker("w1"))

				rows, err := s.ListModelBenchmarksByModel("m1")
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.Equal(t, "w2", rows[0].WorkerID)
			},
		},
		{
			name: "deletes of absent rows are no-ops",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.DeleteModelBenchmarksByModel("nope"))
				require.NoError(t, s.DeleteModelBenchmarksByWorker("nope"))
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
