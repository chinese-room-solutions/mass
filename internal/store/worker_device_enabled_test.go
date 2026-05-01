package store

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerDeviceEnabled(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, s *Store)
	}{
		{
			name: "get missing returns ErrNoRows",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				_, err := s.GetWorkerDeviceEnabled("worker-A", "gpu:0")
				require.ErrorIs(t, err, sql.ErrNoRows)
			},
		},
		{
			name: "set then get round-trips",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SetWorkerDeviceEnabled("worker-A", "gpu:0", false))

				got, err := s.GetWorkerDeviceEnabled("worker-A", "gpu:0")
				require.NoError(t, err)
				require.Equal(t, "worker-A", got.WorkerID)
				require.Equal(t, "gpu:0", got.DeviceID)
				require.False(t, got.Enabled)
				require.False(t, got.UpdatedAt.IsZero())
			},
		},
		{
			name: "set upserts on conflict",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SetWorkerDeviceEnabled("worker-A", "gpu:0", false))
				require.NoError(t, s.SetWorkerDeviceEnabled("worker-A", "gpu:0", true))

				got, err := s.GetWorkerDeviceEnabled("worker-A", "gpu:0")
				require.NoError(t, err)
				require.True(t, got.Enabled)
			},
		},
		{
			name: "list scopes by worker_id",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SetWorkerDeviceEnabled("worker-A", "cpu:0", true))
				require.NoError(t, s.SetWorkerDeviceEnabled("worker-A", "gpu:0", false))
				require.NoError(t, s.SetWorkerDeviceEnabled("worker-A", "gpu:1", true))
				require.NoError(t, s.SetWorkerDeviceEnabled("worker-B", "gpu:0", false))

				rows, err := s.ListWorkerDevicesEnabled("worker-A")
				require.NoError(t, err)
				require.Len(t, rows, 3)
				// Ordered by device_id ascending.
				require.Equal(t, "cpu:0", rows[0].DeviceID)
				require.Equal(t, "gpu:0", rows[1].DeviceID)
				require.Equal(t, "gpu:1", rows[2].DeviceID)

				rowsB, err := s.ListWorkerDevicesEnabled("worker-B")
				require.NoError(t, err)
				require.Len(t, rowsB, 1)
				require.Equal(t, "worker-B", rowsB[0].WorkerID)
			},
		},
		{
			name: "bulk set is atomic and applies to listed devices",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SetWorkerDevicesEnabledBulk(
					"worker-A", []string{"cpu:0", "gpu:0", "gpu:1"}, false))

				rows, err := s.ListWorkerDevicesEnabled("worker-A")
				require.NoError(t, err)
				require.Len(t, rows, 3)
				for _, r := range rows {
					require.False(t, r.Enabled, "device %s should be disabled", r.DeviceID)
				}
			},
		},
		{
			name: "bulk set with empty list is no-op",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				require.NoError(t, s.SetWorkerDevicesEnabledBulk("worker-A", nil, false))
				rows, err := s.ListWorkerDevicesEnabled("worker-A")
				require.NoError(t, err)
				require.Empty(t, rows)
			},
		},
		{
			name: "list returns empty slice for unknown worker",
			run: func(t *testing.T, s *Store) { //nolint:thelper
				rows, err := s.ListWorkerDevicesEnabled("never-seen")
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
