// Package storetest provides reusable contract tests for store.StoreInterface
// implementations. Any storage provider can run these tests to validate
// conformance with the expected behavior.
package storetest

import (
	"database/sql"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/model"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/stretchr/testify/require"
)

// Factory creates a fresh StoreInterface for each test.
type Factory func(t *testing.T) store.StoreInterface

// RunAll runs the full contract test suite against the given factory.
func RunAll(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("Settings", func(t *testing.T) { RunSettings(t, factory) })
	t.Run("Downloads", func(t *testing.T) { RunDownloads(t, factory) })
	t.Run("Benchmarks", func(t *testing.T) { RunBenchmarks(t, factory) })
	t.Run("DeviceQueueState", func(t *testing.T) { RunDeviceQueueState(t, factory) })
}

// RunSettings validates SettingsStoreInterface behavior.
func RunSettings(t *testing.T, factory Factory) {
	t.Helper()

	tests := []struct {
		name string
		run  func(t *testing.T, s store.SettingsStoreInterface)
	}{
		{
			name: "get missing key returns empty",
			run: func(t *testing.T, s store.SettingsStoreInterface) { //nolint:thelper
				val, err := s.GetSetting("nonexistent")
				require.NoError(t, err)
				require.Equal(t, "", val)
			},
		},
		{
			name: "set and get",
			run: func(t *testing.T, s store.SettingsStoreInterface) { //nolint:thelper
				require.NoError(t, s.SetSetting("key1", "value1"))
				val, err := s.GetSetting("key1")
				require.NoError(t, err)
				require.Equal(t, "value1", val)
			},
		},
		{
			name: "upsert overwrites",
			run: func(t *testing.T, s store.SettingsStoreInterface) { //nolint:thelper
				require.NoError(t, s.SetSetting("key1", "v1"))
				require.NoError(t, s.SetSetting("key1", "v2"))
				val, err := s.GetSetting("key1")
				require.NoError(t, err)
				require.Equal(t, "v2", val)
			},
		},
		{
			name: "delete existing",
			run: func(t *testing.T, s store.SettingsStoreInterface) { //nolint:thelper
				require.NoError(t, s.SetSetting("key1", "value1"))
				require.NoError(t, s.DeleteSetting("key1"))
				val, err := s.GetSetting("key1")
				require.NoError(t, err)
				require.Equal(t, "", val)
			},
		},
		{
			name: "delete nonexistent is no-op",
			run: func(t *testing.T, s store.SettingsStoreInterface) { //nolint:thelper
				require.NoError(t, s.DeleteSetting("nope"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := factory(t)
			tt.run(t, s)
		})
	}
}

// RunDownloads validates DownloadStoreInterface behavior.
func RunDownloads(t *testing.T, factory Factory) {
	t.Helper()

	dl := model.Download{
		Filename:   "model.gguf",
		RepoID:     "owner/repo",
		GroupName:  "Test Model",
		Status:     "active",
		Downloaded: 0,
		Total:      1000,
	}

	tests := []struct {
		name string
		run  func(t *testing.T, s store.DownloadStoreInterface)
	}{
		{
			name: "list empty returns nil",
			run: func(t *testing.T, s store.DownloadStoreInterface) { //nolint:thelper
				rows, err := s.ListDownloads()
				require.NoError(t, err)
				require.Empty(t, rows)
			},
		},
		{
			name: "upsert and list",
			run: func(t *testing.T, s store.DownloadStoreInterface) { //nolint:thelper
				require.NoError(t, s.UpsertDownload(dl))
				rows, err := s.ListDownloads()
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.Equal(t, dl.Filename, rows[0].Filename)
				require.Equal(t, dl.RepoID, rows[0].RepoID)
				require.Equal(t, dl.Status, rows[0].Status)
			},
		},
		{
			name: "upsert overwrites",
			run: func(t *testing.T, s store.DownloadStoreInterface) { //nolint:thelper
				require.NoError(t, s.UpsertDownload(dl))
				updated := dl
				updated.Downloaded = 500
				require.NoError(t, s.UpsertDownload(updated))
				rows, err := s.ListDownloads()
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.Equal(t, int64(500), rows[0].Downloaded)
			},
		},
		{
			name: "update progress",
			run: func(t *testing.T, s store.DownloadStoreInterface) { //nolint:thelper
				require.NoError(t, s.UpsertDownload(dl))
				require.NoError(t, s.UpdateProgress("model.gguf", 750, 1000))
				rows, err := s.ListDownloads()
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.Equal(t, int64(750), rows[0].Downloaded)
			},
		},
		{
			name: "set status",
			run: func(t *testing.T, s store.DownloadStoreInterface) { //nolint:thelper
				require.NoError(t, s.UpsertDownload(dl))
				require.NoError(t, s.SetStatus("model.gguf", "paused"))
				rows, err := s.ListDownloads()
				require.NoError(t, err)
				require.Equal(t, "paused", rows[0].Status)
			},
		},
		{
			name: "delete",
			run: func(t *testing.T, s store.DownloadStoreInterface) { //nolint:thelper
				require.NoError(t, s.UpsertDownload(dl))
				require.NoError(t, s.DeleteDownload("model.gguf"))
				rows, err := s.ListDownloads()
				require.NoError(t, err)
				require.Empty(t, rows)
			},
		},
		{
			name: "delete nonexistent is no-op",
			run: func(t *testing.T, s store.DownloadStoreInterface) { //nolint:thelper
				require.NoError(t, s.DeleteDownload("nope"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := factory(t)
			tt.run(t, s)
		})
	}
}

// RunBenchmarks validates BenchmarkStoreInterface behavior.
func RunBenchmarks(t *testing.T, factory Factory) {
	t.Helper()

	now := time.Now().UTC().Truncate(time.Millisecond)
	row := store.BenchmarkRow{
		AgentID:       "local",
		DeviceID:      "cpu:0",
		DeviceName:    "12-core x86_64/linux",
		MemoryGBs:     25.5,
		ComputeGFlops: 42.3,
		BenchedAt:     now,
	}

	tests := []struct {
		name string
		run  func(t *testing.T, s store.BenchmarkStoreInterface)
	}{
		{
			name: "get missing returns error",
			run: func(t *testing.T, s store.BenchmarkStoreInterface) { //nolint:thelper
				_, err := s.GetBenchmark("local", "cpu:0")
				require.ErrorIs(t, err, sql.ErrNoRows)
			},
		},
		{
			name: "has returns false for missing",
			run: func(t *testing.T, s store.BenchmarkStoreInterface) { //nolint:thelper
				ok, err := s.HasBenchmark("local", "cpu:0")
				require.NoError(t, err)
				require.False(t, ok)
			},
		},
		{
			name: "save and get",
			run: func(t *testing.T, s store.BenchmarkStoreInterface) { //nolint:thelper
				require.NoError(t, s.SaveBenchmark(row))
				got, err := s.GetBenchmark("local", "cpu:0")
				require.NoError(t, err)
				require.Equal(t, row.AgentID, got.AgentID)
				require.Equal(t, row.DeviceID, got.DeviceID)
				require.Equal(t, row.DeviceName, got.DeviceName)
				require.InDelta(t, row.MemoryGBs, got.MemoryGBs, 0.01)
				require.InDelta(t, row.ComputeGFlops, got.ComputeGFlops, 0.01)
			},
		},
		{
			name: "save upserts on conflict",
			run: func(t *testing.T, s store.BenchmarkStoreInterface) { //nolint:thelper
				require.NoError(t, s.SaveBenchmark(row))
				updated := row
				updated.MemoryGBs = 30.0
				require.NoError(t, s.SaveBenchmark(updated))
				got, err := s.GetBenchmark("local", "cpu:0")
				require.NoError(t, err)
				require.InDelta(t, 30.0, got.MemoryGBs, 0.01)
			},
		},
		{
			name: "has returns true after save",
			run: func(t *testing.T, s store.BenchmarkStoreInterface) { //nolint:thelper
				require.NoError(t, s.SaveBenchmark(row))
				ok, err := s.HasBenchmark("local", "cpu:0")
				require.NoError(t, err)
				require.True(t, ok)
			},
		},
		{
			name: "different agents are separate",
			run: func(t *testing.T, s store.BenchmarkStoreInterface) { //nolint:thelper
				require.NoError(t, s.SaveBenchmark(row))
				remote := store.BenchmarkRow{
					AgentID:       "remote-1",
					DeviceID:      "cpu:0",
					DeviceName:    "8-core arm64/linux",
					MemoryGBs:     15.0,
					ComputeGFlops: 20.0,
					BenchedAt:     now,
				}
				require.NoError(t, s.SaveBenchmark(remote))

				local, err := s.GetBenchmark("local", "cpu:0")
				require.NoError(t, err)
				require.InDelta(t, 25.5, local.MemoryGBs, 0.01)

				got, err := s.GetBenchmark("remote-1", "cpu:0")
				require.NoError(t, err)
				require.InDelta(t, 15.0, got.MemoryGBs, 0.01)
			},
		},
		{
			name: "list returns all rows",
			run: func(t *testing.T, s store.BenchmarkStoreInterface) { //nolint:thelper
				require.NoError(t, s.SaveBenchmark(row))
				row2 := store.BenchmarkRow{
					AgentID:       "local",
					DeviceID:      "gpu:0",
					DeviceName:    "NVIDIA RTX 4090",
					MemoryGBs:     1008.0,
					ComputeGFlops: 330.0,
					BenchedAt:     now,
				}
				require.NoError(t, s.SaveBenchmark(row2))
				rows, err := s.ListBenchmarks()
				require.NoError(t, err)
				require.Len(t, rows, 2)
			},
		},
		{
			name: "delete removes row",
			run: func(t *testing.T, s store.BenchmarkStoreInterface) { //nolint:thelper
				require.NoError(t, s.SaveBenchmark(row))
				require.NoError(t, s.DeleteBenchmark("local", "cpu:0"))
				ok, err := s.HasBenchmark("local", "cpu:0")
				require.NoError(t, err)
				require.False(t, ok)
			},
		},
		{
			name: "delete nonexistent is no-op",
			run: func(t *testing.T, s store.BenchmarkStoreInterface) { //nolint:thelper
				require.NoError(t, s.DeleteBenchmark("local", "nope"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := factory(t)
			tt.run(t, s)
		})
	}
}

// RunDeviceQueueState validates DeviceQueueStateStoreInterface behavior.
func RunDeviceQueueState(t *testing.T, factory Factory) {
	t.Helper()

	state := store.DeviceQueueState{
		QueueName:  "device:local:gpu:0",
		AgentID:    "local",
		DeviceIDs:  []string{"gpu:0"},
		TailHash:   "abc123",
		TailLength: 5,
		LoadedHash: "def456",
	}

	tests := []struct {
		name string
		run  func(t *testing.T, s store.DeviceQueueStateStoreInterface)
	}{
		{
			name: "get missing returns error",
			run: func(t *testing.T, s store.DeviceQueueStateStoreInterface) { //nolint:thelper
				_, err := s.GetDeviceQueueState("nonexistent")
				require.ErrorIs(t, err, sql.ErrNoRows)
			},
		},
		{
			name: "upsert and get",
			run: func(t *testing.T, s store.DeviceQueueStateStoreInterface) { //nolint:thelper
				require.NoError(t, s.UpsertDeviceQueueState(state))
				got, err := s.GetDeviceQueueState("device:local:gpu:0")
				require.NoError(t, err)
				require.Equal(t, "device:local:gpu:0", got.QueueName)
				require.Equal(t, "local", got.AgentID)
				require.Equal(t, []string{"gpu:0"}, got.DeviceIDs)
				require.Equal(t, "abc123", got.TailHash)
				require.Equal(t, 5, got.TailLength)
				require.Equal(t, "def456", got.LoadedHash)
			},
		},
		{
			name: "upsert replaces",
			run: func(t *testing.T, s store.DeviceQueueStateStoreInterface) { //nolint:thelper
				require.NoError(t, s.UpsertDeviceQueueState(state))
				updated := state
				updated.TailHash = "newHash"
				updated.TailLength = 10
				require.NoError(t, s.UpsertDeviceQueueState(updated))
				got, err := s.GetDeviceQueueState("device:local:gpu:0")
				require.NoError(t, err)
				require.Equal(t, "newHash", got.TailHash)
				require.Equal(t, 10, got.TailLength)
			},
		},
		{
			name: "multi-device IDs",
			run: func(t *testing.T, s store.DeviceQueueStateStoreInterface) { //nolint:thelper
				multi := store.DeviceQueueState{
					QueueName: "device:local:gpu:0+gpu:1",
					AgentID:   "local",
					DeviceIDs: []string{"gpu:0", "gpu:1"},
				}
				require.NoError(t, s.UpsertDeviceQueueState(multi))
				got, err := s.GetDeviceQueueState("device:local:gpu:0+gpu:1")
				require.NoError(t, err)
				require.Equal(t, []string{"gpu:0", "gpu:1"}, got.DeviceIDs)
			},
		},
		{
			name: "list returns all",
			run: func(t *testing.T, s store.DeviceQueueStateStoreInterface) { //nolint:thelper
				s1 := state
				s2 := store.DeviceQueueState{QueueName: "device:local:cpu:0", AgentID: "local", DeviceIDs: []string{"cpu:0"}}
				require.NoError(t, s.UpsertDeviceQueueState(s1))
				require.NoError(t, s.UpsertDeviceQueueState(s2))
				all, err := s.ListDeviceQueueStates()
				require.NoError(t, err)
				require.Len(t, all, 2)
			},
		},
		{
			name: "delete removes entry",
			run: func(t *testing.T, s store.DeviceQueueStateStoreInterface) { //nolint:thelper
				require.NoError(t, s.UpsertDeviceQueueState(state))
				require.NoError(t, s.DeleteDeviceQueueState("device:local:gpu:0"))
				_, err := s.GetDeviceQueueState("device:local:gpu:0")
				require.ErrorIs(t, err, sql.ErrNoRows)
			},
		},
		{
			name: "update tail",
			run: func(t *testing.T, s store.DeviceQueueStateStoreInterface) { //nolint:thelper
				require.NoError(t, s.UpsertDeviceQueueState(state))
				require.NoError(t, s.UpdateTail("device:local:gpu:0", "newTail", 99))
				got, err := s.GetDeviceQueueState("device:local:gpu:0")
				require.NoError(t, err)
				require.Equal(t, "newTail", got.TailHash)
				require.Equal(t, 99, got.TailLength)
			},
		},
		{
			name: "update loaded hash",
			run: func(t *testing.T, s store.DeviceQueueStateStoreInterface) { //nolint:thelper
				require.NoError(t, s.UpsertDeviceQueueState(state))
				require.NoError(t, s.UpdateLoadedHash("device:local:gpu:0", "loaded999"))
				got, err := s.GetDeviceQueueState("device:local:gpu:0")
				require.NoError(t, err)
				require.Equal(t, "loaded999", got.LoadedHash)
			},
		},
		{
			name: "new queue defaults to enabled",
			run: func(t *testing.T, s store.DeviceQueueStateStoreInterface) { //nolint:thelper
				require.NoError(t, s.UpsertDeviceQueueState(state))
				got, err := s.GetDeviceQueueState("device:local:gpu:0")
				require.NoError(t, err)
				require.True(t, got.Enabled)
			},
		},
		{
			name: "set enabled false and back",
			run: func(t *testing.T, s store.DeviceQueueStateStoreInterface) { //nolint:thelper
				require.NoError(t, s.UpsertDeviceQueueState(state))
				require.NoError(t, s.SetEnabled("device:local:gpu:0", false))
				got, err := s.GetDeviceQueueState("device:local:gpu:0")
				require.NoError(t, err)
				require.False(t, got.Enabled)

				require.NoError(t, s.SetEnabled("device:local:gpu:0", true))
				got, err = s.GetDeviceQueueState("device:local:gpu:0")
				require.NoError(t, err)
				require.True(t, got.Enabled)
			},
		},
		{
			name: "upsert preserves enabled state",
			run: func(t *testing.T, s store.DeviceQueueStateStoreInterface) { //nolint:thelper
				require.NoError(t, s.UpsertDeviceQueueState(state))
				require.NoError(t, s.SetEnabled("device:local:gpu:0", false))

				// Upsert again (simulates agent reconnect) — should NOT reset enabled.
				require.NoError(t, s.UpsertDeviceQueueState(state))
				got, err := s.GetDeviceQueueState("device:local:gpu:0")
				require.NoError(t, err)
				require.False(t, got.Enabled, "upsert must preserve enabled=false")
			},
		},
		{
			name: "list includes enabled field",
			run: func(t *testing.T, s store.DeviceQueueStateStoreInterface) { //nolint:thelper
				require.NoError(t, s.UpsertDeviceQueueState(state))
				s2 := store.DeviceQueueState{QueueName: "device:local:cpu:0", AgentID: "local", DeviceIDs: []string{"cpu:0"}}
				require.NoError(t, s.UpsertDeviceQueueState(s2))
				require.NoError(t, s.SetEnabled("device:local:cpu:0", false))

				all, err := s.ListDeviceQueueStates()
				require.NoError(t, err)
				require.Len(t, all, 2)
				for _, st := range all {
					if st.QueueName == "device:local:cpu:0" {
						require.False(t, st.Enabled)
					} else {
						require.True(t, st.Enabled)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := factory(t)
			tt.run(t, s)
		})
	}
}
