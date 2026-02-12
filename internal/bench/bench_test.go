package bench_test

import (
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/pkg/bench"
	"github.com/stretchr/testify/require"
)

func TestRunCPU(t *testing.T) {
	result := bench.RunCPU()
	require.Equal(t, "cpu:0", result.DeviceID)
	require.NotEmpty(t, result.DeviceName)
	require.False(t, result.BenchedAt.IsZero())
}

func TestCPUInfo(t *testing.T) {
	dev := bench.CPUInfo()
	require.Equal(t, "cpu:0", dev.ID)
	require.Equal(t, "CPU", dev.Type)
	require.NotEmpty(t, dev.Name)
	require.Greater(t, dev.TotalMemoryMB, 0)

	t.Logf("CPU device: %s, %d MB RAM", dev.Name, dev.TotalMemoryMB)
}

// mockBencher implements BencherInterface for testing RunAll.
type mockBencher struct{}

func (m *mockBencher) Devices() []bench.Device {
	return []bench.Device{
		{ID: "cpu:0", Name: "test-cpu", Type: "CPU", TotalMemoryMB: 16384},
		{ID: "gpu:0", Name: "test-gpu", Type: "GPU", TotalMemoryMB: 8192},
	}
}

func (m *mockBencher) Bench(deviceID string) (bench.Result, error) {
	return bench.Result{
		DeviceID:      deviceID,
		DeviceName:    "mock-" + deviceID,
		MemoryGBs:     10.0,
		ComputeGFlops: 5.0,
		BenchedAt:     time.Now(),
	}, nil
}

func TestRunAll(t *testing.T) {
	results, err := bench.RunAll(&mockBencher{})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, "cpu:0", results[0].DeviceID)
	require.Equal(t, "gpu:0", results[1].DeviceID)
}
