package stats_test

import (
	"testing"

	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/stretchr/testify/require"
)

func TestCPUInfo(t *testing.T) {
	dev := stats.CPUInfo()
	require.Equal(t, stats.CPUDeviceID, dev.ID)
	require.Equal(t, stats.DeviceTypeCPU, dev.Type)
	require.NotEmpty(t, dev.Name)
	require.Greater(t, dev.TotalMemoryMB, 0)

	t.Logf("CPU device: %s, %d MB RAM", dev.Name, dev.TotalMemoryMB)
}

func TestNoGPU(t *testing.T) {
	var p stats.GPUProviderInterface = stats.NoGPU{}
	require.Empty(t, p.GPUs())
	require.Empty(t, p.GPUStats())
}
