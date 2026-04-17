package scheduler

import (
	"testing"

	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/stretchr/testify/require"
)

func TestDefaultDeviceEnabled(t *testing.T) {
	cpu := stats.Device{ID: "cpu:0", Type: stats.DeviceTypeCPU}
	gpu0 := stats.Device{ID: "gpu:0", Type: stats.DeviceTypeGPU}
	gpu1 := stats.Device{ID: "gpu:1", Type: stats.DeviceTypeGPU}

	tests := []struct {
		name   string
		all    []stats.Device
		dev    stats.Device
		wantOn bool
	}{
		{
			name:   "GPU is always enabled",
			all:    []stats.Device{gpu0},
			dev:    gpu0,
			wantOn: true,
		},
		{
			name:   "CPU on CPU-only worker stays enabled",
			all:    []stats.Device{cpu},
			dev:    cpu,
			wantOn: true,
		},
		{
			name:   "CPU is disabled when worker also has a GPU",
			all:    []stats.Device{cpu, gpu0},
			dev:    cpu,
			wantOn: false,
		},
		{
			name:   "CPU is disabled when worker has multiple GPUs",
			all:    []stats.Device{cpu, gpu0, gpu1},
			dev:    cpu,
			wantOn: false,
		},
		{
			name:   "GPU on a worker with both stays enabled",
			all:    []stats.Device{cpu, gpu0},
			dev:    gpu0,
			wantOn: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantOn, defaultDeviceEnabled(tt.all, tt.dev))
		})
	}
}
