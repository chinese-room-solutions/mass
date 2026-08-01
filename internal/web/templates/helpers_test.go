package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// mismatchedGPUAdvisory must surface a warning only when >=2 enabled,
// benched GPUs differ in compute_gflops by at least mismatchedGPURatio
// (currently 2×). Operator-disabled or unbenched GPUs are ignored.
// CPUs never count.
func TestMismatchedGPUAdvisory(t *testing.T) {
	tests := []struct {
		name        string
		devices     []ComputeView
		wantWarn    bool
		wantContain string // substring that must appear when wantWarn is true
	}{
		{
			name: "discrete + iGPU triggers (36x ratio)",
			devices: []ComputeView{
				{Type: "GPU", DeviceName: "RTX 3070 Ti", Enabled: true, HasBenchmark: true, ComputeGFlops: 1800},
				{Type: "GPU", DeviceName: "AMD Radeon", Enabled: true, HasBenchmark: true, ComputeGFlops: 50},
			},
			wantWarn:    true,
			wantContain: "RTX 3070 Ti",
		},
		{
			name: "tightly-balanced dual-GPU does not trigger (1.5x ratio)",
			devices: []ComputeView{
				{Type: "GPU", DeviceName: "RTX 3090", Enabled: true, HasBenchmark: true, ComputeGFlops: 1800},
				{Type: "GPU", DeviceName: "RTX 3080", Enabled: true, HasBenchmark: true, ComputeGFlops: 1200},
			},
			wantWarn: false,
		},
		{
			name: "operator-disabled slow GPU does not trigger",
			devices: []ComputeView{
				{Type: "GPU", DeviceName: "RTX 3070", Enabled: true, HasBenchmark: true, ComputeGFlops: 1800},
				{Type: "GPU", DeviceName: "AMD Radeon", Enabled: false, HasBenchmark: true, ComputeGFlops: 50},
			},
			wantWarn: false,
		},
		{
			name: "unbenched slow GPU does not trigger (no truth source for ratio)",
			devices: []ComputeView{
				{Type: "GPU", DeviceName: "RTX 3070", Enabled: true, HasBenchmark: true, ComputeGFlops: 1800},
				{Type: "GPU", DeviceName: "AMD Radeon", Enabled: true, HasBenchmark: false},
			},
			wantWarn: false,
		},
		{
			name: "single-GPU worker does not trigger",
			devices: []ComputeView{
				{Type: "GPU", DeviceName: "RTX 3070", Enabled: true, HasBenchmark: true, ComputeGFlops: 1800},
			},
			wantWarn: false,
		},
		{
			name: "GPU + CPU does not trigger (CPU never counts toward the GPU split)",
			devices: []ComputeView{
				{Type: "GPU", DeviceName: "RTX 3070", Enabled: true, HasBenchmark: true, ComputeGFlops: 1800},
				{Type: "CPU", DeviceName: "Ryzen 9", Enabled: true, HasBenchmark: true, ComputeGFlops: 80},
			},
			wantWarn: false,
		},
		{
			name: "exactly at threshold (2x) triggers (ratio >= threshold)",
			devices: []ComputeView{
				{Type: "GPU", DeviceName: "Fast", Enabled: true, HasBenchmark: true, ComputeGFlops: 200},
				{Type: "GPU", DeviceName: "Slow", Enabled: true, HasBenchmark: true, ComputeGFlops: 100},
			},
			wantWarn:    true,
			wantContain: "Slow",
		},
		{
			name: "just under threshold (1.9x) does not trigger",
			devices: []ComputeView{
				{Type: "GPU", DeviceName: "Fast", Enabled: true, HasBenchmark: true, ComputeGFlops: 190},
				{Type: "GPU", DeviceName: "Slow", Enabled: true, HasBenchmark: true, ComputeGFlops: 100},
			},
			wantWarn: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mismatchedGPUAdvisory(tt.devices)
			if !tt.wantWarn {
				require.Empty(t, got)
				return
			}
			require.NotEmpty(t, got)
			require.Contains(t, got, tt.wantContain)
			require.True(t, strings.Contains(got, "Mismatched GPUs"),
				"advisory must announce the mismatch in runtime-agnostic terms")
			require.False(t, strings.Contains(got, "llama"),
				"advisory must not name any specific runtime — MASS is runtime-agnostic")
		})
	}
}
