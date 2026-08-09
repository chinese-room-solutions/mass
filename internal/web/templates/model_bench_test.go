package templates

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Each row state renders its own shape: measured numbers, the incapable
// verdict with its reason, or the in-flight spinner.
func TestRenderModelBenchPanel_RowStates(t *testing.T) {
	tests := []struct {
		name        string
		view        ModelBenchView
		wantContain []string
		wantAbsent  []string
	}{
		{
			name: "measured row shows every number",
			view: ModelBenchView{
				RuntimeName: "llama-cpp",
				ModelKey:    "gguf/qwen/qwen3.gguf",
				Rows: []ModelBenchRowView{{
					WorkerName:   "host-a",
					DeviceSet:    "gpu:0",
					State:        ModelBenchMeasured,
					UnitsPerSec:  42.5,
					GraphSecs:    0.031,
					BaseBytes:    5 << 30,
					PerSlotBytes: 128 << 20,
				}},
			},
			wantContain: []string{"host-a", "gpu:0", "42.5 units/s", "0.031 s", "5.0 GiB", "128.0 MiB"},
			wantAbsent:  []string{"Incapable", "benchmarking"},
		},
		{
			name: "incapable row shows the reason instead of numbers",
			view: ModelBenchView{
				RuntimeName: "llama-cpp",
				ModelKey:    "gguf/qwen/qwen3.gguf",
				Rows: []ModelBenchRowView{{
					WorkerName: "host-b",
					DeviceSet:  "gpu:1",
					State:      ModelBenchIncapable,
					Error:      "failed to allocate KV cache",
				}},
			},
			wantContain: []string{"host-b", "gpu:1", "Incapable", "failed to allocate KV cache"},
			wantAbsent:  []string{"units/s"},
		},
		{
			name: "in-flight row shows the spinner",
			view: ModelBenchView{
				RuntimeName: "llama-cpp",
				ModelKey:    "gguf/qwen/qwen3.gguf",
				Rows: []ModelBenchRowView{{
					WorkerName: "host-c",
					State:      ModelBenchRunning,
				}},
			},
			wantContain: []string{"host-c", "sl-spinner", "benchmarking"},
			wantAbsent:  []string{"units/s", "Incapable"},
		},
		{
			name: "no rows keeps the card and its action",
			view: ModelBenchView{
				RuntimeName: "llama-cpp",
				ModelKey:    "gguf/qwen/qwen3.gguf",
			},
			wantContain: []string{"No benchmark yet"},
			wantAbsent:  []string{"units/s", "Incapable", "benchmarking"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			html := RenderModelBenchPanel(tt.view)
			require.Contains(t, html, `id="model-bench-card"`)
			// The re-bench action always posts the store key, never the
			// catalogue id — a mismatch would silently bench nothing.
			require.Contains(t, html, "/api/models/rebench?runtime=llama-cpp&amp;key=gguf%2Fqwen%2Fqwen3.gguf")
			for _, want := range tt.wantContain {
				require.Contains(t, html, want)
			}
			for _, absent := range tt.wantAbsent {
				require.NotContains(t, html, absent)
			}
		})
	}
}

// The Workers tab renders the device's single FLOPS figure (per-axis
// throughput is gone) and says when a worker is busy benching.
func TestRenderWorkersList_FlopsAndBenchingState(t *testing.T) {
	tests := []struct {
		name        string
		worker      WorkerView
		wantContain []string
		wantAbsent  []string
	}{
		{
			name: "benched GPU shows flops per device and in the summary",
			worker: WorkerView{
				ID: "w1", Name: "host-a", RuntimeName: "llama-cpp", Online: true, Enabled: true,
				Devices: []ComputeView{{
					DeviceID: "gpu:0", DeviceName: "RTX 4090", Type: "GPU", Enabled: true,
					MemoryMB: 24576, HasBenchmark: true, ComputeGFlops: 812.5, MemoryGBs: 900,
				}},
			},
			wantContain: []string{"812.5 GFLOPS", "GPU 812 GF"},
			wantAbsent:  []string{"benchmarking"},
		},
		{
			name: "unbenched device shows a placeholder, not a zero",
			worker: WorkerView{
				ID: "w2", Name: "host-b", RuntimeName: "llama-cpp", Online: true, Enabled: true,
				Devices: []ComputeView{{
					DeviceID: "gpu:0", DeviceName: "RTX 4090", Type: "GPU", Enabled: true, MemoryMB: 24576,
				}},
			},
			wantContain: []string{"No benchmark data"},
			wantAbsent:  []string{"GFLOPS", "benchmarking"},
		},
		{
			// Presence only: the Queue tab carries the live detail, this
			// list is re-fetched on tab entry.
			name: "benching worker gets a bench icon naming the model",
			worker: WorkerView{
				ID: "w3", Name: "host-c", RuntimeName: "llama-cpp", Online: true, Enabled: true,
				Devices: []ComputeView{{
					DeviceID: "gpu:0", DeviceName: "RTX 4090", Type: "GPU", Enabled: true, MemoryMB: 24576,
				}},
				BenchingModel: "gguf/qwen/qwen3.gguf",
			},
			wantContain: []string{
				`content="Benchmarking gguf/qwen/qwen3.gguf"`,
				`<sl-icon name="speedometer2"`,
			},
			wantAbsent: []string{"benchmarking qwen3.gguf"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			html := RenderWorkersList([]WorkerView{tt.worker})
			for _, want := range tt.wantContain {
				require.Contains(t, html, want)
			}
			for _, absent := range tt.wantAbsent {
				require.NotContains(t, html, absent)
			}
		})
	}
}
