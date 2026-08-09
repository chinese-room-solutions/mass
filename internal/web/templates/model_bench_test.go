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
			wantContain: []string{
				"host-a", "gpu:0", "42.5 units/s", "0.031 s", "5.0 GiB", "128.0 MiB",
				`data-filter-text="host-a gpu:0 "`,
			},
			wantAbsent: []string{"Incapable", "benchmarking"},
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
			wantContain: []string{
				"host-b", "gpu:1", "Incapable", "failed to allocate KV cache",
				`data-filter-text="host-b gpu:1 failed to allocate kv cache"`,
			},
			wantAbsent: []string{"units/s"},
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
			// The filter input rides the shell's generic helper: it filters
			// the rows container by their data-filter-text.
			if len(tt.view.Rows) > 0 {
				require.Contains(t, html, `id="model-bench-filter-input"`)
				require.Contains(t, html, `id="model-bench-rows"`)
			} else {
				require.NotContains(t, html, `id="model-bench-filter-input"`)
			}
			for _, want := range tt.wantContain {
				require.Contains(t, html, want)
			}
			for _, absent := range tt.wantAbsent {
				require.NotContains(t, html, absent)
			}
		})
	}
}

// The card's aggregate line counts workers, not rows: fleet coverage, the
// span of what they measured, and how many ruled the model out.
func TestModelBenchSummary(t *testing.T) {
	measured := func(worker, deviceSet string, units float64) ModelBenchRowView {
		return ModelBenchRowView{
			WorkerName: worker, DeviceSet: deviceSet, State: ModelBenchMeasured, UnitsPerSec: units,
		}
	}
	incapable := func(worker, deviceSet string) ModelBenchRowView {
		return ModelBenchRowView{
			WorkerName: worker, DeviceSet: deviceSet, State: ModelBenchIncapable, Error: "out of memory",
		}
	}

	tests := []struct {
		name string
		view ModelBenchView
		want string
	}{
		{
			name: "nothing measured yet",
			view: ModelBenchView{ConnectedWorkers: 3},
			want: "benched on 0/3 workers",
		},
		{
			name: "one worker reports a single figure",
			view: ModelBenchView{ConnectedWorkers: 1, Rows: []ModelBenchRowView{measured("host-a", "gpu:0", 42.5)}},
			want: "benched on 1/1 worker · 42.5 units/s",
		},
		{
			name: "several workers report a range",
			view: ModelBenchView{ConnectedWorkers: 4, Rows: []ModelBenchRowView{
				measured("host-a", "gpu:0", 42.5),
				measured("host-b", "gpu:0", 12.5),
				measured("host-c", "cpu:0", 30),
			}},
			want: "benched on 3/4 workers · 12.5–42.5 units/s",
		},
		{
			name: "a worker's device sets count once",
			view: ModelBenchView{ConnectedWorkers: 2, Rows: []ModelBenchRowView{
				measured("host-a", "gpu:0", 42.5),
				measured("host-a", "gpu:1", 40.5),
			}},
			want: "benched on 1/2 workers · 40.5–42.5 units/s",
		},
		{
			name: "incapable workers are counted after the range",
			view: ModelBenchView{ConnectedWorkers: 3, Rows: []ModelBenchRowView{
				measured("host-a", "gpu:0", 42.5),
				incapable("host-b", "gpu:0"),
				incapable("host-c", "gpu:0"),
			}},
			want: "benched on 1/3 workers · 42.5 units/s · 2 incapable",
		},
		{
			name: "a worker that measured somewhere isn't incapable",
			view: ModelBenchView{ConnectedWorkers: 2, Rows: []ModelBenchRowView{
				measured("host-a", "gpu:0", 42.5),
				incapable("host-a", "gpu:1"),
			}},
			want: "benched on 1/2 workers · 42.5 units/s",
		},
		{
			name: "in-flight rows count towards nothing",
			view: ModelBenchView{ConnectedWorkers: 2, Rows: []ModelBenchRowView{
				{WorkerName: "host-a", State: ModelBenchRunning},
			}},
			want: "benched on 0/2 workers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, modelBenchSummary(tt.view))
			require.Contains(t, RenderModelBenchPanel(tt.view), tt.want)
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
