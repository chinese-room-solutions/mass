package scheduler

import (
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/chinese-room-solutions/mass/pkg/stats"
	"github.com/stretchr/testify/require"
)

// projectedLoadBytes combines the measured (base, perSlot) figures from
// the model_benchmarks row and the effective headroom — the explicit
// per-load override first (the worker honors it over its own flag),
// worker-reported second, default 75 last — with the worker's free
// memory to predict the post-grow pool's total memory footprint. The
// table covers every degenerate branch plus a realistic
// GPU-with-headroom case.
func TestScheduler_ProjectedLoadBytes(t *testing.T) {
	const gb = int64(1024 * 1024 * 1024)
	tests := []struct {
		name        string
		deviceMB    int   // total memory on the worker's single GPU
		usedMB      int   // used memory (free = total - used)
		base        int64 // row.BaseBytes
		perSlot     int64 // row.PerSlotBytes
		headroomPct int32 // env.HeadroomPct (gateway hint)
		workerPct   int32 // registration-reported headroom (0 = not reported)
		want        int64
	}{
		{
			// base == 0 — nothing measured. MASS skips projection
			// and returns 0; callers fall back to
			// totalLoadBytes(env.Files).
			name:    "base=0 returns 0 (unknown)",
			base:    0,
			perSlot: gb,
		},
		{
			// perSlot == 0 — runtime has no concurrency dimension
			// (API proxy, single-shot job).
			// Projection collapses to pool=1 → returns base.
			name:        "perSlot=0 collapses to base",
			deviceMB:    24 * 1024,
			usedMB:      0,
			base:        5 * gb,
			perSlot:     0,
			headroomPct: 75,
			want:        5 * gb,
		},
		{
			// 24 GB GPU, 0 GB used → 24 GB free. base = 5 GB,
			// perSlot = 2 GB, headroom = 75% → available = (24-5) ×
			// 0.75 = 14.25 GB. pool = floor(14.25 / 2) = 7 slots.
			// load = 5 + 7×2 = 19 GB.
			name:        "headroom 75: room for 7 extra slots",
			deviceMB:    24 * 1024,
			usedMB:      0,
			base:        5 * gb,
			perSlot:     2 * gb,
			headroomPct: 75,
			want:        5*gb + 7*2*gb,
		},
		{
			// Same shape but tighter headroom (60%) admits fewer
			// slots. (24-5) × 0.6 = 11.4 GB / 2 GB = 5 slots → 15 GB.
			name:        "headroom 60: fewer slots admitted",
			deviceMB:    24 * 1024,
			usedMB:      0,
			base:        5 * gb,
			perSlot:     2 * gb,
			headroomPct: 60,
			want:        5*gb + 5*2*gb,
		},
		{
			// free < base: no room to grow at all. Projection
			// returns base; eligibility check still gates the
			// load (separate predicate).
			name:        "free below base: returns base only",
			deviceMB:    8 * 1024,
			usedMB:      6 * 1024,
			base:        5 * gb,
			perSlot:     2 * gb,
			headroomPct: 75,
			want:        5 * gb,
		},
		{
			// free exactly == base: available = 0, pool = 0, load = base.
			name:        "free equals base: pool=0, returns base",
			deviceMB:    5 * 1024, // 5 GB exactly
			usedMB:      0,
			base:        5 * gb,
			perSlot:     2 * gb,
			headroomPct: 75,
			want:        5 * gb,
		},
		{
			// Slack above base but smaller than one perSlot after
			// headroom: pool = 0 (nothing fits), load = base.
			// (8 - 5) × 0.75 = 2.25 GB; floor(2.25 / 4) = 0.
			name:        "headroom-clipped: single slot wouldn't fit",
			deviceMB:    8 * 1024,
			usedMB:      0,
			base:        5 * gb,
			perSlot:     4 * gb,
			headroomPct: 75,
			want:        5 * gb,
		},
		{
			// Neither worker nor gateway reported → default 75.
			// Same numbers as the headroom=75 case above.
			name:        "headroom=0 uses default 75",
			deviceMB:    24 * 1024,
			usedMB:      0,
			base:        5 * gb,
			perSlot:     2 * gb,
			headroomPct: 0,
			want:        5*gb + 7*2*gb,
		},
		{
			// headroom > 100 (malformed) → defaultHeadroomPct.
			name:        "headroom out of range uses default",
			deviceMB:    24 * 1024,
			usedMB:      0,
			base:        5 * gb,
			perSlot:     2 * gb,
			headroomPct: 200,
			want:        5*gb + 7*2*gb,
		},
		{
			// The envelope carries an explicit per-load override; the
			// worker applies a hint over its own flag at load time, so
			// the projection follows the override too. 75% instead of
			// the worker's 60% → 7 slots.
			name:        "explicit per-load override beats worker-reported flag",
			deviceMB:    24 * 1024,
			usedMB:      0,
			base:        5 * gb,
			perSlot:     2 * gb,
			headroomPct: 75,
			workerPct:   60,
			want:        5*gb + 7*2*gb,
		},
		{
			// Worker reported, gateway silent: the worker value alone
			// drives the projection.
			name:        "worker-reported headroom with no gateway hint",
			deviceMB:    24 * 1024,
			usedMB:      0,
			base:        5 * gb,
			perSlot:     2 * gb,
			headroomPct: 0,
			workerPct:   100,
			want:        5*gb + 9*2*gb,
		},
		{
			// Worker didn't report (0) → the envelope hint applies as
			// before.
			name:        "worker unreported falls back to envelope hint",
			deviceMB:    24 * 1024,
			usedMB:      0,
			base:        5 * gb,
			perSlot:     2 * gb,
			headroomPct: 60,
			workerPct:   0,
			want:        5*gb + 5*2*gb,
		},
		{
			// Malformed worker report (>100) is treated as unset —
			// the envelope hint (checked first anyway) drives it.
			name:        "worker report out of range falls through",
			deviceMB:    24 * 1024,
			usedMB:      0,
			base:        5 * gb,
			perSlot:     2 * gb,
			headroomPct: 60,
			workerPct:   150,
			want:        5*gb + 5*2*gb,
		},
		{
			// 100% headroom — the operator wants every byte. Same
			// 24 GB GPU, base=5 GB, perSlot=2 GB → (24-5)*1.0 = 19
			// GB → 9 slots → 5 + 18 = 23 GB total.
			name:        "headroom 100: full slack admitted",
			deviceMB:    24 * 1024,
			usedMB:      0,
			base:        5 * gb,
			perSlot:     2 * gb,
			headroomPct: 100,
			want:        5*gb + 9*2*gb,
		},
		{
			// Headroom 1 — extreme floor. (24 - 5) * 0.01 = 190 MB,
			// floor(190 MB / 2 GB) = 0. Returns base only. Tests
			// the lower clamp of the [1,100] range.
			name:        "headroom 1: no slot fits past base",
			deviceMB:    24 * 1024,
			usedMB:      0,
			base:        5 * gb,
			perSlot:     2 * gb,
			headroomPct: 1,
			want:        5 * gb,
		},
		{
			// perSlot dominates: huge slot vs. modest free → 0
			// additional slots. Returns base only.
			name:        "perSlot larger than free slack: pool=0",
			deviceMB:    12 * 1024,
			usedMB:      0,
			base:        5 * gb,
			perSlot:     20 * gb,
			headroomPct: 75,
			want:        5 * gb,
		},
		{
			// base larger than free → free-base goes negative,
			// available <= 0 branch returns base. Eligibility
			// would reject this separately, but the projection
			// must not panic or produce nonsense.
			name:        "base larger than total: returns base",
			deviceMB:    4 * 1024,
			usedMB:      0,
			base:        10 * gb,
			perSlot:     2 * gb,
			headroomPct: 75,
			want:        10 * gb,
		},
		{
			// Two GPUs → free memory sums across the device set.
			// 16+16 = 32 GB free, base=5, perSlot=2, headroom=75 →
			// (32-5)*0.75 = 20.25 GB / 2 GB = 10 slots. 5 + 20 = 25 GB.
			name:        "two GPUs: free sums across device set",
			headroomPct: 75,
			base:        5 * gb,
			perSlot:     2 * gb,
			// twoGPUs hook below — see test body.
			deviceMB: -1, // sentinel: tests body switches to two-GPU setup
			want:     5*gb + 10*2*gb,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestScheduler(t)
			devices := []stats.Device{}
			devStats := []stats.DeviceStats{}
			switch {
			case tt.deviceMB == -1:
				// Two-GPU sentinel: 16 GB each, no usage.
				devices = []stats.Device{
					{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: 16 * 1024},
					{ID: "gpu:1", Type: stats.DeviceTypeGPU, TotalMemoryMB: 16 * 1024},
				}
				devStats = []stats.DeviceStats{
					{DeviceID: "gpu:0", UsedMemoryMB: 0, TotalMemoryMB: 16 * 1024},
					{DeviceID: "gpu:1", UsedMemoryMB: 0, TotalMemoryMB: 16 * 1024},
				}
			case tt.deviceMB > 0:
				devices = append(devices, stats.Device{ID: "gpu:0", Type: stats.DeviceTypeGPU, TotalMemoryMB: tt.deviceMB})
				devStats = append(devStats, stats.DeviceStats{DeviceID: "gpu:0", UsedMemoryMB: tt.usedMB, TotalMemoryMB: tt.deviceMB})
			}
			w := worker.NewFakeStreamWorker("w1", "llama-cpp", devices, time.Now())
			if len(devStats) > 0 {
				w.SetFakeDeviceStats(devStats)
			}
			if tt.workerPct != 0 {
				w.SetFakeVRAMHeadroomPct(tt.workerPct)
			}
			require.NoError(t, s.workers.Register(w))

			env := queue.Envelope{HeadroomPct: tt.headroomPct}
			row := store.ModelBenchmarkRow{BaseBytes: tt.base, PerSlotBytes: tt.perSlot}
			require.Equal(t, tt.want, s.projectedLoadBytes(w, env, row))
		})
	}
}

// Envelope round-trips HeadroomPct verbatim through Marshal/Unmarshal,
// including the zero-valued sentinel MASS treats as "unset". The memory
// figures no longer ride the envelope — they come from the measured row.
func TestEnvelope_HeadroomRoundtrip(t *testing.T) {
	tests := []struct {
		name string
		env  queue.Envelope
	}{
		{
			name: "unset",
			env:  queue.Envelope{RequestID: "r1", RuntimeName: "llama-cpp", Cost: 1.0},
		},
		{
			name: "gateway hint",
			env:  queue.Envelope{RequestID: "r2", RuntimeName: "llama-cpp", Cost: 1.0, HeadroomPct: 75},
		},
		{
			name: "edge: headroom 100",
			env:  queue.Envelope{RequestID: "r3", RuntimeName: "llama-cpp", Cost: 1.0, HeadroomPct: 100},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := queue.UnmarshalEnvelope(tt.env.Marshal())
			require.NoError(t, err)
			require.Equal(t, tt.env.HeadroomPct, out.HeadroomPct)
		})
	}
}
