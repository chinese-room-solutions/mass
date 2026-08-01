package stats

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

// cpuSampleInterval is the window each CPU sample averages over, and the
// delay before retrying a sample that failed.
const cpuSampleInterval = time.Second

// cpuUtilPct holds the latest CPU utilization percentage (0-100).
// Updated by the sampler goroutine every cpuSampleInterval.
var cpuUtilPct atomic.Int64 // stored as pct * 100 (fixed-point, 2 decimals)

var cpuUtilOnce sync.Once

// cpuMonitorCtx bounds the sampler goroutine's lifetime. Package-level
// because the sampler is started lazily from [CPUUtilization], which sits
// behind [HardwareInterface.Stats] and has no context to thread through;
// [StopCPUMonitor] is the exit path.
var cpuMonitorCtx, cpuMonitorStop = context.WithCancel(context.Background())

// sampleFunc matches [cpu.PercentWithContext].
type sampleFunc func(ctx context.Context, interval time.Duration, percpu bool) ([]float64, error)

// initCPUMonitor starts the background goroutine that samples CPU utilization.
func initCPUMonitor() {
	cpuUtilOnce.Do(func() {
		go sampleCPU(cpuMonitorCtx, cpuSampleInterval, cpu.PercentWithContext)
	})
}

// StopCPUMonitor stops the background CPU sampler. Idempotent, and safe to
// call when no sampler was ever started. [CPUUtilization] does not restart
// it — it keeps returning the last sampled value.
func StopCPUMonitor() { cpuMonitorStop() }

// sampleCPU stores a CPU utilization sample every interval until ctx is done.
func sampleCPU(ctx context.Context, interval time.Duration, sample sampleFunc) {
	for ctx.Err() == nil {
		// Blocking call: samples over the interval and returns aggregate %.
		pcts, err := sample(ctx, interval, false)
		if err == nil && len(pcts) > 0 {
			cpuUtilPct.Store(int64(pcts[0] * 100))
			continue
		}
		// A failed sample returns immediately — gopsutil gives up before its
		// own interval sleep when the first counter read fails — so pace the
		// retry here, or an unreadable /proc/stat spins a core.
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// CPUUtilization returns the current CPU utilization percentage (0-100).
func CPUUtilization() float64 {
	initCPUMonitor()
	return float64(cpuUtilPct.Load()) / 100
}

func totalRAMMB() int {
	vm, err := mem.VirtualMemory()
	if err != nil {
		return 0
	}
	return int(vm.Total / (1024 * 1024))
}

func usedRAMMB() int {
	vm, err := mem.VirtualMemory()
	if err != nil {
		return 0
	}
	return int(vm.Used / (1024 * 1024))
}
