package stats

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

// cpuUtilPct holds the latest CPU utilization percentage (0-100).
// Updated by a background goroutine every second.
var cpuUtilPct atomic.Int64 // stored as pct * 100 (fixed-point, 2 decimals)

var cpuUtilOnce sync.Once

// initCPUMonitor starts a background goroutine that samples CPU utilization.
func initCPUMonitor() {
	cpuUtilOnce.Do(func() {
		go func() {
			for {
				// Blocking call: samples over the interval and returns aggregate %.
				pcts, err := cpu.Percent(time.Second, false)
				if err != nil || len(pcts) == 0 {
					continue
				}
				cpuUtilPct.Store(int64(pcts[0] * 100))
			}
		}()
	})
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
