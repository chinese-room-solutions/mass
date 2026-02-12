package bench

import (
	"sync"
	"sync/atomic"
	"time"
)

// cpuUtilPct holds the latest CPU utilization percentage (0-100).
// Updated by a background goroutine every second.
var cpuUtilPct atomic.Int64 // stored as pct * 100 (fixed-point, 2 decimals)

var cpuUtilOnce sync.Once

// initCPUMonitor starts a background goroutine that samples CPU utilization.
func initCPUMonitor() {
	cpuUtilOnce.Do(func() {
		go func() {
			prevIdle, prevTotal := cpuTicks()
			for {
				time.Sleep(1 * time.Second)
				idle, total := cpuTicks()
				dIdle := idle - prevIdle
				dTotal := total - prevTotal
				prevIdle = idle
				prevTotal = total
				if dTotal > 0 {
					pct := (1.0 - float64(dIdle)/float64(dTotal)) * 100
					if pct < 0 {
						pct = 0
					}
					if pct > 100 {
						pct = 100
					}
					cpuUtilPct.Store(int64(pct * 100))
				}
			}
		}()
	})
}

// CPUUtilization returns the current CPU utilization percentage (0-100).
func CPUUtilization() float64 {
	initCPUMonitor()
	return float64(cpuUtilPct.Load()) / 100
}
