package bench

import (
	"os"
	"strconv"
	"strings"
)

// cpuTicks returns (idle, total) CPU ticks from /proc/stat.
func cpuTicks() (idle, total uint64) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0
	}
	// First line: "cpu  user nice system idle iowait irq softirq steal ..."
	line, _, _ := strings.Cut(string(data), "\n")
	fields := strings.Fields(line)
	if len(fields) < 5 || fields[0] != "cpu" {
		return 0, 0
	}
	var sum uint64
	for _, f := range fields[1:] {
		v, _ := strconv.ParseUint(f, 10, 64)
		sum += v
	}
	idleVal, _ := strconv.ParseUint(fields[4], 10, 64)
	return idleVal, sum
}
