package bench

// cpuTicks returns (idle, total) CPU ticks.
// Darwin requires host_statistics (Mach API) which needs cgo.
// Return zeros — CPUUtilization() will report 0%.
func cpuTicks() (idle, total uint64) {
	return 0, 0
}
