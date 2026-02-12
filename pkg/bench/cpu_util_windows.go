package bench

import (
	"syscall"
	"unsafe"
)

var getSystemTimes = kernel32.NewProc("GetSystemTimes")

// cpuTicks returns (idle, total) CPU ticks via GetSystemTimes.
func cpuTicks() (idle, total uint64) {
	var idleTime, kernelTime, userTime syscall.Filetime
	r, _, _ := getSystemTimes.Call(
		uintptr(unsafe.Pointer(&idleTime)),
		uintptr(unsafe.Pointer(&kernelTime)),
		uintptr(unsafe.Pointer(&userTime)),
	)
	if r == 0 {
		return 0, 0
	}
	i := uint64(idleTime.HighDateTime)<<32 | uint64(idleTime.LowDateTime)
	k := uint64(kernelTime.HighDateTime)<<32 | uint64(kernelTime.LowDateTime)
	u := uint64(userTime.HighDateTime)<<32 | uint64(userTime.LowDateTime)
	return i, k + u // kernel time includes idle time
}
