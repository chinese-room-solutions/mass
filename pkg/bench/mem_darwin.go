package bench

import (
	"encoding/binary"
	"syscall"
	"unsafe"
)

func totalRAMMB() int {
	mib := []int32{6, 24} // CTL_HW, HW_MEMSIZE
	buf := make([]byte, 8)
	n := uintptr(len(buf))
	_, _, errno := syscall.Syscall6(
		syscall.SYS___SYSCTL,
		uintptr(unsafe.Pointer(&mib[0])),
		uintptr(len(mib)),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(unsafe.Pointer(&n)),
		0, 0,
	)
	if errno != 0 {
		return 0
	}
	return int(binary.LittleEndian.Uint64(buf) / (1024 * 1024))
}

func usedRAMMB() int {
	// Without cgo, reading precise used memory on Darwin is unreliable.
	// Return 0 to indicate stats are unavailable; the UI handles this gracefully.
	return 0
}
