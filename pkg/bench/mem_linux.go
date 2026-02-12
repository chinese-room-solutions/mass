package bench

import "syscall"

func totalRAMMB() int {
	var info syscall.Sysinfo_t
	if err := syscall.Sysinfo(&info); err != nil {
		return 0
	}
	return int(uint64(info.Totalram) * uint64(info.Unit) / (1024 * 1024))
}

func usedRAMMB() int {
	var info syscall.Sysinfo_t
	if err := syscall.Sysinfo(&info); err != nil {
		return 0
	}
	total := uint64(info.Totalram) * uint64(info.Unit)
	free := uint64(info.Freeram) * uint64(info.Unit)
	return int((total - free) / (1024 * 1024))
}
