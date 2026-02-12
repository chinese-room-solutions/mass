package web

import "golang.org/x/sys/windows"

// listRoots returns all available drive roots on Windows (e.g. "C:\", "D:\").
func listRoots() []string {
	buf := make([]uint16, 256)
	n, err := windows.GetLogicalDriveStrings(uint32(len(buf)), &buf[0])
	if err != nil || n == 0 {
		return []string{`C:\`}
	}
	// Buffer is a sequence of null-terminated strings ending with a double null.
	var roots []string
	for i := 0; i < int(n); {
		j := i
		for j < int(n) && buf[j] != 0 {
			j++
		}
		if j > i {
			roots = append(roots, windows.UTF16ToString(buf[i:j+1]))
		}
		i = j + 1
	}
	return roots
}
