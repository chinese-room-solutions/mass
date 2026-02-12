//go:build !windows

package web

// listRoots returns the filesystem root on non-Windows platforms.
func listRoots() []string {
	return []string{"/"}
}
