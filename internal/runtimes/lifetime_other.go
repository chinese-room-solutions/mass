//go:build !windows

package runtimes

// superviseChild has no counterpart to Windows' job objects here. Linux's
// Pdeathsig fires when the *thread* that forked exits, which the Go runtime is
// free to do at any time, so it would kill healthy gateways. The durable fix on
// Unix is the other end: a gateway that notices its parent is gone and exits.
func superviseChild(int) error { return nil }
