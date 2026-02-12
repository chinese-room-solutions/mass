//go:build !windows

package main

// attachOrAllocConsole is a no-op on non-Windows platforms
// (console is always available).
func attachOrAllocConsole() {}
