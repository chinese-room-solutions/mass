//go:build !windows

package runtimes

import "syscall"

// hiddenSysProcAttr is a no-op on non-Windows platforms (no console concept).
func hiddenSysProcAttr() *syscall.SysProcAttr { return nil }
