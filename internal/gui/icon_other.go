//go:build !windows

package gui

import "unsafe"

// setWindowIcon is a no-op on non-Windows platforms.
// Linux/macOS webviews inherit the icon from the favicon.
func setWindowIcon(_ unsafe.Pointer) {}
