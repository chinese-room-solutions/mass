//go:build nogui

package main

// runApp in a nogui build has no window to offer: a bare `mass` runs the
// daemon in the foreground, exactly like `mass serve` — right for containers
// and services whose entrypoint is the binary. Keeping the webview/tray
// packages out of this build is what makes it CGO-free.
func runApp() int {
	return runServe(0)
}
