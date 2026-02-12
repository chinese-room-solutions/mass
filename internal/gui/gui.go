// Package gui provides a thin abstraction over native webview windows.
// On Windows it uses jchv/go-webview2 (pure Go); on Linux/macOS it uses
// webview/webview_go (CGO + WebKit).
package gui

// WindowInterface represents a native webview window.
type WindowInterface interface {
	// Run starts the event loop and blocks until the window is closed.
	Run()
	// Destroy releases native resources. Must be called after Run returns.
	Destroy()
	// SetDarkMode switches the native title bar between dark and light theme.
	// On platforms that don't support it this is a no-op.
	SetDarkMode(dark bool)
}
