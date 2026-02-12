//go:build windows

package gui

import (
	"unsafe"

	webview2 "github.com/jchv/go-webview2"
	"golang.org/x/sys/windows"
)

var (
	dwmapi = windows.NewLazySystemDLL("dwmapi.dll")
	user32 = windows.NewLazySystemDLL("user32.dll")
)

// DWMWA_USE_IMMERSIVE_DARK_MODE tells the DWM compositor to render the
// title bar and window borders using the dark color scheme.
const dwmwaUseImmersiveDarkMode = 20

type window struct {
	wv   webview2.WebView
	hwnd unsafe.Pointer
}

// New creates a native webview window pointing at the given URL.
// When darkMode is true the window title bar uses the OS dark theme.
func New(title, url string, width, height int, darkMode bool) WindowInterface {
	wv := webview2.NewWithOptions(webview2.WebViewOptions{
		AutoFocus: true,
		WindowOptions: webview2.WindowOptions{
			Title:  title,
			Width:  uint(width),
			Height: uint(height),
			Center: true,
		},
	})
	if wv == nil {
		return nil
	}

	w := &window{wv: wv, hwnd: wv.Window()}
	w.SetDarkMode(darkMode)
	setWindowIcon(w.hwnd)

	wv.Navigate(url)
	return w
}

// SetDarkMode switches the title bar between dark (true) and light (false)
// by setting the DWM immersive dark mode attribute on the window.
func (w *window) SetDarkMode(dark bool) {
	setWindowAttribute := dwmapi.NewProc("DwmSetWindowAttribute")
	val := int32(0)
	if dark {
		val = 1
	}
	_, _, _ = setWindowAttribute.Call(
		uintptr(w.hwnd),
		dwmwaUseImmersiveDarkMode,
		uintptr(unsafe.Pointer(&val)),
		unsafe.Sizeof(val),
	)
}

func (w *window) Run()     { w.wv.Run() }
func (w *window) Destroy() { w.wv.Destroy() }
