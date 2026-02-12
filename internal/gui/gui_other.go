//go:build !windows

package gui

import webview "github.com/webview/webview_go"

type window struct {
	wv webview.WebView
}

// New creates a native webview window pointing at the given URL.
// The darkMode parameter is currently only used on Windows (DWM title bar);
// on Linux/macOS the OS theme is inherited automatically.
func New(title, url string, width, height int, darkMode bool) WindowInterface {
	wv := webview.New(false)
	if wv == nil {
		return nil
	}
	wv.SetTitle(title)
	wv.SetSize(width, height, webview.HintNone)
	wv.Navigate(url)
	return &window{wv: wv}
}

func (w *window) Run()               { w.wv.Run() }
func (w *window) Destroy()           { w.wv.Destroy() }
func (w *window) SetDarkMode(_ bool) {} // no-op: OS theme inherited
