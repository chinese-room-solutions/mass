//go:build !nogui

package main

import (
	"fmt"
	"os"

	"github.com/chinese-room-solutions/mass-sdk/tray"
	"github.com/chinese-room-solutions/mass-sdk/uikit"
	"github.com/chinese-room-solutions/mass-sdk/webview"
	"github.com/chinese-room-solutions/mass/internal/icon"
	"github.com/chinese-room-solutions/mass/internal/web"
	"github.com/rs/zerolog"
)

// runGUI opens the native webview window plus tray icon and blocks until the
// user quits, then tears the process down. Falls back to waiting headless if
// no webview can be created.
func runGUI(logger zerolog.Logger, theme, url string, handler *web.Handler, done <-chan struct{}, shutdown func()) {
	// Native chrome tracks the theme's base (dark/light) so any pluggable
	// theme maps its window frame correctly.
	themeBase, _ := uikit.LookupTheme(string(uikit.ParseTheme(theme)))
	wv := webview.Open(webview.Options{
		Title:   "MASS",
		URL:     url,
		Width:   1440,
		Height:  900,
		IconPNG: icon.PNG,
		Theme:   string(themeBase.Base),
	})
	if wv == nil {
		logger.Warn().Msg("could not create webview window, running headless")
		fmt.Fprintln(os.Stderr, "warning: webview unavailable (missing WebView2 runtime?), running in headless mode")
		fmt.Fprintln(os.Stderr, "access the UI at", url)
		<-done
		return
	}
	handler.SetOnThemeChange(func(dark bool) {
		if dark {
			wv.SetTheme("dark")
		} else {
			wv.SetTheme("light")
		}
	})

	// Fold to the system tray: minimizing hides the window (the backend keeps
	// running); the tray icon / "Show" restores it; "Quit" or closing the
	// window (the X) exits. The tray runs on its own loop; Quit terminates the
	// webview from there, so Run() returns on this (main) thread and the normal
	// teardown below executes.
	trayStart, trayEnd, _ := tray.Register(tray.Options{
		Title:    "MASS",
		IconPNG:  icon.PNG,
		OnShow:   wv.Show,
		OnToggle: wv.Toggle,
		OnQuit:   wv.Terminate,
	})
	wv.SetOnMinimize(wv.Hide)
	trayStart()
	defer trayEnd()

	wv.Run()
	// Destroy the webview before shutting the server down: the dashboard holds a
	// long-lived SSE stream that never goes idle, so srv.Shutdown would block
	// forever while it's open. Tearing down the WebView2 process first closes
	// that connection. (The window-close path already destroys before Run
	// returns; the tray Quit path reaches here with the webview still alive.)
	wv.Destroy()
	shutdown()
}
