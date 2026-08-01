//go:build nogui

package main

import (
	"github.com/chinese-room-solutions/mass/internal/web"
	"github.com/rs/zerolog"
)

// runGUI in a nogui build has no window or tray to offer — the process always
// runs headless and waits for shutdown (signal-driven), exactly like
// --headless. Keeping the webview/tray packages out of this build is what
// makes it CGO-free.
func runGUI(_ zerolog.Logger, _, _ string, _ *web.Handler, done <-chan struct{}, _ func()) {
	<-done
}
