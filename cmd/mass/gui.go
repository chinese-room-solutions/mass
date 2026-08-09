//go:build !nogui

package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/KernelPryanic/golog"
	"github.com/chinese-room-solutions/mass-sdk/tray"
	"github.com/chinese-room-solutions/mass-sdk/uikit"
	"github.com/chinese-room-solutions/mass-sdk/webview"
	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/icon"
	"github.com/rs/zerolog"
)

// runApp opens the native window as a thin client of the local daemon,
// starting one when needed. The window hosts no backend: it is a browser onto
// the daemon's dashboard, and the theme its native chrome shows travels over
// the control channel it holds open — which is also what keeps an on-demand
// daemon alive while the window lives. Quit closes only the window; the
// daemon retires on its own idle timeout (or keeps serving, if it is an
// operator-run `mass serve`).
func runApp() int {
	// The GUI process logs to stderr only — the daemon owns the log file and
	// the in-app system log.
	logger := golog.New(true, os.Stderr).With().Str("app", "mass-gui").Logger()

	ep, err := localDaemonEndpoint(os.Getenv("MASS_AUTH_TOKEN"))
	if err != nil {
		logger.Error().Err(err).Msg("resolving the local daemon address")
		return 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ensureDaemon(ctx, ep, logger); err != nil {
		logger.Error().Err(err).Msg("starting the mass daemon")
		return 1
	}

	wv := webview.Open(webview.Options{
		Title:   "MASS",
		URL:     ep.base,
		Width:   1440,
		Height:  900,
		IconPNG: icon.PNG,
		Theme:   initialThemeBase(),
	})
	if wv == nil {
		// The daemon runs detached and serves the dashboard to any browser, so
		// there is nothing left for this process to hold open.
		fmt.Fprintln(os.Stderr, "webview unavailable (missing WebView2 runtime?). Open this URL in your browser:", ep.base)
		return 0
	}
	// The channel applies the daemon's theme changes to the native chrome and
	// reconnects on its own if the daemon restarts under the window.
	go runClientChannel(ctx, wv, ep, logger)

	// Fold to the system tray: minimizing hides the window (the daemon keeps
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
	wv.Destroy()
	return 0
}

// initialThemeBase is the native chrome the window opens with, read straight
// from the local config — the daemon's first channel event settles it a
// moment later. Only themes known to this process resolve here; a pluggable
// one the daemon loaded briefly gets the dark default and is then corrected.
func initialThemeBase() string {
	dir, err := config.DefaultDir()
	if err != nil {
		return string(uikit.ThemeDark)
	}
	cfg, _, err := config.Load(dir)
	if err != nil {
		return string(uikit.ThemeDark)
	}
	if ti, ok := uikit.LookupTheme(string(uikit.ParseTheme(cfg.Theme))); ok {
		return string(ti.Base)
	}
	return string(uikit.ThemeDark)
}

const (
	// channelRetryMin/Max bound the reconnect backoff. The daemon can go away
	// under a live window — a crash, an operator restart — and the window has
	// to find it again without spinning.
	channelRetryMin = 500 * time.Millisecond
	channelRetryMax = 5 * time.Second
)

// runClientChannel keeps the window attached to the daemon's control channel
// for as long as ctx lives, reconnecting with backoff whenever the stream
// drops. runApp owns it: the goroutine returns when the window closes and
// cancels ctx.
func runClientChannel(ctx context.Context, wv webview.WindowInterface, ep daemonEndpoint, logger zerolog.Logger) {
	logger = logger.With().Str("component", "gui-channel").Logger()
	backoff := channelRetryMin
	for ctx.Err() == nil {
		attached, err := streamClientChannel(ctx, wv, ep)
		if attached {
			backoff = channelRetryMin // a working connection earns a fast retry.
		}
		if err != nil && ctx.Err() == nil {
			logger.Info().Err(err).Msg("gui channel dropped; reconnecting")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if !attached {
			backoff = min(backoff*2, channelRetryMax)
		}
	}
}

// streamClientChannel holds one connection to the daemon's channel, applying
// theme events until it ends. attached reports whether the stream was
// established at all, which tells the caller whether to back off further.
func streamClientChannel(ctx context.Context, wv webview.WindowInterface, ep daemonEndpoint) (attached bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep.base+"/internal/gui/channel", nil)
	if err != nil {
		return false, fmt.Errorf("building the channel request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	// No client timeout: the stream is meant to live as long as the window
	// does, and ctx is what ends it.
	resp, err := ep.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("attaching to the daemon: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("attaching to the daemon: status %d", resp.StatusCode)
	}

	sc := bufio.NewScanner(resp.Body)
	var name, data string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if name == "theme" {
				// The daemon resolves the theme to its native base
				// (dark|light), so this process needs no theme registry.
				wv.SetTheme(data)
			}
			name, data = "", ""
		case strings.HasPrefix(line, "event:"):
			name = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
	return true, sc.Err()
}
