// Daemon control surface: the endpoints the mass launcher (GUI thin client and
// CLI) uses to discover, replace, and attach to a running daemon. They live
// under /internal/ because they are for the binary's own clients, not part of
// the public mass.v1 contract, and they only answer loopback callers — the
// launcher always manages the daemon on its own host.
package web

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/chinese-room-solutions/mass-sdk/uikit"
)

// DaemonPing is /internal/daemon/ping's response. Version lets an upgraded
// launcher spot a stale build; OnDemand says whether this daemon runs with an
// idle timeout — only an on-demand daemon may be replaced on version skew (an
// operator-managed `mass serve` is never restarted from under its workers).
type DaemonPing struct {
	Version  string `json:"version"`
	OnDemand bool   `json:"on_demand"`
}

// SetShutdownFunc wires the process teardown the shutdown endpoint and the
// idle tracker trigger. fn must be idempotent and safe to call from any
// goroutine.
func (h *Handler) SetShutdownFunc(fn func()) {
	h.shutdownMu.Lock()
	h.shutdownFn = fn
	h.shutdownMu.Unlock()
}

// isLoopbackRequest reports whether r came in over a loopback connection.
// The daemon control surface is host-local: a launcher manages only the
// daemon beside it, so remote callers are refused even with valid auth.
func isLoopbackRequest(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// handleDaemonPing identifies the running daemon to a local launcher: which
// build it is, and whether it is an on-demand (idle-timeout) instance the
// launcher may replace on version skew.
func (h *Handler) handleDaemonPing(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackRequest(r) {
		http.Error(w, "loopback only", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(DaemonPing{Version: h.version, OnDemand: h.onDemand}); err != nil {
		h.logger.Debug().Err(err).Msg("writing daemon ping")
	}
}

// handleDaemonShutdown asks the daemon to retire. It answers 200 first and
// runs the teardown from a goroutine — the teardown drains this very request,
// so doing it inline would deadlock the response it is supposed to send.
// Unlike ping, this route is NOT auth-exempt: with an operator token set, the
// caller must present it.
func (h *Handler) handleDaemonShutdown(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackRequest(r) {
		http.Error(w, "loopback only", http.StatusForbidden)
		return
	}
	h.shutdownMu.Lock()
	fn := h.shutdownFn
	h.shutdownMu.Unlock()
	if fn == nil {
		http.Error(w, "shutdown is not wired", http.StatusServiceUnavailable)
		return
	}
	h.logger.Info().Msg("shutdown requested over the daemon control API")
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write([]byte(`{"status":"stopping"}`)); err != nil {
		h.logger.Debug().Err(err).Msg("writing shutdown response")
	}
	go fn()
}

// SSE event names on the GUI channel. Exported because the window that reads
// them lives in another process (cmd/mass) — this is a wire contract between
// the two, like DaemonPing, and neither side may drift from it.
const (
	// GUIEventTheme carries the native chrome base, "dark" or "light".
	GUIEventTheme = "theme"
	// GUIEventUpdateRestarting carries the incoming release tag. It tells the
	// window an update is replacing this very build: it must say so and quit,
	// rather than reconnecting to whatever answers next (the relaunched build
	// opens its own window, and two would be left).
	GUIEventUpdateRestarting = "update-restarting"
)

// guiEvent is one message pushed down the GUI channel.
type guiEvent struct {
	name string
	data string
}

// guiChannel fans daemon events out to the attached GUI windows. The window is
// a plain webview in another process, so the native chrome learns its
// dark/light base — and learns that it is about to be replaced — over this
// stream rather than an in-process callback. Holding the stream open is also
// what keeps an idle-timeout daemon alive under an open window: the request
// counts as in flight for the window's whole life.
type guiChannel struct {
	mu   sync.Mutex
	subs map[chan guiEvent]struct{}
}

func newGUIChannel() *guiChannel {
	return &guiChannel{subs: map[chan guiEvent]struct{}{}}
}

// subscribe registers a listener. The channel is buffered; broadcast drops
// events a wedged listener isn't reading (the next change supersedes them).
func (g *guiChannel) subscribe() chan guiEvent {
	ch := make(chan guiEvent, 4)
	g.mu.Lock()
	g.subs[ch] = struct{}{}
	g.mu.Unlock()
	return ch
}

func (g *guiChannel) unsubscribe(ch chan guiEvent) {
	g.mu.Lock()
	delete(g.subs, ch)
	g.mu.Unlock()
}

// broadcast pushes a theme base ("dark"/"light") to every attached window.
func (g *guiChannel) broadcast(base string) {
	g.send(guiEvent{name: GUIEventTheme, data: base})
}

// send pushes one event to every attached window, dropping it for a listener
// that isn't reading. Safe for a theme tick, which the next change supersedes.
func (g *guiChannel) send(ev guiEvent) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for ch := range g.subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

// themeBase resolves the configured theme name to the base (dark|light) the
// native window chrome understands; anything unknown falls back to dark.
func themeBase(name string) string {
	if ti, ok := uikit.LookupTheme(string(uikit.ParseTheme(name))); ok {
		return string(ti.Base)
	}
	return string(uikit.ThemeDark)
}

// handleGUIChannel streams daemon events to the GUI window as SSE, for as long
// as the window holds the request open. The current theme is sent on attach —
// the window opens before it connects, so its chrome may be a step behind the
// stored theme. The stream ends when the window disconnects or the daemon
// shuts down (BaseContext cancellation fires r.Context()).
func (h *Handler) handleGUIChannel(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackRequest(r) {
		http.Error(w, "loopback only", http.StatusForbidden)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch := h.gui.subscribe()
	defer h.gui.unsubscribe(ch)

	send := func(ev guiEvent) bool {
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.name, ev.data); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	if !send(guiEvent{name: GUIEventTheme, data: themeBase(h.cfg.Theme)}) {
		return
	}
	for {
		select {
		case ev := <-ch:
			if !send(ev) {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}
