//go:build !nogui

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass-sdk/webview"
	"github.com/chinese-room-solutions/mass/internal/web"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// fakeWindow records what the channel asked the window to do. Only the four
// calls the channel makes are meaningful; the rest satisfy the interface.
type fakeWindow struct {
	mu        sync.Mutex
	themes    []string
	evals     []string
	terminate atomic.Int32
}

func (f *fakeWindow) SetTheme(t string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.themes = append(f.themes, t)
}

func (f *fakeWindow) Eval(js string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evals = append(f.evals, js)
}

func (f *fakeWindow) Terminate() { f.terminate.Add(1) }

func (f *fakeWindow) evaled() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.evals...)
}

func (f *fakeWindow) themed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.themes...)
}

func (f *fakeWindow) Run()                         {}
func (f *fakeWindow) Destroy()                     {}
func (f *fakeWindow) Hide()                        {}
func (f *fakeWindow) Show()                        {}
func (f *fakeWindow) Toggle()                      {}
func (f *fakeWindow) SetOnMinimize(func())         {}
func (f *fakeWindow) SetOnFileDrop(func([]string)) {}
func (f *fakeWindow) PickFolder(string) (string, bool, error) {
	return "", false, webview.ErrUnsupported
}
func (f *fakeWindow) Screenshot() ([]byte, error) { return nil, webview.ErrUnsupported }

var _ webview.WindowInterface = (*fakeWindow)(nil)

func TestDispatchGUIEvent(t *testing.T) {
	tests := []struct {
		name          string
		event         string
		data          string
		wantThemes    []string
		wantUpdating  bool
		wantTerminate bool
	}{
		{
			name:       "a theme event repaints the native chrome",
			event:      web.GUIEventTheme,
			data:       "light",
			wantThemes: []string{"light"},
		},
		{
			name:          "update-restarting stops the reconnect and closes the window",
			event:         web.GUIEventUpdateRestarting,
			data:          "v0.5.0",
			wantUpdating:  true,
			wantTerminate: true,
		},
		{
			name:  "an unknown event is ignored",
			event: "something-else",
			data:  "x",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wv := &fakeWindow{}
			var updating atomic.Bool
			dispatchGUIEvent(wv, tt.event, tt.data, &updating, zerolog.Nop())

			// The flag is what ends the reconnect loop, so it must be set by the
			// time dispatch returns — not from the goroutine that closes the
			// window, which sleeps out its notice grace first.
			require.Equal(t, tt.wantUpdating, updating.Load())
			require.Equal(t, tt.wantThemes, wv.themed())
			if tt.wantTerminate {
				require.Eventually(t, func() bool { return wv.terminate.Load() == 1 },
					updateNoticeGrace+5*time.Second, 20*time.Millisecond)
				require.Contains(t, wv.evaled()[0], "massUpdateRestarting")
				require.Contains(t, wv.evaled()[0], `"v0.5.0"`)
			} else {
				require.Zero(t, wv.terminate.Load())
			}
		})
	}

	t.Run("a repeated update-restarting closes the window once", func(t *testing.T) {
		wv := &fakeWindow{}
		var updating atomic.Bool
		dispatchGUIEvent(wv, web.GUIEventUpdateRestarting, "v0.5.0", &updating, zerolog.Nop())
		dispatchGUIEvent(wv, web.GUIEventUpdateRestarting, "v0.5.0", &updating, zerolog.Nop())

		require.Eventually(t, func() bool { return wv.terminate.Load() >= 1 },
			updateNoticeGrace+5*time.Second, 20*time.Millisecond)
		time.Sleep(updateNoticeGrace / 2)
		require.EqualValues(t, 1, wv.terminate.Load())
	})
}

// TestRunClientChannelStopsOnUpdate is the regression this fix exists for: once
// the daemon says an update is coming, the window must NOT re-attach. Before,
// it reconnected to whatever answered on the same fixed port — the relaunched
// build's own daemon — leaving the user with two MASS windows.
func TestRunClientChannelStopsOnUpdate(t *testing.T) {
	var attaches atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attaches.Add(1)
		flusher, ok := w.(http.Flusher)
		require.True(t, ok)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		// Announce the update, then drop the stream the way a retiring daemon
		// does. A window that still reconnects would attach a second time.
		_, _ = w.Write([]byte("event: " + web.GUIEventUpdateRestarting + "\ndata: v0.5.0\n\n"))
		flusher.Flush()
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	wv := &fakeWindow{}
	done := make(chan struct{})
	go func() {
		runClientChannel(ctx, wv, daemonEndpoint{base: srv.URL, client: srv.Client()}, zerolog.Nop())
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("the channel kept reconnecting after the update event")
	}
	require.EqualValues(t, 1, attaches.Load(), "the window must not re-attach after the update event")
	require.Eventually(t, func() bool { return wv.terminate.Load() == 1 },
		updateNoticeGrace+5*time.Second, 20*time.Millisecond)
}
