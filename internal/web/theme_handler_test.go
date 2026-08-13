package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHandleSetTheme drives POST /internal/settings/theme through the real mux.
// A valid theme name persists cfg.Theme, notifies the GUI channel with the new
// base, and patches both the theme and themeBase signals; an unknown/empty
// name is a no-op (no persist, no notification, no signal patch).
func TestHandleSetTheme(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantTheme   string // expected cfg.Theme after the call ("" = unchanged from start)
		wantPersist bool
		wantBase    string // base broadcast to the GUI channel ("" = none)
	}{
		{
			name:        "valid dark persists and patches",
			body:        `{"theme":"dark"}`,
			wantTheme:   "dark",
			wantPersist: true,
			wantBase:    "dark",
		},
		{
			name:        "valid light persists and patches",
			body:        `{"theme":"light"}`,
			wantTheme:   "light",
			wantPersist: true,
			wantBase:    "light",
		},
		{
			name:        "unknown name is a no-op",
			body:        `{"theme":"bogus"}`,
			wantTheme:   "dark", // unchanged (test config starts dark)
			wantPersist: false,
		},
		{
			name:        "empty name is a no-op",
			body:        `{"theme":""}`,
			wantTheme:   "dark",
			wantPersist: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			// Start from a known theme so the no-op cases have a clear baseline.
			h.cfg.Theme = "dark"

			var saved bool
			h.saveFn = func() { saved = true }
			guiCh := h.gui.subscribe()
			defer h.gui.unsubscribe(guiCh)

			req := httptest.NewRequest(http.MethodPost, "/internal/settings/theme", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, tt.wantTheme, h.cfg.Theme)
			require.Equal(t, tt.wantPersist, saved)
			var gotBase string
			select {
			case ev := <-guiCh:
				require.Equal(t, GUIEventTheme, ev.name)
				gotBase = ev.data
			default:
			}
			require.Equal(t, tt.wantBase, gotBase, "GUI channel notification")

			if tt.wantBase != "" {
				body := rec.Body.String()
				require.Contains(t, body, `"theme"`)
				require.Contains(t, body, `"themeBase"`)
				require.Contains(t, body, tt.wantTheme)
			} else {
				require.Empty(t, rec.Body.String(), "no-op must not patch signals")
			}
		})
	}
}
