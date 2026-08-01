package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHandleSetTheme drives POST /internal/settings/theme through the real mux.
// A valid theme name persists cfg.Theme, fires the theme-change callback, and
// patches both the theme and themeBase signals; an unknown/empty name is a
// no-op (no persist, no callback, no signal patch).
func TestHandleSetTheme(t *testing.T) {
	tests := []struct {
		name         string
		body         string
		wantTheme    string // expected cfg.Theme after the call ("" = unchanged from start)
		wantPersist  bool
		wantCallback bool
		wantBaseDark bool // callback arg when fired
	}{
		{
			name:         "valid dark persists and patches",
			body:         `{"theme":"dark"}`,
			wantTheme:    "dark",
			wantPersist:  true,
			wantCallback: true,
			wantBaseDark: true,
		},
		{
			name:         "valid light persists and patches",
			body:         `{"theme":"light"}`,
			wantTheme:    "light",
			wantPersist:  true,
			wantCallback: true,
			wantBaseDark: false,
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
			var cbFired bool
			var cbDark bool
			h.SetOnThemeChange(func(dark bool) { cbFired = true; cbDark = dark })

			req := httptest.NewRequest(http.MethodPost, "/internal/settings/theme", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, tt.wantTheme, h.cfg.Theme)
			require.Equal(t, tt.wantPersist, saved)
			require.Equal(t, tt.wantCallback, cbFired)

			if tt.wantCallback {
				require.Equal(t, tt.wantBaseDark, cbDark)
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
