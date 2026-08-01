package web

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// prune removes exactly the expired sessions and leaves live ones valid.
func TestSessionStore_Prune(t *testing.T) {
	tests := []struct {
		name        string
		expiries    map[string]time.Duration // offset from now
		wantRemain  []string
		wantRemoved []string
	}{
		{
			name:       "no sessions",
			expiries:   map[string]time.Duration{},
			wantRemain: nil,
		},
		{
			name: "all live",
			expiries: map[string]time.Duration{
				"a": time.Hour,
				"b": time.Minute,
			},
			wantRemain: []string{"a", "b"},
		},
		{
			name: "all expired",
			expiries: map[string]time.Duration{
				"a": -time.Hour,
				"b": -time.Second,
			},
			wantRemoved: []string{"a", "b"},
		},
		{
			name: "mixed",
			expiries: map[string]time.Duration{
				"live":    time.Hour,
				"expired": -time.Hour,
			},
			wantRemain:  []string{"live"},
			wantRemoved: []string{"expired"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ss := NewSessionStore(time.Hour)
			now := time.Now()
			ss.mu.Lock()
			for id, off := range tt.expiries {
				ss.sessions[id] = now.Add(off)
			}
			ss.mu.Unlock()

			ss.prune()

			ss.mu.Lock()
			remaining := len(ss.sessions)
			ss.mu.Unlock()
			require.Equal(t, len(tt.wantRemain), remaining)
			for _, id := range tt.wantRemain {
				require.True(t, ss.Valid(id), "session %q should remain valid", id)
			}
			for _, id := range tt.wantRemoved {
				require.False(t, ss.Valid(id), "session %q should be pruned", id)
			}
		})
	}
}

// The session cookie carries the Secure attribute exactly when the server
// serves TLS.
func TestSetSessionCookie_Secure(t *testing.T) {
	tests := []struct {
		name   string
		secure bool
	}{
		{name: "TLS enabled", secure: true},
		{name: "plaintext", secure: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			setSessionCookie(rec, "sid", tt.secure)
			cookies := rec.Result().Cookies()
			require.Len(t, cookies, 1)
			c := cookies[0]
			require.Equal(t, "mass_session", c.Name)
			require.Equal(t, "sid", c.Value)
			require.True(t, c.HttpOnly)
			require.Equal(t, tt.secure, c.Secure)
		})
	}
}
