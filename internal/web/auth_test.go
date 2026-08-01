package web

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// passHandler is a sentinel next-handler that records whether the middleware
// let the request through.
func passHandler(passed *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		*passed = true
		w.WriteHeader(http.StatusOK)
	})
}

func TestAuthMiddleware_NoTokenAllowsAll(t *testing.T) {
	h := newTestHandler(t) // no auth hash configured

	var passed bool
	r := httptest.NewRequest(http.MethodGet, "/api/runtimes", nil)
	w := httptest.NewRecorder()
	h.AuthMiddleware(passHandler(&passed)).ServeHTTP(w, r)

	require.True(t, passed)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestAuthMiddleware_BypassPaths(t *testing.T) {
	h := newTestHandler(t)
	hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
	require.NoError(t, err)
	h.SetAuthHash(hash) // auth ON

	for _, path := range []string{"/login", "/metrics", "/health", "/ready", "/public/dist.css"} {
		t.Run(path, func(t *testing.T) {
			var passed bool
			r := httptest.NewRequest(http.MethodGet, path, nil)
			w := httptest.NewRecorder()
			h.AuthMiddleware(passHandler(&passed)).ServeHTTP(w, r)
			require.True(t, passed, "bypass path %s must skip auth", path)
		})
	}
}

func TestAuthMiddleware_RejectsAndRedirects(t *testing.T) {
	h := newTestHandler(t)
	hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
	require.NoError(t, err)
	h.SetAuthHash(hash)

	tests := []struct {
		name       string
		setup      func(r *http.Request)
		wantStatus int
		wantPass   bool
	}{
		{
			name:       "browser request redirects to login",
			setup:      func(*http.Request) {},
			wantStatus: http.StatusSeeOther,
		},
		{
			name:       "json request gets 401",
			setup:      func(r *http.Request) { r.Header.Set("Content-Type", "application/json") },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "SSE request gets 401",
			setup:      func(r *http.Request) { r.Header.Set("Accept", "text/event-stream") },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "valid bearer token passes",
			setup: func(r *http.Request) {
				r.Header.Set("Authorization", "Bearer secret")
			},
			wantStatus: http.StatusOK,
			wantPass:   true,
		},
		{
			name: "wrong bearer token rejected",
			setup: func(r *http.Request) {
				r.Header.Set("Authorization", "Bearer wrong")
			},
			wantStatus: http.StatusSeeOther,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var passed bool
			r := httptest.NewRequest(http.MethodGet, "/api/runtimes", nil)
			tt.setup(r)
			w := httptest.NewRecorder()
			h.AuthMiddleware(passHandler(&passed)).ServeHTTP(w, r)
			require.Equal(t, tt.wantStatus, w.Code)
			require.Equal(t, tt.wantPass, passed)
		})
	}
}

func TestAuthMiddleware_ValidSessionCookiePasses(t *testing.T) {
	h := newTestHandler(t)
	hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
	require.NoError(t, err)
	h.SetAuthHash(hash)
	h.sessions = NewSessionStore(time.Hour)
	id, err := h.sessions.Create()
	require.NoError(t, err)

	var passed bool
	r := httptest.NewRequest(http.MethodGet, "/api/runtimes", nil)
	r.AddCookie(&http.Cookie{Name: "mass_session", Value: id})
	w := httptest.NewRecorder()
	h.AuthMiddleware(passHandler(&passed)).ServeHTTP(w, r)

	require.True(t, passed)
}
