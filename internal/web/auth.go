package web

import (
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// AuthMiddleware returns middleware that validates requests against the
// handler's current auth hash. The hash is read on every request so that
// adding/removing a token via the settings UI takes effect immediately.
func (h *Handler) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Allow static assets and login page without auth.
		if strings.HasPrefix(r.URL.Path, "/public/") || r.URL.Path == "/login" {
			next.ServeHTTP(w, r)
			return
		}

		// Read the current auth hash.
		h.authHashMu.RLock()
		hash := h.authHash
		h.authHashMu.RUnlock()

		// No token configured — allow all requests.
		if len(hash) == 0 {
			next.ServeHTTP(w, r)
			return
		}

		// Check Authorization: Bearer <token> header.
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			token := strings.TrimPrefix(auth, "Bearer ")
			if bcrypt.CompareHashAndPassword(hash, []byte(token)) == nil {
				next.ServeHTTP(w, r)
				return
			}
		}

		// Check mass_session cookie.
		if cookie, err := r.Cookie("mass_session"); err == nil {
			if h.sessions != nil && h.sessions.Valid(cookie.Value) {
				next.ServeHTTP(w, r)
				return
			}
		}

		// API requests get 401; browser requests redirect to login.
		ct := r.Header.Get("Content-Type")
		accept := r.Header.Get("Accept")
		if strings.Contains(accept, "text/event-stream") ||
			ct == "application/json" ||
			strings.HasPrefix(ct, "application/connect") ||
			strings.HasPrefix(ct, "application/grpc") ||
			strings.HasPrefix(ct, "application/proto") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}

		http.Redirect(w, r, "/login", http.StatusSeeOther)
	})
}

// setSessionCookie sets the auth session cookie with a random session ID.
func setSessionCookie(w http.ResponseWriter, sessionID string) {
	http.SetCookie(w, &http.Cookie{
		Name:     "mass_session",
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int((30 * 24 * time.Hour).Seconds()),
	})
}
