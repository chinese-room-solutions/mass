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
		// Allow static assets, the worker installer proxy, login, ops probes,
		// and the worker hub without operator auth. /setup/* serves the public
		// installer artifact (no secrets — the token rides in the pasted command
		// line). Ops probes (/metrics, /health, /ready)
		// are scraped by Prometheus and K8s, which don't carry session cookies or
		// bearer tokens. The worker hub does its own credential auth per the
		// join-token enrollment contract (join token or per-worker secret, never
		// the shared operator token), so the operator-token middleware must not
		// gate it.
		if strings.HasPrefix(r.URL.Path, "/public/") ||
			strings.HasPrefix(r.URL.Path, "/setup/") ||
			strings.HasPrefix(r.URL.Path, "/mass.v1.worker.WorkerHub/") ||
			r.URL.Path == "/login" ||
			r.URL.Path == "/metrics" ||
			r.URL.Path == "/health" ||
			r.URL.Path == "/ready" {
			next.ServeHTTP(w, r)
			return
		}

		h.authHashMu.RLock()
		hash := h.authHash
		h.authHashMu.RUnlock()

		// No token configured — allow all requests.
		if len(hash) == 0 {
			next.ServeHTTP(w, r)
			return
		}

		if token := bearerToken(r.Header.Get("Authorization")); token != "" {
			if bcrypt.CompareHashAndPassword(hash, []byte(token)) == nil {
				next.ServeHTTP(w, r)
				return
			}
		}

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
			// Best-effort: tolerate a closed client connection silently.
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}

		http.Redirect(w, r, "/login", http.StatusSeeOther)
	})
}

// bearerToken extracts the token from an "Authorization: Bearer <token>"
// header value, or "" when the header is absent or not a bearer credential.
func bearerToken(authHeader string) string {
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(authHeader, "Bearer ")
}

// setSessionCookie sets the auth session cookie with a random session ID.
// secure marks the cookie TLS-only; pass true whenever the server itself
// serves TLS so the browser never sends the session over plaintext.
func setSessionCookie(w http.ResponseWriter, sessionID string, secure bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     "mass_session",
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int((30 * 24 * time.Hour).Seconds()),
	})
}
