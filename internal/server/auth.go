package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// NewAuthMiddleware returns an HTTP middleware that validates
// the Authorization: Bearer <api-key> header against the given bcrypt hash.
// If authHash is nil, validation is skipped (dev mode).
// The /ping health-check path is always allowed through without a key.
func NewAuthMiddleware(authHash []byte, next http.Handler) http.Handler {
	if len(authHash) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" {
			next.ServeHTTP(w, r)
			return
		}
		token := extractBearerToken(r)
		if bcrypt.CompareHashAndPassword(authHash, []byte(token)) != nil {
			writeUnauthenticated(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func extractBearerToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return ""
	}
	return auth[len("Bearer "):]
}

func writeUnauthenticated(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	//nolint:errchkjson // map[string]string cannot fail to marshal
	_ = json.NewEncoder(w).Encode(map[string]string{
		"code": "unauthenticated",
		"msg":  "missing or invalid API key",
	})
}
