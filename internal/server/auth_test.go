package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

func TestNewAuthMiddleware(t *testing.T) {
	const apiKey = "test-secret-key"

	hash, err := bcrypt.GenerateFromPassword([]byte(apiKey), bcrypt.DefaultCost)
	require.NoError(t, err)

	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	tests := []struct {
		name       string
		authHash   []byte
		authHeader string
		wantStatus int
	}{
		{
			name:       "valid key",
			authHash:   hash,
			authHeader: "Bearer " + apiKey,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing header",
			authHash:   hash,
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong key",
			authHash:   hash,
			authHeader: "Bearer wrong-key",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "malformed header no bearer prefix",
			authHash:   hash,
			authHeader: apiKey,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "nil hash skips auth",
			authHash:   nil,
			authHeader: "",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewAuthMiddleware(tt.authHash, ok)

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)

			if rec.Code == http.StatusUnauthorized {
				require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			}
		})
	}
}
