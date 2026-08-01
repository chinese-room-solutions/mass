package worker

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// Credential prefixes. A join token enrolls a worker; a per-worker secret
// authenticates it in steady state. The prefix is a human-readable tag only —
// validation always runs against the stored bcrypt hash, never the prefix.
const (
	joinTokenPrefix    = "mjt_"
	workerSecretPrefix = "mws_"

	// credentialRandomBytes is the entropy behind each token: 32 random bytes
	// encoded as unpadded base64url yields 43 characters.
	credentialRandomBytes = 32
)

// DefaultJoinTokenTTLSeconds is the fallback lifetime for a minted join token
// when the caller requests ttl_seconds == 0.
const DefaultJoinTokenTTLSeconds = 3600

// newCredential returns a fresh token of the form prefix + 43 base64url chars
// (32 random bytes). The plaintext is returned to the caller once; only its
// bcrypt hash is ever persisted.
func newCredential(prefix string) (string, error) {
	buf := make([]byte, credentialRandomBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generating credential: %w", err)
	}
	return prefix + base64.RawURLEncoding.EncodeToString(buf), nil
}

// NewJoinToken mints a plaintext join token.
func NewJoinToken() (string, error) { return newCredential(joinTokenPrefix) }

// NewWorkerSecret mints a plaintext per-worker secret.
func NewWorkerSecret() (string, error) { return newCredential(workerSecretPrefix) }

// HashCredential returns the bcrypt hash to persist for a plaintext credential.
func HashCredential(plaintext string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hashing credential: %w", err)
	}
	return string(h), nil
}

// credentialMatches reports whether plaintext is the credential behind hash.
func credentialMatches(hash, plaintext string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(plaintext)) == nil
}

// looksLikeJoinToken reports whether s carries the join-token prefix. Used only
// to disambiguate operator error messages — never as an auth decision.
func looksLikeJoinToken(s string) bool { return strings.HasPrefix(s, joinTokenPrefix) }
