package web

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// SessionStore manages authenticated browser sessions in memory.
// Sessions are lost on restart; users simply re-login.
type SessionStore struct {
	mu       sync.RWMutex
	sessions map[string]time.Time // sessionID → expiry
	maxAge   time.Duration
}

// NewSessionStore creates a session store with the given max session age.
func NewSessionStore(maxAge time.Duration) *SessionStore {
	return &SessionStore{
		sessions: make(map[string]time.Time),
		maxAge:   maxAge,
	}
}

// Create generates a cryptographically random session ID and stores it.
func (ss *SessionStore) Create() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	id := hex.EncodeToString(b)
	ss.mu.Lock()
	ss.sessions[id] = time.Now().Add(ss.maxAge)
	ss.mu.Unlock()
	return id, nil
}

// Valid reports whether the session ID exists and has not expired.
func (ss *SessionStore) Valid(id string) bool {
	ss.mu.RLock()
	expiry, ok := ss.sessions[id]
	ss.mu.RUnlock()
	if !ok {
		return false
	}
	if time.Now().After(expiry) {
		ss.Invalidate(id)
		return false
	}
	return true
}

// Invalidate removes a session.
func (ss *SessionStore) Invalidate(id string) {
	ss.mu.Lock()
	delete(ss.sessions, id)
	ss.mu.Unlock()
}

// Janitor removes expired sessions every hour until ctx is cancelled.
// Valid only prunes the session it is asked about, so sessions that are
// never presented again would otherwise accumulate for the process
// lifetime. Run as `go sessions.Janitor(ctx)`.
func (ss *SessionStore) Janitor(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ss.prune()
		}
	}
}

// prune removes every expired session.
func (ss *SessionStore) prune() {
	now := time.Now()
	ss.mu.Lock()
	for id, expiry := range ss.sessions {
		if now.After(expiry) {
			delete(ss.sessions, id)
		}
	}
	ss.mu.Unlock()
}
