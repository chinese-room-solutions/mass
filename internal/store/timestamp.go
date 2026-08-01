package store

import (
	"fmt"
	"time"
)

// Timestamps are stored as RFC3339Nano text in both dialects (SQLite has no
// native time type; Postgres parity keeps the read path identical).

// nowStamp returns the current UTC time formatted for storage.
func nowStamp() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

// parseStamp parses a stored timestamp. A failure means the column holds
// something we didn't write, so the error is returned for the caller to wrap.
func parseStamp(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing timestamp %q: %w", s, err)
	}
	return t, nil
}
