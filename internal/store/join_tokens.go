package store

import (
	"fmt"

	"github.com/KernelPryanic/ctxerr"
)

// JoinTokenRow is a stored worker-enrollment join token. Only the bcrypt hash
// of the token is kept; the plaintext is shown once at mint time. ExpiresAt and
// CreatedAt are unix seconds.
type JoinTokenRow struct {
	ID        string
	TokenHash string
	ExpiresAt int64
	CreatedAt int64
}

// InsertJoinToken persists a minted join token and prunes any expired rows in
// the same transaction (opportunistic cleanup so the table can't grow
// unbounded). now is unix seconds; rows with expires_at <= now are pruned.
func (s *Store) InsertJoinToken(row JoinTokenRow, now int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return ctxerr.With(fmt.Errorf("begin tx: %w", err), map[string]any{"join_token_id": row.ID})
	}
	commit := false
	defer func() {
		if !commit {
			if rerr := tx.Rollback(); rerr != nil {
				panic(fmt.Errorf("rollback: %w", rerr))
			}
		}
	}()
	if _, err := tx.Exec(s.rebind(`DELETE FROM join_tokens WHERE expires_at <= ?`), now); err != nil {
		return ctxerr.With(fmt.Errorf("pruning expired join tokens: %w", err), map[string]any{"join_token_id": row.ID})
	}
	if _, err := tx.Exec(s.rebind(`
		INSERT INTO join_tokens (id, token_hash, expires_at, created_at)
		VALUES (?, ?, ?, ?)`),
		row.ID, row.TokenHash, row.ExpiresAt, row.CreatedAt,
	); err != nil {
		return ctxerr.With(fmt.Errorf("inserting join token: %w", err), map[string]any{"join_token_id": row.ID})
	}
	if err := tx.Commit(); err != nil {
		return ctxerr.With(fmt.Errorf("commit: %w", err), map[string]any{"join_token_id": row.ID})
	}
	commit = true
	return nil
}

// ListValidJoinTokens returns every join token not yet expired at now (unix
// seconds), pruning expired rows first. Callers bcrypt-compare a presented
// token against each row's hash — bcrypt is one-way, so lookup can't be keyed by
// the plaintext. Token counts are tiny (short-TTL, operator-minted), so a full
// scan of live rows is cheap.
func (s *Store) ListValidJoinTokens(now int64) ([]JoinTokenRow, error) {
	if _, err := s.db.Exec(s.rebind(`DELETE FROM join_tokens WHERE expires_at <= ?`), now); err != nil {
		return nil, ctxerr.With(fmt.Errorf("pruning expired join tokens: %w", err), map[string]any{"now": now})
	}
	rows, err := s.db.Query(s.rebind(`
		SELECT id, token_hash, expires_at, created_at
		FROM join_tokens WHERE expires_at > ? ORDER BY created_at`), now)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("listing valid join tokens: %w", err), map[string]any{"now": now})
	}
	defer func() {
		if err := rows.Close(); err != nil {
			panic(fmt.Errorf("close rows: %w", err))
		}
	}()
	var out []JoinTokenRow
	for rows.Next() {
		var r JoinTokenRow
		if err := rows.Scan(&r.ID, &r.TokenHash, &r.ExpiresAt, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning join token row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
