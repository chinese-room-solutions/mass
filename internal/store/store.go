// Package store implements [StoreInterface] against a SQL database.
//
// **Postgres readiness:** SQL-only by design. When Postgres lands, work is
// concentrated in:
//   - Paired migrations (`.postgres.up.sql`); [Store.migrate] picks one.
//     SQLite `STRICT`+`TEXT`+`BLOB` → Postgres `TEXT`+`BYTEA`.
//   - `strftime(...)` calls in [device_queue_state.go] — easiest to
//     replace by computing timestamps in Go.
//   - Driver: register `lib/pq` or `pgx` alongside `modernc.org/sqlite`.
//
// [StoreInterface] does not need to change.
package store

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/KernelPryanic/ctxerr"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.up.sql
var migrationsFS embed.FS

// Store wraps a SQLite database for MASS persistent state.
type Store struct {
	db *sql.DB
}

// Open creates or opens a SQLite database at the given path.
func Open(path string) (*Store, error) {
	errCtx := map[string]any{"path": path}

	db, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("opening database: %w", err), errCtx)
	}
	db.SetMaxOpenConns(1)

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		closeErr := db.Close()
		return nil, ctxerr.With(fmt.Errorf("running migrations: %w", errors.Join(err, closeErr)), errCtx)
	}
	return s, nil
}

// DB returns the underlying *sql.DB for use by subsystems that need direct access
// (e.g. goqite queue).
func (s *Store) DB() *sql.DB {
	return s.db
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

// migrate reads numbered .up.sql files from the embedded migrations directory
// and applies them in order. Applied versions are tracked in _migrations.
func (s *Store) migrate() error {
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS _migrations (version INTEGER PRIMARY KEY)`); err != nil {
		return fmt.Errorf("creating migrations table: %w", err)
	}

	files, err := fs.Glob(migrationsFS, "migrations/*.up.sql")
	if err != nil {
		return fmt.Errorf("listing migrations: %w", err)
	}
	sort.Strings(files)

	for _, file := range files {
		version, err := parseVersion(file)
		if err != nil {
			return err
		}

		var exists int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM _migrations WHERE version = ?`, version).Scan(&exists); err != nil {
			return fmt.Errorf("checking migration %d: %w", version, err)
		}
		if exists > 0 {
			continue
		}

		ddl, err := fs.ReadFile(migrationsFS, file)
		if err != nil {
			return fmt.Errorf("reading migration %d: %w", version, err)
		}
		if _, err := s.db.Exec(string(ddl)); err != nil {
			return fmt.Errorf("applying migration %d (%s): %w", version, file, err)
		}
		if _, err := s.db.Exec(`INSERT INTO _migrations (version) VALUES (?)`, version); err != nil {
			return fmt.Errorf("recording migration %d: %w", version, err)
		}
	}
	return nil
}

// parseVersion extracts the numeric prefix from a migration filename.
// E.g. "migrations/000002_settings.up.sql" → 2.
func parseVersion(path string) (int, error) {
	base := path
	if i := strings.LastIndex(path, "/"); i >= 0 {
		base = path[i+1:]
	}
	var version int
	if _, err := fmt.Sscanf(base, "%06d_", &version); err != nil {
		return 0, fmt.Errorf("parsing migration version from %q: %w", path, err)
	}
	return version, nil
}
