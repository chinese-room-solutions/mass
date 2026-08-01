// Package store implements [StoreInterface] against a SQL database.
//
// Two backends are supported, swappable via [Dialect]:
//   - SQLite (default) — file-backed, single-writer; ideal for desktop
//     deployments. Driver: `modernc.org/sqlite` (pure Go).
//   - Postgres — for multi-process or shared deployments. Driver:
//     `github.com/jackc/pgx/v5/stdlib`.
//
// Dialect-specific concerns are confined to [dialect.go] (placeholder
// rewriting, scalar `max`, table catalog) and the per-dialect migration
// directories under [migrations/sqlite] and [migrations/postgres]. Query
// strings in the rest of the package use `?` and route through
// [Store.rebind].
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"

	"github.com/KernelPryanic/ctxerr"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

//go:embed migrations/sqlite/*.up.sql migrations/postgres/*.up.sql
var migrationsFS embed.FS

// Store wraps a SQL database for MASS persistent state.
type Store struct {
	db      *sql.DB
	dialect Dialect
}

// Open opens a database against the given dialect and DSN, running pending
// migrations to bring the schema up to date.
//
// For [DialectSQLite] the DSN is a filesystem path (e.g. ".../mass.db"); a
// `?_journal_mode=WAL&_busy_timeout=5000` suffix is appended automatically.
// MaxOpenConns is forced to 1 because SQLite is single-writer.
//
// For [DialectPostgres] the DSN is passed verbatim to pgx
// (e.g. "postgres://user:pw@host:5432/mass"). MaxOpenConns defaults to 25.
func Open(dialect Dialect, dsn string) (*Store, error) {
	errCtx := map[string]any{"dialect": string(dialect), "dsn": redactDSN(dsn)}

	var (
		db  *sql.DB
		err error
	)
	switch dialect {
	case DialectSQLite:
		db, err = sql.Open("sqlite", dsn+"?_journal_mode=WAL&_busy_timeout=5000")
		if err != nil {
			return nil, ctxerr.With(fmt.Errorf("opening database: %w", err), errCtx)
		}
		db.SetMaxOpenConns(1)
	case DialectPostgres:
		db, err = sql.Open("pgx", dsn)
		if err != nil {
			return nil, ctxerr.With(fmt.Errorf("opening database: %w", err), errCtx)
		}
		db.SetMaxOpenConns(25)
	default:
		return nil, ctxerr.With(fmt.Errorf("unsupported dialect %q", dialect), errCtx)
	}

	s := &Store{db: db, dialect: dialect}
	if err := s.migrate(); err != nil {
		closeErr := db.Close()
		return nil, ctxerr.With(fmt.Errorf("running migrations: %w", errors.Join(err, closeErr)), errCtx)
	}
	if err := s.verifySchema(dsn); err != nil {
		closeErr := db.Close()
		return nil, ctxerr.With(errors.Join(err, closeErr), errCtx)
	}
	return s, nil
}

// redactDSN strips the password from a Postgres URL so it's safe to log.
// SQLite DSNs (filesystem paths) are returned unchanged.
func redactDSN(dsn string) string {
	if !strings.HasPrefix(dsn, "postgres://") && !strings.HasPrefix(dsn, "postgresql://") {
		return dsn
	}
	at := strings.LastIndex(dsn, "@")
	if at < 0 {
		return dsn
	}
	scheme := strings.Index(dsn, "://")
	if scheme < 0 {
		return dsn
	}
	userInfo := dsn[scheme+3 : at]
	colon := strings.Index(userInfo, ":")
	if colon < 0 {
		return dsn
	}
	return dsn[:scheme+3] + userInfo[:colon] + ":***" + dsn[at:]
}

// DB returns the underlying *sql.DB for use by subsystems that need direct access
// (e.g. goqite queue).
func (s *Store) DB() *sql.DB {
	return s.db
}

// Ping checks that the database is reachable. Used by /ready.
func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

// migrate reads numbered .up.sql files from the embedded migrations
// directory for the configured dialect and applies them in order. Applied
// versions are tracked in `_migrations`.
func (s *Store) migrate() error {
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS _migrations (version INTEGER PRIMARY KEY)`); err != nil {
		return fmt.Errorf("creating migrations table: %w", err)
	}

	files, err := migrationFiles(s.dialect)
	if err != nil {
		return err
	}

	for _, file := range files {
		version, err := parseVersion(file)
		if err != nil {
			return err
		}

		var exists int
		if err := s.db.QueryRow(s.rebind(`SELECT COUNT(*) FROM _migrations WHERE version = ?`), version).Scan(&exists); err != nil {
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
		if _, err := s.db.Exec(s.rebind(`INSERT INTO _migrations (version) VALUES (?)`), version); err != nil {
			return fmt.Errorf("recording migration %d: %w", version, err)
		}
	}
	return nil
}

// migrationFiles lists the dialect's embedded .up.sql files in version order.
func migrationFiles(dialect Dialect) ([]string, error) {
	files, err := fs.Glob(migrationsFS, "migrations/"+string(dialect)+"/*.up.sql")
	if err != nil {
		return nil, fmt.Errorf("listing migrations: %w", err)
	}
	sort.Strings(files)
	return files, nil
}

// verifySchema checks the database actually holds every table the migration
// files declare, and fails startup loudly when it doesn't.
//
// Pre-1.0 MASS ships a single migration file per dialect that is edited in
// place as the schema evolves. A database created before a table was added
// already has that file's version recorded, so [Store.migrate] skips it and
// the table is never created — a gap that otherwise stays invisible until
// some unrelated request hits the missing table and fails with a bare
// "no such table". Drift within a table (a column added later) is not
// detectable this way and is deliberately not guessed at: re-running the DDL
// to self-heal would paper over exactly that case.
func (s *Store) verifySchema(dsn string) error {
	declared, err := declaredTables(s.dialect)
	if err != nil {
		return err
	}
	present, err := s.existingTables()
	if err != nil {
		return err
	}

	var missing []string
	for _, table := range declared {
		if !present[table] {
			missing = append(missing, table)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%s database %q is missing table(s) %s declared by the current schema. "+
			"It was created before those tables were added: MASS is pre-1.0 and edits its migration files in place, "+
			"so a database already recorded as migrated never picks up later schema changes. "+
			"Stop MASS and delete the database to have it recreated, or apply the missing DDL by hand",
		s.dialect, redactDSN(dsn), strings.Join(missing, ", "))
}

// declaredTables returns the table names the dialect's migration files create,
// parsed out of the embedded DDL so the list can never drift from the schema.
func declaredTables(dialect Dialect) ([]string, error) {
	files, err := migrationFiles(dialect)
	if err != nil {
		return nil, err
	}
	var (
		tables []string
		seen   = make(map[string]bool)
	)
	for _, file := range files {
		ddl, err := fs.ReadFile(migrationsFS, file)
		if err != nil {
			return nil, fmt.Errorf("reading migration %s: %w", file, err)
		}
		for _, table := range parseTableNames(string(ddl)) {
			if !seen[table] {
				seen[table] = true
				tables = append(tables, table)
			}
		}
	}
	return tables, nil
}

// createTableRe captures the table name of a CREATE TABLE statement, tolerating
// case, arbitrary whitespace and quoted identifiers.
var createTableRe = regexp.MustCompile(`(?i)\bcreate\s+table\s+(?:if\s+not\s+exists\s+)?([^\s(]+)\s*\(`)

// parseTableNames extracts the lowercased names of the tables a DDL script
// creates.
func parseTableNames(ddl string) []string {
	matches := createTableRe.FindAllStringSubmatch(ddl, -1)
	tables := make([]string, 0, len(matches))
	for _, m := range matches {
		if name := strings.ToLower(strings.Trim(m[1], "\"'`[]")); name != "" {
			tables = append(tables, name)
		}
	}
	return tables
}

// existingTables returns the lowercased names of the tables the database
// currently has.
func (s *Store) existingTables() (map[string]bool, error) {
	rows, err := s.db.Query(s.tablesQuery())
	if err != nil {
		return nil, fmt.Errorf("listing existing tables: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			panic(fmt.Errorf("close rows: %w", err))
		}
	}()
	tables := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scanning table name: %w", err)
		}
		tables[strings.ToLower(name)] = true
	}
	return tables, rows.Err()
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
