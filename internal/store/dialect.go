package store

import (
	"strconv"
	"strings"
)

// Dialect names the SQL backend a [Store] talks to. The store layer is
// dialect-aware in only a few small places — placeholders, the `max(a, b)`
// scalar, the table catalog, and the migration directory — concentrated in
// this file. Query
// strings elsewhere use `?` and route through [Store.rebind] so callers
// stay backend-agnostic.
type Dialect string

const (
	DialectSQLite   Dialect = "sqlite"
	DialectPostgres Dialect = "postgres"
)

// rebind rewrites `?` placeholders to `$N` for Postgres. SQLite is a no-op.
// Defensively skips `?` inside single- or double-quoted literals.
func rebind(d Dialect, query string) string {
	if d != DialectPostgres {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + 8)
	n := 0
	inSingle, inDouble := false, false
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch c {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
			b.WriteByte(c)
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
			b.WriteByte(c)
		case '?':
			if inSingle || inDouble {
				b.WriteByte(c)
				continue
			}
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func (s *Store) rebind(query string) string { return rebind(s.dialect, query) }

// Rebind is the exported variant of [rebind], used by sibling packages
// (e.g. internal/queue) that share the store's *sql.DB and need to rewrite
// placeholders for the same dialect.
func Rebind(d Dialect, query string) string { return rebind(d, query) }

// Dialect returns the backend the store was opened against. Used by
// subsystems that share the underlying *sql.DB (e.g. the queue package) and
// need to know which SQL flavor to emit.
func (s *Store) Dialect() Dialect { return s.dialect }

// tablesQuery returns the catalog query listing the tables the database
// currently holds: SQLite's `sqlite_master` vs Postgres's `pg_tables`, scoped
// to the schema the connection actually resolves names in.
func (s *Store) tablesQuery() string {
	if s.dialect == DialectPostgres {
		return `SELECT tablename FROM pg_tables WHERE schemaname = current_schema()`
	}
	return `SELECT name FROM sqlite_master WHERE type = 'table'`
}

// maxFn returns the SQL function for two-argument max: SQLite's scalar
// `max(a, b)` vs Postgres's `greatest(a, b)` (Postgres `max` is aggregate-
// only and would raise at runtime if used as a scalar).
func (s *Store) maxFn() string {
	if s.dialect == DialectPostgres {
		return "greatest"
	}
	return "max"
}
