package store

import (
	"database/sql"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// seedMigratedDB writes a database recorded as already migrated, optionally
// without some of the tables the current schema declares — the shape of every
// database created before those tables were appended to the in-place-edited
// migration file.
func seedMigratedDB(t *testing.T, path string, dropTables ...string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	ddl, err := fs.ReadFile(migrationsFS, "migrations/sqlite/000001_init.up.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(ddl))
	require.NoError(t, err)
	for _, table := range dropTables {
		_, err = db.Exec("DROP TABLE " + table)
		require.NoError(t, err)
	}

	_, err = db.Exec(`CREATE TABLE _migrations (version INTEGER PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO _migrations (version) VALUES (1)`)
	require.NoError(t, err)
}

func TestOpenVerifiesSchema(t *testing.T) {
	tests := []struct {
		name        string
		seed        bool     // pre-create a database recorded as migrated
		dropTables  []string // tables the seeded database lacks
		wantMissing []string
	}{
		{
			name: "fresh database migrates and passes",
		},
		{
			name: "already-migrated database with the full schema passes",
			seed: true,
		},
		{
			name:        "database predating the worker enrollment tables is rejected",
			seed:        true,
			dropTables:  []string{"workers", "join_tokens"},
			wantMissing: []string{"workers", "join_tokens"},
		},
		{
			name:        "single missing table is reported",
			seed:        true,
			dropTables:  []string{"downloads"},
			wantMissing: []string{"downloads"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "test.db")
			if tt.seed {
				seedMigratedDB(t, dbPath, tt.dropTables...)
			}

			s, err := Open(DialectSQLite, dbPath)
			if len(tt.wantMissing) == 0 {
				require.NoError(t, err)
				t.Cleanup(func() { _ = s.Close() })
				require.NoError(t, s.InsertWorker(WorkerRow{WorkerID: "w1", SecretHash: "h", CreatedAt: 1}))
				return
			}

			require.Error(t, err)
			for _, table := range tt.wantMissing {
				require.Contains(t, err.Error(), table)
			}
			require.NotContains(t, err.Error(), "settings")
			require.Contains(t, err.Error(), dbPath)
			require.Contains(t, err.Error(), "delete the database")
		})
	}
}

func TestParseTableNames(t *testing.T) {
	tests := []struct {
		name string
		ddl  string
		want []string
	}{
		{
			name: "plain statement",
			ddl:  "CREATE TABLE workers (worker_id TEXT PRIMARY KEY);",
			want: []string{"workers"},
		},
		{
			name: "lowercase, if not exists, newline before columns",
			ddl:  "create table if not exists  Join_Tokens\n(\n    id TEXT\n);",
			want: []string{"join_tokens"},
		},
		{
			name: "quoted identifiers",
			ddl:  "CREATE TABLE \"workers\" (id TEXT);\nCREATE TABLE `runtimes`(id TEXT);",
			want: []string{"workers", "runtimes"},
		},
		{
			name: "ignores indexes, triggers and table options",
			ddl: "CREATE TABLE settings (key TEXT) STRICT;\n" +
				"CREATE INDEX settings_key_idx ON settings (key);\n" +
				"CREATE TRIGGER t AFTER UPDATE ON settings BEGIN SELECT 1; END;",
			want: []string{"settings"},
		},
		{
			name: "no tables",
			ddl:  "CREATE EXTENSION IF NOT EXISTS pgcrypto;",
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, parseTableNames(tt.ddl))
		})
	}
}

func TestDeclaredTables(t *testing.T) {
	sqliteTables, err := declaredTables(DialectSQLite)
	require.NoError(t, err)
	require.Subset(t, sqliteTables, []string{"workers", "join_tokens"})

	postgresTables, err := declaredTables(DialectPostgres)
	require.NoError(t, err)
	require.ElementsMatch(t, sqliteTables, postgresTables)
}
