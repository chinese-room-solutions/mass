package queuetest

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()

	ddl, err := os.ReadFile("../../store/migrations/000001_init.up.sql")
	require.NoError(t, err, "reading migration file")

	tmpFile := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", tmpFile+"?_journal_mode=WAL&_busy_timeout=10000")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(string(ddl))
	require.NoError(t, err, "applying migration")

	return db
}

func TestGoqiteQueue_Contract(t *testing.T) {
	RunQueueTests(t, func(t *testing.T) queue.QueueInterface {
		t.Helper()
		return queue.New(setupTestDB(t))
	})
}

func TestGoqiteResultStore_Contract(t *testing.T) {
	RunResultStoreTests(t, func(t *testing.T) queue.ResultStoreInterface {
		t.Helper()
		return queue.NewResultStore(setupTestDB(t))
	})
}
