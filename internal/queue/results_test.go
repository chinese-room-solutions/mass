package queue_test

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/chinese-room-solutions/mass/internal/queue"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/stretchr/testify/require"
)

func newTestResultStore(t *testing.T) (*queue.ResultStore, *sql.DB) {
	t.Helper()
	st, err := store.Open(store.DialectSQLite, filepath.Join(t.TempDir(), "r.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return queue.NewResultStore(st.DB(), st.Dialect()), st.DB()
}

// Processing/Pending are guarded status flips: Processing only promotes a
// pending row, Pending only reverts a processing row, and a terminal status
// (done/error) is never regressed by either — the guard is what makes the
// dispatch path race-safe against a concurrent terminal write.
func TestResultStore_GuardedStatusTransitions(t *testing.T) {
	tests := []struct {
		name  string
		stage func(t *testing.T, s *queue.ResultStore, id string)
		act   func(s *queue.ResultStore, id string) error
		want  queue.ResultStatus
	}{
		{
			name:  "processing flips a pending row",
			stage: func(*testing.T, *queue.ResultStore, string) {},
			act:   (*queue.ResultStore).Processing,
			want:  queue.ResultStatusProcessing,
		},
		{
			name: "processing leaves a done row alone",
			stage: func(t *testing.T, s *queue.ResultStore, id string) {
				t.Helper()
				require.NoError(t, s.Complete(id, []byte("body")))
			},
			act:  (*queue.ResultStore).Processing,
			want: queue.ResultStatusDone,
		},
		{
			name: "processing leaves an error row alone",
			stage: func(t *testing.T, s *queue.ResultStore, id string) {
				t.Helper()
				require.NoError(t, s.Fail(id, "boom"))
			},
			act:  (*queue.ResultStore).Processing,
			want: queue.ResultStatusError,
		},
		{
			name: "pending reverts a processing row",
			stage: func(t *testing.T, s *queue.ResultStore, id string) {
				t.Helper()
				require.NoError(t, s.Processing(id))
			},
			act:  (*queue.ResultStore).Pending,
			want: queue.ResultStatusPending,
		},
		{
			name:  "pending on a pending row is a no-op",
			stage: func(*testing.T, *queue.ResultStore, string) {},
			act:   (*queue.ResultStore).Pending,
			want:  queue.ResultStatusPending,
		},
		{
			name: "pending leaves a done row alone",
			stage: func(t *testing.T, s *queue.ResultStore, id string) {
				t.Helper()
				require.NoError(t, s.Processing(id))
				require.NoError(t, s.Complete(id, []byte("body")))
			},
			act:  (*queue.ResultStore).Pending,
			want: queue.ResultStatusDone,
		},
		{
			name: "pending leaves an error row alone",
			stage: func(t *testing.T, s *queue.ResultStore, id string) {
				t.Helper()
				require.NoError(t, s.Processing(id))
				require.NoError(t, s.Fail(id, "boom"))
			},
			act:  (*queue.ResultStore).Pending,
			want: queue.ResultStatusError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestResultStore(t)
			const id = "rid-1"
			require.NoError(t, s.Create(id))
			tt.stage(t, s, id)

			require.NoError(t, tt.act(s, id))

			r, err := s.Get(id)
			require.NoError(t, err)
			require.NotNil(t, r)
			require.Equal(t, tt.want, r.Status)
		})
	}
}

// Cleanup prunes only terminal rows past the TTL. A job that has been
// queued or running longer than the TTL keeps its result row — deleting
// it would make the eventual Complete/Fail update zero rows and the
// caller poll forever against NotFound.
func TestResultStore_Cleanup_TerminalRowsOnly(t *testing.T) {
	const ttl = time.Hour

	tests := []struct {
		name string
		// stage moves the row past Create; backdate ages completed_at
		// beyond the TTL afterwards.
		stage       func(t *testing.T, s *queue.ResultStore, id string)
		backdate    bool
		wantSurvive bool
	}{
		{
			name:        "pending row survives any TTL",
			stage:       func(*testing.T, *queue.ResultStore, string) {},
			wantSurvive: true,
		},
		{
			name: "processing row survives any TTL",
			stage: func(t *testing.T, s *queue.ResultStore, id string) {
				t.Helper()
				require.NoError(t, s.Processing(id))
			},
			wantSurvive: true,
		},
		{
			name: "fresh done row survives",
			stage: func(t *testing.T, s *queue.ResultStore, id string) {
				t.Helper()
				require.NoError(t, s.Complete(id, []byte("body")))
			},
			wantSurvive: true,
		},
		{
			name: "fresh error row survives",
			stage: func(t *testing.T, s *queue.ResultStore, id string) {
				t.Helper()
				require.NoError(t, s.Fail(id, "boom"))
			},
			wantSurvive: true,
		},
		{
			name: "old done row is pruned",
			stage: func(t *testing.T, s *queue.ResultStore, id string) {
				t.Helper()
				require.NoError(t, s.Complete(id, []byte("body")))
			},
			backdate:    true,
			wantSurvive: false,
		},
		{
			name: "old error row is pruned",
			stage: func(t *testing.T, s *queue.ResultStore, id string) {
				t.Helper()
				require.NoError(t, s.Fail(id, "boom"))
			},
			backdate:    true,
			wantSurvive: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, db := newTestResultStore(t)
			const id = "rid-1"
			require.NoError(t, s.Create(id))
			tt.stage(t, s, id)
			if tt.backdate {
				old := time.Now().UTC().Add(-2 * ttl).Format(time.RFC3339Nano)
				_, err := db.Exec(`UPDATE queue_results SET completed_at = ? WHERE id = ?`, old, id)
				require.NoError(t, err)
			}

			_, err := s.Cleanup(ttl)
			require.NoError(t, err)

			r, err := s.Get(id)
			require.NoError(t, err)
			if tt.wantSurvive {
				require.NotNil(t, r, "row must survive cleanup")
			} else {
				require.Nil(t, r, "row must be pruned by cleanup")
			}
		})
	}
}
