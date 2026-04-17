package worker

import (
	"errors"
	"testing"

	workerpb "github.com/chinese-room-solutions/mass-proto/gen/go/worker"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// captureSender records every sent message and can be primed to fail.
type captureSender struct {
	sent []*workerpb.HubMessage
	err  error
}

func (c *captureSender) Send(m *workerpb.HubMessage) error {
	if c.err != nil {
		return c.err
	}
	c.sent = append(c.sent, m)
	return nil
}

func newReconcileWorker(sender *captureSender) *StreamWorker {
	return NewStreamWorker(&workerpb.WorkerRegister{
		Id: "w1", Name: "w1",
	}, sender, "http://mass:3455", "/models", false, zerolog.Nop())
}

// primeCache simulates a heartbeat that reported the given file list.
func primeCache(w *StreamWorker, files []string) {
	w.mu.Lock()
	w.cacheFiles = append(w.cacheFiles[:0:0], files...)
	w.mu.Unlock()
}

func TestReconcile_NoCacheReportedYet(t *testing.T) {
	s := &captureSender{}
	w := newReconcileWorker(s)

	n, err := w.Reconcile(map[string]struct{}{"a.gguf": {}})
	require.NoError(t, err)
	require.Zero(t, n)
	require.Empty(t, s.sent, "must not send when nothing has been reported")
}

func TestReconcile_NoStaleFiles(t *testing.T) {
	s := &captureSender{}
	w := newReconcileWorker(s)
	primeCache(w, []string{"a.gguf", "b.gguf"})

	n, err := w.Reconcile(map[string]struct{}{
		"a.gguf": {}, "b.gguf": {}, "c.gguf": {},
	})
	require.NoError(t, err)
	require.Zero(t, n)
	require.Empty(t, s.sent, "must not send an empty delete list")
}

func TestReconcile_StaleFiles_SendsDelete(t *testing.T) {
	s := &captureSender{}
	w := newReconcileWorker(s)
	primeCache(w, []string{"keep.gguf", "stale1.gguf", "stale2.gguf"})

	n, err := w.Reconcile(map[string]struct{}{"keep.gguf": {}})
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Len(t, s.sent, 1)

	del := s.sent[0].GetDeleteCacheFiles()
	require.NotNil(t, del)
	require.ElementsMatch(t, []string{"stale1.gguf", "stale2.gguf"}, del.GetFilenames())
}

func TestReconcile_SenderErrorIsReturned(t *testing.T) {
	wantErr := errors.New("send failed")
	s := &captureSender{err: wantErr}
	w := newReconcileWorker(s)
	primeCache(w, []string{"stale.gguf"})

	_, err := w.Reconcile(map[string]struct{}{})
	require.ErrorIs(t, err, wantErr)
}

func TestCacheFiles_RaceSafeCopy(t *testing.T) {
	w := newReconcileWorker(&captureSender{})
	primeCache(w, []string{"a.gguf"})

	got := w.CacheFiles()
	got[0] = "MUTATED"

	// Internal slice should not be affected by mutation through the returned copy.
	again := w.CacheFiles()
	require.Equal(t, []string{"a.gguf"}, again)
}
