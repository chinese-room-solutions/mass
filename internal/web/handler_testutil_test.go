package web

import (
	"path/filepath"
	"testing"

	"github.com/chinese-room-solutions/mass/internal/config"
	"github.com/chinese-room-solutions/mass/internal/downloads"
	"github.com/chinese-room-solutions/mass/internal/runtimes"
	"github.com/chinese-room-solutions/mass/internal/scheduler"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/internal/worker"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// newTestHandler builds a Handler backed by a real temp-dir SQLite store, a
// real runtimes manager (no gateways installed), an empty worker fleet, and a
// bare scheduler (queue subsystem not wired — enough to exercise handler
// validation/dispatch branches). It hits the actual mux, middleware, and SSE
// code paths the browser uses, so UI flows are covered without a live server.
func newTestHandler(t *testing.T) *Handler {
	t.Helper()

	dataDir := t.TempDir()
	st, err := store.Open(store.DialectSQLite, filepath.Join(dataDir, "test.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	logger := zerolog.Nop()
	rm, err := runtimes.NewManager(dataDir, st, logger)
	require.NoError(t, err)

	cfg := &config.Config{DataDir: dataDir, Theme: "dark"}
	fleet := worker.NewFleet()
	sched := scheduler.New(cfg, logger, fleet)
	dl := downloads.NewManager(st, filepath.Join(dataDir, "models"), logger)

	h, err := NewHandler(HandlerOptions{
		Config:    cfg,
		Scheduler: sched,
		Runtimes:  rm,
		Store:     st,
		Logger:    logger,
		Workers:   fleet,
		Downloads: dl,
		Enroller:  worker.NewEnroller(st),
		DataDir:   dataDir,
	})
	require.NoError(t, err)
	return h
}
