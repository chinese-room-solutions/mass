// Package downloads owns MASS's view of in-flight model file downloads.
// One [Manager] coordinates the queue, persists each download to SQLite so
// transfers survive restarts, and broadcasts progress events to UI
// subscribers. The actual byte-pumping is delegated to [pkg/download.Manager].
//
// Identity is the file's destination path relative to models_dir — the
// natural unique key, since two installs targeting the same file would
// collide on disk anyway. Companion files (mmproj alongside a chat GGUF)
// are tracked as separate rows tied together by GroupKey, so the UI can
// render them as one operation.
//
// On boot, every persisted row is loaded back as paused. The user clicks
// Resume to pick up partially-downloaded files; pkg/download.Manager
// handles HTTP Range continuation transparently.
package downloads

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/KernelPryanic/ctxerr"
	"github.com/chinese-room-solutions/mass/internal/store"
	"github.com/chinese-room-solutions/mass/pkg/download"
	"github.com/rs/zerolog"
)

// Errors callers can match.
var (
	ErrAlreadyExists = errors.New("download already exists for this file")
	ErrNotFound      = errors.New("download not found")
	ErrNotPaused     = errors.New("download is not paused")
	ErrAlreadyDone   = errors.New("file already exists at destination")
)

// Persistence-cadence: persist progress no more than once every 5s to avoid
// burning IO on hot downloads.
const persistInterval = 5 * time.Second

// maxConcurrent caps how many transfer goroutines (HTTP downloads + local
// imports) move bytes at once. The rest queue on the manager's semaphore
// as "active" jobs; the semaphore acquire selects against the job's
// context so Pause/Cancel stay responsive while queued.
const maxConcurrent = 3

// Job describes one download known to the Manager. It mirrors
// [store.DownloadRow] plus runtime-only fields (cancel func, paused flag).
// Returned in snapshot form via [Manager.List].
type Job struct {
	RelPath     string
	URL         string
	Source      string
	RepoID      string
	RuntimeName string
	GroupKey    string
	GroupName   string // human-readable label the UI uses to group rows (the operator-typed model name)
	Filename    string // human-readable filename (basename of RelPath, no extension)
	Status      string // store.DownloadStatus*
	Downloaded  int64
	Total       int64
	ErrorMsg    string
}

// Event is one progress / lifecycle frame fanned out to subscribers.
type Event struct {
	RelPath    string
	GroupKey   string
	GroupName  string
	Filename   string
	Quant      string
	Status     string // "started" | "progress" | "paused" | "resumed" | "done" | "error" | "cancelled"
	Downloaded int64
	Total      int64
	ErrorMsg   string
}

// Manager owns the lifecycle of every active download.
type Manager struct {
	store     store.DownloadStoreInterface
	modelsDir string
	dl        *download.Manager
	logger    zerolog.Logger

	mu   sync.Mutex
	jobs map[string]*runtimeJob // key = rel_path

	// sem bounds concurrent transfers at maxConcurrent.
	sem chan struct{}

	// Subscribers + their fan-out channel; protected by subMu.
	subMu sync.Mutex
	subs  map[chan Event]struct{}
}

// runtimeJob holds the in-process state for one download — cancellable
// context + last-persist marker + the rendered Job snapshot. The store
// row is the source of truth for restart-survival; this struct is the
// source of truth while the process is running.
//
// done is closed by the runner goroutine on exit. Cancel waits on it
// (with a short timeout) before removing the temp file — otherwise the
// runner's still-open file handle keeps the file pinned on Windows and
// the os.Remove call no-ops, leaving the partial bytes around for a
// subsequent Get to resume from. nil when no runner is active.
type runtimeJob struct {
	mu          sync.Mutex
	job         Job
	cancel      context.CancelFunc // cancels the in-flight HTTP request; no-op when paused
	done        chan struct{}      // closed by runner goroutine on exit
	lastPersist time.Time
}

// NewManager builds a Manager backed by the given store. Call
// [Manager.Recover] once after construction (before serving HTTP) to
// restore persisted rows from the previous session.
func NewManager(s store.DownloadStoreInterface, modelsDir string, logger zerolog.Logger) *Manager {
	return &Manager{
		store:     s,
		modelsDir: modelsDir,
		dl:        download.NewManager(nil),
		logger:    logger.With().Str("component", "downloads").Logger(),
		jobs:      map[string]*runtimeJob{},
		sem:       make(chan struct{}, maxConcurrent),
		subs:      map[chan Event]struct{}{},
	}
}

// Recover reads every persisted download from the store and restores it as
// paused. Stale rows whose temp file no longer exists (and whose destination
// file isn't already present) are removed. The operator clicks Resume to
// pick up; recovery never auto-resumes.
func (m *Manager) Recover() {
	rows, err := m.store.ListDownloads()
	if err != nil {
		m.logger.Error().Err(err).Msg("loading persisted downloads")
		return
	}
	for _, row := range rows {
		destPath := filepath.Join(m.modelsDir, filepath.FromSlash(row.RelPath))
		tempPath := download.TempFilePath(destPath, "")
		if _, err := os.Stat(tempPath); err != nil {
			// Maybe the download finished after the last DB write.
			if _, finalErr := os.Stat(destPath); finalErr != nil {
				m.logger.Info().Str("rel_path", row.RelPath).Msg("removing stale download row (no temp + no final)")
			} else {
				m.logger.Info().Str("rel_path", row.RelPath).Msg("removing download row (file is already complete)")
			}
			if delErr := m.store.DeleteDownload(row.RelPath); delErr != nil {
				m.logger.Warn().Err(delErr).Str("rel_path", row.RelPath).Msg("deleting stale download row")
			}
			continue
		}
		// Restore as paused. Status text becomes "paused" regardless of
		// what the row said before the restart — anything in-flight at
		// shutdown is stopped; anything errored stays errored.
		newStatus := store.DownloadStatusPaused
		if row.Status == store.DownloadStatusError {
			newStatus = store.DownloadStatusError
		}
		row.Status = newStatus
		rj := &runtimeJob{job: jobFromRow(row), cancel: func() {}}
		m.mu.Lock()
		m.jobs[row.RelPath] = rj
		m.mu.Unlock()

		if err := m.store.SetDownloadStatus(row.RelPath, newStatus, row.ErrorMsg); err != nil {
			m.logger.Warn().Err(err).Str("rel_path", row.RelPath).Msg("updating recovered download status")
		}
		m.logger.Info().Str("rel_path", row.RelPath).Int64("downloaded", row.Downloaded).
			Int64("total", row.Total).Msg("recovered download (paused)")
	}
}

// Start kicks off a new download. Returns [ErrAlreadyExists] if relPath is
// already tracked (active or paused), [ErrAlreadyDone] if the destination
// file already exists on disk, or [ErrInvalidRelPath] when relPath would
// escape models_dir.
func (m *Manager) Start(spec Job) error {
	if spec.URL == "" {
		return ctxerr.With(fmt.Errorf("Start: url is required"), nil)
	}
	if err := ValidateRelPath(spec.RelPath); err != nil {
		return err
	}
	destPath := filepath.Join(m.modelsDir, filepath.FromSlash(spec.RelPath))
	if _, err := os.Stat(destPath); err == nil {
		return ErrAlreadyDone
	}

	m.mu.Lock()
	if _, ok := m.jobs[spec.RelPath]; ok {
		m.mu.Unlock()
		return ErrAlreadyExists
	}
	if spec.Status == "" {
		spec.Status = store.DownloadStatusActive
	}
	rj := &runtimeJob{job: spec}
	m.jobs[spec.RelPath] = rj
	m.mu.Unlock()

	if err := m.store.UpsertDownload(rowFromJob(spec)); err != nil {
		m.logger.Warn().Err(err).Str("rel_path", spec.RelPath).Msg("persisting new download")
	}
	m.broadcast(rj.snapshotEvent("started", 0, spec.Total, ""))

	go m.run(rj)
	return nil
}

// Pause cancels the in-flight HTTP request for relPath. The temp file is
// preserved; Resume continues from the last byte. Returns [ErrNotFound]
// when no such download exists.
func (m *Manager) Pause(relPath string) error {
	m.mu.Lock()
	rj, ok := m.jobs[relPath]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}

	rj.mu.Lock()
	if rj.job.Status != store.DownloadStatusActive {
		rj.mu.Unlock()
		return nil // already paused / errored — idempotent
	}
	rj.cancel() // stops the HTTP request; partial temp file stays
	rj.job.Status = store.DownloadStatusPaused
	downloaded, total := rj.job.Downloaded, rj.job.Total
	rj.mu.Unlock()

	if err := m.store.SetDownloadStatus(relPath, store.DownloadStatusPaused, ""); err != nil {
		m.logger.Warn().Err(err).Str("rel_path", relPath).Msg("persisting pause")
	}
	if err := m.store.UpdateDownloadProgress(relPath, downloaded, total); err != nil {
		m.logger.Warn().Err(err).Str("rel_path", relPath).Msg("persisting progress on pause")
	}
	m.broadcast(rj.snapshotEvent("paused", downloaded, total, ""))
	return nil
}

// Resume restarts a paused download. The HTTP Range header carries the
// byte offset; partial files are picked up where they left off. Returns
// [ErrNotFound] when no such download exists, [ErrNotPaused] when it's
// not in a resumable state.
func (m *Manager) Resume(relPath string) error {
	m.mu.Lock()
	rj, ok := m.jobs[relPath]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}

	rj.mu.Lock()
	if rj.job.Status != store.DownloadStatusPaused && rj.job.Status != store.DownloadStatusError {
		rj.mu.Unlock()
		return ErrNotPaused
	}
	rj.job.Status = store.DownloadStatusActive
	rj.job.ErrorMsg = ""
	downloaded, total := rj.job.Downloaded, rj.job.Total
	rj.mu.Unlock()

	if err := m.store.SetDownloadStatus(relPath, store.DownloadStatusActive, ""); err != nil {
		m.logger.Warn().Err(err).Str("rel_path", relPath).Msg("persisting resume")
	}
	m.broadcast(rj.snapshotEvent("resumed", downloaded, total, ""))

	go m.run(rj)
	return nil
}

// Cancel removes the download entirely: stops the in-flight transfer if
// any, removes the partial temp file, and deletes the persisted row.
// Returns [ErrNotFound] when no such download exists.
func (m *Manager) Cancel(relPath string) error {
	m.mu.Lock()
	rj, ok := m.jobs[relPath]
	if ok {
		delete(m.jobs, relPath)
	}
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}

	rj.mu.Lock()
	if rj.cancel != nil {
		rj.cancel()
	}
	done := rj.done
	rj.mu.Unlock()

	// Wait for the runner goroutine to release the file handle. Without
	// this, the Windows os.Remove call below silently fails because the
	// goroutine still holds the file open — leaving partial bytes that a
	// subsequent Get would resume from.
	if done != nil {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			m.logger.Warn().Str("rel_path", relPath).Msg("runner did not exit within 5s; removing temp file anyway")
		}
	}

	destPath := filepath.Join(m.modelsDir, filepath.FromSlash(relPath))
	tempPath := download.TempFilePath(destPath, "")
	if err := os.Remove(tempPath); err != nil && !os.IsNotExist(err) {
		m.logger.Warn().Err(err).Str("rel_path", relPath).Msg("removing partial temp file on cancel")
	}
	if err := m.store.DeleteDownload(relPath); err != nil {
		m.logger.Warn().Err(err).Str("rel_path", relPath).Msg("deleting download row on cancel")
	}
	m.broadcast(rj.snapshotEvent("cancelled", 0, 0, ""))
	return nil
}

// List returns a snapshot of every tracked download. Order is not
// guaranteed; callers sort if needed.
func (m *Manager) List() []Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Job, 0, len(m.jobs))
	for _, rj := range m.jobs {
		rj.mu.Lock()
		out = append(out, rj.job)
		rj.mu.Unlock()
	}
	return out
}

// Subscribe returns a channel that receives every subsequent event. The
// caller MUST eventually call [Manager.Unsubscribe].
func (m *Manager) Subscribe() chan Event {
	ch := make(chan Event, 64)
	m.subMu.Lock()
	m.subs[ch] = struct{}{}
	m.subMu.Unlock()
	return ch
}

// Unsubscribe removes ch and closes it.
func (m *Manager) Unsubscribe(ch chan Event) {
	m.subMu.Lock()
	delete(m.subs, ch)
	m.subMu.Unlock()
	close(ch)
}

func (m *Manager) broadcast(evt Event) {
	m.subMu.Lock()
	defer m.subMu.Unlock()
	for ch := range m.subs {
		select {
		case ch <- evt:
		default:
			m.logger.Debug().Str("rel_path", evt.RelPath).Msg("dropping download event for slow subscriber")
		}
	}
}

// snapshotEvent returns an Event populated with the job's static fields
// (group/filename/quant) plus the supplied status + counters. Keeps every
// broadcast site consistent without 11 copy-paste expansions.
func (rj *runtimeJob) snapshotEvent(status string, downloaded, total int64, errMsg string) Event {
	rj.mu.Lock()
	defer rj.mu.Unlock()
	return Event{
		RelPath:    rj.job.RelPath,
		GroupKey:   rj.job.GroupKey,
		GroupName:  rj.job.GroupName,
		Filename:   rj.job.Filename,
		Status:     status,
		Downloaded: downloaded,
		Total:      total,
		ErrorMsg:   errMsg,
	}
}

// run executes one download attempt. Called fresh on Start and Resume.
// Errors classify into:
//   - context cancellation (user paused) → no-op, the Pause/Cancel
//     handler already updated state.
//   - transport / 4xx / 5xx after retries → mark errored, broadcast.
//   - success → delete row + broadcast done.
func (m *Manager) run(rj *runtimeJob) {
	rj.mu.Lock()
	relPath := rj.job.RelPath
	url := rj.job.URL
	rj.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	rj.mu.Lock()
	rj.cancel = cancel
	rj.done = done
	rj.mu.Unlock()
	defer close(done)

	// Wait for a transfer slot. Cancellable so Pause/Cancel (which fire
	// rj.cancel) release a still-queued job immediately.
	select {
	case m.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-m.sem }()

	destPath := filepath.Join(m.modelsDir, filepath.FromSlash(relPath))
	var lastPctReported int64 = -1

	progressFn := func(downloaded, total int64) {
		now := time.Now()
		rj.mu.Lock()
		rj.job.Downloaded = downloaded
		if total > 0 {
			rj.job.Total = total
		}
		shouldPersist := now.Sub(rj.lastPersist) >= persistInterval
		if shouldPersist {
			rj.lastPersist = now
		}
		t := rj.job.Total
		rj.mu.Unlock()

		if shouldPersist {
			if err := m.store.UpdateDownloadProgress(relPath, downloaded, t); err != nil {
				m.logger.Warn().Err(err).Str("rel_path", relPath).Msg("persisting progress")
			}
		}
		// Throttle SSE broadcasts to one per percent so we don't spam
		// every byte-counter callback.
		if t > 0 {
			pct := 100 * downloaded / t
			if pct == lastPctReported {
				return
			}
			lastPctReported = pct
		}
		m.broadcast(rj.snapshotEvent("progress", downloaded, t, ""))
	}

	err := m.dl.Download(ctx, url, destPath,
		download.WithResume(true),
		download.WithMaxRetries(3),
		download.WithProgress(progressFn),
	)
	if err != nil {
		if ctx.Err() != nil {
			// Paused or cancelled. Pause/Cancel already broadcast + persisted.
			return
		}
		m.logger.Error().Err(err).Str("rel_path", relPath).Msg("download failed")
		rj.mu.Lock()
		rj.job.Status = store.DownloadStatusError
		rj.job.ErrorMsg = err.Error()
		rj.mu.Unlock()
		if perr := m.store.SetDownloadStatus(relPath, store.DownloadStatusError, err.Error()); perr != nil {
			m.logger.Warn().Err(perr).Str("rel_path", relPath).Msg("persisting error status")
		}
		m.broadcast(rj.snapshotEvent("error", 0, 0, err.Error()))
		return
	}

	// Success — drop the row, drop the runtime entry, broadcast done.
	m.mu.Lock()
	delete(m.jobs, relPath)
	m.mu.Unlock()
	if err := m.store.DeleteDownload(relPath); err != nil {
		m.logger.Warn().Err(err).Str("rel_path", relPath).Msg("deleting completed download row")
	}
	m.logger.Info().Str("rel_path", relPath).Msg("download complete")
	m.broadcast(rj.snapshotEvent("done", rj.job.Total, rj.job.Total, ""))
}

// LocalImportLabels supplies the human-readable group/file labels for
// an import. Computed by the caller; the manager just attaches them
// to the Job for SSE clients.
type LocalImportLabels struct {
	GroupName string
	Filename  string
}

// ImportLocal copies srcPath into models_dir at the caller-supplied
// relPath. Reports progress through the same broker as HTTP downloads.
// Returns the relPath of the imported file or an error.
//
// relPath is forward-slash, relative to models_dir, and must already be
// the canonical destination form
// (e.g. "gguf/cjpais-llava-1.6-mistral-7b-gguf-Q4_K_M.gguf"). The caller
// owns canonical-name computation since it knows the format and the
// owner/model split. The manager only performs the copy.
//
// Refuses copy-into-self when the source is already at relPath (that's
// just a no-op).
func (m *Manager) ImportLocal(srcPath, relPath, runtimeName string, labels LocalImportLabels) (string, error) {
	if runtimeName == "" {
		return "", ctxerr.With(fmt.Errorf("runtime name is required"), map[string]any{"path": srcPath})
	}
	if err := ValidateRelPath(relPath); err != nil {
		return "", err
	}
	srcInfo, err := os.Stat(srcPath)
	if err != nil {
		return "", ctxerr.With(fmt.Errorf("stat source: %w", err), map[string]any{"path": srcPath})
	}
	if srcInfo.IsDir() {
		return "", ctxerr.With(fmt.Errorf("source is a directory, not a file"), map[string]any{"path": srcPath})
	}
	srcAbs, err := filepath.Abs(srcPath)
	if err != nil {
		return "", ctxerr.With(fmt.Errorf("resolving source: %w", err), map[string]any{"path": srcPath})
	}
	destPath := filepath.Join(append([]string{m.modelsDir}, strings.Split(relPath, "/")...)...)
	destAbs, _ := filepath.Abs(destPath)
	if destAbs != "" && srcAbs == destAbs {
		// Source already at the destination — no copy needed.
		return relPath, nil
	}
	if _, err := os.Stat(destPath); err == nil {
		return "", ErrAlreadyDone
	}

	m.mu.Lock()
	if _, ok := m.jobs[relPath]; ok {
		m.mu.Unlock()
		return "", ErrAlreadyExists
	}
	groupKey := relPath
	job := Job{
		RelPath:     relPath,
		URL:         "file://" + srcPath,
		Source:      "local",
		RuntimeName: runtimeName,
		GroupKey:    groupKey,
		GroupName:   labels.GroupName,
		Filename:    labels.Filename,
		Status:      store.DownloadStatusActive,
		Total:       srcInfo.Size(),
		Downloaded:  0,
	}
	rj := &runtimeJob{job: job}
	m.jobs[relPath] = rj
	m.mu.Unlock()

	if err := m.store.UpsertDownload(rowFromJob(job)); err != nil {
		m.logger.Warn().Err(err).Str("rel_path", relPath).Msg("persisting local import")
	}
	m.broadcast(rj.snapshotEvent("started", 0, srcInfo.Size(), ""))

	go m.copyLocal(rj, srcPath, destPath)
	return relPath, nil
}

// copyLocal mirrors run() but copies bytes from a local file. Progress is
// reported on every chunk so the UI feels responsive on slow disks.
func (m *Manager) copyLocal(rj *runtimeJob, srcPath, destPath string) {
	rj.mu.Lock()
	relPath := rj.job.RelPath
	total := rj.job.Total
	rj.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	rj.mu.Lock()
	rj.cancel = cancel
	rj.done = done
	rj.mu.Unlock()
	defer close(done)

	// Wait for a transfer slot (same budget as HTTP downloads).
	// Cancellable so Pause/Cancel release a still-queued import immediately.
	select {
	case m.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-m.sem }()

	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		m.failJob(rj, err)
		return
	}
	src, err := os.Open(srcPath)
	if err != nil {
		m.failJob(rj, err)
		return
	}
	defer func() { _ = src.Close() }()

	tempPath := download.TempFilePath(destPath, "")
	dst, err := os.Create(tempPath)
	if err != nil {
		m.failJob(rj, err)
		return
	}

	buf := make([]byte, 512*1024)
	var copied int64
	var lastPctReported int64 = -1
	for {
		select {
		case <-ctx.Done():
			_ = dst.Close()
			// Pause/Cancel handler took care of persistence + broadcast.
			return
		default:
		}
		n, rerr := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				_ = dst.Close()
				m.failJob(rj, werr)
				return
			}
			copied += int64(n)
			rj.mu.Lock()
			rj.job.Downloaded = copied
			rj.mu.Unlock()
			if total > 0 {
				pct := 100 * copied / total
				if pct != lastPctReported {
					lastPctReported = pct
					m.broadcast(rj.snapshotEvent("progress", copied, total, ""))
				}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = dst.Close()
			m.failJob(rj, rerr)
			return
		}
	}
	if err := dst.Close(); err != nil {
		m.failJob(rj, err)
		return
	}
	if err := os.Rename(tempPath, destPath); err != nil {
		m.failJob(rj, err)
		return
	}
	m.mu.Lock()
	delete(m.jobs, relPath)
	m.mu.Unlock()
	if err := m.store.DeleteDownload(relPath); err != nil {
		m.logger.Warn().Err(err).Str("rel_path", relPath).Msg("deleting completed import row")
	}
	m.logger.Info().Str("rel_path", relPath).Msg("import complete")
	m.broadcast(rj.snapshotEvent("done", total, total, ""))
}

// failJob is the shared error-tail for run() and copyLocal().
func (m *Manager) failJob(rj *runtimeJob, err error) {
	rj.mu.Lock()
	relPath := rj.job.RelPath
	rj.job.Status = store.DownloadStatusError
	rj.job.ErrorMsg = err.Error()
	rj.mu.Unlock()
	if perr := m.store.SetDownloadStatus(relPath, store.DownloadStatusError, err.Error()); perr != nil {
		m.logger.Warn().Err(perr).Str("rel_path", relPath).Msg("persisting error status")
	}
	m.logger.Error().Err(err).Str("rel_path", relPath).Msg("import failed")
	m.broadcast(rj.snapshotEvent("error", 0, 0, err.Error()))
}

// jobFromRow / rowFromJob are local conversions between persistence shape
// and runtime shape. Kept here (and not on store.DownloadRow) to keep
// the store package free of any "downloads internal" state.
func jobFromRow(r store.DownloadRow) Job {
	return Job{
		RelPath:     r.RelPath,
		URL:         r.URL,
		Source:      r.Source,
		RepoID:      r.RepoID,
		RuntimeName: r.RuntimeName,
		GroupKey:    r.GroupKey,
		Status:      r.Status,
		Downloaded:  r.Downloaded,
		Total:       r.Total,
		ErrorMsg:    r.ErrorMsg,
	}
}

func rowFromJob(j Job) store.DownloadRow {
	return store.DownloadRow{
		RelPath:     j.RelPath,
		URL:         j.URL,
		Source:      j.Source,
		RepoID:      j.RepoID,
		RuntimeName: j.RuntimeName,
		GroupKey:    j.GroupKey,
		Status:      j.Status,
		Downloaded:  j.Downloaded,
		Total:       j.Total,
		ErrorMsg:    j.ErrorMsg,
	}
}

// RemoveLocal deletes the given store-relative files from models_dir,
// cancelling any in-flight job for them first, and drops their download
// rows. Used to execute a model delete: the runtime decides which files
// constitute the model, MASS removes them here. Best-effort per file;
// returns the first hard error. Empty group directories are pruned.
func (m *Manager) RemoveLocal(relPaths []string) error {
	var firstErr error
	for _, relPath := range relPaths {
		if err := ValidateRelPath(relPath); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// Cancel an in-flight import for this file so its goroutine
		// releases the handle before we unlink (Windows).
		if err := m.Cancel(relPath); err != nil && !errors.Is(err, ErrNotFound) {
			m.logger.Warn().Err(err).Str("rel_path", relPath).Msg("cancelling in-flight job before remove")
		}
		dest := filepath.Join(m.modelsDir, filepath.FromSlash(relPath))
		// A store entry may denote a single file or a directory subtree
		// (directory-shaped models: ONNX, vLLM, diffusion, …). Remove the
		// whole subtree when the resolved path is a directory, the file
		// otherwise. ValidateRelPath above already guarantees dest stays
		// inside modelsDir, so RemoveAll can't escape the store.
		remove := os.Remove
		if info, statErr := os.Stat(dest); statErr == nil && info.IsDir() {
			remove = os.RemoveAll
		}
		if err := remove(dest); err != nil && !os.IsNotExist(err) {
			if firstErr == nil {
				firstErr = ctxerr.With(fmt.Errorf("removing %s: %w", relPath, err), map[string]any{"rel_path": relPath})
			}
			continue
		}
		if err := m.store.DeleteDownload(relPath); err != nil {
			m.logger.Warn().Err(err).Str("rel_path", relPath).Msg("deleting download row on remove")
		}
		// Prune the now-empty group directory (ignore if not empty / gone).
		if dir := filepath.Dir(dest); dir != m.modelsDir {
			_ = os.Remove(dir)
		}
	}
	return firstErr
}
