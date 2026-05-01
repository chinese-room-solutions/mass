package store

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/KernelPryanic/ctxerr"
)

// DownloadStatus values stored in the `status` column.
const (
	DownloadStatusActive = "active"
	DownloadStatusPaused = "paused"
	DownloadStatusError  = "error"
)

// DownloadRow is one persisted download. Identity is RelPath — the file's
// destination under models_dir is the natural unique key. Completed
// downloads are deleted from the table; the file then becomes a regular
// model the model store discovers on its next walk.
type DownloadRow struct {
	RelPath     string
	URL         string
	Source      string // "huggingface" | "local" | runtime-defined
	RepoID      string
	RuntimeName string
	GroupKey    string // ties siblings of one install operation together
	Status      string // DownloadStatus*
	Downloaded  int64
	Total       int64
	ErrorMsg    string
}

// UpsertDownload inserts a new row or updates the bookkeeping fields of an
// existing one. RelPath identifies the row.
func (s *Store) UpsertDownload(row DownloadRow) error {
	_, err := s.db.Exec(`
		INSERT INTO downloads (rel_path, url, source, repo_id, runtime_name, group_key, status, downloaded, total, error_msg)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(rel_path) DO UPDATE SET
			url          = excluded.url,
			source       = excluded.source,
			repo_id      = excluded.repo_id,
			runtime_name = excluded.runtime_name,
			group_key    = excluded.group_key,
			status       = excluded.status,
			downloaded   = excluded.downloaded,
			total        = excluded.total,
			error_msg    = excluded.error_msg,
			updated_at   = strftime('%Y-%m-%dT%H:%M:%fZ')`,
		row.RelPath, row.URL, row.Source, row.RepoID, row.RuntimeName, row.GroupKey,
		row.Status, row.Downloaded, row.Total, row.ErrorMsg)
	if err != nil {
		return ctxerr.With(fmt.Errorf("upserting download: %w", err), map[string]any{"rel_path": row.RelPath})
	}
	return nil
}

// UpdateDownloadProgress updates the byte counters (no status change).
// Hot path — keep tiny and unlocked.
func (s *Store) UpdateDownloadProgress(relPath string, downloaded, total int64) error {
	_, err := s.db.Exec(`
		UPDATE downloads SET downloaded = ?, total = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ')
		WHERE rel_path = ?`, downloaded, total, relPath)
	if err != nil {
		return ctxerr.With(fmt.Errorf("updating download progress: %w", err), map[string]any{"rel_path": relPath})
	}
	return nil
}

// SetDownloadStatus updates only the status (and clears error_msg unless
// the new status is "error"). Used by pause/resume/error transitions.
func (s *Store) SetDownloadStatus(relPath, status, errorMsg string) error {
	_, err := s.db.Exec(`
		UPDATE downloads SET status = ?, error_msg = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ')
		WHERE rel_path = ?`, status, errorMsg, relPath)
	if err != nil {
		return ctxerr.With(fmt.Errorf("setting download status: %w", err),
			map[string]any{"rel_path": relPath, "status": status})
	}
	return nil
}

// DeleteDownload removes the row for relPath. Returns nil when the row
// did not exist (idempotent — completion + cancel + boot recovery races
// can all try to delete the same row).
func (s *Store) DeleteDownload(relPath string) error {
	_, err := s.db.Exec(`DELETE FROM downloads WHERE rel_path = ?`, relPath)
	if err != nil {
		return ctxerr.With(fmt.Errorf("deleting download: %w", err), map[string]any{"rel_path": relPath})
	}
	return nil
}

// GetDownload returns the row for relPath, or [sql.ErrNoRows] when absent.
func (s *Store) GetDownload(relPath string) (DownloadRow, error) {
	var row DownloadRow
	err := s.db.QueryRow(`
		SELECT rel_path, url, source, repo_id, runtime_name, group_key, status, downloaded, total, error_msg
		FROM downloads WHERE rel_path = ?`, relPath).
		Scan(&row.RelPath, &row.URL, &row.Source, &row.RepoID, &row.RuntimeName, &row.GroupKey,
			&row.Status, &row.Downloaded, &row.Total, &row.ErrorMsg)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return DownloadRow{}, err
		}
		return DownloadRow{}, ctxerr.With(fmt.Errorf("reading download: %w", err), map[string]any{"rel_path": relPath})
	}
	return row, nil
}

// ListDownloads returns every persisted download in created-at order.
func (s *Store) ListDownloads() ([]DownloadRow, error) {
	rows, err := s.db.Query(`
		SELECT rel_path, url, source, repo_id, runtime_name, group_key, status, downloaded, total, error_msg
		FROM downloads ORDER BY created_at`)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("listing downloads: %w", err), nil)
	}
	defer func() { _ = rows.Close() }()
	var out []DownloadRow
	for rows.Next() {
		var r DownloadRow
		if err := rows.Scan(&r.RelPath, &r.URL, &r.Source, &r.RepoID, &r.RuntimeName, &r.GroupKey,
			&r.Status, &r.Downloaded, &r.Total, &r.ErrorMsg); err != nil {
			return nil, fmt.Errorf("scanning download row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating downloads: %w", err)
	}
	return out, nil
}
