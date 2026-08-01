package store

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/KernelPryanic/ctxerr"
)

// RuntimeRow is a stored runtime gateway package.
type RuntimeRow struct {
	RuntimeName string
	Version     string
	DisplayName string
	Description string
	InstallPath string
	AutoStart   bool
	InstalledAt time.Time
}

// UpsertRuntime inserts or replaces a runtime row keyed by RuntimeName.
func (s *Store) UpsertRuntime(row RuntimeRow) error {
	if row.InstalledAt.IsZero() {
		row.InstalledAt = time.Now().UTC()
	}
	_, err := s.db.Exec(s.rebind(`
		INSERT INTO runtimes (runtime_name, version, display_name, description, install_path, auto_start, installed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(runtime_name) DO UPDATE SET
			version      = excluded.version,
			display_name = excluded.display_name,
			description  = excluded.description,
			install_path = excluded.install_path,
			auto_start   = excluded.auto_start,
			installed_at = excluded.installed_at`),
		row.RuntimeName, row.Version, row.DisplayName, row.Description,
		row.InstallPath, row.AutoStart, row.InstalledAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return ctxerr.With(fmt.Errorf("upserting runtime %s: %w", row.RuntimeName, err), map[string]any{"runtime_name": row.RuntimeName})
	}
	return nil
}

// SetRuntimeAutoStart toggles the auto_start flag without rewriting the row.
func (s *Store) SetRuntimeAutoStart(runtimeName string, autoStart bool) error {
	_, err := s.db.Exec(s.rebind(`UPDATE runtimes SET auto_start = ? WHERE runtime_name = ?`), autoStart, runtimeName)
	if err != nil {
		return ctxerr.With(fmt.Errorf("setting auto_start for %s: %w", runtimeName, err), map[string]any{"runtime_name": runtimeName})
	}
	return nil
}

// GetRuntime returns the row for runtimeName, or [sql.ErrNoRows] when absent.
func (s *Store) GetRuntime(runtimeName string) (RuntimeRow, error) {
	var row RuntimeRow
	var ts string
	err := s.db.QueryRow(s.rebind(`
		SELECT runtime_name, version, display_name, description, install_path, auto_start, installed_at
		FROM runtimes WHERE runtime_name = ?`), runtimeName).
		Scan(&row.RuntimeName, &row.Version, &row.DisplayName, &row.Description, &row.InstallPath, &row.AutoStart, &ts)
	if err != nil {
		return RuntimeRow{}, err
	}
	row.InstalledAt, err = parseStamp(ts)
	if err != nil {
		return RuntimeRow{}, err
	}
	return row, nil
}

// ListRuntimes returns every installed runtime ordered by display name.
func (s *Store) ListRuntimes() ([]RuntimeRow, error) {
	rows, err := s.db.Query(`
		SELECT runtime_name, version, display_name, description, install_path, auto_start, installed_at
		FROM runtimes ORDER BY display_name, runtime_name`)
	if err != nil {
		return nil, fmt.Errorf("listing runtimes: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			panic(fmt.Errorf("close rows: %w", err))
		}
	}()

	var out []RuntimeRow
	for rows.Next() {
		var r RuntimeRow
		var ts string
		if err := rows.Scan(&r.RuntimeName, &r.Version, &r.DisplayName, &r.Description, &r.InstallPath, &r.AutoStart, &ts); err != nil {
			return nil, fmt.Errorf("scanning runtime row: %w", err)
		}
		r.InstalledAt, err = parseStamp(ts)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// DeleteRuntime removes the row for runtimeName. No-op when absent.
func (s *Store) DeleteRuntime(runtimeName string) error {
	_, err := s.db.Exec(s.rebind(`DELETE FROM runtimes WHERE runtime_name = ?`), runtimeName)
	if err != nil {
		return ctxerr.With(fmt.Errorf("deleting runtime %s: %w", runtimeName, err), map[string]any{"runtime_name": runtimeName})
	}
	return nil
}

// _ keeps sql.ErrNoRows accessible for callers that import this file.
var _ = sql.ErrNoRows
