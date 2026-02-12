package store

import "github.com/chinese-room-solutions/mass/internal/model"

// UpsertDownload inserts or replaces a download record.
func (s *Store) UpsertDownload(dl model.Download) error {
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO downloads (filename, repo_id, group_name, status, downloaded, total, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		dl.Filename, dl.RepoID, dl.GroupName, dl.Status, dl.Downloaded, dl.Total,
	)
	return err
}

// UpdateProgress updates the downloaded/total bytes for a download.
func (s *Store) UpdateProgress(filename string, downloaded, total int64) error {
	_, err := s.db.Exec(
		`UPDATE downloads SET downloaded = ?, total = ?, updated_at = CURRENT_TIMESTAMP WHERE filename = ?`,
		downloaded, total, filename,
	)
	return err
}

// SetStatus updates the status of a download.
func (s *Store) SetStatus(filename, status string) error {
	_, err := s.db.Exec(
		`UPDATE downloads SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE filename = ?`,
		status, filename,
	)
	return err
}

// DeleteDownload removes a download record.
func (s *Store) DeleteDownload(filename string) error {
	_, err := s.db.Exec(`DELETE FROM downloads WHERE filename = ?`, filename)
	return err
}

// ListDownloads returns all download records.
func (s *Store) ListDownloads() ([]model.Download, error) {
	rows, err := s.db.Query(
		`SELECT filename, repo_id, group_name, status, downloaded, total FROM downloads`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck // best-effort cleanup

	var out []model.Download
	for rows.Next() {
		var r model.Download
		if err := rows.Scan(&r.Filename, &r.RepoID, &r.GroupName, &r.Status, &r.Downloaded, &r.Total); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
