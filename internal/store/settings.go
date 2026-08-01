package store

import "database/sql"

// GetSetting retrieves a setting value by key. Returns "" if not found.
func (s *Store) GetSetting(key string) (string, error) {
	var value string
	err := s.db.QueryRow(s.rebind(`SELECT value FROM settings WHERE key = ?`), key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

// SetSetting inserts or replaces a setting.
func (s *Store) SetSetting(key, value string) error {
	_, err := s.db.Exec(
		s.rebind(`INSERT INTO settings (key, value) VALUES (?, ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value`),
		key, value,
	)
	return err
}

// DeleteSetting removes a setting by key.
func (s *Store) DeleteSetting(key string) error {
	_, err := s.db.Exec(s.rebind(`DELETE FROM settings WHERE key = ?`), key)
	return err
}
