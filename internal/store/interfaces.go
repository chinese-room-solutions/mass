package store

import (
	"github.com/chinese-room-solutions/mass/internal/model"
)

// SettingsStoreInterface abstracts key-value settings storage.
type SettingsStoreInterface interface {
	// GetSetting retrieves a setting value by key. Returns "" if not found.
	GetSetting(key string) (string, error)
	// SetSetting inserts or replaces a setting.
	SetSetting(key, value string) error
	// DeleteSetting removes a setting by key.
	DeleteSetting(key string) error
}

// DownloadStoreInterface abstracts download record persistence.
type DownloadStoreInterface interface {
	// UpsertDownload inserts or replaces a download record.
	UpsertDownload(dl model.Download) error
	// UpdateProgress updates the downloaded/total bytes for a download.
	UpdateProgress(filename string, downloaded, total int64) error
	// SetStatus updates the status of a download.
	SetStatus(filename, status string) error
	// DeleteDownload removes a download record.
	DeleteDownload(filename string) error
	// ListDownloads returns all download records.
	ListDownloads() ([]model.Download, error)
}

// BenchmarkStoreInterface abstracts device benchmark result storage.
type BenchmarkStoreInterface interface {
	// SaveBenchmark upserts a benchmark result for the given agent/device pair.
	SaveBenchmark(row BenchmarkRow) error
	// GetBenchmark returns the stored benchmark for an agent/device pair.
	GetBenchmark(workerID, deviceID string) (BenchmarkRow, error)
	// ListBenchmarks returns all stored benchmark results.
	ListBenchmarks() ([]BenchmarkRow, error)
	// DeleteBenchmark removes a stored benchmark result for an agent/device pair.
	DeleteBenchmark(workerID, deviceID string) error
	// HasBenchmark returns true if a benchmark exists for the given agent/device pair.
	HasBenchmark(workerID, deviceID string) (bool, error)
}

// StoreInterface combines all storage interfaces.
//
// **SQL-backed only** — same reasoning as [queue.QueueInterface]: MASS
// relies on transactional SQL primitives (atomicity, ordering, FK
// cascades). Not portable to KV / document / non-relational backends.
//
// Today: SQLite. Postgres on the roadmap (paired migrations + dialect-
// aware timestamp helper).
type StoreInterface interface {
	SettingsStoreInterface
	DownloadStoreInterface
	BenchmarkStoreInterface
	DeviceQueueStateStoreInterface

	// Close releases resources held by the store.
	Close() error
}

// Compile-time checks that Store satisfies all interfaces.
var (
	_ SettingsStoreInterface         = (*Store)(nil)
	_ DownloadStoreInterface         = (*Store)(nil)
	_ BenchmarkStoreInterface        = (*Store)(nil)
	_ DeviceQueueStateStoreInterface = (*Store)(nil)
	_ StoreInterface                 = (*Store)(nil)
)
