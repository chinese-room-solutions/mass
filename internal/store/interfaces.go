package store

// SettingsStoreInterface abstracts key-value settings storage.
type SettingsStoreInterface interface {
	// GetSetting retrieves a setting value by key. Returns "" if not found.
	GetSetting(key string) (string, error)
	// SetSetting inserts or replaces a setting.
	SetSetting(key, value string) error
	// DeleteSetting removes a setting by key.
	DeleteSetting(key string) error
}

// BenchmarkStoreInterface abstracts device benchmark result storage.
type BenchmarkStoreInterface interface {
	// SaveBenchmark upserts a benchmark result for the given worker/device pair.
	SaveBenchmark(row BenchmarkRow) error
	// GetBenchmark returns the stored benchmark for a worker/device pair.
	GetBenchmark(workerID, deviceID string) (BenchmarkRow, error)
	// ListBenchmarks returns all stored benchmark results.
	ListBenchmarks() ([]BenchmarkRow, error)
	// DeleteBenchmark removes a stored benchmark result for a worker/device pair.
	DeleteBenchmark(workerID, deviceID string) error
	// HasBenchmark returns true if a benchmark exists for the given worker/device pair.
	HasBenchmark(workerID, deviceID string) (bool, error)
}

// RuntimeStoreInterface abstracts installed-runtime persistence. A row exists
// for every runtime gateway package installed in MASS, regardless of whether
// the gateway subprocess is currently running.
type RuntimeStoreInterface interface {
	UpsertRuntime(row RuntimeRow) error
	SetRuntimeAutoStart(runtimeName string, autoStart bool) error
	GetRuntime(runtimeName string) (RuntimeRow, error)
	ListRuntimes() ([]RuntimeRow, error)
	DeleteRuntime(runtimeName string) error
}

// WorkerDeviceEnabledStoreInterface abstracts the per-(worker, device)
// enable flag persisted by the operator-controlled toggle in the Workers
// tab. Absent rows mean "enabled" (sane default for newly-connected
// workers without any operator intent).
type WorkerDeviceEnabledStoreInterface interface {
	SetWorkerDeviceEnabled(workerID, deviceID string, enabled bool) error
	GetWorkerDeviceEnabled(workerID, deviceID string) (WorkerDeviceEnabledRow, error)
	ListWorkerDevicesEnabled(workerID string) ([]WorkerDeviceEnabledRow, error)
	SetWorkerDevicesEnabledBulk(workerID string, deviceIDs []string, enabled bool) error
}

// StoreInterface combines all storage interfaces.
//
// **SQL-backed only** — same reasoning as [queue.QueueInterface]: MASS
// relies on transactional SQL primitives (atomicity, ordering, FK
// cascades). Not portable to KV / document / non-relational backends.
//
// Today: SQLite. Postgres on the roadmap (paired migrations + dialect-
// aware timestamp helper).
// DownloadStoreInterface abstracts in-flight + paused download persistence.
// A row's identity is RelPath (the file's destination under models_dir).
// Completed rows are deleted; the file becomes a regular model the
// runtime gateway picks up on its next walk.
type DownloadStoreInterface interface {
	UpsertDownload(row DownloadRow) error
	UpdateDownloadProgress(relPath string, downloaded, total int64) error
	SetDownloadStatus(relPath, status, errorMsg string) error
	DeleteDownload(relPath string) error
	GetDownload(relPath string) (DownloadRow, error)
	ListDownloads() ([]DownloadRow, error)
}

type StoreInterface interface {
	SettingsStoreInterface
	BenchmarkStoreInterface
	DeviceQueueStateStoreInterface
	RuntimeStoreInterface
	WorkerDeviceEnabledStoreInterface
	DownloadStoreInterface

	// Close releases resources held by the store.
	Close() error
}

// Compile-time checks that Store satisfies all interfaces.
var (
	_ SettingsStoreInterface            = (*Store)(nil)
	_ BenchmarkStoreInterface           = (*Store)(nil)
	_ DeviceQueueStateStoreInterface    = (*Store)(nil)
	_ RuntimeStoreInterface             = (*Store)(nil)
	_ WorkerDeviceEnabledStoreInterface = (*Store)(nil)
	_ DownloadStoreInterface            = (*Store)(nil)
	_ StoreInterface                    = (*Store)(nil)
)
