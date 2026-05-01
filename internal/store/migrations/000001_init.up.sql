-- Key-value settings.
CREATE TABLE settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- goqite message queue (SQLite flavor).
CREATE TABLE goqite (
    id       TEXT PRIMARY KEY DEFAULT ('m_' || lower(hex(randomblob(16)))),
    created  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    updated  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    queue    TEXT NOT NULL,
    body     BLOB NOT NULL,
    timeout  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    received INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE TRIGGER goqite_updated_timestamp AFTER UPDATE ON goqite BEGIN
    UPDATE goqite SET updated = strftime('%Y-%m-%dT%H:%M:%fZ') WHERE id = old.id;
END;

CREATE INDEX goqite_queue_priority_created_idx ON goqite (queue, priority DESC, created);

-- Job results cache. Keyed by goqite message ID; gateways poll for completion
-- to receive the worker's final response (or stream chunks live via the
-- scheduler callback). body is gateway-defined opaque bytes.
CREATE TABLE queue_results (
    id           TEXT PRIMARY KEY,
    status       TEXT NOT NULL DEFAULT 'pending',
    body         BLOB,
    error        TEXT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    completed_at TEXT
);

CREATE INDEX queue_results_created_at_idx ON queue_results (created_at);

-- Device queue state (tail tracking for scheduler dispatch).
CREATE TABLE device_queue_state (
    queue_name      TEXT PRIMARY KEY,
    worker_id       TEXT NOT NULL,
    device_ids      TEXT NOT NULL,
    tail_hash       TEXT NOT NULL DEFAULT '',
    tail_length     INTEGER NOT NULL DEFAULT 0,
    tail_difficulty REAL NOT NULL DEFAULT 0,
    loaded_hash     TEXT NOT NULL DEFAULT '',
    enabled         BOOLEAN NOT NULL DEFAULT 1,
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ'))
);

-- Device benchmark results (keyed by worker + device).
CREATE TABLE device_benchmarks (
    worker_id        TEXT NOT NULL,
    device_id       TEXT NOT NULL,
    device_name     TEXT NOT NULL,
    memory_gbs      REAL NOT NULL,
    compute_gflops  REAL NOT NULL DEFAULT 0,
    benched_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    PRIMARY KEY (worker_id, device_id)
);

-- Installed runtime gateway packages. Versioned, persistent across restarts.
-- auto_start = 1 means main() launches this gateway during boot; otherwise
-- it stays dormant until the operator clicks Start.
CREATE TABLE runtimes (
    runtime_name   TEXT PRIMARY KEY,
    version        TEXT NOT NULL,
    display_name   TEXT NOT NULL DEFAULT '',
    description    TEXT NOT NULL DEFAULT '',
    install_path   TEXT NOT NULL,
    auto_start     BOOLEAN NOT NULL DEFAULT 0,
    installed_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ'))
);

-- Per-(worker, device) enable flag. Keeps "is this device allowed to host
-- new model loads on its worker?" — operator-controlled via the Workers
-- tab. Already-loaded models are unaffected. Absent row = enabled (sane
-- default for newly-connected workers without operator intent).
CREATE TABLE worker_device_enabled (
    worker_id  TEXT NOT NULL,
    device_id  TEXT NOT NULL,
    enabled    BOOLEAN NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    PRIMARY KEY (worker_id, device_id)
);

CREATE INDEX worker_device_enabled_worker_idx ON worker_device_enabled (worker_id);

-- In-flight + paused model downloads. One row per (rel_path) — the
-- destination under models_dir is the natural identity. status is one of
-- "active" | "paused" | "error". Completed downloads are deleted from
-- this table; the file itself becomes a regular model the model store
-- discovers on its next walk. group_key ties files of the same install
-- together (e.g. primary + its mmproj companion) so the UI can render
-- them as one operation.
CREATE TABLE downloads (
    rel_path     TEXT PRIMARY KEY,
    url          TEXT NOT NULL,
    source       TEXT NOT NULL,         -- "huggingface" | "local" | runtime-defined
    repo_id      TEXT NOT NULL DEFAULT '',
    runtime_name TEXT NOT NULL DEFAULT '',
    group_key    TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'active',
    downloaded   INTEGER NOT NULL DEFAULT 0,
    total        INTEGER NOT NULL DEFAULT 0,
    error_msg    TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    updated_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ'))
);

CREATE INDEX downloads_group_key_idx ON downloads (group_key);
CREATE INDEX downloads_status_idx ON downloads (status);
