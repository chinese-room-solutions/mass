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

-- Worker queue state (tail tracking for scheduler dispatch).
CREATE TABLE worker_queue_state (
    queue_name      TEXT PRIMARY KEY,
    worker_id       TEXT NOT NULL,
    device_ids      TEXT NOT NULL,
    tail_seconds    REAL NOT NULL DEFAULT 0,
    tail_model_id   TEXT NOT NULL DEFAULT '',
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ'))
);

-- Device benchmark results (keyed by worker + device). flops is the
-- device's generic matmul throughput. These rows describe HARDWARE and
-- are display-only: job estimates come from model_benchmarks, never
-- from here.
CREATE TABLE device_benchmarks (
    worker_id       TEXT NOT NULL,
    device_id       TEXT NOT NULL,
    device_name     TEXT NOT NULL,
    memory_gbs      REAL NOT NULL,
    load_gbs        REAL NOT NULL DEFAULT 0,
    flops           REAL NOT NULL DEFAULT 0,
    benched_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    PRIMARY KEY (worker_id, device_id)
);

-- Measured benchmark per (worker, device set, model). device_set is the
-- canonical sorted comma-joined device-id list one load occupies (e.g.
-- "gpu:0") — the worker owns placement, so MASS mirrors its rule rather
-- than choosing. error IS NULL means the measurements are usable; error
-- set means this device set is incapable of this model and the
-- measurements are zero. No row means the bench hasn't concluded.
-- model_size/model_mtime are the file the row was measured against: a
-- mismatch against the current file invalidates the row.
CREATE TABLE model_benchmarks (
    worker_id      TEXT NOT NULL,
    device_set     TEXT NOT NULL,
    model_id       TEXT NOT NULL,
    units_per_sec  REAL NOT NULL,
    graph_secs     REAL NOT NULL,
    base_bytes     INTEGER NOT NULL,
    per_slot_bytes INTEGER NOT NULL,
    model_size     INTEGER NOT NULL,
    model_mtime    INTEGER NOT NULL,
    error          TEXT,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL,
    PRIMARY KEY (worker_id, device_set, model_id)
);

CREATE INDEX model_benchmarks_model_id_idx ON model_benchmarks (model_id);
CREATE INDEX model_benchmarks_worker_id_idx ON model_benchmarks (worker_id);

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

-- Join tokens for worker enrollment. A worker presents one of these as its
-- bearer credential on its first WorkerHub connect; the server mints per-worker
-- credentials in exchange. Multi-use until expires_at (unix seconds). Only the
-- bcrypt hash is stored — the plaintext is shown once at mint time. Expired rows
-- are pruned opportunistically on mint and validate.
CREATE TABLE join_tokens (
    id         TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL
) STRICT;

CREATE INDEX join_tokens_expires_at_idx ON join_tokens (expires_at);

-- Enrolled workers with their per-worker credentials. worker_id is the
-- server-assigned identity (a ULID); secret_hash is the bcrypt hash of the
-- per-worker secret handed back once at enrollment. Deleting the row revokes
-- the worker: its next steady-state connect is rejected as unknown.
CREATE TABLE workers (
    worker_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL DEFAULT '',
    secret_hash TEXT NOT NULL,
    created_at  INTEGER NOT NULL
) STRICT;

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
