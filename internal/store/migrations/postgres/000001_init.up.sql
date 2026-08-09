-- Postgres counterpart of migrations/sqlite/000001_init.up.sql.
-- Differences from the SQLite flavor:
--   • timestamps are TIMESTAMPTZ (Postgres-native), not TEXT
--   • goqite table uses Postgres-native types per goqite/schema_postgres.sql
--   • bytes are BYTEA, not BLOB
--   • boolean is native BOOLEAN with TRUE/FALSE defaults
--   • no STRICT — Postgres is strict by default
--
-- pgcrypto is required for gen_random_bytes() used in the goqite id default.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Key-value settings.
CREATE TABLE settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- goqite message queue (Postgres flavor). Layout follows
-- vendor/maragu.dev/goqite/schema_postgres.sql verbatim so goqite's own
-- generated SQL matches.
CREATE FUNCTION goqite_update_timestamp() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE goqite (
    id       TEXT PRIMARY KEY DEFAULT ('m_' || encode(gen_random_bytes(16), 'hex')),
    created  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated  TIMESTAMPTZ NOT NULL DEFAULT now(),
    queue    TEXT NOT NULL,
    body     BYTEA NOT NULL,
    timeout  TIMESTAMPTZ NOT NULL DEFAULT now(),
    received INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 0
);

CREATE TRIGGER goqite_updated_timestamp
BEFORE UPDATE ON goqite
FOR EACH ROW EXECUTE PROCEDURE goqite_update_timestamp();

CREATE INDEX goqite_queue_priority_created_idx ON goqite (queue, priority DESC, created);

-- Job results cache. Identity is the goqite message ID. body is gateway-
-- defined opaque bytes; created_at/completed_at are stored as text (RFC3339-
-- nano) for SQLite parity — see internal/queue/results.go which writes and
-- reads them as strings either way.
CREATE TABLE queue_results (
    id           TEXT PRIMARY KEY,
    status       TEXT NOT NULL DEFAULT 'pending',
    body         BYTEA,
    error        TEXT,
    created_at   TEXT NOT NULL,
    completed_at TEXT
);

CREATE INDEX queue_results_created_at_idx ON queue_results (created_at);

-- Worker queue state (tail tracking for scheduler dispatch).
CREATE TABLE worker_queue_state (
    queue_name      TEXT PRIMARY KEY,
    worker_id       TEXT NOT NULL,
    device_ids      TEXT NOT NULL,
    tail_seconds    DOUBLE PRECISION NOT NULL DEFAULT 0,
    tail_model_id   TEXT NOT NULL DEFAULT '',
    updated_at      TEXT NOT NULL DEFAULT ''
);

-- Device benchmark results (keyed by worker + device). flops is the
-- device's generic matmul throughput. These rows describe HARDWARE and
-- are display-only: job estimates come from model_benchmarks, never
-- from here.
CREATE TABLE device_benchmarks (
    worker_id       TEXT NOT NULL,
    device_id       TEXT NOT NULL,
    device_name     TEXT NOT NULL,
    memory_gbs      DOUBLE PRECISION NOT NULL,
    load_gbs        DOUBLE PRECISION NOT NULL DEFAULT 0,
    flops           DOUBLE PRECISION NOT NULL DEFAULT 0,
    benched_at      TEXT NOT NULL,
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
    units_per_sec  DOUBLE PRECISION NOT NULL,
    graph_secs     DOUBLE PRECISION NOT NULL,
    base_bytes     BIGINT NOT NULL,
    per_slot_bytes BIGINT NOT NULL,
    model_size     BIGINT NOT NULL,
    model_mtime    BIGINT NOT NULL,
    error          TEXT,
    created_at     BIGINT NOT NULL,
    updated_at     BIGINT NOT NULL,
    PRIMARY KEY (worker_id, device_set, model_id)
);

CREATE INDEX model_benchmarks_model_id_idx ON model_benchmarks (model_id);
CREATE INDEX model_benchmarks_worker_id_idx ON model_benchmarks (worker_id);

-- Installed runtime gateway packages. Versioned, persistent across restarts.
-- auto_start = TRUE means main() launches this gateway during boot; otherwise
-- it stays dormant until the operator clicks Start.
CREATE TABLE runtimes (
    runtime_name TEXT PRIMARY KEY,
    version      TEXT NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    description  TEXT NOT NULL DEFAULT '',
    install_path TEXT NOT NULL,
    auto_start   BOOLEAN NOT NULL DEFAULT FALSE,
    installed_at TEXT NOT NULL DEFAULT ''
);

-- Per-(worker, device) enable flag. Operator-controlled via the Workers
-- tab. Absent row = enabled (default for newly-connected workers without
-- operator intent).
CREATE TABLE worker_device_enabled (
    worker_id  TEXT NOT NULL,
    device_id  TEXT NOT NULL,
    enabled    BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TEXT NOT NULL DEFAULT '',
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
    expires_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE INDEX join_tokens_expires_at_idx ON join_tokens (expires_at);

-- Enrolled workers with their per-worker credentials. worker_id is the
-- server-assigned identity (a ULID); secret_hash is the bcrypt hash of the
-- per-worker secret handed back once at enrollment. Deleting the row revokes
-- the worker: its next steady-state connect is rejected as unknown.
CREATE TABLE workers (
    worker_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL DEFAULT '',
    secret_hash TEXT NOT NULL,
    created_at  BIGINT NOT NULL
);

-- In-flight + paused model downloads. One row per (rel_path). Completed
-- downloads are deleted; the file then becomes a regular model the runtime
-- gateway picks up on its next walk. group_key ties files of the same
-- install together (e.g. primary + mmproj companion).
CREATE TABLE downloads (
    rel_path     TEXT PRIMARY KEY,
    url          TEXT NOT NULL,
    source       TEXT NOT NULL,
    repo_id      TEXT NOT NULL DEFAULT '',
    runtime_name TEXT NOT NULL DEFAULT '',
    group_key    TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'active',
    downloaded   BIGINT NOT NULL DEFAULT 0,
    total        BIGINT NOT NULL DEFAULT 0,
    error_msg    TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE INDEX downloads_group_key_idx ON downloads (group_key);
CREATE INDEX downloads_status_idx ON downloads (status);
