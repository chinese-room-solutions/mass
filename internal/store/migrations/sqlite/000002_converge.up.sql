-- Converge an older-vintage database onto the full 000001 table set.
--
-- 000001_init.up.sql was edited in place before the schema went append-only,
-- so a database created from an early vintage is recorded at version 1 yet
-- lacks every table appended afterwards. This migration re-declares each
-- object 000001 owns with IF NOT EXISTS: an old database gains what it is
-- missing, a fresh one (which just ran 000001) no-ops through it.
--
-- Nothing new belongs here — new schema goes in a new migration.
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS goqite (
    id       TEXT PRIMARY KEY DEFAULT ('m_' || lower(hex(randomblob(16)))),
    created  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    updated  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    queue    TEXT NOT NULL,
    body     BLOB NOT NULL,
    timeout  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    received INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE TRIGGER IF NOT EXISTS goqite_updated_timestamp AFTER UPDATE ON goqite BEGIN
    UPDATE goqite SET updated = strftime('%Y-%m-%dT%H:%M:%fZ') WHERE id = old.id;
END;

CREATE INDEX IF NOT EXISTS goqite_queue_priority_created_idx ON goqite (queue, priority DESC, created);

CREATE TABLE IF NOT EXISTS queue_results (
    id           TEXT PRIMARY KEY,
    status       TEXT NOT NULL DEFAULT 'pending',
    body         BLOB,
    error        TEXT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    completed_at TEXT
);

CREATE INDEX IF NOT EXISTS queue_results_created_at_idx ON queue_results (created_at);

CREATE TABLE IF NOT EXISTS worker_queue_state (
    queue_name      TEXT PRIMARY KEY,
    worker_id       TEXT NOT NULL,
    device_ids      TEXT NOT NULL,
    tail_seconds    REAL NOT NULL DEFAULT 0,
    tail_model_id   TEXT NOT NULL DEFAULT '',
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ'))
);

CREATE TABLE IF NOT EXISTS device_benchmarks (
    worker_id       TEXT NOT NULL,
    device_id       TEXT NOT NULL,
    device_name     TEXT NOT NULL,
    memory_gbs      REAL NOT NULL,
    load_gbs        REAL NOT NULL DEFAULT 0,
    flops           REAL NOT NULL DEFAULT 0,
    benched_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    PRIMARY KEY (worker_id, device_id)
);

CREATE TABLE IF NOT EXISTS model_benchmarks (
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

CREATE INDEX IF NOT EXISTS model_benchmarks_model_id_idx ON model_benchmarks (model_id);
CREATE INDEX IF NOT EXISTS model_benchmarks_worker_id_idx ON model_benchmarks (worker_id);

CREATE TABLE IF NOT EXISTS runtimes (
    runtime_name   TEXT PRIMARY KEY,
    version        TEXT NOT NULL,
    display_name   TEXT NOT NULL DEFAULT '',
    description    TEXT NOT NULL DEFAULT '',
    install_path   TEXT NOT NULL,
    auto_start     BOOLEAN NOT NULL DEFAULT 0,
    installed_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ'))
);

CREATE TABLE IF NOT EXISTS worker_device_enabled (
    worker_id  TEXT NOT NULL,
    device_id  TEXT NOT NULL,
    enabled    BOOLEAN NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    PRIMARY KEY (worker_id, device_id)
);

CREATE INDEX IF NOT EXISTS worker_device_enabled_worker_idx ON worker_device_enabled (worker_id);

CREATE TABLE IF NOT EXISTS join_tokens (
    id         TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS join_tokens_expires_at_idx ON join_tokens (expires_at);

CREATE TABLE IF NOT EXISTS workers (
    worker_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL DEFAULT '',
    secret_hash TEXT NOT NULL,
    created_at  INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS downloads (
    rel_path     TEXT PRIMARY KEY,
    url          TEXT NOT NULL,
    source       TEXT NOT NULL,
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

CREATE INDEX IF NOT EXISTS downloads_group_key_idx ON downloads (group_key);
CREATE INDEX IF NOT EXISTS downloads_status_idx ON downloads (status);
