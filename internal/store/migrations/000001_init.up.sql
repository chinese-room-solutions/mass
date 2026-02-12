-- Downloads tracking.
CREATE TABLE downloads (
    filename   TEXT PRIMARY KEY,
    repo_id    TEXT NOT NULL,
    group_name TEXT NOT NULL DEFAULT '',
    status     TEXT NOT NULL DEFAULT 'active',
    downloaded INTEGER NOT NULL DEFAULT 0,
    total      INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

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

-- Inference results cache.
CREATE TABLE queue_results (
    id           TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    body         BLOB,
    error        TEXT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    completed_at TEXT
);

CREATE INDEX queue_results_request_hash_idx ON queue_results (request_hash);
CREATE INDEX queue_results_created_at_idx ON queue_results (created_at);

-- Device queue state (tail tracking for scheduler dispatch).
CREATE TABLE device_queue_state (
    queue_name  TEXT PRIMARY KEY,
    agent_id    TEXT NOT NULL,
    device_ids  TEXT NOT NULL,
    tail_hash   TEXT NOT NULL DEFAULT '',
    tail_length INTEGER NOT NULL DEFAULT 0,
    loaded_hash TEXT NOT NULL DEFAULT '',
    enabled     BOOLEAN NOT NULL DEFAULT 1,
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ'))
);

-- Device benchmark results (keyed by agent + device).
CREATE TABLE device_benchmarks (
    agent_id        TEXT NOT NULL,
    device_id       TEXT NOT NULL,
    device_name     TEXT NOT NULL,
    memory_gbs      REAL NOT NULL,
    compute_gflops  REAL NOT NULL DEFAULT 0,
    benched_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ')),
    PRIMARY KEY (agent_id, device_id)
);
