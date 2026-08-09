-- Reverse of 000001_init.up.sql. Drops in reverse dependency-safe order.
-- Indexes are dropped implicitly with their table on SQLite, but they're
-- listed explicitly for symmetry with the Postgres counterpart.
DROP INDEX IF EXISTS downloads_status_idx;
DROP INDEX IF EXISTS downloads_group_key_idx;
DROP TABLE IF EXISTS downloads;
DROP INDEX IF EXISTS worker_device_enabled_worker_idx;
DROP TABLE IF EXISTS worker_device_enabled;
DROP TABLE IF EXISTS runtimes;
DROP TABLE IF EXISTS throughput_corrections;
DROP INDEX IF EXISTS model_benchmarks_worker_id_idx;
DROP INDEX IF EXISTS model_benchmarks_model_id_idx;
DROP TABLE IF EXISTS model_benchmarks;
DROP TABLE IF EXISTS device_benchmarks;
DROP TABLE IF EXISTS worker_queue_state;
DROP INDEX IF EXISTS queue_results_created_at_idx;
DROP TABLE IF EXISTS queue_results;
DROP INDEX IF EXISTS goqite_queue_priority_created_idx;
DROP TRIGGER IF EXISTS goqite_updated_timestamp;
DROP TABLE IF EXISTS goqite;
DROP TABLE IF EXISTS settings;
