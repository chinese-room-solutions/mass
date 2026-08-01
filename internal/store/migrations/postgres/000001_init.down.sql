-- Reverse of 000001_init.up.sql for the Postgres flavor. Drops in reverse
-- dependency order. Triggers are dropped automatically with their table on
-- Postgres, but the trigger function we created is independent and must
-- be dropped explicitly.
--
-- pgcrypto is intentionally NOT dropped — other databases/extensions on
-- the same cluster may rely on it. Operators who want it gone can issue
-- `DROP EXTENSION pgcrypto` themselves.
DROP INDEX IF EXISTS downloads_status_idx;
DROP INDEX IF EXISTS downloads_group_key_idx;
DROP TABLE IF EXISTS downloads;
DROP INDEX IF EXISTS worker_device_enabled_worker_idx;
DROP TABLE IF EXISTS worker_device_enabled;
DROP TABLE IF EXISTS runtimes;
DROP TABLE IF EXISTS throughput_corrections;
DROP TABLE IF EXISTS device_benchmarks;
DROP TABLE IF EXISTS worker_queue_state;
DROP INDEX IF EXISTS queue_results_created_at_idx;
DROP TABLE IF EXISTS queue_results;
DROP INDEX IF EXISTS goqite_queue_priority_created_idx;
DROP TABLE IF EXISTS goqite;
DROP FUNCTION IF EXISTS goqite_update_timestamp();
DROP TABLE IF EXISTS settings;
