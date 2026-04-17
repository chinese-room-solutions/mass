-- Running sum of per-task difficulty for the device queue. Maintained
-- incrementally on enqueue / dequeue / steal / drain. Used by the dispatcher
-- to estimate "time to process" when picking a placement.
ALTER TABLE device_queue_state ADD COLUMN tail_difficulty REAL NOT NULL DEFAULT 0;
