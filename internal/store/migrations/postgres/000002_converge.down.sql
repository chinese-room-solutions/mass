-- No-op. 000002_converge.up.sql creates nothing 000001_init.up.sql does not
-- already own, so reversing it means reversing 000001 — which
-- 000001_init.down.sql already does. Dropping those tables here would
-- destroy schema a database at version 1 is entitled to keep.
SELECT 1;
