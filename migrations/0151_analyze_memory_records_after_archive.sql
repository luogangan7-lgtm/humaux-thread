-- §46 forward-fix for 0150_memory_records_archive. 0150 added a nullable column
-- (private.memory_records.archived_at) and a partial index (idx_memory_records_live
-- WHERE archived_at IS NULL) but did not refresh planner statistics. Until the table is
-- analysed, the planner sizes the new column from defaults and the first cold plan of the
-- recall/context read (which now carries `archived_at IS NULL`, ADR-0024 D-C) is slow enough
-- that the gateway can miss the read-barrier timing the settlement acceptance test observes.
-- ANALYZE is idempotent maintenance with no schema effect; a fresh DB runs it here right after
-- the column/index exist, so the very first read plans against real statistics.
ANALYZE private.memory_records;
