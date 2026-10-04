-- §46 (REVERSIBLE, transaction = "none"; card 34, ADR-0061 D-D). Serves ops.health_snapshot()'s processing_gap_count read of projection.processing_gaps.
--
-- ONE statement, run by `xtask migrate` outside any transaction block (manifest key transaction = "none";
-- CREATE INDEX CONCURRENTLY is illegal inside one, ADR-0052 D-G). Not atomic with the ledger row: a failed build
-- leaves an INVALID index, the rerun's precheck refuses by name, and the manifest's rollback
-- (DROP INDEX CONCURRENTLY IF EXISTS projection.stream_log_gap_idx) is also the fix.
-- The predicate mirrors projection.processing_gaps' WHERE (0007:56) as a planner hint; it is not a definition of a
-- gap. If the view's state set ever changes, the index stops matching and the count falls back to a scan: slower,
-- never a different number.
CREATE INDEX CONCURRENTLY stream_log_gap_idx
  ON projection.stream_log (domain, projection_kind)
  WHERE state IN ('FAILED', 'LOST');
