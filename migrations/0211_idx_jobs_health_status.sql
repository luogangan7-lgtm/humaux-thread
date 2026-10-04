-- §46 (REVERSIBLE, transaction = "none"; card 34, ADR-0061 D-D). Serves ops.health_snapshot()'s four jobs_* counts and the oldest PENDING age index-only; jobs_claim_active_idx covers neither WAITING_KEY nor DEAD.
--
-- ONE statement, run by `xtask migrate` outside any transaction block (manifest key transaction = "none";
-- CREATE INDEX CONCURRENTLY is illegal inside one, ADR-0052 D-G). Not atomic with the ledger row: a failed build
-- leaves an INVALID index, the rerun's precheck refuses by name, and the manifest's rollback
-- (DROP INDEX CONCURRENTLY IF EXISTS ops.jobs_health_status_idx) is also the fix.
CREATE INDEX CONCURRENTLY jobs_health_status_idx
  ON ops.jobs (status, created_at)
  WHERE status IN ('PENDING', 'PROCESSING', 'WAITING_KEY', 'DEAD');
