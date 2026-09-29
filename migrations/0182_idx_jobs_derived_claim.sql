-- §46 (REVERSIBLE, transaction = "none"; card 27, ADR-0052 D-G). Audit P1-15: ops.claim_derived_work (0164) scans only open jobs, ordered by priority DESC,
-- next_retry_at, created_at; settled jobs leave the index.
--
-- ONE statement, run by `xtask migrate` outside any transaction block (manifest key
-- transaction = "none"): CREATE INDEX CONCURRENTLY is illegal inside one. Not atomic with the
-- ledger row: a failed build leaves an INVALID index, the rerun's precheck refuses by name, and the
-- manifest's rollback (DROP INDEX CONCURRENTLY IF EXISTS ops.jobs_claim_active_idx) is also the fix.
CREATE INDEX CONCURRENTLY jobs_claim_active_idx
  ON ops.jobs (job_type, priority DESC, next_retry_at, created_at)
  WHERE status IN ('PENDING', 'RETRY_WAIT', 'PROCESSING');
