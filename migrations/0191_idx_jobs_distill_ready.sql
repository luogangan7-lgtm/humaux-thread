-- §46 (REVERSIBLE, transaction = "none"; card 32, ADR-0058 D-S). Serves the per-tenant READY probe
-- and the tenant FIFO pick of ops.claim_derived_work_v2 (0190): the claim asks, per scheduler row,
-- "does this tenant have a DERIVED_DISTILL job with next_retry_at <= now", then takes that tenant's
-- oldest such job. Settled and in-flight jobs leave the index.
--
-- ONE statement, run by `xtask migrate` outside any transaction block (0182 precedent). A failed
-- build leaves an INVALID index; the rerun's precheck refuses by name and the manifest's rollback
-- (DROP INDEX CONCURRENTLY IF EXISTS ops.jobs_distill_ready_idx) is also the fix.
CREATE INDEX CONCURRENTLY jobs_distill_ready_idx
  ON ops.jobs (tenant_id, next_retry_at)
  WHERE job_type = 'DERIVED_DISTILL' AND status IN ('PENDING', 'RETRY_WAIT', 'WAITING_KEY');
