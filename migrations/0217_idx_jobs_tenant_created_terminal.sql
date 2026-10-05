-- §46 (REVERSIBLE, transaction = "none"; card 35 S2, ADR-0062). Serves ops.purge_terminal_jobs (0218, ADR-0062 D-J): the per-tenant terminal-job scan ordered by created_at; partial on the three terminal states, so a job still being worked is never in it.
--
-- ONE statement, run by `xtask migrate` outside any transaction block (manifest key transaction = "none";
-- CREATE INDEX CONCURRENTLY is illegal inside one, ADR-0052 D-G; the 0213 shape). Not atomic with the ledger row: a
-- failed build leaves an INVALID index, the rerun's precheck refuses by name, and the manifest's rollback
-- (DROP INDEX CONCURRENTLY IF EXISTS ops.jobs_tenant_created_terminal_idx) is also the fix.
CREATE INDEX CONCURRENTLY jobs_tenant_created_terminal_idx
  ON ops.jobs (tenant_id, created_at)
  WHERE status IN ('DONE', 'DEAD', 'FAILED');
