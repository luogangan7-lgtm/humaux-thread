-- §46 (REVERSIBLE, transaction = "none"; card 35 S2, ADR-0062). Serves ops.purge_expired_selection_snapshots (0218, ADR-0062 D-H): the per-tenant victim scan `tenant_id = GUC AND expires_at < now() ORDER BY expires_at LIMIT`; nothing indexed expires_at before.
--
-- ONE statement, run by `xtask migrate` outside any transaction block (manifest key transaction = "none";
-- CREATE INDEX CONCURRENTLY is illegal inside one, ADR-0052 D-G; the 0213 shape). Not atomic with the ledger row: a
-- failed build leaves an INVALID index, the rerun's precheck refuses by name, and the manifest's rollback
-- (DROP INDEX CONCURRENTLY IF EXISTS ops.selection_snapshots_tenant_expires_idx) is also the fix.
CREATE INDEX CONCURRENTLY selection_snapshots_tenant_expires_idx
  ON ops.selection_snapshots (tenant_id, expires_at);
