-- §46 (REVERSIBLE, transaction = "none"; card 35 S2, ADR-0062). Serves control.purge_idle_rate_buckets (0218, ADR-0062 D-I): the per-tenant idle scan `tenant_id = GUC AND updated_at < cutoff ORDER BY updated_at LIMIT`; the primary key leads with tenant_id but not updated_at.
--
-- ONE statement, run by `xtask migrate` outside any transaction block (manifest key transaction = "none";
-- CREATE INDEX CONCURRENTLY is illegal inside one, ADR-0052 D-G; the 0213 shape). Not atomic with the ledger row: a
-- failed build leaves an INVALID index, the rerun's precheck refuses by name, and the manifest's rollback
-- (DROP INDEX CONCURRENTLY IF EXISTS control.rate_buckets_tenant_updated_idx) is also the fix.
CREATE INDEX CONCURRENTLY rate_buckets_tenant_updated_idx
  ON control.rate_buckets (tenant_id, updated_at);
