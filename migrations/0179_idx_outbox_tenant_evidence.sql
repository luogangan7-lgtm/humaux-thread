-- §46 (REVERSIBLE, transaction = "none"; card 27, ADR-0052 D-G). Audit P1-15: per-tenant lookups of an Evidence's outbox rows (read_materialize, continuity_read,
-- the rehearsal's own evidence probes).
--
-- ONE statement, run by `xtask migrate` outside any transaction block (manifest key
-- transaction = "none"): CREATE INDEX CONCURRENTLY is illegal inside one. Not atomic with the
-- ledger row: a failed build leaves an INVALID index, the rerun's precheck refuses by name, and the
-- manifest's rollback (DROP INDEX CONCURRENTLY IF EXISTS ops.outbox_tenant_evidence_idx) is also the fix.
CREATE INDEX CONCURRENTLY outbox_tenant_evidence_idx
  ON ops.outbox (tenant_id, evidence_id)
  WHERE evidence_id IS NOT NULL;
