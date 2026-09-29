-- §46 (REVERSIBLE, transaction = "none"; card 27, ADR-0052 D-G). Audit P1-15: the RYW overlay (adapters::retrieve) and the projection worker join ops.outbox on
-- (tenant_id, commit_seq). UNIQUE makes the overlay's one-Evidence-per-commit_seq assumption enforceable.
--
-- ONE statement, run by `xtask migrate` outside any transaction block (manifest key
-- transaction = "none"): CREATE INDEX CONCURRENTLY is illegal inside one. Not atomic with the
-- ledger row: a failed build leaves an INVALID index, the rerun's precheck refuses by name, and the
-- manifest's rollback (DROP INDEX CONCURRENTLY IF EXISTS ops.outbox_tenant_commit_seq_uidx) is also the fix.
CREATE UNIQUE INDEX CONCURRENTLY outbox_tenant_commit_seq_uidx
  ON ops.outbox (tenant_id, commit_seq)
  WHERE commit_seq IS NOT NULL;
