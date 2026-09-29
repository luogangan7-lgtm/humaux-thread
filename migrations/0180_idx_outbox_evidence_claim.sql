-- §46 (REVERSIBLE, transaction = "none"; card 27, ADR-0052 D-G). Audit P1-15: the distill claim (adapters::distill_repo::claim_pending_evidence) and the projection
-- claim's distill-closed probe read only open EVIDENCE rows.
--
-- ONE statement, run by `xtask migrate` outside any transaction block (manifest key
-- transaction = "none"): CREATE INDEX CONCURRENTLY is illegal inside one. Not atomic with the
-- ledger row: a failed build leaves an INVALID index, the rerun's precheck refuses by name, and the
-- manifest's rollback (DROP INDEX CONCURRENTLY IF EXISTS ops.outbox_evidence_claim_idx) is also the fix.
CREATE INDEX CONCURRENTLY outbox_evidence_claim_idx
  ON ops.outbox (tenant_id, event_type, commit_seq)
  WHERE status IN ('PENDING', 'PROCESSING') AND evidence_id IS NOT NULL;
