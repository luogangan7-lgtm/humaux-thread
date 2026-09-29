-- §46 (REVERSIBLE, transaction = "none"; card 27, ADR-0052 D-G). ADR-0052 D-C: serves projection.claim_issued_tickets' eligibility scan, its family-idle probe and
-- projection.unplaced_issued_tickets. Without it every --serve poll scans every tenant's settled history.
--
-- ONE statement, run by `xtask migrate` outside any transaction block (manifest key
-- transaction = "none"): CREATE INDEX CONCURRENTLY is illegal inside one. Not atomic with the
-- ledger row: a failed build leaves an INVALID index, the rerun's precheck refuses by name, and the
-- manifest's rollback (DROP INDEX CONCURRENTLY IF EXISTS projection.stream_log_issued_claim_idx) is also the fix.
CREATE INDEX CONCURRENTLY stream_log_issued_claim_idx
  ON projection.stream_log (domain, projection_kind, projection_version, tenant_id, scope_kind, scope_id, stream_seq)
  WHERE state = 'ISSUED';
