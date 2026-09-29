-- §46 (REVERSIBLE, transaction = "none"; card 27, ADR-0052 D-G). Audit P1-15: memory_evidence's PK leads with memory_id; the overlay and the projection worker join
-- it on evidence_id.
--
-- ONE statement, run by `xtask migrate` outside any transaction block (manifest key
-- transaction = "none"): CREATE INDEX CONCURRENTLY is illegal inside one. Not atomic with the
-- ledger row: a failed build leaves an INVALID index, the rerun's precheck refuses by name, and the
-- manifest's rollback (DROP INDEX CONCURRENTLY IF EXISTS private.memory_evidence_evidence_idx) is also the fix.
CREATE INDEX CONCURRENTLY memory_evidence_evidence_idx
  ON private.memory_evidence (evidence_id);
