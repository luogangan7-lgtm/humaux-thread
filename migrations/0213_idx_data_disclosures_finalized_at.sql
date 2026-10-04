-- §46 (REVERSIBLE, transaction = "none"; card 34, ADR-0061 D-D). Serves ops.health_snapshot()'s finalized-since-watermark range read (data_disclosures_finalized_total); nothing indexed finalized_at before.
--
-- ONE statement, run by `xtask migrate` outside any transaction block (manifest key transaction = "none";
-- CREATE INDEX CONCURRENTLY is illegal inside one, ADR-0052 D-G). Not atomic with the ledger row: a failed build
-- leaves an INVALID index, the rerun's precheck refuses by name, and the manifest's rollback
-- (DROP INDEX CONCURRENTLY IF EXISTS ops.data_disclosures_finalized_at_idx) is also the fix.
CREATE INDEX CONCURRENTLY data_disclosures_finalized_at_idx
  ON ops.data_disclosures (finalized_at)
  WHERE finalized_at IS NOT NULL;
