-- Card 35 S7 / ADR-0062 D-K (P1-14): a memory.enumerate manifest is capped at the gateway key
-- HUMAUX_GATEWAY_ENUMERATION_MANIFEST_CAP. When the authorized universe is larger, the manifest keeps the first CAP
-- ids in memory_id DESC order and this column records the last id it stored; the cursor that reaches the end of
-- that manifest opens a new segment whose predicate adds `memory_records.memory_id < continues_before` and whose
-- census is minted in its own REPEATABLE READ snapshot (Baseline §22.1, E4).
--
-- Writer: adapters::selection_repo::begin_authorized_snapshot_in_txn (role_gateway, in the minting transaction).
-- Reader: adapters::selection_repo segment check (role_gateway, under the tenant GUC and 0083's RLS policy).
-- NULL = the manifest holds the whole universe (every pre-0222 row, every manifest at or under the cap).
--
-- No new table, role, GRANT or policy: ops.selection_snapshots carries table-level grants (§6.2.1 domain default,
-- see 0165's header), which extend to a column added later, so no §6.2.2 row and no rls_check MATRIX cell changes.

ALTER TABLE ops.selection_snapshots ADD COLUMN continues_before uuid;

COMMENT ON COLUMN ops.selection_snapshots.continues_before IS
  'ADR-0062 D-K: the last memory_id this capped manifest stored (memory_id DESC order); the next segment enumerates '
  'memory_id < this value with its own census. NULL = the manifest is the whole universe.';
