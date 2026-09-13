-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0164). ADR-0041 (card 19):
-- the §22.1 EXACT census readout that belongs to one immutable `memory.enumerate` manifest.
--
-- Why the readout has to be stored at all (§20.4: "EXACT / Export / audit 分页不能使用
-- best-effort"; §22.1: "`total` … 与返回项取同一事务快照"):
--   Page 1 of an enumeration mints the manifest (ops.selection_snapshot_items) inside one
--   REPEATABLE READ transaction, and the census `count(*)` is taken in that same snapshot —
--   so page 1's `total` and its items are same-snapshot by construction. Page 2 arrives in a
--   NEW transaction whose snapshot is younger; re-running the count there would pair a fresh
--   denominator with frozen items, which is exactly the drift §22.1 forbids, while counting
--   the frozen item rows instead would be 分母内生 ("禁止用召回条数冒充 `total`"). The only
--   remaining honest option is to freeze the readout WITH the manifest, which is what these
--   three columns are.
--
-- Nullable on purpose, and all-or-nothing: a snapshot minted by a caller that ran no census
-- (any pre-0165 row, or a future non-EXACT manifest user) carries three NULLs, and
-- adapters::context_repo reads that back as "no census for this manifest" ⇒
-- CensusResult::failed ⇒ §22.4 trigger 4 ⇒ cannot_establish/census_failed. It never reads as
-- a total of 0 (§23.3④: "禁止填 `0` … 充数").
--
-- No new table, no new role, no new GRANT: ops.selection_snapshots is not one of §6.2.2's 14
-- named override tables (migration 0083's header already records this), so it is covered by
-- §6.2.1's domain default `GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA ops TO
-- role_gateway` (migration 0011 line 163) — a table-level grant extends to columns added
-- later, so no §6.2.2 matrix row and no xtask/src/rls_check.rs MATRIX cell changes. RLS
-- (0083's `selection_snapshots_tenant` policy) is row-level and untouched by new columns.

ALTER TABLE ops.selection_snapshots
  ADD COLUMN census_predicate_id     text,
  ADD COLUMN census_total            bigint,
  ADD COLUMN census_excluded_secret  bigint;

-- §22.0: "class=exact 而 predicate_id=null 是不变量违反" — a stored readout with a total but
-- no predicate id could never legally leave the wire, so it cannot be stored either. And
-- §22.1's own constructor invariant (`returned + excluded_secret <= total`) needs both counts
-- to exist together; a half-written readout is refused at the door (§50 fail-loud), not
-- silently completed with a zero.
ALTER TABLE ops.selection_snapshots
  ADD CONSTRAINT selection_snapshots_census_all_or_nothing CHECK (
    (census_predicate_id IS NULL AND census_total IS NULL AND census_excluded_secret IS NULL)
    OR (census_predicate_id <> '' AND census_total >= 0 AND census_excluded_secret >= 0
        AND census_excluded_secret <= census_total)
  );

COMMENT ON COLUMN ops.selection_snapshots.census_predicate_id IS
  '§22.1/§22.0: the predicate identity the frozen manifest''s denominator was counted under '
  '(domain::selection::AUTHORIZED_MEMORY_ENUMERATION_V1 for memory.enumerate). NULL together '
  'with the two counts = this manifest carries no census.';
COMMENT ON COLUMN ops.selection_snapshots.census_total IS
  '§22.1 `total`: SELECT count(*) over the enumerable scope + predicate, taken in the SAME '
  'REPEATABLE READ snapshot that minted ops.selection_snapshot_items. Never derived from the '
  'item count (分母内生), never backfilled on a later page.';
COMMENT ON COLUMN ops.selection_snapshots.census_excluded_secret IS
  '§22.1 `excluded_secret`: rows inside `census_total` that a SECRET_MATERIAL backing source '
  'keeps out of `returned` — counted by their own statement in that same snapshot so coverage '
  'is deducted instead of the rows being silently dropped.';
