-- §20.4 Stable Selection / Pagination Contract (T6.3, G20-1/G80-32). `ops.selection_snapshots`
-- was created as a skeleton table by 0008_ops_core.sql (tenant_id + created_at only, no RLS
-- yet — §8 P0 bootstrap explicitly left the §48 ops.* skeleton tables that way). This is
-- pure ADD/ENABLE DDL against that existing, still-empty table (rule ③: 0008 is applied and
-- immutable; nothing here edits a line 0008 wrote).
--
-- `query_fingerprint`/`expires_at` are the two Mode B cursor fields (§20.4: "cursor 只携带
-- snapshot_id / query_fingerprint / last_sort_tuple / expiry / MAC") that need to survive a
-- round trip beyond the transaction that materializes `ops.selection_snapshot_items` — a
-- later page re-reads this row to confirm the cursor's claimed `query_fingerprint` still
-- matches (defense in depth alongside the MAC itself, `domain::selection::Cursor::validate`).
ALTER TABLE ops.selection_snapshots
  ADD COLUMN query_fingerprint text        NOT NULL,
  ADD COLUMN expires_at        timestamptz NOT NULL;

COMMENT ON TABLE ops.selection_snapshots IS
  '§20.4 Mode B pagination: one row per snapshot. The manifest (ops.selection_snapshot_items) '
  'is materialized once, inside the same REPEATABLE READ transaction that inserts this row '
  '(adapters::selection_repo::begin_enumeration_snapshot) — every later page reads only that '
  'manifest, never the live source table again.';

COMMENT ON COLUMN ops.selection_snapshots.query_fingerprint IS
  '§20.1 predicate_id + tenant, hashed (domain::selection::query_fingerprint). Also bound '
  'into every Cursor''s MAC — this column is a defense-in-depth re-check, not the sole guard.';

COMMENT ON COLUMN ops.selection_snapshots.expires_at IS
  '§20.4 cursor "expiry" field, server-authoritative. A page-fetch that outlives this rejects '
  'before touching ops.selection_snapshot_items (domain::selection::CursorError::Expired).';

-- §62 tenant RLS, NULLIF form (same template as migrations/0050's
-- control.memory_automation_policies) — no user_id half: a selection snapshot is scoped to
-- the tenant that ran the query, not to one member of it.
ALTER TABLE ops.selection_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.selection_snapshots FORCE ROW LEVEL SECURITY;
CREATE POLICY selection_snapshots_tenant ON ops.selection_snapshots
USING (
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
);

-- Already owned by role_migration_owner via 0011's ownership sweep and already covered by
-- 0011's `GRANT ... ON ALL TABLES IN SCHEMA ops` (§6.2.1 domain default — this table is not
-- one of §6.2.2's 14 named overrides) — no GRANT statement needed here (hard rule ④: "GRANT
-- 对齐 §6.2.1" means matching the existing domain-default grant, not adding a new one).
