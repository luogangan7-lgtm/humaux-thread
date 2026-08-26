-- §20.4 Stable Selection / Pagination Contract (T6.3, G20-1/G80-32), continued from 0083.
-- `ops.selection_snapshot_items` was created by 0008_ops_core.sql with only
-- (selection_snapshot_id, item_id) — sufficient for a dedup-guaranteeing PK but not for its
-- own RLS (no literal tenant_id column, §62/§48.2's four-item RLS enumeration keys off a
-- literal `tenant_id` column, not a join through the parent) or for keyset pagination (no
-- stable per-row position). Denormalizing tenant_id onto the child rather than an RLS policy
-- that subqueries ops.selection_snapshots (the private.memory_evidence pattern,
-- migrations/0012_rls.sql) keeps this table's own RLS self-contained and matches this task's
-- explicit instruction ("ops.selection_snapshots / ops.selection_snapshot_items（tenant_id +
-- RLS）").
ALTER TABLE ops.selection_snapshot_items
  ADD COLUMN tenant_id uuid    NOT NULL REFERENCES control.tenants(tenant_id),
  ADD COLUMN ordinal   bigint  NOT NULL;

-- §20.4 "last_sort_tuple": the manifest is materialized once in ORDER BY order
-- (adapters::selection_repo), and `ordinal` is that fixed position. A page reads
-- `WHERE selection_snapshot_id = $1 AND ordinal > $cursor.last_ordinal ORDER BY ordinal
-- LIMIT $page_size` — this index is exactly that access path; the UNIQUE half also proves
-- one snapshot never materializes two rows at the same position (G20-1's "不重复").
CREATE UNIQUE INDEX selection_snapshot_items_snapshot_ordinal
  ON ops.selection_snapshot_items (selection_snapshot_id, ordinal);

COMMENT ON COLUMN ops.selection_snapshot_items.ordinal IS
  '§20.4 last_sort_tuple, concretely: fixed position within the immutable manifest, assigned '
  'once at materialization time (adapters::selection_repo::begin_enumeration_snapshot). Never '
  'recomputed against the live source table on a later page.';

ALTER TABLE ops.selection_snapshot_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.selection_snapshot_items FORCE ROW LEVEL SECURITY;
CREATE POLICY selection_snapshot_items_tenant ON ops.selection_snapshot_items
USING (
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
);

-- Same §6.2.1 domain-default reasoning as 0083: already owned by role_migration_owner,
-- already covered by 0011's schema-wide ops GRANT — no new GRANT statement.
