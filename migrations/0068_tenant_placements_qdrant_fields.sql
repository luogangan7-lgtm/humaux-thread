-- §17.3 Qdrant tiered multitenancy placement columns on the P1 skeleton table
-- (`migrations/0007_projection.sql` created only tenant_id/cell_id/placed_at). ALTERed in
-- place per repo hard rule ④ ("绝不重建、绝不编辑已应用迁移") — 0007 stays untouched,
-- cell_id/placed_at are kept (not deleted), only loosened/extended.
--
-- cell_id/placed_at predate this table's §17.3 use and describe a different, still-
-- unimplemented concept (per-tenant "Data Cell"/residency placement, §6/§67's Control Plane
-- routing — no `control.tenants.cell_id` column or `control.cells` table exists yet
-- anywhere in this migration set, only `ops.mechanism_observations.cell_id`, an unrelated
-- gate-registry column). This is the same schema-name collision already flagged for the
-- Canonical Table Registry follow-up (three spellings of "tenant placement":
-- `control.tenants.cell_id` (not yet built) / a documented-but-never-created
-- `control.tenant_placements` / this table, `projection.tenant_placements`). §17.3's field
-- list does not mention `cell_id` at all, and forcing every future Qdrant-placement row to
-- also invent a fictional Data Cell id would be worse than leaving the column as an unused,
-- nullable historical column — so `cell_id` loses its `NOT NULL` and drops out of the
-- primary key; it is not dropped, so whichever task resolves the residency-cell concept
-- later still has it to build on.
--
-- Table has zero rows anywhere this migration has run — grepped, no INSERT into
-- `projection.tenant_placements` exists in the codebase yet — so every `NOT NULL` column
-- below (with or without a DEFAULT) adds cleanly, no backfill required.

ALTER TABLE projection.tenant_placements
  DROP CONSTRAINT tenant_placements_pkey;

ALTER TABLE projection.tenant_placements
  ALTER COLUMN cell_id DROP NOT NULL;

ALTER TABLE projection.tenant_placements
  ADD COLUMN projection_family text NOT NULL,
  ADD COLUMN collection_name   text NOT NULL,
  ADD COLUMN shard_key         text,
  ADD COLUMN placement_class   text NOT NULL DEFAULT 'SHARED_FALLBACK',
  ADD COLUMN point_count       bigint NOT NULL DEFAULT 0,
  ADD COLUMN bytes_estimate    bigint NOT NULL DEFAULT 0,
  ADD COLUMN promotion_state   text NOT NULL DEFAULT 'STABLE',
  ADD COLUMN updated_at        timestamptz NOT NULL DEFAULT now();

-- §17.3 field list gives each tenant at most one placement row per projection_family — the
-- table's real functional key (cell_id is no longer part of it, see header).
ALTER TABLE projection.tenant_placements
  ADD CONSTRAINT tenant_placements_pkey PRIMARY KEY (tenant_id, projection_family);

-- Closed sets mirrored by `adapters::qdrant::RetrievalFamily`/`PlacementClass`/
-- `PromotionState` (crates/adapters/src/qdrant.rs) — `tests/qdrant_contract.rs`'s
-- `*_matches_migration_0068_check` tests keep the two in sync (§78.2 DB-enum/Rust-enum
-- contract test).
ALTER TABLE projection.tenant_placements
  ADD CONSTRAINT tenant_placements_projection_family_known
    CHECK (projection_family IN ('private_memory_v1', 'public_knowledge_v1', 'code_v1'));

ALTER TABLE projection.tenant_placements
  ADD CONSTRAINT tenant_placements_placement_class_known
    CHECK (placement_class IN ('SHARED_FALLBACK', 'DEDICATED'));

ALTER TABLE projection.tenant_placements
  ADD CONSTRAINT tenant_placements_promotion_state_known
    CHECK (promotion_state IN ('STABLE', 'PROMOTION_PENDING', 'PROMOTED'));

ALTER TABLE projection.tenant_placements
  ADD CONSTRAINT tenant_placements_point_count_nonneg CHECK (point_count >= 0);

ALTER TABLE projection.tenant_placements
  ADD CONSTRAINT tenant_placements_bytes_estimate_nonneg CHECK (bytes_estimate >= 0);

COMMENT ON TABLE projection.tenant_placements IS
  '§17.3 tiered multitenancy: which Qdrant collection/shard a tenant''s projection_family currently lives in, plus promotion bookkeeping. RLS/GRANT already established by 0007+0012+0031 (tenant_id-only predicate, this table has no visibility_class column) and 0011''s §6.2.1 projection.* domain default (role_gateway R, role_retrieval_worker R+W, role_maintenance R, role_migration_owner owner) — neither changes here, this migration only adds columns. Placement writes are control-plane-only (§17.3): the sole Rust-side writer is adapters::qdrant (crates/adapters/src/qdrant.rs), no other module constructs an UPDATE/INSERT against this table.';

COMMENT ON COLUMN projection.tenant_placements.cell_id IS
  'Predates §17.3 (0007 skeleton); unrelated Data Cell/residency concept, not currently written by anything. Nullable so §17.3 rows do not need to invent one — see this migration''s file header for the Canonical Table Registry follow-up this flags.';

COMMENT ON COLUMN projection.tenant_placements.shard_key IS
  '§17.3 "shard_key / shard_id" — nullable: a SHARED_FALLBACK placement has no dedicated shard key, only a DEDICATED one does.';

-- role_migration_owner-owned trigger (same pattern as
-- projection.stream_checkpoints_touch_updated_at, 0007): updated_at is never in any GRANT,
-- written only here.
CREATE FUNCTION projection.tenant_placements_touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

CREATE TRIGGER tenant_placements_touch_updated_at
BEFORE UPDATE ON projection.tenant_placements
FOR EACH ROW EXECUTE FUNCTION projection.tenant_placements_touch_updated_at();
