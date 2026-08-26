-- §16.2: read routing hits exactly one `serving = true` row per stream family
-- (tenant_id, scope_kind, scope_id, domain, projection_kind) — projection_version itself is
-- deliberately excluded from this index's key columns, that is the whole point: it is the
-- column the DB-layer constraint prevents two rows from disagreeing on. `serving` / `shadow`
-- columns themselves already exist (0007_projection.sql skeleton DDL); this migration only
-- adds the partial unique index the spec's own §16.2 code block names verbatim.
--
-- Doubles as `serving_version(stream_family)`'s fast lookup path (crates/adapters/src/
-- serving_repo.rs) — its `WHERE ... AND serving` query is answered by this exact index.
CREATE UNIQUE INDEX ux_serving_one ON projection.stream_checkpoints
  (tenant_id, scope_kind, scope_id, domain, projection_kind) WHERE serving;
