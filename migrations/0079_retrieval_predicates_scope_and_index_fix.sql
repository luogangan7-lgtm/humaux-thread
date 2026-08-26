-- Review-finding fix (§20.1 / §20.2 rule 3 / §22.1), migrations 0077/0078 already applied and
-- therefore not edited in place — both gaps are closed here as additive follow-ups instead.
--
-- 1. §20.2 rule 3 gap: migration 0078's partial index carries `visibility_workspace_id` as its
--    only actual index KEY column — `memory_type`/`superseded_at` appear only in the partial
--    index's WHERE predicate, which Postgres uses for index-applicability matching, not as
--    scannable/filterable key columns. A live `pg_index` catalog check against this workspace's
--    dev instance (`docker exec humaux-thread-pg psql ... pg_index` on
--    `private.memory_records`) confirmed the real indexed KEY-column set was only {memory_id,
--    tenant_id, visibility_workspace_id} — `rejected_decisions_v1`'s `required_columns`
--    {memory_type, superseded_at, visibility_workspace_id} were NOT all indexed under any
--    reasonable per-column "is this column a real index key" derivation, so §20.2 rule 3 could
--    never have honestly returned EXACT for this predicate. This adds a second, non-partial
--    composite index that genuinely carries all three required_columns as key columns, closing
--    the gap at the data layer instead of a test fixture papering over it. 0078's partial index
--    stays in place — it is still the cheaper plan for this predicate's own exact WHERE-shape.
CREATE INDEX idx_memory_records_rejected_decisions_v1_cols
  ON private.memory_records (visibility_workspace_id, memory_type, superseded_at);

-- 2. §20.1 / §22.1 gap: `enumerable_scope` on the `rejected_decisions_v1` seed row (migration
--    0077) was missing the tenant filter §20.1's field table requires ("计 total 的 FROM +
--    租户过滤"; §22.1: total = `SELECT count(*) FROM <enumerable_scope> AND <sql_predicate>`).
--    RLS (0077's own tenant-isolation policy, migration 0012's template) already blocks a
--    cross-tenant read for any role that goes through the normal query path, but
--    `enumerable_scope` is a *declared* denominator description that may be executed
--    independent of which role runs it — a maintenance script or `role_migration_owner` path
--    that runs this literal string bypasses RLS and would silently compute a cross-tenant
--    denominator. Corrected below to filter on both keys.
--
--    Visibility-class scope, declared explicitly rather than left implicit: this predicate's
--    enumerable_scope filters on `visibility_workspace_id`, which private.memory_records'
--    `memory_records_visibility_matches_class` CHECK constraint (migration 0004) forces NOT
--    NULL only for `WORKSPACE_SHARED` rows — `USER_PRIVATE`/`TENANT_SHARED` rows always carry
--    `visibility_workspace_id IS NULL` and are therefore structurally excluded from this
--    denominator by construction. `rejected_decisions_v1` is deliberately scoped to the
--    WORKSPACE_SHARED lane only (a "被否决的方案" memory recorded via this predicate is a
--    workspace-level project decision, not a user-private note or a tenant-wide broadcast);
--    this is a declared 口径, not an oversight left for the next reader to guess at. A future
--    predicate meant to sweep all three visibility classes needs its own predicate_id/scope
--    shape — it structurally cannot filter on a single nullable column the way this one does.
--
--    Any tombstone/status overlay this predicate's EXACT lane must also honor (§23.1②) is
--    injected by whichever execution-side code runs `enumerable_scope` (the later
--    adapters-crate task predicate_registry.rs's module doc already defers to) — this text
--    column has no mechanism to express an overlay itself, and is not claimed to here.
UPDATE control.retrieval_predicates
SET enumerable_scope = 'private.memory_records WHERE tenant_id = $1 AND visibility_workspace_id = $2'
WHERE predicate_id = 'rejected_decisions_v1';
