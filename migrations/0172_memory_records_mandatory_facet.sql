-- §25.4.A v1 (card 22b, ADR-0045): give `required_current_state_facets_v1` the column it
-- probes, as a derived projection with NO independent write entry.
--
-- Why a GENERATED column and not a written one: §25.4.A(3) freezes
-- `mandatory_facet(m) = f(m.memory_type)` — a pure function of one column of the same row.
-- A writable column would be a second source of truth the API, the distiller, consolidation
-- or a backfill script could set independently, which is exactly the thing the ruling
-- forbids. `GENERATED ALWAYS ... STORED` makes "nobody can write it" a property of the
-- engine rather than a review rule: PostgreSQL rejects INSERT/UPDATE of the column outright,
-- so no grant is needed and none is given.
--
-- What it is NOT (§25.4.A(1)/(4)/(5)): not §25.2's display slots, not §24's CandidateVariant
-- set, and not "current / authoritative / must-include". A non-NULL facet only means the
-- type-driven selector MAY nominate the row; scope, authority/origin, lifecycle, temporal
-- validity, conflict and grounding still decide admission. A NULL facet does NOT forbid the
-- memory from entering Mandatory through an explicit binding.
--
-- REJECTION -> 'decisions' is a NEW explicit product contract adopted here (ruling §一.2), not
-- a derivation from §24's reserve names: an explicitly rejected route must stay
-- deterministically reachable instead of depending on similarity recall. It does not rewrite
-- `memory_type`, and it does not merge Continuity's separate Decision / Rejection slots.
--
-- The CASE compares the 12 UPPERCASE labels of `memory_records_memory_type_check` (migration
-- 0004) verbatim; the Rust half is `humaux_domain::memory::facet_for`, and the §78.2 contract
-- test `crates/adapters/tests/facet_contract.rs` reconciles the two against the live column.
--
-- NOT zero-downtime: adding a STORED generated column REWRITES the table and its indexes.
-- On the shared dev database that is 6,736 rows — seconds — but say it rather than imply it.
-- Additive and NULL-safe: every pre-existing row gets its facet computed from the
-- `memory_type` it already has; nothing is deleted, nothing is updated by hand, no backfill
-- statement exists (the engine computes the value during the rewrite).
--
-- No index. Measured decision (recorded in ADR-0045): the facets selector's predicate is
-- `tenant_id = $1 AND facet = ANY($2)` over 6,736 rows on dev; a sequential scan of that
-- table is sub-millisecond and the selector already runs inside a single REPEATABLE READ
-- assembly transaction alongside four other scans. The ruling's optional
-- `(tenant_id, facet, memory_id) WHERE facet IS NOT NULL` index is deferred until a
-- deployment measures the scan as material — adding it later is a CONCURRENTLY forward-fix
-- with no contract change. The ruling's optional bindings index is NOT added either: the
-- task selector's seed predicate is already served by `ix_context_bindings_active_scope`
-- (tenant_id, scope_kind, COALESCE(scope_id, tenant_id), mode) WHERE revoked_at IS NULL.
--
-- Grants: `private.memory_records` is granted at TABLE level (SELECT to role_gateway,
-- role_consolidation_worker, role_maintenance, role_private_worker, role_retrieval_worker;
-- INSERT to role_gateway and role_private_worker), so the new column is readable by exactly
-- the roles that could already read the row and writable by nobody. No §6.2.2 grant cell
-- changes and no `xtask rls_check` MATRIX row changes — the matrix is table x role, and no
-- policy or SECURITY DEFINER function references `facet`. Task is a selector association
-- range, not a tenant boundary (§25.4.A(7)): no policy arm is added.

ALTER TABLE private.memory_records
  ADD COLUMN facet text
  GENERATED ALWAYS AS (
    CASE memory_type
      WHEN 'STATE'      THEN 'state'
      WHEN 'CONSTRAINT' THEN 'constraints'
      WHEN 'DECISION'   THEN 'decisions'
      WHEN 'REJECTION'  THEN 'decisions'
      WHEN 'ISSUE'      THEN 'issues'
      ELSE NULL
    END
  ) STORED;

ALTER TABLE private.memory_records
  ADD CONSTRAINT memory_records_facet_v1_check
  CHECK (facet IS NULL OR facet IN ('state', 'constraints', 'decisions', 'issues'));

COMMENT ON COLUMN private.memory_records.facet IS
  '§25.4.A(1)-(5) MandatoryContextFacet projection: a GENERATED ALWAYS ... STORED function of memory_type alone (state/constraints/decisions/issues, NULL for the other seven types). No independent write entry. Non-NULL means required_current_state_facets_v1 MAY nominate the row; it is not a claim that the row is current, authoritative or must-include. NULL does not forbid Mandatory entry through an explicit binding.';
