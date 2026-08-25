-- §62 RLS tenant policy template + §6.1.1 private visibility predicate. Blanket-applies
-- the §62 template (ENABLE + FORCE RLS, USING/WITH CHECK on tenant_id ==
-- current_setting('humaux.tenant_id', true)::uuid) to every table carrying a `tenant_id`
-- column — the exact enumeration domain xtask/src/rls_check.rs's "RLS 四项" check scans
-- (any schema, `information_schema.columns.column_name = 'tenant_id'`). private.
-- evidence_objects and private.memory_records additionally stack the §6.1.1 visibility
-- disjunction (USER_PRIVATE/WORKSPACE_SHARED/TENANT_SHARED) — the only two tables in this
-- migration set that carry visibility_class/visibility_user_id/visibility_workspace_id
-- columns of their own (§48.0①, T0.8's memory_records addition); every other private
-- table's content-level visibility is enforced where its content actually lives, not
-- re-derived here.

-- =============================================================================
-- Blanket tenant policy for every OTHER table with a tenant_id column. evidence_objects
-- and memory_records are excluded here and handled explicitly below with the combined
-- tenant+visibility predicate, because PostgreSQL combines multiple PERMISSIVE policies
-- on the same table with OR — a separate plain tenant-only policy alongside a visibility
-- policy would let tenant membership alone satisfy the row check, silently widening
-- access instead of narrowing it. One policy per table keeps AND semantics without
-- reaching for RESTRICTIVE policies this migration does not otherwise need.
-- =============================================================================

DO $$
DECLARE
  rec record;
BEGIN
  FOR rec IN
    SELECT DISTINCT c.table_schema, c.table_name
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema AND t.table_name = c.table_name AND t.table_type = 'BASE TABLE'
    WHERE c.column_name = 'tenant_id'
      AND NOT (c.table_schema = 'private' AND c.table_name IN ('evidence_objects', 'memory_records'))
      -- coord.locks / ops.consistency_reports carry a NULLABLE tenant_id by design (a
      -- cross-tenant lock resource, a cluster-wide consistency report) — the blanket
      -- `tenant_id = current_setting(...)::uuid` predicate below evaluates to NULL (not
      -- true) for a NULL tenant_id row under any session context, so it would make those
      -- rows permanently invisible/unwritable to every non-superuser role including
      -- role_maintenance. Handled explicitly below with an `IS NULL OR` branch instead.
      AND NOT (c.table_schema = 'coord' AND c.table_name = 'locks')
      AND NOT (c.table_schema = 'ops' AND c.table_name = 'consistency_reports')
  LOOP
    EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY', rec.table_schema, rec.table_name);
    EXECUTE format('ALTER TABLE %I.%I FORCE ROW LEVEL SECURITY', rec.table_schema, rec.table_name);
    EXECUTE format(
      'CREATE POLICY %I ON %I.%I USING (tenant_id = current_setting(''humaux.tenant_id'', true)::uuid) WITH CHECK (tenant_id = current_setting(''humaux.tenant_id'', true)::uuid)',
      rec.table_name || '_tenant_isolation', rec.table_schema, rec.table_name
    );
  END LOOP;
END
$$;

-- =============================================================================
-- private.evidence_objects / private.memory_records: tenant boundary AND §6.1.1
-- visibility disjunction. WORKSPACE_SHARED's "W ∈ allowed_workspace_ids 且
-- membership/role 允许" collapses here to "an ACTIVE tenant membership exists" —
-- control.memberships is tenant-scoped, not workspace-scoped, so this is a coarser
-- approximation of §6.1.1's full per-workspace grant, not the final word; T1.5's
-- AuthorizationScope/can_read is where the real workspace-membership table and the
-- narrower check land. Flagged, not silently treated as done.
-- =============================================================================

CREATE POLICY evidence_objects_tenant_and_visibility ON private.evidence_objects
USING (
  tenant_id = current_setting('humaux.tenant_id', true)::uuid
  AND (
    visibility_class = 'TENANT_SHARED'
    OR (visibility_class = 'USER_PRIVATE'
        AND visibility_user_id = current_setting('humaux.user_id', true)::uuid)
    OR (visibility_class = 'WORKSPACE_SHARED'
        AND EXISTS (
          SELECT 1 FROM control.memberships m
          WHERE m.tenant_id = evidence_objects.tenant_id
            AND m.user_id = current_setting('humaux.user_id', true)::uuid
            AND m.state = 'ACTIVE'
        ))
  )
)
WITH CHECK (
  tenant_id = current_setting('humaux.tenant_id', true)::uuid
  AND (
    visibility_class = 'TENANT_SHARED'
    OR (visibility_class = 'USER_PRIVATE'
        AND visibility_user_id = current_setting('humaux.user_id', true)::uuid)
    OR (visibility_class = 'WORKSPACE_SHARED'
        AND EXISTS (
          SELECT 1 FROM control.memberships m
          WHERE m.tenant_id = evidence_objects.tenant_id
            AND m.user_id = current_setting('humaux.user_id', true)::uuid
            AND m.state = 'ACTIVE'
        ))
  )
);

ALTER TABLE private.evidence_objects ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.evidence_objects FORCE ROW LEVEL SECURITY;

CREATE POLICY memory_records_tenant_and_visibility ON private.memory_records
USING (
  -- §65 daily job / §59.1 I4: ops.i4_authority_consistency_scan() (0004) is
  -- SECURITY DEFINER owned by role_migration_owner specifically so this cluster-wide
  -- integrity scan can see every tenant's rows with no `humaux.tenant_id`/`user_id`
  -- session context (a daily repair job has no single tenant). This branch is that
  -- function's only extra reach — role_migration_owner is `migration only` (§6.2.1,
  -- never in a runtime connection pool), so the widening never touches a live request.
  current_user = 'role_migration_owner'
  OR (
    tenant_id = current_setting('humaux.tenant_id', true)::uuid
    AND (
      visibility_class = 'TENANT_SHARED'
      OR (visibility_class = 'USER_PRIVATE'
          AND visibility_user_id = current_setting('humaux.user_id', true)::uuid)
      OR (visibility_class = 'WORKSPACE_SHARED'
          AND EXISTS (
            SELECT 1 FROM control.memberships m
            WHERE m.tenant_id = memory_records.tenant_id
              AND m.user_id = current_setting('humaux.user_id', true)::uuid
              AND m.state = 'ACTIVE'
          ))
    )
  )
)
WITH CHECK (
  tenant_id = current_setting('humaux.tenant_id', true)::uuid
  AND (
    visibility_class = 'TENANT_SHARED'
    OR (visibility_class = 'USER_PRIVATE'
        AND visibility_user_id = current_setting('humaux.user_id', true)::uuid)
    OR (visibility_class = 'WORKSPACE_SHARED'
        AND EXISTS (
          SELECT 1 FROM control.memberships m
          WHERE m.tenant_id = memory_records.tenant_id
            AND m.user_id = current_setting('humaux.user_id', true)::uuid
            AND m.state = 'ACTIVE'
        ))
  )
);

ALTER TABLE private.memory_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.memory_records FORCE ROW LEVEL SECURITY;

-- =============================================================================
-- §6.1.1: "PostgreSQL RLS 不只检查 tenant_id" — tables that carry no `tenant_id` column of
-- their own but whose content (or link) is scoped by a FK into a table already policed
-- above. The blanket loop's enumeration domain is a literal `tenant_id` column
-- (information_schema.columns), so it silently skips every one of these — leaving them
-- with zero DB-level tenant isolation. Demonstrated: two tenants, `SET ROLE
-- role_private_worker; SET humaux.tenant_id='<A>'; SELECT payload FROM private.events;`
-- returned rows belonging to tenant B before this block existed.
-- =============================================================================

-- private.events / private.artifacts are 1:1 sub-tables sharing evidence_objects' PK
-- (event_id/artifact_id == evidence_id, §48.0①) and hold Evidence's actual payload — this
-- is where "content-level visibility... enforced where its content actually lives" (the
-- comment at the top of this file) has to mean these two tables specifically, not skip
-- them. Full tenant + §6.1.1 visibility disjunction via evidence_objects, not tenant-only.
CREATE POLICY events_tenant_and_visibility ON private.events
USING (
  EXISTS (
    SELECT 1 FROM private.evidence_objects eo
    WHERE eo.evidence_id = events.event_id
      AND eo.tenant_id = current_setting('humaux.tenant_id', true)::uuid
      AND (
        eo.visibility_class = 'TENANT_SHARED'
        OR (eo.visibility_class = 'USER_PRIVATE'
            AND eo.visibility_user_id = current_setting('humaux.user_id', true)::uuid)
        OR (eo.visibility_class = 'WORKSPACE_SHARED'
            AND EXISTS (
              SELECT 1 FROM control.memberships m
              WHERE m.tenant_id = eo.tenant_id
                AND m.user_id = current_setting('humaux.user_id', true)::uuid
                AND m.state = 'ACTIVE'
            ))
      )
  )
)
WITH CHECK (
  EXISTS (
    SELECT 1 FROM private.evidence_objects eo
    WHERE eo.evidence_id = events.event_id
      AND eo.tenant_id = current_setting('humaux.tenant_id', true)::uuid
  )
);

ALTER TABLE private.events ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.events FORCE ROW LEVEL SECURITY;

CREATE POLICY artifacts_tenant_and_visibility ON private.artifacts
USING (
  EXISTS (
    SELECT 1 FROM private.evidence_objects eo
    WHERE eo.evidence_id = artifacts.artifact_id
      AND eo.tenant_id = current_setting('humaux.tenant_id', true)::uuid
      AND (
        eo.visibility_class = 'TENANT_SHARED'
        OR (eo.visibility_class = 'USER_PRIVATE'
            AND eo.visibility_user_id = current_setting('humaux.user_id', true)::uuid)
        OR (eo.visibility_class = 'WORKSPACE_SHARED'
            AND EXISTS (
              SELECT 1 FROM control.memberships m
              WHERE m.tenant_id = eo.tenant_id
                AND m.user_id = current_setting('humaux.user_id', true)::uuid
                AND m.state = 'ACTIVE'
            ))
      )
  )
)
WITH CHECK (
  EXISTS (
    SELECT 1 FROM private.evidence_objects eo
    WHERE eo.evidence_id = artifacts.artifact_id
      AND eo.tenant_id = current_setting('humaux.tenant_id', true)::uuid
  )
);

ALTER TABLE private.artifacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.artifacts FORCE ROW LEVEL SECURITY;

-- Pure link/junction tables carry no payload of their own — tenant scoping via the parent
-- FK is sufficient (no separate visibility disjunction to re-derive; the parent row's own
-- policy already governs whether the parent is visible at all).
CREATE POLICY memory_evidence_tenant ON private.memory_evidence
USING (EXISTS (
  SELECT 1 FROM private.memory_records mr
  WHERE mr.memory_id = memory_evidence.memory_id
    AND mr.tenant_id = current_setting('humaux.tenant_id', true)::uuid
))
WITH CHECK (EXISTS (
  SELECT 1 FROM private.memory_records mr
  WHERE mr.memory_id = memory_evidence.memory_id
    AND mr.tenant_id = current_setting('humaux.tenant_id', true)::uuid
));

ALTER TABLE private.memory_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.memory_evidence FORCE ROW LEVEL SECURITY;

CREATE POLICY evidence_edges_tenant ON private.evidence_edges
USING (EXISTS (
  SELECT 1 FROM private.evidence_objects eo
  WHERE eo.evidence_id = evidence_edges.child_evidence_id
    AND eo.tenant_id = current_setting('humaux.tenant_id', true)::uuid
))
WITH CHECK (EXISTS (
  SELECT 1 FROM private.evidence_objects eo
  WHERE eo.evidence_id = evidence_edges.child_evidence_id
    AND eo.tenant_id = current_setting('humaux.tenant_id', true)::uuid
));

ALTER TABLE private.evidence_edges ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.evidence_edges FORCE ROW LEVEL SECURITY;

CREATE POLICY memory_consolidation_inputs_tenant ON private.memory_consolidation_inputs
USING (EXISTS (
  SELECT 1 FROM private.memory_consolidation_runs r
  WHERE r.run_id = memory_consolidation_inputs.run_id
    AND r.tenant_id = current_setting('humaux.tenant_id', true)::uuid
))
WITH CHECK (EXISTS (
  SELECT 1 FROM private.memory_consolidation_runs r
  WHERE r.run_id = memory_consolidation_inputs.run_id
    AND r.tenant_id = current_setting('humaux.tenant_id', true)::uuid
));

ALTER TABLE private.memory_consolidation_inputs ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.memory_consolidation_inputs FORCE ROW LEVEL SECURITY;

CREATE POLICY memory_rollup_sources_tenant ON private.memory_rollup_sources
USING (EXISTS (
  SELECT 1 FROM private.memory_rollups mr
  WHERE mr.rollup_id = memory_rollup_sources.rollup_id
    AND mr.tenant_id = current_setting('humaux.tenant_id', true)::uuid
))
WITH CHECK (EXISTS (
  SELECT 1 FROM private.memory_rollups mr
  WHERE mr.rollup_id = memory_rollup_sources.rollup_id
    AND mr.tenant_id = current_setting('humaux.tenant_id', true)::uuid
));

ALTER TABLE private.memory_rollup_sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.memory_rollup_sources FORCE ROW LEVEL SECURITY;

CREATE POLICY contribution_release_sources_tenant ON staging.contribution_release_sources
USING (EXISTS (
  SELECT 1 FROM staging.contribution_releases cr
  WHERE cr.contribution_release_id = contribution_release_sources.contribution_release_id
    AND cr.tenant_id = current_setting('humaux.tenant_id', true)::uuid
))
WITH CHECK (EXISTS (
  SELECT 1 FROM staging.contribution_releases cr
  WHERE cr.contribution_release_id = contribution_release_sources.contribution_release_id
    AND cr.tenant_id = current_setting('humaux.tenant_id', true)::uuid
));

ALTER TABLE staging.contribution_release_sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE staging.contribution_release_sources FORCE ROW LEVEL SECURITY;

-- =============================================================================
-- §62 minor: coord.locks / ops.consistency_reports carry a NULLABLE tenant_id by design
-- (a cross-tenant lock resource, a cluster-wide consistency report) — excluded from the
-- blanket loop above and given an explicit `tenant_id IS NULL OR` branch here so a global
-- row stays visible/writable instead of becoming permanently inaccessible to every
-- non-superuser role under the blanket loop's plain equality predicate.
-- =============================================================================

CREATE POLICY locks_tenant_isolation ON coord.locks
USING (tenant_id IS NULL OR tenant_id = current_setting('humaux.tenant_id', true)::uuid)
WITH CHECK (tenant_id IS NULL OR tenant_id = current_setting('humaux.tenant_id', true)::uuid);

ALTER TABLE coord.locks ENABLE ROW LEVEL SECURITY;
ALTER TABLE coord.locks FORCE ROW LEVEL SECURITY;

CREATE POLICY consistency_reports_tenant_isolation ON ops.consistency_reports
USING (tenant_id IS NULL OR tenant_id = current_setting('humaux.tenant_id', true)::uuid)
WITH CHECK (tenant_id IS NULL OR tenant_id = current_setting('humaux.tenant_id', true)::uuid);

ALTER TABLE ops.consistency_reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.consistency_reports FORCE ROW LEVEL SECURITY;
