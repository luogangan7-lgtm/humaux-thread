-- §6.1.1 / §46 forward-fix / ADR-0035 (card 13). Re-point the WORKSPACE_SHARED arm ONLY.
--
-- 0153 (ADR-0027) re-pointed 0012's three visibility policies at private.visibility_allowed and
-- passed, as the p_workspace_ok boolean, an EXISTS over control.memberships (an ACTIVE *tenant*
-- membership). That is the §6.1.1 bug: a tenant member could read every workspace's
-- WORKSPACE_SHARED data. This migration changes ONLY that boolean, in the same three policies, to
-- an EXISTS over control.workspace_memberships (0162) matching the row's own
-- visibility_workspace_id: readable iff the reader holds an ACTIVE WorkspaceMembership for THAT
-- workspace. Card 7's pure private.visibility_allowed function is untouched; every other arm of
-- each policy (tenant boundary, role bypass arms, constrained write arms) is reproduced verbatim
-- from the live 0153 form. 0012 and 0153 bytes are untouched (§46: forward-fix via ALTER POLICY).
--
-- The frozen §6.1.1 policy-definition hash in xtask/src/rls_check.rs is intentionally re-pinned in
-- the same change (migration-file integrity hash unchanged; policy-definition hash intentionally
-- new — a legal forward-fix, not drift; ADR-0035 records this). No DDL on any table, no DML.
--
-- §46 forward-fix (EXPAND_CONTRACT, next free number after 0162).

-- evidence_objects: tenant + visibility (no role arm). USING == WITH CHECK.
ALTER POLICY evidence_objects_tenant_and_visibility ON private.evidence_objects
USING (
  tenant_id = current_setting('humaux.tenant_id', true)::uuid
  AND private.visibility_allowed(visibility_class, true,
        visibility_user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid,
        EXISTS (SELECT 1 FROM control.workspace_memberships wm
                WHERE wm.tenant_id = evidence_objects.tenant_id
                  AND wm.workspace_id = evidence_objects.visibility_workspace_id
                  AND wm.user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
                  AND wm.state = 'ACTIVE'))
)
WITH CHECK (
  tenant_id = current_setting('humaux.tenant_id', true)::uuid
  AND private.visibility_allowed(visibility_class, true,
        visibility_user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid,
        EXISTS (SELECT 1 FROM control.workspace_memberships wm
                WHERE wm.tenant_id = evidence_objects.tenant_id
                  AND wm.workspace_id = evidence_objects.visibility_workspace_id
                  AND wm.user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
                  AND wm.state = 'ACTIVE'))
);

-- memory_records: role_migration_owner cluster-scan arm + headless read arm on USING;
-- private_worker constrained write arm on WITH CHECK. Only the WORKSPACE_SHARED EXISTS changes.
ALTER POLICY memory_records_tenant_and_visibility ON private.memory_records
USING (
  current_user = 'role_migration_owner'
  OR (
    tenant_id = current_setting('humaux.tenant_id', true)::uuid
    AND (
      current_user IN ('role_retrieval_worker', 'role_consolidation_worker', 'role_private_worker')
      OR private.visibility_allowed(visibility_class, true,
           visibility_user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid,
           EXISTS (SELECT 1 FROM control.workspace_memberships wm
                   WHERE wm.tenant_id = memory_records.tenant_id
                     AND wm.workspace_id = memory_records.visibility_workspace_id
                     AND wm.user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
                     AND wm.state = 'ACTIVE'))
    )
  )
)
WITH CHECK (
  tenant_id = current_setting('humaux.tenant_id', true)::uuid
  AND (
    (current_user = 'role_private_worker'
     AND (visibility_class = 'TENANT_SHARED'
          OR (visibility_class = 'WORKSPACE_SHARED' AND visibility_workspace_id IS NOT NULL)
          OR (visibility_class = 'USER_PRIVATE' AND visibility_user_id IS NOT NULL)))
    OR private.visibility_allowed(visibility_class, true,
         visibility_user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid,
         EXISTS (SELECT 1 FROM control.workspace_memberships wm
                 WHERE wm.tenant_id = memory_records.tenant_id
                   AND wm.workspace_id = memory_records.visibility_workspace_id
                   AND wm.user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
                   AND wm.state = 'ACTIVE'))
  )
);

-- memory_rollups: headless read arm on USING; consolidation_worker constrained write arm on
-- WITH CHECK. Only the WORKSPACE_SHARED EXISTS changes.
ALTER POLICY memory_rollups_tenant_and_visibility ON private.memory_rollups
USING (
  tenant_id = current_setting('humaux.tenant_id', true)::uuid
  AND (
    current_user IN ('role_retrieval_worker', 'role_consolidation_worker', 'role_private_worker')
    OR private.visibility_allowed(visibility_class, true,
         visibility_user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid,
         EXISTS (SELECT 1 FROM control.workspace_memberships wm
                 WHERE wm.tenant_id = memory_rollups.tenant_id
                   AND wm.workspace_id = memory_rollups.visibility_workspace_id
                   AND wm.user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
                   AND wm.state = 'ACTIVE'))
  )
)
WITH CHECK (
  tenant_id = current_setting('humaux.tenant_id', true)::uuid
  AND (
    (current_user = 'role_consolidation_worker'
     AND (visibility_class = 'TENANT_SHARED'
          OR (visibility_class = 'WORKSPACE_SHARED' AND visibility_workspace_id IS NOT NULL)))
    OR private.visibility_allowed(visibility_class, true,
         visibility_user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid,
         EXISTS (SELECT 1 FROM control.workspace_memberships wm
                 WHERE wm.tenant_id = memory_rollups.tenant_id
                   AND wm.workspace_id = memory_rollups.visibility_workspace_id
                   AND wm.user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
                   AND wm.state = 'ACTIVE'))
  )
);
