-- §11.6/§11.8 + ADR-0015 D3, extended to the rollup table itself. Verified live after 0145
-- (consolidation_hop_e2e T4/T1): `private.memory_rollups`' policy
-- `memory_rollups_tenant_and_visibility` (0058) has no headless-role disjunct, so a
-- `role_consolidation_worker` session — tenant-pinned, no `humaux.user_id` — fails the
-- WITH CHECK (42501) when `publish_rollup` inserts the WORKSPACE_SHARED rollup it derives from a
-- workspace-scoped run. Same reasoning as 0140/0145: a headless worker can never satisfy the
-- membership branch; per-query visibility is enforced downstream (§6.1.2).
--
-- Least privilege on the write side: the worker may only write TENANT_SHARED or a
-- WORKSPACE_SHARED row that names its workspace — never USER_PRIVATE. The read side admits the
-- three headless roles (retrieval / consolidation / private worker) like 0140/0145 do for
-- memory_records. Every other branch is byte-identical to the live pg_get_expr of 0058.
ALTER POLICY memory_rollups_tenant_and_visibility ON private.memory_rollups
  USING (
    tenant_id = current_setting('humaux.tenant_id', true)::uuid
    AND (
      current_user IN ('role_retrieval_worker', 'role_consolidation_worker', 'role_private_worker')
      OR visibility_class = 'TENANT_SHARED'
      OR (visibility_class = 'USER_PRIVATE'
          AND visibility_user_id = current_setting('humaux.user_id', true)::uuid)
      OR (visibility_class = 'WORKSPACE_SHARED'
          AND EXISTS (SELECT 1 FROM control.memberships m
                      WHERE m.tenant_id = memory_rollups.tenant_id
                        AND m.user_id = current_setting('humaux.user_id', true)::uuid
                        AND m.state = 'ACTIVE'))
    )
  )
  WITH CHECK (
    tenant_id = current_setting('humaux.tenant_id', true)::uuid
    AND (
      (current_user = 'role_consolidation_worker'
       AND (visibility_class = 'TENANT_SHARED'
            OR (visibility_class = 'WORKSPACE_SHARED' AND visibility_workspace_id IS NOT NULL)))
      OR visibility_class = 'TENANT_SHARED'
      OR (visibility_class = 'USER_PRIVATE'
          AND visibility_user_id = current_setting('humaux.user_id', true)::uuid)
      OR (visibility_class = 'WORKSPACE_SHARED'
          AND EXISTS (SELECT 1 FROM control.memberships m
                      WHERE m.tenant_id = memory_rollups.tenant_id
                        AND m.user_id = current_setting('humaux.user_id', true)::uuid
                        AND m.state = 'ACTIVE'))
    )
  );
