-- §15.5 / §16.1.1 / §10.1 Distill hop (ADR-0016): the private worker turns accepted Evidence
-- into 0..N `private.memory_records` rows. Two things stood between the grants §6.2.2 already
-- gives `role_private_worker` (INSERT on memory_records / memory_evidence, SELECT+UPDATE on
-- ops.outbox, INSERT/UPDATE on processing_runs — all live since 0011/0104) and an actual write:
--
-- (1) `memory_records_tenant_and_visibility`'s WITH CHECK (0012, USING widened by 0140/0145 but
--     WITH CHECK never touched) has no headless-role arm: a WORKSPACE_SHARED row needs an
--     ACTIVE membership for `humaux.user_id`, which a headless worker session does not carry.
--     Same least-privilege shape 0146 gave `role_consolidation_worker` on memory_rollups: the
--     worker may write TENANT_SHARED, a WORKSPACE_SHARED row that names its workspace, or a
--     USER_PRIVATE row that names its user — the three values it copies verbatim from the
--     Evidence row (never widened, ADR-0016 D4) — and nothing else. USING is byte-identical to
--     0145's (pg_get_expr fetched before writing this file).
--     `private.memory_evidence`'s policy is `EXISTS(memory_records mr ... tenant)` (0012/0031)
--     and already admits the row the same transaction just inserted; `private.processing_runs`
--     has the §62 NULLIF tenant policy (0031). Neither needs a change.
--
-- (2) The route binding for purpose `PRIVATE_DISTILL_TEXT` is resolved by
--     (session tenant, reasoning_domain, purpose) — ADR-0016 D1: no binding id in the worker's
--     env. `control.resolve_user_reasoning_admission` (0130) takes a binding id, and
--     `control.reasoning_route_bindings` is a §6.2.2 zero-grant table ("禁止先给通用 runtime
--     SELECT 再补 owner gate"), so the lookup is a second narrow SECURITY DEFINER function with
--     the same owner / search_path / grant discipline as the resolver: it returns only the
--     `(binding_id, binding_version)` pair of the currently effective binding, nothing else.
--     `rls-check`'s R3 function gate pins it alongside the resolver.

ALTER POLICY memory_records_tenant_and_visibility ON private.memory_records
USING (
  current_user = 'role_migration_owner'
  OR (
    tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
    AND (
      current_user IN ('role_retrieval_worker', 'role_consolidation_worker', 'role_private_worker')
      OR visibility_class = 'TENANT_SHARED'
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
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
  AND (
    (current_user = 'role_private_worker'
     AND (visibility_class = 'TENANT_SHARED'
          OR (visibility_class = 'WORKSPACE_SHARED' AND visibility_workspace_id IS NOT NULL)
          OR (visibility_class = 'USER_PRIVATE' AND visibility_user_id IS NOT NULL)))
    OR visibility_class = 'TENANT_SHARED'
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

-- (2) narrow binding lookup — same session_scope recipe as the 0130 resolver: the tenant comes
-- from the session GUC, never from a parameter, so a caller cannot ask about another tenant.
CREATE FUNCTION control.current_reasoning_route_binding(
  p_reasoning_domain_id uuid,
  p_purpose text
)
RETURNS TABLE (
  binding_id uuid,
  binding_version bigint
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  RETURN QUERY
  SELECT b.binding_id, b.binding_version
  FROM control.reasoning_route_bindings b
  WHERE b.tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
    AND b.reasoning_domain_id = p_reasoning_domain_id
    AND b.purpose = p_purpose
    AND b.binding_version IS NOT NULL
    AND b.effective_from <= clock_timestamp()
    AND b.effective_to IS NULL
  ORDER BY b.effective_from DESC
  LIMIT 1;
END;
$$;

ALTER FUNCTION control.current_reasoning_route_binding(uuid, text)
  OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.current_reasoning_route_binding(uuid, text)
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker,
  role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance;
GRANT EXECUTE ON FUNCTION control.current_reasoning_route_binding(uuid, text)
  TO role_private_worker;

COMMENT ON FUNCTION control.current_reasoning_route_binding(uuid, text) IS
  'ADR-0016 D1: (binding_id, binding_version) of the effective route binding for (session tenant, reasoning domain, purpose). Narrow SECURITY DEFINER twin of resolve_user_reasoning_admission; role_private_worker-only EXECUTE.';
