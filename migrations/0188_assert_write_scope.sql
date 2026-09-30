-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0187). ADR-0054 D-B (card 29): the
-- in-transaction recheck of a per-request write scope. Authentication already derives, per
-- request, the live ACTIVE (workspace) membership ceiling and compares both security epochs
-- (gateway auth.rs); nothing re-checked it INSIDE the write transaction, so a suspend / removal /
-- epoch bump committing between authentication and the write's COMMIT did not stop that write.
--
-- control.assert_write_scope(api_key_id, workspace_id) is called by the one adapter helper
-- `confirm_token_repo::set_write_authorization_local` right after the caller's RLS GUCs are
-- installed, by every governance / subject / affect write transaction (both confirm legs). It
-- returns true or raises:
--   28000 (-> UNAUTHORIZED, what authentication answers for the same facts): credential missing,
--         foreign to the tenant GUC, revoked; tenant not ACTIVE or its epoch advanced; user
--         credential whose user is not the user GUC, not ACTIVE, epoch advanced, or whose tenant
--         membership is not ACTIVE.
--   42501 (-> FORBIDDEN, what the workspace narrow answers): machine credential bound to another
--         workspace; user credential bound to another workspace or without an ACTIVE workspace
--         membership in p_workspace_id.
-- The two membership rows are read FOR SHARE: a concurrent suspend / remove / set-role (their
-- UPDATEs, ADR-0033 / 0162) waits for this write to commit, or this write sees the new state and
-- aborts. Share locks do not conflict with each other, so one user's writes never serialise.
--
-- Owner definer, VOLATILE (it takes row locks), search_path pinned, every name qualified. The
-- owner is NOBYPASSRLS and the read tables are FORCE RLS: api_keys / tenants / memberships pass
-- through the 0112 owner SELECT policies and the 0012 tenant policies (the caller's tenant GUC also
-- satisfies the UPDATE-USING arm FOR SHARE needs); workspace_memberships through its 0162 tenant
-- ALL + restrictive self-read (the caller's user GUC). A missing GUC finds no row: refusal, never
-- admission. EXECUTE role_gateway only; no table grant changes.

CREATE FUNCTION control.assert_write_scope(p_api_key_id uuid, p_workspace_id uuid)
RETURNS boolean
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_tenant uuid := NULLIF(current_setting('humaux.tenant_id', true), '')::uuid;
  v_user uuid := NULLIF(current_setting('humaux.user_id', true), '')::uuid;
  k record;
BEGIN
  SELECT a.tenant_id, a.user_id, a.workspace_id, a.tenant_security_epoch, a.user_security_epoch
    INTO k
    FROM control.api_keys a
   WHERE a.api_key_id = p_api_key_id
     AND a.tenant_id = v_tenant
     AND a.revoked_at IS NULL;
  IF NOT FOUND OR p_workspace_id IS NULL THEN
    RAISE EXCEPTION 'write_scope_stale' USING ERRCODE = '28000';
  END IF;
  IF NOT EXISTS (
       SELECT 1 FROM control.tenants t
        WHERE t.tenant_id = k.tenant_id AND t.state = 'ACTIVE'
          AND t.security_epoch = k.tenant_security_epoch) THEN
    RAISE EXCEPTION 'write_scope_stale' USING ERRCODE = '28000';
  END IF;
  IF k.user_id IS NULL THEN
    IF k.workspace_id IS DISTINCT FROM p_workspace_id THEN
      RAISE EXCEPTION 'write_scope_workspace' USING ERRCODE = '42501';
    END IF;
    RETURN true;
  END IF;
  IF k.user_id IS DISTINCT FROM v_user OR NOT EXISTS (
       SELECT 1 FROM control.users u
        WHERE u.user_id = k.user_id AND u.state = 'ACTIVE'
          AND u.security_epoch = k.user_security_epoch) THEN
    RAISE EXCEPTION 'write_scope_stale' USING ERRCODE = '28000';
  END IF;
  PERFORM 1 FROM control.memberships m
   WHERE m.tenant_id = k.tenant_id AND m.user_id = k.user_id AND m.state = 'ACTIVE'
   FOR SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'write_scope_stale' USING ERRCODE = '28000';
  END IF;
  IF k.workspace_id IS NOT NULL AND k.workspace_id <> p_workspace_id THEN
    RAISE EXCEPTION 'write_scope_workspace' USING ERRCODE = '42501';
  END IF;
  PERFORM 1 FROM control.workspace_memberships w
   WHERE w.tenant_id = k.tenant_id AND w.workspace_id = p_workspace_id
     AND w.user_id = k.user_id AND w.state = 'ACTIVE'
   FOR SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'write_scope_workspace' USING ERRCODE = '42501';
  END IF;
  RETURN true;
END;
$$;

ALTER FUNCTION control.assert_write_scope(uuid, uuid) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.assert_write_scope(uuid, uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.assert_write_scope(uuid, uuid) TO role_gateway;

COMMENT ON FUNCTION control.assert_write_scope(uuid, uuid) IS
  'ADR-0054 D-B: in-transaction recheck of a per-request write scope (credential live + unrevoked, tenant ACTIVE at the snapshotted epoch, user ACTIVE at the snapshotted epoch, ACTIVE tenant membership FOR SHARE, ACTIVE workspace membership FOR SHARE / machine binding). 28000 = stale principal, 42501 = workspace not granted. Ceiling: the tenants and api_keys rows are rechecked but not locked (mark_used updates api_keys on every request), so a tenant suspend or key revocation committing between this call and COMMIT is not linearised; credential status/expiry stay authentication-time only.';
