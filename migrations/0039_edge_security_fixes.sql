-- §46.1 migration — fixer-review follow-up to 0035_edge_security. 0035 is already applied
-- and per repo policy applied migrations are additive-only, so these two fixes land as a
-- new migration rather than editing 0035 in place.

-- =============================================================================
-- Fix 1 (blocker, §62 / §48.2 tenant isolation, §73 Admin Plane 与 User Plane 分离):
-- control.support_access_requests had NO row-level security at all, so via 0011's
-- `ALTER DEFAULT PRIVILEGES` it inherited SELECT for all seven non-owner runtime roles —
-- an Admin-Plane table (target_tenant_id, reason, ticket) readable in full, across
-- tenants, through the User-Plane role_gateway.
--
-- This table intentionally has no `tenant_id` column (0035's own comment: `target_tenant_id`
-- is deliberate — the row is created/reviewed by a cross-tenant support/ops actor, so the
-- standard §62 tenant-equality policy does not apply). It must stay that way: xtask
-- rls_check's tenant-table enumeration is a literal `column_name = 'tenant_id'` scan
-- (§48.2/§62 domain), and giving this table a `tenant_id` column would sweep it into that
-- gate incorrectly. The fix is RLS enabled with NO permissive policy: the owner
-- (role_migration_owner) still reads every row; every runtime role gets zero rows via
-- PostgreSQL's RLS default-deny. This changes nothing at the GRANT layer — RLS filters
-- rows, it does not revoke privileges — so §6.2.1's control.* domain-default grant matrix,
-- and `rls-check 域默认授权`, are untouched.
-- =============================================================================

ALTER TABLE control.support_access_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.support_access_requests FORCE ROW LEVEL SECURITY;

COMMENT ON TABLE control.support_access_requests IS
  '§73 SupportAccessRequest — a temporary, auditable grant for a support/ops actor to access '
  'one tenant''s private data. target_tenant_id (not tenant_id, deliberately) is the tenant '
  'being accessed, not a request-issuer''s own tenant scope. RLS is enabled with NO '
  'permissive policy (0037): only role_migration_owner reads this table; every runtime role '
  'gets zero rows. Admin Plane access control is its own layer (§73.4: separate route '
  'namespace / SSO+MFA / step-up auth / full audit), not a database RLS predicate here.';

-- =============================================================================
-- Fix 2 (major, §73.5 "prefix for lookup" + "last_used_at"): control.api_keys' FORCE RLS
-- policy requires `humaux.tenant_id` to already be set — but API-key authentication is
-- what DETERMINES the tenant, so the GUC is necessarily unset at lookup time. role_gateway
-- (SELECT-only on control.* per §6.2.1 domain default) has no path to resolve a presented
-- key's prefix, and no path to ever write `last_used_at`. Two SECURITY DEFINER functions,
-- owned by role_migration_owner, give the gateway a single audited entry point for both,
-- without touching the table's RLS policy or the §6.2.1/§6.2.2 grant matrix.
-- =============================================================================

CREATE FUNCTION control.api_key_lookup(p_prefix text)
RETURNS TABLE (
  api_key_id    uuid,
  tenant_id     uuid,
  key_hash      bytea,
  status        text,
  allowed_cidrs cidr[],
  expires_at    timestamptz,
  revoked_at    timestamptz
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, control
AS $$
  SELECT api_key_id, tenant_id, key_hash, status, allowed_cidrs, expires_at, revoked_at
  FROM control.api_keys
  WHERE prefix = p_prefix;
$$;

ALTER FUNCTION control.api_key_lookup(text) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.api_key_lookup(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.api_key_lookup(text) TO role_gateway;

COMMENT ON FUNCTION control.api_key_lookup(text) IS
  '§73.5 API-key bootstrap lookup by prefix. SECURITY DEFINER: humaux.tenant_id is unset at '
  'auth time (this call is what determines it), so control.api_keys'' tenant-scoped FORCE '
  'RLS cannot be satisfied by role_gateway directly — this is the single audited bypass, '
  'scoped to exactly the columns §73.5 validate_api_key needs (protocol::edge).';

CREATE FUNCTION control.api_key_touch_last_used(p_api_key_id uuid)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, control
AS $$
  UPDATE control.api_keys SET last_used_at = now() WHERE api_key_id = p_api_key_id;
$$;

ALTER FUNCTION control.api_key_touch_last_used(uuid) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.api_key_touch_last_used(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.api_key_touch_last_used(uuid) TO role_gateway;

COMMENT ON FUNCTION control.api_key_touch_last_used(uuid) IS
  '§73.5 last_used_at bookkeeping. role_gateway only holds SELECT on control.* (§6.2.1 '
  'domain default) — this SECURITY DEFINER function is the sole write path, scoped to '
  'exactly one column of one row, no tenant GUC required.';
