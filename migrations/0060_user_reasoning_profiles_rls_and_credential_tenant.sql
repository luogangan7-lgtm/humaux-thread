-- Fixer-review follow-up to 0048_user_reasoning_profiles: closes two gaps without editing the
-- already-applied 0048 (same pattern 0057_data_disclosures_truncate_guard used for 0047).
--
-- Fix 1 (§11.2 "Tenant 可以定义默认 Profile" / RLS availability): 0048's RLS policy required
-- BOTH `humaux.tenant_id` AND `humaux.user_id` to be set for a row to be visible at all. No
-- production code path in this workspace ever sets `humaux.user_id` (grepped: only
-- `crates/adapters/tests/auth_scope_rls.rs` and 0012's unrelated private-schema policies do),
-- so `humaux-private-worker` — the intended reader of this table — read zero rows through this
-- policy. It also made a tenant-default profile (by definition not owned by the reading user)
-- structurally unrepresentable. Split the predicate: reads (`USING`) are tenant-scoped only —
-- ownership enforcement for mutation moves to the future profile-management API (§6.2.2
-- override, not yet built: 0048's own comment already notes no runtime role has INSERT/UPDATE
-- here today) — writes (`WITH CHECK`) keep the stricter tenant-AND-owner predicate so this
-- migration narrows nothing an existing caller could rely on.
--
-- Required GUC from here on: `SET LOCAL humaux.tenant_id` alone is enough to SELECT; both
-- `humaux.tenant_id` AND `humaux.user_id` are still required to INSERT/UPDATE a row (a
-- tenant-default profile with no single owner is therefore write-only through a future
-- superuser/migration path, same class of gap 0048 already documented for
-- control.quota_windows in 0003).

DROP POLICY user_reasoning_profiles_tenant_and_owner ON control.user_reasoning_profiles;

CREATE POLICY user_reasoning_profiles_tenant_and_owner ON control.user_reasoning_profiles
USING (
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
  AND user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
);

-- Fix 2 (§7/§11.1 CredentialRef same-tenant invariant): `credential_ref` carried no same-tenant
-- constraint against `control.credentials` — a profile row in tenant B could point at tenant
-- A's credential row (FK checks bypass RLS, verified live). It failed closed later at decrypt
-- time (`control.credentials` has FORCE tenant-isolation RLS, so the read returns zero rows),
-- but the same class of cross-table tenant invariant got a real write-time trigger for the
-- exactly analogous `private_reasoning_domains`/`user_reasoning_profile_id` pair in 0049 — this
-- sibling invariant had nothing. Mirrors 0049's `private_reasoning_domains_check_profile_owner`
-- shape exactly (same BEFORE-trigger-because-CHECK-cannot-cross-reference-another-table reason).

CREATE FUNCTION control.user_reasoning_profiles_check_credential_tenant()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  cred_tenant uuid;
BEGIN
  SELECT tenant_id INTO cred_tenant
  FROM control.credentials
  WHERE credential_id = NEW.credential_ref;

  IF cred_tenant IS NULL THEN
    RAISE EXCEPTION 'credential_ref % does not exist', NEW.credential_ref;
  END IF;
  IF cred_tenant <> NEW.tenant_id THEN
    RAISE EXCEPTION '§7/§11.1: credential_ref must belong to the same tenant as the reasoning profile (profile tenant=%, credential tenant=%)', NEW.tenant_id, cred_tenant;
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER user_reasoning_profiles_check_credential_tenant
BEFORE INSERT OR UPDATE ON control.user_reasoning_profiles
FOR EACH ROW EXECUTE FUNCTION control.user_reasoning_profiles_check_credential_tenant();

ALTER FUNCTION control.user_reasoning_profiles_check_credential_tenant() OWNER TO role_migration_owner;
