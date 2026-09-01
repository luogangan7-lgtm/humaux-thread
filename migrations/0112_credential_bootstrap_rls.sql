-- §73.5.1: a SECURITY DEFINER still obeys FORCE RLS as its function owner.
-- Bootstrap has no tenant GUC yet. Keep the existing role/grant/ownership model,
-- and explicitly admit only its already-trusted migration/definer owner.
-- These are owner capabilities, NOT function-local or column-level sandboxes.

CREATE POLICY api_keys_bootstrap_owner_select ON control.api_keys
  FOR SELECT TO role_migration_owner
  USING (current_user = 'role_migration_owner');

CREATE POLICY tenants_bootstrap_owner_select ON control.tenants
  FOR SELECT TO role_migration_owner
  USING (current_user = 'role_migration_owner');

CREATE POLICY memberships_bootstrap_owner_select ON control.memberships
  FOR SELECT TO role_migration_owner
  USING (current_user = 'role_migration_owner');

-- RLS does not express a changed-column whitelist. The gateway-only, fixed SQL
-- api_key_touch_last_used(uuid) function remains the one request-side write path;
-- its body only sets one specified row's last_used_at. The owner itself already
-- owns the table and is never an application-pool principal.
CREATE POLICY api_keys_bootstrap_owner_update ON control.api_keys
  FOR UPDATE TO role_migration_owner
  USING (current_user = 'role_migration_owner')
  WITH CHECK (current_user = 'role_migration_owner');
