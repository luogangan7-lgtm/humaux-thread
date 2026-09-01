-- §73.5.1 headless service credentials: preserve legacy rows as inert, and bind
-- every v1 credential to its tenant/workspace/user epoch snapshot without creating a
-- second credential authority.

ALTER TABLE control.workspaces
  ADD CONSTRAINT workspaces_tenant_workspace_key UNIQUE (tenant_id, workspace_id);

ALTER TABLE control.api_keys
  ADD COLUMN authorization_version smallint,
  ADD COLUMN user_id uuid REFERENCES control.users(user_id),
  ADD COLUMN workspace_id uuid,
  ADD COLUMN tenant_security_epoch bigint,
  ADD COLUMN user_security_epoch bigint,
  ADD CONSTRAINT api_keys_workspace_same_tenant_fkey
    FOREIGN KEY (tenant_id, workspace_id)
    REFERENCES control.workspaces(tenant_id, workspace_id),
  ADD CONSTRAINT api_keys_authorization_binding_check CHECK (
    (
      authorization_version IS NULL
      AND user_id IS NULL
      AND workspace_id IS NULL
      AND tenant_security_epoch IS NULL
      AND user_security_epoch IS NULL
    )
    OR
    (
      authorization_version IS NOT NULL
      AND authorization_version = 1
      AND tenant_security_epoch IS NOT NULL
      AND tenant_security_epoch >= 0
      AND (
        (user_id IS NULL AND user_security_epoch IS NULL)
        OR
        (user_id IS NOT NULL AND user_security_epoch IS NOT NULL AND user_security_epoch >= 0)
      )
    )
  );

COMMENT ON CONSTRAINT api_keys_authorization_binding_check ON control.api_keys IS
  '§73.5.1: legacy bindings are all NULL and inert; v1 pins a nonnegative tenant epoch, and PAT rows pin a nonnegative user epoch while machine rows leave it NULL.';

DROP FUNCTION control.api_key_lookup(text);

CREATE FUNCTION control.api_key_lookup(p_prefix text)
RETURNS TABLE (
  api_key_id uuid,
  tenant_id uuid,
  key_hash bytea,
  status text,
  allowed_cidrs cidr[],
  expires_at timestamptz,
  revoked_at timestamptz,
  scopes text[],
  authorization_version smallint,
  user_id uuid,
  workspace_id uuid,
  tenant_security_epoch bigint,
  user_security_epoch bigint,
  tenant_state text,
  live_tenant_security_epoch bigint,
  user_state text,
  live_user_security_epoch bigint,
  membership_state text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, control
AS $$
  SELECT
    k.api_key_id,
    k.tenant_id,
    k.key_hash,
    k.status,
    k.allowed_cidrs,
    k.expires_at,
    k.revoked_at,
    k.scopes,
    k.authorization_version,
    k.user_id,
    k.workspace_id,
    k.tenant_security_epoch,
    k.user_security_epoch,
    t.state,
    t.security_epoch,
    u.state,
    u.security_epoch,
    m.state
  FROM control.api_keys AS k
  JOIN control.tenants AS t ON t.tenant_id = k.tenant_id
  LEFT JOIN control.users AS u ON u.user_id = k.user_id
  LEFT JOIN control.memberships AS m
    ON m.tenant_id = k.tenant_id AND m.user_id = k.user_id
  WHERE k.prefix = p_prefix;
$$;

ALTER FUNCTION control.api_key_lookup(text) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.api_key_lookup(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.api_key_lookup(text) TO role_gateway;

COMMENT ON FUNCTION control.api_key_lookup(text) IS
  '§73.5.1 single gateway-only bootstrap lookup. One SELECT returns credential snapshots and live tenant/user/membership facts from the same statement snapshot; legacy NULL bindings remain visible so the authentication policy can reject them explicitly.';
