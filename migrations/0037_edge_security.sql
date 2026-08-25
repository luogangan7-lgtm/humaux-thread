-- §73.5 API Key / MCP Credential + §73 Admin/Support Privileged Access Plane's
-- SupportAccessRequest. Both tables are new, outside §6.2.2's 14-table matrix — grants come
-- from §6.2.1's domain default via 0011's `ALTER DEFAULT PRIVILEGES FOR ROLE <migration
-- runner>` (already laid down per schema, applies automatically to any table this same
-- connecting role creates); this file adds no GRANT of its own (§6.2.1 "非点名表" — do not
-- widen beyond the domain default the runtime roles get here).
--
-- §73.6's `control.sessions` is deliberately NOT built here: it belongs to §74.1's Email data
-- model (control.users/user_emails/.../sessions/...), landing in the H2 migration range.

-- =============================================================================
-- control.api_keys — §73.5. `prefix` for O(1) lookup, `key_hash` the keyed-hash verifier
-- (never the raw key, never reversible — `protocol::edge::compute_api_key_hash`,
-- HMAC-SHA256). Lifecycle `CREATE -> ACTIVE -> ROTATING -> REVOKED/EXPIRED`; the CHECK below
-- is the DB-side closed set, mirrored Rust-side by
-- `protocol::edge::is_legal_api_key_transition` (kept as a plain CHECK rather than a
-- transition-guard trigger, unlike `projection.stream_log` in 0011 — this table has no
-- cross-role writer split for a trigger to arbitrate; §6.2.2 does not name it).
-- =============================================================================

CREATE TABLE control.api_keys (
  api_key_id    uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id     uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  prefix        text        NOT NULL,
  key_hash      bytea       NOT NULL,
  scopes        text[]      NOT NULL DEFAULT '{}',
  allowed_cidrs cidr[]      NOT NULL DEFAULT '{}',
  status        text        NOT NULL DEFAULT 'CREATE'
                CHECK (status IN ('CREATE','ACTIVE','ROTATING','REVOKED','EXPIRED')),
  expires_at    timestamptz,
  revoked_at    timestamptz,
  last_used_at  timestamptz,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (prefix)
);

COMMENT ON TABLE control.api_keys IS
  '§73.5 API Key / MCP Credential. prefix for O(1) lookup, key_hash the HMAC-SHA256 keyed '
  'verifier (protocol::edge::compute_api_key_hash) — never the raw key, never logged '
  '(protocol::edge::api_key_log_fingerprint is the only sanctioned log-safe derivative). '
  'Lifecycle CREATE -> ACTIVE -> ROTATING -> REVOKED/EXPIRED, legality mirrored in '
  'protocol::edge::is_legal_api_key_transition.';

ALTER TABLE control.api_keys OWNER TO role_migration_owner;
ALTER TABLE control.api_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.api_keys FORCE ROW LEVEL SECURITY;

CREATE POLICY api_keys_tenant_isolation ON control.api_keys
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

-- =============================================================================
-- control.support_access_requests — §73 Admin/Support Privileged Access Plane's
-- SupportAccessRequest{tenant, reason, ticket, requested_scope, approved_by, starts_at,
-- expires_at}, field set verbatim (`created_at` added only as this schema's universal audit
-- housekeeping column, same as every other `control.*` table).
--
-- Deliberately named `target_tenant_id`, not `tenant_id`: this row is created and reviewed by
-- a support/ops actor operating ACROSS tenants — no session that touches this table ever
-- `SET LOCAL humaux.tenant_id` to the tenant being requested, so the standard per-request
-- tenant-isolation RLS policy (§62) does not apply here the way it does everywhere else.
-- Using the literal column name `tenant_id` would silently sweep this table into 0012's /
-- xtask rls_check's blanket tenant-table enumeration and either wall off Admin Plane
-- visibility entirely or force a `role_migration_owner`-style bypass — neither of which is
-- what this table is for. Access control here is the Admin Plane's own layer (§73.4:
-- separate route namespace / SSO+MFA / step-up auth / full audit), not a database RLS
-- predicate.
--
-- No `status` column: PENDING/APPROVED/ACTIVE/EXPIRED is fully derivable from
-- `approved_by IS NULL` and `(starts_at, expires_at)` against `now()` — a column that could
-- drift from those three would be exactly the decorative-column shape §9.1 already guards
-- against elsewhere in this schema.
--
-- §73 "所有 impersonation 记录（actor_user/impersonated_user/reason/action/request_id）必须
-- 不可省略": that is the audit trail for actions TAKEN under an approved grant, a distinct
-- concern from the grant request itself — it lands with H6's double-track audit event table
-- (§77), out of this migration's scope.
-- =============================================================================

CREATE TABLE control.support_access_requests (
  support_access_request_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  target_tenant_id          uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  reason                    text        NOT NULL,
  ticket                    text        NOT NULL,
  requested_scope           text[]      NOT NULL DEFAULT '{}',
  approved_by               uuid        REFERENCES control.users(user_id),
  starts_at                 timestamptz,
  expires_at                timestamptz,
  created_at                timestamptz NOT NULL DEFAULT now(),
  CHECK (expires_at IS NULL OR starts_at IS NULL OR expires_at > starts_at)
);

COMMENT ON TABLE control.support_access_requests IS
  '§73 SupportAccessRequest — a temporary, auditable grant for a support/ops actor to access '
  'one tenant''s private data. target_tenant_id (not tenant_id, deliberately — see file '
  'comment) is the tenant being accessed, not a request-issuer''s own tenant scope. Break-'
  'glass access (§73 Break-glass: strong auth / explicit reason / short TTL / immutable '
  'audit / post-incident review) is issued through this same table with an ADMIN-sourced '
  'reason, not a separate table.';

ALTER TABLE control.support_access_requests OWNER TO role_migration_owner;
