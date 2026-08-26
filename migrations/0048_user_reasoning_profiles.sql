-- §11.2 UserReasoningProfile — control.user_reasoning_profiles. Pure additive DDL (new
-- table only); T4.4 "control.private_reasoning_domains 已在 P1 建...不够则加列" companion
-- table that P1's control_core migration deliberately did not create yet (§48.0① only
-- needed private_reasoning_domains itself as an FK target at that time).
--
-- §6.2.1: this table is not named in §6.2.2's per-table override matrix, so it falls back
-- to the `control.*` domain default (R for every runtime role, owner for
-- role_migration_owner). That default gives every worker role read access to resolve a
-- profile, but no runtime role — including role_gateway — gets INSERT/UPDATE here; the
-- profile-management API that lets a user create/edit their own BYOK profile is a later
-- task's §6.2.2 override (same deliberate-vacancy shape as control.quota_windows in
-- 0003, and the same class of gap 0042 disclosed for control.notifications).

CREATE TABLE control.user_reasoning_profiles (
  profile_id             uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id              uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  user_id                uuid        NOT NULL REFERENCES control.users(user_id),
  provider_id            text        NOT NULL,
  model_id               text        NOT NULL,
  -- CredentialRef pointer only (§7/§11.1) — never the secret bytes; raw key material lives
  -- in OpenBao, this FK is a locator (control.credentials is itself CredentialRef-only,
  -- §3's comment on that table).
  credential_ref         uuid        NOT NULL REFERENCES control.credentials(credential_id),
  -- §11.2 capability closed set: TEXT/VISION/STRUCTURED_OUTPUT/TOKEN_USAGE "至少" these
  -- four — CHECK enforces the closed set today; widening the set is a new migration, not a
  -- silent app-side change, per §78.2 "DB enum 与 Rust enum 走 contract test 对账".
  capabilities           text[]      NOT NULL,
  processing_region      text,
  custom_endpoint_policy jsonb,
  profile_version        bigint      NOT NULL DEFAULT 1,
  enabled                boolean     NOT NULL DEFAULT true,
  created_at             timestamptz NOT NULL DEFAULT now(),
  updated_at             timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT user_reasoning_profiles_capabilities_known CHECK (
    capabilities <@ ARRAY['TEXT', 'VISION', 'STRUCTURED_OUTPUT', 'TOKEN_USAGE']::text[]
    AND array_length(capabilities, 1) > 0
  )
);

COMMENT ON TABLE control.user_reasoning_profiles IS
  '§11.2: user-owned BYOK provider profile. processing_run snapshots pin (profile_id, '
  'profile_version) so a later key/provider/model change never rewrites a historical run''s '
  'interpretability (§11.2 "用户之后换 Key/Provider/Model，不改变历史 run 的可解释性").';

-- §62 tenant RLS, NULLIF form (post-0031, written directly — §11.2.1's own "profile owner
-- is a real user, not a tenant-global shared key" principle means the row predicate is
-- tenant AND owning-user, same shape as control.notifications' §74.7 policy in 0036).
ALTER TABLE control.user_reasoning_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.user_reasoning_profiles FORCE ROW LEVEL SECURITY;
CREATE POLICY user_reasoning_profiles_tenant_and_owner ON control.user_reasoning_profiles
USING (
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
  AND user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
  AND user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
);

-- §6.2.1 ownership: the migration-runner connection (`postgres` in dev) owns the table by
-- default at CREATE time, not role_migration_owner (0011's retroactive loop only reassigned
-- tables that already existed when it ran) — reassign explicitly, mirroring 0032/0036.
ALTER TABLE control.user_reasoning_profiles OWNER TO role_migration_owner;
