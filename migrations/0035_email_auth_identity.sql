-- §74 Email 注册/登录/验证码/账户恢复/变更 — H2/T2.3 schema.
--
-- §74.1 table list, verbatim: control.users / control.user_emails / control.email_challenges
-- / control.password_credentials / control.sessions / control.password_reset_challenges /
-- control.email_change_requests / control.auth_events. control.users already exists (0003,
-- T1.1) — that migration's own comment reserves "§74's richer auth fields" for this Phase 2
-- migration number range, so this file only ADDs the seven sibling tables, it does not ALTER
-- control.users: the §74.2 five-state signup machine (START/SIGNUP_PENDING/
-- EMAIL_CHALLENGE_SENT/EMAIL_VERIFIED/ACCOUNT_ACTIVE) is derived in
-- `humaux_application::auth::signup_state` from three facts this schema already carries —
-- control.users.state (PENDING_VERIFICATION|ACTIVE|...), control.user_emails.verified_at, and
-- whether an email_challenges row exists for that email — instead of adding an eighth status
-- column that would duplicate/shadow T1.1's own state column. control.users.security_epoch
-- (0003, already bigint default 0) is reused as-is for §74.4's "increment session_epoch /
-- revoke sessions" step; no new epoch column on control.users either.
--
-- §74.1 "只规范化 domain case；不要擅自实现 Gmail 去点、加号折叠等 provider-specific
-- 规则": enforced in application code (`humaux_application::auth::canonicalize_email`), not
-- as a DB generated column — canonicalization needs Unicode-aware lowercasing beyond a SQL
-- `lower()` on the domain substring.
--
-- None of these eight tables carry `tenant_id`: identity is not tenant-scoped in this schema
-- (0003's own comment on control.users — one User can hold memberships in multiple
-- Organization Tenants via control.memberships, so tenant_id lives there, not on identity/auth
-- rows). xtask/src/rls_check.rs's RLS four-item enumeration scans exactly the set of tables
-- carrying a `tenant_id` column (§48.2/§62) — none of these qualify, so none need
-- ENABLE/FORCE ROW LEVEL SECURITY or a tenant policy; the CLAUDE.md "tenant 表" RLS
-- instruction is conditional on carrying tenant_id and does not apply here.
--
-- Grants: no §6.2.2 point-named MATRIX cells are added for any of these tables. §6.2.2's own
-- text freezes its column set as "S = 全文权限断言点名的表 ∪ 全文 SQL 代码块里被写的表"
-- (a live scan of Baseline_2.8.md itself, xtask/src/rls_check.rs::extract_s_write/
-- extract_s_grant) — §74's prose describes the flows in ```text blocks, not ```sql code with
-- literal INSERT/UPDATE targets or role+verb+backtick-table assertions, so S does not name any
-- of these eight tables and extending the matrix here would only drift MATRIX away from what
-- `check_table_set_derivation` can independently re-derive from the spec text. Every table
-- below therefore gets exactly §6.2.1's domain default for `control.*` (SELECT-only for every
-- non-owner role) via 0011's already-installed `ALTER DEFAULT PRIVILEGES FOR ROLE <runner> IN
-- SCHEMA control ...` — no GRANT statement is needed in this file for that to take effect,
-- since migrate.rs runs every migrations/*.sql file over one session/connection in filename
-- order (0011's own comment) and default privileges key off the creating role, which does not
-- change between 0011 and this file.
--
-- Ownership: explicit `ALTER TABLE ... OWNER TO role_migration_owner` per table below (P1
-- pattern) rather than leaving it to the connecting migration-runner role — 0011's own comment
-- documents that gap already reproduced once (`ops.column_vitality`, 0030, left owned by
-- `postgres`).

-- =============================================================================
-- control.user_emails — §74.1 original_email/canonical_email/verified_at.
-- =============================================================================
CREATE TABLE control.user_emails (
  user_email_id    uuid        PRIMARY KEY DEFAULT uuidv7(),
  user_id          uuid        NOT NULL REFERENCES control.users(user_id),
  original_email   text        NOT NULL,
  canonical_email  text        NOT NULL,
  is_primary       boolean     NOT NULL DEFAULT true,
  verified_at      timestamptz,
  created_at       timestamptz NOT NULL DEFAULT now(),
  -- One canonical address maps to at most one user: the §74.3 login lookup key and the
  -- structural guard against two accounts racing to claim the same mailbox. Enumeration
  -- safety (same response whether or not this unique constraint is what rejected a signup)
  -- is an application-layer contract (`humaux_application::auth`'s unified error type), not a
  -- DB-visible one — a UNIQUE violation by itself never reaches the caller as a distinct code.
  UNIQUE (canonical_email)
);
COMMENT ON TABLE control.user_emails IS
  '§74.1 email storage: original_email as submitted, canonical_email domain-case-normalized '
  'only (no Gmail dot/plus folding), verified_at set once by the §74.2 challenge flow.';
ALTER TABLE control.user_emails OWNER TO role_migration_owner;

CREATE INDEX idx_user_emails_user_id ON control.user_emails (user_id);

-- =============================================================================
-- control.email_challenges — §74.2 signup verification code.
-- =============================================================================
CREATE TABLE control.email_challenges (
  challenge_id    uuid        PRIMARY KEY DEFAULT uuidv7(),
  user_id         uuid        NOT NULL REFERENCES control.users(user_id),
  user_email_id   uuid        NOT NULL REFERENCES control.user_emails(user_email_id),
  -- §74.2: "stored hashed (where applicable)" — the plaintext code is never written here,
  -- only its SHA-256 hex digest (`humaux_application::auth::CodeHash`); the plaintext exists
  -- only transiently in memory on the way to the outbound email.
  code_hash       text        NOT NULL,
  attempts        integer     NOT NULL DEFAULT 0,
  max_attempts    integer     NOT NULL,
  expires_at      timestamptz NOT NULL,
  -- §74.2 "single-use": set exactly once, on the verifying attempt that matches code_hash.
  consumed_at     timestamptz,
  created_at      timestamptz NOT NULL DEFAULT now(),
  CHECK (attempts >= 0 AND attempts <= max_attempts)
);
COMMENT ON TABLE control.email_challenges IS
  '§74.2 signup email verification code: cryptographically random, single-use, short TTL, '
  'attempt-limited, hashed at rest, never logged (enforced in humaux_application::auth).';
ALTER TABLE control.email_challenges OWNER TO role_migration_owner;

CREATE INDEX idx_email_challenges_user_email_id ON control.email_challenges (user_email_id, created_at DESC);

-- =============================================================================
-- control.password_credentials — §74.3 Argon2id.
-- =============================================================================
CREATE TABLE control.password_credentials (
  user_id        uuid        PRIMARY KEY REFERENCES control.users(user_id),
  password_hash  text        NOT NULL,
  -- §74.3 "参数由性能/安全基线集中配置...不写死在业务 handler": the encoded PHC string
  -- itself carries the exact m/t/p parameters used at hash time (Argon2's own format), so
  -- rehash-on-login (`humaux_application::auth::verify_password`) can detect "hashed under an
  -- older config" without a separate parameter-version column.
  updated_at     timestamptz NOT NULL DEFAULT now(),
  created_at     timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE control.password_credentials IS
  '§74.3 Argon2id password credential, one row per user. updated_at moves on rehash-on-login.';
ALTER TABLE control.password_credentials OWNER TO role_migration_owner;

-- =============================================================================
-- control.sessions — §74.4 session_epoch (invalidated by a password reset's epoch bump).
-- =============================================================================
CREATE TABLE control.sessions (
  session_id     uuid        PRIMARY KEY DEFAULT uuidv7(),
  user_id        uuid        NOT NULL REFERENCES control.users(user_id),
  -- Snapshot of control.users.security_epoch at issuance (0003, already exists). A session is
  -- valid only while this equals the user's *current* security_epoch — reset/"revoke all
  -- sessions" is one UPDATE on control.users, no per-session DELETE/UPDATE sweep needed
  -- (`humaux_application::auth::session_still_valid`).
  session_epoch  bigint      NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now(),
  expires_at     timestamptz NOT NULL,
  last_seen_at   timestamptz NOT NULL DEFAULT now(),
  revoked_at     timestamptz
);
COMMENT ON TABLE control.sessions IS
  '§74.4/§73.6 session row; session_epoch pins it to the security_epoch value at issuance so a '
  'password reset invalidates every prior session by incrementing control.users.security_epoch.';
ALTER TABLE control.sessions OWNER TO role_migration_owner;

CREATE INDEX idx_sessions_user_id ON control.sessions (user_id);

-- =============================================================================
-- control.password_reset_challenges — §74.4.
-- =============================================================================
CREATE TABLE control.password_reset_challenges (
  challenge_id    uuid        PRIMARY KEY DEFAULT uuidv7(),
  user_id         uuid        NOT NULL REFERENCES control.users(user_id),
  code_hash       text        NOT NULL,
  attempts        integer     NOT NULL DEFAULT 0,
  max_attempts    integer     NOT NULL,
  expires_at      timestamptz NOT NULL,
  consumed_at     timestamptz,
  -- §74.4 "per-account + per-IP 防刷": the per-account half is this table's own row cadence
  -- (created_at per user_id); requested_ip carries the correlation key the per-IP half needs.
  -- The rate-limit bucket mechanism itself is §73.3's IP Policy layer (H1/T2.1+T2.2), not
  -- reimplemented here.
  requested_ip    inet,
  created_at      timestamptz NOT NULL DEFAULT now(),
  CHECK (attempts >= 0 AND attempts <= max_attempts)
);
COMMENT ON TABLE control.password_reset_challenges IS
  '§74.4 password reset challenge: single-use, short TTL, attempt-limited, hashed at rest.';
ALTER TABLE control.password_reset_challenges OWNER TO role_migration_owner;

CREATE INDEX idx_password_reset_challenges_user_id ON control.password_reset_challenges (user_id, created_at DESC);

-- =============================================================================
-- control.email_change_requests — §74.5 (identity change: reauth -> pending -> verify ->
-- notify old email -> commit).
-- =============================================================================
CREATE TABLE control.email_change_requests (
  request_id            uuid        PRIMARY KEY DEFAULT uuidv7(),
  user_id                uuid        NOT NULL REFERENCES control.users(user_id),
  new_original_email     text        NOT NULL,
  new_canonical_email    text        NOT NULL,
  code_hash              text        NOT NULL,
  attempts               integer     NOT NULL DEFAULT 0,
  max_attempts           integer     NOT NULL,
  expires_at             timestamptz NOT NULL,
  consumed_at            timestamptz,
  -- §74.5 first step: "reauthenticate" happens before this row is even created; the timestamp
  -- is kept for audit (how long ago the reauth was) rather than re-derived from auth_events.
  reauthenticated_at     timestamptz NOT NULL,
  -- §74.5 "-> notify old email": set once the old-address notification has actually gone out
  -- (via the injected `humaux_application::auth::EmailNotifier` port), independent of whether
  -- the new address has verified yet — the two are not the same event.
  old_email_notified_at  timestamptz,
  created_at             timestamptz NOT NULL DEFAULT now(),
  CHECK (attempts >= 0 AND attempts <= max_attempts)
);
COMMENT ON TABLE control.email_change_requests IS
  '§74.5 email change: reauthenticate -> pending new email -> verify -> notify old email -> '
  'commit. High-risk old-email-confirmation/MFA policy is a future extension of this row, not '
  'built here (spec: "may require", not "must").';
ALTER TABLE control.email_change_requests OWNER TO role_migration_owner;

CREATE INDEX idx_email_change_requests_user_id ON control.email_change_requests (user_id, created_at DESC);

-- =============================================================================
-- control.auth_events — §74.1. Distinct from control.audit_events (0003, §77 general
-- operational audit trail) — this is the identity/auth state machine's own event log, the
-- source e.g. an entitlement check reads to answer "has this account ever reached
-- ACCOUNT_ACTIVE". Append-only by convention (no UPDATE/DELETE grant to anyone but owner,
-- domain default already withholds both from every non-owner role, §6.2.1).
-- =============================================================================
CREATE TABLE control.auth_events (
  auth_event_id  uuid        PRIMARY KEY DEFAULT uuidv7(),
  -- Nullable: a login attempt against a non-existent email has no user_id to attach to, and
  -- must not be synthesized one (that would itself be an enumeration side channel).
  user_id        uuid        REFERENCES control.users(user_id),
  event_type     text        NOT NULL CHECK (event_type IN (
                   'SIGNUP_STARTED', 'EMAIL_CHALLENGE_SENT', 'EMAIL_VERIFIED', 'ACCOUNT_ACTIVATED',
                   'LOGIN_SUCCESS', 'LOGIN_FAILURE',
                   'PASSWORD_RESET_REQUESTED', 'PASSWORD_RESET_COMPLETED',
                   'EMAIL_CHANGE_REQUESTED', 'EMAIL_CHANGE_COMPLETED'
                 )),
  occurred_at    timestamptz NOT NULL DEFAULT now(),
  -- §77's allowlisted-metadata discipline applies here too even though this predates the H6
  -- audit sink: never a password/code/token field (application-layer contract; nothing in
  -- this file's CHECKs can enforce "never a secret", it is a code-review/test invariant).
  metadata       jsonb       NOT NULL DEFAULT '{}'::jsonb
);
COMMENT ON TABLE control.auth_events IS
  '§74.1 auth state-machine event trail (signup/login/reset/change) — distinct from '
  'control.audit_events (0003, §77 general operational audit). metadata never carries a '
  'password/verification-code/session-token value (enforced in application code, not SQL).';
ALTER TABLE control.auth_events OWNER TO role_migration_owner;

CREATE INDEX idx_auth_events_user_id ON control.auth_events (user_id, occurred_at DESC);

-- =============================================================================
-- Identity Evolution V2 reserve — schema only, no service code this phase (spec: "Passkey 可
-- 以 Phase 1/2 实现，但 Schema 不允许以后在 users 表上继续堆凭据字段" / "SCIM 不是认证协议，
-- 不能与 OIDC/SAML 混在一个模块"). Kept here rather than a separate migration file: same
-- "control.* additive DDL, domain-default grants, owner=role_migration_owner" shape as the
-- seven tables above, nothing about reserving these needs its own manifest contract.
-- =============================================================================

CREATE TABLE control.authenticators (
  authenticator_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  user_id          uuid        NOT NULL REFERENCES control.users(user_id),
  -- §74 Identity Evolution diagram, verbatim leaf set: PASSWORD/PASSKEY/TOTP/RECOVERY_CODE/
  -- OIDC/SAML. Unused by this phase's service code (control.password_credentials is still the
  -- live PASSWORD path) — reserved so a future consolidation has one closed enum to land on
  -- instead of users accreting a second ad-hoc credential column.
  kind             text        NOT NULL CHECK (kind IN ('PASSWORD','PASSKEY','TOTP','RECOVERY_CODE','OIDC','SAML')),
  created_at       timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE control.authenticators IS
  '§74 Identity Evolution V2 reserve: User -> Identity/Authenticator split. Schema only, no '
  'Phase 2 service code — control.password_credentials remains the live PASSWORD path.';
ALTER TABLE control.authenticators OWNER TO role_migration_owner;

CREATE TABLE control.webauthn_credentials (
  credential_id     uuid        PRIMARY KEY DEFAULT uuidv7(),
  user_id           uuid        NOT NULL REFERENCES control.users(user_id),
  public_key        bytea       NOT NULL,
  sign_count        bigint      NOT NULL DEFAULT 0,
  created_at        timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE control.webauthn_credentials IS
  '§74 WebAuthn Level 3 V2 reserve (w3.org/TR/webauthn-3). Schema only, protocol not '
  'implemented this phase.';
ALTER TABLE control.webauthn_credentials OWNER TO role_migration_owner;

CREATE TABLE control.recovery_codes (
  recovery_code_id  uuid        PRIMARY KEY DEFAULT uuidv7(),
  user_id           uuid        NOT NULL REFERENCES control.users(user_id),
  code_hash         text        NOT NULL,
  used_at           timestamptz,
  created_at        timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE control.recovery_codes IS
  '§74 Identity Evolution V2 reserve: MFA/passkey recovery codes. Schema only this phase.';
ALTER TABLE control.recovery_codes OWNER TO role_migration_owner;

CREATE TABLE control.identity_connections (
  connection_id     uuid        PRIMARY KEY DEFAULT uuidv7(),
  user_id           uuid        NOT NULL REFERENCES control.users(user_id),
  -- §74 Enterprise SSO, verbatim: "identity_connections / OIDC / SAML" — SCIM is deliberately
  -- excluded from this enum ("SCIM 不是认证协议，不能与 OIDC/SAML 混在一个模块", a dedicated
  -- control.scim_connections table is §82's own inventory line, out of this task's table set).
  provider          text        NOT NULL CHECK (provider IN ('OIDC','SAML')),
  provider_subject  text        NOT NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (provider, provider_subject)
);
COMMENT ON TABLE control.identity_connections IS
  '§74 Enterprise SSO V2 reserve (OIDC/SAML). Schema only this phase; Membership stays '
  'authoritative in control.memberships regardless of SSO provider (spec note).';
ALTER TABLE control.identity_connections OWNER TO role_migration_owner;
