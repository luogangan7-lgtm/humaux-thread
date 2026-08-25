-- §75 Team Invitation 与 Marketing Referral 必须分开 — two independently-tenant-scoped
-- table groups, deliberately NOT sharing an `invite_code`/token table (§75 frozen: "这两者
-- 不能共用 invite_code 表"). control.team_invitations is the security object (bearer
-- token, membership grant); the four control.referral_* tables are the marketing/value
-- object (five-state attribution machine + append-only credit ledger). Both groups are new
-- tables outside §6.2.2's 14-table matrix, so grants come from §6.2.1's domain default via
-- 0011's `ALTER DEFAULT PRIVILEGES FOR ROLE <migration runner>` (already laid down per
-- schema; applies automatically to any table this same connecting role creates from here
-- on — no GRANT statements needed in this file, §6.2.1 "非点名表"). Ownership is
-- reassigned to role_migration_owner explicitly below (0011's retroactive ownership loop
-- only walked tables that existed when 0011 ran).

-- =============================================================================
-- §75.1 Team Invitation (security object)
-- =============================================================================

-- §75.1 PENDING -> ACCEPTED | REVOKED | EXPIRED. `token_hash` only — the bearer token
-- itself is generated and shown to the invitee exactly once by the application layer and
-- never persisted in plaintext (repo CLAUDE.md 安全红线: DB 只存 hash). §75.1: "接受时必
-- 须服务器再次检查 tenant、role、email、token、邀请状态" — invitation_id alone is never
-- sufficient to accept; referral.rs's accept path re-derives every one of those five facts
-- from this row, never trusts a client-supplied echo of them.
CREATE TABLE control.team_invitations (
  invitation_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id     uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  invited_email text        NOT NULL,
  role          text        NOT NULL,
  token_hash    text        NOT NULL,
  expires_at    timestamptz NOT NULL,
  invited_by    uuid        NOT NULL REFERENCES control.users(user_id),
  accepted_by   uuid        REFERENCES control.users(user_id),
  status        text        NOT NULL DEFAULT 'PENDING'
                CHECK (status IN ('PENDING', 'ACCEPTED', 'REVOKED', 'EXPIRED')),
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  -- accepted_by must be set exactly when (and only when) the invitation has settled into
  -- ACCEPTED — prevents an application bug from leaving a half-accepted row.
  CHECK ((status = 'ACCEPTED') = (accepted_by IS NOT NULL)),
  UNIQUE (token_hash)
);

COMMENT ON TABLE control.team_invitations IS
  '§75.1 Team Invitation: PENDING -> ACCEPTED/REVOKED/EXPIRED. Server re-checks tenant/role/'
  'email/token/status on accept — never trusts client-supplied values for any of the five.';

ALTER TABLE control.team_invitations OWNER TO role_migration_owner;
ALTER TABLE control.team_invitations ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.team_invitations FORCE ROW LEVEL SECURITY;

CREATE POLICY team_invitations_tenant_isolation ON control.team_invitations
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

-- =============================================================================
-- §75.2 Referral (marketing/value object) — four independent tables, no invite_code reuse.
-- §75.2 state machine ATTRIBUTED -> QUALIFIED -> MATURING -> GRANTED -> REVOKED lives on
-- referral_rewards ("referral_rewards 只保存资格/成熟/发放状态并引用该 entry" — spec's own
-- sentence names *this* table as the status holder); referral_attributions is the
-- immutable click/signup fact the reward references, kept separate so re-attribution
-- disputes never require rewriting a ledger-adjacent status row.
-- =============================================================================

CREATE TABLE control.referral_codes (
  referral_code_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id         uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  code              text        NOT NULL,
  created_by        uuid        NOT NULL REFERENCES control.users(user_id),
  active            boolean     NOT NULL DEFAULT true,
  created_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (code)
);

COMMENT ON TABLE control.referral_codes IS
  '§75.2 shareable referral code, owned by the referring tenant. Not a bearer credential '
  '(unlike team_invitations.token_hash) — stored plaintext by design, it identifies a '
  'referrer, it does not itself grant access to anything.';

ALTER TABLE control.referral_codes OWNER TO role_migration_owner;
ALTER TABLE control.referral_codes ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.referral_codes FORCE ROW LEVEL SECURITY;

CREATE POLICY referral_codes_tenant_isolation ON control.referral_codes
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

-- §75.3 anti-abuse signal columns (ip_hash/device_hash — hashed, never raw IP/device
-- fingerprint at rest) support the per-IP/per-device caps; §75.2 "no self-referral" gets a
-- DB-level backstop via the CHECK below in addition to the application-level reject path
-- (defense in depth — the CHECK alone cannot express "no self-referral across a *set* of
-- tenants an individual controls", only the direct one-hop case, so referral.rs's
-- application check remains the primary enforcement point).
CREATE TABLE control.referral_attributions (
  attribution_id    uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id         uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  referral_code_id  uuid        NOT NULL REFERENCES control.referral_codes(referral_code_id),
  referred_tenant_id uuid       NOT NULL REFERENCES control.tenants(tenant_id),
  referred_user_id  uuid        NOT NULL REFERENCES control.users(user_id),
  ip_hash           text,
  device_hash       text,
  attributed_at     timestamptz NOT NULL DEFAULT now(),
  CHECK (tenant_id <> referred_tenant_id),
  UNIQUE (referral_code_id, referred_tenant_id)
);

COMMENT ON TABLE control.referral_attributions IS
  '§75.2 immutable attribution fact (referrer tenant_id, referred_tenant_id, referral code) '
  '— never carries a status column; the state machine lives on referral_rewards.';

ALTER TABLE control.referral_attributions OWNER TO role_migration_owner;
ALTER TABLE control.referral_attributions ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.referral_attributions FORCE ROW LEVEL SECURITY;

CREATE POLICY referral_attributions_tenant_isolation ON control.referral_attributions
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

-- §75.2 "绝不能：注册成功 -> 立即送钱/送无限额度" — status defaults to ATTRIBUTED, never
-- GRANTED, and every forward transition is an application-level decision (referral.rs),
-- never a DB default or trigger side-effect. credit_ledger_entry_id/reversal_ledger_
-- entry_id are set exactly once each, by the GRANTED and REVOKED transitions respectively
-- — "每次发奖励必须生成 credit_ledger 不可变 entry...撤销通过反向 entry，不原地改余额".
CREATE TABLE control.referral_rewards (
  reward_id               uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id                uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  attribution_id           uuid        NOT NULL REFERENCES control.referral_attributions(attribution_id),
  status                   text        NOT NULL DEFAULT 'ATTRIBUTED'
                            CHECK (status IN ('ATTRIBUTED', 'QUALIFIED', 'MATURING', 'GRANTED', 'REVOKED')),
  reward_credits           bigint      NOT NULL,
  qualified_at             timestamptz,
  maturing_at              timestamptz,
  granted_at               timestamptz,
  revoked_at               timestamptz,
  -- FK deferred (no REFERENCES) deliberately: control.credit_ledger is created further
  -- down in this same file, and referral_rewards' GRANTED/REVOKED transition writes both
  -- rows (ledger entry + reward status) in one transaction — a same-file forward FK would
  -- require reordering the two CREATE TABLEs for no behavioral gain over the two ALTER
  -- TABLE ... ADD CONSTRAINT statements at the end of this file, which read top-to-bottom
  -- in the table's own natural declaration order instead.
  credit_ledger_entry_id   uuid,
  reversal_ledger_entry_id uuid,
  created_at               timestamptz NOT NULL DEFAULT now(),
  updated_at               timestamptz NOT NULL DEFAULT now(),
  CHECK ((status = 'GRANTED') = (credit_ledger_entry_id IS NOT NULL)),
  CHECK (reversal_ledger_entry_id IS NULL OR status = 'REVOKED'),
  UNIQUE (attribution_id)
);

COMMENT ON TABLE control.referral_rewards IS
  '§75.2 ATTRIBUTED -> QUALIFIED -> MATURING -> GRANTED -> REVOKED. credit_ledger_entry_id '
  'is set only on the GRANTED transition (never backfilled), reversal_ledger_entry_id only '
  'on REVOKED — both point at control.credit_ledger, never a mutated balance column.';

ALTER TABLE control.referral_rewards OWNER TO role_migration_owner;
ALTER TABLE control.referral_rewards ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.referral_rewards FORCE ROW LEVEL SECURITY;

CREATE POLICY referral_rewards_tenant_isolation ON control.referral_rewards
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

-- §75.2 append-only ledger: "每次发奖励必须生成 control.credit_ledger 不可变 entry...撤销
-- 通过反向 entry，不原地改余额". `reverses_entry_id` links a REVOKED reward's negative-
-- delta reversal entry back to the original grant entry it cancels; a tenant's current
-- balance is always SUM(delta) over its rows, never a column anyone writes to.
CREATE TABLE control.credit_ledger (
  ledger_entry_id  uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id        uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  delta            bigint      NOT NULL,
  reason           text        NOT NULL,
  reference_type   text        NOT NULL,
  reference_id     uuid        NOT NULL,
  reverses_entry_id uuid       REFERENCES control.credit_ledger(ledger_entry_id),
  created_at       timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE control.credit_ledger IS
  '§75.2 append-only: no UPDATE/DELETE grant to any role, and the guard trigger below '
  'rejects both outright even for the owning role — balance is derived (SUM(delta)), never '
  'stored and mutated. A REVOKE is a second row with reverses_entry_id set, never an edit.';

ALTER TABLE control.credit_ledger OWNER TO role_migration_owner;
ALTER TABLE control.credit_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.credit_ledger FORCE ROW LEVEL SECURITY;

CREATE POLICY credit_ledger_tenant_isolation ON control.credit_ledger
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

-- Defense in depth beyond "no role has UPDATE/DELETE grant" (§6.2.1 domain default already
-- gives every non-owner role SELECT-only on `control`): the owning role itself
-- (role_migration_owner) is NOSUPERUSER/NOBYPASSRLS but still has implicit owner privilege
-- to UPDATE/DELETE its own table absent this trigger — append-only must hold even under a
-- migration-owner-authenticated connection, not just under the runtime roles' grants.
CREATE FUNCTION control.credit_ledger_reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'control.credit_ledger is append-only (§75.2) — % not permitted', TG_OP
    USING ERRCODE = 'insufficient_privilege';
END;
$$;

CREATE TRIGGER credit_ledger_reject_mutation
BEFORE UPDATE OR DELETE ON control.credit_ledger
FOR EACH ROW EXECUTE FUNCTION control.credit_ledger_reject_mutation();

ALTER FUNCTION control.credit_ledger_reject_mutation() OWNER TO role_migration_owner;

-- Deferred FKs promised in referral_rewards' column comments above, now that
-- control.credit_ledger exists.
ALTER TABLE control.referral_rewards
  ADD CONSTRAINT referral_rewards_credit_ledger_entry_id_fkey
    FOREIGN KEY (credit_ledger_entry_id) REFERENCES control.credit_ledger(ledger_entry_id),
  ADD CONSTRAINT referral_rewards_reversal_ledger_entry_id_fkey
    FOREIGN KEY (reversal_ledger_entry_id) REFERENCES control.credit_ledger(ledger_entry_id);
