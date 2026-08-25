-- §76 套餐、优惠、奖励与 Entitlement 的统一关系: Payment/Subscription -> Base Entitlements
-- (+ Promotion/Coupon, + Referral Reward, + Manual Admin Grant) -> Effective Entitlement
-- Snapshot -> Quota Window / Request Authorization. Two tables only: control.
-- entitlement_grants (the raw, per-source facts) and control.entitlement_snapshots (the
-- Projector's synthesized output). §76 frozen: "运行时不读取散落的 coupon/referral 表计算
-- 权限" — Quota Window / Request Authorization consume entitlement_snapshots exclusively;
-- nothing downstream of the Projector may join back to control.referral_rewards,
-- control.credit_ledger, or any future coupon table. entitlement.rs's rustdoc freezes this
-- (§76): the runtime read path is entitlement_snapshots only.
--
-- Both tables are new, outside §6.2.2's 14-table matrix — grants come from §6.2.1's domain
-- default via 0011's `ALTER DEFAULT PRIVILEGES FOR ROLE <migration runner>` (already laid
-- down per schema, applies automatically to any table this same connecting role creates).

-- §76 grant fields transcribed verbatim: source, feature, value, valid_from, valid_until,
-- priority. `source_ref_id` is this migration's addition — an untyped pointer back to
-- whichever row justified the grant (a referral_rewards.reward_id when source=REFERRAL, a
-- plan_id when source=PLAN, an admin actor's audit event when source=ADMIN, ...); no single
-- FK target fits every source, so it stays a bare uuid with the source column as the
-- caller's own disambiguator, same shape as control.credentials.openbao_ref being a locator
-- rather than a typed FK.
CREATE TABLE control.entitlement_grants (
  grant_id     uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id    uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  source       text        NOT NULL CHECK (source IN ('PLAN', 'PROMOTION', 'REFERRAL', 'ADMIN', 'TRIAL')),
  source_ref_id uuid,
  feature      text        NOT NULL,
  value        jsonb       NOT NULL,
  valid_from   timestamptz NOT NULL DEFAULT now(),
  valid_until  timestamptz,
  priority     integer     NOT NULL DEFAULT 0,
  revoked_at   timestamptz,
  created_by   uuid        REFERENCES control.users(user_id),
  created_at   timestamptz NOT NULL DEFAULT now(),
  CHECK (valid_until IS NULL OR valid_until > valid_from)
);

COMMENT ON TABLE control.entitlement_grants IS
  '§76 source = PLAN|PROMOTION|REFERRAL|ADMIN|TRIAL. Raw per-source fact — never read '
  'directly by an authorization/quota code path (§76: only entitlement_snapshots is).';

ALTER TABLE control.entitlement_grants OWNER TO role_migration_owner;
ALTER TABLE control.entitlement_grants ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.entitlement_grants FORCE ROW LEVEL SECURITY;

CREATE POLICY entitlement_grants_tenant_isolation ON control.entitlement_grants
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

-- One current row per tenant (tenant_id is the PK, not a synthetic snapshot_id) — the
-- Projector overwrites its own prior output on recompute; §76 describes a single "Effective
-- Entitlement Snapshot" feeding Quota Window / Request Authorization, not a version
-- history. `source_grant_ids` records which entitlement_grants rows fed this computation,
-- for audit/debugging the Projector itself — still never a second read path into the raw
-- grants for an authorization decision.
CREATE TABLE control.entitlement_snapshots (
  tenant_id        uuid        PRIMARY KEY REFERENCES control.tenants(tenant_id),
  effective        jsonb       NOT NULL,
  source_grant_ids uuid[]      NOT NULL DEFAULT '{}',
  computed_at      timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE control.entitlement_snapshots IS
  '§76 Effective Entitlement Snapshot: the sole read path for Quota Window / Request '
  'Authorization (rustdoc-frozen in entitlement.rs). One row per tenant, overwritten on '
  'each Projector recompute — not a history table.';

ALTER TABLE control.entitlement_snapshots OWNER TO role_migration_owner;
ALTER TABLE control.entitlement_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.entitlement_snapshots FORCE ROW LEVEL SECURITY;

CREATE POLICY entitlement_snapshots_tenant_isolation ON control.entitlement_snapshots
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);
