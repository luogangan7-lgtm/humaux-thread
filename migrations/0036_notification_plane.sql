-- §74.7 Notification Plane (H4/T2.5): control.notifications is the platform-authoritative
-- user notification record; Email/future Webhook/Push are delivery adapters, not the source
-- of truth (§74.7 "建立平台内 Notification 作为权威用户通知记录"). Three tables:
--   control.notifications          — one row per notification instance
--   control.notification_preferences — per-user, per-category channel opt-in
--   ops.notification_deliveries    — one row per delivery attempt (in-app/email), retried
--
-- §6.2.1 domain-default GRANTs apply automatically via 0011's `ALTER DEFAULT PRIVILEGES FOR
-- ROLE <migration runner>` (verified live against ops.column_vitality/0030, the sibling
-- non-§6.2.2-named table this migration file's own precedent for "no explicit GRANT needed
-- here") — none of these three tables appear in §6.2.2's 14-table named matrix, so no
-- explicit GRANT statement belongs in this file; adding one would only fight the default-
-- privilege mechanism, not add coverage.

CREATE TABLE control.notifications (
  notification_id   uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id          uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  -- §74.7 field list marks this optional ("user_id?") — a tenant-wide notification (e.g.
  -- BILLING_PAST_DUE addressed to every tenant admin) has no single owning user.
  user_id            uuid        REFERENCES control.users(user_id),
  category           text        NOT NULL,
  severity           text        NOT NULL,
  dedup_key          text        NOT NULL,
  title_template_id  text        NOT NULL,
  -- §74.7 "Notification payload 不复制 private Memory 正文": the allowlisted-field shape is
  -- enforced Rust-side by `application::notify::NotificationPayload`'s `#[serde(deny_unknown_
  -- fields)]` (a type-level rejection, not a DB CHECK re-deriving the same allowlist in SQL).
  payload            jsonb       NOT NULL DEFAULT '{}'::jsonb,
  created_at         timestamptz NOT NULL DEFAULT now(),
  read_at            timestamptz,
  resolved_at        timestamptz,
  CHECK (category IN (
    'BYOK_INVALID','WAITING_KEY','QUOTA_80','QUOTA_EXHAUSTED','BILLING_PAST_DUE',
    'MCP_GRANT_CREATED','MCP_GRANT_REVOKED','NEW_LOGIN','SECURITY_EVENT','EXPORT_READY',
    'DELETION_PROGRESS','PUBLIC_CONTRIBUTION_REVIEW'
  )),
  CHECK (severity IN ('INFO','WARNING','SECURITY'))
);

COMMENT ON TABLE control.notifications IS
  '§74.7: platform-authoritative user notification record; category/severity closed sets '
  'mirror application::notify::NotificationCategory/Severity verbatim (§78.2 contract test).';

-- §74.7 "dedup_key / cooldown 防 notification storm": the cooldown lookup is "most recent
-- row for (tenant_id, dedup_key)", scoped by tenant first since dedup_key is caller-chosen
-- and not guaranteed globally unique across tenants.
CREATE INDEX idx_notifications_dedup_cooldown
  ON control.notifications (tenant_id, dedup_key, created_at DESC);

CREATE TABLE control.notification_preferences (
  -- Deliberately no tenant_id column: control.users itself is not tenant-scoped (0003's own
  -- comment — a User can hold memberships across multiple tenants), and a per-category
  -- channel preference is a user-account-level setting, not a per-tenant one. Same shape as
  -- control.users: no RLS policy below, for the identical reason 0012 gave it none.
  user_id       uuid NOT NULL REFERENCES control.users(user_id),
  category      text NOT NULL,
  in_app        boolean NOT NULL DEFAULT true,
  email         boolean NOT NULL DEFAULT true,
  digest_policy text NOT NULL DEFAULT 'IMMEDIATE',
  PRIMARY KEY (user_id, category),
  CHECK (category IN (
    'BYOK_INVALID','WAITING_KEY','QUOTA_80','QUOTA_EXHAUSTED','BILLING_PAST_DUE',
    'MCP_GRANT_CREATED','MCP_GRANT_REVOKED','NEW_LOGIN','SECURITY_EVENT','EXPORT_READY',
    'DELETION_PROGRESS','PUBLIC_CONTRIBUTION_REVIEW'
  )),
  CHECK (digest_policy IN ('IMMEDIATE','DAILY','WEEKLY','OFF'))
);

COMMENT ON TABLE control.notification_preferences IS
  '§74.7: per-user per-category channel opt-in. §74.7 principle "安全关键通知可忽略用户营销'
  '偏好" is enforced Rust-side (application::notify::resolve_channels), not by a DB CHECK — '
  'a SECURITY-severity notification always writes email=true to ops.notification_deliveries '
  'regardless of what this table says for that (user_id, category).';

CREATE TABLE ops.notification_deliveries (
  delivery_id          uuid        PRIMARY KEY DEFAULT uuidv7(),
  -- Denormalized from control.notifications, matching ops.outbox/ops.jobs' own shape (both
  -- carry tenant_id directly rather than resolving it via a join at RLS-check time) — keeps
  -- this table inside the blanket "any table with a literal tenant_id column" RLS domain
  -- instead of needing a hand-written EXISTS-subquery policy.
  tenant_id            uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  notification_id      uuid        NOT NULL REFERENCES control.notifications(notification_id),
  channel              text        NOT NULL,
  provider             text,
  state                text        NOT NULL DEFAULT 'QUEUED',
  attempt              int         NOT NULL DEFAULT 0,
  next_retry_at        timestamptz,
  provider_message_id  text,
  created_at           timestamptz NOT NULL DEFAULT now(),
  CHECK (channel IN ('IN_APP','EMAIL')),
  CHECK (state IN ('QUEUED','SENT','DELIVERED','FAILED'))
);

COMMENT ON TABLE ops.notification_deliveries IS
  '§74.7: one row per delivery attempt. §74.7 "Email delivery failure 不删除 in-app '
  'notification": a FAILED row here never cascades to a DELETE on control.notifications — no '
  'FK in either direction carries ON DELETE, and no writer role holds DELETE on either table '
  '(§6.2.1 "全域硬约束，无例外: runtime role 对任何 schema 的任何表：无 DELETE").';

-- §62 tenant RLS, NULLIF form from first creation (§31 hardening note in 0031 applies this
-- retroactively to tables that predate it; these tables are created after 0031 so the
-- NULLIF-wrapped form is written directly, no later retrofit migration needed).
ALTER TABLE control.notifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.notifications FORCE ROW LEVEL SECURITY;
CREATE POLICY notifications_tenant_and_owner ON control.notifications
USING (
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
  AND (user_id IS NULL OR user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid)
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
  AND (user_id IS NULL OR user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid)
);

ALTER TABLE ops.notification_deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.notification_deliveries FORCE ROW LEVEL SECURITY;
CREATE POLICY notification_deliveries_tenant_isolation ON ops.notification_deliveries
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

-- §6.2.1 ownership: role_migration_owner owns every table in every schema ("owner" column,
-- all seven schemas). Reassigned explicitly here (mirroring 0011's one-time reassignment
-- loop) rather than left on the migration-runner connection role — 0011's loop only ran once
-- over tables that existed at that time (0030's ops.column_vitality is the demonstrated gap:
-- still owned by the runner, never role_migration_owner) — ownership does not retroactively
-- follow new CREATE TABLEs the way the default-privilege GRANTs do.
ALTER TABLE control.notifications OWNER TO role_migration_owner;
ALTER TABLE control.notification_preferences OWNER TO role_migration_owner;
ALTER TABLE ops.notification_deliveries OWNER TO role_migration_owner;
