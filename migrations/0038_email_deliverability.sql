-- §74.6 Email Provider + Email Deliverability Plane — H3/T2.4 schema.
--
-- Five tables: `email_outbox` (queue, §74.6 "发信走 email_outbox") plus the four
-- Deliverability Plane tables §74.6 names as mandatory once the outbox exists
-- ("email_outbox 只是第一步"): `email_delivery_events`, `email_suppressions`,
-- `email_domains`, `email_provider_health`.
--
-- Schema placement — a deliberate deviation from §82's inventory, called out rather than
-- silently taken: §82 lists `control.email_suppressions` / `control.email_delivery_events`
-- explicitly, and is silent on `email_domains`/`email_provider_health` altogether (grep of
-- §82 confirms neither name appears in its control.* or ops.* lists — a genuine gap in that
-- section, not a placement this file can read off it). All five tables below go in `ops.*`
-- instead, for one concrete, testable reason: `control.*`'s §6.2.1 domain default is
-- SELECT-only for every runtime role (sibling 0034_email_auth_identity.sql hits the exact
-- same wall for its eight control.* auth tables and documents it as an accepted, open gap —
-- "every table... gets exactly §6.2.1's domain default... SELECT-only", no MATRIX cell added
-- since none of these tables are named by any ```sql INSERT/UPDATE block or role+verb+table
-- assertion in the spec prose, so `check_table_set_derivation` would flag an ad-hoc grant
-- addition as MATRIX drift). That gap is acceptable for control.user_emails/sessions/etc
-- because a *separate*, not-yet-landed gateway-wiring task owns writing them. It is not
-- acceptable here: this task's own acceptance gate requires a live worker to actually INSERT
-- `email_delivery_events` rows and UPDATE `email_outbox.state` under `role_private_worker`'s
-- real grants today (§74.6 task brief: "provider 失败转 FAILED+事件记录", exercised by a real
-- DB integration test in this same wave, not a future one). `ops.*`'s domain default already
-- grants role_gateway/role_private_worker/role_public_worker/role_retrieval_worker
-- SELECT+INSERT+UPDATE (§6.2.1) with zero new GRANT statements needed — the same
-- already-installed 0011 `ALTER DEFAULT PRIVILEGES` mechanism the sibling migrations rely on,
-- just against a schema whose default already matches what this subsystem needs to function
-- today rather than after a future grants migration lands.
--
-- Tenant scoping: none of the five tables carry `tenant_id`. Same reasoning
-- 0034_email_auth_identity.sql already established for the auth/identity tables this outbox
-- serves — a User is not tenant-scoped in this schema (0003's own comment on control.users;
-- membership is the tenant-scoped join, control.memberships). `email_outbox` addresses a
-- `control.users` row directly; the four Deliverability Plane tables are platform-wide
-- operational/reputation data by nature (a hard bounce or a sending domain's SPF/DKIM/DMARC
-- state is not a per-tenant fact) — matches how ops.consistency_reports/coord.locks already
-- carry cross-cutting operational rows outside the tenant boundary. xtask/src/rls_check.rs's
-- RLS four-item enumeration scans exactly the set of tables carrying `tenant_id`
-- (§48.2/§62); none of these five qualify, so ENABLE/FORCE ROW LEVEL SECURITY + a tenant
-- policy do not apply here (CLAUDE.md's "tenant 表" RLS instruction is conditional on
-- carrying tenant_id).
--
-- Ownership: explicit `ALTER TABLE ... OWNER TO role_migration_owner` per table (P1 pattern,
-- 0032/0034's own precedent) rather than leaving it to the connecting migration-runner role.

-- =============================================================================
-- ops.email_outbox — §74.6 the queue. `state` transitions QUEUED -> SENT -> DELIVERED ->
-- BOUNCED/COMPLAINED/SUPPRESSED/FAILED, verbatim from §74.6's Email Deliverability Plane
-- state list. `stream` is the §74.6 "Transactional 与 Marketing 分离" field — checked at
-- enqueue time against ops.email_suppressions before a row is ever inserted (§74.6
-- Suppression: "对已 suppression 邮箱需返回统一安全语义, 不泄露账户存在状态" — the check
-- happens in `humaux_adapters::email::outbox::enqueue`, this table only records the outcome
-- for rows that passed it).
-- =============================================================================
CREATE TABLE ops.email_outbox (
  outbox_id           uuid        PRIMARY KEY DEFAULT uuidv7(),
  user_id             uuid        NOT NULL REFERENCES control.users(user_id),
  to_email            text        NOT NULL,
  from_email          text        NOT NULL,
  stream              text        NOT NULL
                      CHECK (stream IN ('TRANSACTIONAL', 'MARKETING')),
  -- Which auth/notification flow this is (e.g. `EMAIL_VERIFICATION`, `PASSWORD_RESET`) —
  -- an open string, not a CHECK-constrained enum: the set of templates is a content/product
  -- concern the caller owns, not a business rule this schema should freeze (§78.1 "禁止硬编
  -- 码业务配置" cuts the other way here — hardcoding the *template* list into a DB CHECK
  -- would be exactly that).
  template_id         text        NOT NULL,
  subject             text        NOT NULL,
  -- Allowlisted template variables only, never a private Memory body (§74.7's sibling rule
  -- for notifications applies by the same logic here — an outbox row is provider-bound,
  -- outside every tenant/visibility boundary this workspace otherwise enforces).
  payload             jsonb       NOT NULL DEFAULT '{}'::jsonb,
  state               text        NOT NULL DEFAULT 'QUEUED'
                      CHECK (state IN (
                        'QUEUED', 'SENT', 'DELIVERED', 'BOUNCED', 'COMPLAINED', 'SUPPRESSED', 'FAILED')),
  -- §74.6 "不写死供应商": which EmailProvider adapter actually handled this row, filled in
  -- once dispatched — an open string (adapter identity), not an enum of a fixed provider
  -- list.
  provider            text,
  provider_message_id text,
  attempt             int         NOT NULL DEFAULT 0,
  last_error          text,
  created_at          timestamptz NOT NULL DEFAULT now(),
  sent_at             timestamptz
);

COMMENT ON TABLE ops.email_outbox IS
  '§74.6 Email Provider queue: handler enqueues (INSERT, state=QUEUED) and returns without '
  'touching SMTP; humaux_adapters::email::outbox::run_once claims QUEUED rows (SELECT ... '
  'FOR UPDATE SKIP LOCKED) and drives them to a terminal state.';

ALTER TABLE ops.email_outbox OWNER TO role_migration_owner;

-- Worker's claim query filters/orders on exactly these two columns.
CREATE INDEX idx_email_outbox_queued ON ops.email_outbox (created_at) WHERE state = 'QUEUED';

-- =============================================================================
-- ops.email_delivery_events — §74.6 "email_outbox 只是第一步, 还必须维护
-- email_delivery_events". Append-only log of every state-changing outcome for one outbox
-- row (a row can accumulate more than one event over its life, e.g. SENT then later
-- BOUNCED from an async provider webhook) — a pure link/log table, no `tenant_id` of its
-- own, scoped entirely through `outbox_id`'s FK (same shape as private.memory_evidence's
-- link-table treatment in migration 0012, minus RLS since the parent itself carries none).
-- =============================================================================
CREATE TABLE ops.email_delivery_events (
  event_id    uuid        PRIMARY KEY DEFAULT uuidv7(),
  outbox_id   uuid        NOT NULL REFERENCES ops.email_outbox(outbox_id),
  event_type  text        NOT NULL
              CHECK (event_type IN ('SENT', 'DELIVERED', 'BOUNCED', 'COMPLAINED', 'SUPPRESSED', 'FAILED')),
  provider    text,
  -- Raw provider payload/reason, allowlisted at the call site — never the full webhook body
  -- verbatim (same "not a dumping ground" discipline as email_outbox.payload above).
  detail      jsonb       NOT NULL DEFAULT '{}'::jsonb,
  occurred_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE ops.email_delivery_events IS
  '§74.6 append-only delivery outcome log per ops.email_outbox row (SENT/DELIVERED/BOUNCED/'
  'COMPLAINED/SUPPRESSED/FAILED) — the "运营监控: send success/failure, bounce, complaint" '
  'source of record.';

ALTER TABLE ops.email_delivery_events OWNER TO role_migration_owner;

CREATE INDEX idx_email_delivery_events_outbox ON ops.email_delivery_events (outbox_id, occurred_at);

-- =============================================================================
-- ops.email_suppressions — §74.6 Suppression: "硬退信、投诉等进入 suppression: email /
-- reason / provider / created_at / expires_at?" field list, verbatim, plus `scope` (see
-- module header above for why `scope` exists: keeps a marketing complaint/unsubscribe from
-- suppressing the transactional verification/reset channel, §74.6's frozen "不能让营销投诉
-- 率打坏验证码...通道" rule implemented as data instead of two separate tables).
-- =============================================================================
CREATE TABLE ops.email_suppressions (
  suppression_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  email          text        NOT NULL,
  reason         text        NOT NULL
                 CHECK (reason IN ('HARD_BOUNCE', 'COMPLAINT', 'UNSUBSCRIBE', 'MANUAL')),
  scope          text        NOT NULL
                 CHECK (scope IN ('TRANSACTIONAL', 'MARKETING', 'ALL')),
  provider       text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  expires_at     timestamptz,
  UNIQUE (email, scope)
);

COMMENT ON TABLE ops.email_suppressions IS
  '§74.6 Suppression list: enqueue() checks this before inserting an ops.email_outbox row '
  'and, if matched, returns the identical outcome as a normal enqueue without writing one '
  '(unified safety semantics — never reveals account existence).';

ALTER TABLE ops.email_suppressions OWNER TO role_migration_owner;

CREATE INDEX idx_email_suppressions_email ON ops.email_suppressions (email);

-- =============================================================================
-- ops.email_domains — sending-domain deliverability posture. Backs the §74.6 deploy gate
-- ("生产域名配置 SPF/DKIM/DMARC 属于部署 Gate") — see README/deploy notes for the actual
-- gate procedure; this table is where a deploy-time check records/reads verification state,
-- not a gate implementation itself (§74.6's own notes confirm no G-numbered CI gate exists
-- for this, it is a deploy-process gate).
-- =============================================================================
CREATE TABLE ops.email_domains (
  domain            text        PRIMARY KEY,
  is_production     boolean     NOT NULL DEFAULT false,
  spf_verified_at   timestamptz,
  dkim_verified_at  timestamptz,
  dmarc_verified_at timestamptz,
  last_checked_at   timestamptz,
  notes             text,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE ops.email_domains IS
  '§74.6 sending-domain SPF/DKIM/DMARC verification state. A production domain '
  '(is_production=true) with any of the three *_verified_at columns still NULL fails the '
  '§74.6 deploy gate — see README §"Email deploy gate".';

ALTER TABLE ops.email_domains OWNER TO role_migration_owner;

-- =============================================================================
-- ops.email_provider_health — §74.6 "运营监控" rollup: send success/failure, bounce,
-- complaint, provider quota signal, aggregated per provider per time window. Intentionally
-- minimal (task brief YAGNI: only the counters this wave's worker actually increments);
-- ponytail: a fuller SLA/quota dashboard is a later add, not a schema this task needs to
-- anticipate.
-- =============================================================================
CREATE TABLE ops.email_provider_health (
  provider        text        NOT NULL,
  window_start    timestamptz NOT NULL,
  sent_count      bigint      NOT NULL DEFAULT 0,
  failed_count    bigint      NOT NULL DEFAULT 0,
  bounce_count    bigint      NOT NULL DEFAULT 0,
  complaint_count bigint      NOT NULL DEFAULT 0,
  checked_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (provider, window_start)
);

COMMENT ON TABLE ops.email_provider_health IS
  '§74.6 "运营监控" per-provider-per-window counters (send success/failure, bounce, '
  'complaint) — aggregation cadence/writer is a later wiring task, schema only here.';

ALTER TABLE ops.email_provider_health OWNER TO role_migration_owner;
