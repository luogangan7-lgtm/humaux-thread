-- §6 Identity/SaaS core objects — Phase 1 identity/tenancy/policy subset. SaaS billing
-- proper (control.plans / control.usage — the Payment/Entitlement projector side of §71/§76)
-- is left to the Phase 2 migration number range; this file only builds what Phase 1's own
-- deliverables need to exist and to FK against: Tenant/User/Membership/Workspace core
-- (§6), the two reasoning-domain tables evidence_objects FKs into (§48.0①), and
-- control.quota_windows (§72) — the latter exists here *only* because §6.2.2's GRANT
-- matrix names it explicitly (T1.2 acceptance requires the table to exist); no reserve/
-- commit application code lands in this migration, and no role gets INSERT on it (§6.2.2
-- "显式空缺，不是遗漏").

-- §6.3 TenantState: PROVISIONING/ACTIVE/SUSPENDED/PENDING_DELETE/DELETING/DELETED.
CREATE TABLE control.tenants (
  tenant_id      uuid        PRIMARY KEY DEFAULT uuidv7(),
  name           text        NOT NULL,
  state          text        NOT NULL DEFAULT 'PROVISIONING'
                 CHECK (state IN ('PROVISIONING','ACTIVE','SUSPENDED','PENDING_DELETE','DELETING','DELETED')),
  -- §6.3: recorded here so tenant-suspend/deleting transitions can bump it in the same row
  -- write; the *event* that must bump it (user suspend, membership removal, ...) lives with
  -- whichever task wires the state machine transitions, not this DDL.
  security_epoch bigint      NOT NULL DEFAULT 0,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE control.tenants IS '§6.3 TenantState lifecycle; tenant_id is the RLS boundary root for every other schema (§6.1).';

-- §6.3 UserState: PENDING_VERIFICATION/ACTIVE/SUSPENDED/DEACTIVATED/PENDING_DELETE/DELETED.
-- Deliberately NOT tenant_id-scoped: a User can hold memberships in multiple Organization
-- Tenants (§6.3 Ownership/Offboarding), so tenant_id lives on control.memberships, not here.
-- §74's richer auth fields (email/password_credentials/sessions/...) are Phase 2's own
-- migration number range; this row only carries what Phase 1's FK targets need.
CREATE TABLE control.users (
  user_id         uuid        PRIMARY KEY DEFAULT uuidv7(),
  state           text        NOT NULL DEFAULT 'PENDING_VERIFICATION'
                  CHECK (state IN ('PENDING_VERIFICATION','ACTIVE','SUSPENDED','DEACTIVATED','PENDING_DELETE','DELETED')),
  security_epoch  bigint      NOT NULL DEFAULT 0,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE control.workspaces (
  workspace_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id    uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  name         text        NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- §6.3 MembershipState: INVITED/ACTIVE/SUSPENDED/REMOVED.
CREATE TABLE control.memberships (
  membership_id   uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id       uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  user_id         uuid        NOT NULL REFERENCES control.users(user_id),
  role            text        NOT NULL,
  state           text        NOT NULL DEFAULT 'INVITED'
                  CHECK (state IN ('INVITED','ACTIVE','SUSPENDED','REMOVED')),
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, user_id)
);

CREATE TABLE control.repositories (
  repository_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id     uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  workspace_id  uuid        REFERENCES control.workspaces(workspace_id),
  name          text        NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE control.repository_connections (
  connection_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  repository_id uuid        NOT NULL REFERENCES control.repositories(repository_id),
  tenant_id     uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  provider      text        NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE control.agents (
  agent_id     uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id    uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  workspace_id uuid        REFERENCES control.workspaces(workspace_id),
  name         text        NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);

-- CredentialRef only — raw secret material authority is OpenBao (§5), this row is a
-- pointer/locator, never the secret bytes.
CREATE TABLE control.credentials (
  credential_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id     uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  purpose       text        NOT NULL,
  openbao_ref   text        NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);

-- §48.0①: private.evidence_objects.reasoning_domain_id FK target. Scopes which private
-- reasoning domain (USER_REASONING key/profile boundary, §7.1) an Evidence was processed
-- under.
CREATE TABLE control.private_reasoning_domains (
  reasoning_domain_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id            uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  name                  text        NOT NULL,
  created_at            timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE control.reasoning_domain_grants (
  grant_id             uuid        PRIMARY KEY DEFAULT uuidv7(),
  reasoning_domain_id  uuid        NOT NULL REFERENCES control.private_reasoning_domains(reasoning_domain_id),
  principal_id         uuid        NOT NULL,
  created_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE control.contribution_policies (
  policy_id  uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id  uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  allow_public_contribution boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE control.retention_policies (
  policy_id  uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id  uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  retention_days int     NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Append-heavy (§48.1 partition candidate list); not partitioned in this migration —
-- month-range partitioning of the §48.1 list lands with the tables that generate real
-- write volume first (private.events, this file only creates the identity core).
CREATE TABLE control.audit_events (
  audit_event_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id      uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  actor_user_id  uuid        REFERENCES control.users(user_id),
  action         text        NOT NULL,
  occurred_at    timestamptz NOT NULL DEFAULT now()
);

-- §72 Atomic Usage Reservation window row. Exists only for the §6.2.2 GRANT matrix column
-- (`control.quota_windows`); no writer role is granted INSERT here (§6.2.2 note: "窗口行
-- 由谁 INSERT 文档未冻结" — explicit vacancy, not an oversight).
CREATE TABLE control.quota_windows (
  tenant_id       uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  entitlement_key text        NOT NULL,
  window_start    timestamptz NOT NULL,
  window_end      timestamptz NOT NULL,
  hard_limit      bigint      NOT NULL,
  reserved        bigint      NOT NULL DEFAULT 0,
  consumed        bigint      NOT NULL DEFAULT 0,
  version         bigint      NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, entitlement_key, window_start)
);
