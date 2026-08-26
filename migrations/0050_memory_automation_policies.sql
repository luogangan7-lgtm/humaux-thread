-- §11.5.1 Memory Automation Policy — control.memory_automation_policies. Pure additive DDL
-- (new table only). "全自动不等于偷偷烧用户 BYOK": auto_distill_enabled /
-- auto_consolidate_enabled must be visible+togglable per user, never a hidden always-on
-- default that silently spends the user's own Provider quota.

CREATE TABLE control.memory_automation_policies (
  policy_id                            uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id                            uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  user_id                              uuid        NOT NULL REFERENCES control.users(user_id),
  auto_distill_enabled                 boolean     NOT NULL DEFAULT true,
  auto_consolidate_enabled             boolean     NOT NULL DEFAULT true,
  max_byok_tokens_per_period           bigint,
  consolidation_trigger_policy_version bigint      NOT NULL DEFAULT 1,
  updated_at                           timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, user_id)
);

COMMENT ON TABLE control.memory_automation_policies IS
  '§11.5.1: one row per (tenant, user). Scheduler consults this before ever queuing a '
  'background USER_REASONING call — RUN/SKIP_DISABLED/SKIP_NO_DELTA/SKIP_BUDGET/WAITING_KEY '
  'are all the scheduling outcomes; SKIP_* is a healthy terminal state, never FAILED/DLQ.';

-- §62 tenant RLS, NULLIF form — same tenant+owner shape as user_reasoning_profiles (0048):
-- this is a per-user policy row, not a tenant-wide default.
ALTER TABLE control.memory_automation_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.memory_automation_policies FORCE ROW LEVEL SECURITY;
CREATE POLICY memory_automation_policies_tenant_and_owner ON control.memory_automation_policies
USING (
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
  AND user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
  AND user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
);

ALTER TABLE control.memory_automation_policies OWNER TO role_migration_owner;
