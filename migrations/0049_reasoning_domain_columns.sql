-- §11.2.1 PrivateReasoningDomain / reasoning_domain_grants — completes the canonical schema.
-- 0003 (P1) only built the columns §48.0①'s FK target needed at that time
-- (reasoning_domain_id/tenant_id/name/created_at, plus the bare grant link). This migration
-- adds §11.2.1's remaining frozen fields without touching the existing rows or the two
-- other migrations' FK columns that already reference reasoning_domain_id.
--
-- Both tables are currently empty in every environment this migration has run against (no
-- INSERT into control.reasoning_domain_grants exists anywhere in the codebase yet, and the
-- only two INSERTs into control.private_reasoning_domains — crates/adapters/tests/
-- outbox_batch_remember.rs and retrieve_read_your_writes.rs — only ever set
-- (reasoning_domain_id, tenant_id, name)) — every new column below is therefore added
-- nullable-first and tightened, never NOT NULL in one step, so a future non-empty table
-- fails this migration loudly instead of silently truncating rows (§46 "改成跨事务
-- LIMIT/OFFSET 必须红"-style discipline: prefer a loud migration failure to a quiet one).

-- =============================================================================
-- control.private_reasoning_domains: owner_user_id / user_reasoning_profile_id / status.
-- Nullable — 0003's existing test fixtures insert only (reasoning_domain_id, tenant_id,
-- name) and are out of this task's file scope to edit (CLAUDE.md "只动你拥有的文件").
-- =============================================================================

ALTER TABLE control.private_reasoning_domains
  ADD COLUMN owner_user_id uuid REFERENCES control.users(user_id),
  ADD COLUMN user_reasoning_profile_id uuid REFERENCES control.user_reasoning_profiles(profile_id),
  ADD COLUMN status text;

-- §11.2.1 gives no frozen enum for `status`; ACTIVE/SUSPENDED/REVOKED is this migration's
-- own closed set (documented here, not spec-frozen) — widening it is a new migration, not
-- an app-side string.
ALTER TABLE control.private_reasoning_domains
  ADD CONSTRAINT private_reasoning_domains_status_known
    CHECK (status IN ('ACTIVE', 'SUSPENDED', 'REVOKED'));
ALTER TABLE control.private_reasoning_domains ALTER COLUMN status SET DEFAULT 'ACTIVE';
UPDATE control.private_reasoning_domains SET status = 'ACTIVE' WHERE status IS NULL;
ALTER TABLE control.private_reasoning_domains ALTER COLUMN status SET NOT NULL;

-- §11.2.1 "owner_user_id 与 user_reasoning_profile_id 必须属于同一 tenant/user" — a native
-- CHECK cannot cross-reference another table, so this is a BEFORE trigger. Fires only when
-- user_reasoning_profile_id is actually set (a domain may exist with no profile bound yet,
-- e.g. mid-provisioning) — Ingress rules in §11.2.1 already require *some* real user's
-- profile before any LLM stage may run; that gate lives in application code (the private
-- distillation pipeline itself), not the DB row shape.
CREATE FUNCTION control.private_reasoning_domains_check_profile_owner()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  prof_tenant uuid;
  prof_user   uuid;
BEGIN
  IF NEW.user_reasoning_profile_id IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT tenant_id, user_id INTO prof_tenant, prof_user
  FROM control.user_reasoning_profiles
  WHERE profile_id = NEW.user_reasoning_profile_id;

  IF prof_tenant IS NULL THEN
    RAISE EXCEPTION 'user_reasoning_profile_id % does not exist', NEW.user_reasoning_profile_id;
  END IF;
  IF prof_tenant <> NEW.tenant_id THEN
    RAISE EXCEPTION '§11.2.1: user_reasoning_profile_id must belong to the same tenant as the reasoning domain (domain tenant=%, profile tenant=%)', NEW.tenant_id, prof_tenant;
  END IF;
  IF NEW.owner_user_id IS NOT NULL AND prof_user <> NEW.owner_user_id THEN
    RAISE EXCEPTION '§11.2.1: user_reasoning_profile_id must belong to owner_user_id (domain owner=%, profile owner=%)', NEW.owner_user_id, prof_user;
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER private_reasoning_domains_check_profile_owner
BEFORE INSERT OR UPDATE ON control.private_reasoning_domains
FOR EACH ROW EXECUTE FUNCTION control.private_reasoning_domains_check_profile_owner();

ALTER FUNCTION control.private_reasoning_domains_check_profile_owner() OWNER TO role_migration_owner;

-- =============================================================================
-- control.reasoning_domain_grants: workspace_id / purposes[] / expires_at / revoked_at.
-- Table has zero rows anywhere this migration has run (grepped: no INSERT target exists in
-- the codebase yet) — tightened straight to NOT NULL where §11.2.1 treats the field as
-- required (purposes, expires_at: "Service Credential ... 还必须带 reasoning_domain_grant +
-- workspace scope + purpose + expiry", i.e. a grant without an expiry is not a valid grant).
-- =============================================================================

ALTER TABLE control.reasoning_domain_grants
  ADD COLUMN workspace_id uuid REFERENCES control.workspaces(workspace_id),
  ADD COLUMN purposes text[],
  ADD COLUMN expires_at timestamptz,
  ADD COLUMN revoked_at timestamptz;

UPDATE control.reasoning_domain_grants SET purposes = '{}' WHERE purposes IS NULL;
ALTER TABLE control.reasoning_domain_grants ALTER COLUMN purposes SET DEFAULT '{}';
ALTER TABLE control.reasoning_domain_grants ALTER COLUMN purposes SET NOT NULL;
-- expires_at gets no DEFAULT on purpose: a grant's expiry is a real business value the
-- caller must supply, never silently defaulted — if this ALTER fails here, some other
-- migration inserted a row without one, which is exactly the loud failure this migration
-- prefers (see file header).
ALTER TABLE control.reasoning_domain_grants ALTER COLUMN expires_at SET NOT NULL;

COMMENT ON TABLE control.reasoning_domain_grants IS
  '§11.2.1: Headless Agent / Service Credential grant onto a PrivateReasoningDomain. '
  'purposes[] and expires_at are mandatory — "无 grant 时 Evidence 可以持久化，但 LLM stage '
  '进入 WAITING_USER_REASONING_GRANT"; revoked_at is the soft-revoke marker checked at grant '
  'lookup time, never a DELETE (no runtime role ever gets DELETE, §6.2.1).';

-- §62 RLS: this table carries no `tenant_id` column of its own (§11.2.1's canonical schema
-- has none) — same "junction table scoped through its parent's FK" shape 0012 already used
-- for private.memory_evidence / private.evidence_edges. Without this, the table would be
-- readable by every runtime role across every tenant (control.* domain default = R) with no
-- RLS predicate at all — neither xtask rls-check's literal-`tenant_id`-column scans would
-- catch that gap (check_rls_four_item and check_admin_plane_no_leak both key off an actual
-- `tenant_id`-named column, which this table deliberately does not carry), so it is not a
-- CI-enforced gap — it is fixed here on the merits, not because a gate would otherwise fail.
CREATE POLICY reasoning_domain_grants_tenant ON control.reasoning_domain_grants
USING (EXISTS (
  SELECT 1 FROM control.private_reasoning_domains d
  WHERE d.reasoning_domain_id = reasoning_domain_grants.reasoning_domain_id
    AND d.tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
))
WITH CHECK (EXISTS (
  SELECT 1 FROM control.private_reasoning_domains d
  WHERE d.reasoning_domain_id = reasoning_domain_grants.reasoning_domain_id
    AND d.tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
));

ALTER TABLE control.reasoning_domain_grants ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.reasoning_domain_grants FORCE ROW LEVEL SECURITY;

-- Ownership note: both tables were already owned by role_migration_owner via 0011's
-- retroactive loop (they existed in 0003, before 0011 ran) — ALTER TABLE/CREATE POLICY
-- above do not change ownership, so no ALTER ... OWNER TO is needed for the tables
-- themselves. The new trigger function above is a fresh catalog object created by this
-- migration's own connection (the runner, e.g. `postgres` in dev) and is reassigned
-- explicitly above, matching 0032's control.credit_ledger_reject_mutation() precedent.
