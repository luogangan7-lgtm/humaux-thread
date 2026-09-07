-- §6.1.1 / §6.2.2 / §48.2 / §46 forward-fix / ADR-0035 (card 13). Workspace-level membership.
--
-- Canon §6.1.1 already requires that WORKSPACE_SHARED(W) be readable iff the reader holds an
-- ACTIVE Tenant Membership AND an ACTIVE WorkspaceMembership(T, W, U). Until now no such table
-- existed: 0012's WORKSPACE_SHARED arm read "an ACTIVE tenant membership exists", which grants
-- every tenant member every workspace — below Canon (web-GPT ruling 2026-09-07, archived at
-- research/webgpt-workspace-membership-model-20260907.md). This migration lands the missing
-- table + the backfill that keeps today's access, and 0163 re-points the three policies' arm.
--
-- The table is control-domain (Tenant×Workspace×User admission facts, like control.memberships),
-- tenant-scoped, ENABLE + FORCE RLS. Its only runtime read is the §6.1.1 WORKSPACE_SHARED EXISTS
-- (evaluated by role_gateway on a user read; retrieval_worker/maintenance read for card-9
-- prefilter / ops), so a self-read policy (tenant GUC AND user GUC both match the row) is the
-- whole runtime surface: a request only ever needs to know its OWN memberships. No runtime role
-- gets INSERT/UPDATE; membership writes go through the owner SECURITY DEFINER
-- control.set_workspace_membership(...), EXECUTE to role_maintenance only (the §6.3 admin path,
-- ADR-0033 shape), called after admin authorization.
--
-- Composite (tenant-leg) FKs, exactly card 7's subjects pattern: RI checks bypass RLS, so a
-- single-column FK would let tenant B attach rows to — and probe the existence of — tenant A's
-- ids. control.workspaces already carries UNIQUE(tenant_id, workspace_id)
-- (workspaces_tenant_workspace_key) and control.memberships UNIQUE(tenant_id, user_id), so both
-- composite FK targets exist; no UNIQUE is added here.
--
-- §46 forward-fix (EXPAND_CONTRACT, next free number after 0161). One DML statement: the backfill
-- (D-B) BEFORE 0163 re-points the policy, so no current tenant member loses access at cutover.

CREATE TABLE control.workspace_memberships (
  tenant_id    uuid NOT NULL,
  workspace_id uuid NOT NULL,
  user_id      uuid NOT NULL,
  role         text NOT NULL CHECK (role IN ('OWNER', 'MEMBER')),
  state        text NOT NULL DEFAULT 'ACTIVE'
               CHECK (state IN ('ACTIVE', 'SUSPENDED', 'REMOVED')),
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, workspace_id, user_id),
  -- Tenant-leg composite FKs (card 7 subjects pattern): a workspace membership can only name
  -- its own tenant's membership and workspace rows.
  CONSTRAINT workspace_memberships_membership_fkey
    FOREIGN KEY (tenant_id, user_id) REFERENCES control.memberships (tenant_id, user_id)
    ON DELETE CASCADE,
  CONSTRAINT workspace_memberships_workspace_fkey
    FOREIGN KEY (tenant_id, workspace_id) REFERENCES control.workspaces (tenant_id, workspace_id)
    ON DELETE CASCADE
);

-- The one runtime lookup: "the ACTIVE workspaces of (tenant, user)". Partial index over exactly
-- that predicate so the §6.1.1 EXISTS and the auth-layer derivation are index-only.
CREATE INDEX workspace_memberships_active_idx
  ON control.workspace_memberships (tenant_id, user_id, workspace_id)
  WHERE state = 'ACTIVE';

-- §62 tenant isolation (the §48.2 RLS-四项 template: one ALL policy, tenant clause in USING AND
-- WITH CHECK) AND a self-read narrowing: a request may READ only its own (tenant, user)
-- memberships. The self-read is an AS RESTRICTIVE SELECT policy (AND-ed on top of the permissive
-- tenant policy), so it narrows reads without gating the definer write (WITH CHECK stays the plain
-- tenant clause the admin path's tenant GUC already satisfies). The user leg is NULLIF-guarded so
-- an unset user GUC (headless projection) fails closed to no rows rather than erroring.
ALTER TABLE control.workspace_memberships ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.workspace_memberships FORCE ROW LEVEL SECURITY;
CREATE POLICY workspace_memberships_tenant_isolation ON control.workspace_memberships
  USING (tenant_id = current_setting('humaux.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('humaux.tenant_id', true)::uuid);
CREATE POLICY workspace_memberships_self_read ON control.workspace_memberships
  AS RESTRICTIVE FOR SELECT
  USING (user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid);

-- Owner + NAMED §6.2.2 grants (0161 recipe): the migration connection creates the table as its
-- own login role and the control-domain §6.2.1 default auto-grants SELECT; re-own to
-- role_migration_owner and REVOKE ALL down to nothing, then grant back exactly the matrix.
-- SELECT to the SAME runtime readers control.memberships grants (0163 swapped one control table
-- for the other inside the three visibility policies, so every role that reads
-- evidence_objects / memory_records / memory_rollups must be able to evaluate the EXISTS over this
-- table too — a policy expression's referenced table is permission-checked for the querying role
-- regardless of a role-bypass OR arm). NO runtime role gets INSERT/UPDATE — the definer below is
-- the sole writer. batch_issuer / admin get nothing (they read none of the three tables).
ALTER TABLE control.workspace_memberships OWNER TO role_migration_owner;

REVOKE ALL ON control.workspace_memberships FROM PUBLIC,
  role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;

GRANT SELECT ON control.workspace_memberships
  TO role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
     role_retrieval_worker, role_maintenance;

-- The sole write path (D-A): upsert one workspace membership. SECURITY DEFINER so role_maintenance
-- (the §6.3 admin path, ADR-0033) writes without holding INSERT/UPDATE on the table itself.
-- role_migration_owner is NOSUPERUSER/NOBYPASSRLS (0011) and this table is FORCE RLS, so the
-- definer's own write IS still checked by the permissive tenant policy above: the admin caller
-- MUST have set humaux.tenant_id = p_tenant_id for the transaction (the 0161 adapter pattern) or
-- the WITH CHECK fails closed. Closed role/state sets are
-- the table's CHECK. EXECUTE to role_maintenance only; PUBLIC has none.
CREATE FUNCTION control.set_workspace_membership(
  p_tenant_id    uuid,
  p_workspace_id uuid,
  p_user_id      uuid,
  p_role         text,
  p_state        text
) RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, control
AS $$
  INSERT INTO control.workspace_memberships (tenant_id, workspace_id, user_id, role, state)
  VALUES (p_tenant_id, p_workspace_id, p_user_id, p_role, p_state)
  ON CONFLICT (tenant_id, workspace_id, user_id)
  DO UPDATE SET role = excluded.role, state = excluded.state, updated_at = now();
$$;

ALTER FUNCTION control.set_workspace_membership(uuid, uuid, uuid, text, text)
  OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.set_workspace_membership(uuid, uuid, uuid, text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.set_workspace_membership(uuid, uuid, uuid, text, text)
  TO role_maintenance;

COMMENT ON TABLE control.workspace_memberships IS
  '§6.1.1 WorkspaceMembership: a User''s ACTIVE admission to one Workspace of a Tenant. Tenant '
  'membership is admission only and does NOT fan out to every workspace (ADR-0035, card 13). '
  'Self-read RLS; the sole writer is control.set_workspace_membership (owner definer, '
  'role_maintenance EXECUTE).';
COMMENT ON FUNCTION control.set_workspace_membership(uuid, uuid, uuid, text, text) IS
  '§6.1.1 / §6.3 the sole writer of control.workspace_memberships (ADR-0035): upserts one '
  '(tenant, workspace, user) admission. SECURITY DEFINER so role_maintenance writes without '
  'table INSERT/UPDATE; called only after admin authorization.';

-- D-B backfill (BEFORE 0163 re-points the WORKSPACE_SHARED arm): every current ACTIVE tenant
-- member becomes an ACTIVE MEMBER of every existing workspace of that tenant, so today's access
-- is preserved exactly at cutover. Going forward (ADR-0035 freeze): a new workspace inserts its
-- creator OWNER; a tenant invite adds only the named/default workspace; a new tenant membership
-- never fans out. Runs as the migration superuser (RLS bypassed).
INSERT INTO control.workspace_memberships (tenant_id, workspace_id, user_id, role, state)
SELECT w.tenant_id, w.workspace_id, m.user_id, 'MEMBER', 'ACTIVE'
  FROM control.workspaces w
  JOIN control.memberships m ON m.tenant_id = w.tenant_id
 WHERE m.state = 'ACTIVE'
ON CONFLICT DO NOTHING;
