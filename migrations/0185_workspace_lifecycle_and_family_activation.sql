-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0184). ADR-0053 (card 28): production
-- onboarding + explicit VerifiedEmpty first activation (ADR-0017 amendment).
--
-- 1. control.workspaces.lifecycle — LEGACY (every pre-0185 row and every owner-DSN fixture: never
--    gated), PROVISIONING (written only by control.onboard_workspace, 0186), READY (written only by
--    projection.activate_empty_family, 0186). No non-owner role holds INSERT/UPDATE on the table,
--    so the two owner definers are the only doors (xtask rls-check asserts it).
-- 2. control.tenants.onboarding_name — the idempotency key of `humaux-maintenance onboard tenant`
--    (tenant `name` is not unique: fixtures reuse constant names). NULL for every non-onboarded row.
-- 3. projection.family_activations — the VerifiedEmpty activation receipt, one per family+version,
--    FK to the checkpoint row it activated. Owner-owned, FORCE RLS, role_maintenance SELECT only.
-- 4. The write gate: an owner-owned SECURITY INVOKER trigger on the one statement every
--    stream-issuing write executes (`UPDATE projection.stream_checkpoints SET issued_highwater =
--    issued_highwater + 1`, adapters::remember::issue_stream_log_row). A PROVISIONING workspace
--    refuses it with SQLSTATE 55000 `workspace_provisioning`, inside the writer's transaction.
--    Invoker: every stream-issuing role already holds SELECT on control.workspaces and has its
--    tenant GUC installed (stream_checkpoints' own RLS requires it for the UPDATE to see the row).
--
-- No DML, no new role, no table grant beyond the one SELECT.

-- 1. Lifecycle. Constant default ⇒ metadata-only ADD COLUMN, the CHECK validates in the same scan.
ALTER TABLE control.workspaces
  ADD COLUMN lifecycle text NOT NULL DEFAULT 'LEGACY',
  ADD CONSTRAINT workspaces_lifecycle_known CHECK (lifecycle IN ('LEGACY', 'PROVISIONING', 'READY'));
COMMENT ON COLUMN control.workspaces.lifecycle IS
  'ADR-0053 (0185): LEGACY = created outside onboarding, never gated. PROVISIONING = onboarded, first activation pending, stream-issuing writes refused (55000 workspace_provisioning). READY = every checkpoint family of the workspace has a serving version. Written only by control.onboard_workspace / projection.activate_empty_family.';

-- One onboarded workspace per (tenant, name): the idempotency key of `onboard workspace`.
CREATE UNIQUE INDEX workspaces_onboarded_name_uniq
  ON control.workspaces (tenant_id, name) WHERE lifecycle <> 'LEGACY';

-- 2. Tenant onboarding name.
ALTER TABLE control.tenants
  ADD COLUMN onboarding_name text
    CONSTRAINT tenants_onboarding_name_length CHECK (length(onboarding_name) BETWEEN 1 AND 128);
COMMENT ON COLUMN control.tenants.onboarding_name IS
  'ADR-0053 (0185): idempotency key of humaux-maintenance onboard tenant --name. NULL = not onboarded through control.onboard_tenant.';
CREATE UNIQUE INDEX tenants_onboarding_name_uniq ON control.tenants (onboarding_name);

-- 3. Activation receipts.
CREATE TABLE projection.family_activations (
  tenant_id             uuid NOT NULL,
  scope_kind            text NOT NULL,
  scope_id              uuid NOT NULL,
  domain                text NOT NULL,
  projection_kind       text NOT NULL,
  projection_version    text NOT NULL,
  evidence_kind         text NOT NULL CHECK (evidence_kind = 'VERIFIED_EMPTY'),
  initialized_head      bigint NOT NULL CHECK (initialized_head = 0),
  collection_name       text NOT NULL CHECK (length(collection_name) BETWEEN 1 AND 255),
  collection_generation text NOT NULL CHECK (length(collection_generation) BETWEEN 1 AND 512),
  probe_id              uuid NOT NULL UNIQUE,
  probe_visible         bigint NOT NULL CHECK (probe_visible = 0),
  probed_at             timestamptz NOT NULL,
  activated_at          timestamptz NOT NULL DEFAULT now(),
  activated_by          text NOT NULL DEFAULT session_user,
  PRIMARY KEY (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version),
  FOREIGN KEY (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version)
    REFERENCES projection.stream_checkpoints
      (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version)
);
COMMENT ON TABLE projection.family_activations IS
  'ADR-0053 (0185): VerifiedEmpty first-activation receipt, bound to the full family+version and the physical collection generation the empty probe read. Written only by projection.activate_empty_family (0186).';

ALTER TABLE projection.family_activations ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection.family_activations FORCE ROW LEVEL SECURITY;
CREATE POLICY family_activations_tenant_isolation ON projection.family_activations
  USING (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid)
  WITH CHECK (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid);

-- Owner + NAMED §6.2.2 grants (0161 recipe): re-own, REVOKE everything the projection-domain
-- default may have granted, grant back exactly the matrix column (role_maintenance SELECT).
ALTER TABLE projection.family_activations OWNER TO role_migration_owner;
REVOKE ALL ON projection.family_activations FROM PUBLIC,
  role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT SELECT ON projection.family_activations TO role_maintenance;

-- 4. The write gate.
CREATE FUNCTION projection.stream_checkpoints_workspace_write_gate() RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM control.workspaces w
     WHERE w.tenant_id = NEW.tenant_id
       AND w.workspace_id = NEW.scope_id
       AND w.lifecycle = 'PROVISIONING'
  ) THEN
    RAISE EXCEPTION 'workspace_provisioning'
      USING ERRCODE = '55000', DETAIL = 'workspace ' || NEW.scope_id::text;
  END IF;
  RETURN NEW;
END;
$$;
ALTER FUNCTION projection.stream_checkpoints_workspace_write_gate() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION projection.stream_checkpoints_workspace_write_gate() FROM PUBLIC;

CREATE TRIGGER stream_checkpoints_workspace_write_gate
  BEFORE UPDATE OF issued_highwater ON projection.stream_checkpoints
  FOR EACH ROW
  WHEN (NEW.scope_kind = 'workspace' AND NEW.issued_highwater > OLD.issued_highwater)
  EXECUTE FUNCTION projection.stream_checkpoints_workspace_write_gate();
