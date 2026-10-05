-- §46 (EXPAND_CONTRACT; card 35 S1, ADR-0062 D-D / D-F). Two objects for the resident `humaux-maintenance --serve`:
--   ops.maintenance_receipts                   one row per purge-door call that removed rows (D-F). Written only by
--                                              the owner purge doors of a later migration, inside their own single
--                                              statement; role_maintenance SELECT, every other runtime role nothing.
--   control.maintenance_tenant_page(uuid,int)  caller: humaux-maintenance --serve / sweep once
--                                              (adapters::maintenance_repo::tenant_page); fence: EXECUTE
--                                              role_maintenance only, p_limit > 0, returns tenant ids and nothing
--                                              else; owner role_migration_owner (the 0112 bootstrap policy is what
--                                              lets the owner see every control.tenants row).
-- ADR-0062 D-D: every sweep and purge stays per tenant under the caller's own tenant GUC; this page is the only
-- cross-tenant read the daemon holds, and it carries no write path.

CREATE TABLE ops.maintenance_receipts (
  receipt_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id  uuid        NOT NULL REFERENCES control.tenants (tenant_id),
  task       text        NOT NULL
             CHECK (task IN ('confirm_tokens', 'selection_snapshots', 'rate_buckets', 'terminal_jobs')),
  cutoff     timestamptz NOT NULL,
  row_limit  integer     NOT NULL CHECK (row_limit > 0),
  -- D-F: a call that removed nothing writes no receipt, so idle tenants never grow this table.
  affected   bigint      NOT NULL CHECK (affected > 0),
  ran_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
  ran_by     text        NOT NULL DEFAULT session_user
);
CREATE INDEX maintenance_receipts_tenant_ran_at_idx ON ops.maintenance_receipts (tenant_id, ran_at);

ALTER TABLE ops.maintenance_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.maintenance_receipts FORCE ROW LEVEL SECURITY;
-- No owner arm: the doors run as the owner under the caller's tenant GUC, so the plain tenant clause admits exactly
-- the calling tenant's receipt.
CREATE POLICY maintenance_receipts_tenant_isolation ON ops.maintenance_receipts
  USING (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid)
  WITH CHECK (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid);

-- §6.2.2 column ops.maintenance_receipts (0161 recipe): re-own, revoke the ops-domain default, grant the one cell.
ALTER TABLE ops.maintenance_receipts OWNER TO role_migration_owner;
REVOKE ALL ON ops.maintenance_receipts
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;
GRANT SELECT ON ops.maintenance_receipts TO role_maintenance;

-- plpgsql only for the p_limit refusal (a LANGUAGE sql LIMIT NULL would mean "every row").
CREATE FUNCTION control.maintenance_tenant_page(p_after uuid, p_limit integer)
RETURNS SETOF uuid
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  IF p_limit IS NULL OR p_limit <= 0 THEN
    RAISE EXCEPTION 'maintenance_tenant_page: p_limit must be > 0' USING ERRCODE = '22023';
  END IF;
  -- Keyset page: strictly after the cursor, so one rotation visits every tenant exactly once.
  RETURN QUERY
    SELECT t.tenant_id FROM control.tenants t
     WHERE p_after IS NULL OR t.tenant_id > p_after
     ORDER BY t.tenant_id
     LIMIT p_limit;
END;
$$;
ALTER FUNCTION control.maintenance_tenant_page(uuid, integer) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.maintenance_tenant_page(uuid, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.maintenance_tenant_page(uuid, integer) TO role_maintenance;

COMMENT ON TABLE ops.maintenance_receipts IS
  'ADR-0062 D-F: one row per purge-door call that removed rows, written in the same statement as the delete.';
COMMENT ON FUNCTION control.maintenance_tenant_page(uuid, integer) IS
  'ADR-0062 D-D: keyset page of tenant ids for humaux-maintenance --serve; EXECUTE role_maintenance only.';
