-- §34.0.1: committed local-write receipts. 0113 remains immutable.
-- No body, bearer, verifier, or response cache; only identity and committed references.
CREATE TABLE control.operation_receipts (
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  principal_id uuid NOT NULL CHECK (principal_id <> '00000000-0000-0000-0000-000000000000'),
  operation text NOT NULL CHECK (operation ~ '^[a-z][a-z0-9_.]{0,95}$'),
  idempotency_key text NOT NULL CHECK (idempotency_key ~ '^[A-Za-z0-9._:-]{1,128}$'),
  request_fingerprint text NOT NULL CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
  user_id uuid,
  workspace_id uuid,
  request_id uuid NOT NULL,
  reservation_id uuid NOT NULL UNIQUE REFERENCES control.usage_reservations(reservation_id),
  status text NOT NULL DEFAULT 'COMMITTED' CHECK (status = 'COMMITTED'),
  -- Physical Evidence deletion keeps a non-replayable key tombstone rather than
  -- blocking §37 deletion or making the original key executable again.
  evidence_id uuid REFERENCES private.evidence_objects(evidence_id) ON DELETE SET NULL,
  scope_kind text NOT NULL CHECK (scope_kind IN ('tenant','workspace')),
  scope_id uuid NOT NULL,
  domain text NOT NULL CHECK (length(domain) BETWEEN 1 AND 128),
  projection_kind text NOT NULL CHECK (length(projection_kind) BETWEEN 1 AND 128),
  projection_version text NOT NULL CHECK (length(projection_version) BETWEEN 1 AND 128),
  stream_seq bigint NOT NULL CHECK (stream_seq > 0),
  commit_seq bigint NOT NULL CHECK (commit_seq > 0),
  audit_event_id uuid NOT NULL REFERENCES control.audit_events(audit_event_id),
  committed_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(committed_at)),
  replay_expires_at timestamptz NOT NULL CHECK (isfinite(replay_expires_at)),
  PRIMARY KEY (tenant_id, principal_id, operation, idempotency_key),
  FOREIGN KEY (tenant_id, request_id) REFERENCES control.usage_reservations(tenant_id, request_id),
  CHECK (replay_expires_at > committed_at),
  CHECK (user_id IS NULL OR user_id <> '00000000-0000-0000-0000-000000000000'),
  CHECK (workspace_id IS NULL OR workspace_id <> '00000000-0000-0000-0000-000000000000'),
  CHECK ((scope_kind='tenant' AND scope_id=tenant_id AND workspace_id IS NULL)
      OR (scope_kind='workspace' AND workspace_id IS NOT NULL AND scope_id=workspace_id))
);
ALTER TABLE control.operation_receipts OWNER TO role_migration_owner;
ALTER TABLE control.operation_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.operation_receipts FORCE ROW LEVEL SECURITY;
CREATE POLICY operation_receipts_tenant ON control.operation_receipts
  USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

REVOKE ALL ON control.operation_receipts FROM PUBLIC, role_gateway, role_private_worker,
  role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer,
  role_maintenance, role_admin;
GRANT SELECT, INSERT ON control.operation_receipts TO role_gateway;
GRANT SELECT ON control.operation_receipts TO role_maintenance;

-- This is an ordinary invoker trigger, not a new privileged runtime entry point.
-- gateway already has SELECT on the referenced rows; FORCE RLS remains applicable.
CREATE FUNCTION control.check_operation_receipt() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, control, private, projection, ops
AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF OLD.evidence_id IS NOT NULL AND NEW.evidence_id IS NULL
       AND (to_jsonb(NEW) - 'evidence_id') = (to_jsonb(OLD) - 'evidence_id')
       AND NOT EXISTS (SELECT 1 FROM private.evidence_objects WHERE evidence_id=OLD.evidence_id) THEN
      RETURN NEW;
    END IF;
    RAISE EXCEPTION 'operation receipts are immutable' USING ERRCODE='23514';
  END IF;
  NEW.committed_at := clock_timestamp();
  IF NEW.evidence_id IS NULL
     -- Retain scalar historical routing after workspace deletion. A lifetime FK
     -- would block that deletion; prove the current tenant/workspace at insertion.
     OR (NEW.scope_kind='workspace' AND NOT EXISTS (
       SELECT 1 FROM control.workspaces w
       WHERE w.tenant_id=NEW.tenant_id AND w.workspace_id=NEW.workspace_id
     ))
     OR COALESCE(NEW.user_id, '00000000-0000-0000-0000-000000000000'::uuid)
        IS DISTINCT FROM NULLIF(current_setting('humaux.user_id', true), '')::uuid
     OR NOT EXISTS (
       SELECT 1 FROM control.usage_reservations r
       WHERE r.reservation_id=NEW.reservation_id AND r.tenant_id=NEW.tenant_id
         AND r.principal_id=NEW.principal_id AND r.request_id=NEW.request_id
         AND r.operation=NEW.operation AND r.request_fingerprint=NEW.request_fingerprint
         AND r.status='CONSUMED' AND r.units=1 AND r.finished_at IS NOT NULL
     ) THEN
    RAISE EXCEPTION 'receipt requires the matching consumed reservation' USING ERRCODE='23514';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM private.evidence_objects e
    JOIN ops.outbox o ON o.tenant_id=e.tenant_id AND o.evidence_id=e.evidence_id
    JOIN projection.stream_log s ON s.tenant_id=o.tenant_id AND s.commit_seq=o.commit_seq
    WHERE e.evidence_id=NEW.evidence_id AND e.tenant_id=NEW.tenant_id
      AND e.origin_principal_id=NEW.principal_id AND e.origin_class='AuthenticatedAgent'
      AND o.event_type='EVIDENCE_ACCEPTED' AND o.commit_seq=NEW.commit_seq
      AND s.scope_kind=NEW.scope_kind AND s.scope_id=NEW.scope_id AND s.domain=NEW.domain
      AND s.projection_kind=NEW.projection_kind AND s.projection_version=NEW.projection_version
      AND s.stream_seq=NEW.stream_seq AND s.state<>'TOMBSTONED'
  ) OR NOT EXISTS (
    SELECT 1 FROM control.audit_events a
    WHERE a.audit_event_id=NEW.audit_event_id AND a.tenant_id=NEW.tenant_id
      AND a.actor_id=NEW.principal_id::text AND a.action='MCP_REQUEST_FINISHED'
      AND a.resource_id=NEW.operation AND a.result='OK' AND a.request_id=NEW.request_id::text
  ) THEN
    RAISE EXCEPTION 'receipt requires matching committed business and audit references' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
ALTER FUNCTION control.check_operation_receipt() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.check_operation_receipt() FROM PUBLIC;
CREATE TRIGGER operation_receipt_integrity
  BEFORE INSERT OR UPDATE ON control.operation_receipts
  FOR EACH ROW EXECUTE FUNCTION control.check_operation_receipt();

COMMENT ON TABLE control.operation_receipts IS
  '§34.0.1: append-only local-write idempotency facts. Expiry or Evidence deletion never makes a key reusable. Permissions: §6.2.2 only. No secrets or bodies.';
