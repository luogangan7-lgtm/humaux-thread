-- Metadata-only durable source for native private retrieval query disclosure.
-- No query body or provider response is stored here.
CREATE TABLE private.retrieval_query_sources (
  query_source_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  principal_id uuid NOT NULL CHECK (principal_id <> '00000000-0000-0000-0000-000000000000'),
  user_id uuid NOT NULL CHECK (user_id <> '00000000-0000-0000-0000-000000000000'),
  workspace_id uuid NOT NULL,
  request_id uuid NOT NULL,
  logical_call_id uuid NOT NULL,
  attempt_no integer NOT NULL CHECK (attempt_no > 0),
  profile_fingerprint text NOT NULL CHECK (length(profile_fingerprint) BETWEEN 1 AND 128),
  classifier_revision text NOT NULL CHECK (length(classifier_revision) BETWEEN 1 AND 128),
  data_class text NOT NULL CHECK (data_class IN ('PUBLIC','INTERNAL','PRIVATE','SENSITIVE','SECRET_MATERIAL')),
  purpose text NOT NULL CHECK (purpose = 'RETRIEVAL_EMBEDDING'),
  query_sha256 bytea NOT NULL CHECK (octet_length(query_sha256) = 32),
  query_bytes bigint NOT NULL CHECK (query_bytes >= 0),
  wire_payload_sha256 bytea NOT NULL CHECK (octet_length(wire_payload_sha256) = 32),
  wire_payload_bytes bigint NOT NULL CHECK (wire_payload_bytes >= 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(created_at)),
  expires_at timestamptz NOT NULL CHECK (isfinite(expires_at)),
  revoked_at timestamptz,
  revocation_reason text,
  CONSTRAINT retrieval_query_sources_expiry CHECK (expires_at > created_at),
  CONSTRAINT retrieval_query_sources_revocation_pair CHECK ((revoked_at IS NULL) = (revocation_reason IS NULL)),
  CONSTRAINT retrieval_query_sources_tenant_query_source_unique UNIQUE (tenant_id, query_source_id),
  CONSTRAINT retrieval_query_sources_attempt_unique UNIQUE (tenant_id, logical_call_id, attempt_no)
);
ALTER TABLE private.retrieval_query_sources OWNER TO role_migration_owner;
ALTER TABLE private.retrieval_query_sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.retrieval_query_sources FORCE ROW LEVEL SECURITY;
CREATE POLICY retrieval_query_sources_tenant ON private.retrieval_query_sources
  USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

REVOKE ALL ON private.retrieval_query_sources FROM PUBLIC, role_gateway, role_private_worker,
  role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer,
  role_maintenance, role_admin;
GRANT SELECT, INSERT ON private.retrieval_query_sources TO role_retrieval_worker;
GRANT SELECT, UPDATE (revoked_at, revocation_reason) ON private.retrieval_query_sources TO role_maintenance;

CREATE FUNCTION private.retrieval_query_sources_guard_mutation() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'retrieval query sources are retained metadata; DELETE is not permitted'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF TG_OP = 'INSERT' THEN
    IF NEW.tenant_id IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
       OR NEW.user_id IS DISTINCT FROM NULLIF(current_setting('humaux.user_id', true), '')::uuid
       OR NEW.principal_id IS DISTINCT FROM NULLIF(current_setting('humaux.principal_id', true), '')::uuid
       OR NEW.revoked_at IS NOT NULL
       OR NEW.revocation_reason IS NOT NULL
       OR NOT EXISTS (
         SELECT 1 FROM control.workspaces w
         WHERE w.tenant_id = NEW.tenant_id AND w.workspace_id = NEW.workspace_id
       )
    THEN
      RAISE EXCEPTION 'retrieval query source requires trusted tenant/user/principal/workspace binding'
        USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
  END IF;
  IF current_user = 'role_maintenance'
     AND OLD.revoked_at IS NULL
     AND NEW.revoked_at IS NOT NULL
     AND NULLIF(btrim(NEW.revocation_reason), '') IS NOT NULL
     AND (to_jsonb(NEW) - ARRAY['revoked_at', 'revocation_reason'])
         = (to_jsonb(OLD) - ARRAY['revoked_at', 'revocation_reason'])
  THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'retrieval query source metadata is immutable except maintenance revocation'
    USING ERRCODE = 'insufficient_privilege';
END;
$$;
ALTER FUNCTION private.retrieval_query_sources_guard_mutation() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION private.retrieval_query_sources_guard_mutation() FROM PUBLIC;
CREATE TRIGGER retrieval_query_sources_guard_mutation
BEFORE INSERT OR UPDATE OR DELETE ON private.retrieval_query_sources
FOR EACH ROW EXECUTE FUNCTION private.retrieval_query_sources_guard_mutation();

ALTER TABLE ops.data_disclosures
  ADD CONSTRAINT data_disclosures_tenant_disclosure_unique UNIQUE (tenant_id, disclosure_id);
ALTER TABLE ops.data_disclosure_sources
  ADD COLUMN query_source_id uuid REFERENCES private.retrieval_query_sources(query_source_id),
  ADD CONSTRAINT data_disclosure_sources_tenant_disclosure_fk
    FOREIGN KEY (tenant_id, disclosure_id) REFERENCES ops.data_disclosures(tenant_id, disclosure_id),
  ADD CONSTRAINT data_disclosure_sources_tenant_query_source_fk
    FOREIGN KEY (tenant_id, query_source_id)
    REFERENCES private.retrieval_query_sources(tenant_id, query_source_id);
ALTER TABLE ops.data_disclosure_sources
  DROP CONSTRAINT data_disclosure_sources_source_kind_check,
  DROP CONSTRAINT data_disclosure_sources_exactly_one_id,
  DROP CONSTRAINT data_disclosure_sources_kind_matches_id,
  ADD CONSTRAINT data_disclosure_sources_source_kind_check CHECK (
    source_kind IN ('EVIDENCE','MEMORY','ROLLUP','PUBLIC_RELEASE','RETRIEVAL_QUERY')
  ),
  ADD CONSTRAINT data_disclosure_sources_exactly_one_id CHECK (
    num_nonnulls(evidence_id, memory_id, rollup_id, release_id, query_source_id) = 1
  ),
  ADD CONSTRAINT data_disclosure_sources_kind_matches_id CHECK (
    (source_kind = 'EVIDENCE'        AND evidence_id IS NOT NULL)
    OR (source_kind = 'MEMORY'         AND memory_id   IS NOT NULL)
    OR (source_kind = 'ROLLUP'         AND rollup_id   IS NOT NULL)
    OR (source_kind = 'PUBLIC_RELEASE' AND release_id  IS NOT NULL)
    OR (source_kind = 'RETRIEVAL_QUERY' AND query_source_id IS NOT NULL)
  );
CREATE INDEX idx_data_disclosure_sources_query_source
  ON ops.data_disclosure_sources (query_source_id) WHERE query_source_id IS NOT NULL;

-- Generic INSERT keeps the four established source kinds. Only the SECURITY DEFINER
-- function below can attach RETRIEVAL_QUERY, and it is executable only by retrieval worker.
CREATE FUNCTION ops.data_disclosure_sources_guard_retrieval_query() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.source_kind = 'RETRIEVAL_QUERY' AND current_user <> 'role_migration_owner' THEN
    RAISE EXCEPTION 'RETRIEVAL_QUERY requires the typed retrieval-query reserve path'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  RETURN NEW;
END;
$$;
ALTER FUNCTION ops.data_disclosure_sources_guard_retrieval_query() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.data_disclosure_sources_guard_retrieval_query() FROM PUBLIC;
CREATE TRIGGER data_disclosure_sources_guard_retrieval_query
BEFORE INSERT ON ops.data_disclosure_sources
FOR EACH ROW EXECUTE FUNCTION ops.data_disclosure_sources_guard_retrieval_query();

CREATE FUNCTION ops.attach_retrieval_query_source(
  p_tenant_id uuid,
  p_disclosure_id uuid,
  p_query_source_id uuid,
  p_ordinal integer
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  source_row private.retrieval_query_sources%ROWTYPE;
  disclosure_ok boolean;
BEGIN
  IF session_user <> 'role_retrieval_worker' THEN
    RAISE EXCEPTION 'typed retrieval-query reserve requires role_retrieval_worker LOGIN'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  SELECT * INTO source_row
  FROM private.retrieval_query_sources
  WHERE tenant_id = p_tenant_id AND query_source_id = p_query_source_id;
  IF NOT FOUND OR source_row.revoked_at IS NOT NULL OR source_row.expires_at <= clock_timestamp() THEN
    RAISE EXCEPTION 'retrieval query source is absent, revoked, or expired'
      USING ERRCODE = '23514';
  END IF;
  SELECT EXISTS (
    SELECT 1 FROM ops.data_disclosures d
    WHERE d.tenant_id = p_tenant_id
      AND d.disclosure_id = p_disclosure_id
      AND d.purpose = 'RETRIEVAL_EMBEDDING'
      AND d.data_class = source_row.data_class
      AND d.payload_sha256 = source_row.wire_payload_sha256
      AND d.payload_bytes = source_row.wire_payload_bytes
  ) INTO disclosure_ok;
  IF NOT disclosure_ok THEN
    RAISE EXCEPTION 'retrieval query source must match tenant, embedding permit, and wire payload'
      USING ERRCODE = '23514';
  END IF;
  INSERT INTO ops.data_disclosure_sources
    (tenant_id, disclosure_id, source_kind, evidence_id, memory_id, rollup_id, release_id, query_source_id, ordinal)
  VALUES
    (p_tenant_id, p_disclosure_id, 'RETRIEVAL_QUERY', NULL, NULL, NULL, NULL, p_query_source_id, p_ordinal);
END;
$$;
ALTER FUNCTION ops.attach_retrieval_query_source(uuid, uuid, uuid, integer) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.attach_retrieval_query_source(uuid, uuid, uuid, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.attach_retrieval_query_source(uuid, uuid, uuid, integer)
  TO role_retrieval_worker;

COMMENT ON TABLE private.retrieval_query_sources IS
  'Metadata-only native private retrieval-query disclosure source. No query body, provider response, or credentials; query and serialized-wire digests remain distinct.';
