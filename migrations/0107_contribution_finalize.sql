-- ADR-0009 / §12.1.1: immutable private candidate and exact-byte manual confirmation.
-- No implicit AUTO policy or fake backfill of scanner/provider receipts.
ALTER TABLE control.contribution_policies
  ADD COLUMN policy_version bigint NOT NULL DEFAULT 1 CHECK(policy_version>0),
  ADD COLUMN contribution_mode text NOT NULL DEFAULT 'MANUAL'
    CHECK(contribution_mode IN ('DISABLED','MANUAL','AUTO_AFTER_USER_DISTILLATION')),
  ADD COLUMN rights_basis text,
  ADD COLUMN source_license text,
  ADD COLUMN publisher text,
  ADD COLUMN contributor_attestation text,
  ADD COLUMN redistribution_policy text,
  ADD CONSTRAINT contribution_policy_one_per_tenant UNIQUE(tenant_id),
  ADD CONSTRAINT contribution_policy_tenant_key UNIQUE(tenant_id,policy_id);

-- Short serialized prepare/finalize transactions coordinate with changes to inputs. No
-- control-table UPDATE grant is added just to obtain a row lock. Provider/scanner work runs
-- outside this lock. Upgrade only after measuring contention (same explicit ceiling as §12.3).
CREATE FUNCTION ops.lock_contribution_inputs()
RETURNS void LANGUAGE sql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
  SELECT pg_advisory_xact_lock(hashtextextended('humaux-contribution-inputs-v1',0))
$$;
CREATE FUNCTION ops.guard_contribution_input_change()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  PERFORM ops.lock_contribution_inputs();
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  IF TG_TABLE_SCHEMA='control' AND TG_TABLE_NAME='contribution_policies' THEN
    IF TG_OP='UPDATE' THEN NEW.policy_version:=OLD.policy_version+1; END IF;
  END IF;
  RETURN NEW;
END;
$$;
DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['control.contribution_policies','control.users','control.tenants',
    'control.memberships','control.user_reasoning_profiles','control.private_reasoning_domains',
    'private.evidence_objects','private.memory_records']
  LOOP
    EXECUTE 'CREATE TRIGGER contribution_input_change BEFORE UPDATE OR DELETE ON '||t||
      ' FOR EACH ROW EXECUTE FUNCTION ops.guard_contribution_input_change()';
  END LOOP;
END $$;

CREATE TABLE staging.contribution_candidates (
  candidate_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  user_id uuid NOT NULL REFERENCES control.users(user_id),
  policy_id uuid NOT NULL,
  policy_version bigint NOT NULL CHECK(policy_version>0),
  policy_snapshot jsonb NOT NULL CHECK(policy_snapshot->>'policy' IS NOT NULL
    AND policy_snapshot->>'policy'='MANUAL'),
  reasoning_domain_id uuid NOT NULL REFERENCES control.private_reasoning_domains(reasoning_domain_id),
  profile_version bigint NOT NULL CHECK(profile_version>0),
  source_manifest_hash bytea NOT NULL CHECK(octet_length(source_manifest_hash)=32),
  source_count integer NOT NULL CHECK(source_count>0),
  disclosed_payload bytea NOT NULL CHECK(octet_length(disclosed_payload)>0),
  disclosed_payload_sha256 bytea NOT NULL CHECK(octet_length(disclosed_payload_sha256)=32),
  provider_trace text NOT NULL CHECK(btrim(provider_trace)<>''),
  scan_receipt jsonb NOT NULL CHECK(
    jsonb_typeof(scan_receipt)='object'
    AND COALESCE(scan_receipt->>'privacy_rules_version','')<>''
    AND COALESCE(scan_receipt->>'privacy_rules_digest','')<>''
    AND COALESCE(scan_receipt->>'gitleaks_version','')<>''
    AND COALESCE(scan_receipt->>'gitleaks_binary_sha256','') ~ '^[0-9a-f]{64}$'),
  rights_basis text NOT NULL CHECK(btrim(rights_basis)<>''),
  source_license text,
  publisher text,
  contributor_attestation text,
  redistribution_policy text,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK(disclosed_payload_sha256=sha256(disclosed_payload)),
  FOREIGN KEY(tenant_id,policy_id) REFERENCES control.contribution_policies(tenant_id,policy_id),
  UNIQUE(tenant_id,candidate_id),
  UNIQUE(tenant_id,user_id,candidate_id,disclosed_payload_sha256,policy_version)
);
CREATE TABLE staging.contribution_candidate_sources (
  tenant_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  evidence_id uuid,
  memory_id uuid,
  source_hash bytea NOT NULL CHECK(octet_length(source_hash)=32),
  ordinal integer NOT NULL CHECK(ordinal>=0),
  CHECK((evidence_id IS NULL)<>(memory_id IS NULL)),
  PRIMARY KEY(candidate_id,ordinal),
  FOREIGN KEY(tenant_id,candidate_id) REFERENCES staging.contribution_candidates(tenant_id,candidate_id),
  FOREIGN KEY(tenant_id,evidence_id) REFERENCES private.evidence_objects(tenant_id,evidence_id),
  FOREIGN KEY(tenant_id,memory_id) REFERENCES private.memory_records(tenant_id,memory_id)
);
CREATE UNIQUE INDEX candidate_evidence_once ON staging.contribution_candidate_sources(candidate_id,evidence_id)
  WHERE evidence_id IS NOT NULL;
CREATE UNIQUE INDEX candidate_memory_once ON staging.contribution_candidate_sources(candidate_id,memory_id)
  WHERE memory_id IS NOT NULL;

-- Canonical manifest v1: sorted `e|m:uuid:lowercase-sha256` entries joined with `|`.
-- Both the Rust input manifest and the DB seal use these exact UTF-8 bytes.
CREATE FUNCTION staging.require_candidate_manifest()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE c staging.contribution_candidates; actual_hash bytea; actual_count bigint;
BEGIN
  SELECT * INTO c FROM staging.contribution_candidates WHERE candidate_id=NEW.candidate_id;
  SELECT count(*),sha256(convert_to(string_agg(
    (CASE WHEN evidence_id IS NOT NULL THEN 'e' ELSE 'm' END)||':'||
    COALESCE(evidence_id,memory_id)::text||':'||encode(source_hash,'hex'),'|' ORDER BY
    (CASE WHEN evidence_id IS NOT NULL THEN 'e' ELSE 'm' END),COALESCE(evidence_id,memory_id)),'UTF8'))
    INTO actual_count,actual_hash FROM staging.contribution_candidate_sources WHERE candidate_id=c.candidate_id;
  IF c.candidate_id IS NULL OR actual_count<>c.source_count OR actual_hash IS DISTINCT FROM c.source_manifest_hash THEN
    RAISE EXCEPTION 'candidate roots must equal sealed inference manifest' USING ERRCODE='23514';
  END IF;
  RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER candidate_manifest_complete AFTER INSERT ON staging.contribution_candidates
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION staging.require_candidate_manifest();
CREATE CONSTRAINT TRIGGER candidate_sources_sealed AFTER INSERT ON staging.contribution_candidate_sources
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION staging.require_candidate_manifest();

CREATE TABLE control.contribution_confirmations (
  confirmation_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id uuid NOT NULL,
  user_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  disclosed_payload_sha256 bytea NOT NULL,
  policy_version bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL CHECK(expires_at>created_at),
  FOREIGN KEY(tenant_id,user_id,candidate_id,disclosed_payload_sha256,policy_version)
    REFERENCES staging.contribution_candidates(tenant_id,user_id,candidate_id,disclosed_payload_sha256,policy_version),
  UNIQUE(tenant_id,confirmation_id)
);

ALTER TABLE staging.contribution_candidates ENABLE ROW LEVEL SECURITY;
ALTER TABLE staging.contribution_candidates FORCE ROW LEVEL SECURITY;
CREATE POLICY contribution_candidates_owner ON staging.contribution_candidates
  USING(tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid
    AND user_id=NULLIF(current_setting('humaux.user_id',true),'')::uuid)
  WITH CHECK(tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid
    AND user_id=NULLIF(current_setting('humaux.user_id',true),'')::uuid);
ALTER TABLE staging.contribution_candidate_sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE staging.contribution_candidate_sources FORCE ROW LEVEL SECURITY;
CREATE POLICY contribution_candidate_sources_owner ON staging.contribution_candidate_sources
  USING(tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid AND
    EXISTS(SELECT 1 FROM staging.contribution_candidates c WHERE c.candidate_id=contribution_candidate_sources.candidate_id))
  WITH CHECK(tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid AND
    EXISTS(SELECT 1 FROM staging.contribution_candidates c WHERE c.candidate_id=contribution_candidate_sources.candidate_id));
ALTER TABLE control.contribution_confirmations ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.contribution_confirmations FORCE ROW LEVEL SECURITY;
CREATE POLICY contribution_confirmation_owner ON control.contribution_confirmations
  USING(tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid
    AND user_id=NULLIF(current_setting('humaux.user_id',true),'')::uuid)
  WITH CHECK(tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid
    AND user_id=NULLIF(current_setting('humaux.user_id',true),'')::uuid);

REVOKE ALL ON staging.contribution_candidates,staging.contribution_candidate_sources,
  control.contribution_confirmations FROM role_gateway,role_private_worker,
  role_consolidation_worker,role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;
GRANT SELECT,INSERT ON staging.contribution_candidates,staging.contribution_candidate_sources TO role_private_worker;
GRANT SELECT,INSERT ON control.contribution_confirmations TO role_gateway;
GRANT SELECT ON control.contribution_confirmations TO role_private_worker;
GRANT SELECT ON staging.contribution_candidates,staging.contribution_candidate_sources,
  control.contribution_confirmations TO role_maintenance;
ALTER TABLE staging.contribution_candidates OWNER TO role_migration_owner;
ALTER TABLE staging.contribution_candidate_sources OWNER TO role_migration_owner;
ALTER TABLE control.contribution_confirmations OWNER TO role_migration_owner;

ALTER TABLE staging.contribution_releases
  ADD COLUMN candidate_id uuid REFERENCES staging.contribution_candidates(candidate_id),
  ADD COLUMN confirmation_id uuid,
  ADD COLUMN disclosed_payload bytea,
  ADD COLUMN disclosed_payload_sha256 bytea,
  ADD COLUMN scan_receipt jsonb,
  ADD CONSTRAINT release_confirmation_tenant_fk FOREIGN KEY(tenant_id,confirmation_id)
    REFERENCES control.contribution_confirmations(tenant_id,confirmation_id),
  ADD CONSTRAINT release_candidate_once UNIQUE(candidate_id),
  ADD CONSTRAINT release_finalized_columns CHECK(
    (candidate_id IS NULL AND confirmation_id IS NULL AND disclosed_payload IS NULL
      AND disclosed_payload_sha256 IS NULL AND scan_receipt IS NULL)
    OR (candidate_id IS NOT NULL AND confirmation_id IS NOT NULL AND disclosed_payload IS NOT NULL
      AND disclosed_payload_sha256 IS NOT NULL AND scan_receipt IS NOT NULL
      AND octet_length(disclosed_payload)>0 AND octet_length(disclosed_payload_sha256)=32
      AND sha256(disclosed_payload)=disclosed_payload_sha256));

CREATE FUNCTION staging.guard_finalized_contribution()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE c staging.contribution_candidates;
BEGIN
  IF NEW.candidate_id IS NULL THEN RETURN NEW; END IF; -- Legacy IO is not new public admission.
  PERFORM ops.lock_contribution_inputs();
  SELECT * INTO c FROM staging.contribution_candidates WHERE candidate_id=NEW.candidate_id;
  IF NOT FOUND OR c.tenant_id<>NEW.tenant_id OR
    ROW(c.disclosed_payload,c.disclosed_payload_sha256,c.scan_receipt,c.policy_snapshot,c.rights_basis,
      c.source_license,c.publisher,c.contributor_attestation,c.redistribution_policy) IS DISTINCT FROM
    ROW(NEW.disclosed_payload,NEW.disclosed_payload_sha256,NEW.scan_receipt,NEW.policy_snapshot,NEW.rights_basis,
      NEW.source_license,NEW.publisher,NEW.contributor_attestation,NEW.redistribution_policy) THEN
    RAISE EXCEPTION 'release must preserve the complete visible candidate' USING ERRCODE='23514';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM control.contribution_confirmations f
    WHERE f.confirmation_id=NEW.confirmation_id AND f.candidate_id=c.candidate_id
      AND f.tenant_id=c.tenant_id AND f.user_id=c.user_id
      AND f.disclosed_payload_sha256=c.disclosed_payload_sha256 AND f.policy_version=c.policy_version
      AND f.expires_at>clock_timestamp()) OR
    NOT EXISTS(SELECT 1 FROM control.contribution_policies p WHERE p.policy_id=c.policy_id
      AND p.tenant_id=c.tenant_id AND p.policy_version=c.policy_version
      AND p.allow_public_contribution AND p.contribution_mode='MANUAL') OR
    NOT EXISTS(SELECT 1 FROM control.users u JOIN control.memberships m USING(user_id)
      JOIN control.tenants t USING(tenant_id) WHERE u.user_id=c.user_id AND m.tenant_id=c.tenant_id
      AND u.state='ACTIVE' AND m.state='ACTIVE' AND t.state='ACTIVE') OR
    NOT EXISTS(SELECT 1 FROM control.user_reasoning_profiles p WHERE p.tenant_id=c.tenant_id
      AND p.user_id=c.user_id AND p.profile_version=c.profile_version AND p.enabled) THEN
    RAISE EXCEPTION 'confirmation, authorization, policy or profile is stale' USING ERRCODE='42501';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM staging.contribution_candidate_sources s WHERE s.candidate_id=c.candidate_id)
    OR EXISTS(SELECT 1 FROM staging.contribution_candidate_sources s
      LEFT JOIN private.evidence_objects e ON e.evidence_id=s.evidence_id
      LEFT JOIN private.memory_records m ON m.memory_id=s.memory_id
      WHERE s.candidate_id=c.candidate_id AND (
        (s.evidence_id IS NOT NULL AND (e.evidence_id IS NULL OR e.payload_sha256<>s.source_hash)) OR
        (s.memory_id IS NOT NULL AND (m.memory_id IS NULL OR m.status<>'active'
          OR sha256(convert_to(m.content::text,'UTF8'))<>s.source_hash)))) THEN
    RAISE EXCEPTION 'candidate sources are absent, invisible or changed' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER finalized_contribution_guard BEFORE INSERT ON staging.contribution_releases
  FOR EACH ROW EXECUTE FUNCTION staging.guard_finalized_contribution();

-- Frozen source receipts cannot be extended after any confirmation or release. Runtime
-- roles have no UPDATE/DELETE; this closes the remaining INSERT-after-confirmation path.
CREATE FUNCTION staging.guard_candidate_source_insert()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  PERFORM ops.lock_contribution_inputs();
  IF EXISTS(SELECT 1 FROM control.contribution_confirmations f WHERE f.candidate_id=NEW.candidate_id)
    OR EXISTS(SELECT 1 FROM staging.contribution_releases r WHERE r.candidate_id=NEW.candidate_id) THEN
    RAISE EXCEPTION 'confirmed candidate source manifest is immutable' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER candidate_source_guard BEFORE INSERT ON staging.contribution_candidate_sources
  FOR EACH ROW EXECUTE FUNCTION staging.guard_candidate_source_insert();

CREATE FUNCTION control.guard_contribution_confirmation()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  PERFORM ops.lock_contribution_inputs();
  IF NOT EXISTS(SELECT 1 FROM control.users u JOIN control.memberships m USING(user_id)
    JOIN control.tenants t USING(tenant_id) WHERE u.user_id=NEW.user_id AND m.tenant_id=NEW.tenant_id
      AND u.state='ACTIVE' AND m.state='ACTIVE' AND t.state='ACTIVE') THEN
    RAISE EXCEPTION 'confirmation requires active authorization' USING ERRCODE='42501';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER contribution_confirmation_guard BEFORE INSERT ON control.contribution_confirmations
  FOR EACH ROW EXECUTE FUNCTION control.guard_contribution_confirmation();

-- At commit the public release's source set must exactly equal the candidate's receipt.
CREATE FUNCTION staging.require_finalized_source_set()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  IF NEW.candidate_id IS NULL THEN RETURN NULL; END IF;
  IF EXISTS(
    SELECT evidence_id,memory_id FROM staging.contribution_candidate_sources WHERE candidate_id=NEW.candidate_id
    EXCEPT SELECT evidence_id,memory_id FROM staging.contribution_release_sources WHERE contribution_release_id=NEW.contribution_release_id)
    OR EXISTS(
    SELECT evidence_id,memory_id FROM staging.contribution_release_sources WHERE contribution_release_id=NEW.contribution_release_id
    EXCEPT SELECT evidence_id,memory_id FROM staging.contribution_candidate_sources WHERE candidate_id=NEW.candidate_id) THEN
    RAISE EXCEPTION 'release sources must equal confirmed candidate roots' USING ERRCODE='23514';
  END IF;
  RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER finalized_sources_equal AFTER INSERT ON staging.contribution_releases
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION staging.require_finalized_source_set();

DO $$ DECLARE f text; BEGIN
  FOREACH f IN ARRAY ARRAY['ops.lock_contribution_inputs()','ops.guard_contribution_input_change()',
    'staging.guard_finalized_contribution()','staging.guard_candidate_source_insert()',
    'control.guard_contribution_confirmation()','staging.require_finalized_source_set()',
    'staging.require_candidate_manifest()']
  LOOP
    EXECUTE 'ALTER FUNCTION '||f||' OWNER TO role_migration_owner';
    EXECUTE 'REVOKE ALL ON FUNCTION '||f||' FROM PUBLIC';
  END LOOP;
END $$;
GRANT EXECUTE ON FUNCTION ops.lock_contribution_inputs() TO role_gateway,role_private_worker,
  role_consolidation_worker,role_public_worker,role_retrieval_worker,role_maintenance;
