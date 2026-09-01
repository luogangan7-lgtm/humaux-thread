-- Phase 9 corrective lifecycle: preserve every stored policy version as an exact audit row.
-- This migration is forward-only and does not enable Phase 10.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM private.contribution_executions e
    LEFT JOIN control.contribution_policies p
      ON (p.tenant_id,p.policy_id,p.policy_version)
       =(e.tenant_id,e.policy_id,e.policy_version)
    WHERE p.policy_id IS NULL
  ) THEN
    RAISE EXCEPTION '0132 hard stop: an execution policy version is not exactly resolvable'
      USING ERRCODE='55000';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM staging.contribution_candidates c
    LEFT JOIN control.contribution_policies p
      ON (p.tenant_id,p.policy_id,p.policy_version)
       =(c.tenant_id,c.policy_id,c.policy_version)
    WHERE p.policy_id IS NULL
  ) THEN
    RAISE EXCEPTION '0132 hard stop: a candidate policy version is not exactly resolvable'
      USING ERRCODE='55000';
  END IF;
  IF NOT EXISTS (
      SELECT 1 FROM pg_constraint
      WHERE conrelid='control.contribution_policies'::regclass
        AND conname='contribution_policies_pkey'
        AND pg_get_constraintdef(oid)='PRIMARY KEY (policy_id)'
    ) OR NOT EXISTS (
      SELECT 1 FROM pg_constraint
      WHERE conrelid='control.contribution_policies'::regclass
        AND conname='contribution_policy_one_per_tenant'
        AND pg_get_constraintdef(oid)='UNIQUE (tenant_id)'
    ) OR NOT EXISTS (
      SELECT 1 FROM pg_constraint
      WHERE conrelid='control.contribution_policies'::regclass
        AND conname='contribution_policy_tenant_key'
        AND pg_get_constraintdef(oid)='UNIQUE (tenant_id, policy_id)'
    ) OR NOT EXISTS (
      SELECT 1 FROM pg_constraint
      WHERE conrelid='control.contribution_policies'::regclass
        AND conname='contribution_policies_tenant_version_unique'
        AND pg_get_constraintdef(oid)='UNIQUE (tenant_id, policy_id, policy_version)'
    ) OR NOT EXISTS (
      SELECT 1 FROM pg_constraint
      WHERE conrelid='staging.contribution_candidates'::regclass
        AND conname='contribution_candidates_tenant_id_policy_id_fkey'
        AND pg_get_constraintdef(oid)=
          'FOREIGN KEY (tenant_id, policy_id) REFERENCES control.contribution_policies(tenant_id, policy_id)'
    ) OR NOT EXISTS (
      SELECT 1 FROM pg_constraint
      WHERE conrelid='private.contribution_executions'::regclass
        AND conname='contribution_executions_policy_fk'
        AND pg_get_constraintdef(oid)=
          'FOREIGN KEY (tenant_id, policy_id, policy_version) REFERENCES control.contribution_policies(tenant_id, policy_id, policy_version)'
    ) THEN
    RAISE EXCEPTION '0132 hard stop: contribution policy catalog contract drifted'
      USING ERRCODE='55000';
  END IF;
END;
$$;

ALTER TABLE control.contribution_policies
  ADD COLUMN effective_from timestamptz NOT NULL DEFAULT clock_timestamp(),
  ADD COLUMN effective_to timestamptz,
  ADD CONSTRAINT contribution_policies_effective_interval_check CHECK (
    isfinite(effective_from)
    AND (effective_to IS NULL OR (isfinite(effective_to) AND effective_to>effective_from))
  );
ALTER TABLE control.contribution_policies ALTER COLUMN effective_from DROP DEFAULT;

-- Keep every other contribution_input_change trigger and the global lock function intact.
DROP TRIGGER contribution_input_change ON control.contribution_policies;

ALTER TABLE staging.contribution_candidates
  DROP CONSTRAINT contribution_candidates_tenant_id_policy_id_fkey;
ALTER TABLE control.contribution_policies
  DROP CONSTRAINT contribution_policy_tenant_key,
  DROP CONSTRAINT contribution_policy_one_per_tenant,
  DROP CONSTRAINT contribution_policies_pkey,
  ADD CONSTRAINT contribution_policies_pkey PRIMARY KEY(policy_id,policy_version);
ALTER TABLE staging.contribution_candidates
  ADD CONSTRAINT contribution_candidates_policy_version_fk
  FOREIGN KEY(tenant_id,policy_id,policy_version)
  REFERENCES control.contribution_policies(tenant_id,policy_id,policy_version);

CREATE UNIQUE INDEX contribution_policies_one_open_head_per_tenant
  ON control.contribution_policies(tenant_id)
  WHERE effective_to IS NULL;

CREATE FUNCTION control.guard_contribution_policy_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SECURITY INVOKER
SET search_path=pg_catalog
AS $$
DECLARE
  prior control.contribution_policies;
BEGIN
  PERFORM ops.lock_contribution_inputs();
  IF TG_OP='DELETE' THEN
    RAISE EXCEPTION 'contribution policy versions are append-only' USING ERRCODE='23514';
  END IF;
  IF TG_OP='UPDATE' THEN
    IF OLD.effective_to IS NOT NULL
       OR NEW.effective_to IS NULL
       OR NOT isfinite(NEW.effective_to)
       OR NEW.effective_to<=OLD.effective_from
       OR NEW.policy_id IS DISTINCT FROM OLD.policy_id
       OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.allow_public_contribution IS DISTINCT FROM OLD.allow_public_contribution
       OR NEW.created_at IS DISTINCT FROM OLD.created_at
       OR NEW.policy_version IS DISTINCT FROM OLD.policy_version
       OR NEW.contribution_mode IS DISTINCT FROM OLD.contribution_mode
       OR NEW.rights_basis IS DISTINCT FROM OLD.rights_basis
       OR NEW.source_license IS DISTINCT FROM OLD.source_license
       OR NEW.publisher IS DISTINCT FROM OLD.publisher
       OR NEW.contributor_attestation IS DISTINCT FROM OLD.contributor_attestation
       OR NEW.redistribution_policy IS DISTINCT FROM OLD.redistribution_policy
       OR NEW.effective_from IS DISTINCT FROM OLD.effective_from THEN
      RAISE EXCEPTION 'only a one-time contribution policy head close is allowed'
        USING ERRCODE='23514';
    END IF;
    RETURN NEW;
  END IF;

  IF NEW.effective_to IS NOT NULL THEN
    RAISE EXCEPTION 'a new contribution policy version must be the open head'
      USING ERRCODE='23514';
  END IF;
  SELECT * INTO prior
  FROM control.contribution_policies
  WHERE tenant_id=NEW.tenant_id AND policy_id=NEW.policy_id
  ORDER BY policy_version DESC
  LIMIT 1
  FOR KEY SHARE;
  IF NOT FOUND THEN
    IF EXISTS (
      SELECT 1 FROM control.contribution_policies WHERE tenant_id=NEW.tenant_id
    ) THEN
      RAISE EXCEPTION 'contribution policy identity cannot drift within a tenant'
        USING ERRCODE='23514';
    END IF;
    IF NEW.policy_version<>1 THEN
      RAISE EXCEPTION 'an initial contribution policy must be version 1'
        USING ERRCODE='23514';
    END IF;
    IF NEW.effective_from IS NULL THEN
      NEW.effective_from:=clock_timestamp();
    END IF;
    RETURN NEW;
  END IF;
  IF prior.effective_to IS NULL
     OR NEW.policy_version<>prior.policy_version+1
     OR NEW.effective_from IS DISTINCT FROM prior.effective_to THEN
    RAISE EXCEPTION 'a contribution policy successor must exactly continue the closed head'
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER contribution_policy_lifecycle
  BEFORE INSERT OR UPDATE OR DELETE ON control.contribution_policies
  FOR EACH ROW EXECUTE FUNCTION control.guard_contribution_policy_lifecycle();

CREATE FUNCTION control.append_contribution_policy_successor(
  p_tenant_id uuid,
  p_policy_id uuid,
  p_expected_policy_version bigint,
  p_allow_public_contribution boolean,
  p_contribution_mode text,
  p_rights_basis text,
  p_source_license text,
  p_publisher text,
  p_contributor_attestation text,
  p_redistribution_policy text
)
RETURNS control.contribution_policies
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE
  head control.contribution_policies;
  successor control.contribution_policies;
  effective_at timestamptz;
BEGIN
  IF current_setting('transaction_isolation')<>'read committed' THEN
    RAISE EXCEPTION 'policy successor requires READ COMMITTED' USING ERRCODE='40001';
  END IF;
  IF p_tenant_id IS NULL OR p_policy_id IS NULL OR p_expected_policy_version IS NULL
     OR p_allow_public_contribution IS NULL OR p_contribution_mode IS NULL THEN
    RAISE EXCEPTION 'policy successor identity and mode are required' USING ERRCODE='22023';
  END IF;
  -- The table is FORCE RLS; the owner-only command still supplies the exact tenant context.
  PERFORM set_config('humaux.tenant_id',p_tenant_id::text,true);
  PERFORM ops.lock_contribution_inputs();
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'contribution-policy:'||p_tenant_id::text||':'||p_policy_id::text,0
  ));
  SELECT * INTO head
  FROM control.contribution_policies
  WHERE tenant_id=p_tenant_id AND policy_id=p_policy_id AND effective_to IS NULL
  FOR UPDATE;
  IF NOT FOUND OR head.policy_version<>p_expected_policy_version THEN
    RAISE EXCEPTION 'stale contribution policy successor expectation' USING ERRCODE='40001';
  END IF;
  effective_at:=clock_timestamp();
  UPDATE control.contribution_policies
  SET effective_to=effective_at
  WHERE tenant_id=p_tenant_id AND policy_id=p_policy_id
    AND policy_version=p_expected_policy_version AND effective_to IS NULL;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'contribution policy head changed while appending successor'
      USING ERRCODE='40001';
  END IF;
  INSERT INTO control.contribution_policies(
    policy_id,tenant_id,allow_public_contribution,policy_version,contribution_mode,
    rights_basis,source_license,publisher,contributor_attestation,redistribution_policy,
    effective_from,effective_to
  ) VALUES (
    p_policy_id,p_tenant_id,p_allow_public_contribution,p_expected_policy_version+1,
    p_contribution_mode,p_rights_basis,p_source_license,p_publisher,
    p_contributor_attestation,p_redistribution_policy,effective_at,NULL
  )
  RETURNING * INTO successor;
  RETURN successor;
END;
$$;

ALTER FUNCTION control.guard_contribution_policy_lifecycle() OWNER TO role_migration_owner;
ALTER FUNCTION control.append_contribution_policy_successor(
  uuid,uuid,bigint,boolean,text,text,text,text,text,text
) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.guard_contribution_policy_lifecycle() FROM PUBLIC;
REVOKE ALL ON FUNCTION control.append_contribution_policy_successor(
  uuid,uuid,bigint,boolean,text,text,text,text,text,text
) FROM PUBLIC,role_admin,role_gateway,role_private_worker,role_consolidation_worker,
  role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;
REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON control.contribution_policies
  FROM role_admin,role_gateway,role_private_worker,role_consolidation_worker,
  role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;

-- 0130's complete release guard, with only the exact policy-head predicate added.
CREATE OR REPLACE FUNCTION staging.guard_finalized_contribution()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SECURITY INVOKER
SET search_path = pg_catalog
AS $$
DECLARE c staging.contribution_candidates;
BEGIN
  IF NEW.candidate_id IS NULL THEN RETURN NEW; END IF;
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
      AND p.effective_to IS NULL
      AND p.allow_public_contribution AND p.contribution_mode='MANUAL') OR
    NOT EXISTS(SELECT 1 FROM control.users u JOIN control.memberships m USING(user_id)
      JOIN control.tenants t USING(tenant_id) WHERE u.user_id=c.user_id AND m.tenant_id=c.tenant_id
      AND u.state='ACTIVE' AND m.state='ACTIVE' AND t.state='ACTIVE') OR
    (
      c.model_call_id IS NULL AND NOT EXISTS(
        SELECT 1 FROM control.user_reasoning_profiles p
         WHERE p.tenant_id=c.tenant_id
           AND p.user_id=c.user_id
           AND p.profile_version=c.profile_version
           AND p.enabled
      )
    ) OR
    (
      c.model_call_id IS NOT NULL AND NOT EXISTS(
        SELECT 1 FROM ops.model_call_ledger call
        JOIN ops.data_disclosures disclosure
          ON disclosure.tenant_id=call.tenant_id
         AND disclosure.model_call_id=call.model_call_id
         AND disclosure.processor_id=call.egress_processor_id
         AND disclosure.finalized_at IS NOT NULL
         AND disclosure.outcome='SUCCESS'
         WHERE call.tenant_id=c.tenant_id
           AND call.model_call_id=c.model_call_id
           AND call.purpose='CONTRIBUTION_DEIDENTIFY'
           AND call.call_kind='TYPED_ASSESSMENT'
           AND call.status='SUCCEEDED'
           AND call.binding_id=c.binding_id
           AND call.binding_version=c.binding_version
           AND call.reasoning_domain_id=c.reasoning_domain_id
           AND call.profile_version=c.profile_version
      )
    ) THEN
    RAISE EXCEPTION 'confirmation, authorization, policy or exact reasoning call is stale'
      USING ERRCODE='42501';
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

ALTER FUNCTION staging.guard_finalized_contribution() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION staging.guard_finalized_contribution() FROM PUBLIC;
