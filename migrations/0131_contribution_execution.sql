-- Phase 9 R4: durable authority for the two-call contribution workflow.
-- ops.jobs remains scheduling/lease authority; this table is the only business state root.

CREATE TYPE private.contribution_execution_state AS ENUM (
  'READY_A',
  'A_RESERVED',
  'READY_B',
  'B_RESERVED',
  'READY_CANDIDATE',
  'DONE',
  'NOT_CONTRIBUTABLE',
  'REJECTED_SAFETY',
  'FAILED_TERMINAL'
);

ALTER TABLE control.private_reasoning_domains
  ADD CONSTRAINT private_reasoning_domains_tenant_identity_unique
    UNIQUE (tenant_id, reasoning_domain_id);
ALTER TABLE control.contribution_policies
  ADD CONSTRAINT contribution_policies_tenant_version_unique
    UNIQUE (tenant_id, policy_id, policy_version);
ALTER TABLE ops.jobs
  ADD CONSTRAINT jobs_tenant_job_identity_unique UNIQUE (tenant_id, job_id);

CREATE TABLE private.contribution_executions (
  execution_id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  user_id uuid NOT NULL,
  state private.contribution_execution_state NOT NULL DEFAULT 'READY_A',
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),

  enqueue_idempotency_key text NOT NULL CHECK (btrim(enqueue_idempotency_key) <> ''),
  enqueue_fingerprint bytea NOT NULL CHECK (octet_length(enqueue_fingerprint) = 32),

  reasoning_domain_id uuid NOT NULL,
  input_manifest_hash bytea NOT NULL CHECK (octet_length(input_manifest_hash) = 32),
  source_count integer NOT NULL CHECK (source_count > 0),
  policy_id uuid NOT NULL,
  policy_version bigint NOT NULL CHECK (policy_version > 0),
  policy_snapshot jsonb NOT NULL CHECK (
    jsonb_typeof(policy_snapshot) = 'object'
    AND policy_snapshot->>'policy' = 'MANUAL'
  ),
  rights_basis text NOT NULL CHECK (btrim(rights_basis) <> ''),
  source_license text,
  publisher text,
  contributor_attestation text,
  redistribution_policy text,

  coverage_request_id uuid NOT NULL CHECK (
    coverage_request_id <> '00000000-0000-0000-0000-000000000000'::uuid
  ),
  assessment_request_id uuid NOT NULL CHECK (
    assessment_request_id <> '00000000-0000-0000-0000-000000000000'::uuid
  ),
  candidate_id uuid NOT NULL CHECK (
    candidate_id <> '00000000-0000-0000-0000-000000000000'::uuid
  ),
  coverage_contract_version bigint NOT NULL CHECK (coverage_contract_version > 0),
  assessment_contract_version bigint NOT NULL CHECK (assessment_contract_version > 0),
  coverage_prompt_contract_sha256 bytea NOT NULL
    CHECK (octet_length(coverage_prompt_contract_sha256) = 32),
  assessment_prompt_contract_sha256 bytea NOT NULL
    CHECK (octet_length(assessment_prompt_contract_sha256) = 32),
  binding_id uuid NOT NULL,
  binding_version bigint NOT NULL CHECK (binding_version > 0),

  coverage_model_call_id uuid,
  coverage_disclosure_id uuid,
  coverage_intent_sha256 bytea CHECK (
    coverage_intent_sha256 IS NULL OR octet_length(coverage_intent_sha256) = 32
  ),
  coverage_probe_sha256 bytea CHECK (
    coverage_probe_sha256 IS NULL OR octet_length(coverage_probe_sha256) = 32
  ),
  coverage_snapshot_id uuid,
  coverage_version integer CHECK (coverage_version IS NULL OR coverage_version > 0),
  coverage_summaries_canonical bytea CHECK (
    coverage_summaries_canonical IS NULL OR octet_length(coverage_summaries_canonical) > 0
  ),
  coverage_digest_sha256 bytea CHECK (
    coverage_digest_sha256 IS NULL OR octet_length(coverage_digest_sha256) = 32
  ),
  coverage_scan_receipt jsonb CHECK (
    coverage_scan_receipt IS NULL OR jsonb_typeof(coverage_scan_receipt) = 'object'
  ),
  coverage_scan_receipt_sha256 bytea CHECK (
    coverage_scan_receipt_sha256 IS NULL OR octet_length(coverage_scan_receipt_sha256) = 32
  ),
  coverage_scan_disposition text CHECK (
    coverage_scan_disposition IS NULL OR coverage_scan_disposition IN ('PASS','REJECT')
  ),
  coverage_provider_trace text CHECK (
    coverage_provider_trace IS NULL OR btrim(coverage_provider_trace) <> ''
  ),

  assessment_model_call_id uuid,
  assessment_disclosure_id uuid,
  assessment_intent_sha256 bytea CHECK (
    assessment_intent_sha256 IS NULL OR octet_length(assessment_intent_sha256) = 32
  ),
  assessment_output_canonical bytea CHECK (
    assessment_output_canonical IS NULL OR octet_length(assessment_output_canonical) > 0
  ),
  assessment_output_sha256 bytea CHECK (
    assessment_output_sha256 IS NULL OR octet_length(assessment_output_sha256) = 32
  ),
  novelty_gate text CHECK (novelty_gate IS NULL OR novelty_gate IN ('PASS','FAIL')),
  quality_gate text CHECK (quality_gate IS NULL OR quality_gate IN ('PASS','FAIL')),
  generality_gate text CHECK (generality_gate IS NULL OR generality_gate IN ('PASS','FAIL')),
  grounding_gate text CHECK (grounding_gate IS NULL OR grounding_gate IN ('PASS','FAIL')),
  candidate_body bytea CHECK (candidate_body IS NULL OR octet_length(candidate_body) > 0),
  candidate_sha256 bytea CHECK (
    candidate_sha256 IS NULL OR octet_length(candidate_sha256) = 32
  ),
  candidate_scan_receipt jsonb CHECK (
    candidate_scan_receipt IS NULL OR jsonb_typeof(candidate_scan_receipt) = 'object'
  ),
  candidate_scan_receipt_sha256 bytea CHECK (
    candidate_scan_receipt_sha256 IS NULL OR octet_length(candidate_scan_receipt_sha256) = 32
  ),
  candidate_scan_disposition text CHECK (
    candidate_scan_disposition IS NULL OR candidate_scan_disposition IN ('PASS','REJECT')
  ),
  assessment_provider_trace text CHECK (
    assessment_provider_trace IS NULL OR btrim(assessment_provider_trace) <> ''
  ),

  terminal_call_kind text CHECK (
    terminal_call_kind IS NULL OR terminal_call_kind IN ('COVERAGE_PROBE','TYPED_ASSESSMENT')
  ),
  terminal_recorded_at timestamptz,

  CONSTRAINT contribution_executions_tenant_execution_unique UNIQUE (tenant_id, execution_id),
  CONSTRAINT contribution_executions_tenant_enqueue_unique
    UNIQUE (tenant_id, enqueue_idempotency_key),
  CONSTRAINT contribution_executions_coverage_request_unique
    UNIQUE (tenant_id, coverage_request_id),
  CONSTRAINT contribution_executions_assessment_request_unique
    UNIQUE (tenant_id, assessment_request_id),
  CONSTRAINT contribution_executions_candidate_unique UNIQUE (tenant_id, candidate_id),
  CONSTRAINT contribution_executions_distinct_request_ids
    CHECK (coverage_request_id <> assessment_request_id),
  CONSTRAINT contribution_executions_membership_fk
    FOREIGN KEY (tenant_id, user_id) REFERENCES control.memberships(tenant_id, user_id),
  CONSTRAINT contribution_executions_domain_fk
    FOREIGN KEY (tenant_id, reasoning_domain_id)
    REFERENCES control.private_reasoning_domains(tenant_id, reasoning_domain_id),
  CONSTRAINT contribution_executions_policy_fk
    FOREIGN KEY (tenant_id, policy_id, policy_version)
    REFERENCES control.contribution_policies(tenant_id, policy_id, policy_version),
  CONSTRAINT contribution_executions_binding_fk
    FOREIGN KEY (tenant_id, binding_id, binding_version)
    REFERENCES control.reasoning_route_bindings(tenant_id, binding_id, binding_version),
  CONSTRAINT contribution_executions_coverage_reservation_pair CHECK (
    num_nonnulls(coverage_model_call_id, coverage_disclosure_id, coverage_intent_sha256) IN (0,3)
  ),
  CONSTRAINT contribution_executions_assessment_reservation_pair CHECK (
    num_nonnulls(assessment_model_call_id, assessment_disclosure_id, assessment_intent_sha256) IN (0,3)
  ),
  CONSTRAINT contribution_executions_coverage_receipt_shape CHECK (
    num_nonnulls(
      coverage_probe_sha256, coverage_snapshot_id, coverage_version,
      coverage_summaries_canonical, coverage_digest_sha256,
      coverage_scan_receipt, coverage_scan_receipt_sha256,
      coverage_scan_disposition, coverage_provider_trace
    ) IN (0,4,9)
  ),
  CONSTRAINT contribution_executions_assessment_core_shape CHECK (
    num_nonnulls(
      assessment_output_canonical, assessment_output_sha256,
      novelty_gate, quality_gate, generality_gate, grounding_gate,
      assessment_provider_trace
    ) IN (0,7)
  ),
  CONSTRAINT contribution_executions_candidate_receipt_shape CHECK (
    num_nonnulls(
      candidate_body, candidate_sha256, candidate_scan_receipt,
      candidate_scan_receipt_sha256, candidate_scan_disposition
    ) IN (0,5)
  ),
  CONSTRAINT contribution_executions_terminal_pair CHECK (
    (terminal_call_kind IS NULL) = (terminal_recorded_at IS NULL)
  ),
  CONSTRAINT contribution_executions_coverage_model_call_fk
    FOREIGN KEY (tenant_id, coverage_model_call_id)
    REFERENCES ops.model_call_ledger(tenant_id, model_call_id),
  CONSTRAINT contribution_executions_assessment_model_call_fk
    FOREIGN KEY (tenant_id, assessment_model_call_id)
    REFERENCES ops.model_call_ledger(tenant_id, model_call_id),
  CONSTRAINT contribution_executions_coverage_disclosure_fk
    FOREIGN KEY (tenant_id, coverage_disclosure_id)
    REFERENCES ops.data_disclosures(tenant_id, disclosure_id),
  CONSTRAINT contribution_executions_assessment_disclosure_fk
    FOREIGN KEY (tenant_id, assessment_disclosure_id)
    REFERENCES ops.data_disclosures(tenant_id, disclosure_id)
);

CREATE TABLE private.contribution_execution_sources (
  tenant_id uuid NOT NULL,
  execution_id uuid NOT NULL,
  ordinal integer NOT NULL CHECK (ordinal >= 0),
  evidence_id uuid,
  memory_id uuid,
  source_hash bytea NOT NULL CHECK (octet_length(source_hash) = 32),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (execution_id, ordinal),
  CONSTRAINT contribution_execution_sources_exact_one CHECK (
    (evidence_id IS NULL) <> (memory_id IS NULL)
  ),
  CONSTRAINT contribution_execution_sources_execution_fk
    FOREIGN KEY (tenant_id, execution_id)
    REFERENCES private.contribution_executions(tenant_id, execution_id),
  CONSTRAINT contribution_execution_sources_evidence_fk
    FOREIGN KEY (tenant_id, evidence_id)
    REFERENCES private.evidence_objects(tenant_id, evidence_id),
  CONSTRAINT contribution_execution_sources_memory_fk
    FOREIGN KEY (tenant_id, memory_id)
    REFERENCES private.memory_records(tenant_id, memory_id)
);
CREATE UNIQUE INDEX contribution_execution_source_evidence_once
  ON private.contribution_execution_sources(execution_id, evidence_id)
  WHERE evidence_id IS NOT NULL;
CREATE UNIQUE INDEX contribution_execution_source_memory_once
  ON private.contribution_execution_sources(execution_id, memory_id)
  WHERE memory_id IS NOT NULL;

CREATE TABLE ops.contribution_execution_job_links (
  job_id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  execution_id uuid NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT contribution_execution_job_links_tenant_job_unique UNIQUE (tenant_id, job_id),
  CONSTRAINT contribution_execution_job_links_job_fk
    FOREIGN KEY (tenant_id, job_id) REFERENCES ops.jobs(tenant_id, job_id),
  CONSTRAINT contribution_execution_job_links_execution_fk
    FOREIGN KEY (tenant_id, execution_id)
    REFERENCES private.contribution_executions(tenant_id, execution_id)
);

ALTER TABLE staging.contribution_candidates
  ADD COLUMN contribution_execution_id uuid,
  ADD CONSTRAINT contribution_candidates_execution_unique UNIQUE (contribution_execution_id),
  ADD CONSTRAINT contribution_candidates_execution_fk
    FOREIGN KEY (tenant_id, contribution_execution_id)
    REFERENCES private.contribution_executions(tenant_id, execution_id);

-- R4 intentionally retains only the probe digest after the reconstructible coverage snapshot
-- is durable. Existing rows keep their bytes; execution-linked candidates bind the digest.
ALTER TABLE staging.contribution_candidate_phase9_assessments
  DROP CONSTRAINT contribution_candidate_phase9_assessments_probe_bytes_check,
  ALTER COLUMN probe_bytes DROP NOT NULL,
  ADD CONSTRAINT phase9_assessments_probe_bytes_or_digest CHECK (
    probe_bytes IS NULL OR (
      octet_length(probe_bytes) BETWEEN 1 AND 4096
      AND probe_sha256 = sha256(probe_bytes)
    )
  );

CREATE FUNCTION private.contribution_execution_guard_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
  ledger_status text;
  disclosure_outcome text;
BEGIN
  IF TG_OP IN ('DELETE','TRUNCATE') THEN
    RAISE EXCEPTION 'contribution executions are retained; % is forbidden', TG_OP
      USING ERRCODE = '55000';
  END IF;
  IF TG_OP = 'INSERT' THEN
    IF NEW.state <> 'READY_A'
       OR num_nonnulls(
         NEW.coverage_model_call_id, NEW.coverage_disclosure_id,
         NEW.assessment_model_call_id, NEW.assessment_disclosure_id,
         NEW.terminal_call_kind
       ) <> 0 THEN
      RAISE EXCEPTION 'new contribution execution must be a pristine READY_A root'
        USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
  END IF;

  IF ROW(
    OLD.execution_id, OLD.tenant_id, OLD.user_id, OLD.created_at,
    OLD.enqueue_idempotency_key, OLD.enqueue_fingerprint,
    OLD.reasoning_domain_id, OLD.input_manifest_hash, OLD.source_count,
    OLD.policy_id, OLD.policy_version, OLD.policy_snapshot,
    OLD.rights_basis, OLD.source_license, OLD.publisher,
    OLD.contributor_attestation, OLD.redistribution_policy,
    OLD.coverage_request_id, OLD.assessment_request_id, OLD.candidate_id,
    OLD.coverage_contract_version, OLD.assessment_contract_version,
    OLD.coverage_prompt_contract_sha256, OLD.assessment_prompt_contract_sha256,
    OLD.binding_id, OLD.binding_version
  ) IS DISTINCT FROM ROW(
    NEW.execution_id, NEW.tenant_id, NEW.user_id, NEW.created_at,
    NEW.enqueue_idempotency_key, NEW.enqueue_fingerprint,
    NEW.reasoning_domain_id, NEW.input_manifest_hash, NEW.source_count,
    NEW.policy_id, NEW.policy_version, NEW.policy_snapshot,
    NEW.rights_basis, NEW.source_license, NEW.publisher,
    NEW.contributor_attestation, NEW.redistribution_policy,
    NEW.coverage_request_id, NEW.assessment_request_id, NEW.candidate_id,
    NEW.coverage_contract_version, NEW.assessment_contract_version,
    NEW.coverage_prompt_contract_sha256, NEW.assessment_prompt_contract_sha256,
    NEW.binding_id, NEW.binding_version
  ) THEN
    RAISE EXCEPTION 'contribution execution identity and frozen inputs are immutable'
      USING ERRCODE = '55000';
  END IF;

  IF (OLD.coverage_model_call_id IS NOT NULL AND ROW(
      OLD.coverage_model_call_id, OLD.coverage_disclosure_id, OLD.coverage_intent_sha256
    ) IS DISTINCT FROM ROW(
      NEW.coverage_model_call_id, NEW.coverage_disclosure_id, NEW.coverage_intent_sha256
    ))
    OR (OLD.assessment_model_call_id IS NOT NULL AND ROW(
      OLD.assessment_model_call_id, OLD.assessment_disclosure_id, OLD.assessment_intent_sha256
    ) IS DISTINCT FROM ROW(
      NEW.assessment_model_call_id, NEW.assessment_disclosure_id, NEW.assessment_intent_sha256
    ))
  THEN
    RAISE EXCEPTION 'contribution execution reservations are immutable once set'
      USING ERRCODE = '55000';
  END IF;

  IF (OLD.coverage_probe_sha256 IS NOT NULL AND ROW(
      OLD.coverage_probe_sha256, OLD.coverage_snapshot_id, OLD.coverage_version,
      OLD.coverage_summaries_canonical, OLD.coverage_digest_sha256,
      OLD.coverage_scan_receipt, OLD.coverage_scan_receipt_sha256,
      OLD.coverage_scan_disposition, OLD.coverage_provider_trace
    ) IS DISTINCT FROM ROW(
      NEW.coverage_probe_sha256, NEW.coverage_snapshot_id, NEW.coverage_version,
      NEW.coverage_summaries_canonical, NEW.coverage_digest_sha256,
      NEW.coverage_scan_receipt, NEW.coverage_scan_receipt_sha256,
      NEW.coverage_scan_disposition, NEW.coverage_provider_trace
    ))
    OR (OLD.assessment_output_canonical IS NOT NULL AND ROW(
      OLD.assessment_output_canonical, OLD.assessment_output_sha256,
      OLD.novelty_gate, OLD.quality_gate, OLD.generality_gate, OLD.grounding_gate,
      OLD.candidate_body, OLD.candidate_sha256, OLD.candidate_scan_receipt,
      OLD.candidate_scan_receipt_sha256, OLD.candidate_scan_disposition,
      OLD.assessment_provider_trace
    ) IS DISTINCT FROM ROW(
      NEW.assessment_output_canonical, NEW.assessment_output_sha256,
      NEW.novelty_gate, NEW.quality_gate, NEW.generality_gate, NEW.grounding_gate,
      NEW.candidate_body, NEW.candidate_sha256, NEW.candidate_scan_receipt,
      NEW.candidate_scan_receipt_sha256, NEW.candidate_scan_disposition,
      NEW.assessment_provider_trace
    ))
    OR (OLD.terminal_recorded_at IS NOT NULL AND ROW(
      OLD.terminal_call_kind, OLD.terminal_recorded_at
    ) IS DISTINCT FROM ROW(
      NEW.terminal_call_kind, NEW.terminal_recorded_at
    ))
  THEN
    RAISE EXCEPTION 'contribution execution completion receipts are immutable once set'
      USING ERRCODE = '55000';
  END IF;

  IF NOT (
    (OLD.state = 'READY_A' AND NEW.state = 'A_RESERVED')
    OR (OLD.state = 'A_RESERVED' AND NEW.state IN ('READY_B','REJECTED_SAFETY','FAILED_TERMINAL'))
    OR (OLD.state = 'READY_B' AND NEW.state = 'B_RESERVED')
    OR (OLD.state = 'B_RESERVED' AND NEW.state IN (
      'READY_CANDIDATE','NOT_CONTRIBUTABLE','REJECTED_SAFETY','FAILED_TERMINAL'
    ))
    OR (OLD.state = 'READY_CANDIDATE' AND NEW.state = 'DONE')
  ) THEN
    RAISE EXCEPTION 'illegal contribution execution transition % -> %', OLD.state, NEW.state
      USING ERRCODE = '23514';
  END IF;

  IF NEW.state = 'A_RESERVED' THEN
    SELECT status INTO ledger_status FROM ops.model_call_ledger
      WHERE tenant_id=NEW.tenant_id AND model_call_id=NEW.coverage_model_call_id
        AND request_id=NEW.coverage_request_id AND call_kind='COVERAGE_PROBE'
        AND intent_sha256=NEW.coverage_intent_sha256;
    SELECT outcome INTO disclosure_outcome FROM ops.data_disclosures
      WHERE tenant_id=NEW.tenant_id AND disclosure_id=NEW.coverage_disclosure_id
        AND model_call_id=NEW.coverage_model_call_id AND finalized_at IS NULL;
    IF ledger_status IS DISTINCT FROM 'RESERVED' OR disclosure_outcome IS NOT NULL THEN
      RAISE EXCEPTION 'A reserve requires exact RESERVED ledger and open disclosure'
        USING ERRCODE = '23514';
    END IF;
  ELSIF NEW.state = 'READY_B' THEN
    SELECT status INTO ledger_status FROM ops.model_call_ledger
      WHERE tenant_id=NEW.tenant_id AND model_call_id=NEW.coverage_model_call_id
        AND request_id=NEW.coverage_request_id AND call_kind='COVERAGE_PROBE'
        AND intent_sha256=NEW.coverage_intent_sha256;
    SELECT outcome INTO disclosure_outcome FROM ops.data_disclosures
      WHERE tenant_id=NEW.tenant_id AND disclosure_id=NEW.coverage_disclosure_id
        AND model_call_id=NEW.coverage_model_call_id AND finalized_at IS NOT NULL;
    IF ledger_status IS DISTINCT FROM 'SUCCEEDED'
       OR disclosure_outcome IS DISTINCT FROM 'SUCCESS'
       OR NEW.coverage_scan_disposition <> 'PASS'
       OR num_nonnulls(
         NEW.coverage_probe_sha256, NEW.coverage_snapshot_id, NEW.coverage_version,
         NEW.coverage_summaries_canonical, NEW.coverage_digest_sha256,
         NEW.coverage_scan_receipt, NEW.coverage_scan_receipt_sha256,
         NEW.coverage_provider_trace
       ) <> 8 THEN
      RAISE EXCEPTION 'READY_B requires one complete usable A bundle'
        USING ERRCODE = '23514';
    END IF;
  ELSIF NEW.state = 'B_RESERVED' THEN
    SELECT status INTO ledger_status FROM ops.model_call_ledger
      WHERE tenant_id=NEW.tenant_id AND model_call_id=NEW.assessment_model_call_id
        AND request_id=NEW.assessment_request_id AND call_kind='TYPED_ASSESSMENT'
        AND intent_sha256=NEW.assessment_intent_sha256;
    SELECT outcome INTO disclosure_outcome FROM ops.data_disclosures
      WHERE tenant_id=NEW.tenant_id AND disclosure_id=NEW.assessment_disclosure_id
        AND model_call_id=NEW.assessment_model_call_id AND finalized_at IS NULL;
    IF ledger_status IS DISTINCT FROM 'RESERVED' OR disclosure_outcome IS NOT NULL THEN
      RAISE EXCEPTION 'B reserve requires exact RESERVED ledger and open disclosure'
        USING ERRCODE = '23514';
    END IF;
  ELSIF NEW.state IN ('READY_CANDIDATE','NOT_CONTRIBUTABLE') THEN
    SELECT status INTO ledger_status FROM ops.model_call_ledger
      WHERE tenant_id=NEW.tenant_id AND model_call_id=NEW.assessment_model_call_id
        AND request_id=NEW.assessment_request_id AND call_kind='TYPED_ASSESSMENT'
        AND intent_sha256=NEW.assessment_intent_sha256;
    SELECT outcome INTO disclosure_outcome FROM ops.data_disclosures
      WHERE tenant_id=NEW.tenant_id AND disclosure_id=NEW.assessment_disclosure_id
        AND model_call_id=NEW.assessment_model_call_id AND finalized_at IS NOT NULL;
    IF ledger_status IS DISTINCT FROM 'SUCCEEDED'
       OR disclosure_outcome IS DISTINCT FROM 'SUCCESS'
       OR num_nonnulls(
         NEW.assessment_output_canonical, NEW.assessment_output_sha256,
         NEW.novelty_gate, NEW.quality_gate, NEW.generality_gate, NEW.grounding_gate,
         NEW.assessment_provider_trace
       ) <> 7 THEN
      RAISE EXCEPTION 'B business completion requires one complete successful attempt bundle'
        USING ERRCODE = '23514';
    END IF;
    IF NEW.state = 'READY_CANDIDATE' AND (
      ROW(NEW.novelty_gate,NEW.quality_gate,NEW.generality_gate,NEW.grounding_gate)
        IS DISTINCT FROM ROW('PASS'::text,'PASS'::text,'PASS'::text,'PASS'::text)
      OR NEW.candidate_scan_disposition <> 'PASS'
      OR num_nonnulls(
        NEW.candidate_body, NEW.candidate_sha256, NEW.candidate_scan_receipt,
        NEW.candidate_scan_receipt_sha256
      ) <> 4
    ) THEN
      RAISE EXCEPTION 'READY_CANDIDATE requires all PASS gates and complete PASS scan receipt'
        USING ERRCODE = '23514';
    END IF;
    IF NEW.state = 'NOT_CONTRIBUTABLE' AND NOT (
      NEW.novelty_gate='FAIL' OR NEW.quality_gate='FAIL'
      OR NEW.generality_gate='FAIL' OR NEW.grounding_gate='FAIL'
    ) THEN
      RAISE EXCEPTION 'NOT_CONTRIBUTABLE requires at least one raw FAIL gate'
        USING ERRCODE = '23514';
    END IF;
  ELSIF NEW.state = 'REJECTED_SAFETY' THEN
    IF NEW.terminal_call_kind='COVERAGE_PROBE' THEN
      SELECT status INTO ledger_status FROM ops.model_call_ledger
        WHERE tenant_id=NEW.tenant_id AND model_call_id=NEW.coverage_model_call_id;
      SELECT outcome INTO disclosure_outcome FROM ops.data_disclosures
        WHERE tenant_id=NEW.tenant_id AND disclosure_id=NEW.coverage_disclosure_id;
      IF NEW.coverage_scan_disposition <> 'REJECT' THEN
        RAISE EXCEPTION 'A safety rejection requires a durable REJECT receipt' USING ERRCODE='23514';
      END IF;
    ELSE
      SELECT status INTO ledger_status FROM ops.model_call_ledger
        WHERE tenant_id=NEW.tenant_id AND model_call_id=NEW.assessment_model_call_id;
      SELECT outcome INTO disclosure_outcome FROM ops.data_disclosures
        WHERE tenant_id=NEW.tenant_id AND disclosure_id=NEW.assessment_disclosure_id;
      IF NEW.candidate_scan_disposition <> 'REJECT' THEN
        RAISE EXCEPTION 'B safety rejection requires a durable REJECT receipt' USING ERRCODE='23514';
      END IF;
    END IF;
    IF ledger_status IS DISTINCT FROM 'SUCCEEDED' OR disclosure_outcome IS DISTINCT FROM 'SUCCESS' THEN
      RAISE EXCEPTION 'safety rejection is a successful provider attempt' USING ERRCODE='23514';
    END IF;
  ELSIF NEW.state = 'FAILED_TERMINAL' THEN
    IF NEW.terminal_call_kind='COVERAGE_PROBE' THEN
      SELECT status INTO ledger_status FROM ops.model_call_ledger
        WHERE tenant_id=NEW.tenant_id AND model_call_id=NEW.coverage_model_call_id;
      SELECT outcome INTO disclosure_outcome FROM ops.data_disclosures
        WHERE tenant_id=NEW.tenant_id AND disclosure_id=NEW.coverage_disclosure_id;
    ELSE
      SELECT status INTO ledger_status FROM ops.model_call_ledger
        WHERE tenant_id=NEW.tenant_id AND model_call_id=NEW.assessment_model_call_id;
      SELECT outcome INTO disclosure_outcome FROM ops.data_disclosures
        WHERE tenant_id=NEW.tenant_id AND disclosure_id=NEW.assessment_disclosure_id;
    END IF;
    IF ledger_status IS DISTINCT FROM 'FAILED' OR disclosure_outcome IS DISTINCT FROM 'FAILED' THEN
      RAISE EXCEPTION 'FAILED_TERMINAL requires exact failed ledger and disclosure'
        USING ERRCODE='23514';
    END IF;
  ELSIF NEW.state = 'DONE' AND NOT EXISTS (
    SELECT 1 FROM staging.contribution_candidates c
    WHERE c.tenant_id=NEW.tenant_id AND c.candidate_id=NEW.candidate_id
      AND c.contribution_execution_id=NEW.execution_id
      AND c.model_call_id=NEW.assessment_model_call_id
  ) THEN
    RAISE EXCEPTION 'DONE requires the exact pre-minted execution candidate'
      USING ERRCODE='23514';
  END IF;

  NEW.updated_at := clock_timestamp();
  RETURN NEW;
END;
$$;

CREATE TRIGGER contribution_execution_guard
  BEFORE INSERT OR UPDATE OR DELETE ON private.contribution_executions
  FOR EACH ROW EXECUTE FUNCTION private.contribution_execution_guard_mutation();
CREATE TRIGGER contribution_execution_reject_truncate
  BEFORE TRUNCATE ON private.contribution_executions
  FOR EACH STATEMENT EXECUTE FUNCTION private.contribution_execution_guard_mutation();

CREATE FUNCTION private.contribution_execution_relation_append_only()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
  IF TG_OP <> 'INSERT' THEN
    RAISE EXCEPTION '% is append-only', TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME
      USING ERRCODE='55000';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER contribution_execution_sources_append_only
  BEFORE UPDATE OR DELETE ON private.contribution_execution_sources
  FOR EACH ROW EXECUTE FUNCTION private.contribution_execution_relation_append_only();
CREATE TRIGGER contribution_execution_sources_reject_truncate
  BEFORE TRUNCATE ON private.contribution_execution_sources
  FOR EACH STATEMENT EXECUTE FUNCTION private.contribution_execution_relation_append_only();
CREATE TRIGGER contribution_execution_job_links_append_only
  BEFORE UPDATE OR DELETE ON ops.contribution_execution_job_links
  FOR EACH ROW EXECUTE FUNCTION private.contribution_execution_relation_append_only();
CREATE TRIGGER contribution_execution_job_links_reject_truncate
  BEFORE TRUNCATE ON ops.contribution_execution_job_links
  FOR EACH STATEMENT EXECUTE FUNCTION private.contribution_execution_relation_append_only();

CREATE FUNCTION ops.contribution_execution_job_link_validate()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE j ops.jobs%ROWTYPE;
BEGIN
  SELECT * INTO j FROM ops.jobs WHERE job_id=NEW.job_id FOR KEY SHARE;
  IF NOT FOUND OR j.tenant_id<>NEW.tenant_id OR j.job_type<>'CONTRIBUTION_EXECUTE'
     OR j.payload IS DISTINCT FROM jsonb_build_object(
       'schema_version',1,'execution_id',NEW.execution_id::text
     ) THEN
    RAISE EXCEPTION 'contribution execution job link requires exact typed job payload'
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER contribution_execution_job_link_validate
  BEFORE INSERT ON ops.contribution_execution_job_links
  FOR EACH ROW EXECUTE FUNCTION ops.contribution_execution_job_link_validate();

CREATE FUNCTION private.require_contribution_execution_lease(
  p_tenant_id uuid,
  p_execution_id uuid,
  p_job_id uuid,
  p_lease_owner text,
  p_attempt integer
) RETURNS void
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  IF p_tenant_id IS NULL OR p_execution_id IS NULL OR p_job_id IS NULL
     OR p_lease_owner IS NULL OR btrim(p_lease_owner)=''
     OR p_attempt IS NULL OR p_attempt <= 0 THEN
    RAISE EXCEPTION 'fresh contribution lease identity is required' USING ERRCODE='22023';
  END IF;
  PERFORM set_config('humaux.tenant_id',p_tenant_id::text,true);
  PERFORM 1
  FROM ops.jobs j
  JOIN ops.contribution_execution_job_links link
    ON (link.tenant_id,link.job_id)=(j.tenant_id,j.job_id)
  WHERE j.tenant_id=p_tenant_id AND j.job_id=p_job_id
    AND link.execution_id=p_execution_id
    AND j.job_type='CONTRIBUTION_EXECUTE'
    AND j.status='PROCESSING'
    AND j.lease_owner=p_lease_owner
    AND j.attempt=p_attempt
    AND j.lease_expires_at > clock_timestamp()
  FOR UPDATE OF j;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'fresh contribution job lease required' USING ERRCODE='55000';
  END IF;
END;
$$;

CREATE FUNCTION private.require_contribution_source_manifest()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE root private.contribution_executions%ROWTYPE;
DECLARE actual_count bigint; actual_hash bytea; target_execution_id uuid;
BEGIN
  target_execution_id:=NEW.execution_id;
  SELECT * INTO root FROM private.contribution_executions
    WHERE execution_id=target_execution_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'execution source manifest has no durable root' USING ERRCODE='23503';
  END IF;
  SELECT count(*), sha256(convert_to(string_agg(
    (CASE WHEN evidence_id IS NOT NULL THEN 'e' ELSE 'm' END)||':'||
    coalesce(evidence_id,memory_id)::text||':'||encode(source_hash,'hex'),
    '|' ORDER BY (CASE WHEN evidence_id IS NOT NULL THEN 'e' ELSE 'm' END),
      coalesce(evidence_id,memory_id)
  ),'UTF8'))
  INTO actual_count,actual_hash
  FROM private.contribution_execution_sources
  WHERE execution_id=target_execution_id;
  IF actual_count<>root.source_count OR actual_hash IS DISTINCT FROM root.input_manifest_hash THEN
    RAISE EXCEPTION 'execution sources must equal the frozen canonical input manifest'
      USING ERRCODE='23514';
  END IF;
  RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER contribution_execution_manifest_complete
  AFTER INSERT ON private.contribution_executions
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION private.require_contribution_source_manifest();
CREATE CONSTRAINT TRIGGER contribution_execution_sources_complete
  AFTER INSERT ON private.contribution_execution_sources
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION private.require_contribution_source_manifest();

CREATE FUNCTION private.enqueue_contribution_execution(
  p_tenant_id uuid,
  p_execution_id uuid,
  p_job_id uuid,
  p_coverage_request_id uuid,
  p_assessment_request_id uuid,
  p_candidate_id uuid,
  p_enqueue_idempotency_key text,
  p_enqueue_fingerprint bytea,
  p_user_id uuid,
  p_reasoning_domain_id uuid,
  p_input_manifest_hash bytea,
  p_policy_id uuid,
  p_policy_version bigint,
  p_policy_snapshot jsonb,
  p_rights_basis text,
  p_source_license text,
  p_publisher text,
  p_contributor_attestation text,
  p_redistribution_policy text,
  p_binding_id uuid,
  p_binding_version bigint,
  p_coverage_contract_version bigint,
  p_assessment_contract_version bigint,
  p_coverage_prompt_contract_sha256 bytea,
  p_assessment_prompt_contract_sha256 bytea,
  p_source_kinds text[],
  p_source_ids uuid[],
  p_source_hashes bytea[]
) RETURNS TABLE(
  execution_id uuid,
  job_id uuid,
  coverage_request_id uuid,
  assessment_request_id uuid,
  candidate_id uuid,
  created boolean
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE existing_job ops.jobs%ROWTYPE; existing_execution private.contribution_executions%ROWTYPE;
DECLARE source_len integer; i integer;
BEGIN
  PERFORM set_config('humaux.tenant_id',p_tenant_id::text,true);
  IF p_execution_id IS NULL OR p_job_id IS NULL OR p_coverage_request_id IS NULL
     OR p_assessment_request_id IS NULL OR p_candidate_id IS NULL
     OR p_coverage_request_id=p_assessment_request_id
     OR octet_length(p_enqueue_fingerprint)<>32
     OR octet_length(p_input_manifest_hash)<>32
     OR p_enqueue_idempotency_key IS NULL OR btrim(p_enqueue_idempotency_key)=''
  THEN
    RAISE EXCEPTION 'invalid contribution execution identities or fingerprint'
      USING ERRCODE='22023';
  END IF;
  source_len:=coalesce(array_length(p_source_ids,1),0);
  IF source_len=0 OR source_len IS DISTINCT FROM array_length(p_source_kinds,1)
     OR source_len IS DISTINCT FROM array_length(p_source_hashes,1) THEN
    RAISE EXCEPTION 'typed contribution sources must be non-empty parallel arrays'
      USING ERRCODE='22023';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtextextended(
    'contribution-enqueue:'||p_enqueue_idempotency_key,0
  ));
  SELECT * INTO existing_job FROM ops.jobs
    WHERE idempotency_key=p_enqueue_idempotency_key FOR UPDATE;
  IF FOUND THEN
    SELECT root.* INTO existing_execution
    FROM ops.contribution_execution_job_links link
    JOIN private.contribution_executions root
      ON (root.tenant_id,root.execution_id)=(link.tenant_id,link.execution_id)
    WHERE link.job_id=existing_job.job_id FOR UPDATE OF root;
    IF NOT FOUND OR existing_job.tenant_id<>p_tenant_id
       OR existing_job.job_type<>'CONTRIBUTION_EXECUTE'
       OR existing_execution.enqueue_idempotency_key<>p_enqueue_idempotency_key THEN
      RAISE EXCEPTION 'idempotency key resolves to an invalid contribution execution chain'
        USING ERRCODE='23514';
    END IF;
    IF existing_execution.enqueue_fingerprint IS DISTINCT FROM p_enqueue_fingerprint THEN
      RAISE EXCEPTION 'contribution enqueue idempotency fingerprint conflict'
        USING ERRCODE='23505';
    END IF;
    RETURN QUERY SELECT existing_execution.execution_id,existing_job.job_id,
      existing_execution.coverage_request_id,existing_execution.assessment_request_id,
      existing_execution.candidate_id,false;
    RETURN;
  END IF;

  INSERT INTO ops.jobs(
    job_id,tenant_id,job_type,status,next_retry_at,idempotency_key,payload
  ) VALUES (
    p_job_id,p_tenant_id,'CONTRIBUTION_EXECUTE','PENDING',clock_timestamp(),
    p_enqueue_idempotency_key,
    jsonb_build_object('schema_version',1,'execution_id',p_execution_id::text)
  );
  INSERT INTO private.contribution_executions(
    execution_id,tenant_id,user_id,enqueue_idempotency_key,enqueue_fingerprint,
    reasoning_domain_id,input_manifest_hash,source_count,policy_id,policy_version,
    policy_snapshot,rights_basis,source_license,publisher,contributor_attestation,
    redistribution_policy,coverage_request_id,assessment_request_id,candidate_id,
    coverage_contract_version,assessment_contract_version,
    coverage_prompt_contract_sha256,assessment_prompt_contract_sha256,
    binding_id,binding_version
  ) VALUES (
    p_execution_id,p_tenant_id,p_user_id,p_enqueue_idempotency_key,p_enqueue_fingerprint,
    p_reasoning_domain_id,p_input_manifest_hash,source_len,p_policy_id,p_policy_version,
    p_policy_snapshot,p_rights_basis,p_source_license,p_publisher,p_contributor_attestation,
    p_redistribution_policy,p_coverage_request_id,p_assessment_request_id,p_candidate_id,
    p_coverage_contract_version,p_assessment_contract_version,
    p_coverage_prompt_contract_sha256,p_assessment_prompt_contract_sha256,
    p_binding_id,p_binding_version
  );
  FOR i IN 1..source_len LOOP
    IF p_source_kinds[i] NOT IN ('EVIDENCE','MEMORY')
       OR octet_length(p_source_hashes[i])<>32 THEN
      RAISE EXCEPTION 'invalid typed contribution source' USING ERRCODE='22023';
    END IF;
    INSERT INTO private.contribution_execution_sources(
      tenant_id,execution_id,ordinal,evidence_id,memory_id,source_hash
    ) VALUES (
      p_tenant_id,p_execution_id,i-1,
      CASE WHEN p_source_kinds[i]='EVIDENCE' THEN p_source_ids[i] END,
      CASE WHEN p_source_kinds[i]='MEMORY' THEN p_source_ids[i] END,
      p_source_hashes[i]
    );
  END LOOP;
  INSERT INTO ops.contribution_execution_job_links(job_id,tenant_id,execution_id)
    VALUES(p_job_id,p_tenant_id,p_execution_id);
  RETURN QUERY SELECT p_execution_id,p_job_id,p_coverage_request_id,
    p_assessment_request_id,p_candidate_id,true;
END;
$$;

CREATE FUNCTION private.reserve_contribution_execution_call(
  p_tenant_id uuid,
  p_execution_id uuid,
  p_job_id uuid,
  p_lease_owner text,
  p_attempt integer,
  p_call_kind text,
  p_model_call_id uuid,
  p_disclosure_id uuid,
  p_intent_sha256 bytea,
  p_grant_id uuid,
  p_scope_kind text,
  p_scope_id uuid,
  p_data_class text,
  p_payload_sha256 bytea,
  p_payload_bytes bigint
) RETURNS TABLE(
  model_call_id uuid,
  disclosure_id uuid,
  request_id uuid,
  processor_id text,
  provider_model_id text,
  model_revision text,
  egress_processor_id uuid,
  credential_ref uuid,
  newly_reserved boolean
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE root private.contribution_executions%ROWTYPE; admission record;
DECLARE expected_state private.contribution_execution_state;
DECLARE inserted_source_count bigint;
BEGIN
  IF p_call_kind NOT IN ('COVERAGE_PROBE','TYPED_ASSESSMENT')
     OR octet_length(p_intent_sha256)<>32 OR octet_length(p_payload_sha256)<>32
     OR p_payload_bytes<0 OR p_model_call_id IS NULL OR p_disclosure_id IS NULL THEN
    RAISE EXCEPTION 'invalid contribution call reservation' USING ERRCODE='22023';
  END IF;
  PERFORM private.require_contribution_execution_lease(
    p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt
  );
  SELECT * INTO root FROM private.contribution_executions
    WHERE tenant_id=p_tenant_id AND execution_id=p_execution_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'contribution execution not found' USING ERRCODE='23503'; END IF;
  expected_state:=CASE p_call_kind WHEN 'COVERAGE_PROBE' THEN 'READY_A' ELSE 'READY_B' END;
  IF root.state IN ('A_RESERVED','B_RESERVED') THEN
    IF (p_call_kind='COVERAGE_PROBE' AND root.state='A_RESERVED'
        AND root.coverage_intent_sha256=p_intent_sha256)
       OR (p_call_kind='TYPED_ASSESSMENT' AND root.state='B_RESERVED'
        AND root.assessment_intent_sha256=p_intent_sha256) THEN
      UPDATE ops.jobs SET status='FAILED',last_error_class='RECONCILIATION_REQUIRED'
      WHERE tenant_id=p_tenant_id AND job_id=p_job_id AND status='PROCESSING'
        AND lease_owner=p_lease_owner AND attempt=p_attempt
        AND lease_expires_at>clock_timestamp();
      IF NOT FOUND THEN RAISE EXCEPTION 'fresh contribution job lease required' USING ERRCODE='55000'; END IF;
      RETURN QUERY SELECT
        CASE p_call_kind WHEN 'COVERAGE_PROBE' THEN root.coverage_model_call_id
          ELSE root.assessment_model_call_id END,
        CASE p_call_kind WHEN 'COVERAGE_PROBE' THEN root.coverage_disclosure_id
          ELSE root.assessment_disclosure_id END,
        CASE p_call_kind WHEN 'COVERAGE_PROBE' THEN root.coverage_request_id
          ELSE root.assessment_request_id END,
        call.provider,call.model,call.model_revision,call.egress_processor_id,
        call.credential_ref,false
      FROM ops.model_call_ledger call WHERE call.tenant_id=p_tenant_id
        AND call.model_call_id=CASE p_call_kind WHEN 'COVERAGE_PROBE'
          THEN root.coverage_model_call_id ELSE root.assessment_model_call_id END;
      RETURN;
    END IF;
    RAISE EXCEPTION 'reserved contribution call identity mismatch' USING ERRCODE='23514';
  END IF;
  IF root.state<>expected_state THEN
    RAISE EXCEPTION 'contribution call cannot reserve from state %',root.state USING ERRCODE='23514';
  END IF;

  SELECT * INTO admission FROM control.resolve_user_reasoning_admission(
    root.binding_id,root.binding_version,root.reasoning_domain_id,'CONTRIBUTION_DEIDENTIFY'
  );
  IF NOT FOUND OR admission.tenant_id<>p_tenant_id THEN
    RAISE EXCEPTION 'frozen Binding is not currently admissible' USING ERRCODE='55000';
  END IF;
  INSERT INTO ops.model_call_ledger(
    model_call_id,tenant_id,provider,purpose,request_id,model,model_revision,status,
    reasoning_domain_id,call_kind,intent_sha256,binding_id,binding_version,
    route_policy_id,route_policy_version,profile_id,profile_version,
    provider_account_id,provider_endpoint_id,egress_processor_id,credential_ref,
    billing_account_id,billing_instrument_id,provider_health_observation_id,
    account_health_observation_id,billing_responsibility,admitted_at
  ) VALUES (
    p_model_call_id,p_tenant_id,admission.processor_id,'CONTRIBUTION_DEIDENTIFY',
    CASE p_call_kind WHEN 'COVERAGE_PROBE' THEN root.coverage_request_id
      ELSE root.assessment_request_id END,
    admission.provider_model_id,admission.model_revision,'RESERVED',
    root.reasoning_domain_id,p_call_kind,p_intent_sha256,root.binding_id,root.binding_version,
    admission.route_policy_id,admission.route_policy_version,admission.profile_id,
    admission.profile_version,admission.provider_account_id,admission.provider_endpoint_id,
    admission.egress_processor_id,admission.credential_ref,admission.billing_account_id,
    admission.billing_instrument_id,admission.provider_health_observation_id,
    admission.account_health_observation_id,'USER',admission.admitted_at
  );
  INSERT INTO ops.data_disclosures(
    disclosure_id,grant_id,tenant_id,scope_kind,scope_id,processor_id,region,
    data_class,purpose,payload_sha256,payload_bytes,model_call_id
  ) VALUES (
    p_disclosure_id,p_grant_id,p_tenant_id,p_scope_kind,p_scope_id,
    admission.egress_processor_id,admission.region,p_data_class,'USER_REASONING',
    p_payload_sha256,p_payload_bytes,p_model_call_id
  );
  INSERT INTO ops.data_disclosure_sources(
    tenant_id,disclosure_id,source_kind,evidence_id,memory_id,ordinal
  ) SELECT
    p_tenant_id,p_disclosure_id,
    CASE WHEN source.evidence_id IS NOT NULL THEN 'EVIDENCE' ELSE 'MEMORY' END,
    source.evidence_id,source.memory_id,source.ordinal
  FROM private.contribution_execution_sources source
  WHERE source.tenant_id=p_tenant_id AND source.execution_id=p_execution_id
  ORDER BY source.ordinal;
  GET DIAGNOSTICS inserted_source_count = ROW_COUNT;
  IF inserted_source_count=0 OR inserted_source_count<>root.source_count THEN
    RAISE EXCEPTION 'contribution disclosure source count mismatch' USING ERRCODE='23514';
  END IF;
  IF p_call_kind='COVERAGE_PROBE' THEN
    UPDATE private.contribution_executions SET state='A_RESERVED',
      coverage_model_call_id=p_model_call_id,coverage_disclosure_id=p_disclosure_id,
      coverage_intent_sha256=p_intent_sha256
    WHERE execution_id=p_execution_id;
  ELSE
    UPDATE private.contribution_executions SET state='B_RESERVED',
      assessment_model_call_id=p_model_call_id,assessment_disclosure_id=p_disclosure_id,
      assessment_intent_sha256=p_intent_sha256
    WHERE execution_id=p_execution_id;
  END IF;
  RETURN QUERY SELECT p_model_call_id,p_disclosure_id,
    CASE p_call_kind WHEN 'COVERAGE_PROBE' THEN root.coverage_request_id
      ELSE root.assessment_request_id END,
    admission.processor_id,admission.provider_model_id,admission.model_revision,
    admission.egress_processor_id,admission.credential_ref,true;
END;
$$;

-- Public SQL surface is stage-specific. The shared implementation is owner-only so runtime
-- cannot smuggle a generic call_kind across the frozen A/B state graph.
CREATE FUNCTION private.reserve_contribution_a(
  p_tenant_id uuid,p_execution_id uuid,p_job_id uuid,p_lease_owner text,p_attempt integer,
  p_model_call_id uuid,p_disclosure_id uuid,p_intent_sha256 bytea,p_grant_id uuid,
  p_scope_kind text,p_scope_id uuid,p_data_class text,p_payload_sha256 bytea,p_payload_bytes bigint
) RETURNS TABLE(
  model_call_id uuid,disclosure_id uuid,request_id uuid,processor_id text,
  provider_model_id text,model_revision text,egress_processor_id uuid,
  credential_ref uuid,newly_reserved boolean
)
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
  SELECT * FROM private.reserve_contribution_execution_call(
    p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt,'COVERAGE_PROBE',
    p_model_call_id,p_disclosure_id,p_intent_sha256,p_grant_id,p_scope_kind,p_scope_id,
    p_data_class,p_payload_sha256,p_payload_bytes
  )
$$;

CREATE FUNCTION private.reserve_contribution_b(
  p_tenant_id uuid,p_execution_id uuid,p_job_id uuid,p_lease_owner text,p_attempt integer,
  p_model_call_id uuid,p_disclosure_id uuid,p_intent_sha256 bytea,p_grant_id uuid,
  p_scope_kind text,p_scope_id uuid,p_data_class text,p_payload_sha256 bytea,p_payload_bytes bigint
) RETURNS TABLE(
  model_call_id uuid,disclosure_id uuid,request_id uuid,processor_id text,
  provider_model_id text,model_revision text,egress_processor_id uuid,
  credential_ref uuid,newly_reserved boolean
)
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
  SELECT * FROM private.reserve_contribution_execution_call(
    p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt,'TYPED_ASSESSMENT',
    p_model_call_id,p_disclosure_id,p_intent_sha256,p_grant_id,p_scope_kind,p_scope_id,
    p_data_class,p_payload_sha256,p_payload_bytes
  )
$$;

CREATE FUNCTION private.settle_contribution_job_if_live(
  p_tenant_id uuid,
  p_execution_id uuid,
  p_job_id uuid,
  p_lease_owner text,
  p_attempt integer,
  p_status text,
  p_error_class text
) RETURNS void
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  IF p_job_id IS NULL AND p_lease_owner IS NULL AND p_attempt IS NULL THEN RETURN; END IF;
  IF p_status NOT IN ('DONE','FAILED') THEN
    RAISE EXCEPTION 'invalid terminal job status' USING ERRCODE='22023';
  END IF;
  PERFORM private.require_contribution_execution_lease(
    p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt
  );
  UPDATE ops.jobs SET status=p_status,last_error_class=p_error_class
  WHERE tenant_id=p_tenant_id AND job_id=p_job_id AND status='PROCESSING'
    AND lease_owner=p_lease_owner AND attempt=p_attempt
    AND lease_expires_at>clock_timestamp();
  IF NOT FOUND THEN RAISE EXCEPTION 'fresh contribution job lease required' USING ERRCODE='55000'; END IF;
END;
$$;

CREATE FUNCTION private.complete_contribution_a_exact(
  p_tenant_id uuid,p_execution_id uuid,p_model_call_id uuid,p_request_id uuid,
  p_intent_sha256 bytea,p_disclosure_id uuid,p_outcome text,
  p_coverage_probe_sha256 bytea,p_coverage_snapshot_id uuid,p_coverage_version integer,
  p_coverage_summaries_canonical bytea,p_coverage_digest_sha256 bytea,
  p_scan_receipt jsonb,p_scan_receipt_sha256 bytea,p_provider_trace text,
  p_provider_request_id text,p_error_class text,
  p_job_id uuid,p_lease_owner text,p_attempt integer
) RETURNS private.contribution_execution_state
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE root private.contribution_executions%ROWTYPE; next_state private.contribution_execution_state;
BEGIN
  PERFORM set_config('humaux.tenant_id',p_tenant_id::text,true);
  IF p_outcome NOT IN ('USABLE','REJECTED_SAFETY','FAILED_TERMINAL') THEN
    RAISE EXCEPTION 'invalid A completion outcome' USING ERRCODE='22023';
  END IF;
  -- A caller that presents a lease tuple is asking for the live path and must prove it before
  -- any receipt is consumed.  Only the all-NULL tuple is the bounded exact late-completion path.
  IF p_job_id IS NOT NULL OR p_lease_owner IS NOT NULL OR p_attempt IS NOT NULL THEN
    PERFORM private.require_contribution_execution_lease(
      p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt
    );
  END IF;
  IF p_outcome='FAILED_TERMINAL' AND (p_error_class IS NULL OR btrim(p_error_class)='') THEN
    RAISE EXCEPTION 'definite A provider failure requires an error class' USING ERRCODE='22023';
  END IF;
  SELECT * INTO root FROM private.contribution_executions
    WHERE tenant_id=p_tenant_id AND execution_id=p_execution_id FOR UPDATE;
  IF NOT FOUND OR root.state<>'A_RESERVED'
     OR root.coverage_model_call_id<>p_model_call_id
     OR root.coverage_request_id<>p_request_id
     OR root.coverage_intent_sha256<>p_intent_sha256
     OR root.coverage_disclosure_id<>p_disclosure_id
     OR NOT EXISTS(SELECT 1 FROM ops.model_call_ledger WHERE tenant_id=p_tenant_id
       AND model_call_id=p_model_call_id AND request_id=p_request_id
       AND intent_sha256=p_intent_sha256 AND call_kind='COVERAGE_PROBE' AND status='RESERVED')
     OR NOT EXISTS(SELECT 1 FROM ops.data_disclosures WHERE tenant_id=p_tenant_id
       AND disclosure_id=p_disclosure_id AND model_call_id=p_model_call_id
       AND finalized_at IS NULL AND outcome IS NULL) THEN
    RAISE EXCEPTION 'A exact completion identity or reservation mismatch' USING ERRCODE='23514';
  END IF;
  IF p_outcome<>'FAILED_TERMINAL' AND (
    p_scan_receipt IS NULL OR octet_length(p_scan_receipt_sha256)<>32
    OR p_scan_receipt_sha256<>sha256(convert_to(p_scan_receipt::text,'UTF8'))
    OR p_provider_trace IS NULL OR btrim(p_provider_trace)=''
  ) THEN RAISE EXCEPTION 'successful A completion requires exact scan/provider receipt' USING ERRCODE='23514'; END IF;
  IF p_outcome='USABLE' AND (
    octet_length(p_coverage_probe_sha256)<>32 OR p_coverage_snapshot_id IS NULL
    OR p_coverage_version<=0 OR octet_length(p_coverage_summaries_canonical)=0
    OR octet_length(p_coverage_digest_sha256)<>32
  ) THEN RAISE EXCEPTION 'usable A completion requires full coverage bundle' USING ERRCODE='23514'; END IF;

  UPDATE ops.model_call_ledger SET status=CASE WHEN p_outcome='FAILED_TERMINAL' THEN 'FAILED' ELSE 'SUCCEEDED' END,
    error_class=CASE WHEN p_outcome='FAILED_TERMINAL' THEN p_error_class END,
    provider_request_id=p_provider_request_id
  WHERE tenant_id=p_tenant_id AND model_call_id=p_model_call_id;
  UPDATE ops.data_disclosures SET finalized_at=clock_timestamp(),
    outcome=CASE WHEN p_outcome='FAILED_TERMINAL' THEN 'FAILED' ELSE 'SUCCESS' END
  WHERE tenant_id=p_tenant_id AND disclosure_id=p_disclosure_id;
  next_state:=CASE p_outcome WHEN 'USABLE' THEN 'READY_B'
    WHEN 'REJECTED_SAFETY' THEN 'REJECTED_SAFETY' ELSE 'FAILED_TERMINAL' END;
  UPDATE private.contribution_executions SET state=next_state,
    coverage_probe_sha256=CASE WHEN p_outcome='USABLE' THEN p_coverage_probe_sha256 END,
    coverage_snapshot_id=CASE WHEN p_outcome='USABLE' THEN p_coverage_snapshot_id END,
    coverage_version=CASE WHEN p_outcome='USABLE' THEN p_coverage_version END,
    coverage_summaries_canonical=CASE WHEN p_outcome='USABLE' THEN p_coverage_summaries_canonical END,
    coverage_digest_sha256=CASE WHEN p_outcome='USABLE' THEN p_coverage_digest_sha256 END,
    coverage_scan_receipt=CASE WHEN p_outcome<>'FAILED_TERMINAL' THEN p_scan_receipt END,
    coverage_scan_receipt_sha256=CASE WHEN p_outcome<>'FAILED_TERMINAL' THEN p_scan_receipt_sha256 END,
    coverage_scan_disposition=CASE p_outcome WHEN 'USABLE' THEN 'PASS'
      WHEN 'REJECTED_SAFETY' THEN 'REJECT' END,
    coverage_provider_trace=CASE WHEN p_outcome<>'FAILED_TERMINAL' THEN p_provider_trace END,
    terminal_call_kind=CASE WHEN p_outcome='USABLE' THEN NULL ELSE 'COVERAGE_PROBE' END,
    terminal_recorded_at=CASE WHEN p_outcome='USABLE' THEN NULL ELSE clock_timestamp() END
  WHERE execution_id=p_execution_id;
  IF p_outcome='REJECTED_SAFETY' THEN
    PERFORM private.settle_contribution_job_if_live(
      p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt,'DONE',NULL
    );
  ELSIF p_outcome='FAILED_TERMINAL' THEN
    PERFORM private.settle_contribution_job_if_live(
      p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt,'FAILED',p_error_class
    );
  END IF;
  RETURN next_state;
END;
$$;

CREATE FUNCTION private.complete_contribution_b_exact(
  p_tenant_id uuid,p_execution_id uuid,p_model_call_id uuid,p_request_id uuid,
  p_intent_sha256 bytea,p_disclosure_id uuid,p_outcome text,
  p_assessment_output_canonical bytea,p_assessment_output_sha256 bytea,
  p_novelty_gate text,p_quality_gate text,p_generality_gate text,p_grounding_gate text,
  p_candidate_body bytea,p_candidate_sha256 bytea,p_scan_receipt jsonb,
  p_scan_receipt_sha256 bytea,p_provider_trace text,p_provider_request_id text,
  p_error_class text,p_job_id uuid,p_lease_owner text,p_attempt integer
) RETURNS private.contribution_execution_state
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE root private.contribution_executions%ROWTYPE; next_state private.contribution_execution_state;
BEGIN
  PERFORM set_config('humaux.tenant_id',p_tenant_id::text,true);
  IF p_outcome NOT IN ('READY_CANDIDATE','NOT_CONTRIBUTABLE','REJECTED_SAFETY','FAILED_TERMINAL') THEN
    RAISE EXCEPTION 'invalid B completion outcome' USING ERRCODE='22023';
  END IF;
  -- Match A: a supplied tuple is always live-lease authority; all NULL is the only late path.
  IF p_job_id IS NOT NULL OR p_lease_owner IS NOT NULL OR p_attempt IS NOT NULL THEN
    PERFORM private.require_contribution_execution_lease(
      p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt
    );
  END IF;
  IF p_outcome='FAILED_TERMINAL' AND (p_error_class IS NULL OR btrim(p_error_class)='') THEN
    RAISE EXCEPTION 'definite B provider failure requires an error class' USING ERRCODE='22023';
  END IF;
  SELECT * INTO root FROM private.contribution_executions
    WHERE tenant_id=p_tenant_id AND execution_id=p_execution_id FOR UPDATE;
  IF NOT FOUND OR root.state<>'B_RESERVED'
     OR root.assessment_model_call_id<>p_model_call_id
     OR root.assessment_request_id<>p_request_id
     OR root.assessment_intent_sha256<>p_intent_sha256
     OR root.assessment_disclosure_id<>p_disclosure_id
     OR NOT EXISTS(SELECT 1 FROM ops.model_call_ledger WHERE tenant_id=p_tenant_id
       AND model_call_id=p_model_call_id AND request_id=p_request_id
       AND intent_sha256=p_intent_sha256 AND call_kind='TYPED_ASSESSMENT' AND status='RESERVED')
     OR NOT EXISTS(SELECT 1 FROM ops.data_disclosures WHERE tenant_id=p_tenant_id
       AND disclosure_id=p_disclosure_id AND model_call_id=p_model_call_id
       AND finalized_at IS NULL AND outcome IS NULL) THEN
    RAISE EXCEPTION 'B exact completion identity or reservation mismatch' USING ERRCODE='23514';
  END IF;
  IF p_outcome<>'FAILED_TERMINAL' AND (
    octet_length(p_assessment_output_canonical)=0
    OR p_assessment_output_sha256<>sha256(p_assessment_output_canonical)
    OR p_novelty_gate NOT IN ('PASS','FAIL') OR p_quality_gate NOT IN ('PASS','FAIL')
    OR p_generality_gate NOT IN ('PASS','FAIL') OR p_grounding_gate NOT IN ('PASS','FAIL')
    OR p_provider_trace IS NULL OR btrim(p_provider_trace)=''
  ) THEN RAISE EXCEPTION 'successful B completion requires full typed assessment' USING ERRCODE='23514'; END IF;
  IF p_outcome='READY_CANDIDATE' AND (
    ROW(p_novelty_gate,p_quality_gate,p_generality_gate,p_grounding_gate)
      IS DISTINCT FROM ROW('PASS'::text,'PASS'::text,'PASS'::text,'PASS'::text)
    OR octet_length(p_candidate_body)=0 OR p_candidate_sha256<>sha256(p_candidate_body)
    OR p_scan_receipt_sha256<>sha256(convert_to(p_scan_receipt::text,'UTF8'))
  ) THEN RAISE EXCEPTION 'candidate-ready B completion requires exact all-PASS candidate bundle' USING ERRCODE='23514'; END IF;
  IF p_outcome='NOT_CONTRIBUTABLE' AND NOT (
    p_novelty_gate='FAIL' OR p_quality_gate='FAIL'
    OR p_generality_gate='FAIL' OR p_grounding_gate='FAIL'
  ) THEN RAISE EXCEPTION 'NOT_CONTRIBUTABLE requires a raw FAIL gate' USING ERRCODE='23514'; END IF;
  IF p_outcome='REJECTED_SAFETY' AND (
    octet_length(p_candidate_body)=0 OR p_candidate_sha256<>sha256(p_candidate_body)
    OR p_scan_receipt_sha256<>sha256(convert_to(p_scan_receipt::text,'UTF8'))
  ) THEN RAISE EXCEPTION 'B safety rejection requires exact rejected candidate bundle' USING ERRCODE='23514'; END IF;

  UPDATE ops.model_call_ledger SET status=CASE WHEN p_outcome='FAILED_TERMINAL' THEN 'FAILED' ELSE 'SUCCEEDED' END,
    error_class=CASE WHEN p_outcome='FAILED_TERMINAL' THEN p_error_class END,
    provider_request_id=p_provider_request_id
  WHERE tenant_id=p_tenant_id AND model_call_id=p_model_call_id;
  UPDATE ops.data_disclosures SET finalized_at=clock_timestamp(),
    outcome=CASE WHEN p_outcome='FAILED_TERMINAL' THEN 'FAILED' ELSE 'SUCCESS' END
  WHERE tenant_id=p_tenant_id AND disclosure_id=p_disclosure_id;
  next_state:=p_outcome::private.contribution_execution_state;
  UPDATE private.contribution_executions SET state=next_state,
    assessment_output_canonical=CASE WHEN p_outcome<>'FAILED_TERMINAL' THEN p_assessment_output_canonical END,
    assessment_output_sha256=CASE WHEN p_outcome<>'FAILED_TERMINAL' THEN p_assessment_output_sha256 END,
    novelty_gate=CASE WHEN p_outcome<>'FAILED_TERMINAL' THEN p_novelty_gate END,
    quality_gate=CASE WHEN p_outcome<>'FAILED_TERMINAL' THEN p_quality_gate END,
    generality_gate=CASE WHEN p_outcome<>'FAILED_TERMINAL' THEN p_generality_gate END,
    grounding_gate=CASE WHEN p_outcome<>'FAILED_TERMINAL' THEN p_grounding_gate END,
    candidate_body=CASE WHEN p_outcome IN ('READY_CANDIDATE','REJECTED_SAFETY') THEN p_candidate_body END,
    candidate_sha256=CASE WHEN p_outcome IN ('READY_CANDIDATE','REJECTED_SAFETY') THEN p_candidate_sha256 END,
    candidate_scan_receipt=CASE WHEN p_outcome IN ('READY_CANDIDATE','REJECTED_SAFETY') THEN p_scan_receipt END,
    candidate_scan_receipt_sha256=CASE WHEN p_outcome IN ('READY_CANDIDATE','REJECTED_SAFETY') THEN p_scan_receipt_sha256 END,
    candidate_scan_disposition=CASE p_outcome WHEN 'READY_CANDIDATE' THEN 'PASS'
      WHEN 'REJECTED_SAFETY' THEN 'REJECT' END,
    assessment_provider_trace=CASE WHEN p_outcome<>'FAILED_TERMINAL' THEN p_provider_trace END,
    terminal_call_kind=CASE WHEN p_outcome IN ('REJECTED_SAFETY','FAILED_TERMINAL')
      THEN 'TYPED_ASSESSMENT' END,
    terminal_recorded_at=CASE WHEN p_outcome IN ('REJECTED_SAFETY','FAILED_TERMINAL')
      THEN clock_timestamp() END
  WHERE execution_id=p_execution_id;
  IF p_outcome IN ('NOT_CONTRIBUTABLE','REJECTED_SAFETY') THEN
    PERFORM private.settle_contribution_job_if_live(
      p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt,'DONE',NULL
    );
  ELSIF p_outcome='FAILED_TERMINAL' THEN
    PERFORM private.settle_contribution_job_if_live(
      p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt,'FAILED',p_error_class
    );
  END IF;
  RETURN next_state;
END;
$$;

CREATE FUNCTION staging.contribution_candidate_execution_validate()
RETURNS trigger
LANGUAGE plpgsql
SET search_path=pg_catalog
AS $$
DECLARE root private.contribution_executions%ROWTYPE;
BEGIN
  IF NEW.contribution_execution_id IS NULL THEN RETURN NEW; END IF;
  SELECT * INTO root FROM private.contribution_executions
    WHERE tenant_id=NEW.tenant_id AND execution_id=NEW.contribution_execution_id FOR KEY SHARE;
  IF NOT FOUND OR root.state<>'READY_CANDIDATE' OR NEW.candidate_id<>root.candidate_id
     OR NEW.model_call_id<>root.assessment_model_call_id
     OR NEW.binding_id<>root.binding_id OR NEW.binding_version<>root.binding_version
     OR NEW.reasoning_domain_id<>root.reasoning_domain_id
     OR NEW.source_manifest_hash<>root.input_manifest_hash
     OR NEW.disclosed_payload<>root.candidate_body
     OR NEW.disclosed_payload_sha256<>root.candidate_sha256
     OR NEW.scan_receipt<>root.candidate_scan_receipt
     OR NEW.provider_trace<>root.assessment_provider_trace
     OR ROW(root.novelty_gate,root.quality_gate,root.generality_gate,root.grounding_gate)
       IS DISTINCT FROM ROW('PASS'::text,'PASS'::text,'PASS'::text,'PASS'::text)
     OR root.candidate_scan_disposition<>'PASS'
     OR NOT EXISTS(SELECT 1 FROM ops.model_call_ledger call WHERE call.tenant_id=root.tenant_id
       AND call.model_call_id=root.assessment_model_call_id
       AND call.request_id=root.assessment_request_id
       AND call.call_kind='TYPED_ASSESSMENT' AND call.status='SUCCEEDED')
     OR NOT EXISTS(SELECT 1 FROM ops.data_disclosures disclosure
       WHERE disclosure.tenant_id=root.tenant_id
       AND disclosure.disclosure_id=root.assessment_disclosure_id
       AND disclosure.model_call_id=root.assessment_model_call_id
       AND disclosure.outcome='SUCCESS' AND disclosure.finalized_at IS NOT NULL)
  THEN
    RAISE EXCEPTION 'execution candidate must bind the complete successful B receipt exactly'
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER contribution_candidate_execution_validate
  BEFORE INSERT OR UPDATE OF contribution_execution_id ON staging.contribution_candidates
  FOR EACH ROW EXECUTE FUNCTION staging.contribution_candidate_execution_validate();

CREATE FUNCTION private.commit_contribution_candidate(
  p_tenant_id uuid,p_execution_id uuid,p_job_id uuid,p_lease_owner text,p_attempt integer
) RETURNS uuid
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE root private.contribution_executions%ROWTYPE; profile_version bigint;
DECLARE assessment_receipt jsonb; assessment_digest bytea;
BEGIN
  PERFORM private.require_contribution_execution_lease(
    p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt
  );
  SELECT * INTO root FROM private.contribution_executions
    WHERE tenant_id=p_tenant_id AND execution_id=p_execution_id FOR UPDATE;
  IF NOT FOUND OR root.state<>'READY_CANDIDATE' THEN
    RAISE EXCEPTION 'candidate commit requires READY_CANDIDATE' USING ERRCODE='23514';
  END IF;
  PERFORM set_config('humaux.user_id',root.user_id::text,true);
  SELECT call.profile_version INTO profile_version FROM ops.model_call_ledger call
    WHERE call.tenant_id=p_tenant_id AND call.model_call_id=root.assessment_model_call_id
      AND call.request_id=root.assessment_request_id AND call.status='SUCCEEDED'
      AND call.call_kind='TYPED_ASSESSMENT';
  IF NOT FOUND THEN RAISE EXCEPTION 'candidate commit requires succeeded B ledger' USING ERRCODE='23514'; END IF;
  assessment_receipt:=jsonb_build_object(
    'probe_sha256',encode(root.coverage_probe_sha256,'hex'),
    'coverage_digest_id',root.coverage_snapshot_id::text,
    'coverage_version',root.coverage_version,
    'coverage_digest_sha256',encode(root.coverage_digest_sha256,'hex'),
    'candidate_payload_sha256',encode(root.candidate_sha256,'hex'),
    'novelty',root.novelty_gate,'quality',root.quality_gate,
    'generality',root.generality_gate,'grounding',root.grounding_gate
  );
  assessment_digest:=sha256(convert_to(assessment_receipt::text,'UTF8'));
  INSERT INTO staging.contribution_candidates(
    candidate_id,tenant_id,user_id,policy_id,policy_version,policy_snapshot,
    reasoning_domain_id,profile_version,source_manifest_hash,source_count,
    disclosed_payload,disclosed_payload_sha256,provider_trace,scan_receipt,
    rights_basis,source_license,publisher,contributor_attestation,redistribution_policy,
    binding_id,binding_version,model_call_id,contribution_execution_id
  ) VALUES (
    root.candidate_id,root.tenant_id,root.user_id,root.policy_id,root.policy_version,
    root.policy_snapshot,root.reasoning_domain_id,profile_version,root.input_manifest_hash,
    root.source_count,root.candidate_body,root.candidate_sha256,
    root.assessment_provider_trace,root.candidate_scan_receipt,root.rights_basis,
    root.source_license,root.publisher,root.contributor_attestation,root.redistribution_policy,
    root.binding_id,root.binding_version,root.assessment_model_call_id,root.execution_id
  );
  INSERT INTO staging.contribution_candidate_sources(
    tenant_id,candidate_id,evidence_id,memory_id,source_hash,ordinal
  ) SELECT tenant_id,root.candidate_id,evidence_id,memory_id,source_hash,ordinal
    FROM private.contribution_execution_sources WHERE execution_id=root.execution_id;
  INSERT INTO staging.contribution_candidate_phase9_assessments(
    candidate_id,tenant_id,probe_bytes,probe_sha256,probe_source_manifest_hash,
    probe_provider_trace,probe_scan_receipt,coverage_digest_id,coverage_version,
    coverage_digest_sha256,candidate_payload_sha256,assessment_digest,
    assessment_provider_trace,assessment_receipt
  ) VALUES (
    root.candidate_id,root.tenant_id,NULL,root.coverage_probe_sha256,root.input_manifest_hash,
    root.coverage_provider_trace,root.coverage_scan_receipt,root.coverage_snapshot_id,
    root.coverage_version,root.coverage_digest_sha256,root.candidate_sha256,
    assessment_digest,root.assessment_provider_trace,assessment_receipt
  );
  UPDATE private.contribution_executions SET state='DONE' WHERE execution_id=root.execution_id;
  UPDATE ops.jobs SET status='DONE',last_error_class=NULL
  WHERE tenant_id=p_tenant_id AND job_id=p_job_id AND status='PROCESSING'
    AND lease_owner=p_lease_owner AND attempt=p_attempt
    AND lease_expires_at>clock_timestamp();
  IF NOT FOUND THEN RAISE EXCEPTION 'fresh contribution job lease required' USING ERRCODE='55000'; END IF;
  RETURN root.candidate_id;
END;
$$;

CREATE FUNCTION private.settle_contribution_terminal_job(
  p_tenant_id uuid,p_execution_id uuid,p_job_id uuid,p_lease_owner text,p_attempt integer
) RETURNS text
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE root private.contribution_executions%ROWTYPE; target_status text; error_class text;
BEGIN
  PERFORM private.require_contribution_execution_lease(
    p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt
  );
  SELECT * INTO root FROM private.contribution_executions
    WHERE tenant_id=p_tenant_id AND execution_id=p_execution_id FOR UPDATE;
  IF root.state IN ('NOT_CONTRIBUTABLE','REJECTED_SAFETY') THEN
    target_status:='DONE'; error_class:=NULL;
  ELSIF root.state='FAILED_TERMINAL' THEN
    target_status:='FAILED'; error_class:='CONTRIBUTION_PROVIDER_TERMINAL';
  ELSE
    RAISE EXCEPTION 'execution is not a terminal no-candidate business result'
      USING ERRCODE='23514';
  END IF;
  UPDATE ops.jobs SET status=target_status,last_error_class=error_class
  WHERE tenant_id=p_tenant_id AND job_id=p_job_id AND status='PROCESSING'
    AND lease_owner=p_lease_owner AND attempt=p_attempt
    AND lease_expires_at>clock_timestamp();
  IF NOT FOUND THEN RAISE EXCEPTION 'fresh contribution job lease required' USING ERRCODE='55000'; END IF;
  RETURN target_status;
END;
$$;

CREATE FUNCTION private.mark_contribution_reconciliation_required(
  p_tenant_id uuid,p_execution_id uuid,p_job_id uuid,p_lease_owner text,p_attempt integer
) RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE execution_state private.contribution_execution_state;
BEGIN
  PERFORM private.require_contribution_execution_lease(
    p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt
  );
  SELECT state INTO execution_state FROM private.contribution_executions
    WHERE tenant_id=p_tenant_id AND execution_id=p_execution_id FOR UPDATE;
  IF execution_state NOT IN ('A_RESERVED','B_RESERVED') THEN
    RAISE EXCEPTION 'reconciliation disposition requires an exact reserved execution'
      USING ERRCODE='23514';
  END IF;
  UPDATE ops.jobs SET status='FAILED',last_error_class='RECONCILIATION_REQUIRED'
  WHERE tenant_id=p_tenant_id AND job_id=p_job_id AND status='PROCESSING'
    AND lease_owner=p_lease_owner AND attempt=p_attempt
    AND lease_expires_at>clock_timestamp();
  RETURN FOUND;
END;
$$;

ALTER TABLE private.contribution_executions ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.contribution_executions FORCE ROW LEVEL SECURITY;
ALTER TABLE private.contribution_execution_sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.contribution_execution_sources FORCE ROW LEVEL SECURITY;
ALTER TABLE ops.contribution_execution_job_links ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.contribution_execution_job_links FORCE ROW LEVEL SECURITY;

CREATE POLICY contribution_executions_tenant_isolation ON private.contribution_executions
  USING (current_user='role_migration_owner' OR
    tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid)
  WITH CHECK (current_user='role_migration_owner' OR
    tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid);
CREATE POLICY contribution_execution_sources_tenant_isolation
  ON private.contribution_execution_sources
  USING (current_user='role_migration_owner' OR
    tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid)
  WITH CHECK (current_user='role_migration_owner' OR
    tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid);
CREATE POLICY contribution_execution_job_links_tenant_isolation
  ON ops.contribution_execution_job_links
  USING (current_user='role_migration_owner' OR
    tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid)
  WITH CHECK (current_user='role_migration_owner' OR
    tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid);

ALTER TABLE private.contribution_executions OWNER TO role_migration_owner;
ALTER TABLE private.contribution_execution_sources OWNER TO role_migration_owner;
ALTER TABLE ops.contribution_execution_job_links OWNER TO role_migration_owner;

ALTER FUNCTION private.contribution_execution_guard_mutation() OWNER TO role_migration_owner;
ALTER FUNCTION private.contribution_execution_relation_append_only() OWNER TO role_migration_owner;
ALTER FUNCTION ops.contribution_execution_job_link_validate() OWNER TO role_migration_owner;
ALTER FUNCTION private.require_contribution_execution_lease(uuid,uuid,uuid,text,integer)
  OWNER TO role_migration_owner;
ALTER FUNCTION private.require_contribution_source_manifest() OWNER TO role_migration_owner;
ALTER FUNCTION private.enqueue_contribution_execution(
  uuid,uuid,uuid,uuid,uuid,uuid,text,bytea,uuid,uuid,bytea,uuid,bigint,jsonb,text,
  text,text,text,text,uuid,bigint,bigint,bigint,bytea,bytea,text[],uuid[],bytea[]
) OWNER TO role_migration_owner;
ALTER FUNCTION private.reserve_contribution_execution_call(
  uuid,uuid,uuid,text,integer,text,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint
) OWNER TO role_migration_owner;
ALTER FUNCTION private.reserve_contribution_a(
  uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint
) OWNER TO role_migration_owner;
ALTER FUNCTION private.reserve_contribution_b(
  uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint
) OWNER TO role_migration_owner;
ALTER FUNCTION private.settle_contribution_job_if_live(uuid,uuid,uuid,text,integer,text,text)
  OWNER TO role_migration_owner;
ALTER FUNCTION private.complete_contribution_a_exact(
  uuid,uuid,uuid,uuid,bytea,uuid,text,bytea,uuid,integer,bytea,bytea,jsonb,bytea,text,
  text,text,uuid,text,integer
) OWNER TO role_migration_owner;
ALTER FUNCTION private.complete_contribution_b_exact(
  uuid,uuid,uuid,uuid,bytea,uuid,text,bytea,bytea,text,text,text,text,bytea,bytea,jsonb,
  bytea,text,text,text,uuid,text,integer
) OWNER TO role_migration_owner;
ALTER FUNCTION staging.contribution_candidate_execution_validate() OWNER TO role_migration_owner;
ALTER FUNCTION private.commit_contribution_candidate(uuid,uuid,uuid,text,integer)
  OWNER TO role_migration_owner;
ALTER FUNCTION private.settle_contribution_terminal_job(uuid,uuid,uuid,text,integer)
  OWNER TO role_migration_owner;
ALTER FUNCTION private.mark_contribution_reconciliation_required(uuid,uuid,uuid,text,integer)
  OWNER TO role_migration_owner;

REVOKE ALL ON private.contribution_executions,private.contribution_execution_sources,
  ops.contribution_execution_job_links
  FROM PUBLIC,role_gateway,role_private_worker,role_consolidation_worker,role_public_worker,
    role_retrieval_worker,role_batch_issuer,role_maintenance;
GRANT SELECT ON private.contribution_executions,private.contribution_execution_sources,
  ops.contribution_execution_job_links TO role_private_worker;

REVOKE ALL ON FUNCTION
  private.contribution_execution_guard_mutation(),
  private.contribution_execution_relation_append_only(),
  ops.contribution_execution_job_link_validate(),
  private.require_contribution_execution_lease(uuid,uuid,uuid,text,integer),
  private.require_contribution_source_manifest(),
  private.enqueue_contribution_execution(
    uuid,uuid,uuid,uuid,uuid,uuid,text,bytea,uuid,uuid,bytea,uuid,bigint,jsonb,text,
    text,text,text,text,uuid,bigint,bigint,bigint,bytea,bytea,text[],uuid[],bytea[]
  ),
  private.reserve_contribution_execution_call(
    uuid,uuid,uuid,text,integer,text,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint
  ),
  private.reserve_contribution_a(
    uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint
  ),
  private.reserve_contribution_b(
    uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint
  ),
  private.settle_contribution_job_if_live(uuid,uuid,uuid,text,integer,text,text),
  private.complete_contribution_a_exact(
    uuid,uuid,uuid,uuid,bytea,uuid,text,bytea,uuid,integer,bytea,bytea,jsonb,bytea,text,
    text,text,uuid,text,integer
  ),
  private.complete_contribution_b_exact(
    uuid,uuid,uuid,uuid,bytea,uuid,text,bytea,bytea,text,text,text,text,bytea,bytea,jsonb,
    bytea,text,text,text,uuid,text,integer
  ),
  staging.contribution_candidate_execution_validate(),
  private.commit_contribution_candidate(uuid,uuid,uuid,text,integer),
  private.settle_contribution_terminal_job(uuid,uuid,uuid,text,integer),
  private.mark_contribution_reconciliation_required(uuid,uuid,uuid,text,integer)
  FROM PUBLIC,role_gateway,role_private_worker,role_consolidation_worker,role_public_worker,
    role_retrieval_worker,role_batch_issuer,role_maintenance;

GRANT EXECUTE ON FUNCTION
  private.enqueue_contribution_execution(
    uuid,uuid,uuid,uuid,uuid,uuid,text,bytea,uuid,uuid,bytea,uuid,bigint,jsonb,text,
    text,text,text,text,uuid,bigint,bigint,bigint,bytea,bytea,text[],uuid[],bytea[]
  ),
  private.reserve_contribution_a(
    uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint
  ),
  private.reserve_contribution_b(
    uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint
  ),
  private.complete_contribution_a_exact(
    uuid,uuid,uuid,uuid,bytea,uuid,text,bytea,uuid,integer,bytea,bytea,jsonb,bytea,text,
    text,text,uuid,text,integer
  ),
  private.complete_contribution_b_exact(
    uuid,uuid,uuid,uuid,bytea,uuid,text,bytea,bytea,text,text,text,text,bytea,bytea,jsonb,
    bytea,text,text,text,uuid,text,integer
  ),
  private.commit_contribution_candidate(uuid,uuid,uuid,text,integer),
  private.settle_contribution_terminal_job(uuid,uuid,uuid,text,integer),
  private.mark_contribution_reconciliation_required(uuid,uuid,uuid,text,integer)
  TO role_private_worker;
