-- Phase 9 R3: exact Binding-only USER_REASONING admission.
-- Health authority is append-only observation history; no mutable current projection,
-- fallback, substitution, external I/O, legacy backfill, or Phase 10 activation is introduced.

-- R3 recipient authority is an endpoint-owned UUID. Existing endpoints remain NULL for
-- legacy/DRAFT/SHADOW compatibility; an exact SERVING admission requires a newly provisioned
-- non-NULL value. The 0128 endpoint identity trigger makes it immutable after INSERT.
ALTER TABLE control.provider_endpoints
  ADD COLUMN egress_processor_id uuid,
  ADD CONSTRAINT provider_endpoints_egress_processor_non_nil CHECK (
    egress_processor_id IS NULL
    OR egress_processor_id <> '00000000-0000-0000-0000-000000000000'::uuid
  ),
  ADD CONSTRAINT provider_endpoints_tenant_endpoint_egress_unique
    UNIQUE (tenant_id, endpoint_id, egress_processor_id);

CREATE TABLE ops.reasoning_provider_health_observations (
  observation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  processor_id text NOT NULL CHECK (btrim(processor_id) <> ''),
  processor_model_id uuid NOT NULL REFERENCES control.processor_models(processor_model_id),
  provider_model_id text NOT NULL CHECK (btrim(provider_model_id) <> ''),
  model_revision text,
  provider_endpoint_id uuid NOT NULL,
  endpoint_ref text NOT NULL CHECK (btrim(endpoint_ref) <> ''),
  region text NOT NULL CHECK (btrim(region) <> ''),
  service_tier text NOT NULL CHECK (btrim(service_tier) <> ''),
  source_kind text NOT NULL CHECK (btrim(source_kind) <> ''),
  reason_code text CHECK (reason_code IS NULL OR btrim(reason_code) <> ''),
  verdict text NOT NULL CHECK (verdict IN ('HEALTHY', 'DEGRADED', 'UNAVAILABLE', 'UNKNOWN')),
  observed_at timestamptz NOT NULL,
  valid_until timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (observed_at < valid_until),
  UNIQUE (tenant_id, observation_id),
  FOREIGN KEY (tenant_id, provider_endpoint_id)
    REFERENCES control.provider_endpoints(tenant_id, endpoint_id)
);

CREATE INDEX reasoning_provider_health_latest
  ON ops.reasoning_provider_health_observations (
    tenant_id,
    processor_id,
    processor_model_id,
    provider_model_id,
    model_revision,
    provider_endpoint_id,
    region,
    service_tier,
    observed_at DESC,
    observation_id DESC
  );

CREATE TABLE ops.reasoning_account_health_observations (
  observation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  provider_account_id uuid NOT NULL,
  credential_ref uuid NOT NULL REFERENCES control.credentials(credential_id),
  billing_account_id uuid,
  billing_instrument_id uuid,
  source_kind text NOT NULL CHECK (btrim(source_kind) <> ''),
  reason_code text CHECK (reason_code IS NULL OR btrim(reason_code) <> ''),
  account_verdict text NOT NULL CHECK (account_verdict IN ('HEALTHY', 'UNHEALTHY', 'UNKNOWN')),
  credential_verdict text NOT NULL CHECK (credential_verdict IN ('VALID', 'INVALID', 'UNKNOWN')),
  billing_account_verdict text CHECK (billing_account_verdict IN ('ENABLED', 'DISABLED', 'UNKNOWN')),
  billing_instrument_verdict text CHECK (billing_instrument_verdict IN ('ENABLED', 'DISABLED', 'UNKNOWN')),
  observed_at timestamptz NOT NULL,
  valid_until timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (observed_at < valid_until),
  CHECK (
    (billing_account_id IS NULL AND billing_account_verdict IS NULL)
    OR (billing_account_id IS NOT NULL AND billing_account_verdict IS NOT NULL)
  ),
  CHECK (
    (billing_instrument_id IS NULL AND billing_instrument_verdict IS NULL)
    OR (billing_instrument_id IS NOT NULL AND billing_instrument_verdict IS NOT NULL)
  ),
  CHECK (billing_account_id IS NOT NULL OR billing_instrument_id IS NULL),
  UNIQUE (tenant_id, observation_id),
  FOREIGN KEY (tenant_id, provider_account_id)
    REFERENCES control.provider_accounts(tenant_id, provider_account_id),
  FOREIGN KEY (tenant_id, billing_account_id)
    REFERENCES control.provider_billing_accounts(tenant_id, billing_account_id),
  FOREIGN KEY (tenant_id, billing_instrument_id)
    REFERENCES control.provider_billing_instruments(tenant_id, billing_instrument_id)
);

CREATE INDEX reasoning_account_health_latest
  ON ops.reasoning_account_health_observations (
    tenant_id,
    provider_account_id,
    credential_ref,
    billing_account_id,
    billing_instrument_id,
    observed_at DESC,
    observation_id DESC
  );

-- The existing ModelCallLedger remains the sole actual-provider-attempt ledger. Retrieval
-- rows keep every reasoning column NULL; CONTRIBUTION_DEIDENTIFY reserves one exact immutable
-- admission snapshot. Existing provider/model/model_revision columns are reused.
ALTER TABLE ops.model_call_ledger
  DROP CONSTRAINT model_call_ledger_purpose_known,
  ADD CONSTRAINT model_call_ledger_purpose_known CHECK (
    purpose IS NULL OR purpose = ANY (
      ARRAY['query_rewrite', 'embedding', 'rerank', 'CONTRIBUTION_DEIDENTIFY']::text[]
    )
  ),
  ADD COLUMN reasoning_domain_id uuid,
  ADD COLUMN call_kind text,
  ADD COLUMN intent_sha256 bytea,
  ADD COLUMN binding_id uuid,
  ADD COLUMN binding_version bigint,
  ADD COLUMN route_policy_id uuid,
  ADD COLUMN route_policy_version bigint,
  ADD COLUMN profile_id uuid,
  ADD COLUMN profile_version bigint,
  ADD COLUMN provider_account_id uuid,
  ADD COLUMN provider_endpoint_id uuid,
  ADD COLUMN egress_processor_id uuid,
  ADD COLUMN credential_ref uuid,
  ADD COLUMN billing_account_id uuid,
  ADD COLUMN billing_instrument_id uuid,
  ADD COLUMN provider_health_observation_id bigint,
  ADD COLUMN account_health_observation_id bigint,
  ADD COLUMN billing_responsibility text,
  ADD COLUMN admitted_at timestamptz,
  ADD CONSTRAINT model_call_ledger_reasoning_snapshot_shape CHECK (
    (
      purpose = 'CONTRIBUTION_DEIDENTIFY'
      AND request_id <> '00000000-0000-0000-0000-000000000000'::uuid
      AND call_kind IN ('COVERAGE_PROBE', 'TYPED_ASSESSMENT')
      AND octet_length(intent_sha256) = 32
      AND reasoning_domain_id IS NOT NULL
      AND binding_id IS NOT NULL
      AND binding_version IS NOT NULL AND binding_version > 0
      AND route_policy_id IS NOT NULL
      AND route_policy_version IS NOT NULL AND route_policy_version > 0
      AND profile_id IS NOT NULL
      AND profile_version IS NOT NULL AND profile_version > 0
      AND provider_account_id IS NOT NULL
      AND provider_endpoint_id IS NOT NULL
      AND egress_processor_id IS NOT NULL
      AND credential_ref IS NOT NULL
      AND provider_health_observation_id IS NOT NULL
      AND account_health_observation_id IS NOT NULL
      AND billing_responsibility = 'USER'
      AND admitted_at IS NOT NULL
      AND (billing_account_id IS NOT NULL OR billing_instrument_id IS NULL)
      AND estimated_cost IS NULL
      AND actual_cost IS NULL
    ) OR (
      purpose IS DISTINCT FROM 'CONTRIBUTION_DEIDENTIFY'
      AND num_nonnulls(
        reasoning_domain_id, call_kind, intent_sha256,
        binding_id, binding_version, route_policy_id,
        route_policy_version, profile_id, profile_version, provider_account_id,
        provider_endpoint_id, egress_processor_id, credential_ref,
        billing_account_id, billing_instrument_id,
        provider_health_observation_id, account_health_observation_id,
        billing_responsibility, admitted_at
      ) = 0
    )
  ),
  ADD CONSTRAINT model_call_ledger_reasoning_call_kind_known CHECK (
    call_kind IS NULL OR call_kind IN ('COVERAGE_PROBE', 'TYPED_ASSESSMENT')
  ),
  ADD CONSTRAINT model_call_ledger_reasoning_recipient_unique
    UNIQUE (tenant_id, model_call_id, egress_processor_id),
  ADD CONSTRAINT model_call_ledger_reasoning_binding_unique
    UNIQUE (
      tenant_id, model_call_id, binding_id, binding_version,
      reasoning_domain_id, profile_version
    ),
  ADD CONSTRAINT model_call_ledger_reasoning_binding_fk
    FOREIGN KEY (tenant_id, binding_id, binding_version)
    REFERENCES control.reasoning_route_bindings(tenant_id, binding_id, binding_version),
  ADD CONSTRAINT model_call_ledger_reasoning_policy_fk
    FOREIGN KEY (tenant_id, route_policy_id, route_policy_version)
    REFERENCES control.reasoning_route_policies(tenant_id, route_policy_id, policy_version),
  ADD CONSTRAINT model_call_ledger_reasoning_profile_fk
    FOREIGN KEY (tenant_id, profile_id, profile_version)
    REFERENCES control.reasoning_profiles(tenant_id, profile_id, profile_version),
  ADD CONSTRAINT model_call_ledger_reasoning_provider_account_fk
    FOREIGN KEY (tenant_id, provider_account_id)
    REFERENCES control.provider_accounts(tenant_id, provider_account_id),
  ADD CONSTRAINT model_call_ledger_reasoning_endpoint_fk
    FOREIGN KEY (tenant_id, provider_endpoint_id, egress_processor_id)
    REFERENCES control.provider_endpoints(tenant_id, endpoint_id, egress_processor_id),
  ADD CONSTRAINT model_call_ledger_reasoning_credential_authority_fk
    FOREIGN KEY (tenant_id, credential_ref, provider_account_id)
    REFERENCES control.reasoning_credential_bindings(
      tenant_id, credential_ref, provider_account_id
    ),
  ADD CONSTRAINT model_call_ledger_reasoning_billing_account_fk
    FOREIGN KEY (tenant_id, billing_account_id)
    REFERENCES control.provider_billing_accounts(tenant_id, billing_account_id),
  ADD CONSTRAINT model_call_ledger_reasoning_billing_instrument_fk
    FOREIGN KEY (tenant_id, billing_instrument_id)
    REFERENCES control.provider_billing_instruments(tenant_id, billing_instrument_id),
  ADD CONSTRAINT model_call_ledger_reasoning_provider_health_fk
    FOREIGN KEY (tenant_id, provider_health_observation_id)
    REFERENCES ops.reasoning_provider_health_observations(tenant_id, observation_id),
  ADD CONSTRAINT model_call_ledger_reasoning_account_health_fk
    FOREIGN KEY (tenant_id, account_health_observation_id)
    REFERENCES ops.reasoning_account_health_observations(tenant_id, observation_id);

ALTER TABLE ops.data_disclosures
  ADD COLUMN model_call_id uuid,
  ADD CONSTRAINT data_disclosures_tenant_model_call_unique
    UNIQUE (tenant_id, model_call_id),
  ADD CONSTRAINT data_disclosures_reasoning_model_call_fk
    FOREIGN KEY (tenant_id, model_call_id, processor_id)
    REFERENCES ops.model_call_ledger(tenant_id, model_call_id, egress_processor_id);

CREATE FUNCTION ops.reasoning_health_observation_reject_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
  RAISE EXCEPTION 'reasoning health observations are append-only'
    USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER reasoning_provider_health_append_only
  BEFORE UPDATE OR DELETE ON ops.reasoning_provider_health_observations
  FOR EACH ROW EXECUTE FUNCTION ops.reasoning_health_observation_reject_mutation();
CREATE TRIGGER reasoning_provider_health_reject_truncate
  BEFORE TRUNCATE ON ops.reasoning_provider_health_observations
  FOR EACH STATEMENT EXECUTE FUNCTION ops.reasoning_health_observation_reject_mutation();

CREATE TRIGGER reasoning_account_health_append_only
  BEFORE UPDATE OR DELETE ON ops.reasoning_account_health_observations
  FOR EACH ROW EXECUTE FUNCTION ops.reasoning_health_observation_reject_mutation();
CREATE TRIGGER reasoning_account_health_reject_truncate
  BEFORE TRUNCATE ON ops.reasoning_account_health_observations
  FOR EACH STATEMENT EXECUTE FUNCTION ops.reasoning_health_observation_reject_mutation();

-- Legacy rows deliberately retain a NULL/NULL route seam. New R3 writers persist the
-- exact Binding@version pair; the composite FK prevents cross-tenant or stale identity.
ALTER TABLE staging.contribution_candidates
  ADD COLUMN binding_id uuid,
  ADD COLUMN binding_version bigint,
  ADD COLUMN model_call_id uuid,
  ADD CONSTRAINT contribution_candidates_reasoning_binding_pair CHECK (
    (binding_id IS NULL) = (binding_version IS NULL)
    AND (binding_id IS NULL) = (model_call_id IS NULL)
    AND (binding_version IS NULL OR binding_version > 0)
  ),
  ADD CONSTRAINT contribution_candidates_reasoning_binding_exact_fk
    FOREIGN KEY (tenant_id, binding_id, binding_version)
    REFERENCES control.reasoning_route_bindings(tenant_id, binding_id, binding_version),
  ADD CONSTRAINT contribution_candidates_reasoning_model_call_exact_fk
    FOREIGN KEY (
      tenant_id, model_call_id, binding_id, binding_version,
      reasoning_domain_id, profile_version
    )
    REFERENCES ops.model_call_ledger(
      tenant_id, model_call_id, binding_id, binding_version,
      reasoning_domain_id, profile_version
    );

CREATE FUNCTION control.resolve_user_reasoning_admission(
  p_binding_id uuid,
  p_binding_version bigint,
  p_expected_reasoning_domain_id uuid,
  p_expected_purpose text
)
RETURNS TABLE (
  tenant_id uuid,
  binding_id uuid,
  binding_version bigint,
  reasoning_domain_id uuid,
  purpose text,
  route_policy_id uuid,
  route_policy_version bigint,
  profile_id uuid,
  profile_version bigint,
  provider_account_id uuid,
  processor_id text,
  processor_model_id uuid,
  provider_model_id text,
  model_revision text,
  provider_endpoint_id uuid,
  egress_processor_id uuid,
  endpoint_ref text,
  region text,
  service_tier text,
  credential_ref uuid,
  billing_account_id uuid,
  billing_instrument_id uuid,
  provider_health_observation_id bigint,
  account_health_observation_id bigint,
  admitted_at timestamptz
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  RETURN QUERY
  WITH admission_clock AS MATERIALIZED (
    SELECT clock_timestamp() AS admitted_at
  ),
  session_scope AS MATERIALIZED (
    SELECT NULLIF(current_setting('humaux.tenant_id', true), '')::uuid AS tenant_id
  ),
  exact_route AS MATERIALIZED (
    SELECT
      b.tenant_id,
      b.binding_id,
      b.binding_version,
      b.reasoning_domain_id,
      b.purpose,
      b.route_policy_id,
      b.route_policy_version,
      candidate.profile_id,
      candidate.profile_version,
      profile.provider_account_id,
      account.processor_id,
      profile.processor_model_id,
      model.provider_model_id,
      model.model_revision,
      profile.endpoint_id AS provider_endpoint_id,
      endpoint.egress_processor_id,
      endpoint.endpoint_ref,
      endpoint.region,
      endpoint.service_tier,
      profile.credential_ref,
      profile.billing_account_id,
      profile.default_billing_instrument_id AS billing_instrument_id,
      clock.admitted_at
    FROM admission_clock clock
    CROSS JOIN session_scope session
    JOIN control.reasoning_route_bindings b
      ON b.binding_id = p_binding_id
     AND b.binding_version = p_binding_version
     AND b.tenant_id = session.tenant_id
     AND b.reasoning_domain_id = p_expected_reasoning_domain_id
     AND b.purpose = p_expected_purpose
     AND b.effective_from <= clock.admitted_at
     AND b.effective_to IS NULL
    JOIN control.reasoning_route_policies policy
      ON (policy.tenant_id, policy.route_policy_id, policy.policy_version) =
         (b.tenant_id, b.route_policy_id, b.route_policy_version)
     AND policy.purpose = b.purpose
     AND policy.strategy = 'PINNED'
     AND policy.lifecycle_state = 'SERVING'
     AND policy.effective_from <= clock.admitted_at
     AND (policy.effective_to IS NULL OR clock.admitted_at < policy.effective_to)
    JOIN control.reasoning_route_candidates candidate
      ON (candidate.tenant_id, candidate.route_policy_id, candidate.route_policy_version) =
         (policy.tenant_id, policy.route_policy_id, policy.policy_version)
     AND candidate.enabled
     AND candidate.priority = 0
     AND candidate.fallback_class = 'NONE'
     AND 1 = (
       SELECT count(*)
       FROM control.reasoning_route_candidates candidate_count
       WHERE (candidate_count.tenant_id, candidate_count.route_policy_id,
              candidate_count.route_policy_version) =
             (policy.tenant_id, policy.route_policy_id, policy.policy_version)
     )
    JOIN control.reasoning_profiles profile
      ON (profile.tenant_id, profile.profile_id, profile.profile_version) =
         (candidate.tenant_id, candidate.profile_id, candidate.profile_version)
     AND profile.enabled
     AND profile.owner_kind = 'USER'
     AND profile.trust_domain = 'USER_REASONING'
     AND profile.billing_responsibility = 'USER'
    JOIN control.provider_accounts account
      ON account.provider_account_id = profile.provider_account_id
     AND account.tenant_id = profile.tenant_id
     AND account.owner_user_id = profile.owner_user_id
     AND account.trust_domain = profile.trust_domain
     AND account.billing_responsibility = profile.billing_responsibility
     AND account.status = 'ACTIVE'
     AND account.enabled
    JOIN control.provider_endpoints endpoint
      ON endpoint.endpoint_id = profile.endpoint_id
     AND endpoint.tenant_id = profile.tenant_id
     AND endpoint.provider_account_id = profile.provider_account_id
     AND endpoint.egress_processor_id IS NOT NULL
     AND endpoint.enabled
    JOIN control.processor_models model
      ON model.processor_model_id = profile.processor_model_id
     AND model.processor_id = account.processor_id
     AND model.status = 'ACTIVE'
    JOIN control.credentials credential
      ON credential.credential_id = profile.credential_ref
     AND credential.tenant_id = profile.tenant_id
     AND credential.purpose = 'USER_REASONING'
    JOIN control.reasoning_credential_bindings credential_binding
      ON credential_binding.credential_ref = profile.credential_ref
     AND credential_binding.provider_account_id = profile.provider_account_id
     AND credential_binding.tenant_id = profile.tenant_id
     AND credential_binding.owner_user_id = profile.owner_user_id
     AND credential_binding.processor_id = account.processor_id
     AND credential_binding.invocation_eligibility = 'API_CALLABLE'
     AND credential_binding.credential_purpose = 'USER_REASONING'
    LEFT JOIN control.provider_billing_accounts billing
      ON billing.billing_account_id = profile.billing_account_id
     AND billing.tenant_id = profile.tenant_id
     AND billing.provider_account_id = profile.provider_account_id
     AND billing.owner_user_id = profile.owner_user_id
     AND billing.owner_kind = 'USER'
     AND billing.payer_kind = 'USER'
     AND billing.billing_responsibility = 'USER'
     AND billing.enabled
    LEFT JOIN control.provider_billing_instruments instrument
      ON instrument.billing_instrument_id = profile.default_billing_instrument_id
     AND instrument.tenant_id = profile.tenant_id
     AND instrument.billing_account_id = profile.billing_account_id
     AND instrument.owner_user_id = profile.owner_user_id
     AND instrument.payer_user_id = profile.owner_user_id
     AND instrument.owner_kind = 'USER'
     AND instrument.payer_kind = 'USER'
     AND instrument.invocation_eligibility = 'API_CALLABLE'
     AND instrument.coverage_processor_id = model.processor_id
     AND instrument.coverage_provider_model_id = model.provider_model_id
     AND instrument.coverage_model_revision IS NOT DISTINCT FROM model.model_revision
     AND instrument.coverage_region = endpoint.region
     AND instrument.coverage_service_tier = endpoint.service_tier
     AND instrument.valid_from <= clock.admitted_at
     AND (instrument.valid_to IS NULL OR clock.admitted_at < instrument.valid_to)
     AND instrument.enabled
    WHERE (profile.billing_account_id IS NULL OR billing.billing_account_id IS NOT NULL)
      AND (profile.default_billing_instrument_id IS NULL OR instrument.billing_instrument_id IS NOT NULL)
  )
  SELECT
    route.tenant_id,
    route.binding_id,
    route.binding_version,
    route.reasoning_domain_id,
    route.purpose,
    route.route_policy_id,
    route.route_policy_version,
    route.profile_id,
    route.profile_version,
    route.provider_account_id,
    route.processor_id,
    route.processor_model_id,
    route.provider_model_id,
    route.model_revision,
    route.provider_endpoint_id,
    route.egress_processor_id,
    route.endpoint_ref,
    route.region,
    route.service_tier,
    route.credential_ref,
    route.billing_account_id,
    route.billing_instrument_id,
    provider_health.observation_id,
    account_health.observation_id,
    route.admitted_at
  FROM exact_route route
  JOIN LATERAL (
    SELECT observation.observation_id,
           observation.verdict,
           observation.observed_at,
           observation.valid_until
    FROM ops.reasoning_provider_health_observations observation
    WHERE observation.tenant_id = route.tenant_id
      AND observation.processor_id = route.processor_id
      AND observation.processor_model_id = route.processor_model_id
      AND observation.provider_model_id = route.provider_model_id
      AND observation.model_revision IS NOT DISTINCT FROM route.model_revision
      AND observation.provider_endpoint_id = route.provider_endpoint_id
      AND observation.endpoint_ref = route.endpoint_ref
      AND observation.region = route.region
      AND observation.service_tier = route.service_tier
    ORDER BY observation.observed_at DESC, observation.observation_id DESC
    LIMIT 1
  ) provider_health ON true
  JOIN LATERAL (
    SELECT observation.observation_id,
           observation.account_verdict,
           observation.credential_verdict,
           observation.billing_account_verdict,
           observation.billing_instrument_verdict,
           observation.observed_at,
           observation.valid_until
    FROM ops.reasoning_account_health_observations observation
    WHERE observation.tenant_id = route.tenant_id
      AND observation.provider_account_id = route.provider_account_id
      AND observation.credential_ref = route.credential_ref
      AND observation.billing_account_id IS NOT DISTINCT FROM route.billing_account_id
      AND observation.billing_instrument_id IS NOT DISTINCT FROM route.billing_instrument_id
    ORDER BY observation.observed_at DESC, observation.observation_id DESC
    LIMIT 1
  ) account_health ON true
  WHERE provider_health.verdict = 'HEALTHY'
    AND provider_health.observed_at <= route.admitted_at
    AND route.admitted_at < provider_health.valid_until
    AND account_health.account_verdict = 'HEALTHY'
    AND account_health.credential_verdict = 'VALID'
    AND account_health.billing_account_verdict IS NOT DISTINCT FROM
      CASE WHEN route.billing_account_id IS NULL THEN NULL ELSE 'ENABLED' END
    AND account_health.billing_instrument_verdict IS NOT DISTINCT FROM
      CASE WHEN route.billing_instrument_id IS NULL THEN NULL ELSE 'ENABLED' END
    AND account_health.observed_at <= route.admitted_at
    AND route.admitted_at < account_health.valid_until;
END;
$$;

CREATE FUNCTION ops.reasoning_model_call_validate()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  IF NEW.purpose IS DISTINCT FROM 'CONTRIBUTION_DEIDENTIFY' THEN
    RETURN NEW;
  END IF;
  IF NEW.status <> 'RESERVED'
     OR num_nonnulls(
          NEW.input_tokens, NEW.billable_tokens, NEW.candidate_count,
          NEW.candidate_tokens, NEW.cache_hit, NEW.latency_ms, NEW.actual_cost,
          NEW.error_class, NEW.provider_request_id
        ) <> 0
     OR NEW.admitted_at > clock_timestamp() THEN
    RAISE EXCEPTION 'reasoning model call must begin as an unfinalized USER-paid reservation'
      USING ERRCODE = '23514';
  END IF;
  IF NOT EXISTS (
    SELECT 1
      FROM control.resolve_user_reasoning_admission(
        NEW.binding_id,
        NEW.binding_version,
        NEW.reasoning_domain_id,
        'CONTRIBUTION_DEIDENTIFY'
      ) admission
      JOIN ops.reasoning_provider_health_observations provider_health
        ON provider_health.tenant_id = admission.tenant_id
       AND provider_health.observation_id = admission.provider_health_observation_id
      JOIN ops.reasoning_account_health_observations account_health
        ON account_health.tenant_id = admission.tenant_id
       AND account_health.observation_id = admission.account_health_observation_id
     WHERE admission.tenant_id = NEW.tenant_id
       AND admission.binding_id = NEW.binding_id
       AND admission.binding_version = NEW.binding_version
       AND admission.reasoning_domain_id = NEW.reasoning_domain_id
       AND admission.route_policy_id = NEW.route_policy_id
       AND admission.route_policy_version = NEW.route_policy_version
       AND admission.profile_id = NEW.profile_id
       AND admission.profile_version = NEW.profile_version
       AND admission.provider_account_id = NEW.provider_account_id
       AND admission.processor_id = NEW.provider
       AND admission.provider_model_id = NEW.model
       AND admission.model_revision IS NOT DISTINCT FROM NEW.model_revision
       AND admission.provider_endpoint_id = NEW.provider_endpoint_id
       AND admission.egress_processor_id = NEW.egress_processor_id
       AND admission.credential_ref = NEW.credential_ref
       AND admission.billing_account_id IS NOT DISTINCT FROM NEW.billing_account_id
       AND admission.billing_instrument_id IS NOT DISTINCT FROM NEW.billing_instrument_id
       AND admission.provider_health_observation_id = NEW.provider_health_observation_id
       AND admission.account_health_observation_id = NEW.account_health_observation_id
       AND NEW.billing_responsibility = 'USER'
       AND provider_health.observed_at <= NEW.admitted_at
       AND NEW.admitted_at < provider_health.valid_until
       AND provider_health.verdict = 'HEALTHY'
       AND account_health.observed_at <= NEW.admitted_at
       AND NEW.admitted_at < account_health.valid_until
       AND account_health.account_verdict = 'HEALTHY'
       AND account_health.credential_verdict = 'VALID'
       AND account_health.billing_account_verdict IS NOT DISTINCT FROM
         CASE WHEN NEW.billing_account_id IS NULL THEN NULL ELSE 'ENABLED' END
       AND account_health.billing_instrument_verdict IS NOT DISTINCT FROM
         CASE WHEN NEW.billing_instrument_id IS NULL THEN NULL ELSE 'ENABLED' END
  ) THEN
    RAISE EXCEPTION 'reasoning model call snapshot must equal one current exact admission'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER reasoning_model_call_validate
  BEFORE INSERT ON ops.model_call_ledger
  FOR EACH ROW EXECUTE FUNCTION ops.reasoning_model_call_validate();

CREATE OR REPLACE FUNCTION ops.model_call_ledger_guard_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
  IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
    RAISE EXCEPTION 'ops.model_call_ledger is append-only (§19.1) — % not permitted', TG_OP
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.request_id IS DISTINCT FROM NEW.request_id
     OR OLD.tenant_id IS DISTINCT FROM NEW.tenant_id
     OR OLD.workspace_id IS DISTINCT FROM NEW.workspace_id
     OR OLD.purpose IS DISTINCT FROM NEW.purpose
     OR OLD.provider IS DISTINCT FROM NEW.provider
     OR OLD.model IS DISTINCT FROM NEW.model
     OR OLD.model_revision IS DISTINCT FROM NEW.model_revision
     OR OLD.called_at IS DISTINCT FROM NEW.called_at
     OR OLD.estimated_cost IS DISTINCT FROM NEW.estimated_cost
     OR OLD.reasoning_domain_id IS DISTINCT FROM NEW.reasoning_domain_id
     OR OLD.call_kind IS DISTINCT FROM NEW.call_kind
     OR OLD.intent_sha256 IS DISTINCT FROM NEW.intent_sha256
     OR OLD.binding_id IS DISTINCT FROM NEW.binding_id
     OR OLD.binding_version IS DISTINCT FROM NEW.binding_version
     OR OLD.route_policy_id IS DISTINCT FROM NEW.route_policy_id
     OR OLD.route_policy_version IS DISTINCT FROM NEW.route_policy_version
     OR OLD.profile_id IS DISTINCT FROM NEW.profile_id
     OR OLD.profile_version IS DISTINCT FROM NEW.profile_version
     OR OLD.provider_account_id IS DISTINCT FROM NEW.provider_account_id
     OR OLD.provider_endpoint_id IS DISTINCT FROM NEW.provider_endpoint_id
     OR OLD.egress_processor_id IS DISTINCT FROM NEW.egress_processor_id
     OR OLD.credential_ref IS DISTINCT FROM NEW.credential_ref
     OR OLD.billing_account_id IS DISTINCT FROM NEW.billing_account_id
     OR OLD.billing_instrument_id IS DISTINCT FROM NEW.billing_instrument_id
     OR OLD.provider_health_observation_id IS DISTINCT FROM NEW.provider_health_observation_id
     OR OLD.account_health_observation_id IS DISTINCT FROM NEW.account_health_observation_id
     OR OLD.billing_responsibility IS DISTINCT FROM NEW.billing_responsibility
     OR OLD.admitted_at IS DISTINCT FROM NEW.admitted_at THEN
    RAISE EXCEPTION 'ops.model_call_ledger identity/reservation columns are immutable after INSERT (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.status <> 'RESERVED' AND NEW.status IS DISTINCT FROM OLD.status THEN
    RAISE EXCEPTION 'ops.model_call_ledger already finalized — status cannot change again (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF (OLD.input_tokens IS NOT NULL AND OLD.input_tokens IS DISTINCT FROM NEW.input_tokens)
  OR (OLD.billable_tokens IS NOT NULL AND OLD.billable_tokens IS DISTINCT FROM NEW.billable_tokens)
  OR (OLD.candidate_count IS NOT NULL AND OLD.candidate_count IS DISTINCT FROM NEW.candidate_count)
  OR (OLD.candidate_tokens IS NOT NULL AND OLD.candidate_tokens IS DISTINCT FROM NEW.candidate_tokens)
  OR (OLD.cache_hit IS NOT NULL AND OLD.cache_hit IS DISTINCT FROM NEW.cache_hit)
  OR (OLD.latency_ms IS NOT NULL AND OLD.latency_ms IS DISTINCT FROM NEW.latency_ms)
  OR (OLD.actual_cost IS NOT NULL AND OLD.actual_cost IS DISTINCT FROM NEW.actual_cost)
  OR (OLD.error_class IS NOT NULL AND OLD.error_class IS DISTINCT FROM NEW.error_class)
  OR (OLD.provider_request_id IS NOT NULL AND OLD.provider_request_id IS DISTINCT FROM NEW.provider_request_id) THEN
    RAISE EXCEPTION 'ops.model_call_ledger outcome columns can only be set once, from NULL (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.purpose = 'CONTRIBUTION_DEIDENTIFY' AND NEW.actual_cost IS NOT NULL THEN
    RAISE EXCEPTION 'USER-paid reasoning calls never record platform actual cost'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION ops.data_disclosure_reasoning_model_call_validate()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  IF NEW.model_call_id IS NULL THEN
    RETURN NEW;
  END IF;
  IF NEW.purpose <> 'USER_REASONING' OR NOT EXISTS (
    SELECT 1
      FROM ops.model_call_ledger call
     WHERE call.tenant_id = NEW.tenant_id
       AND call.model_call_id = NEW.model_call_id
       AND call.purpose = 'CONTRIBUTION_DEIDENTIFY'
       AND call.status = 'RESERVED'
       AND call.egress_processor_id = NEW.processor_id
  ) THEN
    RAISE EXCEPTION 'reasoning disclosure must bind the same RESERVED model call'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER data_disclosure_reasoning_model_call_validate
  BEFORE INSERT ON ops.data_disclosures
  FOR EACH ROW EXECUTE FUNCTION ops.data_disclosure_reasoning_model_call_validate();

CREATE OR REPLACE FUNCTION ops.data_disclosures_guard_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
  IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
    RAISE EXCEPTION 'ops.data_disclosures is append-only (§7.4) — % not permitted', TG_OP
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.grant_id IS DISTINCT FROM NEW.grant_id
     OR OLD.tenant_id IS DISTINCT FROM NEW.tenant_id
     OR OLD.scope_kind IS DISTINCT FROM NEW.scope_kind
     OR OLD.scope_id IS DISTINCT FROM NEW.scope_id
     OR OLD.processor_id IS DISTINCT FROM NEW.processor_id
     OR OLD.region IS DISTINCT FROM NEW.region
     OR OLD.data_class IS DISTINCT FROM NEW.data_class
     OR OLD.purpose IS DISTINCT FROM NEW.purpose
     OR OLD.payload_sha256 IS DISTINCT FROM NEW.payload_sha256
     OR OLD.payload_bytes IS DISTINCT FROM NEW.payload_bytes
     OR OLD.reserved_at IS DISTINCT FROM NEW.reserved_at
     OR OLD.model_call_id IS DISTINCT FROM NEW.model_call_id THEN
    RAISE EXCEPTION 'ops.data_disclosures identity/reservation columns are immutable after INSERT (§7.4)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.finalized_at IS NOT NULL
     AND (OLD.finalized_at IS DISTINCT FROM NEW.finalized_at
          OR OLD.outcome IS DISTINCT FROM NEW.outcome) THEN
    RAISE EXCEPTION 'ops.data_disclosures already finalized — finalized_at/outcome cannot change again (§7.4)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.deletion_requested_at IS NOT NULL
     AND OLD.deletion_requested_at IS DISTINCT FROM NEW.deletion_requested_at THEN
    RAISE EXCEPTION 'ops.data_disclosures.deletion_requested_at is monotonic (§37) — cannot change once set'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.deletion_confirmed_at IS NOT NULL
     AND OLD.deletion_confirmed_at IS DISTINCT FROM NEW.deletion_confirmed_at THEN
    RAISE EXCEPTION 'ops.data_disclosures.deletion_confirmed_at is monotonic (§37) — cannot change once set'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION staging.contribution_candidate_reasoning_route_validate()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  IF NEW.model_call_id IS NULL THEN
    RETURN NEW;
  END IF;
  IF NOT EXISTS (
    SELECT 1
      FROM ops.model_call_ledger call
      JOIN ops.data_disclosures disclosure
        ON disclosure.tenant_id = call.tenant_id
       AND disclosure.model_call_id = call.model_call_id
       AND disclosure.processor_id = call.egress_processor_id
       AND disclosure.finalized_at IS NOT NULL
       AND disclosure.outcome = 'SUCCESS'
      JOIN control.private_reasoning_domains domain
        ON domain.tenant_id = call.tenant_id
       AND domain.reasoning_domain_id = call.reasoning_domain_id
     WHERE call.tenant_id = NEW.tenant_id
       AND call.model_call_id = NEW.model_call_id
       AND call.purpose = 'CONTRIBUTION_DEIDENTIFY'
       AND call.call_kind = 'TYPED_ASSESSMENT'
       AND call.status = 'SUCCEEDED'
       AND call.binding_id = NEW.binding_id
       AND call.binding_version = NEW.binding_version
       AND call.reasoning_domain_id = NEW.reasoning_domain_id
       AND call.profile_version = NEW.profile_version
       AND domain.owner_user_id = NEW.user_id
  ) THEN
    RAISE EXCEPTION 'reasoning candidate must reference its exact successful model call'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER contribution_candidate_reasoning_route_validate
  BEFORE INSERT OR UPDATE OF binding_id, binding_version, model_call_id,
    reasoning_domain_id, profile_version
  ON staging.contribution_candidates
  FOR EACH ROW EXECUTE FUNCTION staging.contribution_candidate_reasoning_route_validate();

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

-- 0108's release authorization guard remains legacy-compatible for historical candidates,
-- while R3 candidates prove profile/domain authority from the immutable successful call and
-- its terminal successful disclosure instead of consulting the mutable legacy profile table.
CREATE OR REPLACE FUNCTION staging.guard_contribution_authorization()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SECURITY INVOKER
SET search_path = pg_catalog
AS $$
DECLARE c staging.contribution_candidates; principal uuid; workspaces uuid[];
BEGIN
  IF NEW.candidate_id IS NULL THEN RETURN NEW; END IF;
  PERFORM ops.lock_contribution_inputs();
  SELECT * INTO c FROM staging.contribution_candidates WHERE candidate_id=NEW.candidate_id;
  IF c.candidate_id IS NULL OR NOT (c.policy_snapshot ? 'principal_id') OR
     jsonb_typeof(c.policy_snapshot->'allowed_workspace_ids') IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'candidate lacks authenticated principal scope' USING ERRCODE='42501';
  END IF;
  principal:=(c.policy_snapshot->>'principal_id')::uuid;
  SELECT coalesce(array_agg(value::uuid),'{}'::uuid[]) INTO workspaces
    FROM jsonb_array_elements_text(c.policy_snapshot->'allowed_workspace_ids');
  IF principal IS NULL OR NOT EXISTS(
      SELECT 1 FROM control.private_reasoning_domains d
       WHERE d.reasoning_domain_id=c.reasoning_domain_id
         AND d.tenant_id=c.tenant_id
         AND d.owner_user_id=c.user_id
         AND d.status='ACTIVE'
    ) OR (
      c.model_call_id IS NULL AND NOT EXISTS(
        SELECT 1 FROM control.user_reasoning_profiles p
         WHERE p.tenant_id=c.tenant_id
           AND p.user_id=c.user_id
           AND p.enabled
           AND p.profile_version=c.profile_version
           AND p.capabilities @> ARRAY['TEXT']::text[]
      )
    ) OR (
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
    ) OR (principal<>c.user_id AND NOT EXISTS(
      SELECT 1 FROM control.reasoning_domain_grants g
      WHERE g.reasoning_domain_id=c.reasoning_domain_id AND g.principal_id=principal
        AND g.revoked_at IS NULL AND g.expires_at>clock_timestamp()
        AND g.purposes @> ARRAY['USER_REASONING']::text[]
        AND (g.workspace_id IS NULL OR g.workspace_id=ANY(workspaces)))) THEN
    RAISE EXCEPTION 'domain, exact reasoning call or USER_REASONING grant is stale'
      USING ERRCODE='42501';
  END IF;
  IF EXISTS(SELECT 1 FROM staging.contribution_candidate_sources s
    WHERE s.candidate_id=c.candidate_id AND s.memory_id IS NOT NULL
      AND NOT EXISTS(SELECT 1 FROM private.memory_evidence me WHERE me.memory_id=s.memory_id)) THEN
    RAISE EXCEPTION 'memory lacks backing Evidence' USING ERRCODE='42501';
  END IF;
  IF EXISTS(
    WITH input_rows AS (
      SELECT m.memory_id AS id,m.tenant_id,m.visibility_class,m.visibility_user_id,
        m.visibility_workspace_id,c.reasoning_domain_id AS reasoning_domain_id
      FROM staging.contribution_candidate_sources s
      LEFT JOIN private.memory_records m ON m.memory_id=s.memory_id
      WHERE s.candidate_id=c.candidate_id AND s.memory_id IS NOT NULL
      UNION ALL
      SELECT e.evidence_id,e.tenant_id,e.visibility_class,e.visibility_user_id,
        e.visibility_workspace_id,e.reasoning_domain_id
      FROM staging.contribution_candidate_sources s
      LEFT JOIN private.evidence_objects e ON e.evidence_id=s.evidence_id
      WHERE s.candidate_id=c.candidate_id AND s.evidence_id IS NOT NULL
      UNION ALL
      SELECT e.evidence_id,e.tenant_id,e.visibility_class,e.visibility_user_id,
        e.visibility_workspace_id,e.reasoning_domain_id
      FROM staging.contribution_candidate_sources s
      JOIN private.memory_evidence me ON me.memory_id=s.memory_id
      LEFT JOIN private.evidence_objects e ON e.evidence_id=me.evidence_id
      WHERE s.candidate_id=c.candidate_id
    ) SELECT 1 FROM input_rows i WHERE i.id IS NULL OR i.tenant_id IS DISTINCT FROM c.tenant_id
      OR i.reasoning_domain_id IS DISTINCT FROM c.reasoning_domain_id
      OR NOT coalesce(CASE i.visibility_class
          WHEN 'TENANT_SHARED' THEN true
          WHEN 'USER_PRIVATE' THEN i.visibility_user_id=c.user_id
          WHEN 'WORKSPACE_SHARED' THEN i.visibility_workspace_id=ANY(workspaces)
          ELSE false END,false)
      OR (principal<>c.user_id AND NOT EXISTS(
        SELECT 1 FROM control.reasoning_domain_grants g
        WHERE g.reasoning_domain_id=c.reasoning_domain_id AND g.principal_id=principal
          AND g.revoked_at IS NULL AND g.expires_at>clock_timestamp()
          AND g.purposes @> ARRAY['USER_REASONING']::text[]
          AND (g.workspace_id IS NULL OR (g.workspace_id=i.visibility_workspace_id
            AND g.workspace_id=ANY(workspaces)))))
  ) THEN
    RAISE EXCEPTION 'candidate source or backing Evidence is no longer authorized'
      USING ERRCODE='42501';
  END IF;
  RETURN NEW;
END;
$$;

ALTER FUNCTION ops.reasoning_health_observation_reject_mutation() OWNER TO role_migration_owner;
ALTER FUNCTION ops.reasoning_model_call_validate() OWNER TO role_migration_owner;
ALTER FUNCTION ops.model_call_ledger_guard_mutation() OWNER TO role_migration_owner;
ALTER FUNCTION ops.data_disclosure_reasoning_model_call_validate() OWNER TO role_migration_owner;
ALTER FUNCTION ops.data_disclosures_guard_mutation() OWNER TO role_migration_owner;
ALTER FUNCTION staging.contribution_candidate_reasoning_route_validate()
  OWNER TO role_migration_owner;
ALTER FUNCTION staging.guard_finalized_contribution() OWNER TO role_migration_owner;
ALTER FUNCTION staging.guard_contribution_authorization() OWNER TO role_migration_owner;
ALTER FUNCTION control.resolve_user_reasoning_admission(uuid, bigint, uuid, text)
  OWNER TO role_migration_owner;

ALTER TABLE ops.reasoning_provider_health_observations OWNER TO role_migration_owner;
ALTER TABLE ops.reasoning_account_health_observations OWNER TO role_migration_owner;
ALTER TABLE ops.reasoning_provider_health_observations ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.reasoning_provider_health_observations FORCE ROW LEVEL SECURITY;
ALTER TABLE ops.reasoning_account_health_observations ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.reasoning_account_health_observations FORCE ROW LEVEL SECURITY;

CREATE POLICY reasoning_provider_health_owner
  ON ops.reasoning_provider_health_observations
  TO role_migration_owner
  USING (true)
  WITH CHECK (true);
CREATE POLICY reasoning_account_health_owner
  ON ops.reasoning_account_health_observations
  TO role_migration_owner
  USING (true)
  WITH CHECK (true);

REVOKE ALL ON TABLE
  ops.reasoning_provider_health_observations,
  ops.reasoning_account_health_observations
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker,
  role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance;
REVOKE ALL ON FUNCTION
  ops.reasoning_health_observation_reject_mutation(),
  ops.reasoning_model_call_validate(),
  ops.data_disclosure_reasoning_model_call_validate(),
  staging.contribution_candidate_reasoning_route_validate(),
  control.resolve_user_reasoning_admission(uuid, bigint, uuid, text)
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker,
  role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance;
GRANT EXECUTE ON FUNCTION control.resolve_user_reasoning_admission(uuid, bigint, uuid, text)
  TO role_private_worker;
