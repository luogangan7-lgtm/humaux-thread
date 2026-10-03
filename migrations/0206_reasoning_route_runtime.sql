-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0205). Card 33b slice 1 / ADR-0060
-- (profile-driven reasoning routing, R3 for PRIVATE_DISTILL_* and PRIVATE_CONSOLIDATE).
-- Spec: §11.2 (Profile capabilities, processing_run snapshot), §11.2.2 R3, §11.2.4 (execution
-- identity, credential and billing authority), §11.2.5 (ledger egress id = disclosure processor id
-- = resolver endpoint value in one reserve transaction; health observations), §11.5 (BYOK:
-- billing_responsibility USER, actual_cost NULL when unknown), §19.1, §67.2.
--
-- What this migration changes and why:
--  1. The §11.2 capability closed set gains JSON_OBJECT (research amendment 2: an endpoint that answers
--     `response_format: {"type":"json_object"}`). The three capability CHECKs (0048, 0128 x2, last
--     defined by 0195) are re-created under the same names. Rust twin:
--     humaux_adapters::byok::ReasoningCapability::ALL (contract tests read this file and the live
--     constraints).
--  2. control.reasoning_profiles.request_extras (research amendment 1): vendor request fields are data
--     of a Profile@version, merged into the request body by the adapter. CHECK: a JSON object, bounded
--     size, none of the adapter-owned keys. The key list is humaux_adapters::byok::
--     ADAPTER_OWNED_REQUEST_KEYS (one const; a contract test pins this file's list to it). Existing
--     profiles get '{}' (no extras), which is what the adapter sent for them so far apart from the
--     capability-selected fields.
--  3. control.reasoning_profile_capabilities (ADR-0060 D-A): the admitted Profile@version's declared
--     capabilities, read by the one admission wrapper in the same transaction as the 0130 resolver
--     (whose RETURNS TABLE stays frozen: four SQL callers and architecture-check parse it).
--  4. ops.model_call_ledger (ADR-0060 D-I, ruling E2): private reasoning rows now carry the admitted
--     route. 0166's header called these hops "platform-paid" and kept every 0130 reasoning column
--     NULL; that described one process-level key serving every tenant. A private call is paid by the
--     account of the credential its admitted route names (§11.2.4), so the CHECK gains a private arm:
--     the 14 route columns all NULL (rows reserved before this migration) or all non-NULL with
--     billing_responsibility USER and no cost estimate. The BEFORE INSERT validator requires the
--     non-NULL form for every new private row, with no role exemption, and proves it equals one
--     current exact admission (health ids recorded and checked for validity, not compared).
--  5. private.processing_runs names its Profile@version (ADR-0060 D-K); rows before this stay NULL.
--  6. ADR-0060 D-N: a private reservation is one transaction holding the ledger row and its disclosure;
--     the disclosure validator's purpose list covers the private purposes, and a deferred constraint
--     trigger refuses a private ledger row that commits without its matching disclosure. 0166's
--     "distill / consolidation disclosures keep model_call_id NULL" is narrowed by this decision.
--  7. ops.claim_derived_work_v2 (ADR-0060 D-F): tenant order = fewest held slots, then least recently
--     served (was least recently served only, ADR-0058 D-B).
--  8. control.reasoning_credential_accounts (ADR-0060 D-J): the vendor identity behind each
--     credential reference, for the worker's boot check that one secret serves one vendor account.
--  9. control.observe_reasoning_route_health (ADR-0060 ruling E3): the worker-observed health writer,
--     so a route with traffic never expires and a rejected key denies the next admission at once.
--
-- Locks: ACCESS EXCLUSIVE on control.user_reasoning_profiles, control.processor_models,
-- control.reasoning_profiles, ops.model_call_ledger, ops.data_disclosures (trigger only) and
-- private.processing_runs for one CHECK / FK validation scan each. No table grant and no RLS change.

-- ---------------------------------------------------------------------------------------------
-- 1. Capability closed set + JSON_OBJECT (same constraint names as 0195).
-- ---------------------------------------------------------------------------------------------
ALTER TABLE control.user_reasoning_profiles
  DROP CONSTRAINT user_reasoning_profiles_capabilities_known,
  ADD CONSTRAINT user_reasoning_profiles_capabilities_known CHECK (
    capabilities <@ ARRAY['TEXT', 'VISION', 'STRUCTURED_OUTPUT', 'TOKEN_USAGE', 'TOOL_CALLS', 'REASONING_SPLIT', 'JSON_OBJECT']::text[]
    AND array_length(capabilities, 1) > 0
  );

ALTER TABLE control.processor_models
  DROP CONSTRAINT processor_models_capabilities_check,
  ADD CONSTRAINT processor_models_capabilities_check CHECK (
    cardinality(capabilities) > 0
    AND capabilities <@ ARRAY['TEXT', 'VISION', 'STRUCTURED_OUTPUT', 'TOKEN_USAGE', 'TOOL_CALLS', 'REASONING_SPLIT', 'JSON_OBJECT']::text[]
  );

ALTER TABLE control.reasoning_profiles
  DROP CONSTRAINT reasoning_profiles_capabilities_check,
  ADD CONSTRAINT reasoning_profiles_capabilities_check CHECK (
    cardinality(capabilities) > 0
    AND capabilities <@ ARRAY['TEXT', 'VISION', 'STRUCTURED_OUTPUT', 'TOKEN_USAGE', 'TOOL_CALLS', 'REASONING_SPLIT', 'JSON_OBJECT']::text[]
  );

-- ---------------------------------------------------------------------------------------------
-- 2. Profile request extras. The 0128 state-only trigger keeps the column immutable per
--    Profile@version (only enabled / updated_at may change).
-- ---------------------------------------------------------------------------------------------
-- ponytail: 4096-byte bound on the serialized object, raise it with a forward migration when a bound
-- vendor needs a larger field set.
ALTER TABLE control.reasoning_profiles
  ADD COLUMN request_extras jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD CONSTRAINT reasoning_profiles_request_extras_check CHECK (
    jsonb_typeof(request_extras) = 'object'
    AND octet_length(request_extras::text) <= 4096
    AND NOT (request_extras ?| ARRAY['model', 'messages', 'tools', 'tool_choice', 'functions', 'function_call', 'response_format', 'stream', 'stream_options', 'max_tokens', 'max_completion_tokens']::text[])
  );

COMMENT ON COLUMN control.reasoning_profiles.request_extras IS
  'ADR-0060 research amendment 1: vendor request fields of this Profile@version, merged into the '
  'request body by the adapter. Never an adapter-owned key (humaux_adapters::byok::'
  'ADAPTER_OWNED_REQUEST_KEYS); immutable per version like every other profile column.';

-- ---------------------------------------------------------------------------------------------
-- 3. Declared capabilities of one admitted Profile@version (ADR-0060 D-A).
--    Caller: adapters::reasoning_route_admission::resolve_user_reasoning_admission
--    (role_private_worker), in the transaction that just ran the 0130 resolver.
--    Fence: the humaux.tenant_id GUC (and FORCE RLS on the table): another tenant's profile is NULL.
--    Owner: role_migration_owner.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.reasoning_profile_capabilities(p_profile_id uuid, p_profile_version bigint)
RETURNS text[]
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  SELECT p.capabilities
    FROM control.reasoning_profiles p
   WHERE p.tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
     AND p.profile_id = p_profile_id
     AND p.profile_version = p_profile_version
$$;

ALTER FUNCTION control.reasoning_profile_capabilities(uuid, bigint) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.reasoning_profile_capabilities(uuid, bigint)
  FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
       role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT EXECUTE ON FUNCTION control.reasoning_profile_capabilities(uuid, bigint) TO role_private_worker;
COMMENT ON FUNCTION control.reasoning_profile_capabilities(uuid, bigint) IS
  '§11.2 / ADR-0060 D-A: declared capabilities of one Profile@version of the session tenant; NULL for '
  'any other tenant. Caller: adapters::reasoning_route_admission. EXECUTE: role_private_worker only.';

-- ---------------------------------------------------------------------------------------------
-- 4. Ledger snapshot shape (ADR-0060 D-I). Arm A is 0130 verbatim; arm B is the retrieval plane;
--    arm C is the private plane.
-- ---------------------------------------------------------------------------------------------
ALTER TABLE ops.model_call_ledger
  DROP CONSTRAINT model_call_ledger_reasoning_snapshot_shape,
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
    )
    OR (
      (purpose IS NULL OR purpose IN ('query_rewrite', 'embedding', 'rerank'))
      AND num_nonnulls(
        reasoning_domain_id, call_kind, intent_sha256, binding_id, binding_version,
        route_policy_id, route_policy_version, profile_id, profile_version, provider_account_id,
        provider_endpoint_id, egress_processor_id, credential_ref, billing_account_id,
        billing_instrument_id, provider_health_observation_id, account_health_observation_id,
        billing_responsibility, admitted_at
      ) = 0
    )
    OR (
      purpose IN ('PRIVATE_DISTILL_TEXT', 'PRIVATE_DISTILL_VISION', 'PRIVATE_CONSOLIDATE')
      AND call_kind IS NULL
      AND intent_sha256 IS NULL
      AND (
        -- rows reserved before 0206 (no route recorded); the validator refuses this form for every
        -- INSERT after it, so only the ADD below can accept it.
        num_nonnulls(
          reasoning_domain_id, binding_id, binding_version, route_policy_id, route_policy_version,
          profile_id, profile_version, provider_account_id, provider_endpoint_id,
          egress_processor_id, credential_ref, provider_health_observation_id,
          account_health_observation_id, admitted_at, billing_account_id, billing_instrument_id,
          billing_responsibility
        ) = 0
        OR (
          num_nonnulls(
            reasoning_domain_id, binding_id, binding_version, route_policy_id, route_policy_version,
            profile_id, profile_version, provider_account_id, provider_endpoint_id,
            egress_processor_id, credential_ref, provider_health_observation_id,
            account_health_observation_id, admitted_at
          ) = 14
          AND binding_version > 0
          AND route_policy_version > 0
          AND profile_version > 0
          AND billing_responsibility = 'USER'
          AND (billing_account_id IS NOT NULL OR billing_instrument_id IS NULL)
          AND estimated_cost IS NULL
          AND actual_cost IS NULL
        )
      )
    )
  );

COMMENT ON COLUMN ops.model_call_ledger.purpose IS
  '§19.1 purpose. Closed set, mirrored by humaux_domain::ledger::ModelCallPurpose (0166). '
  'Retrieval plane: query_rewrite/embedding/rerank (no reasoning columns). Private reasoning plane: '
  'CONTRIBUTION_DEIDENTIFY (0130) and PRIVATE_DISTILL_TEXT / PRIVATE_DISTILL_VISION / '
  'PRIVATE_CONSOLIDATE (0206, ADR-0060 D-I): USER-paid, each row carries its admitted route.';

-- ---------------------------------------------------------------------------------------------
-- 5. BEFORE INSERT validator. Caller: the reasoning_model_call_validate trigger (0130) on every
--    INSERT, every role. Fence: no current_user / role branch; a private row must name one current
--    exact admission of the session tenant. Owner: role_migration_owner (SECURITY DEFINER, so the
--    resolver and the health tables are read as owner). The CONTRIBUTION branch is 0130 verbatim.
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ops.reasoning_model_call_validate()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  IF NEW.purpose IN ('PRIVATE_DISTILL_TEXT', 'PRIVATE_DISTILL_VISION', 'PRIVATE_CONSOLIDATE') THEN
    -- ADR-0060 D-I: the all-NULL form of the CHECK's private arm is for rows that predate 0206 only.
    IF num_nonnulls(
         NEW.reasoning_domain_id, NEW.binding_id, NEW.binding_version, NEW.route_policy_id,
         NEW.route_policy_version, NEW.profile_id, NEW.profile_version, NEW.provider_account_id,
         NEW.provider_endpoint_id, NEW.egress_processor_id, NEW.credential_ref,
         NEW.provider_health_observation_id, NEW.account_health_observation_id, NEW.admitted_at
       ) <> 14 THEN
      RAISE EXCEPTION 'private reasoning model call must carry its admitted route'
        USING ERRCODE = '23514';
    END IF;
    IF NEW.status <> 'RESERVED'
       OR num_nonnulls(
            NEW.input_tokens, NEW.output_tokens, NEW.billable_tokens, NEW.candidate_count,
            NEW.candidate_tokens, NEW.cache_hit, NEW.latency_ms, NEW.actual_cost,
            NEW.error_class, NEW.provider_request_id
          ) <> 0
       OR NEW.admitted_at > clock_timestamp() THEN
      RAISE EXCEPTION 'reasoning model call must begin as an unfinalized USER-paid reservation'
        USING ERRCODE = '23514';
    END IF;
    -- §11.2.5 / ADR-0060 D-I: identity equals the current admission; the health ids are recorded and
    -- must have admitted this route at admitted_at, but a newer observation does not refuse the row.
    IF NOT EXISTS (
      SELECT 1
        FROM control.resolve_user_reasoning_admission(
          NEW.binding_id, NEW.binding_version, NEW.reasoning_domain_id, NEW.purpose
        ) admission
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
         AND NEW.billing_responsibility = 'USER'
    ) OR NOT EXISTS (
      SELECT 1
        FROM ops.reasoning_provider_health_observations provider_health
        JOIN ops.reasoning_account_health_observations account_health
          ON account_health.tenant_id = provider_health.tenant_id
         AND account_health.observation_id = NEW.account_health_observation_id
       WHERE provider_health.tenant_id = NEW.tenant_id
         AND provider_health.observation_id = NEW.provider_health_observation_id
         AND provider_health.processor_id = NEW.provider
         AND provider_health.provider_model_id = NEW.model
         AND provider_health.model_revision IS NOT DISTINCT FROM NEW.model_revision
         AND provider_health.provider_endpoint_id = NEW.provider_endpoint_id
         AND provider_health.verdict = 'HEALTHY'
         AND provider_health.observed_at <= NEW.admitted_at
         AND NEW.admitted_at < provider_health.valid_until
         AND account_health.provider_account_id = NEW.provider_account_id
         AND account_health.credential_ref = NEW.credential_ref
         AND account_health.billing_account_id IS NOT DISTINCT FROM NEW.billing_account_id
         AND account_health.billing_instrument_id IS NOT DISTINCT FROM NEW.billing_instrument_id
         AND account_health.account_verdict = 'HEALTHY'
         AND account_health.credential_verdict = 'VALID'
         AND account_health.billing_account_verdict IS NOT DISTINCT FROM
           CASE WHEN NEW.billing_account_id IS NULL THEN NULL ELSE 'ENABLED' END
         AND account_health.billing_instrument_verdict IS NOT DISTINCT FROM
           CASE WHEN NEW.billing_instrument_id IS NULL THEN NULL ELSE 'ENABLED' END
         AND account_health.observed_at <= NEW.admitted_at
         AND NEW.admitted_at < account_health.valid_until
    ) THEN
      RAISE EXCEPTION 'reasoning model call snapshot must equal one current exact admission'
        USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
  END IF;
  IF NEW.purpose IS DISTINCT FROM 'CONTRIBUTION_DEIDENTIFY' THEN
    RETURN NEW;
  END IF;
  IF NEW.status <> 'RESERVED'
     OR num_nonnulls(
          NEW.input_tokens, NEW.output_tokens, NEW.billable_tokens, NEW.candidate_count,
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

-- ---------------------------------------------------------------------------------------------
-- 6. processing_runs names its Profile@version (ADR-0060 D-K). Writer: adapters::distill_repo::
--    start_processing_run (role_private_worker, table-level INSERT unchanged). Profiles are never
--    deletable (0128), so the FK cannot strand a run.
-- ---------------------------------------------------------------------------------------------
ALTER TABLE private.processing_runs
  ADD COLUMN profile_id uuid,
  ADD COLUMN profile_version bigint,
  ADD CONSTRAINT processing_runs_profile_pair CHECK (num_nonnulls(profile_id, profile_version) IN (0, 2)),
  ADD CONSTRAINT processing_runs_profile_fk FOREIGN KEY (tenant_id, profile_id, profile_version)
    REFERENCES control.reasoning_profiles (tenant_id, profile_id, profile_version);

-- ---------------------------------------------------------------------------------------------
-- 7. One reserve transaction for a private call (ADR-0060 D-N, §11.2.5).
--    (i) Disclosure validator: 0130 body with the private purposes in the list. Caller: the
--        data_disclosure_reasoning_model_call_validate trigger (BEFORE INSERT, every role).
--        Fence: same tenant, RESERVED call, call.egress_processor_id = NEW.processor_id.
--        Owner: role_migration_owner.
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ops.data_disclosure_reasoning_model_call_validate()
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
       AND call.purpose IN ('CONTRIBUTION_DEIDENTIFY', 'PRIVATE_DISTILL_TEXT',
                            'PRIVATE_DISTILL_VISION', 'PRIVATE_CONSOLIDATE')
       AND call.status = 'RESERVED'
       AND call.egress_processor_id = NEW.processor_id
  ) THEN
    RAISE EXCEPTION 'reasoning disclosure must bind the same RESERVED model call'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

--    (ii) At commit, a private ledger row must have its matching disclosure. Caller: the deferred
--        constraint trigger below (every role). Fence: same tenant, model_call_id and processor id
--        = the row's egress_processor_id. Owner: role_migration_owner; nobody holds EXECUTE (a
--        trigger function is invoked by the trigger, never called).
CREATE FUNCTION ops.model_call_ledger_private_disclosure_present()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM ops.data_disclosures d
     WHERE d.tenant_id = NEW.tenant_id
       AND d.model_call_id = NEW.model_call_id
       AND d.processor_id = NEW.egress_processor_id
  ) THEN
    RAISE EXCEPTION 'private reasoning reserve must carry its matching disclosure'
      USING ERRCODE = '23514';
  END IF;
  RETURN NULL;
END;
$$;

ALTER FUNCTION ops.model_call_ledger_private_disclosure_present() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.model_call_ledger_private_disclosure_present()
  FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
       role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;

CREATE CONSTRAINT TRIGGER model_call_ledger_private_disclosure_present
  AFTER INSERT ON ops.model_call_ledger
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW
  WHEN (NEW.purpose IN ('PRIVATE_DISTILL_TEXT', 'PRIVATE_DISTILL_VISION', 'PRIVATE_CONSOLIDATE'))
  EXECUTE FUNCTION ops.model_call_ledger_private_disclosure_present();

-- ---------------------------------------------------------------------------------------------
-- 8. Fair-share distill claim (ADR-0060 D-F). The 0193 body byte for byte except the tenant ORDER
--    BY. Caller: adapters::jobs::claim_distill (role_private_worker). Fence and owner: as 0190 §4;
--    CREATE OR REPLACE keeps owner, SECURITY DEFINER, search_path and the ACL.
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ops.claim_derived_work_v2(
  p_lease_owner text, p_lease_seconds double precision, p_hard_deadline_seconds double precision
) RETURNS SETOF ops.jobs
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_turn bigint;
  v_slot smallint;
  v_tenant uuid;
  v_job uuid;
  v_row ops.jobs;
  s record;
BEGIN
  IF p_lease_owner IS NULL OR length(btrim(p_lease_owner)) = 0
     OR p_lease_seconds IS NULL OR p_lease_seconds <= 0
     OR p_hard_deadline_seconds IS NULL OR p_hard_deadline_seconds < p_lease_seconds THEN
    RAISE EXCEPTION 'invalid distill claim' USING ERRCODE = '22023';
  END IF;

  -- R-32 / §61: lock order arbiter -> slot -> tenant -> job.
  SELECT a.next_turn INTO STRICT v_turn
  FROM ops.provider_arbiters a WHERE a.budget = 'PRIVATE_REASONING'
  FOR UPDATE;

  -- Slot sweep. The sweep never locks or waits on a job it reads: the job is read by MVCC only,
  -- and every transition first probes the job row with SKIP LOCKED — NOT FOUND means busy (a
  -- renew / begin_call / finish holds it), so the slot stays bound and the next claim retries.
  FOR s IN
    SELECT sl.slot_no, sl.job_id, sl.claim_generation AS slot_gen, sl.bound_until,
           j.job_id AS live_job, j.status, j.claim_generation AS job_gen, j.dispatch_state,
           j.lease_expires_at, j.hard_deadline
    FROM ops.provider_slots sl
    LEFT JOIN ops.jobs j ON j.job_id = sl.job_id
    WHERE sl.job_id IS NOT NULL
    ORDER BY sl.slot_no
    FOR UPDATE OF sl SKIP LOCKED
  LOOP
    IF s.live_job IS NULL OR s.status <> 'PROCESSING' OR s.job_gen <> s.slot_gen
       OR s.dispatch_state IS NULL THEN
      -- T9 orphan heal: the job vanished or moved on without freeing its slot. Its request (if
      -- any) is counted until bound_until (= that claim's hard_deadline).
      IF s.bound_until <= clock_timestamp() THEN
        UPDATE ops.provider_slots
        SET job_id = NULL, claim_generation = NULL, bound_until = NULL
        WHERE slot_no = s.slot_no;
      END IF;
    ELSIF s.dispatch_state = 'CLAIMED' AND s.lease_expires_at < clock_timestamp() THEN
      -- T4: claimed, never dispatched, lease lost — no request was authorized; READY again.
      PERFORM 1 FROM ops.jobs j WHERE j.job_id = s.job_id FOR UPDATE SKIP LOCKED;
      IF FOUND THEN
        UPDATE ops.jobs j
        SET status = 'PENDING', dispatch_state = NULL, lease_owner = NULL,
            lease_expires_at = NULL, next_retry_at = clock_timestamp(),
            abandoned_claims = j.abandoned_claims + 1
        WHERE j.job_id = s.job_id AND j.claim_generation = s.slot_gen
          AND j.status = 'PROCESSING' AND j.dispatch_state = 'CLAIMED'
          AND j.lease_expires_at < clock_timestamp();
        IF FOUND THEN
          UPDATE ops.provider_slots
          SET job_id = NULL, claim_generation = NULL, bound_until = NULL
          WHERE slot_no = s.slot_no;
        END IF;
      END IF;
    ELSIF s.dispatch_state IN ('DISPATCH_INTENT', 'EXECUTION_UNCERTAIN')
          AND s.hard_deadline <= clock_timestamp() THEN
      -- T6: past hard_deadline no request of this claim can still be open (begin_call admits a
      -- call only with http_timeout + lease left). Re-queue classed and counted (ADR-0058 E1).
      PERFORM 1 FROM ops.jobs j WHERE j.job_id = s.job_id FOR UPDATE SKIP LOCKED;
      IF FOUND THEN
        UPDATE ops.jobs j
        SET status = 'PENDING', dispatch_state = NULL, lease_owner = NULL,
            lease_expires_at = NULL,
            -- ADR-0058 E1 guard (b): the same capped schedule as every other retry
            -- (adapters::jobs::retry_backoff_seconds, cap pinned by contract test), with
            -- +-25% jitter so four reconciled slots never resend in the same instant.
            next_retry_at = clock_timestamp() + make_interval(secs =>
              LEAST(p_lease_seconds * power(2, LEAST(GREATEST(j.attempt, 1), 16) - 1), 300)
              * (0.75 + 0.5 * random())),
            last_error_class = 'EXECUTION_UNCERTAIN'
        WHERE j.job_id = s.job_id AND j.claim_generation = s.slot_gen
          AND j.status = 'PROCESSING'
          AND j.dispatch_state IN ('DISPATCH_INTENT', 'EXECUTION_UNCERTAIN')
          AND j.hard_deadline <= clock_timestamp();
        IF FOUND THEN
          UPDATE ops.provider_slots
          SET job_id = NULL, claim_generation = NULL, bound_until = NULL
          WHERE slot_no = s.slot_no;
        END IF;
      END IF;
    ELSIF s.dispatch_state = 'DISPATCH_INTENT' AND s.lease_expires_at < clock_timestamp() THEN
      -- T5 — ADR-0058 D-G: lease expiry is not the end of execution; the slot is KEPT.
      PERFORM 1 FROM ops.jobs j WHERE j.job_id = s.job_id FOR UPDATE SKIP LOCKED;
      IF FOUND THEN
        UPDATE ops.jobs j
        SET dispatch_state = 'EXECUTION_UNCERTAIN'
        WHERE j.job_id = s.job_id AND j.claim_generation = s.slot_gen
          AND j.status = 'PROCESSING' AND j.dispatch_state = 'DISPATCH_INTENT'
          AND j.lease_expires_at < clock_timestamp() AND j.hard_deadline > clock_timestamp();
      END IF;
    END IF;
  END LOOP;

  SELECT sl.slot_no INTO v_slot
  FROM ops.provider_slots sl
  WHERE sl.job_id IS NULL
  ORDER BY sl.slot_no
  LIMIT 1
  FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN
    RETURN;
  END IF;

  -- ponytail: O(#scheduler rows) scan with one EXISTS probe per tenant (ADR-0058 L4), an
  -- `eligible_at` column maintained by finish + the admit trigger if tenant counts grow.
  SELECT t.tenant_id INTO v_tenant
  FROM ops.distill_tenant_scheduler t
  WHERE EXISTS (
    SELECT 1 FROM ops.jobs j
    WHERE j.tenant_id = t.tenant_id AND j.job_type = 'DERIVED_DISTILL'
      AND j.status IN ('PENDING', 'RETRY_WAIT', 'WAITING_KEY')
      AND j.next_retry_at <= clock_timestamp()
  )
  -- ADR-0060 D-F: the tenant with READY work holding the FEWEST slots first, then least recently
  -- served. Work-conserving (a free slot never idles while any tenant has READY work), and N busy
  -- tenants hold at most ceil(4/N) slots each, so one hanging provider holds only its share.
  ORDER BY (SELECT count(*) FROM ops.provider_slots sl JOIN ops.jobs hj ON hj.job_id = sl.job_id
            WHERE hj.tenant_id = t.tenant_id),
           t.last_served_turn, t.tenant_id
  LIMIT 1
  FOR UPDATE OF t SKIP LOCKED;
  IF NOT FOUND THEN
    RETURN;
  END IF;

  SELECT j.job_id INTO v_job
  FROM ops.jobs j
  WHERE j.tenant_id = v_tenant AND j.job_type = 'DERIVED_DISTILL'
    AND j.status IN ('PENDING', 'RETRY_WAIT', 'WAITING_KEY')
    AND j.next_retry_at <= clock_timestamp()
  ORDER BY j.created_at, j.job_id
  LIMIT 1
  FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN
    RETURN;
  END IF;

  UPDATE ops.jobs j
  SET status = 'PROCESSING',
      dispatch_state = 'CLAIMED',
      claim_generation = j.claim_generation + 1,
      lease_owner = p_lease_owner,
      hard_deadline = clock_timestamp() + make_interval(secs => p_hard_deadline_seconds),
      lease_expires_at = clock_timestamp() + make_interval(secs => p_lease_seconds)
  WHERE j.job_id = v_job AND j.job_type = 'DERIVED_DISTILL'
    AND j.status IN ('PENDING', 'RETRY_WAIT', 'WAITING_KEY')
    AND j.next_retry_at <= clock_timestamp()
  RETURNING j.* INTO v_row;
  IF NOT FOUND THEN
    RETURN;
  END IF;

  UPDATE ops.provider_slots
  SET job_id = v_row.job_id, claim_generation = v_row.claim_generation,
      bound_until = v_row.hard_deadline
  WHERE slot_no = v_slot AND job_id IS NULL;
  IF NOT FOUND THEN
    -- Unreachable while the slot row lock above holds; never leave a PROCESSING job without a slot.
    RAISE EXCEPTION 'provider slot % was bound concurrently', v_slot USING ERRCODE = '40001';
  END IF;

  UPDATE ops.distill_tenant_scheduler SET last_served_turn = v_turn WHERE tenant_id = v_tenant;
  UPDATE ops.provider_arbiters SET next_turn = v_turn + 1 WHERE budget = 'PRIVATE_REASONING';

  RETURN NEXT v_row;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 9. Vendor identity behind credential references (ADR-0060 D-J). Caller: the private worker's boot
--    check (one call per process start). Fence: returns vendor identity only — no tenant, no secret,
--    no openbao_ref; EXECUTE role_private_worker only. Owner: role_migration_owner.
--    The bindings and accounts are FORCE-RLS per tenant, so the function visits each tenant under
--    its own humaux.tenant_id (the 0204 pattern) and restores the caller's value.
-- ---------------------------------------------------------------------------------------------
-- ponytail: one indexed probe per tenant row (~125 ms at 10k tenants, boot only); an owner-read
-- policy on the two tables when tenant counts grow by orders of magnitude.
CREATE FUNCTION control.reasoning_credential_accounts(p_refs uuid[])
RETURNS TABLE (credential_ref uuid, processor_id text, external_account_ref_hash bytea)
LANGUAGE plpgsql
VOLATILE -- it moves the humaux.tenant_id GUC while it runs
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_caller text := current_setting('humaux.tenant_id', true);
  v_tenant uuid;
BEGIN
  FOR v_tenant IN SELECT t.tenant_id FROM control.tenants t LOOP
    PERFORM set_config('humaux.tenant_id', v_tenant::text, true);
    RETURN QUERY
      SELECT b.credential_ref, a.processor_id, a.external_account_ref_hash
        FROM control.reasoning_credential_bindings b
        JOIN control.provider_accounts a
          ON a.tenant_id = b.tenant_id AND a.provider_account_id = b.provider_account_id
       WHERE b.tenant_id = v_tenant
         AND b.credential_ref = ANY (p_refs);
  END LOOP;
  PERFORM set_config('humaux.tenant_id', coalesce(v_caller, ''), true);
END;
$$;

ALTER FUNCTION control.reasoning_credential_accounts(uuid[]) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.reasoning_credential_accounts(uuid[])
  FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
       role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT EXECUTE ON FUNCTION control.reasoning_credential_accounts(uuid[]) TO role_private_worker;
COMMENT ON FUNCTION control.reasoning_credential_accounts(uuid[]) IS
  'ADR-0060 D-J: (credential_ref, processor_id, external_account_ref_hash) for each bound reference, '
  'across tenants, with no tenant column. Caller: the private worker boot check. EXECUTE: '
  'role_private_worker only.';

-- ---------------------------------------------------------------------------------------------
-- 10. Worker-observed route health (ADR-0060 ruling E3, §11.2.5). Caller: the private worker after it
--     finalized one routed call (role_private_worker). Fence: the call must belong to the session
--     tenant, carry a route, be finalized with the outcome the caller reports, be younger than the
--     validity it asks for, and have been admitted under the observations that are still the latest
--     for its identity; the identity written is copied from those observations, never supplied by
--     the caller. A success appends one HEALTHY provider row and one HEALTHY/VALID account row only
--     when the admitted validity has less than half of p_valid_for_seconds left (rate limit); a
--     rejected credential appends one account row with credential verdict INVALID. Anything else
--     appends nothing (both ids NULL). Owner: role_migration_owner (the health tables are
--     owner-only).
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.observe_reasoning_route_health(
  p_model_call_id uuid, p_credential_rejected boolean, p_valid_for_seconds bigint
) RETURNS TABLE (provider_observation_id bigint, account_observation_id bigint)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_call record;
  v_provider record;
  v_account record;
  v_now timestamptz;
BEGIN
  IF p_model_call_id IS NULL OR p_credential_rejected IS NULL
     OR p_valid_for_seconds IS NULL OR p_valid_for_seconds <= 0 THEN
    RAISE EXCEPTION 'invalid route health observation' USING ERRCODE = '22023';
  END IF;
  SELECT c.tenant_id, c.status, c.called_at, c.provider_health_observation_id,
         c.account_health_observation_id
    INTO v_call
    FROM ops.model_call_ledger c
   WHERE c.tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
     AND c.model_call_id = p_model_call_id
     AND c.profile_id IS NOT NULL;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'route_call_not_found' USING ERRCODE = '55000';
  END IF;
  IF v_call.status <> (CASE WHEN p_credential_rejected THEN 'FAILED' ELSE 'SUCCEEDED' END) THEN
    RAISE EXCEPTION 'route_call_outcome_mismatch' USING ERRCODE = '55000';
  END IF;
  -- One observer per admitted account observation at a time, so the rate limit holds under the four
  -- concurrent seats.
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'reasoning_route_health:' || v_call.tenant_id::text || ':'
      || v_call.account_health_observation_id::text, 0));
  v_now := clock_timestamp();
  IF v_call.called_at <= v_now - make_interval(secs => p_valid_for_seconds) THEN
    RETURN QUERY SELECT NULL::bigint, NULL::bigint;
    RETURN;
  END IF;
  SELECT o.* INTO v_provider FROM ops.reasoning_provider_health_observations o
   WHERE o.tenant_id = v_call.tenant_id AND o.observation_id = v_call.provider_health_observation_id;
  SELECT o.* INTO v_account FROM ops.reasoning_account_health_observations o
   WHERE o.tenant_id = v_call.tenant_id AND o.observation_id = v_call.account_health_observation_id;
  -- A newer observation of the same identity already decided: an old call never overrides it.
  IF EXISTS (
       SELECT 1 FROM ops.reasoning_account_health_observations o
        WHERE o.tenant_id = v_account.tenant_id
          AND o.provider_account_id = v_account.provider_account_id
          AND o.credential_ref = v_account.credential_ref
          AND o.billing_account_id IS NOT DISTINCT FROM v_account.billing_account_id
          AND o.billing_instrument_id IS NOT DISTINCT FROM v_account.billing_instrument_id
          AND (o.observed_at, o.observation_id) > (v_account.observed_at, v_account.observation_id))
     OR (NOT p_credential_rejected AND EXISTS (
       SELECT 1 FROM ops.reasoning_provider_health_observations o
        WHERE o.tenant_id = v_provider.tenant_id
          AND o.processor_id = v_provider.processor_id
          AND o.processor_model_id = v_provider.processor_model_id
          AND o.provider_model_id = v_provider.provider_model_id
          AND o.model_revision IS NOT DISTINCT FROM v_provider.model_revision
          AND o.provider_endpoint_id = v_provider.provider_endpoint_id
          AND o.endpoint_ref = v_provider.endpoint_ref
          AND o.region = v_provider.region
          AND o.service_tier = v_provider.service_tier
          AND (o.observed_at, o.observation_id) > (v_provider.observed_at, v_provider.observation_id)))
  THEN
    RETURN QUERY SELECT NULL::bigint, NULL::bigint;
    RETURN;
  END IF;

  IF p_credential_rejected THEN
    INSERT INTO ops.reasoning_account_health_observations AS o
      (tenant_id, provider_account_id, credential_ref, billing_account_id, billing_instrument_id,
       source_kind, reason_code, account_verdict, credential_verdict, billing_account_verdict,
       billing_instrument_verdict, observed_at, valid_until)
    VALUES (v_account.tenant_id, v_account.provider_account_id, v_account.credential_ref,
            v_account.billing_account_id, v_account.billing_instrument_id, 'WORKER_OBSERVED',
            'PROVIDER_AUTH_REJECTED', 'UNKNOWN', 'INVALID',
            CASE WHEN v_account.billing_account_id IS NULL THEN NULL ELSE 'UNKNOWN' END,
            CASE WHEN v_account.billing_instrument_id IS NULL THEN NULL ELSE 'UNKNOWN' END,
            v_now, v_now + make_interval(secs => p_valid_for_seconds))
    RETURNING o.observation_id INTO account_observation_id;
    RETURN NEXT;
    RETURN;
  END IF;

  IF least(v_provider.valid_until, v_account.valid_until) - v_now
       >= make_interval(secs => p_valid_for_seconds / 2.0) THEN
    RETURN QUERY SELECT NULL::bigint, NULL::bigint;
    RETURN;
  END IF;
  INSERT INTO ops.reasoning_provider_health_observations AS o
    (tenant_id, processor_id, processor_model_id, provider_model_id, model_revision,
     provider_endpoint_id, endpoint_ref, region, service_tier, source_kind, reason_code, verdict,
     observed_at, valid_until)
  VALUES (v_provider.tenant_id, v_provider.processor_id, v_provider.processor_model_id,
          v_provider.provider_model_id, v_provider.model_revision, v_provider.provider_endpoint_id,
          v_provider.endpoint_ref, v_provider.region, v_provider.service_tier, 'WORKER_OBSERVED',
          NULL, 'HEALTHY', v_now, v_now + make_interval(secs => p_valid_for_seconds))
  RETURNING o.observation_id INTO provider_observation_id;
  INSERT INTO ops.reasoning_account_health_observations AS o
    (tenant_id, provider_account_id, credential_ref, billing_account_id, billing_instrument_id,
     source_kind, reason_code, account_verdict, credential_verdict, billing_account_verdict,
     billing_instrument_verdict, observed_at, valid_until)
  VALUES (v_account.tenant_id, v_account.provider_account_id, v_account.credential_ref,
          v_account.billing_account_id, v_account.billing_instrument_id, 'WORKER_OBSERVED', NULL,
          'HEALTHY', 'VALID',
          CASE WHEN v_account.billing_account_id IS NULL THEN NULL ELSE 'ENABLED' END,
          CASE WHEN v_account.billing_instrument_id IS NULL THEN NULL ELSE 'ENABLED' END,
          v_now, v_now + make_interval(secs => p_valid_for_seconds))
  RETURNING o.observation_id INTO account_observation_id;
  RETURN NEXT;
END;
$$;

ALTER FUNCTION control.observe_reasoning_route_health(uuid, boolean, bigint) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.observe_reasoning_route_health(uuid, boolean, bigint)
  FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
       role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT EXECUTE ON FUNCTION control.observe_reasoning_route_health(uuid, boolean, bigint)
  TO role_private_worker;
COMMENT ON FUNCTION control.observe_reasoning_route_health(uuid, boolean, bigint) IS
  'ADR-0060 ruling E3 / §11.2.5: worker-observed route health (source_kind WORKER_OBSERVED) derived '
  'from one finalized routed call of the session tenant. EXECUTE: role_private_worker only.';
