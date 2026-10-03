-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0208). Card 33b review pass 2 /
-- ADR-0060 ruling E3. Spec: §11.2.5 (health observations; administrative state), §11.2.2 R3.
-- 0206 and 0207 are applied; their bodies are corrected here, never in place (§46).
--
-- What this migration changes and why:
--  1. control.observe_reasoning_route_health (0206, ruling E3 (b)): a rejected credential appends its
--     INVALID account row even when a newer HEALTHY renewal of the same account exists. 0206 skipped it
--     whenever any newer account row existed, so a seat that renewed just before the key was revoked let
--     one more admission and one more provider call through. Renewals keep the 0206 guard; a rejection
--     is skipped only by a newer INVALID row. A contribution call (purpose CONTRIBUTION_DEIDENTIFY) whose
--     401 left its row RESERVED for reconciliation (0131) may report the rejection on that row; every
--     other purpose must still be finalized with the reported outcome.
--  2. control.reasoning_route_health_state (0207, ruling E3 (c)): the route is read with the
--     administrative conditions the 0130 resolver applies (policy SERVING and in effect, candidate,
--     profile, account and endpoint enabled, account and model ACTIVE). A disabled or retired route
--     returns no row, so the worker reports the generic "reasoning route not admitted" instead of
--     ROUTE_HEALTH_STALE / ROUTE_HEALTH_DENIED for a route that re-attesting would not reopen.
--
-- Both are CREATE OR REPLACE with unchanged signatures and result types: owner, SECURITY DEFINER,
-- search_path and the role_private_worker-only EXECUTE are kept (re-asserted below; postcheck).
-- Locks: none on tables (function replacement only). No table, no grant on a table, no RLS change.

-- ---------------------------------------------------------------------------------------------
-- 1. Worker-observed route health (ADR-0060 ruling E3 (b), §11.2.5). Caller:
--    humaux-private-worker after a routed call (distill, consolidation RPC, contribution runner;
--    role_private_worker). Fence: the call must belong to the session tenant, carry a route, be
--    finalized with the outcome the caller reports (or, for a contribution rejection, still RESERVED),
--    be younger than the validity it asks for, and have been admitted under the observations that are
--    still the latest for its identity (a rejection: no newer INVALID row); the identity written is
--    copied from those observations, never supplied by the caller. Writes as in 0206.
--    Owner: role_migration_owner (the health tables are owner-only).
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION control.observe_reasoning_route_health(
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
  SELECT c.tenant_id, c.purpose, c.status, c.called_at, c.provider_health_observation_id,
         c.account_health_observation_id
    INTO v_call
    FROM ops.model_call_ledger c
   WHERE c.tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
     AND c.model_call_id = p_model_call_id
     AND c.profile_id IS NOT NULL;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'route_call_not_found' USING ERRCODE = '55000';
  END IF;
  -- 0131 leaves a contribution call that met a non-terminal provider error (a 401 included) RESERVED
  -- for reconciliation, so its rejection is observed on that row; every other purpose is finalized.
  IF NOT (v_call.status = (CASE WHEN p_credential_rejected THEN 'FAILED' ELSE 'SUCCEEDED' END)
          OR (p_credential_rejected AND v_call.status = 'RESERVED'
              AND v_call.purpose = 'CONTRIBUTION_DEIDENTIFY')) THEN
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
  -- A newer observation of the same identity already decided: an old call never overrides it. A
  -- rejected credential is overridden only by a newer INVALID row (a newer renewal does not make the
  -- key valid again; only an operator attestation after rotation does).
  IF EXISTS (
       SELECT 1 FROM ops.reasoning_account_health_observations o
        WHERE o.tenant_id = v_account.tenant_id
          AND o.provider_account_id = v_account.provider_account_id
          AND o.credential_ref = v_account.credential_ref
          AND o.billing_account_id IS NOT DISTINCT FROM v_account.billing_account_id
          AND o.billing_instrument_id IS NOT DISTINCT FROM v_account.billing_instrument_id
          AND (o.observed_at, o.observation_id) > (v_account.observed_at, v_account.observation_id)
          AND (NOT p_credential_rejected OR o.credential_verdict = 'INVALID'))
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
  'from one routed call of the session tenant; a rejected credential is recorded even after a newer '
  'renewal (0209). EXECUTE: role_private_worker only.';

-- ---------------------------------------------------------------------------------------------
-- 2. Health state of the route behind one current Binding@version (ADR-0060 ruling E3 (c)).
--    Caller: adapters::reasoning_route_admission::route_health_refusal (role_private_worker), only
--    after the 0130 resolver returned no row, in the same transaction.
--    Fence: the humaux.tenant_id GUC; the binding must be current and its route administratively
--    admissible exactly as the 0130 resolver requires (policy SERVING and in effect, one enabled
--    candidate, enabled profile, ACTIVE enabled account, enabled endpoint, ACTIVE model).
--    Returns no row when that route does not exist or is disabled or retired; otherwise the two
--    booleans of 0207 (health_stale, health_denied).
--    Owner: role_migration_owner (the health tables are owner-only).
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION control.reasoning_route_health_state(p_binding_id uuid, p_binding_version bigint)
RETURNS TABLE (health_stale boolean, health_denied boolean)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  WITH route AS (
    SELECT profile.tenant_id, account.processor_id, profile.processor_model_id,
           model.provider_model_id, model.model_revision, endpoint.endpoint_id,
           endpoint.endpoint_ref, endpoint.region, endpoint.service_tier,
           profile.provider_account_id, profile.credential_ref, profile.billing_account_id,
           profile.default_billing_instrument_id AS billing_instrument_id,
           clock_timestamp() AS at
      FROM control.reasoning_route_bindings b
      JOIN control.reasoning_route_policies policy
        ON (policy.tenant_id, policy.route_policy_id, policy.policy_version) =
           (b.tenant_id, b.route_policy_id, b.route_policy_version)
       AND policy.lifecycle_state = 'SERVING'
       AND policy.effective_from <= clock_timestamp()
       AND (policy.effective_to IS NULL OR clock_timestamp() < policy.effective_to)
      JOIN control.reasoning_route_candidates candidate
        ON (candidate.tenant_id, candidate.route_policy_id, candidate.route_policy_version) =
           (b.tenant_id, b.route_policy_id, b.route_policy_version)
       AND candidate.enabled
       AND 1 = (SELECT count(*) FROM control.reasoning_route_candidates c
                 WHERE (c.tenant_id, c.route_policy_id, c.route_policy_version) =
                       (b.tenant_id, b.route_policy_id, b.route_policy_version))
      JOIN control.reasoning_profiles profile
        ON (profile.tenant_id, profile.profile_id, profile.profile_version) =
           (candidate.tenant_id, candidate.profile_id, candidate.profile_version)
       AND profile.enabled
      JOIN control.provider_accounts account
        ON account.tenant_id = profile.tenant_id
       AND account.provider_account_id = profile.provider_account_id
       AND account.status = 'ACTIVE' AND account.enabled
      JOIN control.provider_endpoints endpoint
        ON endpoint.tenant_id = profile.tenant_id AND endpoint.endpoint_id = profile.endpoint_id
       AND endpoint.enabled
      JOIN control.processor_models model
        ON model.processor_model_id = profile.processor_model_id
       AND model.status = 'ACTIVE'
     WHERE b.tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
       AND b.binding_id = p_binding_id
       AND b.binding_version = p_binding_version
       AND b.effective_to IS NULL
  )
  SELECT (ph.valid_until IS NULL OR route.at >= ph.valid_until
          OR ah.valid_until IS NULL OR route.at >= ah.valid_until),
         (ph.valid_until IS NOT NULL AND route.at < ph.valid_until
          AND ah.valid_until IS NOT NULL AND route.at < ah.valid_until
          AND (ph.verdict <> 'HEALTHY' OR ah.account_verdict <> 'HEALTHY'
               OR ah.credential_verdict <> 'VALID'))
    FROM route
    LEFT JOIN LATERAL (
      SELECT o.verdict, o.valid_until
        FROM ops.reasoning_provider_health_observations o
       WHERE o.tenant_id = route.tenant_id
         AND o.processor_id = route.processor_id
         AND o.processor_model_id = route.processor_model_id
         AND o.provider_model_id = route.provider_model_id
         AND o.model_revision IS NOT DISTINCT FROM route.model_revision
         AND o.provider_endpoint_id = route.endpoint_id
         AND o.endpoint_ref = route.endpoint_ref
         AND o.region = route.region
         AND o.service_tier = route.service_tier
       ORDER BY o.observed_at DESC, o.observation_id DESC
       LIMIT 1
    ) ph ON true
    LEFT JOIN LATERAL (
      SELECT o.account_verdict, o.credential_verdict, o.valid_until
        FROM ops.reasoning_account_health_observations o
       WHERE o.tenant_id = route.tenant_id
         AND o.provider_account_id = route.provider_account_id
         AND o.credential_ref = route.credential_ref
         AND o.billing_account_id IS NOT DISTINCT FROM route.billing_account_id
         AND o.billing_instrument_id IS NOT DISTINCT FROM route.billing_instrument_id
       ORDER BY o.observed_at DESC, o.observation_id DESC
       LIMIT 1
    ) ah ON true
$$;

ALTER FUNCTION control.reasoning_route_health_state(uuid, bigint) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.reasoning_route_health_state(uuid, bigint)
  FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
       role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT EXECUTE ON FUNCTION control.reasoning_route_health_state(uuid, bigint) TO role_private_worker;
COMMENT ON FUNCTION control.reasoning_route_health_state(uuid, bigint) IS
  'ADR-0060 ruling E3 (c) / §11.2.5: whether the latest health observations of the administratively '
  'admissible route behind one current Binding@version of the session tenant are stale or denied (two '
  'booleans only; no row for a disabled or retired route, 0209). Caller: '
  'adapters::reasoning_route_admission. EXECUTE: role_private_worker only.';
