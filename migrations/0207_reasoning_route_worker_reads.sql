-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0206). Card 33b slice 3 / ADR-0060
-- (profile-driven reasoning routing). Spec: §11.2 (Profile request shape), §11.2.5 (health
-- observations; administrative state is read through narrow definers only), §78.1.
--
-- What this migration adds and why (two worker reads 0206 did not provide):
--  1. control.reasoning_profile_request_extras (research amendment 1): the admitted Profile@version's
--     request_extras. 0206 added the column and its CHECK but no reader the private worker may call
--     (role_private_worker has no grant on control.reasoning_profiles), so a route-built provider
--     instance could not carry its profile's vendor fields. Same shape and fence as 0206's
--     reasoning_profile_capabilities.
--  2. control.reasoning_route_health_state (ruling E3 (c)): when the 0130 resolver admits no row, the
--     worker names a route parked for stale or denied health with its own class instead of the
--     generic "not admitted". It returns two booleans about the latest observations of the route
--     behind one current Binding@version of the session tenant; never an id, a verdict text or a
--     timestamp, so it cannot be used to read health tables row by row.
--
-- No table, no grant on a table, no RLS change. The doors of ADR-0060 D-H move to the next number.

-- ---------------------------------------------------------------------------------------------
-- 1. Request extras of one admitted Profile@version (ADR-0060 research amendment 1).
--    Caller: adapters::reasoning_route_admission::resolve_user_reasoning_admission
--    (role_private_worker), in the transaction that just ran the 0130 resolver.
--    Fence: the humaux.tenant_id GUC (and FORCE RLS on the table): another tenant's profile is NULL.
--    Owner: role_migration_owner.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.reasoning_profile_request_extras(p_profile_id uuid, p_profile_version bigint)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  SELECT p.request_extras
    FROM control.reasoning_profiles p
   WHERE p.tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
     AND p.profile_id = p_profile_id
     AND p.profile_version = p_profile_version
$$;

ALTER FUNCTION control.reasoning_profile_request_extras(uuid, bigint) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.reasoning_profile_request_extras(uuid, bigint)
  FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
       role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT EXECUTE ON FUNCTION control.reasoning_profile_request_extras(uuid, bigint) TO role_private_worker;
COMMENT ON FUNCTION control.reasoning_profile_request_extras(uuid, bigint) IS
  '§11.2 / ADR-0060 research amendment 1: request_extras of one Profile@version of the session '
  'tenant; NULL for any other tenant. Caller: adapters::reasoning_route_admission. EXECUTE: '
  'role_private_worker only.';

-- ---------------------------------------------------------------------------------------------
-- 2. Health state of the route behind one current Binding@version (ADR-0060 ruling E3 (c)).
--    Caller: adapters::reasoning_route_admission::route_health_refusal (role_private_worker), only
--    after the 0130 resolver returned no row, in the same transaction.
--    Fence: the humaux.tenant_id GUC; the binding must be current (effective_to IS NULL) and its
--    policy must have exactly one candidate, as the resolver requires. The latest observation of the
--    exact provider / account identity is chosen the way the 0130 resolver chooses it.
--    Returns no row when the route itself does not exist; otherwise
--      health_stale  = a latest observation is missing or no longer valid at clock_timestamp();
--      health_denied = both are valid but a verdict is not the admissible one.
--    Owner: role_migration_owner (the health tables are owner-only).
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.reasoning_route_health_state(p_binding_id uuid, p_binding_version bigint)
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
      JOIN control.reasoning_route_candidates candidate
        ON (candidate.tenant_id, candidate.route_policy_id, candidate.route_policy_version) =
           (b.tenant_id, b.route_policy_id, b.route_policy_version)
       AND 1 = (SELECT count(*) FROM control.reasoning_route_candidates c
                 WHERE (c.tenant_id, c.route_policy_id, c.route_policy_version) =
                       (b.tenant_id, b.route_policy_id, b.route_policy_version))
      JOIN control.reasoning_profiles profile
        ON (profile.tenant_id, profile.profile_id, profile.profile_version) =
           (candidate.tenant_id, candidate.profile_id, candidate.profile_version)
      JOIN control.provider_accounts account
        ON account.tenant_id = profile.tenant_id
       AND account.provider_account_id = profile.provider_account_id
      JOIN control.provider_endpoints endpoint
        ON endpoint.tenant_id = profile.tenant_id AND endpoint.endpoint_id = profile.endpoint_id
      JOIN control.processor_models model
        ON model.processor_model_id = profile.processor_model_id
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
  'ADR-0060 ruling E3 (c) / §11.2.5: whether the latest health observations of the route behind one '
  'current Binding@version of the session tenant are stale or denied (two booleans only). Caller: '
  'adapters::reasoning_route_admission. EXECUTE: role_private_worker only.';
