-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0207). Card 33b slice 4 / ADR-0060 D-H
-- (profile-driven reasoning routing): the operator doors that put a reasoning route in place.
-- Spec: §11.2 (UserReasoningProfile), §11.2.2 (R2 projection, now also for PRIVATE_DISTILL_TEXT /
-- PRIVATE_CONSOLIDATE), §11.2.3 (versioned route schema, 0128), §11.2.5 (health observations),
-- §77 (Sensitive Admin Action audit, written by the Rust caller in the same transaction), §78.1.
--
-- Before this migration a route existed only where xtask e2e-seed (test-only owner INSERTs) or a test
-- fixture had written it. These five owner SECURITY DEFINER functions are the production way, each
-- EXECUTE role_maintenance ONLY (the §4.2 operator-write role; `humaux-maintenance reasoning ...`
-- reaches them through adapters::reasoning_route_onboarding):
--  1. control.register_reasoning_profile   — catalog row, vendor account, credential, endpoint,
--     Profile@version (ADR-0060 D-H 1; research amendment 1: request_extras).
--  2. control.bind_reasoning_domain        — the R2 projection for one (domain, purpose): one PINNED
--     Policy, exactly one Candidate, one current Binding; a rebind closes the current interval and
--     inserts the successor Binding@version (same binding_id) (D-H 2).
--  3. control.attest_reasoning_route_health — one provider and one account observation,
--     source_kind OPERATOR_ATTEST, valid for an explicit number of seconds (D-H 3, ruling E3 (a)).
--  4. control.set_reasoning_profile_enabled — the one mutable Profile column (D-H 4).
--  5. control.reasoning_route_status        — read-only: each (domain, purpose) of the tenant with its
--     bound Profile@version and its latest health verdicts and valid_until (ruling E3 (c));
--     role_maintenance holds no SELECT on the route tables, so this is its only view of them.
--
-- Every door asserts p_tenant = the installed humaux.tenant_id (42501 tenant_context_mismatch; the
-- 0176 / 0197 lesson: an owner door must not become a cross-tenant door), refuses with SQLSTATE 55000
-- and a reason token as the message (nothing written), and rejects a malformed request with 22023.
-- The 0128 triggers still run on every INSERT (READ COMMITTED, advisory locks, owner / payer spine),
-- so the doors cannot write a row the schema would refuse; they only name the refusal first.
-- No table, no grant on a table, no RLS change.

-- ---------------------------------------------------------------------------------------------
-- 1. Register a reasoning profile (ADR-0060 D-H 1).
--    Caller: adapters::reasoning_route_onboarding::register_profile (role_maintenance), one
--    transaction with its §77 audit row. Fence: p_tenant = humaux.tenant_id; every row it reads or
--    writes is the tenant's (FORCE RLS on the tenant tables also scopes the owner).
--    Order (= e2e-seed's lane): catalog -> account -> endpoint -> credential -> profile. An enabled
--    identical profile answers `existing` and writes nothing. p_credential_ref NULL mints a credential
--    whose secret lives in the private worker's env map (ADR-0060 D-J), never in the database.
--    Refusals: endpoint_not_https, owner_not_member, model_retired, capabilities_exceed_catalog,
--    account_not_active, endpoint_identity_conflict, endpoint_disabled, credential_not_bound,
--    profile_not_found, owner_mismatch, request_extras_invalid.
--    Owner: role_migration_owner.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.register_reasoning_profile(
  p_tenant uuid, p_owner_user uuid, p_processor_id text, p_provider_model_id text,
  p_model_revision text, p_capabilities text[], p_request_extras jsonb, p_account_ref_sha256 bytea,
  p_endpoint_ref text, p_region text, p_service_tier text, p_egress_processor_id uuid,
  p_credential_ref uuid, p_successor_of uuid
) RETURNS TABLE (profile_id uuid, profile_version bigint, credential_ref uuid,
                 provider_account_id uuid, endpoint_id uuid, processor_model_id uuid,
                 disposition text)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
DECLARE
  v_model record;
  v_account record;
  v_endpoint record;
  v_existing record;
  v_credential uuid := p_credential_ref;
  v_profile uuid;
  v_version bigint;
  v_constraint text;
BEGIN
  IF p_tenant IS NULL OR p_owner_user IS NULL OR p_egress_processor_id IS NULL
     OR p_egress_processor_id = '00000000-0000-0000-0000-000000000000'::uuid
     OR length(btrim(coalesce(p_processor_id, ''))) = 0
     OR length(btrim(coalesce(p_provider_model_id, ''))) = 0
     OR length(btrim(coalesce(p_region, ''))) = 0
     OR length(btrim(coalesce(p_service_tier, ''))) = 0
     OR length(btrim(coalesce(p_endpoint_ref, ''))) = 0
     OR coalesce(cardinality(p_capabilities), 0) = 0 OR p_request_extras IS NULL
     OR octet_length(p_account_ref_sha256) IS DISTINCT FROM 32 THEN
    RAISE EXCEPTION 'invalid register request' USING ERRCODE = '22023';
  END IF;
  IF p_tenant IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  -- §11.4: the worker re-checks the host against its recipient list; the door can only see the scheme.
  IF p_endpoint_ref NOT LIKE 'https://%' THEN
    RAISE EXCEPTION 'endpoint_not_https' USING ERRCODE = '55000';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM control.memberships m
                  WHERE m.tenant_id = p_tenant AND m.user_id = p_owner_user AND m.state = 'ACTIVE') THEN
    RAISE EXCEPTION 'owner_not_member' USING ERRCODE = '55000';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('reasoning_register:' || p_tenant::text || ':'
                                                 || p_owner_user::text, 0));

  -- Catalog: append-only (F3); a wider capability set needs a new revision label (ruling E5).
  INSERT INTO control.processor_models AS m
    (processor_id, provider_model_id, model_revision, capabilities, status, catalog_observed_at)
  VALUES (p_processor_id, p_provider_model_id, p_model_revision, p_capabilities, 'ACTIVE',
          clock_timestamp())
  ON CONFLICT DO NOTHING;
  SELECT m.processor_model_id, m.status, m.capabilities INTO v_model
    FROM control.processor_models m
   WHERE m.processor_id = p_processor_id AND m.provider_model_id = p_provider_model_id
     AND m.model_revision IS NOT DISTINCT FROM p_model_revision;
  IF v_model.status <> 'ACTIVE' THEN
    RAISE EXCEPTION 'model_retired' USING ERRCODE = '55000';
  END IF;
  IF NOT p_capabilities <@ v_model.capabilities THEN
    RAISE EXCEPTION 'capabilities_exceed_catalog' USING ERRCODE = '55000';
  END IF;

  -- Vendor account: one (tenant, owner, processor, account hash) = one account (ADR-0060 D-J).
  INSERT INTO control.provider_accounts AS a
    (tenant_id, owner_user_id, processor_id, external_account_ref_hash)
  VALUES (p_tenant, p_owner_user, p_processor_id, p_account_ref_sha256)
  ON CONFLICT (tenant_id, owner_user_id, processor_id, external_account_ref_hash) DO NOTHING;
  SELECT a.provider_account_id, a.status, a.enabled INTO v_account
    FROM control.provider_accounts a
   WHERE a.tenant_id = p_tenant AND a.owner_user_id = p_owner_user
     AND a.processor_id = p_processor_id AND a.external_account_ref_hash = p_account_ref_sha256;
  IF v_account.status <> 'ACTIVE' OR NOT v_account.enabled THEN
    RAISE EXCEPTION 'account_not_active' USING ERRCODE = '55000';
  END IF;

  -- Endpoint: identity (region, tier, egress recipient) is immutable, so a reuse must match it.
  SELECT e.endpoint_id, e.region, e.service_tier, e.egress_processor_id, e.enabled INTO v_endpoint
    FROM control.provider_endpoints e
   WHERE e.tenant_id = p_tenant AND e.provider_account_id = v_account.provider_account_id
     AND e.endpoint_ref = p_endpoint_ref;
  IF FOUND THEN
    IF (v_endpoint.region, v_endpoint.service_tier, v_endpoint.egress_processor_id)
       IS DISTINCT FROM (p_region, p_service_tier, p_egress_processor_id) THEN
      RAISE EXCEPTION 'endpoint_identity_conflict' USING ERRCODE = '55000';
    END IF;
    IF NOT v_endpoint.enabled THEN
      RAISE EXCEPTION 'endpoint_disabled' USING ERRCODE = '55000';
    END IF;
  ELSE
    INSERT INTO control.provider_endpoints AS e
      (tenant_id, provider_account_id, region, service_tier, endpoint_ref, egress_processor_id)
    VALUES (p_tenant, v_account.provider_account_id, p_region, p_service_tier, p_endpoint_ref,
            p_egress_processor_id)
    RETURNING e.endpoint_id INTO v_endpoint.endpoint_id;
  END IF;

  IF v_credential IS NOT NULL AND NOT EXISTS (
       SELECT 1 FROM control.reasoning_credential_bindings b
        WHERE b.credential_ref = v_credential AND b.tenant_id = p_tenant
          AND b.provider_account_id = v_account.provider_account_id
          AND b.owner_user_id = p_owner_user) THEN
    RAISE EXCEPTION 'credential_not_bound' USING ERRCODE = '55000';
  END IF;

  -- Idempotency: an enabled profile with this exact identity (any credential when none was named).
  SELECT p.profile_id, p.profile_version, p.credential_ref INTO v_existing
    FROM control.reasoning_profiles p
   WHERE p.tenant_id = p_tenant AND p.owner_user_id = p_owner_user AND p.enabled
     AND p.provider_account_id = v_account.provider_account_id
     AND p.endpoint_id = v_endpoint.endpoint_id
     AND p.processor_model_id = v_model.processor_model_id
     AND (v_credential IS NULL OR p.credential_ref = v_credential)
     AND p.capabilities @> p_capabilities AND p.capabilities <@ p_capabilities
     AND p.request_extras = p_request_extras
     AND p.processing_region IS NOT DISTINCT FROM p_region
     AND p.billing_account_id IS NULL
   ORDER BY p.created_at DESC, p.profile_version DESC
   LIMIT 1;
  IF FOUND THEN
    RETURN QUERY SELECT v_existing.profile_id, v_existing.profile_version, v_existing.credential_ref,
                        v_account.provider_account_id, v_endpoint.endpoint_id,
                        v_model.processor_model_id, 'existing'::text;
    RETURN;
  END IF;

  IF p_successor_of IS NOT NULL THEN
    SELECT p.profile_id, max(p.profile_version) + 1 INTO v_profile, v_version
      FROM control.reasoning_profiles p
     WHERE p.tenant_id = p_tenant AND p.profile_id = p_successor_of
     GROUP BY p.profile_id;
    IF v_profile IS NULL THEN
      RAISE EXCEPTION 'profile_not_found' USING ERRCODE = '55000';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM control.reasoning_profiles p
                    WHERE p.tenant_id = p_tenant AND p.profile_id = p_successor_of
                      AND p.owner_user_id = p_owner_user) THEN
      RAISE EXCEPTION 'owner_mismatch' USING ERRCODE = '55000';
    END IF;
  ELSE
    v_version := 1;
  END IF;

  IF v_credential IS NULL THEN
    INSERT INTO control.credentials AS c (tenant_id, purpose, openbao_ref)
    VALUES (p_tenant, 'USER_REASONING', 'env-map:HUMAUX_PRIVATE_WORKER_CREDENTIALS')
    RETURNING c.credential_id INTO v_credential;
    INSERT INTO control.reasoning_credential_bindings
      (credential_ref, tenant_id, owner_user_id, provider_account_id, processor_id)
    VALUES (v_credential, p_tenant, p_owner_user, v_account.provider_account_id, p_processor_id);
  END IF;

  BEGIN
    INSERT INTO control.reasoning_profiles AS p
      (profile_id, profile_version, tenant_id, owner_user_id, provider_account_id, endpoint_id,
       processor_model_id, credential_ref, billing_account_id, default_billing_instrument_id,
       capabilities, processing_region, request_extras)
    VALUES (coalesce(v_profile, uuidv7()), v_version, p_tenant, p_owner_user,
            v_account.provider_account_id, v_endpoint.endpoint_id, v_model.processor_model_id,
            v_credential, NULL, NULL, p_capabilities, p_region, p_request_extras)
    RETURNING p.profile_id INTO v_profile;
  EXCEPTION WHEN check_violation THEN
    GET STACKED DIAGNOSTICS v_constraint = CONSTRAINT_NAME;
    -- Research amendment 1: the one key list is the 0206 CHECK; the door only names its refusal.
    IF v_constraint = 'reasoning_profiles_request_extras_check' THEN
      RAISE EXCEPTION 'request_extras_invalid' USING ERRCODE = '55000';
    END IF;
    RAISE;
  END;

  RETURN QUERY SELECT v_profile, v_version, v_credential, v_account.provider_account_id,
                      v_endpoint.endpoint_id, v_model.processor_model_id, 'created'::text;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 2. Bind a reasoning domain to one Profile@version (ADR-0060 D-H 2, the R2 projection).
--    Caller: adapters::reasoning_route_onboarding::bind_domain (role_maintenance), one transaction
--    with its §77 audit row. Fence: p_tenant = humaux.tenant_id; an advisory lock per
--    (domain, purpose) serialises concurrent binds.
--    Writes: one Policy (DRAFT -> SHADOW -> SERVING, owner = domain owner), its one Candidate, and
--    either a new Binding@1 or (rebind) the close of the current interval plus the successor
--    Binding@version+1 with the same binding_id. Old Policy / Candidate / Binding rows stay, so every
--    ledger row and processing run keeps naming the route it ran under (§11.2.3, ADR-0060 D-I/D-K).
--    Refusals: purpose_not_bindable (CONTRIBUTION_DEIDENTIFY keeps its 0129 bootstrap; VISION and
--    GROUNDING_RECHECK have no runtime), domain_not_found, domain_not_active, profile_not_found,
--    owner_mismatch, profile_disabled, profile_lacks_structured_output.
--    Owner: role_migration_owner.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.bind_reasoning_domain(
  p_tenant uuid, p_reasoning_domain_id uuid, p_purpose text, p_profile_id uuid,
  p_profile_version bigint
) RETURNS TABLE (binding_id uuid, binding_version bigint, route_policy_id uuid,
                 closed_binding_version bigint, disposition text)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
DECLARE
  v_domain record;
  v_profile record;
  v_current record;
  v_policy uuid;
  v_binding uuid;
  v_version bigint := 1;
BEGIN
  IF p_tenant IS NULL OR p_reasoning_domain_id IS NULL OR p_profile_id IS NULL
     OR p_profile_version IS NULL OR p_purpose IS NULL THEN
    RAISE EXCEPTION 'invalid bind request' USING ERRCODE = '22023';
  END IF;
  IF p_tenant IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  IF p_purpose NOT IN ('PRIVATE_DISTILL_TEXT', 'PRIVATE_CONSOLIDATE') THEN
    RAISE EXCEPTION 'purpose_not_bindable' USING ERRCODE = '55000';
  END IF;
  SELECT d.owner_user_id, d.status INTO v_domain
    FROM control.private_reasoning_domains d
   WHERE d.tenant_id = p_tenant AND d.reasoning_domain_id = p_reasoning_domain_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'domain_not_found' USING ERRCODE = '55000';
  END IF;
  IF v_domain.status <> 'ACTIVE' THEN
    RAISE EXCEPTION 'domain_not_active' USING ERRCODE = '55000';
  END IF;
  SELECT p.owner_user_id, p.enabled, p.capabilities INTO v_profile
    FROM control.reasoning_profiles p
   WHERE p.tenant_id = p_tenant AND p.profile_id = p_profile_id
     AND p.profile_version = p_profile_version;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'profile_not_found' USING ERRCODE = '55000';
  END IF;
  IF v_domain.owner_user_id IS DISTINCT FROM v_profile.owner_user_id THEN
    RAISE EXCEPTION 'owner_mismatch' USING ERRCODE = '55000';
  END IF;
  IF NOT v_profile.enabled THEN
    RAISE EXCEPTION 'profile_disabled' USING ERRCODE = '55000';
  END IF;
  -- ADR-0060 D-C: every derived purpose parses a structured answer.
  IF NOT 'STRUCTURED_OUTPUT' = ANY (v_profile.capabilities) THEN
    RAISE EXCEPTION 'profile_lacks_structured_output' USING ERRCODE = '55000';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('reasoning_bind:' || p_reasoning_domain_id::text
                                                 || ':' || p_purpose, 0));

  SELECT b.binding_id, b.binding_version, b.route_policy_id, b.route_policy_version INTO v_current
    FROM control.reasoning_route_bindings b
   WHERE b.tenant_id = p_tenant AND b.reasoning_domain_id = p_reasoning_domain_id
     AND b.purpose = p_purpose AND b.effective_to IS NULL
   FOR UPDATE;
  IF FOUND AND EXISTS (
       SELECT 1 FROM control.reasoning_route_policies pol
        WHERE pol.tenant_id = p_tenant AND pol.route_policy_id = v_current.route_policy_id
          AND pol.policy_version = v_current.route_policy_version
          AND pol.lifecycle_state = 'SERVING' AND pol.effective_to IS NULL)
     AND (SELECT count(*) = 1 AND bool_and(c.profile_id = p_profile_id
                                           AND c.profile_version = p_profile_version)
            FROM control.reasoning_route_candidates c
           WHERE c.tenant_id = p_tenant AND c.route_policy_id = v_current.route_policy_id
             AND c.route_policy_version = v_current.route_policy_version) THEN
    RETURN QUERY SELECT v_current.binding_id, v_current.binding_version, v_current.route_policy_id,
                        NULL::bigint, 'existing'::text;
    RETURN;
  END IF;

  INSERT INTO control.reasoning_route_policies AS pol (tenant_id, policy_owner_user_id, purpose)
  VALUES (p_tenant, v_domain.owner_user_id, p_purpose)
  RETURNING pol.route_policy_id INTO v_policy;
  INSERT INTO control.reasoning_route_candidates
    (tenant_id, route_policy_id, route_policy_version, profile_id, profile_version, priority)
  VALUES (p_tenant, v_policy, 1, p_profile_id, p_profile_version, 0);
  UPDATE control.reasoning_route_policies pol SET lifecycle_state = 'SHADOW'
   WHERE pol.route_policy_id = v_policy AND pol.policy_version = 1;
  UPDATE control.reasoning_route_policies pol SET lifecycle_state = 'SERVING'
   WHERE pol.route_policy_id = v_policy AND pol.policy_version = 1;

  IF v_current.binding_id IS NOT NULL THEN
    UPDATE control.reasoning_route_bindings b SET effective_to = clock_timestamp()
     WHERE b.binding_id = v_current.binding_id AND b.binding_version = v_current.binding_version;
    v_binding := v_current.binding_id;
    v_version := v_current.binding_version + 1;
  END IF;
  INSERT INTO control.reasoning_route_bindings AS b
    (binding_id, binding_version, tenant_id, reasoning_domain_id, purpose, route_policy_id,
     route_policy_version, effective_from)
  VALUES (coalesce(v_binding, uuidv7()), v_version, p_tenant, p_reasoning_domain_id, p_purpose,
          v_policy, 1, clock_timestamp())
  RETURNING b.binding_id INTO v_binding;

  RETURN QUERY SELECT v_binding, v_version, v_policy, v_current.binding_version,
                      CASE WHEN v_current.binding_id IS NULL THEN 'created' ELSE 'rebound' END;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 3. Attest a profile's route health (ADR-0060 D-H 3, ruling E3 (a)).
--    Caller: adapters::reasoning_route_onboarding::attest_health (role_maintenance), one
--    transaction with its §77 audit row. Fence: p_tenant = humaux.tenant_id.
--    Appends one HEALTHY provider observation (the profile's exact provider identity) and one
--    HEALTHY / VALID account observation (account, credential, billing ids), both OPERATOR_ATTEST,
--    valid for p_valid_for_seconds (> 0, no default: §78.1). The tables are append-only; renewal by
--    traffic is control.observe_reasoning_route_health (0206, ruling E3 (b)).
--    Refusals: profile_not_found. Owner: role_migration_owner (the health tables are owner-only).
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.attest_reasoning_route_health(
  p_tenant uuid, p_profile_id uuid, p_profile_version bigint, p_valid_for_seconds bigint
) RETURNS TABLE (provider_observation_id bigint, account_observation_id bigint,
                 valid_until timestamptz)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
DECLARE
  v_route record;
  v_now timestamptz := clock_timestamp();
  v_until timestamptz;
  v_provider bigint;
  v_account bigint;
BEGIN
  IF p_tenant IS NULL OR p_profile_id IS NULL OR p_profile_version IS NULL
     OR p_valid_for_seconds IS NULL OR p_valid_for_seconds <= 0 THEN
    RAISE EXCEPTION 'invalid attest request (valid_for_seconds must be > 0)' USING ERRCODE = '22023';
  END IF;
  IF p_tenant IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  SELECT account.processor_id, profile.processor_model_id, model.provider_model_id,
         model.model_revision, endpoint.endpoint_id, endpoint.endpoint_ref, endpoint.region,
         endpoint.service_tier, profile.provider_account_id, profile.credential_ref,
         profile.billing_account_id, profile.default_billing_instrument_id
    INTO v_route
    FROM control.reasoning_profiles profile
    JOIN control.provider_accounts account
      ON account.tenant_id = profile.tenant_id
     AND account.provider_account_id = profile.provider_account_id
    JOIN control.provider_endpoints endpoint
      ON endpoint.tenant_id = profile.tenant_id AND endpoint.endpoint_id = profile.endpoint_id
    JOIN control.processor_models model ON model.processor_model_id = profile.processor_model_id
   WHERE profile.tenant_id = p_tenant AND profile.profile_id = p_profile_id
     AND profile.profile_version = p_profile_version;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'profile_not_found' USING ERRCODE = '55000';
  END IF;
  v_until := v_now + make_interval(secs => p_valid_for_seconds);

  INSERT INTO ops.reasoning_provider_health_observations AS o
    (tenant_id, processor_id, processor_model_id, provider_model_id, model_revision,
     provider_endpoint_id, endpoint_ref, region, service_tier, source_kind, reason_code, verdict,
     observed_at, valid_until)
  VALUES (p_tenant, v_route.processor_id, v_route.processor_model_id, v_route.provider_model_id,
          v_route.model_revision, v_route.endpoint_id, v_route.endpoint_ref, v_route.region,
          v_route.service_tier, 'OPERATOR_ATTEST', NULL, 'HEALTHY', v_now, v_until)
  RETURNING o.observation_id INTO v_provider;
  INSERT INTO ops.reasoning_account_health_observations AS o
    (tenant_id, provider_account_id, credential_ref, billing_account_id, billing_instrument_id,
     source_kind, reason_code, account_verdict, credential_verdict, billing_account_verdict,
     billing_instrument_verdict, observed_at, valid_until)
  VALUES (p_tenant, v_route.provider_account_id, v_route.credential_ref,
          v_route.billing_account_id, v_route.default_billing_instrument_id, 'OPERATOR_ATTEST',
          NULL, 'HEALTHY', 'VALID',
          CASE WHEN v_route.billing_account_id IS NULL THEN NULL ELSE 'ENABLED' END,
          CASE WHEN v_route.default_billing_instrument_id IS NULL THEN NULL ELSE 'ENABLED' END,
          v_now, v_until)
  RETURNING o.observation_id INTO v_account;

  RETURN QUERY SELECT v_provider, v_account, v_until;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 4. Enable or disable one Profile@version (ADR-0060 D-H 4): the no-restart stop for one route.
--    Caller: adapters::reasoning_route_onboarding::set_profile_enabled (role_maintenance), one
--    transaction with its §77 audit row. Fence: p_tenant = humaux.tenant_id. A re-enable runs the
--    0128 ownership trigger again. Refusals: profile_not_found. Owner: role_migration_owner.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.set_reasoning_profile_enabled(
  p_tenant uuid, p_profile_id uuid, p_profile_version bigint, p_enabled boolean
) RETURNS TABLE (enabled boolean, disposition text)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
DECLARE
  v_enabled boolean;
BEGIN
  IF p_tenant IS NULL OR p_profile_id IS NULL OR p_profile_version IS NULL OR p_enabled IS NULL THEN
    RAISE EXCEPTION 'invalid profile-state request' USING ERRCODE = '22023';
  END IF;
  IF p_tenant IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  SELECT p.enabled INTO v_enabled FROM control.reasoning_profiles p
   WHERE p.tenant_id = p_tenant AND p.profile_id = p_profile_id
     AND p.profile_version = p_profile_version
   FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'profile_not_found' USING ERRCODE = '55000';
  END IF;
  IF v_enabled = p_enabled THEN
    RETURN QUERY SELECT v_enabled, 'existing'::text;
    RETURN;
  END IF;
  UPDATE control.reasoning_profiles p SET enabled = p_enabled, updated_at = clock_timestamp()
   WHERE p.tenant_id = p_tenant AND p.profile_id = p_profile_id
     AND p.profile_version = p_profile_version;
  RETURN QUERY SELECT p_enabled, 'updated'::text;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 5. Route status of one tenant (ADR-0060 ruling E3 (c); L12's "unbound domains" view).
--    Caller: adapters::reasoning_route_onboarding::route_status (role_maintenance), a read-only
--    transaction. Fence: p_tenant = humaux.tenant_id.
--    One row per (ACTIVE domain, bindable purpose): the current Binding@version and its single
--    Profile@version (NULL when unbound), and the latest provider / account observation of that
--    profile's exact identity, chosen the way the 0130 resolver chooses it (0207 pattern). health:
--    UNBOUND, MISSING (no observation), STALE (one is past valid_until), DENIED (a verdict is not
--    HEALTHY / VALID) or ADMISSIBLE. valid_until is the earlier of the two. No secret, no endpoint_ref.
--    Owner: role_migration_owner.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.reasoning_route_status(p_tenant uuid)
RETURNS TABLE (reasoning_domain_id uuid, purpose text, binding_id uuid, binding_version bigint,
               profile_id uuid, profile_version bigint, profile_enabled boolean,
               processor_id text, provider_model_id text, model_revision text, endpoint_id uuid,
               region text, egress_processor_id uuid, credential_ref uuid, capabilities text[],
               provider_verdict text, account_verdict text, credential_verdict text,
               valid_until timestamptz, health text)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
BEGIN
  IF p_tenant IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  RETURN QUERY
  WITH slot AS (
    SELECT d.tenant_id, d.reasoning_domain_id, pu.purpose
      FROM control.private_reasoning_domains d
     CROSS JOIN (VALUES ('PRIVATE_DISTILL_TEXT'), ('PRIVATE_CONSOLIDATE')) AS pu(purpose)
     WHERE d.tenant_id = p_tenant AND d.status = 'ACTIVE'
  ), route AS (
    SELECT slot.reasoning_domain_id, slot.purpose, b.binding_id, b.binding_version,
           profile.profile_id, profile.profile_version, profile.enabled, account.processor_id,
           profile.processor_model_id, model.provider_model_id, model.model_revision,
           endpoint.endpoint_id, endpoint.endpoint_ref, endpoint.region, endpoint.service_tier,
           endpoint.egress_processor_id, profile.provider_account_id, profile.credential_ref,
           profile.billing_account_id, profile.default_billing_instrument_id, profile.capabilities,
           slot.tenant_id
      FROM slot
      LEFT JOIN control.reasoning_route_bindings b
        ON b.tenant_id = slot.tenant_id AND b.reasoning_domain_id = slot.reasoning_domain_id
       AND b.purpose = slot.purpose AND b.effective_to IS NULL
      LEFT JOIN control.reasoning_route_candidates candidate
        ON (candidate.tenant_id, candidate.route_policy_id, candidate.route_policy_version) =
           (b.tenant_id, b.route_policy_id, b.route_policy_version)
      LEFT JOIN control.reasoning_profiles profile
        ON (profile.tenant_id, profile.profile_id, profile.profile_version) =
           (candidate.tenant_id, candidate.profile_id, candidate.profile_version)
      LEFT JOIN control.provider_accounts account
        ON account.tenant_id = profile.tenant_id
       AND account.provider_account_id = profile.provider_account_id
      LEFT JOIN control.provider_endpoints endpoint
        ON endpoint.tenant_id = profile.tenant_id AND endpoint.endpoint_id = profile.endpoint_id
      LEFT JOIN control.processor_models model
        ON model.processor_model_id = profile.processor_model_id
  )
  SELECT route.reasoning_domain_id, route.purpose, route.binding_id, route.binding_version,
         route.profile_id, route.profile_version, route.enabled, route.processor_id,
         route.provider_model_id, route.model_revision, route.endpoint_id, route.region,
         route.egress_processor_id, route.credential_ref, route.capabilities, ph.verdict,
         ah.account_verdict, ah.credential_verdict, least(ph.valid_until, ah.valid_until),
         CASE
           WHEN route.binding_id IS NULL THEN 'UNBOUND'
           WHEN ph.valid_until IS NULL OR ah.valid_until IS NULL THEN 'MISSING'
           WHEN clock_timestamp() >= least(ph.valid_until, ah.valid_until) THEN 'STALE'
           WHEN ph.verdict <> 'HEALTHY' OR ah.account_verdict <> 'HEALTHY'
                OR ah.credential_verdict <> 'VALID' THEN 'DENIED'
           ELSE 'ADMISSIBLE'
         END
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
    ) ph ON route.profile_id IS NOT NULL
    LEFT JOIN LATERAL (
      SELECT o.account_verdict, o.credential_verdict, o.valid_until
        FROM ops.reasoning_account_health_observations o
       WHERE o.tenant_id = route.tenant_id
         AND o.provider_account_id = route.provider_account_id
         AND o.credential_ref = route.credential_ref
         AND o.billing_account_id IS NOT DISTINCT FROM route.billing_account_id
         AND o.billing_instrument_id IS NOT DISTINCT FROM route.default_billing_instrument_id
       ORDER BY o.observed_at DESC, o.observation_id DESC
       LIMIT 1
    ) ah ON route.profile_id IS NOT NULL
   ORDER BY route.reasoning_domain_id, route.purpose;
END;
$$;

DO $$
DECLARE
  sig text;
BEGIN
  FOREACH sig IN ARRAY ARRAY[
    'control.register_reasoning_profile(uuid,uuid,text,text,text,text[],jsonb,bytea,text,text,text,uuid,uuid,uuid)',
    'control.bind_reasoning_domain(uuid,uuid,text,uuid,bigint)',
    'control.attest_reasoning_route_health(uuid,uuid,bigint,bigint)',
    'control.set_reasoning_profile_enabled(uuid,uuid,bigint,boolean)',
    'control.reasoning_route_status(uuid)'
  ] LOOP
    EXECUTE format('ALTER FUNCTION %s OWNER TO role_migration_owner', sig);
    EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC, role_gateway, role_private_worker, '
                   'role_consolidation_worker, role_public_worker, role_retrieval_worker, '
                   'role_batch_issuer, role_admin', sig);
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO role_maintenance', sig);
    EXECUTE format('COMMENT ON FUNCTION %s IS %L', sig,
                   '§11.2.3 / §11.2.5 / ADR-0060 D-H: operator reasoning-route door. Caller: '
                   'adapters::reasoning_route_onboarding (humaux-maintenance reasoning ...). '
                   'EXECUTE: role_maintenance only.');
  END LOOP;
END
$$;
