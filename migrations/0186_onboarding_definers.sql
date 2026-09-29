-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0185). ADR-0053 D-C / D-D (card 28):
-- the eight doors of production onboarding. Each is owned by role_migration_owner, SECURITY
-- DEFINER, `SET search_path = pg_catalog` with every name fully qualified, REVOKE ALL FROM PUBLIC,
-- EXECUTE to role_maintenance ONLY (the §4.2 operator-write role; `humaux-maintenance` and
-- `xtask e2e-seed` reach them through crates/adapters/src/provisioning.rs). No table grant is
-- added: role_maintenance still writes none of these tables directly, so `lifecycle = 'READY'`
-- is reachable only through activate_empty_family and a key is minted only after the membership
-- checks of issue_api_key.
--
-- role_migration_owner is NOSUPERUSER / NOBYPASSRLS and the touched tables are FORCE RLS, so each
-- write is still checked by its tenant policy: onboard_tenant installs `humaux.tenant_id` itself
-- (set_config(..., true): the setting outlives the call until the CALLER's transaction ends, so the
-- Rust caller opens one transaction per tenant), every other tenant-scoped door asserts
-- `p_tenant_id = <installed tenant GUC>` first (0176 lesson: an owner door must not become a
-- cross-tenant door because an owner arm or the owner-select policy lets it see other tenants).
--
-- Refusals are SQLSTATE 55000 (object_not_in_prerequisite_state) with the machine-readable reason
-- as the message; input violations are 22023; a tenant-context mismatch is 42501.
--
-- The ticket-family triples are never spelled here: onboard_* take them as a flat text[] of
-- (domain, projection_kind, projection_version) triples from domain::ticket_family::TicketFamily
-- (§78.2), and the placement family of a triple is `domain || '_' || projection_version`, the same
-- derivation as TicketFamily::collection_name (pinned equal to RetrievalFamily's DB spelling by
-- adapters::qdrant::ticket_family_tests).

-- ---------------------------------------------------------------------------------------------
-- 1. One 0117 admission tier row. Never overwrites an active row (0093 NULLS NOT DISTINCT unique).
--    Only the four canonical shapes: GLOBAL (-, -, -), REGION (-, r, -), TENANT (t, -, -),
--    PURPOSE (t, -, p).
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.ensure_admission_tier(
  p_provider_id text,
  p_region text,
  p_tenant_id uuid,
  p_purpose text,
  p_tpm bigint,
  p_rpm bigint
) RETURNS boolean
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_n bigint;
BEGIN
  IF p_provider_id IS NULL OR length(p_provider_id) = 0
     OR p_tpm IS NULL OR p_tpm <= 0 OR p_rpm IS NULL OR p_rpm <= 0
     OR NOT ((p_tenant_id IS NULL AND p_purpose IS NULL)
             OR (p_tenant_id IS NOT NULL AND p_region IS NULL)) THEN
    RAISE EXCEPTION 'invalid admission tier' USING ERRCODE = '22023';
  END IF;
  IF p_tenant_id IS NOT NULL
     AND p_tenant_id IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  INSERT INTO control.retrieval_provider_admission_limits
    (tenant_id, provider_id, region, purpose, tpm_limit, rpm_limit, effective_from)
  VALUES (p_tenant_id, p_provider_id, p_region, p_purpose, p_tpm, p_rpm,
          clock_timestamp() - interval '1 second')
  ON CONFLICT (provider_id, region, tenant_id, purpose) WHERE effective_to IS NULL DO NOTHING;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  RETURN v_n = 1;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 2. A user by canonical email. The email is stored UNVERIFIED (verified_at NULL, §74 is
--    post-go-live); nothing may read it as a verified login identity.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.ensure_user(
  p_email_original text,
  p_email_canonical text
) RETURNS TABLE (user_id uuid, created boolean)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
DECLARE
  v_user uuid;
  v_new uuid;
  v_n bigint;
BEGIN
  IF p_email_original IS NULL OR length(p_email_original) NOT BETWEEN 3 AND 320
     OR p_email_canonical IS NULL OR length(p_email_canonical) NOT BETWEEN 3 AND 320 THEN
    RAISE EXCEPTION 'invalid email' USING ERRCODE = '22023';
  END IF;
  SELECT e.user_id INTO v_user FROM control.user_emails e WHERE e.canonical_email = p_email_canonical;
  IF v_user IS NOT NULL THEN
    RETURN QUERY SELECT v_user, false;
    RETURN;
  END IF;
  INSERT INTO control.users AS u (state) VALUES ('ACTIVE') RETURNING u.user_id INTO v_new;
  INSERT INTO control.user_emails (user_id, original_email, canonical_email, is_primary)
  VALUES (v_new, p_email_original, p_email_canonical, true)
  ON CONFLICT (canonical_email) DO NOTHING;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  IF v_n = 1 THEN
    RETURN QUERY SELECT v_new, true;
    RETURN;
  END IF;
  -- A concurrent onboarding committed the same email first: drop our orphan user row and
  -- answer with the winner's (READ COMMITTED: this statement sees the committed row).
  DELETE FROM control.users u WHERE u.user_id = v_new;
  SELECT e.user_id INTO v_user FROM control.user_emails e WHERE e.canonical_email = p_email_canonical;
  RETURN QUERY SELECT v_user, false;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 3. A workspace in PROVISIONING, its OWNER workspace membership, and one zeroed checkpoint row
--    per family triple ("initialise family/version"). The required-family set of the workspace
--    IS this set of checkpoint rows.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.onboard_workspace(
  p_tenant_id uuid,
  p_name text,
  p_owner_user uuid,
  p_families text[]
) RETURNS TABLE (workspace_id uuid, created boolean)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
DECLARE
  v_ws uuid;
  v_prev_user text;
  i integer;
BEGIN
  IF p_tenant_id IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  IF p_name IS NULL OR length(p_name) NOT BETWEEN 1 AND 128 OR p_owner_user IS NULL
     OR p_families IS NULL OR cardinality(p_families) = 0 OR cardinality(p_families) % 3 <> 0
     OR array_position(p_families, NULL) IS NOT NULL THEN
    RAISE EXCEPTION 'invalid workspace onboarding request' USING ERRCODE = '22023';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM control.memberships m
                  WHERE m.tenant_id = p_tenant_id AND m.user_id = p_owner_user AND m.state = 'ACTIVE') THEN
    RAISE EXCEPTION 'owner_not_member' USING ERRCODE = '55000';
  END IF;
  INSERT INTO control.workspaces AS w (tenant_id, name, lifecycle)
  VALUES (p_tenant_id, p_name, 'PROVISIONING')
  ON CONFLICT (tenant_id, name) WHERE lifecycle <> 'LEGACY' DO NOTHING
  RETURNING w.workspace_id INTO v_ws;
  IF v_ws IS NULL THEN
    SELECT w.workspace_id INTO v_ws FROM control.workspaces w
     WHERE w.tenant_id = p_tenant_id AND w.name = p_name AND w.lifecycle <> 'LEGACY';
    RETURN QUERY SELECT v_ws, false;
    RETURN;
  END IF;
  -- 0162's upsert must also pass workspace_memberships' RESTRICTIVE self-read policy (ON CONFLICT
  -- DO UPDATE checks the SELECT policies), so it runs under the owner's user GUC; the caller's
  -- value is restored right after.
  v_prev_user := current_setting('humaux.user_id', true);
  PERFORM set_config('humaux.user_id', p_owner_user::text, true);
  PERFORM control.set_workspace_membership(p_tenant_id, v_ws, p_owner_user, 'OWNER', 'ACTIVE');
  PERFORM set_config('humaux.user_id', coalesce(v_prev_user, ''), true);
  FOR i IN 1 .. cardinality(p_families) / 3 LOOP
    INSERT INTO projection.stream_checkpoints
      (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version)
    VALUES (p_tenant_id, 'workspace', v_ws,
            p_families[3 * i - 2], p_families[3 * i - 1], p_families[3 * i]);
  END LOOP;
  RETURN QUERY SELECT v_ws, true;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 4. A tenant: refuses unless the deployment-level GLOBAL and REGION tiers exist; then tenant
--    (ACTIVE, onboarding_name), OWNER membership, reasoning domain owned by the owner, entitlement
--    snapshot, the TENANT + two PURPOSE tiers, and onboard_workspace. Idempotent on
--    onboarding_name: a re-run (or the loser of a concurrent run) writes nothing and answers the
--    existing ids with created = false.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.onboard_tenant(
  p_name text,
  p_owner_user uuid,
  p_workspace_name text,
  p_reasoning_domain_name text,
  p_plan_limit bigint,
  p_period_start timestamptz,
  p_period_end timestamptz,
  p_provider_id text,
  p_region text,
  p_tenant_tpm bigint,
  p_tenant_rpm bigint,
  p_families text[]
) RETURNS TABLE (tenant_id uuid, workspace_id uuid, reasoning_domain_id uuid, created boolean)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
DECLARE
  v_tenant uuid := uuidv7();
  v_inserted uuid;
  v_ws uuid;
  v_rd uuid;
BEGIN
  IF p_name IS NULL OR length(p_name) NOT BETWEEN 1 AND 128 OR p_owner_user IS NULL
     OR p_workspace_name IS NULL OR length(p_workspace_name) NOT BETWEEN 1 AND 128
     OR p_reasoning_domain_name IS NULL OR length(p_reasoning_domain_name) NOT BETWEEN 1 AND 128
     OR p_plan_limit IS NULL OR p_plan_limit <= 0
     OR p_period_start IS NULL OR p_period_end IS NULL OR p_period_end <= p_period_start
     OR p_provider_id IS NULL OR length(p_provider_id) = 0
     OR p_region IS NULL OR length(p_region) = 0
     OR p_tenant_tpm IS NULL OR p_tenant_tpm <= 0 OR p_tenant_rpm IS NULL OR p_tenant_rpm <= 0
     OR p_families IS NULL OR cardinality(p_families) = 0 OR cardinality(p_families) % 3 <> 0 THEN
    RAISE EXCEPTION 'invalid onboarding request' USING ERRCODE = '22023';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM control.retrieval_provider_admission_limits l
                  WHERE l.provider_id = p_provider_id AND l.tenant_id IS NULL AND l.region IS NULL
                    AND l.purpose IS NULL AND l.effective_to IS NULL)
     OR NOT EXISTS (SELECT 1 FROM control.retrieval_provider_admission_limits l
                     WHERE l.provider_id = p_provider_id AND l.tenant_id IS NULL
                       AND l.region = p_region AND l.purpose IS NULL AND l.effective_to IS NULL) THEN
    RAISE EXCEPTION 'deployment_admission_missing' USING ERRCODE = '55000';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM control.users u WHERE u.user_id = p_owner_user AND u.state = 'ACTIVE') THEN
    RAISE EXCEPTION 'owner_not_active' USING ERRCODE = '55000';
  END IF;

  PERFORM set_config('humaux.tenant_id', v_tenant::text, true);
  INSERT INTO control.tenants AS t (tenant_id, name, state, onboarding_name)
  VALUES (v_tenant, p_name, 'ACTIVE', p_name)
  ON CONFLICT (onboarding_name) DO NOTHING
  RETURNING t.tenant_id INTO v_inserted;

  IF v_inserted IS NULL THEN
    SELECT t.tenant_id INTO v_tenant FROM control.tenants t WHERE t.onboarding_name = p_name;
    PERFORM set_config('humaux.tenant_id', v_tenant::text, true);
    IF NOT EXISTS (SELECT 1 FROM control.memberships m
                    WHERE m.tenant_id = v_tenant AND m.user_id = p_owner_user
                      AND m.role = 'OWNER' AND m.state = 'ACTIVE') THEN
      RAISE EXCEPTION 'onboarding_name_taken' USING ERRCODE = '55000';
    END IF;
    SELECT w.workspace_id INTO v_ws FROM control.workspaces w
     WHERE w.tenant_id = v_tenant AND w.name = p_workspace_name AND w.lifecycle <> 'LEGACY';
    IF v_ws IS NULL THEN
      RAISE EXCEPTION 'workspace_name_mismatch' USING ERRCODE = '55000';
    END IF;
    SELECT d.reasoning_domain_id INTO v_rd FROM control.private_reasoning_domains d
     WHERE d.tenant_id = v_tenant AND d.name = p_reasoning_domain_name
     ORDER BY d.created_at LIMIT 1;
    RETURN QUERY SELECT v_tenant, v_ws, v_rd, false;
    RETURN;
  END IF;

  INSERT INTO control.memberships (tenant_id, user_id, role, state)
  VALUES (v_tenant, p_owner_user, 'OWNER', 'ACTIVE');
  INSERT INTO control.private_reasoning_domains AS d (tenant_id, name, owner_user_id, status)
  VALUES (v_tenant, p_reasoning_domain_name, p_owner_user, 'ACTIVE')
  RETURNING d.reasoning_domain_id INTO v_rd;
  -- The shape control.issue_quota_window (0113) validates; values from the operator.
  INSERT INTO control.entitlement_snapshots (tenant_id, effective, source_grant_ids, computed_at)
  VALUES (
    v_tenant,
    jsonb_build_object('mcp.billable_operations.per_period', jsonb_build_object(
      'limit', p_plan_limit,
      'period', 'subscription_period',
      'charge_policy', 'success_only',
      'period_start', to_char(p_period_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
      'period_end', to_char(p_period_end AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))),
    '{}'::uuid[],
    clock_timestamp());
  PERFORM control.ensure_admission_tier(p_provider_id, NULL, v_tenant, NULL, p_tenant_tpm, p_tenant_rpm);
  PERFORM control.ensure_admission_tier(p_provider_id, NULL, v_tenant, 'RETRIEVAL_EMBEDDING', p_tenant_tpm, p_tenant_rpm);
  PERFORM control.ensure_admission_tier(p_provider_id, NULL, v_tenant, 'RETRIEVAL_RERANK', p_tenant_tpm, p_tenant_rpm);
  SELECT o.workspace_id INTO v_ws
    FROM control.onboard_workspace(v_tenant, p_workspace_name, p_owner_user, p_families) o;
  RETURN QUERY SELECT v_tenant, v_ws, v_rd, true;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 5. An ACTIVE workspace-bound API key. The hash is computed by the caller with the §73.5
--    primitive (never plaintext here). Refuses unless the user holds an ACTIVE tenant membership
--    and an ACTIVE workspace membership. Epochs are the current ones. Idempotent on prefix: the
--    same prefix with the same binding answers created = false; another binding is refused.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.issue_api_key(
  p_tenant_id uuid,
  p_user_id uuid,
  p_workspace_id uuid,
  p_prefix text,
  p_key_hash bytea,
  p_scopes text[]
) RETURNS TABLE (api_key_id uuid, created boolean)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
DECLARE
  v_existing record;
  v_id uuid;
  v_prev_user text;
  v_ws_ok boolean;
  v_tenant_epoch bigint;
  v_user_epoch bigint;
BEGIN
  IF p_tenant_id IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  IF p_user_id IS NULL OR p_workspace_id IS NULL
     OR p_prefix IS NULL OR p_prefix !~ '^[a-z0-9]{4,64}$'
     OR p_key_hash IS NULL OR octet_length(p_key_hash) <> 32
     OR p_scopes IS NULL OR cardinality(p_scopes) = 0 THEN
    RAISE EXCEPTION 'invalid api key request' USING ERRCODE = '22023';
  END IF;
  SELECT k.api_key_id, k.tenant_id, k.user_id, k.workspace_id INTO v_existing
    FROM control.api_keys k WHERE k.prefix = p_prefix;
  IF FOUND THEN
    IF v_existing.tenant_id = p_tenant_id AND v_existing.user_id = p_user_id
       AND v_existing.workspace_id = p_workspace_id THEN
      RETURN QUERY SELECT v_existing.api_key_id, false;
      RETURN;
    END IF;
    RAISE EXCEPTION 'api_key_prefix_conflict' USING ERRCODE = '55000';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM control.memberships m
                  WHERE m.tenant_id = p_tenant_id AND m.user_id = p_user_id AND m.state = 'ACTIVE') THEN
    RAISE EXCEPTION 'user_not_member' USING ERRCODE = '55000';
  END IF;
  -- workspace_memberships carries a RESTRICTIVE self-read policy (0162): the check runs under
  -- the target user's GUC and the caller's value is restored before anything else happens.
  v_prev_user := current_setting('humaux.user_id', true);
  PERFORM set_config('humaux.user_id', p_user_id::text, true);
  SELECT EXISTS (SELECT 1 FROM control.workspace_memberships wm
                  WHERE wm.tenant_id = p_tenant_id AND wm.workspace_id = p_workspace_id
                    AND wm.user_id = p_user_id AND wm.state = 'ACTIVE') INTO v_ws_ok;
  PERFORM set_config('humaux.user_id', coalesce(v_prev_user, ''), true);
  IF NOT v_ws_ok THEN
    RAISE EXCEPTION 'user_not_workspace_member' USING ERRCODE = '55000';
  END IF;
  SELECT t.security_epoch INTO v_tenant_epoch FROM control.tenants t WHERE t.tenant_id = p_tenant_id;
  SELECT u.security_epoch INTO v_user_epoch FROM control.users u WHERE u.user_id = p_user_id;
  INSERT INTO control.api_keys AS k
    (tenant_id, prefix, key_hash, status, scopes, authorization_version, user_id, workspace_id,
     tenant_security_epoch, user_security_epoch)
  VALUES (p_tenant_id, p_prefix, p_key_hash, 'ACTIVE', p_scopes, 1, p_user_id, p_workspace_id,
          v_tenant_epoch, v_user_epoch)
  ON CONFLICT (prefix) DO NOTHING
  RETURNING k.api_key_id INTO v_id;
  IF v_id IS NULL THEN
    RAISE EXCEPTION 'api_key_prefix_conflict' USING ERRCODE = '55000';
  END IF;
  RETURN QUERY SELECT v_id, true;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 6. Revoke by prefix. Idempotent: an already REVOKED key answers changed = false.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION control.revoke_api_key(
  p_tenant_id uuid,
  p_prefix text
) RETURNS TABLE (api_key_id uuid, changed boolean)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
DECLARE
  v_id uuid;
BEGIN
  IF p_tenant_id IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  UPDATE control.api_keys k SET status = 'REVOKED', revoked_at = now(), updated_at = now()
   WHERE k.tenant_id = p_tenant_id AND k.prefix = p_prefix AND k.status <> 'REVOKED'
  RETURNING k.api_key_id INTO v_id;
  IF v_id IS NOT NULL THEN
    RETURN QUERY SELECT v_id, true;
    RETURN;
  END IF;
  SELECT k.api_key_id INTO v_id FROM control.api_keys k
   WHERE k.tenant_id = p_tenant_id AND k.prefix = p_prefix;
  IF v_id IS NULL THEN
    RAISE EXCEPTION 'api_key_not_found' USING ERRCODE = '55000';
  END IF;
  RETURN QUERY SELECT v_id, false;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 7. The §17.3 placement row (SHARED_FALLBACK, zero counts, STABLE). An existing row naming
--    another collection is refused, never silently moved.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION projection.ensure_tenant_placement(
  p_tenant_id uuid,
  p_projection_family text,
  p_collection text
) RETURNS boolean
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_n bigint;
  v_existing text;
BEGIN
  IF p_tenant_id IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  IF p_projection_family IS NULL OR p_collection IS NULL OR length(p_collection) = 0 THEN
    RAISE EXCEPTION 'invalid placement request' USING ERRCODE = '22023';
  END IF;
  INSERT INTO projection.tenant_placements
    (tenant_id, projection_family, collection_name, shard_key, placement_class,
     point_count, bytes_estimate, promotion_state)
  VALUES (p_tenant_id, p_projection_family, p_collection, NULL, 'SHARED_FALLBACK', 0, 0, 'STABLE')
  ON CONFLICT (tenant_id, projection_family) DO NOTHING;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  IF v_n = 1 THEN
    RETURN true;
  END IF;
  SELECT p.collection_name INTO v_existing FROM projection.tenant_placements p
   WHERE p.tenant_id = p_tenant_id AND p.projection_family = p_projection_family;
  IF v_existing IS DISTINCT FROM p_collection THEN
    RAISE EXCEPTION 'placement_conflict' USING ERRCODE = '55000';
  END IF;
  RETURN false;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 8. ADR-0053 D-D: the VerifiedEmpty first activation of one family+version. The caller took the
--    Qdrant probe outside any transaction and passes its facts in; this function re-derives every
--    PostgreSQL-side fact under row locks. Order: lock the workspace row (refuses unless
--    PROVISIONING, after the idempotent EXISTING answer for an already-receipted family), lock the
--    checkpoint row (missing = family_not_initialized, never COALESCE 0), placement FOR SHARE,
--    expected = every highwater + stream_log + outbox + points counts all 0 (not_empty),
--    open gaps 0, probe 0, no serving row for the family. Then serving = true, the receipt, and
--    READY when no checkpoint family of the workspace lacks a serving row. A REFUSED answer has
--    written nothing. The Rust caller re-judges the returned facts with
--    projection::serving::evaluate_switch before it commits.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION projection.activate_empty_family(
  p_tenant_id uuid,
  p_workspace_id uuid,
  p_domain text,
  p_projection_kind text,
  p_projection_version text,
  p_collection text,
  p_generation text,
  p_probe_id uuid,
  p_probe_visible bigint,
  p_probed_at timestamptz
) RETURNS TABLE (
  outcome text,
  reason text,
  initialized_head bigint,
  open_gaps bigint,
  first_activation boolean,
  workspace_ready boolean
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
DECLARE
  v_lifecycle text;
  v_ck record;
  v_collection text;
  v_stream bigint;
  v_outbox bigint;
  v_points bigint;
  v_gaps bigint;
  v_n bigint;
  v_ready boolean := false;
BEGIN
  IF p_tenant_id IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  IF p_workspace_id IS NULL OR p_domain IS NULL OR p_projection_kind IS NULL
     OR p_projection_version IS NULL OR p_collection IS NULL OR p_generation IS NULL
     OR p_probe_id IS NULL OR p_probe_visible IS NULL OR p_probe_visible < 0
     OR p_probed_at IS NULL THEN
    RAISE EXCEPTION 'invalid activation request' USING ERRCODE = '22023';
  END IF;

  SELECT w.lifecycle INTO v_lifecycle FROM control.workspaces w
   WHERE w.tenant_id = p_tenant_id AND w.workspace_id = p_workspace_id
   FOR UPDATE;
  IF NOT FOUND THEN
    RETURN QUERY SELECT 'REFUSED'::text, 'workspace_not_found'::text,
                        NULL::bigint, NULL::bigint, NULL::boolean, false;
    RETURN;
  END IF;

  IF EXISTS (SELECT 1 FROM projection.family_activations a
              WHERE a.tenant_id = p_tenant_id AND a.scope_kind = 'workspace'
                AND a.scope_id = p_workspace_id AND a.domain = p_domain
                AND a.projection_kind = p_projection_kind
                AND a.projection_version = p_projection_version) THEN
    RETURN QUERY SELECT 'EXISTING'::text, NULL::text, 0::bigint, NULL::bigint, false,
                        v_lifecycle = 'READY';
    RETURN;
  END IF;

  IF v_lifecycle <> 'PROVISIONING' THEN
    RETURN QUERY SELECT 'REFUSED'::text, 'not_provisioning'::text,
                        NULL::bigint, NULL::bigint, NULL::boolean, v_lifecycle = 'READY';
    RETURN;
  END IF;

  SELECT c.issued_highwater, c.evidence_highwater, c.knowledge_highwater, c.projection_highwater
    INTO v_ck
    FROM projection.stream_checkpoints c
   WHERE c.tenant_id = p_tenant_id AND c.scope_kind = 'workspace' AND c.scope_id = p_workspace_id
     AND c.domain = p_domain AND c.projection_kind = p_projection_kind
     AND c.projection_version = p_projection_version
   FOR UPDATE;
  IF NOT FOUND THEN
    RETURN QUERY SELECT 'REFUSED'::text, 'family_not_initialized'::text,
                        NULL::bigint, NULL::bigint, NULL::boolean, false;
    RETURN;
  END IF;

  SELECT p.collection_name INTO v_collection FROM projection.tenant_placements p
   WHERE p.tenant_id = p_tenant_id
     AND p.projection_family = p_domain || '_' || p_projection_version
   FOR SHARE;
  IF NOT FOUND THEN
    RETURN QUERY SELECT 'REFUSED'::text, 'placement_missing'::text,
                        v_ck.issued_highwater, NULL::bigint, NULL::boolean, false;
    RETURN;
  END IF;
  IF v_collection <> p_collection THEN
    RETURN QUERY SELECT 'REFUSED'::text, 'generation_mismatch'::text,
                        v_ck.issued_highwater, NULL::bigint, NULL::boolean, false;
    RETURN;
  END IF;

  SELECT count(*) INTO v_stream FROM projection.stream_log s
   WHERE s.tenant_id = p_tenant_id AND s.scope_kind = 'workspace' AND s.scope_id = p_workspace_id
     AND s.domain = p_domain AND s.projection_kind = p_projection_kind
     AND s.projection_version = p_projection_version;
  SELECT count(*) INTO v_outbox FROM ops.outbox o
    JOIN projection.stream_log s ON s.tenant_id = o.tenant_id AND s.commit_seq = o.commit_seq
   WHERE s.tenant_id = p_tenant_id AND s.scope_kind = 'workspace' AND s.scope_id = p_workspace_id
     AND s.domain = p_domain AND s.projection_kind = p_projection_kind
     AND s.projection_version = p_projection_version;
  SELECT count(*) INTO v_points FROM projection.private_memory_points m
   WHERE m.tenant_id = p_tenant_id AND m.scope_kind = 'workspace' AND m.scope_id = p_workspace_id
     AND m.domain = p_domain AND m.projection_kind = p_projection_kind
     AND m.projection_version = p_projection_version;
  SELECT count(*) INTO v_gaps FROM projection.processing_gaps g
   WHERE g.tenant_id = p_tenant_id AND g.scope_kind = 'workspace' AND g.scope_id = p_workspace_id
     AND g.domain = p_domain AND g.projection_kind = p_projection_kind
     AND g.projection_version = p_projection_version;

  IF v_ck.issued_highwater <> 0 OR v_ck.evidence_highwater <> 0 OR v_ck.knowledge_highwater <> 0
     OR v_ck.projection_highwater <> 0 OR v_stream <> 0 OR v_outbox <> 0 OR v_points <> 0 THEN
    RETURN QUERY SELECT 'REFUSED'::text, 'not_empty'::text,
                        v_ck.issued_highwater, v_gaps, NULL::boolean, false;
    RETURN;
  END IF;
  IF v_gaps <> 0 THEN
    RETURN QUERY SELECT 'REFUSED'::text, 'open_gaps'::text, 0::bigint, v_gaps, NULL::boolean, false;
    RETURN;
  END IF;
  IF p_probe_visible <> 0 THEN
    RETURN QUERY SELECT 'REFUSED'::text, 'probe_not_empty'::text, 0::bigint, 0::bigint,
                        NULL::boolean, false;
    RETURN;
  END IF;
  IF EXISTS (SELECT 1 FROM projection.stream_checkpoints c
              WHERE c.tenant_id = p_tenant_id AND c.scope_kind = 'workspace'
                AND c.scope_id = p_workspace_id AND c.domain = p_domain
                AND c.projection_kind = p_projection_kind AND c.serving) THEN
    RETURN QUERY SELECT 'REFUSED'::text, 'already_serving'::text, 0::bigint, 0::bigint, false, false;
    RETURN;
  END IF;

  UPDATE projection.stream_checkpoints c SET serving = true, shadow = false
   WHERE c.tenant_id = p_tenant_id AND c.scope_kind = 'workspace' AND c.scope_id = p_workspace_id
     AND c.domain = p_domain AND c.projection_kind = p_projection_kind
     AND c.projection_version = p_projection_version AND NOT c.serving;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  IF v_n <> 1 THEN
    RAISE EXCEPTION 'activation_update_missed' USING ERRCODE = '40001';
  END IF;

  INSERT INTO projection.family_activations
    (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version,
     evidence_kind, initialized_head, collection_name, collection_generation,
     probe_id, probe_visible, probed_at)
  VALUES (p_tenant_id, 'workspace', p_workspace_id, p_domain, p_projection_kind,
          p_projection_version, 'VERIFIED_EMPTY', 0, p_collection, p_generation,
          p_probe_id, p_probe_visible, p_probed_at);

  IF NOT EXISTS (
    SELECT 1 FROM projection.stream_checkpoints c
     WHERE c.tenant_id = p_tenant_id AND c.scope_kind = 'workspace' AND c.scope_id = p_workspace_id
       AND NOT EXISTS (SELECT 1 FROM projection.stream_checkpoints s
                        WHERE s.tenant_id = c.tenant_id AND s.scope_kind = c.scope_kind
                          AND s.scope_id = c.scope_id AND s.domain = c.domain
                          AND s.projection_kind = c.projection_kind AND s.serving)
  ) THEN
    UPDATE control.workspaces w SET lifecycle = 'READY', updated_at = now()
     WHERE w.tenant_id = p_tenant_id AND w.workspace_id = p_workspace_id;
    v_ready := true;
  END IF;

  RETURN QUERY SELECT 'ACTIVATED'::text, NULL::text, 0::bigint, v_gaps, true, v_ready;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- Owner, ACL: EXECUTE role_maintenance only (0161 / 0162 / 0167 chokepoint discipline).
-- ---------------------------------------------------------------------------------------------
ALTER FUNCTION control.ensure_admission_tier(text, text, uuid, text, bigint, bigint) OWNER TO role_migration_owner;
ALTER FUNCTION control.ensure_user(text, text) OWNER TO role_migration_owner;
ALTER FUNCTION control.onboard_workspace(uuid, text, uuid, text[]) OWNER TO role_migration_owner;
ALTER FUNCTION control.onboard_tenant(text, uuid, text, text, bigint, timestamptz, timestamptz, text, text, bigint, bigint, text[]) OWNER TO role_migration_owner;
ALTER FUNCTION control.issue_api_key(uuid, uuid, uuid, text, bytea, text[]) OWNER TO role_migration_owner;
ALTER FUNCTION control.revoke_api_key(uuid, text) OWNER TO role_migration_owner;
ALTER FUNCTION projection.ensure_tenant_placement(uuid, text, text) OWNER TO role_migration_owner;
ALTER FUNCTION projection.activate_empty_family(uuid, uuid, text, text, text, text, text, uuid, bigint, timestamptz) OWNER TO role_migration_owner;

REVOKE ALL ON FUNCTION control.ensure_admission_tier(text, text, uuid, text, bigint, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION control.ensure_user(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION control.onboard_workspace(uuid, text, uuid, text[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION control.onboard_tenant(text, uuid, text, text, bigint, timestamptz, timestamptz, text, text, bigint, bigint, text[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION control.issue_api_key(uuid, uuid, uuid, text, bytea, text[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION control.revoke_api_key(uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION projection.ensure_tenant_placement(uuid, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION projection.activate_empty_family(uuid, uuid, text, text, text, text, text, uuid, bigint, timestamptz) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION control.ensure_admission_tier(text, text, uuid, text, bigint, bigint) TO role_maintenance;
GRANT EXECUTE ON FUNCTION control.ensure_user(text, text) TO role_maintenance;
GRANT EXECUTE ON FUNCTION control.onboard_workspace(uuid, text, uuid, text[]) TO role_maintenance;
GRANT EXECUTE ON FUNCTION control.onboard_tenant(text, uuid, text, text, bigint, timestamptz, timestamptz, text, text, bigint, bigint, text[]) TO role_maintenance;
GRANT EXECUTE ON FUNCTION control.issue_api_key(uuid, uuid, uuid, text, bytea, text[]) TO role_maintenance;
GRANT EXECUTE ON FUNCTION control.revoke_api_key(uuid, text) TO role_maintenance;
GRANT EXECUTE ON FUNCTION projection.ensure_tenant_placement(uuid, text, text) TO role_maintenance;
GRANT EXECUTE ON FUNCTION projection.activate_empty_family(uuid, uuid, text, text, text, text, text, uuid, bigint, timestamptz) TO role_maintenance;
