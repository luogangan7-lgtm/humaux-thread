-- §11.2.2 / §11.2.6 R2 compatibility bootstrap.  This migration is deliberately
-- inert: it adds the owner-only receipt/resolution surface but does not project any
-- legacy row.  The explicit owner procedure below is the only writer.

CREATE TABLE control.reasoning_route_profile_receipts (
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  legacy_profile_id uuid NOT NULL REFERENCES control.user_reasoning_profiles(profile_id),
  profile_id uuid NOT NULL,
  profile_version bigint NOT NULL DEFAULT 1 CHECK (profile_version = 1),
  route_policy_id uuid NOT NULL,
  policy_version bigint NOT NULL DEFAULT 1 CHECK (policy_version = 1),
  execution_fingerprint text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, legacy_profile_id),
  UNIQUE (tenant_id, profile_id, profile_version),
  UNIQUE (tenant_id, route_policy_id, policy_version),
  FOREIGN KEY (tenant_id, profile_id, profile_version)
    REFERENCES control.reasoning_profiles(tenant_id, profile_id, profile_version),
  FOREIGN KEY (tenant_id, route_policy_id, policy_version)
    REFERENCES control.reasoning_route_policies(tenant_id, route_policy_id, policy_version)
);

CREATE TABLE control.reasoning_route_domain_receipts (
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  reasoning_domain_id uuid NOT NULL REFERENCES control.private_reasoning_domains(reasoning_domain_id),
  legacy_profile_id uuid REFERENCES control.user_reasoning_profiles(profile_id),
  disposition text NOT NULL CHECK (disposition IN ('BOOTSTRAPPED', 'NO_LEGACY_PROFILE')),
  binding_id uuid,
  binding_version bigint,
  execution_fingerprint text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, reasoning_domain_id),
  UNIQUE (tenant_id, binding_id, binding_version),
  CHECK (
    (disposition = 'NO_LEGACY_PROFILE' AND legacy_profile_id IS NULL
      AND binding_id IS NULL AND binding_version IS NULL)
    OR
    (disposition = 'BOOTSTRAPPED' AND legacy_profile_id IS NOT NULL
      AND binding_id IS NOT NULL AND binding_version = 1)
  ),
  FOREIGN KEY (tenant_id, binding_id, binding_version)
    REFERENCES control.reasoning_route_bindings(tenant_id, binding_id, binding_version)
);

ALTER TABLE control.reasoning_route_profile_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.reasoning_route_profile_receipts FORCE ROW LEVEL SECURITY;
ALTER TABLE control.reasoning_route_domain_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.reasoning_route_domain_receipts FORCE ROW LEVEL SECURITY;
CREATE POLICY reasoning_route_profile_receipts_owner_only
  ON control.reasoning_route_profile_receipts TO role_migration_owner
  USING ((tenant_id = current_setting('humaux.tenant_id', true)::uuid) OR current_user = 'role_migration_owner')
  WITH CHECK ((tenant_id = current_setting('humaux.tenant_id', true)::uuid) OR current_user = 'role_migration_owner');
CREATE POLICY reasoning_route_domain_receipts_owner_only
  ON control.reasoning_route_domain_receipts TO role_migration_owner
  USING ((tenant_id = current_setting('humaux.tenant_id', true)::uuid) OR current_user = 'role_migration_owner')
  WITH CHECK ((tenant_id = current_setting('humaux.tenant_id', true)::uuid) OR current_user = 'role_migration_owner');

CREATE FUNCTION control.reasoning_route_shadow_fingerprint(
  p_provider_id text,
  p_model_id text,
  p_credential_ref uuid,
  p_capabilities text[],
  p_processing_region text,
  p_custom_endpoint_policy jsonb,
  p_owner_user_id uuid,
  p_tenant_id uuid,
  p_class text
) RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT jsonb_build_object(
    'class', p_class,
    'processor_id', p_provider_id,
    'provider_model_id', p_model_id,
    'credential_ref', p_credential_ref,
    'capabilities', p_capabilities,
    'region', p_processing_region,
    'custom_endpoint_policy', p_custom_endpoint_policy,
    'owner_user_id', p_owner_user_id,
    'tenant_id', p_tenant_id,
    'trust_domain', 'USER_REASONING',
    'billing_responsibility', 'USER'
  )::text
$$;

CREATE FUNCTION control.assert_reasoning_route_shadow_receipts()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM control.reasoning_route_profile_receipts receipt
      LEFT JOIN control.user_reasoning_profiles legacy
        ON legacy.profile_id = receipt.legacy_profile_id AND legacy.tenant_id = receipt.tenant_id
      LEFT JOIN control.reasoning_profiles profile
        ON (profile.tenant_id, profile.profile_id, profile.profile_version)
         = (receipt.tenant_id, receipt.profile_id, receipt.profile_version)
      LEFT JOIN control.reasoning_route_policies policy
        ON (policy.tenant_id, policy.route_policy_id, policy.policy_version)
         = (receipt.tenant_id, receipt.route_policy_id, receipt.policy_version)
      LEFT JOIN control.reasoning_route_candidates candidate
        ON (candidate.tenant_id, candidate.route_policy_id, candidate.route_policy_version,
            candidate.profile_id, candidate.profile_version)
         = (receipt.tenant_id, receipt.route_policy_id, receipt.policy_version,
            receipt.profile_id, receipt.profile_version)
     WHERE legacy.profile_id IS NULL OR profile.profile_id IS NULL OR policy.route_policy_id IS NULL
        OR candidate.profile_id IS NULL
        OR policy.purpose <> 'CONTRIBUTION_DEIDENTIFY' OR policy.strategy <> 'PINNED'
        OR policy.lifecycle_state <> 'SHADOW' OR policy.effective_to IS NOT NULL
        OR NOT (candidate.enabled AND candidate.priority = 0 AND candidate.fallback_class = 'NONE')
        OR 1 <> (SELECT count(*) FROM control.reasoning_route_candidates one_candidate
                  WHERE one_candidate.tenant_id = receipt.tenant_id
                    AND one_candidate.route_policy_id = receipt.route_policy_id
                    AND one_candidate.route_policy_version = receipt.policy_version)
        OR receipt.execution_fingerprint IS DISTINCT FROM control.reasoning_route_profile_receipt_fingerprint(receipt.legacy_profile_id, receipt.profile_id, receipt.profile_version)
  ) THEN
    RAISE EXCEPTION 'R2 receipt corruption: profile receipt no longer resolves to exactly one frozen shadow route'
      USING ERRCODE = '55000';
  END IF;

  IF EXISTS (
    SELECT 1
      FROM control.reasoning_route_domain_receipts receipt
      LEFT JOIN control.private_reasoning_domains domain
        ON domain.reasoning_domain_id = receipt.reasoning_domain_id AND domain.tenant_id = receipt.tenant_id
      LEFT JOIN control.reasoning_route_bindings binding
        ON (binding.tenant_id, binding.binding_id, binding.binding_version)
         = (receipt.tenant_id, receipt.binding_id, receipt.binding_version)
     WHERE domain.reasoning_domain_id IS NULL
        OR (receipt.disposition = 'NO_LEGACY_PROFILE'
            AND (domain.user_reasoning_profile_id IS NOT NULL OR binding.binding_id IS NOT NULL
                 OR receipt.execution_fingerprint IS DISTINCT FROM control.reasoning_route_shadow_fingerprint(
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL, receipt.tenant_id, 'NO_LEGACY_PROFILE'
                 )))
        OR (receipt.disposition = 'BOOTSTRAPPED'
            AND (domain.user_reasoning_profile_id IS DISTINCT FROM receipt.legacy_profile_id
                 OR binding.binding_id IS NULL OR binding.reasoning_domain_id <> domain.reasoning_domain_id
                 OR binding.purpose <> 'CONTRIBUTION_DEIDENTIFY' OR binding.effective_to IS NOT NULL
                 OR NOT EXISTS (
                   SELECT 1 FROM control.reasoning_route_profile_receipts profile_receipt
                    WHERE (profile_receipt.tenant_id, profile_receipt.legacy_profile_id,
                           profile_receipt.route_policy_id, profile_receipt.policy_version)
                        = (receipt.tenant_id, receipt.legacy_profile_id,
                           binding.route_policy_id, binding.route_policy_version)
                     AND profile_receipt.execution_fingerprint = receipt.execution_fingerprint
                 )))
  ) THEN
    RAISE EXCEPTION 'R2 receipt corruption: domain receipt no longer resolves to its legacy tuple'
      USING ERRCODE = '55000';
  END IF;
END;
$$;

CREATE FUNCTION control.reasoning_route_profile_receipt_fingerprint(
  p_legacy_profile_id uuid, p_profile_id uuid, p_profile_version bigint
) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT jsonb_build_object(
    'absence_or_error_class','EXECUTABLE','legacy',jsonb_build_object('profile_id',legacy.profile_id,'provider_id',legacy.provider_id,'model_id',legacy.model_id,'credential_ref',legacy.credential_ref,'capabilities',legacy.capabilities,'region',legacy.processing_region,'custom_policy',legacy.custom_endpoint_policy,'owner',legacy.user_id,'tenant',legacy.tenant_id),
    'profile',jsonb_build_object('profile_id',profile.profile_id,'version',profile.profile_version,'trust',profile.trust_domain,'billing_responsibility',profile.billing_responsibility,'capabilities',profile.capabilities,'region',profile.processing_region,'custom_policy',profile.custom_endpoint_policy),
    'account',jsonb_build_object('id',account.provider_account_id,'processor',account.processor_id,'owner',account.owner_user_id,'trust',account.trust_domain,'billing_responsibility',account.billing_responsibility),
    'endpoint',jsonb_build_object('id',endpoint.endpoint_id,'ref',endpoint.endpoint_ref,'region',endpoint.region,'tier',endpoint.service_tier),
    'model',jsonb_build_object('id',model.processor_model_id,'processor',model.processor_id,'provider_model',model.provider_model_id,'revision',model.model_revision),
    'credential_binding',jsonb_build_object('credential_ref',credential.credential_ref,'owner',credential.owner_user_id,'account',credential.provider_account_id,'processor',credential.processor_id,'purpose',credential.credential_purpose,'eligibility',credential.invocation_eligibility),
    'billing',jsonb_build_object('account',billing.billing_account_id,'account_owner',billing.owner_user_id,'account_provider',billing.provider_account_id,'account_owner_kind',billing.owner_kind,'account_payer_kind',billing.payer_kind,'account_responsibility',billing.billing_responsibility,'instrument',instrument.billing_instrument_id,'instrument_kind',instrument.instrument_kind,'currency',instrument.currency,'owner',instrument.owner_user_id,'payer',instrument.payer_user_id,'owner_kind',instrument.owner_kind,'payer_kind',instrument.payer_kind,'eligibility',instrument.invocation_eligibility,'coverage_processor',instrument.coverage_processor_id,'coverage_model',instrument.coverage_provider_model_id,'coverage_revision',instrument.coverage_model_revision,'coverage_region',instrument.coverage_region,'coverage_tier',instrument.coverage_service_tier,'valid_from_epoch',extract(epoch FROM instrument.valid_from),'valid_to_epoch',CASE WHEN instrument.valid_to IS NULL THEN NULL ELSE extract(epoch FROM instrument.valid_to) END,'overage',instrument.overage_policy)
  )::text
  FROM control.user_reasoning_profiles legacy JOIN control.reasoning_profiles profile ON profile.profile_id=p_profile_id AND profile.profile_version=p_profile_version
  JOIN control.provider_accounts account ON account.provider_account_id=profile.provider_account_id JOIN control.provider_endpoints endpoint ON endpoint.endpoint_id=profile.endpoint_id JOIN control.processor_models model ON model.processor_model_id=profile.processor_model_id
  JOIN control.reasoning_credential_bindings credential ON credential.credential_ref=profile.credential_ref AND credential.provider_account_id=profile.provider_account_id
  JOIN control.provider_billing_accounts billing ON billing.billing_account_id=profile.billing_account_id JOIN control.provider_billing_instruments instrument ON instrument.billing_instrument_id=profile.default_billing_instrument_id
  WHERE legacy.profile_id=p_legacy_profile_id AND legacy.tenant_id=profile.tenant_id
$$;

CREATE FUNCTION control.reasoning_route_receipt_reject_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'R2 route receipts are append-only; use a successor migration for any repair'
    USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER reasoning_route_profile_receipts_append_only
  BEFORE UPDATE OR DELETE ON control.reasoning_route_profile_receipts
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_route_receipt_reject_mutation();
CREATE TRIGGER reasoning_route_domain_receipts_append_only
  BEFORE UPDATE OR DELETE ON control.reasoning_route_domain_receipts
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_route_receipt_reject_mutation();

CREATE FUNCTION control.bootstrap_contribution_deidentify_shadow()
RETURNS TABLE(profile_receipts bigint, domain_receipts bigint)
LANGUAGE plpgsql
AS $$
DECLARE
  legacy record;
  domain record;
  exact_count bigint;
  new_profile_id uuid;
  new_policy_id uuid;
  new_binding_id uuid;
  route_fingerprint text;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'R2 bootstrap requires READ COMMITTED isolation' USING ERRCODE = '40001';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('r2:contribution-deidentify-bootstrap', 0));
  PERFORM control.assert_reasoning_route_shadow_receipts();

  -- A null profile is an explicit non-route outcome.  It is recorded independently from
  -- profile receipts because several domains may share a non-null legacy profile.
  FOR domain IN
    SELECT d.tenant_id, d.reasoning_domain_id
      FROM control.private_reasoning_domains d
     WHERE d.status = 'ACTIVE' AND d.user_reasoning_profile_id IS NULL
       AND NOT EXISTS (
         SELECT 1 FROM control.reasoning_route_domain_receipts r
          WHERE (r.tenant_id, r.reasoning_domain_id) = (d.tenant_id, d.reasoning_domain_id)
       )
  LOOP
    INSERT INTO control.reasoning_route_domain_receipts(
      tenant_id, reasoning_domain_id, disposition, execution_fingerprint
    ) VALUES (
      domain.tenant_id, domain.reasoning_domain_id, 'NO_LEGACY_PROFILE',
      control.reasoning_route_shadow_fingerprint(NULL, NULL, NULL, NULL, NULL, NULL, NULL, domain.tenant_id, 'NO_LEGACY_PROFILE')
    );
  END LOOP;

  -- Validate every eligible legacy profile before creating any route object, so zero or
  -- multiple typed R1 identities abort the entire caller transaction.
  FOR legacy IN
    SELECT DISTINCT p.*
      FROM control.private_reasoning_domains d
      JOIN control.user_reasoning_profiles p ON p.profile_id = d.user_reasoning_profile_id
     WHERE d.status = 'ACTIVE' AND d.owner_user_id = p.user_id
       AND d.tenant_id = p.tenant_id AND p.enabled
       AND EXISTS (SELECT 1 FROM control.memberships m
                    WHERE m.tenant_id = d.tenant_id AND m.user_id = d.owner_user_id AND m.state = 'ACTIVE')
  LOOP
    SELECT count(*) INTO exact_count
      FROM control.provider_accounts account
      JOIN control.provider_endpoints endpoint
        ON endpoint.tenant_id = account.tenant_id AND endpoint.provider_account_id = account.provider_account_id
      JOIN control.processor_models model
        ON model.processor_id = account.processor_id
      JOIN control.reasoning_credential_bindings credential
        ON credential.tenant_id = account.tenant_id
       AND credential.provider_account_id = account.provider_account_id
       AND credential.credential_ref = legacy.credential_ref
      JOIN control.provider_billing_accounts billing
        ON billing.tenant_id = account.tenant_id AND billing.provider_account_id = account.provider_account_id
       AND billing.owner_user_id = legacy.user_id
      JOIN control.provider_billing_instruments instrument
        ON instrument.tenant_id = billing.tenant_id AND instrument.billing_account_id = billing.billing_account_id
     WHERE account.tenant_id = legacy.tenant_id AND account.owner_user_id = legacy.user_id
       AND account.processor_id = legacy.provider_id AND account.trust_domain = 'USER_REASONING'
       AND account.billing_responsibility = 'USER' AND account.status = 'ACTIVE' AND account.enabled
       AND endpoint.enabled AND (legacy.processing_region IS NULL OR endpoint.region = legacy.processing_region)
       AND model.provider_model_id = legacy.model_id AND model.model_revision IS NULL AND model.status = 'ACTIVE'
       AND legacy.capabilities <@ model.capabilities
       AND credential.owner_user_id = legacy.user_id AND credential.processor_id = account.processor_id
       AND credential.credential_purpose = 'USER_REASONING' AND credential.invocation_eligibility = 'API_CALLABLE'
       AND billing.enabled AND billing.billing_responsibility = 'USER'
       AND instrument.enabled AND instrument.invocation_eligibility = 'API_CALLABLE'
       AND instrument.owner_user_id = legacy.user_id AND instrument.payer_user_id = legacy.user_id
       AND instrument.valid_from <= clock_timestamp()
       AND (instrument.valid_to IS NULL OR instrument.valid_to > clock_timestamp())
       AND instrument.coverage_processor_id = model.processor_id
       AND instrument.coverage_provider_model_id = model.provider_model_id
       AND instrument.coverage_model_revision IS NOT DISTINCT FROM model.model_revision
       AND instrument.coverage_region = endpoint.region
       AND instrument.coverage_service_tier = endpoint.service_tier;
    IF exact_count <> 1 THEN
      RAISE EXCEPTION 'R2 bootstrap legacy profile % resolves to % typed identities (expected exactly 1)', legacy.profile_id, exact_count
        USING ERRCODE = '23514';
    END IF;
  END LOOP;

  FOR legacy IN
    SELECT DISTINCT p.*
      FROM control.private_reasoning_domains d
      JOIN control.user_reasoning_profiles p ON p.profile_id = d.user_reasoning_profile_id
     WHERE d.status = 'ACTIVE' AND d.owner_user_id = p.user_id
       AND d.tenant_id = p.tenant_id AND p.enabled
       AND EXISTS (SELECT 1 FROM control.memberships m
                    WHERE m.tenant_id = d.tenant_id AND m.user_id = d.owner_user_id AND m.state = 'ACTIVE')
       AND NOT EXISTS (SELECT 1 FROM control.reasoning_route_profile_receipts r
                        WHERE (r.tenant_id, r.legacy_profile_id) = (p.tenant_id, p.profile_id))
  LOOP
    SELECT account.provider_account_id, endpoint.endpoint_id, model.processor_model_id,
           billing.billing_account_id, instrument.billing_instrument_id
      INTO STRICT domain
      FROM control.provider_accounts account
      JOIN control.provider_endpoints endpoint ON endpoint.tenant_id = account.tenant_id AND endpoint.provider_account_id = account.provider_account_id
      JOIN control.processor_models model ON model.processor_id = account.processor_id
      JOIN control.reasoning_credential_bindings credential ON credential.tenant_id = account.tenant_id AND credential.provider_account_id = account.provider_account_id AND credential.credential_ref = legacy.credential_ref
      JOIN control.provider_billing_accounts billing ON billing.tenant_id = account.tenant_id AND billing.provider_account_id = account.provider_account_id AND billing.owner_user_id = legacy.user_id
      JOIN control.provider_billing_instruments instrument ON instrument.tenant_id = billing.tenant_id AND instrument.billing_account_id = billing.billing_account_id
     WHERE account.tenant_id = legacy.tenant_id AND account.owner_user_id = legacy.user_id
       AND account.processor_id = legacy.provider_id AND account.trust_domain = 'USER_REASONING' AND account.billing_responsibility = 'USER' AND account.status = 'ACTIVE' AND account.enabled
       AND endpoint.enabled AND (legacy.processing_region IS NULL OR endpoint.region = legacy.processing_region)
       AND model.provider_model_id = legacy.model_id AND model.model_revision IS NULL AND model.status = 'ACTIVE' AND legacy.capabilities <@ model.capabilities
       AND credential.owner_user_id = legacy.user_id AND credential.processor_id = account.processor_id AND credential.credential_purpose = 'USER_REASONING' AND credential.invocation_eligibility = 'API_CALLABLE'
       AND billing.enabled AND billing.billing_responsibility = 'USER' AND instrument.enabled AND instrument.invocation_eligibility = 'API_CALLABLE'
       AND instrument.owner_user_id = legacy.user_id AND instrument.payer_user_id = legacy.user_id AND instrument.valid_from <= clock_timestamp()
       AND (instrument.valid_to IS NULL OR instrument.valid_to > clock_timestamp())
       AND instrument.coverage_processor_id = model.processor_id AND instrument.coverage_provider_model_id = model.provider_model_id
       AND instrument.coverage_model_revision IS NOT DISTINCT FROM model.model_revision
       AND instrument.coverage_region = endpoint.region AND instrument.coverage_service_tier = endpoint.service_tier;

    INSERT INTO control.reasoning_profiles(
      tenant_id, owner_user_id, provider_account_id, endpoint_id, processor_model_id,
      credential_ref, billing_account_id, default_billing_instrument_id, capabilities,
      processing_region, custom_endpoint_policy
    ) VALUES (
      legacy.tenant_id, legacy.user_id, domain.provider_account_id, domain.endpoint_id,
      domain.processor_model_id, legacy.credential_ref, domain.billing_account_id,
      domain.billing_instrument_id, legacy.capabilities, legacy.processing_region,
      legacy.custom_endpoint_policy
    ) RETURNING profile_id INTO new_profile_id;
    INSERT INTO control.reasoning_route_policies(tenant_id, policy_owner_user_id, purpose)
      VALUES (legacy.tenant_id, legacy.user_id, 'CONTRIBUTION_DEIDENTIFY')
      RETURNING route_policy_id INTO new_policy_id;
    INSERT INTO control.reasoning_route_candidates(
      tenant_id, route_policy_id, route_policy_version, profile_id, profile_version, priority, fallback_class, enabled
    ) VALUES (legacy.tenant_id, new_policy_id, 1, new_profile_id, 1, 0, 'NONE', true);
    UPDATE control.reasoning_route_policies
       SET lifecycle_state = 'SHADOW'
     WHERE route_policy_id = new_policy_id AND policy_version = 1;

    route_fingerprint := control.reasoning_route_profile_receipt_fingerprint(legacy.profile_id, new_profile_id, 1);
    INSERT INTO control.reasoning_route_profile_receipts(
      tenant_id, legacy_profile_id, profile_id, route_policy_id, execution_fingerprint
    ) VALUES (legacy.tenant_id, legacy.profile_id, new_profile_id, new_policy_id, route_fingerprint);
  END LOOP;

  FOR domain IN
    SELECT d.tenant_id, d.reasoning_domain_id, d.user_reasoning_profile_id, r.route_policy_id,
           r.policy_version, r.execution_fingerprint
      FROM control.private_reasoning_domains d
      JOIN control.reasoning_route_profile_receipts r
        ON (r.tenant_id, r.legacy_profile_id) = (d.tenant_id, d.user_reasoning_profile_id)
     WHERE d.status = 'ACTIVE' AND d.user_reasoning_profile_id IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM control.reasoning_route_domain_receipts receipt
                        WHERE (receipt.tenant_id, receipt.reasoning_domain_id) = (d.tenant_id, d.reasoning_domain_id))
  LOOP
    INSERT INTO control.reasoning_route_bindings(
      tenant_id, reasoning_domain_id, purpose, route_policy_id, route_policy_version
    ) VALUES (
      domain.tenant_id, domain.reasoning_domain_id, 'CONTRIBUTION_DEIDENTIFY',
      domain.route_policy_id, domain.policy_version
    ) RETURNING binding_id INTO new_binding_id;
    INSERT INTO control.reasoning_route_domain_receipts(
      tenant_id, reasoning_domain_id, legacy_profile_id, disposition, binding_id,
      binding_version, execution_fingerprint
    ) VALUES (
      domain.tenant_id, domain.reasoning_domain_id, domain.user_reasoning_profile_id,
      'BOOTSTRAPPED', new_binding_id, 1, domain.execution_fingerprint
    );
  END LOOP;

  PERFORM control.assert_reasoning_route_shadow_receipts();
  SELECT count(*) INTO profile_receipts FROM control.reasoning_route_profile_receipts;
  SELECT count(*) INTO domain_receipts FROM control.reasoning_route_domain_receipts;
  RETURN NEXT;
END;
$$;

CREATE FUNCTION control.resolve_contribution_deidentify_shadow(p_reasoning_domain_id uuid)
RETURNS TABLE(
  compatibility_state text,
  legacy_output_fingerprint text,
  shadow_output_fingerprint text,
  binding_id uuid,
  binding_version bigint
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE domain record;
DECLARE legacy record;
DECLARE receipt record;
DECLARE profile record;
BEGIN
  SELECT * INTO domain FROM control.private_reasoning_domains WHERE reasoning_domain_id = p_reasoning_domain_id;
  IF NOT FOUND THEN
    compatibility_state := 'ABSENT_DOMAIN';
    RETURN NEXT;
    RETURN;
  END IF;
  IF domain.user_reasoning_profile_id IS NULL THEN
    legacy_output_fingerprint := control.reasoning_route_shadow_fingerprint(NULL, NULL, NULL, NULL, NULL, NULL, NULL, domain.tenant_id, 'NO_LEGACY_PROFILE');
    shadow_output_fingerprint := legacy_output_fingerprint;
    SELECT * INTO receipt FROM control.reasoning_route_domain_receipts r
     WHERE (r.tenant_id, r.reasoning_domain_id, r.disposition) = (domain.tenant_id, domain.reasoning_domain_id, 'NO_LEGACY_PROFILE');
    compatibility_state := CASE
      WHEN NOT FOUND THEN 'UNMAPPED_LEGACY_PROFILE'
      WHEN receipt.execution_fingerprint IS DISTINCT FROM legacy_output_fingerprint THEN 'CORRUPT_RECEIPT'
      ELSE 'EQUIVALENT'
    END;
    RETURN NEXT;
    RETURN;
  END IF;
  SELECT * INTO legacy FROM control.user_reasoning_profiles
   WHERE profile_id = domain.user_reasoning_profile_id AND tenant_id = domain.tenant_id;
  SELECT * INTO receipt FROM control.reasoning_route_domain_receipts
   WHERE (tenant_id, reasoning_domain_id) = (domain.tenant_id, domain.reasoning_domain_id)
     AND disposition = 'BOOTSTRAPPED';
  IF NOT FOUND THEN
    compatibility_state := 'UNMAPPED_LEGACY_PROFILE';
    RETURN NEXT;
    RETURN;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM control.reasoning_route_bindings binding
    JOIN control.reasoning_route_policies policy ON (policy.tenant_id, policy.route_policy_id, policy.policy_version)=(binding.tenant_id,binding.route_policy_id,binding.route_policy_version)
    JOIN control.reasoning_route_profile_receipts profile_receipt ON (profile_receipt.tenant_id,profile_receipt.legacy_profile_id,profile_receipt.route_policy_id,profile_receipt.policy_version)=(receipt.tenant_id,receipt.legacy_profile_id,binding.route_policy_id,binding.route_policy_version)
    JOIN control.reasoning_route_candidates candidate ON (candidate.tenant_id,candidate.route_policy_id,candidate.route_policy_version,candidate.profile_id,candidate.profile_version)=(profile_receipt.tenant_id,profile_receipt.route_policy_id,profile_receipt.policy_version,profile_receipt.profile_id,profile_receipt.profile_version)
    WHERE (binding.tenant_id,binding.binding_id,binding.binding_version)=(receipt.tenant_id,receipt.binding_id,receipt.binding_version)
      AND binding.reasoning_domain_id=domain.reasoning_domain_id
      AND binding.purpose='CONTRIBUTION_DEIDENTIFY'
      AND binding.effective_to IS NULL AND policy.purpose='CONTRIBUTION_DEIDENTIFY'
      AND policy.strategy='PINNED' AND policy.lifecycle_state='SHADOW' AND policy.effective_to IS NULL
      AND candidate.enabled AND candidate.priority=0 AND candidate.fallback_class='NONE'
      AND 1=(SELECT count(*) FROM control.reasoning_route_candidates one_candidate WHERE one_candidate.tenant_id=candidate.tenant_id AND one_candidate.route_policy_id=candidate.route_policy_id AND one_candidate.route_policy_version=candidate.route_policy_version)
  ) THEN
    compatibility_state := 'CORRUPT_ROUTE';
    RETURN NEXT;
    RETURN;
  END IF;
  SELECT p.*, account.processor_id, model.provider_model_id
    INTO profile
    FROM control.reasoning_route_profile_receipts pr
    JOIN control.reasoning_profiles p ON (p.tenant_id, p.profile_id, p.profile_version) = (pr.tenant_id, pr.profile_id, pr.profile_version)
    JOIN control.provider_accounts account ON account.provider_account_id = p.provider_account_id
    JOIN control.processor_models model ON model.processor_model_id = p.processor_model_id
   WHERE (pr.tenant_id, pr.legacy_profile_id) = (domain.tenant_id, domain.user_reasoning_profile_id);
  IF NOT FOUND THEN
    compatibility_state := 'CORRUPT_RECEIPT';
    RETURN NEXT;
    RETURN;
  END IF;
  legacy_output_fingerprint := receipt.execution_fingerprint;
  shadow_output_fingerprint := control.reasoning_route_profile_receipt_fingerprint(
    legacy.profile_id, profile.profile_id, profile.profile_version
  );
  IF receipt.execution_fingerprint IS DISTINCT FROM shadow_output_fingerprint
     OR NOT EXISTS (
       SELECT 1 FROM control.reasoning_route_profile_receipts profile_receipt
        WHERE (profile_receipt.tenant_id, profile_receipt.legacy_profile_id,
               profile_receipt.profile_id, profile_receipt.profile_version)
            = (receipt.tenant_id, receipt.legacy_profile_id,
               profile.profile_id, profile.profile_version)
          AND profile_receipt.execution_fingerprint = receipt.execution_fingerprint
          AND profile_receipt.execution_fingerprint = shadow_output_fingerprint
     ) THEN
    compatibility_state := 'CORRUPT_RECEIPT';
    RETURN NEXT;
    RETURN;
  END IF;
  binding_id := receipt.binding_id;
  binding_version := receipt.binding_version;
  compatibility_state := 'EQUIVALENT';
  RETURN NEXT;
END;
$$;

ALTER FUNCTION control.reasoning_route_shadow_fingerprint(text, text, uuid, text[], text, jsonb, uuid, uuid, text) OWNER TO role_migration_owner;
ALTER FUNCTION control.reasoning_route_profile_receipt_fingerprint(uuid, uuid, bigint) OWNER TO role_migration_owner;
ALTER FUNCTION control.assert_reasoning_route_shadow_receipts() OWNER TO role_migration_owner;
ALTER FUNCTION control.reasoning_route_receipt_reject_mutation() OWNER TO role_migration_owner;
ALTER FUNCTION control.bootstrap_contribution_deidentify_shadow() OWNER TO role_migration_owner;
ALTER FUNCTION control.resolve_contribution_deidentify_shadow(uuid) OWNER TO role_migration_owner;
ALTER TABLE control.reasoning_route_profile_receipts OWNER TO role_migration_owner;
ALTER TABLE control.reasoning_route_domain_receipts OWNER TO role_migration_owner;
REVOKE ALL ON TABLE control.reasoning_route_profile_receipts, control.reasoning_route_domain_receipts FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance;
REVOKE ALL ON FUNCTION control.reasoning_route_shadow_fingerprint(text, text, uuid, text[], text, jsonb, uuid, uuid, text), control.reasoning_route_profile_receipt_fingerprint(uuid, uuid, bigint), control.assert_reasoning_route_shadow_receipts(), control.bootstrap_contribution_deidentify_shadow(), control.resolve_contribution_deidentify_shadow(uuid) FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance;
