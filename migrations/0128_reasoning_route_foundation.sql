-- §11.2 R1 schema expand. This establishes only the USER_REASONING control-plane
-- foundation; it neither migrates control.user_reasoning_profiles nor activates a router.
-- Credential columns are OpenBao locator references, never key material (§7 / §11.1).

CREATE TABLE control.processor_models (
  processor_model_id uuid PRIMARY KEY DEFAULT uuidv7(),
  processor_id text NOT NULL,
  provider_model_id text NOT NULL,
  model_revision text,
  capabilities text[] NOT NULL CHECK (
    cardinality(capabilities) > 0
    AND capabilities <@ ARRAY['TEXT', 'VISION', 'STRUCTURED_OUTPUT', 'TOKEN_USAGE']::text[]
  ),
  status text NOT NULL CHECK (status IN ('ACTIVE', 'RETIRED')),
  catalog_observed_at timestamptz NOT NULL,
  provider_request_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE NULLS NOT DISTINCT (processor_id, provider_model_id, model_revision)
);

CREATE TABLE control.provider_accounts (
  provider_account_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  owner_user_id uuid NOT NULL REFERENCES control.users(user_id),
  trust_domain text NOT NULL DEFAULT 'USER_REASONING' CHECK (trust_domain = 'USER_REASONING'),
  billing_responsibility text NOT NULL DEFAULT 'USER' CHECK (billing_responsibility = 'USER'),
  processor_id text NOT NULL,
  external_account_ref_hash bytea NOT NULL CHECK (octet_length(external_account_ref_hash) = 32),
  home_region text,
  status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'DISABLED')),
  account_version bigint NOT NULL DEFAULT 1 CHECK (account_version > 0),
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, provider_account_id),
  UNIQUE (tenant_id, owner_user_id, processor_id, external_account_ref_hash)
);

CREATE TABLE control.provider_endpoints (
  endpoint_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  provider_account_id uuid NOT NULL REFERENCES control.provider_accounts(provider_account_id),
  region text NOT NULL,
  service_tier text NOT NULL,
  endpoint_ref text NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, endpoint_id),
  UNIQUE (tenant_id, provider_account_id, endpoint_ref)
);

CREATE TABLE control.provider_billing_accounts (
  billing_account_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  owner_user_id uuid NOT NULL REFERENCES control.users(user_id),
  provider_account_id uuid NOT NULL REFERENCES control.provider_accounts(provider_account_id),
  owner_kind text NOT NULL DEFAULT 'USER' CHECK (owner_kind = 'USER'),
  payer_kind text NOT NULL DEFAULT 'USER' CHECK (payer_kind = 'USER'),
  billing_responsibility text NOT NULL DEFAULT 'USER' CHECK (billing_responsibility = 'USER'),
  account_ref text NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, billing_account_id),
  UNIQUE (tenant_id, provider_account_id, account_ref)
);

CREATE TABLE control.provider_billing_instruments (
  billing_instrument_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  billing_account_id uuid NOT NULL REFERENCES control.provider_billing_accounts(billing_account_id),
  owner_kind text NOT NULL DEFAULT 'USER' CHECK (owner_kind = 'USER'),
  payer_kind text NOT NULL DEFAULT 'USER' CHECK (payer_kind = 'USER'),
  owner_user_id uuid NOT NULL REFERENCES control.users(user_id),
  payer_user_id uuid NOT NULL REFERENCES control.users(user_id),
  instrument_kind text NOT NULL CHECK (instrument_kind IN ('PAYG', 'PREPAID_CREDIT', 'RESOURCE_PACKAGE', 'TOKEN_PLAN', 'COMMITTED_SPEND', 'PROVISIONED_THROUGHPUT', 'API_SUBSCRIPTION_ALLOWANCE', 'INVOICED_CONTRACT', 'CUSTOMER_BILLED')),
  invocation_eligibility text NOT NULL CHECK (invocation_eligibility IN ('API_CALLABLE', 'TOOL_SPECIFIC', 'INTERACTIVE_ONLY')),
  currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  coverage_processor_id text NOT NULL CHECK (length(coverage_processor_id) > 0),
  coverage_provider_model_id text NOT NULL CHECK (length(coverage_provider_model_id) > 0),
  coverage_model_revision text,
  coverage_region text NOT NULL CHECK (length(coverage_region) > 0),
  coverage_service_tier text NOT NULL CHECK (length(coverage_service_tier) > 0),
  valid_from timestamptz NOT NULL,
  valid_to timestamptz,
  overage_policy text NOT NULL CHECK (overage_policy IN ('DENY', 'PAYG', 'FALLBACK_SAME_PAYER')),
  contract_version bigint NOT NULL DEFAULT 1 CHECK (contract_version > 0),
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (valid_to IS NULL OR valid_to > valid_from),
  UNIQUE (tenant_id, billing_instrument_id)
);

CREATE TABLE control.reasoning_profiles (
  profile_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  owner_kind text NOT NULL DEFAULT 'USER' CHECK (owner_kind = 'USER'),
  owner_user_id uuid NOT NULL REFERENCES control.users(user_id),
  trust_domain text NOT NULL DEFAULT 'USER_REASONING' CHECK (trust_domain = 'USER_REASONING'),
  billing_responsibility text NOT NULL DEFAULT 'USER' CHECK (billing_responsibility = 'USER'),
  provider_account_id uuid NOT NULL REFERENCES control.provider_accounts(provider_account_id),
  endpoint_id uuid NOT NULL REFERENCES control.provider_endpoints(endpoint_id),
  processor_model_id uuid NOT NULL REFERENCES control.processor_models(processor_model_id),
  credential_ref uuid NOT NULL REFERENCES control.credentials(credential_id),
  billing_account_id uuid REFERENCES control.provider_billing_accounts(billing_account_id),
  default_billing_instrument_id uuid REFERENCES control.provider_billing_instruments(billing_instrument_id),
  capabilities text[] NOT NULL CHECK (
    cardinality(capabilities) > 0
    AND capabilities <@ ARRAY['TEXT', 'VISION', 'STRUCTURED_OUTPUT', 'TOKEN_USAGE']::text[]
  ),
  processing_region text,
  custom_endpoint_policy jsonb,
  profile_version bigint NOT NULL DEFAULT 1 CHECK (profile_version > 0),
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, profile_id, profile_version)
);

CREATE TABLE control.reasoning_route_policies (
  route_policy_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  policy_owner_kind text NOT NULL DEFAULT 'USER' CHECK (policy_owner_kind = 'USER'),
  policy_owner_user_id uuid NOT NULL REFERENCES control.users(user_id),
  purpose text NOT NULL CHECK (purpose IN (
    'PRIVATE_DISTILL_TEXT', 'PRIVATE_DISTILL_VISION', 'PRIVATE_CONSOLIDATE',
    'GROUNDING_RECHECK', 'CONTRIBUTION_DEIDENTIFY'
  )),
  trust_domain text NOT NULL DEFAULT 'USER_REASONING' CHECK (trust_domain = 'USER_REASONING'),
  billing_responsibility text NOT NULL DEFAULT 'USER' CHECK (billing_responsibility = 'USER'),
  strategy text NOT NULL DEFAULT 'PINNED' CHECK (strategy = 'PINNED'),
  policy_version bigint NOT NULL DEFAULT 1 CHECK (policy_version > 0),
  lifecycle_state text NOT NULL DEFAULT 'DRAFT' CHECK (lifecycle_state IN ('DRAFT', 'SHADOW', 'SERVING')),
  effective_from timestamptz NOT NULL DEFAULT now(),
  effective_to timestamptz,
  CHECK (effective_to IS NULL OR effective_to > effective_from),
  UNIQUE (tenant_id, route_policy_id, policy_version)
);

CREATE TABLE control.reasoning_route_candidates (
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  route_policy_id uuid NOT NULL REFERENCES control.reasoning_route_policies(route_policy_id),
  route_policy_version bigint NOT NULL CHECK (route_policy_version > 0),
  profile_id uuid NOT NULL REFERENCES control.reasoning_profiles(profile_id),
  priority integer NOT NULL CHECK (priority >= 0),
  fallback_class text NOT NULL DEFAULT 'NONE' CHECK (fallback_class = 'NONE'),
  enabled boolean NOT NULL DEFAULT true,
  PRIMARY KEY (route_policy_id, route_policy_version, profile_id),
  UNIQUE (tenant_id, route_policy_id, route_policy_version, profile_id),
  UNIQUE (route_policy_id, route_policy_version, priority)
);

CREATE TABLE control.reasoning_route_bindings (
  binding_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  reasoning_domain_id uuid NOT NULL REFERENCES control.private_reasoning_domains(reasoning_domain_id),
  purpose text NOT NULL CHECK (purpose IN (
    'PRIVATE_DISTILL_TEXT', 'PRIVATE_DISTILL_VISION', 'PRIVATE_CONSOLIDATE',
    'GROUNDING_RECHECK', 'CONTRIBUTION_DEIDENTIFY'
  )),
  route_policy_id uuid NOT NULL REFERENCES control.reasoning_route_policies(route_policy_id),
  route_policy_version bigint NOT NULL CHECK (route_policy_version > 0),
  effective_from timestamptz NOT NULL DEFAULT now(),
  effective_to timestamptz,
  CHECK (effective_to IS NULL OR effective_to > effective_from),
  UNIQUE (tenant_id, binding_id),
  UNIQUE (reasoning_domain_id, purpose, effective_from)
);

-- Versioned rows are immutable semantic snapshots. Health/availability is intentionally
-- separate so a status observation cannot rewrite a historical route decision (§11.2).
ALTER TABLE control.reasoning_route_candidates DROP CONSTRAINT reasoning_route_candidates_route_policy_id_fkey;
ALTER TABLE control.reasoning_route_candidates DROP CONSTRAINT reasoning_route_candidates_profile_id_fkey;
ALTER TABLE control.reasoning_route_bindings DROP CONSTRAINT reasoning_route_bindings_route_policy_id_fkey;
ALTER TABLE control.reasoning_profiles DROP CONSTRAINT reasoning_profiles_pkey;
ALTER TABLE control.reasoning_profiles ADD PRIMARY KEY (profile_id, profile_version);
ALTER TABLE control.reasoning_route_policies DROP CONSTRAINT reasoning_route_policies_pkey;
ALTER TABLE control.reasoning_route_policies ADD PRIMARY KEY (route_policy_id, policy_version);
ALTER TABLE control.reasoning_route_candidates ADD COLUMN profile_version bigint NOT NULL;
ALTER TABLE control.reasoning_route_candidates
  ADD CONSTRAINT reasoning_route_candidates_policy_version_fkey
    FOREIGN KEY (tenant_id, route_policy_id, route_policy_version)
    REFERENCES control.reasoning_route_policies(tenant_id, route_policy_id, policy_version),
  ADD CONSTRAINT reasoning_route_candidates_profile_version_fkey
    FOREIGN KEY (tenant_id, profile_id, profile_version)
    REFERENCES control.reasoning_profiles(tenant_id, profile_id, profile_version);
ALTER TABLE control.reasoning_route_candidates DROP CONSTRAINT reasoning_route_candidates_pkey;
ALTER TABLE control.reasoning_route_candidates
  ADD PRIMARY KEY (route_policy_id, route_policy_version, profile_id, profile_version);
ALTER TABLE control.reasoning_route_bindings ADD COLUMN binding_version bigint NOT NULL DEFAULT 1 CHECK (binding_version > 0);
ALTER TABLE control.reasoning_route_bindings
  DROP CONSTRAINT reasoning_route_bindings_tenant_id_binding_id_key;
ALTER TABLE control.reasoning_route_bindings
  ADD CONSTRAINT reasoning_route_bindings_tenant_version_unique
    UNIQUE (tenant_id, binding_id, binding_version);
ALTER TABLE control.reasoning_route_bindings
  ADD CONSTRAINT reasoning_route_bindings_policy_version_fkey
    FOREIGN KEY (tenant_id, route_policy_id, route_policy_version)
    REFERENCES control.reasoning_route_policies(tenant_id, route_policy_id, policy_version);
ALTER TABLE control.reasoning_route_bindings DROP CONSTRAINT reasoning_route_bindings_pkey;
ALTER TABLE control.reasoning_route_bindings ADD PRIMARY KEY (binding_id, binding_version);
DO $$
DECLARE legacy_constraint text;
BEGIN
  SELECT conname INTO legacy_constraint FROM pg_constraint
   WHERE conrelid = 'control.reasoning_route_bindings'::regclass
     AND contype = 'u'
     AND conname LIKE 'reasoning_route_bindings_reasoning_domain_id_purpose%';
  EXECUTE format('ALTER TABLE control.reasoning_route_bindings DROP CONSTRAINT %I', legacy_constraint);
END;
$$;
CREATE UNIQUE INDEX reasoning_route_bindings_one_current
  ON control.reasoning_route_bindings(tenant_id, reasoning_domain_id, purpose)
  WHERE effective_to IS NULL;

CREATE TABLE control.reasoning_credential_bindings (
  credential_ref uuid NOT NULL REFERENCES control.credentials(credential_id),
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  owner_user_id uuid NOT NULL REFERENCES control.users(user_id),
  provider_account_id uuid NOT NULL REFERENCES control.provider_accounts(provider_account_id),
  processor_id text NOT NULL,
  invocation_eligibility text NOT NULL DEFAULT 'API_CALLABLE' CHECK (invocation_eligibility = 'API_CALLABLE'),
  credential_purpose text NOT NULL DEFAULT 'USER_REASONING' CHECK (credential_purpose = 'USER_REASONING'),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (credential_ref, provider_account_id),
  UNIQUE (tenant_id, credential_ref, provider_account_id)
);

COMMENT ON COLUMN control.reasoning_profiles.credential_ref IS
  '§7 / §11.1 CredentialRef locator only; raw secret bytes remain in OpenBao.';
COMMENT ON COLUMN control.provider_billing_instruments.coverage_processor_id IS
  '§19.4 typed exact processor/model/revision/region/service-tier coverage; balance never implies another execution identity is covered.';

CREATE FUNCTION control.provider_accounts_check_owner()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE'
     AND NOT ((NEW.enabled AND NOT OLD.enabled)
              OR (NEW.status = 'ACTIVE' AND OLD.status <> 'ACTIVE')) THEN
    RETURN NEW;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM control.memberships
                 WHERE tenant_id = NEW.tenant_id AND user_id = NEW.owner_user_id AND state = 'ACTIVE') THEN
    RAISE EXCEPTION 'reasoning provider account owner must belong to its tenant' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION control.provider_endpoints_check_account()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NOT (NEW.enabled AND NOT OLD.enabled) THEN
    RETURN NEW;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM control.provider_accounts
                 WHERE provider_account_id = NEW.provider_account_id AND tenant_id = NEW.tenant_id
                   AND status = 'ACTIVE' AND enabled) THEN
    RAISE EXCEPTION 'reasoning endpoint must belong to its tenant provider account' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION control.provider_billing_accounts_check_account()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NOT (NEW.enabled AND NOT OLD.enabled) THEN
    RETURN NEW;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM control.provider_accounts
                 WHERE provider_account_id = NEW.provider_account_id AND tenant_id = NEW.tenant_id
                   AND owner_user_id = NEW.owner_user_id AND billing_responsibility = NEW.billing_responsibility
                   AND status = 'ACTIVE' AND enabled
                   AND NEW.owner_kind = 'USER' AND NEW.payer_kind = 'USER') THEN
    RAISE EXCEPTION 'reasoning billing account must preserve provider account owner, tenant, and payer' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION control.provider_billing_instruments_check_account()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NOT (NEW.enabled AND NOT OLD.enabled) THEN
    RETURN NEW;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM control.provider_billing_accounts billing
                 JOIN control.provider_accounts account ON account.provider_account_id = billing.provider_account_id
                 WHERE billing.billing_account_id = NEW.billing_account_id AND billing.tenant_id = NEW.tenant_id
                   AND NEW.owner_user_id = billing.owner_user_id AND NEW.payer_user_id = billing.owner_user_id
                   AND NEW.owner_kind = 'USER' AND NEW.payer_kind = 'USER'
                   AND billing.enabled AND account.enabled AND account.status = 'ACTIVE'
                   AND account.billing_responsibility = 'USER') THEN
    RAISE EXCEPTION 'reasoning billing instrument must belong to its tenant billing account' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION control.reasoning_credential_bindings_check_authority()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM control.credentials base
      JOIN control.provider_accounts account
        ON account.provider_account_id = NEW.provider_account_id
     WHERE base.credential_id = NEW.credential_ref
       AND base.tenant_id = NEW.tenant_id
       AND base.purpose = NEW.credential_purpose
       AND account.tenant_id = NEW.tenant_id
       AND account.owner_user_id = NEW.owner_user_id
       AND account.processor_id = NEW.processor_id
  ) THEN
    RAISE EXCEPTION 'reasoning credential binding must preserve base credential tenant/purpose and account owner/processor'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION control.reasoning_profiles_check_ownership()
RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE credential_tenant uuid;
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
      RAISE EXCEPTION 'reasoning profile version inserts require READ COMMITTED isolation'
        USING ERRCODE = '40001';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('reasoning_profile:' || NEW.profile_id::text, 0));
    IF EXISTS (
      SELECT 1 FROM control.reasoning_profiles prior
       WHERE prior.profile_id = NEW.profile_id
         AND ROW(prior.tenant_id, prior.owner_kind, prior.owner_user_id,
                 prior.trust_domain, prior.billing_responsibility)
             IS DISTINCT FROM
             ROW(NEW.tenant_id, NEW.owner_kind, NEW.owner_user_id,
                 NEW.trust_domain, NEW.billing_responsibility)
    ) THEN
      RAISE EXCEPTION 'reasoning profile versions must preserve tenant, owner, trust domain, and payer'
        USING ERRCODE = '23514';
    END IF;
  END IF;
  IF TG_OP = 'UPDATE' AND NOT (NEW.enabled AND NOT OLD.enabled) THEN
    RETURN NEW;
  END IF;
  SELECT tenant_id INTO credential_tenant FROM control.credentials
    WHERE credential_id = NEW.credential_ref;
  IF credential_tenant IS DISTINCT FROM NEW.tenant_id THEN
    RAISE EXCEPTION 'reasoning profile credential_ref must belong to its tenant' USING ERRCODE = '23514';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM control.reasoning_credential_bindings credential
    JOIN control.provider_accounts account ON account.provider_account_id = NEW.provider_account_id
    JOIN control.credentials base ON base.credential_id = credential.credential_ref
    WHERE credential.credential_ref = NEW.credential_ref AND credential.provider_account_id = NEW.provider_account_id
      AND credential.tenant_id = NEW.tenant_id
      AND credential.owner_user_id = NEW.owner_user_id
      AND credential.processor_id = account.processor_id
      AND credential.invocation_eligibility = 'API_CALLABLE'
      AND credential.credential_purpose = 'USER_REASONING'
      AND base.tenant_id = NEW.tenant_id AND base.purpose = 'USER_REASONING'
  ) THEN
    RAISE EXCEPTION 'reasoning profile credential binding must preserve tenant, owner, processor, purpose, and API eligibility' USING ERRCODE = '23514';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM control.provider_accounts
                 WHERE provider_account_id = NEW.provider_account_id AND tenant_id = NEW.tenant_id
                   AND owner_user_id = NEW.owner_user_id AND trust_domain = NEW.trust_domain
                   AND billing_responsibility = NEW.billing_responsibility) THEN
    RAISE EXCEPTION 'reasoning profile must preserve provider account tenant, owner, trust, and payer' USING ERRCODE = '23514';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM control.provider_endpoints
                 WHERE endpoint_id = NEW.endpoint_id AND tenant_id = NEW.tenant_id
                   AND provider_account_id = NEW.provider_account_id AND enabled) THEN
    RAISE EXCEPTION 'reasoning profile endpoint must belong to its provider account' USING ERRCODE = '23514';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM control.provider_accounts account
    JOIN control.processor_models model ON model.processor_model_id = NEW.processor_model_id
    WHERE account.provider_account_id = NEW.provider_account_id
      AND account.processor_id = model.processor_id AND account.status = 'ACTIVE' AND account.enabled
      AND model.status = 'ACTIVE' AND NEW.capabilities <@ model.capabilities
  ) THEN
    RAISE EXCEPTION 'reasoning profile model must be active and belong to its provider account processor' USING ERRCODE = '23514';
  END IF;
  IF NEW.billing_account_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM control.provider_billing_accounts
      WHERE billing_account_id = NEW.billing_account_id AND tenant_id = NEW.tenant_id
        AND provider_account_id = NEW.provider_account_id AND owner_user_id = NEW.owner_user_id
        AND billing_responsibility = NEW.billing_responsibility AND enabled
  ) THEN
    RAISE EXCEPTION 'reasoning profile billing account must preserve account owner, tenant, and payer' USING ERRCODE = '23514';
  END IF;
  IF NEW.default_billing_instrument_id IS NOT NULL AND (
    NEW.billing_account_id IS NULL OR NOT EXISTS (
      SELECT 1 FROM control.provider_billing_instruments instrument
      JOIN control.provider_endpoints endpoint ON endpoint.endpoint_id = NEW.endpoint_id
      JOIN control.provider_accounts account ON account.provider_account_id = NEW.provider_account_id
      JOIN control.processor_models model ON model.processor_model_id = NEW.processor_model_id
       WHERE instrument.billing_instrument_id = NEW.default_billing_instrument_id
         AND instrument.billing_account_id = NEW.billing_account_id AND instrument.tenant_id = NEW.tenant_id
         AND instrument.enabled AND instrument.invocation_eligibility = 'API_CALLABLE'
         AND instrument.valid_from <= clock_timestamp()
         AND (instrument.valid_to IS NULL OR instrument.valid_to > clock_timestamp())
         AND instrument.owner_user_id = NEW.owner_user_id AND instrument.payer_user_id = NEW.owner_user_id
         AND endpoint.enabled
         AND account.processor_id = model.processor_id AND account.status = 'ACTIVE' AND account.enabled
         AND model.status = 'ACTIVE'
         AND instrument.coverage_processor_id = model.processor_id
         AND instrument.coverage_provider_model_id = model.provider_model_id
         AND instrument.coverage_region = endpoint.region
         AND instrument.coverage_service_tier = endpoint.service_tier
         AND instrument.coverage_model_revision IS NOT DISTINCT FROM model.model_revision
    )
  ) THEN
    RAISE EXCEPTION 'reasoning profile billing instrument must belong to its selected billing account' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION control.reasoning_route_policies_check_owner()
RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
      RAISE EXCEPTION 'reasoning route policy version inserts require READ COMMITTED isolation'
        USING ERRCODE = '40001';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('reasoning_policy:' || NEW.route_policy_id::text, 0));
    IF NEW.lifecycle_state <> 'DRAFT' THEN
      RAISE EXCEPTION 'reasoning route policy versions must begin in DRAFT'
        USING ERRCODE = '23514';
    END IF;
    IF EXISTS (
      SELECT 1 FROM control.reasoning_route_policies prior
       WHERE prior.route_policy_id = NEW.route_policy_id
         AND ROW(prior.tenant_id, prior.policy_owner_kind, prior.policy_owner_user_id,
                 prior.purpose, prior.trust_domain, prior.billing_responsibility)
             IS DISTINCT FROM
             ROW(NEW.tenant_id, NEW.policy_owner_kind, NEW.policy_owner_user_id,
                 NEW.purpose, NEW.trust_domain, NEW.billing_responsibility)
    ) THEN
      RAISE EXCEPTION 'reasoning route policy versions must preserve tenant, owner, purpose, trust domain, and payer'
        USING ERRCODE = '23514';
    END IF;
  END IF;
  IF TG_OP = 'UPDATE' AND NEW.lifecycle_state = OLD.lifecycle_state THEN
    RETURN NEW;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM control.memberships
                 WHERE tenant_id = NEW.tenant_id AND user_id = NEW.policy_owner_user_id AND state = 'ACTIVE') THEN
    RAISE EXCEPTION 'reasoning route policy owner must belong to its tenant' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION control.reasoning_route_candidates_check_profile()
RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE
  parent_owner_user_id uuid;
  parent_trust_domain text;
  parent_billing_responsibility text;
  parent_lifecycle_state text;
  parent_effective_to timestamptz;
BEGIN
  SELECT policy_owner_user_id, trust_domain, billing_responsibility, lifecycle_state, effective_to
    INTO parent_owner_user_id, parent_trust_domain, parent_billing_responsibility,
         parent_lifecycle_state, parent_effective_to
    FROM control.reasoning_route_policies
   WHERE tenant_id = NEW.tenant_id
     AND route_policy_id = NEW.route_policy_id
     AND policy_version = NEW.route_policy_version
   FOR UPDATE;
  IF NOT FOUND OR parent_lifecycle_state <> 'DRAFT' OR parent_effective_to IS NOT NULL THEN
    RAISE EXCEPTION 'route candidates may only be inserted while the exact policy version is open DRAFT'
      USING ERRCODE = '23514';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM control.reasoning_profiles profile
    WHERE profile.profile_id = NEW.profile_id
      AND profile.profile_version = NEW.profile_version
      AND profile.tenant_id = NEW.tenant_id
      AND parent_owner_user_id = profile.owner_user_id
      AND parent_trust_domain = profile.trust_domain
      AND parent_billing_responsibility = profile.billing_responsibility
  ) THEN
    RAISE EXCEPTION 'route candidate must preserve policy tenant, owner, trust domain, payer, and version' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION control.reasoning_route_bindings_check_domain_and_policy()
RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE
  parent_owner_user_id uuid;
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
      RAISE EXCEPTION 'reasoning route binding version inserts require READ COMMITTED isolation'
        USING ERRCODE = '40001';
    END IF;
    SELECT policy_owner_user_id
      INTO parent_owner_user_id
      FROM control.reasoning_route_policies
     WHERE tenant_id = NEW.tenant_id
       AND route_policy_id = NEW.route_policy_id
       AND policy_version = NEW.route_policy_version
       AND purpose = NEW.purpose
       AND lifecycle_state IN ('SHADOW', 'SERVING')
       AND effective_to IS NULL
     FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'reasoning route binding must reference an exact open SHADOW or SERVING policy'
        USING ERRCODE = '23514';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('reasoning_binding:' || NEW.binding_id::text, 0));
    IF EXISTS (
      SELECT 1 FROM control.reasoning_route_bindings prior
       WHERE prior.binding_id = NEW.binding_id
         AND ROW(prior.tenant_id, prior.reasoning_domain_id, prior.purpose)
             IS DISTINCT FROM
             ROW(NEW.tenant_id, NEW.reasoning_domain_id, NEW.purpose)
    ) THEN
      RAISE EXCEPTION 'reasoning route binding versions must preserve tenant, domain, and purpose'
        USING ERRCODE = '23514';
    END IF;
    IF NOT EXISTS (
      SELECT 1 FROM control.private_reasoning_domains domain
      WHERE domain.reasoning_domain_id = NEW.reasoning_domain_id
        AND domain.tenant_id = NEW.tenant_id
        AND domain.owner_user_id IS NOT NULL
        AND parent_owner_user_id = domain.owner_user_id
        AND EXISTS (SELECT 1 FROM control.memberships membership
                    WHERE membership.tenant_id = NEW.tenant_id
                      AND membership.user_id = domain.owner_user_id
                      AND membership.state = 'ACTIVE')
    ) THEN
      RAISE EXCEPTION 'reasoning route binding must preserve domain tenant, owner, policy purpose, and version' USING ERRCODE = '23514';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION control.reasoning_version_row_reject_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'versioned reasoning semantic rows are append-only; insert a successor version' USING ERRCODE = '55000';
END;
$$;

CREATE FUNCTION control.reasoning_binding_close_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.binding_id = OLD.binding_id AND NEW.binding_version = OLD.binding_version
     AND NEW.tenant_id = OLD.tenant_id AND NEW.reasoning_domain_id = OLD.reasoning_domain_id
     AND NEW.purpose = OLD.purpose AND NEW.route_policy_id = OLD.route_policy_id
     AND NEW.route_policy_version = OLD.route_policy_version AND NEW.effective_from = OLD.effective_from
     AND OLD.effective_to IS NULL AND NEW.effective_to IS NOT NULL THEN RETURN NEW; END IF;
  RAISE EXCEPTION 'binding rows may only close their current effective interval' USING ERRCODE = '55000';
END;
$$;

CREATE FUNCTION control.reasoning_parent_identity_reject_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE allowed text[];
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'provider identity rows are not deletable' USING ERRCODE = '55000';
  END IF;
  allowed := CASE TG_TABLE_NAME
    WHEN 'processor_models' THEN ARRAY['status']
    WHEN 'provider_accounts' THEN ARRAY['status', 'enabled', 'updated_at']
    WHEN 'provider_endpoints' THEN ARRAY['enabled', 'updated_at']
    WHEN 'provider_billing_accounts' THEN ARRAY['enabled', 'updated_at']
    WHEN 'provider_billing_instruments' THEN ARRAY['enabled', 'updated_at']
  END;
  IF (to_jsonb(NEW) - allowed) IS DISTINCT FROM (to_jsonb(OLD) - allowed) THEN
    RAISE EXCEPTION 'provider identity columns are immutable; only typed administrative state may change'
      USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION control.reasoning_profile_state_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR (to_jsonb(NEW) - ARRAY['enabled', 'updated_at'])
      IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['enabled', 'updated_at']) THEN
    RAISE EXCEPTION 'profile semantics are append-only; only enabled administrative state may change'
      USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION control.reasoning_policy_state_only()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  candidate_count bigint;
  pinned_candidate_count bigint;
BEGIN
  IF TG_OP = 'DELETE' OR (to_jsonb(NEW) - ARRAY['lifecycle_state', 'effective_to'])
      IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['lifecycle_state', 'effective_to'])
      OR (OLD.effective_to IS NOT NULL AND NEW.effective_to IS DISTINCT FROM OLD.effective_to) THEN
    RAISE EXCEPTION 'policy semantics are append-only; only lifecycle and one close may change'
      USING ERRCODE = '55000';
  END IF;
  IF NEW.lifecycle_state = OLD.lifecycle_state THEN
    RETURN NEW;
  END IF;
  IF OLD.effective_to IS NOT NULL OR NEW.effective_to IS NOT NULL THEN
    RAISE EXCEPTION 'closed policy versions cannot advance lifecycle'
      USING ERRCODE = '55000';
  END IF;
  IF OLD.lifecycle_state = 'DRAFT' AND NEW.lifecycle_state = 'SHADOW' THEN
    SELECT count(*), count(*) FILTER (
             WHERE enabled AND priority = 0 AND fallback_class = 'NONE'
           )
      INTO candidate_count, pinned_candidate_count
      FROM control.reasoning_route_candidates
     WHERE tenant_id = OLD.tenant_id
       AND route_policy_id = OLD.route_policy_id
       AND route_policy_version = OLD.policy_version;
    IF candidate_count = 1 AND pinned_candidate_count = 1 THEN
      RETURN NEW;
    END IF;
    RAISE EXCEPTION 'PINNED policy requires exactly one priority-0 candidate before SHADOW'
      USING ERRCODE = '55000';
  END IF;
  IF OLD.lifecycle_state = 'SHADOW' AND NEW.lifecycle_state = 'SERVING' THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'policy lifecycle must advance DRAFT to SHADOW to SERVING without backtracking or skips'
    USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER provider_accounts_check_owner
  BEFORE INSERT OR UPDATE ON control.provider_accounts
  FOR EACH ROW EXECUTE FUNCTION control.provider_accounts_check_owner();
CREATE TRIGGER provider_endpoints_check_account
  BEFORE INSERT OR UPDATE ON control.provider_endpoints
  FOR EACH ROW EXECUTE FUNCTION control.provider_endpoints_check_account();
CREATE TRIGGER provider_billing_accounts_check_account
  BEFORE INSERT OR UPDATE ON control.provider_billing_accounts
  FOR EACH ROW EXECUTE FUNCTION control.provider_billing_accounts_check_account();
CREATE TRIGGER provider_billing_instruments_check_account
  BEFORE INSERT OR UPDATE ON control.provider_billing_instruments
  FOR EACH ROW EXECUTE FUNCTION control.provider_billing_instruments_check_account();
CREATE TRIGGER reasoning_credential_bindings_check_authority
  BEFORE INSERT OR UPDATE ON control.reasoning_credential_bindings
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_credential_bindings_check_authority();
CREATE TRIGGER reasoning_profiles_check_ownership
  BEFORE INSERT OR UPDATE ON control.reasoning_profiles
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_profiles_check_ownership();
CREATE TRIGGER reasoning_route_policies_check_owner
  BEFORE INSERT OR UPDATE ON control.reasoning_route_policies
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_route_policies_check_owner();
CREATE TRIGGER reasoning_route_candidates_check_profile
  BEFORE INSERT OR UPDATE ON control.reasoning_route_candidates
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_route_candidates_check_profile();
CREATE TRIGGER reasoning_route_bindings_check_domain_and_policy
  BEFORE INSERT OR UPDATE ON control.reasoning_route_bindings
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_route_bindings_check_domain_and_policy();
CREATE TRIGGER reasoning_profiles_append_only
  BEFORE UPDATE OR DELETE ON control.reasoning_profiles
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_profile_state_only();
CREATE TRIGGER reasoning_credential_bindings_append_only
  BEFORE UPDATE OR DELETE ON control.reasoning_credential_bindings
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_version_row_reject_mutation();
CREATE TRIGGER reasoning_route_policies_append_only
  BEFORE UPDATE OR DELETE ON control.reasoning_route_policies
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_policy_state_only();
CREATE TRIGGER reasoning_route_candidates_append_only
  BEFORE UPDATE OR DELETE ON control.reasoning_route_candidates
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_version_row_reject_mutation();
CREATE TRIGGER reasoning_route_bindings_close_only
  BEFORE UPDATE OR DELETE ON control.reasoning_route_bindings
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_binding_close_only();
CREATE TRIGGER processor_models_identity_immutable
  BEFORE UPDATE OR DELETE ON control.processor_models
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_parent_identity_reject_mutation();
CREATE TRIGGER provider_accounts_identity_immutable
  BEFORE UPDATE OR DELETE ON control.provider_accounts
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_parent_identity_reject_mutation();
CREATE TRIGGER provider_endpoints_identity_immutable
  BEFORE UPDATE OR DELETE ON control.provider_endpoints
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_parent_identity_reject_mutation();
CREATE TRIGGER provider_billing_accounts_identity_immutable
  BEFORE UPDATE OR DELETE ON control.provider_billing_accounts
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_parent_identity_reject_mutation();
CREATE TRIGGER provider_billing_instruments_identity_immutable
  BEFORE UPDATE OR DELETE ON control.provider_billing_instruments
  FOR EACH ROW EXECUTE FUNCTION control.reasoning_parent_identity_reject_mutation();

ALTER FUNCTION control.provider_accounts_check_owner() OWNER TO role_migration_owner;
ALTER FUNCTION control.provider_endpoints_check_account() OWNER TO role_migration_owner;
ALTER FUNCTION control.provider_billing_accounts_check_account() OWNER TO role_migration_owner;
ALTER FUNCTION control.provider_billing_instruments_check_account() OWNER TO role_migration_owner;
ALTER FUNCTION control.reasoning_credential_bindings_check_authority() OWNER TO role_migration_owner;
ALTER FUNCTION control.reasoning_profiles_check_ownership() OWNER TO role_migration_owner;
ALTER FUNCTION control.reasoning_route_policies_check_owner() OWNER TO role_migration_owner;
ALTER FUNCTION control.reasoning_route_candidates_check_profile() OWNER TO role_migration_owner;
ALTER FUNCTION control.reasoning_route_bindings_check_domain_and_policy() OWNER TO role_migration_owner;
ALTER FUNCTION control.reasoning_version_row_reject_mutation() OWNER TO role_migration_owner;
ALTER FUNCTION control.reasoning_binding_close_only() OWNER TO role_migration_owner;
ALTER FUNCTION control.reasoning_parent_identity_reject_mutation() OWNER TO role_migration_owner;
ALTER FUNCTION control.reasoning_profile_state_only() OWNER TO role_migration_owner;
ALTER FUNCTION control.reasoning_policy_state_only() OWNER TO role_migration_owner;

ALTER TABLE control.processor_models OWNER TO role_migration_owner;
REVOKE ALL ON TABLE control.processor_models FROM PUBLIC, role_gateway, role_private_worker,
  role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer,
  role_maintenance;

DO $$
DECLARE table_name text;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'provider_accounts', 'provider_endpoints', 'provider_billing_accounts',
    'provider_billing_instruments', 'reasoning_profiles', 'reasoning_route_policies',
    'reasoning_route_candidates', 'reasoning_route_bindings', 'reasoning_credential_bindings'
  ] LOOP
    EXECUTE format('ALTER TABLE control.%I ENABLE ROW LEVEL SECURITY', table_name);
    EXECUTE format('ALTER TABLE control.%I FORCE ROW LEVEL SECURITY', table_name);
    EXECUTE format(
      'CREATE POLICY %I ON control.%I USING (tenant_id = NULLIF(current_setting(''humaux.tenant_id'', true), '''')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting(''humaux.tenant_id'', true), '''')::uuid)',
      table_name || '_tenant', table_name
    );
    EXECUTE format('ALTER TABLE control.%I OWNER TO role_migration_owner', table_name);
    EXECUTE format('REVOKE ALL ON TABLE control.%I FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance', table_name);
  END LOOP;
END;
$$;
