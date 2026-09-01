-- §19.2 / §72.3 persistent retrieval-provider budget admission.  0091/0092/0093
-- remain the sole limit configuration: this migration records only per-call allocations.
-- A 60-second sliding aggregate of RESERVED and CONSUMED allocations is the safety boundary;
-- RELEASED and EXPIRED rows are retained but no longer count.  This avoids a fixed-window
-- boundary burst and keeps ModelCallLedger as the one external-call identity.

ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_tenant_model_call_unique UNIQUE (tenant_id, model_call_id);

CREATE TABLE ops.retrieval_provider_budget_reservations (
  reservation_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  model_call_id uuid NOT NULL,
  provider_id text NOT NULL CHECK (btrim(provider_id) <> ''),
  model_id text NOT NULL CHECK (btrim(model_id) <> ''),
  region text NOT NULL CHECK (btrim(region) <> ''),
  purpose text NOT NULL CHECK (purpose IN ('embedding', 'rerank')),
  requested_tokens bigint NOT NULL CHECK (requested_tokens > 0),
  ttl_micros bigint NOT NULL CHECK (ttl_micros > 0),
  reserved_at timestamptz NOT NULL CHECK (isfinite(reserved_at)),
  expires_at timestamptz NOT NULL CHECK (isfinite(expires_at) AND expires_at > reserved_at),
  dispatched_at timestamptz,
  settled_at timestamptz,
  status text NOT NULL CHECK (status IN ('RESERVED', 'CONSUMED', 'RELEASED', 'EXPIRED')),
  CONSTRAINT retrieval_provider_budget_reservations_model_call_fk
    FOREIGN KEY (tenant_id, model_call_id)
    REFERENCES ops.model_call_ledger(tenant_id, model_call_id),
  CONSTRAINT retrieval_provider_budget_reservations_model_call_unique
    UNIQUE (tenant_id, model_call_id),
  CONSTRAINT retrieval_provider_budget_reservations_tenant_reservation_unique
    UNIQUE (tenant_id, reservation_id),
  CONSTRAINT retrieval_provider_budget_reservations_state_shape CHECK (
    (status = 'RESERVED' AND settled_at IS NULL)
    OR (status IN ('CONSUMED', 'RELEASED', 'EXPIRED') AND settled_at IS NOT NULL)
  )
);

CREATE INDEX retrieval_provider_budget_reservations_status_reserved_idx
  ON ops.retrieval_provider_budget_reservations (status, reserved_at, reservation_id);
CREATE INDEX retrieval_provider_budget_reservations_tenant_expiry_idx
  ON ops.retrieval_provider_budget_reservations (tenant_id, status, expires_at);

CREATE TABLE ops.retrieval_provider_budget_allocations (
  tenant_id uuid NOT NULL,
  reservation_id uuid NOT NULL,
  limit_id uuid NOT NULL REFERENCES control.retrieval_provider_admission_limits(limit_id),
  tier text NOT NULL CHECK (tier IN ('GLOBAL', 'REGION', 'TENANT', 'PURPOSE')),
  limit_provider_id text NOT NULL CHECK (btrim(limit_provider_id) <> ''),
  limit_tenant_id uuid,
  limit_region text,
  limit_purpose text,
  limit_tpm_limit bigint NOT NULL CHECK (limit_tpm_limit > 0),
  limit_rpm_limit bigint NOT NULL CHECK (limit_rpm_limit > 0),
  tokens bigint NOT NULL CHECK (tokens > 0),
  requests bigint NOT NULL CHECK (requests = 1),
  reserved_at timestamptz NOT NULL CHECK (isfinite(reserved_at)),
  PRIMARY KEY (tenant_id, reservation_id, limit_id),
  CONSTRAINT retrieval_provider_budget_allocations_reservation_fk
    FOREIGN KEY (tenant_id, reservation_id)
    REFERENCES ops.retrieval_provider_budget_reservations(tenant_id, reservation_id)
);

CREATE INDEX retrieval_provider_budget_allocations_limit_sliding_idx
  ON ops.retrieval_provider_budget_allocations (limit_id, reserved_at, reservation_id);

ALTER TABLE ops.retrieval_provider_budget_reservations OWNER TO role_migration_owner;
ALTER TABLE ops.retrieval_provider_budget_allocations OWNER TO role_migration_owner;
ALTER TABLE ops.retrieval_provider_budget_reservations ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.retrieval_provider_budget_reservations FORCE ROW LEVEL SECURITY;
ALTER TABLE ops.retrieval_provider_budget_allocations ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.retrieval_provider_budget_allocations FORCE ROW LEVEL SECURITY;

-- The owner branch is reachable only through the narrowly ACLed SECURITY DEFINER routines
-- below.  It is required for Global/Region rolling sums, which must see allocations belonging
-- to every tenant; ordinary runtime SQL remains tenant-scoped.
CREATE POLICY retrieval_provider_budget_reservations_tenant_isolation
  ON ops.retrieval_provider_budget_reservations
  USING (
    current_user = 'role_migration_owner'
    OR tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
  )
  WITH CHECK (
    current_user = 'role_migration_owner'
    OR tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
  );
CREATE POLICY retrieval_provider_budget_allocations_tenant_isolation
  ON ops.retrieval_provider_budget_allocations
  USING (
    current_user = 'role_migration_owner'
    OR tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
  )
  WITH CHECK (
    current_user = 'role_migration_owner'
    OR tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
  );

CREATE FUNCTION ops.retrieval_provider_budget_reservation_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE ledger ops.model_call_ledger%ROWTYPE;
DECLARE global_count integer;
DECLARE region_count integer;
DECLARE tenant_count integer;
DECLARE purpose_count integer;
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'retrieval provider budget reservations are retained; DELETE is not permitted'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF TG_OP = 'INSERT' THEN
    SELECT * INTO ledger FROM ops.model_call_ledger
      WHERE tenant_id = NEW.tenant_id AND model_call_id = NEW.model_call_id;
    IF NOT FOUND
       OR ledger.status <> 'RESERVED'
       OR ledger.provider IS DISTINCT FROM NEW.provider_id
       OR ledger.model IS DISTINCT FROM NEW.model_id
       OR ledger.purpose IS DISTINCT FROM NEW.purpose
       OR NEW.status <> 'RESERVED'
    THEN
      RAISE EXCEPTION 'provider budget reservation requires matching RESERVED ModelCallLedger identity'
        USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
  END IF;
  IF OLD.tenant_id IS DISTINCT FROM NEW.tenant_id
     OR OLD.model_call_id IS DISTINCT FROM NEW.model_call_id
     OR OLD.provider_id IS DISTINCT FROM NEW.provider_id
     OR OLD.model_id IS DISTINCT FROM NEW.model_id
     OR OLD.region IS DISTINCT FROM NEW.region
     OR OLD.purpose IS DISTINCT FROM NEW.purpose
     OR OLD.requested_tokens IS DISTINCT FROM NEW.requested_tokens
     OR OLD.ttl_micros IS DISTINCT FROM NEW.ttl_micros
     OR OLD.reserved_at IS DISTINCT FROM NEW.reserved_at
     OR OLD.expires_at IS DISTINCT FROM NEW.expires_at
     OR (OLD.dispatched_at IS NOT NULL AND NEW.dispatched_at IS DISTINCT FROM OLD.dispatched_at)
  THEN
    RAISE EXCEPTION 'provider budget reservation identity is immutable'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.status = 'RESERVED' AND NEW.status = 'RESERVED'
     AND OLD.dispatched_at IS NULL AND NEW.dispatched_at IS NOT NULL THEN
    IF (SELECT count(*) FROM ops.retrieval_provider_budget_allocations a
        WHERE a.tenant_id=NEW.tenant_id AND a.reservation_id=NEW.reservation_id) <> 4 THEN
      RAISE EXCEPTION 'dispatch requires exactly four durable budget allocations' USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
  END IF;
  IF OLD.status <> 'RESERVED' OR NEW.status NOT IN ('CONSUMED', 'RELEASED', 'EXPIRED') THEN
    RAISE EXCEPTION 'provider budget reservation may transition once from RESERVED to a terminal state'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  SELECT
    count(*) FILTER (WHERE a.tier = 'GLOBAL'),
    count(*) FILTER (WHERE a.tier = 'REGION'),
    count(*) FILTER (WHERE a.tier = 'TENANT'),
    count(*) FILTER (WHERE a.tier = 'PURPOSE')
  INTO global_count, region_count, tenant_count, purpose_count
  FROM ops.retrieval_provider_budget_allocations a
  WHERE a.tenant_id = NEW.tenant_id AND a.reservation_id = NEW.reservation_id;
  IF global_count <> 1 OR region_count <> 1 OR tenant_count <> 1 OR purpose_count <> 1 THEN
    RAISE EXCEPTION 'terminal provider budget reservation requires exactly four canonical allocations'
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END;
$$;
ALTER FUNCTION ops.retrieval_provider_budget_reservation_guard() OWNER TO role_migration_owner;
CREATE TRIGGER retrieval_provider_budget_reservation_guard
BEFORE INSERT OR UPDATE OR DELETE ON ops.retrieval_provider_budget_reservations
FOR EACH ROW EXECUTE FUNCTION ops.retrieval_provider_budget_reservation_guard();

CREATE FUNCTION ops.retrieval_provider_budget_allocation_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE reservation ops.retrieval_provider_budget_reservations%ROWTYPE;
DECLARE admission_limit control.retrieval_provider_admission_limits%ROWTYPE;
DECLARE config_purpose text;
DECLARE v_tier text;
BEGIN
  IF TG_OP <> 'INSERT' THEN
    RAISE EXCEPTION 'retrieval provider budget allocations are append-only'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  SELECT * INTO reservation FROM ops.retrieval_provider_budget_reservations
    WHERE tenant_id = NEW.tenant_id AND reservation_id = NEW.reservation_id;
  SELECT * INTO admission_limit FROM control.retrieval_provider_admission_limits
    WHERE limit_id = NEW.limit_id;
  config_purpose := CASE reservation.purpose
    WHEN 'embedding' THEN 'RETRIEVAL_EMBEDDING'
    WHEN 'rerank' THEN 'RETRIEVAL_RERANK'
  END;
  IF reservation.reservation_id IS NULL
     OR admission_limit.limit_id IS NULL
     OR NEW.tokens <> reservation.requested_tokens
     OR NEW.requests <> 1
     OR NEW.reserved_at IS DISTINCT FROM reservation.reserved_at
     OR admission_limit.provider_id IS DISTINCT FROM reservation.provider_id
     OR NEW.limit_provider_id IS DISTINCT FROM admission_limit.provider_id
     OR NEW.limit_tenant_id IS DISTINCT FROM admission_limit.tenant_id
     OR NEW.limit_region IS DISTINCT FROM admission_limit.region
     OR NEW.limit_purpose IS DISTINCT FROM admission_limit.purpose
     OR NEW.limit_tpm_limit IS DISTINCT FROM admission_limit.tpm_limit
     OR NEW.limit_rpm_limit IS DISTINCT FROM admission_limit.rpm_limit
  THEN
    RAISE EXCEPTION 'provider budget allocation must bind one canonical admission tier'
      USING ERRCODE = 'check_violation';
  END IF;
  IF admission_limit.tenant_id IS NULL AND admission_limit.region IS NULL AND admission_limit.purpose IS NULL THEN
    v_tier := 'GLOBAL';
  ELSIF admission_limit.tenant_id IS NULL AND admission_limit.region = reservation.region AND admission_limit.purpose IS NULL THEN
    v_tier := 'REGION';
  ELSIF admission_limit.tenant_id = reservation.tenant_id AND admission_limit.region IS NULL AND admission_limit.purpose IS NULL THEN
    v_tier := 'TENANT';
  ELSIF admission_limit.tenant_id = reservation.tenant_id AND admission_limit.region IS NULL AND admission_limit.purpose = config_purpose THEN
    v_tier := 'PURPOSE';
  ELSE
    RAISE EXCEPTION 'provider budget allocation must bind one canonical admission tier'
      USING ERRCODE = 'check_violation';
  END IF;
  IF NEW.tier IS DISTINCT FROM v_tier THEN
    RAISE EXCEPTION 'provider budget allocation tier snapshot must match its admission limit'
      USING ERRCODE = 'check_violation';
  END IF;
  IF EXISTS (
    SELECT 1 FROM ops.retrieval_provider_budget_allocations a
    WHERE a.tenant_id = NEW.tenant_id AND a.reservation_id = NEW.reservation_id
      AND a.tier = v_tier
  ) THEN
    RAISE EXCEPTION 'provider budget reservation cannot allocate a canonical tier twice'
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END;
$$;
ALTER FUNCTION ops.retrieval_provider_budget_allocation_guard() OWNER TO role_migration_owner;
CREATE TRIGGER retrieval_provider_budget_allocation_guard
BEFORE INSERT OR UPDATE OR DELETE ON ops.retrieval_provider_budget_allocations
FOR EACH ROW EXECUTE FUNCTION ops.retrieval_provider_budget_allocation_guard();

CREATE FUNCTION ops.reserve_retrieval_provider_budget(
  p_tenant_id uuid,
  p_model_call_id uuid,
  p_provider_id text,
  p_model_id text,
  p_region text,
  p_purpose text,
  p_estimated_tokens bigint,
  p_ttl_micros bigint
) RETURNS TABLE(reservation_id uuid, reserved_at timestamptz, expires_at timestamptz, status text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_now timestamptz := clock_timestamp();
DECLARE v_locked_now timestamptz;
DECLARE v_expires_at timestamptz;
DECLARE v_config_purpose text;
DECLARE v_existing ops.retrieval_provider_budget_reservations%ROWTYPE;
DECLARE v_ledger ops.model_call_ledger%ROWTYPE;
DECLARE v_limit_ids uuid[];
DECLARE v_rechecked_ids uuid[];
DECLARE v_limit control.retrieval_provider_admission_limits%ROWTYPE;
DECLARE v_tokens bigint;
DECLARE v_requests bigint;
DECLARE v_global_count integer;
DECLARE v_region_count integer;
DECLARE v_tenant_count integer;
DECLARE v_purpose_count integer;
BEGIN
  IF session_user <> 'role_retrieval_worker' THEN
    RAISE EXCEPTION 'provider budget reserve requires role_retrieval_worker LOGIN' USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF NULLIF(current_setting('humaux.tenant_id', true), '') IS DISTINCT FROM p_tenant_id::text THEN
    RAISE EXCEPTION 'provider budget reserve tenant context mismatch' USING ERRCODE = 'insufficient_privilege';
  END IF;
  PERFORM set_config('humaux.tenant_id', p_tenant_id::text, true);
  IF p_tenant_id IS NULL OR p_model_call_id IS NULL OR p_estimated_tokens IS NULL OR p_ttl_micros IS NULL OR p_estimated_tokens <= 0 OR p_ttl_micros <= 0
     OR NULLIF(btrim(p_provider_id), '') IS NULL OR NULLIF(btrim(p_model_id), '') IS NULL
     OR NULLIF(btrim(p_region), '') IS NULL OR p_purpose IS NULL OR p_purpose NOT IN ('embedding', 'rerank')
  THEN RAISE EXCEPTION 'invalid provider budget reservation input' USING ERRCODE = 'P0003'; END IF;
  v_config_purpose := CASE p_purpose WHEN 'embedding' THEN 'RETRIEVAL_EMBEDDING' ELSE 'RETRIEVAL_RERANK' END;
  -- This two-int advisory namespace is distinct from the 64-bit limit locks below.  Every
  -- reserve call takes it first, so concurrent identical calls replay one durable row.
  PERFORM pg_advisory_xact_lock(
    hashtext(p_tenant_id::text),
    hashtext(p_model_call_id::text)
  );
  SELECT * INTO v_existing FROM ops.retrieval_provider_budget_reservations
    WHERE tenant_id = p_tenant_id AND model_call_id = p_model_call_id FOR UPDATE;
  IF FOUND THEN
    IF v_existing.provider_id IS DISTINCT FROM p_provider_id OR v_existing.model_id IS DISTINCT FROM p_model_id
       OR v_existing.region IS DISTINCT FROM p_region OR v_existing.purpose IS DISTINCT FROM p_purpose
       OR v_existing.requested_tokens IS DISTINCT FROM p_estimated_tokens OR v_existing.ttl_micros IS DISTINCT FROM p_ttl_micros
    THEN RAISE EXCEPTION 'provider budget reservation replay identity mismatch' USING ERRCODE = 'P0003'; END IF;
    RETURN QUERY SELECT v_existing.reservation_id, v_existing.reserved_at, v_existing.expires_at, v_existing.status;
    RETURN;
  END IF;
  SELECT * INTO v_ledger FROM ops.model_call_ledger
    WHERE tenant_id = p_tenant_id AND model_call_id = p_model_call_id FOR KEY SHARE;
  IF NOT FOUND OR v_ledger.status <> 'RESERVED' OR v_ledger.provider IS DISTINCT FROM p_provider_id
     OR v_ledger.model IS DISTINCT FROM p_model_id OR v_ledger.purpose IS DISTINCT FROM p_purpose
  THEN RAISE EXCEPTION 'provider budget requires matching RESERVED ModelCallLedger' USING ERRCODE = 'P0003'; END IF;
  SELECT array_agg(limit_id ORDER BY limit_id) INTO v_limit_ids FROM control.retrieval_provider_admission_limits l
   WHERE l.provider_id = p_provider_id AND l.effective_from <= v_now AND (l.effective_to IS NULL OR l.effective_to > v_now)
     AND ((l.tenant_id IS NULL AND l.region IS NULL AND l.purpose IS NULL)
       OR (l.tenant_id IS NULL AND l.region = p_region AND l.purpose IS NULL)
       OR (l.tenant_id = p_tenant_id AND l.region IS NULL AND l.purpose IS NULL)
       OR (l.tenant_id = p_tenant_id AND l.region IS NULL AND l.purpose = v_config_purpose));
  FOR v_limit IN SELECT l.* FROM control.retrieval_provider_admission_limits l WHERE l.limit_id = ANY(v_limit_ids) ORDER BY l.limit_id LOOP
    PERFORM pg_advisory_xact_lock(hashtextextended(v_limit.limit_id::text, 0));
  END LOOP;
  v_locked_now := clock_timestamp();
  SELECT array_agg(limit_id ORDER BY limit_id) INTO v_rechecked_ids FROM control.retrieval_provider_admission_limits l
   WHERE l.provider_id = p_provider_id AND l.effective_from <= v_locked_now AND (l.effective_to IS NULL OR l.effective_to > v_locked_now)
     AND ((l.tenant_id IS NULL AND l.region IS NULL AND l.purpose IS NULL)
       OR (l.tenant_id IS NULL AND l.region = p_region AND l.purpose IS NULL)
       OR (l.tenant_id = p_tenant_id AND l.region IS NULL AND l.purpose IS NULL)
       OR (l.tenant_id = p_tenant_id AND l.region IS NULL AND l.purpose = v_config_purpose));
  IF v_rechecked_ids IS DISTINCT FROM v_limit_ids THEN
    RAISE EXCEPTION 'provider budget active limits changed during admission' USING ERRCODE = 'P0003';
  END IF;
  SELECT
    count(*) FILTER (WHERE l.tenant_id IS NULL AND l.region IS NULL AND l.purpose IS NULL),
    count(*) FILTER (WHERE l.tenant_id IS NULL AND l.region = p_region AND l.purpose IS NULL),
    count(*) FILTER (WHERE l.tenant_id = p_tenant_id AND l.region IS NULL AND l.purpose IS NULL),
    count(*) FILTER (WHERE l.tenant_id = p_tenant_id AND l.region IS NULL AND l.purpose = v_config_purpose)
  INTO v_global_count, v_region_count, v_tenant_count, v_purpose_count
  FROM control.retrieval_provider_admission_limits l WHERE l.limit_id = ANY(v_limit_ids);
  IF v_global_count <> 1 OR v_region_count <> 1 OR v_tenant_count <> 1 OR v_purpose_count <> 1 THEN
    RAISE EXCEPTION 'provider budget requires one active limit for each canonical tier' USING ERRCODE = 'P0003';
  END IF;
  FOR v_limit IN SELECT l.* FROM control.retrieval_provider_admission_limits l WHERE l.limit_id = ANY(v_limit_ids) ORDER BY l.limit_id LOOP
    SELECT coalesce(sum(a.tokens), 0), coalesce(sum(a.requests), 0) INTO v_tokens, v_requests
      FROM ops.retrieval_provider_budget_allocations a
      JOIN ops.retrieval_provider_budget_reservations r ON r.tenant_id = a.tenant_id AND r.reservation_id = a.reservation_id
      WHERE a.limit_id = v_limit.limit_id AND r.reserved_at > v_locked_now - interval '60 seconds'
        AND r.status IN ('RESERVED', 'CONSUMED');
    IF v_tokens::numeric + p_estimated_tokens::numeric > v_limit.tpm_limit::numeric
       OR v_requests::numeric + 1 > v_limit.rpm_limit::numeric
    THEN RAISE EXCEPTION 'provider budget exceeded' USING ERRCODE = 'P0002'; END IF;
  END LOOP;
  v_expires_at := v_locked_now + p_ttl_micros * interval '1 microsecond';
  INSERT INTO ops.retrieval_provider_budget_reservations
    (tenant_id, model_call_id, provider_id, model_id, region, purpose, requested_tokens, ttl_micros, reserved_at, expires_at, status)
  VALUES (p_tenant_id, p_model_call_id, p_provider_id, p_model_id, p_region, p_purpose, p_estimated_tokens, p_ttl_micros, v_locked_now, v_expires_at, 'RESERVED')
  RETURNING retrieval_provider_budget_reservations.reservation_id INTO reservation_id;
  FOR v_limit IN SELECT l.* FROM control.retrieval_provider_admission_limits l WHERE l.limit_id = ANY(v_limit_ids) ORDER BY l.limit_id LOOP
    INSERT INTO ops.retrieval_provider_budget_allocations
      (tenant_id, reservation_id, limit_id, tier, limit_provider_id, limit_tenant_id, limit_region, limit_purpose, limit_tpm_limit, limit_rpm_limit, tokens, requests, reserved_at)
    VALUES (
      p_tenant_id, reservation_id, v_limit.limit_id,
      CASE
        WHEN v_limit.tenant_id IS NULL AND v_limit.region IS NULL AND v_limit.purpose IS NULL THEN 'GLOBAL'
        WHEN v_limit.tenant_id IS NULL AND v_limit.region = p_region AND v_limit.purpose IS NULL THEN 'REGION'
        WHEN v_limit.tenant_id = p_tenant_id AND v_limit.region IS NULL AND v_limit.purpose IS NULL THEN 'TENANT'
        ELSE 'PURPOSE'
      END,
      v_limit.provider_id, v_limit.tenant_id, v_limit.region, v_limit.purpose,
      v_limit.tpm_limit, v_limit.rpm_limit, p_estimated_tokens, 1, v_locked_now
    );
  END LOOP;
  reserved_at := v_locked_now; expires_at := v_expires_at; status := 'RESERVED'; RETURN NEXT;
END;
$$;

CREATE FUNCTION ops.mark_retrieval_provider_budget_dispatched(
  p_tenant_id uuid, p_reservation_id uuid
) RETURNS timestamptz
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_reservation ops.retrieval_provider_budget_reservations%ROWTYPE;
DECLARE v_ledger ops.model_call_ledger%ROWTYPE;
DECLARE v_dispatched_at timestamptz;
BEGIN
  IF p_tenant_id IS NULL OR p_reservation_id IS NULL THEN RAISE EXCEPTION 'provider budget dispatch input is required' USING ERRCODE = 'P0003'; END IF;
  IF session_user <> 'role_retrieval_worker' OR NULLIF(current_setting('humaux.tenant_id', true), '') IS DISTINCT FROM p_tenant_id::text THEN RAISE EXCEPTION 'provider budget dispatch requires tenant-scoped role_retrieval_worker LOGIN' USING ERRCODE = 'insufficient_privilege'; END IF;
  PERFORM set_config('humaux.tenant_id', p_tenant_id::text, true);
  SELECT * INTO v_reservation FROM ops.retrieval_provider_budget_reservations WHERE tenant_id=p_tenant_id AND reservation_id=p_reservation_id FOR UPDATE;
  IF NOT FOUND OR v_reservation.status <> 'RESERVED' OR v_reservation.expires_at <= clock_timestamp() THEN RAISE EXCEPTION 'provider budget reservation is not dispatchable' USING ERRCODE = 'P0003'; END IF;
  SELECT * INTO v_ledger FROM ops.model_call_ledger
    WHERE tenant_id = v_reservation.tenant_id AND model_call_id = v_reservation.model_call_id FOR UPDATE;
  IF NOT FOUND
     OR v_ledger.tenant_id IS DISTINCT FROM v_reservation.tenant_id
     OR v_ledger.model_call_id IS DISTINCT FROM v_reservation.model_call_id
     OR v_ledger.status <> 'RESERVED'
  THEN RAISE EXCEPTION 'provider budget dispatch requires matching RESERVED ModelCallLedger' USING ERRCODE = 'P0003'; END IF;
  IF v_reservation.dispatched_at IS NULL THEN
    UPDATE ops.retrieval_provider_budget_reservations SET dispatched_at=clock_timestamp() WHERE tenant_id=p_tenant_id AND reservation_id=p_reservation_id RETURNING dispatched_at INTO v_dispatched_at;
  ELSE v_dispatched_at := v_reservation.dispatched_at; END IF;
  RETURN v_dispatched_at;
END;
$$;

CREATE FUNCTION ops.settle_retrieval_provider_budget(
  p_tenant_id uuid, p_reservation_id uuid
) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_reservation ops.retrieval_provider_budget_reservations%ROWTYPE;
DECLARE v_ledger_status text;
DECLARE v_target text;
BEGIN
  IF p_tenant_id IS NULL OR p_reservation_id IS NULL THEN RAISE EXCEPTION 'provider budget settle input is required' USING ERRCODE = 'P0003'; END IF;
  IF session_user <> 'role_retrieval_worker' THEN RAISE EXCEPTION 'provider budget settle requires role_retrieval_worker LOGIN' USING ERRCODE = 'insufficient_privilege'; END IF;
  IF NULLIF(current_setting('humaux.tenant_id', true), '') IS DISTINCT FROM p_tenant_id::text THEN RAISE EXCEPTION 'provider budget settle tenant context mismatch' USING ERRCODE = 'insufficient_privilege'; END IF;
  PERFORM set_config('humaux.tenant_id', p_tenant_id::text, true);
  SELECT * INTO v_reservation FROM ops.retrieval_provider_budget_reservations
    WHERE tenant_id = p_tenant_id AND reservation_id = p_reservation_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'provider budget reservation not found' USING ERRCODE = 'P0003'; END IF;
  IF v_reservation.status IN ('CONSUMED','RELEASED') THEN RETURN v_reservation.status; END IF;
  IF v_reservation.status <> 'RESERVED' OR v_reservation.expires_at <= clock_timestamp() THEN
    RAISE EXCEPTION 'provider budget reservation is not settleable' USING ERRCODE = 'P0003';
  END IF;
  SELECT status INTO v_ledger_status FROM ops.model_call_ledger
    WHERE tenant_id = p_tenant_id AND model_call_id = v_reservation.model_call_id FOR KEY SHARE;
  IF v_reservation.dispatched_at IS NULL AND v_ledger_status = 'FAILED' THEN v_target := 'RELEASED';
  ELSIF v_reservation.dispatched_at IS NOT NULL AND v_ledger_status IN ('SUCCEEDED', 'FAILED') THEN v_target := 'CONSUMED';
  ELSE
    RAISE EXCEPTION 'provider budget settlement requires matching terminal ModelCallLedger' USING ERRCODE = 'P0003';
  END IF;
  UPDATE ops.retrieval_provider_budget_reservations
    SET status = v_target, settled_at = clock_timestamp()
    WHERE tenant_id = p_tenant_id AND reservation_id = p_reservation_id;
  RETURN v_target;
END;
$$;

-- Normal completion is one identity-bound database primitive. It locks reservation -> ledger,
-- matching the dispatch path, so callers cannot pair one call's ledger with another call's
-- budget and concurrent dispatch/finalize work cannot form an inverse row-lock cycle.
CREATE FUNCTION ops.finalize_retrieval_provider_budget(
  p_tenant_id uuid,
  p_reservation_id uuid,
  p_model_call_id uuid,
  p_outcome text,
  p_input_tokens bigint,
  p_billable_tokens bigint,
  p_candidate_count integer,
  p_candidate_tokens bigint,
  p_cache_hit boolean,
  p_latency_ms integer,
  p_actual_cost double precision,
  p_error_class text,
  p_provider_request_id text
) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_reservation ops.retrieval_provider_budget_reservations%ROWTYPE;
DECLARE v_ledger ops.model_call_ledger%ROWTYPE;
DECLARE v_target text;
BEGIN
  IF session_user <> 'role_retrieval_worker' THEN RAISE EXCEPTION 'provider budget finalize requires role_retrieval_worker LOGIN' USING ERRCODE = 'insufficient_privilege'; END IF;
  IF p_tenant_id IS NULL OR p_reservation_id IS NULL OR p_model_call_id IS NULL OR p_outcome IS NULL OR p_outcome NOT IN ('SUCCEEDED','FAILED') THEN RAISE EXCEPTION 'provider budget finalize identity and outcome are required' USING ERRCODE = 'P0003'; END IF;
  IF NULLIF(current_setting('humaux.tenant_id', true), '') IS DISTINCT FROM p_tenant_id::text THEN RAISE EXCEPTION 'provider budget finalize tenant context mismatch' USING ERRCODE = 'insufficient_privilege'; END IF;
  PERFORM set_config('humaux.tenant_id', p_tenant_id::text, true);

  SELECT * INTO v_reservation FROM ops.retrieval_provider_budget_reservations
    WHERE tenant_id = p_tenant_id AND reservation_id = p_reservation_id FOR UPDATE;
  IF NOT FOUND OR v_reservation.model_call_id IS DISTINCT FROM p_model_call_id THEN
    RAISE EXCEPTION 'provider budget finalize identity mismatch' USING ERRCODE = 'P0003';
  END IF;

  SELECT * INTO v_ledger FROM ops.model_call_ledger
    WHERE tenant_id = p_tenant_id AND model_call_id = p_model_call_id FOR UPDATE;
  IF NOT FOUND OR v_ledger.model_call_id IS DISTINCT FROM v_reservation.model_call_id THEN
    RAISE EXCEPTION 'provider budget finalize requires matching ModelCallLedger' USING ERRCODE = 'P0003';
  END IF;

  IF v_ledger.status = 'RESERVED' THEN
    UPDATE ops.model_call_ledger
       SET status = p_outcome,
           input_tokens = p_input_tokens,
           billable_tokens = p_billable_tokens,
           candidate_count = p_candidate_count,
           candidate_tokens = p_candidate_tokens,
           cache_hit = p_cache_hit,
           latency_ms = p_latency_ms,
           actual_cost = p_actual_cost,
           error_class = p_error_class,
           provider_request_id = p_provider_request_id
     WHERE tenant_id = p_tenant_id AND model_call_id = p_model_call_id;
    v_ledger.status := p_outcome;
  ELSIF v_ledger.status NOT IN ('SUCCEEDED','FAILED') THEN
    RAISE EXCEPTION 'provider budget finalize requires terminal or RESERVED ModelCallLedger' USING ERRCODE = 'P0003';
  END IF;

  IF v_reservation.status IN ('CONSUMED','RELEASED') THEN RETURN v_reservation.status; END IF;
  IF v_reservation.status <> 'RESERVED' OR v_reservation.expires_at <= clock_timestamp() THEN
    RAISE EXCEPTION 'provider budget reservation is not finalizeable' USING ERRCODE = 'P0003';
  END IF;
  IF v_reservation.dispatched_at IS NULL AND v_ledger.status = 'FAILED' THEN v_target := 'RELEASED';
  ELSIF v_reservation.dispatched_at IS NOT NULL AND v_ledger.status IN ('SUCCEEDED','FAILED') THEN v_target := 'CONSUMED';
  ELSE
    RAISE EXCEPTION 'provider budget finalization requires a valid dispatch/outcome pair' USING ERRCODE = 'P0003';
  END IF;
  UPDATE ops.retrieval_provider_budget_reservations
     SET status = v_target, settled_at = clock_timestamp()
   WHERE tenant_id = p_tenant_id AND reservation_id = p_reservation_id;
  RETURN v_target;
END;
$$;

-- One maintenance primitive closes every crash residue without inventing another budget truth:
-- terminal ledgers settle immediately; an expired MAY_HAVE_REACHED reservation consumes even
-- when ledger finalization was lost; only an undispatched, non-terminal expiry becomes EXPIRED.
CREATE FUNCTION ops.reap_expired_retrieval_provider_budget(p_tenant_id uuid, p_limit integer)
RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_reservation record; DECLARE v_count integer := 0; DECLARE v_target text;
BEGIN
  IF session_user <> 'role_maintenance' THEN RAISE EXCEPTION 'provider budget reaper requires role_maintenance LOGIN' USING ERRCODE = 'insufficient_privilege'; END IF;
  IF NULLIF(current_setting('humaux.tenant_id', true), '') IS DISTINCT FROM p_tenant_id::text THEN RAISE EXCEPTION 'provider budget reaper tenant context mismatch' USING ERRCODE = 'insufficient_privilege'; END IF;
  IF p_tenant_id IS NULL OR p_limit IS NULL OR p_limit <= 0 THEN RAISE EXCEPTION 'provider budget reap limit must be positive' USING ERRCODE = 'P0003'; END IF;
  PERFORM set_config('humaux.tenant_id', p_tenant_id::text, true);
  FOR v_reservation IN
    SELECT r.reservation_id, r.dispatched_at, r.expires_at, l.status AS ledger_status
      FROM ops.retrieval_provider_budget_reservations r
      LEFT JOIN ops.model_call_ledger l
        ON l.tenant_id = r.tenant_id AND l.model_call_id = r.model_call_id
      WHERE r.tenant_id = p_tenant_id AND r.status = 'RESERVED'
        AND (l.status IN ('SUCCEEDED','FAILED') OR r.expires_at <= clock_timestamp())
      ORDER BY CASE WHEN l.status IN ('SUCCEEDED','FAILED') THEN 0 ELSE 1 END, r.expires_at
      FOR UPDATE OF r SKIP LOCKED LIMIT p_limit
  LOOP
    IF v_reservation.dispatched_at IS NOT NULL THEN v_target := 'CONSUMED';
    ELSIF v_reservation.ledger_status = 'FAILED' THEN v_target := 'RELEASED';
    ELSIF v_reservation.ledger_status = 'SUCCEEDED' THEN v_target := 'CONSUMED';
    ELSE v_target := 'EXPIRED'; END IF;
    UPDATE ops.retrieval_provider_budget_reservations SET status = v_target, settled_at = clock_timestamp()
      WHERE tenant_id = p_tenant_id AND reservation_id = v_reservation.reservation_id;
    v_count := v_count + 1;
  END LOOP;
  RETURN v_count;
END;
$$;

ALTER FUNCTION ops.reserve_retrieval_provider_budget(uuid, uuid, text, text, text, text, bigint, bigint) OWNER TO role_migration_owner;
ALTER FUNCTION ops.mark_retrieval_provider_budget_dispatched(uuid, uuid) OWNER TO role_migration_owner;
ALTER FUNCTION ops.settle_retrieval_provider_budget(uuid, uuid) OWNER TO role_migration_owner;
ALTER FUNCTION ops.finalize_retrieval_provider_budget(uuid, uuid, uuid, text, bigint, bigint, integer, bigint, boolean, integer, double precision, text, text) OWNER TO role_migration_owner;
ALTER FUNCTION ops.reap_expired_retrieval_provider_budget(uuid, integer) OWNER TO role_migration_owner;
REVOKE ALL ON ops.retrieval_provider_budget_reservations, ops.retrieval_provider_budget_allocations
  FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
       role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
REVOKE ALL ON FUNCTION ops.reserve_retrieval_provider_budget(uuid, uuid, text, text, text, text, bigint, bigint),
  ops.mark_retrieval_provider_budget_dispatched(uuid, uuid), ops.settle_retrieval_provider_budget(uuid, uuid),
  ops.finalize_retrieval_provider_budget(uuid, uuid, uuid, text, bigint, bigint, integer, bigint, boolean, integer, double precision, text, text),
  ops.reap_expired_retrieval_provider_budget(uuid, integer),
  ops.retrieval_provider_budget_reservation_guard(), ops.retrieval_provider_budget_allocation_guard() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.reserve_retrieval_provider_budget(uuid, uuid, text, text, text, text, bigint, bigint),
  ops.mark_retrieval_provider_budget_dispatched(uuid, uuid), ops.settle_retrieval_provider_budget(uuid, uuid),
  ops.finalize_retrieval_provider_budget(uuid, uuid, uuid, text, bigint, bigint, integer, bigint, boolean, integer, double precision, text, text) TO role_retrieval_worker;
GRANT EXECUTE ON FUNCTION ops.reap_expired_retrieval_provider_budget(uuid, integer) TO role_maintenance;
