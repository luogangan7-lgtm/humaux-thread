-- §72.2.1 / §83: the request plane consumes quota; only a controlled maintenance
-- issuer can create a period from the already-projected entitlement snapshot.
-- No role, password, direct quota-window INSERT, or RLS bypass is introduced.

ALTER TABLE control.quota_windows
  ADD COLUMN issued_snapshot_at timestamptz,
  ADD CONSTRAINT quota_window_bounds CHECK (
    isfinite(window_start) AND isfinite(window_end) AND window_start < window_end
    AND hard_limit >= 0 AND reserved >= 0 AND consumed >= 0 AND version >= 0
    AND reserved::numeric + consumed::numeric <= hard_limit::numeric
  );

CREATE FUNCTION control.quota_window_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, control AS $$
BEGIN
  IF ROW(NEW.tenant_id, NEW.entitlement_key, NEW.window_start, NEW.window_end,
         NEW.hard_limit, NEW.issued_snapshot_at)
     IS DISTINCT FROM
     ROW(OLD.tenant_id, OLD.entitlement_key, OLD.window_start, OLD.window_end,
         OLD.hard_limit, OLD.issued_snapshot_at) THEN
    RAISE EXCEPTION 'quota window identity and issued limit are immutable' USING ERRCODE = '23514';
  END IF;
  NEW.version := OLD.version + CASE WHEN ROW(NEW.reserved, NEW.consumed)
                                      IS DISTINCT FROM ROW(OLD.reserved, OLD.consumed)
                                   THEN 1 ELSE 0 END;
  RETURN NEW;
END;
$$;
ALTER FUNCTION control.quota_window_immutable() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.quota_window_immutable() FROM PUBLIC;
CREATE TRIGGER quota_window_immutable BEFORE UPDATE ON control.quota_windows
FOR EACH ROW EXECUTE FUNCTION control.quota_window_immutable();

CREATE FUNCTION control.issue_quota_window(p_tenant_id uuid, p_entitlement_key text)
RETURNS control.quota_windows
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, control AS $$
DECLARE
  policy jsonb;
  snapshot_at timestamptz;
  start_at timestamptz;
  end_at timestamptz;
  issuer_now timestamptz;
  quota_limit bigint;
  issued control.quota_windows;
BEGIN
  IF p_tenant_id IS NULL OR p_tenant_id = '00000000-0000-0000-0000-000000000000'::uuid
     OR p_entitlement_key IS DISTINCT FROM 'mcp.billable_operations.per_period'
     OR p_tenant_id IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'invalid quota issuer scope' USING ERRCODE = '42501';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('quota-window:' || p_tenant_id::text || ':' || p_entitlement_key, 0));
  SELECT effective -> p_entitlement_key, computed_at INTO policy, snapshot_at
  FROM control.entitlement_snapshots WHERE tenant_id = p_tenant_id FOR SHARE;
  issuer_now := clock_timestamp();
  IF policy IS NULL OR jsonb_typeof(policy) IS DISTINCT FROM 'object'
     OR jsonb_typeof(policy -> 'limit') IS DISTINCT FROM 'number'
     OR (policy ->> 'limit') !~ '^[0-9]+$'
     OR (policy ->> 'period') IS NULL
     OR (policy ->> 'period') NOT IN ('calendar_month', 'subscription_period')
     OR (policy ->> 'charge_policy') IS NULL
     OR (policy ->> 'charge_policy') NOT IN ('success_only', 'completed_business_calls')
     OR (policy ->> 'period_start') IS NULL OR (policy ->> 'period_end') IS NULL
     OR (policy ->> 'period_start') !~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$'
     OR (policy ->> 'period_end') !~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$'
     OR snapshot_at IS NULL OR NOT isfinite(snapshot_at) OR snapshot_at > issuer_now THEN
    RAISE EXCEPTION 'missing or invalid projected quota policy' USING ERRCODE = '22023';
  END IF;
  quota_limit := (policy ->> 'limit')::bigint;
  start_at := (policy ->> 'period_start')::timestamptz;
  end_at := (policy ->> 'period_end')::timestamptz;
  IF quota_limit < 0 OR NOT isfinite(start_at) OR NOT isfinite(end_at)
     OR start_at >= end_at OR issuer_now < start_at OR issuer_now >= end_at THEN
    RAISE EXCEPTION 'quota policy is not a current valid period' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO issued FROM control.quota_windows
  WHERE tenant_id = p_tenant_id AND entitlement_key = p_entitlement_key AND window_start = start_at;
  IF FOUND THEN
    IF issued.window_end <> end_at THEN
      RAISE EXCEPTION 'issued period boundary conflict' USING ERRCODE = '23514';
    END IF;
    RETURN issued; -- A changed snapshot never rewrites an issued hard limit.
  END IF;
  IF EXISTS (SELECT 1 FROM control.quota_windows
             WHERE tenant_id = p_tenant_id AND entitlement_key = p_entitlement_key
               AND window_start < end_at AND window_end > start_at) THEN
    RAISE EXCEPTION 'overlapping quota period' USING ERRCODE = '23514';
  END IF;
  INSERT INTO control.quota_windows
    (tenant_id, entitlement_key, window_start, window_end, hard_limit, issued_snapshot_at)
  VALUES (p_tenant_id, p_entitlement_key, start_at, end_at, quota_limit, snapshot_at)
  RETURNING * INTO issued;
  RETURN issued;
END;
$$;
ALTER FUNCTION control.issue_quota_window(uuid, text) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.issue_quota_window(uuid, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.issue_quota_window(uuid, text) TO role_maintenance;

CREATE TABLE control.usage_reservations (
  reservation_id uuid PRIMARY KEY DEFAULT uuidv7(),
  request_id uuid NOT NULL,
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  principal_id uuid NOT NULL CHECK (principal_id <> '00000000-0000-0000-0000-000000000000'::uuid),
  entitlement_key text NOT NULL,
  window_start timestamptz NOT NULL,
  operation text NOT NULL CHECK (operation ~ '^[a-z][a-z0-9_.]{0,95}$'),
  request_fingerprint text NOT NULL CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
  units bigint NOT NULL CHECK (units > 0),
  status text NOT NULL DEFAULT 'RESERVED' CHECK (status IN ('RESERVED', 'CONSUMED', 'RELEASED')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  expires_at timestamptz NOT NULL,
  finished_at timestamptz,
  UNIQUE (tenant_id, request_id),
  FOREIGN KEY (tenant_id, entitlement_key, window_start)
    REFERENCES control.quota_windows(tenant_id, entitlement_key, window_start),
  CHECK (request_id <> '00000000-0000-0000-0000-000000000000'::uuid),
  CHECK (isfinite(created_at) AND isfinite(expires_at) AND expires_at > created_at),
  CHECK ((status = 'RESERVED' AND finished_at IS NULL)
      OR (status IN ('CONSUMED', 'RELEASED') AND finished_at IS NOT NULL AND isfinite(finished_at)))
);
ALTER TABLE control.usage_reservations OWNER TO role_migration_owner;
ALTER TABLE control.usage_reservations ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.usage_reservations FORCE ROW LEVEL SECURITY;
CREATE POLICY usage_reservations_tenant ON control.usage_reservations
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);
REVOKE ALL ON control.usage_reservations FROM PUBLIC, role_gateway, role_private_worker,
  role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer,
  role_maintenance, role_admin;
GRANT SELECT, INSERT ON control.usage_reservations TO role_gateway;
GRANT UPDATE(status, finished_at) ON control.usage_reservations TO role_gateway;
GRANT SELECT ON control.usage_reservations TO role_maintenance;
CREATE INDEX usage_reservations_expiry ON control.usage_reservations(tenant_id, expires_at)
WHERE status = 'RESERVED';

CREATE FUNCTION control.usage_reservation_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, control AS $$
BEGIN
  IF ROW(NEW.reservation_id, NEW.request_id, NEW.tenant_id, NEW.principal_id,
         NEW.entitlement_key, NEW.window_start, NEW.operation, NEW.request_fingerprint,
         NEW.units, NEW.created_at, NEW.expires_at)
     IS DISTINCT FROM
     ROW(OLD.reservation_id, OLD.request_id, OLD.tenant_id, OLD.principal_id,
         OLD.entitlement_key, OLD.window_start, OLD.operation, OLD.request_fingerprint,
         OLD.units, OLD.created_at, OLD.expires_at)
     OR (OLD.status <> 'RESERVED' AND ROW(NEW.status, NEW.finished_at)
                                    IS DISTINCT FROM ROW(OLD.status, OLD.finished_at)) THEN
    RAISE EXCEPTION 'invalid usage reservation transition' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;
ALTER FUNCTION control.usage_reservation_transition() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.usage_reservation_transition() FROM PUBLIC;
CREATE TRIGGER usage_reservation_transition BEFORE UPDATE ON control.usage_reservations
FOR EACH ROW EXECUTE FUNCTION control.usage_reservation_transition();

CREATE FUNCTION control.reap_quota_reservations(p_tenant_id uuid, p_limit integer)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, control AS $$
DECLARE expired control.usage_reservations; reaped bigint := 0;
BEGIN
  IF p_limit IS NULL OR p_limit <= 0 OR p_tenant_id IS NULL
     OR p_tenant_id IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'invalid quota reaper scope' USING ERRCODE = '42501';
  END IF;
  FOR expired IN SELECT * FROM control.usage_reservations
    WHERE tenant_id = p_tenant_id AND status = 'RESERVED' AND expires_at <= clock_timestamp()
    ORDER BY expires_at, reservation_id LIMIT p_limit FOR UPDATE SKIP LOCKED
  LOOP
    UPDATE control.quota_windows SET reserved = reserved - expired.units
    WHERE tenant_id = expired.tenant_id AND entitlement_key = expired.entitlement_key
      AND window_start = expired.window_start;
    UPDATE control.usage_reservations SET status = 'RELEASED', finished_at = clock_timestamp()
    WHERE reservation_id = expired.reservation_id AND status = 'RESERVED';
    reaped := reaped + 1;
  END LOOP;
  RETURN reaped;
END;
$$;
ALTER FUNCTION control.reap_quota_reservations(uuid, integer) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.reap_quota_reservations(uuid, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.reap_quota_reservations(uuid, integer) TO role_maintenance;

CREATE TABLE control.rate_buckets (
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  subject_kind text NOT NULL CHECK (subject_kind IN ('ip', 'credential', 'user', 'tenant')),
  subject_id text NOT NULL CHECK (length(subject_id) BETWEEN 1 AND 128),
  operation text NOT NULL CHECK (operation ~ '^[a-z][a-z0-9_.]{0,95}$'),
  bucket_key text NOT NULL CHECK (bucket_key ~ '^[a-z][a-z0-9_.]{0,95}$'),
  capacity bigint NOT NULL CHECK (capacity > 0),
  tokens numeric NOT NULL CHECK (tokens >= 0 AND tokens <= capacity),
  refill_per_second bigint NOT NULL CHECK (refill_per_second > 0),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(updated_at)),
  version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
  PRIMARY KEY (tenant_id, subject_kind, subject_id, operation, bucket_key)
);
ALTER TABLE control.rate_buckets OWNER TO role_migration_owner;
ALTER TABLE control.rate_buckets ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.rate_buckets FORCE ROW LEVEL SECURITY;
CREATE POLICY rate_buckets_tenant ON control.rate_buckets
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);
REVOKE ALL ON control.rate_buckets FROM PUBLIC, role_gateway, role_private_worker,
  role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer,
  role_maintenance, role_admin;
GRANT SELECT, INSERT ON control.rate_buckets TO role_gateway;
GRANT UPDATE(capacity, tokens, refill_per_second, updated_at, version) ON control.rate_buckets TO role_gateway;
GRANT SELECT ON control.rate_buckets TO role_maintenance;
