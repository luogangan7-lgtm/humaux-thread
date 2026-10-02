-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0189). ADR-0058 (card 32): distill
-- dispatch v2 — §31/§61 durable jobs, §32 tenant-fair scheduling, §67.2 private reasoning in-flight
-- 4, §11 WAITING_KEY, research ruling R-32.
--
-- What v1 (0164) got wrong for DERIVED_DISTILL: one global-FIFO claim of a batch (one tenant's
-- backlog delays every other tenant; a never-ready tenant heads every pass), `attempt + 1` at claim
-- (attempts count claims, not provider requests), and a lease that is the only fence (a slow call
-- expires its siblings' leases and nothing bounds the provider requests in flight).
--
-- Shape (ADR-0058 D-A/D-B/D-E/D-G):
--   * ops.jobs stays the only job table; `status` keeps the seven frozen values. Call states live
--     in the new `dispatch_state` (CLAIMED | DISPATCH_INTENT | EXECUTION_UNCERTAIN, only while
--     PROCESSING); `claim_generation` is the fencing token; `hard_deadline` bounds one claim.
--   * ops.provider_arbiters (one row) serializes claims and numbers them (`next_turn`);
--     ops.provider_slots holds exactly four rows (§67.2) — a provider request may be sent only
--     while its (job, generation) holds a slot; ops.distill_tenant_scheduler gives each tenant
--     the turn it was last served (least-recently-served tenant first, FIFO inside a tenant);
--     ops.distill_calls is the attempt ledger, one row per admitted provider request.
--   * Four owner SECURITY DEFINER doors, EXECUTE role_private_worker only:
--     claim_derived_work_v2 (T1 + the slot sweep T4/T5/T6/T9), begin_call (T2), renew_lease (T7),
--     finish_derived_work_v2 (T3). One owner trigger admits a tenant's scheduler row on enqueue.
--
-- Function owner role_migration_owner (ADR-0058 D-I): `current_user` inside a definer is the owner
-- and passes the owner arm of jobs_tenant_isolation (0164) and of the two new tenant policies; the
-- definers therefore filter by tenant / job explicitly and touch only job_type = 'DERIVED_DISTILL'.
-- search_path is pinned to pg_catalog and every name is schema-qualified.
--
-- Nothing in this migration changes how v1 serves distill: no worker calls the new doors until the
-- slice-2 binary ships with 0193 (ADR-0058 D-L).

-- ---------------------------------------------------------------------------------------------
-- 1. ops.jobs columns (metadata-only: constant defaults are fast defaults; the CHECK is NOT VALID
--    and validated by 0192 without blocking DML).
-- ---------------------------------------------------------------------------------------------
ALTER TABLE ops.jobs
  ADD COLUMN claim_generation integer NOT NULL DEFAULT 0,
  ADD COLUMN hard_deadline timestamptz,
  ADD COLUMN dispatch_state text,
  ADD COLUMN dispatch_model_call_id uuid,
  ADD COLUMN not_ready_since timestamptz,
  ADD COLUMN abandoned_claims integer NOT NULL DEFAULT 0,
  -- ADR-0058 D-A closed set (§78.2), pinned against adapters::jobs::DispatchState by contract test.
  ADD CONSTRAINT jobs_dispatch_state_check CHECK (
    dispatch_state IS NULL
    OR (status = 'PROCESSING' AND job_type = 'DERIVED_DISTILL'
        AND dispatch_state IN ('CLAIMED', 'DISPATCH_INTENT', 'EXECUTION_UNCERTAIN'))
  ) NOT VALID;

COMMENT ON COLUMN ops.jobs.claim_generation IS
  'ADR-0058 D-E: fencing token of a DERIVED_DISTILL claim; +1 per claim, never at begin_call.';
COMMENT ON COLUMN ops.jobs.dispatch_state IS
  'ADR-0058 D-A: CLAIMED | DISPATCH_INTENT | EXECUTION_UNCERTAIN while PROCESSING (distill only); NULL otherwise.';
COMMENT ON COLUMN ops.jobs.hard_deadline IS
  'ADR-0058 D-G: end of the current claim; the lease never passes it and only it frees a dispatched slot.';

-- ---------------------------------------------------------------------------------------------
-- 2. Scheduling tables. Owner-only: no runtime role holds any privilege on them (§6.2.2 `—`
--    cells); the four doors below are the only way in.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE ops.provider_arbiters (
  budget    text PRIMARY KEY CHECK (budget = 'PRIVATE_REASONING'),
  next_turn bigint NOT NULL CHECK (next_turn > 0)
);
COMMENT ON TABLE ops.provider_arbiters IS
  'ADR-0058 D-B: one row per provider budget; locked FOR UPDATE by every distill claim (claims are serialized and numbered by next_turn).';

-- §67.2: exactly four provider slots; a fifth needs a migration.
CREATE TABLE ops.provider_slots (
  slot_no          smallint PRIMARY KEY CHECK (slot_no BETWEEN 1 AND 4),
  job_id           uuid UNIQUE,
  claim_generation integer,
  bound_until      timestamptz,
  CHECK ((job_id IS NULL) = (claim_generation IS NULL)
         AND (job_id IS NULL) = (bound_until IS NULL))
);
COMMENT ON TABLE ops.provider_slots IS
  'ADR-0058 D-B/§67.2: the four private-reasoning slots. A provider request for (job, generation) may be sent only while a slot is bound to that pair. job_id has no FK on purpose: a vanished job keeps its slot until bound_until (T9).';

CREATE TABLE ops.distill_tenant_scheduler (
  tenant_id        uuid PRIMARY KEY REFERENCES control.tenants (tenant_id) ON DELETE CASCADE,
  last_served_turn bigint NOT NULL DEFAULT 0
);
COMMENT ON TABLE ops.distill_tenant_scheduler IS
  'ADR-0058 D-B: the turn at which each tenant was last served a distill claim; the claim serves the least-recently-served tenant with READY work.';

CREATE TABLE ops.distill_calls (
  model_call_id    uuid PRIMARY KEY,
  tenant_id        uuid NOT NULL,
  job_id           uuid NOT NULL,
  claim_generation integer NOT NULL,
  attempt          integer NOT NULL CHECK (attempt > 0),
  begun_at         timestamptz NOT NULL,
  FOREIGN KEY (tenant_id, job_id) REFERENCES ops.jobs (tenant_id, job_id) ON DELETE CASCADE
);
CREATE INDEX distill_calls_job_idx ON ops.distill_calls (job_id, claim_generation);
COMMENT ON TABLE ops.distill_calls IS
  'ADR-0058 D-E: attempt ledger — one row per provider request admitted by ops.begin_call, naming its job, generation and attempt (the ledger row is joined by model_call_id).';

ALTER TABLE ops.distill_tenant_scheduler ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.distill_tenant_scheduler FORCE ROW LEVEL SECURITY;
CREATE POLICY distill_tenant_scheduler_tenant_isolation ON ops.distill_tenant_scheduler
  USING (
    current_user = 'role_migration_owner'
    OR tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid
  )
  WITH CHECK (
    current_user = 'role_migration_owner'
    OR tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid
  );

ALTER TABLE ops.distill_calls ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.distill_calls FORCE ROW LEVEL SECURITY;
CREATE POLICY distill_calls_tenant_isolation ON ops.distill_calls
  USING (
    current_user = 'role_migration_owner'
    OR tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid
  )
  WITH CHECK (
    current_user = 'role_migration_owner'
    OR tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid
  );

-- Owner + NAMED §6.2.2 cells (0161 recipe): re-own, REVOKE everything the ops-domain default
-- granted; nothing is granted back.
ALTER TABLE ops.provider_arbiters OWNER TO role_migration_owner;
ALTER TABLE ops.provider_slots OWNER TO role_migration_owner;
ALTER TABLE ops.distill_tenant_scheduler OWNER TO role_migration_owner;
ALTER TABLE ops.distill_calls OWNER TO role_migration_owner;
REVOKE ALL ON ops.provider_arbiters, ops.provider_slots, ops.distill_tenant_scheduler,
  ops.distill_calls
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;

INSERT INTO ops.provider_arbiters (budget, next_turn) VALUES ('PRIVATE_REASONING', 1);
INSERT INTO ops.provider_slots (slot_no) SELECT generate_series(1, 4);

-- Every tenant that already has open distill work gets its scheduler row (later tenants are
-- admitted by the trigger below).
INSERT INTO ops.distill_tenant_scheduler (tenant_id)
SELECT DISTINCT j.tenant_id FROM ops.jobs j
WHERE j.job_type = 'DERIVED_DISTILL'
  AND j.status IN ('PENDING', 'RETRY_WAIT', 'WAITING_KEY', 'PROCESSING')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------------------------
-- 3. Scheduler admission on enqueue.
--    Caller: the AFTER INSERT trigger derived_distill_scheduler_admit on ops.jobs (fired by the
--    0164 invoker enqueue trigger under role_gateway / role_private_worker sessions).
--    Fence: ON CONFLICT DO NOTHING (an admitted tenant keeps its turn). Owner: role_migration_owner
--    (definer, so no runtime role needs a grant on the scheduler table).
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION ops.distill_scheduler_admit()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  INSERT INTO ops.distill_tenant_scheduler (tenant_id) VALUES (NEW.tenant_id)
  ON CONFLICT DO NOTHING;
  RETURN NULL;
END;
$$;
ALTER FUNCTION ops.distill_scheduler_admit() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.distill_scheduler_admit()
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker,
  role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;

CREATE TRIGGER derived_distill_scheduler_admit
AFTER INSERT ON ops.jobs
FOR EACH ROW WHEN (NEW.job_type = 'DERIVED_DISTILL')
EXECUTE FUNCTION ops.distill_scheduler_admit();

-- ---------------------------------------------------------------------------------------------
-- 4. The claim (T1) with its slot sweep (T4/T5/T6/T9).
--    Caller: adapters::jobs::claim_distill (role_private_worker), one call per free seat.
--    Fence: arbiter row FOR UPDATE (claims serialized, turns gap-free); free slot FOR UPDATE SKIP
--    LOCKED + `job_id IS NULL` on bind; tenant FOR UPDATE OF t SKIP LOCKED; job FOR UPDATE SKIP
--    LOCKED with the eligibility restated on the UPDATE (EvalPlanQual, as 0164).
--    Owner: role_migration_owner. Returns 0 or 1 ops.jobs row (PROCESSING / CLAIMED). No attempt
--    change here: an attempt is a provider request and is counted by ops.begin_call.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION ops.claim_derived_work_v2(
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
            next_retry_at = clock_timestamp() + make_interval(secs => p_lease_seconds),
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
  ORDER BY t.last_served_turn, t.tenant_id
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
-- 5. begin_call (T2): admits ONE provider request.
--    Caller: adapters::jobs::begin_distill_call (role_private_worker), right before the HTTP call.
--    Fence: lease owner + claim_generation + PROCESSING + state CLAIMED|DISPATCH_INTENT + live
--    lease + a slot bound to (job, generation) + at least p_min_remaining_seconds before
--    hard_deadline. Owner: role_migration_owner. Returns the new attempt; NULL = refused (no HTTP
--    may follow). The only writer of ops.distill_calls.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION ops.begin_call(
  p_job_id uuid, p_tenant_id uuid, p_lease_owner text, p_claim_generation integer,
  p_model_call_id uuid, p_min_remaining_seconds double precision
) RETURNS integer
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_attempt integer;
BEGIN
  IF p_model_call_id IS NULL OR p_min_remaining_seconds IS NULL OR p_min_remaining_seconds < 0 THEN
    RAISE EXCEPTION 'invalid begin_call' USING ERRCODE = '22023';
  END IF;
  UPDATE ops.jobs j
  SET dispatch_state = 'DISPATCH_INTENT',
      attempt = j.attempt + 1,
      dispatch_model_call_id = p_model_call_id,
      not_ready_since = NULL
  WHERE j.job_id = p_job_id AND j.tenant_id = p_tenant_id
    AND j.job_type = 'DERIVED_DISTILL'
    AND j.lease_owner = p_lease_owner
    AND j.claim_generation = p_claim_generation
    AND j.status = 'PROCESSING'
    AND j.dispatch_state IN ('CLAIMED', 'DISPATCH_INTENT')
    AND j.lease_expires_at > clock_timestamp()
    AND j.hard_deadline - clock_timestamp() >= make_interval(secs => p_min_remaining_seconds)
    AND EXISTS (SELECT 1 FROM ops.provider_slots s
                WHERE s.job_id = p_job_id AND s.claim_generation = p_claim_generation)
  RETURNING j.attempt INTO v_attempt;
  IF NOT FOUND THEN
    RETURN NULL;
  END IF;
  INSERT INTO ops.distill_calls (model_call_id, tenant_id, job_id, claim_generation, attempt, begun_at)
  VALUES (p_model_call_id, p_tenant_id, p_job_id, p_claim_generation, v_attempt, clock_timestamp());
  RETURN v_attempt;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 6. renew_lease (T7): the heartbeat.
--    Caller: adapters::jobs::renew_distill_lease (role_private_worker), every lease/3.
--    Fence: lease owner + claim_generation + PROCESSING + state CLAIMED|DISPATCH_INTENT + live
--    lease + hard_deadline not passed; the new lease never passes hard_deadline.
--    Owner: role_migration_owner. Returns the new lease end; NULL = lease lost.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION ops.renew_lease(
  p_job_id uuid, p_tenant_id uuid, p_lease_owner text, p_claim_generation integer,
  p_lease_seconds double precision
) RETURNS timestamptz
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_until timestamptz;
BEGIN
  IF p_lease_seconds IS NULL OR p_lease_seconds <= 0 THEN
    RAISE EXCEPTION 'invalid renew_lease' USING ERRCODE = '22023';
  END IF;
  UPDATE ops.jobs j
  SET lease_expires_at = LEAST(clock_timestamp() + make_interval(secs => p_lease_seconds),
                               j.hard_deadline)
  WHERE j.job_id = p_job_id AND j.tenant_id = p_tenant_id
    AND j.job_type = 'DERIVED_DISTILL'
    AND j.lease_owner = p_lease_owner
    AND j.claim_generation = p_claim_generation
    AND j.status = 'PROCESSING'
    AND j.dispatch_state IN ('CLAIMED', 'DISPATCH_INTENT')
    AND j.lease_expires_at > clock_timestamp()
    AND j.hard_deadline > clock_timestamp()
  RETURNING j.lease_expires_at INTO v_until;
  RETURN v_until;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 7. finish_derived_work_v2 (T3): settles a claim and frees its slot.
--    Caller: adapters::jobs::finish_distill_in_txn (role_private_worker), first statement of the
--    worker's tenant-scoped settle transaction (rolled back by the caller on false).
--    Fence: lease owner + claim_generation + PROCESSING — ADR-0036 D4: no lease predicate, a
--    same-generation late result (including from EXECUTION_UNCERTAIN) is still the truth.
--    Owner: role_migration_owner.
--    Outcomes (closed set, pinned against adapters::jobs::DistillFinish by contract test):
--      DONE        -> DONE (lease_owner kept for audit, as v1)
--      RETRY       -> PENDING, next_retry_at = now + p_backoff_seconds, class
--      NOT_READY   -> PENDING + backoff, or WAITING_KEY + park once not ready for p_park_seconds
--      WAITING_KEY -> WAITING_KEY + park; §11: reverts the attempt its own call counted
--      DEAD        -> DEAD, class
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION ops.finish_derived_work_v2(
  p_job_id uuid, p_tenant_id uuid, p_lease_owner text, p_claim_generation integer,
  p_outcome text, p_error_class text, p_backoff_seconds double precision,
  p_park_seconds double precision
) RETURNS boolean
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_now timestamptz := clock_timestamp();
BEGIN
  IF p_outcome IS NULL
     OR NOT (p_outcome = ANY (ARRAY['DONE', 'RETRY', 'NOT_READY', 'WAITING_KEY', 'DEAD'])) THEN
    RAISE EXCEPTION 'unknown distill finish outcome %', p_outcome USING ERRCODE = '23514';
  END IF;
  IF p_backoff_seconds IS NULL OR p_backoff_seconds < 0
     OR p_park_seconds IS NULL OR p_park_seconds <= 0 THEN
    RAISE EXCEPTION 'invalid distill finish timing' USING ERRCODE = '22023';
  END IF;

  UPDATE ops.jobs j
  SET status = CASE p_outcome
                 WHEN 'DONE' THEN 'DONE'
                 WHEN 'DEAD' THEN 'DEAD'
                 WHEN 'RETRY' THEN 'PENDING'
                 WHEN 'WAITING_KEY' THEN 'WAITING_KEY'
                 ELSE CASE WHEN v_now - coalesce(j.not_ready_since, v_now)
                                  >= make_interval(secs => p_park_seconds)
                           THEN 'WAITING_KEY' ELSE 'PENDING' END
               END,
      next_retry_at = CASE p_outcome
                        WHEN 'RETRY' THEN v_now + make_interval(secs => p_backoff_seconds)
                        WHEN 'WAITING_KEY' THEN v_now + make_interval(secs => p_park_seconds)
                        WHEN 'NOT_READY' THEN
                          CASE WHEN v_now - coalesce(j.not_ready_since, v_now)
                                      >= make_interval(secs => p_park_seconds)
                               THEN v_now + make_interval(secs => p_park_seconds)
                               ELSE v_now + make_interval(secs => p_backoff_seconds) END
                        ELSE j.next_retry_at
                      END,
      not_ready_since = CASE WHEN p_outcome = 'NOT_READY'
                             THEN coalesce(j.not_ready_since, v_now)
                             ELSE j.not_ready_since END,
      -- §11: WAITING_KEY = known blocked, never DEAD, no retry count.
      attempt = CASE WHEN p_outcome = 'WAITING_KEY' THEN GREATEST(j.attempt - 1, 0)
                     ELSE j.attempt END,
      last_error_class = CASE WHEN p_outcome = 'DONE' THEN j.last_error_class
                              ELSE p_error_class END,
      dispatch_state = NULL,
      lease_owner = CASE WHEN p_outcome = 'DONE' THEN j.lease_owner ELSE NULL END,
      lease_expires_at = NULL
  WHERE j.job_id = p_job_id AND j.tenant_id = p_tenant_id
    AND j.job_type = 'DERIVED_DISTILL'
    AND j.lease_owner = p_lease_owner
    AND j.claim_generation = p_claim_generation
    AND j.status = 'PROCESSING';
  IF NOT FOUND THEN
    RETURN false;
  END IF;

  UPDATE ops.provider_slots
  SET job_id = NULL, claim_generation = NULL, bound_until = NULL
  WHERE job_id = p_job_id AND claim_generation = p_claim_generation;
  RETURN true;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 8. Ownership and EXECUTE: role_private_worker only, PUBLIC revoked.
-- ---------------------------------------------------------------------------------------------
ALTER FUNCTION ops.claim_derived_work_v2(text, double precision, double precision)
  OWNER TO role_migration_owner;
ALTER FUNCTION ops.begin_call(uuid, uuid, text, integer, uuid, double precision)
  OWNER TO role_migration_owner;
ALTER FUNCTION ops.renew_lease(uuid, uuid, text, integer, double precision)
  OWNER TO role_migration_owner;
ALTER FUNCTION ops.finish_derived_work_v2(uuid, uuid, text, integer, text, text, double precision, double precision)
  OWNER TO role_migration_owner;

REVOKE ALL ON FUNCTION
  ops.claim_derived_work_v2(text, double precision, double precision),
  ops.begin_call(uuid, uuid, text, integer, uuid, double precision),
  ops.renew_lease(uuid, uuid, text, integer, double precision),
  ops.finish_derived_work_v2(uuid, uuid, text, integer, text, text, double precision, double precision)
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker,
  role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT EXECUTE ON FUNCTION
  ops.claim_derived_work_v2(text, double precision, double precision),
  ops.begin_call(uuid, uuid, text, integer, uuid, double precision),
  ops.renew_lease(uuid, uuid, text, integer, double precision),
  ops.finish_derived_work_v2(uuid, uuid, text, integer, text, text, double precision, double precision)
TO role_private_worker;

COMMENT ON FUNCTION ops.claim_derived_work_v2(text, double precision, double precision) IS
  'ADR-0058 (card 32): tenant-fair DERIVED_DISTILL claim — arbiter -> 4 provider slots -> least-recently-served tenant -> tenant FIFO; one job per call; sweeps expired claims (T4/T5/T6/T9). Owner definer; EXECUTE role_private_worker only.';
COMMENT ON FUNCTION ops.begin_call(uuid, uuid, text, integer, uuid, double precision) IS
  'ADR-0058 (card 32): admits one provider request of a claimed distill job (attempt + 1, DISPATCH_INTENT, ops.distill_calls row); NULL = refused. Owner definer; EXECUTE role_private_worker only.';
COMMENT ON FUNCTION ops.renew_lease(uuid, uuid, text, integer, double precision) IS
  'ADR-0058 (card 32): generation-fenced distill heartbeat capped at hard_deadline; NULL = lease lost. Owner definer; EXECUTE role_private_worker only.';
COMMENT ON FUNCTION ops.finish_derived_work_v2(uuid, uuid, text, integer, text, text, double precision, double precision) IS
  'ADR-0058 (card 32): generation-fenced distill settle (DONE|RETRY|NOT_READY|WAITING_KEY|DEAD) that frees the claim slot. Owner definer; EXECUTE role_private_worker only.';
COMMENT ON FUNCTION ops.distill_scheduler_admit() IS
  'ADR-0058 (card 32): AFTER INSERT trigger on ops.jobs (DERIVED_DISTILL) admitting the tenant to ops.distill_tenant_scheduler. Owner definer; no EXECUTE grant.';
