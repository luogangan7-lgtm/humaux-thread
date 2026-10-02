-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0192). ADR-0058 D-L (card 32, slice 2):
-- the private worker moves off the v1 claim, and the Evidence v1 stranded is re-opened.
-- Spec: §31/§61 durable jobs, §67.2 private reasoning in-flight 4, §11 WAITING_KEY, §15.7.
--
-- Deploy order: stop every private-worker -> migrate -> start the slice-2 binary. No v1 private
-- claimer may run across this migration (the legacy reset below assumes it).
--
-- 1. role_private_worker loses EXECUTE on ops.claim_derived_work; the v1 function itself refuses
--    DERIVED_DISTILL for every caller (role_consolidation_worker keeps it for DERIVED_CONSOLIDATE),
--    so a distill job can only be claimed through the four slots of ops.claim_derived_work_v2.
-- 2. ops.claim_derived_work_v2's T6 re-queue (EXECUTION_UNCERTAIN past hard_deadline) backs off on
--    the same capped schedule as every other retry, with jitter (main-line ruling on E1, guard b).
--    Signature, owner, SECURITY DEFINER, search_path and ACL are kept by CREATE OR REPLACE.
-- 3. The `adr0058-cutover` block (run verbatim by crates/adapters/tests/distill_dispatch_v2.rs T19):
--    v1 left Evidence whose EVIDENCE_ACCEPTED outbox row is open while its job is DEAD (v1 never
--    flipped the outbox on DEAD, and its attempts counted claims) or missing (rows that predate
--    0164's trigger). v1's tenant-batch outbox claim was the only door to those rows and slice 2
--    deletes it, so every such row gets an open job here, with a full budget of real calls.

-- ---------------------------------------------------------------------------------------------
-- 1. The v1 claim: consolidation only.
--    Caller: adapters::jobs::claim_derived_work_consolidation (role_consolidation_worker).
--    Fence: unchanged from 0164 plus the DERIVED_DISTILL refusal. Owner: role_migration_owner.
-- ---------------------------------------------------------------------------------------------
REVOKE EXECUTE ON FUNCTION ops.claim_derived_work(text[], text, double precision, bigint)
FROM role_private_worker;

CREATE OR REPLACE FUNCTION ops.claim_derived_work(
  p_job_types text[], p_lease_owner text, p_lease_seconds double precision, p_limit bigint
) RETURNS SETOF ops.jobs
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  IF p_limit <= 0 OR p_lease_seconds <= 0 OR p_lease_owner IS NULL
     OR length(btrim(p_lease_owner)) = 0 THEN
    RAISE EXCEPTION 'invalid derived work claim' USING ERRCODE = '22023';
  END IF;
  -- Closed set (§78.2), pinned against humaux_adapters::jobs::DerivedJobType by contract test.
  IF p_job_types IS NULL OR array_length(p_job_types, 1) IS NULL
     OR NOT (p_job_types <@ ARRAY['DERIVED_DISTILL', 'DERIVED_CONSOLIDATE']) THEN
    RAISE EXCEPTION 'unknown derived job type' USING ERRCODE = '23514';
  END IF;
  -- ADR-0058 D-L: DERIVED_DISTILL has exactly one claim door (ops.claim_derived_work_v2, slots +
  -- tenant rotation). Refusing the type here closes both arms below (`job_type = ANY (p_job_types)`).
  IF 'DERIVED_DISTILL' = ANY (p_job_types) THEN
    RAISE EXCEPTION 'DERIVED_DISTILL is claimed only by ops.claim_derived_work_v2 (ADR-0058)'
      USING ERRCODE = '23514';
  END IF;
  RETURN QUERY
  WITH picked AS (
    SELECT j.job_id
    FROM ops.jobs j
    WHERE j.job_type = ANY (p_job_types)
      AND ( (j.status IN ('PENDING', 'RETRY_WAIT') AND j.next_retry_at <= clock_timestamp())
            OR (j.status = 'PROCESSING' AND j.lease_expires_at < clock_timestamp()) )
    ORDER BY j.priority DESC, j.next_retry_at, j.created_at
    FOR UPDATE SKIP LOCKED
    LIMIT p_limit
  )
  UPDATE ops.jobs j
  SET status = 'PROCESSING',
      lease_owner = p_lease_owner,
      lease_expires_at = clock_timestamp() + make_interval(secs => p_lease_seconds),
      attempt = j.attempt + 1
  FROM picked
  WHERE j.job_id = picked.job_id
    -- The SAME eligibility predicate as `picked`, re-stated on the UPDATE itself. `SKIP LOCKED`
    -- alone makes exactly-once a property of lock TIMING: joined on `job_id` only, PostgreSQL's
    -- EvalPlanQual re-check on a lock conflict re-applies this UPDATE to the row version the
    -- other claimer just wrote, and a job already PROCESSING under a live lease would be claimed
    -- twice. Restating the predicate makes the re-check reject it, so exactly-once holds even
    -- when two claims land inside one lock window (fault injection: deleting `FOR UPDATE SKIP
    -- LOCKED` must not let `two_serialized_claims_take_each_job_exactly_once_under_a_held_lock`
    -- return the same job twice).
    AND ( (j.status IN ('PENDING', 'RETRY_WAIT') AND j.next_retry_at <= clock_timestamp())
          OR (j.status = 'PROCESSING' AND j.lease_expires_at < clock_timestamp()) )
  RETURNING j.*;
END;
$$;

COMMENT ON FUNCTION ops.claim_derived_work(text[], text, double precision, bigint) IS
  'ADR-0036 (card 14), narrowed by ADR-0058 (card 32): cross-tenant DERIVED_CONSOLIDATE claim. SECURITY DEFINER owned by role_migration_owner; one atomic SKIP LOCKED statement; returns only the rows it just claimed; refuses DERIVED_DISTILL (claimed only by ops.claim_derived_work_v2). EXECUTE: role_consolidation_worker only.';

-- ---------------------------------------------------------------------------------------------
-- 2. claim_derived_work_v2, T6 backoff with jitter (everything else exactly as 0190).
--    Caller: adapters::jobs::claim_distill (role_private_worker). Fence and owner: as 0190 §4.
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
-- 3. Data repair. Runs as the migration superuser (RLS does not apply); every statement touches
--    DERIVED_DISTILL rows and their EVIDENCE_ACCEPTED outbox rows only.
-- ---------------------------------------------------------------------------------------------
-- BEGIN adr0058-cutover
-- T8: v1-claimed distill rows (PROCESSING with no dispatch_state) are READY again. Safe only because
-- no v1 private claimer runs across this migration (deploy order above).
UPDATE ops.jobs
SET status = 'PENDING', lease_owner = NULL, lease_expires_at = NULL,
    next_retry_at = clock_timestamp()
WHERE job_type = 'DERIVED_DISTILL' AND status = 'PROCESSING' AND dispatch_state IS NULL;

-- T10 re-arm: a DEAD distill job whose outbox row is still open. §11: a tenant with no route binding
-- is known-blocked and enters WAITING_KEY (re-checked by the claim, never DEAD, no attempt spent).
UPDATE ops.jobs j
SET status = CASE WHEN EXISTS (SELECT 1 FROM control.reasoning_route_bindings b
                               WHERE b.tenant_id = j.tenant_id)
                  THEN 'PENDING' ELSE 'WAITING_KEY' END,
    lease_owner = NULL, lease_expires_at = NULL, next_retry_at = clock_timestamp()
WHERE j.job_type = 'DERIVED_DISTILL' AND j.status = 'DEAD'
  AND EXISTS (SELECT 1 FROM ops.outbox o
              WHERE o.tenant_id = j.tenant_id AND o.event_type = 'EVIDENCE_ACCEPTED'
                AND o.status IN ('PENDING', 'PROCESSING')
                AND o.evidence_id::text = j.payload ->> 'evidence_id');

-- T10 backfill: one job per open EVIDENCE_ACCEPTED row that has none, in 0164's key and payload
-- shape (the same row ops.enqueue_derived_distill_work would have written).
INSERT INTO ops.jobs (tenant_id, job_type, status, next_retry_at, idempotency_key, payload)
SELECT o.tenant_id, 'DERIVED_DISTILL',
       CASE WHEN EXISTS (SELECT 1 FROM control.reasoning_route_bindings b
                         WHERE b.tenant_id = o.tenant_id)
            THEN 'PENDING' ELSE 'WAITING_KEY' END,
       clock_timestamp(),
       'derived-work:DERIVED_DISTILL:' || o.evidence_id::text,
       jsonb_build_object('schema_version', 1,
                          'reasoning_domain_id', e.reasoning_domain_id::text,
                          'evidence_id', o.evidence_id::text)
FROM ops.outbox o
JOIN private.evidence_objects e ON e.evidence_id = o.evidence_id AND e.tenant_id = o.tenant_id
WHERE o.event_type = 'EVIDENCE_ACCEPTED' AND o.status IN ('PENDING', 'PROCESSING')
  AND e.reasoning_domain_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM ops.jobs j
                  WHERE j.idempotency_key = 'derived-work:DERIVED_DISTILL:' || o.evidence_id::text)
ON CONFLICT (idempotency_key) DO NOTHING;

-- ADR-0058 D-F: v1's attempt counted claims (dev rows reach 31). Every open distill job starts v2
-- with a full budget of real provider calls.
UPDATE ops.jobs
SET attempt = 0, abandoned_claims = 0, not_ready_since = NULL, last_error_class = NULL
WHERE job_type = 'DERIVED_DISTILL'
  AND status IN ('PENDING', 'RETRY_WAIT', 'WAITING_KEY', 'PROCESSING')
  AND (attempt <> 0 OR abandoned_claims <> 0 OR not_ready_since IS NOT NULL
       OR last_error_class IS NOT NULL);

-- The re-arm is an UPDATE (the admit trigger fires on INSERT only): admit every tenant with open work.
INSERT INTO ops.distill_tenant_scheduler (tenant_id)
SELECT DISTINCT j.tenant_id FROM ops.jobs j
WHERE j.job_type = 'DERIVED_DISTILL'
  AND j.status IN ('PENDING', 'RETRY_WAIT', 'WAITING_KEY', 'PROCESSING')
ON CONFLICT DO NOTHING;
-- END adr0058-cutover

