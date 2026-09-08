-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0163). ADR-0036 (card 14):
-- cross-tenant pending-work discovery for the two derived-layer workers
-- (humaux-consolidation-worker, humaux-private-worker --distill-*).
--
-- Problem (bins/consolidation-worker/src/main.rs' own module doc): role_consolidation_worker /
-- role_private_worker set their RLS session context per call from a caller-supplied tenant_id,
-- so a "which tenants have pending work" query returns zero rows under any single session.
-- Both binaries therefore polled ONE env-pinned (tenant, reasoning_domain) pair and multi-tenant
-- serving (cards 10-13) had no derived layer behind it.
--
-- Shape (no new isolation mechanism, only a new caller of an existing one):
--   1. ops.jobs' 0012 blanket policy gains the SAME `current_user = 'role_migration_owner' OR`
--      arm migrations 0004 / 0012 / 0112 / 0147 / 0163 already use. Reproduced from the LIVE
--      0012 expression byte-for-byte (fetched with pg_get_expr before writing this file); only
--      the owner arm is new. NOT a second PERMISSIVE policy: 0012's own header explains why
--      multiple permissive policies OR together and silently widen access.
--   2. ONE SECURITY DEFINER function owned by role_migration_owner does the cross-tenant
--      SKIP LOCKED claim in ONE statement and returns only the rows it just claimed. EXECUTE
--      goes to the two derived-layer worker roles and nobody else; no runtime role gets a
--      broader table grant on ops.jobs than §6.2.2 already lists.
--      `current_user` inside a SECURITY DEFINER function is the function OWNER and is not
--      spoofable: no worker role holds membership in role_migration_owner, so none of them can
--      SET ROLE into it. A GUC arm (`current_setting('humaux.dispatch')`) was REJECTED —
--      any session can set a custom GUC itself, so it is advisory, not an authorization
--      boundary (ADR-0036 D2).
--   3. Everything AFTER the claim runs under normal RLS: the worker installs the claimed job's
--      tenant_id with the existing per-call SET LOCAL and every read/write goes through the
--      existing repos, so a tenant-A context still sees zero tenant-B rows.
--
-- Enqueue is at the events that create pending work (two AFTER INSERT triggers, INVOKER rights
-- — the writing session already holds the tenant context those rows satisfy, so this adds no
-- grant to any role: role_gateway and role_private_worker both already carry INSERT on ops.jobs
-- per §6.2.2).
--
-- No new table. No new LOGIN role (§6.2.0's 8 are frozen). No column added.

-- ---------------------------------------------------------------------------------------------
-- 1. ops.jobs tenant policy: add the owner arm, rest byte-identical to the live 0012 expression.
-- ---------------------------------------------------------------------------------------------
ALTER POLICY jobs_tenant_isolation ON ops.jobs
USING (
  current_user = 'role_migration_owner'
  OR tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid
)
WITH CHECK (
  current_user = 'role_migration_owner'
  OR tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid
);

-- ---------------------------------------------------------------------------------------------
-- 2. Tenant deletion cascades to its jobs.
--    ops.jobs rows are pure scheduling state derived from a tenant's own work; with the enqueue
--    triggers below every tenant that ever accepted an Evidence acquires them, and the 0008
--    RESTRICT default would turn "DELETE FROM control.tenants" (the throwaway-tenant teardown
--    every live-DB test in this workspace ends with) into an FK error. Deleting a tenant already
--    destroys the work these rows point at.
-- ---------------------------------------------------------------------------------------------
ALTER TABLE ops.jobs DROP CONSTRAINT jobs_tenant_id_fkey;
ALTER TABLE ops.jobs ADD CONSTRAINT jobs_tenant_id_fkey
  FOREIGN KEY (tenant_id) REFERENCES control.tenants(tenant_id) ON DELETE CASCADE;

-- ---------------------------------------------------------------------------------------------
-- 3. Enqueue at the events that create pending work.
-- ---------------------------------------------------------------------------------------------

-- Distill: remember's `EVIDENCE_ACCEPTED` ops.outbox row IS the work item
-- (adapters::distill_repo::claim_pending_evidence's claim predicate). The pass also needs the
-- Evidence's reasoning_domain_id (distill_repo::resolve_distill_binding / load_evidence), which
-- lives on private.evidence_objects — read here under the WRITER's own RLS context, the same
-- invoker-rights shape private.memory_affects_inherit_from_evidence (0156) already uses.
CREATE FUNCTION ops.enqueue_derived_distill_work()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE domain_id uuid;
BEGIN
  IF NEW.evidence_id IS NULL THEN
    RETURN NULL;
  END IF;
  SELECT e.reasoning_domain_id INTO domain_id
  FROM private.evidence_objects e
  WHERE e.evidence_id = NEW.evidence_id AND e.tenant_id = NEW.tenant_id;
  IF domain_id IS NULL THEN
    RETURN NULL;
  END IF;
  INSERT INTO ops.jobs (tenant_id, job_type, status, next_retry_at, idempotency_key, payload)
  VALUES (NEW.tenant_id, 'DERIVED_DISTILL', 'PENDING', clock_timestamp(),
          'derived-work:DERIVED_DISTILL:' || NEW.evidence_id::text,
          jsonb_build_object('schema_version', 1,
                             'reasoning_domain_id', domain_id::text,
                             'evidence_id', NEW.evidence_id::text))
  ON CONFLICT (idempotency_key) DO NOTHING;
  RETURN NULL;
END;
$$;

CREATE TRIGGER derived_distill_work_enqueue
AFTER INSERT ON ops.outbox
FOR EACH ROW WHEN (NEW.event_type = 'EVIDENCE_ACCEPTED')
EXECUTE FUNCTION ops.enqueue_derived_distill_work();

-- Consolidation: a new private.memory_records row is what makes a (tenant, reasoning_domain)
-- pair eligible for a rollup pass (consolidate_repo::select_and_materialize_inputs joins the
-- domain through memory_evidence -> evidence_objects, since memory_records carries no
-- reasoning_domain_id column). Fires on the PRIMARY link for exactly that reason.
-- ponytail: invoker-rights read — if the writing session cannot see the Evidence row (a
-- USER_PRIVATE Evidence written by a session acting as another user), no job is emitted and the
-- domain is picked up by the next memory in it. Widening this needs an owner arm on
-- evidence_objects_tenant_and_visibility, which is a §6.1.1 decision this card does not own.
CREATE FUNCTION ops.enqueue_derived_consolidate_work()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE tenant uuid; domain_id uuid;
BEGIN
  SELECT mr.tenant_id, e.reasoning_domain_id INTO tenant, domain_id
  FROM private.memory_records mr
  JOIN private.evidence_objects e
    ON e.evidence_id = NEW.evidence_id AND e.tenant_id = mr.tenant_id
  WHERE mr.memory_id = NEW.memory_id;
  IF tenant IS NULL OR domain_id IS NULL THEN
    RETURN NULL;
  END IF;
  INSERT INTO ops.jobs (tenant_id, job_type, status, next_retry_at, idempotency_key, payload)
  VALUES (tenant, 'DERIVED_CONSOLIDATE', 'PENDING', clock_timestamp(),
          'derived-work:DERIVED_CONSOLIDATE:' || NEW.memory_id::text,
          jsonb_build_object('schema_version', 1,
                             'reasoning_domain_id', domain_id::text,
                             'memory_id', NEW.memory_id::text))
  ON CONFLICT (idempotency_key) DO NOTHING;
  RETURN NULL;
END;
$$;

CREATE TRIGGER derived_consolidate_work_enqueue
AFTER INSERT ON private.memory_evidence
FOR EACH ROW WHEN (NEW.role = 'PRIMARY')
EXECUTE FUNCTION ops.enqueue_derived_consolidate_work();

-- ---------------------------------------------------------------------------------------------
-- 4. The cross-tenant claim. ONE statement, so two racing workers cannot both take one job.
--    The `PROCESSING AND lease_expires_at < clock_timestamp()` arm is what makes a killed
--    worker's job re-claimable (adapters::distill_repo::claim_pending_evidence uses the same
--    arm on ops.outbox). Deleting that arm is the fault injection ADR-0036 records: the
--    recovery test goes red and nothing else does.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION ops.claim_derived_work(
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

ALTER FUNCTION ops.claim_derived_work(text[], text, double precision, bigint)
  OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.claim_derived_work(text[], text, double precision, bigint)
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker,
  role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT EXECUTE ON FUNCTION ops.claim_derived_work(text[], text, double precision, bigint)
  TO role_consolidation_worker, role_private_worker;

COMMENT ON FUNCTION ops.claim_derived_work(text[], text, double precision, bigint) IS
  'ADR-0036 (card 14): the only cross-tenant read of ops.jobs. SECURITY DEFINER owned by role_migration_owner (the non-spoofable current_user arm added to jobs_tenant_isolation in this same migration); one atomic SKIP LOCKED statement; returns only the rows it just claimed. EXECUTE: role_consolidation_worker + role_private_worker only.';

-- ---------------------------------------------------------------------------------------------
-- 5. The consolidation worker resolves its own route binding per claimed tenant.
--    Bindings are per (tenant, reasoning_domain, purpose) — control.reasoning_route_bindings has
--    a tenant_id column — so an env-pinned binding id cannot survive cross-tenant serving. The
--    consolidation worker joins role_private_worker on the SAME narrow 0147 resolver rather than
--    getting any table grant on control.reasoning_route_bindings (whose non-owner cells stay `—`).
-- ---------------------------------------------------------------------------------------------
GRANT EXECUTE ON FUNCTION control.current_reasoning_route_binding(uuid, text)
  TO role_consolidation_worker;

-- ---------------------------------------------------------------------------------------------
-- 6. `next_retry_at` joins role_consolidation_worker's column-level UPDATE on ops.jobs.
--    A derived-work job released after an ENVIRONMENTAL failure (the tenant's route binding is
--    not admitted yet, a provider blip, a DB blip) previously kept the `next_retry_at` its
--    enqueue trigger stamped — already in the past — so every `--serve` poll re-claimed it
--    instantly and burned one attempt with zero delay: a tenant onboarded before its binding was
--    admitted lost the job to `DEAD` within a few poll intervals. Backing the release off needs
--    exactly this one extra column (`adapters::jobs::retry_backoff_seconds`). It is scheduling
--    state on a row the role already owns the lease of, and `role_private_worker` has held the
--    unrestricted table-level UPDATE since 0011; this only closes the gap between the two.
-- ---------------------------------------------------------------------------------------------
GRANT UPDATE (next_retry_at) ON ops.jobs TO role_consolidation_worker;
