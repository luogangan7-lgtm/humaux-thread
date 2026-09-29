-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0175). ADR-0052 (card 27): one
-- resident, tenant-free projection runner (`humaux-retrieval-worker --serve`) claims ISSUED
-- projection tickets across every placed tenant by lease, and a transient failure becomes a
-- bounded retry instead of a permanent FAILED (audit P1-1, C4).
--
-- Shape (0164's, ADR-0036 — no new isolation mechanism, only a new caller of an existing one):
--   1. Four lease/attempt columns on projection.stream_log. The ticket STAYS `ISSUED` while it is
--      leased or backing off: every write below is a column write with OLD.state = NEW.state, so
--      the 0011/0167 transition guard returns early and the §6.2.2 verbatim triple for
--      role_retrieval_worker (ISSUED -> DONE | SKIPPED_BY_POLICY | FAILED) does not change.
--      role_retrieval_worker already holds TABLE-level UPDATE on stream_log (0011), so the
--      heartbeat / settle / retry writes need no new grant. role_private_worker's column-level
--      UPDATE (state, error_class) does not widen: the postcheck pins it.
--   2. stream_log_tenant_isolation and tenant_placements_tenant_isolation gain the SAME
--      `current_user = 'role_migration_owner' OR` arm 0004 / 0012 / 0112 / 0147 / 0163 / 0164
--      use. The tenant arm is reproduced from the LIVE pg_get_expr byte for byte (read
--      2026-09-29 on humaux_thread_dev); only the owner arm is new. Not a second permissive
--      policy (0012's header: permissive policies OR together and silently widen access).
--   3. CONSEQUENCE handled here: with that arm, the one existing owner definer that writes
--      stream_log under a caller-installed tenant, projection.retire_failed_ticket (0167),
--      would silently become cross-tenant — its header says it "relies on FORCE RLS" for the
--      scoping. It is re-stated with ONE extra predicate, `tenant_id = <installed tenant GUC>`,
--      so its documented contract becomes a predicate instead of an RLS side effect. Pinned by
--      crates/adapters/tests/projection_claim.rs::
--      retire_failed_ticket_still_refuses_a_tenant_other_than_the_installed_one.
--   4. projection.claim_issued_tickets — the only cross-tenant claim of stream_log (ADR-0052 D-C).
--   5. projection.unplaced_issued_tickets — the only cross-tenant count of tickets whose tenant
--      has no placement row (feeds the worker's placement_missing line).
--   EXECUTE on both goes to role_retrieval_worker ONLY. `current_user` inside a SECURITY DEFINER
--   function is its owner and is not spoofable (no runtime role holds membership in
--   role_migration_owner); a GUC arm is advisory, not a boundary (0164, ADR-0036 D2).
--
-- No new table, no new role, no new table GRANT, no DML.

-- ---------------------------------------------------------------------------------------------
-- 1. Lease + attempt columns. Constant defaults ⇒ metadata-only ADD COLUMN (PostgreSQL 11+).
-- ---------------------------------------------------------------------------------------------
ALTER TABLE projection.stream_log
  ADD COLUMN lease_owner text,
  ADD COLUMN lease_expires_at timestamptz,
  ADD COLUMN attempts integer NOT NULL DEFAULT 0,
  ADD COLUMN next_attempt_at timestamptz,
  ADD CONSTRAINT stream_log_lease_pair CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL)),
  ADD CONSTRAINT stream_log_attempts_nonneg CHECK (attempts >= 0);

COMMENT ON COLUMN projection.stream_log.lease_owner IS
  'ADR-0052 (0176): the --serve process holding this ISSUED ticket (humaux-retrieval-worker/<uuid v7>). NULL = unleased. Paired with lease_expires_at.';
COMMENT ON COLUMN projection.stream_log.lease_expires_at IS
  'ADR-0052 (0176): lease expiry; an expired lease is re-claimable (a killed worker''s tickets come back). Renewed per row by the owner''s heartbeat.';
COMMENT ON COLUMN projection.stream_log.attempts IS
  'ADR-0052 (0176): claims of this ticket so far (the claim increments it; a distill-pending release gives the attempt back). attempts >= MAX_ATTEMPTS on a transient failure settles FAILED transient_exhausted.';
COMMENT ON COLUMN projection.stream_log.next_attempt_at IS
  'ADR-0052 (0176): transient-failure backoff — the ticket is not claimable before this instant. RETRY = state ISSUED AND attempts >= 1 AND next_attempt_at > now() AND lease_owner IS NULL.';

-- ---------------------------------------------------------------------------------------------
-- 2. Owner arms. Tenant arm byte-identical to the live expression.
-- ---------------------------------------------------------------------------------------------
ALTER POLICY stream_log_tenant_isolation ON projection.stream_log
USING (
  current_user = 'role_migration_owner'
  OR tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid
)
WITH CHECK (
  current_user = 'role_migration_owner'
  OR tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid
);

ALTER POLICY tenant_placements_tenant_isolation ON projection.tenant_placements
USING (
  current_user = 'role_migration_owner'
  OR tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid
)
WITH CHECK (
  current_user = 'role_migration_owner'
  OR tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid
);

-- ---------------------------------------------------------------------------------------------
-- 3. retire_failed_ticket: 0167's body plus the installed-tenant predicate (see header item 3).
--    Signature, owner, ACL and comment are kept by CREATE OR REPLACE.
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION projection.retire_failed_ticket(
  p_tenant_id uuid,
  p_scope_kind text,
  p_scope_id uuid,
  p_domain text,
  p_projection_kind text,
  p_projection_version text,
  p_stream_seq bigint,
  p_failure_class text
) RETURNS bigint
LANGUAGE sql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  WITH retired AS (
    UPDATE projection.stream_log
       SET state = 'RETIRED_FAILED',
           retired_at = now(),
           retired_by = session_user
     WHERE tenant_id = p_tenant_id
       -- 0176: the caller's installed tenant, as a predicate (the owner arm now bypasses RLS).
       AND tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
       AND scope_kind = p_scope_kind
       AND scope_id = p_scope_id
       AND domain = p_domain
       AND projection_kind = p_projection_kind
       AND projection_version = p_projection_version
       AND stream_seq = p_stream_seq
       AND state = 'FAILED'
       AND error_class IS NOT DISTINCT FROM p_failure_class
    RETURNING 1
  )
  SELECT count(*)::bigint FROM retired;
$$;

-- ---------------------------------------------------------------------------------------------
-- 4. The cross-tenant claim (ADR-0052 D-A / D-C).
--
--    Statement 1 validates and takes ONE transaction-scoped advisory lock ("HXPRJCLM"), so
--    claimers are serialized. Statement 2 is the claim; in READ COMMITTED a VOLATILE plpgsql
--    statement takes a fresh snapshot, so every claimer that committed before we got the lock is
--    visible to the family-idle predicate. That is what makes "one family is never held by two
--    workers" true: a per-family try-lock inside one statement is rejected (its snapshot
--    predates the lock). Any other isolation level is refused (25000) because a transaction
--    snapshot would re-open exactly that hole.
--    ponytail: one global claim lock; per-family locks in a plpgsql loop if claim p95 grows.
--
--    Eligible (E): ISSUED, workspace scope, the family triple, lease absent or expired (a killed
--    worker's tickets come back — deleting this arm is the reclaim test's fault injection),
--    backoff elapsed. Placed: the tenant has a placement row for p_projection_family. Family
--    idle: no ISSUED row of the same family holds a live lease. Distill closed: the ticket's
--    EVIDENCE_ACCEPTED outbox row is not PENDING/PROCESSING (ADR-0016 D6 moved into the claim,
--    so tickets whose distill is still open do not crowd out ready ones). Fairness: at most
--    p_per_tenant_cap per tenant, tenants served round robin (ORDER BY rn). The UPDATE restates
--    E (0164's EvalPlanQual argument) and returns the tenant's placement row with the ticket.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION projection.claim_issued_tickets(
  p_domain text,
  p_projection_kind text,
  p_projection_version text,
  p_projection_family text,
  p_lease_owner text,
  p_lease_seconds double precision,
  p_limit bigint,
  p_per_tenant_cap bigint
) RETURNS TABLE (
  tenant_id uuid,
  scope_kind text,
  scope_id uuid,
  domain text,
  projection_kind text,
  projection_version text,
  stream_seq bigint,
  commit_seq bigint,
  attempts integer,
  projection_family text,
  collection_name text,
  shard_key text,
  placement_class text,
  point_count bigint,
  bytes_estimate bigint,
  promotion_state text
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
BEGIN
  IF p_limit IS NULL OR p_limit <= 0 OR p_per_tenant_cap IS NULL OR p_per_tenant_cap <= 0
     OR p_lease_seconds IS NULL OR p_lease_seconds <= 0
     OR p_lease_owner IS NULL OR length(btrim(p_lease_owner)) = 0
     OR p_domain IS NULL OR p_projection_kind IS NULL OR p_projection_version IS NULL
     OR p_projection_family IS NULL THEN
    RAISE EXCEPTION 'invalid projection ticket claim' USING ERRCODE = '22023';
  END IF;
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'projection.claim_issued_tickets requires READ COMMITTED'
      USING ERRCODE = '25000';
  END IF;
  PERFORM pg_advisory_xact_lock(5213004883044813901);

  RETURN QUERY
  WITH cand AS (
    SELECT s.tenant_id, s.scope_kind, s.scope_id, s.domain, s.projection_kind,
           s.projection_version, s.stream_seq, s.issued_at,
           row_number() OVER (PARTITION BY s.tenant_id ORDER BY s.scope_id, s.stream_seq) AS rn
    FROM projection.stream_log s
    JOIN projection.tenant_placements p
      ON p.tenant_id = s.tenant_id AND p.projection_family = p_projection_family
    WHERE s.state = 'ISSUED'
      AND s.scope_kind = 'workspace'
      AND s.domain = p_domain
      AND s.projection_kind = p_projection_kind
      AND s.projection_version = p_projection_version
      AND (s.lease_expires_at IS NULL OR s.lease_expires_at < clock_timestamp())
      AND (s.next_attempt_at IS NULL OR s.next_attempt_at <= clock_timestamp())
      AND NOT EXISTS (
        SELECT 1 FROM projection.stream_log f
        WHERE f.domain = s.domain
          AND f.projection_kind = s.projection_kind
          AND f.projection_version = s.projection_version
          AND f.tenant_id = s.tenant_id
          AND f.scope_kind = s.scope_kind
          AND f.scope_id = s.scope_id
          AND f.state = 'ISSUED'
          AND f.lease_expires_at >= clock_timestamp()
      )
      AND NOT EXISTS (
        SELECT 1 FROM ops.outbox o
        WHERE o.tenant_id = s.tenant_id
          AND o.commit_seq = s.commit_seq
          AND o.event_type = 'EVIDENCE_ACCEPTED'
          AND o.status IN ('PENDING', 'PROCESSING')
      )
  ),
  picked AS (
    SELECT s.tenant_id, s.scope_kind, s.scope_id, s.domain, s.projection_kind,
           s.projection_version, s.stream_seq
    FROM projection.stream_log s
    JOIN cand c
      ON c.tenant_id = s.tenant_id AND c.scope_kind = s.scope_kind AND c.scope_id = s.scope_id
     AND c.domain = s.domain AND c.projection_kind = s.projection_kind
     AND c.projection_version = s.projection_version AND c.stream_seq = s.stream_seq
    WHERE c.rn <= p_per_tenant_cap
    ORDER BY c.rn, c.issued_at
    LIMIT p_limit
    FOR UPDATE OF s SKIP LOCKED
  )
  UPDATE projection.stream_log s
     SET lease_owner = p_lease_owner,
         lease_expires_at = clock_timestamp() + make_interval(secs => p_lease_seconds),
         attempts = s.attempts + 1
    FROM picked k, projection.tenant_placements p
   WHERE s.tenant_id = k.tenant_id AND s.scope_kind = k.scope_kind AND s.scope_id = k.scope_id
     AND s.domain = k.domain AND s.projection_kind = k.projection_kind
     AND s.projection_version = k.projection_version AND s.stream_seq = k.stream_seq
     AND p.tenant_id = s.tenant_id AND p.projection_family = p_projection_family
     -- E restated on the UPDATE itself (0164): an EvalPlanQual re-check against a row another
     -- writer just leased or settled rejects it instead of claiming it twice.
     AND s.state = 'ISSUED'
     AND (s.lease_expires_at IS NULL OR s.lease_expires_at < clock_timestamp())
     AND (s.next_attempt_at IS NULL OR s.next_attempt_at <= clock_timestamp())
  RETURNING s.tenant_id, s.scope_kind, s.scope_id, s.domain, s.projection_kind,
            s.projection_version, s.stream_seq, s.commit_seq, s.attempts,
            p.projection_family, p.collection_name, p.shard_key, p.placement_class,
            p.point_count, p.bytes_estimate, p.promotion_state;
END;
$$;

-- ---------------------------------------------------------------------------------------------
-- 5. Tickets the claim will never take because their tenant has no placement row.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION projection.unplaced_issued_tickets(
  p_domain text,
  p_projection_kind text,
  p_projection_version text,
  p_projection_family text
) RETURNS TABLE (tenant_id uuid, tickets bigint)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  SELECT s.tenant_id, count(*)::bigint
  FROM projection.stream_log s
  WHERE s.state = 'ISSUED'
    AND s.scope_kind = 'workspace'
    AND s.domain = p_domain
    AND s.projection_kind = p_projection_kind
    AND s.projection_version = p_projection_version
    AND NOT EXISTS (
      SELECT 1 FROM projection.tenant_placements p
      WHERE p.tenant_id = s.tenant_id AND p.projection_family = p_projection_family
    )
  GROUP BY s.tenant_id
  ORDER BY s.tenant_id;
$$;

-- ---------------------------------------------------------------------------------------------
-- 6. Ownership and the EXECUTE boundary: role_retrieval_worker only.
-- ---------------------------------------------------------------------------------------------
ALTER FUNCTION projection.claim_issued_tickets(text, text, text, text, text, double precision, bigint, bigint)
  OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION projection.claim_issued_tickets(text, text, text, text, text, double precision, bigint, bigint)
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker,
  role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT EXECUTE ON FUNCTION projection.claim_issued_tickets(text, text, text, text, text, double precision, bigint, bigint)
  TO role_retrieval_worker;

ALTER FUNCTION projection.unplaced_issued_tickets(text, text, text, text)
  OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION projection.unplaced_issued_tickets(text, text, text, text)
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker,
  role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT EXECUTE ON FUNCTION projection.unplaced_issued_tickets(text, text, text, text)
  TO role_retrieval_worker;

COMMENT ON FUNCTION projection.claim_issued_tickets(text, text, text, text, text, double precision, bigint, bigint) IS
  'ADR-0052 (card 27): the only cross-tenant claim of projection.stream_log. SECURITY DEFINER owned by role_migration_owner (the non-spoofable owner arm added to stream_log_tenant_isolation / tenant_placements_tenant_isolation in 0176); one global advisory xact lock, then one SKIP LOCKED statement; per-tenant cap, per-family exclusivity, placement and distill-closed predicates; returns only the rows it just leased, each with its tenant''s placement. EXECUTE: role_retrieval_worker only.';
COMMENT ON FUNCTION projection.unplaced_issued_tickets(text, text, text, text) IS
  'ADR-0052 (card 27): per tenant, the ISSUED workspace tickets the claim can never take because the tenant has no placement row for the family (the worker''s placement_missing line). Read-only. EXECUTE: role_retrieval_worker only.';
