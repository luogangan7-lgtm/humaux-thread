-- T4.8 follow-up (review findings on 0054_deletion_plan.sql — 0054 is already applied and
-- immutable, so every fix here is a new statement, not an edit of that file).
--
-- Finding (blocker): `ops.record_deletion_plan_step` raised on every call —
-- `GET DIAGNOSTICS inserted = ROW_COUNT` assigns a `bigint` into a `boolean` local, then
-- `RETURN inserted > 0` compared boolean > integer (`ERROR: operator does not exist:
-- boolean > integer`). Reproduced verbatim against this DB before this fix. Root cause:
-- `inserted` was declared with the return type instead of `ROW_COUNT`'s own type.
--
-- Finding (minor, §6.2 RLS 租户隔离): `p_tenant_id` was a free parameter, never checked
-- against the request it is recorded against, and the PK `(deletion_request_id, step)`
-- has no tenant component — a caller could pass a foreign `deletion_request_id` with its
-- own `tenant_id` and satisfy the WITH CHECK policy (FK checks bypass RLS). Fixed by (a)
-- deriving tenant_id from `control.deletion_requests` inside the function instead of
-- trusting the parameter, and (b) a matching composite FK so the DB rejects any mismatch
-- even if a future caller changes the function.
--
-- Finding (major, §78.2): `control.advance_deletion_request` had zero callers and zero
-- tests anywhere in the workspace (verified: no Rust reference to it, or to
-- `control.deletion_requests` outside the test fixture's seed INSERT). It is a verbatim
-- duplicate of `humaux_application::forget::DeletionState::transition`'s edge list with no
-- contract test reconciling the two — an undetectable drift risk for a function nothing
-- calls. `humaux_application::forget::DeletionState` is the actual single source of truth
-- (§3/§78.3: application crate owns pure decisions); dropping the unused SQL duplicate
-- removes the drift risk instead of adding an out-of-scope contract test for dead code.
--
-- Finding (minor, §37.2 现算不落列): `ordinal` was a second source of truth for the frozen
-- step order (fully derivable from `step` via `DeletionStep::ordinal()`), with nothing
-- constraining the pair — `INSERT ... ('RELATIONS', 8)` was accepted. Fixed with a CHECK
-- tying each step name to its one legal ordinal (mirrors `DeletionStep::ORDER` exactly).

-- ---------------------------------------------------------------------------------------
-- 1. Boolean/bigint fix + tenant_id derived from the referenced request, not trusted as a
--    parameter (defense in depth alongside the FK added below).
-- ---------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ops.record_deletion_plan_step(
  p_deletion_request_id uuid,
  p_tenant_id           uuid,
  p_step                text,
  p_ordinal             smallint,
  p_outcome             text,
  p_detail              text
) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = ops, control, pg_temp AS $$
DECLARE
  n           bigint;
  real_tenant uuid;
BEGIN
  -- §6.2 RLS 租户隔离: the request's own tenant_id is the only trustworthy source — a
  -- caller-supplied p_tenant_id that disagreed with it (accidentally or not) must never
  -- reach the INSERT below. FOR UPDATE not needed here (deletion_requests.tenant_id is
  -- immutable after insert, no code path updates it).
  SELECT tenant_id INTO real_tenant
    FROM control.deletion_requests
   WHERE deletion_request_id = p_deletion_request_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'deletion_request % not found (§37)', p_deletion_request_id
      USING ERRCODE = 'no_data_found';
  END IF;

  INSERT INTO ops.deletion_plan_steps
    (deletion_request_id, tenant_id, step, ordinal, outcome, detail)
  VALUES (p_deletion_request_id, real_tenant, p_step, p_ordinal, p_outcome, p_detail)
  ON CONFLICT (deletion_request_id, step) DO NOTHING;
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n > 0;
END;
$$;

COMMENT ON FUNCTION ops.record_deletion_plan_step IS
  '§37/§65: idempotent step-completion writer. Returns true the first time a given '
  '(deletion_request_id, step) is recorded, false on a replay after interruption '
  '(ON CONFLICT DO NOTHING). tenant_id is derived from control.deletion_requests, never '
  'trusted from the caller (§6.2 RLS tenant isolation) — see 0055 for the incident this '
  'closes.';

-- Ownership/EXECUTE grant survive CREATE OR REPLACE (same object), no need to reissue.

-- ---------------------------------------------------------------------------------------
-- 2. Composite FK backstop for the same tenant-derivation fix: even if a future edit of
--    the function above regressed back to trusting the parameter, the DB itself now
--    refuses a row whose (deletion_request_id, tenant_id) pair doesn't match the request.
-- ---------------------------------------------------------------------------------------
ALTER TABLE control.deletion_requests
  ADD CONSTRAINT deletion_requests_id_tenant_unique UNIQUE (deletion_request_id, tenant_id);

ALTER TABLE ops.deletion_plan_steps
  ADD CONSTRAINT deletion_plan_steps_request_tenant_fk
  FOREIGN KEY (deletion_request_id, tenant_id)
  REFERENCES control.deletion_requests (deletion_request_id, tenant_id);

-- ---------------------------------------------------------------------------------------
-- 3. ordinal is derivable from step — constrain the pair instead of trusting two
--    independently-supplied values to stay in sync (§37.2 "现算不落列" extended to this
--    redundant column: since the column can't be dropped without an out-of-group change to
--    forget_repo.rs's INSERT column list, freeze the pairing instead).
-- ---------------------------------------------------------------------------------------
ALTER TABLE ops.deletion_plan_steps
  ADD CONSTRAINT deletion_plan_steps_ordinal_matches_step CHECK (
    ordinal = CASE step
      WHEN 'STREAM_LOG_TOMBSTONE'            THEN 1
      WHEN 'AUTHORITY_ROWS'                  THEN 2
      WHEN 'RELATIONS'                       THEN 3
      WHEN 'PROJECTION_EVENTS'               THEN 4
      WHEN 'QDRANT_POINTS'                   THEN 5
      WHEN 'OBJECT_BYTES'                    THEN 6
      WHEN 'CACHE_INVALIDATION'              THEN 7
      WHEN 'PUBLIC_CONTRIBUTION_HANDLING'    THEN 8
    END
  );

-- ---------------------------------------------------------------------------------------
-- 4. control.advance_deletion_request: zero callers, zero tests, verbatim duplicate of
--    humaux_application::forget::DeletionState::transition with no contract test — drop
--    the dead, undetectably-driftable SQL copy. humaux_application::forget::DeletionState
--    remains the single source of truth for the deletion status machine (§3/§78.3).
-- ---------------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS control.advance_deletion_request(uuid, uuid, text);
