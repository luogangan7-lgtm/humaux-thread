-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0166). ADR-0042 (card 20, folded
-- debt 1 from card 18): the audited retirement of an exhausted FAILED ticket.
--
-- Why (card 18 / soak21, soak22): §15.4's contiguous-DONE prefix is
--   min{ s | stream_log[s].state ∉ SETTLED_OK } − 1
-- and §15.2 freezes `SETTLED_OK = DONE | SKIPPED_BY_POLICY | TOMBSTONED`, so ONE settled
-- `FAILED` row pins the prefix of that stream forever. A tenant whose first ticket window
-- catches a distill failure can therefore never satisfy §16.3's criterion ② (`open_gaps == 0`),
-- never gets a serving projection, and answers `no_serving_projection` →
-- `DEPENDENCY_UNAVAILABLE` for every recall from then on (soak21: 27 of 27).
--
-- The REJECTED fix (stored as `rejected`, card 18): "count FAILED as settled in the prefix".
-- Baseline_2.9.md:3805-3818 freezes the opposite with a worked example, and the prefix is the
-- RYW overlay's lower bound (Baseline_2.9.md:3884, "overlay 下界取 contiguous_done_prefix"), so
-- advancing past a FAILED seq silently drops a never-indexed record out of reads while the
-- envelope still claims completeness. That redefines the offset instead of moving the record.
--
-- The ACCEPTED fix, checked against Kafka Connect / Debezium `errors.tolerance=all` + DLQ: move
-- the poison record OUT of the stream into a new terminal state, do not redefine the offset.
-- `RETIRED_FAILED` is a NEW state, deliberately not a reuse of `SKIPPED_BY_POLICY` — that value
-- means §18.2 `SecretMaterial`/policy exclusion, and overloading it onto a distill failure would
-- make the two indistinguishable in every count that already reports `skipped_by_policy`
-- separately (§15.6, §23.1②, the soak's `skipped_by_policy_rate`).
--
-- Read-side consequence, stated because it is the whole risk (§15.4/§15.5, Baseline:3884): a
-- retired seq IS inside the contiguous prefix, so the RYW overlay's lower bound moves past it
-- and that Evidence is no longer served from the overlay. It was never indexed either. The
-- record therefore becomes permanently invisible to recall — that is what retirement MEANS, and
-- it is why this transition is audited (who/when + the preserved `error_class`) and why it is
-- not something the worker that wrote the failure can do to itself. Pinned by
-- `crates/adapters/tests/retrieve_read_your_writes.rs::
-- retired_seq_leaves_the_overlay_and_enters_the_contiguous_prefix`.
--
-- WHO may retire (the §6.3 admin path, `role_maintenance`) and who may NOT:
--   `projection.stream_log.state = 'FAILED'` is written by the projection worker
--   (`adapters::projection_worker::settle_row`, the sole `role_retrieval_worker` edge) on the
--   FIRST error of a pass, with NO retry budget spent — sixteen distinct `error_class` values
--   reach it and most are transient infrastructure (`db_begin_failed`, `db_commit_failed`,
--   `qdrant_upsert_failed`, `embedding_failed`, `registry_failed`, `secret_scan_failed`).
--   `FAILED` is therefore NOT evidence that retries are exhausted, so the role that writes it
--   must not also be the role that settles it OK; that would bury a Qdrant hiccup as "settled"
--   and is exactly the silent-drop harm the rejected option (a) was rejected for.
--   The worker that really does exhaust retries is `humaux-private-worker`
--   (`bins/private-worker/src/distill.rs`: `job.attempt >= dispatch.max_attempts` ⇒
--   `DerivedWorkOutcome::Dead`), but at that moment the TICKET is still `ISSUED` — the projection
--   worker only turns it into `FAILED` on a later pass, when it reads the terminal `ops.outbox`
--   row (`terminal_for_missing_memory(Some("FAILED")) ⇒ (Failed, "distill_failed")`). There is
--   nothing for it to retire at park time, so it gets no EXECUTE: a grant no caller can use is
--   attack surface, not a caller.
-- ⇒ EXECUTE goes to `role_maintenance` only, the same role that owns the §15.2 `ISSUED -> LOST`
--   patrol (`adapters::stream_repo::sweep_lost`), which is the existing sweep of exactly this
--   shape.
--
-- `projection.processing_gaps` needs NO edit: §15.2's frozen view text is
-- `WHERE state IN ('FAILED','LOST')`, and a retired row's state is no longer `FAILED`, so it
-- leaves `open_gaps` by construction. Restating the view to exclude `RETIRED_FAILED` would add a
-- second place that has to agree with the state set.

-- ---------------------------------------------------------------------------------------------
-- 1. The state itself: §15.2's closed set and the TERMINAL/`settled_at` pairing both widen, and
--    the audit columns are NOT NULL exactly when the row is retired (so a retired row without an
--    actor or a timestamp is unrepresentable, not merely discouraged).
-- ---------------------------------------------------------------------------------------------
ALTER TABLE projection.stream_log
  ADD COLUMN retired_at timestamptz,
  ADD COLUMN retired_by text;

ALTER TABLE projection.stream_log
  DROP CONSTRAINT stream_log_state_check,
  ADD CONSTRAINT stream_log_state_check CHECK (
    state = ANY (ARRAY[
      'ISSUED','PROCESSING','WAITING_KEY','RETRY_WAIT','LOST',
      'DONE','SKIPPED_BY_POLICY','FAILED','TOMBSTONED','RETIRED_FAILED'
    ]::text[])
  ),
  DROP CONSTRAINT stream_log_check,
  ADD CONSTRAINT stream_log_check CHECK (
    (state = ANY (ARRAY[
      'DONE','SKIPPED_BY_POLICY','FAILED','TOMBSTONED','RETIRED_FAILED'
    ]::text[])) = (settled_at IS NOT NULL)
  ),
  ADD CONSTRAINT stream_log_retirement_audit CHECK (
    (state = 'RETIRED_FAILED') = (retired_at IS NOT NULL AND retired_by IS NOT NULL)
  );

COMMENT ON COLUMN projection.stream_log.retired_at IS
  '§15.2 (0167): when the exhausted FAILED ticket was retired. NOT NULL exactly when state = RETIRED_FAILED.';
COMMENT ON COLUMN projection.stream_log.retired_by IS
  '§15.2 (0167): session_user that called projection.retire_failed_ticket. The failure class stays in error_class, never overwritten.';

-- ---------------------------------------------------------------------------------------------
-- 2. The transition guard. 0011:415's three role arms are reproduced byte-for-byte (only the
--    owner arm and the widened `settled_at` list are new): `role_retrieval_worker` still settles
--    `ISSUED -> {DONE,SKIPPED_BY_POLICY,FAILED}` and nothing else, so the projection worker
--    cannot reach `RETIRED_FAILED` even though it holds table-level UPDATE.
--
--    `FAILED -> RETIRED_FAILED` is admitted ONLY when `current_user` is the owner. Inside a
--    SECURITY DEFINER function owned by `role_migration_owner`, `current_user` IS the owner and
--    is not spoofable (no runtime role holds membership in it, so none can SET ROLE into it) —
--    the same non-GUC argument 0164 records: any session can set a custom GUC, so a GUC arm is
--    advisory, not an authorization boundary.
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION projection.stream_log_guard_state_transition() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  actor text := current_user;
  legal boolean := false;
BEGIN
  IF OLD.state = NEW.state THEN
    RETURN NEW;
  END IF;

  IF actor = 'role_private_worker' THEN
    -- §15.2 "Private processing transition", verbatim: "PROCESSING -> WAITING_KEY |
    -- RETRY_WAIT | FAILED | ISSUED(next reclaim) | next stage" — PROCESSING -> ISSUED is
    -- the next-reclaim edge, not a typo of WAITING_KEY/RETRY_WAIT -> ISSUED below.
    legal := (OLD.state, NEW.state) IN (
      ('ISSUED','PROCESSING'), ('PROCESSING','WAITING_KEY'), ('PROCESSING','RETRY_WAIT'),
      ('PROCESSING','FAILED'), ('PROCESSING','ISSUED'), ('WAITING_KEY','ISSUED'), ('RETRY_WAIT','ISSUED')
    );
  ELSIF actor = 'role_retrieval_worker' THEN
    -- §6.2.2 line, verbatim: "投影侧终态（ISSUED -> DONE | SKIPPED_BY_POLICY | FAILED）在
    -- role_retrieval_worker" — the parenthesised pair IS the full source-state set (ISSUED
    -- only), not a floor; PROCESSING/WAITING_KEY/RETRY_WAIT never settle straight to a
    -- terminal state through this role. RETIRED_FAILED is deliberately absent: the role that
    -- writes the failure does not get to settle it OK (0167 header).
    legal := OLD.state = 'ISSUED' AND NEW.state IN ('DONE','SKIPPED_BY_POLICY','FAILED');
  ELSIF actor = 'role_maintenance' THEN
    -- §15.2 sweep (ISSUED -> LOST) + §37.2 retention::tombstone (* -> TOMBSTONED).
    legal := (OLD.state = 'ISSUED' AND NEW.state = 'LOST') OR NEW.state = 'TOMBSTONED';
  ELSIF actor = 'role_migration_owner' THEN
    -- §15.2 amendment (0167): the audited retirement, reachable only through
    -- projection.retire_failed_ticket(...) below — the one owner-side edge on this table.
    legal := OLD.state = 'FAILED' AND NEW.state = 'RETIRED_FAILED';
  END IF;

  IF NOT legal THEN
    RAISE EXCEPTION 'illegal projection.stream_log state transition % -> % by role % (§6.2.2/§15.2)',
      OLD.state, NEW.state, actor
      USING ERRCODE = 'check_violation';
  END IF;

  -- 0007's CHECK requires settled_at IS NOT NULL exactly on the TERMINAL states (§15.2, widened
  -- by this migration to include RETIRED_FAILED). No role's §6.2.2 grant includes settled_at
  -- (role_private_worker's is column-limited to state,error_class — see §6.2.2 table) precisely
  -- because this owner trigger, not the caller's UPDATE, is the single writer of it: without
  -- this, the one PROCESSING -> FAILED edge role_private_worker is legally allowed to make above
  -- is un-satisfiable at the CHECK and the branch is dead in practice. COALESCE leaves an
  -- explicitly-supplied value alone (no other role currently supplies one), which is also what
  -- keeps a retirement from rewriting the original settlement time.
  IF NEW.state IN ('DONE','SKIPPED_BY_POLICY','FAILED','TOMBSTONED','RETIRED_FAILED') THEN
    NEW.settled_at := COALESCE(NEW.settled_at, now());
  END IF;

  RETURN NEW;
END;
$$;

ALTER FUNCTION projection.stream_log_guard_state_transition() OWNER TO role_migration_owner;

-- ---------------------------------------------------------------------------------------------
-- 3. The only door into RETIRED_FAILED.
--
--    * `p_failure_class` is what makes this AUDITED rather than blind: the caller must name the
--      `error_class` it is retiring, and a mismatch retires nothing (0 rows). "Retire whatever
--      failed" is exactly the blanket action that would let a transient `qdrant_upsert_failed`
--      disappear behind a policy meant for `distill_failed`.
--    * No tenant GUC is set in here on purpose. `projection.stream_log` is ENABLE + FORCE RLS
--      with a single tenant policy, and FORCE applies to the owner too, so this function can only
--      touch rows in the tenant the CALLER already installed (`SET LOCAL humaux.tenant_id`) —
--      the same scoping `sweep_lost` relies on. Setting it from `p_tenant_id` here would hand
--      any executor a cross-tenant write.
--    * `search_path = pg_catalog` pinned; every object schema-qualified.
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION projection.retire_failed_ticket(
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

ALTER FUNCTION projection.retire_failed_ticket(uuid, text, uuid, text, text, text, bigint, text)
  OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION projection.retire_failed_ticket(uuid, text, uuid, text, text, text, bigint, text)
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker,
  role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT EXECUTE ON FUNCTION projection.retire_failed_ticket(uuid, text, uuid, text, text, text, bigint, text)
  TO role_maintenance;

COMMENT ON FUNCTION projection.retire_failed_ticket(uuid, text, uuid, text, text, text, bigint, text) IS
  'ADR-0042 (card 20): the only FAILED -> RETIRED_FAILED door (§15.2/§15.4). SECURITY DEFINER owned by role_migration_owner — current_user inside it is the non-spoofable owner arm the 0167 transition trigger admits. Caller must name the error_class it retires and must have installed the tenant GUC; EXECUTE: role_maintenance only.';
