-- T4.8 (§37 Retention / Deletion, §37.1, §37.2, Privacy Disclosure Ledger / Deletion
-- Propagation §"DeletionGraph"): audit trail for `DeletionPlan`'s eight steps
-- (`humaux_application::forget::DeletionStep`) and the request-level deletion status machine
-- (`humaux_application::forget::DeletionState`, verbatim variant set REQUESTED … PLANNED …
-- IN_PROGRESS … EXTERNAL_PENDING … COMPLETED … PARTIAL_CANNOT_ERASE … FAILED).
--
-- Step 1 (`STREAM_LOG_TOMBSTONE`) itself is never written by anything in this file — it is
-- `retention::tombstone`'s `UPDATE projection.stream_log SET state = 'TOMBSTONED'`
-- (`humaux_adapters::forget_repo::tombstone`, §37.2's sole such statement in the workspace).
-- This migration only adds the two bookkeeping tables and touches neither `stream_log`'s
-- columns nor its role grants — §37.2's frozen 12-column DDL (§15.1) is unchanged by this
-- file (no `deleted_count` or any other new column is added anywhere here; `deleted` stays
-- `count(state = 'TOMBSTONED')`, current-computed).
--
-- Both new tables are outside §6.2.2's 14-table matrix, so table-level grants come from
-- §6.2.1's domain default via 0011's `ALTER DEFAULT PRIVILEGES FOR ROLE <migration runner>`
-- — no `GRANT` statement on either table in this file (same convention as
-- `0032_invitation_referral.sql`'s header comment). Under that default, `role_maintenance`
-- (the role `retention::tombstone`/the retention job connects as, §6.2.3) only holds `SELECT`
-- on both tables. It still needs to *write* `ops.deletion_plan_steps` (one row per completed
-- step, idempotent — §65 replay) and to advance `control.deletion_requests.state`. Rather
-- than widen its table grant (which would require a §6.2.2 named-table override + a matching
-- xtask MATRIX entry, out of this ticket's file scope), both writes go through a
-- `SECURITY DEFINER` function owned by `role_migration_owner` with `EXECUTE` granted only to
-- `role_maintenance` — a single chokepoint per write, the same "mechanism, not discipline"
-- reasoning §37.2 already applies to `stream_log`'s own guard trigger. `EXECUTE` on a
-- function is not a table privilege, so this leaves `check_domain_default_grants` (rls-check)
-- untouched: the tables' own `role_table_grants` rows stay exactly at domain default.

CREATE TABLE control.deletion_requests (
  deletion_request_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id            uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  -- §15.1's six-column stream key + stream_seq: identifies exactly which stream_log row
  -- step 1 tombstones (the same key `retention::tombstone(scope, seq)` takes).
  scope_kind           text        NOT NULL,
  scope_id             uuid        NOT NULL,
  domain               text        NOT NULL,
  projection_kind      text        NOT NULL,
  projection_version   text        NOT NULL,
  stream_seq           bigint      NOT NULL,
  state                text        NOT NULL DEFAULT 'REQUESTED'
                        CHECK (state IN ('REQUESTED','PLANNED','IN_PROGRESS','EXTERNAL_PENDING',
                                         'COMPLETED','PARTIAL_CANNOT_ERASE','FAILED')),
  requested_at         timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now(),
  completed_at         timestamptz,
  -- Mirrors stream_log's own settled_at discipline (§15.1): completed_at is set exactly on
  -- the three terminal states, never before, never left unset after.
  CHECK ((state IN ('COMPLETED','PARTIAL_CANNOT_ERASE','FAILED')) = (completed_at IS NOT NULL)),
  UNIQUE (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, stream_seq)
);

COMMENT ON TABLE control.deletion_requests IS
  '§37 DeletionPlan request + deletion status machine (REQUESTED…EXTERNAL_PENDING/'
  'PARTIAL_CANNOT_ERASE/FAILED). One row per (tenant, stream key, stream_seq) deletion.';

ALTER TABLE control.deletion_requests OWNER TO role_migration_owner;
ALTER TABLE control.deletion_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.deletion_requests FORCE ROW LEVEL SECURITY;

CREATE POLICY deletion_requests_tenant_isolation ON control.deletion_requests
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

CREATE TABLE ops.deletion_plan_steps (
  deletion_request_id uuid        NOT NULL REFERENCES control.deletion_requests(deletion_request_id),
  tenant_id            uuid        NOT NULL,
  -- Closed set mirrors humaux_application::forget::DeletionStep::as_db_str exactly (§37's
  -- eight-step list) — CLAUDE.md 硬边界: DB enum 与 Rust enum 走 contract test 对账 (§78.2).
  step                 text        NOT NULL CHECK (step IN (
                          'STREAM_LOG_TOMBSTONE','AUTHORITY_ROWS','RELATIONS',
                          'PROJECTION_EVENTS','QDRANT_POINTS','OBJECT_BYTES',
                          'CACHE_INVALIDATION','PUBLIC_CONTRIBUTION_HANDLING')),
  ordinal              smallint    NOT NULL CHECK (ordinal BETWEEN 1 AND 8),
  outcome              text        NOT NULL DEFAULT 'DONE'
                        CHECK (outcome IN ('DONE','EXTERNAL_PENDING','CANNOT_ERASE','FAILED')),
  detail               text,
  completed_at         timestamptz NOT NULL DEFAULT now(),
  -- One row per (request, step): re-recording an already-completed step is a no-op insert
  -- conflict (see ops.record_deletion_plan_step below), not a second row — this is what
  -- makes §65's purge replay idempotent under interruption.
  PRIMARY KEY (deletion_request_id, step)
);

COMMENT ON TABLE ops.deletion_plan_steps IS
  '§37 DeletionPlan per-step audit trail, replayable (§65): one row per completed step per '
  'deletion_request_id. Never written directly — only via ops.record_deletion_plan_step '
  '(SECURITY DEFINER) or the direct role_maintenance table grant this migration deliberately '
  'does not add (see file header).';

ALTER TABLE ops.deletion_plan_steps OWNER TO role_migration_owner;
ALTER TABLE ops.deletion_plan_steps ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.deletion_plan_steps FORCE ROW LEVEL SECURITY;

CREATE POLICY deletion_plan_steps_tenant_isolation ON ops.deletion_plan_steps
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

-- SECURITY DEFINER chokepoint (see file header): runs as role_migration_owner regardless of
-- caller, so role_maintenance needs only EXECUTE, never a table-level INSERT/UPDATE grant.
-- RLS still applies (FORCE ROW LEVEL SECURITY above makes the owner subject to it too) —
-- correctness depends on the calling session having already done `SET LOCAL
-- humaux.tenant_id = ...` (same convention as every adapters::*::set_tenant_local, the GUC is
-- session-scoped and this function runs inside the caller's own transaction).
CREATE FUNCTION ops.record_deletion_plan_step(
  p_deletion_request_id uuid,
  p_tenant_id           uuid,
  p_step                text,
  p_ordinal             smallint,
  p_outcome             text,
  p_detail              text
) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = ops, pg_temp AS $$
DECLARE
  inserted boolean;
BEGIN
  INSERT INTO ops.deletion_plan_steps
    (deletion_request_id, tenant_id, step, ordinal, outcome, detail)
  VALUES (p_deletion_request_id, p_tenant_id, p_step, p_ordinal, p_outcome, p_detail)
  ON CONFLICT (deletion_request_id, step) DO NOTHING;
  GET DIAGNOSTICS inserted = ROW_COUNT;
  RETURN inserted > 0;
END;
$$;

COMMENT ON FUNCTION ops.record_deletion_plan_step IS
  '§37/§65: idempotent step-completion writer. Returns true the first time a given '
  '(deletion_request_id, step) is recorded, false on a replay after interruption '
  '(ON CONFLICT DO NOTHING) — this boolean is what makes purge-replay tests observe '
  '"already done, skipped" instead of a duplicate row or an error.';

ALTER FUNCTION ops.record_deletion_plan_step(uuid, uuid, text, smallint, text, text)
  OWNER TO role_migration_owner;
-- Postgres grants EXECUTE to PUBLIC by default on CREATE FUNCTION (§39_edge_security_fixes.sql
-- precedent) — revoked so only the one role this function exists for can call it.
REVOKE ALL ON FUNCTION ops.record_deletion_plan_step(uuid, uuid, text, smallint, text, text)
  FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.record_deletion_plan_step(uuid, uuid, text, smallint, text, text)
  TO role_maintenance;

-- Same SECURITY DEFINER chokepoint pattern for the request-level state machine
-- (humaux_application::forget::DeletionState::transition mirrors this CASE server-side —
-- defense in depth, not a second source of truth: an illegal edge is rejected identically in
-- both places, and the Rust side is what a caller actually consults before calling this).
CREATE FUNCTION control.advance_deletion_request(
  p_deletion_request_id uuid,
  p_tenant_id           uuid,
  p_next_state          text
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = control, pg_temp AS $$
DECLARE
  cur text;
  legal boolean;
BEGIN
  SELECT state INTO cur FROM control.deletion_requests
   WHERE deletion_request_id = p_deletion_request_id AND tenant_id = p_tenant_id
   FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'deletion_request % not found for tenant % (§37)',
      p_deletion_request_id, p_tenant_id USING ERRCODE = 'no_data_found';
  END IF;

  legal := (cur, p_next_state) IN (
    ('REQUESTED','PLANNED'), ('PLANNED','IN_PROGRESS'),
    ('IN_PROGRESS','EXTERNAL_PENDING'), ('IN_PROGRESS','COMPLETED'),
    ('IN_PROGRESS','PARTIAL_CANNOT_ERASE'), ('IN_PROGRESS','FAILED'),
    ('EXTERNAL_PENDING','IN_PROGRESS'), ('EXTERNAL_PENDING','COMPLETED'),
    ('EXTERNAL_PENDING','PARTIAL_CANNOT_ERASE'), ('EXTERNAL_PENDING','FAILED')
  );
  IF NOT legal THEN
    RAISE EXCEPTION 'illegal deletion_request state transition % -> % (§37)', cur, p_next_state
      USING ERRCODE = 'check_violation';
  END IF;

  UPDATE control.deletion_requests
     SET state = p_next_state,
         updated_at = now(),
         completed_at = CASE WHEN p_next_state IN ('COMPLETED','PARTIAL_CANNOT_ERASE','FAILED')
                              THEN now() ELSE completed_at END
   WHERE deletion_request_id = p_deletion_request_id AND tenant_id = p_tenant_id;
END;
$$;

COMMENT ON FUNCTION control.advance_deletion_request IS
  '§37 deletion status machine, sole writer of control.deletion_requests.state. Fail-loud '
  '(§50) on an illegal edge — never a silent clamp.';

ALTER FUNCTION control.advance_deletion_request(uuid, uuid, text) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.advance_deletion_request(uuid, uuid, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.advance_deletion_request(uuid, uuid, text) TO role_maintenance;
