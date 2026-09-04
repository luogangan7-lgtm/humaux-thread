-- §36 (op set incl. RESTORE) / §37.1 / ADR-0020: the memory-level lifecycle event log and
-- the undo window that makes `memory.restore` possible.
--
-- Research question 1 (where a memory-level lifecycle event lives): neither existing sink
-- fits. projection.stream_log is a frozen 12-column DDL with a closed terminal-state set
-- (§37.1) and carries no memory_id; ops.outbox requires evidence_id NOT NULL. So a new
-- append-only table `ops.memory_lifecycle_events` owns the transition history, and the
-- projection side effect still rides the EXISTING `MEMORY_LIFECYCLE` ticket
-- (projection.stream_log ISSUED row + ops.outbox row) that card 1's supersede already issues
-- — one mechanism, not a second ops.jobs payload.
--
-- Writes to the event table go through ONE owner SECURITY DEFINER function,
-- `ops.append_memory_lifecycle(...)`; no runtime role holds a table INSERT/UPDATE/DELETE
-- grant (§6.2.2). `memory_id` deliberately has NO foreign key so a row survives the §37
-- DeletionPlan purge of the memory it describes (the transition happened; the audit fact is
-- durable). `private.memory_records` gains `lifecycle_head_event_id` (the newest event for
-- that memory) which the gateway writes in the same UPDATE that flips authority status.

-- ---------------------------------------------------------------------------------------
-- The append-only event log.
-- ---------------------------------------------------------------------------------------
CREATE TABLE ops.memory_lifecycle_events (
  event_id      uuid PRIMARY KEY DEFAULT uuidv7(),
  -- Append order within the log; identity so nothing but this table assigns it.
  event_seq     bigint GENERATED ALWAYS AS IDENTITY,
  tenant_id     uuid NOT NULL REFERENCES control.tenants(tenant_id) ON DELETE CASCADE,
  -- Closed set mirrors humaux_domain::lifecycle::LifecycleTargetKind (§78.2 DB<->Rust).
  target_kind   text NOT NULL CHECK (target_kind IN ('MEMORY','CONTRIBUTION_RELEASE')),
  -- NO FK: the memory row may be purged (§37) while this transition record must survive.
  memory_id     uuid,
  -- Closed set mirrors humaux_domain::lifecycle::LifecycleOp (§78.2 DB<->Rust).
  op            text NOT NULL CHECK (op IN
                ('TOMBSTONE','SUPERSEDE','REVOKE','ARCHIVE','RESTORE','ERASE')),
  -- Closed set mirrors humaux_domain::lifecycle::LifecycleReason; NULL only for an undo
  -- (RESTORE), whose reason lives on the event it undoes.
  reason_code   text CHECK (reason_code IN
                ('USER_CORRECTION','EXPLICIT_SUPERSEDE','USER_DELETE','USER_ARCHIVE',
                 'CONTRIBUTOR_REVOKE','PRIVACY_ERASE')),
  actor_principal_id uuid NOT NULL,
  -- The SUPERSEDE successor. NO FK (same purge-survival reason as memory_id).
  replacement_memory_id uuid,
  correction_evidence_id uuid,
  -- Self-FK: a RESTORE names the event it reverses (§36 undo chain).
  undoes_event_id uuid REFERENCES ops.memory_lifecycle_events(event_id),
  -- The instant after which this transition can no longer be undone (§78.1: the window is
  -- process config `HUMAUX_GATEWAY_UNDO_WINDOW_SECONDS`, never a literal here). NULL for a
  -- transition that is not itself undoable (RESTORE/ERASE/ARCHIVE).
  undo_deadline timestamptz,
  -- (tenant, actor, idempotency_key) is unique: a replayed confirmed call returns the
  -- original event instead of appending a second one (§34.0.1 receipts semantics).
  idempotency_key text NOT NULL,
  request_fingerprint text NOT NULL,
  -- The MEMORY_LIFECYCLE ticket this event issued, so a replay can rebuild the same
  -- consistency_token (§15.5) without re-issuing a stream seq.
  stream_seq    bigint,
  commit_seq    bigint,
  created_at    timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(created_at)),
  UNIQUE (tenant_id, actor_principal_id, idempotency_key),
  -- A MEMORY target names its memory; a SUPERSEDE names its successor; a RESTORE names the
  -- event it undoes and carries no fresh reason and no new deadline of its own.
  CONSTRAINT memory_lifecycle_target_has_memory
    CHECK (target_kind <> 'MEMORY' OR memory_id IS NOT NULL),
  CONSTRAINT memory_lifecycle_supersede_has_successor
    CHECK (op <> 'SUPERSEDE' OR replacement_memory_id IS NOT NULL),
  CONSTRAINT memory_lifecycle_restore_is_terminal
    CHECK (op <> 'RESTORE' OR (undoes_event_id IS NOT NULL AND undo_deadline IS NULL
           AND reason_code IS NULL)),
  CONSTRAINT memory_lifecycle_non_undo_has_reason
    CHECK (op = 'RESTORE' OR reason_code IS NOT NULL)
);
ALTER TABLE ops.memory_lifecycle_events OWNER TO role_migration_owner;
ALTER TABLE ops.memory_lifecycle_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.memory_lifecycle_events FORCE ROW LEVEL SECURITY;
CREATE POLICY memory_lifecycle_events_tenant ON ops.memory_lifecycle_events
  USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

CREATE INDEX idx_memory_lifecycle_events_memory
  ON ops.memory_lifecycle_events (tenant_id, memory_id);

-- §6.2.2: no runtime role writes the table directly; writes go through the SECURITY DEFINER
-- function below. Readers per the LIFECYCLE LOG ruling: gateway, retrieval_worker, maintenance.
REVOKE ALL ON ops.memory_lifecycle_events FROM PUBLIC, role_gateway, role_private_worker,
  role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer,
  role_maintenance, role_admin;
GRANT SELECT ON ops.memory_lifecycle_events TO role_gateway;
GRANT SELECT ON ops.memory_lifecycle_events TO role_retrieval_worker;
GRANT SELECT ON ops.memory_lifecycle_events TO role_maintenance;

COMMENT ON TABLE ops.memory_lifecycle_events IS
  '§36/§37.1/ADR-0020: append-only memory lifecycle transition log. Written only via '
  'ops.append_memory_lifecycle (SECURITY DEFINER). memory_id has no FK so a row outlives the '
  'memory''s §37 purge. Permissions: §6.2.2 only (SELECT for gateway/retrieval_worker/'
  'maintenance; no direct write grant).';

-- ---------------------------------------------------------------------------------------
-- The one legal writer. Same chokepoint pattern as ops.record_deletion_plan_step (0054/0055):
-- runs as role_migration_owner, FORCE RLS still applies, correctness depends on the caller
-- having already SET LOCAL humaux.tenant_id (every adapters::*::set_authorization_local does).
-- Returns NULL when the (tenant, actor, idempotency_key) row already exists (the adapter's
-- own idempotency pre-check owns replay; a NULL here means a concurrent racer inserted first).
-- ---------------------------------------------------------------------------------------
CREATE FUNCTION ops.append_memory_lifecycle(
  p_tenant_id             uuid,
  p_target_kind           text,
  p_memory_id             uuid,
  p_op                    text,
  p_reason_code           text,
  p_actor_principal_id    uuid,
  p_replacement_memory_id uuid,
  p_correction_evidence_id uuid,
  p_undoes_event_id       uuid,
  p_undo_deadline         timestamptz,
  p_idempotency_key       text,
  p_request_fingerprint   text,
  p_stream_seq            bigint,
  p_commit_seq            bigint
) RETURNS uuid
LANGUAGE plpgsql SECURITY DEFINER SET search_path = ops, pg_catalog AS $$
DECLARE
  new_event_id uuid;
BEGIN
  INSERT INTO ops.memory_lifecycle_events
    (tenant_id, target_kind, memory_id, op, reason_code, actor_principal_id,
     replacement_memory_id, correction_evidence_id, undoes_event_id, undo_deadline,
     idempotency_key, request_fingerprint, stream_seq, commit_seq)
  VALUES
    (p_tenant_id, p_target_kind, p_memory_id, p_op, p_reason_code, p_actor_principal_id,
     p_replacement_memory_id, p_correction_evidence_id, p_undoes_event_id, p_undo_deadline,
     p_idempotency_key, p_request_fingerprint, p_stream_seq, p_commit_seq)
  ON CONFLICT (tenant_id, actor_principal_id, idempotency_key) DO NOTHING
  RETURNING event_id INTO new_event_id;
  RETURN new_event_id;
END;
$$;
ALTER FUNCTION ops.append_memory_lifecycle(
  uuid, text, uuid, text, text, uuid, uuid, uuid, uuid, timestamptz, text, text, bigint, bigint)
  OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.append_memory_lifecycle(
  uuid, text, uuid, text, text, uuid, uuid, uuid, uuid, timestamptz, text, text, bigint, bigint)
  FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.append_memory_lifecycle(
  uuid, text, uuid, text, text, uuid, uuid, uuid, uuid, timestamptz, text, text, bigint, bigint)
  TO role_gateway;

COMMENT ON FUNCTION ops.append_memory_lifecycle IS
  '§36/ADR-0020: the sole writer of ops.memory_lifecycle_events. Returns the new event_id, or '
  'NULL when (tenant, actor, idempotency_key) already exists (a concurrent racer won). Caller '
  'must have SET LOCAL humaux.tenant_id (FORCE RLS applies to the owner too).';

-- ---------------------------------------------------------------------------------------
-- The head pointer on the memory row. The gateway sets it in the same UPDATE that flips
-- authority status (column-level grant only; the arbiter WHERE clause is unchanged).
-- ---------------------------------------------------------------------------------------
ALTER TABLE private.memory_records ADD COLUMN lifecycle_head_event_id uuid;
COMMENT ON COLUMN private.memory_records.lifecycle_head_event_id IS
  '§36/ADR-0020: newest ops.memory_lifecycle_events.event_id for this memory (no FK — the '
  'event outlives a §37 purge). Backfill for rows superseded before 0149 is not required.';
GRANT UPDATE (lifecycle_head_event_id) ON private.memory_records TO role_gateway;
