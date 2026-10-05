-- §46 (EXPAND_CONTRACT; card 35 fix pass, ADR-0062 D-N/D-L). The reissue cool-down counts from the LOST transition.
--   projection.stream_log.lost_at          when humaux-maintenance's sweep_lost moved the ticket ISSUED -> LOST.
--                                          Writer: adapters::stream_repo::sweep_lost (role_maintenance, the one role
--                                          the 0167 guard lets make that edge; table-level UPDATE from 0011, so no
--                                          §6.2.2 cell changes). Reader: the door below. NULL on every ticket that is
--                                          not LOST (CHECK) and on LOST rows swept before this migration, whose
--                                          cool-down still counts from their other timestamps.
--   projection.reissue_unsettled_tickets   re-created with `lost_at` in last_activity, otherwise 0220's body
--     (uuid, interval, integer)            verbatim (caller, fence, owner and EXECUTE unchanged: see 0220's header).
--
-- Why (review P1, promoted by the main line): a LOST ticket has no settled_at (0007/0167 pair it with the five
-- settled states only), so 0220's last_activity of a just-swept ticket was its issued_at, already older than
-- LOST_AFTER; with no runner on the stream every reissued ticket went LOST and was reissued on the next cycle, with
-- no cool-down at all. Now consecutive reissues of one memory are at least LOST_AFTER + cool-down apart.

ALTER TABLE projection.stream_log
  ADD COLUMN lost_at timestamptz,
  ADD CONSTRAINT stream_log_lost_at_state CHECK (lost_at IS NULL OR state IN ('LOST', 'TOMBSTONED'));

COMMENT ON COLUMN projection.stream_log.lost_at IS
  'ADR-0062 D-N (0223): when sweep_lost moved this ticket ISSUED -> LOST; the reissue cool-down counts from it. '
  'Kept through a later TOMBSTONED; NULL otherwise and on LOST rows swept before 0223.';

CREATE OR REPLACE FUNCTION projection.reissue_unsettled_tickets(p_tenant uuid, p_cooldown interval, p_limit integer)
RETURNS bigint
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  c record;
  v_commit bigint;
  v_seq bigint;
  v_issued bigint := 0;
BEGIN
  IF p_tenant IS NULL OR p_cooldown IS NULL OR p_cooldown <= interval '0'
     OR p_limit IS NULL OR p_limit <= 0 THEN
    RAISE EXCEPTION 'reissue_unsettled_tickets: p_cooldown and p_limit must be > 0' USING ERRCODE = '22023';
  END IF;
  IF p_tenant IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  -- Two concurrent callers would both see the same latest ticket and issue twice; the second waits, then reads the
  -- first one's tickets as in flight (READ COMMITTED: each statement below takes a fresh snapshot).
  PERFORM pg_advisory_xact_lock(hashtextextended('reissue_unsettled_tickets:' || p_tenant::text, 0));

  FOR c IN
    WITH tickets AS (
      SELECT sl.scope_kind, sl.scope_id, sl.domain, sl.projection_kind, sl.projection_version,
             sl.stream_seq, sl.commit_seq, sl.state, sl.error_class, me.memory_id,
             GREATEST(sl.issued_at, sl.settled_at, sl.lost_at, sl.lease_expires_at, sl.next_attempt_at)
               AS last_activity
        FROM projection.stream_log sl
        JOIN ops.outbox ob ON ob.tenant_id = sl.tenant_id AND ob.commit_seq = sl.commit_seq
        JOIN private.memory_evidence me ON me.evidence_id = ob.evidence_id
       WHERE sl.tenant_id = p_tenant
    ), per_memory AS (
      SELECT t.*,
             bool_or(t.state = 'TOMBSTONED') OVER m AS tomb,
             bool_or(t.state IN ('ISSUED', 'PROCESSING', 'WAITING_KEY', 'RETRY_WAIT')) OVER m AS in_flight,
             row_number() OVER (m ORDER BY t.stream_seq DESC) AS recency
        FROM tickets t
      WINDOW m AS (PARTITION BY t.scope_kind, t.scope_id, t.domain, t.projection_kind, t.projection_version,
                                t.memory_id)
    ), eligible AS (
      SELECT pm.scope_kind, pm.scope_id, pm.domain, pm.projection_kind, pm.projection_version,
             pm.stream_seq, pm.commit_seq, pm.state, pm.error_class, pm.last_activity,
             pe.evidence_id AS primary_evidence
        FROM per_memory pm
        JOIN private.memory_records m ON m.memory_id = pm.memory_id AND m.tenant_id = p_tenant
        JOIN private.memory_evidence pe ON pe.memory_id = pm.memory_id AND pe.role = 'PRIMARY'
       WHERE pm.recency = 1 AND NOT pm.tomb AND NOT pm.in_flight
         AND pm.state IN ('FAILED', 'LOST', 'RETIRED_FAILED')
         -- ADR-0062 D-M/D-O: the 0219 `indexable` term; a secret-PRIMARY memory never holds a point.
         -- ponytail: evidence rows are visibility-scoped, so a secret PRIMARY the owner cannot see without a user
         -- GUC reads as indexable here; its one reissue settles SKIPPED_BY_POLICY (the worker reads the ticket
         -- evidence's class), which leaves Q. Upgrade: a tenant-only owner read arm on evidence data_class.
         AND NOT EXISTS (
               SELECT 1 FROM private.evidence_objects eo
                WHERE eo.evidence_id = pe.evidence_id AND eo.tenant_id = p_tenant
                  AND eo.data_class = 'SECRET_MATERIAL')
         -- ADR-0062 D-N: the cool-down holds for every class, retirement included; a LOST ticket's clock is its
         -- lost_at (0223), so a ticket the sweep just made LOST waits the full cool-down.
         AND pm.last_activity < now() - p_cooldown
         AND (pm.state IN ('RETIRED_FAILED', 'LOST')
              -- ADR-0052 D-E / ADR-0057 D-H: the transient classes that reach FAILED.
              OR pm.error_class IN ('transient_exhausted', 'qdrant_upsert_rejected', 'qdrant_delete_rejected',
                                    'registry_conflict', 'registry_failed')
              -- Every other class, unknown ones included: once, then an operator retirement.
              OR NOT EXISTS (
                   SELECT 1 FROM projection.ticket_reissues r
                    WHERE r.tenant_id = p_tenant AND r.reissued_commit_seq = pm.commit_seq))
    )
    SELECT d.* FROM (
      SELECT DISTINCT ON (e.scope_kind, e.scope_id, e.domain, e.projection_kind, e.projection_version,
                          e.primary_evidence) e.*
        FROM eligible e
       ORDER BY e.scope_kind, e.scope_id, e.domain, e.projection_kind, e.projection_version,
                e.primary_evidence, e.stream_seq DESC
    ) d
    ORDER BY d.last_activity, d.commit_seq
    LIMIT p_limit
  LOOP
    v_commit := nextval('ops.commit_seq_seq');
    UPDATE projection.stream_checkpoints k
       SET issued_highwater = k.issued_highwater + 1
     WHERE k.tenant_id = p_tenant AND k.scope_kind = c.scope_kind AND k.scope_id = c.scope_id
       AND k.domain = c.domain AND k.projection_kind = c.projection_kind
       AND k.projection_version = c.projection_version
    RETURNING k.issued_highwater INTO STRICT v_seq;
    INSERT INTO projection.stream_log
      (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, stream_seq, commit_seq)
    VALUES (p_tenant, c.scope_kind, c.scope_id, c.domain, c.projection_kind, c.projection_version, v_seq, v_commit);
    INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id)
    VALUES (p_tenant, v_commit, v_seq, 'MEMORY_LIFECYCLE', c.primary_evidence);
    INSERT INTO projection.ticket_reissues
      (tenant_id, reissued_commit_seq, source_commit_seq, source_state, source_error_class)
    VALUES (p_tenant, v_commit, c.commit_seq, c.state, c.error_class);
    v_issued := v_issued + 1;
  END LOOP;
  RETURN v_issued;
END;
$$;
ALTER FUNCTION projection.reissue_unsettled_tickets(uuid, interval, integer) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION projection.reissue_unsettled_tickets(uuid, interval, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION projection.reissue_unsettled_tickets(uuid, interval, integer) TO role_maintenance;
