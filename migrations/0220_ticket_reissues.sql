-- §46 (EXPAND_CONTRACT; card 35 S5, ADR-0062 D-N; card 31 debt 1, ADR-0057 D-H). Drains Q (`points_unsettled`):
--   projection.ticket_reissues            the once-per-retirement marker: one row per ticket this door issued,
--                                         written only by the door below, in the same transaction as the ticket.
--                                         role_maintenance SELECT, every other runtime role nothing; FORCE RLS on
--                                         the tenant GUC.
--   projection.reissue_unsettled_tickets  caller: humaux-maintenance --serve / sweep once (task `reissue`, through
--     (uuid, interval, integer)           adapters::maintenance_repo::reissue_unsettled_tickets); fence: the tenant
--                                         argument must equal the installed tenant GUC, every relation is filtered
--                                         by that tenant, p_cooldown > 0 and p_limit > 0 (22023), LIMIT counts the
--                                         tickets issued, one call is one transaction (plpgsql), one reissuer per
--                                         tenant at a time (transaction advisory lock); owner role_migration_owner,
--                                         search_path pinned, EXECUTE role_maintenance only.
--
-- Candidates (ADR-0062 D-N): per stream of the tenant and per memory its tickets reach (0189's join: stream_log ->
-- ops.outbox by commit_seq -> memory_evidence by evidence_id), the latest ticket `t` (highest stream_seq) is FAILED,
-- LOST or RETIRED_FAILED; the memory has no TOMBSTONED and no in-flight ticket on that stream and is indexable (the
-- 0219 v2 term: its PRIMARY evidence is not SECRET_MATERIAL), i.e. exactly the memories 0219 counts in Q, without
-- the caller-visibility narrowing. One ticket per (stream, PRIMARY evidence): the new MEMORY_LIFECYCLE carrier is
-- bound to the PRIMARY evidence, and the worker re-reads every memory that evidence reaches.
-- Eligibility: every class waits out the cool-down, counted from the latest timestamp `t` carries (a LOST ticket has
-- no settled_at: the 0007/0167 CHECK pairs it with the five settled states only). RETIRED_FAILED (an operator
-- acknowledgment; once reissued `t` is no longer latest), LOST and the transient FAILED classes are then eligible;
-- every other FAILED class (the deterministic `card_unbuildable`, `secret_scan_rejected`,
-- `embedding_dimension_mismatch`, plus `embedding_rejected` and any class unknown here) only when `t` is not itself a
-- reissue: retried once, then it waits for an operator retirement.
-- Issue sequence: 0198's inline on `t`'s own stream (Q is per stream; under ADR-0057 D-M every ticket of an Evidence
-- sits on its home stream): nextval('ops.commit_seq_seq'), checkpoint issued_highwater + 1, stream_log ISSUED,
-- ops.outbox MEMORY_LIFECYCLE bound to the PRIMARY evidence, then the marker row. Nothing is ever rewound: the
-- old terminal row stays as it is (the 0167 guard has no edge back to ISSUED).

CREATE TABLE projection.ticket_reissues (
  tenant_id           uuid        NOT NULL REFERENCES control.tenants (tenant_id),
  reissued_commit_seq bigint      NOT NULL,
  source_commit_seq   bigint      NOT NULL,
  source_state        text        NOT NULL CHECK (source_state IN ('FAILED', 'LOST', 'RETIRED_FAILED')),
  source_error_class  text,
  reissued_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, reissued_commit_seq)
);

ALTER TABLE projection.ticket_reissues ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection.ticket_reissues FORCE ROW LEVEL SECURITY;
-- No owner arm: the door runs as the owner under the caller's tenant GUC, so the tenant clause admits exactly the
-- calling tenant's markers.
CREATE POLICY ticket_reissues_tenant_isolation ON projection.ticket_reissues
  USING (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid)
  WITH CHECK (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid);

-- §6.2.2 column projection.ticket_reissues (0161 recipe): re-own, revoke the domain default, grant the one cell.
ALTER TABLE projection.ticket_reissues OWNER TO role_migration_owner;
REVOKE ALL ON projection.ticket_reissues
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;
GRANT SELECT ON projection.ticket_reissues TO role_maintenance;

CREATE FUNCTION projection.reissue_unsettled_tickets(p_tenant uuid, p_cooldown interval, p_limit integer)
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
             GREATEST(sl.issued_at, sl.settled_at, sl.lease_expires_at, sl.next_attempt_at) AS last_activity
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
         -- ADR-0062 D-N: the cool-down holds for every class, retirement included.
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

COMMENT ON TABLE projection.ticket_reissues IS
  'ADR-0062 D-N: one row per ticket projection.reissue_unsettled_tickets issued (the once-per-retirement marker).';
COMMENT ON FUNCTION projection.reissue_unsettled_tickets(uuid, interval, integer) IS
  'ADR-0062 D-N: one fresh MEMORY_LIFECYCLE ticket per (stream, PRIMARY evidence) for the memories in Q, on the '
  'latest ticket''s own stream, after a cool-down; deterministic classes once. EXECUTE role_maintenance only.';
