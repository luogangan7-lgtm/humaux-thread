-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0188). ADR-0057 D-L (card 31): the
-- PostgreSQL side of §23.1② A2 in the POINT unit. `done` counts settled tickets of every kind, the
-- Qdrant `visible` count counts points; one EVIDENCE_ACCEPTED ticket projects N memories and every
-- governance op adds a MEMORY_LIFECYCLE ticket that moves 0 / +1 / -1 points, so the two were
-- different units (rehearsal5: done=6 visible=4 on a healthy stream). This definer counts the
-- memories a stream's tickets reach, in the caller's repeatable-read snapshot (STABLE), so A2 can
-- compare points with points (ADR-0057 D-A).
--
-- projection.stream_point_ledger(stream key, p_secret_class, p_workspace_ids) returns four counts
-- over R = memories m whose evidences carry a ticket of the stream, filtered by the caller's §6.1
-- visibility:
--   points_expected  — m not tombstoned, live (active, unsuperseded), PRIMARY evidence not secret
--   points_settled   — the above, no pending ticket, latest ticket on m DONE: must be visible now
--   points_in_flight — m not tombstoned with a pending ticket: 0 or 1 point right now
--   points_unsettled — m not tombstoned, nothing pending, latest ticket FAILED / LOST /
--                      RETIRED_FAILED: 0 or 1 point until a later ticket acts on m (W5)
--
-- Why a definer: role_gateway reads private.memory_records through the 0155 RESTRICTIVE subject
-- policy, which has no Qdrant mirror; read through it, a subject-restricted memory whose point the
-- caller's Qdrant count does see would read as overshoot. The definer applies the SAME §6.1
-- visibility predicate the Qdrant count applies (private.visibility_allowed, the 0163 membership
-- test) and skips only the subject term. The user arm comes from the `humaux.user_id` GUC only
-- (a caller cannot ask for another user's count); p_workspace_ids can only narrow the workspace arm
-- (membership is re-checked against the GUC user), never widen it. Counts only, never ids.
--
-- Owner role_migration_owner (NOBYPASSRLS): stream_log / outbox / memory_records carry an owner
-- arm that is cluster-wide (0176 lesson), so the first statement asserts p_tenant_id equals the
-- installed tenant GUC and every relation is filtered by tenant explicitly. evidence_objects has no
-- owner arm: the secret test reads it under the caller's visibility; an evidence the caller cannot
-- see is treated as not secret (the memory then reads as expected, never as a false close).
-- search_path pinned, every name qualified. EXECUTE role_gateway (request handlers, serving_repo)
-- and role_retrieval_worker (stream_repo::fetch_ledger_closure); PUBLIC revoked. No table grant.
--
-- ponytail: O(tickets of the stream) per read; upgrade when card-30 stage timing shows the ledger
-- stage p95 over budget — per-memory projection state on the registry, never a materialized count
-- (§37.2).

CREATE FUNCTION projection.stream_point_ledger(
  p_tenant_id uuid,
  p_scope_kind text,
  p_scope_id uuid,
  p_domain text,
  p_projection_kind text,
  p_projection_version text,
  p_secret_class text,
  p_workspace_ids uuid[]
) RETURNS TABLE (
  points_expected bigint,
  points_settled bigint,
  points_in_flight bigint,
  points_unsettled bigint
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_user uuid := NULLIF(current_setting('humaux.user_id', true), '')::uuid;
BEGIN
  IF p_tenant_id IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  IF p_secret_class IS NULL OR length(p_secret_class) = 0 THEN
    RAISE EXCEPTION 'invalid secret class' USING ERRCODE = '22023';
  END IF;
  RETURN QUERY
  WITH tickets AS (
    SELECT me.memory_id, sl.stream_seq, sl.state
      FROM projection.stream_log sl
      JOIN ops.outbox ob ON ob.tenant_id = sl.tenant_id AND ob.commit_seq = sl.commit_seq
      JOIN private.memory_evidence me ON me.evidence_id = ob.evidence_id
     WHERE sl.tenant_id = p_tenant_id AND sl.scope_kind = p_scope_kind
       AND sl.scope_id = p_scope_id AND sl.domain = p_domain
       AND sl.projection_kind = p_projection_kind
       AND sl.projection_version = p_projection_version
  ), per_memory AS (
    -- latest = the state of the highest stream_seq ticket on m (W6: one family runs in seq order)
    SELECT t.memory_id,
           bool_or(t.state = 'TOMBSTONED') AS tomb,
           bool_or(t.state IN ('ISSUED', 'PROCESSING', 'WAITING_KEY', 'RETRY_WAIT')) AS in_flight,
           (array_agg(t.state ORDER BY t.stream_seq DESC))[1] AS latest
      FROM tickets t
     GROUP BY t.memory_id
  ), reached AS (
    SELECT pm.tomb, pm.in_flight, pm.latest,
           (m.status = 'active' AND m.superseded_by IS NULL) AS live,
           NOT EXISTS (
             SELECT 1 FROM private.memory_evidence pe
               JOIN private.evidence_objects eo
                 ON eo.evidence_id = pe.evidence_id AND eo.tenant_id = p_tenant_id
              WHERE pe.memory_id = m.memory_id AND pe.role = 'PRIMARY'
                AND eo.data_class = p_secret_class) AS indexable
      FROM per_memory pm
      JOIN private.memory_records m
        ON m.memory_id = pm.memory_id AND m.tenant_id = p_tenant_id
     WHERE COALESCE(private.visibility_allowed(m.visibility_class, true,
             m.visibility_user_id = v_user,
             m.visibility_workspace_id = ANY (p_workspace_ids)
               AND EXISTS (SELECT 1 FROM control.workspace_memberships wm
                            WHERE wm.tenant_id = p_tenant_id
                              AND wm.workspace_id = m.visibility_workspace_id
                              AND wm.user_id = v_user
                              AND wm.state = 'ACTIVE')), false)
  )
  SELECT count(*) FILTER (WHERE NOT r.tomb AND r.live AND r.indexable),
         count(*) FILTER (WHERE NOT r.tomb AND r.live AND r.indexable AND NOT r.in_flight
                            AND r.latest = 'DONE'),
         count(*) FILTER (WHERE NOT r.tomb AND r.in_flight),
         -- ADR-0057 D-A: after FAILED/LOST/RETIRED_FAILED the point state is 0 or 1 (W5); slack,
         -- not settled, until a later ticket acts on the memory.
         count(*) FILTER (WHERE NOT r.tomb AND NOT r.in_flight
                            AND r.latest IN ('FAILED', 'LOST', 'RETIRED_FAILED'))
    FROM reached r;
END;
$$;

ALTER FUNCTION projection.stream_point_ledger(uuid, text, uuid, text, text, text, text, uuid[])
  OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION projection.stream_point_ledger(uuid, text, uuid, text, text, text, text, uuid[])
  FROM PUBLIC;
GRANT EXECUTE ON FUNCTION projection.stream_point_ledger(uuid, text, uuid, text, text, text, text, uuid[])
  TO role_gateway, role_retrieval_worker;

COMMENT ON FUNCTION projection.stream_point_ledger(uuid, text, uuid, text, text, text, text, uuid[]) IS
  '§23.1② A2 in the point unit (ADR-0057 D-A/D-L): points_expected / points_settled / '
  'points_in_flight / points_unsettled over the memories one stream''s tickets reach, under the '
  'caller''s §6.1 visibility (user from the GUC, workspace arm narrowed by membership). Counts only.';
