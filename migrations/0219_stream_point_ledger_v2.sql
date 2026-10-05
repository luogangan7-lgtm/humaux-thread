-- §46 forward-fix (EXPAND_CONTRACT; card 35 S4, ADR-0062 D-M/D-O, debt 2 of card 31). v2 of
-- projection.stream_point_ledger (0189, never edited): `indexable` now also fences points_in_flight (F) and
-- points_unsettled (Q), as it already fenced points_expected (U) and points_settled (L). A memory whose PRIMARY
-- evidence is SECRET_MATERIAL is refused by the worker before any scan and can never hold a point, so counting it
-- as in flight or as slack let it hide one real point loss for ever (ADR-0057 known limit 3): no reissue can
-- drain it. `live` stays out of Q on purpose: an archived memory's FAILED unarchive ticket (ADR-0057 limit 13)
-- must stay visible as slack until card 35's reissue settles it.
-- Not modelled (ADR-0062 L7): card-size and gitleaks refusals (`card_unbuildable`, `secret_scan_rejected`);
-- PostgreSQL cannot recompute them, so those memories stay in Q until an operator retirement.
--
-- Same 8-argument signature and return type, so CREATE OR REPLACE keeps every caller
-- (stream_repo::close_ledger_in_txn as role_gateway and role_retrieval_worker) unchanged; a `_v2` name would be
-- a second definer and a second call site. Fence unchanged from 0189: the tenant argument must equal the
-- installed tenant GUC, every relation is filtered by that tenant, STABLE (the caller's repeatable-read
-- snapshot), counts only. Owner role_migration_owner, search_path pinned, EXECUTE role_gateway and
-- role_retrieval_worker only, PUBLIC revoked; re-asserted below rather than trusted to CREATE OR REPLACE.

CREATE OR REPLACE FUNCTION projection.stream_point_ledger(
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
         -- ADR-0062 D-M: a non-indexable memory never holds a point, so it is neither in flight nor slack.
         count(*) FILTER (WHERE NOT r.tomb AND r.in_flight AND r.indexable),
         -- ADR-0057 D-A: after FAILED/LOST/RETIRED_FAILED the point state is 0 or 1 (W5); slack,
         -- not settled, until a later ticket acts on the memory.
         count(*) FILTER (WHERE NOT r.tomb AND NOT r.in_flight AND r.indexable
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
  '§23.1② A2 in the point unit (ADR-0057 D-A/D-L; v2 ADR-0062 D-M): points_expected / points_settled / '
  'points_in_flight / points_unsettled over the indexable memories one stream''s tickets reach, under '
  'the caller''s §6.1 visibility (user from the GUC, workspace arm narrowed by membership). Counts only.';
