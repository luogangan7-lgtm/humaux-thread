-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0199). ADR-0058 main-line ruling
-- (2026-10-02 20:30, P2 fixed in the third pass): in class mode `requeue-dead` REPORTS the matching
-- DEAD jobs it skipped, with the reason, in the receipt and the audit row.
--
-- Why: 0197/0198 skip a matching DEAD job whose Evidence is gone (no outbox row) or whose outbox row
-- is already DONE with a bare CONTINUE, so the operator's receipt listed only what was re-armed and
-- a skipped job was invisible; when every match was skipped the call was refused `no_dead_job`,
-- which named the wrong cause.
--
-- What: the function returns one more column, `skipped` — NULL on a re-armed row,
-- `evidence_gone` / `outbox_settled` on a skipped one (the reasons job mode already refuses with).
-- `no_dead_job` is now raised only when no DEAD job of that class exists; a call whose matches were
-- all skipped returns the skip rows and re-arms nothing (no scheduler row is admitted for it).
--
-- The return shape changes, which CREATE OR REPLACE cannot do, so the function is dropped and
-- re-created in this one transaction: still ONE definer, same name and argument signature
-- (uuid, uuid, text), same owner, SECURITY DEFINER, pinned search_path and grants as 0197; every
-- other line of 0198's body is unchanged.

DROP FUNCTION ops.requeue_dead_distill(uuid, uuid, text);

CREATE OR REPLACE FUNCTION ops.requeue_dead_distill(
  p_tenant_id uuid, p_job_id uuid, p_error_class text
) RETURNS TABLE (job_id uuid, evidence_id uuid, last_error_class text, attempt_spent integer,
                 skipped text)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
DECLARE
  v record;
  v_evidence uuid;
  v_outbox text;
  v_commit bigint;
  v_stream_seq bigint;
  t record;
  v_new_commit bigint;
  v_new_seq bigint;
  v_by_job boolean := p_job_id IS NOT NULL;
  v_rearmed integer := 0;
  v_skipped integer := 0;
BEGIN
  IF p_tenant_id IS NULL OR (p_job_id IS NULL) = (p_error_class IS NULL)
     OR length(btrim(coalesce(p_error_class, 'x'))) = 0 THEN
    RAISE EXCEPTION 'invalid requeue request' USING ERRCODE = '22023';
  END IF;
  IF p_tenant_id IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;

  FOR v IN
    SELECT j.job_id, j.status, j.payload, j.last_error_class, j.attempt
    FROM ops.jobs j
    WHERE j.tenant_id = p_tenant_id AND j.job_type = 'DERIVED_DISTILL'
      AND CASE WHEN v_by_job THEN j.job_id = p_job_id
               ELSE j.status = 'DEAD' AND j.last_error_class = p_error_class END
    ORDER BY j.created_at, j.job_id
    FOR UPDATE
  LOOP
    IF v.status <> 'DEAD' THEN
      RAISE EXCEPTION 'job_not_dead' USING ERRCODE = '55000';
    END IF;

    v_evidence := CASE WHEN v.payload ->> 'evidence_id' ~* '^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$'
                       THEN (v.payload ->> 'evidence_id')::uuid END;
    -- The Evidence's outbox row is the existence test: its FK (NO ACTION) pins the
    -- evidence_objects row, and it is readable under the tenant GUC alone, while the evidence and
    -- event rows are visibility-scoped (USER_PRIVATE / WORKSPACE_SHARED need a user context).
    SELECT o.status, o.commit_seq, o.stream_seq INTO v_outbox, v_commit, v_stream_seq
    FROM ops.outbox o
    WHERE o.tenant_id = p_tenant_id AND o.event_type = 'EVIDENCE_ACCEPTED'
      AND o.evidence_id = v_evidence
    FOR UPDATE;
    IF v_evidence IS NULL OR v_outbox IS NULL OR v_outbox = 'DONE' THEN
      IF v_by_job THEN
        RAISE EXCEPTION '%', CASE WHEN v_outbox = 'DONE' THEN 'outbox_settled' ELSE 'evidence_gone' END
          USING ERRCODE = '55000';
      END IF;
      -- ADR-0058 R8 skip report: class mode names every matching DEAD job it left DEAD, and why.
      v_skipped := v_skipped + 1;
      job_id := v.job_id;
      evidence_id := v_evidence;
      last_error_class := v.last_error_class;
      attempt_spent := v.attempt;
      skipped := CASE WHEN v_outbox = 'DONE' THEN 'outbox_settled' ELSE 'evidence_gone' END;
      RETURN NEXT;
      CONTINUE;
    END IF;

    -- A DEAD settle writes FAILED; an open row (a v1 leftover, 0193 E8) is taken as it is.
    UPDATE ops.outbox o
    SET status = 'PENDING', processed_at = NULL, lease_owner = NULL, lease_expires_at = NULL
    WHERE o.tenant_id = p_tenant_id AND o.event_type = 'EVIDENCE_ACCEPTED'
      AND o.evidence_id = v_evidence AND o.status = 'FAILED';

    -- ADR-0058 D-U (review P0): a remember ticket that can no longer settle gets a successor on
    -- its own stream (header). commit_seq is unique per tenant, so this is at most one row.
    SELECT sl.scope_kind, sl.scope_id, sl.domain, sl.projection_kind, sl.projection_version
    INTO t
    FROM projection.stream_log sl
    WHERE sl.tenant_id = p_tenant_id AND sl.stream_seq = v_stream_seq
      AND sl.commit_seq = v_commit AND sl.state IN ('FAILED', 'RETIRED_FAILED', 'LOST');
    IF FOUND THEN
      -- ponytail: the successor goes to the remember ticket's own projection_version; a version
      -- retired since then is not followed (ADR-0058 L20); upgrade: take the version as a door
      -- argument from the operator's process configuration (PG does not record the served one).
      v_new_commit := nextval('ops.commit_seq_seq');
      UPDATE projection.stream_checkpoints c
      SET issued_highwater = c.issued_highwater + 1
      WHERE c.tenant_id = p_tenant_id AND c.scope_kind = t.scope_kind AND c.scope_id = t.scope_id
        AND c.domain = t.domain AND c.projection_kind = t.projection_kind
        AND c.projection_version = t.projection_version
      RETURNING c.issued_highwater INTO STRICT v_new_seq;
      INSERT INTO projection.stream_log
        (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version,
         stream_seq, commit_seq)
      VALUES (p_tenant_id, t.scope_kind, t.scope_id, t.domain, t.projection_kind,
              t.projection_version, v_new_seq, v_new_commit);
      INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id)
      VALUES (p_tenant_id, v_new_commit, v_new_seq, 'MEMORY_LIFECYCLE', v_evidence);
    END IF;

    -- ADR-0058 D-F: a full budget of provider requests again; the class stays as the record.
    UPDATE ops.jobs j
    SET status = 'PENDING', attempt = 0, abandoned_claims = 0, not_ready_since = NULL,
        lease_owner = NULL, lease_expires_at = NULL, next_retry_at = clock_timestamp()
    WHERE j.job_id = v.job_id;
    v_rearmed := v_rearmed + 1;

    job_id := v.job_id;
    evidence_id := v_evidence;
    last_error_class := v.last_error_class;
    attempt_spent := v.attempt;
    skipped := NULL;
    RETURN NEXT;
  END LOOP;

  IF v_rearmed + v_skipped = 0 THEN
    RAISE EXCEPTION '%', CASE WHEN v_by_job THEN 'job_not_found' ELSE 'no_dead_job' END
      USING ERRCODE = '55000';
  END IF;
  IF v_rearmed > 0 THEN
    INSERT INTO ops.distill_tenant_scheduler (tenant_id) VALUES (p_tenant_id)
    ON CONFLICT DO NOTHING;
  END IF;
END;
$$;

ALTER FUNCTION ops.requeue_dead_distill(uuid, uuid, text) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.requeue_dead_distill(uuid, uuid, text)
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker,
  role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT EXECUTE ON FUNCTION ops.requeue_dead_distill(uuid, uuid, text) TO role_maintenance;

COMMENT ON FUNCTION ops.requeue_dead_distill(uuid, uuid, text) IS
  'ADR-0058 R4 (card 32): operator re-drive of DEAD DERIVED_DISTILL jobs (one job, or every DEAD job of the tenant with one exact last_error_class) with their FAILED outbox rows, in one transaction: PENDING, attempt 0, class kept, scheduler row admitted, and (0198) a successor projection ticket when the remember ticket is FAILED / RETIRED_FAILED / LOST; (0200) class mode also returns each matching DEAD job it skipped with skipped = evidence_gone / outbox_settled (NULL on a re-armed row); refuses (55000) job_not_found / job_not_dead / evidence_gone / outbox_settled (job mode) / no_dead_job (no DEAD job of the class). Owner definer; EXECUTE role_maintenance only.';
