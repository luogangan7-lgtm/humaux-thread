-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0196). ADR-0058 ruling R4 (card 32,
-- plan v2 OPS-3 "requeue-dead preserves last_error_class"): the operator re-drive of a DEAD
-- DERIVED_DISTILL job. Before this migration a DEAD distill job (and the FAILED outbox row its
-- DEAD settle wrote in the same transaction) could only be revived by hand-written owner SQL.
--
-- One owner SECURITY DEFINER door, EXECUTE role_maintenance ONLY (the §4.2 operator-write role;
-- `humaux-maintenance jobs requeue-dead` reaches it through adapters::provisioning, the card-28
-- onboarding-door pattern of 0186). It re-arms, in the caller's ONE transaction, each selected
-- DEAD job together with its Evidence's EVIDENCE_ACCEPTED outbox row: job PENDING, attempt 0,
-- abandoned_claims 0, ready now, `last_error_class` KEPT (the operator's record of why it died);
-- outbox FAILED -> PENDING (the job's next claim takes it, ADR-0058 D-C); the tenant's scheduler row
-- admitted (the 0190 admit trigger fires on INSERT only).
--
-- Selection is either one job (p_job_id) or every DEAD job of the tenant whose last_error_class
-- equals p_error_class exactly (no pattern). Refusals (SQLSTATE 55000, reason as the message,
-- nothing written): job_not_found (absent, another tenant's, or not DERIVED_DISTILL),
-- job_not_dead, evidence_gone (the payload names no Evidence, or the Evidence has no outbox row —
-- a re-armed job could only die again), outbox_settled (the Evidence's outbox row is DONE:
-- nothing to distil); in class mode those rows are skipped and zero matches is no_dead_job.
-- 22023 = malformed request; 42501 = p_tenant_id differs from the installed humaux.tenant_id
-- (0176 lesson: an owner door must not become a cross-tenant door).

CREATE FUNCTION ops.requeue_dead_distill(
  p_tenant_id uuid, p_job_id uuid, p_error_class text
) RETURNS TABLE (job_id uuid, evidence_id uuid, last_error_class text, attempt_spent integer)
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
  v_by_job boolean := p_job_id IS NOT NULL;
  v_rearmed integer := 0;
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
    SELECT o.status INTO v_outbox FROM ops.outbox o
    WHERE o.tenant_id = p_tenant_id AND o.event_type = 'EVIDENCE_ACCEPTED'
      AND o.evidence_id = v_evidence
    FOR UPDATE;
    IF v_evidence IS NULL OR v_outbox IS NULL OR v_outbox = 'DONE' THEN
      IF v_by_job THEN
        RAISE EXCEPTION '%', CASE WHEN v_outbox = 'DONE' THEN 'outbox_settled' ELSE 'evidence_gone' END
          USING ERRCODE = '55000';
      END IF;
      CONTINUE;
    END IF;

    -- A DEAD settle writes FAILED; an open row (a v1 leftover, 0193 E8) is taken as it is.
    UPDATE ops.outbox o
    SET status = 'PENDING', processed_at = NULL, lease_owner = NULL, lease_expires_at = NULL
    WHERE o.tenant_id = p_tenant_id AND o.event_type = 'EVIDENCE_ACCEPTED'
      AND o.evidence_id = v_evidence AND o.status = 'FAILED';

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
    RETURN NEXT;
  END LOOP;

  IF v_rearmed = 0 THEN
    RAISE EXCEPTION '%', CASE WHEN v_by_job THEN 'job_not_found' ELSE 'no_dead_job' END
      USING ERRCODE = '55000';
  END IF;
  INSERT INTO ops.distill_tenant_scheduler (tenant_id) VALUES (p_tenant_id)
  ON CONFLICT DO NOTHING;
END;
$$;

ALTER FUNCTION ops.requeue_dead_distill(uuid, uuid, text) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.requeue_dead_distill(uuid, uuid, text)
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker,
  role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT EXECUTE ON FUNCTION ops.requeue_dead_distill(uuid, uuid, text) TO role_maintenance;

COMMENT ON FUNCTION ops.requeue_dead_distill(uuid, uuid, text) IS
  'ADR-0058 R4 (card 32): operator re-drive of DEAD DERIVED_DISTILL jobs (one job, or every DEAD job of the tenant with one exact last_error_class) with their FAILED outbox rows, in one transaction: PENDING, attempt 0, class kept, scheduler row admitted; refuses (55000) job_not_found / job_not_dead / evidence_gone / outbox_settled / no_dead_job. Owner definer; EXECUTE role_maintenance only.';
