-- §46 (EXPAND_CONTRACT; card 35 S6, ADR-0062 D-P; card 32 debt, ADR-0058 L23). Automatic re-drive of a
-- schema-failed distill death:
--   ops.jobs.auto_redrives               how many times the daemon re-drove this job (0 or 1): written only by the
--                                        door below (no runtime role gains a grant on it); the operator's
--                                        `jobs requeue-dead` neither reads nor resets it, so one Evidence is
--                                        re-driven automatically at most once.
--   ops.auto_redrive_schema_failed       caller: humaux-maintenance --serve / sweep once (task `redrive`, through
--     (uuid, interval, integer)          adapters::maintenance_repo::auto_redrive_schema_failed, which appends the
--                                        §77 audit row in the same transaction); fence: the tenant argument must
--                                        equal the installed tenant GUC, every relation is filtered by that tenant,
--                                        p_cooldown > 0 and p_limit > 0 (22023), LIMIT counts the jobs taken, rows
--                                        locked FOR UPDATE SKIP LOCKED, one call is one transaction (plpgsql); owner
--                                        role_migration_owner, search_path pinned, EXECUTE role_maintenance only.
--
-- Victims: DERIVED_DISTILL jobs DEAD with exactly `FAILED_OUTPUT_SCHEMA` (never PROVIDER_PERMANENT,
-- ATTEMPTS_EXHAUSTED, PRE_DISPATCH_ABANDONED, EXECUTION_UNCERTAIN or any other class), never re-driven by this door,
-- whose last counted provider call (ops.distill_calls.begun_at; no dead_at column exists) lies more than p_cooldown
-- in the past. A job with no counted call never qualifies.
-- Each victim is marked auto_redrives = 1 first, then re-armed through 0200's ops.requeue_dead_distill in job mode,
-- unchanged: one re-drive code path, on the route's own output channel (ADR-0060 D-D: the request shape follows the
-- admitted Profile). A refusal (55000 evidence_gone / outbox_settled) is caught in a sub-block, returned as a skip
-- row, and the mark stays, so a refused job is never taken again. An always-refusing provider therefore costs
-- 2 + 2 counted calls and then stays DEAD.

ALTER TABLE ops.jobs ADD COLUMN auto_redrives smallint NOT NULL DEFAULT 0;
COMMENT ON COLUMN ops.jobs.auto_redrives IS
  'ADR-0062 D-P: automatic re-drives of this job by ops.auto_redrive_schema_failed (0 or 1); written by that door only.';

CREATE FUNCTION ops.auto_redrive_schema_failed(p_tenant uuid, p_cooldown interval, p_limit integer)
RETURNS TABLE (job_id uuid, evidence_id uuid, skipped text)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
DECLARE
  v record;
BEGIN
  IF p_tenant IS NULL OR p_cooldown IS NULL OR p_cooldown <= interval '0'
     OR p_limit IS NULL OR p_limit <= 0 THEN
    RAISE EXCEPTION 'auto_redrive_schema_failed: p_cooldown and p_limit must be > 0' USING ERRCODE = '22023';
  END IF;
  IF p_tenant IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;

  FOR v IN
    SELECT j.job_id
      FROM ops.jobs j
     WHERE j.tenant_id = p_tenant AND j.job_type = 'DERIVED_DISTILL' AND j.status = 'DEAD'
       AND j.last_error_class = 'FAILED_OUTPUT_SCHEMA'
       AND j.auto_redrives = 0
       AND (SELECT max(c.begun_at) FROM ops.distill_calls c
             WHERE c.tenant_id = p_tenant AND c.job_id = j.job_id) < now() - p_cooldown
     ORDER BY j.job_id
     LIMIT p_limit
     FOR UPDATE SKIP LOCKED
  LOOP
    UPDATE ops.jobs j SET auto_redrives = 1 WHERE j.tenant_id = p_tenant AND j.job_id = v.job_id;
    job_id := v.job_id;
    evidence_id := NULL;
    skipped := NULL;
    BEGIN
      SELECT r.evidence_id INTO evidence_id FROM ops.requeue_dead_distill(p_tenant, v.job_id, NULL) r;
    EXCEPTION WHEN SQLSTATE '55000' THEN
      -- 0200 job mode: evidence_gone / outbox_settled; the sub-block rolls back the re-arm, the mark above stays.
      skipped := SQLERRM;
    END;
    RETURN NEXT;
  END LOOP;
END;
$$;
ALTER FUNCTION ops.auto_redrive_schema_failed(uuid, interval, integer) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.auto_redrive_schema_failed(uuid, interval, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.auto_redrive_schema_failed(uuid, interval, integer) TO role_maintenance;

COMMENT ON FUNCTION ops.auto_redrive_schema_failed(uuid, interval, integer) IS
  'ADR-0062 D-P: re-drives each DEAD FAILED_OUTPUT_SCHEMA distill job of the tenant once, after a cool-down from its '
  'last counted provider call, through ops.requeue_dead_distill (job mode); refusals come back as skip rows. '
  'EXECUTE role_maintenance only.';
