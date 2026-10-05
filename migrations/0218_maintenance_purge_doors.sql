-- §46 (EXPAND_CONTRACT; card 35 S2, ADR-0062 D-E..D-J; Baseline §6.2.1 as amended by ruling E1). The closed set of
-- owner SECURITY DEFINER purge doors for expiring operational state. Rows leave these four tables only here:
--   control.sweep_confirm_tokens(interval, integer)                     confirm tokens (D-G; replaces the 0169 door)
--   ops.purge_expired_selection_snapshots(integer)                      enumerate snapshots and their items (D-H)
--   control.purge_idle_rate_buckets(interval, integer)                  rate buckets that would be full now (D-I)
--   ops.purge_terminal_jobs(interval, interval, interval, integer)      DONE / DEAD / FAILED jobs (D-J)
-- Caller of every door: humaux-maintenance --serve / sweep once (adapters::maintenance_repo, and
-- adapters::confirm_token_repo::sweep_expired for the first), one transaction per tenant with SET LOCAL
-- humaux.tenant_id, then exactly this one statement.
-- Fence (each door, ADR-0062 D-E):
--   * owner role_migration_owner, SECURITY DEFINER, search_path pinned to pg_catalog, EXECUTE role_maintenance only;
--   * typed interval / integer arguments only, no text argument anywhere (a table name has no signature to land in);
--   * STRICT: a NULL argument returns NULL without running the body (a LIMIT NULL would mean every row);
--   * the tenant GUC is asserted: `scope` reads current_setting('humaux.tenant_id') without missing_ok and casts it to
--     uuid, so an unset GUC raises 42704 and an empty one 22P02; the final SELECT reads `scope`, so the assertion
--     runs on every call, not only when a candidate row exists, and every victim predicate names that tenant;
--   * a non-positive LIMIT or a negative age is refused by PostgreSQL's own negative-LIMIT error (2201W), raised
--     when the LIMIT node starts, before any row is read or deleted;
--   * LANGUAGE sql, one statement: victim (ORDER BY age LIMIT FOR UPDATE SKIP LOCKED), delete, receipt and count are
--     sibling CTEs of one statement, so a kill -9 either commits the delete with its receipt or rolls both back;
--   * the receipt row (ops.maintenance_receipts, 0214) is written only when the call removed rows (D-F);
--   * every age is compared with the DB clock inside the body; the caller never sends a timestamp.
-- No outbox door exists (ruling E11): outbox rows are existence authority (ADR-0062 D-J).

-- D-G: one confirm-token door. The 0169 predicate verbatim, now bounded and receipted; the 1-argument door goes.
DROP FUNCTION control.sweep_confirm_tokens(interval);

CREATE FUNCTION control.sweep_confirm_tokens(p_consumed_retention interval, p_limit integer)
RETURNS bigint
LANGUAGE sql
VOLATILE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  WITH scope AS MATERIALIZED (
    SELECT current_setting('humaux.tenant_id')::uuid AS tenant_id
  ), victim AS MATERIALIZED (
    SELECT t.confirm_token_id
      FROM control.confirm_tokens t
     WHERE t.tenant_id = (SELECT s.tenant_id FROM scope s)
       AND t.expires_at < now()
       -- §33.10 rule 9: a recently consumed token stays as the audit answer for the call it authorized.
       AND (t.consumed_at IS NULL OR t.consumed_at < now() - p_consumed_retention)
     ORDER BY t.expires_at
     LIMIT CASE WHEN p_limit > 0 AND p_consumed_retention >= interval '0' THEN p_limit ELSE -1 END
     FOR UPDATE SKIP LOCKED
  ), gone AS (
    DELETE FROM control.confirm_tokens t
     USING victim v
     WHERE t.confirm_token_id = v.confirm_token_id
    RETURNING 1
  ), receipt AS (
    INSERT INTO ops.maintenance_receipts (tenant_id, task, cutoff, row_limit, affected)
    SELECT s.tenant_id, 'confirm_tokens', now() - p_consumed_retention, p_limit, n.affected
      FROM scope s, (SELECT count(*) AS affected FROM gone) n
     WHERE n.affected > 0
  )
  SELECT n.affected FROM scope s, (SELECT count(*)::bigint AS affected FROM gone) n
$$;
ALTER FUNCTION control.sweep_confirm_tokens(interval, integer) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.sweep_confirm_tokens(interval, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.sweep_confirm_tokens(interval, integer) TO role_maintenance;

-- D-H: expired snapshots of the calling tenant with their items. LIMIT counts snapshots, so one call deletes at most
-- LIMIT x manifest-size items. The NO ACTION FK items -> snapshots is checked at the end of the statement, after both
-- sibling deletes. The DB clock is the one the page read checks (`expires_at > now()`, selection_repo).
CREATE FUNCTION ops.purge_expired_selection_snapshots(p_limit integer)
RETURNS bigint
LANGUAGE sql
VOLATILE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  WITH scope AS MATERIALIZED (
    SELECT current_setting('humaux.tenant_id')::uuid AS tenant_id
  ), victim AS MATERIALIZED (
    SELECT s.selection_snapshot_id
      FROM ops.selection_snapshots s
     WHERE s.tenant_id = (SELECT c.tenant_id FROM scope c)
       AND s.expires_at < now()
     ORDER BY s.expires_at
     LIMIT CASE WHEN p_limit > 0 THEN p_limit ELSE -1 END
     FOR UPDATE SKIP LOCKED
  ), items AS (
    DELETE FROM ops.selection_snapshot_items i
     USING victim v
     WHERE i.selection_snapshot_id = v.selection_snapshot_id
    RETURNING 1
  ), gone AS (
    DELETE FROM ops.selection_snapshots s
     USING victim v
     WHERE s.selection_snapshot_id = v.selection_snapshot_id
    RETURNING 1
  ), receipt AS (
    INSERT INTO ops.maintenance_receipts (tenant_id, task, cutoff, row_limit, affected)
    SELECT c.tenant_id, 'selection_snapshots', now(), p_limit, n.affected
      FROM scope c, (SELECT count(*) AS affected FROM gone) n
     WHERE n.affected > 0
  )
  SELECT n.affected FROM scope c, (SELECT count(*)::bigint AS affected FROM gone) n
$$;
ALTER FUNCTION ops.purge_expired_selection_snapshots(integer) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.purge_expired_selection_snapshots(integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.purge_expired_selection_snapshots(integer) TO role_maintenance;

-- D-I: idle buckets that would be full right now, so the consumer's recreate (tokens = capacity, quota_repo
-- consume_rate) answers every later request exactly as the deleted row would have: a purge never changes a rate
-- decision. Each victim must also win consume_rate's own advisory key, so a consumer mid-transaction is skipped and
-- a later one waits for this statement, then recreates a full bucket.
CREATE FUNCTION control.purge_idle_rate_buckets(p_idle interval, p_limit integer)
RETURNS bigint
LANGUAGE sql
VOLATILE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  WITH scope AS MATERIALIZED (
    SELECT current_setting('humaux.tenant_id')::uuid AS tenant_id
  ), victim AS MATERIALIZED (
    SELECT b.tenant_id, b.subject_kind, b.subject_id, b.operation, b.bucket_key
      FROM control.rate_buckets b
     WHERE b.tenant_id = (SELECT c.tenant_id FROM scope c)
       AND b.updated_at < clock_timestamp() - p_idle
       AND b.tokens + extract(epoch FROM clock_timestamp() - b.updated_at) * b.refill_per_second >= b.capacity
       -- ponytail: the planner may test the lock before the cheaper predicates, so a non-victim bucket can stay
       -- locked until this statement commits (milliseconds; consumers wait under lock_timeout); a lock-free
       -- recheck in a second CTE if a purge ever runs long.
       AND pg_try_advisory_xact_lock(hashtextextended(
             'rate:' || b.tenant_id::text || ':' || b.subject_kind || ':' || b.subject_id || ':'
               || b.operation || ':' || b.bucket_key, 0))
     ORDER BY b.updated_at
     LIMIT CASE WHEN p_limit > 0 AND p_idle >= interval '0' THEN p_limit ELSE -1 END
     FOR UPDATE SKIP LOCKED
  ), gone AS (
    DELETE FROM control.rate_buckets b
     USING victim v
     WHERE b.tenant_id = v.tenant_id AND b.subject_kind = v.subject_kind AND b.subject_id = v.subject_id
       AND b.operation = v.operation AND b.bucket_key = v.bucket_key
    RETURNING 1
  ), receipt AS (
    INSERT INTO ops.maintenance_receipts (tenant_id, task, cutoff, row_limit, affected)
    SELECT c.tenant_id, 'rate_buckets', clock_timestamp() - p_idle, p_limit, n.affected
      FROM scope c, (SELECT count(*) AS affected FROM gone) n
     WHERE n.affected > 0
  )
  SELECT n.affected FROM scope c, (SELECT count(*)::bigint AS affected FROM gone) n
$$;
ALTER FUNCTION control.purge_idle_rate_buckets(interval, integer) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.purge_idle_rate_buckets(interval, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.purge_idle_rate_buckets(interval, integer) TO role_maintenance;

-- D-J: terminal jobs past their retention. Last activity is the latest timestamp the row carries (a re-armed job
-- gets next_retry_at = clock_timestamp(), 0200). Kept on purpose, whatever the retentions say:
--   * a job with a provider call inside the distill budget window: the ops.distill_calls FK CASCADEs, and
--     ops.admit_distill_budget counts `begun_at > clock_timestamp() - window` with the private worker's own window;
--   * a job a contribution execution links (that FK is NO ACTION and would abort the whole statement);
--   * a DEAD distill job ops.requeue_dead_distill would still re-arm (ADR-0058 R4): its Evidence's EVIDENCE_ACCEPTED
--     row exists and is not DONE. A DEAD distill job is purgeable iff that door would refuse it.
-- ops.jobs' policy has an owner arm (current_user = role_migration_owner), so the tenant predicate below, not RLS, is
-- what keeps this definer inside the calling tenant. Purging a job frees its idempotency_key: safe while every
-- producer's key derives from a row created once (ADR-0062 L5).
CREATE FUNCTION ops.purge_terminal_jobs(
  p_done_retention interval, p_dead_retention interval, p_budget_window interval, p_limit integer
)
RETURNS bigint
LANGUAGE sql
VOLATILE
STRICT
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  WITH scope AS MATERIALIZED (
    SELECT current_setting('humaux.tenant_id')::uuid AS tenant_id
  ), victim AS MATERIALIZED (
    SELECT j.job_id
      FROM ops.jobs j
     WHERE j.tenant_id = (SELECT c.tenant_id FROM scope c)
       AND j.status IN ('DONE', 'DEAD', 'FAILED')
       AND GREATEST(j.created_at, j.next_retry_at, j.lease_expires_at, j.not_ready_since)
             < clock_timestamp() - CASE WHEN j.status = 'DONE' THEN p_done_retention ELSE p_dead_retention END
       AND NOT EXISTS (SELECT 1 FROM ops.distill_calls k
                        WHERE k.job_id = j.job_id AND k.begun_at > clock_timestamp() - p_budget_window)
       AND NOT EXISTS (SELECT 1 FROM ops.contribution_execution_job_links l WHERE l.job_id = j.job_id)
       AND NOT (j.job_type = 'DERIVED_DISTILL' AND j.status = 'DEAD' AND EXISTS (
             SELECT 1 FROM ops.outbox o
              WHERE o.tenant_id = j.tenant_id AND o.event_type = 'EVIDENCE_ACCEPTED'
                -- 0200's guarded cast: a payload without a uuid evidence_id names no Evidence.
                AND o.evidence_id = CASE WHEN j.payload ->> 'evidence_id'
                                               ~* '^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$'
                                         THEN (j.payload ->> 'evidence_id')::uuid END
                AND o.status <> 'DONE'))
     ORDER BY j.created_at
     LIMIT CASE WHEN p_limit > 0 AND p_done_retention >= interval '0' AND p_dead_retention >= interval '0'
                     AND p_budget_window >= interval '0'
                THEN p_limit ELSE -1 END
     FOR UPDATE OF j SKIP LOCKED
  ), gone AS (
    DELETE FROM ops.jobs j
     USING victim v
     WHERE j.job_id = v.job_id
    RETURNING 1
  ), receipt AS (
    INSERT INTO ops.maintenance_receipts (tenant_id, task, cutoff, row_limit, affected)
    SELECT c.tenant_id, 'terminal_jobs', clock_timestamp() - LEAST(p_done_retention, p_dead_retention), p_limit,
           n.affected
      FROM scope c, (SELECT count(*) AS affected FROM gone) n
     WHERE n.affected > 0
  )
  SELECT n.affected FROM scope c, (SELECT count(*)::bigint AS affected FROM gone) n
$$;
ALTER FUNCTION ops.purge_terminal_jobs(interval, interval, interval, integer) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.purge_terminal_jobs(interval, interval, interval, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.purge_terminal_jobs(interval, interval, interval, integer) TO role_maintenance;

COMMENT ON FUNCTION control.sweep_confirm_tokens(interval, integer) IS
  'ADR-0062 D-G: deletes at most p_limit expired confirm tokens of the calling tenant (unconsumed ones at once, consumed ones after p_consumed_retention) with one receipt; EXECUTE role_maintenance only.';
COMMENT ON FUNCTION ops.purge_expired_selection_snapshots(integer) IS
  'ADR-0062 D-H: deletes at most p_limit DB-expired selection snapshots of the calling tenant and their items in one statement with one receipt; EXECUTE role_maintenance only.';
COMMENT ON FUNCTION control.purge_idle_rate_buckets(interval, integer) IS
  'ADR-0062 D-I: deletes at most p_limit idle rate buckets of the calling tenant that would be full now (no rate decision changes) with one receipt; EXECUTE role_maintenance only.';
COMMENT ON FUNCTION ops.purge_terminal_jobs(interval, interval, interval, integer) IS
  'ADR-0062 D-J: deletes at most p_limit terminal jobs of the calling tenant past their retention, keeping budget-window, contribution-linked and R4-redrivable jobs, with one receipt; EXECUTE role_maintenance only.';
