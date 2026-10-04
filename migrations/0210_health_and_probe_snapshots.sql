-- §46 (REVERSIBLE; card 34, ADR-0061 D-D / D-J). Two read-only aggregate SECURITY DEFINER functions and the
-- NOLOGIN role that owns them:
--   ops.health_snapshot(timestamptz)  caller: humaux-maintenance `health serve` (adapters::health::read_health_snapshot),
--                                     fence: EXECUTE role_maintenance only; feeds the §41.2 SQL gauges.
--   ops.admin_probe_snapshot()        caller: humaux-admin §4.4 probes (adapters::health::read_admin_probe_snapshot),
--                                     fence: EXECUTE role_admin only (§6.2.2 note, ADR-0061 E11).
-- Both return one row of cross-tenant aggregates and no tenant id.
--
-- ADR-0061 D-D: the owner is role_health_reader, never role_migration_owner. A permissive owner-wide read policy
-- would be ORed into every existing owner definer that reads these tables (0115 ops.attach_retrieval_query_source,
-- 0132 joins ops.data_disclosures) and strip their FORCE-RLS tenant filter. role_health_reader is NOLOGIN with no
-- members, so no session can become it; its policies test current_user, which inside the definers is the reader.
-- `TO PUBLIC` (not `TO role_health_reader`): projection.processing_gaps is not security_invoker (0007:56), so
-- stream_log's policies are selected for the view owner, while current_user stays role_health_reader.

-- Cluster-global: another database of the same cluster may already have created it (0110 pattern). The 0210
-- postcheck proves its flags and empty membership either way.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_health_reader') THEN
    CREATE ROLE role_health_reader NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
      NOBYPASSRLS;
  END IF;
END $$;

GRANT USAGE ON SCHEMA ops, projection TO role_health_reader;
GRANT SELECT ON ops.jobs, ops.data_disclosures, ops.outbox, projection.stream_checkpoints,
  projection.processing_gaps TO role_health_reader;

CREATE POLICY jobs_health_reader_read ON ops.jobs
  AS PERMISSIVE FOR SELECT TO PUBLIC USING (current_user = 'role_health_reader');
CREATE POLICY data_disclosures_health_reader_read ON ops.data_disclosures
  AS PERMISSIVE FOR SELECT TO PUBLIC USING (current_user = 'role_health_reader');
CREATE POLICY outbox_health_reader_read ON ops.outbox
  AS PERMISSIVE FOR SELECT TO PUBLIC USING (current_user = 'role_health_reader');
CREATE POLICY stream_checkpoints_health_reader_read ON projection.stream_checkpoints
  AS PERMISSIVE FOR SELECT TO PUBLIC USING (current_user = 'role_health_reader');
-- Read only through projection.processing_gaps (no table grant on stream_log for the reader).
CREATE POLICY stream_log_health_reader_read ON projection.stream_log
  AS PERMISSIVE FOR SELECT TO PUBLIC USING (current_user = 'role_health_reader');

-- §41.2 health families. Every statement is index-backed (0211-0213, 0047's open-reservation index) except the
-- stream_checkpoints sum, one row per stream. ponytail: O(streams) seq scan; index when streams > ~10^5.
-- `processing_gaps` is the only definition of a gap (§15.2); this body never restates its state set.
CREATE FUNCTION ops.health_snapshot(p_finalized_since timestamptz)
RETURNS TABLE (
  as_of timestamptz,
  jobs_pending bigint,
  jobs_processing bigint,
  jobs_waiting_key bigint,
  jobs_dead bigint,
  oldest_pending_age_seconds double precision,
  projection_lag_events bigint,
  gap_domains text[],
  gap_projection_kinds text[],
  gap_counts bigint[],
  reserved_le_10s bigint,
  reserved_le_60s bigint,
  reserved_gt_60s bigint,
  finalized_outcomes text[],
  finalized_counts bigint[]
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, ops, projection
AS $fn$
  WITH jobs AS (
    SELECT count(*) FILTER (WHERE j.status = 'PENDING') AS pending,
           count(*) FILTER (WHERE j.status = 'PROCESSING') AS processing,
           count(*) FILTER (WHERE j.status = 'WAITING_KEY') AS waiting_key,
           count(*) FILTER (WHERE j.status = 'DEAD') AS dead,
           coalesce(extract(epoch FROM now() - min(j.created_at) FILTER (WHERE j.status = 'PENDING')), 0)
             ::double precision AS oldest_pending
      FROM ops.jobs j
     WHERE j.status IN ('PENDING', 'PROCESSING', 'WAITING_KEY', 'DEAD')
  ), lag AS (
    SELECT coalesce(sum(c.issued_highwater - c.projection_highwater), 0)::bigint AS events
      FROM projection.stream_checkpoints c
  ), gaps AS (
    SELECT coalesce(array_agg(g.domain ORDER BY g.domain, g.projection_kind), '{}') AS domains,
           coalesce(array_agg(g.projection_kind ORDER BY g.domain, g.projection_kind), '{}') AS kinds,
           coalesce(array_agg(g.n ORDER BY g.domain, g.projection_kind), '{}') AS counts
      FROM (SELECT p.domain, p.projection_kind, count(*) AS n
              FROM projection.processing_gaps p
             GROUP BY p.domain, p.projection_kind) g
  ), open_reservations AS (
    SELECT count(*) FILTER (WHERE now() - d.reserved_at <= interval '10 seconds') AS le_10s,
           count(*) FILTER (WHERE now() - d.reserved_at > interval '10 seconds'
                              AND now() - d.reserved_at <= interval '60 seconds') AS le_60s,
           count(*) FILTER (WHERE now() - d.reserved_at > interval '60 seconds') AS gt_60s
      FROM ops.data_disclosures d
     WHERE d.finalized_at IS NULL
  ), finalized AS (
    SELECT coalesce(array_agg(f.outcome ORDER BY f.outcome), '{}') AS outcomes,
           coalesce(array_agg(f.n ORDER BY f.outcome), '{}') AS counts
      FROM (SELECT d.outcome, count(*) AS n
              FROM ops.data_disclosures d
             WHERE d.finalized_at > p_finalized_since AND d.finalized_at <= now()
             GROUP BY d.outcome) f
  )
  SELECT now(), jobs.pending, jobs.processing, jobs.waiting_key, jobs.dead, jobs.oldest_pending, lag.events,
         gaps.domains, gaps.kinds, gaps.counts,
         open_reservations.le_10s, open_reservations.le_60s, open_reservations.gt_60s,
         finalized.outcomes, finalized.counts
    FROM jobs, lag, gaps, open_reservations, finalized
$fn$;

ALTER FUNCTION ops.health_snapshot(timestamptz) OWNER TO role_health_reader;
REVOKE ALL ON FUNCTION ops.health_snapshot(timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.health_snapshot(timestamptz) TO role_maintenance;

-- §4.4 / ADR-0037 unlock as an aggregate definer: operator-invoked, never on a scrape path.
-- ponytail: O(table) counts on demand; a cached sample if an operator ever polls it.
CREATE FUNCTION ops.admin_probe_snapshot()
RETURNS TABLE (
  as_of timestamptz,
  stream_domains text[],
  stream_projection_kinds text[],
  stream_counts bigint[],
  stream_lagging bigint[],
  stream_lag_totals bigint[],
  stream_lag_max bigint[],
  outbox_total bigint,
  outbox_undelivered bigint,
  outbox_oldest_undelivered_age_seconds double precision,
  jobs_total bigint,
  jobs_stuck bigint,
  jobs_in_lease bigint
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, ops, projection
AS $fn$
  WITH streams AS (
    SELECT coalesce(array_agg(s.domain ORDER BY s.domain, s.projection_kind), '{}') AS domains,
           coalesce(array_agg(s.projection_kind ORDER BY s.domain, s.projection_kind), '{}') AS kinds,
           coalesce(array_agg(s.n ORDER BY s.domain, s.projection_kind), '{}') AS counts,
           coalesce(array_agg(s.lagging ORDER BY s.domain, s.projection_kind), '{}') AS lagging,
           coalesce(array_agg(s.lag_total ORDER BY s.domain, s.projection_kind), '{}') AS lag_totals,
           coalesce(array_agg(s.lag_max ORDER BY s.domain, s.projection_kind), '{}') AS lag_max
      FROM (SELECT c.domain, c.projection_kind, count(*) AS n,
                   count(*) FILTER (WHERE c.projection_highwater < c.issued_highwater) AS lagging,
                   sum(c.issued_highwater - c.projection_highwater)::bigint AS lag_total,
                   max(c.issued_highwater - c.projection_highwater)::bigint AS lag_max
              FROM projection.stream_checkpoints c
             GROUP BY c.domain, c.projection_kind) s
  ), outbox AS (
    SELECT count(*) AS total,
           count(*) FILTER (WHERE o.status IN ('PENDING', 'PROCESSING')) AS undelivered,
           extract(epoch FROM now() - min(o.created_at) FILTER (WHERE o.status IN ('PENDING', 'PROCESSING')))
             ::double precision AS oldest_undelivered
      FROM ops.outbox o
  ), jobs AS (
    SELECT count(*) AS total,
           count(*) FILTER (WHERE j.status = 'PROCESSING' AND j.lease_expires_at < now()) AS stuck,
           count(*) FILTER (WHERE j.status = 'PROCESSING' AND j.lease_expires_at >= now()) AS in_lease
      FROM ops.jobs j
  )
  SELECT now(), streams.domains, streams.kinds, streams.counts, streams.lagging, streams.lag_totals,
         streams.lag_max, outbox.total, outbox.undelivered, outbox.oldest_undelivered,
         jobs.total, jobs.stuck, jobs.in_lease
    FROM streams, outbox, jobs
$fn$;

ALTER FUNCTION ops.admin_probe_snapshot() OWNER TO role_health_reader;
REVOKE ALL ON FUNCTION ops.admin_probe_snapshot() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.admin_probe_snapshot() TO role_admin;
