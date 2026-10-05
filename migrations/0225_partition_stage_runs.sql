-- §46 FORWARD_ONLY (card 36 S2; ADR-0063 D-D). ops.stage_runs becomes a RANGE (started_at) parent under its own
-- name: the heap is renamed, stripped, attached as one leaf with bounds derived from its data, sealed and registered
-- by control.partition_adopt_leaf, and the current UTC month plus 3 are pre-created by control.partition_create_month
-- (0224). No row is copied or rewritten; no DEFAULT partition. Partition key = server-recorded `started_at`; runtime
-- updates of it are refused by the key-freeze trigger (R-36(a)).
--
-- Contract change (ADR-0063 E3): PK (stage_run_id) -> (stage_run_id, started_at). Nothing references ops.stage_runs
-- (precheck), so no FK moves and no identity table is needed (L10).
--
-- Locks: ACCESS EXCLUSIVE on ops.stage_runs (taken first, so no lock upgrade can deadlock with a writer) and SHARE
--   ROW EXCLUSIVE on control.tenants (the re-created FK), both for the whole transaction. Measured on a throwaway
--   restored from a pg_dump of dev (0 rows, card 36 S2, 2026-10-05): 18 ms (ADR-0063 "Conversion wall clock").

-- D-D 7b precheck, repeated as body for the test throwaways that apply bodies without manifests.
DO $$
BEGIN
  IF (SELECT c.relkind FROM pg_class c WHERE c.oid = 'ops.stage_runs'::regclass) <> 'r' THEN
    RAISE EXCEPTION 'c36 precheck: ops.stage_runs is not a plain heap';
  END IF;
  -- F10: a view or BEGIN ATOMIC body would stay bound to the renamed heap.
  IF EXISTS (SELECT 1 FROM pg_depend d
              WHERE d.refobjid = 'ops.stage_runs'::regclass
                AND d.classid IN ('pg_rewrite'::regclass, 'pg_proc'::regclass)) THEN
    RAISE EXCEPTION 'c36 precheck: a view or function body depends on ops.stage_runs';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_constraint co WHERE co.contype = 'f' AND co.confrelid = 'ops.stage_runs'::regclass) THEN
    RAISE EXCEPTION 'c36 precheck: a foreign key references ops.stage_runs';
  END IF;
END
$$;

LOCK TABLE ops.stage_runs IN ACCESS EXCLUSIVE MODE;

-- D-D step 1: the snapshot only the 7a block below reads (never a manifest check: migration-rehearsal EXPLAINs those
-- at HEAD, where these temp tables do not exist).
CREATE TEMP TABLE c36_pre_stage_runs ON COMMIT DROP AS SELECT count(*) AS n FROM ops.stage_runs;
CREATE TEMP TABLE c36_fp_stage_runs ON COMMIT DROP AS
  SELECT * FROM control.partition_catalog_fingerprint('ops.stage_runs');

-- D-D step 3: the parent re-creates policy, PK and FK; ATTACH clones the FK and index onto the leaf.
ALTER TABLE ops.stage_runs RENAME TO stage_runs_legacy;
DROP POLICY stage_runs_tenant_isolation ON ops.stage_runs_legacy;
ALTER TABLE ops.stage_runs_legacy DROP CONSTRAINT stage_runs_pkey;
ALTER TABLE ops.stage_runs_legacy DROP CONSTRAINT stage_runs_tenant_id_fkey;

-- D-D step 4: LIKE is not enough (R-36); everything else is explicit.
CREATE TABLE ops.stage_runs (
  LIKE ops.stage_runs_legacy INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING GENERATED INCLUDING STORAGE
    INCLUDING COMMENTS INCLUDING COMPRESSION,
  CONSTRAINT stage_runs_pkey PRIMARY KEY (stage_run_id, started_at)
) PARTITION BY RANGE (started_at);
ALTER TABLE ops.stage_runs
  ADD CONSTRAINT stage_runs_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES control.tenants (tenant_id);
ALTER TABLE ops.stage_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.stage_runs FORCE ROW LEVEL SECURITY;
CREATE POLICY stage_runs_tenant_isolation ON ops.stage_runs
  USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);
-- R-36(a): four runtime roles hold UPDATE here; a row never moves between monthly leaves.
CREATE TRIGGER stage_runs_partition_key_frozen BEFORE UPDATE ON ops.stage_runs
  FOR EACH ROW WHEN (OLD.started_at IS DISTINCT FROM NEW.started_at)
  EXECUTE FUNCTION control.reject_partition_key_update();
ALTER TABLE ops.stage_runs OWNER TO role_migration_owner;
-- 0011's default privileges fired for the migrate principal: revoke them, then the exact legacy cells (§6.2.2).
REVOKE ALL ON ops.stage_runs
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;
GRANT SELECT, INSERT, UPDATE ON ops.stage_runs
  TO role_gateway, role_private_worker, role_public_worker, role_retrieval_worker;
GRANT SELECT ON ops.stage_runs TO role_consolidation_worker, role_maintenance;

-- D-D steps 5-6: bounds from data, all UTC. B = the month after max(key, now()); L = MINVALUE when history predates
-- the current month (the transitional `_hist` leaf, L4), else the current month (an empty table: production).
DO $$
DECLARE
  v_month timestamptz := date_trunc('month', now(), 'UTC');
  v_min   timestamptz;
  v_max   timestamptz;
  v_lower timestamptz;
  v_upper timestamptz;
  v_leaf  text;
  v_rel   regclass;
BEGIN
  SELECT min(started_at), max(started_at) INTO v_min, v_max FROM ops.stage_runs_legacy;
  v_upper := date_add(date_trunc('month', greatest(v_max, now()), 'UTC'), interval '1 month', 'UTC');
  v_lower := CASE WHEN v_min < v_month THEN NULL ELSE v_month END;
  -- `_pYYYYMM` only for exactly one UTC month; anything wider is the transitional `_hist` leaf (L4). Renamed before
  -- ATTACH so the indexes ATTACH builds carry the leaf's own name.
  v_leaf := CASE WHEN v_lower IS NOT NULL AND v_upper = date_add(v_lower, interval '1 month', 'UTC')
                 THEN 'stage_runs_p' || to_char(v_lower AT TIME ZONE 'UTC', 'YYYYMM')
                 ELSE 'stage_runs_hist' END;
  EXECUTE format('ALTER TABLE ops.stage_runs_legacy RENAME TO %I', v_leaf);
  v_rel := format('ops.%I', v_leaf)::regclass;
  -- A valid implying CHECK lets ATTACH skip its scan (P7); the partition constraint replaces it afterwards.
  EXECUTE format('ALTER TABLE %s ADD CONSTRAINT stage_runs_legacy_bound'
                 ' CHECK (started_at IS NOT NULL AND started_at < %L%s) NOT VALID', v_rel, v_upper,
                 CASE WHEN v_lower IS NULL THEN '' ELSE format(' AND started_at >= %L', v_lower) END);
  EXECUTE format('ALTER TABLE %s VALIDATE CONSTRAINT stage_runs_legacy_bound', v_rel);
  EXECUTE format('ALTER TABLE ops.stage_runs ATTACH PARTITION %s FOR VALUES FROM (%s) TO (%L)', v_rel,
                 coalesce(quote_literal(v_lower), 'MINVALUE'), v_upper);
  EXECUTE format('ALTER TABLE %s DROP CONSTRAINT stage_runs_legacy_bound', v_rel);
  PERFORM control.partition_adopt_leaf('STAGE_RUNS', v_rel);
  -- D-F: the current month plus 3 future months; never a DEFAULT partition.
  WHILE v_upper < date_add(v_month, interval '4 months', 'UTC') LOOP
    PERFORM control.partition_create_month('STAGE_RUNS', v_upper);
    v_upper := date_add(v_upper, interval '1 month', 'UTC');
  END LOOP;
END
$$;

-- D-D 7a: last statement. Rows and the normalised catalog (columns, CHECK/FK, triggers, policies, RLS flags, owner,
-- ACL) must equal the snapshot, the declared key-freeze trigger aside; any drift aborts the whole transaction.
DO $$
DECLARE
  v_drift text;
BEGIN
  IF (SELECT count(*) FROM ops.stage_runs) <> (SELECT p.n FROM c36_pre_stage_runs p) THEN
    RAISE EXCEPTION 'c36 count drift: ops.stage_runs % rows, % before', (SELECT count(*) FROM ops.stage_runs),
      (SELECT p.n FROM c36_pre_stage_runs p);
  END IF;
  CREATE TEMP TABLE c36_fp_now ON COMMIT DROP AS
    SELECT * FROM control.partition_catalog_fingerprint('ops.stage_runs') f
     WHERE NOT (f.kind = 'trigger' AND starts_with(f.item, 'CREATE TRIGGER stage_runs_partition_key_frozen '));
  IF (SELECT count(*) FROM control.partition_catalog_fingerprint('ops.stage_runs')) - (SELECT count(*) FROM c36_fp_now)
     <> 1 THEN
    RAISE EXCEPTION 'c36 fingerprint drift: ops.stage_runs key-freeze trigger stage_runs_partition_key_frozen missing';
  END IF;
  SELECT format('%s %s: %s', d.side, d.kind, d.item) INTO v_drift
    FROM ((SELECT 'missing' AS side, s.kind, s.item FROM c36_fp_stage_runs s
           EXCEPT ALL SELECT 'missing', n.kind, n.item FROM c36_fp_now n)
          UNION ALL
          (SELECT 'unexpected', n.kind, n.item FROM c36_fp_now n
           EXCEPT ALL SELECT 'unexpected', s.kind, s.item FROM c36_fp_stage_runs s)) d
   ORDER BY d.side, d.kind, d.item LIMIT 1;
  IF v_drift IS NOT NULL THEN
    RAISE EXCEPTION 'c36 fingerprint drift: ops.stage_runs %', v_drift;
  END IF;
  DROP TABLE c36_fp_now;
END
$$;
