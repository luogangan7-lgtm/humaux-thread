-- §46 FORWARD_ONLY (card 36 S2; ADR-0063 D-D). ops.maintenance_receipts becomes a RANGE (ran_at) parent under its
-- own name: the heap is renamed, stripped, attached as one leaf with bounds derived from its data, sealed and
-- registered by control.partition_adopt_leaf, and the current UTC month plus 3 are pre-created by
-- control.partition_create_month (0224). No row is copied or rewritten; no DEFAULT partition. Partition key =
-- server-recorded `ran_at` (clock_timestamp() default); no runtime role holds UPDATE, so no key-freeze trigger is
-- needed. Card 35's purge doors (0218, LANGUAGE sql, late-bound) insert through the parent unchanged. Added to
-- §48.1 by ADR-0062 E6.
--
-- Contract change (ADR-0063 E3): PK (receipt_id) -> (receipt_id, ran_at). Nothing references ops.maintenance_receipts
-- (precheck), so no FK moves and no identity table is needed (L10).
--
-- Locks: ACCESS EXCLUSIVE on ops.maintenance_receipts (taken first, so no lock upgrade can deadlock with a writer)
--   and SHARE ROW EXCLUSIVE on control.tenants (the re-created FK), both for the whole transaction. Measured on a
--   throwaway restored from a pg_dump of dev (0 rows, card 36 S2, 2026-10-05): at most 40 ms including the migrate
--   process exit (ADR-0063 "Conversion wall clock").

-- D-D 7b precheck, repeated as body for the test throwaways that apply bodies without manifests.
DO $$
BEGIN
  IF (SELECT c.relkind FROM pg_class c WHERE c.oid = 'ops.maintenance_receipts'::regclass) <> 'r' THEN
    RAISE EXCEPTION 'c36 precheck: ops.maintenance_receipts is not a plain heap';
  END IF;
  -- F10: a view or BEGIN ATOMIC body would stay bound to the renamed heap.
  IF EXISTS (SELECT 1 FROM pg_depend d
              WHERE d.refobjid = 'ops.maintenance_receipts'::regclass
                AND d.classid IN ('pg_rewrite'::regclass, 'pg_proc'::regclass)) THEN
    RAISE EXCEPTION 'c36 precheck: a view or function body depends on ops.maintenance_receipts';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_constraint co
              WHERE co.contype = 'f' AND co.confrelid = 'ops.maintenance_receipts'::regclass) THEN
    RAISE EXCEPTION 'c36 precheck: a foreign key references ops.maintenance_receipts';
  END IF;
END
$$;

LOCK TABLE ops.maintenance_receipts IN ACCESS EXCLUSIVE MODE;

-- D-D step 1: the snapshot only the 7a block below reads (never a manifest check: migration-rehearsal EXPLAINs those
-- at HEAD, where these temp tables do not exist).
CREATE TEMP TABLE c36_pre_maintenance_receipts ON COMMIT DROP AS SELECT count(*) AS n FROM ops.maintenance_receipts;
CREATE TEMP TABLE c36_fp_maintenance_receipts ON COMMIT DROP AS
  SELECT * FROM control.partition_catalog_fingerprint('ops.maintenance_receipts');

-- D-D step 3: the parent re-creates policy, PK, FK and index; ATTACH clones the FK and indexes onto the leaf.
ALTER TABLE ops.maintenance_receipts RENAME TO maintenance_receipts_legacy;
DROP POLICY maintenance_receipts_tenant_isolation ON ops.maintenance_receipts_legacy;
ALTER TABLE ops.maintenance_receipts_legacy DROP CONSTRAINT maintenance_receipts_pkey;
ALTER TABLE ops.maintenance_receipts_legacy DROP CONSTRAINT maintenance_receipts_tenant_id_fkey;
DROP INDEX ops.maintenance_receipts_tenant_ran_at_idx;

-- D-D step 4: LIKE is not enough (R-36); everything else is explicit.
CREATE TABLE ops.maintenance_receipts (
  LIKE ops.maintenance_receipts_legacy INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING GENERATED INCLUDING STORAGE
    INCLUDING COMMENTS INCLUDING COMPRESSION,
  CONSTRAINT maintenance_receipts_pkey PRIMARY KEY (receipt_id, ran_at)
) PARTITION BY RANGE (ran_at);
ALTER TABLE ops.maintenance_receipts
  ADD CONSTRAINT maintenance_receipts_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES control.tenants (tenant_id);
CREATE INDEX maintenance_receipts_tenant_ran_at_idx ON ops.maintenance_receipts (tenant_id, ran_at);
ALTER TABLE ops.maintenance_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.maintenance_receipts FORCE ROW LEVEL SECURITY;
CREATE POLICY maintenance_receipts_tenant_isolation ON ops.maintenance_receipts
  USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);
ALTER TABLE ops.maintenance_receipts OWNER TO role_migration_owner;
-- 0011's default privileges fired for the migrate principal: revoke them, then the exact legacy cells (§6.2.2).
REVOKE ALL ON ops.maintenance_receipts
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;
GRANT SELECT ON ops.maintenance_receipts TO role_maintenance;
COMMENT ON TABLE ops.maintenance_receipts IS
  'ADR-0062 D-F: one row per purge-door call that removed rows, written in the same statement as the delete.';

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
  SELECT min(ran_at), max(ran_at) INTO v_min, v_max FROM ops.maintenance_receipts_legacy;
  v_upper := date_add(date_trunc('month', greatest(v_max, now()), 'UTC'), interval '1 month', 'UTC');
  v_lower := CASE WHEN v_min < v_month THEN NULL ELSE v_month END;
  -- `_pYYYYMM` only for exactly one UTC month; anything wider is the transitional `_hist` leaf (L4). Renamed before
  -- ATTACH so the indexes ATTACH builds carry the leaf's own name.
  v_leaf := CASE WHEN v_lower IS NOT NULL AND v_upper = date_add(v_lower, interval '1 month', 'UTC')
                 THEN 'maintenance_receipts_p' || to_char(v_lower AT TIME ZONE 'UTC', 'YYYYMM')
                 ELSE 'maintenance_receipts_hist' END;
  EXECUTE format('ALTER TABLE ops.maintenance_receipts_legacy RENAME TO %I', v_leaf);
  v_rel := format('ops.%I', v_leaf)::regclass;
  -- A valid implying CHECK lets ATTACH skip its scan (P7); the partition constraint replaces it afterwards.
  EXECUTE format('ALTER TABLE %s ADD CONSTRAINT maintenance_receipts_legacy_bound'
                 ' CHECK (ran_at IS NOT NULL AND ran_at < %L%s) NOT VALID', v_rel, v_upper,
                 CASE WHEN v_lower IS NULL THEN '' ELSE format(' AND ran_at >= %L', v_lower) END);
  EXECUTE format('ALTER TABLE %s VALIDATE CONSTRAINT maintenance_receipts_legacy_bound', v_rel);
  EXECUTE format('ALTER TABLE ops.maintenance_receipts ATTACH PARTITION %s FOR VALUES FROM (%s) TO (%L)', v_rel,
                 coalesce(quote_literal(v_lower), 'MINVALUE'), v_upper);
  EXECUTE format('ALTER TABLE %s DROP CONSTRAINT maintenance_receipts_legacy_bound', v_rel);
  PERFORM control.partition_adopt_leaf('MAINTENANCE_RECEIPTS', v_rel);
  -- D-F: the current month plus 3 future months; never a DEFAULT partition.
  WHILE v_upper < date_add(v_month, interval '4 months', 'UTC') LOOP
    PERFORM control.partition_create_month('MAINTENANCE_RECEIPTS', v_upper);
    v_upper := date_add(v_upper, interval '1 month', 'UTC');
  END LOOP;
END
$$;

-- D-D 7a: last statement. Rows and the normalised catalog (columns, CHECK/FK, triggers, policies, RLS flags, owner,
-- ACL) must equal the snapshot exactly (no declared addition here); any drift aborts the whole transaction.
DO $$
DECLARE
  v_drift text;
BEGIN
  IF (SELECT count(*) FROM ops.maintenance_receipts) <> (SELECT p.n FROM c36_pre_maintenance_receipts p) THEN
    RAISE EXCEPTION 'c36 count drift: ops.maintenance_receipts % rows, % before',
      (SELECT count(*) FROM ops.maintenance_receipts), (SELECT p.n FROM c36_pre_maintenance_receipts p);
  END IF;
  SELECT format('%s %s: %s', d.side, d.kind, d.item) INTO v_drift
    FROM ((SELECT 'missing' AS side, s.kind, s.item FROM c36_fp_maintenance_receipts s
           EXCEPT ALL SELECT 'missing', n.kind, n.item
                        FROM control.partition_catalog_fingerprint('ops.maintenance_receipts') n)
          UNION ALL
          (SELECT 'unexpected', n.kind, n.item FROM control.partition_catalog_fingerprint('ops.maintenance_receipts') n
           EXCEPT ALL SELECT 'unexpected', s.kind, s.item FROM c36_fp_maintenance_receipts s)) d
   ORDER BY d.side, d.kind, d.item LIMIT 1;
  IF v_drift IS NOT NULL THEN
    RAISE EXCEPTION 'c36 fingerprint drift: ops.maintenance_receipts %', v_drift;
  END IF;
END
$$;
