-- §46 FORWARD_ONLY (card 36 S2; ADR-0063 D-D). private.messages becomes a RANGE (created_at) parent under its own
-- name: the heap is renamed, stripped, attached as one leaf with bounds derived from its data, sealed and registered
-- by control.partition_adopt_leaf, and the current UTC month plus 3 are pre-created by control.partition_create_month
-- (0224). No row is copied or rewritten; no DEFAULT partition. Partition key = server-recorded `created_at`; runtime
-- updates of it are refused by the key-freeze trigger (R-36(a)).
--
-- Contract change (ADR-0063 E3): PK (message_id) -> (message_id, created_at). Nothing references private.messages
-- (precheck), so no FK moves and no identity table is needed (L10). Its outbound FK to private.events(event_id) is
-- re-created unchanged here; 0228 re-points it to private.event_identity.
--
-- Locks: ACCESS EXCLUSIVE on private.messages (taken first, so no lock upgrade can deadlock with a writer) and SHARE
--   ROW EXCLUSIVE on control.tenants, private.conversations and private.events (the three re-created FKs), all for
--   the whole transaction. Measured on a throwaway restored from a pg_dump of dev (0 rows, card 36 S2,
--   2026-10-05): 14 ms (ADR-0063 "Conversion wall clock").

-- D-D 7b precheck, repeated as body for the test throwaways that apply bodies without manifests.
DO $$
BEGIN
  IF (SELECT c.relkind FROM pg_class c WHERE c.oid = 'private.messages'::regclass) <> 'r' THEN
    RAISE EXCEPTION 'c36 precheck: private.messages is not a plain heap';
  END IF;
  -- F10: a view or BEGIN ATOMIC body would stay bound to the renamed heap.
  IF EXISTS (SELECT 1 FROM pg_depend d
              WHERE d.refobjid = 'private.messages'::regclass
                AND d.classid IN ('pg_rewrite'::regclass, 'pg_proc'::regclass)) THEN
    RAISE EXCEPTION 'c36 precheck: a view or function body depends on private.messages';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_constraint co WHERE co.contype = 'f' AND co.confrelid = 'private.messages'::regclass) THEN
    RAISE EXCEPTION 'c36 precheck: a foreign key references private.messages';
  END IF;
END
$$;

LOCK TABLE private.messages IN ACCESS EXCLUSIVE MODE;

-- D-D step 1: the snapshot only the 7a block below reads (never a manifest check: migration-rehearsal EXPLAINs those
-- at HEAD, where these temp tables do not exist).
CREATE TEMP TABLE c36_pre_messages ON COMMIT DROP AS SELECT count(*) AS n FROM private.messages;
CREATE TEMP TABLE c36_fp_messages ON COMMIT DROP AS
  SELECT * FROM control.partition_catalog_fingerprint('private.messages');

-- D-D step 3: the parent re-creates policy, PK and FK; ATTACH clones the FK and index onto the leaf.
ALTER TABLE private.messages RENAME TO messages_legacy;
DROP POLICY messages_tenant_isolation ON private.messages_legacy;
ALTER TABLE private.messages_legacy DROP CONSTRAINT messages_pkey;
ALTER TABLE private.messages_legacy DROP CONSTRAINT messages_conversation_id_fkey;
ALTER TABLE private.messages_legacy DROP CONSTRAINT messages_event_id_fkey;
ALTER TABLE private.messages_legacy DROP CONSTRAINT messages_tenant_id_fkey;

-- D-D step 4: LIKE is not enough (R-36); everything else is explicit.
CREATE TABLE private.messages (
  LIKE private.messages_legacy INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING GENERATED INCLUDING STORAGE
    INCLUDING COMMENTS INCLUDING COMPRESSION,
  CONSTRAINT messages_pkey PRIMARY KEY (message_id, created_at)
) PARTITION BY RANGE (created_at);
ALTER TABLE private.messages
  ADD CONSTRAINT messages_conversation_id_fkey
    FOREIGN KEY (conversation_id) REFERENCES private.conversations (conversation_id),
  ADD CONSTRAINT messages_event_id_fkey FOREIGN KEY (event_id) REFERENCES private.events (event_id),
  ADD CONSTRAINT messages_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES control.tenants (tenant_id);
ALTER TABLE private.messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.messages FORCE ROW LEVEL SECURITY;
CREATE POLICY messages_tenant_isolation ON private.messages
  USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);
-- R-36(a): two runtime roles hold UPDATE here; a row never moves between monthly leaves.
CREATE TRIGGER messages_partition_key_frozen BEFORE UPDATE ON private.messages
  FOR EACH ROW WHEN (OLD.created_at IS DISTINCT FROM NEW.created_at)
  EXECUTE FUNCTION control.reject_partition_key_update();
ALTER TABLE private.messages OWNER TO role_migration_owner;
-- 0011's default privileges fired for the migrate principal: revoke them, then the exact legacy cells (§6.2.2).
REVOKE ALL ON private.messages
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;
GRANT SELECT, INSERT, UPDATE ON private.messages TO role_gateway, role_private_worker;
GRANT SELECT ON private.messages TO role_consolidation_worker, role_retrieval_worker, role_maintenance;

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
  SELECT min(created_at), max(created_at) INTO v_min, v_max FROM private.messages_legacy;
  v_upper := date_add(date_trunc('month', greatest(v_max, now()), 'UTC'), interval '1 month', 'UTC');
  v_lower := CASE WHEN v_min < v_month THEN NULL ELSE v_month END;
  -- `_pYYYYMM` only for exactly one UTC month; anything wider is the transitional `_hist` leaf (L4). Renamed before
  -- ATTACH so the indexes ATTACH builds carry the leaf's own name.
  v_leaf := CASE WHEN v_lower IS NOT NULL AND v_upper = date_add(v_lower, interval '1 month', 'UTC')
                 THEN 'messages_p' || to_char(v_lower AT TIME ZONE 'UTC', 'YYYYMM')
                 ELSE 'messages_hist' END;
  EXECUTE format('ALTER TABLE private.messages_legacy RENAME TO %I', v_leaf);
  v_rel := format('private.%I', v_leaf)::regclass;
  -- A valid implying CHECK lets ATTACH skip its scan (P7); the partition constraint replaces it afterwards.
  EXECUTE format('ALTER TABLE %s ADD CONSTRAINT messages_legacy_bound'
                 ' CHECK (created_at IS NOT NULL AND created_at < %L%s) NOT VALID', v_rel, v_upper,
                 CASE WHEN v_lower IS NULL THEN '' ELSE format(' AND created_at >= %L', v_lower) END);
  EXECUTE format('ALTER TABLE %s VALIDATE CONSTRAINT messages_legacy_bound', v_rel);
  EXECUTE format('ALTER TABLE private.messages ATTACH PARTITION %s FOR VALUES FROM (%s) TO (%L)', v_rel,
                 coalesce(quote_literal(v_lower), 'MINVALUE'), v_upper);
  EXECUTE format('ALTER TABLE %s DROP CONSTRAINT messages_legacy_bound', v_rel);
  PERFORM control.partition_adopt_leaf('MESSAGES', v_rel);
  -- D-F: the current month plus 3 future months; never a DEFAULT partition.
  WHILE v_upper < date_add(v_month, interval '4 months', 'UTC') LOOP
    PERFORM control.partition_create_month('MESSAGES', v_upper);
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
  IF (SELECT count(*) FROM private.messages) <> (SELECT p.n FROM c36_pre_messages p) THEN
    RAISE EXCEPTION 'c36 count drift: private.messages % rows, % before', (SELECT count(*) FROM private.messages),
      (SELECT p.n FROM c36_pre_messages p);
  END IF;
  CREATE TEMP TABLE c36_fp_now ON COMMIT DROP AS
    SELECT * FROM control.partition_catalog_fingerprint('private.messages') f
     WHERE NOT (f.kind = 'trigger' AND starts_with(f.item, 'CREATE TRIGGER messages_partition_key_frozen '));
  IF (SELECT count(*) FROM control.partition_catalog_fingerprint('private.messages'))
     - (SELECT count(*) FROM c36_fp_now) <> 1 THEN
    RAISE EXCEPTION 'c36 fingerprint drift: private.messages key-freeze trigger messages_partition_key_frozen missing';
  END IF;
  SELECT format('%s %s: %s', d.side, d.kind, d.item) INTO v_drift
    FROM ((SELECT 'missing' AS side, s.kind, s.item FROM c36_fp_messages s
           EXCEPT ALL SELECT 'missing', n.kind, n.item FROM c36_fp_now n)
          UNION ALL
          (SELECT 'unexpected', n.kind, n.item FROM c36_fp_now n
           EXCEPT ALL SELECT 'unexpected', s.kind, s.item FROM c36_fp_messages s)) d
   ORDER BY d.side, d.kind, d.item LIMIT 1;
  IF v_drift IS NOT NULL THEN
    RAISE EXCEPTION 'c36 fingerprint drift: private.messages %', v_drift;
  END IF;
  DROP TABLE c36_fp_now;
END
$$;
