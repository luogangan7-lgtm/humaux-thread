-- §46 FORWARD_ONLY (card 36 S3; ADR-0063 D-B, D-D). private.events becomes a RANGE (recorded_at) parent under its own
-- name: the heap is renamed, stripped, attached as one leaf with bounds derived from its data, sealed and registered
-- by control.partition_adopt_leaf, and the current UTC month plus 3 are pre-created by control.partition_create_month
-- (0224). No row is copied; the only rewrite is the recorded_at backfill. No DEFAULT partition.
--
-- Contract changes (ADR-0063 E3):
--   * new column recorded_at timestamptz NOT NULL DEFAULT now(), the partition key (server-recorded time, R-36(a));
--     legacy rows take their evidence's created_at (L12). No runtime role holds UPDATE on private.events, so no
--     key-freeze trigger is needed.
--   * PK (event_id) -> (event_id, recorded_at). Global event_id uniqueness moves to the narrow non-partitioned
--     private.event_identity (event_id PK REFERENCES private.evidence_objects), filled by the BEFORE INSERT claim
--     trigger: a second event for one evidence raises 23505 exactly as the old PK did.
--   * the two inbound FKs ingest_tickets.redeemed_event_id and messages.event_id are re-pointed, column lists and
--     names unchanged, from private.events to private.event_identity (D-B invariant 1: no FK into a partitioned table).
--   * a row DELETE of an event (owner / superuser only; no runtime role holds DELETE) releases its identity row
--     through the AFTER DELETE trigger, so a referenced event still refuses deletion with 23503 and an unreferenced
--     event and its evidence still delete as before. A partition drop fires no row trigger: identity rows of dropped
--     months persist as tombstones (D-B invariant 3).
--
-- Functions (caller / fence / owner role_migration_owner; EXECUTE revoked from PUBLIC, no runtime grant):
--   private.event_identity_claim()    caller: trigger events_identity_claim (BEFORE INSERT on private.events). Fence:
--                                     inserts exactly NEW.event_id. SECURITY DEFINER (role_gateway writes events but
--                                     holds nothing on the identity table).
--   private.event_identity_release()  caller: trigger events_identity_release (AFTER DELETE on private.events). Fence:
--                                     deletes exactly OLD.event_id. SECURITY INVOKER: only a principal that may
--                                     DELETE events (owner, superuser) may release an identity row.
--
-- Locks: ACCESS EXCLUSIVE on private.events (taken first, so no lock upgrade can deadlock with a writer); SHARE ROW
--   EXCLUSIVE on private.evidence_objects, private.ingest_tickets and private.messages (and its leaves) for the
--   re-created FKs; ACCESS EXCLUSIVE on the new private.event_identity; all for the whole transaction. Measured on a
--   throwaway restored from a pg_dump of dev and made FK-consistent (19,832 events, card 36 S3, 2026-10-05): at most
--   245 ms from transaction start to the migrate process exit (ADR-0063 "Conversion wall clock").

-- D-D 7b precheck, repeated as body for the test throwaways that apply bodies without manifests.
DO $$
DECLARE
  v_inbound text[];
  v_orphans text;
BEGIN
  IF (SELECT c.relkind FROM pg_class c WHERE c.oid = 'private.events'::regclass) <> 'r' THEN
    RAISE EXCEPTION 'c36 precheck: private.events is not a plain heap';
  END IF;
  -- F10: a view or BEGIN ATOMIC body would stay bound to the renamed heap.
  IF EXISTS (SELECT 1 FROM pg_depend d
              WHERE d.refobjid = 'private.events'::regclass
                AND d.classid IN ('pg_rewrite'::regclass, 'pg_proc'::regclass)) THEN
    RAISE EXCEPTION 'c36 precheck: a view or function body depends on private.events';
  END IF;
  -- F3: exactly the two FKs this migration re-points (a partitioned referrer's per-leaf clones excluded).
  SELECT array_agg(format('%s.%s', co.conrelid::regclass, co.conname) ORDER BY 1) INTO v_inbound
    FROM pg_constraint co
   WHERE co.contype = 'f' AND co.confrelid = 'private.events'::regclass AND co.conparentid = 0;
  IF v_inbound IS DISTINCT FROM ARRAY['private.ingest_tickets.ingest_tickets_redeemed_event_id_fkey',
                                      'private.messages.messages_event_id_fkey'] THEN
    RAISE EXCEPTION 'c36 precheck: foreign keys into private.events are %, expected the ticket and message FKs',
      v_inbound;
  END IF;
  -- Orphan rows (written under session_replication_role = replica) would make a re-created FK refuse mid-way;
  -- refuse up front with a count per FK. Never weakened to NOT VALID-and-leave (main-line dev orphan note).
  SELECT string_agg(format('%s: %s orphan rows - repair data first', o.fk, o.n), '; ' ORDER BY o.fk)
    INTO v_orphans
    FROM (SELECT 'events_event_id_fkey' AS fk,
                 (SELECT count(*) FROM private.events e
                   WHERE NOT EXISTS (SELECT 1 FROM private.evidence_objects eo WHERE eo.evidence_id = e.event_id)) AS n
          UNION ALL
          SELECT 'ingest_tickets_redeemed_event_id_fkey',
                 (SELECT count(*) FROM private.ingest_tickets t
                   WHERE t.redeemed_event_id IS NOT NULL
                     AND NOT EXISTS (SELECT 1 FROM private.events e WHERE e.event_id = t.redeemed_event_id))
          UNION ALL
          SELECT 'messages_event_id_fkey',
                 (SELECT count(*) FROM private.messages m
                   WHERE m.event_id IS NOT NULL
                     AND NOT EXISTS (SELECT 1 FROM private.events e WHERE e.event_id = m.event_id))) o
   WHERE o.n > 0;
  IF v_orphans IS NOT NULL THEN
    RAISE EXCEPTION 'c36 precheck: %', v_orphans;
  END IF;
END
$$;

LOCK TABLE private.events IN ACCESS EXCLUSIVE MODE;

-- D-D step 1: the snapshot only the 7a block below reads (never a manifest check: migration-rehearsal EXPLAINs those
-- at HEAD, where these temp tables do not exist).
CREATE TEMP TABLE c36_pre_events ON COMMIT DROP AS SELECT count(*) AS n FROM private.events;
CREATE TEMP TABLE c36_fp_events ON COMMIT DROP AS
  SELECT * FROM control.partition_catalog_fingerprint('private.events');

-- D-D step 2 / D-B: the identity table, backfilled from the heap, and the inbound FKs re-pointed to it.
CREATE TABLE private.event_identity (
  event_id uuid PRIMARY KEY REFERENCES private.evidence_objects (evidence_id)
);
COMMENT ON TABLE private.event_identity IS
  'ADR-0063 D-B: one row per private.events row ever inserted; the FK target and the global uniqueness of event_id. '
  'Partition drops leave its rows as tombstones.';
INSERT INTO private.event_identity (event_id) SELECT e.event_id FROM private.events e;
ALTER TABLE private.event_identity OWNER TO role_migration_owner;
-- No tenant_id, so no RLS; reached only through the owner's claim / release functions. 0011's default privileges
-- fired for the migrate principal: revoke them all.
REVOKE ALL ON private.event_identity
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;

ALTER TABLE private.ingest_tickets DROP CONSTRAINT ingest_tickets_redeemed_event_id_fkey;
ALTER TABLE private.ingest_tickets
  ADD CONSTRAINT ingest_tickets_redeemed_event_id_fkey
    FOREIGN KEY (redeemed_event_id) REFERENCES private.event_identity (event_id) NOT VALID;
ALTER TABLE private.ingest_tickets VALIDATE CONSTRAINT ingest_tickets_redeemed_event_id_fkey;
ALTER TABLE private.messages DROP CONSTRAINT messages_event_id_fkey;
ALTER TABLE private.messages
  ADD CONSTRAINT messages_event_id_fkey FOREIGN KEY (event_id) REFERENCES private.event_identity (event_id) NOT VALID;
ALTER TABLE private.messages VALIDATE CONSTRAINT messages_event_id_fkey;

-- D-D step 2 (events only): the partition key. Nullable ADD (no rewrite), backfill from the evidence row every event
-- has (FK, precheck), then NOT NULL and the server default.
ALTER TABLE private.events ADD COLUMN recorded_at timestamptz;
UPDATE private.events e SET recorded_at = eo.created_at
  FROM private.evidence_objects eo WHERE eo.evidence_id = e.event_id;
ALTER TABLE private.events ALTER COLUMN recorded_at SET NOT NULL;
ALTER TABLE private.events ALTER COLUMN recorded_at SET DEFAULT now();

-- D-D step 3: the parent re-creates policy, PK and FK; ATTACH clones the FK, index and row triggers onto the leaf.
ALTER TABLE private.events RENAME TO events_legacy;
DROP POLICY events_tenant_and_visibility ON private.events_legacy;
ALTER TABLE private.events_legacy DROP CONSTRAINT events_pkey;
ALTER TABLE private.events_legacy DROP CONSTRAINT events_event_id_fkey;

-- D-D step 4: LIKE is not enough (R-36); everything else is explicit.
CREATE TABLE private.events (
  LIKE private.events_legacy INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING GENERATED INCLUDING STORAGE
    INCLUDING COMMENTS INCLUDING COMPRESSION,
  CONSTRAINT events_pkey PRIMARY KEY (event_id, recorded_at)
) PARTITION BY RANGE (recorded_at);
ALTER TABLE private.events
  ADD CONSTRAINT events_event_id_fkey FOREIGN KEY (event_id) REFERENCES private.evidence_objects (evidence_id);
ALTER TABLE private.events ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.events FORCE ROW LEVEL SECURITY;
-- The legacy policy text verbatim (its deparse), so the 7a fingerprint compares equal.
CREATE POLICY events_tenant_and_visibility ON private.events
  USING ((EXISTS ( SELECT 1
     FROM private.evidence_objects eo
    WHERE ((eo.evidence_id = events.event_id) AND (eo.tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid) AND ((eo.visibility_class = 'TENANT_SHARED'::text) OR ((eo.visibility_class = 'USER_PRIVATE'::text) AND (eo.visibility_user_id = (current_setting('humaux.user_id'::text, true))::uuid)) OR ((eo.visibility_class = 'WORKSPACE_SHARED'::text) AND (EXISTS ( SELECT 1
             FROM control.memberships m
            WHERE ((m.tenant_id = eo.tenant_id) AND (m.user_id = (current_setting('humaux.user_id'::text, true))::uuid) AND (m.state = 'ACTIVE'::text))))))))))
  WITH CHECK ((EXISTS ( SELECT 1
     FROM private.evidence_objects eo
    WHERE ((eo.evidence_id = events.event_id) AND (eo.tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid)))));

CREATE FUNCTION private.event_identity_claim()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  -- ADR-0063 D-B: a duplicate event_id raises 23505 on event_identity_pkey (no skip branch, unlike the ledger).
  INSERT INTO private.event_identity (event_id) VALUES (NEW.event_id);
  RETURN NEW;
END;
$$;
CREATE FUNCTION private.event_identity_release()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
  -- A referencing ticket or message keeps the identity row: its NO ACTION FK raises 23503 and aborts the DELETE.
  DELETE FROM private.event_identity WHERE event_id = OLD.event_id;
  RETURN NULL;
END;
$$;
ALTER FUNCTION private.event_identity_claim() OWNER TO role_migration_owner;
ALTER FUNCTION private.event_identity_release() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION private.event_identity_claim(), private.event_identity_release() FROM PUBLIC;
CREATE TRIGGER events_identity_claim BEFORE INSERT ON private.events
  FOR EACH ROW EXECUTE FUNCTION private.event_identity_claim();
CREATE TRIGGER events_identity_release AFTER DELETE ON private.events
  FOR EACH ROW EXECUTE FUNCTION private.event_identity_release();

ALTER TABLE private.events OWNER TO role_migration_owner;
-- 0011's default privileges fired for the migrate principal: revoke them, then the exact legacy cells (§6.2.2).
REVOKE ALL ON private.events
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;
GRANT SELECT, INSERT ON private.events TO role_gateway;
GRANT SELECT ON private.events TO role_private_worker, role_maintenance;

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
  SELECT min(recorded_at), max(recorded_at) INTO v_min, v_max FROM private.events_legacy;
  v_upper := date_add(date_trunc('month', greatest(v_max, now()), 'UTC'), interval '1 month', 'UTC');
  v_lower := CASE WHEN v_min < v_month THEN NULL ELSE v_month END;
  -- `_pYYYYMM` only for exactly one UTC month; anything wider is the transitional `_hist` leaf (L4). Renamed before
  -- ATTACH so the indexes ATTACH builds carry the leaf's own name.
  v_leaf := CASE WHEN v_lower IS NOT NULL AND v_upper = date_add(v_lower, interval '1 month', 'UTC')
                 THEN 'events_p' || to_char(v_lower AT TIME ZONE 'UTC', 'YYYYMM')
                 ELSE 'events_hist' END;
  EXECUTE format('ALTER TABLE private.events_legacy RENAME TO %I', v_leaf);
  v_rel := format('private.%I', v_leaf)::regclass;
  -- A valid implying CHECK lets ATTACH skip its scan (P7); the partition constraint replaces it afterwards.
  EXECUTE format('ALTER TABLE %s ADD CONSTRAINT events_legacy_bound'
                 ' CHECK (recorded_at IS NOT NULL AND recorded_at < %L%s) NOT VALID', v_rel, v_upper,
                 CASE WHEN v_lower IS NULL THEN '' ELSE format(' AND recorded_at >= %L', v_lower) END);
  EXECUTE format('ALTER TABLE %s VALIDATE CONSTRAINT events_legacy_bound', v_rel);
  EXECUTE format('ALTER TABLE private.events ATTACH PARTITION %s FOR VALUES FROM (%s) TO (%L)', v_rel,
                 coalesce(quote_literal(v_lower), 'MINVALUE'), v_upper);
  EXECUTE format('ALTER TABLE %s DROP CONSTRAINT events_legacy_bound', v_rel);
  PERFORM control.partition_adopt_leaf('EVENTS', v_rel);
  -- D-F: the current month plus 3 future months; never a DEFAULT partition.
  WHILE v_upper < date_add(v_month, interval '4 months', 'UTC') LOOP
    PERFORM control.partition_create_month('EVENTS', v_upper);
    v_upper := date_add(v_upper, interval '1 month', 'UTC');
  END LOOP;
END
$$;

-- D-D 7a: last statement. Rows, identity rows and the normalised catalog (columns, CHECK/FK, triggers, policies, RLS
-- flags, owner, ACL) must equal the snapshot, the declared additions aside (the recorded_at column and the two
-- identity triggers); any drift aborts the whole transaction.
DO $$
DECLARE
  v_drift text;
BEGIN
  IF (SELECT count(*) FROM private.events) <> (SELECT p.n FROM c36_pre_events p) THEN
    RAISE EXCEPTION 'c36 count drift: private.events % rows, % before', (SELECT count(*) FROM private.events),
      (SELECT p.n FROM c36_pre_events p);
  END IF;
  IF (SELECT count(*) FROM private.event_identity) <> (SELECT count(*) FROM private.events) THEN
    RAISE EXCEPTION 'c36 identity drift: private.event_identity % rows, private.events %',
      (SELECT count(*) FROM private.event_identity), (SELECT count(*) FROM private.events);
  END IF;
  CREATE TEMP TABLE c36_fp_now ON COMMIT DROP AS
    SELECT * FROM control.partition_catalog_fingerprint('private.events') f
     WHERE NOT (f.kind = 'column' AND starts_with(f.item, 'recorded_at timestamp with time zone default=now() notnull=t '))
       AND NOT (f.kind = 'trigger' AND (starts_with(f.item, 'CREATE TRIGGER events_identity_claim BEFORE INSERT ')
                                        OR starts_with(f.item, 'CREATE TRIGGER events_identity_release AFTER DELETE ')));
  IF (SELECT count(*) FROM control.partition_catalog_fingerprint('private.events'))
     - (SELECT count(*) FROM c36_fp_now) <> 3 THEN
    RAISE EXCEPTION 'c36 fingerprint drift: private.events declared additions (recorded_at, events_identity_claim, '
      'events_identity_release) not all present';
  END IF;
  SELECT format('%s %s: %s', d.side, d.kind, d.item) INTO v_drift
    FROM ((SELECT 'missing' AS side, s.kind, s.item FROM c36_fp_events s
           EXCEPT ALL SELECT 'missing', n.kind, n.item FROM c36_fp_now n)
          UNION ALL
          (SELECT 'unexpected', n.kind, n.item FROM c36_fp_now n
           EXCEPT ALL SELECT 'unexpected', s.kind, s.item FROM c36_fp_events s)) d
   ORDER BY d.side, d.kind, d.item LIMIT 1;
  IF v_drift IS NOT NULL THEN
    RAISE EXCEPTION 'c36 fingerprint drift: private.events %', v_drift;
  END IF;
  DROP TABLE c36_fp_now;
END
$$;
