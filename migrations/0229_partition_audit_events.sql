-- §46 FORWARD_ONLY (card 36 S4; ADR-0063 D-B, D-D). control.audit_events becomes a RANGE (occurred_at) parent under
-- its own name: the heap is renamed, stripped, attached as one leaf with bounds derived from its data, sealed and
-- registered by control.partition_adopt_leaf, and the current UTC month plus 3 are pre-created by
-- control.partition_create_month (0224). No row is copied or rewritten. No DEFAULT partition.
--
-- Contract changes (ADR-0063 E3):
--   * PK (audit_event_id) -> (audit_event_id, occurred_at); UNIQUE (audit_seq) -> (audit_seq, occurred_at). Global
--     audit_event_id and audit_seq uniqueness move to the narrow non-partitioned control.audit_event_identity
--     (audit_event_id PK, audit_seq UNIQUE), filled by the BEFORE INSERT claim trigger: a duplicate raises 23505 on
--     the identity table exactly as the old PK / UNIQUE did, whatever occurred_at it carries.
--   * audit_seq keeps GENERATED ALWAYS AS IDENTITY on a new sequence restarted above both the legacy maximum and the
--     legacy sequence position (F7: an identity added to a new table starts at 1), so the §77 Audit Batch order
--     stays monotonic across the conversion.
--   * the one inbound FK operation_receipts.audit_event_id is re-pointed, name and column unchanged, from
--     control.audit_events to control.audit_event_identity (D-B invariant 1: no FK into a partitioned table).
--   * a row DELETE (only possible with audit_events_reject_mutation disabled or in replica mode: test teardown)
--     releases the identity row through the AFTER DELETE trigger, so a receipt-referenced audit row still refuses
--     deletion with 23503 as before. A partition drop fires no row trigger: identity rows persist as tombstones.
--   * occurred_at is the caller's p_ts (L6); the append-only trigger already refuses every UPDATE, so no key-freeze
--     trigger is needed.
--
-- Functions (caller / fence / owner role_migration_owner; EXECUTE revoked from PUBLIC, no runtime grant):
--   control.audit_event_identity_claim()    caller: trigger audit_events_identity_claim (BEFORE INSERT on
--                                           control.audit_events). Fence: inserts exactly NEW's id, seq, time and
--                                           tenant. SECURITY INVOKER: the only writer is the owner definer
--                                           control.audit_event_insert (current_user = owner, tenant GUC set, the
--                                           identity table's policy is the parent's: card 33 lesson, FORCE RLS
--                                           applies to the owner) or a superuser (bypasses RLS on both tables); a
--                                           definer would refuse a superuser's GUC-less insert the parent accepts.
--   control.audit_event_identity_release()  caller: trigger audit_events_identity_release (AFTER DELETE on
--                                           control.audit_events). Fence: deletes exactly OLD.audit_event_id.
--                                           SECURITY INVOKER: only a principal that may DELETE audit rows (owner,
--                                           superuser; no runtime role) may release an identity row.
--
-- Locks: ACCESS EXCLUSIVE on control.audit_events (taken first, before any read, so no lock upgrade can deadlock with
--   a writer); SHARE ROW EXCLUSIVE on control.tenants, control.users and control.operation_receipts for the
--   re-created FKs; ACCESS EXCLUSIVE on the new control.audit_event_identity; all for the whole transaction. Measured
--   on a throwaway restored from a pg_dump of dev and made FK-consistent (231,636 audit rows, 15,616 receipts, card 36
--   S4, 2026-10-05): at most 1,351 ms from transaction start to the migrate process exit (ADR-0063 "Conversion wall
--   clock").

LOCK TABLE control.audit_events IN ACCESS EXCLUSIVE MODE;

-- D-D 7b precheck, repeated as body for the test throwaways that apply bodies without manifests.
DO $$
DECLARE
  v_inbound text[];
  v_orphans text;
BEGIN
  IF (SELECT c.relkind FROM pg_class c WHERE c.oid = 'control.audit_events'::regclass) <> 'r' THEN
    RAISE EXCEPTION 'c36 precheck: control.audit_events is not a plain heap';
  END IF;
  -- F10: a view or BEGIN ATOMIC body would stay bound to the renamed heap.
  IF EXISTS (SELECT 1 FROM pg_depend d
              WHERE d.refobjid = 'control.audit_events'::regclass
                AND d.classid IN ('pg_rewrite'::regclass, 'pg_proc'::regclass)) THEN
    RAISE EXCEPTION 'c36 precheck: a view or function body depends on control.audit_events';
  END IF;
  -- F3: exactly the one FK this migration re-points.
  SELECT array_agg(format('%s.%s', co.conrelid::regclass, co.conname) ORDER BY 1) INTO v_inbound
    FROM pg_constraint co
   WHERE co.contype = 'f' AND co.confrelid = 'control.audit_events'::regclass AND co.conparentid = 0;
  IF v_inbound IS DISTINCT FROM ARRAY['control.operation_receipts.operation_receipts_audit_event_id_fkey'] THEN
    RAISE EXCEPTION 'c36 precheck: foreign keys into control.audit_events are %, expected the operation receipt FK',
      v_inbound;
  END IF;
  -- Orphan rows (written under session_replication_role = replica) would make a re-created FK refuse mid-way;
  -- refuse up front with a count per FK. Never weakened to NOT VALID-and-leave (main-line dev orphan note).
  SELECT string_agg(format('%s: %s orphan rows - repair data first', o.fk, o.n), '; ' ORDER BY o.fk)
    INTO v_orphans
    FROM (SELECT 'audit_events_actor_user_id_fkey' AS fk,
                 (SELECT count(*) FROM control.audit_events a
                   WHERE a.actor_user_id IS NOT NULL
                     AND NOT EXISTS (SELECT 1 FROM control.users u WHERE u.user_id = a.actor_user_id)) AS n
          UNION ALL
          SELECT 'audit_events_tenant_id_fkey',
                 (SELECT count(*) FROM control.audit_events a
                   WHERE NOT EXISTS (SELECT 1 FROM control.tenants t WHERE t.tenant_id = a.tenant_id))
          UNION ALL
          SELECT 'operation_receipts_audit_event_id_fkey',
                 (SELECT count(*) FROM control.operation_receipts r
                   WHERE NOT EXISTS (SELECT 1 FROM control.audit_events a
                                      WHERE a.audit_event_id = r.audit_event_id))) o
   WHERE o.n > 0;
  IF v_orphans IS NOT NULL THEN
    RAISE EXCEPTION 'c36 precheck: %', v_orphans;
  END IF;
END
$$;

-- D-D step 1: the snapshot only the 7a block below reads (never a manifest check: migration-rehearsal EXPLAINs those
-- at HEAD, where these temp tables do not exist). `next_seq` is the value the legacy identity would hand out next.
CREATE TEMP TABLE c36_pre_audit ON COMMIT DROP AS
  SELECT (SELECT count(*) FROM control.audit_events) AS n,
         (SELECT max(audit_seq) FROM control.audit_events) AS max_seq,
         (SELECT CASE WHEN s.is_called THEN s.last_value + 1 ELSE s.last_value END
            FROM control.audit_events_audit_seq_seq s) AS next_seq;
CREATE TEMP TABLE c36_fp_audit ON COMMIT DROP AS
  SELECT * FROM control.partition_catalog_fingerprint('control.audit_events');

-- D-D step 2 / D-B: the identity table, backfilled from the heap, and the inbound FK re-pointed to it.
CREATE TABLE control.audit_event_identity (
  audit_event_id uuid PRIMARY KEY,
  audit_seq bigint NOT NULL UNIQUE,
  occurred_at timestamptz NOT NULL,
  tenant_id uuid NOT NULL
);
COMMENT ON TABLE control.audit_event_identity IS
  'ADR-0063 D-B: one row per control.audit_events row ever inserted; the FK target and the global uniqueness of '
  'audit_event_id and audit_seq. Partition drops leave its rows as tombstones.';
INSERT INTO control.audit_event_identity (audit_event_id, audit_seq, occurred_at, tenant_id)
  SELECT a.audit_event_id, a.audit_seq, a.occurred_at, a.tenant_id FROM control.audit_events a;
ALTER TABLE control.audit_event_identity OWNER TO role_migration_owner;
-- §62 four items: the parent's tenant policy verbatim, forced, so the claim (run as the owner inside
-- control.audit_event_insert) needs exactly the tenant GUC the parent insert needs.
ALTER TABLE control.audit_event_identity ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.audit_event_identity FORCE ROW LEVEL SECURITY;
CREATE POLICY audit_event_identity_tenant_isolation ON control.audit_event_identity
  USING (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid)
  WITH CHECK (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid);
-- Reached only through the owner's claim / release triggers. 0011's default privileges fired for the migrate
-- principal: revoke them all.
REVOKE ALL ON control.audit_event_identity
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;

ALTER TABLE control.operation_receipts DROP CONSTRAINT operation_receipts_audit_event_id_fkey;
ALTER TABLE control.operation_receipts
  ADD CONSTRAINT operation_receipts_audit_event_id_fkey
    FOREIGN KEY (audit_event_id) REFERENCES control.audit_event_identity (audit_event_id) NOT VALID;
ALTER TABLE control.operation_receipts VALIDATE CONSTRAINT operation_receipts_audit_event_id_fkey;

-- D-D step 3: the parent re-creates policy, triggers, PK, UNIQUE, indexes and FKs; ATTACH clones the FKs, indexes
-- and row triggers onto the leaf. The identity goes with its sequence (F7); the snapshot holds its position.
ALTER TABLE control.audit_events RENAME TO audit_events_legacy;
DROP POLICY audit_events_tenant_isolation ON control.audit_events_legacy;
DROP TRIGGER audit_events_reject_mutation ON control.audit_events_legacy;
DROP TRIGGER audit_events_reject_truncate ON control.audit_events_legacy;
ALTER TABLE control.audit_events_legacy DROP CONSTRAINT audit_events_pkey;
ALTER TABLE control.audit_events_legacy DROP CONSTRAINT audit_events_audit_seq_key;
ALTER TABLE control.audit_events_legacy DROP CONSTRAINT audit_events_tenant_id_fkey;
ALTER TABLE control.audit_events_legacy DROP CONSTRAINT audit_events_actor_user_id_fkey;
DROP INDEX control.audit_events_tenant_occurred_at_idx;
DROP INDEX control.audit_events_action_idx;
DROP INDEX control.audit_events_request_id_idx;
ALTER TABLE control.audit_events_legacy ALTER COLUMN audit_seq DROP IDENTITY;

-- D-D step 4: LIKE is not enough (R-36); everything else is explicit, in the legacy definitions' own text.
CREATE TABLE control.audit_events (
  LIKE control.audit_events_legacy INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING GENERATED INCLUDING STORAGE
    INCLUDING COMMENTS INCLUDING COMPRESSION,
  CONSTRAINT audit_events_pkey PRIMARY KEY (audit_event_id, occurred_at),
  CONSTRAINT audit_events_audit_seq_key UNIQUE (audit_seq, occurred_at)
) PARTITION BY RANGE (occurred_at);
ALTER TABLE control.audit_events
  ADD CONSTRAINT audit_events_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES control.tenants (tenant_id);
ALTER TABLE control.audit_events
  ADD CONSTRAINT audit_events_actor_user_id_fkey FOREIGN KEY (actor_user_id) REFERENCES control.users (user_id);
CREATE INDEX audit_events_tenant_occurred_at_idx ON control.audit_events USING btree (tenant_id, occurred_at DESC);
CREATE INDEX audit_events_action_idx ON control.audit_events USING btree (action);
CREATE INDEX audit_events_request_id_idx ON control.audit_events USING btree (request_id)
  WHERE (request_id IS NOT NULL);
ALTER TABLE control.audit_events ALTER COLUMN audit_seq ADD GENERATED ALWAYS AS IDENTITY;
-- F7 / ADR-0063 D-D step 4: above both the legacy maximum and the legacy sequence position.
DO $$
BEGIN
  EXECUTE format('ALTER TABLE control.audit_events ALTER COLUMN audit_seq RESTART WITH %s',
                 (SELECT greatest(coalesce(p.max_seq, 0) + 1, p.next_seq) FROM c36_pre_audit p));
END
$$;
ALTER TABLE control.audit_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.audit_events FORCE ROW LEVEL SECURITY;
CREATE POLICY audit_events_tenant_isolation ON control.audit_events
  USING (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid)
  WITH CHECK (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid);
-- §77 Audit Immutability: the 0041 guards re-created on the parent (the row guard is cloned onto every leaf; the
-- statement guard is copied onto each leaf by control.partition_adopt_leaf, P5).
CREATE TRIGGER audit_events_reject_mutation BEFORE DELETE OR UPDATE ON control.audit_events
  FOR EACH ROW EXECUTE FUNCTION control.audit_events_reject_mutation();
CREATE TRIGGER audit_events_reject_truncate BEFORE TRUNCATE ON control.audit_events
  FOR EACH STATEMENT EXECUTE FUNCTION control.audit_events_reject_mutation();

CREATE FUNCTION control.audit_event_identity_claim()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
  -- ADR-0063 D-B: a duplicate audit_event_id or audit_seq raises 23505 on the identity table (no skip branch,
  -- unlike the ledger). Identity and defaults are already in NEW for BEFORE ROW triggers.
  INSERT INTO control.audit_event_identity (audit_event_id, audit_seq, occurred_at, tenant_id)
  VALUES (NEW.audit_event_id, NEW.audit_seq, NEW.occurred_at, NEW.tenant_id);
  RETURN NEW;
END;
$$;
CREATE FUNCTION control.audit_event_identity_release()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
  -- A referencing operation receipt keeps the identity row: its NO ACTION FK raises 23503 and aborts the DELETE.
  DELETE FROM control.audit_event_identity WHERE audit_event_id = OLD.audit_event_id;
  RETURN NULL;
END;
$$;
ALTER FUNCTION control.audit_event_identity_claim() OWNER TO role_migration_owner;
ALTER FUNCTION control.audit_event_identity_release() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.audit_event_identity_claim(), control.audit_event_identity_release() FROM PUBLIC;
CREATE TRIGGER audit_events_identity_claim BEFORE INSERT ON control.audit_events
  FOR EACH ROW EXECUTE FUNCTION control.audit_event_identity_claim();
CREATE TRIGGER audit_events_identity_release AFTER DELETE ON control.audit_events
  FOR EACH ROW EXECUTE FUNCTION control.audit_event_identity_release();

ALTER TABLE control.audit_events OWNER TO role_migration_owner;
-- 0011's default privileges fired for the migrate principal: revoke them, then the exact legacy cells (§6.2.1 control
-- domain default: SELECT for six runtime roles).
REVOKE ALL ON control.audit_events
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;
GRANT SELECT ON control.audit_events
TO role_gateway, role_private_worker, role_consolidation_worker, role_public_worker, role_retrieval_worker,
  role_maintenance;

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
  SELECT min(occurred_at), max(occurred_at) INTO v_min, v_max FROM control.audit_events_legacy;
  v_upper := date_add(date_trunc('month', greatest(v_max, now()), 'UTC'), interval '1 month', 'UTC');
  v_lower := CASE WHEN v_min < v_month THEN NULL ELSE v_month END;
  -- `_pYYYYMM` only for exactly one UTC month; anything wider is the transitional `_hist` leaf (L4). Renamed before
  -- ATTACH so the indexes ATTACH builds carry the leaf's own name.
  v_leaf := CASE WHEN v_lower IS NOT NULL AND v_upper = date_add(v_lower, interval '1 month', 'UTC')
                 THEN 'audit_events_p' || to_char(v_lower AT TIME ZONE 'UTC', 'YYYYMM')
                 ELSE 'audit_events_hist' END;
  EXECUTE format('ALTER TABLE control.audit_events_legacy RENAME TO %I', v_leaf);
  v_rel := format('control.%I', v_leaf)::regclass;
  -- A valid implying CHECK lets ATTACH skip its scan (P7); the partition constraint replaces it afterwards.
  EXECUTE format('ALTER TABLE %s ADD CONSTRAINT audit_events_legacy_bound'
                 ' CHECK (occurred_at IS NOT NULL AND occurred_at < %L%s) NOT VALID', v_rel, v_upper,
                 CASE WHEN v_lower IS NULL THEN '' ELSE format(' AND occurred_at >= %L', v_lower) END);
  EXECUTE format('ALTER TABLE %s VALIDATE CONSTRAINT audit_events_legacy_bound', v_rel);
  EXECUTE format('ALTER TABLE control.audit_events ATTACH PARTITION %s FOR VALUES FROM (%s) TO (%L)', v_rel,
                 coalesce(quote_literal(v_lower), 'MINVALUE'), v_upper);
  EXECUTE format('ALTER TABLE %s DROP CONSTRAINT audit_events_legacy_bound', v_rel);
  PERFORM control.partition_adopt_leaf('AUDIT_EVENTS', v_rel);
  -- D-F: the current month plus 3 future months; never a DEFAULT partition.
  WHILE v_upper < date_add(v_month, interval '4 months', 'UTC') LOOP
    PERFORM control.partition_create_month('AUDIT_EVENTS', v_upper);
    v_upper := date_add(v_upper, interval '1 month', 'UTC');
  END LOOP;
END
$$;

-- D-D 7a: last statement. Rows, identity rows, the identity position and the normalised catalog (columns with the
-- identity flag, CHECK/FK, triggers, policies, RLS flags, owner, ACL) must equal the snapshot, the declared additions
-- aside (the two identity triggers); any drift aborts the whole transaction.
DO $$
DECLARE
  v_drift text;
  v_next  bigint;
BEGIN
  IF (SELECT count(*) FROM control.audit_events) <> (SELECT p.n FROM c36_pre_audit p) THEN
    RAISE EXCEPTION 'c36 count drift: control.audit_events % rows, % before',
      (SELECT count(*) FROM control.audit_events), (SELECT p.n FROM c36_pre_audit p);
  END IF;
  IF (SELECT count(*) FROM control.audit_event_identity) <> (SELECT count(*) FROM control.audit_events) THEN
    RAISE EXCEPTION 'c36 identity drift: control.audit_event_identity % rows, control.audit_events %',
      (SELECT count(*) FROM control.audit_event_identity), (SELECT count(*) FROM control.audit_events);
  END IF;
  EXECUTE format('SELECT CASE WHEN s.is_called THEN s.last_value + 1 ELSE s.last_value END FROM %s s',
                 pg_get_serial_sequence('control.audit_events', 'audit_seq')) INTO v_next;
  IF v_next <= (SELECT coalesce(p.max_seq, 0) FROM c36_pre_audit p) THEN
    RAISE EXCEPTION 'c36 audit_seq drift: next audit_seq % is not above the legacy maximum %', v_next,
      (SELECT p.max_seq FROM c36_pre_audit p);
  END IF;
  CREATE TEMP TABLE c36_fp_now ON COMMIT DROP AS
    SELECT * FROM control.partition_catalog_fingerprint('control.audit_events') f
     WHERE NOT (f.kind = 'trigger'
                AND (starts_with(f.item, 'CREATE TRIGGER audit_events_identity_claim BEFORE INSERT ')
                     OR starts_with(f.item, 'CREATE TRIGGER audit_events_identity_release AFTER DELETE ')));
  IF (SELECT count(*) FROM control.partition_catalog_fingerprint('control.audit_events'))
     - (SELECT count(*) FROM c36_fp_now) <> 2 THEN
    RAISE EXCEPTION 'c36 fingerprint drift: control.audit_events declared additions (audit_events_identity_claim, '
      'audit_events_identity_release) not all present';
  END IF;
  SELECT format('%s %s: %s', d.side, d.kind, d.item) INTO v_drift
    FROM ((SELECT 'missing' AS side, s.kind, s.item FROM c36_fp_audit s
           EXCEPT ALL SELECT 'missing', n.kind, n.item FROM c36_fp_now n)
          UNION ALL
          (SELECT 'unexpected', n.kind, n.item FROM c36_fp_now n
           EXCEPT ALL SELECT 'unexpected', s.kind, s.item FROM c36_fp_audit s)) d
   ORDER BY d.side, d.kind, d.item LIMIT 1;
  IF v_drift IS NOT NULL THEN
    RAISE EXCEPTION 'c36 fingerprint drift: control.audit_events %', v_drift;
  END IF;
  DROP TABLE c36_fp_now;
END
$$;
