-- §46 FORWARD_ONLY (card 36, logical-restore finding of 2026-10-05; ADR-0063 "Registry by name"). The registry of 0224
-- identified a leaf by a stored relation OID as well as by its name: partition_adopt_leaf matched both on re-adoption
-- and partition_drop_check resolved the leaf through that OID. A logical dump/restore renumbers every relation, so on
-- the copy the main line restored from a dump of dev (the documented rollback, ADR-0063 D-M) every leaf failed the
-- rls-check partition arm ("leaf with no matching ATTACHED registry row") and every drop refused `stale`: the
-- rollback left retention unable to run, ever. Nothing wrong was dropped (name AND OID had to match), but the
-- rollback contract was broken.
--
-- Ruling (main line, 2026-10-05): the registry is keyed by NAME + BOUNDS verified against the catalog; no stored OID
-- is trusted or required. UNIQUE (leaf_name) of 0224 stays as it is (unconditional, so also among non-DROPPED rows;
-- partition_adopt_leaf's ON CONFLICT (leaf_name) arbiter needs it whole).
--
-- leaf_oid is retired, not dropped: the applied manifests of 0225..0230 name `r.leaf_oid` in their postchecks and
-- `cargo xtask migration-rehearsal` EXPLAINs every manifest check against HEAD (ADR-0050 D-E); dropping the column
-- would turn those six frozen checks invalid at HEAD. The column becomes nullable, every value is cleared, and
-- CHECK partition_registry_leaf_oid_retired keeps it NULL; no function, gate or adapter reads it.
--
-- Functions replaced (CREATE OR REPLACE keeps signature, owner and ACL; the SECURITY and SET clauses are restated
-- verbatim, so rls-check's partition arm pins them unchanged):
--   control.partition_adopt_leaf(text,regclass)  registers name + catalog bounds; an existing row of that name must
--                                                be the same table_key, ATTACHED, with those bounds.
--   control.partition_drop_check(uuid,uuid)      D-J step 4: resolves to_regclass(leaf_name) and requires the
--                                                canonical name, relkind 'r', a partition of the table_key's parent
--                                                (control.partition_parent) and a partition bound equal to the
--                                                registry's [lower_bound, upper_bound).
-- Any disagreement raises `retention refused: registry_catalog_mismatch <leaf>` (it replaces `stale`) before
-- anything acts on the leaf. partition_create_month, partition_drop_statements and partition_drop never read the OID
-- and are unchanged (partition_drop runs partition_drop_check before building its statements).
--
-- Locks: ACCESS EXCLUSIVE on control.partition_registry (taken first, so the check below cannot upgrade a lock and
--   deadlock with the daemon's proposal UPDATE) for the whole transaction: one row per leaf is rewritten and a CHECK
--   validated. Measured on the D-M dev copy (c36_devcopy.sh: a dump of dev at 0228 migrated through 0231, 24 leaves):
--   tens of milliseconds including the migrate process exit; the runs are in ADR-0063 "Registry by name".

LOCK TABLE control.partition_registry IN ACCESS EXCLUSIVE MODE;

-- Precheck repeated as body for the test throwaways that apply bodies without manifests: every ATTACHED row must
-- already resolve by name and bounds, so clearing the OID loses nothing (a restored copy passes here; a registry
-- that disagrees with its catalog is named and refused, never silently re-keyed).
DO $$
DECLARE
  v_bad text;
BEGIN
  SELECT string_agg(g.leaf_name, ', ' ORDER BY g.leaf_name) INTO v_bad
    FROM control.partition_registry g
   WHERE g.state = 'ATTACHED'
     AND NOT EXISTS (
           SELECT 1
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_inherits i ON i.inhrelid = c.oid
            WHERE c.oid = to_regclass(g.leaf_name) AND c.relkind = 'r'
              AND format('%I.%I', n.nspname, c.relname) = g.leaf_name
              AND i.inhparent = (SELECT p.parent FROM control.partition_parent(g.table_key) p)
              AND pg_get_expr(c.relpartbound, c.oid)
                  = format('FOR VALUES FROM (%s) TO (%s)', coalesce(quote_literal(g.lower_bound), 'MINVALUE'),
                           quote_literal(g.upper_bound)));
  IF v_bad IS NOT NULL THEN
    RAISE EXCEPTION 'c36 0231 precheck: registry rows that do not match the catalog by name and bounds: %', v_bad;
  END IF;
END
$$;

ALTER TABLE control.partition_registry ALTER COLUMN leaf_oid DROP NOT NULL;
UPDATE control.partition_registry SET leaf_oid = NULL;
ALTER TABLE control.partition_registry
  ADD CONSTRAINT partition_registry_leaf_oid_retired CHECK (leaf_oid IS NULL);
COMMENT ON COLUMN control.partition_registry.leaf_oid IS
  'Retired by 0231 (ADR-0063 "Registry by name"): always NULL; a leaf is its leaf_name + bounds, verified against the catalog.';

CREATE OR REPLACE FUNCTION control.partition_adopt_leaf(p_table_key text, p_leaf regclass)
RETURNS uuid
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_parent regclass;
  v_bound  text;
  v_lo     text;
  v_hi     text;
  v_lower  timestamptz;
  v_upper  timestamptz;
  v_id     uuid;
  v_names  oid;
  r        record;
BEGIN
  SELECT p.parent INTO v_parent FROM control.partition_parent(p_table_key) p;
  IF NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = p_leaf AND i.inhparent = v_parent) THEN
    RAISE EXCEPTION 'partition_adopt_leaf: % is not a partition of %', p_leaf, v_parent USING ERRCODE = '22023';
  END IF;
  SELECT pg_get_expr(c.relpartbound, c.oid) INTO v_bound FROM pg_class c WHERE c.oid = p_leaf AND c.relkind = 'r';
  -- ADR-0063 D-F: no DEFAULT partition, ever.
  IF v_bound IS NULL OR v_bound = 'DEFAULT' THEN
    RAISE EXCEPTION 'partition_adopt_leaf: % is a DEFAULT or sub-partitioned leaf (ADR-0063 D-F)', p_leaf
      USING ERRCODE = '22023';
  END IF;
  SELECT m[1], m[2] INTO v_lo, v_hi FROM regexp_match(v_bound, '^FOR VALUES FROM \((.*)\) TO \((.*)\)$') m;
  IF v_hi IS NULL OR v_hi = 'MAXVALUE' OR v_lo = 'MAXVALUE' THEN
    RAISE EXCEPTION 'partition_adopt_leaf: % has bound % (MAXVALUE is never a monthly leaf)', p_leaf, v_bound
      USING ERRCODE = '22023';
  END IF;
  v_lower := CASE WHEN v_lo = 'MINVALUE' THEN NULL ELSE btrim(v_lo, '''')::timestamptz END;
  v_upper := btrim(v_hi, '''')::timestamptz;

  EXECUTE format('ALTER TABLE %s OWNER TO role_migration_owner', p_leaf);
  EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', p_leaf);
  EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', p_leaf);

  -- The leaf's policy set becomes exactly the parent's, verbatim (rls-check partition arm compares them). A policy
  -- that names its own table (private.events: `events.event_id` inside an EXISTS) only re-parses on the leaf when it
  -- is deparsed against the leaf, which is exact while the leaf's column numbers equal the parent's; otherwise the
  -- parent's column names are used and such a policy fails loudly instead of binding the wrong column.
  v_names := CASE WHEN NOT EXISTS (SELECT a.attnum, a.attname FROM pg_attribute a
                                    WHERE a.attrelid = v_parent AND a.attnum > 0 AND NOT a.attisdropped
                                   EXCEPT
                                   SELECT a.attnum, a.attname FROM pg_attribute a
                                    WHERE a.attrelid = p_leaf AND a.attnum > 0 AND NOT a.attisdropped)
                  THEN p_leaf ELSE v_parent END;
  FOR r IN SELECT p.polname FROM pg_policy p WHERE p.polrelid = p_leaf LOOP
    EXECUTE format('DROP POLICY %I ON %s', r.polname, p_leaf);
  END LOOP;
  FOR r IN SELECT p.polname, p.polpermissive,
                  CASE p.polcmd WHEN 'r' THEN 'SELECT' WHEN 'a' THEN 'INSERT' WHEN 'w' THEN 'UPDATE'
                                WHEN 'd' THEN 'DELETE' ELSE 'ALL' END AS cmd,
                  (SELECT string_agg(CASE WHEN x = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(x)) END, ', ')
                     FROM unnest(p.polroles) x) AS roles,
                  pg_get_expr(p.polqual, v_names) AS qual,
                  pg_get_expr(p.polwithcheck, v_names) AS chk
             FROM pg_policy p WHERE p.polrelid = v_parent LOOP
    EXECUTE format('CREATE POLICY %I ON %s AS %s FOR %s TO %s%s%s', r.polname, p_leaf,
                   CASE WHEN r.polpermissive THEN 'PERMISSIVE' ELSE 'RESTRICTIVE' END, r.cmd, r.roles,
                   CASE WHEN r.qual IS NULL THEN '' ELSE format(' USING (%s)', r.qual) END,
                   CASE WHEN r.chk IS NULL THEN '' ELSE format(' WITH CHECK (%s)', r.chk) END);
  END LOOP;

  -- F9: statement-level triggers (the TRUNCATE guards) are not cloned to partitions.
  FOR r IN SELECT t.tgname, t.tgtype, t.tgfoid, t.tgnargs, t.tgattr, t.tgoldtable, t.tgnewtable
             FROM pg_trigger t
            WHERE t.tgrelid = v_parent AND NOT t.tgisinternal AND (t.tgtype & 1) = 0 LOOP
    IF r.tgnargs <> 0 OR cardinality(r.tgattr::int2[]) <> 0 OR r.tgoldtable IS NOT NULL
       OR r.tgnewtable IS NOT NULL THEN
      RAISE EXCEPTION 'partition_adopt_leaf: statement trigger % on % has arguments, columns or transition tables',
        r.tgname, v_parent USING ERRCODE = '0A000';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger t WHERE t.tgrelid = p_leaf AND t.tgname = r.tgname) THEN
      EXECUTE format('CREATE TRIGGER %I %s %s ON %s FOR EACH STATEMENT EXECUTE FUNCTION %s()', r.tgname,
                     CASE WHEN r.tgtype & 2 <> 0 THEN 'BEFORE' WHEN r.tgtype & 64 <> 0 THEN 'INSTEAD OF'
                          ELSE 'AFTER' END,
                     array_to_string(ARRAY[CASE WHEN r.tgtype & 4 <> 0 THEN 'INSERT' END,
                                           CASE WHEN r.tgtype & 8 <> 0 THEN 'DELETE' END,
                                           CASE WHEN r.tgtype & 16 <> 0 THEN 'UPDATE' END,
                                           CASE WHEN r.tgtype & 32 <> 0 THEN 'TRUNCATE' END], ' OR '),
                     p_leaf, r.tgfoid::regproc);
    END IF;
  END LOOP;

  -- ADR-0063 D-E: runtime roles reach rows only through the parent, so a leaf holds no grant but the owner's.
  EXECUTE format('REVOKE ALL ON %s FROM PUBLIC', p_leaf);
  FOR r IN SELECT DISTINCT x.grantee
             FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) x
            WHERE c.oid = p_leaf AND x.grantee NOT IN (0, c.relowner)
           UNION
           SELECT DISTINCT x.grantee
             FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid CROSS JOIN LATERAL aclexplode(a.attacl) x
            WHERE c.oid = p_leaf AND x.grantee NOT IN (0, c.relowner) LOOP
    EXECUTE format('REVOKE ALL ON %s FROM %I', p_leaf, pg_get_userbyid(r.grantee));
  END LOOP;

  -- 0231: a leaf is registered by its canonical name and catalog bounds only; an existing row of that name must
  -- agree on both (a logical restore renumbers relations, so no stored relation number is ever compared).
  INSERT INTO control.partition_registry (table_key, leaf_name, lower_bound, upper_bound)
  VALUES (p_table_key, p_leaf::text, v_lower, v_upper)
  ON CONFLICT (leaf_name) DO NOTHING
  RETURNING registry_id INTO v_id;
  IF v_id IS NULL THEN
    SELECT g.registry_id INTO v_id FROM control.partition_registry g
     WHERE g.leaf_name = p_leaf::text AND g.table_key = p_table_key
       AND g.state = 'ATTACHED' AND g.lower_bound IS NOT DISTINCT FROM v_lower AND g.upper_bound = v_upper;
    IF v_id IS NULL THEN
      RAISE EXCEPTION 'retention refused: registry_catalog_mismatch %', p_leaf::text;
    END IF;
  END IF;
  RETURN v_id;
END;
$$;

CREATE OR REPLACE FUNCTION control.partition_drop_check(p_policy_id uuid, p_registry_id uuid)
RETURNS TABLE (leaf text, lower_bound timestamptz, upper_bound timestamptz, row_count bigint)
LANGUAGE plpgsql
VOLATILE
SECURITY INVOKER
SET search_path = pg_catalog
SET row_security = off
AS $$
#variable_conflict use_column
DECLARE
  reg      control.partition_registry%ROWTYPE;
  pol      control.retention_policies%ROWTYPE;
  v_parent regclass;
  v_cutoff timestamptz;
  v_hold   boolean;
  v_rows   bigint;
BEGIN
  IF NOT coalesce((SELECT r.rolsuper FROM pg_roles r WHERE r.rolname = current_user), false) THEN
    RAISE EXCEPTION 'retention refused: executor_not_superuser';
  END IF;
  SELECT * INTO reg FROM control.partition_registry g WHERE g.registry_id = p_registry_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'retention refused: unknown_registry_row';
  END IF;
  IF reg.state <> 'ATTACHED' THEN
    RAISE EXCEPTION 'retention refused: not_attached';
  END IF;
  SELECT * INTO pol FROM control.retention_policies p WHERE p.policy_id = p_policy_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'retention refused: unknown_policy';
  END IF;
  IF pol.table_key <> reg.table_key THEN
    RAISE EXCEPTION 'retention refused: wrong_table';
  END IF;
  IF EXISTS (SELECT 1 FROM control.retention_policies p
              WHERE p.table_key = pol.table_key AND p.policy_revision > pol.policy_revision) THEN
    RAISE EXCEPTION 'retention refused: superseded';
  END IF;
  IF pol.effective_at > now() THEN
    RAISE EXCEPTION 'retention refused: not_effective';
  END IF;
  IF pol.retention_months IS NULL THEN
    RAISE EXCEPTION 'retention refused: keep_forever';
  END IF;
  -- D-J step 3: the current and future months can never be due (UTC, whatever the session TimeZone).
  v_cutoff := ((date_trunc('month', now(), 'UTC') AT TIME ZONE 'UTC')
               - make_interval(months => pol.retention_months)) AT TIME ZONE 'UTC';
  IF reg.upper_bound > v_cutoff THEN
    RAISE EXCEPTION 'retention refused: not_due';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM control.partition_registry g
                  WHERE g.table_key = reg.table_key AND g.state = 'ATTACHED' AND g.upper_bound > reg.upper_bound) THEN
    RAISE EXCEPTION 'retention refused: newest_leaf';
  END IF;
  -- D-J step 4 / 0231: the registry must still describe the catalog. The leaf is the relation its canonical name
  -- resolves to, and only if that is an ordinary table attached to this table_key's parent whose partition bound is
  -- exactly the registry's [lower, upper) (both rendered by this session, so equal text = equal instants); nothing
  -- below acts on a leaf that fails this.
  SELECT p.parent INTO v_parent FROM control.partition_parent(reg.table_key) p;
  IF NOT EXISTS (
       SELECT 1
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         JOIN pg_inherits i ON i.inhrelid = c.oid AND i.inhparent = v_parent
        WHERE c.oid = to_regclass(reg.leaf_name) AND c.relkind = 'r'
          AND format('%I.%I', n.nspname, c.relname) = reg.leaf_name
          AND pg_get_expr(c.relpartbound, c.oid)
              = format('FOR VALUES FROM (%s) TO (%s)', coalesce(quote_literal(reg.lower_bound), 'MINVALUE'),
                       quote_literal(reg.upper_bound))) THEN
    RAISE EXCEPTION 'retention refused: registry_catalog_mismatch %', reg.leaf_name;
  END IF;
  IF reg.proposed_policy_revision IS DISTINCT FROM pol.policy_revision OR reg.proposed_at IS NULL
     OR reg.proposed_at < pol.effective_at THEN
    RAISE EXCEPTION 'retention refused: not_proposed';
  END IF;
  -- D-J step 5: writes routed to this past month wait; the parent stays writable.
  EXECUTE format('LOCK TABLE %s IN SHARE MODE', reg.leaf_name);
  -- L13: exhaustive over the closed table_key set; a key without an arm is never dropped by default.
  CASE reg.table_key
    WHEN 'MODEL_CALL_LEDGER' THEN
      EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s l WHERE l.status = ''RESERVED'')', reg.leaf_name)
        INTO v_hold;
      IF v_hold THEN
        RAISE EXCEPTION 'retention refused: hold:unsettled_ledger_calls';
      END IF;
      -- 0117:507-517: the reaper would settle such a reservation EXPIRED instead of RELEASED once its row is gone.
      EXECUTE format('SELECT EXISTS (SELECT 1 FROM ops.retrieval_provider_budget_reservations r JOIN %s l'
                     ' ON l.tenant_id = r.tenant_id AND l.model_call_id = r.model_call_id'
                     ' WHERE r.status = ''RESERVED'')', reg.leaf_name)
        INTO v_hold;
      IF v_hold THEN
        RAISE EXCEPTION 'retention refused: hold:open_budget_reservations';
      END IF;
      -- 0131:4-13 terminal states, written NOT IN so a state added later holds by default.
      EXECUTE format('SELECT EXISTS (SELECT 1 FROM private.contribution_executions e JOIN %s l'
                     ' ON l.tenant_id = e.tenant_id'
                     ' AND l.model_call_id IN (e.coverage_model_call_id, e.assessment_model_call_id)'
                     ' WHERE e.state NOT IN (''DONE'', ''NOT_CONTRIBUTABLE'', ''REJECTED_SAFETY'','
                     ' ''FAILED_TERMINAL''))', reg.leaf_name)
        INTO v_hold;
      IF v_hold THEN
        RAISE EXCEPTION 'retention refused: hold:open_contribution_executions';
      END IF;
    WHEN 'STAGE_RUNS' THEN
      EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s l WHERE l.finished_at IS NULL)', reg.leaf_name) INTO v_hold;
      IF v_hold THEN
        RAISE EXCEPTION 'retention refused: hold:running_stage';
      END IF;
    WHEN 'MESSAGES', 'MAINTENANCE_RECEIPTS' THEN
      NULL;
    ELSE
      RAISE EXCEPTION 'retention refused: no_hold_rule';
  END CASE;
  EXECUTE format('SELECT count(*) FROM %s', reg.leaf_name) INTO v_rows;
  RETURN QUERY SELECT reg.leaf_name, reg.lower_bound, reg.upper_bound, v_rows;
END;
$$;
