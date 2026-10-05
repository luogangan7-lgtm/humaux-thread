-- §46 FORWARD_ONLY (card 36 S1; ADR-0063 D-C, D-E, D-G, D-I, D-J). The §48.1 partition machinery. No table is
-- converted here: 0225..0230 convert one growth table each and call the creator / adopter defined below.
--
-- Locks: ACCESS EXCLUSIVE on control.retention_policies v1 (0 rows, no reader) for its DROP; every other object is
--   new. Measured on a throwaway migrated through 0223 (card 36 S1, 2026-10-05): 33 statements, 9 ms in total, so the
--   v1 lock is held for milliseconds.
--
-- Objects
--   control.retention_policies (v2)  replaces the 0-row per-tenant v1 of 0003 (D-C). One row = one approval of a
--                                    fleet-wide monthly retention for one table_key; the latest revision is the
--                                    policy. No tenant_id, no RLS. role_maintenance SELECT; every other runtime role
--                                    nothing (v1's control-domain default SELECT is revoked: it had no reader).
--   control.partition_registry       one row per leaf of the six §48.1 parents (D-C): bounds, ATTACHED / DROPPED,
--                                    the daemon's proposal columns and the drop receipt. No tenant_id, no RLS.
--                                    role_maintenance SELECT + UPDATE (proposed_at, proposed_policy_revision) only.
--
-- Functions (caller / fence / owner). Every one is owned by role_migration_owner, pins search_path = pg_catalog and
-- has EXECUTE revoked from PUBLIC with no runtime grant: only superusers (the migrate principal, the retention
-- executor of ADR-0063 D-H) and the owner itself can call them.
--   control.partition_parent(text)                      caller: every function below. Fence: the closed table_key
--                                                       CASE; raises on an unknown key or a parent that is not a
--                                                       RANGE-partitioned table on that key. Invoker, catalog only.
--   control.partition_catalog_fingerprint(text)         caller: the 7a assertion block of 0225..0230. Fence: catalog
--                                                       reads only. Invoker.
--   control.partition_create_month(text,timestamptz)    caller: 0225..0230, `retention create-partitions`. Fence:
--                                                       UTC month start, no gap, idempotent. SECURITY DEFINER.
--   control.partition_adopt_leaf(text,regclass)         caller: partition_create_month, 0225..0230. Fence: the leaf
--                                                       must be a non-DEFAULT partition of the mapped parent.
--                                                       SECURITY DEFINER.
--   control.partition_drop_statements(uuid)             caller: partition_drop, `retention execute --dry-run`.
--                                                       Fence: registry row only, never a free name. DEFINER.
--   control.partition_drop_check(uuid,uuid)             caller: partition_drop, `retention execute`. Fence:
--                                                       superuser only, row_security = off; every D-G / D-J
--                                                       predicate. SECURITY INVOKER.
--   control.partition_drop(uuid,uuid,bigint,text,text)  caller: `retention execute`. Fence: re-runs the check,
--                                                       DETACH + DROP RESTRICT, receipt. SECURITY INVOKER,
--                                                       superuser only, row_security = off.
--   control.retention_policy_approve(text,int,timestamptz,text)  caller: `retention approve`. Fence: the table
--                                                       CHECKs; next revision computed here. SECURITY DEFINER.
--   control.reject_partition_key_update()               caller: the key-freeze triggers of 0225/0226. Fence: raises
--                                                       unconditionally (the trigger's WHEN selects key changes).
--                                                       SECURITY DEFINER.

-- D-C: v1 is dropped only when it is empty and nothing references it (the manifest precheck says the same; this
-- block is the fence for the test throwaways that apply bodies without manifests).
DO $$
BEGIN
  IF (SELECT count(*) FROM control.retention_policies) <> 0 THEN
    RAISE EXCEPTION 'c36 precheck: control.retention_policies v1 holds rows; ADR-0063 D-C drops it only when empty';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_constraint
              WHERE contype = 'f' AND confrelid = 'control.retention_policies'::regclass) THEN
    RAISE EXCEPTION 'c36 precheck: a foreign key references control.retention_policies v1';
  END IF;
END
$$;
DROP TABLE control.retention_policies;

-- ADR-0063 D-C / D-G: EVENTS and AUDIT_EVENTS are absent from table_key on purpose (card 37's rebuild baseline and a
-- separate approval line widen it); scope is the stated granularity: per table, every tenant, whole months.
CREATE TABLE control.retention_policies (
  policy_id        uuid        PRIMARY KEY DEFAULT uuidv7(),
  table_key        text        NOT NULL
                   CONSTRAINT retention_policies_table_key
                   CHECK (table_key IN ('MODEL_CALL_LEDGER', 'STAGE_RUNS', 'MESSAGES', 'MAINTENANCE_RECEIPTS')),
  scope            text        NOT NULL DEFAULT 'ALL_TENANTS' CHECK (scope = 'ALL_TENANTS'),
  -- NULL = keep forever: how a policy is withdrawn (D-C).
  retention_months integer     CHECK (retention_months IS NULL OR retention_months >= 1),
  policy_revision  integer     NOT NULL CHECK (policy_revision >= 1),
  approved_by      text        NOT NULL CHECK (btrim(approved_by) <> ''),
  approved_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
  effective_at     timestamptz NOT NULL,
  CONSTRAINT retention_policies_effective_after_approval CHECK (effective_at >= approved_at),
  CONSTRAINT retention_policies_revision UNIQUE (table_key, policy_revision)
);

CREATE TABLE control.partition_registry (
  registry_id              uuid        PRIMARY KEY DEFAULT uuidv7(),
  table_key                text        NOT NULL
                           CONSTRAINT partition_registry_table_key
                           CHECK (table_key IN ('MODEL_CALL_LEDGER', 'STAGE_RUNS', 'MESSAGES', 'MAINTENANCE_RECEIPTS',
                                                'EVENTS', 'AUDIT_EVENTS')),
  -- regclass text of the leaf (schema-qualified); partition_drop_check refuses `stale` when it drifts.
  leaf_name                text        NOT NULL UNIQUE,
  leaf_oid                 oid         NOT NULL,
  -- NULL = MINVALUE (the transitional `_hist` leaf of a converted heap, ADR-0063 D-D step 5).
  lower_bound              timestamptz,
  upper_bound              timestamptz NOT NULL,
  state                    text        NOT NULL DEFAULT 'ATTACHED' CHECK (state IN ('ATTACHED', 'DROPPED')),
  created_at               timestamptz NOT NULL DEFAULT clock_timestamp(),
  created_by               text        NOT NULL DEFAULT session_user,
  -- D-K: written by role_maintenance (column grant). A corroboration, never an authority (D-J step 4).
  proposed_at              timestamptz,
  proposed_policy_revision integer,
  -- D-J step 9: the drop receipt, written only by control.partition_drop.
  dropped_at               timestamptz,
  dropped_by               text,
  drop_policy_id           uuid        REFERENCES control.retention_policies (policy_id),
  drop_policy_revision     integer,
  rows_dropped             bigint      CHECK (rows_dropped >= 0),
  export_path              text,
  export_sha256            text        CHECK (export_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT partition_registry_upper UNIQUE (table_key, upper_bound),
  CONSTRAINT partition_registry_bounds CHECK (lower_bound IS NULL OR lower_bound < upper_bound),
  CONSTRAINT partition_registry_receipt
    CHECK ((state = 'DROPPED') = (dropped_at IS NOT NULL AND rows_dropped IS NOT NULL AND export_sha256 IS NOT NULL))
);

-- §6.2.2 columns (0161 recipe): re-own, revoke the control-domain default the migrate principal's default
-- privileges just granted, grant the one cell each.
ALTER TABLE control.retention_policies OWNER TO role_migration_owner;
ALTER TABLE control.partition_registry OWNER TO role_migration_owner;
REVOKE ALL ON control.retention_policies, control.partition_registry
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;
GRANT SELECT ON control.retention_policies TO role_maintenance;
GRANT SELECT ON control.partition_registry TO role_maintenance;
GRANT UPDATE (proposed_at, proposed_policy_revision) ON control.partition_registry TO role_maintenance;

-- ADR-0063 D-E: the one place a table_key becomes a parent and its partition key. A key added to the CHECKs above
-- without an arm here raises instead of guessing.
CREATE FUNCTION control.partition_parent(p_table_key text, OUT parent regclass, OUT key_column name)
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog
AS $$
DECLARE
  v_rel text;
BEGIN
  SELECT m.rel, m.key INTO v_rel, key_column
    FROM (VALUES ('STAGE_RUNS', 'ops.stage_runs', 'started_at'),
                 ('MESSAGES', 'private.messages', 'created_at'),
                 ('MAINTENANCE_RECEIPTS', 'ops.maintenance_receipts', 'ran_at'),
                 ('EVENTS', 'private.events', 'recorded_at'),
                 ('AUDIT_EVENTS', 'control.audit_events', 'occurred_at'),
                 ('MODEL_CALL_LEDGER', 'ops.model_call_ledger', 'called_at')) m(table_key, rel, key)
   WHERE m.table_key = p_table_key;
  IF v_rel IS NULL THEN
    RAISE EXCEPTION 'partition: unknown table_key %', p_table_key USING ERRCODE = '22023';
  END IF;
  parent := to_regclass(v_rel);
  IF NOT EXISTS (
       SELECT 1 FROM pg_partitioned_table pt JOIN pg_attribute a ON a.attrelid = pt.partrelid
        WHERE pt.partrelid = parent AND pt.partstrat = 'r' AND pt.partnatts = 1
          AND a.attnum = pt.partattrs[0] AND a.attname = key_column) THEN
    RAISE EXCEPTION 'partition: % is not range-partitioned on % (ADR-0063 D-A)', v_rel, key_column
      USING ERRCODE = '55000';
  END IF;
END;
$$;

-- ADR-0063 D-D step 1: the normalised catalog rows a conversion must preserve. PK / UNIQUE / index rows are left out
-- because a conversion changes them on purpose; NOT NULL is read from pg_attribute (PG18 also catalogs it as
-- contype 'n' under generated names, which LIKE does not preserve).
CREATE FUNCTION control.partition_catalog_fingerprint(p_table text)
RETURNS TABLE (kind text, item text)
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $$
  WITH t AS (SELECT c.oid, c.relowner, c.relacl, c.relrowsecurity, c.relforcerowsecurity
               FROM pg_class c WHERE c.oid = to_regclass(p_table))
  SELECT 'column', format('%s %s default=%s notnull=%s identity=%s generated=%s', a.attname,
                          format_type(a.atttypid, a.atttypmod), pg_get_expr(d.adbin, d.adrelid), a.attnotnull,
                          a.attidentity, a.attgenerated)
    FROM t JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum > 0 AND NOT a.attisdropped
    LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
  UNION ALL
  SELECT 'constraint', format('%s %s', co.conname, pg_get_constraintdef(co.oid))
    FROM t JOIN pg_constraint co ON co.conrelid = t.oid WHERE co.contype IN ('c', 'f')
  UNION ALL
  SELECT 'trigger', format('%s enabled=%s', pg_get_triggerdef(tg.oid), tg.tgenabled)
    FROM t JOIN pg_trigger tg ON tg.tgrelid = t.oid WHERE NOT tg.tgisinternal
  UNION ALL
  SELECT 'policy', format('%s cmd=%s permissive=%s roles=%s qual=%s check=%s', p.polname, p.polcmd,
                          p.polpermissive,
                          (SELECT string_agg(CASE WHEN r = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(r)::text END, ','
                                             ORDER BY 1)
                             FROM unnest(p.polroles) r),
                          pg_get_expr(p.polqual, p.polrelid), pg_get_expr(p.polwithcheck, p.polrelid))
    FROM t JOIN pg_policy p ON p.polrelid = t.oid
  UNION ALL
  SELECT 'rls', format('enabled=%s forced=%s owner=%s', t.relrowsecurity, t.relforcerowsecurity,
                       pg_get_userbyid(t.relowner))
    FROM t
  UNION ALL
  SELECT 'acl', format('%s %s grantable=%s',
                       CASE WHEN x.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(x.grantee)::text END,
                       x.privilege_type, x.is_grantable)
    FROM t, aclexplode(coalesce(t.relacl, acldefault('r', t.relowner))) x
  UNION ALL
  SELECT 'column_acl', format('%s %s %s grantable=%s', a.attname,
                              CASE WHEN x.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(x.grantee)::text END,
                              x.privilege_type, x.is_grantable)
    FROM t JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum > 0 AND NOT a.attisdropped
         CROSS JOIN LATERAL aclexplode(a.attacl) x
$$;

-- ADR-0063 D-E: seals one leaf exactly like its parent and registers it. Row triggers, indexes, CHECK, NOT NULL and
-- outbound FKs are cloned by PostgreSQL itself; policies, FORCE, statement triggers and grants are not.
CREATE FUNCTION control.partition_adopt_leaf(p_table_key text, p_leaf regclass)
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

  INSERT INTO control.partition_registry (table_key, leaf_name, leaf_oid, lower_bound, upper_bound)
  VALUES (p_table_key, p_leaf::text, p_leaf, v_lower, v_upper)
  ON CONFLICT (leaf_name) DO NOTHING
  RETURNING registry_id INTO v_id;
  IF v_id IS NULL THEN
    SELECT g.registry_id INTO v_id FROM control.partition_registry g
     WHERE g.leaf_name = p_leaf::text AND g.leaf_oid = p_leaf AND g.table_key = p_table_key
       AND g.state = 'ATTACHED' AND g.lower_bound IS NOT DISTINCT FROM v_lower AND g.upper_bound = v_upper;
    IF v_id IS NULL THEN
      RAISE EXCEPTION 'partition_adopt_leaf: registry row of % disagrees with the catalog', p_leaf
        USING ERRCODE = '55000';
    END IF;
  END IF;
  RETURN v_id;
END;
$$;

-- ADR-0063 D-E / D-F: the next monthly leaf, never a gap and never a DEFAULT partition; 'created' or 'exists'.
CREATE FUNCTION control.partition_create_month(p_table_key text, p_month_start timestamptz)
RETURNS text
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_parent regclass;
  v_max    timestamptz;
  v_next   timestamptz;
  v_leaf   text;
BEGIN
  SELECT p.parent INTO v_parent FROM control.partition_parent(p_table_key) p;
  -- D-D step 5 / T-D5: month boundaries are UTC whatever the session TimeZone is.
  IF p_month_start IS NULL OR p_month_start <> date_trunc('month', p_month_start, 'UTC') THEN
    RAISE EXCEPTION 'partition_create_month: % is not a UTC month start', p_month_start USING ERRCODE = '22023';
  END IF;
  IF EXISTS (SELECT 1 FROM control.partition_registry g
              WHERE g.table_key = p_table_key AND g.state = 'ATTACHED' AND g.lower_bound = p_month_start) THEN
    RETURN 'exists';
  END IF;
  SELECT max(g.upper_bound) INTO v_max FROM control.partition_registry g
   WHERE g.table_key = p_table_key AND g.state = 'ATTACHED';
  IF v_max IS NULL AND p_month_start <> date_trunc('month', now(), 'UTC') THEN
    RAISE EXCEPTION 'partition_create_month: % has no leaf; its first leaf is the current UTC month, not %',
      p_table_key, p_month_start USING ERRCODE = '22023';
  END IF;
  IF v_max IS NOT NULL AND p_month_start <> v_max THEN
    RAISE EXCEPTION 'partition_create_month: the next leaf of % starts at %, not % (no gap, no overlap)',
      p_table_key, v_max, p_month_start USING ERRCODE = '22023';
  END IF;
  -- Interval arithmetic on UTC wall time, so the session TimeZone cannot move the upper bound.
  v_next := (p_month_start AT TIME ZONE 'UTC' + interval '1 month') AT TIME ZONE 'UTC';
  SELECT format('%I.%I', n.nspname, c.relname || '_p' || to_char(p_month_start AT TIME ZONE 'UTC', 'YYYYMM'))
    INTO v_leaf
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = v_parent;
  EXECUTE format('CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%L) TO (%L)', v_leaf, v_parent,
                 to_char(p_month_start AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') || '+00',
                 to_char(v_next AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') || '+00');
  PERFORM control.partition_adopt_leaf(p_table_key, v_leaf::regclass);
  RETURN 'created';
END;
$$;

-- ADR-0063 D-J step 7: the one statement builder; `--dry-run` prints what partition_drop executes.
CREATE FUNCTION control.partition_drop_statements(p_registry_id uuid)
RETURNS text[]
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_key    text;
  v_leaf   text;
  v_parent regclass;
BEGIN
  SELECT g.table_key, g.leaf_name INTO v_key, v_leaf FROM control.partition_registry g
   WHERE g.registry_id = p_registry_id;
  IF v_key IS NULL THEN
    RAISE EXCEPTION 'retention refused: unknown_registry_row';
  END IF;
  SELECT p.parent INTO v_parent FROM control.partition_parent(v_key) p;
  -- Never CASCADE: a dependent object aborts the drop (ADR-0063 D-J, R-36(c)).
  RETURN ARRAY[format('ALTER TABLE %s DETACH PARTITION %s', v_parent, v_leaf),
               format('DROP TABLE %s RESTRICT', v_leaf)];
END;
$$;

-- ADR-0063 D-G / D-J steps 2-6: the database chokepoint. Every predicate lives here; the Rust client only reports the
-- refusal. Invoker + superuser-only + row_security = off: the hold reads see every tenant (F11), and a caller that
-- does not bypass RLS gets the explicit refusal, never a blind "0 unsettled rows".
CREATE FUNCTION control.partition_drop_check(p_policy_id uuid, p_registry_id uuid)
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
  v_bound  text;
  v_lo     text;
  v_hi     text;
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
  -- D-J step 4: the registry must still describe the catalog.
  SELECT p.parent INTO v_parent FROM control.partition_parent(reg.table_key) p;
  SELECT pg_get_expr(c.relpartbound, c.oid) INTO v_bound
    FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid AND i.inhparent = v_parent
   WHERE c.oid = reg.leaf_oid AND c.oid::regclass::text = reg.leaf_name;
  SELECT m[1], m[2] INTO v_lo, v_hi FROM regexp_match(v_bound, '^FOR VALUES FROM \((.*)\) TO \((.*)\)$') m;
  IF v_hi IS NULL
     OR (CASE WHEN v_lo = 'MINVALUE' THEN NULL ELSE btrim(v_lo, '''')::timestamptz END)
        IS DISTINCT FROM reg.lower_bound
     OR btrim(v_hi, '''')::timestamptz IS DISTINCT FROM reg.upper_bound THEN
    RAISE EXCEPTION 'retention refused: stale';
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

-- ADR-0063 D-J step 9: re-derives every predicate itself, takes rows_dropped from its own count, then DETACH and
-- DROP RESTRICT and the receipt, all in the caller's transaction (the §77 audit row follows in the same one).
CREATE FUNCTION control.partition_drop(p_policy_id uuid, p_registry_id uuid, p_rows bigint, p_export_sha256 text,
                                       p_export_path text)
RETURNS text[]
LANGUAGE plpgsql
VOLATILE
SECURITY INVOKER
SET search_path = pg_catalog
SET row_security = off
AS $$
DECLARE
  v_key   text;
  v_rev   integer;
  v_rows  bigint;
  v_stmt  text;
  v_stmts text[];
BEGIN
  IF NOT coalesce((SELECT r.rolsuper FROM pg_roles r WHERE r.rolname = current_user), false) THEN
    RAISE EXCEPTION 'retention refused: executor_not_superuser';
  END IF;
  SELECT g.table_key INTO v_key FROM control.partition_registry g WHERE g.registry_id = p_registry_id;
  -- D-G: no hold-relevant row can appear between the hold read below and COMMIT.
  IF v_key = 'MODEL_CALL_LEDGER' THEN
    LOCK TABLE ops.retrieval_provider_budget_reservations, private.contribution_executions IN SHARE MODE;
  END IF;
  SELECT c.row_count INTO v_rows FROM control.partition_drop_check(p_policy_id, p_registry_id) c;
  IF p_rows IS DISTINCT FROM v_rows THEN
    RAISE EXCEPTION 'retention refused: export_mismatch';
  END IF;
  -- L16: the database cannot read a client-side file; sha256 and path are the executor's attestation.
  IF p_export_sha256 IS NULL OR p_export_sha256 !~ '^[0-9a-f]{64}$' OR coalesce(btrim(p_export_path), '') = '' THEN
    RAISE EXCEPTION 'retention refused: export_unattested';
  END IF;
  v_stmts := control.partition_drop_statements(p_registry_id);
  FOREACH v_stmt IN ARRAY v_stmts LOOP
    EXECUTE v_stmt;
  END LOOP;
  SELECT p.policy_revision INTO v_rev FROM control.retention_policies p WHERE p.policy_id = p_policy_id;
  UPDATE control.partition_registry g
     SET state = 'DROPPED', dropped_at = clock_timestamp(), dropped_by = session_user, drop_policy_id = p_policy_id,
         drop_policy_revision = v_rev, rows_dropped = v_rows, export_path = p_export_path,
         export_sha256 = p_export_sha256
   WHERE g.registry_id = p_registry_id;
  RETURN v_stmts;
END;
$$;

-- ADR-0063 D-I: one approval = the next revision; a concurrent approval loses with 23505 and the operator re-runs.
CREATE FUNCTION control.retention_policy_approve(p_table_key text, p_months integer, p_effective_at timestamptz,
                                                 p_approved_by text)
RETURNS TABLE (policy_id uuid, policy_revision integer)
LANGUAGE sql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  INSERT INTO control.retention_policies AS n
         (table_key, retention_months, policy_revision, approved_by, approved_at, effective_at)
  SELECT p_table_key, p_months, coalesce(max(o.policy_revision), 0) + 1, p_approved_by, clock_timestamp(),
         p_effective_at
    FROM control.retention_policies o WHERE o.table_key = p_table_key
  RETURNING n.policy_id, n.policy_revision
$$;

-- R-36(a) / ADR-0063 D-D step 4: a row never moves between monthly leaves.
CREATE FUNCTION control.reject_partition_key_update()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  RAISE EXCEPTION 'partition key of %.% is frozen (ADR-0063 D-D)', TG_TABLE_SCHEMA, TG_TABLE_NAME;
END;
$$;

ALTER FUNCTION control.partition_parent(text) OWNER TO role_migration_owner;
ALTER FUNCTION control.partition_catalog_fingerprint(text) OWNER TO role_migration_owner;
ALTER FUNCTION control.partition_adopt_leaf(text, regclass) OWNER TO role_migration_owner;
ALTER FUNCTION control.partition_create_month(text, timestamptz) OWNER TO role_migration_owner;
ALTER FUNCTION control.partition_drop_statements(uuid) OWNER TO role_migration_owner;
ALTER FUNCTION control.partition_drop_check(uuid, uuid) OWNER TO role_migration_owner;
ALTER FUNCTION control.partition_drop(uuid, uuid, bigint, text, text) OWNER TO role_migration_owner;
ALTER FUNCTION control.retention_policy_approve(text, integer, timestamptz, text) OWNER TO role_migration_owner;
ALTER FUNCTION control.reject_partition_key_update() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION
  control.partition_parent(text),
  control.partition_catalog_fingerprint(text),
  control.partition_adopt_leaf(text, regclass),
  control.partition_create_month(text, timestamptz),
  control.partition_drop_statements(uuid),
  control.partition_drop_check(uuid, uuid),
  control.partition_drop(uuid, uuid, bigint, text, text),
  control.retention_policy_approve(text, integer, timestamptz, text),
  control.reject_partition_key_update()
FROM PUBLIC;

COMMENT ON TABLE control.retention_policies IS
  'ADR-0063 D-C: fleet-wide monthly retention per table_key; the latest revision is the policy; NULL months = keep.';
COMMENT ON TABLE control.partition_registry IS
  'ADR-0063 D-C: one row per §48.1 leaf (bounds, state, proposal, drop receipt); equals pg_inherits (rls-check).';
