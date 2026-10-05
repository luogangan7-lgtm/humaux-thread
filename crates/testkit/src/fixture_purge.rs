//! `testkit::fixture_purge` — the one teardown of a fixture tenant, and the catalog-driven integrity edges
//!   it and the dev orphan scan share.
//! Depends-on: crates=[]; services=[PostgreSQL(any) w=[control.tenants]]; env=[]; modules=[]
//! Called-by: [tests, xtask::e2e_seed]
//! Invariants: [test infrastructure only, never a runtime path (callers: `e2e-seed --teardown`, the switch_visible and
//!   derived_dispatch_e2e fixtures); the purge names no table: rows are reached through
//!   tenant_id columns, pg_constraint and the ADR-0063 D-B identity rule at run time; a tenant that fails the fixture
//!   predicate, or whose closure reaches another tenant's rows, raises and nothing is purged]
//! Spec: §79.2; ADR-0063 ("Dev integrity finding", D-B)
//!
//! Text builders only (this crate has no normal dependency): the caller runs the text on its own `postgres`
//! connection — `client.batch_execute(&purge_tenant_fixture_sql(&tenant_id.to_string())?)`.

/// The integrity-edge generator (`sql/integrity_edges.sql`): one row per FK (per-leaf clones flagged `inherited`)
/// and per ADR-0063 D-B identity table, with the statement that counts (`scan_sql`) and deletes (`delete_sql`) the
/// rows violating it. Wrap it as a subquery (`select scan_sql from (…) g`); the dev orphan gate
/// (`c36_dev_orphan_repair.sh scan`) reads the same file.
pub const INTEGRITY_EDGES_SQL: &str = include_str!("../sql/integrity_edges.sql");

/// The fixture predicate. A tenant is a fixture tenant when the connected database is a local disposable one (name
/// starts `humaux_thread_`, the binding rule `e2e-seed` applies to its DSN) AND the tenant's name starts with
/// [`FIXTURE_TENANT_PREFIX`] (`e2e-seed-<uuid>` from `e2e-seed`, `e2e-fixture …` from the test fixtures). Anything
/// else is refused before a row is touched.
pub const FIXTURE_TENANT_PREFIX: &str = "e2e-";

/// One `DO` statement that purges fixture tenant `tenant_id` and everything that depends on it, or raises and purges
/// nothing (`Err` here only for a non-canonical uuid). Run it as a superuser; it is one statement, so it is atomic
/// on its own or inside the caller's transaction.
///
/// 1. Refuses unless the tenant exists and passes the fixture predicate ([`FIXTURE_TENANT_PREFIX`]).
/// 2. `SET LOCAL session_replication_role = replica`: the only way past the append-only guards (0128 and the §77
///    audit guard) for test residue. It also skips RI and the ADR-0063 identity release triggers, which is why the
///    rest of this statement must leave no row behind that a trigger or an FK would have handled.
/// 3. Marks, to a fixpoint: every row of every table with a `tenant_id` column equal to the tenant, then every row
///    that references a marked row through any FK (`pg_constraint`, any arity, MATCH SIMPLE) or is the identity row
///    of a marked partitioned-parent row. Refuses if a marked row carries another tenant's id.
/// 4. Deletes children first, by FK depth over the marked tables; then the identity tables (the rows their release
///    triggers would have removed); then the tenant row; then drops its scratch temp tables and restores the caller's
///    replication role — so any number of purges can run in one transaction.
///
/// Refusals the caller must design for (each raises, rolls the statement back and purges nothing):
/// - **Decoys first.** A tenant whose closure reaches another tenant's rows is refused with both tenant ids and the
///   relation (`tenant A reaches rows of tenant B through …`). A test that made tenant A's rows reference a decoy
///   tenant B purges B (the referencing side's dependents) in the order that leaves no cross-tenant edge, or purges
///   both in one transaction in that order.
/// - **No concurrent DDL on `tenant_id` tables.** The purge reads the catalog when it runs; a table dropped by another
///   session meanwhile is refused as `relation … vanished while the purge ran`. A caller whose sibling tests create
///   and drop `tenant_id` tables holds the advisory lock those tests take around their DDL for the purge (the 0137
///   cleanup holds its guards' DDL lock).
pub fn purge_tenant_fixture_sql(tenant_id: &str) -> Result<String, String> {
    let canonical = tenant_id.len() == 36
        && tenant_id.char_indices().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        });
    if !canonical {
        return Err(format!(
            "fixture purge: {tenant_id:?} is not a canonical uuid"
        ));
    }
    Ok(PURGE_TEMPLATE
        .replace("{prefix}", FIXTURE_TENANT_PREFIX)
        .replace("{tenant}", tenant_id)
        .replace("{edges}", INTEGRITY_EDGES_SQL))
}

const PURGE_TEMPLATE: &str = r"DO $fixture_purge$
DECLARE
  v_tenant constant uuid := '{tenant}';
  v_role constant text := current_setting('session_replication_role');
  v_name text;
  v_pass integer := 0;
  v_new bigint;
  v_n bigint;
  v_rels integer;
  v_cross text;
  v_e record;
  v_hit text;
  v_cur text;
  v_cur_oid oid;
BEGIN
  SELECT t.name INTO v_name FROM control.tenants t WHERE t.tenant_id = v_tenant;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'fixture purge: tenant % does not exist', v_tenant;
  END IF;
  IF current_database() NOT LIKE 'humaux\_thread\_%' OR coalesce(v_name, '') NOT LIKE '{prefix}%' THEN
    RAISE EXCEPTION 'fixture purge: refusing tenant % (name %) in database %: not a fixture tenant',
      v_tenant, v_name, current_database();
  END IF;
  SET LOCAL session_replication_role = replica;

  -- `hit` is the open predicate `<child row references a marked parent row>`; the caller closes the IN subquery.
  CREATE TEMP TABLE fixture_purge_edges ON COMMIT DROP AS
    SELECT g.kind, g.child, g.parent, g.child::text AS child_name,
           (SELECT string_agg(format('c.%I IS NOT NULL', u.cc), ' AND ' ORDER BY u.i)
                   || format(' AND (%s) IN (SELECT %s FROM pg_temp.fixture_purge_doomed d WHERE d.rel = %L::regclass',
                             string_agg(format('c.%I', u.cc), ', ' ORDER BY u.i),
                             string_agg(format('(d.doc->>%L)::%s', u.pc, u.ty), ', ' ORDER BY u.i), g.parent)
              FROM unnest(g.child_cols, g.parent_cols, g.child_types) WITH ORDINALITY u(cc, pc, ty, i)) AS hit
      FROM ({edges}) g
     WHERE NOT g.inherited;
  CREATE TEMP TABLE fixture_purge_doomed (
    rel regclass NOT NULL,
    pass integer NOT NULL,
    doc jsonb NOT NULL,
    k text GENERATED ALWAYS AS (md5(doc::text)) STORED,
    UNIQUE (rel, k)
  ) ON COMMIT DROP;

  -- v_cur / v_cur_oid name the relation each dynamic statement touches, so a table dropped by another session between
  -- the catalog read and the statement is reported by name (the handler at the end of this block), not as 42601.
  BEGIN
  -- Mark: pass 0 is every tenant_id row, pass n+1 every row referencing a pass-n row, until nothing new.
  FOR v_e IN
    SELECT c.oid::regclass AS rel, c.oid, format('%I.%I', n.nspname, c.relname) AS name FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'tenant_id' AND NOT a.attisdropped
     WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition
  LOOP
    v_cur := v_e.name; v_cur_oid := v_e.oid;
    EXECUTE format('INSERT INTO pg_temp.fixture_purge_doomed (rel, pass, doc) SELECT %L, 0, to_jsonb(c.*) FROM %s c '
                   'WHERE c.tenant_id = %L ON CONFLICT DO NOTHING', v_e.rel, v_e.name, v_tenant);
  END LOOP;
  LOOP
    v_new := 0;
    FOR v_e IN
      SELECT e.child, e.child_name, e.hit FROM pg_temp.fixture_purge_edges e
       WHERE EXISTS (SELECT 1 FROM pg_temp.fixture_purge_doomed d WHERE d.rel = e.parent AND d.pass = v_pass)
    LOOP
      v_cur := v_e.child_name; v_cur_oid := v_e.child;
      EXECUTE format('INSERT INTO pg_temp.fixture_purge_doomed (rel, pass, doc) SELECT %L, %s, to_jsonb(c.*) FROM %s c '
                     'WHERE %s AND d.pass = %s) ON CONFLICT DO NOTHING', v_e.child, v_pass + 1, v_e.child, v_e.hit,
                     v_pass);
      GET DIAGNOSTICS v_n = ROW_COUNT;
      v_new := v_new + v_n;
    END LOOP;
    EXIT WHEN v_new = 0;
    v_pass := v_pass + 1;
  END LOOP;

  SELECT string_agg(DISTINCT format('rows of tenant %s through %s', d.doc->>'tenant_id', d.rel), ', ') INTO v_cross
    FROM pg_temp.fixture_purge_doomed d
   WHERE (d.doc->>'tenant_id') <> v_tenant::text;
  IF v_cross IS NOT NULL THEN
    RAISE EXCEPTION 'fixture purge: tenant % reaches %; purge the referencing tenant first (decoys first, see rustdoc)',
      v_tenant, v_cross;
  END IF;

  -- Depth: the longest FK path from a marked root (self-references ignored; a cycle stops at the table count).
  CREATE TEMP TABLE fixture_purge_depth ON COMMIT DROP AS
    SELECT DISTINCT d.rel, 0 AS depth FROM pg_temp.fixture_purge_doomed d;
  SELECT count(*) INTO v_rels FROM pg_temp.fixture_purge_depth;
  FOR v_i IN 1 .. v_rels LOOP
    UPDATE pg_temp.fixture_purge_depth c SET depth = p.depth + 1
      FROM pg_temp.fixture_purge_edges e JOIN pg_temp.fixture_purge_depth p ON p.rel = e.parent
     WHERE e.child = c.rel AND e.child <> e.parent AND c.depth < p.depth + 1;
    EXIT WHEN NOT FOUND;
  END LOOP;

  -- Delete: children first by depth, the identity tables after every other table (the rows their skipped release
  -- triggers would have removed, ADR-0063 D-B), the tenant row last. Each table's rows are deleted by the same
  -- predicates that marked them, so the whole marked set goes and nothing else.
  FOR v_e IN
    SELECT p.rel, EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = p.rel AND a.attname = 'tenant_id'
                                                        AND NOT a.attisdropped) AS keyed
      FROM pg_temp.fixture_purge_depth p
     WHERE p.rel <> 'control.tenants'::regclass
     ORDER BY EXISTS (SELECT 1 FROM pg_temp.fixture_purge_edges e WHERE e.kind = 'identity' AND e.child = p.rel),
              p.depth DESC, p.rel::text
  LOOP
    v_cur := v_e.rel::text; v_cur_oid := v_e.rel;
    IF v_e.keyed THEN
      EXECUTE format('DELETE FROM %s c WHERE c.tenant_id = %L', v_e.rel, v_tenant);
    END IF;
    FOR v_hit IN SELECT e.hit FROM pg_temp.fixture_purge_edges e WHERE e.child = v_e.rel LOOP
      EXECUTE format('DELETE FROM %s c WHERE %s)', v_e.rel, v_hit);
    END LOOP;
  END LOOP;
  EXCEPTION WHEN syntax_error OR undefined_table OR internal_error THEN
    IF v_cur_oid IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = v_cur_oid) THEN
      RAISE EXCEPTION 'fixture purge: relation % (oid %) vanished while the purge ran (a concurrent DROP; hold the '
                      'dropping tests'' DDL lock, see rustdoc): %', v_cur, v_cur_oid, SQLERRM;
    END IF;
    RAISE;
  END;
  DELETE FROM control.tenants WHERE tenant_id = v_tenant;
  -- ON COMMIT DROP alone left them for the rest of the transaction: a second purge in it failed 42P07.
  DROP TABLE pg_temp.fixture_purge_edges, pg_temp.fixture_purge_doomed, pg_temp.fixture_purge_depth;
  PERFORM set_config('session_replication_role', v_role, true);
END
$fixture_purge$";

#[cfg(test)]
mod tests {
    use super::*;

    /// The builder refuses anything but a canonical uuid (the text is spliced into the statement).
    /// Fault: drop the check ⇒ `x'; DROP …` is accepted.
    #[test]
    fn purge_sql_takes_a_canonical_uuid_only() {
        let id = "0192f0c4-7e1a-7b3c-9d2e-0123456789ab";
        let sql = purge_tenant_fixture_sql(id).expect("canonical");
        assert!(sql.contains(&format!("v_tenant constant uuid := '{id}'")));
        assert!(sql.contains("LIKE 'e2e-%'"));
        assert!(!sql.contains("{edges}"));
        for bad in [
            "",
            "x'; DROP TABLE control.tenants; --",
            "0192f0c4-7e1a-7b3c-9d2e-0123456789a'",
        ] {
            assert!(purge_tenant_fixture_sql(bad).is_err(), "{bad:?}");
        }
    }
}
