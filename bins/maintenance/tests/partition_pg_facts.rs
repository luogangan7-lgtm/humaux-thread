//! `maintenance::tests::partition_pg_facts` — the PostgreSQL 18 behaviours ADR-0063 builds on (design §5, spike
//!   P1–P10), each pinned on scratch tables in its own bare throwaway database. A red test here means the server
//!   no longer behaves the way the partition machinery (0224) and the conversions (0225..0230) assume.
//! Depends-on: crates=[postgres]; services=[PostgreSQL(owner), PostgreSQL(role_maintenance),
//!   PostgreSQL(role_migration_owner)]; env=[]; modules=[maintenance::tests::support::throwaway]
//! Called-by: [cargo-test]
//! Invariants: [each test owns humaux_thread_c36_facts_<pid>_<n> with no migration applied (roles are cluster-wide
//!   and only used through SET ROLE and table grants inside it), dropped WITH (FORCE) by the fixture's Drop even
//!   on panic; nothing here touches the shared dev database]
//! Spec: Baseline §48.1; ADR-0063 D-B, D-D, D-E, D-F, D-G, D-H, D-J

#[path = "support/throwaway.rs"]
#[allow(dead_code)]
mod throwaway;
use postgres::Client;
use throwaway::Db;

/// A bare throwaway with the scratch schema `c36`, owned by the owner role like every Humaux schema.
fn bare(test: &str) -> Option<Db> {
    // dep: PostgreSQL(owner) — a bare throwaway database and its scratch schema
    let mut db = throwaway::db_through(test, "c36_facts", Some("0000"))?;
    db.sql(
        "CREATE SCHEMA c36 AUTHORIZATION role_migration_owner;
         GRANT USAGE ON SCHEMA c36 TO role_maintenance",
    );
    Some(db)
}

/// `(sqlstate, message)` of a statement that must fail.
fn refused(client: &mut Client, sql: &str) -> (String, String) {
    let e = client
        .batch_execute(sql)
        .expect_err(&format!("expected an error from: {sql}"));
    let db = e
        .as_db_error()
        .unwrap_or_else(|| panic!("not a server error: {e}"));
    (db.code().code().to_owned(), db.message().to_owned())
}

fn scalar_i64(client: &mut Client, sql: &str) -> i64 {
    client.query_one(sql, &[]).expect(sql).get(0)
}

/// P1 (D-D step 4, F7): `LIKE … INCLUDING IDENTITY` or `ADD GENERATED … AS IDENTITY` on the new parent starts a
/// fresh sequence at 1, below the legacy maximum; after `DROP IDENTITY` on the legacy heap, ATTACH and a RESTART
/// above the maximum, the next insert through the parent continues above it.
#[test]
fn identity_restart_after_drop_identity_and_attach() {
    let Some(mut db) = bare("identity_restart_after_drop_identity_and_attach") else {
        return;
    };
    db.sql(
        "CREATE TABLE c36.a_legacy (id bigint GENERATED ALWAYS AS IDENTITY, ts timestamptz NOT NULL);
         INSERT INTO c36.a_legacy (ts) SELECT '2026-08-15+00' FROM generate_series(1, 3);
         ALTER TABLE c36.a_legacy ALTER COLUMN id DROP IDENTITY;
         CREATE TABLE c36.a (LIKE c36.a_legacy INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
           PARTITION BY RANGE (ts);
         ALTER TABLE c36.a ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY;
         ALTER TABLE c36.a ATTACH PARTITION c36.a_legacy FOR VALUES FROM (MINVALUE) TO ('2026-09-01+00');
         CREATE TABLE c36.a_p202609 PARTITION OF c36.a FOR VALUES FROM ('2026-09-01+00') TO ('2026-10-01+00')",
    );
    let c = db.client();
    let start = scalar_i64(
        c,
        "SELECT s.start_value FROM pg_sequences s
          WHERE format('%I.%I', s.schemaname, s.sequencename) = pg_get_serial_sequence('c36.a', 'id')",
    );
    assert_eq!(
        start, 1,
        "a new identity on the parent starts at 1 (F7: it would collide)"
    );
    c.batch_execute(
        "DO $$ BEGIN
           EXECUTE format('ALTER TABLE c36.a ALTER COLUMN id RESTART WITH %s', (SELECT max(id) + 1 FROM c36.a));
         END $$",
    )
    .expect("restart above the legacy maximum");
    let next = scalar_i64(
        c,
        "INSERT INTO c36.a (ts) VALUES ('2026-09-02+00') RETURNING id",
    );
    assert_eq!(next, 4, "the next id continues above the legacy maximum 3");
    assert_eq!(
        scalar_i64(c, "SELECT count(DISTINCT id) FROM c36.a"),
        4,
        "ids stay unique across the legacy leaf and the new leaf"
    );
}

/// P2 (D-B, F4): a BEFORE INSERT row trigger on a partitioned parent that returns NULL skips the row and its
/// RETURNING; same-timing triggers fire in name order, so a validator named before the claim runs first.
#[test]
fn before_insert_trigger_returning_null_skips_the_row_and_returning() {
    let Some(mut db) = bare("before_insert_trigger_returning_null_skips_the_row_and_returning")
    else {
        return;
    };
    db.sql(
        "CREATE TABLE c36.claims (k text PRIMARY KEY);
         CREATE TABLE c36.b (k text NOT NULL, ts timestamptz NOT NULL) PARTITION BY RANGE (ts);
         CREATE TABLE c36.b_p PARTITION OF c36.b FOR VALUES FROM ('2026-01-01+00') TO ('2027-01-01+00');
         CREATE FUNCTION c36.b_validate() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF NEW.k = 'bad' THEN RAISE EXCEPTION 'c36 validator refused %', NEW.k; END IF;
           RETURN NEW;
         END $$;
         CREATE FUNCTION c36.b_claim() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO c36.claims VALUES (NEW.k) ON CONFLICT DO NOTHING;
           IF NOT FOUND THEN RETURN NULL; END IF;
           RETURN NEW;
         END $$;
         CREATE TRIGGER b_2_claim BEFORE INSERT ON c36.b FOR EACH ROW EXECUTE FUNCTION c36.b_claim();
         CREATE TRIGGER b_1_validate BEFORE INSERT ON c36.b FOR EACH ROW EXECUTE FUNCTION c36.b_validate()",
    );
    let c = db.client();
    let insert = "INSERT INTO c36.b VALUES ('x', '2026-05-01+00') RETURNING k";
    assert_eq!(
        c.query(insert, &[]).expect("first insert").len(),
        1,
        "first claim returns its row"
    );
    assert_eq!(
        c.query(insert, &[]).expect("duplicate insert").len(),
        0,
        "a duplicate claim returns no row and raises nothing"
    );
    assert_eq!(scalar_i64(c, "SELECT count(*) FROM c36.b"), 1);
    let (_, message) = refused(c, "INSERT INTO c36.b VALUES ('bad', '2026-05-01+00')");
    assert!(message.starts_with("c36 validator refused"), "{message}");
    assert_eq!(
        scalar_i64(c, "SELECT count(*) FROM c36.claims WHERE k = 'bad'"),
        0,
        "the validator (sorted first by name) ran before the claim"
    );
}

/// P3 (D-D step 4, F9): a deferred constraint trigger created on a partitioned parent fires at COMMIT.
#[test]
fn deferred_constraint_trigger_on_a_partitioned_parent_fires_at_commit() {
    let Some(mut db) = bare("deferred_constraint_trigger_on_a_partitioned_parent_fires_at_commit")
    else {
        return;
    };
    db.sql(
        "CREATE TABLE c36.companion (k text PRIMARY KEY);
         CREATE TABLE c36.c (k text NOT NULL, ts timestamptz NOT NULL) PARTITION BY RANGE (ts);
         CREATE TABLE c36.c_p PARTITION OF c36.c FOR VALUES FROM ('2026-01-01+00') TO ('2027-01-01+00');
         CREATE FUNCTION c36.c_companion_present() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF NOT EXISTS (SELECT 1 FROM c36.companion WHERE k = NEW.k) THEN
             RAISE EXCEPTION 'c36 companion missing for %', NEW.k;
           END IF;
           RETURN NULL;
         END $$;
         CREATE CONSTRAINT TRIGGER c_companion AFTER INSERT ON c36.c DEFERRABLE INITIALLY DEFERRED
           FOR EACH ROW EXECUTE FUNCTION c36.c_companion_present()",
    );
    let c = db.client();
    let mut tx = c.transaction().expect("begin");
    tx.batch_execute("INSERT INTO c36.c VALUES ('x', '2026-05-01+00')")
        .expect("the insert itself passes: the check is deferred");
    let e = tx
        .commit()
        .expect_err("COMMIT must fire the deferred trigger");
    let message = e
        .as_db_error()
        .map(|d| d.message().to_owned())
        .unwrap_or_default();
    assert!(message.starts_with("c36 companion missing for x"), "{e}");
    let mut tx = c.transaction().expect("begin");
    tx.batch_execute(
        "INSERT INTO c36.c VALUES ('y', '2026-05-01+00'); INSERT INTO c36.companion VALUES ('y')",
    )
    .expect("insert with its companion");
    tx.commit().expect("companion present at COMMIT");
    assert_eq!(scalar_i64(c, "SELECT count(*) FROM c36.c"), 1);
}

/// P4 (F11, D-G): FORCE RLS blinds the owner (0 rows without the tenant GUC), and `row_security = off` makes the
/// same read raise 42501 instead of filtering.
#[test]
fn row_security_off_raises_for_a_forced_owner() {
    let Some(mut db) = bare("row_security_off_raises_for_a_forced_owner") else {
        return;
    };
    db.sql(
        "CREATE TABLE c36.d (tenant_id uuid NOT NULL, v int);
         ALTER TABLE c36.d OWNER TO role_migration_owner;
         ALTER TABLE c36.d ENABLE ROW LEVEL SECURITY;
         ALTER TABLE c36.d FORCE ROW LEVEL SECURITY;
         CREATE POLICY d_tenant ON c36.d
           USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);
         INSERT INTO c36.d VALUES (gen_random_uuid(), 1), (gen_random_uuid(), 2)",
    );
    let c = db.client();
    assert_eq!(
        scalar_i64(c, "SELECT count(*) FROM c36.d"),
        2,
        "the superuser sees both tenants"
    );
    let mut tx = c.transaction().expect("begin");
    // dep: PostgreSQL(role_migration_owner) — role switch: the owner's own view of its FORCE-RLS table
    tx.batch_execute("SET LOCAL ROLE role_migration_owner")
        .expect("set role");
    let blind: i64 = tx
        .query_one("SELECT count(*) FROM c36.d", &[])
        .expect("owner read")
        .get(0);
    assert_eq!(
        blind, 0,
        "FORCE RLS: the owner sees nothing without the GUC"
    );
    tx.batch_execute("SET LOCAL row_security = off")
        .expect("row_security off");
    let e = tx
        .query_one("SELECT count(*) FROM c36.d", &[])
        .expect_err("row_security = off must raise for a principal that does not bypass RLS");
    assert_eq!(e.code().map(|s| s.code()), Some("42501"), "{e}");
}

/// P5 (F9, D-E): statement-level triggers of a partitioned parent are not cloned to its partitions.
#[test]
fn statement_triggers_are_not_cloned_to_leaves() {
    let Some(mut db) = bare("statement_triggers_are_not_cloned_to_leaves") else {
        return;
    };
    db.sql(
        "CREATE FUNCTION c36.no_truncate() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'c36 truncate refused'; END $$;
         CREATE TABLE c36.e (ts timestamptz NOT NULL) PARTITION BY RANGE (ts);
         CREATE TRIGGER e_no_truncate BEFORE TRUNCATE ON c36.e FOR EACH STATEMENT
           EXECUTE FUNCTION c36.no_truncate();
         CREATE TABLE c36.e_p PARTITION OF c36.e FOR VALUES FROM ('2026-01-01+00') TO ('2027-01-01+00');
         INSERT INTO c36.e VALUES ('2026-05-01+00')",
    );
    let c = db.client();
    assert_eq!(
        scalar_i64(
            c,
            "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'c36.e_p'::regclass AND NOT tgisinternal"
        ),
        0,
        "the leaf carries no copy of the statement trigger"
    );
    let (_, message) = refused(c, "TRUNCATE c36.e");
    assert!(message.starts_with("c36 truncate refused"), "{message}");
    c.batch_execute("TRUNCATE c36.e_p")
        .expect("the leaf truncates: nothing guards it unless the adopter copies the trigger");
    assert_eq!(scalar_i64(c, "SELECT count(*) FROM c36.e"), 0);
}

/// P6 (D-F): without a DEFAULT partition, an insert past the last leaf fails with 23514 "no partition of relation".
#[test]
fn insert_beyond_the_last_leaf_is_23514() {
    let Some(mut db) = bare("insert_beyond_the_last_leaf_is_23514") else {
        return;
    };
    db.sql(
        "CREATE TABLE c36.h6 (ts timestamptz NOT NULL) PARTITION BY RANGE (ts);
         CREATE TABLE c36.h6_p202610 PARTITION OF c36.h6
           FOR VALUES FROM ('2026-10-01+00') TO ('2026-11-01+00')",
    );
    let c = db.client();
    c.batch_execute("INSERT INTO c36.h6 VALUES ('2026-10-31 23:59:59+00')")
        .expect("inside the last leaf");
    let (state, message) = refused(c, "INSERT INTO c36.h6 VALUES ('2026-11-01 00:00:00+00')");
    assert_eq!(state, "23514", "{message}");
    assert!(message.starts_with("no partition of relation"), "{message}");
}

/// P7 (D-D step 5): ATTACH of a heap whose valid CHECK implies the bound does not scan it; ATTACH without one does.
#[test]
fn attach_with_a_valid_check_skips_the_scan() {
    let Some(mut db) = bare("attach_with_a_valid_check_skips_the_scan") else {
        return;
    };
    db.sql(
        "CREATE TABLE c36.f (id int NOT NULL, ts timestamptz NOT NULL) PARTITION BY RANGE (ts);
         CREATE TABLE c36.f_legacy (id int NOT NULL, ts timestamptz NOT NULL);
         CREATE TABLE c36.g (id int NOT NULL, ts timestamptz NOT NULL) PARTITION BY RANGE (ts);
         CREATE TABLE c36.g_legacy (id int NOT NULL, ts timestamptz NOT NULL);
         INSERT INTO c36.f_legacy SELECT i, '2026-08-15+00' FROM generate_series(1, 1000) i;
         INSERT INTO c36.g_legacy SELECT i, '2026-08-15+00' FROM generate_series(1, 1000) i",
    );
    let c = db.client();
    let mut tx = c.transaction().expect("begin");
    tx.batch_execute(
        "ALTER TABLE c36.f_legacy ADD CONSTRAINT f_legacy_bound
           CHECK (ts IS NOT NULL AND ts < '2026-09-01+00') NOT VALID;
         ALTER TABLE c36.f_legacy VALIDATE CONSTRAINT f_legacy_bound",
    )
    .expect("bound check");
    let scans = |tx: &mut postgres::Transaction, rel: &str| -> i64 {
        tx.query_one("SELECT pg_stat_get_xact_numscans(to_regclass($1))", &[&rel])
            .expect("xact scans")
            .get(0)
    };
    let before = scans(&mut tx, "c36.f_legacy");
    tx.batch_execute(
        "ALTER TABLE c36.f ATTACH PARTITION c36.f_legacy FOR VALUES FROM (MINVALUE) TO ('2026-09-01+00')",
    )
    .expect("attach with check");
    assert_eq!(
        scans(&mut tx, "c36.f_legacy"),
        before,
        "the valid CHECK lets ATTACH skip its scan"
    );
    let before = scans(&mut tx, "c36.g_legacy");
    tx.batch_execute(
        "ALTER TABLE c36.g ATTACH PARTITION c36.g_legacy FOR VALUES FROM (MINVALUE) TO ('2026-09-01+00')",
    )
    .expect("attach without check");
    assert!(
        scans(&mut tx, "c36.g_legacy") > before,
        "control: without the CHECK, ATTACH scans the heap (the counter observes scans)"
    );
    tx.commit().expect("commit");
}

/// P8 (D-J DETACH mode): a plain DETACH holds ACCESS EXCLUSIVE on the parent until the transaction ends.
#[test]
fn plain_detach_takes_access_exclusive_on_the_parent() {
    let Some(mut db) = bare("plain_detach_takes_access_exclusive_on_the_parent") else {
        return;
    };
    db.sql(
        "CREATE TABLE c36.h (ts timestamptz NOT NULL) PARTITION BY RANGE (ts);
         CREATE TABLE c36.h_p PARTITION OF c36.h FOR VALUES FROM ('2026-01-01+00') TO ('2027-01-01+00')",
    );
    let c = db.client();
    let mut tx = c.transaction().expect("begin");
    tx.batch_execute("ALTER TABLE c36.h DETACH PARTITION c36.h_p")
        .expect("detach");
    let held: bool = tx
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'relation' AND pid = pg_backend_pid()
                              AND relation = 'c36.h'::regclass AND mode = 'AccessExclusiveLock' AND granted)",
            &[],
        )
        .expect("pg_locks")
        .get(0);
    assert!(held, "plain DETACH holds ACCESS EXCLUSIVE on the parent");
    tx.rollback().expect("rollback");
}

/// P9 (D-G, D-J step 10, review finding 5): a function-level `SET row_security = off` lasts only for the call. A
/// superuser reads every tenant inside it; afterwards `row_security` is `on` again and an owner definer's FORCE-RLS
/// insert under the tenant GUC succeeds in the same transaction. Control: a transaction-level `off` makes that same
/// insert raise 42501.
#[test]
fn function_level_row_security_off_is_scoped_to_the_call() {
    let Some(mut db) = bare("function_level_row_security_off_is_scoped_to_the_call") else {
        return;
    };
    db.sql(
        "CREATE TABLE c36.k (tenant_id uuid NOT NULL, v int);
         ALTER TABLE c36.k OWNER TO role_migration_owner;
         ALTER TABLE c36.k ENABLE ROW LEVEL SECURITY;
         ALTER TABLE c36.k FORCE ROW LEVEL SECURITY;
         CREATE POLICY k_tenant ON c36.k
           USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
           WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);
         INSERT INTO c36.k VALUES ('00000000-0000-7000-8000-000000000001', 1),
                                  ('00000000-0000-7000-8000-000000000002', 2);
         CREATE FUNCTION c36.read_all() RETURNS bigint LANGUAGE sql SECURITY INVOKER
           SET search_path = pg_catalog SET row_security = off AS $$ SELECT count(*) FROM c36.k $$;
         ALTER FUNCTION c36.read_all() OWNER TO role_migration_owner;
         CREATE FUNCTION c36.owner_insert(p uuid) RETURNS void LANGUAGE sql SECURITY DEFINER
           SET search_path = pg_catalog AS $$ INSERT INTO c36.k VALUES (p, 9) $$;
         ALTER FUNCTION c36.owner_insert(uuid) OWNER TO role_migration_owner",
    );
    let c = db.client();
    let mut tx = c.transaction().expect("begin");
    let all: i64 = tx
        .query_one("SELECT c36.read_all()", &[])
        .expect("read_all")
        .get(0);
    assert_eq!(all, 2, "inside the call the superuser reads every tenant");
    let after: String = tx
        .query_one("SELECT current_setting('row_security')", &[])
        .expect("setting")
        .get(0);
    assert_eq!(after, "on", "the function attribute ended with the call");
    tx.batch_execute(
        "SELECT set_config('humaux.tenant_id', '00000000-0000-7000-8000-000000000001', true);
         SELECT c36.owner_insert('00000000-0000-7000-8000-000000000001')",
    )
    .expect("owner definer insert after the call, same transaction");
    tx.commit().expect("commit");
    let mut tx = c.transaction().expect("begin");
    let e = tx
        .batch_execute(
            "SET LOCAL row_security = off;
             SELECT set_config('humaux.tenant_id', '00000000-0000-7000-8000-000000000001', true);
             SELECT c36.owner_insert('00000000-0000-7000-8000-000000000001')",
        )
        .expect_err("control: a transaction-level off breaks the owner definer");
    assert_eq!(e.code().map(|s| s.code()), Some("42501"), "{e}");
    drop(tx);
    assert_eq!(scalar_i64(c, "SELECT count(*) FROM c36.k"), 3);
}

/// P10 (D-H): `LOCK … IN SHARE MODE` needs a table privilege beyond SELECT, which no runtime role may hold on a
/// leaf (D-L); this is why the executor is a superuser.
#[test]
fn share_lock_on_a_leaf_needs_a_table_privilege_beyond_select() {
    let Some(mut db) = bare("share_lock_on_a_leaf_needs_a_table_privilege_beyond_select") else {
        return;
    };
    db.sql("CREATE TABLE c36.m (v int); GRANT SELECT ON c36.m TO role_maintenance");
    let c = db.client();
    let mut tx = c.transaction().expect("begin");
    // dep: PostgreSQL(role_maintenance) — role switch: a SELECT-only role takes the two lock modes
    tx.batch_execute("SET LOCAL ROLE role_maintenance; LOCK TABLE c36.m IN ACCESS SHARE MODE")
        .expect("control: SELECT suffices for ACCESS SHARE");
    tx.rollback().expect("rollback");
    // dep: PostgreSQL(role_maintenance) — role switch: SHARE needs more than SELECT
    let (state, message) = refused(
        c,
        "BEGIN; SET LOCAL ROLE role_maintenance; LOCK TABLE c36.m IN SHARE MODE",
    );
    c.batch_execute("ROLLBACK").expect("end the failed block");
    assert_eq!(state, "42501", "{message}");
}
