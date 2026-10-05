//! `maintenance::tests::retention` — the §48.1 retention machinery of 0224 and the one-shot superuser executor
//!   `humaux-maintenance retention approve | create-partitions | execute` against throwaway PostgreSQL databases
//!   migrated to head (ADR-0063 D-C, D-F..D-J): which policies can exist at all, who may run the executor, the run-once
//!   key, every hold and refusal re-derived in the database (also when `control.partition_drop` is called directly),
//!   the export (mode 0600) / DETACH / DROP RESTRICT / receipt / §77 commit, the re-run no-op, the dry runs, the
//!   creator's idempotence and its closed --months-ahead range, the daemon's horizon value per missed month, and the
//!   table_key closed set against the Rust enum.
//! Depends-on: crates=[humaux-adapters, postgres, serde_json, tokio, uuid]; services=[PostgreSQL(owner)
//!   r=[control.audit_events, control.partition_registry] w=[control.partition_registry, control.retention_policies,
//!   control.tenants,
//!   ops.maintenance_receipts, ops.model_call_ledger, ops.retrieval_provider_budget_reservations, ops.stage_runs,
//!   private.contribution_executions] x=[control.partition_create_month, control.partition_drop,
//!   control.partition_drop_check, control.partition_parent, control.retention_policy_approve], PostgreSQL(role_maintenance),
//!   PostgreSQL(role_migration_owner), subprocess(humaux-maintenance), subprocess(shasum)];
//!   env=[CARGO_TARGET_TMPDIR, HUMAUX_MIGRATOR_PG_DSN]; modules=[adapters::maintenance_repo, adapters::postgres,
//!   maintenance::tests::support::throwaway]
//! Called-by: [cargo-test]
//! Invariants: [each test owns humaux_thread_c36_retention_<pid>_<n>, dropped WITH (FORCE) by the fixture's Drop
//!   even on panic, and its own export directory under the target dir; the executor runs only against that throwaway
//!   (HUMAUX_MIGRATOR_PG_DSN is set per spawned run to the throwaway's owner DSN); the one cluster role a test creates
//!   is dropped by its guard; no retention runs against the shared dev database, ever (ADR-0063 D-M)]
//! Spec: Baseline §48.1; §77; ADR-0063 D-C; ADR-0063 D-F; ADR-0063 D-G; ADR-0063 D-H; ADR-0063 D-I; ADR-0063 D-J

#[path = "support/throwaway.rs"]
#[allow(dead_code)]
mod throwaway;

use std::path::{Path, PathBuf};
use std::process::Output;

use humaux_adapters::maintenance_repo::{self, PartitionTable, RETENTION_ADVISORY_LOCK};
use humaux_adapters::postgres::MaintenanceDbPool;
use postgres::Client;
use serde_json::Value;
use throwaway::{Db, effective_policy, forge_proposal, past_leaf, with_db};

/// T-G1 (ADR-0063 D-G): no policy row can exist for EVENTS (card 37's rebuild baseline) or AUDIT_EVENTS (a separate
/// approval line), neither through the approve door nor by a direct owner INSERT, while the other keys get
/// consecutive revisions. Fault: widen `retention_policies_table_key` in 0224 ⇒ the EVENTS approval commits ⇒ red.
#[test]
fn events_and_audit_policies_cannot_be_written() {
    let Some(mut db) = throwaway::db(
        "events_and_audit_policies_cannot_be_written",
        "c36_retention",
    ) else {
        return;
    };
    let c = db.client();
    for key in ["EVENTS", "AUDIT_EVENTS"] {
        for sql in [
            "SELECT * FROM control.retention_policy_approve($1, 1, clock_timestamp() + interval '1 day', 'c36 test')",
            "INSERT INTO control.retention_policies (table_key, retention_months, policy_revision, approved_by, \
             effective_at) VALUES ($1, 1, 1, 'c36 test', clock_timestamp() + interval '1 day')",
        ] {
            let e = c
                .execute(sql, &[&key])
                .expect_err(&format!("a {key} policy must be refused: {sql}"));
            let db_error = e.as_db_error().expect("server error");
            assert_eq!(db_error.code().code(), "23514", "{key}: {e}");
            assert_eq!(
                db_error.constraint(),
                Some("retention_policies_table_key"),
                "{key}: {e}"
            );
        }
    }
    for expected in [1, 2] {
        let row = c
            .query_one(
                "SELECT policy_revision FROM control.retention_policy_approve('STAGE_RUNS', 1, \
                 clock_timestamp() + interval '1 day', 'c36 test')",
                &[],
            )
            .expect("control: STAGE_RUNS is approvable");
        assert_eq!(row.get::<_, i32>(0), expected, "the next revision");
    }
    let rows: i64 = c
        .query_one(
            "SELECT count(*) FROM control.retention_policies WHERE table_key IN ('EVENTS', 'AUDIT_EVENTS')",
            &[],
        )
        .expect("count")
        .get(0);
    assert_eq!(rows, 0);
}

/// The §77 fields every writing subcommand requires.
const ADMIN: [&str; 8] = [
    "--actor",
    "ops@example.test",
    "--reason",
    "card 36 S6",
    "--ticket",
    "T-C36",
    "--step-up-auth",
    "test-step-up",
];
const MIGRATOR_DSN: &str = "HUMAUX_MIGRATOR_PG_DSN";

fn head(test: &str) -> Option<Db> {
    // dep: PostgreSQL(owner) — a throwaway migrated 0001→head
    throwaway::db(test, "c36_retention")
}

/// This test's own empty export directory (under the cargo target dir, removed first).
fn export_dir(test: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("c36_{test}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("export dir");
    dir
}

fn exports(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("export dir")
        .map(|e| e.expect("entry").path())
        .collect();
    files.sort();
    files
}

/// One `humaux-maintenance retention <args> + ADMIN` run whose migrator DSN is `dsn`.
fn retention_as(dsn: &str, args: &[&str]) -> Output {
    let mut all: Vec<&str> = vec!["retention"];
    all.extend(args);
    all.extend(ADMIN);
    // dep: subprocess(humaux-maintenance) — one one-shot executor run against this throwaway
    throwaway::run(&all, &[(MIGRATOR_DSN, dsn.to_owned())])
}

/// One `retention` run as this throwaway's superuser owner.
fn retention(db: &Db, args: &[&str]) -> Output {
    retention_as(&with_db(&db.owner_dsn, &db.name), args)
}

/// `(exit code, receipt, stderr)`; the receipt is the last stdout line as JSON (`Null` when there is none).
fn outcome(out: &Output) -> (Option<i32>, Value, String) {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let receipt = stdout
        .lines()
        .last()
        .and_then(|l| serde_json::from_str(l).ok())
        .unwrap_or(Value::Null);
    (
        out.status.code(),
        receipt,
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// `retention execute` of one registry row (or the listing, `registry: None`) with a 5 s lock timeout.
fn execute(db: &Db, policy: &str, registry: Option<&str>, dir: &Path, dry_run: bool) -> Output {
    let dir = dir.display().to_string();
    let mut args = vec![
        "execute",
        "--policy",
        policy,
        "--export-dir",
        &dir,
        "--lock-timeout-ms",
        "5000",
    ];
    if let Some(id) = registry {
        args.extend(["--registry-id", id]);
    }
    if dry_run {
        args.push("--dry-run");
    }
    retention(db, &args)
}

/// Asserts a refusal: exit 3 and `{"outcome":"refused","reason":<reason>}`.
fn assert_refused(out: &Output, reason: &str) {
    let (code, receipt, stderr) = outcome(out);
    assert_eq!(code, Some(3), "{reason}: {receipt} {stderr}");
    assert_eq!(receipt["outcome"], "refused", "{receipt} {stderr}");
    assert_eq!(receipt["reason"], reason, "{receipt} {stderr}");
}

/// `(state, dropped_at IS NULL, to_regclass(leaf) IS NOT NULL)` of one registry row: a refused run leaves
/// `("ATTACHED", true, true)`.
fn registry_state(c: &mut Client, registry_id: &str) -> (String, bool, bool) {
    let row = c
        .query_one(
            "SELECT state, dropped_at IS NULL, to_regclass(leaf_name) IS NOT NULL \
               FROM control.partition_registry WHERE registry_id = $1::text::uuid",
            &[&registry_id],
        )
        .expect("registry row");
    (row.get(0), row.get(1), row.get(2))
}

const UNTOUCHED: (&str, bool, bool) = ("ATTACHED", true, true);

fn assert_untouched(c: &mut Client, registry_id: &str, what: &str) {
    let (state, undropped, exists) = registry_state(c, registry_id);
    assert_eq!(
        (state.as_str(), undropped, exists),
        UNTOUCHED,
        "{what}: the leaf stays attached and its registry row unchanged"
    );
}

/// A tenant, as text id.
fn tenant(c: &mut Client, name: &str) -> String {
    c.query_one(
        "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id::text",
        &[&name],
    )
    .expect("tenant")
    .get(0)
}

/// `n` stage_runs rows of `tenant` in the UTC month `months` from now, finished unless `running`.
fn stage_rows(c: &mut Client, tenant: &str, months: i32, n: i32, running: bool) {
    c.execute(
        &format!(
            "INSERT INTO ops.stage_runs (tenant_id, stage_name, started_at, finished_at) \
             SELECT $1::text::uuid, 'c36 ' || g || E'\\ttab\\nline\\\\slash', {m} + make_interval(days => g), \
                    CASE WHEN $3 THEN NULL ELSE {m} + make_interval(days => g, hours => 1) END \
               FROM generate_series(1, $2) g",
            m = throwaway::month_sql(months)
        ),
        &[&tenant, &n, &running],
    )
    .expect("stage_runs rows");
}

/// One ledger row of `tenant` (a retrieval-plane `embedding` call) in the UTC month `months` from now with `status`,
/// as text `model_call_id`.
fn ledger_row(c: &mut Client, tenant: &str, months: i32, status: &str) -> String {
    c.query_one(
        &format!(
            "INSERT INTO ops.model_call_ledger (tenant_id, provider, model, purpose, status, called_at) \
             VALUES ($1::text::uuid, 'c36', 'c36', 'embedding', $2, {} + interval '2 days') \
             RETURNING model_call_id::text",
            throwaway::month_sql(months)
        ),
        &[&tenant, &status],
    )
    .expect("ledger row")
    .get(0)
}

/// The real daemon proposer (`maintenance_repo::propose_partitions`) as role_maintenance on this throwaway.
fn propose(db: &Db) -> u64 {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        // dep: PostgreSQL(role_maintenance) — the daemon's checked pool on this throwaway
        let pool = MaintenanceDbPool::connect(&db.maintenance_dsn)
            .await
            .expect("maintenance pool");
        maintenance_repo::propose_partitions(&pool)
            .await
            .expect("propose")
            .proposed
    })
}

/// A STAGE_RUNS leaf two UTC months back with `rows` finished rows, a 1-month policy and the daemon's proposal:
/// `(policy_id, registry_id, leaf)`.
fn due_stage_leaf(db: &mut Db, rows: i32) -> (String, String, String) {
    let c = db.client();
    let t = tenant(c, "c36-retention");
    let (registry, leaf) = past_leaf(c, "STAGE_RUNS", "ops.stage_runs", -2);
    stage_rows(c, &t, -2, rows, false);
    let (policy, _) = effective_policy(c, "STAGE_RUNS", Some(1));
    assert_eq!(propose(db), 1, "the daemon proposes the one due leaf");
    (policy, registry, leaf)
}

/// Contract test (§78.2): both DB `table_key` CHECKs equal the Rust closed set — the registry all six keys, the
/// policies all but EVENTS / AUDIT_EVENTS (D-G) — and `control.partition_parent` resolves every key.
#[test]
fn partition_table_keys_match_the_rust_enum() {
    let Some(mut db) = head("partition_table_keys_match_the_rust_enum") else {
        return;
    };
    let c = db.client();
    let keys = |c: &mut Client, constraint: &str| -> Vec<String> {
        let def: String = c
            .query_one(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = $1",
                &[&constraint],
            )
            .expect("constraint")
            .get(0);
        let mut keys: Vec<String> = def
            .split('\'')
            .skip(1)
            .step_by(2)
            .map(str::to_owned)
            .collect();
        keys.sort();
        keys
    };
    let mut all: Vec<String> = PartitionTable::ALL
        .iter()
        .map(|t| t.key().to_owned())
        .collect();
    all.sort();
    assert_eq!(keys(c, "partition_registry_table_key"), all);
    let retainable: Vec<String> = all
        .iter()
        .filter(|k| !matches!(k.as_str(), "EVENTS" | "AUDIT_EVENTS"))
        .cloned()
        .collect();
    assert_eq!(keys(c, "retention_policies_table_key"), retainable);
    for table in PartitionTable::ALL {
        assert_eq!(PartitionTable::parse(table.key()), Some(table));
        c.query_one(
            "SELECT parent::text FROM control.partition_parent($1)",
            &[&table.key()],
        )
        .unwrap_or_else(|e| panic!("{}: {e:?}", table.key()));
    }
}

/// T-I1 (ADR-0063 D-I): `retention approve` refuses without the §77 fields and writes nothing; with them it writes the
/// next revision (`forever` = keep forever) and one `RETENTION_POLICY_APPROVED` row per approval; EVENTS is refused by
/// the table's CHECK (exit 3). Fault: drop `Admin::from` from the arm ⇒ the fieldless run writes revision 1 ⇒ red.
#[test]
fn approve_writes_the_next_revision_and_an_audit_row() {
    let Some(mut db) = head("approve_writes_the_next_revision_and_an_audit_row") else {
        return;
    };
    let dsn = with_db(&db.owner_dsn, &db.name);
    let effective: String = db
        .client()
        .query_one(
            "SELECT to_char((now() + interval '1 day') AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
            &[],
        )
        .expect("effective_at")
        .get(0);
    let approve = |months: &str, table: &str| -> Vec<String> {
        [
            "approve",
            "--table",
            table,
            "--months",
            months,
            "--effective-at",
            &effective,
            "--lock-timeout-ms",
            "5000",
        ]
        .map(str::to_owned)
        .to_vec()
    };
    let bare: Vec<String> = ["retention".to_owned()]
        .into_iter()
        .chain(approve("2", "STAGE_RUNS"))
        .collect();
    let bare: Vec<&str> = bare.iter().map(String::as_str).collect();
    let refused = throwaway::run(&bare, &[(MIGRATOR_DSN, dsn.clone())]);
    let (code, _, stderr) = outcome(&refused);
    assert_eq!(code, Some(2), "{stderr}");
    assert!(stderr.contains("--actor"), "{stderr}");
    let rows = |c: &mut Client| -> i64 {
        c.query_one("SELECT count(*) FROM control.retention_policies", &[])
            .expect("count")
            .get(0)
    };
    assert_eq!(rows(db.client()), 0, "a refused run writes nothing");

    for (months, revision, stored) in [("2", 1, Some(2)), ("forever", 2, None)] {
        let args = approve(months, "STAGE_RUNS");
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let (code, receipt, stderr) = outcome(&retention_as(&dsn, &args));
        assert_eq!(code, Some(0), "{receipt} {stderr}");
        assert_eq!(receipt["outcome"], "created", "{receipt}");
        assert_eq!(receipt["policy_revision"], revision, "{receipt}");
        let row = db
            .client()
            .query_one(
                "SELECT retention_months, approved_by FROM control.retention_policies \
                 WHERE policy_id = $1::text::uuid",
                &[&receipt["policy_id"].as_str().expect("policy_id")],
            )
            .expect("policy row");
        assert_eq!(row.get::<_, Option<i32>>(0), stored);
        assert_eq!(row.get::<_, String>(1), "ops@example.test");
    }
    let audited: i64 = db
        .client()
        .query_one(
            "SELECT count(*) FROM control.audit_events WHERE action = 'RETENTION_POLICY_APPROVED' \
               AND tenant_id = '00000000-0000-0000-0000-000000000000' AND actor_id = 'ops@example.test'",
            &[],
        )
        .expect("audit rows")
        .get(0);
    assert_eq!(audited, 2, "one §77 row per approval");

    let args = approve("1", "EVENTS");
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    assert_refused(
        &retention_as(&dsn, &args),
        "policy_check:retention_policies_table_key",
    );
    assert_eq!(rows(db.client()), 2, "EVENTS gets no policy row");
}

/// Drops a cluster role this test created, even on panic.
struct RoleGuard(String, String);

impl Drop for RoleGuard {
    fn drop(&mut self) {
        // dep: PostgreSQL(owner) — drop the throwaway login role this test created (cluster-global)
        if let Ok(mut admin) = Client::connect(&with_db(&self.1, "postgres"), postgres::NoTls) {
            let _ = admin.batch_execute(&format!("DROP ROLE IF EXISTS {}", self.0));
        }
    }
}

/// T-H1 (ADR-0063 D-H, card fault): `retention execute` with the maintenance DSN is refused by the migrator connect
/// check (exit 2, the RoleMismatch line), and so is a CREATEROLE principal that is not a superuser (the SUPERUSER
/// line); the leaf stays attached. Fault: open the DSN unchecked ⇒ the maintenance run reaches
/// `partition_drop_check` (exit 3 `executor_not_superuser`) ⇒ the line assertion reds.
#[test]
fn retention_execute_with_the_maintenance_dsn_is_refused() {
    let Some(mut db) = head("retention_execute_with_the_maintenance_dsn_is_refused") else {
        return;
    };
    let dir = export_dir("t_h1");
    let (policy, registry, _) = due_stage_leaf(&mut db, 2);
    let args = [
        "execute",
        "--policy",
        &policy,
        "--registry-id",
        &registry,
        "--export-dir",
        &dir.display().to_string(),
        "--lock-timeout-ms",
        "5000",
    ];
    let args: Vec<&str> = args.iter().map(|s| s.as_ref()).collect();
    let (code, receipt, stderr) = outcome(&retention_as(&db.maintenance_dsn, &args));
    assert_eq!(code, Some(2), "{receipt} {stderr}");
    assert!(
        stderr.contains("§6.2.3 role mismatch: expected current_user = \"a superuser or CREATEROLE principal outside the §6.2.0 role set\", got \"role_maintenance\""),
        "{stderr}"
    );

    let role = format!("c36_nonsuper_{}", std::process::id());
    let password = format!("c36-{}", uuid::Uuid::new_v4().simple());
    let _guard = RoleGuard(role.clone(), db.owner_dsn.clone());
    db.sql(&format!(
        "CREATE ROLE {role} LOGIN CREATEROLE NOSUPERUSER PASSWORD '{password}'"
    ));
    let owner = with_db(&db.owner_dsn, &db.name);
    let (scheme, rest) = owner.split_once("://").expect("dsn scheme");
    let host = rest.rsplit_once('@').map_or(rest, |(_, h)| h);
    let nonsuper = format!("{scheme}://{role}:{password}@{host}");
    let (code, receipt, stderr) = outcome(&retention_as(&nonsuper, &args));
    assert_eq!(code, Some(2), "{receipt} {stderr}");
    assert!(
        stderr.contains("expected current_user = \"a SUPERUSER principal (ADR-0063 D-H)\""),
        "{stderr}"
    );
    assert!(!stderr.contains(&password), "the password is never printed");
    assert_untouched(db.client(), &registry, "a refused principal");
    assert!(exports(&dir).is_empty(), "nothing exported");
}

/// T-H2 (ADR-0063 D-H): while another executor holds HXRETAIN, a run is refused `busy` (exit 3) and drops nothing;
/// once released, the same run drops the leaf. Fault: drop the `pg_try_advisory_xact_lock` from `begin_executor` ⇒
/// the first run drops ⇒ red.
#[test]
fn two_concurrent_executors_one_refuses_busy() {
    let Some(mut db) = head("two_concurrent_executors_one_refuses_busy") else {
        return;
    };
    let dir = export_dir("t_h2");
    let (policy, registry, _) = due_stage_leaf(&mut db, 2);
    assert_eq!(RETENTION_ADVISORY_LOCK, i64::from_be_bytes(*b"HXRETAIN"));
    // dep: PostgreSQL(owner) — a second session standing in for a concurrent executor
    let mut other = Client::connect(&with_db(&db.owner_dsn, &db.name), postgres::NoTls)
        .expect("second session");
    other
        .execute("SELECT pg_advisory_lock($1)", &[&RETENTION_ADVISORY_LOCK])
        .expect("hold HXRETAIN");
    assert_refused(&execute(&db, &policy, Some(&registry), &dir, false), "busy");
    assert_untouched(db.client(), &registry, "a busy refusal");
    other
        .execute("SELECT pg_advisory_unlock($1)", &[&RETENTION_ADVISORY_LOCK])
        .expect("release HXRETAIN");
    let (code, receipt, stderr) = outcome(&execute(&db, &policy, Some(&registry), &dir, false));
    assert_eq!(code, Some(0), "{receipt} {stderr}");
    assert_eq!(receipt["outcome"], "dropped", "{receipt}");
}

/// A MODEL_CALL_LEDGER leaf two UTC months back, a 1-month ledger policy and a forged proposal for it:
/// `(tenant, policy_id, registry_id)`.
fn due_ledger_leaf(db: &mut Db) -> (String, String, String) {
    let c = db.client();
    let t = tenant(c, "c36-ledger");
    let (registry, _) = past_leaf(c, "MODEL_CALL_LEDGER", "ops.model_call_ledger", -2);
    let (policy, revision) = effective_policy(c, "MODEL_CALL_LEDGER", Some(1));
    forge_proposal(c, &registry, revision, "clock_timestamp()");
    (t, policy, registry)
}

/// T-G2 (ADR-0063 D-G, card fault): a RESERVED ledger row of a tenant the executor never names holds its expired
/// month: refused `hold:unsettled_ledger_calls`, the leaf attached, the registry unchanged, nothing exported. Fault:
/// delete the RESERVED arm from `control.partition_drop_check` in 0224 ⇒ the leaf drops ⇒ red.
#[test]
fn a_reserved_ledger_row_holds_its_month() {
    let Some(mut db) = head("a_reserved_ledger_row_holds_its_month") else {
        return;
    };
    let dir = export_dir("t_g2");
    let (t, policy, registry) = due_ledger_leaf(&mut db);
    ledger_row(db.client(), &t, -2, "SUCCEEDED");
    ledger_row(db.client(), &t, -2, "RESERVED");
    assert_refused(
        &execute(&db, &policy, Some(&registry), &dir, false),
        "hold:unsettled_ledger_calls",
    );
    assert_untouched(db.client(), &registry, "a held month");
    assert!(exports(&dir).is_empty(), "a held month is never exported");
}

/// T-G6 (ADR-0063 D-G, review 2026-10-05): an unfinished `ops.stage_runs` row (`finished_at IS NULL`) holds its
/// expired STAGE_RUNS month: refused `hold:running_stage`, the leaf attached, the registry unchanged, nothing
/// exported; once the stage finishes, the same leaf drops. Fault: make the STAGE_RUNS arm of
/// `control.partition_drop_check` (0231) `IF false` ⇒ the first run drops the month ⇒ red.
#[test]
fn a_running_stage_holds_its_month() {
    let Some(mut db) = head("a_running_stage_holds_its_month") else {
        return;
    };
    let dir = export_dir("t_g6");
    let (policy, registry, _) = due_stage_leaf(&mut db, 2);
    let c = db.client();
    let t = tenant(c, "c36-running-stage");
    stage_rows(c, &t, -2, 1, true);
    assert_refused(
        &execute(&db, &policy, Some(&registry), &dir, false),
        "hold:running_stage",
    );
    assert_untouched(db.client(), &registry, "a month with a running stage");
    assert!(exports(&dir).is_empty(), "a held month is never exported");
    db.client()
        .execute(
            "UPDATE ops.stage_runs SET finished_at = started_at + interval '1 hour' \
              WHERE tenant_id = $1::text::uuid AND finished_at IS NULL",
            &[&t],
        )
        .expect("the stage finishes");
    let (code, receipt, stderr) = outcome(&execute(&db, &policy, Some(&registry), &dir, false));
    assert_eq!(code, Some(0), "{receipt} {stderr}");
    assert_eq!(receipt["outcome"], "dropped", "{receipt}");
    assert_eq!(receipt["row_count"], 3, "{receipt}");
}

/// T-G3 (ADR-0063 D-G, F11): the owner is blind under FORCE RLS, so `partition_drop_check` refuses every caller that
/// is not a superuser with its own code, never a "0 unsettled rows" pass. Fault: delete the superuser check ⇒ the
/// hold read raises 42501 from `row_security = off` instead ⇒ the exact-message assertion reds.
#[test]
fn hold_reads_refuse_a_non_superuser_caller() {
    let Some(mut db) = head("hold_reads_refuse_a_non_superuser_caller") else {
        return;
    };
    let (t, policy, registry) = due_ledger_leaf(&mut db);
    ledger_row(db.client(), &t, -2, "RESERVED");
    let mut tx = db.client().transaction().expect("begin");
    // dep: PostgreSQL(role_migration_owner) — role switch: the NOLOGIN owner, blind under FORCE RLS
    tx.batch_execute("SET LOCAL ROLE role_migration_owner")
        .expect("set role");
    let e = tx
        .query(
            "SELECT * FROM control.partition_drop_check($1::text::uuid, $2::text::uuid)",
            &[&policy, &registry],
        )
        .expect_err("the owner must be refused");
    let db_error = e.as_db_error().expect("server error");
    assert_eq!(
        (db_error.code().code(), db_error.message()),
        ("P0001", "retention refused: executor_not_superuser"),
        "{e:?}"
    );
}

/// `SELECT control.partition_drop(policy, registry, rows, 'x', '/dev/null')` straight in SQL as the superuser; its
/// refusal message.
fn drop_directly(c: &mut Client, policy: &str, registry: &str, rows: i64) -> String {
    let e = c
        .query(
            "SELECT control.partition_drop($1::text::uuid, $2::text::uuid, $3, 'x', '/dev/null')",
            &[&policy, &registry, &rows],
        )
        .expect_err("partition_drop must refuse");
    e.as_db_error()
        .map(|d| d.message().to_owned())
        .unwrap_or_else(|| panic!("not a server error: {e}"))
}

/// T-G4 (ADR-0063 D-G, D-J step 9, review finding 1): with every proposal forged through role_maintenance's own column
/// UPDATE, a direct `SELECT control.partition_drop(…)` still refuses (a) a held expired ledger leaf, (b) the
/// current-month leaf, (c) a table's newest leaf, (d) a row count that is not the leaf's; each leaf stays attached
/// and its registry row unchanged. Fault: delete the `partition_drop_check` call from `partition_drop` ⇒ (a) drops
/// (or answers another code) ⇒ red.
#[test]
fn partition_drop_called_directly_refuses_held_and_current_month_leaves() {
    let Some(mut db) = head("partition_drop_called_directly_refuses_held_and_current_month_leaves")
    else {
        return;
    };
    let (t, ledger_policy, held) = due_ledger_leaf(&mut db);
    let c = db.client();
    ledger_row(c, &t, -2, "RESERVED");
    let (current, revision): (String, i32) = {
        let row = c
            .query_one(
                &format!(
                    "SELECT g.registry_id::text, p.policy_revision FROM control.partition_registry g, \
                            control.retention_policies p \
                      WHERE g.table_key = 'MODEL_CALL_LEDGER' AND g.lower_bound = {} \
                        AND p.policy_id = $1::text::uuid",
                    throwaway::month_sql(0)
                ),
                &[&ledger_policy],
            )
            .expect("current ledger leaf");
        (row.get(0), row.get(1))
    };
    forge_proposal(c, &current, revision, "clock_timestamp()");

    // (c) STAGE_RUNS whose current and future leaves the owner removed: its expired leaf is the newest.
    let (newest, _) = past_leaf(c, "STAGE_RUNS", "ops.stage_runs", -2);
    c.batch_execute(
        "DO $$ DECLARE r record; BEGIN
           FOR r IN SELECT leaf_name FROM control.partition_registry
                     WHERE table_key = 'STAGE_RUNS' AND lower_bound >= date_trunc('month', now(), 'UTC') LOOP
             EXECUTE format('ALTER TABLE ops.stage_runs DETACH PARTITION %s', r.leaf_name);
             EXECUTE format('DROP TABLE %s', r.leaf_name);
           END LOOP;
           DELETE FROM control.partition_registry
            WHERE table_key = 'STAGE_RUNS' AND lower_bound >= date_trunc('month', now(), 'UTC');
         END $$",
    )
    .expect("remove the current and future stage_runs leaves");
    let (stage_policy, stage_revision) = effective_policy(c, "STAGE_RUNS", Some(1));
    forge_proposal(c, &newest, stage_revision, "clock_timestamp()");

    // (d) a hold-free expired maintenance_receipts leaf with one row, claimed as 0 rows.
    let (receipts, _) = past_leaf(c, "MAINTENANCE_RECEIPTS", "ops.maintenance_receipts", -2);
    c.execute(
        &format!(
            "INSERT INTO ops.maintenance_receipts (tenant_id, task, cutoff, row_limit, affected, ran_at) \
             VALUES ($1::text::uuid, 'rate_buckets', now(), 1, 1, {} + interval '1 day')",
            throwaway::month_sql(-2)
        ),
        &[&t],
    )
    .expect("receipt row");
    let (receipts_policy, receipts_revision) = effective_policy(c, "MAINTENANCE_RECEIPTS", Some(1));
    forge_proposal(c, &receipts, receipts_revision, "clock_timestamp()");

    for (policy, registry, rows, expected) in [
        (
            &ledger_policy,
            &held,
            0,
            "retention refused: hold:unsettled_ledger_calls",
        ),
        (&ledger_policy, &current, 0, "retention refused: not_due"),
        (&stage_policy, &newest, 0, "retention refused: newest_leaf"),
        (
            &receipts_policy,
            &receipts,
            0,
            "retention refused: export_mismatch",
        ),
    ] {
        assert_eq!(drop_directly(c, policy, registry, rows), expected);
        assert_untouched(c, registry, expected);
    }
}

/// T-G5 (ADR-0063 D-G, review finding 3): (a) a RESERVED budget reservation over a FAILED row and (b) a READY_B
/// contribution execution over a SUCCEEDED row each hold the expired ledger month; (c) once the reservation is settled
/// and the execution DONE, the same leaf drops. Fixture rows are planted under `session_replication_role = replica`
/// (their write-path guards are not under test). Fault: delete either arm from `partition_drop_check` ⇒ its case drops
/// the leaf ⇒ red.
#[test]
fn an_open_budget_reservation_or_contribution_holds_its_ledger_month() {
    let Some(mut db) = head("an_open_budget_reservation_or_contribution_holds_its_ledger_month")
    else {
        return;
    };
    let dir = export_dir("t_g5");
    let (t, policy, registry) = due_ledger_leaf(&mut db);
    let c = db.client();
    let failed = ledger_row(c, &t, -2, "FAILED");
    let succeeded = ledger_row(c, &t, -2, "SUCCEEDED");
    // replica-mode: throwaway database only (humaux_thread_c36_retention_<pid>_<n>, created by throwaway::db, dropped WITH (FORCE) by Db::drop)
    c.batch_execute("SET session_replication_role = replica")
        .expect("replica");
    c.execute(
        "INSERT INTO ops.retrieval_provider_budget_reservations \
           (tenant_id, model_call_id, provider_id, model_id, region, purpose, requested_tokens, ttl_micros, \
            reserved_at, expires_at, status) \
         VALUES ($1::text::uuid, $2::text::uuid, 'c36', 'c36', 'c36', 'embedding', 1, 1, now(), \
                 now() + interval '1 hour', 'RESERVED')",
        &[&t, &failed],
    )
    .expect("reservation");
    c.batch_execute("SET session_replication_role = origin")
        .expect("origin");
    assert_refused(
        &execute(&db, &policy, Some(&registry), &dir, false),
        "hold:open_budget_reservations",
    );
    let c = db.client();
    // replica-mode: throwaway database only (humaux_thread_c36_retention_<pid>_<n>, created by throwaway::db, dropped WITH (FORCE) by Db::drop)
    c.batch_execute("SET session_replication_role = replica")
        .expect("replica");
    c.execute(
        "UPDATE ops.retrieval_provider_budget_reservations SET status = 'RELEASED', settled_at = now() \
          WHERE model_call_id = $1::text::uuid",
        &[&failed],
    )
    .expect("settle the reservation");
    let z = "'\\x0000000000000000000000000000000000000000000000000000000000000000'::bytea";
    c.execute(
        &format!(
            "INSERT INTO private.contribution_executions \
               (execution_id, tenant_id, user_id, state, enqueue_idempotency_key, enqueue_fingerprint, \
                reasoning_domain_id, input_manifest_hash, source_count, policy_id, policy_version, policy_snapshot, \
                rights_basis, coverage_request_id, assessment_request_id, candidate_id, coverage_contract_version, \
                assessment_contract_version, coverage_prompt_contract_sha256, assessment_prompt_contract_sha256, \
                binding_id, binding_version, coverage_model_call_id, coverage_disclosure_id, coverage_intent_sha256, \
                source_backing_closure_version, source_backing_closure_sha256, backing_link_count) \
             VALUES (gen_random_uuid(), $1::text::uuid, gen_random_uuid(), 'READY_B', 'c36', {z}, gen_random_uuid(), \
                     {z}, 1, gen_random_uuid(), 1, '{{\"policy\": \"MANUAL\"}}', 'c36', gen_random_uuid(), \
                     gen_random_uuid(), gen_random_uuid(), 1, 1, {z}, {z}, gen_random_uuid(), 1, $2::text::uuid, \
                     gen_random_uuid(), {z}, 1, {z}, 0)"
        ),
        &[&t, &succeeded],
    )
    .expect("contribution execution");
    c.batch_execute("SET session_replication_role = origin")
        .expect("origin");
    assert_refused(
        &execute(&db, &policy, Some(&registry), &dir, false),
        "hold:open_contribution_executions",
    );
    assert_untouched(db.client(), &registry, "a held month");
    let c = db.client();
    // replica-mode: throwaway database only (humaux_thread_c36_retention_<pid>_<n>, created by throwaway::db, dropped WITH (FORCE) by Db::drop)
    c.batch_execute(
        "SET session_replication_role = replica;
         UPDATE private.contribution_executions SET state = 'DONE', source_backing_closure_version = NULL,
                source_backing_closure_sha256 = NULL, backing_link_count = NULL;
         SET session_replication_role = origin",
    )
    .expect("execution DONE");
    let (code, receipt, stderr) = outcome(&execute(&db, &policy, Some(&registry), &dir, false));
    assert_eq!(code, Some(0), "{receipt} {stderr}");
    assert_eq!(receipt["outcome"], "dropped", "{receipt}");
    assert_eq!(receipt["row_count"], 2, "{receipt}");
}

/// `(count, md5 of the sorted row texts)` of a relation.
fn fingerprint(c: &mut Client, relation: &str) -> (i64, String) {
    let row = c
        .query_one(
            &format!(
                "SELECT count(*), coalesce(md5(string_agg(t::text, '|' ORDER BY t::text)), '') FROM {relation} t"
            ),
            &[],
        )
        .expect("fingerprint");
    (row.get(0), row.get(1))
}

/// T-J1 (ADR-0063 D-J, card acceptance; review finding 5): an expired STAGE_RUNS month without holds, proposed by the
/// real daemon proposer, is exported, detached and dropped RESTRICT by `retention execute` (exit 0): exactly that leaf
/// is gone, the registry row is a complete receipt (the database's own row count, the export's sha256, the policy),
/// the export file holds every row and is mode 0600, and exactly one `PARTITION_DROPPED` §77 row committed with it.
/// Faults: create the export with `File::create` (umask mode) ⇒ the mode assertion reds; drop the
/// DETACH from `partition_drop_statements` ⇒ the statement list reds; set `row_security = off` for the client
/// transaction ⇒ the owner definer's audit insert raises 42501, the run rolls back ⇒ red.
#[test]
fn an_expired_month_without_holds_is_exported_detached_and_dropped_with_a_receipt() {
    let Some(mut db) =
        head("an_expired_month_without_holds_is_exported_detached_and_dropped_with_a_receipt")
    else {
        return;
    };
    let dir = export_dir("t_j1");
    let (policy, registry, leaf) = due_stage_leaf(&mut db, 3);
    let attached_before: i64 = db
        .client()
        .query_one(
            "SELECT count(*) FROM pg_inherits WHERE inhparent = 'ops.stage_runs'::regclass",
            &[],
        )
        .expect("leaves")
        .get(0);
    let (code, receipt, stderr) = outcome(&execute(&db, &policy, Some(&registry), &dir, false));
    println!("execute => {receipt}");
    assert_eq!(code, Some(0), "{receipt} {stderr}");
    assert_eq!(receipt["outcome"], "dropped", "{receipt}");
    assert_eq!(
        receipt["statements"],
        serde_json::json!([
            format!("ALTER TABLE ops.stage_runs DETACH PARTITION {leaf}"),
            format!("DROP TABLE {leaf} RESTRICT"),
        ]),
        "{receipt}"
    );
    assert_eq!(receipt["row_count"], 3, "{receipt}");
    let c = db.client();
    let attached_after: i64 = c
        .query_one(
            "SELECT count(*) FROM pg_inherits WHERE inhparent = 'ops.stage_runs'::regclass",
            &[],
        )
        .expect("leaves")
        .get(0);
    assert_eq!(
        attached_after,
        attached_before - 1,
        "exactly one leaf is gone"
    );
    let row = c
        .query_one(
            "SELECT state, rows_dropped, export_sha256, export_path, drop_policy_id::text, dropped_by, \
                    to_regclass(leaf_name) IS NULL \
               FROM control.partition_registry WHERE registry_id = $1::text::uuid",
            &[&registry],
        )
        .expect("receipt row");
    assert_eq!(row.get::<_, String>(0), "DROPPED");
    assert_eq!(row.get::<_, i64>(1), 3);
    let sha: String = row.get(2);
    let path: String = row.get(3);
    assert_eq!(row.get::<_, String>(4), policy);
    assert_eq!(row.get::<_, String>(5), "postgres");
    assert!(row.get::<_, bool>(6), "{leaf} no longer exists");
    assert_eq!(receipt["export_sha256"], sha.as_str(), "{receipt}");
    assert_eq!(
        exports(&dir),
        vec![PathBuf::from(&path)],
        "one export, no temp file"
    );
    // ADR-0063 L8: every tenant's rows of the leaf, so owner-only whatever the umask. Fault: `File::create` ⇒ 0644.
    let mode = std::os::unix::fs::PermissionsExt::mode(
        &std::fs::metadata(&path)
            .expect("export metadata")
            .permissions(),
    );
    assert_eq!(mode & 0o777, 0o600, "export mode {mode:o}");
    // dep: subprocess(shasum) — an independent sha256 of the export file
    let shasum = std::process::Command::new("shasum")
        .args(["-a", "256", &path])
        .output()
        .expect("shasum");
    let computed = String::from_utf8_lossy(&shasum.stdout);
    assert_eq!(computed.split_whitespace().next(), Some(sha.as_str()));
    let lines = std::fs::read(&path)
        .expect("export")
        .iter()
        .filter(|&&b| b == b'\n')
        .count();
    assert_eq!(lines, 3, "one line per dropped row");
    let audit = c
        .query_one(
            "SELECT count(*), min(tenant_id::text), min(actor_id) FROM control.audit_events \
              WHERE action = 'PARTITION_DROPPED' AND resource_id = $1",
            &[&registry],
        )
        .expect("audit row");
    assert_eq!(audit.get::<_, i64>(0), 1, "one §77 row in the same commit");
    assert_eq!(
        audit.get::<_, Option<String>>(1).as_deref(),
        Some("00000000-0000-0000-0000-000000000000")
    );
    assert_eq!(
        audit.get::<_, Option<String>>(2).as_deref(),
        Some("ops@example.test")
    );
}

/// T-J2 (ADR-0063 D-I, review finding 4): with two expired months M1 and M2, `execute --registry-id M1` drops M1; the
/// identical re-run answers `already_dropped` with the stored receipt (exit 0) and never takes M2; a real run without
/// `--registry-id` is refused `registry_id_required` (exit 2). M2 stays attached throughout. Fault: fall back to the
/// oldest due leaf when the named row is DROPPED ⇒ the re-run drops M2 ⇒ red.
#[test]
fn a_rerun_with_the_same_registry_id_is_a_noop_and_never_takes_the_next_month() {
    let Some(mut db) =
        head("a_rerun_with_the_same_registry_id_is_a_noop_and_never_takes_the_next_month")
    else {
        return;
    };
    let dir = export_dir("t_j2");
    let c = db.client();
    let t = tenant(c, "c36-rerun");
    let (m1, _) = past_leaf(c, "STAGE_RUNS", "ops.stage_runs", -3);
    let (m2, _) = past_leaf(c, "STAGE_RUNS", "ops.stage_runs", -2);
    stage_rows(c, &t, -3, 2, false);
    stage_rows(c, &t, -2, 1, false);
    let (policy, _) = effective_policy(c, "STAGE_RUNS", Some(1));
    assert_eq!(propose(&db), 2, "both expired months are proposed");
    let (code, first, stderr) = outcome(&execute(&db, &policy, Some(&m1), &dir, false));
    assert_eq!(code, Some(0), "{first} {stderr}");
    assert_eq!(first["outcome"], "dropped", "{first}");
    let (code, again, stderr) = outcome(&execute(&db, &policy, Some(&m1), &dir, false));
    assert_eq!(code, Some(0), "{again} {stderr}");
    assert_eq!(again["outcome"], "already_dropped", "{again}");
    assert_eq!(again["registry_id"], m1.as_str(), "{again}");
    assert_eq!(again["row_count"], 2, "the stored rows_dropped: {again}");
    assert_eq!(again["export_sha256"], first["export_sha256"], "{again}");
    assert_untouched(db.client(), &m2, "the next month after a re-run");
    let (code, _, stderr) = outcome(&execute(&db, &policy, None, &dir, false));
    assert_eq!(code, Some(2), "{stderr}");
    assert!(stderr.contains("registry_id_required"), "{stderr}");
    assert_untouched(db.client(), &m2, "the next month after an id-less run");
}

/// T-J3 (ADR-0063 D-J step 8): the export restores, through `COPY … FROM STDIN` into `(LIKE parent)`, to exactly the
/// dropped rows (count and md5 of the sorted rows, tabs, newlines and backslashes included). Fault: COPY a filtered
/// SELECT ⇒ the line count differs from the leaf's count, the run refuses `export_mismatch` ⇒ red.
#[test]
fn the_export_restores_to_the_dropped_rows() {
    let Some(mut db) = head("the_export_restores_to_the_dropped_rows") else {
        return;
    };
    let dir = export_dir("t_j3");
    let (policy, registry, leaf) = due_stage_leaf(&mut db, 5);
    let before = fingerprint(db.client(), &leaf);
    let (code, receipt, stderr) = outcome(&execute(&db, &policy, Some(&registry), &dir, false));
    assert_eq!(code, Some(0), "{receipt} {stderr}");
    let path = receipt["export_path"]
        .as_str()
        .expect("export_path")
        .to_owned();
    let c = db.client();
    c.batch_execute("CREATE TABLE c36_restore (LIKE ops.stage_runs)")
        .expect("restore table");
    let mut writer = c.copy_in("COPY c36_restore FROM STDIN").expect("copy in");
    std::io::Write::write_all(&mut writer, &std::fs::read(&path).expect("export")).expect("write");
    writer.finish().expect("finish copy");
    assert_eq!(fingerprint(c, "c36_restore"), before);
    assert_eq!(before.0, 5);
}

/// T-J4 (ADR-0063 D-J step 7): `--dry-run --registry-id` prints the statements `control.partition_drop_statements`
/// builds and changes nothing (leaf, registry, export dir); the real run then executes exactly those statements; the
/// id-less dry run lists the due leaf with verdict `ok` and its row count and leaves out the current and future
/// months. Fault: print a Rust-built statement list ⇒ it differs from what the definer executes ⇒ red.
#[test]
fn dry_run_prints_the_exact_statements_and_changes_nothing() {
    let Some(mut db) = head("dry_run_prints_the_exact_statements_and_changes_nothing") else {
        return;
    };
    let dir = export_dir("t_j4");
    let (policy, registry, leaf) = due_stage_leaf(&mut db, 2);
    let (code, listed, stderr) = outcome(&execute(&db, &policy, None, &dir, true));
    assert_eq!(code, Some(0), "{listed} {stderr}");
    assert_eq!(listed["outcome"], "listed", "{listed}");
    let due = listed["due"].as_array().expect("due array");
    assert_eq!(due.len(), 1, "only the expired month is due: {listed}");
    assert_eq!(due[0]["registry_id"], registry.as_str(), "{listed}");
    assert_eq!(due[0]["leaf"], leaf.as_str(), "{listed}");
    assert_eq!(due[0]["verdict"], "ok", "{listed}");
    assert_eq!(due[0]["row_count"], 2, "{listed}");

    let (code, dry, stderr) = outcome(&execute(&db, &policy, Some(&registry), &dir, true));
    assert_eq!(code, Some(0), "{dry} {stderr}");
    assert_eq!(dry["outcome"], "dry_run", "{dry}");
    assert_untouched(db.client(), &registry, "a dry run");
    assert!(exports(&dir).is_empty(), "a dry run writes no export");
    let audited: i64 = db
        .client()
        .query_one(
            "SELECT count(*) FROM control.audit_events WHERE action = 'PARTITION_DROPPED'",
            &[],
        )
        .expect("audit")
        .get(0);
    assert_eq!(audited, 0, "a dry run writes no audit row");

    let (code, real, stderr) = outcome(&execute(&db, &policy, Some(&registry), &dir, false));
    assert_eq!(code, Some(0), "{real} {stderr}");
    assert_eq!(real["statements"], dry["statements"], "printed = executed");
    assert_eq!(real["export_path"], dry["export_path"], "{real} {dry}");
}

/// T-J5 (ADR-0063 D-J steps 2 and 4): refused (a) `not_proposed` when the proposal names an older revision,
/// (b) `registry_catalog_mismatch <leaf>` when the registry bounds no longer equal the catalog's (0231; it replaced
/// `stale`), (c) `wrong_table` for another table_key's policy; nothing is dropped. Fault: drop any of these checks from `partition_drop_check` ⇒ that case drops ⇒ red.
#[test]
fn a_stale_or_unproposed_registry_row_is_refused() {
    let Some(mut db) = head("a_stale_or_unproposed_registry_row_is_refused") else {
        return;
    };
    let dir = export_dir("t_j5");
    let (first, registry, leaf) = due_stage_leaf(&mut db, 1);
    let c = db.client();
    let (latest, _) = effective_policy(c, "STAGE_RUNS", Some(1));
    assert_ne!(first, latest);
    assert_refused(
        &execute(&db, &latest, Some(&registry), &dir, false),
        "not_proposed",
    );
    assert_untouched(db.client(), &registry, "not proposed");

    let c = db.client();
    forge_proposal(c, &registry, 2, "clock_timestamp()");
    c.execute(
        "UPDATE control.partition_registry SET upper_bound = upper_bound - interval '1 day' \
          WHERE registry_id = $1::text::uuid",
        &[&registry],
    )
    .expect("edit the registry bound");
    assert_refused(
        &execute(&db, &latest, Some(&registry), &dir, false),
        &format!("registry_catalog_mismatch {leaf}"),
    );
    let c = db.client();
    c.execute(
        "UPDATE control.partition_registry SET upper_bound = upper_bound + interval '1 day' \
          WHERE registry_id = $1::text::uuid",
        &[&registry],
    )
    .expect("restore the registry bound");
    let (ledger_policy, _) = effective_policy(c, "MODEL_CALL_LEDGER", Some(1));
    assert_refused(
        &execute(&db, &ledger_policy, Some(&registry), &dir, false),
        "wrong_table",
    );
    assert_untouched(db.client(), &registry, "every refusal");
    assert!(exports(&dir).is_empty(), "nothing exported");
}

/// The last attached upper bound of `table_key` minus the UTC month start, in months.
fn horizon(c: &mut Client, table_key: &str) -> i64 {
    c.query_one(
        "SELECT ((extract(year FROM max(upper_bound) AT TIME ZONE 'UTC') * 12 \
                  + extract(month FROM max(upper_bound) AT TIME ZONE 'UTC')) \
                 - (extract(year FROM now() AT TIME ZONE 'UTC') * 12 + extract(month FROM now() AT TIME ZONE 'UTC'))) \
                ::bigint FROM control.partition_registry WHERE table_key = $1 AND state = 'ATTACHED'",
        &[&table_key],
    )
    .expect("horizon")
    .get(0)
}

/// T-F1 (ADR-0063 D-E, D-F): with the newest STAGE_RUNS leaf removed by the owner, `create-partitions --months-ahead
/// 3` creates exactly that month again (horizon back to the current month + 4) with one `PARTITIONS_CREATED` row; a
/// second run creates nothing and writes no audit row; the creator itself answers `exists` for a present month and
/// refuses a gap. Fault: remove the gap refusal or the `exists` answer from `partition_create_month` ⇒ red.
#[test]
fn create_partitions_is_idempotent_and_closes_the_horizon() {
    let Some(mut db) = head("create_partitions_is_idempotent_and_closes_the_horizon") else {
        return;
    };
    let c = db.client();
    c.batch_execute(
        "DO $$ DECLARE v text; BEGIN
           SELECT leaf_name INTO v FROM control.partition_registry
            WHERE table_key = 'STAGE_RUNS' ORDER BY upper_bound DESC LIMIT 1;
           EXECUTE format('ALTER TABLE ops.stage_runs DETACH PARTITION %s', v);
           EXECUTE format('DROP TABLE %s', v);
           DELETE FROM control.partition_registry WHERE leaf_name = v;
         END $$",
    )
    .expect("remove the newest stage_runs leaf");
    assert_eq!(horizon(c, "STAGE_RUNS"), 3);
    let args = [
        "create-partitions",
        "--months-ahead",
        "3",
        "--lock-timeout-ms",
        "5000",
    ];
    let (code, receipt, stderr) = outcome(&retention(&db, &args));
    assert_eq!(code, Some(0), "{receipt} {stderr}");
    assert_eq!(receipt["outcome"], "created", "{receipt}");
    let created = receipt["created"].as_array().expect("created");
    assert_eq!(created.len(), 1, "{receipt}");
    assert_eq!(created[0]["table_key"], "STAGE_RUNS", "{receipt}");
    let c = db.client();
    for table in PartitionTable::ALL {
        assert_eq!(horizon(c, table.key()), 4, "{}", table.key());
    }
    let (code, again, stderr) = outcome(&retention(&db, &args));
    assert_eq!(code, Some(0), "{again} {stderr}");
    assert_eq!(again["outcome"], "existing", "{again}");
    assert_eq!(again["created"], serde_json::json!([]), "{again}");
    let c = db.client();
    let audited: i64 = c
        .query_one(
            "SELECT count(*) FROM control.audit_events WHERE action = 'PARTITIONS_CREATED'",
            &[],
        )
        .expect("audit")
        .get(0);
    assert_eq!(audited, 1, "only the creating run is audited");
    let present: String = c
        .query_one(
            &format!(
                "SELECT control.partition_create_month('STAGE_RUNS', {})",
                throwaway::month_sql(3)
            ),
            &[],
        )
        .expect("present month")
        .get(0);
    assert_eq!(present, "exists");
    let gap = c
        .query_one(
            &format!(
                "SELECT control.partition_create_month('STAGE_RUNS', {})",
                throwaway::month_sql(5)
            ),
            &[],
        )
        .expect_err("a gap is refused");
    assert!(
        gap.as_db_error()
            .is_some_and(|d| d.message().contains("no gap, no overlap")),
        "{gap:?}"
    );
}

/// `partition_horizon_months{STAGE_RUNS}` exactly as the daemon computes it: one real `propose_partitions` run as
/// role_maintenance on this throwaway (the gauge's only producer, ADR-0063 D-K).
fn daemon_horizon(db: &Db) -> i64 {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        // dep: PostgreSQL(role_maintenance) — the daemon's checked pool on this throwaway
        let pool = MaintenanceDbPool::connect(&db.maintenance_dsn)
            .await
            .expect("maintenance pool");
        let run = maintenance_repo::propose_partitions(&pool)
            .await
            .expect("propose");
        let slot = PartitionTable::ALL
            .iter()
            .position(|t| t.key() == "STAGE_RUNS")
            .expect("STAGE_RUNS slot");
        run.horizon_months[slot]
    })
}

/// T-K4 (ADR-0063 D-F, D-K; Baseline §42 "partition horizon short" / "exhausted" red records): every missed monthly
/// step lowers the daemon's own `partition_horizon_months{STAGE_RUNS}` by one — 3 at head, then 2, 1, 0 as the owner
/// removes the newest leaf each time — and a table with no attached leaf at all reads -1. The values come from the
/// real `propose_partitions` SQL, never from a second formula. Fault: wrap the horizon expression of
/// `propose_partitions` in `GREATEST(…, 1)` ⇒ 0 reads 1 and -1 reads 1 ⇒ red.
#[test]
fn the_horizon_gauge_counts_each_missed_month_down_to_minus_one() {
    let Some(mut db) = head("the_horizon_gauge_counts_each_missed_month_down_to_minus_one") else {
        return;
    };
    let remove_newest = "DO $$ DECLARE v text; BEGIN
           SELECT leaf_name INTO v FROM control.partition_registry
            WHERE table_key = 'STAGE_RUNS' AND state = 'ATTACHED' ORDER BY upper_bound DESC LIMIT 1;
           EXECUTE format('ALTER TABLE ops.stage_runs DETACH PARTITION %s', v);
           EXECUTE format('DROP TABLE %s', v);
           DELETE FROM control.partition_registry WHERE leaf_name = v;
         END $$";
    let mut seen = vec![daemon_horizon(&db)];
    for _ in 0..3 {
        db.client()
            .batch_execute(remove_newest)
            .expect("remove the newest stage_runs leaf");
        seen.push(daemon_horizon(&db));
    }
    db.client()
        .batch_execute(
            "DO $$ DECLARE r record; BEGIN
               FOR r IN SELECT leaf_name FROM control.partition_registry
                         WHERE table_key = 'STAGE_RUNS' AND state = 'ATTACHED' LOOP
                 EXECUTE format('ALTER TABLE ops.stage_runs DETACH PARTITION %s', r.leaf_name);
                 EXECUTE format('DROP TABLE %s', r.leaf_name);
                 DELETE FROM control.partition_registry WHERE leaf_name = r.leaf_name;
               END LOOP;
             END $$",
        )
        .expect("remove every stage_runs leaf");
    seen.push(daemon_horizon(&db));
    assert_eq!(
        seen,
        vec![3, 2, 1, 0, -1],
        "daemon horizon per missed month"
    );
}

/// T-F2 (ADR-0063 D-F, review 2026-10-05): `--months-ahead` is the closed range 2..=24, refused (exit 2, naming the
/// flag) before any connection: 1 and 25 are usage errors although the migrator DSN is not even a DSN, while 2 and 24
/// pass the check and fail only on the connect (exit 1, infrastructure). Fault: drop the upper bound ⇒ 25 reaches the
/// connect ⇒ exit 1 ⇒ red.
#[test]
fn months_ahead_is_a_closed_range_checked_before_connecting() {
    for (months, expected) in [(1, 2), (2, 1), (24, 1), (25, 2)] {
        let n = months.to_string();
        let out = retention_as(
            // Unparseable: the connect fails at once (a closed port waits out the pool's acquire timeout).
            "not-a-postgres-dsn",
            &[
                "create-partitions",
                "--months-ahead",
                &n,
                "--lock-timeout-ms",
                "5000",
            ],
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(expected), "{months}: {stderr}");
        if expected == 2 {
            assert!(
                stderr.contains("--months-ahead must be in 2..=24"),
                "{months}: {stderr}"
            );
        }
    }
}
