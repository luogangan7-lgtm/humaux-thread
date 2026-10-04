//! `adapters::tests::health_snapshot` — migration 0210's two aggregate definers read through `adapters::health`
//!   with real `role_maintenance` / `role_admin` / `role_gateway` logins, plus `RuntimeDbPool::ping` (ADR-0061 D-D,
//!   D-F, D-J; card 34 T-D3..T-D9, T-J1, T-F1).
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-testkit, postgres, sqlx, tokio];
//!   services=[PostgreSQL(owner) r=[ops.schema_migrations] w=[control.tenants, ops.data_disclosures, ops.jobs,
//!   projection.stream_checkpoints, projection.stream_log] x=[ops.admin_probe_snapshot, ops.health_snapshot],
//!   PostgreSQL(role_admin), PostgreSQL(role_gateway), PostgreSQL(role_maintenance),
//!   PostgreSQL(role_migration_owner)];
//!   env=[CARGO_MANIFEST_DIR, HUMAUX_TEST_PG_DSN]; modules=[adapters::disclosure, adapters::health,
//!   adapters::postgres, domain::ticket_family, humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [each test owns its throwaway database humaux_thread_c34_hs_<pid>_<n>, migrated from the files and
//!   dropped by the fixture's Drop even on panic; every fault (a dropped policy, an owner-wide policy, a revoked
//!   CONNECT) is applied inside that database only, the policy faults inside a rolled-back transaction; missing
//!   env -> §79.2 skip_or_fail]
//! Spec: Baseline §41.2; §4.4; §6.2.2; §15.2; §79.2; ADR-0061 D-D; ADR-0061 D-F; ADR-0061 D-J
//!
//! Every positive read goes through the production function and a real role login. Each policy is then shown
//! load-bearing in the same test: dropped inside a never-committed superuser transaction, the same definer read
//! (`SET LOCAL ROLE` to the caller's role) loses the other tenant's rows.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use humaux_adapters::disclosure::DisclosureOutcome;
use humaux_adapters::health::{self, StreamFamilyLag};
use humaux_adapters::postgres::{AdminDbPool, MaintenanceDbPool, RuntimeDbPool};
use humaux_domain::ticket_family::TicketFamily;
use humaux_testkit::{ExternalDep, role_login_dsn, skip_or_fail};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

const OWNER_DSN: &str = "HUMAUX_TEST_PG_DSN";
/// 2026-01-01T00:00:00Z — the finalized watermark of T-D5, as a literal the seed SQL can restate.
const WATERMARK_UNIX: i64 = 1_767_225_600;
const WATERMARK_SQL: &str = "'2026-01-01 00:00:00+00'::timestamptz";

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// `postgres://user:pass@host:port/<db>?query` with the database replaced.
fn with_db(dsn: &str, db: &str) -> String {
    let (head, tail) = dsn.split_at(dsn.rfind('/').expect("dsn has a database path") + 1);
    let query = tail.find('?').map_or("", |i| &tail[i..]);
    format!("{head}{db}{query}")
}

struct Db {
    owner_dsn: String,
    name: String,
    client: Option<Client>,
}

impl Drop for Db {
    fn drop(&mut self) {
        drop(self.client.take());
        // dep: PostgreSQL(owner) — drop this test's throwaway database
        match Client::connect(&with_db(&self.owner_dsn, "postgres"), NoTls) {
            Ok(mut admin) => {
                if let Err(e) = admin.batch_execute(&format!(
                    "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                    self.name
                )) {
                    eprintln!("cleanup: drop database {} failed: {e}", self.name);
                }
            }
            Err(e) => eprintln!("cleanup: cannot reach postgres to drop {}: {e}", self.name),
        }
    }
}

/// Applies every migration body in order (the manifest checks are the `migrate` gate's job).
fn migrate(client: &mut Client) {
    // Role DDL is cluster-global (0201 ALTER ROLE, 0210 CREATE ROLE): one database at a time per process.
    static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _serial = ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("migrations dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS ops; CREATE TABLE IF NOT EXISTS ops.schema_migrations \
             (migration_id text PRIMARY KEY, checksum text NOT NULL, \
              applied_at timestamptz NOT NULL DEFAULT now())",
        )
        .expect("migration ledger bootstrap");
    for file in files {
        let sql = std::fs::read_to_string(&file).expect("migration body");
        client
            .batch_execute(&sql)
            .unwrap_or_else(|e| panic!("apply {}: {e:?}", file.display()));
    }
}

fn fixture(test: &str) -> Option<Db> {
    let Ok(owner_dsn) = std::env::var(OWNER_DSN) else {
        skip_or_fail(test, OWNER_DSN, ExternalDep::Postgres);
        return None;
    };
    let name = format!(
        "humaux_thread_c34_hs_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    );
    // dep: PostgreSQL(owner) — create this test's throwaway database
    let mut admin = match Client::connect(&with_db(&owner_dsn, "postgres"), NoTls) {
        Ok(c) => c,
        Err(e) => {
            skip_or_fail(
                test,
                &format!("reachable {OWNER_DSN}: {e}"),
                ExternalDep::Postgres,
            );
            return None;
        }
    };
    admin
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .expect("create throwaway db");
    let mut db = Db {
        owner_dsn: owner_dsn.clone(),
        name: name.clone(),
        client: None,
    };
    // dep: PostgreSQL(owner) — fixture connection to the throwaway database
    let mut client = Client::connect(&with_db(&owner_dsn, &name), NoTls).expect("connect test db");
    migrate(&mut client);
    db.client = Some(client);
    Some(db)
}

impl Db {
    fn sql(&mut self) -> &mut Client {
        self.client.as_mut().expect("client")
    }

    /// A real login DSN for `role` into this database (ADR-0059 D-D); a missing password is a §79.2 skip.
    fn login(&self, test: &str, role: &str) -> Option<String> {
        let admin = with_db(&self.owner_dsn, &self.name);
        match role_login_dsn(&admin, role, |n| std::env::var(n).ok()) {
            Ok(dsn) => Some(dsn),
            Err(missing) => {
                skip_or_fail(test, &missing, ExternalDep::Postgres);
                None
            }
        }
    }

    fn tenant(&mut self, name: &str) -> Uuid {
        self.sql()
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&name],
            )
            .expect("seed tenant")
            .get(0)
    }

    /// One `ops.jobs` row; `lease` = Some(true) an expired lease, Some(false) a live one.
    fn job(&mut self, tenant: Uuid, status: &str, created_ago_s: i32, lease: Option<bool>) {
        let lease_sql = match lease {
            None => "NULL, NULL",
            Some(true) => "'c34-worker', now() - interval '1 minute'",
            Some(false) => "'c34-worker', now() + interval '10 minutes'",
        };
        self.sql()
            .execute(
                &format!(
                    "INSERT INTO ops.jobs (tenant_id, job_type, status, idempotency_key, payload, \
                       lease_owner, lease_expires_at, created_at) \
                     VALUES ($1, 'C34_HEALTH_TEST', $2, gen_random_uuid()::text, '{{}}'::jsonb, {lease_sql}, \
                       now() - make_interval(secs => $3))"
                ),
                &[&tenant, &status, &f64::from(created_ago_s)],
            )
            .expect("seed job");
    }

    /// One `ops.data_disclosures` row; `finalized` is `(finalized_at SQL, outcome)`.
    fn disclosure(&mut self, tenant: Uuid, reserved_at: &str, finalized: Option<(&str, &str)>) {
        let (fin, outcome) = finalized.map_or(("NULL".to_owned(), "NULL".to_owned()), |(at, o)| {
            (at.to_owned(), format!("'{o}'"))
        });
        self.sql()
            .execute(
                &format!(
                    "INSERT INTO ops.data_disclosures (grant_id, tenant_id, processor_id, region, data_class, \
                       purpose, payload_sha256, payload_bytes, reserved_at, finalized_at, outcome) \
                     VALUES (gen_random_uuid(), $1, gen_random_uuid(), 'cn-beijing', 'PRIVATE', \
                       'RETRIEVAL_EMBEDDING', sha256('c34'::bytea), 3, {reserved_at}, {fin}, {outcome})"
                ),
                &[&tenant],
            )
            .expect("seed disclosure");
    }

    fn checkpoint(&mut self, tenant: Uuid, issued: i64, projected: i64) {
        let f = TicketFamily::PrivateMemory;
        self.sql()
            .execute(
                "INSERT INTO projection.stream_checkpoints (tenant_id, scope_kind, scope_id, domain, \
                   projection_kind, projection_version, issued_highwater, projection_highwater) \
                 VALUES ($1, 'TENANT', $1, $2, $3, $4, $5, $6)",
                &[
                    &tenant,
                    &f.domain(),
                    &f.projection_kind(),
                    &f.projection_version(),
                    &issued,
                    &projected,
                ],
            )
            .expect("seed checkpoint");
    }

    fn ticket(&mut self, tenant: Uuid, seq: i64, state: &str) {
        let f = TicketFamily::PrivateMemory;
        let settled = matches!(state, "DONE" | "FAILED");
        self.sql()
            .execute(
                &format!(
                    "INSERT INTO projection.stream_log (tenant_id, scope_kind, scope_id, domain, projection_kind, \
                       projection_version, stream_seq, commit_seq, state, settled_at) \
                     VALUES ($1, 'TENANT', $1, $2, $3, $4, $5, $5, $6, {})",
                    if settled { "now()" } else { "NULL" }
                ),
                &[
                    &tenant,
                    &f.domain(),
                    &f.projection_kind(),
                    &f.projection_version(),
                    &seq,
                    &state,
                ],
            )
            .expect("seed ticket");
    }

    /// The same definer read as `role` inside a never-committed transaction that first applies `fault`.
    fn read_under_fault(&mut self, role: &str, fault: &str, select: &str) -> postgres::Row {
        let mut txn = self.sql().transaction().expect("fault transaction");
        txn.batch_execute(fault).expect("apply fault");
        // dep: PostgreSQL(owner) — SET LOCAL ROLE to the caller's role, inside the rolled-back transaction
        txn.batch_execute(&format!("SET LOCAL ROLE {role}"))
            .expect("set role");
        txn.query_one(select, &[]).expect("read under fault")
        // `txn` drops here: ROLLBACK, the fault never leaves this transaction.
    }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("tokio runtime")
}

fn maintenance_read(db: &Db, test: &str, since: OffsetDateTime) -> Option<health::HealthSample> {
    let dsn = db.login(test, "role_maintenance")?;
    let rt = rt();
    // dep: PostgreSQL(role_maintenance) — the production read path, a real login
    let pool = rt
        .block_on(MaintenanceDbPool::connect(&dsn))
        .expect("role_maintenance pool");
    Some(
        rt.block_on(health::read_health_snapshot(&pool, since))
            .expect("read_health_snapshot"),
    )
}

fn epoch() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(WATERMARK_UNIX).expect("watermark")
}

const HEALTH_SELECT: &str = "SELECT jobs_pending, jobs_processing, jobs_waiting_key, jobs_dead, reserved_le_10s, \
     reserved_gt_60s, coalesce(array_length(gap_counts, 1), 0) FROM ops.health_snapshot(now())";

/// T-D3: the four job counts and the oldest PENDING age span both tenants.
#[test]
fn job_counts_span_every_tenant() {
    const T: &str = "job_counts_span_every_tenant";
    let Some(mut db) = fixture(T) else { return };
    let (a, b) = (db.tenant("c34-a"), db.tenant("c34-b"));
    db.job(a, "PENDING", 30, None);
    db.job(b, "PENDING", 120, None);
    db.job(a, "PROCESSING", 5, Some(true));
    db.job(b, "DEAD", 5, None);
    db.job(a, "WAITING_KEY", 5, None);
    db.job(b, "DONE", 5, None);
    let Some(s) = maintenance_read(&db, T, epoch()) else {
        return;
    };
    assert_eq!(
        (
            s.jobs_pending,
            s.jobs_processing,
            s.jobs_waiting_key,
            s.jobs_dead
        ),
        (2, 1, 1, 1),
        "{s:?}"
    );
    assert!(
        (120.0..3600.0).contains(&s.oldest_pending_age_seconds),
        "oldest PENDING is tenant B's 120 s job: {s:?}"
    );

    let row = db.read_under_fault(
        "role_maintenance",
        "DROP POLICY jobs_health_reader_read ON ops.jobs",
        HEALTH_SELECT,
    );
    let jobs: [i64; 4] = [row.get(0), row.get(1), row.get(2), row.get(3)];
    assert_eq!(
        jobs, [0; 4],
        "without the reader policy FORCE RLS hides every job"
    );
}

/// T-D4: open reservations land in their §41.2 age bucket across tenants; a finalized row is not open.
#[test]
fn open_reservations_are_bucketed_by_age() {
    const T: &str = "open_reservations_are_bucketed_by_age";
    let Some(mut db) = fixture(T) else { return };
    let (a, b) = (db.tenant("c34-a"), db.tenant("c34-b"));
    db.disclosure(a, "now() - interval '90 seconds'", None);
    db.disclosure(b, "now() - interval '5 seconds'", None);
    db.disclosure(b, "now() - interval '30 seconds'", None);
    db.disclosure(
        a,
        "now() - interval '2 hours'",
        Some(("now() - interval '1 hour'", "SUCCESS")),
    );
    let Some(s) = maintenance_read(&db, T, epoch()) else {
        return;
    };
    assert_eq!(
        (s.reserved_le_10s, s.reserved_le_60s, s.reserved_gt_60s),
        (1, 1, 1),
        "{s:?}"
    );

    let row = db.read_under_fault(
        "role_maintenance",
        "DROP POLICY data_disclosures_health_reader_read ON ops.data_disclosures",
        HEALTH_SELECT,
    );
    let (le_10s, gt_60s): (i64, i64) = (row.get(4), row.get(5));
    assert_eq!(
        (le_10s, gt_60s),
        (0, 0),
        "without the reader policy no reservation is visible"
    );
}

/// T-D5: only rows finalized strictly after the watermark count; the next sample (since = as_of) counts none.
#[test]
fn finalized_counts_only_rows_after_the_watermark() {
    const T: &str = "finalized_counts_only_rows_after_the_watermark";
    let Some(mut db) = fixture(T) else { return };
    let (a, b) = (db.tenant("c34-a"), db.tenant("c34-b"));
    let at = |offset_s: u32| format!("{WATERMARK_SQL} + interval '{offset_s} seconds'");
    db.disclosure(a, WATERMARK_SQL, Some((WATERMARK_SQL, "SUCCESS")));
    db.disclosure(a, WATERMARK_SQL, Some((&at(1), "SUCCESS")));
    db.disclosure(b, WATERMARK_SQL, Some((&at(2), "DENIED")));
    let Some(s) = maintenance_read(&db, T, epoch()) else {
        return;
    };
    let mut got = s.finalized_since.clone();
    got.sort_by_key(|(o, _)| format!("{o:?}"));
    assert_eq!(
        got,
        vec![
            (DisclosureOutcome::Denied, 1),
            (DisclosureOutcome::Success, 1)
        ],
        "the row finalized exactly at the watermark was counted by the previous sample: {s:?}"
    );
    let Some(next) = maintenance_read(&db, T, s.as_of) else {
        return;
    };
    assert!(next.finalized_since.is_empty(), "{next:?}");
}

/// T-D6: projection lag = Σ (issued - projected) over every tenant's stream.
#[test]
fn projection_lag_sums_every_stream() {
    const T: &str = "projection_lag_sums_every_stream";
    let Some(mut db) = fixture(T) else { return };
    let (a, b) = (db.tenant("c34-a"), db.tenant("c34-b"));
    db.checkpoint(a, 10, 4);
    db.checkpoint(b, 3, 3);
    let Some(s) = maintenance_read(&db, T, epoch()) else {
        return;
    };
    assert_eq!(s.projection_lag_events, 6, "{s:?}");
}

/// T-D7: one FAILED and one LOST ticket (two tenants) are two gaps of the family, read through the view.
#[test]
fn processing_gaps_are_read_through_the_view() {
    const T: &str = "processing_gaps_are_read_through_the_view";
    let Some(mut db) = fixture(T) else { return };
    let (a, b) = (db.tenant("c34-a"), db.tenant("c34-b"));
    db.ticket(a, 1, "FAILED");
    db.ticket(b, 1, "LOST");
    db.ticket(a, 2, "ISSUED");
    db.ticket(b, 2, "DONE");
    let Some(s) = maintenance_read(&db, T, epoch()) else {
        return;
    };
    assert_eq!(
        s.processing_gaps,
        vec![(TicketFamily::PrivateMemory, 2)],
        "{s:?}"
    );

    let row = db.read_under_fault(
        "role_maintenance",
        "DROP POLICY stream_log_health_reader_read ON projection.stream_log",
        HEALTH_SELECT,
    );
    let families: i32 = row.get(6);
    assert_eq!(
        families, 0,
        "the view runs as its owner, yet only the reader qual admits its rows"
    );
}

/// T-D8: the EXECUTE matrix is exact, and role_gateway's call is refused by the server.
#[test]
fn only_the_named_role_executes_each_definer() {
    const T: &str = "only_the_named_role_executes_each_definer";
    let Some(mut db) = fixture(T) else { return };
    for role in [
        "role_gateway",
        "role_private_worker",
        "role_consolidation_worker",
        "role_public_worker",
        "role_retrieval_worker",
        "role_batch_issuer",
        "role_maintenance",
        "role_admin",
    ] {
        let row = db
            .sql()
            .query_one(
                "SELECT has_function_privilege($1, 'ops.health_snapshot(timestamptz)', 'EXECUTE'), \
                        has_function_privilege($1, 'ops.admin_probe_snapshot()', 'EXECUTE')",
                &[&role],
            )
            .expect("privilege probe");
        let got: (bool, bool) = (row.get(0), row.get(1));
        assert_eq!(
            got,
            (role == "role_maintenance", role == "role_admin"),
            "{role}"
        );
    }
    let Some(dsn) = db.login(T, "role_gateway") else {
        return;
    };
    // dep: PostgreSQL(role_gateway) — the refused definer calls
    let mut gateway = Client::connect(&dsn, NoTls).expect("role_gateway login");
    for call in [
        "SELECT * FROM ops.health_snapshot(now())",
        "SELECT * FROM ops.admin_probe_snapshot()",
    ] {
        let e = gateway.query(call, &[]).expect_err(call);
        let e = e
            .as_db_error()
            .map_or_else(|| e.to_string(), |d| d.message().to_owned());
        assert!(e.contains("permission denied for function"), "{call}: {e}");
    }
}

/// T-D9: 0210 adds no owner-wide read — an owner session under tenant A's GUC still sees A only.
#[test]
fn the_owner_is_not_widened_on_the_reader_tables() {
    const T: &str = "the_owner_is_not_widened_on_the_reader_tables";
    let Some(mut db) = fixture(T) else { return };
    let (a, b) = (db.tenant("c34-a"), db.tenant("c34-b"));
    db.disclosure(a, "now()", None);
    db.disclosure(b, "now()", None);
    db.disclosure(b, "now()", None);
    db.checkpoint(a, 1, 1);
    db.checkpoint(b, 1, 1);
    let count_as_owner = |db: &mut Db, fault: &str| -> (i64, i64) {
        let mut txn = db.sql().transaction().expect("owner transaction");
        txn.batch_execute(fault).expect("fault");
        // dep: PostgreSQL(role_migration_owner) — an owner session under tenant A's GUC, rolled back
        txn.batch_execute(&format!(
            "SET LOCAL ROLE role_migration_owner; SET LOCAL humaux.tenant_id = '{a}'"
        ))
        .expect("owner session under tenant A");
        let row = txn
            .query_one(
                "SELECT (SELECT count(*) FROM ops.data_disclosures), \
                        (SELECT count(*) FROM projection.stream_checkpoints)",
                &[],
            )
            .expect("owner counts");
        (row.get(0), row.get(1))
    };
    assert_eq!(count_as_owner(&mut db, "SELECT 1"), (1, 1));
    // The previous revision's owner-wide policy: the same read now sees tenant B (the measurement can go red).
    assert_eq!(
        count_as_owner(
            &mut db,
            "CREATE POLICY c34_owner_wide ON ops.data_disclosures FOR SELECT TO role_migration_owner \
             USING (true); CREATE POLICY c34_owner_wide ON projection.stream_checkpoints FOR SELECT \
             TO role_migration_owner USING (true)"
        ),
        (3, 2)
    );
}

/// T-J1: the admin aggregates span tenants: 1 lagging of 2 streams, 1 stuck and 1 in-lease of 3 jobs.
#[test]
fn admin_probe_aggregates_span_every_tenant() {
    const T: &str = "admin_probe_aggregates_span_every_tenant";
    let Some(mut db) = fixture(T) else { return };
    let (a, b) = (db.tenant("c34-a"), db.tenant("c34-b"));
    db.checkpoint(a, 10, 4);
    db.checkpoint(b, 3, 3);
    db.job(a, "PROCESSING", 5, Some(true));
    db.job(b, "PROCESSING", 5, Some(false));
    db.job(b, "PENDING", 5, None);
    let Some(dsn) = db.login(T, "role_admin") else {
        return;
    };
    let rt = rt();
    // dep: PostgreSQL(role_admin) — the production read path, a real login
    let pool = rt
        .block_on(AdminDbPool::connect(&dsn))
        .expect("role_admin pool");
    let s = rt
        .block_on(health::read_admin_probe_snapshot(&pool))
        .expect("read_admin_probe_snapshot");
    assert_eq!(
        s.streams,
        vec![StreamFamilyLag {
            family: TicketFamily::PrivateMemory,
            streams: 2,
            lagging: 1,
            lag_total: 6,
            lag_max: 6,
        }],
        "{s:?}"
    );
    assert_eq!(
        (s.jobs_total, s.jobs_stuck, s.jobs_in_lease),
        (3, 1, 1),
        "{s:?}"
    );
    assert_eq!(
        (
            s.outbox_total,
            s.outbox_undelivered,
            s.outbox_oldest_undelivered_age_seconds
        ),
        (0, 0, None)
    );
    // ADR-0061 review-fix 3 (F3): role_admin reads the deployed statement the probes hash into `scope_hash`.
    assert!(
        s.definition.contains("j.lease_expires_at < now()"),
        "{}",
        s.definition
    );

    let row = db.read_under_fault(
        "role_admin",
        "DROP POLICY stream_checkpoints_health_reader_read ON projection.stream_checkpoints",
        "SELECT coalesce(array_length(stream_counts, 1), 0) FROM ops.admin_probe_snapshot()",
    );
    let families: i32 = row.get(0);
    assert_eq!(
        families, 0,
        "without the reader policy no stream is visible"
    );
}

/// T-F1: `ping` succeeds, then names the error once the database refuses role_gateway.
#[test]
fn ping_names_the_error_when_postgres_refuses() {
    const T: &str = "ping_names_the_error_when_postgres_refuses";
    let Some(mut db) = fixture(T) else { return };
    let Some(dsn) = db.login(T, "role_gateway") else {
        return;
    };
    let rt = rt();
    // dep: PostgreSQL(role_gateway) — the pool whose ping is under test, a real login
    let pool = rt
        .block_on(RuntimeDbPool::connect(&dsn))
        .expect("role_gateway pool");
    rt.block_on(pool.ping()).expect("ping on a live database");
    let name = db.name.clone();
    db.sql()
        .batch_execute(&format!(
            "REVOKE CONNECT ON DATABASE {name} FROM PUBLIC; \
             SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
              WHERE datname = '{name}' AND usename = 'role_gateway'"
        ))
        .expect("refuse role_gateway");
    let errors: Vec<String> = (0..2)
        .map(|_| {
            rt.block_on(pool.ping())
                .expect_err("ping after the refusal")
                .to_string()
        })
        .collect();
    assert!(
        errors
            .iter()
            .any(|e| e.contains("permission denied for database")),
        "{errors:?}"
    );
}
