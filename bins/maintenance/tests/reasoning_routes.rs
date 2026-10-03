//! `maintenance::tests::reasoning_routes` — real-PostgreSQL tests of `humaux-maintenance reasoning ...`
//!   (ADR-0060 D-H, T25–T29).
//! Depends-on: crates=[humaux-testkit, postgres, serde_json, uuid]; services=[PostgreSQL(owner)
//!   r=[control.audit_events, control.private_reasoning_domains, control.reasoning_profiles,
//!   control.reasoning_route_bindings, control.reasoning_route_candidates, control.reasoning_route_policies,
//!   ops.reasoning_account_health_observations, ops.reasoning_provider_health_observations, ops.schema_migrations]
//!   x=[control.ensure_admission_tier, control.ensure_user, control.onboard_tenant,
//!   control.resolve_user_reasoning_admission], PostgreSQL(role_maintenance), subprocess(humaux-maintenance)];
//!   env=[CARGO_BIN_EXE_humaux-maintenance, CARGO_MANIFEST_DIR, HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_TEST_PG_DSN];
//!   modules=[humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [each test owns its throwaway database humaux_thread_c33b_rtest_<pid>_<n>, dropped by the fixture's
//!   Drop even on panic; missing env -> §79.2 skip_or_fail]
//! Spec: Baseline §11.2.2; §11.2.3; §11.2.5; §77; §79.2; ADR-0060 D-H; ADR-0060 E3
//!
//! Every test runs the binary (`CARGO_BIN_EXE_humaux-maintenance`) against its own throwaway
//! database, created from the migration files and dropped by the fixture's `Drop`, so no row of
//! the shared database is written (no job, no tenant to clean up).

use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};

use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use serde_json::Value;
use uuid::Uuid;

const OWNER_DSN: &str = "HUMAUX_TEST_PG_DSN";
const MAINTENANCE_DSN: &str = "HUMAUX_MAINTENANCE_PG_DSN";
const ADMIN: [&str; 8] = [
    "--actor",
    "c33b-test",
    "--reason",
    "card 33b route doors test",
    "--ticket",
    "T-33b",
    "--step-up-auth",
    "test-mfa",
];
/// Fixture route values (test-only; no deployment reads them).
const PROVIDER: &str = "vendor-test";
const ENDPOINT: &str = "https://api.vendor-test.example/v1/chat/completions";
const EGRESS: &str = "0190a000-0000-7000-8000-00000000c33b";

static NEXT: AtomicUsize = AtomicUsize::new(0);

fn with_db(dsn: &str, db: &str) -> String {
    let (head, tail) = dsn.split_at(dsn.rfind('/').expect("dsn has a database path") + 1);
    let query = tail.find('?').map_or("", |i| &tail[i..]);
    format!("{head}{db}{query}")
}

struct Fixture {
    owner_dsn: String,
    maintenance_dsn: String,
    db: String,
    client: Option<Client>,
    tenant: Uuid,
    owner: Uuid,
    domain: Uuid,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.client.take());
        // dep: PostgreSQL(owner) — drop this test's throwaway database
        if let Ok(mut admin) = Client::connect(&with_db(&self.owner_dsn, "postgres"), NoTls) {
            let _ =
                admin.batch_execute(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.db));
        }
    }
}

/// Applies every migration body in order (the manifest checks are `xtask migrate`'s gate).
fn migrate(client: &mut Client) {
    // Role DDL is cluster-global (0201 ALTER ROLE), so this process applies one database at a time.
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

/// A throwaway database with one onboarded tenant (through the 0186 doors as role_maintenance):
/// its owner user and default reasoning domain.
fn fixture(test: &str) -> Option<Fixture> {
    let (Ok(owner_dsn), Ok(maintenance_dsn)) =
        (std::env::var(OWNER_DSN), std::env::var(MAINTENANCE_DSN))
    else {
        skip_or_fail(
            test,
            "HUMAUX_TEST_PG_DSN / HUMAUX_MAINTENANCE_PG_DSN",
            ExternalDep::Postgres,
        );
        return None;
    };
    let db = format!(
        "humaux_thread_c33b_rtest_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    );
    // dep: PostgreSQL(owner) — create this test's throwaway database
    let mut admin = Client::connect(&with_db(&owner_dsn, "postgres"), NoTls).ok()?;
    admin
        .batch_execute(&format!("CREATE DATABASE {db}"))
        .expect("create throwaway db");
    drop(admin);
    let mut fixture = Fixture {
        maintenance_dsn: with_db(&maintenance_dsn, &db),
        owner_dsn: owner_dsn.clone(),
        db: db.clone(),
        client: None,
        tenant: Uuid::nil(),
        owner: Uuid::nil(),
        domain: Uuid::nil(),
    };
    // dep: PostgreSQL(owner) — fixture connection to the throwaway database
    let mut client = Client::connect(&with_db(&owner_dsn, &db), NoTls).expect("connect test db");
    migrate(&mut client);
    // dep: PostgreSQL(role_maintenance) — the 0186 onboarding doors as the only role that may call them
    client
        .batch_execute(
            "SET ROLE role_maintenance; \
             SELECT control.ensure_admission_tier('dashscope', NULL, NULL, NULL, 10, 10); \
             SELECT control.ensure_admission_tier('dashscope', 'cn-beijing', NULL, NULL, 10, 10);",
        )
        .expect("tiers");
    let mut txn = client.transaction().expect("txn");
    let owner: Uuid = txn
        .query_one(
            "SELECT user_id FROM control.ensure_user('owner@c33b.example', 'owner@c33b.example')",
            &[],
        )
        .expect("ensure_user")
        .get(0);
    let tenant: Uuid = txn
        .query_one(
            "SELECT tenant_id FROM control.onboard_tenant('c33b', $1, 'default', 'default', 10, \
             now() - interval '1 second', now() + interval '1 day', 'dashscope', 'cn-beijing', \
             10, 10, ARRAY['private_memory','PRIVATE_MEMORY','v1'])",
            &[&owner],
        )
        .expect("onboard_tenant")
        .get(0);
    txn.commit().expect("commit");
    client.batch_execute("RESET ROLE").expect("reset role");
    fixture.domain = client
        .query_one(
            "SELECT reasoning_domain_id FROM control.private_reasoning_domains WHERE tenant_id = $1",
            &[&tenant],
        )
        .expect("default domain")
        .get(0);
    fixture.tenant = tenant;
    fixture.owner = owner;
    fixture.client = Some(client);
    Some(fixture)
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// The one JSON receipt: always the last stdout line.
fn receipt(out: &Output) -> Value {
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    let last = text.lines().last().unwrap_or_default().to_owned();
    serde_json::from_str(&last).unwrap_or_else(|e| panic!("receipt JSON ({e}): {last}"))
}

fn uuid_of(v: &Value, key: &str) -> Uuid {
    v[key]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("{key} in {v}"))
}

impl Fixture {
    fn db(&mut self) -> &mut Client {
        self.client.as_mut().expect("client")
    }

    fn count(&mut self, sql: &str) -> i64 {
        self.db().query_one(sql, &[]).expect(sql).get(0)
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut all: Vec<&str> = args.to_vec();
        all.extend(ADMIN);
        // dep: subprocess(humaux-maintenance) — the binary under test
        Command::new(env!("CARGO_BIN_EXE_humaux-maintenance"))
            .args(&all)
            .env_clear()
            .env("HUMAUX_MAINTENANCE_PG_DSN", &self.maintenance_dsn)
            .output()
            .expect("run humaux-maintenance")
    }

    /// `reasoning register` of a profile owned by `owner` on model `model` with `caps`.
    fn register(&self, owner: Uuid, model: &str, caps: &str, extras: &str) -> Output {
        self.run(&[
            "reasoning",
            "register",
            "--tenant",
            &self.tenant.to_string(),
            "--owner-user",
            &owner.to_string(),
            "--provider-id",
            PROVIDER,
            "--provider-model-id",
            model,
            "--model-revision",
            "r1",
            "--capabilities",
            caps,
            "--request-extras",
            extras,
            "--account-ref",
            "c33b-account",
            "--endpoint-ref",
            ENDPOINT,
            "--region",
            "test-region",
            "--service-tier",
            "standard",
            "--egress-processor-id",
            EGRESS,
        ])
    }

    fn registered(&self, model: &str) -> (Uuid, i64) {
        let out = self.register(self.owner, model, "TEXT,STRUCTURED_OUTPUT", "{}");
        assert_eq!(out.status.code(), Some(0), "register: {}", stderr(&out));
        let r = receipt(&out);
        (
            uuid_of(&r, "profile_id"),
            r["profile_version"].as_i64().expect("version"),
        )
    }

    fn bind(&self, domain: Uuid, purpose: &str, (profile, version): (Uuid, i64)) -> Output {
        self.run(&[
            "reasoning",
            "bind",
            "--tenant",
            &self.tenant.to_string(),
            "--domain",
            &domain.to_string(),
            "--purpose",
            purpose,
            "--profile",
            &profile.to_string(),
            "--profile-version",
            &version.to_string(),
        ])
    }

    fn attest(&self, (profile, version): (Uuid, i64), secs: &str) -> Output {
        self.run(&[
            "reasoning",
            "attest-health",
            "--tenant",
            &self.tenant.to_string(),
            "--profile",
            &profile.to_string(),
            "--profile-version",
            &version.to_string(),
            "--valid-for-secs",
            secs,
        ])
    }

    /// `(profiles, policies, candidates, current bindings)` of the tenant.
    fn route_rows(&mut self) -> (i64, i64, i64, i64) {
        let t = self.tenant;
        (
            self.count(&format!(
                "SELECT count(*) FROM control.reasoning_profiles WHERE tenant_id = '{t}'"
            )),
            self.count(&format!(
                "SELECT count(*) FROM control.reasoning_route_policies WHERE tenant_id = '{t}'"
            )),
            self.count(&format!(
                "SELECT count(*) FROM control.reasoning_route_candidates WHERE tenant_id = '{t}'"
            )),
            self.count(&format!(
                "SELECT count(*) FROM control.reasoning_route_bindings \
                 WHERE tenant_id = '{t}' AND effective_to IS NULL"
            )),
        )
    }

    fn audits(&mut self, result: &str) -> i64 {
        self.count(&format!(
            "SELECT count(*) FROM control.audit_events WHERE tenant_id = '{}' \
             AND 'reasoning_route' = ANY(risk_tags) AND result = '{result}'",
            self.tenant
        ))
    }

    /// Rows the 0130 resolver admits for the current binding of `(domain, purpose)`.
    fn admitted(&mut self, purpose: &str) -> i64 {
        let (tenant, domain) = (self.tenant, self.domain);
        let client = self.db();
        let mut txn = client.transaction().expect("txn");
        txn.execute(
            "SELECT set_config('humaux.tenant_id', $1, true)",
            &[&tenant.to_string()],
        )
        .expect("guc");
        let n: i64 = txn
            .query_one(
                "SELECT count(*) FROM control.reasoning_route_bindings b, \
                 LATERAL control.resolve_user_reasoning_admission(b.binding_id, b.binding_version, \
                   b.reasoning_domain_id, b.purpose) \
                 WHERE b.tenant_id = $1 AND b.reasoning_domain_id = $2 AND b.purpose = $3 \
                   AND b.effective_to IS NULL",
                &[&tenant, &domain, &purpose],
            )
            .expect("resolver")
            .get(0);
        txn.rollback().expect("rollback");
        n
    }
}

/// T25 (ADR-0060 D-H 2): register + bind leave exactly one Profile, one PINNED Policy, one
/// Candidate and one current Binding for (domain, purpose); a re-run of both answers `existing`
/// and writes no route row; every successful run writes one §77 SUCCESS row.
/// Fault: bind without its `existing` check → a second policy → policy count 2.
#[test]
fn register_then_bind_projects_one_of_each() {
    let Some(mut f) = fixture("register_then_bind_projects_one_of_each") else {
        return;
    };
    let profile = f.registered("m1");
    let out = f.bind(f.domain, "PRIVATE_DISTILL_TEXT", profile);
    assert_eq!(out.status.code(), Some(0), "bind: {}", stderr(&out));
    assert_eq!(receipt(&out)["outcome"], "created");
    assert_eq!(f.route_rows(), (1, 1, 1, 1));

    let again = f.register(f.owner, "m1", "TEXT,STRUCTURED_OUTPUT", "{}");
    assert_eq!(again.status.code(), Some(0), "{}", stderr(&again));
    assert_eq!(receipt(&again)["outcome"], "existing");
    let rebind = f.bind(f.domain, "PRIVATE_DISTILL_TEXT", profile);
    assert_eq!(rebind.status.code(), Some(0), "{}", stderr(&rebind));
    assert_eq!(receipt(&rebind)["outcome"], "existing");
    assert_eq!(f.route_rows(), (1, 1, 1, 1), "re-runs wrote route rows");
    assert_eq!(f.audits("SUCCESS"), 4);
    assert_eq!(f.audits("DENIED"), 0);
}

/// T26 (ADR-0060 D-H 2, §11.2.3): binding a second Profile keeps the binding_id, closes v1's
/// interval and makes v2 current; v1's policy and candidate rows are untouched.
/// Fault: the door mints a new binding_id instead of a successor version → two binding ids.
#[test]
fn rebind_closes_old_interval_and_inserts_successor_version() {
    let Some(mut f) = fixture("rebind_closes_old_interval_and_inserts_successor_version") else {
        return;
    };
    let p1 = f.registered("m1");
    let first = receipt(&f.bind(f.domain, "PRIVATE_CONSOLIDATE", p1));
    let policy_v1 = uuid_of(&first, "route_policy_id");
    let snapshot = |f: &mut Fixture| -> String {
        f.db()
            .query_one(
                "SELECT (SELECT row_to_json(p)::text FROM control.reasoning_route_policies p \
                          WHERE route_policy_id = $1) || \
                        (SELECT row_to_json(c)::text FROM control.reasoning_route_candidates c \
                          WHERE route_policy_id = $1)",
                &[&policy_v1],
            )
            .expect("v1 policy + candidate")
            .get(0)
    };
    let before = snapshot(&mut f);

    let p2 = f.registered("m2");
    assert_ne!(p1.0, p2.0);
    let out = f.bind(f.domain, "PRIVATE_CONSOLIDATE", p2);
    assert_eq!(out.status.code(), Some(0), "rebind: {}", stderr(&out));
    let second = receipt(&out);
    assert_eq!(second["outcome"], "rebound");
    assert_eq!(
        uuid_of(&second, "binding_id"),
        uuid_of(&first, "binding_id")
    );
    assert_eq!(second["binding_version"], 2);
    assert_eq!(second["closed_binding_version"], 1);

    let domain = f.domain;
    let rows = f
        .db()
        .query(
            "SELECT binding_id, binding_version, effective_to IS NULL FROM \
             control.reasoning_route_bindings WHERE reasoning_domain_id = $1 \
             AND purpose = 'PRIVATE_CONSOLIDATE' ORDER BY binding_version",
            &[&domain],
        )
        .expect("bindings");
    let shape: Vec<(Uuid, i64, bool)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    let id = uuid_of(&first, "binding_id");
    assert_eq!(shape, vec![(id, 1, false), (id, 2, true)]);
    assert_eq!(snapshot(&mut f), before, "v1 policy/candidate changed");
}

/// T27 (ADR-0060 D-H 2): a profile owned by another user than the domain owner, and a purpose
/// outside {PRIVATE_DISTILL_TEXT, PRIVATE_CONSOLIDATE}, are refused: exit 3 with the reason, no
/// route row written, one DENIED §77 row each.
/// Fault: drop the purpose check → CONTRIBUTION_DEIDENTIFY is bound.
#[test]
fn bind_refuses_owner_mismatch_and_unbindable_purpose() {
    let Some(mut f) = fixture("bind_refuses_owner_mismatch_and_unbindable_purpose") else {
        return;
    };
    let member = f.run(&[
        "onboard",
        "user",
        "--tenant",
        &f.tenant.to_string(),
        "--email",
        "member@c33b.example",
        "--role",
        "MEMBER",
    ]);
    assert_eq!(
        member.status.code(),
        Some(0),
        "onboard user: {}",
        stderr(&member)
    );
    let other = uuid_of(&receipt(&member), "user_id");
    let out = f.register(other, "m1", "TEXT,STRUCTURED_OUTPUT", "{}");
    assert_eq!(out.status.code(), Some(0), "register: {}", stderr(&out));
    let r = receipt(&out);
    let foreign = (
        uuid_of(&r, "profile_id"),
        r["profile_version"].as_i64().expect("v"),
    );
    let own = f.registered("m2");
    let audits_before = f.audits("SUCCESS");

    let mismatch = f.bind(f.domain, "PRIVATE_DISTILL_TEXT", foreign);
    assert_eq!(mismatch.status.code(), Some(3), "{}", stderr(&mismatch));
    assert_eq!(receipt(&mismatch)["reason"], "owner_mismatch");
    let unbindable = f.bind(f.domain, "CONTRIBUTION_DEIDENTIFY", own);
    assert_eq!(unbindable.status.code(), Some(3), "{}", stderr(&unbindable));
    assert_eq!(receipt(&unbindable)["reason"], "purpose_not_bindable");

    assert_eq!(
        f.route_rows(),
        (2, 0, 0, 0),
        "a refused bind wrote route rows"
    );
    assert_eq!(f.audits("DENIED"), 2);
    assert_eq!(f.audits("SUCCESS"), audits_before);
}

/// T28 (ADR-0060 D-H 1, ruling E5; research amendment 1): on an existing catalog row
/// {TEXT, STRUCTURED_OUTPUT}, a register declaring TOOL_CALLS on the same identity is refused
/// `capabilities_exceed_catalog` (exit 3, no row); an adapter-owned key in `--request-extras` is
/// refused `request_extras_invalid` (exit 3, no row).
/// Faults: drop the subset check → the 0128 trigger's generic 23514 → exit 1; drop the CHECK's
/// key list → the extras profile is written → exit 0.
#[test]
fn register_refuses_capabilities_wider_than_catalog() {
    let Some(mut f) = fixture("register_refuses_capabilities_wider_than_catalog") else {
        return;
    };
    f.registered("m1");
    let before = f.route_rows();
    let wider = f.register(f.owner, "m1", "TEXT,STRUCTURED_OUTPUT,TOOL_CALLS", "{}");
    assert_eq!(wider.status.code(), Some(3), "{}", stderr(&wider));
    assert_eq!(receipt(&wider)["reason"], "capabilities_exceed_catalog");
    let extras = f.register(
        f.owner,
        "m1",
        "TEXT,STRUCTURED_OUTPUT",
        r#"{"model":"other"}"#,
    );
    assert_eq!(extras.status.code(), Some(3), "{}", stderr(&extras));
    assert_eq!(receipt(&extras)["reason"], "request_extras_invalid");
    assert_eq!(f.route_rows(), before, "a refused register wrote a profile");
    assert_eq!(f.audits("DENIED"), 2);
    // A vendor field that is not adapter-owned is profile data (research amendment 1).
    let vendor = f.register(
        f.owner,
        "m1",
        "TEXT,STRUCTURED_OUTPUT",
        r#"{"enable_thinking":false}"#,
    );
    assert_eq!(vendor.status.code(), Some(0), "{}", stderr(&vendor));
    assert_eq!(receipt(&vendor)["outcome"], "created");
}

/// T29 (ADR-0060 D-H 3, ruling E3 (a)/(c)): a bound profile without an attestation is not
/// admitted (status MISSING); after `attest-health` the resolver admits it, both observations are
/// OPERATOR_ATTEST, and status reports ADMISSIBLE with valid_until; `--valid-for-secs 0` is
/// refused; `profile-state --enabled false` stops admission without a restart.
/// Fault: attest writes only the provider observation → resolver 0 rows.
#[test]
fn attest_health_makes_a_bound_profile_admissible() {
    let Some(mut f) = fixture("attest_health_makes_a_bound_profile_admissible") else {
        return;
    };
    let profile = f.registered("m1");
    let out = f.bind(f.domain, "PRIVATE_DISTILL_TEXT", profile);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(f.admitted("PRIVATE_DISTILL_TEXT"), 0);
    let status = |f: &Fixture| {
        let out = f.run(&["reasoning", "status", "--tenant", &f.tenant.to_string()]);
        assert_eq!(out.status.code(), Some(0), "status: {}", stderr(&out));
        let r = receipt(&out);
        r["routes"]
            .as_array()
            .expect("routes")
            .iter()
            .find(|row| row["purpose"] == "PRIVATE_DISTILL_TEXT")
            .cloned()
            .expect("distill row")
    };
    assert_eq!(status(&f)["health"], "MISSING");

    let zero = f.attest(profile, "0");
    assert_eq!(zero.status.code(), Some(2), "{}", stderr(&zero));
    let attest = f.attest(profile, "600");
    assert_eq!(attest.status.code(), Some(0), "attest: {}", stderr(&attest));
    assert_eq!(f.admitted("PRIVATE_DISTILL_TEXT"), 1);
    let t = f.tenant;
    assert_eq!(
        f.count(&format!(
            "SELECT (SELECT count(*) FROM ops.reasoning_provider_health_observations \
                     WHERE tenant_id = '{t}' AND source_kind = 'OPERATOR_ATTEST') + \
                    (SELECT count(*) FROM ops.reasoning_account_health_observations \
                     WHERE tenant_id = '{t}' AND source_kind = 'OPERATOR_ATTEST')"
        )),
        2
    );
    let row = status(&f);
    assert_eq!(row["health"], "ADMISSIBLE", "{row}");
    assert!(row["valid_until"].is_string(), "{row}");
    assert_eq!(status(&f)["purpose"], "PRIVATE_DISTILL_TEXT");

    let off = f.run(&[
        "reasoning",
        "profile-state",
        "--tenant",
        &f.tenant.to_string(),
        "--profile",
        &profile.0.to_string(),
        "--profile-version",
        &profile.1.to_string(),
        "--enabled",
        "false",
    ]);
    assert_eq!(off.status.code(), Some(0), "{}", stderr(&off));
    assert_eq!(receipt(&off)["outcome"], "updated");
    assert_eq!(f.admitted("PRIVATE_DISTILL_TEXT"), 0);
    assert_eq!(status(&f)["profile_enabled"], false);
}
