//! `maintenance::tests::onboarding` — real-PostgreSQL / real-Qdrant tests of the `humaux-maintenance` binary
//!   (ADR-0053).
//! Depends-on: crates=[humaux-protocol, humaux-testkit, postgres, serde_json, uuid]; services=[PostgreSQL(owner)
//!   r=[control.api_keys, control.audit_events, control.credential_pepper_state, control.entitlement_snapshots, control.memberships,
//!   control.private_reasoning_domains, control.quota_windows, control.retrieval_provider_admission_limits,
//!   control.tenants, control.user_emails, control.users, control.workspace_memberships, ops.commit_seq_seq,
//!   ops.schema_migrations,
//!   projection.family_activations, projection.tenant_placements] w=[control.workspaces, ops.jobs, ops.outbox,
//!   private.evidence_objects, projection.stream_checkpoints, projection.stream_log]
//!   x=[control.ensure_admission_tier, control.ensure_user, control.issue_api_key, control.onboard_tenant,
//!   control.onboard_workspace, control.revoke_api_key, projection.activate_empty_family,
//!   projection.ensure_tenant_placement], PostgreSQL(role_gateway), PostgreSQL(role_maintenance), Qdrant(*),
//!   subprocess(humaux-maintenance)];
//!   env=[CARGO_BIN_EXE_humaux-maintenance, CARGO_MANIFEST_DIR, HUMAUX_MAINTENANCE_CREDENTIAL_PEPPER_HEX,
//!   HUMAUX_MAINTENANCE_EMBEDDING_DIMENSION, HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_MAINTENANCE_PRIVATE_MEMORY_COLLECTION,
//!   HUMAUX_MAINTENANCE_QDRANT_CIDR, HUMAUX_MAINTENANCE_QDRANT_HOST, HUMAUX_MAINTENANCE_QDRANT_PORT,
//!   HUMAUX_TEST_PG_DSN, HUMAUX_TEST_QDRANT_PORT]; modules=[humaux-testkit, protocol::edge]
//! Called-by: [cargo-test]
//! Invariants: [each test owns its throwaway database humaux_thread_c28_mtest_<pid>_<n> and Qdrant collection
//!   humaux_c28_mtest_<pid>_<n>, both removed by the fixture's Drop even on panic; missing env -> §79.2 skip_or_fail]
//! Spec: Baseline §4.2; §6.2.2; §16.3; §73.5; §77; §79.2; ADR-0053; ADR-0058
//!
//! Every test runs the release-shaped binary (`CARGO_BIN_EXE_humaux-maintenance`) against its own
//! throwaway database `humaux_thread_c28_mtest_<pid>_<n>` (created from the migration files here,
//! dropped by the fixture's `Drop`, even on panic) and, where the flow touches Qdrant, its own
//! collection `humaux_c28_mtest_<pid>_<n>` on the shared container (deleted by the same `Drop`).
//! Nothing here touches another database or collection.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

use humaux_protocol::edge::compute_api_key_hash;
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use serde_json::Value;
use uuid::Uuid;

const OWNER_DSN: &str = "HUMAUX_TEST_PG_DSN";
const MAINTENANCE_DSN: &str = "HUMAUX_MAINTENANCE_PG_DSN";
const QDRANT_PORT: &str = "HUMAUX_TEST_QDRANT_PORT";
const DIMENSION: u32 = 8;
const PROVIDER: &str = "dashscope";
const REGION: &str = "cn-beijing";
const ADMIN: [&str; 8] = [
    "--actor",
    "c28-test",
    "--reason",
    "card 28 onboarding test",
    "--ticket",
    "T-28",
    "--step-up-auth",
    "test-mfa",
];

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// `postgres://user:pass@host:port/<db>?query` with the database replaced.
fn with_db(dsn: &str, db: &str) -> String {
    let (head, tail) = dsn.split_at(dsn.rfind('/').expect("dsn has a database path") + 1);
    let query = tail.find('?').map_or("", |i| &tail[i..]);
    format!("{head}{db}{query}")
}

/// One minimal HTTP/1.0 exchange with the shared Qdrant (no client crate for a test helper).
fn qdrant(port: u16, method: &str, path: &str, body: Option<&Value>) -> (u16, Value) {
    let body = body.map(Value::to_string).unwrap_or_default();
    // dep: Qdrant(*) — the test's own collection (point seeding, index read, teardown)
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("qdrant reachable");
    write!(
        stream,
        "{method} {path} HTTP/1.0\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .expect("write request");
    let mut raw = String::new();
    stream.read_to_string(&mut raw).expect("read response");
    let status = raw
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let json = raw
        .split_once("\r\n\r\n")
        .and_then(|(_, b)| serde_json::from_str(b).ok())
        .unwrap_or(Value::Null);
    (status, json)
}

struct Fixture {
    owner_dsn: String,
    maintenance_dsn: String,
    qdrant_port: u16,
    db: String,
    collection: String,
    pepper: String,
    client: Option<Client>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.client.take());
        // dep: PostgreSQL(owner) — drop this test's throwaway database
        if let Ok(mut admin) = Client::connect(&with_db(&self.owner_dsn, "postgres"), NoTls) {
            let _ =
                admin.batch_execute(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.db));
        }
        let _ = std::panic::catch_unwind(|| {
            qdrant(
                self.qdrant_port,
                "DELETE",
                &format!("/collections/{}", self.collection),
                None,
            )
        });
    }
}

/// Applies every migration file in order (bodies only; the executed manifest checks are the
/// `xtask migrate` gate's job, run separately on its own throwaway database).
fn migrate(client: &mut Client) {
    // Role DDL is cluster-global: 0201's ALTER ROLE from parallel fixtures onto the one pg_authid row
    // fails "tuple concurrently updated", so this process applies one database at a time.
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
    // `xtask migrate` bootstraps its ledger before the first migration; 0201 (ADR-0059 D-C) revokes
    // runtime writes on it, so a bodies-only apply needs the same table first.
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

fn fixture(test: &str) -> Option<Fixture> {
    let (Ok(owner_dsn), Ok(maintenance_dsn), Ok(port)) = (
        std::env::var(OWNER_DSN),
        std::env::var(MAINTENANCE_DSN),
        std::env::var(QDRANT_PORT),
    ) else {
        skip_or_fail(
            test,
            "HUMAUX_TEST_PG_DSN / HUMAUX_MAINTENANCE_PG_DSN / HUMAUX_TEST_QDRANT_PORT",
            ExternalDep::Postgres,
        );
        return None;
    };
    let n = NEXT.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    let db = format!("humaux_thread_c28_mtest_{pid}_{n}");
    // dep: PostgreSQL(owner) — create this test's throwaway database
    let mut admin = Client::connect(&with_db(&owner_dsn, "postgres"), NoTls).ok()?;
    admin
        .batch_execute(&format!("CREATE DATABASE {db}"))
        .expect("create throwaway db");
    drop(admin);
    let mut fixture = Fixture {
        maintenance_dsn: with_db(&maintenance_dsn, &db),
        owner_dsn: owner_dsn.clone(),
        qdrant_port: port.parse().expect("qdrant port"),
        collection: format!("humaux_c28_mtest_{pid}_{n}"),
        pepper: hex_of(Uuid::new_v4().as_bytes()) + &hex_of(Uuid::new_v4().as_bytes()),
        db: db.clone(),
        client: None,
    };
    // dep: PostgreSQL(owner) — fixture connection to the throwaway database
    let mut client = Client::connect(&with_db(&owner_dsn, &db), NoTls).expect("connect test db");
    migrate(&mut client);
    fixture.client = Some(client);
    Some(fixture)
}

fn hex_of(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl Fixture {
    fn db(&mut self) -> &mut Client {
        self.client.as_mut().expect("client")
    }

    fn run(&self, args: &[&str]) -> Output {
        self.command(args).output().expect("run humaux-maintenance")
    }

    fn command(&self, args: &[&str]) -> Command {
        // dep: subprocess(humaux-maintenance) — the binary under test
        let mut command = Command::new(env!("CARGO_BIN_EXE_humaux-maintenance"));
        command
            .args(args)
            .env_clear()
            .env("HUMAUX_MAINTENANCE_PG_DSN", &self.maintenance_dsn)
            .env("HUMAUX_MAINTENANCE_CREDENTIAL_PEPPER_HEX", &self.pepper)
            .env("HUMAUX_MAINTENANCE_QDRANT_HOST", "127.0.0.1")
            .env(
                "HUMAUX_MAINTENANCE_QDRANT_PORT",
                self.qdrant_port.to_string(),
            )
            .env("HUMAUX_MAINTENANCE_QDRANT_CIDR", "127.0.0.1/32")
            .env(
                "HUMAUX_MAINTENANCE_EMBEDDING_DIMENSION",
                DIMENSION.to_string(),
            )
            .env(
                "HUMAUX_MAINTENANCE_PRIVATE_MEMORY_COLLECTION",
                &self.collection,
            );
        command
    }

    fn run_admin(&self, args: &[&str]) -> Output {
        let mut all: Vec<&str> = args.to_vec();
        all.extend(ADMIN);
        self.run(&all)
    }

    fn deploy_init(&self) -> Value {
        let out = self.run_admin(&[
            "deploy-init",
            "--provider",
            PROVIDER,
            "--region",
            REGION,
            "--tpm",
            "100000",
            "--rpm",
            "1000",
        ]);
        assert_eq!(out.status.code(), Some(0), "deploy-init: {}", stderr(&out));
        receipt(&out)
    }

    fn onboard(&self, name: &str) -> Output {
        self.onboard_command(name)
            .output()
            .expect("run humaux-maintenance")
    }

    fn onboard_command(&self, name: &str) -> Command {
        let mut args = vec![
            "onboard",
            "tenant",
            "--name",
            name,
            "--owner-email",
            "Owner@Example.com",
            "--plan-limit",
            "1000",
            "--period-end",
            "2099-01-01T00:00:00Z",
            "--scopes",
            "memory:write,context:read",
            "--key-name",
            "k1",
            "--provider",
            PROVIDER,
            "--region",
            REGION,
            "--tenant-tpm",
            "10000",
            "--tenant-rpm",
            "100",
        ];
        args.extend(ADMIN);
        self.command(&args)
    }

    /// deploy-init + onboard tenant `name`; returns the receipt and the printed wire key.
    fn onboarded(&self, name: &str) -> (Value, Option<String>) {
        self.deploy_init();
        let out = self.onboard(name);
        assert_eq!(out.status.code(), Some(0), "onboard: {}", stderr(&out));
        (receipt(&out), wire(&out))
    }

    /// A PROVISIONING tenant straight through the 0186 doors as role_maintenance (the state the
    /// CLI passes through between T1 and the activation), plus the deployment tiers.
    fn provisioning_tenant(&mut self, name: &str) -> (Uuid, Uuid, Uuid) {
        let client = self.db();
        // dep: PostgreSQL(role_maintenance) — the 0186 doors as the only role that may call them
        client
            .batch_execute(&format!(
                "SET ROLE role_maintenance; \
                 SELECT control.ensure_admission_tier('{PROVIDER}', NULL, NULL, NULL, 10, 10); \
                 SELECT control.ensure_admission_tier('{PROVIDER}', '{REGION}', NULL, NULL, 10, 10);"
            ))
            .expect("tiers");
        let mut txn = client.transaction().expect("txn");
        let user: Uuid = txn
            .query_one(
                "SELECT user_id FROM control.ensure_user($1, $1)",
                &[&format!("{name}@example.com")],
            )
            .expect("ensure_user")
            .get(0);
        let row = txn
            .query_one(
                "SELECT tenant_id, workspace_id FROM control.onboard_tenant($1, $2, 'default', \
                 'default', 10, now() - interval '1 second', now() + interval '1 day', $3, $4, \
                 10, 10, ARRAY['private_memory','PRIVATE_MEMORY','v1'])",
                &[&name, &user, &PROVIDER, &REGION],
            )
            .expect("onboard_tenant");
        txn.commit().expect("commit");
        client.batch_execute("RESET ROLE").expect("reset role");
        (row.get(0), row.get(1), user)
    }

    fn placement_and_collection(&self, tenant: Uuid) {
        let out = self.run_admin(&["placement", "ensure", "--tenant", &tenant.to_string()]);
        assert_eq!(out.status.code(), Some(0), "placement: {}", stderr(&out));
        let out = self.run(&["collection", "ensure"]);
        assert_eq!(out.status.code(), Some(0), "collection: {}", stderr(&out));
    }

    fn activate(&self, tenant: Uuid, workspace: Uuid) -> Output {
        self.run_admin(&[
            "activate",
            "--tenant",
            &tenant.to_string(),
            "--workspace",
            &workspace.to_string(),
        ])
    }

    fn count(&mut self, sql: &str) -> i64 {
        self.db().query_one(sql, &[]).expect(sql).get(0)
    }

    fn lifecycle(&mut self, workspace: Uuid) -> String {
        self.db()
            .query_one(
                "SELECT lifecycle FROM control.workspaces WHERE workspace_id = $1",
                &[&workspace],
            )
            .expect("lifecycle")
            .get(0)
    }
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// The one JSON receipt: always the last stdout line.
fn receipt(out: &Output) -> Value {
    let text = stdout(out);
    let last = text.lines().last().unwrap_or_default();
    serde_json::from_str(last).unwrap_or_else(|e| panic!("receipt JSON ({e}): {last}"))
}

/// The one-time wire key line, if the run printed one.
fn wire(out: &Output) -> Option<String> {
    stdout(out)
        .lines()
        .find_map(|l| l.strip_prefix("Authorization: Bearer ").map(str::to_owned))
}

fn uuid_of(v: &Value, key: &str) -> Uuid {
    v[key]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("{key} in {v}"))
}

/// Row counts of every table onboarding writes — the idempotency witness.
const TOUCHED: [&str; 14] = [
    "control.tenants",
    "control.users",
    "control.user_emails",
    "control.memberships",
    "control.workspaces",
    "control.workspace_memberships",
    "control.private_reasoning_domains",
    "control.entitlement_snapshots",
    "control.api_keys",
    "control.retrieval_provider_admission_limits",
    "control.quota_windows",
    "control.audit_events",
    "projection.tenant_placements",
    "projection.family_activations",
];

fn counts(f: &mut Fixture) -> Vec<(String, i64)> {
    TOUCHED
        .iter()
        .map(|t| {
            (
                (*t).to_owned(),
                f.count(&format!("SELECT count(*) FROM {t}")),
            )
        })
        .collect()
}

#[test]
fn deploy_init_creates_global_and_region_tiers_once() {
    let Some(mut f) = fixture("deploy_init_creates_global_and_region_tiers_once") else {
        return;
    };
    let first = f.deploy_init();
    assert_eq!(first["outcome"], "created");
    let tiers: Vec<(&str, bool)> = first["tiers"]
        .as_array()
        .expect("tiers")
        .iter()
        .map(|t| (t["tier"].as_str().unwrap(), t["created"].as_bool().unwrap()))
        .collect();
    assert_eq!(tiers, vec![("GLOBAL", true), ("REGION", true)]);
    let second = f.deploy_init();
    assert_eq!(second["outcome"], "existing");
    assert!(second["audit_event_id"].is_null());
    assert_eq!(
        f.count("SELECT count(*) FROM control.retrieval_provider_admission_limits WHERE tenant_id IS NULL"),
        2
    );
}

#[test]
fn onboard_tenant_refuses_without_global_and_region_tiers() {
    let Some(mut f) = fixture("onboard_tenant_refuses_without_global_and_region_tiers") else {
        return;
    };
    let out = f.onboard("t1");
    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    assert_eq!(receipt(&out)["reason"], "deployment_admission_missing");
    assert_eq!(wire(&out), None);
    assert_eq!(
        f.count("SELECT count(*) FROM control.tenants WHERE onboarding_name IS NOT NULL"),
        0
    );
    assert_eq!(
        f.count("SELECT count(*) FROM control.users"),
        0,
        "T1 rolled back whole"
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one receipt, every field of the card's gate"
)]
fn onboard_tenant_creates_every_row_and_activates_verified_empty() {
    let Some(mut f) = fixture("onboard_tenant_creates_every_row_and_activates_verified_empty")
    else {
        return;
    };
    let (r, wire) = f.onboarded("t1");
    assert_eq!(r["outcome"], "created");
    assert_eq!(r["lifecycle"], "READY");
    let tenant = uuid_of(&r, "tenant_id");
    let workspace = uuid_of(&r, "workspace_id");
    let user = uuid_of(&r, "owner_user_id");
    let domain = uuid_of(&r, "reasoning_domain_id");
    let tiers: Vec<&str> = r["admission_tiers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["tier"].as_str().unwrap())
        .collect();
    for tier in ["GLOBAL", "REGION", "TENANT", "PURPOSE"] {
        assert!(tiers.contains(&tier), "{tier} missing from {tiers:?}");
    }
    assert_eq!(r["placements"][0]["collection_name"], f.collection.as_str());
    assert_eq!(
        r["collection"]["payload_indexes"],
        serde_json::json!(["tenant_id", "subject_ids"])
    );
    let activation = &r["activations"][0];
    assert_eq!(activation["outcome"], "activated");
    assert_eq!(activation["evidence"], "VerifiedEmpty");
    assert_eq!(activation["probe_visible"], 0);

    // The printed key authenticates against the stored §73.5 hash; the receipt has only the
    // fingerprint.
    let wire = wire.expect("wire printed once on created");
    let prefix = r["api_key"]["prefix"].as_str().unwrap().to_owned();
    assert!(wire.starts_with(&format!("{prefix}.")));
    let pepper: Vec<u8> = (0..f.pepper.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&f.pepper[i..i + 2], 16).unwrap())
        .collect();
    let db = f.db();
    let key = db
        .query_one(
            "SELECT key_hash, status, user_id, workspace_id, scopes FROM control.api_keys WHERE prefix = $1",
            &[&prefix],
        )
        .expect("api key row");
    assert_eq!(
        key.get::<_, Vec<u8>>(0),
        compute_api_key_hash(&pepper, &wire)
    );
    assert_eq!(key.get::<_, String>(1), "ACTIVE");
    assert_eq!(key.get::<_, Uuid>(2), user);
    assert_eq!(key.get::<_, Uuid>(3), workspace);
    assert_eq!(
        key.get::<_, Vec<String>>(4),
        vec!["memory:write".to_owned(), "context:read".to_owned()]
    );

    for (sql, want) in [
        (
            "SELECT count(*) FROM control.tenants WHERE tenant_id = $1 AND state = 'ACTIVE' AND onboarding_name = 't1'",
            1,
        ),
        (
            "SELECT count(*) FROM control.memberships WHERE tenant_id = $1 AND role = 'OWNER' AND state = 'ACTIVE'",
            1,
        ),
        (
            "SELECT count(*) FROM control.workspace_memberships WHERE tenant_id = $1 AND role = 'OWNER' AND state = 'ACTIVE'",
            1,
        ),
        (
            "SELECT count(*) FROM control.workspaces WHERE tenant_id = $1 AND lifecycle = 'READY'",
            1,
        ),
        (
            "SELECT count(*) FROM control.entitlement_snapshots WHERE tenant_id = $1",
            1,
        ),
        (
            "SELECT count(*) FROM control.quota_windows WHERE tenant_id = $1",
            1,
        ),
        (
            "SELECT count(*) FROM control.retrieval_provider_admission_limits WHERE tenant_id = $1",
            3,
        ),
        (
            "SELECT count(*) FROM projection.tenant_placements WHERE tenant_id = $1",
            1,
        ),
        (
            "SELECT count(*) FROM projection.stream_checkpoints WHERE tenant_id = $1 AND serving AND issued_highwater = 0",
            1,
        ),
        (
            "SELECT count(*) FROM projection.family_activations WHERE tenant_id = $1 AND evidence_kind = 'VERIFIED_EMPTY'",
            1,
        ),
        (
            "SELECT count(*) FROM control.audit_events WHERE tenant_id = $1 AND action IN ('ONBOARD_TENANT','FAMILY_ACTIVATE') AND result = 'SUCCESS'",
            2,
        ),
    ] {
        let n: i64 = db.query_one(sql, &[&tenant]).expect(sql).get(0);
        assert_eq!(n, want, "{sql}");
    }
    let owner: Uuid = db
        .query_one(
            "SELECT owner_user_id FROM control.private_reasoning_domains WHERE reasoning_domain_id = $1",
            &[&domain],
        )
        .unwrap()
        .get(0);
    assert_eq!(owner, user);
    let verified: Option<String> = db
        .query_one(
            "SELECT verified_at::text FROM control.user_emails WHERE user_id = $1 AND canonical_email = 'Owner@example.com'",
            &[&user],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        verified, None,
        "§74 verification is post-go-live: stored unverified"
    );

    let (status, info) = qdrant(
        f.qdrant_port,
        "GET",
        &format!("/collections/{}", f.collection),
        None,
    );
    assert_eq!(status, 200);
    assert_eq!(
        info["result"]["payload_schema"]["tenant_id"]["data_type"],
        "keyword"
    );
    assert_eq!(
        info["result"]["payload_schema"]["subject_ids"]["data_type"],
        "uuid"
    );
}

#[test]
fn onboard_tenant_rerun_is_existing_and_writes_no_row() {
    let Some(mut f) = fixture("onboard_tenant_rerun_is_existing_and_writes_no_row") else {
        return;
    };
    f.onboarded("t1");
    let before = counts(&mut f);
    let out = f.onboard("t1");
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let r = receipt(&out);
    assert_eq!(r["outcome"], "existing");
    assert!(r["api_key"].is_null());
    assert_eq!(r["activations"][0]["outcome"], "existing");
    assert_eq!(r["lifecycle"], "READY");
    assert_eq!(wire(&out), None, "a re-run never prints a key");
    assert_eq!(counts(&mut f), before);
}

#[test]
fn concurrent_onboard_tenant_same_name_yields_exactly_one_tenant_and_one_key() {
    let Some(mut f) =
        fixture("concurrent_onboard_tenant_same_name_yields_exactly_one_tenant_and_one_key")
    else {
        return;
    };
    f.deploy_init();
    // Two processes started back to back, both racing through T1.
    let first = f
        .onboard_command("t2")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();
    let second = f
        .onboard_command("t2")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();
    let a = first.unwrap().wait_with_output().unwrap();
    let b = second.unwrap().wait_with_output().unwrap();
    assert_eq!(a.status.code(), Some(0), "{}", stderr(&a));
    assert_eq!(b.status.code(), Some(0), "{}", stderr(&b));
    let outcomes = [
        receipt(&a)["outcome"].clone(),
        receipt(&b)["outcome"].clone(),
    ];
    assert!(
        outcomes.contains(&Value::from("created")) && outcomes.contains(&Value::from("existing"))
    );
    assert_eq!(
        [wire(&a), wire(&b)].iter().filter(|w| w.is_some()).count(),
        1
    );
    assert_eq!(
        f.count("SELECT count(*) FROM control.tenants WHERE onboarding_name = 't2'"),
        1
    );
    assert_eq!(f.count("SELECT count(*) FROM control.api_keys"), 1);
    assert_eq!(
        f.count("SELECT count(*) FROM control.users"),
        1,
        "no orphan user"
    );
}

/// The write gate on the one statement every stream-issuing write executes
/// (`remember::issue_stream_log_row`), run as `role_gateway` exactly as `remember.put` runs it.
/// The wire-level `remember.put` → CONFLICT is pinned in `bins/gateway/tests/read_decoupling.rs`.
fn remember_ticket(f: &mut Fixture, tenant: Uuid, workspace: Uuid) -> Result<i64, postgres::Error> {
    let db = f.db();
    // dep: PostgreSQL(role_gateway) — the stream-issuing statement as remember.put runs it
    db.batch_execute("SET ROLE role_gateway").unwrap();
    let result = (|| {
        let mut txn = db.transaction()?;
        txn.execute(
            "SELECT set_config('humaux.tenant_id', $1, true)",
            &[&tenant.to_string()],
        )?;
        txn.execute(
            "INSERT INTO projection.stream_checkpoints (tenant_id, scope_kind, scope_id, domain, \
             projection_kind, projection_version) VALUES ($1, 'workspace', $2, 'private_memory', \
             'PRIVATE_MEMORY', 'v1') ON CONFLICT DO NOTHING",
            &[&tenant, &workspace],
        )?;
        let seq: i64 = txn
            .query_one(
                "UPDATE projection.stream_checkpoints SET issued_highwater = issued_highwater + 1 \
                 WHERE tenant_id = $1 AND scope_kind = 'workspace' AND scope_id = $2 \
                 RETURNING issued_highwater",
                &[&tenant, &workspace],
            )?
            .get(0);
        txn.execute(
            "INSERT INTO projection.stream_log (tenant_id, scope_kind, scope_id, domain, \
             projection_kind, projection_version, stream_seq, commit_seq) VALUES ($1, 'workspace', \
             $2, 'private_memory', 'PRIVATE_MEMORY', 'v1', $3, $3)",
            &[&tenant, &workspace, &seq],
        )?;
        txn.commit()?;
        Ok(seq)
    })();
    db.batch_execute("RESET ROLE").unwrap();
    result
}

#[test]
fn remember_while_provisioning_is_refused_55000_and_nothing_commits() {
    let Some(mut f) = fixture("remember_while_provisioning_is_refused_55000_and_nothing_commits")
    else {
        return;
    };
    let (tenant, workspace, _) = f.provisioning_tenant("p1");
    assert_eq!(f.lifecycle(workspace), "PROVISIONING");
    let err = remember_ticket(&mut f, tenant, workspace).expect_err("gated");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "55000");
    assert_eq!(db.message(), "workspace_provisioning");
    assert_eq!(f.count("SELECT count(*) FROM projection.stream_log"), 0);
    assert_eq!(
        f.count("SELECT max(issued_highwater) FROM projection.stream_checkpoints"),
        0
    );
}

#[test]
fn remember_after_ready_succeeds() {
    let Some(mut f) = fixture("remember_after_ready_succeeds") else {
        return;
    };
    let (tenant, workspace, _) = f.provisioning_tenant("p1");
    f.placement_and_collection(tenant);
    let out = f.activate(tenant, workspace);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(f.lifecycle(workspace), "READY");
    assert_eq!(
        remember_ticket(&mut f, tenant, workspace).expect("ungated"),
        1
    );
    // …and the head that moved is exactly the one the activation required to be 0: activating
    // again is `existing`, never a re-switch (R-28: no re-switch after the first batch).
    let again = f.activate(tenant, workspace);
    assert_eq!(receipt(&again)["activations"][0]["outcome"], "existing");
}

#[test]
fn activate_refuses_not_empty_when_family_has_stream_rows() {
    let Some(mut f) = fixture("activate_refuses_not_empty_when_family_has_stream_rows") else {
        return;
    };
    let (tenant, workspace, _) = f.provisioning_tenant("p1");
    f.placement_and_collection(tenant);
    f.db()
        .execute(
            "INSERT INTO projection.stream_log (tenant_id, scope_kind, scope_id, domain, \
             projection_kind, projection_version, stream_seq, commit_seq) VALUES ($1, 'workspace', \
             $2, 'private_memory', 'PRIVATE_MEMORY', 'v1', 1, 1)",
            &[&tenant, &workspace],
        )
        .unwrap();
    let out = f.activate(tenant, workspace);
    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    assert_eq!(receipt(&out)["reason"], "not_empty");
    assert_eq!(f.lifecycle(workspace), "PROVISIONING");
    assert_eq!(
        f.count("SELECT count(*) FROM projection.family_activations"),
        0
    );
    assert_eq!(
        f.count("SELECT count(*) FROM projection.stream_checkpoints WHERE serving"),
        0
    );
    assert_eq!(
        f.count("SELECT count(*) FROM control.audit_events WHERE action = 'FAMILY_ACTIVATE' AND result = 'DENIED'"),
        1
    );
}

#[test]
fn activate_refuses_placement_missing() {
    let Some(mut f) = fixture("activate_refuses_placement_missing") else {
        return;
    };
    let (tenant, workspace, _) = f.provisioning_tenant("p1");
    let out = f.activate(tenant, workspace);
    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    assert_eq!(receipt(&out)["reason"], "placement_missing");
    assert_eq!(f.lifecycle(workspace), "PROVISIONING");
}

#[test]
fn activate_refuses_family_not_initialized() {
    let Some(mut f) = fixture("activate_refuses_family_not_initialized") else {
        return;
    };
    let (tenant, workspace, _) = f.provisioning_tenant("p1");
    f.placement_and_collection(tenant);
    f.db()
        .execute(
            "DELETE FROM projection.stream_checkpoints WHERE scope_id = $1",
            &[&workspace],
        )
        .unwrap();
    let out = f.activate(tenant, workspace);
    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    assert_eq!(receipt(&out)["reason"], "family_not_initialized");
}

#[test]
fn activate_refuses_probe_not_empty() {
    let Some(mut f) = fixture("activate_refuses_probe_not_empty") else {
        return;
    };
    let (tenant, workspace, _) = f.provisioning_tenant("p1");
    f.placement_and_collection(tenant);
    // One stray point for exactly this family in the physical index.
    let point = serde_json::json!({ "points": [{
        "id": Uuid::new_v4().to_string(),
        "vector": vec![0.5_f32; DIMENSION as usize],
        "payload": {
            "tenant_id": tenant.to_string(),
            "workspace_id": workspace.to_string(),
            "projection_version": "v1",
            "visibility_class": "TENANT_SHARED",
        }
    }]});
    let (status, _) = qdrant(
        f.qdrant_port,
        "PUT",
        &format!("/collections/{}/points?wait=true", f.collection),
        Some(&point),
    );
    assert_eq!(status, 200);
    let out = f.activate(tenant, workspace);
    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    let r = receipt(&out);
    assert_eq!(r["reason"], "probe_not_empty");
    assert_eq!(r["activations"][0]["probe_visible"], 1);
    assert_eq!(f.lifecycle(workspace), "PROVISIONING");
}

#[test]
fn activate_refuses_not_provisioning_for_legacy_workspace() {
    let Some(mut f) = fixture("activate_refuses_not_provisioning_for_legacy_workspace") else {
        return;
    };
    let (tenant, _, _) = f.provisioning_tenant("p1");
    f.placement_and_collection(tenant);
    let legacy: Uuid = f
        .db()
        .query_one(
            "INSERT INTO control.workspaces (tenant_id, name) VALUES ($1, 'legacy') RETURNING workspace_id",
            &[&tenant],
        )
        .unwrap()
        .get(0);
    f.db()
        .execute(
            "INSERT INTO projection.stream_checkpoints (tenant_id, scope_kind, scope_id, domain, \
             projection_kind, projection_version) VALUES ($1, 'workspace', $2, 'private_memory', \
             'PRIVATE_MEMORY', 'v1')",
            &[&tenant, &legacy],
        )
        .unwrap();
    assert_eq!(f.lifecycle(legacy), "LEGACY");
    let out = f.activate(tenant, legacy);
    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    assert_eq!(receipt(&out)["reason"], "not_provisioning");
    // …and LEGACY is never gated.
    assert_eq!(
        remember_ticket(&mut f, tenant, legacy).expect("legacy ungated"),
        1
    );
}

#[test]
fn activate_is_idempotent_and_flips_ready_only_after_last_family() {
    let Some(mut f) = fixture("activate_is_idempotent_and_flips_ready_only_after_last_family")
    else {
        return;
    };
    let (tenant, workspace, _) = f.provisioning_tenant("p1");
    f.placement_and_collection(tenant);
    // A second required family of the same workspace (another projection_kind of the domain the
    // placement covers): READY must wait for it.
    f.db()
        .execute(
            "INSERT INTO projection.stream_checkpoints (tenant_id, scope_kind, scope_id, domain, \
             projection_kind, projection_version) VALUES ($1, 'workspace', $2, 'private_memory', \
             'SECOND_KIND', 'v1')",
            &[&tenant, &workspace],
        )
        .unwrap();
    let first = f.activate(tenant, workspace);
    assert_eq!(first.status.code(), Some(0), "{}", stderr(&first));
    assert_eq!(receipt(&first)["activations"][0]["outcome"], "activated");
    assert_eq!(receipt(&first)["lifecycle"], "PROVISIONING");
    let again = f.activate(tenant, workspace);
    assert_eq!(receipt(&again)["activations"][0]["outcome"], "existing");
    assert_eq!(receipt(&again)["outcome"], "existing");
    assert_eq!(
        f.count("SELECT count(*) FROM projection.family_activations"),
        1
    );
    // The last family, through the same door as role_maintenance.
    let collection = f.collection.clone();
    let db = f.db();
    // dep: PostgreSQL(role_maintenance) — the last family through the activation door
    db.batch_execute("SET ROLE role_maintenance").unwrap();
    let mut txn = db.transaction().unwrap();
    txn.execute(
        "SELECT set_config('humaux.tenant_id', $1, true)",
        &[&tenant.to_string()],
    )
    .unwrap();
    let row = txn
        .query_one(
            "SELECT outcome, workspace_ready FROM projection.activate_empty_family($1, $2, \
             'private_memory', 'SECOND_KIND', 'v1', $3, 'g', $4, 0, now())",
            &[&tenant, &workspace, &collection, &Uuid::new_v4()],
        )
        .unwrap();
    txn.commit().unwrap();
    db.batch_execute("RESET ROLE").unwrap();
    assert_eq!(row.get::<_, String>(0), "ACTIVATED");
    assert!(row.get::<_, bool>(1));
    assert_eq!(f.lifecycle(workspace), "READY");
}

#[test]
fn activation_receipt_is_bound_to_the_full_family_and_generation() {
    let Some(mut f) = fixture("activation_receipt_is_bound_to_the_full_family_and_generation")
    else {
        return;
    };
    let (r, _) = f.onboarded("t1");
    let a = &r["activations"][0];
    let row = f
        .db()
        .query_one(
            "SELECT tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
             collection_name, collection_generation, probe_id, initialized_head, probe_visible \
             FROM projection.family_activations",
            &[],
        )
        .unwrap();
    assert_eq!(row.get::<_, Uuid>(0), uuid_of(&r, "tenant_id"));
    assert_eq!(row.get::<_, String>(1), "workspace");
    assert_eq!(row.get::<_, Uuid>(2), uuid_of(&r, "workspace_id"));
    assert_eq!(row.get::<_, String>(3), a["domain"].as_str().unwrap());
    assert_eq!(
        row.get::<_, String>(4),
        a["projection_kind"].as_str().unwrap()
    );
    assert_eq!(
        row.get::<_, String>(5),
        a["projection_version"].as_str().unwrap()
    );
    assert_eq!(row.get::<_, String>(6), f.collection);
    let generation = row.get::<_, String>(7);
    assert_eq!(generation, a["generation"].as_str().unwrap());
    assert_eq!(generation, r["collection"]["generation"].as_str().unwrap());
    let digest = generation
        .strip_prefix(&format!("{}@", f.collection))
        .expect("generation names its collection");
    assert!(digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(row.get::<_, Uuid>(8), uuid_of(a, "probe_id"));
    assert_eq!(row.get::<_, i64>(9), 0);
    assert_eq!(row.get::<_, i64>(10), 0);
}

#[test]
fn apikey_issue_prints_wire_once_and_rerun_is_existing() {
    let Some(mut f) = fixture("apikey_issue_prints_wire_once_and_rerun_is_existing") else {
        return;
    };
    let (r, _) = f.onboarded("t1");
    let (tenant, user, workspace) = (
        uuid_of(&r, "tenant_id").to_string(),
        uuid_of(&r, "owner_user_id").to_string(),
        uuid_of(&r, "workspace_id").to_string(),
    );
    let issue = || {
        f.run_admin(&[
            "apikey",
            "issue",
            "--tenant",
            &tenant,
            "--user",
            &user,
            "--workspace",
            &workspace,
            "--scopes",
            "memory:write",
            "--key-name",
            "k2",
        ])
    };
    let first = issue();
    assert_eq!(first.status.code(), Some(0), "{}", stderr(&first));
    assert_eq!(stdout(&first).lines().count(), 2);
    let wire = wire(&first).expect("wire on created");
    assert_eq!(receipt(&first)["outcome"], "created");
    let second = issue();
    assert_eq!(second.status.code(), Some(0));
    assert_eq!(stdout(&second).lines().count(), 1, "no wire on a re-run");
    assert_eq!(receipt(&second)["outcome"], "existing");
    assert_eq!(receipt(&second)["prefix"], receipt(&first)["prefix"]);
    assert!(!stdout(&second).contains(&wire));
    assert_eq!(f.count("SELECT count(*) FROM control.api_keys"), 2);
}

#[test]
fn apikey_revoke_is_idempotent() {
    let Some(mut f) = fixture("apikey_revoke_is_idempotent") else {
        return;
    };
    let (r, _) = f.onboarded("t1");
    let tenant = uuid_of(&r, "tenant_id").to_string();
    let revoke = || f.run_admin(&["apikey", "revoke", "--tenant", &tenant, "--key-name", "k1"]);
    let first = revoke();
    assert_eq!(first.status.code(), Some(0), "{}", stderr(&first));
    assert_eq!(receipt(&first)["changed"], true);
    let second = revoke();
    assert_eq!(receipt(&second)["changed"], false);
    assert_eq!(receipt(&second)["outcome"], "existing");
    assert_eq!(
        f.count("SELECT count(*) FROM control.api_keys WHERE status = 'REVOKED' AND revoked_at IS NOT NULL"),
        1
    );
}

/// ADR-0059 D-H (migration 0205): `apikey pepper-epoch advance` opens one window per epoch; a second
/// `advance` while it is open is refused `rehash_window_open` (exit 3) and leaves epoch and window
/// unchanged; `close` re-runs as a no-op; after `close` the next `advance` moves on. Runs on this
/// test's own database, so the shared dev epoch never moves.
#[test]
fn apikey_pepper_epoch_advance_refuses_while_the_window_is_open() {
    let Some(mut f) = fixture("apikey_pepper_epoch_advance_refuses_while_the_window_is_open")
    else {
        return;
    };
    let pepper = |f: &mut Fixture| -> (i32, bool) {
        // dep: PostgreSQL(owner) — read the singleton pepper state of this test's database
        let row = f
            .db()
            .query_one(
                "SELECT epoch, rehash_open FROM control.credential_pepper_state",
                &[],
            )
            .expect("pepper state");
        (row.get(0), row.get(1))
    };
    let (start, open) = pepper(&mut f);
    assert!(!open, "a fresh database starts with the window closed");
    let advance = |f: &Fixture| f.run_admin(&["apikey", "pepper-epoch", "advance"]);
    let close = |f: &Fixture| f.run_admin(&["apikey", "pepper-epoch", "close"]);

    let first = advance(&f);
    assert_eq!(first.status.code(), Some(0), "{}", stderr(&first));
    assert_eq!(receipt(&first)["epoch"], start + 1);
    assert_eq!(pepper(&mut f), (start + 1, true));

    let again = advance(&f);
    assert_eq!(again.status.code(), Some(3), "{}", stdout(&again));
    assert_eq!(receipt(&again)["outcome"], "refused");
    assert_eq!(receipt(&again)["reason"], "rehash_window_open");
    assert_eq!(
        pepper(&mut f),
        (start + 1, true),
        "epoch and window unchanged"
    );

    for _ in 0..2 {
        let closed = close(&f);
        assert_eq!(closed.status.code(), Some(0), "{}", stderr(&closed));
        assert_eq!(pepper(&mut f), (start + 1, false));
    }
    let next = advance(&f);
    assert_eq!(next.status.code(), Some(0), "{}", stderr(&next));
    assert_eq!(pepper(&mut f), (start + 2, true));
}

#[test]
fn receipts_and_stderr_contain_no_secret_material() {
    let Some(mut f) = fixture("receipts_and_stderr_contain_no_secret_material") else {
        return;
    };
    f.deploy_init();
    let out = f.onboard("t1");
    let wire = wire(&out).expect("wire");
    let secret = wire.split_once('.').expect("prefix.secret").1.to_owned();
    let rerun = f.onboard("t1");
    let tenant = uuid_of(&receipt(&out), "tenant_id").to_string();
    let status = f.run(&["status", "--tenant", &tenant]);
    let dsn_password = f
        .maintenance_dsn
        .split_once("://")
        .and_then(|(_, rest)| rest.split_once('@'))
        .and_then(|(userinfo, _)| userinfo.split_once(':'))
        .map(|(_, p)| p.to_owned());
    for o in [&out, &rerun, &status] {
        let json_lines: String = stdout(o)
            .lines()
            .filter(|l| !l.starts_with("Authorization: Bearer "))
            .collect();
        for text in [json_lines, stderr(o)] {
            assert!(!text.contains(&secret), "wire secret leaked");
            assert!(!text.contains(&f.pepper), "pepper leaked");
            if let Some(p) = &dsn_password {
                assert!(!text.contains(p.as_str()), "dsn password leaked");
            }
        }
    }
    assert_eq!(
        stdout(&out).matches(&secret).count(),
        1,
        "the wire key appears exactly once"
    );
    assert!(f.count("SELECT count(*) FROM control.audit_events WHERE metadata::text LIKE '%' || 'Bearer' || '%'") == 0);
}

#[test]
fn onboarding_functions_are_not_executable_by_runtime_roles() {
    let Some(mut f) = fixture("onboarding_functions_are_not_executable_by_runtime_roles") else {
        return;
    };
    let functions = [
        "control.ensure_admission_tier(text,text,uuid,text,bigint,bigint)",
        "control.ensure_user(text,text)",
        "control.onboard_tenant(text,uuid,text,text,bigint,timestamptz,timestamptz,text,text,bigint,bigint,text[])",
        "control.onboard_workspace(uuid,text,uuid,text[])",
        "control.issue_api_key(uuid,uuid,uuid,text,bytea,text[])",
        "control.revoke_api_key(uuid,text)",
        "projection.ensure_tenant_placement(uuid,text,text)",
        "projection.activate_empty_family(uuid,uuid,text,text,text,text,text,uuid,bigint,timestamptz)",
    ];
    for function in functions {
        for role in [
            "role_gateway",
            "role_private_worker",
            "role_consolidation_worker",
            "role_public_worker",
            "role_retrieval_worker",
            "role_batch_issuer",
            "role_admin",
        ] {
            let can: bool = f
                .db()
                .query_one(
                    "SELECT has_function_privilege($1, $2::text::regprocedure, 'EXECUTE')",
                    &[&role, &function],
                )
                .unwrap()
                .get(0);
            assert!(!can, "{role} can EXECUTE {function}");
        }
        let maintenance: bool = f
            .db()
            .query_one(
                "SELECT has_function_privilege('role_maintenance', $1::text::regprocedure, 'EXECUTE')",
                &[&function],
            )
            .unwrap()
            .get(0);
        assert!(maintenance, "role_maintenance lacks EXECUTE on {function}");
    }
    // A runtime role calling a door is refused by PostgreSQL itself.
    let db = f.db();
    // dep: PostgreSQL(role_gateway) — a runtime role calling a door
    db.batch_execute("SET ROLE role_gateway").unwrap();
    let err = db
        .query_one("SELECT * FROM control.ensure_user('a@b.c', 'a@b.c')", &[])
        .expect_err("gateway cannot onboard");
    db.batch_execute("RESET ROLE").unwrap();
    assert_eq!(err.as_db_error().unwrap().code().code(), "42501");
}

#[test]
fn cli_refuses_missing_flags_without_defaults() {
    let bin = env!("CARGO_BIN_EXE_humaux-maintenance");
    // dep: subprocess(humaux-maintenance) — the binary under test, no environment
    let run = |args: &[&str]| Command::new(bin).args(args).env_clear().output().unwrap();
    // No admin fields: refused before any connection.
    let out = run(&[
        "deploy-init",
        "--provider",
        "p",
        "--region",
        "r",
        "--tpm",
        "1",
        "--rpm",
        "1",
    ]);
    assert_eq!(out.status.code(), Some(2));
    assert!(stderr(&out).contains("--actor"));
    // A missing business flag has no default.
    let mut args = vec![
        "onboard",
        "tenant",
        "--name",
        "t",
        "--owner-email",
        "a@b.c",
        "--period-end",
        "2099-01-01T00:00:00Z",
        "--scopes",
        "memory:write",
        "--key-name",
        "k",
        "--provider",
        "p",
        "--region",
        "r",
        "--tenant-tpm",
        "1",
        "--tenant-rpm",
        "1",
    ];
    args.extend(ADMIN);
    let out = run(&args);
    assert_eq!(out.status.code(), Some(2));
    assert!(stderr(&out).contains("--plan-limit"), "{}", stderr(&out));
    // Every flag present but no environment: the DSN/pepper are env-only, never defaulted.
    args.extend(["--plan-limit", "1"]);
    let out = run(&args);
    assert_eq!(out.status.code(), Some(2));
    assert!(
        stderr(&out).contains("HUMAUX_MAINTENANCE_"),
        "{}",
        stderr(&out)
    );
    let out = run(&["frobnicate"]);
    assert_eq!(out.status.code(), Some(2));
    assert!(stdout(&out).is_empty());
}

/// One accepted Evidence of `tenant` in its `default` reasoning domain, settled DEAD with `class`
/// the way a DEAD distill settle leaves it (job DEAD, outbox FAILED). Returns the job id.
fn dead_distill_job(f: &mut Fixture, tenant: Uuid, class: &str) -> Uuid {
    let mut txn = f.db().transaction().expect("txn");
    let evidence: Uuid = txn
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             SELECT $1, 'EVENT', sha256(convert_to(gen_random_uuid()::text, 'UTF8')), \
                    'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', d.reasoning_domain_id \
             FROM control.private_reasoning_domains d WHERE d.tenant_id = $1 \
             RETURNING evidence_id",
            &[&tenant],
        )
        .expect("evidence")
        .get(0);
    // The 0164 enqueue trigger writes the DERIVED_DISTILL job for the outbox row.
    txn.execute(
        "INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id) \
         VALUES ($1, nextval('ops.commit_seq_seq'), 1, 'EVIDENCE_ACCEPTED', $2)",
        &[&tenant, &evidence],
    )
    .expect("outbox");
    txn.execute(
        "UPDATE ops.outbox SET status = 'FAILED', processed_at = now() WHERE evidence_id = $1",
        &[&evidence],
    )
    .expect("outbox FAILED");
    let job: Uuid = txn
        .query_one(
            "UPDATE ops.jobs SET status = 'DEAD', attempt = 3, last_error_class = $2 \
             WHERE job_type = 'DERIVED_DISTILL' AND payload ->> 'evidence_id' = $1::uuid::text \
             RETURNING job_id",
            &[&evidence, &class],
        )
        .expect("job DEAD")
        .get(0);
    txn.commit().expect("commit");
    job
}

/// ADR-0058 R4 through the binary: `jobs requeue-dead` takes exactly one of `--job` /
/// `--error-class` (usage otherwise), prints what it re-armed, re-arms by exact class only, and
/// refuses a re-run (exit 3, `job_not_dead`) with its DENIED audit row. Fault: the CLI accepts
/// both flags (takes `--job`) ⇒ the usage step exits 0 (red).
#[test]
fn jobs_requeue_dead_prints_what_it_rearmed_and_refuses_a_rerun() {
    let Some(mut f) = fixture("jobs_requeue_dead_prints_what_it_rearmed_and_refuses_a_rerun")
    else {
        return;
    };
    let (tenant, _, _) = f.provisioning_tenant("c32-r4-cli");
    let j1 = dead_distill_job(&mut f, tenant, "RETRY_WAIT");
    let j2 = dead_distill_job(&mut f, tenant, "RETRY_WAIT");
    let j3 = dead_distill_job(&mut f, tenant, "FAILED_OUTPUT_SCHEMA");
    let (t, j1s) = (tenant.to_string(), j1.to_string());
    let requeue = |extra: &[&str]| {
        let mut args = vec!["jobs", "requeue-dead", "--tenant", t.as_str()];
        args.extend(extra);
        f.run_admin(&args)
    };

    let out = requeue(&["--job", &j1s, "--error-class", "RETRY_WAIT"]);
    assert_eq!(
        out.status.code(),
        Some(2),
        "both selectors: {}",
        stdout(&out)
    );
    let out = requeue(&[]);
    assert_eq!(out.status.code(), Some(2), "no selector: {}", stdout(&out));

    let out = requeue(&["--job", &j1s]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let r = receipt(&out);
    assert_eq!(r["outcome"], "requeued");
    assert_eq!(r["requeued"].as_array().map(Vec::len), Some(1), "{r}");
    assert_eq!(uuid_of(&r["requeued"][0], "job_id"), j1);
    assert_eq!(r["requeued"][0]["last_error_class"], "RETRY_WAIT");
    assert_eq!(r["requeued"][0]["attempt_spent"], 3);

    let out = requeue(&["--job", &j1s]);
    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    assert_eq!(receipt(&out)["reason"], "job_not_dead");

    let out = requeue(&["--error-class", "RETRY_WAIT"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let r = receipt(&out);
    let ids: Vec<Uuid> = r["requeued"]
        .as_array()
        .expect("requeued")
        .iter()
        .map(|j| uuid_of(j, "job_id"))
        .collect();
    assert_eq!(ids, vec![j2], "exact class only, DEAD only");

    let states: Vec<(Uuid, String, i32, Option<String>)> = f
        .db()
        .query(
            "SELECT job_id, status, attempt, last_error_class FROM ops.jobs \
             WHERE job_id = ANY($1) ORDER BY created_at",
            &[&vec![j1, j2, j3]],
        )
        .expect("jobs")
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    assert_eq!(
        states,
        vec![
            (j1, "PENDING".into(), 0, Some("RETRY_WAIT".into())),
            (j2, "PENDING".into(), 0, Some("RETRY_WAIT".into())),
            (j3, "DEAD".into(), 3, Some("FAILED_OUTPUT_SCHEMA".into())),
        ]
    );
    assert_eq!(
        f.count(
            "SELECT count(*) FROM ops.outbox o JOIN ops.jobs j \
               ON j.payload ->> 'evidence_id' = o.evidence_id::text \
             WHERE j.status = 'PENDING' AND o.status = 'PENDING'"
        ),
        2,
        "each re-armed job's outbox row is open again"
    );
    assert_eq!(
        f.count(
            "SELECT count(*) FROM control.audit_events WHERE action = 'DISTILL_REQUEUE_DEAD' \
             AND result = 'SUCCESS'"
        ),
        2
    );
    assert_eq!(
        f.count(
            "SELECT count(*) FROM control.audit_events WHERE action = 'DISTILL_REQUEUE_DEAD' \
             AND result = 'DENIED'"
        ),
        1
    );
}

/// ADR-0058 ruling 2026-10-02 20:30 (migration 0200) through the binary: class mode prints, in its
/// receipt, every matching DEAD job it skipped with the reason, and the SUCCESS audit row carries
/// the same list; a class whose every match is skipped still exits 0 (`nothing_requeued`). Fault:
/// the adapter leaves `skipped` out of the audit metadata ⇒ red.
#[test]
fn jobs_requeue_dead_prints_and_audits_the_dead_jobs_it_skipped() {
    let Some(mut f) = fixture("jobs_requeue_dead_prints_and_audits_the_dead_jobs_it_skipped")
    else {
        return;
    };
    let (tenant, _, _) = f.provisioning_tenant("c32-r4-skip");
    let live = dead_distill_job(&mut f, tenant, "FAILED_OUTPUT_SCHEMA");
    let gone = dead_distill_job(&mut f, tenant, "FAILED_OUTPUT_SCHEMA");
    let settled = dead_distill_job(&mut f, tenant, "FAILED_OUTPUT_SCHEMA");
    f.db()
        .batch_execute(&format!(
            "DELETE FROM ops.outbox o USING ops.jobs j \
               WHERE j.job_id = '{gone}' AND o.evidence_id::text = j.payload ->> 'evidence_id'; \
             UPDATE ops.outbox o SET status = 'DONE' FROM ops.jobs j \
               WHERE j.job_id = '{settled}' AND o.evidence_id::text = j.payload ->> 'evidence_id'"
        ))
        .expect("one Evidence gone, one settled");
    let t = tenant.to_string();
    let requeue = |f: &Fixture| {
        f.run_admin(&[
            "jobs",
            "requeue-dead",
            "--tenant",
            t.as_str(),
            "--error-class",
            "FAILED_OUTPUT_SCHEMA",
        ])
    };

    let out = requeue(&f);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let r = receipt(&out);
    assert_eq!(r["outcome"], "requeued", "{r}");
    assert_eq!(uuid_of(&r["requeued"][0], "job_id"), live, "{r}");
    let skipped: Vec<(Uuid, String)> = r["skipped"]
        .as_array()
        .expect("skipped")
        .iter()
        .map(|j| {
            (
                uuid_of(j, "job_id"),
                j["reason"].as_str().unwrap_or("-").to_owned(),
            )
        })
        .collect();
    assert_eq!(
        skipped,
        vec![
            (gone, "evidence_gone".to_owned()),
            (settled, "outbox_settled".to_owned())
        ],
        "{r}"
    );
    let audited: Value = f
        .db()
        .query_one(
            "SELECT metadata FROM control.audit_events WHERE audit_event_id = $1::text::uuid",
            &[&r["audit_event_id"].as_str().expect("audit_event_id")],
        )
        .expect("audit row")
        .get(0);
    assert_eq!(audited["skipped"], r["skipped"], "{audited}");

    let out = requeue(&f);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let r = receipt(&out);
    assert_eq!(r["outcome"], "nothing_requeued", "{r}");
    assert_eq!(r["skipped"].as_array().map(Vec::len), Some(2), "{r}");
}
