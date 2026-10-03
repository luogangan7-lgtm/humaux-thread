//! `xtask::e2e_onboard` — the card-28 acceptance harness: production onboarding on a fresh database, live reads,
//!   live writes, idempotency and the five faults (ADR-0053 D-H).
//! Depends-on: crates=[humaux-adapters, humaux-domain, postgres, serde_json, uuid]; services=[HTTP(gateway), PostgreSQL(owner)
//!   r=[control.api_keys, control.audit_events, control.entitlement_snapshots, control.memberships,
//!   control.private_reasoning_domains, control.quota_windows, control.retrieval_provider_admission_limits,
//!   control.tenants, control.user_emails, control.users, control.workspace_memberships, control.workspaces,
//!   ops.commit_seq_seq, ops.outbox, private.evidence_objects, projection.family_activations,
//!   projection.private_memory_points, projection.stream_checkpoints] w=[projection.stream_log,
//!   projection.tenant_placements] x=[control.ensure_user, control.issue_quota_window, control.onboard_tenant],
//!   Qdrant(*), subprocess(cargo), subprocess(humaux-gateway), subprocess(humaux-maintenance), subprocess(id),
//!   subprocess(xtask), PostgreSQL(role_maintenance)]; env=[CARGO_TARGET_DIR, HUMAUX_GATEWAY_ALLOWED_HOSTS, HUMAUX_GATEWAY_ALLOWED_ORIGINS,
//!   HUMAUX_GATEWAY_BIND_ADDR, HUMAUX_GATEWAY_CALLER_ID, HUMAUX_GATEWAY_CELL_ID,
//!   HUMAUX_GATEWAY_CONFIRM_TOKEN_TTL_SECONDS, HUMAUX_GATEWAY_CONTEXT_MANDATORY_TOKENS,
//!   HUMAUX_GATEWAY_CONTEXT_TOTAL_TOKENS, HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX, HUMAUX_GATEWAY_EMBEDDING_DIMENSION,
//!   HUMAUX_GATEWAY_EMBEDDING_VERSION, HUMAUX_GATEWAY_FINALIZE_TIMEOUT_SECONDS, HUMAUX_GATEWAY_GLOBAL_DENYLIST,
//!   HUMAUX_GATEWAY_GLOBAL_EMERGENCY_ALLOWLIST, HUMAUX_GATEWAY_HANDLER_TIMEOUT_SECONDS,
//!   HUMAUX_GATEWAY_MAX_FORWARDED_HOPS, HUMAUX_GATEWAY_MAX_REQUEST_BODY_BYTES,
//!   HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS, HUMAUX_GATEWAY_PG_DSN, HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS,
//!   HUMAUX_GATEWAY_QDRANT_CIDR,
//!   HUMAUX_GATEWAY_QDRANT_HOST, HUMAUX_GATEWAY_QDRANT_PORT, HUMAUX_GATEWAY_QDRANT_TLS,
//!   HUMAUX_GATEWAY_RATE_CREDENTIAL_CAPACITY, HUMAUX_GATEWAY_RATE_CREDENTIAL_REFILL_PER_SECOND,
//!   HUMAUX_GATEWAY_RATE_OPERATION_CAPACITY, HUMAUX_GATEWAY_RATE_OPERATION_REFILL_PER_SECOND,
//!   HUMAUX_GATEWAY_RATE_PREAUTH_IP_CAPACITY, HUMAUX_GATEWAY_RATE_PREAUTH_IP_REFILL_PER_SECOND,
//!   HUMAUX_GATEWAY_RATE_TENANT_CAPACITY, HUMAUX_GATEWAY_RATE_TENANT_REFILL_PER_SECOND,
//!   HUMAUX_GATEWAY_RATE_USER_CAPACITY, HUMAUX_GATEWAY_RATE_USER_REFILL_PER_SECOND,
//!   HUMAUX_GATEWAY_REMEMBER_DATA_CLASS, HUMAUX_GATEWAY_REMEMBER_DOMAIN, HUMAUX_GATEWAY_REMEMBER_EVENT_KIND,
//!   HUMAUX_GATEWAY_REMEMBER_PROJECTION_KIND, HUMAUX_GATEWAY_REMEMBER_PROJECTION_VERSION,
//!   HUMAUX_GATEWAY_REMEMBER_REASONING_DOMAIN_ID, HUMAUX_GATEWAY_REMEMBER_SCOPE_KIND,
//!   HUMAUX_GATEWAY_REMEMBER_TENANT_ID, HUMAUX_GATEWAY_REMEMBER_TOKEN_TTL_SECONDS,
//!   HUMAUX_GATEWAY_REMEMBER_VISIBILITY_CLASS, HUMAUX_GATEWAY_REMEMBER_WORKSPACE_ID,
//!   HUMAUX_GATEWAY_REPLAY_TTL_SECONDS, HUMAUX_GATEWAY_RESERVATION_TTL_SECONDS,
//!   HUMAUX_GATEWAY_RETRIEVAL_RPC_PERMIT_TTL_SECONDS, HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH,
//!   HUMAUX_GATEWAY_TOKEN_HMAC_KEY, HUMAUX_GATEWAY_TRUSTED_PROXY_CIDRS, HUMAUX_GATEWAY_UNDO_WINDOW_SECONDS,
//!   HUMAUX_MAINTENANCE_CREDENTIAL_PEPPER_HEX, HUMAUX_MAINTENANCE_EMBEDDING_DIMENSION, HUMAUX_MAINTENANCE_PG_DSN,
//!   HUMAUX_MAINTENANCE_PRIVATE_MEMORY_COLLECTION, HUMAUX_MAINTENANCE_QDRANT_CIDR, HUMAUX_MAINTENANCE_QDRANT_HOST,
//!   HUMAUX_MAINTENANCE_QDRANT_PORT, HUMAUX_MINIMAX_DNS_PINS, HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS,
//!   HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID, HUMAUX_PRIVATE_WORKER_CREDENTIALS,
//!   HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_MAX_CALLS, HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS,
//!   HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_IN_FLIGHT,
//!   HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS,
//!   HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS,
//!   HUMAUX_PRIVATE_WORKER_DNS_PINS, HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS,
//!   HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS, HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS,
//!   HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS, HUMAUX_PRIVATE_WORKER_REGIONS, HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH, HUMAUX_RETRIEVAL_WORKER_BACKOFF_BASE_SECS,
//!   HUMAUX_RETRIEVAL_WORKER_BACKOFF_MAX_SECS, HUMAUX_RETRIEVAL_WORKER_BATCH, HUMAUX_RETRIEVAL_WORKER_CALLER,
//!   HUMAUX_RETRIEVAL_WORKER_CELL_ID, HUMAUX_RETRIEVAL_WORKER_DIMENSION,
//!   HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID, HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL,
//!   HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER, HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION,
//!   HUMAUX_RETRIEVAL_WORKER_GATEWAY_UID, HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN,
//!   HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256, HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION,
//!   HUMAUX_RETRIEVAL_WORKER_LEASE_SECS, HUMAUX_RETRIEVAL_WORKER_MAX_ATTEMPTS,
//!   HUMAUX_RETRIEVAL_WORKER_MAX_INPUT_TOKENS, HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION,
//!   HUMAUX_RETRIEVAL_WORKER_PER_TENANT_CAP, HUMAUX_RETRIEVAL_WORKER_PG_DSN,
//!   HUMAUX_RETRIEVAL_WORKER_POLL_INTERVAL_SECS, HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR,
//!   HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST, HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT, HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS,
//!   HUMAUX_RETRIEVAL_WORKER_REGION, HUMAUX_RETRIEVAL_WORKER_RPC_SOCKET_PATH, HUMAUX_TEST_GITLEAKS_BIN,
//!   HUMAUX_TEST_GITLEAKS_SHA256, HUMAUX_TEST_GITLEAKS_VERSION, HUMAUX_TEST_PG_DSN, HUMAUX_TEST_QDRANT_PORT, PATH];
//!   modules=[adapters::byok, adapters::byok::ssrf, domain::ticket_family, xtask::e2e_seed, xtask::migrate, xtask::soak]
//! Called-by: [xtask::main]
//! Invariants: [owns only its database humaux_thread_c28_onboard_<pid>, collection humaux_c28_onboard_<pid> and the
//!   four children it spawned (stopped via their Child handles, never by port or name), all torn down by Drop regardless
//!   of outcome; each step prints PASS|FAIL and the exit is 0 only if every step passed; PostgreSQL/Qdrant/provider down
//!   -> the step that needed it FAILs with the stuck stage named; no key, pepper or DSN is printed]
//! Spec: Baseline §4.2; §16.2; §23.1; §52.3; ADR-0052; ADR-0053; ADR-0059
//!
//! `cargo xtask e2e-onboard` — every step prints `e2e-onboard: <step> PASS|FAIL <detail>` and the
//! command exits 0 only if every step passed. It owns everything it touches and nothing else:
//! the database `humaux_thread_c28_onboard_<pid>` (created from migrations 0001→head in-process,
//! dropped at the end), the Qdrant collection `humaux_c28_onboard_<pid>` on the shared container
//! (deleted at the end), and the four child processes it spawned (stopped through their own
//! `Child` handles — never by port or name). Role DSNs (each its own env variable, including
//! `PRIVATE_WORKER_PG_DSN`; ADR-0059 D-D) are rewritten to host `localhost`, a hostname, so the
//! maintenance binary is exercised without the seed's 127.0.0.1 guard.
//!
//! Secrets: the pepper is minted per run; `DASHSCOPE_API_KEY` / `MINIMAX_API_KEY` come from the
//! environment or, failing that, from the same two files `docs/ops/rehearse.sh` sources (only the
//! one key each is read); both are handed to the child that needs them and printed nowhere. The
//! issued wire key is captured from the maintenance stdout in memory only.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use humaux_adapters::byok::ReasoningCapability;
use postgres::{Client, NoTls};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::e2e_seed::{LaneFlags, seed_lane};
use crate::soak::{mcp_request, parse_http};

const OWNER_DSN: &str = "HUMAUX_TEST_PG_DSN";
const MAINTENANCE_DSN: &str = "HUMAUX_MAINTENANCE_PG_DSN";
const GATEWAY_DSN: &str = "HUMAUX_GATEWAY_PG_DSN";
const RETRIEVAL_DSN: &str = "HUMAUX_RETRIEVAL_WORKER_PG_DSN";
// ADR-0059 D-D: the private worker logs in with its own env DSN (the name its binary reads).
const PRIVATE_WORKER_DSN: &str = "PRIVATE_WORKER_PG_DSN";
/// The rehearsal's own key files (`docs/ops/rehearse.sh`), read only when the environment does
/// not already carry the key.
const DASHSCOPE_KEY_FILE: &str = "/Volumes/data/humaux-thread/.env.local";
const MINIMAX_KEY_FILE: &str = "/Volumes/data/viral-skill-eval/.env";
/// §19 frozen embedding lane (the rehearsal's values).
const EMB_PROVIDER: &str = "dashscope";
const EMB_MODEL: &str = "text-embedding-v4";
const EMB_REV: &str = "2026-08";
const EMB_VER: &str = "text-embedding-v4@2026-08";
const EMB_REGION: &str = "cn-beijing";
const EMB_DIM: u32 = 1024;
const EMB_MAX_TOK: &str = "8192";
/// The BYOK distill lane (card 52 moves it into onboarding; here it is the seed's).
const MM_URL: &str = "https://api.minimaxi.com/v1/chat/completions";
const MM_PROVIDER: &str = "minimax";
const MM_MODEL: &str = "MiniMax-M3";
/// ADR-0060 E5: the catalog row of a tool-capable profile is a new revision label (the 2026-08 row is
/// frozen at {TEXT, STRUCTURED_OUTPUT}); the label is never sent on the wire.
const MM_REV: &str = "caps-REASONING_SPLIT.STRUCTURED_OUTPUT.TEXT.TOOL_CALLS";
/// ADR-0058 R10 / ADR-0060 D-A: what the rehearsal profile declares and the seeded catalog row holds;
/// the worker builds its instance from that route (ADR-0060 D-B).
const PW_CAPABILITIES: [ReasoningCapability; 4] = [
    ReasoningCapability::Text,
    ReasoningCapability::StructuredOutput,
    ReasoningCapability::ToolCalls,
    ReasoningCapability::ReasoningSplit,
];
const MM_REGION: &str = "cn-shanghai";
const ADMIN: [&str; 8] = [
    "--actor",
    "e2e-onboard",
    "--reason",
    "card 28 acceptance",
    "--ticket",
    "C28",
    "--step-up-auth",
    "e2e-local",
];
/// Distinct, digit-free sentinels (a digit run can trip the deterministic secret rules, card 27).
const SENTINELS: [&str; 5] = [
    "Zephyrine the cartographer maps the northern glaciers every spring.",
    "Quillmoor bakery keeps a sourdough starter older than the town hall.",
    "Brannock ferry crosses the estuary only when the tide is slack.",
    "Ostrevale orchard grows a pear variety that ripens after frost.",
    "Tamsworth observatory logs meteor showers from its copper dome.",
];
const SENTINEL_WORDS: [&str; 5] = [
    "Zephyrine",
    "Quillmoor",
    "Brannock",
    "Ostrevale",
    "Tamsworth",
];

struct Report {
    failed: usize,
}

impl Report {
    fn step(&mut self, name: &str, ok: bool, detail: impl AsRef<str>) -> bool {
        println!(
            "e2e-onboard: {name} {} {}",
            if ok { "PASS" } else { "FAIL" },
            detail.as_ref()
        );
        if !ok {
            self.failed += 1;
        }
        ok
    }
}

/// `postgres://user:pass@host:port/db?q` with host `localhost` and the database replaced.
fn rewrite_dsn(dsn: &str, db: &str) -> Option<String> {
    let (scheme, rest) = dsn.split_once("://")?;
    let (userinfo, hostpath) = rest.rsplit_once('@')?;
    let (hostport, _path) = hostpath.split_once('/')?;
    let port = hostport.rsplit_once(':').map_or("5432", |(_, p)| p);
    Some(format!("{scheme}://{userinfo}@localhost:{port}/{db}"))
}

/// One key out of a dotenv file (`KEY=v` or `export KEY=v`, optional quotes); never logged.
fn key_from(name: &str, file: &str) -> Option<String> {
    if let Ok(v) = std::env::var(name)
        && !v.is_empty()
    {
        return Some(v);
    }
    std::fs::read_to_string(file)
        .ok()?
        .lines()
        .find_map(|line| {
            let line = line.trim().trim_start_matches("export ").trim();
            let value = line.strip_prefix(name)?.strip_prefix('=')?;
            Some(value.trim_matches(['"', '\'']).to_owned())
        })
}

fn free_port() -> Option<u16> {
    TcpListener::bind("127.0.0.1:0")
        .ok()?
        .local_addr()
        .ok()
        .map(|a| a.port())
}

fn target_dir() -> PathBuf {
    std::env::var("CARGO_TARGET_DIR").map_or_else(|_| PathBuf::from("target"), PathBuf::from)
}

/// One minimal HTTP/1.0 exchange with the shared Qdrant (collection teardown / point counts).
fn qdrant(method: &str, path: &str) -> (u16, Value) {
    // dep: Qdrant(*) — this run's own collection
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", qdrant_port())) else {
        return (0, Value::Null);
    };
    let _ = write!(
        stream,
        "{method} {path} HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n"
    );
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    parse_http(&raw).map_or((0, Value::Null), |(s, b)| {
        (s, serde_json::from_str(&b).unwrap_or(Value::Null))
    })
}

fn qdrant_port() -> u16 {
    std::env::var("HUMAUX_TEST_QDRANT_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(6333)
}

/// Everything the run owns; `Drop` is the teardown (step 8), run regardless of outcome.
struct Run {
    owner_dsn: String,
    db: String,
    collection: String,
    work: PathBuf,
    children: Vec<(&'static str, Child)>,
}

impl Drop for Run {
    fn drop(&mut self) {
        for (name, child) in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
            println!(
                "e2e-onboard: teardown stopped own child {name} pid {}",
                child.id()
            );
        }
        let dropped = rewrite_dsn(&self.owner_dsn, "postgres")
            .map(|dsn| dsn.replace("@localhost:", "@127.0.0.1:"))
            // dep: PostgreSQL(owner) — drop this run's throwaway database
            .and_then(|dsn| Client::connect(&dsn, NoTls).ok())
            .map(|mut admin| {
                admin
                    .batch_execute(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.db))
                    .is_ok()
            })
            .unwrap_or(false);
        let (status, _) = qdrant("DELETE", &format!("/collections/{}", self.collection));
        println!(
            "e2e-onboard: teardown {} database {} dropped={dropped}, collection {} deleted={}",
            if dropped && (200..300).contains(&status) {
                "PASS"
            } else {
                "FAIL"
            },
            self.db,
            self.collection,
            (200..300).contains(&status)
        );
        println!("e2e-onboard: logs in {}", self.work.display());
    }
}

struct Env {
    owner: String,
    maintenance: String,
    gateway: String,
    retrieval: String,
    private_worker: String,
    pepper: String,
    /// ADR-0059 D-G: the gateway's per-run consistency-token MAC key (hex, 32 bytes), never printed.
    token_hmac_key: String,
    dashscope: String,
    minimax: String,
    gitleaks: [String; 3],
    dns_pins: String,
}

impl Env {
    fn load(db: &str) -> Result<Self, String> {
        let var = |name: &str| std::env::var(name).map_err(|_| format!("missing env {name}"));
        let rewrite =
            |name: &str| var(name).and_then(|d| rewrite_dsn(&d, db).ok_or(format!("bad {name}")));
        Ok(Self {
            owner: rewrite(OWNER_DSN)?,
            maintenance: rewrite(MAINTENANCE_DSN)?,
            private_worker: rewrite(PRIVATE_WORKER_DSN)?,
            gateway: rewrite(GATEWAY_DSN)?,
            retrieval: rewrite(RETRIEVAL_DSN)?,
            pepper: hex_of(Uuid::new_v4().as_bytes()) + &hex_of(Uuid::new_v4().as_bytes()),
            token_hmac_key: hex_of(Uuid::new_v4().as_bytes()) + &hex_of(Uuid::new_v4().as_bytes()),
            dashscope: key_from("DASHSCOPE_API_KEY", DASHSCOPE_KEY_FILE)
                .ok_or("DASHSCOPE_API_KEY unavailable")?,
            minimax: key_from("MINIMAX_API_KEY", MINIMAX_KEY_FILE)
                .ok_or("MINIMAX_API_KEY unavailable")?,
            gitleaks: [
                var("HUMAUX_TEST_GITLEAKS_BIN")?,
                var("HUMAUX_TEST_GITLEAKS_SHA256")?,
                var("HUMAUX_TEST_GITLEAKS_VERSION")?,
            ],
            dns_pins: var("HUMAUX_MINIMAX_DNS_PINS")?,
        })
    }
}

fn hex_of(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl Run {
    fn maintenance(&self, env: &Env, args: &[&str]) -> Command {
        // dep: subprocess(humaux-maintenance) — the release onboarding CLI under test
        let mut command = Command::new(target_dir().join("release/humaux-maintenance"));
        command
            .args(args)
            .env_clear()
            .env("HUMAUX_MAINTENANCE_PG_DSN", &env.maintenance)
            .env("HUMAUX_MAINTENANCE_CREDENTIAL_PEPPER_HEX", &env.pepper)
            .env("HUMAUX_MAINTENANCE_QDRANT_HOST", "127.0.0.1")
            .env("HUMAUX_MAINTENANCE_QDRANT_PORT", qdrant_port().to_string())
            .env("HUMAUX_MAINTENANCE_QDRANT_CIDR", "127.0.0.1/32")
            .env(
                "HUMAUX_MAINTENANCE_EMBEDDING_DIMENSION",
                EMB_DIM.to_string(),
            )
            .env(
                "HUMAUX_MAINTENANCE_PRIVATE_MEMORY_COLLECTION",
                &self.collection,
            );
        command
    }

    fn run_maintenance(&self, env: &Env, args: &[&str]) -> Output {
        let mut all = args.to_vec();
        if !matches!(args.first(), Some(&"status" | &"collection")) {
            all.extend(ADMIN);
        }
        // dep: subprocess(humaux-maintenance) — the release onboarding CLI under test
        self.maintenance(env, &all)
            .output()
            .unwrap_or_else(|e| panic!("spawn humaux-maintenance: {e}"))
    }

    fn spawn(&mut self, name: &'static str, binary: &str, args: &[&str], envs: &[(&str, &str)]) {
        let log = std::fs::File::create(self.work.join(format!("{name}.log"))).ok();
        let err = log.as_ref().and_then(|f| f.try_clone().ok());
        // dep: subprocess(humaux-gateway) — one of the run's own resident children
        let mut command = Command::new(target_dir().join("release").join(binary));
        command
            .args(args)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .envs(envs.iter().copied())
            .stdout(log.map_or_else(Stdio::null, Stdio::from))
            .stderr(err.map_or_else(Stdio::null, Stdio::from));
        match command.spawn() {
            Ok(child) => {
                println!("e2e-onboard: spawned own child {name} pid {}", child.id());
                self.children.push((name, child));
            }
            Err(e) => println!("e2e-onboard: spawn {name} failed: {e}"),
        }
    }

    fn child_alive(&mut self, name: &str) -> bool {
        self.children
            .iter_mut()
            .find(|(n, _)| *n == name)
            .is_some_and(|(_, c)| matches!(c.try_wait(), Ok(None)))
    }
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn receipt(out: &Output) -> Value {
    stdout(out)
        .lines()
        .last()
        .and_then(|l| serde_json::from_str(l).ok())
        .unwrap_or(Value::Null)
}

fn wire(out: &Output) -> Option<String> {
    stdout(out)
        .lines()
        .find_map(|l| l.strip_prefix("Authorization: Bearer ").map(str::to_owned))
}

fn onboard_args(name: &str) -> Vec<String> {
    [
        "onboard",
        "tenant",
        "--name",
        name,
        "--owner-email",
        &format!("owner-{name}@c28.invalid"),
        "--plan-limit",
        "1000",
        "--period-end",
        "2099-01-01T00:00:00Z",
        "--scopes",
        "memory:write,context:read",
        "--key-name",
        "k1",
        "--provider",
        EMB_PROVIDER,
        "--region",
        EMB_REGION,
        "--tenant-tpm",
        "1000000",
        "--tenant-rpm",
        "10000",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

/// One MCP tool call over the soak's request shape.
fn mcp(port: u16, bearer: &str, tool: &str, args: &Value) -> Result<Value, String> {
    let host = format!("127.0.0.1:{port}");
    let request = mcp_request(
        &host,
        "/mcp",
        &format!("http://{host}"),
        tool,
        &args.to_string(),
    )
    .replace("{BEARER}", bearer);
    // dep: HTTP(gateway) — the run's own gateway child
    let mut stream = TcpStream::connect(&host).map_err(|e| format!("connect: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .map_err(|e| e.to_string())?;
    stream
        .write_all(request.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).map_err(|e| e.to_string())?;
    let (status, body) = parse_http(&raw)?;
    if status != 200 {
        return Err(format!("HTTP {status}"));
    }
    serde_json::from_str(&body).map_err(|e| format!("body: {e}"))
}

fn tool_error(response: &Value) -> Option<&str> {
    (response["result"]["isError"] == true)
        .then(|| response["result"]["structuredContent"]["code"].as_str())
        .flatten()
}

fn content(response: &Value) -> &Value {
    &response["result"]["structuredContent"]
}

fn count(client: &mut Client, sql: &str, tenant: Uuid) -> i64 {
    client
        .query_one(sql, &[&tenant])
        .map_or(-1, |row| row.get::<_, i64>(0))
}

/// The onboarding-owned rows of one tenant — the idempotency witness (the gateway and workers
/// write other tables concurrently, so only rows onboarding itself writes are counted).
fn onboarding_counts(client: &mut Client, tenant: Uuid) -> Vec<i64> {
    [
        "SELECT count(*) FROM control.tenants WHERE tenant_id = $1",
        "SELECT count(*) FROM control.memberships WHERE tenant_id = $1",
        "SELECT count(*) FROM control.users u WHERE EXISTS (SELECT 1 FROM control.memberships m WHERE m.tenant_id = $1 AND m.user_id = u.user_id)",
        "SELECT count(*) FROM control.user_emails e WHERE EXISTS (SELECT 1 FROM control.memberships m WHERE m.tenant_id = $1 AND m.user_id = e.user_id)",
        "SELECT count(*) FROM control.workspaces WHERE tenant_id = $1",
        "SELECT count(*) FROM control.workspace_memberships WHERE tenant_id = $1",
        "SELECT count(*) FROM control.private_reasoning_domains WHERE tenant_id = $1",
        "SELECT count(*) FROM control.entitlement_snapshots WHERE tenant_id = $1",
        "SELECT count(*) FROM control.api_keys WHERE tenant_id = $1",
        "SELECT count(*) FROM control.retrieval_provider_admission_limits WHERE tenant_id = $1 OR tenant_id IS NULL",
        "SELECT count(*) FROM control.quota_windows WHERE tenant_id = $1",
        "SELECT count(*) FROM projection.tenant_placements WHERE tenant_id = $1",
        "SELECT count(*) FROM projection.family_activations WHERE tenant_id = $1",
        "SELECT count(*) FROM control.audit_events WHERE tenant_id = $1 AND action IN ('ONBOARD_TENANT','FAMILY_ACTIVATE','APIKEY_ISSUE','PLACEMENT_ENSURE')",
    ]
    .iter()
    .map(|sql| count(client, sql, tenant))
    .collect()
}

/// A PROVISIONING tenant straight through the 0186 doors as role_maintenance (the state a crash
/// between T1 and the activation leaves), with its quota window; returns (tenant, workspace,
/// user).
fn provisioning_tenant(client: &mut Client, name: &str) -> Result<(Uuid, Uuid, Uuid), String> {
    // dep: PostgreSQL(role_maintenance) — the fault fixture calls the 0186 doors as maintenance
    client
        .batch_execute("SET ROLE role_maintenance")
        .map_err(|e| e.to_string())?;
    let result = (|| {
        let mut txn = client.transaction().map_err(|e| e.to_string())?;
        let user: Uuid = txn
            .query_one(
                "SELECT user_id FROM control.ensure_user($1, $1)",
                &[&format!("owner-{name}@c28.invalid")],
            )
            .map_err(|e| e.to_string())?
            .get(0);
        let row = txn
            .query_one(
                "SELECT tenant_id, workspace_id FROM control.onboard_tenant($1, $2, 'default', \
                 'default', 1000, now() - interval '1 second', now() + interval '1 day', $3, $4, \
                 1000000, 10000, ARRAY['private_memory','PRIVATE_MEMORY','v1'])",
                &[&name, &user, &EMB_PROVIDER, &EMB_REGION],
            )
            .map_err(|e| e.to_string())?;
        let (tenant, workspace): (Uuid, Uuid) = (row.get(0), row.get(1));
        txn.query_one(
            "SELECT window_start FROM control.issue_quota_window($1, 'mcp.billable_operations.per_period')",
            &[&tenant],
        )
        .map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
        Ok((tenant, workspace, user))
    })();
    client
        .batch_execute("RESET ROLE")
        .map_err(|e| e.to_string())?;
    result
}

#[allow(clippy::too_many_lines)] // the harness is one ordered script; each step prints its own verdict
pub fn run(_args: &[String]) -> i32 {
    let pid = std::process::id();
    let db = format!("humaux_thread_c28_onboard_{pid}");
    let collection = format!("humaux_c28_onboard_{pid}");
    let work = std::env::temp_dir().join(format!("humaux-c28-onboard-{pid}"));
    let _ = std::fs::create_dir_all(&work);
    let mut report = Report { failed: 0 };

    let env = match Env::load(&db) {
        Ok(env) => env,
        Err(e) => {
            report.step("env", false, e);
            return 1;
        }
    };

    // ---- 0. release binaries ------------------------------------------------------------------
    // dep: subprocess(cargo) — build the four release binaries the run executes
    let built = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "humaux-maintenance",
            "-p",
            "humaux-gateway",
            "-p",
            "humaux-private-worker",
            "-p",
            "humaux-retrieval-worker",
        ])
        .status()
        .is_ok_and(|s| s.success());
    if !report.step(
        "build",
        built,
        "cargo build --release (maintenance, gateway, private-worker, retrieval-worker)",
    ) {
        return 1;
    }

    // ---- 1. fresh database, migrations 0001 -> head, in-process ------------------------------
    let owner_127 = env.owner.replace("@localhost:", "@127.0.0.1:");
    let created = rewrite_dsn(&owner_127, "postgres")
        .map(|d| d.replace("@localhost:", "@127.0.0.1:"))
        // dep: PostgreSQL(owner) — create this run's throwaway database
        .and_then(|d| Client::connect(&d, NoTls).ok())
        .is_some_and(|mut c| c.batch_execute(&format!("CREATE DATABASE {db}")).is_ok());
    let mut run = Run {
        owner_dsn: owner_127.clone(),
        db: db.clone(),
        collection: collection.clone(),
        work: work.clone(),
        children: Vec::new(),
    };
    let migrated = created && crate::migrate::run(&["--dsn".to_owned(), owner_127.clone()]) == 0;
    if !report.step(
        "fresh_db",
        migrated,
        format!("{db} created, migrations 0001->head applied, role DSNs on host localhost"),
    ) {
        return 1;
    }
    // dep: PostgreSQL(owner) — assertions and fault fixtures on the throwaway database
    let Ok(mut owner) = Client::connect(&owner_127, NoTls) else {
        report.step(
            "owner_connect",
            false,
            "cannot connect to the throwaway database",
        );
        return 1;
    };

    // ---- 3. deploy-init + onboard tenant t1 (+ two more for n=3 wall clock) ------------------
    let out = run.run_maintenance(
        &env,
        &[
            "deploy-init",
            "--provider",
            EMB_PROVIDER,
            "--region",
            EMB_REGION,
            "--tpm",
            "100000000",
            "--rpm",
            "100000",
        ],
    );
    report.step(
        "deploy_init",
        out.status.code() == Some(0) && receipt(&out)["outcome"] == "created",
        format!(
            "exit={:?} outcome={}",
            out.status.code(),
            receipt(&out)["outcome"]
        ),
    );

    let started = Instant::now();
    let args = onboard_args("t1");
    let out = run.run_maintenance(&env, &args.iter().map(String::as_str).collect::<Vec<_>>());
    let t1_ms = started.elapsed().as_millis();
    let r = receipt(&out);
    let bearer = wire(&out);
    let tiers: Vec<&str> = r["admission_tiers"]
        .as_array()
        .map(|a| a.iter().filter_map(|t| t["tier"].as_str()).collect())
        .unwrap_or_default();
    let activation = &r["activations"][0];
    let receipt_ok = out.status.code() == Some(0)
        && r["outcome"] == "created"
        && r["tenant_id"].is_string()
        && r["workspace_id"].is_string()
        && r["owner_user_id"].is_string()
        && r["reasoning_domain_id"].is_string()
        && r["api_key"]["fingerprint"]
            .as_str()
            .is_some_and(|f| f.starts_with(r["api_key"]["prefix"].as_str().unwrap_or("?")))
        && ["GLOBAL", "REGION", "TENANT", "PURPOSE"]
            .iter()
            .all(|t| tiers.contains(t))
        && r["placements"][0]["collection_name"] == collection.as_str()
        && r["collection"]["payload_indexes"] == json!(["tenant_id", "subject_ids"])
        && activation["outcome"] == "activated"
        && activation["evidence"] == "VerifiedEmpty"
        && r["lifecycle"] == "READY"
        && bearer.is_some()
        && !stdout(&out)
            .lines()
            .last()
            .unwrap_or_default()
            .contains(bearer.as_deref().unwrap_or("\u{0}"));
    report.step(
        "onboard_tenant_receipt",
        receipt_ok,
        format!(
            "outcome={} tiers={tiers:?} placement={} indexes={} activation={}/{} lifecycle={} fingerprint={} wire_in_json=false wall_ms={t1_ms} probe_ms={} txn_ms={}",
            r["outcome"], r["placements"][0]["collection_name"], r["collection"]["payload_indexes"],
            activation["outcome"], activation["evidence"], r["lifecycle"], r["api_key"]["fingerprint"],
            activation["probe_latency_ms"], activation["activation_txn_ms"]
        ),
    );
    let mut walls = vec![t1_ms];
    let mut probes = vec![activation["probe_latency_ms"].clone()];
    for name in ["m2", "m3"] {
        let started = Instant::now();
        let args = onboard_args(name);
        let out = run.run_maintenance(&env, &args.iter().map(String::as_str).collect::<Vec<_>>());
        walls.push(started.elapsed().as_millis());
        probes.push(receipt(&out)["activations"][0]["probe_latency_ms"].clone());
        if out.status.code() != Some(0) {
            report.step(
                "onboard_wall_clock",
                false,
                format!("{name} exit {:?}", out.status.code()),
            );
        }
    }
    report.step(
        "onboard_wall_clock",
        walls.len() == 3,
        format!("n=3 onboard tenant wall_ms={walls:?} probe_ms={probes:?}"),
    );
    let (Some(bearer), Some(tenant), Some(workspace), Some(user), Some(domain)) = (
        bearer,
        r["tenant_id"].as_str().and_then(|s| s.parse::<Uuid>().ok()),
        r["workspace_id"]
            .as_str()
            .and_then(|s| s.parse::<Uuid>().ok()),
        r["owner_user_id"]
            .as_str()
            .and_then(|s| s.parse::<Uuid>().ok()),
        r["reasoning_domain_id"]
            .as_str()
            .and_then(|s| s.parse::<Uuid>().ok()),
    ) else {
        report.step(
            "onboard_ids",
            false,
            "receipt ids / wire missing — cannot continue",
        );
        return 1;
    };

    // ---- 4. the four resident children ---------------------------------------------------------
    let Some(gw_port) = free_port() else {
        report.step("processes", false, "no free loopback port");
        return 1;
    };
    let sock = PathBuf::from(format!("/tmp/hqc28-{pid}"));
    let _ = std::fs::create_dir_all(&sock);
    let retrieval_sock = sock.join("retrieval.sock").display().to_string();
    let inference_sock = sock.join("inference.sock").display().to_string();
    let uid = users_uid();
    let cell = Uuid::now_v7().to_string();
    let egress = Uuid::now_v7().to_string();
    let q_port = qdrant_port().to_string();
    let [gl_bin, gl_sha, gl_ver] = env.gitleaks.clone();
    let tenant_s = tenant.to_string();
    let workspace_s = workspace.to_string();
    let domain_s = domain.to_string();
    let dim = EMB_DIM.to_string();
    let retrieval_common: Vec<(&str, &str)> = vec![
        ("HUMAUX_RETRIEVAL_WORKER_PG_DSN", env.retrieval.as_str()),
        ("HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER", EMB_PROVIDER),
        ("HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL", EMB_MODEL),
        ("HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION", EMB_REV),
        ("HUMAUX_RETRIEVAL_WORKER_DIMENSION", dim.as_str()),
        ("HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION", EMB_VER),
        ("HUMAUX_RETRIEVAL_WORKER_REGION", EMB_REGION),
        ("HUMAUX_RETRIEVAL_WORKER_MAX_INPUT_TOKENS", EMB_MAX_TOK),
        ("HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST", "127.0.0.1"),
        ("HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT", q_port.as_str()),
        ("HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR", "127.0.0.1/32"),
        ("HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS", "false"),
        ("HUMAUX_RETRIEVAL_WORKER_CELL_ID", cell.as_str()),
        ("HUMAUX_RETRIEVAL_WORKER_CALLER", "retrieval-worker"),
        ("HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN", gl_bin.as_str()),
        ("HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256", gl_sha.as_str()),
        ("HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION", gl_ver.as_str()),
        (
            "HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID",
            egress.as_str(),
        ),
        ("DASHSCOPE_API_KEY", env.dashscope.as_str()),
    ];
    let mut rpc = retrieval_common.clone();
    rpc.extend([
        (
            "HUMAUX_RETRIEVAL_WORKER_RPC_SOCKET_PATH",
            retrieval_sock.as_str(),
        ),
        ("HUMAUX_RETRIEVAL_WORKER_GATEWAY_UID", uid.as_str()),
    ]);
    run.spawn(
        "retrieval-worker-rpc",
        "humaux-retrieval-worker",
        &["--serve-rpc"],
        &rpc,
    );
    let mut runner = retrieval_common.clone();
    runner.extend([
        ("HUMAUX_RETRIEVAL_WORKER_BATCH", "16"),
        ("HUMAUX_RETRIEVAL_WORKER_PER_TENANT_CAP", "8"),
        ("HUMAUX_RETRIEVAL_WORKER_LEASE_SECS", "60"),
        ("HUMAUX_RETRIEVAL_WORKER_POLL_INTERVAL_SECS", "1"),
        ("HUMAUX_RETRIEVAL_WORKER_MAX_ATTEMPTS", "6"),
        ("HUMAUX_RETRIEVAL_WORKER_BACKOFF_BASE_SECS", "30"),
        ("HUMAUX_RETRIEVAL_WORKER_BACKOFF_MAX_SECS", "300"),
    ]);
    run.spawn(
        "projection-runner",
        "humaux-retrieval-worker",
        &["--serve"],
        &runner,
    );
    // BYOK lane (seed), before the private worker starts: ADR-0059 D-I keys the worker's credential
    // map on the lane's credential reference, which exists only once the lane is seeded. An
    // unseeded lane leaves the map explicitly empty (the worker boots; every route parks
    // CREDENTIAL_NOT_MAPPED), and `byok_lane` is already red.
    let lane = seed_lane(
        &mut owner,
        tenant,
        user,
        domain,
        &LaneFlags {
            egress_processor_id: egress.parse().unwrap_or_else(|_| Uuid::nil()),
            region: MM_REGION.to_owned(),
            service_tier: "standard".to_owned(),
            endpoint_ref: MM_URL.to_owned(),
            provider_id: MM_PROVIDER.to_owned(),
            provider_model_id: MM_MODEL.to_owned(),
            model_revision: MM_REV.to_owned(),
            capabilities: PW_CAPABILITIES.to_vec(),
            // ADR-0060 D-J: the one vendor account behind MINIMAX_API_KEY in this run.
            account_ref: "e2e-onboard-minimax".to_owned(),
            // ADR-0060 research amendment 1: the rehearsal MiniMax profile's request field
            // (rehearse.sh `reasoning register --request-extras`); the adapter no longer writes it
            // for REASONING_SPLIT, so a profile without it is a different request.
            request_extras: serde_json::Map::from_iter([(
                "reasoning_split".to_owned(),
                serde_json::Value::Bool(true),
            )]),
        },
    );
    report.step(
        "byok_lane",
        lane.is_ok(),
        lane.as_ref().map_or_else(Clone::clone, |_| {
            "e2e_seed::seed_lane for t1 (card 52 moves it into onboarding)".to_owned()
        }),
    );
    let credentials = lane.as_ref().map_or_else(
        |_| String::new(),
        |l| format!("{}=MINIMAX_API_KEY", l.credential_id),
    );
    // ADR-0060 D-C / D-L: the worker's deny-only lists name this lane's recipient with the host its
    // endpoint dials, and its region; provider, model, endpoint and capabilities come from the route.
    let recipients = humaux_adapters::byok::ssrf::https_host(MM_URL)
        .map(|host| format!("{egress}={host}"))
        .unwrap_or_default();
    let distill: Vec<(&str, &str)> = vec![
        ("PRIVATE_WORKER_PG_DSN", env.private_worker.as_str()),
        ("HUMAUX_PRIVATE_WORKER_CREDENTIALS", credentials.as_str()),
        ("MINIMAX_API_KEY", env.minimax.as_str()),
        ("HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS", "120"),
        ("HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS", "60"),
        ("HUMAUX_PRIVATE_WORKER_DNS_PINS", env.dns_pins.as_str()),
        (
            "HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH",
            inference_sock.as_str(),
        ),
        ("HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID", uid.as_str()),
        ("HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS", "86400"),
        // ADR-0058 D-K: hard deadline = 2 x (HTTP 120 + lease 30).
        ("HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS", "30"),
        ("HUMAUX_PRIVATE_WORKER_DISTILL_IN_FLIGHT", "4"),
        ("HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS", "300"),
        ("HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS", "600"),
        ("HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS", "5"),
        ("HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS", "2"),
        // ADR-0058 D-T: the rehearsal's §72.3 tenant distill budget.
        ("HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS", "60"),
        ("HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_MAX_CALLS", "120"),
        (
            "HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS",
            recipients.as_str(),
        ),
        ("HUMAUX_PRIVATE_WORKER_REGIONS", MM_REGION),
        // Ruling E3: traffic renews the seed's 30-minute attestation once less than 15 min is left.
        ("HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS", "1800"),
    ];
    run.spawn(
        "private-worker-distill",
        "humaux-private-worker",
        &["--distill-serve"],
        &distill,
    );
    let bind = format!("127.0.0.1:{gw_port}");
    let origin = format!("http://{bind}");
    let family = humaux_domain::ticket_family::TicketFamily::PrivateMemory;
    let gateway: Vec<(&str, &str)> = vec![
        ("HUMAUX_GATEWAY_PG_DSN", env.gateway.as_str()),
        ("HUMAUX_GATEWAY_BIND_ADDR", bind.as_str()),
        ("HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX", env.pepper.as_str()),
        ("HUMAUX_GATEWAY_TOKEN_HMAC_KEY", env.token_hmac_key.as_str()),
        ("HUMAUX_GATEWAY_ALLOWED_HOSTS", bind.as_str()),
        ("HUMAUX_GATEWAY_ALLOWED_ORIGINS", origin.as_str()),
        ("HUMAUX_GATEWAY_MAX_REQUEST_BODY_BYTES", "1048576"),
        ("HUMAUX_GATEWAY_TRUSTED_PROXY_CIDRS", ""),
        ("HUMAUX_GATEWAY_MAX_FORWARDED_HOPS", "1"),
        ("HUMAUX_GATEWAY_GLOBAL_DENYLIST", ""),
        ("HUMAUX_GATEWAY_GLOBAL_EMERGENCY_ALLOWLIST", ""),
        ("HUMAUX_GATEWAY_RESERVATION_TTL_SECONDS", "30"),
        ("HUMAUX_GATEWAY_HANDLER_TIMEOUT_SECONDS", "20"),
        ("HUMAUX_GATEWAY_FINALIZE_TIMEOUT_SECONDS", "5"),
        ("HUMAUX_GATEWAY_REPLAY_TTL_SECONDS", "60"),
        ("HUMAUX_GATEWAY_CONFIRM_TOKEN_TTL_SECONDS", "300"),
        ("HUMAUX_GATEWAY_UNDO_WINDOW_SECONDS", "86400"),
        ("HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS", "60"),
        ("HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS", "21600"),
        ("HUMAUX_GATEWAY_REMEMBER_TENANT_ID", tenant_s.as_str()),
        ("HUMAUX_GATEWAY_REMEMBER_WORKSPACE_ID", workspace_s.as_str()),
        ("HUMAUX_GATEWAY_REMEMBER_SCOPE_KIND", "workspace"),
        ("HUMAUX_GATEWAY_REMEMBER_DOMAIN", family.domain()),
        (
            "HUMAUX_GATEWAY_REMEMBER_PROJECTION_KIND",
            family.projection_kind(),
        ),
        (
            "HUMAUX_GATEWAY_REMEMBER_PROJECTION_VERSION",
            family.projection_version(),
        ),
        (
            "HUMAUX_GATEWAY_REMEMBER_REASONING_DOMAIN_ID",
            domain_s.as_str(),
        ),
        ("HUMAUX_GATEWAY_REMEMBER_TOKEN_TTL_SECONDS", "60"),
        ("HUMAUX_GATEWAY_REMEMBER_DATA_CLASS", "INTERNAL"),
        (
            "HUMAUX_GATEWAY_REMEMBER_VISIBILITY_CLASS",
            "WORKSPACE_SHARED",
        ),
        ("HUMAUX_GATEWAY_REMEMBER_EVENT_KIND", "USER_MESSAGE"),
        ("HUMAUX_GATEWAY_CONTEXT_TOTAL_TOKENS", "2048"),
        ("HUMAUX_GATEWAY_CONTEXT_MANDATORY_TOKENS", "1024"),
        (
            "HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH",
            retrieval_sock.as_str(),
        ),
        ("HUMAUX_GATEWAY_RETRIEVAL_RPC_PERMIT_TTL_SECONDS", "60"),
        ("HUMAUX_GATEWAY_EMBEDDING_DIMENSION", dim.as_str()),
        ("HUMAUX_GATEWAY_EMBEDDING_VERSION", EMB_VER),
        ("HUMAUX_GATEWAY_QDRANT_HOST", "127.0.0.1"),
        ("HUMAUX_GATEWAY_QDRANT_PORT", q_port.as_str()),
        ("HUMAUX_GATEWAY_QDRANT_CIDR", "127.0.0.1/32"),
        ("HUMAUX_GATEWAY_QDRANT_TLS", "false"),
        ("HUMAUX_GATEWAY_CELL_ID", cell.as_str()),
        ("HUMAUX_GATEWAY_CALLER_ID", "gateway"),
        ("HUMAUX_GATEWAY_RATE_PREAUTH_IP_CAPACITY", "100"),
        ("HUMAUX_GATEWAY_RATE_PREAUTH_IP_REFILL_PER_SECOND", "100"),
        ("HUMAUX_GATEWAY_RATE_CREDENTIAL_CAPACITY", "100"),
        ("HUMAUX_GATEWAY_RATE_CREDENTIAL_REFILL_PER_SECOND", "100"),
        ("HUMAUX_GATEWAY_RATE_USER_CAPACITY", "100"),
        ("HUMAUX_GATEWAY_RATE_USER_REFILL_PER_SECOND", "100"),
        ("HUMAUX_GATEWAY_RATE_TENANT_CAPACITY", "100"),
        ("HUMAUX_GATEWAY_RATE_TENANT_REFILL_PER_SECOND", "100"),
        ("HUMAUX_GATEWAY_RATE_OPERATION_CAPACITY", "100"),
        ("HUMAUX_GATEWAY_RATE_OPERATION_REFILL_PER_SECOND", "100"),
    ];
    run.spawn("gateway", "humaux-gateway", &[], &gateway);
    let ready = wait_until(Duration::from_secs(90), || {
        Path::new(&retrieval_sock).exists() && http_get(gw_port, "/readyz") == Some(200)
    });
    let alive = [
        "retrieval-worker-rpc",
        "projection-runner",
        "private-worker-distill",
        "gateway",
    ]
    .iter()
    .all(|name| run.child_alive(name));
    report.step(
        "processes",
        ready && alive,
        format!("gateway :{gw_port} readyz={ready}, four own children alive={alive}"),
    );

    // ---- 4b. immediately: the four read routes on the empty READY workspace ------------------
    let ws = json!(workspace);
    let calls = [
        (
            "memory.get",
            "memory",
            json!({"action":"get","memory_id":Uuid::now_v7(),"workspace_id":ws}),
        ),
        (
            "memory.enumerate",
            "memory",
            json!({"action":"enumerate","workspace_id":ws,"limit":100}),
        ),
        (
            "recall.search",
            "recall",
            json!({"query":"what is known about glaciers","workspace_id":ws,"mode":"semantic"}),
        ),
        ("context.assemble", "context", json!({"workspace_id":ws})),
    ];
    let mut dependency_unavailable = 0;
    for (label, tool, args) in calls {
        let response = mcp(gw_port, &bearer, tool, &args);
        let (ok, detail) = match &response {
            Err(e) => (false, e.clone()),
            Ok(v) => {
                if tool_error(v) == Some("DEPENDENCY_UNAVAILABLE") {
                    dependency_unavailable += 1;
                }
                let c = content(v);
                match label {
                    "memory.get" => (
                        tool_error(v) == Some("NOT_FOUND"),
                        format!("code={:?}", tool_error(v)),
                    ),
                    "memory.enumerate" => (
                        c["content"]["completeness"]["class"] == "exact"
                            && c["content"]["completeness"]["exact"]["total"] == 0,
                        format!(
                            "class={} total={}",
                            c["content"]["completeness"]["class"],
                            c["content"]["completeness"]["exact"]["total"]
                        ),
                    ),
                    "recall.search" => (
                        tool_error(v).is_none()
                            && c["items"] == json!([])
                            && c["pipeline"]["projection"]["current"] == true,
                        format!(
                            "items={} current={} class={}",
                            c["items"].as_array().map_or(0, Vec::len),
                            c["pipeline"]["projection"]["current"],
                            c["completeness"]["class"]
                        ),
                    ),
                    _ => (
                        tool_error(v).is_none() && c["content"]["items"] == json!([]),
                        format!(
                            "items={} class={}",
                            c["content"]["items"].as_array().map_or(0, Vec::len),
                            c["content"]["completeness"]["class"]
                        ),
                    ),
                }
            }
        };
        report.step(&format!("read_{label}"), ok, detail);
    }
    report.step(
        "reads_no_dependency_unavailable",
        dependency_unavailable == 0,
        format!("DEPENDENCY_UNAVAILABLE count={dependency_unavailable}"),
    );

    // ---- 5. 5 remember.put, recall returns all 5 with no operator action ----
    let mut put_ok = 0;
    for sentinel in SENTINELS {
        let response = mcp(
            gw_port,
            &bearer,
            "remember",
            &json!({"operation":"put","content":sentinel,"idempotency_key":Uuid::now_v7(),"workspace_id":ws}),
        );
        if response
            .as_ref()
            .is_ok_and(|v| tool_error(v).is_none() && content(v)["evidence_id"].is_string())
        {
            put_ok += 1;
        }
    }
    report.step(
        "remember_put_x5",
        put_ok == 5,
        format!("{put_ok}/5 accepted"),
    );
    let put_started = Instant::now();
    let mut found = [false; 5];
    let deadline = Duration::from_secs(900);
    while put_started.elapsed() < deadline && !found.iter().all(|f| *f) {
        for (i, sentinel) in SENTINELS.iter().enumerate() {
            if found[i] {
                continue;
            }
            if let Ok(v) = mcp(
                gw_port,
                &bearer,
                "recall",
                &json!({"query":sentinel,"workspace_id":ws,"mode":"semantic"}),
            ) {
                found[i] = content(&v)["items"].as_array().is_some_and(|items| {
                    items.iter().any(|item| {
                        item["kind"] == "memory"
                            && item["content"].to_string().contains(SENTINEL_WORDS[i])
                    })
                });
            }
        }
        if !found.iter().all(|f| *f) {
            std::thread::sleep(Duration::from_secs(5));
        }
    }
    let stages = pipeline_stage(&mut owner, tenant);
    report.step(
        "recall_all_5",
        found.iter().all(|f| *f),
        format!(
            "{}/5 recalled after {:?} (bounded {deadline:?}); pipeline {stages}",
            found.iter().filter(|f| **f).count(),
            put_started.elapsed()
        ),
    );
    let activations = count(
        &mut owner,
        "SELECT count(*) FROM projection.family_activations WHERE tenant_id = $1",
        tenant,
    );
    let serving = count(
        &mut owner,
        "SELECT count(*) FROM projection.stream_checkpoints WHERE tenant_id = $1 AND serving AND projection_version = 'v1'",
        tenant,
    );
    report.step("no_projection_serve", activations == 1 && serving == 1, format!("family_activations={activations} serving_v1={serving}; this run never invokes xtask projection-serve"));

    // ---- 6. idempotent re-onboard -------------------------------------------------------------
    let before = onboarding_counts(&mut owner, tenant);
    let args = onboard_args("t1");
    let out = run.run_maintenance(&env, &args.iter().map(String::as_str).collect::<Vec<_>>());
    let after = onboarding_counts(&mut owner, tenant);
    report.step(
        "reonboard_existing",
        out.status.code() == Some(0)
            && receipt(&out)["outcome"] == "existing"
            && wire(&out).is_none()
            && before == after,
        format!(
            "outcome={} wire_printed={} counts_unchanged={} ({before:?})",
            receipt(&out)["outcome"],
            wire(&out).is_some(),
            before == after
        ),
    );

    // ---- 7. faults ----------------------------------------------------------------------------
    // (a) PROVISIONING workspace with one stream_log row -> activate refused not_empty.
    let fault_a = provisioning_tenant(&mut owner, "fa").and_then(|(t, w, _)| {
        let out = run.run_maintenance(&env, &["placement", "ensure", "--tenant", &t.to_string()]);
        if out.status.code() != Some(0) {
            return Err(format!("placement exit {:?}", out.status.code()));
        }
        owner
            .execute(
                "INSERT INTO projection.stream_log (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, stream_seq, commit_seq) \
                 VALUES ($1, 'workspace', $2, 'private_memory', 'PRIVATE_MEMORY', 'v1', 1, nextval('ops.commit_seq_seq'))",
                &[&t, &w],
            )
            .map_err(|e| e.to_string())?;
        Ok(run.run_maintenance(&env, &["activate", "--tenant", &t.to_string(), "--workspace", &w.to_string()]))
    });
    report.step(
        "fault_a_not_empty",
        fault_a
            .as_ref()
            .is_ok_and(|o| o.status.code() == Some(3) && receipt(o)["reason"] == "not_empty"),
        fault_a.as_ref().map_or_else(Clone::clone, |o| {
            format!("exit={:?} reason={}", o.status.code(), receipt(o)["reason"])
        }),
    );
    // (b) placement row deleted -> activate refused placement_missing.
    let fault_b = provisioning_tenant(&mut owner, "fb").and_then(|(t, w, _)| {
        run.run_maintenance(&env, &["placement", "ensure", "--tenant", &t.to_string()]);
        owner
            .execute(
                "DELETE FROM projection.tenant_placements WHERE tenant_id = $1",
                &[&t],
            )
            .map_err(|e| e.to_string())?;
        Ok(run.run_maintenance(
            &env,
            &[
                "activate",
                "--tenant",
                &t.to_string(),
                "--workspace",
                &w.to_string(),
            ],
        ))
    });
    report.step(
        "fault_b_placement_missing",
        fault_b.as_ref().is_ok_and(|o| {
            o.status.code() == Some(3) && receipt(o)["reason"] == "placement_missing"
        }),
        fault_b.as_ref().map_or_else(Clone::clone, |o| {
            format!("exit={:?} reason={}", o.status.code(), receipt(o)["reason"])
        }),
    );
    // (c) remember.put on a PROVISIONING workspace -> tool error CONFLICT, 0 evidence rows.
    let fault_c = provisioning_tenant(&mut owner, "fc").and_then(|(t, w, u)| {
        let out = run.run_maintenance(&env, &["apikey", "issue", "--tenant", &t.to_string(), "--user", &u.to_string(), "--workspace", &w.to_string(), "--scopes", "memory:write,context:read", "--key-name", "fc"]);
        let key = wire(&out).ok_or(format!("apikey issue exit {:?}", out.status.code()))?;
        let response = mcp(gw_port, &key, "remember", &json!({"operation":"put","content":"written while provisioning","idempotency_key":Uuid::now_v7(),"workspace_id":w}))?;
        let evidence = count(&mut owner, "SELECT count(*) FROM private.evidence_objects WHERE tenant_id = $1", t);
        Ok((tool_error(&response).map(str::to_owned), evidence))
    });
    report.step(
        "fault_c_provisioning_write_conflict",
        fault_c
            .as_ref()
            .is_ok_and(|(code, evidence)| code.as_deref() == Some("CONFLICT") && *evidence == 0),
        format!("{fault_c:?}"),
    );
    // (d) two concurrent onboard tenant --name t2 -> exactly one tenant row and one api key.
    let args = onboard_args("t2");
    let spawn = |run: &Run| {
        let mut all: Vec<&str> = args.iter().map(String::as_str).collect();
        all.extend(ADMIN);
        run.maintenance(&env, &all)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
    };
    let (first, second) = (spawn(&run), spawn(&run));
    let outs: Vec<Output> = [first, second]
        .into_iter()
        .filter_map(|c| c.ok()?.wait_with_output().ok())
        .collect();
    let tenants_t2 = owner
        .query_one(
            "SELECT count(*) FROM control.tenants WHERE onboarding_name = 't2'",
            &[],
        )
        .map_or(-1, |r| r.get::<_, i64>(0));
    let keys_t2 = owner
        .query_one("SELECT count(*) FROM control.api_keys k JOIN control.tenants t USING (tenant_id) WHERE t.onboarding_name = 't2'", &[])
        .map_or(-1, |r| r.get::<_, i64>(0));
    report.step(
        "fault_d_concurrent_onboard",
        outs.len() == 2
            && outs.iter().all(|o| o.status.code() == Some(0))
            && tenants_t2 == 1
            && keys_t2 == 1,
        format!(
            "exits={:?} tenants={tenants_t2} api_keys={keys_t2}",
            outs.iter().map(|o| o.status.code()).collect::<Vec<_>>()
        ),
    );
    // (e) e2e-seed against a `localhost` DSN is still refused (its 127.0.0.1 guard).
    // dep: subprocess(xtask) — this binary's own e2e-seed subcommand under a hostname DSN
    let seed = std::env::current_exe().ok().and_then(|exe| {
        Command::new(exe)
            .args(["e2e-seed", "--pepper-hex", "00", "--scopes", "memory:write"])
            .env(OWNER_DSN, &env.owner)
            .output()
            .ok()
    });
    report.step(
        "fault_e_seed_guard",
        seed.as_ref().is_some_and(|o| {
            o.status.code() == Some(1) && String::from_utf8_lossy(&o.stderr).contains("refusing")
        }),
        seed.as_ref().map_or_else(
            || "spawn failed".to_owned(),
            |o| {
                format!(
                    "exit={:?} {}",
                    o.status.code(),
                    String::from_utf8_lossy(&o.stderr).trim()
                )
            },
        ),
    );

    // ---- 8. teardown (Drop) and verdict -------------------------------------------------------
    drop(owner);
    let verdict = report.failed;
    drop(run);
    let _ = std::fs::remove_dir_all(&sock);
    println!("e2e-onboard: VERDICT {} failed step(s)", verdict);
    i32::from(verdict != 0)
}

/// Where a still-missing sentinel is stuck: distill (outbox), projection ticket, or point.
fn pipeline_stage(client: &mut Client, tenant: Uuid) -> String {
    let q = |client: &mut Client, sql: &str| count(client, sql, tenant);
    format!(
        "outbox_pending={} tickets_issued={} tickets_done={} points={}",
        q(
            client,
            "SELECT count(*) FROM ops.outbox WHERE tenant_id = $1 AND status IN ('PENDING','PROCESSING')"
        ),
        q(
            client,
            "SELECT count(*) FROM projection.stream_log WHERE tenant_id = $1 AND state = 'ISSUED'"
        ),
        q(
            client,
            "SELECT count(*) FROM projection.stream_log WHERE tenant_id = $1 AND state = 'DONE'"
        ),
        q(
            client,
            "SELECT count(*) FROM projection.private_memory_points WHERE tenant_id = $1"
        ),
    )
}

fn users_uid() -> String {
    // dep: subprocess(id) — this process's own uid for the UDS peer checks
    Command::new("id")
        .arg("-u")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default()
}

fn wait_until(limit: Duration, mut ready: impl FnMut() -> bool) -> bool {
    let started = Instant::now();
    while started.elapsed() < limit {
        if ready() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    false
}

fn http_get(port: u16, path: &str) -> Option<u16> {
    // dep: HTTP(gateway) — the run's own gateway readiness probe
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).ok()?;
    parse_http(&raw).ok().map(|(s, _)| s)
}

#[cfg(test)]
mod tests {
    use super::{key_from, rewrite_dsn};

    #[test]
    fn dsns_are_rewritten_to_a_hostname_and_the_run_database() {
        assert_eq!(
            rewrite_dsn("postgres://u:p@127.0.0.1:54329/humaux_thread_dev", "x").as_deref(),
            Some("postgres://u:p@localhost:54329/x")
        );
        assert_eq!(rewrite_dsn("not a dsn", "x"), None);
    }

    #[test]
    fn a_key_file_yields_only_the_named_key() {
        let dir = std::env::temp_dir().join(format!("c28-keyfile-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join(".env");
        std::fs::write(&file, "export OTHER=1\nexport C28_TEST_KEY=\"abc\"\n").unwrap();
        assert_eq!(
            key_from("C28_TEST_KEY", file.to_str().unwrap()).as_deref(),
            Some("abc")
        );
        assert_eq!(key_from("C28_MISSING_KEY", file.to_str().unwrap()), None);
        let _ = std::fs::remove_dir_all(dir);
    }
}
