//! `xtask::soak` — endurance + crash-recovery harness driving continuous load against the four-process deployment.
//! Depends-on: crates=[humaux-domain, humaux-projection, postgres, serde_json, tokio, uuid];
//!   services=[PostgreSQL(role_maintenance) r=[ops.jobs, ops.outbox, projection.private_memory_points,
//!   projection.processing_gaps, projection.stream_checkpoints, projection.stream_log], Qdrant(*), subprocess(ps),
//!   subprocess(sh), HTTP(gateway)]; env=[CARGO_MANIFEST_DIR, HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_SOAK_TEST_BEARER];
//!   modules=[domain::ids, projection::serving, xtask::switch_visible]
//! Called-by: [xtask::e2e_onboard, xtask::main]
//! Invariants: [continuous concurrent load against the real four-process deployment while a chaos hook kills/restarts a worker; every read is asserted live against the database]
//! Spec: Baseline §15.1; §15.3; §15.5; §31; §61; §6.1; ADR-0037; ADR-0050; ADR-0052; ADR-0055
//!
//! `cargo xtask soak` — the endurance + crash-recovery harness (card 16).
//!
//! Every correctness claim before this card rests on single-shot tests. This subcommand drives
//! **continuous concurrent load** against the real four-process deployment (the one
//! `docs/ops/supervision.md` describes) — N agent sessions per tenant across at least two
//! tenants, doing `remember.put` → `recall.search` → `memory.enumerate` → `memory.get` for a
//! stated duration — while a chaos hook periodically kills and restarts a worker, and asserts
//! continuously against the live database:
//!
//! * §15.1 the per-stream dense ledger has no gap and nothing is left in flight (a ticket that
//!   is neither settled nor in flight IS lost; `SKIPPED_BY_POLICY` is **settled**, not lost);
//! * §15.3 `projection.stream_checkpoints` watermarks never go backwards and never stall;
//! * §15.5 read-your-writes: a `consistency_token` this run minted is never rejected, and a
//!   settled write is visible to a recall carrying its own token;
//! * ADR-0052: after the drain, no `projection.stream_log` ticket still holds a live `--serve`
//!   lease (`no_live_ticket_lease_after_drain`; the rotation kills and restarts the runner);
//! * §31/§61 leases: after the drain, no `ops.jobs` row is `PROCESSING` with a *live* lease —
//!   a `kill -9` mid-pass leaves the row leased, and only lease expiry may free it, so a live
//!   lease surviving `drain > LEASE_SECS` means a worker is wedged, not crashed;
//! * §6.1 isolation: no tenant's MCP response ever contains another tenant's sentinel or id;
//! * ADR-0037 probes (the gateway's `GET /livez`, `<worker> --readyz`) stay green throughout — polled
//!   as the real probes through `--probe-cmd`, never re-implemented here. The gateway is graded on
//!   `/livez` since ADR-0061 D-F (E15): its `/readyz` is dependency-truthful, so the chaos kill of the
//!   retrieval worker correctly turns it 503 for that window;
//! * every process the launcher started is actually alive (ADR-0050 D-I, audit TH-4): each
//!   observation runs ONE `ps -axo pid=,rss=,comm=` and looks up every `--watch-pidfile`'s current
//!   pid. Absent inside `[chaos_start, chaos_start + --chaos-grace-secs]` is an *expected* absence
//!   (reported, not a failure); any other absence is a probe failure. A `ps` that cannot run,
//!   exits non-zero or returns an empty table is an assertion failure (`ps_observed`), never an
//!   RSS of 0;
//! * per-operation failure rate at most `--max-op-failure-rate` (`op_failure_rate`);
//! * bounded RSS (only real readings are scored) and bounded PostgreSQL connection count.
//!
//! §78.1: **no literal thresholds and no defaults.** Every duration, ceiling and endpoint is a
//! required flag; a soak whose thresholds were baked in would move with the code it grades.
//! Every number in the report carries its `n` and its `unit` for the same reason.
//!
//! Process ownership stays with the script that started the processes: `--chaos-cmd` and
//! `--probe-cmd` are shell commands, and `--watch-pidfile` names the pidfiles the launcher (and
//! its chaos steps) keep current, so the harness never takes PIDs as arguments or the workers' env
//! (which carries the BYOK key material). Nothing here reads or prints a credential: the bearer
//! for each tenant is named by an env var and stays in process memory.
//!
//! The gateway is dialed over loopback with the standard library's `TcpStream` — the §83.4
//! raw-client rule binds `reqwest`/`hyper` construction in the product's egress lane; this is a
//! test driver on 127.0.0.1 and adding an HTTP dependency to `xtask` would itself trip the
//! §83.4 manifest gate.
//!
//! depends-on: the gateway (loopback HTTP), Postgres as role_maintenance
//! (`HUMAUX_MAINTENANCE_PG_DSN`), Qdrant (visible counts), `ps`, the launcher's pidfiles.
//! called-by: `cargo xtask soak` from the rehearsal launcher (`rehearse_v2.sh`, TW).
//! spec: §15.1/§15.3/§15.5, §16.2, §31/§61, §6.1, ADR-0037, ADR-0038 (vacuous = FAIL), ADR-0050 D-I.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use humaux_domain::ids::{TenantId, WorkspaceId};
use humaux_projection::serving::{
    ActivationEvidence, ContinuationVerdict, StreamFamily, SwitchCriteria, evaluate_switch,
};
use postgres::{Client, NoTls};
use uuid::Uuid;

use crate::switch_visible::{Candidate, VisibleFace, read_candidate_facts, visible_pair};

// ---------------------------------------------------------------------------------------------
// Configuration (§78.1: required flags only, no defaults)
// ---------------------------------------------------------------------------------------------

const USAGE: &str = "usage: cargo xtask soak \
--gateway-url http://HOST:PORT/mcp \
--tenant <tenant_uuid>:<workspace_uuid>:<BEARER_ENV_VAR> (repeat, >= 2) \
--sessions-per-tenant <n> --duration-secs <n> --drain-secs <n> --think-ms <n> \
--probe-every-secs <n> --probe-cmd <sh> (repeat) \
--watch-pidfile <name>=<path> (repeat, >= 1) \
[--chaos-every-secs <n> --chaos-grace-secs <n> --chaos-cmd <sh> (repeat)] \
--lease-secs <n> --max-rss-mib <n> --max-db-connections <n> --max-op-failure-rate <0..1> \
--report <path> \
[--qdrant-host 127.0.0.1] [--qdrant-port 6333]";

/// One tenant lane. `bearer` is read from the env var the flag names and is never printed,
/// logged or written to the report.
pub struct TenantLane {
    tenant_id: Uuid,
    workspace_id: Uuid,
    bearer: String,
    /// Unique per lane, embedded in every `remember.put` body: the cross-tenant witness looks
    /// for *another* lane's sentinel inside this lane's responses.
    sentinel: String,
}

pub struct Config {
    host_port: String,
    path: String,
    origin: String,
    tenants: Vec<TenantLane>,
    sessions_per_tenant: usize,
    duration: Duration,
    drain: Duration,
    think: Duration,
    probe_every: Duration,
    probe_cmds: Vec<String>,
    /// `(name, pidfile)` for every process the launcher started (ADR-0050 D-I).
    watch_pidfiles: Vec<(String, PathBuf)>,
    chaos_every: Option<Duration>,
    /// How long after a chaos step starts an absent watched process is expected.
    chaos_grace: Option<Duration>,
    chaos_cmds: Vec<String>,
    lease_secs: u64,
    max_rss_mib: u64,
    max_db_connections: i64,
    /// Ceiling on any one operation's `failed / n` (fraction, 0..=1).
    max_op_failure_rate: f64,
    report_path: String,
    /// §23.1② visible counts for the §16.2 promotion report (see [`promote_rejections`]) are
    /// taken live against this Qdrant — the same local endpoint every other lane of the
    /// rehearsal dials.
    qdrant_host: String,
    qdrant_port: u16,
}

fn values(args: &[String], flag: &str) -> Vec<String> {
    args.iter()
        .enumerate()
        .filter(|(_, a)| a.as_str() == flag)
        .filter_map(|(i, _)| args.get(i + 1).cloned())
        .collect()
}

fn one(args: &[String], flag: &str) -> Result<String, String> {
    let found = values(args, flag);
    match found.len() {
        1 => Ok(found[0].clone()),
        0 => Err(format!(
            "missing required flag {flag} (§78.1: no default)\n{USAGE}"
        )),
        n => Err(format!("{flag} given {n} times, expected once")),
    }
}

fn num(args: &[String], flag: &str) -> Result<u64, String> {
    one(args, flag)?
        .parse()
        .map_err(|e| format!("{flag} must be a non-negative integer: {e}"))
}

fn parse_lane(raw: &str) -> Result<TenantLane, String> {
    let parts: Vec<&str> = raw.split(':').collect();
    let [tenant, workspace, key_env] = parts.as_slice() else {
        return Err(format!(
            "--tenant {raw:?}: expected <tenant_uuid>:<workspace_uuid>:<BEARER_ENV_VAR>"
        ));
    };
    let tenant_id: Uuid = tenant
        .parse()
        .map_err(|e| format!("--tenant: {tenant:?} is not a uuid: {e}"))?;
    let workspace_id: Uuid = workspace
        .parse()
        .map_err(|e| format!("--tenant: {workspace:?} is not a uuid: {e}"))?;
    // The value never leaves this process; only the variable NAME may appear in diagnostics.
    let bearer = std::env::var(key_env)
        .map_err(|_| format!("--tenant: ${key_env} is not set (it carries the bearer)"))?;
    if bearer.trim().is_empty() {
        return Err(format!("--tenant: ${key_env} is empty"));
    }
    Ok(TenantLane {
        tenant_id,
        workspace_id,
        bearer,
        sentinel: format!(
            "soak-sentinel-{}",
            digit_free(&tenant_id.simple().to_string()[..12])
        ),
    })
}

/// Hex with its digits spelled as letters (`0`→`g` … `9`→`p`): unique like the hex, but it can
/// never hold the 7-digit run the local scanner's deterministic phone rule rejects. Soak content
/// is distilled by a live model that may copy the marker into a memory's key claim; a random hex
/// nonce did (2026-09-29 review-fix chain: `…3831887…` ⇒ `secret_scan_rejected`, a FAILED ticket
/// pinning its stream's prefix, `projection_promoted` red) — a harness artefact, not a finding.
fn digit_free(hex: &str) -> String {
    hex.chars()
        .map(|c| match c.to_digit(10) {
            Some(d) => char::from(b'g' + d as u8),
            None => c,
        })
        .collect()
}

/// Split `http://host:port/path` into the pieces the loopback client needs. Only plain `http`
/// is accepted: this dials 127.0.0.1 and a TLS client here would be a second transport to keep
/// in sync with no caller that needs it.
fn split_url(url: &str) -> Result<(String, String, String), String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("--gateway-url {url:?}: only http:// (loopback) is supported"))?;
    let (host_port, path) = match rest.find('/') {
        Some(i) => (rest[..i].to_string(), rest[i..].to_string()),
        None => (rest.to_string(), "/".to_string()),
    };
    if host_port.is_empty() {
        return Err(format!("--gateway-url {url:?}: empty host"));
    }
    let origin = format!("http://{host_port}");
    Ok((host_port, path, origin))
}

pub fn parse_config(args: &[String]) -> Result<Config, String> {
    let (host_port, path, origin) = split_url(&one(args, "--gateway-url")?)?;
    let lanes = values(args, "--tenant");
    if lanes.len() < 2 {
        return Err(format!(
            "--tenant given {} time(s); the card's scope requires at least two tenants\n{USAGE}",
            lanes.len()
        ));
    }
    let tenants = lanes
        .iter()
        .map(|raw| parse_lane(raw))
        .collect::<Result<Vec<_>, _>>()?;
    let lease_secs = num(args, "--lease-secs")?;
    let drain_secs = num(args, "--drain-secs")?;
    if drain_secs <= lease_secs {
        return Err(format!(
            "--drain-secs {drain_secs} must exceed --lease-secs {lease_secs}: a lease taken by a \
             killed worker is still live until it expires, so a shorter drain cannot tell a \
             wedged worker from a crashed one"
        ));
    }
    let chaos_cmds = values(args, "--chaos-cmd");
    let chaos_every = values(args, "--chaos-every-secs")
        .first()
        .map(|v| v.parse::<u64>().map(Duration::from_secs))
        .transpose()
        .map_err(|e| format!("--chaos-every-secs must be an integer: {e}"))?;
    if chaos_every.is_some() != !chaos_cmds.is_empty() {
        return Err("--chaos-every-secs and --chaos-cmd must be given together".to_string());
    }
    let chaos_grace = values(args, "--chaos-grace-secs")
        .first()
        .map(|v| v.parse::<u64>().map(Duration::from_secs))
        .transpose()
        .map_err(|e| format!("--chaos-grace-secs must be an integer: {e}"))?;
    if chaos_every.is_some() != chaos_grace.is_some() {
        return Err(
            "--chaos-grace-secs is required with --chaos-every-secs (and only then): without it \
             a chaos-killed process cannot be told from an unexpected death"
                .to_string(),
        );
    }
    let watch_pidfiles = values(args, "--watch-pidfile")
        .iter()
        .map(|raw| {
            raw.split_once('=')
                .filter(|(name, path)| !name.is_empty() && !path.is_empty())
                .map(|(name, path)| (name.to_string(), PathBuf::from(path)))
                .ok_or_else(|| format!("--watch-pidfile {raw:?}: expected <name>=<path>"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if watch_pidfiles.is_empty() {
        return Err(format!(
            "--watch-pidfile is required (ADR-0050 D-I: a probe must see the resident process)\n{USAGE}"
        ));
    }
    let max_op_failure_rate: f64 = one(args, "--max-op-failure-rate")?
        .parse()
        .map_err(|e| format!("--max-op-failure-rate must be a number: {e}"))?;
    if !(0.0..=1.0).contains(&max_op_failure_rate) {
        return Err(format!(
            "--max-op-failure-rate {max_op_failure_rate} must be a fraction in 0..=1"
        ));
    }
    let (qdrant_host, qdrant_port) = crate::switch_visible::qdrant_endpoint(args)?;
    let probe_cmds = values(args, "--probe-cmd");
    if probe_cmds.is_empty() {
        return Err(format!(
            "--probe-cmd is required (ADR-0037 probes)\n{USAGE}"
        ));
    }
    Ok(Config {
        host_port,
        path,
        origin,
        tenants,
        sessions_per_tenant: usize::try_from(num(args, "--sessions-per-tenant")?)
            .map_err(|e| format!("--sessions-per-tenant: {e}"))?,
        duration: Duration::from_secs(num(args, "--duration-secs")?),
        drain: Duration::from_secs(drain_secs),
        think: Duration::from_millis(num(args, "--think-ms")?),
        probe_every: Duration::from_secs(num(args, "--probe-every-secs")?),
        probe_cmds,
        watch_pidfiles,
        chaos_every,
        chaos_grace,
        chaos_cmds,
        lease_secs,
        max_rss_mib: num(args, "--max-rss-mib")?,
        max_db_connections: i64::try_from(num(args, "--max-db-connections")?)
            .map_err(|e| format!("--max-db-connections: {e}"))?,
        max_op_failure_rate,
        report_path: one(args, "--report")?,
        qdrant_host,
        qdrant_port,
    })
}

// ---------------------------------------------------------------------------------------------
// Loopback MCP client
// ---------------------------------------------------------------------------------------------

/// Split an HTTP/1.1 response into (status, body), decoding `Transfer-Encoding: chunked`.
/// Pure over the raw bytes so the parser is unit-testable without a socket.
pub fn parse_http(raw: &[u8]) -> Result<(u16, String), String> {
    let head_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| "no header terminator in response".to_string())?;
    let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1).map(str::to_string))
        .ok_or_else(|| "no status line".to_string())?
        .parse()
        .map_err(|e| format!("status line: {e}"))?;
    let chunked = lines.any(|l| {
        let lower = l.to_ascii_lowercase();
        lower.starts_with("transfer-encoding:") && lower.contains("chunked")
    });
    let body = &raw[head_end + 4..];
    if !chunked {
        return Ok((status, String::from_utf8_lossy(body).to_string()));
    }
    let mut out = Vec::new();
    let mut rest = body;
    loop {
        let nl = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or_else(|| "chunked body truncated".to_string())?;
        let size = usize::from_str_radix(String::from_utf8_lossy(&rest[..nl]).trim(), 16)
            .map_err(|e| format!("chunk size: {e}"))?;
        if size == 0 {
            break;
        }
        let start = nl + 2;
        let end = start + size;
        if end > rest.len() {
            return Err("chunked body shorter than its declared chunk".to_string());
        }
        out.extend_from_slice(&rest[start..end]);
        rest = &rest[(end + 2).min(rest.len())..];
    }
    Ok((status, String::from_utf8_lossy(&out).to_string()))
}

/// Build the exact `tools/call` envelope the gateway's MCP surface expects (mirrors the
/// deployment rehearsal's `mcp()` helper, headers included).
pub fn mcp_request(host_port: &str, path: &str, origin: &str, tool: &str, args: &str) -> String {
    let body = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{{\"name\":\"{tool}\",\
         \"arguments\":{args},\"_meta\":{{\"io.modelcontextprotocol/protocolVersion\":\"2026-07-28\",\
         \"io.modelcontextprotocol/clientInfo\":{{\"name\":\"xtask-soak\",\"version\":\"1\"}},\
         \"io.modelcontextprotocol/clientCapabilities\":{{}}}}}}}}"
    );
    format!(
        "POST {path} HTTP/1.1\r\nHost: {host_port}\r\nContent-Type: application/json\r\n\
         Accept: application/json, text/event-stream\r\nMCP-Protocol-Version: 2026-07-28\r\n\
         Mcp-Method: tools/call\r\nMcp-Name: {tool}\r\nOrigin: {origin}\r\n\
         Authorization: Bearer {{BEARER}}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
}

fn call(cfg: &Config, lane: &TenantLane, tool: &str, args: &str) -> Result<(u16, String), String> {
    let wire = mcp_request(&cfg.host_port, &cfg.path, &cfg.origin, tool, args)
        .replace("{BEARER}", &lane.bearer);
    // dep: HTTP(gateway) — cfg.host_port, one soak op on a fresh loopback socket.
    let mut sock = TcpStream::connect(&cfg.host_port).map_err(|e| format!("connect: {e}"))?;
    sock.set_read_timeout(Some(Duration::from_secs(120)))
        .and_then(|()| sock.set_write_timeout(Some(Duration::from_secs(30))))
        .map_err(|e| format!("socket timeouts: {e}"))?;
    sock.write_all(wire.as_bytes())
        .map_err(|e| format!("write: {e}"))?;
    let mut raw = Vec::new();
    sock.read_to_end(&mut raw)
        .map_err(|e| format!("read: {e}"))?;
    parse_http(&raw)
}

// ---------------------------------------------------------------------------------------------
// Collected series (the evaluator's whole input — synthesizable in a unit test)
// ---------------------------------------------------------------------------------------------

/// The three positions one stream moves through, kept apart because they stall for different
/// reasons and only one of them grades the projection worker.
///
/// * `issued` — `stream_checkpoints.issued_highwater`: what the writers produced.
/// * `applied` — `max(stream_seq)` over the stream's **settled** `projection.stream_log` rows:
///   the projection worker's own consumption checkpoint. Terminal is terminal — `FAILED` and
///   `SKIPPED_BY_POLICY` are consumed tickets, so this advances on every ticket the worker
///   finishes and is immune to a gap left behind by one that failed.
/// * `projected` — `stream_checkpoints.projection_highwater`: §15.4's contiguous DONE prefix,
///   which a single `FAILED` ticket pins forever no matter how many later tickets the worker
///   consumes. It is a *promotion* boundary (what the §16.2 serving switch may claim), not a
///   consumption position — grading the worker with it reports a wedged worker and a healthy
///   worker behind one declined ticket as the same thing. Card 16's own run: `applied` reached
///   22/20 while `projected` sat at 3/4, blocked by `FAILED` rows at seq 4 and 5.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamWatermark {
    pub stream: String,
    pub issued: i64,
    pub applied: i64,
    pub projected: i64,
}

#[derive(Debug, Clone, Default)]
pub struct Observation {
    pub at_secs: u64,
    pub watermarks: Vec<StreamWatermark>,
    pub backlog: i64,
    pub db_connections: i64,
    /// Largest `humaux-*` RSS in this observation's `ps` table; `None` when `ps` failed or
    /// listed no `humaux-*` process — never a default `0` that would pass `rss_bounded`.
    pub rss_mib: Option<u64>,
    pub probe_failures: usize,
    pub probes_run: usize,
    /// `ps` ran, exited 0 and returned a non-empty table (ADR-0050 D-I).
    pub ps_ok: bool,
    /// Watched processes looked up in this observation's `ps` table (0 when `ps` failed).
    pub watched: usize,
    /// Watched processes absent outside every chaos grace window — a probe failure.
    pub unexpected_absent: usize,
    /// Watched processes absent inside a chaos grace window — reported, not a failure.
    pub expected_absent: usize,
}

#[derive(Debug, Clone)]
pub struct Sample {
    pub op: String,
    pub ms: f64,
    pub ok: bool,
}

#[derive(Debug, Clone, Default)]
pub struct TenantFinal {
    pub tenant: String,
    pub tickets_total: i64,
    /// Largest `stream_seq` seen across the tenant's streams — context for a reader, never the
    /// input to a gap check (see [`TenantFinal::tickets_missing_seq`]).
    pub tickets_max_seq: i64,
    /// §15.1 gaps, summed **per stream**: `max(stream_seq) - rows` for each
    /// `(scope_kind, scope_id, domain, projection_kind, projection_version)` family this tenant
    /// owns, floored at zero. Never derived from tenant-wide totals: two streams of 10 rows
    /// give `total = 20` against `max_seq = 10`, so a tenant-wide `(max_seq - total)` is
    /// negative — and clamps to 0 — even when a seq is missing from one of them.
    pub tickets_missing_seq: i64,
    pub tickets_settled: i64,
    pub tickets_skipped_by_policy: i64,
    pub tickets_in_flight: i64,
    pub tickets_lost_state: i64,
    pub double_applied_memories: i64,
    pub live_leases_processing: i64,
    /// ADR-0052: `projection.stream_log` tickets still holding a live `--serve` lease after the
    /// drain (the sibling of `live_leases_processing` for the resident projection runner).
    pub live_ticket_leases: i64,
    pub backlog: i64,
    pub writes: i64,
    pub recall_dependency_unavailable: i64,
    pub recalls: i64,
}

#[derive(Debug, Clone, Default)]
pub struct Series {
    pub observations: Vec<Observation>,
    pub samples: Vec<Sample>,
    pub finals: Vec<TenantFinal>,
    /// Recalls that carried a `consistency_token` this run minted.
    pub ryw_checked: i64,
    /// …of which the gateway refused for a token reason (§15.5 violation).
    pub ryw_token_rejections: i64,
    /// §15.5 stream seqs the post-drain replay's own token entitled it to see and that did not
    /// come back in the response.
    pub ryw_stale: i64,
    /// …out of this many (the size of every lane's overlay range `(serving_highwater, token
    /// seq]`). `0` = nobody earned a green tick; the report prints it as the assertion's `n`.
    pub ryw_resettled: i64,
    /// Post-drain token-carrying replays attempted — **one per lane, unconditionally**, so the
    /// denominator cannot shrink to zero by every lane failing early.
    pub ryw_replays: i64,
    /// …of which no answer came back: the post-drain write did not land, its `stream_seq` could
    /// not be resolved, or the recall itself errored. Kept separate from `ryw_stale` on purpose:
    /// a replay that never happened tells you nothing about whether a settled write is visible,
    /// and folding the two together reports a transport failure as a consistency violation.
    /// `latency[]` names which half failed.
    pub ryw_replay_failures: i64,
    /// MCP responses scanned for another lane's sentinel / ids, and hits.
    pub responses_scanned: i64,
    pub cross_tenant_hits: i64,
    /// Why the §16.2 serving switch would refuse each pending promotion candidate, counted at the
    /// final snapshot. See [`promotion_assertion`] / [`promote_candidate_assertion`].
    pub promote_rejections: BTreeMap<String, i64>,
    /// `projection.stream_checkpoints` rows examined for candidacy across every lane — the
    /// denominator of [`promote_candidate_assertion`].
    pub promote_checkpoints_scanned: i64,
    /// …of which were actually offerable to §16.3 (a version that is not already its family's
    /// serving row). Reported explicitly because `0` is the honest answer on a deployment where
    /// every family holds exactly one `projection_version`, and an empty `reject_reasons` map
    /// cannot tell "nothing was refused" from "nothing was graded".
    pub promote_candidates: i64,
}

// ---------------------------------------------------------------------------------------------
// Evaluator — pure; every assertion carries value, unit, n and threshold (§78.2 no bare numbers)
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct Assertion {
    pub id: &'static str,
    pub value: f64,
    pub unit: &'static str,
    pub n: i64,
    pub threshold: f64,
    /// `true` = value is within threshold **and** the assertion had a witness (`n > 0`).
    pub pass: bool,
    /// `Some(card)` = this assertion measures something a **named, already-scheduled** card has
    /// not wired yet, so a red here is expected and does not fail the run. It is still computed,
    /// still printed and still in the report — a known gap that is silently dropped is a gap
    /// nobody re-checks. When that card lands, delete the marker and the assertion goes strict;
    /// nothing else about it changes.
    pub expected_red_until: Option<&'static str>,
    /// Free-form counters that explain `value` (e.g. the promote loop's reject reasons). Lands
    /// verbatim in the report next to the verdict so a red is readable without a second tool.
    pub detail: serde_json::Value,
}

/// `value <= threshold`, **and `n > 0`**.
///
/// The `n > 0` half is not decoration. Every assertion here is a "no bad thing happened" count,
/// so a run in which the thing was never attempted reports `0 <= 0` and looks identical to a run
/// that attempted it a thousand times and survived. Card 16's own final run shipped
/// `ryw_settled_write_visible PASS value 0.0 n 0` that way — every lane's post-drain replay had
/// bailed out before the counter was ever touched, and the report called it a pass. ADR-0038
/// states the rule this enforces: *an assertion whose denominator can silently reach zero is not
/// an assertion*. A zero denominator is now a `FAIL-VACUOUS` verdict that fails the run, so the
/// harness reports "I did not measure this" instead of "this is fine".
fn at_most(id: &'static str, value: f64, unit: &'static str, n: i64, threshold: f64) -> Assertion {
    Assertion {
        id,
        value,
        unit,
        n,
        threshold,
        pass: value <= threshold && n > 0,
        expected_red_until: None,
        detail: serde_json::Value::Null,
    }
}

impl Assertion {
    /// No assertion carries this marker today — card 20 removed the last one
    /// (`projection_promoted`) when the retirement that made it reachable landed. The mechanism
    /// stays because the next "reported, not blocking, until card N" gap will need it, and
    /// `expect` (not `allow`) is what makes the next user delete this line instead of inheriting
    /// a permanent silence.
    #[expect(
        dead_code,
        reason = "no assertion is expected-red right now; the marker mechanism is still the contract (ADR-0038)"
    )]
    fn expected_red_until(mut self, card: &'static str) -> Self {
        self.expected_red_until = Some(card);
        self
    }

    fn with_detail(mut self, detail: serde_json::Value) -> Self {
        self.detail = detail;
        self
    }

    /// A red that fails the run: `pass` is false and no card is named as owning the gap.
    pub fn blocking_failure(&self) -> bool {
        !self.pass && self.expected_red_until.is_none()
    }

    /// `FAIL-VACUOUS` is spelled differently from `FAIL` on purpose: "0 stale reads out of 0"
    /// and "1 stale read out of 28" are different defects — the first says the harness never
    /// measured, the second says the system misbehaved — and a reader must not have to compare
    /// `n` by eye to tell them apart.
    fn verdict(&self) -> &'static str {
        match (self.pass, self.expected_red_until) {
            (true, _) => "PASS",
            (false, Some(_)) => "EXPECTED-RED",
            (false, None) if self.n == 0 => "FAIL-VACUOUS",
            (false, None) => "FAIL",
        }
    }
}

/// A stream is **stalled** when its issued highwater strictly grew across the run while the
/// position `pick` reads never moved at all.
fn stalled_by(observations: &[Observation], pick: fn(&StreamWatermark) -> i64) -> Vec<String> {
    let mut first: BTreeMap<&str, (i64, i64)> = BTreeMap::new();
    let mut last: BTreeMap<&str, (i64, i64)> = BTreeMap::new();
    for obs in observations {
        for w in &obs.watermarks {
            first.entry(&w.stream).or_insert((w.issued, pick(w)));
            last.insert(&w.stream, (w.issued, pick(w)));
        }
    }
    first
        .iter()
        .filter(|(stream, (i0, p0))| {
            last.get(*stream)
                .is_some_and(|(i1, p1)| i1 > i0 && p1 == p0)
        })
        .map(|(stream, _)| (*stream).to_string())
        .collect()
}

/// Streams where the writers kept producing but the **projection worker stopped consuming**:
/// exactly the shape a wedged worker draws, and the shape negative control (a) injects.
///
/// Measured on [`StreamWatermark::applied`], never on `projected`: `projected` is §15.4's
/// contiguous DONE prefix, which one declined ticket pins for the rest of the run while the
/// worker keeps consuming normally. Grading the worker with it made card 16's first run report
/// a stall that was really a promotion boundary — see [`promote_stalled_streams`].
pub fn stalled_streams(observations: &[Observation]) -> Vec<String> {
    stalled_by(observations, |w| w.applied)
}

/// Streams whose §15.4 contiguous DONE prefix never advanced while writers kept producing —
/// i.e. the §16.2 serving switch had nothing new it could legally promote. Reported through
/// `projection_promoted`, not through `watermark_no_stall`: the two answer different questions
/// and a single number cannot answer both.
pub fn promote_stalled_streams(observations: &[Observation]) -> Vec<String> {
    stalled_by(observations, |w| w.projected)
}

/// Count of consecutive-observation transitions where a watermark went backwards. §15.3
/// highwaters are monotonic by construction; a decrease means a projection was rebuilt from a
/// stale checkpoint or two writers raced the same row.
pub fn watermark_regressions(observations: &[Observation]) -> i64 {
    let mut seen: BTreeMap<String, (i64, i64, i64)> = BTreeMap::new();
    let mut regressions = 0;
    for obs in observations {
        for w in &obs.watermarks {
            if let Some((i, a, p)) = seen.get(&w.stream)
                && (w.issued < *i || w.applied < *a || w.projected < *p)
            {
                regressions += 1;
            }
            seen.insert(w.stream.clone(), (w.issued, w.applied, w.projected));
        }
    }
    regressions
}

/// §15.1: the expected set is the dense range `1..=max(stream_seq)` **of one stream**. A missing
/// seq is a ticket that was issued and then vanished — lost, not skipped.
///
/// The per-stream arithmetic happens in [`fold_census`], which is where the stream key is still
/// in hand; this only sums what it found. Doing the subtraction here, on tenant-wide totals,
/// is the masking bug [`TenantFinal::tickets_missing_seq`] documents.
pub fn lost_tickets(finals: &[TenantFinal]) -> i64 {
    finals
        .iter()
        .map(|f| f.tickets_missing_seq + f.tickets_lost_state)
        .sum()
}

/// One `projection.stream_log` census row: the stream it belongs to, its §15.1 state, how many
/// rows carry that state, and the largest `stream_seq` among them.
#[derive(Debug, Clone)]
pub struct LedgerRow {
    /// `(scope_kind, scope_id, domain, projection_kind, projection_version)`, rendered — the
    /// full §15.3 stream key, not the tenant.
    pub stream: String,
    pub state: String,
    pub rows: i64,
    pub max_seq: i64,
}

/// Folds the per-(stream, state) census into one tenant's totals, computing the §15.1 dense-range
/// gap **per stream** before summing. Pure so the masking case (two streams, one short) is a
/// hermetic test rather than a two-workspace live run nobody will stage.
pub fn fold_census(rows: &[LedgerRow], f: &mut TenantFinal) {
    let mut per_stream: BTreeMap<&str, (i64, i64)> = BTreeMap::new();
    for r in rows {
        f.tickets_total += r.rows;
        f.tickets_max_seq = f.tickets_max_seq.max(r.max_seq);
        let entry = per_stream.entry(&r.stream).or_insert((0, 0));
        entry.0 += r.rows;
        entry.1 = entry.1.max(r.max_seq);
        match r.state.as_str() {
            // `RETIRED_FAILED` (§15.2 amendment, migration 0167) is settled like the rest — it is
            // a FAILED ticket an operator retired, not work still in flight. Without this arm it
            // would fall through to `_` and be counted as in-flight forever.
            "DONE" | "FAILED" | "TOMBSTONED" | "RETIRED_FAILED" => f.tickets_settled += r.rows,
            "SKIPPED_BY_POLICY" => {
                f.tickets_settled += r.rows;
                f.tickets_skipped_by_policy += r.rows;
            }
            "LOST" => f.tickets_lost_state += r.rows,
            _ => f.tickets_in_flight += r.rows,
        }
    }
    f.tickets_missing_seq += per_stream
        .values()
        .map(|(rows, max_seq)| (max_seq - rows).max(0))
        .sum::<i64>();
}

pub fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    // Nearest-rank: rank = ceil(q * n), clamped into the slice.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "rank is clamped into 1..=len before it is used as an index"
    )]
    let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

pub fn evaluate(
    series: &Series,
    max_rss: u64,
    max_conns: i64,
    max_op_failure_rate: f64,
) -> Vec<Assertion> {
    let mut all = observation_assertions(&series.observations, max_rss, max_conns);
    all.push(op_failure_rate(&series.samples, max_op_failure_rate));
    all.push(promotion_assertion(series));
    all.push(promote_candidate_assertion(series));
    all.extend(settlement_assertions(series));
    all
}

/// §16.2's candidate set, graded separately from [`promotion_assertion`] — card 20 folded debt 3.
///
/// **Why this shape and not "create a second projection version to grade".** The card offered
/// both. A promotion candidate is a version that is NOT its family's serving row; on this
/// deployment every family holds exactly one (card 21: the projection version is DERIVED from
/// `domain::ticket_family::TicketFamily` — one value for the whole build, no longer three
/// hand-aligned env values), so the honest candidate count is 0 and the old loop's
/// `VisibleSameVersionDeclared` tally was the harness grading a version against itself. The
/// alternative — have the harness manufacture a `v2` checkpoint row — would grade a synthetic
/// backfill rather than the deployment: criterion ① would compare `visible(v2) = 0` against a
/// populated `visible(v1)` unless the whole corpus were re-projected under v2, and criterion ③
/// cannot admit a non-first activation at all while `continuation_198_v2` is `NOT_DECLARED`
/// (§69), so an "admitted v2" would have to be bought with a `--continuation pass` nobody can
/// honestly declare. Either way the number would say nothing about the system under soak.
///
/// So the denominator here is the **population scanned** (`projection.stream_checkpoints` rows),
/// not the candidate count: `n = 0` then means "this run never looked at a checkpoint row", which
/// is a real FAIL-VACUOUS (ADR-0038) and cannot be reached by a healthy run, while
/// `candidates = 0` is reported in the detail as its own explicit statement instead of hiding
/// inside an empty `reject_reasons` map. `value` is the number of pending candidates §16.3 would
/// refuse — falsifiable the moment a family ever does hold a non-serving version, which is
/// exactly when this assertion has something to say. `projection_promoted` keeps its own real
/// witness (streams whose §15.4 prefix moved, n = streams) and is now REQUIRED.
#[expect(
    clippy::cast_precision_loss,
    reason = "counts feed a report field, not an equality test"
)]
fn promote_candidate_assertion(series: &Series) -> Assertion {
    let refused: i64 = series.promote_rejections.values().sum();
    at_most(
        "promote_candidates_admitted",
        refused as f64,
        "candidates refused by §16.3",
        series.promote_checkpoints_scanned,
        0.0,
    )
    .with_detail(serde_json::json!({
        "checkpoint_rows_scanned": series.promote_checkpoints_scanned,
        "candidates_pending": series.promote_candidates,
        "reject_reasons": series.promote_rejections,
        "note": "a candidate is a projection_version that is NOT its family's serving row; \
                 candidates_pending = 0 means §16.3 was not exercised this run",
    }))
}

/// §16.2 blue/green promotion. **REQUIRED** since card 20 — the `expected_red_until` marker is
/// gone and a red here fails the run.
///
/// `projection_promoted` counts streams whose §15.4 contiguous DONE prefix
/// (`projection_highwater`) never advanced across the run — the boundary the serving switch is
/// allowed to promote to. It is separate from `watermark_no_stall` because it grades the
/// *promote loop*, not the projection worker: card 16's run had the worker consuming every
/// ticket while the prefix stayed pinned behind two `FAILED` rows.
///
/// That pin is what card 20's migration `0167` discharges: a `FAILED` ticket is settled, so it
/// held the prefix of its stream for the rest of the run and no amount of healthy consumption
/// moved it. With the audited `FAILED -> RETIRED_FAILED` retirement (`xtask projection-serve
/// --retire-failed <class>` on the ops path) the prefix advances past a retired seq and this
/// assertion becomes a statement about the system again rather than about one unlucky ticket.
///
/// `reject_reasons` is whatever [`evaluate_switch`] actually returned for each **pending
/// candidate** — see [`promote_candidate_assertion`], which owns that population and its
/// denominator. Card 18 / ADR-0040 discharged `VisibleUnavailable`; card 20 discharged
/// `VisibleSameVersionDeclared` (the loop no longer offers a serving version to itself),
/// `OpenGaps` (retirement) and `BenchmarkNotPass` at a first activation (ADR-0017: no serving
/// version ⇒ no `benchmark(serving)` to be worse than). The map is keyed by the variant name the
/// evaluator produced, so a reason disappears from the report on its own the moment its cause is
/// fixed.
#[expect(
    clippy::cast_precision_loss,
    reason = "counts feed a report field, not an equality test"
)]
fn promotion_assertion(series: &Series) -> Assertion {
    let streams: i64 = series
        .observations
        .first()
        .map_or(0, |o| i64::try_from(o.watermarks.len()).unwrap_or(i64::MAX));
    at_most(
        "projection_promoted",
        promote_stalled_streams(&series.observations).len() as f64,
        "streams",
        streams,
        0.0,
    )
    .with_detail(serde_json::json!({
        "reject_reasons": series.promote_rejections,
        "unit": "streams whose §15.4 contiguous DONE prefix never advanced",
    }))
}

/// Everything the periodic monitor sees while the load is running: watermark movement, the
/// ADR-0037 probe runs, and the two leak ceilings. §16.2 promotion is graded separately, in
/// [`promotion_assertion`].
#[expect(
    clippy::cast_precision_loss,
    reason = "counts feed a report field, not an equality test"
)]
fn observation_assertions(obs: &[Observation], max_rss: u64, max_conns: i64) -> Vec<Assertion> {
    let streams: i64 = obs.first().map_or(0, |o| o.watermarks.len() as i64);
    let n_obs = obs.len() as i64;
    vec![
        at_most(
            "watermark_no_stall",
            stalled_streams(obs).len() as f64,
            "streams",
            streams,
            0.0,
        ),
        at_most(
            "watermark_monotonic",
            watermark_regressions(obs) as f64,
            "regressions",
            streams.max(1) * n_obs,
            0.0,
        ),
        at_most(
            "db_connections_bounded",
            obs.iter().map(|o| o.db_connections).max().unwrap_or(0) as f64,
            "connections",
            n_obs,
            max_conns as f64,
        ),
        // Only real readings are scored; `n` counts them, so a run where `ps` never produced
        // one is FAIL-VACUOUS (ADR-0038), not "0 MiB <= ceiling".
        at_most(
            "rss_bounded",
            obs.iter().filter_map(|o| o.rss_mib).max().unwrap_or(0) as f64,
            "MiB",
            obs.iter().filter(|o| o.rss_mib.is_some()).count() as i64,
            max_rss as f64,
        ),
        at_most(
            "probes_green",
            obs.iter()
                .map(|o| (o.probe_failures + o.unexpected_absent) as i64)
                .sum::<i64>() as f64,
            "failed probe runs + unexpected process absences",
            obs.iter()
                .map(|o| (o.probes_run + o.watched) as i64)
                .sum::<i64>(),
            0.0,
        )
        .with_detail(serde_json::json!({
            "failed_probe_runs": obs.iter().map(|o| o.probe_failures).sum::<usize>(),
            "unexpected_absent": obs.iter().map(|o| o.unexpected_absent).sum::<usize>(),
            "expected_absent_in_chaos_window": obs.iter().map(|o| o.expected_absent).sum::<usize>(),
        })),
        at_most(
            "ps_observed",
            obs.iter().filter(|o| !o.ps_ok).count() as f64,
            "observations whose ps failed",
            n_obs,
            0.0,
        ),
    ]
}

/// ADR-0050 D-I: value = the worst operation's `failed / n`, so a red names its op in
/// `detail`; `n` = every sample, so a run with no load is FAIL-VACUOUS (ADR-0038). Chaos-window
/// failures count: excusing them needs chaos-to-op attribution (card 54).
#[expect(
    clippy::cast_precision_loss,
    reason = "counts feed a report field, not an equality test"
)]
pub fn op_failure_rate(samples: &[Sample], threshold: f64) -> Assertion {
    let mut per_op: BTreeMap<&str, (i64, i64)> = BTreeMap::new();
    // ADR-0055 D-E: `recall.stage*` samples are derived from a successful recall, not calls.
    let calls: Vec<&Sample> = samples
        .iter()
        .filter(|s| !s.op.starts_with(RECALL_STAGE_PREFIX))
        .collect();
    for s in &calls {
        let e = per_op.entry(s.op.as_str()).or_default();
        e.0 += 1;
        e.1 += i64::from(!s.ok);
    }
    let rate = |(n, failed): (i64, i64)| failed as f64 / n as f64;
    let worst = per_op.values().map(|v| rate(*v)).fold(0.0, f64::max);
    at_most(
        "op_failure_rate",
        worst,
        "fraction of calls failed (worst op)",
        calls.len() as i64,
        threshold,
    )
    .with_detail(serde_json::Value::Array(
        per_op
            .iter()
            .map(|(op, v)| serde_json::json!({"op": op, "n": v.0, "failed": v.1, "rate": rate(*v)}))
            .collect(),
    ))
}

/// Everything only the post-drain snapshot can answer: the §15.1 ledger, exactly-once, the
/// §31/§61 lease, and §15.5 read-your-writes.
#[expect(
    clippy::cast_precision_loss,
    reason = "counts feed a report field, not an equality test"
)]
fn settlement_assertions(series: &Series) -> Vec<Assertion> {
    let tickets: i64 = series.finals.iter().map(|f| f.tickets_total).sum();
    let sum = |pick: fn(&TenantFinal) -> i64| series.finals.iter().map(pick).sum::<i64>() as f64;
    vec![
        at_most(
            "tickets_not_lost",
            lost_tickets(&series.finals) as f64,
            "tickets",
            tickets,
            0.0,
        ),
        at_most(
            "tickets_settled_after_drain",
            sum(|f| f.tickets_in_flight),
            "tickets",
            tickets,
            0.0,
        ),
        at_most(
            "tickets_applied_once",
            sum(|f| f.double_applied_memories),
            "memories",
            tickets,
            0.0,
        ),
        at_most(
            "no_live_lease_after_drain",
            sum(|f| f.live_leases_processing),
            "ops.jobs rows",
            tickets,
            0.0,
        ),
        at_most(
            "no_live_ticket_lease_after_drain",
            sum(|f| f.live_ticket_leases),
            "stream_log tickets",
            tickets,
            0.0,
        ),
        at_most(
            "backlog_drained",
            sum(|f| f.backlog),
            "queued rows",
            tickets,
            0.0,
        ),
        at_most(
            "ryw_token_honoured",
            series.ryw_token_rejections as f64,
            "rejections",
            series.ryw_checked,
            0.0,
        ),
        at_most(
            "ryw_replay_answered",
            series.ryw_replay_failures as f64,
            "unanswered replays",
            series.ryw_replays,
            0.0,
        ),
        at_most(
            "ryw_settled_write_visible",
            series.ryw_stale as f64,
            "stale reads",
            series.ryw_resettled,
            0.0,
        ),
        at_most(
            "no_cross_tenant_row",
            series.cross_tenant_hits as f64,
            "responses",
            series.responses_scanned,
            0.0,
        ),
    ]
}

#[expect(
    clippy::cast_precision_loss,
    reason = "counts feed a report field, not an equality test"
)]
pub fn report_json(
    series: &Series,
    assertions: &[Assertion],
    cfg_summary: serde_json::Value,
) -> serde_json::Value {
    let mut by_op: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    let mut fails: BTreeMap<&str, i64> = BTreeMap::new();
    for s in &series.samples {
        // Seed the entry either way: an operation whose calls ALL failed must not vanish from
        // the report (it did, in this harness's own first live run — the least visible shape a
        // total failure can take).
        let slot = by_op.entry(&s.op).or_default();
        if s.ok {
            slot.push(s.ms);
        } else {
            *fails.entry(&s.op).or_default() += 1;
        }
    }
    let latency: Vec<serde_json::Value> = by_op
        .iter_mut()
        .map(|(op, v)| {
            v.sort_by(f64::total_cmp);
            serde_json::json!({
                "operation": op, "unit": "ms", "n": v.len(),
                "p50": percentile(v, 0.50), "p95": percentile(v, 0.95),
                "failed_calls": fails.get(op).copied().unwrap_or(0),
            })
        })
        .collect();
    let tenants: Vec<serde_json::Value> = series
        .finals
        .iter()
        .map(|f| {
            serde_json::json!({
                "tenant_id": f.tenant, "writes": f.writes, "recalls": f.recalls,
                "tickets": {"total": f.tickets_total, "max_stream_seq": f.tickets_max_seq,
                            "settled": f.tickets_settled, "in_flight": f.tickets_in_flight,
                            "lost_state": f.tickets_lost_state,
                            "skipped_by_policy": f.tickets_skipped_by_policy,
                            "unit": "tickets"},
                // D1 (card 24): live-model non-determinism is a first-class metric, never
                // absorbed into "lost" — a SKIPPED_BY_POLICY ticket is settled.
                "skipped_by_policy_rate": {
                    "value": if f.tickets_total == 0 { 0.0 }
                             else { f.tickets_skipped_by_policy as f64 / f.tickets_total as f64 },
                    "unit": "fraction of tickets", "n": f.tickets_total },
                "recall_dependency_unavailable_rate": {
                    "value": if f.recalls == 0 { 0.0 }
                             else { f.recall_dependency_unavailable as f64 / f.recalls as f64 },
                    "unit": "fraction of recalls", "n": f.recalls },
            })
        })
        .collect();
    // The timeline is the evidence behind `watermark_no_stall`: a reader has to be able to see
    // the highwater move, not just take the verdict's word for it.
    let timeline: Vec<serde_json::Value> = series
        .observations
        .iter()
        .map(|o| {
            serde_json::json!({
                "at_secs": o.at_secs,
                "issued_highwater_sum": o.watermarks.iter().map(|w| w.issued).sum::<i64>(),
                // The consumption position `watermark_no_stall` grades…
                "applied_highwater_sum": o.watermarks.iter().map(|w| w.applied).sum::<i64>(),
                // …and the promotion boundary `projection_promoted` grades. Both, so a reader
                // can see the two diverge instead of taking the split on trust.
                "projection_highwater_sum": o.watermarks.iter().map(|w| w.projected).sum::<i64>(),
                "backlog_rows": o.backlog, "db_connections": o.db_connections,
                "max_process_rss_mib": o.rss_mib,
                "probe_failures": o.probe_failures, "probes_run": o.probes_run,
                "ps_ok": o.ps_ok, "watched": o.watched,
                "unexpected_absent": o.unexpected_absent, "expected_absent": o.expected_absent,
            })
        })
        .collect();
    serde_json::json!({
        "report": "humaux soak", "config": cfg_summary,
        "observations": series.observations.len(),
        "timeline": timeline,
        "assertions": assertions.iter().map(|a| serde_json::json!({
            "id": a.id, "value": a.value, "unit": a.unit, "n": a.n,
            "threshold_at_most": a.threshold, "verdict": a.verdict(),
            "expected_red_until": a.expected_red_until,
            "detail": a.detail,
        })).collect::<Vec<_>>(),
        "latency": latency,
        "tenants": tenants,
        // An `EXPECTED-RED` assertion does not fail the run: the card that owns the gap is
        // named on the row, and the run's verdict must not become a permanent red that
        // everyone learns to ignore. It is still counted here so the reader sees it exists.
        "verdict": if assertions.iter().any(Assertion::blocking_failure) { "FAIL" } else { "PASS" },
        "expected_red": assertions.iter().filter(|a| !a.pass && a.expected_red_until.is_some())
            .map(|a| serde_json::json!({"id": a.id, "until": a.expected_red_until}))
            .collect::<Vec<_>>(),
    })
}

// ---------------------------------------------------------------------------------------------
// Live collection
// ---------------------------------------------------------------------------------------------

const MAINTENANCE_DSN_ENV: &str = "HUMAUX_MAINTENANCE_PG_DSN";

/// §6.2.2: `role_maintenance` holds SELECT on every table read below, and RLS keys off
/// `humaux.tenant_id` — so each read is scoped by installing the lane's tenant, which is also
/// what makes the per-tenant numbers trustworthy.
fn set_tenant(db: &mut Client, tenant: Uuid) -> Result<(), String> {
    db.batch_execute(&format!("SET humaux.tenant_id = '{tenant}'"))
        .map_err(|e| format!("SET humaux.tenant_id: {e}"))
}

fn scalar(db: &mut Client, sql: &str) -> Result<i64, String> {
    db.query_one(sql, &[])
        .map(|r| r.get::<_, i64>(0))
        .map_err(|e| format!("{sql}: {e}"))
}

/// The three positions of every stream this tenant owns. `applied` is a correlated subquery on
/// `projection.stream_log` rather than another checkpoint column because no checkpoint column
/// holds it: §15.3 stores the *prefix*, and the worker's consumption position is only derivable
/// from the ledger's settled rows (`settled_at IS NOT NULL` is the §15.1 CHECK's own definition
/// of the four terminal states, so this cannot drift from that closed set).
/// One stream's label: the tenant plus the five other §15.1 key columns (scope kind, scope id,
/// domain, projection kind, version). Card 27: without the scope a tenant with two workspaces
/// folded two checkpoint rows into one label, and every observation compared workspace 1's
/// watermark with workspace 2's — 53 spurious "regressions" in the first 3 × 2 soak.
fn stream_label(tenant: Uuid, key: [&str; 5]) -> String {
    format!("{tenant}|{}", key.join("|"))
}

fn watermarks(db: &mut Client, tenant: Uuid) -> Result<Vec<StreamWatermark>, String> {
    db.query(
        "SELECT c.domain, c.projection_kind, c.projection_version, c.issued_highwater, \
                c.projection_highwater, c.scope_kind, c.scope_id::text, \
                COALESCE((SELECT max(l.stream_seq) FROM projection.stream_log l \
                           WHERE l.tenant_id = c.tenant_id AND l.scope_kind = c.scope_kind \
                             AND l.scope_id = c.scope_id AND l.domain = c.domain \
                             AND l.projection_kind = c.projection_kind \
                             AND l.projection_version = c.projection_version \
                             AND l.settled_at IS NOT NULL), 0) AS applied_highwater \
         FROM projection.stream_checkpoints c",
        &[],
    )
    .map_err(|e| format!("stream_checkpoints: {e}"))
    .map(|rows| {
        rows.iter()
            .map(|r| StreamWatermark {
                stream: stream_label(
                    tenant,
                    [
                        &r.get::<_, String>(5),
                        &r.get::<_, String>(6),
                        &r.get::<_, String>(0),
                        &r.get::<_, String>(1),
                        &r.get::<_, String>(2),
                    ],
                ),
                issued: r.get(3),
                projected: r.get(4),
                applied: r.get(7),
            })
            .collect()
    })
}

/// One §16.2 promotion candidate, as the database describes it: a `projection.stream_checkpoints`
/// row plus the two facts [`evaluate_switch`] needs about it that only the DB can answer.
#[derive(Debug, Clone)]
pub struct PromoteCandidate {
    /// ADR-0017: the candidate's family has no `serving` row yet, so there is nothing to
    /// compare the shadow against.
    pub first_activation: bool,
    /// `count(*)` from `projection.processing_gaps` for this candidate's own full stream key
    /// (§16.3 criterion ②) — the same filter `adapters::serving_repo` uses.
    pub open_gaps: i64,
    /// §23.1②'s live Qdrant count for the **candidate** version, tagged with that version
    /// (`crate::switch_visible`, which delegates to card 18's shared producer
    /// `adapters::retrieve::stream_count_of_version`, ADR-0057 D-C). `None` only when the count
    /// genuinely could not be taken — no placement row or Qdrant unreachable — never as a
    /// stand-in for "this harness has no counter".
    pub visible_shadow: Option<(String, u64)>,
    /// The same count for the family's current `serving` version, or `None` for a first
    /// activation (there is no serving face to compare against yet).
    pub visible_serving: Option<(String, u64)>,
}

/// What one tenant's promote-loop snapshot graded, and over what population — card 20 folded
/// debt 3.
///
/// `scanned` is every `projection.stream_checkpoints` row the tenant owns; `candidates` is the
/// subset that is actually offerable to §16.3, i.e. the versions that are **not** already their
/// family's serving row. Before this split the soak offered each family's serving version back to
/// itself every 5 s and the evaluator answered `VisibleSameVersionDeclared` — a refusal that
/// describes the harness, not the deployment (soak27: `VisibleSameVersionDeclared` 2, one per
/// family, each family holding exactly one `projection_version`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PromoteGrading {
    pub scanned: i64,
    pub candidates: i64,
    pub reject_reasons: BTreeMap<String, i64>,
}

/// Why the §16.2 serving switch would refuse each candidate, **taken from
/// [`evaluate_switch`]** — the sole judgement point (§16.3 "无裁量口") — rather than restated
/// here. Counted per candidate, keyed by the `SwitchRejection` variant's own name, so a reason
/// this harness has never seen still appears the moment the evaluator returns it.
///
/// The two `visible_*` counts arrive on [`PromoteCandidate`], taken live from Qdrant by
/// [`promote_rejections`] through `crate::switch_visible` — the ops producer
/// (`adapters::retrieve::stream_count_of_version`, ADR-0057 D-C: every point of the stream,
/// same tombstone overlay as the read routes), so there is no second hand-written filter for
/// this call (`crates/projection/tests/no_handwritten_filter_scan.rs`). Until that wiring existed this
/// function passed a literal `None` on both sides and every candidate in every soak witness was
/// refused `VisibleUnavailable` — a statement about the harness, not the deployment. A `None`
/// here is now a real refusal (no placement row or Qdrant unreachable), and §23.1's rule still
/// holds: an uncountable
/// index is `visible: null`, never a backfill from another number.
///
/// `continuation` is [`ContinuationVerdict::CannotEstablish`] for a similar reason: §69's
/// `baseline_min` / `frozen_by` are `NOT_DECLARED` on this deployment, and §69 forbids reading
/// "not established" as a pass. It is the same verdict `xtask projection-serve` now declares by
/// default (card 20 folded debt 2) — before that the CLI defaulted to `Pass` and this tally
/// reported `BenchmarkNotPass` against switches that had in fact succeeded. Note what
/// `evaluate_switch` does with it: on a FIRST activation there is no `benchmark(serving)` to be
/// worse than, so the verdict does not refuse; off that path it still does.
///
/// The previous version of this function returned `count(*) FROM stream_checkpoints` under the
/// label `VisibleUnavailable` without calling the evaluator at all. That is a fabricated
/// measurement: it reported a refusal on streams whose promotion had demonstrably succeeded,
/// it could never surface `VersionCollision` or the benchmark refusal, and it would have kept
/// reporting the stream count after card 18 landed.
pub fn switch_rejections(candidates: &[PromoteCandidate]) -> BTreeMap<String, i64> {
    let mut counts = BTreeMap::new();
    for c in candidates {
        let criteria = SwitchCriteria {
            shadow: ActivationEvidence::VisibleThrough(c.visible_shadow.clone()),
            visible_serving: c.visible_serving.clone(),
            first_activation: c.first_activation,
            shadow_open_gaps: u64::try_from(c.open_gaps).unwrap_or(u64::MAX),
            continuation: ContinuationVerdict::CannotEstablish,
        };
        if let Err(rejections) = evaluate_switch(&criteria) {
            for r in rejections {
                *counts.entry(format!("{r:?}")).or_insert(0) += 1;
            }
        }
    }
    counts
}

/// Reads every §16.2 promotion candidate this tenant owns, takes both §23.1② `visible` counts
/// for it live, and grades each one through [`switch_rejections`].
///
/// **Which version each side counts** (ADR-0040): `visible_shadow` is the **candidate** row's
/// own `projection_version` — `evaluate_switch` judges the version being promoted, not the one
/// already serving — and `visible_serving` is whatever version currently holds the family's
/// `serving` row (`None` ⇒ ADR-0017 first activation). Card 18's read-route guard
/// (`serving_version != key.projection_version ⇒ None`) is deliberately not applied here: it
/// protects an A2 comparison against a `LedgerClosure` closed at one version, while §16.3's
/// criterion ① compares two counts whose versions are *required* to differ.
fn promote_rejections(
    db: &mut Client,
    tenant: Uuid,
    face: &VisibleFace,
    rt: &tokio::runtime::Runtime,
) -> Result<PromoteGrading, String> {
    set_tenant(db, tenant)?;
    let scanned: i64 = db
        .query_one(
            "SELECT count(*)::bigint FROM projection.stream_checkpoints",
            &[],
        )
        .map_err(|e| format!("promote scan: {e}"))?
        .get(0);
    let rows = db
        .query(
            "SELECT c.scope_kind, c.scope_id, c.domain, c.projection_kind, c.projection_version, \
                    (SELECT s.projection_version FROM projection.stream_checkpoints s \
                      WHERE s.tenant_id = c.tenant_id AND s.scope_kind = c.scope_kind \
                        AND s.scope_id = c.scope_id AND s.domain = c.domain \
                        AND s.projection_kind = c.projection_kind AND s.serving), \
                    (SELECT count(*)::bigint FROM projection.processing_gaps g \
                      WHERE g.tenant_id = c.tenant_id AND g.scope_kind = c.scope_kind \
                        AND g.scope_id = c.scope_id AND g.domain = c.domain \
                        AND g.projection_kind = c.projection_kind \
                        AND g.projection_version = c.projection_version) \
             FROM projection.stream_checkpoints c \
             WHERE NOT c.serving",
            &[],
        )
        .map_err(|e| format!("promote candidates: {e}"))?;
    let mut candidates = Vec::with_capacity(rows.len());
    for r in &rows {
        let family = StreamFamily::new(
            TenantId(tenant),
            r.get::<_, String>(0),
            r.get::<_, Uuid>(1),
            r.get::<_, String>(2),
            r.get::<_, String>(3),
        );
        let candidate_version: String = r.get(4);
        let serving: Option<String> = r.get(5);
        let (visible_shadow, visible_serving) =
            match read_candidate_facts(db, &family, &candidate_version, serving.as_deref())? {
                Some(facts) => rt.block_on(visible_pair(
                    face,
                    &Candidate {
                        tenant: TenantId(tenant),
                        workspace: WorkspaceId(family.scope_id),
                        collection: &facts.collection,
                        candidate_version: &candidate_version,
                        candidate_tombstoned: &facts.candidate_tombstoned,
                        serving_version: serving.as_deref(),
                        serving_tombstoned: &facts.serving_tombstoned,
                    },
                )),
                // No §17.3 placement row for this tenant: nothing to count against, so both
                // sides stay the honest `None` (`VisibleUnavailable`).
                None => (None, None),
            };
        candidates.push(PromoteCandidate {
            first_activation: serving.is_none(),
            open_gaps: r.get(6),
            visible_shadow,
            visible_serving,
        });
    }
    Ok(PromoteGrading {
        scanned,
        candidates: i64::try_from(candidates.len()).unwrap_or(i64::MAX),
        reject_reasons: switch_rejections(&candidates),
    })
}

const BACKLOG_SQL: &str = "SELECT (SELECT count(*) FROM ops.jobs \
     WHERE status IN ('PENDING','PROCESSING','RETRY_WAIT','WAITING_KEY')) \
   + (SELECT count(*) FROM ops.outbox WHERE status IN ('PENDING','PROCESSING'))";

/// `pid -> (rss_kib, comm)` from one `ps -axo pid=,rss=,comm=` run. A non-zero exit or an
/// empty table is an `Err` (ADR-0050 D-I: a `ps` failure is an assertion failure, never RSS 0).
pub fn parse_ps_table(
    stdout: &str,
    status_ok: bool,
) -> Result<BTreeMap<u32, (u64, String)>, String> {
    if !status_ok {
        return Err("ps exited non-zero".to_string());
    }
    let table: BTreeMap<u32, (u64, String)> = stdout
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let pid = parts.next()?.parse().ok()?;
            let rss = parts.next()?.parse().ok()?;
            let comm = parts.collect::<Vec<_>>().join(" ");
            Some((pid, (rss, comm)))
        })
        .collect();
    if table.is_empty() {
        return Err("ps returned an empty table".to_string());
    }
    Ok(table)
}

/// Whether one watched process is alive at one observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Presence {
    Present,
    /// Absent inside `[chaos_start, chaos_start + grace]` of some chaos step.
    ExpectedAbsent,
    UnexpectedAbsent,
}

/// A watched process is present only when its pidfile names a pid that `ps` lists with a
/// command whose executable **basename** starts with `humaux-` (a reused pid running something
/// else is absent). Basename, not substring: macOS `ps -o comm=` prints the full path, and any
/// binary built under `…/humaux-target-boot/…` would otherwise count as alive (ADR-0051 D-L).
pub fn classify_presence(
    pidfile_pid: Option<u32>,
    table: &BTreeMap<u32, (u64, String)>,
    now: Instant,
    chaos_starts: &[Instant],
    grace: Option<Duration>,
) -> Presence {
    let alive = pidfile_pid
        .and_then(|pid| table.get(&pid))
        .is_some_and(|(_, comm)| {
            std::path::Path::new(comm)
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("humaux-"))
        });
    if alive {
        return Presence::Present;
    }
    let in_window = grace.is_some_and(|grace| {
        chaos_starts
            .iter()
            .any(|start| *start <= now && now <= *start + grace)
    });
    if in_window {
        Presence::ExpectedAbsent
    } else {
        Presence::UnexpectedAbsent
    }
}

/// One `ps` run → `ps_ok`, `rss_mib`, and a presence verdict per watched pidfile.
fn observe_processes(cfg: &Config, chaos_starts: &Mutex<Vec<Instant>>, obs: &mut Observation) {
    // dep: subprocess(ps) — the portable way to read other processes' liveness and RSS without taking PIDs.
    let table = std::process::Command::new("ps")
        .args(["-axo", "pid=,rss=,comm="])
        .output()
        .map_err(|e| format!("ps could not run: {e}"))
        .and_then(|out| {
            parse_ps_table(&String::from_utf8_lossy(&out.stdout), out.status.success())
        });
    let table = match table {
        Ok(table) => table,
        Err(e) => {
            eprintln!("soak: ps_observed fail at {}s: {e}", obs.at_secs);
            return;
        }
    };
    obs.ps_ok = true;
    obs.rss_mib = table
        .values()
        .filter(|(_, comm)| comm.contains("humaux-"))
        .map(|(kib, _)| kib / 1024)
        .max();
    let starts = chaos_starts.lock().map(|s| s.clone()).unwrap_or_default();
    let now = Instant::now();
    for (name, pidfile) in &cfg.watch_pidfiles {
        let pid = std::fs::read_to_string(pidfile)
            .ok()
            .and_then(|s| s.trim().parse().ok());
        obs.watched += 1;
        match classify_presence(pid, &table, now, &starts, cfg.chaos_grace) {
            Presence::Present => {}
            Presence::ExpectedAbsent => obs.expected_absent += 1,
            Presence::UnexpectedAbsent => {
                eprintln!(
                    "soak: {name} absent at {}s outside any chaos window",
                    obs.at_secs
                );
                obs.unexpected_absent += 1;
            }
        }
    }
}

fn run_shell(cmd: &str) -> bool {
    // dep: subprocess(sh) — runs the operator-supplied chaos command.
    std::process::Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .status()
        .is_ok_and(|s| s.success())
}

fn observe(
    cfg: &Config,
    db: &mut Client,
    at_secs: u64,
    chaos_starts: &Mutex<Vec<Instant>>,
) -> Result<Observation, String> {
    let mut obs = Observation {
        at_secs,
        db_connections: scalar(
            db,
            "SELECT count(*)::bigint FROM pg_stat_activity WHERE datname = current_database()",
        )?,
        ..Observation::default()
    };
    for lane in &cfg.tenants {
        set_tenant(db, lane.tenant_id)?;
        obs.watermarks.extend(watermarks(db, lane.tenant_id)?);
        obs.backlog += scalar(db, BACKLOG_SQL)?;
    }
    for cmd in &cfg.probe_cmds {
        obs.probes_run += 1;
        if !run_shell(cmd) {
            obs.probe_failures += 1;
        }
    }
    observe_processes(cfg, chaos_starts, &mut obs);
    Ok(obs)
}

fn tenant_final(db: &mut Client, lane: &TenantLane) -> Result<TenantFinal, String> {
    set_tenant(db, lane.tenant_id)?;
    // Grouped by the full §15.3 stream key, not by state alone: the §15.1 dense range is a
    // property of ONE stream, and a tenant with two workspaces (or a projection_version bump,
    // which is exactly what card 18's blue/green promotion introduces) has more than one.
    let states = db
        .query(
            "SELECT scope_kind, scope_id::text, domain, projection_kind, projection_version, \
                    state, count(*)::bigint, coalesce(max(stream_seq),0) \
             FROM projection.stream_log \
             GROUP BY scope_kind, scope_id, domain, projection_kind, projection_version, state",
            &[],
        )
        .map_err(|e| format!("stream_log: {e}"))?;
    let mut f = TenantFinal {
        tenant: lane.tenant_id.to_string(),
        ..TenantFinal::default()
    };
    let census: Vec<LedgerRow> = states
        .iter()
        .map(|r| LedgerRow {
            stream: format!(
                "{}|{}|{}|{}|{}",
                r.get::<_, String>(0),
                r.get::<_, String>(1),
                r.get::<_, String>(2),
                r.get::<_, String>(3),
                r.get::<_, String>(4)
            ),
            state: r.get(5),
            rows: r.get(6),
            max_seq: r.get(7),
        })
        .collect();
    fold_census(&census, &mut f);
    // §31/§61: after a drain longer than LEASE_SECS every lease a killed worker held has
    // expired, so a still-live lease on a PROCESSING row means a wedged worker.
    f.live_leases_processing = scalar(
        db,
        "SELECT count(*)::bigint FROM ops.jobs WHERE status = 'PROCESSING' \
         AND lease_expires_at IS NOT NULL AND lease_expires_at > now()",
    )?;
    // ADR-0052: the resident projection runner leases tickets on stream_log itself. A --serve
    // process killed mid-batch leaves leases that expire after LEASE_SECS and are re-claimed; a
    // lease still live after the drain means a runner that is wedged or was never replaced.
    // Scoped to this lane's tenant by the RLS context `set_tenant` installed above.
    f.live_ticket_leases = scalar(
        db,
        "SELECT count(*)::bigint FROM projection.stream_log \
         WHERE lease_owner IS NOT NULL AND lease_expires_at > now()",
    )?;
    // Exactly-once at the projection registry: the UNIQUE constraint already refuses a
    // byte-identical replay, so the observable double-apply is a SECOND live point for the
    // same memory in the same family — which is what negative control (b) injects.
    f.double_applied_memories = scalar(
        db,
        "SELECT count(*)::bigint FROM (SELECT memory_id FROM projection.private_memory_points \
         WHERE projection_live GROUP BY scope_kind, scope_id, domain, projection_kind, \
         projection_version, embedding_version, memory_id HAVING count(*) > 1) dup",
    )?;
    f.backlog = scalar(db, BACKLOG_SQL)?;
    Ok(f)
}

// ---------------------------------------------------------------------------------------------
// Load
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
struct Shared {
    samples: Vec<Sample>,
    per_lane_writes: BTreeMap<usize, i64>,
    per_lane_recalls: BTreeMap<usize, i64>,
    per_lane_dep_unavailable: BTreeMap<usize, i64>,
    responses_scanned: i64,
    cross_tenant_hits: i64,
    ryw_checked: i64,
    ryw_token_rejections: i64,
}

/// The other lanes' sentinels and ids must never appear in this lane's response.
fn cross_tenant_hit(cfg: &Config, lane_idx: usize, body: &str) -> bool {
    cfg.tenants.iter().enumerate().any(|(i, other)| {
        i != lane_idx
            && (body.contains(&other.sentinel)
                || body.contains(&other.tenant_id.to_string())
                || body.contains(&other.workspace_id.to_string()))
    })
}

/// Latency bucket of the `memory` tool's `enumerate` action (card 30: this bucket was reported
/// as plain `"memory"` and misread as `memory.get` in delivery_point_report §4.1).
const OP_MEMORY_ENUMERATE: &str = "memory.enumerate";
/// Latency bucket of the `memory` tool's `get` action — never timed before card 30.
const OP_MEMORY_GET: &str = "memory.get";
/// Prefix of the derived per-stage recall samples (ADR-0055 D-E): one `recall.stage.<name>` per
/// stage and one `recall.stage_sum` per successful recall. Derived from a call, not calls.
const RECALL_STAGE_PREFIX: &str = "recall.stage";
/// `provenance.stage_ms` keys `recall.search` reports (bins/gateway/src/recall.rs `STAGE_NAMES`).
const RECALL_STAGES: [&str; 8] = [
    "route", "planner", "scan", "embed", "qdrant", "hydrate", "rerank", "assemble",
];

/// `op` is the latency bucket, `tool` the MCP tool dialed — they differ for the `memory` tool,
/// whose two actions are bucketed separately.
fn timed(cfg: &Config, lane: &TenantLane, op: &str, tool: &str, args: &str) -> (Sample, String) {
    let started = Instant::now();
    let result = call(cfg, lane, tool, args);
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    let (sample, body) = match result {
        Ok((status, body)) => (
            Sample {
                op: op.to_string(),
                ms,
                ok: status == 200 && !body.contains("\"isError\":true"),
            },
            body,
        ),
        Err(e) => (
            Sample {
                op: op.to_string(),
                ms,
                ok: false,
            },
            format!("{{\"transport_error\":\"{e}\"}}"),
        ),
    };
    // Card 24 rehearsal4 run 1: `remember failed_calls = 1` with nothing anywhere saying
    // what the reply was. A failed call is a finding; a finding without its shape is
    // uninvestigable, so the head of the reply goes to stderr (the soak's own log), never
    // into the report.
    if !sample.ok {
        let head: String = body.chars().take(240).collect();
        eprintln!(
            "soak: {op} failed on lane {} after {ms:.0}ms: {head}",
            lane.sentinel
        );
    }
    (sample, body)
}

fn json_field(body: &str, key: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    v.pointer(&format!("/result/structuredContent/{key}"))?
        .as_str()
        .map(str::to_string)
}

/// One agent session: write, read it back with its own token, then two governance reads.
/// §33.10 keeps the confirm gate on destructive verbs only, and this harness deliberately
/// drives none — ponytail: a soak that pins and unpins under load would be grading card 8's
/// gate, not this card's endurance claim. Upgrade path is in ADR-0038.
fn session(cfg: &Config, lane_idx: usize, deadline: Instant, shared: &Mutex<Shared>) {
    let lane = &cfg.tenants[lane_idx];
    while Instant::now() < deadline {
        let nonce = Uuid::new_v4().simple().to_string();
        let marker = digit_free(&nonce);
        let content = format!(
            "{} {marker}: the soak lane records that a backend service must publish a readiness \
             probe before traffic reaches it.",
            lane.sentinel
        );
        let put = format!(
            "{{\"operation\":\"put\",\"content\":\"{content}\",\"idempotency_key\":\"soak-{nonce}\",\
             \"workspace_id\":\"{}\"}}",
            lane.workspace_id
        );
        let (s_put, put_body) = timed(cfg, lane, "remember", "remember", &put);
        let token = json_field(&put_body, "consistency_token");
        let recall_args = token.as_ref().map_or_else(
            || {
                format!(
                    "{{\"query\":\"readiness probe before traffic\",\"workspace_id\":\"{}\",\"mode\":\"semantic\"}}",
                    lane.workspace_id
                )
            },
            |t| {
                format!(
                    "{{\"query\":\"readiness probe before traffic\",\"workspace_id\":\"{}\",\
                     \"mode\":\"semantic\",\"consistency_token\":\"{t}\"}}",
                    lane.workspace_id
                )
            },
        );
        let (s_recall, recall_body) = timed(cfg, lane, "recall", "recall", &recall_args);
        let enumerate = format!(
            "{{\"action\":\"enumerate\",\"workspace_id\":\"{}\",\"limit\":10}}",
            lane.workspace_id
        );
        let (s_enum, enum_body) = timed(cfg, lane, OP_MEMORY_ENUMERATE, "memory", &enumerate);
        // Skipped (no sample), not failed, when this session's recall returned no memory item.
        let got = memory_get_args(&recall_body, &lane.workspace_id.to_string())
            .map(|args| timed(cfg, lane, OP_MEMORY_GET, "memory", &args));
        let mut calls = vec![
            (s_put, put_body),
            (s_recall, recall_body),
            (s_enum, enum_body),
        ];
        calls.extend(got);
        record(cfg, lane_idx, shared, calls, token.is_some());
        std::thread::sleep(cfg.think);
    }
}

/// `memory.get` arguments for the first memory item of a recall response, `None` when the
/// recall returned no memory item (or failed).
fn memory_get_args(recall_body: &str, workspace_id: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(recall_body).ok()?;
    let id = v
        .pointer("/result/structuredContent/items")?
        .as_array()?
        .iter()
        .find(|item| item["kind"] == "memory")?["memory_id"]
        .as_str()?
        .to_owned();
    Some(format!(
        "{{\"action\":\"get\",\"memory_id\":\"{id}\",\"workspace_id\":\"{workspace_id}\"}}"
    ))
}

/// ADR-0055 D-E: one `recall.stage.<name>` sample per stage plus one `recall.stage_sum`, read
/// from a successful recall's `provenance.stage_ms`. A failed recall, or one without all eight
/// stages, yields none — a missing stage is not a zero.
fn recall_stage_samples(recall: &Sample, body: &str) -> Vec<Sample> {
    if !recall.ok {
        return Vec::new();
    }
    let Some(stage_ms) = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| {
            v.pointer("/result/structuredContent/provenance/stage_ms")
                .cloned()
        })
    else {
        return Vec::new();
    };
    let Some(values) = RECALL_STAGES
        .iter()
        .map(|name| stage_ms.get(name).and_then(serde_json::Value::as_f64))
        .collect::<Option<Vec<f64>>>()
    else {
        return Vec::new();
    };
    let sample = |op: String, ms: f64| Sample { op, ms, ok: true };
    let mut out: Vec<Sample> = RECALL_STAGES
        .iter()
        .zip(&values)
        .map(|(name, ms)| sample(format!("{RECALL_STAGE_PREFIX}.{name}"), *ms))
        .collect();
    out.push(sample(
        format!("{RECALL_STAGE_PREFIX}_sum"),
        values.iter().sum(),
    ));
    out
}

fn record(
    cfg: &Config,
    lane_idx: usize,
    shared: &Mutex<Shared>,
    // `[remember, recall, memory.enumerate, memory.get?]` — the recall is always `calls[1]`.
    calls: Vec<(Sample, String)>,
    // Whether the `remember.put` above handed back a `consistency_token`, i.e. whether the
    // recall in `calls[1]` carried one.
    token_issued: bool,
) {
    let Ok(mut g) = shared.lock() else { return };
    let recall_body = calls[1].1.clone();
    let stages = recall_stage_samples(&calls[1].0, &recall_body);
    g.samples.extend(stages);
    for (sample, body) in calls {
        g.responses_scanned += 1;
        if cross_tenant_hit(cfg, lane_idx, &body) {
            g.cross_tenant_hits += 1;
        }
        g.samples.push(sample);
    }
    *g.per_lane_writes.entry(lane_idx).or_default() += 1;
    *g.per_lane_recalls.entry(lane_idx).or_default() += 1;
    if recall_body.contains("DEPENDENCY_UNAVAILABLE") {
        *g.per_lane_dep_unavailable.entry(lane_idx).or_default() += 1;
    }
    if token_issued {
        g.ryw_checked += 1;
        // §15.5: the token this very session was handed must never be refused as malformed,
        // expired, not-issued, or out of scope.
        //
        // A bare `INVALID_INPUT` is the whole detector, and it is sound *because* this request
        // shape has exactly one caller-supplied field that can be refused: `query`,
        // `workspace_id` and `mode` are fixed and valid on every call, and the harness sends no
        // `limit`. The earlier detector also required the response to echo `consistency_token`,
        // which recall never does — so it could not observe its own failure (§80.1) and stayed
        // green through the expired-token refusals that made every card 16 replay unanswerable.
        if recall_body.contains("INVALID_INPUT") {
            g.ryw_token_rejections += 1;
        }
    }
}

/// The stream position the lane's freshest token actually names.
///
/// `ops.outbox` is the one table that carries `(evidence_id, stream_seq)` under a role this
/// harness can use. `private.memory_records` / `private.memory_evidence` are NOT usable here:
/// the §6.1.3 RESTRICTIVE visibility policy needs an identity context `role_maintenance` does
/// not carry, so both come back empty for every tenant (observed) — a witness built on them
/// would be a green tick over `n = 0`.
/// The `evidence_id` is bound as a `Uuid`, not as text with a `::uuid` cast: rust-postgres
/// infers the parameter type from the cast and then refuses to serialize a `&str` into it
/// (`error serializing parameter 0` — observed, and it cost one whole live run).
fn token_stream_seq(db: &mut Client, evidence_id: &str) -> Result<Option<i64>, String> {
    let Ok(evidence_id) = evidence_id.parse::<Uuid>() else {
        return Err(format!(
            "remember returned a non-uuid evidence_id: {evidence_id}"
        ));
    };
    db.query(
        "SELECT stream_seq FROM ops.outbox \
         WHERE evidence_id = $1 AND stream_seq IS NOT NULL LIMIT 1",
        &[&evidence_id],
    )
    .map_err(|e| format!("ops.outbox stream_seq: {e}"))
    .map(|rows| rows.first().map(|r| r.get::<_, i64>(0)))
}

/// §16.2: the read path routes only at the `serving` row, so this is the boundary below which a
/// recall is served by the projection and above which §15.5's PG delta overlay takes over.
fn serving_highwater(db: &mut Client, workspace: Uuid) -> Result<i64, String> {
    db.query(
        "SELECT projection_highwater FROM projection.stream_checkpoints \
         WHERE scope_id = $1 AND serving",
        &[&workspace],
    )
    .map_err(|e| format!("serving stream_checkpoints: {e}"))
    .map(|rows| rows.first().map_or(0, |r| r.get::<_, i64>(0)))
}

/// Every `stream_seq` the response surfaced as a §15.5 overlay item.
fn overlay_stream_seqs(body: &str) -> Vec<i64> {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| {
            v.pointer("/result/structuredContent/items")?
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.get("stream_seq")?.as_i64())
                        .collect()
                })
        })
        .unwrap_or_default()
}

/// After the drain, write once more per lane and replay a recall carrying **that write's own,
/// still-valid token**.
///
/// §15.5 read-your-writes, stated in the only form this pipeline can actually falsify. Three
/// earlier shapes do not work here and all three were tried:
///
/// * a marker planted in the write's body — the distill hop rewrites content through the model,
///   so nothing planted survives into the memory, and the witness goes vacuous;
/// * "every live projection point must come back" — the response is capped at the registered
///   profile's `top_k` (§55.1: the caller does not choose it), so for any lane with more than
///   `top_k` memories that claim is false against a perfectly correct system. Card 16's first
///   run tried to widen it by sending `"limit": <live point count>`, which is exactly the value
///   §55.1 forbids: the gateway answered `INVALID_INPUT` and the replay never ran at all;
/// * **replaying the load's freshest token after the drain** — a `consistency_token` has a §15.5
///   lifetime (`HUMAUX_GATEWAY_REMEMBER_TOKEN_TTL_SECONDS`, 60 s on this deployment), and
///   `--drain-secs` must exceed `--lease-secs` (120 s), so a token minted during the load is
///   *always* expired by replay time. The gateway refused it as `TokenExpired` → `INVALID_INPUT`,
///   which is correct behaviour and an unwinnable assertion. So the replay mints its own token
///   here, after the drain.
///
/// What §15.5 does promise, and what this asserts: with a token in hand, the read is served by
/// the projection up to the `serving` row's highwater and by a **PG delta overlay** above it —
/// one item per `projection.stream_log` row in `(serving_highwater, token stream_seq]`, each
/// carrying its own `stream_seq`. So the expected set is that seq range, taken from the
/// database, and a seq in it that the response does not carry is a stale read. `n` is the size
/// of the range: a lane whose overlay range is empty reports `n = 0` rather than a pass nobody
/// earned.
///
/// Runs **after** [`tenant_final`] on purpose: this write is issued after the drain, so counting
/// it in the ticket census would report the harness's own probe as an unsettled ticket.
fn replay_ryw(cfg: &Config, db: &mut Client, series: &mut Series) -> Result<(), String> {
    for (idx, lane) in cfg.tenants.iter().enumerate() {
        // Counted here, at the top, so every lane is in the denominator whatever happens below.
        // Incrementing it just before the recall (as this did) let a lane that bailed out
        // earlier leave `ryw_replays = 0`, and `at_most(..., n = 0, ...)` then read `0 <= 0` as
        // a pass — a whole assertion nobody had earned.
        series.ryw_replays += 1;
        let nonce = Uuid::new_v4().simple().to_string();
        let put = format!(
            "{{\"operation\":\"put\",\"content\":\"{} {}: the post-drain replay records that \
             a backend service must publish a readiness probe before traffic reaches it.\",\
             \"idempotency_key\":\"soak-replay-{nonce}\",\"workspace_id\":\"{}\"}}",
            lane.sentinel,
            digit_free(&nonce),
            lane.workspace_id
        );
        let (put_sample, put_body) = timed(cfg, lane, "remember", "remember", &put);
        series.samples.push(Sample {
            op: "remember.ryw_replay".to_string(),
            ..put_sample
        });
        series.responses_scanned += 1;
        if cross_tenant_hit(cfg, idx, &put_body) {
            series.cross_tenant_hits += 1;
        }
        // A post-drain write that does not land leaves nothing to read back — so this lane's
        // replay was NOT answered, and it is counted as such. Which half failed is one lookup
        // away in `latency[]`: a `remember.ryw_replay` with a `failed_calls` is the write, a
        // `recall.ryw_replay` with one is the read. Silently `continue`ing here is what let a
        // run report `ryw_replay_answered PASS n 0`.
        let (Some(token), Some(evidence_id)) = (
            json_field(&put_body, "consistency_token"),
            json_field(&put_body, "evidence_id"),
        ) else {
            series.ryw_replay_failures += 1;
            continue;
        };
        set_tenant(db, lane.tenant_id)?;
        let Some(token_seq) = token_stream_seq(db, &evidence_id)? else {
            series.ryw_replay_failures += 1;
            continue;
        };
        let expected: Vec<i64> =
            ((serving_highwater(db, lane.workspace_id)? + 1)..=token_seq).collect();
        // No `limit`: §55.1 reserves candidate depth to the registered profile, and any value
        // other than that profile's own `top_k` is `INVALID_INPUT`.
        let args = format!(
            "{{\"query\":\"readiness probe before traffic\",\"workspace_id\":\"{}\",\
             \"mode\":\"semantic\",\"consistency_token\":\"{token}\"}}",
            lane.workspace_id,
        );
        let (sample, body) = timed(cfg, lane, "recall", "recall", &args);
        let sample_ok = sample.ok;
        series.samples.push(Sample {
            op: "recall.ryw_replay".to_string(),
            ..sample
        });
        series.responses_scanned += 1;
        if cross_tenant_hit(cfg, idx, &body) {
            series.cross_tenant_hits += 1;
        }
        if sample_ok {
            let surfaced = overlay_stream_seqs(&body);
            series.ryw_resettled += i64::try_from(expected.len()).unwrap_or(i64::MAX);
            series.ryw_stale +=
                i64::try_from(expected.iter().filter(|s| !surfaced.contains(s)).count())
                    .unwrap_or(i64::MAX);
        } else {
            // Unanswerable, not stale. `latency[].operation = "recall.ryw_replay"` carries the
            // failed call count so the reason is one lookup away.
            series.ryw_replay_failures += 1;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------------------------

pub fn run(args: &[String]) -> i32 {
    let cfg = match parse_config(args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("soak: {e}");
            return 2;
        }
    };
    let dsn = match std::env::var(MAINTENANCE_DSN_ENV) {
        Ok(v) => v,
        Err(_) => {
            eprintln!("soak: missing object: ${MAINTENANCE_DSN_ENV} env var");
            return 2;
        }
    };
    // dep: PostgreSQL(role_maintenance) — HUMAUX_MAINTENANCE_PG_DSN — end-of-soak ledger reads.
    let mut db = match Client::connect(&dsn, NoTls) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("soak: missing object: PostgreSQL as role_maintenance ({e})");
            return 2;
        }
    };
    let mut series = match drive(&cfg, &mut db) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("soak: {e}");
            return 3;
        }
    };
    let assertions = evaluate(
        &series,
        cfg.max_rss_mib,
        cfg.max_db_connections,
        cfg.max_op_failure_rate,
    );
    let report = report_json(&series, &assertions, config_summary(&cfg));
    series.samples.clear();
    let rendered = serde_json::to_string_pretty(&report)
        .unwrap_or_else(|e| format!("{{\"serialize_error\":\"{e}\"}}"));
    if let Err(e) = std::fs::write(&cfg.report_path, &rendered) {
        eprintln!("soak: cannot write {}: {e}", cfg.report_path);
        return 3;
    }
    for a in &assertions {
        println!(
            "soak {} {}: {} = {} {} (n={}, at most {}){}{}",
            a.verdict(),
            a.id,
            a.id,
            a.value,
            a.unit,
            a.n,
            a.threshold,
            a.expected_red_until
                .map(|card| format!(" [expected_red_until: {card}]"))
                .unwrap_or_default(),
            if a.detail.is_null() {
                String::new()
            } else {
                format!(" {}", a.detail)
            }
        );
    }
    println!("soak: report written to {}", cfg.report_path);
    i32::from(assertions.iter().any(Assertion::blocking_failure))
}

fn config_summary(cfg: &Config) -> serde_json::Value {
    serde_json::json!({
        "tenants": cfg.tenants.iter().map(|t| t.tenant_id.to_string()).collect::<Vec<_>>(),
        "sessions_per_tenant": cfg.sessions_per_tenant,
        "duration_secs": cfg.duration.as_secs(),
        "drain_secs": cfg.drain.as_secs(),
        "think_ms": cfg.think.as_millis(),
        "probe_every_secs": cfg.probe_every.as_secs(),
        "probe_cmds": cfg.probe_cmds.len(),
        "chaos_cmds": cfg.chaos_cmds.len(),
        "chaos_every_secs": cfg.chaos_every.map(|d| d.as_secs()),
        "chaos_grace_secs": cfg.chaos_grace.map(|d| d.as_secs()),
        "watch_pidfiles": cfg.watch_pidfiles.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        "max_op_failure_rate": cfg.max_op_failure_rate,
        "lease_secs": cfg.lease_secs,
        "max_rss_mib": cfg.max_rss_mib,
        "max_db_connections": cfg.max_db_connections,
        "qdrant": format!("{}:{}", cfg.qdrant_host, cfg.qdrant_port),
    })
}

fn drive(cfg: &Config, db: &mut Client) -> Result<Series, String> {
    let shared = Mutex::new(Shared::default());
    let chaos_starts: Mutex<Vec<Instant>> = Mutex::new(Vec::new());
    let started = Instant::now();
    let load_deadline = started + cfg.duration;
    let mut series = Series::default();
    std::thread::scope(|scope| {
        for lane_idx in 0..cfg.tenants.len() {
            for _ in 0..cfg.sessions_per_tenant {
                let shared = &shared;
                scope.spawn(move || session(cfg, lane_idx, load_deadline, shared));
            }
        }
        if let Some(every) = cfg.chaos_every {
            let chaos_starts = &chaos_starts;
            scope.spawn(move || {
                let mut turn = 0usize;
                while Instant::now() + every < load_deadline {
                    std::thread::sleep(every);
                    let cmd = &cfg.chaos_cmds[turn % cfg.chaos_cmds.len()];
                    turn += 1;
                    if let Ok(mut starts) = chaos_starts.lock() {
                        starts.push(Instant::now());
                    }
                    println!("soak: chaos step {turn}: exit_ok={}", run_shell(cmd));
                }
            });
        }
        // The monitor stays on this thread: it owns the only PostgreSQL client.
        while Instant::now() < load_deadline {
            std::thread::sleep(cfg.probe_every);
            match observe(cfg, db, started.elapsed().as_secs(), &chaos_starts) {
                Ok(o) => series.observations.push(o),
                Err(e) => eprintln!("soak: observation skipped: {e}"),
            }
        }
    });
    // Drain: no new load, chain settles, every lease a killed worker held expires.
    let drain_end = Instant::now() + cfg.drain;
    while Instant::now() < drain_end {
        std::thread::sleep(cfg.probe_every.min(drain_end - Instant::now()));
        match observe(cfg, db, started.elapsed().as_secs(), &chaos_starts) {
            Ok(o) => series.observations.push(o),
            Err(e) => eprintln!("soak: drain observation skipped: {e}"),
        }
    }
    let shared = shared
        .into_inner()
        .map_err(|e| format!("load state poisoned: {e}"))?;
    finish(cfg, db, shared, &mut series)?;
    Ok(series)
}

fn finish(
    cfg: &Config,
    db: &mut Client,
    shared: Shared,
    series: &mut Series,
) -> Result<(), String> {
    series.samples = shared.samples;
    series.responses_scanned = shared.responses_scanned;
    series.cross_tenant_hits = shared.cross_tenant_hits;
    series.ryw_checked = shared.ryw_checked;
    series.ryw_token_rejections = shared.ryw_token_rejections;
    // One Qdrant face and one runtime for every lane's §23.1② counts — see
    // [`promote_rejections`]. Built here (after the load has stopped) so nothing in the timed
    // window pays for it.
    // dep: Qdrant(*) — cfg.qdrant_host:port, §23.1② visible-face counts after the load stops.
    let face = VisibleFace::connect(&cfg.qdrant_host, cfg.qdrant_port)?;
    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("soak: runtime: {e}"))?;
    for (idx, lane) in cfg.tenants.iter().enumerate() {
        let mut f = tenant_final(db, lane)?;
        f.writes = shared.per_lane_writes.get(&idx).copied().unwrap_or(0);
        f.recalls = shared.per_lane_recalls.get(&idx).copied().unwrap_or(0);
        f.recall_dependency_unavailable = shared
            .per_lane_dep_unavailable
            .get(&idx)
            .copied()
            .unwrap_or(0);
        series.finals.push(f);
        let grading = promote_rejections(db, lane.tenant_id, &face, &rt)?;
        series.promote_checkpoints_scanned += grading.scanned;
        series.promote_candidates += grading.candidates;
        for (reason, n) in grading.reject_reasons {
            *series.promote_rejections.entry(reason).or_default() += n;
        }
    }
    // Last, so the post-drain probe write it makes is never counted by the census above.
    replay_ryw(cfg, db, series)
}

// ---------------------------------------------------------------------------------------------
// Hermetic tests — no DB, no socket. The three negative controls the card's acceptance gate
// names are injected here at the evaluator boundary, where they are repeatable.
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Card 26 (folded card-25 debt): the rehearsal's soak invocation must satisfy the real
    /// parser — every required flag (`--watch-pidfile`, `--chaos-grace-secs`,
    /// `--max-op-failure-rate`) — or card 27's rehearsal dies at argument parsing. The shell
    /// words are taken from `docs/ops/rehearse.sh` itself (TW `rehearse_v2.sh` differs from it
    /// only on the work-dir line); `${X:-d}` becomes `d`, ids become uuids, and the bearer
    /// variable becomes `PATH` (always set) so no test touches the environment.
    #[test]
    fn rehearse_script_soak_invocation_parses() {
        let cfg = rehearse_soak_config();
        let names: Vec<&str> = cfg.watch_pidfiles.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["gw", "rw", "pw", "ds", "cw", "rp", "md"]);
        assert!(cfg.chaos_grace.is_some() && cfg.max_op_failure_rate > 0.0);
        // card 27 / ADR-0052: the projection runner is in the kill rotation, and the soak grades
        // all three tenants the rehearsal seeds.
        assert!(
            cfg.chaos_cmds
                .iter()
                .any(|c| c.ends_with("soak_chaos_rp.sh"))
        );
        assert_eq!(cfg.tenants.len(), 3);
    }

    /// ADR-0062 D-T (card 35): the resident maintenance daemon is in the kill rotation — its chaos hook is passed to
    /// the soak, its pidfile is watched, and the hook kill -9s exactly that pidfile's `humaux-maintenance` and
    /// restarts `--serve` from the one `$MD_ENV` definition. Fault: drop it from the rotation list ⇒ red.
    #[test]
    fn rotation_includes_the_maintenance_daemon() {
        let cfg = rehearse_soak_config();
        assert!(
            cfg.chaos_cmds
                .iter()
                .any(|c| c.ends_with("/soak_chaos_md.sh")),
            "chaos rotation: {:?}",
            cfg.chaos_cmds
        );
        assert!(
            cfg.watch_pidfiles
                .iter()
                .any(|(n, p)| n == "md" && p.ends_with("md.pid")),
            "watched: {:?}",
            cfg.watch_pidfiles
        );
        let script = rehearse_script();
        let hook = script
            .split("cat > $S/soak_chaos_md.sh <<EOF")
            .nth(1)
            .and_then(|rest| rest.split("\nEOF\n").next())
            .expect("the soak_chaos_md.sh heredoc");
        assert!(
            hook.contains("own_signal $S/md.pid humaux-maintenance 9"),
            "kill -9 by pidfile only: {hook}"
        );
        assert!(
            hook.contains("$MD_ENV") && hook.contains("humaux-maintenance --serve"),
            "restarts the same daemon: {hook}"
        );
        assert!(
            hook.contains("echo \\$! > $S/md.pid"),
            "pidfile kept current: {hook}"
        );
    }

    fn rehearse_script() -> String {
        std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/ops/rehearse.sh"),
        )
        .expect("docs/ops/rehearse.sh")
    }

    /// The rehearsal's soak invocation through the real parser (see the test above).
    fn rehearse_soak_config() -> Config {
        let script = rehearse_script();
        let start = script
            .find("cargo run -q -p xtask -- soak")
            .expect("soak invocation");
        let end = start + script[start..].find(" 2>&1").expect("end of invocation");
        let mut text = script[start..end].replace("\\\n", " ");
        while let Some(i) = text.find("${") {
            let close = i + text[i..].find('}').expect("closing brace");
            let inner = &text[i + 2..close];
            let default = inner.split_once(":-").map_or("", |(_, d)| d).to_string();
            text.replace_range(i..=close, &default);
        }
        let uuid = |n: u8| format!("00000000-0000-0000-0000-0000000000{n:02}");
        for (var, val) in [
            ("$TENANT_B", uuid(3)),
            ("$TENANT_C", uuid(5)),
            ("$TENANT", uuid(1)),
            ("$WS_B", uuid(4)),
            ("$WS_C", uuid(6)),
            ("$WS", uuid(2)),
            ("$SOAK_SECS", "600".to_string()),
            ("$S", "/tmp/s".to_string()),
            ("$EV", "/tmp/ev".to_string()),
            ("BEARER_A", "PATH".to_string()),
            ("BEARER_B", "PATH".to_string()),
            ("BEARER_C", "PATH".to_string()),
        ] {
            text = text.replace(var, &val);
        }
        let mut words = Vec::new();
        let mut cur = String::new();
        let mut quoted = false;
        for ch in text.chars() {
            match ch {
                '"' => quoted = !quoted,
                c if c.is_whitespace() && !quoted => {
                    if !cur.is_empty() {
                        words.push(std::mem::take(&mut cur));
                    }
                }
                c => cur.push(c),
            }
        }
        words.push(cur);
        let args: Vec<String> = words
            .into_iter()
            .skip_while(|w| w != "soak")
            .skip(1)
            .collect();
        parse_config(&args).unwrap_or_else(|e| panic!("{e}\nargs: {args:?}"))
    }

    /// `applied` and `projected` move together in the healthy fixture; the tests that care
    /// about the split set them apart explicitly through [`obs_split`].
    fn obs(at: u64, issued: i64, projected: i64, conns: i64, rss: u64) -> Observation {
        obs_split(at, issued, projected, projected, conns, rss)
    }

    fn obs_split(
        at: u64,
        issued: i64,
        applied: i64,
        projected: i64,
        conns: i64,
        rss: u64,
    ) -> Observation {
        Observation {
            at_secs: at,
            watermarks: vec![StreamWatermark {
                stream: "t|private_memory|PRIVATE_MEMORY|v1".to_string(),
                issued,
                applied,
                projected,
            }],
            backlog: 0,
            db_connections: conns,
            rss_mib: Some(rss),
            probe_failures: 0,
            probes_run: 2,
            ps_ok: true,
            watched: 3,
            unexpected_absent: 0,
            expected_absent: 0,
        }
    }

    fn healthy() -> Series {
        Series {
            observations: vec![obs(10, 4, 2, 12, 300), obs(20, 9, 7, 13, 320)],
            samples: vec![
                Sample {
                    op: "remember".into(),
                    ms: 120.0,
                    ok: true,
                },
                Sample {
                    op: "remember".into(),
                    ms: 300.0,
                    ok: true,
                },
            ],
            finals: vec![TenantFinal {
                tenant: "t".into(),
                tickets_total: 9,
                tickets_max_seq: 9,
                tickets_settled: 9,
                tickets_skipped_by_policy: 2,
                ..TenantFinal::default()
            }],
            ryw_checked: 9,
            ryw_replays: 1,
            ryw_resettled: 7,
            responses_scanned: 27,
            // One checkpoint row per lane was examined for candidacy and none of them was a
            // pending candidate (the single version each family holds is already serving) —
            // `promote_candidates_admitted`'s healthy shape, n > 0 and nothing refused.
            promote_checkpoints_scanned: 1,
            promote_candidates: 0,
            ..Series::default()
        }
    }

    /// `Config` deliberately derives no `Debug` (it holds a bearer), so a failing
    /// `expect_err` must not be allowed to format it. This unwraps the error side by hand.
    fn err_of(result: Result<Config, String>, why: &str) -> String {
        match result {
            Err(e) => e,
            Ok(_) => panic!("{why}"),
        }
    }

    fn verdict(series: &Series, id: &str) -> bool {
        evaluate(series, 4096, 64, 0.0)
            .into_iter()
            .find(|a| a.id == id)
            .expect("assertion id must exist")
            .pass
    }

    #[test]
    fn a_healthy_run_passes_every_assertion() {
        let series = healthy();
        let assertions = evaluate(&series, 4096, 64, 0.0);
        assert!(
            assertions.iter().all(|a| a.pass),
            "unexpected failures: {:?}",
            assertions.iter().filter(|a| !a.pass).collect::<Vec<_>>()
        );
        // §78.2: no bare numbers — every assertion carries its unit and its n.
        assert!(assertions.iter().all(|a| !a.unit.is_empty()));
    }

    #[test]
    fn negative_control_a_stalled_projection_worker_fails_watermark_advance() {
        let mut series = healthy();
        // Issued keeps climbing (writers are alive); the worker's own applied position never
        // moves (wedged). Measured on `applied`, not on the promotion prefix.
        series.observations = vec![
            obs_split(10, 4, 2, 2, 12, 300),
            obs_split(20, 9, 2, 2, 13, 320),
        ];
        assert!(!verdict(&series, "watermark_no_stall"));
        assert_eq!(
            stalled_streams(&series.observations),
            vec!["t|private_memory|PRIVATE_MEMORY|v1".to_string()]
        );
        // A stall must not be confused with a regression: nothing went backwards here.
        assert!(verdict(&series, "watermark_monotonic"));
    }

    /// The defect the old measurement had: a worker consuming every ticket while one `FAILED`
    /// row pins §15.4's contiguous prefix. `watermark_no_stall` must stay GREEN (the worker is
    /// healthy) and `projection_promoted` must be the one that goes red — and since card 20
    /// landed the retirement that discharges the pin, that red now **fails the run**: no
    /// `expected_red_until` marker, `blocking_failure`, and the report verdict is FAIL with an
    /// empty `expected_red` list.
    #[test]
    fn a_pinned_promotion_prefix_is_not_reported_as_a_stalled_worker() {
        let mut series = healthy();
        series.observations = vec![
            obs_split(10, 4, 2, 1, 12, 300),
            obs_split(20, 22, 22, 1, 13, 320),
        ];
        series.promote_rejections =
            BTreeMap::from([("VisibleUnavailable".into(), 2), ("OpenGaps".into(), 1)]);
        series.promote_candidates = 3;
        assert!(verdict(&series, "watermark_no_stall"));
        assert!(!verdict(&series, "projection_promoted"));

        let assertions = evaluate(&series, 4096, 64, 0.0);
        let promoted = assertions
            .iter()
            .find(|a| a.id == "projection_promoted")
            .expect("projection_promoted must be reported");
        assert_eq!(promoted.expected_red_until, None);
        assert_eq!(promoted.verdict(), "FAIL");
        assert!(promoted.blocking_failure());
        assert_eq!(promoted.detail["reject_reasons"]["OpenGaps"], 1);
        assert_eq!(promoted.detail["reject_reasons"]["VisibleUnavailable"], 2);

        let report = report_json(&series, &assertions, serde_json::json!({}));
        assert_eq!(report["verdict"], "FAIL");
        assert_eq!(report["expected_red"].as_array().map(Vec::len), Some(0));
    }

    /// Card 20 folded debt 3, at the evaluator boundary. `promote_candidates_admitted` grades the
    /// candidates §16.3 would refuse over the checkpoint rows actually scanned:
    /// * healthy deployment — one version per family, all serving ⇒ 0 refused out of 1 scanned,
    ///   `candidates_pending` stated explicitly as 0, PASS (not a silent empty map);
    /// * a real pending candidate the gate refuses ⇒ FAIL, with the reasons;
    /// * a run that scanned no checkpoint row at all ⇒ FAIL-VACUOUS (ADR-0038), never PASS.
    #[test]
    fn promote_candidate_set_is_graded_over_the_rows_it_scanned() {
        let healthy = healthy();
        let a = evaluate(&healthy, 4096, 64, 0.0)
            .into_iter()
            .find(|a| a.id == "promote_candidates_admitted")
            .expect("assertion must exist");
        assert_eq!(a.verdict(), "PASS");
        assert_eq!(a.n, 1);
        assert_eq!(a.detail["candidates_pending"], 0);

        let mut refused = healthy.clone();
        refused.promote_candidates = 1;
        refused.promote_rejections = BTreeMap::from([("OpenGaps".into(), 1)]);
        let a = evaluate(&refused, 4096, 64, 0.0)
            .into_iter()
            .find(|a| a.id == "promote_candidates_admitted")
            .expect("assertion must exist");
        assert_eq!(a.verdict(), "FAIL");
        assert_eq!(a.detail["reject_reasons"]["OpenGaps"], 1);

        let mut blind = healthy;
        blind.promote_checkpoints_scanned = 0;
        let a = evaluate(&blind, 4096, 64, 0.0)
            .into_iter()
            .find(|a| a.id == "promote_candidates_admitted")
            .expect("assertion must exist");
        assert_eq!(a.verdict(), "FAIL-VACUOUS");
        assert!(a.blocking_failure());
    }

    /// The overlay seq range the replay is entitled to, parsed out of a real response shape.
    #[test]
    fn overlay_stream_seqs_are_read_from_the_items_block() {
        let body = r#"{"result":{"structuredContent":{"items":[
            {"kind":"memory","memory_id":"m1"},
            {"kind":"temporary_evidence","stream_seq":7,"processing_state":"ISSUED"},
            {"kind":"artifact_unavailable","stream_seq":8,"processing_state":"DONE"}]}}}"#;
        assert_eq!(overlay_stream_seqs(body), vec![7, 8]);
        assert!(overlay_stream_seqs("not json").is_empty());
        assert!(overlay_stream_seqs(r#"{"result":{}}"#).is_empty());
    }

    #[test]
    fn negative_control_b_double_apply_fails_exactly_once() {
        let mut series = healthy();
        series.finals[0].double_applied_memories = 1;
        assert!(!verdict(&series, "tickets_applied_once"));
        // and it is not misreported as a lost ticket
        assert!(verdict(&series, "tickets_not_lost"));
    }

    #[test]
    fn negative_control_c_leaked_connections_fails_bounded_connections() {
        let mut series = healthy();
        series.observations.push(obs(30, 9, 9, 65, 320));
        assert!(!verdict(&series, "db_connections_bounded"));
        assert!(verdict(&series, "rss_bounded"));
    }

    fn row(stream: &str, state: &str, rows: i64, max_seq: i64) -> LedgerRow {
        LedgerRow {
            stream: stream.to_owned(),
            state: state.to_owned(),
            rows,
            max_seq,
        }
    }

    #[test]
    fn a_gap_in_the_dense_ledger_is_a_lost_ticket_but_skipped_by_policy_is_not() {
        let mut series = healthy();
        // 9 issued seqs, only 8 rows present: one ticket vanished (§15.1 expected set).
        series.finals[0].tickets_missing_seq = 1;
        assert_eq!(lost_tickets(&series.finals), 1);
        assert!(!verdict(&series, "tickets_not_lost"));

        // All nine present, two declined by policy: settled, not lost.
        let clean = healthy();
        assert_eq!(lost_tickets(&clean.finals), 0);
        assert!(verdict(&clean, "tickets_not_lost"));

        // …and that census comes out of `fold_census`, not out of a hand-set field.
        let mut f = TenantFinal::default();
        fold_census(
            &[
                row(
                    "workspace|w1|private_memory|PRIVATE_MEMORY|v1",
                    "DONE",
                    7,
                    9,
                ),
                row(
                    "workspace|w1|private_memory|PRIVATE_MEMORY|v1",
                    "SKIPPED_BY_POLICY",
                    2,
                    8,
                ),
            ],
            &mut f,
        );
        assert_eq!(f.tickets_total, 9);
        assert_eq!(f.tickets_settled, 9);
        assert_eq!(f.tickets_skipped_by_policy, 2);
        assert_eq!(f.tickets_missing_seq, 0);
        assert_eq!(lost_tickets(&[f]), 0);
    }

    /// §15.1's dense range is per **stream**, and a tenant gets a second stream the moment it
    /// has a second workspace or card 18 bumps a `projection_version`. Aggregating the census
    /// per tenant hid a lost ticket behind the other stream's row count: two streams of 10 rows
    /// give `total = 20` against `max_seq = 10`, and `(10 - 20).max(0)` is 0 however many seqs
    /// are missing. This is the case that was silently green.
    #[test]
    fn a_lost_seq_in_one_stream_is_not_masked_by_a_sibling_streams_rows() {
        let a = "workspace|w1|private_memory|PRIVATE_MEMORY|v1";
        let b = "workspace|w2|private_memory|PRIVATE_MEMORY|v1";
        let mut f = TenantFinal::default();
        // Stream A lost seq 4 (9 rows, highest seq 10); stream B is intact.
        fold_census(&[row(a, "DONE", 9, 10), row(b, "DONE", 10, 10)], &mut f);
        assert_eq!(f.tickets_total, 19);
        assert_eq!(f.tickets_max_seq, 10);
        // The tenant-wide arithmetic this replaced would have scored the same census as clean:
        assert_eq!((f.tickets_max_seq - f.tickets_total).max(0), 0);
        assert_eq!(f.tickets_missing_seq, 1);
        assert_eq!(lost_tickets(std::slice::from_ref(&f)), 1);

        let mut series = healthy();
        series.finals = vec![f];
        assert!(!verdict(&series, "tickets_not_lost"));

        // Both streams intact: still clean, so the check is not simply always red.
        let mut clean = TenantFinal::default();
        fold_census(
            &[row(a, "DONE", 10, 10), row(b, "DONE", 10, 10)],
            &mut clean,
        );
        assert_eq!(clean.tickets_missing_seq, 0);
        assert_eq!(lost_tickets(&[clean]), 0);
    }

    /// The reject reasons must be whatever `evaluate_switch` returned for the candidates the
    /// database described — never a row count wearing a rejection's name. The old version
    /// returned `count(*) FROM stream_checkpoints` as `VisibleUnavailable` unconditionally, so
    /// it reported a refusal on streams whose promotion had succeeded and could never surface
    /// any of the evaluator's other variants.
    #[test]
    fn promote_reject_reasons_are_taken_from_evaluate_switch() {
        let counts = switch_rejections(&[
            PromoteCandidate {
                first_activation: true,
                open_gaps: 0,
                visible_shadow: None,
                visible_serving: None,
            },
            PromoteCandidate {
                first_activation: false,
                open_gaps: 3,
                visible_shadow: None,
                visible_serving: None,
            },
        ]);
        // Both counts unavailable ⇒ criterion ① is unevaluable on both candidates…
        assert_eq!(counts.get("VisibleUnavailable"), Some(&2));
        // …while §69's undeclared baseline only refuses the one that HAS a serving version to be
        // compared against (card 20: a first activation has no `benchmark(serving)`, ADR-0017).
        assert_eq!(counts.get("BenchmarkNotPass"), Some(&1));
        // …and only the one that actually has `projection.processing_gaps` rows carries this,
        // counted once per candidate rather than once per gap row.
        assert_eq!(counts.get("OpenGaps"), Some(&1));
        // No candidates ⇒ no refusals. The old code reported the stream count here.
        assert!(switch_rejections(&[]).is_empty());
    }

    /// ADR-0040 / card 18's folded debt, at the harness boundary: once the live counts reach
    /// [`PromoteCandidate`], `VisibleUnavailable` must disappear from the tally — for a first
    /// activation (lone candidate read-back) and for a real promotion (candidate version vs a
    /// *different* serving version). Drop the wiring in [`promote_rejections`] back to a literal
    /// `None` and this goes red on the first assertion; that is the injection this test exists
    /// for.
    #[test]
    fn a_live_visible_count_removes_visible_unavailable_from_the_promote_tally() {
        let counts = switch_rejections(&[
            // First activation: nothing serving yet, so the candidate read-back alone satisfies
            // criterion ① (ADR-0017) and the only refusal left is §69's undeclared baseline.
            PromoteCandidate {
                first_activation: true,
                open_gaps: 0,
                visible_shadow: Some(("v2".to_string(), 100)),
                visible_serving: None,
            },
            // A genuine promotion: two different versions, counted separately, equal.
            PromoteCandidate {
                first_activation: false,
                open_gaps: 0,
                visible_shadow: Some(("v2".to_string(), 100)),
                visible_serving: Some(("v1".to_string(), 100)),
            },
        ]);
        assert_eq!(
            counts.get("VisibleUnavailable"),
            None,
            "a taken count must not still be reported as an unavailable one"
        );
        // Card 20: the first activation is now admitted outright — no serving version means no
        // baseline to be worse than — so only the real promotion carries §69's refusal.
        assert_eq!(counts.get("BenchmarkNotPass"), Some(&1));
        assert_eq!(counts.get("OpenGaps"), None);

        // The other direction, unchanged: a count that truly could not be taken (no placement
        // row, Qdrant unreachable, the user-private blind spot) is still a refusal, on either
        // side of the pair.
        let shadow_missing = switch_rejections(&[PromoteCandidate {
            first_activation: false,
            open_gaps: 0,
            visible_shadow: None,
            visible_serving: Some(("v1".to_string(), 100)),
        }]);
        assert_eq!(shadow_missing.get("VisibleUnavailable"), Some(&1));
        let serving_missing = switch_rejections(&[PromoteCandidate {
            first_activation: false,
            open_gaps: 0,
            visible_shadow: Some(("v2".to_string(), 100)),
            visible_serving: None,
        }]);
        assert_eq!(serving_missing.get("VisibleUnavailable"), Some(&1));
    }

    /// ADR-0038: *an assertion whose denominator can silently reach zero is not an assertion*.
    /// Card 16's own final run shipped `ryw_settled_write_visible PASS value 0.0 n 0`.
    #[test]
    fn an_assertion_with_no_witness_is_vacuous_not_a_pass() {
        let mut series = healthy();
        // Every lane's post-drain replay bailed out before the overlay range was computed.
        series.ryw_resettled = 0;
        series.ryw_stale = 0;
        let assertions = evaluate(&series, 4096, 64, 0.0);
        let a = assertions
            .iter()
            .find(|a| a.id == "ryw_settled_write_visible")
            .expect("assertion id must exist");
        assert_eq!(a.value, 0.0);
        assert_eq!(a.n, 0);
        assert!(!a.pass, "0 <= 0 with n = 0 must not be a pass");
        assert_eq!(a.verdict(), "FAIL-VACUOUS");
        assert!(a.blocking_failure(), "a vacuous witness must fail the run");
        let report = report_json(&series, &assertions, serde_json::json!({}));
        assert_eq!(report["verdict"], "FAIL");

        // The same guard on the other RYW assertion, whose denominator is the replay count.
        let mut none_attempted = healthy();
        none_attempted.ryw_replays = 0;
        assert!(!verdict(&none_attempted, "ryw_replay_answered"));
    }

    #[test]
    fn an_unsettled_ticket_after_the_drain_is_a_failure() {
        let mut series = healthy();
        series.finals[0].tickets_in_flight = 1;
        assert!(!verdict(&series, "tickets_settled_after_drain"));
    }

    #[test]
    fn a_live_lease_on_a_processing_row_after_the_drain_is_a_failure() {
        let mut series = healthy();
        series.finals[0].live_leases_processing = 1;
        assert!(!verdict(&series, "no_live_lease_after_drain"));
    }

    /// Card 27: two workspaces of one tenant are two streams, never one label — otherwise the
    /// monotonic check compares one workspace's watermark with the other's.
    #[test]
    fn two_workspaces_of_one_tenant_are_two_stream_labels() {
        let t = Uuid::from_u128(27);
        let a = stream_label(
            t,
            [
                "workspace",
                "ws-1",
                "private_memory",
                "PRIVATE_MEMORY",
                "v1",
            ],
        );
        let b = stream_label(
            t,
            [
                "workspace",
                "ws-2",
                "private_memory",
                "PRIVATE_MEMORY",
                "v1",
            ],
        );
        assert_ne!(a, b);
    }

    /// ADR-0052: a projection ticket still leased after the drain fails the run (the kill
    /// rotation's `--serve` restart must have re-claimed and settled everything its victim held).
    #[test]
    fn a_live_ticket_lease_after_the_drain_is_a_failure() {
        let mut series = healthy();
        assert!(verdict(&series, "no_live_ticket_lease_after_drain"));
        series.finals[0].live_ticket_leases = 1;
        assert!(!verdict(&series, "no_live_ticket_lease_after_drain"));
    }

    #[test]
    fn a_watermark_that_goes_backwards_is_a_regression() {
        let mut series = healthy();
        series.observations.push(obs(30, 9, 3, 12, 300));
        // (`obs` moves `applied` with `projected`, so this is one regression, not two.)
        assert_eq!(watermark_regressions(&series.observations), 1);
        assert!(!verdict(&series, "watermark_monotonic"));
    }

    #[test]
    fn a_failed_probe_run_and_a_cross_tenant_hit_and_a_token_rejection_each_go_red() {
        let mut series = healthy();
        series.observations[0].probe_failures = 1;
        assert!(!verdict(&series, "probes_green"));

        let mut series = healthy();
        series.cross_tenant_hits = 1;
        assert!(!verdict(&series, "no_cross_tenant_row"));

        let mut series = healthy();
        series.ryw_token_rejections = 1;
        assert!(!verdict(&series, "ryw_token_honoured"));

        let mut series = healthy();
        series.ryw_stale = 1;
        assert!(!verdict(&series, "ryw_settled_write_visible"));
    }

    #[test]
    fn an_unanswerable_replay_is_reported_as_unanswered_not_as_a_stale_read() {
        let mut series = healthy();
        series.ryw_replay_failures = 1;
        assert!(!verdict(&series, "ryw_replay_answered"));
        // The staleness verdict must NOT be dragged red by a transport failure: they are
        // different defects and folding them together is how a soak lies about a system.
        assert!(verdict(&series, "ryw_settled_write_visible"));
    }

    #[test]
    fn an_operation_whose_every_call_failed_still_appears_in_the_report() {
        let mut series = healthy();
        series.samples.push(Sample {
            op: "recall.ryw_replay".into(),
            ms: 9.0,
            ok: false,
        });
        let report = report_json(
            &series,
            &evaluate(&series, 4096, 64, 1.0),
            serde_json::json!({}),
        );
        let row = report["latency"]
            .as_array()
            .expect("latency")
            .iter()
            .find(|r| r["operation"] == "recall.ryw_replay")
            .expect("a totally failing operation must still be listed");
        assert_eq!(row["n"], 0);
        assert_eq!(row["failed_calls"], 1);
    }

    #[test]
    fn percentiles_are_nearest_rank_and_empty_is_zero() {
        let v = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        assert!((percentile(&v, 0.50) - 5.0).abs() < f64::EPSILON);
        assert!((percentile(&v, 0.95) - 10.0).abs() < f64::EPSILON);
        assert!((percentile(&[], 0.95)).abs() < f64::EPSILON);
    }

    #[test]
    fn the_report_carries_n_and_unit_for_every_number_and_keeps_skips_out_of_lost() {
        let series = healthy();
        let assertions = evaluate(&series, 4096, 64, 0.0);
        let report = report_json(&series, &assertions, serde_json::json!({}));
        assert_eq!(report["verdict"], "PASS");
        for a in report["assertions"].as_array().expect("assertions array") {
            assert!(
                a["unit"].is_string() && a["n"].is_number() && a["threshold_at_most"].is_number()
            );
        }
        let tenant = &report["tenants"][0];
        assert!(
            (tenant["skipped_by_policy_rate"]["value"]
                .as_f64()
                .expect("rate")
                - 2.0 / 9.0)
                .abs()
                < 1e-9
        );
        assert_eq!(tenant["skipped_by_policy_rate"]["n"], 9);
        assert_eq!(tenant["tickets"]["unit"], "tickets");
        let latency = &report["latency"][0];
        assert_eq!(latency["unit"], "ms");
        assert_eq!(latency["n"], 2);
    }

    #[test]
    fn config_refuses_one_tenant_a_short_drain_and_a_half_configured_chaos_hook() {
        let base: Vec<String> = [
            "--gateway-url", "http://127.0.0.1:8080/mcp",
            "--tenant", "11111111-1111-1111-1111-111111111111:22222222-2222-2222-2222-222222222222:HUMAUX_SOAK_TEST_BEARER",
            "--sessions-per-tenant", "2", "--duration-secs", "60", "--drain-secs", "10",
            "--think-ms", "100", "--probe-every-secs", "5", "--probe-cmd", "true",
            "--lease-secs", "120", "--max-rss-mib", "4096", "--max-db-connections", "64",
            "--report", "/dev/null", "--watch-pidfile", "gateway=/tmp/soak-test-gw.pid",
            "--max-op-failure-rate", "0.05",
        ]
        .iter()
        .map(|s| (*s).to_string())
        .collect();
        // SAFETY: single-threaded test setup before any other thread reads the environment.
        unsafe { std::env::set_var("HUMAUX_SOAK_TEST_BEARER", "not-a-real-token") };

        let err = err_of(parse_config(&base), "one tenant must be refused");
        assert!(err.contains("at least two tenants"), "{err}");

        let mut two = base.clone();
        two.push("--tenant".to_string());
        two.push("33333333-3333-3333-3333-333333333333:44444444-4444-4444-4444-444444444444:HUMAUX_SOAK_TEST_BEARER".to_string());
        let err = err_of(parse_config(&two), "drain <= lease must be refused");
        assert!(err.contains("must exceed --lease-secs"), "{err}");

        let mut ok = two.clone();
        for (i, v) in ok.iter_mut().enumerate() {
            if two.get(i.wrapping_sub(1)).map(String::as_str) == Some("--drain-secs") {
                *v = "150".to_string();
            }
        }
        assert!(
            parse_config(&ok).is_ok(),
            "a two-tenant config with drain > lease must parse"
        );

        let mut half_chaos = ok.clone();
        half_chaos.push("--chaos-cmd".to_string());
        half_chaos.push("true".to_string());
        let err = err_of(
            parse_config(&half_chaos),
            "chaos cmd without period must be refused",
        );
        assert!(err.contains("must be given together"), "{err}");

        // ADR-0050 D-I: chaos without a grace window, a missing watch list and a missing
        // failure-rate ceiling are each refused, never defaulted.
        let mut no_grace = ok.clone();
        no_grace.extend(["--chaos-cmd", "true", "--chaos-every-secs", "30"].map(String::from));
        let err = err_of(
            parse_config(&no_grace),
            "chaos without grace must be refused",
        );
        assert!(err.contains("--chaos-grace-secs"), "{err}");
        let mut graced = no_grace.clone();
        graced.extend(["--chaos-grace-secs", "20"].map(String::from));
        assert!(parse_config(&graced).is_ok(), "chaos + grace must parse");

        let without = |flag: &str| -> Vec<String> {
            let mut out = Vec::new();
            let mut skip = false;
            for a in &ok {
                if skip {
                    skip = false;
                } else if a == flag {
                    skip = true;
                } else {
                    out.push(a.clone());
                }
            }
            out
        };
        let err = err_of(
            parse_config(&without("--watch-pidfile")),
            "no watched process must be refused",
        );
        assert!(err.contains("--watch-pidfile"), "{err}");
        let err = err_of(
            parse_config(&without("--max-op-failure-rate")),
            "no failure-rate ceiling must be refused",
        );
        assert!(err.contains("--max-op-failure-rate"), "{err}");
    }

    fn ps_table(rows: &[(u32, u64, &str)]) -> BTreeMap<u32, (u64, String)> {
        rows.iter()
            .map(|(pid, rss, comm)| (*pid, (*rss, (*comm).to_string())))
            .collect()
    }

    #[test]
    fn soak_ps_failure_is_an_assertion_failure_not_rss_zero() {
        assert!(parse_ps_table("  101 2048 humaux-gateway\n", false).is_err());
        assert!(
            parse_ps_table("", true).is_err(),
            "empty table is a failure"
        );
        let table = parse_ps_table("  101 2048 humaux-gateway\n  7 10 /sbin/launchd\n", true)
            .expect("a real table parses");
        assert_eq!(table[&101], (2048, "humaux-gateway".to_string()));

        let mut series = healthy();
        for o in &mut series.observations {
            o.ps_ok = false;
            o.rss_mib = None;
            o.watched = 0;
        }
        assert!(!verdict(&series, "ps_observed"));
        let rss = evaluate(&series, 4096, 64, 0.0)
            .into_iter()
            .find(|a| a.id == "rss_bounded")
            .expect("rss_bounded");
        assert!(
            !rss.pass && rss.n == 0,
            "no reading is FAIL-VACUOUS, never 0 MiB = pass"
        );
    }

    #[test]
    fn soak_expected_chaos_absence_is_not_a_probe_failure() {
        let start = Instant::now();
        let now = start + Duration::from_secs(5);
        let table = ps_table(&[(101, 2048, "humaux-gateway")]);
        let grace = Some(Duration::from_secs(10));
        assert_eq!(
            classify_presence(Some(202), &table, now, &[start], grace),
            Presence::ExpectedAbsent
        );
        assert_eq!(
            classify_presence(Some(101), &table, now, &[start], grace),
            Presence::Present
        );
        let mut series = healthy();
        series.observations[0].expected_absent = 1;
        assert!(verdict(&series, "probes_green"));
    }

    #[test]
    fn soak_unexpected_absence_is_a_probe_failure() {
        let start = Instant::now();
        let table = ps_table(&[(101, 2048, "humaux-gateway")]);
        // After the grace window, and with no chaos at all, an absence is unexpected.
        let late = start + Duration::from_secs(30);
        let grace = Some(Duration::from_secs(10));
        assert_eq!(
            classify_presence(Some(202), &table, late, &[start], grace),
            Presence::UnexpectedAbsent
        );
        assert_eq!(
            classify_presence(None, &table, late, &[], None),
            Presence::UnexpectedAbsent
        );
        let mut series = healthy();
        series.observations[1].unexpected_absent = 1;
        assert!(!verdict(&series, "probes_green"));
    }

    #[test]
    fn soak_reused_pid_with_foreign_comm_counts_as_absent() {
        let table = ps_table(&[(101, 2048, "/usr/bin/some-other-daemon")]);
        assert_eq!(
            classify_presence(Some(101), &table, Instant::now(), &[], None),
            Presence::UnexpectedAbsent
        );
    }

    #[test]
    fn presence_matches_executable_basename_not_path() {
        let table = ps_table(&[
            (101, 2048, "/Users/x/humaux-target-boot/debug/deps/foo-1a2b"),
            (
                102,
                2048,
                "/Users/x/humaux-target-boot/debug/humaux-gateway",
            ),
            (103, 2048, "humaux-retrieva"),
        ]);
        let at = |pid| classify_presence(Some(pid), &table, Instant::now(), &[], None);
        assert_eq!(
            at(101),
            Presence::UnexpectedAbsent,
            "path contains humaux-, basename does not"
        );
        assert_eq!(at(102), Presence::Present);
        assert_eq!(at(103), Presence::Present, "Linux 15-char comm");
    }

    fn sample(op: &str, ok: bool) -> Sample {
        Sample {
            op: op.into(),
            ms: 1.0,
            ok,
        }
    }

    #[test]
    fn soak_op_failure_rate_scored_per_op_against_threshold() {
        // 1/10 recalls failed, 0/10 writes: the worst op (recall) is what is scored, so a
        // healthy op cannot dilute a failing one.
        let mut samples: Vec<Sample> = (0..10).map(|_| sample("remember", true)).collect();
        samples.extend((0..9).map(|_| sample("recall", true)));
        samples.push(sample("recall", false));
        let a = op_failure_rate(&samples, 0.05);
        assert!(!a.pass, "{a:?}");
        assert!((a.value - 0.1).abs() < 1e-9);
        assert_eq!(a.n, 20);
        assert!(a.detail.to_string().contains("\"op\":\"recall\""));
        assert!(op_failure_rate(&samples, 0.1).pass);
    }

    #[test]
    fn soak_buckets_split_memory_get_from_memory_enumerate() {
        assert_ne!(OP_MEMORY_GET, OP_MEMORY_ENUMERATE);
        let recall = r#"{"result":{"structuredContent":{"items":[
            {"kind":"temporary_evidence","evidence_id":"e1"},
            {"kind":"memory","memory_id":"m1"},{"kind":"memory","memory_id":"m2"}]}}}"#;
        let args = memory_get_args(recall, "w1").expect("first memory item");
        let parsed: serde_json::Value = serde_json::from_str(&args).expect("json args");
        assert_eq!(parsed["action"], "get");
        assert_eq!(parsed["memory_id"], "m1");
        assert_eq!(parsed["workspace_id"], "w1");
        let empty = r#"{"result":{"structuredContent":{"items":[]}}}"#;
        assert_eq!(memory_get_args(empty, "w1"), None, "skipped, not failed");
        let series = Series {
            samples: vec![
                sample(OP_MEMORY_ENUMERATE, true),
                sample(OP_MEMORY_GET, true),
            ],
            ..Series::default()
        };
        let report = report_json(&series, &[], serde_json::json!({}));
        let ops: Vec<&str> = report["latency"]
            .as_array()
            .expect("latency")
            .iter()
            .map(|row| row["operation"].as_str().expect("op"))
            .collect();
        assert_eq!(ops, vec![OP_MEMORY_ENUMERATE, OP_MEMORY_GET]);
        assert!(
            !ops.contains(&"memory"),
            "the §4.1 mislabelled bucket is gone"
        );
    }

    #[test]
    fn recall_stage_samples_come_from_provenance_stage_ms_and_skip_failed_recalls() {
        let body = serde_json::json!({"result":{"structuredContent":{"provenance":{"stage_ms":{
            "route":1.0,"planner":0.5,"scan":200.0,"embed":250.0,"qdrant":10.0,
            "hydrate":20.0,"rerank":0.5,"assemble":18.0,"total":500.0}}}}})
        .to_string();
        let got = recall_stage_samples(&sample("recall", true), &body);
        assert_eq!(got.len(), 9);
        assert_eq!(got[2].op, "recall.stage.scan");
        assert!((got[2].ms - 200.0).abs() < 1e-9);
        assert_eq!(got[8].op, "recall.stage_sum");
        assert!((got[8].ms - 500.0).abs() < 1e-9);
        assert!(recall_stage_samples(&sample("recall", false), &body).is_empty());
        let partial = body.replace("\"rerank\":0.5,", "");
        assert!(
            recall_stage_samples(&sample("recall", true), &partial).is_empty(),
            "a missing stage is not a zero"
        );
    }

    #[test]
    fn op_failure_rate_ignores_derived_stage_samples() {
        let mut samples: Vec<Sample> = (0..10).map(|_| sample("recall", true)).collect();
        samples.extend((0..90).map(|_| sample("recall.stage.scan", true)));
        samples.push(sample("recall.stage_sum", false));
        let a = op_failure_rate(&samples, 0.0);
        assert!(a.pass, "{a:?}");
        assert_eq!(a.n, 10);
        assert!(!a.detail.to_string().contains("recall.stage"));
    }

    #[test]
    fn soak_op_failure_rate_with_zero_samples_is_vacuous_fail() {
        let a = op_failure_rate(&[], 1.0);
        assert!(!a.pass && a.n == 0);
        assert_eq!(a.verdict(), "FAIL-VACUOUS");
    }

    #[test]
    fn missing_flags_and_a_bad_url_are_refused_rather_than_defaulted() {
        // §78.1: every threshold is a required flag; nothing here has a default to fall back on.
        let err = err_of(parse_config(&[]), "empty argv must be refused");
        assert!(err.contains("--gateway-url"), "{err}");
        let err = split_url("https://example.test/mcp").expect_err("only http is supported");
        assert!(err.contains("loopback"), "{err}");
        let (host, path, origin) = split_url("http://127.0.0.1:8080/mcp").expect("split");
        assert_eq!(
            (host.as_str(), path.as_str(), origin.as_str()),
            ("127.0.0.1:8080", "/mcp", "http://127.0.0.1:8080")
        );
    }

    #[test]
    fn the_request_carries_the_mcp_headers_and_the_bearer_never_reaches_the_template() {
        let wire = mcp_request(
            "127.0.0.1:8080",
            "/mcp",
            "http://127.0.0.1:8080",
            "recall",
            "{\"q\":1}",
        );
        for needle in [
            "POST /mcp HTTP/1.1",
            "Host: 127.0.0.1:8080",
            "MCP-Protocol-Version: 2026-07-28",
            "Mcp-Name: recall",
            "Origin: http://127.0.0.1:8080",
            "Connection: close",
            "\"method\":\"tools/call\"",
            "\"name\":\"recall\"",
        ] {
            assert!(wire.contains(needle), "missing {needle} in\n{wire}");
        }
        // The template carries a placeholder, never a credential.
        assert!(wire.contains("Authorization: Bearer {BEARER}"));
        let len: usize = wire
            .split("Content-Length: ")
            .nth(1)
            .and_then(|s| s.split("\r\n").next())
            .and_then(|s| s.parse().ok())
            .expect("content-length");
        assert_eq!(len, wire.split("\r\n\r\n").nth(1).expect("body").len());
    }

    #[test]
    fn http_responses_parse_with_and_without_chunked_framing() {
        let plain = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n{}";
        assert_eq!(parse_http(plain).expect("plain"), (200, "{}".to_string()));
        let chunked = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n";
        assert_eq!(
            parse_http(chunked).expect("chunked"),
            (200, "{}".to_string())
        );
        let refused = b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n";
        assert_eq!(parse_http(refused).expect("503").0, 503);
        assert!(parse_http(b"garbage").is_err());
    }

    #[test]
    fn soak_markers_never_carry_a_phone_like_digit_run() {
        // The nonce that failed the 2026-09-29 chain; its `3831887` is 7 digits.
        let marker = super::digit_free("2faf14fe7d5243c3831887ce21e2447d");
        assert!(!marker.chars().any(|c| c.is_ascii_digit()), "{marker}");
        assert_eq!(marker.len(), 32);
        assert_ne!(
            super::digit_free("0a"),
            super::digit_free("1a"),
            "stays injective"
        );
    }

    #[test]
    fn a_response_carrying_another_lanes_sentinel_or_ids_is_a_cross_tenant_hit() {
        let cfg = Config {
            host_port: "127.0.0.1:8080".into(),
            path: "/mcp".into(),
            origin: "http://127.0.0.1:8080".into(),
            tenants: vec![
                TenantLane {
                    tenant_id: Uuid::from_u128(1),
                    workspace_id: Uuid::from_u128(2),
                    bearer: "a".into(),
                    sentinel: "soak-sentinel-aaa".into(),
                },
                TenantLane {
                    tenant_id: Uuid::from_u128(3),
                    workspace_id: Uuid::from_u128(4),
                    bearer: "b".into(),
                    sentinel: "soak-sentinel-bbb".into(),
                },
            ],
            sessions_per_tenant: 1,
            duration: Duration::from_secs(1),
            drain: Duration::from_secs(2),
            think: Duration::from_millis(1),
            probe_every: Duration::from_secs(1),
            probe_cmds: vec!["true".into()],
            watch_pidfiles: vec![("gateway".into(), PathBuf::from("/dev/null"))],
            chaos_every: None,
            chaos_grace: None,
            chaos_cmds: vec![],
            lease_secs: 1,
            max_rss_mib: 1,
            max_db_connections: 1,
            max_op_failure_rate: 0.0,
            report_path: "/dev/null".into(),
            qdrant_host: "127.0.0.1".into(),
            qdrant_port: 6333,
        };
        assert!(!cross_tenant_hit(
            &cfg,
            0,
            "{\"memories\":[\"soak-sentinel-aaa x\"]}"
        ));
        assert!(cross_tenant_hit(
            &cfg,
            0,
            "{\"memories\":[\"soak-sentinel-bbb x\"]}"
        ));
        assert!(cross_tenant_hit(
            &cfg,
            0,
            &format!("{{\"t\":\"{}\"}}", Uuid::from_u128(3))
        ));
        assert!(cross_tenant_hit(
            &cfg,
            0,
            &format!("{{\"w\":\"{}\"}}", Uuid::from_u128(4))
        ));
        assert!(!cross_tenant_hit(
            &cfg,
            1,
            "{\"memories\":[\"soak-sentinel-bbb x\"]}"
        ));
    }

    #[test]
    fn the_consistency_token_is_read_from_the_structured_content() {
        let body =
            r#"{"result":{"structuredContent":{"consistency_token":"tok-1","accepted":true}}}"#;
        assert_eq!(
            json_field(body, "consistency_token"),
            Some("tok-1".to_string())
        );
        assert_eq!(json_field(r#"{"result":{}}"#, "consistency_token"), None);
        assert_eq!(json_field("not json", "consistency_token"), None);
    }
}
