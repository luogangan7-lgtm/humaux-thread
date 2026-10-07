//! `xtask::load` — `cargo xtask load`: closed-loop keep-alive load against one gateway, phase by phase, graded by the ADR-0065 D-F assertions.
//! Depends-on: crates=[humaux-adapters, humaux-protocol, postgres, serde_json, time]; services=[PostgreSQL(any) r=[control.rate_buckets,
//!   control.workspaces], HTTP(gateway), subprocess(docker), subprocess(sh), subprocess(sysctl)]; env=[];
//!   modules=[adapters::quota_repo, xtask::soak]
//! Called-by: [xtask::main]
//! Invariants: [no load-report.json and a non-zero exit unless the gateway's /readyz answered 200 before the first phase and
//!   after every restart (assertion 0); every number in the report and every LOAD / ISO / ISO_NOISE line carries its n,
//!   its unit, host= and measured_at=; admission 503s (body RATE_LIMITED) are counted apart from failed_calls; a 503
//!   pauses its client for Retry-After seconds; the database sessions are READ ONLY and read pg_stat_activity,
//!   pg_locks, control.rate_buckets and control.workspaces only]
//! Spec: Baseline §67.2; §72.3; §73.2; §78.1; ADR-0065
//!
//! The measurement harness of card 38 (ADR-0065 D-F). The rehearsal (`docs/ops/rehearse.sh`, step `load`) starts a
//! scratch gateway, seeds two load tenants (LA with 16 workspace-bound keys, LB with one) and runs this subcommand
//! against it. The op mix is `memory.enumerate` 70% / `tools/list` 30%: neither reaches a provider, the outbox or
//! Qdrant (ADR-0065 D-F rejected (e)), so a run spends no money and leaves nothing for the projection runner.
//!
//! Phases, in order: `L<n>` for each level (LA, 16 keys round-robin, warm-up then measured window), `BURST` (LA),
//! `ISO-BASE-<i>` / `ISO-BURST-<i>` pairs (LB alone, then LA on ONE key plus LB), and `BURST-W` (LA, after a restart
//! command given with `--restart BURST-W=<sh>`; skipped without one). Each phase scrapes the gateway's ops `/metrics`
//! before and after it and samples `pg_stat_activity` at 1 Hz on its own read-only connection; a second read-only
//! connection samples the rate advisory-lock waits of `role_gateway` every `--lock-sample-ms` (the `LOCKWAIT` line:
//! ADR-0065 D-D's measure of the tenant lock's cost).
//!
//! The S7 measurement-only runs (ADR-0065 "Measurement": the C sweep and the load faults) reuse the same plan:
//! `--only <prefix,..>` keeps the phases whose name starts with one of the prefixes, and `--rfc7239-lane <letter>`
//! appends phase `FWD`, whose clients send the RFC 7239 quoted form `for="[…]"` the edge parser refuses (fault F-FWD).
//! Assertion 8 grades exactly the lanes the run's phases used.
//!
//! Every client keeps one HTTP/1.1 connection alive (a fresh socket per call exhausts macOS's ephemeral ports near
//! 546 connections/s, design F14) and sends the bare `Forwarded: 2001:db8:<lane>::<n>` form, the only one the edge
//! parser accepts (`crates/protocol/src/edge.rs`), so each lane keys into its own preauth /64 bucket (assertion 8).
//!
//! §78.1: every duration, ceiling and endpoint is a required flag; the bearers are named by env var and never printed.
//! The gateway is dialed with std's `TcpStream` for the reason `xtask::soak` gives (no HTTP dependency in xtask).

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use postgres::{Client, NoTls};

use crate::soak::{mcp_request, percentile};

const USAGE: &str = "usage: cargo xtask load \
--gateway-url http://HOST:PORT/mcp --ops-url http://HOST:PORT/metrics \
--lane <letter>=<workspace_uuid>:<BEARER_ENV_VAR> (repeat; lanes a and b required) \
--levels <n,n,..> --burst <n> --warmup-secs <n> --level-secs <n> --burst-secs <n> \
--iso-pairs <n> --iso-secs <n> --iso-base-clients <n> --iso-burst-clients <n> --burst-w-secs <n> \
--max-wait-ms <n> --burst-w-max-wait-ms <n> --handler-timeout-secs <n> --pool-max <n> --max-backends <n> \
--iso-floor-ms <n> --ready-timeout-secs <n> --lock-sample-ms <n> --pg-dsn-env <VAR> --pg-container <name> --report <path> \
[--restart <PHASE>=<sh> (repeat)] [--only <PHASE_PREFIX,..>] [--rfc7239-lane <letter>]";

/// Share of `memory.enumerate` in each client's closed loop, in tenths (the rest is `tools/list`).
const ENUMERATE_TENTHS: usize = 7;

/// One workspace-bound credential: the bearer is read from the env var the flag names.
#[derive(Clone)]
pub struct Key {
    workspace: String,
    bearer: String,
}

/// Parsed flags (§78.1: all required except `--restart`).
pub struct Config {
    host_port: String,
    path: String,
    origin: String,
    ops_host_port: String,
    ops_path: String,
    lanes: BTreeMap<char, Vec<Key>>,
    levels: Vec<usize>,
    burst: usize,
    warmup: Duration,
    level: Duration,
    burst_secs: Duration,
    iso_pairs: usize,
    iso: Duration,
    iso_base_clients: usize,
    iso_burst_clients: usize,
    burst_w: Duration,
    max_wait_ms: f64,
    burst_w_max_wait_ms: f64,
    handler_timeout: Duration,
    pool_max: i64,
    max_backends: i64,
    iso_floor_ms: f64,
    ready_timeout: Duration,
    /// Interval of the rate advisory-lock wait sampler: the resolution of every `LOCKWAIT` number.
    lock_sample: Duration,
    pg_dsn: Option<String>,
    pg_container: String,
    restarts: BTreeMap<String, String>,
    report_path: String,
    /// Phase-name prefixes to run; empty = the whole plan.
    only: Vec<String>,
    /// The lane whose clients send the RFC 7239 quoted form (S7 fault F-FWD); it gets the extra phase `FWD`.
    rfc7239_lane: Option<char>,
}

fn values(args: &[String], flag: &str) -> Vec<String> {
    args.iter()
        .enumerate()
        .filter(|(_, a)| a.as_str() == flag)
        .filter_map(|(i, _)| args.get(i + 1).cloned())
        .collect()
}

fn one(args: &[String], flag: &str) -> Result<String, String> {
    values(args, flag)
        .pop()
        .ok_or_else(|| format!("missing required flag {flag} (§78.1: no default)"))
}

fn num(args: &[String], flag: &str) -> Result<u64, String> {
    one(args, flag)?.parse().map_err(|e| format!("{flag}: {e}"))
}

fn count(args: &[String], flag: &str) -> Result<usize, String> {
    usize::try_from(num(args, flag)?).map_err(|e| format!("{flag}: {e}"))
}

/// `http://HOST:PORT/path` → (`HOST:PORT`, `/path`).
fn split_url(url: &str) -> Result<(String, String), String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("{url}: only http:// loopback URLs are supported"))?;
    let (host_port, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
    Ok((
        host_port.to_string(),
        if path.is_empty() { "/" } else { path }.to_string(),
    ))
}

fn parse_lane(raw: &str) -> Result<(char, Key), String> {
    let (letter, rest) = raw
        .split_once('=')
        .ok_or_else(|| format!("--lane {raw}: want <letter>=<workspace>:<ENV>"))?;
    let mut chars = letter.chars();
    let lane = match (chars.next(), chars.next()) {
        (Some(c), None) if c.is_ascii_hexdigit() && c.is_ascii_lowercase() => c,
        _ => {
            return Err(format!(
                "--lane {raw}: the lane is one lowercase hex letter (it becomes an IPv6 group)"
            ));
        }
    };
    let (workspace, env) = rest
        .split_once(':')
        .ok_or_else(|| format!("--lane {raw}: want <letter>=<workspace>:<ENV>"))?;
    let bearer =
        std::env::var(env).map_err(|_| format!("--lane {lane}: env var {env} is not set"))?;
    Ok((
        lane,
        Key {
            workspace: workspace.to_string(),
            bearer,
        },
    ))
}

/// Parse the flags. Lanes `a` (the bursting tenant) and `b` (the isolation witness) are required.
pub fn parse_config(args: &[String]) -> Result<Config, String> {
    let (host_port, path) = split_url(&one(args, "--gateway-url")?)?;
    let (ops_host_port, ops_path) = split_url(&one(args, "--ops-url")?)?;
    let mut lanes: BTreeMap<char, Vec<Key>> = BTreeMap::new();
    for raw in values(args, "--lane") {
        let (lane, key) = parse_lane(&raw)?;
        lanes.entry(lane).or_default().push(key);
    }
    if !lanes.contains_key(&'a') || !lanes.contains_key(&'b') {
        return Err("--lane: lanes a and b are both required".to_string());
    }
    let levels = one(args, "--levels")?
        .split(',')
        .map(|l| {
            l.trim()
                .parse::<usize>()
                .map_err(|e| format!("--levels {l}: {e}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut restarts = BTreeMap::new();
    for raw in values(args, "--restart") {
        let (phase, cmd) = raw
            .split_once('=')
            .ok_or_else(|| format!("--restart {raw}: want <PHASE>=<sh>"))?;
        restarts.insert(phase.to_string(), cmd.to_string());
    }
    let only: Vec<String> = values(args, "--only")
        .iter()
        .flat_map(|v| v.split(','))
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect();
    let rfc7239_lane = match values(args, "--rfc7239-lane").pop() {
        None => None,
        Some(l) => match l.chars().next() {
            Some(c) if l.len() == 1 && lanes.contains_key(&c) => Some(c),
            _ => return Err(format!("--rfc7239-lane {l}: not one of the --lane letters")),
        },
    };
    let dsn_env = one(args, "--pg-dsn-env")?;
    let pg_dsn =
        Some(std::env::var(&dsn_env).map_err(|_| format!("--pg-dsn-env: {dsn_env} is not set"))?);
    #[expect(
        clippy::cast_precision_loss,
        reason = "millisecond flags far below 2^52"
    )]
    let ms = |flag: &str| num(args, flag).map(|v| v as f64);
    Ok(Config {
        origin: format!("http://{host_port}"),
        host_port,
        path,
        ops_host_port,
        ops_path,
        lanes,
        levels,
        burst: count(args, "--burst")?,
        warmup: Duration::from_secs(num(args, "--warmup-secs")?),
        level: Duration::from_secs(num(args, "--level-secs")?),
        burst_secs: Duration::from_secs(num(args, "--burst-secs")?),
        iso_pairs: count(args, "--iso-pairs")?,
        iso: Duration::from_secs(num(args, "--iso-secs")?),
        iso_base_clients: count(args, "--iso-base-clients")?,
        iso_burst_clients: count(args, "--iso-burst-clients")?,
        burst_w: Duration::from_secs(num(args, "--burst-w-secs")?),
        max_wait_ms: ms("--max-wait-ms")?,
        burst_w_max_wait_ms: ms("--burst-w-max-wait-ms")?,
        handler_timeout: Duration::from_secs(num(args, "--handler-timeout-secs")?),
        pool_max: i64::try_from(num(args, "--pool-max")?)
            .map_err(|e| format!("--pool-max: {e}"))?,
        max_backends: i64::try_from(num(args, "--max-backends")?)
            .map_err(|e| format!("--max-backends: {e}"))?,
        iso_floor_ms: ms("--iso-floor-ms")?,
        ready_timeout: Duration::from_secs(num(args, "--ready-timeout-secs")?),
        lock_sample: Duration::from_millis(num(args, "--lock-sample-ms")?)
            .max(Duration::from_millis(1)),
        pg_dsn,
        pg_container: one(args, "--pg-container")?,
        restarts,
        report_path: one(args, "--report")?,
        only,
        rfc7239_lane,
    })
}

// ---------------------------------------------------------------------------------------------
// Keep-alive HTTP/1.1 client
// ---------------------------------------------------------------------------------------------

/// One parsed response.
#[derive(Debug)]
pub struct Response {
    status: u16,
    retry_after: Option<String>,
    close: bool,
    body: String,
}

/// Read exactly one response from a keep-alive stream: Content-Length, chunked, or close-delimited.
fn read_response<R: BufRead>(r: &mut R) -> Result<Response, String> {
    let mut line = String::new();
    if r.read_line(&mut line)
        .map_err(|e| format!("read status: {e}"))?
        == 0
    {
        return Err("eof before status line".to_string());
    }
    let status: u16 = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("bad status line {:?}", line.trim_end()))?;
    let (mut length, mut chunked, mut close, mut retry_after) = (None, false, false, None);
    loop {
        line.clear();
        r.read_line(&mut line)
            .map_err(|e| format!("read header: {e}"))?;
        let h = line.trim_end();
        if h.is_empty() {
            break;
        }
        let Some((name, value)) = h.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.to_ascii_lowercase().as_str() {
            "content-length" => length = value.parse::<usize>().ok(),
            "transfer-encoding" => chunked = value.to_ascii_lowercase().contains("chunked"),
            "connection" => close = value.eq_ignore_ascii_case("close"),
            "retry-after" => retry_after = Some(value.to_string()),
            _ => {}
        }
    }
    let mut body = Vec::new();
    if chunked {
        loop {
            line.clear();
            r.read_line(&mut line)
                .map_err(|e| format!("read chunk size: {e}"))?;
            let hex = line.trim_end().split(';').next().unwrap_or("");
            let size = usize::from_str_radix(hex.trim(), 16)
                .map_err(|e| format!("chunk size {hex:?}: {e}"))?;
            if size == 0 {
                // Trailers, then the empty line that ends the message.
                loop {
                    line.clear();
                    if r.read_line(&mut line)
                        .map_err(|e| format!("read trailer: {e}"))?
                        == 0
                        || line.trim_end().is_empty()
                    {
                        break;
                    }
                }
                break;
            }
            let start = body.len();
            body.resize(start + size + 2, 0);
            r.read_exact(&mut body[start..])
                .map_err(|e| format!("read chunk: {e}"))?;
            body.truncate(start + size);
        }
    } else if let Some(n) = length {
        body.resize(n, 0);
        r.read_exact(&mut body)
            .map_err(|e| format!("read body: {e}"))?;
    } else {
        close = true;
        r.read_to_end(&mut body)
            .map_err(|e| format!("read body: {e}"))?;
    }
    Ok(Response {
        status,
        retry_after,
        close,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

/// One client's connection, reopened after a close or an error.
pub struct Conn {
    host_port: String,
    io_timeout: Duration,
    stream: Option<BufReader<TcpStream>>,
}

impl Conn {
    fn new(host_port: &str, io_timeout: Duration) -> Self {
        Self {
            host_port: host_port.to_string(),
            io_timeout,
            stream: None,
        }
    }

    fn open(&self) -> Result<BufReader<TcpStream>, String> {
        // dep: HTTP(gateway) — one keep-alive loopback connection per client.
        let s = TcpStream::connect(&self.host_port).map_err(|e| format!("connect: {e}"))?;
        s.set_read_timeout(Some(self.io_timeout))
            .and_then(|()| s.set_write_timeout(Some(self.io_timeout)))
            .and_then(|()| s.set_nodelay(true))
            .map_err(|e| format!("socket options: {e}"))?;
        Ok(BufReader::new(s))
    }

    fn exchange(stream: &mut BufReader<TcpStream>, wire: &str) -> Result<Response, String> {
        stream
            .get_mut()
            .write_all(wire.as_bytes())
            .map_err(|e| format!("write: {e}"))?;
        read_response(stream)
    }

    /// Send one request. A reused connection the server closed while idle fails before any byte of the response;
    /// that request is sent again once on a fresh connection, as any HTTP/1.1 client does.
    pub fn send(&mut self, wire: &str) -> Result<Response, String> {
        let reused = self.stream.is_some();
        let mut stream = match self.stream.take() {
            Some(s) => s,
            None => self.open()?,
        };
        let mut result = Self::exchange(&mut stream, wire);
        if reused
            && matches!(&result, Err(e) if e.starts_with("eof before status") || e.starts_with("write:"))
        {
            stream = self.open()?;
            result = Self::exchange(&mut stream, wire);
        }
        if matches!(&result, Ok(r) if !r.close) {
            self.stream = Some(stream);
        }
        result
    }
}

// ---------------------------------------------------------------------------------------------
// Requests and outcomes
// ---------------------------------------------------------------------------------------------

/// The two ops of the mix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Op {
    Enumerate,
    ToolsList,
}

impl Op {
    fn label(self) -> &'static str {
        match self {
            Op::Enumerate => "enumerate",
            Op::ToolsList => "tools_list",
        }
    }
}

/// The bare `Forwarded` value of one client: `2001:db8:<lane>::<n>` (documentation range, design F20).
pub fn forwarded_for(lane: char, index: usize) -> String {
    format!("2001:db8:{lane}::{:x}", index + 1)
}

/// The wire request of one call, keep-alive, with the lane's `Forwarded` header.
fn request(target: &Target, key: &Key, forwarded: &str, op: Op) -> String {
    let wire = match op {
        Op::Enumerate => mcp_request(
            &target.host_port,
            &target.path,
            &target.origin,
            "memory",
            &format!(
                "{{\"action\":\"enumerate\",\"workspace_id\":\"{}\",\"limit\":10}}",
                key.workspace
            ),
        ),
        Op::ToolsList => {
            let body = "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{\"_meta\":{\
                \"io.modelcontextprotocol/protocolVersion\":\"2026-07-28\",\
                \"io.modelcontextprotocol/clientInfo\":{\"name\":\"xtask-load\",\"version\":\"1\"},\
                \"io.modelcontextprotocol/clientCapabilities\":{}}}}";
            format!(
                "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\n\
                 Accept: application/json, text/event-stream\r\nMCP-Protocol-Version: 2026-07-28\r\n\
                 Mcp-Method: tools/list\r\nOrigin: {}\r\nAuthorization: Bearer {{BEARER}}\r\n\
                 Content-Length: {}\r\n\r\n{body}",
                target.path,
                target.host_port,
                target.origin,
                body.len()
            )
        }
    };
    wire.replacen("Connection: close\r\n", "", 1).replacen(
        "Authorization: Bearer {BEARER}",
        &format!(
            "Forwarded: {forwarded}\r\nAuthorization: Bearer {}",
            key.bearer
        ),
        1,
    )
}

/// What one call came back as.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Ok,
    /// §67.2 admission refusal: HTTP 503 with the pre-parse body `RATE_LIMITED`.
    Refused {
        retry_after: Option<String>,
    },
    /// Any other answer, by wire code (`HTTP_<status>` when the body names none).
    Failed(String),
    /// No answer: reset, EOF or socket timeout.
    Dropped,
}

fn wire_code(status: u16, body: &str) -> String {
    let named = body.find("\"code\":\"").and_then(|i| {
        let rest = &body[i + 8..];
        rest.find('"').map(|j| rest[..j].to_string())
    });
    let bare = body.trim();
    named
        .or_else(|| {
            (!bare.is_empty()
                && bare.len() <= 64
                && bare.bytes().all(|b| b.is_ascii_uppercase() || b == b'_'))
            .then(|| bare.to_string())
        })
        .unwrap_or_else(|| format!("HTTP_{status}"))
}

/// Classify one response. Only a 503 whose body is exactly `RATE_LIMITED` is an admission refusal.
pub fn outcome_of(r: &Response) -> Outcome {
    let body = r.body.trim();
    if r.status == 503 && body == "RATE_LIMITED" {
        return Outcome::Refused {
            retry_after: r.retry_after.clone(),
        };
    }
    if r.status == 200 && !body.contains("\"error\":{") && !body.contains("\"isError\":true") {
        return Outcome::Ok;
    }
    Outcome::Failed(wire_code(r.status, body))
}

/// A refusal's `Retry-After` when it is the integer seconds RFC 9110 §10.2.3 allows (and §67.2 caps at 30).
fn retry_after_secs(raw: Option<&String>) -> Option<u64> {
    raw.and_then(|v| v.parse::<u64>().ok())
}

/// One call's record.
#[derive(Debug, Clone)]
pub struct Sample {
    group: &'static str,
    op: Op,
    ms: f64,
    status: u16,
    outcome: Outcome,
    /// Started before the warm-up ended: counted, never in a latency percentile.
    warm: bool,
}

/// Where the clients send.
pub struct Target {
    host_port: String,
    path: String,
    origin: String,
    io_timeout: Duration,
}

/// One client of a phase.
pub struct ClientSpec {
    group: &'static str,
    key: Key,
    forwarded: String,
    first_op: usize,
}

/// One client's closed loop until `deadline`: zero think time; a 503 pauses it for `Retry-After` seconds.
pub fn client_loop(
    target: &Target,
    spec: &ClientSpec,
    warm_until: Instant,
    deadline: Instant,
) -> Vec<Sample> {
    let mut conn = Conn::new(&target.host_port, target.io_timeout);
    let mut samples = Vec::new();
    let mut seq = spec.first_op;
    while Instant::now() < deadline {
        let op = if seq % 10 < ENUMERATE_TENTHS {
            Op::Enumerate
        } else {
            Op::ToolsList
        };
        seq += 1;
        let started = Instant::now();
        let result = conn.send(&request(target, &spec.key, &spec.forwarded, op));
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        let (status, outcome) = match &result {
            Ok(r) => (r.status, outcome_of(r)),
            Err(_) => (0, Outcome::Dropped),
        };
        let pause = match &outcome {
            Outcome::Refused { retry_after } => {
                retry_after_secs(retry_after.as_ref()).map(Duration::from_secs)
            }
            _ => None,
        };
        samples.push(Sample {
            group: spec.group,
            op,
            ms,
            status,
            outcome,
            warm: started < warm_until,
        });
        if let Some(p) = pause {
            // ADR-0065 D-F: a well-behaved client waits Retry-After before its next call.
            std::thread::sleep(p.min(deadline.saturating_duration_since(Instant::now())));
        }
    }
    samples
}

// ---------------------------------------------------------------------------------------------
// Phase report
// ---------------------------------------------------------------------------------------------

/// Latency of admitted calls of one op (nearest rank, ms).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct OpStats {
    n: usize,
    p50: f64,
    p95: f64,
    p99: f64,
}

fn stats(mut ms: Vec<f64>) -> OpStats {
    ms.sort_by(f64::total_cmp);
    OpStats {
        n: ms.len(),
        p50: percentile(&ms, 0.50),
        p95: percentile(&ms, 0.95),
        p99: percentile(&ms, 0.99),
    }
}

/// One group (tenant lane) of one phase. Counts cover the whole phase; latencies only the measured window.
#[derive(Debug, Clone, Default)]
pub struct GroupReport {
    ops: BTreeMap<&'static str, OpStats>,
    /// Admitted latency over both ops (the isolation witness grades this).
    all: OpStats,
    calls: usize,
    failed_calls: BTreeMap<String, usize>,
    refused_503: usize,
    http_503: usize,
    p100_503_ms: f64,
    retry_after_min: Option<u64>,
    retry_after_max: Option<u64>,
    retry_after_bad: usize,
    drops: usize,
    max_ms: f64,
}

/// Fold one group's samples. A response that took the handler timeout or longer counts as a drop too
/// (the handler gave up on it; ADR-0065 D-A's drop rate).
pub fn group_report(samples: &[Sample], group: &str, handler_timeout_ms: f64) -> GroupReport {
    let mine: Vec<&Sample> = samples.iter().filter(|s| s.group == group).collect();
    let mut g = GroupReport {
        calls: mine.len(),
        ..GroupReport::default()
    };
    let mut by_op: BTreeMap<&'static str, Vec<f64>> = BTreeMap::new();
    let mut all = Vec::new();
    for s in &mine {
        g.max_ms = g.max_ms.max(s.ms);
        if s.status == 503 {
            g.http_503 += 1;
        }
        match &s.outcome {
            Outcome::Ok if !s.warm => {
                by_op.entry(s.op.label()).or_default().push(s.ms);
                all.push(s.ms);
            }
            Outcome::Ok => {}
            Outcome::Refused { retry_after } => {
                g.refused_503 += 1;
                g.p100_503_ms = g.p100_503_ms.max(s.ms);
                match retry_after_secs(retry_after.as_ref()) {
                    Some(v) if (1..=30).contains(&v) => {
                        g.retry_after_min = Some(g.retry_after_min.map_or(v, |m| m.min(v)));
                        g.retry_after_max = Some(g.retry_after_max.map_or(v, |m| m.max(v)));
                    }
                    _ => g.retry_after_bad += 1,
                }
            }
            Outcome::Failed(code) => {
                *g.failed_calls.entry(code.clone()).or_default() += 1;
                if s.ms >= handler_timeout_ms {
                    g.drops += 1;
                }
            }
            Outcome::Dropped => g.drops += 1,
        }
    }
    g.ops = by_op.into_iter().map(|(op, ms)| (op, stats(ms))).collect();
    g.all = stats(all);
    g
}

/// `pg_stat_activity` peaks over one phase (1 Hz).
#[derive(Debug, Clone, Default)]
pub struct PgPeaks {
    samples: usize,
    backends: i64,
    role_gateway: i64,
    active_gateway_max_ms: f64,
    idle_in_txn_max_ms: f64,
    waits: BTreeMap<String, i64>,
    by_user_state: BTreeMap<String, i64>,
    mem_available_kb_at_peak: Option<u64>,
    error: Option<String>,
}

/// One phase.
#[derive(Debug, Clone, Default)]
pub struct PhaseReport {
    name: String,
    clients: usize,
    secs: f64,
    max_wait_ms: f64,
    groups: BTreeMap<&'static str, GroupReport>,
    /// Scraped `admission_rejected_total` delta per `reason`; `None` when a scrape failed.
    admission_delta: Option<BTreeMap<String, f64>>,
    pg: PgPeaks,
    /// Rate advisory-lock waits of `role_gateway` (ADR-0065 D-D); `None` without a database or a tenant to name.
    lock: Option<LockWaits>,
    measured_at: String,
}

impl PhaseReport {
    fn refused(&self) -> usize {
        self.groups.values().map(|g| g.refused_503).sum()
    }
    fn delta(&self, reason: &str) -> f64 {
        self.admission_delta
            .as_ref()
            .and_then(|d| d.get(reason).copied())
            .unwrap_or(0.0)
    }
}

fn json_stats(s: &OpStats) -> serde_json::Value {
    serde_json::json!({"n": s.n, "p50": s.p50, "p95": s.p95, "p99": s.p99, "unit": "ms"})
}

fn phase_json(p: &PhaseReport) -> serde_json::Value {
    let groups: serde_json::Map<String, serde_json::Value> = p
        .groups
        .iter()
        .map(|(name, g)| {
            let ops: serde_json::Map<String, serde_json::Value> =
                g.ops.iter().map(|(op, s)| ((*op).to_string(), json_stats(s))).collect();
            (
                (*name).to_string(),
                serde_json::json!({
                    "calls": g.calls, "ops": ops, "admitted_all_ops": json_stats(&g.all),
                    "failed_calls": g.failed_calls, "admission_503": g.refused_503, "http_503": g.http_503,
                    "p100_503_ms": g.p100_503_ms, "retry_after_min_s": g.retry_after_min,
                    "retry_after_max_s": g.retry_after_max, "retry_after_invalid": g.retry_after_bad,
                    "drops": g.drops, "max_latency_ms": g.max_ms,
                }),
            )
        })
        .collect();
    serde_json::json!({
        "phase": p.name, "clients": p.clients, "secs": p.secs, "max_wait_ms": p.max_wait_ms,
        "groups": groups, "admission_rejected_delta": p.admission_delta,
        "pg_stat_activity": {
            "samples": p.pg.samples, "client_backends_peak": p.pg.backends, "role_gateway_peak": p.pg.role_gateway,
            "active_role_gateway_max_ms": p.pg.active_gateway_max_ms, "idle_in_transaction_max_ms": p.pg.idle_in_txn_max_ms,
            "wait_event_type_peak": p.pg.waits, "by_user_state_peak": p.pg.by_user_state,
            "mem_available_kb_at_peak": p.pg.mem_available_kb_at_peak, "error": p.pg.error,
        },
        "rate_lock_wait": p.lock.as_ref().map(lock_json),
        "measured_at": p.measured_at,
    })
}

/// The `LOAD` lines of one phase: one per group and op (ADR-0065 "Measurement").
fn load_lines(p: &PhaseReport, host: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (group, g) in &p.groups {
        for (op, s) in &g.ops {
            out.push(format!(
                "LOAD {} group={group} clients={} op={op} n={} p50={:.1}ms p95={:.1}ms p99={:.1}ms failed={} admission_503={} drops={} host={host} measured_at={}",
                p.name, p.clients, s.n, s.p50, s.p95, s.p99,
                g.failed_calls.values().sum::<usize>(), g.refused_503, g.drops, p.measured_at
            ));
        }
    }
    out
}

/// The `pct`-th percentile of the wait of ALL `acquisitions` of one lock (nearest rank), from the waits the sampler
/// saw. An acquisition the sampler never saw waiting waited less than one sampling interval, so it ranks below every
/// observed wait: `None` = the percentile is below the resolution. Integer ranks: no float rounding at the boundary.
pub fn wait_quantile(observed_ms: &[f64], acquisitions: u64, pct: u64) -> Option<f64> {
    let mut desc = observed_ms.to_vec();
    desc.sort_by(|a, b| b.total_cmp(a));
    let rank = (acquisitions * (100 - pct.min(100))).div_ceil(100).max(1);
    desc.get(usize::try_from(rank).ok()? - 1).copied()
}

fn lock_json(l: &LockWaits) -> serde_json::Value {
    let q = |p| wait_quantile(&l.tenant_waits_ms, l.acquisitions, p);
    serde_json::json!({
        "lock": "tenant", "tenant_id": l.tenant, "acquisitions": l.acquisitions,
        "observed_waits": l.tenant_waits_ms.len(), "p50_ms": q(50), "p99_ms": q(99),
        "max_ms": l.tenant_waits_ms.iter().copied().fold(0.0, f64::max),
        "other_rate_lock_observed_waits": l.other_waits_ms.len(),
        "other_rate_lock_max_ms": l.other_waits_ms.iter().copied().fold(0.0, f64::max),
        "resolution_ms": l.interval_ms, "samples": l.samples, "error": l.error,
        "note": "a quantile of null is below resolution_ms; acquisitions = the tenant bucket's version delta",
    })
}

/// The `LOCKWAIT` line of one phase (ADR-0065 D-D, the S7 measure of the tenant lock).
fn lock_line(p: &PhaseReport, host: &str) -> Option<String> {
    let l = p.lock.as_ref()?;
    let q = |p| {
        wait_quantile(&l.tenant_waits_ms, l.acquisitions, p).map_or_else(
            || format!("<{:.0}ms", l.interval_ms),
            |v| format!("{v:.1}ms"),
        )
    };
    Some(format!(
        "LOCKWAIT {} lock=tenant group=LA acquisitions={} observed_waits={} p50={} p99={} max={:.1}ms other_rate_lock_waits={} other_max={:.1}ms resolution={:.0}ms samples={}{} host={host} measured_at={}",
        p.name,
        l.acquisitions,
        l.tenant_waits_ms.len(),
        q(50),
        q(99),
        l.tenant_waits_ms.iter().copied().fold(0.0, f64::max),
        l.other_waits_ms.len(),
        l.other_waits_ms.iter().copied().fold(0.0, f64::max),
        l.interval_ms,
        l.samples,
        l.error
            .as_ref()
            .map_or_else(String::new, |e| format!(" error={e}")),
        p.measured_at
    ))
}

// ---------------------------------------------------------------------------------------------
// Assertions (ADR-0065 D-F; 7 is the rehearsal's, after teardown)
// ---------------------------------------------------------------------------------------------

/// One graded line.
#[derive(Debug, Clone)]
pub struct Verdict {
    id: &'static str,
    pass: bool,
    detail: String,
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    if v.is_empty() {
        return f64::NAN;
    }
    let m = v.len() / 2;
    if v.len() % 2 == 1 {
        v[m]
    } else {
        (v[m - 1] + v[m]) / 2.0
    }
}

fn find<'a>(phases: &'a [PhaseReport], name: &str) -> Option<&'a PhaseReport> {
    phases.iter().find(|p| p.name == name)
}

fn a1_no_pool_or_statement_timeout(cfg: &Config, phases: &[PhaseReport]) -> Verdict {
    let top = cfg
        .levels
        .iter()
        .max()
        .map(|l| format!("L{l}"))
        .unwrap_or_default();
    let (pass, detail) = match find(phases, &top).and_then(|p| p.groups.get("LA")) {
        Some(g) => {
            let du = g
                .failed_calls
                .get("DEPENDENCY_UNAVAILABLE")
                .copied()
                .unwrap_or(0);
            let internal = g.failed_calls.get("INTERNAL").copied().unwrap_or(0);
            (
                du == 0 && internal == 0 && g.drops == 0,
                format!(
                    "{top}: DEPENDENCY_UNAVAILABLE={du} INTERNAL={internal} drops={}",
                    g.drops
                ),
            )
        }
        None => (false, format!("phase {top} not run")),
    };
    Verdict {
        id: "load_no_pool_or_statement_timeout",
        pass,
        detail,
    }
}

fn a2_overflow(cfg: &Config, phases: &[PhaseReport]) -> Verdict {
    let mut bad = Vec::new();
    let handler_ms = cfg.handler_timeout.as_secs_f64() * 1000.0;
    for p in phases {
        let (http_503, refused, invalid, max_ms) =
            p.groups.values().fold((0, 0, 0, 0.0f64), |a, g| {
                (
                    a.0 + g.http_503,
                    a.1 + g.refused_503,
                    a.2 + g.retry_after_bad,
                    a.3.max(g.max_ms),
                )
            });
        if http_503 != refused || invalid > 0 {
            bad.push(format!(
                "{}: http_503={http_503} RATE_LIMITED={refused} retry_after_invalid={invalid}",
                p.name
            ));
        }
        if max_ms > p.max_wait_ms + handler_ms {
            bad.push(format!(
                "{}: a call took {max_ms:.0}ms > W + handler timeout",
                p.name
            ));
        }
        if p.name.starts_with('L') && p.delta("key_limit") > 0.0 {
            bad.push(format!("{}: key_limit={}", p.name, p.delta("key_limit")));
        }
    }
    let p100 = |p: &PhaseReport| p.groups.values().map(|g| g.p100_503_ms).fold(0.0, f64::max);
    match find(phases, "BURST") {
        Some(p)
            if p.refused() > 0
                && p.delta("queue_full") >= 1.0
                && p.delta("key_limit") == 0.0
                && p100(p) <= p.max_wait_ms + 200.0 => {}
        Some(p) => bad.push(format!(
            "BURST: 503={} queue_full={} key_limit={} p100_503={:.0}ms (W={})",
            p.refused(),
            p.delta("queue_full"),
            p.delta("key_limit"),
            p100(p),
            p.max_wait_ms
        )),
        None => bad.push("BURST not run".to_string()),
    }
    match find(phases, "BURST-W") {
        Some(p) if p.delta("wait_timeout") >= 1.0 && p100(p) <= p.max_wait_ms + 200.0 => {}
        Some(p) => bad.push(format!(
            "BURST-W: wait_timeout={} p100_503={:.0}ms (W={})",
            p.delta("wait_timeout"),
            p100(p),
            p.max_wait_ms
        )),
        None => bad.push("BURST-W not run".to_string()),
    }
    Verdict {
        id: "load_overflow_503_within_max_wait",
        pass: bad.is_empty(),
        detail: if bad.is_empty() {
            "every phase".to_string()
        } else {
            bad.join("; ")
        },
    }
}

fn a3_counter(phases: &[PhaseReport]) -> Verdict {
    let mut bad = Vec::new();
    for p in phases {
        #[expect(clippy::cast_precision_loss, reason = "call counts far below 2^52")]
        let refused = p.refused() as f64;
        match &p.admission_delta {
            Some(d) if (d.values().sum::<f64>() - refused).abs() < f64::EPSILON => {}
            Some(d) => bad.push(format!(
                "{}: scraped={} client_503={}",
                p.name,
                d.values().sum::<f64>(),
                p.refused()
            )),
            None => bad.push(format!("{}: ops /metrics scrape failed", p.name)),
        }
    }
    Verdict {
        id: "load_admission_counter_equals_503s",
        pass: bad.is_empty(),
        detail: if bad.is_empty() {
            format!("{} phases", phases.len())
        } else {
            bad.join("; ")
        },
    }
}

fn a4_p95(cfg: &Config, phases: &[PhaseReport]) -> Verdict {
    let mut bad = Vec::new();
    for l in &cfg.levels {
        let name = format!("L{l}");
        for op in [Op::Enumerate, Op::ToolsList] {
            let n = find(phases, &name)
                .and_then(|p| p.groups.get("LA"))
                .and_then(|g| g.ops.get(op.label()))
                .map_or(0, |s| s.n);
            if n < 50 {
                bad.push(format!("{name}/{}: n={n}", op.label()));
            }
        }
    }
    Verdict {
        id: "load_p95_recorded",
        pass: bad.is_empty(),
        detail: if bad.is_empty() {
            "n >= 50 for every level and op".to_string()
        } else {
            bad.join("; ")
        },
    }
}

/// Per ISO pair: (base p95, burst p95) of LB, both ms.
fn iso_pairs(phases: &[PhaseReport], pairs: usize) -> Vec<(f64, f64)> {
    (1..=pairs)
        .filter_map(|i| {
            let lb = |n: String| {
                find(phases, &n)
                    .and_then(|p| p.groups.get("LB"))
                    .map(|g| g.all.p95)
            };
            Some((lb(format!("ISO-BASE-{i}"))?, lb(format!("ISO-BURST-{i}"))?))
        })
        .collect()
}

fn a5_isolation(cfg: &Config, phases: &[PhaseReport]) -> Verdict {
    let pairs = iso_pairs(phases, cfg.iso_pairs);
    let iso: Vec<&PhaseReport> = phases
        .iter()
        .filter(|p| p.name.starts_with("ISO-"))
        .collect();
    let lb_ok = iso.len() == 2 * cfg.iso_pairs
        && iso.iter().all(|p| {
            p.groups
                .get("LB")
                .is_some_and(|g| g.all.n >= 50 && g.refused_503 == 0)
        });
    let ratio = median(pairs.iter().map(|(b, x)| x / b).collect());
    let increase = median(pairs.iter().map(|(b, x)| x - b).collect());
    let pass =
        lb_ok && pairs.len() == cfg.iso_pairs && (ratio < 2.0 || increase < cfg.iso_floor_ms);
    Verdict {
        id: "load_tenant_isolation",
        pass,
        detail: format!(
            "pairs={} median_ratio={ratio:.2} median_increase={increase:.1}ms floor={}ms lb_n_and_no_503={lb_ok}",
            pairs.len(),
            cfg.iso_floor_ms
        ),
    }
}

fn a6_backends(cfg: &Config, phases: &[PhaseReport]) -> Verdict {
    let total = phases.iter().map(|p| p.pg.backends).max().unwrap_or(0);
    let gw = phases.iter().map(|p| p.pg.role_gateway).max().unwrap_or(0);
    let sampled = phases
        .iter()
        .all(|p| p.pg.error.is_none() && p.pg.samples > 0);
    Verdict {
        id: "load_pg_backends_bounded",
        pass: sampled && total <= cfg.max_backends && gw <= 2 * cfg.pool_max,
        detail: format!(
            "client_backends_peak={total} (<= {}) role_gateway_peak={gw} (<= 2x{}) sampled={sampled}",
            cfg.max_backends, cfg.pool_max
        ),
    }
}

/// Assertion 8 over the `2001:db8:` preauth subjects written since the load started.
pub fn a8_lanes(lanes: &[char], subjects: Option<&[String]>) -> Verdict {
    let want: Vec<String> = lanes.iter().map(|l| format!("2001:db8:{l}::/64")).collect();
    let (pass, detail) = match subjects {
        Some(got) => {
            let ours: Vec<&String> = got.iter().filter(|s| want.contains(s)).collect();
            (
                ours.len() == want.len(),
                format!("want {want:?} got {ours:?}"),
            )
        }
        None => (false, "control.rate_buckets not read".to_string()),
    };
    Verdict {
        id: "load_forwarded_lanes_keyed",
        pass,
        detail,
    }
}

fn grade(
    cfg: &Config,
    phases: &[PhaseReport],
    lanes_used: &[char],
    subjects: Option<&[String]>,
) -> Vec<Verdict> {
    vec![
        a1_no_pool_or_statement_timeout(cfg, phases),
        a2_overflow(cfg, phases),
        a3_counter(phases),
        a4_p95(cfg, phases),
        a5_isolation(cfg, phases),
        a6_backends(cfg, phases),
        a8_lanes(lanes_used, subjects),
    ]
}

// ---------------------------------------------------------------------------------------------
// Side readings: readiness, ops /metrics, pg_stat_activity, host noise
// ---------------------------------------------------------------------------------------------

fn get(host_port: &str, path: &str) -> Result<Response, String> {
    let mut c = Conn::new(host_port, Duration::from_secs(5));
    c.send(&format!(
        "GET {path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n"
    ))
}

/// Poll `/readyz` until 200 or the timeout (assertion 0).
fn wait_ready(cfg: &Config) -> Result<Duration, String> {
    let started = Instant::now();
    let mut last = String::from("no attempt");
    while started.elapsed() <= cfg.ready_timeout {
        // dep: HTTP(gateway) — the gateway's own /readyz (ADR-0037), not a re-implementation.
        match get(&cfg.host_port, "/readyz") {
            Ok(r) if r.status == 200 => return Ok(started.elapsed()),
            Ok(r) => last = format!("HTTP {}", r.status),
            Err(e) => last = e,
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    Err(format!(
        "{}/readyz not 200 after {:?}: {last}",
        cfg.host_port, cfg.ready_timeout
    ))
}

/// `admission_rejected_total` per `reason` from the ops listener (absent family = empty map).
fn scrape(cfg: &Config) -> Result<BTreeMap<String, f64>, String> {
    // dep: HTTP(gateway) — the ops listener's /metrics (ADR-0061).
    let r = get(&cfg.ops_host_port, &cfg.ops_path)?;
    if r.status != 200 {
        return Err(format!("ops metrics HTTP {}", r.status));
    }
    let mut out = BTreeMap::new();
    for line in r
        .body
        .lines()
        .filter(|l| l.starts_with("admission_rejected_total{"))
    {
        let reason = line
            .split("reason=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .unwrap_or("?")
            .to_string();
        let v: f64 = line
            .rsplit(' ')
            .next()
            .and_then(|v| v.parse().ok())
            .unwrap_or(f64::NAN);
        *out.entry(reason).or_insert(0.0) += v;
    }
    Ok(out)
}

fn delta(before: &BTreeMap<String, f64>, after: &BTreeMap<String, f64>) -> BTreeMap<String, f64> {
    after
        .iter()
        .map(|(k, v)| (k.clone(), v - before.get(k).copied().unwrap_or(0.0)))
        .collect()
}

fn run_cmd(program: &str, args: &[&str]) -> Option<String> {
    // dep: subprocess(docker) — read-only host readings (`docker exec … cat /proc/meminfo`, `docker stats`).
    let out = Command::new(program).args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// `MemTotal` and `MemAvailable` of the Docker VM in kB, read inside the PG container (read-only).
fn vm_meminfo(container: &str) -> Option<(u64, u64)> {
    let text = run_cmd("docker", &["exec", container, "cat", "/proc/meminfo"])?;
    let field = |name: &str| {
        text.lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse().ok())
    };
    Some((field("MemTotal:")?, field("MemAvailable:")?))
}

/// `host=<cpus>cpu-vm<GiB>` (ADR-0065 R14: every number names the host it was measured on).
fn host_label(container: &str) -> String {
    let cpus = std::thread::available_parallelism().map_or(0, std::num::NonZero::get);
    #[expect(clippy::cast_precision_loss, reason = "kB of one VM")]
    let vm = vm_meminfo(container).map_or_else(
        || "?".to_string(),
        |(t, _)| format!("{:.1}", t as f64 / 1_048_576.0),
    );
    format!("{cpus}cpu-vm{vm}")
}

fn utc_now() -> String {
    let t = time::OffsetDateTime::now_utc();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
}

/// Read-only session for the sampler and assertion 8.
fn pg_connect(dsn: &str) -> Result<Client, String> {
    // dep: PostgreSQL(any) — read-only sampler session (pg_stat_activity, control.rate_buckets).
    let mut c = Client::connect(dsn, NoTls).map_err(|e| format!("connect: {e}"))?;
    c.batch_execute(
        "SET default_transaction_read_only = on; SET application_name = 'xtask-load-sampler'",
    )
    .map_err(|e| format!("session setup: {e}"))?;
    Ok(c)
}

const ACTIVITY_SQL: &str = "select count(*) filter (where backend_type = 'client backend'), \
 count(*) filter (where usename = 'role_gateway'), \
 coalesce(max(extract(epoch from now() - query_start) * 1000) filter (where usename = 'role_gateway' and state = 'active'), 0)::float8, \
 coalesce(max(extract(epoch from now() - state_change) * 1000) filter (where state like 'idle in transaction%'), 0)::float8 \
 from pg_stat_activity";
const GROUPS_SQL: &str = "select 'wait:' || coalesce(wait_event_type, '-'), count(*) from pg_stat_activity \
 where backend_type = 'client backend' group by 1 union all \
 select 'who:' || coalesce(usename::text, '-') || '/' || coalesce(state, '-'), count(*) from pg_stat_activity \
 where backend_type = 'client backend' group by 1";

fn sample_once(db: &mut Client, container: &str, peaks: &mut PgPeaks) -> Result<(), String> {
    // dep: PostgreSQL(any) — pg_stat_activity, read-only.
    let row = db
        .query_one(ACTIVITY_SQL, &[])
        .map_err(|e| format!("pg_stat_activity: {e}"))?;
    let backends: i64 = row.get(0);
    if backends > peaks.backends || peaks.samples == 0 {
        peaks.mem_available_kb_at_peak = vm_meminfo(container).map(|(_, a)| a);
    }
    peaks.backends = peaks.backends.max(backends);
    peaks.role_gateway = peaks.role_gateway.max(row.get(1));
    peaks.active_gateway_max_ms = peaks.active_gateway_max_ms.max(row.get(2));
    peaks.idle_in_txn_max_ms = peaks.idle_in_txn_max_ms.max(row.get(3));
    // dep: PostgreSQL(any) — pg_stat_activity histograms, read-only.
    for r in db
        .query(GROUPS_SQL, &[])
        .map_err(|e| format!("pg_stat_activity groups: {e}"))?
    {
        let (k, n): (String, i64) = (r.get(0), r.get(1));
        let map = if let Some(w) = k.strip_prefix("wait:") {
            (&mut peaks.waits, w)
        } else {
            (&mut peaks.by_user_state, &k[4..])
        };
        let e = map.0.entry(map.1.to_string()).or_insert(0);
        *e = (*e).max(n);
    }
    peaks.samples += 1;
    Ok(())
}

/// 1 Hz until `stop`; the first error ends sampling for the phase and is recorded.
fn sampler(db: Option<&mut Client>, container: &str, stop: &AtomicBool) -> PgPeaks {
    let mut peaks = PgPeaks::default();
    let Some(db) = db else {
        peaks.error = Some("no sampler session".to_string());
        return peaks;
    };
    while !stop.load(Ordering::Relaxed) {
        if let Err(e) = sample_once(db, container, &mut peaks) {
            peaks.error = Some(e);
            break;
        }
        let tick = Instant::now();
        while tick.elapsed() < Duration::from_secs(1) && !stop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    peaks
}

/// The tenant whose bucket lock the `LOCKWAIT` lines measure (lane a's tenant) and that lock's 64-bit key.
#[derive(Debug, Clone)]
pub struct TenantLock {
    tenant: String,
    key: i64,
}

/// Rate advisory-lock waits of `role_gateway` over one phase, split into the tenant lock and every other rate lock.
#[derive(Debug, Clone, Default)]
pub struct LockWaits {
    tenant: String,
    interval_ms: f64,
    samples: usize,
    /// Tenant-bucket takes in the phase (its `version` delta): every one took the tenant lock first.
    acquisitions: u64,
    /// The longest age the sampler saw of each distinct wait (backend pid, statement start), ms.
    tenant_waits_ms: Vec<f64>,
    other_waits_ms: Vec<f64>,
    error: Option<String>,
}

/// Lane a's tenant and its tenant-bucket lock key, read once on the sampler session.
fn tenant_lock(db: &mut Client, workspace: &str) -> Result<TenantLock, String> {
    // dep: PostgreSQL(any) — control.workspaces, read-only: lane a's tenant.
    let tenant: String = db
        .query_one(
            "select tenant_id::text from control.workspaces where workspace_id::text = $1",
            &[&workspace],
        )
        .map_err(|e| format!("control.workspaces: {e}"))?
        .get(0);
    let text = humaux_adapters::quota_repo::rate_lock_key(
        &tenant,
        "tenant",
        &tenant,
        humaux_adapters::quota_repo::SHARED_RATE_OPERATION,
        "tenant",
    );
    // dep: PostgreSQL(any) — the server's own hash, so the key is the one consume_rate_batch locks.
    let key: i64 = db
        .query_one("select hashtextextended($1, 0)", &[&text])
        .map_err(|e| format!("hashtextextended: {e}"))?
        .get(0);
    Ok(TenantLock { tenant, key })
}

// A bigint advisory key shows as classid = high half, objid = low half, objsubid = 1 (PostgreSQL pg_locks docs).
const LOCK_WAIT_SQL: &str = "select a.pid, extract(epoch from a.query_start)::float8, \
 (extract(epoch from clock_timestamp() - a.query_start) * 1000)::float8, \
 ((l.classid::bigint << 32) | l.objid::bigint) = $1 \
 from pg_locks l join pg_stat_activity a on a.pid = l.pid \
 where l.locktype = 'advisory' and l.objsubid = 1 and not l.granted and a.usename = 'role_gateway'";
const TENANT_VERSION_SQL: &str = "select coalesce(sum(version), 0)::bigint from control.rate_buckets \
 where tenant_id::text = $1 and subject_kind = 'tenant'";

/// Every `interval` until `stop`, on its own read-only session: the age of each waiting rate advisory lock of
/// `role_gateway`. One wait is one (pid, statement start); its longest observed age is its wait, short by at most one
/// interval. The first error ends sampling for the phase and is recorded.
fn lock_sampler(
    dsn: &str,
    target: &TenantLock,
    interval: Duration,
    stop: &AtomicBool,
) -> LockWaits {
    let mut out = LockWaits {
        tenant: target.tenant.clone(),
        interval_ms: interval.as_secs_f64() * 1000.0,
        ..LockWaits::default()
    };
    let mut db = match pg_connect(dsn) {
        Ok(c) => c,
        Err(e) => {
            out.error = Some(e);
            return out;
        }
    };
    let version = |db: &mut Client| -> Result<i64, String> {
        // dep: PostgreSQL(any) — control.rate_buckets, read-only: the tenant bucket's take count.
        db.query_one(TENANT_VERSION_SQL, &[&target.tenant])
            .map(|r| r.get(0))
            .map_err(|e| format!("control.rate_buckets: {e}"))
    };
    let start = version(&mut db);
    let mut seen: BTreeMap<(i32, u64), (bool, f64)> = BTreeMap::new();
    while !stop.load(Ordering::Relaxed) {
        let tick = Instant::now();
        // dep: PostgreSQL(any) — pg_locks / pg_stat_activity, read-only.
        match db.query(LOCK_WAIT_SQL, &[&target.key]) {
            Ok(rows) => {
                for r in rows {
                    let (pid, since, age, tenant): (i32, f64, f64, bool) =
                        (r.get(0), r.get(1), r.get(2), r.get(3));
                    let e = seen.entry((pid, since.to_bits())).or_insert((tenant, 0.0));
                    e.1 = e.1.max(age);
                }
                out.samples += 1;
            }
            Err(e) => {
                out.error = Some(format!("pg_locks: {e}"));
                break;
            }
        }
        if let Some(rest) = interval.checked_sub(tick.elapsed()) {
            std::thread::sleep(rest);
        }
    }
    match (start, version(&mut db)) {
        (Ok(a), Ok(b)) => out.acquisitions = u64::try_from(b - a).unwrap_or(0),
        (Err(e), _) | (_, Err(e)) => out.error = Some(e),
    }
    for (tenant, ms) in seen.into_values() {
        if tenant {
            out.tenant_waits_ms.push(ms);
        } else {
            out.other_waits_ms.push(ms);
        }
    }
    out
}

/// The `ISO_NOISE` line of one pair: what else was running when the number was taken (ruling R-10).
fn iso_noise(cfg: &Config, pair: &str, host: &str) -> String {
    // dep: subprocess(sysctl) — `sysctl -n vm.loadavg`, read-only.
    let load = run_cmd("sysctl", &["-n", "vm.loadavg"]).unwrap_or_else(|| "?".to_string());
    // dep: subprocess(docker) — `docker stats --no-stream`, read-only.
    let cpu = run_cmd(
        "docker",
        &["stats", "--no-stream", "--format", "{{.Name}}={{.CPUPerc}}"],
    )
    .map_or_else(
        || "?".to_string(),
        |s| s.lines().collect::<Vec<_>>().join(","),
    );
    let mem = vm_meminfo(&cfg.pg_container)
        .map_or_else(|| "?".to_string(), |(t, a)| format!("{t}/{a}kB"));
    format!(
        "ISO_NOISE pair={pair} loadavg={load} docker_cpu=[{cpu}] vm_mem_total/available={mem} host={host} measured_at={}",
        utc_now()
    )
}

// ---------------------------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------------------------

/// The clients of one phase: `(group, lane, count, one_key)`.
type Mix = Vec<(&'static str, char, usize, bool)>;

fn clients(cfg: &Config, mix: &Mix) -> Vec<ClientSpec> {
    let mut out = Vec::new();
    for &(group, lane, n, one_key) in mix {
        let keys = &cfg.lanes[&lane];
        for i in 0..n {
            out.push(ClientSpec {
                group,
                key: keys[if one_key { 0 } else { i % keys.len() }].clone(),
                forwarded: if cfg.rfc7239_lane == Some(lane) {
                    // ADR-0065 S7 fault F-FWD: the form the edge parser refuses, so no preauth row for this lane.
                    format!("for=\"[{}]\"", forwarded_for(lane, i))
                } else {
                    forwarded_for(lane, i)
                },
                first_op: i,
            });
        }
    }
    out
}

fn run_phase(
    cfg: &Config,
    db: Option<&mut Client>,
    lock: Option<&TenantLock>,
    phase: &Phase,
) -> PhaseReport {
    let (name, mix, warm, dur, max_wait_ms) = (&phase.0, &phase.1, phase.2, phase.3, phase.4);
    let specs = clients(cfg, mix);
    let target = Target {
        host_port: cfg.host_port.clone(),
        path: cfg.path.clone(),
        origin: cfg.origin.clone(),
        io_timeout: cfg.handler_timeout
            + Duration::from_secs_f64(max_wait_ms / 1000.0)
            + Duration::from_secs(10),
    };
    let before = scrape(cfg);
    let stop = AtomicBool::new(false);
    let started = Instant::now();
    let (samples, pg, lock) = std::thread::scope(|s| {
        let sampler = s.spawn(|| sampler(db, &cfg.pg_container, &stop));
        let locks = s.spawn(|| {
            let dsn = cfg.pg_dsn.as_deref()?;
            Some(lock_sampler(dsn, lock?, cfg.lock_sample, &stop))
        });
        let handles: Vec<_> = specs
            .iter()
            .map(|spec| {
                s.spawn(|| client_loop(&target, spec, started + warm, started + warm + dur))
            })
            .collect();
        let samples: Vec<Sample> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap_or_default())
            .collect();
        stop.store(true, Ordering::Relaxed);
        (
            samples,
            sampler.join().unwrap_or_default(),
            locks.join().ok().flatten(),
        )
    });
    let after = scrape(cfg);
    let handler_ms = cfg.handler_timeout.as_secs_f64() * 1000.0;
    let groups = mix
        .iter()
        .map(|m| (m.0, group_report(&samples, m.0, handler_ms)))
        .collect();
    PhaseReport {
        name: name.to_string(),
        clients: specs.len(),
        secs: started.elapsed().as_secs_f64(),
        max_wait_ms,
        groups,
        admission_delta: before.and_then(|b| after.map(|a| delta(&b, &a))).ok(),
        pg,
        lock,
        measured_at: utc_now(),
    }
}

/// One planned phase: `(name, mix, warm-up, duration, W)`.
type Phase = (String, Mix, Duration, Duration, f64);

/// The phase plan in chain order.
fn plan(cfg: &Config) -> Vec<Phase> {
    let mut out: Vec<Phase> = cfg
        .levels
        .iter()
        .map(|&l| {
            (
                format!("L{l}"),
                vec![("LA", 'a', l, false)],
                cfg.warmup,
                cfg.level,
                cfg.max_wait_ms,
            )
        })
        .collect();
    out.push((
        "BURST".to_string(),
        vec![("LA", 'a', cfg.burst, false)],
        Duration::ZERO,
        cfg.burst_secs,
        cfg.max_wait_ms,
    ));
    for i in 1..=cfg.iso_pairs {
        out.push((
            format!("ISO-BASE-{i}"),
            vec![("LB", 'b', cfg.iso_base_clients, false)],
            Duration::ZERO,
            cfg.iso,
            cfg.max_wait_ms,
        ));
        out.push((
            format!("ISO-BURST-{i}"),
            vec![
                ("LA", 'a', cfg.iso_burst_clients, true),
                ("LB", 'b', cfg.iso_base_clients, false),
            ],
            Duration::ZERO,
            cfg.iso,
            cfg.max_wait_ms,
        ));
    }
    out.push((
        "BURST-W".to_string(),
        vec![("LA", 'a', cfg.burst, false)],
        Duration::ZERO,
        cfg.burst_w,
        cfg.burst_w_max_wait_ms,
    ));
    if let Some(lane) = cfg.rfc7239_lane {
        out.push((
            "FWD".to_string(),
            vec![("FWD", lane, cfg.iso_base_clients, false)],
            Duration::ZERO,
            cfg.iso,
            cfg.max_wait_ms,
        ));
    }
    out.retain(|(name, ..)| {
        cfg.only.is_empty() || cfg.only.iter().any(|p| name.starts_with(p.as_str()))
    });
    out
}

/// The lanes the run's phases send on, in order: assertion 8 grades exactly these.
fn lanes_used(phases: &[(String, Mix, Duration, Duration, f64)]) -> Vec<char> {
    let mut out: Vec<char> = phases
        .iter()
        .flat_map(|p| p.1.iter().map(|m| m.1))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// `2001:db8:` preauth subjects written since `since` (DB clock), read-only.
fn lane_subjects(db: &mut Client, since: &str) -> Result<Vec<String>, String> {
    // dep: PostgreSQL(any) — control.rate_buckets, read-only (assertion 8).
    let rows = db
        .query(
            "select distinct subject_id from control.rate_buckets where subject_kind = 'ip' \
             and subject_id like '2001:db8:%' and updated_at >= $1::text::timestamptz order by 1",
            &[&since],
        )
        .map_err(|e| format!("control.rate_buckets: {e}"))?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// §57.1 tri-state: with `--only`, the phases an assertion grades that did not run make it NOT_APPLICABLE, named.
/// Without `--only` this is always `None`: a phase that never ran stays a FAIL.
fn not_applicable(cfg: &Config, ran: &[String], id: &str) -> Option<Vec<String>> {
    if cfg.only.is_empty() {
        return None;
    }
    let needs: Vec<String> = match id {
        "load_no_pool_or_statement_timeout" => cfg
            .levels
            .iter()
            .max()
            .map(|l| format!("L{l}"))
            .into_iter()
            .collect(),
        "load_overflow_503_within_max_wait" => vec!["BURST".to_string(), "BURST-W".to_string()],
        "load_p95_recorded" => cfg.levels.iter().map(|l| format!("L{l}")).collect(),
        "load_tenant_isolation" => (1..=cfg.iso_pairs)
            .flat_map(|i| [format!("ISO-BASE-{i}"), format!("ISO-BURST-{i}")])
            .collect(),
        _ => Vec::new(),
    };
    let missing: Vec<String> = needs.into_iter().filter(|n| !ran.contains(n)).collect();
    (!missing.is_empty()).then_some(missing)
}

fn print_verdict(v: &Verdict) {
    println!(
        "ASSERTION {} {}: {}",
        if v.pass { "PASS" } else { "FAIL" },
        v.id,
        v.detail
    );
}

/// Run every phase and write the report. Exit 0 = every assertion passed, 1 = one failed or the gateway was never
/// ready (then no report is written), 2 = usage.
pub fn execute(cfg: &Config) -> i32 {
    match wait_ready(cfg) {
        Ok(t) => println!("load: gateway ready after {:.1}s", t.as_secs_f64()),
        Err(e) => {
            print_verdict(&Verdict {
                id: "load_gateway_ready",
                pass: false,
                detail: e,
            });
            return 1;
        }
    }
    let host = host_label(&cfg.pg_container);
    let mut db = cfg
        .pg_dsn
        .as_deref()
        .map(pg_connect)
        .transpose()
        .unwrap_or_else(|e| {
            eprintln!("load: sampler session unavailable: {e}");
            None
        });
    // dep: PostgreSQL(any) — the DB clock marks the start for assertion 8.
    let since = db
        .as_mut()
        .and_then(|c| c.query_one("select now()::text", &[]).ok())
        .map(|r| r.get::<_, String>(0));
    let first_a = cfg.lanes.get(&'a').and_then(|keys| keys.first());
    let lock = match (db.as_mut(), first_a) {
        (Some(c), Some(k)) => tenant_lock(c, &k.workspace)
            .map_err(|e| eprintln!("load: no LOCKWAIT lines: {e}"))
            .ok(),
        _ => None,
    };
    let mut phases = Vec::new();
    let mut ready_after_restarts = Vec::new();
    let plan = plan(cfg);
    let lanes = lanes_used(&plan);
    for phase in plan {
        let name = phase.0.clone();
        if let Some(cmd) = cfg.restarts.get(&name) {
            // dep: subprocess(sh) — the launcher's own restart hook (it owns the gateway's pidfile and env).
            let ok = Command::new("sh")
                .args(["-c", cmd])
                .status()
                .is_ok_and(|s| s.success());
            match (ok, wait_ready(cfg)) {
                (true, Ok(t)) => {
                    ready_after_restarts.push(format!("{name}:{:.1}s", t.as_secs_f64()))
                }
                (ok, r) => {
                    print_verdict(&Verdict {
                        id: "load_gateway_ready",
                        pass: false,
                        detail: format!("restart before {name}: hook_ok={ok} {r:?}"),
                    });
                    return 1;
                }
            }
        } else if name == "BURST-W" {
            println!(
                "LOAD_SKIP BURST-W: no --restart BURST-W hook (the tree under test registers no admission key)"
            );
            continue;
        }
        if let Some(pair) = name.strip_prefix("ISO-BASE-") {
            println!("{}", iso_noise(cfg, pair, &host));
        }
        let p = run_phase(cfg, db.as_mut(), lock.as_ref(), &phase);
        for l in load_lines(&p, &host) {
            println!("{l}");
        }
        if let Some(l) = lock_line(&p, &host) {
            println!("{l}");
        }
        phases.push(p);
    }
    finish(
        cfg,
        &mut db,
        &phases,
        &lanes,
        &host,
        since.as_deref(),
        &ready_after_restarts,
    )
}

fn finish(
    cfg: &Config,
    db: &mut Option<Client>,
    phases: &[PhaseReport],
    lanes: &[char],
    host: &str,
    since: Option<&str>,
    restarts: &[String],
) -> i32 {
    for (i, (base, burst)) in iso_pairs(phases, cfg.iso_pairs).iter().enumerate() {
        println!(
            "ISO_RATIO pair={} lb_base_p95={base:.1}ms lb_burst_p95={burst:.1}ms ratio={:.2} host={host} measured_at={}",
            i + 1,
            burst / base,
            utc_now()
        );
    }
    let subjects = match (db.as_mut(), since) {
        (Some(c), Some(t)) => lane_subjects(c, t).map_err(|e| eprintln!("load: {e}")).ok(),
        _ => None,
    };
    let mut verdicts = vec![Verdict {
        id: "load_gateway_ready",
        pass: true,
        detail: format!("before the first phase and after restarts {restarts:?}"),
    }];
    verdicts.extend(grade(cfg, phases, lanes, subjects.as_deref()));
    let ran: Vec<String> = phases.iter().map(|p| p.name.clone()).collect();
    let graded: Vec<(Verdict, Option<Vec<String>>)> = verdicts
        .into_iter()
        .map(|v| {
            let na = not_applicable(cfg, &ran, v.id);
            (v, na)
        })
        .collect();
    for (v, na) in &graded {
        match na {
            Some(missing) => println!(
                "ASSERTION NOT_APPLICABLE {}: phases {missing:?} not in --only {:?}",
                v.id, cfg.only
            ),
            None => print_verdict(v),
        }
    }
    let report = serde_json::json!({
        "host": host, "measured_at": utc_now(),
        "note": "counts cover the whole phase; latency percentiles (nearest rank, admitted 2xx only) the window after warm-up",
        "phases": phases.iter().map(phase_json).collect::<Vec<_>>(),
        "lane_subjects_since_start": subjects,
        "assertions": graded.iter().map(|(v, na)| serde_json::json!({
            "id": v.id, "state": if na.is_some() { "not_applicable" } else if v.pass { "pass" } else { "fail" },
            "missing_phases": na, "detail": v.detail,
        })).collect::<Vec<_>>(),
    });
    let text = serde_json::to_string_pretty(&report).unwrap_or_default();
    if let Err(e) = std::fs::write(&cfg.report_path, text) {
        eprintln!("load: cannot write {}: {e}", cfg.report_path);
        return 1;
    }
    i32::from(!graded.iter().all(|(v, na)| na.is_some() || v.pass))
}

/// `cargo xtask load …`
pub fn run(args: &[String]) -> i32 {
    match parse_config(args) {
        Ok(cfg) => execute(&cfg),
        Err(e) => {
            eprintln!("load: {e}\n{USAGE}");
            2
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;
    use std::sync::Mutex;

    /// Read one request (head + Content-Length body) from a stub connection; `None` at EOF.
    fn read_request(r: &mut BufReader<TcpStream>) -> Option<String> {
        let mut head = String::new();
        loop {
            let mut line = String::new();
            if r.read_line(&mut line).ok()? == 0 {
                return None;
            }
            head.push_str(&line);
            if line == "\r\n" {
                break;
            }
        }
        let len = head
            .lines()
            .find_map(|l| {
                l.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|v| v.trim().to_string())
            })
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = vec![0; len];
        r.read_exact(&mut body).ok()?;
        Some(head)
    }

    fn target(port: u16) -> Target {
        Target {
            host_port: format!("127.0.0.1:{port}"),
            path: "/mcp".to_string(),
            origin: format!("http://127.0.0.1:{port}"),
            io_timeout: Duration::from_secs(2),
        }
    }

    fn spec(lane: char, i: usize) -> ClientSpec {
        ClientSpec {
            group: "LA",
            key: Key {
                workspace: "00000000-0000-0000-0000-00000000000a".to_string(),
                bearer: "test-bearer".to_string(),
            },
            forwarded: forwarded_for(lane, i),
            first_op: 0,
        }
    }

    #[test]
    fn keepalive_reads_content_length_and_chunked_bodies() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        // ONE accept: a client that opened a second connection would never be answered and time out.
        let server = std::thread::spawn(move || {
            let (s, _) = listener.accept().expect("accept");
            let mut r = BufReader::new(s);
            read_request(&mut r).expect("first request");
            r.get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst")
                .expect("w1");
            read_request(&mut r).expect("second request");
            r.get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nsec\r\n3;x=1\r\nond\r\n0\r\n\r\n")
                .expect("w2");
            read_request(&mut r)
        });
        let mut conn = Conn::new(&format!("127.0.0.1:{port}"), Duration::from_secs(2));
        let first = conn
            .send("GET /a HTTP/1.1\r\nHost: x\r\n\r\n")
            .expect("first response");
        let second = conn
            .send("GET /b HTTP/1.1\r\nHost: x\r\n\r\n")
            .expect("second response on the same connection");
        assert_eq!((first.status, first.body.as_str()), (200, "first"));
        assert_eq!(
            (second.status, second.body.as_str()),
            (200, "second"),
            "chunked body decoded"
        );
        drop(conn);
        assert!(
            server.join().expect("server").is_none(),
            "the client closed its one connection"
        );
    }

    fn sample(group: &'static str, op: Op, ms: f64, status: u16, outcome: Outcome) -> Sample {
        Sample {
            group,
            op,
            ms,
            status,
            outcome,
            warm: false,
        }
    }

    #[test]
    fn report_carries_n_p50_p95_and_503s_per_level() {
        let mut samples: Vec<Sample> = (1..=100)
            .map(|i| sample("LA", Op::Enumerate, f64::from(i), 200, Outcome::Ok))
            .collect();
        samples
            .extend((1..=60).map(|i| sample("LA", Op::ToolsList, f64::from(i), 200, Outcome::Ok)));
        samples.extend((0..3).map(|_| {
            sample(
                "LA",
                Op::Enumerate,
                5100.0,
                503,
                Outcome::Refused {
                    retry_after: Some("2".to_string()),
                },
            )
        }));
        samples.push(sample(
            "LA",
            Op::Enumerate,
            20.0,
            403,
            Outcome::Failed("TENANT_BOUNDARY".to_string()),
        ));
        samples.push(Sample {
            warm: true,
            ..sample("LA", Op::Enumerate, 9999.0, 200, Outcome::Ok)
        });
        let g = group_report(&samples, "LA", 20_000.0);
        let e = &g.ops["enumerate"];
        assert_eq!(
            (e.n, e.p50, e.p95),
            (100, 50.0, 95.0),
            "warm-up sample excluded from latency"
        );
        assert_eq!(g.ops["tools_list"].n, 60);
        assert_eq!(g.refused_503, 3, "admission 503s counted on their own");
        assert_eq!(
            g.failed_calls,
            BTreeMap::from([("TENANT_BOUNDARY".to_string(), 1)]),
            "503s are not failed_calls"
        );
        assert_eq!(
            (g.retry_after_min, g.retry_after_max, g.retry_after_bad),
            (Some(2), Some(2), 0)
        );
        let p = PhaseReport {
            name: "L8".to_string(),
            clients: 8,
            groups: BTreeMap::from([("LA", g)]),
            ..PhaseReport::default()
        };
        let j = phase_json(&p);
        assert_eq!(j["groups"]["LA"]["ops"]["enumerate"]["n"], 100);
        assert_eq!(j["groups"]["LA"]["ops"]["enumerate"]["p95"], 95.0);
        assert_eq!(j["groups"]["LA"]["admission_503"], 3);
        assert_eq!(j["groups"]["LA"]["failed_calls"]["TENANT_BOUNDARY"], 1);
        let lines = load_lines(&p, "10cpu-vm3.8");
        assert_eq!(lines.len(), 2);
        assert!(
            lines
                .iter()
                .all(|l| l.starts_with("LOAD L8 ") && l.contains("host=10cpu-vm3.8 measured_at=")),
            "{lines:?}"
        );
    }

    #[test]
    fn each_lane_sends_its_own_forwarded_for() {
        use humaux_protocol::edge::{Cidr, TrustedProxyConfig, build_client_network_identity};
        let trusted = TrustedProxyConfig {
            trusted_proxy_cidrs: vec!["127.0.0.1/32".parse::<Cidr>().expect("cidr")],
            max_forwarded_hops: 1,
        };
        let peer: std::net::IpAddr = "127.0.0.1".parse().expect("peer");
        let key = Key {
            workspace: "w".to_string(),
            bearer: "b".to_string(),
        };
        let t = target(1);
        let mut subjects = Vec::new();
        for lane in ['a', 'b'] {
            for i in [0, 7, 127] {
                let wire = request(&t, &key, &forwarded_for(lane, i), Op::Enumerate);
                let header = wire
                    .lines()
                    .find_map(|l| l.strip_prefix("Forwarded: "))
                    .expect("Forwarded header sent");
                let id = build_client_network_identity(peer, Some(header), &trusted, None, None);
                assert!(id.risk_tags.is_empty(), "{header}: {:?}", id.risk_tags);
                let subject = humaux_adapters::quota_repo::preauth_ip_subject(id.client_ip, 64);
                assert_eq!(subject, format!("2001:db8:{lane}::/64"), "{header}");
                subjects.push(subject);
            }
            assert!(
                !request(&t, &key, "x", Op::ToolsList).contains("Connection: close"),
                "keep-alive"
            );
        }
        subjects.dedup();
        assert_eq!(subjects.len(), 2, "the two lanes key apart");
    }

    fn closed_port() -> u16 {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind");
        l.local_addr().expect("addr").port()
    }

    /// A config whose every duration is zero and whose gateway is `127.0.0.1:<port>`.
    fn cfg_for(port: u16, report_path: String) -> Config {
        let key = Key {
            workspace: "w".to_string(),
            bearer: "b".to_string(),
        };
        Config {
            host_port: format!("127.0.0.1:{port}"),
            path: "/mcp".to_string(),
            origin: format!("http://127.0.0.1:{port}"),
            ops_host_port: format!("127.0.0.1:{port}"),
            ops_path: "/metrics".to_string(),
            lanes: BTreeMap::from([('a', vec![key.clone()]), ('b', vec![key])]),
            levels: vec![1],
            burst: 1,
            warmup: Duration::ZERO,
            level: Duration::ZERO,
            burst_secs: Duration::ZERO,
            iso_pairs: 1,
            iso: Duration::ZERO,
            iso_base_clients: 1,
            iso_burst_clients: 1,
            burst_w: Duration::ZERO,
            max_wait_ms: 1.0,
            burst_w_max_wait_ms: 1.0,
            handler_timeout: Duration::from_secs(1),
            pool_max: 1,
            max_backends: 1,
            iso_floor_ms: 2.0,
            ready_timeout: Duration::from_secs(1),
            lock_sample: Duration::from_millis(10),
            pg_dsn: None,
            pg_container: "humaux-c38-load-none".to_string(),
            restarts: BTreeMap::new(),
            report_path,
            only: Vec::new(),
            rfc7239_lane: None,
        }
    }

    /// The S7 runs: `--only` keeps a prefix of the plan, `--rfc7239-lane` adds phase FWD in the form the edge parser
    /// refuses, and assertion 8 grades exactly the lanes the run sent on (fault F-FWD reds it, a / b do not).
    #[test]
    fn only_and_rfc7239_lane_shape_the_plan_and_the_graded_lanes() {
        let mut cfg = cfg_for(closed_port(), String::new());
        cfg.lanes.insert('f', cfg.lanes[&'a'].clone());
        cfg.rfc7239_lane = Some('f');
        cfg.only = vec!["FWD".to_string()];
        let fwd = plan(&cfg);
        let names: Vec<&str> = fwd.iter().map(|p| p.0.as_str()).collect();
        assert_eq!(names, ["FWD"], "--only FWD runs phase FWD alone");
        assert_eq!(lanes_used(&fwd), ['f']);
        let specs = clients(&cfg, &fwd[0].1);
        assert_eq!(specs[0].forwarded, "for=\"[2001:db8:f::1]\"");
        let ab = ["2001:db8:a::/64".to_string(), "2001:db8:b::/64".to_string()];
        let v = a8_lanes(&lanes_used(&fwd), Some(&ab));
        assert!(!v.pass, "no f row: {}", v.detail);
        assert!(
            v.detail.starts_with("want [\"2001:db8:f::/64\"]"),
            "{}",
            v.detail
        );

        cfg.rfc7239_lane = None;
        cfg.only = vec!["ISO".to_string()];
        let iso = plan(&cfg);
        assert!(iso.iter().all(|p| p.0.starts_with("ISO-")) && iso.len() == 2 * cfg.iso_pairs);
        assert_eq!(lanes_used(&iso), ['a', 'b']);
        assert!(a8_lanes(&lanes_used(&iso), Some(&ab)).pass);
        assert_eq!(clients(&cfg, &iso[0].1)[0].forwarded, "2001:db8:b::1");
    }

    /// §57.1: with `--only`, an assertion whose phases the filter dropped is NOT_APPLICABLE and names them; without
    /// `--only` none is, so a phase that never ran in a full run stays a FAIL.
    #[test]
    fn an_assertion_whose_phases_only_dropped_is_not_applicable() {
        let mut cfg = cfg_for(closed_port(), String::new());
        cfg.only = vec!["L".to_string()];
        let planned: Vec<String> = plan(&cfg).into_iter().map(|p| p.0).collect();
        assert_eq!(planned, ["L1"]);
        assert_eq!(
            not_applicable(&cfg, &planned, "load_no_pool_or_statement_timeout"),
            None
        );
        assert_eq!(
            not_applicable(&cfg, &planned, "load_tenant_isolation"),
            Some(vec!["ISO-BASE-1".to_string(), "ISO-BURST-1".to_string()])
        );
        assert_eq!(
            not_applicable(&cfg, &planned, "load_overflow_503_within_max_wait"),
            Some(vec!["BURST".to_string(), "BURST-W".to_string()])
        );
        assert_eq!(
            not_applicable(&cfg, &planned, "load_forwarded_lanes_keyed"),
            None
        );
        cfg.only.clear();
        assert_eq!(
            not_applicable(&cfg, &planned, "load_tenant_isolation"),
            None,
            "without --only a phase that did not run is a FAIL, never NOT_APPLICABLE"
        );
    }

    #[test]
    fn no_report_without_a_ready_gateway() {
        let port = closed_port();
        let report =
            std::env::temp_dir().join(format!("c38-load-report-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&report);
        let cfg = cfg_for(port, report.display().to_string());
        let code = execute(&cfg);
        let written = report.exists();
        let _ = std::fs::remove_file(&report);
        assert_ne!(
            code, 0,
            "a gateway that never answered /readyz is a failed run"
        );
        assert!(!written, "no load-report.json without a ready gateway");
    }

    #[test]
    fn a_503_pauses_the_client_for_retry_after() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let arrivals = std::sync::Arc::new(Mutex::new(Vec::new()));
        let seen = arrivals.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(s) = stream else { return };
                let mut r = BufReader::new(s);
                while read_request(&mut r).is_some() {
                    let mut a = seen.lock().expect("lock");
                    a.push(Instant::now());
                    let reply: &[u8] = if a.len() == 1 {
                        b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 1\r\nContent-Type: text/plain\r\nContent-Length: 12\r\n\r\nRATE_LIMITED"
                    } else {
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"
                    };
                    drop(a);
                    if r.get_mut().write_all(reply).is_err() {
                        break;
                    }
                }
            }
        });
        let now = Instant::now();
        let samples = client_loop(
            &target(port),
            &spec('a', 0),
            now,
            now + Duration::from_millis(1500),
        );
        let a = arrivals.lock().expect("lock");
        assert!(
            matches!(
                samples.first().map(|s| &s.outcome),
                Some(Outcome::Refused { .. })
            ),
            "{samples:?}"
        );
        assert!(
            a.len() >= 2,
            "the client resumed after the pause: {} requests",
            a.len()
        );
        let gap = a[1] - a[0];
        assert!(
            gap >= Duration::from_millis(950),
            "next call {gap:?} after a 503 with Retry-After: 1"
        );
    }

    #[test]
    fn assertion_8_requires_every_lane_that_ran() {
        let got = vec![
            "2001:db8:a::/64".to_string(),
            "2001:db8:b::/64".to_string(),
            "2001:db8:fb4b:46ad::/64".to_string(),
        ];
        assert!(a8_lanes(&['a', 'b'], Some(&got)).pass);
        assert!(
            !a8_lanes(&['a', 'b', 'f'], Some(&got)).pass,
            "a lane whose Forwarded did not parse has no row"
        );
        assert!(!a8_lanes(&['a', 'b'], None).pass);
    }

    /// Assertion 3 compares the scraped `admission_rejected_total` delta with the client's own 503 count per phase:
    /// equal passes; a scrape that saw fewer (the wrong ops endpoint, a lost increment) or none fails, naming the
    /// phase. Fault: the comparison reduced to "a scrape exists" ⇒ the mismatch passes ⇒ red.
    #[test]
    fn assertion_3_fails_when_the_scrape_differs_from_the_client_503s() {
        let phase = |name: &str, refused: usize, delta: Option<f64>| PhaseReport {
            name: name.to_string(),
            groups: BTreeMap::from([(
                "LA",
                GroupReport {
                    refused_503: refused,
                    ..GroupReport::default()
                },
            )]),
            admission_delta: delta.map(|d| BTreeMap::from([("queue_full".to_string(), d)])),
            ..PhaseReport::default()
        };
        assert!(a3_counter(&[phase("BURST", 960, Some(960.0))]).pass);
        let v = a3_counter(&[
            phase("BURST", 960, Some(960.0)),
            phase("ISO-BURST-1", 1120, Some(0.0)),
        ]);
        assert!(
            !v.pass,
            "a scrape that missed every 503 passed: {}",
            v.detail
        );
        assert_eq!(v.detail, "ISO-BURST-1: scraped=0 client_503=1120");
        assert!(!a3_counter(&[phase("BURST", 3, None)]).pass, "no scrape");
    }

    /// The `LOCKWAIT` quantile ranks over EVERY acquisition: the ones the sampler never saw waiting waited less than
    /// one interval and rank below the observed waits. 1000 acquisitions, 11 observed: p99 is the 10th longest
    /// observed wait, p50 is below the resolution. Fault: the quantile taken over the observed waits only ⇒ p99 =
    /// the longest wait, p50 an observed one ⇒ red.
    #[test]
    fn lock_wait_quantile_ranks_unobserved_acquisitions_below_the_resolution() {
        let observed = [
            11.0, 500.0, 12.0, 11.0, 300.0, 15.0, 11.0, 400.0, 20.0, 11.0, 11.0,
        ];
        assert_eq!(wait_quantile(&observed, 1000, 99), Some(11.0));
        assert_eq!(wait_quantile(&observed, 1000, 50), None, "below resolution");
        assert_eq!(
            wait_quantile(&observed, 100, 99),
            Some(500.0),
            "rank 1 = the longest"
        );
        assert_eq!(wait_quantile(&[], 0, 99), None, "no acquisition, no wait");
        let p = PhaseReport {
            name: "L64".to_string(),
            measured_at: "2026-10-07T00:00:00Z".to_string(),
            lock: Some(LockWaits {
                tenant: "t".to_string(),
                interval_ms: 10.0,
                samples: 4500,
                acquisitions: 1000,
                tenant_waits_ms: observed.to_vec(),
                other_waits_ms: vec![3.0],
                error: None,
            }),
            ..PhaseReport::default()
        };
        assert_eq!(
            lock_line(&p, "h").as_deref(),
            Some(
                "LOCKWAIT L64 lock=tenant group=LA acquisitions=1000 observed_waits=11 p50=<10ms p99=11.0ms max=500.0ms other_rate_lock_waits=1 other_max=3.0ms resolution=10ms samples=4500 host=h measured_at=2026-10-07T00:00:00Z"
            )
        );
    }
}
