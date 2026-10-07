//! `telemetry::metrics` — the §41.2 family table this workspace exports, a hand-written Prometheus text-format
//!   0.0.4 encoder, and the std-only loopback listener that serves `/metrics` and `/status` (ADR-0061 D-A, D-B).
//! Depends-on: crates=[]; services=[HTTP(loopback)]; env=[]; modules=[telemetry::degrade]
//! Called-by: [admin::cell_resources, admin::ops_status, consolidation-worker::main, gateway::bootstrap,
//!   gateway::guard, gateway::main, maintenance::health_serve, maintenance::resident, maintenance::serve,
//!   private-worker::main, retrieval-worker::main, retrieval::completeness, telemetry::admission, telemetry::degrade,
//!   telemetry::dr, telemetry::health, tests]
//! Invariants: [every exported family is a `families::*` const and nowhere else; label values are `&'static str`
//!   from a closed enum; the listener refuses a non-loopback address and names the config key]
//! Spec: Baseline §41.2; §53.5; ADR-0061 D-A; ADR-0061 D-B
//!
//! No metrics crate is linked (ADR-0061 D-A): the exposition is a few `write!` calls, and a
//! crate's registry would be a second family table next to [`families`]. Label values are
//! `&'static str` so a tenant id, user id or free text cannot become a label without
//! `Box::leak` — the cardinality and privacy bound is carried by the type.

use std::fmt::Write as _;
use std::io::{self, Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::degrade::{DegradeCode, degrade_last_fired_unix, degrade_total_count};

/// The exposition content type Prometheus 3 requires on a scrape (ADR-0061 research W4).
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// §41.2 「量纲」 column, first segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Monotonic count; resets only on process restart.
    Counter,
    /// Last sampled value.
    Gauge,
    /// Rendered as one `le="+Inf"` bucket plus `_sum` and `_count` (ADR-0061 D-A).
    Histogram,
}

impl Kind {
    /// The `# TYPE` word.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

/// One §41.2 family: name, HELP text, kind and the full label-key set (§41.2 R6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Family {
    /// §41.2 name, verbatim.
    pub name: &'static str,
    /// `# HELP` text.
    pub help: &'static str,
    /// §41.2 kind.
    pub kind: Kind,
    /// §41.2 label keys, in render order.
    pub labels: &'static [&'static str],
}

/// Every family this workspace exports, one const per family (ADR-0061 D-A: the only family table;
/// `cargo xtask metrics-registry --check` D7 compares what the processes render against §41.2).
pub mod families {
    use super::{Family, Kind};

    const fn f(
        name: &'static str,
        help: &'static str,
        kind: Kind,
        labels: &'static [&'static str],
    ) -> Family {
        Family {
            name,
            help,
            kind,
            labels,
        }
    }

    // §41.2: §53.1 abstain() — gateway.
    /// `degrade_total{code}`.
    pub const DEGRADE_TOTAL: Family = f(
        "degrade_total",
        "Fail-open exits through abstain(), by DegradeCode variant name.",
        Kind::Counter,
        &["code"],
    );
    // §41.2: §20 envelope returns — gateway.
    /// `humaux_retrieval_requests_total{intent,completeness_class}`.
    pub const HUMAUX_RETRIEVAL_REQUESTS_TOTAL: Family = f(
        "humaux_retrieval_requests_total",
        "Retrieval envelopes returned, by input intent and completeness class.",
        Kind::Counter,
        &["intent", "completeness_class"],
    );
    // §41.2: §22.5 record_final_classification — gateway.
    /// `retrieval_completeness_total{class,reason}`.
    pub const RETRIEVAL_COMPLETENESS_TOTAL: Family = f(
        "retrieval_completeness_total",
        "Final completeness classifications, by class and reason.",
        Kind::Counter,
        &["class", "reason"],
    );
    // §41.2: §33 tool call return — gateway.
    /// `humaux_mcp_requests_total{tool,result,plan_class}`.
    pub const HUMAUX_MCP_REQUESTS_TOTAL: Family = f(
        "humaux_mcp_requests_total",
        "MCP tool calls, by tool, result and plan class.",
        Kind::Counter,
        &["tool", "result", "plan_class"],
    );
    // §41.2: §74 authentication decision — gateway.
    /// `mcp_auth_attempts_total{result,flow}`.
    pub const MCP_AUTH_ATTEMPTS_TOTAL: Family = f(
        "mcp_auth_attempts_total",
        "Authentication decisions, by result and flow.",
        Kind::Counter,
        &["result", "flow"],
    );
    // §41.2: §74 authorization denial — gateway.
    /// `mcp_authz_denied_total{reason}`.
    pub const MCP_AUTHZ_DENIED_TOTAL: Family = f(
        "mcp_authz_denied_total",
        "Authorization denials, by reason.",
        Kind::Counter,
        &["reason"],
    );
    // §41.2: §72.1 reservation — gateway.
    /// `mcp_quota_reservations_total{result}`.
    pub const MCP_QUOTA_RESERVATIONS_TOTAL: Family = f(
        "mcp_quota_reservations_total",
        "Quota reservations, by result.",
        Kind::Counter,
        &["result"],
    );
    // §41.2: §72.1 settlement — gateway.
    /// `mcp_bmo_consumed_total{plan_class}`.
    pub const MCP_BMO_CONSUMED_TOTAL: Family = f(
        "mcp_bmo_consumed_total",
        "Billable memory operations consumed, by plan class.",
        Kind::Counter,
        &["plan_class"],
    );
    // §41.2: §72 RATE LIMIT decision — gateway.
    /// `rate_limit_rejected_total{scope}`.
    pub const RATE_LIMIT_REJECTED_TOTAL: Family = f(
        "rate_limit_rejected_total",
        "Rate-limit rejections, by scope.",
        Kind::Counter,
        &["scope"],
    );
    // §41.2: §67 admission control 返 503 处 — gateway (ADR-0065 D-C).
    /// `admission_rejected_total{class,reason}`.
    pub const ADMISSION_REJECTED_TOTAL: Family = f(
        "admission_rejected_total",
        "Requests the admission layer refused with 503, by traffic class and reason.",
        Kind::Counter,
        &["class", "reason"],
    );
    // §41.2: §19 provider call end — retrieval worker.
    /// `retrieval_provider_requests_total{provider,purpose,region,result}`.
    pub const RETRIEVAL_PROVIDER_REQUESTS_TOTAL: Family = f(
        "retrieval_provider_requests_total",
        "Retrieval provider calls, by provider, purpose, region and result.",
        Kind::Counter,
        &["provider", "purpose", "region", "result"],
    );
    // §41.2: §19 provider call end — retrieval worker.
    /// `retrieval_provider_latency_seconds{provider,purpose,region}`.
    pub const RETRIEVAL_PROVIDER_LATENCY_SECONDS: Family = f(
        "retrieval_provider_latency_seconds",
        "Retrieval provider call latency in seconds.",
        Kind::Histogram,
        &["provider", "purpose", "region"],
    );
    // §41.2: §19 provider call end — retrieval worker.
    /// `retrieval_provider_tokens_total{provider,purpose}`.
    pub const RETRIEVAL_PROVIDER_TOKENS_TOTAL: Family = f(
        "retrieval_provider_tokens_total",
        "Retrieval provider tokens, by provider and purpose.",
        Kind::Counter,
        &["provider", "purpose"],
    );
    // §41.2: §19 provider call end — retrieval worker.
    /// `retrieval_provider_cost_total{provider,purpose,currency}`.
    pub const RETRIEVAL_PROVIDER_COST_TOTAL: Family = f(
        "retrieval_provider_cost_total",
        "Retrieval provider cost in the currency's minor unit.",
        Kind::Counter,
        &["provider", "purpose", "currency"],
    );
    // §41.2: §31 queue sample (ops.health_snapshot) — maintenance health serve.
    /// `jobs_pending`.
    pub const JOBS_PENDING: Family = f("jobs_pending", "Queue jobs in PENDING.", Kind::Gauge, &[]);
    // §41.2: §31 queue sample — maintenance health serve.
    /// `jobs_processing`.
    pub const JOBS_PROCESSING: Family = f(
        "jobs_processing",
        "Queue jobs in PROCESSING.",
        Kind::Gauge,
        &[],
    );
    // §41.2: §31 queue sample — maintenance health serve.
    /// `jobs_waiting_key`.
    pub const JOBS_WAITING_KEY: Family = f(
        "jobs_waiting_key",
        "Queue jobs in WAITING_KEY.",
        Kind::Gauge,
        &[],
    );
    // §41.2: §31 queue sample — maintenance health serve.
    /// `jobs_dead`.
    pub const JOBS_DEAD: Family = f("jobs_dead", "Queue jobs in DEAD.", Kind::Gauge, &[]);
    // §41.2: §31 queue sample — maintenance health serve.
    /// `oldest_pending_age_seconds`.
    pub const OLDEST_PENDING_AGE_SECONDS: Family = f(
        "oldest_pending_age_seconds",
        "Age in seconds of the oldest PENDING queue job; 0 when none.",
        Kind::Gauge,
        &[],
    );
    // §41.2: §16 periodic sample — maintenance health serve.
    /// `projection_lag_events`.
    pub const PROJECTION_LAG_EVENTS: Family = f(
        "projection_lag_events",
        "Stream events issued but not yet contiguously projected, across all streams.",
        Kind::Gauge,
        &[],
    );
    // §41.2: §15.4 processing_gaps view — maintenance health serve.
    /// `processing_gap_count{stream}`.
    pub const PROCESSING_GAP_COUNT: Family = f(
        "processing_gap_count",
        "Open processing gaps, by ticket family domain.",
        Kind::Gauge,
        &["stream"],
    );
    // §41.2: §7.4 finalized_at written — maintenance health serve.
    /// `data_disclosures_finalized_total{outcome}`.
    pub const DATA_DISCLOSURES_FINALIZED_TOTAL: Family = f(
        "data_disclosures_finalized_total",
        "Data disclosures finalized since process start, by outcome.",
        Kind::Counter,
        &["outcome"],
    );
    // §41.2: §7.4 open reservations — maintenance health serve.
    /// `data_disclosures_reserved_unfinalized{age_bucket}`.
    pub const DATA_DISCLOSURES_RESERVED_UNFINALIZED: Family = f(
        "data_disclosures_reserved_unfinalized",
        "Reserved, not finalized data disclosures, by reservation age bucket.",
        Kind::Gauge,
        &["age_bucket"],
    );
    // §41.2: §11 每次 run — private worker (card 34b; emit `adapters::distill_repo::finish_processing_run`).
    /// `private_distill_runs_total`.
    pub const PRIVATE_DISTILL_RUNS_TOTAL: Family = f(
        "private_distill_runs_total",
        "Distill processing runs finished, whatever their output count.",
        Kind::Counter,
        &[],
    );
    // §41.2: §11 每条产出 — private worker (card 34b; emit `adapters::distill_repo::insert_memory`).
    /// `private_distill_outputs_total`.
    pub const PRIVATE_DISTILL_OUTPUTS_TOTAL: Family = f(
        "private_distill_outputs_total",
        "Memory records the distill hop wrote, one per record.",
        Kind::Counter,
        &[],
    );
    // §41.2: §11 推理调用返回读 usage — private worker (card 34b; emit
    // `adapters::model_call_ledger::finalize_private_call`).
    /// `private_reasoning_usage_total`.
    pub const PRIVATE_REASONING_USAGE_TOTAL: Family = f(
        "private_reasoning_usage_total",
        "Tokens private reasoning providers reported (input + output) on finalized calls.",
        Kind::Counter,
        &[],
    );
    // §41.2: §4.2 every door call of the resident daemon — `humaux-maintenance --serve` (card 35; emit
    // `adapters::maintenance_repo::count_task_call`, ADR-0062 D-S).
    /// `maintenance_task_runs_total{task,outcome}`.
    pub const MAINTENANCE_TASK_RUNS_TOTAL: Family = f(
        "maintenance_task_runs_total",
        "Scheduled maintenance door calls (one per tenant), by D-C task and outcome.",
        Kind::Counter,
        &["task", "outcome"],
    );
    // §41.2: §4.2 the same door call's affected rows — `humaux-maintenance --serve` (card 35, ADR-0062 D-S).
    /// `maintenance_task_rows_total{task}`.
    pub const MAINTENANCE_TASK_ROWS_TOTAL: Family = f(
        "maintenance_task_rows_total",
        "Rows the scheduled maintenance doors affected (deleted, swept, reaped, reissued, re-driven), by D-C task.",
        Kind::Counter,
        &["task"],
    );
    // §41.2: the PARTITIONS run of `humaux-maintenance --serve` (card 36; emit
    // `adapters::maintenance_repo::set_partition_horizon_months`, ADR-0063 D-K; a failed run renders no sample).
    /// `partition_horizon_months{table}`.
    pub const PARTITION_HORIZON_MONTHS: Family = f(
        "partition_horizon_months",
        "Whole months of pre-created partitions after the current UTC month, per §48.1 table_key (-1: no leaf).",
        Kind::Gauge,
        &["table"],
    );
    // §41.2: §44 the DR_EVIDENCE run of `humaux-maintenance --serve` (card 37; emit `telemetry::dr::publish`,
    // ADR-0064 D-K / 10.11 D). `target` is the one value `telemetry::dr::TARGET_LOCAL` (ruling E17).
    /// `backup_last_success_timestamp_seconds{target}`.
    pub const BACKUP_LAST_SUCCESS_TIMESTAMP_SECONDS: Family = f(
        "backup_last_success_timestamp_seconds",
        "Stop time of the newest backup set whose latest verification is VERIFIED (0: never).",
        Kind::Gauge,
        &["target"],
    );
    // §41.2: §44 the same DR_EVIDENCE run (ADR-0064 D-K).
    /// `restore_drill_last_success_timestamp_seconds{target}`.
    pub const RESTORE_DRILL_LAST_SUCCESS_TIMESTAMP_SECONDS: Family = f(
        "restore_drill_last_success_timestamp_seconds",
        "Finish time of the newest restore drill the table derived as succeeded (0: never).",
        Kind::Gauge,
        &["target"],
    );
    // §41.2: §44 the same DR_EVIDENCE run (ADR-0064 E15.6 / 10.11 D).
    /// `backup_repo_bytes`.
    pub const BACKUP_REPO_BYTES: Family = f(
        "backup_repo_bytes",
        "Repository bytes as the newest backup arm run measured them (refusals included; 0: no run).",
        Kind::Gauge,
        &[],
    );
    // §41.2: §44 the same DR_EVIDENCE run, live `df -Pk` (ADR-0064 E15.6 / 10.11 D).
    /// `backup_disk_free_bytes{volume}`.
    pub const BACKUP_DISK_FREE_BYTES: Family = f(
        "backup_disk_free_bytes",
        "Free bytes of the repository filesystem and of the PGDATA filesystem, by volume.",
        Kind::Gauge,
        &["volume"],
    );
    // §41.2: §44 the same DR_EVIDENCE run (ADR-0064 D-V / 10.11 D).
    /// `backup_budget_headroom_bytes{limit}`.
    pub const BACKUP_BUDGET_HEADROOM_BYTES: Family = f(
        "backup_budget_headroom_bytes",
        "Bytes left under each backup budget limit after the next set's estimate (< 0: the next backup is refused).",
        Kind::Gauge,
        &["limit"],
    );
    // §41.2: §44 the same DR_EVIDENCE run, latched in ops.wal_archive_failures (ADR-0064 10.11 D).
    /// `wal_archive_failing`.
    pub const WAL_ARCHIVE_FAILING: Family = f(
        "wal_archive_failing",
        "1 while a WAL archive failure is latched (until a later VERIFIED full) or archiving fails now, else 0.",
        Kind::Gauge,
        &[],
    );
}

/// Process-local counters, one slot per closed label value (index = the enum's `ALL` position).
pub(crate) struct Counters<const N: usize>([AtomicU64; N]);

impl<const N: usize> Counters<N> {
    pub(crate) const fn new() -> Self {
        Self([const { AtomicU64::new(0) }; N])
    }

    pub(crate) fn inc(&self, slot: usize, by: u64) {
        self.0[slot].fetch_add(by, Ordering::Relaxed);
    }

    pub(crate) fn get(&self, slot: usize) -> u64 {
        self.0[slot].load(Ordering::Relaxed)
    }
}

/// Process-local gauges (f64 bits), one slot per closed label value.
pub(crate) struct Gauges<const N: usize>([AtomicU64; N]);

impl<const N: usize> Gauges<N> {
    pub(crate) const fn new() -> Self {
        Self([const { AtomicU64::new(0) }; N])
    }

    pub(crate) fn set(&self, slot: usize, value: f64) {
        self.0[slot].store(value.to_bits(), Ordering::Relaxed);
    }

    pub(crate) fn get(&self, slot: usize) -> f64 {
        f64::from_bits(self.0[slot].load(Ordering::Relaxed))
    }
}

/// Label-value escaping of the text format: `\` → `\\`, `"` → `\"`, newline → `\n`.
pub fn escape_label_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

/// HELP escaping of the text format: `\` → `\\`, newline → `\n` (quotes stay literal).
pub fn escape_help(help: &str) -> String {
    help.replace('\\', "\\\\").replace('\n', "\\n")
}

fn write_header(out: &mut String, f: &Family) {
    let _ = writeln!(out, "# HELP {} {}", f.name, escape_help(f.help));
    let _ = writeln!(out, "# TYPE {} {}", f.name, f.kind.as_str());
}

fn write_sample(
    out: &mut String,
    name: &str,
    suffix: &str,
    keys: &[&str],
    values: &[&'static str],
    le: Option<&str>,
    value: f64,
) {
    debug_assert_eq!(keys.len(), values.len(), "{name}: label arity");
    let _ = write!(out, "{name}{suffix}");
    let mut pairs = keys
        .iter()
        .zip(values)
        .map(|(k, v)| format!("{k}=\"{}\"", escape_label_value(v)))
        .chain(le.map(|le| format!("le=\"{le}\"")))
        .peekable();
    if pairs.peek().is_some() {
        let joined: Vec<String> = pairs.collect();
        let _ = write!(out, "{{{}}}", joined.join(","));
    }
    let _ = writeln!(out, " {}", format_value(value));
}

fn format_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 { "+Inf" } else { "-Inf" }.to_string()
    } else {
        format!("{v}")
    }
}

/// `# HELP`, `# TYPE` and one line per sample. Each sample carries one value per `f.labels` key,
/// in the same order (§41.2 R6).
pub fn write_family(out: &mut String, f: &Family, samples: &[(&[&'static str], f64)]) {
    write_header(out, f);
    for (values, value) in samples {
        write_sample(out, f.name, "", f.labels, values, None, *value);
    }
}

/// A single-label family seeded over a closed value set: one sample per value, in order
/// (ADR-0061 D-A seeding rule — every value is present from the first scrape).
pub fn write_single_label(
    out: &mut String,
    f: &Family,
    label_values: &[&'static str],
    value_at: impl Fn(usize) -> f64,
) {
    let samples: Vec<(&[&'static str], f64)> = label_values
        .iter()
        .enumerate()
        .map(|(i, v)| (std::slice::from_ref(v), value_at(i)))
        .collect();
    write_family(out, f, &samples);
}

/// A histogram family with count and sum only, rendered as one `le="+Inf"` bucket.
// ponytail: +Inf bucket only — no quantiles; upgrade = a §78 bucket-boundary key and real buckets
// in HistogramFamily when §54 p95 needs it.
pub fn write_histogram(out: &mut String, f: &Family, samples: &[(&[&'static str], u64, f64)]) {
    write_header(out, f);
    for (values, count, sum) in samples {
        let count = *count as f64;
        write_sample(
            out,
            f.name,
            "_bucket",
            f.labels,
            values,
            Some("+Inf"),
            count,
        );
        write_sample(out, f.name, "_sum", f.labels, values, None, *sum);
        write_sample(out, f.name, "_count", f.labels, values, None, count);
    }
}

/// One route's renderer: `Ok(body)` is served 200, `Err(text)` 503 with the text.
pub type Render = Box<dyn Fn() -> Result<String, String> + Send + Sync>;

/// The two routes every process serves on its ops listener (ADR-0061 D-B).
pub struct Routes {
    /// `GET /metrics`, served as [`CONTENT_TYPE`].
    pub metrics: Render,
    /// `GET /status`, served as `application/json`.
    pub status: Render,
}

/// A running ops listener. Dropping it stops the thread and closes the port.
pub struct OpsListener {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl OpsListener {
    /// The bound address (the configured one; with port 0, the port the OS picked).
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for OpsListener {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        // dep: HTTP(loopback) — one self-dial wakes the blocking accept so the thread sees `stop`
        let _ = TcpStream::connect_timeout(&self.addr, IO_TIMEOUT);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The ops listener's per-connection budget. ADR-0061 D-B: one connection's head read and response write together
/// end within this (a total deadline, not a per-read timeout), so it bounds how long one stuck or trickling loopback
/// client can delay the next scrape. A `/status` client (ADR-0061 D-J) spends no longer on one fetch either.
pub const IO_TIMEOUT: Duration = Duration::from_secs(2);
// ADR-0061 D-B: request head bound; a GET from Prometheus or curl is a few hundred bytes.
const MAX_REQUEST_HEAD: usize = 8 * 1024;

/// Parses a `*_METRICS_ADDR` value (ADR-0061 D-B): a socket address on loopback with a fixed, non-zero port, since
/// Prometheus targets a fixed port and port 0 would bind a random one. The one parser every process calls; the error
/// is the reason only, the caller names its key.
///
/// # Errors
/// `invalid socket address`, or the loopback-and-fixed-port reason.
pub fn parse_ops_addr(value: &str) -> Result<SocketAddr, &'static str> {
    let addr = value
        .parse::<SocketAddr>()
        .map_err(|_| "invalid socket address")?;
    if !addr.ip().is_loopback() || addr.port() == 0 {
        return Err("must be a loopback address with a fixed port (ADR-0061 D-B)");
    }
    Ok(addr)
}

/// Binds `addr` and serves `/metrics` and `/status` on one std thread (ADR-0061 D-B). `key` is the
/// §78 config key `addr` came from; every refusal names it. A non-loopback address is refused,
/// a bind failure is returned as is (no fallback port).
// ponytail: one connection at a time; ceiling = a stuck or trickling loopback client delays a scrape ≤2 s;
// upgrade to a tokio listener if a scrape timeout is ever observed.
pub fn serve_loopback(key: &str, addr: SocketAddr, routes: Routes) -> io::Result<OpsListener> {
    if !addr.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{key}={addr}: the ops listener binds a loopback address only (ADR-0061 D-B)"),
        ));
    }
    let listener = TcpListener::bind(addr)
        .map_err(|e| io::Error::new(e.kind(), format!("{key}={addr}: bind failed: {e}")))?;
    let addr = listener.local_addr()?;
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let thread = std::thread::Builder::new()
        .name("ops-listener".into())
        .spawn(move || {
            for conn in listener.incoming() {
                if thread_stop.load(Ordering::Acquire) {
                    break;
                }
                if let Ok(stream) = conn {
                    answer(stream, &routes);
                }
            }
        })?;
    Ok(OpsListener {
        addr,
        stop,
        thread: Some(thread),
    })
}

fn answer(mut stream: TcpStream, routes: &Routes) {
    // ADR-0061 review-fix 3 (F8): one total deadline for the head read and the response write. A
    // per-read timeout alone lets a client sending one byte per second hold the single thread forever.
    let deadline = Instant::now() + IO_TIMEOUT;
    let remaining = || {
        deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
    };
    // Read the whole head before answering: closing with unread bytes makes the kernel send a
    // reset, which a client may see instead of the response.
    let mut head = Vec::with_capacity(512);
    let mut chunk = [0u8; 1024];
    while head.len() < MAX_REQUEST_HEAD && !head.windows(4).any(|w| w == b"\r\n\r\n") {
        let Some(left) = remaining() else { return };
        // macOS refuses setsockopt (EINVAL) once the peer has shut down its side; the read then cannot
        // block, and the previous timeout still bounds it.
        let _ = stream.set_read_timeout(Some(left));
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => head.extend_from_slice(&chunk[..n]),
        }
    }
    let head = String::from_utf8_lossy(&head);
    let mut words = head.lines().next().unwrap_or("").split(' ');
    let method = words.next().unwrap_or("");
    let path = words.next().unwrap_or("").split('?').next().unwrap_or("");
    let route = match (method, path) {
        ("GET", "/metrics") => Some((&routes.metrics, CONTENT_TYPE)),
        ("GET", "/status") => Some((&routes.status, "application/json")),
        _ => None,
    };
    let (code, content_type, body) = match route {
        Some((render, content_type)) => match render() {
            Ok(body) => ("200 OK", content_type, body),
            Err(text) => ("503 Service Unavailable", "text/plain; charset=utf-8", text),
        },
        None => (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n".to_string(),
        ),
    };
    let response = format!(
        "HTTP/1.1 {code}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut rest = response.as_bytes();
    while !rest.is_empty() {
        let Some(left) = remaining() else { return };
        // As above: a refused setsockopt means the peer is gone, and the write fails at once.
        let _ = stream.set_write_timeout(Some(left));
        match stream.write(rest) {
            Ok(0) | Err(_) => return,
            Ok(n) => rest = &rest[n..],
        }
    }
    let _ = stream.flush();
}

/// [`Routes`] for a process whose ops listener carries no process-specific state (ADR-0061 D-B): `/metrics` is
/// `render` at the current counters; `/status` is the identity document every process serves plus
/// `degrade_total` per code with its last fire time. `crate_version` and `git_sha` are the calling binary's own
/// build values (`env!("CARGO_PKG_VERSION")`, `option_env!("HUMAUX_BUILD_GIT_SHA")`).
pub fn process_routes(
    process: &'static str,
    mode: &'static str,
    crate_version: &'static str,
    git_sha: Option<&'static str>,
    render: fn(&mut String),
) -> Routes {
    let started = SystemTime::now();
    Routes {
        metrics: Box::new(move || {
            let mut out = String::new();
            render(&mut out);
            Ok(out)
        }),
        status: Box::new(move || Ok(status_json(process, mode, crate_version, git_sha, started))),
    }
}

fn status_json(
    process: &str,
    mode: &str,
    crate_version: &str,
    git_sha: Option<&str>,
    started: SystemTime,
) -> String {
    let started_at = started
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let uptime = started.elapsed().map_or(0, |d| d.as_secs());
    let git_sha = git_sha.map_or_else(|| "null".to_string(), json_str);
    let mut out = format!(
        "{{\"process\":{},\"mode\":{},\"crate_version\":{},\"git_sha\":{git_sha},\"started_at\":{started_at},\
         \"uptime_seconds\":{uptime},\"degrade\":{{",
        json_str(process),
        json_str(mode),
        json_str(crate_version),
    );
    for (i, &code) in DegradeCode::ALL.iter().enumerate() {
        let last =
            degrade_last_fired_unix(code).map_or_else(|| "null".to_string(), |t| t.to_string());
        let _ = write!(
            out,
            "{}{}:{{\"count\":{},\"last_fired_at\":{last}}}",
            if i == 0 { "" } else { "," },
            json_str(code.as_str()),
            degrade_total_count(code),
        );
    }
    out.push_str("}}");
    out
}

/// A JSON string literal: `"` and `\` escaped, every control character as `\u00XX` (RFC 8259 §7). No JSON crate is
/// linked here (ADR-0061 D-A).
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            c if c.is_control() => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::mpsc;

    fn unescape(s: &str) -> String {
        let mut out = String::new();
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.next() {
                    Some('n') => out.push('\n'),
                    Some(other) => out.push(other),
                    None => out.push('\\'),
                }
            } else {
                out.push(c);
            }
        }
        out
    }

    /// T-A1: label values escape `\`, `"` and newline, and the escaped form round-trips.
    #[test]
    fn label_value_escaping_round_trips() {
        let raw = "a\"b\\c\nd";
        let escaped = escape_label_value(raw);
        assert_eq!(escaped, "a\\\"b\\\\c\\nd");
        assert!(
            !escaped.contains('\n'),
            "a raw newline would end the sample line"
        );
        assert_eq!(unescape(&escaped), raw);
        assert_eq!(escape_help("x\\y\nz \"q\""), "x\\\\y\\nz \"q\"");
    }

    /// T-A3: a count+sum histogram renders the `+Inf` bucket equal to `_count`.
    #[test]
    fn histogram_renders_inf_bucket_equal_to_count() {
        let mut out = String::new();
        write_histogram(
            &mut out,
            &families::RETRIEVAL_PROVIDER_LATENCY_SECONDS,
            &[(&["dashscope", "embedding", "cn-hangzhou"], 3, 1.5)],
        );
        let labels = "provider=\"dashscope\",purpose=\"embedding\",region=\"cn-hangzhou\"";
        assert!(out.contains("# TYPE retrieval_provider_latency_seconds histogram\n"));
        assert!(
            out.contains(&format!(
                "retrieval_provider_latency_seconds_bucket{{{labels},le=\"+Inf\"}} 3\n"
            )),
            "{out}"
        );
        assert!(out.contains(&format!(
            "retrieval_provider_latency_seconds_sum{{{labels}}} 1.5\n"
        )));
        assert!(out.contains(&format!(
            "retrieval_provider_latency_seconds_count{{{labels}}} 3\n"
        )));
    }

    #[test]
    fn unlabeled_family_renders_a_bare_sample() {
        let mut out = String::new();
        write_family(&mut out, &families::JOBS_DEAD, &[(&[], 2.0)]);
        assert_eq!(
            out,
            "# HELP jobs_dead Queue jobs in DEAD.\n# TYPE jobs_dead gauge\njobs_dead 2\n"
        );
    }

    fn get(addr: SocketAddr, path: &str) -> String {
        // dep: HTTP(loopback) — test client against the in-process ops listener
        let mut s = TcpStream::connect(addr).expect("connect");
        write!(s, "GET {path} HTTP/1.1\r\nHost: {addr}\r\n\r\n").unwrap();
        let mut resp = String::new();
        s.read_to_string(&mut resp).unwrap();
        resp
    }

    fn loopback0() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
    }

    fn routes(metrics: Result<&'static str, &'static str>) -> Routes {
        Routes {
            metrics: Box::new(move || metrics.map(String::from).map_err(String::from)),
            status: Box::new(|| Ok("{\"process\":\"test\"}".to_string())),
        }
    }

    /// T-B1: `/metrics` carries the exact content type, `/status` is JSON, anything else 404.
    #[test]
    fn serves_metrics_status_and_404() {
        let ops = serve_loopback("TEST_METRICS_ADDR", loopback0(), routes(Ok("m 1\n"))).unwrap();
        let metrics = get(ops.local_addr(), "/metrics");
        assert!(metrics.starts_with("HTTP/1.1 200 OK\r\n"), "{metrics}");
        assert!(
            metrics.contains("\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n"),
            "{metrics}"
        );
        assert!(metrics.ends_with("\r\n\r\nm 1\n"));
        let status = get(ops.local_addr(), "/status");
        assert!(status.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(status.contains("\r\nContent-Type: application/json\r\n"));
        assert!(status.ends_with("{\"process\":\"test\"}"));
        assert!(get(ops.local_addr(), "/other").starts_with("HTTP/1.1 404 "));
    }

    /// T-B2: a non-loopback address is refused, naming the key.
    #[test]
    fn non_loopback_addr_is_refused_naming_the_key() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
        let err = serve_loopback("TEST_X_METRICS_ADDR", addr, routes(Ok(""))).err();
        let err = err.expect("0.0.0.0 must be refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("TEST_X_METRICS_ADDR"), "{err}");
    }

    /// T-B3: dropping the handle closes the port; a hang in drop trips the 5 s watchdog.
    #[test]
    fn drop_closes_the_port() {
        let ops = serve_loopback("TEST_METRICS_ADDR", loopback0(), routes(Ok(""))).unwrap();
        let addr = ops.local_addr();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            drop(ops);
            let _ = tx.send(());
        });
        rx.recv_timeout(Duration::from_secs(5))
            .expect("dropping OpsListener must join its thread within 5 s");
        // dep: HTTP(loopback) — probes that the closed ops port refuses connections
        let refused = TcpStream::connect_timeout(&addr, Duration::from_secs(1));
        assert!(refused.is_err(), "port {addr} still accepts after drop");
    }

    /// T-B4: a renderer error is a 503 carrying its text, never a 200.
    #[test]
    fn renderer_error_is_503_with_text() {
        let ops =
            serve_loopback("TEST_METRICS_ADDR", loopback0(), routes(Err("stale 9s"))).unwrap();
        let resp = get(ops.local_addr(), "/metrics");
        assert!(resp.starts_with("HTTP/1.1 503 "), "{resp}");
        assert!(resp.ends_with("stale 9s"));
    }

    /// ADR-0061 review-fix 3 (F9): the shared `*_METRICS_ADDR` parser refuses port 0, a non-loopback address and
    /// garbage. Fault: drop `|| addr.port() == 0` ⇒ red.
    #[test]
    fn parse_ops_addr_refuses_port_zero_and_non_loopback() {
        assert_eq!(
            parse_ops_addr("127.0.0.1:19101"),
            Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19101))
        );
        assert!(parse_ops_addr("[::1]:19101").is_ok());
        for bad in [
            "127.0.0.1:0",
            "[::1]:0",
            "0.0.0.0:19101",
            "10.0.0.1:19101",
            "nonsense",
        ] {
            assert!(parse_ops_addr(bad).is_err(), "{bad} accepted");
        }
    }

    /// ADR-0061 review-fix 3 (F8): a client trickling one byte per second never completes its head; the listener
    /// cuts it at the total deadline and answers the next scrape within it. Fault: a per-read timeout only (each
    /// byte arrives inside 2 s) ⇒ the scrape waits for the trickler's ten bytes ⇒ red.
    #[test]
    fn a_trickling_client_cannot_hold_the_listener_past_the_deadline() {
        let ops = serve_loopback("TEST_METRICS_ADDR", loopback0(), routes(Ok("m 1\n"))).unwrap();
        let addr = ops.local_addr();
        let started = Instant::now();
        // dep: HTTP(loopback) — a client sending one byte per second, never finishing its head
        let mut slow = TcpStream::connect(addr).expect("connect");
        std::thread::spawn(move || {
            for byte in b"GET /metr" {
                if slow.write_all(&[*byte]).is_err() {
                    return;
                }
                std::thread::sleep(Duration::from_secs(1));
            }
        });
        std::thread::sleep(Duration::from_millis(200));
        let metrics = get(addr, "/metrics");
        let waited = started.elapsed();
        assert!(metrics.starts_with("HTTP/1.1 200 OK\r\n"), "{metrics}");
        assert!(
            waited < IO_TIMEOUT + Duration::from_secs(2),
            "the scrape waited {waited:?} behind a trickling client (deadline {IO_TIMEOUT:?})"
        );
    }

    fn render_one(out: &mut String) {
        write_family(out, &families::JOBS_DEAD, &[(&[], 0.0)]);
    }

    /// `process_routes`: `/metrics` is the render fn, `/status` is JSON carrying the identity, an escaped
    /// `git_sha` and all 11 degrade codes (drop the control-character arm ⇒ a raw newline ⇒ red).
    #[test]
    fn process_routes_serve_render_and_identity_status() {
        let routes = process_routes("p", "serve-rpc", "1.2.3", Some("a\"b\n"), render_one);
        assert_eq!(
            (routes.metrics)().unwrap(),
            "# HELP jobs_dead Queue jobs in DEAD.\n# TYPE jobs_dead gauge\njobs_dead 0\n"
        );
        let status = (routes.status)().unwrap();
        assert!(
            status.starts_with(
                "{\"process\":\"p\",\"mode\":\"serve-rpc\",\"crate_version\":\"1.2.3\",\"git_sha\":\"a\\\"b\\u000a\","
            ),
            "{status}"
        );
        assert!(!status.contains('\n'), "{status}");
        assert_eq!(status.matches("\"last_fired_at\":").count(), 11, "{status}");
        for code in DegradeCode::ALL {
            assert!(
                status.contains(&format!("\"{}\":{{\"count\":", code.as_str())),
                "{status}"
            );
        }
        assert!(status.ends_with("}}}"), "{status}");
        let none = process_routes("p", "serve", "1", None, |_| {});
        assert_eq!((none.metrics)().unwrap(), "");
        assert!((none.status)().unwrap().contains("\"git_sha\":null,"));
    }
}
