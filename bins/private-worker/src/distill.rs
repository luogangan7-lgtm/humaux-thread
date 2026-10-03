//! `private-worker::distill` — The Distill hop (§15.5 / §16.1.1 / §10.1, ADR-0016): accepted Evidence → 0..N
//!   `private.memory_records`, owned by this process because §6.2.2 already gives `role_private_worker` both the
//!   provider capability and the only INSERT on `memory_records`/`memory_evidence` (no RPC needed, contrast
//!   ADR-0015's Consolidate hop). Dispatch is ADR-0058's: one `DERIVED_DISTILL` job = one Evidence, claimed through
//!   the four provider slots, worked by at most IN_FLIGHT seats of one task.
//! Depends-on: crates=[hex, humaux-adapters, humaux-application, humaux-domain, humaux-projection, serde_json, sha2, tokio, uuid]; services=[]; env=[HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS]; modules=[adapters::affect_repo, adapters::byok, adapters::contribution_reasoner, adapters::distill_reasoner, adapters::distill_repo, adapters::jobs, adapters::postgres, application::consolidate, domain::authority, domain::dataclass, domain::error, domain::evidence, domain::ids, domain::memory, domain::policy, projection::fingerprint]
//! Called-by: [private-worker::main, tests]
//! Invariants: [a candidate over the §10.1 origin-bound ceiling is rejected, never downgraded; one job = one Evidence
//!   and a job only takes its own Evidence's outbox row; at most IN_FLIGHT seats per process and four bound slots
//!   across all processes; a lost lease never drops an in-flight call; the deadline cuts only the HTTP future; an
//!   attempt is an admitted provider request; DEAD only with its outbox row FAILED in the same transaction; a
//!   refused retry settles as its exhausted budget; memories, run completion, job settle and outbox flip commit in
//!   one generation-fenced transaction; only a fence refusal is a lost lease, an escaped error is ERROR (ADR-0058
//!   R3); an invalid inferred affect drops the reply's affects, never its memories (R1); a drain names why it stopped,
//!   no_slot or no_work (R5); a route whose credential reference is not in the worker's key map is NOT_READY
//!   CREDENTIAL_NOT_MAPPED before any ledger row or provider call (ADR-0059 D-I)]
//! Spec: Baseline §16.1.1; §10.1; ADR-0016; §15.7; §67.2; §11; ADR-0058; ADR-0059
//!
//! One job (ADR-0058 D-C): take the job's own outbox row, (a) check the Evidence still names the
//! job's reasoning domain, resolve the admitted Distill route, load the Evidence, record the
//! §16.1.1 processing run (fingerprint first — before any bytes leave), (b) reserve the ledger and
//! disclosure rows, ask `ops.begin_call` to admit the request (the attempt is counted there), send
//! it, (c) parse fail-closed, authorize each candidate against §10.1's origin-bound ceiling —
//! over-ceiling candidates are rejected with a reason and never downgraded — and commit the job
//! settle (first, generation-fenced), the surviving memories, the run's completion and the outbox
//! DONE flip in ONE transaction (idempotency, ADR-0016 D5 / ADR-0058 D-E).
//!
//! Settles (ADR-0058 D-D/D-F/D-H): anything before the first admitted request is NOT_READY (backs
//! off, parks as WAITING_KEY past the park age, never spends an attempt) — including a request the
//! tenant's §72.3 budget refused (`PROVIDER_BUDGET`, D-T); a DB/reasoning error after an admitted
//! request settles one fenced RETRY in a fresh transaction (D-E); a provider 401 parks as
//! WAITING_KEY and reverts its attempt (§11); any other failed call is RETRY with the capped
//! backoff, or DEAD with its class once `max_attempts` calls were spent — DEAD always flips the
//! outbox row FAILED in the same transaction, so the ticket ends as `distill_failed` and the
//! tenant's later rows keep flowing (§15.7). Input-bound rejections (Evidence gone) settle the
//! outbox FAILED and the job DONE.
//!
//! Tickets: remember already issued the `projection.stream_log` row for this Evidence; once the
//! outbox row is DONE the retrieval worker resolves it through `memory_evidence` (or settles it
//! as a no-op when the answer was "nothing memorable", `projection_worker` D6).

use std::cell::Cell;
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Duration;

use humaux_adapters::{
    affect_repo,
    byok::{ReasoningProviderError, UserReasoningProvider},
    contribution_reasoner::ContributionReasonerConfig,
    distill_reasoner::{
        AFFECT_INVALID_CLASS, DISPATCH_REFUSED, DISTILL_PARSER_VERSION, DISTILL_PROCESSOR_KIND,
        DISTILL_PROCESSOR_VERSION, DistillAdmission, DistillCallOutcome, DistillCandidate,
        DistillEnvelopeInput, DistillInferenceResult, DistillParseError, DistillReasoner,
        DistillReply, distill_prompt_contract, offers_affects, parse_distill_output_detailed,
    },
    distill_repo::{
        self, ClaimedEvidence, DbError, LoadedEvidence, NewMemory, OutboxSettle,
        ProcessingRunStart, TakenOutbox,
    },
    jobs::{self, CallAdmission, DistillClaim, DistillFinish, DistillLease},
    postgres::PrivateWorkerDbPool,
};
use humaux_application::consolidate::PrivateReasoningError;
use humaux_domain::{
    authority::{AuthorityPolicy, CandidateRejection, NonEmptyVec},
    dataclass::DataClass,
    error::ErrorCode,
    evidence::{EvidenceOriginClass, EvidencePayloadSha256, payload_sha256},
    ids::{Scope, TenantId, UserId, WorkspaceId},
    memory::MemoryType,
    policy::OriginBoundAuthorityPolicy,
};
use humaux_projection::fingerprint::{ProcessingInputFingerprintInputs, source_hash};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::time::Instant;
use uuid::Uuid;

/// Deployment-owned inputs of the cross-tenant dispatcher (ADR-0036 / ADR-0058 D-K). No tenant
/// id and no reasoning domain: both come from each claimed `DERIVED_DISTILL` job. Every value is
/// a required `HUMAUX_PRIVATE_WORKER_*` key (§78.1: no code default).
#[derive(Debug, Clone)]
pub struct DistillDispatchConfig {
    /// Per-process lease owner of every claim and of the outbox row the job takes.
    pub lease_owner: String,
    /// Job lease; the heartbeat renews it every third of it.
    pub lease_seconds: f64,
    /// Seats of this process (ADR-0058 D-J), `1..=`[`PROVIDER_SLOTS`]. The four slot rows are the
    /// global bound; this is the in-process second line.
    pub in_flight: u32,
    /// End of one claim; the lease never passes it and only it frees a dispatched slot (D-G).
    pub hard_deadline_seconds: f64,
    /// The provider transport timeout (`HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS`): `ops.begin_call`
    /// admits a request only with `http_timeout + lease` left before `hard_deadline`.
    pub http_timeout_seconds: f64,
    /// How long a job may be not ready before it parks as `WAITING_KEY`, and the park interval.
    pub not_ready_park_seconds: f64,
    /// Admitted provider requests after which a failing job is DEAD with its class.
    pub max_attempts: i32,
    /// §72.3 tenant distill budget every request passes before it may leave (ADR-0058 D-T).
    pub budget: jobs::DistillCallBudget,
    /// The credential references this process's key map holds (`HUMAUX_PRIVATE_WORKER_CREDENTIALS`,
    /// ADR-0059 D-I). An admitted route whose reference is not here is NOT_READY
    /// [`CREDENTIAL_NOT_MAPPED`] before anything is reserved. Empty = every route parks.
    pub credential_refs: BTreeSet<Uuid>,
}

/// ADR-0058 R2: the `ops.provider_slots` rows `migrations/0190_distill_dispatch_v2.sql` seeds
/// (gate `four_slots`). A seat past them can never hold a slot, and the pool is sized for them.
pub const PROVIDER_SLOTS: u32 = 4;

impl DistillDispatchConfig {
    /// ADR-0058 D-K: `hard_deadline >= 2 × (http_timeout + lease)` — the first call and the one
    /// ADR-0048 re-ask each need `http_timeout + lease` (`ops.begin_call`'s `min_remaining`).
    pub fn validate(&self) -> Result<(), ErrorCode> {
        let positive = |v: f64| v.is_finite() && v > 0.0;
        if self.lease_owner.trim().is_empty()
            || !positive(self.lease_seconds)
            || !(1..=PROVIDER_SLOTS).contains(&self.in_flight)
            || self.max_attempts <= 0
            || !positive(self.not_ready_park_seconds)
            || !positive(self.http_timeout_seconds)
            || !self.hard_deadline_seconds.is_finite()
            || self.hard_deadline_seconds < 2.0 * self.min_remaining_seconds()
            || !positive(self.budget.window_seconds)
            || self.budget.max_calls < 1
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(())
    }

    /// The window `ops.begin_call` must still see before `hard_deadline`: the HTTP call plus one
    /// lease for the post-call legs (ADR-0058 D-J).
    // ponytail: post-call legs slower than one lease let T6 fire before a known answer is written
    // (ADR-0058 L14); upgrade: a fenced renew allowed past the HTTP window for a completed call.
    fn min_remaining_seconds(&self) -> f64 {
        self.http_timeout_seconds + self.lease_seconds
    }
}

/// ADR-0058 R5: why a `--distill-once` seat stopped — its claim came back empty because every
/// provider slot was bound (READY work may be left; another process holds the slots), or because a
/// slot was free and no READY job could be taken. Ordered so the pass reports the worse one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DrainStop {
    /// A slot was free and the claim found no READY job it could take.
    NoWork,
    /// Every provider slot was bound when the empty claim was read back.
    NoSlot,
}

impl DrainStop {
    /// The `stopped=` value of the summary line.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoWork => "no_work",
            Self::NoSlot => "no_slot",
        }
    }
}

/// What one dispatcher run did, summed over its seats. `claimed == 0` is the "no input" signal
/// `--distill-once` exits on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DistillDispatchReport {
    /// `DERIVED_DISTILL` jobs claimed.
    pub claimed: u32,
    /// Jobs settled DONE with their outbox row DONE (memories written, or nothing memorable, or
    /// the Evidence was already settled).
    pub completed: u32,
    /// Jobs whose outbox row was settled FAILED (input-bound rejection, or DEAD).
    pub failed: u32,
    /// NOT_READY settles: nothing was dispatched (no usable binding, admission mismatch, domain
    /// mismatch, a refused first call). Backed off, never an attempt.
    pub not_ready: u32,
    /// Jobs settled `WAITING_KEY` (not ready past the park age, or a provider 401).
    pub parked: u32,
    /// RETRY settles after a counted failed call.
    pub deferred: u32,
    /// DEAD settles (attempts exhausted, abandoned claims, fail-closed refusals).
    pub dead: u32,
    /// Jobs whose take or settle was rejected by the generation fence (another claim owns them).
    /// Fence losses only (ADR-0058 R3).
    pub lost_lease: u32,
    /// Jobs an error escaped (a DB, reasoner or membership failure the job could not settle);
    /// the job line names its class (ADR-0058 R3).
    pub errors: u32,
    /// Jobs whose heartbeat found the lease gone while their work was still running.
    pub heartbeat_lost: u32,
    /// Provider requests admitted by `ops.begin_call` (each one an attempt).
    pub attempts: u32,
    /// Calls the HTTP cutoff ended with no answer (outcome unknown; reconciled by T5 → T6).
    pub unknown: u32,
    /// Memories written.
    pub memories: u32,
    /// §10.1 `memory_candidate_rejections_total` count (every reason).
    pub rejected: u32,
    /// ADR-0048: re-asks after a clean parse with zero candidates.
    pub empty_retries: u32,
    /// ADR-0048 addendum: re-asks after a reply the parser refused.
    pub malformed_retries: u32,
    /// ADR-0058 R1: written replies whose inferred affects were discarded as invalid.
    pub affects_dropped: u32,
    /// ADR-0058 R9: written replies the tool channel delivered in `content` (no tool call) and the
    /// parser accepted.
    pub channel_fallback: u32,
    /// ADR-0058 R5: why the drain stopped (`None` for `--distill-serve`, which never stops on an
    /// empty claim). Summed as the worse of the seats' reasons.
    pub stopped: Option<DrainStop>,
}

impl DistillDispatchReport {
    fn add(&mut self, other: DistillDispatchReport) {
        self.claimed += other.claimed;
        self.completed += other.completed;
        self.failed += other.failed;
        self.not_ready += other.not_ready;
        self.parked += other.parked;
        self.deferred += other.deferred;
        self.dead += other.dead;
        self.lost_lease += other.lost_lease;
        self.errors += other.errors;
        self.heartbeat_lost += other.heartbeat_lost;
        self.attempts += other.attempts;
        self.unknown += other.unknown;
        self.memories += other.memories;
        self.rejected += other.rejected;
        self.empty_retries += other.empty_retries;
        self.malformed_retries += other.malformed_retries;
        self.affects_dropped += other.affects_dropped;
        self.channel_fallback += other.channel_fallback;
        self.stopped = self.stopped.max(other.stopped);
    }

    /// The operator-facing summary line (`claimed=` = jobs claimed this run).
    pub fn summary_line(&self) -> String {
        format!(
            "humaux-private-worker: distill dispatch claimed={} completed={} failed={} not_ready={} parked={} deferred={} dead={} lost_lease={} errors={} heartbeat_lost={} attempts={} unknown={} memories={} rejected={} empty_retries={} malformed_retries={} affects_dropped={} channel_fallback={} stopped={}",
            self.claimed,
            self.completed,
            self.failed,
            self.not_ready,
            self.parked,
            self.deferred,
            self.dead,
            self.lost_lease,
            self.errors,
            self.heartbeat_lost,
            self.attempts,
            self.unknown,
            self.memories,
            self.rejected,
            self.empty_retries,
            self.malformed_retries,
            self.affects_dropped,
            self.channel_fallback,
            self.stopped.map_or("-", DrainStop::as_str),
        )
    }
}

/// Why a dispatcher run stopped early.
#[derive(Debug)]
pub enum DistillError {
    Db(DbError),
    Reasoning(PrivateReasoningError),
    Config(ErrorCode),
}

impl DistillError {
    /// Static class for `last_error_class` and the job line — never DB or provider text.
    fn class(&self) -> &'static str {
        match self {
            Self::Db(_) => "WORKER_DB_ERROR",
            Self::Reasoning(error) => error.class().unwrap_or("UNCLASSIFIED_REASONING_ERROR"),
            Self::Config(_) => "WORKER_CONFIG_ERROR",
        }
    }
}

impl std::fmt::Display for DistillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(error) => write!(f, "distill database error: {error}"),
            Self::Reasoning(error) => write!(f, "distill reasoning error: {error}"),
            Self::Config(code) => write!(f, "distill configuration rejected: {code:?}"),
        }
    }
}

impl std::error::Error for DistillError {}

impl From<DbError> for DistillError {
    fn from(value: DbError) -> Self {
        Self::Db(value)
    }
}

impl From<PrivateReasoningError> for DistillError {
    fn from(value: PrivateReasoningError) -> Self {
        Self::Reasoning(value)
    }
}

impl From<jobs::JobsError> for DistillError {
    fn from(value: jobs::JobsError) -> Self {
        match value {
            jobs::JobsError::Db(error) => Self::Db(error),
            other => Self::Reasoning(PrivateReasoningError::new(other.to_string())),
        }
    }
}

/// §10.1 closed reason set → the `memory_candidate_rejections_total{reason}` label values
/// (`Baseline_2.9.md` metrics registry: origin_authority_ceiling | untrusted_instruction |
/// cross_tenant_evidence | missing_confirmation).
fn rejection_reason(rejection: CandidateRejection) -> &'static str {
    // One mapping (§78.2): the domain enum owns the label; this is the metric-value view of the
    // same string `private.distill_candidates.rejection_reason` stores.
    rejection.as_db_str()
}

/// §78.1 candidate TTL, from `HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS` (no literal default).
/// Consulted only when a rejection is actually persisted. Missing/invalid = config error, which
/// rolls the write back and retries the job (fail-closed) rather than writing a candidate with an
/// ad-hoc deadline.
fn candidate_ttl_seconds() -> Result<i64, DistillError> {
    let raw = std::env::var("HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS")
        .map_err(|_| DistillError::Config(ErrorCode::InvalidInput))?;
    let secs: i64 = raw
        .trim()
        .parse()
        .map_err(|_| DistillError::Config(ErrorCode::InvalidInput))?;
    if secs <= 0 {
        return Err(DistillError::Config(ErrorCode::InvalidInput));
    }
    Ok(secs)
}

/// ADR-0016 D2: the acting identity the §11.1 context carries — the Evidence's own principal
/// when its origin is a user-shaped one, else the reasoning domain owner.
fn acting_user(evidence: &LoadedEvidence, owner_user_id: Uuid) -> Uuid {
    match evidence.origin_class {
        EvidenceOriginClass::DirectUserInput
        | EvidenceOriginClass::UserConfirmed
        | EvidenceOriginClass::TenantAdmin => evidence.origin_principal_id.unwrap_or(owner_user_id),
        _ => owner_user_id,
    }
}

/// `memory_records.content` shape this hop writes — the fields `projection_worker::card_input`
/// reads (`title` + `key_claim`; `build_card` needs at least one of `key_claim`/
/// `evidence_excerpt`) and `consolidation_reasoner` forwards as opaque JSON.
fn memory_content(text: &str) -> Value {
    serde_json::json!({
        "title": text.chars().take(80).collect::<String>(),
        "key_claim": text,
    })
}

/// The `(reasoning_domain_id, evidence_id)` `migrations/0164_derived_work_dispatch.sql`'s enqueue
/// trigger (and 0193's backfill) write into the job payload — the job's identity (ADR-0058 D-C).
fn payload_identity(payload: &Value) -> Option<(Uuid, Uuid)> {
    let field = |name: &str| payload.get(name)?.as_str()?.parse::<Uuid>().ok();
    Some((field("reasoning_domain_id")?, field("evidence_id")?))
}

/// The static NOT_READY class for a tenant whose `(domain, PRIVATE_DISTILL_TEXT)` binding has not
/// been admitted yet — the "onboarded before its route" case.
const NO_DISTILL_BINDING: &str = "no admitted PRIVATE_DISTILL_TEXT route binding";
/// ADR-0059 D-I: the admitted route's credential reference is not in this process's key map.
pub const CREDENTIAL_NOT_MAPPED: &str = "CREDENTIAL_NOT_MAPPED";
/// ADR-0058 D-D: the Evidence names another reasoning domain than its job.
const DOMAIN_MISMATCH: &str = "DOMAIN_MISMATCH";
/// A job payload without the 0164 identity: no Evidence can ever be named for it.
const INVALID_JOB_PAYLOAD: &str = "INVALID_JOB_PAYLOAD";
/// ADR-0058 D-F: claimed with `attempt >= max_attempts` (after a T6 reconcile).
const ATTEMPTS_EXHAUSTED: &str = "ATTEMPTS_EXHAUSTED";
/// ADR-0058 D-F: claimed with `abandoned_claims >= max_attempts` (pre-dispatch crashes).
const PRE_DISPATCH_ABANDONED: &str = "PRE_DISPATCH_ABANDONED";
/// ADR-0048 D-D: the reply failed the parser (or the tool-call shape, ADR-0058 D-M) after its
/// re-ask budget.
const FAILED_OUTPUT_SCHEMA: &str = ReasoningProviderError::FAILED_OUTPUT_SCHEMA_CLASS;
/// The ADR-0058 T6 class the claim's sweep writes; seen again on the re-claim.
const EXECUTION_UNCERTAIN: &str = "EXECUTION_UNCERTAIN";
/// ADR-0058 D-T: the first request of a claim was refused by the tenant's §72.3 budget (NOT_READY,
/// no attempt spent).
const PROVIDER_BUDGET: &str = "PROVIDER_BUDGET";

// ADR-0048: one flat re-ask each per claim; a refused or exhausted one settles as its budget does.
const DISTILL_EMPTY_RETRY_BUDGET: u32 = 1;
const DISTILL_MALFORMED_RETRY_BUDGET: u32 = 1;

/// How one claimed job ended, as the job line names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobOutcome {
    /// Job DONE, outbox DONE.
    Done,
    /// Job DONE, outbox FAILED (input-bound).
    Failed,
    Retry,
    NotReady,
    Parked,
    Dead,
    /// The generation fence refused the take or the settle (ADR-0058 R3: nothing else).
    LeaseLost,
    /// An error escaped the job; the claim reconciles through the sweep (ADR-0058 R3).
    Error,
    /// The HTTP cutoff won: no settle, the claim reconciles through T5 → T6.
    Unknown,
}

impl JobOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Done => "DONE",
            Self::Failed => "FAILED",
            Self::Retry => "RETRY",
            Self::NotReady => "NOT_READY",
            Self::Parked => "PARKED",
            Self::Dead => "DEAD",
            Self::LeaseLost => "LEASE_LOST",
            Self::Error => "ERROR",
            Self::Unknown => "UNKNOWN",
        }
    }
}

/// One settled job, for the job line.
struct Settled {
    outcome: JobOutcome,
    class: Option<&'static str>,
    next_retry_seconds: Option<f64>,
    /// ADR-0058 R9: the written reply came from the tool channel's `content`.
    channel_fallback: bool,
}

impl Settled {
    fn new(outcome: JobOutcome, class: Option<&'static str>) -> Self {
        Self {
            outcome,
            class,
            next_retry_seconds: None,
            channel_fallback: false,
        }
    }
}

/// Everything the seats share: one pool, one reasoner, one config.
struct Dispatcher<'a> {
    pool: &'a PrivateWorkerDbPool,
    reasoner: DistillReasoner<'a>,
    dispatch: &'a DistillDispatchConfig,
}

/// How the seats run: `Drain` = each seat stops at its first empty claim (`--distill-once`);
/// `Serve` = each seat sleeps `poll` after an empty claim and stops at shutdown, after its job.
#[derive(Clone, Copy)]
enum Mode<'a> {
    Drain,
    Serve {
        poll: Duration,
        shutdown: &'a AtomicBool,
    },
}

/// `--distill-once` (and every test): IN_FLIGHT seats work the cross-tenant backlog until each
/// seat's claim comes back empty. Returns the summed report; a seat's claim error is returned
/// after every other seat has finished its own job.
pub async fn dispatch_pass(
    pool: &PrivateWorkerDbPool,
    provider: &dyn UserReasoningProvider,
    config: ContributionReasonerConfig,
    dispatch: &DistillDispatchConfig,
) -> Result<DistillDispatchReport, DistillError> {
    run_seats(pool, provider, config, dispatch, Mode::Drain).await
}

/// `--distill-serve`: the same seats, resident. A seat sleeps `poll` only after its own claim
/// came back empty ("skip the poll sleep while work exists"); `shutdown` stops every seat after
/// the job it holds.
pub async fn dispatch_serve(
    pool: &PrivateWorkerDbPool,
    provider: &dyn UserReasoningProvider,
    config: ContributionReasonerConfig,
    dispatch: &DistillDispatchConfig,
    poll: Duration,
    shutdown: &AtomicBool,
) -> Result<DistillDispatchReport, DistillError> {
    run_seats(
        pool,
        provider,
        config,
        dispatch,
        Mode::Serve { poll, shutdown },
    )
    .await
}

async fn run_seats(
    pool: &PrivateWorkerDbPool,
    provider: &dyn UserReasoningProvider,
    config: ContributionReasonerConfig,
    dispatch: &DistillDispatchConfig,
    mode: Mode<'_>,
) -> Result<DistillDispatchReport, DistillError> {
    dispatch.validate().map_err(DistillError::Config)?;
    // ponytail: the sqlx default pool (10) covers two connections per seat plus a claim for the
    // `PROVIDER_SLOTS` seats `validate` allows (ADR-0058 L7 / R2); upgrade: size the pool from
    // IN_FLIGHT in adapters::postgres.
    let dispatcher = Dispatcher {
        pool,
        reasoner: DistillReasoner::new(pool, provider, config).map_err(DistillError::Config)?,
        dispatch,
    };
    let seats: Vec<SeatFuture<'_>> = (0..dispatch.in_flight)
        .map(|_| Box::pin(seat(&dispatcher, mode)) as SeatFuture<'_>)
        .collect();
    let mut report = DistillDispatchReport::default();
    let mut first_error = None;
    for result in join_all(seats).await {
        match result {
            Ok(seat_report) => report.add(seat_report),
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(report),
    }
}

/// One seat's future, as [`join_all`] polls it.
type SeatFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DistillDispatchReport, DistillError>> + 'a>>;

/// Polls every seat in this one task until all are done.
// ponytail: one task polls all seats — no CPU parallelism, and a panic in one job ends the
// dispatcher (ADR-0058 L6). Upgrade: a JoinSet of spawned seats once the provider is an `Arc`.
async fn join_all<T>(mut futures: Vec<Pin<Box<dyn Future<Output = T> + '_>>>) -> Vec<T> {
    let mut results: Vec<Option<T>> = futures.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (future, result) in futures.iter_mut().zip(results.iter_mut()) {
            if result.is_none() {
                match future.as_mut().poll(cx) {
                    Poll::Ready(value) => *result = Some(value),
                    Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    results.into_iter().flatten().collect()
}

/// One seat: claim one job, run it, claim again at once.
async fn seat(
    dispatcher: &Dispatcher<'_>,
    mode: Mode<'_>,
) -> Result<DistillDispatchReport, DistillError> {
    let dispatch = dispatcher.dispatch;
    let mut report = DistillDispatchReport::default();
    loop {
        if let Mode::Serve { shutdown, .. } = mode
            && shutdown.load(Ordering::SeqCst)
        {
            return Ok(report);
        }
        // ADR-0058 D-J: taken before the claim call, so the local deadline is never later than
        // the claim's `hard_deadline` (no cross-clock comparison).
        let local_deadline =
            Instant::now() + Duration::from_secs_f64(dispatch.hard_deadline_seconds);
        let claimed = jobs::claim_distill(
            dispatcher.pool,
            &dispatch.lease_owner,
            dispatch.lease_seconds,
            dispatch.hard_deadline_seconds,
        )
        .await;
        match (claimed, mode) {
            (Ok(Some(claim)), _) => {
                report.claimed += 1;
                run_claimed(dispatcher, &claim, local_deadline, &mut report).await;
            }
            (Ok(None), Mode::Drain) => {
                // ADR-0058 R5: read right after the empty claim, so the reason is that claim's.
                let all_bound = jobs::distill_slots_all_bound(dispatcher.pool).await?;
                report.stopped = Some(if all_bound {
                    DrainStop::NoSlot
                } else {
                    DrainStop::NoWork
                });
                return Ok(report);
            }
            (Err(error), Mode::Drain) => return Err(error.into()),
            (Ok(None), Mode::Serve { poll, shutdown }) => idle(poll, shutdown).await,
            (Err(error), Mode::Serve { poll, shutdown }) => {
                eprintln!("humaux-private-worker: distill claim failed: {error}");
                idle(poll, shutdown).await;
            }
        }
    }
}

/// Sleeps `poll`, waking early on shutdown.
// ponytail: checks the flag every 100 ms (no tokio `sync` feature here, ADR-0058 D-J); a Notify
// replaces the tick if the feature is ever enabled.
async fn idle(poll: Duration, shutdown: &AtomicBool) {
    const SHUTDOWN_TICK: Duration = Duration::from_millis(100);
    let until = Instant::now() + poll;
    while !shutdown.load(Ordering::SeqCst) {
        let now = Instant::now();
        if now >= until {
            return;
        }
        tokio::time::sleep((until - now).min(SHUTDOWN_TICK)).await;
    }
}

/// One claimed job: its work and its own heartbeat, polled together (ADR-0058 D-J), then the job
/// line. A lost lease does not drop the work: it runs to its end (bounded by the HTTP cutoff) so
/// the call is still ledgered, and its settle is refused by the generation fence.
async fn run_claimed(
    dispatcher: &Dispatcher<'_>,
    claim: &DistillClaim,
    local_deadline: Instant,
    report: &mut DistillDispatchReport,
) {
    let dispatch = dispatcher.dispatch;
    let lease = DistillLease::of(claim, &dispatch.lease_owner);
    if claim.last_error_class.as_deref() == Some(EXECUTION_UNCERTAIN) {
        // Main-line ruling E1: one line per T6 re-queue, naming the call whose outcome stayed
        // unknown — the resend is never silent.
        eprintln!(
            "humaux-private-worker: distill job={} tenant={} gen={} re-claimed after EXECUTION_UNCERTAIN (T6) uncertain_model_call_id={} attempt={}/{}",
            claim.job_id,
            claim.tenant_id,
            claim.claim_generation,
            claim
                .dispatch_model_call_id
                .map_or_else(|| "-".to_owned(), |id| id.to_string()),
            claim.attempt,
            dispatch.max_attempts,
        );
    }
    let lost = Cell::new(false);
    let mut attempt = claim.attempt;
    let heartbeat = async {
        let period = Duration::from_secs_f64(dispatch.lease_seconds / 3.0);
        loop {
            tokio::time::sleep(period).await;
            match jobs::renew_distill_lease(dispatcher.pool, &lease, dispatch.lease_seconds).await {
                Ok(Some(_)) => {}
                Ok(None) => {
                    lost.set(true);
                    std::future::pending::<()>().await;
                }
                // Transient: the next tick retries; the lease outlives two missed ticks.
                Err(error) => {
                    eprintln!(
                        "humaux-private-worker: distill job={} heartbeat failed: {error}",
                        claim.job_id
                    );
                }
            }
        }
    };
    let work = work_job(
        dispatcher,
        claim,
        &lease,
        local_deadline,
        &mut attempt,
        report,
    );
    let settled = tokio::select! {
        biased;
        settled = work => settled,
        () = heartbeat => unreachable!("the heartbeat never completes"),
    };
    let settled = match settled {
        Ok(settled) => settled,
        Err(error) => {
            eprintln!(
                "humaux-private-worker: distill job={} failed: {error}",
                claim.job_id
            );
            Settled::new(JobOutcome::Error, Some(error.class()))
        }
    };
    match settled.outcome {
        JobOutcome::Done => report.completed += 1,
        JobOutcome::Failed => report.failed += 1,
        JobOutcome::Retry => report.deferred += 1,
        JobOutcome::NotReady => report.not_ready += 1,
        JobOutcome::Parked => report.parked += 1,
        JobOutcome::Dead => {
            report.dead += 1;
            report.failed += 1;
        }
        JobOutcome::LeaseLost => report.lost_lease += 1,
        JobOutcome::Error => report.errors += 1,
        JobOutcome::Unknown => report.unknown += 1,
    }
    // A renew that lost the race with this job's own committed settle is not a lost lease.
    if lost.get() && matches!(settled.outcome, JobOutcome::LeaseLost | JobOutcome::Unknown) {
        report.heartbeat_lost += 1;
    }
    eprintln!(
        "humaux-private-worker: distill job={} tenant={} evidence={} gen={} outcome={} attempt={}/{} error_class={} next_retry_s={} channel_fallback={}",
        claim.job_id,
        claim.tenant_id,
        payload_identity(&claim.payload).map_or_else(|| "-".to_owned(), |(_, e)| e.to_string()),
        claim.claim_generation,
        settled.outcome.as_str(),
        attempt,
        dispatch.max_attempts,
        settled.class.unwrap_or("-"),
        settled
            .next_retry_seconds
            .map_or_else(|| "-".to_owned(), |s| format!("{s:.0}")),
        u8::from(settled.channel_fallback),
    );
}

/// Settles the job (first, generation-fenced) and its taken outbox row in one transaction.
/// `None` outbox = the job never took a row. Returns `false` when the fence refused.
async fn settle(
    dispatcher: &Dispatcher<'_>,
    lease: &DistillLease<'_>,
    outbox: Option<(&ClaimedEvidence, OutboxSettle)>,
    finish: DistillFinish,
    class: Option<&'static str>,
    backoff_seconds: f64,
) -> Result<bool, DistillError> {
    let mut txn =
        distill_repo::begin_write_context(dispatcher.pool, lease.tenant_id, Uuid::nil()).await?;
    if !distill_repo::finish_job_in_txn(
        &mut txn,
        lease,
        finish,
        class,
        backoff_seconds,
        dispatcher.dispatch.not_ready_park_seconds,
    )
    .await?
    {
        txn.rollback().await?;
        return Ok(false);
    }
    if let Some((row, how)) = outbox
        && !distill_repo::settle_outbox_in_txn(&mut txn, row.outbox_id, lease.lease_owner, how)
            .await?
    {
        txn.rollback().await?;
        return Ok(false);
    }
    txn.commit().await?;
    Ok(true)
}

/// A settle that maps "fence refused" to `LeaseLost`.
async fn settle_as(
    dispatcher: &Dispatcher<'_>,
    lease: &DistillLease<'_>,
    outbox: Option<(&ClaimedEvidence, OutboxSettle)>,
    finish: DistillFinish,
    class: Option<&'static str>,
    backoff_seconds: f64,
    outcome: JobOutcome,
) -> Result<Settled, DistillError> {
    if settle(dispatcher, lease, outbox, finish, class, backoff_seconds).await? {
        let next_retry_seconds = match finish {
            DistillFinish::Retry | DistillFinish::NotReady => Some(backoff_seconds),
            DistillFinish::WaitingKey => Some(dispatcher.dispatch.not_ready_park_seconds),
            DistillFinish::Done | DistillFinish::Dead => None,
        };
        Ok(Settled {
            outcome,
            class,
            next_retry_seconds,
            channel_fallback: false,
        })
    } else {
        Ok(Settled::new(JobOutcome::LeaseLost, class))
    }
}

/// NOT_READY (ADR-0058 D-H) with the outbox row handed back; reported as PARKED when the claim
/// row says the job has been not ready for the park age already (DB clock on both sides).
async fn settle_not_ready(
    dispatcher: &Dispatcher<'_>,
    claim: &DistillClaim,
    lease: &DistillLease<'_>,
    row: &ClaimedEvidence,
    class: &'static str,
) -> Result<Settled, DistillError> {
    let dispatch = dispatcher.dispatch;
    let claimed_at = claim.lease_expires_at - Duration::from_secs_f64(dispatch.lease_seconds);
    let parks = claim.not_ready_since.is_some_and(|since| {
        (claimed_at - since).as_seconds_f64() >= dispatch.not_ready_park_seconds
    });
    settle_as(
        dispatcher,
        lease,
        Some((row, OutboxSettle::Pending)),
        DistillFinish::NotReady,
        Some(class),
        jobs::retry_backoff_seconds(dispatch.lease_seconds, claim.attempt),
        if parks {
            JobOutcome::Parked
        } else {
            JobOutcome::NotReady
        },
    )
    .await
}

/// A counted call failed (or a post-call leg did): RETRY with the capped backoff, or DEAD with
/// the class once `max_attempts` admitted calls were spent (ADR-0058 D-F).
async fn settle_failed_call(
    dispatcher: &Dispatcher<'_>,
    lease: &DistillLease<'_>,
    row: &ClaimedEvidence,
    attempt: i32,
    class: &'static str,
) -> Result<Settled, DistillError> {
    let dispatch = dispatcher.dispatch;
    if attempt >= dispatch.max_attempts {
        return settle_as(
            dispatcher,
            lease,
            Some((row, OutboxSettle::Failed)),
            DistillFinish::Dead,
            Some(class),
            0.0,
            JobOutcome::Dead,
        )
        .await;
    }
    settle_as(
        dispatcher,
        lease,
        Some((row, OutboxSettle::Pending)),
        DistillFinish::Retry,
        Some(class),
        jobs::retry_backoff_seconds(dispatch.lease_seconds, attempt),
        JobOutcome::Retry,
    )
    .await
}

/// The read leg of one call: everything up to the committed processing run.
struct Ready {
    admission: DistillAdmission,
    evidence: LoadedEvidence,
    processing_run_id: Uuid,
    /// The contract this run was fingerprinted with offers the affect menu (ADR-0058 D-P).
    offer_affects: bool,
}

/// What the read leg found.
enum ReadLeg {
    Ready(Box<Ready>),
    /// The Evidence is gone (or not an EVENT): input-bound.
    Unavailable,
    /// Nothing can be dispatched now; the static class says why.
    NotReady(&'static str),
}

/// (a) of the module doc: domain check, binding, admission, credential check, Evidence load,
/// processing run (committed before the call).
#[allow(
    clippy::too_many_lines,
    reason = "one read transaction whose NOT_READY gates (domain, binding, admission, credential map) must all precede the reservation; ADR-0058 D-H, ADR-0059 D-I"
)]
async fn read_leg(
    dispatcher: &Dispatcher<'_>,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    evidence_id: Uuid,
) -> Result<ReadLeg, DistillError> {
    let mut txn = distill_repo::begin_read_context(dispatcher.pool, tenant_id).await?;
    // ADR-0058 D-D: compare BEFORE any binding of the payload's domain is resolved.
    match distill_repo::evidence_domain(&mut txn, tenant_id, evidence_id).await? {
        None => {
            txn.rollback().await?;
            return Ok(ReadLeg::Unavailable);
        }
        Some(domain) if domain != reasoning_domain_id => {
            txn.rollback().await?;
            return Ok(ReadLeg::NotReady(DOMAIN_MISMATCH));
        }
        Some(_) => {}
    }
    let Some((binding_id, binding_version)) =
        distill_repo::resolve_distill_binding(&mut txn, reasoning_domain_id).await?
    else {
        txn.rollback().await?;
        return Ok(ReadLeg::NotReady(NO_DISTILL_BINDING));
    };
    let admission = match dispatcher
        .reasoner
        .admit(
            &mut txn,
            tenant_id,
            reasoning_domain_id,
            binding_id,
            binding_version,
        )
        .await
    {
        Ok(admission) => admission,
        Err(error) => {
            txn.rollback().await?;
            return Ok(ReadLeg::NotReady(
                error
                    .class()
                    .unwrap_or("unclassified private reasoning failure"),
            ));
        }
    };
    // ADR-0058 D-H / ADR-0059 D-I: pre-reserve — a reference the key map does not hold is NOT_READY
    // here, before any ledger row, `ops.begin_call` or provider call exists.
    if !dispatcher
        .dispatch
        .credential_refs
        .contains(&admission.locator.credential_ref)
    {
        txn.rollback().await?;
        return Ok(ReadLeg::NotReady(CREDENTIAL_NOT_MAPPED));
    }
    let Some(evidence) = distill_repo::load_evidence(
        &mut txn,
        tenant_id,
        reasoning_domain_id,
        admission.owner_user_id,
        evidence_id,
    )
    .await?
    else {
        txn.rollback().await?;
        return Ok(ReadLeg::Unavailable);
    };
    let context_snapshot_seq = distill_repo::context_snapshot_seq(&mut txn, tenant_id).await?;

    // ADR-0048: ONE ceiling, resolved once here, feeds both the rendered contract (the class
    // menu the model reads) and the envelope's `max_class`; ADR-0058 D-M: the contract is the one
    // `prepare` sends, for the channel the provider declares.
    let ceiling = evidence.origin_class.authority_ceiling(MemoryType::Fact);
    let affects = affect_menu(&mut txn, tenant_id, &evidence).await?;
    let contract = distill_prompt_contract(ceiling, affects, dispatcher.reasoner.output_channel());
    let evidence_hashes = [evidence_axis(&evidence)?];
    let prompt_hash = hex::encode(contract.sha256.0);
    let prompt_version = contract.version.to_string();
    let fingerprint = source_hash(&ProcessingInputFingerprintInputs {
        evidence_payload_sha256: &evidence_hashes,
        processor_kind: DISTILL_PROCESSOR_KIND,
        processor_version: DISTILL_PROCESSOR_VERSION,
        model_provider: &admission.locator.processor_id,
        model_id: &admission.locator.provider_model_id,
        model_revision: admission.locator.model_revision.as_deref().unwrap_or(""),
        prompt_version: &prompt_version,
        prompt_hash: &prompt_hash,
        embedding_version: None,
        parser_version: DISTILL_PARSER_VERSION,
        card_builder_version: None,
        context_snapshot_seq: u64::try_from(context_snapshot_seq).unwrap_or_default(),
    });
    let processing_run_id = distill_repo::start_processing_run(
        &mut txn,
        tenant_id,
        &ProcessingRunStart {
            evidence_id: evidence.evidence_id,
            processor_kind: DISTILL_PROCESSOR_KIND,
            processor_version: DISTILL_PROCESSOR_VERSION,
            model_provider: &admission.locator.processor_id,
            model_id: &admission.locator.provider_model_id,
            model_revision: admission.locator.model_revision.as_deref().unwrap_or(""),
            prompt_version: &prompt_version,
            prompt_hash: &prompt_hash,
            parser_version: DISTILL_PARSER_VERSION,
            evidence_payload_sha256: vec![evidence.payload_sha256.clone()],
            source_hash: fingerprint.as_bytes(),
            context_snapshot_seq,
        },
    )
    .await?;
    txn.commit().await?;
    Ok(ReadLeg::Ready(Box::new(Ready {
        admission,
        evidence,
        processing_run_id,
        offer_affects: affects,
    })))
}

/// ADR-0058 D-P: the affect menu is offered for a user-origin Evidence that declared no affect
/// itself (a declared one reaches the memory as EXPLICIT through the 0157 trigger).
async fn affect_menu(
    txn: &mut distill_repo::DbTransaction<'_>,
    tenant_id: Uuid,
    evidence: &LoadedEvidence,
) -> Result<bool, DbError> {
    Ok(offers_affects(evidence.origin_class)
        && !distill_repo::evidence_has_declared_affects(txn, tenant_id, evidence.evidence_id)
            .await?)
}

/// §16.1/§16.1.1 evidence axis — card 21 / ADR-0043: the **persisted**
/// `private.evidence_objects.payload_sha256` read back (a read-back, not a second hasher).
fn evidence_axis(evidence: &LoadedEvidence) -> Result<EvidencePayloadSha256, DistillError> {
    EvidencePayloadSha256::from_stored_digest(&evidence.payload_sha256).ok_or_else(|| {
        DistillError::Reasoning(PrivateReasoningError::classified(
            "evidence_objects.payload_sha256 is not a 32-byte digest",
        ))
    })
}

/// The last answered call whose (empty) result the job may still settle with.
struct EmptyAnswer {
    inferred: DistillInferenceResult,
    processing_run_id: Uuid,
    evidence: LoadedEvidence,
}

/// One claimed job end to end; every exit settles (or, for an unknown call, deliberately does
/// not). `attempt` tracks the job's counted calls for the job line.
async fn work_job(
    dispatcher: &Dispatcher<'_>,
    claim: &DistillClaim,
    lease: &DistillLease<'_>,
    local_deadline: Instant,
    attempt: &mut i32,
    report: &mut DistillDispatchReport,
) -> Result<Settled, DistillError> {
    let dispatch = dispatcher.dispatch;
    let Some((reasoning_domain_id, evidence_id)) = payload_identity(&claim.payload) else {
        return settle_as(
            dispatcher,
            lease,
            None,
            DistillFinish::Dead,
            Some(INVALID_JOB_PAYLOAD),
            0.0,
            JobOutcome::Dead,
        )
        .await;
    };
    let row = match distill_repo::take_outbox_row(
        dispatcher.pool,
        lease,
        evidence_id,
        claim.hard_deadline,
    )
    .await?
    {
        TakenOutbox::LeaseLost => return Ok(Settled::new(JobOutcome::LeaseLost, None)),
        TakenOutbox::AlreadySettled => {
            return settle_as(
                dispatcher,
                lease,
                None,
                DistillFinish::Done,
                None,
                0.0,
                JobOutcome::Done,
            )
            .await;
        }
        TakenOutbox::Taken(row) => row,
    };
    // ADR-0058 D-F: exhausted on claim — DEAD at once, no HTTP, outbox FAILED in the same txn.
    for (spent, class) in [
        (claim.attempt, ATTEMPTS_EXHAUSTED),
        (claim.abandoned_claims, PRE_DISPATCH_ABANDONED),
    ] {
        if spent >= dispatch.max_attempts {
            return settle_as(
                dispatcher,
                lease,
                Some((&row, OutboxSettle::Failed)),
                DistillFinish::Dead,
                Some(class),
                0.0,
                JobOutcome::Dead,
            )
            .await;
        }
    }

    let mut calls = 0_u32;
    // ADR-0058 D-E: every error exit after the take settles here — one fenced settle in a fresh
    // transaction (NOT_READY before any admitted call, a failed counted call after one), so a job
    // whose outcome the worker already knows neither keeps its slot until hard_deadline nor comes
    // back as EXECUTION_UNCERTAIN.
    match call_loop(
        dispatcher,
        claim,
        lease,
        &row,
        reasoning_domain_id,
        local_deadline,
        attempt,
        &mut calls,
        report,
    )
    .await
    {
        Err(error) => {
            settle_after_error(dispatcher, claim, lease, &row, calls, *attempt, &error).await
        }
        settled => settled,
    }
}

/// The read → call → parse → write loop of one taken job (re-asks included). `calls` counts the
/// admitted requests so far; an `Err` is settled by [`work_job`].
#[allow(
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "one claim's call loop (read → prepare → admit → call → parse → write, with the ADR-0048 re-asks) reads best in one place; ADR-0058 D-F"
)]
async fn call_loop(
    dispatcher: &Dispatcher<'_>,
    claim: &DistillClaim,
    lease: &DistillLease<'_>,
    row: &ClaimedEvidence,
    reasoning_domain_id: Uuid,
    local_deadline: Instant,
    attempt: &mut i32,
    calls: &mut u32,
    report: &mut DistillDispatchReport,
) -> Result<Settled, DistillError> {
    let dispatch = dispatcher.dispatch;
    // The Evidence of the row this claim took (equal to the payload's by construction): the hop
    // works and settles exactly what it took.
    let evidence_id = row.evidence_id;
    let http_cutoff = local_deadline - Duration::from_secs_f64(dispatch.lease_seconds);
    let mut empty_retries = 0_u32;
    let mut malformed_retries = 0_u32;
    let mut empty_answer: Option<EmptyAnswer> = None;
    loop {
        let ready = match read_leg(
            dispatcher,
            claim.tenant_id,
            reasoning_domain_id,
            evidence_id,
        )
        .await
        {
            Ok(ReadLeg::Ready(ready)) => *ready,
            Ok(ReadLeg::Unavailable) => {
                eprintln!(
                    "humaux-private-worker: distill evidence={evidence_id} failed: no_output"
                );
                return settle_as(
                    dispatcher,
                    lease,
                    Some((row, OutboxSettle::Failed)),
                    DistillFinish::Done,
                    None,
                    0.0,
                    JobOutcome::Failed,
                )
                .await;
            }
            Ok(ReadLeg::NotReady(class)) if *calls == 0 => {
                return settle_not_ready(dispatcher, claim, lease, row, class).await;
            }
            // Became unready between two calls of one claim: a counted call happened.
            Ok(ReadLeg::NotReady(class)) => {
                return settle_failed_call(dispatcher, lease, row, *attempt, class).await;
            }
            Err(error) => return Err(error),
        };
        let evidence = &ready.evidence;
        let ceiling = evidence.origin_class.authority_ceiling(MemoryType::Fact);
        let envelope = DistillEnvelopeInput {
            origin_class: &evidence.origin_class_wire,
            max_class: ceiling,
            occurred_at: evidence.occurred_at,
            payload: &evidence.payload,
            offer_affects: ready.offer_affects,
        };
        let principal = acting_user(evidence, ready.admission.owner_user_id);
        let acting = if principal == ready.admission.owner_user_id
            || distill_repo::active_member(dispatcher.pool, claim.tenant_id, principal).await?
        {
            principal
        } else {
            ready.admission.owner_user_id
        };
        let prepared = dispatcher
            .reasoner
            .prepare(
                &ready.admission,
                acting,
                evidence.evidence_id,
                DataClass::parse_or_secret(&evidence.data_class),
                &envelope,
            )
            .await?;
        let admission = jobs::begin_distill_call(
            dispatcher.pool,
            lease,
            prepared.model_call_id,
            dispatch.min_remaining_seconds(),
            dispatch.budget,
        )
        .await?;
        let admitted_attempt = if let CallAdmission::Admitted(admitted) = admission {
            admitted
        } else {
            let refused = if admission == CallAdmission::OverBudget {
                PROVIDER_BUDGET
            } else {
                DISPATCH_REFUSED
            };
            dispatcher.reasoner.abandon(prepared).await?;
            if *calls == 0 {
                return settle_not_ready(dispatcher, claim, lease, row, refused).await;
            }
            // ADR-0048 D-D: a refused re-ask fails closed, never re-sends the prompt.
            if let Some(empty) = empty_answer.take() {
                return write_leg(
                    dispatcher,
                    lease,
                    row,
                    *attempt,
                    &empty.evidence,
                    &empty.inferred,
                    empty.processing_run_id,
                    &DistillReply::default(),
                    report,
                )
                .await;
            }
            return settle_as(
                dispatcher,
                lease,
                Some((row, OutboxSettle::Failed)),
                DistillFinish::Dead,
                Some(FAILED_OUTPUT_SCHEMA),
                0.0,
                JobOutcome::Dead,
            )
            .await;
        };
        *calls += 1;
        *attempt = admitted_attempt;
        report.attempts += 1;
        let outcome = dispatcher
            .reasoner
            .call(prepared, tokio::time::sleep_until(http_cutoff))
            .await;
        let answered = match outcome {
            Ok(DistillCallOutcome::Unknown) => {
                return Ok(Settled::new(JobOutcome::Unknown, Some(EXECUTION_UNCERTAIN)));
            }
            Ok(DistillCallOutcome::Failed(class))
                if class == ReasoningProviderError::WAITING_KEY_CLASS =>
            {
                // §11: WAITING_KEY = known blocked, never DEAD, no retry count.
                *attempt = (*attempt - 1).max(0);
                return settle_as(
                    dispatcher,
                    lease,
                    Some((row, OutboxSettle::Pending)),
                    DistillFinish::WaitingKey,
                    Some(class),
                    0.0,
                    JobOutcome::Parked,
                )
                .await;
            }
            // ADR-0058 D-M: a reply that broke the tool-call contract spends the malformed budget
            // exactly like one the parser refused (ADR-0048 D-D), never the provider-retry path.
            Ok(DistillCallOutcome::Failed(class)) if class == FAILED_OUTPUT_SCHEMA => None,
            Ok(DistillCallOutcome::Failed(class)) => {
                return settle_failed_call(dispatcher, lease, row, *attempt, class).await;
            }
            Ok(DistillCallOutcome::Answered(inferred)) => Some(inferred),
            Err(error) => return Err(error.into()),
        };
        let parsed = match answered {
            Some(inferred) => {
                parse_distill_output_detailed(&inferred.output_bytes, ready.offer_affects)
                    .map(|reply| (inferred, reply))
                    .map_err(DistillParseError::as_str)
            }
            None => Err("tool_call_shape"),
        };
        let (inferred, reply) = match parsed {
            Ok(parsed) => parsed,
            Err(reason) => {
                // Structural class only (`DistillParseError`), never payload text. The re-ask is
                // one more counted call (its own run / disclosure / ledger / distill_calls rows);
                // after the budget the job is DEAD and the outbox FAILED (ADR-0048 D-D).
                if malformed_retries < DISTILL_MALFORMED_RETRY_BUDGET {
                    malformed_retries += 1;
                    report.malformed_retries += 1;
                    empty_answer = None;
                    eprintln!(
                        "humaux-private-worker: distill evidence={evidence_id} malformed reply ({reason}), retry {malformed_retries}/{DISTILL_MALFORMED_RETRY_BUDGET}"
                    );
                    continue;
                }
                eprintln!(
                    "humaux-private-worker: distill evidence={evidence_id} failed: InvalidInput ({reason})"
                );
                return settle_as(
                    dispatcher,
                    lease,
                    Some((row, OutboxSettle::Failed)),
                    DistillFinish::Dead,
                    Some(FAILED_OUTPUT_SCHEMA),
                    0.0,
                    JobOutcome::Dead,
                )
                .await;
            }
        };
        if !reply.memories.is_empty() || empty_retries >= DISTILL_EMPTY_RETRY_BUDGET {
            return write_leg(
                dispatcher,
                lease,
                row,
                *attempt,
                &ready.evidence,
                &inferred,
                ready.processing_run_id,
                &reply,
                report,
            )
            .await;
        }
        // ADR-0048 (card 24) backstop: a clean parse with ZERO candidates is re-asked once. The
        // abandoned attempt is a call that SUCCEEDED with nothing in it, so its run is closed
        // with its own digest and `output_count = 0` (not the ADR-0016 D4 failure marker).
        let abandoned_digest: [u8; 32] = Sha256::digest(&inferred.output_bytes).into();
        let mut abandoned_txn = distill_repo::begin_write_context(
            dispatcher.pool,
            claim.tenant_id,
            ready.evidence.rls_user_id,
        )
        .await?;
        distill_repo::finish_processing_run(
            &mut abandoned_txn,
            ready.processing_run_id,
            &abandoned_digest,
            0,
            Some(&inferred.disclosure_id.to_string()),
        )
        .await?;
        abandoned_txn.commit().await?;
        empty_retries += 1;
        report.empty_retries += 1;
        eprintln!(
            "humaux-private-worker: distill evidence={evidence_id} returned zero candidates, retry {empty_retries}/{DISTILL_EMPTY_RETRY_BUDGET}"
        );
        empty_answer = Some(EmptyAnswer {
            inferred,
            processing_run_id: ready.processing_run_id,
            evidence: ready.evidence,
        });
    }
}

/// A DB/reasoning error inside a job: before any admitted call it is NOT_READY (with the error's
/// static class), after one it is a failed counted call (ADR-0058 D-E: one fenced settle in a
/// fresh transaction, so the job neither keeps its slot until `hard_deadline` nor loses it).
async fn settle_after_error(
    dispatcher: &Dispatcher<'_>,
    claim: &DistillClaim,
    lease: &DistillLease<'_>,
    row: &ClaimedEvidence,
    calls: u32,
    attempt: i32,
    error: &DistillError,
) -> Result<Settled, DistillError> {
    eprintln!(
        "humaux-private-worker: distill job={} evidence={} error: {error}",
        claim.job_id, row.evidence_id
    );
    if calls == 0 {
        settle_not_ready(dispatcher, claim, lease, row, error.class()).await
    } else {
        // ponytail: ADR-0058 R8 (c) known limit — a transient DB error in the write leg AFTER a
        // succeeded provider call settles RETRY here, and the next claim calls the provider again
        // (one extra billed call, counted). Upgrade: retry the write leg in place with the parsed
        // reply, inside the claim's hard deadline.
        settle_failed_call(dispatcher, lease, row, attempt, error.class()).await
    }
}

/// (c) of the module doc: ONE transaction — the generation-fenced job finish first, then the
/// authorized memories / rejected candidates, the run completion and the outbox DONE flip. Any
/// failure inside it rolls everything back and settles one fenced RETRY in a fresh transaction.
#[allow(
    clippy::too_many_arguments,
    reason = "the write leg's inputs are the job's lease, its row, its attempt and the one answered call"
)]
async fn write_leg(
    dispatcher: &Dispatcher<'_>,
    lease: &DistillLease<'_>,
    row: &ClaimedEvidence,
    attempt: i32,
    evidence: &LoadedEvidence,
    inferred: &DistillInferenceResult,
    processing_run_id: Uuid,
    reply: &DistillReply,
    report: &mut DistillDispatchReport,
) -> Result<Settled, DistillError> {
    match write_txn(
        dispatcher,
        lease,
        row,
        evidence,
        inferred,
        processing_run_id,
        &reply.memories,
    )
    .await
    {
        Ok(Some((inserted, rejected))) => {
            report.memories += inserted;
            report.rejected += rejected;
            // ADR-0058 R1: the memories are written; the job line names why no inferred affect was.
            report.affects_dropped += u32::from(reply.affects_dropped);
            // ADR-0058 R9: counted only once the parser accepted it and it was written.
            report.channel_fallback += u32::from(inferred.channel_fallback);
            Ok(Settled {
                channel_fallback: inferred.channel_fallback,
                ..Settled::new(
                    JobOutcome::Done,
                    reply.affects_dropped.then_some(AFFECT_INVALID_CLASS),
                )
            })
        }
        Ok(None) => Ok(Settled::new(JobOutcome::LeaseLost, None)),
        Err(error) => {
            eprintln!(
                "humaux-private-worker: distill job={} write failed: {error}",
                lease.job_id
            );
            settle_failed_call(dispatcher, lease, row, attempt, error.class()).await
        }
    }
}

/// `Ok(None)` = the generation fence refused (rolled back); `Ok(Some((memories, rejected)))`.
#[allow(
    clippy::too_many_lines,
    reason = "one transaction: finish first, then every candidate's insert or rejection, the run close and the outbox flip (ADR-0058 D-E)"
)]
async fn write_txn(
    dispatcher: &Dispatcher<'_>,
    lease: &DistillLease<'_>,
    row: &ClaimedEvidence,
    evidence: &LoadedEvidence,
    inferred: &DistillInferenceResult,
    processing_run_id: Uuid,
    candidates: &[DistillCandidate],
) -> Result<Option<(u32, u32)>, DistillError> {
    let tenant_id = lease.tenant_id;
    let scope = Scope {
        tenant_id: TenantId(tenant_id),
        user_id: evidence.visibility_user_id.map(UserId),
        workspace_id: evidence.visibility_workspace_id.map(WorkspaceId),
        repository_id: None,
        task_id: None,
        run_id: None,
        agent_id: None,
    };
    let mut txn =
        distill_repo::begin_write_context(dispatcher.pool, tenant_id, evidence.rls_user_id).await?;
    // ADR-0058 D-E: the finish is the FIRST statement — it locks the job row under its
    // generation, so a superseded worker writes nothing.
    if !distill_repo::finish_job_in_txn(
        &mut txn,
        lease,
        DistillFinish::Done,
        None,
        0.0,
        dispatcher.dispatch.not_ready_park_seconds,
    )
    .await?
    {
        txn.rollback().await?;
        return Ok(None);
    }
    let mut inserted: i32 = 0;
    let mut rejected = 0_u32;
    for candidate in candidates {
        let basis =
            NonEmptyVec::new(vec![evidence.origin_class]).expect("one-element basis is non-empty");
        match OriginBoundAuthorityPolicy.authorize(
            candidate.class,
            candidate.memory_type,
            basis,
            &scope,
        ) {
            Ok(authorized) => {
                let content = memory_content(&candidate.content);
                let memory_id = distill_repo::insert_memory(
                    &mut txn,
                    tenant_id,
                    evidence,
                    &NewMemory {
                        content: &content,
                        memory_type: candidate.memory_type,
                        class: authorized.0,
                        confidence: candidate.confidence,
                    },
                )
                .await?;
                // ADR-0058 D-P: inferred affects ride the memory's own write (the one affect row
                // issuer, origin DISTILL); a rejected candidate has no memory and its affects are
                // dropped with it.
                affect_repo::insert_inferred_in_txn(
                    &mut txn,
                    tenant_id,
                    memory_id,
                    evidence.evidence_id,
                    &candidate.affects,
                )
                .await
                .map_err(DistillError::Config)?;
                inserted += 1;
            }
            Err(rejection) => {
                // ponytail: no metrics facility exists in this crate tree yet (grepped:
                // `memory_candidate_rejections` has no code consumer); this structured line IS
                // the counter until a §53 emitter lands — same name, same label.
                eprintln!(
                    "memory_candidate_rejections_total{{reason=\"{}\"}} 1 tenant={} evidence={} requested={:?} origin={:?}",
                    rejection_reason(rejection),
                    tenant_id,
                    evidence.evidence_id,
                    candidate.class,
                    evidence.origin_class,
                );
                // ADR-0026 (Card 6): a §10.1-rejected candidate is persisted PENDING (same write
                // txn as the admitted memories) so a user can promote it via memory.confirm.
                let ttl_secs = candidate_ttl_seconds()?;
                let content = memory_content(&candidate.content);
                let content_bytes = serde_json::to_vec(&content)
                    .map_err(|_| DistillError::Config(ErrorCode::Internal))?;
                let sha_bytes = hex::decode(payload_sha256(&content_bytes).to_hex())
                    .map_err(|_| DistillError::Config(ErrorCode::Internal))?;
                distill_repo::insert_candidate(
                    &mut txn,
                    tenant_id,
                    evidence,
                    &distill_repo::NewCandidate {
                        body: &content,
                        sha256: &sha_bytes,
                        rejection,
                        requested_class: candidate.class,
                        memory_type: candidate.memory_type,
                        confidence: candidate.confidence,
                        ttl_seconds: ttl_secs,
                    },
                )
                .await?;
                rejected += 1;
            }
        }
    }
    let output_digest: [u8; 32] = Sha256::digest(&inferred.output_bytes).into();
    distill_repo::finish_processing_run(
        &mut txn,
        processing_run_id,
        &output_digest,
        inserted,
        Some(&inferred.disclosure_id.to_string()),
    )
    .await?;
    if !distill_repo::settle_outbox_in_txn(
        &mut txn,
        row.outbox_id,
        lease.lease_owner,
        OutboxSettle::Done,
    )
    .await?
    {
        txn.rollback().await?;
        return Ok(None);
    }
    txn.commit().await?;
    Ok(Some((
        u32::try_from(inserted).unwrap_or_default(),
        rejected,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> DistillDispatchConfig {
        DistillDispatchConfig {
            lease_owner: "worker-a".into(),
            lease_seconds: 30.0,
            in_flight: 4,
            hard_deadline_seconds: 300.0,
            http_timeout_seconds: 120.0,
            not_ready_park_seconds: 600.0,
            max_attempts: 5,
            budget: jobs::DistillCallBudget {
                window_seconds: 60.0,
                max_calls: 120,
            },
            credential_refs: BTreeSet::new(),
        }
    }

    /// ADR-0058 D-K — fault: weaken the bound to `http_timeout + lease`.
    #[test]
    fn config_rejects_a_hard_deadline_that_cannot_hold_a_call_and_its_reask() {
        let ok = config();
        assert!(
            ok.validate().is_ok(),
            "2 × (120 + 30) = 300 is exactly enough"
        );
        let short = DistillDispatchConfig {
            hard_deadline_seconds: 2.0 * (120.0 + 30.0) - 1.0,
            ..ok.clone()
        };
        assert!(short.validate().is_err());
        for bad in [
            DistillDispatchConfig {
                in_flight: 0,
                ..ok.clone()
            },
            DistillDispatchConfig {
                lease_seconds: 0.0,
                ..ok.clone()
            },
            DistillDispatchConfig {
                max_attempts: 0,
                ..ok.clone()
            },
            DistillDispatchConfig {
                not_ready_park_seconds: 0.0,
                ..ok.clone()
            },
            DistillDispatchConfig {
                lease_owner: " ".into(),
                ..ok.clone()
            },
            DistillDispatchConfig {
                budget: jobs::DistillCallBudget {
                    window_seconds: 0.0,
                    max_calls: 120,
                },
                ..ok.clone()
            },
            DistillDispatchConfig {
                budget: jobs::DistillCallBudget {
                    window_seconds: 60.0,
                    max_calls: 0,
                },
                ..ok.clone()
            },
        ] {
            assert!(bad.validate().is_err());
        }
    }

    /// ADR-0058 R2 — fault: accept `in_flight = 5`. A seat past the seeded slot rows can never
    /// hold a slot.
    #[test]
    fn in_flight_is_bounded_by_the_seeded_slot_rows() {
        for in_flight in 1..=PROVIDER_SLOTS {
            let ok = DistillDispatchConfig {
                in_flight,
                ..config()
            };
            assert!(ok.validate().is_ok(), "in_flight={in_flight}");
        }
        for in_flight in [0, PROVIDER_SLOTS + 1] {
            let bad = DistillDispatchConfig {
                in_flight,
                ..config()
            };
            assert_eq!(
                bad.validate(),
                Err(ErrorCode::InvalidInput),
                "in_flight={in_flight}"
            );
        }
    }

    /// Card 24 review P1: a counter `add` forgets is structurally zero on the only line
    /// production prints.
    #[test]
    fn add_folds_every_counter() {
        let one = DistillDispatchReport {
            claimed: 1,
            completed: 1,
            failed: 1,
            not_ready: 1,
            parked: 1,
            deferred: 1,
            dead: 1,
            lost_lease: 1,
            errors: 1,
            heartbeat_lost: 1,
            attempts: 1,
            unknown: 1,
            memories: 1,
            rejected: 1,
            empty_retries: 1,
            malformed_retries: 1,
            affects_dropped: 1,
            channel_fallback: 1,
            stopped: Some(DrainStop::NoWork),
        };
        let mut total = DistillDispatchReport::default();
        total.add(one);
        total.add(DistillDispatchReport {
            stopped: Some(DrainStop::NoSlot),
            ..one
        });
        total.add(one);
        let line = total.summary_line();
        assert_eq!(line.matches("=3").count(), 18, "{line}");
        assert!(
            line.ends_with(" stopped=no_slot"),
            "the worse seat reason wins: {line}"
        );
    }

    #[test]
    fn payload_identity_needs_both_ids() {
        let d = Uuid::from_u128(1);
        let e = Uuid::from_u128(2);
        let payload =
            serde_json::json!({"reasoning_domain_id": d.to_string(), "evidence_id": e.to_string()});
        assert_eq!(payload_identity(&payload), Some((d, e)));
        assert_eq!(
            payload_identity(&serde_json::json!({"evidence_id": e.to_string()})),
            None
        );
        assert_eq!(payload_identity(&serde_json::json!({})), None);
    }

    #[test]
    fn memory_content_carries_the_card_fields() {
        let content = memory_content("New services must expose a health endpoint.");
        assert_eq!(
            content["key_claim"],
            "New services must expose a health endpoint."
        );
        assert_eq!(
            content["title"],
            "New services must expose a health endpoint."
        );
        let long = memory_content(&"x".repeat(200));
        assert_eq!(long["title"].as_str().map(str::len), Some(80));
    }

    #[test]
    fn rejection_reasons_match_the_metrics_registry_labels() {
        assert_eq!(
            rejection_reason(CandidateRejection::OriginAuthorityCeiling),
            "origin_authority_ceiling"
        );
        assert_eq!(
            rejection_reason(CandidateRejection::UntrustedInstruction),
            "untrusted_instruction"
        );
    }
}
