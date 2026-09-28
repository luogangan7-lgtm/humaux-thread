//! `retrieval::completeness` — `LedgerCounts` / `CompletenessClass` / `FreshnessClass` / the `ledger::close` and
//!   `classify()` sole constructors (§22.4 / §22.5 / §59).
//! Depends-on: crates=[humaux-domain, serde]; services=[];
//!   env=[]; modules=[domain::context, domain::ledger, retrieval::envelope, retrieval::planner]
//! Called-by: [adapters::context_repo, adapters::exact_census, adapters::retrieve, adapters::stream_repo, gateway::context, gateway::memory, gateway::recall, retrieval::envelope, retrieval::signals, tests]
//! Invariants: []
//! Spec: §22.4; §22.5; §59; §41.2; §25.3; §78.2
//!
//! `classify()` is the sole constructor of [`CompletenessClass`]; final outcome emission is
//! the sole increment point of `retrieval_completeness_total{class,reason}` (§22.5, §41.2).
//! Its
//! parameters are `planner_output` / `lane_status` / `census_result` / `ledger` /
//! `mandatory_missing` — §22.5's frozen four plus the §25.3 Mandatory shortfall added by card
//! 22c's review (an unmet obligation moves `completeness`, see
//! [`CannotEstablishReason::MandatoryNotSatisfied`]); this module reuses [`crate::planner::PlannerDecision`] for
//! `planner_output` and [`crate::envelope::LaneStatus`] for `lane_status` rather than
//! inventing second representations of either — only `CensusResult` is minted here, because
//! no Authority-census type exists anywhere in the workspace yet (see its own doc for scope).
//!
//! Note: the exhaustiveness test at the bottom of this file pins variant *count and shape*,
//! not the verbatim wire name of each variant. Verbatim string pinning is deferred to the
//! §78.2 DB↔Rust contract-test card, not covered here.
//!
//! Spec adjudications (recorded here so a later wave does not flip them back):
//! 1. Visibility: §59's code block writes `pub enum CompletenessClass`; §22.5 freezes it as
//!    private, produced only by the sole constructor `classify()`. §22 is the home chapter
//!    for the Completeness Contract, so per CLAUDE.md ("on conflict with spec, the spec's own
//!    home chapter wins") §22.5 wins — `CompletenessClass` is `pub(crate)` here.
//!    `FreshnessClass` has no such rule (§21.5 imposes no privacy constraint) and stays `pub`.
//! 2. Shape: §22.5 freezes `CannotEstablish` as `CannotEstablish { reason: "ledger_not_closed" }`
//!    — a reason field. §78.2 bans stringly-typed domain, so `reason` here is a closed enum
//!    ([`CannotEstablishReason`]), not a loose `&'static str`.
//! 3. G80-6 witness reachability: `classify()` must stay `pub(crate)` (adjudication 1), but
//!    §80.1 G80-6's component witness harness runs from `crates/testkit` — a different crate —
//!    and can only call `pub` entry points. [`classify_for_witness`] calls `classify()` verbatim
//!    and returns only its wire label pair, never the `CompletenessClass` value (a `pub` fn
//!    cannot name a `pub(crate)` return type anyway). It is label-only: the actual final metric
//!    remains reachable solely through `envelope_outcome_block` after full Envelope validation.
//! 4. `PlannerDecision::Class(_)` (every §20 wire class except `DirectGet`/`Enumerate`,
//!    including `State` — §22.2 FACET_COMPLETE's natural source) maps to `SemanticBounded`,
//!    not `FacetComplete`: the signature carries no `covered_facets` /
//!    `required_facets` count, so claiming `FacetComplete` here would be an unproven
//!    completeness claim (§22.0's "no verbal EXACT claim" invariant, generalized).
//!    `FacetComplete` stays a real, constructible variant (exercised directly by this file's
//!    own exhaustiveness test) — just not reachable through `classify()` until a later wave
//!    threads facet-coverage counts into the signature.
// ponytail: adjudication 4's gap — `classify()` never emits `FacetComplete` today. Upgrade
// path: add a facet-coverage input (covered/required counts) to `classify()`'s signature and
// give `PlannerDecision::Class(QueryClass::State)` its own arm once that lands.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Ledger's six fields (§22.5): produced by `ledger::close(repo, stream_key)` taking three
/// independent reads (`stream_log_agg` / `count_open_gaps` / `contiguous_done_prefix`) and
/// judging A1 on the spot. Callers cannot reach the fields, nor assemble a
/// `LedgerClosure::Closed` themselves — that variant belongs to `ledger::close`'s output.
/// The architecture-check for "field set is exactly these 6" is §23.1②.
#[derive(Debug, Clone)]
pub struct LedgerCounts {
    expected: u64,
    done: u64,
    deleted: u64,
    skipped: u64,
    open_gaps: u64,
    pending: u64,
}

/// Completeness class, a closed set of 4 (§59; degrade direction per §22.5:
/// `EXACT -> SemanticBounded` is not allowed — that transition does not exist in the enum).
///
/// `pub(crate)`: §22.5 freezes this as private, produced only by [`classify()`] — see the
/// module-level spec-adjudication note above for why this overrides §59's `pub` code block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletenessClass {
    /// Every fact in scope was verified present (§59).
    Exact,
    /// Complete along every requested facet, though not exhaustively verified (§59).
    // ponytail: not constructed by `classify()` yet (adjudication 4 above) — allow(dead_code)
    // on this one variant only; remove once facet-coverage counts thread into the signature.
    #[allow(dead_code)]
    FacetComplete,
    /// Bounded by semantic/embedding recall, not a hard count (§59).
    SemanticBounded,
    /// Completeness could not be determined for this query (§59), with a reason (adjudication
    /// 2 above) and never silent — a bare unit variant cannot express §22.4's "at least N, I
    /// cannot prove this is all of it".
    CannotEstablish { reason: CannotEstablishReason },
}

impl CompletenessClass {
    /// §23.3 wire label pair (`class`, `reason`) — `reason` is `"none"` for every class except
    /// `CannotEstablish`. This is the single place the counter's label strings and any future
    /// JSON `class` string share a source, so the two cannot drift apart.
    pub(crate) fn wire_labels(self) -> (&'static str, &'static str) {
        match self {
            Self::Exact => ("exact", "none"),
            Self::FacetComplete => ("facet_complete", "none"),
            Self::SemanticBounded => ("semantic_bounded", "none"),
            Self::CannotEstablish { reason } => ("cannot_establish", reason.label()),
        }
    }
}

/// §22.4 `CANNOT_ESTABLISH` reason, a closed set (§78.2: no stringly-typed domain) covering
/// exactly what `classify()`'s signature can observe: A1 broken, Planner's own non-enumerable
/// verdict, census failure, lane failure, and (card 22c) an unmet Mandatory obligation.
/// `projection lag` (also named in §22.4's trigger list) has no input path into this signature
/// and so is not a variant here — out of scope for this constructor, not silently folded into
/// one of the others.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CannotEstablishReason {
    /// A1 broken (§22.4/§23.1②): `done + open_gaps + pending != expected`.
    LedgerNotClosed,
    /// §20.2 rule 4 / §22.4 "谓词不可枚举": the Planner itself returned `CannotEstablish` —
    /// never re-classified as `SemanticBounded` (§20.2/§22.5 frozen).
    PredicateNotEnumerable,
    /// §22.4 trigger 4: Authority census failed, or could not define an enumerable universe.
    CensusFailed,
    /// §22.4 "lane 故障".
    LaneFailed,
    /// §23.1②: A1 held but the Qdrant `visible` count could not be taken at all — a projection-
    /// side cannot-establish `classify()`'s own signature (no `visible` input)
    /// cannot see; only [`crate::envelope::assemble_completeness_class`] can observe it.
    // ponytail: only constructed from that fn's own `#[cfg(test)]` callers today — no
    // `application`-layer caller exists yet to assemble a full envelope in production (out of
    // this crate's scope, see `envelope.rs`'s own module doc). `allow(dead_code)` on this one
    // variant, same precedent as `CompletenessClass::FacetComplete` above; remove once a real
    // caller reaches `assemble_completeness_class`.
    #[allow(dead_code)]
    IndexCountUnavailable,
    /// §23.1②: A2's `>` side exceeded `pending` — index holds points the ledger never issued a
    /// ticket for, untrustworthy on both sides. Same "projection-side, not visible to
    /// `classify()`'s signature" reasoning, and the same test-only-caller ceiling, as
    /// [`Self::IndexCountUnavailable`].
    #[allow(dead_code)]
    A2OvershootBeyondPending,
    /// §25.5：Mandatory Context 自身超过硬上限。
    ///
    /// **这是唯一允许的结果**——§25.5 原文「禁止静默截掉后半段并仍声称 complete」。
    /// 产出它的是 `domain::context::ContextBudget::reserve` 的 `Err` 臂，而
    /// `MandatoryOverflow` 里**没有任何可返回的 Context**：截断不是不该做的操作，
    /// 是那个臂里没有那个值可以返回。
    MandatoryContextOverflow,
    /// A required pipeline count is unavailable in the declared universe.
    CountUnknown,
    /// Pipeline count blocks cannot be compared because their universes differ.
    CountScopeMismatch,
    /// Known pipeline counts in one universe disagree.
    PipelineCountMismatch,
    /// §22.4 / §25.3 (card 22c review debt): the Mandatory lane finished, but it did not
    /// return everything it was obliged to return — `handoff.counts.mandatory_missing > 0`.
    ///
    /// An unmet **obligation** is not a weaker answer, it is an unestablished one: the caller
    /// asked for context that §25.4 says must be present, and some of it is not. Before this
    /// variant existed the shortfall was reported only as a number inside the handoff counts
    /// while `completeness.class` still said `semantic_bounded` — i.e. the envelope claimed a
    /// bounded-but-sound answer over a context it knew was short. §25.5 already refuses to
    /// silently truncate Mandatory ([`Self::MandatoryContextOverflow`]); this is the same
    /// refusal for the "lane ran and came back short" shape, which overflow cannot express.
    MandatoryNotSatisfied,
}

/// §25.5 的唯一映射：Mandatory Context 溢出 ⇒ `cannot_establish`。
///
/// 「mandatory overflow 只能 `cannot_establish`，不能静默截断」这句话在类型上的落点。
/// 收 [`humaux_domain::context::MandatoryOverflow`] 而不是收一个 bool：那个类型只可能来自
/// `ContextBudget::reserve` 的 `Err` 臂，所以**造不出一个"假装溢出"的调用**；而它里面
/// 没有任何可返回的 Context，所以调用方在这条路径上也拿不到"截断后的前半段"。
///
/// 返回 [`CompletenessClass`] 而不是直接返回 reason：§25.5 要的是 class **和** reason
/// 一起变，分开返回就给了「reason 记了但 class 还是 exact」这条静默通道。
#[must_use]
pub(crate) const fn overflow_class(
    _overflow: &humaux_domain::context::MandatoryOverflow,
) -> CompletenessClass {
    CompletenessClass::CannotEstablish {
        reason: CannotEstablishReason::MandatoryContextOverflow,
    }
}

impl CannotEstablishReason {
    fn label(self) -> &'static str {
        match self {
            Self::LedgerNotClosed => "ledger_not_closed",
            Self::PredicateNotEnumerable => "predicate_not_enumerable",
            Self::CensusFailed => "census_failed",
            Self::LaneFailed => "lane_failed",
            Self::IndexCountUnavailable => "index_count_unavailable",
            Self::A2OvershootBeyondPending => "a2_overshoot_beyond_pending",
            Self::MandatoryContextOverflow => "mandatory_context_overflow",
            Self::CountUnknown => "count_unknown",
            Self::CountScopeMismatch => "count_scope_mismatch",
            Self::PipelineCountMismatch => "pipeline_count_mismatch",
            Self::MandatoryNotSatisfied => "mandatory_not_satisfied",
        }
    }
}

/// §22.1 EXACT enumeration readout — the six-field structured block behind "必须结构化枚举".
///
/// Fields are private and [`ExactEnumeration::new`] is the sole constructor so the two §22.1
/// derivation disciplines cannot be hand-picked around:
///
/// - `coverage` / `truncated` are **derived** accessors, not stored fields — the same
///   "derived, not hand-picked" rule [`crate::envelope::RetrievalBlock`] applies to its own
///   `truncated == (candidate_count > returned)` (§23.3). A stored `coverage` field would be
///   a second representation of `returned / total` that only stays honest by discipline.
/// - `new` rejects `returned + excluded_secret > total`: a readout claiming to have returned
///   (or excluded) more rows than its own denominator holds is exactly the 分母内生 shape
///   §22.1 forbids ("禁止用召回条数冒充 `total` —— 那是分母内生，永远得 1.0"). The reverse
///   direction (`returned + excluded_secret < total`) is legal — that is what `truncated`
///   reports.
/// - `new` rejects a blank `predicate_id`: §22.0 freezes "`class=exact` 而 `predicate_id=null`
///   是不变量违反" — an enumeration with no predicate id could never legally reach the wire,
///   so it cannot be minted at all.
#[derive(Debug, Clone, PartialEq)]
pub struct ExactEnumeration {
    predicate_id: String,
    total: u64,
    returned: u64,
    excluded_secret: u64,
}

/// [`ExactEnumeration::new`] rejection — a §50-style construction-time fault (garbage readout
/// refused at the door), not a §52 runtime `ErrorCode`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactEnumerationError(pub String);

impl fmt::Display for ExactEnumerationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ExactEnumerationError {}

impl ExactEnumeration {
    /// Sole constructor (see the type doc for the two invariants it enforces).
    pub fn new(
        predicate_id: impl Into<String>,
        total: u64,
        returned: u64,
        excluded_secret: u64,
    ) -> Result<Self, ExactEnumerationError> {
        let predicate_id = predicate_id.into();
        if predicate_id.trim().is_empty() {
            return Err(ExactEnumerationError(
                "predicate_id must not be blank — §22.0: class=exact with predicate_id=null \
                 is an invariant violation, so the enumeration cannot even be minted"
                    .to_string(),
            ));
        }
        if returned + excluded_secret > total {
            return Err(ExactEnumerationError(format!(
                "returned ({returned}) + excluded_secret ({excluded_secret}) > total ({total}) \
                 — §22.1: total comes from count(*) in the same transaction snapshot; a readout \
                 that claims more rows than its own denominator is the 分母内生 shape §22.1 \
                 forbids"
            )));
        }
        Ok(Self {
            predicate_id,
            total,
            returned,
            excluded_secret,
        })
    }

    pub fn predicate_id(&self) -> &str {
        &self.predicate_id
    }
    pub fn total(&self) -> u64 {
        self.total
    }
    pub fn returned(&self) -> u64 {
        self.returned
    }
    pub fn excluded_secret(&self) -> u64 {
        self.excluded_secret
    }

    /// §22.1 `coverage` = `returned / total`, derived on read. `excluded_secret > 0` deducts
    /// coverage by construction (the excluded rows stay in `total`, are absent from
    /// `returned`) — "不许静默少给". Empty universe (`total == 0`) is vacuously complete.
    pub fn coverage(&self) -> f64 {
        if self.total == 0 {
            1.0
        } else {
            // Precision note: exact only up to 2^53 rows, far beyond any real enumeration.
            #[allow(clippy::cast_precision_loss)]
            {
                self.returned as f64 / self.total as f64
            }
        }
    }

    /// §22.1 `truncated`: rows that are neither returned nor accounted for as
    /// `excluded_secret` — i.e. a caller-side cap cut the list. Secret exclusion alone does
    /// NOT set this: those rows are named by `excluded_secret`, not silently dropped.
    pub fn truncated(&self) -> bool {
        self.returned + self.excluded_secret < self.total
    }
}

/// Readout `classify()` needs from Authority census (§22.4 trigger 4: "Authority
/// census 本身失败或当前授权策略明确排除了一部分权威行且无法定义可枚举的 authorized
/// universe"), now also the carrier of the §22.1 enumeration block when the census actually
/// enumerated (T8 census wiring — the previous `pub ok: bool` placeholder let any caller
/// claim a passing census without handing over what it counted).
///
/// Fields are private; the three constructors below are the closed set of shapes:
/// `enumerated` (EXACT path — ok, with the §22.1 block), `ok_without_enumeration` (semantic
/// path — census passed, nothing to enumerate), `failed`. The fourth combination (failed but
/// carrying an enumeration) is unconstructible.
///
/// `pub`: `adapters` is this type's constructor (`exact_census`), same crate-boundary
/// reasoning as `ledger::LedgerReads` below, and the G80-6 witness (adjudication 3) must be
/// able to build one from `crates/testkit`.
#[derive(Debug, Clone, PartialEq)]
pub struct CensusResult {
    ok: bool,
    enumeration: Option<ExactEnumeration>,
}

impl CensusResult {
    /// Census ran and enumerated: the EXACT path (§22.1). The only way to attach an
    /// enumeration — and it is inseparable from `ok`.
    pub fn enumerated(enumeration: ExactEnumeration) -> Self {
        Self {
            ok: true,
            enumeration: Some(enumeration),
        }
    }

    /// Census passed with nothing to enumerate: the semantic path, where no §22.1 universe
    /// was requested (a `SemanticBounded` answer never carries an enumeration block).
    pub fn ok_without_enumeration() -> Self {
        Self {
            ok: true,
            enumeration: None,
        }
    }

    /// §22.4 trigger 4: census failed ⇒ `classify()` yields `CannotEstablish/CensusFailed`.
    pub fn failed() -> Self {
        Self {
            ok: false,
            enumeration: None,
        }
    }

    pub fn is_ok(&self) -> bool {
        self.ok
    }

    pub fn enumeration(&self) -> Option<&ExactEnumeration> {
        self.enumeration.as_ref()
    }
}

/// Freshness class, a closed set of 4 (§59).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessClass {
    /// Within the fresh window (§59).
    Fresh,
    /// Past fresh but not yet stale (§59).
    Aging,
    /// Past the staleness threshold (§59).
    Stale,
    /// Freshness could not be determined (§59).
    Unknown,
}

// ============================================================================
// §22.5 — `ledger::close`: the sole constructor of `LedgerCounts` / `LedgerClosure`.
// ============================================================================

/// §22.4/§22.5 "ledger closed" outcome. Both variants still carry the six counts — a
/// `Broken` ledger must still report what it saw (§22.4: `CANNOT_ESTABLISH` needs "at least N,
/// I cannot prove this is all of it", not silence).
///
/// `pub`, unlike `LedgerCounts`/`CompletenessClass`: this is the opaque handle `adapters`
/// passes across the crate boundary (obtained from [`ledger::close`], consumed by
/// [`crate::envelope::build_projection_block`] and by [`classify`]) — its own two variants
/// carry `LedgerCounts` by value, but `LedgerCounts`'s *fields* stay unreachable from outside
/// this module (private + no external constructor), so exposing this enum does not reopen the
/// sole-construction-point guarantee `ledger::close` exists to hold.
#[derive(Debug, Clone)]
pub enum LedgerClosure {
    Closed(LedgerCounts),
    Broken(LedgerCounts),
}

impl LedgerClosure {
    /// A1 (§23.1②): `true` iff `done + open_gaps + pending == expected` held.
    pub fn is_closed(&self) -> bool {
        matches!(self, Self::Closed(_))
    }

    /// The six counts either way — a `Broken` closure is still required to report its known
    /// lower bound (§22.4), never to withhold the numbers it did manage to read.
    pub fn counts(&self) -> &LedgerCounts {
        match self {
            Self::Closed(c) | Self::Broken(c) => c,
        }
    }
}

impl LedgerCounts {
    // Read-only accessors, not `pub` fields: making the fields themselves `pub` (even just
    // `pub(crate)`) would let any caller write a `LedgerCounts { .. }` struct literal, which
    // is exactly the second construction point §22.5's "sole constructor" invariant (and its
    // architecture-check, §23.1②) exists to forbid. Fields stay fully private (module +
    // descendants only, i.e. `ledger::close` below) even though the *struct* itself is `pub`
    // (needed so `LedgerClosure` — which cross-crate callers must be able to name — can hold
    // one without tripping rustc's `private_interfaces` lint); every reader, in this crate or
    // out, goes through these getters.
    pub fn expected(&self) -> u64 {
        self.expected
    }
    pub fn done(&self) -> u64 {
        self.done
    }
    pub fn deleted(&self) -> u64 {
        self.deleted
    }
    pub fn skipped(&self) -> u64 {
        self.skipped
    }
    pub fn open_gaps(&self) -> u64 {
        self.open_gaps
    }
    pub fn pending(&self) -> u64 {
        self.pending
    }
}

/// §22.5's sole constructor of [`LedgerCounts`]/[`LedgerClosure`] — nested as a descendant
/// module of `completeness` precisely so it (and only it) can see `LedgerCounts`'s private
/// fields and legally write the one `LedgerCounts { .. }` struct literal in the workspace.
/// `pub`: `adapters` (a later task, out of this crate) is the eventual caller that has
/// actually run the independent PostgreSQL reads and needs to hand them in.
pub mod ledger {
    use super::{LedgerClosure, LedgerCounts};

    /// The independently-fetched raw counts [`close`] judges A1 over. A plain, freely
    /// constructible struct — same split `projection::stream::advance_prefix` already uses
    /// (`StreamLedgerSnapshot` is a bare pub struct; the *judged output* is what stays
    /// guarded). The actual PostgreSQL round trips (`stream_checkpoints.issued_highwater` /
    /// `stream_log` state-filtered counts / the `processing_gaps` view, each a separate query
    /// under one `SET LOCAL humaux.tenant_id`-scoped transaction) are `adapters`-layer work —
    /// this crate holds no SQL (§3/§78.3, matching `projection::stream`'s own module doc) and
    /// only judges the identity over numbers a caller already read independently.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct LedgerReads {
        /// `projection.stream_checkpoints.issued_highwater` — §23.1② table's
        /// `projection.expected` (not `evidence.expected`, a different quantity, §23.1②).
        pub expected: u64,
        /// `count(state IN ('DONE','SKIPPED_BY_POLICY','TOMBSTONED','RETIRED_FAILED'))` — the
        /// §15.2 SETTLED_OK union as 0167 widened it, not decomposed (§23.3 worked example:
        /// `done 95 = 90 DONE + 3 TOMBSTONED + 2 SKIPPED_BY_POLICY`).
        pub done: u64,
        /// `count(state = 'TOMBSTONED')` — a subset already counted inside `done` above, also
        /// reported standalone: §23.1②'s A2 and the `visible` overlay both need it on its own.
        pub deleted: u64,
        /// `count(state IN ('SKIPPED_BY_POLICY','RETIRED_FAILED'))` — subset of `done`,
        /// reported standalone so A2 can credit it on its own left-hand term without it
        /// inflating the denominator (§23.1②: "计入 A2 左边、不进分母"). 0167's audited
        /// retirement joins this term rather than `deleted`: it is settled and can never become
        /// visible (so A2 needs it here), but it is not a §37 deletion (so it must not leave the
        /// denominator, and it has no bytes for the §65 purge job to chase).
        pub skipped: u64,
        /// `count(*)` from the `projection.processing_gaps` view (`FAILED | LOST`, §15.1).
        pub open_gaps: u64,
        /// `count(state IN ('ISSUED','PROCESSING','WAITING_KEY','RETRY_WAIT'))`.
        pub pending: u64,
    }

    /// §22.5 `ledger::close`: judges §23.1② A1 (`done + open_gaps + pending == expected`) and
    /// returns `Broken` — never panics, never silently clamps — when it does not hold. Does
    /// **not** judge A2 (§22.4/§23.1②: "A2 只能在 envelope 层求值", `ledger::close` 不判 A2") —
    /// this function has no `visible` input at all, by construction, so A2 cannot be judged
    /// here even by accident. `retrieval_completeness_total{class,reason}`'s single increment
    /// point (§22.5) lives in final Envelope outcome assembly, not here.
    pub fn close(reads: LedgerReads) -> LedgerClosure {
        let counts = LedgerCounts {
            expected: reads.expected,
            done: reads.done,
            deleted: reads.deleted,
            skipped: reads.skipped,
            open_gaps: reads.open_gaps,
            pending: reads.pending,
        };
        // §22.5「A1 的算式全库只此一处」：算式在 `humaux_domain::ledger::a1_holds`，本处只做
        // 三次独立取数与 fail-closed 分流（§78.3：domain 不碰 repo，取数留在这里）。
        if humaux_domain::ledger::a1_holds(
            reads.expected,
            reads.done,
            reads.open_gaps,
            reads.pending,
        ) {
            LedgerClosure::Closed(counts)
        } else {
            LedgerClosure::Broken(counts)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn close_reports_closed_when_a1_identity_holds() {
            let closure = close(LedgerReads {
                expected: 100,
                done: 100,
                deleted: 10,
                skipped: 0,
                open_gaps: 0,
                pending: 0,
            });
            assert!(closure.is_closed());
            assert_eq!(closure.counts().expected(), 100);
            assert_eq!(closure.counts().deleted(), 10);
        }

        /// §22.4: `done + open_gaps + pending != expected` ⇒ `Broken`, still carrying the
        /// counts it read (the "known lower bound" `CANNOT_ESTABLISH` must report).
        #[test]
        fn close_reports_broken_when_a1_identity_breaks() {
            let closure = close(LedgerReads {
                expected: 100,
                done: 90,
                deleted: 0,
                skipped: 0,
                open_gaps: 0,
                pending: 0, // 90+0+0 = 90 != 100
            });
            assert!(!closure.is_closed());
            assert_eq!(closure.counts().done(), 90);
        }

        /// §23.3 worked example verbatim: `95 + 1 + 2 == 98` ⇒ closed, and `done` stays the
        /// union (95), not decomposed by this function.
        #[test]
        fn close_matches_23_3_worked_example() {
            let closure = close(LedgerReads {
                expected: 98,
                done: 95,
                deleted: 3,
                skipped: 2,
                open_gaps: 1,
                pending: 2,
            });
            assert!(closure.is_closed());
            let c = closure.counts();
            assert_eq!(
                (c.expected(), c.done(), c.deleted(), c.skipped()),
                (98, 95, 3, 2)
            );
        }
    }
}

// ============================================================================
// §41.2 `retrieval_completeness_total{class,reason}` — process-local placeholder counter.
// ============================================================================

/// Same pattern as `telemetry::degrade::DegradeTotal` (`crates/telemetry/src/degrade.rs`):
/// keeps the count observable for §80.1 G80-6's witness before the real Prometheus
/// `IntCounterVec` registration lands with the `telemetry::metrics` task.
// ponytail: process-local `AtomicU64` grid, no real Prometheus `IntCounterVec` yet — same
// ceiling and upgrade path as `DEGRADE_TOTAL`; replace both together when that task lands.
struct CompletenessTotal([AtomicU64; CompletenessTotal::CELLS]);

impl CompletenessTotal {
    const CLASSES: [&'static str; 4] = [
        "exact",
        "facet_complete",
        "semantic_bounded",
        "cannot_establish",
    ];
    const REASONS: [&'static str; 12] = [
        "none",
        "ledger_not_closed",
        "predicate_not_enumerable",
        "census_failed",
        "lane_failed",
        "index_count_unavailable",
        "a2_overshoot_beyond_pending",
        "mandatory_context_overflow",
        "count_unknown",
        "count_scope_mismatch",
        "pipeline_count_mismatch",
        "mandatory_not_satisfied",
    ];
    const CELLS: usize = Self::CLASSES.len() * Self::REASONS.len();

    // One cell per (class, reason) pair. Written as an inline-`const` repeat expression so
    // `CELLS` is the only place the length lives: the previous hand-written expansion had to
    // be grown by hand every time `REASONS` gained a variant, and the comment saying so was
    // the only thing enforcing it. `const { ... }` sidesteps the `AtomicU64: !Copy` problem
    // that forced the expansion, and — unlike a `const ZERO` item — does not trip
    // `clippy::declare_interior_mutable_const`.
    const fn new() -> Self {
        Self([const { AtomicU64::new(0) }; Self::CELLS])
    }

    fn idx(class_label: &str, reason_label: &str) -> usize {
        let c = Self::CLASSES
            .iter()
            .position(|&s| s == class_label)
            .expect("class_label must be one of CompletenessClass::wire_labels' outputs");
        let r = Self::REASONS
            .iter()
            .position(|&s| s == reason_label)
            .expect(
                "reason_label must be one of CannotEstablishReason::label's outputs, or \"none\"",
            );
        c * Self::REASONS.len() + r
    }

    fn inc(&self, class_label: &str, reason_label: &str) {
        self.0[Self::idx(class_label, reason_label)].fetch_add(1, Ordering::Relaxed);
    }

    fn count(&self, class_label: &str, reason_label: &str) -> u64 {
        self.0[Self::idx(class_label, reason_label)].load(Ordering::Relaxed)
    }
}

static RETRIEVAL_COMPLETENESS_TOTAL: CompletenessTotal = CompletenessTotal::new();

/// current count for `{class,reason}` — read-only accessor for witness/test assertions,
/// without exposing the counter itself (mirrors `degrade::degrade_total_count`). `class`/
/// `reason` are the wire label strings [`CompletenessClass::wire_labels`] produces (e.g.
/// `"cannot_establish"` / `"ledger_not_closed"`, or `"none"` for a non-`CannotEstablish`
/// class's reason).
pub fn retrieval_completeness_total_count(class: &str, reason: &str) -> u64 {
    RETRIEVAL_COMPLETENESS_TOTAL.count(class, reason)
}

/// Sole final-outcome metric emission. Call only after the envelope outcome's invariants have
/// been checked; provisional classifier answers must never be observed as result statistics.
pub(crate) fn record_final_classification(class: CompletenessClass) {
    let (class_label, reason_label) = class.wire_labels();
    RETRIEVAL_COMPLETENESS_TOTAL.inc(class_label, reason_label);
    #[cfg(test)]
    FINAL_RECORD_TRACE.with(|trace| trace.borrow_mut().push((class_label, reason_label)));
}

#[cfg(test)]
thread_local! {
    static FINAL_RECORD_TRACE: std::cell::RefCell<Vec<(&'static str, &'static str)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
pub(crate) fn take_final_record_trace() -> Vec<(&'static str, &'static str)> {
    FINAL_RECORD_TRACE.with(|trace| std::mem::take(&mut *trace.borrow_mut()))
}

// ============================================================================
// §22.5 — `classify()`: the sole constructor of `CompletenessClass`.
// ============================================================================

/// §22.5's sole pure constructor of [`CompletenessClass`]. Final outcome assembly records the
/// resulting class only after all later gates have passed.
///
/// Match order is the frozen degrade-direction table (§22.5): ledger closure is checked
/// *before* `planner_output` is read at all — a broken ledger blocks every class, EXACT
/// included (§22.4). Census failure and lane failure are checked next (§22.4's other two
/// `CANNOT_ESTABLISH` triggers this signature can observe), then `planner_output` decides
/// between `Exact` (a `DirectGet`/`Enumerate` decision — both are fully-defined, enumerable
/// sets) and the fallback `SemanticBounded` for everything else (adjudication 4 in this
/// module's doc: `FacetComplete` is not reachable here yet). `PlannerDecision::CannotEstablish`
/// is never re-classified — §20.2/§22.5 freeze "不降到 SEMANTIC_BOUNDED, 直接
/// CANNOT_ESTABLISH".
pub(crate) fn classify(
    planner_output: &crate::planner::PlannerDecision,
    lane_status: crate::envelope::LaneStatus,
    census_result: &CensusResult,
    ledger: &LedgerClosure,
    mandatory_missing: u64,
) -> CompletenessClass {
    use crate::envelope::LaneStatus;
    use crate::planner::PlannerDecision;

    if !ledger.is_closed() {
        CompletenessClass::CannotEstablish {
            reason: CannotEstablishReason::LedgerNotClosed,
        }
    } else if !census_result.is_ok() {
        CompletenessClass::CannotEstablish {
            reason: CannotEstablishReason::CensusFailed,
        }
    } else if lane_status == LaneStatus::Failed {
        CompletenessClass::CannotEstablish {
            reason: CannotEstablishReason::LaneFailed,
        }
    } else if mandatory_missing > 0 {
        // card 22c review debt: an unmet Mandatory obligation moves `completeness`, it is not
        // just a count in the handoff. Placed **before** the planner leg on purpose — the
        // planner's own verdict is about the predicate, and a sound predicate over a short
        // Mandatory context is still an answer nobody can establish.
        CompletenessClass::CannotEstablish {
            reason: CannotEstablishReason::MandatoryNotSatisfied,
        }
    } else {
        match planner_output {
            PlannerDecision::CannotEstablish => CompletenessClass::CannotEstablish {
                reason: CannotEstablishReason::PredicateNotEnumerable,
            },
            PlannerDecision::DirectGet(_) | PlannerDecision::Enumerate { .. } => {
                CompletenessClass::Exact
            }
            PlannerDecision::Class(_) => CompletenessClass::SemanticBounded,
        }
    }
}

/// §80.1 G80-6 crate-external witness door (adjudication 3 above). `classify()` itself stays
/// `pub(crate)` — this is not a second constructor, it calls `classify()` verbatim and hands
/// back only the wire label pair the real call produced, never the `CompletenessClass` value
/// (which a `pub` fn could not name outside this crate regardless). It is deliberately
/// label-only: a component classifier has no provenance/pipeline/context inputs and must not
/// increment the final result metric.
pub fn classify_for_witness(
    planner_output: &crate::planner::PlannerDecision,
    lane_status: crate::envelope::LaneStatus,
    census_result: &CensusResult,
    ledger: &LedgerClosure,
    mandatory_missing: u64,
) -> (&'static str, &'static str) {
    let class = classify(
        planner_output,
        lane_status,
        census_result,
        ledger,
        mandatory_missing,
    );
    class.wire_labels()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **每个 reason label 都必须在指标格子里有位置。**
    ///
    /// `CompletenessTotal` 的 `AtomicU64` 数组是**手写展开**的（`AtomicU64` 不是 `Copy`，
    /// `[X; N]` 重复表达式要一个 `const ZERO`，那会触 clippy 的
    /// `declare_interior_mutable_const`）。所以 `REASONS` 加一个变体而数组没跟着加一格时，
    /// `idx()` 的 `expect` 会在**运行时**炸——而且只在那个 reason 真被记一次的时候才炸，
    /// 也就是最不该炸的时候。本条把它提前到编译-测试期。
    ///
    /// 顺带钉住 label 的唯一来源：格子的键取自 `wire_labels()`，与 JSON 同源，
    /// 所以「指标 label 与 JSON 漂移」也在这里被拦。
    #[test]
    fn every_class_reason_pair_has_a_metric_cell() {
        let mut seen = std::collections::BTreeSet::new();
        for class in [
            CompletenessClass::Exact,
            CompletenessClass::FacetComplete,
            CompletenessClass::SemanticBounded,
        ] {
            let (c, r) = class.wire_labels();
            let idx = CompletenessTotal::idx(c, r);
            assert!(
                idx < CompletenessTotal::CELLS,
                "{c}/{r} 的下标 {idx} 越界（CELLS={})",
                CompletenessTotal::CELLS
            );
            seen.insert(idx);
        }
        // 十一个 CannotEstablish reason 逐个走一遍。少一个变体这里就少一个 idx，
        // 而 REASONS 与数组长度对不上时 `idx()` 会直接 panic。
        for reason in [
            CannotEstablishReason::LedgerNotClosed,
            CannotEstablishReason::PredicateNotEnumerable,
            CannotEstablishReason::CensusFailed,
            CannotEstablishReason::LaneFailed,
            CannotEstablishReason::IndexCountUnavailable,
            CannotEstablishReason::A2OvershootBeyondPending,
            CannotEstablishReason::MandatoryContextOverflow,
            CannotEstablishReason::CountUnknown,
            CannotEstablishReason::CountScopeMismatch,
            CannotEstablishReason::PipelineCountMismatch,
            CannotEstablishReason::MandatoryNotSatisfied,
        ] {
            let (c, r) = CompletenessClass::CannotEstablish { reason }.wire_labels();
            let idx = CompletenessTotal::idx(c, r);
            assert!(
                idx < CompletenessTotal::CELLS,
                "{c}/{r} 的下标 {idx} 越界（CELLS={}）——REASONS 加了变体但手写数组没跟着加格",
                CompletenessTotal::CELLS
            );
            seen.insert(idx);
        }
        assert_eq!(
            seen.len(),
            14,
            "十四个 (class, reason) 组合应当落在十四个不同的格子里"
        );
    }

    /// 数组长度必须等于 `CELLS`。手写展开漏加一格时，上面那条要等到那个 reason 真被用到
    /// 才炸；这条在任何一次 `cargo test` 里都炸。
    #[test]
    fn the_hand_written_cell_array_matches_cells() {
        let t = CompletenessTotal::new();
        assert_eq!(
            t.0.len(),
            CompletenessTotal::CELLS,
            "手写的 AtomicU64 数组长度与 CLASSES × REASONS 对不上"
        );
    }
    use crate::envelope::LaneStatus;
    use crate::planner::{DirectGetLocator, PlannerDecision, QueryClass};

    #[test]
    fn completeness_class_and_freshness_class_have_four_variants_each() {
        fn assert_completeness_exhaustive(c: CompletenessClass) {
            match c {
                CompletenessClass::Exact
                | CompletenessClass::FacetComplete
                | CompletenessClass::SemanticBounded
                | CompletenessClass::CannotEstablish { reason: _ } => {}
            }
        }
        fn assert_freshness_exhaustive(f: FreshnessClass) {
            match f {
                FreshnessClass::Fresh
                | FreshnessClass::Aging
                | FreshnessClass::Stale
                | FreshnessClass::Unknown => {}
            }
        }
        assert_completeness_exhaustive(CompletenessClass::Exact);
        assert_completeness_exhaustive(CompletenessClass::FacetComplete);
        assert_freshness_exhaustive(FreshnessClass::Fresh);
    }

    fn closed_ledger() -> LedgerClosure {
        ledger::close(ledger::LedgerReads {
            expected: 10,
            done: 10,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        })
    }

    fn broken_ledger() -> LedgerClosure {
        ledger::close(ledger::LedgerReads {
            expected: 10,
            done: 5,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0, // 5 != 10
        })
    }

    /// §22.5's frozen first branch: a broken ledger wins even when everything else (census,
    /// lane, planner) looks fine — read *before* `planner_output`.
    #[test]
    fn broken_ledger_yields_cannot_establish_ledger_not_closed_ahead_of_everything_else() {
        let class = classify(
            &PlannerDecision::Enumerate {
                predicate_id: "rejected_decisions_v1".to_string(),
            },
            LaneStatus::Ok,
            &CensusResult::ok_without_enumeration(),
            &broken_ledger(),
            0,
        );
        assert_eq!(
            class,
            CompletenessClass::CannotEstablish {
                reason: CannotEstablishReason::LedgerNotClosed
            }
        );
    }

    #[test]
    fn census_failure_yields_cannot_establish_census_failed() {
        let class = classify(
            &PlannerDecision::Class(QueryClass::Semantic),
            LaneStatus::Ok,
            &CensusResult::failed(),
            &closed_ledger(),
            0,
        );
        assert_eq!(
            class,
            CompletenessClass::CannotEstablish {
                reason: CannotEstablishReason::CensusFailed
            }
        );
    }

    #[test]
    fn failed_lane_yields_cannot_establish_lane_failed() {
        let class = classify(
            &PlannerDecision::Class(QueryClass::Semantic),
            LaneStatus::Failed,
            &CensusResult::ok_without_enumeration(),
            &closed_ledger(),
            0,
        );
        assert_eq!(
            class,
            CompletenessClass::CannotEstablish {
                reason: CannotEstablishReason::LaneFailed
            }
        );
    }

    /// card 22c review debt: an unmet Mandatory obligation moves `completeness`, and it is
    /// read **before** the planner leg — a planner verdict that would otherwise have said
    /// `exact` or `semantic_bounded` does not get to speak over a short Mandatory context.
    ///
    /// Fault injection: delete the `mandatory_missing > 0` branch in `classify()` and both
    /// halves go red (the `Enumerate` case falls back to `Exact`, the `Class` case to
    /// `SemanticBounded`) — which is exactly the silent shape this branch exists to stop.
    #[test]
    fn unmet_mandatory_obligation_yields_cannot_establish_ahead_of_the_planner_leg() {
        for decision in [
            PlannerDecision::Enumerate {
                predicate_id: "rejected_decisions_v1".to_string(),
            },
            PlannerDecision::Class(QueryClass::Semantic),
        ] {
            let class = classify(
                &decision,
                LaneStatus::Ok,
                &CensusResult::ok_without_enumeration(),
                &closed_ledger(),
                1,
            );
            assert_eq!(
                class,
                CompletenessClass::CannotEstablish {
                    reason: CannotEstablishReason::MandatoryNotSatisfied
                },
                "{decision:?} with mandatory_missing=1 must not claim an establishable answer"
            );
            assert_eq!(
                class.wire_labels(),
                ("cannot_establish", "mandatory_not_satisfied")
            );
        }
        // The four §22.4 triggers still win: they describe a broken run, this one a short one.
        assert_eq!(
            classify(
                &PlannerDecision::Class(QueryClass::Semantic),
                LaneStatus::Failed,
                &CensusResult::ok_without_enumeration(),
                &closed_ledger(),
                1,
            ),
            CompletenessClass::CannotEstablish {
                reason: CannotEstablishReason::LaneFailed
            }
        );
    }

    /// §20.2/§22.5 frozen: Planner's own `CannotEstablish` is never re-classified as
    /// `SemanticBounded`.
    #[test]
    fn planner_cannot_establish_stays_cannot_establish_not_downgraded_to_semantic() {
        let class = classify(
            &PlannerDecision::CannotEstablish,
            LaneStatus::Ok,
            &CensusResult::ok_without_enumeration(),
            &closed_ledger(),
            0,
        );
        assert_eq!(
            class,
            CompletenessClass::CannotEstablish {
                reason: CannotEstablishReason::PredicateNotEnumerable
            }
        );
    }

    #[test]
    fn enumerate_decision_yields_exact() {
        let class = classify(
            &PlannerDecision::Enumerate {
                predicate_id: "rejected_decisions_v1".to_string(),
            },
            LaneStatus::Ok,
            &CensusResult::ok_without_enumeration(),
            &closed_ledger(),
            0,
        );
        assert_eq!(class, CompletenessClass::Exact);
    }

    #[test]
    fn direct_get_decision_yields_exact() {
        let class = classify(
            &PlannerDecision::DirectGet(DirectGetLocator::MemoryId(
                "0000-0000-0000-0000-000000000000".to_string(),
            )),
            LaneStatus::Ok,
            &CensusResult::ok_without_enumeration(),
            &closed_ledger(),
            0,
        );
        assert_eq!(class, CompletenessClass::Exact);
    }

    /// EXACT's error path in the type only has `CannotEstablish` — the enum has no
    /// `Exact -> SemanticBounded` transition to write by mistake (§22.5).
    #[test]
    fn class_decision_falls_back_to_semantic_bounded_never_facet_complete() {
        let class = classify(
            &PlannerDecision::Class(QueryClass::State),
            LaneStatus::Ok,
            &CensusResult::ok_without_enumeration(),
            &closed_ledger(),
            0,
        );
        assert_eq!(class, CompletenessClass::SemanticBounded);
    }

    /// The public component witness is label-only; final result metrics require the full
    /// Envelope path, which checks provenance, pipeline/A2, and mandatory context first.
    #[test]
    fn classify_for_witness_is_label_only() {
        assert!(take_final_record_trace().is_empty());
        let census = CensusResult::enumerated(
            ExactEnumeration::new("rejected_decisions_v1", 1, 1, 0).unwrap(),
        );
        let (class_label, reason_label) = classify_for_witness(
            &PlannerDecision::Enumerate {
                predicate_id: "rejected_decisions_v1".to_string(),
            },
            LaneStatus::Ok,
            &census,
            &closed_ledger(),
            0,
        );
        assert_eq!((class_label, reason_label), ("exact", "none"));
        assert!(take_final_record_trace().is_empty());
    }

    /// §22.1 derivation: `coverage` and `truncated` are read off the constructor's inputs —
    /// secret exclusion deducts coverage *without* setting `truncated` (those rows are named
    /// by `excluded_secret`, not silently dropped), while a caller-side cap does set it.
    #[test]
    fn exact_enumeration_derives_coverage_and_truncated() {
        // Full enumeration — §22.1's own example numbers.
        let full = ExactEnumeration::new("rejected_decisions_v1", 17, 17, 0).unwrap();
        assert!((full.coverage() - 1.0).abs() < f64::EPSILON);
        assert!(!full.truncated());

        // Secret exclusion: 15 returned + 2 excluded == 17 accounted ⇒ coverage deducted,
        // truncated stays false.
        let secret = ExactEnumeration::new("rejected_decisions_v1", 17, 15, 2).unwrap();
        assert!((secret.coverage() - 15.0 / 17.0).abs() < f64::EPSILON);
        assert!(!secret.truncated());
        assert_eq!(secret.excluded_secret(), 2);

        // Caller cap: 10 returned + 0 excluded < 17 ⇒ truncated.
        let capped = ExactEnumeration::new("rejected_decisions_v1", 17, 10, 0).unwrap();
        assert!(capped.truncated());
    }

    /// §22.1 fault, executable: "禁止用召回条数冒充 `total` —— 那是分母内生" — a readout
    /// claiming more returned+excluded rows than its own denominator cannot be minted.
    #[test]
    fn fault_recall_count_cannot_overrun_the_denominator() {
        let err = ExactEnumeration::new("rejected_decisions_v1", 10, 11, 0).unwrap_err();
        assert!(
            err.0.contains("分母内生"),
            "error must name the forbidden shape: {err}"
        );
        assert!(ExactEnumeration::new("rejected_decisions_v1", 10, 9, 2).is_err());
    }

    /// §22.0 fault, executable at the *minting* layer: an enumeration with a blank
    /// `predicate_id` could only ever surface as `class=exact` + `predicate_id=null` — the
    /// frozen invariant violation — so it is unconstructible.
    #[test]
    fn fault_blank_predicate_id_is_unmintable() {
        assert!(ExactEnumeration::new("", 3, 3, 0).is_err());
        assert!(ExactEnumeration::new("   ", 3, 3, 0).is_err());
    }

    /// §22.1 edge: an empty universe (`total == 0`) is vacuously complete, not 0/0 = NaN.
    #[test]
    fn empty_universe_is_vacuously_complete() {
        let e = ExactEnumeration::new("rejected_decisions_v1", 0, 0, 0).unwrap();
        assert!((e.coverage() - 1.0).abs() < f64::EPSILON);
        assert!(!e.truncated());
    }

    /// The placeholder `pub ok: bool` let any caller claim a passing census bare-handed; the
    /// closed constructor set pins ok-ness to what was actually handed over.
    #[test]
    fn census_result_constructors_pin_ok_to_content() {
        let e = ExactEnumeration::new("rejected_decisions_v1", 2, 2, 0).unwrap();
        let enumerated = CensusResult::enumerated(e);
        assert!(enumerated.is_ok());
        assert_eq!(enumerated.enumeration().unwrap().total(), 2);
        assert!(CensusResult::ok_without_enumeration().is_ok());
        assert!(
            CensusResult::ok_without_enumeration()
                .enumeration()
                .is_none()
        );
        assert!(!CensusResult::failed().is_ok());
        assert!(CensusResult::failed().enumeration().is_none());
    }
}
