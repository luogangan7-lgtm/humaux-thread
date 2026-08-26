//! `retrieval::completeness` — `LedgerCounts` / `CompletenessClass` / `FreshnessClass` / the
//! `ledger::close` and `classify()` sole constructors (§22.4 / §22.5 / §59).
//!
//! `classify()` is the sole constructor of [`CompletenessClass`] and the single increment
//! point of `retrieval_completeness_total{class,reason}` (§22.5, §41.2 registry row). Its
//! four parameters are the frozen signature (`planner_output` / `lane_status` /
//! `census_result` / `ledger`); this module reuses [`crate::planner::PlannerDecision`] for
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
//!    §80.1 G80-6's witness harness runs from `crates/testkit` — a different crate — and can
//!    only call `pub` entry points. [`classify_for_witness`] is the resolution: it calls
//!    `classify()` verbatim and returns only the wire label pair the counter itself used,
//!    never the `CompletenessClass` value (a `pub` fn cannot name a `pub(crate)` return type
//!    anyway) — so the "sole constructor" privacy invariant (nothing outside this module ever
//!    holds a `CompletenessClass`) still holds, while the real production emit path stays
//!    genuinely externally triggerable.
//! 4. `PlannerDecision::Class(_)` (every §20 wire class except `DirectGet`/`Enumerate`,
//!    including `State` — §22.2 FACET_COMPLETE's natural source) maps to `SemanticBounded`,
//!    not `FacetComplete`: the frozen 4-param signature carries no `covered_facets` /
//!    `required_facets` count, so claiming `FacetComplete` here would be an unproven
//!    completeness claim (§22.0's "no verbal EXACT claim" invariant, generalized).
//!    `FacetComplete` stays a real, constructible variant (exercised directly by this file's
//!    own exhaustiveness test) — just not reachable through `classify()` until a later wave
//!    threads facet-coverage counts into the signature.
// ponytail: adjudication 4's gap — `classify()` never emits `FacetComplete` today. Upgrade
// path: add a facet-coverage input (covered/required counts) to `classify()`'s signature and
// give `PlannerDecision::Class(QueryClass::State)` its own arm once that lands.

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
    fn wire_labels(self) -> (&'static str, &'static str) {
        match self {
            Self::Exact => ("exact", "none"),
            Self::FacetComplete => ("facet_complete", "none"),
            Self::SemanticBounded => ("semantic_bounded", "none"),
            Self::CannotEstablish { reason } => ("cannot_establish", reason.label()),
        }
    }
}

/// §22.4 `CANNOT_ESTABLISH` reason, a closed set (§78.2: no stringly-typed domain) covering
/// exactly what `classify()`'s frozen 4-param signature can observe: A1 broken, Planner's own
/// non-enumerable verdict, census failure, and lane failure. `projection lag` (also named in
/// §22.4's trigger list) has no input path into this signature and so is not a variant here —
/// out of scope for this constructor, not silently folded into one of these four.
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
    /// side cannot-establish `classify()`'s own 4-param signature (frozen, no `visible` input)
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
        }
    }
}

/// Minimal readout `classify()` needs from Authority census (§22.4 trigger 4: "Authority
/// census 本身失败或当前授权策略明确排除了一部分权威行且无法定义可枚举的 authorized
/// universe"). Not the real census pipeline — that lives with `domain::authority`/`adapters`
/// once wired; this is only the one bit the frozen 4-param signature consumes. `pub`:
/// `adapters` is this type's eventual constructor, same crate-boundary reasoning as
/// `ledger::LedgerReads` below, and the G80-6 witness (adjudication 3) must be able to build
/// one from `crates/testkit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CensusResult {
    pub ok: bool,
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
        /// `count(state IN ('DONE','SKIPPED_BY_POLICY','TOMBSTONED'))` — the §15.2 SETTLED_OK
        /// union, not decomposed (§23.3 worked example: `done 95 = 90 DONE + 3 TOMBSTONED + 2
        /// SKIPPED_BY_POLICY`).
        pub done: u64,
        /// `count(state = 'TOMBSTONED')` — a subset already counted inside `done` above, also
        /// reported standalone: §23.1②'s A2 and the `visible` overlay both need it on its own.
        pub deleted: u64,
        /// `count(state = 'SKIPPED_BY_POLICY')` — subset of `done`, reported standalone so A2
        /// can credit it on its own left-hand term without it inflating the denominator
        /// (§23.1②: "计入 A2 左边、不进分母").
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
    /// point (§22.5) lives in `classify()` (this module's parent), not here.
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
    const REASONS: [&'static str; 7] = [
        "none",
        "ledger_not_closed",
        "predicate_not_enumerable",
        "census_failed",
        "lane_failed",
        "index_count_unavailable",
        "a2_overshoot_beyond_pending",
    ];
    const CELLS: usize = Self::CLASSES.len() * Self::REASONS.len();

    // 28 cells, hand-written: `AtomicU64` isn't `Copy`, and a `[X; N]` repeat expression
    // needs a `const ZERO`, which trips `clippy::declare_interior_mutable_const` (same
    // rationale `DegradeTotal::new` already documents). Update by hand if `CLASSES` or
    // `REASONS` ever grows.
    const fn new() -> Self {
        Self([
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
        ])
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

// ============================================================================
// §22.5 — `classify()`: the sole constructor of `CompletenessClass`.
// ============================================================================

/// §22.5's sole constructor of [`CompletenessClass`] and single increment point of
/// `retrieval_completeness_total{class,reason}` (§41.2 registry row, §80.1 G80-6).
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
) -> CompletenessClass {
    use crate::envelope::LaneStatus;
    use crate::planner::PlannerDecision;

    let class = if !ledger.is_closed() {
        CompletenessClass::CannotEstablish {
            reason: CannotEstablishReason::LedgerNotClosed,
        }
    } else if !census_result.ok {
        CompletenessClass::CannotEstablish {
            reason: CannotEstablishReason::CensusFailed,
        }
    } else if lane_status == LaneStatus::Failed {
        CompletenessClass::CannotEstablish {
            reason: CannotEstablishReason::LaneFailed,
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
    };

    let (class_label, reason_label) = class.wire_labels();
    // labels: class,reason
    RETRIEVAL_COMPLETENESS_TOTAL.inc(class_label, reason_label);

    class
}

/// §80.1 G80-6 crate-external witness door (adjudication 3 above). `classify()` itself stays
/// `pub(crate)` — this is not a second constructor, it calls `classify()` verbatim and hands
/// back only the wire label pair the real call produced, never the `CompletenessClass` value
/// (which a `pub` fn could not name outside this crate regardless). Exists solely so
/// `crates/testkit`'s metrics witness can exercise the genuine production emit path.
pub fn classify_for_witness(
    planner_output: &crate::planner::PlannerDecision,
    lane_status: crate::envelope::LaneStatus,
    census_result: &CensusResult,
    ledger: &LedgerClosure,
) -> (&'static str, &'static str) {
    classify(planner_output, lane_status, census_result, ledger).wire_labels()
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let before = retrieval_completeness_total_count("cannot_establish", "ledger_not_closed");
        let class = classify(
            &PlannerDecision::Enumerate {
                predicate_id: "rejected_decisions_v1".to_string(),
            },
            LaneStatus::Ok,
            &CensusResult { ok: true },
            &broken_ledger(),
        );
        assert_eq!(
            class,
            CompletenessClass::CannotEstablish {
                reason: CannotEstablishReason::LedgerNotClosed
            }
        );
        assert_eq!(
            retrieval_completeness_total_count("cannot_establish", "ledger_not_closed"),
            before + 1
        );
    }

    #[test]
    fn census_failure_yields_cannot_establish_census_failed() {
        let class = classify(
            &PlannerDecision::Class(QueryClass::Semantic),
            LaneStatus::Ok,
            &CensusResult { ok: false },
            &closed_ledger(),
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
            &CensusResult { ok: true },
            &closed_ledger(),
        );
        assert_eq!(
            class,
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
            &CensusResult { ok: true },
            &closed_ledger(),
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
            &CensusResult { ok: true },
            &closed_ledger(),
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
            &CensusResult { ok: true },
            &closed_ledger(),
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
            &CensusResult { ok: true },
            &closed_ledger(),
        );
        assert_eq!(class, CompletenessClass::SemanticBounded);
    }

    /// Adjudication 3: the witness door returns labels only, and drives the same counter as
    /// direct `classify()` calls.
    #[test]
    fn classify_for_witness_drives_the_real_counter() {
        let before = retrieval_completeness_total_count("exact", "none");
        let (class_label, reason_label) = classify_for_witness(
            &PlannerDecision::Enumerate {
                predicate_id: "rejected_decisions_v1".to_string(),
            },
            LaneStatus::Ok,
            &CensusResult { ok: true },
            &closed_ledger(),
        );
        assert_eq!((class_label, reason_label), ("exact", "none"));
        assert_eq!(
            retrieval_completeness_total_count("exact", "none"),
            before + 1
        );
    }
}
