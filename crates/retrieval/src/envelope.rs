//! `retrieval::envelope` — §23 Recall Result Envelope.
//! Depends-on: crates=[humaux-domain, humaux-telemetry, serde, serde_json]; services=[];
//!   env=[]; modules=[domain::context, domain::error, domain::grounding, retrieval::compiler, retrieval::completeness, retrieval::planner, retrieval::request, telemetry::degrade]
//! Called-by: [adapters::context_repo, adapters::retrieve, gateway::context, gateway::memory, gateway::recall, retrieval::completeness, retrieval::signals, tests]
//! Invariants: [A2 compares the Qdrant point count with the ledger's ProjectionReads points, never with ticket
//!   counts (ADR-0057 D-A); build_projection_block is the only place completeness_ratio / current are computed
//!   and the only producer of PROJECTION_INVISIBLE_LOSS / PROJECTION_LAG, composed loss-then-lag (ADR-0057 D-E)]
//! Spec: §23; §23.1; ADR-0057
//!
//! Assembles the five blocks
//! (`pipeline` / `completeness` / `provenance` / `freshness` / `grounding`) and, most
//! load-bearing, the §23.1② A1/A2 arithmetic that turns a
//! [`LedgerClosure`](crate::completeness) plus an independently-read Qdrant `visible` count
//! into `completeness_ratio` / `current` / `PROJECTION_INVISIBLE_LOSS`.
//!
//! Pure, no IO — same "adapters does the round trips, this crate only judges" split as
//! [`crate::completeness::ledger`]. §23.1②'s "取数顺序固定：先读索引 count，后读账本快照" is a
//! constraint on the *caller* that reads `visible` and builds the `LedgerClosure` before
//! calling [`build_projection_block`]; this module has no IO to order.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::completeness::{
    CannotEstablishReason, CensusResult, CompletenessClass, FreshnessClass, LedgerClosure, classify,
};
use humaux_domain::context::MandatoryOverflow;

use crate::compiler::ContextOutcome;
use crate::planner::{PlannerDecision, QueryClass};
use crate::request::{ProfileFingerprint, RetrievalRequest};
use humaux_domain::grounding::{GroundingState, GroundingStateKind};
use humaux_telemetry::degrade::{DegradeCode, Outcome, abstain};

// ============================================================================
// §23.1① `pipeline.evidence`
// ============================================================================

/// §23.1① `expected_source` — exactly two values, mutually exclusive and exhaustive. `census`
/// is not a third value (this chapter overrides §1.2.2/§15.6's older "batch import ⇒ census
/// row count" language).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedSource {
    Ticket,
    None,
}

/// The universe a pipeline block's counts describe. Projection is always the StreamLedger
/// universe because it is derived from the fixed stream ledger snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CountScope {
    AuthorizedView,
    StreamLedger,
}

/// §23.3 `pipeline.evidence` block.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EvidenceBlock {
    /// `null` iff `expected_source == None` (§23.1①: "恒等值就是装饰列，不许输出数字充数").
    pub expected: Option<u64>,
    pub expected_source: ExpectedSource,
    /// `null` means the caller cannot establish a count in this block's declared universe.
    pub persisted: Option<u64>,
    pub count_scope: CountScope,
}

impl EvidenceBlock {
    /// The call carried a `batch_id` whose `begin_batch` transaction A already committed —
    /// `expected` is that batch's ticket count (§23.1①, "一经发放不可回缩").
    pub fn ticket(expected: u64, persisted: Option<u64>, count_scope: CountScope) -> Self {
        Self {
            expected: Some(expected),
            expected_source: ExpectedSource::Ticket,
            persisted,
            count_scope,
        }
    }

    /// The call carried no `batch_id` — `expected` must be `null`, never backfilled from
    /// `persisted` (§23.1①).
    pub fn no_batch(persisted: Option<u64>, count_scope: CountScope) -> Self {
        Self {
            expected: None,
            expected_source: ExpectedSource::None,
            persisted,
            count_scope,
        }
    }
}

// ============================================================================
// `pipeline.knowledge`
// ============================================================================

/// §23.3 `pipeline.knowledge` block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct KnowledgeBlock {
    pub eligible: Option<u64>,
    pub processed: Option<u64>,
    pub waiting_key: Option<u64>,
    pub failed: Option<u64>,
    pub count_scope: CountScope,
}

// ============================================================================
// §23.1② `pipeline.projection` — A1/A2 closure
// ============================================================================

/// §23.1② A2 (可见闭合) outcome — directional and three-way, never a bare bool: the `<` and
/// `>` sides get opposite treatment (real loss vs. harmless in-flight write), and the `>` side
/// itself splits again at `points_in_flight` (in-flight vs. untrustworthy).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum A2Closure {
    /// `points_settled <= visible <= points_settled + points_unsettled`.
    Closed,
    /// `visible < points_settled` — the index lacks a point the ledger says must be visible: a
    /// real loss.
    InvisibleLoss,
    /// Above the closed band but within `points_in_flight` — a normal in-flight write (§17.4:
    /// point becomes search-visible before its `stream_log` row settles), not a loss.
    InFlight,
    /// Beyond `points_settled + points_unsettled + points_in_flight` — the index holds points
    /// no ticket accounts for; neither side is trustworthy (§23.1②: "同 A1 判 cannot_establish").
    Inconsistent,
}

// §23.1② (ADR-0057 D-A): both sides count memory points; tickets stay A1's unit. Unsettled
// memories (latest ticket FAILED/LOST/RETIRED_FAILED) hold 0 or 1 point, so they widen the
// closed band instead of reading as in-flight (known limit 12: the slack can absorb one loss each).
fn judge_a2(visible: u64, settled: u64, in_flight: u64, unsettled: u64) -> A2Closure {
    if visible < settled {
        A2Closure::InvisibleLoss
    } else if visible - settled <= unsettled {
        A2Closure::Closed
    } else if visible - settled - unsettled <= in_flight {
        A2Closure::InFlight
    } else {
        A2Closure::Inconsistent
    }
}

/// §23.3 `pipeline.projection` block.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct ProjectionBlock {
    pub expected: u64,
    pub done: u64,
    pub deleted: u64,
    pub skipped: u64,
    /// `null` when the Qdrant count could not be taken — never backfilled from
    /// `done - deleted` (§23.1②).
    pub visible: Option<u64>,
    pub open_gaps: u64,
    pub pending: u64,
    /// ADR-0057 D-A: the A2 point readings `visible` is judged against (see
    /// [`crate::completeness::ledger::ProjectionReads`]); the six ticket fields above stay A1's.
    /// The universe that should eventually be visible — the ratio's denominator.
    pub points_expected: u64,
    /// Must be visible now — A2's lower bound.
    pub points_settled: u64,
    /// Memories with a pending ticket — the in-flight slack above the closed band.
    pub points_in_flight: u64,
    /// Memories whose latest ticket failed or was retired — the closed band's width.
    pub points_unsettled: u64,
    /// `null` whenever it cannot be established: A1 broken, `visible` unavailable, or A2's
    /// `>` side exceeds the in-flight slack — all three are the *same* "don't know the true
    /// count" failure (§23.1②: "把 A2 的 `<` 侧也判成 cannot_establish 等于把真实的丢失藏进测不出来
    /// 里"，其反面同样成立). Otherwise `visible / points_expected` (ADR-0057 D-A: one unit on both
    /// sides). Only one ratio is ever output — there is no second "ledger-only" ratio (§23.1②).
    pub completeness_ratio: Option<f64>,
    /// §23.1② frozen definition, this crate's only computation of it:
    /// `current = (open_gaps == 0) && A2 闭合`. `pending` never enters this — see this
    /// function's doc.
    pub current: bool,
}

/// §23.1②'s full A1/A2 assembly: the one place `completeness_ratio` / `current` /
/// `PROJECTION_INVISIBLE_LOSS` / `PROJECTION_LAG` are computed from a [`LedgerClosure`] and an
/// independently-read Qdrant `visible` count (`None` when the index count could not be
/// taken). Returns the block plus whatever `abstain()` degradations fired — at most
/// `[ProjectionInvisibleLoss, ProjectionLag]`, in that order, and only via `abstain()` (§53.1
/// single exit point; this function never builds `Outcome { degradations: ... }` by hand).
///
/// §52.2 (ADR-0057 D-E): a result read while [`LedgerClosure::lagging`] holds is a lagged
/// result, so it degrades `PROJECTION_LAG` on every path — `current` is untouched (its formula
/// is frozen); the class side is `classify()`'s lag arm.
pub fn build_projection_block(
    ledger: &LedgerClosure,
    visible: Option<u64>,
    lag_threshold: Duration,
) -> Outcome<ProjectionBlock> {
    let out = projection_block_a2(ledger, visible);
    // ADR-0057 D-E: compose, never early-return — loss and lag can both hold (a stalled runner
    // plus a lost point) and each must reach the wire.
    if ledger.lagging(lag_threshold) {
        out.also(DegradeCode::ProjectionLag)
    } else {
        out
    }
}

/// [`build_projection_block`]'s A1/A2 half (no lag judgement): also the block of the
/// no-serving-projection envelope, whose class is already `cannot_establish` and whose index
/// face does not exist to lag behind.
fn projection_block_a2(ledger: &LedgerClosure, visible: Option<u64>) -> Outcome<ProjectionBlock> {
    let counts = ledger.counts();
    let points = ledger.points();
    let no_ratio = |visible: Option<u64>| ProjectionBlock {
        expected: counts.expected(),
        done: counts.done(),
        deleted: counts.deleted(),
        skipped: counts.skipped(),
        visible,
        open_gaps: counts.open_gaps(),
        pending: counts.pending(),
        points_expected: points.points_expected,
        points_settled: points.points_settled,
        points_in_flight: points.points_in_flight,
        points_unsettled: points.points_unsettled,
        completeness_ratio: None,
        current: false,
    };

    // §22.4: A1 broken ⇒ cannot_establish, no ratio at all, whatever `visible` says.
    if !ledger.is_closed() {
        return Outcome::clean(no_ratio(visible));
    }

    // §23.1②: index count unavailable ⇒ `visible: null` + cannot_establish, never backfilled.
    let Some(v) = visible else {
        return Outcome::clean(no_ratio(None));
    };

    let a2 = judge_a2(
        v,
        points.points_settled,
        points.points_in_flight,
        points.points_unsettled,
    );

    // §23.1②: `>` side beyond the in-flight slack is untrustworthy on both sides — same
    // treatment as A1 broken, no ratio.
    if a2 == A2Closure::Inconsistent {
        return Outcome::clean(no_ratio(Some(v)));
    }

    // A vacuous universe (`points_expected == 0`, e.g. every memory was tombstoned) is none of
    // the three named cannot-establish cases: A1 and A2 both hold trivially. Pinned as `1.0` —
    // "0 of a 0-point universe" is complete by definition — rather than `null`, so this branch
    // cannot silently drift back to the undefined `ratio: null, current: true` shape.
    let denom = points.points_expected;
    let ratio = Some(if denom == 0 {
        1.0
    } else {
        v as f64 / denom as f64
    });
    let block = ProjectionBlock {
        completeness_ratio: ratio,
        current: counts.open_gaps() == 0 && a2 == A2Closure::Closed,
        ..no_ratio(Some(v))
    };

    if a2 == A2Closure::InvisibleLoss {
        abstain(DegradeCode::ProjectionInvisibleLoss, block)
    } else {
        Outcome::clean(block)
    }
}

/// §22.4's `classify()`/[`build_projection_block`] coupling, made mechanical (major finding:
/// "no type, no gate, no assembly function prevents an envelope with `visible: null,
/// completeness_ratio: null, class: semantic_bounded`"). `classify()`'s
/// signature has no `visible`/A2 input (§22.5's own module doc), so it cannot see the two
/// projection-side cannot-establish triggers on its own; this function is the one place that
/// downgrades its answer using the *already-computed* [`ProjectionBlock`] instead of leaving
/// that downgrade to caller discipline. `classify()`'s own triggers (ledger/census/
/// lane/planner) still take priority when they already produced `CannotEstablish` — this only
/// ever adds a reason, never removes one.
// ponytail: no `application`-layer caller assembles a full envelope yet (out of this crate's
// scope, see this module's own top-of-file doc) — today's only callers are this file's own
// `#[cfg(test)]` tests, so a plain (non-test) build sees this as dead code. `allow(dead_code)`
// until a real caller lands; remove then.
#[allow(dead_code)]
pub(crate) fn assemble_completeness_class(
    planner_output: &crate::planner::PlannerDecision,
    lane_status: LaneStatus,
    census_result: &CensusResult,
    ledger: &LedgerClosure,
    visible: Option<u64>,
    pipeline: &PipelineBlock,
    lag_threshold: Duration,
) -> (CompletenessClassWire, Option<CannotEstablishReasonWire>) {
    let class = final_completeness_class(
        planner_output,
        lane_status,
        census_result,
        ledger,
        pipeline,
        visible,
        None,
        0,
        lag_threshold,
    );

    (class.into(), CannotEstablishReasonWire::from_class(class))
}

#[allow(clippy::too_many_arguments)] // every input is a distinct §22.4/§23.1② trigger source
fn final_completeness_class(
    planner_output: &crate::planner::PlannerDecision,
    lane_status: LaneStatus,
    census_result: &CensusResult,
    ledger: &LedgerClosure,
    pipeline: &PipelineBlock,
    visible: Option<u64>,
    context: Option<&ContextOutcome>,
    mandatory_missing: u64,
    lag_threshold: Duration,
) -> CompletenessClass {
    let classified = classify(
        planner_output,
        lane_status,
        census_result,
        ledger,
        mandatory_missing,
        lag_threshold,
    );
    // §25.5's final outcome makes a real Mandatory Context overflow the canonical reason even
    // when §22's pure classifier has already found a different failure. The `PipelineBlock` is
    // still retained by the caller as the diagnostic record; only the one final wire reason is
    // prioritized here.
    if let Some(ContextOutcome::Overflow(overflow)) = context {
        return crate::completeness::overflow_class(overflow);
    }
    if matches!(classified, CompletenessClass::CannotEstablish { .. }) {
        return classified;
    }
    if pipeline.projection.completeness_ratio.is_none() {
        let reason = if !ledger.is_closed() {
            CannotEstablishReason::LedgerNotClosed
        } else if visible.is_none() {
            CannotEstablishReason::IndexCountUnavailable
        } else {
            CannotEstablishReason::A2OvershootBeyondPending
        };
        return CompletenessClass::CannotEstablish { reason };
    }
    if let Some(reason) = pipeline.count_inconsistency_reason() {
        return CompletenessClass::CannotEstablish { reason };
    }
    classified
}

// ============================================================================
// `pipeline` (evidence + knowledge + projection)
// ============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct PipelineBlock {
    pub evidence: EvidenceBlock,
    pub knowledge: KnowledgeBlock,
    pub projection: ProjectionBlock,
}

/// Single pipeline-count admission authority. It never mutates stream state: unknown or
/// cross-scope values are epistemic limits, not FAILED/LOST rows.
impl PipelineBlock {
    /// §23.3's pipeline-chaining fixture invariant, named exactly to guard the substitution
    /// mistake §23.3 itself warns about: the chain is `evidence.persisted == knowledge.eligible
    /// == projection.expected` — **not** `knowledge.processed`, a different quantity (rows
    /// actually finished, vs. rows the pipeline considers in scope at all) that nothing else in
    /// this type would catch if swapped in by hand.
    pub fn chaining_consistent(&self) -> bool {
        self.count_inconsistency_reason().is_none()
    }

    fn count_inconsistency_reason(&self) -> Option<CannotEstablishReason> {
        // The assembler may label scopes equal only after it read the same authorized universe
        // and snapshot; this pure type checks the declared contract, never manufactures it.
        let (Some(persisted), Some(eligible), Some(processed), Some(waiting_key), Some(failed)) = (
            self.evidence.persisted,
            self.knowledge.eligible,
            self.knowledge.processed,
            self.knowledge.waiting_key,
            self.knowledge.failed,
        ) else {
            return Some(CannotEstablishReason::CountUnknown);
        };
        if self.evidence.count_scope != self.knowledge.count_scope
            || self.evidence.count_scope != CountScope::StreamLedger
        {
            return Some(CannotEstablishReason::CountScopeMismatch);
        }
        let knowledge_total = processed
            .checked_add(waiting_key)
            .and_then(|total| total.checked_add(failed));
        if persisted != eligible
            || eligible != self.projection.expected
            || knowledge_total != Some(eligible)
        {
            return Some(CannotEstablishReason::PipelineCountMismatch);
        }
        None
    }
}

// ============================================================================
// §22 `completeness` block
// ============================================================================

/// Wire mirror of [`CompletenessClass`] (§22.5's private type — see `completeness`'s module
/// doc for why). A conversion, not a re-export: `CompletenessClass` stays `pub(crate)` so
/// `classify()` (out of this task's scope) stays its sole producer; this type is what the
/// public envelope actually serializes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletenessClassWire {
    Exact,
    FacetComplete,
    SemanticBounded,
    CannotEstablish,
}

impl From<CompletenessClass> for CompletenessClassWire {
    fn from(c: CompletenessClass) -> Self {
        match c {
            CompletenessClass::Exact => Self::Exact,
            CompletenessClass::FacetComplete => Self::FacetComplete,
            CompletenessClass::SemanticBounded => Self::SemanticBounded,
            // 这个 wire 枚举只镜像 §59 的 **class** 词表；reason 由
            // [`CannotEstablishReasonWire`] 单独承载（见 [`CompletenessBlock::reason`]）。
            CompletenessClass::CannotEstablish { reason: _ } => Self::CannotEstablish,
        }
    }
}

/// §22.4 `cannot_establish` 的 reason 线格式。**闭集镜像，不是自由 `String`**（§78.2）。
///
/// 先前 [`CompletenessClassWire`] 的 `From` 里写着「reason is dropped here on purpose …
/// 待后续 wave」。那条断链的代价是：§25.5 的 `reason = mandatory_context_overflow` 在
/// JSON 上**不可观测**——于是「溢出必须可见」这条判据不可能红转绿，只改枚举就成了本仓
/// 点名的伪修复形状。本 wave 接通它。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CannotEstablishReasonWire {
    /// A1 不成立（§23.1②）。
    LedgerNotClosed,
    /// 谓词不可枚举（§20.2 rule 4）。
    PredicateNotEnumerable,
    /// Authority census 失败（§22.4 trigger 4）。
    CensusFailed,
    /// lane 故障（§22.4）。
    LaneFailed,
    /// Qdrant visible 计数取不到（§23.1②）。
    IndexCountUnavailable,
    /// A2 的 `>` 侧超过 pending（§23.1②）。
    A2OvershootBeyondPending,
    /// §25.5：Mandatory Context 超硬上限。
    MandatoryContextOverflow,
    /// A required pipeline count is unknown; it must not be filled from returned/highwater.
    CountUnknown,
    /// Pipeline blocks describe different count universes.
    CountScopeMismatch,
    /// Known values in the same universe disagree.
    PipelineCountMismatch,
    /// §25.3 (card 22c)：Mandatory lane 跑完了但没带回它有义务带回的全部内容
    /// （`handoff.counts.mandatory_missing > 0`）。
    MandatoryNotSatisfied,
    /// ADR-0053 D-E：账本已初始化但该 family 尚无 serving version。
    NoServingProjection,
    /// §22.4 / ADR-0057 D-E: the stream's oldest pending ticket is older than the threshold.
    ProjectionLag,
}

impl CannotEstablishReasonWire {
    /// 从 class 取 reason。非 `CannotEstablish` 的 class 没有 reason。
    ///
    /// `pub(crate)`：参数类型 [`CompletenessClass`] 本身是 crate 私有的，把这个函数暴露成
    /// `pub` 只会得到一个外部调用不了的签名。
    ///
    /// 线值经 [`CompletenessClass::wire_labels`] 复核：那是 JSON 与 §41.2 计数器 label 的
    /// **同一个来源**，所以「指标 label 与 JSON 漂移」在这里就被拦住，不需要第二处对账。
    #[must_use]
    pub(crate) fn from_class(c: CompletenessClass) -> Option<Self> {
        let (_, reason_label) = c.wire_labels();
        match reason_label {
            "none" => None,
            "ledger_not_closed" => Some(Self::LedgerNotClosed),
            "predicate_not_enumerable" => Some(Self::PredicateNotEnumerable),
            "census_failed" => Some(Self::CensusFailed),
            "lane_failed" => Some(Self::LaneFailed),
            "index_count_unavailable" => Some(Self::IndexCountUnavailable),
            "a2_overshoot_beyond_pending" => Some(Self::A2OvershootBeyondPending),
            "mandatory_context_overflow" => Some(Self::MandatoryContextOverflow),
            "count_unknown" => Some(Self::CountUnknown),
            "count_scope_mismatch" => Some(Self::CountScopeMismatch),
            "pipeline_count_mismatch" => Some(Self::PipelineCountMismatch),
            "mandatory_not_satisfied" => Some(Self::MandatoryNotSatisfied),
            "no_serving_projection" => Some(Self::NoServingProjection),
            "projection_lag" => Some(Self::ProjectionLag),
            // 到不了：`wire_labels` 是闭集。真到了说明有人加了 reason 变体却没加这里，
            // 那时 `None` 会让新 reason 在 JSON 上静默消失——所以 panic 而不是 None。
            other => unreachable!("未登记的 reason 线值: {other}"),
        }
    }
}

/// One retrieval lane's outcome for this query (§22.3: "哪些 lane 执行成功").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LaneStatus {
    Ok,
    Failed,
    NotRequired,
}

/// §23.3 `completeness` block. `degradations` replaces the old bare `degraded: bool` — field
/// name is final, appears exactly once workspace-wide (§23.3), members are
/// [`DegradeCode::line_format`] strings.
#[derive(Debug, Clone, Serialize)]
pub struct CompletenessBlock {
    pub class: CompletenessClassWire,
    /// §22.4 的 reason；非 `cannot_establish` 时为 `None`。由
    /// [`CannotEstablishReasonWire::from_class`] 单点产出。
    pub reason: Option<CannotEstablishReasonWire>,
    /// §22.1 structured enumeration — present iff `class == exact` (§22.0 same-source
    /// invariant, enforced by the final envelope outcome producer).
    pub exact: Option<ExactReport>,
    /// §22.4's known lower bound when the final class is `cannot_establish`. It is copied from
    /// the actual census outcome, never inferred from rendered body/item length.
    pub known_lower_bound: Option<u64>,
    pub lanes: BTreeMap<String, LaneStatus>,
    pub candidate_count: u32,
    pub reranked_count: u32,
    pub returned: u32,
    pub truncated: bool,
    pub degradations: Vec<String>,
}

/// §22.1 EXACT enumeration wire block — the six readouts the spec's own example freezes
/// (`predicate_id / total / returned / coverage / truncated / excluded_secret`). Serialized
/// verbatim; every value is copied from one [`crate::completeness::ExactEnumeration`] (whose
/// sole constructor already enforced `returned + excluded_secret <= total` and derived
/// `coverage`/`truncated`), so no field here can disagree with the census that produced it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExactReport {
    pub predicate_id: String,
    pub total: u64,
    pub returned: u64,
    pub coverage: f64,
    pub truncated: bool,
    pub excluded_secret: u64,
}

impl ExactReport {
    fn from_enumeration(e: &crate::completeness::ExactEnumeration) -> Self {
        Self {
            predicate_id: e.predicate_id().to_string(),
            total: e.total(),
            returned: e.returned(),
            coverage: e.coverage(),
            truncated: e.truncated(),
            excluded_secret: e.excluded_secret(),
        }
    }
}

/// Final/component outcome return: wire class + reason + the §22.1 block + §22.4's known
/// lower bound, produced together so none of the four can be assembled independently of the
/// others (the same one-place discipline as [`mandatory_outcome_blocks`]).
#[derive(Debug, Clone, PartialEq)]
pub struct ExactOutcome {
    pub class: CompletenessClassWire,
    pub reason: Option<CannotEstablishReasonWire>,
    /// `Some` iff `class == Exact` (§22.0).
    pub exact: Option<ExactReport>,
    /// §22.4: `CANNOT_ESTABLISH` "必须带 reason 与已知下界：「至少有 N 条，我无法证明这是
    /// 全部」". `Some(returned)` when the census did enumerate before a later trigger (broken
    /// ledger, lane failure) blocked the class; `None` when nothing was ever counted.
    pub known_lower_bound: Option<u64>,
}

/// Borrowed facts required to produce one final, metric-qualified completeness outcome.
/// The caller retains all diagnostics, including invalid provenance, if this returns an error.
pub struct CompletenessInputs<'a> {
    pub lane_status: &'a LaneStatus,
    pub census: &'a CensusResult,
    pub ledger: &'a LedgerClosure,
    pub pipeline: &'a PipelineBlock,
    pub provenance: &'a ProvenanceBlock,
    pub visible: Option<u64>,
    pub context: Option<&'a ContextOutcome>,
    /// §25.3 `handoff.counts.mandatory_missing` for this request — `0` on every route that
    /// has no Mandatory lane. Non-zero forces `cannot_establish/mandatory_not_satisfied`
    /// (card 22c review debt): the shortfall must move `completeness.class`, not only sit in
    /// the handoff counts.
    pub mandatory_missing: u64,
    /// §22.4 / §78.1 (ADR-0057 D-E/D-F): the projection-lag threshold from the gateway's
    /// `PROJECTION_LAG_SECONDS` key, judged only by [`LedgerClosure::lagging`].
    pub lag_threshold: Duration,
}

/// A fully validated outcome whose final metric is still pending downstream acceptance.
///
/// The value cannot be cloned or inspected before [`Self::finish`] consumes it. Dropping this
/// token deliberately records nothing: an output that did not survive its final schema and
/// transaction boundary is not a final retrieval result.
#[must_use = "call PendingEnvelope::finish only after the caller's final acceptance boundary"]
pub struct PendingEnvelope<T> {
    value: T,
    class: CompletenessClass,
}

impl<T> PendingEnvelope<T> {
    /// Commits the one observable final classification after the caller has accepted its output.
    pub fn finish(self) -> T {
        crate::completeness::record_final_classification(self.class);
        self.value
    }
}

/// The final envelope completeness path. It validates request-bound provenance and produces an
/// accepted-but-unrecorded outcome. Only [`PendingEnvelope::finish`] records the sole final
/// metric, after the caller's final schema or transaction boundary succeeds.
pub fn envelope_outcome_block<T>(
    request: &RetrievalRequest,
    inputs: CompletenessInputs<'_>,
    accept: impl FnOnce(ExactOutcome) -> Result<T, humaux_domain::error::ErrorCode>,
) -> Result<PendingEnvelope<T>, humaux_domain::error::ErrorCode> {
    outcome_block_under(request.planner_decision(), request, inputs, accept)
}

/// ADR-0055 D-C: [`envelope_outcome_block`] for a `recall.search` answer the dense lane produced
/// whatever the planner decided. The dense lane's answer is `semantic_bounded` by construction,
/// so it is classified under `Class(Semantic)`, not the request's own decision — classifying a
/// substituted `DirectGet` as `Exact` would demand a census this lane never runs and turn the
/// answer into §22.0's `Internal`.
pub fn dense_lane_outcome_block<T>(
    request: &RetrievalRequest,
    inputs: CompletenessInputs<'_>,
    accept: impl FnOnce(ExactOutcome) -> Result<T, humaux_domain::error::ErrorCode>,
) -> Result<PendingEnvelope<T>, humaux_domain::error::ErrorCode> {
    outcome_block_under(
        &PlannerDecision::Class(QueryClass::Semantic),
        request,
        inputs,
        accept,
    )
}

/// ADR-0055 D-C / §53.1: the one place `LANE_SUBSTITUTED` fires — through `abstain()` — when the
/// planner's decision names a lane other than dense and the caller sent no `mode`. An explicit
/// `mode` (only `semantic` reaches here) means the caller chose dense: nothing was substituted.
pub fn dense_lane_substitution(
    decision: &PlannerDecision,
    explicit_mode: bool,
) -> Vec<DegradeCode> {
    if explicit_mode || *decision == PlannerDecision::Class(QueryClass::Semantic) {
        return Vec::new();
    }
    abstain(DegradeCode::LaneSubstituted, ())
        .degradations
        .into_vec()
}

fn outcome_block_under<T>(
    decision: &PlannerDecision,
    request: &RetrievalRequest,
    inputs: CompletenessInputs<'_>,
    accept: impl FnOnce(ExactOutcome) -> Result<T, humaux_domain::error::ErrorCode>,
) -> Result<PendingEnvelope<T>, humaux_domain::error::ErrorCode> {
    if !inputs.provenance.is_valid(request) {
        return Err(humaux_domain::error::ErrorCode::Internal);
    }
    let class = final_completeness_class(
        decision,
        *inputs.lane_status,
        inputs.census,
        inputs.ledger,
        inputs.pipeline,
        inputs.visible,
        inputs.context,
        inputs.mandatory_missing,
        inputs.lag_threshold,
    );
    let outcome = exact_outcome_from_class(class, inputs.census)?;
    let value = accept(outcome)?;
    Ok(PendingEnvelope { value, class })
}

/// Pure component-level producer of the (`class`, `exact` block) pair, retained only for this
/// crate's classifier/unit invariants. It intentionally emits no final metric: an exact/census
/// component has no request-bound provenance, pipeline/A2, or mandatory-context facts and
/// therefore cannot truthfully report a full Envelope outcome.
///
/// Final result reporting must use [`envelope_outcome_block`].
///
/// The one hard error: `class == exact` while the census carries no enumeration is §22.0's
/// frozen invariant violation ("出现 `class=exact` 而 `predicate_id=null` 是不变量违反，直接
/// 5xx，**不是降级**") ⇒ `Err(ErrorCode::Internal)`, never a silent downgrade to a weaker
/// class (§22.5 direction table has no such transition).
///
/// Conversely a non-`exact` class never emits the block — an enumeration attached to a
/// `cannot_establish` answer would be a second, contradicting completeness claim; the census's
/// count survives only as `known_lower_bound` (§22.4).
#[cfg(test)]
pub(crate) fn component_exact_outcome(
    planner_output: &crate::planner::PlannerDecision,
    lane_status: LaneStatus,
    census: &crate::completeness::CensusResult,
    ledger: &crate::completeness::LedgerClosure,
    lag_threshold: Duration,
) -> Result<ExactOutcome, humaux_domain::error::ErrorCode> {
    let class = crate::completeness::classify(
        planner_output,
        lane_status,
        census,
        ledger,
        0,
        lag_threshold,
    );
    exact_outcome_from_class(class, census)
}

pub(crate) fn exact_outcome_from_class(
    class: CompletenessClass,
    census: &crate::completeness::CensusResult,
) -> Result<ExactOutcome, humaux_domain::error::ErrorCode> {
    let reason = CannotEstablishReasonWire::from_class(class);
    let wire = CompletenessClassWire::from(class);
    let exact = match (wire, census.enumeration()) {
        (CompletenessClassWire::Exact, Some(e)) => Some(ExactReport::from_enumeration(e)),
        (CompletenessClassWire::Exact, None) => {
            return Err(humaux_domain::error::ErrorCode::Internal);
        }
        (_, _) => None,
    };
    // §22.4 scopes the lower bound to `CANNOT_ESTABLISH` alone — a `semantic_bounded`
    // answer carrying "at least N" would be a partial completeness claim §22.2/§22.3 never
    // defined for it.
    let known_lower_bound = if wire == CompletenessClassWire::CannotEstablish {
        census.enumeration().map(|e| e.returned())
    } else {
        None
    };
    let outcome = ExactOutcome {
        class: wire,
        reason,
        exact,
        known_lower_bound,
    };
    Ok(outcome)
}

impl CompletenessBlock {
    /// §23.3's rerank-count invariant (its own worked example calls out violating this by
    /// name): `reranked_count == candidate_count` ⇒ no rerank-class degradation may be
    /// present, and conversely a rerank-class degradation present ⇒ `reranked_count` must be
    /// strictly less than `candidate_count`. "Rerank-class" is any [`DegradeCode`] whose
    /// [`DegradeCode::line_format`] starts with `RERANK` — `RerankModelMismatch` /
    /// `RerankProviderTimeout`, the only two today.
    pub fn rerank_degradation_consistent(&self) -> bool {
        let has_rerank_degrade = self.degradations.iter().any(|d| d.starts_with("RERANK"));
        if self.reranked_count == self.candidate_count {
            !has_rerank_degrade
        } else {
            // Not equal doesn't by itself require a degradation (e.g. `returned < candidate`
            // truncation with no rerank fallback) — the invariant is one-directional: a
            // rerank degradation implies strict inequality, not the reverse.
            !has_rerank_degrade || self.reranked_count < self.candidate_count
        }
    }

    /// §23.3's other three named fixture invariants, tying this block to the request's own
    /// [`ProfileBlock`]: `returned == profile.top_k`, `candidate_count == profile.cand_k ==
    /// min(top_k*5, 200)`, and `truncated == (candidate_count > returned)`. All four must hold
    /// together — a mismatch in any one is "不是示例，是反例" (§23.3).
    pub fn profile_consistent(&self, profile: &ProfileBlock) -> bool {
        let expected_cand_k = profile.top_k.saturating_mul(5).min(200);
        self.returned == profile.top_k
            && self.candidate_count == profile.cand_k
            && profile.cand_k == expected_cand_k
            && self.truncated == (self.candidate_count > self.returned)
    }
}

// ============================================================================
// §23.1③ `provenance` — G23-6
// ============================================================================

/// §23.3 `provenance.profile` sub-block.
#[derive(Debug, Clone, Serialize)]
pub struct ProfileBlock {
    pub top_k: u32,
    pub cand_k: u32,
    pub cand_k_formula: String,
    pub lanes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status", deny_unknown_fields)]
pub enum ProvenanceValue {
    Used { id: String },
    NotApplicable {},
    CannotEstablish {},
}

impl ProvenanceValue {
    fn is_valid(&self) -> bool {
        match self {
            Self::Used { id } => {
                let id = id.trim();
                !id.is_empty()
                    && !matches!(
                        id.to_ascii_lowercase().as_str(),
                        "none" | "n/a" | "n-a" | "unknown" | "null"
                    )
            }
            Self::NotApplicable {} => true,
            Self::CannotEstablish {} => false,
        }
    }

    fn is_not_applicable(&self) -> bool {
        matches!(self, Self::NotApplicable {})
    }
}

/// §23.3 `provenance` block — "在所有模式下都必须完整，不允许裁剪" (§23.1③). The six required
/// fields are named verbatim in §23.4 G23-6; `profile` is a nested block, not one of the six.
#[derive(Debug, Clone, Serialize)]
pub struct ProvenanceBlock {
    pub binary_build: String,
    pub projection_version: ProvenanceValue,
    pub embedding_model_id: ProvenanceValue,
    pub rerank_model_id: ProvenanceValue,
    pub card_builder_version: ProvenanceValue,
    /// Minted only by the sole request constructor; cannot be populated from a raw string.
    pub profile_fingerprint: ProfileFingerprint,
    pub profile: ProfileBlock,
}

impl ProvenanceBlock {
    /// G23-6's sole validity gate: the identities and effective profile must belong to the
    /// request actually executed. Unknown provenance never enters result statistics.
    #[must_use]
    pub fn is_valid(&self, request: &RetrievalRequest) -> bool {
        if self.binary_build.trim().is_empty()
            || self.profile_fingerprint != *request.profile_fingerprint_identity()
            || self.profile.top_k != request.top_k()
            || self.profile.cand_k != request.cand_k()
            || matches!(request.planner_decision(), PlannerDecision::CannotEstablish)
        {
            return false;
        }
        let values = [
            &self.projection_version,
            &self.embedding_model_id,
            &self.rerank_model_id,
            &self.card_builder_version,
        ];
        let structured_read = matches!(
            request.planner_decision(),
            PlannerDecision::DirectGet(_)
                | PlannerDecision::Enumerate { .. }
                | PlannerDecision::Class(QueryClass::Continuity)
        );
        values.into_iter().all(ProvenanceValue::is_valid)
            && (structured_read
                || (!self.projection_version.is_not_applicable()
                    && !self.embedding_model_id.is_not_applicable()))
    }
}

/// G23-5: e2e three-arm proof that each arm actually ran a distinct binary — same
/// `binary_build` string on two arms is red (§23.4: "e2e 中三臂的 `binary_build` 必须两两不同，
/// 相同即红"). Pure set-cardinality check; the e2e wiring that gathers the three strings is
/// out of this crate's scope.
pub fn binary_builds_pairwise_distinct(binary_builds: &[&str]) -> bool {
    let unique: std::collections::HashSet<&str> = binary_builds.iter().copied().collect();
    unique.len() == binary_builds.len()
}

/// G23-4: benchmark/evaluation summaries must be grouped by `profile_fingerprint` — mixing
/// rows from two fingerprints into one aggregate is exactly the "量具不同源" failure §23.4
/// names. Returns `Err` naming the offending fingerprints the moment a summary set contains
/// more than one, instead of silently averaging across them.
pub fn require_single_fingerprint<'a>(
    fingerprints: impl IntoIterator<Item = &'a str>,
) -> Result<(), Vec<&'a str>> {
    let unique: std::collections::BTreeSet<&str> = fingerprints.into_iter().collect();
    if unique.len() <= 1 {
        Ok(())
    } else {
        Err(unique.into_iter().collect())
    }
}

// ============================================================================
// `freshness`
// ============================================================================

/// §23.3 `freshness` block.
#[derive(Debug, Clone, Serialize)]
pub struct FreshnessBlock {
    pub class: FreshnessClass,
    /// RFC 3339, matching §23.3's `"2026-08-24T12:00:00Z"` wire form. `None` when no evidence
    /// underlies this result (e.g. an empty result set).
    pub latest_evidence_at: Option<String>,
    pub state_age_seconds: Option<u64>,
}

// ============================================================================
// §8.8 `grounding` — a separate block from `freshness` above, on purpose
// ============================================================================

/// Wire mirror of [`GroundingStateKind`], for the same reason [`CompletenessClassWire`]
/// exists: §8.8's type carries no serde derive (domain stays free of wire concerns, §3) and
/// the envelope must not re-spell the vocabulary as free strings (§78.2).
///
/// Deliberately **not** `Ord`: §8.8's priority (`CANNOT_ESTABLISH > UNRESOLVED >
/// RECHECK_REQUIRED > CURRENT`) is domain-private on purpose, and a derived ordering here
/// would be a second, silently-drifting copy of it that happens to agree today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroundingStateWire {
    Current,
    RecheckRequired,
    Unresolved,
    CannotEstablish,
}

impl From<GroundingStateKind> for GroundingStateWire {
    fn from(k: GroundingStateKind) -> Self {
        match k {
            GroundingStateKind::Current => Self::Current,
            GroundingStateKind::RecheckRequired => Self::RecheckRequired,
            GroundingStateKind::Unresolved => Self::Unresolved,
            GroundingStateKind::CannotEstablish => Self::CannotEstablish,
        }
    }
}

/// §8.8 grounding, aggregated over the items this result returned — the result-level view of
/// [`QualitySignals::grounding`](crate::signals::QualitySignals::grounding), the way
/// [`FreshnessBlock`] is the result-level view of the freshness signal.
///
/// Its own block rather than a field on [`FreshnessBlock`]: §8.8 freezes 「禁止把两者压回一个
/// `stale` 字段」, and a nested field would be that merge in all but name.
///
/// The four state counts stay broken out because §8.8 spends its `Missing`-vs-resolver-error
/// text insisting the distinction survive to the consumer — collapsing them here would undo
/// that at the reporting boundary, where the operational responses differ (recheck the source
/// vs. fix the resolver).
// ponytail: no `needs_verification[]` list of item ids — DOD-093 marks that phase=8 (Mandatory/
// Pinned context lanes), and [`Envelope`] is generic over `T` so this crate has no item-id type
// to list. Add it when §25's Mandatory Context Lane lands and can name the ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct GroundingBlock {
    pub current: u32,
    pub recheck_required: u32,
    pub unresolved: u32,
    pub cannot_establish: u32,
    /// Items §8.8's judgement never ran for — **not** a fifth state and never folded into
    /// `current`. Same discipline as §23.1's `expected: null` / `visible: null`: an
    /// un-attempted judgement is not a passing one.
    pub not_judged: u32,
    /// How many returned items no longer qualify to be assumed current truth (§8.8). Kept as
    /// its own count rather than left to the reader to sum: the "which states revoke" rule is
    /// §8.8's, read here off [`GroundingState::revokes_current_truth_assumption`], so a JSON
    /// consumer never has to re-implement it and this number cannot disagree with the domain.
    pub revokes_current_truth_assumption: u32,
}

impl GroundingBlock {
    /// One entry per returned item; `None` means §8.8's judgement never ran for that item.
    ///
    /// Takes [`GroundingState`] values rather than re-deriving anything — this crate has no
    /// derivation path of its own (§8.8: `derive_grounding_state` is the sole one).
    #[must_use]
    pub fn tally(states: impl IntoIterator<Item = Option<GroundingState>>) -> Self {
        let mut b = Self {
            current: 0,
            recheck_required: 0,
            unresolved: 0,
            cannot_establish: 0,
            not_judged: 0,
            revokes_current_truth_assumption: 0,
        };
        for state in states {
            let Some(state) = state else {
                b.not_judged += 1;
                continue;
            };
            match state.kind() {
                GroundingStateKind::Current => b.current += 1,
                GroundingStateKind::RecheckRequired => b.recheck_required += 1,
                GroundingStateKind::Unresolved => b.unresolved += 1,
                GroundingStateKind::CannotEstablish => b.cannot_establish += 1,
            }
            if state.revokes_current_truth_assumption() {
                b.revokes_current_truth_assumption += 1;
            }
        }
        b
    }
}

// ============================================================================
// §25.5 Mandatory / Pinned 顶层块
// ============================================================================

/// §25.5 的 `mandatory.*` 顶层块。
///
/// **必填枚举，不是 `Option`。** `Option` 会给出「整块省略」这条静默通道——拿到
/// `Err(MandatoryOverflow)` 之后照发一个 `mandatory: null` 的 envelope，调用方看不出区别。
/// `NotRun` 表达「这条 lane 本次没跑」（同 `EvidenceBlock.expected: Option` 立的「未做的
/// 判断不是通过的判断」纪律），但它**不可省略**。
///
/// 只有两个构造点（[`Self::from_compiled`] / [`Self::from_overflow`]），无 pub 字面量、
/// 无 `new`：一份 report 只能由真实的装配结果产出。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum MandatoryReport {
    /// 这条 lane 本次没跑。
    NotRun,
    /// 装配成功。
    Assembled {
        /// 各可用 selector 独立授权候选集合的并集基数。
        expected: u64,
        /// 实际进入 Context 的条数（从装配结果数出来，不是输入字段照抄）。
        returned: u64,
        /// 应有但没进 Context 的 id。
        missing: Vec<String>,
        /// 恒 `false`——装配成功就不是溢出。
        overflow: bool,
    },
    /// §25.5 溢出。**这个变体里没有 Context**，`returned` 恒 0。
    Overflow {
        /// 应有条数。
        expected: u64,
        /// 恒 0：溢出路径一条都没交付。
        returned: u64,
        /// §25.5 要求返回的 manifest/IDs，全量——分页建议要靠它才提得出来。
        missing: Vec<String>,
        /// 恒 `true`。
        overflow: bool,
    },
}

impl MandatoryReport {
    /// 从装配结果产出。
    ///
    /// `returned` 从 [`crate::compiler::CompiledContext`] 里**数出来**，不是把 lane 的输入
    /// 字段照抄回来——照抄的话，装配阶段把 mandatory 全丢了这里也照样报得出它们。
    #[must_use]
    pub fn from_compiled(expected: u64, c: &crate::compiler::CompiledContext) -> Self {
        let returned = c.mandatory_returned();
        Self::Assembled {
            expected,
            returned,
            // 差额只知道数量、不知道具体是哪几条（id 差集在 selector 侧才有）——
            // 这里给空 vec 而不是编造 id；数量由 expected - returned 可得。
            missing: Vec::new(),
            overflow: false,
        }
    }

    /// 从溢出产出。
    #[must_use]
    pub fn from_overflow(o: &MandatoryOverflow) -> Self {
        Self::Overflow {
            expected: o.expected(),
            returned: 0,
            missing: o.manifest().iter().map(|m| m.0.to_string()).collect(),
            overflow: true,
        }
    }

    /// 本次是否溢出。
    #[must_use]
    pub const fn is_overflow(&self) -> bool {
        matches!(self, Self::Overflow { .. })
    }

    /// `expected` / `returned`；`NotRun` 时为 `None`。
    #[must_use]
    pub const fn counts(&self) -> Option<(u64, u64)> {
        match self {
            Self::NotRun => None,
            Self::Assembled {
                expected, returned, ..
            }
            | Self::Overflow {
                expected, returned, ..
            } => Some((*expected, *returned)),
        }
    }
}

/// §25.5 的单点：把一次装配结果翻成 envelope 的三个可观测面。
///
/// **class 与 reason 与 mandatory 块在同一处产出**，这是刻意的：分开产出就给了
/// 「reason 记了但 class 还是 exact」「mandatory 报了 overflow 但 completeness 说 complete」
/// 这两条静默通道。§25.5 要的正是它们一起变。
///
/// 溢出路径没有第三种可能——[`MandatoryOverflow`] 只可能来自
/// `ContextBudget::reserve` 的 `Err` 臂，而那个类型里没有任何可返回的 Context，
/// 所以调用方在这条路径上想「截一半发出去」也没有值可发。
#[must_use]
pub fn mandatory_outcome_blocks(
    outcome: &ContextOutcome,
    expected: u64,
) -> (
    CompletenessClassWire,
    Option<CannotEstablishReasonWire>,
    MandatoryReport,
) {
    match outcome {
        ContextOutcome::Overflow(o) => {
            // 走 completeness 侧的唯一映射，不在这里手写一个 CannotEstablish：
            // 手写就等于把 §25.5 的判据抄了第二份。
            let class = crate::completeness::overflow_class(o);
            (
                CompletenessClassWire::from(class),
                CannotEstablishReasonWire::from_class(class),
                MandatoryReport::from_overflow(o),
            )
        }
        ContextOutcome::Compiled(c) => (
            // 装配成功时本函数不裁定 class——那由 §22 的 classify() 按它自己的判据决定。
            // 这里只报「不是 mandatory 溢出」，用 SemanticBounded 作占位会是越权裁定，
            // 所以返回 None 让调用方用 classify() 的结果填。
            CompletenessClassWire::SemanticBounded,
            None,
            MandatoryReport::from_compiled(expected, c),
        ),
    }
}

/// §25.5 的 `pinned.*` 顶层块。没有 `missing`——Pinned 是显式钉的，钉几条是几条。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum PinnedReport {
    /// 本次没跑。
    NotRun,
    /// 跑了。
    Ran {
        /// 钉了几条。
        expected: u64,
        /// 实际进入 Context 几条。
        returned: u64,
    },
}

// ============================================================================
// ADR-0053 D-E — the B-shaped explicit read of an unactivated family
// ============================================================================

/// ADR-0053 D-E: the one success envelope for `recall.search` / `context.assemble` on a family
/// whose ledger key is initialised but which has no serving version — `items: []`,
/// `completeness = cannot_establish / no_serving_projection` with lane `semantic: failed`,
/// `projection.visible = null` and `current = false` (from the real ledger through
/// [`build_projection_block`], never invented), `provenance.projection_version =
/// cannot_establish`, `embedding_model_id = not_applicable`, freshness `unknown`.
///
/// It bypasses [`envelope_outcome_block`] on purpose: that path refuses a `cannot_establish`
/// provenance (G23-6), and this read's provenance is exactly that. The class it records on
/// [`PendingEnvelope::finish`] is the one it serialises. B is a protocol state for the optional
/// semantic lanes only — the PG-authoritative routes (`memory.get` / `memory.enumerate`) never
/// use it (R-28: never get → NOT_FOUND, never enumerate → a complete empty set).
pub fn no_serving_projection_envelope<I, T>(
    request: &RetrievalRequest,
    binary_build: &str,
    profile_lanes: Vec<String>,
    ledger: &LedgerClosure,
    evidence: EvidenceBlock,
    knowledge: KnowledgeBlock,
    accept: impl FnOnce(Envelope<I>) -> Result<T, humaux_domain::error::ErrorCode>,
) -> Result<PendingEnvelope<T>, humaux_domain::error::ErrorCode> {
    let class = CompletenessClass::CannotEstablish {
        reason: CannotEstablishReason::NoServingProjection,
    };
    let projection = projection_block_a2(ledger, None);
    let envelope = Envelope {
        items: Vec::new(),
        pipeline: PipelineBlock {
            evidence,
            knowledge,
            projection: projection.value,
        },
        completeness: CompletenessBlock {
            class: CompletenessClassWire::from(class),
            reason: CannotEstablishReasonWire::from_class(class),
            exact: None,
            known_lower_bound: None,
            lanes: BTreeMap::from([("semantic".to_owned(), LaneStatus::Failed)]),
            candidate_count: 0,
            reranked_count: 0,
            returned: 0,
            truncated: false,
            degradations: Vec::new(),
        },
        provenance: ProvenanceBlock {
            binary_build: binary_build.to_owned(),
            projection_version: ProvenanceValue::CannotEstablish {},
            embedding_model_id: ProvenanceValue::NotApplicable {},
            rerank_model_id: ProvenanceValue::NotApplicable {},
            card_builder_version: ProvenanceValue::NotApplicable {},
            profile_fingerprint: request.profile_fingerprint_identity().clone(),
            profile: ProfileBlock {
                top_k: request.top_k(),
                cand_k: request.cand_k(),
                cand_k_formula: request.cand_k_formula(),
                lanes: profile_lanes,
            },
        },
        freshness: FreshnessBlock {
            class: FreshnessClass::Unknown,
            latest_evidence_at: None,
            state_age_seconds: None,
        },
        grounding: GroundingBlock::tally(std::iter::empty()),
        mandatory: MandatoryReport::NotRun,
        pinned: PinnedReport::NotRun,
    };
    let value = accept(envelope)?;
    Ok(PendingEnvelope { value, class })
}

// ============================================================================
// Envelope
// ============================================================================

/// §23 full envelope. Generic over the item type — this crate does not itself produce
/// recall items (that is the Planner/compiler/rerank pipeline's job, out of this task's
/// scope); `T` lets the eventual `application`-layer caller plug its own item type in
/// without a second envelope type being invented downstream.
#[derive(Debug, Clone, Serialize)]
pub struct Envelope<T> {
    pub items: Vec<T>,
    pub pipeline: PipelineBlock,
    pub completeness: CompletenessBlock,
    pub provenance: ProvenanceBlock,
    pub freshness: FreshnessBlock,
    /// §8.8, reported next to `freshness` and never inside it — the two are orthogonal.
    pub grounding: GroundingBlock,
    /// §25.5 `mandatory.*`——**顶层平级 key**，不是 `context.mandatory.*`（§25.5 的线格式
    /// 逐字如此）。先例是 `grounding`：§23.3 的 JSON 没画它，代码按 §8.8 自己加成第 6 个
    /// 顶层 block。
    pub mandatory: MandatoryReport,
    /// §25.5 `pinned.*`，同上。
    pub pinned: PinnedReport,
}

#[cfg(test)]
mod tests {

    /// §25.5 核心：溢出 ⇒ class 与 reason **一起**变成 cannot_establish /
    /// mandatory_context_overflow，且 mandatory 块报 returned=0、manifest 非空。
    ///
    /// 注错：让 `mandatory_outcome_blocks` 的 Overflow 臂返回 `SemanticBounded` ⇒ 本条红；
    /// 让它返回 `None` reason ⇒ 也红。两个面必须同时对。
    fn overflow_context_outcome() -> ContextOutcome {
        use humaux_domain::authority::MemoryId;
        use humaux_domain::context::{
            ContextBudget, MandatoryRow, PinnedLane, SelectorId, SelectorOutcome, spec,
        };

        let selector = spec(SelectorId::ProjectActiveConstraintsV1);
        let rows: Vec<humaux_domain::context::MandatoryRow> = (0..2)
            .map(|_| {
                match MandatoryRow::from_selector(
                    selector,
                    MemoryId::new(),
                    selector.min_authority,
                    80,
                    humaux_domain::grounding::RowGrounding::Judged(
                        humaux_domain::grounding::derive_grounding_state(
                            humaux_domain::grounding::GroundingInputs::Edges(&[]),
                        ),
                    ),
                )
                .expect("min_authority")
                {
                    humaux_domain::context::Admitted::Row(row) => row,
                    humaux_domain::context::Admitted::NeedsVerification(value) => {
                        panic!("CURRENT: {value:?}")
                    }
                }
            })
            .collect();
        let lane = humaux_domain::context::MandatoryLane::from_selectors([
            SelectorOutcome::Ran {
                id: SelectorId::TaskExplicitContextV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ProjectActiveConstraintsV1,
                candidate_ids: rows.iter().map(|row| row.memory_id()).collect(),
                rows,
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::UserConfirmedCorrectionsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::RequiredCurrentStateFacetsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ExplicitMandatoryBindingsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
        ])
        .expect("selector outcomes");
        let pinned = PinnedLane::new(0, vec![], vec![]);
        let overflow = ContextBudget::new(500, 100)
            .expect("budget")
            .reserve(&lane, &pinned)
            .expect_err("80 + 80 > 100");
        ContextOutcome::Overflow(overflow)
    }

    #[test]
    fn overflow_turns_class_and_reason_and_mandatory_block_together() {
        let outcome = overflow_context_outcome();
        let (class, reason, report) = mandatory_outcome_blocks(&outcome, 2);

        assert_eq!(class, CompletenessClassWire::CannotEstablish);
        assert_eq!(
            reason,
            Some(CannotEstablishReasonWire::MandatoryContextOverflow),
            "§25.5：溢出的 reason 必须是 mandatory_context_overflow"
        );
        assert!(report.is_overflow());
        assert_eq!(
            report.counts(),
            Some((2, 0)),
            "溢出路径一条都没交付，returned 必须是 0"
        );
        match &report {
            MandatoryReport::Overflow { missing, .. } => assert_eq!(
                missing.len(),
                2,
                "manifest 必须全量，否则提不出 §25.5 要的分页建议"
            ),
            other => panic!("必须是 Overflow 变体: {other:?}"),
        }

        // 线格式：mandatory 是**顶层平级 key**，且带 state 判别式。
        let v = serde_json::to_value(&report).expect("serialize");
        assert_eq!(
            v.get("state").and_then(serde_json::Value::as_str),
            Some("overflow")
        );
        assert_eq!(
            v.get("returned").and_then(serde_json::Value::as_u64),
            Some(0)
        );

        // The final outcome keeps this manifest-carrying overflow as cannot_establish and
        // cannot emit an Exact block before recording its one final metric.
        use crate::completeness::take_final_record_trace;
        let request = provenance_request("all rejected", 5, true);
        let ledger = closed(LedgerReads {
            expected: 1,
            done: 1,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        let pipeline = PipelineBlock {
            evidence: EvidenceBlock::no_batch(Some(1), CountScope::StreamLedger),
            knowledge: KnowledgeBlock {
                eligible: Some(1),
                processed: Some(1),
                waiting_key: Some(0),
                failed: Some(0),
                count_scope: CountScope::StreamLedger,
            },
            projection: build_projection_block(&ledger, Some(1), LAG).value,
        };
        assert!(take_final_record_trace().is_empty());
        let final_out = envelope_outcome_block(
            &request,
            CompletenessInputs {
                lane_status: &LaneStatus::Ok,
                census: &CensusResult::ok_without_enumeration(),
                ledger: &ledger,
                pipeline: &pipeline,
                provenance: &full_provenance(),
                visible: Some(1),
                context: Some(&outcome),
                mandatory_missing: 0,
                lag_threshold: LAG,
            },
            Ok,
        )
        .unwrap()
        .finish();
        assert_eq!(final_out.class, CompletenessClassWire::CannotEstablish);
        assert_eq!(
            final_out.reason,
            Some(CannotEstablishReasonWire::MandatoryContextOverflow)
        );
        assert!(final_out.exact.is_none());
        assert_eq!(
            take_final_record_trace(),
            vec![("cannot_establish", "mandatory_context_overflow")]
        );
    }

    /// 反向对照：装配成功时 reason 为 None、mandatory 报 assembled 且 overflow=false。
    /// 没有这条，上面那条可能因为「恒返回 overflow」而绿。
    #[test]
    fn final_overflow_prioritizes_reason_over_broken_ledger_and_keeps_census_lower_bound() {
        use crate::completeness::{ExactEnumeration, take_final_record_trace};

        let request = provenance_request("all rejected", 5, true);
        let ledger = close_mirrored(LedgerReads {
            expected: 2,
            done: 1,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        assert!(
            !ledger.is_closed(),
            "fixture must preserve the broken A1 diagnostic"
        );
        let pipeline = PipelineBlock {
            evidence: EvidenceBlock::no_batch(Some(2), CountScope::StreamLedger),
            knowledge: KnowledgeBlock {
                eligible: Some(2),
                processed: Some(2),
                waiting_key: Some(0),
                failed: Some(0),
                count_scope: CountScope::StreamLedger,
            },
            projection: build_projection_block(&ledger, Some(1), LAG).value,
        };
        assert!(
            pipeline.projection.completeness_ratio.is_none(),
            "broken projection stays attached as a diagnostic instead of being rewritten"
        );
        let census = CensusResult::enumerated(ExactEnumeration::new("p", 3, 2, 0).unwrap());
        let overflow = overflow_context_outcome();
        assert!(take_final_record_trace().is_empty());
        let out = envelope_outcome_block(
            &request,
            CompletenessInputs {
                lane_status: &LaneStatus::Ok,
                census: &census,
                ledger: &ledger,
                pipeline: &pipeline,
                provenance: &full_provenance(),
                visible: Some(1),
                context: Some(&overflow),
                mandatory_missing: 0,
                lag_threshold: LAG,
            },
            Ok,
        )
        .unwrap()
        .finish();
        assert_eq!(out.class, CompletenessClassWire::CannotEstablish);
        assert_eq!(
            out.reason,
            Some(CannotEstablishReasonWire::MandatoryContextOverflow)
        );
        assert!(out.exact.is_none());
        assert_eq!(out.known_lower_bound, Some(2));
        assert_eq!(
            take_final_record_trace(),
            vec![("cannot_establish", "mandatory_context_overflow")],
            "the combined failure records exactly one final, prioritized result"
        );

        let block = CompletenessBlock {
            class: out.class,
            reason: out.reason,
            exact: out.exact.clone(),
            known_lower_bound: out.known_lower_bound,
            lanes: BTreeMap::new(),
            candidate_count: 0,
            reranked_count: 0,
            returned: 0,
            truncated: false,
            degradations: vec![],
        };
        assert_eq!(
            serde_json::to_value(block)
                .unwrap()
                .get("known_lower_bound")
                .and_then(serde_json::Value::as_u64),
            Some(2),
            "CompletenessBlock carries the actual census lower bound, never body length"
        );
    }

    #[test]
    fn broken_ledger_without_overflow_keeps_its_own_reason() {
        use crate::completeness::{ExactEnumeration, take_final_record_trace};

        let request = provenance_request("all rejected", 5, true);
        let ledger = close_mirrored(LedgerReads {
            expected: 2,
            done: 1,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        let pipeline = PipelineBlock {
            evidence: EvidenceBlock::no_batch(Some(2), CountScope::StreamLedger),
            knowledge: KnowledgeBlock {
                eligible: Some(2),
                processed: Some(2),
                waiting_key: Some(0),
                failed: Some(0),
                count_scope: CountScope::StreamLedger,
            },
            projection: build_projection_block(&ledger, Some(1), LAG).value,
        };
        let census = CensusResult::enumerated(ExactEnumeration::new("p", 3, 2, 0).unwrap());
        assert!(take_final_record_trace().is_empty());
        let out = envelope_outcome_block(
            &request,
            CompletenessInputs {
                lane_status: &LaneStatus::Ok,
                census: &census,
                ledger: &ledger,
                pipeline: &pipeline,
                provenance: &full_provenance(),
                visible: Some(1),
                context: None,
                mandatory_missing: 0,
                lag_threshold: LAG,
            },
            Ok,
        )
        .unwrap()
        .finish();
        assert_eq!(out.reason, Some(CannotEstablishReasonWire::LedgerNotClosed));
        assert_eq!(out.known_lower_bound, Some(2));
        assert_eq!(
            take_final_record_trace(),
            vec![("cannot_establish", "ledger_not_closed")]
        );
    }

    #[test]
    fn a_compiled_context_reports_no_reason_and_no_overflow() {
        use humaux_domain::context::{ContextBudget, PinnedLane, SelectorId, SelectorOutcome};

        let lane = humaux_domain::context::MandatoryLane::from_selectors([
            SelectorOutcome::Ran {
                id: SelectorId::TaskExplicitContextV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ProjectActiveConstraintsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::UserConfirmedCorrectionsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::RequiredCurrentStateFacetsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ExplicitMandatoryBindingsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
        ])
        .expect("selector outcomes");
        let pinned = PinnedLane::new(0, vec![], vec![]);
        let budget = ContextBudget::new(100, 50)
            .expect("budget")
            .reserve(&lane, &pinned)
            .expect("空 lane 不该溢出");
        let compiled = crate::compiler::compile(lane, pinned, budget, vec![]);

        let outcome = ContextOutcome::Compiled(compiled);
        let (_class, reason, report) = mandatory_outcome_blocks(&outcome, 0);
        assert_eq!(reason, None, "没溢出就不该有 reason");
        assert!(!report.is_overflow());
        assert_eq!(report.counts(), Some((0, 0)));
    }

    /// **reason 必须在 JSON 上可观测。**
    ///
    /// 这条接的是先前那句「reason is dropped here on purpose … 待后续 wave」留下的断链。
    /// 断链没接通时，§25.5 的 `mandatory_context_overflow` 在 envelope 里根本看不见——
    /// 于是「溢出必须可见」这条判据不可能红转绿，只改枚举就是伪修复。
    #[test]
    fn cannot_establish_carries_its_reason_onto_the_wire() {
        for (domain_reason, expected_wire) in [
            (CannotEstablishReason::LedgerNotClosed, "ledger_not_closed"),
            (
                CannotEstablishReason::MandatoryContextOverflow,
                "mandatory_context_overflow",
            ),
            (CannotEstablishReason::CountUnknown, "count_unknown"),
            (
                CannotEstablishReason::CountScopeMismatch,
                "count_scope_mismatch",
            ),
            (
                CannotEstablishReason::PipelineCountMismatch,
                "pipeline_count_mismatch",
            ),
        ] {
            let class = CompletenessClass::CannotEstablish {
                reason: domain_reason,
            };
            let wire = CannotEstablishReasonWire::from_class(class)
                .expect("cannot_establish 必须带 reason");
            let json = serde_json::to_string(&wire).expect("serialize");
            assert_eq!(
                json,
                format!("\"{expected_wire}\""),
                "线值必须与 wire_labels 同源"
            );
        }
    }

    /// 反向：非 cannot_establish 的 class 没有 reason。没有这条，上面那条可能因为
    /// `from_class` 恒返回 `Some` 而绿。
    #[test]
    fn non_cannot_establish_classes_have_no_reason() {
        for class in [
            CompletenessClass::Exact,
            CompletenessClass::FacetComplete,
            CompletenessClass::SemanticBounded,
        ] {
            assert!(
                CannotEstablishReasonWire::from_class(class).is_none(),
                "{class:?} 不该有 reason"
            );
        }
    }

    /// `reason` 必须是 `completeness` 块里的**平级字段**，不是嵌在 class 里。
    /// 按路径断言而不是 `contains` 子串——嵌套结构下子串照样能命中。
    #[test]
    fn reason_is_a_sibling_of_class_in_the_completeness_block() {
        let block = CompletenessBlock {
            class: CompletenessClassWire::CannotEstablish,
            reason: Some(CannotEstablishReasonWire::MandatoryContextOverflow),
            exact: None,
            known_lower_bound: None,
            lanes: BTreeMap::new(),
            candidate_count: 0,
            reranked_count: 0,
            returned: 0,
            truncated: false,
            degradations: vec![],
        };
        let v: serde_json::Value =
            serde_json::to_value(&block).expect("serialize completeness block");
        assert_eq!(
            v.get("class").and_then(serde_json::Value::as_str),
            Some("cannot_establish")
        );
        assert_eq!(
            v.get("reason").and_then(serde_json::Value::as_str),
            Some("mandatory_context_overflow"),
            "reason 必须是顶层平级 key: {v}"
        );
        assert_eq!(
            v.get("known_lower_bound"),
            Some(&serde_json::Value::Null),
            "non-census cannot_establish keeps the lower bound explicitly null"
        );
    }

    use super::*;
    use crate::completeness::ledger::{self, LedgerReads};

    /// No fixture in this module has a pending age, so the threshold never fires here.
    const LAG: Duration = Duration::from_secs(30);
    use humaux_domain::grounding::{
        EdgeOutcome, GroundingEdge, GroundingInputs, GroundingMode, GroundingVersionToken,
        derive_grounding_state,
    };

    /// The pre-ADR-0057 fixtures state A2 in tickets. On a stream where every ticket projects
    /// exactly one memory (no fan-out, no lifecycle ticket, no failure) the point readings equal
    /// the ticket terms: settled = done - deleted - skipped, expected = expected - deleted,
    /// in-flight = pending, unsettled = 0. The point-unit tests below state their readings
    /// directly instead.
    fn mirrored(reads: &LedgerReads) -> ledger::ProjectionReads {
        ledger::ProjectionReads {
            points_expected: reads.expected.saturating_sub(reads.deleted),
            points_settled: reads.done.saturating_sub(reads.deleted + reads.skipped),
            points_in_flight: reads.pending,
            points_unsettled: 0,
            oldest_pending_age_secs: None,
        }
    }

    fn close_mirrored(reads: LedgerReads) -> LedgerClosure {
        ledger::close(reads, mirrored(&reads))
    }

    fn closed(reads: LedgerReads) -> LedgerClosure {
        let c = close_mirrored(reads);
        assert!(c.is_closed(), "fixture must have A1 closed");
        c
    }

    // ---- §23.2 三对照：删了 10 条 / 未完成 10 条 / 丢了 10 条 ----

    /// §23.2 例一：删了 10 条 — tombstone，前缀完整，A1/A2 都成立，ratio 恒 1.0。
    #[test]
    fn deleted_ten_ratio_one_current_true() {
        let ledger = closed(LedgerReads {
            expected: 100,
            done: 100,
            deleted: 10,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        let out = build_projection_block(&ledger, Some(90), LAG);
        assert!(out.degradations.is_empty());
        let b = out.value;
        assert_eq!(b.completeness_ratio, Some(1.0));
        assert!(b.current);
        assert_eq!(b.visible, Some(90));
    }

    /// §23.2 例二：未完成 10 条（8 gap + 2 pending）— A1/A2 都成立，ratio 0.9，`current=false`
    /// 因 `open_gaps != 0`（与例三同一个数字比值，两者必须能区分——见 §23.2 判据）。
    #[test]
    fn unfinished_ten_ratio_point_nine_current_false_via_open_gaps() {
        let ledger = closed(LedgerReads {
            expected: 100,
            done: 90,
            deleted: 0,
            skipped: 0,
            open_gaps: 8,
            pending: 2,
        });
        let out = build_projection_block(&ledger, Some(90), LAG);
        assert!(out.degradations.is_empty(), "no loss here, just open gaps");
        let b = out.value;
        assert_eq!(b.completeness_ratio, Some(0.9));
        assert!(!b.current);
    }

    /// §23.2 判据例（不可省）：丢了 10 条 — 账本说全部结清、没洞、没删，索引少 10。A1 成立，
    /// A2 违反。新口径必须读 `0.9`；旧口径 `(done-deleted)/(expected-deleted)` 在同一组数上
    /// 恒读 `1.0` — 这条断言就是"新旧口径不可能同时通过"的那个判据。
    #[test]
    fn lost_ten_ratio_point_nine_not_one_and_flags_invisible_loss() {
        let ledger = closed(LedgerReads {
            expected: 100,
            done: 100,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        let out = build_projection_block(&ledger, Some(90), LAG);
        let b = &out.value;
        assert_eq!(b.completeness_ratio, Some(0.9));
        assert!(!b.current);
        assert_eq!(
            out.degradations.as_slice(),
            &[DegradeCode::ProjectionInvisibleLoss]
        );

        // Reverse falsification: the old formula reads 1.0 on this exact fixture, unchanged —
        // proof the new formula is what makes this fixture able to fail.
        let (done, deleted, expected) = (100u64, 0u64, 100u64);
        let old_ratio = (done - deleted) as f64 / (expected - deleted) as f64;
        assert_eq!(old_ratio, 1.0);
        assert_ne!(b.completeness_ratio, Some(old_ratio));
    }

    // ---- §23.3 完整示例：全部衔接断言 ----

    #[test]
    fn section_23_3_full_worked_example() {
        let ledger = closed(LedgerReads {
            expected: 98,
            done: 95,
            deleted: 3,
            skipped: 2,
            open_gaps: 1,
            pending: 2,
        });
        let out = build_projection_block(&ledger, Some(88), LAG);
        let b = &out.value;
        assert_eq!(b.expected, 98);
        assert_eq!(b.done, 95);
        assert_eq!(b.deleted, 3);
        assert_eq!(b.skipped, 2);
        assert_eq!(b.visible, Some(88));
        assert_eq!(b.open_gaps, 1);
        assert_eq!(b.pending, 2);
        assert!((b.completeness_ratio.unwrap() - 0.926).abs() < 1e-3);
        assert!(!b.current);
        assert_eq!(
            out.degradations.as_slice(),
            &[DegradeCode::ProjectionInvisibleLoss]
        );

        // Old-口径 comparison named in §23.3's own text: 0.968, must differ from ours.
        let old_ratio: f64 = (95.0 - 3.0) / (98.0 - 3.0);
        assert!((old_ratio - 0.968).abs() < 1e-3);
        assert_ne!(b.completeness_ratio, Some(old_ratio));

        // ---- §23.3's other named fixture invariants, exercised on the same worked example
        // rather than left uncovered (major finding fix): pipeline chaining
        // (evidence.persisted == knowledge.eligible == projection.expected — NOT
        // knowledge.processed, §23.3's own callout) and the completeness/profile trio.
        let pipeline = PipelineBlock {
            evidence: EvidenceBlock::ticket(98, Some(98), CountScope::StreamLedger),
            knowledge: KnowledgeBlock {
                eligible: Some(98),
                processed: Some(95),
                waiting_key: Some(1),
                failed: Some(2),
                count_scope: CountScope::StreamLedger,
            },
            projection: *b,
        };
        assert!(
            pipeline.chaining_consistent(),
            // dep-map: allow table-undeclared — unit-test string naming a table/pipeline stage; retrieval has no DB access
            "evidence.persisted == knowledge.eligible == projection.expected must all be 98"
        );
        // §23.3: swapping in `knowledge.processed` (95) instead of `eligible` (98) must break
        // the chain — proves the invariant actually reads the right field.
        let wrong_chain = PipelineBlock {
            knowledge: KnowledgeBlock {
                eligible: Some(95), // would-be mistake: processed's value, not eligible's
                ..pipeline.knowledge
            },
            ..pipeline.clone()
        };
        assert!(!wrong_chain.chaining_consistent());

        let cross_scope = PipelineBlock {
            evidence: EvidenceBlock::no_batch(Some(98), CountScope::AuthorizedView),
            ..pipeline.clone()
        };
        assert_eq!(
            cross_scope.count_inconsistency_reason(),
            Some(CannotEstablishReason::CountScopeMismatch)
        );
        assert!(!cross_scope.chaining_consistent());

        let unknown = PipelineBlock {
            evidence: EvidenceBlock::no_batch(None, CountScope::AuthorizedView),
            ..pipeline.clone()
        };
        assert_eq!(
            unknown.count_inconsistency_reason(),
            Some(CannotEstablishReason::CountUnknown)
        );
        assert!(!unknown.chaining_consistent());

        let profile = ProfileBlock {
            top_k: 20,
            cand_k: 100,
            cand_k_formula: "min(top_k*5, 200)".to_string(),
            lanes: vec!["dense".to_string(), "sparse".to_string()],
        };
        let completeness = CompletenessBlock {
            class: CompletenessClassWire::SemanticBounded,
            // 非 cannot_establish 的 class 没有 reason（见 CannotEstablishReasonWire）。
            reason: None,
            exact: None,
            known_lower_bound: None,
            lanes: BTreeMap::new(),
            candidate_count: 100,
            reranked_count: 100,
            returned: 20,
            truncated: true,
            degradations: vec![],
        };
        assert!(completeness.profile_consistent(&profile));
        assert!(completeness.rerank_degradation_consistent());
    }

    // ---- A1 broken / visible unavailable / A2 inconsistent: no ratio, not current ----

    /// §22.4/G23-3: A1 broken ⇒ `cannot_establish`, no ratio output at all.
    #[test]
    fn a1_broken_yields_no_ratio_and_not_current() {
        let ledger = close_mirrored(LedgerReads {
            expected: 100,
            done: 90,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0, // 90 != 100
        });
        assert!(!ledger.is_closed());
        let out = build_projection_block(&ledger, Some(90), LAG);
        assert_eq!(out.value.completeness_ratio, None);
        assert!(!out.value.current);
        assert!(
            out.degradations.is_empty(),
            "A1 break is not the same fault as A2 loss"
        );
    }

    /// §23.1②: index count unavailable ⇒ `visible: null`, never backfilled with
    /// `done - deleted`.
    #[test]
    fn visible_unavailable_yields_null_not_backfilled() {
        let ledger = closed(LedgerReads {
            expected: 100,
            done: 100,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        let out = build_projection_block(&ledger, None, LAG);
        assert_eq!(out.value.visible, None);
        assert_eq!(out.value.completeness_ratio, None);
        assert!(!out.value.current);
    }

    /// A2 `>` side exceeding `pending` — index has points the ledger never issued a ticket
    /// for. Same treatment as A1 broken: no ratio.
    #[test]
    fn a2_overshoot_beyond_pending_is_cannot_establish() {
        let ledger = closed(LedgerReads {
            expected: 100,
            done: 90,
            deleted: 0,
            skipped: 0,
            open_gaps: 5,
            pending: 5,
        });
        // visible=98 ⇒ lhs=98, done=90, over=8 > pending(5).
        let out = build_projection_block(&ledger, Some(98), LAG);
        assert_eq!(out.value.completeness_ratio, None);
        assert!(!out.value.current);
        assert!(out.degradations.is_empty());
    }

    // ---- `assemble_completeness_class`: the ratio's absence is the single source of the
    // `class` downgrade, so `classify()`'s own answer and `build_projection_block`'s own
    // `visible`/ratio cannot silently disagree (major finding fix). ----

    #[test]
    fn assemble_downgrades_to_index_count_unavailable_when_visible_is_none() {
        use crate::planner::PlannerDecision;
        let ledger = closed(LedgerReads {
            expected: 100,
            done: 100,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        let out = build_projection_block(&ledger, None, LAG);
        assert_eq!(out.value.completeness_ratio, None, "fixture precondition");
        let (class, reason) = assemble_completeness_class(
            &PlannerDecision::Class(crate::planner::QueryClass::Semantic),
            LaneStatus::Ok,
            &CensusResult::ok_without_enumeration(),
            &ledger,
            None,
            &PipelineBlock {
                evidence: EvidenceBlock::no_batch(Some(100), CountScope::StreamLedger),
                knowledge: KnowledgeBlock {
                    eligible: Some(100),
                    processed: Some(100),
                    waiting_key: Some(0),
                    failed: Some(0),
                    count_scope: CountScope::StreamLedger,
                },
                projection: out.value,
            },
            LAG,
        );
        assert_eq!(class, CompletenessClassWire::CannotEstablish);
        assert_eq!(
            reason,
            Some(CannotEstablishReasonWire::IndexCountUnavailable)
        );
    }

    #[test]
    fn assemble_downgrades_to_a2_overshoot_when_ratio_none_but_visible_present() {
        use crate::planner::PlannerDecision;
        let ledger = closed(LedgerReads {
            expected: 100,
            done: 90,
            deleted: 0,
            skipped: 0,
            open_gaps: 5,
            pending: 5,
        });
        let out = build_projection_block(&ledger, Some(98), LAG); // over=8 > pending(5)
        assert_eq!(out.value.completeness_ratio, None, "fixture precondition");
        let (class, reason) = assemble_completeness_class(
            &PlannerDecision::Class(crate::planner::QueryClass::Semantic),
            LaneStatus::Ok,
            &CensusResult::ok_without_enumeration(),
            &ledger,
            Some(98),
            &PipelineBlock {
                evidence: EvidenceBlock::no_batch(Some(100), CountScope::StreamLedger),
                knowledge: KnowledgeBlock {
                    eligible: Some(100),
                    processed: Some(90),
                    waiting_key: Some(5),
                    failed: Some(5),
                    count_scope: CountScope::StreamLedger,
                },
                projection: out.value,
            },
            LAG,
        );
        assert_eq!(class, CompletenessClassWire::CannotEstablish);
        assert_eq!(
            reason,
            Some(CannotEstablishReasonWire::A2OvershootBeyondPending)
        );
    }

    #[test]
    fn assemble_leaves_classify_answer_untouched_when_ratio_is_some() {
        use crate::planner::PlannerDecision;
        let ledger = closed(LedgerReads {
            expected: 100,
            done: 100,
            deleted: 10,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        let out = build_projection_block(&ledger, Some(90), LAG);
        assert!(
            out.value.completeness_ratio.is_some(),
            "fixture precondition"
        );
        let (class, reason) = assemble_completeness_class(
            &PlannerDecision::Class(crate::planner::QueryClass::Semantic),
            LaneStatus::Ok,
            &CensusResult::ok_without_enumeration(),
            &ledger,
            Some(90),
            &PipelineBlock {
                evidence: EvidenceBlock::no_batch(Some(100), CountScope::StreamLedger),
                knowledge: KnowledgeBlock {
                    eligible: Some(100),
                    processed: Some(100),
                    waiting_key: Some(0),
                    failed: Some(0),
                    count_scope: CountScope::StreamLedger,
                },
                projection: out.value,
            },
            LAG,
        );
        assert_eq!(class, CompletenessClassWire::SemanticBounded);
        assert_eq!(reason, None);
    }

    fn exact_pipeline_fixture() -> (LedgerClosure, PipelineBlock, CensusResult) {
        use crate::completeness::ExactEnumeration;

        let ledger = closed(LedgerReads {
            expected: 100,
            done: 100,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        let pipeline = PipelineBlock {
            evidence: EvidenceBlock::no_batch(Some(100), CountScope::StreamLedger),
            knowledge: KnowledgeBlock {
                eligible: Some(100),
                processed: Some(100),
                waiting_key: Some(0),
                failed: Some(0),
                count_scope: CountScope::StreamLedger,
            },
            projection: build_projection_block(&ledger, Some(100), LAG).value,
        };
        let census = CensusResult::enumerated(ExactEnumeration::new("p", 100, 100, 0).unwrap());
        (ledger, pipeline, census)
    }

    fn pipeline_count_failure_cases(
        base: &PipelineBlock,
    ) -> Vec<(PipelineBlock, CannotEstablishReasonWire)> {
        let mut cases = vec![
            (
                PipelineBlock {
                    evidence: EvidenceBlock::no_batch(None, CountScope::StreamLedger),
                    ..base.clone()
                },
                CannotEstablishReasonWire::CountUnknown,
            ),
            (
                PipelineBlock {
                    evidence: EvidenceBlock::no_batch(Some(100), CountScope::AuthorizedView),
                    ..base.clone()
                },
                CannotEstablishReasonWire::CountScopeMismatch,
            ),
            (
                PipelineBlock {
                    knowledge: KnowledgeBlock {
                        eligible: Some(99),
                        ..base.knowledge
                    },
                    ..base.clone()
                },
                CannotEstablishReasonWire::PipelineCountMismatch,
            ),
            (
                PipelineBlock {
                    knowledge: KnowledgeBlock {
                        processed: Some(99),
                        ..base.knowledge
                    },
                    ..base.clone()
                },
                CannotEstablishReasonWire::PipelineCountMismatch,
            ),
            (
                PipelineBlock {
                    knowledge: KnowledgeBlock {
                        processed: Some(u64::MAX),
                        waiting_key: Some(1),
                        failed: Some(0),
                        ..base.knowledge
                    },
                    ..base.clone()
                },
                CannotEstablishReasonWire::PipelineCountMismatch,
            ),
        ];
        for field in ["eligible", "processed", "waiting_key", "failed"] {
            let mut knowledge = base.knowledge;
            match field {
                "eligible" => knowledge.eligible = None,
                "processed" => knowledge.processed = None,
                "waiting_key" => knowledge.waiting_key = None,
                "failed" => knowledge.failed = None,
                _ => unreachable!(),
            }
            assert_eq!(
                serde_json::to_value(knowledge).unwrap().get(field),
                Some(&serde_json::Value::Null),
                "unknown knowledge {field} stays JSON null"
            );
            cases.push((
                PipelineBlock {
                    knowledge,
                    ..base.clone()
                },
                CannotEstablishReasonWire::CountUnknown,
            ));
        }
        cases
    }

    #[test]
    fn exact_pipeline_fixture_keeps_no_batch_expected_null_and_exact_precondition() {
        use crate::planner::PlannerDecision;

        let (ledger, base, census) = exact_pipeline_fixture();
        assert_eq!(base.evidence.expected, None, "no batch keeps expected null");
        assert!(
            base.chaining_consistent(),
            "no-batch expected=null is legitimate when the count chain is known"
        );
        assert_eq!(
            serde_json::to_value(&base.evidence)
                .unwrap()
                .get("expected")
                .cloned(),
            Some(serde_json::Value::Null),
            "the wire preserves no-batch expected as JSON null"
        );
        let evidence_unknown_json =
            serde_json::to_value(EvidenceBlock::no_batch(None, CountScope::StreamLedger)).unwrap();
        assert_eq!(
            evidence_unknown_json.get("persisted"),
            Some(&serde_json::Value::Null),
            "unknown persisted stays JSON null"
        );
        assert_eq!(
            classify(
                &PlannerDecision::Enumerate {
                    predicate_id: "p".to_string(),
                },
                LaneStatus::Ok,
                &census,
                &ledger,
                0,
                LAG,
            ),
            CompletenessClass::Exact,
            "the count gate, not a census failure, downgrades this fixture"
        );
    }

    #[test]
    fn assemble_rejects_each_pipeline_count_failure_from_a_real_exact_census() {
        use crate::planner::PlannerDecision;

        let (ledger, base, census) = exact_pipeline_fixture();
        for (pipeline, expected_reason) in pipeline_count_failure_cases(&base) {
            assert!(
                !pipeline.chaining_consistent(),
                "a failed pipeline count gate cannot chain"
            );
            let (class, reason) = assemble_completeness_class(
                &PlannerDecision::Enumerate {
                    predicate_id: "p".to_string(),
                },
                LaneStatus::Ok,
                &census,
                &ledger,
                Some(100),
                &pipeline,
                LAG,
            );
            assert_eq!(class, CompletenessClassWire::CannotEstablish);
            assert_eq!(reason, Some(expected_reason));
        }
    }

    fn valid_final_exact_outcome<T>(
        accept: impl FnOnce(ExactOutcome) -> Result<T, humaux_domain::error::ErrorCode>,
    ) -> Result<PendingEnvelope<T>, humaux_domain::error::ErrorCode> {
        let request = provenance_request("all rejected", 5, true);
        let ledger = closed(LedgerReads {
            expected: 1,
            done: 1,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        let pipeline = PipelineBlock {
            evidence: EvidenceBlock::no_batch(Some(1), CountScope::StreamLedger),
            knowledge: KnowledgeBlock {
                eligible: Some(1),
                processed: Some(1),
                waiting_key: Some(0),
                failed: Some(0),
                count_scope: CountScope::StreamLedger,
            },
            projection: build_projection_block(&ledger, Some(1), LAG).value,
        };
        let census = CensusResult::enumerated(
            crate::completeness::ExactEnumeration::new("p", 1, 1, 0).unwrap(),
        );
        envelope_outcome_block(
            &request,
            CompletenessInputs {
                lane_status: &LaneStatus::Ok,
                census: &census,
                ledger: &ledger,
                pipeline: &pipeline,
                provenance: &full_provenance(),
                visible: Some(1),
                context: None,
                mandatory_missing: 0,
                lag_threshold: LAG,
            },
            accept,
        )
    }

    #[test]
    fn final_outcome_records_once_only_after_a_real_exact_passes_every_gate() {
        use crate::completeness::take_final_record_trace;
        assert!(take_final_record_trace().is_empty());
        let pending = valid_final_exact_outcome(Ok).unwrap();
        assert!(take_final_record_trace().is_empty());
        let out = pending.finish();
        assert_eq!(out.class, CompletenessClassWire::Exact);
        assert!(out.exact.is_some());
        assert_eq!(take_final_record_trace(), vec![("exact", "none")]);
    }

    #[test]
    fn final_outcome_accept_failure_and_unfinished_pending_record_nothing() {
        use crate::completeness::take_final_record_trace;
        assert!(take_final_record_trace().is_empty());
        assert!(matches!(
            valid_final_exact_outcome(|_| Err::<(), _>(humaux_domain::error::ErrorCode::Internal)),
            Err(humaux_domain::error::ErrorCode::Internal)
        ));
        assert!(take_final_record_trace().is_empty());

        let pending = valid_final_exact_outcome(|_| Ok::<(), humaux_domain::error::ErrorCode>(()))
            .expect("all final Envelope gates pass before a caller chooses to finish");
        assert!(take_final_record_trace().is_empty());
        drop(pending);
        assert!(take_final_record_trace().is_empty());
    }

    #[test]
    fn final_outcome_rejects_unknown_counts_and_invalid_provenance_without_exact_recording() {
        use crate::completeness::{ExactEnumeration, take_final_record_trace};
        let request = provenance_request("all rejected", 5, true);
        let ledger = closed(LedgerReads {
            expected: 1,
            done: 1,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        let census = CensusResult::enumerated(ExactEnumeration::new("p", 1, 1, 0).unwrap());
        let pipeline = PipelineBlock {
            evidence: EvidenceBlock::no_batch(None, CountScope::StreamLedger),
            knowledge: KnowledgeBlock {
                eligible: Some(1),
                processed: Some(1),
                waiting_key: Some(0),
                failed: Some(0),
                count_scope: CountScope::StreamLedger,
            },
            projection: build_projection_block(&ledger, Some(1), LAG).value,
        };
        assert!(take_final_record_trace().is_empty());
        let out = envelope_outcome_block(
            &request,
            CompletenessInputs {
                lane_status: &LaneStatus::Ok,
                census: &census,
                ledger: &ledger,
                pipeline: &pipeline,
                provenance: &full_provenance(),
                visible: Some(1),
                context: None,
                mandatory_missing: 0,
                lag_threshold: LAG,
            },
            Ok,
        )
        .unwrap()
        .finish();
        assert_eq!(out.class, CompletenessClassWire::CannotEstablish);
        assert_eq!(out.reason, Some(CannotEstablishReasonWire::CountUnknown));
        assert!(out.exact.is_none());
        assert_eq!(
            take_final_record_trace(),
            vec![("cannot_establish", "count_unknown")]
        );

        let cross_scope = PipelineBlock {
            evidence: EvidenceBlock::no_batch(Some(1), CountScope::AuthorizedView),
            ..pipeline.clone()
        };
        assert!(take_final_record_trace().is_empty());
        let cross_scope_out = envelope_outcome_block(
            &request,
            CompletenessInputs {
                lane_status: &LaneStatus::Ok,
                census: &census,
                ledger: &ledger,
                pipeline: &cross_scope,
                provenance: &full_provenance(),
                visible: Some(1),
                context: None,
                mandatory_missing: 0,
                lag_threshold: LAG,
            },
            Ok,
        )
        .unwrap()
        .finish();
        assert_eq!(
            cross_scope_out.reason,
            Some(CannotEstablishReasonWire::CountScopeMismatch)
        );
        assert_eq!(
            take_final_record_trace(),
            vec![("cannot_establish", "count_scope_mismatch")]
        );

        let mut invalid = full_provenance();
        invalid.binary_build.clear();
        assert!(take_final_record_trace().is_empty());
        assert!(matches!(
            envelope_outcome_block(
                &request,
                CompletenessInputs {
                    lane_status: &LaneStatus::Ok,
                    census: &census,
                    ledger: &ledger,
                    pipeline: &pipeline,
                    provenance: &invalid,
                    visible: Some(1),
                    context: None,
                    mandatory_missing: 0,
                    lag_threshold: LAG,
                },
                Ok::<_, humaux_domain::error::ErrorCode>,
            ),
            Err(humaux_domain::error::ErrorCode::Internal)
        ));
        assert!(take_final_record_trace().is_empty());
    }

    #[test]
    fn final_exact_without_enumeration_is_an_error_and_never_records() {
        use crate::completeness::take_final_record_trace;
        let request = provenance_request("all rejected", 5, true);
        let ledger = closed(LedgerReads {
            expected: 1,
            done: 1,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        let pipeline = PipelineBlock {
            evidence: EvidenceBlock::no_batch(Some(1), CountScope::StreamLedger),
            knowledge: KnowledgeBlock {
                eligible: Some(1),
                processed: Some(1),
                waiting_key: Some(0),
                failed: Some(0),
                count_scope: CountScope::StreamLedger,
            },
            projection: build_projection_block(&ledger, Some(1), LAG).value,
        };
        assert!(take_final_record_trace().is_empty());
        assert!(matches!(
            envelope_outcome_block(
                &request,
                CompletenessInputs {
                    lane_status: &LaneStatus::Ok,
                    census: &CensusResult::ok_without_enumeration(),
                    ledger: &ledger,
                    pipeline: &pipeline,
                    provenance: &full_provenance(),
                    visible: Some(1),
                    context: None,
                    mandatory_missing: 0,
                    lag_threshold: LAG,
                },
                Ok::<_, humaux_domain::error::ErrorCode>,
            ),
            Err(humaux_domain::error::ErrorCode::Internal)
        ));
        assert!(take_final_record_trace().is_empty());
    }

    /// A2 `>` side within `pending` — normal in-flight write, not a loss, ratio still
    /// computed, but not `current` (equality is the only "closed" A2 outcome).
    #[test]
    fn a2_inflight_within_pending_computes_ratio_not_current_no_degrade() {
        let ledger = closed(LedgerReads {
            expected: 100,
            done: 90,
            deleted: 0,
            skipped: 0,
            open_gaps: 5,
            pending: 5,
        });
        // visible=93 ⇒ lhs=93, done=90, over=3 <= pending(5).
        let out = build_projection_block(&ledger, Some(93), LAG);
        assert_eq!(out.value.completeness_ratio, Some(0.93));
        assert!(!out.value.current);
        assert!(out.degradations.is_empty());
    }

    /// §23.1②: `skipped` credited on A2's left side but excluded from the denominator — a
    /// tenant whose policy skips everything must not be permanently A2-red (the "两个恒" bug
    /// this paragraph calls out), yet the exclusion must not be freely available to raise the
    /// ratio by shrinking the denominator too.
    #[test]
    fn skipped_credits_a2_left_side_but_not_denominator() {
        let ledger = closed(LedgerReads {
            expected: 100,
            done: 100,
            deleted: 0,
            skipped: 100,
            open_gaps: 0,
            pending: 0,
        });
        // visible=0 (policy-skipped content never enters the index) ⇒ lhs = 0+0+100 = 100 = done.
        let out = build_projection_block(&ledger, Some(0), LAG);
        assert!(
            out.value.current,
            "skipped must not leave A2 permanently red"
        );
        assert_eq!(out.value.completeness_ratio, Some(0.0 / 100.0));
    }

    /// Vacuous case (`expected == deleted`, denominator 0 — everything issued was later
    /// tombstoned): pinned to `ratio: Some(1.0)`, not the undefined `ratio: null, current:
    /// true` shape a naive `denom != 0` guard would otherwise leave untested.
    #[test]
    fn vacuous_all_deleted_pins_ratio_one_not_null() {
        let ledger = closed(LedgerReads {
            expected: 10,
            done: 10,
            deleted: 10,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        // visible=0: every point was tombstoned, none left visible; lhs = 0+10+0 = 10 = done.
        let out = build_projection_block(&ledger, Some(0), LAG);
        assert_eq!(out.value.completeness_ratio, Some(1.0));
        assert!(out.value.current);
        assert!(out.degradations.is_empty());
    }

    // ---- CompletenessBlock invariants (§23.3 worked-example assertion) ----

    fn completeness_block(
        candidate: u32,
        reranked: u32,
        degradations: Vec<String>,
    ) -> CompletenessBlock {
        let returned = 5;
        CompletenessBlock {
            class: CompletenessClassWire::SemanticBounded,
            // 非 cannot_establish 的 class 没有 reason（见 CannotEstablishReasonWire）。
            reason: None,
            exact: None,
            known_lower_bound: None,
            lanes: BTreeMap::new(),
            candidate_count: candidate,
            reranked_count: reranked,
            returned,
            // §23.3: `truncated == (candidate_count > returned)`, derived, not hand-picked —
            // see `CompletenessBlock::profile_consistent`.
            truncated: candidate > returned,
            degradations,
        }
    }

    #[test]
    fn equal_counts_forbid_rerank_degradation() {
        let ok = completeness_block(25, 25, vec![]);
        assert!(ok.rerank_degradation_consistent());
        let bad = completeness_block(25, 25, vec!["RERANK_PROVIDER_TIMEOUT".to_string()]);
        assert!(!bad.rerank_degradation_consistent());
    }

    #[test]
    fn rerank_degradation_requires_strict_inequality() {
        let ok = completeness_block(25, 12, vec!["RERANK_PROVIDER_TIMEOUT".to_string()]);
        assert!(ok.rerank_degradation_consistent());
        // §23.3 worked example, corrected form: 12/25 with the degradation present.
        assert!(ok.reranked_count < ok.candidate_count);
    }

    // ---- ProvenanceBlock / G23-6 ----

    fn provenance_request(query: &str, top_k: u32, enumerable: bool) -> RetrievalRequest {
        use crate::predicate_registry::{PredicateRow, load_registry};
        use crate::request::{
            RetrievalIntent, build_request, resolve_registered_retrieval_profile,
        };
        use std::collections::BTreeSet;

        // dep-map: allow table-undeclared — unit-test string naming a table/pipeline stage; retrieval has no DB access
        let scope = "private.memory_records WHERE tenant_id = $1 AND visibility_workspace_id = $2";
        let columns = ["memory_type", "superseded_at", "visibility_workspace_id"];
        let registry = load_registry(vec![PredicateRow {
            predicate_id: "rejected_decisions_v1".to_string(),
            sql_predicate: "memory_type='REJECTION' AND superseded_at IS NULL".to_string(),
            required_columns: columns.map(str::to_string).to_vec(),
            enumerable_scope: scope.to_string(),
            surface_patterns: vec!["all rejected".to_string()],
            owner_module: "retrieval::planner".to_string(),
        }])
        .expect("validated predicate");
        let indexed = columns.into_iter().map(str::to_string).collect();
        let scopes = if enumerable {
            BTreeSet::from([scope.to_string()])
        } else {
            BTreeSet::new()
        };
        let raw = BTreeMap::from([("retrieval.profile.top_k".to_string(), top_k.to_string())]);
        let profile = resolve_registered_retrieval_profile(&raw).expect("registered profile");
        let intent = RetrievalIntent::new(query.to_string(), registry, indexed, scopes)
            .expect("valid intent");
        build_request(intent, &profile).expect("request")
    }

    fn full_provenance() -> ProvenanceBlock {
        let request = provenance_request("fixture", 5, true);
        ProvenanceBlock {
            binary_build: "humaux-gateway 2026-08-24T09:11:03Z g1e1529f".to_string(),
            projection_version: ProvenanceValue::Used {
                id: "dense-v3".to_string(),
            },
            embedding_model_id: ProvenanceValue::Used {
                id: "text-embedding-v4@2026-06-11".to_string(),
            },
            rerank_model_id: ProvenanceValue::Used {
                id: "qwen3-rerank@rev".to_string(),
            },
            card_builder_version: ProvenanceValue::Used {
                id: "card-v2".to_string(),
            },
            profile_fingerprint: request.profile_fingerprint_identity().clone(),
            profile: ProfileBlock {
                top_k: request.top_k(),
                cand_k: request.cand_k(),
                cand_k_formula: "min(top_k*5, 200)".to_string(),
                lanes: vec!["literal".to_string(), "dense".to_string()],
            },
        }
    }

    fn provenance_fields(value: &mut ProvenanceBlock) -> [&mut ProvenanceValue; 4] {
        [
            &mut value.projection_version,
            &mut value.embedding_model_id,
            &mut value.rerank_model_id,
            &mut value.card_builder_version,
        ]
    }

    #[test]
    fn g23_6_all_six_fields_present_is_valid() {
        let request = provenance_request("fixture", 5, true);
        assert_eq!(
            request.planner_decision(),
            &PlannerDecision::Class(QueryClass::Semantic)
        );
        assert!(full_provenance().is_valid(&request));
    }

    #[test]
    fn g23_6_only_established_structured_reads_admit_not_applicable() {
        let queries = [
            "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            "all rejected",
            "continue from last session",
        ];
        for query in queries {
            let request = provenance_request(query, 5, true);
            let mut value = full_provenance();
            for field in provenance_fields(&mut value) {
                *field = ProvenanceValue::NotApplicable {};
            }
            assert!(value.is_valid(&request), "structured request {query:?}");
        }
        let unestablished = provenance_request("all rejected", 5, false);
        assert_eq!(
            unestablished.planner_decision(),
            &PlannerDecision::CannotEstablish
        );
        assert!(!full_provenance().is_valid(&unestablished));
    }

    #[test]
    fn g23_6_semantic_requires_projection_and_embedding_but_allows_unused_stages() {
        let semantic = provenance_request("fixture", 5, true);
        let structured = provenance_request("all rejected", 5, true);
        for index in 0..4 {
            let mut value = full_provenance();
            *provenance_fields(&mut value)[index] = ProvenanceValue::CannotEstablish {};
            assert!(!value.is_valid(&semantic));
            assert!(!value.is_valid(&structured));
        }
        for index in 0..2 {
            let mut value = full_provenance();
            *provenance_fields(&mut value)[index] = ProvenanceValue::NotApplicable {};
            assert!(
                !value.is_valid(&semantic),
                "semantic required component NA at {index}"
            );
        }
        for index in 2..4 {
            let mut value = full_provenance();
            *provenance_fields(&mut value)[index] = ProvenanceValue::NotApplicable {};
            assert!(
                value.is_valid(&semantic),
                "semantic unused component NA at {index}"
            );
        }
    }

    #[test]
    fn g23_6_rejects_reserved_used_ids_and_unknown_serde_shapes() {
        let request = provenance_request("fixture", 5, true);
        for id in ["", "none", "N/A", "n-a", " UNKNOWN ", "null", "   "] {
            for index in 0..4 {
                let mut value = full_provenance();
                *provenance_fields(&mut value)[index] =
                    ProvenanceValue::Used { id: id.to_string() };
                assert!(!value.is_valid(&request), "reserved id {id:?} at {index}");
            }
        }
        for value in [
            ProvenanceValue::Used {
                id: "dense-v3".to_string(),
            },
            ProvenanceValue::NotApplicable {},
            ProvenanceValue::CannotEstablish {},
        ] {
            let json = serde_json::to_string(&value).expect("serialize provenance");
            assert_eq!(
                serde_json::from_str::<ProvenanceValue>(&json).unwrap(),
                value
            );
        }
        for json in [
            r#"{"status":"made_up"}"#,
            r#"{"id":"dense-v3"}"#,
            r#"{"status":"used"}"#,
            r#"{"status":"used","id":"dense-v3","extra":true}"#,
            r#"{"status":"not_applicable","id":"dense-v3"}"#,
            r#"{"status":"cannot_establish","id":"dense-v3"}"#,
        ] {
            assert!(
                serde_json::from_str::<ProvenanceValue>(json).is_err(),
                "accepted {json}"
            );
        }
    }

    #[test]
    fn g23_6_requires_the_executed_request_fingerprint_and_depth() {
        let request = provenance_request("fixture", 5, true);
        let different = provenance_request("fixture", 10, true);
        let mut value = full_provenance();
        value.profile_fingerprint = different.profile_fingerprint_identity().clone();
        assert!(!value.is_valid(&request));
        value = full_provenance();
        value.profile.top_k += 1;
        assert!(!value.is_valid(&request));
        value = full_provenance();
        value.profile.cand_k += 1;
        assert!(!value.is_valid(&request));
        value = full_provenance();
        value.binary_build = " \n\t".to_string();
        assert!(!value.is_valid(&request));
    }

    #[test]
    fn g23_5_binary_builds_must_be_pairwise_distinct() {
        assert!(binary_builds_pairwise_distinct(&["a", "b", "c"]));
        assert!(!binary_builds_pairwise_distinct(&["a", "b", "a"]));
    }

    #[test]
    fn g23_4_single_fingerprint_ok_mixed_fingerprints_rejected() {
        assert!(require_single_fingerprint(["fp1", "fp1", "fp1"]).is_ok());
        let err = require_single_fingerprint(["fp1", "fp2"]).unwrap_err();
        assert_eq!(err, vec!["fp1", "fp2"]);
    }

    // ---- G23-3: envelope must not output a ratio when the view of it in the ledger is
    // untrustworthy — proved above via a1_broken/a2_overshoot; this proves it end-to-end
    // through `Envelope<T>` serialization (`null`, not `0.0` or omitted).
    #[test]
    fn envelope_serializes_null_ratio_as_json_null_not_omitted() {
        let ledger = close_mirrored(LedgerReads {
            expected: 10,
            done: 5,
            deleted: 0,
            skipped: 0,
            open_gaps: 1,
            pending: 1, // 5+1+1=7 != 10 -> Broken
        });
        let out = build_projection_block(&ledger, Some(5), LAG);
        let envelope = Envelope::<()> {
            items: vec![],
            // 本条不测 §25.5 的两条 lane —— `NotRun` 是「本次没跑」的显式表达，
            // 不是省略（Option 才是省略，而那正是本类型不用 Option 的理由）。
            mandatory: MandatoryReport::NotRun,
            pinned: PinnedReport::NotRun,
            pipeline: PipelineBlock {
                evidence: EvidenceBlock::no_batch(Some(5), CountScope::StreamLedger),
                knowledge: KnowledgeBlock {
                    eligible: Some(5),
                    processed: Some(5),
                    waiting_key: Some(0),
                    failed: Some(0),
                    count_scope: CountScope::StreamLedger,
                },
                projection: out.value,
            },
            completeness: completeness_block(5, 5, vec![]),
            provenance: full_provenance(),
            freshness: FreshnessBlock {
                class: FreshnessClass::Unknown,
                latest_evidence_at: None,
                state_age_seconds: None,
            },
            grounding: GroundingBlock::tally([None]),
        };
        let json = serde_json::to_string(&envelope).unwrap();
        assert!(json.contains(r#""completeness_ratio":null"#));
        assert!(json.contains(r#""current":false"#));
    }

    // ---- §8.8 `grounding` block ----

    /// Builds a real [`GroundingState`] through §8.8's sole derivation point — this crate has
    /// no way to fabricate one, which is the property under test as much as the tally is.
    fn live_state(recorded: &str, outcome: EdgeOutcome) -> GroundingState {
        let edges = [GroundingEdge {
            mode: GroundingMode::Live,
            recorded_version: Some(GroundingVersionToken::new(recorded)),
            outcome,
        }];
        derive_grounding_state(GroundingInputs::Edges(&edges))
    }

    fn state_of(recorded: &str, resolved: &str) -> GroundingState {
        live_state(
            recorded,
            EdgeOutcome::Resolved(GroundingVersionToken::new(resolved)),
        )
    }

    /// All four §8.8 states, each built through the derivation point rather than named — so a
    /// fixture can cover the whole vocabulary without this file re-encoding how they arise.
    fn one_of_each_state() -> [GroundingState; 4] {
        [
            state_of("v1", "v1"),
            state_of("v1", "v2"),
            live_state("v1", EdgeOutcome::Missing),
            derive_grounding_state(GroundingInputs::DenominatorUnavailable),
        ]
    }

    /// `None` is not a fifth state and must never be counted as `current` — the grounding-side
    /// twin of §23.1's "不许输出数字充数".
    #[test]
    fn unjudged_items_are_counted_apart_from_current() {
        let b = GroundingBlock::tally([None, None, Some(state_of("v1", "v1"))]);
        assert_eq!(b.not_judged, 2);
        assert_eq!(b.current, 1);
        assert_eq!(b.revokes_current_truth_assumption, 0);
    }

    /// The four states stay broken out (§8.8 keeps `Missing` and resolver error distinct), and
    /// `revokes_current_truth_assumption` tracks §8.8's own predicate rather than a local
    /// "everything but current" sum. The fixture carries **one of each** state on purpose: with
    /// any state absent, folding it into a neighbour is unobservable here.
    #[test]
    fn tally_keeps_the_four_states_apart_and_counts_revocations() {
        let [current, recheck, unresolved, cannot_establish] = one_of_each_state();
        let b = GroundingBlock::tally([
            Some(current),
            Some(recheck),
            Some(unresolved),
            Some(cannot_establish),
            None,
        ]);
        assert_eq!(b.current, 1);
        assert_eq!(b.recheck_required, 1);
        assert_eq!(
            b.unresolved, 1,
            "「来源确实不在了」must not be lumped in with 「我没能问出来」(§8.8)"
        );
        assert_eq!(b.cannot_establish, 1);
        assert_eq!(b.not_judged, 1);
        assert_eq!(
            b.revokes_current_truth_assumption, 3,
            "the three non-CURRENT states revoke; `not_judged` is not a revocation, it is an \
             absence of judgement"
        );
    }

    /// One item at a time, each state's own revocation answer must equal §8.8's — proves the
    /// count comes from `GroundingState::revokes_current_truth_assumption`, not from a
    /// hand-maintained list of which states are "bad".
    #[test]
    fn revocation_count_agrees_with_the_domain_predicate_state_by_state() {
        for state in one_of_each_state() {
            let b = GroundingBlock::tally([Some(state)]);
            assert_eq!(
                b.revokes_current_truth_assumption == 1,
                state.revokes_current_truth_assumption(),
                "{:?} disagreed with §8.8's own predicate",
                state.kind()
            );
        }
    }

    /// §8.8「禁止把两者压回一个 `stale` 字段」at the wire boundary: `freshness` and `grounding`
    /// are two top-level keys, neither nested in the other.
    #[test]
    fn envelope_reports_grounding_beside_freshness_not_inside_it() {
        let ledger = closed(LedgerReads {
            expected: 1,
            done: 1,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        let envelope = Envelope::<()> {
            items: vec![],
            // 本条不测 §25.5 的两条 lane —— `NotRun` 是「本次没跑」的显式表达，
            // 不是省略（Option 才是省略，而那正是本类型不用 Option 的理由）。
            mandatory: MandatoryReport::NotRun,
            pinned: PinnedReport::NotRun,
            pipeline: PipelineBlock {
                evidence: EvidenceBlock::no_batch(Some(1), CountScope::StreamLedger),
                knowledge: KnowledgeBlock {
                    eligible: Some(1),
                    processed: Some(1),
                    waiting_key: Some(0),
                    failed: Some(0),
                    count_scope: CountScope::StreamLedger,
                },
                projection: build_projection_block(&ledger, Some(1), LAG).value,
            },
            completeness: completeness_block(5, 5, vec![]),
            provenance: full_provenance(),
            freshness: FreshnessBlock {
                class: FreshnessClass::Fresh,
                latest_evidence_at: Some("2026-08-24T12:00:00Z".to_string()),
                state_age_seconds: Some(300),
            },
            // §8.8's own worked example: a 5-minute-old memory whose source just moved.
            grounding: GroundingBlock::tally([Some(state_of("v1", "v2"))]),
        };
        let json = serde_json::to_string(&envelope).unwrap();
        assert!(json.contains(r#""freshness":{"class":"fresh""#));
        assert!(json.contains(r#""grounding":{"current":0,"recheck_required":1"#));
    }

    /// §22.1: the exact class carries the full six-field block, every value copied from the
    /// census's own enumeration (no independent assembly path exists).
    #[test]
    fn component_exact_outcome_carries_the_full_22_1_block_without_recording() {
        use crate::completeness::{CensusResult, ExactEnumeration};
        use crate::planner::PlannerDecision;

        let census = CensusResult::enumerated(
            ExactEnumeration::new("rejected_decisions_v1", 17, 15, 2).unwrap(),
        );
        use crate::completeness::take_final_record_trace;
        assert!(take_final_record_trace().is_empty());
        let out = component_exact_outcome(
            &PlannerDecision::Enumerate {
                predicate_id: "rejected_decisions_v1".to_string(),
            },
            LaneStatus::Ok,
            &census,
            &close_mirrored(LedgerReads {
                expected: 10,
                done: 10,
                deleted: 0,
                skipped: 0,
                open_gaps: 0,
                pending: 0,
            }),
            LAG,
        )
        .unwrap();
        assert_eq!(out.class, CompletenessClassWire::Exact);
        assert_eq!(out.reason, None);
        assert_eq!(out.known_lower_bound, None);
        let exact = out.exact.unwrap();
        assert_eq!(exact.predicate_id, "rejected_decisions_v1");
        assert_eq!(exact.total, 17);
        assert_eq!(exact.returned, 15);
        assert_eq!(exact.excluded_secret, 2);
        assert!(!exact.truncated);
        assert!((exact.coverage - 15.0 / 17.0).abs() < f64::EPSILON);
        assert!(take_final_record_trace().is_empty());
    }

    /// §22.0 fault, executable: an Enumerate decision whose census never enumerated would
    /// surface as `class=exact` with no `predicate_id`-bearing block — "不变量违反，直接
    /// 5xx，**不是降级**". The one Err path; nothing here downgrades to a weaker class.
    #[test]
    fn g22_0_fault_a_verbal_exact_claim_is_a_hard_error_not_a_downgrade() {
        use crate::completeness::CensusResult;
        use crate::planner::PlannerDecision;

        let err = component_exact_outcome(
            &PlannerDecision::Enumerate {
                predicate_id: "rejected_decisions_v1".to_string(),
            },
            LaneStatus::Ok,
            &CensusResult::ok_without_enumeration(),
            &close_mirrored(LedgerReads {
                expected: 1,
                done: 1,
                deleted: 0,
                skipped: 0,
                open_gaps: 0,
                pending: 0,
            }),
            LAG,
        )
        .unwrap_err();
        assert_eq!(err, humaux_domain::error::ErrorCode::Internal);
    }

    /// §22.4: when a later trigger (here a broken ledger) blocks the class *after* the census
    /// already counted, the count survives only as `known_lower_bound` — the §22.1 block
    /// itself must not ride along on a `cannot_establish` answer.
    #[test]
    fn cannot_establish_keeps_the_census_count_as_lower_bound_only() {
        use crate::completeness::{CensusResult, ExactEnumeration};
        use crate::planner::PlannerDecision;

        let census = CensusResult::enumerated(
            ExactEnumeration::new("rejected_decisions_v1", 17, 17, 0).unwrap(),
        );
        let out = component_exact_outcome(
            &PlannerDecision::Enumerate {
                predicate_id: "rejected_decisions_v1".to_string(),
            },
            LaneStatus::Ok,
            &census,
            &close_mirrored(LedgerReads {
                expected: 10,
                done: 5,
                deleted: 0,
                skipped: 0,
                open_gaps: 0,
                pending: 0,
            }),
            LAG,
        )
        .unwrap();
        assert_eq!(out.class, CompletenessClassWire::CannotEstablish);
        assert!(out.reason.is_some());
        assert_eq!(out.exact, None);
        assert_eq!(out.known_lower_bound, Some(17));
    }

    /// A semantic answer never emits the block *nor* a lower bound (§22.2/§22.3 define no
    /// partial-count claim for `semantic_bounded`).
    #[test]
    fn non_exact_class_never_emits_the_enumeration_block() {
        use crate::completeness::CensusResult;
        use crate::planner::{PlannerDecision, QueryClass};

        let out = component_exact_outcome(
            &PlannerDecision::Class(QueryClass::Semantic),
            LaneStatus::Ok,
            &CensusResult::ok_without_enumeration(),
            &close_mirrored(LedgerReads {
                expected: 1,
                done: 1,
                deleted: 0,
                skipped: 0,
                open_gaps: 0,
                pending: 0,
            }),
            LAG,
        )
        .unwrap();
        assert_eq!(out.class, CompletenessClassWire::SemanticBounded);
        assert_eq!(out.exact, None);
        assert_eq!(out.known_lower_bound, None);
    }

    // ---- ADR-0055 D-C: dense lane substitution ----

    #[test]
    fn dense_lane_substitution_is_empty_for_semantic_or_explicit_mode_and_lane_substituted_otherwise()
     {
        let semantic = PlannerDecision::Class(QueryClass::Semantic);
        let temporal = PlannerDecision::Class(QueryClass::Temporal);
        let direct = PlannerDecision::DirectGet(crate::planner::DirectGetLocator::MemoryId(
            "0190f7a8-0000-7000-8000-000000000001".to_string(),
        ));
        assert!(dense_lane_substitution(&semantic, false).is_empty());
        assert!(dense_lane_substitution(&semantic, true).is_empty());
        assert!(
            dense_lane_substitution(&temporal, true).is_empty(),
            "an explicit mode chose dense: nothing was substituted"
        );
        let before = humaux_telemetry::degrade::degrade_total_count(DegradeCode::LaneSubstituted);
        assert_eq!(
            dense_lane_substitution(&temporal, false),
            vec![DegradeCode::LaneSubstituted]
        );
        assert_eq!(
            dense_lane_substitution(&direct, false),
            vec![DegradeCode::LaneSubstituted]
        );
        assert!(
            humaux_telemetry::degrade::degrade_total_count(DegradeCode::LaneSubstituted)
                >= before + 2,
            "the substitution must go through abstain() (§53.1)"
        );
        assert_eq!(
            DegradeCode::LaneSubstituted.line_format(),
            "LANE_SUBSTITUTED"
        );
    }

    #[test]
    fn dense_lane_outcome_classifies_direct_get_as_semantic_bounded_not_exact() {
        let request = provenance_request("0190f7a8-0000-7000-8000-000000000001", 5, true);
        assert!(matches!(
            request.planner_decision(),
            PlannerDecision::DirectGet(_)
        ));
        let ledger = closed(LedgerReads {
            expected: 1,
            done: 1,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        });
        let pipeline = PipelineBlock {
            evidence: EvidenceBlock::no_batch(Some(1), CountScope::StreamLedger),
            knowledge: KnowledgeBlock {
                eligible: Some(1),
                processed: Some(1),
                waiting_key: Some(0),
                failed: Some(0),
                count_scope: CountScope::StreamLedger,
            },
            projection: build_projection_block(&ledger, Some(1), LAG).value,
        };
        let provenance = full_provenance();
        let census = CensusResult::ok_without_enumeration();
        let inputs = || CompletenessInputs {
            lane_status: &LaneStatus::Ok,
            census: &census,
            ledger: &ledger,
            pipeline: &pipeline,
            provenance: &provenance,
            visible: Some(1),
            context: None,
            mandatory_missing: 0,
            lag_threshold: LAG,
        };
        // The request's own decision (DirectGet ⇒ Exact without a census) is §22.0's 5xx.
        assert!(matches!(
            envelope_outcome_block(&request, inputs(), Ok),
            Err(humaux_domain::error::ErrorCode::Internal)
        ));
        let out = dense_lane_outcome_block(&request, inputs(), Ok)
            .expect("dense lane answer")
            .finish();
        assert_eq!(out.class, CompletenessClassWire::SemanticBounded);
        assert!(out.exact.is_none());
    }
}

/// ADR-0057 D-A: A2 judged in memory points. Each test names the single fault that turns it red.
#[cfg(test)]
mod a2_point_tests {
    use super::*;
    use crate::completeness::ledger::{self, LedgerReads, ProjectionReads};

    const LAG: Duration = Duration::from_secs(30);

    fn ledger_of(
        reads: LedgerReads,
        points_expected: u64,
        points_settled: u64,
        points_in_flight: u64,
        points_unsettled: u64,
    ) -> LedgerClosure {
        let closure = ledger::close(
            reads,
            ProjectionReads {
                points_expected,
                points_settled,
                points_in_flight,
                points_unsettled,
                oldest_pending_age_secs: None,
            },
        );
        assert!(closure.is_closed(), "fixture must have A1 closed");
        closure
    }

    /// A1-closed stream with one pending ticket of `age_secs` and the given point readings.
    fn pending_for(age_secs: u64, settled: u64, in_flight: u64) -> LedgerClosure {
        ledger::close(
            LedgerReads {
                expected: 5,
                done: 4,
                pending: 1,
                ..LedgerReads::default()
            },
            ProjectionReads {
                points_expected: settled + in_flight,
                points_settled: settled,
                points_in_flight: in_flight,
                points_unsettled: 0,
                oldest_pending_age_secs: Some(age_secs),
            },
        )
    }

    /// ADR-0057 D-E (test 10): a lagging stream degrades `PROJECTION_LAG` through `abstain()`
    /// and leaves `current` to its frozen formula. Fault: remove the `.also(ProjectionLag)`.
    #[test]
    fn build_projection_block_degrades_projection_lag_via_abstain() {
        let before = humaux_telemetry::degrade::degrade_total_count(DegradeCode::ProjectionLag);
        let out = build_projection_block(&pending_for(31, 4, 1), Some(4), LAG);
        assert_eq!(out.degradations.as_slice(), &[DegradeCode::ProjectionLag]);
        assert!(
            humaux_telemetry::degrade::degrade_total_count(DegradeCode::ProjectionLag) > before,
            "PROJECTION_LAG must be counted through abstain()"
        );
        assert!(
            out.value.current,
            "lag does not move the frozen `current` formula"
        );
        // At the threshold: no degradation (strict `>`).
        let at = build_projection_block(&pending_for(30, 4, 1), Some(4), LAG);
        assert!(at.degradations.is_empty(), "{:?}", at.degradations);
    }

    /// ADR-0057 D-E (test 10b): loss and lag hold together ⇒ both codes, loss first, and the
    /// class is `cannot_establish/projection_lag`. Faults: return early after the loss abstain
    /// (lag dropped) / test lag first and return (loss dropped).
    #[test]
    fn build_projection_block_carries_loss_and_lag_together() {
        let ledger = pending_for(3_600, 4, 1);
        let out = build_projection_block(&ledger, Some(3), LAG);
        assert_eq!(
            out.degradations.as_slice(),
            &[
                DegradeCode::ProjectionInvisibleLoss,
                DegradeCode::ProjectionLag
            ]
        );
        assert!(!out.value.current);
        let class = classify(
            &PlannerDecision::Class(QueryClass::Semantic),
            LaneStatus::Ok,
            &CensusResult::ok_without_enumeration(),
            &ledger,
            0,
            LAG,
        );
        assert_eq!(class.wire_labels(), ("cannot_establish", "projection_lag"));
        assert_eq!(
            CannotEstablishReasonWire::from_class(class),
            Some(CannotEstablishReasonWire::ProjectionLag)
        );
    }

    fn settled_tickets(n: u64) -> LedgerReads {
        LedgerReads {
            expected: n,
            done: n,
            ..LedgerReads::default()
        }
    }

    /// rehearsal5 `recall_after_restore`: 6 settled tickets (2 EVIDENCE_ACCEPTED fan-outs + 4
    /// lifecycle tickets) project 4 live points. Fault: judge `visible + deleted + skipped`
    /// against `done` again (4 < 6 ⇒ loss).
    #[test]
    fn a2_closes_in_points_after_lifecycle_tickets() {
        let out = build_projection_block(&ledger_of(settled_tickets(6), 4, 4, 0, 0), Some(4), LAG);
        assert!(out.degradations.is_empty(), "{:?}", out.degradations);
        assert!(out.value.current);
        assert_eq!(out.value.completeness_ratio, Some(1.0));
        assert_eq!(
            (out.value.done, out.value.points_settled, out.value.visible),
            (6, 4, Some(4))
        );
    }

    /// One Evidence → three memories: one ticket, three points. Fault: the ticket-unit judge
    /// (3 > 1 + pending 0 ⇒ Inconsistent, ratio null).
    #[test]
    fn a2_fan_out_one_evidence_three_memories_is_closed() {
        let out = build_projection_block(&ledger_of(settled_tickets(1), 3, 3, 0, 0), Some(3), LAG);
        assert!(out.degradations.is_empty());
        assert!(out.value.current);
        assert_eq!(out.value.completeness_ratio, Some(1.0));
    }

    /// `visible < points_settled` is a real loss: ratio still computed, `current=false`,
    /// `PROJECTION_INVISIBLE_LOSS`. Fault: route `<` to cannot_establish (ratio null).
    #[test]
    fn a2_visible_below_points_settled_is_loss_with_ratio() {
        let out = build_projection_block(&ledger_of(settled_tickets(2), 4, 4, 0, 0), Some(3), LAG);
        assert_eq!(out.value.completeness_ratio, Some(0.75));
        assert!(!out.value.current);
        assert_eq!(
            out.degradations.as_slice(),
            &[DegradeCode::ProjectionInvisibleLoss]
        );
    }

    /// One pending ticket whose evidence fans out to two memories: both points may already be
    /// written. Fault: use ticket `pending` (1) as the slack ⇒ L+2 reads Inconsistent.
    #[test]
    fn a2_overshoot_within_points_in_flight_is_in_flight() {
        let reads = LedgerReads {
            expected: 3,
            done: 2,
            pending: 1,
            ..LedgerReads::default()
        };
        let out = build_projection_block(&ledger_of(reads, 4, 2, 2, 0), Some(4), LAG);
        assert!(out.degradations.is_empty());
        assert!(
            out.value.completeness_ratio.is_some(),
            "in-flight keeps a ratio"
        );
        assert!(!out.value.current, "in-flight is not closed");
    }

    /// Beyond `points_settled + points_unsettled + points_in_flight` ⇒ untrustworthy, no ratio.
    /// Fault: drop the upper bound (treat every `>` as in-flight).
    #[test]
    fn a2_overshoot_beyond_points_in_flight_is_cannot_establish() {
        let reads = LedgerReads {
            expected: 3,
            done: 2,
            pending: 1,
            ..LedgerReads::default()
        };
        let out = build_projection_block(&ledger_of(reads, 4, 2, 2, 0), Some(5), LAG);
        assert_eq!(out.value.completeness_ratio, None);
        assert!(!out.value.current);
        assert!(out.degradations.is_empty());
    }

    /// Unsettled memories (latest ticket failed/retired) hold 0 or 1 point: up to Q above L is
    /// Closed, not in-flight; one more is Inconsistent. Fault: drop Q from the judge (L+2 reads
    /// Inconsistent).
    #[test]
    fn a2_points_unsettled_is_closed_slack_not_in_flight() {
        let closed =
            build_projection_block(&ledger_of(settled_tickets(5), 5, 3, 0, 2), Some(5), LAG);
        assert!(closed.value.current, "L+Q is inside the closed band");
        assert!(closed.degradations.is_empty());
        let over = build_projection_block(&ledger_of(settled_tickets(5), 5, 3, 0, 2), Some(6), LAG);
        assert_eq!(
            over.value.completeness_ratio, None,
            "L+Q+1 with F=0 is beyond the slack"
        );
        assert!(!over.value.current);
    }

    /// `completeness_ratio = visible / points_expected`. Fault: denominator `expected -
    /// deleted` (10 - 2 = 8 ⇒ 0.75, not 0.5).
    #[test]
    fn completeness_ratio_is_visible_over_points_expected() {
        let reads = LedgerReads {
            expected: 10,
            done: 8,
            deleted: 2,
            open_gaps: 2,
            ..LedgerReads::default()
        };
        let out = build_projection_block(&ledger_of(reads, 12, 6, 0, 0), Some(6), LAG);
        assert_eq!(out.value.completeness_ratio, Some(0.5));
        assert!(!out.value.current, "open_gaps > 0");
    }
}
