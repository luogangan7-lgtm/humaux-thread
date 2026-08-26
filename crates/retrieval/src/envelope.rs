//! `retrieval::envelope` — §23 Recall Result Envelope. Assembles the four blocks
//! (`pipeline` / `completeness` / `provenance` / `freshness`) and, most load-bearing, the
//! §23.1② A1/A2 arithmetic that turns a [`LedgerClosure`](crate::completeness) plus an
//! independently-read Qdrant `visible` count into `completeness_ratio` / `current` /
//! `PROJECTION_INVISIBLE_LOSS`.
//!
//! Pure, no IO — same "adapters does the round trips, this crate only judges" split as
//! [`crate::completeness::ledger`]. §23.1②'s "取数顺序固定：先读索引 count，后读账本快照" is a
//! constraint on the *caller* that reads `visible` and builds the `LedgerClosure` before
//! calling [`build_projection_block`]; this module has no IO to order.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::completeness::{
    CannotEstablishReason, CensusResult, CompletenessClass, FreshnessClass, LedgerClosure, classify,
};
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

/// §23.3 `pipeline.evidence` block.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EvidenceBlock {
    /// `null` iff `expected_source == None` (§23.1①: "恒等值就是装饰列，不许输出数字充数").
    pub expected: Option<u64>,
    pub expected_source: ExpectedSource,
    pub persisted: u64,
}

impl EvidenceBlock {
    /// The call carried a `batch_id` whose `begin_batch` transaction A already committed —
    /// `expected` is that batch's ticket count (§23.1①, "一经发放不可回缩").
    pub fn ticket(expected: u64, persisted: u64) -> Self {
        Self {
            expected: Some(expected),
            expected_source: ExpectedSource::Ticket,
            persisted,
        }
    }

    /// The call carried no `batch_id` — `expected` must be `null`, never backfilled from
    /// `persisted` (§23.1①).
    pub fn no_batch(persisted: u64) -> Self {
        Self {
            expected: None,
            expected_source: ExpectedSource::None,
            persisted,
        }
    }
}

// ============================================================================
// `pipeline.knowledge`
// ============================================================================

/// §23.3 `pipeline.knowledge` block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct KnowledgeBlock {
    pub eligible: u64,
    pub processed: u64,
    pub waiting_key: u64,
    pub failed: u64,
}

// ============================================================================
// §23.1② `pipeline.projection` — A1/A2 closure
// ============================================================================

/// §23.1② A2 (可见闭合) outcome — directional and three-way, never a bare bool: the `<` and
/// `>` sides get opposite treatment (real loss vs. harmless in-flight write), and the `>` side
/// itself splits again at `pending` (in-flight vs. untrustworthy).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum A2Closure {
    /// `visible + deleted + skipped == done`.
    Closed,
    /// `< done` — index has fewer than the ledger says settled: a real loss.
    InvisibleLoss,
    /// `> done` but the excess is `<= pending` — a normal in-flight write (§17.4: point
    /// becomes search-visible before its `stream_log` row settles), not a loss.
    InFlight,
    /// `> done` and the excess exceeds `pending` — the index holds points the ledger never
    /// issued a ticket for; neither side is trustworthy (§23.1②: "同 A1 判 cannot_establish").
    Inconsistent,
}

fn judge_a2(visible: u64, deleted: u64, skipped: u64, done: u64, pending: u64) -> A2Closure {
    let lhs = visible + deleted + skipped;
    match lhs.cmp(&done) {
        std::cmp::Ordering::Equal => A2Closure::Closed,
        std::cmp::Ordering::Less => A2Closure::InvisibleLoss,
        std::cmp::Ordering::Greater => {
            let over = lhs - done;
            if over <= pending {
                A2Closure::InFlight
            } else {
                A2Closure::Inconsistent
            }
        }
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
    /// `null` whenever it cannot be established: A1 broken, `visible` unavailable, or A2's
    /// `>` side exceeds `pending` — all three are the *same* "don't know the true count"
    /// failure (§23.1②: "把 A2 的 `<` 侧也判成 cannot_establish 等于把真实的丢失藏进测不出来
    /// 里"，其反面同样成立：只有 `>` 超出 `pending` 才与 A1 同判). Only one ratio is ever
    /// output — there is no second "ledger-only" ratio (§23.1②).
    pub completeness_ratio: Option<f64>,
    /// §23.1② frozen definition, this crate's only computation of it:
    /// `current = (open_gaps == 0) && A2 闭合`. `pending` never enters this — see this
    /// function's doc.
    pub current: bool,
}

/// §23.1②'s full A1/A2 assembly: the one place `completeness_ratio` / `current` /
/// `PROJECTION_INVISIBLE_LOSS` are computed from a [`LedgerClosure`] and an
/// independently-read Qdrant `visible` count (`None` when the index count could not be
/// taken). Returns the block plus whatever `abstain()` degradations fired — only ever
/// [`DegradeCode::ProjectionInvisibleLoss`], and only via `abstain()` (§53.1 single exit
/// point; this function never builds `Outcome { degradations: ... }` by hand).
pub fn build_projection_block(
    ledger: &LedgerClosure,
    visible: Option<u64>,
) -> Outcome<ProjectionBlock> {
    let counts = ledger.counts();
    let expected = counts.expected();
    let done = counts.done();
    let deleted = counts.deleted();
    let skipped = counts.skipped();
    let open_gaps = counts.open_gaps();
    let pending = counts.pending();

    let no_ratio = |visible: Option<u64>| ProjectionBlock {
        expected,
        done,
        deleted,
        skipped,
        visible,
        open_gaps,
        pending,
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

    let a2 = judge_a2(v, deleted, skipped, done, pending);

    // §23.1②: `>` side exceeding `pending` is untrustworthy on both sides — same treatment
    // as A1 broken, no ratio.
    if a2 == A2Closure::Inconsistent {
        return Outcome::clean(no_ratio(Some(v)));
    }

    let denom = expected.saturating_sub(deleted);
    // §23.1②'s ratio is only ever `null` for the three named cannot-establish cases (A1
    // broken / `visible` unavailable / A2 overshoot beyond `pending`) — a vacuous stream
    // (`expected == deleted`, e.g. every issued record was tombstoned) is none of those three:
    // A1 and A2 both hold trivially. Pinned here as `1.0` — "0 of a 0-record universe" is
    // complete by definition — rather than `null`, so this branch cannot silently drift back
    // to the undefined `ratio: null, current: true` shape.
    let ratio = Some(if denom == 0 {
        1.0
    } else {
        v as f64 / denom as f64
    });
    let current = open_gaps == 0 && a2 == A2Closure::Closed;
    let block = ProjectionBlock {
        expected,
        done,
        deleted,
        skipped,
        visible: Some(v),
        open_gaps,
        pending,
        completeness_ratio: ratio,
        current,
    };

    if a2 == A2Closure::InvisibleLoss {
        abstain(DegradeCode::ProjectionInvisibleLoss, block)
    } else {
        Outcome::clean(block)
    }
}

/// §22.4's `classify()`/[`build_projection_block`] coupling, made mechanical (major finding:
/// "no type, no gate, no assembly function prevents an envelope with `visible: null,
/// completeness_ratio: null, class: semantic_bounded`"). `classify()`'s frozen 4-param
/// signature has no `visible`/A2 input (§22.5's own module doc), so it cannot see the two
/// projection-side cannot-establish triggers on its own; this function is the one place that
/// downgrades its answer using the *already-computed* [`ProjectionBlock`] instead of leaving
/// that downgrade to caller discipline. `classify()`'s own four triggers (ledger/census/
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
    projection: &ProjectionBlock,
) -> CompletenessClassWire {
    let class = classify(planner_output, lane_status, census_result, ledger);

    let class = if projection.completeness_ratio.is_none()
        && !matches!(class, CompletenessClass::CannotEstablish { .. })
    {
        let reason = if !ledger.is_closed() {
            CannotEstablishReason::LedgerNotClosed
        } else if visible.is_none() {
            CannotEstablishReason::IndexCountUnavailable
        } else {
            CannotEstablishReason::A2OvershootBeyondPending
        };
        CompletenessClass::CannotEstablish { reason }
    } else {
        class
    };

    class.into()
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

impl PipelineBlock {
    /// §23.3's pipeline-chaining fixture invariant, named exactly to guard the substitution
    /// mistake §23.3 itself warns about: the chain is `evidence.persisted == knowledge.eligible
    /// == projection.expected` — **not** `knowledge.processed`, a different quantity (rows
    /// actually finished, vs. rows the pipeline considers in scope at all) that nothing else in
    /// this type would catch if swapped in by hand.
    pub fn chaining_consistent(&self) -> bool {
        self.evidence.persisted == self.knowledge.eligible
            && self.knowledge.eligible == self.projection.expected
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
            // §22.5 adjudication 2 (completeness.rs module doc): `reason` is dropped here on
            // purpose — this wire enum mirrors §59's *class* vocabulary only; the reason
            // string belongs to `completeness` block's own `reason`-carrying field once a
            // later wave wires the JSON shape (out of this crate's current envelope scope).
            CompletenessClass::CannotEstablish { reason: _ } => Self::CannotEstablish,
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
    pub lanes: BTreeMap<String, LaneStatus>,
    pub candidate_count: u32,
    pub reranked_count: u32,
    pub returned: u32,
    pub truncated: bool,
    pub degradations: Vec<String>,
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

/// §23.3 `provenance` block — "在所有模式下都必须完整，不允许裁剪" (§23.1③). The six required
/// fields are named verbatim in §23.4 G23-6; `profile` is a nested block, not one of the six.
#[derive(Debug, Clone, Serialize)]
pub struct ProvenanceBlock {
    pub binary_build: String,
    pub projection_version: String,
    pub embedding_model_id: String,
    pub rerank_model_id: String,
    pub card_builder_version: String,
    // TODO(§55.1 build_request): `profile_fingerprint` is a bare `String` today — G23-6's
    // `is_valid()` below only checks non-empty, so a fabricated string (not §55's canonical
    // sha256 of the retrieval request) passes. `architecture-check`'s own §55.1 G80-2
    // (`build_request` sole construction point) currently reports `not_applicable` — nothing
    // covers this field's provenance from either end yet. Once `build_request` lands, this
    // must become a newtype it alone mints (§78.2 — no stringly-typed domain), not a free
    // `String` any caller can populate by hand.
    pub profile_fingerprint: String,
    pub profile: ProfileBlock,
}

impl ProvenanceBlock {
    /// G23-6: any of the six required fields empty ⇒ the whole result is invalid and must not
    /// enter any statistic (§23.4).
    pub fn is_valid(&self) -> bool {
        !self.binary_build.is_empty()
            && !self.projection_version.is_empty()
            && !self.embedding_model_id.is_empty()
            && !self.rerank_model_id.is_empty()
            && !self.card_builder_version.is_empty()
            && !self.profile_fingerprint.is_empty()
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::completeness::ledger::{self, LedgerReads};

    fn closed(reads: LedgerReads) -> LedgerClosure {
        let c = ledger::close(reads);
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
        let out = build_projection_block(&ledger, Some(90));
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
        let out = build_projection_block(&ledger, Some(90));
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
        let out = build_projection_block(&ledger, Some(90));
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
        let out = build_projection_block(&ledger, Some(88));
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
            evidence: EvidenceBlock::ticket(98, 98),
            knowledge: KnowledgeBlock {
                eligible: 98,
                processed: 95,
                waiting_key: 1,
                failed: 2,
            },
            projection: *b,
        };
        assert!(
            pipeline.chaining_consistent(),
            "evidence.persisted == knowledge.eligible == projection.expected must all be 98"
        );
        // §23.3: swapping in `knowledge.processed` (95) instead of `eligible` (98) must break
        // the chain — proves the invariant actually reads the right field.
        let wrong_chain = PipelineBlock {
            knowledge: KnowledgeBlock {
                eligible: 95, // would-be mistake: processed's value, not eligible's
                ..pipeline.knowledge
            },
            ..pipeline.clone()
        };
        assert!(!wrong_chain.chaining_consistent());

        let profile = ProfileBlock {
            top_k: 20,
            cand_k: 100,
            cand_k_formula: "min(top_k*5, 200)".to_string(),
            lanes: vec!["dense".to_string(), "sparse".to_string()],
        };
        let completeness = CompletenessBlock {
            class: CompletenessClassWire::SemanticBounded,
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
        let ledger = ledger::close(LedgerReads {
            expected: 100,
            done: 90,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0, // 90 != 100
        });
        assert!(!ledger.is_closed());
        let out = build_projection_block(&ledger, Some(90));
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
        let out = build_projection_block(&ledger, None);
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
        let out = build_projection_block(&ledger, Some(98));
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
        let out = build_projection_block(&ledger, None);
        assert_eq!(out.value.completeness_ratio, None, "fixture precondition");
        let class = assemble_completeness_class(
            &PlannerDecision::Class(crate::planner::QueryClass::Semantic),
            LaneStatus::Ok,
            &CensusResult { ok: true },
            &ledger,
            None,
            &out.value,
        );
        assert_eq!(class, CompletenessClassWire::CannotEstablish);
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
        let out = build_projection_block(&ledger, Some(98)); // over=8 > pending(5)
        assert_eq!(out.value.completeness_ratio, None, "fixture precondition");
        let class = assemble_completeness_class(
            &PlannerDecision::Class(crate::planner::QueryClass::Semantic),
            LaneStatus::Ok,
            &CensusResult { ok: true },
            &ledger,
            Some(98),
            &out.value,
        );
        assert_eq!(class, CompletenessClassWire::CannotEstablish);
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
        let out = build_projection_block(&ledger, Some(90));
        assert!(
            out.value.completeness_ratio.is_some(),
            "fixture precondition"
        );
        let class = assemble_completeness_class(
            &PlannerDecision::Class(crate::planner::QueryClass::Semantic),
            LaneStatus::Ok,
            &CensusResult { ok: true },
            &ledger,
            Some(90),
            &out.value,
        );
        assert_eq!(class, CompletenessClassWire::SemanticBounded);
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
        let out = build_projection_block(&ledger, Some(93));
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
        let out = build_projection_block(&ledger, Some(0));
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
        let out = build_projection_block(&ledger, Some(0));
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

    fn full_provenance() -> ProvenanceBlock {
        ProvenanceBlock {
            binary_build: "humaux-gateway 2026-08-24T09:11:03Z g1e1529f".to_string(),
            projection_version: "dense-v3".to_string(),
            embedding_model_id: "text-embedding-v4@2026-06-11".to_string(),
            rerank_model_id: "qwen3-rerank@rev".to_string(),
            card_builder_version: "card-v2".to_string(),
            profile_fingerprint: "sha256:abc".to_string(),
            profile: ProfileBlock {
                top_k: 5,
                cand_k: 25,
                cand_k_formula: "min(top_k*5, 200)".to_string(),
                lanes: vec!["literal".to_string(), "dense".to_string()],
            },
        }
    }

    #[test]
    fn g23_6_all_six_fields_present_is_valid() {
        assert!(full_provenance().is_valid());
    }

    #[test]
    fn g23_6_any_empty_field_invalidates_the_result() {
        let mut p = full_provenance();
        p.rerank_model_id = String::new();
        assert!(!p.is_valid());

        let mut p2 = full_provenance();
        p2.profile_fingerprint = String::new();
        assert!(!p2.is_valid());
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
        let ledger = ledger::close(LedgerReads {
            expected: 10,
            done: 5,
            deleted: 0,
            skipped: 0,
            open_gaps: 1,
            pending: 1, // 5+1+1=7 != 10 -> Broken
        });
        let out = build_projection_block(&ledger, Some(5));
        let envelope = Envelope::<()> {
            items: vec![],
            pipeline: PipelineBlock {
                evidence: EvidenceBlock::no_batch(5),
                knowledge: KnowledgeBlock {
                    eligible: 5,
                    processed: 5,
                    waiting_key: 0,
                    failed: 0,
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
        };
        let json = serde_json::to_string(&envelope).unwrap();
        assert!(json.contains(r#""completeness_ratio":null"#));
        assert!(json.contains(r#""current":false"#));
    }
}
