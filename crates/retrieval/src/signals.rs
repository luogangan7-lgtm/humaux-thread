//! `retrieval::signals` — §21 检索质量信号，按 §21.5 逐条点名的六个轴：Similarity /
//! Relevance / Association / Completeness / Temporal Freshness / Grounding State.
//!
//! 最后一个轴与 Temporal Freshness **正交**（§8.8「Temporal Freshness 与 Grounding Validity
//! 正交」）：前者答「这条状态有多旧」，后者答「当初支持它的可变来源还是同一版本吗」。§8.8
//! 「禁止把两者压回一个 `stale` 字段」，所以 [`QualitySignals::grounding`] 是自己的具名字段，
//! 不是 [`FreshnessClass`] 的第五个变体、也不是布尔。
//!
//! §21.5 frozen: **禁止把这六类压成一个不可解释总分**. [`QualitySignals`] therefore carries
//! them as independent fields with no `Add`/`Sum` impl and no method that folds them into
//! one number. This is a *documented* prohibition, not a mechanically-checked one: no
//! `xtask::architecture_check` gate scans this file today (verified — it appears exactly once
//! in `architecture_check.rs`, inside `ONLINE_LANE_FILES`, an unrelated §20#G20-2 scan). A
//! future wave adding such a gate must give it its own red→green fault-injection proof
//! (§80.1) before this doc may claim mechanical enforcement again.
//!
//! §21.5 also freezes: freshness "不能用统一 7 天硬编码覆盖所有知识" — [`default_freshness_policy`]
//! is keyed by [`MemoryType`], not one constant.

use std::time::Duration;

use serde::{Serialize, Serializer};

use humaux_domain::grounding::GroundingState;
use humaux_domain::memory::MemoryType;

use crate::completeness::FreshnessClass;
use crate::envelope::{CompletenessBlock, CompletenessClassWire, GroundingStateWire};

// ============================================================================
// §21.1 Similarity
// ============================================================================

/// Dense embedding cosine similarity. Not clamped to `[0,1]` — Qdrant's own `Distance::Cosine`
/// range is `[-1.0, 1.0]`; clamping here would silently discard a legitimately negative signal.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct SimilarityScore(pub f32);

// ============================================================================
// §21.2 Relevance
// ============================================================================

/// Cloud reranker relevance. Deliberately its own type, not a bare `f32`: §21.2's warning
/// ("reranker score 是请求内相对值，不跨 query 直接比较") lives right next to the value a
/// caller would otherwise be tempted to store and compare across queries — `request_id` makes
/// a cross-request comparison at least detectable by a caller that checks it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RelevanceScore {
    pub score: f32,
    pub request_id: String,
}

// ============================================================================
// §21.3 Association
// ============================================================================

/// §21.3's six sources, closed set. More than one may hold for the same pair at once (e.g. a
/// shared entity that is also an explicit relation), so [`AssociationSignal::sources`] is a
/// `Vec`, not a single value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssociationSource {
    ExplicitRelation,
    SharedEntity,
    TemporalRelation,
    ProjectTaskRunRelation,
    CodeGraph,
    ProvenanceLink,
}

#[derive(Debug, Clone, Serialize)]
pub struct AssociationSignal {
    pub sources: Vec<AssociationSource>,
}

impl AssociationSignal {
    pub fn none() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    pub fn from_sources(sources: impl IntoIterator<Item = AssociationSource>) -> Self {
        Self {
            sources: sources.into_iter().collect(),
        }
    }

    pub fn is_associated(&self) -> bool {
        !self.sources.is_empty()
    }
}

// ============================================================================
// §21.4 Completeness — thin view onto §23's `CompletenessBlock`, not a second computation
// ============================================================================

/// §21.4's completeness signal is the *same* completeness the envelope already reports
/// (§22/§23) — this is a read-only projection of [`CompletenessBlock`], not a parallel
/// computation that could drift from it.
#[derive(Debug, Clone, Serialize)]
pub struct CompletenessSignal {
    pub class: CompletenessClassWire,
    pub truncated: bool,
    pub degradations: Vec<String>,
}

impl From<&CompletenessBlock> for CompletenessSignal {
    fn from(b: &CompletenessBlock) -> Self {
        Self {
            class: b.class,
            truncated: b.truncated,
            degradations: b.degradations.clone(),
        }
    }
}

// ============================================================================
// §21.5 Freshness
// ============================================================================

/// A freshness policy: the boundary between `Fresh`/`Aging` and `Aging`/`Stale`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshnessPolicy {
    pub fresh_within: Duration,
    pub aging_within: Duration,
}

impl FreshnessPolicy {
    pub fn classify_age(&self, age: Duration) -> FreshnessClass {
        if age <= self.fresh_within {
            FreshnessClass::Fresh
        } else if age <= self.aging_within {
            FreshnessClass::Aging
        } else {
            FreshnessClass::Stale
        }
    }
}

/// §21.5 per-[`MemoryType`] default policy — **not** one constant for every type. Fast-moving
/// operational memory (`State`/`Issue`) ages out in hours; decisions/constraints/lessons move
/// on the order of weeks; long-lived reference material (facts, preferences, procedures,
/// outcomes, references, notes) is not expected to go stale on any short clock at all.
///
/// A workspace-level override always wins over this default when the caller has one (see
/// [`freshness_policy_for`]) — persisting/administering that override is out of this crate's
/// reach (no DB access here, §3/§78.3); this function only supplies the fallback when none is
/// configured.
// ponytail: §78.1 lists TTL among the forbidden business-config hardcodes, and this match is
// ten TTL windows keyed by MemoryType — it belongs in the §50 Typed Config Registry
// (`contracts::config_registry`), not this crate. Registering it there today would still leave
// no runtime path to supply the values (this crate has no DB access, §3/§78.3, and §50's
// registry is a schema+fingerprint utility with no store wired to it yet), so per §78.1's
// escape hatch: owner = `retrieval::signals` (this module), reason = "no live config-store
// integration exists yet for a DB-less crate to read from; the ten windows below are §21.5's
// own worked numbers, not arbitrary". Upgrade path: once a config-store read path reaches this
// crate's caller, move these ten `(fresh_within, aging_within)` pairs into `ConfigEntry` rows
// keyed by `MemoryType` and have `freshness_policy_for`'s `workspace_override` resolve from it
// instead of `None` always falling through to this fn.
pub fn default_freshness_policy(memory_type: MemoryType) -> FreshnessPolicy {
    const HOUR: u64 = 3600;
    const DAY: u64 = 24 * HOUR;
    match memory_type {
        MemoryType::State | MemoryType::Issue => FreshnessPolicy {
            fresh_within: Duration::from_secs(6 * HOUR),
            aging_within: Duration::from_secs(DAY),
        },
        MemoryType::Decision
        | MemoryType::Rejection
        | MemoryType::Constraint
        | MemoryType::Lesson => FreshnessPolicy {
            fresh_within: Duration::from_secs(7 * DAY),
            aging_within: Duration::from_secs(30 * DAY),
        },
        MemoryType::Fact
        | MemoryType::Preference
        | MemoryType::Procedure
        | MemoryType::Outcome
        | MemoryType::Reference
        | MemoryType::Note => FreshnessPolicy {
            fresh_within: Duration::from_secs(30 * DAY),
            aging_within: Duration::from_secs(180 * DAY),
        },
    }
}

/// §21.5 resolution order: a workspace override (if the caller has one) beats the per-type
/// default — never the reverse.
pub fn freshness_policy_for(
    memory_type: MemoryType,
    workspace_override: Option<FreshnessPolicy>,
) -> FreshnessPolicy {
    workspace_override.unwrap_or_else(|| default_freshness_policy(memory_type))
}

/// §21.5's four reported quantities: `latest_evidence_at` / `latest_effective_at` /
/// `state_age` / `freshness_class`.
#[derive(Debug, Clone, Serialize)]
pub struct FreshnessSignal {
    pub class: FreshnessClass,
    pub latest_evidence_at: Option<String>,
    pub latest_effective_at: Option<String>,
    pub state_age_seconds: Option<u64>,
}

/// Builds [`FreshnessSignal`], applying §21.5's per-type/per-workspace policy. `class` is
/// `Unknown` whenever `age` itself is unknown (no evidence to age at all) — never guessed at
/// from a default policy applied to a missing timestamp.
pub fn compute_freshness(
    memory_type: MemoryType,
    age: Option<Duration>,
    workspace_override: Option<FreshnessPolicy>,
    latest_evidence_at: Option<String>,
    latest_effective_at: Option<String>,
) -> FreshnessSignal {
    let Some(age) = age else {
        return FreshnessSignal {
            class: FreshnessClass::Unknown,
            latest_evidence_at,
            latest_effective_at,
            state_age_seconds: None,
        };
    };
    let policy = freshness_policy_for(memory_type, workspace_override);
    FreshnessSignal {
        class: policy.classify_age(age),
        latest_evidence_at,
        latest_effective_at,
        state_age_seconds: Some(age.as_secs()),
    }
}

// ============================================================================
// §8.8 Grounding State — its own axis, never folded into Freshness
// ============================================================================

/// §8.8's `GroundingState` carried as a §21 signal.
///
/// The wrapped [`GroundingState`] is **private and this type has no field-literal
/// constructor**: the only way in is [`From<GroundingState>`], and a `GroundingState` in turn
/// can only leave `domain::grounding::derive_grounding_state`. DOD-092's「不存在可手改
/// `memory.stale=true` 真源」therefore survives the crate boundary — this crate can relay a
/// grounding verdict but cannot mint one, and there is no second derivation site to drift.
///
/// Serializes as §8.8's state name alone ([`GroundingStateWire`]); the mode/version/edge
/// detail that produced it belongs to the resolver side, not to a retrieval result.
// ponytail: no `revokes_current_truth_assumption` mirror field and no triggering-edge list —
// the former is one call on §8.8's own type, the latter has no producer (no `EvidenceResolver`
// impl exists in this workspace yet). Add the edge list when a resolver lands and a caller
// actually needs to say *which* source moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroundingSignal(GroundingState);

impl GroundingSignal {
    /// The relayed §8.8 verdict, still as the domain type — callers needing
    /// `revokes_current_truth_assumption()` read it off §8.8's own type rather than a copy.
    #[must_use]
    pub const fn state(self) -> GroundingState {
        self.0
    }
}

impl From<GroundingState> for GroundingSignal {
    fn from(state: GroundingState) -> Self {
        Self(state)
    }
}

impl Serialize for GroundingSignal {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        GroundingStateWire::from(self.0.kind()).serialize(serializer)
    }
}

// ============================================================================
// §21 的五类信号 + §8.8 的 Grounding State —— 分开摆，不是风格问题（§21.5 冻结）
// ============================================================================

/// One item's six quality axes: §21's five signals (§21.1–§21.5) plus §8.8's Grounding
/// State. The sixth is **not** a §21 signal — §21 is still「五类检索质量信号」; §21.5 is
/// where the six are named together as the axes that must stay separable.
/// **Deliberately six separate fields, no combined
/// score.** Do not add an `Add`/`Sum` impl or a method returning one number derived from more
/// than one field here — that is exactly what §21.5 forbids. No `xtask::architecture_check`
/// gate watches this file for that shape today (see this module's own top-of-file doc); the
/// prohibition currently rests on this comment and code review alone.
#[derive(Debug, Clone, Serialize)]
pub struct QualitySignals {
    pub similarity: Option<SimilarityScore>,
    pub relevance: Option<RelevanceScore>,
    pub association: AssociationSignal,
    pub completeness: CompletenessSignal,
    pub freshness: FreshnessSignal,
    /// §8.8, orthogonal to `freshness` above — merging the two is the one thing §8.8 names as
    /// forbidden. `None` means §8.8's judgement never ran for this item, **not** `Current`:
    /// no `EvidenceResolver` producer exists in this workspace yet, and reporting an
    /// un-attempted judgement as a passing one is exactly §23's「不许输出数字充数」.
    pub grounding: Option<GroundingSignal>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_domain::grounding::{
        EdgeOutcome, GroundingEdge, GroundingInputs, GroundingMode, GroundingStateKind,
        GroundingVersionToken, derive_grounding_state,
    };

    #[test]
    fn similarity_score_carries_raw_cosine_range_uncapped() {
        assert_eq!(SimilarityScore(-0.3).0, -0.3);
        assert_eq!(SimilarityScore(0.97).0, 0.97);
    }

    #[test]
    fn relevance_score_carries_request_scope() {
        let r = RelevanceScore {
            score: 0.8,
            request_id: "req-1".to_string(),
        };
        assert_eq!(r.request_id, "req-1");
    }

    #[test]
    fn association_signal_reports_multiple_co_occurring_sources() {
        let a = AssociationSignal::from_sources([
            AssociationSource::SharedEntity,
            AssociationSource::CodeGraph,
        ]);
        assert!(a.is_associated());
        assert_eq!(a.sources.len(), 2);
        assert!(!AssociationSignal::none().is_associated());
    }

    #[test]
    fn completeness_signal_is_a_view_not_a_recomputation() {
        let block = CompletenessBlock {
            class: CompletenessClassWire::Exact,
            lanes: Default::default(),
            candidate_count: 10,
            reranked_count: 10,
            returned: 5,
            truncated: true,
            degradations: vec!["RERANK_PROVIDER_TIMEOUT".to_string()],
        };
        let signal = CompletenessSignal::from(&block);
        assert_eq!(signal.class, CompletenessClassWire::Exact);
        assert!(signal.truncated);
        assert_eq!(
            signal.degradations,
            vec!["RERANK_PROVIDER_TIMEOUT".to_string()]
        );
    }

    /// §21.5: `State`/`Issue` get a same-day policy; long-lived types get a much longer one —
    /// proves there is no single constant shared by every `MemoryType`.
    #[test]
    fn freshness_policy_differs_by_memory_type_not_one_constant() {
        let state = default_freshness_policy(MemoryType::State);
        let fact = default_freshness_policy(MemoryType::Fact);
        assert_ne!(state, fact);
        assert!(state.fresh_within < fact.fresh_within);
    }

    #[test]
    fn workspace_override_beats_per_type_default() {
        let custom = FreshnessPolicy {
            fresh_within: Duration::from_secs(1),
            aging_within: Duration::from_secs(2),
        };
        let resolved = freshness_policy_for(MemoryType::Fact, Some(custom));
        assert_eq!(resolved, custom);
        assert_ne!(resolved, default_freshness_policy(MemoryType::Fact));
    }

    #[test]
    fn compute_freshness_unknown_when_no_age_available() {
        let signal = compute_freshness(MemoryType::State, None, None, None, None);
        assert_eq!(signal.class, FreshnessClass::Unknown);
        assert_eq!(signal.state_age_seconds, None);
    }

    #[test]
    fn compute_freshness_classifies_within_policy_windows() {
        let fresh = compute_freshness(
            MemoryType::State,
            Some(Duration::from_secs(3600)),
            None,
            Some("2026-08-24T12:00:00Z".to_string()),
            None,
        );
        assert_eq!(fresh.class, FreshnessClass::Fresh);
        assert_eq!(fresh.state_age_seconds, Some(3600));

        let stale = compute_freshness(
            MemoryType::State,
            Some(Duration::from_secs(10 * 86400)),
            None,
            None,
            None,
        );
        assert_eq!(stale.class, FreshnessClass::Stale);
    }

    // ---- §8.8 grounding, and its orthogonality to §21.5 freshness ----

    /// §8.8's sole derivation point is the only way this crate can obtain a [`GroundingState`]
    /// — there is no constructor to reach for here, which is the point of DOD-092.
    fn state_of(recorded: &str, resolved: &str) -> GroundingState {
        let edges = [GroundingEdge {
            mode: GroundingMode::Live,
            recorded_version: Some(GroundingVersionToken::new(recorded)),
            outcome: EdgeOutcome::Resolved(GroundingVersionToken::new(resolved)),
        }];
        derive_grounding_state(GroundingInputs::Edges(&edges))
    }

    /// §8.8's own worked example, built for real: a two-year-old memory whose source never
    /// moved is `Stale × Current` — a combination that cannot exist if either signal is
    /// derived from the other.
    fn stale_but_grounded() -> QualitySignals {
        QualitySignals {
            similarity: None,
            relevance: None,
            association: AssociationSignal::none(),
            completeness: CompletenessSignal {
                class: CompletenessClassWire::SemanticBounded,
                truncated: false,
                degradations: vec![],
            },
            freshness: compute_freshness(
                MemoryType::State,
                Some(Duration::from_secs(730 * 86_400)),
                None,
                None,
                None,
            ),
            grounding: Some(state_of("v1", "v1").into()),
        }
    }

    fn grounding_kind(s: &QualitySignals) -> Option<GroundingStateKind> {
        s.grounding.map(|g| g.state().kind())
    }

    /// §8.8「禁止把两者压回一个 `stale` 字段」/ §21.5「禁止压成一个不可解释总分」: moving one
    /// axis must leave the other untouched. Both off-diagonal combinations §8.8 names appear
    /// here — `Stale × Current` and `Fresh × RecheckRequired` — so folding either signal into
    /// the other (or into one score) makes one of these four assertions unsatisfiable.
    #[test]
    fn grounding_and_freshness_move_independently() {
        let mut s = stale_but_grounded();
        assert_eq!(s.freshness.class, FreshnessClass::Stale);
        assert_eq!(grounding_kind(&s), Some(GroundingStateKind::Current));

        // Move grounding only: the source moved under a memory that is just as old as before.
        s.grounding = Some(state_of("v1", "v2").into());
        assert_eq!(
            s.freshness.class,
            FreshnessClass::Stale,
            "grounding 变化不得改动 freshness"
        );
        assert_eq!(
            grounding_kind(&s),
            Some(GroundingStateKind::RecheckRequired)
        );

        // Move freshness only: rewritten 5 minutes ago, source still at the version it moved
        // to — §8.8's「刚写 5 分钟的 current-state Memory + 代码刚变化 ⇒ RECHECK_REQUIRED」.
        s.freshness = compute_freshness(
            MemoryType::State,
            Some(Duration::from_secs(300)),
            None,
            None,
            None,
        );
        assert_eq!(s.freshness.class, FreshnessClass::Fresh);
        assert_eq!(
            grounding_kind(&s),
            Some(GroundingStateKind::RecheckRequired),
            "freshness 变化不得改动 grounding"
        );
    }

    /// The same orthogonality at the wire boundary: two separate keys, neither nested in the
    /// other and neither a `stale` boolean.
    #[test]
    fn grounding_serializes_as_its_own_key_beside_freshness() {
        let mut s = stale_but_grounded();
        s.grounding = Some(state_of("v1", "v2").into());
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains(r#""freshness":{"class":"stale""#));
        assert!(json.contains(r#""grounding":"recheck_required""#));
    }

    /// `None` is "judgement never ran", not `Current` — no resolver produces grounding today,
    /// and reporting that absence as a pass would be the §23「不许输出数字充数」failure.
    #[test]
    fn absent_grounding_is_not_reported_as_current() {
        let mut s = stale_but_grounded();
        s.grounding = None;
        assert_eq!(grounding_kind(&s), None);
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains(r#""grounding":null"#));
    }

    /// §21.5's own worked example: `STATE` ages out same-day, unlike a uniform 7-day window.
    #[test]
    fn state_is_aging_by_one_day_not_seven() {
        let one_day_plus = compute_freshness(
            MemoryType::State,
            Some(Duration::from_secs(86_401)),
            None,
            None,
            None,
        );
        assert_eq!(one_day_plus.class, FreshnessClass::Stale);
        let six_days = compute_freshness(
            MemoryType::Fact,
            Some(Duration::from_secs(6 * 86_400)),
            None,
            None,
            None,
        );
        assert_eq!(
            six_days.class,
            FreshnessClass::Fresh,
            "Fact's policy is not the 7-day constant §21.5 forbids either"
        );
    }
}
