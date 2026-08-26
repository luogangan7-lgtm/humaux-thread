//! `retrieval::signals` — §21 五类检索质量信号：Similarity / Relevance / Association /
//! Completeness / Freshness.
//!
//! §21.5 frozen: **禁止把这五类压成一个不可解释总分**. [`QualitySignals`] therefore carries
//! the five as independent fields with no `Add`/`Sum` impl and no method that folds them into
//! one number. This is a *documented* prohibition, not a mechanically-checked one: no
//! `xtask::architecture_check` gate scans this file today (verified — it appears exactly once
//! in `architecture_check.rs`, inside `ONLINE_LANE_FILES`, an unrelated §20#G20-2 scan). A
//! future wave adding such a gate must give it its own red→green fault-injection proof
//! (§80.1) before this doc may claim mechanical enforcement again.
//!
//! §21.5 also freezes: freshness "不能用统一 7 天硬编码覆盖所有知识" — [`default_freshness_policy`]
//! is keyed by [`MemoryType`], not one constant.

use std::time::Duration;

use serde::Serialize;

use humaux_domain::memory::MemoryType;

use crate::completeness::FreshnessClass;
use crate::envelope::{CompletenessBlock, CompletenessClassWire};

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
// The five signals — kept apart, on purpose (§21.5)
// ============================================================================

/// The five §21 signals for one item. **Deliberately five separate fields, no combined
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
