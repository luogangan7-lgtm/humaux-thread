//! `retrieval-provider::failover` — §19 **Embedding Provider Failover 硬规则** and
//! **Rerank Provider Failover 规则**.
//!
//! Embedding and rerank are *not* symmetric here (§19 states this explicitly): embedding
//! writes a durable, provider-specific vector space, so only a **request-level failover
//! within the same Projection Contract** is legal — anything else is a **Projection
//! Migration**, a different, multi-step workflow this module does not perform, it only
//! refuses to let a mismatched pair through as if it were a failover
//! ([`decide_embedding_failover`]). Rerank forms no durable vector space, so a request-level
//! fallback is always legal — the hazard there is a **silently stale relevance threshold**
//! after a provider/model swap, which [`rerank_threshold_gate_state`] forces to surface loud.
//!
//! Identifiers (`ProviderId`/`ModelId`/`CalibrationProfileId`) are `crate::contract`'s T7.1
//! newtypes, reused here rather than duplicated (§41.2 R3 "同一被测对象只允许一个名字",
//! applied to types, not just metric names) — `model_revision`/`projection_version` stay
//! plain `String`, matching `contract::EmbeddingModelDescriptor::model_revision` and
//! `humaux_projection::stream::StreamKey::projection_version`'s own convention.

use std::fmt;

use humaux_domain::egress::{EgressPermit, PrivateDataPurpose};

use crate::contract::{CalibrationProfileId, ModelId, ProviderId};

/// §19 Compatible Failover's `normalization` axis. Two variants only: whether the embedding
/// vectors this contract produces are L2-normalized. A third normalization scheme is not a
/// case this workspace has a provider for yet.
///
// ponytail: 2 variants, not an open string — every real scheme observed across the OSS
// bootstrap provider set (§19 OSS: Alibaba Cloud v1) is one of these two; widen only against
// a real provider that needs a third.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Normalization {
    L2,
    None,
}

/// §19 Compatible Failover's five-axis identity: `same provider/model semantics, same model
/// revision compatibility, same dimension, same normalization, same projection version`.
/// Two contracts are failover-compatible **iff** every field compares equal
/// ([`decide_embedding_failover`]) — this struct carries no partial/fuzzy comparison, on
/// purpose: §19 draws no middle ground between "compatible failover" and "Projection
/// Migration".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionContract {
    pub provider_id: ProviderId,
    pub model_id: ModelId,
    pub model_revision: String,
    pub dimension: u32,
    pub normalization: Normalization,
    pub projection_version: String,
}

/// One axis [`decide_embedding_failover`] found mismatched between `current` and `candidate`.
/// §19 Incompatible Model Change names three trigger conditions verbatim (`model A -> model
/// B`, `dimension changes`, `normalization changes`) plus a compatibility qualifier on model
/// revision and an explicit projection-version axis; this enum keeps all five as distinct,
/// independently-reportable reasons rather than folding them into one boolean, so a caller (or
/// a 注错 test) can see exactly which axis broke compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncompatibilityReason {
    /// `model A -> model B` (§19 Incompatible Model Change) — provider and/or model identity
    /// differs, i.e. "same provider/model semantics" does not hold.
    ProviderModelSemanticsChanged,
    /// "same model revision compatibility" does not hold.
    ModelRevisionChanged,
    /// `dimension changes` (§19 Incompatible Model Change, verbatim).
    DimensionChanged,
    /// `normalization changes` (§19 Incompatible Model Change, verbatim).
    NormalizationChanged,
    /// "same projection version" does not hold.
    ProjectionVersionChanged,
}

impl fmt::Display for IncompatibilityReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::ProviderModelSemanticsChanged => "provider_model_semantics_changed",
            Self::ModelRevisionChanged => "model_revision_changed",
            Self::DimensionChanged => "dimension_changed",
            Self::NormalizationChanged => "normalization_changed",
            Self::ProjectionVersionChanged => "projection_version_changed",
        };
        f.write_str(s)
    }
}

/// [`decide_embedding_failover`]'s outcome. There is no third variant that lets a caller keep
/// writing the *current* collection under a *different* [`ProjectionContract`] — that is
/// exactly the shape §19 forbids (`qwen embedding failure -> switch unrelated embedding model
/// -> continue writing existing collection`). A caller holding
/// `RequiresProjectionMigration` has no path back to `CompatibleFailover` for the same pair;
/// it must instead drive the separate `create new ProjectionVersion -> backfill -> benchmark
/// -> shadow compare -> cutover -> retire old projection` workflow (§19), which is out of
/// this module's scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbeddingFailoverDecision {
    /// All five [`ProjectionContract`] axes match and the supplied [`EgressPermit`] is a live
    /// `RetrievalEmbedding` grant — `candidate` may serve requests for the same collection
    /// `current` was serving.
    CompatibleFailover,
    /// At least one axis differs — §19 Incompatible Model Change: "It is a Projection
    /// Migration, not a Failover." Never empty.
    RequiresProjectionMigration(Vec<IncompatibilityReason>),
}

/// §19 Embedding Provider Failover 硬规则's sole decision point: the only place in the
/// workspace that may declare two [`ProjectionContract`]s failover-compatible. A caller that
/// compares contracts itself and skips this function is exactly the 注错 shape this task's
/// task card names ("切到不同 dimension 的模型继续写旧 collection ⇒ 必须被拒").
///
/// # Errors
///
/// [`ErrorCode::Forbidden`](humaux_domain::error::ErrorCode::Forbidden) if `permit` was not
/// minted for [`PrivateDataPurpose::RetrievalEmbedding`], or has expired as of `now`.
///
/// ponytail: this only proves the permit is live and correctly-purposed — it does **not**
/// evaluate residency. §19 Compatible Failover's "仍需通过 Egress/Data Residency Policy" is
/// not satisfied by this function alone: [`ProjectionContract`] carries no region, and
/// `permit`'s `processor`/`tenant_id` are never compared against `candidate`, so a
/// `CompatibleFailover` verdict here can legitimately be the cross-region case residency
/// policy exists to constrain (§19's own worked example — "same managed model across
/// compatible endpoint/region" — is exactly that case). `domain::egress::authorize` does not
/// close this gap either (its own module doc records that it never consults
/// `control.egress_policies.private_retrieval_external_allowed`). Upgrade path: add a
/// `region: RegionId` axis to [`ProjectionContract`] and require it to be in the permit's/
/// tenant's allowed residency set before returning `CompatibleFailover`.
pub fn decide_embedding_failover(
    current: &ProjectionContract,
    candidate: &ProjectionContract,
    permit: &EgressPermit,
    now: std::time::Instant,
) -> Result<EmbeddingFailoverDecision, humaux_domain::error::ErrorCode> {
    use humaux_domain::error::ErrorCode;

    if permit.purpose() != PrivateDataPurpose::RetrievalEmbedding {
        return Err(ErrorCode::Forbidden);
    }
    if permit.is_expired(now) {
        return Err(ErrorCode::Forbidden);
    }

    Ok(projection_contracts_compatible(current, candidate))
}

/// The pure five-axis comparison at the heart of [`decide_embedding_failover`] — every caller
/// in the workspace that needs to compare two [`ProjectionContract`]s, permit-bearing or not,
/// MUST route through this function rather than hand-roll the field-by-field comparison itself
/// (`xtask architecture-check`'s embedding-failover sole-decision-point gate greps for a call
/// to this function or to [`decide_embedding_failover`] wherever [`crate::router::
/// ProjectionRef`] is used).
///
/// Split out of `decide_embedding_failover` rather than inlined there so `router::
/// projection_compatible` can reach the same comparison *without* an [`EgressPermit`]:
/// `router::resolve` runs before a route is chosen, and an `EgressPermit` cannot exist yet at
/// that point — minting one requires the [`humaux_domain::egress::ProcessorId`] a route
/// decision is what produces in the first place. `decide_embedding_failover` stays the sole
/// entry point for any caller that already holds a live permit (the real egress call site);
/// this function is the sole entry point for the structural pre-egress comparison itself —
/// there is still exactly one place that may declare two contracts compatible, just reached
/// through two permit-gated/permit-free doors.
pub fn projection_contracts_compatible(
    current: &ProjectionContract,
    candidate: &ProjectionContract,
) -> EmbeddingFailoverDecision {
    let mut reasons = Vec::new();
    if current.provider_id != candidate.provider_id || current.model_id != candidate.model_id {
        reasons.push(IncompatibilityReason::ProviderModelSemanticsChanged);
    }
    if current.model_revision != candidate.model_revision {
        reasons.push(IncompatibilityReason::ModelRevisionChanged);
    }
    if current.dimension != candidate.dimension {
        reasons.push(IncompatibilityReason::DimensionChanged);
    }
    if current.normalization != candidate.normalization {
        reasons.push(IncompatibilityReason::NormalizationChanged);
    }
    if current.projection_version != candidate.projection_version {
        reasons.push(IncompatibilityReason::ProjectionVersionChanged);
    }

    if reasons.is_empty() {
        EmbeddingFailoverDecision::CompatibleFailover
    } else {
        EmbeddingFailoverDecision::RequiresProjectionMigration(reasons)
    }
}

// ============================================================================
// §19 Rerank Provider Failover 规则 — calibration_profile binding + loud stand-down
// ============================================================================

/// §19: "任何相关性 Gate 必须绑定 `(provider, model, revision, calibration_profile)`". A
/// rerank relevance threshold gate is only ever evaluated against one of these, never against
/// a bare threshold number — see [`rerank_threshold_gate_state`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalibrationBinding {
    pub provider_id: ProviderId,
    pub model_id: ModelId,
    pub model_revision: String,
    pub calibration_profile: CalibrationProfileId,
}

/// [`rerank_threshold_gate_state`]'s outcome. There is deliberately no variant that carries a
/// live threshold number computed from any `calibration_profile` other than the one bound to
/// the *current* `(provider, model, revision)` — that would be exactly the 病 §19 names:
/// "模型改了、阈值仍沿用导致闸静默失效".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThresholdGateState {
    /// A completed calibration exists for the live `(provider, model, revision)` triple —
    /// ordinary relevance-threshold gating using `calibration_profile` applies.
    Calibrated {
        calibration_profile: CalibrationProfileId,
    },
    /// §19: "未完成 calibration：threshold gate MUST stand down loudly. 但仍允许 fallback
    /// ranking 返回." No threshold decision is made — `attempted_profile` is *some* prior
    /// calibration for this `(provider, model)` pair (if any), reported so a caller/operator
    /// can see *what* just went stale, not silently reuse it as if it still applied to
    /// `live_revision`. Not necessarily the *most recent* one: [`CalibrationBinding`] carries
    /// no timestamp and `known_calibrations` is an unordered slice, so this is whichever entry
    /// the caller happened to list first for the pair — widen with a `calibrated_at` field and
    /// pick by max if "most recent" ever needs to be a real guarantee.
    StandDownLoud {
        live_revision: String,
        attempted_profile: Option<CalibrationProfileId>,
    },
}

/// §19 Rerank Provider Failover 规则's sole decision point for whether a relevance threshold
/// gate may fire. `known_calibrations` is the caller-supplied set of completed calibrations
/// (the calibration *registry* is a later task's deliverable — out of this module's scope,
/// see module doc); this function makes no assumption about where that set came from beyond
/// "each entry names a `(provider, model, revision)` triple it was actually calibrated for".
///
/// Always logs at `warn` on the stand-down path — "loudly" (§19) is not satisfied by a return
/// value a caller might discard; `tracing::warn!` fires unconditionally whenever
/// `StandDownLoud` is produced, independent of what the caller does with the result.
pub fn rerank_threshold_gate_state(
    live: &CalibrationBinding,
    known_calibrations: &[CalibrationBinding],
) -> ThresholdGateState {
    let calibrated = known_calibrations.iter().any(|c| {
        c.provider_id == live.provider_id
            && c.model_id == live.model_id
            && c.model_revision == live.model_revision
            && c.calibration_profile == live.calibration_profile
    });
    if calibrated {
        return ThresholdGateState::Calibrated {
            calibration_profile: live.calibration_profile.clone(),
        };
    }

    // §19: report *a* prior revision this (provider, model) pair was calibrated for, if any —
    // this is what an operator would otherwise silently keep using. Not "the most recent" —
    // see this function's own rustdoc for why that claim would be false as written.
    let attempted_profile = known_calibrations
        .iter()
        .find(|c| c.provider_id == live.provider_id && c.model_id == live.model_id)
        .map(|c| c.calibration_profile.clone());

    tracing::warn!(
        target: "retrieval_provider::failover",
        provider = live.provider_id.0.as_str(),
        model = live.model_id.0.as_str(),
        revision = live.model_revision.as_str(),
        "rerank relevance threshold gate standing down: no completed calibration for this \
         (provider, model, revision); previous calibration_profile, if any, is NOT reused \
         (§19 loud stand-down)"
    );
    ThresholdGateState::StandDownLoud {
        live_revision: live.model_revision.clone(),
        attempted_profile,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_domain::dataclass::DataClass;
    use humaux_domain::egress::{AuthorizedEgressPayload, authorize};
    use humaux_domain::ids::TenantId;
    use std::time::{Duration, Instant};
    use uuid::Uuid;

    fn contract(dim: u32, norm: Normalization, version: &str) -> ProjectionContract {
        ProjectionContract {
            provider_id: ProviderId("dashscope".into()),
            model_id: ModelId("text-embedding-v4".into()),
            model_revision: "2026-08".to_string(),
            dimension: dim,
            normalization: norm,
            projection_version: version.to_string(),
        }
    }

    fn embedding_permit(purpose: PrivateDataPurpose, ttl: Duration) -> EgressPermit {
        let payload = AuthorizedEgressPayload::new(b"query".to_vec());
        authorize(
            TenantId::new(),
            humaux_domain::egress::ProcessorId(Uuid::now_v7()),
            purpose,
            DataClass::Private,
            &payload,
            ttl,
        )
        .expect("authorize succeeds for a non-SecretMaterial retrieval payload")
    }

    #[test]
    fn identical_contracts_are_a_compatible_failover() {
        let a = contract(1024, Normalization::L2, "v1");
        let b = contract(1024, Normalization::L2, "v1");
        let permit = embedding_permit(
            PrivateDataPurpose::RetrievalEmbedding,
            Duration::from_secs(60),
        );
        let decision = decide_embedding_failover(&a, &b, &permit, Instant::now()).unwrap();
        assert_eq!(decision, EmbeddingFailoverDecision::CompatibleFailover);
    }

    /// 注错 shape named in this task's own task card: switching to a model with a different
    /// dimension must never be classified as a failover that can keep writing the old
    /// collection.
    #[test]
    fn dimension_change_is_rejected_as_migration_not_failover() {
        let current = contract(1024, Normalization::L2, "v1");
        let different_dimension = contract(768, Normalization::L2, "v1");
        let permit = embedding_permit(
            PrivateDataPurpose::RetrievalEmbedding,
            Duration::from_secs(60),
        );
        let decision =
            decide_embedding_failover(&current, &different_dimension, &permit, Instant::now())
                .unwrap();
        match decision {
            EmbeddingFailoverDecision::RequiresProjectionMigration(reasons) => {
                assert!(reasons.contains(&IncompatibilityReason::DimensionChanged));
            }
            EmbeddingFailoverDecision::CompatibleFailover => {
                panic!("dimension change must never be classified as a compatible failover")
            }
        }
    }

    #[test]
    fn normalization_change_requires_migration() {
        let current = contract(1024, Normalization::L2, "v1");
        let candidate = contract(1024, Normalization::None, "v1");
        let permit = embedding_permit(
            PrivateDataPurpose::RetrievalEmbedding,
            Duration::from_secs(60),
        );
        let decision =
            decide_embedding_failover(&current, &candidate, &permit, Instant::now()).unwrap();
        assert_eq!(
            decision,
            EmbeddingFailoverDecision::RequiresProjectionMigration(vec![
                IncompatibilityReason::NormalizationChanged
            ])
        );
    }

    #[test]
    fn projection_version_change_requires_migration() {
        let current = contract(1024, Normalization::L2, "v1");
        let candidate = contract(1024, Normalization::L2, "v2");
        let permit = embedding_permit(
            PrivateDataPurpose::RetrievalEmbedding,
            Duration::from_secs(60),
        );
        let decision =
            decide_embedding_failover(&current, &candidate, &permit, Instant::now()).unwrap();
        assert_eq!(
            decision,
            EmbeddingFailoverDecision::RequiresProjectionMigration(vec![
                IncompatibilityReason::ProjectionVersionChanged
            ])
        );
    }

    /// Even a fully-compatible pair must still pass Egress/Data Residality Policy (§19
    /// Compatible Failover: "仍需通过 Egress/Data Residency Policy").
    #[test]
    fn compatible_pair_with_wrong_purpose_permit_is_rejected() {
        let a = contract(1024, Normalization::L2, "v1");
        let b = contract(1024, Normalization::L2, "v1");
        let wrong_purpose_permit =
            embedding_permit(PrivateDataPurpose::RetrievalRerank, Duration::from_secs(60));
        let result = decide_embedding_failover(&a, &b, &wrong_purpose_permit, Instant::now());
        assert_eq!(result, Err(humaux_domain::error::ErrorCode::Forbidden));
    }

    #[test]
    fn compatible_pair_with_expired_permit_is_rejected() {
        let a = contract(1024, Normalization::L2, "v1");
        let b = contract(1024, Normalization::L2, "v1");
        let permit = embedding_permit(
            PrivateDataPurpose::RetrievalEmbedding,
            Duration::from_millis(0),
        );
        std::thread::sleep(Duration::from_millis(5));
        let result = decide_embedding_failover(&a, &b, &permit, Instant::now());
        assert_eq!(result, Err(humaux_domain::error::ErrorCode::Forbidden));
    }

    fn binding(revision: &str, profile: &str) -> CalibrationBinding {
        binding_on("dashscope", "gte-rerank-v2", revision, profile)
    }

    /// `binding` 的四轴版本。**存在的理由**：`binding` 把 `provider_id`/`model_id` 写死了，
    /// 于是 §19 那条四合取判定里只有 `model_revision`/`calibration_profile` 两轴被测过——
    /// 删掉 `c.provider_id == live.provider_id` 或 `c.model_id == live.model_id` 任一项，
    /// 本模块此前的每一条测试都照样绿。DOD-022 要求的正是「provider/model **变化**绑定新
    /// 的 calibration profile」，而变化的那两轴恰恰是没被覆盖的两轴。
    fn binding_on(
        provider: &str,
        model: &str,
        revision: &str,
        profile: &str,
    ) -> CalibrationBinding {
        CalibrationBinding {
            provider_id: ProviderId(provider.into()),
            model_id: ModelId(model.into()),
            model_revision: revision.to_string(),
            calibration_profile: CalibrationProfileId(profile.into()),
        }
    }

    /// DOD-022 第一轴：**换了 provider** 就不能沿用旧 provider 的 calibration。
    /// 注错：删掉判定里的 `c.provider_id == live.provider_id` ⇒ 本条必红。
    #[test]
    fn a_different_provider_is_not_calibrated_by_the_old_providers_profile() {
        let live = binding_on("minimax", "gte-rerank-v2", "2026-08", "profile-a");
        // 其余三轴逐字相同——只有 provider 变了。这样才能证明红的原因是那一轴，
        // 而不是"反正有什么不一样"。
        let known = vec![binding_on(
            "dashscope",
            "gte-rerank-v2",
            "2026-08",
            "profile-a",
        )];
        assert!(
            matches!(
                rerank_threshold_gate_state(&live, &known),
                ThresholdGateState::StandDownLoud { .. }
            ),
            "换 provider 之后沿用旧 calibration 会让阈值门用错分布"
        );
    }

    /// DOD-022 第二轴：**换了 model** 同理。
    /// 注错：删掉 `c.model_id == live.model_id` ⇒ 本条必红。
    #[test]
    fn a_different_model_is_not_calibrated_by_the_old_models_profile() {
        let live = binding_on("dashscope", "qwen3-rerank", "2026-08", "profile-a");
        let known = vec![binding_on(
            "dashscope",
            "gte-rerank-v2",
            "2026-08",
            "profile-a",
        )];
        assert!(
            matches!(
                rerank_threshold_gate_state(&live, &known),
                ThresholdGateState::StandDownLoud { .. }
            ),
            "换 model 之后沿用旧 calibration 会让阈值门用错分布"
        );
    }

    /// 正对照：四轴逐字相同才算 calibrated。没有这条的话，上面两条也可能因为
    /// 「判定恒返回 StandDownLoud」而绿——那同样是假绿，只是方向相反。
    #[test]
    fn all_four_axes_matching_is_what_makes_it_calibrated() {
        let live = binding_on("minimax", "qwen3-rerank", "2026-09", "profile-b");
        let known = vec![
            binding_on("dashscope", "qwen3-rerank", "2026-09", "profile-b"), // provider 不同
            binding_on("minimax", "gte-rerank-v2", "2026-09", "profile-b"),  // model 不同
            binding_on("minimax", "qwen3-rerank", "2026-08", "profile-b"),   // revision 不同
            binding_on("minimax", "qwen3-rerank", "2026-09", "profile-a"),   // profile 不同
            binding_on("minimax", "qwen3-rerank", "2026-09", "profile-b"),   // 四轴全同
        ];
        assert_eq!(
            rerank_threshold_gate_state(&live, &known),
            ThresholdGateState::Calibrated {
                calibration_profile: CalibrationProfileId("profile-b".into())
            },
            "四轴全同的那一条在集合里，必须判 Calibrated"
        );
    }

    #[test]
    fn calibrated_revision_gates_normally() {
        let live = binding("2026-08", "profile-a");
        let known = vec![binding("2026-08", "profile-a")];
        assert_eq!(
            rerank_threshold_gate_state(&live, &known),
            ThresholdGateState::Calibrated {
                calibration_profile: CalibrationProfileId("profile-a".into())
            }
        );
    }

    /// 注错 shape named in this task's own task card: swapping to a new revision must never
    /// silently keep gating with the old revision's threshold.
    #[test]
    fn uncalibrated_revision_stands_down_loudly_instead_of_reusing_old_profile() {
        let live = binding("2026-09", "profile-a"); // new revision, profile not (yet) valid for it
        let known = vec![binding("2026-08", "profile-a")]; // only the old revision was calibrated
        let state = rerank_threshold_gate_state(&live, &known);
        match state {
            ThresholdGateState::StandDownLoud {
                live_revision,
                attempted_profile,
            } => {
                assert_eq!(live_revision, "2026-09".to_string());
                assert_eq!(
                    attempted_profile,
                    Some(CalibrationProfileId("profile-a".into()))
                );
            }
            ThresholdGateState::Calibrated { .. } => {
                panic!("an uncalibrated revision must never silently gate as Calibrated")
            }
        }
    }

    #[test]
    fn never_calibrated_pair_stands_down_with_no_attempted_profile() {
        let live = binding("2026-09", "profile-z");
        let state = rerank_threshold_gate_state(&live, &[]);
        assert_eq!(
            state,
            ThresholdGateState::StandDownLoud {
                live_revision: "2026-09".to_string(),
                attempted_profile: None,
            }
        );
    }

    // -- §80.1 loudness gate: the `warn!` on the StandDownLoud path must actually fire --------
    //
    // Minimal hand-rolled `tracing::Subscriber` rather than pulling in `tracing-subscriber`/
    // `tracing-test` (neither is a workspace dependency, and `tracing` itself — already a
    // dependency — is enough to count one event): ladder rung 5, use what is already
    // installed before adding anything.

    struct WarnEventCounter(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl tracing::Subscriber for WarnEventCounter {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            if *event.metadata().level() == tracing::Level::WARN
                && event.metadata().target() == "retrieval_provider::failover"
            {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}
    }

    /// 注错 shape: strip the `tracing::warn!` out of the `StandDownLoud` path and this test
    /// goes red (count stays 0) — proving the "loudly" half of §19's requirement actually has
    /// a check, not just a return-value assertion (§80.1 "一道闸没有注错红转绿记录就不算存在").
    #[test]
    fn stand_down_loud_actually_emits_a_warn_event() {
        let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let subscriber = WarnEventCounter(count.clone());
        let live = binding("2026-09", "profile-a");
        let known = vec![binding("2026-08", "profile-a")];

        tracing::subscriber::with_default(subscriber, || {
            let state = rerank_threshold_gate_state(&live, &known);
            assert!(matches!(state, ThresholdGateState::StandDownLoud { .. }));
        });

        assert_eq!(
            count.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "StandDownLoud must emit exactly one warn! event at target \
             retrieval_provider::failover (§19 loud stand-down)"
        );
    }

    /// 正向对照: the `Calibrated` path emits no such warning.
    #[test]
    fn calibrated_path_emits_no_warn_event() {
        let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let subscriber = WarnEventCounter(count.clone());
        let live = binding("2026-08", "profile-a");
        let known = vec![binding("2026-08", "profile-a")];

        tracing::subscriber::with_default(subscriber, || {
            let state = rerank_threshold_gate_state(&live, &known);
            assert!(matches!(state, ThresholdGateState::Calibrated { .. }));
        });

        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}
