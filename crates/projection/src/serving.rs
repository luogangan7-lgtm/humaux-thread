//! `projection::serving` — §16.2 换代期读路由 identity + §16.3 无裁量切换判据 (T5.2/T5.3).
//! Depends-on: crates=[humaux-domain, uuid]; services=[]; env=[]; modules=[domain::ids, projection::stream]
//! Called-by: [adapters::context_repo, adapters::private_projection_registry, adapters::projection_worker, adapters::read_materialize, adapters::retrieve, adapters::serving_repo, gateway::context, gateway::memory, retrieval-worker::main, tests, xtask::projection_serve, xtask::soak, xtask::switch_visible]
//! Invariants: [pure half of the serving switch: the three-criteria evaluator never reuses one query result for two
//!   independent checks; the SQL and the role_maintenance-only UPDATE live in adapters::serving_repo]
//! Spec: Baseline §6.2.2
//!
//! Same split as `projection::stream` / `adapters::stream_repo`: this module holds the pure
//! (no-IO) half — the stream-family key and the three-criteria switch evaluator — so the
//! "无裁量口" arithmetic is unit-testable without a database and cannot itself be tempted into
//! reusing a query result across two supposedly-independent checks. The SQL (`serving_version`
//! read, the atomic switch `UPDATE`) lives in `humaux_adapters::serving_repo` against
//! `&RetrievalWorkerDbPool` / `&MaintenanceDbPool` (§6.2.2 grant matrix: only `role_maintenance`
//! holds `UPDATE(serving, shadow)` on `projection.stream_checkpoints`).

use humaux_domain::ids::TenantId;
use uuid::Uuid;

use crate::stream::StreamKey;

/// §15.1's six-column key minus `projection_version` — the identity `ux_serving_one` (§16.2)
/// constrains to at most one `serving = true` row: `(tenant_id, scope_kind, scope_id, domain,
/// projection_kind)`. `projection_version` is deliberately excluded here — it is the column
/// the family can hold *multiple* rows for (one `serving`, at most one `shadow`, and any
/// number of retired non-serving rows kept as rollback targets, §16.3).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamFamily {
    pub tenant_id: TenantId,
    pub scope_kind: String,
    pub scope_id: Uuid,
    pub domain: String,
    pub projection_kind: String,
}

impl StreamFamily {
    /// Builds a family from its five identity columns.
    pub fn new(
        tenant_id: TenantId,
        scope_kind: impl Into<String>,
        scope_id: Uuid,
        domain: impl Into<String>,
        projection_kind: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id,
            scope_kind: scope_kind.into(),
            scope_id,
            domain: domain.into(),
            projection_kind: projection_kind.into(),
        }
    }

    /// Composes a full six-column [`StreamKey`] by attaching one `projection_version` to this
    /// family — the one place the switch/serving-read call sites build a `StreamKey` from a
    /// family + a version string, so that composition isn't hand-rolled differently at each
    /// call site.
    pub fn with_version(&self, projection_version: impl Into<String>) -> StreamKey {
        StreamKey::new(
            self.tenant_id,
            self.scope_kind.clone(),
            self.scope_id,
            self.domain.clone(),
            self.projection_kind.clone(),
            projection_version,
        )
    }
}

/// §16.3 判据③'s benchmark comparison, in the shape the spec names verbatim: "判据形态取 §69
/// Continuation Gate 的 FAIL 侧". Four states, not two — `Fail` is the only state that *proves*
/// degradation; `Inconclusive` and `CannotEstablish` both mean "not proven", and per §69
/// ("INCONCLUSIVE ... 不得表述为『不劣于基线』") neither may be read as license to switch.
///
/// **Current real value**: `continuation_198_v2`'s `frozen_by` / `baseline_min` are not yet
/// written (§69 Benchmark 集合分母声明 table: `NOT_DECLARED`, missing `frozen_by`) — until that
/// lands, no caller can honestly produce anything but [`ContinuationVerdict::CannotEstablish`]
/// for this field. There is no stub "always Pass" path here; wiring the real `§55`/`§69`
/// benchmark harness is out of this task's scope (T5.2/T5.3 only owns §16.2/§16.3's read
/// routing and switch arithmetic).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuationVerdict {
    /// `new_min >= baseline_min + Δ`, gate self-check passed (§69 `PASS`).
    Pass,
    /// `baseline_min - new_min >= Δ` — proven degraded (§69 `FAIL`).
    Fail,
    /// Neither `PASS` nor `FAIL` condition reached — §69 `INCONCLUSIVE`, "不得勾选，不得表述为
    /// 『不劣于基线』".
    Inconclusive,
    /// Gate's own preconditions unmet (`baseline_min`/`frozen_by`/`spread_tol` missing, or
    /// declaration `NOT_DECLARED` per §55.3) — §69 `cannot_establish`.
    CannotEstablish,
}

/// §16.3's three independently-taken inputs to [`evaluate_switch`]. None of these are computed
/// by this module — that is the entire point of "无裁量口": the *inputs* come from real
/// measurements (Qdrant `visible` counts per §23.1②, `projection.processing_gaps` per §15.1,
/// the §69 Continuation Gate) taken by the caller, and this type only pins their shape so the
/// evaluator can't quietly accept a partial or substituted set.
#[derive(Debug, Clone)]
pub struct SwitchCriteria {
    /// §23.1②'s `visible` value for the **shadow** side, tagged with the `projection_version`
    /// it was counted against: Qdrant count filtered by `tenant + scope + projection_version =
    /// <shadow version>`, minus the tombstone overlay. `None` when the index count could not
    /// be taken (§23.1②: "索引 count 取不到时输出 `visible: null`" — the same rule applies
    /// here, not only to the envelope; a missing count is never backfilled from another
    /// number). The version tag is what lets [`evaluate_switch`] catch a caller that
    /// (accidentally or not) counted the *same* `projection_version` on both sides — see
    /// [`SwitchRejection::VisibleSameVersionDeclared`].
    pub visible_shadow: Option<(String, u64)>,
    /// Same computation as `visible_shadow`, filtered by `projection_version = <serving
    /// version>` instead — the spec's own warning is that this and `visible_shadow` must
    /// differ **only** in that one filter, taken at the same instant (§16.3, §23.1②); this
    /// type cannot enforce "same instant" across an IO boundary, but the version tag lets it
    /// refuse a declared-but-not-actually-distinct pair, and lets the DB-layer caller
    /// (`adapters::serving_repo::switch_projection_version`) cross-check the tag against the
    /// real DB version before trusting the count at all.
    pub visible_serving: Option<(String, u64)>,
    /// `count(*)` from `projection.processing_gaps` (§15.1's sole gap-count source) filtered to
    /// the **shadow** version's full [`StreamKey`] — §16.3's second criterion,
    /// `shadow.open_gaps == 0`.
    /// §16.2 first activation (ADR-0017): the family has NO serving version yet, so there is
    /// nothing to compare the shadow against — the shadow read-back and zero open gaps are the
    /// whole criterion, and criterion ③'s comparison has no second operand either (see
    /// [`evaluate_switch`]; a *proven* `Fail` still refuses). Derived by the DB layer from the
    /// checkpoint rows, never declared by a caller.
    pub first_activation: bool,
    pub shadow_open_gaps: u64,
    /// §16.3's third criterion input — see [`ContinuationVerdict`]'s doc for why this is a
    /// 4-state enum, not a bool.
    pub continuation: ContinuationVerdict,
}

/// One of §16.3's three criteria failed to hold. [`evaluate_switch`] collects every failing
/// reason (not just the first) so a caller/log/test can see exactly which of the three "无裁量
/// 口" gates blocked the switch — useful both for the G80-28 恒真闸 regression (below) and for
/// an operator reading a rejected-switch log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwitchRejection {
    /// Either side's `visible` count was unavailable (`None`), so criterion ① cannot be
    /// evaluated at all — treated as a rejection, never silently skipped or backfilled.
    VisibleUnavailable,
    /// Both counts were available but unequal — `visible(shadow) != visible(serving)`.
    VisibleMismatch,
    /// Both sides declared the *same* `projection_version` — i.e. `visible_shadow` and
    /// `visible_serving` were (mistakenly or maliciously) counted against one identical
    /// version instead of the shadow/serving pair the spec requires. Comparing a count to
    /// itself always holds, which is exactly the 恒真闸 (always-true gate) §16.3 warns about
    /// — this rejects it at the type level rather than trusting equal counts alone.
    VisibleSameVersionDeclared,
    /// A declared `visible_*` tag names a `projection_version` that does not match the DB's
    /// actual shadow/serving version for this family at switch time (checked only by the
    /// DB-layer caller, `adapters::serving_repo::switch_projection_version`, which is the only
    /// place that can see "actual" — this pure evaluator never sets this variant itself).
    VisibleVersionMismatch,
    /// `shadow.open_gaps != 0`.
    OpenGaps,
    /// The §69 Continuation Gate did not return `Pass` (§16.3's判据③: `Fail` proves
    /// degradation; `Inconclusive` / `CannotEstablish` are both "not proven not-worse" and per
    /// §69 must not be read as a pass — see [`ContinuationVerdict`]'s doc).
    BenchmarkNotPass,
}

/// §16.3's frozen judgement: "三条全真才允许切换，任一为假直接拒绝，不存在『人工判断可以上』的
/// 分支". A pure function on already-taken measurements is itself the enforcement of "no
/// discretion branch" — there is no parameter here through which a caller could override a
/// failing criterion, and every one of the three is checked (never short-circuited), so a
/// caller inspecting `Err` sees the complete failing set, not just the first.
///
/// §16.3 gate ("恒真闸检测", G80-28 per this task's card): injecting "shadow 少回填 1 个点"
/// (`visible_shadow` one less than `visible_serving`, all else held equal) must flip criterion
/// ① from held to failed and the overall result from `Ok` to `Err` — pinned by
/// `evaluate_switch_flips_red_when_shadow_is_missing_one_point` below. An implementation that
/// only checks e.g. `shadow_open_gaps` and `continuation` while ignoring the `visible` pair
/// would stay `Ok` under that injection — that is exactly the "永远不会拒绝任何切换的闸" §16.3
/// warns about, and the reason this function takes the two `visible_*` counts as separate
/// fields rather than a pre-reduced `bool`.
///
/// **G80-28 status (honest, not "已过")**: the test below injects the shortfall by mutating
/// `SwitchCriteria.visible_shadow` directly, one layer below where the real defect could occur
/// (a `count()` query built without a `projection_version` filter). It is a genuine positive
/// control for *this* pure function, but it cannot catch a future `visible` implementation
/// that ignores the filter and still happens to return equal numbers by construction — G80-28
/// proper is `not_applicable` until a real Qdrant-backed `visible` counter exists
/// (`adapters::qdrant` is still the T0.x placeholder) and the injection point moves to "really
/// delete one point from the shadow version in Qdrant".
pub fn evaluate_switch(criteria: &SwitchCriteria) -> Result<(), Vec<SwitchRejection>> {
    let mut rejections = Vec::new();

    match (&criteria.visible_shadow, &criteria.visible_serving) {
        (Some(_), None) if criteria.first_activation => {}
        (Some((shadow_version, _)), Some((serving_version, _)))
            if shadow_version == serving_version =>
        {
            // §23.1②/§16.3: a genuine comparison requires two *different* versions. Equal
            // counts here would always agree with themselves — the 恒真闸 shape — so this is
            // rejected before the counts are even looked at.
            rejections.push(SwitchRejection::VisibleSameVersionDeclared);
        }
        (Some((_, shadow_count)), Some((_, serving_count))) if shadow_count == serving_count => {}
        (Some(_), Some(_)) => rejections.push(SwitchRejection::VisibleMismatch),
        _ => rejections.push(SwitchRejection::VisibleUnavailable),
    }

    if criteria.shadow_open_gaps != 0 {
        rejections.push(SwitchRejection::OpenGaps);
    }

    // §16.3 criterion ③ is a comparison — "benchmark(shadow) 未被证伪劣化于 benchmark(serving)"
    // (Baseline_2.9.md:4046). ADR-0017 first activation: the family has no serving version, so
    // there is no `benchmark(serving)` to compare against and no baseline the §69 gate could have
    // been run on. That is the SAME missing second operand criterion ① is already exempted for
    // ten lines above; leaving it in force meant `continuation_198_v2` (NOT_DECLARED, §69:12477 —
    // "写入前 Continuation Gate 输出 cannot_establish") refused every fresh tenant's FIRST
    // promotion forever, which is how soak25/26/27 reported `BenchmarkNotPass` on a first
    // activation that had nothing to be worse than.
    //
    // A proven `Fail` still refuses even here, and that is not decoration: it is the injection
    // that keeps this branch observable (`first_activation_still_refuses_a_proven_degradation`).
    // A criterion no input can ever fail is not a criterion (§80.1).
    //
    // Everything OFF the first-activation path is untouched: with a serving version present the
    // gate still requires `Pass`, so `Inconclusive` / `CannotEstablish` keep refusing exactly as
    // §69 ("INCONCLUSIVE ... 不得表述为『不劣于基线』") and the earlier card that pinned this
    // required.
    let benchmark_refused = if criteria.first_activation {
        criteria.continuation == ContinuationVerdict::Fail
    } else {
        criteria.continuation != ContinuationVerdict::Pass
    };
    if benchmark_refused {
        rejections.push(SwitchRejection::BenchmarkNotPass);
    }

    if rejections.is_empty() {
        Ok(())
    } else {
        Err(rejections)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn family() -> StreamFamily {
        StreamFamily::new(
            TenantId::new(),
            "workspace",
            Uuid::now_v7(),
            "code",
            "retrieval_card",
        )
    }

    fn all_true() -> SwitchCriteria {
        SwitchCriteria {
            visible_shadow: Some(("v2".to_string(), 10)),
            visible_serving: Some(("v1".to_string(), 10)),
            first_activation: false,
            shadow_open_gaps: 0,
            continuation: ContinuationVerdict::Pass,
        }
    }

    #[test]
    fn first_activation_accepts_a_shadow_read_back_without_a_serving_version() {
        let criteria = SwitchCriteria {
            visible_serving: None,
            first_activation: true,
            ..all_true()
        };
        assert_eq!(evaluate_switch(&criteria), Ok(()));
    }

    #[test]
    fn first_activation_still_needs_the_shadow_read_back_and_no_open_gaps() {
        let no_shadow = SwitchCriteria {
            visible_shadow: None,
            visible_serving: None,
            first_activation: true,
            ..all_true()
        };
        assert_eq!(
            evaluate_switch(&no_shadow),
            Err(vec![SwitchRejection::VisibleUnavailable])
        );
        let gaps = SwitchCriteria {
            visible_serving: None,
            first_activation: true,
            shadow_open_gaps: 1,
            ..all_true()
        };
        assert_eq!(evaluate_switch(&gaps), Err(vec![SwitchRejection::OpenGaps]));
    }

    #[test]
    fn a_missing_serving_version_without_first_activation_is_still_unavailable() {
        let criteria = SwitchCriteria {
            visible_serving: None,
            ..all_true()
        };
        assert_eq!(
            evaluate_switch(&criteria),
            Err(vec![SwitchRejection::VisibleUnavailable])
        );
    }

    #[test]
    fn stream_family_with_version_composes_a_stream_key() {
        let f = family();
        let key = f.with_version("v2");
        assert_eq!(key.tenant_id, f.tenant_id);
        assert_eq!(key.scope_kind, f.scope_kind);
        assert_eq!(key.scope_id, f.scope_id);
        assert_eq!(key.domain, f.domain);
        assert_eq!(key.projection_kind, f.projection_kind);
        assert_eq!(key.projection_version, "v2");
    }

    /// Positive control: three criteria all true ⇒ `Ok(())`, no rejection reasons at all.
    #[test]
    fn evaluate_switch_ok_when_all_three_criteria_hold() {
        assert_eq!(evaluate_switch(&all_true()), Ok(()));
    }

    /// §16.3 gate / G80-28 恒真闸检测: baseline has `visible_shadow == visible_serving`
    /// (criterion ① holds, switch would be `Ok`). Inject "shadow 少回填 1 个点" — `visible_shadow`
    /// drops by exactly one, everything else held bit-for-bit identical — and criterion ① must
    /// flip from held to failed, flipping the overall result from `Ok` to `Err` containing
    /// `VisibleMismatch`. An evaluator that (bug) ignores the `visible_*` pair would stay `Ok`
    /// here — that non-observation is precisely the "恒真闸" this test exists to catch; if this
    /// assertion is ever weakened to allow that, the injection stops being detectable.
    #[test]
    fn evaluate_switch_flips_red_when_shadow_is_missing_one_point() {
        let baseline = all_true();
        assert_eq!(
            evaluate_switch(&baseline),
            Ok(()),
            "baseline must be a genuine positive control before the injection is meaningful"
        );

        let mut injected = baseline.clone();
        let (version, count) = baseline.visible_shadow.unwrap();
        injected.visible_shadow = Some((version, count - 1));

        let result = evaluate_switch(&injected);
        assert_ne!(
            result,
            Ok(()),
            "criterion ① must flip real ⇒ false under a one-point shadow shortfall, not stay Ok \
             (a still-Ok result here is the always-true gate §16.3 warns about)"
        );
        assert_eq!(result, Err(vec![SwitchRejection::VisibleMismatch]));
    }

    /// §23.1②/§16.3 恒真闸 (second shape): both sides declared the *same* `projection_version`
    /// — a caller (bug, or a hand-forged criteria) counting one version and reusing it for
    /// both fields. Equal counts here would always pass criterion ① regardless of the actual
    /// numbers — this must be rejected on the version tag alone, before comparing counts.
    #[test]
    fn evaluate_switch_rejects_when_both_sides_declare_the_same_version() {
        let c = SwitchCriteria {
            visible_shadow: Some(("v1".to_string(), 10)),
            visible_serving: Some(("v1".to_string(), 10)),
            first_activation: false,
            shadow_open_gaps: 0,
            continuation: ContinuationVerdict::Pass,
        };
        assert_eq!(
            evaluate_switch(&c),
            Err(vec![SwitchRejection::VisibleSameVersionDeclared]),
            "identical declared versions must never be trusted as a real shadow/serving \
             comparison, even with equal counts"
        );
    }

    #[test]
    fn evaluate_switch_rejects_when_visible_unavailable_on_either_side() {
        let mut c = all_true();
        c.visible_shadow = None;
        assert_eq!(
            evaluate_switch(&c),
            Err(vec![SwitchRejection::VisibleUnavailable])
        );

        let mut c2 = all_true();
        c2.visible_serving = None;
        assert_eq!(
            evaluate_switch(&c2),
            Err(vec![SwitchRejection::VisibleUnavailable])
        );
    }

    /// ADR-0040 / card 18's folded debt, stated at this module's own boundary: the serve
    /// switch's criterion ① is `VisibleUnavailable` **only** while a count is genuinely
    /// missing. Feed the pair the two live §23.1② counts the ops path now takes
    /// (`xtask::switch_visible` → `adapters::retrieve::visible_count_of_version`, the same
    /// producer the three read routes use) — the candidate version on the shadow side, the
    /// family's serving version on the other — and that reason must be gone, while §16.3's
    /// other two criteria are judged exactly as before. Take either count away again and the
    /// refusal must come back: §23.1②'s `visible: null` is never backfilled.
    #[test]
    fn a_taken_visible_count_clears_visible_unavailable_and_a_missing_one_restores_it() {
        // A real promotion: candidate `v2` counted against serving `v1`, equal.
        let taken = SwitchCriteria {
            visible_shadow: Some(("v2".to_string(), 100)),
            visible_serving: Some(("v1".to_string(), 100)),
            first_activation: false,
            shadow_open_gaps: 0,
            continuation: ContinuationVerdict::CannotEstablish,
        };
        assert_eq!(
            evaluate_switch(&taken),
            Err(vec![SwitchRejection::BenchmarkNotPass]),
            "with both counts taken, the only surviving refusal is §69's undeclared baseline \
             (card 20) — criterion ① must not still report VisibleUnavailable"
        );

        // ADR-0017 first activation: the candidate read-back alone is criterion ①, and with no
        // serving version there is no benchmark to be worse than either — so this now holds
        // outright. That is card 20's folded debt 2: the same criteria refused `BenchmarkNotPass`
        // before, on a promotion that had no baseline in the first place.
        let first = SwitchCriteria {
            visible_serving: None,
            first_activation: true,
            ..taken.clone()
        };
        assert_eq!(evaluate_switch(&first), Ok(()));

        // Either side missing on a non-first activation is still a refusal, both directions.
        for missing in [
            SwitchCriteria {
                visible_shadow: None,
                ..taken.clone()
            },
            SwitchCriteria {
                visible_serving: None,
                ..taken.clone()
            },
        ] {
            assert!(
                evaluate_switch(&missing)
                    .unwrap_err()
                    .contains(&SwitchRejection::VisibleUnavailable),
                "an untaken count must stay VisibleUnavailable, never be read as 0"
            );
        }
    }

    #[test]
    fn evaluate_switch_rejects_on_open_gaps() {
        let mut c = all_true();
        c.shadow_open_gaps = 1;
        assert_eq!(evaluate_switch(&c), Err(vec![SwitchRejection::OpenGaps]));
    }

    /// §55.4/§69: threshold not yet frozen ⇒ `CannotEstablish` ⇒ criterion ③ must reject, not
    /// pass — "该条输出 cannot_establish 并拒绝切换，不得当通过" (this task's own card text).
    #[test]
    fn evaluate_switch_rejects_when_benchmark_cannot_establish() {
        let mut c = all_true();
        c.continuation = ContinuationVerdict::CannotEstablish;
        assert_eq!(
            evaluate_switch(&c),
            Err(vec![SwitchRejection::BenchmarkNotPass])
        );
    }

    /// §69 "INCONCLUSIVE ... 不得表述为『不劣于基线』" — must reject exactly like
    /// `CannotEstablish`, not be treated as a soft pass.
    #[test]
    fn evaluate_switch_rejects_when_benchmark_inconclusive() {
        let mut c = all_true();
        c.continuation = ContinuationVerdict::Inconclusive;
        assert_eq!(
            evaluate_switch(&c),
            Err(vec![SwitchRejection::BenchmarkNotPass])
        );
    }

    /// Card 20 folded debt 2, the positive half: on a FIRST activation §69's undeclared baseline
    /// (`CannotEstablish`) and an `Inconclusive` run are both "there is no baseline", not
    /// "shadow is worse" — criterion ③ has no second operand, exactly like criterion ①. Before
    /// this, `continuation_198_v2` being NOT_DECLARED (§69) made a fresh tenant's first promotion
    /// unreachable by construction, and the soak reported `BenchmarkNotPass` on switches that had
    /// in fact succeeded through `xtask projection-serve`'s own declared verdict.
    #[test]
    fn first_activation_needs_no_benchmark_because_there_is_no_baseline() {
        for verdict in [
            ContinuationVerdict::CannotEstablish,
            ContinuationVerdict::Inconclusive,
            ContinuationVerdict::Pass,
        ] {
            let c = SwitchCriteria {
                visible_serving: None,
                first_activation: true,
                continuation: verdict,
                ..all_true()
            };
            assert_eq!(evaluate_switch(&c), Ok(()), "first activation, {verdict:?}");
        }
    }

    /// …and the injection that keeps the exemption above from being a gate that can never fire:
    /// a *proven* degradation (§69 `FAIL`) refuses a first activation too. If this ever goes
    /// green, criterion ③ has stopped being a criterion on that path (§80.1).
    #[test]
    fn first_activation_still_refuses_a_proven_degradation() {
        let c = SwitchCriteria {
            visible_serving: None,
            first_activation: true,
            continuation: ContinuationVerdict::Fail,
            ..all_true()
        };
        assert_eq!(
            evaluate_switch(&c),
            Err(vec![SwitchRejection::BenchmarkNotPass])
        );
    }

    /// The exemption is scoped to the first activation and nothing else: with a serving version
    /// present there IS a `benchmark(serving)`, so `CannotEstablish` and `Inconclusive` keep
    /// refusing (the two tests above this one) — pinned here as one explicit pair so a future
    /// widening of the `first_activation` branch cannot quietly take the normal path with it.
    #[test]
    fn a_real_promotion_still_requires_the_benchmark_to_pass() {
        for verdict in [
            ContinuationVerdict::CannotEstablish,
            ContinuationVerdict::Inconclusive,
            ContinuationVerdict::Fail,
        ] {
            let c = SwitchCriteria {
                continuation: verdict,
                ..all_true()
            };
            assert_eq!(
                evaluate_switch(&c),
                Err(vec![SwitchRejection::BenchmarkNotPass]),
                "non-first activation, {verdict:?}"
            );
        }
    }

    #[test]
    fn evaluate_switch_rejects_when_benchmark_fails() {
        let mut c = all_true();
        c.continuation = ContinuationVerdict::Fail;
        assert_eq!(
            evaluate_switch(&c),
            Err(vec![SwitchRejection::BenchmarkNotPass])
        );
    }

    /// All three criteria false at once ⇒ every rejection reason present, not just the first
    /// — §16.3 "任一为假直接拒绝" does not mean "stop checking after the first failure" for
    /// this pure evaluator (the adapter layer never proceeds past a non-empty `Err` either
    /// way, but the full reason set matters for operator-facing logs).
    #[test]
    fn evaluate_switch_reports_every_failing_criterion_at_once() {
        let c = SwitchCriteria {
            visible_shadow: Some(("v2".to_string(), 9)),
            visible_serving: Some(("v1".to_string(), 10)),
            first_activation: false,
            shadow_open_gaps: 3,
            continuation: ContinuationVerdict::Fail,
        };
        let err = evaluate_switch(&c).unwrap_err();
        assert_eq!(
            err,
            vec![
                SwitchRejection::VisibleMismatch,
                SwitchRejection::OpenGaps,
                SwitchRejection::BenchmarkNotPass,
            ]
        );
    }
}
