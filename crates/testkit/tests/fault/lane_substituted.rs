//! `testkit::tests::fault::lane_substituted` — §53.4 注错测试：`DegradeCode::LaneSubstituted` 的一条注错记录（每个 reason
//!   必须有一条测试，从未触发过的 reason 不许合并，§53.3 规则2 的三数相等 把这句话变成 CI 事实）。
//! Depends-on: crates=[humaux-telemetry]; services=[]; env=[]; modules=[humaux-testkit, telemetry::degrade]
//! Called-by: [testkit::tests::fault_main]
//! Invariants: []
//! Spec: Baseline §53.4; §53.1; ADR-0055
//!
//! planner 把查询路由到未交付的 lane、调用方未指定 `mode`，dense lane 代答（ADR-0055 D-C）。
//! 真实触发点是 `retrieval::envelope::dense_lane_substitution`，它本身只调 `abstain()`；
//! 注入手段与同目录其余文件一致：直接调用 §53.1 全仓唯一出口点。

use humaux_telemetry::degrade::{DegradeCode, abstain, degrade_total_count};
use humaux_testkit::{FaultObservation, assert_fault_observed};

#[test]
fn lane_substituted_increments_counter_and_reports_both_forms() {
    let before = degrade_total_count(DegradeCode::LaneSubstituted);
    let outcome = abstain(DegradeCode::LaneSubstituted, ());
    let after = degrade_total_count(DegradeCode::LaneSubstituted);

    let degradations_wire: Vec<String> = outcome
        .degradations
        .iter()
        .map(|c| c.line_format())
        .collect();

    // §53.4: unify both assertions in one place (see tests/fault/README.md).
    assert_fault_observed(&FaultObservation {
        degrade_variant: DegradeCode::LaneSubstituted.as_str(),
        degrade_total_before: before,
        degrade_total_after: after,
        completeness_degradations: &degradations_wire,
    });
}
