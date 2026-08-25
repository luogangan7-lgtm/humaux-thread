//! §53.4 注错测试：`DegradeCode::StatePinAmbiguous` 的一条注错记录（每个 reason
//! 必须有一条测试，从未触发过的 reason 不许合并，§53.3 规则2 的三数相等
//! 把这句话变成 CI 事实）。
//!
//! 多个 state_pin 候选且无法择一（§53.4 照变体语义注入）。
//!
//! 本仓库尚无真实业务调用点会自然触发这条 abstain()（rerank/embedding/
//! egress/graph 等 provider 集成留待后续任务落地），注入手段就是直接调用
//! `abstain()`——它是 §53.1 全仓唯一出口点，直接调用即是对这条路径最贴近
//! 的注错，不是绕过它。

use humaux_telemetry::degrade::{DegradeCode, abstain, degrade_total_count};
use humaux_testkit::{FaultObservation, assert_fault_observed};

#[test]
fn state_pin_ambiguous_increments_counter_and_reports_both_forms() {
    let before = degrade_total_count(DegradeCode::StatePinAmbiguous);
    let outcome = abstain(
        DegradeCode::StatePinAmbiguous,
        "ambiguous-state-pin".to_string(),
    );
    let after = degrade_total_count(DegradeCode::StatePinAmbiguous);

    let degradations_wire: Vec<String> = outcome
        .degradations
        .iter()
        .map(|c| c.line_format())
        .collect();

    // §53.4: unify both assertions in one place (see tests/fault/README.md).
    assert_fault_observed(&FaultObservation {
        degrade_variant: DegradeCode::StatePinAmbiguous.as_str(),
        degrade_total_before: before,
        degrade_total_after: after,
        completeness_degradations: &degradations_wire,
    });
}
