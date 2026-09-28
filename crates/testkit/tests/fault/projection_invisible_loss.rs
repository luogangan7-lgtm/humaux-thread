//! `testkit::tests::fault::projection_invisible_loss` — §53.4 注错测试：`DegradeCode::ProjectionInvisibleLoss` 的一条注错记录（每个
//!   reason 必须有一条测试，从未触发过的 reason 不许合并，§53.3 规则2 的三数相等 把这句话变成 CI 事实）。
//! Depends-on: crates=[humaux-telemetry]; services=[]; env=[]; modules=[humaux-testkit, telemetry::degrade]
//! Called-by: [testkit::tests::fault_main]
//! Invariants: [injects ProjectionInvisibleLoss through abstain(), the single emit point (§53.3 rule 2); the
//!   registered real-Qdrant injection (delete 10 points) is not implemented here]
//! Spec: Baseline §53.4; §23.4; §53.1; §53.3
//!
//! §53.4 登记项（尚未按登记语义实现，见下方 ponytail 标注）：绕过
//! `retention::tombstone` 直接从 Qdrant 删 10 个 point（= §23.4 G23-2 注入 1）；
//! visible 100 → 90，done 恒 100，deleted 恒 0，A2 违反。
//!
//! `crates/adapters/src/qdrant.rs` 目前仍是 1 行占位模块（`T0.x 任务填充`），
//! 没有真实的 Qdrant client / point 可删——§53.4 登记的注入方式（真删 10 个
//! point、断言 visible/done/deleted 三个计数）在这个占位模块存在之前做不到。
//! 本文件与其余 8 个通用变体一样，改为直接调用 `abstain()`（§53.1 全仓唯一
//! 出口点）注错，这满足 §53.3 规则2「每个 reason 一条测试」的计数要求，
//! 但**不满足** §53.4 对这条 reason 特别登记的注入方式——不得把这句头注释
//! 读作「已实现登记语义」。
//!
// ponytail: 用 abstain() 直接注错代替真实 Qdrant 删点，待
// `crates/adapters/src/qdrant.rs` 有真实 client 实现后，把本测试换成真正
// 绕过 retention::tombstone 删 10 个 point + 断言 visible 100→90 / done 恒
// 100 / deleted 恒 0。

use humaux_telemetry::degrade::{DegradeCode, abstain, degrade_total_count};
use humaux_testkit::{FaultObservation, assert_fault_observed};

#[test]
fn projection_invisible_loss_increments_counter_and_reports_both_forms() {
    let before = degrade_total_count(DegradeCode::ProjectionInvisibleLoss);
    let outcome = abstain(
        DegradeCode::ProjectionInvisibleLoss,
        "visible-undercount-fallback".to_string(),
    );
    let after = degrade_total_count(DegradeCode::ProjectionInvisibleLoss);

    let degradations_wire: Vec<String> = outcome
        .degradations
        .iter()
        .map(|c| c.line_format())
        .collect();

    // §53.4: unify both assertions in one place (see tests/fault/README.md).
    assert_fault_observed(&FaultObservation {
        degrade_variant: DegradeCode::ProjectionInvisibleLoss.as_str(),
        degrade_total_before: before,
        degrade_total_after: after,
        completeness_degradations: &degradations_wire,
    });
}
