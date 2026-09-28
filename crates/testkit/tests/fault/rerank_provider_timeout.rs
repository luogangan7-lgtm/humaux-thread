//! `testkit::tests::fault::rerank_provider_timeout` — §53.4 注错测试：`DegradeCode::RerankProviderTimeout` 的一条注错记录（每个
//!   reason 必须有一条测试，从未触发过的 reason 不许合并，§53.3 规则2 的三数相等 把这句话变成 CI 事实）。
//! Depends-on: crates=[humaux-telemetry]; services=[]; env=[]; modules=[humaux-testkit, telemetry::degrade]
//! Called-by: [testkit::tests::fault_main]
//! Invariants: []
//! Spec: Baseline §53.4; §19; §23.3; §53.1; §53.3
//!
//! §53.4 登记项（尚未按登记语义实现，见下方 ponytail 标注）：rerank
//! provider 注入读超时，按 §19 走 fallback ranking 仍返回结果（items 非空）；
//! §23.3 早前示例写的 RERANK_TIMEOUT_PARTIAL 就是这一条，不是第十一个变体。
//!
//! `crates/retrieval/src/rerank.rs` 目前仍是 1 行占位模块（`T0.x 任务填充`），
//! 没有真实的 rerank provider 调用可注入读超时——§53.4 登记的注入方式（真
//! 超时 + 断言 fallback items 非空）在这个占位模块存在之前做不到。本文件
//! 与其余 8 个通用变体一样，改为直接调用 `abstain()`（§53.1 全仓唯一出口
//! 点）注错，这满足 §53.3 规则2「每个 reason 一条测试」的计数要求，但
//! **不满足** §53.4 对这条 reason 特别登记的注入方式——不得把这句头注释
//! 读作「已实现登记语义」。
//!
// ponytail: 用 abstain() 直接注错代替真实 rerank 超时注入，待
// `crates/retrieval/src/rerank.rs` 有真实 provider 调用实现后，把本测试换
// 成真正的读超时注入 + 断言 fallback ranking 的 items 非空。

use humaux_telemetry::degrade::{DegradeCode, abstain, degrade_total_count};
use humaux_testkit::{FaultObservation, assert_fault_observed};

#[test]
fn rerank_provider_timeout_increments_counter_and_reports_both_forms() {
    let before = degrade_total_count(DegradeCode::RerankProviderTimeout);
    let outcome = abstain(
        DegradeCode::RerankProviderTimeout,
        "fallback-ranked-items".to_string(),
    );
    let after = degrade_total_count(DegradeCode::RerankProviderTimeout);

    let degradations_wire: Vec<String> = outcome
        .degradations
        .iter()
        .map(|c| c.line_format())
        .collect();

    // §53.4: unify both assertions in one place (see tests/fault/README.md).
    assert_fault_observed(&FaultObservation {
        degrade_variant: DegradeCode::RerankProviderTimeout.as_str(),
        degrade_total_before: before,
        degrade_total_after: after,
        completeness_degradations: &degradations_wire,
    });
}
