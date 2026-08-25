//! §52.2 概念对登记表 row 2（`ErrorCode::CannotEstablishCompleteness` /
//! `DegradeCode::CompletenessUnknown`）的降级侧，G52-5 成对测试。terminal
//! 侧见同目录 `cannot_establish_completeness_terminal.rs`。
//!
//! 分界判据（§52.2 表）：调用方不要求 completeness class 达标，已返回结果
//! 只是完备性未知 ⇒ 降级。

use humaux_telemetry::degrade::{DegradeCode, Response, abstain, degrade_total_count};

/// 与 `cannot_establish_completeness_terminal.rs` 同一根因（系统给不出完
/// 备性判定）的注入桩，只是 `require_completeness_class` 开关拨到另一侧。
fn simulate_completeness(require_completeness_class: bool) -> Response<String> {
    let completeness_established = false; // fault injection: never established here
    if !completeness_established && require_completeness_class {
        return Err(humaux_domain::error::ErrorCode::CannotEstablishCompleteness);
    }
    Ok(abstain(
        DegradeCode::CompletenessUnknown,
        "result-with-unknown-completeness".to_string(),
    ))
}

#[test]
fn cannot_establish_completeness_degraded_when_class_not_required() {
    let before = degrade_total_count(DegradeCode::CompletenessUnknown);

    let response = simulate_completeness(false);

    let outcome = match response {
        Ok(outcome) => outcome,
        Err(code) => panic!(
            "G52-4: completeness class not required must degrade, not terminate (got Err({code}))"
        ),
    };

    assert_eq!(outcome.value, "result-with-unknown-completeness");
    assert_eq!(
        outcome.degradations.as_slice(),
        &[DegradeCode::CompletenessUnknown],
        "still-successful response must carry the degradation marker"
    );
    assert_eq!(
        degrade_total_count(DegradeCode::CompletenessUnknown),
        before + 1,
        "degraded path must actually go through abstain()"
    );
}
