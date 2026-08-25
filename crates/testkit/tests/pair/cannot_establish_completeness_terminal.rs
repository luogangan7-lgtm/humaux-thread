//! §52.2 概念对登记表 row 2（`ErrorCode::CannotEstablishCompleteness` /
//! `DegradeCode::CompletenessUnknown`）的终止侧，G52-5 成对测试。
//! 降级侧见同目录 `cannot_establish_completeness_degraded.rs`。
//!
//! 分界判据（§52.2 表）：调用方要求 completeness class 达标而系统给不出
//! ⇒ 终止；已返回结果只是完备性未知 ⇒ 降级。注意此对「字面同名」列记
//! 「否」（`fold("CompletenessUnknown")` != `"CANNOT_ESTABLISH_COMPLETENESS"`）
//! ——两层用不同的字面表达同一现象，登记表存在正是为了不让这被当成巧合。

use humaux_domain::error::ErrorCode;
use humaux_telemetry::degrade::Response;

/// 同一根因（系统给不出完备性判定）的注入桩：
/// `require_completeness_class` 就是 §52.2 表里的分界判据开关。
fn simulate_completeness(require_completeness_class: bool) -> Response<String> {
    let completeness_established = false; // fault injection: never established here
    if !completeness_established && require_completeness_class {
        return Err(ErrorCode::CannotEstablishCompleteness);
    }
    Ok(humaux_telemetry::degrade::abstain(
        humaux_telemetry::degrade::DegradeCode::CompletenessUnknown,
        "result-with-unknown-completeness".to_string(),
    ))
}

#[test]
fn cannot_establish_completeness_terminal_when_class_required() {
    let response = simulate_completeness(true);

    // G52-4: `Err` structurally carries no `Outcome::degradations` — asserting
    // the terminal code already proves "not degraded" for this response.
    match response {
        Err(code) => assert_eq!(code, ErrorCode::CannotEstablishCompleteness),
        Ok(_) => {
            panic!("G52-4: completeness class required + unestablished must terminate, not degrade")
        }
    }
}
