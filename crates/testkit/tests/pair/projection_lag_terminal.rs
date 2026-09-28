//! `testkit::tests::pair::projection_lag_terminal` — §52.2 概念对登记表 row 1（`ErrorCode::ProjectionLag` /
//!   `DegradeCode::ProjectionLag`） 的终止侧，G52-5 成对测试。
//! Depends-on: crates=[humaux-domain, humaux-telemetry]; services=[]; env=[]; modules=[domain::error, telemetry::degrade]
//! Called-by: [testkit::tests::pair_main]
//! Invariants: []
//! Spec: Baseline §52.2
//!
//! 分界判据（§52.2 表）：请求要求 read-your-write 而投影未追上 ⇒ 终止；
//! 不要求且已返回滞后结果 ⇒ 降级（降级侧见同目录 `projection_lag_degraded.rs`）。
//! 两个文件共享同一根因（投影落后）、只切换"是否要求 read-your-write"这一
//! 个开关，不是两个无关场景。

use humaux_domain::error::ErrorCode;
use humaux_telemetry::degrade::Response;

/// 同一根因（投影落后）的注入桩：`require_read_your_write` 就是 §52.2
/// 表里的分界判据开关。
fn simulate_projection_lag(require_read_your_write: bool) -> Response<String> {
    let projection_caught_up = false; // fault injection: projection is always behind here
    if !projection_caught_up && require_read_your_write {
        return Err(ErrorCode::ProjectionLag);
    }
    Ok(humaux_telemetry::degrade::abstain(
        humaux_telemetry::degrade::DegradeCode::ProjectionLag,
        "stale-result".to_string(),
    ))
}

#[test]
fn projection_lag_terminal_when_read_your_write_required() {
    let response = simulate_projection_lag(true);

    // G52-4: `Response<T> = Result<Outcome<T>, ErrorCode>` — an `Err` value
    // structurally cannot also carry `Outcome::degradations` (mutual
    // exclusion is a type guarantee, not a runtime check), so asserting
    // `Err(ProjectionLag)` here already proves "not degraded" for free.
    match response {
        Err(code) => assert_eq!(code, ErrorCode::ProjectionLag),
        Ok(_) => panic!(
            "G52-4: read-your-write required + lagging projection must terminate, not degrade"
        ),
    }
}
