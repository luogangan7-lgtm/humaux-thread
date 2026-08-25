//! §52.2 概念对登记表 row 1（`ErrorCode::ProjectionLag` / `DegradeCode::ProjectionLag`）
//! 的降级侧，G52-5 成对测试。terminal 侧见同目录 `projection_lag_terminal.rs`。
//!
//! 分界判据（§52.2 表）：不要求 read-your-write 且已返回滞后结果 ⇒ 降级。

use humaux_telemetry::degrade::{DegradeCode, Response, abstain, degrade_total_count};

/// 与 `projection_lag_terminal.rs` 同一根因（投影落后）的注入桩，只是
/// `require_read_your_write` 这个开关拨到另一侧。
fn simulate_projection_lag(require_read_your_write: bool) -> Response<String> {
    let projection_caught_up = false; // fault injection: projection is always behind here
    if !projection_caught_up && require_read_your_write {
        return Err(humaux_domain::error::ErrorCode::ProjectionLag);
    }
    Ok(abstain(
        DegradeCode::ProjectionLag,
        "stale-result".to_string(),
    ))
}

#[test]
fn projection_lag_degraded_when_read_your_write_not_required() {
    let before = degrade_total_count(DegradeCode::ProjectionLag);

    let response = simulate_projection_lag(false);

    let outcome = match response {
        Ok(outcome) => outcome,
        Err(code) => panic!(
            "G52-4: read-your-write not required must degrade, not terminate (got Err({code}))"
        ),
    };

    assert_eq!(outcome.value, "stale-result");
    assert_eq!(
        outcome.degradations.as_slice(),
        &[DegradeCode::ProjectionLag],
        "still-successful response must carry the degradation marker"
    );
    assert_eq!(
        degrade_total_count(DegradeCode::ProjectionLag),
        before + 1,
        "degraded path must actually go through abstain()"
    );
}
