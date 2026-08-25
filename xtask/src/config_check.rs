//! xtask `config-check` — 占位（对应任务卡见 DevPlan v1 Phase 0）。
//! 闸必须三态 pass/fail/not_applicable；not_applicable 必须打印缺失对象名（§57.1 第2条）。

pub fn run(_args: &[String]) -> i32 {
    eprintln!("config-check: not_applicable (missing object: not implemented yet — Phase 0 scaffold)");
    0
}
