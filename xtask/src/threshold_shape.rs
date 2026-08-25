//! xtask `threshold-shape` — G80-15 threshold-shape-check（判据 §69 Continuation Gate CI 两条：①§55/§69 比例无量纲词即红 ②全文「不劣于」必须伴随禁止词）。
//! 占位：Wave3 实现。闸三态；not_applicable 必须打印缺失对象名（§57.1）。

pub fn run(_args: &[String]) -> i32 {
    eprintln!(
        "threshold-shape: not_applicable (missing object: not implemented yet — p0-wave3 pending)"
    );
    0
}
