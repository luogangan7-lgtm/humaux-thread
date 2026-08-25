//! xtask `dod-check` — G80-33 DoD verifier closure + G80-17.D2 BootstrapDeferredSpec 键集相等（§69 DoD Verifier Contract）。
//! 占位：Wave3 实现。闸三态；not_applicable 必须打印缺失对象名（§57.1）。

pub fn run(_args: &[String]) -> i32 {
    eprintln!("dod-check: not_applicable (missing object: not implemented yet — p0-wave3 pending)");
    0
}
