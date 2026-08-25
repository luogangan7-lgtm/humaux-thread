//! xtask `direction-table` — G80-9 direction table 由源生成与手写表比对（§53.6）。
//! 占位：Wave3 实现。闸三态；not_applicable 必须打印缺失对象名（§57.1）。

pub fn run(_args: &[String]) -> i32 {
    eprintln!(
        "direction-table: not_applicable (missing object: not implemented yet — p0-wave3 pending)"
    );
    0
}
