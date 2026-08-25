//! xtask `gate-registry` — G80-23 gate-anchor-check + G80-24 gate-registry-coverage（§80.1.2 是家章：锚文法闭集、id 族绑定表、§57.1 生效期恰一次）。
//! 占位：Wave3 实现。闸三态；not_applicable 必须打印缺失对象名（§57.1）。

pub fn run(_args: &[String]) -> i32 {
    eprintln!(
        "gate-registry: not_applicable (missing object: not implemented yet — p0-wave3 pending)"
    );
    0
}
