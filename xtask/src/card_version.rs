//! xtask `card-version` — §18.4 CI 闸：按 (card_builder_version, card_template_hash) 去重，
//! 出现一对多即红。占位：Phase 5 wave 实现。闸三态；not_applicable 必须打印缺失对象名（§57.1）。

pub fn run(_args: &[String]) -> i32 {
    eprintln!("card-version: not_applicable (missing object: not implemented yet — p5 pending)");
    0
}
