//! xtask `migrate` — 按序应用 migrations/*.sql（§46 迁移安全；DSN 取 HUMAUX_TEST_PG_DSN）。
//! 占位：Phase 1 G1 实现。闸三态。

pub fn run(_args: &[String]) -> i32 {
    eprintln!("migrate: not_applicable (missing object: not implemented yet — p1 pending)");
    0
}
