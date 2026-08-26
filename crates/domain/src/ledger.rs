//! §23.1② / §22.5 的 **A1 恒等式唯一实现处**。
//!
//! §22.5 冻结：「§15.4 的 `advance_prefix` 改为复用同一个 `ledger::close`…那三行内联断言
//! 随之删掉：**A1 的算式全库只此一处**，不会两边各写一遍再漂移。」
//!
//! 分层说明（§78.3）：`retrieval`（`completeness::ledger::close` 做三次独立取数）与
//! `projection`（`stream::advance_prefix` 推进 watermark）是同级 crate、互不依赖，两者都只
//! 依赖本 crate。因此把**算式本身**放在这里，是让「只此一处」成为依赖拓扑的结果，而不是靠
//! 两处各自维护同一个表达式的纪律 —— 后者正是本项目反复堵的漂移形态。
//! 取数动作留在各自的家：`close` 要 repo（domain 不碰 DB），`advance_prefix` 要 stream 快照。

/// A1（§23.1②）：`done + open_gaps + pending == expected`。
///
/// 不成立意味着账本三个数互相矛盾 —— 调用方**只能**据此走各自的 fail-closed 出口
/// （`retrieval` ⇒ `LedgerClosure::Broken` ⇒ `CannotEstablish`；`projection` ⇒
/// `Err(Inconsistent)`），**禁止**退而取 `done`/`expected` 当近似值（§15.4 冻结）。
#[must_use]
pub fn a1_holds(expected: u64, done: u64, open_gaps: u64, pending: u64) -> bool {
    done.saturating_add(open_gaps).saturating_add(pending) == expected
}

#[cfg(test)]
mod tests {
    use super::a1_holds;

    #[test]
    fn holds_when_the_three_parts_sum_to_expected() {
        assert!(a1_holds(100, 90, 7, 3));
        assert!(a1_holds(0, 0, 0, 0));
    }

    #[test]
    fn broken_when_any_part_drifts() {
        assert!(!a1_holds(100, 90, 7, 2), "one short must not close");
        assert!(!a1_holds(100, 90, 7, 4), "one over must not close");
    }

    /// 溢出不得回绕成「恰好相等」—— 那会把一个坏账本读成闭合。
    #[test]
    fn saturating_addition_cannot_wrap_into_a_false_close() {
        assert!(!a1_holds(0, u64::MAX, 1, 0));
    }
}
