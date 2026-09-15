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

/// §19.1 `ops.model_call_ledger.purpose` 的**闭集唯一真源**（§78.2「closed enums」）。
///
/// DB 侧由 `model_call_ledger_purpose_known` CHECK 镜像
/// （`migrations/0166_model_call_ledger_private_purposes.sql`，把 0130 的四值集合放宽到
/// 也接受三个私有推理 purpose）。两侧**只允许**通过
/// `crates/adapters/tests/model_call_ledger.rs::db_purpose_check_mirrors_the_rust_closed_set`
/// 这一条契约测试保持同步：任何一侧多出或少掉一个值，那条测试立刻红。
///
/// 放在 domain 而不是 adapters：§78.3 —— 检索面（`retrieval-provider`）与私有推理面
/// （`private-worker` / `consolidation-worker`）是同级 crate、互不依赖，两者都写同一张账本。
/// 枚举放在共同依赖里，「只此一处」才是依赖拓扑的结果，而不是两处各自维护同一串字面量。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ModelCallPurpose {
    /// §20 检索面：查询改写。
    QueryRewrite,
    /// §19 检索面：embedding。
    Embedding,
    /// §19 检索面：rerank。
    Rerank,
    /// §11.8 贡献去标识化（0130 引入，USER 付费，带完整 route 快照）。
    ContributionDeidentify,
    /// §11.6 distill（文本证据 → 候选记忆）。
    PrivateDistillText,
    /// §11.6 distill（视觉证据）。
    PrivateDistillVision,
    /// §11.7 consolidation（多条记忆 → rollup）。
    PrivateConsolidate,
}

impl ModelCallPurpose {
    /// 全集。契约测试拿它跟 DB CHECK 对账，所以顺序无所谓、**完整性有所谓**。
    pub const ALL: [Self; 7] = [
        Self::QueryRewrite,
        Self::Embedding,
        Self::Rerank,
        Self::ContributionDeidentify,
        Self::PrivateDistillText,
        Self::PrivateDistillVision,
        Self::PrivateConsolidate,
    ];

    /// 落库字面量。三个私有值刻意与 §7.4 披露行的 `PrivateDataPurpose` 同名
    /// （`adapters::contribution_execution_repo` 已经在用这三个串），所以一次 provider 调用的
    /// 账本行与披露行读起来是同一个 purpose，不用再维护第二张对照表。
    #[must_use]
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::QueryRewrite => "query_rewrite",
            Self::Embedding => "embedding",
            Self::Rerank => "rerank",
            Self::ContributionDeidentify => "CONTRIBUTION_DEIDENTIFY",
            Self::PrivateDistillText => "PRIVATE_DISTILL_TEXT",
            Self::PrivateDistillVision => "PRIVATE_DISTILL_VISION",
            Self::PrivateConsolidate => "PRIVATE_CONSOLIDATE",
        }
    }

    /// 读侧逆映射。未知串返回 `None`（fail-closed）——**禁止**退化成自由字符串。
    #[must_use]
    pub fn from_db_str(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|purpose| purpose.as_db_str() == value)
    }
}

#[cfg(test)]
mod purpose_tests {
    use super::ModelCallPurpose;

    #[test]
    fn db_strings_are_unique_and_round_trip() {
        let mut seen: Vec<&'static str> = ModelCallPurpose::ALL
            .into_iter()
            .map(ModelCallPurpose::as_db_str)
            .collect();
        seen.sort_unstable();
        let mut deduped = seen.clone();
        deduped.dedup();
        assert_eq!(seen, deduped, "two purposes must never share a DB literal");
        for purpose in ModelCallPurpose::ALL {
            assert_eq!(
                ModelCallPurpose::from_db_str(purpose.as_db_str()),
                Some(purpose)
            );
        }
    }

    #[test]
    fn unknown_literal_fails_closed() {
        assert_eq!(ModelCallPurpose::from_db_str("PRIVATE_SOMETHING"), None);
        assert_eq!(ModelCallPurpose::from_db_str(""), None);
    }
}
