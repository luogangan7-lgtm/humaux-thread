//! `domain::grounding` — §8.8 Grounding Validity：把「证据变了」与「事实错了」分开。
//! Depends-on: crates=[async-trait]; services=[]; env=[]; modules=[domain::evidence]
//! Called-by: [adapters::context_repo, adapters::continuity_read, domain::context, retrieval::envelope, retrieval::handoff, retrieval::signals, tests]
//! Invariants: []
//! Spec: Baseline §3; §8.1; §8.8; ADR-0006
//!
//! 这个模块只回答一个问题：**当初支持这条 Memory 的可变来源，还是同一个版本吗？**
//! 它不回答「这条 Memory 有多旧」——那是 [`crate::temporal`] 的 `TemporalFreshness`。
//! 两者正交（§8.8「Temporal Freshness 与 Grounding Validity 正交」），§21.5 明确禁止把它们
//! 压成一个 `stale` 字段：允许「两年前的历史 Memory + SNAPSHOT evidence ⇒ CURRENT」，也允许
//! 「刚写 5 分钟的 current-state Memory + 代码刚变化 ⇒ RECHECK_REQUIRED」。
//!
//! **`RECHECK_REQUIRED` 不等于事实错误**（§8.8）。它只撤销「继续安全假设为当前真值」的资格；
//! 真正的对错要等 revalidation 产出 `CONFIRM/REVISE/RETRACT/DEFER`（§11.10）。

use crate::evidence::EvidencePayloadSha256;

/// §8.8：这条 grounding edge 的陈述**绑定的是哪一种时间语义**——决定来源版本变化算不算过时。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GroundingMode {
    /// 陈述的是「当前事实」；来源版本一变就必须重验。
    Live,
    /// 陈述明确绑定历史 snapshot/commit；当前 HEAD 怎么动都不使其过时。
    Snapshot,
    /// 原始消息/事件等不可变 Evidence；只通过 correction/supersession 演化，不通过重验。
    Immutable,
}

impl GroundingMode {
    /// 只有 `Live` 参与 [`derive_grounding_state`] 的判定（§8.8 四态定义逐条只说 LIVE）。
    #[must_use]
    pub const fn participates_in_revalidation(self) -> bool {
        matches!(self, Self::Live)
    }
}

/// §8.8：**完全由 resolver 拥有**的版本标识。调用方只允许比较相等，不允许解释内容、
/// 不允许排序、不允许从中推断「新旧」——不同 resolver 的 token 语义互不相通
/// （git blob sha、ETag、内容哈希、文档 revision id……）。
///
/// 因此这里刻意不实现 `PartialOrd`：一旦能比大小，就会有人写出「token 变大 = 更新」这种
/// 跨 resolver 不成立的假设。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroundingVersionToken(String);

impl GroundingVersionToken {
    /// Resolver 侧唯一构造点。调用方拿到之后只能 `==`。
    #[must_use]
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }

    /// 持久化用的不透明字符串。**不要**在业务判定里解析它。
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// 要 resolve 的来源。`kind` 与 `locator` 的取值域由各 resolver 自己定义——domain 不认识
/// 「git」「http」这些具体协议（§3：domain 永不 import HTTP/SDK）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroundingResource {
    /// Resolver 选择键（例如 `"repo-file"`、`"web-doc"`）。
    pub kind: String,
    /// 该 resolver 语义下的定位串。
    pub locator: String,
}

/// §8.1 身份三要素的最小草稿——resolver 解析成功后交回的新证据，供重绑定路径落库。
///
// ponytail: 只带身份必需的 payload hash + kind；正文/分类等在真正写 EvidenceObject 的
// 那条路径上补（§11.10 重绑定，Phase 4+），这里不预建字段。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceObjectDraft {
    /// §8.1 的身份哈希，由 [`crate::evidence::payload_sha256`] 算出——不自造第二个身份函数。
    pub payload_sha256: EvidencePayloadSha256,
    /// EvidenceObject 的 kind（`EVENT` / `ARTIFACT` / …），取值域由 §8.1 定义。
    pub evidence_kind: String,
}

/// §8.8 resolver contract 的返回值。
///
/// **`Missing` 与 resolver error 必须分开**（§8.8；G11-2 夹具 D 就是拿「把 error 当 Missing」
/// 注错的）：`Missing` 是「来源确实不在了」这一**已确定的事实**，error 是「我没能问出来」。
/// 前者派生 `UNRESOLVED`，后者派生 `CANNOT_ESTABLISH`——两者对下游的含义完全不同。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedGroundingEvidence {
    /// 来源仍在，`version` 是**此刻**观测到的 token（不是传入的 `previous_version`）。
    Current {
        /// 解析到的新证据，供重绑定路径落库。
        evidence: EvidenceObjectDraft,
        /// 此刻观测到的版本。
        version: GroundingVersionToken,
    },
    /// 来源**确定**已不存在——与「没能问出来」（[`GroundingResolveError`]）分属两个类型。
    Missing,
}

/// Resolver 失败原因。刻意与 [`ResolvedGroundingEvidence::Missing`] 分属两个类型，
/// 让「把 error 塞进 Missing」需要显式写代码，而不是顺手就能做到。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroundingResolveError {
    /// 超时 / 网络 / 上游 5xx 等可重试失败。
    Transient(String),
    /// 权限、policy 阻断、resource kind 无人认领等不可重试失败。
    Permanent(String),
}

/// §8.8 resolver contract。`dyn`-compatible（`Arc<dyn EvidenceResolver>` 注入），故用
/// `async_trait` 而非 AFIT。
#[async_trait::async_trait]
pub trait EvidenceResolver: Send + Sync {
    /// `previous_version` 传入**记录在案**的 token，供 resolver 做条件请求（If-None-Match
    /// 之类）优化；resolver 仍必须在 `Current` 里回填它**当前**观测到的 version。
    async fn resolve(
        &self,
        resource: &GroundingResource,
        previous_version: Option<&GroundingVersionToken>,
    ) -> Result<ResolvedGroundingEvidence, GroundingResolveError>;
}

/// 单条 grounding edge 的解析结果（`private.memory_evidence` 一行对应一条）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeOutcome {
    /// 解析成功，`current` 是此刻观测到的 version。
    Resolved(GroundingVersionToken),
    /// 来源确定已不存在（**不是**「没问出来」）。
    Missing,
    /// Resolver 报错 / 超时 / policy 阻断。
    ResolveFailed(GroundingResolveError),
}

/// 判定输入：一条 edge 的 mode、**记录在案**的 version、以及本轮解析结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroundingEdge {
    /// 这条 edge 的时间语义；只有 [`GroundingMode::Live`] 参与判定。
    pub mode: GroundingMode,
    /// 写入这条 Memory 时记录的 version。`None` 表示当初就没记（历史数据/非 LIVE）。
    pub recorded_version: Option<GroundingVersionToken>,
    /// 本轮解析结果。
    pub outcome: EdgeOutcome,
}

/// [`derive_grounding_state`] 的输入。用枚举而不是 `&[GroundingEdge]`，是为了让「edge 集合
/// 本身建立不起来」（§8.8 的「分母无法建立」）也走**同一个**入口——否则调用方就得在别处
/// 自己造一个 `CANNOT_ESTABLISH`，那正是本模块要杜绝的第二真源。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroundingInputs<'a> {
    /// 这条 Memory 的全部 grounding edge（非 LIVE 的会在判定里被跳过）。
    Edges(&'a [GroundingEdge]),
    /// 连「这条 Memory 有哪些 LIVE edge」都枚举不出来（reverse index 不可用、查询超时……）。
    DenominatorUnavailable,
}

/// §8.8 四个派生状态。变体公开供 `match`，但值本身只能由 [`derive_grounding_state`] 造出
/// （见 [`GroundingState`]）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GroundingStateKind {
    /// 所有 LIVE evidence resolve 成相同 version（**零条 LIVE edge 也算**：SNAPSHOT/
    /// IMMUTABLE-only 的历史 Memory 天然 CURRENT，§8.8 的正例之一）。
    Current,
    /// 所有 LIVE evidence 仍可解析，但至少一个 version != recorded version。
    RecheckRequired,
    /// 至少一个 LIVE evidence 明确 Missing。
    Unresolved,
    /// Resolver error / timeout / policy block / 分母无法建立。
    CannotEstablish,
}

impl GroundingStateKind {
    /// §8.8 优先级：`CANNOT_ESTABLISH > UNRESOLVED > RECHECK_REQUIRED > CURRENT`。
    /// 数值只用于取最大者，不对外承诺任何序关系。
    const fn severity(self) -> u8 {
        match self {
            Self::Current => 0,
            Self::RecheckRequired => 1,
            Self::Unresolved => 2,
            Self::CannotEstablish => 3,
        }
    }

    /// 是否撤销了「继续安全假设为当前真值」的资格（§8.8）。DOD-093 的 Mandatory/Pinned
    /// context 判据读这一个方法，而不是各自枚举三个变体——少一处漏枚举的机会。
    #[must_use]
    pub const fn revokes_current_truth_assumption(self) -> bool {
        !matches!(self, Self::Current)
    }
}

/// §8.8「派生状态，不新增可手改 stale flag」的**类型层实现**。
///
/// 私有字段 + 本模块外无构造路径 ⇒ 想手工把一条 Memory 标成 stale，在编译期就不可能，
/// 而不是靠评审发现（同 [`crate::temporal::RankInstant`] 的先例）。DOD-092 要求的
/// 「不存在可手改 `memory.stale=true` 真源」由此成为依赖拓扑的结论，不是纪律。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GroundingState(GroundingStateKind);

impl GroundingState {
    /// 取出四态之一供 `match`。**只有读取路径**——没有对应的 `set`，见本类型的 doc。
    #[must_use]
    pub const fn kind(self) -> GroundingStateKind {
        self.0
    }

    /// 见 [`GroundingStateKind::revokes_current_truth_assumption`]。
    #[must_use]
    pub const fn revokes_current_truth_assumption(self) -> bool {
        self.0.revokes_current_truth_assumption()
    }
}

/// §8.8 的唯一派生入口：`GroundingState` 只能从「记录在案的 version vs resolver 此刻给出的
/// version」推出来。
///
/// 非 LIVE edge（SNAPSHOT / IMMUTABLE）**完全不参与**：它们的来源怎么变都不使这条陈述过时，
/// 这正是 §8.8 举的「两年前的历史 Memory + SNAPSHOT evidence ⇒ CURRENT」。
///
/// `recorded_version` 为 `None` 的 LIVE edge 判 `RECHECK_REQUIRED`：当初没记版本，就无法证明
/// 「还是同一版」——按 §8.8 的语义这恰恰是「没有资格继续假设为当前真值」，不是 CURRENT。
#[must_use]
pub fn derive_grounding_state(inputs: GroundingInputs<'_>) -> GroundingState {
    let edges = match inputs {
        GroundingInputs::DenominatorUnavailable => {
            return GroundingState(GroundingStateKind::CannotEstablish);
        }
        GroundingInputs::Edges(edges) => edges,
    };

    let worst = edges
        .iter()
        .filter(|e| e.mode.participates_in_revalidation())
        .map(|e| match &e.outcome {
            EdgeOutcome::ResolveFailed(_) => GroundingStateKind::CannotEstablish,
            EdgeOutcome::Missing => GroundingStateKind::Unresolved,
            EdgeOutcome::Resolved(current) => match &e.recorded_version {
                Some(recorded) if recorded == current => GroundingStateKind::Current,
                _ => GroundingStateKind::RecheckRequired,
            },
        })
        .max_by_key(|k| k.severity())
        .unwrap_or(GroundingStateKind::Current);

    GroundingState(worst)
}

/// 快照内可见的单条 edge 事实——[`classify_in_snapshot`] 的输入。
///
/// 与 [`GroundingEdge`] 的区别：那边有 resolver 的**本轮解析结果**（`EdgeOutcome`），
/// 这边只有装配读事务里查得到的两个事实（mode + 当初记没记版本）。resolver 永远不进
/// 装配事务——外呼的时延与失败模式会毁掉「同一快照两次装配逐字节相同」（§57.1 Phase 8）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotEdge {
    /// 时间语义档。
    pub mode: GroundingMode,
    /// 写入这条 edge 时是否记录了版本。
    pub recorded_version_present: bool,
}

/// 快照内派生的行级 grounding 结论。
///
/// **`NotJudged` 是第三臂，不是 `Judged(Current)` 的别名**：LIVE 且已记版本的 edge 在
/// 快照内无从比对（比对要 resolver），「没判」与「判过且通过」必须可区分——
/// 同 `EvidenceBlock.expected: Option` 立的「未做的判断不是通过的判断」纪律。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowGrounding {
    /// 快照内就能裁定的情形。
    Judged(GroundingState),
    /// 快照内不可裁（有 LIVE 且全部已记版本——要 resolver 才知道版本还同不同）。
    NotJudged,
}

/// §8.8 的**快照内保守投影**——[`derive_grounding_state`] 的 resolver-free 姊妹入口。
///
/// 规则与 migration `0100` 写死的语义逐字对应：
/// - 无 LIVE edge ⇒ `Judged(CURRENT)`（SNAPSHOT/IMMUTABLE-only 天然 CURRENT，§8.8 正例）；
/// - 任一 LIVE 且 `recorded_version` 缺席 ⇒ `Judged(RECHECK_REQUIRED)`（当初没记版本就
///   无法证明「还是同一版」，0100 的默认回填正是让历史行落进这一档）；
/// - LIVE 全部已记版本 ⇒ [`RowGrounding::NotJudged`]。
///
/// 两个入口的重叠情形（无 LIVE / LIVE+无版本）由本模块测试钉死一致——分类规则只有一份，
/// 这里不是第二份判据，是同一份判据在「拿不到 resolver」时能覆盖的那部分。
///
// ponytail: 两入口并存是事实（uncertain：上游是否要求收敛为一）；收敛的前提是 resolver
// 进得了某种预算好的离线通道，那是 §11.10 census 的形态，不是装配的。
#[must_use]
pub fn classify_in_snapshot(edges: &[SnapshotEdge]) -> RowGrounding {
    let mut live_all_versioned = true;
    let mut has_live = false;
    for e in edges {
        if !e.mode.participates_in_revalidation() {
            continue;
        }
        has_live = true;
        if !e.recorded_version_present {
            return RowGrounding::Judged(GroundingState(GroundingStateKind::RecheckRequired));
        }
        live_all_versioned &= e.recorded_version_present;
    }
    if !has_live {
        return RowGrounding::Judged(GroundingState(GroundingStateKind::Current));
    }
    debug_assert!(live_all_versioned);
    RowGrounding::NotJudged
}

/// §11.10 revalidation 的语义结论。检测阶段（版本比对）不产出这个——它只能由重验产出。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RevalidationOutcome {
    /// statement 保持，绑定当前 Evidence revision。
    Confirm,
    /// 写新 Memory/Correction，旧 Memory supersede。
    Revise,
    /// revoke/supersede，**不物理删 Evidence**。
    Retract,
    /// Grounding debt 保持，下次继续。
    Defer,
}

/// §11.10 Finalization CAS 冻结的输入指纹。revalidator 开始时冻结，提交事务里重算比对。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevalidationInputSet {
    /// 冻结时这条 Memory 的版本号。
    pub input_memory_version: u64,
    /// 参与本轮重验的 evidence id（调用方自己的 id 类型序列化成串，domain 不绑定 id 表示）。
    pub input_evidence_ids: Vec<String>,
    /// 上述 evidence 当时的 version token 集合的折叠哈希。
    pub input_version_set_hash: String,
}

/// §11.10 Finalization CAS 的判定结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizationVerdict {
    /// 输入未变，可以提交。
    Commit,
    /// **验证期间来源又改了一次**——丢弃未提交结果并重新排队。与 Consolidation 的
    /// stale-input CAS 同源（§11.10）。
    StaleInput,
}

/// §11.10 Finalization CAS 的唯一判定点：提交前重算的 version set hash 必须与冻结时相同。
///
/// 这里刻意只比 hash 不比 `input_evidence_ids`：id 集合的变化本来就会改变 hash，多比一次
/// 只会造出第二处可以各自演化的判据。
#[must_use]
pub fn finalize_revalidation(
    frozen: &RevalidationInputSet,
    current_version_set_hash: &str,
) -> FinalizationVerdict {
    if frozen.input_version_set_hash == current_version_set_hash {
        FinalizationVerdict::Commit
    } else {
        FinalizationVerdict::StaleInput
    }
}

#[cfg(test)]
mod tests {
    //! §11.10#G11-2 / G80-43 的八条固定夹具 A–H。
    //!
    //! 夹具 G（`RECHECK_REQUIRED` 的 ProjectConstraint 不得进入 Mandatory behavior context）
    //! 的被测对象是 §25 Mandatory Context Lane，而 `domain::context` 目前还是占位模块——按
    //! §57.1 第2条走 `not_applicable` 并**打印缺失对象名**，不静默跳过、更不假装通过。
    //! 见本模块末尾 `fixture_g_*`。
    //!
    //! 相位口径要分清（先前这里写混过）：§25 那条 lane 本身是 **DOD-020，phase=7**——
    //! 也就是说它已经欠账了，不是「还没到时候」；而本夹具要断言的那一条（Mandatory/Pinned
    //! 不得静默消费非 CURRENT 的 Memory、须 fail-loud + `needs_verification[]`）是
    //! **DOD-093，phase=8**。lane 在 7 建出来，grounding 这一层的准入判定在 8 补。

    use super::*;
    use crate::evidence::payload_sha256;

    fn token(s: &str) -> GroundingVersionToken {
        GroundingVersionToken::new(s)
    }

    fn live(recorded: Option<&str>, outcome: EdgeOutcome) -> GroundingEdge {
        GroundingEdge {
            mode: GroundingMode::Live,
            recorded_version: recorded.map(token),
            outcome,
        }
    }

    fn kind_of(edges: &[GroundingEdge]) -> GroundingStateKind {
        derive_grounding_state(GroundingInputs::Edges(edges)).kind()
    }

    /// 夹具 A：version same -> CURRENT。
    #[test]
    fn fixture_a_same_version_is_current() {
        let edges = [live(Some("v1"), EdgeOutcome::Resolved(token("v1")))];
        assert_eq!(kind_of(&edges), GroundingStateKind::Current);
    }

    /// 夹具 B：version changed -> RECHECK_REQUIRED。
    /// 注错「把 version compare 删除」必须让这条从 RECHECK_REQUIRED 翻成 CURRENT 而变红。
    #[test]
    fn fixture_b_changed_version_is_recheck_required() {
        let edges = [live(Some("v1"), EdgeOutcome::Resolved(token("v2")))];
        assert_eq!(kind_of(&edges), GroundingStateKind::RecheckRequired);
    }

    /// 夹具 C：resource missing -> UNRESOLVED。
    #[test]
    fn fixture_c_missing_resource_is_unresolved() {
        let edges = [live(Some("v1"), EdgeOutcome::Missing)];
        assert_eq!(kind_of(&edges), GroundingStateKind::Unresolved);
    }

    /// 夹具 D：resolver throws -> CANNOT_ESTABLISH，**不能伪装成 Missing**（§8.8）。
    /// 断言同时钉「是 CannotEstablish」与「不是 Unresolved」：只写前者的话，把 error 当
    /// Missing 的注错会让它变成 Unresolved 而 `assert_ne` 之外无人看守——这两条是同一个
    /// 注错的正反面，缺一就有绕过。
    #[test]
    fn fixture_d_resolver_error_is_cannot_establish_not_missing() {
        let edges = [live(
            Some("v1"),
            EdgeOutcome::ResolveFailed(GroundingResolveError::Transient("timeout".into())),
        )];
        assert_eq!(kind_of(&edges), GroundingStateKind::CannotEstablish);
        assert_ne!(
            kind_of(&edges),
            GroundingStateKind::Unresolved,
            "resolver error 被当成 Missing 了——§8.8 要求两者分开"
        );
    }

    /// 夹具 E：代码只是挪了位置，内容/锚点 resolve 出同一个 token -> 仍然 CURRENT。
    /// 判定只看 token 相等，不看 locator 是否变化——这正是「relocation 不算过时」的实现方式。
    #[test]
    fn fixture_e_relocation_with_same_token_stays_current() {
        let moved = GroundingEdge {
            mode: GroundingMode::Live,
            recorded_version: Some(token("blob-sha-abc")),
            outcome: EdgeOutcome::Resolved(token("blob-sha-abc")),
        };
        assert_eq!(kind_of(&[moved]), GroundingStateKind::Current);
    }

    /// 夹具 F：重验期间来源又改了一次 -> STALE_INPUT，**不得提交 CONFIRM**。
    /// 注错「去掉 finalization CAS」必须让这条红。
    #[test]
    fn fixture_f_source_changed_mid_revalidation_is_stale_input() {
        let frozen = RevalidationInputSet {
            input_memory_version: 7,
            input_evidence_ids: vec!["ev-1".into()],
            input_version_set_hash: "hash-at-freeze".into(),
        };
        assert_eq!(
            finalize_revalidation(&frozen, "hash-changed-underneath"),
            FinalizationVerdict::StaleInput
        );
        // 正对照：输入没变才允许提交——否则「永远 StaleInput」也能让上一条绿。
        assert_eq!(
            finalize_revalidation(&frozen, "hash-at-freeze"),
            FinalizationVerdict::Commit
        );
    }

    /// 夹具 H 正对照：CONFIRM 之后把 edge 重绑到新 version -> 回到 CURRENT。
    #[test]
    fn fixture_h_after_confirm_rebinding_returns_to_current() {
        let before = [live(Some("v1"), EdgeOutcome::Resolved(token("v2")))];
        assert_eq!(kind_of(&before), GroundingStateKind::RecheckRequired);

        let outcome = RevalidationOutcome::Confirm;
        assert_eq!(outcome, RevalidationOutcome::Confirm);
        // CONFIRM 的语义是「statement 保持，绑定当前 Evidence revision」——重绑后 recorded
        // 就是 v2，再算一次必须是 CURRENT。
        let after = [live(Some("v2"), EdgeOutcome::Resolved(token("v2")))];
        assert_eq!(kind_of(&after), GroundingStateKind::Current);
    }

    /// 夹具 G（§11.10#G11-2）：`RECHECK_REQUIRED` 的 ProjectConstraint 不得进入
    /// Mandatory behavior context——**真断言**，不再是 NA。
    ///
    /// 曾经这里打印「missing object: §25 Mandatory Context Lane（domain::context 仍是
    /// 占位模块）」——那句话在 lane 落地后**继续打印了一段时间**，成了 ADR-0006 说的
    /// 「情况变化后继续沉默的 NA」。教训同款：NA prose 是写下当天的判断，被测对象一落地
    /// 它就开始说谎。现在的断言链：快照内派生 RECHECK_REQUIRED ⇒
    /// `revokes_current_truth_assumption` ⇒ `context::MandatoryRow::from_selector` 的
    /// 铸造门把它分流进 `NeedsVerification` 臂——row 铸不出来（DOD-093 的结构保证，
    /// 完整判据在 `domain::context` 的测试里，这里钉派生侧那半）。
    #[test]
    fn fixture_g_recheck_required_must_not_enter_mandatory_context() {
        let constraint = [live(Some("v1"), EdgeOutcome::Resolved(token("v2")))];
        let state = derive_grounding_state(GroundingInputs::Edges(&constraint));
        assert!(state.revokes_current_truth_assumption());

        // 快照内入口对同一情形（LIVE + 无版本）给出同样的撤销结论。
        let snap = classify_in_snapshot(&[SnapshotEdge {
            mode: GroundingMode::Live,
            recorded_version_present: false,
        }]);
        match snap {
            RowGrounding::Judged(s) => assert!(
                s.revokes_current_truth_assumption(),
                "快照内派生的 RECHECK_REQUIRED 同样撤销当前真值假设"
            ),
            RowGrounding::NotJudged => panic!("LIVE + 无版本在快照内是可裁的"),
        }
    }

    /// 两个派生入口在重叠情形上必须一致——分类规则只有一份。
    /// 注错：把 `classify_in_snapshot` 的无-LIVE 分支改判 NotJudged ⇒ 本条红。
    #[test]
    fn snapshot_and_full_derivation_agree_on_their_overlap() {
        // 情形①：无 LIVE edge ⇒ 两边都 CURRENT。
        let full = derive_grounding_state(GroundingInputs::Edges(&[GroundingEdge {
            mode: GroundingMode::Snapshot,
            recorded_version: Some(token("old")),
            outcome: EdgeOutcome::Resolved(token("new")),
        }]));
        assert_eq!(full.kind(), GroundingStateKind::Current);
        assert_eq!(
            classify_in_snapshot(&[SnapshotEdge {
                mode: GroundingMode::Snapshot,
                recorded_version_present: true,
            }]),
            RowGrounding::Judged(full),
            "无 LIVE 的情形两入口必须同判 CURRENT"
        );

        // 情形②：LIVE + 无版本 ⇒ 两边都 RECHECK_REQUIRED。
        let full2 = derive_grounding_state(GroundingInputs::Edges(&[live(
            None,
            EdgeOutcome::Resolved(token("whatever")),
        )]));
        assert_eq!(full2.kind(), GroundingStateKind::RecheckRequired);
        assert_eq!(
            classify_in_snapshot(&[SnapshotEdge {
                mode: GroundingMode::Live,
                recorded_version_present: false,
            }]),
            RowGrounding::Judged(full2),
            "LIVE+无版本的情形两入口必须同判 RECHECK_REQUIRED"
        );

        // 情形③：LIVE + 已记版本 ⇒ 快照内不可裁（第三臂，不是 CURRENT 的别名）。
        assert_eq!(
            classify_in_snapshot(&[SnapshotEdge {
                mode: GroundingMode::Live,
                recorded_version_present: true,
            }]),
            RowGrounding::NotJudged,
            "快照里比不了版本——「没判」不许伪装成「判过且通过」"
        );
    }

    // ---- 判定本体的其余约束 ----

    /// 非 LIVE edge 完全不参与：SNAPSHOT/IMMUTABLE 的来源怎么变都不使陈述过时（§8.8 正例）。
    #[test]
    fn non_live_modes_never_make_a_memory_stale() {
        for mode in [GroundingMode::Snapshot, GroundingMode::Immutable] {
            let edges = [GroundingEdge {
                mode,
                recorded_version: Some(token("old")),
                outcome: EdgeOutcome::Resolved(token("brand-new")),
            }];
            assert_eq!(
                kind_of(&edges),
                GroundingStateKind::Current,
                "{mode:?} 的来源变化不应影响 grounding"
            );
        }
    }

    /// 零条 LIVE edge 天然 CURRENT——§8.8「两年前的历史 Memory + SNAPSHOT evidence ⇒ CURRENT」。
    #[test]
    fn no_live_edges_is_vacuously_current() {
        assert_eq!(kind_of(&[]), GroundingStateKind::Current);
    }

    /// 当初没记 version 的 LIVE edge 判 RECHECK_REQUIRED：无法证明「还是同一版」，就没有
    /// 资格继续假设为当前真值。**不是** CURRENT。
    #[test]
    fn live_edge_without_recorded_version_is_recheck_required() {
        let edges = [live(None, EdgeOutcome::Resolved(token("whatever")))];
        assert_eq!(kind_of(&edges), GroundingStateKind::RecheckRequired);
    }

    /// 分母建立不起来 -> CANNOT_ESTABLISH（§8.8 四态定义的最后一项）。
    #[test]
    fn denominator_unavailable_is_cannot_establish() {
        assert_eq!(
            derive_grounding_state(GroundingInputs::DenominatorUnavailable).kind(),
            GroundingStateKind::CannotEstablish
        );
    }

    /// 优先级 `CANNOT_ESTABLISH > UNRESOLVED > RECHECK_REQUIRED > CURRENT`：混合 edge 集合
    /// 取最严者，且**顺序无关**（逐个旋转输入顺序都必须同答案，防止实现写成「取第一个」）。
    #[test]
    fn severity_priority_holds_regardless_of_edge_order() {
        let mut edges = vec![
            live(Some("v1"), EdgeOutcome::Resolved(token("v1"))), // CURRENT
            live(Some("v1"), EdgeOutcome::Resolved(token("v2"))), // RECHECK_REQUIRED
            live(Some("v1"), EdgeOutcome::Missing),               // UNRESOLVED
            live(
                Some("v1"),
                EdgeOutcome::ResolveFailed(GroundingResolveError::Permanent("policy".into())),
            ), // CANNOT_ESTABLISH
        ];
        for _ in 0..edges.len() {
            assert_eq!(kind_of(&edges), GroundingStateKind::CannotEstablish);
            edges.rotate_left(1);
        }

        // 去掉最严的一条，答案必须降一档——否则「恒返回 CannotEstablish」也能让上面全绿。
        let without_failure = &edges[..3];
        assert_eq!(kind_of(without_failure), GroundingStateKind::Unresolved);
    }

    /// `revokes_current_truth_assumption` 只对 CURRENT 放行——DOD-093 的准入判据读它。
    #[test]
    fn only_current_keeps_the_truth_assumption() {
        assert!(!GroundingStateKind::Current.revokes_current_truth_assumption());
        for k in [
            GroundingStateKind::RecheckRequired,
            GroundingStateKind::Unresolved,
            GroundingStateKind::CannotEstablish,
        ] {
            assert!(k.revokes_current_truth_assumption(), "{k:?} 必须撤销资格");
        }
    }

    /// `EvidenceObjectDraft` 走 §8.1 的真哈希函数，不自造第二个身份计算。
    #[test]
    fn draft_identity_uses_the_canonical_payload_hash() {
        let draft = EvidenceObjectDraft {
            payload_sha256: payload_sha256(b"resolved bytes"),
            evidence_kind: "ARTIFACT".into(),
        };
        assert_eq!(draft.payload_sha256, payload_sha256(b"resolved bytes"));
        assert_ne!(draft.payload_sha256, payload_sha256(b"other bytes"));
    }
}
