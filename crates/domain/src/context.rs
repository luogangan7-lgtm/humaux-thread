//! `domain::context` — §25.4 Mandatory Context Lane + §25.5 Budget/Overflow。
//!
//! DOD-020（**phase=7，已欠账**）：「Mandatory/Pinned Context 不参与 semantic 淘汰；
//! mandatory overflow 只能 `cannot_establish`，不能静默截断。」
//!
//! 本模块零 IO——SQL 全在 `adapters::context_repo`。三条保证各自靠什么成立，逐条说清，
//! 因为「靠调用方守纪律」不算保证：
//!
//! 1. **不用 semantic 相似度选 Mandatory**：[`SelectorInput`] 里**没有** query、没有
//!    embedding、没有 top_k。§25.4 那句「active UserCorrection relevant to scope 不允许用
//!    embedding similarity 解释」不是被禁止，是**写不出来**——这是本模块唯一一条纯拓扑保证。
//! 2. **Mandatory 不可被 rerank 淘汰**：[`MandatoryRow`] 没有 score/rank 字段、不实现
//!    `Clone`，且唯一铸造点 [`MandatoryRow::from_selector`] 要一个 `&'static SelectorSpec`
//!    ——那只能来自 [`REGISTRY`] 的五个 const，semantic lane 手里没有它可交。
//! 3. **溢出不可静默截断**：[`ContextBudget::reserve`] 按值消费 `self`，且是拿到
//!    [`SupplementalBudget`] 的**唯一**路径；溢出走 `Err` 臂，而 [`MandatoryOverflow`]
//!    **不含任何可返回的 Context**。「截掉后半段还声称 complete」不是不该做的操作，
//!    是那个臂里没有那个值可以返回。
//!
//! 写侧：MANDATORY 见 [`ElevatedActor`]——今天**没有铸造路径**。PINNED 见
//! [`ConfirmedUserActor`]——唯一铸造点是 §33.10 规则 9 的 confirm_token 被消费之后（ADR-0019）。

use crate::authority::{
    AuthorityClass, AuthorityPolicy, AuthorityStatus, CandidateRejection, MemoryId, NonEmptyVec,
};
use crate::confirm::DestructiveOp;
use crate::error::ErrorCode;
use crate::evidence::EvidenceOriginClass;
use crate::grounding::{GroundingState, GroundingStateKind, RowGrounding};
use crate::ids::Scope;
use crate::memory::MemoryType;
use std::collections::HashSet;
use uuid::Uuid;

// =============================================================================
// §25.4 ContextSelectorRegistry
// =============================================================================

/// §25.4 逐字点名的五个确定性 selector。**闭集**——加一个 selector 必须改这里，
/// 编译器会逼所有 `match` 跟着改。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SelectorId {
    /// 任务显式指定的上下文。
    TaskExplicitContextV1,
    /// 项目当前生效的约束。
    ProjectActiveConstraintsV1,
    /// 用户已确认的纠正。
    UserConfirmedCorrectionsV1,
    /// 必须带上的 current-state facet。
    RequiredCurrentStateFacetsV1,
    /// 显式建立的 MANDATORY binding。
    ExplicitMandatoryBindingsV1,
}

/// §59 Scope 的层级。由窄到宽，[`scope_chain`] 按这个序展开。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScopeKind {
    /// 单个 agent。
    Agent,
    /// 单次 run。
    Run,
    /// 单个 task。
    Task,
    /// 单个 repository。
    Repository,
    /// 单个 workspace。
    Workspace,
    /// 单个 user。
    User,
    /// 整个租户（最宽，恒存在）。
    Tenant,
}

impl ScopeKind {
    /// DB 侧 `private.context_bindings.scope_kind` 的线值（migration 0102 的 CHECK 闭集）。
    #[must_use]
    pub const fn wire(self) -> &'static str {
        match self {
            Self::Agent => "AGENT",
            Self::Run => "RUN",
            Self::Task => "TASK",
            Self::Repository => "REPOSITORY",
            Self::Workspace => "WORKSPACE",
            Self::User => "USER",
            Self::Tenant => "TENANT",
        }
    }
}

/// §25.4 每个 selector 要声明的 freshness 规则。
///
// ponytail: 今天只有一档。不先造第二档——第二档的语义得由真需求定，
// 猜一个出来只会让 `match` 多一条永远走不到的臂。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreshnessRule {
    /// 只收 `AuthorityStatus::Active` 且未被撤销的。
    ActiveOnly,
}

/// §25.4 要求每个 selector 声明的七项。**声明在 domain，查询在 adapters**——
/// 本结构不含 SQL，只含「要什么」。
#[derive(Debug, Clone, Copy)]
pub struct SelectorSpec {
    /// 身份。
    pub id: SelectorId,
    /// scope 继承链，由窄到宽。
    pub scope_inheritance: &'static [ScopeKind],
    /// 所选 memory 的 evidence 必须至少命中其一的 origin（空 = 不约束 origin）。
    pub required_origin: &'static [EvidenceOriginClass],
    /// 所选 memory 的 authority 下限。
    pub min_authority: AuthorityClass,
    /// freshness 规则。
    pub freshness: FreshnessRule,
    /// owner，写模块路径不写人名（人会走，模块不会）。
    pub owner: &'static str,
    /// §25.4 第七项：正夹具的测试函数名。由 xtask 臂断言这个名字真的存在。
    pub positive_fixture: &'static str,
    /// 负夹具的测试函数名。
    pub negative_fixture: &'static str,
    /// 这个 selector 需要哪些列才跑得起来：`(schema, table, column)`。
    ///
    /// domain 只声明名字，**不发查询**；`adapters::context_repo::probe_selectors` 拿它去比
    /// `information_schema.columns`。列一落地 selector 自动可用，不需要有人回来改代码——
    /// 这是 ADR-0006 那条「NA 的缺失对象必须是探测出来的」在本模块的落点。
    pub required_columns: &'static [(&'static str, &'static str, &'static str)],
}

/// §25.4 的五个 selector。**唯一真源**——`spec()` 之外没有第二处描述它们。
pub const REGISTRY: [SelectorSpec; 5] = [
    SelectorSpec {
        id: SelectorId::TaskExplicitContextV1,
        scope_inheritance: &[ScopeKind::Task],
        required_origin: &[],
        min_authority: AuthorityClass::ExplicitTaskContext,
        freshness: FreshnessRule::ActiveOnly,
        owner: "domain::context",
        positive_fixture: "task_explicit_context_positive",
        negative_fixture: "task_explicit_context_negative",
        // §25.4 待交付：`private.memory_records` 今天没有 task 维度。
        required_columns: &[("private", "memory_records", "task_id")],
    },
    SelectorSpec {
        id: SelectorId::ProjectActiveConstraintsV1,
        scope_inheritance: &[ScopeKind::Workspace, ScopeKind::Tenant],
        required_origin: &[],
        min_authority: AuthorityClass::ProjectConstraint,
        freshness: FreshnessRule::ActiveOnly,
        owner: "domain::context",
        positive_fixture: "project_active_constraints_positive",
        negative_fixture: "project_active_constraints_negative",
        required_columns: &[
            ("private", "memory_records", "authority_class"),
            ("private", "memory_records", "status"),
        ],
    },
    SelectorSpec {
        id: SelectorId::UserConfirmedCorrectionsV1,
        scope_inheritance: &[ScopeKind::User, ScopeKind::Tenant],
        // §25.4「active UserCorrection relevant to scope 不允许用 embedding similarity
        // 解释」——机械替代品就是这一条 origin 约束。
        required_origin: &[EvidenceOriginClass::UserConfirmed],
        min_authority: AuthorityClass::UserCorrection,
        freshness: FreshnessRule::ActiveOnly,
        owner: "domain::context",
        positive_fixture: "user_confirmed_corrections_positive",
        negative_fixture: "user_confirmed_corrections_negative",
        required_columns: &[("private", "evidence_objects", "origin_class")],
    },
    SelectorSpec {
        id: SelectorId::RequiredCurrentStateFacetsV1,
        scope_inheritance: &[ScopeKind::Workspace, ScopeKind::Tenant],
        required_origin: &[],
        min_authority: AuthorityClass::PrivateKnowledge,
        freshness: FreshnessRule::ActiveOnly,
        owner: "domain::context",
        positive_fixture: "required_current_state_facets_positive",
        negative_fixture: "required_current_state_facets_negative",
        // §25.4 待交付：`memory_records` 无 facet 列；且 §25.2 的五 facet 与 §24 的九变体
        // 之间没有对齐条款——**不猜映射**，猜错会让 G25-1 在小夹具上偶然变绿。
        required_columns: &[("private", "memory_records", "facet")],
    },
    SelectorSpec {
        id: SelectorId::ExplicitMandatoryBindingsV1,
        scope_inheritance: &[
            ScopeKind::Agent,
            ScopeKind::Run,
            ScopeKind::Task,
            ScopeKind::Repository,
            ScopeKind::Workspace,
            ScopeKind::User,
            ScopeKind::Tenant,
        ],
        required_origin: &[],
        min_authority: AuthorityClass::ProjectConstraint,
        freshness: FreshnessRule::ActiveOnly,
        owner: "domain::context",
        positive_fixture: "explicit_mandatory_bindings_positive",
        negative_fixture: "explicit_mandatory_bindings_negative",
        required_columns: &[
            ("private", "context_bindings", "mode"),
            ("private", "context_bindings", "revoked_at"),
        ],
    },
];

/// 按 id 取 spec。
#[must_use]
pub const fn spec(id: SelectorId) -> &'static SelectorSpec {
    match id {
        SelectorId::TaskExplicitContextV1 => &REGISTRY[0],
        SelectorId::ProjectActiveConstraintsV1 => &REGISTRY[1],
        SelectorId::UserConfirmedCorrectionsV1 => &REGISTRY[2],
        SelectorId::RequiredCurrentStateFacetsV1 => &REGISTRY[3],
        SelectorId::ExplicitMandatoryBindingsV1 => &REGISTRY[4],
    }
}

/// §25.4「relevant to scope」的**机械定义**，全仓唯一一处。
///
/// 把 [`Scope`] 展成由窄到宽的 `(kind, id)` 链；未narrow 到的层不出现。`Tenant` 恒在最后
/// （它是必填的）。selector 用它做 scope 继承，不用任何相似度。
#[must_use]
pub fn scope_chain(scope: &Scope) -> Vec<(ScopeKind, Uuid)> {
    let mut chain = Vec::with_capacity(7);
    if let Some(id) = scope.agent_id {
        chain.push((ScopeKind::Agent, id.0));
    }
    if let Some(id) = scope.run_id {
        chain.push((ScopeKind::Run, id.0));
    }
    if let Some(id) = scope.task_id {
        chain.push((ScopeKind::Task, id.0));
    }
    if let Some(id) = scope.repository_id {
        chain.push((ScopeKind::Repository, id.0));
    }
    if let Some(id) = scope.workspace_id {
        chain.push((ScopeKind::Workspace, id.0));
    }
    if let Some(id) = scope.user_id {
        chain.push((ScopeKind::User, id.0));
    }
    chain.push((ScopeKind::Tenant, scope.tenant_id.0));
    chain
}

/// freshness 判定的纯函数形态。
///
/// 单独抽出来是为了让 §25.5 的正对照（binding 被撤销 / memory 被 supersede 之后必须从
/// Context 里消失）**不依赖真库**也有一个能红的形态。
#[must_use]
pub const fn admits(rule: FreshnessRule, status: AuthorityStatus, revoked: bool) -> bool {
    match rule {
        FreshnessRule::ActiveOnly => matches!(status, AuthorityStatus::Active) && !revoked,
    }
}

/// 五个 selector 的**全部**输入。
///
/// 没有 `query: String`，没有 `embedding: Vec<f32>`，没有 `top_k`。§25.4 那句
/// 「不允许用 embedding similarity 解释」因此不是一条需要有人遵守的禁令，而是**写不出来**。
/// 往这里加一个 query 字段的那次改动，就是这条保证失效的那一刻——所以它值得被单独看着。
#[derive(Debug, Clone, Copy)]
pub struct SelectorInput<'a> {
    /// 唯一输入。
    pub scope: &'a Scope,
}

// =============================================================================
// §25.4 步骤 2/5：两条不参与 semantic 淘汰的 lane
// =============================================================================

/// 一条 Mandatory 行。
///
/// **没有 score / rank / fusion_score 字段，也不实现 `Clone`。** 唯一铸造点是
/// [`Self::from_selector`]，它要一个 `&'static SelectorSpec`——那只能来自 [`REGISTRY`]
/// 的五个 const。semantic lane 手里没有 `SelectorId` 可交，因此**造不出**一条 MandatoryRow
/// 混进排序，反过来也一样：这个类型进不了 `retrieval::Candidate` 的位置。
#[derive(Debug, PartialEq, Eq)]
pub struct MandatoryRow {
    memory_id: MemoryId,
    selector: SelectorId,
    authority: AuthorityClass,
    est_tokens: u32,
    grounding_state: Option<GroundingState>,
}

/// DOD-093 的 fail-loud 载体：一条**没能**进 lane 的行，与它没能进的原因。
///
/// 它不是错误——装配继续；它是必须出现在 `needs_verification[]` 顶层块里的披露。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NeedsVerification {
    /// 哪条 memory。
    pub memory_id: MemoryId,
    /// 哪个 selector 想选它。
    pub selector: SelectorId,
    /// 被撤销资格时的 grounding 状态（RECHECK_REQUIRED / UNRESOLVED / CANNOT_ESTABLISH）。
    pub state: GroundingStateKind,
}

/// [`MandatoryRow::from_selector`] 的产出：铸成了，或者分流进 needs_verification。
///
/// **两臂都不是错误**——`Err` 留给「参数本身不合法」（authority 低于下限）。DOD-093 的
/// 「不得静默消费」靠这个形状成立：RECHECK_REQUIRED 的行**铸不出** `MandatoryRow`，
/// 而 [`NeedsVerification`] 是必须上报的另一个类型——「静默」没有可写的形态。
#[derive(Debug)]
pub enum Admitted {
    /// 通过铸造门。
    Row(MandatoryRow),
    /// §8.8 撤销了「继续当作当前真值」的资格（DOD-093）。
    NeedsVerification(NeedsVerification),
}

impl MandatoryRow {
    /// 唯一铸造点。三道门，顺序即语义：
    ///
    /// 1. `authority` 低于 `spec.min_authority` ⇒ `Err`（参数不合法，不是分流）；
    /// 2. `grounding` 撤销当前真值资格（[`GroundingStateKind::revokes_current_truth_assumption`]）
    ///    ⇒ [`Admitted::NeedsVerification`]——**row 铸不出来**，这就是 DOD-093 的
    ///    「Mandatory/Pinned 不得静默消费」在类型上的形状（§11.10 注错四的落点）；
    /// 3. [`RowGrounding::NotJudged`]（快照内不可裁）⇒ 铸进 row 但打上 `not_judged`——
    ///    「没判」进 lane 但必须具名披露，不许伪装成「判过且通过」。
    ///
    /// # Errors
    /// `authority` 低于 `spec.min_authority` 时返回 [`ErrorCode::InvalidInput`]。
    pub fn from_selector(
        spec: &'static SelectorSpec,
        memory_id: MemoryId,
        authority: AuthorityClass,
        est_tokens: u32,
        grounding: RowGrounding,
    ) -> Result<Admitted, ErrorCode> {
        if (authority as u8) < (spec.min_authority as u8) {
            return Err(ErrorCode::InvalidInput);
        }
        let grounding_state = match grounding {
            RowGrounding::Judged(state) => {
                if state.revokes_current_truth_assumption() {
                    return Ok(Admitted::NeedsVerification(NeedsVerification {
                        memory_id,
                        selector: spec.id,
                        state: state.kind(),
                    }));
                }
                Some(state)
            }
            RowGrounding::NotJudged => None,
        };
        Ok(Admitted::Row(Self {
            memory_id,
            selector: spec.id,
            authority,
            est_tokens,
            grounding_state,
        }))
    }

    /// 快照内没能裁定 grounding 的行（见 [`RowGrounding::NotJudged`]）。上报用。
    #[must_use]
    pub const fn not_judged(&self) -> bool {
        self.grounding_state.is_none()
    }

    /// §8.8 已经派生的 grounding 状态；`None` 表示本快照未裁定。
    #[must_use]
    pub const fn grounding_state(&self) -> Option<GroundingState> {
        self.grounding_state
    }

    /// 这行指向的 memory。
    #[must_use]
    pub const fn memory_id(&self) -> MemoryId {
        self.memory_id
    }

    /// 是哪个 selector 选进来的（上报与审计用）。
    #[must_use]
    pub const fn selector(&self) -> SelectorId {
        self.selector
    }

    /// 该行的 authority。
    #[must_use]
    pub const fn authority(&self) -> AuthorityClass {
        self.authority
    }

    /// 估计占用的 token 数（§25.5 预算依据）。
    #[must_use]
    pub const fn est_tokens(&self) -> u32 {
        self.est_tokens
    }
}

/// 一个 selector 跑完的结果。
///
/// **`Unavailable` 与「跑了但零行」是两回事**，所以它们是两个变体而不是一个空 `rows`：
/// 前者是「这个 selector 的被测对象还不存在」（§57.1 的 not_applicable，缺失对象由 probe
/// 探测出来），后者是「确实没有符合的 memory」。混成一个，Context 就会在 selector 坏掉时
/// 安静地少带东西。
#[derive(Debug)]
pub enum SelectorOutcome {
    /// 独立、无 LIMIT 的授权候选枚举，与返回 rows 分离。保留 ID 才能在 selector
    /// 重叠时建立唯一全集；用 rows.len() 会让 §25.5 的守恒式退化成恒真算术。
    Ran {
        /// 哪个 selector。
        id: SelectorId,
        /// 独立授权枚举得到的候选 identity；不是 rows 的派生值。
        candidate_ids: Vec<MemoryId>,
        /// 实际取回的行（已过铸造门）。
        rows: Vec<MandatoryRow>,
        /// 被铸造门分流的行（DOD-093：与 `rows` 同源同到场，不许丢）。
        needs_verification: Vec<NeedsVerification>,
    },
    /// 被测对象缺席。
    Unavailable {
        /// 哪个 selector。
        id: SelectorId,
        /// **探测出来的**缺失对象名（如 `private.memory_records.task_id`）。
        missing_object: String,
    },
}

/// §25.4 步骤 2 的产物。字段私有：`expected` 不许被调用方改写。
#[derive(Debug)]
pub struct MandatoryLane {
    expected: u64,
    rows: Vec<MandatoryRow>,
    needs_verification: Vec<NeedsVerification>,
    unavailable: Vec<(SelectorId, String)>,
}

impl MandatoryLane {
    /// 唯一构造点，收**定长数组**而不是 `Vec`——调用方少传一个 selector 会编译不过。
    ///
    /// **Unavailable 可形成 partial lane，不因缺失 selector 整条拒建。** 上一版：任一
    /// `Unavailable` ⇒ `Err(LaneUnavailable)`。后果（实测）：真 schema 缺
    /// `private.memory_records.task_id` 与 `.facet` 两列，两个 selector **恒**不可用 ⇒
    /// lane **永远**构不出来 ⇒ G80-31 永远 NA——按「没有注错红转绿的闸不算存在」，
    /// 整条 Phase 8 出场判据就不存在。
    ///
    /// §25.5 禁的是「静默截掉仍声称 complete」，不禁 **loud partial**：`unavailable`
    /// 逐个具名（probe 探测出的缺失对象），completeness 在它非空时构造不出 complete
    /// （envelope 侧断言），handoff 顶层块如实携带。缺列补上的那天 probe 自动改口，
    /// 无人需要回来改代码（ADR-0006）。
    ///
    /// 同一候选的治理状态矛盾、行不属于自己的 selector 候选集时返回 Internal。
    pub fn from_selectors(out: [SelectorOutcome; 5]) -> Result<Self, ErrorCode> {
        let mut unavailable = Vec::new();
        let mut candidate_ids = HashSet::new();
        let mut seen_selectors = HashSet::new();
        let mut rows: Vec<MandatoryRow> = Vec::new();
        let mut needs: Vec<NeedsVerification> = Vec::new();
        for outcome in out {
            let id = match &outcome {
                SelectorOutcome::Ran { id, .. } | SelectorOutcome::Unavailable { id, .. } => *id,
            };
            if !seen_selectors.insert(id) {
                return Err(ErrorCode::Internal);
            }
            match outcome {
                SelectorOutcome::Unavailable { id, missing_object } => {
                    unavailable.push((id, missing_object));
                }
                SelectorOutcome::Ran {
                    id,
                    candidate_ids: candidates,
                    rows: mut selector_rows,
                    needs_verification: mut selector_needs,
                } => {
                    let selector_candidates: HashSet<MemoryId> = candidates.into_iter().collect();
                    if selector_rows.iter().any(|row| {
                        row.selector() != id || !selector_candidates.contains(&row.memory_id())
                    }) || selector_needs.iter().any(|need| {
                        need.selector != id || !selector_candidates.contains(&need.memory_id)
                    }) {
                        return Err(ErrorCode::Internal);
                    }
                    candidate_ids.extend(selector_candidates);
                    rows.append(&mut selector_rows);
                    needs.append(&mut selector_needs);
                }
            }
        }
        rows.sort_by_key(|row| (row.memory_id.0, row.selector as u8));
        needs.sort_by_key(|need| (need.memory_id.0, need.selector as u8));
        unavailable.sort_by_key(|(id, _)| *id as u8);

        let mut unique_rows: Vec<MandatoryRow> = Vec::with_capacity(rows.len());
        for row in rows {
            if !candidate_ids.contains(&row.memory_id()) {
                return Err(ErrorCode::Internal);
            }
            if let Some(previous) = unique_rows.last()
                && previous.memory_id() == row.memory_id()
            {
                if previous.grounding_state() != row.grounding_state() {
                    return Err(ErrorCode::Internal);
                }
                continue;
            }
            unique_rows.push(row);
        }

        let mut unique_needs: Vec<NeedsVerification> = Vec::with_capacity(needs.len());
        for need in needs {
            if !candidate_ids.contains(&need.memory_id) {
                return Err(ErrorCode::Internal);
            }
            if unique_rows
                .iter()
                .any(|row: &MandatoryRow| row.memory_id() == need.memory_id)
            {
                return Err(ErrorCode::Internal);
            }
            if let Some(previous) = unique_needs.last()
                && previous.memory_id == need.memory_id
            {
                if previous.state != need.state {
                    return Err(ErrorCode::Internal);
                }
                continue;
            }
            unique_needs.push(need);
        }

        unique_rows.sort_by_key(|row| (row.selector as u8, row.memory_id.0));
        unique_needs.sort_by_key(|need| (need.selector as u8, need.memory_id.0));
        Ok(Self {
            expected: candidate_ids.len() as u64,
            rows: unique_rows,
            needs_verification: unique_needs,
            unavailable,
        })
    }

    /// 被铸造门分流的行（DOD-093 的 `needs_verification[]` 来源）。
    #[must_use]
    pub fn needs_verification(&self) -> &[NeedsVerification] {
        &self.needs_verification
    }

    /// 不可用的 selector 与**探测出的**缺失对象名。非空 ⇒ 这条 lane 是 partial，
    /// completeness 不得报 complete。
    #[must_use]
    pub fn unavailable(&self) -> &[(SelectorId, String)] {
        &self.unavailable
    }

    /// 可用 selector 独立授权候选 ID 的并集基数；不可用的 selector 另行具名。
    #[must_use]
    pub const fn expected(&self) -> u64 {
        self.expected
    }

    /// 实际取回的行。**只读借用**——交付需要读它，但拿不到所有权就没法把它塞进别的排序结构。
    #[must_use]
    pub fn rows(&self) -> &[MandatoryRow] {
        &self.rows
    }

    /// 实际条数。是算出来的，不是字段——字段会和 `rows` 漂移。
    #[must_use]
    pub fn returned(&self) -> u64 {
        self.rows.len() as u64
    }

    /// 差额。`expected` 与 `returned` 之差，§25.5 的 `mandatory.missing`。
    #[must_use]
    pub fn missing(&self) -> u64 {
        self.expected.saturating_sub(self.returned())
    }

    /// 这条 lane 要占的 token 总量。
    #[must_use]
    pub fn total_tokens(&self) -> u32 {
        self.rows
            .iter()
            .fold(0u32, |acc, r| acc.saturating_add(r.est_tokens))
    }

    /// 拆成 `(expected, rows)` 交给装配层。
    ///
    /// [`MandatoryRow`] 刻意不实现 `Clone`（见它的 doc），所以装配层要把行放进最终 Context
    /// 就必须**拿走所有权**——`self` 按值消费。副作用正是想要的：一条 lane 只能被装配一次，
    /// 装完之后调用方手里没有第二份可以再塞进别处。
    #[must_use]
    pub fn into_parts(self) -> (u64, Vec<MandatoryRow>) {
        (self.expected, self.rows)
    }
}

/// §25.4 步骤 5 的产物。没有 `missing`（Pinned 是显式钉的），但有 **`expected` 与
/// `excluded`**：`expected` 来自独立 COUNT（外部 oracle，防「钉 3 带 2 不可观测」——
/// 内部守恒式在 expected 内生时恒真），`excluded` 具名被跳过的低 authority 行。
#[derive(Debug)]
pub struct PinnedLane {
    expected: u64,
    rows: Vec<MandatoryRow>,
    excluded: Vec<MemoryId>,
}

fn mandatory_memory_ids(mandatory: &MandatoryLane) -> HashSet<MemoryId> {
    mandatory
        .rows()
        .iter()
        .map(MandatoryRow::memory_id)
        .collect()
}

fn is_pinned_only(row: &MandatoryRow, mandatory_ids: &HashSet<MemoryId>) -> bool {
    !mandatory_ids.contains(&row.memory_id())
}

impl PinnedLane {
    /// 唯一构造点。`expected` 必须来自与取行分离的独立 COUNT（同 Mandatory 的纪律）。
    #[must_use]
    pub fn new(expected: u64, mut rows: Vec<MandatoryRow>, mut excluded: Vec<MemoryId>) -> Self {
        rows.sort_by_key(|r| (r.selector as u8, r.memory_id.0));
        excluded.sort_by_key(|m| m.0);
        Self {
            expected,
            rows,
            excluded,
        }
    }

    /// Removes Pinned rows already emitted by Mandatory. The independent PINNED `expected`
    /// oracle remains unchanged; the overlap is named as excluded so the lane's coverage
    /// remains observable while one Memory consumes one Context slot and token charge.
    #[must_use]
    pub fn excluding_mandatory(mut self, mandatory: &MandatoryLane) -> Self {
        let mandatory_ids = mandatory_memory_ids(mandatory);
        self.rows.retain(|row| {
            if is_pinned_only(row, &mandatory_ids) {
                true
            } else {
                self.excluded.push(row.memory_id());
                false
            }
        });
        self.excluded.sort_by_key(|memory_id| memory_id.0);
        self.excluded.dedup();
        self
    }

    /// 独立 COUNT 得到的「钉了几条」。
    #[must_use]
    pub const fn expected(&self) -> u64 {
        self.expected
    }

    /// 被跳过的低 authority 行，具名——「钉 3 带 2」必须可观测。
    #[must_use]
    pub fn excluded(&self) -> &[MemoryId] {
        &self.excluded
    }

    /// 实际取回的行。
    #[must_use]
    pub fn rows(&self) -> &[MandatoryRow] {
        &self.rows
    }

    /// 条数。
    #[must_use]
    pub fn returned(&self) -> u64 {
        self.rows.len() as u64
    }

    /// token 总量。
    #[must_use]
    pub fn total_tokens(&self) -> u32 {
        self.rows
            .iter()
            .fold(0u32, |acc, r| acc.saturating_add(r.est_tokens))
    }

    /// 拿走行。理由同 [`MandatoryLane::into_parts`]。
    #[must_use]
    pub fn into_rows(self) -> Vec<MandatoryRow> {
        self.rows
    }
}

/// 一次装配的**全部冻结读数**——[`crate::grounding`] 的快照内派生 + 两条 lane +
/// 快照身份。纯数据无 IO；`retrieval` 侧的 handoff 装配只收它，收不到时钟、收不到连接。
///
/// **`snapshot_token_sha256` 才是快照身份**；`context_snapshot_seq`（= `pg_snapshot_xmin`）
/// 是 §1.2.1/§16.1 的 fingerprint 轴，**必要非充分**：并发长事务钉住 xmin 时，两次装配
/// 可以同 seq 而读到不同集合（写入只对第二次可见）。「同快照 ⇒ 同字节」的充分条件是
/// 完整 snapshot token（xmin:xmax:xip 全等 ⇒ 可见性全等）。
#[derive(Debug)]
pub struct FrozenReads {
    /// §25.4 步骤 2。
    pub mandatory: MandatoryLane,
    /// §25.4 步骤 5。
    pub pinned: PinnedLane,
    /// `pg_snapshot_xmin(pg_current_snapshot())`——fingerprint 轴，必要非充分（见类型 doc）。
    pub context_snapshot_seq: i64,
    /// `SHA256(pg_current_snapshot()::text)`——快照身份，逐字节重现的充分条件。
    pub snapshot_token_sha256: String,
}

/// §25.5 守恒：`expected == returned + missing`。
///
/// **刻意不复用 [`crate::ledger::a1_holds`]**：那是四参三加数的 A1（§23.1②
/// `done + open_gaps + pending == expected`），硬塞一个 0 进去凑参数，正是 §23.1① 骂的
/// 「装饰性列」。两个恒等式长得像不代表是同一个——同一个才该只有一处实现。
///
/// 放在本模块而不是 `ledger.rs`：让「只此一处」是依赖拓扑的结论而不是纪律，
/// 与 `ledger.rs` 自己的理由同源。
#[must_use]
pub const fn mandatory_accounted(expected: u64, returned: u64, missing: u64) -> bool {
    expected == returned.saturating_add(missing)
}

// =============================================================================
// §25.5 预算 / 溢出
// =============================================================================

/// §25.4 步骤 4 的输入。
///
/// cap 由调用方从上层配置传入，**domain 不内置默认值**——默认值就是第二真源（§78.1）。
#[derive(Debug, Clone, Copy)]
pub struct ContextBudget {
    total_tokens: u32,
    mandatory_cap_tokens: u32,
}

impl ContextBudget {
    /// 构造。
    ///
    /// # Errors
    /// `mandatory_cap_tokens > total_tokens` 时返回 [`ErrorCode::InvalidInput`]——
    /// 一个比总额还大的 mandatory 上限是配置错误，不是运行期溢出。
    pub const fn new(total_tokens: u32, mandatory_cap_tokens: u32) -> Result<Self, ErrorCode> {
        if mandatory_cap_tokens > total_tokens {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            total_tokens,
            mandatory_cap_tokens,
        })
    }

    /// §25.4 步骤 4：给 Mandatory + Pinned 预留，返回补充位可用的剩余预算。
    ///
    /// **这是拿到 [`SupplementalBudget`] 的唯一路径，且 `self` 按值消费**：不先给
    /// Mandatory 预留就没有补充位预算可用，而且预算不能被重复领取。
    ///
    /// # Errors
    /// Mandatory 自身超过硬上限 ⇒ [`MandatoryOverflow`]。注意这个错误类型里
    /// **没有任何可返回的 Context**——§25.5「禁止静默截掉后半段并仍声称 complete」
    /// 因此不是一条禁令，是 `Err` 臂里没有那个值可以返回。
    pub fn reserve(
        self,
        m: &MandatoryLane,
        p: &PinnedLane,
    ) -> Result<SupplementalBudget, MandatoryOverflow> {
        let mandatory_ids = mandatory_memory_ids(m);
        let pinned_rows: Vec<&MandatoryRow> = p
            .rows()
            .iter()
            .filter(|row| is_pinned_only(row, &mandatory_ids))
            .collect();
        let pinned_tokens = pinned_rows
            .iter()
            .fold(0u32, |acc, row| acc.saturating_add(row.est_tokens));
        let required = m.total_tokens().saturating_add(pinned_tokens);
        if required > self.mandatory_cap_tokens {
            return Err(MandatoryOverflow {
                expected: m.expected(),
                manifest: m
                    .rows()
                    .iter()
                    .chain(pinned_rows)
                    .map(MandatoryRow::memory_id)
                    .collect(),
                budget_tokens: self.mandatory_cap_tokens,
                required_tokens: required,
            });
        }
        Ok(SupplementalBudget(
            self.total_tokens.saturating_sub(required),
        ))
    }
}

/// 补充位可用的 token 预算。
///
/// 私有字段、无 `Default`、无 `From<u32>`、无第二构造点——只能由
/// [`ContextBudget::reserve`] 产出。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupplementalBudget(u32);

impl SupplementalBudget {
    /// 剩余 token。
    #[must_use]
    pub const fn tokens(self) -> u32 {
        self.0
    }
}

/// §25.5 溢出。
///
/// **本类型不含任何 Context / rows / 可交付内容**，这是刻意的：调用方在这个臂里
/// 拿不到「截断后的前半段」，所以「截掉后半段还声称 complete」写不出来。
/// 它只带 §25.5 要求的 manifest/IDs 与缩减建议所需的量纲。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MandatoryOverflow {
    expected: u64,
    manifest: Vec<MemoryId>,
    budget_tokens: u32,
    required_tokens: u32,
}

impl MandatoryOverflow {
    /// 应有条数。
    #[must_use]
    pub const fn expected(&self) -> u64 {
        self.expected
    }

    /// §25.5 要求返回的 manifest/IDs，**全量**——分页建议要靠它才提得出来。
    #[must_use]
    pub fn manifest(&self) -> &[MemoryId] {
        &self.manifest
    }

    /// 硬上限。
    #[must_use]
    pub const fn budget_tokens(&self) -> u32 {
        self.budget_tokens
    }

    /// 实际需要。与上限之差就是缩减建议的量纲。
    #[must_use]
    pub const fn required_tokens(&self) -> u32 {
        self.required_tokens
    }
}

// =============================================================================
// §25.4 binding 授权
// =============================================================================

/// binding 的三档。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingMode {
    /// 必带，不参与 semantic 淘汰。
    Mandatory,
    /// 用户/管理员显式钉住。
    Pinned,
    /// 普通补充位。
    Supplemental,
}

impl BindingMode {
    /// DB 线值（migration 0102 的 CHECK 闭集）。
    #[must_use]
    pub const fn wire(self) -> &'static str {
        match self {
            Self::Mandatory => "MANDATORY",
            Self::Pinned => "PINNED",
            Self::Supplemental => "SUPPLEMENTAL",
        }
    }
}

/// 建 MANDATORY binding 所需的提权 actor。
///
/// **今天没有任何铸造函数，这是刻意的，也是唯一诚实的答案。** §25.4 要求
/// 「普通 Agent 只能 propose，不能自己 promote」，而仓里现在拿不出能表达这个区分的东西：
/// §6 的 `Principal` 全仓零命中；[`crate::identity::AuthorizationScope`] 没有角色轴，
/// 唯一能用的 `user_id.is_some()` **判别方向还是反的**（它的 doc 明写：代用户行动的 agent
/// 正是 `Some`，无特定用户的服务才是 `None`）。
///
/// 与其放一个判反的门假装挡住了，不如把写路径结构性关死：MANDATORY binding 今天**建不出来**。
/// §25.4 那条攻击路径（诱导 Agent 调一次 `memory.pin` 就把低 origin 内容永久钉进 Context）
/// 因此不成立——不是因为门够严，是因为没有门可以走。
///
// ponytail: 铸造点等 §6 `Principal` 落地后加，加的同时必须配一条注错证明「普通 Agent
// 拿不到它」。在那之前不要为了「让流程跑通」而加一个 `pub fn new()`。
#[derive(Debug)]
pub struct ElevatedActor {
    _priv: (),
}

/// 建 PINNED binding 所需的、经过交互确认的用户 actor（ADR-0019 D-A）。
///
/// 唯一铸造点是 [`ConfirmedUserActor::from_consumed_confirmation`]，且只认
/// [`DestructiveOp::MemoryPin`] / [`DestructiveOp::MemoryUnpin`]：调用方必须**刚刚在同一事务里
/// 消费了**那条 memory 的 confirm_token（`adapters::confirm_token_repo::consume_in_txn`，
/// 0 行即 `Conflict`，永远到不了这里）。它绑定一条 memory：拿 X 的确认去建 Y 的 binding，
/// [`authorize_pinned`] 以 `MissingConfirmation` 拒。字段私有、无 `Default`、无字面量构造
/// （`tests/ui/fail_confirmed_user_actor_literal.rs` 注错证明），所以 consolidation /
/// retention / private-worker 代码即便拿到 `BindingRequest` 也造不出它。
///
// 「只有 context_repo 的确认信封能调 from_consumed_confirmation」在 Rust 可见性上表达不出来
// （同 workspace 任何 crate 都能调 pub fn），所以它和 `authorize_pinned(` 一样是
// architecture-check A3 的 sole-caller needle：生产代码里只允许出现在本文件与
// `crates/adapters/src/context_repo.rs`，而那里的唯一调用点先 `consume_in_txn` 再铸。
// 连同 A2（`BindingGrant {` 只许在本文件）三道闸闭合：别处即便拿到 `BindingRequest`
// 也铸不出 actor、造不出 grant、调不了 authorize。
#[derive(Debug)]
pub struct ConfirmedUserActor {
    memory_id: MemoryId,
    _priv: (),
}

impl ConfirmedUserActor {
    /// ADR-0019 D-A：只在 `op` 是 pin / unpin 时铸造，其它任何 op（包括另一个被门控的
    /// `MemorySupersede`）都是 `MissingConfirmation`——supersede 的确认不是 pin 的确认。
    ///
    /// # Errors
    /// `op` 不是 [`DestructiveOp::MemoryPin`] / [`DestructiveOp::MemoryUnpin`]。
    pub const fn from_consumed_confirmation(
        op: DestructiveOp,
        memory_id: MemoryId,
    ) -> Result<Self, CandidateRejection> {
        match op {
            DestructiveOp::MemoryPin | DestructiveOp::MemoryUnpin => Ok(Self {
                memory_id,
                _priv: (),
            }),
            DestructiveOp::MemorySupersede => Err(CandidateRejection::MissingConfirmation),
        }
    }

    /// 这次确认绑定的 memory。
    #[must_use]
    pub const fn memory_id(&self) -> MemoryId {
        self.memory_id
    }
}

/// 建 binding 的请求。
#[derive(Debug, Clone, Copy)]
pub struct BindingRequest {
    /// 档位。
    pub mode: BindingMode,
    /// scope 层级。
    pub scope_kind: ScopeKind,
    /// scope id；`Tenant` 档为 `None`。
    pub scope_id: Option<Uuid>,
    /// 要绑的 memory。
    pub memory_id: MemoryId,
}

/// 写库入口**唯一接受**的类型。
///
/// 字段私有、无 `Default`、无 pub 构造式、无 `From<BindingRequest>`——拿到它的唯一办法是
/// 走本模块的三个 `authorize_*` 之一。`adapters::context_repo::insert_binding` 只收它，
/// 所以「绕过授权直接写 binding」不是一条要靠评审拦住的路径。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindingGrant {
    mode: BindingMode,
    scope_kind: ScopeKind,
    scope_id: Option<Uuid>,
    memory_id: MemoryId,
}

impl BindingGrant {
    /// 档位。
    #[must_use]
    pub const fn mode(&self) -> BindingMode {
        self.mode
    }

    /// scope 层级。
    #[must_use]
    pub const fn scope_kind(&self) -> ScopeKind {
        self.scope_kind
    }

    /// scope id。
    #[must_use]
    pub const fn scope_id(&self) -> Option<Uuid> {
        self.scope_id
    }

    /// 被绑的 memory。
    #[must_use]
    pub const fn memory_id(&self) -> MemoryId {
        self.memory_id
    }
}

/// SUPPLEMENTAL binding 的唯一入口——也是今天**唯一能成功**的入口。
///
/// # Errors
/// `req.mode` 不是 [`BindingMode::Supplemental`] 时返回
/// [`CandidateRejection::OriginAuthorityCeiling`]：拿 supplemental 的门去建 mandatory，
/// 就是在越过 §10.1 的上限。
pub const fn authorize_supplemental(
    req: BindingRequest,
) -> Result<BindingGrant, CandidateRejection> {
    if !matches!(req.mode, BindingMode::Supplemental) {
        return Err(CandidateRejection::OriginAuthorityCeiling);
    }
    Ok(BindingGrant {
        mode: req.mode,
        scope_kind: req.scope_kind,
        scope_id: req.scope_id,
        memory_id: req.memory_id,
    })
}

/// MANDATORY binding 的唯一入口。
///
/// `memory_authority` / `memory_type` / `basis` **由 adapters 在同一事务里从库里读出**后
/// 传入，不是调用方的自由参数——否则「复核 §10.1」就退化成「复核调用方自己说的话」。
/// 唯一调用点由 architecture-check 钉住。
///
/// # Errors
/// mode 不符、policy 拒绝，或 policy 授权到的等级低于 `ProjectConstraint`。
/// 最后一条是 §25.4 的读取侧门：MANDATORY lane 只放得下 ProjectConstraint 及以上。
pub fn authorize_mandatory(
    policy: &dyn AuthorityPolicy,
    _actor: &ElevatedActor,
    req: BindingRequest,
    memory_authority: AuthorityClass,
    memory_type: MemoryType,
    basis: NonEmptyVec<EvidenceOriginClass>,
    scope: &Scope,
) -> Result<BindingGrant, CandidateRejection> {
    if !matches!(req.mode, BindingMode::Mandatory) {
        return Err(CandidateRejection::OriginAuthorityCeiling);
    }
    let authorized = policy.authorize(memory_authority, memory_type, basis, scope)?;
    if (authorized.0 as u8) < (AuthorityClass::ProjectConstraint as u8) {
        return Err(CandidateRejection::OriginAuthorityCeiling);
    }
    Ok(BindingGrant {
        mode: req.mode,
        scope_kind: req.scope_kind,
        scope_id: req.scope_id,
        memory_id: req.memory_id,
    })
}

/// PINNED binding 的唯一入口（唯一调用点由 architecture-check A3 钉在 `context_repo`）。
///
/// # Errors
/// `actor` 缺席、或 `actor` 确认的不是 `req.memory_id` 这条 memory ⇒
/// [`CandidateRejection::MissingConfirmation`]。mode 不符 ⇒ `OriginAuthorityCeiling`。
pub fn authorize_pinned(
    actor: Option<&ConfirmedUserActor>,
    req: BindingRequest,
) -> Result<BindingGrant, CandidateRejection> {
    if !matches!(req.mode, BindingMode::Pinned) {
        return Err(CandidateRejection::OriginAuthorityCeiling);
    }
    match actor {
        Some(actor) if actor.memory_id() == req.memory_id => {}
        _ => return Err(CandidateRejection::MissingConfirmation),
    }
    Ok(BindingGrant {
        mode: req.mode,
        scope_kind: req.scope_kind,
        scope_id: req.scope_id,
        memory_id: req.memory_id,
    })
}

#[cfg(test)]
mod tests {
    //! §25.4/§25.5 的判据。三条保证各自有能红的断言；纯拓扑那条（`SelectorInput` 没有
    //! query 字段）**测不出来**，因为它的失效形态是"有人加了个字段"——那由
    //! `architecture-check` 的臂看着，不由运行时断言看着。这里诚实标注，不假装测了。

    use super::*;
    use crate::authority::AuthorizedAuthority;
    use crate::grounding::{GroundingInputs, derive_grounding_state};
    use crate::ids::{TenantId, UserId, WorkspaceId};

    fn scope_of(tenant: Uuid, user: Option<Uuid>, workspace: Option<Uuid>) -> Scope {
        Scope {
            tenant_id: TenantId(tenant),
            user_id: user.map(UserId),
            workspace_id: workspace.map(WorkspaceId),
            repository_id: None,
            task_id: None,
            run_id: None,
            agent_id: None,
        }
    }

    fn current() -> RowGrounding {
        RowGrounding::Judged(derive_grounding_state(GroundingInputs::Edges(&[])))
    }

    fn row(spec: &'static SelectorSpec, tokens: u32) -> MandatoryRow {
        row_with_id(spec, MemoryId(Uuid::now_v7()), tokens)
    }

    fn row_with_id(spec: &'static SelectorSpec, memory_id: MemoryId, tokens: u32) -> MandatoryRow {
        match MandatoryRow::from_selector(spec, memory_id, spec.min_authority, tokens, current())
            .expect("min_authority 恰好等于下限，必须收下")
        {
            Admitted::Row(r) => r,
            Admitted::NeedsVerification(nv) => panic!("CURRENT 行不该被分流: {nv:?}"),
        }
    }

    fn empty_selector_outcomes() -> [SelectorOutcome; 5] {
        std::array::from_fn(|index| SelectorOutcome::Ran {
            id: REGISTRY[index].id,
            candidate_ids: vec![],
            rows: vec![],
            needs_verification: vec![],
        })
    }

    // ---- registry ----

    /// 五条 spec 的 id 必须与 `spec()` 的映射一一对应。写错一处，那个 selector 的声明
    /// （origin/authority/required_columns）会静默变成另一条的。
    #[test]
    fn spec_lookup_matches_every_registry_entry() {
        for entry in &REGISTRY {
            assert_eq!(
                spec(entry.id).id,
                entry.id,
                "spec() 把 {:?} 映射到了别的条目",
                entry.id
            );
        }
        // 反向：五条 id 互不相同（否则上面那条循环会因为覆盖而恒真）。
        let mut ids: Vec<SelectorId> = REGISTRY.iter().map(|s| s.id).collect();
        let before = ids.len();
        ids.sort_by_key(|id| format!("{id:?}"));
        ids.dedup();
        assert_eq!(before, ids.len(), "REGISTRY 里有重复的 SelectorId");
    }

    /// §25.4 要求每个 selector 声明七项。这里钉住其中三项**非空**——空的声明等于没声明。
    #[test]
    fn every_selector_declares_owner_and_both_fixtures() {
        for s in &REGISTRY {
            assert!(!s.owner.is_empty(), "{:?} 没有 owner", s.id);
            assert!(!s.positive_fixture.is_empty(), "{:?} 缺正夹具名", s.id);
            assert!(!s.negative_fixture.is_empty(), "{:?} 缺负夹具名", s.id);
            assert!(
                !s.scope_inheritance.is_empty(),
                "{:?} 没有 scope 继承链——那它按什么范围选？",
                s.id
            );
        }
    }

    // ---- scope_chain ----

    /// 由窄到宽，且 `Tenant` 恒在最后。顺序错了，scope 继承就会先命中宽的再命中窄的，
    /// 于是租户级 binding 会盖住 workspace 级的。
    #[test]
    fn scope_chain_is_narrow_to_wide_and_always_ends_at_tenant() {
        let t = Uuid::now_v7();
        let u = Uuid::now_v7();
        let w = Uuid::now_v7();

        let chain = scope_chain(&scope_of(t, Some(u), Some(w)));
        assert_eq!(
            chain,
            vec![
                (ScopeKind::Workspace, w),
                (ScopeKind::User, u),
                (ScopeKind::Tenant, t),
            ]
        );

        // 未 narrow 到的层不出现；tenant 仍在。
        let bare = scope_chain(&scope_of(t, None, None));
        assert_eq!(bare, vec![(ScopeKind::Tenant, t)]);
    }

    // ---- freshness ----

    /// §25.5 正对照：被 supersede 或被撤销之后必须从 lane 里消失。
    /// 这条不依赖真库，所以「撤销后仍在 Context」有一个纯函数形态的红。
    #[test]
    fn admits_only_active_and_not_revoked() {
        assert!(admits(
            FreshnessRule::ActiveOnly,
            AuthorityStatus::Active,
            false
        ));
        assert!(
            !admits(FreshnessRule::ActiveOnly, AuthorityStatus::Active, true),
            "已撤销的 binding 不得再进 lane"
        );
        for st in [
            AuthorityStatus::Superseded,
            AuthorityStatus::Revoked,
            AuthorityStatus::Expired,
        ] {
            assert!(
                !admits(FreshnessRule::ActiveOnly, st, false),
                "{st:?} 不是 Active，不得进 lane"
            );
        }
    }

    // ---- MandatoryRow 的铸造门 ----

    /// authority 低于 spec 下限的行**不该在这条 lane 里**——它不是"排序靠后"，是不合格。
    #[test]
    fn from_selector_rejects_authority_below_the_specs_floor() {
        let s = spec(SelectorId::ProjectActiveConstraintsV1); // 下限 ProjectConstraint
        assert!(
            MandatoryRow::from_selector(
                s,
                MemoryId(Uuid::now_v7()),
                AuthorityClass::PrivateKnowledge, // 低于下限
                10,
                current(),
            )
            .is_err(),
            "低于 min_authority 的行必须被拒"
        );
        assert!(
            MandatoryRow::from_selector(
                s,
                MemoryId(Uuid::now_v7()),
                AuthorityClass::ExplicitTaskContext, // 高于下限
                10,
                current(),
            )
            .is_ok(),
            "高于下限的行应当收下"
        );
    }

    // ---- lane 构造 ----

    /// 不可用的 selector **逐个具名**留在 lane 上（partial-lane）。
    ///
    /// 本条推翻上一版的整条拒建（任一 Unavailable ⇒ Err）：真 schema 缺 task_id/facet
    /// 两列，两个 selector 恒不可用 ⇒ 整条拒建让 lane 永远构不出来 ⇒ G80-31 永远 NA
    /// ——按「没有红转绿的闸不算存在」，Phase 8 的出场判据就不存在。§25.5 禁的是
    /// 「静默截掉仍声称 complete」，不禁 loud partial：unavailable 非空时 completeness
    /// 构造不出 complete（envelope 侧断言），handoff 如实携带。
    #[test]
    fn unavailable_selectors_are_named_on_the_lane_not_fatal() {
        let memory_id = MemoryId(Uuid::now_v7());
        let out = [
            SelectorOutcome::Unavailable {
                id: SelectorId::TaskExplicitContextV1,
                missing_object: "private.memory_records.task_id".into(),
            },
            SelectorOutcome::Ran {
                id: SelectorId::ProjectActiveConstraintsV1,
                candidate_ids: vec![memory_id],
                rows: vec![row_with_id(
                    spec(SelectorId::ProjectActiveConstraintsV1),
                    memory_id,
                    10,
                )],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::UserConfirmedCorrectionsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Unavailable {
                id: SelectorId::RequiredCurrentStateFacetsV1,
                missing_object: "private.memory_records.facet".into(),
            },
            SelectorOutcome::Ran {
                id: SelectorId::ExplicitMandatoryBindingsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
        ];
        let lane = MandatoryLane::from_selectors(out).expect("consistent selector snapshots");
        assert_eq!(
            lane.unavailable().len(),
            2,
            "两个都要点名: {:?}",
            lane.unavailable()
        );
        assert!(
            lane.unavailable()
                .iter()
                .any(|(_, m)| m.contains("task_id"))
        );
        assert!(lane.unavailable().iter().any(|(_, m)| m.contains("facet")));
        // partial 不是空转：可用 selector 的行照常在。
        assert_eq!(lane.returned(), 1);
        assert_eq!(lane.expected(), 1);
    }

    /// DOD-093：RECHECK_REQUIRED 的行铸不出 row，分流进 needs_verification 且**不许丢**。
    /// 注错：铸造门恒喂 Judged(CURRENT)（「忽略 GroundingState」唯一可写出的形态）⇒ 本条红。
    #[test]
    fn recheck_required_rows_are_diverted_and_named_not_minted() {
        let s = spec(SelectorId::ProjectActiveConstraintsV1);
        let recheck = RowGrounding::Judged(derive_grounding_state(GroundingInputs::Edges(&[
            crate::grounding::GroundingEdge {
                mode: crate::grounding::GroundingMode::Live,
                recorded_version: None,
                outcome: crate::grounding::EdgeOutcome::Resolved(
                    crate::grounding::GroundingVersionToken::new("v2"),
                ),
            },
        ])));
        let id = MemoryId(Uuid::now_v7());
        let admitted =
            MandatoryRow::from_selector(s, id, s.min_authority, 10, recheck).expect("参数合法");
        let nv = match admitted {
            Admitted::NeedsVerification(nv) => nv,
            Admitted::Row(r) => panic!("RECHECK_REQUIRED 的行铸出了 row: {r:?}"),
        };
        assert_eq!(nv.memory_id, id);
        assert_eq!(nv.state, GroundingStateKind::RecheckRequired);

        // 经 lane 聚合后仍然在场（同源同到场，不许丢）。
        let lane = MandatoryLane::from_selectors([
            SelectorOutcome::Ran {
                id: SelectorId::TaskExplicitContextV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ProjectActiveConstraintsV1,
                candidate_ids: vec![id],
                rows: vec![],
                needs_verification: vec![nv],
            },
            SelectorOutcome::Ran {
                id: SelectorId::UserConfirmedCorrectionsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::RequiredCurrentStateFacetsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ExplicitMandatoryBindingsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
        ])
        .expect("consistent selector snapshots");
        assert_eq!(lane.needs_verification().len(), 1);
        assert_eq!(lane.needs_verification()[0].memory_id, id);
    }

    /// Judged CURRENT survives admission so later Context aggregation does not invent a state.
    #[test]
    fn current_rows_retain_their_derived_grounding_state() {
        let state = row(spec(SelectorId::ProjectActiveConstraintsV1), 10)
            .grounding_state()
            .expect("CURRENT must remain available to the reader");
        assert_eq!(state.kind(), GroundingStateKind::Current);
    }

    /// NotJudged（快照内不可裁）铸进 row 但带 not_judged 标——第三臂不是 CURRENT 的别名。
    #[test]
    fn not_judged_rows_are_minted_but_flagged() {
        let s = spec(SelectorId::ProjectActiveConstraintsV1);
        let admitted = MandatoryRow::from_selector(
            s,
            MemoryId(Uuid::now_v7()),
            s.min_authority,
            10,
            RowGrounding::NotJudged,
        )
        .expect("参数合法");
        match admitted {
            Admitted::Row(r) => {
                assert!(r.not_judged(), "NotJudged 必须留痕");
                assert!(r.grounding_state().is_none());
            }
            Admitted::NeedsVerification(nv) => panic!("NotJudged 不该被分流: {nv:?}"),
        }
    }

    /// `expected` 来自各 selector 的独立授权候选并集，`returned` 是算出来的。
    /// **守恒式必须能红**：expected 若退化成 rows.len()，下面第二条断言就永远成立了。
    #[test]
    fn expected_comes_from_counts_not_from_rows_len() {
        let s = spec(SelectorId::ProjectActiveConstraintsV1);
        let candidates = [
            MemoryId(Uuid::now_v7()),
            MemoryId(Uuid::now_v7()),
            MemoryId(Uuid::now_v7()),
        ];
        let out = [
            SelectorOutcome::Ran {
                id: SelectorId::TaskExplicitContextV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            // 独立候选集合有 3 条，实际只取回 1 条（分页/LIMIT 之类）。
            SelectorOutcome::Ran {
                id: SelectorId::ProjectActiveConstraintsV1,
                candidate_ids: candidates.to_vec(),
                rows: vec![row_with_id(s, candidates[0], 10)],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::UserConfirmedCorrectionsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::RequiredCurrentStateFacetsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ExplicitMandatoryBindingsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
        ];
        let lane = MandatoryLane::from_selectors(out).expect("consistent selector snapshots");
        assert_eq!(lane.expected(), 3);
        assert_eq!(lane.returned(), 1);
        assert_eq!(lane.missing(), 2, "少带了 2 条，这个差额必须显式可见");
        assert!(mandatory_accounted(
            lane.expected(),
            lane.returned(),
            lane.missing()
        ));
    }

    #[test]
    fn selector_union_counts_and_budgets_each_memory_once_without_reordering() {
        let project = SelectorId::ProjectActiveConstraintsV1;
        let explicit = SelectorId::ExplicitMandatoryBindingsV1;
        // The explicit-only ID sorts first by UUID, but selector order must still put it last.
        let explicit_only = MemoryId(Uuid::from_u128(1));
        let project_only = MemoryId(Uuid::from_u128(2));
        let overlap = MemoryId(Uuid::from_u128(3));
        let not_judged_row = |id, memory_id| {
            let selector = spec(id);
            match MandatoryRow::from_selector(
                selector,
                memory_id,
                selector.min_authority,
                10,
                RowGrounding::NotJudged,
            )
            .expect("valid NotJudged candidate")
            {
                Admitted::Row(row) => row,
                Admitted::NeedsVerification(_) => panic!("NotJudged remains deliverable"),
            }
        };
        let mut outcomes = empty_selector_outcomes();
        outcomes[project as usize] = SelectorOutcome::Ran {
            id: project,
            candidate_ids: vec![project_only, overlap],
            rows: vec![
                not_judged_row(project, overlap),
                not_judged_row(project, project_only),
            ],
            needs_verification: vec![],
        };
        outcomes[explicit as usize] = SelectorOutcome::Ran {
            id: explicit,
            candidate_ids: vec![explicit_only, overlap],
            rows: vec![
                not_judged_row(explicit, overlap),
                not_judged_row(explicit, explicit_only),
            ],
            needs_verification: vec![],
        };
        let lane = MandatoryLane::from_selectors(outcomes).expect("same candidate facts");
        assert_eq!(
            (lane.expected(), lane.returned(), lane.missing()),
            (3, 3, 0)
        );
        assert_eq!(
            lane.rows()
                .iter()
                .map(MandatoryRow::memory_id)
                .collect::<Vec<_>>(),
            vec![project_only, overlap, explicit_only]
        );
        assert!(lane.rows().iter().all(MandatoryRow::not_judged));
        let remaining = ContextBudget::new(35, 30)
            .expect("budget")
            .reserve(&lane, &PinnedLane::new(0, vec![], vec![]))
            .expect("duplicate selector hit must not consume another 10 tokens");
        assert_eq!(remaining.tokens(), 5);
    }

    #[test]
    fn overlapping_selector_grounding_cannot_be_resolved_by_priority() {
        let project = SelectorId::ProjectActiveConstraintsV1;
        let explicit = SelectorId::ExplicitMandatoryBindingsV1;
        let id = MemoryId(Uuid::now_v7());
        for diverted in [false, true] {
            let mut outcomes = empty_selector_outcomes();
            outcomes[project as usize] = SelectorOutcome::Ran {
                id: project,
                candidate_ids: vec![id],
                rows: vec![row_with_id(spec(project), id, 10)],
                needs_verification: vec![],
            };
            let (rows, needs_verification) = if diverted {
                (
                    vec![],
                    vec![NeedsVerification {
                        memory_id: id,
                        selector: explicit,
                        state: GroundingStateKind::RecheckRequired,
                    }],
                )
            } else {
                let row = match MandatoryRow::from_selector(
                    spec(explicit),
                    id,
                    spec(explicit).min_authority,
                    10,
                    RowGrounding::NotJudged,
                )
                .expect("NotJudged row")
                {
                    Admitted::Row(row) => row,
                    Admitted::NeedsVerification(_) => panic!("NotJudged must be deliverable"),
                };
                (vec![row], vec![])
            };
            outcomes[explicit as usize] = SelectorOutcome::Ran {
                id: explicit,
                candidate_ids: vec![id],
                rows,
                needs_verification,
            };
            assert!(
                matches!(
                    MandatoryLane::from_selectors(outcomes),
                    Err(ErrorCode::Internal)
                ),
                "same-RR Current vs NotJudged/needs conflict cannot choose a favorable selector"
            );
        }
    }

    #[test]
    fn overlapping_verification_needs_are_unique_but_conflicting_states_fail_closed() {
        let project = SelectorId::ProjectActiveConstraintsV1;
        let explicit = SelectorId::ExplicitMandatoryBindingsV1;
        let id = MemoryId(Uuid::now_v7());
        for conflict in [false, true] {
            let mut outcomes = empty_selector_outcomes();
            for selector in [project, explicit] {
                outcomes[selector as usize] = SelectorOutcome::Ran {
                    id: selector,
                    candidate_ids: vec![id],
                    rows: vec![],
                    needs_verification: vec![NeedsVerification {
                        memory_id: id,
                        selector,
                        state: if conflict && selector == explicit {
                            GroundingStateKind::Unresolved
                        } else {
                            GroundingStateKind::RecheckRequired
                        },
                    }],
                };
            }
            let result = MandatoryLane::from_selectors(outcomes);
            if conflict {
                assert!(matches!(result, Err(ErrorCode::Internal)));
            } else {
                let lane = result.expect("consistent diagnostic");
                assert_eq!(
                    (lane.expected(), lane.returned(), lane.missing()),
                    (1, 0, 1)
                );
                assert_eq!(lane.needs_verification().len(), 1);
                assert_eq!(lane.needs_verification()[0].selector, project);
            }
        }
    }

    #[test]
    fn selector_identity_and_candidate_membership_are_checked_before_union() {
        let project = SelectorId::ProjectActiveConstraintsV1;
        let explicit = SelectorId::ExplicitMandatoryBindingsV1;
        let id = MemoryId(Uuid::now_v7());
        for fault in 0..3 {
            let mut outcomes = empty_selector_outcomes();
            outcomes[project as usize] = SelectorOutcome::Ran {
                id: project,
                candidate_ids: if fault == 0 { vec![] } else { vec![id] },
                rows: vec![row_with_id(
                    spec(if fault == 1 { explicit } else { project }),
                    id,
                    10,
                )],
                needs_verification: vec![],
            };
            if fault == 2 {
                outcomes[explicit as usize] = SelectorOutcome::Ran {
                    id: project,
                    candidate_ids: vec![],
                    rows: vec![],
                    needs_verification: vec![],
                };
            }
            assert!(
                matches!(
                    MandatoryLane::from_selectors(outcomes),
                    Err(ErrorCode::Internal)
                ),
                "missing candidate, wrong selector, or duplicate selector must fail closed"
            );
        }
    }

    /// 守恒式本身要能判假——否则它只是一句装饰。
    #[test]
    fn mandatory_accounted_rejects_an_unbalanced_triple() {
        assert!(mandatory_accounted(5, 3, 2));
        assert!(!mandatory_accounted(5, 3, 1), "3+1 != 5，必须判假");
        assert!(!mandatory_accounted(5, 3, 3), "3+3 != 5，必须判假");
    }

    // ---- 预算 / 溢出 ----

    /// 配置错误（上限比总额还大）在构造期就拒，不留到运行期变成溢出。
    #[test]
    fn budget_rejects_a_cap_larger_than_the_total() {
        assert!(ContextBudget::new(100, 200).is_err());
        assert!(ContextBudget::new(100, 100).is_ok());
    }

    /// 正常路径：预留之后拿到补充位预算，且额度是总额减去两条 lane 的实际占用。
    #[test]
    fn reserve_returns_the_remaining_budget() {
        let s = spec(SelectorId::ProjectActiveConstraintsV1);
        let memory_id = MemoryId(Uuid::now_v7());
        let m = MandatoryLane::from_selectors([
            SelectorOutcome::Ran {
                id: SelectorId::TaskExplicitContextV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ProjectActiveConstraintsV1,
                candidate_ids: vec![memory_id],
                rows: vec![row_with_id(s, memory_id, 30)],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::UserConfirmedCorrectionsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::RequiredCurrentStateFacetsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ExplicitMandatoryBindingsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
        ])
        .expect("consistent selector snapshots");
        let p = PinnedLane::new(1, vec![row(s, 20)], vec![]);
        let budget = ContextBudget::new(200, 100).expect("budget");
        let rest = budget.reserve(&m, &p).expect("30 + 20 <= 100，不该溢出");
        assert_eq!(rest.tokens(), 150, "200 - (30 + 20)");
    }

    /// A raw overlap still represents one physical Context item: mandatory owns it, so
    /// reservation neither double charges nor falsely overflows.
    #[test]
    fn reserve_charges_a_raw_cross_lane_overlap_once() {
        let s = spec(SelectorId::ProjectActiveConstraintsV1);
        let memory_id = MemoryId(Uuid::now_v7());
        let m = MandatoryLane::from_selectors([
            SelectorOutcome::Ran {
                id: SelectorId::TaskExplicitContextV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ProjectActiveConstraintsV1,
                candidate_ids: vec![memory_id],
                rows: vec![row_with_id(s, memory_id, 10)],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::UserConfirmedCorrectionsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::RequiredCurrentStateFacetsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ExplicitMandatoryBindingsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
        ])
        .expect("consistent selector snapshots");
        let p = PinnedLane::new(1, vec![row_with_id(s, memory_id, 10)], vec![]);

        let supplemental = ContextBudget::new(15, 10)
            .expect("budget")
            .reserve(&m, &p)
            .expect("overlapping rows must not be double charged");

        assert_eq!(supplemental.tokens(), 5);
    }

    /// Mandatory over cap returns an error with the complete manifest and no deliverable Context.
    #[test]
    fn mandatory_overflow_carries_a_full_manifest_and_no_context() {
        let s = spec(SelectorId::ProjectActiveConstraintsV1);
        let candidates = [MemoryId(Uuid::now_v7()), MemoryId(Uuid::now_v7())];
        let m = MandatoryLane::from_selectors([
            SelectorOutcome::Ran {
                id: SelectorId::TaskExplicitContextV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ProjectActiveConstraintsV1,
                candidate_ids: candidates.to_vec(),
                rows: vec![
                    row_with_id(s, candidates[0], 80),
                    row_with_id(s, candidates[1], 80),
                ],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::UserConfirmedCorrectionsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::RequiredCurrentStateFacetsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ExplicitMandatoryBindingsV1,
                candidate_ids: vec![],
                rows: vec![],
                needs_verification: vec![],
            },
        ])
        .expect("consistent selector snapshots");
        let p = PinnedLane::new(1, vec![row(s, 10)], vec![]);
        let budget = ContextBudget::new(500, 100).expect("budget");

        let overflow = budget
            .reserve(&m, &p)
            .expect_err("80 + 80 + 10 > 100，必须溢出而不是截断");
        assert_eq!(overflow.expected(), 2);
        assert_eq!(
            overflow.manifest().len(),
            3,
            "manifest 必须全量（两条 mandatory + 一条 pinned），否则提不出分页建议"
        );
        assert_eq!(overflow.budget_tokens(), 100);
        assert_eq!(overflow.required_tokens(), 170);
        assert!(
            overflow.required_tokens() > overflow.budget_tokens(),
            "缩减建议的量纲必须为正"
        );
    }

    // ---- binding 授权 ----

    struct AlwaysAuthorizes(AuthorityClass);
    impl AuthorityPolicy for AlwaysAuthorizes {
        fn authorize(
            &self,
            _requested: AuthorityClass,
            _memory_type: MemoryType,
            _basis: NonEmptyVec<EvidenceOriginClass>,
            _scope: &Scope,
        ) -> Result<AuthorizedAuthority, CandidateRejection> {
            Ok(AuthorizedAuthority(self.0))
        }
    }

    fn req(mode: BindingMode) -> BindingRequest {
        BindingRequest {
            mode,
            scope_kind: ScopeKind::Tenant,
            scope_id: None,
            memory_id: MemoryId(Uuid::now_v7()),
        }
    }

    /// 拿 supplemental 的门去建 mandatory ⇒ 拒。三个 `authorize_*` 各管各的档，
    /// 不许互相顶替——否则最松的那个门就成了所有档的入口。
    #[test]
    fn each_authorize_entry_only_accepts_its_own_mode() {
        assert!(authorize_supplemental(req(BindingMode::Supplemental)).is_ok());
        assert!(authorize_supplemental(req(BindingMode::Mandatory)).is_err());
        assert!(authorize_supplemental(req(BindingMode::Pinned)).is_err());

        assert_eq!(
            authorize_pinned(None, req(BindingMode::Supplemental)),
            Err(CandidateRejection::OriginAuthorityCeiling)
        );
    }

    /// PINNED 缺交互确认 ⇒ `MissingConfirmation`。这给了那个此前结构上不可达的变体
    /// 第一个可达的生产者。
    #[test]
    fn pinned_without_confirmation_is_missing_confirmation() {
        assert_eq!(
            authorize_pinned(None, req(BindingMode::Pinned)),
            Err(CandidateRejection::MissingConfirmation)
        );
    }

    /// ADR-0019 D-A：actor 只从 pin/unpin 的已消费确认铸造，且只对它确认的那条 memory 有效。
    /// 注错：去掉 `from_consumed_confirmation` 的 op 判定或 `authorize_pinned` 的 memory 比对
    /// ⇒ 本条红。
    #[test]
    fn confirmed_actor_binds_one_memory_and_only_pin_ops_mint_it() {
        let request = req(BindingMode::Pinned);
        assert!(
            ConfirmedUserActor::from_consumed_confirmation(
                DestructiveOp::MemorySupersede,
                request.memory_id
            )
            .is_err(),
            "a supersede confirmation is not a pin confirmation"
        );
        let actor = ConfirmedUserActor::from_consumed_confirmation(
            DestructiveOp::MemoryPin,
            request.memory_id,
        )
        .expect("pin confirmation mints the actor");
        let grant = authorize_pinned(Some(&actor), request).expect("grant");
        assert_eq!(grant.mode(), BindingMode::Pinned);
        assert_eq!(grant.memory_id(), request.memory_id);

        let other = req(BindingMode::Pinned);
        assert_eq!(
            authorize_pinned(Some(&actor), other),
            Err(CandidateRejection::MissingConfirmation),
            "a confirmation for X never grants a binding on Y"
        );
        assert!(
            ConfirmedUserActor::from_consumed_confirmation(
                DestructiveOp::MemoryUnpin,
                other.memory_id
            )
            .is_ok()
        );
    }

    /// MANDATORY 的读取侧门：policy 即使放行，授权到的等级低于 ProjectConstraint 也不许进。
    /// 注错：把那条下限判定删掉 ⇒ 本条红。
    #[test]
    fn mandatory_requires_project_constraint_or_above_even_if_policy_allows() {
        let actor = ElevatedActor { _priv: () };
        let scope = scope_of(Uuid::now_v7(), None, None);
        let basis = NonEmptyVec::new(vec![EvidenceOriginClass::DirectUserInput]).expect("basis");

        let lenient = AlwaysAuthorizes(AuthorityClass::PrivateKnowledge);
        assert_eq!(
            authorize_mandatory(
                &lenient,
                &actor,
                req(BindingMode::Mandatory),
                AuthorityClass::PrivateKnowledge,
                MemoryType::Note,
                basis.clone(),
                &scope,
            ),
            Err(CandidateRejection::OriginAuthorityCeiling),
            "policy 放行不等于够格进 MANDATORY lane"
        );

        let strict = AlwaysAuthorizes(AuthorityClass::ProjectConstraint);
        assert!(
            authorize_mandatory(
                &strict,
                &actor,
                req(BindingMode::Mandatory),
                AuthorityClass::ProjectConstraint,
                MemoryType::Note,
                basis,
                &scope,
            )
            .is_ok()
        );
    }
}
