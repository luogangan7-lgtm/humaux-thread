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
//! 写侧的诚实答案见 [`ElevatedActor`]：MANDATORY/PINNED 的 binding 今天**没有铸造路径**。

use crate::authority::{
    AuthorityClass, AuthorityPolicy, AuthorityStatus, CandidateRejection, MemoryId, NonEmptyVec,
};
use crate::error::ErrorCode;
use crate::evidence::EvidenceOriginClass;
use crate::ids::Scope;
use crate::memory::MemoryType;
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
}

impl MandatoryRow {
    /// 唯一铸造点。按 spec 复核 authority 下限——低于下限的行不是「排序靠后」，是**不该在
    /// 这条 lane 里**。
    ///
    /// # Errors
    /// `authority` 低于 `spec.min_authority` 时返回 [`ErrorCode::InvalidInput`]。
    pub fn from_selector(
        spec: &'static SelectorSpec,
        memory_id: MemoryId,
        authority: AuthorityClass,
        est_tokens: u32,
    ) -> Result<Self, ErrorCode> {
        if (authority as u8) < (spec.min_authority as u8) {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            memory_id,
            selector: spec.id,
            authority,
            est_tokens,
        })
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
    /// 跑到了。`expected` 必须来自与 `rows` **分离的一次 COUNT**（不带 LIMIT），
    /// 不是 `rows.len()`——内生的 expected 会让 §25.5 的守恒式退化成恒真算术。
    Ran {
        /// 哪个 selector。
        id: SelectorId,
        /// 独立 COUNT 得到的应有条数。
        expected: u64,
        /// 实际取回的行。
        rows: Vec<MandatoryRow>,
    },
    /// 被测对象缺席。
    Unavailable {
        /// 哪个 selector。
        id: SelectorId,
        /// **探测出来的**缺失对象名（如 `private.memory_records.task_id`）。
        missing_object: String,
    },
}

/// 至少一个 selector 不可用 ⇒ 整条 lane 构不出来。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneUnavailable {
    /// 逐个点名，不合并成一句——调用方要能照着这个列表去建缺的东西。
    pub missing: Vec<(SelectorId, String)>,
}

/// §25.4 步骤 2 的产物。字段私有：`expected` 不许被调用方改写。
#[derive(Debug)]
pub struct MandatoryLane {
    expected: u64,
    rows: Vec<MandatoryRow>,
}

impl MandatoryLane {
    /// 唯一构造点，收**定长数组**而不是 `Vec`。
    ///
    /// 定长是刻意的：调用方少传一个 selector 会编译不过，而不是让 `expected` 悄悄少算一截。
    ///
    /// # Errors
    /// 任一 selector `Unavailable` ⇒ [`LaneUnavailable`]，整条 lane 构不出来。
    /// 「有几个 selector 坏了但先凑合上」不是一个可表达的状态。
    pub fn from_selectors(out: [SelectorOutcome; 5]) -> Result<Self, LaneUnavailable> {
        let mut missing = Vec::new();
        let mut expected = 0u64;
        let mut rows = Vec::new();
        for o in out {
            match o {
                SelectorOutcome::Unavailable { id, missing_object } => {
                    missing.push((id, missing_object));
                }
                SelectorOutcome::Ran {
                    expected: e,
                    rows: mut r,
                    ..
                } => {
                    expected = expected.saturating_add(e);
                    rows.append(&mut r);
                }
            }
        }
        if missing.is_empty() {
            Ok(Self { expected, rows })
        } else {
            Err(LaneUnavailable { missing })
        }
    }

    /// 应有条数（各 selector 独立 COUNT 之和）。
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

/// §25.4 步骤 5 的产物。形状同 [`MandatoryLane`]，但**没有 `missing`**：
/// Pinned 是用户/管理员显式钉的，钉了几条就是几条，没有「应有但没取到」这个概念。
#[derive(Debug)]
pub struct PinnedLane {
    rows: Vec<MandatoryRow>,
}

impl PinnedLane {
    /// 唯一构造点。
    #[must_use]
    pub const fn new(rows: Vec<MandatoryRow>) -> Self {
        Self { rows }
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
        let required = m.total_tokens().saturating_add(p.total_tokens());
        if required > self.mandatory_cap_tokens {
            return Err(MandatoryOverflow {
                expected: m.expected(),
                manifest: m
                    .rows()
                    .iter()
                    .chain(p.rows().iter())
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

/// 建 PINNED binding 所需的、经过交互确认的用户 actor。
///
/// 同样**今天没有铸造路径**：`confirm_token` / MRTR 交互确认全仓零命中
/// （`crates/protocol/src/mcp.rs` 仍是占位模块）。
#[derive(Debug)]
pub struct ConfirmedUserActor {
    _priv: (),
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

/// PINNED binding 的唯一入口。
///
/// # Errors
/// `actor` 缺席 ⇒ [`CandidateRejection::MissingConfirmation`]。这给了那个变体第一个
/// **可达**的生产者——它此前在仓里结构上不可达。mode 不符 ⇒ `OriginAuthorityCeiling`。
pub const fn authorize_pinned(
    actor: Option<&ConfirmedUserActor>,
    req: BindingRequest,
) -> Result<BindingGrant, CandidateRejection> {
    if !matches!(req.mode, BindingMode::Pinned) {
        return Err(CandidateRejection::OriginAuthorityCeiling);
    }
    if actor.is_none() {
        return Err(CandidateRejection::MissingConfirmation);
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

    fn row(spec: &'static SelectorSpec, tokens: u32) -> MandatoryRow {
        MandatoryRow::from_selector(spec, MemoryId(Uuid::now_v7()), spec.min_authority, tokens)
            .expect("min_authority 恰好等于下限，必须收下")
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
            )
            .is_ok(),
            "高于下限的行应当收下"
        );
    }

    // ---- lane 构造 ----

    /// 任一 selector 不可用 ⇒ 整条 lane 构不出来，且**逐个点名**。
    /// 「有几个坏了但先凑合上」不是一个可表达的状态——那正是 Context 静默少带东西的形态。
    #[test]
    fn any_unavailable_selector_kills_the_whole_lane_and_names_each_one() {
        let out = [
            SelectorOutcome::Unavailable {
                id: SelectorId::TaskExplicitContextV1,
                missing_object: "private.memory_records.task_id".into(),
            },
            SelectorOutcome::Ran {
                id: SelectorId::ProjectActiveConstraintsV1,
                expected: 1,
                rows: vec![row(spec(SelectorId::ProjectActiveConstraintsV1), 10)],
            },
            SelectorOutcome::Ran {
                id: SelectorId::UserConfirmedCorrectionsV1,
                expected: 0,
                rows: vec![],
            },
            SelectorOutcome::Unavailable {
                id: SelectorId::RequiredCurrentStateFacetsV1,
                missing_object: "private.memory_records.facet".into(),
            },
            SelectorOutcome::Ran {
                id: SelectorId::ExplicitMandatoryBindingsV1,
                expected: 0,
                rows: vec![],
            },
        ];
        match MandatoryLane::from_selectors(out) {
            Err(LaneUnavailable { missing }) => {
                assert_eq!(missing.len(), 2, "两个都要点名: {missing:?}");
                assert!(missing.iter().any(|(_, m)| m.contains("task_id")));
                assert!(missing.iter().any(|(_, m)| m.contains("facet")));
            }
            Ok(_) => panic!("有 selector 不可用时 lane 不得构造成功"),
        }
    }

    /// `expected` 来自各 selector 的独立 COUNT，`returned` 是算出来的。
    /// **守恒式必须能红**：expected 若退化成 rows.len()，下面第二条断言就永远成立了。
    #[test]
    fn expected_comes_from_counts_not_from_rows_len() {
        let s = spec(SelectorId::ProjectActiveConstraintsV1);
        let out = [
            SelectorOutcome::Ran {
                id: SelectorId::TaskExplicitContextV1,
                expected: 0,
                rows: vec![],
            },
            // COUNT 说有 3 条，实际只取回 1 条（分页/LIMIT 之类）。
            SelectorOutcome::Ran {
                id: SelectorId::ProjectActiveConstraintsV1,
                expected: 3,
                rows: vec![row(s, 10)],
            },
            SelectorOutcome::Ran {
                id: SelectorId::UserConfirmedCorrectionsV1,
                expected: 0,
                rows: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::RequiredCurrentStateFacetsV1,
                expected: 0,
                rows: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ExplicitMandatoryBindingsV1,
                expected: 0,
                rows: vec![],
            },
        ];
        let lane = MandatoryLane::from_selectors(out).expect("全部 Ran，应当构造成功");
        assert_eq!(lane.expected(), 3);
        assert_eq!(lane.returned(), 1);
        assert_eq!(lane.missing(), 2, "少带了 2 条，这个差额必须显式可见");
        assert!(mandatory_accounted(
            lane.expected(),
            lane.returned(),
            lane.missing()
        ));
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
        let m = MandatoryLane::from_selectors([
            SelectorOutcome::Ran {
                id: SelectorId::TaskExplicitContextV1,
                expected: 0,
                rows: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ProjectActiveConstraintsV1,
                expected: 1,
                rows: vec![row(s, 30)],
            },
            SelectorOutcome::Ran {
                id: SelectorId::UserConfirmedCorrectionsV1,
                expected: 0,
                rows: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::RequiredCurrentStateFacetsV1,
                expected: 0,
                rows: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ExplicitMandatoryBindingsV1,
                expected: 0,
                rows: vec![],
            },
        ])
        .expect("lane");
        let p = PinnedLane::new(vec![row(s, 20)]);
        let budget = ContextBudget::new(200, 100).expect("budget");
        let rest = budget.reserve(&m, &p).expect("30 + 20 <= 100，不该溢出");
        assert_eq!(rest.tokens(), 150, "200 - (30 + 20)");
    }

    /// §25.5 的核心：Mandatory 超上限 ⇒ `Err`，**且那个 Err 里没有可交付的 Context**。
    ///
    /// 「截掉后半段还声称 complete」在这里不是被禁止的操作——`MandatoryOverflow` 根本没有
    /// 装 rows 的地方。本条能断言的是它带齐了 §25.5 要求的 manifest 与量纲；
    /// 「没有 rows 可返回」由类型定义保证，不由本条保证。
    #[test]
    fn mandatory_overflow_carries_a_full_manifest_and_no_context() {
        let s = spec(SelectorId::ProjectActiveConstraintsV1);
        let m = MandatoryLane::from_selectors([
            SelectorOutcome::Ran {
                id: SelectorId::TaskExplicitContextV1,
                expected: 0,
                rows: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ProjectActiveConstraintsV1,
                expected: 2,
                rows: vec![row(s, 80), row(s, 80)],
            },
            SelectorOutcome::Ran {
                id: SelectorId::UserConfirmedCorrectionsV1,
                expected: 0,
                rows: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::RequiredCurrentStateFacetsV1,
                expected: 0,
                rows: vec![],
            },
            SelectorOutcome::Ran {
                id: SelectorId::ExplicitMandatoryBindingsV1,
                expected: 0,
                rows: vec![],
            },
        ])
        .expect("lane");
        let p = PinnedLane::new(vec![row(s, 10)]);
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
