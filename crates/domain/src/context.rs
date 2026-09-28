//! `domain::context` — §25.4 Mandatory Context Lane + §25.5 Budget/Overflow。
//! Depends-on: crates=[sha2, uuid]; services=[PostgreSQL(any) r=[private.memory_records]]; env=[]; modules=[domain::authority, domain::confirm, domain::error, domain::evidence, domain::grounding, domain::ids, domain::memory]
//! Called-by: [adapters::context_repo, adapters::continuity_read, application::continuity, application::pin, gateway::bootstrap, gateway::context, gateway::continuity, gateway::mcp_application, gateway::memory, retrieval::compiler, retrieval::completeness, retrieval::envelope, retrieval::handoff, tests]
//! Invariants: [zero IO: Mandatory selection has no query/embedding input, Mandatory items are never evicted by
//!   rerank, and an overflow is cannot_establish, never a silent truncation]
//! Spec: Baseline §1.2.1; §8.8; §10.1; ADR-0006; ADR-0019; ADR-0045
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
//! 写侧：MANDATORY 见 [`ElevatedActor`]——铸造路径是 `memory.bind` / `memory.unbind` 消费
//! confirm_token 之后（card 22b, ADR-0045）。PINNED 见
//! [`ConfirmedUserActor`]——唯一铸造点是 §33.10 规则 9 的 confirm_token 被消费之后（ADR-0019）。

use crate::authority::{
    AuthorityClass, AuthorityPolicy, AuthorityStatus, CandidateRejection, MemoryId, NonEmptyVec,
};
use crate::confirm::DestructiveOp;
use crate::error::ErrorCode;
use crate::evidence::{EvidenceOriginClass, InstructionDisposition};
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    /// §25.4 登记名——**版本在名字里**。裁决 §三：「同名 selector 不得偷换准入对象」，所以
    /// 把 `task_explicit_context_v1` 的准入对象从「行上的 authority_class」换成「验过的任务
    /// 授权」不能沿用旧名字，它登记为 `task_explicit_context_v2`，v1 的记录留在
    /// [`RETIRED_SELECTORS`] 里供审计（[`SelectorId`] 的变体名是**槽位**标识，不是登记名——
    /// 它被 handoff / envelope / 测试按变体名引用，改它只会制造一次无信息量的大改）。
    pub registered_name: &'static str,
    /// scope 继承链，由窄到宽。
    pub scope_inheritance: &'static [ScopeKind],
    /// 所选 memory 的 evidence 必须至少命中其一的 origin（空 = 不约束 origin）。
    pub required_origin: &'static [EvidenceOriginClass],
    /// §25.4 v2（card 22c, ADR-0046）：这个 selector 对候选**权威**的真实要求。
    ///
    /// 四个 selector 是 [`AuthorityRequirement::StoredAtLeast`]——存储行自己的
    /// `authority_class` 就是判据。`task_explicit_context_v1` 是
    /// [`AuthorityRequirement::VerifiedCurrentTaskBinding`]：判据不在行上，而在
    /// [`authorize_task_item`] 验过的一条任务授权上（I-TASK）。这两件事**不是同一种门**，
    /// 所以它们不能共用一个 `AuthorityClass` 下限字段假装是同一种门。
    pub authority: AuthorityRequirement,
    /// 所选 memory 的**存储** authority 下限。
    ///
    /// 恒等于 `authority.stored_floor()`——它是派生值，留成字段只因为调用方按字段读它。
    /// 漂移由 `registry_min_authority_is_derived_from_requirement` 当场判红，不靠人守。
    pub min_authority: AuthorityClass,
    /// freshness 规则。
    pub freshness: FreshnessRule,
    /// owner，写模块路径不写人名（人会走，模块不会）。
    pub owner: &'static str,
    /// §25.4 第七项：正夹具的测试函数名。由 xtask 臂断言这个名字真的存在。
    pub positive_fixture: &'static str,
    /// 负夹具的测试函数名。
    pub negative_fixture: &'static str,
    /// 这个 selector 需要哪些列才跑得起来：`(schema, table, column, stored_generated)`。
    ///
    /// domain 只声明名字，**不发查询**；`adapters::context_repo::probe_selectors` 拿它去比
    /// `pg_attribute`。列一落地 selector 自动可用，不需要有人回来改代码——
    /// 这是 ADR-0006 那条「NA 的缺失对象必须是探测出来的」在本模块的落点。
    ///
    /// 第四项 `stored_generated`（card 22b / §25.4.A(11)）：`true` 要求该列必须是
    /// `GENERATED ALWAYS ... STORED`（`pg_attribute.attgenerated = 's'`）。存在性不是契约
    /// ——一个同名的**可写**列意味着 facet 有了独立写入口，那正是 §25.4.A(3) 禁止的东西，
    /// 所以它必须探测成缺失而不是被当作可用。
    pub required_columns: &'static [(&'static str, &'static str, &'static str, bool)],
}

/// §25.4 的五个 selector。**唯一真源**——`spec()` 之外没有第二处描述它们。
pub const REGISTRY: [SelectorSpec; 5] = [
    SelectorSpec {
        id: SelectorId::TaskExplicitContextV1,
        registered_name: "task_explicit_context_v2",
        scope_inheritance: &[ScopeKind::Task],
        required_origin: &[],
        authority: AuthorityRequirement::VerifiedCurrentTaskBinding,
        min_authority: AuthorityClass::PrivateKnowledge,
        freshness: FreshnessRule::ActiveOnly,
        owner: "domain::context",
        positive_fixture: "task_explicit_context_v2_positive",
        negative_fixture: "task_explicit_context_v2_negative",
        // §25.4.A(7)/(11)（card 22b, ADR-0045）：task 关联**唯一**来自有效的 ContextBinding
        // （`scope_kind='TASK'` / `mode='MANDATORY'` / `revoked_at IS NULL`），不来自
        // `memory_records.task_id`——那一列不存在，也**不会**被加上：provenance（这条输入是在
        // 任务 T 里摄入的）不等于 authority（这条记忆被授权为 T 的必带上下文）。探测因此登记
        // 真实关系上的真实列；§25.4.A(11) 明文禁止继续探测 `memory_records.task_id`。
        required_columns: &[
            ("private", "context_bindings", "scope_kind", false),
            ("private", "context_bindings", "scope_id", false),
            ("private", "context_bindings", "mode", false),
            ("private", "context_bindings", "revoked_at", false),
            // card 22c / ADR-0046：v2 的准入对象是**授权行**，所以探测必须落在授权表上。
            // 探不到 `private.task_binding_grants` 时这个 selector 是 Unavailable —— 不是
            // 「跑了但零行」：后者会把「授权机制根本没交付」伪装成「今天没有需要带的东西」。
            ("private", "task_binding_grants", "task_epoch", false),
            ("private", "task_binding_grants", "payload_sha256", false),
            ("private", "task_binding_grants", "purpose", false),
            ("private", "task_binding_grants", "revoked_at", false),
        ],
    },
    SelectorSpec {
        id: SelectorId::ProjectActiveConstraintsV1,
        registered_name: "project_active_constraints_v1",
        scope_inheritance: &[ScopeKind::Workspace, ScopeKind::Tenant],
        required_origin: &[],
        authority: AuthorityRequirement::StoredAtLeast(AuthorityClass::ProjectConstraint),
        min_authority: AuthorityClass::ProjectConstraint,
        freshness: FreshnessRule::ActiveOnly,
        owner: "domain::context",
        positive_fixture: "project_active_constraints_positive",
        negative_fixture: "project_active_constraints_negative",
        required_columns: &[
            ("private", "memory_records", "authority_class", false),
            ("private", "memory_records", "status", false),
        ],
    },
    SelectorSpec {
        id: SelectorId::UserConfirmedCorrectionsV1,
        registered_name: "user_confirmed_corrections_v1",
        scope_inheritance: &[ScopeKind::User, ScopeKind::Tenant],
        // §25.4「active UserCorrection relevant to scope 不允许用 embedding similarity
        // 解释」——机械替代品就是这一条 origin 约束。
        required_origin: &[EvidenceOriginClass::UserConfirmed],
        authority: AuthorityRequirement::StoredAtLeast(AuthorityClass::UserCorrection),
        min_authority: AuthorityClass::UserCorrection,
        freshness: FreshnessRule::ActiveOnly,
        owner: "domain::context",
        positive_fixture: "user_confirmed_corrections_positive",
        negative_fixture: "user_confirmed_corrections_negative",
        required_columns: &[("private", "evidence_objects", "origin_class", false)],
    },
    SelectorSpec {
        id: SelectorId::RequiredCurrentStateFacetsV1,
        registered_name: "required_current_state_facets_v1",
        scope_inheritance: &[ScopeKind::Workspace, ScopeKind::Tenant],
        required_origin: &[],
        authority: AuthorityRequirement::StoredAtLeast(AuthorityClass::PrivateKnowledge),
        min_authority: AuthorityClass::PrivateKnowledge,
        freshness: FreshnessRule::ActiveOnly,
        owner: "domain::context",
        positive_fixture: "required_current_state_facets_positive",
        negative_fixture: "required_current_state_facets_negative",
        // §25.4.A（card 22b, ADR-0045）：对齐条款已写下——facet 是
        // [`crate::memory::MandatoryContextFacet`] 的数据库投影，由 `memory_type` 唯一派生，
        // 迁移 0172 落为 `GENERATED ALWAYS ... STORED`。第四项 `true` 就是「必须是生成列」
        // 这条契约：可写的同名列 = facet 有了独立写入口 = §25.4.A(3) 被破坏。
        required_columns: &[("private", "memory_records", "facet", true)],
    },
    SelectorSpec {
        id: SelectorId::ExplicitMandatoryBindingsV1,
        registered_name: "explicit_mandatory_bindings_v1",
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
        authority: AuthorityRequirement::StoredAtLeast(AuthorityClass::ProjectConstraint),
        min_authority: AuthorityClass::ProjectConstraint,
        freshness: FreshnessRule::ActiveOnly,
        owner: "domain::context",
        positive_fixture: "explicit_mandatory_bindings_positive",
        negative_fixture: "explicit_mandatory_bindings_negative",
        required_columns: &[
            ("private", "context_bindings", "mode", false),
            ("private", "context_bindings", "revoked_at", false),
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
    /// card 22c / ADR-0046：这条记忆**存储**的权威。
    ///
    /// 四个 `StoredAtLeast` selector 上它恒等于 `authority`；`task_explicit_context_v2` 上
    /// `authority` 是被授权的使用权威（6），`source_authority` 是原样的存储权威（≤ 5）。
    /// 两个字段并存是 I-STORE 的可观察面——只留一个就等于把「谁批准的」洗成内容属性。
    source_authority: AuthorityClass,
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
        // card 22c: 这条入口只服务 `StoredAtLeast`。`VerifiedCurrentTaskBinding` 的准入对象
        // 不是行上的 authority，走 [`Self::from_task_grant`]——否则「按存储权威准入」会从这里
        // 重新长出来，而那正是 v1 的病。
        if matches!(
            spec.authority,
            AuthorityRequirement::VerifiedCurrentTaskBinding
        ) {
            return Err(ErrorCode::InvalidInput);
        }
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
            source_authority: authority,
            est_tokens,
            grounding_state,
        }))
    }

    /// card 22c / ADR-0046：[`AuthorityRequirement::VerifiedCurrentTaskBinding`] 的唯一铸造点。
    ///
    /// 它要一个 [`AuthorizedTaskItem`]——而那个类型只能由 [`authorize_task_item`] 造出来。
    /// 所以「没有验过授权就把一行塞进 task lane」不是一条要靠评审拦住的路径，是一个**造不出
    /// 参数**的调用。grounding 分流门与 [`Self::from_selector`] 逐字相同（DOD-093 不因为有
    /// 授权就放松：批准的是「可以当指令用」，不是「不必再判它是否仍然成立」）。
    ///
    /// # Errors
    /// `spec` 不是 `VerifiedCurrentTaskBinding` 时返回 [`ErrorCode::InvalidInput`]。
    pub fn from_task_grant(
        spec: &'static SelectorSpec,
        item: AuthorizedTaskItem,
        est_tokens: u32,
        grounding: RowGrounding,
    ) -> Result<Admitted, ErrorCode> {
        if !matches!(
            spec.authority,
            AuthorityRequirement::VerifiedCurrentTaskBinding
        ) {
            return Err(ErrorCode::InvalidInput);
        }
        let grounding_state = match grounding {
            RowGrounding::Judged(state) => {
                if state.revokes_current_truth_assumption() {
                    return Ok(Admitted::NeedsVerification(NeedsVerification {
                        memory_id: item.memory_id(),
                        selector: spec.id,
                        state: state.kind(),
                    }));
                }
                Some(state)
            }
            RowGrounding::NotJudged => None,
        };
        Ok(Admitted::Row(Self {
            memory_id: item.memory_id(),
            selector: spec.id,
            authority: item.effective_context_authority(),
            source_authority: item.source_authority(),
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

    /// 该行本次**使用**的 authority（task lane 上就是被授权的 `ExplicitTaskContext`）。
    #[must_use]
    pub const fn authority(&self) -> AuthorityClass {
        self.authority
    }

    /// 该行**存储**的 authority（I-STORE：≤ 5，授权不改它）。
    #[must_use]
    pub const fn source_authority(&self) -> AuthorityClass {
        self.source_authority
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
/// **唯一铸造点是 [`ElevatedActor::from_consumed_confirmation`]**（card 22b, ADR-0045），
/// 形状与 [`ConfirmedUserActor`] 逐字对称：只认 [`DestructiveOp::MemoryBind`] /
/// [`DestructiveOp::MemoryUnbind`]，且调用方必须**刚刚在同一事务里消费了**那条 memory 的
/// confirm_token。字段私有、无 `Default`、无字面量构造，所以 consolidation / distiller /
/// retention 代码即便拿到 [`BindingRequest`] 也造不出它。
///
/// 此前这里写的是「今天没有任何铸造函数」。那在 card 22b 之前是诚实的，但也意味着
/// `task_explicit_context_v1` 的真源（TASK/MANDATORY binding）**永远建不出来**——
/// 裁决 §三.2 把这一条点名了：「表结构表达关系，受控写入建立关系；单有表不证明关系已经被
/// 合法建立」。所以铸造点必须存在，而不是继续用「没有门」冒充「门够严」。
///
/// 它不是权威证明（§25.4.A(8)）：拿到 actor 只解锁「可以请求」，
/// [`authorize_mandatory`] 仍然要在同一事务里用库里读出的 authority/type/basis 过
/// [`AuthorityPolicy`]，并复核 `ProjectConstraint` 下限。§25.4 那条攻击路径（诱导 Agent
/// 调一次写就把低 origin 内容永久钉进 Context）因此仍不成立——现在是因为门够严。
#[derive(Debug)]
pub struct ElevatedActor {
    memory_id: MemoryId,
    _priv: (),
}

impl ElevatedActor {
    /// ADR-0045 D-A：只在 `op` 是 bind / unbind 时铸造。别的 op（包括同样被门控的
    /// [`DestructiveOp::MemoryPin`]）都是 `MissingConfirmation`——pin 的确认不是 bind 的确认。
    ///
    /// # Errors
    /// `op` 不是 [`DestructiveOp::MemoryBind`] / [`DestructiveOp::MemoryUnbind`]。
    pub const fn from_consumed_confirmation(
        op: DestructiveOp,
        memory_id: MemoryId,
    ) -> Result<Self, CandidateRejection> {
        match op {
            DestructiveOp::MemoryBind | DestructiveOp::MemoryUnbind => Ok(Self {
                memory_id,
                _priv: (),
            }),
            DestructiveOp::MemorySupersede
            | DestructiveOp::MemoryPin
            | DestructiveOp::MemoryUnpin
            | DestructiveOp::MemoryRestore
            | DestructiveOp::MemoryArchive
            | DestructiveOp::MemoryUnarchive
            | DestructiveOp::MemoryCorrect
            | DestructiveOp::MemoryConfirm
            | DestructiveOp::MemoryReject => Err(CandidateRejection::MissingConfirmation),
        }
    }

    /// 这次确认绑定的 memory。
    #[must_use]
    pub const fn memory_id(&self) -> MemoryId {
        self.memory_id
    }
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
            DestructiveOp::MemorySupersede
            | DestructiveOp::MemoryRestore
            | DestructiveOp::MemoryArchive
            | DestructiveOp::MemoryUnarchive
            | DestructiveOp::MemoryCorrect
            | DestructiveOp::MemoryConfirm
            | DestructiveOp::MemoryReject
            // card 22b: a bind confirmation is not a pin confirmation, in this direction too.
            | DestructiveOp::MemoryBind
            | DestructiveOp::MemoryUnbind => Err(CandidateRejection::MissingConfirmation),
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
    actor: &ElevatedActor,
    req: BindingRequest,
    memory_authority: AuthorityClass,
    memory_type: MemoryType,
    basis: NonEmptyVec<EvidenceOriginClass>,
    scope: &Scope,
) -> Result<BindingGrant, CandidateRejection> {
    if !matches!(req.mode, BindingMode::Mandatory) {
        return Err(CandidateRejection::OriginAuthorityCeiling);
    }
    // card 22b: the actor was minted against ONE memory's consumed confirmation. Taking X's
    // confirmation to bind Y is `MissingConfirmation`, the same rule `authorize_pinned` runs.
    if actor.memory_id() != req.memory_id {
        return Err(CandidateRejection::MissingConfirmation);
    }
    let authorized = policy.authorize(memory_authority, memory_type, basis, scope)?;
    // card 22c / ADR-0046 D-J: the extra floor here used to be `ProjectConstraint` — card 22b's
    // stand-in for "this memory deserves to be in Mandatory", written when a TASK/MANDATORY
    // binding was **itself** the admission. It no longer is: v2 admits on a verified task
    // authorization, and the ruling (§二.4) rejects a 5-floor by name because it excludes the
    // legitimate `UserConfirmed(4)` target the positive control uses. The floor is therefore the
    // v2 stored floor — `PrivateKnowledge`, i.e. "not public-pool content".
    //
    // What did NOT move, and is why this is not a hole: `explicit_mandatory_bindings_v1`'s own
    // SQL still admits only `ProjectConstraint`, so a lower-authority TASK binding cannot enter
    // Mandatory through that selector; and `task_explicit_context_v2` admits it only with a
    // grant (confirm-token-gated, epoch-bound, content-hash-bound, revocable). The §25.4 attack
    // path — one induced write pinning low-origin content into Context forever — is closed by
    // the authorization, not by a number.
    if (authorized.0 as u8)
        < (AuthorityRequirement::VerifiedCurrentTaskBinding.stored_floor() as u8)
    {
        return Err(CandidateRejection::OriginAuthorityCeiling);
    }
    Ok(BindingGrant {
        mode: req.mode,
        scope_kind: req.scope_kind,
        scope_id: req.scope_id,
        memory_id: req.memory_id,
    })
}

/// 裁决 §四.4 写路径的 `require_behavior_eligible_target`：**铸任务授权之前**，目标的
/// origin basis 必须允许行为资格（§10.1 row 4/5）。
///
/// 读法与 `AuthorityPolicy::authorize` 的 ceiling 一致：basis 里**有任一** origin 允许
/// `BehaviorEligible` 才算允许；origin → disposition 的表只有
/// [`EvidenceOriginClass::max_disposition`] 一处。
///
/// 为什么写路径要再查一次，而不是只靠 [`authorize_task_item`] 的读侧同款检查：读侧拒绝只让
/// 这条义务进不了 admitted，**事务照样提交**一行 `purpose = ADOPT_TASK_INSTRUCTION` 的
/// `task_binding_grants` 和一条 `UserConfirmed` 授权 Evidence——即一份持久的、断言「用户批准
/// 把这段内容当指令」的记录，而 §10.1 row 4/5 说这段内容永远不能是指令。授权不得被铸造出来，
/// 不是「铸出来以后读不到」。
///
/// # Errors
/// basis 里没有任何 `BehaviorEligible` origin ⇒ [`TaskContextReject::UntrustedInstruction`]，
/// 与读侧同一个原因码（同一条 §10.1 判据，不新开错误面）。
pub fn require_behavior_eligible_target(
    basis: &[EvidenceOriginClass],
) -> Result<(), TaskContextReject> {
    if basis.iter().any(|origin| {
        matches!(
            origin.max_disposition(),
            InstructionDisposition::BehaviorEligible
        )
    }) {
        Ok(())
    } else {
        Err(TaskContextReject::UntrustedInstruction)
    }
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

// =============================================================================
// §25.4 v2 / §10.1 任务授权（card 22c, ADR-0046）
// =============================================================================

/// 一个 selector 对候选**权威**的要求。两条臂是**两种不同的门**，不是同一条数轴上的两个点。
///
/// - [`Self::StoredAtLeast`]：判据在行上——`memory_records.authority_class` 自己。
/// - [`Self::VerifiedCurrentTaskBinding`]：判据**不在行上**。存储权威永远 ≤ 5（I-STORE），
///   6 是「当前任务上下文实例」的**使用**权威，由 [`authorize_task_item`] 验过的一条
///   任务绑定授权证明（I-TASK）。把它写成 `StoredAtLeast(ExplicitTaskContext)` 就是
///   card 22b 留下的那个空集：∀m 存储权威 ≤ ceiling(origin) ≤ 5 < 6 ⇒ admitted 恒空。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityRequirement {
    /// 行自己的存储权威必须达到这个下限。
    StoredAtLeast(AuthorityClass),
    /// 必须有一条经过验证的、指向当前任务的绑定授权。
    VerifiedCurrentTaskBinding,
}

impl AuthorityRequirement {
    /// 这条要求隐含的**存储**权威下限。
    ///
    /// `VerifiedCurrentTaskBinding` 的下限是 [`AuthorityClass::PrivateKnowledge`]：授权证明的是
    /// 「当前任务批准把它当指令用」，不是「这条内容本身很可信」，所以它**不**追加一个高存储
    /// 门槛（裁决 §二.4 明说 C 方案不成立：5 的门槛会把合法的 UserConfirmed(4) 目标排除掉）。
    /// 它排除的只有 `PublicKnowledge`——公共池内容不是本租户的任务指令来源。
    #[must_use]
    pub const fn stored_floor(self) -> AuthorityClass {
        match self {
            Self::StoredAtLeast(class) => class,
            Self::VerifiedCurrentTaskBinding => AuthorityClass::PrivateKnowledge,
        }
    }
}

/// 一条被 v2 取代的 selector 登记记录（裁决 §三：v1 的解释与审计记录必须保留）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetiredSelector {
    /// 登记名。
    pub registered_name: &'static str,
    /// 取代它的登记名。
    pub superseded_by: &'static str,
    /// 为什么退役。
    pub reason: &'static str,
}

/// 退役但保留的 selector 登记记录。
pub const RETIRED_SELECTORS: [RetiredSelector; 1] = [RetiredSelector {
    registered_name: "task_explicit_context_v1",
    superseded_by: "task_explicit_context_v2",
    reason: "card 22b 冻结的准入对象是 memory_records.authority_class >= ExplicitTaskContext。\
             §10.1 的 origin ceiling 表让任何 origin 都到不了 6，于是它的 admitted 集**结构性**\
             恒空（ADR-0045 Open debt）。card 22c 的裁决把 6 从内容属性改为受验证的当前任务\
             授权，准入对象因此变了——裁决 §三 禁止同名 selector 偷换准入对象，所以 v1 退役、\
             v2 登记，v1 的记录留在这里供审计。",
}];

/// `memory.bind` 的用途（裁决 §二.2）。**只有** [`Self::AdoptTaskInstruction`] 会产生授权。
///
/// 这不是一个可选的注释字段：`REFERENCE_ONLY` 的绑定照样是一条 MANDATORY 义务、照样被提名、
/// 照样在诊断里出现，它只是拿不到 6。「绑定 = 授权」正是裁决点名要拆开的那一条。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingPurpose {
    /// 带上它当参考数据，不当指令。
    ReferenceOnly,
    /// 当前任务批准把它当任务指令采纳——唯一能铸出 [`VerifiedTaskGrant`] 的用途。
    AdoptTaskInstruction,
}

impl BindingPurpose {
    /// 线值（`memory.schema.json` 与 `private.task_binding_grants.purpose` 的闭集）。
    #[must_use]
    pub const fn wire(self) -> &'static str {
        match self {
            Self::ReferenceOnly => "REFERENCE_ONLY",
            Self::AdoptTaskInstruction => "ADOPT_TASK_INSTRUCTION",
        }
    }

    /// 线值 → 枚举。未知线值是 `None`，**不是**默认值：一个认不出来的 purpose 不能悄悄
    /// 退化成 `ReferenceOnly`（那会把一次写坏的升级读成一次温和的降级）。
    #[must_use]
    pub fn parse_wire(wire: &str) -> Option<Self> {
        match wire {
            "REFERENCE_ONLY" => Some(Self::ReferenceOnly),
            "ADOPT_TASK_INSTRUCTION" => Some(Self::AdoptTaskInstruction),
            _ => None,
        }
    }
}

/// 谁签发了这条授权（裁决 §二.3：「经过 gateway」不等于「来自有权任务请求」）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskGrantIssuer {
    /// 当前经过认证的任务请求（交互确认凭据已被消费）。
    AuthenticatedTaskRequest,
    /// 具体适用的租户策略。
    TenantPolicy,
}

impl TaskGrantIssuer {
    /// 线值（`private.task_binding_grants.issuer_kind` 的闭集）。
    #[must_use]
    pub const fn wire(self) -> &'static str {
        match self {
            Self::AuthenticatedTaskRequest => "AUTHENTICATED_TASK_REQUEST",
            Self::TenantPolicy => "TENANT_POLICY",
        }
    }

    /// 线值 → 枚举；未知线值 `None`（同 [`BindingPurpose::parse_wire`] 的理由）。
    #[must_use]
    pub fn parse_wire(wire: &str) -> Option<Self> {
        match wire {
            "AUTHENTICATED_TASK_REQUEST" => Some(Self::AuthenticatedTaskRequest),
            "TENANT_POLICY" => Some(Self::TenantPolicy),
            _ => None,
        }
    }
}

/// 本卡冻结的任务授权契约版本，写进每一条 grant 的 `policy_version`。
pub const TASK_AUTHORIZATION_POLICY_VERSION: &str = "task_explicit_context_v2";

/// grant 行上那个不许变的数：6。写成常量而不是字面量，是为了让「6 只有一个来源」成立。
pub const TASK_GRANT_AUTHORITY: i16 = AuthorityClass::ExplicitTaskContext as i16;

/// §33.10 rule 9 / ADR-0046 D-D: the successor leg of a `memory.bind` / `memory.unbind`
/// confirm claim is the intent digest (task + purpose), not the bare task id.
/// `control.confirm_tokens` binds (tenant, user, operation, target, successor) and has no room
/// for a third argument, and a parameter the token does not cover must not carry permission;
/// `purpose` decides whether the call writes an authorization at all, so it rides here. The
/// gateway mints with this function and the adapter's confirm check compares with it — one
/// construction, so the two sides cannot drift (card 22c's first cut-off run left them apart
/// and every confirmed bind answered CONFLICT).
///
/// Domain-separated SHA-256 truncated to 16 bytes: a lookup key inside one tenant's
/// confirm-token table, not a security boundary on its own (the row also pins tenant, user,
/// operation and target).
#[must_use]
pub fn binding_confirmation_successor(
    task: crate::ids::TaskId,
    purpose: Option<BindingPurpose>,
) -> Uuid {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"humaux.memory.binding.intent.v2\0");
    hasher.update(task.0.as_bytes());
    hasher.update(b"\0");
    hasher.update(purpose.map_or("", BindingPurpose::wire).as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

/// 本次请求中**已认证解析**的任务，连同它当下的授权 epoch。
///
/// 字段私有、无 `Default`、无字面量构造：唯一铸造点是 [`Self::from_resolved_task`]，而
/// `adapters::context_repo` 的唯一调用点是在同一事务里从 `coord.tasks` 解析成功之后。
/// wire 上的一个 uuid 不是任务。
#[derive(Debug, Clone, Copy)]
pub struct AuthenticatedTask {
    tenant_id: Uuid,
    task_id: Uuid,
    authorization_epoch: i64,
    _priv: (),
}

impl AuthenticatedTask {
    /// 唯一铸造点——调用方必须刚刚在同一事务里解析到这一行。
    #[must_use]
    pub const fn from_resolved_task(
        tenant_id: Uuid,
        task_id: Uuid,
        authorization_epoch: i64,
    ) -> Self {
        Self {
            tenant_id,
            task_id,
            authorization_epoch,
            _priv: (),
        }
    }

    /// 租户。
    #[must_use]
    pub const fn tenant_id(&self) -> Uuid {
        self.tenant_id
    }

    /// 任务。
    #[must_use]
    pub const fn task_id(&self) -> Uuid {
        self.task_id
    }

    /// 当下的授权 epoch。
    #[must_use]
    pub const fn authorization_epoch(&self) -> i64 {
        self.authorization_epoch
    }
}

/// 一条**提名**（绑定义务）的事实，从 `private.context_bindings` 逐字读出。
///
/// 提名集按绑定本身枚举，不按目标是否可用枚举（裁决 §六）：目标不可读、没有授权、grounding
/// 未决都**不**能让这条义务从清单里消失。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskBindingObligation {
    /// 绑定行。
    pub context_binding_id: Uuid,
    /// 租户。
    pub tenant_id: Uuid,
    /// `scope_kind`。
    pub scope_kind: ScopeKind,
    /// `scope_id`——TASK 档就是任务 id。
    pub scope_id: Option<Uuid>,
    /// 档位。
    pub mode: BindingMode,
    /// 绑定的目标。
    pub memory_id: MemoryId,
    /// 已撤销？
    pub revoked: bool,
}

/// 一条 grant 行的原始字段，逐字从 `private.task_binding_grants` 读出。**未经验证**——
/// 它只是「库里长这样」，[`authorize_task_item`] 之前它什么都不证明。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawTaskGrant {
    /// 租户。
    pub tenant_id: Uuid,
    /// 绑定。
    pub context_binding_id: Uuid,
    /// 任务。
    pub task_id: Uuid,
    /// 目标 memory。
    pub memory_id: MemoryId,
    /// `scope_kind` 是 `TASK`？
    pub scope_kind_is_task: bool,
    /// `mode` 是 `MANDATORY`？
    pub mode_is_mandatory: bool,
    /// 签发时任务的授权 epoch。
    pub task_epoch: i64,
    /// 被批准的**那一份**内容的规范化摘要。
    pub payload_sha256: [u8; 32],
    /// 行上写的授权等级（必须是 [`TASK_GRANT_AUTHORITY`]）。
    pub grant_authority: i16,
    /// 用途；线值认不出来时 `None`。
    pub purpose: Option<BindingPurpose>,
    /// 签发者类别；线值认不出来时 `None`。
    pub issuer_kind: Option<TaskGrantIssuer>,
    /// `policy_version` 与本卡冻结的版本一致？
    pub policy_version_matches: bool,
    /// 授权证据行真的存在（FK 只保证引用完整，不保证这一次读得到）。
    pub authorization_evidence_present: bool,
    /// 签发时刻（epoch 秒）。
    pub issued_at_epoch_s: i64,
    /// 过期时刻（epoch 秒）；`None` = 不按墙钟过期，由任务 epoch 与撤销决定寿命。
    pub expires_at_epoch_s: Option<i64>,
    /// 已撤销？
    pub revoked: bool,
}

/// 目标 memory 在本快照里的事实。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskTargetFacts {
    /// 租户。
    pub tenant_id: Uuid,
    /// memory。
    pub memory_id: MemoryId,
    /// 存储权威（I-STORE：这个值**不会**因为有授权而改变）。
    pub stored_authority: AuthorityClass,
    /// 它的 evidence origin basis 允许的最高 disposition（§10.1 row 4/5 的 DATA_ONLY 半边）。
    pub max_disposition: InstructionDisposition,
    /// active 且未被 supersede、未归档。
    pub active: bool,
    /// 本次请求可读（`can_read`，含 backing evidence 的失败关闭）。
    pub readable: bool,
    /// **当下**内容的规范化摘要。
    pub payload_sha256: [u8; 32],
}

/// [`authorize_task_item`] 的全部输入：一条义务，加上围着它的三组事实。
#[derive(Debug, Clone, Copy)]
pub struct TaskGrantFacts {
    /// 这条义务。
    pub obligation: TaskBindingObligation,
    /// 对应的 grant 行；`None` = 根本没有（LEFT JOIN 的真实结果，不是被过滤掉的）。
    pub grant: Option<RawTaskGrant>,
    /// 目标行；`None` = 读不到这一行。
    pub target: Option<TaskTargetFacts>,
    /// 判定时刻（epoch 秒），由 adapters 用库时钟取。
    pub now_epoch_s: i64,
}

/// 准入失败的**内部**诊断原因集（裁决 §四）。对外经 [`Self::error_code`] 落到既有信封上，
/// 在诊断块里以 [`Self::wire`] 的脱敏字符串出现——它描述的是**义务的状态**，不泄露内容。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskContextReject {
    /// 这条义务不属于本次已认证的当前任务。
    TaskNotCurrent,
    /// 绑定本身的 tenant/scope/mode/撤销状态不对。
    BindingScopeMismatch,
    /// 没有授权行。**最常见的一条**，也是「绑定 ≠ 授权」的落点。
    MissingTaskAuthorization,
    /// 有授权行，但它指向的 (tenant, task, binding, memory) 不是这一条。
    GrantTargetMismatch,
    /// 授权行存在且对得上，但它此刻无效：等级/用途/签发者/policy 版本/epoch/撤销/未生效/已过期。
    TaskAuthorizationInactive,
    /// 目标行读不到（不存在，或 RLS 挡住）。
    TargetMissing,
    /// 目标行存在但本次请求无权读。
    TargetNotReadable,
    /// 目标的 origin 决定它只能当数据用（§10.1 row 4/5）——授权**不能**把 DATA_ONLY 改成指令。
    UntrustedInstruction,
    /// 目标不是 active / 已被 supersede / 已归档，或存储权威低于 selector 的存储下限。
    TargetNotActiveOrGrounded,
    /// 被批准的那一份内容与当下的内容不是同一份。
    TargetRevisionChanged,
}

impl TaskContextReject {
    /// 诊断块里的脱敏原因串。
    #[must_use]
    pub const fn wire(self) -> &'static str {
        match self {
            Self::TaskNotCurrent => "TASK_NOT_CURRENT",
            Self::BindingScopeMismatch => "BINDING_SCOPE_MISMATCH",
            Self::MissingTaskAuthorization => "MISSING_TASK_AUTHORIZATION",
            Self::GrantTargetMismatch => "GRANT_TARGET_MISMATCH",
            Self::TaskAuthorizationInactive => "TASK_AUTHORIZATION_INACTIVE",
            Self::TargetMissing => "TARGET_MISSING",
            Self::TargetNotReadable => "TARGET_NOT_READABLE",
            Self::UntrustedInstruction => "UNTRUSTED_INSTRUCTION",
            Self::TargetNotActiveOrGrounded => "TARGET_NOT_ACTIVE_OR_GROUNDED",
            Self::TargetRevisionChanged => "TARGET_REVISION_CHANGED",
        }
    }

    /// 落到既有 [`ErrorCode`] 闭集上（不新增错误码，裁决 §四「接入现有 envelope」）。
    ///
    /// 全部是 [`ErrorCode::Forbidden`] 只有一个例外：目标行根本读不到是 `NotFound`——
    /// 「没有授权」和「没有这条记忆」是两件事，合并成一个码会让调试从这里开始往错的方向走。
    #[must_use]
    pub const fn error_code(self) -> ErrorCode {
        match self {
            Self::TargetMissing => ErrorCode::NotFound,
            _ => ErrorCode::Forbidden,
        }
    }
}

/// 一条**验过的**任务授权。字段私有、无 `Default`、无 `Deserialize`、无字面量构造：
/// 唯一铸造点是 [`authorize_task_item`]。拿到它就等于「I-TASK 的全部条件刚刚被逐条检查过」。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifiedTaskGrant {
    grant: RawTaskGrant,
    _priv: (),
}

impl VerifiedTaskGrant {
    /// 这条授权所属的绑定。
    #[must_use]
    pub const fn context_binding_id(&self) -> Uuid {
        self.grant.context_binding_id
    }

    /// 这条授权所属的任务。
    #[must_use]
    pub const fn task_id(&self) -> Uuid {
        self.grant.task_id
    }

    /// 签发时的任务 epoch。
    #[must_use]
    pub const fn task_epoch(&self) -> i64 {
        self.grant.task_epoch
    }

    /// 被批准的那一份内容的摘要。
    #[must_use]
    pub const fn payload_sha256(&self) -> [u8; 32] {
        self.grant.payload_sha256
    }
}

/// 一条**当前任务上下文实例**——被授权按 [`AuthorityClass::ExplicitTaskContext`] 使用的目标。
///
/// 两个权威并排放着，而不是把低的那个覆盖掉：`source_authority` 是这条记忆**存储**的权威
/// （I-STORE，永远 ≤ 5，授权不改它），`effective_context_authority()` 是这一次**使用**的权威。
/// 把它们合成一个字段，就等于把「谁批准的」洗成「它本来就是这么可信」。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorizedTaskItem {
    memory_id: MemoryId,
    source_authority: AuthorityClass,
    grant: VerifiedTaskGrant,
    _priv: (),
}

impl AuthorizedTaskItem {
    /// 目标。
    #[must_use]
    pub const fn memory_id(&self) -> MemoryId {
        self.memory_id
    }

    /// 存储权威——**不变量 I-STORE 的可观察面**。
    #[must_use]
    pub const fn source_authority(&self) -> AuthorityClass {
        self.source_authority
    }

    /// 本次使用的有效上下文权威。恒为 [`AuthorityClass::ExplicitTaskContext`]；它是常量而不是
    /// 字段，因为这个类型存在的全部意义就是「这一次可以用 6」。
    #[must_use]
    pub const fn effective_context_authority(&self) -> AuthorityClass {
        AuthorityClass::ExplicitTaskContext
    }

    /// 支撑它的那条授权。
    #[must_use]
    pub const fn grant(&self) -> VerifiedTaskGrant {
        self.grant
    }
}

/// I-TASK 的唯一判定点（裁决 §四.2 的顺序，逐条）。
///
/// 顺序即语义，且**顺序本身是判据的一部分**：先确定这条义务属于当前任务，再看绑定形状，
/// 再看有没有授权，再看授权指向谁，再看授权此刻是否有效，最后才看目标本身。反过来先看
/// 目标，就会让「目标很干净」把「没有人批准过」盖过去。
///
/// 绝不会写 `memory.authority_class = 6`，也绝不会 `max()`：本函数不修改任何东西，它只
/// 决定「这一次能不能用 6」。找不到授权时也**不**退化成「当作低一点权威但 Mandatory 已履行」
/// ——那条路径在返回类型里不存在。
///
/// # Errors
/// [`TaskContextReject`] 的任一条；第一条不满足的检查就是返回值。
pub fn authorize_task_item(
    task: &AuthenticatedTask,
    stored_floor: AuthorityClass,
    facts: &TaskGrantFacts,
) -> Result<AuthorizedTaskItem, TaskContextReject> {
    let obligation = facts.obligation;

    // 1. 当前任务：租户一致、scope 就是本次已认证的那个任务。
    if obligation.tenant_id != task.tenant_id() || obligation.scope_id != Some(task.task_id()) {
        return Err(TaskContextReject::TaskNotCurrent);
    }
    // 2. 绑定形状：TASK + MANDATORY + 未撤销。
    if !matches!(obligation.scope_kind, ScopeKind::Task)
        || !matches!(obligation.mode, BindingMode::Mandatory)
        || obligation.revoked
    {
        return Err(TaskContextReject::BindingScopeMismatch);
    }
    // 3. 有没有授权。**绑定不是授权**——这条臂就是那句话的可执行形态。
    let Some(grant) = facts.grant else {
        return Err(TaskContextReject::MissingTaskAuthorization);
    };
    // 4. 授权指向的是不是这一条义务。
    if grant.tenant_id != task.tenant_id()
        || grant.task_id != task.task_id()
        || grant.context_binding_id != obligation.context_binding_id
        || grant.memory_id != obligation.memory_id
        || !grant.scope_kind_is_task
        || !grant.mode_is_mandatory
    {
        return Err(TaskContextReject::GrantTargetMismatch);
    }
    // 5. 授权此刻有效吗：等级、用途、签发者、policy 版本、证据、epoch、撤销、生效窗口。
    let purpose_ok = matches!(grant.purpose, Some(BindingPurpose::AdoptTaskInstruction));
    let issuer_ok = grant.issuer_kind.is_some();
    let window_ok = grant.issued_at_epoch_s <= facts.now_epoch_s
        && grant
            .expires_at_epoch_s
            .is_none_or(|expires| expires > facts.now_epoch_s);
    if grant.grant_authority != TASK_GRANT_AUTHORITY
        || !purpose_ok
        || !issuer_ok
        || !grant.policy_version_matches
        || !grant.authorization_evidence_present
        || grant.revoked
        || grant.task_epoch != task.authorization_epoch()
        || !window_ok
    {
        return Err(TaskContextReject::TaskAuthorizationInactive);
    }
    // 6. 目标本身。授权不能替目标回答「它在不在、能不能读、能不能当指令」。
    let Some(target) = facts.target else {
        return Err(TaskContextReject::TargetMissing);
    };
    if target.tenant_id != task.tenant_id() || target.memory_id != obligation.memory_id {
        return Err(TaskContextReject::GrantTargetMismatch);
    }
    if !target.readable {
        return Err(TaskContextReject::TargetNotReadable);
    }
    // §10.1 row 4/5：DATA_ONLY 的 origin 不因为有人批准就变成行为指令（G59-6 边界不动）。
    if !matches!(
        target.max_disposition,
        InstructionDisposition::BehaviorEligible
    ) {
        return Err(TaskContextReject::UntrustedInstruction);
    }
    // I-STORE 在读侧再执行一次：存储权威仍然受 origin ceiling 约束，且仍然 ≤ 5。
    if !target.active
        || (target.stored_authority as u8) < (stored_floor as u8)
        || matches!(target.stored_authority, AuthorityClass::ExplicitTaskContext)
    {
        return Err(TaskContextReject::TargetNotActiveOrGrounded);
    }
    // 7. 精确内容：被批准的那一份，不是「同一个 memory_id 的最新一份」。
    if target.payload_sha256 != grant.payload_sha256 {
        return Err(TaskContextReject::TargetRevisionChanged);
    }
    Ok(AuthorizedTaskItem {
        memory_id: obligation.memory_id,
        source_authority: target.stored_authority,
        grant: VerifiedTaskGrant { grant, _priv: () },
        _priv: (),
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

    // =========================================================================
    // card 22c / ADR-0046 —— I-TASK 与 I-NONINHERIT 的定向注错闸
    // =========================================================================

    /// §78.1 GOLDEN：`min_authority` 是 `authority.stored_floor()` 的派生值，两者不许漂。
    /// 注错：把任一条 REGISTRY 的 `min_authority` 改成别的类 ⇒ 红。
    #[test]
    fn registry_min_authority_is_derived_from_requirement() {
        for s in &REGISTRY {
            assert_eq!(
                s.min_authority,
                s.authority.stored_floor(),
                "{:?} 的 min_authority 与 authority 漂了",
                s.id
            );
        }
    }

    /// 裁决 §三：v2 登记，v1 的记录保留且指向 v2。
    #[test]
    fn task_selector_is_registered_as_v2_and_v1_is_retired() {
        assert_eq!(
            spec(SelectorId::TaskExplicitContextV1).registered_name,
            "task_explicit_context_v2"
        );
        assert!(matches!(
            spec(SelectorId::TaskExplicitContextV1).authority,
            AuthorityRequirement::VerifiedCurrentTaskBinding
        ));
        let retired = RETIRED_SELECTORS
            .iter()
            .find(|r| r.registered_name == "task_explicit_context_v1")
            .expect("v1 的登记记录必须保留供审计");
        assert_eq!(retired.superseded_by, "task_explicit_context_v2");
        assert!(!retired.reason.is_empty());
        // 登记名全集不重复——「同名 selector 偷换准入对象」在名字层就不成立。
        let names: HashSet<&str> = REGISTRY
            .iter()
            .map(|s| s.registered_name)
            .chain(RETIRED_SELECTORS.iter().map(|r| r.registered_name))
            .collect();
        assert_eq!(names.len(), REGISTRY.len() + RETIRED_SELECTORS.len());
    }

    const TEST_DIGEST: [u8; 32] = [7u8; 32];
    const OTHER_DIGEST: [u8; 32] = [9u8; 32];

    fn task_of(tenant: Uuid, task: Uuid, epoch: i64) -> AuthenticatedTask {
        AuthenticatedTask::from_resolved_task(tenant, task, epoch)
    }

    fn obligation_of(
        tenant: Uuid,
        task: Uuid,
        binding: Uuid,
        memory: MemoryId,
    ) -> TaskBindingObligation {
        TaskBindingObligation {
            context_binding_id: binding,
            tenant_id: tenant,
            scope_kind: ScopeKind::Task,
            scope_id: Some(task),
            mode: BindingMode::Mandatory,
            memory_id: memory,
            revoked: false,
        }
    }

    fn grant_of(tenant: Uuid, task: Uuid, binding: Uuid, memory: MemoryId) -> RawTaskGrant {
        RawTaskGrant {
            tenant_id: tenant,
            context_binding_id: binding,
            task_id: task,
            memory_id: memory,
            scope_kind_is_task: true,
            mode_is_mandatory: true,
            task_epoch: 0,
            payload_sha256: TEST_DIGEST,
            grant_authority: TASK_GRANT_AUTHORITY,
            purpose: Some(BindingPurpose::AdoptTaskInstruction),
            issuer_kind: Some(TaskGrantIssuer::AuthenticatedTaskRequest),
            policy_version_matches: true,
            authorization_evidence_present: true,
            issued_at_epoch_s: 1_000,
            expires_at_epoch_s: None,
            revoked: false,
        }
    }

    fn target_of(tenant: Uuid, memory: MemoryId) -> TaskTargetFacts {
        TaskTargetFacts {
            tenant_id: tenant,
            memory_id: memory,
            // 正对照就是裁决点名的那一条：合法可读 grounded 的 UserConfirmed(4)。
            stored_authority: AuthorityClass::UserCorrection,
            max_disposition: InstructionDisposition::BehaviorEligible,
            active: true,
            readable: true,
            payload_sha256: TEST_DIGEST,
        }
    }

    /// 一套完整的正对照事实。每个注错测试从它出发，只动一处。
    fn positive_facts(tenant: Uuid, task: Uuid, binding: Uuid, memory: MemoryId) -> TaskGrantFacts {
        TaskGrantFacts {
            obligation: obligation_of(tenant, task, binding, memory),
            grant: Some(grant_of(tenant, task, binding, memory)),
            target: Some(target_of(tenant, memory)),
            now_epoch_s: 2_000,
        }
    }

    fn floor() -> AuthorityClass {
        spec(SelectorId::TaskExplicitContextV1)
            .authority
            .stored_floor()
    }

    /// 正对照（裁决 §七 的前置条件）：合法可读 grounded 的 UserCorrection(4) 记忆，
    /// 经当前任务精确批准，拿到 6 —— 而**存储**权威仍然是 4。
    #[test]
    fn task_explicit_context_v2_positive_control() {
        let (tenant, task, binding, memory) = (
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            MemoryId::new(),
        );
        let item = authorize_task_item(
            &task_of(tenant, task, 0),
            floor(),
            &positive_facts(tenant, task, binding, memory),
        )
        .expect("正对照必须准入");
        assert_eq!(
            item.effective_context_authority(),
            AuthorityClass::ExplicitTaskContext
        );
        assert_eq!(item.source_authority(), AuthorityClass::UserCorrection);
        assert_eq!(item.grant().task_id(), task);
        assert_eq!(item.grant().payload_sha256(), TEST_DIGEST);

        // 铸出的 MandatoryRow 两个权威并排，不是一个覆盖另一个。
        let s = spec(SelectorId::TaskExplicitContextV1);
        let admitted = MandatoryRow::from_task_grant(s, item, 10, RowGrounding::NotJudged)
            .expect("spec 是 VerifiedCurrentTaskBinding");
        match admitted {
            Admitted::Row(row) => {
                assert_eq!(row.authority(), AuthorityClass::ExplicitTaskContext);
                assert_eq!(row.source_authority(), AuthorityClass::UserCorrection);
            }
            Admitted::NeedsVerification(nv) => panic!("不该被分流: {nv:?}"),
        }
    }

    /// 「绑定 ≠ 授权」：同一条 MANDATORY/TASK 绑定，没有 grant ⇒ 不准入，原因具名。
    /// 注错（裁决 §七「绑定≠授权」）：让 mode=MANDATORY 本身就授 6 ⇒ 本测试红。
    #[test]
    fn task_explicit_context_v2_negative_control_missing_authorization() {
        let (tenant, task, binding, memory) = (
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            MemoryId::new(),
        );
        let mut facts = positive_facts(tenant, task, binding, memory);
        facts.grant = None;
        assert_eq!(
            authorize_task_item(&task_of(tenant, task, 0), floor(), &facts),
            Err(TaskContextReject::MissingTaskAuthorization)
        );
        assert_eq!(
            TaskContextReject::MissingTaskAuthorization.wire(),
            "MISSING_TASK_AUTHORIZATION"
        );
    }

    /// 裁决 §七 的其余机制闸，逐条一个定向用例。每一条只动正对照的一个字段。
    #[test]
    // 一条注错一段，逐条对照同一份正对照事实；拆成十几个 #[test] 只会让「改了哪一处」
    // 从眼前消失，而这正是本测试要盯的东西。
    #[allow(clippy::too_many_lines)]
    fn task_authorization_rejects_each_broken_dimension() {
        let (tenant, task, binding, memory) = (
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            MemoryId::new(),
        );
        let base = positive_facts(tenant, task, binding, memory);
        let current = task_of(tenant, task, 0);

        // 用途：reference-only 的确认不是采纳指令的授权。
        let mut f = base;
        f.grant.as_mut().expect("grant").purpose = Some(BindingPurpose::ReferenceOnly);
        assert_eq!(
            authorize_task_item(&current, floor(), &f),
            Err(TaskContextReject::TaskAuthorizationInactive)
        );

        // 作用域：换任务。
        let other_task = Uuid::now_v7();
        let mut f = base;
        f.grant.as_mut().expect("grant").task_id = other_task;
        assert_eq!(
            authorize_task_item(&current, floor(), &f),
            Err(TaskContextReject::GrantTargetMismatch)
        );
        // 同一条 grant 拿到另一个任务的读取里，也进不去。
        assert_eq!(
            authorize_task_item(&task_of(tenant, other_task, 0), floor(), &base),
            Err(TaskContextReject::TaskNotCurrent)
        );

        // 作用域：关闭重开任务（epoch 变了），旧授权立即失效。
        assert_eq!(
            authorize_task_item(&task_of(tenant, task, 1), floor(), &base),
            Err(TaskContextReject::TaskAuthorizationInactive)
        );

        // 跨租户。
        assert_eq!(
            authorize_task_item(&task_of(Uuid::now_v7(), task, 0), floor(), &base),
            Err(TaskContextReject::TaskNotCurrent)
        );

        // 撤销：撤销后不可恢复。
        let mut f = base;
        f.grant.as_mut().expect("grant").revoked = true;
        assert_eq!(
            authorize_task_item(&current, floor(), &f),
            Err(TaskContextReject::TaskAuthorizationInactive)
        );

        // 过期。
        let mut f = base;
        f.grant.as_mut().expect("grant").expires_at_epoch_s = Some(1_500);
        assert_eq!(
            authorize_task_item(&current, floor(), &f),
            Err(TaskContextReject::TaskAuthorizationInactive)
        );

        // 尚未生效。
        let mut f = base;
        f.grant.as_mut().expect("grant").issued_at_epoch_s = 9_999;
        assert_eq!(
            authorize_task_item(&current, floor(), &f),
            Err(TaskContextReject::TaskAuthorizationInactive)
        );

        // 等级：grant_authority 不是 6。
        let mut f = base;
        f.grant.as_mut().expect("grant").grant_authority = 5;
        assert_eq!(
            authorize_task_item(&current, floor(), &f),
            Err(TaskContextReject::TaskAuthorizationInactive)
        );

        // 授权证据缺席 / policy 版本不符。
        for mutate in [
            (|g: &mut RawTaskGrant| g.authorization_evidence_present = false)
                as fn(&mut RawTaskGrant),
            |g: &mut RawTaskGrant| g.policy_version_matches = false,
            |g: &mut RawTaskGrant| g.issuer_kind = None,
        ] {
            let mut f = base;
            mutate(f.grant.as_mut().expect("grant"));
            assert_eq!(
                authorize_task_item(&current, floor(), &f),
                Err(TaskContextReject::TaskAuthorizationInactive)
            );
        }

        // 精确内容：同一个 memory_id，内容变了 ⇒ 不准入（「自动 follow latest」的注错落点）。
        let mut f = base;
        f.target.as_mut().expect("target").payload_sha256 = OTHER_DIGEST;
        assert_eq!(
            authorize_task_item(&current, floor(), &f),
            Err(TaskContextReject::TargetRevisionChanged)
        );

        // 目标不可读 / 不存在 / 非 active。
        let mut f = base;
        f.target.as_mut().expect("target").readable = false;
        assert_eq!(
            authorize_task_item(&current, floor(), &f),
            Err(TaskContextReject::TargetNotReadable)
        );
        let mut f = base;
        f.target = None;
        assert_eq!(
            authorize_task_item(&current, floor(), &f),
            Err(TaskContextReject::TargetMissing)
        );
        let mut f = base;
        f.target.as_mut().expect("target").active = false;
        assert_eq!(
            authorize_task_item(&current, floor(), &f),
            Err(TaskContextReject::TargetNotActiveOrGrounded)
        );

        // DATA_ONLY 边界：授权不能把只能当数据的 origin 改成行为指令（G59-6 不动）。
        let mut f = base;
        f.target.as_mut().expect("target").max_disposition = InstructionDisposition::DataOnly;
        assert_eq!(
            authorize_task_item(&current, floor(), &f),
            Err(TaskContextReject::UntrustedInstruction)
        );

        // 同一条 §10.1 row 4/5 判据在**写侧**（裁决 §四.4 的
        // `require_behavior_eligible_target`）：DATA_ONLY-only basis 连授权都不许铸出来。
        // 存在性读法——混合 basis 里有一个 BehaviorEligible origin 就够。
        assert_eq!(
            require_behavior_eligible_target(&[
                EvidenceOriginClass::ToolResult,
                EvidenceOriginClass::ExternalContent,
                EvidenceOriginClass::UploadedArtifact,
                EvidenceOriginClass::SystemMigration,
                EvidenceOriginClass::TrustedConnector,
            ]),
            Err(TaskContextReject::UntrustedInstruction)
        );
        assert_eq!(
            require_behavior_eligible_target(&[
                EvidenceOriginClass::ToolResult,
                EvidenceOriginClass::UserConfirmed,
            ]),
            Ok(())
        );
        assert_eq!(
            require_behavior_eligible_target(&[]),
            Err(TaskContextReject::UntrustedInstruction),
            "没有 basis 就没有行为资格——失败关闭"
        );

        // I-STORE 的读侧复核：一条**存储**着 6 的历史行不能靠授权进来。
        let mut f = base;
        f.target.as_mut().expect("target").stored_authority = AuthorityClass::ExplicitTaskContext;
        assert_eq!(
            authorize_task_item(&current, floor(), &f),
            Err(TaskContextReject::TargetNotActiveOrGrounded)
        );

        // 绑定形状：撤销的绑定、非 TASK scope、非 MANDATORY。
        for mutate in [
            (|o: &mut TaskBindingObligation| o.revoked = true) as fn(&mut TaskBindingObligation),
            |o: &mut TaskBindingObligation| o.scope_kind = ScopeKind::Workspace,
            |o: &mut TaskBindingObligation| o.mode = BindingMode::Pinned,
        ] {
            let mut f = base;
            mutate(&mut f.obligation);
            assert_eq!(
                authorize_task_item(&current, floor(), &f),
                Err(TaskContextReject::BindingScopeMismatch)
            );
        }
    }

    /// I-NONINHERIT 的类型面：`VerifiedTaskGrant` / `AuthorizedTaskItem` 只能由
    /// [`authorize_task_item`] 造出来，所以「consolidation 复制一条授权」写不出来；
    /// 而 `from_selector` 拒绝为 v2 的 spec 按存储权威铸行，`from_task_grant` 拒绝为
    /// `StoredAtLeast` 的 spec 铸行 —— 两条入口不能互相冒充。
    #[test]
    fn task_lane_entry_points_do_not_impersonate_each_other() {
        let v2 = spec(SelectorId::TaskExplicitContextV1);
        assert!(matches!(
            MandatoryRow::from_selector(
                v2,
                MemoryId::new(),
                AuthorityClass::ExplicitTaskContext,
                10,
                RowGrounding::NotJudged
            ),
            Err(ErrorCode::InvalidInput)
        ));
        let (tenant, task, binding, memory) = (
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            MemoryId::new(),
        );
        let item = authorize_task_item(
            &task_of(tenant, task, 0),
            floor(),
            &positive_facts(tenant, task, binding, memory),
        )
        .expect("正对照");
        assert!(matches!(
            MandatoryRow::from_task_grant(
                spec(SelectorId::ProjectActiveConstraintsV1),
                item,
                10,
                RowGrounding::NotJudged
            ),
            Err(ErrorCode::InvalidInput)
        ));
    }

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
    /// card 22c / ADR-0046 D-J：MANDATORY 写侧的额外下限**换成**了 v2 的存储下限。
    ///
    /// 下限本身还在（policy 放行 ≠ 够格建 MANDATORY 绑定），但它不再是 `ProjectConstraint`
    /// ——裁决 §二.4 点名拒绝 5-下限，因为它把正对照要用的合法 `UserCorrection(4)` 目标排除掉。
    /// 现在的下限是 `PrivateKnowledge`：公共池内容不是本租户的任务指令来源。
    ///
    /// 两头都断言，否则这条闸会因为「把什么都放行」而假绿：`PublicKnowledge` 仍被拒，
    /// `UserCorrection` 必须通过。
    #[test]
    fn mandatory_write_floor_is_the_v2_stored_floor_not_project_constraint() {
        let request = req(BindingMode::Mandatory);
        let actor =
            ElevatedActor::from_consumed_confirmation(DestructiveOp::MemoryBind, request.memory_id)
                .expect("bind confirmation mints the elevated actor");
        let scope = scope_of(Uuid::now_v7(), None, None);
        let basis = NonEmptyVec::new(vec![EvidenceOriginClass::DirectUserInput]).expect("basis");

        let public = AlwaysAuthorizes(AuthorityClass::PublicKnowledge);
        assert_eq!(
            authorize_mandatory(
                &public,
                &actor,
                request,
                AuthorityClass::PublicKnowledge,
                MemoryType::Note,
                basis.clone(),
                &scope,
            ),
            Err(CandidateRejection::OriginAuthorityCeiling),
            "policy 放行不等于够格进 MANDATORY lane：公共池内容仍被拒"
        );

        // 正对照，也是本卡的解锁点：UserCorrection(4) 必须能被绑定，它的准入靠的是授权，
        // 不是一个更高的存储等级。注错：把下限改回 ProjectConstraint ⇒ 本条红。
        let correction = AlwaysAuthorizes(AuthorityClass::UserCorrection);
        assert!(
            authorize_mandatory(
                &correction,
                &actor,
                request,
                AuthorityClass::UserCorrection,
                MemoryType::Note,
                basis.clone(),
                &scope,
            )
            .is_ok(),
            "裁决 §二.4：5-下限会把合法的 UserConfirmed(4) 目标排除掉"
        );

        let strict = AlwaysAuthorizes(AuthorityClass::ProjectConstraint);
        assert!(
            authorize_mandatory(
                &strict,
                &actor,
                request,
                AuthorityClass::ProjectConstraint,
                MemoryType::Note,
                basis,
                &scope,
            )
            .is_ok()
        );
    }

    /// card 22b / ADR-0045 D-A：`ElevatedActor` 的铸造门与 memory 绑定。
    /// 注错：删掉 `from_consumed_confirmation` 的 op 判定，或删掉 `authorize_mandatory` 里
    /// 的 `actor.memory_id() != req.memory_id` 复核 ⇒ 本条红。
    #[test]
    fn elevated_actor_is_bound_to_one_op_and_one_memory() {
        let request = req(BindingMode::Mandatory);
        for op in DestructiveOp::ALL {
            let minted = ElevatedActor::from_consumed_confirmation(op, request.memory_id);
            assert_eq!(
                minted.is_ok(),
                matches!(op, DestructiveOp::MemoryBind | DestructiveOp::MemoryUnbind),
                "{} must not mint an ElevatedActor",
                op.operation_key()
            );
        }

        // X 的确认拿去绑 Y：MissingConfirmation，不是「反正都提权了」。
        let other = MemoryId(Uuid::now_v7());
        assert_ne!(other, request.memory_id);
        let actor = ElevatedActor::from_consumed_confirmation(DestructiveOp::MemoryBind, other)
            .expect("mint");
        let scope = scope_of(Uuid::now_v7(), None, None);
        let basis = NonEmptyVec::new(vec![EvidenceOriginClass::DirectUserInput]).expect("basis");
        assert_eq!(
            authorize_mandatory(
                &AlwaysAuthorizes(AuthorityClass::ExplicitTaskContext),
                &actor,
                request,
                AuthorityClass::ExplicitTaskContext,
                MemoryType::State,
                basis,
                &scope,
            ),
            Err(CandidateRejection::MissingConfirmation)
        );
    }
}
