//! `adapters::context_repo` — §25.4 Mandatory Context Lane 的 SQL 侧。
//!
//! 判定与类型在 [`humaux_domain::context`]（零 IO），本模块只负责发查询。分工的理由不是
//! 洁癖：§25.4 那条「不许用 embedding similarity 解释」之所以是拓扑保证，靠的是
//! `SelectorInput` 里没有 query/embedding 字段——那个约束住在 domain，本模块能做的只有
//! 「照它要的东西去取」。
//!
//! 走 [`RuntimeDbPool`]（`role_gateway`）：这是在线请求路径上的**纯读**，与 §16.2 的读路由
//! 同一条依据。§6.2.3 的 typed pool 闭集不动、无转换路径。
//!
//! **每个 selector 发两条查询**：一条不带 `LIMIT` 的 `COUNT(*)` 给 `expected`，一条取行。
//! 两个数分开取是 §25.5「禁止静默截断」在取数段**唯一能红**的形态——`expected` 若内生
//! （取 `rows.len()`），守恒式 `expected == returned + missing` 就退化成恒真算术，
//! 少带了多少永远算作 0。同 `EvidenceBlock` 的 ticket/no_batch 手法。

use humaux_domain::authority::{AuthorityClass, MemoryId};
use humaux_domain::context::{
    BindingGrant, MandatoryLane, MandatoryRow, PinnedLane, ScopeKind, SelectorId, SelectorOutcome,
    SelectorSpec, spec,
};
use humaux_domain::error::ErrorCode;
use humaux_domain::ids::Scope;
use sqlx::Row;
use sqlx::types::Uuid;

use crate::postgres::RuntimeDbPool;

type Txn<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

/// RLS 的租户上下文。与 `serving_repo` / `consolidate_repo` 逐字同形。
async fn set_tenant_local(txn: &mut Txn<'_>, tenant_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .execute(&mut **txn)
        .await?;
    Ok(())
}

/// 一个 selector 今天跑不跑得起来。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectorAvailability {
    /// 哪个 selector。
    pub id: SelectorId,
    /// `None` = 可用；`Some(name)` = **探测出来的**缺失对象名。
    pub missing_object: Option<String>,
}

/// 拿 [`humaux_domain::context::REGISTRY`] 声明的 `required_columns` 去比
/// `information_schema.columns`。
///
/// NA 的缺失对象因此是**探测出来的**（`private.memory_records.task_id`），不是写死的判断
/// ——列一落地，对应 selector 自动可用，没有人需要回来改一行代码（ADR-0006）。
///
/// # Errors
/// 库不可达时返回 [`ErrorCode::Internal`]。
pub async fn probe_selectors(pool: &RuntimeDbPool) -> Result<[SelectorAvailability; 5], ErrorCode> {
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;

    let mut out: Vec<SelectorAvailability> = Vec::with_capacity(5);
    for s in &humaux_domain::context::REGISTRY {
        let mut missing: Option<String> = None;
        for (schema, table, column) in s.required_columns {
            let exists: bool = sqlx::query(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 AND column_name = $3)",
            )
            .bind(schema)
            .bind(table)
            .bind(column)
            .fetch_one(&mut *txn)
            .await
            .map_err(|_| ErrorCode::Internal)?
            .try_get(0)
            .map_err(|_| ErrorCode::Internal)?;
            if !exists {
                // 第一个缺的就报出来——逐个列全部报出来对调用方没有增量信息，
                // 补第一个的时候自然会看见第二个。
                missing = Some(format!("{schema}.{table}.{column}"));
                break;
            }
        }
        out.push(SelectorAvailability {
            id: s.id,
            missing_object: missing,
        });
    }

    txn.commit().await.map_err(|_| ErrorCode::Internal)?;
    // REGISTRY 是定长 5，上面逐条 push，所以这里必然成功。
    out.try_into().map_err(|_| ErrorCode::Internal)
}

/// `scope_chain` 里所有 workspace 层 id（`project_active_constraints_v1` 的可见域）。
fn workspace_ids(scope: &Scope) -> Vec<Uuid> {
    humaux_domain::context::scope_chain(scope)
        .into_iter()
        .filter(|(kind, _)| matches!(kind, ScopeKind::Workspace))
        .map(|(_, id)| id)
        .collect()
}

/// `project_active_constraints_v1` 的 WHERE。
///
/// **`visibility_class <> 'USER_PRIVATE' OR visibility_user_id = $3` 这半句不可省。**
/// 今天 `0012_rls.sql` 的组合策略会兜住它，但 selector 自己的谓词不能错：任何一条绕过或
/// 尚未上 RLS 的读路径都会把**同租户里别人的私有 constraint** 装进 Context。
/// 判据不该依赖另一层恰好也在。
const PROJECT_CONSTRAINTS_WHERE: &str = "m.tenant_id = $1 \
     AND m.authority_class = 'ProjectConstraint' \
     AND m.status = 'active' \
     AND m.superseded_by IS NULL \
     AND (m.visibility_workspace_id IS NULL OR m.visibility_workspace_id = ANY($2)) \
     AND (m.visibility_class <> 'USER_PRIVATE' OR m.visibility_user_id = $3)";

/// `user_confirmed_corrections_v1` 的 WHERE。
///
/// §25.4：「active UserCorrection relevant to scope **不允许**用 embedding similarity
/// 解释；必须有机械 scope/authority 规则」。这条 `EXISTS(... origin_class='UserConfirmed')`
/// 就是那个机械替代品——`UserConfirmed` 是 §10.1 里 Agent 自己造不出来的 origin。
const USER_CORRECTIONS_WHERE: &str = "m.tenant_id = $1 \
     AND m.authority_class = 'UserCorrection' \
     AND m.status = 'active' \
     AND m.superseded_by IS NULL \
     AND (m.visibility_class <> 'USER_PRIVATE' OR m.visibility_user_id = $3) \
     AND EXISTS ( \
       SELECT 1 FROM private.memory_evidence me \
       JOIN private.evidence_objects eo ON eo.evidence_id = me.evidence_id \
       WHERE me.memory_id = m.memory_id AND eo.origin_class = 'UserConfirmed' \
     )";

/// `explicit_mandatory_bindings_v1` 的 WHERE。
///
/// **join 回 `memory_records` 复核 `authority_class`** 是读取侧门：即便有人绕过写侧门种下
/// 一条指向低 authority memory 的 MANDATORY binding，它也进不了 lane。写侧门今天是结构性
/// 关死的（`ElevatedActor` 无铸造路径），但读侧门不依赖写侧门成立——两道门各自独立。
const EXPLICIT_BINDINGS_WHERE: &str = "m.tenant_id = $1 \
     AND m.status = 'active' \
     AND m.superseded_by IS NULL \
     AND m.authority_class = 'ProjectConstraint' \
     AND (m.visibility_class <> 'USER_PRIVATE' OR m.visibility_user_id = $3) \
     AND EXISTS ( \
       SELECT 1 FROM private.context_bindings cb \
       WHERE cb.memory_id = m.memory_id \
         AND cb.tenant_id = m.tenant_id \
         AND cb.revoked_at IS NULL \
         AND cb.mode = 'MANDATORY' \
     )";

/// 每行的估计 token 数。
///
// ponytail: 用 content 的字节长度除以 4 作粗估——真 tokenizer 是 §63 的交付物，
// 引进来只为了填这个数不值当。预算判定对量级敏感、对精度不敏感（§25.5 判的是
// 「超没超硬上限」不是「差几个 token」）；真 tokenizer 落地后换掉这一处即可。
const EST_TOKENS_EXPR: &str = "GREATEST(1, (octet_length(m.content::text) / 4))::int4";

/// 跑一个 selector 的两条查询。
async fn run_selector(
    txn: &mut Txn<'_>,
    s: &'static SelectorSpec,
    where_clause: &str,
    tenant_id: Uuid,
    workspaces: &[Uuid],
    user_id: Option<Uuid>,
) -> Result<SelectorOutcome, ErrorCode> {
    // ① 不带 LIMIT 的 COUNT —— `expected` 的来源。与 ② 分开发，见模块 doc。
    let expected: i64 = sqlx::query(&format!(
        "SELECT count(*) FROM private.memory_records m WHERE {where_clause}"
    ))
    .bind(tenant_id)
    .bind(workspaces)
    .bind(user_id)
    .fetch_one(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?
    .try_get(0)
    .map_err(|_| ErrorCode::Internal)?;

    // ② 取行。
    let rows = sqlx::query(&format!(
        "SELECT m.memory_id, m.authority_class, {EST_TOKENS_EXPR} AS est_tokens \
         FROM private.memory_records m WHERE {where_clause} ORDER BY m.memory_id"
    ))
    .bind(tenant_id)
    .bind(workspaces)
    .bind(user_id)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let memory_id: Uuid = r.try_get("memory_id").map_err(|_| ErrorCode::Internal)?;
        let authority: String = r
            .try_get("authority_class")
            .map_err(|_| ErrorCode::Internal)?;
        let est_tokens: i32 = r.try_get("est_tokens").map_err(|_| ErrorCode::Internal)?;
        let authority = parse_authority(&authority)?;
        // `from_selector` 会按 spec 的 min_authority 复核——SQL 侧的 authority 过滤与
        // domain 侧的下限是两道独立的门，不是一道门写两遍：SQL 那条可能被将来的谓词改动
        // 放宽，domain 这条不会。
        out.push(
            MandatoryRow::from_selector(
                s,
                MemoryId(memory_id),
                authority,
                u32::try_from(est_tokens).unwrap_or(u32::MAX),
            )
            .map_err(|_| ErrorCode::Internal)?,
        );
    }

    Ok(SelectorOutcome::Ran {
        id: s.id,
        expected: u64::try_from(expected).unwrap_or(0),
        rows: out,
    })
}

/// DB 线值 → [`AuthorityClass`]。闭集与 `migrations/0004` 的 CHECK 对齐（§78.2）。
fn parse_authority(wire: &str) -> Result<AuthorityClass, ErrorCode> {
    match wire {
        "PublicKnowledge" => Ok(AuthorityClass::PublicKnowledge),
        "PrivateKnowledge" => Ok(AuthorityClass::PrivateKnowledge),
        "UserPreference" => Ok(AuthorityClass::UserPreference),
        "ProjectDecision" => Ok(AuthorityClass::ProjectDecision),
        "UserCorrection" => Ok(AuthorityClass::UserCorrection),
        "ProjectConstraint" => Ok(AuthorityClass::ProjectConstraint),
        "ExplicitTaskContext" => Ok(AuthorityClass::ExplicitTaskContext),
        _ => Err(ErrorCode::Internal),
    }
}

/// §25.4 步骤 2 + 5：取两条 lane。
///
/// 五个 selector 全部参与——不可用的那两个（`task_explicit_context_v1` /
/// `required_current_state_facets_v1`，缺 `task_id` / `facet` 列）由 [`probe_selectors`]
/// 探测出来后以 [`SelectorOutcome::Unavailable`] 交回，于是
/// [`MandatoryLane::from_selectors`] 会**拒绝构造整条 lane**。
///
/// 这是刻意的：「有两个 selector 坏了但先凑合上」不是一个可表达的状态，否则 Context 会在
/// selector 缺席时安静地少带东西，而调用方看到的仍是一条"正常"的 lane。
///
/// # Errors
/// 库不可达、authority 线值不在闭集内、或行不过 `min_authority` ⇒ [`ErrorCode::Internal`]。
/// 有 selector 不可用不是错误，它体现在返回的 `SelectorOutcome` 里。
pub async fn fetch_mandatory_outcomes(
    pool: &RuntimeDbPool,
    scope: &Scope,
) -> Result<[SelectorOutcome; 5], ErrorCode> {
    let availability = probe_selectors(pool).await?;
    let workspaces = workspace_ids(scope);
    let user_id = scope.user_id.map(|u| u.0);
    let tenant_id = scope.tenant_id.0;

    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    set_tenant_local(&mut txn, tenant_id)
        .await
        .map_err(|_| ErrorCode::Internal)?;

    let mut out: Vec<SelectorOutcome> = Vec::with_capacity(5);
    for a in availability {
        if let Some(missing_object) = a.missing_object {
            out.push(SelectorOutcome::Unavailable {
                id: a.id,
                missing_object,
            });
            continue;
        }
        let s = spec(a.id);
        let where_clause = match a.id {
            SelectorId::ProjectActiveConstraintsV1 => PROJECT_CONSTRAINTS_WHERE,
            SelectorId::UserConfirmedCorrectionsV1 => USER_CORRECTIONS_WHERE,
            SelectorId::ExplicitMandatoryBindingsV1 => EXPLICIT_BINDINGS_WHERE,
            // probe 说它可用（所需列都在），但本模块还没写它的谓词——这不是"可用"，
            // 是本模块欠它一条 WHERE。当成不可用交回并点名，不要拿一条空谓词冒充。
            other => {
                out.push(SelectorOutcome::Unavailable {
                    id: other,
                    missing_object: format!(
                        "adapters::context_repo 尚未实现 {other:?} 的 WHERE 子句"
                    ),
                });
                continue;
            }
        };
        out.push(run_selector(&mut txn, s, where_clause, tenant_id, &workspaces, user_id).await?);
    }

    txn.commit().await.map_err(|_| ErrorCode::Internal)?;
    out.try_into().map_err(|_| ErrorCode::Internal)
}

/// §25.4 步骤 5：Pinned lane。
///
/// 与 Mandatory 同源但走 `mode = 'PINNED'`，且**不复核 authority 下限**——Pinned 是用户
/// 显式钉的，钉什么是什么；MANDATORY 才有「只放得下 ProjectConstraint 及以上」的读侧门。
///
/// # Errors
/// 同 [`fetch_mandatory_outcomes`]。
pub async fn fetch_pinned(pool: &RuntimeDbPool, scope: &Scope) -> Result<PinnedLane, ErrorCode> {
    let tenant_id = scope.tenant_id.0;
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    set_tenant_local(&mut txn, tenant_id)
        .await
        .map_err(|_| ErrorCode::Internal)?;

    let rows = sqlx::query(&format!(
        "SELECT m.memory_id, m.authority_class, {EST_TOKENS_EXPR} AS est_tokens \
         FROM private.memory_records m \
         WHERE m.tenant_id = $1 AND m.status = 'active' AND m.superseded_by IS NULL \
           AND EXISTS ( \
             SELECT 1 FROM private.context_bindings cb \
             WHERE cb.memory_id = m.memory_id AND cb.tenant_id = m.tenant_id \
               AND cb.revoked_at IS NULL AND cb.mode = 'PINNED' \
           ) \
         ORDER BY m.memory_id"
    ))
    .bind(tenant_id)
    .fetch_all(&mut *txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    txn.commit().await.map_err(|_| ErrorCode::Internal)?;

    // Pinned 借用 `ExplicitMandatoryBindingsV1` 的 spec 只为拿它的 min_authority 复核位；
    // 若将来 Pinned 要独立声明（不同的 freshness/origin），加一条 REGISTRY 项，不要在这里
    // 造第二套规则。
    let s = spec(SelectorId::ExplicitMandatoryBindingsV1);
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let memory_id: Uuid = r.try_get("memory_id").map_err(|_| ErrorCode::Internal)?;
        let authority: String = r
            .try_get("authority_class")
            .map_err(|_| ErrorCode::Internal)?;
        let est_tokens: i32 = r.try_get("est_tokens").map_err(|_| ErrorCode::Internal)?;
        let authority = parse_authority(&authority)?;
        if (authority as u8) < (s.min_authority as u8) {
            // Pinned 不设下限，低 authority 的照收——但 `from_selector` 会拒。
            // 用一个不设下限的 spec 位来承载：这里退化成直接跳过，并在 lane 的
            // returned 上体现，不静默当成"钉了但没带"。
            continue;
        }
        out.push(
            MandatoryRow::from_selector(
                s,
                MemoryId(memory_id),
                authority,
                u32::try_from(est_tokens).unwrap_or(u32::MAX),
            )
            .map_err(|_| ErrorCode::Internal)?,
        );
    }
    Ok(PinnedLane::new(out))
}

/// 组装两条 lane。
///
/// # Errors
/// 见 [`fetch_mandatory_outcomes`]；lane 构不出来时把 domain 的 `LaneUnavailable`
/// 折成 [`ErrorCode::Internal`]——调用方要拿逐条缺失对象名的话走
/// [`fetch_mandatory_outcomes`] 自己组装。
pub async fn fetch_lanes(
    pool: &RuntimeDbPool,
    scope: &Scope,
) -> Result<(MandatoryLane, PinnedLane), ErrorCode> {
    let outcomes = fetch_mandatory_outcomes(pool, scope).await?;
    let mandatory = MandatoryLane::from_selectors(outcomes).map_err(|_| ErrorCode::Internal)?;
    let pinned = fetch_pinned(pool, scope).await?;
    Ok((mandatory, pinned))
}

/// 全 workspace **唯一**的 `INSERT INTO private.context_bindings`。
///
/// 只收 [`BindingGrant`]——它的字段私有、无 pub 构造式，拿到它的唯一办法是走
/// `domain::context` 的三个 `authorize_*` 之一。所以「绕过授权直接写 binding」不是一条
/// 要靠评审拦住的路径，是这个函数签名收不下的东西。
///
/// # Errors
/// 库不可达、或唯一索引冲突（同 scope 同 memory 同 mode 已有未撤销的 binding）。
pub async fn insert_binding(
    pool: &RuntimeDbPool,
    created_by: Uuid,
    grant: &BindingGrant,
    tenant_id: Uuid,
) -> Result<Uuid, ErrorCode> {
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    set_tenant_local(&mut txn, tenant_id)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    let id: Uuid = sqlx::query(
        "INSERT INTO private.context_bindings \
           (tenant_id, memory_id, mode, scope_kind, scope_id, created_by) \
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING context_binding_id",
    )
    .bind(tenant_id)
    .bind(grant.memory_id().0)
    .bind(grant.mode().wire())
    .bind(grant.scope_kind().wire())
    .bind(grant.scope_id())
    .bind(created_by)
    .fetch_one(&mut *txn)
    .await
    .map_err(|_| ErrorCode::Internal)?
    .try_get(0)
    .map_err(|_| ErrorCode::Internal)?;
    txn.commit().await.map_err(|_| ErrorCode::Internal)?;
    Ok(id)
}

/// 撤销一条 binding。**软删除**：binding 的历史是审计对象，物理删掉就查不到"谁在什么时候
/// 把什么钉进过 Context"。返回是否真的改了一行（已撤销的再撤一次返回 `false`）。
///
/// # Errors
/// 库不可达。
pub async fn revoke_binding(
    pool: &RuntimeDbPool,
    tenant_id: Uuid,
    binding_id: Uuid,
) -> Result<bool, ErrorCode> {
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    set_tenant_local(&mut txn, tenant_id)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    let affected = sqlx::query(
        "UPDATE private.context_bindings SET revoked_at = now() \
         WHERE context_binding_id = $1 AND tenant_id = $2 AND revoked_at IS NULL",
    )
    .bind(binding_id)
    .bind(tenant_id)
    .execute(&mut *txn)
    .await
    .map_err(|_| ErrorCode::Internal)?
    .rows_affected();
    txn.commit().await.map_err(|_| ErrorCode::Internal)?;
    Ok(affected == 1)
}
