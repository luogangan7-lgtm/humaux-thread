//! `adapters::context_repo` — §25.4 Mandatory Context Lane 的 SQL 侧。
//! Depends-on: crates=[async-trait, humaux-application, humaux-domain, humaux-projection, humaux-retrieval, sha2, sqlx]; services=[PostgreSQL(any) r=[coord.tasks, private.memory_evidence, private.memory_records, private.memory_subjects, projection.stream_log] w=[ops.selection_snapshots, private.context_bindings, private.evidence_objects, private.task_binding_grants]]; env=[]; modules=[adapters::confirm_token_repo, adapters::distill_repo, adapters::exact_census, adapters::postgres, adapters::quota_repo, adapters::read_materialize, adapters::request_guard_repo, adapters::selection_repo, adapters::stream_repo, application::continuity, application::pin, domain::audit, domain::authority, domain::confirm, domain::context, domain::error, domain::evidence, domain::grounding, domain::identity, domain::ids, domain::memory, domain::policy, domain::selection, projection::serving, projection::stream, retrieval::compiler, retrieval::completeness, retrieval::envelope, retrieval::handoff]
//! Called-by: [adapters::continuity_read, adapters::exact_census, adapters::memory_governance_repo, adapters::read_materialize, adapters::retrieve, gateway::context, gateway::mcp_application, gateway::memory, tests]
//! Invariants: [pure reads on role_gateway; each selector enumerates twice without LIMIT so expected is never derived
//!   from returned rows; both passes go through the same can_read re-check; a PG error is
//!   DependencyUnavailable/Internal, never an empty context]
//! Spec: Baseline §25.4; §16.2; §6.2.3
//!
//! 判定与类型在 [`humaux_domain::context`]（零 IO），本模块只负责发查询。分工的理由不是
//! 洁癖：§25.4 那条「不许用 embedding similarity 解释」之所以是拓扑保证，靠的是
//! `SelectorInput` 里没有 query/embedding 字段——那个约束住在 domain，本模块能做的只有
//! 「照它要的东西去取」。
//!
//! 走 [`RuntimeDbPool`]（`role_gateway`）：这是在线请求路径上的**纯读**，与 §16.2 的读路由
//! 同一条依据。§6.2.3 的 typed pool 闭集不动、无转换路径。
//!
//! **每个 selector 发两次独立的无 `LIMIT` 枚举**：第一次得到 `expected` 的候选集合，第二次
//! 投影 lane 行。两次都经过同一个 `can_read` 回验，但不能从投影行的长度反推 expected；否则
//! `expected == returned + missing` 会退化成恒真算术，少带了多少永远算作 0。

use humaux_domain::audit::{AuditEvent, AuditEventId, McpAuditAction};
use humaux_domain::authority::{AuthorityClass, MemoryId};
use humaux_domain::confirm::{DestructiveOp, RISK_TAG_CONFIRMATION_MINTED};
use humaux_domain::context::{
    Admitted, AuthenticatedTask, AuthorityRequirement, BindingGrant, BindingMode, BindingPurpose,
    BindingRequest, ConfirmedUserActor, ContextBudget, ElevatedActor, FrozenReads, MandatoryLane,
    MandatoryRow, PinnedLane, RawTaskGrant, ScopeKind, SelectorId, SelectorOutcome, SelectorSpec,
    TASK_AUTHORIZATION_POLICY_VERSION, TaskBindingObligation, TaskContextReject, TaskGrantFacts,
    TaskGrantIssuer, TaskTargetFacts, authorize_mandatory, authorize_pinned, authorize_task_item,
    spec,
};
use humaux_domain::error::ErrorCode;
use humaux_domain::grounding::{GroundingMode, RowGrounding, SnapshotEdge, classify_in_snapshot};
use humaux_domain::identity::{
    AuthorizationScope, VisibilityClass, VisibilityDescriptor, can_read,
};
use humaux_domain::ids::{Scope, UserId, WorkspaceId};
use humaux_domain::memory::{MandatoryContextFacet, MemoryType};
use humaux_domain::selection::{AUTHORIZED_MEMORY_ENUMERATION_V1, Cursor, query_fingerprint};
use humaux_projection::serving::StreamFamily;
use humaux_projection::stream::StreamKey;
use humaux_retrieval::compiler::{ContextItem, ContextOutcome};
use humaux_retrieval::envelope::{CountScope, EvidenceBlock, GroundingBlock, KnowledgeBlock};
use humaux_retrieval::handoff::{Handoff, assemble_with_context};
use sha2::{Digest, Sha256};
use sqlx::Row;
use sqlx::types::Uuid;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};

use crate::confirm_token_repo::{self, ConfirmationClaim};
use crate::exact_census::{ACTIVE_FINAL, CensusInputs, census_in_txn};
use crate::postgres::RuntimeDbPool;
use crate::quota_repo::{self, ReservationStatus, ReserveResult};
use crate::read_materialize::{
    MaterializedBodies, MaterializedItem, final_memory_ids_in_txn, materialize_final_bodies_in_txn,
    materialize_one_memory_in_txn,
};
use crate::request_guard_repo::{self, AuditTenant};
use crate::selection_repo::{
    begin_authorized_snapshot_in_txn, fetch_authorized_snapshot_page_in_txn,
};
use crate::stream_repo::close_ledger_in_txn;

type Txn<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

/// RLS 的租户上下文。与 `serving_repo` / `consolidate_repo` 逐字同形。
async fn set_tenant_local(txn: &mut Txn<'_>, tenant_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .execute(&mut **txn)
        .await?;
    Ok(())
}

pub(crate) async fn set_authorization_local(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
) -> Result<(), ErrorCode> {
    set_tenant_local(txn, authorization.tenant_id().0)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    // A pooled connection can have a session-level value from outside this adapter. Always
    // install a transaction-local value. Existing visibility RLS casts this GUC directly to
    // UUID, so a headless scope uses the nil UUID sentinel rather than an empty string; it
    // cannot match a normal authenticated user and keeps the query fail-closed.
    sqlx::query("SELECT set_config('humaux.user_id', $1, true)")
        .bind(
            authorization
                .user_id()
                .map(|user| user.0.to_string())
                .unwrap_or_else(|| Uuid::nil().to_string()),
        )
        .execute(&mut **txn)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(())
}

/// Resolves a tool-supplied scope against the authenticated scope. The resulting scope is
/// made only from authenticated fields; caller input can select one already-authorized
/// workspace but cannot manufacture a tenant, user, or deeper resource grant (§6.1.1).
fn canonical_scope(
    authorization: &AuthorizationScope,
    requested: &Scope,
) -> Result<(AuthorizationScope, Scope), ErrorCode> {
    // card 22b / §25.4.A(7)/(8): `task_id` is carried through, the other three narrow axes are
    // still refused. This function is pure, so the "已认证解析" half of §25.4.A(7) cannot happen
    // here: the id is RESOLVED against `coord.tasks` in the caller's own transaction
    // (`resolve_task_in_txn`), on both the read path (`run_selectors_in_txn`) and the write
    // path (`write_binding_confirmed`). A TASK is a **selector association range**, not an
    // authorization axis: it grants nothing on its own (the binding's target still runs the
    // full authority/origin floor), and without it `task_explicit_context_v1` has no TaskId to
    // resolve and is
    // structurally empty on every request. repository / run / agent stay refused because no
    // selector reads them yet, and a scope axis nothing consumes is a silent widening.
    if requested.tenant_id != authorization.tenant_id()
        || requested.user_id != authorization.user_id()
        || requested.repository_id.is_some()
        || requested.run_id.is_some()
        || requested.agent_id.is_some()
    {
        return Err(ErrorCode::Forbidden);
    }
    let authorization = match requested.workspace_id {
        Some(workspace) => authorization.narrow(workspace)?,
        None => authorization.clone(),
    };
    Ok((
        authorization.clone(),
        Scope {
            tenant_id: authorization.tenant_id(),
            user_id: authorization.user_id(),
            workspace_id: requested.workspace_id,
            repository_id: None,
            task_id: requested.task_id,
            run_id: None,
            agent_id: None,
        },
    ))
}

/// §25.4.A(7)'s "本次已认证解析的 TaskId": the wire uuid must name a real task of **this**
/// tenant (`coord.tasks`, §48; the row is RLS tenant-isolated and this transaction already
/// carries `humaux.tenant_id`), or the request is [`ErrorCode::NotFound`] — never a scope axis
/// the caller invented. Both directions go through here: the read path resolves before any
/// selector runs, the write path before a TASK binding is created or revoked.
///
/// Ceiling, stated rather than implied: `coord.tasks` models a task's **existence and tenant**,
/// not its membership — it has no owner/participant column, so "the caller belongs to this
/// task" is not checkable anywhere in this schema and is NOT asserted here. Bounded by the two
/// gates that do exist: every selector row still passes `readable_memory_ids`/`can_read`, and
/// every binding write still passes the §33.10 confirm gate bound to (user, memory, task).
/// ponytail: per-task membership when `coord.tasks` gains a participant table.
///
/// # Errors
/// [`ErrorCode::NotFound`] when no such task exists in this tenant; `Internal` on a DB error.
async fn resolve_task_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    task: humaux_domain::ids::TaskId,
) -> Result<AuthenticatedTask, ErrorCode> {
    // card 22c: the resolution now also reads `authorization_epoch` — the task's current
    // authorization generation. A grant issued under an older epoch is dead the moment the
    // task's lifecycle bumps it, and `AuthenticatedTask` is the ONLY carrier of that number
    // into `authorize_task_item`, so "which generation is current" cannot come from the wire.
    let epoch: Option<i64> = sqlx::query_scalar(
        "SELECT authorization_epoch FROM coord.tasks WHERE task_id = $1 AND tenant_id = $2",
    )
    .bind(task.0)
    .bind(tenant_id)
    .fetch_optional(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    let epoch = epoch.ok_or(ErrorCode::NotFound)?;
    Ok(AuthenticatedTask::from_resolved_task(
        tenant_id, task.0, epoch,
    ))
}

/// 一个 selector 今天跑不跑得起来。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectorAvailability {
    /// 哪个 selector。
    pub id: SelectorId,
    /// `None` = 可用；`Some(name)` = **探测出来的**缺失对象名。
    pub missing_object: Option<String>,
}

/// 一个 selector 的 `required_columns` 在**本事务的快照里**缺了什么。`None` = 齐全。
///
/// 探测走 `pg_attribute` 而不是 `information_schema.columns`：后者答不出
/// `attgenerated`。§25.4.A(11)（card 22b）要求 facet 那一列的契约是「存在**且**是
/// `GENERATED ALWAYS ... STORED`」——一个同名的可写列意味着 facet 有了独立写入口，
/// 那是 §25.4.A(3) 禁止的东西，必须探测成缺失而不是被当作可用。
///
/// 缺失对象名逐字是 `schema.table.column`，生成列契约不成立时后缀 `(not stored-generated)`
/// ——两者都是**探测出来的**，不是写死的判断（ADR-0006）。
async fn probe_required_columns(
    txn: &mut Txn<'_>,
    s: &'static SelectorSpec,
) -> Result<Option<String>, ErrorCode> {
    for (schema, table, column, stored_generated) in s.required_columns {
        let found: Option<String> = sqlx::query_scalar(
            "SELECT a.attgenerated::text FROM pg_attribute a \
               JOIN pg_class c ON c.oid = a.attrelid \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
              WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3 \
                AND a.attnum > 0 AND NOT a.attisdropped",
        )
        .bind(schema)
        .bind(table)
        .bind(column)
        .fetch_optional(&mut **txn)
        .await
        .map_err(|_| ErrorCode::Internal)?;
        // 第一个缺的就报出来——逐个列全部报出来对调用方没有增量信息，
        // 补第一个的时候自然会看见第二个。
        let Some(attgenerated) = found else {
            return Ok(Some(format!("{schema}.{table}.{column}")));
        };
        if *stored_generated && attgenerated != "s" {
            return Ok(Some(format!(
                "{schema}.{table}.{column} (not stored-generated)"
            )));
        }
    }
    Ok(None)
}

/// 拿 [`humaux_domain::context::REGISTRY`] 声明的 `required_columns` 去比 `pg_attribute`。
///
/// NA 的缺失对象因此是**探测出来的**，不是写死的判断——列一落地，对应 selector 自动可用，
/// 没有人需要回来改一行代码（ADR-0006）。
///
/// # Errors
/// 库不可达时返回 [`ErrorCode::Internal`]。
pub async fn probe_selectors(pool: &RuntimeDbPool) -> Result<[SelectorAvailability; 5], ErrorCode> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;

    let mut out: Vec<SelectorAvailability> = Vec::with_capacity(5);
    for s in &humaux_domain::context::REGISTRY {
        let missing = probe_required_columns(&mut txn, s).await?;
        out.push(SelectorAvailability {
            id: s.id,
            missing_object: missing,
        });
    }

    txn.commit().await.map_err(|_| ErrorCode::Internal)?;
    // REGISTRY 是定长 5，上面逐条 push，所以这里必然成功。
    out.try_into().map_err(|_| ErrorCode::Internal)
}

/// `project_active_constraints_v1` 的 WHERE。
const PROJECT_CONSTRAINTS_WHERE: &str = "m.tenant_id = $1 \
     AND m.authority_class = 'ProjectConstraint' \
     AND m.status = 'active' \
     AND m.superseded_by IS NULL \
     AND m.archived_at IS NULL";

/// `user_confirmed_corrections_v1` 的 WHERE。
///
/// §25.4：「active UserCorrection relevant to scope **不允许**用 embedding similarity
/// 解释；必须有机械 scope/authority 规则」。这条 `EXISTS(... origin_class='UserConfirmed')`
/// 就是那个机械替代品——`UserConfirmed` 是 §10.1 里 Agent 自己造不出来的 origin。
const USER_CORRECTIONS_WHERE: &str = "m.tenant_id = $1 \
     AND m.authority_class = 'UserCorrection' \
     AND m.status = 'active' \
     AND m.superseded_by IS NULL \
     AND m.archived_at IS NULL \
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
     AND m.archived_at IS NULL \
     AND m.authority_class IN ('ProjectConstraint', 'ExplicitTaskContext') \
     AND EXISTS ( \
       SELECT 1 FROM private.context_bindings cb \
       WHERE cb.memory_id = m.memory_id \
         AND cb.tenant_id = m.tenant_id \
         AND cb.revoked_at IS NULL \
         AND cb.mode = 'MANDATORY' \
         AND EXISTS ( \
           SELECT 1 FROM unnest($2::text[], $3::uuid[]) AS request_scope(kind, id) \
           WHERE cb.scope_kind = request_scope.kind \
             AND COALESCE(cb.scope_id, cb.tenant_id) = request_scope.id \
         ) \
     )";

/// `task_explicit_context_v2` 的**提名**（绑定义务）枚举 + 授权/目标事实（card 22c, ADR-0046）。
///
/// 裁决 §六.2 逐字点名的形状：`WITH nominated AS MATERIALIZED (...) ... LEFT JOIN`。
///
/// 是函数而不是 `const`：两个共享表达式（内容摘要、token 估算）必须与别处**同一个**常量，
/// 而 `concat!` 只吃字面量。每次请求一次 `format!`，代价可忽略；两处各抄一遍的代价不可忽略。
///
/// **不许**在这条 SQL 里出现 `INNER JOIN grants`、`WHERE g.revoked_at IS NULL`、
/// `WHERE m.status='ACTIVE'` 或任何 authority 过滤。它们都会把一条**仍然成立的义务**从诊断
/// 输入里删掉，于是「有 3 条必带项，其中 2 条没有授权」会读成「没有任何必带项」——这正是
/// 裁决 §六.3 禁止的「用 admitted 当分母」。准入判定全部在
/// [`humaux_domain::context::authorize_task_item`]，SQL 只负责把事实端上来。
///
/// `MATERIALIZED` 不是性能提示：它阻止 planner 把外层的过滤条件下推进提名子查询，
/// 也就是阻止优化器**重新**做掉我们刚刚拒绝做的那件事。
fn task_nominated_sql() -> String {
    format!(
        "WITH nominated AS MATERIALIZED ( \
           SELECT cb.context_binding_id, cb.tenant_id, cb.scope_kind, cb.scope_id, cb.mode, \
                  cb.memory_id, (cb.revoked_at IS NOT NULL) AS binding_revoked \
             FROM private.context_bindings cb \
            WHERE cb.tenant_id = $1 AND cb.scope_kind = 'TASK' AND cb.scope_id = $2 \
              AND cb.mode = 'MANDATORY' AND cb.revoked_at IS NULL \
         ) \
         SELECT n.context_binding_id, n.tenant_id, n.scope_kind, n.scope_id, n.mode, n.memory_id, \
                n.binding_revoked, \
                g.tenant_id AS g_tenant_id, g.task_id AS g_task_id, g.memory_id AS g_memory_id, \
                g.scope_kind AS g_scope_kind, g.mode AS g_mode, g.task_epoch AS g_task_epoch, \
                g.payload_sha256 AS g_payload_sha256, g.grant_authority AS g_grant_authority, \
                g.purpose AS g_purpose, g.issuer_kind AS g_issuer_kind, \
                g.policy_version AS g_policy_version, \
                (g.revoked_at IS NOT NULL) AS g_revoked, \
                EXTRACT(EPOCH FROM g.issued_at)::int8 AS g_issued_at_s, \
                EXTRACT(EPOCH FROM g.expires_at)::int8 AS g_expires_at_s, \
                EXISTS (SELECT 1 FROM private.evidence_objects eo \
                         WHERE eo.tenant_id = g.tenant_id \
                           AND eo.evidence_id = g.authorization_evidence_id) \
                  AS g_evidence_present, \
                m.memory_id AS m_memory_id, m.tenant_id AS m_tenant_id, \
                m.authority_class AS m_authority_class, \
                (m.status = 'active' AND m.superseded_by IS NULL AND m.archived_at IS NULL) \
                  AS m_active, \
                {CANONICAL_PAYLOAD_SHA256_EXPR} AS m_payload_sha256, \
                {EST_TOKENS_EXPR} AS m_est_tokens, \
                COALESCE(EXISTS(SELECT 1 FROM private.memory_evidence me \
                       WHERE me.memory_id = m.memory_id AND me.grounding_mode = 'LIVE'), false) \
                  AS has_live, \
                COALESCE(EXISTS(SELECT 1 FROM private.memory_evidence me \
                       WHERE me.memory_id = m.memory_id AND me.grounding_mode = 'LIVE' \
                         AND me.recorded_version IS NULL), false) AS has_live_unversioned, \
                EXTRACT(EPOCH FROM statement_timestamp())::int8 AS now_s \
           FROM nominated n \
           LEFT JOIN private.task_binding_grants g \
                  ON g.tenant_id = n.tenant_id AND g.context_binding_id = n.context_binding_id \
           LEFT JOIN private.memory_records m \
                  ON m.tenant_id = n.tenant_id AND m.memory_id = n.memory_id \
          ORDER BY n.context_binding_id"
    )
}

/// 被批准的**那一份**内容的规范化摘要，全仓唯一一处定义。
///
/// `jsonb::text` 是 PostgreSQL 归一化过的形态（键序、空白都已规范），所以同一份内容在写侧
/// 与读侧算出同一个摘要；`sha256` / `convert_to` 都是核心 PostgreSQL，不需要 pgcrypto。
/// 写侧（`insert_task_binding_grant_in_txn`）与读侧（[`TASK_NOMINATED_SQL`]）共用这一个常量
/// ——两处各写一遍的那天，「精确内容批准」会在某一次改写里静默失效。
pub(crate) const CANONICAL_PAYLOAD_SHA256_EXPR: &str =
    "sha256(convert_to(m.content::text, 'UTF8'))";

/// `required_current_state_facets_v1` 的 WHERE（§25.4.A(1)/(2)/(4)）。
///
/// `$2` 是 [`MandatoryContextFacet::ALL`] 的线值数组——facet 名字只有枚举序列化一处，
/// SQL 不写字面量。`min_authority` 是 `PrivateKnowledge`，所以 SQL 侧只排除
/// `PublicKnowledge`；scope 由 `readable_memory_ids` 的 `can_read` 判（与
/// `project_active_constraints_v1` 同一条依据）。
const REQUIRED_FACETS_WHERE: &str = "m.tenant_id = $1 \
     AND m.facet = ANY($2::text[]) \
     AND m.authority_class <> 'PublicKnowledge' \
     AND m.status = 'active' \
     AND m.superseded_by IS NULL \
     AND m.archived_at IS NULL";

/// 每行的估计 token 数。
///
// ponytail: 用 content 的字节长度除以 4 作粗估——真 tokenizer 是 §63 的交付物，
// 引进来只为了填这个数不值当。预算判定对量级敏感、对精度不敏感（§25.5 判的是
// 「超没超硬上限」不是「差几个 token」）；真 tokenizer 落地后换掉这一处即可。
const EST_TOKENS_EXPR: &str = "GREATEST(1, (octet_length(m.content::text) / 4))::int4";

/// Converts the database's closed visibility wire values into the actual descriptor on that
/// row. The visibility decision itself remains [`can_read`]'s single implementation.
pub(crate) fn visibility_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<VisibilityDescriptor, ErrorCode> {
    let class: String = row
        .try_get("visibility_class")
        .map_err(|_| ErrorCode::Internal)?;
    let class = match class.as_str() {
        "USER_PRIVATE" => VisibilityClass::UserPrivate,
        "WORKSPACE_SHARED" => VisibilityClass::WorkspaceShared,
        "TENANT_SHARED" => VisibilityClass::TenantShared,
        _ => return Err(ErrorCode::Internal),
    };
    let user_id = row
        .try_get::<Option<Uuid>, _>("visibility_user_id")
        .map_err(|_| ErrorCode::Internal)?
        .map(UserId);
    let workspace_id = row
        .try_get::<Option<Uuid>, _>("visibility_workspace_id")
        .map_err(|_| ErrorCode::Internal)?
        .map(WorkspaceId);
    Ok(VisibilityDescriptor {
        class,
        user_id,
        workspace_id,
    })
}

fn scope_chain_params(scope: &Scope) -> (Vec<String>, Vec<Uuid>) {
    humaux_domain::context::scope_chain(scope)
        .into_iter()
        .map(|(kind, id)| (kind.wire().to_owned(), id))
        .unzip()
}

/// Runs `can_read` against real Memory and backing Evidence rows. A Memory without a backing
/// Evidence row, or with any Evidence row hidden by RLS, fails closed (§6.1.1/§8.6).
pub(crate) async fn readable_memory_ids(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    candidate_ids: &[Uuid],
) -> Result<HashSet<Uuid>, ErrorCode> {
    if candidate_ids.is_empty() {
        return Ok(HashSet::new());
    }
    let memories = sqlx::query(
        "SELECT memory_id, visibility_class, visibility_user_id, visibility_workspace_id \
         FROM private.memory_records WHERE tenant_id = $1 AND memory_id = ANY($2)",
    )
    .bind(authorization.tenant_id().0)
    .bind(candidate_ids)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    let mut memory_visibility = HashMap::with_capacity(memories.len());
    for row in memories {
        let memory_id = row.try_get("memory_id").map_err(|_| ErrorCode::Internal)?;
        memory_visibility.insert(memory_id, visibility_from_row(&row)?);
    }

    // `memory_evidence` remains visible through its Memory RLS policy; the LEFT JOIN exposes
    // a hidden Evidence row as NULL, so a tenant-visible Memory cannot smuggle a private source
    // into Context merely because the Evidence table's RLS omitted it from the join.
    let evidence = sqlx::query(
        "SELECT me.memory_id, eo.evidence_id AS visible_evidence_id, eo.visibility_class, \
                eo.visibility_user_id, eo.visibility_workspace_id \
         FROM private.memory_evidence me \
         LEFT JOIN private.evidence_objects eo \
           ON eo.evidence_id = me.evidence_id AND eo.tenant_id = $1 \
         WHERE me.memory_id = ANY($2)",
    )
    .bind(authorization.tenant_id().0)
    .bind(candidate_ids)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    let mut evidence_by_memory: HashMap<Uuid, Vec<Option<VisibilityDescriptor>>> = HashMap::new();
    for row in evidence {
        let memory_id = row.try_get("memory_id").map_err(|_| ErrorCode::Internal)?;
        let visible: Option<Uuid> = row
            .try_get("visible_evidence_id")
            .map_err(|_| ErrorCode::Internal)?;
        let descriptor = visible.map(|_| visibility_from_row(&row)).transpose()?;
        evidence_by_memory
            .entry(memory_id)
            .or_default()
            .push(descriptor);
    }

    Ok(memory_visibility
        .into_iter()
        .filter_map(|(memory_id, descriptor)| {
            let evidence = evidence_by_memory.get(&memory_id)?;
            (can_read(authorization, &descriptor)
                && !evidence.is_empty()
                && evidence.iter().all(|descriptor| {
                    descriptor.is_some_and(|value| can_read(authorization, &value))
                }))
            .then_some(memory_id)
        })
        .collect())
}

/// 每个 selector 的两条 SQL：候选（提名 / 义务）枚举与取行。
///
/// 四个 `StoredAtLeast` selector 两者同一条谓词。`task_explicit_context_v2` **不在这里**：
/// 它的准入对象不是行上的 authority，而是一条验过的任务授权，取数形状因此完全不同
/// （[`TASK_NOMINATED_SQL`] + [`authorize_task_item`]）——把它塞进这个 `(候选, 取行)` 二元组
/// 就等于宣称「提名和准入只差一个 WHERE」，而那正是 v1 的形状。card 22 之前这里还有一条
/// `other =>` 兜底臂，
/// 把「有列但没写谓词」的 selector 判成 `Unavailable`。两个缺谓词的 selector 现在都有了
/// 谓词，match 是**闭集穷举**：再加 selector 编译器会逼这里跟着改，兜底臂反而会把那次遗漏
/// 变成一条安静的 NA，所以删掉。
const fn selector_sql(id: SelectorId) -> (&'static str, &'static str) {
    match id {
        // 不可达：`run_selector` 在分派到这里之前已经把 task selector 交给
        // `run_task_explicit_selector`。留一条会 panic 的臂而不是一条“合理”的默认谓词——
        // 一条能跑的默认谓词就是一条悄悄按存储权威准入的后门。
        SelectorId::TaskExplicitContextV1 => {
            panic!("task_explicit_context_v2 不走 selector_sql，见 run_task_explicit_selector")
        }
        SelectorId::ProjectActiveConstraintsV1 => {
            (PROJECT_CONSTRAINTS_WHERE, PROJECT_CONSTRAINTS_WHERE)
        }
        SelectorId::UserConfirmedCorrectionsV1 => (USER_CORRECTIONS_WHERE, USER_CORRECTIONS_WHERE),
        SelectorId::RequiredCurrentStateFacetsV1 => (REQUIRED_FACETS_WHERE, REQUIRED_FACETS_WHERE),
        SelectorId::ExplicitMandatoryBindingsV1 => {
            (EXPLICIT_BINDINGS_WHERE, EXPLICIT_BINDINGS_WHERE)
        }
    }
}

/// 发一条 selector SQL，按 selector 绑它自己的参数。`$1` 恒为 tenant；其余按 id。
///
/// 这是参数**唯一**被拼进去的地方——候选枚举与取行共用它，所以「两次枚举用的是同一组
/// 参数」不靠两处代码各自守纪律。
async fn fetch_selector_rows(
    txn: &mut Txn<'_>,
    sql: &str,
    id: SelectorId,
    scope: &Scope,
) -> Result<Vec<sqlx::postgres::PgRow>, ErrorCode> {
    let query = sqlx::query(sql).bind(scope.tenant_id.0);
    let query = match id {
        SelectorId::ExplicitMandatoryBindingsV1 => {
            let (kinds, ids) = scope_chain_params(scope);
            query.bind(kinds).bind(ids)
        }
        // card 22c: unreachable for the same reason `selector_sql` is — the task selector
        // never reaches this generic path.
        SelectorId::TaskExplicitContextV1 => {
            return Err(ErrorCode::Internal);
        }
        // §25.4.A(1): the facet wire values come from the closed enum's serialization —
        // `MandatoryContextFacet::ALL`, never a literal list in SQL.
        SelectorId::RequiredCurrentStateFacetsV1 => query.bind(
            MandatoryContextFacet::ALL
                .iter()
                .map(|facet| facet.wire().to_owned())
                .collect::<Vec<String>>(),
        ),
        SelectorId::ProjectActiveConstraintsV1 | SelectorId::UserConfirmedCorrectionsV1 => query,
    };
    query
        .fetch_all(&mut **txn)
        .await
        .map_err(|_| ErrorCode::Internal)
}

async fn selector_candidate_ids(
    txn: &mut Txn<'_>,
    s: &'static SelectorSpec,
    where_clause: &str,
    scope: &Scope,
) -> Result<Vec<Uuid>, ErrorCode> {
    let sql = format!("SELECT m.memory_id FROM private.memory_records m WHERE {where_clause}");
    let rows = fetch_selector_rows(txn, &sql, s.id, scope).await?;
    rows.into_iter()
        .map(|row| row.try_get("memory_id").map_err(|_| ErrorCode::Internal))
        .collect()
}

/// Runs one selector's independent candidate enumeration and row projection. The former
/// remains the expected-count oracle; both are filtered by the single Rust `can_read` policy.
async fn run_selector(
    txn: &mut Txn<'_>,
    s: &'static SelectorSpec,
    authorization: &AuthorizationScope,
    scope: &Scope,
) -> Result<SelectorOutcome, ErrorCode> {
    let (candidates_where, where_clause) = selector_sql(s.id);
    let expected_candidates = selector_candidate_ids(txn, s, candidates_where, scope).await?;
    let expected_ids = readable_memory_ids(txn, authorization, &expected_candidates).await?;

    // ② 取行——每行带两个快照内 grounding 事实（有没有 LIVE edge / 有没有未记版本的
    //    LIVE edge），喂 `classify_in_snapshot`。resolver 永不进本事务。
    let sql = format!(
        "SELECT m.memory_id, m.authority_class, {EST_TOKENS_EXPR} AS est_tokens, \
                EXISTS(SELECT 1 FROM private.memory_evidence me \
                       WHERE me.memory_id = m.memory_id AND me.grounding_mode = 'LIVE') \
                  AS has_live, \
                EXISTS(SELECT 1 FROM private.memory_evidence me \
                       WHERE me.memory_id = m.memory_id AND me.grounding_mode = 'LIVE' \
                         AND me.recorded_version IS NULL) \
                  AS has_live_unversioned \
         FROM private.memory_records m WHERE {where_clause} ORDER BY m.memory_id"
    );
    let rows = fetch_selector_rows(txn, &sql, s.id, scope).await?;

    let mut out = Vec::with_capacity(rows.len());
    let mut needs = Vec::new();
    for r in rows {
        let memory_id: Uuid = r.try_get("memory_id").map_err(|_| ErrorCode::Internal)?;
        if !expected_ids.contains(&memory_id) {
            continue;
        }
        let authority: String = r
            .try_get("authority_class")
            .map_err(|_| ErrorCode::Internal)?;
        let est_tokens: i32 = r.try_get("est_tokens").map_err(|_| ErrorCode::Internal)?;
        let has_live: bool = r.try_get("has_live").map_err(|_| ErrorCode::Internal)?;
        let has_live_unversioned: bool = r
            .try_get("has_live_unversioned")
            .map_err(|_| ErrorCode::Internal)?;
        let authority = parse_authority(&authority)?;

        // 两个 bool → 最小 SnapshotEdge 集：分类规则住在 domain，这里只是投影出它要的事实。
        let grounding = grounding_from_facts(has_live, has_live_unversioned);

        // `from_selector` 按 spec 复核 authority 下限并跑 DOD-093 铸造门——SQL 侧过滤与
        // domain 侧下限是两道独立的门；grounding 分流在 domain，不在 SQL。
        match MandatoryRow::from_selector(
            s,
            MemoryId(memory_id),
            authority,
            u32::try_from(est_tokens).unwrap_or(u32::MAX),
            grounding,
        )
        .map_err(|_| ErrorCode::Internal)?
        {
            Admitted::Row(row) => out.push(row),
            Admitted::NeedsVerification(nv) => needs.push(nv),
        }
    }

    Ok(SelectorOutcome::Ran {
        id: s.id,
        candidate_ids: expected_ids.into_iter().map(MemoryId).collect(),
        rows: out,
        needs_verification: needs,
    })
}

/// 一条提名义务的**报告行**：它是不是被准入，没被准入的话原因是什么（脱敏）。
///
/// 裁决 §六：`rejected = N − A`，逐条具名。这不是 `SelectorOutcome` 的一个新字段——那个
/// 枚举是 `crates/retrieval` 也在构造的闭集，本卡不改它；提名集本身已经在
/// `SelectorOutcome::Ran::candidate_ids` 里诚实地全量保留（**不**按可读性过滤），
/// 本结构是它旁边那份「为什么」的读数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskObligationReport {
    /// 哪条绑定义务。
    pub context_binding_id: Uuid,
    /// 它指向的 memory。
    pub memory_id: Uuid,
    /// 准入了吗。
    pub admitted: bool,
    /// 没准入的原因（脱敏、不含内容）。`None` ⇔ `admitted`。
    pub reject: Option<TaskContextReject>,
}

/// `task_explicit_context_v2` 的一次运行：提名集 + 准入集 + 逐条理由。
struct TaskSelectorRun {
    outcome: SelectorOutcome,
    reports: Vec<TaskObligationReport>,
}

/// `private.memory_records.content` 的 origin basis 允许的最高 disposition，逐 memory。
///
/// 存在性读法（与 §10.1 rule 1 的 ceiling 一致、与 `AuthorityPolicy::authorize` 逐字同款）：
/// basis 里**有任一** origin 允许 `BehaviorEligible` 才算 `BehaviorEligible`。origin → disposition
/// 的表只有 `EvidenceOriginClass::max_disposition` 一处，SQL 里不复制它。
///
/// 没有任何可读 evidence 的 memory 得到 `DataOnly`（失败关闭）——没有 basis 就没有行为资格。
async fn max_dispositions_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    memory_ids: &[Uuid],
) -> Result<HashMap<Uuid, humaux_domain::evidence::InstructionDisposition>, ErrorCode> {
    use humaux_domain::evidence::InstructionDisposition;
    let mut out: HashMap<Uuid, InstructionDisposition> = memory_ids
        .iter()
        .map(|id| (*id, InstructionDisposition::DataOnly))
        .collect();
    if memory_ids.is_empty() {
        return Ok(out);
    }
    let rows = sqlx::query(
        "SELECT me.memory_id, eo.origin_class FROM private.memory_evidence me \
           JOIN private.evidence_objects eo \
             ON eo.evidence_id = me.evidence_id AND eo.tenant_id = $1 \
          WHERE me.memory_id = ANY($2)",
    )
    .bind(tenant_id)
    .bind(memory_ids)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    for row in rows {
        let memory_id: Uuid = row.try_get("memory_id").map_err(|_| ErrorCode::Internal)?;
        let origin: String = row
            .try_get("origin_class")
            .map_err(|_| ErrorCode::Internal)?;
        let origin =
            crate::distill_repo::origin_class_from_db_str(&origin).ok_or(ErrorCode::Internal)?;
        if matches!(
            origin.max_disposition(),
            InstructionDisposition::BehaviorEligible
        ) && let Some(slot) = out.get_mut(&memory_id)
        {
            *slot = InstructionDisposition::BehaviorEligible;
        }
    }
    Ok(out)
}

/// `bytea` → `[u8; 32]`。长度不对是 `Internal`，不是补零——一个 31 字节的摘要不是摘要。
fn digest32(raw: &[u8]) -> Result<[u8; 32], ErrorCode> {
    <[u8; 32]>::try_from(raw).map_err(|_| ErrorCode::Internal)
}

/// `task_explicit_context_v2` 的唯一运行点（card 22c, ADR-0046）。
///
/// 顺序：提名（绑定义务，无过滤）→ 目标可读性 → origin disposition → 逐条
/// [`authorize_task_item`] → 准入的行经 [`MandatoryRow::from_task_grant`] 过 DOD-093 铸造门。
///
/// `candidate_ids` 是**提名集**（N），不是可读集：一条目标不可读的义务仍然是一条义务，
/// 它必须留在分母里（裁决 §六.3）。这跟其余四个 selector 不同，理由写在这里而不是靠读者
/// 自己发现：那四个的「候选」本来就是「按谓词枚举到的可读行」，而这一个的候选是**义务**。
async fn run_task_explicit_selector(
    txn: &mut Txn<'_>,
    s: &'static SelectorSpec,
    authorization: &AuthorizationScope,
    scope: &Scope,
) -> Result<TaskSelectorRun, ErrorCode> {
    let empty = |id| TaskSelectorRun {
        outcome: SelectorOutcome::Ran {
            id,
            candidate_ids: Vec::new(),
            rows: Vec::new(),
            needs_verification: Vec::new(),
        },
        reports: Vec::new(),
    };
    // §25.4.A(7): no authenticated task in this request ⇒ no TASK obligation can be inherited.
    let Some(task_id) = scope.task_id else {
        return Ok(empty(s.id));
    };
    // The task must resolve in THIS transaction; a wire uuid naming nothing is not a task.
    // `resolve_task_in_txn` is also the only producer of the epoch admission compares against.
    let task = resolve_task_in_txn(txn, scope.tenant_id.0, task_id).await?;

    let rows = sqlx::query(&task_nominated_sql())
        .bind(scope.tenant_id.0)
        .bind(task_id.0)
        .fetch_all(&mut **txn)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    if rows.is_empty() {
        return Ok(empty(s.id));
    }

    let nominated_ids: Vec<Uuid> = rows
        .iter()
        .map(|r| r.try_get("memory_id").map_err(|_| ErrorCode::Internal))
        .collect::<Result<Vec<Uuid>, _>>()?;
    let readable = readable_memory_ids(txn, authorization, &nominated_ids).await?;
    let dispositions = max_dispositions_in_txn(txn, scope.tenant_id.0, &nominated_ids).await?;
    let stored_floor = s.authority.stored_floor();

    let mut admitted_rows = Vec::new();
    let mut needs = Vec::new();
    let mut reports = Vec::with_capacity(rows.len());
    let mut candidate_ids: Vec<MemoryId> = Vec::with_capacity(rows.len());

    for r in rows {
        let facts = task_grant_facts_from_row(&r, &readable, &dispositions)?;
        let context_binding_id: Uuid = r
            .try_get("context_binding_id")
            .map_err(|_| ErrorCode::Internal)?;
        let memory_id = facts.obligation.memory_id;
        candidate_ids.push(memory_id);

        match authorize_task_item(&task, stored_floor, &facts) {
            Err(reject) => reports.push(TaskObligationReport {
                context_binding_id,
                memory_id: memory_id.0,
                admitted: false,
                reject: Some(reject),
            }),
            Ok(item) => {
                let est_tokens: i32 = r.try_get("m_est_tokens").map_err(|_| ErrorCode::Internal)?;
                let has_live: bool = r.try_get("has_live").map_err(|_| ErrorCode::Internal)?;
                let has_live_unversioned: bool = r
                    .try_get("has_live_unversioned")
                    .map_err(|_| ErrorCode::Internal)?;
                let grounding = grounding_from_facts(has_live, has_live_unversioned);
                match MandatoryRow::from_task_grant(
                    s,
                    item,
                    u32::try_from(est_tokens).unwrap_or(u32::MAX),
                    grounding,
                )
                .map_err(|_| ErrorCode::Internal)?
                {
                    Admitted::Row(row) => {
                        admitted_rows.push(row);
                        reports.push(TaskObligationReport {
                            context_binding_id,
                            memory_id: memory_id.0,
                            admitted: true,
                            reject: None,
                        });
                    }
                    Admitted::NeedsVerification(nv) => {
                        needs.push(nv);
                        reports.push(TaskObligationReport {
                            context_binding_id,
                            memory_id: memory_id.0,
                            admitted: false,
                            reject: Some(TaskContextReject::TargetNotActiveOrGrounded),
                        });
                    }
                }
            }
        }
    }
    candidate_ids.sort_by_key(|id| id.0);
    candidate_ids.dedup();

    Ok(TaskSelectorRun {
        outcome: SelectorOutcome::Ran {
            id: s.id,
            candidate_ids,
            rows: admitted_rows,
            needs_verification: needs,
        },
        reports,
    })
}

/// 一行 [`task_nominated_sql`] → [`TaskGrantFacts`]。**纯投影**，没有任何判定：判定全部在
/// `authorize_task_item`。这里唯一的"决定"是把 NULL 读成 `None` 而不是读成默认值。
// 长度来自字段数（一条 grant 要绑的东西有十几样，裁决 §二.1 逐条点名），不是来自分支。
// 拆成几个"取一半字段"的函数不会让它更短，只会让"这一列读成了什么"散到两处。
#[allow(clippy::too_many_lines)]
fn task_grant_facts_from_row(
    r: &sqlx::postgres::PgRow,
    readable: &HashSet<Uuid>,
    dispositions: &HashMap<Uuid, humaux_domain::evidence::InstructionDisposition>,
) -> Result<TaskGrantFacts, ErrorCode> {
    use humaux_domain::evidence::InstructionDisposition;
    let get = |name: &str| -> Result<Uuid, ErrorCode> {
        r.try_get(name).map_err(|_| ErrorCode::Internal)
    };
    let memory_id = MemoryId(get("memory_id")?);
    let scope_kind: String = r.try_get("scope_kind").map_err(|_| ErrorCode::Internal)?;
    let mode: String = r.try_get("mode").map_err(|_| ErrorCode::Internal)?;
    let obligation = TaskBindingObligation {
        context_binding_id: get("context_binding_id")?,
        tenant_id: get("tenant_id")?,
        // 线值认不出来 ⇒ 落到一个**不会**通过 `authorize_task_item` 第 2 道门的档位，
        // 而不是静默当成 TASK/MANDATORY。
        scope_kind: if scope_kind == "TASK" {
            ScopeKind::Task
        } else {
            ScopeKind::Tenant
        },
        scope_id: r
            .try_get::<Option<Uuid>, _>("scope_id")
            .map_err(|_| ErrorCode::Internal)?,
        mode: if mode == "MANDATORY" {
            BindingMode::Mandatory
        } else {
            BindingMode::Supplemental
        },
        memory_id,
        revoked: r
            .try_get("binding_revoked")
            .map_err(|_| ErrorCode::Internal)?,
    };

    let grant_tenant: Option<Uuid> = r.try_get("g_tenant_id").map_err(|_| ErrorCode::Internal)?;
    let grant = match grant_tenant {
        None => None,
        Some(tenant_id) => {
            let purpose: String = r.try_get("g_purpose").map_err(|_| ErrorCode::Internal)?;
            let issuer: String = r
                .try_get("g_issuer_kind")
                .map_err(|_| ErrorCode::Internal)?;
            let policy_version: String = r
                .try_get("g_policy_version")
                .map_err(|_| ErrorCode::Internal)?;
            let g_scope_kind: String =
                r.try_get("g_scope_kind").map_err(|_| ErrorCode::Internal)?;
            let g_mode: String = r.try_get("g_mode").map_err(|_| ErrorCode::Internal)?;
            let payload: Vec<u8> = r
                .try_get("g_payload_sha256")
                .map_err(|_| ErrorCode::Internal)?;
            Some(RawTaskGrant {
                tenant_id,
                context_binding_id: get("context_binding_id")?,
                task_id: get("g_task_id")?,
                memory_id: MemoryId(get("g_memory_id")?),
                scope_kind_is_task: g_scope_kind == "TASK",
                mode_is_mandatory: g_mode == "MANDATORY",
                task_epoch: r.try_get("g_task_epoch").map_err(|_| ErrorCode::Internal)?,
                payload_sha256: digest32(&payload)?,
                grant_authority: r
                    .try_get("g_grant_authority")
                    .map_err(|_| ErrorCode::Internal)?,
                purpose: BindingPurpose::parse_wire(&purpose),
                issuer_kind: TaskGrantIssuer::parse_wire(&issuer),
                policy_version_matches: policy_version == TASK_AUTHORIZATION_POLICY_VERSION,
                authorization_evidence_present: r
                    .try_get("g_evidence_present")
                    .map_err(|_| ErrorCode::Internal)?,
                issued_at_epoch_s: r
                    .try_get("g_issued_at_s")
                    .map_err(|_| ErrorCode::Internal)?,
                expires_at_epoch_s: r
                    .try_get("g_expires_at_s")
                    .map_err(|_| ErrorCode::Internal)?,
                revoked: r.try_get("g_revoked").map_err(|_| ErrorCode::Internal)?,
            })
        }
    };

    let target_tenant: Option<Uuid> = r.try_get("m_tenant_id").map_err(|_| ErrorCode::Internal)?;
    let target = match target_tenant {
        None => None,
        Some(tenant_id) => {
            let authority: String = r
                .try_get("m_authority_class")
                .map_err(|_| ErrorCode::Internal)?;
            let payload: Vec<u8> = r
                .try_get("m_payload_sha256")
                .map_err(|_| ErrorCode::Internal)?;
            Some(TaskTargetFacts {
                tenant_id,
                memory_id,
                stored_authority: parse_authority(&authority)?,
                max_disposition: dispositions
                    .get(&memory_id.0)
                    .copied()
                    .unwrap_or(InstructionDisposition::DataOnly),
                active: r.try_get("m_active").map_err(|_| ErrorCode::Internal)?,
                readable: readable.contains(&memory_id.0),
                payload_sha256: digest32(&payload)?,
            })
        }
    };

    Ok(TaskGrantFacts {
        obligation,
        grant,
        target,
        now_epoch_s: r.try_get("now_s").map_err(|_| ErrorCode::Internal)?,
    })
}

/// `task_explicit_context_v2` 的逐条义务报告（提名、准入、脱敏原因），在它自己的
/// REPEATABLE READ 事务里。装配路径与本函数共用 [`run_task_explicit_selector`]，
/// 所以「报告说的」和「lane 做的」不会是两套逻辑。
///
/// # Errors
/// 库不可达、scope 越权、任务解析不到 ⇒ [`ErrorCode::Internal`] / `Forbidden` / `NotFound`。
pub async fn task_obligation_report(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    requested_scope: &Scope,
) -> Result<Vec<TaskObligationReport>, ErrorCode> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *txn)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    let (authorization, scope) = canonical_scope(authorization, requested_scope)?;
    set_authorization_local(&mut txn, &authorization).await?;
    let run = run_task_explicit_selector(
        &mut txn,
        spec(SelectorId::TaskExplicitContextV1),
        &authorization,
        &scope,
    )
    .await?;
    txn.commit().await.map_err(|_| ErrorCode::Internal)?;
    Ok(run.reports)
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

/// 两个快照内事实 → [`RowGrounding`]。分类规则在 [`classify_in_snapshot`]（domain），
/// 这里只投影出它要的最小 [`SnapshotEdge`] 集——不是第二份判据。
fn grounding_from_facts(has_live: bool, has_live_unversioned: bool) -> RowGrounding {
    let mut edges = Vec::with_capacity(2);
    if has_live_unversioned {
        edges.push(SnapshotEdge {
            mode: GroundingMode::Live,
            recorded_version_present: false,
        });
    } else if has_live {
        edges.push(SnapshotEdge {
            mode: GroundingMode::Live,
            recorded_version_present: true,
        });
    }
    classify_in_snapshot(&edges)
}

/// §25.4 的五个 selector，**合并之前**的逐个结果：probe（同快照）→ 可用者跑自己的候选
/// 枚举 + 取行，不可用者具名 `Unavailable`。
///
/// 单独抽出来不是为了复用，是为了可断言：`MandatoryLane` 按 Memory 身份去重，所以
/// 「Task(T) 恰好是 {S}」「Facets(W) 恰好是 {S,D,State-U}」在合并后的 lane 里**读不出来**
/// ——同一条 S 被两个 selector 提名时只剩一行。裁决 §五.3 要的正是合并前的精确集合：
/// 只看最终并集时，Facets 的 STATE 分支坏掉仍会被 Task lane 把 S 补回去，那是假绿。
async fn run_selectors_in_txn(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    scope: &Scope,
) -> Result<[SelectorOutcome; 5], ErrorCode> {
    // §25.4.A(7): a TaskId on the read path is resolved BEFORE any selector runs — an
    // unresolvable task must not read back as "this task has no mandatory context".
    if let Some(task) = scope.task_id {
        resolve_task_in_txn(txn, scope.tenant_id.0, task).await?;
    }
    // probe（同快照）。DDL 探测滞后于快照是接受语义——本次装配看到的世界就是这个快照的世界。
    let mut availability: Vec<(SelectorId, Option<String>)> = Vec::with_capacity(5);
    for sp in &humaux_domain::context::REGISTRY {
        availability.push((sp.id, probe_required_columns(txn, sp).await?));
    }

    let mut out: Vec<SelectorOutcome> = Vec::with_capacity(5);
    for (id, missing) in availability {
        if let Some(missing_object) = missing {
            out.push(SelectorOutcome::Unavailable { id, missing_object });
            continue;
        }
        let sp = spec(id);
        // card 22c: the requirement decides the取数 shape, not the id — so a future selector
        // that also admits by a verified authorization cannot forget to take this branch.
        out.push(match sp.authority {
            AuthorityRequirement::VerifiedCurrentTaskBinding => {
                run_task_explicit_selector(txn, sp, authorization, scope)
                    .await?
                    .outcome
            }
            AuthorityRequirement::StoredAtLeast(_) => {
                run_selector(txn, sp, authorization, scope).await?
            }
        });
    }
    out.try_into().map_err(|_| ErrorCode::Internal)
}

/// [`run_selectors_in_txn`] on its own REPEATABLE READ transaction — the pre-merge readback
/// the card-22b live witness asserts per-selector exact id sets against.
///
/// # Errors
/// 库不可达、scope 越权、authority 线值不在闭集内 ⇒ [`ErrorCode::Internal`] / `Forbidden`。
pub async fn selector_outcomes(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    requested_scope: &Scope,
) -> Result<[SelectorOutcome; 5], ErrorCode> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *txn)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    let (authorization, scope) = canonical_scope(authorization, requested_scope)?;
    set_authorization_local(&mut txn, &authorization).await?;
    let outcomes = run_selectors_in_txn(&mut txn, &authorization, &scope).await?;
    txn.commit().await.map_err(|_| ErrorCode::Internal)?;
    Ok(outcomes)
}

/// §25.4 装配的**全部冻结读数**，单事务取齐——G80-31「同一 `context_snapshot_seq` 两次
/// 装配逐字节相同」的取数半边。
///
/// 一个 `REPEATABLE READ` 事务（`consolidate_repo` 的 §11.7 同款配方，只读路径不带
/// `READ WRITE`），依次：隔离级 → 租户上下文 → probe（`pg_attribute` 进同快照；
/// DDL 探测滞后于快照是**接受语义**——本次装配看到的世界就是这个快照的世界）→ 各
/// selector 的独立候选枚举 + 取行 → pinned 的独立候选枚举 + 取行 + excluded 具名 → 快照身份。
///
/// 此前这里是三个各自开事务的 pub 函数（probe / mandatory / pinned）——READ COMMITTED
/// 下每条语句各看各的快照，probe 与取数之间还有 TOCTOU；「两次装配逐字节相同」在那个
/// 形状下无从谈起。三函数已并入本函数（probe 保留 pub 供独立探测）。
///
/// # Errors
/// 库不可达、authority 线值不在闭集内 ⇒ [`ErrorCode::Internal`]。
pub async fn fetch_frozen(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    requested_scope: &Scope,
) -> Result<FrozenReads, ErrorCode> {
    let _ = canonical_scope(authorization, requested_scope)?;

    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    // 必须是本事务第一条语句：隔离级在第一个取快照的语句之后就改不了了。
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *txn)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    let frozen = fetch_frozen_in_txn(&mut txn, authorization, requested_scope).await?;
    txn.commit().await.map_err(|_| ErrorCode::Internal)?;
    Ok(frozen)
}

/// Reads frozen Context lanes through a caller-owned repeatable-read transaction.
///
/// The adapter reruns raw scope narrowing and installs the transaction-local
/// authorization guard before issuing its first Context query. Callers establish
/// repeatable-read mode before their first query.
pub(crate) async fn fetch_frozen_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    authorization: &AuthorizationScope,
    requested_scope: &Scope,
) -> Result<FrozenReads, ErrorCode> {
    let (authorization, scope) = canonical_scope(authorization, requested_scope)?;
    set_authorization_local(txn, &authorization).await?;
    let outcomes = run_selectors_in_txn(txn, &authorization, &scope).await?;
    let mandatory = MandatoryLane::from_selectors(outcomes)?;

    let pinned = fetch_pinned_in_txn(txn, &authorization, &scope).await?;

    // 快照身份：同一事务内取。seq（xmin）是 fingerprint 轴——必要非充分；
    // token（完整 snapshot）才是「同快照 ⇒ 同字节」的充分条件（见 FrozenReads doc）。
    let row = sqlx::query(
        "SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint AS seq, \
                pg_current_snapshot()::text AS token",
    )
    .fetch_one(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    let seq: i64 = row.try_get("seq").map_err(|_| ErrorCode::Internal)?;
    let token: String = row.try_get("token").map_err(|_| ErrorCode::Internal)?;
    let digest = Sha256::digest(token.as_bytes());
    let snapshot_token_sha256 = digest
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();

    Ok(FrozenReads {
        mandatory,
        pinned,
        context_snapshot_seq: seq,
        snapshot_token_sha256,
    })
}

/// One exact, object-level Memory read from the caller's trusted serving stream.
///
/// `bodies` carries the same RR snapshot identity as the final Memory body. `ledger` and
/// `grounding` were read in that same transaction, but this type does not claim a complete
/// stream or pipeline census.
pub struct MaterializedMemory {
    pub bodies: MaterializedBodies,
    pub ledger: humaux_retrieval::completeness::LedgerClosure,
    pub grounding: GroundingBlock,
    /// §6.1.3 (ADR-0028 D-D): `memory_id → linked subject ids` (link order) for every body in
    /// `bodies`, read in the same RR snapshot under the caller's RLS. A memory with no link has
    /// no entry.
    pub subjects: HashMap<Uuid, Vec<Uuid>>,
    /// Q3/ADR-0024 D-C: `true` when this memory carries `archived_at IS NOT NULL`. Only
    /// `memory.get` ever sees `true` — recall/context/enumerate exclude archived rows at their
    /// candidate step, so those paths always set `false`.
    pub archived: bool,
}

/// Trusted server-side pagination inputs for an authorized memory enumeration.
pub struct MemoryEnumerationParams<'a> {
    pub cursor: Option<&'a str>,
    pub page_size: u16,
    pub ttl: std::time::Duration,
    pub mac_key: &'a [u8],
    /// §6.1.3 (ADR-0028 D-D): restrict the EXACT enumeration predicate to memories linked to
    /// this subject (`private.memory_subjects`, under RLS). Part of the manifest's query
    /// fingerprint, so a cursor minted with one filter cannot page another; never a
    /// post-filter, so the page's completeness claim stays what the manifest says.
    pub subject_id: Option<Uuid>,
}

/// One immutable manifest page whose body, grounding and ledger share one PostgreSQL snapshot.
pub struct MaterializedMemoryPage {
    pub snapshot_id: Uuid,
    pub next_cursor: Option<String>,
    pub memory: MaterializedMemory,
    /// §22.1 census + §23.3④ pipeline readings for THIS page, or `None` when this call had no
    /// workspace-scoped universe to enumerate at all.
    ///
    /// The two travel together on purpose: §22.0 makes `class=exact` without an enumeration a
    /// hard 5xx, and `classify()` maps this route's frozen `PlannerDecision::Enumerate` to
    /// `Exact` as soon as the pipeline counts stop being unknown — so a caller that took the
    /// counts without the census would 5xx, and one that took the census without the counts
    /// would report `cannot_establish/count_unknown` while holding a proven total. One
    /// `Option` over both makes neither half reachable on its own.
    pub census: Option<EnumerationCensus>,
}

/// [`MaterializedMemoryPage::census`]'s payload: the §22.1 verdict for this page and the
/// `stream_ledger`-universe pipeline counts read in the same transaction.
pub struct EnumerationCensus {
    pub census: humaux_retrieval::completeness::CensusResult,
    pub pipeline: StreamPipelineCounts,
}

/// §23.3④ `pipeline.evidence` / `pipeline.knowledge` readings in the **`stream_ledger`**
/// universe — the request's full six-column [`StreamKey`] ledger, not the caller's authorized
/// view (§23.3④: "同一块的计数必须来自同一实际全集、同一授权与快照"; the authorized EXACT
/// census is a different universe and may never fill these, "禁止填 `0`、返回条数、
/// `issued_highwater` 或其他块的值充数").
///
/// Both blocks read `projection.stream_log` rows for that key. §15.1/§60 issue a stream_seq in
/// the same transaction that persists its Evidence and its `ops.outbox` row, so a row's
/// existence *is* the "Evidence fully persisted" reading; §15.2's state set is what partitions
/// the knowledge layer. The equation §23.3④ freezes (`evidence.persisted ==
/// knowledge.eligible == projection.expected`, and `processed + waiting_key + failed ==
/// eligible`) therefore discriminates on two real axes: the ledger rows against
/// `stream_checkpoints.issued_highwater` (a separately-written watermark — a hole in the dense
/// sequence, or a watermark ahead of its rows, fails it), and the knowledge partition against
/// its own base (anything still in flight fails it).
// ponytail: derived from `stream_log` at read time because nothing in the workspace advances
// `stream_checkpoints.evidence_highwater` / `knowledge_highwater` (grep: migration 0011's
// GRANT and two fixtures). Upgrade path: when the distill/projection workers start advancing
// those two watermarks, read them here instead — the two blocks then become genuinely
// independent per-layer readings rather than two statements over one ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamPipelineCounts {
    pub evidence_persisted: u64,
    pub knowledge_eligible: u64,
    pub knowledge_processed: u64,
    pub knowledge_waiting_key: u64,
    pub knowledge_failed: u64,
}

impl StreamPipelineCounts {
    /// The §23.3④ wire pair these readings are allowed to fill, minted in ONE place for all
    /// three read routes. §23.3④ freezes the scope label together with the numbers ("同一块的
    /// 计数必须来自同一实际全集、同一授权与快照"), so a route that hand-wrote the two blocks
    /// could pair a `stream_ledger` reading with an `authorized_view` label — a
    /// `count_scope_mismatch` that no type would catch. `expected` stays `null`: §23.1①, these
    /// routes carry no `batch_id`, and it is never backfilled from `persisted`.
    #[must_use]
    pub fn blocks(self) -> (EvidenceBlock, KnowledgeBlock) {
        (
            EvidenceBlock::no_batch(Some(self.evidence_persisted), CountScope::StreamLedger),
            KnowledgeBlock {
                eligible: Some(self.knowledge_eligible),
                processed: Some(self.knowledge_processed),
                waiting_key: Some(self.knowledge_waiting_key),
                failed: Some(self.knowledge_failed),
                count_scope: CountScope::StreamLedger,
            },
        )
    }
}

/// §22.1 one-predicate-two-faces for `memory.enumerate`: the FROM + tenant/workspace filter
/// (`$1` tenant, `$2` workspace) and the predicate that [`materialize_memory_enumeration`]'s
/// own candidate query and its census denominator BOTH run. One text, two faces — a second,
/// hand-written filter is what makes a `coverage` describe a set the page never had.
///
/// `visibility_workspace_id IS NULL OR = $2` is `can_read`'s own shape written in SQL:
/// `memory_records_visibility_matches_class` (migration 0004) forces that column NOT NULL
/// exactly for `WORKSPACE_SHARED` rows, so another workspace's shared row leaves the universe
/// here and `USER_PRIVATE`/`TENANT_SHARED` rows (always NULL) stay in it — then
/// [`readable_memory_ids`] applies the per-row authority (card 9/13).
///
/// Deliberately NOT a `control.retrieval_predicates` row: §20.1's registry is the entry point
/// for §20.2's *surface-pattern* path, and §20.3 obliges every row there to carry ≥5 natural
/// language questions with ≥2 near-miss negatives. This route never reaches the planner —
/// `RetrievalIntent::trusted_memory_enumerate` hands `build_request` a frozen
/// `PlannerDecision::Enumerate` whose id is `domain::selection`'s
/// `AUTHORIZED_MEMORY_ENUMERATION_V1` ("Authorized Gateway enumeration, distinct from the
/// worker's tenant-shared predicate"). Registering an NL surface for a trusted-intent
/// predicate would make it reachable from query text, which is the opposite of what it is.
const ENUMERATION_SCOPE: &str = "private.memory_records \
     WHERE memory_records.tenant_id = $1 \
       AND (memory_records.visibility_workspace_id IS NULL \
            OR memory_records.visibility_workspace_id = $2)";

/// Q3/ADR-0024 D-C: enumerate excludes archived rows, so they are outside the denominator too.
const ENUMERATION_PREDICATE: &str = "memory_records.archived_at IS NULL";

fn enumeration_fingerprint(
    authorization: &AuthorizationScope,
    scope: &Scope,
    subject_id: Option<Uuid>,
) -> String {
    let workspace = scope
        .workspace_id
        .map(|id| id.0.to_string())
        .unwrap_or_default();
    let user = authorization
        .user_id()
        .map(|id| id.0.to_string())
        .unwrap_or_default();
    // The subject filter is part of the predicate identity (empty segment when absent), so a
    // cursor minted under one filter never pages another manifest.
    let subject = subject_id.map(|id| id.to_string()).unwrap_or_default();
    query_fingerprint(
        &format!(
            "{AUTHORIZED_MEMORY_ENUMERATION_V1}:{}:{}:{}:{subject}",
            authorization.principal().0,
            user,
            workspace
        ),
        authorization.tenant_id().0,
    )
}

/// §6.1.3 (ADR-0028 D-D): linked subject ids for each of `ids`, in link order, under the
/// transaction's RLS (`private.memory_subjects` is visible exactly where its memory is).
async fn memory_subjects_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    ids: &[Uuid],
) -> Result<HashMap<Uuid, Vec<Uuid>>, ErrorCode> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = sqlx::query(
        "SELECT memory_id, subject_id FROM private.memory_subjects \
         WHERE tenant_id = $1 AND memory_id = ANY($2) \
         ORDER BY memory_id, created_at, subject_id",
    )
    .bind(tenant_id)
    .bind(ids)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let mut out: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for row in rows {
        let memory_id: Uuid = row.try_get("memory_id").map_err(|_| ErrorCode::Internal)?;
        let subject_id: Uuid = row.try_get("subject_id").map_err(|_| ErrorCode::Internal)?;
        out.entry(memory_id).or_default().push(subject_id);
    }
    Ok(out)
}

fn validate_enumeration_params(params: &MemoryEnumerationParams<'_>) -> Result<(), ErrorCode> {
    if !(1..=100).contains(&params.page_size)
        || !(1..=86_400).contains(&params.ttl.as_secs())
        || params.ttl.subsec_nanos() != 0
        || params.mac_key.is_empty()
        || params
            .cursor
            .is_some_and(|cursor| cursor.is_empty() || cursor.len() > 1024)
    {
        return Err(ErrorCode::InvalidInput);
    }
    Ok(())
}

async fn page_grounding_in_txn(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    ids: &[Uuid],
) -> Result<GroundingBlock, ErrorCode> {
    let mut result = GroundingBlock::tally([]);
    for id in ids {
        let block = direct_get_grounding_in_txn(txn, authorization, MemoryId(*id)).await?;
        result.current += block.current;
        result.recheck_required += block.recheck_required;
        result.unresolved += block.unresolved;
        result.cannot_establish += block.cannot_establish;
        result.not_judged += block.not_judged;
        result.revokes_current_truth_assumption += block.revokes_current_truth_assumption;
    }
    Ok(result)
}
/// The six-column `StreamKey` WHERE clause, `$1..$6` (same column order `stream_repo` binds).
const PIPELINE_KEY_WHERE: &str = "tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 \
     AND domain = $4 AND projection_kind = $5 AND projection_version = $6";

fn bind_pipeline_key<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    key: &'q StreamKey,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    query
        .bind(key.tenant_id.0)
        .bind(&key.scope_kind)
        .bind(key.scope_id)
        .bind(&key.domain)
        .bind(&key.projection_kind)
        .bind(&key.projection_version)
}

/// Reads [`StreamPipelineCounts`] in the caller's transaction (see that type for the §23.3④
/// derivation and its marked ceiling). Two independent statements, one per pipeline block.
pub(crate) async fn stream_pipeline_counts_in_txn(
    txn: &mut Txn<'_>,
    key: &StreamKey,
) -> Result<StreamPipelineCounts, ErrorCode> {
    let evidence_persisted: i64 = bind_pipeline_key(
        sqlx::query(&format!(
            "SELECT count(*) AS n FROM projection.stream_log WHERE {PIPELINE_KEY_WHERE}"
        )),
        key,
    )
    .fetch_one(&mut **txn)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?
    .try_get("n")
    .map_err(|_| ErrorCode::Internal)?;

    // §15.2's state set, partitioned the way §23.3④'s knowledge equation needs it: `processed`
    // is the settled set (the same states `stream_repo` counts as `done` — including the
    // audited `RETIRED_FAILED` migration 0167 adds, or a retired ticket would fall out of all
    // three buckets and silently shrink the equation's left side), `failed` is
    // the `processing_gaps` view's own set, `waiting_key` is its own §15.3 stall. ISSUED /
    // PROCESSING / RETRY_WAIT are in NONE of the three on purpose — work still in flight is
    // not processed, and reporting it as such is exactly the "填数充数" §23.3④ forbids; the
    // sum then legitimately falls short of `eligible` and the envelope says so.
    let knowledge = bind_pipeline_key(
        sqlx::query(&format!(
            "SELECT count(*) AS eligible, \
                    count(*) FILTER (WHERE state IN \
                      ('DONE','SKIPPED_BY_POLICY','TOMBSTONED','RETIRED_FAILED')) \
                      AS processed, \
                    count(*) FILTER (WHERE state = 'WAITING_KEY') AS waiting_key, \
                    count(*) FILTER (WHERE state IN ('FAILED','LOST')) AS failed \
             FROM projection.stream_log WHERE {PIPELINE_KEY_WHERE}"
        )),
        key,
    )
    .fetch_one(&mut **txn)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let read = |name: &str| -> Result<u64, ErrorCode> {
        let value: i64 = knowledge.try_get(name).map_err(|_| ErrorCode::Internal)?;
        u64::try_from(value).map_err(|_| ErrorCode::Internal)
    };
    Ok(StreamPipelineCounts {
        evidence_persisted: u64::try_from(evidence_persisted).map_err(|_| ErrorCode::Internal)?,
        knowledge_eligible: read("eligible")?,
        knowledge_processed: read("processed")?,
        knowledge_waiting_key: read("waiting_key")?,
        knowledge_failed: read("failed")?,
    })
}

/// §22.1 readout frozen with one enumeration manifest. `total` / `excluded_secret` describe the
/// whole authorized universe the manifest was minted from; `returned` is per page, so it is
/// supplied by [`Self::for_page`] rather than stored.
struct FrozenCensus {
    predicate_id: String,
    total: u64,
    excluded_secret: u64,
}

impl FrozenCensus {
    fn for_page(&self, returned: usize) -> humaux_retrieval::completeness::CensusResult {
        use humaux_retrieval::completeness::{CensusResult, ExactEnumeration};
        // A page longer than its own denominator allows is the 分母内生 shape §22.1 refuses;
        // the sole constructor rejecting our readout is §22.4 trigger 4, not a 500.
        match ExactEnumeration::new(
            self.predicate_id.as_str(),
            self.total,
            returned as u64,
            self.excluded_secret,
        ) {
            Ok(enumeration) => CensusResult::enumerated(enumeration),
            Err(_) => CensusResult::failed(),
        }
    }
}

/// Freezes this manifest's census readout onto its own `ops.selection_snapshots` row
/// (migration 0165) inside the minting transaction, so every later page reads the denominator
/// that was counted in the manifest's snapshot instead of re-counting in a younger one.
async fn store_frozen_census_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    snapshot_id: Uuid,
    census: &FrozenCensus,
) -> Result<(), ErrorCode> {
    sqlx::query(
        "UPDATE ops.selection_snapshots \
            SET census_predicate_id = $3, census_total = $4, census_excluded_secret = $5 \
          WHERE selection_snapshot_id = $1 AND tenant_id = $2",
    )
    .bind(snapshot_id)
    .bind(tenant_id)
    .bind(census.predicate_id.as_str())
    .bind(i64::try_from(census.total).map_err(|_| ErrorCode::Internal)?)
    .bind(i64::try_from(census.excluded_secret).map_err(|_| ErrorCode::Internal)?)
    .execute(&mut **txn)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(())
}

/// Reads back [`store_frozen_census_in_txn`]'s readout. `None` = this manifest carries no
/// census (three NULLs — a pre-0165 snapshot, or a mint whose census failed); the caller turns
/// that into [`CensusResult::failed`], never into a total of `0` (§23.3④).
async fn frozen_census_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    snapshot_id: Uuid,
) -> Result<Option<FrozenCensus>, ErrorCode> {
    let row = sqlx::query(
        "SELECT census_predicate_id, census_total, census_excluded_secret \
           FROM ops.selection_snapshots \
          WHERE selection_snapshot_id = $1 AND tenant_id = $2",
    )
    .bind(snapshot_id)
    .bind(tenant_id)
    .fetch_optional(&mut **txn)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let predicate_id: Option<String> = row
        .try_get("census_predicate_id")
        .map_err(|_| ErrorCode::Internal)?;
    let total: Option<i64> = row
        .try_get("census_total")
        .map_err(|_| ErrorCode::Internal)?;
    let excluded: Option<i64> = row
        .try_get("census_excluded_secret")
        .map_err(|_| ErrorCode::Internal)?;
    // The 0165 CHECK makes the three all-or-nothing at the DB layer; this reads them as such
    // rather than defaulting any one of them.
    let (Some(predicate_id), Some(total), Some(excluded)) = (predicate_id, total, excluded) else {
        return Ok(None);
    };
    Ok(Some(FrozenCensus {
        predicate_id,
        total: u64::try_from(total).map_err(|_| ErrorCode::Internal)?,
        excluded_secret: u64::try_from(excluded).map_err(|_| ErrorCode::Internal)?,
    }))
}

/// §23.1④ negative control seam (ADR-0041 D-G). Zero — the default, and the only value any
/// production process ever holds — means the barrier below emits no statement at all.
static CENSUS_MINT_BARRIER: AtomicI64 = AtomicI64::new(0);

/// Arms [`census_mint_barrier_in_txn`] with a PostgreSQL advisory-lock key; `0` disarms it.
///
/// §22.1's load-bearing claim is that `total` is counted "与返回项取**同一事务快照**", and an
/// insert that lands after a mint has already returned cannot tell that apart from a census
/// taken in its own younger transaction — both answer with the pre-insert number. The only
/// witness that separates them is a commit INSIDE the mint's window, which needs the mint to
/// hold still for one. A test takes the advisory lock on a second connection, arms this key,
/// issues the enumerate call, commits its row while the mint is parked, then releases.
pub fn arm_census_mint_barrier(advisory_key: i64) {
    CENSUS_MINT_BARRIER.store(advisory_key, Ordering::SeqCst);
}

/// Parks the minting transaction after its snapshot and id list are taken and before its
/// census, when (and only when) [`arm_census_mint_barrier`] armed a key. `pg_advisory_xact_lock`
/// releases with the transaction, so no unlock path can leak a held lock onto a pooled
/// connection.
async fn census_mint_barrier_in_txn(txn: &mut Txn<'_>) -> Result<(), ErrorCode> {
    let advisory_key = CENSUS_MINT_BARRIER.load(Ordering::SeqCst);
    if advisory_key == 0 {
        return Ok(());
    }
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(advisory_key)
        .execute(&mut **txn)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(())
}

/// Runs the §22.1 census for the authorized universe this manifest is being minted from, in
/// the SAME transaction (and therefore the same snapshot) as `universe` — the id list
/// `final_memory_ids_in_txn` just produced.
///
/// `universe` is not the source of any count (§22.1: "禁止用召回条数冒充 `total`" — the three
/// statements inside the census do their own `count(*)`). It is the cross-check: the census
/// and the manifest are the two faces of ONE predicate ([`ENUMERATION_SCOPE`] /
/// [`ENUMERATION_PREDICATE`]), so their id sets must be identical. Disagreement means the two
/// faces drifted apart — §22.4 trigger 4, reported as a failed census instead of publishing a
/// `coverage` for a set the page never held.
async fn mint_census_in_txn(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    workspace: WorkspaceId,
    stream: &StreamKey,
    subject_id: Option<Uuid>,
    universe: &[Uuid],
) -> Result<Option<FrozenCensus>, ErrorCode> {
    let outcome = census_in_txn(
        txn,
        &CensusInputs {
            predicate_id: AUTHORIZED_MEMORY_ENUMERATION_V1,
            enumerable_scope: ENUMERATION_SCOPE,
            sql_predicate: ENUMERATION_PREDICATE,
            authorization,
            workspace,
            stream,
            subject_id,
        },
    )
    .await?;
    let Some(enumeration) = outcome.census.enumeration() else {
        return Ok(None);
    };
    if outcome.returned_ids.iter().copied().collect::<HashSet<_>>()
        != universe.iter().copied().collect::<HashSet<_>>()
    {
        return Ok(None);
    }
    Ok(Some(FrozenCensus {
        predicate_id: enumeration.predicate_id().to_owned(),
        total: enumeration.total(),
        excluded_secret: enumeration.excluded_secret(),
    }))
}

/// Materializes an authorization-bound immutable Memory page.
#[allow(clippy::too_many_lines)] // ADR-0028 D-D: the exact enumeration predicate now carries the subject_id filter inside the same manifest SQL + fingerprint; splitting it would separate the predicate from the completeness classification it must stay honest with.
pub async fn materialize_memory_enumeration(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    scope: &Scope,
    expected_family: &StreamFamily,
    validated_key: &StreamKey,
    params: MemoryEnumerationParams<'_>,
) -> Result<MaterializedMemoryPage, ErrorCode> {
    validate_enumeration_params(&params)?;
    let (authorization, scope) = canonical_scope(authorization, scope)?;
    materialized_identity(&scope, expected_family, validated_key)?;
    let fingerprint = enumeration_fingerprint(&authorization, &scope, params.subject_id);
    let mut txn = pool
        .pool()
        // dep: PostgreSQL(any) — opens a PostgreSQL transaction
        .begin()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let read_write = params.cursor.is_none();
    sqlx::query(if read_write {
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE"
    } else {
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
    })
    .execute(&mut *txn)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;
    set_authorization_local(&mut txn, &authorization).await?;
    let (page, frozen_census) = if let Some(encoded) = params.cursor {
        let cursor = Cursor::decode(encoded).map_err(|_| ErrorCode::InvalidInput)?;
        let page = fetch_authorized_snapshot_page_in_txn(
            &mut txn,
            authorization.tenant_id().0,
            &cursor,
            &fingerprint,
            params.page_size as i64,
            params.mac_key,
        )
        .await?;
        // §22.1: a later page's denominator is the one frozen WITH the manifest (migration
        // 0165). Re-counting here would pair a younger snapshot's total with frozen items.
        let census =
            frozen_census_in_txn(&mut txn, authorization.tenant_id().0, page.snapshot_id).await?;
        (page, census)
    } else {
        // §6.1.3 D-D: the subject filter is part of the EXACT manifest predicate (RLS-visible
        // memory_subjects rows), never a post-filter over an unfiltered page. The scope +
        // predicate are the SAME two fragments the census counts (see `ENUMERATION_SCOPE`);
        // `$3` is the subject filter, matching `exact_census`'s own fragment.
        let rows = sqlx::query(&format!(
            "SELECT memory_id FROM {ENUMERATION_SCOPE} AND ({ENUMERATION_PREDICATE}) \
               AND {ACTIVE_FINAL} \
               AND ($3::uuid IS NULL OR EXISTS (SELECT 1 FROM private.memory_subjects ms \
                    WHERE ms.tenant_id = memory_records.tenant_id \
                      AND ms.memory_id = memory_records.memory_id AND ms.subject_id = $3)) \
             ORDER BY memory_id DESC"
        ))
        .bind(authorization.tenant_id().0)
        .bind(scope.workspace_id.map(|workspace| workspace.0))
        .bind(params.subject_id)
        .fetch_all(&mut *txn)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
        let candidates = rows
            .into_iter()
            .map(|row| row.try_get("memory_id").map_err(|_| ErrorCode::Internal))
            .collect::<Result<Vec<Uuid>, ErrorCode>>()?;
        let ids = final_memory_ids_in_txn(&mut txn, &authorization, &candidates, false).await?;
        // Disarmed in production; a test parks the mint here to commit a row into the window
        // between this id list and the census below (ADR-0041 D-G).
        census_mint_barrier_in_txn(&mut txn).await?;
        // Same transaction, therefore the same REPEATABLE READ snapshot as `ids` and as every
        // read below it (§22.1's "与返回项取同一事务快照").
        let census = match scope.workspace_id {
            Some(workspace) => {
                mint_census_in_txn(
                    &mut txn,
                    &authorization,
                    workspace,
                    validated_key,
                    params.subject_id,
                    &ids,
                )
                .await?
            }
            // No workspace means no `$2` for the enumerable scope — there is no universe to
            // count, so nothing is claimed (never a total of 0).
            None => None,
        };
        let page = begin_authorized_snapshot_in_txn(
            &mut txn,
            authorization.tenant_id().0,
            &fingerprint,
            params.ttl,
            params.page_size as i64,
            params.mac_key,
            &ids,
        )
        .await?;
        if let Some(census) = census.as_ref() {
            store_frozen_census_in_txn(
                &mut txn,
                authorization.tenant_id().0,
                page.snapshot_id,
                census,
            )
            .await?;
        }
        (page, census)
    };
    let expected: HashSet<_> = page.items.iter().copied().collect();
    if expected.len() != page.items.len() {
        return Err(ErrorCode::Internal);
    }
    let bodies = materialize_final_bodies_in_txn(
        &mut txn,
        &authorization,
        expected_family,
        validated_key,
        &page.items,
        &[],
        false,
    )
    .await?;
    let materialized = body_ids(&bodies);
    if materialized.len() != bodies.items.len()
        || materialized.iter().any(|id| !expected.contains(id))
        || materialized.iter().collect::<HashSet<_>>().len() != materialized.len()
    {
        return Err(ErrorCode::Internal);
    }
    if materialized.len() != page.items.len() {
        return Err(ErrorCode::NotFound);
    }
    let grounding = page_grounding_in_txn(&mut txn, &authorization, &page.items).await?;
    let subjects =
        memory_subjects_in_txn(&mut txn, authorization.tenant_id().0, &page.items).await?;
    let ledger = close_ledger_in_txn(&mut txn, validated_key)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    // §23.3④: read in the same transaction as the ledger whose `expected` they are compared
    // against. `None` census ⇒ no counts either (see `MaterializedMemoryPage::census`).
    let census = match scope.workspace_id {
        Some(_) => Some(EnumerationCensus {
            census: frozen_census.as_ref().map_or_else(
                humaux_retrieval::completeness::CensusResult::failed,
                |census| census.for_page(page.items.len()),
            ),
            pipeline: stream_pipeline_counts_in_txn(&mut txn, validated_key).await?,
        }),
        None => None,
    };
    txn.commit()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(MaterializedMemoryPage {
        snapshot_id: page.snapshot_id,
        next_cursor: page.next_cursor.map(|cursor| cursor.encode()),
        memory: MaterializedMemory {
            bodies,
            ledger,
            grounding,
            subjects,
            // Enumerate excludes archived rows at the candidate query (D-C), so a page never
            // carries one; the flag is meaningful only on memory.get.
            archived: false,
        },
        census,
    })
}

/// Gateway-callable Context manifest and bodies produced from one PostgreSQL snapshot.
pub struct MaterializedContext {
    /// Canonical v1 manifest assembled from the frozen lanes.
    pub handoff: Handoff,
    /// Compiler outcome that produced the manifest, including mandatory overflow.
    pub outcome: ContextOutcome,
    /// Final body rows rechecked in the manifest's transaction snapshot.
    pub bodies: MaterializedBodies,
    /// Stream ledger closure read in that same transaction snapshot.
    pub ledger: humaux_retrieval::completeness::LedgerClosure,
    /// §23.3④ `stream_ledger` pipeline readings for `validated_key`, taken in that same
    /// snapshot as the ledger and the bodies (ADR-0041 D-H). Not an `Option`: unlike
    /// [`MaterializedMemoryPage::census`] there is no census travelling with it and
    /// `classify()` maps this route's `PlannerDecision::Class(_)` to `SemanticBounded`, so
    /// §22.0's exact-without-a-predicate trap is not on this path.
    pub pipeline: StreamPipelineCounts,
    /// Actual §8.8 states retained from compiled mandatory and pinned rows.
    pub grounding: GroundingBlock,
}

fn grounding_block(outcome: &ContextOutcome) -> Result<GroundingBlock, ErrorCode> {
    let ContextOutcome::Compiled(compiled) = outcome else {
        return Ok(GroundingBlock::tally([]));
    };
    let mut states = Vec::with_capacity(compiled.items().len());
    for item in compiled.items() {
        let state = match item {
            ContextItem::Mandatory(row) | ContextItem::Pinned(row) => row.grounding_state(),
            ContextItem::Supplemental(_) => return Err(ErrorCode::DependencyUnavailable),
        };
        states.push(state);
    }
    Ok(GroundingBlock::tally(states))
}

fn materialized_ids(outcome: &ContextOutcome) -> Vec<Uuid> {
    let ContextOutcome::Compiled(compiled) = outcome else {
        return Vec::new();
    };
    compiled
        .mandatory_ids()
        .into_iter()
        .chain(compiled.pinned_ids())
        .map(|id| id.0)
        .collect()
}

fn handoff_ids(handoff: &Handoff) -> Result<Vec<Uuid>, ErrorCode> {
    handoff
        .mandatory
        .iter()
        .chain(&handoff.pinned)
        .map(|item| Uuid::parse_str(&item.memory_id).map_err(|_| ErrorCode::DependencyUnavailable))
        .collect()
}

fn body_ids(bodies: &MaterializedBodies) -> Vec<Uuid> {
    bodies
        .items
        .iter()
        .filter_map(|item| match item {
            MaterializedItem::Memory { memory_id, .. } => Some(*memory_id),
            _ => None,
        })
        .collect()
}

fn same_id_set(left: &[Uuid], right: &[Uuid]) -> bool {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort_unstable();
    left.dedup();
    right.sort_unstable();
    right.dedup();
    left == right
}
fn materialized_identity(
    scope: &Scope,
    family: &StreamFamily,
    key: &StreamKey,
) -> Result<(), ErrorCode> {
    if family.tenant_id != scope.tenant_id
        || key.tenant_id != scope.tenant_id
        || family.tenant_id != key.tenant_id
        || family.scope_kind != key.scope_kind
        || family.scope_id != key.scope_id
        || family.domain != key.domain
        || family.projection_kind != key.projection_kind
    {
        return Err(ErrorCode::Forbidden);
    }
    match (scope.workspace_id, key.scope_kind.as_str()) {
        (None, "tenant") if key.scope_id == scope.tenant_id.0 => Ok(()),
        (Some(workspace), "workspace") if key.scope_id == workspace.0 => Ok(()),
        _ => Err(ErrorCode::Forbidden),
    }
}
/// Reads grounding facts for a Memory already admitted by final materialization.
///
/// The preceding body read has already performed the Memory-plus-all-Evidence authorization
/// gate. A missing row in this same RR snapshot is therefore an invariant break, rather than
/// a second observable existence oracle.
async fn direct_get_grounding_in_txn(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    memory_id: MemoryId,
) -> Result<GroundingBlock, ErrorCode> {
    let row = sqlx::query(
        r#"
        SELECT EXISTS(
                   SELECT 1
                   FROM private.memory_evidence AS me
                   WHERE me.memory_id = m.memory_id
                     AND me.grounding_mode = 'LIVE'
               ) AS has_live,
               EXISTS(
                   SELECT 1
                   FROM private.memory_evidence AS me
                   WHERE me.memory_id = m.memory_id
                     AND me.grounding_mode = 'LIVE'
                     AND me.recorded_version IS NULL
               ) AS has_live_unversioned
        FROM private.memory_records AS m
        WHERE m.tenant_id = $1
          AND m.memory_id = $2
          AND m.status = 'active'
          AND m.superseded_by IS NULL
        "#,
    )
    .bind(authorization.tenant_id().0)
    .bind(memory_id.0)
    .fetch_optional(&mut **txn)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?
    .ok_or(ErrorCode::Internal)?;
    let grounding = grounding_from_facts(
        row.try_get("has_live").map_err(|_| ErrorCode::Internal)?,
        row.try_get("has_live_unversioned")
            .map_err(|_| ErrorCode::Internal)?,
    );
    let state = match grounding {
        RowGrounding::Judged(state) => Some(state),
        RowGrounding::NotJudged => None,
    };
    Ok(GroundingBlock::tally([state]))
}

/// Materializes one exact Memory, its final body, ledger and grounding in one RR snapshot.
///
/// Scope canonicalization runs before object access, so a requested foreign workspace remains
/// `FORBIDDEN`; all object-level absence and final-body policy failures map to `NOT_FOUND`.
/// `expected_family` and `validated_key` must be supplied by trusted serving/token validation.
pub async fn materialize_memory_get(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    scope: &Scope,
    expected_family: &StreamFamily,
    validated_key: &StreamKey,
    memory_id: MemoryId,
) -> Result<MaterializedMemory, ErrorCode> {
    let _ = canonical_scope(authorization, scope)?;
    materialized_identity(scope, expected_family, validated_key)?;
    let mut txn = pool
        .pool()
        // dep: PostgreSQL(any) — opens a PostgreSQL transaction
        .begin()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *txn)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let bodies = materialize_one_memory_in_txn(
        &mut txn,
        authorization,
        expected_family,
        validated_key,
        memory_id,
    )
    .await?;
    let grounding = direct_get_grounding_in_txn(&mut txn, authorization, memory_id).await?;
    // Q3/ADR-0024 D-C: memory.get surfaces `archived:true` (unlike recall/context, which
    // exclude archived rows). The body read above already gated visibility/lifecycle, so a
    // missing row here is an invariant break in the same RR snapshot, not an existence oracle.
    let archived: bool = sqlx::query_scalar(
        "SELECT archived_at IS NOT NULL FROM private.memory_records \
         WHERE tenant_id = $1 AND memory_id = $2",
    )
    .bind(authorization.tenant_id().0)
    .bind(memory_id.0)
    .fetch_optional(&mut *txn)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?
    .ok_or(ErrorCode::Internal)?;
    let subjects =
        memory_subjects_in_txn(&mut txn, authorization.tenant_id().0, &[memory_id.0]).await?;
    let ledger = close_ledger_in_txn(&mut txn, validated_key)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    txn.commit()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(MaterializedMemory {
        bodies,
        ledger,
        grounding,
        subjects,
        archived,
    })
}

/// Assembles v1 Handoff and its mandatory/pinned bodies in one RR read-only transaction.
pub async fn assemble_materialized(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    scope: &Scope,
    budget: ContextBudget,
    expected_family: &StreamFamily,
    validated_key: &StreamKey,
) -> Result<MaterializedContext, ErrorCode> {
    let _ = canonical_scope(authorization, scope)?;
    materialized_identity(scope, expected_family, validated_key)?;
    let mut txn = pool
        .pool()
        // dep: PostgreSQL(any) — opens a PostgreSQL transaction
        .begin()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *txn)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let frozen = fetch_frozen_in_txn(&mut txn, authorization, scope).await?;
    let (handoff, outcome) = assemble_with_context(frozen, budget);
    let ids = materialized_ids(&outcome);
    let grounding = grounding_block(&outcome)?;
    let bodies = materialize_final_bodies_in_txn(
        &mut txn,
        authorization,
        expected_family,
        validated_key,
        &ids,
        &[],
        false,
    )
    .await?;
    let emitted_ids = handoff_ids(&handoff)?;
    if handoff.context_snapshot_seq != bodies.snapshot.context_snapshot_seq
        || handoff.snapshot_token_sha256 != bodies.snapshot.snapshot_token_sha256
        || !same_id_set(&ids, &emitted_ids)
        || !same_id_set(&ids, &body_ids(&bodies))
    {
        return Err(ErrorCode::DependencyUnavailable);
    }
    let ledger = close_ledger_in_txn(&mut txn, validated_key)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let pipeline = stream_pipeline_counts_in_txn(&mut txn, validated_key).await?;
    txn.commit()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(MaterializedContext {
        handoff,
        outcome,
        bodies,
        ledger,
        pipeline,
        grounding,
    })
}

/// [`fetch_frozen`] 的 pinned 半边：独立候选枚举（外部 oracle——「钉 3 带 2」必须可观测）
/// + 取行 + excluded 具名。抽成函数只为行数闸，语义与内联时逐字相同。
async fn fetch_pinned_in_txn(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    scope: &Scope,
) -> Result<PinnedLane, ErrorCode> {
    let (kinds, ids) = scope_chain_params(scope);
    let where_clause = "m.tenant_id = $1 AND m.status = 'active' AND m.superseded_by IS NULL \
        AND m.archived_at IS NULL \
        AND EXISTS ( \
          SELECT 1 FROM private.context_bindings cb \
          WHERE cb.memory_id = m.memory_id AND cb.tenant_id = m.tenant_id \
            AND cb.revoked_at IS NULL AND cb.mode = 'PINNED' \
            AND EXISTS ( \
              SELECT 1 FROM unnest($2::text[], $3::uuid[]) AS request_scope(kind, id) \
              WHERE cb.scope_kind = request_scope.kind \
                AND COALESCE(cb.scope_id, cb.tenant_id) = request_scope.id \
            ) \
        )";
    let candidates = sqlx::query(&format!(
        "SELECT m.memory_id FROM private.memory_records m WHERE {where_clause}"
    ))
    .bind(scope.tenant_id.0)
    .bind(&kinds)
    .bind(&ids)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    let candidate_ids: Vec<Uuid> = candidates
        .into_iter()
        .map(|row| row.try_get("memory_id").map_err(|_| ErrorCode::Internal))
        .collect::<Result<_, _>>()?;
    let expected_ids = readable_memory_ids(txn, authorization, &candidate_ids).await?;

    let rows = sqlx::query(&format!(
        "SELECT m.memory_id, m.authority_class, {EST_TOKENS_EXPR} AS est_tokens, \
                EXISTS(SELECT 1 FROM private.memory_evidence me \
                       WHERE me.memory_id = m.memory_id AND me.grounding_mode = 'LIVE') \
                  AS has_live, \
                EXISTS(SELECT 1 FROM private.memory_evidence me \
                       WHERE me.memory_id = m.memory_id AND me.grounding_mode = 'LIVE' \
                         AND me.recorded_version IS NULL) \
                  AS has_live_unversioned \
         FROM private.memory_records m \
         WHERE {where_clause} \
         ORDER BY m.memory_id"
    ))
    .bind(scope.tenant_id.0)
    .bind(kinds)
    .bind(ids)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;

    let sp = spec(SelectorId::ExplicitMandatoryBindingsV1);
    let mut pinned_rows = Vec::with_capacity(rows.len());
    let mut excluded = Vec::new();
    for r in rows {
        let memory_id: Uuid = r.try_get("memory_id").map_err(|_| ErrorCode::Internal)?;
        if !expected_ids.contains(&memory_id) {
            continue;
        }
        let authority: String = r
            .try_get("authority_class")
            .map_err(|_| ErrorCode::Internal)?;
        let est_tokens: i32 = r.try_get("est_tokens").map_err(|_| ErrorCode::Internal)?;
        let has_live: bool = r.try_get("has_live").map_err(|_| ErrorCode::Internal)?;
        let has_live_unversioned: bool = r
            .try_get("has_live_unversioned")
            .map_err(|_| ErrorCode::Internal)?;
        let authority = parse_authority(&authority)?;
        if (authority as u8) < (sp.min_authority as u8) {
            // 低 authority 的 pinned 行不进 lane，但**具名**——「钉 3 带 2」必须可观测。
            excluded.push(MemoryId(memory_id));
            continue;
        }
        match MandatoryRow::from_selector(
            sp,
            MemoryId(memory_id),
            authority,
            u32::try_from(est_tokens).unwrap_or(u32::MAX),
            grounding_from_facts(has_live, has_live_unversioned),
        )
        .map_err(|_| ErrorCode::Internal)?
        {
            Admitted::Row(row) => pinned_rows.push(row),
            // pinned 行同样过 DOD-093 门：RECHECK_REQUIRED 的 pinned 不进 lane。
            // 它的披露并入 mandatory 侧的 needs_verification 是错的（不同 lane）——
            // 以 excluded 具名。ponytail: pinned 专属 needs_verification 列表等
            // §25.5 顶层块形状定了再拆，excluded 先保证可观测。
            Admitted::NeedsVerification(nv) => excluded.push(nv.memory_id),
        }
    }
    Ok(PinnedLane::new(
        u64::try_from(expected_ids.len()).unwrap_or(0),
        pinned_rows,
        excluded,
    ))
}

/// 全 workspace **唯一**的 `INSERT INTO private.context_bindings`，在调用方的事务里。
///
/// 只收 [`BindingGrant`]——它的字段私有、无 pub 构造式，拿到它的唯一办法是走
/// `domain::context` 的三个 `authorize_*` 之一。所以「绕过授权直接写 binding」不是一条
/// 要靠评审拦住的路径，是这个函数签名收不下的东西。
///
/// 调用方负责 `humaux.tenant_id` 已在该事务里 `SET LOCAL`（[`insert_binding`] 自己做；
/// [`write_binding_confirmed`] 的信封在开头做）——binding 写与它的确认/审计**同一事务**，
/// 信封回滚时它一起回滚，不存在「行已提交、token 未消费」的窗口。
///
/// # Errors
/// 库不可达、或唯一索引冲突（同 scope 同 memory 同 mode 已有未撤销的 binding）。
pub async fn insert_binding_in_txn(
    txn: &mut Txn<'_>,
    created_by: Uuid,
    grant: &BindingGrant,
    tenant_id: Uuid,
) -> Result<Uuid, ErrorCode> {
    sqlx::query(
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
    .fetch_one(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?
    .try_get(0)
    .map_err(|_| ErrorCode::Internal)
}

/// [`insert_binding_in_txn`] in its own short transaction (non-gated callers).
///
/// # Errors
/// As [`insert_binding_in_txn`].
pub async fn insert_binding(
    pool: &RuntimeDbPool,
    created_by: Uuid,
    grant: &BindingGrant,
    tenant_id: Uuid,
) -> Result<Uuid, ErrorCode> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    set_tenant_local(&mut txn, tenant_id)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    let id = insert_binding_in_txn(&mut txn, created_by, grant, tenant_id).await?;
    txn.commit().await.map_err(|_| ErrorCode::Internal)?;
    Ok(id)
}

/// 撤销一条 binding，在调用方的事务里。**软删除**：binding 的历史是审计对象，物理删掉就
/// 查不到"谁在什么时候把什么钉进过 Context"。返回是否真的改了一行（已撤销的再撤一次返回
/// `false`）。`tenant_id` 同时进 WHERE，不只依赖 GUC。
///
/// # Errors
/// 库不可达。
pub async fn revoke_binding_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    binding_id: Uuid,
) -> Result<bool, ErrorCode> {
    let affected = sqlx::query(
        "UPDATE private.context_bindings SET revoked_at = now() \
         WHERE context_binding_id = $1 AND tenant_id = $2 AND revoked_at IS NULL",
    )
    .bind(binding_id)
    .bind(tenant_id)
    .execute(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?
    .rows_affected();
    Ok(affected == 1)
}

/// §25.4.A(9)'s A->B replacement, the **only** site that takes a binding id from the wire:
/// revokes exactly the binding this `memory.bind` is authorized to replace.
///
/// [`revoke_binding_in_txn`]'s two other callers derive their binding id in-transaction
/// (`active_pinned_binding_in_txn` / `active_task_mandatory_binding_in_txn`), so the id there
/// is already known to be the right row. `replaces_binding_id` is not: it arrives unvalidated
/// from the MCP argument, and the confirm token binds only (tenant, user, op, memory, task).
/// The extra predicates are what keeps the replacement inside the dimension the token DOES
/// cover — the same (TASK, `scope_id`, MANDATORY) tuple this call is writing. A caller-supplied
/// id outside it (another task's MANDATORY row, any WORKSPACE/PINNED row, another tenant's
/// binding) matches zero rows and the call is `Conflict`; it is never revoked.
///
/// # Errors
/// [`ErrorCode::Internal`] on a DB error. `Ok(false)` = no row matched (caller: `Conflict`).
async fn revoke_task_mandatory_binding_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    task: humaux_domain::ids::TaskId,
    binding_id: Uuid,
) -> Result<bool, ErrorCode> {
    let affected = sqlx::query(
        "UPDATE private.context_bindings SET revoked_at = now() \
         WHERE context_binding_id = $1 AND tenant_id = $2 \
           AND scope_kind = 'TASK' AND scope_id = $3 AND mode = 'MANDATORY' \
           AND revoked_at IS NULL",
    )
    .bind(binding_id)
    .bind(tenant_id)
    .bind(task.0)
    .execute(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?
    .rows_affected();
    Ok(affected == 1)
}

/// The authorization Evidence row for ONE task-instruction approval (card 22c, ADR-0046).
///
/// What it is: a `UserConfirmed` EVENT whose `payload_sha256` is the digest of the **complete
/// intent** that was just confirmed — tenant, task, epoch, binding, memory, that memory's exact
/// canonical payload digest, and the purpose. Recomputing that digest from the grant row is how
/// an auditor checks that the approval on file is the approval that happened.
///
/// What it is deliberately NOT: it is **never** linked into the target memory's
/// `private.memory_evidence`. A task-authorization receipt is not a general-purpose
/// authority basis — linking it would let the next `AuthorityPolicy::authorize` read a
/// `UserConfirmed` origin that nobody asserted about the *content* (ruling §五, last line:
/// 授权材料不洗白). That is why this is a plain INSERT here and not a call into the
/// memory-evidence writers.
///
/// `reasoning_domain_id` is taken from the target memory's own Evidence, in this transaction:
/// the domain is a property of where the memory lives, and re-deriving it from anywhere else
/// would be a second source of truth for a column this row only has to be consistent with.
///
/// # Errors
/// [`ErrorCode::Forbidden`] when the memory has no readable Evidence to take the domain from
/// (no basis ⇒ nothing to authorize), `Internal` on a DB error.
async fn insert_task_authorization_evidence_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    user_id: Uuid,
    memory: MemoryId,
    intent: &str,
) -> Result<Uuid, ErrorCode> {
    let domain: Option<Uuid> = sqlx::query_scalar(
        "SELECT eo.reasoning_domain_id FROM private.memory_evidence me \
           JOIN private.evidence_objects eo \
             ON eo.evidence_id = me.evidence_id AND eo.tenant_id = $2 \
          WHERE me.memory_id = $1 ORDER BY eo.evidence_id LIMIT 1",
    )
    .bind(memory.0)
    .bind(tenant_id)
    .fetch_optional(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    let domain = domain.ok_or(ErrorCode::Forbidden)?;

    sqlx::query_scalar(
        "INSERT INTO private.evidence_objects \
           (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
            origin_principal_id, visibility_class, visibility_user_id, reasoning_domain_id, \
            occurred_at) \
         VALUES ($1, 'EVENT', sha256(convert_to($2, 'UTF8')), 'INTERNAL', 'UserConfirmed', \
                 $3, 'USER_PRIVATE', $3, $4, clock_timestamp()) \
         RETURNING evidence_id",
    )
    .bind(tenant_id)
    .bind(intent)
    .bind(user_id)
    .bind(domain)
    .fetch_one(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)
}

/// The **sole** `private.task_binding_grants` INSERT site (pinned by architecture-check A3).
///
/// Everything it writes is either derived in this transaction from rows it just read, or a
/// frozen constant — nothing comes from the wire:
///
/// * `payload_sha256` is computed by the database from the target row itself, through the one
///   [`CANONICAL_PAYLOAD_SHA256_EXPR`] the read side also uses. The caller cannot hand in a
///   digest, so "approve this id, then swap the content" has no writable surface.
/// * `task_epoch` comes from the `AuthenticatedTask` the same transaction resolved.
/// * `grant_authority` / `purpose` / `policy_version` are the frozen constants of §25.4 v2.
/// * `operation_id` is the request's BMO operation, UNIQUE per tenant, so a replayed operation
///   cannot mint a second grant for the same approval.
///
/// The `INSERT ... SELECT` reads `private.memory_records` inside the statement: if the target
/// disappeared between the visibility check and here, zero rows are inserted and the caller
/// sees `Conflict` rather than a grant over nothing.
///
/// # Errors
/// [`ErrorCode::Conflict`] when no row was inserted; `Internal` on a DB error.
async fn insert_task_binding_grant_in_txn(
    txn: &mut Txn<'_>,
    task: &AuthenticatedTask,
    binding_id: Uuid,
    memory: MemoryId,
    issued_by: Uuid,
    authorization_evidence_id: Uuid,
    operation_id: Uuid,
) -> Result<(), ErrorCode> {
    let sql = format!(
        "INSERT INTO private.task_binding_grants \
           (tenant_id, context_binding_id, scope_kind, task_id, memory_id, mode, task_epoch, \
            payload_sha256, grant_authority, purpose, issuer_kind, issued_by_principal_id, \
            authorization_evidence_id, operation_id, policy_version) \
         SELECT $1, $2, 'TASK', $3, m.memory_id, 'MANDATORY', $4, \
                {CANONICAL_PAYLOAD_SHA256_EXPR}, $5, $6, $7, $8, $9, $10, $11 \
           FROM private.memory_records m \
          WHERE m.tenant_id = $1 AND m.memory_id = $12"
    );
    let affected = sqlx::query(&sql)
        .bind(task.tenant_id())
        .bind(binding_id)
        .bind(task.task_id())
        .bind(task.authorization_epoch())
        .bind(humaux_domain::context::TASK_GRANT_AUTHORITY)
        .bind(BindingPurpose::AdoptTaskInstruction.wire())
        .bind(TaskGrantIssuer::AuthenticatedTaskRequest.wire())
        .bind(issued_by)
        .bind(authorization_evidence_id)
        .bind(operation_id)
        .bind(TASK_AUTHORIZATION_POLICY_VERSION)
        .bind(memory.0)
        .execute(&mut **txn)
        .await
        .map_err(|_| ErrorCode::Internal)?
        .rows_affected();
    if affected == 1 {
        Ok(())
    } else {
        Err(ErrorCode::Conflict)
    }
}

/// Is there an ACTIVE (unrevoked) task authorization for this binding right now?
///
/// Read-back, used by the write path to report `task_authorized` honestly instead of echoing
/// the request's `purpose` back at the caller.
///
/// # Errors
/// `Internal` on a DB error.
async fn active_task_grant_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    binding_id: Uuid,
) -> Result<bool, ErrorCode> {
    let found: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM private.task_binding_grants \
          WHERE tenant_id = $1 AND context_binding_id = $2 AND revoked_at IS NULL",
    )
    .bind(tenant_id)
    .bind(binding_id)
    .fetch_optional(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    Ok(found.is_some())
}

/// Revokes the grant that belongs to one binding — `memory.unbind`'s half of I-NONINHERIT.
///
/// Revoke-only in the same sense the DB trigger enforces: a single `SET revoked_at` on a row
/// that is still active. Zero rows is **not** an error here (a REFERENCE_ONLY binding has no
/// grant to revoke, and an already-revoked grant stays revoked); the binding revoke above it
/// is what decides `Conflict`. `revoked_at` is the only column any runtime role can write.
///
/// # Errors
/// `Internal` on a DB error.
async fn revoke_task_binding_grant_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    binding_id: Uuid,
) -> Result<(), ErrorCode> {
    sqlx::query(
        "UPDATE private.task_binding_grants SET revoked_at = statement_timestamp() \
          WHERE tenant_id = $1 AND context_binding_id = $2 AND revoked_at IS NULL",
    )
    .bind(tenant_id)
    .bind(binding_id)
    .execute(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    Ok(())
}

/// [`revoke_binding_in_txn`] in its own short transaction (non-gated callers).
///
/// # Errors
/// As [`revoke_binding_in_txn`].
pub async fn revoke_binding(
    pool: &RuntimeDbPool,
    tenant_id: Uuid,
    binding_id: Uuid,
) -> Result<bool, ErrorCode> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    set_tenant_local(&mut txn, tenant_id)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    let revoked = revoke_binding_in_txn(&mut txn, tenant_id, binding_id).await?;
    txn.commit().await.map_err(|_| ErrorCode::Internal)?;
    Ok(revoked)
}

// =============================================================================
// §36 memory.pin / memory.unpin —— ADR-0019 的确认写入（confirm-gated binding writes）
// =============================================================================

/// Trusted inputs of one confirmed pin/unpin (built by the gateway gate, never deserialized
/// from MCP). `claim` is the presented token's binding, consumed **inside** this write's
/// transaction (`confirm_token_repo::consume_in_txn`, the only consume entry).
pub struct BindingWriteRequest {
    pub request_id: Uuid,
    pub request_fingerprint: String,
    pub reservation_ttl: std::time::Duration,
    pub memory: MemoryId,
    /// The credential's bound workspace: the PINNED row's scope (`application::pin::pin_request`).
    pub workspace: WorkspaceId,
    /// `memory.bind` / `memory.unbind` only (§25.4.A(7)): the authenticated TASK the MANDATORY
    /// binding is written under. `None` for pin/unpin; required (and rejected when absent) for
    /// bind/unbind — there is no tenant-wide MANDATORY binding on this route.
    pub task: Option<humaux_domain::ids::TaskId>,
    /// `memory.bind` only (§25.4.A(9)): the authorized A->B explicit replacement. The named
    /// binding is revoked and B's binding created **in this same transaction**; a binding that
    /// is already revoked (or is not this tenant's) is `Conflict`, never a silent skip.
    pub replaces_binding_id: Option<Uuid>,
    /// `memory.bind` only (card 22c, ADR-0046): what this binding is FOR.
    ///
    /// [`BindingPurpose::AdoptTaskInstruction`] is the only value that writes a
    /// `private.task_binding_grants` row — i.e. the only value that can ever make this memory
    /// usable at `ExplicitTaskContext` inside this task. [`BindingPurpose::ReferenceOnly`]
    /// still creates the binding, still shows up as a nominated obligation, and still gets
    /// rejected with `MISSING_TASK_AUTHORIZATION` at read time. `None` for the other three ops.
    pub purpose: Option<BindingPurpose>,
    pub claim: ConfirmationClaim,
    pub finished_audit: AuditEvent,
}

/// Result of a confirmed pin (D-C: idempotent) or unpin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindingWriteOutcome {
    pub binding_id: Uuid,
    /// pin: `false` when the row already existed (nothing inserted). unpin: always `false`.
    pub inserted: bool,
    /// card 22c (ADR-0046): does an ACTIVE task authorization exist for this binding now?
    ///
    /// Read back, not inferred from the request: on the idempotent `ReturnExisting` arm nothing
    /// was written this call, so "the caller asked for ADOPT_TASK_INSTRUCTION" says nothing
    /// about whether a grant is actually there. Always `false` for pin / unpin / unbind.
    pub task_authorized: bool,
}

/// Pure input contract (same shape as `memory_governance_repo::validate`): a real user, a
/// well-formed reservation, the claim bound to exactly this op + memory, the success audit
/// describing this op as an executed write (never carrying the mint tag).
fn validate_binding_write(
    auth: &AuthorizationScope,
    op: DestructiveOp,
    request: &BindingWriteRequest,
) -> Result<(), ErrorCode> {
    if auth.tenant_id().0.is_nil() || auth.principal().0.is_nil() || auth.user_id().is_none() {
        return Err(ErrorCode::Unauthorized);
    }
    if request.request_id.is_nil()
        || request.reservation_ttl.is_zero()
        || request.request_fingerprint.len() != 64
        || !request
            .request_fingerprint
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ErrorCode::InvalidInput);
    }
    // §25.4.A(7): the TASK dimension is required by exactly the two ops that write a TASK
    // binding, and must be absent on the two that write a WORKSPACE one. A bind with no task
    // is InvalidInput, not a tenant-wide MANDATORY binding by accident.
    match op {
        DestructiveOp::MemoryBind | DestructiveOp::MemoryUnbind => {
            if request.task.is_none() {
                return Err(ErrorCode::InvalidInput);
            }
            if op == DestructiveOp::MemoryUnbind && request.replaces_binding_id.is_some() {
                return Err(ErrorCode::InvalidInput);
            }
        }
        _ => {
            if request.task.is_some() || request.replaces_binding_id.is_some() {
                return Err(ErrorCode::InvalidInput);
            }
        }
    }
    match op {
        DestructiveOp::MemoryPin | DestructiveOp::MemoryUnpin => {
            humaux_application::pin::check_claim(
                op,
                request.claim.op,
                request.claim.target_id,
                request.claim.successor_id,
                request.memory,
            )?
        }
        // §25.4.A(7)/(8): the MANDATORY pair's claim binds a PAIR, not a single target — the
        // task rides the successor leg. This is not a copy of `pin::check_claim`: that one
        // REQUIRES `successor_id` to be absent (a pin has no second argument), so the two rules
        // are different assertions about different tuples and cannot share one implementation.
        DestructiveOp::MemoryBind | DestructiveOp::MemoryUnbind => {
            let task = request.task.ok_or(ErrorCode::InvalidInput)?;
            if request.claim.op != op
                || request.claim.target_id != request.memory.0
                // card 22c / ADR-0046 D-D: the successor leg is the intent digest (task +
                // purpose) the gateway minted with the SAME domain function — never `task.0`.
                || request.claim.successor_id
                    != Some(humaux_domain::context::binding_confirmation_successor(
                        task,
                        request.purpose,
                    ))
            {
                return Err(ErrorCode::Conflict);
            }
        }
        _ => return Err(ErrorCode::InvalidInput),
    }
    let event = &request.finished_audit;
    if event.tenant_id != auth.tenant_id()
        || event.actor_id != auth.principal().0.to_string()
        || event.request_id != request.request_id.to_string()
        || event.action != McpAuditAction::McpRequestFinished.as_str()
        || event.resource_id != op.operation_key()
        || event.result != "OK"
        || event
            .risk_tags
            .iter()
            .any(|tag| tag == RISK_TAG_CONFIRMATION_MINTED)
    {
        return Err(ErrorCode::InvalidInput);
    }
    Ok(())
}

/// The active PINNED row for (tenant, WORKSPACE scope, memory), if any — the same key
/// `ux_context_bindings_active` makes unique, so at most one row can match.
async fn active_pinned_binding_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    workspace: WorkspaceId,
    memory: MemoryId,
) -> Result<Option<Uuid>, ErrorCode> {
    sqlx::query_scalar(
        "SELECT context_binding_id FROM private.context_bindings \
         WHERE tenant_id = $1 AND memory_id = $2 AND mode = 'PINNED' \
           AND scope_kind = 'WORKSPACE' AND scope_id = $3 AND revoked_at IS NULL",
    )
    .bind(tenant_id)
    .bind(memory.0)
    .bind(workspace.0)
    .fetch_optional(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)
}

/// The active MANDATORY row for (tenant, TASK scope, memory), if any — the same key
/// `ux_context_bindings_active` makes unique, so at most one row can match.
async fn active_task_mandatory_binding_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    task: humaux_domain::ids::TaskId,
    memory: MemoryId,
) -> Result<Option<Uuid>, ErrorCode> {
    sqlx::query_scalar(
        "SELECT context_binding_id FROM private.context_bindings \
         WHERE tenant_id = $1 AND memory_id = $2 AND mode = 'MANDATORY' \
           AND scope_kind = 'TASK' AND scope_id = $3 AND revoked_at IS NULL",
    )
    .bind(tenant_id)
    .bind(memory.0)
    .bind(task.0)
    .fetch_optional(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)
}

/// The three facts `authorize_mandatory` re-checks §10.1 against, read **in this transaction**
/// from the row itself (ruling §三.2: a binding request may not hand the gate its own claims).
///
/// A memory with no readable Evidence origin has no basis at all, so there is nothing to
/// authorize against: `Forbidden`, not an empty-basis default.
async fn mandatory_binding_facts_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    memory: MemoryId,
) -> Result<
    (
        AuthorityClass,
        MemoryType,
        humaux_domain::authority::NonEmptyVec<humaux_domain::evidence::EvidenceOriginClass>,
    ),
    ErrorCode,
> {
    let row = sqlx::query(
        "SELECT m.authority_class, m.memory_type FROM private.memory_records m \
         WHERE m.memory_id = $1 AND m.tenant_id = $2",
    )
    .bind(memory.0)
    .bind(tenant_id)
    .fetch_optional(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?
    .ok_or(ErrorCode::NotFound)?;
    let authority: String = row
        .try_get("authority_class")
        .map_err(|_| ErrorCode::Internal)?;
    let memory_type: String = row
        .try_get("memory_type")
        .map_err(|_| ErrorCode::Internal)?;
    let authority = parse_authority(&authority)?;
    let memory_type = MemoryType::parse_wire(&memory_type).ok_or(ErrorCode::Internal)?;

    let origins: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT eo.origin_class FROM private.memory_evidence me \
           JOIN private.evidence_objects eo ON eo.evidence_id = me.evidence_id \
          WHERE me.memory_id = $1 AND eo.tenant_id = $2 ORDER BY 1",
    )
    .bind(memory.0)
    .bind(tenant_id)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    let basis = origins
        .iter()
        .map(|wire| crate::distill_repo::origin_class_from_db_str(wire).ok_or(ErrorCode::Internal))
        .collect::<Result<Vec<_>, _>>()?;
    let basis =
        humaux_domain::authority::NonEmptyVec::new(basis).map_err(|_| ErrorCode::Forbidden)?;
    Ok((authority, memory_type, basis))
}

fn rejection(_: humaux_domain::authority::CandidateRejection) -> ErrorCode {
    ErrorCode::Forbidden
}

/// `memory.pin`, confirmed call (ADR-0019 D-B/D-C). See [`write_binding_confirmed`].
///
/// # Errors
/// `Conflict` for a replayed/expired/misbound token or a lost BMO race; `NotFound` when the
/// memory is not visible to the caller; `DependencyUnavailable` when COMMIT is unproven.
pub async fn pin_confirmed(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    request: BindingWriteRequest,
) -> Result<BindingWriteOutcome, ErrorCode> {
    write_binding_confirmed(pool, auth, DestructiveOp::MemoryPin, request).await
}

/// `memory.unpin`, confirmed call: revokes the PINNED row only — Evidence/Memory untouched
/// (§36). `Conflict` when nothing is pinned (D-C).
///
/// # Errors
/// As [`pin_confirmed`].
pub async fn unpin_confirmed(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    request: BindingWriteRequest,
) -> Result<BindingWriteOutcome, ErrorCode> {
    write_binding_confirmed(pool, auth, DestructiveOp::MemoryUnpin, request).await
}

/// `memory.bind`, confirmed call (card 22b, ADR-0045): the controlled write path the §25.4.A
/// ruling requires before a TASK/MANDATORY `context_bindings` row can exist at all.
///
/// Mirrors [`pin_confirmed`] exactly except for what the binding step does: the envelope reads
/// the memory's `authority_class` / `memory_type` / Evidence origin basis **in the same
/// transaction** and hands them to `domain::context::authorize_mandatory`, the only way to
/// obtain a `BindingGrant` for `BindingMode::Mandatory`. A TASK + MANDATORY request is not
/// itself an authority proof (§25.4.A(8)).
///
/// # Errors
/// `Forbidden` when §10.1 refuses the memory's own authority/origin; `Conflict` for a
/// replayed/expired/misbound token, a lost BMO race, or a `replaces_binding_id` that is not an
/// active binding of this tenant; `NotFound` when the memory is not visible to the caller.
pub async fn bind_confirmed(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    request: BindingWriteRequest,
) -> Result<BindingWriteOutcome, ErrorCode> {
    write_binding_confirmed(pool, auth, DestructiveOp::MemoryBind, request).await
}

/// `memory.unbind`, confirmed call: revokes the TASK/MANDATORY row only — Evidence/Memory
/// untouched (§36). `Conflict` when nothing is bound.
///
/// # Errors
/// As [`bind_confirmed`].
pub async fn unbind_confirmed(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    request: BindingWriteRequest,
) -> Result<BindingWriteOutcome, ErrorCode> {
    write_binding_confirmed(pool, auth, DestructiveOp::MemoryUnbind, request).await
}

/// The binding step of [`write_binding_confirmed`], past the consumed token and **in the
/// same transaction**: D-C idempotent pin through the sole INSERT site, unpin through the
/// sole revoke site.
// Eight arguments and one long `match`, deliberately. Every argument is a fact the caller
// resolved INSIDE this transaction (tenant, user, the resolved task + its epoch, the existing
// binding, the narrowed scope); bundling them into a struct would move the same eight values
// one line up and cost the compiler's "you forgot one" check at each call site. Splitting the
// `match` per op would scatter "what one confirmed binding write does" across four functions
// whose only shared contract is that they run on THIS transaction.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn apply_binding_write(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    user_id: Uuid,
    op: DestructiveOp,
    request: &BindingWriteRequest,
    existing: Option<Uuid>,
    scope: &Scope,
    // card 22c: resolved from `coord.tasks` in THIS transaction by the caller, carrying the
    // task's current `authorization_epoch`. `None` for the PINNED pair.
    authenticated_task: Option<AuthenticatedTask>,
) -> Result<BindingWriteOutcome, ErrorCode> {
    use humaux_application::pin::{PinAction, pin_action, pin_request, unpin_target};
    match op {
        DestructiveOp::MemoryPin => match pin_action(existing) {
            PinAction::ReturnExisting(binding_id) => Ok(BindingWriteOutcome {
                binding_id,
                inserted: false,
                task_authorized: false,
            }),
            PinAction::Insert => {
                // D-A: the actor exists only past a consumed confirmation for this memory.
                let actor = ConfirmedUserActor::from_consumed_confirmation(op, request.memory)
                    .map_err(rejection)?;
                let grant =
                    authorize_pinned(Some(&actor), pin_request(request.memory, request.workspace))
                        .map_err(rejection)?;
                let binding_id = insert_binding_in_txn(txn, user_id, &grant, tenant_id).await?;
                Ok(BindingWriteOutcome {
                    binding_id,
                    inserted: true,
                    task_authorized: false,
                })
            }
        },
        DestructiveOp::MemoryUnpin => {
            let binding_id = unpin_target(existing)?;
            if !revoke_binding_in_txn(txn, tenant_id, binding_id).await? {
                return Err(ErrorCode::Conflict);
            }
            Ok(BindingWriteOutcome {
                binding_id,
                inserted: false,
                task_authorized: false,
            })
        }
        // card 22b / §25.4.A(8): MANDATORY at a TASK scope. Same confirm gate, different
        // authorization — the memory's own §10.1 standing is re-read here and re-checked by
        // `authorize_mandatory`; the binding itself grants nothing.
        DestructiveOp::MemoryBind => {
            let task = request.task.ok_or(ErrorCode::InvalidInput)?;
            let authenticated_task = authenticated_task.ok_or(ErrorCode::Internal)?;
            // card 22c: `purpose` is required on bind. A missing purpose is INVALID_INPUT, not
            // a default — defaulting it either way is a policy decision made by an omission.
            let purpose = request.purpose.ok_or(ErrorCode::InvalidInput)?;
            // §25.4.A(9): the authorized A->B replacement revokes A in THIS transaction, before
            // B's row is created — scoped to the (TASK = this task, MANDATORY) dimension this
            // operation is authorized for, so the wire argument cannot reach a PINNED row or
            // another task's binding (see `revoke_task_mandatory_binding_in_txn`). Zero rows
            // affected = already revoked, or outside that dimension: `Conflict`, never a silent
            // continue. Replacing a binding with itself is refused before the UPDATE — it would
            // otherwise revoke the very row the idempotent `ReturnExisting` arm below reports
            // back as `state: "bound"`.
            if let Some(replaced) = request.replaces_binding_id {
                if existing == Some(replaced) {
                    return Err(ErrorCode::Conflict);
                }
                if !revoke_task_mandatory_binding_in_txn(txn, tenant_id, task, replaced).await? {
                    return Err(ErrorCode::Conflict);
                }
                // card 22c: the replaced binding's authorization goes with it. I-NONINHERIT's
                // "授权替换在一个事务内撤销旧绑定并建立新绑定" — the new binding gets a NEW
                // grant below (or none), never the old one redirected.
                revoke_task_binding_grant_in_txn(txn, tenant_id, replaced).await?;
            }
            match pin_action(existing) {
                // Idempotent, same rule as pin (ADR-0019 D-C): already bound is not an error.
                // card 22c: read the authorization back rather than echoing the request —
                // an already-bound REFERENCE_ONLY row does not become authorized because the
                // second call asked for ADOPT_TASK_INSTRUCTION, and saying otherwise would be
                // the API reporting an authorization nobody wrote.
                PinAction::ReturnExisting(binding_id) => Ok(BindingWriteOutcome {
                    binding_id,
                    inserted: false,
                    task_authorized: active_task_grant_in_txn(txn, tenant_id, binding_id).await?,
                }),
                PinAction::Insert => {
                    // D-A: the actor exists only past a consumed confirmation for this memory.
                    let actor = ElevatedActor::from_consumed_confirmation(op, request.memory)
                        .map_err(rejection)?;
                    let (authority, memory_type, basis) =
                        mandatory_binding_facts_in_txn(txn, tenant_id, request.memory).await?;
                    // Read now, applied after `authorize_mandatory` (the ruling §四.4 order):
                    // `basis` is moved into the policy call below, and re-reading it from the DB
                    // a second time would make the two checks answerable by two different rows.
                    let behavior_eligible =
                        humaux_domain::context::require_behavior_eligible_target(basis.as_slice());
                    let grant = authorize_mandatory(
                        &humaux_domain::policy::OriginBoundAuthorityPolicy,
                        &actor,
                        BindingRequest {
                            mode: BindingMode::Mandatory,
                            scope_kind: ScopeKind::Task,
                            scope_id: Some(task.0),
                            memory_id: request.memory,
                        },
                        authority,
                        memory_type,
                        basis,
                        scope,
                    )
                    .map_err(rejection)?;
                    // 裁决 §四.4 write order: `authorize_mandatory -> require_behavior_eligible_target
                    // -> insert_binding_and_task_grant`. Only the ADOPT_TASK_INSTRUCTION branch
                    // mints an authorization, so only it carries the §10.1 row 4/5 obligation:
                    // a REFERENCE_ONLY binding asserts nothing about behaviour eligibility and
                    // the read side rejects a DATA_ONLY target either way.
                    if matches!(purpose, BindingPurpose::AdoptTaskInstruction) {
                        behavior_eligible
                            .map_err(humaux_domain::context::TaskContextReject::error_code)?;
                    }
                    let binding_id = insert_binding_in_txn(txn, user_id, &grant, tenant_id).await?;
                    // card 22c / ADR-0046: binding and authorization are written in ONE
                    // transaction, and ONLY for ADOPT_TASK_INSTRUCTION. REFERENCE_ONLY stops
                    // here with a binding and no grant — that pair IS the negative control.
                    if matches!(purpose, BindingPurpose::AdoptTaskInstruction) {
                        // The complete intent, digested into the authorization Evidence. It
                        // names the exact content through the same expression the grant and the
                        // read side use, so the receipt cannot describe a different approval
                        // than the one the grant row records.
                        let payload_hex: String = sqlx::query_scalar(&format!(
                            "SELECT encode({CANONICAL_PAYLOAD_SHA256_EXPR}, 'hex')                                FROM private.memory_records m                               WHERE m.tenant_id = $1 AND m.memory_id = $2"
                        ))
                        .bind(tenant_id)
                        .bind(request.memory.0)
                        .fetch_optional(&mut **txn)
                        .await
                        .map_err(|_| ErrorCode::Internal)?
                        .ok_or(ErrorCode::Conflict)?;
                        let intent = format!(
                            "{}|{}|{}|{}|{}|{}|{}|{}",
                            TASK_AUTHORIZATION_POLICY_VERSION,
                            tenant_id,
                            authenticated_task.task_id(),
                            authenticated_task.authorization_epoch(),
                            binding_id,
                            request.memory.0,
                            payload_hex,
                            BindingPurpose::AdoptTaskInstruction.wire(),
                        );
                        let evidence_id = insert_task_authorization_evidence_in_txn(
                            txn,
                            tenant_id,
                            user_id,
                            request.memory,
                            &intent,
                        )
                        .await?;
                        insert_task_binding_grant_in_txn(
                            txn,
                            &authenticated_task,
                            binding_id,
                            request.memory,
                            user_id,
                            evidence_id,
                            request.request_id,
                        )
                        .await?;
                    }
                    Ok(BindingWriteOutcome {
                        binding_id,
                        inserted: true,
                        task_authorized: active_task_grant_in_txn(txn, tenant_id, binding_id)
                            .await?,
                    })
                }
            }
        }
        DestructiveOp::MemoryUnbind => {
            let binding_id = unpin_target(existing)?;
            if !revoke_binding_in_txn(txn, tenant_id, binding_id).await? {
                return Err(ErrorCode::Conflict);
            }
            // card 22c: unbind revokes the authorization too, in the same transaction. Leaving
            // a live grant behind a revoked binding would keep an authorization alive with no
            // obligation to observe it — the exact shape I-NONINHERIT's revocation arm forbids.
            revoke_task_binding_grant_in_txn(txn, tenant_id, binding_id).await?;
            Ok(BindingWriteOutcome {
                binding_id,
                inserted: false,
                task_authorized: false,
            })
        }
        DestructiveOp::MemorySupersede
        | DestructiveOp::MemoryRestore
        | DestructiveOp::MemoryArchive
        | DestructiveOp::MemoryUnarchive
        | DestructiveOp::MemoryCorrect
        | DestructiveOp::MemoryConfirm
        | DestructiveOp::MemoryReject => Err(ErrorCode::InvalidInput),
    }
}

/// One `role_gateway` transaction, in this order: reserve BMO -> quota audit -> **consume the
/// confirm token** (0 rows = `Conflict`, nothing below runs) -> visibility of the memory
/// (`readable_memory_ids`, the workspace's own `can_read` judge) -> the binding write ->
/// quota CONSUMED -> audits -> COMMIT.
///
/// The binding write goes through the two sole writers [`insert_binding_in_txn`] /
/// [`revoke_binding_in_txn`] **on this transaction** (D-B, same shape as
/// `memory_governance_repo::supersede_atomically`): one pooled connection for the whole
/// envelope, and every later rejection — reservation not `Consumed`, lease expired at
/// finalize, COMMIT failure — rolls the binding row back together with the token consume and
/// the audits. No path leaves a PINNED row changed without a consumed confirmation.
async fn write_binding_confirmed(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    op: DestructiveOp,
    request: BindingWriteRequest,
) -> Result<BindingWriteOutcome, ErrorCode> {
    validate_binding_write(auth, op, &request)?;
    let user_id = auth.user_id().ok_or(ErrorCode::Unauthorized)?.0;
    let tenant_id = auth.tenant_id().0;
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    set_authorization_local(&mut txn, auth).await?;

    let reservation = match quota_repo::reserve_bmo_in_txn(
        &mut txn,
        auth,
        request.request_id,
        op.operation_key(),
        &request.request_fingerprint,
        request.reservation_ttl,
    )
    .await?
    {
        ReserveResult::Created(reservation) => reservation,
        ReserveResult::Existing(_) => return Err(ErrorCode::Conflict),
    };
    let mut quota_audit = request.finished_audit.clone();
    quota_audit.event_id = AuditEventId::new();
    quota_audit.action = McpAuditAction::McpQuotaReserved.as_str().to_owned();
    request_guard_repo::audit_event_insert_in_txn(
        &mut txn,
        AuditTenant::Authenticated(auth),
        &quota_audit,
    )
    .await?;

    // Token first: a replayed/expired/misbound token must never reach the binding writers.
    confirm_token_repo::consume_in_txn(&mut txn, auth, &request.claim).await?;

    if !readable_memory_ids(&mut txn, auth, &[request.memory.0])
        .await?
        .contains(&request.memory.0)
    {
        return Err(ErrorCode::NotFound);
    }
    let mut authenticated_task = None;
    let existing = match op {
        DestructiveOp::MemoryBind | DestructiveOp::MemoryUnbind => {
            let task = request.task.ok_or(ErrorCode::InvalidInput)?;
            // §25.4.A(7): the TaskId must resolve to a task of this tenant before a binding is
            // written under it — a wire uuid naming nothing is `NotFound`, not a new task scope.
            // card 22c: the same resolution also produces the task's current authorization
            // epoch, which is what the grant is stamped with.
            authenticated_task = Some(resolve_task_in_txn(&mut txn, tenant_id, task).await?);
            active_task_mandatory_binding_in_txn(&mut txn, tenant_id, task, request.memory).await?
        }
        _ => {
            active_pinned_binding_in_txn(&mut txn, tenant_id, request.workspace, request.memory)
                .await?
        }
    };
    let scope = Scope {
        tenant_id: auth.tenant_id(),
        user_id: auth.user_id(),
        workspace_id: Some(request.workspace),
        repository_id: None,
        task_id: request.task,
        run_id: None,
        agent_id: None,
    };
    let outcome = apply_binding_write(
        &mut txn,
        tenant_id,
        user_id,
        op,
        &request,
        existing,
        &scope,
        authenticated_task,
    )
    .await?;

    if quota_repo::finish_reservation_in_txn(&mut txn, auth, &reservation, true).await?
        != ReservationStatus::Consumed
    {
        return Err(ErrorCode::Conflict);
    }
    quota_audit.event_id = AuditEventId::new();
    quota_audit.action = McpAuditAction::McpQuotaConsumed.as_str().to_owned();
    request_guard_repo::audit_event_insert_in_txn(
        &mut txn,
        AuditTenant::Authenticated(auth),
        &quota_audit,
    )
    .await?;
    request_guard_repo::audit_event_insert_in_txn(
        &mut txn,
        AuditTenant::Authenticated(auth),
        &request.finished_audit,
    )
    .await?;
    let finalized_at: sqlx::types::time::OffsetDateTime =
        sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *txn)
            .await
            .map_err(|_| ErrorCode::Internal)?;
    if finalized_at >= reservation.expires_at() {
        return Err(ErrorCode::Conflict);
    }
    // A COMMIT error cannot establish rollback (§34.0.1); the caller reports retryable.
    txn.commit()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(outcome)
}

/// [`humaux_application::continuity::ContextReadPort`] 的生产实现——委托
/// [`fetch_frozen`]。application 依赖方向不许反转（它不能 import 本 crate），
/// 端口在那边、实现在这边（`PrivateReasoningPort` 同款先例）。
pub struct ContextReadAdapter {
    pool: std::sync::Arc<RuntimeDbPool>,
    authorization: AuthorizationScope,
}

impl ContextReadAdapter {
    /// 构造。
    #[must_use]
    pub fn new(
        pool: impl Into<std::sync::Arc<RuntimeDbPool>>,
        authorization: AuthorizationScope,
    ) -> Self {
        Self {
            pool: pool.into(),
            authorization,
        }
    }
}

#[async_trait::async_trait]
impl humaux_application::continuity::ContextReadPort for ContextReadAdapter {
    async fn fetch_frozen(
        &self,
        scope: &Scope,
    ) -> Result<humaux_domain::context::FrozenReads, ErrorCode> {
        fetch_frozen(&self.pool, &self.authorization, scope).await
    }
}
