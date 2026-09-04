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
//! **每个 selector 发两次独立的无 `LIMIT` 枚举**：第一次得到 `expected` 的候选集合，第二次
//! 投影 lane 行。两次都经过同一个 `can_read` 回验，但不能从投影行的长度反推 expected；否则
//! `expected == returned + missing` 会退化成恒真算术，少带了多少永远算作 0。

use humaux_domain::audit::{AuditEvent, AuditEventId, McpAuditAction};
use humaux_domain::authority::{AuthorityClass, MemoryId};
use humaux_domain::confirm::{DestructiveOp, RISK_TAG_CONFIRMATION_MINTED};
use humaux_domain::context::{
    Admitted, BindingGrant, ConfirmedUserActor, ContextBudget, FrozenReads, MandatoryLane,
    MandatoryRow, PinnedLane, SelectorId, SelectorOutcome, SelectorSpec, authorize_pinned, spec,
};
use humaux_domain::error::ErrorCode;
use humaux_domain::grounding::{GroundingMode, RowGrounding, SnapshotEdge, classify_in_snapshot};
use humaux_domain::identity::{
    AuthorizationScope, VisibilityClass, VisibilityDescriptor, can_read,
};
use humaux_domain::ids::{Scope, UserId, WorkspaceId};
use humaux_domain::selection::{AUTHORIZED_MEMORY_ENUMERATION_V1, Cursor, query_fingerprint};
use humaux_projection::serving::StreamFamily;
use humaux_projection::stream::StreamKey;
use humaux_retrieval::compiler::{ContextItem, ContextOutcome};
use humaux_retrieval::envelope::GroundingBlock;
use humaux_retrieval::handoff::{Handoff, assemble_with_context};
use sha2::{Digest, Sha256};
use sqlx::Row;
use sqlx::types::Uuid;
use std::collections::{HashMap, HashSet};

use crate::confirm_token_repo::{self, ConfirmationClaim};
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
    if requested.tenant_id != authorization.tenant_id()
        || requested.user_id != authorization.user_id()
        || requested.repository_id.is_some()
        || requested.task_id.is_some()
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
            task_id: None,
            run_id: None,
            agent_id: None,
        },
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

async fn selector_candidate_ids(
    txn: &mut Txn<'_>,
    s: &'static SelectorSpec,
    where_clause: &str,
    scope: &Scope,
) -> Result<Vec<Uuid>, ErrorCode> {
    let sql = format!("SELECT m.memory_id FROM private.memory_records m WHERE {where_clause}");
    let rows = if s.id == SelectorId::ExplicitMandatoryBindingsV1 {
        let (kinds, ids) = scope_chain_params(scope);
        sqlx::query(&sql)
            .bind(scope.tenant_id.0)
            .bind(kinds)
            .bind(ids)
            .fetch_all(&mut **txn)
            .await
    } else {
        sqlx::query(&sql)
            .bind(scope.tenant_id.0)
            .fetch_all(&mut **txn)
            .await
    }
    .map_err(|_| ErrorCode::Internal)?;
    rows.into_iter()
        .map(|row| row.try_get("memory_id").map_err(|_| ErrorCode::Internal))
        .collect()
}

/// Runs one selector's independent candidate enumeration and row projection. The former
/// remains the expected-count oracle; both are filtered by the single Rust `can_read` policy.
async fn run_selector(
    txn: &mut Txn<'_>,
    s: &'static SelectorSpec,
    where_clause: &str,
    authorization: &AuthorizationScope,
    scope: &Scope,
) -> Result<SelectorOutcome, ErrorCode> {
    let expected_candidates = selector_candidate_ids(txn, s, where_clause, scope).await?;
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
    let rows = if s.id == SelectorId::ExplicitMandatoryBindingsV1 {
        let (kinds, ids) = scope_chain_params(scope);
        sqlx::query(&sql)
            .bind(scope.tenant_id.0)
            .bind(kinds)
            .bind(ids)
            .fetch_all(&mut **txn)
            .await
    } else {
        sqlx::query(&sql)
            .bind(scope.tenant_id.0)
            .fetch_all(&mut **txn)
            .await
    }
    .map_err(|_| ErrorCode::Internal)?;

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

/// §25.4 装配的**全部冻结读数**，单事务取齐——G80-31「同一 `context_snapshot_seq` 两次
/// 装配逐字节相同」的取数半边。
///
/// 一个 `REPEATABLE READ` 事务（`consolidate_repo` 的 §11.7 同款配方，只读路径不带
/// `READ WRITE`），依次：隔离级 → 租户上下文 → probe（`information_schema` 进同快照；
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
    // probe（同快照）。
    let mut availability: Vec<(SelectorId, Option<String>)> = Vec::with_capacity(5);
    for sp in &humaux_domain::context::REGISTRY {
        let mut missing: Option<String> = None;
        for (schema, table, column) in sp.required_columns {
            let exists: bool = sqlx::query(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 AND column_name = $3)",
            )
            .bind(schema)
            .bind(table)
            .bind(column)
            .fetch_one(&mut **txn)
            .await
            .map_err(|_| ErrorCode::Internal)?
            .try_get(0)
            .map_err(|_| ErrorCode::Internal)?;
            if !exists {
                missing = Some(format!("{schema}.{table}.{column}"));
                break;
            }
        }
        availability.push((sp.id, missing));
    }

    // mandatory 五个 selector。
    let mut out: Vec<SelectorOutcome> = Vec::with_capacity(5);
    for (id, missing) in availability {
        if let Some(missing_object) = missing {
            out.push(SelectorOutcome::Unavailable { id, missing_object });
            continue;
        }
        let sp = spec(id);
        let where_clause = match id {
            SelectorId::ProjectActiveConstraintsV1 => PROJECT_CONSTRAINTS_WHERE,
            SelectorId::UserConfirmedCorrectionsV1 => USER_CORRECTIONS_WHERE,
            SelectorId::ExplicitMandatoryBindingsV1 => EXPLICIT_BINDINGS_WHERE,
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
        out.push(run_selector(txn, sp, where_clause, &authorization, &scope).await?);
    }
    let outcomes: [SelectorOutcome; 5] = out.try_into().map_err(|_| ErrorCode::Internal)?;
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
}

/// One immutable manifest page whose body, grounding and ledger share one PostgreSQL snapshot.
pub struct MaterializedMemoryPage {
    pub snapshot_id: Uuid,
    pub next_cursor: Option<String>,
    pub memory: MaterializedMemory,
}

fn enumeration_fingerprint(authorization: &AuthorizationScope, scope: &Scope) -> String {
    let workspace = scope
        .workspace_id
        .map(|id| id.0.to_string())
        .unwrap_or_default();
    let user = authorization
        .user_id()
        .map(|id| id.0.to_string())
        .unwrap_or_default();
    query_fingerprint(
        &format!(
            "{AUTHORIZED_MEMORY_ENUMERATION_V1}:{}:{}:{}",
            authorization.principal().0,
            user,
            workspace
        ),
        authorization.tenant_id().0,
    )
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
/// Materializes an authorization-bound immutable Memory page.
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
    let fingerprint = enumeration_fingerprint(&authorization, &scope);
    let mut txn = pool
        .pool()
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
    let page = if let Some(encoded) = params.cursor {
        let cursor = Cursor::decode(encoded).map_err(|_| ErrorCode::InvalidInput)?;
        fetch_authorized_snapshot_page_in_txn(
            &mut txn,
            authorization.tenant_id().0,
            &cursor,
            &fingerprint,
            params.page_size as i64,
            params.mac_key,
        )
        .await?
    } else {
        let rows = sqlx::query("SELECT memory_id FROM private.memory_records WHERE tenant_id=$1 AND status='active' AND superseded_by IS NULL AND archived_at IS NULL ORDER BY memory_id DESC")
            .bind(authorization.tenant_id().0).fetch_all(&mut *txn).await.map_err(|_| ErrorCode::DependencyUnavailable)?;
        let candidates = rows
            .into_iter()
            .map(|row| row.try_get("memory_id").map_err(|_| ErrorCode::Internal))
            .collect::<Result<Vec<Uuid>, ErrorCode>>()?;
        let ids = final_memory_ids_in_txn(&mut txn, &authorization, &candidates, false).await?;
        begin_authorized_snapshot_in_txn(
            &mut txn,
            authorization.tenant_id().0,
            &fingerprint,
            params.ttl,
            params.page_size as i64,
            params.mac_key,
            &ids,
        )
        .await?
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
    let ledger = close_ledger_in_txn(&mut txn, validated_key)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
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
            // Enumerate excludes archived rows at the candidate query (D-C), so a page never
            // carries one; the flag is meaningful only on memory.get.
            archived: false,
        },
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
    txn.commit()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(MaterializedContext {
        handoff,
        outcome,
        bodies,
        ledger,
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

/// [`revoke_binding_in_txn`] in its own short transaction (non-gated callers).
///
/// # Errors
/// As [`revoke_binding_in_txn`].
pub async fn revoke_binding(
    pool: &RuntimeDbPool,
    tenant_id: Uuid,
    binding_id: Uuid,
) -> Result<bool, ErrorCode> {
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
    pub claim: ConfirmationClaim,
    pub finished_audit: AuditEvent,
}

/// Result of a confirmed pin (D-C: idempotent) or unpin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindingWriteOutcome {
    pub binding_id: Uuid,
    /// pin: `false` when the row already existed (nothing inserted). unpin: always `false`.
    pub inserted: bool,
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
    humaux_application::pin::check_claim(
        op,
        request.claim.op,
        request.claim.target_id,
        request.claim.successor_id,
        request.memory,
    )?;
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

/// The binding step of [`write_binding_confirmed`], past the consumed token and **in the
/// same transaction**: D-C idempotent pin through the sole INSERT site, unpin through the
/// sole revoke site.
async fn apply_binding_write(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    user_id: Uuid,
    op: DestructiveOp,
    request: &BindingWriteRequest,
    existing: Option<Uuid>,
) -> Result<BindingWriteOutcome, ErrorCode> {
    use humaux_application::pin::{PinAction, pin_action, pin_request, unpin_target};
    match op {
        DestructiveOp::MemoryPin => match pin_action(existing) {
            PinAction::ReturnExisting(binding_id) => Ok(BindingWriteOutcome {
                binding_id,
                inserted: false,
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
            })
        }
        DestructiveOp::MemorySupersede
        | DestructiveOp::MemoryRestore
        | DestructiveOp::MemoryArchive
        | DestructiveOp::MemoryUnarchive => Err(ErrorCode::InvalidInput),
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
    let existing =
        active_pinned_binding_in_txn(&mut txn, tenant_id, request.workspace, request.memory)
            .await?;
    let outcome = apply_binding_write(&mut txn, tenant_id, user_id, op, &request, existing).await?;

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
