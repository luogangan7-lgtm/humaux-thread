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
    Admitted, BindingGrant, FrozenReads, MandatoryLane, MandatoryRow, PinnedLane, ScopeKind,
    SelectorId, SelectorOutcome, SelectorSpec, spec,
};
use humaux_domain::error::ErrorCode;
use humaux_domain::grounding::{GroundingMode, RowGrounding, SnapshotEdge, classify_in_snapshot};
use humaux_domain::ids::Scope;
use sha2::{Digest, Sha256};
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

    // ② 取行——每行带两个快照内 grounding 事实（有没有 LIVE edge / 有没有未记版本的
    //    LIVE edge），喂 `classify_in_snapshot`。resolver 永不进本事务。
    let rows = sqlx::query(&format!(
        "SELECT m.memory_id, m.authority_class, {EST_TOKENS_EXPR} AS est_tokens, \
                EXISTS(SELECT 1 FROM private.memory_evidence me \
                       WHERE me.memory_id = m.memory_id AND me.grounding_mode = 'LIVE') \
                  AS has_live, \
                EXISTS(SELECT 1 FROM private.memory_evidence me \
                       WHERE me.memory_id = m.memory_id AND me.grounding_mode = 'LIVE' \
                         AND me.recorded_version IS NULL) \
                  AS has_live_unversioned \
         FROM private.memory_records m WHERE {where_clause} ORDER BY m.memory_id"
    ))
    .bind(tenant_id)
    .bind(workspaces)
    .bind(user_id)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;

    let mut out = Vec::with_capacity(rows.len());
    let mut needs = Vec::new();
    for r in rows {
        let memory_id: Uuid = r.try_get("memory_id").map_err(|_| ErrorCode::Internal)?;
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
        expected: u64::try_from(expected).unwrap_or(0),
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
/// selector 的 COUNT + 取行 → pinned 的独立 COUNT + 取行 + excluded 具名 → 快照身份。
///
/// 此前这里是三个各自开事务的 pub 函数（probe / mandatory / pinned）——READ COMMITTED
/// 下每条语句各看各的快照，probe 与取数之间还有 TOCTOU；「两次装配逐字节相同」在那个
/// 形状下无从谈起。三函数已并入本函数（probe 保留 pub 供独立探测）。
///
/// # Errors
/// 库不可达、authority 线值不在闭集内 ⇒ [`ErrorCode::Internal`]。
pub async fn fetch_frozen(pool: &RuntimeDbPool, scope: &Scope) -> Result<FrozenReads, ErrorCode> {
    let workspaces = workspace_ids(scope);
    let user_id = scope.user_id.map(|u| u.0);
    let tenant_id = scope.tenant_id.0;

    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    // 必须是本事务第一条语句：隔离级在第一个取快照的语句之后就改不了了。
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *txn)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    set_tenant_local(&mut txn, tenant_id)
        .await
        .map_err(|_| ErrorCode::Internal)?;

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
            .fetch_one(&mut *txn)
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
        out.push(run_selector(&mut txn, sp, where_clause, tenant_id, &workspaces, user_id).await?);
    }
    let outcomes: [SelectorOutcome; 5] = out.try_into().map_err(|_| ErrorCode::Internal)?;
    let mandatory = MandatoryLane::from_selectors(outcomes);

    let pinned = fetch_pinned_in_txn(&mut txn, tenant_id).await?;

    // 快照身份：同一事务内取。seq（xmin）是 fingerprint 轴——必要非充分；
    // token（完整 snapshot）才是「同快照 ⇒ 同字节」的充分条件（见 FrozenReads doc）。
    let row = sqlx::query(
        "SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint AS seq, \
                pg_current_snapshot()::text AS token",
    )
    .fetch_one(&mut *txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    let seq: i64 = row.try_get("seq").map_err(|_| ErrorCode::Internal)?;
    let token: String = row.try_get("token").map_err(|_| ErrorCode::Internal)?;
    let digest = Sha256::digest(token.as_bytes());
    let snapshot_token_sha256 = digest
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();

    txn.commit().await.map_err(|_| ErrorCode::Internal)?;

    Ok(FrozenReads {
        mandatory,
        pinned,
        context_snapshot_seq: seq,
        snapshot_token_sha256,
    })
}

/// [`fetch_frozen`] 的 pinned 半边：独立 COUNT（外部 oracle——「钉 3 带 2」必须可观测）
/// + 取行 + excluded 具名。抽成函数只为行数闸，语义与内联时逐字相同。
async fn fetch_pinned_in_txn(txn: &mut Txn<'_>, tenant_id: Uuid) -> Result<PinnedLane, ErrorCode> {
    // pinned：独立 COUNT（外部 oracle——「钉 3 带 2」必须可观测）+ 取行 + excluded 具名。
    let pinned_expected: i64 = sqlx::query(
        "SELECT count(*) FROM private.memory_records m \
         WHERE m.tenant_id = $1 AND m.status = 'active' AND m.superseded_by IS NULL \
           AND EXISTS ( \
             SELECT 1 FROM private.context_bindings cb \
             WHERE cb.memory_id = m.memory_id AND cb.tenant_id = m.tenant_id \
               AND cb.revoked_at IS NULL AND cb.mode = 'PINNED' \
           )",
    )
    .bind(tenant_id)
    .fetch_one(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?
    .try_get(0)
    .map_err(|_| ErrorCode::Internal)?;

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
         WHERE m.tenant_id = $1 AND m.status = 'active' AND m.superseded_by IS NULL \
           AND EXISTS ( \
             SELECT 1 FROM private.context_bindings cb \
             WHERE cb.memory_id = m.memory_id AND cb.tenant_id = m.tenant_id \
               AND cb.revoked_at IS NULL AND cb.mode = 'PINNED' \
           ) \
         ORDER BY m.memory_id"
    ))
    .bind(tenant_id)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;

    let sp = spec(SelectorId::ExplicitMandatoryBindingsV1);
    let mut pinned_rows = Vec::with_capacity(rows.len());
    let mut excluded = Vec::new();
    for r in rows {
        let memory_id: Uuid = r.try_get("memory_id").map_err(|_| ErrorCode::Internal)?;
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
        u64::try_from(pinned_expected).unwrap_or(0),
        pinned_rows,
        excluded,
    ))
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
