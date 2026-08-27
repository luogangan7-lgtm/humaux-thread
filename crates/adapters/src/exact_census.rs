//! `adapters::exact_census` — §22.1 EXACT enumeration census: the physical IO behind
//! `completeness::CensusResult`.
//!
//! Split of responsibilities (frozen by the pure crates' own docs):
//! - `retrieval::predicate_registry` validates rows but "does not import sqlx or issue any
//!   query itself" — the census IO lives here.
//! - `retrieval::planner::decide` adjudicates §22.4 triggers 1–3 (no registry row / columns
//!   unindexed / scope not enumerable) from two caller-supplied probe snapshots — produced
//!   here by [`probe_predicate_inputs`].
//! - This module observes only trigger 4 (the census itself failing) plus the §22.1 readout
//!   when it succeeds; `classify()` stays the sole class constructor.
//!
//! §22.1's three disciplines, each landing on one concrete choice below:
//! - "`total` 必须来自 `SELECT count(*) FROM <enumerable_scope> AND <sql_predicate>`，与返回
//!   项取**同一事务快照**" ⇒ one `REPEATABLE READ` transaction for all three statements
//!   (`context_repo::fetch_frozen`'s recipe).
//! - "禁止用召回条数冒充 `total`" ⇒ `total` is its own `count(*)`, never derived from the
//!   returned id list; `ExactEnumeration::new` additionally rejects any overrun.
//! - "`excluded_secret` … > 0 时 `coverage` 相应扣减，不许静默少给" ⇒ secret-linked rows stay
//!   in `total`, leave `returned`, and are counted by their own third statement.
//!
//! §23.4's deletion-tour requirement ("被 tombstone 的那 10 条一次都不许出现在 `items` 里 ——
//! 包括第 5 步物理 purge 还没跑的那些采样点") is why every statement carries the tombstone
//! overlay: a memory whose evidence's stream entry is `TOMBSTONED` leaves the EXACT channel
//! (denominator *and* items) the moment `retention::tombstone` commits — same one-predicate-
//! two-faces overlay §23.1② freezes, joined here via `private.memory_evidence` →
//! `ops.outbox(evidence_id → stream_seq)` → `projection.stream_log(state)` under the caller's
//! [`StreamKey`] (`ops.outbox` is written in the same transaction as the seq issuance, §60, so
//! the mapping shares the write's atomicity).

use std::collections::BTreeSet;

use humaux_domain::error::ErrorCode;
use humaux_projection::stream::StreamKey;
use humaux_retrieval::completeness::{CensusResult, ExactEnumeration};
use humaux_retrieval::predicate_registry::PredicateEntry;
use sqlx::Row;
use sqlx::types::Uuid;

use crate::postgres::RuntimeDbPool;

type Txn<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

/// RLS 的请求上下文：tenant + (§6.1.1) user。`humaux.user_id` is what admits
/// `WORKSPACE_SHARED` rows through 0012's membership branch — a census run without it sees
/// only `TENANT_SHARED`, which is a *different authorized universe*, not an error (§22.4:
/// "EXACT 应优先从 PostgreSQL 权威数据在当前授权快照内完成" — the user context IS the
/// authorization snapshot).
async fn set_request_context(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    user_id: Option<Uuid>,
) -> Result<(), sqlx::Error> {
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .execute(&mut **txn)
        .await?;
    if let Some(user_id) = user_id {
        sqlx::query(&format!("SET LOCAL humaux.user_id = '{user_id}'"))
            .execute(&mut **txn)
            .await?;
    }
    Ok(())
}

/// The `FROM` face of an `enumerable_scope` fragment, split into probe-able parts.
/// `None` = the fragment does not parse as `schema.table WHERE …` — which is §22.4 trigger 3's
/// shape (a mixed/foreign-domain scope has no single probe-able table), so the caller simply
/// leaves it out of `enumerable_scopes` and `decide()` refuses it.
fn scope_table(scope: &str) -> Option<(&str, &str)> {
    let from = scope.split(" WHERE ").next()?.trim();
    let (schema, table) = from.split_once('.')?;
    let ident = |s: &str| {
        !s.is_empty()
            && s.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    };
    (ident(schema) && ident(table)).then_some((schema, table))
}

/// Builds `decide()`'s two probe snapshots (§20.2 rule 3, both conjuncts) for one candidate
/// predicate — probed from the live catalogs, never hardcoded (ADR-0006's probing discipline).
///
/// A required column counts as indexed if some index on the scope table carries it either as
/// a key column **or** inside its partial-index predicate: `migrations/0078`'s
/// `idx_memory_records_rejected_decisions_v1` puts `memory_type`/`superseded_at` in the
/// `WHERE` clause and only `visibility_workspace_id` in the key — an indkey-only probe would
/// report the registry's own seeded predicate unenumerable (a probe that fails its one real
/// positive is measuring nothing).
///
/// # Errors
/// [`ErrorCode::Internal`] when the catalog probe itself cannot run.
pub async fn probe_predicate_inputs(
    pool: &RuntimeDbPool,
    entry: &PredicateEntry,
) -> Result<(BTreeSet<String>, BTreeSet<String>), ErrorCode> {
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;

    let mut indexed: BTreeSet<String> = BTreeSet::new();
    let mut scopes: BTreeSet<String> = BTreeSet::new();

    if let Some((schema, table)) = scope_table(entry.enumerable_scope()) {
        let table_exists: bool = sqlx::query(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = $2)",
        )
        .bind(schema)
        .bind(table)
        .fetch_one(&mut *txn)
        .await
        .map_err(|_| ErrorCode::Internal)?
        .try_get(0)
        .map_err(|_| ErrorCode::Internal)?;

        if table_exists {
            scopes.insert(entry.enumerable_scope().to_string());
            for col in entry.required_columns() {
                let covered: bool = sqlx::query(
                    "SELECT EXISTS ( \
                       SELECT 1 FROM pg_index i \
                       JOIN pg_class c ON c.oid = i.indrelid \
                       JOIN pg_namespace n ON n.oid = c.relnamespace \
                       WHERE n.nspname = $1 AND c.relname = $2 \
                         AND ( \
                           EXISTS (SELECT 1 FROM pg_attribute a \
                                   WHERE a.attrelid = c.oid \
                                     AND a.attnum = ANY(i.indkey) \
                                     AND a.attname = $3) \
                           OR COALESCE(pg_get_expr(i.indpred, i.indrelid), '') \
                              ~ ('\\m' || $3 || '\\M') \
                         ) \
                     )",
                )
                .bind(schema)
                .bind(table)
                .bind(col)
                .fetch_one(&mut *txn)
                .await
                .map_err(|_| ErrorCode::Internal)?
                .try_get(0)
                .map_err(|_| ErrorCode::Internal)?;
                if covered {
                    indexed.insert(col.clone());
                }
            }
        }
    }

    txn.commit().await.map_err(|_| ErrorCode::Internal)?;
    Ok((indexed, scopes))
}

/// [`exact_enumerate`]'s return: the census verdict plus the returned item ids (sorted by
/// `memory_id` — a deterministic order so two runs over the same snapshot compare bytewise).
#[derive(Debug, Clone, PartialEq)]
pub struct ExactCensusOutcome {
    pub census: CensusResult,
    /// Empty whenever `census` is not `enumerated` — a failed census returns no items
    /// (§22.4: the count that survives is `known_lower_bound`, carried inside the
    /// enumeration when one exists, never a bare id list).
    pub returned_ids: Vec<Uuid>,
}

/// §23.1② overlay, EXACT-channel face (see module doc): correlated on the outer
/// `memory_records` row, scoped to one [`StreamKey`] via `$3..$7` (the registry's
/// post-0079 `enumerable_scope` already claims `$1` = tenant and `$2` = workspace).
const NOT_TOMBSTONED: &str = "NOT EXISTS ( \
   SELECT 1 FROM private.memory_evidence me \
   JOIN ops.outbox ob ON ob.evidence_id = me.evidence_id \
   JOIN projection.stream_log sl \
     ON sl.tenant_id = ob.tenant_id AND sl.scope_kind = $3 AND sl.scope_id = $4 \
    AND sl.domain = $5 AND sl.projection_kind = $6 AND sl.projection_version = $7 \
    AND sl.stream_seq = ob.stream_seq \
   WHERE me.memory_id = memory_records.memory_id AND sl.state = 'TOMBSTONED')";

/// §18/§22.1 `SECRET_MATERIAL` linkage: a memory whose evidence chain carries secret material
/// is excluded from `returned` and counted by `excluded_secret`.
const SECRET_LINKED: &str = "EXISTS ( \
   SELECT 1 FROM private.memory_evidence me2 \
   JOIN private.evidence_objects eo ON eo.evidence_id = me2.evidence_id \
   WHERE me2.memory_id = memory_records.memory_id \
     AND eo.data_class = 'SECRET_MATERIAL')";

/// §22.4 trigger 4 exit: closes the (read-only) transaction and hands back the failed-census
/// verdict for `classify()` to render, instead of surfacing a transport error.
async fn census_failed(txn: Txn<'_>) -> Result<ExactCensusOutcome, ErrorCode> {
    // Commit rather than roll back: the transaction only read.
    let _ = txn.commit().await;
    Ok(ExactCensusOutcome {
        census: CensusResult::failed(),
        returned_ids: Vec::new(),
    })
}

async fn census_count(txn: &mut Txn<'_>, sql: &str, args: &CensusArgs<'_>) -> sqlx::Result<i64> {
    sqlx::query(sql)
        .bind(args.tenant_id)
        .bind(args.workspace_id)
        .bind(&args.stream.scope_kind)
        .bind(args.stream.scope_id)
        .bind(&args.stream.domain)
        .bind(&args.stream.projection_kind)
        .bind(&args.stream.projection_version)
        .fetch_one(&mut **txn)
        .await?
        .try_get(0)
}

struct CensusArgs<'a> {
    tenant_id: Uuid,
    workspace_id: Uuid,
    stream: &'a StreamKey,
}

/// Runs the §22.1 census for one already-established predicate (the caller has run
/// [`probe_predicate_inputs`] → `decide()` and got `PlannerDecision::Enumerate`; running this
/// for an unestablished predicate measures nothing the classifier will ever consume).
///
/// One `REPEATABLE READ` transaction, three statements over the same snapshot:
/// 1. `total`  — scope + predicate + tombstone overlay (secret rows stay in);
/// 2. returned ids — additionally excludes secret-linked rows, `ORDER BY memory_id`;
/// 3. `excluded_secret` — the secret-linked complement of 2 within 1.
///
/// The registry fragments (`enumerable_scope` / `sql_predicate`) are interpolated, not bound:
/// they are §50 typed config from `control.retrieval_predicates` (operator-written,
/// fail-loud-validated, CHECK-constrained), not caller input — same trust boundary as the
/// migration files themselves.
///
/// # Errors
/// [`ErrorCode::Internal`] only when the transaction itself cannot be opened/committed. A
/// statement-level failure inside the census (e.g. a predicate referencing a column the
/// snapshot does not have) is §22.4 trigger 4 — returned as `Ok` with
/// [`CensusResult::failed`], so `classify()` renders it `cannot_establish/census_failed`
/// instead of this adapter swallowing the verdict into a transport error.
pub async fn exact_enumerate(
    pool: &RuntimeDbPool,
    entry: &PredicateEntry,
    tenant_id: Uuid,
    user_id: Option<Uuid>,
    workspace_id: Uuid,
    stream: &StreamKey,
) -> Result<ExactCensusOutcome, ErrorCode> {
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    // 必须是本事务第一条语句：隔离级在第一个取快照的语句之后就改不了了（context_repo 同款）。
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *txn)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    set_request_context(&mut txn, tenant_id, user_id)
        .await
        .map_err(|_| ErrorCode::Internal)?;

    let scope = entry.enumerable_scope();
    let pred = entry.sql_predicate();
    let args = CensusArgs {
        tenant_id,
        workspace_id,
        stream,
    };

    let total_sql = format!("SELECT count(*) FROM {scope} AND ({pred}) AND {NOT_TOMBSTONED}");
    let total = match census_count(&mut txn, &total_sql, &args).await {
        Ok(n) => n,
        Err(_) => return census_failed(txn).await,
    };

    let ids_sql = format!(
        "SELECT memory_id FROM {scope} AND ({pred}) AND {NOT_TOMBSTONED} \
         AND NOT {SECRET_LINKED} ORDER BY memory_id"
    );
    let ids: Vec<Uuid> = match sqlx::query(&ids_sql)
        .bind(args.tenant_id)
        .bind(args.workspace_id)
        .bind(&args.stream.scope_kind)
        .bind(args.stream.scope_id)
        .bind(&args.stream.domain)
        .bind(&args.stream.projection_kind)
        .bind(&args.stream.projection_version)
        .fetch_all(&mut *txn)
        .await
    {
        Ok(rows) => match rows
            .iter()
            .map(|r| r.try_get::<Uuid, _>("memory_id"))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(ids) => ids,
            Err(_) => return census_failed(txn).await,
        },
        Err(_) => return census_failed(txn).await,
    };

    let secret_sql = format!(
        "SELECT count(*) FROM {scope} AND ({pred}) AND {NOT_TOMBSTONED} AND {SECRET_LINKED}"
    );
    let excluded_secret = match census_count(&mut txn, &secret_sql, &args).await {
        Ok(n) => n,
        Err(_) => return census_failed(txn).await,
    };

    txn.commit().await.map_err(|_| ErrorCode::Internal)?;

    // Negative counts cannot come out of count(*); the casts below are shape-only.
    let (total, returned, excluded) = (
        u64::try_from(total).map_err(|_| ErrorCode::Internal)?,
        ids.len() as u64,
        u64::try_from(excluded_secret).map_err(|_| ErrorCode::Internal)?,
    );

    match ExactEnumeration::new(entry.predicate_id(), total, returned, excluded) {
        Ok(e) => Ok(ExactCensusOutcome {
            census: CensusResult::enumerated(e),
            returned_ids: ids,
        }),
        // The constructor refusing our own readout means the three statements disagree beyond
        // what one snapshot allows — that *is* a failed census (§22.4 trigger 4), not a
        // transport error.
        Err(_) => Ok(ExactCensusOutcome {
            census: CensusResult::failed(),
            returned_ids: Vec::new(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::scope_table;

    #[test]
    fn scope_table_parses_the_registry_shape_and_rejects_mixed_scopes() {
        assert_eq!(
            scope_table("private.memory_records WHERE visibility_workspace_id = $1"),
            Some(("private", "memory_records"))
        );
        // §22.4 trigger 3's shape: a mixed private+public enumeration has no single probe-able
        // table face — the parser refuses, so `decide()` never sees it as established.
        assert_eq!(
            scope_table("private.memory_records JOIN public.pool USING (x) WHERE y = $1"),
            None
        );
        assert_eq!(scope_table("no_schema_qualifier WHERE x = $1"), None);
        assert_eq!(
            scope_table("private.memory_records; DROP TABLE x WHERE a = $1"),
            None
        );
    }
}
