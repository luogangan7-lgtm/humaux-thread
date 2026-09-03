//! `adapters::serving_repo` — §16.2 换代期读路由 SQL + §16.3 无裁量切换事务(T5.2/T5.3).
//!
//! Pure arithmetic ([`humaux_projection::serving::evaluate_switch`], the "无裁量口" itself)
//! lives in `humaux_projection::serving` (no IO, unit-tested there); this module only fetches
//! the numbers a real DB can give (`serving_version`'s read, the shadow version's `open_gaps`
//! count from `projection.processing_gaps`, inlined into [`switch_projection_version`]'s own
//! transaction — no free-standing duplicate of that query exists) and performs the atomic
//! switch `UPDATE`. `visible(shadow)` / `visible(serving)` (§23.1②) and the §69 Continuation
//! Gate verdict are **not** computed by this module — the former needs a Qdrant client (§17,
//! not built in this crate — `adapters::qdrant` 已实装，但 §23.1② 的 visible 计数由它的
//! `VisibleCountFilter` 承担，不在本模块), the latter
//! needs the §55/§69 benchmark harness (`continuation_198_v2` is currently `NOT_DECLARED`, no
//! `frozen_by` — see `humaux_projection::serving::ContinuationVerdict`'s doc).
//! [`switch_projection_version`] therefore takes both as `(projection_version, count)`-tagged
//! caller-supplied inputs and cross-checks the declared version against the DB's own state
//! before trusting the count (§16.3's "双侧各带自己的 projection_version filter、其余 filter
//! 逐字相同、同一时刻取" contract) — this module cannot enforce "same instant" across an IO
//! boundary it does not perform, but it can and does reject a declared version that does not
//! match reality.
//!
//! `serving_version` is `stream_family` retrieval's **sole entry point** for a family's active
//! `projection_version` (§16.2: "检索侧禁止把 `projection_version` 当常量读，只能经
//! `serving_version(stream_family)` 取"). G80-4 registers the workspace-wide call-site count
//! as the enforcement mechanism, and `adapters::retrieve::recall_with_overlay` is that one
//! consumer-side call site.
//!
//! 这段先前写的是「检索读路径尚未接入 ⇒ G80-4 判 not_applicable」。那不是三态的合法用法，
//! 是**自我豁免**：G80-4 的 NA 条件（检索侧一个调用点都没有）恰好就是 §16.2 被违反的状态，
//! 于是闸在违规最严重的时候最安静。§57.1 第2条允许 NA 的前提是**被测对象尚未交付**，而
//! `serving_version` 一直都在——缺的是调用它。已按 ADR-0006 改：闸的 NA 主语改成
//! `serving_version` 函数本身，读路径接进来。

use humaux_domain::identity::AuthorizationScope;
use humaux_projection::serving::{
    ContinuationVerdict, StreamFamily, SwitchCriteria, SwitchRejection, evaluate_switch,
};
use sqlx::Row;
use sqlx::types::Uuid;

use crate::postgres::{MaintenanceDbPool, RuntimeDbPool};

type Txn<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

/// DB-layer failure. Not one of the workspace's two frozen domain error enums (§52) — same
/// "adapter-local, not domain" reasoning as `stream_repo::StreamRepoError`.
#[derive(Debug)]
pub enum ServingRepoError {
    Db(sqlx::Error),
    CrossTenant,
    MissingAuthenticatedUser,
}

impl From<sqlx::Error> for ServingRepoError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl std::fmt::Display for ServingRepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "serving_repo DB error: {e}"),
            Self::CrossTenant => write!(f, "serving family is outside the authenticated tenant"),
            Self::MissingAuthenticatedUser => {
                write!(f, "serving read requires an authenticated user")
            }
        }
    }
}

impl std::error::Error for ServingRepoError {}

/// [`switch_projection_version`]'s result: either §16.3's three criteria all held and the
/// atomic `UPDATE` committed, or at least one failed and the DB was never touched (§16.3 "任一
/// 为假直接拒绝，不存在『人工判断可以上』的分支" — there is no partial-write state between
/// these two, `Rejected` carries the complete failing-criteria set from `evaluate_switch`, not
/// a per-criterion retry).
#[must_use = "Rejected/TargetVersionMissing mean the switch did NOT happen — silently \
              discarding this hides a real switch failure behind an apparent success"]
#[derive(Debug, PartialEq, Eq)]
pub enum SwitchOutcome {
    /// The atomic switch committed; `new_version` is now the family's sole `serving` row, the
    /// previously-serving row is retired (`serving = false`, row still present as a rollback
    /// target, §16.3).
    Switched,
    /// At least one of §16.3's three criteria failed — carries every failing reason
    /// (`humaux_projection::serving::SwitchRejection`), not just the first.
    Rejected(Vec<humaux_projection::serving::SwitchRejection>),
    /// §16.3's three criteria all held, but `new_version` names no row in
    /// `projection.stream_checkpoints` for this family (typo'd version, a shadow row that was
    /// never created, or one deleted concurrently) — the clear-`UPDATE` of the previously
    /// `serving` row is never committed in this case, so the family keeps its prior `serving`
    /// row untouched rather than ending up with zero `serving` rows. Distinct from `Rejected`:
    /// the criteria genuinely held, the target row just does not exist.
    TargetVersionMissing,
}

/// Maintenance writes remain tenant-scoped but have no end-user request context.
async fn set_tenant_local(txn: &mut Txn<'_>, tenant_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .execute(&mut **txn)
        .await?;
    Ok(())
}

/// Binds the authenticated tenant and user for this independently-opened read transaction.
async fn set_authorization_local(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
) -> Result<(), ServingRepoError> {
    let Some(user_id) = authorization.user_id() else {
        return Err(ServingRepoError::MissingAuthenticatedUser);
    };
    sqlx::query(
        "SELECT set_config('humaux.tenant_id', $1, true), set_config('humaux.user_id', $2, true)",
    )
    .bind(authorization.tenant_id().0.to_string())
    .bind(user_id.0.to_string())
    .execute(&mut **txn)
    .await?;
    Ok(())
}

const FAMILY_WHERE: &str =
    "tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 AND projection_kind = $5";

type PgQuery<'q> = sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>;

/// Binds the five [`StreamFamily`] columns in `FAMILY_WHERE`'s fixed `$1..$5` order.
fn bind_family<'q>(query: PgQuery<'q>, family: &'q StreamFamily) -> PgQuery<'q> {
    query
        .bind(family.tenant_id.0)
        .bind(&family.scope_kind)
        .bind(family.scope_id)
        .bind(&family.domain)
        .bind(&family.projection_kind)
}

/// §16.2's sole read-routing entry point: the family's current `serving` `projection_version`,
/// or `None` if no row is `serving = true` yet for this family (e.g. before the very first
/// version has ever been activated). Never returns a `shadow` row's version — `ux_serving_one`
/// (migration `0065`) guarantees at most one `serving = true` row exists per family, so this
/// query can never itself return more than one; a caller reading `projection_version` from
/// `stream_checkpoints` any other way (a hand-written query, a cached constant) bypasses this
/// contract and is exactly what G80-4 exists to catch.
///
/// 收 [`RuntimeDbPool`]（`role_gateway`）而不是 `RetrievalWorkerDbPool`：读路由是**请求路径
/// 上的纯读**，调用方是 `recall_with_overlay`，而 §6.2.3 的 typed pool 是闭集、没有任何转换
/// 路径，所以要么在这里换边、要么让请求路径凭空拿到第二个 pool。授权侧支持这么换：
/// `migrations/0011_roles_and_grants.sql:257` 给 `role_gateway` 的是表级无列限定
/// `GRANT SELECT ON projection.stream_checkpoints`（§6.2.2 矩阵同款），而写面不变——
/// `projection_highwater` 归 retrieval_worker、`serving`/`shadow` 归 maintenance。
pub async fn serving_version(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    family: &StreamFamily,
) -> Result<Option<String>, ServingRepoError> {
    let mut txn = pool.pool().begin().await?;
    set_authorization_local(&mut txn, authorization).await?;
    let version = serving_version_in_txn(&mut txn, authorization, family).await?;
    txn.commit().await?;
    Ok(version)
}

/// Transaction-owned form of [`serving_version`]. The caller has already established its
/// request snapshot and bound the authorization GUCs; this keeps §16.2's one authoritative
/// serving-version SQL query inside that snapshot instead of opening a second read transaction.
pub(crate) async fn serving_version_in_txn(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    family: &StreamFamily,
) -> Result<Option<String>, ServingRepoError> {
    if authorization.tenant_id() != family.tenant_id {
        return Err(ServingRepoError::CrossTenant);
    }
    if authorization.user_id().is_none() {
        return Err(ServingRepoError::MissingAuthenticatedUser);
    }
    bind_family(
        sqlx::query(&format!(
            "SELECT projection_version FROM projection.stream_checkpoints \
             WHERE {FAMILY_WHERE} AND serving"
        )),
        family,
    )
    .fetch_optional(&mut **txn)
    .await?
    .map(|row| row.try_get::<String, _>("projection_version"))
    .transpose()
    .map_err(ServingRepoError::Db)
}

/// Composes the single `bigint` key `pg_advisory_xact_lock` takes from a family's five
/// identity columns — same columns as `FAMILY_WHERE`, joined with a prefix and separator that
/// never collide with a `Uuid`'s hyphen-hex form or the closed-set identifiers
/// `scope_kind`/`domain`/`projection_kind` (§78.1/§78.2: never raw user text), so two distinct
/// families never hash-collide into the same lock key by column-boundary confusion.
fn family_lock_key(family: &StreamFamily) -> String {
    format!(
        "serving_repo:{}|{}|{}|{}|{}",
        family.tenant_id.0,
        family.scope_kind,
        family.scope_id,
        family.domain,
        family.projection_kind
    )
}

/// §16.3's atomic switch: fetches the shadow version's `open_gaps` count inside the same
/// transaction the switch itself would commit in, folds it together with the caller-supplied
/// `visible_shadow` / `visible_serving` / `continuation` into a
/// [`SwitchCriteria`](humaux_projection::serving::SwitchCriteria), and runs
/// [`evaluate_switch`] — the sole judgement point, no override parameter exists. Only on `Ok`
/// does it run the spec's own two-statement `UPDATE` block (§16.3 code block, verbatim: clear
/// the old `serving` row, then set the new one `serving = true, shadow = false`) and commit;
/// on any rejection (or on the target-missing case below) the transaction is rolled back
/// (`Drop` on an uncommitted `sqlx::Transaction` rolls back) and the DB is left exactly as it
/// was.
///
/// Before touching any row, this takes a `pg_advisory_xact_lock` scoped to `family` (released
/// automatically at commit/rollback) — this serializes concurrent switches on the same family
/// so the "current serving version" read below can never go stale between the read and this
/// transaction's own `UPDATE`s, and a second concurrent switch waits rather than racing
/// `ux_serving_one` into an opaque `Db` error.
///
/// `visible_shadow` / `visible_serving` are `(projection_version, count)` pairs, not bare
/// counts: this function cross-checks the declared version against the DB's actual state
/// (`new_version` for the shadow side, the freshly-read current `serving` version for the
/// other) before trusting either count — a mismatch is `SwitchRejection::VisibleVersionMismatch`,
/// caught here because only the DB layer knows what "actual" is (§16.2/§16.3; the pure
/// `evaluate_switch` only catches the case where both declared versions are identical).
///
/// Runs under [`MaintenanceDbPool`] — the only role holding column-limited `UPDATE(serving,
/// shadow)` on `projection.stream_checkpoints` (§6.2.2 grant matrix row `role_maintenance`).
pub async fn switch_projection_version(
    pool: &MaintenanceDbPool,
    family: &StreamFamily,
    new_version: &str,
    visible_shadow: Option<(String, u64)>,
    visible_serving: Option<(String, u64)>,
    continuation: ContinuationVerdict,
) -> Result<SwitchOutcome, ServingRepoError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, family.tenant_id.0).await?;

    // ponytail: one advisory lock per family via hashtextextended, not a dedicated lock table
    // or SELECT ... FOR UPDATE — upgrade only if advisory-lock key collisions are ever observed.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(family_lock_key(family))
        .execute(&mut *txn)
        .await?;

    // Current `serving` version, read under the advisory lock above so a concurrent switch
    // cannot change it out from under this one — also §16.3's version-consistency check input.
    let current_serving_version: Option<String> = bind_family(
        sqlx::query(&format!(
            "SELECT projection_version FROM projection.stream_checkpoints \
             WHERE {FAMILY_WHERE} AND serving"
        )),
        family,
    )
    .fetch_optional(&mut *txn)
    .await?
    .map(|row| row.try_get::<String, _>("projection_version"))
    .transpose()?;

    let shadow_key = family.with_version(new_version);
    let shadow_open_gaps: i64 = sqlx::query(
        "SELECT count(*) AS n FROM projection.processing_gaps \
         WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
           AND projection_kind = $5 AND projection_version = $6",
    )
    .bind(shadow_key.tenant_id.0)
    .bind(&shadow_key.scope_kind)
    .bind(shadow_key.scope_id)
    .bind(&shadow_key.domain)
    .bind(&shadow_key.projection_kind)
    .bind(&shadow_key.projection_version)
    .fetch_one(&mut *txn)
    .await?
    .try_get("n")?;

    // §16.2/§16.3: a `visible_*` tag naming a version other than the one it is actually being
    // compared against is untrustworthy — never handed to `evaluate_switch` as-is, and always
    // its own rejection reason (`VisibleVersionMismatch`), collected alongside whatever
    // `evaluate_switch` finds on the (now possibly narrower) checked criteria.
    let mut rejections = Vec::new();
    let checked_shadow = match visible_shadow {
        Some((v, n)) if v == new_version => Some((v, n)),
        Some(_) => {
            rejections.push(SwitchRejection::VisibleVersionMismatch);
            None
        }
        None => None,
    };
    let checked_serving = match (visible_serving, &current_serving_version) {
        (Some((v, n)), Some(cv)) if &v == cv => Some((v, n)),
        (Some(_), _) => {
            rejections.push(SwitchRejection::VisibleVersionMismatch);
            None
        }
        (None, _) => None,
    };

    let criteria = SwitchCriteria {
        visible_shadow: checked_shadow,
        visible_serving: checked_serving,
        // ADR-0017: no serving row for this family ⇒ first activation of a projection version.
        first_activation: current_serving_version.is_none(),
        shadow_open_gaps: shadow_open_gaps as u64,
        continuation,
    };
    if let Err(mut criteria_rejections) = evaluate_switch(&criteria) {
        rejections.append(&mut criteria_rejections);
    }

    if !rejections.is_empty() {
        // Rejected: `txn` is dropped without `commit()`, rolling back (and releasing the
        // advisory lock) — no row this function read (or would have written) survives. Never
        // call `txn.rollback()` explicitly here just to "be sure": an explicit rollback and a
        // dropped-uncommitted transaction are the same no-op-on-the-DB outcome, and sqlx's
        // `Drop` impl already guarantees it.
        return Ok(SwitchOutcome::Rejected(rejections));
    }

    // §16.3 code block, verbatim: clear whichever row is currently `serving`, then flip
    // the new version on. Both statements share this one transaction — "切换是一次原子
    // UPDATE" — so a reader can never observe zero or two `serving` rows mid-switch.
    bind_family(
        sqlx::query(&format!(
            "UPDATE projection.stream_checkpoints SET serving = false \
             WHERE {FAMILY_WHERE} AND serving"
        )),
        family,
    )
    .execute(&mut *txn)
    .await?;

    let set_result = sqlx::query(
        "UPDATE projection.stream_checkpoints SET serving = true, shadow = false \
         WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
           AND projection_kind = $5 AND projection_version = $6",
    )
    .bind(family.tenant_id.0)
    .bind(&family.scope_kind)
    .bind(family.scope_id)
    .bind(&family.domain)
    .bind(&family.projection_kind)
    .bind(new_version)
    .execute(&mut *txn)
    .await?;

    // Blocker fix (§16.3 "旧 version 行留作回滚目标"): the clear-`UPDATE` above already turned
    // off the previously-`serving` row (if any); if `new_version` names zero rows here (typo'd
    // version, a shadow row never created, or deleted before this txn's advisory lock was
    // taken), committing would leave the family with **zero** `serving` rows and report
    // `Switched` regardless. Never commit that: fall through without `commit()` (txn `Drop`
    // rolls back both `UPDATE`s) and report the distinguishable `TargetVersionMissing` instead.
    if set_result.rows_affected() != 1 {
        return Ok(SwitchOutcome::TargetVersionMissing);
    }

    txn.commit().await?;
    Ok(SwitchOutcome::Switched)
}
