//! `retrieve` — §15.5 Read-your-writes (`consistency_token` issue/decode/scope-check) and
//! the PostgreSQL delta overlay `recall`/`context` fall back to when serving Qdrant has not
//! yet caught up to a token's write (T3.8).
//!
//! **Why this lives in `humaux-adapters`, not `humaux-application`**: every function below
//! that touches PostgreSQL needs `&RuntimeDbPool`, and [`crate::postgres::RuntimeDbPool`]'s
//! `pool()` accessor is `pub(crate)` to this crate on purpose (§6.2.3 closed-set doctrine —
//! see that module's doc comment). `humaux-application` cannot extract a query surface from
//! the type even if it depended on this crate to name it. T3.5's sibling module
//! (`crate::jobs`, `ops.jobs` SKIP LOCKED claim) already established the working precedent
//! for Phase 3 J*-wave DB-backed logic: the real implementation lands here, and
//! `humaux_application::retrieve` stays the unfilled T0.x placeholder. This module follows
//! that precedent rather than the (stale, pre-J3) task brief's literal file path, to avoid a
//! `crates/application/Cargo.toml` dependency edit against a shared file with no compile-time
//! way to use the added dependency anyway (see the doc comment cited above).
//!
//! Token format is self-contained here (hex-encoded field list, no signature — see
//! [`decode_consistency_token`]'s doc for the ponytail note on that ceiling) rather than
//! reusing `humaux_projection::stream::StreamKey`'s `Serialize`/`Deserialize` (it derives
//! neither, and this module does not own that file to add them). [`issue_consistency_token`]
//! is the only encoder: `crate::remember::remember` builds [`TokenClaims`] from its in-transaction
//! Evidence/stream write and returns the result only after commit, so every returned token is
//! accepted by [`recall_with_overlay`] without a compatibility parser.

use std::collections::{BTreeMap, HashSet};

use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use humaux_domain::affect::AffectFilter;
use humaux_domain::error::ErrorCode;
use humaux_domain::identity::AuthorizationScope;
use humaux_domain::ids::{TenantId, WorkspaceId};
use humaux_domain::subject::SubjectId;
use humaux_projection::serving::StreamFamily;
use humaux_projection::stream::StreamKey;
use humaux_retrieval::completeness::LedgerClosure;
use humaux_retrieval::envelope::GroundingBlock;

use crate::postgres::RuntimeDbPool;
use crate::private_projection_registry::{
    PrivateProjectionRegistryError, ProjectionPointId, resolve_private_memory_points_in_txn,
};
use crate::qdrant::{DenseCandidate, PointId};
use crate::read_materialize::{MaterializedBodies, materialize_final_bodies_about_in_txn};
use crate::serving_repo::{self, ServingRepoError};
use crate::stream_repo::close_ledger_in_txn;

type Txn<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

/// DB/decode-layer failure. Adapter-local, not one of the workspace's two frozen domain
/// error enums (§52) — same reasoning as `jobs::JobsError` / `postgres::PoolInitError`.
#[derive(Debug)]
pub enum RetrieveError {
    Db(sqlx::Error),
    /// Malformed token: not valid hex, wrong field count, or an unparsable field.
    TokenMalformed(&'static str),
    /// §15.1 nine-state closed set: a `state` value came back that matches none of them —
    /// only reachable if `stream_log_state_check` has drifted from [`ProcessingState`] (§78.2).
    UnknownProcessingState(String),
    /// §15.5 "不可跨 tenant/workspace 使用": the token's bound tenant does not match the
    /// tenant the caller is actually authenticated/scoped as for this request.
    CrossTenant,
    /// Same rule, workspace half — `None` (tenant-shared scope) and `Some(_)` are also a
    /// mismatch, not just two different `Some` values (fail-closed, no partial credit).
    CrossWorkspace,
    /// The token's configured/policy expiry has passed. The token remains non-authentication
    /// data; expiry only bounds read-your-writes overlay use.
    TokenExpired,
    /// The token claims a stream family that does not equal the caller/bootstrap-selected
    /// five-column family. Tokens never select a retrieval route (§15.5/§16.2).
    UntrustedStreamFamily,
    /// Only the existing lowercase `tenant` and `workspace` scope forms are admitted here.
    UnknownScopeKind,
    /// A syntactically valid token did not match one exact issued stream row and registered
    /// checkpoint version for its trusted family.
    TokenNotIssued,
    /// Gateway RLS needs an authenticated user setting for private visibility reads.
    MissingAuthenticatedUser,
    /// Dense Qdrant candidates have opaque projection point identities. Until PostgreSQL owns
    /// an immutable point-to-object registry, no candidate can be hydrated as a Memory.
    SemanticCandidateProjectionRegistryUnavailable,
    /// Private semantic serving accepts only the UUID point-id family registered in PG.
    UnsupportedPrivateProjectionPointId,
    ProjectionRegistry(PrivateProjectionRegistryError),
    FinalMaterialization(ErrorCode),
    /// A caught-up envelope cannot carry an overlay. Treat a contradictory input as a failed
    /// invariant rather than returning Evidence whose status cannot be explained.
    CaughtUpEnvelopeHasOverlay,
    /// The authoritative overlay has one object identity per Evidence. Conflicting copies
    /// cannot be deterministically merged without inventing a processing state.
    ConflictingOverlayEvidence,
    /// The Qdrant candidate query used a version that stopped being the serving projection
    /// before the authoritative PostgreSQL snapshot began. The caller must retry from routing.
    ServingProjectionChanged,
}

impl From<sqlx::Error> for RetrieveError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl std::fmt::Display for RetrieveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "retrieve overlay DB error: {e}"),
            Self::TokenMalformed(why) => write!(f, "consistency_token malformed: {why}"),
            Self::UnknownProcessingState(s) => {
                write!(
                    f,
                    "stream_log.state {s:?} matches no ProcessingState variant"
                )
            }
            Self::CrossTenant => {
                write!(f, "§15.5: consistency_token used outside its bound tenant")
            }
            Self::CrossWorkspace => {
                write!(
                    f,
                    "§15.5: consistency_token used outside its bound workspace"
                )
            }
            Self::TokenExpired => write!(f, "§15.5: consistency_token has expired"),
            Self::UntrustedStreamFamily => write!(f, "§15.5: token cannot select a stream family"),
            Self::UnknownScopeKind => write!(f, "§15.5: token scope kind is not supported"),
            Self::TokenNotIssued => write!(
                f,
                "§15.5: token does not name an issued registered stream row"
            ),
            Self::MissingAuthenticatedUser => {
                write!(f, "§6.1: recall requires an authenticated user context")
            }
            Self::SemanticCandidateProjectionRegistryUnavailable => write!(
                f,
                "private semantic read serving requires a PostgreSQL projection identity registry"
            ),
            Self::UnsupportedPrivateProjectionPointId => write!(
                f,
                "private semantic candidate uses an unregistered point-id family"
            ),
            Self::ProjectionRegistry(error) => write!(f, "{error}"),
            Self::FinalMaterialization(error) => {
                write!(
                    f,
                    "private semantic final materialization failed: {error:?}"
                )
            }
            Self::CaughtUpEnvelopeHasOverlay => write!(
                f,
                "caught-up read-your-writes envelope unexpectedly carries an overlay"
            ),
            Self::ConflictingOverlayEvidence => write!(
                f,
                "read-your-writes overlay repeats an Evidence with conflicting state"
            ),
            Self::ServingProjectionChanged => write!(
                f,
                "private semantic candidate version is no longer the serving projection"
            ),
        }
    }
}

impl From<PrivateProjectionRegistryError> for RetrieveError {
    fn from(value: PrivateProjectionRegistryError) -> Self {
        Self::ProjectionRegistry(value)
    }
}

impl std::error::Error for RetrieveError {}

/// §15.1 nine-state closed set, reused here as the wire value returned to callers under
/// `processing_state` for an Evidence that has not finished distillation (§15.5: "允许把该
/// Evidence 作为带 `processing_state` 的临时上下文候选返回...但不能冒充已完成 Memory").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingState {
    Issued,
    Processing,
    WaitingKey,
    RetryWait,
    Lost,
    Done,
    SkippedByPolicy,
    Failed,
    Tombstoned,
}

impl ProcessingState {
    pub const ALL: [ProcessingState; 9] = [
        Self::Issued,
        Self::Processing,
        Self::WaitingKey,
        Self::RetryWait,
        Self::Lost,
        Self::Done,
        Self::SkippedByPolicy,
        Self::Failed,
        Self::Tombstoned,
    ];

    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Issued => "ISSUED",
            Self::Processing => "PROCESSING",
            Self::WaitingKey => "WAITING_KEY",
            Self::RetryWait => "RETRY_WAIT",
            Self::Lost => "LOST",
            Self::Done => "DONE",
            Self::SkippedByPolicy => "SKIPPED_BY_POLICY",
            Self::Failed => "FAILED",
            Self::Tombstoned => "TOMBSTONED",
        }
    }

    pub(crate) fn parse(s: &str) -> Result<Self, RetrieveError> {
        Self::ALL
            .into_iter()
            .find(|v| v.as_db_str() == s)
            .ok_or_else(|| RetrieveError::UnknownProcessingState(s.to_string()))
    }

    /// §15.2/§15.4 `SETTLED_OK = DONE | SKIPPED_BY_POLICY | TOMBSTONED` — an Evidence in one
    /// of these states is safe to present as if it were a completed Memory candidate (still
    /// carrying `processing_state` per §15.5's "不能冒充已完成 Memory", but no longer *only*
    /// a temporary placeholder).
    pub fn is_settled_ok(self) -> bool {
        matches!(self, Self::Done | Self::SkippedByPolicy | Self::Tombstoned)
    }
}

/// §15.5 `consistency_token` claims. All fields the token binds (tenant/workspace/pipeline
/// stream key/stream_seq/commit_seq/issued_at/expires_at) — verbatim field list from the
/// spec prose, `commit_seq` audit-only per that same passage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenClaims {
    pub tenant_id: Uuid,
    /// `None` = tenant-shared scope (mirrors `visibility_workspace_id IS NULL`, §8.1).
    pub workspace_id: Option<Uuid>,
    pub scope_kind: String,
    pub scope_id: Uuid,
    pub domain: String,
    pub projection_kind: String,
    pub projection_version: String,
    pub stream_seq: i64,
    /// Audit-only (§15.5 "Agent 不需要理解 commit_seq" / §15 "commit_seq 只剩全局审计总序一
    /// 个用途，禁止用于判定任何 stream 的完整性") — carried through, never compared.
    pub commit_seq: i64,
    pub issued_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
}

impl TokenClaims {
    /// The [`StreamKey`] this token's stream identity resolves to (§15.1's six PK columns).
    pub fn stream_key(&self) -> StreamKey {
        StreamKey::new(
            TenantId(self.tenant_id),
            self.scope_kind.clone(),
            self.scope_id,
            self.domain.clone(),
            self.projection_kind.clone(),
            self.projection_version.clone(),
        )
    }
}

const FIELD_SEP: char = '\u{1}';

/// Sole constructor for an opaque `consistency_token` string (§15.5 "consistency_token 是服务
/// 器生成的不透明值...Agent 不需要理解...也不能自行构造 token"). `remember()`'s transaction B
/// (T3.1/T3.2, not yet landed) is expected to call this exact function once it exists —
/// documented here so that task converges on it instead of re-deriving the wire format.
///
/// ponytail: field-list + hex, no signature. §15.5 states outright this is "只提供
/// read-your-writes 约束，不是认证 token" — the real security boundary is PostgreSQL RLS on
/// `humaux.tenant_id` from the *authenticated session*, not from anything this token claims;
/// [`validate_scope`] below rejects a mismatch before any query runs, so a hand-forged token
/// cannot widen what RLS already lets the caller see, only make `recall`/`context` return a
/// `CrossTenant`/`CrossWorkspace` error for its own request. Upgrade path if that stops being
/// true (e.g. this token starts gating something RLS does not independently enforce): HMAC-
/// sign the field list with a server-held key, verify in [`decode_consistency_token`].
pub fn issue_consistency_token(claims: &TokenClaims) -> String {
    let fields = [
        claims.tenant_id.to_string(),
        claims
            .workspace_id
            .map(|w| w.to_string())
            .unwrap_or_default(),
        claims.scope_kind.clone(),
        claims.scope_id.to_string(),
        claims.domain.clone(),
        claims.projection_kind.clone(),
        claims.projection_version.clone(),
        claims.stream_seq.to_string(),
        claims.commit_seq.to_string(),
        claims.issued_at.unix_timestamp().to_string(),
        claims.expires_at.unix_timestamp().to_string(),
    ];
    let plain = fields.join("\u{1}");
    plain
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Inverse of [`issue_consistency_token`]. Every failure mode returns
/// [`RetrieveError::TokenMalformed`] — never panics on attacker/client-controlled input.
pub fn decode_consistency_token(token: &str) -> Result<TokenClaims, RetrieveError> {
    if !token.len().is_multiple_of(2) || token.is_empty() {
        return Err(RetrieveError::TokenMalformed("odd length or empty"));
    }
    let mut bytes = Vec::with_capacity(token.len() / 2);
    let chars: Vec<char> = token.chars().collect();
    for pair in chars.chunks(2) {
        let hex: String = pair.iter().collect();
        let b =
            u8::from_str_radix(&hex, 16).map_err(|_| RetrieveError::TokenMalformed("not hex"))?;
        bytes.push(b);
    }
    let plain = String::from_utf8(bytes).map_err(|_| RetrieveError::TokenMalformed("not utf8"))?;
    let parts: Vec<&str> = plain.split(FIELD_SEP).collect();
    let [
        tenant_id,
        workspace_id,
        scope_kind,
        scope_id,
        domain,
        projection_kind,
        projection_version,
        stream_seq,
        commit_seq,
        issued_at,
        expires_at,
    ] = parts.as_slice()
    else {
        return Err(RetrieveError::TokenMalformed("wrong field count"));
    };

    let parse_uuid =
        |s: &str| Uuid::parse_str(s).map_err(|_| RetrieveError::TokenMalformed("bad uuid field"));
    let parse_i64 = |s: &str| {
        s.parse::<i64>()
            .map_err(|_| RetrieveError::TokenMalformed("bad int field"))
    };
    let parse_ts = |secs: i64| {
        OffsetDateTime::from_unix_timestamp(secs)
            .map_err(|_| RetrieveError::TokenMalformed("bad timestamp field"))
    };

    Ok(TokenClaims {
        tenant_id: parse_uuid(tenant_id)?,
        workspace_id: if workspace_id.is_empty() {
            None
        } else {
            Some(parse_uuid(workspace_id)?)
        },
        scope_kind: (*scope_kind).to_string(),
        scope_id: parse_uuid(scope_id)?,
        domain: (*domain).to_string(),
        projection_kind: (*projection_kind).to_string(),
        projection_version: (*projection_version).to_string(),
        stream_seq: parse_i64(stream_seq)?,
        commit_seq: parse_i64(commit_seq)?,
        issued_at: parse_ts(parse_i64(issued_at)?)?,
        expires_at: parse_ts(parse_i64(expires_at)?)?,
    })
}

/// Validates the only supported token-to-scope mappings and returns the effective, possibly
/// narrowed scope.  The token is never an authorization capability: `AuthorizationScope` comes
/// from the authenticated request and a workspace token can only shrink it.
pub fn validate_scope(
    claims: &TokenClaims,
    authorization: &AuthorizationScope,
) -> Result<AuthorizationScope, RetrieveError> {
    if claims.tenant_id != authorization.tenant_id().0 {
        return Err(RetrieveError::CrossTenant);
    }

    match claims.scope_kind.as_str() {
        "tenant" if claims.workspace_id.is_none() && claims.scope_id == claims.tenant_id => {
            Ok(authorization.clone())
        }
        "workspace" if claims.workspace_id == Some(claims.scope_id) => authorization
            .narrow(WorkspaceId(claims.scope_id))
            .map_err(|_| RetrieveError::CrossWorkspace),
        "tenant" | "workspace" => Err(RetrieveError::CrossWorkspace),
        _ => Err(RetrieveError::UnknownScopeKind),
    }
}

fn validate_stream_family(
    claims: &TokenClaims,
    family: &StreamFamily,
) -> Result<(), RetrieveError> {
    if claims.tenant_id != family.tenant_id.0
        || claims.scope_kind != family.scope_kind
        || claims.scope_id != family.scope_id
        || claims.domain != family.domain
        || claims.projection_kind != family.projection_kind
    {
        return Err(RetrieveError::UntrustedStreamFamily);
    }
    Ok(())
}

fn validate_expiry(claims: &TokenClaims, now: OffsetDateTime) -> Result<(), RetrieveError> {
    if claims.expires_at <= now {
        return Err(RetrieveError::TokenExpired);
    }
    Ok(())
}

/// Binds both values that private RLS uses.  A request without an authenticated user fails
/// closed instead of borrowing a pooled connection's previous setting or using the token.
async fn set_authorization_local(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    authorization: &AuthorizationScope,
) -> Result<(), RetrieveError> {
    let Some(user_id) = authorization.user_id() else {
        return Err(RetrieveError::MissingAuthenticatedUser);
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

/// §15.4 `contiguous_done_prefix(stream) = min{s | state ∉ SETTLED_OK} - 1`, or
/// `max(stream_seq)` when every issued row is `SETTLED_OK`, or `0` when nothing has been
/// issued for this stream yet. Read-only, via `role_gateway` (`RuntimeDbPool`) — deliberately
/// independent of `humaux_adapters::stream_repo`'s (T3.3, `RetrievalWorkerDbPool`) copy of
/// this same formula for the periodic `advance_prefix` writer job: that module computes it as
/// one of *four* independently cross-proved numbers before *writing*
/// `stream_checkpoints.projection_highwater`; this one only *reads* it, on the request path,
/// under a role that has no write grant on that column at all. Folding them into one shared
/// query helper is a reasonable post-wave cleanup once both have landed, not a correctness
/// requirement — §15.4's "三数独立取数" is about not reusing one query result for two of the
/// *write-path's own* cross-checks, not about every reader in the codebase sharing one query.
pub async fn contiguous_done_prefix(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    key: &StreamKey,
) -> Result<i64, RetrieveError> {
    let mut txn = pool.pool().begin().await?;
    set_authorization_local(&mut txn, authorization).await?;
    let prefix = contiguous_done_prefix_in_txn(&mut txn, key).await?;
    txn.commit().await?;
    Ok(prefix)
}

pub(crate) async fn contiguous_done_prefix_in_txn(
    txn: &mut Txn<'_>,
    key: &StreamKey,
) -> Result<i64, RetrieveError> {
    let row = sqlx::query(
        "SELECT COALESCE(
           MIN(stream_seq) FILTER (WHERE state NOT IN ('DONE','SKIPPED_BY_POLICY','TOMBSTONED')) - 1,
           MAX(stream_seq),
           0
         )::bigint AS prefix
         FROM projection.stream_log
         WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
           AND domain = $4 AND projection_kind = $5 AND projection_version = $6",
    )
    .bind(key.tenant_id.0)
    .bind(&key.scope_kind)
    .bind(key.scope_id)
    .bind(&key.domain)
    .bind(&key.projection_kind)
    .bind(&key.projection_version)
    .fetch_one(&mut **txn)
    .await?;
    Ok(row.try_get::<i64, _>("prefix")?)
}

/// `projection.stream_checkpoints.projection_highwater`，**且只取该 family 的 serving 行**
/// ——"已对检索可见的边界"（§15.3）。§16.2 冻结「读路由只打 `serving` 行」，所以这里的
/// `AND serving` 不是优化而是判据本身。
///
/// `0`（而非报错）覆盖两种情形，两者对 overlay 决策的含义相同：
/// ① 这条流还没有 checkpoint 行——没服务过任何东西；
/// ② 行在，但它不是 serving 行（token 指向已退役或仍在 shadow 回填的版本）——按 §16.2
///    「该行不进入任何 envelope」，对检索面而言等同于没服务过。
///
/// 两种情形都 fail-closed 地走 overlay，也就是从 PG 直读而不是相信一个非 serving 版本的
/// 水位。这里**不再另设一道「token.version == serving version」的等值守卫**：
/// `ux_serving_one`（migration `0065`）保证每 family 至多一行 `serving = true`，因此
/// 「六列命中且 serving」与那道等值判定在所有输入下同结果——留两道就必有一道永远观察不到
/// 自己失败，那种判据按 §80.1 不算判据。
pub async fn serving_projection_highwater(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    key: &StreamKey,
) -> Result<i64, RetrieveError> {
    let mut txn = pool.pool().begin().await?;
    set_authorization_local(&mut txn, authorization).await?;
    let highwater = serving_projection_highwater_in_txn(&mut txn, key).await?;
    txn.commit().await?;
    Ok(highwater)
}

pub(crate) async fn serving_projection_highwater_in_txn(
    txn: &mut Txn<'_>,
    key: &StreamKey,
) -> Result<i64, RetrieveError> {
    let row = sqlx::query(
        "SELECT projection_highwater FROM projection.stream_checkpoints
         WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
           AND domain = $4 AND projection_kind = $5 AND projection_version = $6
           AND serving",
    )
    .bind(key.tenant_id.0)
    .bind(&key.scope_kind)
    .bind(key.scope_id)
    .bind(&key.domain)
    .bind(&key.projection_kind)
    .bind(&key.projection_version)
    .fetch_optional(&mut **txn)
    .await?;
    match row {
        Some(row) => Ok(row.try_get::<i64, _>("projection_highwater")?),
        None => Ok(0),
    }
}

pub(crate) async fn validate_issued_token_in_txn(
    txn: &mut Txn<'_>,
    key: &StreamKey,
    claims: &TokenClaims,
) -> Result<(), RetrieveError> {
    let registered_and_issued: bool = sqlx::query_scalar(
        "SELECT EXISTS (
           SELECT 1 FROM projection.stream_checkpoints checkpoint
            WHERE checkpoint.tenant_id = $1 AND checkpoint.scope_kind = $2
              AND checkpoint.scope_id = $3 AND checkpoint.domain = $4
              AND checkpoint.projection_kind = $5 AND checkpoint.projection_version = $6
         ) AND EXISTS (
           SELECT 1 FROM projection.stream_log stream
            WHERE stream.tenant_id = $1 AND stream.scope_kind = $2
              AND stream.scope_id = $3 AND stream.domain = $4
              AND stream.projection_kind = $5 AND stream.projection_version = $6
              AND stream.stream_seq = $7 AND stream.commit_seq = $8
         )",
    )
    .bind(key.tenant_id.0)
    .bind(&key.scope_kind)
    .bind(key.scope_id)
    .bind(&key.domain)
    .bind(&key.projection_kind)
    .bind(&key.projection_version)
    .bind(claims.stream_seq)
    .bind(claims.commit_seq)
    .fetch_one(&mut **txn)
    .await?;
    if registered_and_issued {
        Ok(())
    } else {
        Err(RetrieveError::TokenNotIssued)
    }
}

/// One overlay candidate — an Evidence the serving projection has not (yet, or ever will)
/// surface on its own, returned instead from `projection.stream_log` directly.
#[derive(Debug, Clone)]
pub struct OverlayCandidate {
    pub stream_seq: i64,
    pub evidence_id: Uuid,
    /// §15.5: always present, so a not-yet-distilled Evidence never masquerades as a
    /// finished Memory — the caller renders this alongside `memory_id` rather than hiding it.
    pub processing_state: ProcessingState,
    /// Every distinct `memory_id` with a `private.memory_evidence` link to `evidence_id`
    /// (§8.6/§15.5: "一次 Evidence 可能产生 0/1/N 条 Memory, 因此不得同步返回伪造的单一
    /// `memory_id`"). Empty = no Memory has been distilled from this Evidence yet; `role` is
    /// not surfaced here (`memory_evidence`'s PK is `(memory_id, evidence_id, role)`, so a
    /// single Memory can carry >1 role row for the same Evidence — deduplicated below so this
    /// list is one entry per Memory, not one per link row).
    pub memory_ids: Vec<Uuid>,
}

/// §15.5's PG delta overlay: every `stream_log` row for `key` with `stream_seq >
/// serving_highwater`, up to and including `up_to_stream_seq_inclusive` (the caller passes
/// the token's own `stream_seq` — the one write the token exists to guarantee visible), each
/// labeled with its live `processing_state` and every `memory_id` that has already landed for
/// it (0/1/N, never a fabricated single value). Deliberately not capped at
/// `contiguous_done_prefix`: §15.5's very next
/// sentence after naming that bound is "如果 Evidence 尚未完成蒸馏，允许把该 Evidence 作为带
/// `processing_state` 的临时上下文候选返回" — an `ISSUED`/`PROCESSING` row past the prefix is
/// exactly the "just written, not yet settled" case this overlay exists to surface, not a
/// gap to hide. `contiguous_done_prefix` is exposed on [`RecallEnvelope`] instead, as the
/// caller's *completeness* bound (how far the overlay can vouch for "nothing missing"), which
/// is the reading that keeps every other §15 use of this quantity (always a cap on what a
/// *watermark* may claim, never a filter on what a *candidate list* may contain) consistent.
///
/// The `private.memory_evidence` link is aggregated per Evidence rather than left-joined flat:
/// its PK is `(memory_id, evidence_id, role)`, so a plain `LEFT JOIN ... ON evidence_id` fans
/// out one row per link (N rows for one Evidence with N Memories, or even for one Memory with
/// more than one role), which would duplicate the same `stream_seq`/`evidence_id` across N
/// distinct `OverlayCandidate`s instead of surfacing all its memories on one. `GROUP BY` +
/// `array_agg DISTINCT` keeps the query's cardinality equal to the outer `stream_log` row count.
///
/// `evidence_id` is recovered by joining `ops.outbox` on `(tenant_id, commit_seq)` — §60's
/// `remember::issue_stream_log_row` (T3.2, a file this task does not own) never binds an
/// `evidence_id` onto `stream_log` itself, but `remember::insert_outbox` writes exactly that
/// triple in the same transaction. `commit_seq`, not `stream_seq`, is the join key: it is the
/// one column here that is genuinely global-unique (`ops.commit_seq_seq`, "全局审计总序"), so
/// two different streams that both legitimately have a row at `stream_seq = 1` cannot
/// cross-match the way a `(tenant_id, stream_seq)` join would let them. See migration
/// `0046_stream_log_evidence_id_via_outbox.sql` for the full reasoning, including why an
/// earlier version of this task added and then dropped a `stream_log.evidence_id` column
/// instead of granting `role_gateway` `SELECT` on `ops.outbox`.
pub async fn pg_delta_overlay(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    key: &StreamKey,
    serving_highwater: i64,
    up_to_stream_seq_inclusive: i64,
) -> Result<Vec<OverlayCandidate>, RetrieveError> {
    let mut txn = pool.pool().begin().await?;
    set_authorization_local(&mut txn, authorization).await?;
    let overlay = pg_delta_overlay_in_txn(
        &mut txn,
        authorization,
        key,
        serving_highwater,
        up_to_stream_seq_inclusive,
    )
    .await?;
    txn.commit().await?;
    Ok(overlay)
}

pub(crate) async fn pg_delta_overlay_in_txn(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    key: &StreamKey,
    serving_highwater: i64,
    up_to_stream_seq_inclusive: i64,
) -> Result<Vec<OverlayCandidate>, RetrieveError> {
    let rows = sqlx::query(
        "SELECT sl.stream_seq, sl.state, ob.evidence_id,
                array_agg(DISTINCT mr.memory_id) FILTER (WHERE mr.memory_id IS NOT NULL)
                  AS memory_ids
         FROM projection.stream_log sl
         JOIN ops.outbox ob ON ob.tenant_id = sl.tenant_id AND ob.commit_seq = sl.commit_seq
         JOIN private.evidence_objects evidence
           ON evidence.evidence_id = ob.evidence_id AND evidence.tenant_id = sl.tenant_id
         LEFT JOIN private.memory_evidence me ON me.evidence_id = evidence.evidence_id
         LEFT JOIN private.memory_records mr
           ON mr.memory_id = me.memory_id AND mr.tenant_id = sl.tenant_id
          AND mr.status = 'active'
          AND (mr.visibility_class = 'TENANT_SHARED'
            OR (mr.visibility_class = 'USER_PRIVATE' AND mr.visibility_user_id = $9)
            OR (mr.visibility_class = 'WORKSPACE_SHARED' AND mr.visibility_workspace_id = ANY($10)))
         WHERE sl.tenant_id = $1 AND sl.scope_kind = $2 AND sl.scope_id = $3
           AND sl.domain = $4 AND sl.projection_kind = $5 AND sl.projection_version = $6
           AND sl.stream_seq > $7 AND sl.stream_seq <= $8 AND sl.state <> 'TOMBSTONED'
           AND (evidence.visibility_class = 'TENANT_SHARED'
             OR (evidence.visibility_class = 'USER_PRIVATE' AND evidence.visibility_user_id = $9)
             OR (evidence.visibility_class = 'WORKSPACE_SHARED'
                 AND evidence.visibility_workspace_id = ANY($10)))
         GROUP BY sl.stream_seq, sl.state, ob.evidence_id
         ORDER BY sl.stream_seq",
    )
    .bind(key.tenant_id.0)
    .bind(&key.scope_kind)
    .bind(key.scope_id)
    .bind(&key.domain)
    .bind(&key.projection_kind)
    .bind(&key.projection_version)
    .bind(serving_highwater)
    .bind(up_to_stream_seq_inclusive)
    .bind(authorization.user_id().expect("bound before query").0)
    .bind(
        authorization
            .allowed_workspace_ids()
            .iter()
            .map(|workspace| workspace.0)
            .collect::<Vec<_>>(),
    )
    .fetch_all(&mut **txn)
    .await?;

    rows.iter()
        .map(|row| {
            let state: String = row.try_get("state")?;
            Ok(OverlayCandidate {
                stream_seq: row.try_get("stream_seq")?,
                evidence_id: row.try_get("evidence_id")?,
                processing_state: ProcessingState::parse(&state)?,
                memory_ids: row
                    .try_get::<Option<Vec<Uuid>>, _>("memory_ids")?
                    .unwrap_or_default(),
            })
        })
        .collect()
}

/// One `recall`/`context` read-your-writes decision (§15.5). `served_by_projection = true`
/// means serving Qdrant already covers the token's write — callers should not merge anything
/// from `overlay` (it is empty in that case).
#[derive(Debug, Clone)]
pub struct RecallEnvelope {
    validated_stream_key: StreamKey,
    pub served_by_projection: bool,
    pub overlay: Vec<OverlayCandidate>,
    /// §15.4 `contiguous_done_prefix` at decision time — the overlay's completeness bound,
    /// not a filter on `overlay`'s contents (see [`pg_delta_overlay`]'s doc).
    pub contiguous_done_prefix: i64,
    /// §16.2：本次响应**唯一合法的检索面版本**，取自该 family 的 `serving` 行
    /// （[`crate::serving_repo::serving_version`]），**不是** token 里带的那个。
    ///
    /// 构造 §23.1② 的 visible filter（`qdrant::VisibleCountFilter::new`）时只许读这里：
    /// token 是客户端提交的，拿它当路由依据等于让调用方指定读哪个版本，正是 §16.2 要禁的。
    /// token 里的 version 仍然有用，但角色是**被核对项**（它命中的行是不是 serving），
    /// 不是路由依据。
    ///
    /// `None` = 该 family 还没有任何 serving 行（正常引导态：§6.2.2 下 gateway 造不出
    /// `serving = true` 的 checkpoint）。此时**不得**构建任何 visible count filter——
    /// 没有 serving 版本时「可见计数」没有分母，按 §57.1 那是 `cannot_establish` 而不是 0。
    pub serving_version: Option<String>,
}

impl RecallEnvelope {
    /// Full stream identity was independently checked against the token ledger before this
    /// envelope existed. Subsequent snapshot reads may use it but must not reconstruct it from
    /// the client token.
    pub fn validated_stream_key(&self) -> &StreamKey {
        &self.validated_stream_key
    }
}

/// Canonical input for the existing final PostgreSQL materializer.
///
/// This is deliberately only a candidate composition seam: it has no bodies and no alternate
/// hydrate path. Its output must be passed to `read_materialize::materialize_final_bodies_in_txn`,
/// which owns final visibility, lifecycle, secret-source, tombstone, and revocation checks.
/// `memory_ids` remains empty until a PostgreSQL projection identity registry can resolve
/// opaque Qdrant point ids to canonical objects.
#[derive(Debug, Clone)]
pub struct PrivateReadServingCandidates {
    pub memory_ids: Vec<Uuid>,
    pub overlay: Vec<OverlayCandidate>,
    pub serving_version: Option<String>,
}

/// One private semantic result whose bodies, ledger closure, and conservative grounding report
/// were all produced under the same PostgreSQL repeatable-read snapshot.
#[derive(Debug, Clone)]
pub struct MaterializedPrivateReadServing {
    pub bodies: MaterializedBodies,
    pub ledger: LedgerClosure,
    pub grounding: GroundingBlock,
}

/// Carries an already-authorized RYW decision to the final materializer without making Qdrant
/// a source of truth.
///
/// Every nonempty semantic list fails loudly: `DenseCandidate::point_id` is an opaque projection
/// identifier, never a `MemoryId` even when its wire shape is a UUID. A future registry resolver
/// must supply canonical object identity, revision, body hash, projection version, and scope
/// from PostgreSQL before semantic candidates enter this seam. The RYW overlay is deduplicated
/// by stable Evidence identity and emitted by `(stream_seq, evidence_id)`; the materializer then
/// rechecks it and performs the authoritative Memory/overlay union.
pub fn private_read_serving_candidates(
    semantic: &[DenseCandidate],
    envelope: &RecallEnvelope,
) -> Result<PrivateReadServingCandidates, RetrieveError> {
    if envelope.served_by_projection && !envelope.overlay.is_empty() {
        return Err(RetrieveError::CaughtUpEnvelopeHasOverlay);
    }
    if !semantic.is_empty() {
        return Err(RetrieveError::SemanticCandidateProjectionRegistryUnavailable);
    }

    let mut overlay_by_evidence = BTreeMap::<Uuid, OverlayCandidate>::new();
    for candidate in &envelope.overlay {
        let mut candidate = candidate.clone();
        candidate.memory_ids.sort_unstable();
        candidate.memory_ids.dedup();
        if let Some(existing) = overlay_by_evidence.get(&candidate.evidence_id) {
            if existing.stream_seq != candidate.stream_seq
                || existing.processing_state != candidate.processing_state
                || existing.memory_ids != candidate.memory_ids
            {
                return Err(RetrieveError::ConflictingOverlayEvidence);
            }
            continue;
        }
        overlay_by_evidence.insert(candidate.evidence_id, candidate);
    }
    let mut overlay = overlay_by_evidence.into_values().collect::<Vec<_>>();
    overlay.sort_by_key(|candidate| (candidate.stream_seq, candidate.evidence_id));

    Ok(PrivateReadServingCandidates {
        memory_ids: Vec::new(),
        overlay,
        serving_version: envelope.serving_version.clone(),
    })
}

/// Resolves opaque Qdrant candidates through PostgreSQL and hydrates them with the RYW overlay
/// under one repeatable-read snapshot. The Qdrant score controls only candidate order; identity,
/// liveness, body hash, authorization and final content all come from PG.
pub async fn materialize_private_read_serving(
    pool: &RuntimeDbPool,
    consistency_token: Option<&str>,
    authorization: &AuthorizationScope,
    family: &StreamFamily,
    projection_version: &str,
    embedding_version: &str,
    semantic: &[DenseCandidate],
) -> Result<MaterializedPrivateReadServing, RetrieveError> {
    materialize_private_read_serving_about(
        pool,
        consistency_token,
        authorization,
        family,
        projection_version,
        embedding_version,
        semantic,
        &[],
        None,
    )
    .await
}

/// [`materialize_private_read_serving`] narrowed to memories linked to any of `subject_ids`
/// (§6.1.3 / ADR-0029 D-A). The Qdrant `subject_ids` prefilter the caller applied is not
/// trusted: membership is re-checked against `private.memory_subjects` at the shared PG hydrate
/// gate (`read_materialize::final_memory_ids_about_in_txn`), the same place `include_archived`
/// is enforced. Empty `subject_ids` = the unscoped read. `affect` (§8.5.1 / ADR-0030 D-D) is
/// re-checked at the same gate per annotation on the read-time effective intensity.
#[allow(clippy::too_many_arguments)] // Same fixed argument set as the unscoped entry + two axes.
pub async fn materialize_private_read_serving_about(
    pool: &RuntimeDbPool,
    consistency_token: Option<&str>,
    authorization: &AuthorizationScope,
    family: &StreamFamily,
    projection_version: &str,
    embedding_version: &str,
    semantic: &[DenseCandidate],
    subject_ids: &[SubjectId],
    affect: Option<&AffectFilter>,
) -> Result<MaterializedPrivateReadServing, RetrieveError> {
    let mut txn = pool.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *txn)
        .await?;
    let bodies = materialize_private_read_serving_in_txn(
        &mut txn,
        consistency_token,
        authorization,
        family,
        projection_version,
        embedding_version,
        semantic,
        subject_ids,
        affect,
    )
    .await?;
    let key = family.with_version(projection_version);
    let ledger = close_ledger_in_txn(&mut txn, &key).await?;
    // Grounding derivation is intentionally not reconstructed from bodies here. Until the
    // claim-level resolver is joined to this semantic path, every returned item is reported as
    // not judged instead of being silently treated as current.
    let grounding = GroundingBlock::tally(std::iter::repeat_n(None, bodies.items.len()));
    txn.commit().await?;
    Ok(MaterializedPrivateReadServing {
        bodies,
        ledger,
        grounding,
    })
}

/// Reads the private projection selector through the same authenticated adapter boundary used
/// by final materialization. Gateway uses this value only to address Qdrant; final hydration
/// rechecks it in its own repeatable-read snapshot before trusting any candidate.
pub async fn private_read_projection_selector(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    family: &StreamFamily,
) -> Result<Option<String>, RetrieveError> {
    let mut txn = pool.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *txn)
        .await?;
    set_authorization_local(&mut txn, authorization).await?;
    let version = private_read_projection_selector_in_txn(&mut txn, authorization, family).await?;
    txn.commit().await?;
    Ok(version)
}

async fn private_read_projection_selector_in_txn(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    family: &StreamFamily,
) -> Result<Option<String>, RetrieveError> {
    serving_repo::serving_version_in_txn(txn, authorization, family)
        .await
        .map_err(|error| match error {
            ServingRepoError::Db(error) => RetrieveError::Db(error),
            ServingRepoError::CrossTenant => RetrieveError::CrossTenant,
            ServingRepoError::MissingAuthenticatedUser => RetrieveError::MissingAuthenticatedUser,
        })
}

#[allow(clippy::too_many_arguments)] // Transaction-owned twin of `materialize_private_read_serving_about`.
pub(crate) async fn materialize_private_read_serving_in_txn(
    txn: &mut Txn<'_>,
    consistency_token: Option<&str>,
    authorization: &AuthorizationScope,
    family: &StreamFamily,
    projection_version: &str,
    embedding_version: &str,
    semantic: &[DenseCandidate],
    subject_ids: &[SubjectId],
    affect: Option<&AffectFilter>,
) -> Result<MaterializedBodies, RetrieveError> {
    let envelope = match consistency_token {
        Some(token) => recall_with_overlay_in_txn(txn, token, authorization, family).await?,
        None => {
            set_authorization_local(txn, authorization).await?;
            let serving_version =
                private_read_projection_selector_in_txn(txn, authorization, family).await?;
            RecallEnvelope {
                validated_stream_key: family.with_version(projection_version),
                served_by_projection: true,
                overlay: Vec::new(),
                contiguous_done_prefix: 0,
                serving_version,
            }
        }
    };
    if envelope.validated_stream_key != family.with_version(projection_version) {
        return Err(RetrieveError::UntrustedStreamFamily);
    }
    if envelope.serving_version.as_deref() != Some(projection_version) {
        return Err(RetrieveError::ServingProjectionChanged);
    }
    let mut serving = private_read_serving_candidates(&[], &envelope)?;
    let point_ids = semantic
        .iter()
        .map(|candidate| match candidate.point_id {
            PointId::Uuid(id) => Ok(ProjectionPointId::new(id)),
            PointId::Num(_) => Err(RetrieveError::UnsupportedPrivateProjectionPointId),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let resolved = resolve_private_memory_points_in_txn(
        txn,
        authorization,
        family,
        projection_version,
        embedding_version,
        &point_ids,
    )
    .await?;
    let mut seen = HashSet::new();
    serving.memory_ids = resolved
        .into_iter()
        .map(|candidate| candidate.memory_id.0)
        .filter(|memory_id| seen.insert(*memory_id))
        .collect();
    materialize_final_bodies_about_in_txn(
        txn,
        authorization,
        family,
        &envelope.validated_stream_key,
        &serving.memory_ids,
        &serving.overlay,
        // Q3/ADR-0024 D-C: recall.search excludes archived rows at this shared hydrate gate
        // (the Qdrant points stay; PG filters). memory.get is the only caller passing true.
        false,
        // §6.1.3/ADR-0029 D-A: subject membership re-check at the same gate.
        subject_ids,
        // §8.5.1/ADR-0030 D-D: affect re-check (per annotation, effective intensity) too.
        affect,
    )
    .await
    .map_err(RetrieveError::FinalMaterialization)
}

/// Top-level read-your-writes entry. The token only proves the requested overlay boundary;
/// authenticated authorization and the five-column stream family arrive independently from
/// the request/router, and no token field may select either.
pub async fn recall_with_overlay(
    pool: &RuntimeDbPool,
    token: &str,
    authorization: &AuthorizationScope,
    family: &StreamFamily,
) -> Result<RecallEnvelope, RetrieveError> {
    let mut txn = pool.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *txn)
        .await?;
    let envelope = recall_with_overlay_in_txn(&mut txn, token, authorization, family).await?;
    txn.commit().await?;
    Ok(envelope)
}

/// Transaction-owned RYW decision. The trusted authorization and stream family come from the
/// request route; the opaque token only supplies the ledger boundary. The caller owns the
/// transaction/snapshot and may continue with final body, grounding, and ledger reads before
/// its single commit.
///
/// This decides only the PG delta. When `served_by_projection` is false, callers must union
/// normal serving candidates with [`RecallEnvelope::overlay`] before final authorization and
/// lifecycle materialization; lag never replaces the serving result set with overlay alone.
pub(crate) async fn recall_with_overlay_in_txn(
    txn: &mut Txn<'_>,
    token: &str,
    authorization: &AuthorizationScope,
    family: &StreamFamily,
) -> Result<RecallEnvelope, RetrieveError> {
    let claims = decode_consistency_token(token)?;
    let authorization = validate_scope(&claims, authorization)?;
    validate_stream_family(&claims, family)?;
    validate_expiry(&claims, OffsetDateTime::now_utc())?;
    let key = family.with_version(claims.projection_version.clone());

    set_authorization_local(txn, &authorization).await?;
    validate_issued_token_in_txn(txn, &key, &claims).await?;
    let serving_version =
        private_read_projection_selector_in_txn(txn, &authorization, family).await?;
    let serving_highwater = serving_projection_highwater_in_txn(txn, &key).await?;
    let prefix = contiguous_done_prefix_in_txn(txn, &key).await?;
    if serving_highwater >= claims.stream_seq {
        return Ok(RecallEnvelope {
            validated_stream_key: key,
            served_by_projection: true,
            overlay: Vec::new(),
            contiguous_done_prefix: prefix,
            serving_version,
        });
    }
    let overlay = pg_delta_overlay_in_txn(
        txn,
        &authorization,
        &key,
        serving_highwater,
        claims.stream_seq,
    )
    .await?;
    Ok(RecallEnvelope {
        validated_stream_key: key,
        served_by_projection: false,
        overlay,
        contiguous_done_prefix: prefix,
        serving_version,
    })
}

/// §78.2 "DB enum 与 Rust enum 走 contract test 对账": [`ProcessingState::ALL`] must list
/// exactly `projection.stream_log`'s `state` CHECK values, in order, both directions. Runs
/// against the real migration file text, not a live DB (mirrors `jobs::contract_tests`).
#[cfg(test)]
mod contract_tests {
    use super::*;

    const MIGRATION_SQL: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/0007_projection.sql"
    ));

    fn stream_log_table_sql() -> &'static str {
        let start = MIGRATION_SQL
            .find("CREATE TABLE projection.stream_log (")
            .expect("migration must define projection.stream_log");
        let end = MIGRATION_SQL[start..]
            .find(");\n")
            .expect("unterminated projection.stream_log table definition")
            + start;
        &MIGRATION_SQL[start..end]
    }

    fn state_check_values() -> Vec<String> {
        let table_sql = stream_log_table_sql();
        let needle = "state       text NOT NULL DEFAULT 'ISSUED' CHECK (state IN";
        let after_needle = table_sql
            .find(needle)
            .expect("projection.stream_log has no `state ... CHECK (state IN` clause")
            + needle.len();
        let open = table_sql[after_needle..]
            .find('(')
            .expect("CHECK IN clause missing opening paren")
            + after_needle
            + 1;
        let close = table_sql[open..]
            .find(')')
            .expect("unterminated CHECK IN (...) clause")
            + open;
        table_sql[open..close]
            .split(',')
            .map(|s| s.trim().trim_matches('\'').to_string())
            .collect()
    }

    #[test]
    fn processing_state_matches_stream_log_check_constraint() {
        let db = state_check_values();
        let rust: Vec<String> = ProcessingState::ALL
            .iter()
            .map(|s| s.as_db_str().to_string())
            .collect();
        assert_eq!(
            db, rust,
            "ProcessingState::ALL must list every stream_log.state in DB order"
        );
    }

    #[test]
    fn settled_ok_matches_spec_three_variant_set() {
        // §15.2/§15.4 verbatim: SETTLED_OK = DONE | SKIPPED_BY_POLICY | TOMBSTONED.
        let settled: Vec<&str> = ProcessingState::ALL
            .iter()
            .copied()
            .filter(|s| s.is_settled_ok())
            .map(ProcessingState::as_db_str)
            .collect();
        assert_eq!(settled, vec!["DONE", "SKIPPED_BY_POLICY", "TOMBSTONED"]);
    }

    #[test]
    fn token_round_trips_through_hex_encoding() {
        let claims = TokenClaims {
            tenant_id: Uuid::new_v4(),
            workspace_id: Some(Uuid::new_v4()),
            scope_kind: "workspace".to_string(),
            scope_id: Uuid::new_v4(),
            domain: "knowledge".to_string(),
            projection_kind: "ingest".to_string(),
            projection_version: "v1".to_string(),
            stream_seq: 42,
            commit_seq: 1000,
            issued_at: OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
            expires_at: OffsetDateTime::from_unix_timestamp(1_700_003_600).unwrap(),
        };
        let token = issue_consistency_token(&claims);
        assert!(
            token.chars().all(|c| c.is_ascii_hexdigit()),
            "token must be pure hex"
        );
        let decoded = decode_consistency_token(&token).expect("round trip must decode");
        assert_eq!(decoded, claims);
    }

    #[test]
    fn token_round_trips_with_no_workspace() {
        let claims = TokenClaims {
            tenant_id: Uuid::new_v4(),
            workspace_id: None,
            scope_kind: "tenant".to_string(),
            scope_id: Uuid::new_v4(),
            domain: "knowledge".to_string(),
            projection_kind: "ingest".to_string(),
            projection_version: "v1".to_string(),
            stream_seq: 1,
            commit_seq: 1,
            issued_at: OffsetDateTime::from_unix_timestamp(0).unwrap(),
            expires_at: OffsetDateTime::from_unix_timestamp(3600).unwrap(),
        };
        let token = issue_consistency_token(&claims);
        let decoded = decode_consistency_token(&token).expect("round trip must decode");
        assert_eq!(decoded.workspace_id, None);
        assert_eq!(decoded, claims);
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(matches!(
            decode_consistency_token("not hex at all!!"),
            Err(RetrieveError::TokenMalformed(_))
        ));
        assert!(matches!(
            decode_consistency_token(""),
            Err(RetrieveError::TokenMalformed(_))
        ));
        assert!(matches!(
            decode_consistency_token("abc"),
            Err(RetrieveError::TokenMalformed(_))
        ));
    }

    fn auth(tenant_id: Uuid, workspaces: impl IntoIterator<Item = Uuid>) -> AuthorizationScope {
        AuthorizationScope::new(
            TenantId(tenant_id),
            humaux_domain::identity::PrincipalId::new(),
            Some(humaux_domain::ids::UserId::new()),
            humaux_domain::identity::BoundedSet::new(workspaces.into_iter().map(WorkspaceId))
                .expect("test workspace set"),
        )
    }

    fn tenant_claims(tenant_id: Uuid) -> TokenClaims {
        TokenClaims {
            tenant_id,
            workspace_id: None,
            scope_kind: "tenant".to_string(),
            scope_id: tenant_id,
            domain: "knowledge".to_string(),
            projection_kind: "ingest".to_string(),
            projection_version: "v1".to_string(),
            stream_seq: 7,
            commit_seq: 7,
            issued_at: OffsetDateTime::from_unix_timestamp(0).unwrap(),
            expires_at: OffsetDateTime::from_unix_timestamp(3600).unwrap(),
        }
    }

    #[test]
    fn validate_scope_rejects_cross_tenant() {
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let claims = tenant_claims(tenant_a);
        assert!(matches!(
            validate_scope(&claims, &auth(tenant_b, [])),
            Err(RetrieveError::CrossTenant)
        ));
        assert!(validate_scope(&claims, &auth(tenant_a, [])).is_ok());
    }

    #[test]
    fn validate_scope_only_allows_authorized_workspace_narrowing() {
        let tenant = Uuid::new_v4();
        let workspace_a = Uuid::new_v4();
        let workspace_b = Uuid::new_v4();
        let mut claims = tenant_claims(tenant);
        claims.scope_kind = "workspace".to_string();
        claims.scope_id = workspace_a;
        claims.workspace_id = Some(workspace_a);

        assert!(matches!(
            validate_scope(&claims, &auth(tenant, [workspace_b])),
            Err(RetrieveError::CrossWorkspace)
        ));
        assert_eq!(
            validate_scope(&claims, &auth(tenant, [workspace_a]))
                .expect("authorized workspace may narrow")
                .allowed_workspace_ids()
                .len(),
            1
        );
        claims.scope_kind = "unrecognized".to_string();
        assert!(matches!(
            validate_scope(&claims, &auth(tenant, [workspace_a])),
            Err(RetrieveError::UnknownScopeKind)
        ));
    }

    #[test]
    fn private_read_serving_canonicalizes_ryw_overlay_without_semantic_candidates() {
        let memory_a = Uuid::from_u128(1);
        let memory_b = Uuid::from_u128(2);
        let evidence_a = Uuid::from_u128(11);
        let evidence_b = Uuid::from_u128(12);
        let envelope = RecallEnvelope {
            validated_stream_key: StreamKey::new(
                TenantId(Uuid::from_u128(99)),
                "tenant",
                Uuid::from_u128(99),
                "knowledge",
                "ingest",
                "v1",
            ),
            served_by_projection: false,
            overlay: vec![
                OverlayCandidate {
                    stream_seq: 2,
                    evidence_id: evidence_b,
                    processing_state: ProcessingState::Processing,
                    memory_ids: vec![memory_b],
                },
                OverlayCandidate {
                    stream_seq: 1,
                    evidence_id: evidence_a,
                    processing_state: ProcessingState::Issued,
                    memory_ids: vec![memory_b, memory_a, memory_a],
                },
                OverlayCandidate {
                    stream_seq: 1,
                    evidence_id: evidence_a,
                    processing_state: ProcessingState::Issued,
                    memory_ids: vec![memory_a, memory_b],
                },
            ],
            contiguous_done_prefix: 0,
            serving_version: Some("v1".to_owned()),
        };
        let serving = private_read_serving_candidates(&[], &envelope)
            .expect("RYW overlay composes without a semantic identity registry");

        assert!(serving.memory_ids.is_empty());
        assert_eq!(serving.serving_version.as_deref(), Some("v1"));
        assert_eq!(serving.overlay.len(), 2);
        assert_eq!(serving.overlay[0].evidence_id, evidence_a);
        assert_eq!(serving.overlay[0].processing_state, ProcessingState::Issued);
        assert_eq!(serving.overlay[0].memory_ids, vec![memory_a, memory_b]);
        assert_eq!(serving.overlay[1].evidence_id, evidence_b);
        assert_eq!(
            serving.overlay[1].processing_state,
            ProcessingState::Processing
        );
    }

    #[test]
    fn private_read_serving_keeps_a_caught_up_overlay_empty() {
        let envelope = RecallEnvelope {
            validated_stream_key: StreamKey::new(
                TenantId(Uuid::from_u128(99)),
                "tenant",
                Uuid::from_u128(99),
                "knowledge",
                "ingest",
                "v1",
            ),
            served_by_projection: true,
            overlay: Vec::new(),
            contiguous_done_prefix: 0,
            serving_version: Some("v1".to_owned()),
        };

        let serving = private_read_serving_candidates(&[], &envelope)
            .expect("caught-up envelope remains a serving-only read");

        assert!(serving.memory_ids.is_empty());
        assert!(serving.overlay.is_empty());
    }

    #[test]
    fn private_read_serving_requires_a_projection_registry_for_semantic_candidates() {
        let envelope = RecallEnvelope {
            validated_stream_key: StreamKey::new(
                TenantId(Uuid::from_u128(99)),
                "tenant",
                Uuid::from_u128(99),
                "knowledge",
                "ingest",
                "v1",
            ),
            served_by_projection: true,
            overlay: Vec::new(),
            contiguous_done_prefix: 0,
            serving_version: Some("v1".to_owned()),
        };

        let result = private_read_serving_candidates(
            &[crate::qdrant::DenseCandidate {
                point_id: crate::qdrant::PointId::Uuid(Uuid::from_u128(1)),
                score: 0.9,
            }],
            &envelope,
        );

        assert!(matches!(
            result,
            Err(RetrieveError::SemanticCandidateProjectionRegistryUnavailable)
        ));
    }
}
