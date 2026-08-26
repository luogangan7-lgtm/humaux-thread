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
//! neither, and this module does not own that file to add them). `crate::remember`'s (T3.2)
//! own `issue_consistency_token` is a private `fn` scoped to that module, and its doc calls
//! its own format "**Placeholder encoding**... T3.8's job... may change this format
//! entirely" — this module's [`issue_consistency_token`] is the real one that doc points at;
//! `remember()` has not been switched over to it (that would edit a file this task does not
//! own), so the two token formats coexist as separate, non-interoperable encodings for now
//! (documented on both sides), not a "唯一构造点" violation of the same mechanism.

use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use humaux_domain::ids::TenantId;
use humaux_projection::stream::StreamKey;

use crate::postgres::RuntimeDbPool;

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
        }
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

    fn parse(s: &str) -> Result<Self, RetrieveError> {
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

/// §15.5 "consistency_token 只提供 read-your-writes 约束...不可跨 tenant/workspace 使用" —
/// fail-closed: any mismatch is an `Err`, never a silent downgrade to "ignore the token".
pub fn validate_scope(
    claims: &TokenClaims,
    requested_tenant_id: Uuid,
    requested_workspace_id: Option<Uuid>,
) -> Result<(), RetrieveError> {
    if claims.tenant_id != requested_tenant_id {
        return Err(RetrieveError::CrossTenant);
    }
    if claims.workspace_id != requested_workspace_id {
        return Err(RetrieveError::CrossWorkspace);
    }
    Ok(())
}

/// Sets `humaux.tenant_id` for the remainder of `txn` (§6.1 RLS context) — same technique and
/// same non-bind-parameter rationale as `crate::jobs::set_tenant_local` (a `Uuid`'s `Display`
/// only ever emits the canonical lowercase-hex form, so this formatted string carries no
/// injectable characters).
async fn set_tenant_local(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
) -> Result<(), RetrieveError> {
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
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
    key: &StreamKey,
) -> Result<i64, RetrieveError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, key.tenant_id.0).await?;

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
    .fetch_one(&mut *txn)
    .await?;
    txn.commit().await?;

    Ok(row.try_get::<i64, _>("prefix")?)
}

/// `projection.stream_checkpoints.projection_highwater` for this exact stream key — "已对检
/// 索可见的边界" (§15.3), i.e. how far serving Qdrant has actually caught up. `0` (not an
/// error) when no checkpoint row exists yet for this stream — an unstarted stream has served
/// nothing, which is the correct starting point for the overlay decision below, not a
/// distinct failure mode.
pub async fn serving_projection_highwater(
    pool: &RuntimeDbPool,
    key: &StreamKey,
) -> Result<i64, RetrieveError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, key.tenant_id.0).await?;

    let row = sqlx::query(
        "SELECT projection_highwater FROM projection.stream_checkpoints
         WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
           AND domain = $4 AND projection_kind = $5 AND projection_version = $6",
    )
    .bind(key.tenant_id.0)
    .bind(&key.scope_kind)
    .bind(key.scope_id)
    .bind(&key.domain)
    .bind(&key.projection_kind)
    .bind(&key.projection_version)
    .fetch_optional(&mut *txn)
    .await?;
    txn.commit().await?;

    match row {
        Some(r) => Ok(r.try_get::<i64, _>("projection_highwater")?),
        None => Ok(0),
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
    key: &StreamKey,
    serving_highwater: i64,
    up_to_stream_seq_inclusive: i64,
) -> Result<Vec<OverlayCandidate>, RetrieveError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, key.tenant_id.0).await?;

    let rows = sqlx::query(
        "SELECT sl.stream_seq, sl.state, ob.evidence_id,
                array_agg(DISTINCT me.memory_id) FILTER (WHERE me.memory_id IS NOT NULL)
                  AS memory_ids
         FROM projection.stream_log sl
         JOIN ops.outbox ob ON ob.tenant_id = sl.tenant_id AND ob.commit_seq = sl.commit_seq
         LEFT JOIN private.memory_evidence me ON me.evidence_id = ob.evidence_id
         WHERE sl.tenant_id = $1 AND sl.scope_kind = $2 AND sl.scope_id = $3
           AND sl.domain = $4 AND sl.projection_kind = $5 AND sl.projection_version = $6
           AND sl.stream_seq > $7 AND sl.stream_seq <= $8
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
    .fetch_all(&mut *txn)
    .await?;
    txn.commit().await?;

    rows.iter()
        .map(|r| {
            let state: String = r.try_get("state")?;
            Ok(OverlayCandidate {
                stream_seq: r.try_get("stream_seq")?,
                evidence_id: r.try_get("evidence_id")?,
                processing_state: ProcessingState::parse(&state)?,
                memory_ids: r
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
    pub served_by_projection: bool,
    pub overlay: Vec<OverlayCandidate>,
    /// §15.4 `contiguous_done_prefix` at decision time — the overlay's completeness bound,
    /// not a filter on `overlay`'s contents (see [`pg_delta_overlay`]'s doc).
    pub contiguous_done_prefix: i64,
}

/// Top-level entry point: decode `token`, reject cross-tenant/cross-workspace use, then decide
/// whether serving already covers the write or a PG overlay is needed (§15.5). The only
/// caller-supplied identity is the opaque token string plus the request's own authenticated
/// tenant/workspace — no stream key, no `stream_seq`: exactly the "Agent 不需要理解...也不能
/// 自行构造 token" contract.
pub async fn recall_with_overlay(
    pool: &RuntimeDbPool,
    token: &str,
    requested_tenant_id: Uuid,
    requested_workspace_id: Option<Uuid>,
) -> Result<RecallEnvelope, RetrieveError> {
    let claims = decode_consistency_token(token)?;
    validate_scope(&claims, requested_tenant_id, requested_workspace_id)?;

    let key = claims.stream_key();
    let serving_hw = serving_projection_highwater(pool, &key).await?;
    if serving_hw >= claims.stream_seq {
        return Ok(RecallEnvelope {
            served_by_projection: true,
            overlay: vec![],
            contiguous_done_prefix: contiguous_done_prefix(pool, &key).await?,
        });
    }

    let prefix = contiguous_done_prefix(pool, &key).await?;
    let overlay = pg_delta_overlay(pool, &key, serving_hw, claims.stream_seq).await?;
    Ok(RecallEnvelope {
        served_by_projection: false,
        overlay,
        contiguous_done_prefix: prefix,
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

    fn sample_claims(tenant_id: Uuid, workspace_id: Option<Uuid>) -> TokenClaims {
        TokenClaims {
            tenant_id,
            workspace_id,
            scope_kind: "workspace".to_string(),
            scope_id: Uuid::new_v4(),
            domain: "knowledge".to_string(),
            projection_kind: "ingest".to_string(),
            projection_version: "v1".to_string(),
            stream_seq: 7,
            commit_seq: 7,
            issued_at: OffsetDateTime::from_unix_timestamp(0).unwrap(),
            expires_at: OffsetDateTime::from_unix_timestamp(3600).unwrap(),
        }
    }

    /// §15.5 "不可跨 tenant...使用" — pure, no DB needed (the DB-backed test additionally
    /// proves the same rule end to end through `recall_with_overlay`, see
    /// `tests/retrieve_read_your_writes.rs`).
    #[test]
    fn validate_scope_rejects_cross_tenant() {
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let claims = sample_claims(tenant_a, None);
        assert!(matches!(
            validate_scope(&claims, tenant_b, None),
            Err(RetrieveError::CrossTenant)
        ));
        assert!(validate_scope(&claims, tenant_a, None).is_ok());
    }

    /// §15.5 "...workspace 使用" — both directions of the mismatch (different workspace, and
    /// tenant-shared-vs-workspace-scoped) must reject, not just literal inequality of two
    /// `Some` values.
    #[test]
    fn validate_scope_rejects_cross_workspace() {
        let tenant = Uuid::new_v4();
        let workspace_a = Uuid::new_v4();
        let workspace_b = Uuid::new_v4();
        let claims = sample_claims(tenant, Some(workspace_a));

        assert!(matches!(
            validate_scope(&claims, tenant, Some(workspace_b)),
            Err(RetrieveError::CrossWorkspace)
        ));
        assert!(
            matches!(
                validate_scope(&claims, tenant, None),
                Err(RetrieveError::CrossWorkspace)
            ),
            "tenant-shared request against a workspace-bound token must also reject"
        );
        assert!(validate_scope(&claims, tenant, Some(workspace_a)).is_ok());
    }
}
