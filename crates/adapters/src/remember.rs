//! `adapters::remember` — T3.2 `remember` (§34 / §34.1 / §60 transaction B).
//!
//! [`remember`] is `role_gateway`'s single write path for Evidence acceptance. It runs against
//! [`RuntimeDbPool`] — never [`crate::batch::begin_batch`]'s [`crate::postgres::BatchIssuerDbPool`]
//! (§34.2 point 1: `RememberService`/this module holds no `BatchIssuerPort`). `role_gateway` has
//! no `INSERT` on `private.ingest_tickets` (`migrations/0011_roles_and_grants.sql`, §60.1's
//! authorization table) — every SQL statement below against that table is an `UPDATE`, never an
//! `INSERT`; that omission is not a style choice, it is the one property `crates/adapters/tests/
//! outbox_batch_remember.rs`'s G23-1c fault injection exists to prove is load-bearing.
//!
//! Steps, in one transaction, verbatim to §60's pseudocode:
//! `next_commit_seq` -> `create_evidence_object` -> `insert_event_subtype` -> `redeem_ticket`
//! (skipped when `cmd.batch_id` is `None`, §34.1 "不带 batch_id 的调用方行为一字不变") ->
//! `issue_stream_log_row` -> `insert_outbox` -> sign `consistency_token`.

use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use humaux_domain::affect::MoodHalfLife;
use humaux_domain::error::ErrorCode;
use humaux_domain::evidence::{EvidenceOriginClass, EvidencePayloadSha256};
use humaux_domain::ids::TenantId;
use humaux_domain::subject::SubjectDeclaration;
use humaux_projection::stream::StreamKey;

use crate::affect_repo::{self, AffectInput, AffectParent};
use crate::postgres::RuntimeDbPool;
use crate::retrieve::{self, TokenClaims};

/// DB-layer failure plus the one domain-shaped terminal case this module can produce
/// (§34.1 `BATCH_EXHAUSTED`). Adapter-local, not `humaux_domain::error::ErrorCode` itself —
/// same "adapter boundary, not Domain" reasoning as `email::OutboxError` / `jobs::JobsError`;
/// the MCP/REST mapping to `ErrorCode::Conflict` (§52 "CONFLICT") with a `BATCH_EXHAUSTED`
/// reason string is `crates/protocol/src/error_map.rs`'s job, out of this task's scope.
#[derive(Debug)]
pub enum RememberError {
    Db(sqlx::Error),
    /// The caller-supplied consistency-token expiry is not after a validation clock reading.
    /// `remember` returns this only with no committed writes; a late check rolls its transaction
    /// back after a lock wait consumed the deadline.
    ConsistencyTokenExpiryNotFuture,
    /// `cmd.batch_id` named a batch with zero remaining `state = 'ISSUED'` tickets (already
    /// fully redeemed, or every ticket expired). §34.1: "拒绝，错误码 BATCH_EXHAUSTED，不得
    /// 自动补票" — this variant carries no ticket, the transaction is rolled back (nothing this
    /// call would have written — Evidence, event, stream_log row, outbox row — is committed).
    BatchExhausted,
    /// §6.1.3 rules 1/2 (ADR-0028): the command's `subjects` declaration did not resolve under
    /// the tenant's RLS (unknown, merged-away or another tenant's id/key ⇒ `INVALID_INPUT`), or
    /// the resolve itself failed. Raised BEFORE the first write, so nothing is committed.
    Subject(ErrorCode),
    /// §8.5.1 (ADR-0030 D-C): an affect in `cmd.affects` was refused — its `target_subject`
    /// did not resolve (`INVALID_INPUT`, raised BEFORE the first write like [`Self::Subject`]),
    /// or a MOOD arrived with no half-life policy (`DEPENDENCY_UNAVAILABLE`).
    Affect(ErrorCode),
    /// §11.2.1 ingress (ADR-0032 D-A): the command's tenant has no reasoning domain this
    /// Evidence could be processed under — the process-configured domain is another tenant's
    /// and the on-behalf-of user owns no ACTIVE domain here. The tenant is not provisioned for
    /// writes (`DEPENDENCY_UNAVAILABLE`); raised BEFORE the first write, nothing is committed.
    ReasoningDomainUnresolved,
}

impl From<sqlx::Error> for RememberError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl std::fmt::Display for RememberError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "remember DB error: {e}"),
            Self::ConsistencyTokenExpiryNotFuture => {
                write!(f, "consistency_token expiry must be after issuance time")
            }
            Self::BatchExhausted => write!(f, "BATCH_EXHAUSTED (§34.1)"),
            Self::Subject(code) => write!(f, "subject declaration rejected: {code}"),
            Self::Affect(code) => write!(f, "affect declaration rejected: {code}"),
            Self::ReasoningDomainUnresolved => {
                write!(f, "no reasoning domain for this tenant (§11.2.1)")
            }
        }
    }
}

impl std::error::Error for RememberError {}

/// `origin_class` DB wire form — literally the domain enum's variant name (§8.7's 9-variant
/// closed set doubles as `private.evidence_objects.origin_class`'s `CHECK (... IN (...))`
/// list). Kept local to this module rather than added as an `EvidenceOriginClass::as_db_str`
/// method on the domain type: that type's own doc says verbatim-string pinning "is deferred to
/// the §78.2 DB↔Rust contract-test card, not covered here" (T1.6) — this module is that card's
/// first real consumer, and the contract test below pins this mapping against the live CHECK
/// list, satisfying §78.2 without editing a file this task does not own.
fn origin_class_db_str(c: EvidenceOriginClass) -> &'static str {
    match c {
        EvidenceOriginClass::DirectUserInput => "DirectUserInput",
        EvidenceOriginClass::UserConfirmed => "UserConfirmed",
        EvidenceOriginClass::TenantAdmin => "TenantAdmin",
        EvidenceOriginClass::AuthenticatedAgent => "AuthenticatedAgent",
        EvidenceOriginClass::TrustedConnector => "TrustedConnector",
        EvidenceOriginClass::ToolResult => "ToolResult",
        EvidenceOriginClass::UploadedArtifact => "UploadedArtifact",
        EvidenceOriginClass::ExternalContent => "ExternalContent",
        EvidenceOriginClass::SystemMigration => "SystemMigration",
    }
}

/// `remember()` input — already-resolved/validated fields for `private.evidence_objects` +
/// `private.events`, one call = one `evidence_kind = 'EVENT'` row pair (the only subtype this
/// task's `remember` MCP contract produces, §34; `'ARTIFACT'` rows are a different write path,
/// out of scope). Mapping raw MCP `remember(content, kind, source, ...)` input into this shape
/// (client-facing `kind` string -> one of the 9 `event_kind` CHECK values, `source.type` ->
/// [`EvidenceOriginClass`], etc.) is the MCP handler / `application::remember`'s job, not this
/// module's — §60's own pseudocode also starts from an opaque `cmd`, not raw JSON.
#[derive(Debug, Clone)]
pub struct RememberCommand {
    pub tenant_id: Uuid,
    /// Authenticated actor from the trusted gateway request context. This is deliberately
    /// distinct from `visibility_user_id`: workspace-shared Evidence has no visibility user,
    /// but RLS still needs the acting member in `humaux.user_id`.
    pub authorization_user_id: Option<Uuid>,
    /// `projection.stream_log` / `stream_checkpoints` locator (§15.1's other five PK columns,
    /// beyond `tenant_id`) — which pipeline this Evidence's stream_seq is issued against.
    pub scope_kind: String,
    pub scope_id: Uuid,
    pub domain: String,
    pub projection_kind: String,
    pub projection_version: String,
    /// Explicit policy/configuration input for §15.5's token `expiry or policy` field.
    /// It is validated against the one issuance clock captured by [`remember`] before any DB
    /// write. This is an API break: callers must provide their own configured/policy expiry;
    /// the adapter intentionally has no business TTL default.
    pub consistency_token_expires_at: OffsetDateTime,
    /// `Some` = batch write (redeem one ticket, §34.1 row 1); `None` = single sync write, ticket
    /// untouched (§34.1 row 3, `expected_source = "none"`).
    pub batch_id: Option<Uuid>,
    /// §48.0① sole-constructor anchor — accepted as the already-hashed
    /// [`EvidencePayloadSha256`] (never a raw `[u8; 32]`) so this command struct cannot be
    /// built with a digest that skipped `evidence::payload_sha256`'s "no trim/NFC/NFKC/
    /// transcode" contract; the type has no `pub` field and no second constructor, so the only
    /// way a caller has one to put here is to have called that function.
    pub payload_sha256: EvidencePayloadSha256,
    pub data_class: String,
    pub origin_class: EvidenceOriginClass,
    pub origin_principal_id: Option<Uuid>,
    pub origin_connector_id: Option<Uuid>,
    pub visibility_class: String,
    pub visibility_user_id: Option<Uuid>,
    pub visibility_workspace_id: Option<Uuid>,
    pub reasoning_domain_id: Uuid,
    pub occurred_at: Option<OffsetDateTime>,
    pub event_kind: String,
    pub event_payload: serde_json::Value,
    /// §6.1.3 rules 1/2 (ADR-0028): the explicit `subject_ids` / `subject_keys` this Evidence is
    /// about. Resolved under the tenant's RLS before the first write and recorded on
    /// `private.evidence_subjects` in the same transaction; an unknown id/key is
    /// [`RememberError::Subject`]`(INVALID_INPUT)` and nothing is written.
    pub subjects: SubjectDeclaration,
    /// §8.5.1 (ADR-0030 D-C): the affects declared at `remember.put`. Their `target_subject`
    /// ids/keys are resolved with `subjects` BEFORE the first write; the rows land on
    /// `private.evidence_affects` in this same transaction (0157) and reach every memory born
    /// from this Evidence through the `memory_evidence` PRIMARY trigger. Empty = none.
    pub affects: Vec<AffectInput>,
    /// The frozen MOOD half-life policy stamped onto MOOD rows; required only when `affects`
    /// carries a MOOD ([`RememberError::Affect`]`(DEPENDENCY_UNAVAILABLE)` otherwise).
    pub mood_half_life: Option<MoodHalfLife>,
}

/// `remember()`'s accepted result (§34 / §15.5, verbatim field set).
#[derive(Debug, Clone)]
pub struct RememberAccepted {
    pub evidence_id: Uuid,
    /// §34: "通过 processing_handle / memory / context 查询处理结果" — opaque to the caller,
    /// currently just the evidence id's string form (one Evidence : one handle, §34's own
    /// "一个 Evidence 后续可产生多条 memory_id" is a *query-time* fan-out, not something this
    /// handle itself encodes).
    pub processing_handle: String,
    /// §15.5 opaque read-your-writes token, emitted only after the Evidence transaction commits.
    /// Its one encoder is [`crate::retrieve::issue_consistency_token`]; it is not an
    /// authentication token, and `recall` validates tenant/workspace against the authenticated
    /// request context separately.
    pub consistency_token: String,
    pub ticket_ordinal: Option<i32>,
    pub batch_remaining: Option<i64>,
    pub status: &'static str,
}

/// Installs both RLS GUCs for this transaction. The user setting is always overwritten so a
/// pooled connection never inherits a prior request; a headless write uses the UUID sentinel
/// required by the current direct UUID cast in the user-visibility policy.
async fn set_authorization_local(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    user_id: Option<Uuid>,
) -> Result<(), RememberError> {
    sqlx::query(
        "SELECT set_config('humaux.tenant_id', $1, true), \
                set_config('humaux.user_id', $2, true)",
    )
    .bind(tenant_id.to_string())
    .bind(user_id.unwrap_or_else(Uuid::nil).to_string())
    .execute(&mut **txn)
    .await?;
    Ok(())
}

/// §11.2.1 ingress (ADR-0032 D-A): which `control.private_reasoning_domains` row this Evidence
/// is processed under, chosen inside the write transaction from the command's OWN tenant (the
/// tenant-isolation policy on that table is the fence; `set_authorization_local` has already
/// pinned it). Precedence, one indexed read: first `cmd.reasoning_domain_id` — the
/// process-configured domain (`HUMAUX_GATEWAY_REMEMBER_REASONING_DOMAIN_ID`) — when it is one
/// of THIS tenant's ACTIVE domains (the deployment's own tenant keeps its boot-time domain
/// byte-for-byte; the private worker distills exactly that domain); otherwise the ACTIVE domain
/// owned by the on-behalf-of user (§11.2.1 "用户直接输入 -> reasoning_domain = 该用户"); neither
/// ⇒ [`RememberError::ReasoningDomainUnresolved`] — "无法确定 processing principal": the tenant
/// is not provisioned for writes, nothing is written.
/// The single-column FK of 0004 never refused a cross-tenant domain (RI bypasses RLS); 0159's
/// composite FK now does, so a bypass of this resolve is a `23503`, not a silent cross-tenant
/// pointer.
// ponytail: earliest user-owned ACTIVE domain when a user owns several; the §11.2.1
// reasoning_domain_grants binding replaces rule 2 once grants are issued anywhere.
async fn resolve_reasoning_domain(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    cmd: &RememberCommand,
) -> Result<Uuid, RememberError> {
    sqlx::query_scalar(
        "SELECT reasoning_domain_id FROM control.private_reasoning_domains \
         WHERE tenant_id = $1 AND status = 'ACTIVE' \
           AND (reasoning_domain_id = $2 OR owner_user_id = $3) \
         ORDER BY reasoning_domain_id = $2 DESC, created_at, reasoning_domain_id \
         LIMIT 1",
    )
    .bind(cmd.tenant_id)
    .bind(cmd.reasoning_domain_id)
    .bind(cmd.authorization_user_id)
    .fetch_optional(&mut **txn)
    .await?
    .ok_or(RememberError::ReasoningDomainUnresolved)
}

/// §15.1: `next_commit_seq` — the sole caller of `ops.commit_seq_seq`
/// (`migrations/0043_commit_sequence.sql`). "仅审计总序，不参与完整性判定" (§15) — nothing
/// downstream may branch on this value's magnitude, only carry it through to `stream_log` /
/// `outbox` for audit/cross-stream reconciliation.
pub(crate) async fn next_commit_seq(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<i64, RememberError> {
    Ok(sqlx::query_scalar("SELECT nextval('ops.commit_seq_seq')")
        .fetch_one(&mut **txn)
        .await?)
}

/// `create_evidence_object` — one `private.evidence_objects` row, `evidence_kind = 'EVENT'`
/// fixed (see [`RememberCommand`]'s doc). `evidence_id` / `created_at` are the table's own
/// `DEFAULT uuidv7()` / `DEFAULT now()`, never bound here — same reasoning `begin_batch` gives
/// for asking Postgres for `uuidv7()` rather than minting client-side.
pub(crate) async fn create_evidence_object(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    cmd: &RememberCommand,
) -> Result<Uuid, RememberError> {
    let row = sqlx::query(
        "INSERT INTO private.evidence_objects \
           (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
            origin_principal_id, origin_connector_id, visibility_class, visibility_user_id, \
            visibility_workspace_id, reasoning_domain_id, occurred_at) \
         VALUES ($1, 'EVENT', decode($2, 'hex'), $3, $4, $5, $6, $7, $8, $9, $10, $11) \
         RETURNING evidence_id",
    )
    .bind(cmd.tenant_id)
    // `decode(hex, 'hex')` round-trips `EvidencePayloadSha256::to_hex()` back into `bytea` —
    // the type has no raw-byte accessor by design (§48.0①'s doc only exposes `to_hex()`), so
    // binding through its one public projection is the only option, not a workaround.
    .bind(cmd.payload_sha256.to_hex())
    .bind(&cmd.data_class)
    .bind(origin_class_db_str(cmd.origin_class))
    .bind(cmd.origin_principal_id)
    .bind(cmd.origin_connector_id)
    .bind(&cmd.visibility_class)
    .bind(cmd.visibility_user_id)
    .bind(cmd.visibility_workspace_id)
    .bind(cmd.reasoning_domain_id)
    .bind(cmd.occurred_at)
    .fetch_one(&mut **txn)
    .await?;
    Ok(row.try_get("evidence_id")?)
}

/// The token's optional workspace is a stream-routing binding, not Evidence visibility. A
/// USER_PRIVATE Evidence in a workspace stream therefore carries the workspace in its token
/// even though its stored `visibility_workspace_id` is necessarily NULL.
fn token_workspace_id(key: &StreamKey) -> Option<Uuid> {
    (key.scope_kind == "workspace").then_some(key.scope_id)
}

/// `insert_event_subtype` — `private.events` shares its primary key with the
/// `evidence_objects` row it subtypes (`events.event_id REFERENCES evidence_objects.evidence_id`,
/// 1:1, no separate id minted here).
pub(crate) async fn insert_event_subtype(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    evidence_id: Uuid,
    cmd: &RememberCommand,
) -> Result<(), RememberError> {
    sqlx::query("INSERT INTO private.events (event_id, event_kind, payload) VALUES ($1, $2, $3)")
        .bind(evidence_id)
        .bind(&cmd.event_kind)
        .bind(&cmd.event_payload)
        .execute(&mut **txn)
        .await?;
    Ok(())
}

/// One redeemed ticket's observable result (§34 `ticket_ordinal` / `batch_remaining`).
struct RedeemedTicket {
    ordinal: i32,
    remaining: i64,
}

/// `redeem_ticket` — §60's merged `maybe_issue_ingest_ticket`+`redeem_ticket_if_any` replacement,
/// "纯 UPDATE" (§60 "这个事务里没有任何一条 SQL 能增加票数"). Claims the minimum-`ordinal`
/// still-`ISSUED` ticket in `batch_id` (`FOR UPDATE SKIP LOCKED` so two concurrent `remember`
/// calls against the same batch never redeem the same ticket twice, mirroring
/// `jobs::claim`'s SKIP LOCKED reasoning) and flips it to `REDEEMED`. Zero rows updated —
/// no `ISSUED` ticket left, exhausted or the batch never existed — is [`RememberError::BatchExhausted`]
/// (§34.1), never a silent no-op and never a fallback `INSERT` of a fresh ticket ("绝不补票").
async fn redeem_ticket(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    batch_id: Uuid,
    event_id: Uuid,
) -> Result<RedeemedTicket, RememberError> {
    let updated = sqlx::query(
        "UPDATE private.ingest_tickets \
            SET redeemed_event_id = $2, state = 'REDEEMED' \
          WHERE ticket_id = ( \
                  SELECT ticket_id FROM private.ingest_tickets \
                   WHERE batch_id = $1 AND state = 'ISSUED' \
                   ORDER BY ordinal ASC \
                   FOR UPDATE SKIP LOCKED \
                   LIMIT 1 \
                ) \
          RETURNING ordinal",
    )
    .bind(batch_id)
    .bind(event_id)
    .fetch_optional(&mut **txn)
    .await?;

    let Some(row) = updated else {
        return Err(RememberError::BatchExhausted);
    };
    let ordinal: i32 = row.try_get("ordinal")?;

    let remaining: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM private.ingest_tickets WHERE batch_id = $1 AND state = 'ISSUED'",
    )
    .bind(batch_id)
    .fetch_one(&mut **txn)
    .await?;

    Ok(RedeemedTicket { ordinal, remaining })
}

/// `issue_stream_log_row` — §15.1 verbatim: atomically bumps `stream_checkpoints.
/// issued_highwater` (creating the checkpoint row on first use — `role_gateway`'s `INSERT`
/// grant on that table is scoped to exactly its six PK columns, §60.1's grant table, so this
/// bootstrap `INSERT` never touches `issued_highwater` itself; the `UPDATE` right after is what
/// actually increments it, matching the column-scoped `UPDATE(issued_highwater)` grant) and
/// inserts the matching `stream_log` row in the same transaction as the `commit_seq` it
/// carries — the six stream-identity columns plus `stream_seq`/`commit_seq`, verbatim to
/// §60's pseudocode. **Does not** carry `evidence_id`: `migrations/0045_stream_log_evidence_id
/// .sql` briefly added that column, but `migrations/0046_stream_log_evidence_id_via_outbox.sql`
/// (same wave, T3.8) dropped it again in favor of joining `stream_log` to `ops.outbox` on
/// `(tenant_id, commit_seq)` — 0046's own comment names this function as the reason: T3.1/T3.2
/// owns §60's pseudocode field list and must not add a column to it for a different task's
/// read path.
pub(crate) async fn issue_stream_log_row(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: &StreamKey,
    commit_seq: i64,
) -> Result<i64, RememberError> {
    sqlx::query(
        "INSERT INTO projection.stream_checkpoints \
           (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version) \
         DO NOTHING",
    )
    .bind(key.tenant_id.0)
    .bind(&key.scope_kind)
    .bind(key.scope_id)
    .bind(&key.domain)
    .bind(&key.projection_kind)
    .bind(&key.projection_version)
    .execute(&mut **txn)
    .await?;

    let stream_seq: i64 = sqlx::query_scalar(
        "UPDATE projection.stream_checkpoints \
            SET issued_highwater = issued_highwater + 1 \
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
            AND projection_kind = $5 AND projection_version = $6 \
          RETURNING issued_highwater",
    )
    .bind(key.tenant_id.0)
    .bind(&key.scope_kind)
    .bind(key.scope_id)
    .bind(&key.domain)
    .bind(&key.projection_kind)
    .bind(&key.projection_version)
    .fetch_one(&mut **txn)
    .await?;

    sqlx::query(
        "INSERT INTO projection.stream_log \
           (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
            stream_seq, commit_seq) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(key.tenant_id.0)
    .bind(&key.scope_kind)
    .bind(key.scope_id)
    .bind(&key.domain)
    .bind(&key.projection_kind)
    .bind(&key.projection_version)
    .bind(stream_seq)
    .bind(commit_seq)
    .execute(&mut **txn)
    .await?;

    Ok(stream_seq)
}

/// `insert_outbox` — §14's transactional-outbox row, same transaction as the Evidence write it
/// announces (§14 "DB write -> INSERT outbox_event -> COMMIT"; `ops.outbox` is this schema's
/// canonical name for what §14/§60's prose calls `outbox_event`, per `migrations/
/// 0008_ops_core.sql`'s table comment).
/// §14/§15.1: `role_consolidation_worker`'s own private-Evidence-shaped outbox event for a
/// published `private.memory_rollups` row (§78.2: pinned here, not reused from
/// `EVIDENCE_ACCEPTED`, so a reader can tell "a rollup published" from "an Evidence was
/// remembered" without inspecting the row's other columns).
pub(crate) const MEMORY_PUBLISHED: &str = "MEMORY_PUBLISHED";

/// §14/§15.1: reserved sibling of [`MEMORY_PUBLISHED`] for a future consolidation undo/
/// retraction op — not emitted by this crate today, named now so the two literals stay next to
/// each other (§78.2 "no second literal elsewhere").
// ponytail: unused until an undo/retraction write path exists; add its call site then.
#[allow(dead_code)]
pub(crate) const MEMORY_LIFECYCLE: &str = "MEMORY_LIFECYCLE";

pub(crate) async fn insert_outbox(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    commit_seq: i64,
    stream_seq: i64,
    event_type: &str,
    evidence_id: Uuid,
) -> Result<(), RememberError> {
    sqlx::query(
        "INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(tenant_id)
    .bind(commit_seq)
    .bind(stream_seq)
    .bind(event_type)
    .bind(evidence_id)
    .execute(&mut **txn)
    .await?;
    Ok(())
}

/// The uncommitted outcome of [`remember_in_txn`]. The receipt/orchestrator caller receives
/// only facts emitted by the successful SQL writes, then decides when to commit its larger
/// transaction. It must call [`Self::into_accepted`] only after that transaction commits.
#[derive(Debug, Clone)]
pub struct RememberPending {
    accepted: RememberAccepted,
    stream_key: StreamKey,
    commit_seq: i64,
    stream_seq: i64,
}

impl RememberPending {
    #[must_use]
    pub fn accepted(&self) -> &RememberAccepted {
        &self.accepted
    }

    #[must_use]
    pub fn into_accepted(self) -> RememberAccepted {
        self.accepted
    }

    #[must_use]
    pub fn stream_key(&self) -> &StreamKey {
        &self.stream_key
    }

    #[must_use]
    pub const fn commit_seq(&self) -> i64 {
        self.commit_seq
    }

    #[must_use]
    pub const fn stream_seq(&self) -> i64 {
        self.stream_seq
    }
}

/// §60 transaction B without a commit. This is the atomic gateway-write seam: the caller may
/// append receipt, quota, and audit rows in the same transaction, then commit exactly once.
/// Errors leave rollback to the owning transaction; no SQL here commits independently.
pub async fn remember_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    mut cmd: RememberCommand,
) -> Result<RememberPending, RememberError> {
    // This check is deliberately inside the reusable path, before its first write. The public
    // wrapper's fast check only avoids opening a transaction for an already-expired command.
    if cmd.consistency_token_expires_at <= OffsetDateTime::now_utc() {
        return Err(RememberError::ConsistencyTokenExpiryNotFuture);
    }

    let key = StreamKey::new(
        TenantId(cmd.tenant_id),
        cmd.scope_kind.clone(),
        cmd.scope_id,
        cmd.domain.clone(),
        cmd.projection_kind.clone(),
        cmd.projection_version.clone(),
    );

    set_authorization_local(txn, cmd.tenant_id, cmd.authorization_user_id).await?;

    // §11.2.1 (ADR-0032 D-A): the reasoning domain is the caller tenant's, resolved per request
    // under that tenant's RLS BEFORE the first write — never the process's boot constant
    // stamped onto another tenant's Evidence (0159's composite FK is the DB-side invariant).
    cmd.reasoning_domain_id = resolve_reasoning_domain(txn, &cmd).await?;

    // §6.1.3 rules 1/2 (ADR-0028): resolve the declaration BEFORE the first write so an unknown
    // id/key rejects with nothing committed — no Evidence, no ticket, no outbox row.
    let subjects =
        crate::subject_repo::resolve_declaration_in_txn(txn, cmd.tenant_id, &cmd.subjects)
            .await
            .map_err(RememberError::Subject)?;
    // §8.5.1 (ADR-0030 D-C): the affects' target subjects resolve under the same rule, also
    // before the first write — an unknown target refuses the whole put with nothing accepted.
    let affect_targets = affect_repo::resolve_targets_in_txn(txn, cmd.tenant_id, &cmd.affects)
        .await
        .map_err(RememberError::Affect)?;

    let commit_seq = next_commit_seq(txn).await?;
    let evidence_id = create_evidence_object(txn, &cmd).await?;
    insert_event_subtype(txn, evidence_id, &cmd).await?;
    // The declaration rides on the Evidence in this same transaction (the memory is born later
    // in the Distill hop and inherits it through link_memory_subjects rule 3a).
    crate::subject_repo::declare_evidence_in_txn(txn, cmd.tenant_id, evidence_id, &subjects)
        .await?;
    // The affect declaration rides on the Evidence the same way (0157 evidence_affects, the
    // sole affect issuer); the newborn memory's copy is the memory_evidence PRIMARY trigger's.
    affect_repo::insert_in_txn(
        txn,
        cmd.tenant_id,
        AffectParent::Evidence,
        evidence_id,
        &cmd.affects,
        &affect_targets,
        cmd.mood_half_life,
    )
    .await
    .map_err(RememberError::Affect)?;

    // §34.1: batch_id present -> redeem (BATCH_EXHAUSTED rolls the whole transaction back,
    // never partially — see this function's doc); absent -> ticket untouched, both output
    // fields stay None (expected_source = "none").
    let ticket = match cmd.batch_id {
        Some(batch_id) => Some(redeem_ticket(txn, batch_id, evidence_id).await?),
        None => None,
    };

    let stream_seq = issue_stream_log_row(txn, &key, commit_seq).await?;
    insert_outbox(
        txn,
        cmd.tenant_id,
        commit_seq,
        stream_seq,
        "EVIDENCE_ACCEPTED",
        evidence_id,
    )
    .await?;

    // Sign at the actual issuance point, after every potentially blocking write. If a lock
    // wait consumed the deadline, returning here drops `txn` and rolls every write back.
    let issued_at = OffsetDateTime::now_utc();
    if cmd.consistency_token_expires_at <= issued_at {
        return Err(RememberError::ConsistencyTokenExpiryNotFuture);
    }
    let consistency_token = retrieve::issue_consistency_token(&TokenClaims {
        tenant_id: key.tenant_id.0,
        // This is routing identity from the trusted stream key, independent of Evidence's
        // row-level visibility. `recall` compares it with its authenticated request context.
        workspace_id: token_workspace_id(&key),
        scope_kind: key.scope_kind.clone(),
        scope_id: key.scope_id,
        domain: key.domain.clone(),
        projection_kind: key.projection_kind.clone(),
        projection_version: key.projection_version.clone(),
        stream_seq,
        commit_seq,
        issued_at,
        expires_at: cmd.consistency_token_expires_at,
    });

    Ok(RememberPending {
        accepted: RememberAccepted {
            evidence_id,
            processing_handle: evidence_id.to_string(),
            consistency_token,
            ticket_ordinal: ticket.as_ref().map(|t| t.ordinal),
            batch_remaining: ticket.as_ref().map(|t| t.remaining),
            status: "accepted",
        },
        stream_key: key,
        commit_seq,
        stream_seq,
    })
}

/// Public one-shot wrapper. It preserves the original accepted response and owns the commit;
/// callers that need an atomic receipt/quota/audit bundle use [`remember_in_txn`] instead.
pub async fn remember(
    pool: &RuntimeDbPool,
    cmd: RememberCommand,
) -> Result<RememberAccepted, RememberError> {
    if cmd.consistency_token_expires_at <= OffsetDateTime::now_utc() {
        return Err(RememberError::ConsistencyTokenExpiryNotFuture);
    }
    let mut txn = pool.pool().begin().await?;
    let pending = remember_in_txn(&mut txn, cmd).await?;
    txn.commit().await?;
    Ok(pending.into_accepted())
}

/// §78.2 "DB enum 与 Rust enum 走 contract test 对账", scoped to [`origin_class_db_str`] (see
/// its doc for why the mapping lives here rather than on the domain type). Runs against the
/// real migration file text, no live DB needed.
#[cfg(test)]
mod contract_tests {
    use super::*;

    const MIGRATION_SQL: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/0004_private_evidence_memory.sql"
    ));

    fn origin_class_check_values() -> Vec<String> {
        let needle = "origin_class IN (";
        let start = MIGRATION_SQL
            .find(needle)
            .expect("migration must define an origin_class CHECK (origin_class IN (...))")
            + needle.len();
        let close = MIGRATION_SQL[start..]
            .find(')')
            .expect("unterminated origin_class CHECK list")
            + start;
        MIGRATION_SQL[start..close]
            .split(',')
            .map(|s| s.trim().trim_matches('\'').to_string())
            .collect()
    }

    #[test]
    fn origin_class_db_str_matches_check_constraint_for_every_variant() {
        let db = origin_class_check_values();
        let rust: Vec<String> = [
            EvidenceOriginClass::DirectUserInput,
            EvidenceOriginClass::UserConfirmed,
            EvidenceOriginClass::TenantAdmin,
            EvidenceOriginClass::AuthenticatedAgent,
            EvidenceOriginClass::TrustedConnector,
            EvidenceOriginClass::ToolResult,
            EvidenceOriginClass::UploadedArtifact,
            EvidenceOriginClass::ExternalContent,
            EvidenceOriginClass::SystemMigration,
        ]
        .into_iter()
        .map(|c| origin_class_db_str(c).to_string())
        .collect();
        assert_eq!(
            db, rust,
            "origin_class_db_str must list every CHECK value, in DB order"
        );
    }
}
