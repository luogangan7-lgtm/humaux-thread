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

use humaux_domain::evidence::{EvidenceOriginClass, EvidencePayloadSha256};
use humaux_domain::ids::TenantId;
use humaux_projection::stream::StreamKey;

use crate::postgres::RuntimeDbPool;

/// DB-layer failure plus the one domain-shaped terminal case this module can produce
/// (§34.1 `BATCH_EXHAUSTED`). Adapter-local, not `humaux_domain::error::ErrorCode` itself —
/// same "adapter boundary, not Domain" reasoning as `email::OutboxError` / `jobs::JobsError`;
/// the MCP/REST mapping to `ErrorCode::Conflict` (§52 "CONFLICT") with a `BATCH_EXHAUSTED`
/// reason string is `crates/protocol/src/error_map.rs`'s job, out of this task's scope.
#[derive(Debug)]
pub enum RememberError {
    Db(sqlx::Error),
    /// `cmd.batch_id` named a batch with zero remaining `state = 'ISSUED'` tickets (already
    /// fully redeemed, or every ticket expired). §34.1: "拒绝，错误码 BATCH_EXHAUSTED，不得
    /// 自动补票" — this variant carries no ticket, the transaction is rolled back (nothing this
    /// call would have written — Evidence, event, stream_log row, outbox row — is committed).
    BatchExhausted,
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
            Self::BatchExhausted => write!(f, "BATCH_EXHAUSTED (§34.1)"),
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
    /// `projection.stream_log` / `stream_checkpoints` locator (§15.1's other five PK columns,
    /// beyond `tenant_id`) — which pipeline this Evidence's stream_seq is issued against.
    pub scope_kind: String,
    pub scope_id: Uuid,
    pub domain: String,
    pub projection_kind: String,
    pub projection_version: String,
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
    /// §15.5 opaque read-your-writes token. **Placeholder encoding** — a plain delimited
    /// string, not signed/encrypted. §15.5's actual consumer (`recall`/`context` accepting
    /// `consistency_token=<token>`, and the overlay-lower-bound logic reading it) is T3.8's
    /// job, not this task's; that task owns the real encoding and may change this format
    /// entirely. This task's literal scope per §60 is only "sign consistency_token" as
    /// `remember`'s last step — never parsed by anything in this crate.
    pub consistency_token: String,
    pub ticket_ordinal: Option<i32>,
    pub batch_remaining: Option<i64>,
    pub status: &'static str,
}

/// Sets `humaux.tenant_id` for the remainder of `txn` (`SET LOCAL`, §62) — same pattern and
/// same non-bindable-parameter reasoning as `jobs::set_tenant_local`; `tenant_id: Uuid`'s
/// `Display` never emits anything but canonical lowercase hex, so this format is not an
/// injection surface the way a user-supplied string would be.
async fn set_tenant_local(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
) -> Result<(), RememberError> {
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .execute(&mut **txn)
        .await?;
    Ok(())
}

/// §15.1: `next_commit_seq` — the sole caller of `ops.commit_seq_seq`
/// (`migrations/0043_commit_sequence.sql`). "仅审计总序，不参与完整性判定" (§15) — nothing
/// downstream may branch on this value's magnitude, only carry it through to `stream_log` /
/// `outbox` for audit/cross-stream reconciliation.
async fn next_commit_seq(
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
async fn create_evidence_object(
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

/// `insert_event_subtype` — `private.events` shares its primary key with the
/// `evidence_objects` row it subtypes (`events.event_id REFERENCES evidence_objects.evidence_id`,
/// 1:1, no separate id minted here).
async fn insert_event_subtype(
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
async fn issue_stream_log_row(
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
async fn insert_outbox(
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

/// §15.5 `consistency_token` — see [`RememberAccepted::consistency_token`]'s doc for why this
/// is a plain placeholder encoding, not signed. Binds exactly the fields §15.5 lists: tenant/
/// scope, stream key, `stream_seq`, `commit_seq` (audit only), `issued_at`.
fn issue_consistency_token(
    key: &StreamKey,
    stream_seq: i64,
    commit_seq: i64,
    issued_at: OffsetDateTime,
) -> String {
    format!(
        "ct1:{}:{}:{}:{}:{}:{}:{}:{}:{}",
        key.tenant_id.0,
        key.scope_kind,
        key.scope_id,
        key.domain,
        key.projection_kind,
        key.projection_version,
        stream_seq,
        commit_seq,
        issued_at.unix_timestamp(),
    )
}

/// §60 transaction B, verbatim step order. Commits — or, on any error (including
/// [`RememberError::BatchExhausted`]), rolls back everything this call attempted: dropping
/// `txn` without calling `.commit()` on the early-return path undoes the Evidence/event rows
/// already inserted earlier in this same function (§60 "DB 权威写与 Outbox 在同一事务" cuts
/// both ways — nothing here is durable until every step, including the ticket redemption,
/// succeeds).
pub async fn remember(
    pool: &RuntimeDbPool,
    cmd: RememberCommand,
) -> Result<RememberAccepted, RememberError> {
    let key = StreamKey::new(
        TenantId(cmd.tenant_id),
        cmd.scope_kind.clone(),
        cmd.scope_id,
        cmd.domain.clone(),
        cmd.projection_kind.clone(),
        cmd.projection_version.clone(),
    );

    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, cmd.tenant_id).await?;

    let commit_seq = next_commit_seq(&mut txn).await?;
    let evidence_id = create_evidence_object(&mut txn, &cmd).await?;
    insert_event_subtype(&mut txn, evidence_id, &cmd).await?;

    // §34.1: batch_id present -> redeem (BATCH_EXHAUSTED rolls the whole transaction back,
    // never partially — see this function's doc); absent -> ticket untouched, both output
    // fields stay None (expected_source = "none").
    let ticket = match cmd.batch_id {
        Some(batch_id) => Some(redeem_ticket(&mut txn, batch_id, evidence_id).await?),
        None => None,
    };

    let stream_seq = issue_stream_log_row(&mut txn, &key, commit_seq).await?;
    insert_outbox(
        &mut txn,
        cmd.tenant_id,
        commit_seq,
        stream_seq,
        "EVIDENCE_ACCEPTED",
        evidence_id,
    )
    .await?;

    let issued_at = OffsetDateTime::now_utc();
    let consistency_token = issue_consistency_token(&key, stream_seq, commit_seq, issued_at);

    txn.commit().await?;

    Ok(RememberAccepted {
        evidence_id,
        processing_handle: evidence_id.to_string(),
        consistency_token,
        ticket_ordinal: ticket.as_ref().map(|t| t.ordinal),
        batch_remaining: ticket.as_ref().map(|t| t.remaining),
        status: "accepted",
    })
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
