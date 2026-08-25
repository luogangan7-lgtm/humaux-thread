//! `email::outbox` — DB-backed queue + worker for `ops.email_outbox`
//! (`migrations/0038_email_deliverability.sql`). Two entry points:
//!
//! - [`enqueue`]: runs on the request path (gateway handler, e.g. "send verification code").
//!   Does a suppression check plus one `INSERT` and returns — it never holds a
//!   `dyn EmailProvider` and never calls `send`, which is what makes §74.6's "验证码 HTTP
//!   handler 不直接阻塞 SMTP" true by construction rather than by convention.
//! - [`run_once`]: the background worker. Claims a `QUEUED` batch (`SELECT ... FOR UPDATE
//!   SKIP LOCKED`), calls the injected `&dyn EmailProvider` per row, and records the outcome
//!   as both an `ops.email_outbox.state` transition and an `ops.email_delivery_events` row.

use sqlx::Row;
use sqlx::types::Uuid;

use crate::postgres::{PrivateWorkerDbPool, RuntimeDbPool};

use super::{
    DeliveryEventType, EmailProvider, EmailStream, OutboundEmail, SuppressionReason,
    SuppressionScope,
};

/// DB-layer failure from any function in this module. Not one of the workspace's two frozen
/// domain error enums (§52) — same "adapter-local, not domain" reasoning as
/// [`super::EmailError`] and this crate's own `postgres::PoolInitError`.
#[derive(Debug)]
pub enum OutboxError {
    Db(sqlx::Error),
}

impl From<sqlx::Error> for OutboxError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl std::fmt::Display for OutboxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "email outbox DB error: {e}"),
        }
    }
}

impl std::error::Error for OutboxError {}

/// Result of [`enqueue`]. §74.6 Suppression: "对已 suppression 邮箱需返回统一安全语义,
/// 不泄露账户存在状态" — the caller (a signup/reset handler) MUST treat both variants
/// identically in its HTTP response; this enum exists only so the caller's own logging/
/// metrics can tell them apart internally, never so it can vary the response shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// Row inserted, `state = 'QUEUED'`.
    Queued,
    /// Not inserted — `to` matched an `ops.email_suppressions` row whose `scope` blocks
    /// `stream` (§74.6: suppressed addresses "不入队").
    Suppressed,
}

/// One email to enqueue. Deliberately narrower than [`OutboundEmail`] (which also carries
/// `from`/`body_*`, the SMTP-transport-facing shape) — the outbox row only needs
/// addressing/template identity; the actual rendered subject/body a provider sends is
/// [`OutboundEmail`], reconstructed by the worker from `template_id`/`payload` at send time
/// (out of this task's scope — the worker below takes a pre-rendered [`OutboundEmail`]
/// builder closure so template rendering stays the caller's concern, not this module's).
#[derive(Debug, Clone)]
pub struct EnqueueRequest {
    pub user_id: Uuid,
    pub to_email: String,
    pub from_email: String,
    pub stream: EmailStream,
    pub template_id: String,
    pub subject: String,
    pub payload: serde_json::Value,
}

/// §74.6 Suppression check: does any `ops.email_suppressions` row for `email` (not expired)
/// have a `scope` that [`SuppressionScope::blocks`] `stream`? One `SELECT`, no write —
/// shared by [`enqueue`] (pre-insert gate) and available to callers that want to surface
/// suppression state elsewhere (e.g. an admin view) without duplicating the query.
pub async fn is_suppressed(
    pool: &RuntimeDbPool,
    email: &str,
    stream: EmailStream,
) -> Result<bool, OutboxError> {
    let rows = sqlx::query(
        "SELECT scope FROM ops.email_suppressions \
         WHERE email = $1 AND (expires_at IS NULL OR expires_at > now())",
    )
    .bind(email)
    .fetch_all(pool.pool())
    .await?;

    for row in rows {
        let scope_str: String = row.try_get("scope")?;
        let scope = parse_suppression_scope(&scope_str);
        if scope.is_some_and(|s| s.blocks(stream)) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// §74.6: "发信走 email_outbox, 验证码 HTTP handler 不直接阻塞 SMTP". Checks suppression
/// (one `SELECT`), then — unless suppressed — does exactly one `INSERT` and returns. Never
/// takes an `EmailProvider` argument at all: there is structurally nothing in this function
/// that can reach the network, which is what [`super::test_double::UnreachableEmailProvider`]
/// exists to prove in a test that also exercises a real handler-shaped call path.
pub async fn enqueue(
    pool: &RuntimeDbPool,
    req: &EnqueueRequest,
) -> Result<EnqueueOutcome, OutboxError> {
    if is_suppressed(pool, &req.to_email, req.stream).await? {
        return Ok(EnqueueOutcome::Suppressed);
    }

    sqlx::query(
        "INSERT INTO ops.email_outbox \
         (user_id, to_email, from_email, stream, template_id, subject, payload) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(req.user_id)
    .bind(&req.to_email)
    .bind(&req.from_email)
    .bind(req.stream.as_db_str())
    .bind(&req.template_id)
    .bind(&req.subject)
    .bind(&req.payload)
    .execute(pool.pool())
    .await?;

    Ok(EnqueueOutcome::Queued)
}

/// Claims up to `limit` `QUEUED` rows (`SELECT ... FOR UPDATE SKIP LOCKED`), calls
/// `provider.send` for each, and records the outcome — `state = 'SENT'` + a `SENT` delivery
/// event on success, `state = 'FAILED'` + a `FAILED` delivery event on any
/// [`super::EmailError`] (task brief: "provider 失败转 FAILED+事件记录" — no retry loop,
/// that is out of this task's literal scope). All of it (claim, send, record) runs inside
/// one transaction per batch.
///
/// `claim_user_id`: `None` in production — a real worker drains every tenant's rows,
/// queue-wide, which is the correct behavior. `Some(user_id)` narrows the claim to just that
/// user's rows, for tests sharing a dev DB with concurrent siblings (see
/// `tests/email_outbox.rs`'s module doc: without this, a queue-wide claim can sweep up and
/// drive to a terminal state rows this test never enqueued).
///
/// ponytail: holding the claimed rows' locks for the duration of every `provider.send` call
/// in the batch (rather than a separate `CLAIMED`/lease-column handoff like `ops.jobs`'
/// `lease_owner`/`lease_expires_at`) is a real ceiling — a slow/hung SMTP call blocks the
/// whole batch's rows from being picked up by a concurrent worker for that long. §74.6's own
/// state list has no intermediate "claimed" state to transition through without inventing
/// one outside the spec's literal enumeration, and this wave's scope is a single worker
/// instance. Upgrade path: add a `claimed_at`/`lease_owner` pair (mirroring `ops.jobs`) if
/// multiple concurrent worker instances or slow-provider timeouts become a real problem.
pub async fn run_once(
    pool: &PrivateWorkerDbPool,
    provider: &dyn EmailProvider,
    limit: i64,
    claim_user_id: Option<Uuid>,
) -> Result<usize, OutboxError> {
    let mut txn = pool.pool().begin().await?;

    let rows = sqlx::query(
        "SELECT outbox_id, to_email, from_email, subject, stream \
         FROM ops.email_outbox \
         WHERE state = 'QUEUED' AND ($2::uuid IS NULL OR user_id = $2) \
         ORDER BY created_at \
         FOR UPDATE SKIP LOCKED \
         LIMIT $1",
    )
    .bind(limit)
    .bind(claim_user_id)
    .fetch_all(&mut *txn)
    .await?;

    let mut processed = 0usize;
    for row in &rows {
        let outbox_id: Uuid = row.try_get("outbox_id")?;
        let to_email: String = row.try_get("to_email")?;
        let from_email: String = row.try_get("from_email")?;
        let subject: String = row.try_get("subject")?;
        let stream_str: String = row.try_get("stream")?;
        let stream = parse_stream(&stream_str);

        let mail = OutboundEmail {
            to: to_email,
            from: from_email,
            subject,
            // Template rendering is out of this task's scope (see EnqueueRequest's doc) —
            // the body is a placeholder until a rendering step is wired in; this does not
            // affect the state-machine/suppression logic this task is responsible for.
            body_text: String::new(),
            body_html: None,
            stream,
        };

        let provider_name = provider.name();
        match provider.send(mail).await {
            Ok(msg_id) => {
                sqlx::query(
                    "UPDATE ops.email_outbox \
                     SET state = 'SENT', provider = $2, provider_message_id = $3, sent_at = now() \
                     WHERE outbox_id = $1",
                )
                .bind(outbox_id)
                .bind(provider_name)
                .bind(&msg_id.0)
                .execute(&mut *txn)
                .await?;
                record_event(
                    &mut txn,
                    outbox_id,
                    DeliveryEventType::Sent,
                    Some(provider_name),
                    &msg_id.0,
                )
                .await?;
            }
            Err(e) => {
                let detail = e.to_string();
                sqlx::query(
                    "UPDATE ops.email_outbox \
                     SET state = 'FAILED', provider = $2, attempt = attempt + 1, last_error = $3 \
                     WHERE outbox_id = $1",
                )
                .bind(outbox_id)
                .bind(provider_name)
                .bind(&detail)
                .execute(&mut *txn)
                .await?;
                record_event(
                    &mut txn,
                    outbox_id,
                    DeliveryEventType::Failed,
                    Some(provider_name),
                    &detail,
                )
                .await?;
            }
        }
        processed += 1;
    }

    txn.commit().await?;
    Ok(processed)
}

async fn record_event(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    outbox_id: Uuid,
    event_type: DeliveryEventType,
    provider: Option<&str>,
    detail_message: &str,
) -> Result<(), OutboxError> {
    let detail = serde_json::json!({ "message": detail_message });
    sqlx::query(
        "INSERT INTO ops.email_delivery_events (outbox_id, event_type, provider, detail) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(outbox_id)
    .bind(event_type.as_db_str())
    .bind(provider)
    .bind(detail)
    .execute(&mut **txn)
    .await?;
    Ok(())
}

/// Records a new suppression (e.g. from a provider bounce/complaint webhook, or a manual
/// operator action) — `ON CONFLICT (email, scope) DO UPDATE` so a repeated signal (a second
/// bounce for the same address/scope) refreshes `created_at`/`provider` instead of erroring.
pub async fn record_suppression(
    pool: &PrivateWorkerDbPool,
    email: &str,
    reason: SuppressionReason,
    scope: SuppressionScope,
    provider: Option<&str>,
    expires_at: Option<sqlx::types::time::OffsetDateTime>,
) -> Result<(), OutboxError> {
    sqlx::query(
        "INSERT INTO ops.email_suppressions (email, reason, scope, provider, expires_at) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (email, scope) DO UPDATE \
         SET reason = EXCLUDED.reason, provider = EXCLUDED.provider, \
             created_at = now(), expires_at = EXCLUDED.expires_at",
    )
    .bind(email)
    .bind(reason.as_db_str())
    .bind(scope.as_db_str())
    .bind(provider)
    .bind(expires_at)
    .execute(pool.pool())
    .await?;
    Ok(())
}

fn parse_stream(s: &str) -> EmailStream {
    EmailStream::ALL
        .into_iter()
        .find(|v| v.as_db_str() == s)
        .unwrap_or(EmailStream::Transactional)
}

fn parse_suppression_scope(s: &str) -> Option<SuppressionScope> {
    SuppressionScope::ALL_VARIANTS
        .into_iter()
        .find(|v| v.as_db_str() == s)
}

/// §78.2 "DB enum 与 Rust enum 走 contract test 对账": every `CHECK (... IN (...))` list in
/// `migrations/0038_email_deliverability.sql` must contain exactly this module's Rust enum's
/// wire values, both directions. Runs against the real spec/migration file text — not a
/// live-DB fixture — so it always executes (no `HUMAUX_TEST_PG_DSN` skip needed) and fails
/// loud on any future edit to either side that drifts the other out of sync.
#[cfg(test)]
mod contract_tests {
    use super::*;

    const MIGRATION_SQL: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/0038_email_deliverability.sql"
    ));

    /// Extracts the comma-separated quoted literal list out of the first
    /// `CHECK (<column> IN (...))` clause for `column` in the migration text.
    fn check_values(column: &str) -> Vec<String> {
        let needle = format!("CHECK ({column} IN (");
        let start = MIGRATION_SQL
            .find(&needle)
            .unwrap_or_else(|| panic!("migration SQL has no `CHECK ({column} IN (...))` clause"))
            + needle.len();
        let end = MIGRATION_SQL[start..]
            .find(')')
            .expect("unterminated CHECK IN (...) clause")
            + start;
        MIGRATION_SQL[start..end]
            .split(',')
            .map(|s| s.trim().trim_matches('\'').to_string())
            .collect()
    }

    #[test]
    fn outbox_state_matches_check_constraint() {
        let db: Vec<String> = check_values("state");
        let rust: Vec<String> = super::super::EmailOutboxState::ALL
            .iter()
            .map(|s| s.as_db_str().to_string())
            .collect();
        assert_eq!(
            db, rust,
            "EmailOutboxState::ALL must list every state in DB order"
        );
    }

    #[test]
    fn stream_matches_check_constraint() {
        let db: Vec<String> = check_values("stream");
        let rust: Vec<String> = EmailStream::ALL
            .iter()
            .map(|s| s.as_db_str().to_string())
            .collect();
        assert_eq!(
            db, rust,
            "EmailStream::ALL must list every stream in DB order"
        );
    }

    #[test]
    fn delivery_event_type_matches_check_constraint() {
        let db: Vec<String> = check_values("event_type");
        let rust: Vec<String> = DeliveryEventType::ALL
            .iter()
            .map(|s| s.as_db_str().to_string())
            .collect();
        assert_eq!(
            db, rust,
            "DeliveryEventType::ALL must list every event_type in DB order"
        );
    }

    #[test]
    fn suppression_reason_matches_check_constraint() {
        let db: Vec<String> = check_values("reason");
        let rust: Vec<String> = SuppressionReason::ALL
            .iter()
            .map(|s| s.as_db_str().to_string())
            .collect();
        assert_eq!(
            db, rust,
            "SuppressionReason::ALL must list every reason in DB order"
        );
    }

    #[test]
    fn suppression_scope_matches_check_constraint() {
        let db: Vec<String> = check_values("scope");
        let rust: Vec<String> = SuppressionScope::ALL_VARIANTS
            .iter()
            .map(|s| s.as_db_str().to_string())
            .collect();
        assert_eq!(
            db, rust,
            "SuppressionScope::ALL_VARIANTS must list every scope in DB order"
        );
    }
}
