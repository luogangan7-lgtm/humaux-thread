//! `adapters::distill_repo` — SQL half of the Distill hop (ADR-0016), through
//! [`PrivateWorkerDbPool`] only: claim `EVIDENCE_ACCEPTED` outbox rows, load one Evidence, record
//! the §16.1.1 processing run, insert the authorized memories, settle the outbox row.
//!
//! §15.5: one Evidence → 0/1/N `private.memory_records`; §14: the `ops.outbox` row remember wrote
//! is the work item (PENDING → PROCESSING with a lease → DONE | FAILED, or back to PENDING when
//! the attempt was retryable), expired PROCESSING leases are reclaimable. Idempotency (ADR-0016
//! D5): memory rows, the run's completion and the outbox DONE flip commit in ONE transaction
//! fenced on the claimer's `lease_owner`, so a late worker whose lease was reclaimed rolls its
//! inserts back rather than duplicating them.
//!
//! Every SELECT/INSERT here runs under `role_private_worker`'s grants and RLS: `private.events`
//! keeps the §6.1.1 visibility disjunction inline (no headless bypass), so [`load_evidence`]
//! installs the acting user — the Evidence's own `visibility_user_id` when USER_PRIVATE, else the
//! reasoning domain's owner (an ACTIVE member) — before reading the payload.

use humaux_application::consolidate::{ReasoningRouteBindingId, ReasoningRouteBindingVersion};
use humaux_domain::authority::AuthorityClass;
use humaux_domain::evidence::EvidenceOriginClass;
use humaux_domain::memory::MemoryType;
use serde_json::Value;
use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use crate::postgres::PrivateWorkerDbPool;

/// The database error/transaction types the private-worker binary names without depending on
/// `sqlx` itself (same "fewer capability-shaped names in a bin" reasoning as
/// `bins/consolidation-worker/Cargo.toml`).
pub type DbError = sqlx::Error;
pub type DbTransaction<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

/// Opens a transaction on the private worker pool with the tenant pinned and the user GUC at
/// the nil sentinel (`remember::set_authorization_local`'s headless shape) — the read leg's
/// starting point; [`load_evidence`] narrows the user before touching `private.events`.
pub async fn begin_read_context(
    pool: &PrivateWorkerDbPool,
    tenant_id: Uuid,
) -> Result<DbTransaction<'_>, DbError> {
    let mut txn = pool.pool().begin().await?;
    set_rls_context(&mut txn, tenant_id, Uuid::nil()).await?;
    Ok(txn)
}

/// Opens the write-leg transaction with the tenant + acting user installed
/// ([`insert_memory`], [`finish_processing_run`], [`complete_outbox`]).
pub async fn begin_write_context(
    pool: &PrivateWorkerDbPool,
    tenant_id: Uuid,
    user_id: Uuid,
) -> Result<DbTransaction<'_>, DbError> {
    let mut txn = pool.pool().begin().await?;
    set_rls_context(&mut txn, tenant_id, user_id).await?;
    Ok(txn)
}

/// `ops.outbox.event_type` remember writes for an accepted Evidence (§14; the literal lives in
/// `remember::remember_in_txn`'s call site, pinned here for the claim predicate).
pub const EVIDENCE_ACCEPTED: &str = "EVIDENCE_ACCEPTED";

/// Read-side inverse of `remember::origin_class_db_str` (the CHECK list of
/// `migrations/0004_private_evidence_memory.sql`, pinned by the contract test below).
pub(crate) fn origin_class_from_db_str(s: &str) -> Option<EvidenceOriginClass> {
    Some(match s {
        "DirectUserInput" => EvidenceOriginClass::DirectUserInput,
        "UserConfirmed" => EvidenceOriginClass::UserConfirmed,
        "TenantAdmin" => EvidenceOriginClass::TenantAdmin,
        "AuthenticatedAgent" => EvidenceOriginClass::AuthenticatedAgent,
        "TrustedConnector" => EvidenceOriginClass::TrustedConnector,
        "ToolResult" => EvidenceOriginClass::ToolResult,
        "UploadedArtifact" => EvidenceOriginClass::UploadedArtifact,
        "ExternalContent" => EvidenceOriginClass::ExternalContent,
        "SystemMigration" => EvidenceOriginClass::SystemMigration,
        _ => return None,
    })
}

/// `private.memory_records.memory_type` CHECK literal for the six types the Distill contract
/// may emit (write-side inverse of `projection_worker::parse_memory_type`, scoped to them).
pub(crate) const fn memory_type_db_str(memory_type: MemoryType) -> &'static str {
    match memory_type {
        MemoryType::Fact => "FACT",
        MemoryType::Preference => "PREFERENCE",
        MemoryType::Decision => "DECISION",
        MemoryType::Rejection => "REJECTION",
        MemoryType::State => "STATE",
        MemoryType::Issue => "ISSUE",
        MemoryType::Lesson => "LESSON",
        MemoryType::Constraint => "CONSTRAINT",
        MemoryType::Procedure => "PROCEDURE",
        MemoryType::Outcome => "OUTCOME",
        MemoryType::Reference => "REFERENCE",
        MemoryType::Note => "NOTE",
    }
}

const fn authority_class_db_str(class: AuthorityClass) -> &'static str {
    // One mapping for the whole crate (§78.2): consolidate_repo owns it.
    crate::consolidate_repo::authority_class_to_db_str(class)
}

/// One claimed `ops.outbox` work item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimedEvidence {
    pub outbox_id: Uuid,
    pub evidence_id: Uuid,
    pub commit_seq: i64,
    pub stream_seq: i64,
}
/// ADR-0016 D2 (review P2): the Evidence's `origin_principal_id` carries no FK — before it is
/// used as the acting identity of the §11.1 context it must still be an ACTIVE member of the
/// tenant; otherwise the caller falls back to the reasoning domain owner.
pub async fn active_member(
    pool: &PrivateWorkerDbPool,
    tenant_id: Uuid,
    user_id: Uuid,
) -> Result<bool, DbError> {
    let mut txn = pool.pool().begin().await?;
    set_rls_context(&mut txn, tenant_id, user_id).await?;
    let ok: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM control.memberships m \
         WHERE m.tenant_id = $1 AND m.user_id = $2 AND m.state = 'ACTIVE')",
    )
    .bind(tenant_id)
    .bind(user_id)
    .fetch_one(&mut *txn)
    .await?;
    txn.commit().await?;
    Ok(ok)
}

/// Same `set_config(..., true)` pair `remember::set_authorization_local` installs; the user is
/// always overwritten so a pooled connection never inherits a prior row's identity.
async fn set_rls_context(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    user_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "SELECT set_config('humaux.tenant_id', $1, true), set_config('humaux.user_id', $2, true)",
    )
    .bind(tenant_id.to_string())
    .bind(user_id.to_string())
    .execute(&mut **txn)
    .await?;
    Ok(())
}

/// PENDING (or lease-expired PROCESSING) `EVIDENCE_ACCEPTED` rows of one tenant, oldest
/// `commit_seq` first, `FOR UPDATE SKIP LOCKED` (same recipe as `jobs::claim_in_txn`), flipped
/// to PROCESSING under `lease_owner` for `lease_seconds`.
pub async fn claim_pending_evidence(
    pool: &PrivateWorkerDbPool,
    tenant_id: Uuid,
    lease_owner: &str,
    batch: i64,
    lease_seconds: f64,
) -> Result<Vec<ClaimedEvidence>, sqlx::Error> {
    let mut txn = pool.pool().begin().await?;
    set_rls_context(&mut txn, tenant_id, Uuid::nil()).await?;
    let rows = sqlx::query(
        "WITH picked AS ( \
           SELECT outbox_id FROM ops.outbox \
           WHERE tenant_id = $1 AND event_type = $2 AND evidence_id IS NOT NULL \
             AND (status = 'PENDING' \
                  OR (status = 'PROCESSING' AND lease_expires_at < clock_timestamp())) \
           ORDER BY commit_seq \
           FOR UPDATE SKIP LOCKED \
           LIMIT $3 \
         ) \
         UPDATE ops.outbox o \
         SET status = 'PROCESSING', lease_owner = $4, \
             lease_expires_at = clock_timestamp() + make_interval(secs => $5) \
         FROM picked WHERE o.outbox_id = picked.outbox_id \
         RETURNING o.outbox_id, o.evidence_id, o.commit_seq, o.stream_seq",
    )
    .bind(tenant_id)
    .bind(EVIDENCE_ACCEPTED)
    .bind(batch)
    .bind(lease_owner)
    .bind(lease_seconds)
    .fetch_all(&mut *txn)
    .await?;
    txn.commit().await?;
    rows.iter()
        .map(|row| {
            Ok(ClaimedEvidence {
                outbox_id: row.try_get("outbox_id")?,
                evidence_id: row.try_get("evidence_id")?,
                commit_seq: row.try_get("commit_seq")?,
                stream_seq: row.try_get("stream_seq")?,
            })
        })
        .collect()
}

/// §16.1 `context_snapshot_seq`: the highest `commit_seq` this tenant has issued (the memory
/// visibility upper bound the run reads under), taken at claim time.
pub async fn context_snapshot_seq(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, Option<i64>>(
        "SELECT max(commit_seq) FROM ops.outbox WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_one(&mut **txn)
    .await
    .map(Option::unwrap_or_default)
}

/// ADR-0016 D1: the effective Distill binding for `(session tenant, reasoning_domain,
/// PRIVATE_DISTILL_TEXT)` through the narrow `control.current_reasoning_route_binding`
/// SECURITY DEFINER function (migration 0147) — no binding id in configuration.
pub async fn resolve_distill_binding(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    reasoning_domain_id: Uuid,
) -> Result<Option<(ReasoningRouteBindingId, ReasoningRouteBindingVersion)>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT binding_id, binding_version \
         FROM control.current_reasoning_route_binding($1, 'PRIVATE_DISTILL_TEXT')",
    )
    .bind(reasoning_domain_id)
    .fetch_optional(&mut **txn)
    .await?;
    row.map(|row| {
        Ok((
            ReasoningRouteBindingId(row.try_get("binding_id")?),
            ReasoningRouteBindingVersion(row.try_get("binding_version")?),
        ))
    })
    .transpose()
}

/// One Evidence as the hop reads it: the `evidence_objects` row plus its `events` subtype.
#[derive(Debug, Clone)]
pub struct LoadedEvidence {
    pub evidence_id: Uuid,
    pub reasoning_domain_id: Uuid,
    pub origin_class: EvidenceOriginClass,
    /// The DB CHECK literal of `origin_class` (what the envelope shows the model).
    pub origin_class_wire: String,
    pub origin_principal_id: Option<Uuid>,
    pub data_class: String,
    pub visibility_class: String,
    pub visibility_user_id: Option<Uuid>,
    pub visibility_workspace_id: Option<Uuid>,
    pub occurred_at: Option<OffsetDateTime>,
    /// `private.evidence_objects.payload_sha256` verbatim (what `processing_runs.
    /// evidence_payload_sha256[]` records).
    pub payload_sha256: Vec<u8>,
    pub event_kind: String,
    pub payload: Value,
    /// The acting user installed for RLS while reading `payload` — the Evidence's own user
    /// for USER_PRIVATE, else `owner_user_id`; the write leg reuses it.
    pub rls_user_id: Uuid,
}

/// Loads `evidence_id` (must belong to `tenant_id` and `reasoning_domain_id`) and its event
/// payload. `Ok(None)` when the row is not there or not an EVENT (an ARTIFACT has no
/// `events` row — a different hop, §15.5 "0 memories" is not the answer for it either, so it
/// is reported as unavailable and the outbox row fails).
pub async fn load_evidence(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    owner_user_id: Uuid,
    evidence_id: Uuid,
) -> Result<Option<LoadedEvidence>, sqlx::Error> {
    set_rls_context(txn, tenant_id, owner_user_id).await?;
    let Some(row) = sqlx::query(
        "SELECT origin_class, origin_principal_id, data_class, visibility_class, \
                visibility_user_id, visibility_workspace_id, occurred_at, payload_sha256 \
         FROM private.evidence_objects \
         WHERE evidence_id = $1 AND tenant_id = $2 AND reasoning_domain_id = $3 \
           AND evidence_kind = 'EVENT'",
    )
    .bind(evidence_id)
    .bind(tenant_id)
    .bind(reasoning_domain_id)
    .fetch_optional(&mut **txn)
    .await?
    else {
        return Ok(None);
    };
    let origin_class_wire: String = row.try_get("origin_class")?;
    let Some(origin_class) = origin_class_from_db_str(&origin_class_wire) else {
        return Ok(None);
    };
    let visibility_user_id: Option<Uuid> = row.try_get("visibility_user_id")?;
    let rls_user_id = visibility_user_id.unwrap_or(owner_user_id);
    if rls_user_id != owner_user_id {
        set_rls_context(txn, tenant_id, rls_user_id).await?;
    }
    let Some(event) =
        sqlx::query("SELECT event_kind, payload FROM private.events WHERE event_id = $1")
            .bind(evidence_id)
            .fetch_optional(&mut **txn)
            .await?
    else {
        return Ok(None);
    };
    Ok(Some(LoadedEvidence {
        evidence_id,
        reasoning_domain_id,
        origin_class,
        origin_class_wire,
        origin_principal_id: row.try_get("origin_principal_id")?,
        data_class: row.try_get("data_class")?,
        visibility_class: row.try_get("visibility_class")?,
        visibility_user_id,
        visibility_workspace_id: row.try_get("visibility_workspace_id")?,
        occurred_at: row.try_get("occurred_at")?,
        payload_sha256: row.try_get("payload_sha256")?,
        event_kind: event.try_get("event_kind")?,
        payload: event.try_get("payload")?,
        rls_user_id,
    }))
}

/// §16.1.1 processing-run fingerprint columns, written at start (before the provider call).
#[derive(Debug, Clone)]
pub struct ProcessingRunStart<'a> {
    pub evidence_id: Uuid,
    pub processor_kind: &'a str,
    pub processor_version: &'a str,
    pub model_provider: &'a str,
    pub model_id: &'a str,
    pub model_revision: &'a str,
    pub prompt_version: &'a str,
    pub prompt_hash: &'a str,
    pub parser_version: &'a str,
    pub evidence_payload_sha256: Vec<Vec<u8>>,
    pub source_hash: &'a [u8],
    pub context_snapshot_seq: i64,
}

/// One new `private.processing_runs` row (`started_at` = now, `completed_at` NULL).
pub async fn start_processing_run(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    start: &ProcessingRunStart<'_>,
) -> Result<Uuid, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO private.processing_runs \
           (tenant_id, evidence_id, processor_kind, processor_version, model_provider, model_id, \
            model_revision, prompt_version, prompt_hash, parser_version, embedding_version, \
            card_builder_version, evidence_payload_sha256, source_hash, context_snapshot_seq) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, NULL, $11, $12, $13) \
         RETURNING processing_run_id",
    )
    .bind(tenant_id)
    .bind(start.evidence_id)
    .bind(start.processor_kind)
    .bind(start.processor_version)
    .bind(start.model_provider)
    .bind(start.model_id)
    .bind(start.model_revision)
    .bind(start.prompt_version)
    .bind(start.prompt_hash)
    .bind(start.parser_version)
    .bind(&start.evidence_payload_sha256)
    .bind(start.source_hash)
    .bind(start.context_snapshot_seq)
    .fetch_one(&mut **txn)
    .await
}

/// Completes a run: `output_digest` + `output_count` + `completed_at` together (0064's
/// `processing_runs_completed_has_output` CHECK). A failed attempt never calls this — its row
/// keeps `completed_at IS NULL` as the failure marker (ADR-0016 D4).
pub async fn finish_processing_run(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    processing_run_id: Uuid,
    output_digest: &[u8],
    output_count: i32,
    provider_request_id: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE private.processing_runs \
         SET output_digest = $2, output_count = $3, provider_request_id = $4, completed_at = now() \
         WHERE processing_run_id = $1 AND completed_at IS NULL",
    )
    .bind(processing_run_id)
    .bind(output_digest)
    .bind(output_count)
    .bind(provider_request_id)
    .execute(&mut **txn)
    .await?;
    Ok(())
}

/// One authorized memory to insert — `class` is the `AuthorizedAuthority` §10.1 returned, never
/// the raw candidate class.
#[derive(Debug, Clone)]
pub struct NewMemory<'a> {
    pub content: &'a Value,
    pub memory_type: MemoryType,
    pub class: AuthorityClass,
    pub confidence: f32,
}

/// `memory_records` + its PRIMARY `memory_evidence` link (ordinal 0) in the caller's
/// transaction (0004's deferred `memory_records_requires_evidence` trigger needs both by
/// COMMIT). Visibility is the Evidence's own three columns verbatim (ADR-0016 D4).
/// Grounding: the link is `IMMUTABLE` against the Evidence's `payload_sha256` — an EVENT
/// payload is content-addressed and never rewritten (§8.1), so the recorded version is that
/// digest's hex (§8.8: IMMUTABLE edges are excluded from the recheck derivation).
pub async fn insert_memory(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    evidence: &LoadedEvidence,
    memory: &NewMemory<'_>,
) -> Result<Uuid, sqlx::Error> {
    let memory_id: Uuid = sqlx::query_scalar(
        "INSERT INTO private.memory_records \
           (tenant_id, memory_type, content, visibility_class, visibility_user_id, \
            visibility_workspace_id, authority_class, confidence, status, asserted_at, occurred_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', now(), $9) \
         RETURNING memory_id",
    )
    .bind(tenant_id)
    .bind(memory_type_db_str(memory.memory_type))
    .bind(memory.content)
    .bind(&evidence.visibility_class)
    .bind(evidence.visibility_user_id)
    .bind(evidence.visibility_workspace_id)
    .bind(authority_class_db_str(memory.class))
    .bind(memory.confidence)
    .bind(evidence.occurred_at)
    .fetch_one(&mut **txn)
    .await?;
    sqlx::query(
        "INSERT INTO private.memory_evidence \
           (memory_id, evidence_id, role, ordinal, grounding_mode, recorded_version) \
         VALUES ($1, $2, 'PRIMARY', 0, 'IMMUTABLE', $3)",
    )
    .bind(memory_id)
    .bind(evidence.evidence_id)
    .bind(hex::encode(&evidence.payload_sha256))
    .execute(&mut **txn)
    .await?;
    Ok(memory_id)
}

/// Terminal outbox state for one claimed row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxTerminal {
    Done,
    Failed,
}

/// Flips the claimed row to its terminal state, fenced on the lease: `false` (0 rows) means the
/// lease was reclaimed by another worker — the caller must roll its transaction back.
pub async fn complete_outbox(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    outbox_id: Uuid,
    lease_owner: &str,
    terminal: OutboxTerminal,
) -> Result<bool, sqlx::Error> {
    let status = match terminal {
        OutboxTerminal::Done => "DONE",
        OutboxTerminal::Failed => "FAILED",
    };
    let result = sqlx::query(
        "UPDATE ops.outbox \
         SET status = $2, processed_at = now(), lease_owner = NULL, lease_expires_at = NULL \
         WHERE outbox_id = $1 AND status = 'PROCESSING' AND lease_owner = $3",
    )
    .bind(outbox_id)
    .bind(status)
    .bind(lease_owner)
    .execute(&mut **txn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Hands a claimed row back (PROCESSING → PENDING, lease cleared) in its own transaction, fenced
/// on the lease like [`complete_outbox`]: the retryable paths (route not yet bound / not
/// admitted, provider 429/5xx, disclosure ledger hiccup) must not spend the row — FAILED is
/// terminal for [`claim_pending_evidence`] and for the ticket (`projection_worker`
/// `distill_failed`), so it is reserved for input-bound rejections (ADR-0016 D4/D5).
/// `false` = the lease was already reclaimed; nothing to hand back.
// ponytail: unbounded retry — `ops.outbox` carries no attempts counter, so a poison row is
// re-claimed every pass (one DB round trip, no egress when admission fails). Add
// attempts + a DEAD terminal via a forward-fix migration when one is observed in production.
pub async fn release_outbox(
    pool: &PrivateWorkerDbPool,
    tenant_id: Uuid,
    outbox_id: Uuid,
    lease_owner: &str,
) -> Result<bool, sqlx::Error> {
    let mut txn = pool.pool().begin().await?;
    set_rls_context(&mut txn, tenant_id, Uuid::nil()).await?;
    let result = sqlx::query(
        "UPDATE ops.outbox \
         SET status = 'PENDING', lease_owner = NULL, lease_expires_at = NULL \
         WHERE outbox_id = $1 AND status = 'PROCESSING' AND lease_owner = $2",
    )
    .bind(outbox_id)
    .bind(lease_owner)
    .execute(&mut *txn)
    .await?;
    txn.commit().await?;
    Ok(result.rows_affected() == 1)
}

/// Settles a claimed row FAILED in its own transaction — only for input-bound rejections
/// (Evidence unavailable to this hop, parser fail-closed); see [`release_outbox`] for the rest.
pub async fn fail_outbox(
    pool: &PrivateWorkerDbPool,
    tenant_id: Uuid,
    outbox_id: Uuid,
    lease_owner: &str,
) -> Result<bool, sqlx::Error> {
    let mut txn = pool.pool().begin().await?;
    set_rls_context(&mut txn, tenant_id, Uuid::nil()).await?;
    let fenced = complete_outbox(&mut txn, outbox_id, lease_owner, OutboxTerminal::Failed).await?;
    txn.commit().await?;
    Ok(fenced)
}

/// §78.2: the read-side origin mapping is pinned against the same migration CHECK list
/// `remember::contract_tests` pins the write side against.
#[cfg(test)]
mod contract_tests {
    use super::*;

    const MIGRATION_SQL: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/0004_private_evidence_memory.sql"
    ));

    #[test]
    fn origin_class_from_db_str_round_trips_every_check_value() {
        let needle = "origin_class IN (";
        let start = MIGRATION_SQL.find(needle).expect("origin_class CHECK") + needle.len();
        let close = MIGRATION_SQL[start..].find(')').expect("CHECK close") + start;
        let db: Vec<&str> = MIGRATION_SQL[start..close]
            .split(',')
            .map(|s| s.trim().trim_matches('\''))
            .collect();
        assert_eq!(db.len(), 9);
        for value in db {
            assert!(
                origin_class_from_db_str(value).is_some(),
                "{value} must parse"
            );
        }
        assert!(origin_class_from_db_str("Bogus").is_none());
    }

    #[test]
    fn memory_type_db_str_matches_check_list() {
        let needle = "memory_type IN (";
        let start = MIGRATION_SQL.find(needle).expect("memory_type CHECK") + needle.len();
        let close = MIGRATION_SQL[start..].find(')').expect("CHECK close") + start;
        let db: Vec<String> = MIGRATION_SQL[start..close]
            .split(',')
            .map(|s| s.trim().trim_matches('\'').to_owned())
            .collect();
        for memory_type in [
            MemoryType::Fact,
            MemoryType::Preference,
            MemoryType::Decision,
            MemoryType::Rejection,
            MemoryType::State,
            MemoryType::Issue,
            MemoryType::Lesson,
            MemoryType::Constraint,
            MemoryType::Procedure,
            MemoryType::Outcome,
            MemoryType::Reference,
            MemoryType::Note,
        ] {
            assert!(db.contains(&memory_type_db_str(memory_type).to_owned()));
        }
    }
}
