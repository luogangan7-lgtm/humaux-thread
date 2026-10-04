//! `adapters::distill_repo` — SQL half of the Distill hop (ADR-0016), through [`PrivateWorkerDbPool`] only: take the
//!   one `EVIDENCE_ACCEPTED` outbox row of a claimed `DERIVED_DISTILL` job (ADR-0058), load that Evidence, record the
//!   §16.1.1 processing run, insert the authorized memories, settle the job and its outbox row together.
//! Depends-on: crates=[hex, humaux-application, humaux-domain, humaux-projection, serde_json, sqlx, time]; services=[PostgreSQL(any) r=[control.memberships, ops.jobs, private.events, private.evidence_affects, private.evidence_objects] w=[ops.outbox, private.distill_candidates, private.memory_evidence, private.memory_records, private.processing_runs] x=[control.current_reasoning_route_binding]]; env=[CARGO_MANIFEST_DIR]; modules=[adapters::confirm_token_repo, adapters::consolidate_repo, adapters::jobs, adapters::postgres, adapters::quota_repo, adapters::remember, adapters::request_guard_repo, adapters::retrieve, adapters::subject_repo, application::confirm, application::consolidate, domain::audit, domain::authority, domain::confirm, domain::error, domain::evidence, domain::identity, domain::ids, domain::memory, domain::subject, humaux-adapters, projection::stream]
//! Called-by: [adapters::context_repo, adapters::memory_governance_repo, gateway::mcp_application, gateway::memory, private-worker::distill, private-worker::main, tests]
//! Invariants: [memories, run completion, the job settle and the outbox flip commit in ONE transaction whose first
//!   statement is the job's generation-fenced finish (ADR-0058 D-E), so a superseded worker's inserts roll back; a job
//!   only ever takes the outbox row of its own Evidence; every read runs under role_private_worker grants + RLS with the acting
//!   user installed first; memory.confirm / memory.reject lock a candidate only inside the narrowed write scope
//!   (its visibility against the scope's one workspace and user, ADR-0054); metrics families emitted:
//!   private_distill_runs_total and private_distill_outputs_total, counted only after the distill write commits
//!   ([`commit_distill_write`], ADR-0061 card 34b)]
//! Spec: Baseline §15.5; §14; ADR-0016; §6.1.1; ADR-0054; ADR-0058; ADR-0060 D-K; §41.2; §42; ADR-0061 D-C
//!
//! §15.5: one Evidence → 0/1/N `private.memory_records`; §14: the `ops.outbox` row remember wrote
//! is the work item (PENDING → PROCESSING → DONE | FAILED, or back to PENDING when the attempt was
//! retryable). ADR-0058 D-C: the row is reached only through its `DERIVED_DISTILL` job's current
//! claim ([`take_outbox_row`]); nothing reclaims outbox rows by lease expiry. Idempotency
//! (ADR-0016 D5, ADR-0058 D-E): memory rows, the run's completion, the job settle and the outbox
//! flip commit in ONE transaction opened by the generation-fenced [`finish_job_in_txn`], so a late
//! worker whose claim was superseded rolls its inserts back rather than duplicating them.
//!
//! Every SELECT/INSERT here runs under `role_private_worker`'s grants and RLS: `private.events`
//! keeps the §6.1.1 visibility disjunction inline (no headless bypass), so [`load_evidence`]
//! installs the acting user — the Evidence's own `visibility_user_id` when USER_PRIVATE, else the
//! reasoning domain's owner (an ACTIVE member) — before reading the payload.

use std::time::Duration;

use humaux_application::consolidate::{ReasoningRouteBindingId, ReasoningRouteBindingVersion};
use humaux_domain::audit::{AuditEvent, AuditEventId, McpAuditAction};
use humaux_domain::authority::{AuthorityClass, CandidateRejection};
use humaux_domain::confirm::DestructiveOp;
use humaux_domain::error::{ConflictReason, ErrorCode};
use humaux_domain::evidence::{EvidenceOriginClass, payload_sha256};
use humaux_domain::identity::AuthorizationScope;
use humaux_domain::ids::{Scope, WorkspaceId};
use humaux_domain::memory::MemoryType;
use humaux_domain::subject::SubjectDeclaration;
use humaux_projection::stream::StreamKey;
use serde_json::Value;
use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use crate::postgres::{PrivateWorkerDbPool, RuntimeDbPool};
use crate::{
    Counter,
    confirm_token_repo::{self, ConfirmationClaim},
    quota_repo::{self, ReservationStatus, ReserveResult},
    remember::{self, RememberCommand},
    request_guard_repo::{self, AuditTenant},
    retrieve::{self, TokenClaims},
};

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
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await?;
    set_rls_context(&mut txn, tenant_id, Uuid::nil()).await?;
    Ok(txn)
}

/// Opens the write-leg transaction with the tenant + acting user installed
/// ([`finish_job_in_txn`] first, then [`insert_memory`], [`finish_processing_run`],
/// [`settle_outbox_in_txn`]).
pub async fn begin_write_context(
    pool: &PrivateWorkerDbPool,
    tenant_id: Uuid,
    user_id: Uuid,
) -> Result<DbTransaction<'_>, DbError> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
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
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
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

/// ADR-0058 D-C: what [`take_outbox_row`] found for a claimed job's one Evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakenOutbox {
    /// The row is this claim's to work (now `PROCESSING`, leased until the claim's `hard_deadline`).
    Taken(ClaimedEvidence),
    /// No open row: the Evidence was already settled (DONE/FAILED) — the job has nothing to do.
    AlreadySettled,
    /// The claim's generation was superseded (or the job settled): nothing was touched.
    LeaseLost,
}

/// ADR-0058 D-C: takes exactly the `EVIDENCE_ACCEPTED` row of the job's own Evidence, in a
/// transaction that first locks the job row under its generation fence — the only door to an
/// outbox row is the job's current claim, so no batch and no second process can take it mid-run
/// (C7). The outbox lease is the claim's `hard_deadline`: nothing reclaims outbox rows by lease
/// expiry any more, the job's generation does.
pub async fn take_outbox_row(
    pool: &PrivateWorkerDbPool,
    lease: &crate::jobs::DistillLease<'_>,
    evidence_id: Uuid,
    hold_until: OffsetDateTime,
) -> Result<TakenOutbox, sqlx::Error> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await?;
    set_rls_context(&mut txn, lease.tenant_id, Uuid::nil()).await?;
    let fenced = sqlx::query_scalar::<_, i32>(
        "SELECT 1 FROM ops.jobs \
         WHERE job_id = $1 AND tenant_id = $2 AND lease_owner = $3 AND claim_generation = $4 \
           AND status = 'PROCESSING' \
         FOR UPDATE",
    )
    .bind(lease.job_id)
    .bind(lease.tenant_id)
    .bind(lease.lease_owner)
    .bind(lease.claim_generation)
    .fetch_optional(&mut *txn)
    .await?;
    if fenced.is_none() {
        txn.rollback().await?;
        return Ok(TakenOutbox::LeaseLost);
    }
    // Served by outbox_tenant_evidence_idx (tenant_id, evidence_id).
    let row = sqlx::query(
        "UPDATE ops.outbox \
         SET status = 'PROCESSING', lease_owner = $4, lease_expires_at = $5 \
         WHERE tenant_id = $1 AND event_type = $2 AND evidence_id = $3 \
           AND status IN ('PENDING', 'PROCESSING') \
         RETURNING outbox_id, evidence_id, commit_seq, stream_seq",
    )
    .bind(lease.tenant_id)
    .bind(EVIDENCE_ACCEPTED)
    .bind(evidence_id)
    .bind(lease.lease_owner)
    .bind(hold_until)
    .fetch_optional(&mut *txn)
    .await?;
    txn.commit().await?;
    let Some(row) = row else {
        return Ok(TakenOutbox::AlreadySettled);
    };
    Ok(TakenOutbox::Taken(ClaimedEvidence {
        outbox_id: row.try_get("outbox_id")?,
        evidence_id: row.try_get("evidence_id")?,
        commit_seq: row.try_get("commit_seq")?,
        stream_seq: row.try_get("stream_seq")?,
    }))
}

/// ADR-0058 D-D: the reasoning domain the Evidence itself names (`None` = the Evidence is gone).
/// Compared with the job payload's domain BEFORE any binding of that domain is used, so no
/// Evidence is ever sent through another domain's route; `load_evidence` keeps its own domain
/// predicate as the second layer.
pub async fn evidence_domain(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    evidence_id: Uuid,
) -> Result<Option<Uuid>, sqlx::Error> {
    let domain: Option<Option<Uuid>> = sqlx::query_scalar(
        "SELECT reasoning_domain_id FROM private.evidence_objects \
         WHERE evidence_id = $1 AND tenant_id = $2",
    )
    .bind(evidence_id)
    .bind(tenant_id)
    .fetch_optional(&mut **txn)
    .await?;
    Ok(domain.flatten())
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

/// ADR-0058 D-P: whether `evidence_id` carries declared `private.evidence_affects` rows — the 0157
/// PRIMARY trigger copies those onto every memory born from it as EXPLICIT, so distill offers no
/// affect menu for it. Runs in the [`load_evidence`] transaction (its RLS context).
pub async fn evidence_has_declared_affects(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    evidence_id: Uuid,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM private.evidence_affects \
                        WHERE tenant_id = $1 AND evidence_id = $2)",
    )
    .bind(tenant_id)
    .bind(evidence_id)
    .fetch_one(&mut **txn)
    .await
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
    /// §11.2 / ADR-0060 D-K: the admitted Profile@version this run was shaped by (FK to
    /// `control.reasoning_profiles`), so history names the route that produced it.
    pub profile_id: Uuid,
    /// See [`Self::profile_id`].
    pub profile_version: i64,
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
            card_builder_version, evidence_payload_sha256, source_hash, context_snapshot_seq, \
            profile_id, profile_version) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, NULL, $11, $12, $13, $14, $15) \
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
    .bind(start.profile_id)
    .bind(start.profile_version)
    .fetch_one(&mut **txn)
    .await
}

/// Completes a run: `output_digest` + `output_count` + `completed_at` together (0064's
/// `processing_runs_completed_has_output` CHECK). A failed attempt never calls this — its row
/// keeps `completed_at IS NULL` as the failure marker (ADR-0016 D4). Returns the runs this call
/// finished (0 when the run was already complete), which the caller hands to
/// [`commit_distill_write`].
pub async fn finish_processing_run(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    processing_run_id: Uuid,
    output_digest: &[u8],
    output_count: i32,
    provider_request_id: Option<&str>,
) -> Result<u64, sqlx::Error> {
    let finished = sqlx::query(
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
    Ok(finished.rows_affected())
}

/// Commits a distill write transaction, then counts what it made durable: `finished_runs` runs
/// ([`finish_processing_run`]'s return) and `outputs` memory records ([`insert_memory`] calls).
/// ADR-0061 card 34b: counted only after the commit — a rolled-back write (lost generation or
/// outbox lease, a failed statement) counts nothing, and its retry counts once.
pub async fn commit_distill_write(
    txn: sqlx::Transaction<'_, sqlx::Postgres>,
    finished_runs: u64,
    outputs: u64,
) -> Result<(), sqlx::Error> {
    // dep: PostgreSQL(any) — commits the distill write transaction
    txn.commit().await?;
    count_committed_distill(finished_runs, outputs);
    Ok(())
}

// §41.2 row `private_distill_runs_total` / `private_distill_outputs_total` (§11 每次 run / 每条产出 · 各 1); consumers
// §39 stage liveness and the §42 no-output stage. Rendered by `humaux-private-worker` (ADR-0061 D-C).
static PRIVATE_DISTILL_RUNS_TOTAL: Counter = Counter::new();
static PRIVATE_DISTILL_OUTPUTS_TOTAL: Counter = Counter::new();

/// §41.2 `private_distill_runs_total` / `private_distill_outputs_total`: adds `runs` finished runs (a run counts
/// whatever its output count — the §42 no-output stage is about runs without outputs) and `outputs` memory records.
/// Public so the G80-6 witnesses can drive the emit without a database; the one production caller is
/// [`commit_distill_write`] (`cargo xtask metrics-registry` D5 asserts exactly one, ADR-0061 card 34b).
pub fn count_committed_distill(runs: u64, outputs: u64) {
    PRIVATE_DISTILL_RUNS_TOTAL.inc(runs);
    PRIVATE_DISTILL_OUTPUTS_TOTAL.inc(outputs);
}

/// This process's `private_distill_runs_total` (ADR-0061 D-C: the private worker renders it).
pub fn private_distill_runs_total() -> u64 {
    PRIVATE_DISTILL_RUNS_TOTAL.get()
}

/// This process's `private_distill_outputs_total` (ADR-0061 D-C: the private worker renders it).
pub fn private_distill_outputs_total() -> u64 {
    PRIVATE_DISTILL_OUTPUTS_TOTAL.get()
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
///
/// §6.1.3 / ADR-0028: this is the workspace's single `INSERT INTO private.memory_records`
/// (Distill hop, `memory.confirm`, `memory.correct` all route here), so the deterministic
/// subject resolve hook runs here once — after the PRIMARY link exists, because the hook reads
/// it — and no sibling writer can forget it. Explicit links (rules 1/2) are the caller's; the
/// hook adds rule 3 (Evidence declaration / correction predecessor) and byte-span mentions.
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
    crate::subject_repo::link_memory_in_txn(txn, tenant_id, memory_id, &[]).await?;
    Ok(memory_id)
}

/// How a settle leaves the job's one outbox row (ADR-0058 D-C/D-F).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxSettle {
    /// Memories (or "nothing memorable") committed: the ticket resolves.
    Done,
    /// Input-bound rejection or DEAD: the ticket ends as `distill_failed` (§15.7 gap).
    Failed,
    /// Handed back for a later claim of the same job (retry, not ready, parked).
    Pending,
}

/// Flips the job's taken row, fenced on `status = 'PROCESSING' AND lease_owner`: `false` means
/// the row is no longer this owner's — the caller rolls back. Runs AFTER
/// [`finish_job_in_txn`] in the same transaction, whose generation fence is the real one (two
/// generations may share one `lease_owner` string).
pub async fn settle_outbox_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    outbox_id: Uuid,
    lease_owner: &str,
    settle: OutboxSettle,
) -> Result<bool, sqlx::Error> {
    let (status, processed) = match settle {
        OutboxSettle::Done => ("DONE", true),
        OutboxSettle::Failed => ("FAILED", true),
        OutboxSettle::Pending => ("PENDING", false),
    };
    let result = sqlx::query(
        "UPDATE ops.outbox \
         SET status = $2, processed_at = CASE WHEN $4 THEN now() ELSE processed_at END, \
             lease_owner = NULL, lease_expires_at = NULL \
         WHERE outbox_id = $1 AND status = 'PROCESSING' AND lease_owner = $3",
    )
    .bind(outbox_id)
    .bind(status)
    .bind(lease_owner)
    .bind(processed)
    .execute(&mut **txn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// ADR-0058 T3 as the FIRST statement of a settle transaction (it locks and fences the job row
/// on its generation and frees the claim's slot). `false` = superseded: the caller rolls back
/// every business write of the transaction.
pub async fn finish_job_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    lease: &crate::jobs::DistillLease<'_>,
    finish: crate::jobs::DistillFinish,
    error_class: Option<&str>,
    backoff_seconds: f64,
    park_seconds: f64,
) -> Result<bool, sqlx::Error> {
    crate::jobs::finish_distill_in_txn(
        txn,
        lease,
        finish,
        error_class,
        backoff_seconds,
        park_seconds,
    )
    .await
    .map_err(|e| match e {
        crate::jobs::JobsError::Db(e) => e,
        other => sqlx::Error::Protocol(other.to_string()),
    })
}

// ============================================================================
// Distill candidate queue — private.distill_candidates (ADR-0026, Card 6).
//
// Producer (`insert_candidate`) runs on role_private_worker inside the Distill write leg.
// Gateway consumers (`confirm_candidate_atomically` / `reject_candidate_atomically` /
// `list_pending_candidates`) run on role_gateway. They live here (not memory_governance_repo)
// because the whole candidate lifecycle belongs to the Distill hop; they reuse remember's sole
// evidence issuer + the shared BMO/confirm-token/audit helpers, never a second token path.
// ============================================================================

/// Read-side inverse of [`authority_class_db_str`] (over all 7 variants; no parallel CHECK copy).
fn authority_class_from_db_str(wire: &str) -> Option<AuthorityClass> {
    [
        AuthorityClass::PublicKnowledge,
        AuthorityClass::PrivateKnowledge,
        AuthorityClass::UserPreference,
        AuthorityClass::ProjectDecision,
        AuthorityClass::UserCorrection,
        AuthorityClass::ProjectConstraint,
        AuthorityClass::ExplicitTaskContext,
    ]
    .into_iter()
    .find(|c| authority_class_db_str(*c) == wire)
}

/// Read-side inverse of [`memory_type_db_str`] (over all 12 variants).
fn memory_type_from_db_str(wire: &str) -> Option<MemoryType> {
    [
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
    ]
    .into_iter()
    .find(|t| memory_type_db_str(*t) == wire)
}

fn candidate_db_error(error: sqlx::Error) -> ErrorCode {
    match error {
        sqlx::Error::RowNotFound => ErrorCode::NotFound,
        sqlx::Error::Database(ref db) => match db.code().as_deref() {
            Some("42501") => ErrorCode::Forbidden,
            Some("23503") => ErrorCode::TenantBoundary,
            Some("23505" | "40001" | "40P01" | "55P03" | "23514") => ErrorCode::Conflict,
            Some("22023" | "22P02" | "22003") => ErrorCode::InvalidInput,
            _ => ErrorCode::Internal,
        },
        _ => ErrorCode::DependencyUnavailable,
    }
}

fn remember_err(error: remember::RememberError) -> ErrorCode {
    match error {
        remember::RememberError::Db(error) => candidate_db_error(error),
        remember::RememberError::ConsistencyTokenExpiryNotFuture
        | remember::RememberError::BatchExhausted => ErrorCode::Conflict,
        remember::RememberError::Subject(code) | remember::RememberError::Affect(code) => code,
        remember::RememberError::ReasoningDomainUnresolved
        | remember::RememberError::TokenKeysUnset => ErrorCode::DependencyUnavailable,
    }
}

/// Producer input for one PENDING candidate — the §10.1-rejected distill output plus the TTL
/// the deployment configured (§78.1). Visibility / reasoning domain / data_class / occurred_at
/// are copied from `evidence` (the confirm materialize needs them without re-reading Evidence).
pub struct NewCandidate<'a> {
    /// The parsed memory content (the `memory_content` JSON the hop would have written).
    pub body: &'a Value,
    /// sha256 over the canonical `body` bytes (the confirm lock; `payload_sha256(body)`).
    pub sha256: &'a [u8],
    pub rejection: CandidateRejection,
    /// The class distill requested and §10.1 rejected for the source origin.
    pub requested_class: AuthorityClass,
    pub memory_type: MemoryType,
    pub confidence: f32,
    /// HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS (§78.1, no literal): expires_at is computed as
    /// clock_timestamp() + this in the INSERT, so the deadline is the DB clock, not a client one.
    pub ttl_seconds: i64,
}

/// Inserts one PENDING candidate in the caller's Distill write-leg transaction (role_private_
/// worker). Same transaction as the admitted memories, so a fault that drops this INSERT is
/// observable as a missing candidate row after a rejected distill (Card 6 fault-injection gate).
pub async fn insert_candidate(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    evidence: &LoadedEvidence,
    candidate: &NewCandidate<'_>,
) -> Result<Uuid, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO private.distill_candidates \
           (tenant_id, source_evidence_id, candidate_body, candidate_sha256, rejection_reason, \
            requested_class, memory_type, confidence, data_class, visibility_class, \
            visibility_user_id, visibility_workspace_id, reasoning_domain_id, occurred_at, \
            state, expires_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'PENDING', \
                 clock_timestamp() + make_interval(secs => $15::double precision)) \
         RETURNING candidate_id",
    )
    .bind(tenant_id)
    .bind(evidence.evidence_id)
    .bind(candidate.body)
    .bind(candidate.sha256)
    .bind(candidate.rejection.as_db_str())
    .bind(authority_class_db_str(candidate.requested_class))
    .bind(memory_type_db_str(candidate.memory_type))
    .bind(candidate.confidence)
    .bind(&evidence.data_class)
    .bind(&evidence.visibility_class)
    .bind(evidence.visibility_user_id)
    .bind(evidence.visibility_workspace_id)
    .bind(evidence.reasoning_domain_id)
    .bind(evidence.occurred_at)
    .bind(candidate.ttl_seconds)
    .fetch_one(&mut **txn)
    .await
}

/// Trusted application inputs for `memory.confirm` (built by the gateway gate, never
/// deserialized from MCP).
pub struct ConfirmRequest {
    pub request_id: Uuid,
    pub request_fingerprint: String,
    pub reservation_ttl: Duration,
    pub candidate_id: Uuid,
    /// The exact body digest the user echoed (confirm binds by candidate_id + this).
    pub candidate_sha256: Vec<u8>,
    /// Stream family the UserConfirmed Evidence + its MEMORY_LIFECYCLE ticket are issued on.
    pub stream: StreamKey,
    pub claim: ConfirmationClaim,
    pub finished_audit: AuditEvent,
    pub consistency_token_ttl: Duration,
    /// §6.1.3 rules 1/2: explicit subjects the confirmed memory is about (resolved under the
    /// tenant's RLS before the token is consumed; unknown ⇒ `INVALID_INPUT`, nothing written).
    pub subjects: SubjectDeclaration,
}

#[derive(Debug, Clone)]
pub struct ConfirmDone {
    /// The new active UserConfirmed Memory.
    pub memory_id: Uuid,
    /// The new UserConfirmed Evidence.
    pub evidence_id: Uuid,
    pub candidate_id: Uuid,
    pub stream_seq: i64,
    pub commit_seq: i64,
    pub consistency_token: String,
    /// §6.1.3: every subject M is linked to after the hook ran (explicit + inherited from the
    /// candidate's source Evidence declaration).
    pub subject_ids: Vec<Uuid>,
}

/// Success-shaped outcome: an executed confirm, or a refused one carrying a §52.1 sub-reason.
pub enum ConfirmOutcome {
    Confirmed(ConfirmDone),
    Refused(ConflictReason),
}

/// Trusted application inputs for `memory.reject`.
pub struct RejectRequest {
    pub request_id: Uuid,
    pub request_fingerprint: String,
    pub reservation_ttl: Duration,
    pub candidate_id: Uuid,
    pub claim: ConfirmationClaim,
    pub finished_audit: AuditEvent,
}

pub enum RejectOutcome {
    Rejected(Uuid),
    Refused(ConflictReason),
}

/// One PENDING candidate as `memory.enumerate {candidates:true}` returns it.
#[derive(Debug, Clone)]
pub struct PendingCandidate {
    pub candidate_id: Uuid,
    pub candidate_sha256: Vec<u8>,
    pub candidate_body: Value,
    pub requested_class: String,
    pub memory_type: String,
    pub rejection_reason: String,
    pub confidence: f32,
    pub source_evidence_id: Uuid,
    pub created_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
}

/// The candidate columns the confirm materialize needs, read under RLS (tenant + the lock sha).
struct LockedCandidate {
    state: String,
    expired: bool,
    /// Provenance: the Evidence the candidate was distilled from (its subject declaration is
    /// carried onto E2 so the confirmed memory inherits it, §6.1.3 rule 3a).
    source_evidence_id: Uuid,
    body: Value,
    payload_sha256: Vec<u8>,
    requested_class: AuthorityClass,
    memory_type: MemoryType,
    confidence: f32,
    data_class: String,
    visibility_class: String,
    visibility_user_id: Option<Uuid>,
    visibility_workspace_id: Option<Uuid>,
    reasoning_domain_id: Uuid,
    occurred_at: Option<OffsetDateTime>,
}

#[allow(clippy::too_many_arguments)] // one confirmed-write's worth of trusted, gate-built inputs (mirrors validate_correct)
fn validate_candidate_write(
    auth: &AuthorizationScope,
    op: DestructiveOp,
    request_id: Uuid,
    fingerprint: &str,
    reservation_ttl: Duration,
    claim: &ConfirmationClaim,
    candidate_id: Uuid,
    finished_audit: &AuditEvent,
) -> Result<(), ErrorCode> {
    if auth.tenant_id().0.is_nil() || auth.principal().0.is_nil() || auth.user_id().is_none() {
        return Err(ErrorCode::Unauthorized);
    }
    if request_id.is_nil()
        || reservation_ttl.is_zero()
        || fingerprint.len() != 64
        || !fingerprint
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ErrorCode::InvalidInput);
    }
    // The token confirms THIS op on THIS candidate, no successor (the memory is minted here).
    if claim.op != op || claim.target_id != candidate_id || claim.successor_id.is_some() {
        return Err(ErrorCode::Conflict);
    }
    if finished_audit.tenant_id != auth.tenant_id()
        || finished_audit.actor_id != auth.principal().0.to_string()
        || finished_audit.request_id != request_id.to_string()
        || finished_audit.action != McpAuditAction::McpRequestFinished.as_str()
        || finished_audit.resource_id != op.operation_key()
        || finished_audit.result != "OK"
        || finished_audit
            .risk_tags
            .iter()
            .any(|tag| tag == humaux_domain::confirm::RISK_TAG_CONFIRMATION_MINTED)
    {
        return Err(ErrorCode::InvalidInput);
    }
    Ok(())
}

/// ADR-0054 D-B: a candidate is writable only inside the narrowed write scope — the same
/// visibility disjunction [`list_pending_candidates`] lists by (`$ws` = the scope's one
/// workspace, `$user` = its user). The table's RLS is tenant-only (0152: visibility is data), so
/// without this a member routed to W2 could reject / confirm a W1 WORKSPACE_SHARED (or another
/// user's USER_PRIVATE) candidate by id. Out of scope reads as absent (NOT_FOUND, no oracle).
fn candidate_in_scope(ws: &str, user: &str) -> String {
    format!(
        "(visibility_class = 'TENANT_SHARED' \
          OR (visibility_class = 'WORKSPACE_SHARED' AND visibility_workspace_id = {ws}) \
          OR (visibility_class = 'USER_PRIVATE' AND visibility_user_id = {user}))"
    )
}

/// Reads the candidate by (candidate_id, sha) under the caller's tenant RLS and write scope
/// ([`candidate_in_scope`]). `None` = no such row visible to this scope
/// (invisible/cross-tenant/other-workspace/sha-mismatch) → the caller returns NOT_FOUND. Uses
/// `FOR UPDATE` to serialize concurrent confirm/reject on the same row.
async fn lock_candidate(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    auth: &AuthorizationScope,
    workspace: WorkspaceId,
    candidate_id: Uuid,
    sha256: &[u8],
) -> Result<Option<LockedCandidate>, ErrorCode> {
    let sql = format!(
        "SELECT state, expires_at <= clock_timestamp() AS expired, source_evidence_id, \
                candidate_body, \
                candidate_sha256, requested_class, memory_type, confidence, data_class, \
                visibility_class, visibility_user_id, visibility_workspace_id, \
                reasoning_domain_id, occurred_at \
         FROM private.distill_candidates \
         WHERE tenant_id = $1 AND candidate_id = $2 AND candidate_sha256 = $3 AND {} \
         FOR UPDATE",
        candidate_in_scope("$4", "$5")
    );
    let Some(row) = sqlx::query(&sql)
        .bind(auth.tenant_id().0)
        .bind(candidate_id)
        .bind(sha256)
        .bind(workspace.0)
        .bind(auth.user_id().map_or_else(Uuid::nil, |u| u.0))
        .fetch_optional(&mut **txn)
        .await
        .map_err(candidate_db_error)?
    else {
        return Ok(None);
    };
    let requested_wire: String = row
        .try_get("requested_class")
        .map_err(|_| ErrorCode::Internal)?;
    let type_wire: String = row
        .try_get("memory_type")
        .map_err(|_| ErrorCode::Internal)?;
    Ok(Some(LockedCandidate {
        state: row.try_get("state").map_err(|_| ErrorCode::Internal)?,
        expired: row.try_get("expired").map_err(|_| ErrorCode::Internal)?,
        source_evidence_id: row
            .try_get("source_evidence_id")
            .map_err(|_| ErrorCode::Internal)?,
        body: row
            .try_get("candidate_body")
            .map_err(|_| ErrorCode::Internal)?,
        payload_sha256: row
            .try_get("candidate_sha256")
            .map_err(|_| ErrorCode::Internal)?,
        requested_class: authority_class_from_db_str(&requested_wire).ok_or(ErrorCode::Internal)?,
        memory_type: memory_type_from_db_str(&type_wire).ok_or(ErrorCode::Internal)?,
        confidence: row.try_get("confidence").map_err(|_| ErrorCode::Internal)?,
        data_class: row.try_get("data_class").map_err(|_| ErrorCode::Internal)?,
        visibility_class: row
            .try_get("visibility_class")
            .map_err(|_| ErrorCode::Internal)?,
        visibility_user_id: row
            .try_get("visibility_user_id")
            .map_err(|_| ErrorCode::Internal)?,
        visibility_workspace_id: row
            .try_get("visibility_workspace_id")
            .map_err(|_| ErrorCode::Internal)?,
        reasoning_domain_id: row
            .try_get("reasoning_domain_id")
            .map_err(|_| ErrorCode::Internal)?,
        occurred_at: row
            .try_get("occurred_at")
            .map_err(|_| ErrorCode::Internal)?,
    }))
}

/// ADR-0026 D-C, atomically. Order: lock candidate (tenant RLS) -> branch on state/expiry ->
/// consume token -> reserve BMO -> insert Evidence E2 (UserConfirmed) via remember's own issuers
/// -> issue a MEMORY_LIFECYCLE ticket bound to E2 (M is materialized here, no distill round trip)
/// -> materialize M via OriginBoundAuthorityPolicy (UserConfirmed basis) -> mark candidate
/// CONFIRMED naming M -> quota CONSUMED + audits -> COMMIT. A confirm is a CREATION, so no
/// lifecycle event is appended (the log has no CREATE op — ADR-0026). Idempotency: the confirm
/// token is single-use and the state guard is PENDING, so a replay is a Conflict/Refused, never
/// a second Evidence.
#[allow(clippy::too_many_lines)] // one confirmed creation transaction, read top to bottom like correct.
pub async fn confirm_candidate_atomically(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    request: ConfirmRequest,
) -> Result<ConfirmOutcome, ErrorCode> {
    validate_candidate_write(
        auth,
        DestructiveOp::MemoryConfirm,
        request.request_id,
        &request.request_fingerprint,
        request.reservation_ttl,
        &request.claim,
        request.candidate_id,
        &request.finished_audit,
    )?;
    if request.stream.tenant_id != auth.tenant_id() {
        return Err(ErrorCode::TenantBoundary);
    }
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(candidate_db_error)?;
    let workspace = confirm_token_repo::set_write_authorization_local(&mut txn, auth).await?;

    let Some(candidate) = lock_candidate(
        &mut txn,
        auth,
        workspace,
        request.candidate_id,
        &request.candidate_sha256,
    )
    .await?
    else {
        return Err(ErrorCode::NotFound);
    };
    // Refusals before token consume / BMO: an already-confirmed candidate is terminal (1101);
    // a rejected/expired one, or a PENDING one past its deadline, is 1102.
    if candidate.state == "CONFIRMED" {
        return Ok(ConfirmOutcome::Refused(
            ConflictReason::CANDIDATE_ALREADY_CONFIRMED,
        ));
    }
    if candidate.state != "PENDING" || candidate.expired {
        return Ok(ConfirmOutcome::Refused(ConflictReason::CANDIDATE_EXPIRED));
    }

    // §6.1.3 rules 1/2 under this tenant's RLS, before the token is consumed: an unknown (or
    // another tenant's) subject is INVALID_INPUT with nothing written and the token intact.
    let explicit_subjects = crate::subject_repo::resolve_declaration_in_txn(
        &mut txn,
        auth.tenant_id().0,
        &request.subjects,
    )
    .await?;

    // Token first (same as correct/restore): a replayed/expired/misbound token is Conflict.
    confirm_token_repo::consume_in_txn(&mut txn, auth, &request.claim).await?;

    let reservation = match quota_repo::reserve_bmo_in_txn(
        &mut txn,
        auth,
        request.request_id,
        DestructiveOp::MemoryConfirm.operation_key(),
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

    // E2: a new UserConfirmed Evidence carrying the reviewed body, visibility + reasoning-domain
    // + data_class copied from the candidate (its source Evidence's own values). Through
    // remember's sole evidence/event issuers (never a second hand-written INSERT). Provenance to
    // the source Evidence lives on the candidate row (source_evidence_id), which the CONFIRMED
    // mark below joins to M via confirmed_memory_id.
    let digest =
        payload_sha256(&serde_json::to_vec(&candidate.body).map_err(|_| ErrorCode::Internal)?);
    let now = OffsetDateTime::now_utc();
    let expires_at = now
        .checked_add(
            time::Duration::try_from(request.consistency_token_ttl)
                .map_err(|_| ErrorCode::InvalidInput)?,
        )
        .ok_or(ErrorCode::InvalidInput)?;
    let cmd = RememberCommand {
        tenant_id: auth.tenant_id().0,
        authorization_user_id: auth.user_id().map(|u| u.0),
        scope_kind: request.stream.scope_kind.clone(),
        scope_id: request.stream.scope_id,
        domain: request.stream.domain.clone(),
        projection_kind: request.stream.projection_kind.clone(),
        projection_version: request.stream.projection_version.clone(),
        consistency_token_expires_at: expires_at,
        batch_id: None,
        payload_sha256: digest,
        data_class: candidate.data_class.clone(),
        origin_class: EvidenceOriginClass::UserConfirmed,
        origin_principal_id: Some(auth.principal().0),
        origin_connector_id: None,
        visibility_class: candidate.visibility_class.clone(),
        visibility_user_id: candidate.visibility_user_id,
        visibility_workspace_id: candidate.visibility_workspace_id,
        reasoning_domain_id: candidate.reasoning_domain_id,
        occurred_at: candidate.occurred_at,
        // A user confirming a distilled candidate is a manual user note (event subtype); the
        // trust axis is origin_class = UserConfirmed above.
        event_kind: "MANUAL_NOTE".to_owned(),
        event_payload: candidate.body.clone(),
        subjects: humaux_domain::subject::SubjectDeclaration::default(),
        affects: Vec::new(),
        mood_half_life: None,
    };
    let evidence_id = remember::create_evidence_object(&mut txn, &cmd)
        .await
        .map_err(remember_err)?;
    remember::insert_event_subtype(&mut txn, evidence_id, &cmd)
        .await
        .map_err(remember_err)?;
    // §6.1.3 rule 3a: E2's provenance is the candidate's source Evidence, so its subject
    // declaration rides onto E2 and the ordinary hook in insert_memory links M from it.
    crate::subject_repo::inherit_evidence_declarations_in_txn(
        &mut txn,
        auth.tenant_id().0,
        candidate.source_evidence_id,
        evidence_id,
    )
    .await
    .map_err(candidate_db_error)?;

    // One MEMORY_LIFECYCLE ticket bound to E2 (NOT EVIDENCE_ACCEPTED — M is materialized below;
    // projection resolves it via memory_evidence(E2)).
    let commit_seq = remember::next_commit_seq(&mut txn)
        .await
        .map_err(remember_err)?;
    let stream_seq = remember::issue_stream_log_row(&mut txn, &request.stream, commit_seq)
        .await
        .map_err(remember_err)?;
    remember::insert_outbox(
        &mut txn,
        auth.tenant_id().0,
        commit_seq,
        stream_seq,
        remember::MEMORY_LIFECYCLE,
        evidence_id,
    )
    .await
    .map_err(remember_err)?;

    // M: materialize via OriginBoundAuthorityPolicy for the candidate's requested class under a
    // UserConfirmed basis (not inherited, not the source origin's lower ceiling).
    let scope = Scope {
        tenant_id: auth.tenant_id(),
        user_id: auth.user_id(),
        workspace_id: (request.stream.scope_kind == "workspace")
            .then_some(WorkspaceId(request.stream.scope_id)),
        repository_id: None,
        task_id: None,
        run_id: None,
        agent_id: None,
    };
    // A candidate whose stored requested_class exceeds even the UserConfirmed §10.1 ceiling is a
    // determinate policy refusal, not a server fault (OriginBoundAuthorityPolicy rejects an
    // over-ceiling request outright, never clamps): surface CONFLICT — same shape as the token
    // conflicts above — so the caller can memory.reject it, never an opaque 500. The `?` drops
    // txn, rolling back the evidence/stream rows written just above; the candidate stays PENDING.
    let class = humaux_application::confirm::authorize_confirmed(
        candidate.requested_class,
        candidate.memory_type,
        &scope,
    )
    .map_err(|_| ErrorCode::Conflict)?;
    let loaded = LoadedEvidence {
        evidence_id,
        reasoning_domain_id: candidate.reasoning_domain_id,
        origin_class: EvidenceOriginClass::UserConfirmed,
        origin_class_wire: "UserConfirmed".to_owned(),
        origin_principal_id: Some(auth.principal().0),
        data_class: candidate.data_class.clone(),
        visibility_class: candidate.visibility_class.clone(),
        visibility_user_id: candidate.visibility_user_id,
        visibility_workspace_id: candidate.visibility_workspace_id,
        occurred_at: candidate.occurred_at,
        payload_sha256: candidate.payload_sha256.clone(),
        event_kind: "MANUAL_NOTE".to_owned(),
        payload: candidate.body.clone(),
        rls_user_id: auth.user_id().map_or_else(Uuid::nil, |u| u.0),
    };
    let memory_id = insert_memory(
        &mut txn,
        auth.tenant_id().0,
        &loaded,
        &NewMemory {
            content: &candidate.body,
            memory_type: candidate.memory_type,
            class,
            confidence: candidate.confidence,
        },
    )
    .await
    .map_err(candidate_db_error)?;
    // §6.1.3 rules 1/2: the caller's explicit subjects, through the same hook (idempotent on
    // top of what insert_memory already inherited).
    if !explicit_subjects.is_empty() {
        crate::subject_repo::link_memory_in_txn(
            &mut txn,
            auth.tenant_id().0,
            memory_id,
            &explicit_subjects,
        )
        .await
        .map_err(candidate_db_error)?;
    }
    let subject_ids =
        crate::subject_repo::memory_subject_ids_in_txn(&mut txn, auth.tenant_id().0, memory_id)
            .await
            .map_err(candidate_db_error)?;

    // Mark the candidate CONFIRMED naming M. Guarded WHERE state='PENDING' under the row lock:
    // 0 rows means a concurrent confirm/reject/expire won — roll E2/M back as a Conflict.
    let updated: Option<Uuid> = sqlx::query_scalar(
        "UPDATE private.distill_candidates \
            SET state = 'CONFIRMED', confirmed_memory_id = $3 \
          WHERE tenant_id = $2 AND candidate_id = $1 AND state = 'PENDING' \
          RETURNING candidate_id",
    )
    .bind(request.candidate_id)
    .bind(auth.tenant_id().0)
    .bind(memory_id)
    .fetch_optional(&mut *txn)
    .await
    .map_err(candidate_db_error)?;
    updated.ok_or(ErrorCode::Conflict)?;

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
    let finalized_at: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut *txn)
        .await
        .map_err(candidate_db_error)?;
    if finalized_at >= reservation.expires_at() {
        return Err(ErrorCode::Conflict);
    }
    let consistency_token = build_candidate_consistency_token(
        &request.stream,
        stream_seq,
        commit_seq,
        request.consistency_token_ttl,
    )?;
    txn.commit()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(ConfirmOutcome::Confirmed(ConfirmDone {
        memory_id,
        evidence_id,
        candidate_id: request.candidate_id,
        stream_seq,
        commit_seq,
        consistency_token,
        subject_ids,
    }))
}

/// §15.5 consistency_token from the workspace stream key + the new lifecycle stream seq (the
/// same issuer remember/correct use — `retrieve::issue_consistency_token`, never a fork).
fn build_candidate_consistency_token(
    stream: &StreamKey,
    stream_seq: i64,
    commit_seq: i64,
    ttl: Duration,
) -> Result<String, ErrorCode> {
    let issued_at = OffsetDateTime::now_utc();
    let ttl = time::Duration::try_from(ttl).map_err(|_| ErrorCode::InvalidInput)?;
    let expires_at = issued_at.checked_add(ttl).ok_or(ErrorCode::InvalidInput)?;
    retrieve::issue_consistency_token(&TokenClaims {
        tenant_id: stream.tenant_id.0,
        workspace_id: (stream.scope_kind == "workspace").then_some(stream.scope_id),
        scope_kind: stream.scope_kind.clone(),
        scope_id: stream.scope_id,
        domain: stream.domain.clone(),
        projection_kind: stream.projection_kind.clone(),
        projection_version: stream.projection_version.clone(),
        stream_seq,
        commit_seq,
        issued_at,
        expires_at,
    })
    // ADR-0059 D-G: an uninstalled key set is a missing dependency, never an unsigned token.
    .map_err(|_| ErrorCode::DependencyUnavailable)
}

/// ADR-0026 D-D `memory.reject`, atomically: consume token -> reserve BMO -> flip PENDING to
/// REJECTED (state only) -> quota CONSUMED + audits -> COMMIT. No Evidence/Memory. A candidate
/// that is CONFIRMED refuses with 1101; anything else not PENDING (or expired) with 1102.
pub async fn reject_candidate_atomically(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    request: RejectRequest,
) -> Result<RejectOutcome, ErrorCode> {
    validate_candidate_write(
        auth,
        DestructiveOp::MemoryReject,
        request.request_id,
        &request.request_fingerprint,
        request.reservation_ttl,
        &request.claim,
        request.candidate_id,
        &request.finished_audit,
    )?;
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(candidate_db_error)?;
    let workspace = confirm_token_repo::set_write_authorization_local(&mut txn, auth).await?;

    // Read state (tenant RLS + the write scope, ADR-0054) to distinguish the refusal reason
    // before any write; a candidate outside the scope is NOT_FOUND.
    let sql = format!(
        "SELECT state FROM private.distill_candidates \
         WHERE tenant_id = $1 AND candidate_id = $2 AND {} FOR UPDATE",
        candidate_in_scope("$3", "$4")
    );
    let Some(row) = sqlx::query(&sql)
        .bind(auth.tenant_id().0)
        .bind(request.candidate_id)
        .bind(workspace.0)
        .bind(auth.user_id().map_or_else(Uuid::nil, |u| u.0))
        .fetch_optional(&mut *txn)
        .await
        .map_err(candidate_db_error)?
    else {
        return Err(ErrorCode::NotFound);
    };
    let state: String = row.try_get("state").map_err(|_| ErrorCode::Internal)?;
    if state == "CONFIRMED" {
        return Ok(RejectOutcome::Refused(
            ConflictReason::CANDIDATE_ALREADY_CONFIRMED,
        ));
    }
    if state != "PENDING" {
        return Ok(RejectOutcome::Refused(ConflictReason::CANDIDATE_EXPIRED));
    }

    confirm_token_repo::consume_in_txn(&mut txn, auth, &request.claim).await?;
    let reservation = match quota_repo::reserve_bmo_in_txn(
        &mut txn,
        auth,
        request.request_id,
        DestructiveOp::MemoryReject.operation_key(),
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
    let updated: Option<Uuid> = sqlx::query_scalar(
        "UPDATE private.distill_candidates SET state = 'REJECTED' \
          WHERE tenant_id = $2 AND candidate_id = $1 AND state = 'PENDING' \
          RETURNING candidate_id",
    )
    .bind(request.candidate_id)
    .bind(auth.tenant_id().0)
    .fetch_optional(&mut *txn)
    .await
    .map_err(candidate_db_error)?;
    updated.ok_or(ErrorCode::Conflict)?;
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
    txn.commit()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(RejectOutcome::Rejected(request.candidate_id))
}

/// D-E: PENDING candidates visible to the caller (tenant RLS + the source Evidence's visibility
/// mirrored on the row), newest first. Read-only, `context:read`.
pub async fn list_pending_candidates(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    workspace: WorkspaceId,
    limit: i64,
) -> Result<Vec<PendingCandidate>, ErrorCode> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(candidate_db_error)?;
    confirm_token_repo::set_authorization_local(&mut txn, auth).await?;
    let user_id = auth.user_id().map_or_else(Uuid::nil, |u| u.0);
    let sql = format!(
        "SELECT candidate_id, candidate_sha256, candidate_body, requested_class, memory_type, \
                rejection_reason, confidence, source_evidence_id, created_at, expires_at \
         FROM private.distill_candidates \
         WHERE tenant_id = $1 AND state = 'PENDING' AND expires_at > clock_timestamp() \
           AND {} \
         ORDER BY created_at DESC LIMIT $4",
        candidate_in_scope("$2", "$3")
    );
    let rows = sqlx::query(&sql)
        .bind(auth.tenant_id().0)
        .bind(workspace.0)
        .bind(user_id)
        .bind(limit)
        .fetch_all(&mut *txn)
        .await
        .map_err(candidate_db_error)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(PendingCandidate {
            candidate_id: row
                .try_get("candidate_id")
                .map_err(|_| ErrorCode::Internal)?,
            candidate_sha256: row
                .try_get("candidate_sha256")
                .map_err(|_| ErrorCode::Internal)?,
            candidate_body: row
                .try_get("candidate_body")
                .map_err(|_| ErrorCode::Internal)?,
            requested_class: row
                .try_get("requested_class")
                .map_err(|_| ErrorCode::Internal)?,
            memory_type: row
                .try_get("memory_type")
                .map_err(|_| ErrorCode::Internal)?,
            rejection_reason: row
                .try_get("rejection_reason")
                .map_err(|_| ErrorCode::Internal)?,
            confidence: row.try_get("confidence").map_err(|_| ErrorCode::Internal)?,
            source_evidence_id: row
                .try_get("source_evidence_id")
                .map_err(|_| ErrorCode::Internal)?,
            created_at: row.try_get("created_at").map_err(|_| ErrorCode::Internal)?,
            expires_at: row.try_get("expires_at").map_err(|_| ErrorCode::Internal)?,
        });
    }
    txn.commit().await.map_err(candidate_db_error)?;
    Ok(out)
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
