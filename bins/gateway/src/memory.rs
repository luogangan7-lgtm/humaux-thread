//! Authenticated Memory reads through the existing final-body and Envelope boundaries.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use humaux_adapters::{
    affect_repo::{self, AffectInput, AffectRow, AnnotateDone},
    context_repo::{
        self, BindingWriteOutcome, BindingWriteRequest, EnumerationCensus, MaterializedMemory,
        MemoryEnumerationParams, materialize_memory_enumeration, materialize_memory_get,
    },
    distill_repo::{
        self, ConfirmOutcome, ConfirmRequest, PendingCandidate, RejectOutcome, RejectRequest,
    },
    memory_governance_repo::{
        self, ArchiveRequest, ArchiveResult, CorrectDone, CorrectRequest, RestoreRequest,
        RestoreResult, SupersedeOutcome, SupersedeRequest,
    },
    postgres::RuntimeDbPool,
    read_materialize::MaterializedItem,
    subject_repo,
};
use humaux_domain::{
    affect::{BasisPoints, MoodHalfLife},
    authority::MemoryId,
    confirm::DestructiveOp,
    error::ErrorCode,
    identity::AuthorizationScope,
    ids::{Scope, WorkspaceId},
    subject::{SubjectDeclaration, SubjectId, SubjectKey, SubjectKind, SubjectRole},
};
use humaux_projection::{serving::StreamFamily, stream::StreamKey};
use humaux_retrieval::{
    completeness::{CensusResult, FreshnessClass},
    envelope::{
        CompletenessBlock, CompletenessInputs, CountScope, Envelope, EvidenceBlock, FreshnessBlock,
        KnowledgeBlock, LaneStatus, MandatoryReport, PendingEnvelope, PinnedReport, PipelineBlock,
        build_projection_block, envelope_outcome_block,
    },
    request::{RetrievalIntent, RetrievalRequest, build_request},
};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    context::{ContextBootstrap, ContextItem, provenance},
    guard::ConfirmedWrite,
};

/// Pagination metadata deliberately omits snapshot totals and skipped/lost-access counts.
#[derive(Serialize)]
pub(crate) struct Pagination {
    snapshot_id: Uuid,
    next_cursor: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct EnumerationResult {
    content: Envelope<MemoryItem>,
    pagination: Pagination,
}

/// One Memory body as `memory.get` / `memory.enumerate` return it: the shared Envelope item
/// (`ContextItem`) plus its §6.1.3 subject links (ADR-0028 D-D). Always present on these two
/// reads — an empty list is a fact ("linked to nothing"), unlike `context.assemble`, which does
/// not read the axis and omits the field.
#[derive(Serialize)]
pub(crate) struct MemoryItem {
    #[serde(flatten)]
    item: ContextItem,
    subjects: Vec<Uuid>,
    /// §8.5.1 (ADR-0030 D-D): the memory's affect annotations, raw row + read-time
    /// `effective_intensity` (never persisted). Empty = un-annotated.
    affects: Vec<AffectItem>,
}

/// One affect annotation as `memory.get` / `memory.enumerate` render it (ADR-0030 D-D).
#[derive(Serialize)]
pub(crate) struct AffectItem {
    affect_id: Uuid,
    kind: &'static str,
    label: Option<&'static str>,
    valence: Option<i16>,
    arousal: Option<i16>,
    dominance: Option<i16>,
    intensity: i16,
    /// `effective_intensity(now)`: equals `intensity` for an EMOTION; decayed for a MOOD.
    effective_intensity: i16,
    confidence: i16,
    evidence_id: Uuid,
    target_subject_id: Option<Uuid>,
    target_scope: Option<AffectScopeItem>,
    observed_at: String,
    half_life_seconds: Option<u64>,
}

#[derive(Serialize)]
pub(crate) struct AffectScopeItem {
    kind: &'static str,
    id: Uuid,
}

fn affect_item(row: AffectRow, effective: BasisPoints) -> Result<AffectItem, ErrorCode> {
    let a = row.annotation;
    Ok(AffectItem {
        affect_id: row.affect_id,
        kind: a.kind.as_str(),
        label: a.label.map(|l| l.as_str()),
        valence: a.valence.map(BasisPoints::get),
        arousal: a.arousal.map(BasisPoints::get),
        dominance: a.dominance.map(BasisPoints::get),
        intensity: a.intensity.get(),
        effective_intensity: effective.get(),
        confidence: a.confidence.get(),
        evidence_id: row.evidence_id,
        target_subject_id: a.target_subject.map(|s| s.0),
        target_scope: a.target_scope.map(|s| AffectScopeItem {
            kind: s.kind.as_str(),
            id: s.id,
        }),
        observed_at: row
            .observed_at
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|_| ErrorCode::Internal)?,
        half_life_seconds: row.half_life.map(|h| h.duration().as_secs()),
    })
}

/// The affects of every memory in `materialized`, `memory_id → items`, read under the caller's
/// RLS in one round trip with `effective_intensity(now)` derived per row (ADR-0030 D-B).
async fn affects_by_memory(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    materialized: &MaterializedMemory,
) -> Result<BTreeMap<Uuid, Vec<AffectItem>>, ErrorCode> {
    let memory_ids: Vec<Uuid> = materialized
        .bodies
        .items
        .iter()
        .filter_map(|item| match item {
            MaterializedItem::Memory { memory_id, .. } => Some(*memory_id),
            _ => None,
        })
        .collect();
    let rows = affect_repo::affects_for_memories(pool, authorization, &memory_ids).await?;
    let observed = affect_repo::observed(&rows, time::OffsetDateTime::now_utc());
    let mut out: BTreeMap<Uuid, Vec<AffectItem>> = BTreeMap::new();
    for (row, observed) in rows.into_iter().zip(observed) {
        out.entry(row.memory_id)
            .or_default()
            .push(affect_item(row, observed.effective_intensity)?);
    }
    Ok(out)
}

pub(crate) async fn get<T>(
    pool: Arc<RuntimeDbPool>,
    authorization: AuthorizationScope,
    requested_workspace: Option<WorkspaceId>,
    bootstrap: ContextBootstrap,
    memory_id: MemoryId,
    accept: impl FnOnce(Envelope<MemoryItem>) -> Result<T, ErrorCode>,
) -> Result<PendingEnvelope<T>, ErrorCode> {
    let (authorization, scope, family, stream, serving_version) =
        read_scope(&pool, authorization, requested_workspace, &bootstrap).await?;
    let request = build_request(
        RetrievalIntent::trusted_memory_get(memory_id),
        &bootstrap.profile,
    )
    .map_err(|_| ErrorCode::Internal)?;
    let materialized =
        materialize_memory_get(&pool, &authorization, &scope, &family, &stream, memory_id).await?;
    let affects = affects_by_memory(&pool, &authorization, &materialized).await?;
    let archived = materialized.archived;
    let visible = bootstrap
        .visible_index_count(
            &pool,
            &authorization,
            &stream,
            Some(serving_version.as_str()),
            &materialized.ledger,
        )
        .await;
    accept_memory_envelope(
        materialized,
        affects,
        &request,
        &bootstrap.binary_build,
        "direct_get",
        false,
        archived,
        visible,
        // DirectGet has no enumerable universe (§23.3④) — see `accept_memory_envelope`.
        None,
        accept,
    )
}

/// One PENDING candidate as `memory.enumerate {candidates:true}` serializes it (ADR-0026 D-E).
#[derive(Serialize)]
pub(crate) struct CandidateItem {
    candidate_id: Uuid,
    candidate_sha256: String,
    candidate_body: serde_json::Value,
    requested_class: String,
    memory_type: String,
    rejection_reason: String,
    confidence: f32,
    source_evidence_id: Uuid,
    created_at: String,
    expires_at: String,
}

#[derive(Serialize)]
pub(crate) struct CandidatesResult {
    candidates: Vec<CandidateItem>,
    snapshot_id: Uuid,
}

/// §36/ADR-0026 D-E `memory.enumerate {candidates:true}`: the tenant's PENDING distill
/// candidates visible to the caller. Read-only; same workspace rule as `memory.get`.
pub(crate) async fn list_candidates(
    pool: Arc<RuntimeDbPool>,
    authorization: AuthorizationScope,
    requested_workspace: Option<WorkspaceId>,
    bootstrap: ContextBootstrap,
    limit: i64,
) -> Result<CandidatesResult, ErrorCode> {
    let (authorization, _scope, _family, _stream, _serving) =
        read_scope(&pool, authorization, requested_workspace, &bootstrap).await?;
    let workspace = requested_workspace.ok_or(ErrorCode::DependencyUnavailable)?;
    let rows =
        distill_repo::list_pending_candidates(&pool, &authorization, workspace, limit).await?;
    let candidates = rows
        .into_iter()
        .map(candidate_item)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CandidatesResult {
        candidates,
        snapshot_id: Uuid::now_v7(),
    })
}

fn candidate_item(row: PendingCandidate) -> Result<CandidateItem, ErrorCode> {
    Ok(CandidateItem {
        candidate_id: row.candidate_id,
        candidate_sha256: hex::encode(&row.candidate_sha256),
        candidate_body: row.candidate_body,
        requested_class: row.requested_class,
        memory_type: row.memory_type,
        rejection_reason: row.rejection_reason,
        confidence: row.confidence,
        source_evidence_id: row.source_evidence_id,
        created_at: row
            .created_at
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|_| ErrorCode::Internal)?,
        expires_at: row
            .expires_at
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|_| ErrorCode::Internal)?,
    })
}

pub(crate) async fn enumerate<T>(
    pool: Arc<RuntimeDbPool>,
    authorization: AuthorizationScope,
    requested_workspace: Option<WorkspaceId>,
    bootstrap: ContextBootstrap,
    params: MemoryEnumerationParams<'_>,
    accept: impl FnOnce(EnumerationResult) -> Result<T, ErrorCode>,
) -> Result<PendingEnvelope<T>, ErrorCode> {
    let (authorization, scope, family, stream, serving_version) =
        read_scope(&pool, authorization, requested_workspace, &bootstrap).await?;
    let request = build_request(
        RetrievalIntent::trusted_memory_enumerate(),
        &bootstrap.profile,
    )
    .map_err(|_| ErrorCode::Internal)?;
    let page =
        materialize_memory_enumeration(&pool, &authorization, &scope, &family, &stream, params)
            .await?;
    let pagination = Pagination {
        snapshot_id: page.snapshot_id,
        next_cursor: page.next_cursor,
    };
    let affects = affects_by_memory(&pool, &authorization, &page.memory).await?;
    let visible = bootstrap
        .visible_index_count(
            &pool,
            &authorization,
            &stream,
            Some(serving_version.as_str()),
            &page.memory.ledger,
        )
        .await;
    let enumeration = page.census;
    accept_memory_envelope(
        page.memory,
        affects,
        &request,
        &bootstrap.binary_build,
        "enumerate",
        pagination.next_cursor.is_some(),
        false,
        visible,
        enumeration,
        |content| {
            accept(EnumerationResult {
                content,
                pagination,
            })
        },
    )
}

/// Bootstrap policy for this cursor protocol; callers cannot extend it in MCP arguments.
pub(crate) const ENUMERATION_TTL: Duration = Duration::from_secs(15 * 60);

/// §36 `memory.supersede`, second (confirmed) call. The bound workspace must be the
/// bootstrap stream's workspace (write routes stay bootstrap-bound until card 11; the read
/// routes derive their stream per request, ADR-0031) so the lifecycle ticket lands on the
/// stream whose ledger the reads consult.
pub(crate) async fn supersede(
    pool: Arc<RuntimeDbPool>,
    write: ConfirmedWrite,
    stream: StreamKey,
    target: MemoryId,
    successor: MemoryId,
    undo_window: Duration,
) -> Result<SupersedeOutcome, ErrorCode> {
    let workspace = write
        .request
        .workspace_id()
        .ok_or(ErrorCode::DependencyUnavailable)?;
    let authorization = write.request.authorization().narrow(workspace)?;
    if stream.tenant_id != authorization.tenant_id() || stream.scope_id != workspace.0 {
        return Err(ErrorCode::DependencyUnavailable);
    }
    memory_governance_repo::supersede_atomically(
        &pool,
        &authorization,
        SupersedeRequest {
            request_id: write.request.request_id(),
            request_fingerprint: write.request_fingerprint,
            reservation_ttl: write.reservation_ttl,
            target,
            successor,
            stream,
            claim: write.claim,
            finished_audit: write.finished_audit,
            undo_window,
        },
    )
    .await
}

/// §36 `memory.restore`, second (confirmed) call (ADR-0020). Same `read_scope` workspace rule
/// as `memory.supersede`: the lifecycle ticket lands on the bootstrap stream's workspace.
pub(crate) async fn restore(
    pool: Arc<RuntimeDbPool>,
    write: ConfirmedWrite,
    stream: StreamKey,
    target: MemoryId,
    consistency_token_ttl: Duration,
) -> Result<RestoreResult, ErrorCode> {
    let workspace = write
        .request
        .workspace_id()
        .ok_or(ErrorCode::DependencyUnavailable)?;
    let authorization = write.request.authorization().narrow(workspace)?;
    if stream.tenant_id != authorization.tenant_id() || stream.scope_id != workspace.0 {
        return Err(ErrorCode::DependencyUnavailable);
    }
    memory_governance_repo::restore_atomically(
        &pool,
        &authorization,
        RestoreRequest {
            request_id: write.request.request_id(),
            request_fingerprint: write.request_fingerprint,
            reservation_ttl: write.reservation_ttl,
            target,
            stream,
            claim: write.claim,
            finished_audit: write.finished_audit,
            consistency_token_ttl,
        },
    )
    .await
}

/// §Q4 `memory.correct`, second (confirmed) call (ADR-0025). One transaction inserts a new
/// DirectUserInput Evidence + a new Memory version and supersedes the original with reason
/// USER_CORRECTION. Same `read_scope` workspace rule as the other governance writes. The
/// corrected content arrives already hashed (`payload_sha256`) through the sole constructor.
///
/// §6.1.3 (ADR-0028): the new version inherits the original's subjects inside that transaction
/// (0154 trigger on `superseded_by`); an explicit `subjects` declaration is resolved BEFORE the
/// token is consumed (unknown ⇒ `INVALID_INPUT`, token untouched) and applied in the same
/// transaction. `CorrectDone::subject_ids` is the version's full subject id list.
///
/// §8.5.1 (ADR-0030 D-E): `affects` are the new version's annotations, resolved before the
/// consume and written inside `correct_atomically`'s transaction (`CorrectDone::affect_ids`) —
/// one ticket, never a second transaction.
#[allow(clippy::too_many_arguments)] // one confirmed-write's worth of trusted, gate-built inputs
pub(crate) async fn correct(
    pool: Arc<RuntimeDbPool>,
    write: ConfirmedWrite,
    stream: StreamKey,
    target: MemoryId,
    content: serde_json::Value,
    payload_sha256: humaux_domain::evidence::EvidencePayloadSha256,
    undo_window: Duration,
    consistency_token_ttl: Duration,
    subjects: SubjectDeclaration,
    affects: Vec<AffectInput>,
    mood_half_life: Option<MoodHalfLife>,
) -> Result<CorrectDone, ErrorCode> {
    let workspace = write
        .request
        .workspace_id()
        .ok_or(ErrorCode::DependencyUnavailable)?;
    let authorization = write.request.authorization().narrow(workspace)?;
    if stream.tenant_id != authorization.tenant_id() || stream.scope_id != workspace.0 {
        return Err(ErrorCode::DependencyUnavailable);
    }
    memory_governance_repo::correct_atomically(
        &pool,
        &authorization,
        CorrectRequest {
            request_id: write.request.request_id(),
            request_fingerprint: write.request_fingerprint,
            reservation_ttl: write.reservation_ttl,
            target,
            content,
            payload_sha256,
            stream,
            claim: write.claim,
            finished_audit: write.finished_audit,
            undo_window,
            consistency_token_ttl,
            subjects,
            affects,
            mood_half_life,
        },
    )
    .await
}

/// §36/§10.1 `memory.confirm`, second (confirmed) call (ADR-0026, Card 6). One transaction
/// promotes a `private.distill_candidates` row into a new UserConfirmed Evidence + a new Memory
/// version. Same `read_scope` workspace rule as the other governance writes: the lifecycle ticket
/// lands on the bootstrap stream's workspace. `candidate_sha256` binds the confirm to the exact
/// body the user reviewed.
pub(crate) async fn confirm(
    pool: Arc<RuntimeDbPool>,
    write: ConfirmedWrite,
    stream: StreamKey,
    candidate_id: Uuid,
    candidate_sha256: Vec<u8>,
    consistency_token_ttl: Duration,
    subjects: SubjectDeclaration,
) -> Result<ConfirmOutcome, ErrorCode> {
    let workspace = write
        .request
        .workspace_id()
        .ok_or(ErrorCode::DependencyUnavailable)?;
    let authorization = write.request.authorization().narrow(workspace)?;
    if stream.tenant_id != authorization.tenant_id() || stream.scope_id != workspace.0 {
        return Err(ErrorCode::DependencyUnavailable);
    }
    distill_repo::confirm_candidate_atomically(
        &pool,
        &authorization,
        ConfirmRequest {
            request_id: write.request.request_id(),
            request_fingerprint: write.request_fingerprint,
            reservation_ttl: write.reservation_ttl,
            candidate_id,
            candidate_sha256,
            stream,
            claim: write.claim,
            finished_audit: write.finished_audit,
            consistency_token_ttl,
            subjects,
        },
    )
    .await
}

/// One registered subject as `memory.enumerate {subjects:true}` serializes it (ADR-0028).
#[derive(Serialize)]
pub(crate) struct SubjectItem {
    subject_id: Uuid,
    kind: &'static str,
    display_name: String,
    keys: Vec<SubjectKeyItem>,
    roles: Vec<&'static str>,
}

#[derive(Serialize)]
pub(crate) struct SubjectKeyItem {
    kind: &'static str,
    value: String,
}

#[derive(Serialize)]
pub(crate) struct SubjectsResult {
    subjects: Vec<SubjectItem>,
    snapshot_id: Uuid,
}

impl From<subject_repo::SubjectListing> for SubjectItem {
    fn from(row: subject_repo::SubjectListing) -> Self {
        SubjectItem {
            subject_id: row.subject_id,
            kind: row.kind.as_str(),
            display_name: row.display_name,
            keys: row
                .keys
                .into_iter()
                .map(|(kind, value)| SubjectKeyItem {
                    kind: kind.as_str(),
                    value,
                })
                .collect(),
            roles: row.roles.into_iter().map(SubjectRole::as_str).collect(),
        }
    }
}

/// §6.1.3 / ADR-0028 `memory.enumerate {subjects:true}`: the caller's tenant's registered
/// subjects (live heads) with keys and roles, under RLS. Read-only; same workspace rule as
/// `memory.get` (`read_scope`). This is the gateway face of the registry's cross-tenant
/// invisibility.
pub(crate) async fn list_subjects(
    pool: Arc<RuntimeDbPool>,
    authorization: AuthorizationScope,
    requested_workspace: Option<WorkspaceId>,
    bootstrap: ContextBootstrap,
    limit: i64,
) -> Result<SubjectsResult, ErrorCode> {
    let (authorization, _scope, _family, _stream, _serving) =
        read_scope(&pool, authorization, requested_workspace, &bootstrap).await?;
    let rows = subject_repo::list_subjects(&pool, &authorization, limit).await?;
    Ok(SubjectsResult {
        subjects: rows.into_iter().map(SubjectItem::from).collect(),
        snapshot_id: Uuid::now_v7(),
    })
}

/// §6.1.3 / ADR-0028 D-F `memory.subject_register` (card 7 D-E1): registers one subject under
/// the authenticated tenant. Same workspace rule as `memory.supersede` (`write_scope`); the
/// tenant is the credential's, never an argument.
pub(crate) async fn register_subject(
    pool: Arc<RuntimeDbPool>,
    authorization: AuthorizationScope,
    requested_workspace: Option<WorkspaceId>,
    bootstrap: ContextBootstrap,
    kind: SubjectKind,
    display_name: String,
    roles: Vec<SubjectRole>,
) -> Result<SubjectItem, ErrorCode> {
    let authorization = write_scope(authorization, requested_workspace, &bootstrap)?;
    subject_repo::register_subject(&pool, &authorization, kind, &display_name, &roles)
        .await
        .map(SubjectItem::from)
}

/// §6.1.3 / ADR-0028 D-F `memory.subject_link_key`: attaches an exact external key to one of the
/// tenant's registered subjects (unknown / another tenant's subject ⇒ `INVALID_INPUT`, a key
/// already registered ⇒ `CONFLICT`).
pub(crate) async fn link_subject_key(
    pool: Arc<RuntimeDbPool>,
    authorization: AuthorizationScope,
    requested_workspace: Option<WorkspaceId>,
    bootstrap: ContextBootstrap,
    subject_id: SubjectId,
    key: SubjectKey,
) -> Result<SubjectItem, ErrorCode> {
    let authorization = write_scope(authorization, requested_workspace, &bootstrap)?;
    subject_repo::link_key(&pool, &authorization, subject_id, &key)
        .await
        .map(SubjectItem::from)
}

/// `memory.annotate_affect` result shape (ADR-0030 D-C).
#[derive(Serialize)]
pub(crate) struct AnnotateResult {
    memory_id: Uuid,
    pub(crate) affect_ids: Vec<Uuid>,
    stream_seq: i64,
    commit_seq: i64,
}

impl From<AnnotateDone> for AnnotateResult {
    fn from(done: AnnotateDone) -> Self {
        AnnotateResult {
            memory_id: done.memory_id,
            affect_ids: done.affect_ids,
            stream_seq: done.stream_seq,
            commit_seq: done.commit_seq,
        }
    }
}

/// §8.5.1 / ADR-0030 D-C `memory.annotate_affect`: appends immutable affect rows to the visible
/// active head `memory_id` (provenance = its PRIMARY Evidence) and issues the re-projection
/// ticket on the bootstrap stream. Same workspace rule as `memory.supersede` (`write_scope`);
/// the tenant is the credential's. Not confirm-gated: nothing is deleted, superseded or hidden.
/// Also the sole path `memory.correct {affects}` re-supplies the new version's affects through.
pub(crate) async fn annotate_affect(
    pool: Arc<RuntimeDbPool>,
    authorization: AuthorizationScope,
    requested_workspace: Option<WorkspaceId>,
    bootstrap: ContextBootstrap,
    memory_id: MemoryId,
    inputs: Vec<AffectInput>,
    mood_half_life: MoodHalfLife,
) -> Result<AnnotateResult, ErrorCode> {
    let authorization = write_scope(authorization, requested_workspace, &bootstrap)?;
    affect_repo::annotate(
        &pool,
        &authorization,
        &bootstrap.stream,
        memory_id,
        &inputs,
        mood_half_life,
    )
    .await
    .map(AnnotateResult::from)
}

/// §36 `memory.reject`, second (confirmed) call (ADR-0026, Card 6). Marks a pending candidate
/// REJECTED; writes no Evidence/Memory and issues no ticket. Same gate as confirm.
pub(crate) async fn reject(
    pool: Arc<RuntimeDbPool>,
    write: ConfirmedWrite,
    stream: StreamKey,
    candidate_id: Uuid,
) -> Result<RejectOutcome, ErrorCode> {
    let workspace = write
        .request
        .workspace_id()
        .ok_or(ErrorCode::DependencyUnavailable)?;
    let authorization = write.request.authorization().narrow(workspace)?;
    if stream.tenant_id != authorization.tenant_id() || stream.scope_id != workspace.0 {
        return Err(ErrorCode::DependencyUnavailable);
    }
    distill_repo::reject_candidate_atomically(
        &pool,
        &authorization,
        RejectRequest {
            request_id: write.request.request_id(),
            request_fingerprint: write.request_fingerprint,
            reservation_ttl: write.reservation_ttl,
            candidate_id,
            claim: write.claim,
            finished_audit: write.finished_audit,
        },
    )
    .await
}

/// §36 `memory.archive` / `memory.unarchive`, second (confirmed) call (ADR-0024). Same
/// `read_scope` workspace rule as the other governance writes: the lifecycle ticket lands on
/// the bootstrap stream's workspace. `op` is `MemoryArchive` or `MemoryUnarchive`.
pub(crate) async fn archive(
    pool: Arc<RuntimeDbPool>,
    write: ConfirmedWrite,
    stream: StreamKey,
    op: DestructiveOp,
    target: MemoryId,
) -> Result<ArchiveResult, ErrorCode> {
    let workspace = write
        .request
        .workspace_id()
        .ok_or(ErrorCode::DependencyUnavailable)?;
    let authorization = write.request.authorization().narrow(workspace)?;
    if stream.tenant_id != authorization.tenant_id() || stream.scope_id != workspace.0 {
        return Err(ErrorCode::DependencyUnavailable);
    }
    memory_governance_repo::archive_or_unarchive_atomically(
        &pool,
        &authorization,
        ArchiveRequest {
            request_id: write.request.request_id(),
            request_fingerprint: write.request_fingerprint,
            reservation_ttl: write.reservation_ttl,
            target,
            stream,
            claim: write.claim,
            finished_audit: write.finished_audit,
            op,
        },
    )
    .await
}

/// §36 `memory.pin` / `memory.unpin`, second (confirmed) call (ADR-0019). The PINNED row is
/// scoped to the credential's bound workspace, which must be the bootstrap stream's
/// workspace — the same rule `memory.supersede` applies — so `context.assemble` reads it back
/// through the same scope chain.
pub(crate) async fn write_binding(
    pool: Arc<RuntimeDbPool>,
    write: ConfirmedWrite,
    stream: StreamKey,
    op: DestructiveOp,
    memory: MemoryId,
    task: Option<humaux_domain::ids::TaskId>,
    replaces_binding_id: Option<uuid::Uuid>,
) -> Result<BindingWriteOutcome, ErrorCode> {
    let workspace = write
        .request
        .workspace_id()
        .ok_or(ErrorCode::DependencyUnavailable)?;
    let authorization = write.request.authorization().narrow(workspace)?;
    if stream.tenant_id != authorization.tenant_id() || stream.scope_id != workspace.0 {
        return Err(ErrorCode::DependencyUnavailable);
    }
    let request = BindingWriteRequest {
        request_id: write.request.request_id(),
        request_fingerprint: write.request_fingerprint,
        reservation_ttl: write.reservation_ttl,
        memory,
        workspace,
        task,
        replaces_binding_id,
        claim: write.claim,
        finished_audit: write.finished_audit,
    };
    match op {
        DestructiveOp::MemoryPin => {
            context_repo::pin_confirmed(&pool, &authorization, request).await
        }
        DestructiveOp::MemoryUnpin => {
            context_repo::unpin_confirmed(&pool, &authorization, request).await
        }
        // card 22b (ADR-0045): the MANDATORY/TASK pair. Same credential route, same confirm
        // gate; `context_repo` re-reads §10.1 standing in the write transaction.
        DestructiveOp::MemoryBind => {
            context_repo::bind_confirmed(&pool, &authorization, request).await
        }
        DestructiveOp::MemoryUnbind => {
            context_repo::unbind_confirmed(&pool, &authorization, request).await
        }
        DestructiveOp::MemorySupersede
        | DestructiveOp::MemoryRestore
        | DestructiveOp::MemoryArchive
        | DestructiveOp::MemoryUnarchive
        | DestructiveOp::MemoryCorrect
        | DestructiveOp::MemoryConfirm
        | DestructiveOp::MemoryReject => Err(ErrorCode::InvalidInput),
    }
}

/// The read-route scope rule (memory.get / memory.enumerate and its `candidates` / `subjects`
/// faces; context.assemble applies the same one inline): membership-narrow to the requested
/// workspace (`Forbidden` outside the credential's set — the guard's `credential.authorize`
/// already narrowed on the wire path, so this `narrow` is defense in depth for in-process
/// callers, not the load-bearing gate), then derive the stream per request from the
/// credential's tenant + that workspace and admit it only if that family has a `serving`
/// projection (`ContextBootstrap::provisioned_request_stream`, ADR-0031 D-A / §16.2). The
/// bootstrap stream's tenant/workspace are not compared against the request here — one
/// process serves every provisioned pair; an unprovisioned one is `DEPENDENCY_UNAVAILABLE`
/// from that serving read, and an invisible object stays `NOT_FOUND`.
async fn read_scope(
    pool: &RuntimeDbPool,
    authorization: AuthorizationScope,
    requested_workspace: Option<WorkspaceId>,
    bootstrap: &ContextBootstrap,
) -> Result<(AuthorizationScope, Scope, StreamFamily, StreamKey, String), ErrorCode> {
    let workspace = requested_workspace.ok_or(ErrorCode::DependencyUnavailable)?;
    let authorization = authorization.narrow(workspace)?;
    // The third element is §16.2's `serving = true` version — §23.1②'s visible count is scoped
    // to it, not to the ledger key's process-configured version.
    let (family, stream, serving_version) = bootstrap
        .provisioned_request_stream(pool, &authorization, workspace)
        .await?;
    let scope = Scope {
        tenant_id: authorization.tenant_id(),
        user_id: authorization.user_id(),
        workspace_id: Some(workspace),
        repository_id: None,
        task_id: None,
        run_id: None,
        agent_id: None,
    };
    Ok((authorization, scope, family, stream, serving_version))
}

/// The write-route scope rule for the non-confirm-gated writers that share the memory op
/// (`subject_register`, `subject_link_key`, `annotate_affect`): until card 11 lifts the write
/// route, a write may only land on the one bootstrap-configured stream, so the request's
/// (tenant, workspace) must still equal it — the constant comparison the read routes dropped.
fn write_scope(
    authorization: AuthorizationScope,
    requested_workspace: Option<WorkspaceId>,
    bootstrap: &ContextBootstrap,
) -> Result<AuthorizationScope, ErrorCode> {
    let workspace = requested_workspace.ok_or(ErrorCode::DependencyUnavailable)?;
    let authorization = authorization.narrow(workspace)?;
    if bootstrap.stream.tenant_id != authorization.tenant_id()
        || bootstrap.stream.scope_id != workspace.0
    {
        return Err(ErrorCode::DependencyUnavailable);
    }
    Ok(authorization)
}

#[allow(clippy::too_many_arguments)] // One envelope assembly over the read's fixed inputs + the affect axis.
fn accept_memory_envelope<T>(
    mut materialized: MaterializedMemory,
    mut affects: BTreeMap<Uuid, Vec<AffectItem>>,
    request: &RetrievalRequest,
    binary_build: &str,
    lane: &str,
    truncated: bool,
    archived: bool,
    visible: Option<u64>,
    // `Some` only for `memory.enumerate` (ADR-0041): the §22.1 census this page's manifest was
    // minted under plus the §23.3④ `stream_ledger` pipeline counts read in that same snapshot.
    enumeration: Option<EnumerationCensus>,
    accept: impl FnOnce(Envelope<MemoryItem>) -> Result<T, ErrorCode>,
) -> Result<PendingEnvelope<T>, ErrorCode> {
    let items = materialized
        .bodies
        .items
        .into_iter()
        .map(|item| match item {
            // Q3/ADR-0024 D-C: memory.get carries `archived` on its single item; enumerate
            // never returns an archived row so it passes `false`.
            MaterializedItem::Memory { memory_id, content } => Ok(MemoryItem {
                item: ContextItem {
                    memory_id,
                    content,
                    archived,
                },
                subjects: materialized.subjects.remove(&memory_id).unwrap_or_default(),
                affects: affects.remove(&memory_id).unwrap_or_default(),
            }),
            _ => Err(ErrorCode::Internal),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let returned = u32::try_from(items.len()).map_err(|_| ErrorCode::Internal)?;
    let projection = build_projection_block(&materialized.ledger, visible);
    let (census, pipeline) = match enumeration {
        // §22.1/§23.3④ (ADR-0041, card 19): `memory.enumerate` counted its own authorized
        // universe and read the stream ledger's evidence/knowledge counts in that same
        // snapshot. Both halves arrive together or not at all — see
        // `MaterializedMemoryPage::census` for why neither is reachable alone.
        Some(enumeration) => {
            let (evidence, knowledge) = enumeration.pipeline.blocks();
            (
                enumeration.census,
                PipelineBlock {
                    evidence,
                    knowledge,
                    projection: projection.value,
                },
            )
        }
        // `memory.get` (§23.3④: "没有独立 census 的对象读取不生成 `exact`"): a DirectGet has no
        // enumerable universe, so its census carries no enumeration — and §22.0 makes
        // `class=exact` without one a hard 5xx while `classify()` maps DirectGet to `Exact`.
        // The unknown pipeline counts are therefore not laziness here, they are the only
        // reading that keeps this route's own answer (`cannot_establish/count_unknown`) both
        // legal and true: an object read never established a whole-pipeline census, and a page
        // length must never become one.
        None => (
            CensusResult::ok_without_enumeration(),
            PipelineBlock {
                evidence: EvidenceBlock::no_batch(None, CountScope::AuthorizedView),
                knowledge: KnowledgeBlock {
                    eligible: None,
                    processed: None,
                    waiting_key: None,
                    failed: None,
                    count_scope: CountScope::AuthorizedView,
                },
                projection: projection.value,
            },
        ),
    };
    let provenance = provenance(request, binary_build, vec![lane.to_owned()]);
    let lane_status = LaneStatus::Ok;
    envelope_outcome_block(
        request,
        CompletenessInputs {
            lane_status: &lane_status,
            census: &census,
            ledger: &materialized.ledger,
            pipeline: &pipeline,
            provenance: &provenance,
            visible,
            context: None,
        },
        |final_outcome| {
            accept(Envelope {
                items,
                completeness: CompletenessBlock {
                    class: final_outcome.class,
                    reason: final_outcome.reason,
                    exact: final_outcome.exact,
                    known_lower_bound: final_outcome.known_lower_bound,
                    lanes: BTreeMap::from([(lane.to_owned(), lane_status)]),
                    candidate_count: 0,
                    reranked_count: 0,
                    returned,
                    truncated,
                    degradations: projection
                        .degradations
                        .iter()
                        .map(|code| code.line_format())
                        .collect(),
                },
                pipeline: pipeline.clone(),
                provenance: provenance.clone(),
                freshness: FreshnessBlock {
                    class: FreshnessClass::Unknown,
                    latest_evidence_at: None,
                    state_age_seconds: None,
                },
                grounding: materialized.grounding,
                mandatory: MandatoryReport::NotRun,
                pinned: PinnedReport::NotRun,
            })
        },
    )
}
