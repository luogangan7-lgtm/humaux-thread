//! `adapters::projection_worker` — T-projection-worker: makes `humaux-retrieval-worker`
//! actually index private memories per §17.4's contract, restated verbatim in the task card:
//! "upsert -> await/verify search-visible according to adapter policy -> commit stream
//! checkpoint". §4.2 (line 818): there is no separate `projection-worker` process — this is
//! `humaux-retrieval-worker`'s own consumer loop, owned by `role_retrieval_worker`.
//!
//! ## What one [`run_once`] call processes
//!
//! Exactly one §15.1 stream identity (a [`StreamFamily`] + `projection_version`, i.e. one
//! [`StreamKey`]) — never "every ISSUED row across every tenant/stream in one call". A caller
//! (the `humaux-retrieval-worker` binary) instantiates one [`ProjectionWorkerDeps`] per stream
//! it is responsible for and calls [`run_once`] on a timer/loop, same shape
//! `humaux_adapters::public_repo::run_once` already uses for the public plane.
//!
//! ## Per-row order (§17.4, verbatim in the task card)
//!
//! (a) read the next ISSUED `stream_log` rows for this key: (b) resolve the bound
//! `private.memory_records` row through `ops.outbox`/`private.memory_evidence` (c)
//! `embed_cards` (d) reject a dimension mismatch (e) build a `QdrantPointPayload` (f)
//! [`crate::qdrant::upsert`] (g) [`crate::private_projection_registry::register_private_memory_point`]
//! (h) [`crate::qdrant::verify_visible_via_transport`] (i) only then mark the row `DONE` (j)
//! [`crate::stream_repo::advance_prefix`] once for the whole batch.
//!
//! A failure at (b)-(h) marks that one row `FAILED` (or `SKIPPED_BY_POLICY` for the two cases
//! §18.2 defines as a policy exclusion, not a failure — `DataClass::SecretMaterial` and
//! `CardBuildOutcome::Unbuildable`'s sibling `ExcludedSecret`) and moves on to the next row
//! *without* touching the checkpoint — §15.7: one `FAILED` seq blocks every later seq from
//! ever crossing it, so a batch that fails row N and settles row N+1 must still leave
//! `projection_highwater` at `N-1` after this function's own [`stream_repo::advance_prefix`]
//! call. `migrations/0011_roles_and_grants.sql`'s
//! `projection.stream_log_guard_state_transition` trigger only allows `role_retrieval_worker`
//! to move `ISSUED -> {DONE,SKIPPED_BY_POLICY,FAILED}` — there is no legal in-between "leased"
//! state for this role (unlike `role_private_worker`'s `ISSUED -> PROCESSING`), so this module
//! never attempts one; a row is read, fully processed end-to-end (Qdrant calls included), and
//! written straight to its terminal state in one final `UPDATE`.
//!
//! ## RLS and visibility
//!
//! `private.memory_records`' RLS policy (`migrations/0012_row_level_security.sql`, amended by
//! `migrations/0140_memory_records_rls_retrieval_worker_read.sql`) carves out a read-only
//! bypass for `role_retrieval_worker`, mirroring the `role_migration_owner` clause already in
//! the policy: inside the tenant-equality branch, `current_user = 'role_retrieval_worker'`
//! short-circuits the `USER_PRIVATE`/`WORKSPACE_SHARED` visibility disjunction, so this
//! tenant-scoped, headless worker can read every row of its own tenant regardless of
//! `visibility_class`/`visibility_user_id`/membership — real per-query visibility is enforced
//! downstream at Qdrant read time (§6.1.2) via the payload fields this module writes
//! (`visibility_class`/`visibility_user_id`/`visibility_workspace_id`) plus
//! `humaux_projection::dense::visibility_disjunction`, not by restricting what this worker can
//! index. The `WITH CHECK` clause is untouched — this worker never writes
//! `private.memory_records`. [`set_worker_rls_context`] still pins `humaux.user_id` to the nil
//! sentinel on every transaction (not to gate this policy — `current_user` does that — but to
//! keep a pooled connection's copy of that GUC always cast-safe for `private.evidence_objects`'
//! own, un-bypassed policy; see that function's doc for the plan-time crash this avoids).
//!
//! ## Scope
//!
//! This module only supports `scope_kind == "workspace"` streams: [`crate::qdrant::QdrantPointPayload`]'s
//! `workspace_id` field is mandatory (not `Option`), and the only stream-routing workspace this
//! module has to offer it is the family's own `scope_id` (mirrors `remember::token_workspace_id`'s
//! "stream-routing binding, not Evidence visibility" reasoning). A `"tenant"`-scoped family is
//! rejected with [`ErrorCode::InvalidInput`] before any row is touched.

use std::sync::Arc;

use humaux_domain::affect::AffectAnnotation;
use humaux_domain::authority::{AuthorityClass, AuthorityStatus, MemoryId};
use humaux_domain::dataclass::DataClass;
use humaux_domain::error::ErrorCode;
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_domain::memory::MemoryType;
use humaux_domain::subject::SubjectId;
use humaux_infra_cell::{CellAccessPermit, IntraCellHttpTransport};
use humaux_local_secret_scan::{LocalSecretScanner, SealedRetrievalCard};
use humaux_projection::card::{
    CardBudget, CardBuildOutcome, CardInput, EgressDisposition, build_card,
};
use humaux_projection::serving::StreamFamily;
use humaux_projection::stream::StreamKey;
use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use crate::postgres::RetrievalWorkerDbPool;
use crate::private_projection_registry::{
    self, PrivateMemoryPointRegistration, PrivateProjectionRegistryError, RegistrationOutcome,
};
use crate::qdrant::{
    self, PointId, QdrantOperation, QdrantPointPayload, TenantPlacementRow, ha_profile_for,
    verify_visible_via_transport,
};
use crate::stream_repo;

/// The dense-write embedding call this worker needs, shaped like
/// `humaux_retrieval_provider::contract::EmbeddingProvider::embed_cards` but declared locally
/// rather than importing that trait.
///
/// `humaux-retrieval-provider`'s own `Cargo.toml` already depends on `humaux-adapters` (for
/// `RetrievalWorkerDbPool`/`disclosure::reserve_retrieval`/`finalize_retrieval` — see that
/// crate's manifest comment), so `humaux-adapters` cannot add a real (non-dev) dependency back
/// on `humaux-retrieval-provider` without a circular package graph; `EmbeddingProvider` itself
/// is therefore reachable from this crate's `tests/` (a dev-dependency) but never from `src/`.
/// This trait is the seam that keeps `run_once` generic over "something that embeds
/// `SealedRetrievalCard`s" without naming the crate on the wrong side of that edge — a caller
/// in `bins/retrieval-worker` (which depends on both crates with no cycle) or in this crate's
/// own `tests/` implements it by delegating one call to a real `EmbeddingProvider`.
#[async_trait::async_trait]
pub trait CardEmbedder: Send + Sync {
    /// Returns exactly one embedding vector per input card, in order. The caller
    /// ([`process_row`]) checks the returned vector length against the configured dimension
    /// itself — this trait carries no `EmbeddingBatch`-shaped metadata, only the vectors,
    /// precisely to avoid needing that type's crate.
    async fn embed_cards(
        &self,
        tenant_id: TenantId,
        dimension: u32,
        cards: &[SealedRetrievalCard],
        // §7.4: the memories the cards belong to (one per card) — the disclosure sources.
        memory_ids: &[Uuid],
    ) -> Result<Vec<Vec<f32>>, ErrorCode>;
}

/// Everything one [`run_once`] call needs. One instance == one `StreamKey` (`family` +
/// `projection_version`) — see module doc.
pub struct ProjectionWorkerDeps {
    /// `role_retrieval_worker`'s pool — the sole legal writer of this stream's `stream_log`/
    /// `stream_checkpoints` terminal edges (§6.2.2).
    pub pool: RetrievalWorkerDbPool,
    /// The dense-write embedding call — see [`CardEmbedder`]'s doc for why this is a local
    /// trait rather than `humaux_retrieval_provider::contract::EmbeddingProvider` directly.
    pub embedder: Arc<dyn CardEmbedder>,
    /// §1.2.3's sole constructor path for a card `embed_cards` may accept
    /// ([`humaux_local_secret_scan::SealedRetrievalCard`]) — every real caller of
    /// [`CardEmbedder::embed_cards`], this worker included, must scan first.
    pub scanner: Arc<LocalSecretScanner>,
    /// ADR-0003/§83.4 Layer 1B: the injected Qdrant transport (never `reqwest` directly).
    pub transport: Arc<dyn IntraCellHttpTransport>,
    /// The Qdrant `IntraCellResource` access token this deps instance authorizes with.
    pub permit: CellAccessPermit,
    /// §17.3 placement (collection name / shard key) for this tenant's `private_memory_v1`
    /// projection family.
    pub placement: TenantPlacementRow,
    /// The one §15.1 stream identity this deps instance processes — see module doc.
    pub family: StreamFamily,
    /// §17's `embedding_version` payload field — distinct from `projection_version` (module
    /// doc's own frozen warning, `qdrant::QdrantPointPayload`'s doc restates it).
    pub embedding_version: String,
    /// The `stream_log`/`stream_checkpoints` `projection_version` this deps instance settles.
    pub projection_version: String,
    /// The embedding vector width every card this deps instance embeds must produce.
    pub dimension: u32,
}

/// [`run_once`]'s per-batch result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunOnceOutcome {
    /// Rows this call settled `DONE`.
    pub done: u64,
    /// Rows this call settled `SKIPPED_BY_POLICY` (§18.2 `SecretMaterial`/`ExcludedSecret`).
    pub skipped_by_policy: u64,
    /// Rows this call settled `FAILED`.
    pub failed: u64,
    /// Rows left `ISSUED` because their Evidence is not distilled yet (ADR-0016 D6).
    pub pending: u64,
    /// `projection_highwater` after this call's [`stream_repo::advance_prefix`] — unchanged
    /// from before the call if nothing in this batch was contiguous-done-eligible (§15.7).
    pub projection_highwater: u64,
}

/// Sets `humaux.tenant_id` for the remainder of `txn` — same technique as
/// `stream_repo::set_tenant_local`. `humaux.user_id` is pinned to the nil sentinel on every
/// call, not to gate `private.memory_records` (0140's `current_user = 'role_retrieval_worker'`
/// clause does that regardless of this value — this worker never impersonates a principal, per
/// the module doc's RLS note) but to keep a *pooled* connection's `humaux.user_id` GUC always a
/// syntactically valid uuid. Leaving it unset on a connection this worker's own
/// `private_projection_registry::register_private_memory_point` call has previously `SET
/// LOCAL`-ed (even to this same nil value) is not safe: PostgreSQL reverts a custom GUC to an
/// empty-string placeholder, not to NULL, once a transaction that `SET LOCAL`-ed it commits
/// (0031's documented tenant_id hazard, never patched for user_id) — and `private.evidence_objects`'
/// own RLS policy (no role bypass, out of 0140's scope) casts
/// `current_setting('humaux.user_id', true)::uuid` unconditionally in its `USER_PRIVATE`
/// branch. PostgreSQL's planner constant-folds that stable-function cast at *plan time*
/// (verified: even a bare `EXPLAIN`, no `ANALYZE`, throws 22P02 on a leaked `''`) — before the
/// executor's branch-level short-circuiting would ever get a chance to skip it — so re-pinning
/// a valid value here every transaction is the only way to keep this worker's `resolve_memory`
/// join from crashing on a `TENANT_SHARED`-evidence row it has every right to read.
async fn set_worker_rls_context(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "SELECT set_config('humaux.tenant_id', $1, true), set_config('humaux.user_id', $2, true)",
    )
    .bind(tenant_id.to_string())
    .bind(Uuid::nil().to_string())
    .execute(&mut **txn)
    .await?;
    Ok(())
}

fn parse_memory_type(wire: &str) -> Option<MemoryType> {
    Some(match wire {
        "FACT" => MemoryType::Fact,
        "PREFERENCE" => MemoryType::Preference,
        "DECISION" => MemoryType::Decision,
        "REJECTION" => MemoryType::Rejection,
        "STATE" => MemoryType::State,
        "ISSUE" => MemoryType::Issue,
        "LESSON" => MemoryType::Lesson,
        "CONSTRAINT" => MemoryType::Constraint,
        "PROCEDURE" => MemoryType::Procedure,
        "OUTCOME" => MemoryType::Outcome,
        "REFERENCE" => MemoryType::Reference,
        "NOTE" => MemoryType::Note,
        _ => return None,
    })
}

fn parse_authority_status(wire: &str) -> Option<AuthorityStatus> {
    Some(match wire {
        "active" => AuthorityStatus::Active,
        "superseded" => AuthorityStatus::Superseded,
        "revoked" => AuthorityStatus::Revoked,
        "expired" => AuthorityStatus::Expired,
        _ => return None,
    })
}

fn parse_authority_class(wire: &str) -> Option<AuthorityClass> {
    Some(match wire {
        "PublicKnowledge" => AuthorityClass::PublicKnowledge,
        "PrivateKnowledge" => AuthorityClass::PrivateKnowledge,
        "UserPreference" => AuthorityClass::UserPreference,
        "ProjectDecision" => AuthorityClass::ProjectDecision,
        "UserCorrection" => AuthorityClass::UserCorrection,
        "ProjectConstraint" => AuthorityClass::ProjectConstraint,
        "ExplicitTaskContext" => AuthorityClass::ExplicitTaskContext,
        _ => return None,
    })
}

fn parse_visibility_class(wire: &str) -> Option<humaux_domain::identity::VisibilityClass> {
    use humaux_domain::identity::VisibilityClass;
    Some(match wire {
        "USER_PRIVATE" => VisibilityClass::UserPrivate,
        "WORKSPACE_SHARED" => VisibilityClass::WorkspaceShared,
        "TENANT_SHARED" => VisibilityClass::TenantShared,
        _ => return None,
    })
}

/// §7.5.1-adjacent, first-pass mapping (no §18.4 policy registry exists yet, same "later task's
/// deliverable" situation `retrieval-provider::contract`'s module doc names for the pricing/
/// calibration registries): `Sensitive` content needs a policy check before egress,
/// `SecretMaterial` never reaches this function (excluded at [`build_card`] already), anything
/// else is unconditionally allowed.
// ponytail: a real `§18.4` per-tenant policy gate is a separate deliverable; this is the
// smallest disposition that is not simply always-`Allowed` (tracked in coord task 7e6da2f9).
fn egress_disposition_for(data_class: DataClass) -> EgressDisposition {
    match data_class {
        DataClass::Sensitive => EgressDisposition::PolicyGated,
        _ => EgressDisposition::Allowed,
    }
}

fn to_system_time(t: OffsetDateTime) -> std::time::SystemTime {
    std::time::UNIX_EPOCH + std::time::Duration::from_secs(t.unix_timestamp().max(0) as u64)
}

/// One row read out of the [`ResolvedMemory`] join described in the module doc.
struct ResolvedMemory {
    memory_id: MemoryId,
    content: serde_json::Value,
    visibility: humaux_domain::identity::VisibilityDescriptor,
    memory_type: MemoryType,
    status: AuthorityStatus,
    authority: AuthorityClass,
    data_class: DataClass,
    occurred_at: Option<OffsetDateTime>,
    effective_from: Option<OffsetDateTime>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    body_sha256: Vec<u8>,
    /// §6.1.3 / ADR-0029 D-A: the memory's `private.memory_subjects` links, read in the same
    /// transaction as the row (role_retrieval_worker's own SELECT; the 0155 RESTRICTIVE subject
    /// policy exempts this role precisely so a subject-gated row still projects — §15.7).
    subject_ids: Vec<SubjectId>,
    /// §8.5.1 / ADR-0030 D-D: the memory's `private.memory_affects` rows, read in the same
    /// transaction (role_retrieval_worker's own SELECT) through the ONE set-based read
    /// (`affect_repo::affects_for_memories_in_txn`), flattened into the payload by `finish_row`.
    affects: Vec<AffectAnnotation>,
}

/// (b): resolves one `stream_log` row's bound Memory through `ops.outbox` ->
/// `private.memory_evidence` -> `private.memory_records`/`private.evidence_objects` — see
/// module doc for why a row this worker cannot see under RLS resolves to `Ok(None)`, not an
/// error.
async fn resolve_memory(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    commit_seq: i64,
) -> Result<Option<ResolvedMemory>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT m.memory_id, m.content, m.visibility_class, m.visibility_user_id, \
                m.visibility_workspace_id, m.memory_type, m.status, m.authority_class, \
                m.occurred_at, m.effective_from, m.created_at, m.updated_at, eo.data_class, \
                sha256(convert_to(m.content::text, 'UTF8')) AS body_sha256 \
         FROM ops.outbox ob \
         JOIN private.memory_evidence me ON me.evidence_id = ob.evidence_id \
         JOIN private.memory_records m ON m.memory_id = me.memory_id \
         JOIN private.evidence_objects eo ON eo.evidence_id = ob.evidence_id \
         WHERE ob.tenant_id = $1 AND ob.commit_seq = $2 \
         ORDER BY (me.role = 'PRIMARY') DESC, me.ordinal ASC \
         LIMIT 1",
    )
    .bind(tenant_id)
    .bind(commit_seq)
    .fetch_optional(&mut **txn)
    .await?;
    let Some(row) = row else { return Ok(None) };

    let visibility_class: String = row.try_get("visibility_class")?;
    let Some(class) = parse_visibility_class(&visibility_class) else {
        return Ok(None);
    };
    let visibility = humaux_domain::identity::VisibilityDescriptor {
        class,
        user_id: row
            .try_get::<Option<Uuid>, _>("visibility_user_id")?
            .map(UserId),
        workspace_id: row
            .try_get::<Option<Uuid>, _>("visibility_workspace_id")?
            .map(WorkspaceId),
    };
    let memory_type_wire: String = row.try_get("memory_type")?;
    let status_wire: String = row.try_get("status")?;
    let authority_wire: String = row.try_get("authority_class")?;
    let data_class_wire: String = row.try_get("data_class")?;
    let (Some(memory_type), Some(status), Some(authority)) = (
        parse_memory_type(&memory_type_wire),
        parse_authority_status(&status_wire),
        parse_authority_class(&authority_wire),
    ) else {
        return Ok(None);
    };
    let memory_id: Uuid = row.try_get("memory_id")?;
    let subject_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT subject_id FROM private.memory_subjects \
         WHERE tenant_id = $1 AND memory_id = $2 ORDER BY subject_id",
    )
    .bind(tenant_id)
    .bind(memory_id)
    .fetch_all(&mut **txn)
    .await?
    .into_iter()
    .map(SubjectId)
    .collect();
    let affects = crate::affect_repo::affects_for_memories_in_txn(txn, tenant_id, &[memory_id])
        .await?
        .into_iter()
        .map(|row| row.annotation)
        .collect();

    Ok(Some(ResolvedMemory {
        memory_id: MemoryId(memory_id),
        content: row.try_get("content")?,
        visibility,
        memory_type,
        status,
        authority,
        data_class: DataClass::parse_or_secret(&data_class_wire),
        occurred_at: row.try_get("occurred_at")?,
        effective_from: row.try_get("effective_from")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        body_sha256: row.try_get("body_sha256")?,
        subject_ids,
        affects,
    }))
}

fn extract_str(value: &serde_json::Value, key: &str) -> Option<String> {
    value.get(key).and_then(|v| v.as_str()).map(str::to_owned)
}

/// Builds the [`CardInput`] this worker feeds [`build_card`] from a [`ResolvedMemory`].
/// `content` is arbitrary `jsonb` (§8.5) — `title`/`key_claim`/`evidence_excerpt` are read as
/// optional string fields on it, falling back to a truncated stringified `content` for `title`
/// only (never for `key_claim`/`evidence_excerpt` — a missing one of those is a genuine §18.4
/// "缺字段", not something to paper over here).
fn card_input(memory: &ResolvedMemory, workspace_id: WorkspaceId) -> CardInput {
    let title = extract_str(&memory.content, "title")
        .unwrap_or_else(|| memory.content.to_string().chars().take(80).collect());
    let entities = memory
        .content
        .get("entities")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    CardInput {
        memory_id: memory.memory_id,
        memory_type: memory.memory_type,
        data_class: memory.data_class,
        egress_disposition: egress_disposition_for(memory.data_class),
        workspace_id: Some(workspace_id),
        topic: extract_str(&memory.content, "topic"),
        effective_from: to_system_time(
            memory
                .effective_from
                .or(memory.occurred_at)
                .unwrap_or(memory.created_at),
        ),
        title,
        key_claim: extract_str(&memory.content, "key_claim"),
        entities,
        evidence_excerpt: extract_str(&memory.content, "evidence_excerpt"),
    }
}

/// One row's terminal disposition, decided before any `stream_log` write (module doc's per-row
/// order (i)) — `stream_seq`/`error_class` are supplied by the caller, this only carries the
/// state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowTerminal {
    Done,
    SkippedByPolicy,
    Failed,
    /// Not a terminal: the Evidence behind this ticket has not been distilled yet (its
    /// `ops.outbox` row is still PENDING/PROCESSING, ADR-0016 D6) — the row stays `ISSUED` and
    /// is re-read next pass. Never written to `stream_log`.
    Pending,
}

/// ADR-0016 D6: what a ticket whose `ops.outbox` row resolves to no memory means, decided from
/// that row's own status. Distill is asynchronous to remember (§15.5 "0/1/N later"), so "no
/// memory yet" is only a gap while the row is still open; a DONE row with no memory is the
/// legitimate 0-memory outcome and settles as a no-op (`SKIPPED_BY_POLICY`, counted toward the
/// contiguous prefix like every policy exclusion), and a FAILED row fails the ticket.
fn terminal_for_missing_memory(outbox_status: Option<&str>) -> (RowTerminal, &'static str) {
    match outbox_status {
        Some("DONE") => (RowTerminal::SkippedByPolicy, "no_memory_distilled"),
        Some("PENDING" | "PROCESSING") => (RowTerminal::Pending, "distill_pending"),
        Some("FAILED") => (RowTerminal::Failed, "distill_failed"),
        _ => (RowTerminal::Failed, "no_visible_memory_record"),
    }
}

/// Runs (b)-(h) of the module doc's per-row order for exactly one `stream_seq`, given its
/// `commit_seq`. Never returns an `Err` — every failure mode short-circuits into a
/// [`RowTerminal::Failed`]/[`RowTerminal::SkippedByPolicy`] outcome plus a fixed `error_class`
/// tag, because a row failure must never abort the batch (module doc).
async fn process_row(
    deps: &ProjectionWorkerDeps,
    stream_seq: i64,
    commit_seq: i64,
) -> (RowTerminal, &'static str) {
    let workspace_id = WorkspaceId(deps.family.scope_id);

    let (memory, vector) = match resolve_and_embed(deps, workspace_id, commit_seq).await {
        Ok(pair) => pair,
        Err(terminal) => return terminal,
    };

    finish_row(deps, stream_seq, workspace_id, memory, vector).await
}

/// (b)-(d) of the module doc's per-row order: resolve the bound Memory, build+seal its card,
/// and embed it. Split out of [`process_row`] purely to stay under this repo's line-count
/// lint — see that function for the full per-row order.
async fn resolve_and_embed(
    deps: &ProjectionWorkerDeps,
    workspace_id: WorkspaceId,
    commit_seq: i64,
) -> Result<(ResolvedMemory, Vec<f32>), (RowTerminal, &'static str)> {
    let mut txn = deps
        .pool
        .pool()
        .begin()
        .await
        .map_err(|_| (RowTerminal::Failed, "db_begin_failed"))?;
    set_worker_rls_context(&mut txn, deps.family.tenant_id.0)
        .await
        .map_err(|_| (RowTerminal::Failed, "db_rls_context_failed"))?;
    let memory = resolve_memory(&mut txn, deps.family.tenant_id.0, commit_seq)
        .await
        .map_err(|_| (RowTerminal::Failed, "db_resolve_failed"))?;
    let outbox_status: Option<String> = if memory.is_none() {
        sqlx::query_scalar(
            "SELECT status FROM ops.outbox WHERE tenant_id = $1 AND commit_seq = $2 \
             AND event_type = 'EVIDENCE_ACCEPTED' ORDER BY created_at DESC LIMIT 1",
        )
        .bind(deps.family.tenant_id.0)
        .bind(commit_seq)
        .fetch_optional(&mut *txn)
        .await
        .map_err(|_| (RowTerminal::Failed, "db_resolve_failed"))?
    } else {
        None
    };
    txn.commit()
        .await
        .map_err(|_| (RowTerminal::Failed, "db_commit_failed"))?;
    let memory = memory.ok_or_else(|| terminal_for_missing_memory(outbox_status.as_deref()))?;

    let input = card_input(&memory, workspace_id);
    let card = match build_card(input, CardBudget::default()) {
        CardBuildOutcome::Card(card) => card,
        CardBuildOutcome::ExcludedSecret => {
            return Err((RowTerminal::SkippedByPolicy, "secret_material"));
        }
        CardBuildOutcome::Unbuildable => return Err((RowTerminal::Failed, "card_unbuildable")),
    };

    let sealed = deps
        .scanner
        .seal_card(&card)
        .map_err(|_| (RowTerminal::Failed, "secret_scan_failed"))?;

    let vectors = deps
        .embedder
        .embed_cards(
            deps.family.tenant_id,
            deps.dimension,
            &[sealed],
            &[memory.memory_id.0],
        )
        .await
        .map_err(|code| {
            // Operator signal only: the wire `ErrorCode` variant, never provider text (§7.x).
            eprintln!("projection_worker: commit_seq={commit_seq} embedding_failed code={code:?}");
            (RowTerminal::Failed, "embedding_failed")
        })?;
    // (d): reject a dimension mismatch — checked against the actual returned vector length,
    // since this trait carries no separate `EmbeddingBatch::dimension` field (see
    // `CardEmbedder`'s doc).
    let vector = vectors
        .into_iter()
        .next()
        .ok_or((RowTerminal::Failed, "embedding_batch_empty"))?;
    if vector.len() != deps.dimension as usize {
        return Err((RowTerminal::Failed, "embedding_dimension_mismatch"));
    }

    Ok((memory, vector))
}

/// (e)-(h) of the module doc's per-row order: build the payload, upsert, register, verify.
/// Split out of [`process_row`] purely to stay under this repo's line-count lint.
async fn finish_row(
    deps: &ProjectionWorkerDeps,
    stream_seq: i64,
    workspace_id: WorkspaceId,
    memory: ResolvedMemory,
    vector: Vec<f32>,
) -> (RowTerminal, &'static str) {
    let payload = QdrantPointPayload {
        tenant_id: deps.family.tenant_id,
        workspace_id,
        visibility_class: memory.visibility.class,
        visibility_user_id: memory.visibility.user_id,
        visibility_workspace_id: memory.visibility.workspace_id,
        object_type: "memory_record".to_owned(),
        memory_type: memory.memory_type,
        status: memory.status,
        authority: memory.authority,
        created_at: memory.created_at,
        effective_at: memory
            .effective_from
            .or(memory.occurred_at)
            .unwrap_or(memory.created_at),
        embedding_version: deps.embedding_version.clone(),
        projection_version: deps.projection_version.clone(),
        source_stream_seq: stream_seq,
        data_class: memory.data_class,
        egress_disposition: egress_disposition_for(memory.data_class),
    };
    // §18.2: `into_indexable` is the sole index-write gate. `build_card` already refused a
    // `SecretMaterial` input above, so this can only fail if the two disagree — kept as a
    // belt-and-braces check rather than an `expect`, per repo fail-loud convention (§50).
    let Some(indexable) = payload.into_indexable() else {
        return (RowTerminal::SkippedByPolicy, "secret_material");
    };
    // ADR-0029 D-A: subject linkage rides the indexable payload (`subject_ids` array field).
    let indexable = indexable
        .with_subject_ids(memory.subject_ids)
        // ADR-0030 D-D: affect annotations ride the same payload (six flat array fields).
        .with_affects(memory.affects);

    let registration = PrivateMemoryPointRegistration::deterministic(
        deps.family.clone(),
        deps.projection_version.clone(),
        deps.embedding_version.clone(),
        memory.memory_id,
        memory.updated_at,
        memory.body_sha256,
    );
    let point_id = PointId::Uuid(registration.point_id.as_uuid());

    if qdrant::upsert(
        deps.transport.as_ref(),
        &deps.permit,
        &deps.placement.collection_name,
        &[(point_id, &indexable, vector)],
        ha_profile_for(QdrantOperation::NormalImmutableUpsert),
    )
    .await
    .is_err()
    {
        return (RowTerminal::Failed, "qdrant_upsert_failed");
    }

    let authorization = worker_authorization_scope(&deps.family, workspace_id);
    match private_projection_registry::register_private_memory_point(
        &deps.pool,
        &authorization,
        &registration,
    )
    .await
    {
        Ok(RegistrationOutcome::Inserted | RegistrationOutcome::AlreadyRegistered) => {}
        Err(PrivateProjectionRegistryError::PointIdCollision)
        | Err(PrivateProjectionRegistryError::IdentityAlreadyBound)
        | Err(PrivateProjectionRegistryError::RegistryRaceLost) => {
            return (RowTerminal::Failed, "registry_conflict");
        }
        Err(_) => return (RowTerminal::Failed, "registry_failed"),
    }

    match verify_visible_via_transport(
        deps.transport.as_ref(),
        &deps.permit,
        &deps.placement.collection_name,
        &[point_id],
    )
    .await
    {
        Ok(Some(confirmation)) if confirmation.contains(&point_id) => {}
        _ => return (RowTerminal::Failed, "visibility_not_confirmed"),
    }

    (RowTerminal::Done, "")
}

/// Builds the fixed, non-impersonating [`AuthorizationScope`] this headless worker presents to
/// [`private_projection_registry::register_private_memory_point`] — module doc's RLS note: a
/// worker principal, not a real user, `principal`/`user_id` both the nil sentinel
/// (`remember::set_authorization_local`'s own precedent for a headless write).
fn worker_authorization_scope(
    family: &StreamFamily,
    workspace_id: WorkspaceId,
) -> AuthorizationScope {
    AuthorizationScope::new(
        family.tenant_id,
        PrincipalId(Uuid::nil()),
        Some(UserId(Uuid::nil())),
        BoundedSet::new([workspace_id]).expect("one-element set is always within MAX_LEN"),
    )
}

/// Reads up to `batch` `ISSUED` `stream_log` rows for `key`, ascending `stream_seq` — no lease
/// column exists for `role_retrieval_worker` (module doc), so this is a plain read; a second
/// concurrent `run_once` against the same key would race it (single-worker-per-key assumption,
/// same as `humaux_adapters::public_repo`'s own `run_once`).
async fn fetch_issued_rows(
    pool: &RetrievalWorkerDbPool,
    key: &StreamKey,
    batch: i64,
) -> Result<Vec<(i64, i64)>, ErrorCode> {
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    set_worker_rls_context(&mut txn, key.tenant_id.0)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    let rows = sqlx::query(
        "SELECT stream_seq, commit_seq FROM projection.stream_log \
         WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
           AND projection_kind = $5 AND projection_version = $6 AND state = 'ISSUED' \
         ORDER BY stream_seq ASC LIMIT $7",
    )
    .bind(key.tenant_id.0)
    .bind(&key.scope_kind)
    .bind(key.scope_id)
    .bind(&key.domain)
    .bind(&key.projection_kind)
    .bind(&key.projection_version)
    .bind(batch)
    .fetch_all(&mut *txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    txn.commit().await.map_err(|_| ErrorCode::Internal)?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get::<i64, _>("stream_seq")
                    .map_err(|_| ErrorCode::Internal)?,
                row.try_get::<i64, _>("commit_seq")
                    .map_err(|_| ErrorCode::Internal)?,
            ))
        })
        .collect()
}

/// Writes one row's terminal state — the sole legal `role_retrieval_worker` transition,
/// `ISSUED -> {DONE,SKIPPED_BY_POLICY,FAILED}` (`migrations/0011_roles_and_grants.sql`'s
/// `stream_log_guard_state_transition`). A no-op (0 rows) if the row already left `ISSUED`
/// (concurrent settlement) — not an error.
async fn settle_row(
    pool: &RetrievalWorkerDbPool,
    key: &StreamKey,
    stream_seq: i64,
    terminal: RowTerminal,
    error_class: &str,
) -> Result<(), ErrorCode> {
    let state = match terminal {
        RowTerminal::Done => "DONE",
        RowTerminal::SkippedByPolicy => "SKIPPED_BY_POLICY",
        RowTerminal::Failed => "FAILED",
        RowTerminal::Pending => return Ok(()),
    };
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    set_worker_rls_context(&mut txn, key.tenant_id.0)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    let error_class = (!error_class.is_empty()).then_some(error_class);
    sqlx::query(
        "UPDATE projection.stream_log SET state = $8, error_class = $7 \
         WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
           AND projection_kind = $5 AND projection_version = $6 AND stream_seq = $9 \
           AND state = 'ISSUED'",
    )
    .bind(key.tenant_id.0)
    .bind(&key.scope_kind)
    .bind(key.scope_id)
    .bind(&key.domain)
    .bind(&key.projection_kind)
    .bind(&key.projection_version)
    .bind(error_class)
    .bind(state)
    .bind(stream_seq)
    .execute(&mut *txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    txn.commit().await.map_err(|_| ErrorCode::Internal)?;
    Ok(())
}

/// The full consumer loop, one batch: see module doc for the per-row order and the §15.7
/// checkpoint discipline.
pub async fn run_once(
    deps: &ProjectionWorkerDeps,
    batch: usize,
) -> Result<RunOnceOutcome, ErrorCode> {
    if deps.family.scope_kind != "workspace" {
        return Err(ErrorCode::InvalidInput);
    }
    let key = StreamKey::new(
        deps.family.tenant_id,
        deps.family.scope_kind.clone(),
        deps.family.scope_id,
        deps.family.domain.clone(),
        deps.family.projection_kind.clone(),
        deps.projection_version.clone(),
    );

    let rows = fetch_issued_rows(&deps.pool, &key, batch as i64).await?;
    let mut outcome = RunOnceOutcome::default();
    for (stream_seq, commit_seq) in rows {
        let (terminal, error_class) = process_row(deps, stream_seq, commit_seq).await;
        settle_row(&deps.pool, &key, stream_seq, terminal, error_class).await?;
        match terminal {
            RowTerminal::Done => outcome.done += 1,
            RowTerminal::SkippedByPolicy => outcome.skipped_by_policy += 1,
            RowTerminal::Failed => outcome.failed += 1,
            RowTerminal::Pending => outcome.pending += 1,
        }
    }

    // (j): one `advance_prefix` call for the whole batch. §15.7: a `FAILED` seq blocks every
    // later seq — that discipline lives entirely inside `advance_prefix`'s own
    // `contiguous_done_prefix` arithmetic (a `FAILED` row counts as an open gap, never as
    // done), so this call needs no special-casing here. `Inconsistent` means the ledger's own
    // §15.4 identity did not hold — a real fault, not a retry signal, so it surfaces as
    // `ErrorCode::Internal` rather than a guessed fallback number.
    outcome.projection_highwater = stream_repo::advance_prefix(&deps.pool, &key)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    Ok(outcome)
}
