//! `adapters::projection_worker` — T-projection-worker: makes `humaux-retrieval-worker` actually index private
//!   memories per §17.4's contract, restated verbatim in the task card: "upsert -> await/verify search-visible
//!   according to adapter policy -> commit stream checkpoint".
//! Depends-on: crates=[async-trait, humaux-domain, humaux-infra-cell, humaux-local-secret-scan, humaux-projection,
//!   serde_json, sqlx]; services=[PostgreSQL(any) r=[private.evidence_objects, private.memory_evidence,
//!   private.memory_records, private.memory_subjects] w=[ops.outbox, projection.stream_log],
//!   PostgreSQL(role_retrieval_worker)]; env=[]; modules=[adapters::affect_repo, adapters::postgres,
//!   adapters::private_projection_registry, adapters::qdrant, adapters::remember, adapters::stream_repo,
//!   domain::affect, domain::authority, domain::dataclass, domain::egress, domain::error, domain::identity,
//!   domain::ids, domain::memory, domain::subject, humaux-local-secret-scan, infra-cell::permit,
//!   infra-cell::transport, projection::card, projection::serving, projection::stream]
//! Called-by: [retrieval-worker::main, tests]
//! Invariants: [run_claimed_pass claims across tenants by lease and processes one family's tickets sequentially in one
//!   worker; every family a pass holds is lease-renewed while any of them is worked; per row: resolve bound memories,
//!   upsert, verify search-visible, then settle; a transient failure returns the ticket to ISSUED with backoff
//!   (bounded by max attempts, then FAILED transient_exhausted), except that a dependency-outage failure while the
//!   dependency is already known down spends no attempt; a permanent one settles FAILED at once; every settle/retry/release is fenced on (lease_owner, attempts); a DONE is never written
//!   before search-visible confirmation; every point upsert and delete is fenced on the ticket's source_stream_seq, so a
//!   reclaimed worker's late write cannot overwrite or remove a point a later ticket wrote; the fence has no
//!   tombstone, so a stale upsert that lands after a retire delete re-inserts the point (ADR-0057 D-I, known limit 9)]
//! Spec: Baseline §4.2; §15.1; §17.4; §18.2; §15.7; §6.1.2; ADR-0052; ADR-0057
//!
//! §4.2 (line 818): there is no separate `projection-worker` process — this is
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
//! (a) read the next ISSUED `stream_log` rows for this key: (b) resolve the bound Memories —
//! ALL `private.memory_records` rows the ticket's Evidence carries, through
//! `ops.outbox`/`private.memory_evidence` (see `resolve_memories`: one Evidence routinely
//! carries N memories, and card 9 reported that only the first one used to be projected) (c)
//! `embed_cards` (one batched call for the whole Evidence) (d) reject a short batch or a
//! dimension mismatch (e) build a `QdrantPointPayload` per memory (f)
//! [`crate::qdrant::upsert_fenced`] (g) [`crate::private_projection_registry::register_private_memory_point`]
//! (h) [`crate::qdrant::verify_visible_via_transport`] (i) only then mark the row `DONE` (j)
//! [`crate::stream_repo::advance_prefix`] once for the whole batch.
//!
//! A failure at (b)-(h) is classified (ADR-0052 D-E, card 27 — before it, every failure was a
//! permanent `FAILED`, audit C4). A **permanent** one (a 4xx schema/payload refusal, a dimension
//! mismatch, an unbuildable card, a registry conflict, a terminally failed distill) settles the
//! row `FAILED` with its `error_class`; a **transient** one (PG connection/serialization/lock,
//! Qdrant transport/5xx/timeout, a provider 429/5xx, a lost registry race, an unconfirmed
//! visibility probe) returns it to the pool still `ISSUED` ([`stream_repo::release_for_retry`]),
//! with backoff, until `max_attempts` turns it into `FAILED` `transient_exhausted` — never an
//! unbounded retry (ADR-0048). `SKIPPED_BY_POLICY` stays the §18.2 policy exclusion. Either way
//! the next row runs *without* the checkpoint crossing this one — §15.7: one not-yet-DONE seq
//! blocks every later seq, so a batch that fails row N and settles row N+1 still leaves
//! `projection_highwater` at `N-1` after [`stream_repo::advance_prefix`].
//! `migrations/0011_roles_and_grants.sql`'s `projection.stream_log_guard_state_transition`
//! trigger only allows `role_retrieval_worker` to move `ISSUED -> {DONE,SKIPPED_BY_POLICY,FAILED}`;
//! the lease, the attempt counter and the backoff (migration 0176) are column writes with
//! `OLD.state = NEW.state`, which the guard admits, so a leased or backing-off ticket is still
//! `ISSUED` and there is still no in-between state for this role.
//!
//! ## The resident path: [`run_claimed_pass`] (ADR-0052)
//!
//! `humaux-retrieval-worker --serve` / `--run-once` call [`run_claimed_pass`]: the unplaced count
//! ([`stream_repo::unplaced_issued`], one `placement_missing` line), one cross-tenant claim
//! ([`stream_repo::claim_issued`], which also returns each ticket's placement; a ticket whose
//! placement does not parse is parked, never processed), then per family —
//! one worker holds a family at a time (D-A) and runs its tickets in `stream_seq` order — per
//! ticket: the exhaustion check, the family heartbeat ([`stream_repo::renew_family_leases`]; a
//! ticket not renewed was lost and is not settled), a freshly minted Qdrant permit, the same
//! [`process_row`] as below, and a fenced settle / retry / release; then one
//! [`stream_repo::advance_prefix`] per family. For the whole pass a background heartbeat renews
//! every claimed family each `lease_secs / 3`, so a family waiting behind a slow one keeps its
//! lease. A dependency-outage failure (Qdrant, provider incl. quota/budget, scanner, PG
//! connection) while [`SharedProjectionDeps::dependency_down`] is already set is released
//! without spending an attempt; the flag clears on the next `DONE`. [`run_once`] (one key, no lease) stays for the
//! adapter tests and keeps its signature.
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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Duration;

use humaux_domain::affect::AffectAnnotation;
use humaux_domain::authority::{AuthorityClass, AuthorityStatus, MemoryId};
use humaux_domain::dataclass::DataClass;
use humaux_domain::egress::ProcessorId;
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
    retire_points_for_memory,
};
use crate::qdrant::{
    self, PointId, QdrantOperation, QdrantPointPayload, TenantPlacementRow, delete_points,
    ha_profile_for, verify_visible_via_transport,
};
use crate::remember;
use crate::stream_repo::{self, Backoff, ClaimFamily, ClaimedTicket, TicketFence};

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
    /// §7.4 identity of the process running this loop — the same [`ProcessorId`] its embedding
    /// provider discloses under, taken from the deployment's own identity and never
    /// `Uuid::nil()` (card 21). [`stream_repo::advance_prefix`] writes it into
    /// `projection.stream_checkpoints.projection_processor_id`, so a checkpoint names the
    /// worker that advanced it.
    pub processor_id: ProcessorId,
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
    /// Rows returned to `ISSUED` after a transient failure, one attempt spent (ADR-0052 D-E).
    pub retried: u64,
    /// Rows left `ISSUED` because their Evidence is not distilled yet (ADR-0016 D6).
    pub pending: u64,
    /// `projection_highwater` after this call's [`stream_repo::advance_prefix`] — unchanged
    /// from before the call if nothing in this batch was contiguous-done-eligible (§15.7).
    pub projection_highwater: u64,
}

/// Everything [`run_claimed_pass`] shares across tenants (ADR-0052 D-F). No tenant, no placement,
/// no family and no permit: the claim supplies the first three per ticket, and a permit is
/// minted per ticket by `mint_permit` (a resident process outlives any single permit's TTL).
pub struct SharedProjectionDeps {
    /// `role_retrieval_worker`'s pool — claim, heartbeat, settle and every per-row read.
    pub pool: RetrievalWorkerDbPool,
    /// The dense-write embedding call (see [`CardEmbedder`]).
    pub embedder: Arc<dyn CardEmbedder>,
    /// §1.2.3: every card is sealed by the local scanner before it is embedded.
    pub scanner: Arc<LocalSecretScanner>,
    /// ADR-0003/§83.4 Layer 1B: the injected Qdrant transport.
    pub transport: Arc<dyn IntraCellHttpTransport>,
    /// Mints a fresh Qdrant permit (the bin closes over its cell registry and TTL); `None` = the
    /// registry refused, which the pass treats as a transient failure of that ticket.
    pub mint_permit: Arc<dyn Fn() -> Option<CellAccessPermit> + Send + Sync>,
    /// §17 `embedding_version` payload field.
    pub embedding_version: String,
    /// The embedding width every card must produce.
    pub dimension: u32,
    /// §7.4 identity of this process; written into `projection_processor_id` by `advance_prefix`.
    pub processor_id: ProcessorId,
    /// The async timer the pass's background lease heartbeat waits on (the bin's
    /// `tokio::time::sleep`). Injected like `mint_permit` because this crate has no async-runtime
    /// dependency (the `byok.rs` precedent).
    pub sleep: Arc<dyn Fn(Duration) -> Sleep + Send + Sync>,
    /// Review 2026-09-29 P1 (dependency-level breaker): `true` once a ticket failed on a
    /// dependency outage (Qdrant, embedding provider incl. quota/budget, scanner process, PG
    /// connection) and no ticket has reached `DONE` since. While it is `true`, further outage
    /// failures do not spend attempts. Starts `false`; the process owns it across passes.
    pub dependency_down: AtomicBool,
}

/// What [`SharedProjectionDeps::sleep`] returns.
pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send>>;

/// One pass's knobs (ADR-0052 D-F). Every value comes from a required
/// `HUMAUX_RETRIEVAL_WORKER_*` key (§78); nothing here has a default.
#[derive(Debug, Clone)]
pub struct PassConfig {
    /// The §15.1 triple claimed and the §17.3 placement family its tenants must have
    /// ([`ClaimFamily::of`] of the one family the process projects).
    pub claim: ClaimFamily,
    /// `humaux-retrieval-worker/<uuid v7>`, one per process — the fence of every write.
    pub lease_owner: String,
    /// Lease length. A background heartbeat renews every family the pass holds each
    /// `lease_secs / 3`, and each ticket's family once more right before it is processed.
    pub lease_secs: f64,
    /// Tickets per claim (`HUMAUX_RETRIEVAL_WORKER_BATCH`).
    pub batch: i64,
    /// Tickets per tenant per claim (fairness).
    pub per_tenant_cap: i64,
    /// A transient failure at `attempts >= max_attempts` settles `FAILED` `transient_exhausted`.
    pub max_attempts: i32,
    /// Transient-failure backoff.
    pub backoff: Backoff,
}

/// [`run_claimed_pass`]'s counters — one log line per pass (ADR-0052 D-F).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PassOutcome {
    /// Tickets the claim leased.
    pub claimed: u64,
    /// Settled `DONE`.
    pub done: u64,
    /// Settled `SKIPPED_BY_POLICY`.
    pub skipped: u64,
    /// Settled `FAILED` (permanent, or `transient_exhausted`).
    pub failed: u64,
    /// Returned to `ISSUED` with backoff after a transient failure (charged or not).
    pub retried: u64,
    /// Of `retried`: outage failures while the dependency was already down — no attempt spent.
    pub refunded: u64,
    /// Released because the Evidence is still being distilled (attempt given back).
    pub pending: u64,
    /// Tickets not settled because the heartbeat or a fenced write found the lease gone.
    pub lost_lease: u64,
    /// ISSUED tickets whose tenant has no placement row (never claimed).
    pub placement_missing: u64,
    /// Claimed tickets whose placement row this build could not parse — parked with backoff,
    /// attempt given back, never processed.
    pub placement_invalid: u64,
    /// Wall time of the claim call alone.
    pub claim_ms: u64,
}

/// The borrowed view every per-row step reads. [`run_once`] builds it from one
/// [`ProjectionWorkerDeps`]; [`run_claimed_pass`] builds one per family from
/// [`SharedProjectionDeps`] plus the claimed ticket's key and placement.
struct RowCtx<'a> {
    pool: &'a RetrievalWorkerDbPool,
    embedder: &'a dyn CardEmbedder,
    scanner: &'a LocalSecretScanner,
    transport: &'a dyn IntraCellHttpTransport,
    permit: &'a CellAccessPermit,
    placement: &'a TenantPlacementRow,
    family: &'a StreamFamily,
    embedding_version: &'a str,
    projection_version: &'a str,
    dimension: u32,
}

impl<'a> RowCtx<'a> {
    fn of(deps: &'a ProjectionWorkerDeps) -> Self {
        Self {
            pool: &deps.pool,
            embedder: deps.embedder.as_ref(),
            scanner: deps.scanner.as_ref(),
            transport: deps.transport.as_ref(),
            permit: &deps.permit,
            placement: &deps.placement,
            family: &deps.family,
            embedding_version: &deps.embedding_version,
            projection_version: &deps.projection_version,
            dimension: deps.dimension,
        }
    }
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
/// a valid value here every transaction is the only way to keep this worker's `resolve_memories`
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
    /// ADR-0055 D-B: `archived_at IS NOT NULL`, read here so every ticket that re-projects the
    /// memory writes the current flag (no transition-specific ticket exists or is needed).
    archived: bool,
}

/// (b): resolves the Memories one `stream_log` row's bound Evidence carries, through
/// `ops.outbox` -> `private.memory_evidence` -> `private.memory_records`/
/// `private.evidence_objects` — see module doc for why rows this worker cannot see under RLS
/// resolve to an empty `Vec`, not an error.
///
/// **ALL of them, not the PRIMARY one** (card 9's reported limit, closed by card 20 / ADR-0042).
/// A ticket binds an EVIDENCE, and one Evidence routinely carries N Memories: `distill` writes
/// every memory it extracted against the single Evidence the `remember` ticket was issued for,
/// `memory_governance_repo::issue_lifecycle_ticket` binds the target memory's PRIMARY Evidence,
/// and 0155's backfill does the same. With `ORDER BY (role='PRIMARY') DESC, ordinal ASC LIMIT 1`
/// every one of those three issuers could only ever get the FIRST memory indexed — a
/// lifecycle change to memory #2 of an Evidence re-projected memory #1 and silently dropped
/// itself, and a multi-output distill indexed one card out of N. Generalising the resolution
/// here (rather than at each of the three issuers, or by adding a memory-bound ticket shape)
/// is the single point every ticket already routes through.
///
/// The `ORDER BY` is kept: it makes the projection order deterministic (PRIMARY first), which
/// is what the partial-failure retry in [`process_row`] leans on.
async fn resolve_memories(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    commit_seq: i64,
) -> Result<Vec<ResolvedMemory>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT m.memory_id, m.content, m.visibility_class, m.visibility_user_id, \
                m.visibility_workspace_id, m.memory_type, m.status, m.authority_class, \
                m.occurred_at, m.effective_from, m.created_at, m.updated_at, eo.data_class, \
                m.archived_at IS NOT NULL AS archived, \
                sha256(convert_to(m.content::text, 'UTF8')) AS body_sha256 \
         FROM ops.outbox ob \
         JOIN private.memory_evidence me ON me.evidence_id = ob.evidence_id \
         JOIN private.memory_records m ON m.memory_id = me.memory_id \
         JOIN private.evidence_objects eo ON eo.evidence_id = ob.evidence_id \
         WHERE ob.tenant_id = $1 AND ob.commit_seq = $2 \
         ORDER BY (me.role = 'PRIMARY') DESC, me.ordinal ASC",
    )
    .bind(tenant_id)
    .bind(commit_seq)
    .fetch_all(&mut **txn)
    .await?;
    let mut resolved = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(memory) = parse_resolved_memory(txn, tenant_id, row).await? {
            resolved.push(memory);
        }
    }
    Ok(resolved)
}

/// One `resolve_memories` row -> [`ResolvedMemory`]; `Ok(None)` for a row whose closed-enum
/// wire values this build does not know (same fail-soft the single-row version had).
async fn parse_resolved_memory(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    row: sqlx::postgres::PgRow,
) -> Result<Option<ResolvedMemory>, sqlx::Error> {
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
        archived: row.try_get("archived")?,
    }))
}

fn extract_str(value: &serde_json::Value, key: &str) -> Option<String> {
    value.get(key).and_then(|v| v.as_str()).map(str::to_owned)
}

/// Builds the [`CardInput`] this worker feeds [`build_card`] from a [`ResolvedMemory`].
/// `content` is arbitrary `jsonb` (§8.5) — `title`/`key_claim`/`evidence_excerpt` are read as
/// optional string fields on it, falling back to a truncated stringified `content` for `title`
/// only (never for `key_claim`/`evidence_excerpt` — a missing one of those is a genuine §18.4
/// "缺字段", not something to paper over here). A bare JSON string is not a record with missing
/// fields: it is the whole statement, so it is the claim and the title.
fn card_input(memory: &ResolvedMemory, workspace_id: WorkspaceId) -> CardInput {
    // ADR-0057 D-B: `memory.correct` stores the user's corrected text verbatim as a JSON string
    // (gateway `memory_correct`); read as an object it had no claim, so every correction settled
    // FAILED `card_unbuildable` and the corrected memory never reached the index.
    let statement = memory.content.as_str();
    let title = extract_str(&memory.content, "title")
        .or_else(|| statement.map(|text| text.chars().take(80).collect()))
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
        key_claim: extract_str(&memory.content, "key_claim")
            .or_else(|| statement.map(str::to_owned)),
        entities,
        evidence_excerpt: extract_str(&memory.content, "evidence_excerpt"),
    }
}

/// One row's disposition, decided before any `stream_log` write (module doc's per-row order
/// (i)) — `stream_seq`/`error_class` are supplied by the caller, this only carries the state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowTerminal {
    Done,
    SkippedByPolicy,
    /// Permanent: the identical attempt would fail again (ADR-0052 D-E).
    Failed,
    /// Transient: back to the pool, still `ISSUED`, one attempt spent (ADR-0052 D-E). Never a
    /// terminal state; bounded by `max_attempts` in [`run_claimed_pass`].
    Retry,
    /// Not a terminal: the Evidence behind this ticket has not been distilled yet (its
    /// `ops.outbox` row is still PENDING/PROCESSING, ADR-0016 D6) — the row stays `ISSUED` and
    /// is re-read next pass. Never written as a state.
    Pending,
}

/// ADR-0052 D-E, PG half: a lost connection, an exhausted or closed pool, a serialization
/// failure, a deadlock, a lock timeout, an admin shutdown or a resource shortage can succeed on
/// retry; a data or constraint error cannot.
fn pg_is_transient(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => true,
        sqlx::Error::Database(db) => db.code().is_some_and(|code| {
            code.starts_with("08")
                || code.starts_with("57P")
                || code.starts_with("53")
                || matches!(code.as_ref(), "40001" | "40P01" | "55P03")
        }),
        _ => false,
    }
}

/// A PG failure at `class`'s site, classified.
fn pg_failure(error: &sqlx::Error, class: &'static str) -> (RowTerminal, &'static str) {
    if pg_is_transient(error) {
        (RowTerminal::Retry, class)
    } else {
        (RowTerminal::Failed, class)
    }
}

/// ADR-0052 D-E, `ErrorCode` half (embedding provider and local secret scanner): the callee
/// refused this input or this caller — retrying the identical call cannot help — versus
/// everything else (rate limits, 5xx, budget, key pending, a scanner process that did not run).
fn code_is_transient(code: ErrorCode) -> bool {
    !matches!(
        code,
        ErrorCode::ProviderPermanent
            | ErrorCode::InvalidInput
            | ErrorCode::Forbidden
            | ErrorCode::Unauthorized
            | ErrorCode::EntitlementRequired
            | ErrorCode::TenantBoundary
            | ErrorCode::NotFound
    )
}

/// Review 2026-09-29 P1: of the transient classes, the ones that say "a dependency this worker
/// needs for every ticket is unavailable" — Qdrant (transport, 5xx, 429, and a refused permit),
/// the embedding provider (429/5xx, quota, cost budget, key pending), the scanner process, a lost
/// PG connection — as opposed to a race or probe about this one ticket (`registry_failed`,
/// `visibility_not_confirmed`, `embedding_batch_empty`), which always spends.
fn is_dependency_outage(class: &str) -> bool {
    matches!(
        class,
        "qdrant_upsert_failed" | "qdrant_delete_failed" | "embedding_failed" | "secret_scan_failed"
    ) || class.starts_with("db_")
}

/// ADR-0052 D-E, registry half. A source that stopped being live or changed, a lost race and a
/// transient DB error resolve on the next attempt (which re-reads the memory's current state);
/// an identity conflict, a scope violation or a data error does not.
fn registry_failure(error: &PrivateProjectionRegistryError) -> (RowTerminal, &'static str) {
    match error {
        PrivateProjectionRegistryError::SourceNotLive
        | PrivateProjectionRegistryError::SourceChanged
        | PrivateProjectionRegistryError::RegistryRaceLost => {
            (RowTerminal::Retry, "registry_failed")
        }
        PrivateProjectionRegistryError::Db(e) => pg_failure(e, "registry_failed"),
        PrivateProjectionRegistryError::PointIdCollision
        | PrivateProjectionRegistryError::IdentityAlreadyBound => {
            (RowTerminal::Failed, "registry_conflict")
        }
        PrivateProjectionRegistryError::CrossTenant
        | PrivateProjectionRegistryError::CrossWorkspace
        | PrivateProjectionRegistryError::InvalidInput
        | PrivateProjectionRegistryError::MissingAuthenticatedUser => {
            (RowTerminal::Failed, "registry_failed")
        }
    }
}

/// ADR-0016 D6: what a ticket whose `ops.outbox` row resolves to no memory means, decided from
/// that row's own status. Distill is asynchronous to remember (§15.5 "0/1/N later"), so "no
/// memory yet" is only a gap while the row is still open; a DONE row with no memory is the
/// legitimate 0-memory outcome and settles as a no-op (`SKIPPED_BY_POLICY`, counted toward the
/// contiguous prefix like every policy exclusion), and a FAILED row fails the ticket
/// (permanently: 0167's retirable classes).
fn terminal_for_missing_memory(outbox_status: Option<&str>) -> (RowTerminal, &'static str) {
    match outbox_status {
        Some("DONE") => (RowTerminal::SkippedByPolicy, "no_memory_distilled"),
        Some("PENDING" | "PROCESSING") => (RowTerminal::Pending, "distill_pending"),
        Some("FAILED") => (RowTerminal::Failed, "distill_failed"),
        _ => (RowTerminal::Failed, "no_visible_memory_record"),
    }
}

/// Runs (b)-(h) of the module doc's per-row order for exactly one `stream_seq`, given its
/// `commit_seq`. Never returns an `Err` — every failure mode short-circuits into a classified
/// [`RowTerminal`] plus a fixed `error_class` tag, because a row failure must never abort the
/// batch (module doc).
async fn process_row(
    ctx: &RowCtx<'_>,
    stream_seq: i64,
    commit_seq: i64,
) -> (RowTerminal, &'static str) {
    let workspace_id = WorkspaceId(ctx.family.scope_id);

    let prepared = match resolve_and_embed(ctx, workspace_id, commit_seq).await {
        Ok(prepared) => prepared,
        Err(terminal) => return terminal,
    };

    // One ticket, N memories (see `resolve_memories`). The row's disposition is the fold:
    //   * the FIRST failure short-circuits the whole row (Retry or Failed as classified) — §15.7
    //     already requires one unsettled seq to block the prefix, and the retry is safe because
    //     every step `finish_row` performs is idempotent on a deterministic `point_id` (upsert,
    //     `AlreadyRegistered`, verify), so the memories that already landed simply land again;
    //   * `SKIPPED_BY_POLICY` is per-memory (§18.2 secret material), so it only becomes the
    //     row's terminal when NO memory on this Evidence was indexable;
    //   * one indexed memory makes the row `DONE`;
    //   * a memory that is no longer live (ADR-0049) is retired instead of indexed, and a
    //     retirement that lands counts toward `DONE` like an index write.
    let mut any_done = false;
    let mut skipped: Option<(RowTerminal, &'static str)> = None;
    for memory in prepared.dead {
        match retire_row(ctx, stream_seq, workspace_id, &memory).await {
            (RowTerminal::Done, _) => any_done = true,
            other => return other,
        }
    }
    for (memory, vector) in prepared.live {
        match finish_row(ctx, stream_seq, workspace_id, memory, vector).await {
            (RowTerminal::Done, _) => any_done = true,
            (RowTerminal::SkippedByPolicy, class) => {
                skipped = Some((RowTerminal::SkippedByPolicy, class));
            }
            other => return other,
        }
    }
    if any_done {
        (RowTerminal::Done, "")
    } else {
        // `resolve_and_embed` returns a non-empty vec or an `Err`, so `skipped` is `Some` here.
        skipped.unwrap_or((RowTerminal::Failed, "no_visible_memory_record"))
    }
}

/// What [`resolve_and_embed`] hands [`process_row`]: the live memories with their vectors, and
/// the memories whose ticket means retirement (ADR-0049). Never both empty.
struct Prepared {
    live: Vec<(ResolvedMemory, Vec<f32>)>,
    dead: Vec<ResolvedMemory>,
}

/// (b)-(d) of the module doc's per-row order: resolve the bound Evidence's Memories, build+seal
/// each card, and embed them in ONE batch. Split out of [`process_row`] purely to stay under
/// this repo's line-count lint — see that function for the full per-row order. Returns a
/// non-empty vec or an `Err`.
async fn resolve_and_embed(
    ctx: &RowCtx<'_>,
    workspace_id: WorkspaceId,
    commit_seq: i64,
) -> Result<Prepared, (RowTerminal, &'static str)> {
    // dep: PostgreSQL(any) — transaction entry for `resolve_and_embed`
    let mut txn = ctx
        .pool
        .pool()
        .begin()
        .await
        .map_err(|e| pg_failure(&e, "db_begin_failed"))?;
    set_worker_rls_context(&mut txn, ctx.family.tenant_id.0)
        .await
        .map_err(|e| pg_failure(&e, "db_rls_context_failed"))?;
    let memories = resolve_memories(&mut txn, ctx.family.tenant_id.0, commit_seq)
        .await
        .map_err(|e| pg_failure(&e, "db_resolve_failed"))?;
    let outbox_status: Option<String> = if memories.is_empty() {
        sqlx::query_scalar(
            "SELECT status FROM ops.outbox WHERE tenant_id = $1 AND commit_seq = $2 \
             AND event_type = 'EVIDENCE_ACCEPTED' ORDER BY created_at DESC LIMIT 1",
        )
        .bind(ctx.family.tenant_id.0)
        .bind(commit_seq)
        .fetch_optional(&mut *txn)
        .await
        .map_err(|e| pg_failure(&e, "db_resolve_failed"))?
    } else {
        None
    };
    txn.commit()
        .await
        .map_err(|e| pg_failure(&e, "db_commit_failed"))?;
    if memories.is_empty() {
        return Err(terminal_for_missing_memory(outbox_status.as_deref()));
    }
    // ADR-0049: a memory that stopped being live (superseded / revoked / expired) gets no card,
    // no vector and no registration — the registry binds live sources only
    // (`current_source_matches`), so the ticket's consequence for it is retirement
    // (`retire_row`). Before this the supersede ticket failed `registry_failed` every time
    // (card 24 rehearsal 2026-09-26) and wedged the §15.4 prefix behind it.
    let (dead, memories): (Vec<ResolvedMemory>, Vec<ResolvedMemory>) = memories
        .into_iter()
        .partition(|memory| memory.status != AuthorityStatus::Active);

    // §18.2 `ExcludedSecret` is a property of ONE memory, not of the ticket: on an Evidence that
    // carries a secret memory and an ordinary one, the ordinary one must still be indexed. Only
    // an Evidence where NOTHING is indexable settles the row `SKIPPED_BY_POLICY`. `Unbuildable`
    // stays a whole-row permanent failure — the stored record itself is malformed (§18.4).
    let mut kept: Vec<ResolvedMemory> = Vec::with_capacity(memories.len());
    let mut sealed_cards = Vec::with_capacity(memories.len());
    let mut memory_ids: Vec<Uuid> = Vec::with_capacity(memories.len());
    for memory in memories {
        let card = match build_card(card_input(&memory, workspace_id), CardBudget::default()) {
            CardBuildOutcome::Card(card) => card,
            CardBuildOutcome::ExcludedSecret => continue,
            CardBuildOutcome::Unbuildable => {
                return Err((RowTerminal::Failed, "card_unbuildable"));
            }
        };
        // ADR-0052 D-E: a scanner process that did not run (`DependencyUnavailable`) is
        // environmental; a gitleaks finding (`Forbidden`) or an unsealable card (`InvalidInput`)
        // is a verdict on this card and would be the same verdict on every retry.
        sealed_cards.push(ctx.scanner.seal_card(&card).map_err(|code| {
            if code_is_transient(code) {
                (RowTerminal::Retry, "secret_scan_failed")
            } else {
                (RowTerminal::Failed, "secret_scan_rejected")
            }
        })?);
        memory_ids.push(memory.memory_id.0);
        kept.push(memory);
    }
    if kept.is_empty() {
        if dead.is_empty() {
            return Err((RowTerminal::SkippedByPolicy, "secret_material"));
        }
        return Ok(Prepared {
            live: Vec::new(),
            dead,
        });
    }

    // One embed call for the whole Evidence — `embed_cards` is already batch-shaped, so N
    // memories cost one provider round trip, not N (§19.2 per-purpose budget).
    let vectors = ctx
        .embedder
        .embed_cards(
            ctx.family.tenant_id,
            ctx.dimension,
            &sealed_cards,
            &memory_ids,
        )
        .await
        .map_err(|code| {
            // Operator signal only: the wire `ErrorCode` variant, never provider text (§7.x).
            eprintln!("projection_worker: commit_seq={commit_seq} embedding_failed code={code:?}");
            if code_is_transient(code) {
                (RowTerminal::Retry, "embedding_failed")
            } else {
                (RowTerminal::Failed, "embedding_rejected")
            }
        })?;
    // (d): reject a short batch or a dimension mismatch — checked against the actual returned
    // vector lengths, since this trait carries no separate `EmbeddingBatch::dimension` field
    // (see `CardEmbedder`'s doc). A provider that returns fewer vectors than cards would
    // otherwise silently drop the tail memories of a multi-memory Evidence. A short batch is a
    // provider blip (transient); a wrong width is configuration (permanent).
    if vectors.len() != kept.len() {
        return Err((RowTerminal::Retry, "embedding_batch_empty"));
    }
    if vectors.iter().any(|v| v.len() != ctx.dimension as usize) {
        return Err((RowTerminal::Failed, "embedding_dimension_mismatch"));
    }

    Ok(Prepared {
        live: kept.into_iter().zip(vectors).collect(),
        dead,
    })
}

/// (e)-(h) of the module doc's per-row order: build the payload, upsert, register, verify.
/// Split out of [`process_row`] purely to stay under this repo's line-count lint.
#[allow(clippy::too_many_lines)] // 101: ADR-0055's `with_archived` is one more payload builder step.
async fn finish_row(
    ctx: &RowCtx<'_>,
    stream_seq: i64,
    workspace_id: WorkspaceId,
    memory: ResolvedMemory,
    vector: Vec<f32>,
) -> (RowTerminal, &'static str) {
    let payload = QdrantPointPayload {
        tenant_id: ctx.family.tenant_id,
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
        embedding_version: ctx.embedding_version.to_owned(),
        projection_version: ctx.projection_version.to_owned(),
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
        .with_affects(memory.affects)
        // ADR-0055 D-B: the lifecycle prefilter flag, from the PG row this ticket resolved.
        .with_archived(memory.archived);

    let registration = PrivateMemoryPointRegistration::deterministic(
        ctx.family.clone(),
        ctx.projection_version.to_owned(),
        ctx.embedding_version.to_owned(),
        memory.memory_id,
        memory.updated_at,
        memory.body_sha256,
    );
    let point_id = PointId::Uuid(registration.point_id.as_uuid());

    // ADR-0057 D-I: fenced on this ticket's seq — a stalled worker whose lease was reclaimed
    // cannot overwrite a point a later ticket of the same memory already wrote.
    // ponytail: no tombstone behind the seq fence — a stale upsert after a retire delete finds no
    // point and re-inserts it (reads as A2 overshoot, never a false close; ADR-0057 known limits
    // 9-10). Card 37's generation fence on the registry row is the upgrade path.
    if let Err(error) = qdrant::upsert_fenced(
        ctx.transport,
        ctx.permit,
        &ctx.placement.collection_name,
        (point_id, &indexable, vector),
        ha_profile_for(QdrantOperation::NormalImmutableUpsert),
    )
    .await
    {
        // ADR-0052 D-E: 400/422 (payload, schema, vector width) is permanent; transport, 5xx,
        // 404, 408, 409, 429 are transient. Operator signal: the status class only.
        eprintln!(
            "projection_worker: stream_seq={stream_seq} qdrant_upsert transient={}",
            error.is_transient()
        );
        return if error.is_transient() {
            (RowTerminal::Retry, "qdrant_upsert_failed")
        } else {
            (RowTerminal::Failed, "qdrant_upsert_rejected")
        };
    }

    let authorization = worker_authorization_scope(ctx.family, workspace_id);
    match private_projection_registry::register_private_memory_point(
        ctx.pool,
        &authorization,
        &registration,
    )
    .await
    {
        Ok(
            RegistrationOutcome::Inserted
            | RegistrationOutcome::AlreadyRegistered
            | RegistrationOutcome::Revived,
        ) => {}
        Err(error) => {
            let (terminal, class) = registry_failure(&error);
            // ADR-0052 D-E (b): the upsert above landed a point the registry refused to bind, so
            // no later retirement could ever find it (retirement walks registered points only,
            // D-A's race). Delete that deterministic id before returning — except on a point-id
            // collision, where the id IS registered, to another source, and deleting it would
            // take that source's live point with it. A failed compensation is transient: the
            // retry re-upserts the same id and tries again.
            if !matches!(error, PrivateProjectionRegistryError::PointIdCollision)
                && delete_points(
                    ctx.transport,
                    ctx.permit,
                    &ctx.placement.collection_name,
                    &[point_id],
                    stream_seq,
                    ha_profile_for(QdrantOperation::CorrectionDeleteSupersede),
                )
                .await
                .is_err()
            {
                return (RowTerminal::Retry, class);
            }
            return (terminal, class);
        }
    }

    match verify_visible_via_transport(
        ctx.transport,
        ctx.permit,
        &ctx.placement.collection_name,
        &[point_id],
    )
    .await
    {
        Ok(Some(confirmation)) if confirmation.contains(&point_id) => {}
        // ADR-0052 D-E: not yet observed is a retry, not a verdict — the point is registered,
        // so the next attempt re-upserts the same id and probes again.
        _ => return (RowTerminal::Retry, "visibility_not_confirmed"),
    }

    (RowTerminal::Done, "")
}

/// ADR-0049: the projection consequence of a memory that is no longer live. The registry binds
/// live sources only, so the memory's bindings in this family are retired and every point ever
/// bound for it (live or already retired — retry-safe) leaves the index. `DONE` settles the
/// ticket and lets the §15.4 prefix advance past it; the reader's PG re-check
/// (`resolve_private_memory_points`) never served the dead memory anyway, so this is hygiene
/// plus settlement, not a visibility change.
async fn retire_row(
    ctx: &RowCtx<'_>,
    stream_seq: i64,
    workspace_id: WorkspaceId,
    memory: &ResolvedMemory,
) -> (RowTerminal, &'static str) {
    let authorization = worker_authorization_scope(ctx.family, workspace_id);
    let points = match retire_points_for_memory(
        ctx.pool,
        &authorization,
        ctx.family,
        ctx.projection_version,
        ctx.embedding_version,
        memory.memory_id,
    )
    .await
    {
        Ok(points) => points,
        Err(error) => return registry_failure(&error),
    };
    if points.is_empty() {
        return (RowTerminal::Done, "");
    }
    let ids = points
        .iter()
        .map(|point| PointId::Uuid(point.as_uuid()))
        .collect::<Vec<_>>();
    if let Err(error) = delete_points(
        ctx.transport,
        ctx.permit,
        &ctx.placement.collection_name,
        &ids,
        // ADR-0057 D-I: the delete mirror of the upsert fence (a later ticket's revive survives).
        stream_seq,
        ha_profile_for(QdrantOperation::CorrectionDeleteSupersede),
    )
    .await
    {
        return if error.is_transient() {
            (RowTerminal::Retry, "qdrant_delete_failed")
        } else {
            (RowTerminal::Failed, "qdrant_delete_rejected")
        };
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

/// The legacy single-key read: up to `batch` `ISSUED` rows for `key`, ascending `stream_seq`,
/// returning `(stream_seq, commit_seq, attempts)`. Unleased on purpose — it takes only rows no
/// `--serve` process holds (`lease_owner IS NULL`) and whose backoff has elapsed; a second
/// concurrent `run_once` against the same key would race it (single-worker-per-key assumption,
/// same as `humaux_adapters::public_repo`'s own `run_once`). The resident path claims instead
/// ([`stream_repo::claim_issued`]).
async fn fetch_issued_rows(
    pool: &RetrievalWorkerDbPool,
    key: &StreamKey,
    batch: i64,
) -> Result<Vec<(i64, i64, i32)>, ErrorCode> {
    // dep: PostgreSQL(role_retrieval_worker) — transaction entry for `fetch_issued_rows`
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    set_worker_rls_context(&mut txn, key.tenant_id.0)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    let rows = sqlx::query(
        "SELECT stream_seq, commit_seq, attempts FROM projection.stream_log \
         WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
           AND projection_kind = $5 AND projection_version = $6 AND state = 'ISSUED' \
           AND lease_owner IS NULL \
           AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()) \
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
                row.try_get::<i32, _>("attempts")
                    .map_err(|_| ErrorCode::Internal)?,
            ))
        })
        .collect()
}

/// Writes one row's terminal state — the sole legal `role_retrieval_worker` transition,
/// `ISSUED -> {DONE,SKIPPED_BY_POLICY,FAILED}` (`migrations/0011_roles_and_grants.sql`'s
/// `stream_log_guard_state_transition`) — and clears the lease in the same statement. Fenced on
/// `fence` (ADR-0052 D-D): returns `false`, not an error, when the fence matched nothing (the
/// row already left `ISSUED`, or its lease was re-claimed by another worker).
async fn settle_row(
    pool: &RetrievalWorkerDbPool,
    key: &StreamKey,
    stream_seq: i64,
    commit_seq: i64,
    terminal: RowTerminal,
    error_class: &str,
    fence: TicketFence<'_>,
) -> Result<bool, ErrorCode> {
    let state = match terminal {
        RowTerminal::Done => "DONE",
        RowTerminal::SkippedByPolicy => "SKIPPED_BY_POLICY",
        RowTerminal::Failed => "FAILED",
        RowTerminal::Retry | RowTerminal::Pending => return Ok(false),
    };
    // dep: PostgreSQL(role_retrieval_worker) — transaction entry for `settle_row`
    let mut txn = pool.pool().begin().await.map_err(|_| ErrorCode::Internal)?;
    set_worker_rls_context(&mut txn, key.tenant_id.0)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    let error_class = (!error_class.is_empty()).then_some(error_class);
    let settled = sqlx::query(
        "UPDATE projection.stream_log \
            SET state = $8, error_class = $7, \
                lease_owner = NULL, lease_expires_at = NULL, next_attempt_at = NULL \
         WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
           AND projection_kind = $5 AND projection_version = $6 AND stream_seq = $9 \
           AND state = 'ISSUED' \
           AND lease_owner IS NOT DISTINCT FROM $10 AND attempts = $11",
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
    .bind(fence.lease_owner)
    .bind(fence.attempts)
    .execute(&mut *txn)
    .await
    .map_err(|_| ErrorCode::Internal)?
    .rows_affected()
        == 1;
    if !settled {
        txn.commit().await.map_err(|_| ErrorCode::Internal)?;
        return Ok(false);
    }
    // ADR-0049 D-C: the ticket's carrier row. A MEMORY_LIFECYCLE / MEMORY_PUBLISHED outbox row
    // exists to bind `evidence_id` to this ticket (ADR-0018 §4); no distiller ever claims it,
    // so until now it stayed PENDING forever and every "backlog drained" measure (the soak's
    // `backlog_drained`, the rehearsal's undrained-writes assertion) counted it as open work.
    // The settled ticket is its completion, and the carrier MIRRORS the ticket's terminal:
    // DONE / SKIPPED_BY_POLICY settle it DONE, a FAILED ticket leaves it FAILED — a failed
    // lifecycle projection must not read as drained work (card 24 review P1). EVIDENCE_ACCEPTED
    // rows belong to the distiller and are kept apart by the event_type filter, not by status.
    let carrier_status = if terminal == RowTerminal::Failed {
        "FAILED"
    } else {
        "DONE"
    };
    sqlx::query(
        "UPDATE ops.outbox SET status = $4, processed_at = now() \
         WHERE tenant_id = $1 AND commit_seq = $2 AND event_type = ANY($3) \
           AND status = 'PENDING'",
    )
    .bind(key.tenant_id.0)
    .bind(commit_seq)
    .bind(vec![remember::MEMORY_LIFECYCLE, remember::MEMORY_PUBLISHED])
    .bind(carrier_status)
    .execute(&mut *txn)
    .await
    .map_err(|_| ErrorCode::Internal)?;
    txn.commit().await.map_err(|_| ErrorCode::Internal)?;
    Ok(true)
}

/// The legacy single-key consumer loop, one batch: see module doc for the per-row order and the
/// §15.7 checkpoint discipline. No lease and no `max_attempts` of its own — nothing resident
/// drives it any more (the binary's `--run-once` is one [`run_claimed_pass`]); a transient row is
/// returned with one attempt spent and no backoff, and the next caller decides.
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
    let ctx = RowCtx::of(deps);

    let rows = fetch_issued_rows(&deps.pool, &key, batch as i64).await?;
    let mut outcome = RunOnceOutcome::default();
    for (stream_seq, commit_seq, attempts) in rows {
        let fence = TicketFence {
            lease_owner: None,
            attempts,
        };
        let (terminal, error_class) = process_row(&ctx, stream_seq, commit_seq).await;
        match terminal {
            RowTerminal::Retry => {
                stream_repo::release_for_retry(
                    &deps.pool,
                    &key,
                    stream_seq,
                    fence,
                    error_class,
                    None,
                    true,
                )
                .await
                .map_err(|_| ErrorCode::Internal)?;
            }
            RowTerminal::Pending => {}
            _ => {
                settle_row(
                    &deps.pool,
                    &key,
                    stream_seq,
                    commit_seq,
                    terminal,
                    error_class,
                    fence,
                )
                .await?;
            }
        }
        match terminal {
            RowTerminal::Done => outcome.done += 1,
            RowTerminal::SkippedByPolicy => outcome.skipped_by_policy += 1,
            RowTerminal::Failed => outcome.failed += 1,
            RowTerminal::Retry => outcome.retried += 1,
            RowTerminal::Pending => outcome.pending += 1,
        }
    }

    // (j): one `advance_prefix` call for the whole batch. §15.7: a not-yet-DONE seq blocks
    // every later seq — that discipline lives entirely inside `advance_prefix`'s own
    // `contiguous_done_prefix` arithmetic, so this call needs no special-casing here.
    // `Inconsistent` means the ledger's own §15.4 identity did not hold — a real fault, not a
    // retry signal, so it surfaces as `ErrorCode::Internal` rather than a guessed fallback.
    // The watermark carries this worker's §7.4 identity into `projection_processor_id`
    // (migration 0171): the checkpoint this process advanced says which process advanced it.
    outcome.projection_highwater = stream_repo::advance_prefix(&deps.pool, &key, deps.processor_id)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    Ok(outcome)
}

/// ADR-0052 D-F: one resident pass across every placed tenant — see the module doc's "resident
/// path" section for the order. Returns `Err` only when the database is unreachable for the
/// unplaced count or the claim itself (the caller logs it and polls again); a per-ticket failure
/// is classified and counted, never propagated. A settle/retry/release that cannot reach the
/// database either leaves the ticket leased, and the lease expiry returns it (the expired-lease
/// arm of the claim).
pub async fn run_claimed_pass(
    shared: &SharedProjectionDeps,
    cfg: &PassConfig,
) -> Result<PassOutcome, ErrorCode> {
    let mut outcome = PassOutcome::default();

    let unplaced = stream_repo::unplaced_issued(&shared.pool, &cfg.claim)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    outcome.placement_missing = unplaced.iter().map(|(_, n)| *n as u64).sum();
    if let Some((first, _)) = unplaced.first() {
        // One line per pass, not per tenant: a poll every second must not flood the log.
        eprintln!(
            "projection_worker: placement_missing tenants={} tickets={} first_tenant={first}",
            unplaced.len(),
            outcome.placement_missing
        );
    }

    let started = std::time::Instant::now();
    let batch = stream_repo::claim_issued(
        &shared.pool,
        &cfg.claim,
        &cfg.lease_owner,
        cfg.lease_secs,
        cfg.batch,
        cfg.per_tenant_cap,
    )
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;
    outcome.claim_ms = started.elapsed().as_millis() as u64;
    let mut claimed = batch.tickets;
    outcome.claimed = (claimed.len() + batch.unplaceable.len()) as u64;
    // Review 2026-09-29 P1: a placement this build cannot parse (deploy skew) parks only its own
    // tickets — lease cleared, attempt given back, backoff so it is not re-claimed every poll —
    // and every co-claimed ticket is processed as usual.
    for ticket in batch.unplaceable {
        eprintln!(
            "projection_worker: placement_invalid tenant={} stream_seq={}",
            ticket.tenant_id, ticket.stream_seq
        );
        let key = StreamKey::new(
            TenantId(ticket.tenant_id),
            ticket.scope_kind,
            ticket.scope_id,
            ticket.domain,
            ticket.projection_kind,
            ticket.projection_version,
        );
        let fence = TicketFence {
            lease_owner: Some(&cfg.lease_owner),
            attempts: ticket.attempts,
        };
        let parked = stream_repo::release_for_retry(
            &shared.pool,
            &key,
            ticket.stream_seq,
            fence,
            "placement_invalid",
            Some(cfg.backoff),
            false,
        )
        .await;
        if parked.is_ok_and(|written| written) {
            outcome.placement_invalid += 1;
        } else {
            outcome.lost_lease += 1;
        }
    }

    // D-A: one family, one worker, `stream_seq` order. `RETURNING` has no order of its own.
    claimed.sort_by(|a, b| {
        (a.key.tenant_id.0, a.key.scope_id, a.stream_seq).cmp(&(
            b.key.tenant_id.0,
            b.key.scope_id,
            b.stream_seq,
        ))
    });
    let families: Vec<&[ClaimedTicket]> = claimed.chunk_by(|a, b| a.key == b.key).collect();
    // ponytail: families run one after another inside a pass; run them concurrently if the
    // projection-lag p95 (ADR-0052 measurements) says a pass is the bottleneck.
    let work = async {
        for family in &families {
            run_family(shared, cfg, family, &mut outcome).await;
        }
    };
    // Review 2026-09-29 P0: every family this pass holds is renewed while ANY of them is being
    // worked, so the tail of a slow batch keeps its leases (before this, only the family in
    // hand was renewed and later families expired, were re-claimed, and burned attempts to
    // `transient_exhausted` without ever being tried). The lease is a liveness signal: it has
    // to outlast `lease_secs / 3` plus one renew round, not the batch.
    let heartbeat = async {
        loop {
            (shared.sleep)(Duration::from_secs_f64(cfg.lease_secs / 3.0)).await;
            for family in &families {
                // A failed renew is not acted on here: the per-ticket renew in `run_family`
                // sees the lost lease and stops that family before any write.
                let _ = stream_repo::renew_family_leases(
                    &shared.pool,
                    &family[0].key,
                    &cfg.lease_owner,
                    cfg.lease_secs,
                )
                .await;
            }
        }
    };
    while_running(work, heartbeat).await;
    Ok(outcome)
}

/// Drives `work` to completion while also polling `side` (a loop that never finishes); `side` is
/// dropped the moment `work` is done. std-only: this crate has no async runtime dependency.
async fn while_running(work: impl Future<Output = ()>, side: impl Future<Output = ()>) {
    let mut work = std::pin::pin!(work);
    let mut side = std::pin::pin!(side);
    let mut side_done = false;
    std::future::poll_fn(|cx| {
        if work.as_mut().poll(cx).is_ready() {
            return Poll::Ready(());
        }
        if !side_done {
            side_done = side.as_mut().poll(cx).is_ready();
        }
        Poll::Pending
    })
    .await
}

/// One claimed family of [`run_claimed_pass`]: per ticket exhaustion → heartbeat → fresh permit →
/// [`process_row`] → fenced settle / retry / release, then one `advance_prefix`.
async fn run_family(
    shared: &SharedProjectionDeps,
    cfg: &PassConfig,
    tickets: &[ClaimedTicket],
    outcome: &mut PassOutcome,
) {
    let Some(first) = tickets.first() else {
        return;
    };
    let key = &first.key;
    let family = StreamFamily::new(
        key.tenant_id,
        key.scope_kind.clone(),
        key.scope_id,
        key.domain.clone(),
        key.projection_kind.clone(),
    );
    for (index, ticket) in tickets.iter().enumerate() {
        let fence = TicketFence {
            lease_owner: Some(&cfg.lease_owner),
            attempts: ticket.attempts,
        };
        // A worker that dies on this ticket every time never reaches the retry branch below;
        // the claim's increment is what still bounds it.
        if ticket.attempts > cfg.max_attempts {
            tally(
                outcome,
                settle_exhausted(shared, ticket, fence).await,
                RowTerminal::Failed,
            );
            continue;
        }
        let renewed =
            stream_repo::renew_family_leases(&shared.pool, key, &cfg.lease_owner, cfg.lease_secs)
                .await;
        if !renewed.is_ok_and(|seqs| seqs.contains(&ticket.stream_seq)) {
            // The lease is gone (expired, maybe re-claimed): nothing of this family may be
            // written by this worker any more.
            outcome.lost_lease += (tickets.len() - index) as u64;
            break;
        }
        process_and_write(shared, cfg, &family, ticket, fence, outcome).await;
    }
    // One watermark move per family (j). A failure here is logged: the next pass over this
    // family (or the next write) moves it, and the settled rows above stay settled.
    if let Err(error) = stream_repo::advance_prefix(&shared.pool, key, shared.processor_id).await {
        eprintln!(
            "projection_worker: advance_prefix failed tenant={} scope={}: {error}",
            key.tenant_id.0, key.scope_id
        );
    }
}

/// One leased, heartbeated ticket of [`run_family`]: a fresh permit, [`process_row`], then the
/// fenced write its disposition calls for, counted into `outcome` ([`tally`]).
async fn process_and_write(
    shared: &SharedProjectionDeps,
    cfg: &PassConfig,
    family: &StreamFamily,
    ticket: &ClaimedTicket,
    fence: TicketFence<'_>,
    outcome: &mut PassOutcome,
) {
    let key = &ticket.key;
    // A permit the cell registry refused is a Qdrant-side outage of this ticket like any other.
    let (terminal, class) = match (shared.mint_permit)() {
        Some(permit) => {
            let ctx = RowCtx {
                pool: &shared.pool,
                embedder: shared.embedder.as_ref(),
                scanner: shared.scanner.as_ref(),
                transport: shared.transport.as_ref(),
                permit: &permit,
                placement: &ticket.placement,
                family,
                embedding_version: &shared.embedding_version,
                projection_version: &key.projection_version,
                dimension: shared.dimension,
            };
            process_row(&ctx, ticket.stream_seq, ticket.commit_seq).await
        }
        None => (RowTerminal::Retry, "qdrant_upsert_failed"),
    };
    if terminal == RowTerminal::Done {
        // Every dependency answered: the next outage failure is charged again.
        shared.dependency_down.store(false, Ordering::Relaxed);
    }
    // Review 2026-09-29 P1: an outage failure while the dependency is already known down is not
    // evidence against this ticket, so it spends no attempt and cannot exhaust — a Qdrant or
    // provider outage (or a quota/budget window) longer than the backoff series no longer FAILs
    // every ticket written during it. The first outage failure after a DONE is charged like any
    // transient, so a ticket that fails alone while others succeed is still bounded
    // (ADR-0048); the backoff still spaces the uncharged retries.
    let refund = terminal == RowTerminal::Retry
        && is_dependency_outage(class)
        && shared.dependency_down.swap(true, Ordering::Relaxed);
    let written = match terminal {
        RowTerminal::Retry if !refund && ticket.attempts >= cfg.max_attempts => {
            eprintln!(
                "projection_worker: stream_seq={} transient_exhausted last_class={class}",
                ticket.stream_seq
            );
            tally(
                outcome,
                settle_exhausted(shared, ticket, fence).await,
                RowTerminal::Failed,
            );
            return;
        }
        RowTerminal::Retry => stream_repo::release_for_retry(
            &shared.pool,
            key,
            ticket.stream_seq,
            fence,
            class,
            Some(cfg.backoff),
            !refund,
        )
        .await
        .ok(),
        RowTerminal::Pending => {
            stream_repo::release_pending(&shared.pool, key, ticket.stream_seq, fence)
                .await
                .ok()
        }
        _ => settle_row(
            &shared.pool,
            key,
            ticket.stream_seq,
            ticket.commit_seq,
            terminal,
            class,
            fence,
        )
        .await
        .ok(),
    };
    if refund && written == Some(true) {
        outcome.refunded += 1;
    }
    tally(outcome, written, terminal);
}

/// Settles `ticket` `FAILED` `transient_exhausted` (ADR-0052 D-E); `Some(false)` = lease lost.
async fn settle_exhausted(
    shared: &SharedProjectionDeps,
    ticket: &ClaimedTicket,
    fence: TicketFence<'_>,
) -> Option<bool> {
    settle_row(
        &shared.pool,
        &ticket.key,
        ticket.stream_seq,
        ticket.commit_seq,
        RowTerminal::Failed,
        "transient_exhausted",
        fence,
    )
    .await
    .ok()
}

/// Counts one ticket's written outcome: `Some(true)` under its disposition, `Some(false)` (the
/// fence matched nothing) as a lost lease, `None` (the write itself failed; the lease expiry
/// returns the ticket) as a lost lease too — this worker no longer owns what happens to it.
fn tally(outcome: &mut PassOutcome, written: Option<bool>, terminal: RowTerminal) {
    if written != Some(true) {
        outcome.lost_lease += 1;
        return;
    }
    match terminal {
        RowTerminal::Done => outcome.done += 1,
        RowTerminal::SkippedByPolicy => outcome.skipped += 1,
        RowTerminal::Failed => outcome.failed += 1,
        RowTerminal::Retry => outcome.retried += 1,
        RowTerminal::Pending => outcome.pending += 1,
    }
}

#[cfg(test)]
mod classification_tests {
    use super::{RowTerminal, code_is_transient, pg_is_transient, terminal_for_missing_memory};
    use humaux_domain::error::ErrorCode;

    /// ADR-0052 D-E, the `ErrorCode` (embedding + secret scanner) and PG halves of the table (the
    /// Qdrant half is pinned in `qdrant.rs`). `Forbidden` is the scanner's gitleaks finding: the
    /// 2026-09-29 rehearsal retried one to `transient_exhausted` before this row existed. Fault
    /// injection: classify `ProviderPermanent` as transient ⇒ red, and a provider 4xx would retry
    /// to `transient_exhausted` instead of failing `embedding_rejected`.
    #[test]
    fn embedding_and_pg_errors_classify_per_the_adr_0052_table() {
        for transient in [
            ErrorCode::ProviderRateLimited,
            ErrorCode::ProviderTransient,
            ErrorCode::DependencyUnavailable,
            ErrorCode::RateLimited,
            ErrorCode::QuotaExhausted,
            ErrorCode::CostBudgetExceeded,
            ErrorCode::WaitingKey,
            ErrorCode::Internal,
        ] {
            assert!(code_is_transient(transient), "{transient:?}");
        }
        for permanent in [
            ErrorCode::ProviderPermanent,
            ErrorCode::InvalidInput,
            ErrorCode::Forbidden,
            ErrorCode::Unauthorized,
            ErrorCode::EntitlementRequired,
            ErrorCode::TenantBoundary,
        ] {
            assert!(!code_is_transient(permanent), "{permanent:?}");
        }
        assert!(pg_is_transient(&sqlx::Error::PoolTimedOut));
        assert!(pg_is_transient(&sqlx::Error::PoolClosed));
        assert!(!pg_is_transient(&sqlx::Error::RowNotFound));
        // 0167's retirable classes stay permanent; an open distill is not a failure at all.
        assert_eq!(
            terminal_for_missing_memory(Some("FAILED")).0,
            RowTerminal::Failed
        );
        assert_eq!(
            terminal_for_missing_memory(Some("PENDING")).0,
            RowTerminal::Pending
        );
    }
}
