//! `gateway::recall` — Native authenticated semantic recall wiring.
//! Depends-on: crates=[humaux-adapters, humaux-application, humaux-domain, humaux-infra-cell,
//!   humaux-projection, humaux-protocol, humaux-retrieval, serde_json, time, uuid]; services=[]; env=[]; modules=[adapters::affect_repo, adapters::placement_repo, adapters::postgres, adapters::qdrant, adapters::read_materialize, adapters::retrieve, adapters::serving_repo, application::affect, application::retrieval_embedding_port, application::retrieve, domain::affect, domain::error, domain::identity, domain::ids, domain::subject, gateway::context, infra-cell::permit, infra-cell::resource, infra-cell::transport, projection::stream, protocol::mcp, protocol::mcp_catalog, retrieval::completeness, retrieval::envelope, retrieval::planner, retrieval::request]
//! Called-by: [gateway::bootstrap, gateway::context, gateway::mcp_application, tests]
//! Invariants: [this module owns no alternate search or body fallback path; a Qdrant or provider failure surfaces as the typed retrieval error, never a degraded silent result; Qdrant is asked for cand_k for this request's workspace stream only; memory items are cut to top_k after the PG gate, the mood rerank permutes only that visible set (ADR-0030 D-D), then limit shortens it; every planner class is answered by dense (LANE_SUBSTITUTED unless the caller named a mode), only an explicit undelivered mode refuses; stage_ms partitions search() exhaustively and is reported only when every lap followed LAP_SEQUENCE]
//! Spec: Baseline §17.3; §23.3; §33; §55.1; §78.1; ADR-0029; ADR-0031; ADR-0055; ADR-0056
//!
//! This module composes the existing request builder, the retrieval-worker query embedding
//! (which alone seals the query, ADR-0056 D-C), same-Cell Qdrant candidate lookup, and the sole
//! PostgreSQL final hydration boundary. It owns no alternate search or body fallback.

use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use humaux_adapters::{
    affect_repo,
    placement_repo::tenant_placement,
    postgres::RuntimeDbPool,
    qdrant::{
        DenseQuery, DenseQueryVersions, QdrantOperation, RetrievalFamily, ha_profile_for,
        query_dense,
    },
    read_materialize::MaterializedItem,
    retrieve::{
        IndexFace, MaterializedPrivateReadServing, materialize_private_read_serving_about,
        tombstoned_source_seqs, visible_index_count,
    },
    serving_repo::family_read_state,
};
use humaux_application::affect::rerank_by_mood;
use humaux_application::{
    retrieval_embedding_port::{
        RetrievalEmbeddingInput, RetrievalEmbeddingOutcome, RetrievalEmbeddingPort,
    },
    retrieve::{RetrievalIntent, prepare_request},
};
use humaux_domain::{
    affect::{AffectFilter, MoodPoint},
    error::ErrorCode,
    identity::AuthorizationScope,
    ids::WorkspaceId,
    subject::SubjectId,
};
use humaux_infra_cell::{
    CellAccessPermit, IntraCellHttpTransport, IntraCellResource, IntraCellResourceRegistry,
    authorize_cell_access,
};
use humaux_projection::stream::StreamKey;
use humaux_protocol::{
    mcp::{ToolName, ToolOutput},
    mcp_catalog::CanonicalCatalog,
};
use humaux_retrieval::{
    completeness::{CensusResult, FreshnessClass, LedgerClosure},
    envelope::{
        CompletenessBlock, CompletenessInputs, Envelope, FreshnessBlock, LaneStatus,
        MandatoryReport, PendingEnvelope, PinnedReport, PipelineBlock, ProfileBlock,
        ProvenanceBlock, ProvenanceValue, build_projection_block, dense_lane_outcome_block,
        dense_lane_substitution, no_serving_projection_envelope,
    },
    planner::QueryClass,
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::context::ContextBootstrap;

/// Bootstrap-owned components for the one native private-memory dense lane.
///
/// Holds no [`TenantPlacementRow`](humaux_adapters::qdrant::TenantPlacementRow) — an earlier
/// shape baked in exactly one tenant's placement at construction time, which made every other
/// tenant's `recall.search` call assert against it and fail closed. [`search`] resolves a
/// placement per request instead, through [`tenant_placement`], so this runtime carries no
/// single-tenant assumption at all (ADR-0012 gateway wiring card).
///
/// Holds `port: Arc<dyn RetrievalEmbeddingPort>`, not a provider trait object — §4.2:
/// the gateway process must never hold a provider descriptor/credential; the real
/// implementation (`crate::retrieval_embedding_client::GatewayRetrievalEmbeddingClient`) RPCs
/// `humaux-retrieval-worker`, which alone calls the real provider.
pub struct SemanticRecallRuntime {
    port: Arc<dyn RetrievalEmbeddingPort>,
    qdrant: Arc<dyn IntraCellHttpTransport>,
    cell_registry: IntraCellResourceRegistry,
    embedding_version: String,
    dimension: u32,
    /// Bounds [`RetrievalEmbeddingInput::deadline_unix_ms`] — the gateway's own configured
    /// handler timeout (`bins/gateway/src/guard.rs::GuardSettings::handler_timeout`), not a
    /// literal (§78.1).
    handler_timeout: Duration,
}

/// `ContextBootstrap` derives `Debug` and now carries an optional handle to this runtime, so
/// the runtime needs one. Deliberately opaque: every field here is either a credential-adjacent
/// handle (embedding port, Qdrant transport, Cell registry) or a version string already
/// reported in `provenance`. Formatting them would put transport/permit detail into any operator
/// line that `{:?}`s a bootstrap.
impl std::fmt::Debug for SemanticRecallRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SemanticRecallRuntime")
    }
}

pub struct SemanticRecallVersions {
    pub embedding_version: String,
    pub dimension: u32,
}

impl SemanticRecallRuntime {
    pub fn new(
        port: Arc<dyn RetrievalEmbeddingPort>,
        qdrant: Arc<dyn IntraCellHttpTransport>,
        cell_registry: IntraCellResourceRegistry,
        versions: SemanticRecallVersions,
        handler_timeout: Duration,
    ) -> Result<Self, ErrorCode> {
        let SemanticRecallVersions {
            embedding_version,
            dimension,
        } = versions;
        if embedding_version.trim().is_empty() || dimension == 0 || handler_timeout.is_zero() {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            port,
            qdrant,
            cell_registry,
            embedding_version,
            dimension,
            handler_timeout,
        })
    }

    /// §23.1② live `visible` for the two PG-only read routes (`memory.*`, `context.assemble`),
    /// which hold no Qdrant transport of their own. They reach this through
    /// [`crate::context::ContextBootstrap::visible_index_count`]; `recall.search` skips it and
    /// calls `humaux_adapters::retrieve::visible_index_count` directly, because it has already
    /// resolved the placement and the permit for its own dense query and must not pay for a
    /// second placement round trip. The counting rules themselves live in exactly one place —
    /// that adapter function — not here.
    pub(crate) async fn visible_index_count(
        &self,
        pool: &RuntimeDbPool,
        authorization: &AuthorizationScope,
        key: &StreamKey,
        serving_version: Option<&str>,
        ledger: &LedgerClosure,
    ) -> Option<u64> {
        let placement = tenant_placement(
            pool,
            authorization.tenant_id(),
            RetrievalFamily::PrivateMemoryV1,
        )
        .await
        .ok()??;
        let permit = self.qdrant_permit().ok()?;
        visible_index_count(
            pool,
            authorization,
            IndexFace {
                transport: self.qdrant.as_ref(),
                permit: &permit,
                collection: &placement.collection_name,
            },
            key,
            serving_version,
            ledger,
        )
        .await
    }

    fn qdrant_permit(&self) -> Result<CellAccessPermit, ErrorCode> {
        authorize_cell_access(
            &self.cell_registry,
            IntraCellResource::QDRANT_REST,
            Duration::from_secs(30),
        )
        .map_err(|_| ErrorCode::DependencyUnavailable)
    }

    fn deadline_unix_ms(&self) -> Result<i64, ErrorCode> {
        let deadline = SystemTime::now() + self.handler_timeout;
        i64::try_from(
            deadline
                .duration_since(UNIX_EPOCH)
                .map_err(|_| ErrorCode::Internal)?
                .as_millis(),
        )
        .map_err(|_| ErrorCode::Internal)
    }
}

pub struct RecallSearchRequest {
    pub query: String,
    pub workspace_id: WorkspaceId,
    pub consistency_token: Option<String>,
    pub mode: Option<String>,
    pub completeness_request: Option<String>,
    pub limit: Option<u32>,
    /// §6.1.3 / ADR-0029 D-A: any-of subject narrowing (`recall.search.subject_ids`). Empty =
    /// unscoped. Applied twice by design — as a Qdrant payload prefilter
    /// (`DenseQuery::with_subject_ids`) and re-checked at the PG hydrate gate
    /// (`materialize_private_read_serving_about`); the prefilter is never the authority.
    pub subject_ids: Vec<SubjectId>,
    /// §8.5.1 / ADR-0030 D-D: the explicit affect query (`recall.search.affect`). Same double
    /// application as `subject_ids`: Qdrant structured-payload prefilter (flat affect arrays,
    /// ANDed inside the same filter) + PG hydrate re-check per annotation on the read-time
    /// effective intensity. `None` = unscoped.
    pub affect: Option<AffectFilter>,
    /// §8.5.1 / ADR-0030 D-D: optional mood-congruent late rerank — a bounded permutation of the
    /// already-visible set (never widens it), applied after the hydrate gate.
    pub mood_congruence: Option<MoodPoint>,
}

/// ADR-0055 D-E: the fixed, exhaustive partition of [`search`] reported as
/// `provenance.stage_ms` — every `.await` belongs to exactly one stage, so the stage sum accounts
/// for the request's own wall time (RQ-5 attribution).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// narrow + placement + `family_read_state`.
    Route,
    /// intent + `prepare_request` + the `limit` check.
    Planner,
    /// `trusted_query()` only: since ADR-0056 D-C the gateway does not scan (the worker's one
    /// seal is inside `embed`), so this reads about 0 ms.
    // ponytail: key kept at ~0 ms so xtask soak.rs RECALL_STAGES still parses every recall; drop
    // it together with that list when a card owns xtask.
    Scan,
    /// the `embed_query` RPC (worker scan + provider + ledger).
    Embed,
    /// tombstoned-seq read + `query_dense`.
    Qdrant,
    /// `materialize_private_read_serving_about` (the PG gate).
    Hydrate,
    /// truncation to `top_k` + `mood_rerank` + truncation to `limit`.
    Rerank,
    /// `visible_index_count` + envelope build.
    Assemble,
}

const STAGE_NAMES: [&str; 8] = [
    "route", "planner", "scan", "embed", "qdrant", "hydrate", "rerank", "assemble",
];

/// The one lap order [`search`] takes on its success path (route is entered twice: before and
/// after the planner). A lap is attributed "time since the previous lap", so a dropped or moved
/// lap would silently fold its stage into the next one and keep the sum whole; [`StageClock`]
/// therefore checks every lap against this sequence.
const LAP_SEQUENCE: [Stage; 9] = [
    Stage::Route,
    Stage::Planner,
    Stage::Route,
    Stage::Scan,
    Stage::Embed,
    Stage::Qdrant,
    Stage::Hydrate,
    Stage::Rerank,
    Stage::Assemble,
];

/// Monotonic lap clock over [`Stage`]; a stage entered twice (route) accumulates.
struct StageClock {
    start: Instant,
    last: Instant,
    ms: [f64; 8],
    /// Laps taken so far, or `None` once a lap broke [`LAP_SEQUENCE`].
    laps: Option<usize>,
}

impl StageClock {
    fn start() -> Self {
        let now = Instant::now();
        Self {
            start: now,
            last: now,
            ms: [0.0; 8],
            laps: Some(0),
        }
    }

    fn lap(&mut self, stage: Stage) {
        let now = Instant::now();
        self.ms[stage as usize] += now.duration_since(self.last).as_secs_f64() * 1_000.0;
        self.last = now;
        self.laps = self
            .laps
            .filter(|&n| LAP_SEQUENCE.get(n) == Some(&stage))
            .map(|n| n + 1);
    }

    /// `{route, …, assemble, total}` in ms at 0.1 ms resolution; `total` = entry → now.
    /// A clock whose laps did not follow [`LAP_SEQUENCE`] exactly reports `total` only: its
    /// per-stage split would misattribute time, and a missing key is what the stage tests and
    /// the soak's `recall.stage.*` sampler refuse (no fake zeros).
    fn to_json(&self) -> Value {
        let round = |ms: f64| (ms * 10.0).round() / 10.0;
        let mut map = serde_json::Map::with_capacity(9);
        if self.laps == Some(LAP_SEQUENCE.len()) {
            for (name, ms) in STAGE_NAMES.iter().zip(self.ms) {
                map.insert((*name).to_owned(), json!(round(ms)));
            }
        } else {
            eprintln!(
                "humaux-gateway: recall stage_clock_out_of_sequence laps={:?}",
                self.laps
            );
        }
        map.insert(
            "total".to_owned(),
            json!(round(self.start.elapsed().as_secs_f64() * 1_000.0)),
        );
        Value::Object(map)
    }
}

#[allow(clippy::too_many_lines)] // Keep the one native semantic-recall request/response chain together.
pub async fn search(
    pool: Arc<RuntimeDbPool>,
    runtime: Arc<SemanticRecallRuntime>,
    catalog: Arc<CanonicalCatalog>,
    authorization: AuthorizationScope,
    request_id: Uuid,
    bootstrap: ContextBootstrap,
    input: RecallSearchRequest,
) -> Result<PendingEnvelope<ToolOutput>, ErrorCode> {
    let mut clock = StageClock::start();
    if input.mode.as_deref().is_some_and(|mode| mode != "semantic")
        || input.completeness_request.as_deref() == Some("required")
    {
        return Err(ErrorCode::DependencyUnavailable);
    }
    // Membership narrowing (`Forbidden` outside the credential's workspaces) already happened
    // in the guard's `credential.authorize` on the wire path; this `narrow` is defense in depth
    // for in-process callers. The stream family below is derived per request (ADR-0031 D-A);
    // an unprovisioned pair fails closed at the placement / serving-projection reads that
    // follow — the same serving read the PG read routes run in `read_scope` / `assemble`.
    let authorization = authorization.narrow(input.workspace_id)?;
    // §17.3 per-tenant placement, resolved fresh every request — `None` means "not indexed
    // yet for this tenant", never a fallback onto another tenant's collection.
    let placement = tenant_placement(
        &pool,
        authorization.tenant_id(),
        RetrievalFamily::PrivateMemoryV1,
    )
    .await
    .map_err(|_| {
        eprintln!("humaux-gateway: recall request_id={request_id} placement_lookup_failed");
        ErrorCode::DependencyUnavailable
    })?
    .ok_or_else(|| {
        eprintln!("humaux-gateway: recall request_id={request_id} placement_missing");
        ErrorCode::DependencyUnavailable
    })?;
    let (family, key) = bootstrap.request_stream(authorization.tenant_id(), input.workspace_id);
    clock.lap(Stage::Route);
    let intent = RetrievalIntent::new(input.query, Vec::new(), BTreeSet::new(), BTreeSet::new())
        .map_err(|_| ErrorCode::InvalidInput)?;
    let retrieval = prepare_request(intent, &bootstrap.profile).map_err(|_| ErrorCode::Internal)?;
    // ADR-0055 D-C (amends §33 / ADR-0044 D-A): the planner decision is recorded
    // (`provenance.planner_class`), never a refusal — while dense is the only delivered lane it
    // answers every class; `accepted_output` adds `LANE_SUBSTITUTED` when no `mode` was sent.
    // Only an explicit undelivered `mode` refuses (DEPENDENCY_UNAVAILABLE, above).
    //
    // §55.1: candidate depth (`cand_k`) and `top_k` come only from the registered profile.
    // ADR-0055 D-D (amends ADR-0044 D-B): a `limit` in 1..=top_k only shortens the returned
    // list (§33.5's bounded output); above `top_k` stays INVALID_INPUT. Refusing it *silently*
    // is what card 16's soak paid for — its replay sent `limit = <live point count>` and the
    // refusal was misread for a day as an embedding fault — so the operator line carries both
    // numbers, never the query.
    if let Some(limit) = input.limit
        && (limit == 0 || limit > retrieval.top_k())
    {
        eprintln!(
            "humaux-gateway: recall request_id={request_id} limit_not_profile_top_k \
             limit={limit} profile_top_k={}",
            retrieval.top_k()
        );
        return Err(ErrorCode::InvalidInput);
    }
    clock.lap(Stage::Planner);
    // ADR-0053 D-E: the serving read comes BEFORE the query embedding — an unactivated family
    // cannot use a vector, so it pays no provider egress and no provider budget. Uninitialised
    // key ⇒ DEPENDENCY_UNAVAILABLE (unchanged); initialised but unserved ⇒ the B-shaped read.
    let state = family_read_state(&pool, &authorization, &key)
        .await
        .map_err(|_| {
            eprintln!("humaux-gateway: recall request_id={request_id} projection_selector_failed");
            ErrorCode::DependencyUnavailable
        })?;
    if !state.initialized {
        eprintln!("humaux-gateway: recall request_id={request_id} stream_uninitialized");
        return Err(ErrorCode::DependencyUnavailable);
    }
    let projection_version = match (state.serving, state.unserved) {
        (Some(version), _) => version,
        (None, Some((ledger, pipeline))) => {
            eprintln!("humaux-gateway: recall request_id={request_id} no_serving_projection");
            let (evidence, knowledge) = pipeline.blocks();
            return no_serving_projection_envelope(
                &retrieval,
                &bootstrap.binary_build,
                vec!["dense".to_owned()],
                &ledger,
                evidence,
                knowledge,
                |envelope: Envelope<Value>| {
                    let value = serde_json::to_value(envelope).map_err(|_| ErrorCode::Internal)?;
                    catalog.validate_output(ToolName::Recall, &value)?;
                    let text = serde_json::to_string(&value).map_err(|_| ErrorCode::Internal)?;
                    Ok(ToolOutput {
                        text,
                        structured_content: value,
                    })
                },
            );
        }
        (None, None) => {
            eprintln!("humaux-gateway: recall request_id={request_id} no_serving_projection");
            return Err(ErrorCode::DependencyUnavailable);
        }
    };
    clock.lap(Stage::Route);
    // ADR-0056 D-C / ADR-0012 §2: raw query text crosses the same-entity UDS; the worker's
    // `seal_query` is the one egress seal, and its gitleaks verdict comes back as SCAN_REJECTED.
    let trusted_query = retrieval.trusted_query().ok_or(ErrorCode::Internal)?;
    clock.lap(Stage::Scan);
    let embedding_input = RetrievalEmbeddingInput {
        authorization: &authorization,
        workspace_id: input.workspace_id,
        request_id,
        logical_call_id: request_id,
        attempt_no: 1,
        profile_fingerprint: retrieval.profile_fingerprint_identity().as_str(),
        dimension: runtime.dimension,
        query: trusted_query.text(),
        deadline_unix_ms: runtime.deadline_unix_ms()?,
    };
    let (vector, embedding_model_id) = match runtime.port.embed_query(embedding_input).await? {
        RetrievalEmbeddingOutcome::Embedded {
            vector,
            dimension,
            model_id,
            ..
        } if dimension == runtime.dimension && vector.len() == runtime.dimension as usize => {
            (vector, model_id)
        }
        // Three different failures, three different operator lines. One collapsed
        // `query_embedding_unusable` covered all of them and cost card 16's soak a day of
        // diagnosis: "the provider declined" (Skipped), "the worker could not be reached or
        // failed" (Unavailable) and "the vector came back the wrong shape" (dimension
        // mismatch) have disjoint fixes, and a log line that cannot tell them apart forces
        // the reader to guess which one they are looking at.
        //
        // A wrong-dimension vector is never truncated/padded to fit — ADR-0012 gateway wiring
        // card: "mismatch ⇒ DependencyUnavailable, never truncate". Only the two lengths are
        // logged, never an element of the vector.
        RetrievalEmbeddingOutcome::Embedded {
            vector, dimension, ..
        } => {
            eprintln!(
                "humaux-gateway: recall request_id={request_id} query_embedding_dimension_mismatch \
                 reported={dimension} vector_len={} expected={}",
                vector.len(),
                runtime.dimension
            );
            return Err(ErrorCode::DependencyUnavailable);
        }
        RetrievalEmbeddingOutcome::Skipped => {
            eprintln!("humaux-gateway: recall request_id={request_id} query_embedding_skipped");
            return Err(ErrorCode::DependencyUnavailable);
        }
        // §7.5 C / §52: a gitleaks finding on the query is the caller's FORBIDDEN, as it was
        // when the gateway sealed it itself (ADR-0056 D-C); only the request id is logged.
        RetrievalEmbeddingOutcome::Unavailable { reason } if reason == "SCAN_REJECTED" => {
            eprintln!("humaux-gateway: recall request_id={request_id} query_scan_rejected");
            return Err(ErrorCode::Forbidden);
        }
        // `reason` is the worker's closed failure code (or this client's own transport class),
        // never provider text and never any part of the query — ADR-0014 operator-signal rule.
        RetrievalEmbeddingOutcome::Unavailable { reason } => {
            eprintln!(
                "humaux-gateway: recall request_id={request_id} query_embedding_unavailable \
                 reason={reason}"
            );
            return Err(ErrorCode::DependencyUnavailable);
        }
    };
    clock.lap(Stage::Embed);
    // ADR-0055 D-B / §37: TOMBSTONED seqs of the serving stream reach the Qdrant overlay, so a
    // tombstoned point cannot occupy a candidate slot (the PG gate re-checks by Evidence). The
    // seqs are this workspace stream's, so the candidate set is scoped to it as well.
    let serving_key = family.with_version(projection_version.clone());
    let tombstoned_seqs = tombstoned_source_seqs(&pool, &authorization, &serving_key)
        .await
        .map_err(|_| {
            eprintln!("humaux-gateway: recall request_id={request_id} tombstone_read_failed");
            ErrorCode::DependencyUnavailable
        })?;
    // §23.3 / ADR-0055 D-A: ask Qdrant for `cand_k`, not `top_k` — the PG gate below drops
    // archived/superseded/secret/tombstoned rows, and truncation to the returned depth happens
    // only after it and the mood rerank.
    let dense = DenseQuery::new(
        &authorization,
        &placement,
        DenseQueryVersions {
            projection: &projection_version,
            embedding: &runtime.embedding_version,
        },
        vector,
        retrieval.cand_k(),
        Vec::new(),
        ha_profile_for(QdrantOperation::ReadYourWriteStrict),
    )
    .map_err(|_| {
        eprintln!("humaux-gateway: recall request_id={request_id} dense_query_build_failed");
        ErrorCode::DependencyUnavailable
    })?
    .with_subject_ids(&input.subject_ids)
    .with_affect_filter(input.affect.as_ref())
    .in_workspace_stream(input.workspace_id, tombstoned_seqs);
    let candidates = query_dense(runtime.qdrant.as_ref(), &runtime.qdrant_permit()?, &dense)
        .await
        .map_err(|error| {
            // ADR-0014: a `WriteDenied` here means the registered `QDRANT_REST` entry itself is
            // misconfigured (a read-only permit rejected a request `query_dense` never should
            // have shaped as a write) — a Forbidden, not a transient dependency failure a
            // caller could usefully retry.
            if matches!(
                error,
                humaux_adapters::qdrant::QdrantTransportError::Transport(
                    humaux_infra_cell::IntraCellError::WriteDenied
                )
            ) {
                ErrorCode::Forbidden
            } else {
                // Operator signal: transport error class only (no body/URL), §ADR-0014.
                eprintln!("humaux-gateway: recall request_id={request_id} qdrant_query_failed");
                ErrorCode::DependencyUnavailable
            }
        })?;
    clock.lap(Stage::Qdrant);
    let mut materialized = materialize_private_read_serving_about(
        &pool,
        input.consistency_token.as_deref(),
        &authorization,
        &family,
        &projection_version,
        &runtime.embedding_version,
        &candidates,
        &input.subject_ids,
        input.affect.as_ref(),
    )
    .await
    .map_err(|error| match error {
        humaux_adapters::retrieve::RetrieveError::CrossTenant
        | humaux_adapters::retrieve::RetrieveError::CrossWorkspace
        | humaux_adapters::retrieve::RetrieveError::UntrustedStreamFamily => ErrorCode::Forbidden,
        // §15.5 token refusals are caller-fault, but they must not be *silent* caller-fault:
        // an expired token and a malformed one produce the same `INVALID_INPUT` on the wire,
        // and card 16's post-drain replay spent a run being refused for a TTL it had outlived
        // with nothing in the log to say so. The reason class only — never the token.
        error @ (humaux_adapters::retrieve::RetrieveError::TokenMalformed(_)
        | humaux_adapters::retrieve::RetrieveError::TokenExpired
        | humaux_adapters::retrieve::RetrieveError::TokenNotIssued) => {
            let reason = match error {
                humaux_adapters::retrieve::RetrieveError::TokenMalformed(_) => "token_malformed",
                humaux_adapters::retrieve::RetrieveError::TokenExpired => "token_expired",
                _ => "token_not_issued",
            };
            eprintln!(
                "humaux-gateway: recall request_id={request_id} consistency_token_refused \
                 reason={reason}"
            );
            ErrorCode::InvalidInput
        }
        error => {
            // Operator signal, same discipline as the qdrant_query_failed line above: the
            // RetrieveError class only — `Db` carries driver text and is reduced to its name.
            let class = if matches!(error, humaux_adapters::retrieve::RetrieveError::Db(_)) {
                "db_error".to_owned()
            } else {
                error.to_string()
            };
            eprintln!(
                "humaux-gateway: recall request_id={request_id} materialize_failed class={class}"
            );
            ErrorCode::DependencyUnavailable
        }
    })?;
    clock.lap(Stage::Hydrate);
    // ADR-0030 D-D (kept by ADR-0055 D-A): the mood rerank is a permutation of the visible set
    // — the first profile `top_k` survivors in dense order — never a selection over all
    // `cand_k` survivors, where a congruent deep candidate would evict a relevant top hit
    // (no-affect memories score the 5000 midpoint). `limit` then shortens the reranked list.
    truncate_memory_items(&mut materialized.bodies.items, retrieval.top_k());
    let reranked = match input.mood_congruence {
        None => 0,
        Some(mood) => mood_rerank(&pool, &authorization, &mut materialized.bodies.items, mood)
            .await
            .map_err(|_| {
                eprintln!("humaux-gateway: recall request_id={request_id} mood_rerank_failed");
                ErrorCode::DependencyUnavailable
            })?,
    };
    truncate_memory_items(
        &mut materialized.bodies.items,
        input.limit.unwrap_or_else(|| retrieval.top_k()),
    );
    clock.lap(Stage::Rerank);
    // §23.1②: the live index count, taken against the SERVING version this request actually
    // read (`family_read_state` above), never the token's. `None` here is not a
    // failure to handle — it is the honest "cannot establish" input `build_projection_block`
    // already knows how to report.
    let visible = visible_index_count(
        &pool,
        &authorization,
        IndexFace {
            transport: runtime.qdrant.as_ref(),
            permit: &runtime.qdrant_permit()?,
            collection: &placement.collection_name,
        },
        &family.with_version(projection_version.clone()),
        Some(projection_version.as_str()),
        &materialized.ledger,
    )
    .await;
    if visible.is_none() {
        eprintln!("humaux-gateway: recall request_id={request_id} visible_index_count_unavailable");
    }
    accepted_output(
        materialized,
        &retrieval,
        &bootstrap,
        &runtime,
        &projection_version,
        &embedding_model_id,
        candidates.len(),
        reranked,
        &catalog,
        visible,
        input.mode.is_some(),
        clock,
    )
}

/// ADR-0055 D-A: keeps the first `keep` memory items (current order) and every overlay item
/// — a §15.5 read-your-writes item is bounded by its token's range, and dropping one would break
/// RYW.
fn truncate_memory_items(items: &mut Vec<MaterializedItem>, keep: u32) {
    let mut memories = 0_u32;
    items.retain(|item| match item {
        MaterializedItem::Memory { .. } => {
            memories += 1;
            memories <= keep
        }
        _ => true,
    });
}

/// ADR-0030 D-D late rerank: reorders the visible Memory items by mood congruence
/// (`application::affect::rerank_by_mood`, stable) using the rows' affects read under the
/// caller's RLS; overlay items keep their place after the memories. Returns how many memory
/// items were reranked (the envelope's `reranked_count`). Never adds or drops an item.
async fn mood_rerank(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    items: &mut [MaterializedItem],
    mood: MoodPoint,
) -> Result<u32, ErrorCode> {
    let memory_ids: Vec<uuid::Uuid> = items
        .iter()
        .filter_map(|item| match item {
            MaterializedItem::Memory { memory_id, .. } => Some(*memory_id),
            _ => None,
        })
        .collect();
    let rows = affect_repo::affects_for_memories(pool, authorization, &memory_ids).await?;
    let observed = affect_repo::observed(&rows, time::OffsetDateTime::now_utc());
    let order = rerank_by_mood(memory_ids, mood, &observed);
    let rank = |item: &MaterializedItem| match item {
        MaterializedItem::Memory { memory_id, .. } => order
            .iter()
            .position(|id| id == memory_id)
            .unwrap_or(usize::MAX),
        _ => usize::MAX,
    };
    items.sort_by_key(rank);
    u32::try_from(order.len()).map_err(|_| ErrorCode::Internal)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)] // One envelope-assembly step over the request's own fixed field set.
fn accepted_output(
    materialized: MaterializedPrivateReadServing,
    request: &humaux_retrieval::request::RetrievalRequest,
    bootstrap: &ContextBootstrap,
    runtime: &SemanticRecallRuntime,
    projection_version: &str,
    embedding_model_id: &str,
    candidate_count: usize,
    reranked_count: u32,
    catalog: &CanonicalCatalog,
    visible: Option<u64>,
    explicit_mode: bool,
    mut clock: StageClock,
) -> Result<PendingEnvelope<ToolOutput>, ErrorCode> {
    let items = render_items(materialized.bodies.items);
    let returned = u32::try_from(items.len()).map_err(|_| ErrorCode::Internal)?;
    let candidate_count = u32::try_from(candidate_count).map_err(|_| ErrorCode::Internal)?;
    let projection =
        build_projection_block(&materialized.ledger, visible, bootstrap.projection_lag);
    // §23.3④ (ADR-0041 D-H): the request's own six-column `StreamKey` ledger, counted in the
    // same RR snapshot that closed the ledger and hydrated the bodies. `classify()` maps this
    // route's `PlannerDecision::Class(_)` to `SemanticBounded`, so — unlike `memory.enumerate`
    // — no census travels with these counts and §22.0's exact-without-a-predicate trap is not
    // on this path. What they buy is the removal of `count_unknown`: the one reading that kept
    // a healthy semantic read at `cannot_establish` after card 18 made its ratio real.
    let (evidence, knowledge) = materialized.pipeline.blocks();
    let pipeline = PipelineBlock {
        evidence,
        knowledge,
        projection: projection.value,
    };
    let provenance = ProvenanceBlock {
        binary_build: bootstrap.binary_build.clone(),
        projection_version: ProvenanceValue::Used {
            id: projection_version.to_owned(),
        },
        embedding_model_id: ProvenanceValue::Used {
            id: format!("{embedding_model_id}@{}", runtime.embedding_version),
        },
        rerank_model_id: ProvenanceValue::NotApplicable {},
        card_builder_version: ProvenanceValue::NotApplicable {},
        profile_fingerprint: request.profile_fingerprint_identity().clone(),
        profile: ProfileBlock {
            top_k: request.top_k(),
            cand_k: request.cand_k(),
            cand_k_formula: request.cand_k_formula(),
            lanes: vec!["dense".to_owned()],
        },
    };
    let lane_status = LaneStatus::Ok;
    let census = CensusResult::ok_without_enumeration();
    let planner_class = request
        .planner_decision()
        .wire_class()
        .map_or("CANNOT_ESTABLISH", QueryClass::as_wire_name);
    let substituted = dense_lane_substitution(request.planner_decision(), explicit_mode);
    dense_lane_outcome_block(
        request,
        CompletenessInputs {
            lane_status: &lane_status,
            census: &census,
            ledger: &materialized.ledger,
            pipeline: &pipeline,
            provenance: &provenance,
            visible,
            context: None,
            mandatory_missing: 0,
            lag_threshold: bootstrap.projection_lag,
        },
        |outcome| {
            let value = serde_json::to_value(Envelope {
                items,
                completeness: CompletenessBlock {
                    class: outcome.class,
                    reason: outcome.reason,
                    exact: outcome.exact,
                    known_lower_bound: outcome.known_lower_bound,
                    lanes: std::collections::BTreeMap::from([("dense".to_owned(), lane_status)]),
                    candidate_count,
                    reranked_count,
                    returned,
                    truncated: candidate_count > returned,
                    degradations: projection
                        .degradations
                        .iter()
                        .chain(&substituted)
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
            .map_err(|_| ErrorCode::Internal)?;
            clock.lap(Stage::Assemble);
            // ADR-0055 D-C/D-E: recall-only provenance keys, inserted here rather than on the
            // shared `ProvenanceBlock` (context/memory schemas are additionalProperties:false).
            let mut value = value;
            let provenance = value
                .get_mut("provenance")
                .and_then(Value::as_object_mut)
                .ok_or(ErrorCode::Internal)?;
            provenance.insert("planner_class".to_owned(), json!(planner_class));
            provenance.insert("stage_ms".to_owned(), clock.to_json());
            catalog.validate_output(ToolName::Recall, &value)?;
            let text = serde_json::to_string(&value).map_err(|_| ErrorCode::Internal)?;
            Ok(ToolOutput {
                text,
                structured_content: value,
            })
        },
    )
}

fn render_items(items: Vec<MaterializedItem>) -> Vec<Value> {
    items
        .into_iter()
        .map(|item| match item {
            MaterializedItem::Memory { memory_id, content } => json!({
                "kind": "memory",
                "memory_id": memory_id,
                "content": content,
            }),
            MaterializedItem::TemporaryEvidence {
                evidence_id,
                stream_seq,
                processing_state,
                payload,
                linked_memory_ids,
            } => json!({
                "kind": "temporary_evidence",
                "evidence_id": evidence_id,
                "stream_seq": stream_seq,
                "processing_state": processing_state.as_db_str(),
                "payload": payload,
                "linked_memory_ids": linked_memory_ids,
            }),
            MaterializedItem::ArtifactUnavailable {
                evidence_id,
                stream_seq,
                processing_state,
                linked_memory_ids,
            } => json!({
                "kind": "artifact_unavailable",
                "evidence_id": evidence_id,
                "stream_seq": stream_seq,
                "processing_state": processing_state.as_db_str(),
                "linked_memory_ids": linked_memory_ids,
            }),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clock_after(laps: &[Stage]) -> Value {
        let mut clock = StageClock::start();
        for stage in laps {
            clock.lap(*stage);
        }
        clock.to_json()
    }

    #[test]
    fn stage_clock_reports_every_stage_only_for_the_full_lap_sequence() {
        let full = clock_after(&LAP_SEQUENCE);
        for name in STAGE_NAMES.iter().chain(&["total"]) {
            assert!(full[name].is_f64(), "{name} missing: {full}");
        }
        // A dropped inner lap (hydrate), a dropped first route lap, a dropped final assemble lap
        // and a doubled lap all fold time into a neighbour — each must lose the split.
        let without = |skip: usize| {
            let laps: Vec<Stage> = LAP_SEQUENCE
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != skip)
                .map(|(_, s)| *s)
                .collect();
            clock_after(&laps)
        };
        let mut doubled = LAP_SEQUENCE.to_vec();
        doubled.insert(4, Stage::Embed);
        for (label, broken) in [
            ("no hydrate lap", without(6)),
            ("no first route lap", without(0)),
            ("no assemble lap", without(8)),
            ("doubled embed lap", clock_after(&doubled)),
        ] {
            let object = broken.as_object().expect("stage_ms object");
            assert_eq!(
                object.keys().collect::<Vec<_>>(),
                ["total"],
                "{label}: {broken}"
            );
        }
    }
}
