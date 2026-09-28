//! `retrieval-provider::router` — §19 Provider Route resolution (T7.2).
//! Depends-on: crates=[humaux-domain, uuid]; services=[]; env=[]; modules=[domain::ids, retrieval-provider::contract,
//!   retrieval-provider::failover]
//! Called-by: [tests]
//! Invariants: []
//! Spec: §19; §3; §78.3
//!
//! Pure decision logic only: [`resolve`] takes an already-fetched candidate row set (no SQLx
//! import here — §3/§78.3 "Domain 永不 import HTTP/SQLx/Qdrant/Provider SDK/ENV"; reading
//! `control.retrieval_provider_routes` itself is an adapters-crate concern, out of this
//! module's scope — see `migrations/0088_retrieval_provider_routes.sql`'s header for why no
//! runtime role can write that table and its own index for the lookup shape a future reader
//! issues) and resolves them against §19's literal Router contract:
//!
//! ```text
//! Router 输入: Tenant Policy / Data Class / Region / Purpose / Provider Health / Quota /
//!              Cost Policy / Projection Compatibility
//! Router 输出: Resolved Retrieval Route
//! ```
//!
//! Of those eight inputs, four are modeled as first-class, independently testable behavior
//! here because `control.retrieval_provider_routes`'s own column set carries them:
//!
//! - **Purpose** — [`RouteQuery::purpose`], an exact-match filter (§19: Retrieval Provider
//!   Plane covers only dense embedding + rerank, never a third purpose).
//! - **Region** — [`RouteQuery::region`]; a `NULL` `region` column is region-agnostic and
//!   matches any query, a specific `region` column only matches an identical query region.
//! - **Tenant Policy** — [`RouteQuery::tenant_id`]; same `NULL`-is-wildcard shape as region,
//!   plus specificity-based ranking (a tenant-specific row beats a platform-wide default row
//!   at equal `priority`, migrations/0088's own header comment on why `tenant_id` is nullable).
//! - **Projection Compatibility** — [`ProjectionRef`]/[`RouteRejection::ProjectionIncompatible`],
//!   §19's "Embedding Provider Failover 硬规则": an embedding-purpose candidate whose
//!   `(embedding_provider_id, embedding_model_id)` differs from the tenant's already-committed
//!   projection identity is an Incompatible Model Change (§19: "它是 Projection Migration，
//!   不是 Failover"), not a same-request failover option, and is rejected outright rather than
//!   silently selected. Rerank purposes have no such gate — §19: "Rerank 不形成持久向量空间，
//!   因此可以采用请求级 fallback".
//!
//! The remaining three (Data Class, Provider Health, Quota, Cost Policy — four named, but
//! Data Class folds into the same "is this candidate admissible at all" question Health/
//! Quota/Cost ask) have no concrete subsystem in this codebase yet (separate Phase 7 task
//! cards: Admission Controller, Pricing Registry, Circuit Breaker). Rather than fabricate
//! unused structs for subsystems nothing calls yet (YAGNI), [`resolve`] takes one caller-
//! supplied `admit` closure as the single seam those future checks compose through — see its
//! own doc.

use std::time::SystemTime;

use humaux_domain::ids::TenantId;
use uuid::Uuid;

/// §19 Provider Route purpose — the closed set `migrations/0088_retrieval_provider_routes.sql`
/// enforces via its `purpose` CHECK constraint. Deliberately narrower than
/// `humaux_domain::egress::PrivateDataPurpose`: §19's Retrieval Provider Plane is scoped to
/// "外部 managed neural retrieval (dense embedding + rerank)" only and explicitly "不扩展成
/// 通用聊天 LLM Gateway" — a `UserReasoning`-equivalent third variant has no meaning for a
/// route in this table.
///
/// Named `RoutePurpose`, not `RetrievalPurpose`: this crate's `admission` module (a sibling
/// Phase 7 task, T7.1's own Cargo.toml doc comment references it) independently defines its
/// own same-shape `RetrievalPurpose` for the identical two call kinds. Both types compile fine
/// side by side (different module paths), but they are almost certainly the same concept
/// duplicated — flagged for the integrating change to unify rather than silently left as two
/// parallel enums nothing converts between.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RoutePurpose {
    Embedding,
    Rerank,
}

impl RoutePurpose {
    /// Wire form — matches `migrations/0088_retrieval_provider_routes.sql`'s CHECK and the
    /// same `RETRIEVAL_EMBEDDING`/`RETRIEVAL_RERANK` spelling
    /// `crates/adapters/src/disclosure.rs::purpose_as_db_str` and migration
    /// 0061_reasoning_domain_grants_purposes_closed_set.sql already use for the identical two
    /// call kinds — no third spelling introduced for the same two concepts.
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Embedding => "RETRIEVAL_EMBEDDING",
            Self::Rerank => "RETRIEVAL_RERANK",
        }
    }
}

/// One `control.retrieval_provider_routes` row, already fetched by the caller (adapters
/// crate) — this module never issues SQL. Field set and nullability mirror
/// `migrations/0088_retrieval_provider_routes.sql` column-for-column.
#[derive(Debug, Clone)]
pub struct RouteCandidate {
    pub route_id: Uuid,
    /// `NULL` in the DB = platform-wide default route (migrations/0088 header).
    pub tenant_id: Option<TenantId>,
    /// `NULL` in the DB = region-agnostic.
    pub region: Option<String>,
    pub purpose: RoutePurpose,
    pub embedding_provider_id: Option<String>,
    pub embedding_model_id: Option<String>,
    /// `migrations/0089_retrieval_provider_routes_dimension.sql` — required together with
    /// `embedding_provider_id`/`embedding_model_id` (`None` only on a rerank-only row). A
    /// fixed `(embedding_provider_id, embedding_model_id)` pair can legitimately span
    /// multiple valid dimensions (a Matryoshka model's `dimension_options`,
    /// `contract.rs::EmbeddingModelDescriptor`), so this is a third, independent axis of
    /// [`ProjectionRef`]'s compatibility key, not implied by provider/model alone.
    pub embedding_dimension: Option<u32>,
    pub rerank_provider_id: Option<String>,
    pub rerank_model_id: Option<String>,
    /// Higher value = more preferred (tie-break dial once tenant/region specificity is
    /// equal — see [`resolve`]'s ordering doc).
    pub priority: i32,
    pub enabled: bool,
    pub effective_from: SystemTime,
    /// `None` = still in effect (no upper bound).
    pub effective_to: Option<SystemTime>,
}

/// §19 Router's Purpose/Region/Tenant-Policy/point-in-time inputs — the four the caller
/// supplies directly. Provider Health/Quota/Cost Policy/Data Class arrive via [`resolve`]'s
/// `admit` closure instead; see the module doc for why.
#[derive(Debug, Clone)]
pub struct RouteQuery {
    pub tenant_id: TenantId,
    pub region: Option<String>,
    pub purpose: RoutePurpose,
    pub as_of: SystemTime,
}

/// The tenant's already-committed embedding projection identity — §19 "Embedding Provider
/// Failover 硬规则"'s compatibility anchor. `None` means the tenant has no committed
/// embedding projection yet (nothing to conflict with, any embedding route is compatible).
///
/// §19's "Compatible Failover" list names three independent compatibility criteria — "same
/// provider/model semantics", "same dimension", "same normalization/projection version" —
/// not one. `(provider_id, model_id)` alone is **not** sufficient to prove "same embedding
/// space": `contract.rs::EmbeddingModelDescriptor.dimension_options` shows one
/// `(provider_id, model_id)` pair (e.g. `text-embedding-v4`) legitimately spans multiple
/// valid dimensions (a Matryoshka model), so `dimension` is modeled here as a third,
/// independent field rather than assumed to be implied by the first two
/// (`migrations/0089_retrieval_provider_routes_dimension.sql`). `normalization`/
/// `projection_version` still belong to the fuller `EmbeddingModelDescriptor` identity (§19,
/// a sibling Phase 7 task's `descriptor.rs`, not this task's file) and remain an open gap in
/// this gate — tracked, not silently assumed away.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionRef {
    pub provider_id: String,
    pub model_id: String,
    pub dimension: u32,
}

/// Why one candidate did not resolve — [`resolve`]'s internal filter, exposed as
/// [`reject_reason`] so a test can assert *which* rule excluded a given candidate rather than
/// only the aggregate outcome (needed for this task's "Projection 不兼容的路由被拒" fault-
/// injection: red = router picks the incompatible-but-higher-priority candidate anyway,
/// green = this variant fires and the router falls through to the next candidate instead).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteRejection {
    WrongPurpose,
    TenantMismatch,
    RegionMismatch,
    Disabled,
    NotYetEffective,
    Expired,
    /// §19 Embedding Provider Failover 硬规则.
    ProjectionIncompatible {
        candidate: ProjectionRef,
        required: ProjectionRef,
    },
    /// Caller's `admit` closure rejected the candidate — the seam Provider Health/Quota/Cost
    /// Policy/Data Class checks compose through once those subsystems exist (module doc).
    PolicyRejected(String),
}

/// §19 Router 输出: the winning candidate, reduced to just the fields a caller dispatching an
/// actual provider call needs — `route_id` for audit/ledger correlation, the rest routed by
/// `query.purpose` (only the relevant provider/model pair is populated; migrations/0088's
/// CHECK constraints guarantee that pair is `Some` on any row that reaches here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRoute {
    pub route_id: Uuid,
    pub provider_id: String,
    pub model_id: String,
    pub priority: i32,
}

/// [`resolve`] found no admissible candidate. `attempted` is the number of rows considered
/// (0 distinguishes "no candidates handed in at all" from "candidates existed but every one
/// was rejected" without carrying a full reason list — callers that need the per-candidate
/// breakdown call [`reject_reason`] themselves, same as this module's own tests do).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoRoute {
    pub attempted: usize,
}

/// Half-open effective-window membership, `[effective_from, effective_to)` — same boundary
/// convention `humaux_domain::temporal::visible_at` already uses for Memory rows (`effective_to`
/// itself is excluded, `None` imposes no upper bound), reused here for the identical shape on
/// a Provider Route row rather than inventing a second boundary rule.
fn in_effective_window(
    candidate: &RouteCandidate,
    as_of: SystemTime,
) -> Result<(), RouteRejection> {
    if as_of < candidate.effective_from {
        return Err(RouteRejection::NotYetEffective);
    }
    if let Some(effective_to) = candidate.effective_to
        && as_of >= effective_to
    {
        return Err(RouteRejection::Expired);
    }
    Ok(())
}

/// §19 Embedding Provider Failover 硬规则's own 5-axis identity (`failover::ProjectionContract`)
/// has two axes [`ProjectionRef`] does not carry (`model_revision`, `normalization`,
/// `projection_version` — this struct's own doc: "remain an open gap in this gate, tracked,
/// not silently assumed away"). Filling both sides with the identical fixed placeholder keeps
/// those two axes always equal (never contributing a spurious mismatch) while the three axes
/// this module actually has real data for (`provider_id`/`model_id`/`dimension`) are compared
/// for real through the one shared comparator (`failover::projection_contracts_compatible`) —
/// see [`projection_compatible`]'s own doc for why it cannot instead call the permit-gated
/// `failover::decide_embedding_failover`.
const UNTRACKED_PROJECTION_AXES_PLACEHOLDER: &str = "";

fn to_failover_contract(reference: &ProjectionRef) -> crate::failover::ProjectionContract {
    crate::failover::ProjectionContract {
        provider_id: crate::contract::ProviderId(reference.provider_id.clone()),
        model_id: crate::contract::ModelId(reference.model_id.clone()),
        model_revision: UNTRACKED_PROJECTION_AXES_PLACEHOLDER.to_string(),
        dimension: reference.dimension,
        normalization: crate::failover::Normalization::None,
        projection_version: UNTRACKED_PROJECTION_AXES_PLACEHOLDER.to_string(),
    }
}

/// Compatibility check restricted to `RoutePurpose::Embedding` — always `Ok` for a rerank
/// candidate (module doc: rerank forms no persistent vector space, §19).
///
/// §19 Embedding Provider Failover 硬规则 sole-decision-point gate (`xtask architecture-check`):
/// delegates to [`crate::failover::projection_contracts_compatible`] rather than hand-rolling
/// the `provider_id`/`model_id`/`dimension` comparison inline, so this module can never
/// independently drift from the one real 5-axis rule. Not
/// [`crate::failover::decide_embedding_failover`] itself: that function requires a live
/// [`humaux_domain::egress::EgressPermit`], and `resolve` runs *before* a route (and therefore
/// a `ProcessorId` a permit could be minted for) has been chosen — see
/// `projection_contracts_compatible`'s own doc for the full reasoning.
fn projection_compatible(
    candidate: &RouteCandidate,
    current_projection: Option<&ProjectionRef>,
) -> Result<(), RouteRejection> {
    if candidate.purpose != RoutePurpose::Embedding {
        return Ok(());
    }
    let Some(required) = current_projection else {
        return Ok(());
    };
    // migrations/0088's CHECK constraints guarantee an Embedding-purpose row carries both
    // embedding_provider_id and embedding_model_id, and migrations/0089's CHECK guarantees
    // embedding_dimension is `Some` exactly when embedding_provider_id is — `unwrap_or_default`
    // only protects a caller handing in a row built outside those constraints (e.g. a test
    // fixture), never a real DB row.
    let candidate_ref = ProjectionRef {
        provider_id: candidate.embedding_provider_id.clone().unwrap_or_default(),
        model_id: candidate.embedding_model_id.clone().unwrap_or_default(),
        dimension: candidate.embedding_dimension.unwrap_or_default(),
    };
    use crate::failover::EmbeddingFailoverDecision;
    match crate::failover::projection_contracts_compatible(
        &to_failover_contract(required),
        &to_failover_contract(&candidate_ref),
    ) {
        EmbeddingFailoverDecision::CompatibleFailover => Ok(()),
        EmbeddingFailoverDecision::RequiresProjectionMigration(_) => {
            Err(RouteRejection::ProjectionIncompatible {
                candidate: candidate_ref,
                required: required.clone(),
            })
        }
    }
}

/// Every §19 Router filter for one candidate against one query, in order — the same order
/// [`resolve`] walks so `attempted`/short-circuit behavior matches exactly. `admit` is
/// [`resolve`]'s Health/Quota/Cost Policy/Data Class seam (module doc); it only runs once a
/// candidate has already cleared every structural filter, matching the natural cost order
/// (cheap local checks before a caller-supplied — possibly I/O-backed — admission check).
pub fn reject_reason(
    candidate: &RouteCandidate,
    query: &RouteQuery,
    current_projection: Option<&ProjectionRef>,
    admit: &dyn Fn(&RouteCandidate) -> Result<(), String>,
) -> Option<RouteRejection> {
    if candidate.purpose != query.purpose {
        return Some(RouteRejection::WrongPurpose);
    }
    if let Some(tenant_id) = candidate.tenant_id
        && tenant_id != query.tenant_id
    {
        return Some(RouteRejection::TenantMismatch);
    }
    if let Some(region) = &candidate.region
        && Some(region.as_str()) != query.region.as_deref()
    {
        return Some(RouteRejection::RegionMismatch);
    }
    if !candidate.enabled {
        return Some(RouteRejection::Disabled);
    }
    if let Err(reason) = in_effective_window(candidate, query.as_of) {
        return Some(reason);
    }
    if let Err(reason) = projection_compatible(candidate, current_projection) {
        return Some(reason);
    }
    if let Err(msg) = admit(candidate) {
        return Some(RouteRejection::PolicyRejected(msg));
    }
    None
}

/// Specificity/priority ordering key — more-preferred sorts *greater*. Tenant specificity
/// outranks region specificity outranks the `priority` column outranks `route_id` (final
/// deterministic tie-break, migrations/0088's own doc comment on the `priority` column).
/// A tenant- or region-*specific* row (`Some`) beats a platform-wide/region-agnostic row
/// (`None`) at equal standing on every coarser key — this is the "tenant/region ... 优先级"
/// behavior this task's acceptance test exercises.
fn ordering_key(candidate: &RouteCandidate) -> (bool, bool, i32, Uuid) {
    (
        candidate.tenant_id.is_some(),
        candidate.region.is_some(),
        candidate.priority,
        candidate.route_id,
    )
}

/// §19 Router 主入口: resolves `candidates` against `query`, returning the single best
/// [`ResolvedRoute`] or [`NoRoute`] if none is admissible.
///
/// `admit` is the seam Provider Health / Quota / Cost Policy / Data Class checks compose
/// through — those subsystems don't exist as separate modules yet (module doc), so this
/// parameter is the whole of §19's remaining three Router inputs today. Pass `&|_| Ok(())`
/// where none of them apply yet (every test below does exactly that unless it's specifically
/// exercising this seam).
pub fn resolve(
    candidates: &[RouteCandidate],
    query: &RouteQuery,
    current_projection: Option<&ProjectionRef>,
    admit: &dyn Fn(&RouteCandidate) -> Result<(), String>,
) -> Result<ResolvedRoute, NoRoute> {
    let winner = candidates
        .iter()
        .filter(|c| reject_reason(c, query, current_projection, admit).is_none())
        .max_by_key(|c| ordering_key(c));

    match winner {
        Some(c) => Ok(match query.purpose {
            RoutePurpose::Embedding => ResolvedRoute {
                route_id: c.route_id,
                // migrations/0088's CHECK constraint guarantees these are `Some` on any
                // Embedding-purpose row.
                provider_id: c.embedding_provider_id.clone().unwrap_or_default(),
                model_id: c.embedding_model_id.clone().unwrap_or_default(),
                priority: c.priority,
            },
            RoutePurpose::Rerank => ResolvedRoute {
                route_id: c.route_id,
                provider_id: c.rerank_provider_id.clone().unwrap_or_default(),
                model_id: c.rerank_model_id.clone().unwrap_or_default(),
                priority: c.priority,
            },
        }),
        None => Err(NoRoute {
            attempted: candidates.len(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn tenant(n: u8) -> TenantId {
        // Deterministic per-test tenant id — v7 UUIDs embed a timestamp, but equality/
        // inequality is all these tests need, not chronological meaning.
        TenantId(Uuid::from_u128(n as u128))
    }

    /// Distinct-per-call test route id via `Uuid::from_u128` (needs no `uuid` crate feature
    /// flag — `now_v7()`/`new_v4()` would require this crate to enable "v7"/"v4" on its
    /// `[dependencies]` `uuid` entry, which no production code here needs at all).
    fn next_route_id() -> Uuid {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        Uuid::from_u128(COUNTER.fetch_add(1, Ordering::Relaxed) as u128)
    }

    fn route(purpose: RoutePurpose) -> RouteCandidate {
        RouteCandidate {
            route_id: next_route_id(),
            tenant_id: None,
            region: None,
            purpose,
            embedding_provider_id: (purpose == RoutePurpose::Embedding)
                .then(|| "dashscope".to_string()),
            embedding_model_id: (purpose == RoutePurpose::Embedding)
                .then(|| "text-embedding-v4".to_string()),
            embedding_dimension: (purpose == RoutePurpose::Embedding).then_some(1024),
            rerank_provider_id: (purpose == RoutePurpose::Rerank).then(|| "dashscope".to_string()),
            rerank_model_id: (purpose == RoutePurpose::Rerank).then(|| "qwen3-rerank".to_string()),
            priority: 0,
            enabled: true,
            effective_from: SystemTime::UNIX_EPOCH,
            effective_to: None,
        }
    }

    fn query(tenant_id: TenantId) -> RouteQuery {
        RouteQuery {
            tenant_id,
            region: None,
            purpose: RoutePurpose::Embedding,
            as_of: SystemTime::UNIX_EPOCH + Duration::from_secs(1_000),
        }
    }

    fn admit_all(_c: &RouteCandidate) -> Result<(), String> {
        Ok(())
    }

    #[test]
    fn tenant_specific_route_beats_global_default_at_equal_priority() {
        let t = tenant(1);
        let mut global = route(RoutePurpose::Embedding);
        global.embedding_model_id = Some("global-model".into());
        let mut specific = route(RoutePurpose::Embedding);
        specific.tenant_id = Some(t);
        specific.embedding_model_id = Some("tenant-model".into());

        let candidates = [global, specific];
        let resolved = resolve(&candidates, &query(t), None, &admit_all).expect("must resolve");
        assert_eq!(resolved.model_id, "tenant-model");
    }

    #[test]
    fn region_specific_route_beats_region_agnostic_at_equal_priority() {
        let t = tenant(1);
        let mut agnostic = route(RoutePurpose::Embedding);
        agnostic.embedding_model_id = Some("agnostic-model".into());
        let mut specific = route(RoutePurpose::Embedding);
        specific.region = Some("cn-hangzhou".into());
        specific.embedding_model_id = Some("region-model".into());

        let candidates = [agnostic, specific];
        let mut q = query(t);
        q.region = Some("cn-hangzhou".into());
        let resolved = resolve(&candidates, &q, None, &admit_all).expect("must resolve");
        assert_eq!(resolved.model_id, "region-model");
    }

    #[test]
    fn region_specific_route_does_not_match_a_different_region_query() {
        let t = tenant(1);
        let mut wrong_region = route(RoutePurpose::Embedding);
        wrong_region.region = Some("us-east-1".into());
        let candidates = [wrong_region];
        let mut q = query(t);
        q.region = Some("cn-hangzhou".into());
        assert_eq!(
            resolve(&candidates, &q, None, &admit_all),
            Err(NoRoute { attempted: 1 })
        );
    }

    #[test]
    fn wrong_purpose_never_matches_regardless_of_priority() {
        let t = tenant(1);
        let mut rerank_route = route(RoutePurpose::Rerank);
        rerank_route.priority = 100;
        let candidates = [rerank_route];
        // query() defaults to Embedding purpose.
        assert_eq!(
            resolve(&candidates, &query(t), None, &admit_all),
            Err(NoRoute { attempted: 1 })
        );
    }

    #[test]
    fn priority_breaks_ties_among_equally_specific_candidates() {
        let t = tenant(1);
        let mut low = route(RoutePurpose::Embedding);
        low.priority = 1;
        low.embedding_model_id = Some("low".into());
        let mut high = route(RoutePurpose::Embedding);
        high.priority = 5;
        high.embedding_model_id = Some("high".into());

        let candidates = [low, high];
        let resolved = resolve(&candidates, &query(t), None, &admit_all).expect("must resolve");
        assert_eq!(resolved.model_id, "high");
    }

    #[test]
    fn enabled_false_never_matches() {
        let t = tenant(1);
        let mut disabled = route(RoutePurpose::Embedding);
        disabled.enabled = false;
        disabled.priority = 100;
        let candidates = [disabled];
        assert_eq!(
            resolve(&candidates, &query(t), None, &admit_all),
            Err(NoRoute { attempted: 1 })
        );
        assert_eq!(
            reject_reason(&candidates[0], &query(t), None, &admit_all),
            Some(RouteRejection::Disabled)
        );
    }

    #[test]
    fn effective_from_boundary_is_inclusive() {
        let t = tenant(1);
        let mut r = route(RoutePurpose::Embedding);
        r.effective_from = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let mut q = query(t);
        q.as_of = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        assert!(resolve(&[r.clone()], &q, None, &admit_all).is_ok());

        q.as_of = SystemTime::UNIX_EPOCH + Duration::from_secs(999);
        assert_eq!(
            reject_reason(&r, &q, None, &admit_all),
            Some(RouteRejection::NotYetEffective)
        );
    }

    #[test]
    fn effective_to_boundary_is_exclusive() {
        let t = tenant(1);
        let mut r = route(RoutePurpose::Embedding);
        r.effective_to = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(2_000));
        let mut q = query(t);

        q.as_of = SystemTime::UNIX_EPOCH + Duration::from_secs(1_999);
        assert!(resolve(&[r.clone()], &q, None, &admit_all).is_ok());

        q.as_of = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);
        assert_eq!(
            reject_reason(&r, &q, None, &admit_all),
            Some(RouteRejection::Expired)
        );
    }

    /// §19 Embedding Provider Failover 硬规则, red→green: without `projection_compatible`'s
    /// check wired into `reject_reason` (the "red" state — simulated below by calling the
    /// filters directly minus the projection check), a higher-priority but dimensionally
    /// incompatible route would win. With it wired in (the real `resolve`, "green"), the
    /// incompatible route is rejected and the compatible-but-lower-priority route wins
    /// instead.
    #[test]
    fn incompatible_embedding_route_is_rejected_even_at_higher_priority() {
        let t = tenant(1);
        let required = ProjectionRef {
            provider_id: "dashscope".into(),
            model_id: "text-embedding-v4".into(),
            dimension: 1024,
        };

        let mut incompatible = route(RoutePurpose::Embedding);
        incompatible.priority = 100; // would win on priority alone
        incompatible.embedding_provider_id = Some("dashscope".into());
        incompatible.embedding_model_id = Some("text-embedding-v3-different-dim".into());
        incompatible.embedding_dimension = Some(1536);

        let mut compatible = route(RoutePurpose::Embedding);
        compatible.priority = 1;
        compatible.embedding_provider_id = Some(required.provider_id.clone());
        compatible.embedding_model_id = Some(required.model_id.clone());

        // RED: the incompatible route is structurally the top candidate by every filter
        // this module has *except* projection compatibility — proves the fixture is a real
        // priority trap, not a vacuous test.
        assert!(
            reject_reason(&incompatible, &query(t), None, &admit_all).is_none(),
            "fixture sanity: incompatible route must clear every non-projection filter"
        );

        // GREEN: with `current_projection` supplied, resolve() must pick the compatible,
        // lower-priority route instead — never the incompatible one, regardless of priority.
        let candidates = [incompatible, compatible];
        let resolved = resolve(&candidates, &query(t), Some(&required), &admit_all)
            .expect("compatible route must still resolve");
        assert_eq!(resolved.model_id, required.model_id);
        assert_eq!(resolved.provider_id, required.provider_id);

        assert_eq!(
            reject_reason(&candidates[0], &query(t), Some(&required), &admit_all),
            Some(RouteRejection::ProjectionIncompatible {
                candidate: ProjectionRef {
                    provider_id: "dashscope".into(),
                    model_id: "text-embedding-v3-different-dim".into(),
                    dimension: 1536,
                },
                required: required.clone(),
            })
        );
    }

    /// Code-review finding fix: `(provider_id, model_id)` alone does not denote one fixed
    /// embedding space (a Matryoshka model spans multiple `dimension_options`). Same
    /// provider *and* same model, only the dimension differs — must still be rejected as a
    /// Projection Incompatible Model Change, not silently treated as the same embedding
    /// space (§19 Embedding Provider Failover 硬规则: "换 dimension 还写旧 collection" is the
    /// exact antipattern this gate exists to catch).
    #[test]
    fn same_provider_and_model_but_different_dimension_is_rejected() {
        let t = tenant(1);
        let required = ProjectionRef {
            provider_id: "dashscope".into(),
            model_id: "text-embedding-v4".into(),
            dimension: 1024,
        };
        let mut same_pair_different_dim = route(RoutePurpose::Embedding);
        same_pair_different_dim.embedding_provider_id = Some(required.provider_id.clone());
        same_pair_different_dim.embedding_model_id = Some(required.model_id.clone());
        same_pair_different_dim.embedding_dimension = Some(2048); // Matryoshka: different dim

        assert_eq!(
            reject_reason(
                &same_pair_different_dim,
                &query(t),
                Some(&required),
                &admit_all
            ),
            Some(RouteRejection::ProjectionIncompatible {
                candidate: ProjectionRef {
                    provider_id: required.provider_id.clone(),
                    model_id: required.model_id.clone(),
                    dimension: 2048,
                },
                required: required.clone(),
            }),
            "same (provider_id, model_id) but a different dimension must still be an \
             Incompatible Model Change — dimension is an independent compatibility axis, \
             not implied by provider/model alone"
        );
    }

    /// §19: rerank purposes form no persistent vector space, so `current_projection` never
    /// gates a rerank candidate even when its provider/model differs from the embedding
    /// projection identity.
    #[test]
    fn projection_compatibility_does_not_apply_to_rerank_purpose() {
        let t = tenant(1);
        let required = ProjectionRef {
            provider_id: "dashscope".into(),
            model_id: "text-embedding-v4".into(),
            dimension: 1024,
        };
        let rerank_route = route(RoutePurpose::Rerank);
        let mut q = query(t);
        q.purpose = RoutePurpose::Rerank;
        assert_eq!(
            reject_reason(&rerank_route, &q, Some(&required), &admit_all),
            None
        );
    }

    #[test]
    fn admit_closure_rejection_surfaces_as_policy_rejected() {
        let t = tenant(1);
        let r = route(RoutePurpose::Embedding);
        let deny = |_: &RouteCandidate| Err("quota exhausted".to_string());
        assert_eq!(
            reject_reason(&r, &query(t), None, &deny),
            Some(RouteRejection::PolicyRejected(
                "quota exhausted".to_string()
            ))
        );
    }

    #[test]
    fn no_candidates_reports_zero_attempted() {
        let t = tenant(1);
        assert_eq!(
            resolve(&[], &query(t), None, &admit_all),
            Err(NoRoute { attempted: 0 })
        );
    }
}
