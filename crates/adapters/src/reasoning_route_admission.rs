//! `adapters::reasoning_route_admission` — Phase 9 R3 narrow Binding-only USER_REASONING admission adapter.
//! Depends-on: crates=[humaux-application, humaux-domain, serde_json, sqlx, uuid]; services=[PostgreSQL(any)
//!   x=[control.observe_reasoning_route_health, control.reasoning_credential_accounts,
//!   control.reasoning_profile_capabilities, control.reasoning_profile_request_extras,
//!   control.reasoning_route_health_state, control.resolve_user_reasoning_admission]];
//!   env=[]; modules=[adapters::byok, adapters::postgres, application::consolidate, domain::egress]
//! Called-by: [adapters::consolidation_reasoner, adapters::contribution_entry_repo, adapters::contribution_execution_repo, adapters::contribution_reasoner, adapters::distill_reasoner, adapters::model_call_ledger, adapters::reasoning_route_onboarding, humaux-private-worker, private-worker::distill, private-worker::inference_rpc, private-worker::main, private-worker::route_providers, tests]
//! Invariants: [the private worker never reads route/health/credential tables directly; it calls the frozen SECURITY
//!   DEFINER resolver with one binding and gets one typed locator or none; a malformed locator is InvalidLocator;
//!   the locator's capabilities and request extras are the admitted Profile@version's own, read in the same
//!   transaction (ADR-0060 D-A, research amendment 1); a provider instance is asked for only with an admitted
//!   locator (`ProviderFor`, ADR-0060 D-B); worker-observed health is derived by the owner definer from one
//!   finalized routed call, never from caller-supplied identity (ruling E3)]
//! Spec: Baseline §11.2; §11.2.2; §11.2.5; ADR-0060 D-A; ADR-0060 D-B; ADR-0060 D-J; ADR-0060 D-M; ADR-0060 E3
//!
//! The private worker has no direct read path to route, health, or credential authority tables.
//! It supplies one exact Binding identity to the frozen security-definer resolver and receives
//! either one immutable, typed locator or no row.

use humaux_application::consolidate::{
    PrivateReasoningDomainId, PrivateReasoningPurpose, ReasoningRouteBindingId,
    ReasoningRouteBindingVersion,
};
use humaux_domain::egress::ProcessorId;

use std::sync::Arc;

use crate::byok::{ReasoningCapability, UserReasoningProvider};
use crate::postgres::PrivateWorkerDbPool;
use sqlx::{Row, types::time::OffsetDateTime};
use uuid::Uuid;

/// ADR-0060 D-B: the seam from one admitted route to the provider instance that serves it. The
/// reasoners call it only after the resolver admitted the locator (D-G), then compare the
/// instance with the route (`contribution_reasoner::provider_matches_admission`, D-C). The
/// private worker passes one closure over its per-Profile@version instances; tests pass closures
/// over stub providers. An `Err` is a static NOT_READY class (ADR-0058 D-H), never provider text.
pub type ProviderFor = dyn Fn(&ReasoningAdmissionLocator) -> Result<Arc<dyn UserReasoningProvider>, &'static str>
    + Send
    + Sync;

/// One fully admitted route. Every field comes from one resolver statement snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReasoningAdmissionLocator {
    pub tenant_id: Uuid,
    pub binding_id: ReasoningRouteBindingId,
    pub binding_version: ReasoningRouteBindingVersion,
    pub reasoning_domain_id: PrivateReasoningDomainId,
    pub purpose: PrivateReasoningPurpose,
    pub route_policy_id: Uuid,
    pub route_policy_version: i64,
    pub profile_id: Uuid,
    pub profile_version: i64,
    pub provider_account_id: Uuid,
    pub processor_id: String,
    pub processor_model_id: Uuid,
    pub provider_model_id: String,
    pub model_revision: Option<String>,
    pub provider_endpoint_id: Uuid,
    pub egress_processor_id: ProcessorId,
    pub endpoint_ref: String,
    pub region: String,
    pub service_tier: String,
    pub credential_ref: Uuid,
    pub billing_account_id: Option<Uuid>,
    pub billing_instrument_id: Option<Uuid>,
    pub provider_health_observation_id: i64,
    pub account_health_observation_id: i64,
    pub admitted_at: OffsetDateTime,
    /// §11.2 / ADR-0060 D-A: the capabilities the admitted Profile@version declares (never the
    /// catalog upper bound, never process configuration). Immutable per Profile@version (0128).
    pub capabilities: Vec<ReasoningCapability>,
    /// ADR-0060 research amendment 1: the admitted Profile@version's `request_extras` (vendor
    /// request fields as profile data, 0206 CHECK). Immutable per Profile@version (0128).
    pub request_extras: serde_json::Map<String, serde_json::Value>,
}

impl ReasoningAdmissionLocator {
    /// ADR-0060 D-M: the route fields every operator line about one call carries — provider and
    /// model as registered, Profile@version and Binding@version. Never `endpoint_ref`, never a
    /// key, never provider or user text.
    #[must_use]
    pub fn route_fields(&self) -> String {
        format!(
            "provider={} model={} profile={}@{} binding={}@{}",
            self.processor_id,
            self.provider_model_id,
            self.profile_id,
            self.profile_version,
            self.binding_id.0,
            self.binding_version.0
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasoningAdmissionError {
    Database,
    InvalidLocator,
}

impl std::fmt::Display for ReasoningAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database => f.write_str("reasoning admission resolver unavailable"),
            Self::InvalidLocator => f.write_str("reasoning admission locator invalid"),
        }
    }
}

impl std::error::Error for ReasoningAdmissionError {}

pub(crate) fn purpose_db_value(purpose: PrivateReasoningPurpose) -> &'static str {
    match purpose {
        PrivateReasoningPurpose::Distill => "PRIVATE_DISTILL_TEXT",
        PrivateReasoningPurpose::Consolidate => "PRIVATE_CONSOLIDATE",
        PrivateReasoningPurpose::Vision => "PRIVATE_DISTILL_VISION",
        PrivateReasoningPurpose::ContributionDeidentify => "CONTRIBUTION_DEIDENTIFY",
    }
}

/// Resolves the exact Binding authority under the tenant already installed in session GUCs.
///
/// `Ok(None)` is a normal fail-closed denial. No legacy table or health table is queried here.
#[allow(clippy::too_many_lines)] // One explicit typed mapping mirrors the frozen 25-column resolver row.
pub async fn resolve_user_reasoning_admission(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    binding_id: ReasoningRouteBindingId,
    binding_version: ReasoningRouteBindingVersion,
    expected_reasoning_domain_id: PrivateReasoningDomainId,
    expected_purpose: PrivateReasoningPurpose,
) -> Result<Option<ReasoningAdmissionLocator>, ReasoningAdmissionError> {
    let expected_purpose_text = purpose_db_value(expected_purpose);
    let Some(row) =
        sqlx::query("SELECT * FROM control.resolve_user_reasoning_admission($1,$2,$3,$4)")
            .bind(binding_id.0)
            .bind(binding_version.0)
            .bind(expected_reasoning_domain_id.0)
            .bind(expected_purpose_text)
            .fetch_optional(&mut **txn)
            .await
            .map_err(|_| ReasoningAdmissionError::Database)?
    else {
        return Ok(None);
    };

    let text = |column| {
        row.try_get::<String, _>(column)
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)
    };
    let row_binding_id = ReasoningRouteBindingId(
        row.try_get("binding_id")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
    );
    let row_binding_version = ReasoningRouteBindingVersion(
        row.try_get("binding_version")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
    );
    let row_domain = PrivateReasoningDomainId(
        row.try_get("reasoning_domain_id")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
    );
    let row_purpose = text("purpose")?;
    if row_binding_id != binding_id
        || row_binding_version != binding_version
        || row_domain != expected_reasoning_domain_id
        || row_purpose != expected_purpose_text
    {
        return Err(ReasoningAdmissionError::InvalidLocator);
    }
    let profile_id: Uuid = row
        .try_get("profile_id")
        .map_err(|_| ReasoningAdmissionError::InvalidLocator)?;
    let profile_version: i64 = row
        .try_get("profile_version")
        .map_err(|_| ReasoningAdmissionError::InvalidLocator)?;
    // dep: PostgreSQL(any) — control.reasoning_profile_capabilities, tenant-scoped SECURITY DEFINER
    // (ADR-0060 D-A), in the resolver's transaction; another tenant's profile answers NULL.
    let declared: Option<Vec<String>> =
        sqlx::query_scalar("SELECT control.reasoning_profile_capabilities($1,$2)")
            .bind(profile_id)
            .bind(profile_version)
            .fetch_one(&mut **txn)
            .await
            .map_err(|_| ReasoningAdmissionError::Database)?;
    let capabilities = declared
        .filter(|caps| !caps.is_empty())
        .ok_or(ReasoningAdmissionError::InvalidLocator)?
        .iter()
        .map(|cap| ReasoningCapability::parse(cap).ok_or(ReasoningAdmissionError::InvalidLocator))
        .collect::<Result<Vec<_>, _>>()?;
    // dep: PostgreSQL(any) — control.reasoning_profile_request_extras, tenant-scoped SECURITY DEFINER
    // (0207, research amendment 1), same transaction and fence as the capabilities read.
    let extras: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT control.reasoning_profile_request_extras($1,$2)")
            .bind(profile_id)
            .bind(profile_version)
            .fetch_one(&mut **txn)
            .await
            .map_err(|_| ReasoningAdmissionError::Database)?;
    let Some(serde_json::Value::Object(request_extras)) = extras else {
        return Err(ReasoningAdmissionError::InvalidLocator);
    };

    let locator = ReasoningAdmissionLocator {
        tenant_id: row
            .try_get("tenant_id")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        binding_id: row_binding_id,
        binding_version: row_binding_version,
        reasoning_domain_id: row_domain,
        purpose: expected_purpose,
        route_policy_id: row
            .try_get("route_policy_id")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        route_policy_version: row
            .try_get("route_policy_version")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        profile_id,
        profile_version,
        provider_account_id: row
            .try_get("provider_account_id")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        processor_id: text("processor_id")?,
        processor_model_id: row
            .try_get("processor_model_id")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        provider_model_id: text("provider_model_id")?,
        model_revision: row
            .try_get("model_revision")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        provider_endpoint_id: row
            .try_get("provider_endpoint_id")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        egress_processor_id: ProcessorId(
            row.try_get("egress_processor_id")
                .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        ),
        endpoint_ref: text("endpoint_ref")?,
        region: text("region")?,
        service_tier: text("service_tier")?,
        credential_ref: row
            .try_get("credential_ref")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        billing_account_id: row
            .try_get("billing_account_id")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        billing_instrument_id: row
            .try_get("billing_instrument_id")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        provider_health_observation_id: row
            .try_get("provider_health_observation_id")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        account_health_observation_id: row
            .try_get("account_health_observation_id")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        admitted_at: row
            .try_get("admitted_at")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        capabilities,
        request_extras,
    };
    if locator.tenant_id.is_nil()
        || locator.binding_id.0.is_nil()
        || locator.binding_version.0 <= 0
        || locator.reasoning_domain_id.0.is_nil()
        || locator.route_policy_id.is_nil()
        || locator.route_policy_version <= 0
        || locator.profile_id.is_nil()
        || locator.profile_version <= 0
        || locator.provider_account_id.is_nil()
        || locator.processor_id.trim().is_empty()
        || locator.processor_model_id.is_nil()
        || locator.provider_model_id.trim().is_empty()
        || locator.provider_endpoint_id.is_nil()
        || locator.egress_processor_id.0.is_nil()
        || locator.endpoint_ref.trim().is_empty()
        || locator.region.trim().is_empty()
        || locator.service_tier.trim().is_empty()
        || locator.credential_ref.is_nil()
        || locator.provider_health_observation_id <= 0
        || locator.account_health_observation_id <= 0
    {
        return Err(ReasoningAdmissionError::InvalidLocator);
    }
    Ok(Some(locator))
}

/// ADR-0060 ruling E3 (c): the NOT_READY class of a route whose latest health observation is
/// missing or no longer valid (distinct from an unbound domain and from `CREDENTIAL_NOT_MAPPED`).
pub const ROUTE_HEALTH_STALE: &str = "ROUTE_HEALTH_STALE";
/// ADR-0060 ruling E3 (c): the latest observations are valid but not admissible (e.g. a credential
/// a provider rejected, observed INVALID by the worker).
pub const ROUTE_HEALTH_DENIED: &str = "ROUTE_HEALTH_DENIED";

/// Why a binding the resolver did not admit is refused, when the reason is health (ruling E3 (c)):
/// [`ROUTE_HEALTH_STALE`], [`ROUTE_HEALTH_DENIED`] or `None` (another reason, or no such route).
/// Call it only after [`resolve_user_reasoning_admission`] returned `Ok(None)`, in its transaction.
pub async fn route_health_refusal(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    binding_id: ReasoningRouteBindingId,
    binding_version: ReasoningRouteBindingVersion,
) -> Result<Option<&'static str>, ReasoningAdmissionError> {
    // dep: PostgreSQL(any) — control.reasoning_route_health_state, tenant-scoped SECURITY DEFINER (0207)
    let row: Option<(Option<bool>, Option<bool>)> =
        sqlx::query_as("SELECT * FROM control.reasoning_route_health_state($1,$2)")
            .bind(binding_id.0)
            .bind(binding_version.0)
            .fetch_optional(&mut **txn)
            .await
            .map_err(|_| ReasoningAdmissionError::Database)?;
    Ok(match row {
        Some((Some(true), _)) => Some(ROUTE_HEALTH_STALE),
        Some((_, Some(true))) => Some(ROUTE_HEALTH_DENIED),
        _ => None,
    })
}

/// Ruling E3 (b): asks the owner definer to append the worker-observed health of one finalized
/// routed call of `tenant_id` — after a SUCCEEDED call (`credential_rejected = false`) a HEALTHY
/// provider + account pair valid for `valid_for_seconds`, appended only when the admitted validity
/// has less than half of it left; after a provider 401 (`true`) one account row with the
/// credential verdict INVALID. Returns the appended observation ids (`None` = nothing appended).
pub async fn observe_route_health(
    pool: &PrivateWorkerDbPool,
    tenant_id: Uuid,
    model_call_id: Uuid,
    credential_rejected: bool,
    valid_for_seconds: i64,
) -> Result<(Option<i64>, Option<i64>), ReasoningAdmissionError> {
    let mut txn = pool
        .pool()
        // dep: PostgreSQL(any) — opens a PostgreSQL transaction
        .begin()
        .await
        .map_err(|_| ReasoningAdmissionError::Database)?;
    // dep: PostgreSQL(any) — tenant GUC for the definer's fence
    sqlx::query("SELECT set_config('humaux.tenant_id', $1, true)")
        .bind(tenant_id.to_string())
        .execute(&mut *txn)
        .await
        .map_err(|_| ReasoningAdmissionError::Database)?;
    // dep: PostgreSQL(any) — control.observe_reasoning_route_health (0206, E3)
    let observed: (Option<i64>, Option<i64>) =
        sqlx::query_as("SELECT * FROM control.observe_reasoning_route_health($1,$2,$3)")
            .bind(model_call_id)
            .bind(credential_rejected)
            .bind(valid_for_seconds)
            .fetch_one(&mut *txn)
            .await
            .map_err(|_| ReasoningAdmissionError::Database)?;
    txn.commit()
        .await
        .map_err(|_| ReasoningAdmissionError::Database)?;
    Ok(observed)
}

/// ADR-0060 D-J: the vendor identity `(credential_ref, processor_id, external_account_ref_hash)`
/// behind each of `refs` that is bound to an account, across tenants (no tenant column). The
/// private worker's boot check that one secret serves one vendor account reads it once.
pub async fn credential_accounts(
    pool: &PrivateWorkerDbPool,
    refs: &[Uuid],
) -> Result<Vec<(Uuid, String, Vec<u8>)>, ReasoningAdmissionError> {
    // dep: PostgreSQL(any) — control.reasoning_credential_accounts (0206, D-J)
    sqlx::query_as("SELECT * FROM control.reasoning_credential_accounts($1)")
        .bind(refs)
        .fetch_all(pool.pool())
        .await
        .map_err(|_| ReasoningAdmissionError::Database)
}
