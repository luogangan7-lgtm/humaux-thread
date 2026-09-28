//! `adapters::reasoning_route_admission` — Phase 9 R3 narrow Binding-only USER_REASONING admission adapter.
//! Depends-on: crates=[humaux-application, humaux-domain, sqlx, uuid]; services=[PostgreSQL(any) x=[control.resolve_user_reasoning_admission]];
//!   env=[]; modules=[application::consolidate, domain::egress]
//! Called-by: [adapters::consolidation_reasoner, adapters::contribution_entry_repo, adapters::contribution_execution_repo, adapters::contribution_reasoner, adapters::distill_reasoner, adapters::model_call_ledger]
//! Invariants: [the private worker never reads route/health/credential tables directly; it calls the frozen SECURITY
//!   DEFINER resolver with one binding and gets one typed locator or none; a malformed locator is InvalidLocator]
//! Spec: none
//!
//! The private worker has no direct read path to route, health, or credential authority tables.
//! It supplies one exact Binding identity to the frozen security-definer resolver and receives
//! either one immutable, typed locator or no row.

use humaux_application::consolidate::{
    PrivateReasoningDomainId, PrivateReasoningPurpose, ReasoningRouteBindingId,
    ReasoningRouteBindingVersion,
};
use humaux_domain::egress::ProcessorId;
use sqlx::{Row, types::time::OffsetDateTime};
use uuid::Uuid;

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

fn purpose_db_value(purpose: PrivateReasoningPurpose) -> &'static str {
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
        profile_id: row
            .try_get("profile_id")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
        profile_version: row
            .try_get("profile_version")
            .map_err(|_| ReasoningAdmissionError::InvalidLocator)?,
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
