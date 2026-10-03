//! `adapters::reasoning_route_onboarding` — the operator writes that put a reasoning route in place
//!   (ADR-0060 D-H): register a Profile@version, bind a domain (R2 projection for PRIVATE_DISTILL_TEXT /
//!   PRIVATE_CONSOLIDATE), attest route health, enable/disable a profile, and read a tenant's route status.
//! Depends-on: crates=[humaux-application, serde, serde_json, sha2, sqlx, uuid]; services=[PostgreSQL(role_maintenance)
//!   x=[control.attest_reasoning_route_health, control.bind_reasoning_domain, control.reasoning_route_status,
//!   control.register_reasoning_profile, control.set_reasoning_profile_enabled]];
//!   env=[]; modules=[adapters::byok, adapters::membership_repo, adapters::postgres, adapters::provisioning,
//!   adapters::reasoning_route_admission, application::consolidate]
//! Called-by: [maintenance::main]
//! Invariants: [every write goes through one 0208 owner door as role_maintenance (no table INSERT here); one
//!   transaction per command: door + §77 SUCCESS audit row; a door refusal (55000) rolls back and writes only its
//!   DENIED audit row; the account reference text never reaches the database (its sha256 does); no receipt
//!   carries a secret or an endpoint_ref]
//! Spec: Baseline §11.2; §11.2.2; §11.2.3; §11.2.5; §77; §78.1; ADR-0053; ADR-0060 D-H; ADR-0060 E3
//!
//! Errors reuse [`ProvisioningError`] (Refused = exit 3, InvalidInput = exit 2, Db = exit 1), so the
//! CLI maps these commands exactly as it maps onboarding (ADR-0053 D-F); no new error enum.

use humaux_application::consolidate::PrivateReasoningPurpose;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use crate::byok::ReasoningCapability;
use crate::membership_repo::AdminAction;
use crate::postgres::MaintenanceDbPool;
use crate::provisioning::{
    AUDIT_RESULT_SUCCESS, ProvisioningError, audit_denied, audit_tagged, require_admin, set_tenant,
};
use crate::reasoning_route_admission::purpose_db_value;

type Result<T> = std::result::Result<T, ProvisioningError>;

/// §77 risk tag and resource type of every row this module writes (one spelling, §78.2).
const RISK_TAG: &str = "reasoning_route";
const RESOURCE_PROFILE: &str = "reasoning_profile";
const RESOURCE_BINDING: &str = "reasoning_binding";
const ACTION_REGISTER: &str = "REASONING_PROFILE_REGISTER";
const ACTION_BIND: &str = "REASONING_DOMAIN_BIND";
const ACTION_ATTEST: &str = "REASONING_ROUTE_ATTEST_HEALTH";
const ACTION_PROFILE_STATE: &str = "REASONING_PROFILE_SET_ENABLED";

/// Parses a §11.2.3 purpose wire value over the closed [`PrivateReasoningPurpose`] set (the 0208
/// bind door, not this parser, decides which purposes are bindable: ADR-0060 D-H 2).
pub fn parse_purpose(wire: &str) -> Option<PrivateReasoningPurpose> {
    [
        PrivateReasoningPurpose::Distill,
        PrivateReasoningPurpose::Consolidate,
        PrivateReasoningPurpose::Vision,
        PrivateReasoningPurpose::ContributionDeidentify,
    ]
    .into_iter()
    .find(|p| purpose_db_value(*p) == wire)
}

/// One `reasoning register` request (ADR-0060 D-H 1).
#[derive(Debug, Clone)]
pub struct RegisterProfile<'a> {
    pub tenant_id: Uuid,
    pub owner_user_id: Uuid,
    pub processor_id: &'a str,
    pub provider_model_id: &'a str,
    /// Catalog label (ruling E5), never sent on the wire; `None` = the unlabelled catalog row.
    pub model_revision: Option<&'a str>,
    pub capabilities: &'a [ReasoningCapability],
    /// Research amendment 1: vendor request fields, refused by the 0206 CHECK if adapter-owned.
    pub request_extras: &'a Value,
    /// The vendor account reference; only its sha256 is stored (ADR-0060 D-J).
    pub account_ref: &'a str,
    pub endpoint_ref: &'a str,
    pub region: &'a str,
    pub service_tier: &'a str,
    /// ADR-0060 D-L: the recipient uuid of this vendor/region, listed with its hosts in the worker env.
    pub egress_processor_id: Uuid,
    /// `None` mints a credential reference (its secret is the worker's env map entry, D-J).
    pub credential_ref: Option<Uuid>,
    /// `Some(profile_id)` registers the next version of that profile (a model switch, D-H).
    pub successor_of: Option<Uuid>,
}

/// `reasoning register` receipt.
#[derive(Debug, Clone, Serialize)]
pub struct RegisterReceipt {
    /// `created` or `existing`.
    pub outcome: String,
    pub tenant_id: Uuid,
    pub profile_id: Uuid,
    pub profile_version: i64,
    pub credential_ref: Uuid,
    pub provider_account_id: Uuid,
    pub endpoint_id: Uuid,
    pub processor_model_id: Uuid,
    /// The line to add to `HUMAUX_PRIVATE_WORKER_CREDENTIALS`; the operator names the variable.
    pub credential_map_entry: String,
    pub audit_event_id: Uuid,
}

/// `reasoning bind` receipt.
#[derive(Debug, Clone, Serialize)]
pub struct BindReceipt {
    /// `created`, `rebound` or `existing`.
    pub outcome: String,
    pub tenant_id: Uuid,
    pub reasoning_domain_id: Uuid,
    pub purpose: &'static str,
    pub binding_id: Uuid,
    pub binding_version: i64,
    pub route_policy_id: Uuid,
    /// The Binding@version a rebind closed.
    pub closed_binding_version: Option<i64>,
    pub audit_event_id: Uuid,
}

/// `reasoning attest-health` receipt.
#[derive(Debug, Clone, Serialize)]
pub struct AttestReceipt {
    pub outcome: &'static str,
    pub tenant_id: Uuid,
    pub profile_id: Uuid,
    pub profile_version: i64,
    pub provider_observation_id: i64,
    pub account_observation_id: i64,
    pub valid_until: String,
    pub audit_event_id: Uuid,
}

/// `reasoning profile-state` receipt.
#[derive(Debug, Clone, Serialize)]
pub struct ProfileStateReceipt {
    /// `updated` or `existing`.
    pub outcome: String,
    pub tenant_id: Uuid,
    pub profile_id: Uuid,
    pub profile_version: i64,
    pub enabled: bool,
    pub audit_event_id: Uuid,
}

/// Runs one door in its own transaction (READ COMMITTED, the 0128 triggers' requirement) under the
/// tenant GUC and appends the §77 SUCCESS row; a 55000 refusal rolls back and is audited DENIED.
async fn in_door<T>(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    (action, resource_type, resource_id): (&str, &str, &str),
    admin: &AdminAction<'_>,
    door: impl AsyncFnOnce(&mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<(T, Value)>,
) -> Result<(T, Uuid)> {
    require_admin(admin)?;
    // dep: PostgreSQL(role_maintenance) — one 0208 door + its §77 audit row, one transaction
    let mut txn = pool.pool().begin().await?;
    set_tenant(&mut txn, tenant_id).await?;
    let (value, metadata) = match door(&mut txn).await {
        Ok(done) => done,
        Err(ProvisioningError::Refused(reason)) => {
            txn.rollback().await?;
            audit_denied(
                pool,
                tenant_id,
                (action, RISK_TAG),
                (resource_type, resource_id),
                &reason,
                admin,
            )
            .await?;
            return Err(ProvisioningError::Refused(reason));
        }
        Err(error) => return Err(error),
    };
    let audit_event_id = audit_tagged(
        &mut txn,
        tenant_id,
        (action, RISK_TAG),
        resource_type,
        resource_id,
        AUDIT_RESULT_SUCCESS,
        admin,
        metadata,
    )
    .await?;
    txn.commit().await?;
    Ok((value, audit_event_id))
}

/// `reasoning register` (ADR-0060 D-H 1): catalog row → vendor account → endpoint → credential →
/// Profile@version through `control.register_reasoning_profile`; an enabled identical profile is
/// `existing` (nothing written but the audit row).
pub async fn register_profile(
    pool: &MaintenanceDbPool,
    request: &RegisterProfile<'_>,
    admin: &AdminAction<'_>,
) -> Result<RegisterReceipt> {
    let capabilities: Vec<&str> = request.capabilities.iter().map(|c| c.as_str()).collect();
    let account_hash = Sha256::digest(request.account_ref.as_bytes()).to_vec();
    let resource = format!(
        "{}:{}:{}",
        request.processor_id,
        request.provider_model_id,
        request.model_revision.unwrap_or("-")
    );
    let (row, audit_event_id) = in_door(
        pool,
        request.tenant_id,
        (ACTION_REGISTER, RESOURCE_PROFILE, &resource),
        admin,
        async |txn| {
            // dep: PostgreSQL(role_maintenance) — control.register_reasoning_profile (0208 owner door)
            let row = sqlx::query(
                "SELECT profile_id, profile_version, credential_ref, provider_account_id, \
                        endpoint_id, processor_model_id, disposition \
                 FROM control.register_reasoning_profile( \
                   $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
            )
            .bind(request.tenant_id)
            .bind(request.owner_user_id)
            .bind(request.processor_id)
            .bind(request.provider_model_id)
            .bind(request.model_revision)
            .bind(&capabilities)
            .bind(request.request_extras)
            .bind(&account_hash)
            .bind(request.endpoint_ref)
            .bind(request.region)
            .bind(request.service_tier)
            .bind(request.egress_processor_id)
            .bind(request.credential_ref)
            .bind(request.successor_of)
            .fetch_one(&mut **txn)
            .await?;
            let metadata = json!({
                "disposition": row.try_get::<String, _>("disposition")?,
                "profile_id": row.try_get::<Uuid, _>("profile_id")?,
                "profile_version": row.try_get::<i64, _>("profile_version")?,
                "capabilities": capabilities,
                "egress_processor_id": request.egress_processor_id,
                "region": request.region,
            });
            Ok((row, metadata))
        },
    )
    .await?;
    let credential_ref: Uuid = row.try_get("credential_ref")?;
    Ok(RegisterReceipt {
        outcome: row.try_get("disposition")?,
        tenant_id: request.tenant_id,
        profile_id: row.try_get("profile_id")?,
        profile_version: row.try_get("profile_version")?,
        credential_ref,
        provider_account_id: row.try_get("provider_account_id")?,
        endpoint_id: row.try_get("endpoint_id")?,
        processor_model_id: row.try_get("processor_model_id")?,
        credential_map_entry: format!("{credential_ref}=<ENV_NAME>"),
        audit_event_id,
    })
}

/// `reasoning bind` (ADR-0060 D-H 2): one PINNED Policy, one Candidate and the current Binding of
/// `(domain, purpose)` through `control.bind_reasoning_domain`; a rebind closes the current
/// interval and inserts the successor Binding@version.
pub async fn bind_domain(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    purpose: PrivateReasoningPurpose,
    (profile_id, profile_version): (Uuid, i64),
    admin: &AdminAction<'_>,
) -> Result<BindReceipt> {
    let purpose_text = purpose_db_value(purpose);
    let resource = format!("{reasoning_domain_id}:{purpose_text}");
    let (row, audit_event_id) = in_door(
        pool,
        tenant_id,
        (ACTION_BIND, RESOURCE_BINDING, &resource),
        admin,
        async |txn| {
            // dep: PostgreSQL(role_maintenance) — control.bind_reasoning_domain (0208 owner door)
            let row = sqlx::query(
                "SELECT binding_id, binding_version, route_policy_id, closed_binding_version, \
                        disposition \
                 FROM control.bind_reasoning_domain($1, $2, $3, $4, $5)",
            )
            .bind(tenant_id)
            .bind(reasoning_domain_id)
            .bind(purpose_text)
            .bind(profile_id)
            .bind(profile_version)
            .fetch_one(&mut **txn)
            .await?;
            let metadata = json!({
                "disposition": row.try_get::<String, _>("disposition")?,
                "binding_id": row.try_get::<Uuid, _>("binding_id")?,
                "binding_version": row.try_get::<i64, _>("binding_version")?,
                "profile_id": profile_id,
                "profile_version": profile_version,
            });
            Ok((row, metadata))
        },
    )
    .await?;
    Ok(BindReceipt {
        outcome: row.try_get("disposition")?,
        tenant_id,
        reasoning_domain_id,
        purpose: purpose_text,
        binding_id: row.try_get("binding_id")?,
        binding_version: row.try_get("binding_version")?,
        route_policy_id: row.try_get("route_policy_id")?,
        closed_binding_version: row.try_get("closed_binding_version")?,
        audit_event_id,
    })
}

/// `reasoning attest-health` (ADR-0060 D-H 3, ruling E3 (a)): one HEALTHY provider and one
/// HEALTHY/VALID account observation of the profile's exact identity, `OPERATOR_ATTEST`, valid for
/// `valid_for_secs` (the door refuses ≤ 0; §78.1: the caller always names it).
pub async fn attest_health(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    (profile_id, profile_version): (Uuid, i64),
    valid_for_secs: i64,
    admin: &AdminAction<'_>,
) -> Result<AttestReceipt> {
    let resource = format!("{profile_id}@{profile_version}");
    let (row, audit_event_id) = in_door(
        pool,
        tenant_id,
        (ACTION_ATTEST, RESOURCE_PROFILE, &resource),
        admin,
        async |txn| {
            // dep: PostgreSQL(role_maintenance) — control.attest_reasoning_route_health (0208 owner door)
            let row = sqlx::query(
                "SELECT provider_observation_id, account_observation_id, valid_until \
                 FROM control.attest_reasoning_route_health($1, $2, $3, $4)",
            )
            .bind(tenant_id)
            .bind(profile_id)
            .bind(profile_version)
            .bind(valid_for_secs)
            .fetch_one(&mut **txn)
            .await?;
            let metadata = json!({ "valid_for_secs": valid_for_secs });
            Ok((row, metadata))
        },
    )
    .await?;
    let valid_until: OffsetDateTime = row.try_get("valid_until")?;
    Ok(AttestReceipt {
        outcome: "attested",
        tenant_id,
        profile_id,
        profile_version,
        provider_observation_id: row.try_get("provider_observation_id")?,
        account_observation_id: row.try_get("account_observation_id")?,
        valid_until: valid_until.to_string(),
        audit_event_id,
    })
}

/// `reasoning profile-state` (ADR-0060 D-H 4): the no-restart stop (or restart) of one route.
pub async fn set_profile_enabled(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    (profile_id, profile_version): (Uuid, i64),
    enabled: bool,
    admin: &AdminAction<'_>,
) -> Result<ProfileStateReceipt> {
    let resource = format!("{profile_id}@{profile_version}");
    let (row, audit_event_id) = in_door(
        pool,
        tenant_id,
        (ACTION_PROFILE_STATE, RESOURCE_PROFILE, &resource),
        admin,
        async |txn| {
            // dep: PostgreSQL(role_maintenance) — control.set_reasoning_profile_enabled (0208 owner door)
            let row = sqlx::query(
                "SELECT enabled, disposition \
                 FROM control.set_reasoning_profile_enabled($1, $2, $3, $4)",
            )
            .bind(tenant_id)
            .bind(profile_id)
            .bind(profile_version)
            .bind(enabled)
            .fetch_one(&mut **txn)
            .await?;
            let metadata = json!({ "enabled": enabled });
            Ok((row, metadata))
        },
    )
    .await?;
    Ok(ProfileStateReceipt {
        outcome: row.try_get("disposition")?,
        tenant_id,
        profile_id,
        profile_version,
        enabled: row.try_get("enabled")?,
        audit_event_id,
    })
}

/// `reasoning status --tenant` (ruling E3 (c)): one entry per (ACTIVE domain, bindable purpose)
/// with its bound Profile@version, its health verdicts, `valid_until` and a `health` class
/// (`UNBOUND` / `MISSING` / `STALE` / `DENIED` / `ADMISSIBLE`). Read-only, no audit row.
pub async fn route_status(pool: &MaintenanceDbPool, tenant_id: Uuid) -> Result<Value> {
    // dep: PostgreSQL(role_maintenance) — control.reasoning_route_status (0208 owner read door)
    let mut txn = pool.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *txn)
        .await?;
    set_tenant(&mut txn, tenant_id).await?;
    let rows = sqlx::query(
        "SELECT reasoning_domain_id, purpose, binding_id, binding_version, profile_id, \
                profile_version, profile_enabled, processor_id, provider_model_id, model_revision, \
                endpoint_id, region, egress_processor_id, credential_ref, capabilities, \
                provider_verdict, account_verdict, credential_verdict, valid_until::text AS valid_until, \
                health \
         FROM control.reasoning_route_status($1)",
    )
    .bind(tenant_id)
    .fetch_all(&mut *txn)
    .await?;
    txn.commit().await?;
    let routes = rows
        .iter()
        .map(|r| {
            Ok(json!({
                "reasoning_domain_id": r.try_get::<Uuid, _>("reasoning_domain_id")?,
                "purpose": r.try_get::<String, _>("purpose")?,
                "binding": r.try_get::<Option<Uuid>, _>("binding_id")?
                    .map(|id| format!("{id}@{}", r.try_get::<i64, _>("binding_version").unwrap_or(0))),
                "profile": r.try_get::<Option<Uuid>, _>("profile_id")?
                    .map(|id| format!("{id}@{}", r.try_get::<i64, _>("profile_version").unwrap_or(0))),
                "profile_enabled": r.try_get::<Option<bool>, _>("profile_enabled")?,
                "provider": r.try_get::<Option<String>, _>("processor_id")?,
                "model": r.try_get::<Option<String>, _>("provider_model_id")?,
                "model_revision": r.try_get::<Option<String>, _>("model_revision")?,
                "endpoint_id": r.try_get::<Option<Uuid>, _>("endpoint_id")?,
                "region": r.try_get::<Option<String>, _>("region")?,
                "egress_processor_id": r.try_get::<Option<Uuid>, _>("egress_processor_id")?,
                "credential_ref": r.try_get::<Option<Uuid>, _>("credential_ref")?,
                "capabilities": r.try_get::<Option<Vec<String>>, _>("capabilities")?,
                "provider_verdict": r.try_get::<Option<String>, _>("provider_verdict")?,
                "account_verdict": r.try_get::<Option<String>, _>("account_verdict")?,
                "credential_verdict": r.try_get::<Option<String>, _>("credential_verdict")?,
                "valid_until": r.try_get::<Option<String>, _>("valid_until")?,
                "health": r.try_get::<String, _>("health")?,
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({ "outcome": "existing", "tenant_id": tenant_id, "routes": routes }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn purpose_parses_the_closed_set_only() {
        assert_eq!(
            parse_purpose("PRIVATE_CONSOLIDATE"),
            Some(PrivateReasoningPurpose::Consolidate)
        );
        assert_eq!(
            parse_purpose("CONTRIBUTION_DEIDENTIFY"),
            Some(PrivateReasoningPurpose::ContributionDeidentify)
        );
        assert_eq!(parse_purpose("private_consolidate"), None);
    }
}
