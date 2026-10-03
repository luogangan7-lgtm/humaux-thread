//! `adapters::provisioning` — the single home of production onboarding writes (ADR-0053) and of the operator
//!   re-drive of DEAD distill jobs (ADR-0058 R4).
//! Depends-on: crates=[hex, humaux-application, humaux-domain, humaux-infra-cell, humaux-projection, serde, serde_json,
//!   sha2, sqlx, uuid]; services=[PostgreSQL(role_maintenance) r=[control.api_keys, control.memberships,
//!   control.retrieval_provider_admission_limits, control.tenants, control.workspaces, projection.family_activations,
//!   projection.stream_checkpoints, projection.tenant_placements] x=[control.audit_event_insert,
//!   control.ensure_admission_tier, control.ensure_user, control.issue_api_key, control.onboard_tenant,
//!   control.onboard_workspace, control.revoke_api_key, control.set_workspace_membership, ops.requeue_dead_distill,
//!   projection.activate_empty_family, projection.ensure_tenant_placement], Qdrant(*)]; env=[];
//!   modules=[adapters::membership_repo, adapters::postgres, adapters::qdrant, adapters::retrieve, application::auth, domain::audit,
//!   domain::identity, domain::ids, domain::ticket_family, infra-cell::permit, infra-cell::resource,
//!   infra-cell::transport, projection::serving]
//! Called-by: [adapters::role_hygiene, maintenance::main, tests, xtask::e2e_seed]
//! Invariants: [every write goes through a 0186 / 0197 owner definer as role_maintenance, no table INSERT here; one transaction
//!   per tenant (onboard_tenant installs the tenant GUC for the caller's transaction); the Qdrant probe runs outside any
//!   transaction and the activation commits only if evaluate_switch accepts the DB-returned facts and the collection
//!   generation is unchanged; a refusal writes nothing but its DENIED audit row; Qdrant or PostgreSQL down -> a typed
//!   ProvisioningError (exit 1), never a partial activation; no receipt carries a wire key or the pepper]
//! Spec: Baseline §4.2; §6.2.2; §16.2; §16.3; §17.3; §77; ADR-0017; ADR-0053; ADR-0057; ADR-0058
//!
//! Every onboarding write runs as `role_maintenance` ([`MaintenanceDbPool`]) through the eight
//! owner SECURITY DEFINER doors of migration 0186 — this module holds no table INSERT of its own.
//! `humaux-maintenance` (the operator CLI) and `xtask e2e-seed` (a thin, 127.0.0.1-only wrapper)
//! both call it, so there is exactly one copy of each onboarding step (ADR-0053 D-A/D-G).
//!
//! Transactions (ADR-0053 D-D / D-F):
//! - [`onboard_tenant`] is ONE transaction: `ensure_user` → `onboard_tenant` (→
//!   `onboard_workspace`) → `ensure_tenant_placement` → `issue_api_key` (only when the tenant was
//!   created) → the §77 audit row. `control.onboard_tenant` installs `humaux.tenant_id` with
//!   `set_config(…, true)`, which lasts until this transaction ends — one transaction per tenant.
//! - [`activate_workspace`] takes the collection generation and the Qdrant empty probe OUTSIDE
//!   any transaction, then opens a short transaction: the `projection.activate_empty_family`
//!   definer re-derives every PostgreSQL fact under row locks, `evaluate_switch` re-judges the
//!   facts it returned (the §16.3 single judgement point — a disagreement is a bug and rolls
//!   back), the §77 audit row is appended, and the generation is read again; any drift rolls back.
//!
//! Secrets: the API key is minted by the caller (the §73.5 hash lives in `humaux-protocol`, which
//! this crate does not depend on) and only its prefix, hash and log fingerprint reach this module;
//! no receipt carries a wire key or the pepper.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use humaux_application::auth::canonicalize_email;
use humaux_domain::audit::SYSTEM_TENANT_ID;
use humaux_domain::identity::{MembershipConflict, MembershipMutation, MembershipRole};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_domain::ticket_family::TicketFamily;
use humaux_infra_cell::{
    CallerId, CellAccessPermit, CellId, DEFAULT_MAX_RESPONSE_BYTES, HttpIntraCellTransport,
    IntraCellHttpTransport, IntraCellMethod, IntraCellRequest, IntraCellResource,
    IntraCellResourceRegistry, ResourceEntry, authorize_cell_access,
};
use humaux_projection::serving::{
    ActivationEvidence, ContinuationVerdict, GenerationId, SwitchCriteria, VerifiedEmpty,
    evaluate_switch,
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use crate::membership_repo::{self, AdminAction, MembershipRepoError, MembershipRequest};
use crate::postgres::MaintenanceDbPool;
use crate::qdrant::{
    Distance, ShardingMethod, create_collection_body, subject_index_body, tenant_index_body,
};
use crate::retrieve::{IndexFace, stream_count_of_version};

type Txn<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

/// §77 `AuditEvent` spellings for this path, defined once next to their only writer (§78.2).
const AUDIT_ACTOR_TYPE: &str = "ADMIN";
const AUDIT_RESULT_SUCCESS: &str = "SUCCESS";
const AUDIT_RESULT_DENIED: &str = "DENIED";
const AUDIT_RISK_TAG: &str = "onboarding";
const AUDIT_ABSENT: &str = "";

/// Qdrant caller identity of this module's own ReadWrite registry (it creates collections, so
/// it can never share the gateway's read-only one, ADR-0014).
const QDRANT_CALLER: &str = "humaux-maintenance";
const QDRANT_PERMIT_TTL: Duration = Duration::from_secs(60);
const QDRANT_TIMEOUT: Duration = Duration::from_secs(10);

/// Why a provisioning step did not complete. `Refused` is a named, definite refusal that wrote
/// nothing (CLI exit 3); `InvalidInput` a malformed request (exit 2); `Db` / `Qdrant` an
/// infrastructure failure (exit 1). None of them ever carries secret material.
#[derive(Debug)]
pub enum ProvisioningError {
    Refused(String),
    InvalidInput(String),
    Db(sqlx::Error),
    Qdrant(String),
}

impl ProvisioningError {
    /// The `humaux-maintenance` exit code for this failure (ADR-0053 D-F).
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Refused(_) => 3,
            Self::InvalidInput(_) => 2,
            Self::Db(_) | Self::Qdrant(_) => 1,
        }
    }
}

impl std::fmt::Display for ProvisioningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(reason) => write!(f, "refused: {reason}"),
            Self::InvalidInput(reason) => write!(f, "invalid input: {reason}"),
            Self::Db(error) => write!(f, "db: {error}"),
            Self::Qdrant(reason) => write!(f, "qdrant: {reason}"),
        }
    }
}

impl std::error::Error for ProvisioningError {}

impl From<sqlx::Error> for ProvisioningError {
    /// 0186's doors refuse with SQLSTATE 55000 and the reason as the message, and reject a
    /// malformed request with 22023.
    fn from(error: sqlx::Error) -> Self {
        if let sqlx::Error::Database(database) = &error {
            match database.code().as_deref() {
                Some("55000") => return Self::Refused(database.message().to_owned()),
                Some("22023") => return Self::InvalidInput(database.message().to_owned()),
                _ => {}
            }
        }
        Self::Db(error)
    }
}

type Result<T> = std::result::Result<T, ProvisioningError>;

// ============================================================================
// Receipts (one JSON object per `humaux-maintenance` subcommand)
// ============================================================================

/// One active 0117 admission-limit row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TierReceipt {
    /// `GLOBAL` | `REGION` | `TENANT` | `PURPOSE`.
    pub tier: &'static str,
    pub provider_id: String,
    pub region: Option<String>,
    pub purpose: Option<String>,
    pub tpm_limit: i64,
    pub rpm_limit: i64,
    /// Set only by the call that attempted the insert (`deploy-init`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<bool>,
}

/// `deploy-init`.
#[derive(Debug, Clone, Serialize)]
pub struct DeployInitReceipt {
    pub outcome: &'static str,
    pub tiers: Vec<TierReceipt>,
    pub audit_event_id: Option<Uuid>,
}

/// An API key as the receipt may show it: never the wire key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApiKeyReceipt {
    pub api_key_id: Uuid,
    pub prefix: String,
    /// `edge::api_key_log_fingerprint` — prefix plus the first four hash bytes.
    pub fingerprint: String,
    pub created: bool,
}

/// The §17.3 placement row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlacementReceipt {
    pub projection_family: String,
    pub collection_name: String,
    pub created: bool,
}

/// The Qdrant collection a tenant's family points live in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollectionReceipt {
    pub name: String,
    pub created: bool,
    pub dimension: u32,
    pub payload_indexes: Vec<String>,
    pub generation: String,
}

/// `onboard tenant`'s transactional part (T1).
#[derive(Debug, Clone, Serialize)]
pub struct TenantReceipt {
    pub outcome: &'static str,
    pub tenant_id: Uuid,
    pub tenant_name: String,
    pub owner_user_id: Uuid,
    pub owner_user_created: bool,
    pub workspace_id: Uuid,
    pub workspace_name: String,
    pub reasoning_domain_id: Uuid,
    pub api_key: Option<ApiKeyReceipt>,
    pub admission_tiers: Vec<TierReceipt>,
    pub placements: Vec<PlacementReceipt>,
    pub audit_event_id: Option<Uuid>,
}

/// `onboard workspace`'s transactional part.
#[derive(Debug, Clone, Serialize)]
pub struct WorkspaceReceipt {
    pub outcome: &'static str,
    pub tenant_id: Uuid,
    pub workspace_id: Uuid,
    pub workspace_name: String,
    pub audit_event_id: Option<Uuid>,
}

/// One family's activation (ADR-0053 D-D).
#[derive(Debug, Clone, Serialize)]
pub struct ActivationReceipt {
    pub domain: &'static str,
    pub projection_kind: &'static str,
    pub projection_version: &'static str,
    /// `activated` | `existing` | `refused`.
    pub outcome: &'static str,
    pub reason: Option<String>,
    pub evidence: &'static str,
    pub collection: Option<String>,
    pub generation: Option<String>,
    pub probe_id: Option<Uuid>,
    pub probe_visible: Option<u64>,
    /// `GET /collections/{c}` + the exact count, milliseconds.
    pub probe_latency_ms: Option<u64>,
    /// The short activation transaction including the generation re-read, milliseconds.
    pub activation_txn_ms: Option<u64>,
    pub audit_event_id: Option<Uuid>,
}

/// `activate` over one workspace.
#[derive(Debug, Clone, Serialize)]
pub struct WorkspaceActivation {
    pub tenant_id: Uuid,
    pub workspace_id: Uuid,
    pub activations: Vec<ActivationReceipt>,
    pub lifecycle: String,
}

impl WorkspaceActivation {
    /// The first refusal's reason, if any family was refused.
    pub fn refusal(&self) -> Option<&str> {
        self.activations
            .iter()
            .find(|a| a.outcome == "refused")
            .map(|a| a.reason.as_deref().unwrap_or("refused"))
    }
}

/// `apikey revoke`.
#[derive(Debug, Clone, Serialize)]
pub struct RevokeReceipt {
    pub outcome: &'static str,
    pub api_key_id: Uuid,
    pub prefix: String,
    pub changed: bool,
    pub audit_event_id: Option<Uuid>,
}

/// `onboard user`.
#[derive(Debug, Clone, Serialize)]
pub struct UserReceipt {
    pub outcome: &'static str,
    pub tenant_id: Uuid,
    pub user_id: Uuid,
    pub user_created: bool,
    pub membership_state: String,
    pub membership_role: String,
    pub workspace_id: Option<Uuid>,
}

/// A caller-minted API key: `prefix`, the §73.5 keyed hash of `"<prefix>.<secret>"`, and the log
/// fingerprint. The wire key itself never enters this module.
#[derive(Debug, Clone)]
pub struct NewApiKey {
    pub prefix: String,
    pub key_hash: Vec<u8>,
    pub fingerprint: String,
}

/// `"hx"` + the first 12 hex digits of `sha256(tenant_id ‖ 0x00 ‖ key_name)`: deterministic per
/// (tenant, key name), so re-running `apikey issue` / `onboard tenant` finds the same prefix and
/// answers `existing` instead of minting a second key.
pub fn api_key_prefix(tenant_id: Uuid, key_name: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(tenant_id.as_bytes());
    digest.update([0u8]);
    digest.update(key_name.as_bytes());
    let hex = hex::encode(digest.finalize());
    format!("hx{}", &hex[..12])
}

/// The flat `(domain, projection_kind, projection_version)` triples 0186 takes, from the one
/// closed set that owns them (§78.2 — SQL never spells a family).
fn family_triples() -> Vec<String> {
    TicketFamily::ALL
        .iter()
        .flat_map(|f| {
            [
                f.domain().to_owned(),
                f.projection_kind().to_owned(),
                f.projection_version().to_owned(),
            ]
        })
        .collect()
}

fn require_admin(admin: &AdminAction<'_>) -> Result<()> {
    let complete = [
        admin.actor,
        admin.reason,
        admin.ticket,
        admin.trace_id,
        admin.step_up_auth_context,
    ]
    .iter()
    .all(|field| !field.trim().is_empty());
    if complete {
        Ok(())
    } else {
        Err(ProvisioningError::InvalidInput(
            "actor, reason, ticket, trace_id and step_up_auth_context must all be non-empty \
             (§77 Sensitive Admin Action)"
                .to_owned(),
        ))
    }
}

async fn set_tenant(txn: &mut Txn<'_>, tenant_id: Uuid) -> Result<()> {
    sqlx::query("SELECT set_config('humaux.tenant_id', $1, true)")
        .bind(tenant_id.to_string())
        .execute(&mut **txn)
        .await?;
    Ok(())
}

/// One onboarding §77 row (risk tag [`AUDIT_RISK_TAG`]); see [`audit_tagged`].
#[allow(clippy::too_many_arguments)] // one audit row = these facts, in one place
async fn audit(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    action: &str,
    resource_type: &str,
    resource_id: &str,
    result: &str,
    admin: &AdminAction<'_>,
    metadata: Value,
) -> Result<Uuid> {
    audit_tagged(
        txn,
        tenant_id,
        (action, AUDIT_RISK_TAG),
        resource_type,
        resource_id,
        result,
        admin,
        metadata,
    )
    .await
}

/// One §77 row through `control.audit_event_insert` (the only audit writer, 0161) — same shape
/// as `membership_repo`'s rows; `(action, risk_tag)`. The tenant GUC must already be installed.
#[allow(clippy::too_many_arguments)] // one audit row = these facts, in one place
async fn audit_tagged(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    (action, risk_tag): (&str, &str),
    resource_type: &str,
    resource_id: &str,
    result: &str,
    admin: &AdminAction<'_>,
    mut metadata: Value,
) -> Result<Uuid> {
    if let Value::Object(map) = &mut metadata {
        map.insert("reason".into(), json!(admin.reason));
        map.insert("ticket".into(), json!(admin.ticket));
        map.insert(
            "step_up_auth_context".into(),
            json!(admin.step_up_auth_context),
        );
    }
    let event_id: Uuid = sqlx::query_scalar(
        "SELECT control.audit_event_insert( \
           $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULL::inet, $12, $13, NULL, NULL, $14)",
    )
    .bind(Uuid::now_v7())
    .bind(OffsetDateTime::now_utc())
    .bind(tenant_id)
    .bind(AUDIT_ACTOR_TYPE)
    .bind(admin.actor)
    .bind(action)
    .bind(resource_type)
    .bind(resource_id)
    .bind(result)
    .bind(admin.ticket)
    .bind(admin.trace_id)
    .bind(AUDIT_ABSENT)
    .bind(vec![risk_tag.to_owned()])
    .bind(metadata)
    .fetch_one(&mut **txn)
    .await?;
    Ok(event_id)
}

/// A refusal decided after its transaction rolled back still gets its `DENIED` row (§77 "全部
/// 审计"), in a transaction of its own.
async fn audit_denied(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    (action, risk_tag): (&str, &str),
    (resource_type, resource_id): (&str, &str),
    reason: &str,
    admin: &AdminAction<'_>,
) -> Result<Uuid> {
    // dep: PostgreSQL(role_maintenance) — the DENIED audit row of a rolled-back refusal
    let mut txn = pool.pool().begin().await?;
    set_tenant(&mut txn, tenant_id).await?;
    let id = audit_tagged(
        &mut txn,
        tenant_id,
        (action, risk_tag),
        resource_type,
        resource_id,
        AUDIT_RESULT_DENIED,
        admin,
        json!({ "refusal": reason }),
    )
    .await?;
    txn.commit().await?;
    Ok(id)
}

/// The active 0117 rows that admit `provider_id` for `tenant_id` (GLOBAL, REGION, TENANT,
/// PURPOSE), read under the installed tenant GUC.
async fn read_tiers(
    txn: &mut Txn<'_>,
    provider_id: &str,
    region: &str,
    tenant_id: Option<Uuid>,
) -> Result<Vec<TierReceipt>> {
    let rows = sqlx::query(
        "SELECT tenant_id, region, purpose, tpm_limit, rpm_limit \
           FROM control.retrieval_provider_admission_limits \
          WHERE provider_id = $1 AND effective_to IS NULL \
            AND ((tenant_id IS NULL AND purpose IS NULL AND (region IS NULL OR region = $2)) \
                 OR tenant_id = $3) \
          ORDER BY tenant_id NULLS FIRST, region NULLS FIRST, purpose NULLS FIRST",
    )
    .bind(provider_id)
    .bind(region)
    .bind(tenant_id)
    .fetch_all(&mut **txn)
    .await?;
    rows.iter()
        .map(|row| {
            let tenant: Option<Uuid> = row.try_get("tenant_id")?;
            let region: Option<String> = row.try_get("region")?;
            let purpose: Option<String> = row.try_get("purpose")?;
            let tier = match (tenant.is_some(), region.is_some(), purpose.is_some()) {
                (false, false, _) => "GLOBAL",
                (false, true, _) => "REGION",
                (true, _, false) => "TENANT",
                (true, _, true) => "PURPOSE",
            };
            Ok(TierReceipt {
                tier,
                provider_id: provider_id.to_owned(),
                region,
                purpose,
                tpm_limit: row.try_get("tpm_limit")?,
                rpm_limit: row.try_get("rpm_limit")?,
                created: None,
            })
        })
        .collect()
}

// ============================================================================
// PostgreSQL steps
// ============================================================================

/// `deploy-init`: the deployment-level GLOBAL and REGION 0117 tiers of one provider. Never
/// overwrites an active row. Audited under the §77 system tenant (the rows belong to no tenant).
pub async fn deploy_init(
    pool: &MaintenanceDbPool,
    provider_id: &str,
    region: &str,
    tpm: i64,
    rpm: i64,
    admin: &AdminAction<'_>,
) -> Result<DeployInitReceipt> {
    require_admin(admin)?;
    // dep: PostgreSQL(role_maintenance) — deploy-init transaction (two tier rows + audit)
    let mut txn = pool.pool().begin().await?;
    set_tenant(&mut txn, SYSTEM_TENANT_ID.0).await?;
    let mut created = BTreeMap::new();
    for (tier, tier_region) in [("GLOBAL", None), ("REGION", Some(region))] {
        let inserted: bool =
            sqlx::query_scalar("SELECT control.ensure_admission_tier($1, $2, NULL, NULL, $3, $4)")
                .bind(provider_id)
                .bind(tier_region)
                .bind(tpm)
                .bind(rpm)
                .fetch_one(&mut *txn)
                .await?;
        created.insert(tier, inserted);
    }
    let mut tiers = read_tiers(&mut txn, provider_id, region, None).await?;
    for tier in &mut tiers {
        tier.created = created.get(tier.tier).copied();
    }
    let any_created = created.values().any(|c| *c);
    let audit_event_id = if any_created {
        Some(
            audit(
                &mut txn,
                SYSTEM_TENANT_ID.0,
                "ONBOARD_DEPLOY_INIT",
                "admission_limits",
                provider_id,
                AUDIT_RESULT_SUCCESS,
                admin,
                json!({ "provider_id": provider_id, "region": region, "created": created }),
            )
            .await?,
        )
    } else {
        None
    };
    txn.commit().await?;
    Ok(DeployInitReceipt {
        outcome: if any_created { "created" } else { "existing" },
        tiers,
        audit_event_id,
    })
}

/// Everything `onboard tenant` writes in PostgreSQL besides the quota window and the activation.
#[derive(Debug, Clone)]
pub struct TenantRequest<'a> {
    pub name: &'a str,
    pub owner_email: &'a str,
    pub workspace_name: &'a str,
    pub reasoning_domain_name: &'a str,
    pub plan_limit: i64,
    pub period_start: OffsetDateTime,
    pub period_end: OffsetDateTime,
    pub provider_id: &'a str,
    pub region: &'a str,
    pub tenant_tpm: i64,
    pub tenant_rpm: i64,
    pub scopes: &'a [String],
    /// The Qdrant collection the tenant's placement names.
    pub collection: &'a str,
}

/// `onboard tenant`, transaction T1 (module doc). `mint` is called at most once, with the new
/// tenant's id, and only when the tenant was created — a re-run writes nothing and answers
/// `existing` (a lost key is revoked and reissued under a new key name, never recovered).
#[allow(clippy::too_many_lines)] // T1 is one transaction; splitting it hides its boundary
pub async fn onboard_tenant(
    pool: &MaintenanceDbPool,
    request: &TenantRequest<'_>,
    mint: &mut (dyn FnMut(Uuid) -> NewApiKey + Send),
    admin: &AdminAction<'_>,
) -> Result<TenantReceipt> {
    require_admin(admin)?;
    let (email_original, email_canonical) = canonicalize_email(request.owner_email)
        .map_err(|_| ProvisioningError::InvalidInput("owner email".to_owned()))?;
    if request.scopes.is_empty() {
        return Err(ProvisioningError::InvalidInput("scopes".to_owned()));
    }
    // dep: PostgreSQL(role_maintenance) — onboard tenant T1 (one transaction per tenant)
    let mut txn = pool.pool().begin().await?;
    let user = sqlx::query("SELECT user_id, created FROM control.ensure_user($1, $2)")
        .bind(&email_original)
        .bind(&email_canonical)
        .fetch_one(&mut *txn)
        .await?;
    let owner_user_id: Uuid = user.try_get("user_id")?;
    let owner_user_created: bool = user.try_get("created")?;

    let tenant = sqlx::query(
        "SELECT tenant_id, workspace_id, reasoning_domain_id, created \
           FROM control.onboard_tenant($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
    )
    .bind(request.name)
    .bind(owner_user_id)
    .bind(request.workspace_name)
    .bind(request.reasoning_domain_name)
    .bind(request.plan_limit)
    .bind(request.period_start)
    .bind(request.period_end)
    .bind(request.provider_id)
    .bind(request.region)
    .bind(request.tenant_tpm)
    .bind(request.tenant_rpm)
    .bind(family_triples())
    .fetch_one(&mut *txn)
    .await?;
    let tenant_id: Uuid = tenant.try_get("tenant_id")?;
    let workspace_id: Uuid = tenant.try_get("workspace_id")?;
    let reasoning_domain_id: Uuid = tenant.try_get("reasoning_domain_id")?;
    let created: bool = tenant.try_get("created")?;
    // Already installed by the definer; restated so every later statement's scope is explicit.
    set_tenant(&mut txn, tenant_id).await?;

    let mut placements = Vec::new();
    for family in TicketFamily::ALL {
        let projection_family = family.collection_name();
        let placed: bool =
            sqlx::query_scalar("SELECT projection.ensure_tenant_placement($1, $2, $3)")
                .bind(tenant_id)
                .bind(&projection_family)
                .bind(request.collection)
                .fetch_one(&mut *txn)
                .await?;
        placements.push(PlacementReceipt {
            projection_family,
            collection_name: request.collection.to_owned(),
            created: placed,
        });
    }

    let api_key = if created {
        let key = mint(tenant_id);
        let api_key_id: Uuid = sqlx::query_scalar(
            "SELECT api_key_id FROM control.issue_api_key($1, $2, $3, $4, $5, $6)",
        )
        .bind(tenant_id)
        .bind(owner_user_id)
        .bind(workspace_id)
        .bind(&key.prefix)
        .bind(&key.key_hash)
        .bind(request.scopes)
        .fetch_one(&mut *txn)
        .await?;
        Some(ApiKeyReceipt {
            api_key_id,
            prefix: key.prefix,
            fingerprint: key.fingerprint,
            created: true,
        })
    } else {
        None
    };

    let admission_tiers = read_tiers(
        &mut txn,
        request.provider_id,
        request.region,
        Some(tenant_id),
    )
    .await?;
    let audit_event_id = if created || placements.iter().any(|p| p.created) {
        Some(
            audit(
                &mut txn,
                tenant_id,
                "ONBOARD_TENANT",
                "tenant",
                &tenant_id.to_string(),
                AUDIT_RESULT_SUCCESS,
                admin,
                json!({
                    "tenant_created": created,
                    "workspace_id": workspace_id.to_string(),
                    "owner_user_id": owner_user_id.to_string(),
                    "api_key_prefix": api_key.as_ref().map(|k| k.prefix.clone()),
                }),
            )
            .await?,
        )
    } else {
        None
    };
    txn.commit().await?;
    Ok(TenantReceipt {
        outcome: if created { "created" } else { "existing" },
        tenant_id,
        tenant_name: request.name.to_owned(),
        owner_user_id,
        owner_user_created,
        workspace_id,
        workspace_name: request.workspace_name.to_owned(),
        reasoning_domain_id,
        api_key,
        admission_tiers,
        placements,
        audit_event_id,
    })
}

/// `onboard workspace`: a PROVISIONING workspace of an existing tenant, its OWNER workspace
/// membership and its zeroed checkpoint rows. Idempotent on (tenant, name).
pub async fn onboard_workspace(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    name: &str,
    owner_user_id: Uuid,
    admin: &AdminAction<'_>,
) -> Result<WorkspaceReceipt> {
    require_admin(admin)?;
    // dep: PostgreSQL(role_maintenance) — onboard workspace transaction
    let mut txn = pool.pool().begin().await?;
    set_tenant(&mut txn, tenant_id).await?;
    let row =
        sqlx::query("SELECT workspace_id, created FROM control.onboard_workspace($1, $2, $3, $4)")
            .bind(tenant_id)
            .bind(name)
            .bind(owner_user_id)
            .bind(family_triples())
            .fetch_one(&mut *txn)
            .await?;
    let workspace_id: Uuid = row.try_get("workspace_id")?;
    let created: bool = row.try_get("created")?;
    let audit_event_id = if created {
        Some(
            audit(
                &mut txn,
                tenant_id,
                "ONBOARD_WORKSPACE",
                "workspace",
                &workspace_id.to_string(),
                AUDIT_RESULT_SUCCESS,
                admin,
                json!({ "name": name, "owner_user_id": owner_user_id.to_string() }),
            )
            .await?,
        )
    } else {
        None
    };
    txn.commit().await?;
    Ok(WorkspaceReceipt {
        outcome: if created { "created" } else { "existing" },
        tenant_id,
        workspace_id,
        workspace_name: name.to_owned(),
        audit_event_id,
    })
}

/// `apikey issue`: one ACTIVE workspace-bound key for (tenant, user, workspace). `existing`
/// when the same prefix is already bound to the same triple.
pub async fn issue_api_key(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    user_id: Uuid,
    workspace_id: Uuid,
    key: &NewApiKey,
    scopes: &[String],
    admin: &AdminAction<'_>,
) -> Result<ApiKeyReceipt> {
    require_admin(admin)?;
    // dep: PostgreSQL(role_maintenance) — apikey issue transaction
    let mut txn = pool.pool().begin().await?;
    set_tenant(&mut txn, tenant_id).await?;
    let row = sqlx::query(
        "SELECT api_key_id, created FROM control.issue_api_key($1, $2, $3, $4, $5, $6)",
    )
    .bind(tenant_id)
    .bind(user_id)
    .bind(workspace_id)
    .bind(&key.prefix)
    .bind(&key.key_hash)
    .bind(scopes)
    .fetch_one(&mut *txn)
    .await?;
    let api_key_id: Uuid = row.try_get("api_key_id")?;
    let created: bool = row.try_get("created")?;
    if created {
        audit(
            &mut txn,
            tenant_id,
            "APIKEY_ISSUE",
            "api_key",
            &api_key_id.to_string(),
            AUDIT_RESULT_SUCCESS,
            admin,
            json!({ "prefix": key.prefix, "workspace_id": workspace_id.to_string() }),
        )
        .await?;
    }
    txn.commit().await?;
    Ok(ApiKeyReceipt {
        api_key_id,
        prefix: key.prefix.clone(),
        fingerprint: key.fingerprint.clone(),
        created,
    })
}

/// `apikey revoke`: idempotent (`changed = false` on an already revoked key).
pub async fn revoke_api_key(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    prefix: &str,
    admin: &AdminAction<'_>,
) -> Result<RevokeReceipt> {
    require_admin(admin)?;
    // dep: PostgreSQL(role_maintenance) — apikey revoke transaction
    let mut txn = pool.pool().begin().await?;
    set_tenant(&mut txn, tenant_id).await?;
    let row = sqlx::query("SELECT api_key_id, changed FROM control.revoke_api_key($1, $2)")
        .bind(tenant_id)
        .bind(prefix)
        .fetch_one(&mut *txn)
        .await?;
    let api_key_id: Uuid = row.try_get("api_key_id")?;
    let changed: bool = row.try_get("changed")?;
    let audit_event_id = if changed {
        Some(
            audit(
                &mut txn,
                tenant_id,
                "APIKEY_REVOKE",
                "api_key",
                &api_key_id.to_string(),
                AUDIT_RESULT_SUCCESS,
                admin,
                json!({ "prefix": prefix }),
            )
            .await?,
        )
    } else {
        None
    };
    txn.commit().await?;
    Ok(RevokeReceipt {
        outcome: if changed { "created" } else { "existing" },
        api_key_id,
        prefix: prefix.to_owned(),
        changed,
        audit_event_id,
    })
}

/// `placement ensure`: the tenant's §17.3 placement row for every ticket family.
pub async fn ensure_placement(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    collection: &str,
    admin: &AdminAction<'_>,
) -> Result<Vec<PlacementReceipt>> {
    require_admin(admin)?;
    // dep: PostgreSQL(role_maintenance) — placement ensure transaction
    let mut txn = pool.pool().begin().await?;
    set_tenant(&mut txn, tenant_id).await?;
    let mut placements = Vec::new();
    for family in TicketFamily::ALL {
        let projection_family = family.collection_name();
        let created: bool =
            sqlx::query_scalar("SELECT projection.ensure_tenant_placement($1, $2, $3)")
                .bind(tenant_id)
                .bind(&projection_family)
                .bind(collection)
                .fetch_one(&mut *txn)
                .await?;
        placements.push(PlacementReceipt {
            projection_family,
            collection_name: collection.to_owned(),
            created,
        });
    }
    if placements.iter().any(|p| p.created) {
        audit(
            &mut txn,
            tenant_id,
            "PLACEMENT_ENSURE",
            "tenant_placement",
            collection,
            AUDIT_RESULT_SUCCESS,
            admin,
            json!({ "collection": collection }),
        )
        .await?;
    }
    txn.commit().await?;
    Ok(placements)
}

/// `onboard user`: the user (by canonical email), an ACTIVE tenant membership through the §6.3
/// admin path (`membership_repo::apply`, invite then activate — both audited there), and, when
/// `workspace_id` is given, an ACTIVE workspace membership through 0162's definer.
pub async fn onboard_user(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    email: &str,
    role: MembershipRole,
    workspace_id: Option<Uuid>,
    admin: &AdminAction<'_>,
) -> Result<UserReceipt> {
    require_admin(admin)?;
    let (email_original, email_canonical) = canonicalize_email(email)
        .map_err(|_| ProvisioningError::InvalidInput("email".to_owned()))?;
    // dep: PostgreSQL(role_maintenance) — ensure_user transaction
    let mut txn = pool.pool().begin().await?;
    let row = sqlx::query("SELECT user_id, created FROM control.ensure_user($1, $2)")
        .bind(&email_original)
        .bind(&email_canonical)
        .fetch_one(&mut *txn)
        .await?;
    txn.commit().await?;
    let user_id: Uuid = row.try_get("user_id")?;
    let user_created: bool = row.try_get("created")?;

    let mut changed = user_created;
    for request in [
        MembershipRequest::Invite(role),
        MembershipRequest::Mutate(MembershipMutation::Activate),
    ] {
        match membership_repo::apply(pool, TenantId(tenant_id), UserId(user_id), request, *admin)
            .await
        {
            Ok(_) => changed = true,
            Err(MembershipRepoError::Conflict(MembershipConflict::AlreadyInState)) => {}
            Err(MembershipRepoError::Db(error)) => return Err(ProvisioningError::Db(error)),
            Err(other) => return Err(ProvisioningError::Refused(other.to_string())),
        }
    }

    if let Some(workspace_id) = workspace_id {
        let workspace_role = if role == MembershipRole::Owner {
            "OWNER"
        } else {
            "MEMBER"
        };
        // dep: PostgreSQL(role_maintenance) — workspace membership transaction
        let mut txn = pool.pool().begin().await?;
        set_tenant(&mut txn, tenant_id).await?;
        // 0162's upsert must pass workspace_memberships' RESTRICTIVE self-read policy.
        sqlx::query("SELECT set_config('humaux.user_id', $1, true)")
            .bind(user_id.to_string())
            .execute(&mut *txn)
            .await?;
        sqlx::query("SELECT control.set_workspace_membership($1, $2, $3, $4, 'ACTIVE')")
            .bind(tenant_id)
            .bind(workspace_id)
            .bind(user_id)
            .bind(workspace_role)
            .execute(&mut *txn)
            .await?;
        audit(
            &mut txn,
            tenant_id,
            "ONBOARD_WORKSPACE_MEMBERSHIP",
            "workspace_membership",
            &workspace_id.to_string(),
            AUDIT_RESULT_SUCCESS,
            admin,
            json!({ "user_id": user_id.to_string(), "role": workspace_role }),
        )
        .await?;
        txn.commit().await?;
    }

    // dep: PostgreSQL(role_maintenance) — read back the membership the admin path left
    let mut txn = pool.pool().begin().await?;
    set_tenant(&mut txn, tenant_id).await?;
    let membership = sqlx::query(
        "SELECT state, role FROM control.memberships WHERE tenant_id = $1 AND user_id = $2",
    )
    .bind(tenant_id)
    .bind(user_id)
    .fetch_one(&mut *txn)
    .await?;
    txn.commit().await?;
    Ok(UserReceipt {
        outcome: if changed { "created" } else { "existing" },
        tenant_id,
        user_id,
        user_created,
        membership_state: membership.try_get("state")?,
        membership_role: membership.try_get("role")?,
        workspace_id,
    })
}

// ============================================================================
// Operator re-drive of DEAD distill jobs (ADR-0058 R4, migration 0197)
// ============================================================================

/// §77 risk tag of the distill re-drive rows (not an onboarding step).
const REDRIVE_RISK_TAG: &str = "distill_redrive";
const REDRIVE_ACTION: &str = "DISTILL_REQUEUE_DEAD";
const REDRIVE_RESOURCE: &str = "distill_job";

/// Which DEAD `DERIVED_DISTILL` jobs of one tenant `jobs requeue-dead` re-arms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequeueTarget<'a> {
    /// Exactly this job; anything but a DEAD job of the tenant is refused.
    Job(Uuid),
    /// Every DEAD job of the tenant whose `last_error_class` equals this one exactly (the class a
    /// worker stored; ADR-0058 R4: chosen after the cause was fixed).
    ErrorClass(&'a str),
}

/// One job `jobs requeue-dead` re-armed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RequeuedJob {
    pub job_id: Uuid,
    pub evidence_id: Uuid,
    /// Kept on the re-armed row (plan v2 OPS-3).
    pub last_error_class: Option<String>,
    /// The counted provider requests the job had spent when it died (reset to 0).
    pub attempt_spent: i32,
}

/// Why class-mode `jobs requeue-dead` left a matching DEAD job DEAD (ADR-0058 ruling 2026-10-02
/// 20:30: the skip is reported, never silent).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequeueSkipReason {
    /// The Evidence has no outbox row: a re-armed job could only die again.
    EvidenceGone,
    /// The Evidence's outbox row is already DONE.
    OutboxSettled,
}

impl RequeueSkipReason {
    /// The definer's `skipped` value (0200).
    fn from_db(value: &str) -> Option<Self> {
        match value {
            "evidence_gone" => Some(Self::EvidenceGone),
            "outbox_settled" => Some(Self::OutboxSettled),
            _ => None,
        }
    }
}

/// One matching DEAD job class-mode `jobs requeue-dead` left DEAD.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkippedJob {
    pub job_id: Uuid,
    /// `None` when the job's payload names no Evidence.
    pub evidence_id: Option<Uuid>,
    pub last_error_class: Option<String>,
    pub attempt_spent: i32,
    pub reason: RequeueSkipReason,
}

/// `jobs requeue-dead`.
#[derive(Debug, Clone, Serialize)]
pub struct RequeueReceipt {
    /// `requeued`, or `nothing_requeued` when every matching DEAD job was skipped.
    pub outcome: &'static str,
    pub tenant_id: Uuid,
    pub requeued: Vec<RequeuedJob>,
    /// Class mode only (job mode refuses instead): the matching DEAD jobs left DEAD, with why.
    pub skipped: Vec<SkippedJob>,
    pub audit_event_id: Uuid,
}

/// `jobs requeue-dead` (ADR-0058 R4): one transaction re-arms the selected DEAD jobs and their
/// FAILED outbox rows through `ops.requeue_dead_distill` (PENDING, attempt 0, class kept,
/// scheduler row admitted) and appends the §77 row; in class mode the receipt and that row also
/// name every matching DEAD job it skipped, with the reason (0200). A refusal (`job_not_found`,
/// `job_not_dead`, `evidence_gone` / `outbox_settled` in job mode, `no_dead_job`) writes nothing
/// but its DENIED row.
pub async fn requeue_dead_distill(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    target: RequeueTarget<'_>,
    admin: &AdminAction<'_>,
) -> Result<RequeueReceipt> {
    require_admin(admin)?;
    let (job_id, error_class, resource_id) = match target {
        RequeueTarget::Job(job_id) => (Some(job_id), None, job_id.to_string()),
        RequeueTarget::ErrorClass(class) => (None, Some(class), format!("error_class:{class}")),
    };
    // dep: PostgreSQL(role_maintenance) — requeue-dead transaction (definer + audit row)
    let mut txn = pool.pool().begin().await?;
    set_tenant(&mut txn, tenant_id).await?;
    let rows = sqlx::query(
        "SELECT job_id, evidence_id, last_error_class, attempt_spent, skipped \
         FROM ops.requeue_dead_distill($1, $2, $3)",
    )
    .bind(tenant_id)
    .bind(job_id)
    .bind(error_class)
    .fetch_all(&mut *txn)
    .await;
    let rows = match rows.map_err(ProvisioningError::from) {
        Ok(rows) => rows,
        Err(ProvisioningError::Refused(reason)) => {
            txn.rollback().await?;
            audit_denied(
                pool,
                tenant_id,
                (REDRIVE_ACTION, REDRIVE_RISK_TAG),
                (REDRIVE_RESOURCE, &resource_id),
                &reason,
                admin,
            )
            .await?;
            return Err(ProvisioningError::Refused(reason));
        }
        Err(error) => return Err(error),
    };
    let (mut requeued, mut skipped) = (Vec::new(), Vec::new());
    for row in &rows {
        let job_id = row.try_get("job_id")?;
        let last_error_class = row.try_get("last_error_class")?;
        let attempt_spent = row.try_get("attempt_spent")?;
        match row.try_get::<Option<String>, _>("skipped")? {
            None => requeued.push(RequeuedJob {
                job_id,
                evidence_id: row.try_get("evidence_id")?,
                last_error_class,
                attempt_spent,
            }),
            Some(reason) => skipped.push(SkippedJob {
                job_id,
                evidence_id: row.try_get("evidence_id")?,
                last_error_class,
                attempt_spent,
                reason: RequeueSkipReason::from_db(&reason).ok_or_else(|| {
                    sqlx::Error::Decode(format!("unknown requeue skip reason {reason}").into())
                })?,
            }),
        }
    }
    let audit_event_id = audit_tagged(
        &mut txn,
        tenant_id,
        (REDRIVE_ACTION, REDRIVE_RISK_TAG),
        REDRIVE_RESOURCE,
        &resource_id,
        AUDIT_RESULT_SUCCESS,
        admin,
        json!({ "requeued": requeued, "skipped": skipped }),
    )
    .await?;
    txn.commit().await?;
    Ok(RequeueReceipt {
        outcome: if requeued.is_empty() {
            "nothing_requeued"
        } else {
            "requeued"
        },
        tenant_id,
        requeued,
        skipped,
        audit_event_id,
    })
}

/// `status --tenant`: read-only facts of one tenant (maintenance SELECTs under its tenant GUC).
#[allow(clippy::too_many_lines)] // one read-only snapshot, one row mapper per table
pub async fn status(pool: &MaintenanceDbPool, tenant_id: Uuid) -> Result<Value> {
    // dep: PostgreSQL(role_maintenance) — read-only status snapshot
    let mut txn = pool.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *txn)
        .await?;
    set_tenant(&mut txn, tenant_id).await?;
    let tenant = sqlx::query(
        "SELECT name, state, onboarding_name FROM control.tenants WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_optional(&mut *txn)
    .await?
    .ok_or_else(|| ProvisioningError::Refused("tenant_not_found".to_owned()))?;
    let workspaces = sqlx::query(
        "SELECT workspace_id, name, lifecycle FROM control.workspaces \
          WHERE tenant_id = $1 ORDER BY created_at",
    )
    .bind(tenant_id)
    .fetch_all(&mut *txn)
    .await?
    .iter()
    .map(|r| {
        Ok(json!({
            "workspace_id": r.try_get::<Uuid, _>("workspace_id")?,
            "name": r.try_get::<String, _>("name")?,
            "lifecycle": r.try_get::<String, _>("lifecycle")?,
        }))
    })
    .collect::<Result<Vec<_>>>()?;
    let families = sqlx::query(
        "SELECT scope_id, domain, projection_kind, projection_version, serving, \
                issued_highwater, projection_highwater \
           FROM projection.stream_checkpoints WHERE tenant_id = $1 \
          ORDER BY scope_id, domain, projection_kind, projection_version",
    )
    .bind(tenant_id)
    .fetch_all(&mut *txn)
    .await?
    .iter()
    .map(|r| {
        Ok(json!({
            "workspace_id": r.try_get::<Uuid, _>("scope_id")?,
            "domain": r.try_get::<String, _>("domain")?,
            "projection_kind": r.try_get::<String, _>("projection_kind")?,
            "projection_version": r.try_get::<String, _>("projection_version")?,
            "serving": r.try_get::<bool, _>("serving")?,
            "issued_highwater": r.try_get::<i64, _>("issued_highwater")?,
            "projection_highwater": r.try_get::<i64, _>("projection_highwater")?,
        }))
    })
    .collect::<Result<Vec<_>>>()?;
    let activations = sqlx::query(
        "SELECT scope_id, domain, projection_version, evidence_kind, collection_generation, \
                probe_id, activated_at::text AS activated_at \
           FROM projection.family_activations WHERE tenant_id = $1 ORDER BY activated_at",
    )
    .bind(tenant_id)
    .fetch_all(&mut *txn)
    .await?
    .iter()
    .map(|r| {
        Ok(json!({
            "workspace_id": r.try_get::<Uuid, _>("scope_id")?,
            "domain": r.try_get::<String, _>("domain")?,
            "projection_version": r.try_get::<String, _>("projection_version")?,
            "evidence_kind": r.try_get::<String, _>("evidence_kind")?,
            "generation": r.try_get::<String, _>("collection_generation")?,
            "probe_id": r.try_get::<Uuid, _>("probe_id")?,
            "activated_at": r.try_get::<String, _>("activated_at")?,
        }))
    })
    .collect::<Result<Vec<_>>>()?;
    let placements = sqlx::query(
        "SELECT projection_family, collection_name FROM projection.tenant_placements \
          WHERE tenant_id = $1 ORDER BY projection_family",
    )
    .bind(tenant_id)
    .fetch_all(&mut *txn)
    .await?
    .iter()
    .map(|r| {
        Ok(json!({
            "projection_family": r.try_get::<String, _>("projection_family")?,
            "collection_name": r.try_get::<String, _>("collection_name")?,
        }))
    })
    .collect::<Result<Vec<_>>>()?;
    let api_keys = sqlx::query(
        "SELECT prefix, status, workspace_id FROM control.api_keys \
          WHERE tenant_id = $1 ORDER BY created_at",
    )
    .bind(tenant_id)
    .fetch_all(&mut *txn)
    .await?
    .iter()
    .map(|r| {
        Ok(json!({
            "prefix": r.try_get::<String, _>("prefix")?,
            "status": r.try_get::<String, _>("status")?,
            "workspace_id": r.try_get::<Option<Uuid>, _>("workspace_id")?,
        }))
    })
    .collect::<Result<Vec<_>>>()?;
    let tiers = sqlx::query(
        "SELECT provider_id, region, purpose, tpm_limit, rpm_limit \
           FROM control.retrieval_provider_admission_limits \
          WHERE tenant_id = $1 AND effective_to IS NULL ORDER BY provider_id, purpose NULLS FIRST",
    )
    .bind(tenant_id)
    .fetch_all(&mut *txn)
    .await?
    .iter()
    .map(|r| {
        Ok(json!({
            "provider_id": r.try_get::<String, _>("provider_id")?,
            "purpose": r.try_get::<Option<String>, _>("purpose")?,
            "tpm_limit": r.try_get::<i64, _>("tpm_limit")?,
            "rpm_limit": r.try_get::<i64, _>("rpm_limit")?,
        }))
    })
    .collect::<Result<Vec<_>>>()?;
    txn.commit().await?;
    Ok(json!({
        "outcome": "existing",
        "tenant_id": tenant_id,
        "tenant_name": tenant.try_get::<String, _>("name")?,
        "tenant_state": tenant.try_get::<String, _>("state")?,
        "onboarding_name": tenant.try_get::<Option<String>, _>("onboarding_name")?,
        "workspaces": workspaces,
        "families": families,
        "activations": activations,
        "placements": placements,
        "api_keys": api_keys,
        "tenant_tiers": tiers,
    }))
}

// ============================================================================
// Qdrant steps
// ============================================================================

/// A ReadWrite `QDRANT_REST` face of this operator process (ADR-0014's read-only rule binds the
/// gateway's runtime registry, not the tool that creates collections).
// ponytail: plaintext (tls = false), like the seed it replaces; add a TLS flag when a production
// Qdrant needs it.
pub struct QdrantFace {
    registry: IntraCellResourceRegistry,
    transport: HttpIntraCellTransport,
}

impl QdrantFace {
    /// `host`/`port` of the same-Cell Qdrant and the private CIDR its address must resolve into
    /// (§83.4 判据3).
    pub fn new(host: &str, port: u16, cidr: &str) -> Result<Self> {
        let cell = CellId(Uuid::now_v7());
        let caller = CallerId(QDRANT_CALLER.to_owned());
        let cidr = cidr
            .parse()
            .map_err(|_| ProvisioningError::InvalidInput(format!("qdrant cidr {cidr}")))?;
        let mut entries = BTreeMap::new();
        entries.insert(
            IntraCellResource::QDRANT_REST,
            ResourceEntry::new(
                host,
                port,
                cell,
                vec![cidr],
                BTreeSet::from([caller.clone()]),
                false,
            )
            .map_err(|e| ProvisioningError::InvalidInput(format!("qdrant resource entry: {e}")))?,
        );
        let registry = IntraCellResourceRegistry::new(entries, cell, caller);
        let transport = HttpIntraCellTransport::new(
            registry.clone(),
            QDRANT_TIMEOUT,
            DEFAULT_MAX_RESPONSE_BYTES,
        )
        .map_err(|e| ProvisioningError::Qdrant(format!("transport: {e}")))?;
        Ok(Self {
            registry,
            transport,
        })
    }

    fn permit(&self) -> Result<CellAccessPermit> {
        authorize_cell_access(
            &self.registry,
            IntraCellResource::QDRANT_REST,
            QDRANT_PERMIT_TTL,
        )
        .map_err(|e| ProvisioningError::Qdrant(format!("permit: {e:?}")))
    }

    /// One raw REST call: `(status, body)` — the caller decides what a status means.
    async fn request(
        &self,
        method: IntraCellMethod,
        path: String,
        json_body: Option<Value>,
    ) -> Result<(u16, Option<Value>)> {
        let permit = self.permit()?;
        let response = self
            .transport
            .execute(
                &permit,
                // dep: Qdrant(*) — collection create/index/config/delete for onboarding
                IntraCellRequest {
                    method,
                    path,
                    json_body,
                    headers: Vec::new(),
                },
            )
            .await
            .map_err(|e| ProvisioningError::Qdrant(format!("{e:?}")))?;
        Ok((response.status, response.json_body))
    }

    /// `DELETE /collections/{c}` (e2e teardown only; never called by onboarding).
    pub async fn delete_collection(&self, collection: &str) -> Result<()> {
        let (status, _) = self
            .request(
                IntraCellMethod::Delete,
                format!("/collections/{collection}"),
                None,
            )
            .await?;
        if (200..300).contains(&status) || status == 404 {
            Ok(())
        } else {
            Err(ProvisioningError::Qdrant(format!(
                "DELETE collection returned {status}"
            )))
        }
    }
}

/// The exact `PUT` sequence that brings one collection into existence: the collection itself,
/// then BOTH payload indexes §17.1 / §6.1.3 require (card 9 P2: the `subject_ids` uuid index was
/// once simply never PUT and nothing could see it missing — the sentinel test below pins both).
pub fn collection_setup_puts(collection: &str, dimension: u32) -> Vec<(String, Value)> {
    vec![
        (
            format!("/collections/{collection}"),
            create_collection_body(
                dimension.into(),
                Distance::Cosine,
                1,
                1,
                1,
                ShardingMethod::Auto,
            ),
        ),
        (
            format!("/collections/{collection}/index"),
            tenant_index_body(),
        ),
        (
            format!("/collections/{collection}/index"),
            subject_index_body(),
        ),
    ]
}

/// The payload index names `collection_setup_puts` creates, in order.
fn payload_index_names(collection: &str) -> Vec<String> {
    collection_setup_puts(collection, 1)[1..]
        .iter()
        .filter_map(|(_, body)| body["field_name"].as_str().map(str::to_owned))
        .collect()
}

/// ADR-0053: the collection's generation from `GET /collections/{c}` — its name plus the sha256
/// of the canonical config subset (vector size + distance, and each required payload index's
/// type and params; never the live `points` counts). A missing collection is `collection_missing`;
/// a dimension or index mismatch is `collection_misconfigured`.
pub async fn collection_generation(
    face: &QdrantFace,
    collection: &str,
    dimension: u32,
) -> Result<GenerationId> {
    let (status, body) = face
        .request(
            IntraCellMethod::Get,
            format!("/collections/{collection}"),
            None,
        )
        .await?;
    if status == 404 {
        return Err(ProvisioningError::Refused("collection_missing".to_owned()));
    }
    if !(200..300).contains(&status) {
        return Err(ProvisioningError::Qdrant(format!(
            "GET collection returned {status}"
        )));
    }
    let body = body.unwrap_or(Value::Null);
    let result = &body["result"];
    let vectors = &result["config"]["params"]["vectors"];
    let mut canonical = format!(
        "size={};distance={}",
        vectors["size"],
        vectors["distance"].as_str().unwrap_or("")
    );
    if vectors["size"].as_u64() != Some(u64::from(dimension)) {
        return Err(ProvisioningError::Refused(
            "collection_misconfigured".to_owned(),
        ));
    }
    for index in payload_index_names(collection) {
        let schema = &result["payload_schema"][&index];
        if schema.is_null() {
            return Err(ProvisioningError::Refused(
                "collection_misconfigured".to_owned(),
            ));
        }
        canonical.push_str(&format!(
            ";{index}={}:{}",
            schema["data_type"], schema["params"]
        ));
    }
    let digest = hex::encode(Sha256::digest(canonical.as_bytes()));
    Ok(GenerationId(format!("{collection}@{digest}")))
}

/// `collection ensure`: creates the collection and both payload indexes unless present. A
/// non-2xx create is re-read and accepted when the collection now exists (a concurrent
/// onboarding created it); the index PUTs are idempotent and wait for completion so the
/// generation read right after sees them.
pub async fn ensure_collection(
    face: &QdrantFace,
    collection: &str,
    dimension: u32,
) -> Result<CollectionReceipt> {
    let (status, _) = face
        .request(
            IntraCellMethod::Get,
            format!("/collections/{collection}"),
            None,
        )
        .await?;
    let existed = (200..300).contains(&status);
    let mut puts = collection_setup_puts(collection, dimension).into_iter();
    let (create_path, create_body) = puts
        .next()
        .ok_or_else(|| ProvisioningError::Qdrant("empty setup sequence".to_owned()))?;
    let mut created = false;
    if !existed {
        let (status, _) = face
            .request(IntraCellMethod::Put, create_path.clone(), Some(create_body))
            .await?;
        if (200..300).contains(&status) {
            created = true;
        } else {
            // A concurrent onboarding holds the name (409) and may still be creating it: accept
            // the collection once it reads back.
            // ponytail: bounded blocking re-read (10 × 100 ms) in a one-shot CLI; an async timer
            // needs a tokio dependency this crate does not carry outside tests.
            let mut exists = false;
            for _ in 0..10 {
                let (again, _) = face
                    .request(IntraCellMethod::Get, create_path.clone(), None)
                    .await?;
                if (200..300).contains(&again) {
                    exists = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            if !exists {
                return Err(ProvisioningError::Qdrant(format!(
                    "PUT collection returned {status}"
                )));
            }
        }
    }
    for (path, body) in puts {
        let (status, _) = face
            .request(
                IntraCellMethod::Put,
                format!("{path}?wait=true"),
                Some(body),
            )
            .await?;
        if !(200..300).contains(&status) {
            return Err(ProvisioningError::Qdrant(format!(
                "PUT payload index returned {status}"
            )));
        }
    }
    let generation = collection_generation(face, collection, dimension).await?;
    Ok(CollectionReceipt {
        name: collection.to_owned(),
        created,
        dimension,
        payload_indexes: payload_index_names(collection),
        generation: generation.0,
    })
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// `activate`: the ADR-0053 D-D VerifiedEmpty first activation of every family of one workspace
/// (or only `only`). A refused family is reported with its named reason, audited `DENIED`, and
/// wrote nothing; the caller maps any refusal to exit 3.
pub async fn activate_workspace(
    pool: &MaintenanceDbPool,
    face: &QdrantFace,
    tenant_id: Uuid,
    workspace_id: Uuid,
    dimension: u32,
    only: Option<TicketFamily>,
    admin: &AdminAction<'_>,
) -> Result<WorkspaceActivation> {
    require_admin(admin)?;
    let mut activations = Vec::new();
    for family in TicketFamily::ALL
        .into_iter()
        .filter(|f| only.is_none_or(|o| o == *f))
    {
        let receipt = activate_family(
            pool,
            face,
            tenant_id,
            workspace_id,
            dimension,
            family,
            admin,
        )
        .await?;
        activations.push(receipt);
    }
    // dep: PostgreSQL(role_maintenance) — read the workspace lifecycle after activation
    let mut txn = pool.pool().begin().await?;
    set_tenant(&mut txn, tenant_id).await?;
    let lifecycle: Option<String> = sqlx::query_scalar(
        "SELECT lifecycle FROM control.workspaces WHERE tenant_id = $1 AND workspace_id = $2",
    )
    .bind(tenant_id)
    .bind(workspace_id)
    .fetch_optional(&mut *txn)
    .await?;
    txn.commit().await?;
    Ok(WorkspaceActivation {
        tenant_id,
        workspace_id,
        activations,
        lifecycle: lifecycle.unwrap_or_else(|| "NOT_FOUND".to_owned()),
    })
}

fn family_receipt(family: TicketFamily) -> ActivationReceipt {
    ActivationReceipt {
        domain: family.domain(),
        projection_kind: family.projection_kind(),
        projection_version: family.projection_version(),
        outcome: "refused",
        reason: None,
        evidence: "VerifiedEmpty",
        collection: None,
        generation: None,
        probe_id: None,
        probe_visible: None,
        probe_latency_ms: None,
        activation_txn_ms: None,
        audit_event_id: None,
    }
}

async fn refuse(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    workspace_id: Uuid,
    mut receipt: ActivationReceipt,
    reason: String,
    admin: &AdminAction<'_>,
) -> Result<ActivationReceipt> {
    receipt.audit_event_id = Some(
        audit_denied(
            pool,
            tenant_id,
            ("FAMILY_ACTIVATE", AUDIT_RISK_TAG),
            ("workspace", &workspace_id.to_string()),
            &reason,
            admin,
        )
        .await?,
    );
    receipt.outcome = "refused";
    receipt.reason = Some(reason);
    Ok(receipt)
}

#[allow(clippy::too_many_lines)] // one linear D-D sequence; splitting it hides the txn boundary
async fn activate_family(
    pool: &MaintenanceDbPool,
    face: &QdrantFace,
    tenant_id: Uuid,
    workspace_id: Uuid,
    dimension: u32,
    family: TicketFamily,
    admin: &AdminAction<'_>,
) -> Result<ActivationReceipt> {
    let mut receipt = family_receipt(family);

    // Step 1, outside any transaction: placement, generation g1, the probe.
    // dep: PostgreSQL(role_maintenance) — placement read before the probe
    let mut txn = pool.pool().begin().await?;
    set_tenant(&mut txn, tenant_id).await?;
    let collection: Option<String> = sqlx::query_scalar(
        "SELECT collection_name FROM projection.tenant_placements \
          WHERE tenant_id = $1 AND projection_family = $2",
    )
    .bind(tenant_id)
    .bind(family.collection_name())
    .fetch_optional(&mut *txn)
    .await?;
    txn.commit().await?;
    let Some(collection) = collection else {
        return refuse(
            pool,
            tenant_id,
            workspace_id,
            receipt,
            "placement_missing".to_owned(),
            admin,
        )
        .await;
    };
    receipt.collection = Some(collection.clone());

    let probe_started = Instant::now();
    let g1 = match collection_generation(face, &collection, dimension).await {
        Ok(generation) => generation,
        Err(ProvisioningError::Refused(reason)) => {
            return refuse(pool, tenant_id, workspace_id, receipt, reason, admin).await;
        }
        Err(other) => return Err(other),
    };
    let probe_id = Uuid::now_v7();
    if family.projection_version().is_empty() {
        return Err(ProvisioningError::InvalidInput(
            "projection_version".to_owned(),
        ));
    }
    let permit = face.permit()?;
    // ADR-0057 D-C: the ops stream count — every point of the family whatever its visibility
    // class (the previous user-less probe scope could not see USER_PRIVATE points).
    let probe_visible = stream_count_of_version(
        &IndexFace {
            transport: &face.transport,
            permit: &permit,
            collection: &collection,
        },
        TenantId(tenant_id),
        WorkspaceId(workspace_id),
        family.projection_version(),
        &[],
    )
    .await
    .ok_or_else(|| ProvisioningError::Qdrant("probe count unavailable".to_owned()))?;
    let probed_at = OffsetDateTime::now_utc();
    receipt.probe_latency_ms = Some(millis(probe_started.elapsed()));
    receipt.generation = Some(g1.0.clone());
    receipt.probe_id = Some(probe_id);
    receipt.probe_visible = Some(probe_visible);

    // Step 2-3: the short re-verifying transaction.
    let txn_started = Instant::now();
    // dep: PostgreSQL(role_maintenance) — the short activation transaction (definer + audit)
    let mut txn = pool.pool().begin().await?;
    set_tenant(&mut txn, tenant_id).await?;
    let row = sqlx::query(
        "SELECT outcome, reason, initialized_head, open_gaps, first_activation, workspace_ready \
           FROM projection.activate_empty_family($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(tenant_id)
    .bind(workspace_id)
    .bind(family.domain())
    .bind(family.projection_kind())
    .bind(family.projection_version())
    .bind(&collection)
    .bind(&g1.0)
    .bind(probe_id)
    .bind(i64::try_from(probe_visible).unwrap_or(i64::MAX))
    .bind(probed_at)
    .fetch_one(&mut *txn)
    .await?;
    let outcome: String = row.try_get("outcome")?;
    match outcome.as_str() {
        "EXISTING" => {
            txn.commit().await?;
            receipt.outcome = "existing";
            receipt.activation_txn_ms = Some(millis(txn_started.elapsed()));
            return Ok(receipt);
        }
        "REFUSED" => {
            drop(txn);
            let reason: Option<String> = row.try_get("reason")?;
            return refuse(
                pool,
                tenant_id,
                workspace_id,
                receipt,
                reason.unwrap_or_else(|| "refused".to_owned()),
                admin,
            )
            .await;
        }
        "ACTIVATED" => {}
        other => {
            return Err(ProvisioningError::Qdrant(format!(
                "unexpected activation outcome {other}"
            )));
        }
    }

    // §16.3's single judgement point, on the facts the DB returned (never on caller claims).
    let initialized_head: Option<i64> = row.try_get("initialized_head")?;
    let open_gaps: Option<i64> = row.try_get("open_gaps")?;
    let first_activation: Option<bool> = row.try_get("first_activation")?;
    let criteria = SwitchCriteria {
        shadow: ActivationEvidence::VerifiedEmpty(VerifiedEmpty {
            initialized_head: initialized_head
                .and_then(|h| u64::try_from(h).ok())
                .unwrap_or(u64::MAX),
            target_generation: g1.clone(),
            probe_id,
            probe_visible,
        }),
        visible_serving: None,
        first_activation: first_activation.unwrap_or(false),
        shadow_open_gaps: open_gaps
            .and_then(|g| u64::try_from(g).ok())
            .unwrap_or(u64::MAX),
        // No baseline exists for a first activation (ADR-0017); only a proven Fail refuses.
        continuation: ContinuationVerdict::CannotEstablish,
    };
    if let Err(rejections) = evaluate_switch(&criteria) {
        drop(txn);
        return refuse(
            pool,
            tenant_id,
            workspace_id,
            receipt,
            format!("evaluator_veto {rejections:?}"),
            admin,
        )
        .await;
    }
    let audit_event_id = audit(
        &mut txn,
        tenant_id,
        "FAMILY_ACTIVATE",
        "workspace",
        &workspace_id.to_string(),
        AUDIT_RESULT_SUCCESS,
        admin,
        json!({
            "evidence": "VerifiedEmpty",
            "domain": family.domain(),
            "projection_kind": family.projection_kind(),
            "projection_version": family.projection_version(),
            "collection": collection,
            "generation": g1.0,
            "probe_id": probe_id.to_string(),
            "probe_visible": probe_visible,
        }),
    )
    .await?;
    let g2 = collection_generation(face, &collection, dimension).await;
    if g2.as_ref().ok() != Some(&g1) {
        drop(txn);
        return refuse(
            pool,
            tenant_id,
            workspace_id,
            receipt,
            "generation_changed".to_owned(),
            admin,
        )
        .await;
    }
    txn.commit().await?;
    receipt.outcome = "activated";
    receipt.audit_event_id = Some(audit_event_id);
    receipt.activation_txn_ms = Some(millis(txn_started.elapsed()));
    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use super::{api_key_prefix, collection_setup_puts, payload_index_names};
    use uuid::Uuid;

    /// Card 9 P2 / card 24 (moved from `xtask::e2e_seed` with the function). FAULT SENTINEL:
    /// goes red the moment a seeded collection stops getting the `subject_ids` uuid payload
    /// index, the state this repo shipped in while `docs/ops/runbook.md` already told operators
    /// both indexes were created.
    #[test]
    fn a_seeded_collection_gets_both_payload_indexes() {
        let puts = collection_setup_puts("c", 1024);
        assert_eq!(puts.len(), 3, "collection + tenant index + subject index");
        assert_eq!(puts[0].0, "/collections/c");
        let fields: Vec<&str> = puts[1..]
            .iter()
            .map(|(path, body)| {
                assert_eq!(path, "/collections/c/index");
                body["field_name"].as_str().expect("field_name")
            })
            .collect();
        assert_eq!(fields, vec!["tenant_id", "subject_ids"]);
        assert_eq!(puts[1].1["field_schema"]["is_tenant"], true);
        assert_eq!(puts[2].1["field_schema"]["type"], "uuid");
        assert_eq!(payload_index_names("c"), vec!["tenant_id", "subject_ids"]);
    }

    /// The prefix is deterministic per (tenant, key name) — a re-run finds the same key — and
    /// differs across tenants and key names; it satisfies 0186's `^[a-z0-9]{4,64}$`.
    #[test]
    fn api_key_prefix_is_deterministic_and_well_formed() {
        let t = Uuid::now_v7();
        let p = api_key_prefix(t, "k1");
        assert_eq!(p, api_key_prefix(t, "k1"));
        assert_ne!(p, api_key_prefix(t, "k2"));
        assert_ne!(p, api_key_prefix(Uuid::now_v7(), "k1"));
        assert_eq!(p.len(), 14);
        assert!(
            p.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        );
    }
}
