//! `adapters::model_call_ledger` — `ops.model_call_ledger` two-phase ledger writer (§19.1,
//! T7.4), same reserve()->finalize() shape as `adapters::disclosure` (§7.4): [`reserve_call`]
//! inserts identity columns + a pre-call cost estimate before the external provider call is
//! made, `status='RESERVED'`; [`finalize_call`] fills in the token/latency/actual-cost/
//! error columns and flips `status` to `SUCCEEDED`/`FAILED` after the call returns. Every
//! caller that makes a real `EmbeddingProvider`/`RerankProvider` call (§19 Retrieval Provider
//! Plane) is expected to bracket it with these two — a caller that skips `reserve_call` for
//! "just this one call" produces no ledger row at all
//! (`migrations/0094_model_call_ledger_fields.sql`'s own guard trigger cannot detect an
//! absent row, only mutate an existing one), exactly the same structural point
//! `adapters::disclosure`'s module doc makes about `ops.data_disclosures`.
//!
//! Cost numbers passed in here (`estimated_cost`/`actual_cost`) are never computed by this
//! module — §19/§78.1 "价格禁止硬编码在 Rust" places that arithmetic in
//! `humaux_retrieval_provider::cost::compute_cost` against a
//! `humaux_retrieval_provider::pricing::resolve`d `control.provider_pricing_versions` row
//! (loaded via [`load_pricing_versions`] below); this module only ever persists whatever
//! number its caller already computed.
//!
//! Runtime role: [`RetrievalWorkerDbPool`] (§6.2.1 `ops.* = R + W` domain default — this table
//! is not one of §6.2.2's named tables, same reasoning `disclosure.rs` documents for
//! `ops.data_disclosures`). `control.provider_pricing_versions` is `control.* = R`-only for
//! every runtime role (§6.2.1) — [`load_pricing_versions`] only ever `SELECT`s it.

use humaux_application::consolidate::{
    ContributionReasoningCallKind, LogicalReasoningCallId, ReasoningIntentSha256,
};
use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use crate::{
    postgres::RetrievalWorkerDbPool, reasoning_route_admission::ReasoningAdmissionLocator,
};

const REASONING_REQUEST_ADVISORY_LOCK_SQL: &str = "SELECT pg_advisory_xact_lock(hashtextextended('model-call-request:' || $1::uuid::text || ':' || $2::uuid::text,0))";

/// DB-layer failure from any function in this module. Adapter-local, not one of the
/// workspace's two frozen domain error enums (§52) — same reasoning as `disclosure.rs`'s
/// `DisclosureError`/`jobs.rs`'s `JobsError`.
#[derive(Debug)]
pub enum ModelCallLedgerError {
    Db(sqlx::Error),
    ReasoningReservationConflict,
}

impl From<sqlx::Error> for ModelCallLedgerError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl std::fmt::Display for ModelCallLedgerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(error) => write!(f, "ops.model_call_ledger DB error: {error}"),
            Self::ReasoningReservationConflict => {
                f.write_str("reasoning model-call reservation conflicts with durable state")
            }
        }
    }
}

impl std::error::Error for ModelCallLedgerError {}

/// §19.1 `status`'s two finalize()-reachable values (`RESERVED` is the DB `DEFAULT` —
/// `reserve_call` never names it explicitly, so it is not a variant here).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelCallOutcome {
    Succeeded,
    Failed,
}

impl ModelCallOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
        }
    }
}

/// [`reserve_call`]'s input — the identity/estimate columns known before the external call is
/// made. `purpose` is caller-supplied text rather than a Rust enum: this table's closed set
/// (`migrations/0094_model_call_ledger_fields.sql`'s `model_call_ledger_purpose_known` CHECK)
/// is a DB-level contract this module does not re-derive a second copy of — an unknown value
/// surfaces as a [`ModelCallLedgerError`] from the CHECK violation, the same split
/// `disclosure.rs`'s `purpose_as_db_str` avoids needing (that one *does* mirror a Rust enum,
/// because `PrivateDataPurpose` already exists in `domain::egress` for other reasons; no such
/// enum exists for this table's purpose values).
#[derive(Debug, Clone)]
pub struct ReserveCall {
    /// Caller's idempotency key for the logical call — `None` mints a fresh one
    /// (`Uuid::now_v7()`), matching `domain::ids`'s own minting convention
    /// (`crate::byok`'s doc). A retry that supplies the *same* value as a prior attempt hits
    /// `model_call_ledger_request_id_unique` and [`reserve_call`] returns the original
    /// reservation instead of a second row.
    pub request_id: Option<Uuid>,
    pub tenant_id: Uuid,
    pub workspace_id: Option<Uuid>,
    pub purpose: Option<String>,
    pub provider: String,
    pub model: Option<String>,
    pub model_revision: Option<String>,
    /// `humaux_retrieval_provider::cost::compute_cost` output for a pre-call usage estimate —
    /// this module persists it verbatim, never recomputes it.
    pub estimated_cost: Option<f64>,
}

/// [`reserve_call`]'s result — enough for the caller to make the real external call and then
/// pass `model_call_id` to [`finalize_call`].
#[derive(Debug, Clone, Copy)]
pub struct ReservedCall {
    pub model_call_id: Uuid,
    pub request_id: Uuid,
    pub called_at: OffsetDateTime,
    /// `true` when this reservation already existed (a retry with the same `request_id`
    /// hit `model_call_ledger_request_id_unique`) — the caller should not repeat the external
    /// call, only re-check `status`/re-attempt `finalize_call` if the prior attempt never got
    /// that far.
    pub already_reserved: bool,
}

/// Binding-only USER_REASONING reservation. The locator is one resolver row and is persisted
/// verbatim as the immutable route, credential, health, and payer snapshot for the attempt.
#[derive(Debug, Clone)]
pub struct ReasoningReserveCall {
    pub logical_call_id: LogicalReasoningCallId,
    pub call_kind: ContributionReasoningCallKind,
    pub intent_sha256: ReasoningIntentSha256,
    pub locator: ReasoningAdmissionLocator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReasoningCallLookup {
    Missing,
    ExistingReserved {
        model_call_id: Uuid,
        disclosure_id: Uuid,
    },
}

const CONTRIBUTION_DEIDENTIFY: &str = "CONTRIBUTION_DEIDENTIFY";

fn reasoning_snapshot_matches(
    row: &sqlx::postgres::PgRow,
    input: &ReasoningReserveCall,
) -> Result<bool, ModelCallLedgerError> {
    let locator = &input.locator;
    Ok(row.try_get::<Uuid, _>("tenant_id")? == locator.tenant_id
        && row.try_get::<Option<Uuid>, _>("workspace_id")?.is_none()
        && row.try_get::<Option<String>, _>("purpose")?.as_deref() == Some(CONTRIBUTION_DEIDENTIFY)
        && row.try_get::<String, _>("provider")? == locator.processor_id
        && row.try_get::<Option<String>, _>("model")?.as_deref()
            == Some(locator.provider_model_id.as_str())
        && row.try_get::<Option<String>, _>("model_revision")? == locator.model_revision
        && row.try_get::<Option<f64>, _>("estimated_cost")?.is_none()
        && row.try_get::<Option<String>, _>("call_kind")?.as_deref()
            == Some(input.call_kind.as_db_str())
        && row
            .try_get::<Option<Vec<u8>>, _>("intent_sha256")?
            .as_deref()
            == Some(input.intent_sha256.0.as_slice())
        && row.try_get::<Option<Uuid>, _>("reasoning_domain_id")?
            == Some(locator.reasoning_domain_id.0)
        && row.try_get::<Option<Uuid>, _>("binding_id")? == Some(locator.binding_id.0)
        && row.try_get::<Option<i64>, _>("binding_version")? == Some(locator.binding_version.0)
        && row.try_get::<Option<Uuid>, _>("route_policy_id")? == Some(locator.route_policy_id)
        && row.try_get::<Option<i64>, _>("route_policy_version")?
            == Some(locator.route_policy_version)
        && row.try_get::<Option<Uuid>, _>("profile_id")? == Some(locator.profile_id)
        && row.try_get::<Option<i64>, _>("profile_version")? == Some(locator.profile_version)
        && row.try_get::<Option<Uuid>, _>("provider_account_id")?
            == Some(locator.provider_account_id)
        && row.try_get::<Option<Uuid>, _>("provider_endpoint_id")?
            == Some(locator.provider_endpoint_id)
        && row.try_get::<Option<Uuid>, _>("egress_processor_id")?
            == Some(locator.egress_processor_id.0)
        && row.try_get::<Option<Uuid>, _>("credential_ref")? == Some(locator.credential_ref)
        && row.try_get::<Option<Uuid>, _>("billing_account_id")? == locator.billing_account_id
        && row.try_get::<Option<Uuid>, _>("billing_instrument_id")?
            == locator.billing_instrument_id
        && row.try_get::<Option<i64>, _>("provider_health_observation_id")?
            == Some(locator.provider_health_observation_id)
        && row.try_get::<Option<i64>, _>("account_health_observation_id")?
            == Some(locator.account_health_observation_id)
        && row
            .try_get::<Option<String>, _>("billing_responsibility")?
            .as_deref()
            == Some("USER")
        && row
            .try_get::<Option<OffsetDateTime>, _>("admitted_at")?
            .is_some())
}

/// Serializes one caller-carried logical contribution call before any current route resolution.
/// A matching open reservation returns its durable identities and must never dispatch again.
pub(crate) async fn lookup_reasoning_call_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    logical_call_id: LogicalReasoningCallId,
    call_kind: ContributionReasoningCallKind,
    intent_sha256: ReasoningIntentSha256,
) -> Result<ReasoningCallLookup, ModelCallLedgerError> {
    set_tenant_local(txn, tenant_id).await?;
    sqlx::query(REASONING_REQUEST_ADVISORY_LOCK_SQL)
        .bind(tenant_id)
        .bind(logical_call_id.0)
        .execute(&mut **txn)
        .await?;
    let row = sqlx::query(
        "SELECT call.model_call_id,call.status,call.call_kind,call.intent_sha256,\
                disclosure.disclosure_id \
         FROM ops.model_call_ledger call \
         LEFT JOIN ops.data_disclosures disclosure \
           ON disclosure.tenant_id=call.tenant_id \
          AND disclosure.model_call_id=call.model_call_id \
          AND disclosure.processor_id=call.egress_processor_id \
          AND disclosure.finalized_at IS NULL \
          AND disclosure.outcome IS NULL \
         WHERE call.tenant_id=$1 AND call.request_id=$2 \
         FOR UPDATE OF call",
    )
    .bind(tenant_id)
    .bind(logical_call_id.0)
    .fetch_optional(&mut **txn)
    .await?;
    let Some(row) = row else {
        return Ok(ReasoningCallLookup::Missing);
    };
    if row.try_get::<String, _>("status")? != "RESERVED"
        || row.try_get::<Option<String>, _>("call_kind")?.as_deref() != Some(call_kind.as_db_str())
        || row
            .try_get::<Option<Vec<u8>>, _>("intent_sha256")?
            .as_deref()
            != Some(intent_sha256.0.as_slice())
    {
        return Err(ModelCallLedgerError::ReasoningReservationConflict);
    }
    let disclosure_id = row
        .try_get::<Option<Uuid>, _>("disclosure_id")?
        .ok_or(ModelCallLedgerError::ReasoningReservationConflict)?;
    Ok(ReasoningCallLookup::ExistingReserved {
        model_call_id: row.try_get("model_call_id")?,
        disclosure_id,
    })
}

/// Reserves one admitted contribution attempt in the caller's private-worker transaction.
/// A retry may reuse the same request id only while the durable row is still RESERVED and every
/// immutable snapshot field is byte-for-field equivalent.
pub(crate) async fn reserve_reasoning_call_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    input: &ReasoningReserveCall,
) -> Result<ReservedCall, ModelCallLedgerError> {
    let locator = &input.locator;
    set_tenant_local(txn, locator.tenant_id).await?;
    let inserted = sqlx::query(
        "INSERT INTO ops.model_call_ledger (request_id,tenant_id,workspace_id,purpose,provider,model,model_revision,estimated_cost,call_kind,intent_sha256,reasoning_domain_id,binding_id,binding_version,route_policy_id,route_policy_version,profile_id,profile_version,provider_account_id,provider_endpoint_id,egress_processor_id,credential_ref,billing_account_id,billing_instrument_id,provider_health_observation_id,account_health_observation_id,billing_responsibility,admitted_at) VALUES ($1,$2,NULL,$3,$4,$5,$6,NULL,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,'USER',$24) ON CONFLICT (tenant_id,request_id) DO NOTHING RETURNING model_call_id,request_id,called_at",
    )
    .bind(input.logical_call_id.0)
    .bind(locator.tenant_id)
    .bind(CONTRIBUTION_DEIDENTIFY)
    .bind(&locator.processor_id)
    .bind(&locator.provider_model_id)
    .bind(&locator.model_revision)
    .bind(input.call_kind.as_db_str())
    .bind(input.intent_sha256.0.as_slice())
    .bind(locator.reasoning_domain_id.0)
    .bind(locator.binding_id.0)
    .bind(locator.binding_version.0)
    .bind(locator.route_policy_id)
    .bind(locator.route_policy_version)
    .bind(locator.profile_id)
    .bind(locator.profile_version)
    .bind(locator.provider_account_id)
    .bind(locator.provider_endpoint_id)
    .bind(locator.egress_processor_id.0)
    .bind(locator.credential_ref)
    .bind(locator.billing_account_id)
    .bind(locator.billing_instrument_id)
    .bind(locator.provider_health_observation_id)
    .bind(locator.account_health_observation_id)
    .bind(locator.admitted_at)
    .fetch_optional(&mut **txn)
    .await?;
    if let Some(row) = inserted {
        return Ok(ReservedCall {
            model_call_id: row.try_get("model_call_id")?,
            request_id: row.try_get("request_id")?,
            called_at: row.try_get("called_at")?,
            already_reserved: false,
        });
    }

    let row = sqlx::query(
        "SELECT * FROM ops.model_call_ledger WHERE tenant_id=$1 AND request_id=$2 FOR UPDATE",
    )
    .bind(locator.tenant_id)
    .bind(input.logical_call_id.0)
    .fetch_one(&mut **txn)
    .await?;
    if row.try_get::<String, _>("status")? != "RESERVED"
        || !reasoning_snapshot_matches(&row, input)?
    {
        return Err(ModelCallLedgerError::ReasoningReservationConflict);
    }
    Ok(ReservedCall {
        model_call_id: row.try_get("model_call_id")?,
        request_id: row.try_get("request_id")?,
        called_at: row.try_get("called_at")?,
        already_reserved: true,
    })
}

async fn set_tenant_local(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
) -> Result<(), ModelCallLedgerError> {
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .execute(&mut **txn)
        .await?;
    Ok(())
}

async fn reserve_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    input: &ReserveCall,
) -> Result<ReservedCall, ModelCallLedgerError> {
    set_tenant_local(txn, input.tenant_id).await?;
    let request_id = input.request_id.unwrap_or_else(Uuid::now_v7);

    let inserted = sqlx::query(
        "INSERT INTO ops.model_call_ledger \
           (request_id, tenant_id, workspace_id, purpose, provider, model, model_revision, \
            estimated_cost) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
         ON CONFLICT (tenant_id, request_id) DO NOTHING \
         RETURNING model_call_id, request_id, called_at",
    )
    .bind(request_id)
    .bind(input.tenant_id)
    .bind(input.workspace_id)
    .bind(&input.purpose)
    .bind(&input.provider)
    .bind(&input.model)
    .bind(&input.model_revision)
    .bind(input.estimated_cost)
    .fetch_optional(&mut **txn)
    .await?;

    if let Some(row) = inserted {
        return Ok(ReservedCall {
            model_call_id: row.get("model_call_id"),
            request_id: row.get("request_id"),
            called_at: row.get("called_at"),
            already_reserved: false,
        });
    }

    // ON CONFLICT DO NOTHING: a prior reservation with this (tenant_id, request_id) already
    // exists (§19.1 idempotent retry) — return it instead of a second row.
    let row = sqlx::query(
        "SELECT model_call_id, request_id, called_at FROM ops.model_call_ledger \
         WHERE tenant_id = $1 AND request_id = $2",
    )
    .bind(input.tenant_id)
    .bind(request_id)
    .fetch_one(&mut **txn)
    .await?;
    Ok(ReservedCall {
        model_call_id: row.get("model_call_id"),
        request_id: row.get("request_id"),
        called_at: row.get("called_at"),
        already_reserved: true,
    })
}

/// §19.1 reserve() — see module doc. Runs in its own transaction (same per-call-connection
/// shape `disclosure.rs`/`jobs.rs` use, not a long-lived transaction spanning the external
/// call itself).
pub async fn reserve_call(
    pool: &RetrievalWorkerDbPool,
    input: &ReserveCall,
) -> Result<ReservedCall, ModelCallLedgerError> {
    let mut txn = pool.pool().begin().await?;
    let reserved = reserve_in_txn(&mut txn, input).await?;
    txn.commit().await?;
    Ok(reserved)
}

/// [`finalize_call`]'s input — the outcome columns only known after the external call
/// returns. All optional: a `Failed` outcome from a provider that never responded (e.g.
/// `ErrorCode::ProviderTransient`) legitimately has no token/latency numbers at all.
#[derive(Debug, Clone, Default)]
pub struct FinalizeCall {
    pub input_tokens: Option<i64>,
    pub billable_tokens: Option<i64>,
    pub candidate_count: Option<i32>,
    pub candidate_tokens: Option<i64>,
    pub cache_hit: Option<bool>,
    pub latency_ms: Option<i32>,
    /// `humaux_retrieval_provider::cost::compute_cost` output for the provider's *actually*
    /// reported usage — persisted verbatim, never recomputed here.
    pub actual_cost: Option<f64>,
    pub error_class: Option<String>,
    pub provider_request_id: Option<String>,
}

async fn finalize_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    model_call_id: Uuid,
    outcome: ModelCallOutcome,
    finalize: &FinalizeCall,
) -> Result<bool, ModelCallLedgerError> {
    set_tenant_local(txn, tenant_id).await?;

    let result = sqlx::query(
        "UPDATE ops.model_call_ledger \
         SET status = $3, input_tokens = $4, billable_tokens = $5, candidate_count = $6, \
             candidate_tokens = $7, cache_hit = $8, latency_ms = $9, actual_cost = $10, \
             error_class = $11, provider_request_id = $12 \
         WHERE model_call_id = $1 AND tenant_id = $2 AND status = 'RESERVED'",
    )
    .bind(model_call_id)
    .bind(tenant_id)
    .bind(outcome.as_str())
    .bind(finalize.input_tokens)
    .bind(finalize.billable_tokens)
    .bind(finalize.candidate_count)
    .bind(finalize.candidate_tokens)
    .bind(finalize.cache_hit)
    .bind(finalize.latency_ms)
    .bind(finalize.actual_cost)
    .bind(&finalize.error_class)
    .bind(&finalize.provider_request_id)
    .execute(&mut **txn)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Finalizes an admitted contribution ledger row inside the caller's post-provider transaction.
pub(crate) async fn finalize_reasoning_call_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    model_call_id: Uuid,
    outcome: ModelCallOutcome,
    finalize: &FinalizeCall,
) -> Result<bool, ModelCallLedgerError> {
    if finalize.actual_cost.is_some() {
        return Err(ModelCallLedgerError::ReasoningReservationConflict);
    }
    set_tenant_local(txn, tenant_id).await?;
    let result = sqlx::query(
        "UPDATE ops.model_call_ledger SET status=$3,input_tokens=$4,billable_tokens=$5,candidate_count=$6,candidate_tokens=$7,cache_hit=$8,latency_ms=$9,actual_cost=$10,error_class=$11,provider_request_id=$12 WHERE model_call_id=$1 AND tenant_id=$2 AND purpose='CONTRIBUTION_DEIDENTIFY' AND status='RESERVED'",
    )
    .bind(model_call_id)
    .bind(tenant_id)
    .bind(outcome.as_str())
    .bind(finalize.input_tokens)
    .bind(finalize.billable_tokens)
    .bind(finalize.candidate_count)
    .bind(finalize.candidate_tokens)
    .bind(finalize.cache_hit)
    .bind(finalize.latency_ms)
    .bind(finalize.actual_cost)
    .bind(&finalize.error_class)
    .bind(&finalize.provider_request_id)
    .execute(&mut **txn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// §19.1 finalize() — see module doc. Returns `Ok(false)` if `model_call_id` does not exist
/// under this tenant or was already finalized (the guard trigger in
/// `migrations/0094_model_call_ledger_fields.sql` would reject a genuine re-finalize attempt
/// with a hard DB error instead; this surfaces the ordinary "nothing to do" case as a plain
/// `bool`, matching `disclosure::finalize_in_txn`'s own convention) rather than as a write.
pub async fn finalize_call(
    pool: &RetrievalWorkerDbPool,
    tenant_id: Uuid,
    model_call_id: Uuid,
    outcome: ModelCallOutcome,
    finalize: &FinalizeCall,
) -> Result<bool, ModelCallLedgerError> {
    let mut txn = pool.pool().begin().await?;
    let changed = finalize_in_txn(&mut txn, tenant_id, model_call_id, outcome, finalize).await?;
    txn.commit().await?;
    Ok(changed)
}

/// One `control.provider_pricing_versions` row — plain data, deliberately shaped to match
/// `humaux_retrieval_provider::pricing::PricingVersion` field-for-field (`effective_from`/
/// `effective_to` already converted to Unix seconds) without this crate depending on that
/// one's crate: `humaux-retrieval-provider` already depends on `humaux-adapters` (for
/// `disclosure::reserve_retrieval`/`finalize_retrieval`, that crate's own Cargo.toml), so the
/// reverse edge would be a cycle. A caller that has both crates in scope (any real
/// `EmbeddingProvider`/`RerankProvider` adapter, or this task's own integration tests) builds
/// a `PricingVersion` from this in one line; this module stays retrieval-provider-agnostic.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PricingVersionRow {
    pub input_token_price: f64,
    pub output_token_price: Option<f64>,
    pub request_price: Option<f64>,
    pub batch_discount: Option<f64>,
    pub effective_from: i64,
    pub effective_to: Option<i64>,
}

/// The sole DB-backed producer of [`PricingVersionRow`] (§3/§78.3: the pure
/// `humaux-retrieval-provider::pricing` crate never queries the DB itself). Ordered
/// `effective_from DESC` so an accidental overlap (migrations/0095's own doc — not enforced
/// by a DB constraint) resolves to the most recently opened row, matching
/// `pricing::resolve`'s doc on how it treats an ambiguous candidate slice.
pub async fn load_pricing_versions(
    pool: &RetrievalWorkerDbPool,
    provider_id: &str,
    model_id: &str,
    region: &str,
) -> Result<Vec<PricingVersionRow>, ModelCallLedgerError> {
    let rows = sqlx::query(
        "SELECT input_token_price, output_token_price, request_price, batch_discount, \
                effective_from, effective_to \
         FROM control.provider_pricing_versions \
         WHERE provider_id = $1 AND model_id = $2 AND region = $3 \
         ORDER BY effective_from DESC",
    )
    .bind(provider_id)
    .bind(model_id)
    .bind(region)
    .fetch_all(pool.pool())
    .await?;

    Ok(rows
        .iter()
        .map(|r| {
            let effective_from: OffsetDateTime = r.get("effective_from");
            let effective_to: Option<OffsetDateTime> = r.get("effective_to");
            PricingVersionRow {
                input_token_price: r.get("input_token_price"),
                output_token_price: r.get("output_token_price"),
                request_price: r.get("request_price"),
                batch_discount: r.get("batch_discount"),
                effective_from: effective_from.unix_timestamp(),
                effective_to: effective_to.map(|t| t.unix_timestamp()),
            }
        })
        .collect())
}

/// §19 Tenant Full Cost Ledger bridge: one `ops.tenant_cost_events` row for a finalized
/// external-model-token call (`cost_type = 'external_model_tokens'`, §19's own nine-kind
/// list) — a separate, explicit call rather than something `finalize_call` does implicitly,
/// so a caller that only wants the ModelCallLedger row (e.g. a failed call with no billable
/// usage) is not forced to also produce a cost event. `source` is the ModelCallLedger
/// `request_id` (text) — see `migrations/0096_tenant_cost_events.sql`'s doc on why `source`
/// is free text, not a foreign key.
pub async fn record_external_model_cost_event(
    pool: &RetrievalWorkerDbPool,
    tenant_id: Uuid,
    request_id: Uuid,
    billable_tokens: i64,
    estimated_unit_cost: Option<f64>,
    cost: f64,
    period: sqlx::types::time::Date,
) -> Result<(), ModelCallLedgerError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, tenant_id).await?;

    sqlx::query(
        "INSERT INTO ops.tenant_cost_events \
           (tenant_id, cost_type, quantity, unit, estimated_unit_cost, estimated_cost, \
            source, period) \
         VALUES ($1, 'external_model_tokens', $2, 'tokens', $3, $4, $5, $6)",
    )
    .bind(tenant_id)
    .bind(billable_tokens as f64)
    .bind(estimated_unit_cost)
    .bind(cost)
    .bind(request_id.to_string())
    .bind(period)
    .execute(&mut *txn)
    .await?;

    txn.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_wire_strings_are_stable() {
        assert_eq!(ModelCallOutcome::Succeeded.as_str(), "SUCCEEDED");
        assert_eq!(ModelCallOutcome::Failed.as_str(), "FAILED");
    }

    #[test]
    fn reasoning_request_lock_key_is_tenant_scoped() {
        assert_eq!(
            REASONING_REQUEST_ADVISORY_LOCK_SQL,
            "SELECT pg_advisory_xact_lock(hashtextextended('model-call-request:' || $1::uuid::text || ':' || $2::uuid::text,0))"
        );
    }
}
