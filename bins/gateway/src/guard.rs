//! `gateway::guard` — §83: one business admission path for every canonical MCP operation.
//! Depends-on: crates=[hex, humaux-adapters, humaux-application, humaux-domain, humaux-protocol, humaux-telemetry,
//!   serde_json, sha2, time, tokio, uuid]; services=[]; env=[]; modules=[adapters::confirm_token_repo,
//!   adapters::operation_receipt, adapters::postgres, adapters::quota_repo, adapters::remember,
//!   adapters::request_guard_repo, application::supersede, domain::affect, domain::audit, domain::confirm,
//!   domain::error, domain::identity, domain::ids, domain::selection, domain::subject, gateway::auth,
//!   protocol::edge, protocol::mcp, protocol::mcp_catalog, telemetry::admission, telemetry::metrics]
//! Called-by: [gateway::bootstrap, gateway::main, gateway::mcp_application, gateway::memory, gateway::status, tests]
//! Invariants: [transport owns HTTP validation only; this layer is the sole place that authenticates, authorizes and admits a request, so an operation that bypasses it is a bug, not a variant path]
//! Spec: Baseline §83; §52.1; ADR-0018; ADR-0028; ADR-0030; ADR-0054; ADR-0065 D-D
//!
//! Transport owns HTTP validation; this layer owns authentication through finalization.

use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use humaux_adapters::{
    confirm_token_repo::{self, ConfirmationClaim},
    operation_receipt::{self, AtomicRememberRequest, AtomicRememberResult},
    postgres::RuntimeDbPool,
    quota_repo::{
        self, QuotaReservation, RateCharge, RatePolicy, RateSubject, SHARED_RATE_OPERATION,
    },
    remember::RememberCommand,
    request_guard_repo::{self, AuditTenant, EffectiveEntitlementFacts},
};
use humaux_domain::{
    affect::AffectWriteOp,
    audit::{AuditEvent, AuditEventId, AuditMetadata, McpAuditAction, SYSTEM_TENANT_ID},
    confirm::{ConfirmToken, DestructiveOp, RISK_TAG_CONFIRMATION_MINTED},
    error::ErrorCode,
    identity::AuthorizationScope,
    ids::WorkspaceId,
    subject::SubjectWriteOp,
};
use humaux_protocol::{
    edge::{
        Cidr, ClientNetworkIdentity, IpPolicyDecision, IpPolicyInput, TrustedProxyConfig,
        build_client_network_identity, evaluate_ip_policy,
    },
    mcp::{McpHttpContext, McpOperation, ToolName},
    mcp_catalog::{MeterKind, OperationDescriptor},
};
use humaux_telemetry::metrics::{Family, families, write_family};
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::auth::{
    AuthenticatedServiceCredential, CredentialScope, authenticate_service_credential,
};

/// Explicit bootstrap policy. Empty CIDR lists mean the operator has imposed no such
/// network restriction; they never replace credential authentication or scoped grants.
pub struct GuardSettings {
    pub credential_pepper: Vec<u8>,
    /// ADR-0059 D-H: the previous pepper while a rotation window is open, `None` otherwise.
    pub credential_pepper_previous: Option<Vec<u8>>,
    pub trusted_proxies: TrustedProxyConfig,
    pub global_denylist: Vec<Cidr>,
    pub global_emergency_allowlist: Vec<Cidr>,
    pub tenant_network: BTreeMap<Uuid, TenantNetworkPolicy>,
    pub rates: GuardRatePolicies,
    pub reservation_ttl: Duration,
    pub handler_timeout: Duration,
    pub finalize_timeout: Duration,
    pub replay_ttl: Duration,
}

pub struct TenantNetworkPolicy {
    pub denylist: Vec<Cidr>,
    pub allowlist: Vec<Cidr>,
}

pub struct GuardRatePolicies {
    pub preauth_ip: RatePolicy,
    pub credential: RatePolicy,
    pub user: RatePolicy,
    pub tenant: RatePolicy,
    pub operation: RatePolicy,
    /// ADR-0065 D-D: `HUMAUX_GATEWAY_RATE_LOCK_TIMEOUT_MS`, the bound on a rate-bucket lock wait.
    pub lock_timeout: Duration,
    /// ADR-0065 D-E: `HUMAUX_GATEWAY_RATE_PREAUTH_IPV6_PREFIX_BITS`, the IPv6 prefix a pre-auth client is keyed by.
    pub preauth_ipv6_prefix_bits: u8,
}

/// A handler receives an already narrowed resource route, not credential material.
#[derive(Clone)]
pub struct AuthorizedRequest {
    authorization: AuthorizationScope,
    workspace_id: Option<WorkspaceId>,
    request_id: Uuid,
}

impl AuthorizedRequest {
    pub fn authorization(&self) -> &AuthorizationScope {
        &self.authorization
    }
    pub fn workspace_id(&self) -> Option<WorkspaceId> {
        self.workspace_id
    }
    pub fn request_id(&self) -> Uuid {
        self.request_id
    }
}

/// Operational counters only. They are never an authority for rate limits or billing.
/// Every label value is a `&'static str` from this module or a closed protocol/error enum, in the
/// order of its family's §41.2 label list (ADR-0061 D-A).
#[derive(Default)]
pub struct GuardMetrics {
    counters: Mutex<BTreeMap<(&'static str, Vec<&'static str>), u64>>,
}

/// The six guard families, in render order (ADR-0061 D-C).
const GUARD_FAMILIES: [&Family; 6] = [
    &families::HUMAUX_MCP_REQUESTS_TOTAL,
    &families::MCP_AUTH_ATTEMPTS_TOTAL,
    &families::MCP_AUTHZ_DENIED_TOTAL,
    &families::MCP_QUOTA_RESERVATIONS_TOTAL,
    &families::MCP_BMO_CONSUMED_TOTAL,
    &families::RATE_LIMIT_REJECTED_TOTAL,
];

/// Every `ToolName` (protocol keeps its own list private); the exhaustive match in
/// `tools_list_every_variant_once` makes a new variant a compile error until it is added here (§78.2).
const TOOLS: [ToolName; 8] = [
    ToolName::Remember,
    ToolName::Recall,
    ToolName::Memory,
    ToolName::Context,
    ToolName::Continuity,
    ToolName::Artifact,
    ToolName::Code,
    ToolName::Coordinate,
];

/// The closed value set of each label of `f`, in label order (ADR-0061 D-A seeding rule). The
/// values are exactly what this module's `increment` calls pass.
fn label_values(f: &Family) -> Vec<Vec<&'static str>> {
    let error_codes = || ErrorCode::ALL.iter().map(|c| c.as_str());
    match f.name {
        "humaux_mcp_requests_total" => vec![
            TOOLS.iter().map(|t| t.as_str()).collect(),
            std::iter::once("ok").chain(error_codes()).collect(),
            vec![PLAN_CLASS],
        ],
        // §41.2 frozen `result` set; `flow` has one authenticate path (`authenticate`).
        "mcp_auth_attempts_total" => vec![
            vec!["ok", "bad_credential", "expired", "locked"],
            vec![AUTH_FLOW],
        ],
        "mcp_authz_denied_total" => vec![error_codes().collect()],
        "mcp_quota_reservations_total" => vec![vec!["ok", "denied"]],
        "mcp_bmo_consumed_total" => vec![vec![PLAN_CLASS]],
        "rate_limit_rejected_total" => {
            vec![vec!["ip", "credential", "user", "tenant", "operation"]]
        }
        other => unreachable!("{other} is not a guard family"),
    }
}

/// Every combination of the per-label value sets.
fn combinations(sets: &[Vec<&'static str>]) -> Vec<Vec<&'static str>> {
    sets.iter().fold(vec![Vec::new()], |acc, set| {
        acc.iter()
            .flat_map(|prefix| {
                set.iter().map(move |v| {
                    let mut next = prefix.clone();
                    next.push(*v);
                    next
                })
            })
            .collect()
    })
}

// §41.2: plan classes are not modelled yet; every call is `unclassified` (guard.rs record sites).
const PLAN_CLASS: &str = "unclassified";
const AUTH_FLOW: &str = "service_credential";

impl GuardMetrics {
    fn increment(&self, family: &Family, values: &[&'static str]) {
        debug_assert_eq!(values.len(), family.labels.len(), "{}", family.name);
        let mut counters = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        let count = counters.entry((family.name, values.to_vec())).or_default();
        *count = count.saturating_add(1);
    }

    /// The six guard families seeded over their closed label sets, plus any recorded combination
    /// outside them, through `write_family` (ADR-0061 D-A, D-C); then the gateway's admission family, whose one
    /// counter lives in `telemetry::admission` (ADR-0065 D-C).
    pub fn render(&self, out: &mut String) {
        let counters = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        for family in GUARD_FAMILIES {
            let mut series: BTreeMap<Vec<&'static str>, u64> = combinations(&label_values(family))
                .into_iter()
                .map(|values| (values, 0))
                .collect();
            for ((name, values), count) in counters.iter() {
                if *name == family.name {
                    series.insert(values.clone(), *count);
                }
            }
            let samples: Vec<(&[&'static str], f64)> = series
                .iter()
                .map(|(values, count)| (values.as_slice(), *count as f64))
                .collect();
            write_family(out, family, &samples);
        }
        humaux_telemetry::admission::render(out);
    }
}

pub struct GatewayGuard {
    pool: Arc<RuntimeDbPool>,
    settings: GuardSettings,
    metrics: GuardMetrics,
}

struct AdmittedOperation {
    request: AuthorizedRequest,
    network: ClientNetworkIdentity,
    charge_policy: ChargePolicy,
}

struct ReadCompletion<'a> {
    reservation: Option<&'a QuotaReservation>,
    consume: bool,
    error: Option<ErrorCode>,
}

/// §33.10 rule 9 / ADR-0018: the shared precondition of every destructive action. The
/// dispatch arm names the closed operation and its target; the guard decides between
/// "mint and ask" (no token) and "hand the claim to a same-transaction consumer" (token).
pub(crate) struct ConfirmGate {
    pub op: DestructiveOp,
    pub target_id: Uuid,
    /// The operation's second argument, bound with the target (a confirmed `supersede C
    /// with B` can never execute as `supersede C with E`).
    pub successor_id: Option<Uuid>,
    pub presented: Option<ConfirmToken>,
    /// Server policy TTL (`HUMAUX_GATEWAY_CONFIRM_TOKEN_TTL_SECONDS`), never a literal.
    pub ttl: Duration,
}

/// Everything a confirmed write handler needs to run its one transaction. The claim is
/// consumed by the adapter inside that transaction (`confirm_token_repo::consume_in_txn`),
/// never here — that is what makes verify+consume+mutate atomic by construction.
pub(crate) struct ConfirmedWrite {
    pub request: AuthorizedRequest,
    pub claim: ConfirmationClaim,
    pub request_fingerprint: String,
    pub reservation_ttl: Duration,
    pub finished_audit: AuditEvent,
}

/// D-B: the first call is a success-shaped result, not an error (§52.1 keeps 18 codes).
pub(crate) enum ConfirmedOutcome<T> {
    ConfirmationRequired {
        token: ConfirmToken,
        expires_at: OffsetDateTime,
    },
    Executed(T),
}

#[derive(Clone, Copy)]
enum WorkspaceAdmission {
    NarrowRequested,
    PreserveContinuityFilter,
}

impl GatewayGuard {
    pub fn new(pool: RuntimeDbPool, settings: GuardSettings) -> Result<Self, ErrorCode> {
        let execution_budget = settings
            .handler_timeout
            .checked_add(settings.finalize_timeout)
            .ok_or(ErrorCode::InvalidInput)?;
        if settings.credential_pepper.is_empty()
            || settings.trusted_proxies.max_forwarded_hops == 0
            || settings.handler_timeout.is_zero()
            || settings.finalize_timeout.is_zero()
            || settings.replay_ttl.is_zero()
            || settings.reservation_ttl <= execution_budget
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            pool: Arc::new(pool),
            settings,
            metrics: GuardMetrics::default(),
        })
    }

    pub fn metrics(&self) -> &GuardMetrics {
        &self.metrics
    }

    pub(crate) fn runtime_pool(&self) -> Arc<RuntimeDbPool> {
        Arc::clone(&self.pool)
    }

    pub(crate) fn enumeration_mac_key(&self) -> [u8; 32] {
        humaux_domain::selection::cursor_mac_key(&self.settings.credential_pepper)
    }

    fn network(&self, context: &McpHttpContext) -> ClientNetworkIdentity {
        build_client_network_identity(
            context.peer_ip(),
            context.forwarded(),
            &self.settings.trusted_proxies,
            None,
            None,
        )
    }

    fn network_allowed(
        &self,
        network: &ClientNetworkIdentity,
        auth: Option<&AuthorizationScope>,
    ) -> bool {
        let tenant = auth.and_then(|auth| self.settings.tenant_network.get(&auth.tenant_id().0));
        matches!(
            evaluate_ip_policy(&IpPolicyInput {
                ip: network.client_ip,
                global_denylist: &self.settings.global_denylist,
                global_emergency_allowlist: &self.settings.global_emergency_allowlist,
                tenant_denylist: tenant.map(|p| p.denylist.as_slice()).unwrap_or(&[]),
                tenant_allowlist: tenant.map(|p| p.allowlist.as_slice()).unwrap_or(&[]),
                // authenticate_service_credential already checks the actual DB credential CIDRs.
                credential_allowed_cidrs: &[],
                region_asn_risk_tags: &network.risk_tags,
            }),
            IpPolicyDecision::Allow { .. }
        )
    }

    /// ADR-0065 D-D: the post-auth charges of one request in one transaction; a denial counts
    /// `rate_limit_rejected_total` under the scope of the charge that was denied, as the sequential path did.
    async fn rate(&self, charges: Vec<(RateCharge<'_>, &'static str)>) -> Result<(), ErrorCode> {
        let (charges, scopes): (Vec<_>, Vec<_>) = charges.into_iter().unzip();
        match quota_repo::consume_rate_batch(&self.pool, &charges, self.settings.rates.lock_timeout)
            .await
        {
            Ok(()) => Ok(()),
            Err((code, i)) => {
                if code == ErrorCode::RateLimited {
                    self.metrics
                        .increment(&families::RATE_LIMIT_REJECTED_TOTAL, &[scopes[i]]);
                }
                Err(code)
            }
        }
    }

    async fn authenticate(
        &self,
        context: &McpHttpContext,
        network: &ClientNetworkIdentity,
    ) -> Result<AuthenticatedServiceCredential, ErrorCode> {
        let result = match context.authorization() {
            Some(header) => {
                authenticate_service_credential(
                    &self.pool,
                    header,
                    &self.settings.credential_pepper,
                    self.settings.credential_pepper_previous.as_deref(),
                    network.client_ip,
                    SystemTime::now(),
                )
                .await
            }
            None => Err(ErrorCode::Unauthorized),
        };
        self.metrics.increment(
            &families::MCP_AUTH_ATTEMPTS_TOTAL,
            &[
                if result.is_ok() {
                    "ok"
                } else {
                    "bad_credential"
                },
                AUTH_FLOW,
            ],
        );
        match &result {
            Ok(credential) => {
                self.audit(
                    context,
                    network,
                    Some(credential.identity()),
                    McpAuditAction::McpAuthLogin,
                    "protocol",
                    None,
                )
                .await?
            }
            Err(code) => {
                self.audit(
                    context,
                    network,
                    None,
                    McpAuditAction::McpAuthLogin,
                    "protocol",
                    Some(*code),
                )
                .await?
            }
        }
        result
    }

    /// Exactly once per HTTP request, before JSON-RPC parsing (including malformed RPC).
    pub async fn preflight(&self, context: &McpHttpContext) -> Result<(), ErrorCode> {
        let network = self.network(context);
        let result = if !self.network_allowed(&network, None) {
            Err(ErrorCode::Forbidden)
        } else {
            let rates = &self.settings.rates;
            let result = quota_repo::consume_rate(
                &self.pool,
                RateSubject::PreauthIp(network.client_ip, rates.preauth_ipv6_prefix_bits),
                SHARED_RATE_OPERATION,
                "preauth",
                rates.preauth_ip,
                rates.lock_timeout,
            )
            .await;
            if result == Err(ErrorCode::RateLimited) {
                self.metrics
                    .increment(&families::RATE_LIMIT_REJECTED_TOTAL, &["ip"]);
            }
            result
        };
        if let Err(code) = result {
            self.audit(
                context,
                &network,
                None,
                McpAuditAction::McpRequestDenied,
                "protocol",
                Some(code),
            )
            .await?;
        }
        result
    }

    /// Authenticated protocol/control traffic is rate-limited but has no BMO reservation.
    pub async fn protocol(
        &self,
        context: &McpHttpContext,
        operation: McpOperation,
    ) -> Result<(), ErrorCode> {
        if matches!(operation, McpOperation::Preflight) {
            return self.preflight(context).await;
        }
        let network = self.network(context);
        let credential = self.authenticate(context, &network).await?;
        let auth = credential.identity();
        let result = if !self.network_allowed(&network, Some(auth)) {
            Err(ErrorCode::Forbidden)
        } else {
            self.postauth_rate(auth, "protocol").await
        };
        self.audit(
            context,
            &network,
            Some(auth),
            if result.is_ok() {
                McpAuditAction::McpRequestFinished
            } else {
                McpAuditAction::McpRequestDenied
            },
            "protocol",
            result.as_ref().err().copied(),
        )
        .await?;
        result
    }

    /// ADR-0065 D-D: credential/shared, user (when present), tenant, credential/operation — the evaluation order of
    /// the former four transactions; the repository takes their locks in tier order.
    async fn postauth_rate(
        &self,
        auth: &AuthorizationScope,
        operation: &str,
    ) -> Result<(), ErrorCode> {
        let rates = &self.settings.rates;
        let credential = || RateSubject::Credential {
            auth,
            credential_id: auth.principal().0,
        };
        let charge = |subject, operation, bucket_key, policy| RateCharge {
            subject,
            operation,
            bucket_key,
            policy,
        };
        let mut charges = vec![(
            charge(
                credential(),
                SHARED_RATE_OPERATION,
                "credential",
                rates.credential,
            ),
            "credential",
        )];
        if auth.user_id().is_some() {
            charges.push((
                charge(
                    RateSubject::User(auth),
                    SHARED_RATE_OPERATION,
                    "user",
                    rates.user,
                ),
                "user",
            ));
        }
        charges.push((
            charge(
                RateSubject::Tenant(auth),
                SHARED_RATE_OPERATION,
                "tenant",
                rates.tenant,
            ),
            "tenant",
        ));
        charges.push((
            charge(credential(), operation, "operation", rates.operation),
            "operation",
        ));
        self.rate(charges).await
    }

    /// Only local reads with no external provider cost can use this entry.
    /// The fixed dispatch table must not send writes or provider calls here.
    pub(crate) async fn run_local_read<T, F, Fut>(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        requested_workspace: Option<WorkspaceId>,
        raw_arguments: &str,
        handler: F,
    ) -> Result<T, ErrorCode>
    where
        F: FnOnce(AuthorizedRequest) -> Fut,
        Fut: Future<Output = Result<T, ErrorCode>>,
    {
        let result = async {
            let admitted = self.admit(context, operation, requested_workspace).await?;
            if !matches!(
                operation.operation_key(),
                "context.assemble" | "memory.get" | "memory.enumerate"
            ) {
                return self
                    .denied(
                        context,
                        operation,
                        &admitted,
                        ErrorCode::DependencyUnavailable,
                    )
                    .await;
            }
            self.read_admitted(context, operation, raw_arguments, &admitted, handler)
                .await
        }
        .await;
        self.record_request(operation, &result);
        result
    }

    /// A non-destructive admitted write (§6.1.3 / ADR-0028 D-F: the `SubjectWriteOp` registry
    /// ops — `memory.subject_register` / `memory.subject_link_key`). Same admission, rate,
    /// entitlement, ordinary BMO reservation/settlement and finished audit as a local read; no
    /// confirm gate because nothing is deleted, superseded or hidden. The handler owns its own
    /// transaction; an error there settles as a refused write (reservation released).
    pub(crate) async fn run_local_write<T, F, Fut>(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        requested_workspace: Option<WorkspaceId>,
        raw_arguments: &str,
        handler: F,
    ) -> Result<T, ErrorCode>
    where
        F: FnOnce(AuthorizedRequest) -> Fut,
        Fut: Future<Output = Result<T, ErrorCode>>,
    {
        let result = async {
            let admitted = self.admit(context, operation, requested_workspace).await?;
            // ADR-0028 D-F / ADR-0030 D-C: the two closed non-destructive write sets (subject
            // registry ops, affect annotation) — never a generic "any write" admission.
            if (SubjectWriteOp::parse_operation_key(operation.operation_key()).is_none()
                && AffectWriteOp::parse_operation_key(operation.operation_key()).is_none())
                || operation.meter_kind() != MeterKind::Ordinary
            {
                return self
                    .denied(
                        context,
                        operation,
                        &admitted,
                        ErrorCode::DependencyUnavailable,
                    )
                    .await;
            }
            self.read_admitted(context, operation, raw_arguments, &admitted, handler)
                .await
        }
        .await;
        self.record_request(operation, &result);
        result
    }

    /// Continuity keeps the raw optional workspace only as a database result filter. The
    /// credential binding still narrows the trusted scope, but the tool argument cannot take
    /// the generic pre-database `AuthorizationScope::narrow` path and disclose a 403 oracle.
    pub(crate) async fn run_continuity_read<T, F, Fut>(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        requested_workspace: Option<WorkspaceId>,
        raw_arguments: &str,
        handler: F,
    ) -> Result<T, ErrorCode>
    where
        F: FnOnce(AuthorizedRequest) -> Fut,
        Fut: Future<Output = Result<T, ErrorCode>>,
    {
        let result = async {
            let admitted = self
                .admit_with_workspace_policy(
                    context,
                    operation,
                    requested_workspace,
                    WorkspaceAdmission::PreserveContinuityFilter,
                )
                .await?;
            if operation.operation_key() != "continuity.get" {
                return self
                    .denied(
                        context,
                        operation,
                        &admitted,
                        ErrorCode::DependencyUnavailable,
                    )
                    .await;
            }
            self.read_admitted(context, operation, raw_arguments, &admitted, handler)
                .await
        }
        .await;
        self.record_request(operation, &result);
        result
    }

    /// Test-only admission seam for a trusted authn/authz result. The real HTTP path still
    /// authenticates the service credential; this helper only lets the unit test exercise the
    /// frozen multi-workspace scope through the same continuity read, reservation, and finalize
    /// path without adding a production injection point.
    #[cfg(test)]
    pub(crate) async fn run_continuity_read_with_trusted_scope<T, F, Fut>(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        requested_workspace: Option<WorkspaceId>,
        raw_arguments: &str,
        authorization: AuthorizationScope,
        handler: F,
    ) -> Result<T, ErrorCode>
    where
        F: FnOnce(AuthorizedRequest) -> Fut,
        Fut: Future<Output = Result<T, ErrorCode>>,
    {
        let result = async {
            let mut admitted = self
                .admit_with_workspace_policy(
                    context,
                    operation,
                    requested_workspace,
                    WorkspaceAdmission::PreserveContinuityFilter,
                )
                .await?;
            admitted.request.authorization = authorization;
            if operation.operation_key() != "continuity.get" {
                return self
                    .denied(
                        context,
                        operation,
                        &admitted,
                        ErrorCode::DependencyUnavailable,
                    )
                    .await;
            }
            self.read_admitted(context, operation, raw_arguments, &admitted, handler)
                .await
        }
        .await;
        self.record_request(operation, &result);
        result
    }

    /// Authenticated retrieval read. External provider admission, persistent provider budget,
    /// egress authorization, and disclosure provenance remain inside the injected
    /// `EmbeddingProvider`; this guard owns request admission and ordinary BMO settlement.
    pub(crate) async fn run_retrieval_read<T, F, Fut>(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        requested_workspace: Option<WorkspaceId>,
        raw_arguments: &str,
        handler: F,
    ) -> Result<T, ErrorCode>
    where
        F: FnOnce(AuthorizedRequest) -> Fut,
        Fut: Future<Output = Result<T, ErrorCode>>,
    {
        let result = async {
            let admitted = self.admit(context, operation, requested_workspace).await?;
            if operation.operation_key() != "recall.search" {
                return self
                    .denied(
                        context,
                        operation,
                        &admitted,
                        ErrorCode::DependencyUnavailable,
                    )
                    .await;
            }
            self.read_admitted(context, operation, raw_arguments, &admitted, handler)
                .await
        }
        .await;
        self.record_request(operation, &result);
        result
    }

    /// A local write, its BMO settlement and its success audit share one commit.
    /// An unknown commit outcome is resolved by replaying the same logical key.
    pub(crate) async fn run_atomic_remember<F>(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        requested_workspace: Option<WorkspaceId>,
        raw_arguments: &str,
        idempotency_key: String,
        prepare: F,
    ) -> Result<AtomicRememberResult, ErrorCode>
    where
        F: FnOnce(&AuthorizedRequest) -> Result<RememberCommand, ErrorCode>,
    {
        let result = async {
            let admitted = self.admit(context, operation, requested_workspace).await?;
            if operation.operation_key() != "remember.put"
                || operation.meter_kind() != MeterKind::Ordinary
            {
                return self
                    .denied(
                        context,
                        operation,
                        &admitted,
                        ErrorCode::DependencyUnavailable,
                    )
                    .await;
            }
            let command = match prepare(&admitted.request) {
                Ok(command) => command,
                Err(code) => return self.denied(context, operation, &admitted, code).await,
            };
            let finished_audit = self.audit_event(
                context,
                &admitted.network,
                Some(admitted.request.authorization()),
                McpAuditAction::McpRequestFinished,
                operation.operation_key(),
                None,
            );
            let request = AtomicRememberRequest {
                request_id: context.request_id(),
                idempotency_key,
                request_fingerprint: hex::encode(Sha256::digest(raw_arguments.as_bytes())),
                workspace_id: admitted.request.workspace_id(),
                reservation_ttl: self.settings.reservation_ttl,
                replay_ttl: self.settings.replay_ttl,
                command,
                finished_audit,
            };
            let result = tokio::time::timeout(
                self.settings.handler_timeout,
                operation_receipt::remember_atomically(
                    &self.pool,
                    admitted.request.authorization(),
                    request,
                ),
            )
            .await
            .unwrap_or(Err(ErrorCode::DependencyUnavailable));
            match &result {
                Ok(outcome) if !outcome.replayed => {
                    self.metrics
                        .increment(&families::MCP_QUOTA_RESERVATIONS_TOTAL, &["ok"]);
                    self.metrics
                        .increment(&families::MCP_BMO_CONSUMED_TOTAL, &[PLAN_CLASS]);
                }
                // A replay is a newly authenticated request, but never another business write/BMO.
                Ok(_) => {
                    self.audit(
                        context,
                        &admitted.network,
                        Some(admitted.request.authorization()),
                        McpAuditAction::McpRequestFinished,
                        operation.operation_key(),
                        None,
                    )
                    .await?
                }
                Err(ErrorCode::DependencyUnavailable) => {
                    self.observe_unknown(context, operation, &admitted).await;
                }
                Err(code) => {
                    self.audit(
                        context,
                        &admitted.network,
                        Some(admitted.request.authorization()),
                        McpAuditAction::McpRequestFinished,
                        operation.operation_key(),
                        Some(*code),
                    )
                    .await?
                }
            }
            result
        }
        .await;
        self.record_request(operation, &result);
        result
    }

    /// A confirm-gated local write (§33.10 rule 9, ADR-0018). Same admission as every other
    /// route; the first call mints a token (one transaction: token row + finished audit, no
    /// other durable write, no BMO); the second call runs `handler` under the handler
    /// timeout with the claim it must consume in its own transaction. The token binds the
    /// routed workspace (ADR-0054): presented in another workspace it matches nothing (CONFLICT).
    pub(crate) async fn run_confirmed_write<T, F, Fut>(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        requested_workspace: Option<WorkspaceId>,
        raw_arguments: &str,
        gate: ConfirmGate,
        handler: F,
    ) -> Result<ConfirmedOutcome<T>, ErrorCode>
    where
        F: FnOnce(ConfirmedWrite) -> Fut,
        Fut: Future<Output = Result<T, ErrorCode>>,
    {
        let result = async {
            let admitted = self.admit(context, operation, requested_workspace).await?;
            if DestructiveOp::parse_operation_key(operation.operation_key()) != Some(gate.op)
                || operation.meter_kind() != MeterKind::Ordinary
            {
                return self
                    .denied(
                        context,
                        operation,
                        &admitted,
                        ErrorCode::DependencyUnavailable,
                    )
                    .await;
            }
            // D-A: a token binds to a real user; a headless credential cannot confirm.
            if admitted.request.authorization().user_id().is_none() {
                return self
                    .denied(context, operation, &admitted, ErrorCode::Forbidden)
                    .await;
            }
            let finished_audit = self.audit_event(
                context,
                &admitted.network,
                Some(admitted.request.authorization()),
                McpAuditAction::McpRequestFinished,
                operation.operation_key(),
                None,
            );
            let Some(token) = gate.presented else {
                // §77: the mint's finished audit must not read as an executed write.
                let mut mint_audit = finished_audit;
                mint_audit
                    .risk_tags
                    .push(RISK_TAG_CONFIRMATION_MINTED.to_owned());
                return match self.mint_confirmation(&admitted, &gate, &mint_audit).await {
                    Ok((token, expires_at)) => {
                        Ok(ConfirmedOutcome::ConfirmationRequired { token, expires_at })
                    }
                    Err(code) => self.denied(context, operation, &admitted, code).await,
                };
            };
            let write = ConfirmedWrite {
                request: admitted.request.clone(),
                claim: ConfirmationClaim {
                    op: gate.op,
                    target_id: gate.target_id,
                    successor_id: gate.successor_id,
                    nonce_sha256: token.sha256(),
                },
                request_fingerprint: hex::encode(Sha256::digest(raw_arguments.as_bytes())),
                reservation_ttl: self.settings.reservation_ttl,
                finished_audit,
            };
            let result = tokio::time::timeout(self.settings.handler_timeout, handler(write))
                .await
                .unwrap_or(Err(ErrorCode::DependencyUnavailable));
            match &result {
                Ok(_) => {
                    self.metrics
                        .increment(&families::MCP_QUOTA_RESERVATIONS_TOTAL, &["ok"]);
                    self.metrics
                        .increment(&families::MCP_BMO_CONSUMED_TOTAL, &[PLAN_CLASS]);
                }
                Err(ErrorCode::DependencyUnavailable) => {
                    self.observe_unknown(context, operation, &admitted).await;
                }
                Err(code) => {
                    self.audit(
                        context,
                        &admitted.network,
                        Some(admitted.request.authorization()),
                        McpAuditAction::McpRequestFinished,
                        operation.operation_key(),
                        Some(*code),
                    )
                    .await?;
                }
            }
            result.map(ConfirmedOutcome::Executed)
        }
        .await;
        self.record_request(operation, &result);
        result
    }

    /// D-B first call: mint the nonce, persist only its digest + the finished audit. ADR-0054
    /// D-C: the token binds the routed workspace — an admitted request without one (an unbound
    /// PAT on a confirm-gated op, whose schema carries no `workspace_id`) is refused before any
    /// row is written, with the code its second leg would answer.
    async fn mint_confirmation(
        &self,
        admitted: &AdmittedOperation,
        gate: &ConfirmGate,
        finished_audit: &AuditEvent,
    ) -> Result<(ConfirmToken, OffsetDateTime), ErrorCode> {
        let workspace = admitted
            .request
            .workspace_id()
            .ok_or(ErrorCode::DependencyUnavailable)?;
        let authorization = admitted.request.authorization().narrow(workspace)?;
        let token = humaux_application::supersede::mint_confirm_token();
        let minted = tokio::time::timeout(
            self.settings.handler_timeout,
            confirm_token_repo::mint_with_audit(
                &self.pool,
                &authorization,
                gate.op,
                gate.target_id,
                gate.successor_id,
                gate.ttl,
                token.sha256(),
                finished_audit,
            ),
        )
        .await
        .unwrap_or(Err(ErrorCode::DependencyUnavailable))?;
        Ok((token, minted.expires_at))
    }

    async fn observe_unknown(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        admitted: &AdmittedOperation,
    ) {
        // Timeout or a lost COMMIT acknowledgement does not prove rollback.
        // This observation is not a terminal business outcome; the receipt
        // remains authoritative when the client retries the same logical key.
        let mut observation = self.audit_event(
            context,
            &admitted.network,
            Some(admitted.request.authorization()),
            McpAuditAction::McpRequestOutcomeUnknown,
            operation.operation_key(),
            None,
        );
        observation.result = "UNKNOWN".to_owned();
        // An unavailable database may also prevent this observation. Never
        // replace the retryable unknown outcome with a purported failure.
        let _ = tokio::time::timeout(
            self.settings.finalize_timeout,
            request_guard_repo::audit_event_insert(
                &self.pool,
                AuditTenant::Authenticated(admitted.request.authorization()),
                &observation,
            ),
        )
        .await;
    }

    /// Schema validity never silently turns an unimplemented action into a successful no-op.
    pub(crate) async fn reject_unsupported(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        requested_workspace: Option<WorkspaceId>,
    ) -> Result<(), ErrorCode> {
        let result = async {
            let admitted = self.admit(context, operation, requested_workspace).await?;
            self.denied(
                context,
                operation,
                &admitted,
                ErrorCode::DependencyUnavailable,
            )
            .await
        }
        .await;
        self.record_request(operation, &result);
        result
    }

    fn record_request<T>(&self, operation: &OperationDescriptor, result: &Result<T, ErrorCode>) {
        self.metrics.increment(
            &families::HUMAUX_MCP_REQUESTS_TOTAL,
            &[
                operation.tool().as_str(),
                result.as_ref().err().map(|e| e.as_str()).unwrap_or("ok"),
                PLAN_CLASS,
            ],
        );
    }

    async fn admit(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        requested_workspace: Option<WorkspaceId>,
    ) -> Result<AdmittedOperation, ErrorCode> {
        self.admit_with_workspace_policy(
            context,
            operation,
            requested_workspace,
            WorkspaceAdmission::NarrowRequested,
        )
        .await
    }

    async fn admit_with_workspace_policy(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        requested_workspace: Option<WorkspaceId>,
        workspace_policy: WorkspaceAdmission,
    ) -> Result<AdmittedOperation, ErrorCode> {
        let network = self.network(context);
        let credential = self.authenticate(context, &network).await?;
        let required_scope = CredentialScope::parse(operation.required_scope())?;
        let authorization_filter = match workspace_policy {
            WorkspaceAdmission::NarrowRequested => requested_workspace,
            WorkspaceAdmission::PreserveContinuityFilter => None,
        };
        let auth = match credential.authorize(required_scope, authorization_filter) {
            Ok(auth) => auth,
            Err(code) => {
                self.metrics
                    .increment(&families::MCP_AUTHZ_DENIED_TOTAL, &[code.as_str()]);
                self.audit(
                    context,
                    &network,
                    Some(credential.identity()),
                    McpAuditAction::McpScopeDenied,
                    operation.operation_key(),
                    Some(code),
                )
                .await?;
                return Err(code);
            }
        };
        let routed_workspace = match workspace_policy {
            WorkspaceAdmission::NarrowRequested => {
                requested_workspace.or(credential.bound_workspace_id())
            }
            WorkspaceAdmission::PreserveContinuityFilter => requested_workspace,
        };
        let request = AuthorizedRequest {
            authorization: auth,
            workspace_id: routed_workspace,
            request_id: context.request_id(),
        };
        let policy_result = async {
            if !self.network_allowed(&network, Some(request.authorization())) {
                return Err(ErrorCode::Forbidden);
            }
            self.postauth_rate(request.authorization(), operation.operation_key())
                .await?;
            let facts = request_guard_repo::read_effective_entitlements(
                &self.pool,
                request.authorization(),
            )
            .await?;
            let policy = quota_policy(&facts, operation.feature_key())?;
            if matches!(
                operation.meter_kind(),
                MeterKind::BatchReservation | MeterKind::BatchConsumption
            ) {
                return Err(ErrorCode::DependencyUnavailable);
            }
            Ok(policy)
        }
        .await;
        match policy_result {
            Ok(charge_policy) => Ok(AdmittedOperation {
                request,
                network,
                charge_policy,
            }),
            Err(code) => {
                self.audit(
                    context,
                    &network,
                    Some(request.authorization()),
                    McpAuditAction::McpRequestDenied,
                    operation.operation_key(),
                    Some(code),
                )
                .await?;
                Err(code)
            }
        }
    }

    async fn denied<T>(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        admitted: &AdmittedOperation,
        code: ErrorCode,
    ) -> Result<T, ErrorCode> {
        self.audit(
            context,
            &admitted.network,
            Some(admitted.request.authorization()),
            McpAuditAction::McpRequestDenied,
            operation.operation_key(),
            Some(code),
        )
        .await?;
        Err(code)
    }

    async fn read_admitted<T, F, Fut>(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        admitted: &AdmittedOperation,
        handler: F,
    ) -> Result<T, ErrorCode>
    where
        F: FnOnce(AuthorizedRequest) -> Fut,
        Fut: Future<Output = Result<T, ErrorCode>>,
    {
        let reservation_started = Instant::now();
        let reservation = match self
            .reserve_read(context, operation, raw_arguments, admitted)
            .await
        {
            Ok(value) => value,
            Err(code) => return self.denied(context, operation, admitted, code).await,
        };
        let execution_limit = reservation
            .as_ref()
            .map(|r| {
                r.valid_for()
                    .saturating_sub(reservation_started.elapsed())
                    .saturating_sub(self.settings.finalize_timeout)
                    .min(self.settings.handler_timeout)
            })
            .unwrap_or(self.settings.handler_timeout);
        let result = if execution_limit.is_zero() {
            Err(ErrorCode::Conflict)
        } else {
            tokio::time::timeout(execution_limit, handler(admitted.request.clone()))
                .await
                .unwrap_or(Err(ErrorCode::DependencyUnavailable))
        };
        let consume = !execution_limit.is_zero() && admitted.charge_policy.consumes(&result);
        let completion = ReadCompletion {
            reservation: reservation.as_ref(),
            consume,
            error: result.as_ref().err().copied(),
        };
        let settled = tokio::time::timeout(
            self.settings.finalize_timeout,
            self.finish_read(context, operation, admitted, completion),
        )
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)??;
        if consume && !settled {
            return Err(ErrorCode::Conflict);
        }
        result
    }

    async fn reserve_read(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        admitted: &AdmittedOperation,
    ) -> Result<Option<QuotaReservation>, ErrorCode> {
        if operation.meter_kind() == MeterKind::Zero {
            return Ok(None);
        }
        let event = self.audit_event(
            context,
            &admitted.network,
            Some(admitted.request.authorization()),
            McpAuditAction::McpQuotaReserved,
            operation.operation_key(),
            None,
        );
        let digest = hex::encode(Sha256::digest(raw_arguments.as_bytes()));
        let result = tokio::time::timeout(
            self.settings.finalize_timeout,
            request_guard_repo::reserve_read_with_audit(
                &self.pool,
                admitted.request.authorization(),
                operation.operation_key(),
                &digest,
                self.settings.reservation_ttl,
                &event,
            ),
        )
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?
        .map(Some);
        self.metrics.increment(
            &families::MCP_QUOTA_RESERVATIONS_TOTAL,
            &[if result.is_ok() { "ok" } else { "denied" }],
        );
        result
    }

    async fn finish_read(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        admitted: &AdmittedOperation,
        completion: ReadCompletion<'_>,
    ) -> Result<bool, ErrorCode> {
        let event = self.audit_event(
            context,
            &admitted.network,
            Some(admitted.request.authorization()),
            McpAuditAction::McpRequestFinished,
            operation.operation_key(),
            completion.error,
        );
        let settled = request_guard_repo::settle_read_with_audit(
            &self.pool,
            admitted.request.authorization(),
            completion.reservation,
            completion.consume,
            &event,
        )
        .await?;
        if settled && completion.reservation.is_some() && completion.consume {
            self.metrics
                .increment(&families::MCP_BMO_CONSUMED_TOTAL, &[PLAN_CLASS]);
        }
        Ok(settled)
    }
    async fn audit(
        &self,
        context: &McpHttpContext,
        network: &ClientNetworkIdentity,
        auth: Option<&AuthorizationScope>,
        action: McpAuditAction,
        operation: &str,
        error: Option<ErrorCode>,
    ) -> Result<(), ErrorCode> {
        let event = self.audit_event(context, network, auth, action, operation, error);
        request_guard_repo::audit_event_insert(
            &self.pool,
            auth.map(AuditTenant::Authenticated)
                .unwrap_or(AuditTenant::System),
            &event,
        )
        .await?;
        Ok(())
    }

    fn audit_event(
        &self,
        context: &McpHttpContext,
        network: &ClientNetworkIdentity,
        auth: Option<&AuthorizationScope>,
        action: McpAuditAction,
        operation: &str,
        error: Option<ErrorCode>,
    ) -> AuditEvent {
        AuditEvent {
            event_id: AuditEventId::new(),
            ts: SystemTime::now(),
            tenant_id: auth
                .map(|auth| auth.tenant_id())
                .unwrap_or(SYSTEM_TENANT_ID),
            actor_type: if auth.is_some() {
                "SERVICE_CREDENTIAL"
            } else {
                "ANONYMOUS"
            }
            .to_owned(),
            actor_id: auth
                .map(|auth| auth.principal().0.to_string())
                .unwrap_or_else(|| "anonymous".to_owned()),
            action: action.as_str().to_owned(),
            resource_type: "MCP_OPERATION".to_owned(),
            resource_id: operation.to_owned(),
            result: error.map(|code| code.as_str()).unwrap_or("OK").to_owned(),
            request_id: context.request_id().to_string(),
            trace_id: context.request_id().to_string(),
            client_ip: network.client_ip.to_string(),
            user_agent_hash: String::new(),
            risk_tags: network.risk_tags.clone(),
            before_fingerprint: None,
            after_fingerprint: None,
            metadata: AuditMetadata::new(),
        }
    }
}

enum ChargePolicy {
    SuccessOnly,
    CompletedBusinessCalls,
}

impl ChargePolicy {
    fn consumes<T>(&self, result: &Result<T, ErrorCode>) -> bool {
        result.is_ok()
            || (matches!(self, Self::CompletedBusinessCalls)
                && matches!(
                    result,
                    Err(ErrorCode::NotFound
                        | ErrorCode::Conflict
                        | ErrorCode::ProjectionLag
                        | ErrorCode::CannotEstablishCompleteness)
                ))
    }
}

fn quota_policy(
    facts: &EffectiveEntitlementFacts,
    feature: &str,
) -> Result<ChargePolicy, ErrorCode> {
    if feature != quota_repo::BMO_ENTITLEMENT {
        return Err(ErrorCode::Internal);
    }
    let policy = facts
        .effective
        .get(feature)
        .and_then(|v| v.as_object())
        .ok_or(ErrorCode::EntitlementRequired)?;
    if policy
        .get("limit")
        .and_then(|v| v.as_u64())
        .is_none_or(|n| n > i64::MAX as u64)
        || !matches!(
            policy.get("period").and_then(|v| v.as_str()),
            Some("calendar_month" | "subscription_period")
        )
    {
        return Err(ErrorCode::EntitlementRequired);
    }
    let parse = |key| {
        policy
            .get(key)
            .and_then(|v| v.as_str())
            .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
            .ok_or(ErrorCode::EntitlementRequired)
    };
    let start = parse("period_start")?;
    let end = parse("period_end")?;
    if start >= end
        || facts.observed_at < start
        || facts.observed_at >= end
        || facts.computed_at > facts.observed_at
    {
        return Err(ErrorCode::EntitlementRequired);
    }
    match policy.get("charge_policy").and_then(|v| v.as_str()) {
        Some("success_only") => Ok(ChargePolicy::SuccessOnly),
        Some("completed_business_calls") => Ok(ChargePolicy::CompletedBusinessCalls),
        _ => Err(ErrorCode::EntitlementRequired),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn snapshot() -> EffectiveEntitlementFacts {
        let observed_at = OffsetDateTime::parse("2026-08-28T00:00:00Z", &Rfc3339).unwrap();
        EffectiveEntitlementFacts {
            effective: json!({quota_repo::BMO_ENTITLEMENT: {
                "limit": 50,
                "period": "subscription_period",
                "period_start": "2026-08-28T00:00:00Z",
                "period_end": "2026-09-28T00:00:00Z",
                "charge_policy": "success_only"
            }}),
            source_grant_ids: vec![],
            computed_at: observed_at,
            observed_at,
        }
    }

    #[test]
    fn entitlement_uses_projected_period_and_database_observation_time() {
        let mut facts = snapshot();
        assert!(quota_policy(&facts, quota_repo::BMO_ENTITLEMENT).is_ok());
        facts.computed_at += time::Duration::seconds(1);
        assert!(matches!(
            quota_policy(&facts, quota_repo::BMO_ENTITLEMENT),
            Err(ErrorCode::EntitlementRequired)
        ));
        facts = snapshot();
        facts.observed_at = OffsetDateTime::parse("2026-09-28T00:00:00Z", &Rfc3339).unwrap();
        assert!(matches!(
            quota_policy(&facts, quota_repo::BMO_ENTITLEMENT),
            Err(ErrorCode::EntitlementRequired)
        ));
        facts = snapshot();
        facts.effective[quota_repo::BMO_ENTITLEMENT] = json!(50);
        assert!(matches!(
            quota_policy(&facts, quota_repo::BMO_ENTITLEMENT),
            Err(ErrorCode::EntitlementRequired)
        ));
    }

    #[test]
    fn charge_policy_does_not_bill_infrastructure_or_authorization_failures() {
        let ok: Result<(), ErrorCode> = Ok(());
        assert!(ChargePolicy::SuccessOnly.consumes(&ok));
        assert!(!ChargePolicy::SuccessOnly.consumes(&Err::<(), _>(ErrorCode::NotFound)));
        assert!(ChargePolicy::CompletedBusinessCalls.consumes(&Err::<(), _>(ErrorCode::NotFound)));
        for code in [
            ErrorCode::Unauthorized,
            ErrorCode::InvalidInput,
            ErrorCode::Internal,
            ErrorCode::DependencyUnavailable,
        ] {
            assert!(!ChargePolicy::CompletedBusinessCalls.consumes(&Err::<(), _>(code)));
        }
    }

    /// Exhaustive over `ToolName`: a new variant does not compile until it is placed in `TOOLS`.
    #[test]
    fn tools_list_every_variant_once() {
        let slot = |tool: ToolName| match tool {
            ToolName::Remember => 0,
            ToolName::Recall => 1,
            ToolName::Memory => 2,
            ToolName::Context => 3,
            ToolName::Continuity => 4,
            ToolName::Artifact => 5,
            ToolName::Code => 6,
            ToolName::Coordinate => 7,
        };
        for (i, tool) in TOOLS.iter().enumerate() {
            assert_eq!(slot(*tool), i, "{tool:?}");
        }
    }

    fn series(out: &str, family: &str) -> Vec<String> {
        out.lines()
            .filter(|l| {
                l.starts_with(&format!("{family}{{")) || l.starts_with(&format!("{family} "))
            })
            .map(str::to_owned)
            .collect()
    }

    /// ADR-0061 D-A seeding: every guard family has HELP/TYPE and its full closed product at zero
    /// state, with exactly the §41.2 label keys. Fault: seed one family from an empty set ⇒ red.
    #[test]
    fn render_seeds_every_guard_family_over_its_closed_sets() {
        let mut out = String::new();
        GuardMetrics::default().render(&mut out);
        let expected = [
            ("humaux_mcp_requests_total", 8 * (ErrorCode::ALL.len() + 1)),
            ("mcp_auth_attempts_total", 4),
            ("mcp_authz_denied_total", ErrorCode::ALL.len()),
            ("mcp_quota_reservations_total", 2),
            ("mcp_bmo_consumed_total", 1),
            ("rate_limit_rejected_total", 5),
        ];
        for (family, n) in expected {
            assert!(
                out.contains(&format!("# TYPE {family} counter\n")),
                "{family}"
            );
            let lines = series(&out, family);
            assert_eq!(lines.len(), n, "{family}: {lines:?}");
            let f = GUARD_FAMILIES
                .iter()
                .find(|f| f.name == family)
                .expect("guard family");
            for line in &lines {
                let keys: Vec<&str> = line[family.len() + 1..line.find('}').expect("labels")]
                    .split(',')
                    .map(|kv| kv.split('=').next().expect("key"))
                    .collect();
                assert_eq!(keys, f.labels, "{line}");
                assert!(line.ends_with(" 0"), "{line}");
            }
        }
    }

    /// An increment lands on its seeded series, and a value outside the seed set still renders.
    #[test]
    fn increments_render_on_their_series() {
        let metrics = GuardMetrics::default();
        metrics.increment(&families::MCP_AUTHZ_DENIED_TOTAL, &["FORBIDDEN"]);
        metrics.increment(&families::MCP_AUTHZ_DENIED_TOTAL, &["FORBIDDEN"]);
        metrics.increment(&families::MCP_AUTH_ATTEMPTS_TOTAL, &["ok", AUTH_FLOW]);
        metrics.increment(&families::RATE_LIMIT_REJECTED_TOTAL, &["unseeded"]);
        let mut out = String::new();
        metrics.render(&mut out);
        assert!(
            out.contains("mcp_authz_denied_total{reason=\"FORBIDDEN\"} 2\n"),
            "{out}"
        );
        assert!(
            out.contains("mcp_auth_attempts_total{result=\"ok\",flow=\"service_credential\"} 1\n"),
            "{out}"
        );
        assert!(
            out.contains("rate_limit_rejected_total{scope=\"unseeded\"} 1\n"),
            "{out}"
        );
        assert_eq!(series(&out, "rate_limit_rejected_total").len(), 6);
    }
}
