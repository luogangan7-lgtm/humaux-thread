//! §83: one business admission path for every canonical MCP operation.
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
    quota_repo::{self, QuotaReservation, RatePolicy, RateSubject},
    remember::RememberCommand,
    request_guard_repo::{self, AuditTenant, EffectiveEntitlementFacts},
};
use humaux_domain::{
    audit::{AuditEvent, AuditEventId, AuditMetadata, McpAuditAction, SYSTEM_TENANT_ID},
    confirm::{ConfirmToken, DestructiveOp, RISK_TAG_CONFIRMATION_MINTED},
    error::ErrorCode,
    identity::AuthorizationScope,
    ids::WorkspaceId,
};
use humaux_protocol::{
    edge::{
        Cidr, ClientNetworkIdentity, IpPolicyDecision, IpPolicyInput, TrustedProxyConfig,
        build_client_network_identity, evaluate_ip_policy,
    },
    mcp::{McpHttpContext, McpOperation},
    mcp_catalog::{MeterKind, OperationDescriptor},
};
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
/// All family/label strings originate from this module or closed protocol/error enums.
#[derive(Default)]
pub struct GuardMetrics {
    counters: Mutex<BTreeMap<String, u64>>,
}

impl GuardMetrics {
    fn increment(&self, family: &str, labels: &[(&str, &str)]) {
        let labels = labels
            .iter()
            .map(|(key, value)| format!("{key}=\"{value}\""))
            .collect::<Vec<_>>()
            .join(",");
        let key = format!("{family}{{{labels}}}");
        let mut counters = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        let count = counters.entry(key).or_default();
        *count = count.saturating_add(1);
    }

    pub fn exposition(&self) -> String {
        self.counters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(key, count)| format!("{key} {count}\n"))
            .collect()
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

    async fn rate(
        &self,
        subject: RateSubject<'_>,
        operation: &str,
        bucket: &str,
        policy: RatePolicy,
        scope: &'static str,
    ) -> Result<(), ErrorCode> {
        let result = quota_repo::consume_rate(&self.pool, subject, operation, bucket, policy).await;
        if result == Err(ErrorCode::RateLimited) {
            self.metrics
                .increment("rate_limit_rejected_total", &[("scope", scope)]);
        }
        result
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
                    network.client_ip,
                    SystemTime::now(),
                )
                .await
            }
            None => Err(ErrorCode::Unauthorized),
        };
        self.metrics.increment(
            "mcp_auth_attempts_total",
            &[
                (
                    "result",
                    if result.is_ok() {
                        "ok"
                    } else {
                        "bad_credential"
                    },
                ),
                ("flow", "service_credential"),
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
            self.rate(
                RateSubject::PreauthIp(network.client_ip),
                "mcp",
                "preauth",
                self.settings.rates.preauth_ip,
                "ip",
            )
            .await
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

    async fn postauth_rate(
        &self,
        auth: &AuthorizationScope,
        operation: &str,
    ) -> Result<(), ErrorCode> {
        self.rate(
            RateSubject::Credential {
                auth,
                credential_id: auth.principal().0,
            },
            "mcp",
            "credential",
            self.settings.rates.credential,
            "credential",
        )
        .await?;
        if auth.user_id().is_some() {
            self.rate(
                RateSubject::User(auth),
                "mcp",
                "user",
                self.settings.rates.user,
                "user",
            )
            .await?;
        }
        self.rate(
            RateSubject::Tenant(auth),
            "mcp",
            "tenant",
            self.settings.rates.tenant,
            "tenant",
        )
        .await?;
        self.rate(
            RateSubject::Credential {
                auth,
                credential_id: auth.principal().0,
            },
            operation,
            "operation",
            self.settings.rates.operation,
            "operation",
        )
        .await
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
                        .increment("mcp_quota_reservations_total", &[("result", "ok")]);
                    self.metrics
                        .increment("mcp_bmo_consumed_total", &[("plan_class", "unclassified")]);
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
    /// timeout with the claim it must consume in its own transaction.
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
                        .increment("mcp_quota_reservations_total", &[("result", "ok")]);
                    self.metrics
                        .increment("mcp_bmo_consumed_total", &[("plan_class", "unclassified")]);
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

    /// D-B first call: mint the nonce, persist only its digest + the finished audit.
    async fn mint_confirmation(
        &self,
        admitted: &AdmittedOperation,
        gate: &ConfirmGate,
        finished_audit: &AuditEvent,
    ) -> Result<(ConfirmToken, OffsetDateTime), ErrorCode> {
        let token = humaux_application::supersede::mint_confirm_token();
        let minted = tokio::time::timeout(
            self.settings.handler_timeout,
            confirm_token_repo::mint_with_audit(
                &self.pool,
                admitted.request.authorization(),
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
            "humaux_mcp_requests_total",
            &[
                ("tool", operation.tool().as_str()),
                (
                    "result",
                    result.as_ref().err().map(|e| e.as_str()).unwrap_or("ok"),
                ),
                ("plan_class", "unclassified"),
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
                    .increment("mcp_authz_denied_total", &[("reason", code.as_str())]);
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
            "mcp_quota_reservations_total",
            &[("result", if result.is_ok() { "ok" } else { "denied" })],
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
                .increment("mcp_bmo_consumed_total", &[("plan_class", "unclassified")]);
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
}
