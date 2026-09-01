//! Gateway implementation of the native MCP application port.
//!
//! The canonical catalog validates wire arguments before this fixed dispatch
//! table runs.  Only routes with a complete local implementation appear here;
//! every other valid contract is admitted and denied by [`GatewayGuard`] rather
//! than becoming a successful no-op.

use std::sync::Arc;

use async_trait::async_trait;
use humaux_adapters::{context_repo::MemoryEnumerationParams, postgres::RuntimeDbPool};
use humaux_domain::{
    authority::MemoryId, continuity::ProjectId, error::ErrorCode, ids::WorkspaceId,
};
use humaux_protocol::{
    mcp::{McpApplication, McpHttpContext, McpOperation, McpToolArguments, ToolName, ToolOutput},
    mcp_catalog::{CanonicalCatalog, OperationDescriptor},
};
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    context::{self, ContextBootstrap},
    continuity,
    guard::GatewayGuard,
    memory,
    recall::{self, RecallSearchRequest, SemanticRecallRuntime},
    remember::{self, PreparedEvidencePayload, RememberEventKind, RememberPolicy},
};

/// The only real MCP business routes currently available from Gateway.
pub const SUPPORTED_OPERATION_KEYS: [&str; 6] = [
    "remember.put",
    "recall.search",
    "context.assemble",
    "memory.get",
    "memory.enumerate",
    "continuity.get",
];

/// Bootstrap-owned, authenticated MCP dispatch.  It has no client-selected
/// tenant, user, stream, profile, or policy fields.
pub struct GatewayMcpApplication {
    catalog: Arc<CanonicalCatalog>,
    guard: Arc<GatewayGuard>,
    runtime_pool: Arc<RuntimeDbPool>,
    remember_policy: RememberPolicy,
    remember_workspace: WorkspaceId,
    remember_event_kind: RememberEventKind,
    context_bootstrap: ContextBootstrap,
    semantic_recall: Option<Arc<SemanticRecallRuntime>>,
    #[cfg(test)]
    trusted_continuity_scope: Option<humaux_domain::identity::AuthorizationScope>,
}

impl GatewayMcpApplication {
    pub fn new(
        catalog: CanonicalCatalog,
        guard: Arc<GatewayGuard>,
        remember_policy: RememberPolicy,
        remember_event_kind: RememberEventKind,
        context_bootstrap: ContextBootstrap,
    ) -> Self {
        let runtime_pool = guard.runtime_pool();
        let remember_workspace = remember_policy.workspace_id();
        Self {
            catalog: Arc::new(catalog),
            guard,
            runtime_pool,
            remember_policy,
            remember_workspace,
            remember_event_kind,
            context_bootstrap,
            semantic_recall: None,
            #[cfg(test)]
            trusted_continuity_scope: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_trusted_continuity_scope(
        mut self,
        scope: humaux_domain::identity::AuthorizationScope,
    ) -> Self {
        self.trusted_continuity_scope = Some(scope);
        self
    }

    /// Adds the native semantic lane using trusted bootstrap dependencies. Existing local-only
    /// deployments keep failing `recall.search` closed until this is configured.
    #[must_use]
    pub fn with_semantic_recall(mut self, runtime: SemanticRecallRuntime) -> Self {
        self.semantic_recall = Some(Arc::new(runtime));
        self
    }

    fn validated(
        &self,
        tool: ToolName,
        arguments: McpToolArguments<'_>,
    ) -> Result<(OperationDescriptor, String, Value), ErrorCode> {
        let decoded = Value::Object(arguments.decoded().clone());
        let descriptor = self.catalog.validate(tool, &decoded)?;
        let raw = arguments.raw_json().ok_or(ErrorCode::InvalidInput)?;
        let raw_value: Value = serde_json::from_str(raw).map_err(|_| ErrorCode::InvalidInput)?;
        if raw_value != decoded {
            return Err(ErrorCode::InvalidInput);
        }
        Ok((descriptor, raw.to_owned(), raw_value))
    }

    async fn reject_unsupported(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        requested_workspace: Option<WorkspaceId>,
    ) -> Result<ToolOutput, ErrorCode> {
        self.guard
            .reject_unsupported(context, operation, requested_workspace)
            .await?;
        Err(ErrorCode::DependencyUnavailable)
    }

    async fn remember_put(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
    ) -> Result<ToolOutput, ErrorCode> {
        let wire = RememberPutWire::from_raw(raw_arguments, value)?;
        let workspace = wire.workspace_id.unwrap_or(self.remember_workspace);
        let configured_workspace = self.remember_workspace;
        let policy = self.remember_policy.clone();
        let event_kind = self.remember_event_kind;
        let result = self
            .guard
            .run_atomic_remember(
                context,
                operation,
                Some(workspace),
                raw_arguments,
                wire.idempotency_key,
                move |request| {
                    if request.workspace_id() != Some(configured_workspace) {
                        return Err(ErrorCode::Forbidden);
                    }
                    remember::command(
                        request.authorization(),
                        &policy,
                        wire.content,
                        event_kind,
                        None,
                        OffsetDateTime::now_utc(),
                    )
                },
            )
            .await?;
        let accepted = result.accepted;
        output(json!({
            "evidence_id": accepted.evidence_id,
            "processing_handle": accepted.processing_handle,
            "consistency_token": accepted.consistency_token,
            "status": accepted.status,
            "replayed": result.replayed,
        }))
    }

    async fn context_assemble(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
    ) -> Result<ToolOutput, ErrorCode> {
        let requested_workspace = workspace(value)?;
        // These schema fields are valid contracts but this concrete route has no
        // semantics for them yet.  Admit/audit the request and then fail closed.
        if ["project_id", "task_id", "query", "limit"]
            .iter()
            .any(|field| value.get(*field).is_some())
        {
            return self
                .reject_unsupported(context, operation, requested_workspace)
                .await;
        }
        let pool = self.runtime_pool.clone();
        let bootstrap = self.context_bootstrap.clone();
        let catalog = self.catalog.clone();
        let pending = self
            .guard
            .run_local_read(
                context,
                operation,
                requested_workspace,
                raw_arguments,
                move |request| async move {
                    context::assemble(
                        pool,
                        request.authorization().clone(),
                        request.workspace_id(),
                        bootstrap,
                        |result| {
                            let value =
                                serde_json::to_value(result).map_err(|_| ErrorCode::Internal)?;
                            catalog.validate_output(ToolName::Context, &value)?;
                            output(value)
                        },
                    )
                    .await
                },
            )
            .await?;
        // Failed output validation, quota settlement or audit drops the pending result.
        // Only the accepted result may publish its final completeness metric.
        Ok(pending.finish())
    }

    async fn memory_get(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
    ) -> Result<ToolOutput, ErrorCode> {
        let memory_id =
            MemoryId::parse(value["memory_id"].as_str().ok_or(ErrorCode::InvalidInput)?)?;
        let requested_workspace = workspace(value)?;
        let pool = self.runtime_pool.clone();
        let bootstrap = self.context_bootstrap.clone();
        let catalog = self.catalog.clone();
        let pending = self
            .guard
            .run_local_read(
                context,
                operation,
                requested_workspace,
                raw_arguments,
                move |request| async move {
                    memory::get(
                        pool,
                        request.authorization().clone(),
                        request.workspace_id(),
                        bootstrap,
                        memory_id,
                        |result| {
                            let value =
                                serde_json::to_value(result).map_err(|_| ErrorCode::Internal)?;
                            catalog.validate_output(ToolName::Memory, &value)?;
                            output(value)
                        },
                    )
                    .await
                },
            )
            .await?;
        Ok(pending.finish())
    }

    async fn continuity_get(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
    ) -> Result<ToolOutput, ErrorCode> {
        let project_id = ProjectId::parse(
            value["project_id"]
                .as_str()
                .ok_or(ErrorCode::InvalidInput)?,
        )?;
        let requested_workspace = workspace(value)?;
        let pool = self.runtime_pool.clone();
        let budget = self.context_bootstrap.budget();
        let catalog = self.catalog.clone();
        #[cfg(test)]
        if let Some(authorization) = self.trusted_continuity_scope.clone() {
            return self
                .guard
                .run_continuity_read_with_trusted_scope(
                    context,
                    operation,
                    requested_workspace,
                    raw_arguments,
                    authorization,
                    move |request| async move {
                        continuity::get(
                            pool,
                            request.authorization().clone(),
                            project_id,
                            request.workspace_id(),
                            budget,
                            |result| {
                                let value = serde_json::to_value(result)
                                    .map_err(|_| ErrorCode::Internal)?;
                                catalog.validate_output(ToolName::Continuity, &value)?;
                                output(value)
                            },
                        )
                        .await
                    },
                )
                .await;
        }
        self.guard
            .run_continuity_read(
                context,
                operation,
                requested_workspace,
                raw_arguments,
                move |request| async move {
                    continuity::get(
                        pool,
                        request.authorization().clone(),
                        project_id,
                        request.workspace_id(),
                        budget,
                        |result| {
                            let value =
                                serde_json::to_value(result).map_err(|_| ErrorCode::Internal)?;
                            catalog.validate_output(ToolName::Continuity, &value)?;
                            output(value)
                        },
                    )
                    .await
                },
            )
            .await
    }

    async fn memory_enumerate(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
    ) -> Result<ToolOutput, ErrorCode> {
        let requested_workspace = workspace(value)?;
        let page_size = u16::try_from(
            value
                .get("limit")
                .map_or(Ok(50), |v| v.as_u64().ok_or(ErrorCode::InvalidInput))?,
        )
        .map_err(|_| ErrorCode::InvalidInput)?;
        let cursor = value
            .get("cursor")
            .map(|v| v.as_str().map(str::to_owned).ok_or(ErrorCode::InvalidInput))
            .transpose()?;
        let pool = self.runtime_pool.clone();
        let bootstrap = self.context_bootstrap.clone();
        let catalog = self.catalog.clone();
        let mac_key = self.guard.enumeration_mac_key();
        let pending = self
            .guard
            .run_local_read(
                context,
                operation,
                requested_workspace,
                raw_arguments,
                move |request| async move {
                    memory::enumerate(
                        pool,
                        request.authorization().clone(),
                        request.workspace_id(),
                        bootstrap,
                        MemoryEnumerationParams {
                            cursor: cursor.as_deref(),
                            page_size,
                            ttl: memory::ENUMERATION_TTL,
                            mac_key: &mac_key,
                        },
                        |result| {
                            let value =
                                serde_json::to_value(result).map_err(|_| ErrorCode::Internal)?;
                            catalog.validate_output(ToolName::Memory, &value)?;
                            output(value)
                        },
                    )
                    .await
                },
            )
            .await?;
        Ok(pending.finish())
    }

    async fn recall_search(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
    ) -> Result<ToolOutput, ErrorCode> {
        let requested_workspace = workspace(value)?;
        let workspace_id = requested_workspace.ok_or(ErrorCode::DependencyUnavailable)?;
        let runtime = self
            .semantic_recall
            .clone()
            .ok_or(ErrorCode::DependencyUnavailable)?;
        let query = value["query"]
            .as_str()
            .ok_or(ErrorCode::InvalidInput)?
            .to_owned();
        let consistency_token = value
            .get("consistency_token")
            .map(|token| {
                token
                    .as_str()
                    .map(str::to_owned)
                    .ok_or(ErrorCode::InvalidInput)
            })
            .transpose()?;
        let limit = value
            .get("limit")
            .map(|limit| {
                limit
                    .as_u64()
                    .and_then(|value| u32::try_from(value).ok())
                    .ok_or(ErrorCode::InvalidInput)
            })
            .transpose()?;
        let input = RecallSearchRequest {
            query,
            workspace_id,
            consistency_token,
            mode: value.get("mode").and_then(Value::as_str).map(str::to_owned),
            completeness_request: value
                .get("completeness_request")
                .and_then(Value::as_str)
                .map(str::to_owned),
            limit,
        };
        let pool = self.runtime_pool.clone();
        let bootstrap = self.context_bootstrap.clone();
        let catalog = self.catalog.clone();
        let pending = self
            .guard
            .run_retrieval_read(
                context,
                operation,
                requested_workspace,
                raw_arguments,
                move |request| async move {
                    recall::search(
                        pool,
                        runtime,
                        catalog,
                        request.authorization().clone(),
                        request.request_id(),
                        bootstrap,
                        input,
                    )
                    .await
                },
            )
            .await?;
        Ok(pending.finish())
    }
}

#[async_trait]
impl McpApplication for GatewayMcpApplication {
    async fn check_access(
        &self,
        context: &McpHttpContext,
        operation: McpOperation,
    ) -> Result<(), ErrorCode> {
        if matches!(operation, McpOperation::Preflight) {
            self.guard.preflight(context).await
        } else {
            self.guard.protocol(context, operation).await
        }
    }

    async fn invoke(
        &self,
        context: &McpHttpContext,
        tool: ToolName,
        arguments: McpToolArguments<'_>,
    ) -> Result<ToolOutput, ErrorCode> {
        let (operation, raw_arguments, value) = self.validated(tool, arguments)?;
        match operation.operation_key() {
            "remember.put" => {
                self.remember_put(context, &operation, &raw_arguments, &value)
                    .await
            }
            "context.assemble" => {
                self.context_assemble(context, &operation, &raw_arguments, &value)
                    .await
            }
            "recall.search" => {
                self.recall_search(context, &operation, &raw_arguments, &value)
                    .await
            }
            "memory.get" => {
                self.memory_get(context, &operation, &raw_arguments, &value)
                    .await
            }
            "memory.enumerate" => {
                self.memory_enumerate(context, &operation, &raw_arguments, &value)
                    .await
            }
            "continuity.get" => {
                self.continuity_get(context, &operation, &raw_arguments, &value)
                    .await
            }
            _ => {
                self.reject_unsupported(context, &operation, workspace(&value)?)
                    .await
            }
        }
    }
}

struct RememberPutWire {
    content: PreparedEvidencePayload,
    idempotency_key: String,
    workspace_id: Option<WorkspaceId>,
}

#[derive(Deserialize)]
struct RawRememberPutWire {
    content: Box<RawValue>,
    idempotency_key: String,
    workspace_id: Option<Uuid>,
}

impl RememberPutWire {
    fn from_raw(raw_arguments: &str, decoded: &Value) -> Result<Self, ErrorCode> {
        let raw: RawRememberPutWire =
            serde_json::from_str(raw_arguments).map_err(|_| ErrorCode::InvalidInput)?;
        let content: Value =
            serde_json::from_str(raw.content.get()).map_err(|_| ErrorCode::InvalidInput)?;
        if decoded.get("content") != Some(&content) {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            content: PreparedEvidencePayload::new(raw.content.get().as_bytes().to_vec(), content)?,
            idempotency_key: raw.idempotency_key,
            workspace_id: raw.workspace_id.map(WorkspaceId),
        })
    }
}

fn workspace(value: &Value) -> Result<Option<WorkspaceId>, ErrorCode> {
    let Some(value) = value.get("workspace_id") else {
        return Ok(None);
    };
    let value = value.as_str().ok_or(ErrorCode::InvalidInput)?;
    Ok(Some(WorkspaceId(
        Uuid::parse_str(value).map_err(|_| ErrorCode::InvalidInput)?,
    )))
}

fn output(structured_content: Value) -> Result<ToolOutput, ErrorCode> {
    if !structured_content.is_object() {
        return Err(ErrorCode::Internal);
    }
    let text = serde_json::to_string(&structured_content).map_err(|_| ErrorCode::Internal)?;
    Ok(ToolOutput {
        text,
        structured_content,
    })
}

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../../crates/adapters/tests/support/operation_receipt_fixture.rs"]
mod operation_receipt_fixture;

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        collections::BTreeMap,
        ffi::OsString,
        net::{Ipv6Addr, SocketAddr},
        os::unix::ffi::OsStringExt,
        str::FromStr,
        sync::Arc,
        time::Duration,
    };

    use humaux_adapters::{postgres::RuntimeDbPool, quota_repo::RatePolicy};
    use humaux_domain::{
        context::ContextBudget,
        dataclass::DataClass,
        identity::{BoundedSet, PrincipalId, VisibilityClass},
        ids::{TenantId, UserId, WorkspaceId},
    };
    use humaux_protocol::{
        edge::{Cidr, TrustedProxyConfig, compute_api_key_hash},
        mcp::{McpAdapter, McpHttpConfig},
        mcp_catalog::CanonicalCatalog,
    };
    use humaux_testkit::run_db_fixture;
    use postgres::{Client, GenericClient, NoTls};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    use uuid::Uuid;

    use super::operation_receipt_fixture::{
        Fixture, Handle, SYNTHETIC_CREDENTIAL_PEPPER, SyntheticCredentialScopes,
    };
    use crate::guard::{GuardRatePolicies, GuardSettings};

    const HOST: &str = "mcp.test";
    const ORIGIN: &str = "https://mcp.test";
    const BARRIER_RUN_ID: &str = "HUMAUX_CONTINUITY_W2_BARRIER_RUN_ID";
    const BARRIER_SIDE: &str = "HUMAUX_CONTINUITY_W2_BARRIER_SIDE";
    const SAME_KEY_SENTINEL: &str = "W2_EXPECTED_SAME_KEY_429:v1";
    const SAME_KEY_PANIC: &str = "W2_EXPECTED_SAME_KEY_429_PANIC:v1";

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum BarrierSide {
        A,
        B,
    }

    impl BarrierSide {
        fn as_str(self) -> &'static str {
            match self {
                Self::A => "A",
                Self::B => "B",
            }
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum BarrierConfig {
        Disabled,
        Enabled { run_id: Uuid, side: BarrierSide },
    }

    fn parse_barrier_config(
        run_id: Result<String, std::env::VarError>,
        side: Result<String, std::env::VarError>,
    ) -> Result<BarrierConfig, String> {
        match (run_id, side) {
            (Err(std::env::VarError::NotPresent), Err(std::env::VarError::NotPresent)) => {
                Ok(BarrierConfig::Disabled)
            }
            (Ok(run_id), Ok(side)) => {
                let parsed = Uuid::parse_str(&run_id)
                    .map_err(|_| format!("{BARRIER_RUN_ID} must be a canonical UUID"))?;
                if parsed.hyphenated().to_string() != run_id {
                    return Err(format!("{BARRIER_RUN_ID} must be a canonical UUID"));
                }
                let side = match side.as_str() {
                    "A" => BarrierSide::A,
                    "B" => BarrierSide::B,
                    _ => return Err(format!("{BARRIER_SIDE} must be A or B")),
                };
                Ok(BarrierConfig::Enabled {
                    run_id: parsed,
                    side,
                })
            }
            (Err(error), _) | (_, Err(error)) => Err(format!(
                "{BARRIER_RUN_ID} and {BARRIER_SIDE} must both be absent or both be valid: {error}"
            )),
        }
    }

    fn barrier_config() -> BarrierConfig {
        parse_barrier_config(std::env::var(BARRIER_RUN_ID), std::env::var(BARRIER_SIDE))
            .unwrap_or_else(|error| panic!("invalid W2 barrier configuration: {error}"))
    }

    fn rate() -> RatePolicy {
        RatePolicy::new(10_000, 10_000).expect("nonbinding continuity fixture rate")
    }

    fn test_guard(runtime: RuntimeDbPool) -> Arc<GatewayGuard> {
        Arc::new(
            GatewayGuard::new(
                runtime,
                GuardSettings {
                    credential_pepper: SYNTHETIC_CREDENTIAL_PEPPER.to_vec(),
                    trusted_proxies: TrustedProxyConfig {
                        trusted_proxy_cidrs: vec![Cidr::from_str("127.0.0.1/32").unwrap()],
                        max_forwarded_hops: 1,
                    },
                    global_denylist: vec![],
                    global_emergency_allowlist: vec![],
                    tenant_network: BTreeMap::new(),
                    rates: GuardRatePolicies {
                        preauth_ip: rate(),
                        credential: rate(),
                        user: rate(),
                        tenant: rate(),
                        operation: rate(),
                    },
                    reservation_ttl: Duration::from_secs(30),
                    handler_timeout: Duration::from_secs(5),
                    finalize_timeout: Duration::from_secs(2),
                    replay_ttl: Duration::from_secs(60),
                },
            )
            .expect("explicit guard settings"),
        )
    }

    fn test_application(
        handle: &Handle,
        runtime: RuntimeDbPool,
        scope: humaux_domain::identity::AuthorizationScope,
    ) -> GatewayMcpApplication {
        let policy = RememberPolicy::new(
            humaux_projection::stream::StreamKey::new(
                TenantId(handle.tenant_id),
                "workspace",
                handle.workspace_id,
                "knowledge",
                "ingest",
                "v1",
            ),
            handle.reasoning_domain_id,
            Duration::from_secs(60),
            DataClass::Internal,
            VisibilityClass::WorkspaceShared,
        )
        .unwrap();
        let bootstrap = ContextBootstrap::new(
            ContextBudget::new(2_048, 1_024).unwrap(),
            humaux_contracts::retrieval_config::resolve_registered_retrieval_profile(
                &Default::default(),
            )
            .unwrap(),
            &policy,
        )
        .unwrap();
        GatewayMcpApplication::new(
            CanonicalCatalog::load().unwrap(),
            test_guard(runtime),
            policy,
            RememberEventKind::UserMessage,
            bootstrap,
        )
        .with_trusted_continuity_scope(scope)
    }

    fn set_context(
        client: &mut impl GenericClient,
        tenant: Uuid,
        workspace: Uuid,
        principal: Uuid,
        user: Uuid,
    ) {
        client
            .query_one(
                "SELECT set_config('humaux.tenant_id',$1,true),\
                        set_config('humaux.workspace_id',$2,true),\
                        set_config('humaux.principal_id',$3,true),\
                        set_config('humaux.user_id',$4,true)",
                &[
                    &tenant.to_string(),
                    &workspace.to_string(),
                    &principal.to_string(),
                    &user.to_string(),
                ],
            )
            .unwrap();
    }

    fn register_for(client: &mut Client, handle: &Handle, project: Uuid, title: &str) {
        client.batch_execute("BEGIN").unwrap();
        set_context(
            client,
            handle.tenant_id,
            handle.workspace_id,
            handle.principal_id,
            handle.user_id,
        );
        client
            .query_one(
                "SELECT private.register_continuity_project($1,$2,$3,$4,$5,$6)",
                &[
                    &handle.tenant_id,
                    &handle.workspace_id,
                    &project,
                    &handle.principal_id,
                    &Some(handle.user_id),
                    &title,
                ],
            )
            .unwrap();
        client.batch_execute("COMMIT").unwrap();
    }

    fn publish_goal(client: &mut Client, handle: &mut Handle, project: Uuid, memory: Uuid) {
        let memory_hash: Vec<u8> = handle
            .admin
            .query_one(
                "SELECT sha256(convert_to(content::text,'UTF8'))\
                 FROM private.memory_records WHERE tenant_id=$1 AND memory_id=$2",
                &[&handle.tenant_id, &memory],
            )
            .unwrap()
            .get(0);
        client.batch_execute("BEGIN").unwrap();
        set_context(
            client,
            handle.tenant_id,
            handle.workspace_id,
            handle.principal_id,
            handle.user_id,
        );
        client
            .query_one(
                "SELECT facet_version_id FROM private.publish_continuity_facet(\
                   $1,$2,$3,$4,$5,'GOAL',0,'CURRENT',$6,$7,$8,$9,$10)",
                &[
                    &handle.tenant_id,
                    &handle.workspace_id,
                    &project,
                    &handle.principal_id,
                    &Some(handle.user_id),
                    &json!({"goal":"native continuity"}),
                    &vec![memory],
                    &vec![memory_hash],
                    &Vec::<Uuid>::new(),
                    &Vec::<Vec<u8>>::new(),
                ],
            )
            .unwrap();
        client.batch_execute("COMMIT").unwrap();
    }

    async fn start(
        application: GatewayMcpApplication,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let catalog = CanonicalCatalog::load().unwrap().trusted_catalog().unwrap();
        let adapter = McpAdapter::new(
            Arc::new(application),
            catalog,
            McpHttpConfig::new(vec![HOST.into()], vec![ORIGIN.into()], 64 * 1024).unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                adapter
                    .router()
                    .into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        (address, server)
    }

    async fn stop_server(server: tokio::task::JoinHandle<()>) {
        server.abort();
        match server.await {
            Ok(()) => eprintln!(
                "continuity_server_join pid={} result=clean",
                std::process::id()
            ),
            Err(error) if error.is_cancelled() => eprintln!(
                "continuity_server_join pid={} result=cancelled",
                std::process::id()
            ),
            Err(error) => panic!("continuity test server failed: {error}"),
        }
    }

    async fn request_and_stop(
        address: SocketAddr,
        bearer: &str,
        project: Uuid,
        logical_request: &'static str,
        forwarded: Option<Ipv6Addr>,
        server: tokio::task::JoinHandle<()>,
    ) -> Value {
        let bearer = bearer.to_owned();
        let request = tokio::spawn(async move {
            request(address, &bearer, project, logical_request, forwarded).await
        });
        let result = request.await;
        stop_server(server).await;
        match result {
            Ok(value) => value,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => panic!("continuity request task failed: {error}"),
        }
    }

    fn barrier_sql(statement: &'static str) -> String {
        match statement {
            "ready" => "INSERT INTO control.w2_test_request_intervals(run_uuid,side,ready_at)\nVALUES($1,$2,clock_timestamp())".to_owned(),
            "start" => "UPDATE control.w2_test_request_intervals\nSET request_start_at=clock_timestamp()\nWHERE run_uuid=$1 AND side=$2 AND request_start_at IS NULL".to_owned(),
            "end" => "UPDATE control.w2_test_request_intervals\nSET request_end_at=clock_timestamp()\nWHERE run_uuid=$1 AND side=$2 AND request_start_at IS NOT NULL\n  AND request_end_at IS NULL".to_owned(),
            "both_ready" => "SELECT count(*) FROM control.w2_test_request_intervals\nWHERE run_uuid=$1 AND side IN ('A','B') AND ready_at IS NOT NULL".to_owned(),
            "both_end" => "SELECT count(*) FROM control.w2_test_request_intervals\nWHERE run_uuid=$1 AND side IN ('A','B') AND request_end_at IS NOT NULL".to_owned(),
            _ => unreachable!("fixed W2 barrier statement"),
        }
    }

    async fn barrier_statement(dsn: String, config: BarrierConfig, statement: &'static str) {
        let BarrierConfig::Enabled { run_id, side } = config else {
            return;
        };
        tokio::task::spawn_blocking(move || {
            let mut client = Client::connect(&dsn, NoTls)
                .unwrap_or_else(|error| panic!("W2 barrier connect failed: {error}"));
            let side = side.as_str();
            match statement {
                "ready" => assert_eq!(
                    client
                        .execute(&barrier_sql(statement), &[&run_id, &side])
                        .unwrap(),
                    1,
                    "W2 barrier ready must insert exactly one row"
                ),
                "start" => assert_eq!(
                    client
                        .execute(&barrier_sql(statement), &[&run_id, &side])
                        .unwrap(),
                    1,
                    "W2 barrier start must update exactly one row"
                ),
                "end" => assert_eq!(
                    client
                        .execute(&barrier_sql(statement), &[&run_id, &side])
                        .unwrap(),
                    1,
                    "W2 barrier end must update exactly one row"
                ),
                "both_ready" | "both_end" => loop {
                    let sql = barrier_sql(statement);
                    let count: i64 = client.query_one(&sql, &[&run_id]).unwrap().get(0);
                    if count == 2 {
                        break;
                    }
                    if count > 2 {
                        panic!("W2 barrier {statement} observed unexpected row count {count}");
                    }
                    std::thread::sleep(Duration::from_millis(10));
                },
                _ => unreachable!("fixed W2 barrier statement"),
            }
        })
        .await
        .unwrap_or_else(|error| panic!("W2 barrier worker failed: {error}"));
    }

    async fn barrier_request_and_stop(
        dsn: &str,
        config: BarrierConfig,
        address: SocketAddr,
        bearer: &str,
        project: Uuid,
        forwarded: Option<Ipv6Addr>,
        server: tokio::task::JoinHandle<()>,
    ) -> Value {
        barrier_statement(dsn.to_owned(), config, "ready").await;
        barrier_statement(dsn.to_owned(), config, "both_ready").await;
        barrier_statement(dsn.to_owned(), config, "start").await;
        let bearer = bearer.to_owned();
        let result =
            tokio::spawn(
                async move { request(address, &bearer, project, "A_then_B", forwarded).await },
            )
            .await;
        match result {
            Ok(value) => {
                barrier_statement(dsn.to_owned(), config, "end").await;
                stop_server(server).await;
                barrier_statement(dsn.to_owned(), config, "both_end").await;
                value
            }
            Err(error) if error.is_panic() => {
                stop_server(server).await;
                std::panic::resume_unwind(error.into_panic());
            }
            Err(error) => {
                stop_server(server).await;
                panic!("continuity request task failed: {error}");
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn request(
        address: SocketAddr,
        bearer: &str,
        project: Uuid,
        logical_request: &str,
        forwarded: Option<Ipv6Addr>,
    ) -> Value {
        let body = json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"tools/call",
            "params":{
                "name":"continuity",
                "arguments":{"project_id":project},
                "_meta":{
                    "io.modelcontextprotocol/protocolVersion":"2026-07-28",
                    "io.modelcontextprotocol/clientInfo":{"name":"continuity-w2","version":"1"},
                    "io.modelcontextprotocol/clientCapabilities":{}
                }
            }
        })
        .to_string();
        let headers = [
            ("MCP-Protocol-Version", "2026-07-28"),
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "continuity"),
            ("Authorization", bearer),
        ];
        let mut stream = TcpStream::connect(address).await.unwrap();
        let mut wire = format!(
            "POST /mcp HTTP/1.1\r\nHost: {HOST}\r\nOrigin: {ORIGIN}\r\nConnection: close\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        );
        for (name, value) in headers {
            wire.push_str(name);
            wire.push_str(": ");
            wire.push_str(value);
            wire.push_str("\r\n");
        }
        if let Some(forwarded) = forwarded {
            wire.push_str("Forwarded: ");
            wire.push_str(&forwarded.to_string());
            wire.push_str("\r\n");
        }
        wire.push_str("\r\n");
        wire.push_str(&body);
        stream.write_all(wire.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let response_bytes = response.len();
        let response = String::from_utf8(response).unwrap();
        let (head, encoded_body) = response.split_once("\r\n\r\n").unwrap();
        let status_line = head.lines().next().unwrap_or("<missing>");
        let mut content_length = None;
        let mut transfer_encoding = None;
        let mut content_type = None;
        let headers = head
            .lines()
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| {
                let name = name.trim();
                let value = value.trim();
                match name.to_ascii_lowercase().as_str() {
                    "content-length" => content_length = Some(value),
                    "transfer-encoding" => transfer_encoding = Some(value),
                    "content-type" => content_type = Some(value),
                    _ => {}
                }
                let value = if matches!(
                    name.to_ascii_lowercase().as_str(),
                    "authorization" | "cookie" | "set-cookie" | "x-api-key"
                ) {
                    "<redacted>"
                } else {
                    value
                };
                format!("{name}={value}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let encoded_body_bytes = encoded_body.len();
        let diagnostic = || {
            format!(
                "pid={} logical_request={logical_request} status_line={status_line:?} \
                 headers=[{headers}] response_bytes={response_bytes} \
                 encoded_body_bytes={encoded_body_bytes} content_length={content_length:?} \
                 transfer_encoding={transfer_encoding:?} content_type={content_type:?} \
                 body_prefix={:?}",
                std::process::id(),
                encoded_body
                    .as_bytes()
                    .get(..encoded_body_bytes.min(256))
                    .unwrap_or_default()
                    .escape_ascii()
                    .to_string(),
            )
        };
        let status = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or_else(|| panic!("continuity HTTP response missing status: {}", diagnostic()));
        assert!(
            transfer_encoding.is_none(),
            "continuity HTTP response unsupported transfer encoding: {}",
            diagnostic()
        );
        if let Some(content_length) = content_length {
            let content_length = content_length.parse::<usize>().unwrap_or_else(|_| {
                panic!(
                    "continuity HTTP response invalid Content-Length: {}",
                    diagnostic()
                )
            });
            assert_eq!(
                encoded_body_bytes,
                content_length,
                "continuity HTTP response Content-Length mismatch: {}",
                diagnostic()
            );
        }
        let body = if encoded_body.starts_with("event:") {
            encoded_body
                .lines()
                .find_map(|line| line.strip_prefix("data: "))
                .unwrap()
        } else {
            encoded_body
        };
        let value: Value = serde_json::from_str(body).unwrap_or_else(|error| {
            panic!(
                "continuity HTTP response parse failure: {} error={error}",
                diagnostic(),
            );
        });
        if status == 429 && direct_preauth_same_key() {
            assert_eq!(
                value["result"]["isError"],
                true,
                "strict same-key 429 must be an MCP error: {}",
                diagnostic()
            );
            assert_eq!(
                value["result"]["structuredContent"]["code"],
                "RATE_LIMITED",
                "strict same-key 429 must be RATE_LIMITED: {}",
                diagnostic()
            );
            eprintln!("{SAME_KEY_SENTINEL}");
            panic!("{SAME_KEY_PANIC}");
        }
        assert_eq!(
            status,
            200,
            "continuity HTTP response unexpected status: {}",
            diagnostic()
        );
        value
    }

    fn direct_preauth_same_key() -> bool {
        match std::env::var("HUMAUX_CONTINUITY_DIRECT_PREAUTH_SAME_KEY") {
            Err(std::env::VarError::NotPresent) => false,
            Ok(value) if value == "1" => true,
            _ => panic!("HUMAUX_CONTINUITY_DIRECT_PREAUTH_SAME_KEY must be unset or 1"),
        }
    }

    fn fixture_forwarded(tenant_id: Uuid) -> Option<Ipv6Addr> {
        (!direct_preauth_same_key()).then(|| {
            Ipv6Addr::from((0x2001_0db8_u128 << 96) | (tenant_id.as_u128() & ((1_u128 << 96) - 1)))
        })
    }

    #[test]
    fn w2_barrier_config_is_pure_and_fail_closed() {
        let run_id = Uuid::now_v7();
        assert_eq!(
            parse_barrier_config(
                Err(std::env::VarError::NotPresent),
                Err(std::env::VarError::NotPresent)
            ),
            Ok(BarrierConfig::Disabled)
        );
        assert_eq!(
            parse_barrier_config(Ok(run_id.to_string()), Ok("A".to_owned())),
            Ok(BarrierConfig::Enabled {
                run_id,
                side: BarrierSide::A,
            })
        );
        for (run_id, side) in [
            (Ok(run_id.to_string()), Err(std::env::VarError::NotPresent)),
            (Err(std::env::VarError::NotPresent), Ok("B".to_owned())),
            (Ok("not-a-uuid".to_owned()), Ok("A".to_owned())),
            (Ok(run_id.to_string().to_uppercase()), Ok("A".to_owned())),
            (Ok(run_id.to_string()), Ok("C".to_owned())),
            (
                Err(std::env::VarError::NotUnicode(OsString::from_vec(vec![
                    0xff,
                ]))),
                Ok("A".to_owned()),
            ),
        ] {
            assert!(parse_barrier_config(run_id, side).is_err());
        }
    }

    #[test]
    fn w2_barrier_sql_is_whitespace_separated_and_parameterized() {
        let cases = [
            (
                "ready",
                "INSERT INTO control.w2_test_request_intervals(run_uuid,side,ready_at)",
                &["$1", "$2"][..],
            ),
            (
                "start",
                "UPDATE control.w2_test_request_intervals",
                &["$1", "$2"][..],
            ),
            (
                "end",
                "UPDATE control.w2_test_request_intervals",
                &["$1", "$2"][..],
            ),
            (
                "both_ready",
                "SELECT count(*) FROM control.w2_test_request_intervals",
                &["$1"][..],
            ),
            (
                "both_end",
                "SELECT count(*) FROM control.w2_test_request_intervals",
                &["$1"][..],
            ),
        ];
        for (statement, prefix, placeholders) in cases {
            let sql = barrier_sql(statement);
            assert!(!sql.as_bytes().contains(&b'\\'), "{statement}: {sql:?}");
            assert!(sql.starts_with(prefix), "{statement}: {sql}");
            for placeholder in placeholders {
                assert!(sql.contains(placeholder), "{statement}: {sql}");
            }
            assert!(sql.contains("\n"), "{statement}: {sql}");
        }
        assert!(barrier_sql("ready").contains("VALUES($1,$2,clock_timestamp())"));
        assert!(barrier_sql("start").contains("SET request_start_at=clock_timestamp()"));
        assert!(barrier_sql("end").contains("SET request_end_at=clock_timestamp()"));
        assert!(barrier_sql("both_ready").contains("ready_at IS NOT NULL"));
        assert!(barrier_sql("both_end").contains("request_end_at IS NOT NULL"));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn trusted_multi_workspace_omission_resolves_exact_project_parent_not_first() {
        run_db_fixture::<Fixture, _>(
            "trusted_multi_workspace_omission_resolves_exact_project_parent_not_first",
            |mut handle| {
                let prefix = format!("ct{}", Uuid::now_v7().simple());
                let wire = format!("{prefix}.{}", "d".repeat(32));
                let credential = handle.seed_synthetic_service_credential_and_window(
                    SyntheticCredentialScopes::ContextRead,
                    &prefix,
                    &wire,
                    &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
                    20,
                );
                let source = handle.seed_workspace_visible_context_record();
                let project = Uuid::now_v7();
                let parent_b = handle.workspace_id;
                assert_ne!(parent_b, Uuid::nil(), "fixture parent B must be non-nil");
                let workspace_a =
                    handle.seed_workspace_with_id(Uuid::from_u128(parent_b.as_u128() - 1));
                let scope_a_then_b = humaux_domain::identity::AuthorizationScope::new(
                    TenantId(handle.tenant_id),
                    PrincipalId(handle.principal_id),
                    Some(UserId(handle.user_id)),
                    BoundedSet::new([WorkspaceId(workspace_a), WorkspaceId(parent_b)]).unwrap(),
                );
                let scope_b_then_a = humaux_domain::identity::AuthorizationScope::new(
                    TenantId(handle.tenant_id),
                    PrincipalId(handle.principal_id),
                    Some(UserId(handle.user_id)),
                    BoundedSet::new([WorkspaceId(parent_b), WorkspaceId(workspace_a)]).unwrap(),
                );
                assert!(
                    workspace_a < parent_b,
                    "A must sort before B for the mutation control"
                );
                assert_eq!(
                    scope_a_then_b
                        .allowed_workspace_ids()
                        .iter()
                        .next()
                        .copied(),
                    Some(WorkspaceId(workspace_a)),
                    "the first allowed workspace is deliberately the wrong parent A"
                );
                let mut gateway = handle.gateway_client().unwrap();
                register_for(&mut gateway, &handle, project, "parent-b");
                publish_goal(&mut gateway, &mut handle, project, source.memory_id);
                let dsn = handle.gateway_dsn_for_process().to_owned();
                let forwarded = fixture_forwarded(handle.tenant_id);
                let barrier = barrier_config();
                if let Some(forwarded) = forwarded {
                    handle.seed_legacy_system_preauth_bucket(forwarded.to_string());
                }
                let runtime_handle = handle.rt.handle().clone();
                let first_only = runtime_handle.block_on(async {
                    let runtime = RuntimeDbPool::connect(&dsn).await.unwrap();
                    let scope = humaux_domain::identity::AuthorizationScope::new(
                        TenantId(handle.tenant_id),
                        PrincipalId(handle.principal_id),
                        Some(UserId(handle.user_id)),
                        BoundedSet::new([WorkspaceId(workspace_a)]).unwrap(),
                    );
                    let (address, server) = start(test_application(&handle, runtime, scope)).await;
                    request_and_stop(
                        address,
                        &credential.bearer,
                        project,
                        "first_only",
                        forwarded,
                        server,
                    )
                    .await
                });
                assert_eq!(
                    first_only["result"]["structuredContent"]["code"], "NOT_FOUND",
                    "a first-only A authorization cannot read the B parent"
                );
                if let Some(forwarded) = forwarded {
                    let row = handle
                        .admin
                        .query_one(
                            "SELECT capacity, refill_per_second FROM control.rate_buckets \
                         WHERE tenant_id='00000000-0000-0000-0000-000000000000' \
                           AND subject_kind='ip' AND subject_id=$1 \
                           AND operation='mcp' AND bucket_key='preauth'",
                            &[&forwarded.to_string()],
                        )
                        .unwrap();
                    assert_eq!(row.get::<_, i64>(0), 10_000);
                    assert_eq!(row.get::<_, i64>(1), 10_000);
                }
                let first = runtime_handle.block_on(async {
                    let runtime = RuntimeDbPool::connect(&dsn).await.unwrap();
                    let (address, server) =
                        start(test_application(&handle, runtime, scope_a_then_b)).await;
                    match barrier {
                        BarrierConfig::Disabled => {
                            request_and_stop(
                                address,
                                &credential.bearer,
                                project,
                                "A_then_B",
                                forwarded,
                                server,
                            )
                            .await
                        }
                        BarrierConfig::Enabled { .. } => {
                            barrier_request_and_stop(
                                &dsn,
                                barrier,
                                address,
                                &credential.bearer,
                                project,
                                forwarded,
                                server,
                            )
                            .await
                        }
                    }
                });
                let second = runtime_handle.block_on(async {
                    let runtime = RuntimeDbPool::connect(&dsn).await.unwrap();
                    let (address, server) =
                        start(test_application(&handle, runtime, scope_b_then_a)).await;
                    request_and_stop(
                        address,
                        &credential.bearer,
                        project,
                        "B_then_A",
                        forwarded,
                        server,
                    )
                    .await
                });
                for value in [&first, &second] {
                    assert_ne!(value["result"]["isError"], true, "{value}");
                    assert_eq!(
                        value["result"]["structuredContent"]["project_id"],
                        project.to_string()
                    );
                    assert_eq!(
                        value["result"]["structuredContent"]["facets"]
                            .as_array()
                            .unwrap()
                            .len(),
                        17
                    );
                }
                assert_eq!(
                    first["result"]["structuredContent"]["project_id"],
                    second["result"]["structuredContent"]["project_id"]
                );
                assert_eq!(
                    first["result"]["structuredContent"]["facets"],
                    second["result"]["structuredContent"]["facets"],
                    "scope insertion order must not affect the B parent result"
                );
                assert_eq!(
                    first["result"]["structuredContent"]["coverage"],
                    second["result"]["structuredContent"]["coverage"]
                );
            },
        );
    }

    #[test]
    fn remember_preserves_content_bytes_and_rejects_mismatched_decoding() {
        let raw = r#"{"operation":"put","content":"kept\u0020raw","idempotency_key":"one"}"#;
        let decoded: Value = serde_json::from_str(raw).unwrap();
        assert!(
            CanonicalCatalog::load()
                .unwrap()
                .validate(ToolName::Remember, &decoded)
                .is_ok()
        );
        let prepared = RememberPutWire::from_raw(raw, &decoded).unwrap();
        assert_eq!(prepared.content.raw_json(), br#""kept\u0020raw""#);
        let mut unrelated = decoded;
        unrelated["content"] = json!("changed");
        assert!(matches!(
            RememberPutWire::from_raw(raw, &unrelated),
            Err(ErrorCode::InvalidInput)
        ));
    }
}
