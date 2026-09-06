//! Gateway implementation of the native MCP application port.
//!
//! The canonical catalog validates wire arguments before this fixed dispatch
//! table runs.  Only routes with a complete local implementation appear here;
//! every other valid contract is admitted and denied by [`GatewayGuard`] rather
//! than becoming a successful no-op.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use humaux_adapters::{
    affect_repo,
    context_repo::MemoryEnumerationParams,
    distill_repo::{ConfirmOutcome, RejectOutcome},
    memory_governance_repo::{ArchiveResult, RestoreResult},
    postgres::RuntimeDbPool,
    subject_repo,
};
use humaux_domain::{
    affect::{AffectWriteOp, MoodHalfLife},
    authority::MemoryId,
    confirm::{ConfirmToken, DestructiveOp},
    continuity::ProjectId,
    dataclass::DataClass,
    error::ErrorCode,
    evidence::payload_sha256,
    identity::VisibilityClass,
    ids::WorkspaceId,
    subject::{SubjectId, SubjectKey, SubjectKeyKind, SubjectKind, SubjectRole, SubjectWriteOp},
};
use humaux_protocol::{
    mcp::{McpApplication, McpHttpContext, McpOperation, McpToolArguments, ToolName, ToolOutput},
    mcp_catalog::{CanonicalCatalog, OperationDescriptor},
};
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    context::{self, ContextBootstrap},
    continuity,
    guard::{ConfirmGate, ConfirmedOutcome, GatewayGuard},
    memory,
    recall::{self, RecallSearchRequest, SemanticRecallRuntime},
    remember::{
        self, PreparedEvidencePayload, PutClassification, RememberEventKind, RememberPolicy,
    },
};

/// The only real MCP business routes currently available from Gateway. Confirm-gated
/// destructive keys come from the closed `DestructiveOp` table (§78.2, ADR-0018), never a
/// second literal.
pub const SUPPORTED_OPERATION_KEYS: [&str; 15] = [
    "remember.put",
    "recall.search",
    "context.assemble",
    "memory.get",
    "memory.enumerate",
    "continuity.get",
    DestructiveOp::MemorySupersede.operation_key(),
    DestructiveOp::MemoryPin.operation_key(),
    DestructiveOp::MemoryUnpin.operation_key(),
    DestructiveOp::MemoryRestore.operation_key(),
    DestructiveOp::MemoryArchive.operation_key(),
    DestructiveOp::MemoryUnarchive.operation_key(),
    SubjectWriteOp::Register.operation_key(),
    SubjectWriteOp::LinkKey.operation_key(),
    AffectWriteOp::Annotate.operation_key(),
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
    /// §33.10 rule 9 token lifetime (ADR-0018). `None` keeps every confirm-gated route
    /// failing closed through `reject_unsupported`.
    confirm_token_ttl: Option<Duration>,
    /// §78.1 memory.restore undo window (ADR-0020). `None` keeps memory.supersede and
    /// memory.restore failing closed (both write the lifecycle log).
    undo_window: Option<Duration>,
    /// §8.5.1 / ADR-0030 D-B frozen mood half-life policy
    /// (`HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS`, §78.1). `None` keeps `memory.annotate_affect`
    /// failing closed through `reject_unsupported` (a MOOD row needs the policy to stamp).
    mood_half_life: Option<MoodHalfLife>,
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
            confirm_token_ttl: None,
            undo_window: None,
            mood_half_life: None,
            #[cfg(test)]
            trusted_continuity_scope: None,
        }
    }

    /// Sets the §8.5.1 mood half-life (`HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS`, ADR-0030 D-B).
    /// A zero half-life is refused. Until set, `memory.annotate_affect` (and `memory.correct
    /// {affects}`) fail closed through `reject_unsupported`.
    pub fn with_mood_half_life(mut self, half_life: Duration) -> Result<Self, ErrorCode> {
        self.mood_half_life = Some(MoodHalfLife::new(half_life)?);
        Ok(self)
    }

    /// Enables the confirm-gated governance routes with the bootstrap-owned token TTL
    /// (`HUMAUX_GATEWAY_CONFIRM_TOKEN_TTL_SECONDS`, §78.1). A zero TTL is refused.
    pub fn with_confirm_token_ttl(mut self, ttl: Duration) -> Result<Self, ErrorCode> {
        if ttl.is_zero() {
            return Err(ErrorCode::InvalidInput);
        }
        self.confirm_token_ttl = Some(ttl);
        Ok(self)
    }

    /// Sets the §78.1 memory.restore undo window (`HUMAUX_GATEWAY_UNDO_WINDOW_SECONDS`,
    /// ADR-0020). A zero window is refused. Until set, memory.supersede and memory.restore
    /// fail closed through `reject_unsupported` (both append to the lifecycle log).
    pub fn with_undo_window(mut self, window: Duration) -> Result<Self, ErrorCode> {
        if window.is_zero() {
            return Err(ErrorCode::InvalidInput);
        }
        self.undo_window = Some(window);
        Ok(self)
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
        // §6.1.3 rules 1/2 (ADR-0028): parse the declaration here (malformed ⇒ INVALID_INPUT
        // before admission); it is RESOLVED inside `remember_in_txn`, before the Evidence is
        // written, so an unknown id/key rejects with nothing accepted or metered.
        let subjects = subject_repo::parse_declaration(value)?;
        // §8.5.1 (ADR-0030 D-C): `affects` parse here the same way (closed sets, basis-point
        // ranges ⇒ INVALID_INPUT before admission), resolve their target subjects inside
        // `remember_in_txn` before the Evidence is written, and land on `evidence_affects` in
        // that same transaction; the Distill-born memory inherits them (0157 trigger).
        let affects = affect_repo::parse_affects(value)?;
        let mood_half_life = match (affects.is_empty(), self.mood_half_life) {
            (true, _) => None,
            (false, Some(half_life)) => Some(half_life),
            (false, None) => return self.reject_unsupported(context, operation, None).await,
        };
        // ADR-0032 D-B: per-call classification within the closed sets, process defaults
        // when absent (backward compatible). Reachability of the class is judged downstream
        // (`remember::command` for scope, the receipt transaction for the TENANT_SHARED role).
        let classification = PutClassification {
            visibility_class: wire
                .visibility_class
                .unwrap_or(self.remember_policy.visibility_class()),
            data_class: wire.data_class.unwrap_or(self.remember_policy.data_class()),
            event_kind: wire.event_kind.unwrap_or(self.remember_event_kind),
        };
        let workspace = wire.workspace_id.unwrap_or(self.remember_workspace);
        let policy = self.remember_policy.clone();
        let bootstrap = self.context_bootstrap.clone();
        let result = self
            .guard
            .run_atomic_remember(
                context,
                operation,
                Some(workspace),
                raw_arguments,
                wire.idempotency_key,
                move |request| {
                    if request.workspace_id() != Some(workspace) {
                        return Err(ErrorCode::Forbidden);
                    }
                    // ADR-0032 D-A (§34.0.1 Q9): the write stream is derived per request at
                    // the read routes' single derivation point — principal tenant + the
                    // membership-narrowed requested workspace + the process family. Pure, no
                    // PG round trip; the pair's checkpoint row is created idempotently by the
                    // first write (`issue_stream_log_row`).
                    let (_, stream) =
                        bootstrap.request_stream(request.authorization().tenant_id(), workspace);
                    remember::command(
                        request.authorization(),
                        &policy,
                        &stream,
                        wire.content,
                        classification,
                        None,
                        OffsetDateTime::now_utc(),
                        subjects,
                        affects,
                        mood_half_life,
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
        // ADR-0026 D-E: `{candidates:true}` lists PENDING distill candidates instead of memories
        // (same read gate + workspace rule). The smaller schema change vs. a new op.
        if value.get("candidates").and_then(Value::as_bool) == Some(true) {
            return self
                .memory_enumerate_candidates(context, operation, raw_arguments, value)
                .await;
        }
        // ADR-0028: `{subjects:true}` lists the tenant's registered subjects (§6.1.3) — the
        // registry read-back, same read gate + workspace rule.
        if value.get("subjects").and_then(Value::as_bool) == Some(true) {
            return self
                .memory_enumerate_subjects(context, operation, raw_arguments, value)
                .await;
        }
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
        // §6.1.3 D-D (ADR-0028): an exact subject filter on the manifest predicate.
        let subject_id = value
            .get("subject_id")
            .map(|v| SubjectId::parse(v.as_str().ok_or(ErrorCode::InvalidInput)?))
            .transpose()?
            .map(|id| id.0);
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
                            subject_id,
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

    /// ADR-0026 D-E: `memory.enumerate {candidates:true}` — the tenant's PENDING distill
    /// candidates visible to the caller. Same read gate as memory.enumerate.
    async fn memory_enumerate_candidates(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
    ) -> Result<ToolOutput, ErrorCode> {
        let requested_workspace = workspace(value)?;
        let limit = i64::try_from(
            value
                .get("limit")
                .map_or(Ok(50), |v| v.as_u64().ok_or(ErrorCode::InvalidInput))?,
        )
        .map_err(|_| ErrorCode::InvalidInput)?
        .clamp(1, 100);
        let pool = self.runtime_pool.clone();
        let bootstrap = self.context_bootstrap.clone();
        let catalog = self.catalog.clone();
        self.guard
            .run_local_read(
                context,
                operation,
                requested_workspace,
                raw_arguments,
                move |request| async move {
                    let result = memory::list_candidates(
                        pool,
                        request.authorization().clone(),
                        request.workspace_id(),
                        bootstrap,
                        limit,
                    )
                    .await?;
                    let value = serde_json::to_value(result).map_err(|_| ErrorCode::Internal)?;
                    catalog.validate_output(ToolName::Memory, &value)?;
                    output(value)
                },
            )
            .await
    }

    /// §6.1.3 / ADR-0028: `memory.enumerate {subjects:true}` — the tenant's registered subjects
    /// with keys and roles, under RLS. Same read gate as memory.enumerate.
    async fn memory_enumerate_subjects(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
    ) -> Result<ToolOutput, ErrorCode> {
        let requested_workspace = workspace(value)?;
        let limit = i64::try_from(
            value
                .get("limit")
                .map_or(Ok(50), |v| v.as_u64().ok_or(ErrorCode::InvalidInput))?,
        )
        .map_err(|_| ErrorCode::InvalidInput)?
        .clamp(1, 100);
        let pool = self.runtime_pool.clone();
        let bootstrap = self.context_bootstrap.clone();
        let catalog = self.catalog.clone();
        self.guard
            .run_local_read(
                context,
                operation,
                requested_workspace,
                raw_arguments,
                move |request| async move {
                    let result = memory::list_subjects(
                        pool,
                        request.authorization().clone(),
                        request.workspace_id(),
                        bootstrap,
                        limit,
                    )
                    .await?;
                    let value = serde_json::to_value(result).map_err(|_| ErrorCode::Internal)?;
                    catalog.validate_output(ToolName::Memory, &value)?;
                    output(value)
                },
            )
            .await
    }

    /// §6.1.3 / ADR-0028 D-F (card 7 D-E1): `memory.subject_register` / `memory.subject_link_key`
    /// — the two non-destructive registry writes, through the guard's admitted-write runner (no
    /// confirm gate). Kinds/roles/key kinds are the closed domain sets; anything else is
    /// INVALID_INPUT before admission.
    async fn memory_subject_write(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
        op: SubjectWriteOp,
    ) -> Result<ToolOutput, ErrorCode> {
        let requested_workspace = workspace(value)?;
        let text = |field: &str| -> Result<String, ErrorCode> {
            value
                .get(field)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or(ErrorCode::InvalidInput)
        };
        enum Write {
            Register {
                kind: SubjectKind,
                display_name: String,
                roles: Vec<SubjectRole>,
            },
            LinkKey {
                subject_id: SubjectId,
                key: SubjectKey,
            },
        }
        let write = match op {
            SubjectWriteOp::Register => Write::Register {
                kind: SubjectKind::parse(&text("kind")?).ok_or(ErrorCode::InvalidInput)?,
                display_name: text("display_name")?,
                roles: match value.get("roles") {
                    None => Vec::new(),
                    Some(list) => list
                        .as_array()
                        .ok_or(ErrorCode::InvalidInput)?
                        .iter()
                        .map(|r| {
                            r.as_str()
                                .and_then(SubjectRole::parse)
                                .ok_or(ErrorCode::InvalidInput)
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                },
            },
            SubjectWriteOp::LinkKey => Write::LinkKey {
                subject_id: SubjectId::parse(&text("subject_id")?)?,
                key: SubjectKey::new(
                    SubjectKeyKind::parse(&text("kind")?).ok_or(ErrorCode::InvalidInput)?,
                    text("value")?,
                )?,
            },
        };
        let pool = self.runtime_pool.clone();
        let bootstrap = self.context_bootstrap.clone();
        let catalog = self.catalog.clone();
        self.guard
            .run_local_write(
                context,
                operation,
                requested_workspace,
                raw_arguments,
                move |request| async move {
                    let authorization = request.authorization().clone();
                    let workspace = request.workspace_id();
                    let subject = match write {
                        Write::Register {
                            kind,
                            display_name,
                            roles,
                        } => {
                            memory::register_subject(
                                pool,
                                authorization,
                                workspace,
                                bootstrap,
                                kind,
                                display_name,
                                roles,
                            )
                            .await?
                        }
                        Write::LinkKey { subject_id, key } => {
                            memory::link_subject_key(
                                pool,
                                authorization,
                                workspace,
                                bootstrap,
                                subject_id,
                                key,
                            )
                            .await?
                        }
                    };
                    let value = serde_json::to_value(subject).map_err(|_| ErrorCode::Internal)?;
                    catalog.validate_output(ToolName::Memory, &value)?;
                    output(value)
                },
            )
            .await
    }

    /// §8.5.1 / ADR-0030 D-C `memory.annotate_affect` — the non-destructive affect write,
    /// through the guard's admitted-write runner (no confirm gate, like the subject registry
    /// ops). Kinds/labels/scope kinds are the closed domain sets and every basis-point value is
    /// range-checked by the domain constructors: anything outside is INVALID_INPUT before
    /// admission, never clamped.
    async fn memory_annotate_affect(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
    ) -> Result<ToolOutput, ErrorCode> {
        let memory_id =
            MemoryId::parse(value["memory_id"].as_str().ok_or(ErrorCode::InvalidInput)?)?;
        let inputs = affect_repo::parse_affects(value)?;
        if inputs.is_empty() {
            return Err(ErrorCode::InvalidInput);
        }
        let requested_workspace = workspace(value)?;
        let Some(mood_half_life) = self.mood_half_life else {
            return self
                .reject_unsupported(context, operation, requested_workspace)
                .await;
        };
        let pool = self.runtime_pool.clone();
        let bootstrap = self.context_bootstrap.clone();
        let catalog = self.catalog.clone();
        self.guard
            .run_local_write(
                context,
                operation,
                requested_workspace,
                raw_arguments,
                move |request| async move {
                    let result = memory::annotate_affect(
                        pool,
                        request.authorization().clone(),
                        request.workspace_id(),
                        bootstrap,
                        memory_id,
                        inputs,
                        mood_half_life,
                    )
                    .await?;
                    let value = serde_json::to_value(result).map_err(|_| ErrorCode::Internal)?;
                    catalog.validate_output(ToolName::Memory, &value)?;
                    output(value)
                },
            )
            .await
    }

    /// §36 `memory.supersede` through the shared §33.10 confirm gate (ADR-0018). The
    /// schema carries no `workspace_id`: the route is the credential's bound workspace,
    /// which must be the bootstrap projection stream's workspace (the confirm-gated governance
    /// writers stay bootstrap-bound; `remember.put` and the reads derive their stream per
    /// request, ADR-0031 / ADR-0032).
    async fn memory_supersede(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
    ) -> Result<ToolOutput, ErrorCode> {
        let target = MemoryId::parse(value["memory_id"].as_str().ok_or(ErrorCode::InvalidInput)?)?;
        let successor = MemoryId::parse(
            value["replacement_memory_id"]
                .as_str()
                .ok_or(ErrorCode::InvalidInput)?,
        )?;
        let presented = value
            .get("confirm_token")
            .map(|token| {
                token
                    .as_str()
                    .ok_or(ErrorCode::InvalidInput)
                    .and_then(ConfirmToken::decode)
            })
            .transpose()?;
        let (Some(ttl), Some(undo_window)) = (self.confirm_token_ttl, self.undo_window) else {
            return self.reject_unsupported(context, operation, None).await;
        };
        let pool = self.runtime_pool.clone();
        let stream = self.context_bootstrap.stream.clone();
        let outcome = self
            .guard
            .run_confirmed_write(
                context,
                operation,
                None,
                raw_arguments,
                ConfirmGate {
                    op: DestructiveOp::MemorySupersede,
                    target_id: target.0,
                    successor_id: Some(successor.0),
                    presented,
                    ttl,
                },
                move |write| async move {
                    memory::supersede(pool, write, stream, target, successor, undo_window).await
                },
            )
            .await?;
        match outcome {
            ConfirmedOutcome::ConfirmationRequired { token, expires_at } => {
                let value = json!({
                    "confirmation_required": true,
                    "confirm_token": token.encode(),
                    "operation": operation.operation_key(),
                    "target": {
                        "memory_id": target.0,
                        "replacement_memory_id": successor.0,
                    },
                    "expires_at": rfc3339(expires_at)?,
                });
                // Both supersede results are branches of memory.output.schema.json (tools/list
                // advertises it); validate like every other memory arm so the wire contract
                // cannot drift silently.
                self.catalog.validate_output(ToolName::Memory, &value)?;
                output(value)
            }
            ConfirmedOutcome::Executed(done) => {
                let value = json!({
                    "memory_id": target.0,
                    "replacement_memory_id": successor.0,
                    "superseded_at": rfc3339(done.superseded_at)?,
                    "stream_seq": done.stream_seq,
                    "commit_seq": done.commit_seq,
                });
                self.catalog.validate_output(ToolName::Memory, &value)?;
                output(value)
            }
        }
    }

    /// §Q4 `memory.correct` through the same §33.10 confirm gate (ADR-0025). One transaction
    /// inserts a new DirectUserInput Evidence + a new Memory version and supersedes the
    /// original (reason USER_CORRECTION); the original Evidence body is never edited in place.
    /// No successor on the wire (M2 is minted inside the confirmed transaction, so the token
    /// carries no successor). Same workspace rule as `memory.get` / `memory.supersede`.
    async fn memory_correct(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
    ) -> Result<ToolOutput, ErrorCode> {
        let target = MemoryId::parse(value["memory_id"].as_str().ok_or(ErrorCode::InvalidInput)?)?;
        let text = value["text"].as_str().ok_or(ErrorCode::InvalidInput)?;
        // The corrected content is stored verbatim as E2's payload and M2's content; the digest
        // is over the exact raw bytes (§48.0①: the sole constructor, no normalization).
        let content = Value::String(text.to_owned());
        let content_raw = serde_json::to_vec(&content).map_err(|_| ErrorCode::Internal)?;
        let digest = payload_sha256(&content_raw);
        let presented = value
            .get("confirm_token")
            .map(|token| {
                token
                    .as_str()
                    .ok_or(ErrorCode::InvalidInput)
                    .and_then(ConfirmToken::decode)
            })
            .transpose()?;
        let (Some(ttl), Some(undo_window)) = (self.confirm_token_ttl, self.undo_window) else {
            return self.reject_unsupported(context, operation, None).await;
        };
        let subjects = subject_repo::parse_declaration(value)?;
        // §8.5.1 / ADR-0030 D-E: a correction re-supplies the new version's affects (the old
        // rows ride with the superseded version). Parsed before the gate (malformed ⇒
        // INVALID_INPUT, token untouched); their target subjects resolve before the consume and
        // the rows are written inside correct_atomically's transaction — one ticket, no second
        // transaction.
        let affects = affect_repo::parse_affects(value)?;
        let mood_half_life = match (affects.is_empty(), self.mood_half_life) {
            (true, _) => None,
            (false, Some(half_life)) => Some(half_life),
            (false, None) => return self.reject_unsupported(context, operation, None).await,
        };
        let pool = self.runtime_pool.clone();
        let stream = self.context_bootstrap.stream.clone();
        let consistency_token_ttl = self.remember_policy.consistency_token_ttl();
        let outcome = self
            .guard
            .run_confirmed_write(
                context,
                operation,
                None,
                raw_arguments,
                ConfirmGate {
                    op: DestructiveOp::MemoryCorrect,
                    target_id: target.0,
                    successor_id: None,
                    presented,
                    ttl,
                },
                move |write| async move {
                    memory::correct(
                        pool,
                        write,
                        stream,
                        target,
                        content,
                        digest,
                        undo_window,
                        consistency_token_ttl,
                        subjects,
                        affects,
                        mood_half_life,
                    )
                    .await
                },
            )
            .await?;
        let value = match outcome {
            ConfirmedOutcome::ConfirmationRequired { token, expires_at } => json!({
                "confirmation_required": true,
                "confirm_token": token.encode(),
                "operation": operation.operation_key(),
                "target": { "memory_id": target.0 },
                "expires_at": rfc3339(expires_at)?,
            }),
            ConfirmedOutcome::Executed(done) => json!({
                "memory_id": done.new_memory_id.0,
                "superseded": done.superseded.0,
                "evidence_id": done.evidence_id,
                "superseded_at": rfc3339(done.superseded_at)?,
                "stream_seq": done.stream_seq,
                "commit_seq": done.commit_seq,
                "consistency_token": done.consistency_token,
                "subject_ids": done.subject_ids,
                "affect_ids": done.affect_ids,
            }),
        };
        self.catalog.validate_output(ToolName::Memory, &value)?;
        output(value)
    }

    /// §36 `memory.restore` through the same §33.10 confirm gate (ADR-0020). Undoes a
    /// SUPERSEDE within the window: reuses card 1's token (no successor argument), returns the
    /// reactivated memory on a new stream seq with a new consistency_token, or a success-shaped
    /// `{code:"CONFLICT", reason:<u16>}` when the undo is refused (D-B — §52.1 keeps 18 codes).
    async fn memory_restore(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
    ) -> Result<ToolOutput, ErrorCode> {
        let target = MemoryId::parse(value["memory_id"].as_str().ok_or(ErrorCode::InvalidInput)?)?;
        let presented = value
            .get("confirm_token")
            .map(|token| {
                token
                    .as_str()
                    .ok_or(ErrorCode::InvalidInput)
                    .and_then(ConfirmToken::decode)
            })
            .transpose()?;
        // Both lifecycle-writing routes gate on the undo-window config together; restore reads
        // the stored deadline but stays disabled until the process declares the window.
        let (Some(ttl), Some(_window)) = (self.confirm_token_ttl, self.undo_window) else {
            return self.reject_unsupported(context, operation, None).await;
        };
        let pool = self.runtime_pool.clone();
        let stream = self.context_bootstrap.stream.clone();
        let consistency_token_ttl = self.remember_policy.consistency_token_ttl();
        let outcome = self
            .guard
            .run_confirmed_write(
                context,
                operation,
                None,
                raw_arguments,
                ConfirmGate {
                    op: DestructiveOp::MemoryRestore,
                    target_id: target.0,
                    successor_id: None,
                    presented,
                    ttl,
                },
                move |write| async move {
                    memory::restore(pool, write, stream, target, consistency_token_ttl).await
                },
            )
            .await?;
        let value = match outcome {
            ConfirmedOutcome::ConfirmationRequired { token, expires_at } => json!({
                "confirmation_required": true,
                "confirm_token": token.encode(),
                "operation": operation.operation_key(),
                "target": { "memory_id": target.0 },
                "expires_at": rfc3339(expires_at)?,
            }),
            ConfirmedOutcome::Executed(RestoreResult::Restored(done)) => json!({
                "memory_id": target.0,
                "restored_at": rfc3339(done.restored_at)?,
                "stream_seq": done.stream_seq,
                "commit_seq": done.commit_seq,
                "consistency_token": done.consistency_token,
            }),
            // D-B: a refused undo is a success-shaped CONFLICT-with-reason result, not an
            // ErrorCode (which would carry no reason through the frozen §52.1 error map).
            ConfirmedOutcome::Executed(RestoreResult::Refused(reason)) => json!({
                "code": "CONFLICT",
                "reason": reason.code(),
                "reason_label": reason.label().unwrap_or("UNKNOWN"),
            }),
        };
        self.catalog.validate_output(ToolName::Memory, &value)?;
        output(value)
    }

    /// §36/§10.1 `memory.confirm` through the same §33.10 confirm gate (ADR-0026, Card 6).
    /// Promotes a `private.distill_candidates` row into UserConfirmed Evidence + a new Memory.
    /// The gate target is the `candidate_id` (not a memory_id); `candidate_sha256` binds the
    /// confirm to the exact reviewed body. A refused confirm (already confirmed / expired) is a
    /// success-shaped `{code:CONFLICT, reason:<u16>}` (§52.1 keeps 18 codes).
    async fn memory_confirm(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
    ) -> Result<ToolOutput, ErrorCode> {
        let candidate_id = Uuid::parse_str(
            value["candidate_id"]
                .as_str()
                .ok_or(ErrorCode::InvalidInput)?,
        )
        .map_err(|_| ErrorCode::InvalidInput)?;
        let candidate_sha256 = hex::decode(
            value["candidate_sha256"]
                .as_str()
                .ok_or(ErrorCode::InvalidInput)?,
        )
        .map_err(|_| ErrorCode::InvalidInput)?;
        if candidate_sha256.len() != 32 {
            return Err(ErrorCode::InvalidInput);
        }
        let presented = value
            .get("confirm_token")
            .map(|token| {
                token
                    .as_str()
                    .ok_or(ErrorCode::InvalidInput)
                    .and_then(ConfirmToken::decode)
            })
            .transpose()?;
        let Some(ttl) = self.confirm_token_ttl else {
            return self.reject_unsupported(context, operation, None).await;
        };
        let subjects = subject_repo::parse_declaration(value)?;
        let pool = self.runtime_pool.clone();
        let stream = self.context_bootstrap.stream.clone();
        let consistency_token_ttl = self.remember_policy.consistency_token_ttl();
        let outcome = self
            .guard
            .run_confirmed_write(
                context,
                operation,
                None,
                raw_arguments,
                ConfirmGate {
                    op: DestructiveOp::MemoryConfirm,
                    target_id: candidate_id,
                    successor_id: None,
                    presented,
                    ttl,
                },
                move |write| async move {
                    memory::confirm(
                        pool,
                        write,
                        stream,
                        candidate_id,
                        candidate_sha256,
                        consistency_token_ttl,
                        subjects,
                    )
                    .await
                },
            )
            .await?;
        let value = match outcome {
            ConfirmedOutcome::ConfirmationRequired { token, expires_at } => json!({
                "confirmation_required": true,
                "confirm_token": token.encode(),
                "operation": operation.operation_key(),
                "target": { "candidate_id": candidate_id },
                "expires_at": rfc3339(expires_at)?,
            }),
            ConfirmedOutcome::Executed(ConfirmOutcome::Confirmed(done)) => json!({
                "memory_id": done.memory_id,
                "evidence_id": done.evidence_id,
                "candidate_id": done.candidate_id,
                "stream_seq": done.stream_seq,
                "commit_seq": done.commit_seq,
                "consistency_token": done.consistency_token,
                "subject_ids": done.subject_ids,
            }),
            ConfirmedOutcome::Executed(ConfirmOutcome::Refused(reason)) => json!({
                "code": "CONFLICT",
                "reason": reason.code(),
                "reason_label": reason.label().unwrap_or("UNKNOWN"),
            }),
        };
        self.catalog.validate_output(ToolName::Memory, &value)?;
        output(value)
    }

    /// §36 `memory.reject` through the same §33.10 confirm gate (ADR-0026, Card 6). Marks a
    /// pending candidate REJECTED; writes no Evidence/Memory.
    async fn memory_reject(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
    ) -> Result<ToolOutput, ErrorCode> {
        let candidate_id = Uuid::parse_str(
            value["candidate_id"]
                .as_str()
                .ok_or(ErrorCode::InvalidInput)?,
        )
        .map_err(|_| ErrorCode::InvalidInput)?;
        let presented = value
            .get("confirm_token")
            .map(|token| {
                token
                    .as_str()
                    .ok_or(ErrorCode::InvalidInput)
                    .and_then(ConfirmToken::decode)
            })
            .transpose()?;
        let Some(ttl) = self.confirm_token_ttl else {
            return self.reject_unsupported(context, operation, None).await;
        };
        let pool = self.runtime_pool.clone();
        let stream = self.context_bootstrap.stream.clone();
        let outcome = self
            .guard
            .run_confirmed_write(
                context,
                operation,
                None,
                raw_arguments,
                ConfirmGate {
                    op: DestructiveOp::MemoryReject,
                    target_id: candidate_id,
                    successor_id: None,
                    presented,
                    ttl,
                },
                move |write| async move { memory::reject(pool, write, stream, candidate_id).await },
            )
            .await?;
        let value = match outcome {
            ConfirmedOutcome::ConfirmationRequired { token, expires_at } => json!({
                "confirmation_required": true,
                "confirm_token": token.encode(),
                "operation": operation.operation_key(),
                "target": { "candidate_id": candidate_id },
                "expires_at": rfc3339(expires_at)?,
            }),
            ConfirmedOutcome::Executed(RejectOutcome::Rejected(candidate_id)) => json!({
                "candidate_id": candidate_id,
                "state": "rejected",
            }),
            ConfirmedOutcome::Executed(RejectOutcome::Refused(reason)) => json!({
                "code": "CONFLICT",
                "reason": reason.code(),
                "reason_label": reason.label().unwrap_or("UNKNOWN"),
            }),
        };
        self.catalog.validate_output(ToolName::Memory, &value)?;
        output(value)
    }

    /// §36 `memory.pin` / `memory.unpin` through the same §33.10 confirm gate (ADR-0019).
    /// One arm for both: identical wire shape (`memory_id` + optional `confirm_token`), the
    /// closed `op` is the only difference, and the token is bound to it (a pin confirmation
    /// never executes an unpin). No `workspace_id` on the wire: the route is the credential's
    /// bound workspace (same rule as `memory.get` / `memory.supersede`).
    async fn memory_binding_write(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
        op: DestructiveOp,
    ) -> Result<ToolOutput, ErrorCode> {
        let memory = MemoryId::parse(value["memory_id"].as_str().ok_or(ErrorCode::InvalidInput)?)?;
        let presented = value
            .get("confirm_token")
            .map(|token| {
                token
                    .as_str()
                    .ok_or(ErrorCode::InvalidInput)
                    .and_then(ConfirmToken::decode)
            })
            .transpose()?;
        let Some(ttl) = self.confirm_token_ttl else {
            return self.reject_unsupported(context, operation, None).await;
        };
        let pool = self.runtime_pool.clone();
        let stream = self.context_bootstrap.stream.clone();
        let outcome =
            self.guard
                .run_confirmed_write(
                    context,
                    operation,
                    None,
                    raw_arguments,
                    ConfirmGate {
                        op,
                        target_id: memory.0,
                        successor_id: None,
                        presented,
                        ttl,
                    },
                    move |write| async move {
                        memory::write_binding(pool, write, stream, op, memory).await
                    },
                )
                .await?;
        let value = match outcome {
            ConfirmedOutcome::ConfirmationRequired { token, expires_at } => json!({
                "confirmation_required": true,
                "confirm_token": token.encode(),
                "operation": operation.operation_key(),
                "target": { "memory_id": memory.0 },
                "expires_at": rfc3339(expires_at)?,
            }),
            ConfirmedOutcome::Executed(done) => json!({
                "memory_id": memory.0,
                "binding_id": done.binding_id,
                "mode": "PINNED",
                "state": if op == DestructiveOp::MemoryPin { "pinned" } else { "unpinned" },
                "inserted": done.inserted,
            }),
        };
        // Both results are branches of memory.output.schema.json (tools/list advertises it).
        self.catalog.validate_output(ToolName::Memory, &value)?;
        output(value)
    }

    /// §36 `memory.archive` / `memory.unarchive` through the same §33.10 confirm gate
    /// (ADR-0024, Q3). One arm for both: identical wire shape (`memory_id` + optional
    /// `confirm_token`), the closed `op` is the only difference and the token is bound to it (an
    /// archive confirmation never runs an unarchive). No `workspace_id` on the wire: the route
    /// is the credential's bound workspace (same rule as `memory.get` / `memory.supersede`).
    /// Archive appends an ARCHIVE lifecycle event and sets `archived_at`; unarchive appends a
    /// RESTORE undoing it and clears `archived_at`. An already-in-state call is a success-shaped
    /// `{code:"CONFLICT", reason:1201}` (D-B — §52.1 keeps 18 codes).
    async fn memory_archive_write(
        &self,
        context: &McpHttpContext,
        operation: &OperationDescriptor,
        raw_arguments: &str,
        value: &Value,
        op: DestructiveOp,
    ) -> Result<ToolOutput, ErrorCode> {
        let memory = MemoryId::parse(value["memory_id"].as_str().ok_or(ErrorCode::InvalidInput)?)?;
        let presented = value
            .get("confirm_token")
            .map(|token| {
                token
                    .as_str()
                    .ok_or(ErrorCode::InvalidInput)
                    .and_then(ConfirmToken::decode)
            })
            .transpose()?;
        let Some(ttl) = self.confirm_token_ttl else {
            return self.reject_unsupported(context, operation, None).await;
        };
        let pool = self.runtime_pool.clone();
        let stream = self.context_bootstrap.stream.clone();
        let outcome = self
            .guard
            .run_confirmed_write(
                context,
                operation,
                None,
                raw_arguments,
                ConfirmGate {
                    op,
                    target_id: memory.0,
                    successor_id: None,
                    presented,
                    ttl,
                },
                move |write| async move { memory::archive(pool, write, stream, op, memory).await },
            )
            .await?;
        let changed_field = if op == DestructiveOp::MemoryArchive {
            "archived_at"
        } else {
            "unarchived_at"
        };
        let value = match outcome {
            ConfirmedOutcome::ConfirmationRequired { token, expires_at } => json!({
                "confirmation_required": true,
                "confirm_token": token.encode(),
                "operation": operation.operation_key(),
                "target": { "memory_id": memory.0 },
                "expires_at": rfc3339(expires_at)?,
            }),
            ConfirmedOutcome::Executed(ArchiveResult::Done(done)) => json!({
                "memory_id": memory.0,
                changed_field: rfc3339(done.changed_at)?,
                "stream_seq": done.stream_seq,
                "commit_seq": done.commit_seq,
            }),
            // D-B: an already-in-state archive/unarchive is a success-shaped CONFLICT-with-reason.
            ConfirmedOutcome::Executed(ArchiveResult::Refused(reason)) => json!({
                "code": "CONFLICT",
                "reason": reason.code(),
                "reason_label": reason.label().unwrap_or("UNKNOWN"),
            }),
        };
        self.catalog.validate_output(ToolName::Memory, &value)?;
        output(value)
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
        // §6.1.3/ADR-0029: `subject_ids` is schema-validated as an array of uuid strings
        // upstream; a malformed element here is still INVALID_INPUT, never a silent drop.
        let subject_ids = value
            .get("subject_ids")
            .map(|ids| {
                ids.as_array()
                    .ok_or(ErrorCode::InvalidInput)?
                    .iter()
                    .map(|id| SubjectId::parse(id.as_str().ok_or(ErrorCode::InvalidInput)?))
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        // §8.5.1/ADR-0030 D-D: the explicit affect query + optional mood point, parsed the one
        // way every affect-carrying op parses (closed sets, basis-point ranges, lo <= hi).
        let affect = affect_repo::parse_filter(value)?;
        let mood_congruence = affect_repo::parse_mood(value)?;
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
            subject_ids,
            affect,
            mood_congruence,
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
            key if key == DestructiveOp::MemorySupersede.operation_key() => {
                self.memory_supersede(context, &operation, &raw_arguments, &value)
                    .await
            }
            key if key == DestructiveOp::MemoryCorrect.operation_key() => {
                self.memory_correct(context, &operation, &raw_arguments, &value)
                    .await
            }
            key if key == DestructiveOp::MemoryConfirm.operation_key() => {
                self.memory_confirm(context, &operation, &raw_arguments, &value)
                    .await
            }
            key if key == DestructiveOp::MemoryReject.operation_key() => {
                self.memory_reject(context, &operation, &raw_arguments, &value)
                    .await
            }
            key if key == DestructiveOp::MemoryPin.operation_key() => {
                self.memory_binding_write(
                    context,
                    &operation,
                    &raw_arguments,
                    &value,
                    DestructiveOp::MemoryPin,
                )
                .await
            }
            key if key == DestructiveOp::MemoryUnpin.operation_key() => {
                self.memory_binding_write(
                    context,
                    &operation,
                    &raw_arguments,
                    &value,
                    DestructiveOp::MemoryUnpin,
                )
                .await
            }
            key if key == DestructiveOp::MemoryRestore.operation_key() => {
                self.memory_restore(context, &operation, &raw_arguments, &value)
                    .await
            }
            key if key == DestructiveOp::MemoryArchive.operation_key() => {
                self.memory_archive_write(
                    context,
                    &operation,
                    &raw_arguments,
                    &value,
                    DestructiveOp::MemoryArchive,
                )
                .await
            }
            key if key == DestructiveOp::MemoryUnarchive.operation_key() => {
                self.memory_archive_write(
                    context,
                    &operation,
                    &raw_arguments,
                    &value,
                    DestructiveOp::MemoryUnarchive,
                )
                .await
            }
            key if SubjectWriteOp::parse_operation_key(key).is_some() => {
                let op = SubjectWriteOp::parse_operation_key(key).ok_or(ErrorCode::Internal)?;
                self.memory_subject_write(context, &operation, &raw_arguments, &value, op)
                    .await
            }
            key if AffectWriteOp::parse_operation_key(key) == Some(AffectWriteOp::Annotate) => {
                self.memory_annotate_affect(context, &operation, &raw_arguments, &value)
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
    /// ADR-0032 D-B per-call overrides, each parsed into its closed set (unknown ⇒
    /// `INVALID_INPUT` before admission; the schema already refuses them at the catalog).
    visibility_class: Option<VisibilityClass>,
    data_class: Option<DataClass>,
    event_kind: Option<RememberEventKind>,
}

#[derive(Deserialize)]
struct RawRememberPutWire {
    content: Box<RawValue>,
    idempotency_key: String,
    workspace_id: Option<Uuid>,
    visibility_class: Option<String>,
    data_class: Option<String>,
    event_kind: Option<String>,
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
            visibility_class: raw
                .visibility_class
                .as_deref()
                .map(remember::parse_visibility_class)
                .transpose()?,
            data_class: raw
                .data_class
                .as_deref()
                .map(remember::parse_data_class)
                .transpose()?,
            event_kind: raw
                .event_kind
                .as_deref()
                .map(RememberEventKind::parse)
                .transpose()?,
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

fn rfc3339(at: OffsetDateTime) -> Result<String, ErrorCode> {
    at.format(&Rfc3339).map_err(|_| ErrorCode::Internal)
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
