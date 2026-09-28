//! `gateway::remember` — Guarded gateway entry point for one authenticated `remember` operation.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-projection, serde_json, sqlx, time,
//!   uuid]; services=[]; env=[]; modules=[adapters::affect_repo, adapters::postgres, adapters::remember,
//!   domain::affect, domain::dataclass, domain::error, domain::evidence, domain::identity, domain::ids,
//!   domain::subject, projection::stream]
//! Called-by: [gateway::bootstrap, gateway::context, gateway::mcp_application, tests]
//! Invariants: [the protocol layer decodes wire input and the request guard produces the AuthorizationScope; this module never deserializes either boundary itself]
//! Spec: Baseline §11.2.1; §15.5; §78.2; ADR-0020; ADR-0032
//!
//! The protocol layer decodes wire input and the request guard produces the
//! [`AuthorizationScope`]. This module does not deserialize either of them.

use std::time::Duration;

use humaux_adapters::affect_repo::AffectInput;
use humaux_adapters::postgres::RuntimeDbPool;
use humaux_adapters::remember::{self, RememberAccepted, RememberCommand};
use humaux_domain::affect::MoodHalfLife;
use humaux_domain::dataclass::DataClass;
use humaux_domain::error::ErrorCode;
use humaux_domain::evidence::{EvidenceOriginClass, payload_sha256};
use humaux_domain::identity::{AuthorizationScope, VisibilityClass};
use humaux_domain::ids::WorkspaceId;
use humaux_domain::subject::SubjectDeclaration;
use humaux_projection::stream::StreamKey;
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

/// The frozen `private.events.event_kind` vocabulary accepted by `remember`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RememberEventKind {
    /// A user-originated message represented by this agent-facing ingress path.
    UserMessage,
    /// An assistant message.
    AssistantMessage,
    /// A tool invocation.
    ToolCall,
    /// A tool result.
    ToolResult,
    /// A manual note.
    ManualNote,
    /// An explicit user correction.
    UserCorrection,
    /// A task lifecycle event.
    TaskEvent,
    /// A source-control event.
    GitEvent,
    /// A trusted system import.
    SystemImport,
}

impl RememberEventKind {
    /// The closed set, in `private.events.event_kind` CHECK order (§78.2).
    pub const ALL: [Self; 9] = [
        Self::UserMessage,
        Self::AssistantMessage,
        Self::ToolCall,
        Self::ToolResult,
        Self::ManualNote,
        Self::UserCorrection,
        Self::TaskEvent,
        Self::GitEvent,
        Self::SystemImport,
    ];

    /// Parses the wire/env spelling; anything outside the closed set is `INVALID_INPUT`.
    /// One parser for bootstrap (`HUMAUX_GATEWAY_REMEMBER_EVENT_KIND`) and the per-call
    /// `event_kind` argument (ADR-0032 D-B).
    pub fn parse(value: &str) -> Result<Self, ErrorCode> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.as_db_str() == value)
            .ok_or(ErrorCode::InvalidInput)
    }

    const fn as_db_str(self) -> &'static str {
        match self {
            Self::UserMessage => "USER_MESSAGE",
            Self::AssistantMessage => "ASSISTANT_MESSAGE",
            Self::ToolCall => "TOOL_CALL",
            Self::ToolResult => "TOOL_RESULT",
            Self::ManualNote => "MANUAL_NOTE",
            Self::UserCorrection => "USER_CORRECTION",
            Self::TaskEvent => "TASK_EVENT",
            Self::GitEvent => "GIT_EVENT",
            Self::SystemImport => "SYSTEM_IMPORT",
        }
    }
}

/// Parses a `DataClass` wire/env spelling (§78.2 closed set); unknown is `INVALID_INPUT`.
/// One parser for bootstrap (`HUMAUX_GATEWAY_REMEMBER_DATA_CLASS`) and the per-call
/// `data_class` argument (ADR-0032 D-B).
pub fn parse_data_class(value: &str) -> Result<DataClass, ErrorCode> {
    DataClass::ALL
        .into_iter()
        .find(|class| class.as_str() == value)
        .ok_or(ErrorCode::InvalidInput)
}

/// The three §6.1.1 visibility classes in their `private.evidence_objects.visibility_class`
/// spelling (§78.2: the DB<->Rust mapping lives here once, next to the write that stores it).
pub const fn visibility_class_db_str(class: VisibilityClass) -> &'static str {
    match class {
        VisibilityClass::UserPrivate => "USER_PRIVATE",
        VisibilityClass::WorkspaceShared => "WORKSPACE_SHARED",
        VisibilityClass::TenantShared => "TENANT_SHARED",
    }
}

/// Parses a visibility class spelling; unknown is `INVALID_INPUT`. One parser for bootstrap
/// (`HUMAUX_GATEWAY_REMEMBER_VISIBILITY_CLASS`, which then refuses `TENANT_SHARED` as a
/// process default) and the per-call `visibility_class` argument (ADR-0032 D-B).
pub fn parse_visibility_class(value: &str) -> Result<VisibilityClass, ErrorCode> {
    [
        VisibilityClass::UserPrivate,
        VisibilityClass::WorkspaceShared,
        VisibilityClass::TenantShared,
    ]
    .into_iter()
    .find(|class| visibility_class_db_str(*class) == value)
    .ok_or(ErrorCode::InvalidInput)
}

/// One `remember.put`'s classification (ADR-0032 D-B): the process defaults with the call's
/// optional `visibility_class` / `data_class` / `event_kind` applied, each already parsed
/// into its closed set. Whether the chosen visibility is *reachable* for this caller is
/// [`command`]'s (scope) and the receipt transaction's (membership role) job, not the
/// parser's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutClassification {
    pub visibility_class: VisibilityClass,
    pub data_class: DataClass,
    pub event_kind: RememberEventKind,
}

/// Bootstrap-owned immutable configuration for the gateway write route.
///
/// Since ADR-0032 (card 11) the `stream` here is the process family plus the *default* pair:
/// `remember.put` derives its own `StreamKey` per request (principal tenant + requested
/// workspace + this family's `(scope_kind, domain, projection_kind, projection_version)`,
/// the same derivation the read routes use) and falls back to this pair's workspace only when
/// the call names none; the confirm-gated governance writers still compare against it. The
/// reasoning domain and token lifetime are never supplied by a tool argument; `data_class`
/// and `visibility_class` are the per-call defaults (a `TENANT_SHARED` default is refused —
/// that class is per-call and membership-gated only). Only workspace-scoped stream families
/// are enabled here; another scope requires a separately reviewed operation.
#[derive(Debug, Clone)]
pub struct RememberPolicy {
    stream: StreamKey,
    reasoning_domain_id: Uuid,
    consistency_token_ttl: time::Duration,
    data_class: DataClass,
    visibility_class: VisibilityClass,
}

impl RememberPolicy {
    pub(crate) fn stream_key(&self) -> &StreamKey {
        &self.stream
    }

    pub(crate) fn workspace_id(&self) -> WorkspaceId {
        WorkspaceId(self.stream.scope_id)
    }

    /// The per-call `visibility_class` default (`HUMAUX_GATEWAY_REMEMBER_VISIBILITY_CLASS`).
    pub(crate) fn visibility_class(&self) -> VisibilityClass {
        self.visibility_class
    }

    /// The per-call `data_class` default (`HUMAUX_GATEWAY_REMEMBER_DATA_CLASS`).
    pub(crate) fn data_class(&self) -> DataClass {
        self.data_class
    }

    /// §15.5 consistency_token lifetime — reused by `memory.restore` for the token it returns
    /// on the restored memory's new stream seq (ADR-0020). Positive by construction.
    pub(crate) fn consistency_token_ttl(&self) -> Duration {
        Duration::try_from(self.consistency_token_ttl).unwrap_or(Duration::from_secs(0))
    }

    /// Builds a policy after bootstrap has selected its trusted stream and expiry.
    pub fn new(
        stream: StreamKey,
        reasoning_domain_id: Uuid,
        consistency_token_ttl: Duration,
        data_class: DataClass,
        visibility_class: VisibilityClass,
    ) -> Result<Self, ErrorCode> {
        if stream.tenant_id.0.is_nil()
            || stream.scope_kind != "workspace"
            || stream.scope_id.is_nil()
            || stream.domain.is_empty()
            || stream.projection_kind.is_empty()
            || stream.projection_version.is_empty()
            || reasoning_domain_id.is_nil()
            || consistency_token_ttl.is_zero()
            || visibility_class == VisibilityClass::TenantShared
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            stream,
            reasoning_domain_id,
            consistency_token_ttl: consistency_token_ttl
                .try_into()
                .map_err(|_| ErrorCode::InvalidInput)?,
            data_class,
            visibility_class,
        })
    }
}

/// A raw JSON payload paired with the value parsed from those exact bytes.
///
/// The gateway hashes `raw_json` without normalizing it. The Protocol layer must
/// pass the original request bytes and its parsed value together; unrelated bytes
/// and values are rejected before the database transaction begins.
#[derive(Debug, Clone)]
pub struct PreparedEvidencePayload {
    raw_json: Vec<u8>,
    value: Value,
}

impl PreparedEvidencePayload {
    /// Binds raw JSON bytes to their matching parsed JSON value.
    pub fn new(raw_json: Vec<u8>, value: Value) -> Result<Self, ErrorCode> {
        if serde_json::from_slice::<Value>(&raw_json).ok().as_ref() != Some(&value) {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self { raw_json, value })
    }

    /// The exact raw bytes used for the Evidence digest.
    pub fn raw_json(&self) -> &[u8] {
        &self.raw_json
    }
}

/// Accepts one already-guarded event through the real Evidence/outbox transaction.
///
/// `authorization` must come from gateway authentication and authorization, not a
/// tool request. The raw payload is neither trimmed, normalized, nor reconstructed
/// from its parsed JSON value before it is hashed.
pub async fn put(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    policy: &RememberPolicy,
    content: PreparedEvidencePayload,
    event_kind: RememberEventKind,
    occurred_at: Option<OffsetDateTime>,
    now: OffsetDateTime,
) -> Result<RememberAccepted, ErrorCode> {
    let cmd = command(
        authorization,
        policy,
        policy.stream_key(),
        content,
        PutClassification {
            visibility_class: policy.visibility_class,
            data_class: policy.data_class,
            event_kind,
        },
        occurred_at,
        now,
        SubjectDeclaration::default(),
        Vec::new(),
        None,
    )?;
    remember::remember(pool, cmd)
        .await
        .map_err(map_remember_error)
}

/// Builds the guarded [`RememberCommand`] for one put.
///
/// `stream` is the per-request identity (ADR-0032 D-A: `ContextBootstrap::request_stream`
/// of the principal tenant and the membership-narrowed requested workspace); it must lie
/// inside `authorization` or the put is `FORBIDDEN` (defence in depth for in-process callers —
/// the HTTP route derives `stream` from `authorization` itself). The command carries
/// `policy.reasoning_domain_id` only as the process DEFAULT: the write transaction resolves the
/// caller tenant's own domain (`adapters::remember::resolve_reasoning_domain`, §11.2.1) and
/// migration 0159's composite FK refuses any cross-tenant domain — the tenant pin of the write
/// route is the database's, not this guard's. `classification.visibility_class` may
/// only narrow the caller's scope, never widen it (§6.1.1): `USER_PRIVATE` binds the
/// on-behalf-of user, `WORKSPACE_SHARED` binds the stream's (already narrowed) workspace,
/// `TENANT_SHARED` binds nothing here — its OWNER/ADMIN membership gate is judged inside the
/// receipt transaction (`operation_receipt::TENANT_SHARED_WRITER_ROLES`), which is the only
/// place that can read the role without a second round trip.
#[allow(clippy::too_many_arguments)] // one remember.put's worth of gate-built inputs (subjects + affects ride with the Evidence)
pub(crate) fn command(
    authorization: &AuthorizationScope,
    policy: &RememberPolicy,
    stream: &StreamKey,
    content: PreparedEvidencePayload,
    classification: PutClassification,
    occurred_at: Option<OffsetDateTime>,
    now: OffsetDateTime,
    subjects: SubjectDeclaration,
    affects: Vec<AffectInput>,
    mood_half_life: Option<MoodHalfLife>,
) -> Result<RememberCommand, ErrorCode> {
    if authorization.user_id().is_none()
        || stream.tenant_id != authorization.tenant_id()
        || stream.scope_kind != "workspace"
        || !authorization
            .allowed_workspace_ids()
            .contains(&WorkspaceId(stream.scope_id))
    {
        return Err(ErrorCode::Forbidden);
    }
    let consistency_token_expires_at = now
        .checked_add(policy.consistency_token_ttl)
        .ok_or(ErrorCode::InvalidInput)?;
    let (visibility_user_id, visibility_workspace_id) = match classification.visibility_class {
        VisibilityClass::UserPrivate => (authorization.user_id().map(|user| user.0), None),
        VisibilityClass::WorkspaceShared => (None, Some(stream.scope_id)),
        VisibilityClass::TenantShared => (None, None),
    };

    Ok(RememberCommand {
        tenant_id: authorization.tenant_id().0,
        authorization_user_id: authorization.user_id().map(|user| user.0),
        scope_kind: stream.scope_kind.clone(),
        scope_id: stream.scope_id,
        domain: stream.domain.clone(),
        projection_kind: stream.projection_kind.clone(),
        projection_version: stream.projection_version.clone(),
        consistency_token_expires_at,
        batch_id: None,
        payload_sha256: payload_sha256(content.raw_json()),
        data_class: classification.data_class.as_str().to_owned(),
        origin_class: EvidenceOriginClass::AuthenticatedAgent,
        origin_principal_id: Some(authorization.principal().0),
        origin_connector_id: None,
        visibility_class: visibility_class_db_str(classification.visibility_class).to_owned(),
        visibility_user_id,
        visibility_workspace_id,
        // The process default; `remember_in_txn` re-resolves it for the caller's tenant.
        reasoning_domain_id: policy.reasoning_domain_id,
        occurred_at,
        event_kind: classification.event_kind.as_db_str().to_owned(),
        event_payload: content.value,
        subjects,
        affects,
        mood_half_life,
    })
}

fn map_remember_error(error: remember::RememberError) -> ErrorCode {
    match error {
        remember::RememberError::ConsistencyTokenExpiryNotFuture => ErrorCode::InvalidInput,
        remember::RememberError::BatchExhausted => ErrorCode::Conflict,
        remember::RememberError::Subject(code) | remember::RememberError::Affect(code) => code,
        remember::RememberError::ReasoningDomainUnresolved => ErrorCode::DependencyUnavailable,
        remember::RememberError::Db(error) => match error {
            sqlx::Error::RowNotFound => ErrorCode::NotFound,
            sqlx::Error::Database(ref database) => match database.code().as_deref() {
                Some("23503") => ErrorCode::TenantBoundary,
                Some("42501") => ErrorCode::Forbidden,
                Some("23505") | Some("40001") | Some("40P01") => ErrorCode::Conflict,
                Some("22023") | Some("22P02") | Some("23514") => ErrorCode::InvalidInput,
                _ => ErrorCode::Internal,
            },
            _ => ErrorCode::DependencyUnavailable,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_domain::identity::{BoundedSet, PrincipalId};
    use humaux_domain::ids::{TenantId, UserId};

    fn authorization(
        tenant: TenantId,
        user: Option<UserId>,
        workspace: WorkspaceId,
    ) -> AuthorizationScope {
        AuthorizationScope::new(
            tenant,
            PrincipalId::new(),
            user,
            BoundedSet::new([workspace]).unwrap(),
        )
    }

    fn policy(
        tenant: TenantId,
        workspace: WorkspaceId,
        visibility_class: VisibilityClass,
    ) -> RememberPolicy {
        RememberPolicy::new(
            StreamKey::new(
                tenant,
                "workspace",
                workspace.0,
                "reasoning",
                "memory",
                "v1",
            ),
            Uuid::now_v7(),
            Duration::from_secs(60),
            DataClass::Private,
            visibility_class,
        )
        .unwrap()
    }

    fn payload(value: Value) -> PreparedEvidencePayload {
        PreparedEvidencePayload::new(serde_json::to_vec(&value).unwrap(), value).unwrap()
    }

    fn classify(policy: &RememberPolicy, event_kind: RememberEventKind) -> PutClassification {
        PutClassification {
            visibility_class: policy.visibility_class,
            data_class: policy.data_class,
            event_kind,
        }
    }

    #[test]
    fn rejects_userless_authorization() {
        let tenant = TenantId::new();
        let workspace = WorkspaceId::new();
        assert!(matches!(
            command(
                &authorization(tenant, None, workspace),
                &policy(tenant, workspace, VisibilityClass::UserPrivate),
                policy(tenant, workspace, VisibilityClass::UserPrivate).stream_key(),
                payload(Value::Null),
                classify(
                    &policy(tenant, workspace, VisibilityClass::UserPrivate),
                    RememberEventKind::ManualNote
                ),
                None,
                OffsetDateTime::now_utc(),
                SubjectDeclaration::default(),
                Vec::new(),
                None,
            ),
            Err(ErrorCode::Forbidden)
        ));
    }

    #[test]
    fn rejects_policy_workspace_outside_authorization() {
        let tenant = TenantId::new();
        let granted = WorkspaceId::new();
        assert!(matches!(
            command(
                &authorization(tenant, Some(UserId::new()), granted),
                &policy(tenant, granted, VisibilityClass::UserPrivate),
                policy(tenant, WorkspaceId::new(), VisibilityClass::UserPrivate).stream_key(),
                payload(Value::Null),
                classify(
                    &policy(tenant, granted, VisibilityClass::UserPrivate),
                    RememberEventKind::ManualNote
                ),
                None,
                OffsetDateTime::now_utc(),
                SubjectDeclaration::default(),
                Vec::new(),
                None,
            ),
            Err(ErrorCode::Forbidden)
        ));
    }

    #[test]
    fn agent_origin_and_private_visibility_come_from_authorization() {
        let tenant = TenantId::new();
        let workspace = WorkspaceId::new();
        let user = UserId::new();
        let policy = policy(tenant, workspace, VisibilityClass::UserPrivate);
        let cmd = command(
            &authorization(tenant, Some(user), workspace),
            &policy,
            policy.stream_key(),
            payload(Value::Null),
            classify(&policy, RememberEventKind::UserMessage),
            None,
            OffsetDateTime::now_utc(),
            SubjectDeclaration::default(),
            Vec::new(),
            None,
        )
        .unwrap();
        assert_eq!(cmd.origin_class, EvidenceOriginClass::AuthenticatedAgent);
        assert_eq!(cmd.visibility_class, "USER_PRIVATE");
        assert_eq!(cmd.visibility_user_id, Some(user.0));
        assert_eq!(cmd.authorization_user_id, Some(user.0));
        assert_eq!(cmd.visibility_workspace_id, None);
    }

    #[test]
    fn raw_json_bytes_are_hashed_without_normalization() {
        let raw = b"{\r\n  \"text\": \"e\xCC\x81\"\r\n}".to_vec();
        let value: Value = serde_json::from_slice(&raw).unwrap();
        let payload = PreparedEvidencePayload::new(raw.clone(), value.clone()).unwrap();
        let tenant = TenantId::new();
        let workspace = WorkspaceId::new();
        let policy = policy(tenant, workspace, VisibilityClass::UserPrivate);
        let cmd = command(
            &authorization(tenant, Some(UserId::new()), workspace),
            &policy,
            policy.stream_key(),
            payload,
            classify(&policy, RememberEventKind::UserMessage),
            None,
            OffsetDateTime::now_utc(),
            SubjectDeclaration::default(),
            Vec::new(),
            None,
        )
        .unwrap();
        assert_eq!(cmd.event_payload, value);
        assert_eq!(cmd.payload_sha256.to_hex(), payload_sha256(&raw).to_hex());
        assert_ne!(raw, serde_json::to_vec(&value).unwrap());
    }

    #[test]
    fn rejects_bytes_that_do_not_parse_to_the_supplied_value() {
        assert!(matches!(
            PreparedEvidencePayload::new(b"{\"a\": 1}".to_vec(), serde_json::json!({"a": 2})),
            Err(ErrorCode::InvalidInput)
        ));
    }

    /// ADR-0032 D-B: the per-call class only narrows the scope — a derived stream outside the
    /// authorization is refused before any class is applied, TENANT_SHARED binds no user or
    /// workspace (its role gate is the receipt transaction's), and the three parsers are the
    /// closed sets, nothing else.
    #[test]
    fn per_call_classification_binds_visibility_to_the_derived_stream() {
        let tenant = TenantId::new();
        let workspace = WorkspaceId::new();
        let user = UserId::new();
        let policy = policy(tenant, workspace, VisibilityClass::UserPrivate);
        let authorization = authorization(tenant, Some(user), workspace);
        for (class, expect_user, expect_workspace) in [
            (VisibilityClass::UserPrivate, Some(user.0), None),
            (VisibilityClass::WorkspaceShared, None, Some(workspace.0)),
            (VisibilityClass::TenantShared, None, None),
        ] {
            let cmd = command(
                &authorization,
                &policy,
                policy.stream_key(),
                payload(Value::Null),
                PutClassification {
                    visibility_class: class,
                    data_class: DataClass::Sensitive,
                    event_kind: RememberEventKind::GitEvent,
                },
                None,
                OffsetDateTime::now_utc(),
                SubjectDeclaration::default(),
                Vec::new(),
                None,
            )
            .unwrap();
            assert_eq!(cmd.visibility_class, visibility_class_db_str(class));
            assert_eq!(cmd.visibility_user_id, expect_user);
            assert_eq!(cmd.visibility_workspace_id, expect_workspace);
            assert_eq!(cmd.data_class, "SENSITIVE");
            assert_eq!(cmd.event_kind, "GIT_EVENT");
            assert_eq!(cmd.scope_id, workspace.0);
        }
        // A per-request stream for a workspace the scope does not hold is FORBIDDEN even
        // though the policy's own pair is authorized.
        let other = StreamKey::new(
            tenant,
            "workspace",
            WorkspaceId::new().0,
            "reasoning",
            "memory",
            "v1",
        );
        assert!(matches!(
            command(
                &authorization,
                &policy,
                &other,
                payload(Value::Null),
                classify(&policy, RememberEventKind::ManualNote),
                None,
                OffsetDateTime::now_utc(),
                SubjectDeclaration::default(),
                Vec::new(),
                None,
            ),
            Err(ErrorCode::Forbidden)
        ));
        for class in [
            VisibilityClass::UserPrivate,
            VisibilityClass::WorkspaceShared,
            VisibilityClass::TenantShared,
        ] {
            assert_eq!(
                parse_visibility_class(visibility_class_db_str(class)),
                Ok(class)
            );
        }
        assert_eq!(
            parse_visibility_class("tenant_shared"),
            Err(ErrorCode::InvalidInput)
        );
        for kind in RememberEventKind::ALL {
            assert_eq!(RememberEventKind::parse(kind.as_db_str()), Ok(kind));
        }
        assert_eq!(
            RememberEventKind::parse("NOTE"),
            Err(ErrorCode::InvalidInput)
        );
        for class in DataClass::ALL {
            assert_eq!(parse_data_class(class.as_str()), Ok(class));
        }
        assert_eq!(parse_data_class("private"), Err(ErrorCode::InvalidInput));
    }

    #[test]
    fn maps_terminal_remember_errors_without_exposing_details() {
        assert_eq!(
            map_remember_error(remember::RememberError::ConsistencyTokenExpiryNotFuture),
            ErrorCode::InvalidInput
        );
        assert_eq!(
            map_remember_error(remember::RememberError::BatchExhausted),
            ErrorCode::Conflict
        );
    }
}
