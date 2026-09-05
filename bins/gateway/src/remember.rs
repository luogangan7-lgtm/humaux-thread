//! Guarded gateway entry point for one authenticated `remember` operation.
//!
//! The protocol layer decodes wire input and the request guard produces the
//! [`AuthorizationScope`]. This module does not deserialize either of them.

use std::time::Duration;

use humaux_adapters::postgres::RuntimeDbPool;
use humaux_adapters::remember::{self, RememberAccepted, RememberCommand};
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

/// Bootstrap-owned immutable configuration for one gateway write route.
///
/// The stream identity, reasoning domain, classification, and token lifetime are
/// never supplied by a tool argument. Only workspace-scoped stream families are
/// enabled here; another scope requires a separately reviewed operation.
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
        content,
        event_kind,
        occurred_at,
        now,
        SubjectDeclaration::default(),
    )?;
    remember::remember(pool, cmd)
        .await
        .map_err(map_remember_error)
}

pub(crate) fn command(
    authorization: &AuthorizationScope,
    policy: &RememberPolicy,
    content: PreparedEvidencePayload,
    event_kind: RememberEventKind,
    occurred_at: Option<OffsetDateTime>,
    now: OffsetDateTime,
    subjects: SubjectDeclaration,
) -> Result<RememberCommand, ErrorCode> {
    if authorization.user_id().is_none()
        || policy.stream.tenant_id != authorization.tenant_id()
        || !authorization
            .allowed_workspace_ids()
            .contains(&WorkspaceId(policy.stream.scope_id))
    {
        return Err(ErrorCode::Forbidden);
    }
    let consistency_token_expires_at = now
        .checked_add(policy.consistency_token_ttl)
        .ok_or(ErrorCode::InvalidInput)?;
    let (visibility_class, visibility_user_id, visibility_workspace_id) =
        match policy.visibility_class {
            VisibilityClass::UserPrivate => (
                "USER_PRIVATE".to_owned(),
                authorization.user_id().map(|user| user.0),
                None,
            ),
            VisibilityClass::WorkspaceShared => (
                "WORKSPACE_SHARED".to_owned(),
                None,
                Some(policy.stream.scope_id),
            ),
            VisibilityClass::TenantShared => return Err(ErrorCode::InvalidInput),
        };

    Ok(RememberCommand {
        tenant_id: authorization.tenant_id().0,
        authorization_user_id: authorization.user_id().map(|user| user.0),
        scope_kind: policy.stream.scope_kind.clone(),
        scope_id: policy.stream.scope_id,
        domain: policy.stream.domain.clone(),
        projection_kind: policy.stream.projection_kind.clone(),
        projection_version: policy.stream.projection_version.clone(),
        consistency_token_expires_at,
        batch_id: None,
        payload_sha256: payload_sha256(content.raw_json()),
        data_class: policy.data_class.as_str().to_owned(),
        origin_class: EvidenceOriginClass::AuthenticatedAgent,
        origin_principal_id: Some(authorization.principal().0),
        origin_connector_id: None,
        visibility_class,
        visibility_user_id,
        visibility_workspace_id,
        reasoning_domain_id: policy.reasoning_domain_id,
        occurred_at,
        event_kind: event_kind.as_db_str().to_owned(),
        event_payload: content.value,
        subjects,
    })
}

fn map_remember_error(error: remember::RememberError) -> ErrorCode {
    match error {
        remember::RememberError::ConsistencyTokenExpiryNotFuture => ErrorCode::InvalidInput,
        remember::RememberError::BatchExhausted => ErrorCode::Conflict,
        remember::RememberError::Subject(code) => code,
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

    #[test]
    fn rejects_userless_authorization() {
        let tenant = TenantId::new();
        let workspace = WorkspaceId::new();
        assert!(matches!(
            command(
                &authorization(tenant, None, workspace),
                &policy(tenant, workspace, VisibilityClass::UserPrivate),
                payload(Value::Null),
                RememberEventKind::ManualNote,
                None,
                OffsetDateTime::now_utc(),
                SubjectDeclaration::default(),
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
                &policy(tenant, WorkspaceId::new(), VisibilityClass::UserPrivate,),
                payload(Value::Null),
                RememberEventKind::ManualNote,
                None,
                OffsetDateTime::now_utc(),
                SubjectDeclaration::default(),
            ),
            Err(ErrorCode::Forbidden)
        ));
    }

    #[test]
    fn agent_origin_and_private_visibility_come_from_authorization() {
        let tenant = TenantId::new();
        let workspace = WorkspaceId::new();
        let user = UserId::new();
        let cmd = command(
            &authorization(tenant, Some(user), workspace),
            &policy(tenant, workspace, VisibilityClass::UserPrivate),
            payload(Value::Null),
            RememberEventKind::UserMessage,
            None,
            OffsetDateTime::now_utc(),
            SubjectDeclaration::default(),
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
        let cmd = command(
            &authorization(tenant, Some(UserId::new()), workspace),
            &policy(tenant, workspace, VisibilityClass::UserPrivate),
            payload,
            RememberEventKind::UserMessage,
            None,
            OffsetDateTime::now_utc(),
            SubjectDeclaration::default(),
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
