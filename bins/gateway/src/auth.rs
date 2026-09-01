//! §73.5.1 headless authentication. Credentials and live grants come from the
//! database; tool arguments never supply a principal, user, or workspace grant.

use std::collections::BTreeSet;
use std::net::IpAddr;
use std::time::SystemTime;

use humaux_adapters::credential_repo::{self, CredentialRecord};
use humaux_adapters::postgres::RuntimeDbPool;
use humaux_domain::error::ErrorCode;
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_protocol::edge::{ApiKeyRecord, ApiKeyStatus, validate_api_key};

/// The closed service-credential scope vocabulary from §33.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CredentialScope {
    /// Read context and recall visible memories.
    ContextRead,
    /// Write memories and evidence through the guarded application path.
    MemoryWrite,
    /// Manage artifacts.
    ArtifactManage,
    /// Manage code knowledge.
    CodeManage,
    /// Manage agent coordination.
    CoordinationManage,
}

impl CredentialScope {
    /// Parses an explicit stored grant; unknown values never imply a permission.
    pub fn parse(value: &str) -> Result<Self, ErrorCode> {
        match value {
            "context:read" => Ok(Self::ContextRead),
            "memory:write" => Ok(Self::MemoryWrite),
            "artifact:manage" => Ok(Self::ArtifactManage),
            "code:manage" => Ok(Self::CodeManage),
            "coordination:manage" => Ok(Self::CoordinationManage),
            _ => Err(ErrorCode::Unauthorized),
        }
    }
}

/// An authenticated request, not a reusable session or a deserializable tool input.
///
/// This contains no credential secret or verifier. Call the authenticator for each
/// request so revocations, account state, membership, and epochs are re-read.
pub struct AuthenticatedServiceCredential {
    authorization: AuthorizationScope,
    scopes: BTreeSet<CredentialScope>,
    workspace_id: Option<WorkspaceId>,
}

impl AuthenticatedServiceCredential {
    /// RequestGuard may inspect the current authenticated identity for non-business
    /// protocol traffic. Business operations must still call `authorize` for a scope.
    pub(crate) fn identity(&self) -> &AuthorizationScope {
        &self.authorization
    }

    /// The server-stored resource-routing bound. A missing tool workspace defaults
    /// to this value; routing must not widen a workspace-bound key to its tenant.
    pub fn bound_workspace_id(&self) -> Option<WorkspaceId> {
        self.workspace_id
    }

    /// Authorizes one operation and only narrows the database-derived scope.
    ///
    /// The RequestGuard must also bind its resource route to
    /// [`Self::bound_workspace_id`]; a visibility predicate alone is not a route.
    pub fn authorize(
        &self,
        required_scope: CredentialScope,
        requested_workspace: Option<WorkspaceId>,
    ) -> Result<AuthorizationScope, ErrorCode> {
        if !self.scopes.contains(&required_scope) {
            return Err(ErrorCode::Forbidden);
        }
        match requested_workspace.or(self.workspace_id) {
            Some(workspace) => self.authorization.narrow(workspace),
            None => Ok(self.authorization.clone()),
        }
    }
}

/// Authenticates an HTTP Bearer service credential against current database facts.
///
/// `request_ip` must be the trusted edge-derived client address (§73.1), not a
/// forwarded header or an MCP argument. `pepper` is supplied by bootstrap, never
/// loaded from a request. Errors intentionally contain no credential material.
pub async fn authenticate_service_credential(
    pool: &RuntimeDbPool,
    authorization_header: &str,
    pepper: &[u8],
    request_ip: IpAddr,
    now: SystemTime,
) -> Result<AuthenticatedServiceCredential, ErrorCode> {
    if pepper.is_empty() {
        return Err(ErrorCode::Internal);
    }
    let (prefix, raw_key) = parse_bearer_credential(authorization_header)?;
    let record = credential_repo::lookup(pool, prefix)
        .await?
        .ok_or(ErrorCode::Unauthorized)?;
    let key = ApiKeyRecord {
        key_hash: record.key_hash().to_vec(),
        status: ApiKeyStatus::from_db_str(record.status()).ok_or(ErrorCode::Unauthorized)?,
        allowed_cidrs: record
            .allowed_cidrs()
            .iter()
            .map(|cidr| cidr.parse().map_err(|_| ErrorCode::Unauthorized))
            .collect::<Result<_, _>>()?,
        expires_at: record.expires_at().map(SystemTime::from),
        revoked_at: record.revoked_at().map(SystemTime::from),
    };
    validate_api_key(&key, raw_key, pepper, request_ip, now)
        .map_err(|_| ErrorCode::Unauthorized)?;
    let authenticated = authenticated_binding(&record)?;
    // Reuse the gateway-only definer; rejected credentials never get a usage write.
    credential_repo::mark_used(pool, record.api_key_id()).await?;
    Ok(authenticated)
}

fn authenticated_binding(
    record: &CredentialRecord,
) -> Result<AuthenticatedServiceCredential, ErrorCode> {
    if record.authorization_version() != Some(1)
        || record.api_key_id().is_nil()
        || record.tenant_id().is_nil()
        || record.tenant_state() != "ACTIVE"
        || record.live_tenant_security_epoch() < 0
        || record.tenant_security_epoch() != Some(record.live_tenant_security_epoch())
        || record.workspace_id().is_some_and(|id| id.is_nil())
    {
        return Err(ErrorCode::Unauthorized);
    }
    if let Some(user_id) = record.user_id() {
        if user_id.is_nil()
            || record.user_state() != Some("ACTIVE")
            || record.membership_state() != Some("ACTIVE")
            || record
                .live_user_security_epoch()
                .is_none_or(|epoch| epoch < 0)
            || record.user_security_epoch() != record.live_user_security_epoch()
        {
            return Err(ErrorCode::Unauthorized);
        }
    } else if record.user_security_epoch().is_some() {
        return Err(ErrorCode::Unauthorized);
    }
    let scopes = record
        .scopes()
        .iter()
        .map(|scope| CredentialScope::parse(scope))
        .collect::<Result<_, _>>()?;
    let workspace_id = record.workspace_id().map(WorkspaceId);
    let authorization = AuthorizationScope::new(
        TenantId(record.tenant_id()),
        PrincipalId(record.api_key_id()),
        record.user_id().map(UserId),
        BoundedSet::new(workspace_id)?,
    );
    Ok(AuthenticatedServiceCredential {
        authorization,
        scopes,
        workspace_id,
    })
}

fn parse_bearer_credential(header: &str) -> Result<(&str, &str), ErrorCode> {
    let (scheme, raw_key) = header.split_once(' ').ok_or(ErrorCode::Unauthorized)?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return Err(ErrorCode::Unauthorized);
    }
    let (prefix, secret) = raw_key.split_once('.').ok_or(ErrorCode::Unauthorized)?;
    let valid_part = |part: &str, min, max| {
        (min..=max).contains(&part.len())
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    };
    if !valid_part(prefix, 1, 64) || !valid_part(secret, 32, 256) {
        return Err(ErrorCode::Unauthorized);
    }
    Ok((prefix, raw_key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_wire_is_bounded_and_never_accepts_extra_fields() {
        let secret = "a".repeat(32);
        let raw_key = format!("prefix_1.{secret}");
        let header = format!("bEaReR {raw_key}");
        assert_eq!(
            parse_bearer_credential(&header),
            Ok(("prefix_1", raw_key.as_str()))
        );
        for invalid in [
            raw_key.clone(),
            format!("Basic {raw_key}"),
            format!("Bearer  {raw_key}"),
            format!("Bearer {raw_key} "),
            format!("Bearer {raw_key}.extra"),
            format!("Bearer .{secret}"),
            format!("Bearer {}.{secret}", "p".repeat(65)),
            format!("Bearer prefix.{}", "a".repeat(31)),
            format!("Bearer prefix.{}", "a".repeat(257)),
            format!("Bearer prefix.{secret}\r\n"),
            format!("Bearer 前缀.{secret}"),
        ] {
            assert_eq!(
                parse_bearer_credential(&invalid),
                Err(ErrorCode::Unauthorized)
            );
        }
    }

    #[test]
    fn scopes_and_workspace_arguments_can_only_narrow() {
        let workspace = WorkspaceId::new();
        let other = WorkspaceId::new();
        let credential = AuthenticatedServiceCredential {
            authorization: AuthorizationScope::new(
                TenantId::new(),
                PrincipalId::new(),
                None,
                BoundedSet::new([workspace]).unwrap(),
            ),
            scopes: BTreeSet::from([CredentialScope::ContextRead]),
            workspace_id: Some(workspace),
        };
        assert_eq!(credential.bound_workspace_id(), Some(workspace));
        assert!(
            credential
                .authorize(CredentialScope::ContextRead, None)
                .is_ok()
        );
        assert!(
            credential
                .authorize(CredentialScope::ContextRead, Some(workspace))
                .is_ok()
        );
        assert_eq!(
            credential.authorize(CredentialScope::ContextRead, Some(other)),
            Err(ErrorCode::Forbidden)
        );
        assert_eq!(
            credential.authorize(CredentialScope::MemoryWrite, None),
            Err(ErrorCode::Forbidden)
        );
        let unbound = AuthenticatedServiceCredential {
            authorization: AuthorizationScope::new(
                TenantId::new(),
                PrincipalId::new(),
                None,
                BoundedSet::new([]).unwrap(),
            ),
            scopes: BTreeSet::from([CredentialScope::ContextRead]),
            workspace_id: None,
        };
        assert!(
            unbound
                .authorize(CredentialScope::ContextRead, None)
                .unwrap()
                .allowed_workspace_ids()
                .is_empty()
        );
        assert_eq!(
            unbound.authorize(CredentialScope::ContextRead, Some(workspace)),
            Err(ErrorCode::Forbidden)
        );
        for unknown in [
            "*",
            "context:*",
            "memory:read",
            "CONTEXT:READ",
            "context:read ",
            "",
        ] {
            assert_eq!(
                CredentialScope::parse(unknown),
                Err(ErrorCode::Unauthorized)
            );
        }
    }
}
