//! `adapters::tests::support::governance_ops` — the five confirm-gated governance writes driven through the real
//!   `memory_governance_repo` ops (ADR-0057 D-J), for projection / completeness integration tests.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-projection, postgres, serde_json, sqlx, tokio]; services=[PostgreSQL(owner)
//!   w=[control.api_keys, control.confirm_tokens, control.memberships, control.quota_windows, control.tenants,
//!   control.usage_reservations, control.users, control.workspace_memberships], PostgreSQL(role_gateway)]; env=[]; modules=[adapters::confirm_token_repo,
//!   adapters::memory_governance_repo, adapters::postgres, adapters::quota_repo, adapters::tests::support::token_keys, domain::audit, domain::authority,
//!   domain::confirm, domain::error, domain::evidence, domain::identity, domain::ids, domain::subject,
//!   projection::stream]
//! Called-by: [adapters::tests::a2_point_identity, adapters::tests::projection_lag,
//!   adapters::tests::projection_worker, adapters::tests::rebuild, adapters::tests::support::a2_fixture,
//!   adapters::tests::switch_user_private, maintenance::tests::drill, maintenance::tests::measure]
//! Invariants: [test-only, included by #[path]; the owner connection only seeds the principal the ops run as;
//!   every governance write is a real role_gateway call: mint (first leg) then the op (second leg)]
//! Spec: Baseline §36; §79.2; ADR-0018; ADR-0054; ADR-0057
//!
//! A [`Governor`] is one ACTIVE tenant member with an unbound user API key (its principal) and a
//! BMO quota window; [`Governor::scope`] narrows it to one workspace, as the gateway's write
//! route does (ADR-0054 D-A). Each op mints a confirm token through `mint_with_audit` and then
//! runs the confirmed op, so the ticket a test observes is the one production issues.

#[path = "token_keys.rs"]
pub mod token_keys;
use std::time::{Duration, SystemTime};

use humaux_adapters::confirm_token_repo::{self, ConfirmationClaim};
use humaux_adapters::memory_governance_repo::{
    self, ArchiveRequest, ArchiveResult, CorrectDone, CorrectRequest, RestoreRequest,
    RestoreResult, SupersedeOutcome, SupersedeRequest,
};
use humaux_adapters::postgres::RuntimeDbPool;
use humaux_adapters::quota_repo;
use humaux_domain::audit::{AuditEvent, AuditEventId, AuditMetadata, McpAuditAction};
use humaux_domain::authority::MemoryId;
use humaux_domain::confirm::{DestructiveOp, RISK_TAG_CONFIRMATION_MINTED};
use humaux_domain::error::ErrorCode;
use humaux_domain::evidence::payload_sha256;
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_projection::stream::StreamKey;
use postgres::Client;
use sqlx::types::Uuid;

/// The governance principal of one fixture tenant.
pub struct Governor {
    pub tenant_id: Uuid,
    pub user_id: Uuid,
    pub api_key_id: Uuid,
}

impl Governor {
    /// Activates `tenant_id` and seeds an ACTIVE user with an ACTIVE tenant membership, an
    /// unbound user API key and a BMO quota window. Workspaces are joined with [`Self::join`].
    pub fn seed(admin: &mut Client, tenant_id: Uuid) -> Self {
        token_keys::install();
        admin
            .execute(
                "UPDATE control.tenants SET state = 'ACTIVE' WHERE tenant_id = $1",
                &[&tenant_id],
            )
            .expect("activate the fixture tenant");
        let user_id: Uuid = admin
            .query_one(
                "INSERT INTO control.users (state) VALUES ('ACTIVE') RETURNING user_id",
                &[],
            )
            .expect("seed governor user")
            .get(0);
        admin
            .execute(
                "INSERT INTO control.memberships (tenant_id, user_id, role, state) \
                 VALUES ($1, $2, 'MEMBER', 'ACTIVE')",
                &[&tenant_id, &user_id],
            )
            .expect("seed governor tenant membership");
        let api_key_id = Uuid::new_v4();
        admin
            .execute(
                "INSERT INTO control.api_keys (api_key_id, tenant_id, prefix, key_hash, status, \
                   authorization_version, user_id, workspace_id, tenant_security_epoch, \
                   user_security_epoch) \
                 VALUES ($1, $2, $3, $4, 'ACTIVE', 1, $5, NULL, 0, 0)",
                &[
                    &api_key_id,
                    &tenant_id,
                    &format!("gov_{}", api_key_id.simple()),
                    &api_key_id.as_bytes().to_vec(),
                    &user_id,
                ],
            )
            .expect("seed governor api key");
        admin
            .execute(
                "INSERT INTO control.quota_windows \
                   (tenant_id, entitlement_key, window_start, window_end, hard_limit) \
                 VALUES ($1, $2, clock_timestamp() - interval '1 minute', \
                         clock_timestamp() + interval '1 hour', 1000000)",
                &[&tenant_id, &quota_repo::BMO_ENTITLEMENT],
            )
            .expect("seed BMO quota window");
        Self {
            tenant_id,
            user_id,
            api_key_id,
        }
    }

    /// An ACTIVE workspace membership for the governor in `workspace_id`.
    pub fn join(&self, admin: &mut Client, workspace_id: Uuid) {
        admin
            .execute(
                "INSERT INTO control.workspace_memberships (tenant_id, workspace_id, user_id, role, state) \
                 VALUES ($1, $2, $3, 'MEMBER', 'ACTIVE') ON CONFLICT DO NOTHING",
                &[&self.tenant_id, &workspace_id, &self.user_id],
            )
            .expect("seed governor workspace membership");
    }

    /// The write scope of one request routed to `workspace_id`.
    pub fn scope(&self, workspace_id: Uuid) -> AuthorizationScope {
        AuthorizationScope::new(
            TenantId(self.tenant_id),
            PrincipalId(self.api_key_id),
            Some(UserId(self.user_id)),
            BoundedSet::new([WorkspaceId(workspace_id)]).expect("one workspace"),
        )
    }

    /// Best-effort cleanup of the rows [`Self::seed`] / [`Self::join`] wrote.
    pub fn cleanup(admin: &mut Client, tenant_id: Uuid) {
        let _ = admin.batch_execute(&format!(
            "DELETE FROM control.confirm_tokens WHERE tenant_id = '{tenant_id}'; \
             DELETE FROM control.usage_reservations WHERE tenant_id = '{tenant_id}'; \
             DELETE FROM control.quota_windows WHERE tenant_id = '{tenant_id}'; \
             DELETE FROM control.api_keys WHERE tenant_id = '{tenant_id}'; \
             DELETE FROM control.workspace_memberships WHERE tenant_id = '{tenant_id}'; \
             DELETE FROM control.memberships WHERE tenant_id = '{tenant_id}';"
        ));
    }
}

/// The workspace stream key the fixtures project (`private_memory` / `PRIVATE_MEMORY` / `v1`).
pub fn stream(tenant_id: Uuid, workspace_id: Uuid) -> StreamKey {
    StreamKey::new(
        TenantId(tenant_id),
        "workspace",
        workspace_id,
        "private_memory",
        "PRIVATE_MEMORY",
        "v1",
    )
}

fn audit(
    auth: &AuthorizationScope,
    op: DestructiveOp,
    request_id: Uuid,
    minted: bool,
) -> AuditEvent {
    AuditEvent {
        event_id: AuditEventId::new(),
        ts: SystemTime::now(),
        tenant_id: auth.tenant_id(),
        actor_type: "SERVICE_CREDENTIAL".into(),
        actor_id: auth.principal().0.to_string(),
        action: McpAuditAction::McpRequestFinished.as_str().into(),
        resource_type: "MCP_OPERATION".into(),
        resource_id: op.operation_key().into(),
        result: "OK".into(),
        request_id: request_id.to_string(),
        trace_id: String::new(),
        client_ip: "127.0.0.1".into(),
        user_agent_hash: String::new(),
        risk_tags: if minted {
            vec![RISK_TAG_CONFIRMATION_MINTED.to_owned()]
        } else {
            Vec::new()
        },
        before_fingerprint: None,
        after_fingerprint: None,
        metadata: AuditMetadata::new(),
    }
}

/// First leg: mints the confirm token for `(op, target, successor)` and returns its claim.
fn confirm(
    rt: &tokio::runtime::Runtime,
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    op: DestructiveOp,
    target: Uuid,
    successor: Option<Uuid>,
) -> ConfirmationClaim {
    let nonce_sha256 = *Uuid::new_v4().as_bytes();
    let nonce_sha256: [u8; 32] = [nonce_sha256, *Uuid::new_v4().as_bytes()]
        .concat()
        .try_into()
        .expect("32 bytes");
    rt.block_on(confirm_token_repo::mint_with_audit(
        pool,
        auth,
        op,
        target,
        successor,
        Duration::from_secs(600),
        nonce_sha256,
        &audit(auth, op, Uuid::new_v4(), true),
    ))
    .expect("mint confirm token");
    ConfirmationClaim {
        op,
        target_id: target,
        successor_id: successor,
        nonce_sha256,
    }
}

fn fingerprint() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

/// `memory.supersede` (target → successor) on `stream`.
pub fn supersede(
    rt: &tokio::runtime::Runtime,
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    stream: &StreamKey,
    target: Uuid,
    successor: Uuid,
) -> Result<SupersedeOutcome, ErrorCode> {
    let op = DestructiveOp::MemorySupersede;
    let claim = confirm(rt, pool, auth, op, target, Some(successor));
    let request_id = Uuid::now_v7();
    rt.block_on(memory_governance_repo::supersede_atomically(
        pool,
        auth,
        SupersedeRequest {
            request_id,
            request_fingerprint: fingerprint(),
            reservation_ttl: Duration::from_secs(30),
            target: MemoryId(target),
            successor: MemoryId(successor),
            stream: stream.clone(),
            claim,
            finished_audit: audit(auth, op, request_id, false),
            undo_window: Duration::from_secs(86_400),
        },
    ))
}

/// `memory.restore` of `target` (undoes its newest SUPERSEDE, including a correction's).
pub fn restore(
    rt: &tokio::runtime::Runtime,
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    stream: &StreamKey,
    target: Uuid,
) -> Result<RestoreResult, ErrorCode> {
    let op = DestructiveOp::MemoryRestore;
    let claim = confirm(rt, pool, auth, op, target, None);
    let request_id = Uuid::now_v7();
    rt.block_on(memory_governance_repo::restore_atomically(
        pool,
        auth,
        RestoreRequest {
            request_id,
            request_fingerprint: fingerprint(),
            reservation_ttl: Duration::from_secs(30),
            target: MemoryId(target),
            stream: stream.clone(),
            claim,
            finished_audit: audit(auth, op, request_id, false),
            consistency_token_ttl: Duration::from_secs(600),
        },
    ))
}

/// `memory.archive` (`op = MemoryArchive`) or `memory.unarchive` (`MemoryUnarchive`).
pub fn archive(
    rt: &tokio::runtime::Runtime,
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    stream: &StreamKey,
    target: Uuid,
    op: DestructiveOp,
) -> Result<ArchiveResult, ErrorCode> {
    let claim = confirm(rt, pool, auth, op, target, None);
    let request_id = Uuid::now_v7();
    rt.block_on(memory_governance_repo::archive_or_unarchive_atomically(
        pool,
        auth,
        ArchiveRequest {
            request_id,
            request_fingerprint: fingerprint(),
            reservation_ttl: Duration::from_secs(30),
            target: MemoryId(target),
            stream: stream.clone(),
            claim,
            finished_audit: audit(auth, op, request_id, false),
            op,
        },
    ))
}

/// `memory.correct` of `target` with a new body.
pub fn correct(
    rt: &tokio::runtime::Runtime,
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    stream: &StreamKey,
    target: Uuid,
    body: &str,
) -> Result<CorrectDone, ErrorCode> {
    let op = DestructiveOp::MemoryCorrect;
    let claim = confirm(rt, pool, auth, op, target, None);
    let request_id = Uuid::now_v7();
    // The shape `memory.correct` writes: the user's text verbatim as a JSON string (gateway
    // `memory_correct`), so every correct test projects what production stores.
    let content = serde_json::Value::String(body.to_owned());
    let bytes = serde_json::to_vec(&content).expect("json bytes");
    rt.block_on(memory_governance_repo::correct_atomically(
        pool,
        auth,
        CorrectRequest {
            request_id,
            request_fingerprint: fingerprint(),
            reservation_ttl: Duration::from_secs(30),
            target: MemoryId(target),
            content,
            payload_sha256: payload_sha256(&bytes),
            stream: stream.clone(),
            claim,
            finished_audit: audit(auth, op, request_id, false),
            undo_window: Duration::from_secs(86_400),
            consistency_token_ttl: Duration::from_secs(600),
            subjects: humaux_domain::subject::SubjectDeclaration::default(),
            affects: Vec::new(),
            mood_half_life: None,
        },
    ))
}
