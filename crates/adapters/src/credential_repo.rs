//! `adapters::credential_repo` — Gateway-only read adapter for §73.5.1 service credential bindings.
//! Depends-on: crates=[humaux-domain, sqlx]; services=[PostgreSQL(any) r=[control.workspace_memberships] x=[control.api_key_lookup, control.api_key_rehash, control.api_key_touch_last_used]]; env=[]; modules=[adapters::postgres, domain::error, domain::identity]
//! Called-by: [gateway::auth, tests]
//! Invariants: [returns database facts only (HMAC and authorization stay in the auth layer); the workspace-ceiling
//!   read runs only after a user-bound key validates, so a bad key does no membership work; a PG error is
//!   DependencyUnavailable; a verifier is rewritten only through the epoch-gated, window-bounded api_key_rehash door (ADR-0059 D-H, migration 0204)]
//! Spec: Baseline §6.1.1, §73.5; ADR-0035; ADR-0059
//!
//! This module returns database facts only; HMAC and authorization decisions remain in
//! the protocol/authentication layer.
//!
//! [`lookup`] is the single cheap credential snapshot (no membership work). The per-request
//! §6.1.1 workspace ceiling is a SEPARATE read, [`load_live_workspace_ids`], the authentication
//! layer runs only after `validate_api_key` succeeds for a user-bound credential (ADR-0035, card
//! 13) — so a machine credential or a bad key does no membership DB work.

use humaux_domain::error::ErrorCode;
use humaux_domain::identity::MembershipState;
use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use crate::postgres::RuntimeDbPool;

/// §6.1.1 ceiling: no request may carry more workspaces than this. Queried `LIMIT 257` so a
/// membership set of exactly the 256-cap ([`humaux_domain::identity::BoundedSet::MAX_LEN`]) is
/// returned whole while one row beyond it is observed (not silently truncated to 256) and turned
/// into `INVALID_INPUT` by the caller. ADR-0035, card 13.
const LIVE_WORKSPACE_QUERY_LIMIT: i64 = 257;

/// One row from the sole gateway-only api_key_lookup(text) entry point.
///
/// Fields are intentionally private: callers may inspect the authenticated database facts,
/// but cannot construct a credential record or serialize verifier material accidentally.
pub struct CredentialRecord {
    api_key_id: Uuid,
    tenant_id: Uuid,
    key_hash: Vec<u8>,
    status: String,
    allowed_cidrs: Vec<String>,
    expires_at: Option<OffsetDateTime>,
    revoked_at: Option<OffsetDateTime>,
    scopes: Vec<String>,
    authorization_version: Option<i16>,
    user_id: Option<Uuid>,
    workspace_id: Option<Uuid>,
    tenant_security_epoch: Option<i64>,
    user_security_epoch: Option<i64>,
    tenant_state: String,
    live_tenant_security_epoch: i64,
    user_state: Option<String>,
    live_user_security_epoch: Option<i64>,
    membership_state: Option<String>,
}

impl CredentialRecord {
    /// Immutable machine-principal identifier for this credential.
    pub fn api_key_id(&self) -> Uuid {
        self.api_key_id
    }

    /// Tenant bound to this credential row.
    pub fn tenant_id(&self) -> Uuid {
        self.tenant_id
    }

    /// Stored HMAC verifier bytes; callers must not log them.
    pub fn key_hash(&self) -> &[u8] {
        &self.key_hash
    }

    /// Stored lifecycle state, evaluated by the authentication layer.
    pub fn status(&self) -> &str {
        &self.status
    }

    /// CIDR allowlist rendered as text by the adapter query.
    pub fn allowed_cidrs(&self) -> &[String] {
        &self.allowed_cidrs
    }

    /// Optional credential expiry time.
    pub fn expires_at(&self) -> Option<OffsetDateTime> {
        self.expires_at
    }

    /// Optional credential revocation time.
    pub fn revoked_at(&self) -> Option<OffsetDateTime> {
        self.revoked_at
    }

    /// Scopes recorded on the credential row.
    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }

    /// Explicit authorization schema version, absent for inert legacy rows.
    pub fn authorization_version(&self) -> Option<i16> {
        self.authorization_version
    }

    /// Optional PAT on-behalf-of user binding.
    pub fn user_id(&self) -> Option<Uuid> {
        self.user_id
    }

    /// Optional workspace binding that narrows this credential.
    pub fn workspace_id(&self) -> Option<Uuid> {
        self.workspace_id
    }

    /// Tenant security epoch captured when this credential was authorized.
    pub fn tenant_security_epoch(&self) -> Option<i64> {
        self.tenant_security_epoch
    }

    /// User security epoch captured for a PAT, absent for a machine credential.
    pub fn user_security_epoch(&self) -> Option<i64> {
        self.user_security_epoch
    }

    /// Live tenant lifecycle state from the same lookup statement.
    pub fn tenant_state(&self) -> &str {
        &self.tenant_state
    }

    /// Live tenant security epoch from the same lookup statement.
    pub fn live_tenant_security_epoch(&self) -> i64 {
        self.live_tenant_security_epoch
    }

    /// Live bound-user lifecycle state, if this is a PAT.
    pub fn user_state(&self) -> Option<&str> {
        self.user_state.as_deref()
    }

    /// Live bound-user security epoch, if this is a PAT.
    pub fn live_user_security_epoch(&self) -> Option<i64> {
        self.live_user_security_epoch
    }

    /// Live tenant membership state for the bound PAT user.
    pub fn membership_state(&self) -> Option<&str> {
        self.membership_state.as_deref()
    }
}

fn db_error(error: sqlx::Error) -> ErrorCode {
    match error {
        sqlx::Error::Database(database) if database.code().as_deref() == Some("42501") => {
            ErrorCode::Forbidden
        }
        _ => ErrorCode::DependencyUnavailable,
    }
}

/// Reads one credential binding through the only gateway-authorized lookup.
///
/// The returned record may be legacy, revoked, expired, or otherwise unauthorized. The caller
/// must verify the HMAC and decide authorization from every returned live fact.
pub async fn lookup(
    pool: &RuntimeDbPool,
    prefix: &str,
) -> Result<Option<CredentialRecord>, ErrorCode> {
    let row = sqlx::query(
        r#"SELECT api_key_id, tenant_id, key_hash, status,
                  allowed_cidrs::text[] AS allowed_cidrs,
                  expires_at, revoked_at, scopes, authorization_version, user_id, workspace_id,
                  tenant_security_epoch, user_security_epoch, tenant_state,
                  live_tenant_security_epoch, user_state, live_user_security_epoch, membership_state
           FROM control.api_key_lookup($1)"#,
    )
    .bind(prefix)
    // dep: PostgreSQL(any) — executes a query against the pool
    .fetch_optional(pool.pool())
    .await
    .map_err(db_error)?;

    row.map(|row| {
        Ok(CredentialRecord {
            api_key_id: row.try_get("api_key_id").map_err(db_error)?,
            tenant_id: row.try_get("tenant_id").map_err(db_error)?,
            key_hash: row.try_get("key_hash").map_err(db_error)?,
            status: row.try_get("status").map_err(db_error)?,
            allowed_cidrs: row.try_get("allowed_cidrs").map_err(db_error)?,
            expires_at: row.try_get("expires_at").map_err(db_error)?,
            revoked_at: row.try_get("revoked_at").map_err(db_error)?,
            scopes: row.try_get("scopes").map_err(db_error)?,
            authorization_version: row.try_get("authorization_version").map_err(db_error)?,
            user_id: row.try_get("user_id").map_err(db_error)?,
            workspace_id: row.try_get("workspace_id").map_err(db_error)?,
            tenant_security_epoch: row.try_get("tenant_security_epoch").map_err(db_error)?,
            user_security_epoch: row.try_get("user_security_epoch").map_err(db_error)?,
            tenant_state: row.try_get("tenant_state").map_err(db_error)?,
            live_tenant_security_epoch: row
                .try_get("live_tenant_security_epoch")
                .map_err(db_error)?,
            user_state: row.try_get("user_state").map_err(db_error)?,
            live_user_security_epoch: row.try_get("live_user_security_epoch").map_err(db_error)?,
            membership_state: row.try_get("membership_state").map_err(db_error)?,
        })
    })
    .transpose()
}

/// The live workspace ceiling for one (tenant, user): every workspace the user holds an ACTIVE
/// [`MembershipState`] `control.workspace_memberships` row in (ADR-0035 / §6.1.1). Read per request
/// in the authentication layer (`bins/gateway/src/auth.rs`) AFTER `validate_api_key` succeeds and
/// only when the credential carries a `user_id` — a machine credential or a bad key never reaches
/// here, so this adds no pre-auth DB work.
///
/// `control.workspace_memberships` is FORCE RLS with a self-read policy (tenant GUC AND user GUC
/// match the row), so both GUCs are set transaction-locally first; without them the gateway role
/// reads nothing, which is the fail-closed direction, and the `SET LOCAL` dies with the READ
/// COMMITTED transaction (never leaking onto another pooled request). More than
/// [`humaux_domain::identity::BoundedSet::MAX_LEN`] memberships is `INVALID_INPUT`, not a silent
/// truncation (see [`LIVE_WORKSPACE_QUERY_LIMIT`]).
pub async fn load_live_workspace_ids(
    pool: &RuntimeDbPool,
    tenant_id: Uuid,
    user_id: Uuid,
) -> Result<Vec<Uuid>, ErrorCode> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    sqlx::query(
        "SELECT set_config('humaux.tenant_id', $1, true), set_config('humaux.user_id', $2, true)",
    )
    .bind(tenant_id.to_string())
    .bind(user_id.to_string())
    .execute(&mut *txn)
    .await
    .map_err(db_error)?;
    let rows = sqlx::query_scalar::<_, Uuid>(
        "SELECT workspace_id FROM control.workspace_memberships \
         WHERE tenant_id = $1 AND user_id = $2 AND state = $3 \
         ORDER BY workspace_id LIMIT $4",
    )
    .bind(tenant_id)
    .bind(user_id)
    .bind(MembershipState::Active.as_db_str())
    .bind(LIVE_WORKSPACE_QUERY_LIMIT)
    .fetch_all(&mut *txn)
    .await
    .map_err(db_error)?;
    txn.commit().await.map_err(db_error)?;
    if rows.len() as i64 >= LIVE_WORKSPACE_QUERY_LIMIT {
        // The 257th row proves the set exceeds the 256 cap: refuse rather than truncate.
        return Err(ErrorCode::InvalidInput);
    }
    Ok(rows)
}

/// Records successful credential use through the existing gateway-only touch function.
///
/// Authorization must already have succeeded; this only updates last_used_at.
pub async fn mark_used(pool: &RuntimeDbPool, api_key_id: Uuid) -> Result<(), ErrorCode> {
    sqlx::query("SELECT control.api_key_touch_last_used($1)")
        .bind(api_key_id)
        // dep: PostgreSQL(any) — executes a query against the pool
        .execute(pool.pool())
        .await
        .map_err(db_error)?;
    Ok(())
}

/// §73.5 / ADR-0059 D-H: rewrites a key's verifier from `old_hash` (previous pepper) to `new_hash`
/// (current pepper) through the epoch-gated, audited `control.api_key_rehash` door.
///
/// `Some(true)` = rewritten; `Some(false)` = nothing rewritten (compare-and-set lost, key revoked,
/// or no epoch open). `None` is decoded rather than assumed away even though the function never
/// returns NULL, so the caller treats every non-`true` outcome alike: never fatal to a request
/// that already validated.
pub async fn rehash(
    pool: &RuntimeDbPool,
    api_key_id: Uuid,
    old_hash: &[u8],
    new_hash: &[u8],
) -> Result<Option<bool>, ErrorCode> {
    sqlx::query_scalar("SELECT control.api_key_rehash($1, $2, $3)")
        .bind(api_key_id)
        .bind(old_hash)
        .bind(new_hash)
        // dep: PostgreSQL(any) — control.api_key_rehash owner definer (migration 0202)
        .fetch_one(pool.pool())
        .await
        .map_err(db_error)
}
