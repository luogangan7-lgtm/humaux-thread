//! PostgreSQL-only identity binding for private Qdrant candidates.
//!
//! Qdrant returns an opaque point id plus a score.  This module is the sole private-plane
//! point-id resolver: it binds that id to a Memory through PostgreSQL and then rechecks the
//! mutable Memory source fence.  It deliberately performs no Qdrant I/O; remote projection
//! work must happen after a committed PG registration transaction.

use std::collections::{HashMap, HashSet};

use humaux_domain::authority::MemoryId;
use humaux_domain::identity::AuthorizationScope;
use humaux_domain::ids::WorkspaceId;
use humaux_projection::serving::StreamFamily;
use sha2::{Digest, Sha256};
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;
use sqlx::{Row, Transaction};

use crate::postgres::{RetrievalWorkerDbPool, RuntimeDbPool};

type Txn<'c> = Transaction<'c, sqlx::Postgres>;

/// Opaque Qdrant UUID point id.  It intentionally has no conversion to [`MemoryId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProjectionPointId(Uuid);

impl ProjectionPointId {
    pub const fn new(value: Uuid) -> Self {
        Self(value)
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// The PG-fenced source identity a private Qdrant point represents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateMemoryPointRegistration {
    pub point_id: ProjectionPointId,
    pub family: StreamFamily,
    pub projection_version: String,
    pub embedding_version: String,
    pub memory_id: MemoryId,
    /// Mutable source fence, deliberately not called a revision.
    pub source_updated_at: OffsetDateTime,
    pub body_sha256: Vec<u8>,
}

impl PrivateMemoryPointRegistration {
    /// Builds the replay-stable opaque UUID used for one exact source/projection identity.
    /// Length-prefixing every variable field prevents tuple-boundary ambiguity; UUIDv8 bits
    /// label this as an application-defined digest, not a reversible Memory id.
    pub fn deterministic(
        family: StreamFamily,
        projection_version: String,
        embedding_version: String,
        memory_id: MemoryId,
        source_updated_at: OffsetDateTime,
        body_sha256: Vec<u8>,
    ) -> Self {
        fn field(hasher: &mut Sha256, bytes: &[u8]) {
            hasher.update((bytes.len() as u64).to_be_bytes());
            hasher.update(bytes);
        }

        let mut hasher = Sha256::new();
        field(&mut hasher, b"humaux.private-memory-point.v1");
        field(&mut hasher, family.tenant_id.0.as_bytes());
        field(&mut hasher, family.scope_kind.as_bytes());
        field(&mut hasher, family.scope_id.as_bytes());
        field(&mut hasher, family.domain.as_bytes());
        field(&mut hasher, family.projection_kind.as_bytes());
        field(&mut hasher, projection_version.as_bytes());
        field(&mut hasher, embedding_version.as_bytes());
        field(&mut hasher, memory_id.0.as_bytes());
        field(
            &mut hasher,
            &source_updated_at.unix_timestamp_nanos().to_be_bytes(),
        );
        field(&mut hasher, &body_sha256);
        let digest = hasher.finalize();
        let mut point_bytes = [0_u8; 16];
        point_bytes.copy_from_slice(&digest[..16]);
        point_bytes[6] = (point_bytes[6] & 0x0f) | 0x80;
        point_bytes[8] = (point_bytes[8] & 0x3f) | 0x80;
        Self {
            point_id: ProjectionPointId::new(Uuid::from_bytes(point_bytes)),
            family,
            projection_version,
            embedding_version,
            memory_id,
            source_updated_at,
            body_sha256,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationOutcome {
    Inserted,
    AlreadyRegistered,
}

/// A candidate whose Qdrant point identity and current PG source both passed validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedPrivateMemoryPoint {
    pub point_id: ProjectionPointId,
    pub memory_id: MemoryId,
}

#[derive(Debug)]
pub enum PrivateProjectionRegistryError {
    Db(sqlx::Error),
    InvalidInput,
    MissingAuthenticatedUser,
    CrossTenant,
    CrossWorkspace,
    SourceNotLive,
    SourceChanged,
    PointIdCollision,
    IdentityAlreadyBound,
    RegistryRaceLost,
}

impl From<sqlx::Error> for PrivateProjectionRegistryError {
    fn from(value: sqlx::Error) -> Self {
        Self::Db(value)
    }
}

impl std::fmt::Display for PrivateProjectionRegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(error) => write!(f, "private projection registry database error: {error}"),
            Self::InvalidInput => write!(f, "invalid private projection registry input"),
            Self::MissingAuthenticatedUser => write!(
                f,
                "private projection registry requires an authenticated user"
            ),
            Self::CrossTenant => write!(f, "private projection registry cross-tenant request"),
            Self::CrossWorkspace => {
                write!(f, "private projection registry cross-workspace request")
            }
            Self::SourceNotLive => write!(f, "private projection source is absent or not live"),
            Self::SourceChanged => {
                write!(f, "private projection source changed before registration")
            }
            Self::PointIdCollision => write!(f, "private projection point id collision"),
            Self::IdentityAlreadyBound => write!(
                f,
                "private projection source identity already has another point id"
            ),
            Self::RegistryRaceLost => write!(
                f,
                "private projection registry insert lost without a readable binding"
            ),
        }
    }
}

impl std::error::Error for PrivateProjectionRegistryError {}

fn valid_text(value: &str, max_len: usize) -> bool {
    !value.is_empty() && value.len() <= max_len
}

fn validate_input(
    authorization: &AuthorizationScope,
    registration: &PrivateMemoryPointRegistration,
) -> Result<(), PrivateProjectionRegistryError> {
    validate_family(
        authorization,
        &registration.family,
        &registration.projection_version,
        &registration.embedding_version,
    )?;
    if registration.body_sha256.len() != 32 {
        return Err(PrivateProjectionRegistryError::InvalidInput);
    }
    Ok(())
}

fn validate_family(
    authorization: &AuthorizationScope,
    family: &StreamFamily,
    projection_version: &str,
    embedding_version: &str,
) -> Result<(), PrivateProjectionRegistryError> {
    if authorization.user_id().is_none() {
        return Err(PrivateProjectionRegistryError::MissingAuthenticatedUser);
    }
    if family.tenant_id != authorization.tenant_id() {
        return Err(PrivateProjectionRegistryError::CrossTenant);
    }
    match family.scope_kind.as_str() {
        "tenant" if family.scope_id == family.tenant_id.0 => {}
        "tenant" => return Err(PrivateProjectionRegistryError::CrossTenant),
        "workspace"
            if authorization
                .allowed_workspace_ids()
                .contains(&WorkspaceId(family.scope_id)) => {}
        "workspace" => return Err(PrivateProjectionRegistryError::CrossWorkspace),
        _ => return Err(PrivateProjectionRegistryError::InvalidInput),
    }
    if !valid_text(&family.scope_kind, 64)
        || !valid_text(&family.domain, 128)
        || !valid_text(&family.projection_kind, 128)
        || !valid_text(projection_version, 128)
        || !valid_text(embedding_version, 128)
    {
        return Err(PrivateProjectionRegistryError::InvalidInput);
    }
    Ok(())
}

async fn set_authorization_local(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
) -> Result<(), PrivateProjectionRegistryError> {
    let Some(user_id) = authorization.user_id() else {
        return Err(PrivateProjectionRegistryError::MissingAuthenticatedUser);
    };
    sqlx::query(
        "SELECT set_config('humaux.tenant_id', $1, true), set_config('humaux.user_id', $2, true)",
    )
    .bind(authorization.tenant_id().0.to_string())
    .bind(user_id.0.to_string())
    .execute(&mut **txn)
    .await?;
    Ok(())
}

async fn current_source_matches(
    txn: &mut Txn<'_>,
    registration: &PrivateMemoryPointRegistration,
) -> Result<(), PrivateProjectionRegistryError> {
    let row = sqlx::query(
        "SELECT updated_at, sha256(convert_to(content::text,'UTF8')) AS body_sha256, \
                status, superseded_by \
         FROM private.memory_records \
         WHERE tenant_id = $1 AND memory_id = $2",
    )
    .bind(registration.family.tenant_id.0)
    .bind(registration.memory_id.0)
    .fetch_optional(&mut **txn)
    .await?;
    let Some(row) = row else {
        return Err(PrivateProjectionRegistryError::SourceNotLive);
    };
    let status: String = row.try_get("status")?;
    let superseded_by: Option<Uuid> = row.try_get("superseded_by")?;
    if status != "active" || superseded_by.is_some() {
        return Err(PrivateProjectionRegistryError::SourceNotLive);
    }
    let updated_at: OffsetDateTime = row.try_get("updated_at")?;
    let body_sha256: Vec<u8> = row.try_get("body_sha256")?;
    if updated_at != registration.source_updated_at || body_sha256 != registration.body_sha256 {
        return Err(PrivateProjectionRegistryError::SourceChanged);
    }
    Ok(())
}

fn binding_matches(
    row: &sqlx::postgres::PgRow,
    registration: &PrivateMemoryPointRegistration,
) -> Result<bool, PrivateProjectionRegistryError> {
    Ok(
        row.try_get::<Uuid, _>("point_id")? == registration.point_id.0
            && row.try_get::<Uuid, _>("tenant_id")? == registration.family.tenant_id.0
            && row.try_get::<String, _>("scope_kind")? == registration.family.scope_kind
            && row.try_get::<Uuid, _>("scope_id")? == registration.family.scope_id
            && row.try_get::<String, _>("domain")? == registration.family.domain
            && row.try_get::<String, _>("projection_kind")? == registration.family.projection_kind
            && row.try_get::<String, _>("projection_version")? == registration.projection_version
            && row.try_get::<String, _>("embedding_version")? == registration.embedding_version
            && row.try_get::<Uuid, _>("memory_id")? == registration.memory_id.0
            && row.try_get::<OffsetDateTime, _>("source_updated_at")?
                == registration.source_updated_at
            && row.try_get::<Vec<u8>, _>("body_sha256")? == registration.body_sha256,
    )
}

async fn point_binding_in_txn(
    txn: &mut Txn<'_>,
    point_id: ProjectionPointId,
) -> Result<Option<sqlx::postgres::PgRow>, PrivateProjectionRegistryError> {
    Ok(sqlx::query(
        "SELECT point_id, tenant_id, scope_kind, scope_id, domain, projection_kind, \
                projection_version, embedding_version, memory_id, source_updated_at, body_sha256 \
         FROM projection.private_memory_points WHERE point_id = $1",
    )
    .bind(point_id.0)
    .fetch_optional(&mut **txn)
    .await?)
}

async fn identity_binding_in_txn(
    txn: &mut Txn<'_>,
    registration: &PrivateMemoryPointRegistration,
) -> Result<Option<sqlx::postgres::PgRow>, PrivateProjectionRegistryError> {
    Ok(sqlx::query(
        "SELECT point_id, tenant_id, scope_kind, scope_id, domain, projection_kind, \
                projection_version, embedding_version, memory_id, source_updated_at, body_sha256 \
         FROM projection.private_memory_points \
         WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
           AND projection_kind = $5 AND projection_version = $6 AND embedding_version = $7 \
           AND memory_id = $8 AND source_updated_at = $9 AND body_sha256 = $10",
    )
    .bind(registration.family.tenant_id.0)
    .bind(&registration.family.scope_kind)
    .bind(registration.family.scope_id)
    .bind(&registration.family.domain)
    .bind(&registration.family.projection_kind)
    .bind(&registration.projection_version)
    .bind(&registration.embedding_version)
    .bind(registration.memory_id.0)
    .bind(registration.source_updated_at)
    .bind(&registration.body_sha256)
    .fetch_optional(&mut **txn)
    .await?)
}

/// Registers one exact PG source binding before any Qdrant I/O.  A replay with every identity
/// field unchanged is a no-op; a reused point id or duplicate identity with a different point
/// is an error and must not be repaired by silently minting another id.
pub async fn register_private_memory_point(
    pool: &RetrievalWorkerDbPool,
    authorization: &AuthorizationScope,
    registration: &PrivateMemoryPointRegistration,
) -> Result<RegistrationOutcome, PrivateProjectionRegistryError> {
    validate_input(authorization, registration)?;
    let mut txn = pool.pool().begin().await?;
    set_authorization_local(&mut txn, authorization).await?;
    current_source_matches(&mut txn, registration).await?;

    let inserted = sqlx::query(
        "INSERT INTO projection.private_memory_points \
             (point_id, tenant_id, scope_kind, scope_id, domain, projection_kind, \
              projection_version, embedding_version, memory_id, source_updated_at, body_sha256) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
         ON CONFLICT DO NOTHING RETURNING point_id",
    )
    .bind(registration.point_id.0)
    .bind(registration.family.tenant_id.0)
    .bind(&registration.family.scope_kind)
    .bind(registration.family.scope_id)
    .bind(&registration.family.domain)
    .bind(&registration.family.projection_kind)
    .bind(&registration.projection_version)
    .bind(&registration.embedding_version)
    .bind(registration.memory_id.0)
    .bind(registration.source_updated_at)
    .bind(&registration.body_sha256)
    .fetch_optional(&mut *txn)
    .await?;
    if inserted.is_some() {
        txn.commit().await?;
        return Ok(RegistrationOutcome::Inserted);
    }

    if let Some(existing) = point_binding_in_txn(&mut txn, registration.point_id).await? {
        if binding_matches(&existing, registration)? {
            txn.commit().await?;
            return Ok(RegistrationOutcome::AlreadyRegistered);
        }
        return Err(PrivateProjectionRegistryError::PointIdCollision);
    }
    if identity_binding_in_txn(&mut txn, registration)
        .await?
        .is_some()
    {
        return Err(PrivateProjectionRegistryError::IdentityAlreadyBound);
    }
    Err(PrivateProjectionRegistryError::RegistryRaceLost)
}

/// Marks exactly one existing binding non-live after the authoritative source is revoked or a
/// newer projection takes over.  It changes only registry liveness; Qdrant cleanup is outside
/// this transaction and a delayed remote write cannot bypass later PG resolution.
pub async fn retire_private_memory_point(
    pool: &RetrievalWorkerDbPool,
    authorization: &AuthorizationScope,
    family: &StreamFamily,
    projection_version: &str,
    embedding_version: &str,
    point_id: ProjectionPointId,
) -> Result<bool, PrivateProjectionRegistryError> {
    validate_family(authorization, family, projection_version, embedding_version)?;
    let mut txn = pool.pool().begin().await?;
    set_authorization_local(&mut txn, authorization).await?;
    let result = sqlx::query(
        "UPDATE projection.private_memory_points \
            SET projection_live = false, retired_at = now() \
          WHERE point_id = $1 AND tenant_id = $2 AND scope_kind = $3 AND scope_id = $4 \
            AND domain = $5 AND projection_kind = $6 AND projection_version = $7 \
            AND embedding_version = $8 \
            AND projection_live",
    )
    .bind(point_id.0)
    .bind(family.tenant_id.0)
    .bind(&family.scope_kind)
    .bind(family.scope_id)
    .bind(&family.domain)
    .bind(&family.projection_kind)
    .bind(projection_version)
    .bind(embedding_version)
    .execute(&mut *txn)
    .await?;
    txn.commit().await?;
    Ok(result.rows_affected() == 1)
}

/// Resolves only current, live candidate bindings.  Unknown, wrong-family/version, retired,
/// revoked/superseded, and source-stale rows are silently omitted so the later materializer sees
/// no untrusted Memory id at all.
pub async fn resolve_private_memory_points(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    family: &StreamFamily,
    projection_version: &str,
    embedding_version: &str,
    point_ids: &[ProjectionPointId],
) -> Result<Vec<ResolvedPrivateMemoryPoint>, PrivateProjectionRegistryError> {
    let mut txn = pool.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *txn)
        .await?;
    let resolved = resolve_private_memory_points_in_txn(
        &mut txn,
        authorization,
        family,
        projection_version,
        embedding_version,
        point_ids,
    )
    .await?;
    txn.commit().await?;
    Ok(resolved)
}

/// Same resolver under a caller-owned repeatable-read transaction so point identity and final
/// PostgreSQL hydration observe one snapshot. Qdrant scores remain candidate metadata only.
pub(crate) async fn resolve_private_memory_points_in_txn(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    family: &StreamFamily,
    projection_version: &str,
    embedding_version: &str,
    point_ids: &[ProjectionPointId],
) -> Result<Vec<ResolvedPrivateMemoryPoint>, PrivateProjectionRegistryError> {
    validate_family(authorization, family, projection_version, embedding_version)?;
    if point_ids.is_empty() {
        return Ok(Vec::new());
    }
    set_authorization_local(txn, authorization).await?;
    let ids = point_ids.iter().map(|id| id.0).collect::<Vec<_>>();
    let rows = sqlx::query(
        "SELECT r.point_id, r.memory_id \
         FROM projection.private_memory_points AS r \
         JOIN private.memory_records AS m \
           ON m.tenant_id = r.tenant_id AND m.memory_id = r.memory_id \
         WHERE r.tenant_id = $1 AND r.scope_kind = $2 AND r.scope_id = $3 AND r.domain = $4 \
           AND r.projection_kind = $5 AND r.projection_version = $6 \
           AND r.embedding_version = $7 AND r.point_id = ANY($8) \
           AND r.projection_live AND r.retired_at IS NULL \
           AND m.status = 'active' AND m.superseded_by IS NULL \
           AND r.source_updated_at = m.updated_at \
           AND r.body_sha256 = sha256(convert_to(m.content::text,'UTF8'))",
    )
    .bind(family.tenant_id.0)
    .bind(&family.scope_kind)
    .bind(family.scope_id)
    .bind(&family.domain)
    .bind(&family.projection_kind)
    .bind(projection_version)
    .bind(embedding_version)
    .bind(ids)
    .fetch_all(&mut **txn)
    .await?;

    let mut resolved = HashMap::with_capacity(rows.len());
    for row in rows {
        let point_id: Uuid = row.try_get("point_id")?;
        let memory_id: Uuid = row.try_get("memory_id")?;
        resolved.insert(point_id, MemoryId(memory_id));
    }

    let mut seen = HashSet::new();
    Ok(point_ids
        .iter()
        .filter(|point_id| seen.insert(point_id.0))
        .filter_map(|point_id| {
            resolved
                .get(&point_id.0)
                .copied()
                .map(|memory_id| ResolvedPrivateMemoryPoint {
                    point_id: *point_id,
                    memory_id,
                })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_domain::ids::TenantId;

    #[test]
    fn projection_point_id_has_no_memory_id_conversion() {
        let point = ProjectionPointId::new(Uuid::nil());
        assert_eq!(point.as_uuid(), Uuid::nil());
        let _tenant = TenantId(Uuid::nil());
    }

    #[test]
    fn deterministic_point_id_changes_with_embedding_identity() {
        let family = StreamFamily::new(
            TenantId(Uuid::from_u128(1)),
            "tenant",
            Uuid::from_u128(1),
            "knowledge",
            "dense",
        );
        let make = |embedding: &str| {
            PrivateMemoryPointRegistration::deterministic(
                family.clone(),
                "projection-v1".to_owned(),
                embedding.to_owned(),
                MemoryId(Uuid::from_u128(2)),
                OffsetDateTime::from_unix_timestamp(1_704_067_200).unwrap(),
                vec![7; 32],
            )
        };
        assert_eq!(make("embed-v1").point_id, make("embed-v1").point_id);
        assert_ne!(make("embed-v1").point_id, make("embed-v2").point_id);
        assert_ne!(make("embed-v1").point_id.as_uuid(), Uuid::from_u128(2));
    }
}
