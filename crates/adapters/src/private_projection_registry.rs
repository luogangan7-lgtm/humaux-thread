//! `adapters::private_projection_registry` — PostgreSQL-only identity binding for private Qdrant candidates, the
//!   stored vector each binding was projected from, and the embedding label's fingerprint binding.
//! Depends-on: crates=[humaux-domain, humaux-projection, sha2, sqlx]; services=[PostgreSQL(any)
//!   r=[private.memory_records] w=[projection.embedding_fingerprints, projection.memory_vectors,
//!   projection.private_memory_points], PostgreSQL(role_gateway), PostgreSQL(role_retrieval_worker)]; env=[];
//!   modules=[adapters::postgres, domain::authority, domain::identity, domain::ids, projection::card,
//!   projection::embedding_fingerprint, projection::serving]
//! Called-by: [adapters::projection_worker, adapters::retrieve, retrieval-worker::main, tests]
//! Invariants: [sole private point-id resolver: binds a Qdrant point id to a Memory in PostgreSQL and re-checks the
//!   source fence; no Qdrant IO here; cross-tenant/workspace, collisions and lost races are typed errors, never a
//!   guessed binding; a registration that carries a vector writes it in the SAME transaction as the registry row
//!   (ADR-0064 D-B), so a fingerprinted registry row always has its vector row (0232 FK); every retirement purges
//!   the vector bytes the last live binding held, by UPDATE (ADR-0064 D-D); one embedding label is bound to one
//!   fingerprint and a different one is refused, never overwritten (ADR-0064 D-C)]
//! Spec: Baseline §16.1; §17; §44; ADR-0049; ADR-0064 D-B; ADR-0064 D-C; ADR-0064 D-D
//!
//! Qdrant returns an opaque point id plus a score.  This module is the sole private-plane
//! point-id resolver: it binds that id to a Memory through PostgreSQL and then rechecks the
//! mutable Memory source fence.  It deliberately performs no Qdrant I/O; remote projection
//! work must happen after a committed PG registration transaction.

use std::collections::{HashMap, HashSet};

use humaux_domain::authority::MemoryId;
use humaux_domain::identity::AuthorizationScope;
use humaux_domain::ids::WorkspaceId;
use humaux_projection::card::CARD_TEMPLATE_HASH;
use humaux_projection::embedding_fingerprint::{
    DISTANCE, DTYPE, EmbeddingFingerprint, EmbeddingFingerprintInputs, NORMALIZATION,
};
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

/// ADR-0064 D-B: the provider vector a registration was projected from, keyed by the vector space and the exact
/// embedded bytes. Written in the registry transaction; never constructed from a Qdrant read.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredVector {
    /// The worker label's fingerprint (`projection.embedding_fingerprints`, ADR-0064 D-C).
    pub fingerprint_sha256: [u8; 32],
    /// sha256 of the sealed card text that was (or would be) embedded.
    pub input_sha256: [u8; 32],
    /// The provider's raw vector; finite and non-empty, else the registration is `InvalidInput`.
    pub vector: Vec<f32>,
}

/// ADR-0064 D-C: the provider task type of every vector the projection worker stores — cards are documents (query
/// vectors are embedded by `--serve-rpc` and never stored).
pub const PROJECTION_TASK_TYPE: &str = "document";

/// ADR-0064 D-D: the one purge statement both retire paths run in their liveness transaction — the bytes of a
/// memory's vectors that no live binding references any more become NULL (the key row stays, ~150 B).
const PURGE_UNREFERENCED_VECTORS: &str = "UPDATE projection.memory_vectors v \
        SET vector = NULL, purged_at = clock_timestamp() \
      WHERE v.tenant_id = $1 AND v.memory_id = $2 AND v.vector IS NOT NULL \
        AND NOT EXISTS (SELECT 1 FROM projection.private_memory_points p \
                         WHERE p.projection_live AND p.tenant_id = v.tenant_id AND p.memory_id = v.memory_id \
                           AND p.fingerprint_sha256 = v.fingerprint_sha256 AND p.input_sha256 = v.input_sha256)";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationOutcome {
    Inserted,
    AlreadyRegistered,
    /// ADR-0049: the identical binding existed but was retired (the memory had been
    /// superseded); the source is live again under the same identity, so the binding is too.
    Revived,
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
    /// ADR-0064 D-C: the embedding label and the computed fingerprint are not bound to each other (the label holds
    /// another fingerprint, or the fingerprint is bound under another label).
    FingerprintMismatch,
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
            Self::FingerprintMismatch => write!(
                f,
                "embedding label is bound to another model fingerprint (ADR-0064 D-C)"
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

/// Registers one exact PG source binding without a stored vector (a legacy row, ADR-0064 D-G). The projection
/// worker calls [`register_private_memory_point_with_vector`]; this form stays for callers that bind no vector.
pub async fn register_private_memory_point(
    pool: &RetrievalWorkerDbPool,
    authorization: &AuthorizationScope,
    registration: &PrivateMemoryPointRegistration,
) -> Result<RegistrationOutcome, PrivateProjectionRegistryError> {
    register_private_memory_point_with_vector(pool, authorization, registration, None).await
}

/// Registers one exact PG source binding before any Qdrant I/O.  A replay with every identity
/// field unchanged is a no-op; a reused point id or duplicate identity with a different point
/// is an error and must not be repaired by silently minting another id.
///
/// ADR-0064 D-B: with `vector`, the vector row is upserted first and the registry row carries its key, in ONE
/// transaction, so DONE ⇒ live registry row ⇒ stored vector. An existing binding without a key (a legacy row) is
/// given this one (backfill-on-touch); a purged vector row is re-stored (a revival).
pub async fn register_private_memory_point_with_vector(
    pool: &RetrievalWorkerDbPool,
    authorization: &AuthorizationScope,
    registration: &PrivateMemoryPointRegistration,
    vector: Option<&StoredVector>,
) -> Result<RegistrationOutcome, PrivateProjectionRegistryError> {
    validate_input(authorization, registration)?;
    // ADR-0064 D-B: PostgreSQL has no cheap all-finite CHECK on real[]; a NaN/inf vector is refused here.
    if vector.is_some_and(|v| v.vector.is_empty() || !v.vector.iter().all(|x| x.is_finite())) {
        return Err(PrivateProjectionRegistryError::InvalidInput);
    }
    // dep: PostgreSQL(role_retrieval_worker) — transaction entry for `register_private_memory_point_with_vector`
    let mut txn = pool.pool().begin().await?;
    set_authorization_local(&mut txn, authorization).await?;
    current_source_matches(&mut txn, registration).await?;
    if let Some(stored) = vector {
        let dimension = i32::try_from(stored.vector.len())
            .map_err(|_| PrivateProjectionRegistryError::InvalidInput)?;
        sqlx::query(
            "INSERT INTO projection.memory_vectors \
                 (tenant_id, memory_id, fingerprint_sha256, input_sha256, dimension, vector) \
             VALUES ($1,$2,$3,$4,$5,$6) \
             ON CONFLICT (tenant_id, memory_id, fingerprint_sha256, input_sha256) \
             DO UPDATE SET vector = EXCLUDED.vector, purged_at = NULL WHERE memory_vectors.vector IS NULL",
        )
        .bind(registration.family.tenant_id.0)
        .bind(registration.memory_id.0)
        .bind(&stored.fingerprint_sha256[..])
        .bind(&stored.input_sha256[..])
        .bind(dimension)
        .bind(&stored.vector)
        .execute(&mut *txn)
        .await?;
    }
    let fingerprint = vector.map(|v| &v.fingerprint_sha256[..]);
    let input = vector.map(|v| &v.input_sha256[..]);

    let inserted = sqlx::query(
        "INSERT INTO projection.private_memory_points \
             (point_id, tenant_id, scope_kind, scope_id, domain, projection_kind, \
              projection_version, embedding_version, memory_id, source_updated_at, body_sha256, \
              fingerprint_sha256, input_sha256) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) \
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
    .bind(fingerprint)
    .bind(input)
    .fetch_optional(&mut *txn)
    .await?;
    if inserted.is_some() {
        txn.commit().await?;
        return Ok(RegistrationOutcome::Inserted);
    }

    if let Some(existing) = point_binding_in_txn(&mut txn, registration.point_id).await? {
        if binding_matches(&existing, registration)? {
            // ADR-0049: an identical registration of a RETIRED binding is a restore — the
            // source is live again under the same identity (`memory.restore` flips status
            // back without touching `updated_at`), so the binding comes back with it rather
            // than staying retired behind an `AlreadyRegistered` nobody could resolve
            // (card 24 rehearsal 2026-09-26: `restored_memory_is_servable_again = 0`).
            let revived = sqlx::query(
                "UPDATE projection.private_memory_points \
                    SET projection_live = true, retired_at = NULL \
                  WHERE point_id = $1 AND NOT projection_live",
            )
            .bind(registration.point_id.0)
            .execute(&mut *txn)
            .await?;
            // ADR-0064 D-B backfill-on-touch: a binding registered before card 37 takes this vector's key.
            if let (Some(fingerprint), Some(input)) = (fingerprint, input) {
                sqlx::query(
                    "UPDATE projection.private_memory_points \
                        SET fingerprint_sha256 = $2, input_sha256 = $3 \
                      WHERE point_id = $1 AND fingerprint_sha256 IS NULL",
                )
                .bind(registration.point_id.0)
                .bind(fingerprint)
                .bind(input)
                .execute(&mut *txn)
                .await?;
            }
            txn.commit().await?;
            return Ok(if revived.rows_affected() == 1 {
                RegistrationOutcome::Revived
            } else {
                RegistrationOutcome::AlreadyRegistered
            });
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
    // dep: PostgreSQL(role_retrieval_worker) — transaction entry for `retire_private_memory_point`
    let mut txn = pool.pool().begin().await?;
    set_authorization_local(&mut txn, authorization).await?;
    let retired: Option<Uuid> = sqlx::query_scalar(
        "UPDATE projection.private_memory_points \
            SET projection_live = false, retired_at = now() \
          WHERE point_id = $1 AND tenant_id = $2 AND scope_kind = $3 AND scope_id = $4 \
            AND domain = $5 AND projection_kind = $6 AND projection_version = $7 \
            AND embedding_version = $8 \
            AND projection_live \
          RETURNING memory_id",
    )
    .bind(point_id.0)
    .bind(family.tenant_id.0)
    .bind(&family.scope_kind)
    .bind(family.scope_id)
    .bind(&family.domain)
    .bind(&family.projection_kind)
    .bind(projection_version)
    .bind(embedding_version)
    .fetch_optional(&mut *txn)
    .await?;
    if let Some(memory_id) = retired {
        sqlx::query(PURGE_UNREFERENCED_VECTORS)
            .bind(family.tenant_id.0)
            .bind(memory_id)
            .execute(&mut *txn)
            .await?;
    }
    txn.commit().await?;
    Ok(retired.is_some())
}

/// ADR-0049: the projection consequence of a memory that stopped being live (superseded,
/// revoked, expired). Every binding this family/version holds for it is retired, and every
/// point id ever bound for it — live or already retired — is returned, so the caller's Qdrant
/// delete is safe to repeat after a failure between the two writes.
pub async fn retire_points_for_memory(
    pool: &RetrievalWorkerDbPool,
    authorization: &AuthorizationScope,
    family: &StreamFamily,
    projection_version: &str,
    embedding_version: &str,
    memory_id: MemoryId,
) -> Result<Vec<ProjectionPointId>, PrivateProjectionRegistryError> {
    validate_family(authorization, family, projection_version, embedding_version)?;
    // dep: PostgreSQL(role_retrieval_worker) — transaction entry for `retire_points_for_memory`
    let mut txn = pool.pool().begin().await?;
    set_authorization_local(&mut txn, authorization).await?;
    let point_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT point_id FROM projection.private_memory_points \
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
            AND projection_kind = $5 AND projection_version = $6 AND embedding_version = $7 \
            AND memory_id = $8 \
          ORDER BY created_at, point_id",
    )
    .bind(family.tenant_id.0)
    .bind(&family.scope_kind)
    .bind(family.scope_id)
    .bind(&family.domain)
    .bind(&family.projection_kind)
    .bind(projection_version)
    .bind(embedding_version)
    .bind(memory_id.0)
    .fetch_all(&mut *txn)
    .await?;
    sqlx::query(
        "UPDATE projection.private_memory_points \
            SET projection_live = false, retired_at = now() \
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
            AND projection_kind = $5 AND projection_version = $6 AND embedding_version = $7 \
            AND memory_id = $8 AND projection_live",
    )
    .bind(family.tenant_id.0)
    .bind(&family.scope_kind)
    .bind(family.scope_id)
    .bind(&family.domain)
    .bind(&family.projection_kind)
    .bind(projection_version)
    .bind(embedding_version)
    .bind(memory_id.0)
    .execute(&mut *txn)
    .await?;
    sqlx::query(PURGE_UNREFERENCED_VECTORS)
        .bind(family.tenant_id.0)
        .bind(memory_id.0)
        .execute(&mut *txn)
        .await?;
    txn.commit().await?;
    Ok(point_ids.into_iter().map(ProjectionPointId::new).collect())
}

/// ADR-0064 D-C: the fingerprint inputs of the projection worker's embedding configuration. The task type, the card
/// template hash and the ticket family's projection version are fixed by the build, never by configuration.
pub fn worker_fingerprint_inputs<'a>(
    provider: &'a str,
    model_id: &'a str,
    model_revision: &'a str,
    dimension: u32,
    projection_version: &'a str,
) -> EmbeddingFingerprintInputs<'a> {
    EmbeddingFingerprintInputs {
        provider,
        model_id,
        model_revision,
        dimension,
        task_type: PROJECTION_TASK_TYPE,
        preprocessing_version: CARD_TEMPLATE_HASH,
        projection_contract_version: projection_version,
    }
}

/// ADR-0064 D-C: binds `embedding_version` to the fingerprint of `inputs` (`INSERT .. ON CONFLICT DO NOTHING`) and
/// reads the label's row back. A label already bound to another fingerprint (or this fingerprint already bound under
/// another label) is [`PrivateProjectionRegistryError::FingerprintMismatch`]; the row is never overwritten. The
/// retrieval worker calls this before its first pass and refuses to run on a mismatch.
pub async fn bind_embedding_fingerprint(
    pool: &RetrievalWorkerDbPool,
    embedding_version: &str,
    inputs: &EmbeddingFingerprintInputs<'_>,
) -> Result<EmbeddingFingerprint, PrivateProjectionRegistryError> {
    let fingerprint = EmbeddingFingerprint::compute(inputs)
        .map_err(|_| PrivateProjectionRegistryError::InvalidInput)?;
    let dimension = i32::try_from(inputs.dimension)
        .map_err(|_| PrivateProjectionRegistryError::InvalidInput)?;
    if !valid_text(embedding_version, 128) {
        return Err(PrivateProjectionRegistryError::InvalidInput);
    }
    // dep: PostgreSQL(role_retrieval_worker) — the embedding label's one-time fingerprint binding
    sqlx::query(
        "INSERT INTO projection.embedding_fingerprints (fingerprint_sha256, embedding_version, provider, model_id, \
           model_revision, dimension, task_type, preprocessing_version, projection_contract_version, dtype, \
           normalization, distance) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING",
    )
    .bind(&fingerprint.0[..])
    .bind(embedding_version)
    .bind(inputs.provider)
    .bind(inputs.model_id)
    .bind(inputs.model_revision)
    .bind(dimension)
    .bind(inputs.task_type)
    .bind(inputs.preprocessing_version)
    .bind(inputs.projection_contract_version)
    .bind(DTYPE)
    .bind(NORMALIZATION)
    .bind(DISTANCE)
    .execute(pool.pool())
    .await?;
    let bound: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT fingerprint_sha256 FROM projection.embedding_fingerprints WHERE embedding_version = $1",
    )
    .bind(embedding_version)
    // dep: PostgreSQL(role_retrieval_worker) — reads the label's binding back (the compare is by fingerprint)
    .fetch_optional(pool.pool())
    .await?;
    if bound.as_deref() != Some(&fingerprint.0[..]) {
        return Err(PrivateProjectionRegistryError::FingerprintMismatch);
    }
    Ok(fingerprint)
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
    // dep: PostgreSQL(role_gateway) — transaction entry for `resolve_private_memory_points`
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
