//! `adapters::continuity_repo` — Typed role_gateway bindings for migration 0136's two Project Continuity commands.
//! Depends-on: crates=[humaux-domain, serde_json, sqlx, uuid]; services=[PostgreSQL(any) x=[private.publish_continuity_facet, private.register_continuity_project]]; env=[]; modules=[adapters::postgres, domain::continuity, domain::error, domain::identity, domain::ids]
//! Called-by: []
//! Invariants: [registration and facet publication go only through the two SECURITY DEFINER functions; their
//!   SQLSTATEs map to Conflict/Forbidden/InvalidInput, never a silent retry]
//! Spec: none

use crate::postgres::RuntimeDbPool;
use humaux_domain::{
    continuity::{
        ContinuityFacetKind, ContinuityFacetVersionId, ContinuityPublishResult,
        ContinuityPublishState, ProjectId,
    },
    error::ErrorCode,
    identity::AuthorizationScope,
    ids::WorkspaceId,
};
use serde_json::Value;
use sqlx::{Row, Transaction};
use uuid::Uuid;

const NIL_UUID: Uuid = Uuid::nil();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinuityMemorySource {
    pub memory_id: Uuid,
    pub content_sha256: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinuityEvidenceSource {
    pub evidence_id: Uuid,
    pub payload_sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterContinuityProject<'a> {
    pub workspace_id: WorkspaceId,
    pub project_id: ProjectId,
    pub title: &'a str,
}

#[derive(Debug, Clone)]
pub struct PublishContinuityFacet<'a> {
    pub workspace_id: WorkspaceId,
    pub project_id: ProjectId,
    pub facet_kind: ContinuityFacetKind,
    pub expected_slot_version: i64,
    pub published_state: ContinuityPublishState,
    pub body: &'a Value,
    pub memories: &'a [ContinuityMemorySource],
    pub evidence: &'a [ContinuityEvidenceSource],
}

async fn install_context(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    scope: &AuthorizationScope,
    workspace_id: WorkspaceId,
) -> Result<(), ErrorCode> {
    let user = scope.user_id().map_or(NIL_UUID, |id| id.0);
    sqlx::query(
        "SELECT set_config('humaux.tenant_id',$1,true),\
                set_config('humaux.workspace_id',$2,true),\
                set_config('humaux.principal_id',$3,true),\
                set_config('humaux.user_id',$4,true)",
    )
    .bind(scope.tenant_id().0.to_string())
    .bind(workspace_id.0.to_string())
    .bind(scope.principal().0.to_string())
    .bind(user.to_string())
    .execute(&mut **tx)
    .await
    .map_err(map_db_error)?;
    Ok(())
}

fn map_db_error(error: sqlx::Error) -> ErrorCode {
    match error.as_database_error().and_then(|db| db.code()) {
        Some(code) if code == "P9C01" => ErrorCode::Conflict,
        Some(code) if code == "P9I01" => ErrorCode::Internal,
        Some(code) if code == "42501" => ErrorCode::Forbidden,
        Some(code) if code.starts_with("22") || code == "23514" || code == "23503" => {
            ErrorCode::InvalidInput
        }
        _ => ErrorCode::Internal,
    }
}

pub async fn register_project(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    command: &RegisterContinuityProject<'_>,
) -> Result<ProjectId, ErrorCode> {
    let narrowed = authorization.narrow(command.workspace_id)?;
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut tx = pool.pool().begin().await.map_err(map_db_error)?;
    install_context(&mut tx, &narrowed, command.workspace_id).await?;
    let user = narrowed.user_id().map(|id| id.0);
    let row =
        sqlx::query("SELECT private.register_continuity_project($1,$2,$3,$4,$5,$6) AS project_id")
            .bind(narrowed.tenant_id().0)
            .bind(command.workspace_id.0)
            .bind(command.project_id.0)
            .bind(narrowed.principal().0)
            .bind(user)
            .bind(command.title)
            .fetch_one(&mut *tx)
            .await
            .map_err(map_db_error)?;
    let project_id: Uuid = row.try_get("project_id").map_err(map_db_error)?;
    tx.commit().await.map_err(map_db_error)?;
    Ok(ProjectId(project_id))
}

pub async fn publish_facet(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    command: &PublishContinuityFacet<'_>,
) -> Result<ContinuityPublishResult, ErrorCode> {
    let narrowed = authorization.narrow(command.workspace_id)?;
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut tx = pool.pool().begin().await.map_err(map_db_error)?;
    install_context(&mut tx, &narrowed, command.workspace_id).await?;
    let user = narrowed.user_id().map(|id| id.0);
    let memory_ids: Vec<Uuid> = command
        .memories
        .iter()
        .map(|source| source.memory_id)
        .collect();
    let memory_hashes: Vec<Vec<u8>> = command
        .memories
        .iter()
        .map(|source| source.content_sha256.to_vec())
        .collect();
    let evidence_ids: Vec<Uuid> = command
        .evidence
        .iter()
        .map(|source| source.evidence_id)
        .collect();
    let evidence_hashes: Vec<Vec<u8>> = command
        .evidence
        .iter()
        .map(|source| source.payload_sha256.to_vec())
        .collect();
    let row = sqlx::query(
        "SELECT facet_version_id,facet_version,slot_version,body_sha256 \
         FROM private.publish_continuity_facet(\
           $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
    )
    .bind(narrowed.tenant_id().0)
    .bind(command.workspace_id.0)
    .bind(command.project_id.0)
    .bind(narrowed.principal().0)
    .bind(user)
    .bind(command.facet_kind.as_str())
    .bind(command.expected_slot_version)
    .bind(command.published_state.as_str())
    .bind(command.body)
    .bind(memory_ids)
    .bind(memory_hashes)
    .bind(evidence_ids)
    .bind(evidence_hashes)
    .fetch_one(&mut *tx)
    .await
    .map_err(map_db_error)?;
    let body_hash: Vec<u8> = row.try_get("body_sha256").map_err(map_db_error)?;
    let body_sha256: [u8; 32] = body_hash.try_into().map_err(|_| ErrorCode::Internal)?;
    let result = ContinuityPublishResult {
        facet_version_id: ContinuityFacetVersionId(
            row.try_get("facet_version_id").map_err(map_db_error)?,
        ),
        facet_version: row.try_get("facet_version").map_err(map_db_error)?,
        slot_version: row.try_get("slot_version").map_err(map_db_error)?,
        body_sha256,
    };
    tx.commit().await.map_err(map_db_error)?;
    Ok(result)
}
