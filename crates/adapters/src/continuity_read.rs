//! `adapters::continuity_read` — Project Continuity W2 PostgreSQL reader.
//! Depends-on: crates=[async-trait, hex, humaux-application, humaux-domain, humaux-retrieval, sha2, sqlx, uuid]; services=[PostgreSQL(any) r=[ops.outbox, private.evidence_objects, private.memory_evidence, private.memory_records, projection.stream_log] x=[private.read_continuity_project_storage_v1]]; env=[]; modules=[adapters::context_repo, adapters::postgres, adapters::read_materialize, application::continuity, domain::context, domain::continuity, domain::error, domain::grounding, domain::identity, domain::ids, retrieval::handoff]
//! Called-by: [gateway::continuity, tests]
//! Invariants: [the whole read is one REPEATABLE READ READ ONLY gateway transaction; only an authorized parent
//!   installs the workspace; an incomplete read is CannotEstablishCompleteness, an unauthorized one NotFound]
//! Spec: none
//!
//! The whole read is one Gateway-owned, repeatable-read, read-only transaction. The project
//! lookup runs with a nil workspace GUC; only an authorized returned parent can install the
//! exact workspace used by source visibility and deterministic Handoff assembly.

use std::{collections::HashMap, str::FromStr, sync::Arc};

use humaux_application::continuity::{
    ClosedContinuitySnapshot, ContinuityReadPort, DiagnosticCode, SourceFacetInput,
    SourceFacetState, VerifiedCurrentFacet, W2_FORCED_UNAVAILABLE,
};
use humaux_domain::{
    context::ContextBudget,
    continuity::{ContinuityFacetKind, ProjectId},
    error::ErrorCode,
    grounding::{
        GroundingMode, GroundingStateKind, RowGrounding, SnapshotEdge, classify_in_snapshot,
    },
    identity::{AuthorizationScope, can_read},
    ids::{Scope, WorkspaceId},
};
use humaux_retrieval::handoff::assemble;
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use crate::{
    context_repo::{fetch_frozen_in_txn, visibility_from_row},
    postgres::RuntimeDbPool,
    read_materialize::final_memory_ids_in_txn,
};

type Txn<'c> = Transaction<'c, Postgres>;
const NIL_UUID: Uuid = Uuid::nil();

pub struct PostgresContinuityReadPort {
    pool: Arc<RuntimeDbPool>,
}

impl PostgresContinuityReadPort {
    #[must_use]
    pub fn new(pool: Arc<RuntimeDbPool>) -> Self {
        Self { pool }
    }
}

#[derive(Debug)]
struct RawSlot {
    project_id: Option<Uuid>,
    tenant_id: Option<Uuid>,
    workspace_id: Option<Uuid>,
    storage_snapshot_seq: Option<i64>,
    storage_snapshot_token: Option<String>,
    kind: Option<String>,
    slot_version: Option<i64>,
    slot_state: Option<String>,
    current_version_id: Option<Uuid>,
    facet_version: Option<i64>,
    body_text: Option<String>,
    stored_body_sha256: Option<Vec<u8>>,
    body_hash_closed: Option<bool>,
    selected_is_latest: Option<bool>,
    memory_links_exact: Option<bool>,
    memory_source_ids: Option<Vec<Option<Uuid>>>,
    memory_source_hashes: Option<Vec<Option<Vec<u8>>>>,
    evidence_links_exact: Option<bool>,
    evidence_source_ids: Option<Vec<Option<Uuid>>>,
    evidence_source_hashes: Option<Vec<Option<Vec<u8>>>>,
}

impl RawSlot {
    fn decode(row: &PgRow) -> Result<Self, ErrorCode> {
        macro_rules! get {
            ($name:literal) => {
                row.try_get($name)
                    .map_err(|_| ErrorCode::CannotEstablishCompleteness)?
            };
        }
        Ok(Self {
            project_id: get!("project_id"),
            tenant_id: get!("tenant_id"),
            workspace_id: get!("workspace_id"),
            storage_snapshot_seq: get!("storage_snapshot_seq"),
            storage_snapshot_token: get!("storage_snapshot_token"),
            kind: get!("facet_kind"),
            slot_version: get!("slot_version"),
            slot_state: get!("slot_state"),
            current_version_id: get!("current_version_id"),
            facet_version: get!("facet_version"),
            body_text: get!("body_text"),
            stored_body_sha256: get!("stored_body_sha256"),
            body_hash_closed: get!("body_hash_closed"),
            selected_is_latest: get!("selected_is_latest"),
            memory_links_exact: get!("memory_links_exact"),
            memory_source_ids: get!("memory_source_ids"),
            memory_source_hashes: get!("memory_source_hashes"),
            evidence_links_exact: get!("evidence_links_exact"),
            evidence_source_ids: get!("evidence_source_ids"),
            evidence_source_hashes: get!("evidence_source_hashes"),
        })
    }
}

fn digest(bytes: Option<Vec<u8>>) -> Result<[u8; 32], ErrorCode> {
    bytes
        .ok_or(ErrorCode::CannotEstablishCompleteness)?
        .try_into()
        .map_err(|_| ErrorCode::CannotEstablishCompleteness)
}

fn source_pairs(
    ids: Option<Vec<Option<Uuid>>>,
    hashes: Option<Vec<Option<Vec<u8>>>>,
) -> Result<Vec<(Uuid, [u8; 32])>, ErrorCode> {
    let ids = ids.ok_or(ErrorCode::CannotEstablishCompleteness)?;
    let hashes = hashes.ok_or(ErrorCode::CannotEstablishCompleteness)?;
    if ids.len() != hashes.len() {
        return Err(ErrorCode::CannotEstablishCompleteness);
    }
    let mut out = Vec::with_capacity(ids.len());
    for (id, hash) in ids.into_iter().zip(hashes) {
        let id = id
            .filter(|value| !value.is_nil())
            .ok_or(ErrorCode::CannotEstablishCompleteness)?;
        out.push((id, digest(hash)?));
    }
    if out.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(ErrorCode::CannotEstablishCompleteness);
    }
    Ok(out)
}

async fn install_lookup_context(
    tx: &mut Txn<'_>,
    authorization: &AuthorizationScope,
) -> Result<(), ErrorCode> {
    sqlx::query(
        "SELECT set_config('humaux.tenant_id',$1,true),\
                set_config('humaux.workspace_id',$2,true),\
                set_config('humaux.principal_id',$3,true),\
                set_config('humaux.user_id',$4,true)",
    )
    .bind(authorization.tenant_id().0.to_string())
    .bind(NIL_UUID.to_string())
    .bind(authorization.principal().0.to_string())
    .bind(
        authorization
            .user_id()
            .map_or(NIL_UUID, |value| value.0)
            .to_string(),
    )
    .execute(&mut **tx)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(())
}

async fn install_workspace_context(
    tx: &mut Txn<'_>,
    workspace_id: WorkspaceId,
) -> Result<(), ErrorCode> {
    sqlx::query("SELECT set_config('humaux.workspace_id',$1,true)")
        .bind(workspace_id.0.to_string())
        .execute(&mut **tx)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(())
}

async fn read_raw_slots(
    tx: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    project_id: ProjectId,
    requested_workspace: Option<WorkspaceId>,
) -> Result<Vec<RawSlot>, ErrorCode> {
    let mut workspaces: Vec<Uuid> = authorization
        .allowed_workspace_ids()
        .iter()
        .map(|value| value.0)
        .collect();
    workspaces.sort_unstable();
    let rows =
        sqlx::query("SELECT * FROM private.read_continuity_project_storage_v1($1,$2,$3,$4,$5,$6)")
            .bind(authorization.tenant_id().0)
            .bind(project_id.0)
            .bind(requested_workspace.map(|value| value.0))
            .bind(authorization.principal().0)
            .bind(authorization.user_id().map(|value| value.0))
            .bind(workspaces)
            .fetch_all(&mut **tx)
            .await
            .map_err(
                |error| match error.as_database_error().and_then(|db| db.code()) {
                    Some(code) if code == "42501" => ErrorCode::Internal,
                    _ => ErrorCode::DependencyUnavailable,
                },
            )?;
    rows.iter().map(RawSlot::decode).collect()
}

fn memory_grounding_current(row: &PgRow) -> Result<bool, ErrorCode> {
    let has_live: bool = row
        .try_get("has_live")
        .map_err(|_| ErrorCode::CannotEstablishCompleteness)?;
    let has_live_unversioned: bool = row
        .try_get("has_live_unversioned")
        .map_err(|_| ErrorCode::CannotEstablishCompleteness)?;
    let edges = if has_live_unversioned {
        vec![SnapshotEdge {
            mode: GroundingMode::Live,
            recorded_version_present: false,
        }]
    } else if has_live {
        vec![SnapshotEdge {
            mode: GroundingMode::Live,
            recorded_version_present: true,
        }]
    } else {
        Vec::new()
    };
    Ok(matches!(
        classify_in_snapshot(&edges),
        RowGrounding::Judged(state) if state.kind() == GroundingStateKind::Current
    ))
}

async fn validate_memories(
    tx: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    expected: &[(Uuid, [u8; 32])],
) -> Result<bool, ErrorCode> {
    if expected.is_empty() {
        return Ok(true);
    }
    let ids: Vec<Uuid> = expected.iter().map(|item| item.0).collect();
    let final_ids = final_memory_ids_in_txn(tx, authorization, &ids, false)
        .await
        .map_err(|error| match error {
            ErrorCode::Internal => ErrorCode::DependencyUnavailable,
            other => other,
        })?;
    if final_ids != ids {
        return Ok(false);
    }
    let rows = sqlx::query(
        r#"SELECT m.memory_id,sha256(convert_to(m.content::text,'UTF8')) AS content_sha256,
                EXISTS(SELECT 1 FROM private.memory_evidence me
                  WHERE me.memory_id=m.memory_id AND me.grounding_mode='LIVE') AS has_live,
                EXISTS(SELECT 1 FROM private.memory_evidence me
                  WHERE me.memory_id=m.memory_id AND me.grounding_mode='LIVE'
                    AND me.recorded_version IS NULL) AS has_live_unversioned
         FROM private.memory_records m WHERE m.tenant_id=$1 AND m.memory_id=ANY($2)
         ORDER BY m.memory_id"#,
    )
    .bind(authorization.tenant_id().0)
    .bind(&ids)
    .fetch_all(&mut **tx)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;
    if rows.len() != expected.len() {
        return Ok(false);
    }
    for (row, (expected_id, expected_hash)) in rows.iter().zip(expected) {
        let id: Uuid = row
            .try_get("memory_id")
            .map_err(|_| ErrorCode::CannotEstablishCompleteness)?;
        let hash: Option<Vec<u8>> = row
            .try_get("content_sha256")
            .map_err(|_| ErrorCode::CannotEstablishCompleteness)?;
        if id != *expected_id || digest(hash)? != *expected_hash || !memory_grounding_current(row)?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EvidenceValidation {
    Valid,
    VisibilityFailed,
    SourceFailed,
}

async fn validate_evidence(
    tx: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    expected: &[(Uuid, [u8; 32])],
) -> Result<EvidenceValidation, ErrorCode> {
    if expected.is_empty() {
        return Ok(EvidenceValidation::Valid);
    }
    let ids: Vec<Uuid> = expected.iter().map(|item| item.0).collect();
    let rows = sqlx::query(
        r#"SELECT evidence_id,payload_sha256,data_class,visibility_class,
                visibility_user_id,visibility_workspace_id
         FROM private.evidence_objects WHERE tenant_id=$1 AND evidence_id=ANY($2)
         ORDER BY evidence_id"#,
    )
    .bind(authorization.tenant_id().0)
    .bind(&ids)
    .fetch_all(&mut **tx)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;
    if rows.len() != expected.len() {
        return Ok(EvidenceValidation::SourceFailed);
    }
    for (row, (expected_id, expected_hash)) in rows.iter().zip(expected) {
        let id: Uuid = row
            .try_get("evidence_id")
            .map_err(|_| ErrorCode::CannotEstablishCompleteness)?;
        let hash: Option<Vec<u8>> = row
            .try_get("payload_sha256")
            .map_err(|_| ErrorCode::CannotEstablishCompleteness)?;
        let data_class: Option<String> = row
            .try_get("data_class")
            .map_err(|_| ErrorCode::CannotEstablishCompleteness)?;
        if id != *expected_id || digest(hash)? != *expected_hash {
            return Ok(EvidenceValidation::SourceFailed);
        }
        if data_class.as_deref() == Some("SECRET_MATERIAL")
            || !can_read(authorization, &visibility_from_row(row)?)
        {
            return Ok(EvidenceValidation::VisibilityFailed);
        }
    }
    let lifecycle = sqlx::query(
        r#"SELECT expected.id,count(stream.commit_seq) AS association_count,
                coalesce(bool_or(stream.state='TOMBSTONED'),false) AS has_tombstone
         FROM unnest($2::uuid[]) expected(id)
         LEFT JOIN ops.outbox outbox ON outbox.tenant_id=$1
           AND outbox.evidence_id=expected.id
         LEFT JOIN projection.stream_log stream ON stream.tenant_id=outbox.tenant_id
           AND stream.commit_seq=outbox.commit_seq
         GROUP BY expected.id ORDER BY expected.id"#,
    )
    .bind(authorization.tenant_id().0)
    .bind(&ids)
    .fetch_all(&mut **tx)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;
    if lifecycle.len() != expected.len() {
        return Ok(EvidenceValidation::SourceFailed);
    }
    for (row, expected_id) in lifecycle.iter().zip(&ids) {
        let id: Uuid = row
            .try_get("id")
            .map_err(|_| ErrorCode::CannotEstablishCompleteness)?;
        let count: i64 = row
            .try_get("association_count")
            .map_err(|_| ErrorCode::CannotEstablishCompleteness)?;
        let tombstoned: bool = row
            .try_get("has_tombstone")
            .map_err(|_| ErrorCode::CannotEstablishCompleteness)?;
        if id != *expected_id || count <= 0 || tombstoned {
            return Ok(EvidenceValidation::SourceFailed);
        }
    }
    Ok(EvidenceValidation::Valid)
}

async fn close_slot(
    tx: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    raw: RawSlot,
) -> Result<SourceFacetInput, ErrorCode> {
    let kind = ContinuityFacetKind::from_str(
        raw.kind
            .as_deref()
            .ok_or(ErrorCode::CannotEstablishCompleteness)?,
    )
    .map_err(|_| ErrorCode::CannotEstablishCompleteness)?;
    let slot_version = raw
        .slot_version
        .ok_or(ErrorCode::CannotEstablishCompleteness)?;
    let memories = source_pairs(raw.memory_source_ids, raw.memory_source_hashes)?;
    let evidence = source_pairs(raw.evidence_source_ids, raw.evidence_source_hashes)?;
    if raw.memory_links_exact != Some(true) || raw.evidence_links_exact != Some(true) {
        return Err(ErrorCode::CannotEstablishCompleteness);
    }
    if slot_version == 0 {
        if raw.slot_state.is_some()
            || raw.current_version_id.is_some()
            || raw.facet_version.is_some()
            || raw.body_text.is_some()
            || raw.stored_body_sha256.is_some()
            || raw.body_hash_closed.is_some()
            || raw.selected_is_latest != Some(true)
            || !memories.is_empty()
            || !evidence.is_empty()
        {
            return Err(ErrorCode::CannotEstablishCompleteness);
        }
        return Ok(SourceFacetInput {
            kind,
            state: SourceFacetState::Missing,
        });
    }
    if slot_version < 0
        || raw.facet_version != Some(slot_version)
        || raw.body_hash_closed != Some(true)
        || raw.selected_is_latest != Some(true)
        || memories.len().saturating_add(evidence.len()) == 0
    {
        return Err(ErrorCode::CannotEstablishCompleteness);
    }
    let version_id = raw
        .current_version_id
        .filter(|value| !value.is_nil() && value.get_version_num() == 7)
        .ok_or(ErrorCode::CannotEstablishCompleteness)?;
    let body_text = raw
        .body_text
        .ok_or(ErrorCode::CannotEstablishCompleteness)?;
    let body_sha256 = digest(raw.stored_body_sha256)?;
    let state = match raw.slot_state.as_deref() {
        Some("CONFLICTED") => SourceFacetState::Conflicted,
        Some("STALE") => SourceFacetState::Stale(DiagnosticCode::StoredStale),
        Some("CURRENT") => {
            if W2_FORCED_UNAVAILABLE.contains(&kind) {
                SourceFacetState::Stale(DiagnosticCode::AuthoritativeSourceUnavailable)
            } else if !validate_memories(tx, authorization, &memories).await? {
                SourceFacetState::Stale(DiagnosticCode::SourceRevalidationFailed)
            } else {
                match validate_evidence(tx, authorization, &evidence).await? {
                    EvidenceValidation::VisibilityFailed => {
                        SourceFacetState::Stale(DiagnosticCode::VisibilityRevalidationFailed)
                    }
                    EvidenceValidation::SourceFailed => {
                        SourceFacetState::Stale(DiagnosticCode::SourceRevalidationFailed)
                    }
                    EvidenceValidation::Valid => SourceFacetState::Current(
                        VerifiedCurrentFacet::from_authoritative_jsonb_text(
                            version_id,
                            slot_version,
                            body_text,
                            body_sha256,
                            memories.iter().map(|item| item.0).collect(),
                            evidence.iter().map(|item| item.0).collect(),
                        )?,
                    ),
                }
            }
        }
        _ => return Err(ErrorCode::CannotEstablishCompleteness),
    };
    Ok(SourceFacetInput { kind, state })
}

#[async_trait::async_trait]
impl ContinuityReadPort for PostgresContinuityReadPort {
    async fn read_snapshot(
        &self,
        authorization: &AuthorizationScope,
        project_id: ProjectId,
        requested_workspace: Option<WorkspaceId>,
        budget: ContextBudget,
    ) -> Result<ClosedContinuitySnapshot, ErrorCode> {
        let mut tx = self
            .pool
            .pool()
            // dep: PostgreSQL(any) — opens a PostgreSQL transaction
            .begin()
            .await
            .map_err(|_| ErrorCode::DependencyUnavailable)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(|_| ErrorCode::DependencyUnavailable)?;
        install_lookup_context(&mut tx, authorization).await?;
        let raw = read_raw_slots(&mut tx, authorization, project_id, requested_workspace).await?;
        if raw.is_empty() {
            return Err(ErrorCode::NotFound);
        }
        if raw.len() != ContinuityFacetKind::ALL.len() {
            return Err(ErrorCode::CannotEstablishCompleteness);
        }
        let first = raw.first().ok_or(ErrorCode::CannotEstablishCompleteness)?;
        let storage_snapshot_seq = first
            .storage_snapshot_seq
            .filter(|value| *value >= 0)
            .ok_or(ErrorCode::CannotEstablishCompleteness)?;
        let storage_snapshot_token = first
            .storage_snapshot_token
            .clone()
            .filter(|value| !value.is_empty())
            .ok_or(ErrorCode::CannotEstablishCompleteness)?;
        let workspace = WorkspaceId(
            first
                .workspace_id
                .filter(|value| !value.is_nil())
                .ok_or(ErrorCode::CannotEstablishCompleteness)?,
        );
        if first.project_id != Some(project_id.0)
            || first.tenant_id != Some(authorization.tenant_id().0)
            || requested_workspace.is_some_and(|requested| requested != workspace)
            || raw.iter().any(|row| {
                row.project_id != first.project_id
                    || row.tenant_id != first.tenant_id
                    || row.workspace_id != first.workspace_id
                    || row.storage_snapshot_seq != first.storage_snapshot_seq
                    || row.storage_snapshot_token != first.storage_snapshot_token
            })
        {
            return Err(ErrorCode::CannotEstablishCompleteness);
        }
        let narrowed = authorization
            .narrow(workspace)
            .map_err(|_| ErrorCode::CannotEstablishCompleteness)?;
        install_workspace_context(&mut tx, workspace).await?;
        let mut facets = HashMap::with_capacity(ContinuityFacetKind::ALL.len());
        for row in raw {
            let facet = close_slot(&mut tx, &narrowed, row).await?;
            if facets.insert(facet.kind, facet).is_some() {
                return Err(ErrorCode::CannotEstablishCompleteness);
            }
        }
        let facets: Vec<SourceFacetInput> = ContinuityFacetKind::ALL
            .into_iter()
            .map(|kind| {
                facets
                    .remove(&kind)
                    .ok_or(ErrorCode::CannotEstablishCompleteness)
            })
            .collect::<Result<_, _>>()?;
        let facets: [SourceFacetInput; 15] = facets
            .try_into()
            .map_err(|_| ErrorCode::CannotEstablishCompleteness)?;
        let scope = Scope {
            tenant_id: narrowed.tenant_id(),
            user_id: narrowed.user_id(),
            workspace_id: Some(workspace),
            repository_id: None,
            task_id: None,
            run_id: None,
            agent_id: None,
        };
        let frozen = fetch_frozen_in_txn(&mut tx, &narrowed, &scope).await?;
        let handoff = assemble(frozen, budget);
        let storage_token_sha256: [u8; 32] =
            Sha256::digest(storage_snapshot_token.as_bytes()).into();
        if handoff.context_snapshot_seq != storage_snapshot_seq
            || handoff.snapshot_token_sha256 != hex::encode(storage_token_sha256)
        {
            return Err(ErrorCode::CannotEstablishCompleteness);
        }
        tx.commit()
            .await
            .map_err(|_| ErrorCode::DependencyUnavailable)?;
        Ok(ClosedContinuitySnapshot {
            project_id,
            facets,
            handoff,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::source_pairs;
    use humaux_domain::error::ErrorCode;
    use uuid::Uuid;

    fn completeness<T: std::fmt::Debug>(result: Result<T, ErrorCode>) {
        assert_eq!(result.unwrap_err(), ErrorCode::CannotEstablishCompleteness);
    }

    #[test]
    fn source_pairs_reject_null_length_order_and_digest_corruption() {
        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);
        let hash = || Some(vec![0x11; 32]);

        completeness(source_pairs(None, Some(Vec::new())));
        completeness(source_pairs(Some(Vec::new()), None));
        completeness(source_pairs(Some(vec![Some(first)]), Some(Vec::new())));
        completeness(source_pairs(Some(vec![None]), Some(vec![hash()])));
        completeness(source_pairs(
            Some(vec![Some(Uuid::nil())]),
            Some(vec![hash()]),
        ));
        completeness(source_pairs(Some(vec![Some(first)]), Some(vec![None])));
        completeness(source_pairs(
            Some(vec![Some(first)]),
            Some(vec![Some(vec![0x11; 31])]),
        ));
        completeness(source_pairs(
            Some(vec![Some(second), Some(first)]),
            Some(vec![hash(), hash()]),
        ));
        completeness(source_pairs(
            Some(vec![Some(first), Some(first)]),
            Some(vec![hash(), hash()]),
        ));

        assert_eq!(
            source_pairs(
                Some(vec![Some(first), Some(second)]),
                Some(vec![hash(), hash()]),
            )
            .unwrap()
            .len(),
            2
        );
    }
}
