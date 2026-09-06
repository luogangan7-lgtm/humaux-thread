//! Final PostgreSQL body materialization after authorization and token-ledger validation.

use std::collections::{HashMap, HashSet};

use humaux_application::affect::memories_matching;
use humaux_domain::affect::AffectFilter;
use humaux_domain::authority::MemoryId;
use humaux_domain::error::ErrorCode;
use humaux_domain::identity::{AuthorizationScope, can_read};
use humaux_domain::ids::WorkspaceId;
use humaux_domain::subject::SubjectId;
use humaux_projection::serving::StreamFamily;
use humaux_projection::stream::StreamKey;
use sha2::{Digest, Sha256};
use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use crate::affect_repo;
use crate::context_repo::{readable_memory_ids, set_authorization_local, visibility_from_row};
use crate::postgres::RuntimeDbPool;
use crate::retrieve::{OverlayCandidate, ProcessingState};

type Txn<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializationSnapshot {
    pub context_snapshot_seq: i64,
    pub snapshot_token_sha256: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MaterializedItem {
    Memory {
        memory_id: Uuid,
        content: serde_json::Value,
    },
    TemporaryEvidence {
        evidence_id: Uuid,
        stream_seq: i64,
        processing_state: ProcessingState,
        payload: serde_json::Value,
        linked_memory_ids: Vec<Uuid>,
    },
    ArtifactUnavailable {
        evidence_id: Uuid,
        stream_seq: i64,
        processing_state: ProcessingState,
        linked_memory_ids: Vec<Uuid>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedBodies {
    pub snapshot: MaterializationSnapshot,
    pub items: Vec<MaterializedItem>,
}

fn dedup<T: std::hash::Hash + Eq + Copy>(items: impl IntoIterator<Item = T>) -> Vec<T> {
    let mut seen = HashSet::new();
    items
        .into_iter()
        .filter(|item| seen.insert(*item))
        .collect()
}

fn effective_authorization(
    authorization: &AuthorizationScope,
    expected_family: &StreamFamily,
    key: &StreamKey,
) -> Result<AuthorizationScope, ErrorCode> {
    if key.tenant_id != authorization.tenant_id()
        || expected_family.tenant_id != authorization.tenant_id()
        || key.tenant_id != expected_family.tenant_id
        || key.scope_kind != expected_family.scope_kind
        || key.scope_id != expected_family.scope_id
        || key.domain != expected_family.domain
        || key.projection_kind != expected_family.projection_kind
    {
        return Err(ErrorCode::Forbidden);
    }
    match key.scope_kind.as_str() {
        "tenant" if key.scope_id == authorization.tenant_id().0 => Ok(authorization.clone()),
        "workspace" => authorization.narrow(WorkspaceId(key.scope_id)),
        _ => Err(ErrorCode::Forbidden),
    }
}

/// Returns only Memory ids that may enter a final materialization in this transaction.
///
/// This is the shared final eligibility gate: `can_read`, lifecycle, secret backing sources,
/// and tombstoned backing Evidence. It deliberately returns ids rather than content so callers
/// creating a snapshot manifest do not load and discard every Memory body.
pub(crate) async fn final_memory_ids_in_txn(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    candidates: &[Uuid],
    include_archived: bool,
) -> Result<Vec<Uuid>, ErrorCode> {
    final_memory_ids_about_in_txn(txn, authorization, candidates, include_archived, &[], None).await
}

/// [`final_memory_ids_in_txn`] plus the §6.1.3 / ADR-0029 D-A subject re-check: when
/// `subject_ids` is non-empty, only memories linked (`private.memory_subjects`, read under the
/// caller's RLS) to at least one of them survive. Qdrant's `subject_ids` payload prefilter is
/// never trusted on its own — this is the authoritative membership test, in the same shared
/// gate `include_archived` lives in. Empty `subject_ids` = no subject narrowing.
///
/// §8.5.1 / ADR-0030 D-D: `affect` is the explicit affect query, re-checked here per annotation
/// on the read-time effective intensity (Qdrant's flat-array prefilter is an over-approximation
/// across annotations and cannot compute decay). ONE extra round trip for the whole candidate
/// set (`affect_repo::AFFECTS_FOR_MEMORIES_SQL`), never a per-row query. `None` = unscoped.
pub(crate) async fn final_memory_ids_about_in_txn(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    candidates: &[Uuid],
    include_archived: bool,
    subject_ids: &[SubjectId],
    affect: Option<&AffectFilter>,
) -> Result<Vec<Uuid>, ErrorCode> {
    let readable = readable_memory_ids(txn, authorization, candidates).await?;
    let subject_filter: Option<Vec<Uuid>> =
        (!subject_ids.is_empty()).then(|| subject_ids.iter().map(|s| s.0).collect());
    let rows = sqlx::query(
        r#"
        SELECT m.memory_id
        FROM private.memory_records AS m
        WHERE m.tenant_id = $1
          AND m.memory_id = ANY($2)
          AND m.status = 'active'
          AND m.superseded_by IS NULL
          -- Q3/ADR-0024 D-C: recall/context/enumerate exclude archived rows here (the shared
          -- final-eligibility gate); only memory.get passes include_archived=true.
          AND (m.archived_at IS NULL OR $3::boolean)
          -- §6.1.3/ADR-0029 D-A: subject any-of re-check (NULL = not subject-scoped).
          AND ($4::uuid[] IS NULL OR EXISTS (
              SELECT 1
              FROM private.memory_subjects AS ms
              WHERE ms.tenant_id = m.tenant_id
                AND ms.memory_id = m.memory_id
                AND ms.subject_id = ANY($4::uuid[])
          ))
          AND NOT EXISTS (
              SELECT 1
              FROM private.memory_evidence AS me
              JOIN private.evidence_objects AS source
                ON source.evidence_id = me.evidence_id
               AND source.tenant_id = m.tenant_id
              WHERE me.memory_id = m.memory_id
                AND source.data_class = 'SECRET_MATERIAL'
          )
          AND NOT EXISTS (
              SELECT 1
              FROM private.memory_evidence AS me
              JOIN ops.outbox AS ob
                ON ob.tenant_id = m.tenant_id
               AND ob.evidence_id = me.evidence_id
              JOIN projection.stream_log AS sl
                ON sl.tenant_id = ob.tenant_id
               AND sl.commit_seq = ob.commit_seq
              WHERE me.memory_id = m.memory_id
                AND sl.state = 'TOMBSTONED'
          )
        "#,
    )
    .bind(authorization.tenant_id().0)
    .bind(candidates)
    .bind(include_archived)
    .bind(subject_filter)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;

    let eligible = rows
        .into_iter()
        .map(|row| row.try_get("memory_id").map_err(|_| ErrorCode::Internal))
        .collect::<Result<HashSet<Uuid>, ErrorCode>>()?;
    let mut survivors: Vec<Uuid> = candidates
        .iter()
        .filter(|id| readable.contains(id) && eligible.contains(id))
        .copied()
        .collect();
    if let Some(filter) = affect {
        let rows =
            affect_repo::affects_for_memories_in_txn(txn, authorization.tenant_id().0, &survivors)
                .await
                .map_err(|_| ErrorCode::DependencyUnavailable)?;
        let matching = memories_matching(
            filter,
            &affect_repo::observed(&rows, OffsetDateTime::now_utc()),
        );
        survivors.retain(|id| matching.contains(id));
    }
    Ok(survivors)
}

async fn load_memories(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    candidates: &[Uuid],
    include_archived: bool,
    subject_ids: &[SubjectId],
    affect: Option<&AffectFilter>,
) -> Result<Vec<(Uuid, serde_json::Value)>, ErrorCode> {
    let eligible = final_memory_ids_about_in_txn(
        txn,
        authorization,
        candidates,
        include_archived,
        subject_ids,
        affect,
    )
    .await?;
    let rows = sqlx::query(
        r#"
        SELECT m.memory_id, m.content
        FROM private.memory_records AS m
        WHERE m.tenant_id = $1
          AND m.memory_id = ANY($2)
        "#,
    )
    .bind(authorization.tenant_id().0)
    .bind(&eligible)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;

    let mut values = HashMap::with_capacity(rows.len());
    for row in rows {
        let memory_id: Uuid = row.try_get("memory_id").map_err(|_| ErrorCode::Internal)?;
        values.insert(
            memory_id,
            row.try_get("content").map_err(|_| ErrorCode::Internal)?,
        );
    }
    eligible
        .into_iter()
        .map(|id| {
            values
                .remove(&id)
                .map(|content| (id, content))
                .ok_or(ErrorCode::Internal)
        })
        .collect::<Result<_, _>>()
}

struct RecheckedOverlay {
    evidence_id: Uuid,
    stream_seq: i64,
    processing_state: ProcessingState,
    evidence_kind: String,
    payload: Option<serde_json::Value>,
    linked_memory_ids: Vec<Uuid>,
}

/// Re-checks the RYW overlay under the caller's RLS snapshot. `subject_ids` (§6.1.3 / ADR-0029
/// D-A) is the same any-of scope the memory gate applies: when non-empty, an overlay Evidence
/// rides in only if `private.evidence_subjects` declares it about one of those subjects —
/// otherwise a subject-scoped recall carrying a consistency_token would return the raw body of
/// a just-written Evidence about someone else as `temporary_evidence`. Applied here, in the one
/// overlay loader, so every caller of the shared gate inherits it.
async fn load_overlay(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    key: &StreamKey,
    input: &[OverlayCandidate],
    subject_ids: &[SubjectId],
) -> Result<Vec<RecheckedOverlay>, ErrorCode> {
    let requested = dedup(input.iter().map(|item| (item.stream_seq, item.evidence_id)));
    if requested.is_empty() {
        return Ok(Vec::new());
    }
    let subject_filter: Option<Vec<Uuid>> =
        (!subject_ids.is_empty()).then(|| subject_ids.iter().map(|s| s.0).collect());
    let seqs = requested.iter().map(|(seq, _)| *seq).collect::<Vec<_>>();
    let evidence_ids = requested.iter().map(|(_, id)| *id).collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        WITH requested(stream_seq, evidence_id) AS (
            SELECT * FROM UNNEST($7::bigint[], $8::uuid[])
        )
        SELECT sl.stream_seq,
               sl.state,
               ob.evidence_id,
               eo.evidence_kind,
               eo.visibility_class,
               eo.visibility_user_id,
               eo.visibility_workspace_id,
               event.payload
        FROM projection.stream_log AS sl
        JOIN ops.outbox AS ob
          ON ob.tenant_id = sl.tenant_id
         AND ob.commit_seq = sl.commit_seq
        JOIN requested AS requested
          ON requested.stream_seq = sl.stream_seq
         AND requested.evidence_id = ob.evidence_id
        JOIN private.evidence_objects AS eo
          ON eo.evidence_id = ob.evidence_id
         AND eo.tenant_id = sl.tenant_id
        LEFT JOIN private.events AS event ON event.event_id = eo.evidence_id
        WHERE sl.tenant_id = $1
          AND sl.scope_kind = $2
          AND sl.scope_id = $3
          AND sl.domain = $4
          AND sl.projection_kind = $5
          AND sl.projection_version = $6
          AND sl.state <> 'TOMBSTONED'
          AND eo.data_class <> 'SECRET_MATERIAL'
          -- §6.1.3/ADR-0029 D-A: subject any-of re-check for the overlay (NULL = not scoped).
          AND ($9::uuid[] IS NULL OR EXISTS (
              SELECT 1
              FROM private.evidence_subjects AS es
              WHERE es.tenant_id = eo.tenant_id
                AND es.evidence_id = eo.evidence_id
                AND es.subject_id = ANY($9::uuid[])
          ))
        ORDER BY sl.stream_seq, ob.evidence_id
        "#,
    )
    .bind(key.tenant_id.0)
    .bind(&key.scope_kind)
    .bind(key.scope_id)
    .bind(&key.domain)
    .bind(&key.projection_kind)
    .bind(&key.projection_version)
    .bind(seqs)
    .bind(evidence_ids)
    .bind(subject_filter)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let visibility = visibility_from_row(&row)?;
        if !can_read(authorization, &visibility) {
            continue;
        }
        let state: String = row.try_get("state").map_err(|_| ErrorCode::Internal)?;
        out.push(RecheckedOverlay {
            evidence_id: row
                .try_get("evidence_id")
                .map_err(|_| ErrorCode::Internal)?,
            stream_seq: row.try_get("stream_seq").map_err(|_| ErrorCode::Internal)?,
            processing_state: ProcessingState::parse(&state).map_err(|_| ErrorCode::Internal)?,
            evidence_kind: row
                .try_get("evidence_kind")
                .map_err(|_| ErrorCode::Internal)?,
            payload: row.try_get("payload").map_err(|_| ErrorCode::Internal)?,
            linked_memory_ids: Vec::new(),
        });
    }
    let evidence_ids = out.iter().map(|item| item.evidence_id).collect::<Vec<_>>();
    let mut links = load_links(txn, authorization, &evidence_ids).await?;
    for item in &mut out {
        item.linked_memory_ids = links.remove(&item.evidence_id).unwrap_or_default();
    }
    Ok(out)
}

async fn load_links(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
    evidence_ids: &[Uuid],
) -> Result<HashMap<Uuid, Vec<Uuid>>, ErrorCode> {
    let rows = sqlx::query(
        r#"
        SELECT me.evidence_id, me.memory_id
        FROM private.memory_evidence AS me
        JOIN private.memory_records AS m
          ON m.memory_id = me.memory_id
         AND m.tenant_id = $1
         AND m.status = 'active'
         AND m.superseded_by IS NULL
        WHERE me.evidence_id = ANY($2)
        ORDER BY me.evidence_id, me.memory_id
        "#,
    )
    .bind(authorization.tenant_id().0)
    .bind(evidence_ids)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let memory_ids = dedup(
        rows.iter()
            .map(|row| row.try_get("memory_id").map_err(|_| ErrorCode::Internal))
            .collect::<Result<Vec<Uuid>, ErrorCode>>()?,
    );
    let readable = readable_memory_ids(txn, authorization, &memory_ids).await?;
    let mut result = HashMap::<Uuid, Vec<Uuid>>::new();
    for row in rows {
        let memory_id = row.try_get("memory_id").map_err(|_| ErrorCode::Internal)?;
        if readable.contains(&memory_id) {
            let evidence_id = row
                .try_get("evidence_id")
                .map_err(|_| ErrorCode::Internal)?;
            result.entry(evidence_id).or_default().push(memory_id);
        }
    }
    for ids in result.values_mut() {
        *ids = dedup(ids.iter().copied());
    }
    Ok(result)
}

async fn snapshot(txn: &mut Txn<'_>) -> Result<MaterializationSnapshot, ErrorCode> {
    let row = sqlx::query(
        r#"
        SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint AS seq,
               pg_current_snapshot()::text AS token
        "#,
    )
    .fetch_one(&mut **txn)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let token: String = row.try_get("token").map_err(|_| ErrorCode::Internal)?;
    Ok(MaterializationSnapshot {
        context_snapshot_seq: row.try_get("seq").map_err(|_| ErrorCode::Internal)?,
        snapshot_token_sha256: Sha256::digest(token.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    })
}

pub async fn materialize_final_bodies(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
    expected_family: &StreamFamily,
    validated_key: &StreamKey,
    memory_ids: &[Uuid],
    overlay: &[OverlayCandidate],
) -> Result<MaterializedBodies, ErrorCode> {
    let _ = effective_authorization(authorization, expected_family, validated_key)?;
    let mut txn = pool
        .pool()
        .begin()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *txn)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let bodies = materialize_final_bodies_in_txn(
        &mut txn,
        authorization,
        expected_family,
        validated_key,
        memory_ids,
        overlay,
        false,
    )
    .await?;
    txn.commit()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(bodies)
}

/// Materializes one trusted Memory id through a caller-owned repeatable-read transaction.
///
/// This keeps the final body policy in [`materialize_final_bodies_in_txn`]: tenant and
/// visibility gates, active/supersession lifecycle, secret backing-source exclusion, and
/// tombstoned Evidence exclusion all stay in that single implementation. An absent final
/// Memory row intentionally maps to the object-level `NOT_FOUND` result.
pub(crate) async fn materialize_one_memory_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    authorization: &AuthorizationScope,
    expected_family: &StreamFamily,
    validated_key: &StreamKey,
    memory_id: MemoryId,
) -> Result<MaterializedBodies, ErrorCode> {
    // Q3/ADR-0024 D-C: memory.get is the one read that must still return an archived row.
    let bodies = materialize_final_bodies_in_txn(
        txn,
        authorization,
        expected_family,
        validated_key,
        &[memory_id.0],
        &[],
        true,
    )
    .await?;
    match bodies.items.as_slice() {
        [] => return Err(ErrorCode::NotFound),
        [
            MaterializedItem::Memory {
                memory_id: actual, ..
            },
        ] if *actual == memory_id.0 => {}
        _ => return Err(ErrorCode::Internal),
    }
    Ok(bodies)
}

/// Materializes final bodies through a caller-owned repeatable-read transaction.
///
/// The trusted StreamFamily and complete StreamKey are revalidated before any
/// query. Callers establish repeatable-read before their first query; authorized manifest
/// creation may use read-write mode without changing the final body policy.
pub(crate) async fn materialize_final_bodies_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    authorization: &AuthorizationScope,
    expected_family: &StreamFamily,
    validated_key: &StreamKey,
    memory_ids: &[Uuid],
    overlay: &[OverlayCandidate],
    include_archived: bool,
) -> Result<MaterializedBodies, ErrorCode> {
    materialize_final_bodies_about_in_txn(
        txn,
        authorization,
        expected_family,
        validated_key,
        memory_ids,
        overlay,
        include_archived,
        &[],
        None,
    )
    .await
}

/// [`materialize_final_bodies_in_txn`] with the §6.1.3 / ADR-0029 D-A subject any-of re-check
/// applied at the shared final-eligibility gate — to the candidate memories, to the memories an
/// overlay item links, and to the overlay Evidence items themselves (via
/// `private.evidence_subjects` in [`load_overlay`]): a not-yet-projected write about someone
/// else never rides in on the RYW overlay, neither as a linked memory nor as raw
/// `temporary_evidence`.
///
/// §8.5.1 / ADR-0030 D-D: with an `affect` filter the overlay's raw Evidence items are dropped
/// too — an Evidence has no affect rows (affects hang on memories), so it can never satisfy the
/// filter; the memories it links still enter the candidate set and pass the same gate.
#[allow(clippy::too_many_arguments)] // One gate, two more axes (subject, affect) next to `include_archived`.
pub(crate) async fn materialize_final_bodies_about_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    authorization: &AuthorizationScope,
    expected_family: &StreamFamily,
    validated_key: &StreamKey,
    memory_ids: &[Uuid],
    overlay: &[OverlayCandidate],
    include_archived: bool,
    subject_ids: &[SubjectId],
    affect: Option<&AffectFilter>,
) -> Result<MaterializedBodies, ErrorCode> {
    let authorization = effective_authorization(authorization, expected_family, validated_key)?;
    set_authorization_local(txn, &authorization).await?;
    let mut overlay =
        load_overlay(txn, &authorization, validated_key, overlay, subject_ids).await?;
    let candidate_ids = dedup(
        memory_ids.iter().copied().chain(
            overlay
                .iter()
                .flat_map(|item| item.linked_memory_ids.iter().copied()),
        ),
    );
    let memories = load_memories(
        txn,
        &authorization,
        &candidate_ids,
        include_archived,
        subject_ids,
        affect,
    )
    .await?;
    if affect.is_some() {
        overlay.clear();
    }
    let readable_memory_ids = memories.iter().map(|(id, _)| *id).collect::<HashSet<_>>();
    for item in &mut overlay {
        item.linked_memory_ids
            .retain(|id| readable_memory_ids.contains(id));
    }
    let mut items = memories
        .into_iter()
        .map(|(memory_id, content)| MaterializedItem::Memory { memory_id, content })
        .collect::<Vec<_>>();
    for item in overlay {
        let RecheckedOverlay {
            evidence_id,
            stream_seq,
            processing_state,
            evidence_kind,
            payload,
            linked_memory_ids,
        } = item;
        match (evidence_kind.as_str(), payload) {
            ("EVENT", Some(payload)) => items.push(MaterializedItem::TemporaryEvidence {
                evidence_id,
                stream_seq,
                processing_state,
                payload,
                linked_memory_ids,
            }),
            ("EVENT", None) => return Err(ErrorCode::Internal),
            (_, _) => items.push(MaterializedItem::ArtifactUnavailable {
                evidence_id,
                stream_seq,
                processing_state,
                linked_memory_ids,
            }),
        }
    }
    let snapshot = snapshot(txn).await?;
    Ok(MaterializedBodies { snapshot, items })
}
