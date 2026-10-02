//! `adapters::affect_repo` — SQL half of the §8.5.1 affect annotation axis (ADR-0030, card E1): the ONE write path
//!   onto `private.memory_affects` ([`annotate`]) and the ONE set-based read ([`AFFECTS_FOR_MEMORIES_SQL`] /
//!   [`affects_for_memories_in_txn`]) every reader shares — the PG hydrate re-check (`read_materialize`), the
//!   projection worker's payload build, and `memory.get` / `memory.enumerate` / the recall rerank.
//! Depends-on: crates=[humaux-application, humaux-domain, humaux-projection, serde_json, sqlx, time]; services=[PostgreSQL(any) r=[private.memory_evidence, private.memory_records] w=[private.evidence_affects, private.memory_affects]]; env=[]; modules=[adapters::confirm_token_repo, adapters::memory_governance_repo, adapters::postgres, adapters::remember, adapters::subject_repo, application::affect, domain::affect, domain::authority, domain::error, domain::identity, domain::subject, projection::stream]
//! Called-by: [adapters::distill_reasoner, adapters::memory_governance_repo, adapters::projection_worker, adapters::read_materialize, adapters::remember, gateway::mcp_application, gateway::memory, gateway::recall, gateway::remember, private-worker::distill, tests]
//! Invariants: [affect rows are INSERT-only through the one row issuer insert_rows (declared rows via insert_in_txn,
//!   inferred DISTILL rows via insert_inferred_in_txn) and never decayed on write; the one read lets an EXPLICIT row
//!   shadow every DISTILL row of its memory; everything runs under
//!   the caller's role + RLS, so another tenant's memory is NOT_FOUND like an unknown id; a PG error propagates as
//!   ErrorCode, no fallback; the annotate re-projection ticket lands on the memory's home stream (ADR-0057 D-M)]
//! Spec: Baseline §60; ADR-0057; ADR-0058
//!
//! Rows are immutable (0156 owner trigger; no UPDATE/DELETE grant): a write is INSERT-only through
//! the ONE row issuer `insert_rows` — declared rows through [`insert_in_txn`]
//! (`memory.annotate_affect` on its own transaction, `memory.correct` inside `correct_atomically`,
//! `remember.put` inside `remember_in_txn` onto the 0157 `evidence_affects` carrier), inferred rows
//! through [`insert_inferred_in_txn`] (the distill hop's write transaction, ADR-0058 D-P; that
//! transaction's outbox DONE flip is the projection ticket) — and, for an annotated live memory, ends with one `MEMORY_LIFECYCLE` ticket on the memory's home stream (ADR-0057 D-M; the §60 issuers `remember`
//! owns, verbatim — the same ticket `memory.supersede`/`restore`/`archive` issue) so the worker
//! re-projects the SAME deterministic point with the fresh affect payload. Decay is never
//! written: [`observed`] derives `effective_intensity(now)` from the raw row at read time.
//!
//! Every statement runs under the caller's role + RLS: another tenant's memory is not there, so
//! annotating it fails exactly like an unknown id (`NOT_FOUND`); an unknown `target_subject`
//! (id or key) is `INVALID_INPUT` through `subject_repo`'s own rule-1/2 resolver — never an
//! auto-registration (mirrors card 8).

use humaux_application::affect::{ObservedAffect, effective_intensity_at};
use humaux_domain::affect::{
    AffectAnnotation, AffectFilter, AffectKind, AffectOrigin, AffectTargetScope,
    AffectTargetScopeKind, BasisPointRange, BasisPoints, EmotionLabel,
    INFERRED_CONFIDENCE_CEILING_BP, MoodHalfLife, MoodPoint,
};
use humaux_domain::authority::MemoryId;
use humaux_domain::error::ErrorCode;
use humaux_domain::identity::AuthorizationScope;
use humaux_domain::subject::{SubjectDeclaration, SubjectId, SubjectKey, SubjectKeyKind};
use humaux_projection::stream::StreamKey;
use serde_json::Value;
use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use crate::confirm_token_repo;
use crate::postgres::RuntimeDbPool;
use crate::remember::{self, RememberError};
use crate::subject_repo;

type Txn<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

/// One stored `private.memory_affects` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffectRow {
    pub affect_id: Uuid,
    pub memory_id: Uuid,
    pub evidence_id: Uuid,
    pub annotation: AffectAnnotation,
    pub observed_at: OffsetDateTime,
    /// The write-time policy captured on a MOOD row; `None` for an EMOTION.
    pub half_life: Option<MoodHalfLife>,
}

/// One annotation to append (`memory.annotate_affect.affects[]` / `memory.correct.affects[]`).
/// `target_subject_key` is an exact trusted key resolved under RLS (rule 2); the annotation's own
/// `target_subject` is an explicit id (rule 1). At most one of the two.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffectInput {
    pub annotation: AffectAnnotation,
    /// Absent ⇒ `now()` at write time.
    pub observed_at: Option<OffsetDateTime>,
    pub target_subject_key: Option<SubjectKey>,
}

/// Result of [`annotate`]: the new immutable rows and the re-projection ticket they issued.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotateDone {
    pub memory_id: Uuid,
    pub affect_ids: Vec<Uuid>,
    pub stream_seq: i64,
    pub commit_seq: i64,
}

fn db_error(error: sqlx::Error) -> ErrorCode {
    match error {
        sqlx::Error::RowNotFound => ErrorCode::NotFound,
        sqlx::Error::Database(ref db) => match db.code().as_deref() {
            Some("42501") => ErrorCode::Forbidden,
            Some("23503") => ErrorCode::TenantBoundary,
            // 55000: the ADR-0053 PROVISIONING write gate — reachable since ADR-0054 lands the
            // annotate ticket on the request's own pair (a definite, rolled-back refusal).
            Some("23505" | "40001" | "40P01" | "55P03" | "23514" | "55000") => ErrorCode::Conflict,
            Some("22023" | "22P02" | "22003") => ErrorCode::InvalidInput,
            _ => ErrorCode::Internal,
        },
        _ => ErrorCode::DependencyUnavailable,
    }
}

fn remember_error(error: RememberError) -> ErrorCode {
    match error {
        RememberError::Db(error) => db_error(error),
        RememberError::ConsistencyTokenExpiryNotFuture | RememberError::BatchExhausted => {
            ErrorCode::Conflict
        }
        RememberError::Subject(code) | RememberError::Affect(code) => code,
        RememberError::ReasoningDomainUnresolved => ErrorCode::DependencyUnavailable,
    }
}

/// The ONE read statement: every affect row of every memory in `$2`, one round trip whatever the
/// candidate count (the card E1 speed goal; `tests/memory_affects.rs` pins it to one table scan).
/// Ordered so a memory's annotations come back in write order.
///
/// ADR-0058 D-P (explicit remains authoritative): a memory's `DISTILL` (inferred) rows are dropped
/// when that memory has any `EXPLICIT` row — decided by a window over the SAME single scan, so the
/// recall filter, the mood rerank, `memory.get` and the projection payload all see one answer.
pub const AFFECTS_FOR_MEMORIES_SQL: &str = "SELECT affect_id, memory_id, evidence_id, affect_kind, label, valence_bp, arousal_bp, \
            dominance_bp, intensity_bp, confidence_bp, target_subject_id, target_scope_kind, \
            target_scope_id, observed_at, half_life_seconds \
     FROM (SELECT affect_id, memory_id, evidence_id, affect_kind, label, valence_bp, arousal_bp, \
                  dominance_bp, intensity_bp, confidence_bp, target_subject_id, target_scope_kind, \
                  target_scope_id, observed_at, half_life_seconds, created_at, origin, \
                  bool_or(origin = 'EXPLICIT') OVER (PARTITION BY memory_id) AS has_explicit \
           FROM private.memory_affects \
           WHERE tenant_id = $1 AND memory_id = ANY($2)) a \
     WHERE a.origin = 'EXPLICIT' OR NOT a.has_explicit \
     ORDER BY memory_id, created_at, affect_id";

fn decode_error(what: &str) -> sqlx::Error {
    sqlx::Error::Decode(format!("memory_affects.{what}: value outside the closed set").into())
}

fn row_to_affect(row: &sqlx::postgres::PgRow) -> Result<AffectRow, sqlx::Error> {
    let kind: String = row.try_get("affect_kind")?;
    let label: Option<String> = row.try_get("label")?;
    let scope_kind: Option<String> = row.try_get("target_scope_kind")?;
    let scope_id: Option<Uuid> = row.try_get("target_scope_id")?;
    let bp_signed = |name: &str| -> Result<Option<BasisPoints>, sqlx::Error> {
        row.try_get::<Option<i16>, _>(name)?
            .map(|v| BasisPoints::signed(v).map_err(|_| decode_error(name)))
            .transpose()
    };
    let bp_unit = |name: &str| -> Result<BasisPoints, sqlx::Error> {
        BasisPoints::unit(row.try_get::<i16, _>(name)?).map_err(|_| decode_error(name))
    };
    let half_life_seconds: Option<i32> = row.try_get("half_life_seconds")?;
    Ok(AffectRow {
        affect_id: row.try_get("affect_id")?,
        memory_id: row.try_get("memory_id")?,
        evidence_id: row.try_get("evidence_id")?,
        annotation: AffectAnnotation {
            kind: AffectKind::parse(&kind).ok_or_else(|| decode_error("affect_kind"))?,
            label: label
                .map(|l| EmotionLabel::parse(&l).ok_or_else(|| decode_error("label")))
                .transpose()?,
            valence: bp_signed("valence_bp")?,
            arousal: bp_signed("arousal_bp")?,
            dominance: bp_signed("dominance_bp")?,
            intensity: bp_unit("intensity_bp")?,
            confidence: bp_unit("confidence_bp")?,
            target_subject: row
                .try_get::<Option<Uuid>, _>("target_subject_id")?
                .map(SubjectId),
            target_scope: match (scope_kind, scope_id) {
                (Some(kind), Some(id)) => Some(AffectTargetScope {
                    kind: AffectTargetScopeKind::parse(&kind)
                        .ok_or_else(|| decode_error("target_scope_kind"))?,
                    id,
                }),
                _ => None,
            },
        },
        observed_at: row.try_get("observed_at")?,
        half_life: half_life_seconds
            .map(|s| {
                u64::try_from(s)
                    .ok()
                    .and_then(|s| MoodHalfLife::new(std::time::Duration::from_secs(s)).ok())
                    .ok_or_else(|| decode_error("half_life_seconds"))
            })
            .transpose()?,
    })
}

/// [`AFFECTS_FOR_MEMORIES_SQL`] in the caller's transaction (its role + RLS: the gateway's
/// tenant snapshot, the retrieval worker's headless read arc). Empty input ⇒ no query.
pub async fn affects_for_memories_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    memory_ids: &[Uuid],
) -> Result<Vec<AffectRow>, sqlx::Error> {
    if memory_ids.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query(AFFECTS_FOR_MEMORIES_SQL)
        .bind(tenant_id)
        .bind(memory_ids)
        .fetch_all(&mut **txn)
        .await?
        .iter()
        .map(row_to_affect)
        .collect()
}

/// [`affects_for_memories_in_txn`] on its own read-only transaction under `auth` (memory.get /
/// memory.enumerate item enrichment, the recall mood rerank).
pub async fn affects_for_memories(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    memory_ids: &[Uuid],
) -> Result<Vec<AffectRow>, ErrorCode> {
    if memory_ids.is_empty() {
        return Ok(Vec::new());
    }
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    confirm_token_repo::set_authorization_local(&mut txn, auth).await?;
    let rows = affects_for_memories_in_txn(&mut txn, auth.tenant_id().0, memory_ids)
        .await
        .map_err(db_error)?;
    txn.rollback().await.map_err(db_error)?;
    Ok(rows)
}

/// Read-time derivation (ADR-0030 D-B): each row with its `effective_intensity(now)` — raw for an
/// EMOTION, `raw · 2^(−Δt/half_life)` for a MOOD. Never persisted.
pub fn observed(rows: &[AffectRow], now: OffsetDateTime) -> Vec<ObservedAffect> {
    rows.iter()
        .map(|row| ObservedAffect {
            memory_id: row.memory_id,
            annotation: row.annotation.clone(),
            effective_intensity: effective_intensity_at(
                row.annotation.intensity,
                row.observed_at.unix_timestamp(),
                now.unix_timestamp(),
                row.half_life,
            ),
        })
        .collect()
}

/// Which row the sole affect issuer writes: a memory's own annotation (`memory_affects`, the
/// `memory.annotate_affect` / `memory.correct` paths) or the write-side carrier declared at
/// `remember.put` (`evidence_affects`, copied onto every memory born from that Evidence by the
/// 0157 `memory_evidence` PRIMARY trigger).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AffectParent {
    Memory(Uuid),
    Evidence,
}

/// Rules 1/2 for every input's `target_subject` through the sole resolver, in the caller's
/// transaction and BEFORE any write: unknown ⇒ `INVALID_INPUT`, never auto-registered. Returns
/// the resolved id per input (same order), ready for [`insert_in_txn`].
pub async fn resolve_targets_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    inputs: &[AffectInput],
) -> Result<Vec<Option<Uuid>>, ErrorCode> {
    let mut targets = Vec::with_capacity(inputs.len());
    for input in inputs {
        let target = match (input.annotation.target_subject, &input.target_subject_key) {
            (None, None) => None,
            (Some(_), Some(_)) => return Err(ErrorCode::InvalidInput),
            (id, key) => {
                let declaration = SubjectDeclaration::new(
                    id.into_iter().collect(),
                    key.iter().cloned().collect(),
                )?;
                let resolved =
                    subject_repo::resolve_declaration_in_txn(txn, tenant_id, &declaration).await?;
                Some(resolved.first().ok_or(ErrorCode::InvalidInput)?.subject_id)
            }
        };
        targets.push(target);
    }
    Ok(targets)
}

/// The declared-affect entry onto `memory_affects` / `evidence_affects`: `targets` come from
/// [`resolve_targets_in_txn`] (same length, same order); `mood_half_life` is the frozen policy
/// stamped onto every MOOD row (`None` with a MOOD input ⇒ `DEPENDENCY_UNAVAILABLE`: the
/// deployment has not declared the policy). Rows are [`AffectOrigin::Explicit`]. INSERT-only;
/// nothing here commits.
pub async fn insert_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    parent: AffectParent,
    evidence_id: Uuid,
    inputs: &[AffectInput],
    targets: &[Option<Uuid>],
    mood_half_life: Option<MoodHalfLife>,
) -> Result<Vec<Uuid>, ErrorCode> {
    insert_rows(
        txn,
        tenant_id,
        parent,
        evidence_id,
        inputs,
        targets,
        mood_half_life,
        AffectOrigin::Explicit,
    )
    .await
}

/// ADR-0058 D-P: the distill hop's inferred affects of ONE memory it just inserted, in the hop's
/// write transaction. Only an untargeted `EMOTION` (event-bound, no half-life) whose confidence is
/// at most [`INFERRED_CONFIDENCE_CEILING_BP`] is accepted — anything else is `INVALID_INPUT`, never
/// clamped (the parser already refused it; the store CHECK refuses it a third time). Rows are
/// [`AffectOrigin::Distill`].
pub async fn insert_inferred_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    memory_id: Uuid,
    evidence_id: Uuid,
    inferred: &[AffectAnnotation],
) -> Result<Vec<Uuid>, ErrorCode> {
    let mut inputs = Vec::with_capacity(inferred.len());
    for annotation in inferred {
        if annotation.kind != AffectKind::Emotion
            || annotation.target_subject.is_some()
            || annotation.target_scope.is_some()
            || annotation.confidence.get() > INFERRED_CONFIDENCE_CEILING_BP
        {
            return Err(ErrorCode::InvalidInput);
        }
        inputs.push(AffectInput {
            annotation: annotation.clone(),
            observed_at: None,
            target_subject_key: None,
        });
    }
    insert_rows(
        txn,
        tenant_id,
        AffectParent::Memory(memory_id),
        evidence_id,
        &inputs,
        &vec![None; inputs.len()],
        None,
        AffectOrigin::Distill,
    )
    .await
}

/// The ONE row issuer onto `memory_affects` / `evidence_affects` behind [`insert_in_txn`] and
/// [`insert_inferred_in_txn`]. `evidence_affects` has no origin column: an Evidence-carried affect
/// is declared by construction, so a non-EXPLICIT row on that arm inserts nothing and fails.
#[allow(
    clippy::too_many_arguments,
    reason = "the row issuer's inputs are exactly the row's parents, its values and its origin"
)]
async fn insert_rows(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    parent: AffectParent,
    evidence_id: Uuid,
    inputs: &[AffectInput],
    targets: &[Option<Uuid>],
    mood_half_life: Option<MoodHalfLife>,
    origin: AffectOrigin,
) -> Result<Vec<Uuid>, ErrorCode> {
    if inputs.len() != targets.len() {
        return Err(ErrorCode::Internal);
    }
    let sql = match parent {
        AffectParent::Memory(_) => {
            "INSERT INTO private.memory_affects \
               (tenant_id, memory_id, affect_kind, label, valence_bp, arousal_bp, dominance_bp, \
                intensity_bp, confidence_bp, evidence_id, target_subject_id, target_scope_kind, \
                target_scope_id, observed_at, half_life_seconds, origin) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, coalesce($14, now()), $15, $16) \
             RETURNING affect_id"
        }
        AffectParent::Evidence => {
            "INSERT INTO private.evidence_affects \
               (tenant_id, affect_kind, label, valence_bp, arousal_bp, dominance_bp, \
                intensity_bp, confidence_bp, evidence_id, target_subject_id, target_scope_kind, \
                target_scope_id, observed_at, half_life_seconds) \
             SELECT $1, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, coalesce($14, now()), $15 \
             WHERE $2::uuid IS NULL AND $16::text = 'EXPLICIT' /* no memory yet: same 16 binds as the memory arm */ \
             RETURNING affect_id"
        }
    };
    let memory_id = match parent {
        AffectParent::Memory(id) => Some(id),
        AffectParent::Evidence => None,
    };
    let mut affect_ids = Vec::with_capacity(inputs.len());
    for (input, target_subject) in inputs.iter().zip(targets) {
        let a = &input.annotation;
        let half_life_seconds: Option<i32> = match a.kind {
            AffectKind::Emotion => None,
            AffectKind::Mood => Some(
                i32::try_from(
                    mood_half_life
                        .ok_or(ErrorCode::DependencyUnavailable)?
                        .duration()
                        .as_secs(),
                )
                .map_err(|_| ErrorCode::InvalidInput)?,
            ),
        };
        let affect_id: Uuid = sqlx::query_scalar(sql)
            .bind(tenant_id)
            .bind(memory_id)
            .bind(a.kind.as_str())
            .bind(a.label.map(EmotionLabel::as_str))
            .bind(a.valence.map(BasisPoints::get))
            .bind(a.arousal.map(BasisPoints::get))
            .bind(a.dominance.map(BasisPoints::get))
            .bind(a.intensity.get())
            .bind(a.confidence.get())
            .bind(evidence_id)
            .bind(*target_subject)
            .bind(a.target_scope.map(|s| s.kind.as_str()))
            .bind(a.target_scope.map(|s| s.id))
            .bind(input.observed_at)
            .bind(half_life_seconds)
            .bind(origin.as_str())
            .fetch_one(&mut **txn)
            .await
            .map_err(db_error)?;
        affect_ids.push(affect_id);
    }
    Ok(affect_ids)
}

/// `memory.annotate_affect` (ADR-0030 D-C) — appends `inputs` to the visible, active head memory
/// `memory_id` in one transaction and issues one `MEMORY_LIFECYCLE` re-projection ticket bound
/// to the memory's PRIMARY Evidence (also the rows' provenance `evidence_id`), on the memory's
/// home stream (`memory_governance_repo::home_stream`; `stream` is the request's, the fallback
/// for a never-projected memory and the source of tenant / domain / kind / version). Refusals, in
/// order and with nothing written: unknown / another tenant's / archived-but-invisible memory
/// ⇒ `NOT_FOUND`; a superseded or non-active version ⇒ `CONFLICT` (annotate the head); unknown
/// `target_subject` id/key ⇒ `INVALID_INPUT`. `mood_half_life` is the frozen policy stamped onto
/// every MOOD row.
pub async fn annotate(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    stream: &StreamKey,
    memory_id: MemoryId,
    inputs: &[AffectInput],
    mood_half_life: MoodHalfLife,
) -> Result<AnnotateDone, ErrorCode> {
    if inputs.is_empty() {
        return Err(ErrorCode::InvalidInput);
    }
    let tenant_id = auth.tenant_id().0;
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    // ADR-0054 D-B: the write-scope recheck (principal + its one workspace) inside this txn.
    confirm_token_repo::set_write_authorization_local(&mut txn, auth).await?;

    let head: Option<(String, Option<Uuid>)> = sqlx::query_as(
        "SELECT status, superseded_by FROM private.memory_records \
         WHERE tenant_id = $1 AND memory_id = $2",
    )
    .bind(tenant_id)
    .bind(memory_id.0)
    .fetch_optional(&mut *txn)
    .await
    .map_err(db_error)?;
    let (status, superseded_by) = head.ok_or(ErrorCode::NotFound)?;
    if status != "active" || superseded_by.is_some() {
        return Err(ErrorCode::Conflict);
    }
    let evidence_id: Uuid = sqlx::query_scalar(
        "SELECT evidence_id FROM private.memory_evidence WHERE memory_id = $1 \
         ORDER BY (role = 'PRIMARY') DESC, ordinal ASC LIMIT 1",
    )
    .bind(memory_id.0)
    .fetch_optional(&mut *txn)
    .await
    .map_err(db_error)?
    .ok_or(ErrorCode::NotFound)?;

    let targets = resolve_targets_in_txn(&mut txn, tenant_id, inputs).await?;
    let affect_ids = insert_in_txn(
        &mut txn,
        tenant_id,
        AffectParent::Memory(memory_id.0),
        evidence_id,
        inputs,
        &targets,
        Some(mood_half_life),
    )
    .await?;

    // Re-projection: the payload's affect fields are written only when the worker (re)projects
    // the row — same mechanism as 0155's back-fill and every governance write.
    // ADR-0057 D-M (W3): on the memory's home stream, not the request's — a W2-routed ticket
    // would write a duplicate point into W2's family that no later W1 retire ever reaches.
    let stream = crate::memory_governance_repo::home_stream(&mut txn, stream, evidence_id).await?;
    let commit_seq = remember::next_commit_seq(&mut txn)
        .await
        .map_err(remember_error)?;
    let stream_seq = remember::issue_stream_log_row(&mut txn, &stream, commit_seq)
        .await
        .map_err(remember_error)?;
    remember::insert_outbox(
        &mut txn,
        tenant_id,
        commit_seq,
        stream_seq,
        remember::MEMORY_LIFECYCLE,
        evidence_id,
    )
    .await
    .map_err(remember_error)?;
    txn.commit().await.map_err(db_error)?;
    Ok(AnnotateDone {
        memory_id: memory_id.0,
        affect_ids,
        stream_seq,
        commit_seq,
    })
}

// ---------------------------------------------------------------------------------------------
// Wire-side parsing, shared by memory.annotate_affect / memory.correct / recall.search so every
// op parses one way (subject_repo::parse_declaration's discipline).
// ---------------------------------------------------------------------------------------------

fn bp_field(
    value: &Value,
    field: &str,
    ctor: fn(i16) -> Result<BasisPoints, ErrorCode>,
) -> Result<Option<BasisPoints>, ErrorCode> {
    value
        .get(field)
        .filter(|v| !v.is_null())
        .map(|v| {
            v.as_i64()
                .and_then(|n| i16::try_from(n).ok())
                .ok_or(ErrorCode::InvalidInput)
                .and_then(ctor)
        })
        .transpose()
}

fn uuid_field(value: &Value, field: &str) -> Result<Option<Uuid>, ErrorCode> {
    value
        .get(field)
        .filter(|v| !v.is_null())
        .map(|v| {
            v.as_str()
                .and_then(|s| Uuid::parse_str(s).ok())
                .ok_or(ErrorCode::InvalidInput)
        })
        .transpose()
}

/// `affects: [{kind,label?,valence?,arousal?,dominance?,intensity,confidence,target_subject_id?|
/// target_subject_key?,target_scope?,observed_at?}]`. Absent ⇒ empty. Anything outside the closed
/// sets or basis-point ranges ⇒ `INVALID_INPUT` (fail closed, never clamped).
pub fn parse_affects(value: &Value) -> Result<Vec<AffectInput>, ErrorCode> {
    let Some(list) = value.get("affects") else {
        return Ok(Vec::new());
    };
    list.as_array()
        .ok_or(ErrorCode::InvalidInput)?
        .iter()
        .map(parse_affect_input)
        .collect()
}

fn parse_affect_input(v: &Value) -> Result<AffectInput, ErrorCode> {
    let text = |field: &str| v.get(field).and_then(Value::as_str);
    let kind = text("kind")
        .and_then(AffectKind::parse)
        .ok_or(ErrorCode::InvalidInput)?;
    let label = match v.get("label").filter(|l| !l.is_null()) {
        None => None,
        Some(l) => Some(
            l.as_str()
                .and_then(EmotionLabel::parse)
                .ok_or(ErrorCode::InvalidInput)?,
        ),
    };
    let target_scope = match v.get("target_scope").filter(|s| !s.is_null()) {
        None => None,
        Some(s) => Some(AffectTargetScope {
            kind: s
                .get("kind")
                .and_then(Value::as_str)
                .and_then(AffectTargetScopeKind::parse)
                .ok_or(ErrorCode::InvalidInput)?,
            id: uuid_field(s, "id")?.ok_or(ErrorCode::InvalidInput)?,
        }),
    };
    let target_subject_key = match v.get("target_subject_key").filter(|k| !k.is_null()) {
        None => None,
        Some(k) => Some(SubjectKey::new(
            k.get("kind")
                .and_then(Value::as_str)
                .and_then(SubjectKeyKind::parse)
                .ok_or(ErrorCode::InvalidInput)?,
            k.get("value")
                .and_then(Value::as_str)
                .ok_or(ErrorCode::InvalidInput)?,
        )?),
    };
    let observed_at = match text("observed_at") {
        None => None,
        Some(s) => Some(
            OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                .map_err(|_| ErrorCode::InvalidInput)?,
        ),
    };
    // ADR-0030 D-B / review P2: a MOOD whose observed_at lies in the future would never decay
    // (elapsed saturates at zero), so a client could defeat the Emotion/Mood distinction by
    // post-dating the observation. Reject anything later than now + a small clock-skew allowance;
    // the DB cannot express this bound (now() is not immutable), so the write path is the guard.
    if let Some(at) = observed_at {
        let skew = time::Duration::minutes(5);
        if at > time::OffsetDateTime::now_utc() + skew {
            return Err(ErrorCode::InvalidInput);
        }
    }
    Ok(AffectInput {
        annotation: AffectAnnotation {
            kind,
            label,
            valence: bp_field(v, "valence", BasisPoints::signed)?,
            arousal: bp_field(v, "arousal", BasisPoints::signed)?,
            dominance: bp_field(v, "dominance", BasisPoints::signed)?,
            intensity: bp_field(v, "intensity", BasisPoints::unit)?
                .ok_or(ErrorCode::InvalidInput)?,
            confidence: bp_field(v, "confidence", BasisPoints::unit)?
                .ok_or(ErrorCode::InvalidInput)?,
            target_subject: uuid_field(v, "target_subject_id")?.map(SubjectId),
            target_scope,
        },
        observed_at,
        target_subject_key,
    })
}

fn range_field(value: &Value, field: &str) -> Result<Option<BasisPointRange>, ErrorCode> {
    let Some(pair) = value.get(field).filter(|p| !p.is_null()) else {
        return Ok(None);
    };
    let pair = pair.as_array().ok_or(ErrorCode::InvalidInput)?;
    let [lo, hi] = pair.as_slice() else {
        return Err(ErrorCode::InvalidInput);
    };
    let bp = |v: &Value| {
        v.as_i64()
            .and_then(|n| i16::try_from(n).ok())
            .ok_or(ErrorCode::InvalidInput)
            .and_then(BasisPoints::signed)
    };
    Ok(Some(BasisPointRange::new(bp(lo)?, bp(hi)?)?))
}

fn closed_list<T>(
    value: &Value,
    field: &str,
    parse: fn(&str) -> Option<T>,
) -> Result<Vec<T>, ErrorCode> {
    match value.get(field).filter(|l| !l.is_null()) {
        None => Ok(Vec::new()),
        Some(list) => list
            .as_array()
            .ok_or(ErrorCode::InvalidInput)?
            .iter()
            .map(|x| x.as_str().and_then(parse).ok_or(ErrorCode::InvalidInput))
            .collect(),
    }
}

/// `recall.search.affect: {kinds?, labels_any?, valence:[lo,hi]?, arousal?, dominance?,
/// min_effective_intensity?}` → `Some(filter)`; absent ⇒ `None`; present but empty ⇒ `None`
/// too (no clause = no narrowing). `lo > hi` or an unknown label/kind ⇒ `INVALID_INPUT`.
pub fn parse_filter(value: &Value) -> Result<Option<AffectFilter>, ErrorCode> {
    let Some(a) = value.get("affect").filter(|a| !a.is_null()) else {
        return Ok(None);
    };
    if !a.is_object() {
        return Err(ErrorCode::InvalidInput);
    }
    let filter = AffectFilter {
        kinds: closed_list(a, "kinds", AffectKind::parse)?,
        labels_any: closed_list(a, "labels_any", EmotionLabel::parse)?,
        valence: range_field(a, "valence")?,
        arousal: range_field(a, "arousal")?,
        dominance: range_field(a, "dominance")?,
        min_effective_intensity: bp_field(a, "min_effective_intensity", BasisPoints::unit)?,
    };
    Ok((!filter.is_empty()).then_some(filter))
}

/// `recall.search.mood_congruence: {valence, arousal}` → the reader's mood point.
pub fn parse_mood(value: &Value) -> Result<Option<MoodPoint>, ErrorCode> {
    let Some(m) = value.get("mood_congruence").filter(|m| !m.is_null()) else {
        return Ok(None);
    };
    Ok(Some(MoodPoint {
        valence: bp_field(m, "valence", BasisPoints::signed)?.ok_or(ErrorCode::InvalidInput)?,
        arousal: bp_field(m, "arousal", BasisPoints::signed)?.ok_or(ErrorCode::InvalidInput)?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_affects_accepts_closed_values_and_fails_closed_on_range() {
        let subject = Uuid::now_v7();
        let parsed = parse_affects(&json!({"affects": [{
            "kind": "EMOTION", "label": "FRUSTRATION", "valence": -8000, "arousal": 5000,
            "dominance": -4000, "intensity": 9000, "confidence": 10000,
            "target_subject_id": subject.to_string(), "observed_at": "2026-09-01T00:00:00Z",
            "target_scope": {"kind": "TASK", "id": Uuid::now_v7().to_string()}
        }, {
            "kind": "MOOD", "intensity": 8200, "confidence": 7000,
            "target_subject_key": {"kind": "CRM", "value": "CRM-1"}
        }]}))
        .expect("valid affects");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].annotation.kind, AffectKind::Emotion);
        assert_eq!(parsed[0].annotation.label, Some(EmotionLabel::Frustration));
        assert_eq!(
            parsed[0].annotation.valence.map(BasisPoints::get),
            Some(-8000)
        );
        assert_eq!(
            parsed[0].annotation.target_subject,
            Some(SubjectId(subject))
        );
        assert!(parsed[0].observed_at.is_some());
        assert_eq!(parsed[1].annotation.kind, AffectKind::Mood);
        assert_eq!(
            parsed[1]
                .target_subject_key
                .as_ref()
                .map(|k| k.value.as_str()),
            Some("CRM-1")
        );
        assert!(parse_affects(&json!({})).expect("absent").is_empty());
        // review P2: a post-dated MOOD would never decay — the write path must refuse it.
        let future = (time::OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&time::format_description::well_known::Rfc3339)
            .expect("rfc3339");
        assert!(parse_affects(&json!({"affects": [{"kind": "MOOD", "intensity": 1, "confidence": 1, "observed_at": future}]})).is_err());
        let recent = (time::OffsetDateTime::now_utc() - time::Duration::minutes(1))
            .format(&time::format_description::well_known::Rfc3339)
            .expect("rfc3339");
        assert!(parse_affects(&json!({"affects": [{"kind": "MOOD", "intensity": 1, "confidence": 1, "observed_at": recent}]})).is_ok());
        for bad in [
            json!({"affects": [{"kind": "EMOTION", "intensity": 10001, "confidence": 1}]}),
            json!({"affects": [{"kind": "EMOTION", "valence": -10001, "intensity": 1, "confidence": 1}]}),
            json!({"affects": [{"kind": "emotion", "intensity": 1, "confidence": 1}]}),
            json!({"affects": [{"kind": "MOOD", "label": "BOREDOM", "intensity": 1, "confidence": 1}]}),
            json!({"affects": [{"kind": "MOOD", "confidence": 1}]}),
            json!({"affects": [{"kind": "MOOD", "intensity": 1, "confidence": 1, "observed_at": "yesterday"}]}),
            json!({"affects": [{"kind": "MOOD", "intensity": 1, "confidence": 1, "target_scope": {"kind": "TENANT", "id": Uuid::now_v7().to_string()}}]}),
        ] {
            assert_eq!(parse_affects(&bad), Err(ErrorCode::InvalidInput), "{bad}");
        }
    }

    #[test]
    fn parse_filter_and_mood_reject_inverted_ranges_and_unknown_labels() {
        let f = parse_filter(&json!({"affect": {
            "kinds": ["EMOTION"], "labels_any": ["FRUSTRATION", "ANGER"],
            "valence": [-10000, -1], "min_effective_intensity": 5000
        }}))
        .expect("valid")
        .expect("non-empty");
        assert_eq!(f.kinds, vec![AffectKind::Emotion]);
        assert_eq!(f.labels_any.len(), 2);
        assert_eq!(
            f.valence.map(|r| (r.lo.get(), r.hi.get())),
            Some((-10000, -1))
        );
        assert_eq!(f.min_effective_intensity.map(BasisPoints::get), Some(5000));
        assert_eq!(parse_filter(&json!({})), Ok(None));
        assert_eq!(
            parse_filter(&json!({"affect": {}})),
            Ok(None),
            "no clause = no narrowing"
        );
        assert_eq!(
            parse_filter(&json!({"affect": {"valence": [1, -1]}})),
            Err(ErrorCode::InvalidInput)
        );
        assert_eq!(
            parse_filter(&json!({"affect": {"labels_any": ["BOREDOM"]}})),
            Err(ErrorCode::InvalidInput)
        );
        assert_eq!(
            parse_filter(&json!({"affect": {"min_effective_intensity": -1}})),
            Err(ErrorCode::InvalidInput)
        );
        let mood = parse_mood(&json!({"mood_congruence": {"valence": -8000, "arousal": 5000}}))
            .expect("valid")
            .expect("present");
        assert_eq!((mood.valence.get(), mood.arousal.get()), (-8000, 5000));
        assert_eq!(
            parse_mood(&json!({"mood_congruence": {"valence": -8000}})),
            Err(ErrorCode::InvalidInput)
        );
    }
}
