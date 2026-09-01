//! `adapters::public_repo` — Phase 9 public admission, receipt evaluation, revocation, and
//! explicit outbox dispatch (§12.6 / §13 / §14 / §31).
//!
//! Every write uses `PublicWorkerDbPool` and a real tenant transaction.  The public database
//! remains the serving authority: no function reads private rows or accepts caller-provided
//! released content or scanner outcomes.

use humaux_application::public_evolve::{self, RootIdentity};
use humaux_domain::{
    error::ErrorCode, identity::AuthorizationScope, ids::TenantId, public::ModerationState,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{Row, types::Uuid};

use crate::postgres::{
    PrivateWorkerDbPool, PublicWorkerDbPool, RetrievalWorkerDbPool, RuntimeDbPool,
};

type Txn<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

fn db_error(error: sqlx::Error) -> ErrorCode {
    match error {
        sqlx::Error::RowNotFound => ErrorCode::NotFound,
        sqlx::Error::Database(ref db) => match db.code().as_deref() {
            Some("23503") => ErrorCode::TenantBoundary,
            Some("42501") => ErrorCode::Forbidden,
            Some("23505") | Some("40001") | Some("40P01") => ErrorCode::Conflict,
            Some("22023") | Some("23514") => ErrorCode::InvalidInput,
            _ => ErrorCode::Internal,
        },
        _ => ErrorCode::DependencyUnavailable,
    }
}

async fn set_tenant_local(txn: &mut Txn<'_>, tenant_id: TenantId) -> Result<(), ErrorCode> {
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{}'", tenant_id.0))
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;
    Ok(())
}

async fn set_user_local(txn: &mut Txn<'_>, user_id: Uuid) -> Result<(), ErrorCode> {
    sqlx::query(&format!("SET LOCAL humaux.user_id = '{user_id}'"))
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;
    Ok(())
}

/// Durable identity of a release-admitted initial claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedRelease {
    pub release_id: Uuid,
    pub source_id: Uuid,
    pub claim_id: Uuid,
    pub object_revision: i64,
}

/// The protected dispatcher intentionally omits `tenant_id` and queue metadata (§13).
#[derive(Clone, Debug)]
struct PublicDispatchJob {
    job_id: Uuid,
    job_type: String,
    attempt: i32,
    payload: Value,
}

impl PublicDispatchJob {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, ErrorCode> {
        Ok(Self {
            job_id: row.try_get("job_id").map_err(db_error)?,
            job_type: row.try_get("job_type").map_err(db_error)?,
            attempt: row.try_get("attempt").map_err(db_error)?,
            payload: row.try_get("payload").map_err(db_error)?,
        })
    }
}

/// Anonymous public work is globally leased and never carries a tenant identifier (§13).
#[derive(Clone, Debug)]
struct AnonymousDispatchJob {
    dispatch_id: Uuid,
    job_type: String,
    attempt: i32,
    payload: Value,
}

impl AnonymousDispatchJob {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, ErrorCode> {
        Ok(Self {
            dispatch_id: row.try_get("dispatch_id").map_err(db_error)?,
            job_type: row.try_get("job_type").map_err(db_error)?,
            attempt: row.try_get("attempt").map_err(db_error)?,
            payload: row.try_get("payload").map_err(db_error)?,
        })
    }
}

/// Authenticated, revision-pinned manual review request.  `expected_body_sha256` is the
/// database body's SHA-256, never a digest supplied in place of the stored body (§12.6).
#[derive(Clone, Debug)]
pub struct EvaluateClaim<'a> {
    pub claim_id: Uuid,
    pub expected_revision: i64,
    pub expected_body_sha256: &'a [u8],
    pub policy_version: &'a str,
    pub rationale: &'a str,
    pub target_state: ModerationState,
}

/// Immutable review result linked from the current object revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvaluationResult {
    pub evaluation_id: Uuid,
    pub object_revision: i64,
    pub moderation_state: String,
}

/// Immutable identity of one public revision.  It is the complete identity accepted by a
/// projection candidate hydrate and the permanent tombstone protocol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionIdentity {
    pub object_id: Uuid,
    pub object_kind: String,
    pub object_revision: i64,
    pub evaluation_id: Uuid,
    pub body_sha256: [u8; 32],
}

/// One public object visible through the database's current-receipt/revocation predicate.
/// `canonical_body` and `body_sha256` are both derived by PostgreSQL from `content::text`; JSON
/// re-serialization never defines a projection identity.
#[derive(Clone, Debug, PartialEq)]
pub struct EligibleObject {
    pub object_id: Uuid,
    pub object_kind: String,
    pub content: Value,
    pub canonical_body: String,
    pub object_revision: i64,
    pub evaluation_id: Uuid,
    pub body_sha256: [u8; 32],
}

impl EligibleObject {
    pub fn identity(&self) -> ProjectionIdentity {
        ProjectionIdentity {
            object_id: self.object_id,
            object_kind: self.object_kind.clone(),
            object_revision: self.object_revision,
            evaluation_id: self.evaluation_id,
            body_sha256: self.body_sha256,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionWriteOutcome {
    Applied,
    Superseded,
}

/// Public worker's only open projection seam.  Live writes receive a freshly hydrated eligible
/// body; retirement writes the deterministic permanent tombstone for a prior revision.
#[async_trait::async_trait]
pub trait PublicProjectionPort: Send + Sync {
    async fn project_live(
        &self,
        object: &EligibleObject,
    ) -> Result<ProjectionWriteOutcome, ErrorCode>;
    async fn retire(&self, identity: &ProjectionIdentity) -> Result<(), ErrorCode>;
}

/// Admits the exact bytes and opaque scanner receipt already frozen on an ACTIVE release.
/// The function reads all release material from staging and is idempotent by `intake_release_id`.
pub async fn admit_release(
    pool: &PublicWorkerDbPool,
    tenant_id: TenantId,
    release_id: Uuid,
) -> Result<AdmittedRelease, ErrorCode> {
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    set_tenant_local(&mut txn, tenant_id).await?;
    let admitted = admit_release_in_txn(&mut txn, tenant_id, release_id).await?;
    txn.commit().await.map_err(db_error)?;
    Ok(admitted)
}

#[allow(clippy::too_many_lines)] // one transaction keeps the release lock, source graph, and outbox atomic.
async fn admit_release_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: TenantId,
    release_id: Uuid,
) -> Result<AdmittedRelease, ErrorCode> {
    // This takes the canonical shared release lock and obtains a post-lock READ COMMITTED view.
    sqlx::query("SELECT staging.assert_active_contribution_release($1)")
        .bind(release_id)
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;

    if let Some(row) = sqlx::query(
        "SELECT c.claim_id, p.source_id, c.object_revision \
         FROM public.claims c JOIN public.provenance_edges p USING(claim_id) \
         WHERE c.intake_release_id=$1",
    )
    .bind(release_id)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?
    {
        return Ok(AdmittedRelease {
            release_id,
            claim_id: row.try_get("claim_id").map_err(db_error)?,
            source_id: row.try_get("source_id").map_err(db_error)?,
            object_revision: row.try_get("object_revision").map_err(db_error)?,
        });
    }

    let row = sqlx::query(
        "SELECT candidate_id, confirmation_id, disclosed_payload, disclosed_payload_sha256, \
                scan_receipt, rights_basis, source_license, publisher, redistribution_policy \
         FROM staging.contribution_releases \
         WHERE tenant_id=$1 AND contribution_release_id=$2 AND state='ACTIVE'",
    )
    .bind(tenant_id.0)
    .bind(release_id)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?
    .ok_or(ErrorCode::NotFound)?;
    let payload: Option<Vec<u8>> = row.try_get("disclosed_payload").map_err(db_error)?;
    let stored_hash: Option<Vec<u8>> = row.try_get("disclosed_payload_sha256").map_err(db_error)?;
    let candidate_id: Option<Uuid> = row.try_get("candidate_id").map_err(db_error)?;
    let confirmation_id: Option<Uuid> = row.try_get("confirmation_id").map_err(db_error)?;
    let receipt: Option<Value> = row.try_get("scan_receipt").map_err(db_error)?;
    let (payload, stored_hash, receipt) =
        match (payload, stored_hash, candidate_id, confirmation_id, receipt) {
            (Some(payload), Some(hash), Some(_), Some(_), Some(receipt)) => {
                (payload, hash, receipt)
            }
            _ => return Err(ErrorCode::InvalidInput), // nullable legacy release: never newly admitted.
        };
    let actual_hash = Sha256::digest(&payload);
    let receipt_is_real = receipt.is_object()
        && [
            "privacy_rules_version",
            "privacy_rules_digest",
            "gitleaks_version",
        ]
        .into_iter()
        .all(|key| {
            receipt
                .get(key)
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty())
        })
        && receipt
            .get("gitleaks_binary_sha256")
            .and_then(Value::as_str)
            .is_some_and(|value| {
                value.len() == 64
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            });
    if payload.is_empty()
        || stored_hash.len() != 32
        || actual_hash.as_slice() != stored_hash.as_slice()
        || !receipt_is_real
    {
        return Err(ErrorCode::InvalidInput);
    }
    let text = std::str::from_utf8(&payload).map_err(|_| ErrorCode::InvalidInput)?;
    let body = json!({"text": text});
    let claim_id: Option<Uuid> = sqlx::query_scalar(
        "INSERT INTO public.claims(content, moderation_state, intake_release_id) \
         VALUES($1,'PUBLIC_STAGING',$2) \
         ON CONFLICT (intake_release_id) WHERE intake_release_id IS NOT NULL DO NOTHING \
         RETURNING claim_id",
    )
    .bind(&body)
    .bind(release_id)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?;
    let Some(claim_id) = claim_id else {
        let existing = sqlx::query("SELECT c.claim_id,p.source_id,c.object_revision FROM public.claims c JOIN public.provenance_edges p USING(claim_id) WHERE c.intake_release_id=$1")
            .bind(release_id).fetch_one(&mut **txn).await.map_err(db_error)?;
        return Ok(AdmittedRelease {
            release_id,
            claim_id: existing.try_get("claim_id").map_err(db_error)?,
            source_id: existing.try_get("source_id").map_err(db_error)?,
            object_revision: existing.try_get("object_revision").map_err(db_error)?,
        });
    };
    let source_id: Uuid = sqlx::query_scalar(
        "INSERT INTO public.sources(source_type,publisher,content_hash,source_license,rights_basis, \
             redistribution_policy,trust_class,contribution_release_id) \
         VALUES('USER_CONTRIBUTION',$1,$2,$3,$4,$5,'UNDER_REVIEW',$6) RETURNING source_id",
    )
    .bind(row.try_get::<Option<String>, _>("publisher").map_err(db_error)?)
    .bind(hex::encode(&stored_hash))
    .bind(row.try_get::<Option<String>, _>("source_license").map_err(db_error)?)
    .bind(row.try_get::<String, _>("rights_basis").map_err(db_error)?)
    .bind(row.try_get::<Option<String>, _>("redistribution_policy").map_err(db_error)?)
    .bind(release_id)
    .fetch_one(&mut **txn)
    .await
    .map_err(db_error)?;
    sqlx::query("INSERT INTO public.provenance_edges(claim_id,source_id) VALUES($1,$2)")
        .bind(claim_id)
        .bind(source_id)
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;
    sqlx::query(
        "INSERT INTO public.source_closure(claim_id,root_source_id,depth,is_current) \
         VALUES($1,$2,1,true)",
    )
    .bind(claim_id)
    .bind(source_id)
    .execute(&mut **txn)
    .await
    .map_err(db_error)?;
    let revision: i64 = sqlx::query_scalar(
        "UPDATE public.claims SET moderation_state='UNDER_REVIEW',object_revision=object_revision+1 \
         WHERE claim_id=$1 RETURNING object_revision",
    )
    .bind(claim_id)
    .fetch_one(&mut **txn)
    .await
    .map_err(db_error)?;
    insert_object_outbox(txn, tenant_id, Some(claim_id), None, revision).await?;
    Ok(AdmittedRelease {
        release_id,
        source_id,
        claim_id,
        object_revision: revision,
    })
}

/// Stores a complete immutable receipt and applies one allowed manual review state. Every
/// state transition requires an authenticated active global moderator (§12.6).
pub async fn evaluate_claim(
    pool: &PublicWorkerDbPool,
    authorization: &AuthorizationScope,
    input: &EvaluateClaim<'_>,
) -> Result<EvaluationResult, ErrorCode> {
    if input.expected_revision <= 0
        || input.policy_version.trim().is_empty()
        || input.rationale.trim().is_empty()
        || !matches!(
            input.target_state,
            ModerationState::UnderReview
                | ModerationState::Supported
                | ModerationState::Quarantined
        )
    {
        return Err(ErrorCode::InvalidInput);
    }
    let tenant_id = authorization.tenant_id();
    let evaluator_user_id = authorization.user_id().ok_or(ErrorCode::Unauthorized)?;
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    set_tenant_local(&mut txn, tenant_id).await?;
    set_user_local(&mut txn, evaluator_user_id.0).await?;
    let result = evaluate_claim_in_txn(&mut txn, tenant_id, evaluator_user_id.0, input).await?;
    txn.commit().await.map_err(db_error)?;
    Ok(result)
}

/// Applies an anonymous-root review through the sole protected database boundary.  The
/// private worker passes authenticated scope, but receives only the public receipt identity.
pub async fn evaluate_anonymous_claim(
    pool: &PrivateWorkerDbPool,
    authorization: &AuthorizationScope,
    input: &EvaluateClaim<'_>,
) -> Result<EvaluationResult, ErrorCode> {
    if input.expected_revision <= 0
        || input.expected_body_sha256.len() != 32
        || input.policy_version.trim().is_empty()
        || input.rationale.trim().is_empty()
        || !matches!(
            input.target_state,
            ModerationState::UnderReview
                | ModerationState::Supported
                | ModerationState::Quarantined
        )
    {
        return Err(ErrorCode::InvalidInput);
    }
    let evaluator_user_id = authorization.user_id().ok_or(ErrorCode::Unauthorized)?;
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    set_tenant_local(&mut txn, authorization.tenant_id()).await?;
    set_user_local(&mut txn, evaluator_user_id.0).await?;
    let row = sqlx::query(
        "SELECT anonymous_trust_receipt_id,object_revision,moderation_state \
         FROM public.evaluate_anonymous_claim($1,$2,$3,$4,$5,$6,$7,$8)",
    )
    .bind(authorization.tenant_id().0)
    .bind(evaluator_user_id.0)
    .bind(input.claim_id)
    .bind(input.expected_revision)
    .bind(input.expected_body_sha256)
    .bind(input.policy_version)
    .bind(input.rationale)
    .bind(input.target_state.as_db_str())
    .fetch_one(&mut *txn)
    .await
    .map_err(db_error)?;
    let result = EvaluationResult {
        evaluation_id: row
            .try_get("anonymous_trust_receipt_id")
            .map_err(db_error)?,
        object_revision: row.try_get("object_revision").map_err(db_error)?,
        moderation_state: row.try_get("moderation_state").map_err(db_error)?,
    };
    txn.commit().await.map_err(db_error)?;
    Ok(result)
}

#[allow(clippy::too_many_lines)] // one transaction pins the reviewed body, complete receipt, state CAS, and outbox.
async fn evaluate_claim_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: TenantId,
    evaluator_user_id: Uuid,
    input: &EvaluateClaim<'_>,
) -> Result<EvaluationResult, ErrorCode> {
    sqlx::query("SELECT ops.lock_contribution_inputs()")
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;
    lock_evaluation_graph(txn).await?;
    let row = sqlx::query(
        "SELECT content, object_revision, sha256(convert_to(content::text,'UTF8')) AS body_sha256 \
         FROM public.claims WHERE claim_id=$1 FOR UPDATE",
    )
    .bind(input.claim_id)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?
    .ok_or(ErrorCode::NotFound)?;
    let current_revision: i64 = row.try_get("object_revision").map_err(db_error)?;
    let body_hash: Vec<u8> = row.try_get("body_sha256").map_err(db_error)?;
    if current_revision != input.expected_revision
        || body_hash.as_slice() != input.expected_body_sha256
    {
        return Err(ErrorCode::Conflict);
    }
    let roots = load_claim_roots(txn, input.claim_id).await?;
    let assessment = public_evolve::assess_roots(&roots).map_err(|_| ErrorCode::InvalidInput)?;
    let contradiction_count: i32 = sqlx::query_scalar(
        "SELECT count(*)::integer FROM public.relations \
         WHERE relation_kind='CONTRADICTS' AND (from_claim_id=$1 OR to_claim_id=$1)",
    )
    .bind(input.claim_id)
    .fetch_one(&mut **txn)
    .await
    .map_err(db_error)?;
    let is_moderator = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM control.public_moderator_grants g \
         JOIN control.users u USING(user_id) JOIN control.memberships m USING(user_id) \
         WHERE g.user_id=$1 AND g.enabled AND u.state='ACTIVE' AND m.state='ACTIVE' \
           AND m.tenant_id=$2)",
    )
    .bind(evaluator_user_id)
    .bind(tenant_id.0)
    .fetch_one(&mut **txn)
    .await
    .map_err(db_error)?;
    if !is_moderator {
        return Err(ErrorCode::Forbidden);
    }
    let next_revision = current_revision
        .checked_add(1)
        .ok_or(ErrorCode::InvalidInput)?;
    let moderation_state = input.target_state.as_db_str();
    let evaluation_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO public.claim_trust_evaluations( \
           evaluation_id,claim_id,object_revision,body_sha256,policy_version,moderation_state,evaluator_user_id, \
           evaluator_grant_version,rationale,support_count,independent_support_count, \
           trusted_source_count,identity_incomplete,contradiction_count,checks_complete) \
         SELECT $1,$2,$3,$4,$5,$6,$7,g.grant_version,$8,$9,$10,$11,$12,$13,$14 \
         FROM (SELECT 1) x LEFT JOIN control.public_moderator_grants g \
           ON g.user_id=$7 AND g.enabled",
    )
    .bind(evaluation_id)
    .bind(input.claim_id)
    .bind(next_revision)
    .bind(&body_hash)
    .bind(input.policy_version)
    .bind(moderation_state)
    .bind(evaluator_user_id)
    .bind(input.rationale)
    .bind(assessment.support_count)
    .bind(assessment.independent_support_count)
    .bind(assessment.trusted_source_count)
    .bind(assessment.identity_incomplete)
    .bind(contradiction_count)
    .bind(matches!(input.target_state, ModerationState::Supported))
    .execute(&mut **txn)
    .await
    .map_err(db_error)?;
    for root in &roots {
        let release_id: Option<Uuid> = sqlx::query_scalar(
            "SELECT contribution_release_id FROM public.sources WHERE source_id=$1",
        )
        .bind(root.source_id)
        .fetch_one(&mut **txn)
        .await
        .map_err(db_error)?;
        sqlx::query(
            "INSERT INTO public.claim_trust_evaluation_sources( \
              evaluation_id,root_source_id,source_content_hash,contribution_release_id) \
             VALUES($1,$2,$3,$4)",
        )
        .bind(evaluation_id)
        .bind(root.source_id)
        .bind(&root.content_hash)
        .bind(release_id)
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;
    }
    insert_signal(
        txn,
        evaluation_id,
        "IDENTITY_INCOMPLETE",
        input.policy_version,
        assessment.identity_incomplete,
    )
    .await?;
    insert_signal(
        txn,
        evaluation_id,
        "CONTRADICTION_PRESENT",
        input.policy_version,
        contradiction_count > 0,
    )
    .await?;
    let revision: i64 = sqlx::query_scalar(
        "UPDATE public.claims SET current_evaluation_id=$2, moderation_state=$3, \
             object_revision=$4 WHERE claim_id=$1 RETURNING object_revision",
    )
    .bind(input.claim_id)
    .bind(evaluation_id)
    .bind(moderation_state)
    .bind(next_revision)
    .fetch_one(&mut **txn)
    .await
    .map_err(db_error)?;
    sqlx::query("SELECT public.carry_forward_anonymous_claim_lifecycle($1,$2,$3)")
        .bind(input.claim_id)
        .bind(current_revision)
        .bind(revision)
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;
    insert_object_outbox(txn, tenant_id, Some(input.claim_id), None, revision).await?;
    Ok(EvaluationResult {
        evaluation_id,
        object_revision: revision,
        moderation_state: moderation_state.into(),
    })
}

async fn lock_evaluation_graph(txn: &mut Txn<'_>) -> Result<(), ErrorCode> {
    sqlx::query(
        "LOCK TABLE public.provenance_edges, public.sources, public.synthesis_inputs, \
         public.source_closure, public.relations IN SHARE MODE",
    )
    .execute(&mut **txn)
    .await
    .map_err(db_error)?;
    Ok(())
}

async fn load_claim_roots(
    txn: &mut Txn<'_>,
    claim_id: Uuid,
) -> Result<Vec<RootIdentity>, ErrorCode> {
    let rows = sqlx::query(
        "SELECT s.source_id,s.content_hash,s.verified_organization,s.canonical_url, \
                s.document_fingerprint,s.upstream_root,s.identity_policy_version,s.trusted_by_policy \
         FROM public.current_public_roots($1,NULL) r JOIN public.sources s \
           ON s.source_id=r.root_source_id ORDER BY s.source_id",
    )
    .bind(claim_id).fetch_all(&mut **txn).await.map_err(db_error)?;
    if rows.is_empty() {
        return Err(ErrorCode::InvalidInput);
    }
    rows.into_iter()
        .map(|row| {
            let verified: Option<String> =
                row.try_get("identity_policy_version").map_err(db_error)?;
            Ok(RootIdentity {
                source_id: row.try_get("source_id").map_err(db_error)?,
                content_hash: row.try_get("content_hash").map_err(db_error)?,
                verified_organization: if verified.is_some() {
                    row.try_get("verified_organization").map_err(db_error)?
                } else {
                    None
                },
                canonical_url: if verified.is_some() {
                    row.try_get("canonical_url").map_err(db_error)?
                } else {
                    None
                },
                document_fingerprint: if verified.is_some() {
                    row.try_get("document_fingerprint").map_err(db_error)?
                } else {
                    None
                },
                upstream_root: if verified.is_some() {
                    row.try_get("upstream_root").map_err(db_error)?
                } else {
                    None
                },
                trusted: if verified.is_some() {
                    row.try_get("trusted_by_policy").map_err(db_error)?
                } else {
                    None
                },
            })
        })
        .collect()
}

async fn insert_signal(
    txn: &mut Txn<'_>,
    evaluation_id: Uuid,
    signal: &str,
    version: &str,
    detected: bool,
) -> Result<(), ErrorCode> {
    sqlx::query(
        "INSERT INTO public.poisoning_signals(evaluation_id,signal_code,check_version,detected) \
         VALUES($1,$2,$3,$4)",
    )
    .bind(evaluation_id)
    .bind(signal)
    .bind(version)
    .bind(detected)
    .execute(&mut **txn)
    .await
    .map_err(db_error)?;
    Ok(())
}

async fn insert_object_outbox(
    txn: &mut Txn<'_>,
    tenant_id: TenantId,
    claim_id: Option<Uuid>,
    synthesis_id: Option<Uuid>,
    revision: i64,
) -> Result<(), ErrorCode> {
    sqlx::query("SELECT ops.emit_public_object_changed($1,$2,$3,$4)")
        .bind(tenant_id.0)
        .bind(claim_id)
        .bind(synthesis_id)
        .bind(revision)
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;
    Ok(())
}

/// Reads the current serving set through `public.eligible_objects` in one fresh READ COMMITTED
/// statement and verifies every immutable projection-identity field. Gateway callers get no
/// staging access through this typed entry point.
pub async fn hydrate_gateway(
    pool: &RuntimeDbPool,
    expected: &ProjectionIdentity,
) -> Result<Option<EligibleObject>, ErrorCode> {
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    set_read_committed(&mut txn).await?;
    let hydrated = hydrate_strict_in_txn(&mut txn, expected).await?;
    txn.commit().await.map_err(db_error)?;
    Ok(hydrated)
}

/// Retrieval worker equivalent of [`hydrate_gateway`], kept typed so it cannot accidentally
/// acquire public-worker mutation rights.
pub async fn hydrate_retrieval(
    pool: &RetrievalWorkerDbPool,
    expected: &ProjectionIdentity,
) -> Result<Option<EligibleObject>, ErrorCode> {
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    set_read_committed(&mut txn).await?;
    let hydrated = hydrate_strict_in_txn(&mut txn, expected).await?;
    txn.commit().await.map_err(db_error)?;
    Ok(hydrated)
}

async fn set_read_committed(txn: &mut Txn<'_>) -> Result<(), ErrorCode> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut **txn)
        .await
        .map(|_| ())
        .map_err(db_error)
}

async fn hydrate_strict_in_txn(
    txn: &mut Txn<'_>,
    expected: &ProjectionIdentity,
) -> Result<Option<EligibleObject>, ErrorCode> {
    sqlx::query(
        "SELECT object_id,object_kind,content,content::text AS canonical_body,object_revision, \
                current_evaluation_id,sha256(convert_to(content::text,'UTF8')) AS body_sha256 \
         FROM public.eligible_objects WHERE object_id=$1 AND object_kind=$2 AND object_revision=$3 \
           AND current_evaluation_id=$4 AND sha256(convert_to(content::text,'UTF8'))=$5",
    )
    .bind(expected.object_id)
    .bind(&expected.object_kind)
    .bind(expected.object_revision)
    .bind(expected.evaluation_id)
    .bind(expected.body_sha256.as_slice())
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?
    .map(eligible_from_row)
    .transpose()
}

fn eligible_from_row(row: sqlx::postgres::PgRow) -> Result<EligibleObject, ErrorCode> {
    let body_sha256: Vec<u8> = row.try_get("body_sha256").map_err(db_error)?;
    let body_sha256: [u8; 32] = body_sha256.try_into().map_err(|_| ErrorCode::Internal)?;
    Ok(EligibleObject {
        object_id: row.try_get("object_id").map_err(db_error)?,
        object_kind: row.try_get("object_kind").map_err(db_error)?,
        content: row.try_get("content").map_err(db_error)?,
        canonical_body: row.try_get("canonical_body").map_err(db_error)?,
        object_revision: row.try_get("object_revision").map_err(db_error)?,
        evaluation_id: row.try_get("current_evaluation_id").map_err(db_error)?,
        body_sha256,
    })
}

/// Enqueues exactly one public job per relevant outbox event and consumer in the same
/// transaction that acknowledges the event (§14).  Synthesis changes become the deliberately
/// unclaimed Phase 10 rebuild placeholder; claims become public projection work.
pub async fn drain_outbox(
    pool: &PublicWorkerDbPool,
    tenant_id: TenantId,
    limit: i64,
) -> Result<u64, ErrorCode> {
    if limit <= 0 {
        return Err(ErrorCode::InvalidInput);
    }
    let count: i64 = sqlx::query_scalar("SELECT ops.dispatch_public_outbox($1,$2)")
        .bind(tenant_id.0)
        .bind(limit)
        .fetch_one(pool.pool())
        .await
        .map_err(db_error)?;
    u64::try_from(count).map_err(|_| ErrorCode::Internal)
}

/// Applies a release revoke to currently evaluated objects.  The immutable revocation fact is
/// already the serving fence; this transaction only records the asynchronous object states.
async fn apply_revocation_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: TenantId,
    release_id: Uuid,
) -> Result<(), ErrorCode> {
    // The closure, rather than a current trust receipt, names every object whose root set
    // contains the revoked release. This includes initial under-review claims and objects whose
    // receipt was already cleared by an earlier revoke.
    let claims: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT c.claim_id FROM public.claims c \
         JOIN public.source_closure closure ON closure.claim_id=c.claim_id AND closure.is_current \
         JOIN public.sources source ON source.source_id=closure.root_source_id \
         WHERE source.contribution_release_id=$1",
    )
    .bind(release_id)
    .fetch_all(&mut **txn)
    .await
    .map_err(db_error)?;
    for claim_id in claims {
        let survivors: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM public.current_public_roots($1,NULL) root \
             JOIN public.sources s ON s.source_id=root.root_source_id \
             LEFT JOIN ops.public_release_revocations v ON v.release_id=s.contribution_release_id \
             WHERE v.release_id IS NULL)",
        )
        .bind(claim_id)
        .fetch_one(&mut **txn)
        .await
        .map_err(db_error)?;
        let state = if survivors { "UNDER_REVIEW" } else { "REVOKED" };
        let revision: Option<i64> = sqlx::query_scalar(
            "UPDATE public.claims SET moderation_state=$2,current_evaluation_id=NULL, \
                object_revision=object_revision+1 WHERE claim_id=$1 \
                AND (moderation_state IS DISTINCT FROM $2 OR current_evaluation_id IS NOT NULL) \
                RETURNING object_revision",
        )
        .bind(claim_id)
        .bind(state)
        .fetch_optional(&mut **txn)
        .await
        .map_err(db_error)?;
        if let Some(revision) = revision {
            insert_object_outbox(txn, tenant_id, Some(claim_id), None, revision).await?;
        }
    }
    let syntheses: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT synthesis.synthesis_id FROM public.syntheses synthesis \
         JOIN public.source_closure closure ON closure.synthesis_id=synthesis.synthesis_id AND closure.is_current \
         JOIN public.sources source ON source.source_id=closure.root_source_id \
         WHERE source.contribution_release_id=$1",
    )
    .bind(release_id)
    .fetch_all(&mut **txn)
    .await
    .map_err(db_error)?;
    for synthesis_id in syntheses {
        let survivors: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM public.current_public_roots(NULL,$1) root \
             JOIN public.sources s ON s.source_id=root.root_source_id \
             LEFT JOIN ops.public_release_revocations v ON v.release_id=s.contribution_release_id \
             WHERE v.release_id IS NULL)",
        )
        .bind(synthesis_id)
        .fetch_one(&mut **txn)
        .await
        .map_err(db_error)?;
        let state = if survivors { "UNDER_REVIEW" } else { "REVOKED" };
        let revision: Option<i64> = sqlx::query_scalar(
            "UPDATE public.syntheses SET moderation_state=$2,current_evaluation_id=NULL, \
                object_revision=object_revision+1 WHERE synthesis_id=$1 \
                AND (moderation_state IS DISTINCT FROM $2 OR current_evaluation_id IS NOT NULL) \
                RETURNING object_revision",
        )
        .bind(synthesis_id)
        .bind(state)
        .fetch_optional(&mut **txn)
        .await
        .map_err(db_error)?;
        if let Some(revision) = revision {
            insert_object_outbox(txn, tenant_id, None, Some(synthesis_id), revision).await?;
        }
    }
    Ok(())
}

async fn admit_anonymous_in_txn(
    txn: &mut Txn<'_>,
    dispatch: &AnonymousDispatchJob,
    lease_owner: &str,
) -> Result<(), ErrorCode> {
    let admitted = sqlx::query(
        "SELECT claim_id,object_revision FROM public.admit_anonymous_dispatch($1,$2,$3)",
    )
    .bind(dispatch.dispatch_id)
    .bind(lease_owner)
    .bind(dispatch.attempt)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?;
    let Some(admitted) = admitted else {
        return Ok(());
    };
    let claim_id: Uuid = admitted.try_get("claim_id").map_err(db_error)?;
    let revision: i64 = admitted.try_get("object_revision").map_err(db_error)?;
    enqueue_anonymous_projection(txn, dispatch, lease_owner, Some(claim_id), None, revision).await
}

async fn revoke_anonymous_in_txn(
    txn: &mut Txn<'_>,
    dispatch: &AnonymousDispatchJob,
    lease_owner: &str,
) -> Result<(), ErrorCode> {
    let rows = sqlx::query(
        "SELECT claim_id,synthesis_id,object_revision \
         FROM public.revoke_anonymous_dispatch($1,$2,$3)",
    )
    .bind(dispatch.dispatch_id)
    .bind(lease_owner)
    .bind(dispatch.attempt)
    .fetch_all(&mut **txn)
    .await
    .map_err(db_error)?;
    for row in rows {
        let claim_id: Option<Uuid> = row.try_get("claim_id").map_err(db_error)?;
        let synthesis_id: Option<Uuid> = row.try_get("synthesis_id").map_err(db_error)?;
        let revision: i64 = row.try_get("object_revision").map_err(db_error)?;
        enqueue_anonymous_projection(txn, dispatch, lease_owner, claim_id, synthesis_id, revision)
            .await?;
    }
    Ok(())
}

async fn enqueue_anonymous_projection(
    txn: &mut Txn<'_>,
    dispatch: &AnonymousDispatchJob,
    lease_owner: &str,
    claim_id: Option<Uuid>,
    synthesis_id: Option<Uuid>,
    revision: i64,
) -> Result<(), ErrorCode> {
    sqlx::query("SELECT ops.enqueue_public_projection_from_anonymous_dispatch($1,$2,$3,$4,$5,$6)")
        .bind(dispatch.dispatch_id)
        .bind(lease_owner)
        .bind(dispatch.attempt)
        .bind(claim_id)
        .bind(synthesis_id)
        .bind(revision)
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;
    Ok(())
}

/// Runs at most `limit` public work items.  There is no resident loop: the caller must invoke
/// this explicit bounded operation (§31).  Each handled job locks and rechecks its attempt in
/// the same transaction as the business writes and final DONE transition.
pub async fn run_once(
    pool: &PublicWorkerDbPool,
    tenant_id: TenantId,
    lease_owner: &str,
    limit: i64,
    projection: &dyn PublicProjectionPort,
) -> Result<u64, ErrorCode> {
    let rows = sqlx::query(
        "SELECT job_id,job_type,attempt,payload \
         FROM ops.claim_public_dispatch_jobs($1,$2,$3,$4)",
    )
    .bind(tenant_id.0)
    .bind(lease_owner)
    .bind(60.0)
    .bind(limit)
    .fetch_all(pool.pool())
    .await
    .map_err(db_error)?;
    let claimed = rows
        .iter()
        .map(PublicDispatchJob::from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let mut completed = 0;
    for job in claimed {
        match execute_claimed_job(pool, tenant_id, lease_owner, &job, projection).await {
            Ok(true) => completed += 1,
            Ok(false) => {}
            Err(error) => {
                let _ = fail_public_dispatch_job(pool, tenant_id, &job, lease_owner).await;
                if matches!(error, ErrorCode::Forbidden | ErrorCode::TenantBoundary) {
                    return Err(error);
                }
            }
        }
    }
    Ok(completed)
}

async fn execute_claimed_job(
    pool: &PublicWorkerDbPool,
    tenant_id: TenantId,
    lease_owner: &str,
    job: &PublicDispatchJob,
    projection: &dyn PublicProjectionPort,
) -> Result<bool, ErrorCode> {
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    set_tenant_local(&mut txn, tenant_id).await?;
    if !public_dispatch_live_lease(&mut txn, tenant_id, job, lease_owner).await? {
        return Ok(false);
    }
    match job.job_type.as_str() {
        "PUBLIC_RELEASE_APPLY" => {
            let release_id = payload_uuid(&job.payload, "release_id")?;
            admit_release_in_txn(&mut txn, tenant_id, release_id).await?;
        }
        "PUBLIC_REVOKE_APPLY" => {
            let release_id = payload_uuid(&job.payload, "release_id")?;
            apply_revocation_in_txn(&mut txn, tenant_id, release_id).await?;
        }
        "PUBLIC_PROJECT" => {
            let object_id = payload_uuid(&job.payload, "object_id")?;
            let object_kind = job
                .payload
                .get("object_kind")
                .and_then(Value::as_str)
                .ok_or(ErrorCode::InvalidInput)?;
            let revision = job
                .payload
                .get("object_revision")
                .and_then(Value::as_i64)
                .ok_or(ErrorCode::InvalidInput)?;
            let current =
                hydrate_current_in_txn(&mut txn, object_id, object_kind, revision).await?;
            let current_identity = current.as_ref().map(EligibleObject::identity);
            let historical =
                historical_projection_identities_in_txn(&mut txn, object_id, object_kind, revision)
                    .await?;
            // Every past evaluation at or before this event becomes a permanent tombstone unless
            // it is precisely the currently eligible identity. This handles a delayed old live
            // write even when a later revision remains eligible.
            for identity in historical
                .iter()
                .filter(|identity| current_identity.as_ref() != Some(*identity))
            {
                recheck_live_lease(&mut txn, tenant_id, job, lease_owner).await?;
                projection.retire(identity).await?;
            }
            if let Some(object) = current {
                recheck_live_lease(&mut txn, tenant_id, job, lease_owner).await?;
                if matches!(
                    projection.project_live(&object).await?,
                    ProjectionWriteOutcome::Superseded
                ) && hydrate_identity_in_txn(&mut txn, &object.identity())
                    .await?
                    .is_some()
                {
                    // A tombstone cannot coexist with a still-eligible exact identity. Leave the
                    // job retryable rather than accepting a stale external projection as DONE.
                    return Err(ErrorCode::Conflict);
                }
            }
        }
        _ => return Err(ErrorCode::InvalidInput),
    }
    let done: bool = sqlx::query_scalar("SELECT ops.complete_public_dispatch_job($1,$2,$3,$4)")
        .bind(tenant_id.0)
        .bind(job.job_id)
        .bind(lease_owner)
        .bind(job.attempt)
        .fetch_one(&mut *txn)
        .await
        .map_err(db_error)?;
    if !done {
        return Ok(false);
    }
    txn.commit().await.map_err(db_error)?;
    Ok(true)
}

/// Runs anonymous public dispatches through a global queue. Neither the claim result nor this
/// API accepts tenant identity; the protected queue functions retain the private association.
pub async fn run_anonymous_once(
    pool: &PublicWorkerDbPool,
    lease_owner: &str,
    limit: i64,
    projection: &dyn PublicProjectionPort,
) -> Result<u64, ErrorCode> {
    if limit <= 0 || lease_owner.trim().is_empty() {
        return Err(ErrorCode::InvalidInput);
    }
    let rows = sqlx::query(
        "SELECT dispatch_id,job_type,attempt,payload \
         FROM ops.claim_global_anonymous_public_dispatches($1,$2,$3)",
    )
    .bind(lease_owner)
    .bind(60.0)
    .bind(limit)
    .fetch_all(pool.pool())
    .await
    .map_err(db_error)?;
    let claimed = rows
        .iter()
        .map(AnonymousDispatchJob::from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let mut completed = 0;
    for job in claimed {
        match execute_anonymous_dispatch(pool, lease_owner, &job, projection).await {
            Ok(true) => completed += 1,
            Ok(false) => {}
            Err(error) => {
                let _ = fail_anonymous_dispatch(pool, &job, lease_owner).await;
                if matches!(error, ErrorCode::Forbidden | ErrorCode::TenantBoundary) {
                    return Err(error);
                }
            }
        }
    }
    Ok(completed)
}

async fn execute_anonymous_dispatch(
    pool: &PublicWorkerDbPool,
    lease_owner: &str,
    job: &AnonymousDispatchJob,
    projection: &dyn PublicProjectionPort,
) -> Result<bool, ErrorCode> {
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    if !anonymous_dispatch_live_lease(&mut txn, job, lease_owner).await? {
        return Ok(false);
    }
    match job.job_type.as_str() {
        "PUBLIC_ANONYMOUS_RELEASE_APPLY" => {
            admit_anonymous_in_txn(&mut txn, job, lease_owner).await?;
        }
        "PUBLIC_ANONYMOUS_REVOKE_APPLY" => {
            revoke_anonymous_in_txn(&mut txn, job, lease_owner).await?;
        }
        "PUBLIC_PROJECT" => {
            let object_id = payload_uuid(&job.payload, "object_id")?;
            let object_kind = job
                .payload
                .get("object_kind")
                .and_then(Value::as_str)
                .ok_or(ErrorCode::InvalidInput)?;
            let revision = job
                .payload
                .get("object_revision")
                .and_then(Value::as_i64)
                .ok_or(ErrorCode::InvalidInput)?;
            let current =
                hydrate_current_in_txn(&mut txn, object_id, object_kind, revision).await?;
            let current_identity = current.as_ref().map(EligibleObject::identity);
            let historical =
                historical_projection_identities_in_txn(&mut txn, object_id, object_kind, revision)
                    .await?;
            for identity in historical
                .iter()
                .filter(|identity| current_identity.as_ref() != Some(*identity))
            {
                recheck_anonymous_dispatch_lease(&mut txn, job, lease_owner).await?;
                projection.retire(identity).await?;
            }
            if let Some(object) = current {
                recheck_anonymous_dispatch_lease(&mut txn, job, lease_owner).await?;
                if matches!(
                    projection.project_live(&object).await?,
                    ProjectionWriteOutcome::Superseded
                ) && hydrate_identity_in_txn(&mut txn, &object.identity())
                    .await?
                    .is_some()
                {
                    return Err(ErrorCode::Conflict);
                }
            }
        }
        _ => return Err(ErrorCode::InvalidInput),
    }
    let done: bool =
        sqlx::query_scalar("SELECT ops.complete_global_anonymous_public_dispatch($1,$2,$3)")
            .bind(job.dispatch_id)
            .bind(lease_owner)
            .bind(job.attempt)
            .fetch_one(&mut *txn)
            .await
            .map_err(db_error)?;
    if !done {
        return Ok(false);
    }
    txn.commit().await.map_err(db_error)?;
    Ok(true)
}

async fn recheck_live_lease(
    txn: &mut Txn<'_>,
    tenant_id: TenantId,
    job: &PublicDispatchJob,
    lease_owner: &str,
) -> Result<(), ErrorCode> {
    // Qdrant cannot join this PostgreSQL transaction. Check the exact unexpired fence directly
    // before each remote mutation; `complete_in_txn` repeats it afterwards. If expiry races the
    // I/O, the database write rolls back and insert-only/tombstone semantics keep the external
    // projection safe until a fenced retry observes the authoritative current identity.
    if public_dispatch_live_lease(txn, tenant_id, job, lease_owner).await? {
        Ok(())
    } else {
        Err(ErrorCode::Conflict)
    }
}

async fn public_dispatch_live_lease(
    txn: &mut Txn<'_>,
    tenant_id: TenantId,
    job: &PublicDispatchJob,
    lease_owner: &str,
) -> Result<bool, ErrorCode> {
    sqlx::query_scalar("SELECT ops.public_dispatch_live_lease($1,$2,$3,$4)")
        .bind(tenant_id.0)
        .bind(job.job_id)
        .bind(lease_owner)
        .bind(job.attempt)
        .fetch_one(&mut **txn)
        .await
        .map_err(db_error)
}

async fn fail_public_dispatch_job(
    pool: &PublicWorkerDbPool,
    tenant_id: TenantId,
    job: &PublicDispatchJob,
    lease_owner: &str,
) -> Result<(), ErrorCode> {
    let _: Option<String> = sqlx::query_scalar(
        "SELECT ops.fail_public_dispatch_job($1,$2,$3,$4,'PUBLIC_RUNTIME',true,3,1.0)",
    )
    .bind(tenant_id.0)
    .bind(job.job_id)
    .bind(lease_owner)
    .bind(job.attempt)
    .fetch_one(pool.pool())
    .await
    .map_err(db_error)?;
    Ok(())
}

async fn anonymous_dispatch_live_lease(
    txn: &mut Txn<'_>,
    job: &AnonymousDispatchJob,
    lease_owner: &str,
) -> Result<bool, ErrorCode> {
    sqlx::query_scalar("SELECT ops.global_anonymous_public_dispatch_live_lease($1,$2,$3)")
        .bind(job.dispatch_id)
        .bind(lease_owner)
        .bind(job.attempt)
        .fetch_one(&mut **txn)
        .await
        .map_err(db_error)
}

async fn recheck_anonymous_dispatch_lease(
    txn: &mut Txn<'_>,
    job: &AnonymousDispatchJob,
    lease_owner: &str,
) -> Result<(), ErrorCode> {
    if anonymous_dispatch_live_lease(txn, job, lease_owner).await? {
        Ok(())
    } else {
        Err(ErrorCode::Conflict)
    }
}

async fn fail_anonymous_dispatch(
    pool: &PublicWorkerDbPool,
    job: &AnonymousDispatchJob,
    lease_owner: &str,
) -> Result<(), ErrorCode> {
    let _: Option<String> = sqlx::query_scalar(
        "SELECT ops.fail_global_anonymous_public_dispatch($1,$2,$3,'PUBLIC_RUNTIME',true,3,1.0)",
    )
    .bind(job.dispatch_id)
    .bind(lease_owner)
    .bind(job.attempt)
    .fetch_one(pool.pool())
    .await
    .map_err(db_error)?;
    Ok(())
}

async fn hydrate_identity_in_txn(
    txn: &mut Txn<'_>,
    expected: &ProjectionIdentity,
) -> Result<Option<EligibleObject>, ErrorCode> {
    sqlx::query(
        "SELECT object_id,object_kind,content,content::text AS canonical_body,object_revision, \
                current_evaluation_id,sha256(convert_to(content::text,'UTF8')) AS body_sha256 \
         FROM public.eligible_objects WHERE object_id=$1 AND object_kind=$2 AND object_revision=$3 \
           AND current_evaluation_id=$4 AND sha256(convert_to(content::text,'UTF8'))=$5",
    )
    .bind(expected.object_id)
    .bind(&expected.object_kind)
    .bind(expected.object_revision)
    .bind(expected.evaluation_id)
    .bind(expected.body_sha256.as_slice())
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?
    .map(eligible_from_row)
    .transpose()
}

async fn hydrate_current_in_txn(
    txn: &mut Txn<'_>,
    object_id: Uuid,
    object_kind: &str,
    expected_revision: i64,
) -> Result<Option<EligibleObject>, ErrorCode> {
    sqlx::query(
        "SELECT object_id,object_kind,content,content::text AS canonical_body,object_revision, \
                current_evaluation_id,sha256(convert_to(content::text,'UTF8')) AS body_sha256 \
         FROM public.eligible_objects WHERE object_id=$1 AND object_kind=$2 AND object_revision=$3",
    )
    .bind(object_id)
    .bind(object_kind)
    .bind(expected_revision)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?
    .map(eligible_from_row)
    .transpose()
}

async fn historical_projection_identities_in_txn(
    txn: &mut Txn<'_>,
    object_id: Uuid,
    object_kind: &str,
    through_revision: i64,
) -> Result<Vec<ProjectionIdentity>, ErrorCode> {
    let kind = match object_kind {
        "CLAIM" => "CLAIM",
        "SYNTHESIS" => "SYNTHESIS",
        _ => return Err(ErrorCode::InvalidInput),
    };
    let rows = sqlx::query(
        "SELECT object_id,object_revision,evaluation_id,body_sha256 \
         FROM public.public_projection_identities($1,$2,$3)",
    )
    .bind(object_id)
    .bind(kind)
    .bind(through_revision)
    .fetch_all(&mut **txn)
    .await
    .map_err(db_error)?;
    rows.into_iter()
        .map(|row| {
            let body_sha256: Vec<u8> = row.try_get("body_sha256").map_err(db_error)?;
            Ok(ProjectionIdentity {
                object_id: row.try_get("object_id").map_err(db_error)?,
                object_kind: kind.to_owned(),
                object_revision: row.try_get("object_revision").map_err(db_error)?,
                evaluation_id: row.try_get("evaluation_id").map_err(db_error)?,
                body_sha256: body_sha256.try_into().map_err(|_| ErrorCode::Internal)?,
            })
        })
        .collect()
}

fn payload_uuid(payload: &Value, key: &str) -> Result<Uuid, ErrorCode> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .ok_or(ErrorCode::InvalidInput)?
        .parse()
        .map_err(|_| ErrorCode::InvalidInput)
}
