//! `adapters::contribution_repo` — §12/§13 physical IO for public contributions.
//! Depends-on: crates=[humaux-domain, serde_json, sqlx]; services=[PostgreSQL(any)
//!   r=[control.anonymous_source_lineage, private.evidence_objects, private.memory_records, public.sources,
//!   public.syntheses, public.synthesis_inputs, staging.sanitized_public_candidates] w=[ops.outbox, public.claims,
//!   public.provenance_edges, public.source_closure, staging.contribution_release_sources,
//!   staging.contribution_releases] x=[staging.assert_active_release_rights, staging.lock_contribution_release]];
//!   env=[]; modules=[adapters::postgres, adapters::remember, domain::error, domain::ids, domain::public]
//! Called-by: [tests]
//! Invariants: [private releases are written only on role_private_worker and public promotion only on
//!   role_public_worker (§12/§13); there is no raw-pool entry point; tenant or visibility failures are
//!   TenantBoundary/NotFound]
//! Spec: Baseline §12; §13
//!
//! The typed pools are part of this module's contract: private releases are written only by
//! `role_private_worker`, while public promotion and closure work are written only by
//! `role_public_worker` (§12/§13).  There is deliberately no raw-pool entry point.

use std::collections::{BTreeSet, HashMap, HashSet};

use humaux_domain::{
    error::ErrorCode,
    ids::TenantId,
    public::{ContributionRelease, ModerationState, ReleaseSource},
};
use serde_json::json;
use sqlx::{Row, types::Uuid};

use crate::{
    postgres::{PrivateWorkerDbPool, PublicWorkerDbPool},
    remember,
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

/// Creates one explicit private release and its corresponding `PUBLIC_RELEASE` outbox row in
/// the same transaction (§12.1, §13).  The database validates source visibility under RLS.
pub async fn create_release(
    pool: &PrivateWorkerDbPool,
    tenant_id: TenantId,
    release: &ContributionRelease,
) -> Result<Uuid, ErrorCode> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    set_tenant_local(&mut txn, tenant_id).await?;

    let mut seen = BTreeSet::new();
    for source in release.sources() {
        let (kind, id) = match source {
            ReleaseSource::Evidence(id) => ("evidence", id.0),
            ReleaseSource::Memory(id) => ("memory", id.0),
        };
        if !seen.insert((kind, id)) {
            return Err(ErrorCode::InvalidInput);
        }
        let visible = match source {
            ReleaseSource::Evidence(_) => {
                sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS (SELECT 1 FROM private.evidence_objects \
                 WHERE tenant_id = $1 AND evidence_id = $2)",
                )
                .bind(tenant_id.0)
                .bind(id)
                .fetch_one(&mut *txn)
                .await
            }
            ReleaseSource::Memory(_) => {
                sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS (SELECT 1 FROM private.memory_records \
                 WHERE tenant_id = $1 AND memory_id = $2)",
                )
                .bind(tenant_id.0)
                .bind(id)
                .fetch_one(&mut *txn)
                .await
            }
        }
        .map_err(db_error)?;
        if !visible {
            return Err(ErrorCode::TenantBoundary);
        }
    }

    let policy_snapshot = json!({
        "policy": release.policy().as_db_str(),
        "consent_version": release.consent_version(),
    });
    let release_id: Uuid = sqlx::query_scalar(
        "INSERT INTO staging.contribution_releases \
         (tenant_id, policy_snapshot, privacy_scan_outcome, secret_scan_outcome, rights_basis, \
          source_license, publisher, contributor_attestation, redistribution_policy) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         RETURNING contribution_release_id",
    )
    .bind(tenant_id.0)
    .bind(policy_snapshot)
    .bind(release.privacy_scan().as_db_str())
    .bind(release.secret_scan().as_db_str())
    .bind(release.rights().rights_basis())
    .bind(release.rights().source_license())
    .bind(release.rights().publisher())
    .bind(release.rights().contributor_attestation())
    .bind(release.rights().redistribution_policy())
    .fetch_one(&mut *txn)
    .await
    .map_err(db_error)?;

    for (ordinal, source) in release.sources().iter().enumerate() {
        let (evidence_id, memory_id) = match source {
            ReleaseSource::Evidence(id) => (Some(id.0), None),
            ReleaseSource::Memory(id) => (None, Some(id.0)),
        };
        sqlx::query(
            "INSERT INTO staging.contribution_release_sources \
             (tenant_id, contribution_release_id, evidence_id, memory_id, ordinal) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(tenant_id.0)
        .bind(release_id)
        .bind(evidence_id)
        .bind(memory_id)
        .bind(ordinal as i32)
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;
    }

    let commit_seq = remember::next_commit_seq(&mut txn)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    sqlx::query(
        "INSERT INTO ops.outbox (tenant_id, commit_seq, event_type, contribution_release_id) \
         VALUES ($1, $2, 'PUBLIC_RELEASE', $3)",
    )
    .bind(tenant_id.0)
    .bind(commit_seq)
    .bind(release_id)
    .execute(&mut *txn)
    .await
    .map_err(db_error)?;

    txn.commit().await.map_err(db_error)?;
    Ok(release_id)
}

/// Revokes an active release once.  Repeated calls return `false` and do not create another
/// outbox event (§13); the SQL lock helper serializes concurrent revocations.
pub async fn revoke_release(
    pool: &PrivateWorkerDbPool,
    tenant_id: TenantId,
    release_id: Uuid,
) -> Result<bool, ErrorCode> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    set_tenant_local(&mut txn, tenant_id).await?;
    sqlx::query("SELECT staging.lock_contribution_release($1, true)")
        .bind(release_id)
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;
    let state: Option<String> = sqlx::query_scalar(
        "SELECT state FROM staging.contribution_releases \
         WHERE tenant_id = $1 AND contribution_release_id = $2 FOR UPDATE",
    )
    .bind(tenant_id.0)
    .bind(release_id)
    .fetch_optional(&mut *txn)
    .await
    .map_err(db_error)?;
    let Some(state) = state else {
        return Err(ErrorCode::NotFound);
    };
    if state == "REVOKED" {
        txn.commit().await.map_err(db_error)?;
        return Ok(false);
    }
    let changed = sqlx::query(
        "UPDATE staging.contribution_releases \
         SET state = 'REVOKED', revoked_at = now() \
         WHERE tenant_id = $1 AND contribution_release_id = $2 AND state = 'ACTIVE'",
    )
    .bind(tenant_id.0)
    .bind(release_id)
    .execute(&mut *txn)
    .await
    .map_err(db_error)?
    .rows_affected();
    if changed == 0 {
        txn.commit().await.map_err(db_error)?;
        return Ok(false);
    }
    let commit_seq = remember::next_commit_seq(&mut txn)
        .await
        .map_err(|_| ErrorCode::Internal)?;
    let anonymous = sqlx::query(
        "SELECT l.anonymous_source_id,c.envelope_sha256 \
         FROM control.anonymous_source_lineage l \
         JOIN staging.sanitized_public_candidates c \
           ON c.tenant_id=l.tenant_id AND c.anonymous_source_id=l.anonymous_source_id \
         WHERE l.contribution_release_id=$1",
    )
    .bind(release_id)
    .fetch_optional(&mut *txn)
    .await
    .map_err(db_error)?;
    if let Some(binding) = anonymous {
        let anonymous_source_id: Uuid = binding.try_get("anonymous_source_id").map_err(db_error)?;
        let envelope_sha256: Vec<u8> = binding.try_get("envelope_sha256").map_err(db_error)?;
        sqlx::query(
            "INSERT INTO ops.outbox(tenant_id,commit_seq,event_type,anonymous_source_id,candidate_envelope_sha256,anonymous_source_revision) \
             VALUES($1,$2,'PUBLIC_ANONYMOUS_REVOKE',$3,$4,2)",
        )
        .bind(tenant_id.0)
        .bind(commit_seq)
        .bind(anonymous_source_id)
        .bind(envelope_sha256)
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;
    } else {
        sqlx::query(
            "INSERT INTO ops.outbox(tenant_id,commit_seq,event_type,contribution_release_id) \
             VALUES($1,$2,'PUBLIC_REVOKE',$3)",
        )
        .bind(tenant_id.0)
        .bind(commit_seq)
        .bind(release_id)
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;
    }
    txn.commit().await.map_err(db_error)?;
    Ok(true)
}

/// Promotes validated public sources into a `PUBLIC_STAGING` claim (§12).  Release locks are
/// acquired in UUID order before source rows are locked and rechecked, preventing revocation
/// from racing admission.
pub async fn promote_claim(
    pool: &PublicWorkerDbPool,
    tenant_id: TenantId,
    content: &serde_json::Value,
    source_ids: &[Uuid],
) -> Result<Uuid, ErrorCode> {
    if source_ids.is_empty() || !content.is_object() {
        return Err(ErrorCode::InvalidInput);
    }
    let source_ids: Vec<_> = source_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    set_tenant_local(&mut txn, tenant_id).await?;

    let releases: Vec<Option<Uuid>> =
        sqlx::query("SELECT contribution_release_id FROM public.sources WHERE source_id = ANY($1)")
            .bind(&source_ids)
            .fetch_all(&mut *txn)
            .await
            .map_err(db_error)?
            .into_iter()
            .map(|row| row.try_get("contribution_release_id").map_err(db_error))
            .collect::<Result<_, _>>()?;
    if releases.len() != source_ids.len() {
        return Err(ErrorCode::NotFound);
    }
    let release_ids: BTreeSet<_> = releases.into_iter().flatten().collect();
    for release_id in release_ids {
        sqlx::query("SELECT staging.lock_contribution_release($1, false)")
            .bind(release_id)
            .execute(&mut *txn)
            .await
            .map_err(db_error)?;
    }

    let rows = sqlx::query(
        "SELECT source_id, source_type, contribution_release_id \
         FROM public.sources WHERE source_id = ANY($1) FOR UPDATE",
    )
    .bind(&source_ids)
    .fetch_all(&mut *txn)
    .await
    .map_err(db_error)?;
    if rows.len() != source_ids.len() {
        return Err(ErrorCode::NotFound);
    }
    for row in rows {
        let source_type: String = row.try_get("source_type").map_err(db_error)?;
        let release_id: Option<Uuid> = row.try_get("contribution_release_id").map_err(db_error)?;
        if source_type == "USER_CONTRIBUTION" {
            let release_id = release_id.ok_or(ErrorCode::InvalidInput)?;
            // §6.2.2 anonymous boundary: this pool runs as `role_public_worker`, which has no
            // table-level SELECT on `staging.contribution_releases` (spec pins that cell to
            // `—`). Go through the narrow SECURITY DEFINER lookup added by migration 0139 —
            // it re-establishes the tenant check the DEFINER context bypasses in RLS.
            sqlx::query("SELECT staging.assert_active_release_rights($1)")
                .bind(release_id)
                .execute(&mut *txn)
                .await
                .map_err(db_error)?;
        } else if release_id.is_some() {
            return Err(ErrorCode::InvalidInput);
        }
    }

    let claim_id: Uuid = sqlx::query_scalar(
        "INSERT INTO public.claims (content, moderation_state) VALUES ($1, 'PUBLIC_STAGING') \
         RETURNING claim_id",
    )
    .bind(content)
    .fetch_one(&mut *txn)
    .await
    .map_err(db_error)?;
    for source_id in &source_ids {
        sqlx::query("INSERT INTO public.provenance_edges (claim_id, source_id) VALUES ($1, $2)")
            .bind(claim_id)
            .bind(source_id)
            .execute(&mut *txn)
            .await
            .map_err(db_error)?;
        sqlx::query(
            "INSERT INTO public.source_closure (claim_id, root_source_id, depth, is_current) \
             VALUES ($1, $2, 1, true) \
             ON CONFLICT (claim_id, root_source_id) WHERE claim_id IS NOT NULL \
             DO UPDATE SET depth = EXCLUDED.depth, is_current = true, computed_at = now()",
        )
        .bind(claim_id)
        .bind(source_id)
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;
    }
    txn.commit().await.map_err(db_error)?;
    Ok(claim_id)
}

/// Recomputes the complete, minimum-depth source closure for a synthesis (§12.3).  It locks
/// every contributing table before its first query in repeatable-read, validates the entire
/// graph, and only then marks the previous closure stale and upserts the new projection.
pub async fn recompute_source_closure(
    pool: &PublicWorkerDbPool,
    synthesis_id: Uuid,
) -> Result<u64, ErrorCode> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;
    sqlx::query(
        "LOCK TABLE public.provenance_edges, public.sources, public.synthesis_inputs IN SHARE MODE",
    )
    .execute(&mut *txn)
    .await
    .map_err(db_error)?;
    sqlx::query("LOCK TABLE public.source_closure IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;

    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM public.syntheses WHERE synthesis_id = $1)",
    )
    .bind(synthesis_id)
    .fetch_one(&mut *txn)
    .await
    .map_err(db_error)?;
    if !exists {
        return Err(ErrorCode::NotFound);
    }
    let inputs = sqlx::query(
        "WITH RECURSIVE reachable(synthesis_id) AS ( \
           SELECT $1::uuid UNION \
           SELECT i.input_synthesis_id FROM public.synthesis_inputs i \
           JOIN reachable r USING (synthesis_id) WHERE i.input_synthesis_id IS NOT NULL \
         ) SELECT i.synthesis_id, i.claim_id, i.input_synthesis_id \
           FROM public.synthesis_inputs i JOIN reachable r USING (synthesis_id)",
    )
    .bind(synthesis_id)
    .fetch_all(&mut *txn)
    .await
    .map_err(db_error)?;
    let mut synthesis_inputs = SynthesisInputs::new();
    let mut claim_ids = BTreeSet::new();
    for row in inputs {
        let claim_id = row.try_get("claim_id").map_err(db_error)?;
        if let Some(id) = claim_id {
            claim_ids.insert(id);
        }
        synthesis_inputs
            .entry(row.try_get("synthesis_id").map_err(db_error)?)
            .or_default()
            .push((
                claim_id,
                row.try_get("input_synthesis_id").map_err(db_error)?,
            ));
    }
    let edges = sqlx::query(
        "SELECT claim_id, source_id FROM public.provenance_edges WHERE claim_id = ANY($1)",
    )
    .bind(claim_ids.into_iter().collect::<Vec<Uuid>>())
    .fetch_all(&mut *txn)
    .await
    .map_err(db_error)?;
    let mut claim_sources: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for row in edges {
        claim_sources
            .entry(row.try_get("claim_id").map_err(db_error)?)
            .or_default()
            .push(row.try_get("source_id").map_err(db_error)?);
    }

    let roots = minimum_source_depths(synthesis_id, &synthesis_inputs, &claim_sources)?;
    sqlx::query("UPDATE public.source_closure SET is_current = false WHERE synthesis_id = $1 AND is_current")
        .bind(synthesis_id).execute(&mut *txn).await.map_err(db_error)?;
    for (source_id, depth) in &roots {
        sqlx::query(
            "INSERT INTO public.source_closure (synthesis_id, root_source_id, depth, is_current) \
             VALUES ($1, $2, $3, true) \
             ON CONFLICT (synthesis_id, root_source_id) WHERE synthesis_id IS NOT NULL \
             DO UPDATE SET depth = EXCLUDED.depth, is_current = true, computed_at = now()",
        )
        .bind(synthesis_id)
        .bind(source_id)
        .bind(depth)
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;
    }
    let count = roots.len() as u64;
    txn.commit().await.map_err(db_error)?;
    Ok(count)
}

type SynthesisInputs = HashMap<Uuid, Vec<(Option<Uuid>, Option<Uuid>)>>;

fn minimum_source_depths(
    synthesis_id: Uuid,
    synthesis_inputs: &SynthesisInputs,
    claim_sources: &HashMap<Uuid, Vec<Uuid>>,
) -> Result<HashMap<Uuid, i32>, ErrorCode> {
    // Post-order memoization evaluates each reachable synthesis once. A path-cloning
    // traversal would expand every route through a layered diamond exponentially.
    let mut calculated: HashMap<Uuid, HashMap<Uuid, i32>> = HashMap::new();
    let mut visiting = HashSet::new();
    let mut stack = vec![(synthesis_id, false)];
    while let Some((id, expanded)) = stack.pop() {
        if calculated.contains_key(&id) {
            continue;
        }
        let children = synthesis_inputs.get(&id).ok_or(ErrorCode::InvalidInput)?;
        if !expanded {
            if !visiting.insert(id) {
                return Err(ErrorCode::InvalidInput);
            }
            stack.push((id, true));
            for (_, child_id) in children {
                if let Some(child_id) = child_id {
                    stack.push((*child_id, false));
                }
            }
            continue;
        }
        let mut node_roots: HashMap<Uuid, i32> = HashMap::new();
        for (claim_id, child_id) in children {
            match (claim_id, child_id) {
                (Some(claim_id), None) => {
                    let sources = claim_sources.get(claim_id).ok_or(ErrorCode::InvalidInput)?;
                    for source in sources {
                        // Root -> claim is 1; claim -> this synthesis adds 1.
                        node_roots
                            .entry(*source)
                            .and_modify(|d| *d = (*d).min(2))
                            .or_insert(2);
                    }
                }
                (None, Some(child_id)) => {
                    let sources = calculated.get(child_id).ok_or(ErrorCode::InvalidInput)?;
                    for (source, depth) in sources {
                        let depth = depth.checked_add(1).ok_or(ErrorCode::InvalidInput)?;
                        node_roots
                            .entry(*source)
                            .and_modify(|d| *d = (*d).min(depth))
                            .or_insert(depth);
                    }
                }
                _ => return Err(ErrorCode::InvalidInput),
            }
        }
        if node_roots.is_empty() {
            return Err(ErrorCode::InvalidInput);
        }
        visiting.remove(&id);
        calculated.insert(id, node_roots);
    }
    let roots = calculated
        .remove(&synthesis_id)
        .ok_or(ErrorCode::InvalidInput)?;
    if roots.is_empty() {
        return Err(ErrorCode::InvalidInput);
    }
    Ok(roots)
}

/// Legacy moderation facade is closed; use [`crate::public_repo::evaluate_claim`], which
/// requires the tenant, expected revision, authorization and trust receipt.
pub async fn set_moderation_state(
    _pool: &PublicWorkerDbPool,
    _claim_id: Uuid,
    _state: ModerationState,
) -> Result<(), ErrorCode> {
    Err(ErrorCode::Forbidden)
}
