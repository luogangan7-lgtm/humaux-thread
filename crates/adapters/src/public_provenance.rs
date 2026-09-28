//! `adapters::public_provenance` — Bounded §42 reconciliation of the direct public graph against its derived current
//!   closure.
//! Depends-on: crates=[humaux-domain, sqlx, uuid]; services=[PostgreSQL(role_public_worker) r=[public.claims,
//!   public.provenance_edges, public.source_closure, public.sources, public.syntheses, public.synthesis_inputs]
//!   x=[public.current_public_roots]]; env=[]; modules=[adapters::postgres, domain::error, domain::ids]
//! Called-by: [tests]
//! Invariants: [reads the public provenance graph on role_public_worker only; an unknown target is NotFound and a
//!   malformed one InvalidInput; a PG error is DependencyUnavailable, never an empty closure]
//! Spec: none

use std::sync::atomic::{AtomicU64, Ordering};

use humaux_domain::{error::ErrorCode, ids::TenantId};
use sqlx::Row;
use uuid::Uuid;

use crate::postgres::PublicWorkerDbPool;

static PUBLIC_PROVENANCE_ORPHANS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Exactly one public target. This keeps a probe bounded and avoids an accidental pool-wide scan.
#[derive(Clone, Copy, Debug)]
pub struct PublicProvenanceTarget {
    claim_id: Option<Uuid>,
    synthesis_id: Option<Uuid>,
}

impl PublicProvenanceTarget {
    pub fn new(claim_id: Option<Uuid>, synthesis_id: Option<Uuid>) -> Result<Self, ErrorCode> {
        if (claim_id.is_some() == synthesis_id.is_some())
            || claim_id.is_some_and(|id| id.is_nil())
            || synthesis_id.is_some_and(|id| id.is_nil())
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            claim_id,
            synthesis_id,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicProvenanceProbe {
    pub orphaned: bool,
    pub direct_root_count: u64,
    pub closure_root_count: u64,
    /// The derived closure retained a root but not its shortest direct-graph depth.
    pub closure_depth_drift: bool,
    /// A discovered root violates the USER_CONTRIBUTION <-> release identity shape.
    /// This is structural only: revoked roots remain roots for impact analysis.
    pub typed_root_invalid: bool,
}

pub fn public_provenance_orphans_total() -> u64 {
    PUBLIC_PROVENANCE_ORPHANS_TOTAL.load(Ordering::Relaxed)
}

/// Reconciles authoritative direct-graph roots with `source_closure` for one target.
/// Empty roots are unhealthy: they cover no-root and invalid cyclic/unrooted graph outcomes.
pub async fn probe_public_provenance(
    pool: &PublicWorkerDbPool,
    tenant_id: TenantId,
    target: PublicProvenanceTarget,
) -> Result<PublicProvenanceProbe, ErrorCode> {
    // dep: PostgreSQL(role_public_worker) — transaction entry for `probe_public_provenance`
    let mut txn = pool
        .pool()
        .begin()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    sqlx::query("SELECT set_config('humaux.tenant_id',$1,true)")
        .bind(tenant_id.0.to_string())
        .execute(&mut *txn)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let exists: bool = sqlx::query_scalar(
        "SELECT CASE WHEN $1 IS NOT NULL THEN EXISTS(SELECT 1 FROM public.claims WHERE claim_id=$1) \
          ELSE EXISTS(SELECT 1 FROM public.syntheses WHERE synthesis_id=$2) END",
    )
    .bind(target.claim_id)
    .bind(target.synthesis_id)
    .fetch_one(&mut *txn)
    .await
    .map_err(|_| ErrorCode::DependencyUnavailable)?;
    if !exists {
        return Err(ErrorCode::NotFound);
    }
    let row = sqlx::query(
        "WITH RECURSIVE nodes(kind,id,path,depth,cycle) AS ( \
           SELECT CASE WHEN $1 IS NOT NULL THEN 'C' ELSE 'S' END, COALESCE($1,$2), \
             ARRAY[COALESCE($1,$2)], 1, false WHERE ($1 IS NULL)<>($2 IS NULL) \
           UNION ALL \
           SELECT CASE WHEN i.claim_id IS NOT NULL THEN 'C' ELSE 'S' END, \
             COALESCE(i.claim_id,i.input_synthesis_id), n.path||COALESCE(i.claim_id,i.input_synthesis_id), \
             n.depth+1, COALESCE(i.claim_id,i.input_synthesis_id)=ANY(n.path) \
           FROM nodes n JOIN public.synthesis_inputs i ON n.kind='S' AND i.synthesis_id=n.id \
           WHERE NOT n.cycle \
         ), direct AS (SELECT root_source_id FROM public.current_public_roots($1,$2)), \
              direct_depth AS (SELECT p.source_id AS root_source_id,min(n.depth)::integer AS depth \
                FROM nodes n JOIN public.provenance_edges p ON n.kind='C' AND n.id=p.claim_id \
                WHERE NOT EXISTS (SELECT 1 FROM nodes x WHERE x.cycle \
                  OR (x.kind='C' AND NOT EXISTS(SELECT 1 FROM public.provenance_edges e WHERE e.claim_id=x.id)) \
                  OR (x.kind='S' AND NOT EXISTS(SELECT 1 FROM public.synthesis_inputs i WHERE i.synthesis_id=x.id))) \
                GROUP BY p.source_id), \
              closure AS (SELECT root_source_id FROM public.source_closure \
                WHERE is_current AND claim_id IS NOT DISTINCT FROM $1 \
                  AND synthesis_id IS NOT DISTINCT FROM $2), \
              closure_depth AS (SELECT root_source_id,depth FROM public.source_closure \
                WHERE is_current AND claim_id IS NOT DISTINCT FROM $1 \
                  AND synthesis_id IS NOT DISTINCT FROM $2) \
         SELECT (SELECT count(*) FROM direct)::bigint AS direct_count, \
                (SELECT count(*) FROM closure)::bigint AS closure_count, \
                EXISTS((SELECT root_source_id FROM direct EXCEPT SELECT root_source_id FROM closure) \
                  UNION ALL (SELECT root_source_id FROM closure EXCEPT SELECT root_source_id FROM direct)) AS drift, \
                EXISTS((SELECT root_source_id,depth FROM direct_depth EXCEPT SELECT root_source_id,depth FROM closure_depth) \
                  UNION ALL (SELECT root_source_id,depth FROM closure_depth EXCEPT SELECT root_source_id,depth FROM direct_depth)) \
                  AS closure_depth_drift, \
                EXISTS(SELECT 1 FROM direct d JOIN public.sources s ON s.source_id=d.root_source_id \
                  WHERE (s.source_type='USER_CONTRIBUTION') <> (s.contribution_release_id IS NOT NULL)) \
                  AS typed_root_invalid",
    )
    .bind(target.claim_id).bind(target.synthesis_id)
    .fetch_one(&mut *txn).await.map_err(|_| ErrorCode::DependencyUnavailable)?;
    txn.commit()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let direct: i64 = row
        .try_get("direct_count")
        .map_err(|_| ErrorCode::Internal)?;
    let closure: i64 = row
        .try_get("closure_count")
        .map_err(|_| ErrorCode::Internal)?;
    let drift: bool = row.try_get("drift").map_err(|_| ErrorCode::Internal)?;
    let closure_depth_drift: bool = row
        .try_get("closure_depth_drift")
        .map_err(|_| ErrorCode::Internal)?;
    let typed_root_invalid: bool = row
        .try_get("typed_root_invalid")
        .map_err(|_| ErrorCode::Internal)?;
    let orphaned =
        direct == 0 || closure == 0 || drift || closure_depth_drift || typed_root_invalid;
    if orphaned {
        PUBLIC_PROVENANCE_ORPHANS_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    Ok(PublicProvenanceProbe {
        orphaned,
        direct_root_count: direct as u64,
        closure_root_count: closure as u64,
        closure_depth_drift,
        typed_root_invalid,
    })
}
