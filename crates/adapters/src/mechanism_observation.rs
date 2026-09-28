//! `adapters::mechanism_observation` — Actual §1.14 runtime observations and immutable E2E execution receipts.
//! Depends-on: crates=[humaux-contracts, humaux-domain, humaux-infra-cell, serde_json, sha2, sqlx];
//!   services=[PostgreSQL(any) r=[public.claim_trust_evaluations, public.claims, public.consensus_ready,
//!   public.corroborated, public.eligible_objects] w=[ops.mechanism_e2e_runs, ops.mechanism_observations]]; env=[];
//!   modules=[adapters::postgres, adapters::public_repo, contracts::mechanism_registry, domain::error,
//!   domain::identity, infra-cell::resource]
//! Called-by: [admin::mechanism, tests, xtask::mechanism_registry]
//! Invariants: [reads use the dedicated read-only identity; recording runs on role_maintenance through an existing
//!   authorized operation; no SQL or shell executor is exposed; bad input is InvalidInput]
//! Spec: none
//!
//! Reading uses the dedicated read-only identity. Recording uses maintenance and
//! calls an existing authorized business operation; it exposes no SQL/shell executor.

use crate::postgres::{AdminDbPool, MaintenanceDbPool, PublicWorkerDbPool};
use crate::public_repo::{EvaluateClaim, EvaluationResult};
use humaux_contracts::mechanism_registry::{
    DerivedMechanismStatus, MechanismE2eEvidence, MechanismObservation, MechanismSpec,
    MechanismStatus, ObservationTarget, derive_status, parse_registry,
};
use humaux_domain::{error::ErrorCode, identity::AuthorizationScope};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{
    Row,
    postgres::PgRow,
    types::{Uuid, time::OffsetDateTime},
};
use std::collections::BTreeMap;

const SPEC_TEXT: &str = include_str!("../../../docs/architecture/Baseline_2.9.md");

fn db_error(_: sqlx::Error) -> ErrorCode {
    ErrorCode::Internal
}

fn uuid(value: &str) -> Result<Uuid, ErrorCode> {
    Uuid::parse_str(value).map_err(|_| ErrorCode::InvalidInput)
}

fn micros(value: OffsetDateTime) -> Result<i64, ErrorCode> {
    i64::try_from(value.unix_timestamp_nanos() / 1_000).map_err(|_| ErrorCode::Internal)
}

fn observation(row: &PgRow) -> Result<MechanismObservation, ErrorCode> {
    Ok(MechanismObservation {
        observation_id: row
            .try_get::<Uuid, _>("observation_id")
            .map_err(db_error)?
            .to_string(),
        target: ObservationTarget {
            deployment_id: row
                .try_get::<Uuid, _>("deployment_id")
                .map_err(db_error)?
                .to_string(),
            cell_id: row
                .try_get::<Uuid, _>("cell_id")
                .map_err(db_error)?
                .to_string(),
        },
        mechanism_id: row.try_get("mechanism_id").map_err(db_error)?,
        value: row.try_get("value").map_err(db_error)?,
        scanned_n: row.try_get("scanned_n").map_err(db_error)?,
        measured_at_micros: micros(row.try_get("measured_at").map_err(db_error)?)?,
        recorded_status: MechanismStatus::parse(
            &row.try_get::<String, _>("derived_status")
                .map_err(db_error)?,
        )
        .map_err(|_| ErrorCode::Internal)?,
        probe_version: row.try_get("probe_version").map_err(db_error)?,
        binary_build: row.try_get("binary_build").map_err(db_error)?,
        scope_hash: row.try_get("scope_hash").map_err(db_error)?,
    })
}

/// A consistent read of the requested deployment/cell, never of a fallback target.
#[derive(Clone, Debug)]
pub struct RuntimeObservations {
    /// Scope supplied by the caller and included in both database queries.
    pub target: ObservationTarget,
    /// Database clock at the end of the read, in Unix microseconds.
    pub checked_at_micros: i64,
    /// Latest row per canonical mechanism ID, including stale rows.
    pub latest: BTreeMap<String, MechanismObservation>,
    /// Completed runs whose `after` is the selected latest observation.
    pub evidence: BTreeMap<String, MechanismE2eEvidence>,
}

impl RuntimeObservations {
    /// Recompute, rather than trust the stored `derived_status` cache.
    pub fn status(&self, spec: &MechanismSpec) -> DerivedMechanismStatus {
        let id = spec.id();
        derive_status(
            spec,
            &self.target,
            self.latest.get(&id),
            self.evidence.get(&id),
            self.checked_at_micros,
        )
    }

    /// Operational output keeps absence and stale evidence visible and reports the
    /// exact target. This is a registry view, not a §4.4 scalar probe envelope.
    pub fn render_json(&self, specs: &[MechanismSpec]) -> Value {
        json!({"deployment_id": self.target.deployment_id, "cell_id": self.target.cell_id,
            "checked_at_micros": self.checked_at_micros,
            "mechanisms": specs.iter().map(|spec| {
                let derived = self.status(spec);
                json!({"mechanism_id": spec.id(), "status": derived.status.map(MechanismStatus::as_str),
                    "reason": derived.reason, "observation": self.latest.get(&spec.id()),
                    "e2e_run_id": self.evidence.get(&spec.id()).map(|e| &e.run_id)})
            }).collect::<Vec<_>>()})
    }

    /// A stale mechanism cannot establish runtime readiness. A fresh stored cache
    /// contradicting the recomputed result also fails G5 rather than being ignored.
    pub fn cannot_establish(&self, specs: &[MechanismSpec]) -> bool {
        specs.iter().any(|spec| {
            let derived = self.status(spec);
            derived.status == Some(MechanismStatus::Stale)
                || self.latest.get(&spec.id()).is_some_and(|obs| {
                    derived
                        .status
                        .is_some_and(|status| status != obs.recorded_status)
                })
        })
    }
}

/// Read actual PostgreSQL state in one read-only repeatable-read snapshot.
/// Missing rows stay missing. Missing tables, bad credentials and invalid targets
/// return an error; none is rewritten to an empty successful result or “not deployed”.
pub async fn read_target(
    pool: &AdminDbPool,
    target: &ObservationTarget,
) -> Result<RuntimeObservations, ErrorCode> {
    let deployment_id = uuid(&target.deployment_id)?;
    let cell_id = uuid(&target.cell_id)?;
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;
    let rows = sqlx::query(
        "SELECT DISTINCT ON (mechanism_id) * FROM ops.mechanism_observations \
         WHERE deployment_id=$1 AND cell_id=$2 \
         ORDER BY mechanism_id, measured_at DESC, created_at DESC, observation_id DESC",
    )
    .bind(deployment_id)
    .bind(cell_id)
    .fetch_all(&mut *txn)
    .await
    .map_err(db_error)?;
    let mut latest = BTreeMap::new();
    for row in rows {
        let obs = observation(&row)?;
        latest.insert(obs.mechanism_id.clone(), obs);
    }
    let after_ids: Vec<Uuid> = latest
        .values()
        .map(|obs| uuid(&obs.observation_id))
        .collect::<Result<_, _>>()?;
    let rows = sqlx::query(
        "SELECT b.*, r.run_id, r.deployment_id AS run_deployment_id, r.cell_id AS run_cell_id, \
           r.mechanism_id AS run_mechanism_id, r.after_observation_id, \
           r.probe_version AS run_probe_version, r.binary_build AS run_binary_build, r.scope_hash AS run_scope_hash, \
           r.started_at, r.completed_at \
         FROM ops.mechanism_e2e_runs r JOIN ops.mechanism_observations b \
           ON b.observation_id=r.before_observation_id \
         WHERE r.deployment_id=$1 AND r.cell_id=$2 AND r.after_observation_id=ANY($3)")
        .bind(deployment_id).bind(cell_id).bind(&after_ids).fetch_all(&mut *txn).await.map_err(db_error)?;
    let mut evidence = BTreeMap::new();
    for row in rows {
        let run = MechanismE2eEvidence {
            run_id: row
                .try_get::<Uuid, _>("run_id")
                .map_err(db_error)?
                .to_string(),
            target: ObservationTarget {
                deployment_id: row
                    .try_get::<Uuid, _>("run_deployment_id")
                    .map_err(db_error)?
                    .to_string(),
                cell_id: row
                    .try_get::<Uuid, _>("run_cell_id")
                    .map_err(db_error)?
                    .to_string(),
            },
            mechanism_id: row.try_get("run_mechanism_id").map_err(db_error)?,
            before: observation(&row)?,
            after_observation_id: row
                .try_get::<Uuid, _>("after_observation_id")
                .map_err(db_error)?
                .to_string(),
            scope_hash: row.try_get("run_scope_hash").map_err(db_error)?,
            probe_version: row.try_get("run_probe_version").map_err(db_error)?,
            binary_build: row.try_get("run_binary_build").map_err(db_error)?,
            started_at_micros: micros(row.try_get("started_at").map_err(db_error)?)?,
            completed_at_micros: micros(row.try_get("completed_at").map_err(db_error)?)?,
        };
        evidence.insert(run.mechanism_id.clone(), run);
    }
    let checked_at: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut *txn)
        .await
        .map_err(db_error)?;
    txn.commit().await.map_err(db_error)?;
    Ok(RuntimeObservations {
        target: target.clone(),
        checked_at_micros: micros(checked_at)?,
        latest,
        evidence,
    })
}

/// Actual public-pool scan. Cached support counts are used only behind the existing
/// current-receipt eligibility view, which rejects stale roots, revocations and body drift.
async fn public_scan(
    pool: &MaintenanceDbPool,
) -> Result<(i64, i64, i64, OffsetDateTime), ErrorCode> {
    let row = sqlx::query(
        "SELECT (SELECT count(*) FROM public.claims) AS scanned_n, \
           count(*) FILTER (WHERE e.independent_support_count>=4 AND NOT e.identity_incomplete) AS consensus_ready, \
           count(*) FILTER (WHERE e.independent_support_count>1 AND NOT e.identity_incomplete) AS corroborated, \
           clock_timestamp() AS measured_at \
         FROM public.eligible_objects p JOIN public.claims c ON c.claim_id=p.object_id AND p.object_kind='CLAIM' \
         JOIN public.claim_trust_evaluations e ON e.evaluation_id=c.current_evaluation_id")
        // dep: PostgreSQL(any) — executes a query against the pool
        .fetch_one(pool.pool()).await.map_err(db_error)?;
    Ok((
        row.try_get("scanned_n").map_err(db_error)?,
        row.try_get("consensus_ready").map_err(db_error)?,
        row.try_get("corroborated").map_err(db_error)?,
        row.try_get("measured_at").map_err(db_error)?,
    ))
}

/// Controlled public review recorder bound once to trusted process configuration.
///
/// Both pools must name the same database/server and both process registries must
/// name the same non-nil cell. Deployment/build come only from trusted bootstrap
/// configuration, not request parameters. This does not attest operator honesty or
/// grant business authority: every review still checks its moderator authorization.
pub struct PublicObservationRecorder<'a> {
    writer: &'a MaintenanceDbPool,
    business: &'a PublicWorkerDbPool,
    target: ObservationTarget,
    binary_build: String,
}

impl<'a> PublicObservationRecorder<'a> {
    /// Bind immutable identity at startup. A mismatch or unknown database identity
    /// fails before any business mutation or observation is attempted.
    pub async fn new(
        writer: &'a MaintenanceDbPool,
        business: &'a PublicWorkerDbPool,
        writer_resources: &humaux_infra_cell::IntraCellResourceRegistry,
        business_resources: &humaux_infra_cell::IntraCellResourceRegistry,
        deployment_id: Uuid,
        binary_build: &str,
    ) -> Result<Self, ErrorCode> {
        let cell_id = writer_resources.local_cell_id();
        if deployment_id.is_nil()
            || cell_id.0.is_nil()
            || cell_id != business_resources.local_cell_id()
            || binary_build.trim().is_empty()
        {
            return Err(ErrorCode::InvalidInput);
        }
        if !crate::postgres::public_observation_database_matches(writer, business).await? {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            writer,
            business,
            target: ObservationTarget {
                deployment_id: deployment_id.to_string(),
                cell_id: cell_id.0.to_string(),
            },
            binary_build: binary_build.into(),
        })
    }

    /// Target frozen by the trusted bootstrap constructor; callers cannot mutate it.
    pub fn target(&self) -> &ObservationTarget {
        &self.target
    }

    /// Execute a real authorized review between actual scans. Target/build cannot be
    /// overridden per request. A failed receipt write cannot report a successful run;
    /// business review and evidence commit are separate transactions.
    pub async fn review(
        &self,
        authorization: &AuthorizationScope,
        input: &EvaluateClaim<'_>,
    ) -> Result<EvaluationResult, ErrorCode> {
        observe_public_review(
            self.writer,
            self.business,
            authorization,
            &self.target,
            &self.binary_build,
            input,
        )
        .await
    }
}

/// Only the startup-bound recorder can call this implementation.
async fn observe_public_review(
    writer: &MaintenanceDbPool,
    business: &PublicWorkerDbPool,
    authorization: &AuthorizationScope,
    target: &ObservationTarget,
    binary_build: &str,
    input: &EvaluateClaim<'_>,
) -> Result<EvaluationResult, ErrorCode> {
    uuid(&target.deployment_id)?;
    uuid(&target.cell_id)?;
    if binary_build.trim().is_empty() {
        return Err(ErrorCode::InvalidInput);
    }
    let specs = parse_registry(SPEC_TEXT).map_err(|_| ErrorCode::Internal)?;
    let started: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
        // dep: PostgreSQL(any) — executes a query against the pool
        .fetch_one(writer.pool())
        .await
        .map_err(db_error)?;
    let before = public_scan(writer).await?;
    let result = crate::public_repo::evaluate_claim(business, authorization, input).await?;
    let after = public_scan(writer).await?;
    let completed: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
        // dep: PostgreSQL(any) — executes a query against the pool
        .fetch_one(writer.pool())
        .await
        .map_err(db_error)?;
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = writer.pool().begin().await.map_err(db_error)?;
    for (ch, before_value, after_value, name) in [
        (12, before.1, after.1, "public.consensus_ready@1"),
        (21, before.2, after.2, "public.corroborated@1"),
    ] {
        let spec = specs
            .iter()
            .find(|s| s.ch == ch)
            .ok_or(ErrorCode::Internal)?;
        let scan_domain = format!(
            "{}|{}|public.claims:all|eligible_objects:current_receipt|{name}",
            target.deployment_id, target.cell_id
        );
        let digest: String = Sha256::digest(scan_domain.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let scope_hash = format!("sha256:{digest}");
        let before_obs = MechanismObservation {
            observation_id: Uuid::now_v7().to_string(),
            target: target.clone(),
            mechanism_id: spec.id(),
            value: before_value,
            scanned_n: Some(before.0),
            measured_at_micros: micros(before.3)?,
            recorded_status: MechanismStatus::Stale,
            probe_version: name.into(),
            binary_build: binary_build.into(),
            scope_hash: Some(scope_hash.clone()),
        };
        let after_obs = MechanismObservation {
            observation_id: Uuid::now_v7().to_string(),
            value: after_value,
            scanned_n: Some(after.0),
            measured_at_micros: micros(after.3)?,
            ..before_obs.clone()
        };
        let run = MechanismE2eEvidence {
            run_id: Uuid::now_v7().to_string(),
            target: target.clone(),
            mechanism_id: spec.id(),
            before: before_obs.clone(),
            after_observation_id: after_obs.observation_id.clone(),
            probe_version: name.into(),
            scope_hash: scope_hash.clone(),
            binary_build: binary_build.into(),
            started_at_micros: micros(started)?,
            completed_at_micros: micros(completed)?,
        };
        for (obs, witness) in [(&before_obs, None), (&after_obs, Some(&run))] {
            let status = derive_status(spec, target, Some(obs), witness, micros(completed)?)
                .status
                .ok_or(ErrorCode::Internal)?;
            sqlx::query("INSERT INTO ops.mechanism_observations(observation_id,deployment_id,cell_id,mechanism_id,value,scanned_n,measured_at,derived_status,probe_version,binary_build,scope_hash) \
              VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)")
                .bind(uuid(&obs.observation_id)?).bind(uuid(&target.deployment_id)?).bind(uuid(&target.cell_id)?)
                .bind(&obs.mechanism_id).bind(obs.value).bind(obs.scanned_n)
                .bind(OffsetDateTime::from_unix_timestamp_nanos(i128::from(obs.measured_at_micros)*1_000).map_err(|_| ErrorCode::InvalidInput)?)
                .bind(status.as_str()).bind(&obs.probe_version).bind(binary_build).bind(&scope_hash)
                .execute(&mut *txn).await.map_err(db_error)?;
        }
        // An empty before/after scan cannot form a valid E2E receipt. The actual
        // observations remain useful as STALE/low-denominator diagnostics.
        if before.0 > 0 && after.0 > 0 {
            sqlx::query("INSERT INTO ops.mechanism_e2e_runs(run_id,deployment_id,cell_id,mechanism_id,before_observation_id,after_observation_id,scope_hash,probe_version,binary_build,started_at,completed_at) \
              VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)")
                .bind(uuid(&run.run_id)?).bind(uuid(&target.deployment_id)?).bind(uuid(&target.cell_id)?).bind(spec.id())
                .bind(uuid(&before_obs.observation_id)?).bind(uuid(&after_obs.observation_id)?).bind(&scope_hash)
                .bind(name).bind(binary_build).bind(started).bind(completed)
                .execute(&mut *txn).await.map_err(db_error)?;
        }
    }
    txn.commit().await.map_err(db_error)?;
    Ok(result)
}
