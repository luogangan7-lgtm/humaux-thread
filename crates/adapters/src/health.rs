//! `adapters::health` — the two read-only aggregate reads of ADR-0061: the §41.2 SQL-derived health sample
//!   (`ops.health_snapshot`, maintenance) and the §4.4 probe aggregates (`ops.admin_probe_snapshot`, admin).
//! Depends-on: crates=[humaux-domain, sqlx]; services=[PostgreSQL(role_admin) x=[ops.admin_probe_snapshot],
//!   PostgreSQL(role_maintenance) x=[ops.health_snapshot]]; env=[]; modules=[adapters::disclosure,
//!   adapters::postgres, domain::ticket_family]
//! Called-by: [admin::probe, maintenance::health_serve, tests]
//! Invariants: [the only SQL of the health gauges and the aggregate probes; each read is one definer call that
//!   returns cross-tenant aggregates and no tenant id; an unknown (domain, projection_kind) pair or outcome literal
//!   fails the read naming it, never a synthesized label (§78.2)]
//! Spec: Baseline §41.2; §4.4; §15.2; §78.2; ADR-0061 D-D; ADR-0061 D-J
//!
//! Both functions are owned by the NOLOGIN `role_health_reader` (migration 0210), whose `_health_reader_read`
//! policies are what let one call see every tenant; the caller's own role never gains a table grant.

use std::fmt;

use humaux_domain::ticket_family::TicketFamily;
use sqlx::Row;
use sqlx::postgres::PgRow;
use sqlx::types::time::OffsetDateTime;

use crate::disclosure::DisclosureOutcome;
use crate::postgres::{AdminDbPool, MaintenanceDbPool};

/// A failed aggregate read, as text an operator can act on: the PostgreSQL error (which names a missing or
/// forbidden function, e.g. `ops.health_snapshot(timestamptz)`) or the unknown value that failed closed.
/// ADR-0061 D-D: `/metrics` answers 503 with this text instead of serving a stale sample.
#[derive(Debug)]
pub struct HealthReadError(String);

impl fmt::Display for HealthReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for HealthReadError {}

impl From<sqlx::Error> for HealthReadError {
    fn from(e: sqlx::Error) -> Self {
        Self(e.to_string())
    }
}

/// One `ops.health_snapshot(since)` row. Plain data; `humaux-maintenance` maps it onto
/// `telemetry::health::HealthSnapshot` (this crate has no telemetry dependency).
#[derive(Debug, Clone, PartialEq)]
pub struct HealthSample {
    /// The database `now()` the sample was taken at; the next call's `since` (the finalized watermark).
    pub as_of: OffsetDateTime,
    /// `ops.jobs` rows in PENDING.
    pub jobs_pending: u64,
    /// `ops.jobs` rows in PROCESSING.
    pub jobs_processing: u64,
    /// `ops.jobs` rows in WAITING_KEY.
    pub jobs_waiting_key: u64,
    /// `ops.jobs` rows in DEAD.
    pub jobs_dead: u64,
    /// Seconds since the oldest PENDING job was created, 0 when none.
    pub oldest_pending_age_seconds: f64,
    /// Σ `issued_highwater - projection_highwater` over every stream.
    pub projection_lag_events: u64,
    /// `projection.processing_gaps` rows per ticket family (families with no gap are absent).
    pub processing_gaps: Vec<(TicketFamily, u64)>,
    /// Open reservations reserved ≤ 10 s ago.
    pub reserved_le_10s: u64,
    /// Open reservations reserved > 10 s and ≤ 60 s ago.
    pub reserved_le_60s: u64,
    /// Open reservations reserved > 60 s ago (§53.5 INV-3).
    pub reserved_gt_60s: u64,
    /// Rows with `since < finalized_at <= as_of`, per outcome (outcomes with none are absent).
    pub finalized_since: Vec<(DisclosureOutcome, u64)>,
}

/// One stream family's rows of `ops.admin_probe_snapshot()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamFamilyLag {
    /// The family the `(domain, projection_kind)` pair names.
    pub family: TicketFamily,
    /// `projection.stream_checkpoints` rows of the family.
    pub streams: u64,
    /// Rows with `projection_highwater < issued_highwater`.
    pub lagging: u64,
    /// Σ `issued_highwater - projection_highwater`.
    pub lag_total: u64,
    /// max `issued_highwater - projection_highwater`.
    pub lag_max: u64,
}

/// One `ops.admin_probe_snapshot()` row: the §4.4 `stream.watermark` / `outbox.backlog` / `jobs.stuck` inputs.
#[derive(Debug, Clone, PartialEq)]
pub struct AdminProbeSample {
    /// The database `now()` of the read.
    pub as_of: OffsetDateTime,
    /// Per stream family (families with no checkpoint row are absent).
    pub streams: Vec<StreamFamilyLag>,
    /// `ops.outbox` rows.
    pub outbox_total: u64,
    /// `ops.outbox` rows in PENDING or PROCESSING.
    pub outbox_undelivered: u64,
    /// Seconds since the oldest undelivered row was created; `None` when nothing is undelivered.
    pub outbox_oldest_undelivered_age_seconds: Option<f64>,
    /// `ops.jobs` rows.
    pub jobs_total: u64,
    /// PROCESSING rows whose lease has expired.
    pub jobs_stuck: u64,
    /// PROCESSING rows still inside their lease.
    pub jobs_in_lease: u64,
    /// `pg_get_functiondef` of `ops.admin_probe_snapshot()` as deployed when the row was read: the statement the
    /// three DB probes actually ran, so §4.4's `scope_hash` changes with any forward migration that redefines it
    /// (ADR-0061 review-fix 3, F3).
    pub definition: String,
}

fn count(row: &PgRow, column: &str) -> Result<u64, HealthReadError> {
    let n: i64 = row.try_get(column)?;
    u64::try_from(n).map_err(|_| HealthReadError(format!("{column} = {n} is negative")))
}

fn counts(row: &PgRow, column: &str) -> Result<Vec<u64>, HealthReadError> {
    let ns: Vec<i64> = row.try_get(column)?;
    ns.into_iter()
        .map(|n| u64::try_from(n).map_err(|_| HealthReadError(format!("{column} has {n}"))))
        .collect()
}

/// §78.2 fail-closed: the pair must name a [`TicketFamily`]; the family's version is not part of a gap's key.
fn family(domain: &str, projection_kind: &str) -> Result<TicketFamily, HealthReadError> {
    TicketFamily::ALL
        .into_iter()
        .find(|f| f.domain() == domain && f.projection_kind() == projection_kind)
        .ok_or_else(|| {
            HealthReadError(format!(
                "unknown stream family (domain={domain}, projection_kind={projection_kind}): no TicketFamily"
            ))
        })
}

fn outcome(literal: &str) -> Result<DisclosureOutcome, HealthReadError> {
    DisclosureOutcome::ALL
        .into_iter()
        .find(|o| o.as_str() == literal)
        .ok_or_else(|| HealthReadError(format!("unknown disclosure outcome literal {literal:?}")))
}

/// Reads one health sample; `since` is the previous sample's [`HealthSample::as_of`] (or the process start).
///
/// # Errors
/// The PostgreSQL error (it names `ops.health_snapshot(timestamptz)` when the function is missing or its
/// EXECUTE is revoked), or an unknown stream family / outcome.
pub async fn read_health_snapshot(
    pool: &MaintenanceDbPool,
    since: OffsetDateTime,
) -> Result<HealthSample, HealthReadError> {
    // dep: PostgreSQL(role_maintenance) — ops.health_snapshot (0210 definer, EXECUTE role_maintenance only)
    let row = sqlx::query("SELECT * FROM ops.health_snapshot($1)")
        .bind(since)
        .fetch_one(pool.pool())
        .await?;
    let domains: Vec<String> = row.try_get("gap_domains")?;
    let kinds: Vec<String> = row.try_get("gap_projection_kinds")?;
    let processing_gaps = domains
        .iter()
        .zip(&kinds)
        .zip(counts(&row, "gap_counts")?)
        .map(|((d, k), n)| Ok((family(d, k)?, n)))
        .collect::<Result<_, HealthReadError>>()?;
    let outcomes: Vec<String> = row.try_get("finalized_outcomes")?;
    let finalized_since = outcomes
        .iter()
        .zip(counts(&row, "finalized_counts")?)
        .map(|(o, n)| Ok((outcome(o)?, n)))
        .collect::<Result<_, HealthReadError>>()?;
    Ok(HealthSample {
        as_of: row.try_get("as_of")?,
        jobs_pending: count(&row, "jobs_pending")?,
        jobs_processing: count(&row, "jobs_processing")?,
        jobs_waiting_key: count(&row, "jobs_waiting_key")?,
        jobs_dead: count(&row, "jobs_dead")?,
        oldest_pending_age_seconds: row.try_get("oldest_pending_age_seconds")?,
        projection_lag_events: count(&row, "projection_lag_events")?,
        processing_gaps,
        reserved_le_10s: count(&row, "reserved_le_10s")?,
        reserved_le_60s: count(&row, "reserved_le_60s")?,
        reserved_gt_60s: count(&row, "reserved_gt_60s")?,
        finalized_since,
    })
}

/// Reads the §4.4 probe aggregates across every tenant (ADR-0037 unlock as an aggregate definer).
///
/// # Errors
/// The PostgreSQL error (naming `ops.admin_probe_snapshot()` when missing or not executable), or an unknown
/// stream family.
pub async fn read_admin_probe_snapshot(
    pool: &AdminDbPool,
) -> Result<AdminProbeSample, HealthReadError> {
    // dep: PostgreSQL(role_admin) — ops.admin_probe_snapshot (0210 definer, EXECUTE role_admin only) and its definition
    let row = sqlx::query(
        "SELECT s.*, pg_get_functiondef('ops.admin_probe_snapshot()'::regprocedure) AS definition \
           FROM ops.admin_probe_snapshot() s",
    )
    .fetch_one(pool.pool())
    .await?;
    let domains: Vec<String> = row.try_get("stream_domains")?;
    let kinds: Vec<String> = row.try_get("stream_projection_kinds")?;
    let (n, lagging) = (
        counts(&row, "stream_counts")?,
        counts(&row, "stream_lagging")?,
    );
    let (totals, maxes) = (
        counts(&row, "stream_lag_totals")?,
        counts(&row, "stream_lag_max")?,
    );
    let streams = (0..domains.len())
        .map(|i| {
            Ok(StreamFamilyLag {
                family: family(&domains[i], &kinds[i])?,
                streams: n[i],
                lagging: lagging[i],
                lag_total: totals[i],
                lag_max: maxes[i],
            })
        })
        .collect::<Result<_, HealthReadError>>()?;
    Ok(AdminProbeSample {
        as_of: row.try_get("as_of")?,
        streams,
        outbox_total: count(&row, "outbox_total")?,
        outbox_undelivered: count(&row, "outbox_undelivered")?,
        outbox_oldest_undelivered_age_seconds: row
            .try_get("outbox_oldest_undelivered_age_seconds")?,
        jobs_total: count(&row, "jobs_total")?,
        jobs_stuck: count(&row, "jobs_stuck")?,
        jobs_in_lease: count(&row, "jobs_in_lease")?,
        definition: row.try_get("definition")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_family_or_outcome_fails_closed_naming_it() {
        let f = TicketFamily::PrivateMemory;
        assert_eq!(family(f.domain(), f.projection_kind()).unwrap(), f);
        let e = family("knowledge", "PRIVATE_MEMORY")
            .unwrap_err()
            .to_string();
        assert!(e.contains("domain=knowledge"), "{e}");
        assert_eq!(outcome("DENIED").unwrap(), DisclosureOutcome::Denied);
        let e = outcome("LOST").unwrap_err().to_string();
        assert!(e.contains("\"LOST\""), "{e}");
    }
}
