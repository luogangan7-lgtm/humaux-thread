//! `telemetry::health` — the §41.2 SQL-derived health families: `publish()` is their one emit point, `render()` their
//!   exposition (ADR-0061 D-D emit side).
//! Depends-on: crates=[humaux-domain]; services=[]; env=[]; modules=[domain::ticket_family, telemetry::metrics]
//! Called-by: [maintenance::health_serve, tests]
//! Invariants: [publish() holds exactly one `.set(` per health gauge and one `.inc(` for the finalized counter; every
//!   label value comes from a closed enum and every value of it is seeded]
//! Spec: Baseline §41.2; §53.5; §15.4; §7.4; ADR-0061 D-D
//!
//! [`HealthSnapshot`] is plain data: the SQL that fills it (`ops.health_snapshot()`) and the sampling
//! loop live outside this crate, so no scrape ever runs SQL — it renders the last publish.

use crate::metrics::{Counters, Gauges, families, write_family, write_single_label};
use humaux_domain::ticket_family::TicketFamily;

/// §41.2 frozen `data_disclosures_reserved_unfinalized.age_bucket` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgeBucket {
    /// Reserved ≤ 10 s ago.
    Le10s,
    /// Reserved more than 10 s and ≤ 60 s ago.
    Le60s,
    /// Reserved more than 60 s ago (§53.5 INV-3 fires on this bucket).
    Gt60s,
}

impl AgeBucket {
    /// Every bucket, in render order.
    pub const ALL: [AgeBucket; 3] = [Self::Le10s, Self::Le60s, Self::Gt60s];

    /// The label value, verbatim §41.2.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Le10s => "le_10s",
            Self::Le60s => "le_60s",
            Self::Gt60s => "gt_60s",
        }
    }
}

/// `ops.data_disclosures.outcome` (§7.4), the CHECK literals of migration 0047 verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisclosureOutcome {
    /// `SUCCESS`.
    Success,
    /// `FAILED`.
    Failed,
    /// `DENIED`.
    Denied,
}

impl DisclosureOutcome {
    /// Every outcome, in render order.
    pub const ALL: [DisclosureOutcome; 3] = [Self::Success, Self::Failed, Self::Denied];

    /// The DB literal, also the `outcome` label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "SUCCESS",
            Self::Failed => "FAILED",
            Self::Denied => "DENIED",
        }
    }

    /// DB literal → outcome; an unknown literal is `None`, never a synthesized label (§78.2).
    pub fn from_db(literal: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|o| o.as_str() == literal)
    }
}

/// One sample of the health aggregates. A label value missing from a list publishes 0.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HealthSnapshot {
    /// `ops.jobs` rows in PENDING.
    pub jobs_pending: u64,
    /// `ops.jobs` rows in PROCESSING.
    pub jobs_processing: u64,
    /// `ops.jobs` rows in WAITING_KEY.
    pub jobs_waiting_key: u64,
    /// `ops.jobs` rows in DEAD.
    pub jobs_dead: u64,
    /// `now() - min(created_at)` over PENDING rows, 0 when none.
    pub oldest_pending_age_seconds: f64,
    /// Σ `issued_highwater - projection_highwater` over `projection.stream_checkpoints`.
    pub projection_lag_events: u64,
    /// `projection.processing_gaps` rows per ticket family.
    pub processing_gaps: Vec<(TicketFamily, u64)>,
    /// Open reservations per age bucket.
    pub reserved_unfinalized: Vec<(AgeBucket, u64)>,
    /// Rows finalized after the sampler's watermark, per outcome — a delta, added to the counter.
    pub finalized_since_watermark: Vec<(DisclosureOutcome, u64)>,
}

static JOBS_PENDING: Gauges<1> = Gauges::new();
static JOBS_PROCESSING: Gauges<1> = Gauges::new();
static JOBS_WAITING_KEY: Gauges<1> = Gauges::new();
static JOBS_DEAD: Gauges<1> = Gauges::new();
static OLDEST_PENDING_AGE_SECONDS: Gauges<1> = Gauges::new();
static PROJECTION_LAG_EVENTS: Gauges<1> = Gauges::new();
static PROCESSING_GAP_COUNT: Gauges<{ TicketFamily::ALL.len() }> = Gauges::new();
static DATA_DISCLOSURES_RESERVED_UNFINALIZED: Gauges<{ AgeBucket::ALL.len() }> = Gauges::new();
static DATA_DISCLOSURES_FINALIZED_TOTAL: Counters<{ DisclosureOutcome::ALL.len() }> =
    Counters::new();

fn total<K: PartialEq>(pairs: &[(K, u64)], key: &K) -> u64 {
    pairs.iter().filter(|(k, _)| k == key).map(|(_, n)| n).sum()
}

/// Publishes one sample: every gauge is set, the finalized counter grows by the snapshot's delta.
pub fn publish(s: &HealthSnapshot) {
    JOBS_PENDING.set(0, s.jobs_pending as f64);
    JOBS_PROCESSING.set(0, s.jobs_processing as f64);
    JOBS_WAITING_KEY.set(0, s.jobs_waiting_key as f64);
    JOBS_DEAD.set(0, s.jobs_dead as f64);
    OLDEST_PENDING_AGE_SECONDS.set(0, s.oldest_pending_age_seconds);
    PROJECTION_LAG_EVENTS.set(0, s.projection_lag_events as f64);
    for (slot, family) in TicketFamily::ALL.iter().enumerate() {
        // labels: stream
        PROCESSING_GAP_COUNT.set(slot, total(&s.processing_gaps, family) as f64);
    }
    for (slot, bucket) in AgeBucket::ALL.iter().enumerate() {
        let open = total(&s.reserved_unfinalized, bucket) as f64;
        // labels: age_bucket
        DATA_DISCLOSURES_RESERVED_UNFINALIZED.set(slot, open);
    }
    for (slot, outcome) in DisclosureOutcome::ALL.iter().enumerate() {
        // labels: outcome
        DATA_DISCLOSURES_FINALIZED_TOTAL.inc(slot, total(&s.finalized_since_watermark, outcome));
    }
}

/// Renders the nine health families from the last [`publish`], every label value seeded.
pub fn render(out: &mut String) {
    for (family, cell) in [
        (&families::JOBS_PENDING, &JOBS_PENDING),
        (&families::JOBS_PROCESSING, &JOBS_PROCESSING),
        (&families::JOBS_WAITING_KEY, &JOBS_WAITING_KEY),
        (&families::JOBS_DEAD, &JOBS_DEAD),
        (
            &families::OLDEST_PENDING_AGE_SECONDS,
            &OLDEST_PENDING_AGE_SECONDS,
        ),
        (&families::PROJECTION_LAG_EVENTS, &PROJECTION_LAG_EVENTS),
    ] {
        write_family(out, family, &[(&[], cell.get(0))]);
    }
    // §41.2 / ADR-0061 E3: `stream` = TicketFamily::domain(), never a tenant or scope id.
    write_single_label(
        out,
        &families::PROCESSING_GAP_COUNT,
        &TicketFamily::ALL.map(TicketFamily::domain),
        |i| PROCESSING_GAP_COUNT.get(i),
    );
    write_single_label(
        out,
        &families::DATA_DISCLOSURES_FINALIZED_TOTAL,
        &DisclosureOutcome::ALL.map(DisclosureOutcome::as_str),
        |i| DATA_DISCLOSURES_FINALIZED_TOTAL.get(i) as f64,
    );
    write_single_label(
        out,
        &families::DATA_DISCLOSURES_RESERVED_UNFINALIZED,
        &AgeBucket::ALL.map(AgeBucket::as_str),
        |i| DATA_DISCLOSURES_RESERVED_UNFINALIZED.get(i),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(out: &str, series: &str) -> f64 {
        out.lines()
            .find_map(|l| l.strip_prefix(series)?.strip_prefix(' '))
            .unwrap_or_else(|| panic!("no series {series} in\n{out}"))
            .parse()
            .unwrap()
    }

    fn series_count(out: &str, family: &str) -> usize {
        out.lines()
            .filter(|l| !l.starts_with('#'))
            .filter(|l| l.split(['{', ' ']).next() == Some(family))
            .count()
    }

    /// T-D2: each gauge takes the latest sample; the finalized counter accumulates the deltas.
    /// The only lib test that publishes, so no other test races these statics.
    #[test]
    fn publish_sets_gauges_and_accumulates_the_finalized_counter() {
        let mut out = String::new();
        render(&mut out);
        let before = value(
            &out,
            "data_disclosures_finalized_total{outcome=\"SUCCESS\"}",
        );
        let first = HealthSnapshot {
            jobs_dead: 3,
            reserved_unfinalized: vec![(AgeBucket::Gt60s, 1)],
            finalized_since_watermark: vec![(DisclosureOutcome::Success, 2)],
            ..HealthSnapshot::default()
        };
        publish(&first);
        publish(&HealthSnapshot {
            jobs_dead: 1,
            projection_lag_events: 6,
            finalized_since_watermark: vec![(DisclosureOutcome::Success, 5)],
            ..first.clone()
        });
        let mut out = String::new();
        render(&mut out);
        assert_eq!(value(&out, "jobs_dead"), 1.0);
        assert_eq!(value(&out, "projection_lag_events"), 6.0);
        assert_eq!(
            value(
                &out,
                "data_disclosures_reserved_unfinalized{age_bucket=\"gt_60s\"}"
            ),
            1.0
        );
        assert_eq!(
            value(
                &out,
                "data_disclosures_finalized_total{outcome=\"SUCCESS\"}"
            ),
            before + 7.0
        );
    }

    /// T-A2 / T-A4: every health family renders HELP + TYPE and exactly the product of its label
    /// enum sizes (frozen sets: age_bucket 3 per §41.2, outcome 3 per the 0047 CHECK).
    #[test]
    fn render_seeds_every_family_at_its_cardinality_bound() {
        let mut out = String::new();
        render(&mut out);
        let expected = [
            ("jobs_pending", 1),
            ("jobs_processing", 1),
            ("jobs_waiting_key", 1),
            ("jobs_dead", 1),
            ("oldest_pending_age_seconds", 1),
            ("projection_lag_events", 1),
            ("processing_gap_count", TicketFamily::ALL.len()),
            ("data_disclosures_finalized_total", 3),
            ("data_disclosures_reserved_unfinalized", 3),
        ];
        for (family, n) in expected {
            assert!(out.contains(&format!("# HELP {family} ")), "HELP {family}");
            assert!(out.contains(&format!("# TYPE {family} ")), "TYPE {family}");
            assert_eq!(series_count(&out, family), n, "{family}\n{out}");
        }
        assert_eq!(out.lines().filter(|l| l.starts_with("# TYPE ")).count(), 9);
    }

    /// §78.2 contract: the outcome enum equals the `ops.data_disclosures.outcome` CHECK set.
    #[test]
    fn outcome_enum_matches_the_0047_check_literals() {
        let sql = include_str!("../../../migrations/0047_data_disclosure_ledger.sql");
        let clause = sql
            .split("outcome IS NULL OR outcome IN (")
            .nth(1)
            .and_then(|rest| rest.split(')').next())
            .expect("0047 outcome CHECK clause");
        let literals: Vec<&str> = clause
            .split(',')
            .map(|l| l.trim().trim_matches('\''))
            .collect();
        let ours: Vec<&str> = DisclosureOutcome::ALL
            .map(DisclosureOutcome::as_str)
            .to_vec();
        assert_eq!(literals, ours);
        assert_eq!(
            DisclosureOutcome::from_db("DENIED"),
            Some(DisclosureOutcome::Denied)
        );
        assert_eq!(DisclosureOutcome::from_db("denied"), None);
    }
}
