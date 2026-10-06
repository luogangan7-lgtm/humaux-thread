//! `telemetry::dr` — the six §41.2 disaster-recovery families of card 37: `publish()` is their one emit point,
//!   `render()` their exposition (ADR-0064 D-K as corrected by 10.11 D; ruling E17: one local repository).
//! Depends-on: crates=[]; services=[]; env=[]; modules=[telemetry::metrics]
//! Called-by: [maintenance::serve, tests]
//! Invariants: [publish() holds exactly one `.set(` per family; the `target` label renders only [`TARGET_LOCAL`],
//!   so no code path can render another target value; every `volume` and `limit` value is seeded from its closed
//!   enum; nothing here resets a family: a failed DR_EVIDENCE run keeps the last published values]
//! Spec: Baseline §41.2; §42; §44; ADR-0064 D-K; ADR-0064 E17
//!
//! [`DrReading`] is plain data: the statement that fills it (`adapters::maintenance_repo::dr_evidence`), the two
//! `df -Pk` children and the headroom arithmetic live in `humaux-maintenance --serve`, so no scrape runs SQL — it
//! renders the last publish. Before the first publish every family renders 0 (D-K zero state: `/metrics` answers
//! 503 until the first cycle, so a 0 is never scraped as a live reading).

use crate::metrics::{Gauges, families, write_family, write_single_label};

/// ADR-0064 E17 / 10.2: the only `target` label value card 37 can render (every copy is on this host).
// ponytail: one value on purpose (E17); card 37d adds the second value and its derivation
pub const TARGET_LOCAL: &str = "local";

/// §41.2 frozen `backup_disk_free_bytes.volume` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Volume {
    /// The repository's own filesystem (`HUMAUX_MAINTENANCE_DR_REPO_FS_PATH`, ADR-0064 10.11 A).
    Repo,
    /// The filesystem holding PGDATA (`HUMAUX_MAINTENANCE_DR_PGDATA_FS_PATH`).
    Pgdata,
}

impl Volume {
    /// Every volume, in render order (the slot order of [`DrReading::disk_free_bytes`]).
    pub const ALL: [Volume; 2] = [Self::Repo, Self::Pgdata];

    /// The label value, verbatim §41.2.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Repo => "repo",
            Self::Pgdata => "pgdata",
        }
    }
}

/// §41.2 frozen `backup_budget_headroom_bytes.limit` values (the two D-V budget keys).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Limit {
    /// `repo_max_bytes − repo_bytes − estimate`.
    RepoMax,
    /// live repository free bytes `− min_free_bytes − estimate`.
    FreeFloor,
}

impl Limit {
    /// Every limit, in render order (the slot order of [`DrReading::budget_headroom_bytes`]).
    pub const ALL: [Limit; 2] = [Self::RepoMax, Self::FreeFloor];

    /// The label value, verbatim §41.2.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RepoMax => "repo_max",
            Self::FreeFloor => "free_floor",
        }
    }
}

/// One DR_EVIDENCE reading (ADR-0064 10.11 D). Timestamps are unix seconds, 0 = never.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DrReading {
    /// Stop time of the newest set whose latest receipt is VERIFIED.
    pub backup_last_success: f64,
    /// `finished_at` of the newest drill the table derived as succeeded.
    pub restore_drill_last_success: f64,
    /// `repo_bytes` of the newest receipt that measured it (a refusal row qualifies).
    pub repo_bytes: i64,
    /// Live free bytes per [`Volume::ALL`] slot.
    pub disk_free_bytes: [i64; 2],
    /// Headroom per [`Limit::ALL`] slot.
    pub budget_headroom_bytes: [i64; 2],
    /// The WAL latch (10.11 D): set, or archiving fails now.
    pub wal_archive_failing: bool,
}

static BACKUP_LAST_SUCCESS_TIMESTAMP_SECONDS: Gauges<1> = Gauges::new();
static RESTORE_DRILL_LAST_SUCCESS_TIMESTAMP_SECONDS: Gauges<1> = Gauges::new();
static BACKUP_REPO_BYTES: Gauges<1> = Gauges::new();
static BACKUP_DISK_FREE_BYTES: Gauges<{ Volume::ALL.len() }> = Gauges::new();
static BACKUP_BUDGET_HEADROOM_BYTES: Gauges<{ Limit::ALL.len() }> = Gauges::new();
static WAL_ARCHIVE_FAILING: Gauges<1> = Gauges::new();

/// Publishes one reading: every family is set. Called only after a DR_EVIDENCE run succeeded; a failed run calls
/// nothing here, so the last values stay (ADR-0064 D-K: an absent series would silence BackupFailure).
pub fn publish(r: &DrReading) {
    // labels: target
    BACKUP_LAST_SUCCESS_TIMESTAMP_SECONDS.set(0, r.backup_last_success);
    // labels: target
    RESTORE_DRILL_LAST_SUCCESS_TIMESTAMP_SECONDS.set(0, r.restore_drill_last_success);
    BACKUP_REPO_BYTES.set(0, r.repo_bytes as f64);
    for (slot, free) in r.disk_free_bytes.iter().enumerate() {
        // labels: volume
        BACKUP_DISK_FREE_BYTES.set(slot, *free as f64);
    }
    for (slot, headroom) in r.budget_headroom_bytes.iter().enumerate() {
        // labels: limit
        BACKUP_BUDGET_HEADROOM_BYTES.set(slot, *headroom as f64);
    }
    WAL_ARCHIVE_FAILING.set(0, f64::from(u8::from(r.wal_archive_failing)));
}

/// Renders the six families from the last [`publish`]; `target` is always [`TARGET_LOCAL`].
pub fn render(out: &mut String) {
    for (family, cell) in [
        (
            &families::BACKUP_LAST_SUCCESS_TIMESTAMP_SECONDS,
            &BACKUP_LAST_SUCCESS_TIMESTAMP_SECONDS,
        ),
        (
            &families::RESTORE_DRILL_LAST_SUCCESS_TIMESTAMP_SECONDS,
            &RESTORE_DRILL_LAST_SUCCESS_TIMESTAMP_SECONDS,
        ),
    ] {
        write_single_label(out, family, &[TARGET_LOCAL], |_| cell.get(0));
    }
    write_family(
        out,
        &families::BACKUP_REPO_BYTES,
        &[(&[], BACKUP_REPO_BYTES.get(0))],
    );
    write_single_label(
        out,
        &families::BACKUP_DISK_FREE_BYTES,
        &Volume::ALL.map(Volume::as_str),
        |i| BACKUP_DISK_FREE_BYTES.get(i),
    );
    write_single_label(
        out,
        &families::BACKUP_BUDGET_HEADROOM_BYTES,
        &Limit::ALL.map(Limit::as_str),
        |i| BACKUP_BUDGET_HEADROOM_BYTES.get(i),
    );
    write_family(
        out,
        &families::WAL_ARCHIVE_FAILING,
        &[(&[], WAL_ARCHIVE_FAILING.get(0))],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The only lib test that publishes, so no other test races these statics.
    #[test]
    fn publish_sets_every_series_and_target_is_local_only() {
        publish(&DrReading {
            backup_last_success: 1_700_000_000.5,
            restore_drill_last_success: 1_700_000_100.0,
            repo_bytes: 7,
            disk_free_bytes: [11, 13],
            budget_headroom_bytes: [-17, 19],
            wal_archive_failing: true,
        });
        let mut out = String::new();
        render(&mut out);
        for line in [
            "backup_last_success_timestamp_seconds{target=\"local\"} 1700000000.5",
            "restore_drill_last_success_timestamp_seconds{target=\"local\"} 1700000100",
            "backup_repo_bytes 7",
            "backup_disk_free_bytes{volume=\"repo\"} 11",
            "backup_disk_free_bytes{volume=\"pgdata\"} 13",
            "backup_budget_headroom_bytes{limit=\"repo_max\"} -17",
            "backup_budget_headroom_bytes{limit=\"free_floor\"} 19",
            "wal_archive_failing 1",
        ] {
            assert!(out.lines().any(|l| l == line), "{line} missing in\n{out}");
        }
        let targets = out.matches("target=").count();
        assert_eq!(targets, out.matches("target=\"local\"").count(), "{out}");
    }
}
