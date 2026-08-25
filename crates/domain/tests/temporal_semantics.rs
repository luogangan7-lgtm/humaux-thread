//! Integration coverage for `domain::temporal` (§9): `rank_time` reads only `occurred_at`,
//! and the three audit-only fields (`observed_at`/`created_at`/`updated_at`) never move a
//! row's rank no matter how far apart they are. Lives under `tests/` (not `#[cfg(test)]`
//! inside `src/temporal.rs`) so it also exercises the module's public surface exactly the way
//! an outside crate would — the same compilation boundary the `pub(crate)` fields are meant
//! to hold against (see the `compile_fail` doctest in `src/temporal.rs` for the negative case).

use humaux_domain::temporal::{MemoryTimes, rank_time, visible_at};
use std::time::{Duration, SystemTime};

fn at(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

/// Two rows that agree on `occurred_at` but disagree on every audit-only field must rank
/// identically — the only way this fails is if one of those fields leaked into `rank_time`.
#[test]
fn rank_time_ignores_audit_only_fields() {
    let occurred = at(1_000);
    let row_a = MemoryTimes::new(occurred, at(1), None, None, at(2), at(3), None);
    let row_b = MemoryTimes::new(occurred, at(999_999), None, None, at(0), at(1), None);

    assert_eq!(rank_time(&row_a), rank_time(&row_b));
}

#[test]
fn rank_time_tracks_occurred_at() {
    let earlier = MemoryTimes::new(at(100), at(0), None, None, at(0), at(0), None);
    let later = MemoryTimes::new(at(200), at(0), None, None, at(0), at(0), None);

    assert!(rank_time(&earlier) < rank_time(&later));
}

#[test]
fn visible_at_respects_effective_range() {
    let row = MemoryTimes::new(
        at(0),
        at(0),
        Some(at(100)),
        Some(at(200)),
        at(0),
        at(0),
        None,
    );

    assert!(!visible_at(&row, at(99)), "before effective_from");
    assert!(visible_at(&row, at(100)), "at effective_from");
    assert!(visible_at(&row, at(150)), "inside range");
    assert!(!visible_at(&row, at(200)), "effective_to is exclusive");
}

#[test]
fn visible_at_open_effective_to_means_still_valid() {
    let row = MemoryTimes::new(at(0), at(0), Some(at(100)), None, at(0), at(0), None);

    assert!(visible_at(&row, at(1_000_000)));
}

#[test]
fn visible_at_excludes_once_superseded() {
    let row = MemoryTimes::new(at(0), at(0), None, None, at(0), at(0), Some(at(500)));

    assert!(!visible_at(&row, at(500)));
    assert!(!visible_at(&row, at(1_000)));
}

/// §9 time-travel semantics: `as_of` earlier than `superseded_at` means the row was still
/// current at that point in time.
#[test]
fn visible_at_before_supersession_still_sees_it() {
    let row = MemoryTimes::new(at(0), at(0), None, None, at(0), at(0), Some(at(500)));

    assert!(visible_at(&row, at(499)));
}
