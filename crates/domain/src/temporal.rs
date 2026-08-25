//! `domain::temporal` — the sole entry point for reading a Memory row's seven timestamp
//! fields (§9).
//!
//! §9 freezes exactly seven fields, each with a declared retrieval role — the failure mode
//! this guards against is the old system's `valid_from`/`valid_to`, which sat at 0 live rows
//! across the whole table because nothing forced a role to be declared for it (§9, §9.1).
//!
//! | field | role | judged by |
//! |---|---|---|
//! | `occurred_at` | sorts + filters | sole `rank_time` input; sole `since`/`until` basis |
//! | `observed_at` | audit only | ingest-lag diagnostics (`observed_at − occurred_at`), never sorting |
//! | `effective_from` | filters | only `state`/`fact`/`decision` claim types; `as_of` visibility |
//! | `effective_to` | filters | same three claim types; `None` = still in effect |
//! | `created_at` | audit only | DB write time |
//! | `updated_at` | audit only | canvas/task "latest" reads it directly, never retrieval |
//! | `superseded_at` | filters | `Some` ⇒ `temporal_status = 'superseded'`, excluded by default |
//!
//! **Time has exactly one exit.** Sorting and filtering code must never read the fields
//! above directly. The only two entry points are [`rank_time`] (implementation-wise this can
//! only ever return `occurred_at`) and [`visible_at`] (looks only at the `effective_*` pair
//! and `superseded_at`). [`MemoryTimes`]'s seven fields are `pub(crate)` — a crate outside
//! `humaux-domain` (in particular `humaux-retrieval`) that tries to read one of them fails to
//! *compile*; it is not a review comment to catch later (§9).
//!
//! §9.1's decorative-column detector is a separate, DB-side concern: the daily probe lives in
//! `ops.column_vitality` (`migrations/0030_ops_column_vitality.sql`), and any column that
//! trips its 90-day-window FAIL (`total > 0 && live = 0`) must get an explicit ruling —
//! keep-with-rationale or `DROP COLUMN` — registered in `docs/decisions/columns.md`
//! (`total = 0` instead yields `no_data`, which is not a verdict).

use std::time::SystemTime;

/// A Memory row's seven §9 timestamps. Fields are `pub(crate)`: constructible and readable
/// from within `humaux-domain`, invisible to every other crate. The only sanctioned way for
/// outside code to get anything out of one is [`rank_time`] or [`visible_at`] — see the module
/// doc for why direct field reads are a compile error rather than a lint.
///
/// ```rust,compile_fail
/// // §9: reading a `MemoryTimes` field from outside `humaux-domain` must not compile.
/// let times = humaux_domain::temporal::MemoryTimes::new(
///     std::time::SystemTime::now(),
///     std::time::SystemTime::now(),
///     None,
///     None,
///     std::time::SystemTime::now(),
///     std::time::SystemTime::now(),
///     None,
/// );
/// let _ = times.occurred_at; // error[E0616]: field `occurred_at` of struct `MemoryTimes` is private
/// ```
// §9: `observed_at`/`created_at`/`updated_at` are audit-only by design — this module's own
// two exits (`rank_time`/`visible_at`) never read them, so rustc's dead-code lint sees no
// reader at all yet. That is the point, not a bug: a future audit/ingest-lag diagnostic (§9's
// `observed_at − occurred_at`) reads them directly, still within this crate, once it exists.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct MemoryTimes {
    pub(crate) occurred_at: SystemTime,
    pub(crate) observed_at: SystemTime,
    pub(crate) effective_from: Option<SystemTime>,
    pub(crate) effective_to: Option<SystemTime>,
    pub(crate) created_at: SystemTime,
    pub(crate) updated_at: SystemTime,
    pub(crate) superseded_at: Option<SystemTime>,
}

impl MemoryTimes {
    /// Builds a row's time bundle. This constructor is the only public surface — once built,
    /// the seven values are reachable only through [`rank_time`]/[`visible_at`] (§9).
    #[allow(clippy::too_many_arguments)] // §9 freezes exactly these seven fields, no fewer.
    pub fn new(
        occurred_at: SystemTime,
        observed_at: SystemTime,
        effective_from: Option<SystemTime>,
        effective_to: Option<SystemTime>,
        created_at: SystemTime,
        updated_at: SystemTime,
        superseded_at: Option<SystemTime>,
    ) -> Self {
        Self {
            occurred_at,
            observed_at,
            effective_from,
            effective_to,
            created_at,
            updated_at,
            superseded_at,
        }
    }
}

/// The single value retrieval ranks by (§9). A newtype rather than a bare `SystemTime` so a
/// caller can order rows by it without ever holding — or being tempted to reconstruct — the
/// row's other six fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RankInstant(SystemTime);

/// §9's sole ranking entry point. Implementation-wise this can only ever return
/// `occurred_at` — `observed_at`/`created_at`/`updated_at` are audit-only and must never move
/// a row's rank, no matter how far apart they are from each other.
pub fn rank_time(row: &MemoryTimes) -> RankInstant {
    RankInstant(row.occurred_at)
}

/// §9's sole visibility entry point: is `row` visible as of `as_of`, looking only at
/// `effective_from`/`effective_to`/`superseded_at`.
///
/// - `superseded_at` is time-travel aware: a row superseded *after* `as_of` was still current
///   at that point in time and stays visible; only `superseded_at <= as_of` excludes it.
/// - `effective_from`/`effective_to` are `None` for claim types outside `state`/`fact`/
///   `decision` (§9 CHECK-enforced) and impose no bound in that case; a `None` `effective_to`
///   means "still in effect" — the row is not excluded on the upper end.
pub fn visible_at(row: &MemoryTimes, as_of: SystemTime) -> bool {
    if let Some(superseded_at) = row.superseded_at
        && superseded_at <= as_of
    {
        return false;
    }
    if let Some(effective_from) = row.effective_from
        && as_of < effective_from
    {
        return false;
    }
    if let Some(effective_to) = row.effective_to
        && as_of >= effective_to
    {
        return false;
    }
    true
}
