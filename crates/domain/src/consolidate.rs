//! `domain::consolidate` — Private Memory Consolidation invariants (§11.6-§11.9, T4.6+T4.7).
//!
//! Pure types only (§3/§78.3: Domain never imports SQLx/HTTP/ENV). The DB-side snapshot-bound
//! selection SQL lives in `adapters::consolidate_repo`; this module holds the two invariants
//! that must hold regardless of which adapter runs it:
//!
//! 1. an automatic consolidation run can never mint a mutation handle for a memory that
//!    carries an active `private.context_bindings` row (§11.8: "MUST NOT mutate Pinned/
//!    Mandatory binding") — enforced as a closed type conversion with no runtime bypass, not
//!    a "please remember not to" rule (§11.8: "这条不是 prompt 纪律").
//! 2. a `MemoryRollup`'s Authority never outranks the Authority of the source Memories it
//!    closes over (§11.9: "禁止 Rollup 自己成为比 source Memory 更高的 Authority").

use crate::authority::{AuthorityClass, MemoryId};

/// `private.memory_consolidation_runs.status`'s CHECK constraint, verbatim and in order
/// (`migrations/0005_private_pipeline.sql`) — the §78.2 contract test below pins both
/// directions against the real migration file text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsolidationRunState {
    Pending,
    Selecting,
    Running,
    Succeeded,
    /// Healthy terminal state: a run that found nothing worth rolling up is not a failure
    /// (§11.7: "是健康终态，不得与 FAILED 混在一起") and [`is_healthy_terminal`] must say so
    /// so a job-retry/DLQ layer never requeues or dead-letters it.
    ///
    /// [`is_healthy_terminal`]: ConsolidationRunState::is_healthy_terminal
    SucceededNoOutput,
    /// An input was corrected/superseded/revoked after the run's snapshot was taken (§11.7).
    /// The caller must discard any unpublished rollup and enqueue a fresh run — never publish
    /// a stale snapshot's result over newer facts.
    StaleInput,
    Failed,
    Cancelled,
}

impl ConsolidationRunState {
    /// Verbatim DB order — the §78.2 contract test asserts this equals
    /// `memory_consolidation_runs`'s `CHECK (status IN (...))` list element-for-element.
    pub const ALL: [ConsolidationRunState; 8] = [
        Self::Pending,
        Self::Selecting,
        Self::Running,
        Self::Succeeded,
        Self::SucceededNoOutput,
        Self::StaleInput,
        Self::Failed,
        Self::Cancelled,
    ];

    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Selecting => "SELECTING",
            Self::Running => "RUNNING",
            Self::Succeeded => "SUCCEEDED",
            Self::SucceededNoOutput => "SUCCEEDED_NO_OUTPUT",
            Self::StaleInput => "STALE_INPUT",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }

    pub fn from_db_str(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.as_db_str() == s)
    }

    /// §11.7: "`SUCCEEDED_NO_OUTPUT` 是健康终态，不得与 FAILED 混在一起" — these two states
    /// (and only these two) must never be routed to a retry queue or a dead-letter queue.
    pub const fn is_healthy_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::SucceededNoOutput)
    }

    /// A run in any of these states will not transition again on its own.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::SucceededNoOutput | Self::Failed | Self::Cancelled
        )
    }
}

// ---------------------------------------------------------------------------------------
// §11.8 AutoMutableMemoryId — Pinned/Mandatory cannot be constructed into it, structurally.
// ---------------------------------------------------------------------------------------

/// A [`MemoryId`] known (at the moment [`classify`] ran) to carry an active
/// `private.context_bindings` row.
///
/// Field is private to this module: nothing outside [`classify`] can mint one, and there is
/// deliberately no `From<BoundMemoryId> for AutoMutableMemoryId` anywhere in this crate — see
/// `tests/ui/fail_bound_to_auto_mutable.rs` (a compile-fail fixture, not a runtime assertion).
///
/// ponytail: `private.context_bindings` (`migrations/0006_private_skeleton.sql`, §25.4's
/// T-card) has not yet grown its `mode` column (`MANDATORY | PINNED | SUPPLEMENTAL`) — until
/// it does, ANY active binding row is treated as exclusionary here, collapsing both
/// no-auto-mutate modes into one type. Upgrade: split into `MandatoryMemoryId`/`PinnedMemoryId`
/// once `mode` lands on that table; [`classify`] (the sole call site adapters use) is the only
/// place that needs to change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundMemoryId(MemoryId);

impl BoundMemoryId {
    /// Recovers the underlying id — for logging/governance-suggestion output only (§11.6
    /// "MAY emit governance suggestions"); it does not grant a path back to
    /// [`AutoMutableMemoryId`].
    pub fn into_inner(self) -> MemoryId {
        self.0
    }
}

/// A [`MemoryId`] known to carry no active `private.context_bindings` row at classification
/// time — the only type that can become an [`AutoMutableMemoryId`] (via [`From`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnboundMemoryId(MemoryId);

/// Result of asking "does this memory currently have an active context binding". The only
/// public way to produce either variant is [`classify`] — both tuple fields are private, so
/// no `BoundMemoryId(id)` / `UnboundMemoryId(id)` literal is reachable outside this module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassifiedMemoryId {
    Bound(BoundMemoryId),
    Unbound(UnboundMemoryId),
}

/// Sole classification entry point (§11.8). `has_active_binding` is the caller's DB-backed
/// answer to "does `private.context_bindings` have any row for this `memory_id`"
/// (`adapters::consolidate_repo` computes it inside the same snapshot transaction as
/// selection, §11.7 — see this module's own ponytail note above on why "any row" rather than
/// a `mode`-filtered subset) — this function itself does no I/O (domain never touches SQLx,
/// §3/§78.3).
pub fn classify(id: MemoryId, has_active_binding: bool) -> ClassifiedMemoryId {
    if has_active_binding {
        ClassifiedMemoryId::Bound(BoundMemoryId(id))
    } else {
        ClassifiedMemoryId::Unbound(UnboundMemoryId(id))
    }
}

/// Consolidation's sole mutation-target handle (§11.8: "Consolidation mutation API 的参数
/// 类型只能接 `AutoMutableMemoryId`"). Field is private; the only constructor is
/// `From<UnboundMemoryId>` below. A [`BoundMemoryId`] has no path here at all — proven by
/// `tests/ui/fail_bound_to_auto_mutable.rs` failing to compile, not by a runtime check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoMutableMemoryId(MemoryId);

impl From<UnboundMemoryId> for AutoMutableMemoryId {
    fn from(id: UnboundMemoryId) -> Self {
        Self(id.0)
    }
}

impl AutoMutableMemoryId {
    pub fn into_inner(self) -> MemoryId {
        self.0
    }
}

// ---------------------------------------------------------------------------------------
// §11.9 Rollup authority ceiling.
// ---------------------------------------------------------------------------------------

/// Why a proposed `MemoryRollup` was rejected (§11.9 / G11-1's sibling normative rule, same
/// section as the snapshot-integrity gate).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollupAuthorityViolation {
    /// `rollup_class` outranks every one of `max_source_class` — "禁止 Rollup 自己成为比
    /// source Memory 更高的 Authority".
    ExceedsSource {
        rollup_class: AuthorityClass,
        max_source_class: AuthorityClass,
    },
    /// No source Memories at all — §11.6: "没有 source closure 的 rollup 不可发布". This is a
    /// second, independent check; it does not replace the FK-enforced closure in
    /// `private.memory_rollup_sources`.
    NoSourceClosure,
}

/// G11-1/§11.9 judgment: a rollup's Authority class must be `<=` the highest-ranked class
/// among the Memories it closes over.
pub fn check_rollup_authority_ceiling(
    rollup_class: AuthorityClass,
    source_classes: &[AuthorityClass],
) -> Result<(), RollupAuthorityViolation> {
    let Some(max_source_class) = source_classes.iter().copied().max() else {
        return Err(RollupAuthorityViolation::NoSourceClosure);
    };
    if rollup_class <= max_source_class {
        Ok(())
    } else {
        Err(RollupAuthorityViolation::ExceedsSource {
            rollup_class,
            max_source_class,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_state_db_str_roundtrips() {
        for state in ConsolidationRunState::ALL {
            assert_eq!(
                ConsolidationRunState::from_db_str(state.as_db_str()),
                Some(state)
            );
        }
    }

    #[test]
    fn run_state_unknown_string_is_none() {
        assert_eq!(ConsolidationRunState::from_db_str("NOT_A_STATE"), None);
    }

    #[test]
    fn only_succeeded_variants_are_healthy_terminal() {
        for state in ConsolidationRunState::ALL {
            assert_eq!(
                state.is_healthy_terminal(),
                matches!(
                    state,
                    ConsolidationRunState::Succeeded | ConsolidationRunState::SucceededNoOutput
                ),
                "{state:?}"
            );
        }
    }

    #[test]
    fn classify_unbound_converts_to_auto_mutable() {
        let id = MemoryId::new();
        match classify(id, false) {
            ClassifiedMemoryId::Unbound(u) => {
                let auto: AutoMutableMemoryId = u.into();
                assert_eq!(auto.into_inner(), id);
            }
            ClassifiedMemoryId::Bound(_) => {
                panic!("has_active_binding=false must classify Unbound")
            }
        }
    }

    #[test]
    fn classify_bound_has_no_conversion_available_at_runtime_either() {
        let id = MemoryId::new();
        match classify(id, true) {
            ClassifiedMemoryId::Bound(b) => assert_eq!(b.into_inner(), id),
            ClassifiedMemoryId::Unbound(_) => panic!("has_active_binding=true must classify Bound"),
        }
        // There is intentionally no expression that turns a `BoundMemoryId` into an
        // `AutoMutableMemoryId` for this test to call — see `tests/ui/fail_bound_to_auto_mutable.rs`.
    }

    #[test]
    fn rollup_ceiling_allows_equal_or_lower_class() {
        use AuthorityClass::*;
        assert!(
            check_rollup_authority_ceiling(UserPreference, &[ProjectDecision, UserPreference])
                .is_ok()
        );
        assert!(check_rollup_authority_ceiling(ProjectDecision, &[ProjectDecision]).is_ok());
    }

    #[test]
    fn rollup_ceiling_rejects_exceeding_source() {
        use AuthorityClass::*;
        let err =
            check_rollup_authority_ceiling(ProjectConstraint, &[UserPreference, ProjectDecision])
                .unwrap_err();
        assert_eq!(
            err,
            RollupAuthorityViolation::ExceedsSource {
                rollup_class: ProjectConstraint,
                max_source_class: ProjectDecision,
            }
        );
    }

    #[test]
    fn rollup_ceiling_rejects_empty_source_closure() {
        let err = check_rollup_authority_ceiling(AuthorityClass::PublicKnowledge, &[]).unwrap_err();
        assert_eq!(err, RollupAuthorityViolation::NoSourceClosure);
    }
}

/// §78.2 "DB enum 与 Rust enum 走 contract test 对账": [`ConsolidationRunState::ALL`] must
/// list exactly `private.memory_consolidation_runs`'s `status` CHECK values, in order, both
/// directions. Runs against the real migration file text, not a live DB, so it always
/// executes (same technique as `adapters::jobs::contract_tests`).
#[cfg(test)]
mod contract_tests {
    use super::*;

    const MIGRATION_SQL: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/0005_private_pipeline.sql"
    ));

    /// Scopes to just the `CREATE TABLE private.memory_consolidation_runs ( ... )` block —
    /// the migration also defines `memory_consolidation_inputs`/`memory_rollups`/
    /// `memory_rollup_sources`, none of which carry a `status` column.
    fn runs_table_sql() -> &'static str {
        let start = MIGRATION_SQL
            .find("CREATE TABLE private.memory_consolidation_runs (")
            .expect("migration must define private.memory_consolidation_runs");
        let end = MIGRATION_SQL[start..]
            .find(");\n")
            .expect("unterminated memory_consolidation_runs table definition")
            + start;
        &MIGRATION_SQL[start..end]
    }

    fn check_values(table_sql: &str, column: &str) -> Vec<String> {
        let needle = format!("{column} IN");
        let after_needle = table_sql.find(&needle).unwrap_or_else(|| {
            panic!("memory_consolidation_runs has no `{column} IN (...)` CHECK clause")
        }) + needle.len();
        let open = table_sql[after_needle..]
            .find('(')
            .expect("CHECK IN clause missing opening paren")
            + after_needle
            + 1;
        let close = table_sql[open..]
            .find(')')
            .expect("unterminated CHECK IN (...) clause")
            + open;
        table_sql[open..close]
            .split(',')
            .map(|s| s.trim().trim_matches('\'').to_string())
            .collect()
    }

    #[test]
    fn run_state_matches_check_constraint() {
        let db = check_values(runs_table_sql(), "status");
        let rust: Vec<String> = ConsolidationRunState::ALL
            .iter()
            .map(|s| s.as_db_str().to_string())
            .collect();
        assert_eq!(
            db, rust,
            "ConsolidationRunState::ALL must list every status in DB order"
        );
    }
}
