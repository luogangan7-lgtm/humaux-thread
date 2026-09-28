//! `domain::lifecycle` — §36/§37.1 memory lifecycle transitions and the `memory.restore` undo decision (ADR-0020).
//! Depends-on: crates=[uuid]; services=[]; env=[]; modules=[domain::authority, domain::error]
//! Called-by: [adapters::memory_governance_repo]
//! Invariants: []
//! Spec: Baseline §78.2
//!
//! Three closed enums mirror the `ops.memory_lifecycle_events` DB CHECK sets verbatim
//! (§78.2 DB<->Rust contract; no stringly-typed domain state), and one pure decision
//! function, [`restore_allowed`], decides whether a memory's head transition may be undone.
//! The domain has no clock: the caller passes `window_expired` (it owns `now >= undo_deadline`),
//! so this function is fully table-testable and never reaches for wall-clock time.

use crate::{
    authority::{AuthorityStatus, EvidenceId, MemoryId},
    error::ConflictReason,
};

/// The kind of object a lifecycle event is about (`target_kind` column).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LifecycleTargetKind {
    /// A `private.memory_records` row.
    Memory,
    /// A public contribution release (card 6+; not wired to restore yet).
    ContributionRelease,
}

impl LifecycleTargetKind {
    /// Every variant, for table-driven lookups.
    pub const ALL: [LifecycleTargetKind; 2] = [Self::Memory, Self::ContributionRelease];

    /// DB wire value (`ops.memory_lifecycle_events.target_kind` CHECK).
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::Memory => "MEMORY",
            Self::ContributionRelease => "CONTRIBUTION_RELEASE",
        }
    }

    /// Reverse of [`Self::as_db_str`].
    pub fn parse_db(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.as_db_str() == value)
    }
}

/// A lifecycle operation (`op` column). Closed set; add a variant only alongside its DB CHECK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LifecycleOp {
    /// §37.2 retention tombstone (terminal; forget path, not yet undoable via restore).
    Tombstone,
    /// §36 supersede: this memory was replaced by a successor (undoable within the window).
    Supersede,
    /// A contributor/user revoke of authority.
    Revoke,
    /// §Q3 archive: hidden from recall/context but not a fifth AuthorityStatus.
    Archive,
    /// §36 restore: the undo of an earlier transition. Not itself restorable.
    Restore,
    /// §37 erase: permanent removal. Never restorable.
    Erase,
}

impl LifecycleOp {
    /// Every variant, for table-driven lookups.
    pub const ALL: [LifecycleOp; 6] = [
        Self::Tombstone,
        Self::Supersede,
        Self::Revoke,
        Self::Archive,
        Self::Restore,
        Self::Erase,
    ];

    /// DB wire value (`ops.memory_lifecycle_events.op` CHECK).
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::Tombstone => "TOMBSTONE",
            Self::Supersede => "SUPERSEDE",
            Self::Revoke => "REVOKE",
            Self::Archive => "ARCHIVE",
            Self::Restore => "RESTORE",
            Self::Erase => "ERASE",
        }
    }

    /// Reverse of [`Self::as_db_str`].
    pub fn parse_db(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|op| op.as_db_str() == value)
    }
}

/// Why a lifecycle transition happened (`reason_code` column). An undo (RESTORE) carries no
/// fresh reason — its reason lives on the event it reverses — so the DB column is nullable
/// and the Rust side uses `Option<LifecycleReason>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LifecycleReason {
    /// §Q4 correct: a user correction superseded the memory with new Evidence.
    UserCorrection,
    /// A plain user-driven supersede (no correction Evidence).
    ExplicitSupersede,
    /// User deleted the memory (tombstone).
    UserDelete,
    /// User archived the memory.
    UserArchive,
    /// A contributor revoked their release.
    ContributorRevoke,
    /// §37 privacy erase.
    PrivacyErase,
}

impl LifecycleReason {
    /// Every variant, for table-driven lookups.
    pub const ALL: [LifecycleReason; 6] = [
        Self::UserCorrection,
        Self::ExplicitSupersede,
        Self::UserDelete,
        Self::UserArchive,
        Self::ContributorRevoke,
        Self::PrivacyErase,
    ];

    /// DB wire value (`ops.memory_lifecycle_events.reason_code` CHECK).
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::UserCorrection => "USER_CORRECTION",
            Self::ExplicitSupersede => "EXPLICIT_SUPERSEDE",
            Self::UserDelete => "USER_DELETE",
            Self::UserArchive => "USER_ARCHIVE",
            Self::ContributorRevoke => "CONTRIBUTOR_REVOKE",
            Self::PrivacyErase => "PRIVACY_ERASE",
        }
    }

    /// Reverse of [`Self::as_db_str`].
    pub fn parse_db(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|r| r.as_db_str() == value)
    }
}

/// The head-transition facts `memory.restore` decides on (ADR-0020 D-C). All read inside the
/// gated transaction; the caller owns the clock, so `window_expired` is `now >= undo_deadline`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestoreTargetState {
    /// The op of the memory's newest lifecycle event, or `None` if it has never had one.
    pub head_op: Option<LifecycleOp>,
    /// The memory's current authority status.
    pub memory_status: AuthorityStatus,
    /// The current status of the SUPERSEDE successor, if the head is a SUPERSEDE.
    pub successor_status: Option<AuthorityStatus>,
    /// Whether the head transition's undo deadline has already passed.
    pub window_expired: bool,
}

/// D-C: decide whether the memory's head transition may be undone. `Ok(())` means reactivate
/// (`status='superseded' -> 'active'`, clearing `superseded_by`). The refusal order is fixed
/// (§36): a non-undoable head first (ERASE terminal, RESTORE/ARCHIVE not reversible), then for
/// a SUPERSEDE head the arbiter sub-order TARGET_NOT_CURRENT -> TARGET_ADVANCED ->
/// UNDO_WINDOW_EXPIRED. This is a pre-flight for a clean reason code; the adapter's
/// `UPDATE ... WHERE status='superseded'` remains the sole arbiter under the row lock.
pub fn restore_allowed(state: RestoreTargetState) -> Result<(), ConflictReason> {
    match state.head_op {
        // A memory with no lifecycle history was never superseded — nothing to undo.
        None => Err(ConflictReason::TARGET_NOT_CURRENT),
        Some(LifecycleOp::Erase) => Err(ConflictReason::ERASE_TERMINAL),
        // Restore is not itself restorable; archive is unarchivable via restore (§Q3);
        // tombstone/revoke are the forget path, not wired to this undo yet (SUPERSEDE-only).
        Some(
            LifecycleOp::Restore
            | LifecycleOp::Archive
            | LifecycleOp::Tombstone
            | LifecycleOp::Revoke,
        ) => Err(ConflictReason::NOT_REVERSIBLE),
        Some(LifecycleOp::Supersede) => {
            if state.memory_status != AuthorityStatus::Superseded {
                return Err(ConflictReason::TARGET_NOT_CURRENT);
            }
            if state.successor_status != Some(AuthorityStatus::Active) {
                return Err(ConflictReason::TARGET_ADVANCED);
            }
            if state.window_expired {
                return Err(ConflictReason::UNDO_WINDOW_EXPIRED);
            }
            Ok(())
        }
    }
}

/// §Q4 (ADR-0025) Correction Event — the shape the spec mandates (spec :8568: "a user edit
/// writes a Correction Event plus a new version and never edits historical evidence in place")
/// but leaves undefined. Research question 4's answer is: correction needs no new table — a
/// correction *is* a `SUPERSEDE` lifecycle event whose reason is [`LifecycleReason::UserCorrection`]
/// and which additionally names the new DirectUserInput Evidence that justifies it. This value
/// object is that record's in-memory shape (the persisted form is one `ops.memory_lifecycle_events`
/// row): who corrected what, the old and new Memory versions, and the correction Evidence.
///
/// It is a witness, not a second source of truth: the adapter builds one from the row it just
/// appended so the gateway result and any audit reader share one definition of "what a
/// correction is", instead of each re-deriving the (op, reason, replacement, evidence) tuple.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorrectionEvent {
    /// The principal who issued the correction (`actor_principal_id`).
    pub actor_principal_id: uuid::Uuid,
    /// The corrected (now-superseded) Memory, `M1`.
    pub superseded_memory_id: MemoryId,
    /// The new active Memory version, `M2` (`replacement_memory_id`).
    pub replacement_memory_id: MemoryId,
    /// The new DirectUserInput Evidence justifying the correction, `E2`
    /// (`correction_evidence_id`).
    pub correction_evidence_id: EvidenceId,
}

impl CorrectionEvent {
    /// The invariant every correction shares (§Q4): op is `SUPERSEDE`, reason is
    /// `USER_CORRECTION`, and it carries both a distinct replacement Memory and a correction
    /// Evidence. Returns `Err(ErrorCode::InvalidInput)` rather than silently accepting a
    /// malformed record — a correction that superseded a Memory with itself, or that named a
    /// plain (non-correction) reason, is not a correction.
    pub fn new(
        actor_principal_id: uuid::Uuid,
        superseded_memory_id: MemoryId,
        replacement_memory_id: MemoryId,
        correction_evidence_id: EvidenceId,
    ) -> Result<Self, crate::error::ErrorCode> {
        if superseded_memory_id == replacement_memory_id {
            return Err(crate::error::ErrorCode::InvalidInput);
        }
        Ok(Self {
            actor_principal_id,
            superseded_memory_id,
            replacement_memory_id,
            correction_evidence_id,
        })
    }

    /// The lifecycle `(op, reason)` a correction always writes — the pin that keeps the adapter's
    /// SUPERSEDE-with-USER_CORRECTION append and this domain shape from drifting apart.
    #[must_use]
    pub const fn lifecycle(&self) -> (LifecycleOp, LifecycleReason) {
        (LifecycleOp::Supersede, LifecycleReason::UserCorrection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_wire_forms_round_trip_and_reject_unknown() {
        for kind in LifecycleTargetKind::ALL {
            assert_eq!(LifecycleTargetKind::parse_db(kind.as_db_str()), Some(kind));
        }
        for op in LifecycleOp::ALL {
            assert_eq!(LifecycleOp::parse_db(op.as_db_str()), Some(op));
        }
        for reason in LifecycleReason::ALL {
            assert_eq!(LifecycleReason::parse_db(reason.as_db_str()), Some(reason));
        }
        assert_eq!(LifecycleOp::parse_db("supersede"), None);
        assert_eq!(LifecycleOp::parse_db(""), None);
        assert_eq!(LifecycleReason::parse_db("USER_RESTORE"), None);
    }

    #[test]
    fn correction_event_is_a_supersede_with_user_correction_and_rejects_self() {
        let m1 = MemoryId::new();
        let m2 = MemoryId::new();
        let e2 = EvidenceId::new();
        let actor = uuid::Uuid::now_v7();
        let event = CorrectionEvent::new(actor, m1, m2, e2).expect("distinct m1/m2");
        assert_eq!(
            event.lifecycle(),
            (LifecycleOp::Supersede, LifecycleReason::UserCorrection)
        );
        assert_eq!(event.superseded_memory_id, m1);
        assert_eq!(event.replacement_memory_id, m2);
        assert_eq!(event.correction_evidence_id, e2);
        // A memory cannot correct itself.
        assert!(CorrectionEvent::new(actor, m1, m1, e2).is_err());
    }

    fn supersede_head(
        memory_status: AuthorityStatus,
        successor_status: Option<AuthorityStatus>,
        window_expired: bool,
    ) -> RestoreTargetState {
        RestoreTargetState {
            head_op: Some(LifecycleOp::Supersede),
            memory_status,
            successor_status,
            window_expired,
        }
    }

    #[test]
    fn restore_allowed_refusal_order_is_fixed() {
        use AuthorityStatus::{Active, Expired, Revoked, Superseded};

        // Happy path: superseded row, active successor, within window.
        assert_eq!(
            restore_allowed(supersede_head(Superseded, Some(Active), false)),
            Ok(())
        );

        // No head -> nothing to undo.
        assert_eq!(
            restore_allowed(RestoreTargetState {
                head_op: None,
                memory_status: Active,
                successor_status: None,
                window_expired: false,
            }),
            Err(ConflictReason::TARGET_NOT_CURRENT)
        );

        // ERASE beats every other refusal, even a would-be TARGET_ADVANCED.
        assert_eq!(
            restore_allowed(RestoreTargetState {
                head_op: Some(LifecycleOp::Erase),
                memory_status: Superseded,
                successor_status: Some(Superseded),
                window_expired: true,
            }),
            Err(ConflictReason::ERASE_TERMINAL)
        );

        // RESTORE/ARCHIVE/TOMBSTONE/REVOKE heads are not reversible.
        for op in [
            LifecycleOp::Restore,
            LifecycleOp::Archive,
            LifecycleOp::Tombstone,
            LifecycleOp::Revoke,
        ] {
            assert_eq!(
                restore_allowed(RestoreTargetState {
                    head_op: Some(op),
                    memory_status: Active,
                    successor_status: None,
                    window_expired: false,
                }),
                Err(ConflictReason::NOT_REVERSIBLE)
            );
        }

        // SUPERSEDE head sub-order: TARGET_NOT_CURRENT -> TARGET_ADVANCED -> UNDO_WINDOW_EXPIRED.
        assert_eq!(
            restore_allowed(supersede_head(Active, Some(Active), false)),
            Err(ConflictReason::TARGET_NOT_CURRENT),
            "row no longer superseded outranks window and successor checks"
        );
        for successor in [Superseded, Revoked, Expired] {
            assert_eq!(
                restore_allowed(supersede_head(Superseded, Some(successor), false)),
                Err(ConflictReason::TARGET_ADVANCED)
            );
        }
        assert_eq!(
            restore_allowed(supersede_head(Superseded, None, false)),
            Err(ConflictReason::TARGET_ADVANCED),
            "a missing successor is an advanced graph, not an active one"
        );
        assert_eq!(
            restore_allowed(supersede_head(Superseded, Some(Active), true)),
            Err(ConflictReason::UNDO_WINDOW_EXPIRED),
            "window is the last gate, only reached once status and successor are clean"
        );
    }
}
