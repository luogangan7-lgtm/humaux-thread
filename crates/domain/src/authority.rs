//! `domain::authority` — `AuthorityClass` / `AuthorityStatus` / `Confidence` (§59 / §59.1).
//!
//! The `Authority` struct body (fields all private, sole construction entry point
//! `Authority::new`, the four-tier `resolve()` total order) belongs to T0.7 and is out of
//! scope for this module; this module lands only the three value-type skeletons §59.1
//! depends on.
//!
//! §59.1 freezes: this section promotes §10's "recommended" `authority_class`/`confidence`/
//! `status` to required. §10's priority chain itself is unchanged, it merely gains typed
//! expression — `AuthorityClass`'s discriminants 0→6 map item-for-item, low to high, onto
//! §10's table.

use crate::error::ErrorCode;

/// Typed expression of §10's priority chain, a closed set of 7 variants (§59).
///
/// The discriminant *is* the priority — larger outranks smaller, mapping item-for-item onto
/// §10's priority chain (§59.1 I1, first tier of the adjudication total order). No
/// `#[non_exhaustive]`, no `Other`/`Unknown`/`Custom(String)` (§59 frozen).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AuthorityClass {
    /// General public knowledge, no personal or project context (§10, lowest priority).
    PublicKnowledge = 0,
    /// Private knowledge specific to this tenant/user (§10).
    PrivateKnowledge = 1,
    /// A stated user preference (§10).
    UserPreference = 2,
    /// A recorded project decision (§10).
    ProjectDecision = 3,
    /// The user explicitly correcting prior output (§10).
    UserCorrection = 4,
    /// A project-level constraint that must hold (§10).
    ProjectConstraint = 5,
    /// Context explicitly supplied for the current task (§10, highest priority).
    ExplicitTaskContext = 6,
}

/// Authority lifecycle state, a closed set of 4 variants (§59).
///
/// A non-`Active` row does not participate in adjudication (§59.1 I3), but still counts
/// toward §23's `visible` and `done` as usual — completeness measures "what you cannot see",
/// not "which row won adjudication"; the two must never be conflated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityStatus {
    /// Currently in force and eligible for adjudication (§59).
    Active,
    /// Replaced by a newer Authority row; kept for history (§59).
    Superseded,
    /// Explicitly withdrawn (§59).
    Revoked,
    /// Past its validity window (§59).
    Expired,
}

/// Confidence in the closed `[0.0, 1.0]` interval (§59).
///
/// Out-of-range values (including `NaN` / `Inf`) return `Err(ErrorCode::InvalidInput)` at
/// `new` — clamping back into `[0,1]` is forbidden: garbage fails loud (§50), it is never
/// silently corrected. Used only for tie-breaks **within the same class**, never in effect
/// across `AuthorityClass` boundaries (§59.1).
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Confidence(f32);

impl Confidence {
    /// Construction guard: `v` must be a finite float within the closed `[0.0, 1.0]`
    /// interval, otherwise returns `Err(ErrorCode::InvalidInput)` (§52 `INVALID_INPUT`).
    /// Clamping is forbidden.
    pub fn new(v: f32) -> Result<Self, ErrorCode> {
        if v.is_finite() && (0.0..=1.0).contains(&v) {
            Ok(Self(v))
        } else {
            Err(ErrorCode::InvalidInput)
        }
    }

    /// Reads out the inner `f32` value.
    pub fn get(self) -> f32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confidence_rejects_nan() {
        assert!(Confidence::new(f32::NAN).is_err());
    }

    #[test]
    fn confidence_rejects_positive_infinity() {
        assert!(Confidence::new(f32::INFINITY).is_err());
    }

    #[test]
    fn confidence_rejects_above_upper_bound() {
        assert!(Confidence::new(1.5).is_err());
    }

    #[test]
    fn confidence_rejects_below_lower_bound() {
        assert!(Confidence::new(-0.5).is_err());
    }

    #[test]
    fn confidence_accepts_lower_closed_bound() {
        assert_eq!(Confidence::new(0.0).expect("0.0 is in range").get(), 0.0);
    }

    #[test]
    fn confidence_accepts_upper_closed_bound() {
        assert_eq!(Confidence::new(1.0).expect("1.0 is in range").get(), 1.0);
    }

    #[test]
    fn authority_class_discriminants_are_0_through_6_in_priority_order() {
        // §59.1 I1's first tier depends on the discriminant being the priority; this locks
        // the exact values and order — discriminant drift would silently change the meaning
        // of §10's priority chain at the type level.
        assert_eq!(AuthorityClass::PublicKnowledge as i32, 0);
        assert_eq!(AuthorityClass::PrivateKnowledge as i32, 1);
        assert_eq!(AuthorityClass::UserPreference as i32, 2);
        assert_eq!(AuthorityClass::ProjectDecision as i32, 3);
        assert_eq!(AuthorityClass::UserCorrection as i32, 4);
        assert_eq!(AuthorityClass::ProjectConstraint as i32, 5);
        assert_eq!(AuthorityClass::ExplicitTaskContext as i32, 6);

        assert!(AuthorityClass::PublicKnowledge < AuthorityClass::PrivateKnowledge);
        assert!(AuthorityClass::PrivateKnowledge < AuthorityClass::UserPreference);
        assert!(AuthorityClass::UserPreference < AuthorityClass::ProjectDecision);
        assert!(AuthorityClass::ProjectDecision < AuthorityClass::UserCorrection);
        assert!(AuthorityClass::UserCorrection < AuthorityClass::ProjectConstraint);
        assert!(AuthorityClass::ProjectConstraint < AuthorityClass::ExplicitTaskContext);
    }
}
