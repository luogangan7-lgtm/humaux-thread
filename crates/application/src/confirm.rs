//! `application::confirm` — §36/§10.1 `memory.confirm` use case, pure half (ADR-0026, Card 6).
//!
//! `memory.confirm` promotes a `private.distill_candidates` row (a distill output the §10.1
//! origin-bound ceiling rejected for its source origin) into a `UserConfirmed` Evidence + a new
//! active Memory version. The IO order (lock candidate -> reserve BMO -> consume token -> insert
//! E2(UserConfirmed) -> materialize M -> mark candidate CONFIRMED -> issue MEMORY_LIFECYCLE
//! ticket -> COMMIT) lives in `adapters::distill_repo::confirm_candidate_atomically`.
//!
//! What is pure and unit-tested here: the one authority decision the CANDIDATES ruling calls out
//! — the confirmed memory's authority is re-derived by `AuthorityPolicy` for a **UserConfirmed**
//! basis (the candidate's originally-requested class, now justified by the user's confirmation),
//! never inherited from the rejected candidate and never the source origin's lower ceiling.

use humaux_domain::{
    authority::{AuthorityClass, AuthorityPolicy, CandidateRejection, NonEmptyVec},
    evidence::EvidenceOriginClass,
    ids::Scope,
    memory::MemoryType,
    policy::OriginBoundAuthorityPolicy,
};

/// Runs the §10.1 policy for the confirmed memory on a single `UserConfirmed` basis, returning
/// the authorized class for the candidate's originally-requested class. Routed through the real
/// policy (not hard-coded) so a future change to the ceiling table flows here without a second
/// copy of the rule.
///
/// `Err(CandidateRejection)` is returned when even a `UserConfirmed` basis cannot justify
/// `requested` (e.g. a candidate that requested `ProjectConstraint`/`ExplicitTaskContext` for a
/// non-Constraint memory — above the UserConfirmed ceiling of `UserCorrection`). The caller
/// surfaces this as an internal invariant / refusal, never a silent downgrade (§10.1 rule 3).
pub fn authorize_confirmed(
    requested: AuthorityClass,
    memory_type: MemoryType,
    scope: &Scope,
) -> Result<AuthorityClass, CandidateRejection> {
    let basis = NonEmptyVec::new(vec![EvidenceOriginClass::UserConfirmed])
        .expect("one-element basis is non-empty");
    OriginBoundAuthorityPolicy
        .authorize(requested, memory_type, basis, scope)
        .map(|authorized| authorized.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_domain::ids::{Scope, TenantId};
    use uuid::Uuid;

    fn scope() -> Scope {
        Scope {
            tenant_id: TenantId(Uuid::from_u128(1)),
            user_id: None,
            workspace_id: None,
            repository_id: None,
            task_id: None,
            run_id: None,
            agent_id: None,
        }
    }

    #[test]
    fn user_confirmed_admits_up_to_its_ceiling() {
        // A candidate an AuthenticatedAgent origin could not reach (PrivateKnowledge ceiling) but
        // the user confirmed: ProjectDecision(3) <= UserConfirmed ceiling UserCorrection(4).
        assert_eq!(
            authorize_confirmed(
                AuthorityClass::ProjectDecision,
                MemoryType::Decision,
                &scope()
            )
            .expect("ProjectDecision is under the UserConfirmed ceiling"),
            AuthorityClass::ProjectDecision
        );
        // Never downgraded: the authorized class is exactly what was requested.
        assert_eq!(
            authorize_confirmed(
                AuthorityClass::UserPreference,
                MemoryType::Preference,
                &scope()
            )
            .expect("UserPreference is under the ceiling"),
            AuthorityClass::UserPreference
        );
    }

    #[test]
    fn constraint_memory_reaches_project_constraint() {
        // §10.1 row-1 exception: a Constraint memory confirmed by the user reaches ProjectConstraint.
        assert_eq!(
            authorize_confirmed(
                AuthorityClass::ProjectConstraint,
                MemoryType::Constraint,
                &scope()
            )
            .expect("Constraint memory reaches ProjectConstraint under UserConfirmed"),
            AuthorityClass::ProjectConstraint
        );
    }

    #[test]
    fn above_ceiling_is_rejected_not_downgraded() {
        // ExplicitTaskContext(6) is above the UserConfirmed ceiling for a non-Constraint memory.
        assert_eq!(
            authorize_confirmed(
                AuthorityClass::ExplicitTaskContext,
                MemoryType::Fact,
                &scope()
            )
            .unwrap_err(),
            CandidateRejection::OriginAuthorityCeiling
        );
    }
}
