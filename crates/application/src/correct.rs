//! `application::correct` — §Q4 `memory.correct` use case, pure half (ADR-0025).
//!
//! Spec :8568 states the hard rule (a user edit writes a Correction Event plus a NEW version
//! and never edits historical Evidence in place) but defines no shape; research question 4's
//! answer (ADR-0025) is: no new table — a correction is one transaction that inserts a new
//! DirectUserInput Evidence `E2` + a new Memory version `M2`, supersedes `M1` by `M2`, and
//! appends a `SUPERSEDE` lifecycle event with reason `USER_CORRECTION` (restorable via card 3).
//!
//! The IO order (reserve BMO -> consume token -> visibility -> insert E2 -> materialize M2 ->
//! supersede UPDATE -> lifecycle append -> COMMIT) lives in `adapters::memory_governance_repo`.
//! What is pure and unit-tested here: the one authority decision the CORRECTION ruling calls
//! out — `M2`'s authority is derived by `AuthorityPolicy` for a DirectUserInput basis, **not
//! inherited** from `M1`. The claim-binding check (token is *this* correct on *this* target,
//! no successor) lives inline in the adapter's `validate`, exactly where supersede/restore/
//! archive keep theirs (ponytail: one `if` at the trust boundary, not a reusable abstraction).

use humaux_domain::{
    authority::{AuthorityClass, AuthorityPolicy, CandidateRejection, NonEmptyVec},
    evidence::EvidenceOriginClass,
    ids::Scope,
    memory::MemoryType,
    policy::OriginBoundAuthorityPolicy,
};

/// The `AuthorityClass` a direct user correction requests for the new version `M2`.
///
/// A correction's Evidence origin is always [`EvidenceOriginClass::DirectUserInput`], so the
/// §10.1 origin-bound ceiling for that origin (given the memory's type) is exactly the class a
/// user correction is entitled to — `UserCorrection` for most types, raised to
/// `ProjectConstraint` for a `Constraint` memory (§10.1 row-1 exception). Requesting the
/// ceiling itself and running it back through `OriginBoundAuthorityPolicy::authorize` means
/// `M2` gets an authority the policy positively grants on a DirectUserInput basis, never one
/// silently inherited from `M1` (which could sit above what the corrected Evidence justifies).
#[must_use]
pub fn correction_requested_class(memory_type: MemoryType) -> AuthorityClass {
    EvidenceOriginClass::DirectUserInput.authority_ceiling(memory_type)
}

/// Runs the §10.1 policy for `M2` on a single DirectUserInput basis, returning the authorized
/// class. By construction (`requested == the DirectUserInput ceiling`) this always authorizes,
/// but it is routed through the real policy — not hard-coded — so a future change to the
/// ceiling table flows here without a second copy of the rule. `Err` only if the policy itself
/// rejects (it cannot, for `requested == ceiling`), surfaced as the ceiling class fallback's
/// absence — the caller treats an `Err` as an internal invariant break.
pub fn authorize_correction(
    memory_type: MemoryType,
    scope: &Scope,
) -> Result<AuthorityClass, CandidateRejection> {
    let requested = correction_requested_class(memory_type);
    let basis = NonEmptyVec::new(vec![EvidenceOriginClass::DirectUserInput])
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
            tenant_id: TenantId(Uuid::now_v7()),
            user_id: None,
            workspace_id: None,
            repository_id: None,
            task_id: None,
            run_id: None,
            agent_id: None,
        }
    }

    #[test]
    fn direct_user_correction_authorizes_at_the_origin_ceiling_not_inherited() {
        // A plain fact correction reaches UserCorrection; a Constraint correction reaches
        // ProjectConstraint (the §10.1 row-1 exception) — both positively granted, never a
        // class inherited from the corrected memory.
        assert_eq!(
            correction_requested_class(MemoryType::Fact),
            AuthorityClass::UserCorrection
        );
        assert_eq!(
            correction_requested_class(MemoryType::Constraint),
            AuthorityClass::ProjectConstraint
        );
        for memory_type in [
            MemoryType::Fact,
            MemoryType::Constraint,
            MemoryType::Decision,
        ] {
            assert_eq!(
                authorize_correction(memory_type, &scope()).unwrap(),
                correction_requested_class(memory_type),
                "policy grants exactly the requested (=ceiling) class"
            );
        }
    }
}
