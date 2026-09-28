//! `domain::tests::authority_policy_g59_6` — G59-6 / Private Authority Boundary injected-fault matrix (§59.1 "###
//!   G59-6 / Private Authority Boundary", moved here from §10 per ADR-0001).
//! Depends-on: crates=[]; services=[]; env=[]; modules=[domain::authority, domain::evidence, domain::ids, domain::memory, domain::policy]
//! Called-by: [cargo-test]
//! Invariants: []
//! Spec: Baseline §10; §10.1; §53.3; ADR-0001
//!
//! The four cases A/B/C/D below must all
//! stay present together — dropping the positive controls (C, and D's outcome half) would let a
//! blanket-reject implementation pass the malicious cases too (§53.3 规则 3).
//!
//! Lives under `tests/` rather than `#[cfg(test)]` inside `src/policy.rs` so it exercises
//! `OriginBoundAuthorityPolicy` exactly the way an outside caller (e.g. the `remember()` MCP
//! tool handler) would, through the crate's public surface only.

use humaux_domain::authority::{AuthorityClass, AuthorityPolicy, CandidateRejection, NonEmptyVec};
use humaux_domain::evidence::{EvidenceOriginClass, InstructionDisposition};
use humaux_domain::ids::{Scope, TenantId};
use humaux_domain::memory::MemoryType;
use humaux_domain::policy::OriginBoundAuthorityPolicy;

fn scope() -> Scope {
    Scope {
        tenant_id: TenantId::new(),
        user_id: None,
        workspace_id: None,
        repository_id: None,
        task_id: None,
        run_id: None,
        agent_id: None,
    }
}

fn basis(origins: &[EvidenceOriginClass]) -> NonEmptyVec<EvidenceOriginClass> {
    NonEmptyVec::new(origins.to_vec()).expect("fixture basis is non-empty")
}

/// **A** (§59.1 G59-6 case A): injected instruction in an `UploadedArtifact` requesting
/// `ProjectConstraint` must be ceiling-rejected, never promoted to a high-authority Memory.
#[test]
fn case_a_uploaded_artifact_requesting_project_constraint_is_ceiling_rejected() {
    let policy = OriginBoundAuthorityPolicy;
    let result = policy.authorize(
        AuthorityClass::ProjectConstraint,
        MemoryType::Constraint,
        basis(&[EvidenceOriginClass::UploadedArtifact]),
        &scope(),
    );
    assert_eq!(
        result,
        Err(CandidateRejection::OriginAuthorityCeiling),
        "case A: UploadedArtifact basis must not justify ProjectConstraint"
    );
}

/// **B** (§59.1 G59-6 case B): instruction-injection content from a `ToolResult` must stay
/// DATA_ONLY, never reach Mandatory Context. Three facets, per §10.1 row 4: the type-level
/// disposition cap never allows `BehaviorEligible` regardless of content (facet 1); the same
/// origin can still land as a plain fact at its own ceiling (facet 2, the positive control); and
/// promoting it past that ceiling is rejected (facet 3). Facet 3's specific reason
/// (`UntrustedInstruction` vs. the generic `OriginAuthorityCeiling` case A gets) is this
/// implementation's chosen split of the closed reason set, not spec-mandated per-case text —
/// §59.1's matrix names no reason code for case B.
#[test]
fn case_b_tool_result_is_data_only_and_cannot_reach_mandatory_context() {
    // Facet 1: the origin's content can never render as anything but DATA_ONLY — written into
    // the type itself (EvidenceOriginClass::max_disposition), not detected from the string.
    assert_eq!(
        EvidenceOriginClass::ToolResult.max_disposition(),
        InstructionDisposition::DataOnly,
        "case B: ToolResult content must render DATA_ONLY, never BehaviorEligible"
    );

    // Facet 2: it CAN still land as a plain fact at its own ceiling (PrivateKnowledge) — proves
    // this isn't a blanket "ToolResult always rejected" fake, only the request over its ceiling
    // (entering Mandatory Context) is refused below.
    let policy = OriginBoundAuthorityPolicy;
    let as_fact = policy.authorize(
        AuthorityClass::PrivateKnowledge,
        MemoryType::Fact,
        basis(&[EvidenceOriginClass::ToolResult]),
        &scope(),
    );
    assert_eq!(
        as_fact,
        Ok(humaux_domain::authority::AuthorizedAuthority(
            AuthorityClass::PrivateKnowledge
        )),
        "case B positive half: ToolResult may still become a PrivateKnowledge fact"
    );

    // Facet 3: promoting the same origin into Mandatory Context (a class above its DATA_ONLY
    // ceiling) is rejected, not silently downgraded. The specific reason asserted below
    // (UntrustedInstruction) is this implementation's choice of which closed-set reason applies
    // to ToolResult specifically, not text the G59-6 matrix itself specifies for case B.
    let as_correction = policy.authorize(
        AuthorityClass::UserCorrection,
        MemoryType::Fact,
        basis(&[EvidenceOriginClass::ToolResult]),
        &scope(),
    );
    assert_eq!(
        as_correction,
        Err(CandidateRejection::UntrustedInstruction),
        "case B: ToolResult must not reach Mandatory-context authority"
    );
}

/// **C** (§59.1 G59-6 case C, positive control): the same content, once the user explicitly
/// confirms it via the UI, gains `UserConfirmed` Evidence and the policy must now authorize it.
/// Same requested class and MemoryType as case A, only the evidence origin changes — this is the
/// sample that would catch an implementation degenerated into "reject everything".
#[test]
fn case_c_user_confirmed_evidence_allows_the_same_request_case_a_rejected() {
    let policy = OriginBoundAuthorityPolicy;
    let result = policy.authorize(
        AuthorityClass::ProjectConstraint,
        MemoryType::Constraint,
        basis(&[EvidenceOriginClass::UserConfirmed]),
        &scope(),
    );
    assert_eq!(
        result,
        Ok(humaux_domain::authority::AuthorizedAuthority(
            AuthorityClass::ProjectConstraint
        )),
        "case C: UserConfirmed evidence must authorize what case A's UploadedArtifact could not"
    );
}

/// **D** (§59.1 G59-6 case D): an Agent calling `remember("user prefers X")` on its own
/// (origin = `AuthenticatedAgent`) must not mint `UserPreference`/`UserCorrection`. Both named
/// classes must be rejected; a positive control alongside proves `AuthenticatedAgent` isn't
/// rejected across the board (§10.1 row 3).
#[test]
fn case_d_authenticated_agent_cannot_mint_user_preference_or_correction() {
    let policy = OriginBoundAuthorityPolicy;

    let preference = policy.authorize(
        AuthorityClass::UserPreference,
        MemoryType::Preference,
        basis(&[EvidenceOriginClass::AuthenticatedAgent]),
        &scope(),
    );
    assert_eq!(
        preference,
        Err(CandidateRejection::OriginAuthorityCeiling),
        "case D: AuthenticatedAgent must not mint UserPreference"
    );

    let correction = policy.authorize(
        AuthorityClass::UserCorrection,
        MemoryType::Fact,
        basis(&[EvidenceOriginClass::AuthenticatedAgent]),
        &scope(),
    );
    assert_eq!(
        correction,
        Err(CandidateRejection::OriginAuthorityCeiling),
        "case D: AuthenticatedAgent must not mint UserCorrection"
    );

    // Positive control: the same origin CAN record a fact/state/outcome at its own ceiling
    // (§10.1 row 3) — without this, a blanket-reject fake would pass the two asserts above too.
    let outcome = policy.authorize(
        AuthorityClass::PrivateKnowledge,
        MemoryType::Outcome,
        basis(&[EvidenceOriginClass::AuthenticatedAgent]),
        &scope(),
    );
    assert!(outcome.is_ok(), "case D positive control must succeed");
}

/// §10.1 rule 1's existential reading, pinned against `policy.rs`'s `authorize` doc comment: a
/// low-ceiling origin (case A's `UploadedArtifact`) sitting in the SAME basis as a confirmed one
/// (case C's `UserConfirmed`) must not lower case C's outcome — one origin covering `requested`
/// is enough, per rule 1's own "必须有满足该 Authority 的 basis Evidence" (§10.1).
#[test]
fn authorize_ceiling_is_existential_over_basis_not_universal() {
    let policy = OriginBoundAuthorityPolicy;
    let result = policy.authorize(
        AuthorityClass::ProjectConstraint,
        MemoryType::Constraint,
        basis(&[
            EvidenceOriginClass::UploadedArtifact,
            EvidenceOriginClass::UserConfirmed,
        ]),
        &scope(),
    );
    assert_eq!(
        result,
        Ok(humaux_domain::authority::AuthorizedAuthority(
            AuthorityClass::ProjectConstraint
        )),
        "a confirmed origin alongside an unconfirmed one must still authorize (existential over basis)"
    );
}

/// §10.1 拒绝原因闭集 must be a function of the `basis` set, not of `Vec` insertion order: both
/// orderings of the same two origins must reject with the same reason.
#[test]
fn authorize_reason_is_independent_of_basis_order() {
    let policy = OriginBoundAuthorityPolicy;
    let forward = policy.authorize(
        AuthorityClass::UserCorrection,
        MemoryType::Fact,
        basis(&[
            EvidenceOriginClass::ToolResult,
            EvidenceOriginClass::UploadedArtifact,
        ]),
        &scope(),
    );
    let reversed = policy.authorize(
        AuthorityClass::UserCorrection,
        MemoryType::Fact,
        basis(&[
            EvidenceOriginClass::UploadedArtifact,
            EvidenceOriginClass::ToolResult,
        ]),
        &scope(),
    );
    assert_eq!(
        forward, reversed,
        "same basis set in a different Vec order must reject with the same reason"
    );
    assert_eq!(forward, Err(CandidateRejection::UntrustedInstruction));
}
