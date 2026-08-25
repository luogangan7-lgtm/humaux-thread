//! `domain::evidence` — `EvidenceOriginClass` / `InstructionDisposition` (§8.7 / §59).
//!
//! §8.7: `origin_class` is stamped by **ingress path + AuthContext**, never self-reported by
//! the client/model through a JSON parameter — this is the first boundary against private
//! Memory Poisoning. Taint must survive derivation: an Observation/Memory's provenance must
//! trace back to its origin Evidence, and Agent/LLM summarization must never upgrade
//! `origin_class` (§59.1 I7: AuthorityPolicy must not let summarization launder
//! UploadedArtifact/ExternalContent/ToolResult/AuthenticatedAgent into a high-authority
//! class). `SystemMigration` only inherits the legacy Evidence's original origin; with no
//! recoverable legacy origin it defaults to the lower-privilege treatment.
//!
//! Note: the exhaustiveness test below pins variant *count and shape*, not the verbatim
//! wire name of each variant. Verbatim string pinning is deferred to the §78.2 DB↔Rust
//! contract-test card, not covered here.

/// Evidence's ingress origin classification, a frozen closed set of 9 variants (§8.7).
///
/// A user uploading a document != the user personally asserting every sentence in it:
/// `UploadedArtifact` / `ExternalContent` remain low behavioral-authority sources even when
/// the upload action itself was performed by an authenticated user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceOriginClass {
    /// The user typed or spoke this directly (§8.7).
    DirectUserInput,
    /// The user explicitly confirmed this content (§8.7).
    UserConfirmed,
    /// A tenant administrator asserted this (§8.7).
    TenantAdmin,
    /// An authenticated agent produced this (§8.7).
    AuthenticatedAgent,
    /// A trusted connector ingested this (§8.7).
    TrustedConnector,
    /// A tool call returned this (§8.7).
    ToolResult,
    /// The user uploaded this as a file/artifact (§8.7) — low behavioral authority.
    UploadedArtifact,
    /// This was ingested from outside the trust boundary (§8.7) — low behavioral authority.
    ExternalContent,
    /// Carried over from a legacy Evidence during system migration (§8.7).
    SystemMigration,
}

/// Whether an instruction inside one Evidence is eligible to influence Agent behavior
/// (the behavioral-authority binary in the §8.7 context).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstructionDisposition {
    /// Data only — must never be executed as an instruction to the Agent.
    DataOnly,
    /// Eligible to influence Agent behavior (still bound by the §59.1 I7 ceiling).
    BehaviorEligible,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_origin_class_has_exactly_nine_variants() {
        let all = [
            EvidenceOriginClass::DirectUserInput,
            EvidenceOriginClass::UserConfirmed,
            EvidenceOriginClass::TenantAdmin,
            EvidenceOriginClass::AuthenticatedAgent,
            EvidenceOriginClass::TrustedConnector,
            EvidenceOriginClass::ToolResult,
            EvidenceOriginClass::UploadedArtifact,
            EvidenceOriginClass::ExternalContent,
            EvidenceOriginClass::SystemMigration,
        ];
        assert_eq!(all.len(), 9);

        fn assert_exhaustive(c: EvidenceOriginClass) {
            match c {
                EvidenceOriginClass::DirectUserInput
                | EvidenceOriginClass::UserConfirmed
                | EvidenceOriginClass::TenantAdmin
                | EvidenceOriginClass::AuthenticatedAgent
                | EvidenceOriginClass::TrustedConnector
                | EvidenceOriginClass::ToolResult
                | EvidenceOriginClass::UploadedArtifact
                | EvidenceOriginClass::ExternalContent
                | EvidenceOriginClass::SystemMigration => {}
            }
        }
        for c in all {
            assert_exhaustive(c);
        }
    }

    #[test]
    fn instruction_disposition_has_exactly_two_variants() {
        fn assert_exhaustive(d: InstructionDisposition) {
            match d {
                InstructionDisposition::DataOnly | InstructionDisposition::BehaviorEligible => {}
            }
        }
        assert_exhaustive(InstructionDisposition::DataOnly);
        assert_exhaustive(InstructionDisposition::BehaviorEligible);
    }
}
