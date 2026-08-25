//! `domain::error` — `ErrorCode`: the sole closed set of terminal error
//! codes for the whole system (§52.1). A request that carries an
//! `ErrorCode` has no result; that is the entire distinction from
//! `DegradeCode` (defined in `humaux-telemetry`, §53.2), which marks a
//! request that *does* have a result but took a fail-open path. Domain
//! never produces an HTTP status — the MCP/REST mapping lives in
//! `crates/protocol/src/error_map.rs` (§52.1).

use std::fmt;

/// Generates `ErrorCode`, `ErrorCode::ALL`, and `ErrorCode::as_str` from one
/// variant list (§52.4 G52-2). Before this macro, `ALL` was a hand-written
/// array with no compile-time link to the enum: a 19th variant compiled
/// clean while `ALL`/`as_str` silently stayed at 18, and `error_map::lookup`
/// paniced at runtime instead of failing to build. Expanding the enum and
/// `ALL` from the same token list makes that impossible — adding a variant
/// is the same edit as extending `ALL`, there is no second place to forget.
macro_rules! error_code {
    (
        $(
            $(#[$doc:meta])*
            $variant:ident => $wire:literal,
        )+
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum ErrorCode {
            $(
                $(#[$doc])*
                $variant,
            )+
        }

        impl ErrorCode {
            /// All variants, §52.1 order. Compiler-derived from the same
            /// list that defines the enum (§52.4 G52-2) — cannot drift.
            pub const ALL: [ErrorCode; { [$(stringify!($variant)),+].len() }] = [
                $(ErrorCode::$variant,)+
            ];

            /// SCREAMING_SNAKE wire form — the only serialized shape for
            /// `ErrorCode` (§52.2: "`ErrorCode` 一律 `SCREAMING_SNAKE`，且只有这一种形式").
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(ErrorCode::$variant => $wire,)+
                }
            }
        }
    };
}

error_code! {
    /// Request shape/content fails validation before any domain logic runs.
    InvalidInput => "INVALID_INPUT",
    /// Referenced object does not exist, or is not visible to the caller.
    NotFound => "NOT_FOUND",
    /// Caller identity is missing or invalid.
    Unauthorized => "UNAUTHORIZED",
    /// Caller identity is known but lacks permission for this action.
    Forbidden => "FORBIDDEN",
    /// Requested scope does not belong to the caller's tenant (§10160, §52.1 direction table).
    TenantBoundary => "TENANT_BOUNDARY",
    /// Blocked on a missing/invalid BYOK credential; known-pending, not failed (§11.3, §2852).
    WaitingKey => "WAITING_KEY",
    /// Short-window abuse-cost limiter tripped (§72.3); counted independently of quota (§72).
    RateLimited => "RATE_LIMITED",
    /// Cycle quota exhausted (§35); counted independently of rate limit and budget (§72).
    QuotaExhausted => "QUOTA_EXHAUSTED",
    /// Caller's plan does not include this capability.
    EntitlementRequired => "ENTITLEMENT_REQUIRED",
    /// Real provider-cost budget exceeded (§72.4 `ProviderCost`); independent of quota (§72).
    CostBudgetExceeded => "COST_BUDGET_EXCEEDED",
    /// Upstream model/embedding/rerank provider rate-limited this system.
    ProviderRateLimited => "PROVIDER_RATE_LIMITED",
    /// Upstream provider failure expected to clear on retry (5xx/timeout, §11.3).
    ProviderTransient => "PROVIDER_TRANSIENT",
    /// Upstream provider failure that retrying will not fix (403 policy denial, §11.3).
    ProviderPermanent => "PROVIDER_PERMANENT",
    /// A required downstream dependency (DB/index/queue) is unavailable.
    DependencyUnavailable => "DEPENDENCY_UNAVAILABLE",
    /// Caller required read-your-write and the projection has not caught up
    /// (§52.2 concept pair with `DegradeCode::ProjectionLag`).
    ProjectionLag => "PROJECTION_LAG",
    /// Caller required a completeness class the system cannot currently
    /// vouch for (§52.2 concept pair with `DegradeCode::CompletenessUnknown`).
    CannotEstablishCompleteness => "CANNOT_ESTABLISH_COMPLETENESS",
    /// Concurrent or idempotency-key conflict (e.g. batch id mismatch, §7484).
    Conflict => "CONFLICT",
    /// Unclassified internal failure.
    Internal => "INTERNAL",
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One row of the §52.2 cross-layer concept-pair registry: an `ErrorCode`
/// and a `DegradeCode` variant (named here by string — this crate may not
/// depend on `humaux-telemetry`, §3/§78.3) that describe the same
/// underlying condition at two different layers, plus whether their wire
/// forms are literally the same string after `fold` (§53.2).
///
/// The 判据 prose itself (which layer applies for a given occurrence) lives
/// only in spec §52.2 — see `spec_ref` — so it cannot drift from a copy
/// pasted into source (repo `CLAUDE.md`: "判据正文不得复制到任何第二处").
pub struct ConceptPair {
    /// `ErrorCode` wire form (`as_str()`).
    pub error_code: &'static str,
    /// `DegradeCode` variant name, Rust-source PascalCase literal (§53.2).
    pub degrade_variant: &'static str,
    /// §52.2 "字面同名" column: whether `fold(degrade_variant) == error_code`
    /// (`fold` defined in `humaux-telemetry::degrade`, §53.2). Checked
    /// against the live enums by `humaux-telemetry`'s own G52-3 test; this
    /// bool is the human-maintained expectation that test verifies.
    pub literally_same: bool,
    /// Anchor to the spec section carrying the terminate-vs-degrade 判据
    /// prose for this pair — not a copy of it.
    pub spec_ref: &'static str,
}

/// §52.2 registry, 2 rows, verbatim. Any `DegradeCode` variant whose
/// `fold()` equals an `ErrorCode` wire form but is *not* listed here is
/// "the same concept with two enums", not a coincidence, and must be CI-red
/// (§52.2 final sentence, G52-3).
pub const CONCEPT_PAIRS: [ConceptPair; 2] = [
    ConceptPair {
        error_code: ErrorCode::ProjectionLag.as_str(),
        degrade_variant: "ProjectionLag",
        literally_same: true,
        spec_ref: "§52.2",
    },
    ConceptPair {
        error_code: ErrorCode::CannotEstablishCompleteness.as_str(),
        degrade_variant: "CompletenessUnknown",
        literally_same: false,
        spec_ref: "§52.2",
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    /// G52-2 left operand: the Rust enum's own variant count == 18.
    /// `ALL` is macro-derived from the enum's variant list (see
    /// `error_code!` above), so this can no longer silently pass while a
    /// 19th variant exists — the injection §52.4 G52-2 names now reaches it.
    #[test]
    fn all_has_18_entries() {
        assert_eq!(ErrorCode::ALL.len(), 18);
    }

    /// §52.2: `ErrorCode` has exactly one serialized form, SCREAMING_SNAKE,
    /// and every variant round-trips through it distinctly (no collisions).
    #[test]
    fn as_str_is_screaming_snake_and_unique() {
        let mut seen = std::collections::HashSet::new();
        for code in ErrorCode::ALL {
            let s = code.as_str();
            assert!(
                s.chars().all(|c| c.is_ascii_uppercase() || c == '_'),
                "{s} is not SCREAMING_SNAKE"
            );
            assert!(seen.insert(s), "duplicate wire form {s}");
        }
        assert_eq!(seen.len(), 18);
    }

    /// §52.2 table: only the `PROJECTION_LAG` row is literally-same; this
    /// pins the registry's own claim so a later edit that silently flips it
    /// fails loudly here (the live-enum cross-check is G52-3 in telemetry).
    #[test]
    fn concept_pairs_match_spec_table() {
        assert_eq!(CONCEPT_PAIRS.len(), 2);
        assert_eq!(CONCEPT_PAIRS[0].error_code, "PROJECTION_LAG");
        assert_eq!(CONCEPT_PAIRS[0].degrade_variant, "ProjectionLag");
        assert!(CONCEPT_PAIRS[0].literally_same);
        assert_eq!(CONCEPT_PAIRS[1].error_code, "CANNOT_ESTABLISH_COMPLETENESS");
        assert_eq!(CONCEPT_PAIRS[1].degrade_variant, "CompletenessUnknown");
        assert!(!CONCEPT_PAIRS[1].literally_same);
    }

    /// Every `CONCEPT_PAIRS[i].error_code` must name a real `ErrorCode`
    /// variant — a rename on either side must not leave the registry
    /// pointing at a wire form that no longer exists.
    #[test]
    fn concept_pairs_error_code_matches_a_real_variant() {
        for pair in CONCEPT_PAIRS {
            assert!(
                ErrorCode::ALL.iter().any(|c| c.as_str() == pair.error_code),
                "CONCEPT_PAIRS entry {} has no matching ErrorCode variant",
                pair.error_code
            );
        }
    }
}
