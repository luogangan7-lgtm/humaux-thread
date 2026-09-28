//! `telemetry::degrade` — `DegradeCode`, `Outcome<T>`, and `abstain()`: the single fail-open topology for the whole
//!   workspace (§53).
//! Depends-on: crates=[humaux-domain, smallvec, tracing]; services=[]; env=[]; modules=[domain::error]
//! Called-by: [retrieval::envelope, tests]
//! Invariants: []
//! Spec: Baseline §53.3
//!
//! Every fail-open
//! / degrade / abstain path goes through `abstain()`; a caller that returns
//! a fallback value any other way is what §53.3 rule 1 exists to catch.

use humaux_domain::error::ErrorCode;
use smallvec::{SmallVec, smallvec};
use std::sync::atomic::{AtomicU64, Ordering};

/// Generates `DegradeCode`, `DegradeCode::ALL`, and `DegradeCode::as_str`
/// from one variant list (§52.4 G52-3, §53.3 规则2). Before this macro,
/// `ALL` was a hand-written array with no compile-time link to the enum: an
/// 11th variant compiled clean while `ALL` silently stayed at 10, so
/// `g52_3_literal_intersection_is_exactly_projection_lag` could not observe
/// the addition-direction injection §52.4 names. Expanding the enum and
/// `ALL` from the same token list closes that gap — adding a variant is the
/// same edit as extending `ALL`.
macro_rules! degrade_code {
    ($($(#[$doc:meta])* $variant:ident,)+) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum DegradeCode {
            $($(#[$doc])* $variant,)+
        }

        impl DegradeCode {
            /// All variants. Compiler-derived from the same list that
            /// defines the enum (§52.4 G52-3, §53.3 规则2) — cannot drift.
            pub const ALL: [DegradeCode; { [$(stringify!($variant)),+].len() }] = [
                $(DegradeCode::$variant,)+
            ];

            /// label value == variant name, verbatim (§53.2). Used as the
            /// `degrade_total{code}` label and as the value `tracing::warn!` logs.
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(DegradeCode::$variant => stringify!($variant),)+
                }
            }
        }
    };
}

degrade_code! {
    /// Rerank model actually used differs from the one requested.
    RerankModelMismatch,
    /// Rerank provider call timed out; fell back to unranked/heuristic order (§53.4).
    RerankProviderTimeout,
    /// Embedding provider call timed out.
    EmbedProviderTimeout,
    /// Egress blocked by policy (private-content disclosure guard).
    EgressDenied,
    /// No usable `state_pin` was available for this call.
    StatePinMissing,
    /// More than one `state_pin` candidate matched and none could be preferred.
    StatePinAmbiguous,
    /// Result returned but its completeness class could not be established
    /// (§52.2 concept pair with `ErrorCode::CannotEstablishCompleteness`).
    CompletenessUnknown,
    /// Result returned but is known to be behind the write it should reflect
    /// (§52.2 concept pair with `ErrorCode::ProjectionLag`).
    ProjectionLag,
    /// Graph expansion hit its node/edge cap and was truncated.
    GraphExpandCapped,
    /// §23.1② A2: the ledger says a record is closed but the index does not
    /// have it — visible under-count with no matching ledger movement.
    ProjectionInvisibleLoss,
}

impl DegradeCode {
    /// §23 envelope line format: `fold(variant name)`, SCREAMING_SNAKE.
    /// This form — not `as_str()` — is what serializes into
    /// `completeness.degradations[]`; the two must never be swapped
    /// (§53.2: swapping breaks INV-2's `code=~"Egress.*"` label matcher or
    /// G52-3's fold-and-intersect check, each in one direction only).
    pub fn line_format(self) -> String {
        fold(self.as_str())
    }
}

/// §53.2 `fold`: insert `_` before every non-first uppercase letter, then
/// uppercase the whole string. Variant names contain no digits and no
/// consecutive uppercase letters (enforced by convention here, checked by
/// `fold_is_injective` below), which is exactly the precondition that makes
/// `fold` injective and its output mechanically reversible to the variant
/// name (§53.2).
pub fn fold(variant_name: &str) -> String {
    let mut out = String::with_capacity(variant_name.len() + 4);
    for (i, ch) in variant_name.chars().enumerate() {
        if ch.is_ascii_uppercase() && i != 0 {
            out.push('_');
        }
        out.push(ch.to_ascii_uppercase());
    }
    out
}

/// Inverse of `fold`: SCREAMING_SNAKE → PascalCase. Exists only so tests can
/// assert `fold` is mechanically reversible (§53.2), which is the stated
/// consequence of the "no digits, no consecutive uppercase" precondition.
#[cfg(test)]
fn unfold(folded: &str) -> String {
    folded
        .split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => {
                    first.to_ascii_uppercase().to_string() + &chars.as_str().to_ascii_lowercase()
                }
                None => String::new(),
            }
        })
        .collect()
}

/// `Outcome<T>`: the return shape for every function that can take a
/// fail-open path (§53.2). Returning bare `T` from such a function is what
/// §53.3 rule 1 forbids — `degradations` empty means "no abstain happened",
/// not "this function can't abstain".
///
/// Fields are `pub` (spec-mandated shape), so nothing in the type system
/// stops a caller from constructing a non-empty `degradations` directly
/// instead of going through `abstain()`. That is `abstain()`'s own §53.1
/// single-exit-point claim's blind spot; enforcing it is G80-1's job
/// (architecture-check scanning for direct `Outcome { degradations: ... }`
/// construction outside this module), not something this type can do alone.
pub struct Outcome<T> {
    /// The value to return — the real result, or the fallback `abstain()` was given.
    pub value: T,
    /// Non-empty iff a fail-open path was taken (§53.1); empty means success with no degradation.
    pub degradations: SmallVec<[DegradeCode; 4]>,
}

impl<T> Outcome<T> {
    /// The non-degraded case: `degradations` empty. Exists so a caller with no fail-open path
    /// to report never has to spell `Outcome { value, degradations: Default::default() }` (or
    /// `smallvec![]`) by hand — every such literal outside this module is one more shape G80-1
    /// (architecture-check scan for direct `Outcome { degradations: ... }` construction, see
    /// this type's own doc) would have to special-case as "fine, it's empty" instead of a flat
    /// "not through `abstain()`, therefore red". Routing clean returns through here leaves
    /// `abstain()` as the only remaining place that ever writes a non-empty `degradations`.
    pub fn clean(value: T) -> Self {
        Self {
            value,
            degradations: SmallVec::new(),
        }
    }
}

/// §52: a single request is either `Err(ErrorCode)` and terminated, or
/// `Ok(Outcome<T>)` and successful (possibly degraded) — never both (G52-4).
/// This is not a runtime check: `Result`'s `Ok`/`Err` variants each carry
/// only one of the two enums, so a value with both a non-empty `error` and
/// non-empty `degradations` is not constructible through this alias.
///
/// That type-level guarantee only covers values built through `Response<T>`
/// itself. The scenario G52-4's spec injection names — a caller that
/// `abstain()`s and *then* still returns `Err` on some other path — happens
/// above this alias, at the point two independently-computed halves get
/// combined into one response. This module has no such combination point to
/// test; a real red→green for that injection belongs at the response/
/// envelope serialization boundary (out of this crate's scope) where both
/// halves are actually assembled.
pub type Response<T> = Result<Outcome<T>, ErrorCode>;

/// Process-local placeholder for `degrade_total{code}` (§53.1). Full
/// Prometheus `IntCounterVec` registration lands with the `telemetry::metrics`
/// task; until then this keeps the count observable (§53.4's per-code
/// injection assertions need *a* counter to read) while keeping `abstain()`
/// the one place any counter is touched.
///
// ponytail: no Prometheus `degrade_total{code}` family is emitted at all —
// §53.5 INV-1/INV-2 have no real data source yet. Upgrade path: replace this
// with `prometheus::IntCounterVec` in the `telemetry::metrics` task (§41.2
// `degrade_total{code}`); `label_cardinality`-shaped operands for §53.3 规则2
// must read that counter's live label set, not this process-local stand-in.
struct DegradeTotal([AtomicU64; DegradeCode::ALL.len()]);

impl DegradeTotal {
    // ponytail: 10 literal AtomicU64::new(0) instead of a `[X; N]` repeat
    // expression — AtomicU64 isn't Copy, and naming a `const ZERO` for the
    // repeat trips clippy::declare_interior_mutable_const (a const with
    // interior mutability silently re-evaluates per use site, which is
    // exactly wrong for a shared atomic). Update the count by hand if
    // DegradeCode ever grows past 10.
    const fn new() -> Self {
        DegradeTotal([
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
        ])
    }

    fn inc(&self, code: DegradeCode) {
        let idx = DegradeCode::ALL
            .iter()
            .position(|c| *c == code)
            .expect("DegradeCode::ALL is exhaustive");
        self.0[idx].fetch_add(1, Ordering::Relaxed);
    }

    /// count for one code — exposed for tests / future metrics wiring.
    fn count(&self, code: DegradeCode) -> u64 {
        let idx = DegradeCode::ALL
            .iter()
            .position(|c| *c == code)
            .expect("DegradeCode::ALL is exhaustive");
        self.0[idx].load(Ordering::Relaxed)
    }
}

static DEGRADE_TOTAL: DegradeTotal = DegradeTotal::new();

/// current count for `code` — read-only accessor for注错测试 assertions
/// ("① `degrade_total{code}` 恰 +1", §53.4), without exposing the counter itself.
pub fn degrade_total_count(code: DegradeCode) -> u64 {
    DEGRADE_TOTAL.count(code)
}

/// §53.1 single exit point for every fail-open / degrade / abstain path in
/// the workspace. Warns loudly, bumps the named counter, and returns
/// `fallback` wrapped so the caller's response envelope carries the reason
/// (§23 `completeness.degradations`). No other function may increment
/// `DEGRADE_TOTAL` or construct an `Outcome` with a non-empty
/// `degradations` — that would be a second exit point (§53.1); see
/// `Outcome`'s doc comment for why this module cannot enforce that itself.
pub fn abstain<T>(code: DegradeCode, fallback: T) -> Outcome<T> {
    tracing::warn!(target: "degrade", code = code.as_str(), "abstain");
    DEGRADE_TOTAL.inc(code);
    Outcome {
        value: fallback,
        degradations: smallvec![code],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_domain::error::CONCEPT_PAIRS;

    /// left operand of §53.3 rule 2 and of the fold-injectivity check.
    /// `ALL` is macro-derived from the enum's variant list (see
    /// `degrade_code!` above), so this can no longer silently pass while an
    /// 11th variant exists.
    #[test]
    fn all_has_10_entries() {
        assert_eq!(DegradeCode::ALL.len(), 10);
    }

    /// §53.2: variant names have no digits and no consecutive uppercase
    /// letters (precondition for `fold` being injective and mechanically
    /// reversible), `fold` is injective over the live set, and `unfold(fold(name))`
    /// round-trips back to the exact variant name — the spec explicitly
    /// invites this last assertion ("这条本身可 CI 断言"). Also pins `as_str()`
    /// to the frozen PascalCase label form (starts uppercase, alphanumeric only).
    #[test]
    fn fold_is_injective_over_all_variants() {
        let mut seen = std::collections::HashSet::new();
        for code in DegradeCode::ALL {
            let name = code.as_str();
            assert!(
                !name.chars().any(|c| c.is_ascii_digit()),
                "{name} contains a digit"
            );
            assert!(
                !name
                    .as_bytes()
                    .windows(2)
                    .any(|w| w[0].is_ascii_uppercase() && w[1].is_ascii_uppercase()),
                "{name} has consecutive uppercase letters — fold() would not be reversible"
            );
            assert!(
                name.chars().next().is_some_and(|c| c.is_ascii_uppercase()),
                "{name} is not PascalCase (must start uppercase)"
            );
            assert!(
                name.chars().all(|c| c.is_ascii_alphanumeric()),
                "{name} is not PascalCase (non-alphanumeric char)"
            );
            let folded = fold(name);
            assert!(seen.insert(folded.clone()), "fold collision on {folded}");
            assert_eq!(unfold(&folded), name, "fold({name}) does not round-trip");
        }
        assert_eq!(seen.len(), 10);
    }

    /// §53.2 worked example, verbatim: `ProjectionInvisibleLoss` folds to
    /// `PROJECTION_INVISIBLE_LOSS`.
    #[test]
    fn fold_matches_spec_example() {
        assert_eq!(fold("ProjectionInvisibleLoss"), "PROJECTION_INVISIBLE_LOSS");
        assert_eq!(DegradeCode::ProjectionLag.line_format(), "PROJECTION_LAG");
    }

    /// G52-3: fold every `DegradeCode` variant to SCREAMING_SNAKE, intersect
    /// with the `ErrorCode` wire-form set, and the result must be exactly
    /// the §52.2 registry's `literally_same` rows — derived from
    /// `domain::CONCEPT_PAIRS` itself, not hardcoded, so a registry edit
    /// that isn't matched by a real fold-collision (or vice versa) goes red
    /// here instead of the two silently drifting apart.
    #[test]
    fn g52_3_literal_intersection_is_exactly_projection_lag() {
        let degrade_folded: std::collections::HashSet<String> =
            DegradeCode::ALL.iter().map(|c| c.line_format()).collect();
        let error_codes: std::collections::HashSet<&'static str> =
            ErrorCode::ALL.iter().map(|c| c.as_str()).collect();
        let intersection: std::collections::HashSet<&str> = degrade_folded
            .iter()
            .map(String::as_str)
            .filter(|s| error_codes.contains(s))
            .collect();

        let expected: std::collections::HashSet<&'static str> = CONCEPT_PAIRS
            .iter()
            .filter(|p| p.literally_same)
            .map(|p| p.error_code)
            .collect();
        assert_eq!(
            intersection, expected,
            "live fold-intersection must equal the §52.2 registry's literally_same rows"
        );
        assert_eq!(
            intersection,
            std::collections::HashSet::from(["PROJECTION_LAG"])
        );
    }

    /// §52.2 registry can rot silently if a variant it names is renamed on
    /// either side. Every `CONCEPT_PAIRS[i].degrade_variant` must match a
    /// real `DegradeCode` (the `error_code` half is checked in
    /// `domain::error`, which cannot see `DegradeCode`).
    #[test]
    fn concept_pairs_degrade_variant_matches_a_real_variant() {
        for pair in CONCEPT_PAIRS {
            assert!(
                DegradeCode::ALL
                    .iter()
                    .any(|c| c.as_str() == pair.degrade_variant),
                "CONCEPT_PAIRS entry {} has no matching DegradeCode variant",
                pair.degrade_variant
            );
        }
    }

    /// abstain() is the only place `DEGRADE_TOTAL` moves, and it moves by
    /// exactly one per call, keyed by the code passed in.
    #[test]
    fn abstain_increments_exactly_its_own_code() {
        let before_a = degrade_total_count(DegradeCode::EgressDenied);
        let before_b = degrade_total_count(DegradeCode::StatePinMissing);
        let outcome = abstain(DegradeCode::EgressDenied, 0u8);
        assert_eq!(outcome.value, 0);
        assert_eq!(
            outcome.degradations.as_slice(),
            &[DegradeCode::EgressDenied]
        );
        assert_eq!(degrade_total_count(DegradeCode::EgressDenied), before_a + 1);
        assert_eq!(degrade_total_count(DegradeCode::StatePinMissing), before_b);
    }
}
