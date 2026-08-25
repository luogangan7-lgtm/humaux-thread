//! `retrieval::completeness` — `LedgerCounts` / `PipelineCompleteness` / `CompletenessClass` /
//! `FreshnessClass` skeleton (§22.5 / §59).
//!
//! The `ledger::close` / `classify()` implementations (the sole construction point, A1/A2
//! determination) are out of scope for T0.6; this module lands only these four types'
//! skeletons for a later task to wire the constructors onto.
//!
//! Note: the exhaustiveness test at the bottom of this file pins variant *count and shape*,
//! not the verbatim wire name of each variant. Verbatim string pinning is deferred to the
//! §78.2 DB↔Rust contract-test card, not covered here.
//!
//! Spec adjudication (recorded here so a later wave does not flip it back): §22.5 and §59
//! disagree on two points for this module —
//! 1. Visibility: §59's code block writes `pub enum CompletenessClass`; §22.5 freezes it as
//!    private, produced only by the sole constructor `classify()`. §22 is the home chapter
//!    for the Completeness Contract, so per CLAUDE.md ("on conflict with spec, the spec's own
//!    home chapter wins") §22.5
//!    wins — `CompletenessClass` is `pub(crate)` here. `FreshnessClass` has no such rule
//!    (§21.5 imposes no privacy constraint) and stays `pub`.
//! 2. Shape: §22.5's first `classify()` match arm is
//!    `LedgerClosure::Broken(_) => CannotEstablish { reason: "ledger_not_closed" }` (a reason
//!    field), while §59 — and this skeleton — make `CannotEstablish` a bare unit variant.
//!    Left unresolved here on purpose: this task does not implement `classify()`, and
//!    changing the variant shape without it would just guess. Whoever schedules `classify()`
//!    must adjudicate this first, otherwise `reason` gets bolted on as a loose `&'static str`
//!    beside the enum, which is the stringly-typed domain §78.2 bans.

/// Ledger's six fields (§22.5): produced by `ledger::close(repo, stream_key)` taking three
/// independent reads (`stream_log_agg` / `count_open_gaps` / `contiguous_done_prefix`) and
/// judging A1 on the spot. Callers cannot reach the fields, nor assemble a
/// `LedgerClosure::Closed` themselves (that variant belongs to `ledger::close`'s future
/// output, not to this module) — fields are all private, the sole construction point is left
/// for `ledger::close` (not implemented by this task). The architecture-check for "field set
/// is exactly these 6" is §23.1②.
// ponytail: no consumer at skeleton stage (the sole construction point, ledger::close, is a
// later task); allow(dead_code) on the whole struct — delete this line once the constructor
// lands and the struct is consumed.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct LedgerCounts {
    expected: u64,
    done: u64,
    deleted: u64,
    skipped: u64,
    open_gaps: u64,
    pending: u64,
}

/// §22.5 / §59: same crate (`completeness`) as `LedgerCounts`; only ever surfaces externally
/// as the §23.3 JSON envelope, `LedgerCounts` itself is never exposed as a pub API, so this
/// struct is `pub(crate)`.
///
/// No `Default`: the projection section can only come from `ledger::close`'s output —
/// conjuring an all-zero ledger out of thin air via `default` is exactly the path §22.5's
/// "callers cannot assemble a `Closed` themselves" rule exists to block.
// ponytail: same as above, no consumer at skeleton stage — delete once the constructor lands.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct PipelineCompleteness {
    pub evidence_expected: Option<u64>,
    pub evidence_persisted: u64,
    pub knowledge_eligible: u64,
    pub knowledge_processed: u64,
    pub knowledge_waiting_key: u64,
    pub knowledge_failed: u64,
    /// The ledger side's six numbers are not restated in this struct — embeds §22.5's
    /// `LedgerCounts` directly.
    pub projection: LedgerCounts,
    /// Standalone column, **must never be folded into `LedgerCounts`** (§22.5 frozen): per
    /// §23.1② the numerator must land outside the ledger — moving it in would make G23-2's
    /// two injections unable to observe their own failure. `None` when unavailable ⇒ the
    /// envelope emits `visible: null` and `class = cannot_establish`; backfilling with
    /// `done - deleted` is forbidden.
    pub projection_visible: Option<u64>,
}

/// Completeness class, a closed set of 4 (§59; degrade direction per §22.5:
/// `EXACT -> SemanticBounded` is not allowed — that transition does not exist in the enum;
/// the sole constructor `classify()` is out of scope for this task).
///
/// `pub(crate)`: §22.5 freezes this as private, produced only by `classify()` — see the
/// module-level spec-adjudication note above for why this overrides §59's `pub` code block.
// ponytail: no consumer at skeleton stage (the sole constructor, classify(), is a later
// task) — now that this is pub(crate) instead of pub, rustc's dead-code lint can see it;
// delete this line once classify() lands and consumes it.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletenessClass {
    /// Every fact in scope was verified present (§59).
    Exact,
    /// Complete along every requested facet, though not exhaustively verified (§59).
    FacetComplete,
    /// Bounded by semantic/embedding recall, not a hard count (§59).
    SemanticBounded,
    /// Completeness could not be determined for this query (§59).
    CannotEstablish,
}

/// Freshness class, a closed set of 4 (§59).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreshnessClass {
    /// Within the fresh window (§59).
    Fresh,
    /// Past fresh but not yet stale (§59).
    Aging,
    /// Past the staleness threshold (§59).
    Stale,
    /// Freshness could not be determined (§59).
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    // `LedgerCounts` / `PipelineCompleteness` field-set and wiring tests are deliberately
    // deferred to the task that implements `ledger::close` (fixtures should be built through
    // that constructor, not ad-hoc struct literals here). Both types' fields are private with
    // no `pub(crate)` constructor precisely because `ledger::close` is meant to be the *sole*
    // construction point (CLAUDE.md hard boundary "sole construction point pattern …
    // ledger::close … assert == 1, not <= 1"; §59.1 G59-3). A `LedgerCounts { .. }` literal
    // here would pre-burn that check's positive control: once `ledger::close` lands, the
    // literal count would already be 2 before its own constructor is even the second, forcing
    // either a red build or a
    // `#[cfg(test)]` carve-out — and a carve-out is exactly what makes G59-3's fault
    // injection ("write one struct literal elsewhere ⇒ count goes 1 → 2 ⇒ red") unobservable.

    #[test]
    fn completeness_class_and_freshness_class_have_four_variants_each() {
        fn assert_completeness_exhaustive(c: CompletenessClass) {
            match c {
                CompletenessClass::Exact
                | CompletenessClass::FacetComplete
                | CompletenessClass::SemanticBounded
                | CompletenessClass::CannotEstablish => {}
            }
        }
        fn assert_freshness_exhaustive(f: FreshnessClass) {
            match f {
                FreshnessClass::Fresh
                | FreshnessClass::Aging
                | FreshnessClass::Stale
                | FreshnessClass::Unknown => {}
            }
        }
        assert_completeness_exhaustive(CompletenessClass::Exact);
        assert_freshness_exhaustive(FreshnessClass::Fresh);
    }
}
