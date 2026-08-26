//! §19 Pricing Registry — pure snapshot resolution over `control.provider_pricing_versions`
//! rows (§19 "字段": provider_id/model_id/region/pricing_version/currency/
//! input_token_price/output_token_price?/request_price?/batch_discount?/effective_from/
//! effective_to/source_ref/verified_at). "模型价格不能硬编码在 Rust" (§19/§78.1) — every price
//! this module ever sees arrives as a [`PricingVersion`] value the caller loaded from that
//! table; nothing here contains a numeric literal for a real provider's price.
//!
//! No SQLx/DB access (§3/§78.3) — the DB-backed lookup lives in
//! `crates/adapters/src/model_call_ledger.rs`, which `SELECT`s the candidate rows and hands
//! them to [`resolve`]. Timestamps are plain Unix seconds (`i64`), not `time::OffsetDateTime`,
//! so this crate stays dependency-free — the adapters-crate caller converts via
//! `OffsetDateTime::unix_timestamp()` at the boundary.

/// One `control.provider_pricing_versions` row (migrations/0095), the fields
/// [`resolve`]/[`crate::cost::compute_cost`] actually need. `provider_id`/`model_id`/`region`
/// are deliberately absent — the caller has already filtered to one candidate set (`WHERE
/// provider_id = $1 AND model_id = $2 AND region = $3`) before calling [`resolve`], the same
/// division of labor `humaux_domain::egress::authorize`'s callers use for `ProcessorId`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PricingVersion {
    /// §19: currency units per 1,000,000 input tokens.
    pub input_token_price: f64,
    pub output_token_price: Option<f64>,
    /// Flat per-request fee, added on top of the per-token cost.
    pub request_price: Option<f64>,
    /// Fraction (0.0..=1.0) off `input_token_price` for a batch-priced call.
    pub batch_discount: Option<f64>,
    /// Unix seconds — inclusive.
    pub effective_from: i64,
    /// Unix seconds — exclusive; `None` means still open-ended/current.
    pub effective_to: Option<i64>,
}

impl PricingVersion {
    fn covers(&self, at: i64) -> bool {
        self.effective_from <= at && self.effective_to.is_none_or(|end| at < end)
    }
}

/// §19 "历史调用永远按当时 pricing snapshot 归因": returns the row in `versions` whose
/// `[effective_from, effective_to)` window contains `at` — never "whichever price is current
/// right now". Called with the *same* `at` (a `ModelCallLedger.called_at`) both when a call is
/// first reserved/finalized and, later, when re-deriving that same historical row's cost after
/// `control.provider_pricing_versions` has gained newer rows — this is the one function that
/// makes both calls resolve to the identical [`PricingVersion`], which is this task's core
/// acceptance property (`crates/adapters/tests/model_call_ledger.rs`).
///
/// `versions` is assumed already filtered to one `(provider_id, model_id, region)` — see the
/// struct doc. Ambiguous overlapping windows are a data bug the DB layer prevents going
/// forward (migrations/0097's `provider_pricing_versions_one_open_window` unique index closes
/// the ordinary "leave two open-ended rows" hole 0095 originally left), but this function does
/// not lean on that guarantee: among every row whose window covers `at`, it picks the one with
/// the greatest `effective_from` — the most-recently-opened covering window — independent of
/// whatever order `versions` arrives in. The sole caller
/// (`crates/adapters/src/model_call_ledger.rs::load_pricing_versions`) happens to already sort
/// `effective_from DESC`, but that ordering is no longer load-bearing for correctness here.
pub fn resolve(versions: &[PricingVersion], at: i64) -> Option<&PricingVersion> {
    versions
        .iter()
        .filter(|v| v.covers(at))
        .max_by_key(|v| v.effective_from)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(from: i64, to: Option<i64>, price: f64) -> PricingVersion {
        PricingVersion {
            input_token_price: price,
            output_token_price: None,
            request_price: None,
            batch_discount: None,
            effective_from: from,
            effective_to: to,
        }
    }

    #[test]
    fn resolves_the_window_containing_at() {
        let versions = [version(0, Some(100), 0.5), version(100, None, 0.75)];
        assert_eq!(resolve(&versions, 50).unwrap().input_token_price, 0.5);
        assert_eq!(resolve(&versions, 100).unwrap().input_token_price, 0.75);
        assert_eq!(resolve(&versions, 99).unwrap().input_token_price, 0.5);
    }

    #[test]
    fn effective_to_is_exclusive() {
        let versions = [version(0, Some(100), 0.5)];
        assert!(resolve(&versions, 100).is_none());
    }

    #[test]
    fn open_ended_window_covers_everything_from_effective_from() {
        let versions = [version(100, None, 0.75)];
        assert_eq!(
            resolve(&versions, i64::MAX).unwrap().input_token_price,
            0.75
        );
        assert!(resolve(&versions, 99).is_none());
    }

    #[test]
    fn no_covering_window_returns_none() {
        let versions = [version(0, Some(10), 0.5)];
        assert!(resolve(&versions, 20).is_none());
    }

    /// Root-cause coverage for the doc's "independent of `versions`' order" claim: on an
    /// accidental overlap, the row with the greatest `effective_from` wins regardless of
    /// which order the slice presents them in — not just under the caller's actual
    /// `effective_from DESC` ordering.
    #[test]
    fn on_an_overlap_the_newest_effective_from_wins_regardless_of_slice_order() {
        let older = version(0, None, 0.5);
        let newer = version(50, None, 0.9);

        let desc = [newer, older];
        let asc = [older, newer];
        assert_eq!(resolve(&desc, 75).unwrap().input_token_price, 0.9);
        assert_eq!(resolve(&asc, 75).unwrap().input_token_price, 0.9);
    }

    /// §19 core acceptance: a historical timestamp resolves to the same row regardless of
    /// what gets appended to `versions` later — the exact property `compute_cost` relies on to
    /// make "recompute this old call's cost" stable across a price update.
    #[test]
    fn a_historical_timestamp_still_resolves_to_the_old_row_after_a_newer_one_is_added() {
        let historical_at = 50;
        let before_update = [version(0, None, 0.5)];
        assert_eq!(
            resolve(&before_update, historical_at)
                .unwrap()
                .input_token_price,
            0.5
        );

        // A price update: the old row is closed at effective_to=100, a new row opens there.
        let after_update = [version(0, Some(100), 0.5), version(100, None, 0.9)];
        assert_eq!(
            resolve(&after_update, historical_at)
                .unwrap()
                .input_token_price,
            0.5,
            "the historical call's own timestamp must still land in the pre-update window"
        );
    }
}
