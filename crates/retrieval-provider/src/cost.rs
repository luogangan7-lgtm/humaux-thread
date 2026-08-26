//! §19 `费用计算：ModelCallLedger + PricingVersion` — pure cost arithmetic, no SQLx/DB access
//! (§3/§78.3). [`compute_cost`] is the one function that turns a measured/estimated usage
//! snapshot plus a [`crate::pricing::PricingVersion`] into a cost number; every caller (the
//! reserve-time estimate and the finalize-time actual in
//! `crates/adapters/src/model_call_ledger.rs`) goes through it, so "estimated_cost" and
//! "actual_cost" are never two different formulas that happen to agree by convention.

use crate::pricing::PricingVersion;

/// The subset of §19.1 ModelCallLedger's usage fields [`compute_cost`] needs. Used both for a
/// pre-call *estimate* (reserve()) and the provider's actually-reported usage (finalize()) —
/// same type, same formula, different numbers is exactly what "estimated vs actual" means
/// here; there is no second `EstimatedUsage`/`ActualUsage` pair of types.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UsageSnapshot {
    /// §19 rerank formula: `query_tokens * document_count + sum(document_tokens)`; for a
    /// single-input embedding call this is simply the input's own token count. Whichever
    /// number is actually billed — not `input_tokens` when the two differ (§19.1 keeps both
    /// as separate ModelCallLedger columns; only this one feeds cost).
    pub billable_tokens: u64,
    /// Generated/output tokens for a generative purpose (e.g. `query_rewrite`, widened onto
    /// `model_call_ledger_purpose_known` by migrations/0094). Zero for a purely-embedding/
    /// rerank call, which has no output-token dimension to bill. Feeds
    /// `price.output_token_price` in [`compute_cost`] — a pricing row that sets that field
    /// with this left at 0 legitimately bills 0 for the output component, same as any other
    /// priced-but-unused dimension.
    pub output_tokens: u64,
    /// Whether this call used the provider's batch (discounted) price tier — §19's own
    /// example: `text-embedding-v4 Batch ¥0.25` vs non-batch `¥0.5`.
    pub batch: bool,
}

/// §19 "费用计算：ModelCallLedger + PricingVersion". `price.input_token_price` and
/// `price.output_token_price` are both per-1M-token rates (§19's own units — precondition on
/// every `PricingVersion` this module is handed, not restated per call); `batch_discount`/
/// `request_price`/`output_token_price` apply only when present — absent from `price` means
/// "not part of this provider's price shape", not zero. `batch_discount` only ever discounts
/// `input_token_price` (its own field doc): output tokens bill at the full
/// `output_token_price` regardless of `usage.batch`, since §19 documents no batch tier for
/// generative output pricing.
///
/// No literal price appears in this function's body (§19/§78.1 "价格禁止硬编码在 Rust") — every
/// number that determines the result is a field read off `price`.
pub fn compute_cost(usage: &UsageSnapshot, price: &PricingVersion) -> f64 {
    let discount = if usage.batch {
        price.batch_discount.unwrap_or(0.0)
    } else {
        0.0
    };
    debug_assert!(
        (0.0..=1.0).contains(&discount),
        "batch_discount must be a 0.0..=1.0 fraction (DB CHECK enforces this on write; a \
         Rust-constructed PricingVersion must too, or the formula below goes negative)"
    );
    let input_per_token = price.input_token_price * (1.0 - discount);
    let output_per_token = price.output_token_price.unwrap_or(0.0);
    (usage.billable_tokens as f64 / 1_000_000.0) * input_per_token
        + (usage.output_tokens as f64 / 1_000_000.0) * output_per_token
        + price.request_price.unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn price(input_token_price: f64) -> PricingVersion {
        PricingVersion {
            input_token_price,
            output_token_price: None,
            request_price: None,
            batch_discount: None,
            effective_from: 0,
            effective_to: None,
        }
    }

    #[test]
    fn per_million_token_pricing_scales_linearly() {
        let p = price(0.5); // §19 bootstrap: ¥0.5 / 1M input tokens
        let usage = UsageSnapshot {
            billable_tokens: 1_000_000,
            output_tokens: 0,
            batch: false,
        };
        assert_eq!(compute_cost(&usage, &p), 0.5);

        let half = UsageSnapshot {
            billable_tokens: 500_000,
            output_tokens: 0,
            batch: false,
        };
        assert_eq!(compute_cost(&half, &p), 0.25);
    }

    #[test]
    fn batch_discount_only_applies_when_the_call_is_batched() {
        let p = PricingVersion {
            batch_discount: Some(0.5), // §19 bootstrap: batch ¥0.25 == 0.5 off ¥0.5
            ..price(0.5)
        };
        let non_batch = UsageSnapshot {
            billable_tokens: 1_000_000,
            output_tokens: 0,
            batch: false,
        };
        let batch = UsageSnapshot {
            billable_tokens: 1_000_000,
            output_tokens: 0,
            batch: true,
        };
        assert_eq!(compute_cost(&non_batch, &p), 0.5);
        assert_eq!(compute_cost(&batch, &p), 0.25);
    }

    /// The major finding this fixes: `output_token_price` was a `PricingVersion` field
    /// nothing in this function ever read, so a priced output-token dimension silently billed
    /// 0 regardless of `usage.output_tokens`. Also pins that the batch discount — scoped to
    /// `input_token_price` by its own field doc — never applies to the output-token term.
    #[test]
    fn output_token_price_bills_the_generated_tokens() {
        let p = PricingVersion {
            output_token_price: Some(2.0),
            batch_discount: Some(0.5),
            ..price(0.5)
        };
        let usage = UsageSnapshot {
            billable_tokens: 1_000_000, // input: 0.5 * (1-0.5) = 0.25 (batched)
            output_tokens: 1_000_000,   // output: 2.0 (never discounted)
            batch: true,
        };
        assert_eq!(compute_cost(&usage, &p), 2.25);

        // output_token_price absent from the pricing row ⇒ output tokens bill 0, not an error.
        let no_output_price = price(0.5);
        let same_usage = UsageSnapshot {
            billable_tokens: 0,
            output_tokens: 1_000_000,
            batch: false,
        };
        assert_eq!(compute_cost(&same_usage, &no_output_price), 0.0);
    }

    #[test]
    fn request_price_is_a_flat_addition() {
        let p = PricingVersion {
            request_price: Some(0.01),
            ..price(0.5)
        };
        let usage = UsageSnapshot {
            billable_tokens: 0,
            output_tokens: 0,
            batch: false,
        };
        assert_eq!(compute_cost(&usage, &p), 0.01);
    }

    /// Estimated vs actual: the same `compute_cost` formula fed a smaller pre-call estimate
    /// and a larger post-call actual usage produces two different, individually correct
    /// numbers — not two different code paths.
    #[test]
    fn estimated_and_actual_usage_produce_independently_correct_costs() {
        let p = price(0.5);
        let estimated = UsageSnapshot {
            billable_tokens: 400_000,
            output_tokens: 0,
            batch: false,
        };
        let actual = UsageSnapshot {
            billable_tokens: 550_000,
            output_tokens: 0,
            batch: false,
        };
        assert_eq!(compute_cost(&estimated, &p), 0.2);
        assert_eq!(compute_cost(&actual, &p), 0.275);
        assert_ne!(compute_cost(&estimated, &p), compute_cost(&actual, &p));
    }

    /// §19 core acceptance (paired with `pricing::resolve`'s own test of the same property):
    /// once a `PricingVersion` is resolved for a historical call, feeding it the same usage
    /// always yields the same cost — recomputing never drifts on its own. The "a later price
    /// update doesn't change it" half lives in `pricing::resolve`'s test (this function has no
    /// notion of "later" — that's `resolve`'s job); this test pins the other half of the same
    /// invariant: this function itself is pure/deterministic given a snapshot.
    #[test]
    fn same_usage_and_pricing_snapshot_always_yields_the_same_cost() {
        let p = price(0.5);
        let usage = UsageSnapshot {
            billable_tokens: 123_456,
            output_tokens: 0,
            batch: false,
        };
        let first = compute_cost(&usage, &p);
        let second = compute_cost(&usage, &p);
        assert_eq!(first, second);
    }
}
