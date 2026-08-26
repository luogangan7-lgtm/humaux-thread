//! `retrieval-provider::metrics` — §19 **Provider Plane Observability** / §41.2 registry: the
//! four `retrieval_provider_*` families.
//!
// ponytail: process-local `Mutex<HashMap<..>>` counters, not a real `prometheus::*Vec`
// registration — this workspace has no Prometheus client dependency anywhere yet (see
// `telemetry::degrade`'s `DEGRADE_TOTAL`, the one precedent, whose own doc names the same
// deferral). Upgrade path: swap each `CounterFamily`/`HistogramFamily` body for a real
// `prometheus::IntCounterVec`/`HistogramVec` when that dependency lands workspace-wide; the
// four `pub fn record_*`/`pub fn *_count` signatures below do not need to change.
//!
//! §19 Observability: "禁止 tenant_id / query / memory_id 作为 Prometheus label" — enforced
//! here by construction, not by a runtime denylist: [`record_provider_call`]'s parameter list
//! is exactly `Provider`/`RetrievalPurpose`/`Region`/a closed `result` label/token
//! count/cost amount/`Currency`/latency seconds. None of those types can hold a tenant id, a
//! query string, or a memory id — there is no field anywhere in this module a caller could
//! even attempt to pass one through. Tenant-level cost instead goes to `ModelCallLedger` /
//! `ops.tenant_cost_events` (§19 Observability, a different task's deliverable).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::admission::RetrievalPurpose;

/// §19 OSS 默认配置: "Retrieval Provider 选 Alibaba Cloud / Custom Endpoint" — the only two
/// provider identities the v1 bootstrap set has.
///
// ponytail: 2 variants fixed by the v1 OSS wizard, not an open string (§41.2 R5 low
// cardinality); widen only alongside a real third provider onboarding, which is a spec-table
// edit in its own right (§19: "V2 初始正式支持可以只有 Alibaba Cloud … 但 Provider Contract 与
// DB Schema 从第一版支持多 Provider" — this label enum is deliberately *not* where that
// multi-provider extensibility lives; the `descriptor`/`registry` modules are).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Provider {
    DashScope,
    Custom,
}

impl Provider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DashScope => "dashscope",
            Self::Custom => "custom",
        }
    }
}

/// §19 Retrieval Provider Plane's Alibaba Cloud v1 region bootstrap set.
///
// ponytail: four literal regions, not an open string — same R5 low-cardinality reasoning as
// `Provider`; extend when a real deployment actually onboards a fifth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Region {
    CnHangzhou,
    CnShanghai,
    CnBeijing,
    ApSoutheast1,
}

impl Region {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CnHangzhou => "cn-hangzhou",
            Self::CnShanghai => "cn-shanghai",
            Self::CnBeijing => "cn-beijing",
            Self::ApSoutheast1 => "ap-southeast-1",
        }
    }
}

/// `retrieval_provider_cost_total{..., currency}`'s label — the two currencies the §19
/// bootstrap pricing snapshot and Alibaba Cloud's own billing use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Currency {
    Usd,
    Cny,
}

impl Currency {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Usd => "usd",
            Self::Cny => "cny",
        }
    }
}

/// Maps the two Provider-Plane purposes onto their §41.2 label value. Reuses
/// `crate::admission::RetrievalPurpose` (already the crate-established purpose type, with its
/// own `From<RetrievalPurpose> for humaux_domain::egress::PrivateDataPurpose`) rather than a
/// third parallel enum (R3: "同一被测对象只允许一个名字") — the domain-level
/// `PrivateDataPurpose::UserReasoning` case that a bare `PrivateDataPurpose` parameter would
/// have to reject is structurally absent here: `RetrievalPurpose` only ever has these two
/// variants, so this match is exhaustive with no wildcard/panic branch needed.
pub fn purpose_label(purpose: RetrievalPurpose) -> &'static str {
    match purpose {
        RetrievalPurpose::Embedding => "embedding",
        RetrievalPurpose::Rerank => "rerank",
    }
}

/// Process-local counter family: `label tuple -> running total`. See module doc's ponytail
/// note for the real-Prometheus upgrade path.
struct CounterFamily {
    counts: OnceLock<Mutex<HashMap<Vec<&'static str>, u64>>>,
}

impl CounterFamily {
    const fn new() -> Self {
        Self {
            counts: OnceLock::new(),
        }
    }

    fn map(&self) -> &Mutex<HashMap<Vec<&'static str>, u64>> {
        self.counts.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn inc(&self, key: Vec<&'static str>, amount: u64) {
        let mut map = self.map().lock().expect("counter mutex poisoned");
        *map.entry(key).or_insert(0) += amount;
    }

    fn get(&self, key: &[&'static str]) -> u64 {
        self.map()
            .lock()
            .expect("counter mutex poisoned")
            .get(key)
            .copied()
            .unwrap_or(0)
    }
}

/// Process-local histogram family: `label tuple -> (sample count, sum of observed seconds)`.
type HistogramData = HashMap<Vec<&'static str>, (u64, f64)>;

struct HistogramFamily {
    data: OnceLock<Mutex<HistogramData>>,
}

impl HistogramFamily {
    const fn new() -> Self {
        Self {
            data: OnceLock::new(),
        }
    }

    fn map(&self) -> &Mutex<HistogramData> {
        self.data.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn observe(&self, key: Vec<&'static str>, value_seconds: f64) {
        let mut map = self.map().lock().expect("histogram mutex poisoned");
        let entry = map.entry(key).or_insert((0, 0.0));
        entry.0 += 1;
        entry.1 += value_seconds;
    }

    fn sample_count(&self, key: &[&'static str]) -> u64 {
        self.map()
            .lock()
            .expect("histogram mutex poisoned")
            .get(key)
            .map(|(count, _)| *count)
            .unwrap_or(0)
    }

    fn sum_seconds(&self, key: &[&'static str]) -> f64 {
        self.map()
            .lock()
            .expect("histogram mutex poisoned")
            .get(key)
            .map(|(_, sum)| *sum)
            .unwrap_or(0.0)
    }
}

/// §41.2 row: `retrieval_provider_requests_total{provider,purpose,region,result}` — counter.
static RETRIEVAL_PROVIDER_REQUESTS_TOTAL: CounterFamily = CounterFamily::new();
/// §41.2 row: `retrieval_provider_latency_seconds{provider,purpose,region}` — histogram.
static RETRIEVAL_PROVIDER_LATENCY_SECONDS: HistogramFamily = HistogramFamily::new();
/// §41.2 row: `retrieval_provider_tokens_total{provider,purpose}` — counter.
static RETRIEVAL_PROVIDER_TOKENS_TOTAL: CounterFamily = CounterFamily::new();
/// §41.2 row: `retrieval_provider_cost_total{provider,purpose,currency}` — counter, value in
/// the currency's minor unit (§41.2 R1: "货币最小单位，币种进 currency label").
static RETRIEVAL_PROVIDER_COST_TOTAL: CounterFamily = CounterFamily::new();

/// §19 Provider Plane Observability's sole recording entry point for all four
/// `retrieval_provider_*` families — §41.2's "取数点" column names exactly one call site per
/// family ("§19 Provider Plane 每次外呼收尾 · 1"), which is this function: nowhere else in
/// the crate calls `.inc(`/`.observe(` on any of the four statics above (`xtask
/// metrics-registry`'s D5 cross-checks that declared count against the real one, §80.2).
///
/// `result` must be one of [`humaux_domain`]-independent §41.2's frozen six values — pass
/// [`crate::health::ProviderCallOutcome::metric_result_label`] for a completed call, or
/// [`crate::health::CIRCUIT_OPEN_RESULT_LABEL`] for a call the breaker refused before it was
/// attempted (§19 Circuit Breaker).
#[allow(clippy::too_many_arguments)]
pub fn record_provider_call(
    provider: Provider,
    purpose: RetrievalPurpose,
    region: Region,
    result: &'static str,
    input_tokens: u64,
    cost_minor_units: u64,
    currency: Currency,
    latency_seconds: f64,
) {
    let provider_s = provider.as_str();
    let purpose_s = purpose_label(purpose);
    let region_s = region.as_str();

    // labels: provider,purpose,region,result
    RETRIEVAL_PROVIDER_REQUESTS_TOTAL.inc(vec![provider_s, purpose_s, region_s, result], 1);
    // labels: provider,purpose,region
    let latency_key = vec![provider_s, purpose_s, region_s];
    RETRIEVAL_PROVIDER_LATENCY_SECONDS.observe(latency_key, latency_seconds);
    // labels: provider,purpose
    RETRIEVAL_PROVIDER_TOKENS_TOTAL.inc(vec![provider_s, purpose_s], input_tokens);
    // labels: provider,purpose,currency
    RETRIEVAL_PROVIDER_COST_TOTAL.inc(
        vec![provider_s, purpose_s, currency.as_str()],
        cost_minor_units,
    );
}

/// Current `retrieval_provider_requests_total{provider,purpose,region,result}` value —
/// test/witness accessor (§80.2 W-side needs a real observable delta, not just a call that
/// returns `()`).
pub fn retrieval_provider_requests_total_count(
    provider: Provider,
    purpose: RetrievalPurpose,
    region: Region,
    result: &'static str,
) -> u64 {
    RETRIEVAL_PROVIDER_REQUESTS_TOTAL.get(&[
        provider.as_str(),
        purpose_label(purpose),
        region.as_str(),
        result,
    ])
}

/// Current `retrieval_provider_latency_seconds{provider,purpose,region}` sample count.
pub fn retrieval_provider_latency_seconds_sample_count(
    provider: Provider,
    purpose: RetrievalPurpose,
    region: Region,
) -> u64 {
    RETRIEVAL_PROVIDER_LATENCY_SECONDS.sample_count(&[
        provider.as_str(),
        purpose_label(purpose),
        region.as_str(),
    ])
}

/// Current `retrieval_provider_latency_seconds{provider,purpose,region}` sum of observed
/// seconds.
pub fn retrieval_provider_latency_seconds_sum(
    provider: Provider,
    purpose: RetrievalPurpose,
    region: Region,
) -> f64 {
    RETRIEVAL_PROVIDER_LATENCY_SECONDS.sum_seconds(&[
        provider.as_str(),
        purpose_label(purpose),
        region.as_str(),
    ])
}

/// Current `retrieval_provider_tokens_total{provider,purpose}` value.
pub fn retrieval_provider_tokens_total_count(provider: Provider, purpose: RetrievalPurpose) -> u64 {
    RETRIEVAL_PROVIDER_TOKENS_TOTAL.get(&[provider.as_str(), purpose_label(purpose)])
}

/// Current `retrieval_provider_cost_total{provider,purpose,currency}` value (minor units).
pub fn retrieval_provider_cost_total_count(
    provider: Provider,
    purpose: RetrievalPurpose,
    currency: Currency,
) -> u64 {
    RETRIEVAL_PROVIDER_COST_TOTAL.get(&[
        provider.as_str(),
        purpose_label(purpose),
        currency.as_str(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::{CIRCUIT_OPEN_RESULT_LABEL, ProviderCallOutcome};

    /// One `record_provider_call` moves all four families by exactly the recorded amount —
    /// §41.2's per-family "取数点数处 · 1" holds *and* actually observes something.
    #[test]
    fn one_call_increments_all_four_families_by_the_recorded_amount() {
        let provider = Provider::DashScope;
        let purpose = RetrievalPurpose::Embedding;
        let region = Region::CnHangzhou;
        let result = ProviderCallOutcome::Success.metric_result_label();

        let before_requests =
            retrieval_provider_requests_total_count(provider, purpose, region, result);
        let before_tokens = retrieval_provider_tokens_total_count(provider, purpose);
        let before_cost = retrieval_provider_cost_total_count(provider, purpose, Currency::Usd);
        let before_samples =
            retrieval_provider_latency_seconds_sample_count(provider, purpose, region);
        let before_sum = retrieval_provider_latency_seconds_sum(provider, purpose, region);

        record_provider_call(
            provider,
            purpose,
            region,
            result,
            128,
            42,
            Currency::Usd,
            0.25,
        );

        assert_eq!(
            retrieval_provider_requests_total_count(provider, purpose, region, result),
            before_requests + 1
        );
        assert_eq!(
            retrieval_provider_tokens_total_count(provider, purpose),
            before_tokens + 128
        );
        assert_eq!(
            retrieval_provider_cost_total_count(provider, purpose, Currency::Usd),
            before_cost + 42
        );
        assert_eq!(
            retrieval_provider_latency_seconds_sample_count(provider, purpose, region),
            before_samples + 1
        );
        assert!(
            (retrieval_provider_latency_seconds_sum(provider, purpose, region)
                - (before_sum + 0.25))
                .abs()
                < 1e-9
        );
    }

    /// §19 Observability's frozen `result` label set, exercised end to end through
    /// `record_provider_call` — every value §41.2 declares must actually be reachable and
    /// independently countable, including `circuit_open` (never produced by
    /// `ProviderCallOutcome` itself, see that type's doc).
    #[test]
    fn every_frozen_result_label_value_is_independently_observable() {
        let provider = Provider::Custom;
        let purpose = RetrievalPurpose::Rerank;
        let region = Region::ApSoutheast1;
        let all_results: [&'static str; 6] = [
            ProviderCallOutcome::Success.metric_result_label(), // "ok"
            ProviderCallOutcome::Http401.metric_result_label(), // "http_401"
            ProviderCallOutcome::Http429.metric_result_label(), // "http_429"
            ProviderCallOutcome::Http5xx.metric_result_label(), // "http_5xx"
            ProviderCallOutcome::Timeout.metric_result_label(), // "timeout"
            CIRCUIT_OPEN_RESULT_LABEL,                          // "circuit_open"
        ];
        assert_eq!(
            all_results,
            [
                "ok",
                "http_401",
                "http_429",
                "http_5xx",
                "timeout",
                "circuit_open"
            ]
        );
        for result in all_results {
            let before = retrieval_provider_requests_total_count(provider, purpose, region, result);
            record_provider_call(provider, purpose, region, result, 1, 1, Currency::Cny, 0.01);
            assert_eq!(
                retrieval_provider_requests_total_count(provider, purpose, region, result),
                before + 1,
                "result={result} must be independently countable"
            );
        }
    }

    #[test]
    fn purpose_label_matches_frozen_values() {
        assert_eq!(purpose_label(RetrievalPurpose::Embedding), "embedding");
        assert_eq!(purpose_label(RetrievalPurpose::Rerank), "rerank");
    }
}
