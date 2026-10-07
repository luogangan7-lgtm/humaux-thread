//! `telemetry::admission` — the §67.2 admission refusal reasons and the one `admission_rejected_total{class,reason}`
//!   counter (§41.2), rendered by the gateway.
//! Depends-on: crates=[]; services=[]; env=[]; modules=[telemetry::metrics]
//! Called-by: [gateway::admission, gateway::guard, tests]
//! Invariants: [`count_admission_rejected` holds the family's one increment; `reason` is the closed set of
//!   `AdmissionRefusal::ALL` and `class` the one value `gateway_inbound`; a refusal is counted, never logged]
//! Spec: Baseline §67.2; §41.2; §42; ADR-0065 D-C
//!
//! The counter lives here, not in the gateway binary, so the G80-6 witness (which may only depend on library
//! crates) drives the real emit, and `cargo xtask metrics-registry` finds its one call site (ADR-0065 D-C).

use crate::metrics::{Counters, families, write_family};

/// Why the gateway's admission layer refused a request with 503 (ADR-0065 D-C). The variant name is never a
/// wire value: the 503 body is always `RATE_LIMITED`; the reason only labels the counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRefusal {
    /// The waiter cap Q was reached (`HUMAUX_GATEWAY_ADMISSION_QUEUE_DEPTH`).
    QueueFull,
    /// A waiter did not get a permit within W (`HUMAUX_GATEWAY_ADMISSION_MAX_WAIT_MS`).
    WaitTimeout,
    /// The credential already holds K requests in the system (`HUMAUX_GATEWAY_ADMISSION_PER_KEY_LIMIT`).
    KeyLimit,
}

impl AdmissionRefusal {
    /// Every variant, in label order.
    pub const ALL: [Self; 3] = [Self::QueueFull, Self::WaitTimeout, Self::KeyLimit];

    /// The `reason` label value (§41.2 closed set, ADR-0065 D-C).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QueueFull => "queue_full",
            Self::WaitTimeout => "wait_timeout",
            Self::KeyLimit => "key_limit",
        }
    }

    const fn slot(self) -> usize {
        self as usize
    }
}

/// §67.2 traffic class: the gateway has one inbound class.
pub const GATEWAY_INBOUND: &str = "gateway_inbound";

// §41.2 row `admission_rejected_total{class,reason}` (§67 admission control 返 503 处 · 1); consumer §42
// AdmissionRejected. One slot per `AdmissionRefusal::ALL` position; `class` has one value.
static ADMISSION_REJECTED_TOTAL: Counters<{ AdmissionRefusal::ALL.len() }> = Counters::new();

/// Counts one 503 of the admission layer. Public so the G80-6 witness drives the emit without a gateway; the one
/// production caller is `gateway::admission`'s refusal response (ADR-0065 D-C: exactly 1 per 503).
pub fn count_admission_rejected(reason: AdmissionRefusal) {
    // labels: class,reason
    ADMISSION_REJECTED_TOTAL.inc(reason.slot(), 1);
}

/// This process's `admission_rejected_total{class="gateway_inbound",reason}`.
pub fn admission_rejected_total(reason: AdmissionRefusal) -> u64 {
    ADMISSION_REJECTED_TOTAL.get(reason.slot())
}

/// Renders the family seeded over its closed sets (ADR-0061 D-A: every series exists from the first scrape).
pub fn render(out: &mut String) {
    let samples: Vec<([&'static str; 2], f64)> = AdmissionRefusal::ALL
        .iter()
        .map(|r| {
            #[expect(
                clippy::cast_precision_loss,
                reason = "a process-local request count stays far below 2^52"
            )]
            let value = admission_rejected_total(*r) as f64;
            ([GATEWAY_INBOUND, r.as_str()], value)
        })
        .collect();
    let samples: Vec<(&[&'static str], f64)> =
        samples.iter().map(|(v, n)| (v.as_slice(), *n)).collect();
    write_family(out, &families::ADMISSION_REJECTED_TOTAL, &samples);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Seeded at zero over the 3 reasons with the §41.2 label keys, and an increment lands on its own series.
    #[test]
    fn render_is_seeded_and_counts_per_reason() {
        let before = admission_rejected_total(AdmissionRefusal::KeyLimit);
        count_admission_rejected(AdmissionRefusal::KeyLimit);
        assert_eq!(
            admission_rejected_total(AdmissionRefusal::KeyLimit),
            before + 1
        );
        let mut out = String::new();
        render(&mut out);
        assert!(
            out.contains("# TYPE admission_rejected_total counter\n"),
            "{out}"
        );
        for r in AdmissionRefusal::ALL {
            assert!(
                out.contains(&format!(
                    "admission_rejected_total{{class=\"gateway_inbound\",reason=\"{}\"}} ",
                    r.as_str()
                )),
                "{out}"
            );
        }
    }
}
