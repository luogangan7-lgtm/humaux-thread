//! `retrieval-provider::health` — §19 **Provider Health / Circuit Breaker**.
//! Depends-on: crates=[]; services=[]; env=[]; modules=[retrieval-provider::admission, retrieval-provider::contract]
//! Called-by: [retrieval-provider::metrics, tests]
//! Invariants: []
//! Spec: §19
//!
//! Every `(provider, model, region)` triple carries its own independent five-state health
//! value ([`HealthState`]). The circuit breaker built on top of it
//! ([`ProviderHealthRegistry::admits_new_call`]) is, by construction, incapable of touching
//! anything Projection-related: this module has no dependency on `humaux-projection` and no
//! method anywhere in it accepts or returns a Projection/collection handle — "Circuit Breaker
//! 只影响 new admission，不修改历史 Projection" (§19) is a *topology* fact here, not a
//! runtime check a caller could bypass by calling the wrong method.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::contract::{ModelId, ProviderId, RegionId};

/// §19 Provider Health / Circuit Breaker's five states, verbatim.
///
/// `retrieval-provider::admission`'s own `ProviderHealthState` (same five names, defined
/// before this module existed — see that type's doc: "that monitor itself is a separate
/// module's deliverable ... this module only consumes its output") is the pre-existing
/// placeholder this type is the real implementation for; the `From<HealthState> for
/// crate::admission::ProviderHealthState` impl right below bridges the two rather than
/// editing `admission.rs`'s own module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HealthState {
    Healthy,
    Degraded,
    RateLimited,
    Unavailable,
    PolicyBlocked,
}

impl From<HealthState> for crate::admission::ProviderHealthState {
    fn from(state: HealthState) -> Self {
        match state {
            HealthState::Healthy => Self::Healthy,
            HealthState::Degraded => Self::Degraded,
            HealthState::RateLimited => Self::RateLimited,
            HealthState::Unavailable => Self::Unavailable,
            HealthState::PolicyBlocked => Self::PolicyBlocked,
        }
    }
}

/// The independent-health key: §19 "每个 provider/model/region 独立健康状态" — three axes,
/// not fewer (a single global or per-provider-only state would let one model's outage close
/// the breaker for every other model on the same provider/region). Reuses `crate::contract`'s
/// T7.1 identifier newtypes (§41.2 R3 "同一被测对象只允许一个名字", applied to types).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HealthKey {
    pub provider_id: ProviderId,
    pub model_id: ModelId,
    pub region_id: RegionId,
}

/// §19.2 "429：有界退避" — the default bounded-backoff window a `RateLimited`/`Unavailable`
/// state holds the breaker open for before [`ProviderHealthRegistry::admits_new_call`] allows
/// a half-open re-probe. [`ProviderHealthRegistry::with_backoff`] overrides it (tests use a
/// millisecond-scale window instead of sleeping 30s for real).
///
/// ponytail: one fixed window for every transient state, not a per-outcome/exponential curve —
/// §19.2 names only "有界" (bounded), not a specific shape. Upgrade to exponential-with-jitter
/// if a real 429 storm shows a flat 30s window is too coarse.
pub const DEFAULT_BACKOFF: Duration = Duration::from_secs(30);

/// Per-`(provider, model, region)` health machine. Unknown keys default to
/// [`HealthState::Healthy`] (an admission decision must always resolve to something; treating
/// an unseen triple as unhealthy-by-default would fail closed on process start for every real
/// caller before a single observation ever landed).
#[derive(Debug)]
pub struct ProviderHealthRegistry {
    states: HashMap<HealthKey, (HealthState, Instant)>,
    backoff: Duration,
}

impl Default for ProviderHealthRegistry {
    fn default() -> Self {
        Self::with_backoff(DEFAULT_BACKOFF)
    }
}

impl ProviderHealthRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Same registry, with an explicit bounded-backoff window instead of [`DEFAULT_BACKOFF`].
    pub fn with_backoff(backoff: Duration) -> Self {
        Self {
            states: HashMap::new(),
            backoff,
        }
    }

    /// Records the current health of one `(provider, model, region)` triple, timestamped now.
    /// The only writer of `states` — there is no other method on this type that can mutate it,
    /// so an admission decision ([`admits_new_call`](Self::admits_new_call)) can only ever
    /// have been set here, never inferred from anything Projection-side.
    pub fn set_state(&mut self, key: HealthKey, state: HealthState) {
        self.states.insert(key, (state, Instant::now()));
    }

    /// Current state for `key`, defaulting to [`HealthState::Healthy`] for an unseen key (see
    /// struct doc). This is the *last recorded* state — it does not itself apply the
    /// half-open backoff window [`admits_new_call`](Self::admits_new_call) does; a caller that
    /// wants "would a new call be admitted" must call that method, not infer it from this one.
    pub fn state(&self, key: &HealthKey) -> HealthState {
        self.states
            .get(key)
            .map(|(state, _)| *state)
            .unwrap_or(HealthState::Healthy)
    }

    /// §19 Circuit Breaker: whether a **new** call to `key` may be admitted right now.
    /// `Degraded` always admits (a degraded provider is slower/less reliable, not something
    /// admission should refuse outright — that judgment call belongs to the Admission
    /// Controller's own budget math, a later task, not to health alone).
    ///
    /// `RateLimited`/`Unavailable` open the breaker but re-admit (half-open probe) once
    /// `backoff` has elapsed since the state was last recorded (§19.2 "429：有界退避") — an
    /// open breaker with no re-probe path can never recover without an external timer this
    /// registry does not otherwise have. `PolicyBlocked` does **not** auto-recover on a timer:
    /// §19.2 treats it as a credential/permission state ("进入 invalid/waiting_key") that
    /// clears only on an explicit credential-domain event (e.g. a key rotation), never a
    /// timeout — half-opening it would let an admission attempt through with credentials
    /// already known to be rejected.
    ///
    /// This method's signature is the whole enforcement: it takes a [`HealthKey`] and returns
    /// a `bool`, nothing that could reach into or mutate a Projection — "不修改历史
    /// Projection" (§19) holds because there is no argument or return type here through which
    /// it could.
    pub fn admits_new_call(&self, key: &HealthKey) -> bool {
        let Some(&(state, since)) = self.states.get(key) else {
            return true; // unseen key defaults Healthy — see struct doc.
        };
        match state {
            HealthState::Healthy | HealthState::Degraded => true,
            HealthState::PolicyBlocked => false,
            HealthState::RateLimited | HealthState::Unavailable => since.elapsed() >= self.backoff,
        }
    }
}

// ============================================================================
// §19 Provider Plane 测试 · Failure Tests — outcome -> health-state / metric-result mapping
// ============================================================================

/// The Failure Tests scenario set §19 names verbatim (`401 / 403 / 429 / 5xx / timeout /
/// connection reset / malformed response / provider returns fewer docs / duplicate document
/// result`), plus `Success`. This is the *classification* input
/// [`health_state_for_outcome`]/[`ProviderCallOutcome::metric_result_label`] consume — mapping
/// a raw transport/provider event onto this closed set is a caller concern (e.g. the eventual
/// `retrieval-provider::adapters` HTTP call site), out of this module's scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderCallOutcome {
    Success,
    Http401,
    Http403,
    Http429,
    Http5xx,
    Timeout,
    ConnectionReset,
    MalformedResponse,
    FewerDocumentsThanRequested,
    DuplicateDocumentResult,
}

impl ProviderCallOutcome {
    /// §19.2: "429：有界退避；401：根据 provider/key domain 进入 invalid/waiting_key；5xx：
    /// transient retry." Folded into a health-state transition per outcome:
    /// `RateLimited`/`PolicyBlocked` open the breaker (§19 Circuit Breaker) immediately on the
    /// event that caused them; `Unavailable` covers every "the provider did not answer
    /// usefully at the transport level" case; malformed/short/duplicate provider *content*
    /// (still a 200-level response) is `Degraded`, not breaker-opening — the transport
    /// succeeded, the payload just was not trustworthy.
    pub fn health_state(self) -> HealthState {
        match self {
            Self::Success => HealthState::Healthy,
            Self::Http429 => HealthState::RateLimited,
            Self::Http401 | Self::Http403 => HealthState::PolicyBlocked,
            Self::Http5xx | Self::Timeout | Self::ConnectionReset => HealthState::Unavailable,
            Self::MalformedResponse
            | Self::FewerDocumentsThanRequested
            | Self::DuplicateDocumentResult => HealthState::Degraded,
        }
    }

    /// §41.2's frozen `retrieval_provider_requests_total.result` label value set is exactly
    /// `ok | http_401 | http_429 | http_5xx | timeout | circuit_open` — six values, not the
    /// nine [`ProviderCallOutcome`] carries. Bucketing here (not a 1:1 mapping) is deliberate:
    /// `Http403` joins `http_401` (both are provider auth/authz refusals — R5 low-cardinality
    /// forbids a seventh label value for a distinction §41.2 never asked for);
    /// `ConnectionReset` joins `timeout` (both are "no usable response arrived", same as
    /// `health_state` groups them under `Unavailable`); a malformed/short/duplicate response
    /// is `ok` at the transport-result label's altitude — the HTTP call itself succeeded, the
    /// *content* problem is a `health_state` / `DegradeCode` concern, not this label's job.
    /// `circuit_open` is never produced by this method — it is the caller's own label for a
    /// call [`ProviderHealthRegistry::admits_new_call`] refused before any request was even
    /// attempted, see [`CIRCUIT_OPEN_RESULT_LABEL`].
    ///
    /// Explicit consequence of this decision, recorded rather than left implicit: §19's
    /// Failure Tests name `MalformedResponse`/`FewerDocumentsThanRequested`/
    /// `DuplicateDocumentResult` as failure scenarios, but under this frozen label set the
    /// *only* signal for them is [`Self::health_state`] returning `Degraded` — the §42
    /// `provider 401/429/5xx spike` alert cannot see them at all. Until something in this
    /// workspace consumes `Degraded` (writes it to `DegradeCode`/`degrade_total{code}`, §53.1,
    /// or otherwise), these three scenarios are unobservable end to end even though this
    /// method itself is doing exactly what it documents.
    pub fn metric_result_label(self) -> &'static str {
        match self {
            Self::Success
            | Self::MalformedResponse
            | Self::FewerDocumentsThanRequested
            | Self::DuplicateDocumentResult => "ok",
            Self::Http401 | Self::Http403 => "http_401",
            Self::Http429 => "http_429",
            Self::Http5xx => "http_5xx",
            Self::Timeout | Self::ConnectionReset => "timeout",
        }
    }
}

/// §41.2 frozen label value for a call the circuit breaker refused before attempting it — the
/// one member of the frozen `result` set [`ProviderCallOutcome::metric_result_label`] cannot
/// produce, because a breaker-open call never reaches a [`ProviderCallOutcome`] at all.
pub const CIRCUIT_OPEN_RESULT_LABEL: &str = "circuit_open";

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> HealthKey {
        HealthKey {
            provider_id: ProviderId("dashscope".into()),
            model_id: ModelId("text-embedding-v4".into()),
            region_id: RegionId("cn-hangzhou".into()),
        }
    }

    #[test]
    fn unknown_key_defaults_healthy_and_admits() {
        let registry = ProviderHealthRegistry::new();
        assert_eq!(registry.state(&key()), HealthState::Healthy);
        assert!(registry.admits_new_call(&key()));
    }

    #[test]
    fn degraded_still_admits_new_calls() {
        let mut registry = ProviderHealthRegistry::new();
        registry.set_state(key(), HealthState::Degraded);
        assert!(registry.admits_new_call(&key()));
    }

    /// §19 Circuit Breaker Failure Test: an open breaker blocks only *new admission* — it has
    /// no method that could touch, and this test proves no side channel silently does, a
    /// separately-held "historical Projection" value (stood in here by an unrelated
    /// [`crate::failover::ProjectionContract`]) across the state transition.
    #[test]
    fn unavailable_opens_breaker_without_touching_unrelated_projection_state() {
        use crate::failover::{Normalization, ProjectionContract};
        let historical_projection = ProjectionContract {
            provider_id: ProviderId("dashscope".into()),
            model_id: ModelId("text-embedding-v4".into()),
            model_revision: "2026-08".to_string(),
            dimension: 1024,
            normalization: Normalization::L2,
            projection_version: "v1".to_string(),
        };
        let before = historical_projection.clone();

        let mut registry = ProviderHealthRegistry::new();
        assert!(registry.admits_new_call(&key()));
        registry.set_state(key(), HealthState::Unavailable);
        assert!(!registry.admits_new_call(&key()));

        // The historical Projection value is untouched — no method above ever took it as an
        // argument, so this equality is really just confirming the type system's guarantee.
        assert_eq!(historical_projection, before);
    }

    /// §19.2 "429：有界退避": a `RateLimited` breaker re-admits (half-open probe) once the
    /// configured backoff window elapses — it must not latch closed forever.
    #[test]
    fn rate_limited_half_opens_after_backoff_elapses() {
        let mut registry = ProviderHealthRegistry::with_backoff(Duration::from_millis(20));
        registry.set_state(key(), HealthState::RateLimited);
        assert!(
            !registry.admits_new_call(&key()),
            "must not admit immediately"
        );
        std::thread::sleep(Duration::from_millis(30));
        assert!(
            registry.admits_new_call(&key()),
            "must half-open once backoff has elapsed"
        );
    }

    /// Same half-open shape for `Unavailable` (§19.2 "5xx：transient retry").
    #[test]
    fn unavailable_half_opens_after_backoff_elapses() {
        let mut registry = ProviderHealthRegistry::with_backoff(Duration::from_millis(20));
        registry.set_state(key(), HealthState::Unavailable);
        assert!(!registry.admits_new_call(&key()));
        std::thread::sleep(Duration::from_millis(30));
        assert!(registry.admits_new_call(&key()));
    }

    /// 注错 shape this task's review named: `PolicyBlocked` must **not** auto-recover on a
    /// timer — a single `Success` clearing it instantly is the opposite bug, but a timer
    /// silently letting a known-bad-credential probe through is the one this test guards.
    #[test]
    fn policy_blocked_never_half_opens_on_a_timer() {
        let mut registry = ProviderHealthRegistry::with_backoff(Duration::from_millis(1));
        registry.set_state(key(), HealthState::PolicyBlocked);
        std::thread::sleep(Duration::from_millis(20));
        assert!(
            !registry.admits_new_call(&key()),
            "PolicyBlocked must only clear via an explicit credential-domain event, never a \
             timeout"
        );
    }

    #[test]
    fn rate_limited_and_policy_blocked_also_open_the_breaker() {
        let mut registry = ProviderHealthRegistry::new();
        registry.set_state(key(), HealthState::RateLimited);
        assert!(!registry.admits_new_call(&key()));
        registry.set_state(key(), HealthState::PolicyBlocked);
        assert!(!registry.admits_new_call(&key()));
    }

    #[test]
    fn independent_keys_do_not_share_state() {
        let mut registry = ProviderHealthRegistry::new();
        let other = HealthKey {
            provider_id: ProviderId("dashscope".into()),
            model_id: ModelId("gte-rerank-v2".into()), // different model, same provider/region
            region_id: RegionId("cn-hangzhou".into()),
        };
        registry.set_state(key(), HealthState::Unavailable);
        assert!(!registry.admits_new_call(&key()));
        assert!(
            registry.admits_new_call(&other),
            "a different model must not inherit the outage"
        );
    }

    /// §19 Failure Tests scenario set, mapped onto both consumers ([`HealthState`] and the
    /// §41.2 metric `result` label) in one table so a new [`ProviderCallOutcome`] variant
    /// cannot silently land on only one side.
    #[test]
    fn every_failure_scenario_maps_to_its_documented_state_and_label() {
        let cases = [
            (ProviderCallOutcome::Success, HealthState::Healthy, "ok"),
            (
                ProviderCallOutcome::Http401,
                HealthState::PolicyBlocked,
                "http_401",
            ),
            (
                ProviderCallOutcome::Http403,
                HealthState::PolicyBlocked,
                "http_401",
            ),
            (
                ProviderCallOutcome::Http429,
                HealthState::RateLimited,
                "http_429",
            ),
            (
                ProviderCallOutcome::Http5xx,
                HealthState::Unavailable,
                "http_5xx",
            ),
            (
                ProviderCallOutcome::Timeout,
                HealthState::Unavailable,
                "timeout",
            ),
            (
                ProviderCallOutcome::ConnectionReset,
                HealthState::Unavailable,
                "timeout",
            ),
            (
                ProviderCallOutcome::MalformedResponse,
                HealthState::Degraded,
                "ok",
            ),
            (
                ProviderCallOutcome::FewerDocumentsThanRequested,
                HealthState::Degraded,
                "ok",
            ),
            (
                ProviderCallOutcome::DuplicateDocumentResult,
                HealthState::Degraded,
                "ok",
            ),
        ];
        for (outcome, expected_state, expected_label) in cases {
            assert_eq!(
                outcome.health_state(),
                expected_state,
                "{outcome:?} health_state"
            );
            assert_eq!(
                outcome.metric_result_label(),
                expected_label,
                "{outcome:?} metric_result_label"
            );
        }
    }

    /// §41.2 R3 "同一被测对象只允许一个名字" (applied to types, this module's own doc's
    /// argument for the `From` bridge): `HealthState` and `admission::ProviderHealthState`
    /// must stay the exact same five-value set. The `From` impl already gives exhaustiveness
    /// in one direction (adding a sixth `HealthState` variant fails to compile); this test
    /// gives it in the other — an exhaustive `match` with no wildcard arm over
    /// `admission::ProviderHealthState`, so a variant added there without a matching
    /// `HealthState`/`From` update fails to compile too, instead of silently drifting.
    #[test]
    fn health_state_and_admission_provider_health_state_round_trip_every_variant() {
        for state in [
            HealthState::Healthy,
            HealthState::Degraded,
            HealthState::RateLimited,
            HealthState::Unavailable,
            HealthState::PolicyBlocked,
        ] {
            let bridged: crate::admission::ProviderHealthState = state.into();
            let back_to_health_state = match bridged {
                crate::admission::ProviderHealthState::Healthy => HealthState::Healthy,
                crate::admission::ProviderHealthState::Degraded => HealthState::Degraded,
                crate::admission::ProviderHealthState::RateLimited => HealthState::RateLimited,
                crate::admission::ProviderHealthState::Unavailable => HealthState::Unavailable,
                crate::admission::ProviderHealthState::PolicyBlocked => HealthState::PolicyBlocked,
            };
            assert_eq!(back_to_health_state, state);
        }
    }
}
