//! `private-worker::tests::support::live_provider` — one live OpenAI-compatible reasoning provider a test can
//!   bind a Profile to: endpoint, hosts, model, declared capabilities, request extras, the NAME of its key
//!   variable, region, recipient and DNS pins. The second provider is read from the environment only.
//! Depends-on: crates=[humaux-adapters, serde_json, uuid]; services=[]; env=[HUMAUX_LIVE_P2_BASE_URL,
//!   HUMAUX_LIVE_P2_CAPABILITIES, HUMAUX_LIVE_P2_DNS_PINS, HUMAUX_LIVE_P2_EGRESS_PROCESSOR_ID, HUMAUX_LIVE_P2_HOSTS,
//!   HUMAUX_LIVE_P2_KEY_ENV, HUMAUX_LIVE_P2_MODEL, HUMAUX_LIVE_P2_PROVIDER_ID, HUMAUX_LIVE_P2_REGION,
//!   HUMAUX_LIVE_P2_REQUEST_EXTRAS, HUMAUX_REQUIRE_SECOND_PROVIDER]; modules=[adapters::byok]
//! Called-by: [private-worker::tests::derived_dispatch_e2e, private-worker::tests::distill_hop_e2e,
//!   private-worker::tests::support::live_minimax]
//! Invariants: [no vendor name, endpoint or model literal for the second provider (ADR-0060 research amendment 5);
//!   a key is read only from the variable a profile names and never printed; with
//!   HUMAUX_REQUIRE_SECOND_PROVIDER=1 a missing variable panics, it is never a skip]
//! Spec: ADR-0060 D-B; ADR-0060 D-L; ADR-0060 research amendment 5
//!
//! `live_minimax.rs` is the first provider's instance of [`LiveProfile`]; the second is
//! [`LiveProfile::second_provider`], whose nine required `HUMAUX_LIVE_P2_*` names plus the optional
//! `HUMAUX_LIVE_P2_DNS_PINS` are the canonical set the main line exports (values are not secrets; the
//! key stays in the variable `_KEY_ENV` names). `HUMAUX_LIVE_P2_HOSTS` is `|`-separated, the separator
//! `HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS` uses inside one recipient, so it is pasted there as is.

use humaux_adapters::byok::ReasoningCapability;
use uuid::Uuid;

/// One live provider a Profile can be bound to.
#[derive(Debug, Clone)]
pub struct LiveProfile {
    pub provider_id: String,
    pub chat_url: String,
    /// The hosts its recipient may dial (`HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS`, ADR-0060 D-L).
    pub hosts: Vec<String>,
    pub model_id: String,
    pub capabilities: Vec<ReasoningCapability>,
    /// The Profile's `request_extras` (ADR-0060 research amendment 1).
    pub request_extras: serde_json::Map<String, serde_json::Value>,
    /// The NAME of the variable holding its key.
    pub key_env: String,
    pub region: String,
    /// Its egress recipient uuid, when the deployment fixes one (`None`: the test picks one).
    pub egress_processor_id: Option<Uuid>,
    /// `host=ip[|ip],...` pins for hosts this node resolves into a forbidden range; empty = none.
    pub dns_pins: String,
}

/// `HUMAUX_LIVE_P2_*` — the nine required names (ADR-0060 research amendment 5).
const P2: [&str; 9] = [
    "HUMAUX_LIVE_P2_BASE_URL",
    "HUMAUX_LIVE_P2_HOSTS",
    "HUMAUX_LIVE_P2_MODEL",
    "HUMAUX_LIVE_P2_CAPABILITIES",
    "HUMAUX_LIVE_P2_REQUEST_EXTRAS",
    "HUMAUX_LIVE_P2_KEY_ENV",
    "HUMAUX_LIVE_P2_PROVIDER_ID",
    "HUMAUX_LIVE_P2_REGION",
    "HUMAUX_LIVE_P2_EGRESS_PROCESSOR_ID",
];
/// Optional: absent or empty = no pin (the system resolver is trusted for the hosts).
const P2_DNS_PINS: &str = "HUMAUX_LIVE_P2_DNS_PINS";

impl LiveProfile {
    /// The second provider, from `HUMAUX_LIVE_P2_*` only. `None` when one is missing and
    /// `HUMAUX_REQUIRE_SECOND_PROVIDER` is not `1`; a panic naming the variable when it is.
    pub fn second_provider() -> Option<Self> {
        Self::second_provider_from(
            |name| std::env::var(name).ok(),
            std::env::var("HUMAUX_REQUIRE_SECOND_PROVIDER").as_deref() == Ok("1"),
        )
    }

    /// [`Self::second_provider`] over `lookup` (the environment in production use).
    pub fn second_provider_from(
        lookup: impl Fn(&str) -> Option<String>,
        require: bool,
    ) -> Option<Self> {
        let mut values = Vec::new();
        for name in P2 {
            match lookup(name).filter(|v| !v.is_empty()) {
                Some(v) => values.push(v),
                None if require => panic!(
                    "missing object: {name} (HUMAUX_REQUIRE_SECOND_PROVIDER=1, ADR-0060 amendment 5)"
                ),
                None => {
                    eprintln!("SKIP: {name} unset — the second live provider is not configured");
                    return None;
                }
            }
        }
        let [
            url,
            hosts,
            model,
            caps,
            extras,
            key_env,
            provider,
            region,
            egress,
        ] = <[String; 9]>::try_from(values).expect("nine values");
        assert!(
            !hosts.contains(','),
            "HUMAUX_LIVE_P2_HOSTS is `|`-separated (one recipient's hosts), not comma-separated"
        );
        let capabilities = caps
            .split(',')
            .map(|c| {
                ReasoningCapability::parse(c.trim()).unwrap_or_else(|| {
                    panic!("HUMAUX_LIVE_P2_CAPABILITIES: {c:?} is outside §11.2")
                })
            })
            .collect();
        let request_extras = match serde_json::from_str(&extras) {
            Ok(serde_json::Value::Object(map)) => map,
            _ => panic!("HUMAUX_LIVE_P2_REQUEST_EXTRAS must be a JSON object"),
        };
        Some(Self {
            provider_id: provider,
            chat_url: url,
            hosts: hosts.split('|').map(str::to_owned).collect(),
            model_id: model,
            capabilities,
            request_extras,
            key_env,
            region,
            egress_processor_id: Some(
                egress
                    .parse()
                    .expect("HUMAUX_LIVE_P2_EGRESS_PROCESSOR_ID must be a uuid"),
            ),
            dns_pins: lookup(P2_DNS_PINS).unwrap_or_default(),
        })
    }

    /// The key from the variable this profile names (never printed). `None` when unset or empty.
    pub fn key(&self) -> Option<String> {
        std::env::var(&self.key_env).ok().filter(|v| !v.is_empty())
    }
}

/// Finding 4 (card 33b review pass 2): `HUMAUX_LIVE_P2_DNS_PINS` is optional and `HUMAUX_LIVE_P2_HOSTS`
/// is `|`-separated. Fault: make the pins mandatory again → `None` without them.
#[test]
fn second_provider_pins_are_optional_and_hosts_are_pipe_separated() {
    let lookup = |name: &str| {
        Some(
            match name {
                "HUMAUX_LIVE_P2_HOSTS" => "a.example|b.example",
                "HUMAUX_LIVE_P2_CAPABILITIES" => "TEXT,STRUCTURED_OUTPUT",
                "HUMAUX_LIVE_P2_REQUEST_EXTRAS" => "{}",
                "HUMAUX_LIVE_P2_EGRESS_PROCESSOR_ID" => "00000000-0000-4000-8000-000000000001",
                "HUMAUX_LIVE_P2_DNS_PINS" => return None,
                _ => "x",
            }
            .to_owned(),
        )
    };
    let p2 = LiveProfile::second_provider_from(lookup, false).expect("configured without pins");
    assert_eq!(p2.hosts, ["a.example", "b.example"]);
    assert!(p2.dns_pins.is_empty(), "absent pins = no pin");
}
