//! `private-worker::tests::support::live_minimax` — the live MiniMax rehearsal binding the private-worker tests dial:
//!   key lookup, DNS pins, descriptor and the real `OpenAiCompatibleProvider` over the egress transport.
//! Depends-on: crates=[async-trait, humaux-adapters, serde_json]; services=[MiniMax]; env=[HUMAUX_MINIMAX_DNS_PINS,
//!   MINIMAX_API_KEY]; modules=[adapters::byok, adapters::byok::ssrf, private-worker::tests::support::live_provider]
//! Called-by: [private-worker::tests::derived_dispatch_e2e, private-worker::tests::distill_hop_e2e]
//! Invariants: [the key never reaches a println, a panic message or a child's argv (only a child's environment); the
//!   DNS pins are the one dial-time resolver, as in production (ADR-0039 D0)]
//! Spec: ADR-0005; ADR-0039; ADR-0058
//!
//! The first live provider's instance of `support::live_provider::LiveProfile` (ADR-0060 D-B) is
//! [`minimax_profile`]. Moved here (ADR-0058 D-Q) so `distill_hop_e2e`'s D1 and `derived_dispatch_e2e`'s
//! `distill_fairness_live` dial MiniMax through one definition instead of two copies.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use humaux_adapters::byok::{
    CredentialDecryptor, CredentialRef, EgressHttpTransport, OpenAiCompatibleProvider,
    PlaintextApiKey, ReasoningCapability, ReasoningProviderDescriptor, ReasoningProviderError,
    ssrf,
};

use crate::live_provider::LiveProfile;

/// The rehearsal endpoint every live test (and the route rows they seed) names.
pub const MINIMAX_CHAT_URL: &str = "https://api.minimaxi.com/v1/chat/completions";
/// The host the rehearsal recipient may dial (ADR-0060 D-L).
#[allow(dead_code)] // derived_dispatch_e2e only; distill_hop_e2e routes through stub closures
pub const MINIMAX_HOST: &str = "api.minimaxi.com";
/// The rehearsal model; route admission compares it with the seeded `processor_models` row.
pub const MINIMAX_MODEL: &str = "MiniMax-M3";
/// The provider id the seeded route rows and the descriptor share.
pub const MINIMAX_PROVIDER: &str = "minimax";
/// What the rehearsal Profile declares (the seeded catalog row, the Profile and every in-process
/// descriptor built from its route — ADR-0060 D-C compares the sets). ADR-0058 R10: the tool channel is kept because the live A/B probe
/// (`derived_dispatch_e2e::distill_channel_ab_live`) measured it no worse.
pub const REHEARSAL_CAPABILITIES: [ReasoningCapability; 4] = [
    ReasoningCapability::Text,
    ReasoningCapability::StructuredOutput,
    ReasoningCapability::ToolCalls,
    ReasoningCapability::ReasoningSplit,
];

/// ADR-0060 E5: the catalog `model_revision` label of a capability set — `caps-` and the sorted
/// wire names joined by `.` — so a wider set never collides with a frozen catalog row (the
/// catalog is append-only). Never sent on the wire.
pub fn caps_revision_label(capabilities: &[ReasoningCapability]) -> String {
    let mut names: Vec<&str> = capabilities.iter().map(|c| c.as_str()).collect();
    names.sort_unstable();
    format!("caps-{}", names.join("."))
}

/// The first live provider as a [`LiveProfile`] (ADR-0060 D-B): the rehearsal binding with its
/// `{"reasoning_split": true}` request extra (research amendment 1: a vendor field is profile data).
#[allow(dead_code)] // derived_dispatch_e2e only (the two-provider live gate)
pub fn minimax_profile(region: &str) -> LiveProfile {
    LiveProfile {
        provider_id: MINIMAX_PROVIDER.to_owned(),
        chat_url: MINIMAX_CHAT_URL.to_owned(),
        hosts: vec![MINIMAX_HOST.to_owned()],
        model_id: MINIMAX_MODEL.to_owned(),
        capabilities: REHEARSAL_CAPABILITIES.to_vec(),
        request_extras: serde_json::json!({"reasoning_split": true})
            .as_object()
            .cloned()
            .unwrap_or_default(),
        key_env: "MINIMAX_API_KEY".to_owned(),
        region: region.to_owned(),
        egress_processor_id: None,
        dns_pins: dns_pins(),
    }
}

/// Same env + `/Volumes/data/viral-skill-eval/.env` fallback `minimax_live_smoke.rs` uses.
/// Zero println/panic-message exposure of the value.
pub fn load_minimax_key() -> Option<String> {
    if let Some(v) = std::env::var("MINIMAX_API_KEY")
        .ok()
        .filter(|v| !v.is_empty())
    {
        return Some(v);
    }
    let raw = std::fs::read_to_string("/Volumes/data/viral-skill-eval/.env").ok()?;
    for line in raw.lines() {
        let line = line.trim().strip_prefix("export ").unwrap_or(line.trim());
        if let Some(v) = line.strip_prefix("MINIMAX_API_KEY=") {
            let v = v.trim().trim_matches('"').trim_matches('\'');
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// Hands the test's key to the provider exactly where production's env credential does.
pub struct EnvKeyDecryptor {
    pub key_material: String,
}

#[async_trait]
impl CredentialDecryptor for EnvKeyDecryptor {
    async fn resolve(
        &self,
        _credential_ref: CredentialRef,
    ) -> Result<PlaintextApiKey, ReasoningProviderError> {
        Ok(PlaintextApiKey::new(self.key_material.clone()))
    }
}

/// `HUMAUX_MINIMAX_DNS_PINS` (`host=ip[|ip],...`, the `HUMAUX_PRIVATE_WORKER_DNS_PINS` format);
/// empty = system DNS for every host. A subprocess worker gets the same string as its
/// `HUMAUX_PRIVATE_WORKER_DNS_PINS`.
///
/// This development machine's DNS is taken over by a local proxy's fake-IP range
/// (`api.minimaxi.com` → `198.18.0.x`, RFC 2544), which the §11.4 choke point correctly refuses as
/// `ResolvedIpForbidden`; the pins give the real address. Reading env in `/tests/` is the §78
/// boundary lint's standing exemption.
pub fn dns_pins() -> String {
    std::env::var("HUMAUX_MINIMAX_DNS_PINS").unwrap_or_default()
}

/// ADR-0039 D0: the resolver the check AND the dial use — the production mechanism.
pub fn live_dns_resolver() -> Arc<dyn ssrf::DnsResolver> {
    Arc::new(
        ssrf::PinnedDnsResolver::parse(&dns_pins())
            .expect("HUMAUX_MINIMAX_DNS_PINS must parse as host=ip[|ip],..."),
    )
}

/// The rehearsal descriptor with `model_id` (a test that needs a provider-side refusal names a
/// model MiniMax does not serve).
pub fn descriptor_for(model_id: &str) -> ReasoningProviderDescriptor {
    // The rehearsal profile's declared capabilities (ADR-0058 D-M).
    descriptor_declaring(model_id, &REHEARSAL_CAPABILITIES)
}

/// The rehearsal endpoint and `model_id` declaring exactly `capabilities` — the A/B probe's two
/// channels (ADR-0058 R10) differ only here.
pub fn descriptor_declaring(
    model_id: &str,
    capabilities: &[ReasoningCapability],
) -> ReasoningProviderDescriptor {
    ReasoningProviderDescriptor {
        provider_id: MINIMAX_PROVIDER.to_string(),
        model_id: model_id.to_string(),
        model_revision: Some(caps_revision_label(capabilities)),
        capabilities: capabilities.to_vec(),
        custom_endpoint: Some(MINIMAX_CHAT_URL.to_string()),
        request_extras: Default::default(),
    }
}

/// [`descriptor_for`] the rehearsal model.
pub fn descriptor() -> ReasoningProviderDescriptor {
    descriptor_for(MINIMAX_MODEL)
}

/// The real BYOK provider over the egress transport. `http_timeout` must equal the dispatch
/// config's `http_timeout_seconds` (ADR-0058 D-K sizes `ops.begin_call`'s window from it).
pub fn live_provider(
    key: String,
    model_id: &str,
    http_timeout: Duration,
) -> OpenAiCompatibleProvider<EgressHttpTransport, EnvKeyDecryptor> {
    live_provider_declaring(key, model_id, http_timeout, &REHEARSAL_CAPABILITIES)
}

/// [`live_provider`] whose descriptor declares exactly `capabilities` ([`descriptor_declaring`]).
pub fn live_provider_declaring(
    key: String,
    model_id: &str,
    http_timeout: Duration,
    capabilities: &[ReasoningCapability],
) -> OpenAiCompatibleProvider<EgressHttpTransport, EnvKeyDecryptor> {
    // dep: MiniMax — live provider over the egress transport
    OpenAiCompatibleProvider::with_egress_transport(
        descriptor_declaring(model_id, capabilities),
        MINIMAX_CHAT_URL.to_string(),
        http_timeout,
        EnvKeyDecryptor { key_material: key },
        ssrf::CustomEndpointPolicy::default(),
        live_dns_resolver(),
    )
    .expect("SSRF choke point must accept the endpoint (see live_dns_resolver / HUMAUX_MINIMAX_DNS_PINS)")
}
