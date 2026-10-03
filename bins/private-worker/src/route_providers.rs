//! `private-worker::route_providers` — the one production source of provider instances: one
//!   `OpenAiCompatibleProvider` per admitted Profile@version, built from the route, behind the deployment's
//!   deny-only credential map, recipient ↔ host list and region list (ADR-0060 D-B, D-C, D-J, D-L).
//! Depends-on: crates=[humaux-adapters, humaux-application, humaux-domain, serde_json, sha2, time, tokio, uuid];
//!   services=[HTTP(provider endpoint), PostgreSQL(role_private_worker)]; env=[HUMAUX_PRIVATE_WORKER_CREDENTIALS,
//!   HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS, HUMAUX_PRIVATE_WORKER_REGIONS]; modules=[adapters::byok,
//!   adapters::byok::ssrf, adapters::postgres, adapters::reasoning_route_admission]
//! Called-by: [private-worker::main, tests]
//! Invariants: [an instance is built only from an admitted locator and only after its credential reference is mapped,
//!   its recipient names the host it dials and its region is allowed — no DNS lookup or client build before that;
//!   instances are keyed by (tenant, profile_id, profile_version) and never shared across tenants; a credential
//!   reference resolves to its own mapped key or to none (ADR-0059 D-I); refs sharing one secret (by value) name one
//!   vendor account or the worker does not boot (D-J); an explicitly empty map / list is a legal state in which
//!   every route parks with a class]
//! Spec: Baseline §11.1; §11.2.3; §11.2.4; §11.2.5; §11.4; ADR-0059 D-I; ADR-0060 D-B; ADR-0060 D-C; ADR-0060 D-H;
//!   ADR-0060 D-J; ADR-0060 D-L
//!
//! The reasoners never see this type: they get a `ProviderFor` closure over it (ADR-0060 D-B) and
//! compare every instance with its route (`provider_matches_admission`, D-C). The lists here can
//! refuse a route, never select one (§11.2.5: configuration is not authority).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use humaux_adapters::byok::{
    CredentialDecryptor, CredentialRef, OpenAiCompatibleProvider, PlaintextApiKey,
    ReasoningProviderDescriptor, ReasoningProviderError, UserReasoningProvider, ssrf,
};
use humaux_adapters::postgres::PrivateWorkerDbPool;
use humaux_adapters::reasoning_route_admission::{self, ReasoningAdmissionLocator};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// The required credential map (ADR-0059 D-I): `<credential_ref uuid>=<ENV_NAME>[,…]`. It holds
/// variable NAMES only; each key stays in its own environment variable.
pub const CREDENTIALS: &str = "HUMAUX_PRIVATE_WORKER_CREDENTIALS";
/// ADR-0060 D-C / D-L: `<egress_processor_id uuid>=<host>[|<host>…][,…]` — every recipient this
/// deployment may disclose to, with the hosts it may dial. Required; an explicitly empty value
/// allows none (every route parks).
pub const EGRESS_RECIPIENTS: &str = "HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS";
/// ADR-0060 D-C: `<region>[,…]` — the processing regions this deployment may send to. Required;
/// an explicitly empty value allows none.
pub const REGIONS: &str = "HUMAUX_PRIVATE_WORKER_REGIONS";

/// ADR-0059 D-I: the admitted route's credential reference is not in this process's key map.
pub const CREDENTIAL_NOT_MAPPED: &str = "CREDENTIAL_NOT_MAPPED";
/// ADR-0060 D-C: the route's egress recipient is not one this deployment lists.
pub const EGRESS_PROCESSOR_NOT_ALLOWED: &str = "EGRESS_PROCESSOR_NOT_ALLOWED";
/// ADR-0060 D-L: the route's endpoint host is not one its recipient may dial.
pub const EGRESS_HOST_NOT_ALLOWED: &str = "EGRESS_HOST_NOT_ALLOWED";
/// ADR-0060 D-C: the route's region is not one this deployment lists.
pub const REGION_NOT_ALLOWED: &str = "REGION_NOT_ALLOWED";

/// One mapped key: the variable that holds it and its value.
struct MappedKey {
    env_name: String,
    key: String,
}

/// The BYOK keys this process holds, one per `credential_ref` (§11.1, ADR-0059 D-I). Built once at
/// startup by [`parse_credential_map`] and shared by every cached instance (one map, many
/// clones of the `Arc`); a key leaves it only as a [`PlaintextApiKey`] inside the provider's
/// request build, the §11.1 "closest to the adapter" point. No `Debug`: nothing can format it.
// ponytail: env-held keys until card 54 (OpenBao) replaces this impl. Written out as the desugared
// `async_trait` signature because `async-trait` is a dev-only dependency of this binary.
#[derive(Clone)]
pub struct EnvCredentialMap(Arc<BTreeMap<Uuid, MappedKey>>);

impl EnvCredentialMap {
    /// The references this process can serve.
    #[must_use]
    pub fn refs(&self) -> BTreeSet<Uuid> {
        self.0.keys().copied().collect()
    }

    fn contains(&self, credential_ref: Uuid) -> bool {
        self.0.contains_key(&credential_ref)
    }

    /// `(ref, ENV_NAME, sha256(value))` per entry, for [`check_secret_groups`]. The digest never
    /// leaves this process and is never printed.
    fn secrets(&self) -> Vec<(Uuid, &str, [u8; 32])> {
        self.0
            .iter()
            .map(|(r, m)| (*r, m.env_name.as_str(), Sha256::digest(&m.key).into()))
            .collect()
    }
}

impl CredentialDecryptor for EnvCredentialMap {
    fn resolve<'life0, 'async_trait>(
        &'life0 self,
        credential_ref: CredentialRef,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<PlaintextApiKey, ReasoningProviderError>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        // ADR-0059 D-I: exactly the key of this reference, never a fallback to another one. The
        // miss is defence in depth: `RouteProviders::provider_for` refuses an unmapped reference
        // before any instance, ledger row or call exists (ADR-0060 D-B step 1).
        let key = self
            .0
            .get(&credential_ref.credential_id())
            .map(|mapped| PlaintextApiKey::new(mapped.key.clone()))
            .ok_or(ReasoningProviderError::WaitingKey { fingerprint: None });
        Box::pin(std::future::ready(key))
    }
}

/// Parses [`CREDENTIALS`] through `lookup` (production: the process environment). Refuses, naming
/// the variable and never a value: an unset map (required, no default); an entry without exactly
/// one `=`; a non-UUID or nil reference; a name outside `[A-Z0-9_]+`; a duplicate reference; a
/// named variable that is empty (named) or unset (named by its entry and credential_ref only,
/// never echoed: ADR-0059 D-I). An explicitly empty map is accepted (ADR-0060 D-J): every route
/// then parks `CREDENTIAL_NOT_MAPPED`, so a node with no credentials yet does not crash-loop.
pub fn parse_credential_map(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<EnvCredentialMap, String> {
    let spec = lookup(CREDENTIALS)
        .ok_or_else(|| format!("missing required configuration: {CREDENTIALS}"))?;
    let mut keys = BTreeMap::new();
    if spec.is_empty() {
        return Ok(EnvCredentialMap(Arc::new(keys)));
    }
    for (n, entry) in spec.split(',').enumerate() {
        let invalid = |what: &str| {
            format!(
                "invalid configuration: {CREDENTIALS} entry {} {what}",
                n + 1
            )
        };
        let (reference, name) = entry
            .split_once('=')
            .filter(|(_, name)| !name.contains('='))
            .ok_or_else(|| invalid("is not <credential_ref>=<ENV_NAME>"))?;
        let reference = reference
            .trim()
            .parse::<Uuid>()
            .ok()
            .filter(|r| !r.is_nil())
            .ok_or_else(|| invalid("has no non-nil UUID reference"))?;
        let name = name.trim();
        if name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err(invalid("names a variable outside [A-Z0-9_]+"));
        }
        // ADR-0059 D-I: the right-hand side is echoed only when that variable exists (then it is
        // a variable name); an unset one may be a pasted secret that happens to match
        // [A-Z0-9_]+, so the refusal names the entry, its credential_ref and the pattern only.
        let key = match lookup(name) {
            Some(key) if !key.is_empty() => key,
            Some(_) => {
                return Err(format!(
                    "missing required configuration: {name} is empty (named by {CREDENTIALS} \
                     entry {} for credential_ref {reference})",
                    n + 1
                ));
            }
            None => {
                return Err(format!(
                    "missing required configuration: <unset variable> named by {CREDENTIALS} \
                     entry {} for credential_ref {reference} (a [A-Z0-9_]+ name that is not set; \
                     not echoed)",
                    n + 1
                ));
            }
        };
        let mapped = MappedKey {
            env_name: name.to_owned(),
            key,
        };
        if keys.insert(reference, mapped).is_some() {
            return Err(invalid("repeats a credential reference"));
        }
    }
    Ok(EnvCredentialMap(Arc::new(keys)))
}

/// A bare lowercase DNS name or IPv4 literal: no scheme, port, path, userinfo or wildcard.
fn bare_host(host: &str) -> bool {
    !host.is_empty()
        && !host.starts_with('.')
        && !host.ends_with('.')
        && host
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
}

/// Parses [`EGRESS_RECIPIENTS`] (ADR-0060 D-C / D-L). Unset is refused (required, no default); an
/// explicitly empty value is no recipient. Refused, naming the variable: a blank entry, a missing
/// `=`, a non-UUID or nil uuid, a repeated uuid, an entry with no host, or a host that is not bare
/// (scheme, port, path, uppercase, wildcard).
pub fn parse_recipients(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<BTreeMap<Uuid, Vec<String>>, String> {
    let spec = lookup(EGRESS_RECIPIENTS)
        .ok_or_else(|| format!("missing required configuration: {EGRESS_RECIPIENTS}"))?;
    let mut recipients = BTreeMap::new();
    if spec.is_empty() {
        return Ok(recipients);
    }
    for (n, entry) in spec.split(',').enumerate() {
        let invalid = || {
            format!(
                "invalid configuration: {EGRESS_RECIPIENTS} entry {} is not <egress_processor_id>=<host>[|<host>…] with bare lowercase hosts",
                n + 1
            )
        };
        let (id, hosts) = entry.trim().split_once('=').ok_or_else(invalid)?;
        let id = id
            .parse::<Uuid>()
            .ok()
            .filter(|id| !id.is_nil())
            .ok_or_else(invalid)?;
        let hosts: Vec<String> = hosts.split('|').map(str::to_owned).collect();
        if !hosts.iter().all(|h| bare_host(h)) {
            return Err(invalid());
        }
        if recipients.insert(id, hosts).is_some() {
            return Err(format!(
                "invalid configuration: {EGRESS_RECIPIENTS} entry {} repeats a recipient",
                n + 1
            ));
        }
    }
    Ok(recipients)
}

/// Parses [`REGIONS`] (ADR-0060 D-C). Unset is refused; an explicitly empty value is no region; a
/// blank or repeated entry, or one carrying whitespace, is refused naming the variable.
pub fn parse_regions(lookup: impl Fn(&str) -> Option<String>) -> Result<BTreeSet<String>, String> {
    let spec =
        lookup(REGIONS).ok_or_else(|| format!("missing required configuration: {REGIONS}"))?;
    let mut regions = BTreeSet::new();
    if spec.is_empty() {
        return Ok(regions);
    }
    for (n, region) in spec.split(',').enumerate() {
        if region.is_empty()
            || region.chars().any(char::is_whitespace)
            || !regions.insert(region.to_owned())
        {
            return Err(format!(
                "invalid configuration: {REGIONS} entry {} is blank, repeated or carries whitespace",
                n + 1
            ));
        }
    }
    Ok(regions)
}

/// ADR-0060 D-J — the pure rule behind the boot check. `secrets` is `(ref, ENV_NAME,
/// sha256(value))` per map entry; `accounts` is `(ref, processor_id, external_account_ref_hash)`
/// per account binding the DB knows (`control.reasoning_credential_accounts`). Refs are grouped by
/// the secret VALUE, so two names holding one key (an alias) are one group. Refused, naming refs and
/// ENV_NAMEs only: a ref bound to more than one vendor account; a group spanning more than one
/// vendor account; a ref unknown to the DB that shares its secret with another ref. Returns one
/// boot line per entry (`account_hash=<8 hex>` or `unregistered`), never a value or a digest.
pub fn check_secret_groups(
    secrets: &[(Uuid, &str, [u8; 32])],
    accounts: &[(Uuid, String, Vec<u8>)],
) -> Result<Vec<String>, String> {
    let account_of = |r: Uuid| -> BTreeSet<(&str, &[u8])> {
        accounts
            .iter()
            .filter(|(a, ..)| *a == r)
            .map(|(_, p, h)| (p.as_str(), h.as_slice()))
            .collect()
    };
    let mut groups: BTreeMap<[u8; 32], Vec<(Uuid, &str)>> = BTreeMap::new();
    for (r, name, digest) in secrets {
        groups.entry(*digest).or_default().push((*r, name));
    }
    let mut lines = Vec::new();
    for members in groups.values() {
        let named = || {
            members
                .iter()
                .map(|(r, n)| format!("{r}={n}"))
                .collect::<Vec<_>>()
                .join(",")
        };
        let mut vendor_accounts = BTreeSet::new();
        for (r, name) in members {
            let own = account_of(*r);
            if own.len() > 1 {
                return Err(format!(
                    "invalid configuration: {CREDENTIALS} credential_ref {r} (env {name}) is bound to more than one vendor account"
                ));
            }
            if own.is_empty() && members.len() > 1 {
                return Err(format!(
                    "invalid configuration: {CREDENTIALS} entries {} share one secret but credential_ref {r} is not registered, so its vendor account cannot be proven (ADR-0060 D-J)",
                    named()
                ));
            }
            let hash = own.first().map_or_else(
                || "unregistered".to_owned(),
                |(_, h)| h.iter().take(4).map(|b| format!("{b:02x}")).collect(),
            );
            lines.push(format!("credential ref={r} env={name} account_hash={hash}"));
            vendor_accounts.extend(own);
        }
        if vendor_accounts.len() > 1 {
            return Err(format!(
                "invalid configuration: {CREDENTIALS} entries {} share one secret but name {} vendor accounts; one secret serves one vendor account (ADR-0060 D-J, §11.2.4)",
                named(),
                vendor_accounts.len()
            ));
        }
    }
    Ok(lines)
}

/// ADR-0060 D-J boot check: reads the vendor identity of every mapped reference through the owner
/// definer and applies [`check_secret_groups`]. Bindings and account identity are append-only /
/// immutable (0128), so the verdict holds for the process lifetime.
pub async fn verify_credential_accounts(
    pool: &PrivateWorkerDbPool,
    credentials: &EnvCredentialMap,
) -> Result<Vec<String>, String> {
    let refs: Vec<Uuid> = credentials.refs().into_iter().collect();
    // dep: PostgreSQL(role_private_worker) — control.reasoning_credential_accounts (0206, D-J)
    let accounts = reasoning_route_admission::credential_accounts(pool, &refs)
        .await
        .map_err(|e| format!("credential account check failed: {e}"))?;
    check_secret_groups(&credentials.secrets(), &accounts)
}

/// Cache key: Profile@version is immutable (0128), so the key needs no eviction; the tenant is in
/// it so an instance is never shared across tenants.
type RouteKey = (Uuid, Uuid, i64);

/// ADR-0060 D-B: one provider instance per admitted Profile@version, built from the route.
pub struct RouteProviders {
    credentials: EnvCredentialMap,
    recipients: BTreeMap<Uuid, Vec<String>>,
    regions: BTreeSet<String>,
    resolver: Arc<dyn ssrf::DnsResolver>,
    http_timeout: Duration,
    // ponytail: never evicts; bounded by the Profile@versions admitted in one process lifetime
    // (ADR-0060 L4), each an idle HTTP client. Upgrade: drop entries not admitted for N hours.
    cache: Mutex<HashMap<RouteKey, Arc<dyn UserReasoningProvider>>>,
}

impl RouteProviders {
    /// `resolver` is the one §11.4 resolver every instance's check and dial use (ADR-0039 判据0).
    #[must_use]
    pub fn new(
        credentials: EnvCredentialMap,
        recipients: BTreeMap<Uuid, Vec<String>>,
        regions: BTreeSet<String>,
        resolver: Arc<dyn ssrf::DnsResolver>,
        http_timeout: Duration,
    ) -> Self {
        Self {
            credentials,
            recipients,
            regions,
            resolver,
            http_timeout,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Instances built so far (operator and test visibility of the D-B cache).
    #[must_use]
    pub fn built(&self) -> usize {
        self.cache.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// The instance serving admitted route `l`, or the static NOT_READY class that refuses it
    /// (ADR-0058 D-H). Order: credential map → recipient and host → region → cache → build.
    pub fn provider_for(
        &self,
        l: &ReasoningAdmissionLocator,
    ) -> Result<Arc<dyn UserReasoningProvider>, &'static str> {
        // ADR-0060 D-B step 1 (closes ADR-0059 L7): before any build, ledger row or call.
        if !self.credentials.contains(l.credential_ref) {
            return Err(CREDENTIAL_NOT_MAPPED);
        }
        // D-B step 2 / D-L: the recipient the ledger and the disclosure will name must be allowed
        // to dial this host; parsed without DNS. §11.2.5: deny-only, never selects.
        let hosts = self
            .recipients
            .get(&l.egress_processor_id.0)
            .ok_or(EGRESS_PROCESSOR_NOT_ALLOWED)?;
        let host = ssrf::https_host(&l.endpoint_ref)
            .map_err(|e| ReasoningProviderError::EndpointRejected(e).class())?;
        if !hosts
            .iter()
            .any(|allowed| host == *allowed || host.ends_with(&format!(".{allowed}")))
        {
            return Err(EGRESS_HOST_NOT_ALLOWED);
        }
        if !self.regions.contains(&l.region) {
            return Err(REGION_NOT_ALLOWED);
        }
        let key = (l.tenant_id, l.profile_id, l.profile_version);
        if let Some(hit) = self
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
        {
            return Ok(Arc::clone(hit));
        }
        // ponytail: the §11.4 DNS check runs synchronously on the seat's task, once per
        // Profile@version (ADR-0060 L5); a failure is not cached. Upgrade: spawn_blocking + a
        // negative cache with a TTL.
        let descriptor = ReasoningProviderDescriptor {
            provider_id: l.processor_id.clone(),
            model_id: l.provider_model_id.clone(),
            model_revision: l.model_revision.clone(),
            capabilities: l.capabilities.clone(),
            custom_endpoint: Some(l.endpoint_ref.clone()),
            request_extras: l.request_extras.clone(),
        };
        // dep: HTTP(provider endpoint) — §11.4 SSRF check + egress client build (no request sent)
        let built: Arc<dyn UserReasoningProvider> = Arc::new(
            OpenAiCompatibleProvider::with_egress_transport(
                descriptor,
                l.endpoint_ref.clone(),
                self.http_timeout,
                self.credentials.clone(),
                ssrf::CustomEndpointPolicy::default(),
                Arc::clone(&self.resolver),
            )
            .map_err(|e| e.class())?,
        );
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        let entry = cache.entry(key).or_insert_with(|| {
            // ADR-0060 D-M: once per instance; never endpoint_ref, never a key.
            eprintln!(
                "humaux-private-worker: route provider built tenant={} profile={}@{} provider={} model={} revision={} endpoint_id={} region={} capabilities={}",
                l.tenant_id,
                l.profile_id,
                l.profile_version,
                l.processor_id,
                l.provider_model_id,
                l.model_revision.as_deref().unwrap_or("-"),
                l.provider_endpoint_id,
                l.region,
                l.capabilities
                    .iter()
                    .map(|c| c.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            built
        });
        Ok(Arc::clone(entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_adapters::byok::{
        OutputChannel, ReasoningCapability, StructuredReasoningRequest, structured_request_body,
    };
    use humaux_adapters::distill_reasoner::distill_output_channel;
    use humaux_application::consolidate::{
        PrivateReasoningDomainId, PrivateReasoningPurpose, ReasoningRouteBindingId,
        ReasoningRouteBindingVersion,
    };
    use humaux_domain::egress::ProcessorId;
    use std::net::IpAddr;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// `(variable name, value)` pairs one lookup serves.
    type Vars = Vec<(&'static str, String)>;

    /// A lookup over generated throwaway values (never a literal key).
    fn lookup(vars: Vars) -> impl Fn(&str) -> Option<String> {
        move |name| {
            vars.iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| v.clone())
        }
    }

    fn resolve(map: &EnvCredentialMap, r: Uuid) -> Result<PlaintextApiKey, ReasoningProviderError> {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(map.resolve(CredentialRef::new(r)))
    }

    /// A resolver that answers one reserved-free public test address (TEST-NET is forbidden by
    /// §11.4, so 192.88.99.1 — the 6to4 relay anycast range the e2e tests also use) and counts
    /// lookups; no network.
    #[derive(Default)]
    struct CountingResolver(AtomicU32);

    impl ssrf::DnsResolver for CountingResolver {
        fn resolve(&self, _host: &str) -> Result<Vec<IpAddr>, ssrf::SsrfError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(vec!["192.88.99.1".parse().expect("ip")])
        }
    }

    const U_MM: Uuid = Uuid::from_u128(0xA1);
    const HOST_MM: &str = "api.mm.example";

    fn route(endpoint_ref: &str, caps: &[ReasoningCapability]) -> ReasoningAdmissionLocator {
        ReasoningAdmissionLocator {
            tenant_id: Uuid::from_u128(1),
            binding_id: ReasoningRouteBindingId(Uuid::from_u128(2)),
            binding_version: ReasoningRouteBindingVersion(1),
            reasoning_domain_id: PrivateReasoningDomainId(Uuid::from_u128(3)),
            purpose: PrivateReasoningPurpose::Distill,
            route_policy_id: Uuid::from_u128(4),
            route_policy_version: 1,
            profile_id: Uuid::from_u128(5),
            profile_version: 1,
            provider_account_id: Uuid::from_u128(6),
            processor_id: "vendor-a".to_owned(),
            processor_model_id: Uuid::from_u128(7),
            provider_model_id: "model-a".to_owned(),
            model_revision: None,
            provider_endpoint_id: Uuid::from_u128(8),
            egress_processor_id: ProcessorId(U_MM),
            endpoint_ref: endpoint_ref.to_owned(),
            region: "region-a".to_owned(),
            service_tier: "standard".to_owned(),
            credential_ref: Uuid::from_u128(9),
            billing_account_id: None,
            billing_instrument_id: None,
            provider_health_observation_id: 1,
            account_health_observation_id: 1,
            admitted_at: time::OffsetDateTime::UNIX_EPOCH,
            capabilities: caps.to_vec(),
            request_extras: serde_json::Map::new(),
        }
    }

    const STRUCTURED: [ReasoningCapability; 2] = [
        ReasoningCapability::Text,
        ReasoningCapability::StructuredOutput,
    ];

    /// The deployment of these tests: one key for ref 9, recipient U_MM → HOST_MM, one region.
    fn providers(resolver: &Arc<CountingResolver>) -> RouteProviders {
        let map = parse_credential_map(lookup(vec![
            (CREDENTIALS, format!("{}=HX33B_KEY", Uuid::from_u128(9))),
            ("HX33B_KEY", Uuid::new_v4().simple().to_string()),
        ]))
        .expect("map");
        RouteProviders::new(
            map,
            BTreeMap::from([(U_MM, vec![HOST_MM.to_owned()])]),
            BTreeSet::from(["region-a".to_owned()]),
            Arc::clone(resolver) as Arc<dyn ssrf::DnsResolver>,
            Duration::from_secs(5),
        )
    }

    fn mm_url(host: &str) -> String {
        format!("https://{host}/v1/chat/completions")
    }

    /// T13 (ADR-0060 D-B) — fault: key the cache on `(processor_id, provider_model_id)` ⇒ v2 gets
    /// v1's instance.
    #[test]
    fn route_cache_builds_once_per_profile_version() {
        let resolver = Arc::new(CountingResolver::default());
        let routes = providers(&resolver);
        let v1 = route(&mm_url(HOST_MM), &STRUCTURED);
        let a = routes.provider_for(&v1).expect("built");
        let b = routes.provider_for(&v1).expect("cached");
        assert!(Arc::ptr_eq(&a, &b), "same Profile@version → same instance");
        assert_eq!(resolver.0.load(Ordering::SeqCst), 1, "SSRF check once");
        let mut v2 = v1.clone();
        v2.profile_version = 2;
        v2.endpoint_ref = mm_url(&format!("eu.{HOST_MM}"));
        let c = routes.provider_for(&v2).expect("v2 built");
        assert!(
            !Arc::ptr_eq(&a, &c),
            "a new Profile@version is a new instance"
        );
        assert_eq!(c.endpoint_ref(), v2.endpoint_ref);
        assert_eq!(c.descriptor().model_id, v2.provider_model_id);
        assert_eq!(routes.built(), 2);
    }

    /// T14 (ADR-0060 D-B, card fault 3) — fault: build the descriptor with
    /// `ReasoningCapability::ALL` (or any process list) ⇒ the tool channel reaches a profile that
    /// does not declare TOOL_CALLS. Extras come from the profile too (research amendment 1).
    #[test]
    fn route_descriptor_takes_capabilities_from_the_profile() {
        let resolver = Arc::new(CountingResolver::default());
        let routes = providers(&resolver);
        let request = |channel| StructuredReasoningRequest {
            system_prompt: "s".to_owned(),
            user_prompt: "u".to_owned(),
            json_schema: "{}".to_owned(),
            max_output_tokens: 8,
            output: channel,
        };
        let mut plain = route(&mm_url(HOST_MM), &STRUCTURED);
        plain
            .request_extras
            .insert("vendor_switch".to_owned(), serde_json::Value::Bool(false));
        let instance = routes.provider_for(&plain).expect("built");
        let channel = distill_output_channel(instance.descriptor());
        assert_eq!(channel, OutputChannel::Content);
        let body: serde_json::Value = serde_json::from_slice(&structured_request_body(
            instance.descriptor(),
            &request(channel),
        ))
        .expect("json body");
        assert!(body.get("tools").is_none(), "{body}");
        assert!(body.get("reasoning_split").is_none(), "{body}");
        assert_eq!(body["vendor_switch"], serde_json::Value::Bool(false));
        let mut tools = route(
            &mm_url(HOST_MM),
            &[
                ReasoningCapability::Text,
                ReasoningCapability::StructuredOutput,
                ReasoningCapability::ToolCalls,
            ],
        );
        tools.profile_version = 2;
        let instance = routes.provider_for(&tools).expect("built");
        assert!(matches!(
            distill_output_channel(instance.descriptor()),
            OutputChannel::Tool(_)
        ));
    }

    /// T15 (ADR-0060 D-B step 1) — fault: check the map after the build ⇒ the cache is not empty.
    #[test]
    fn unmapped_credential_ref_is_refused_before_any_build() {
        let resolver = Arc::new(CountingResolver::default());
        let routes = providers(&resolver);
        let mut foreign = route(&mm_url(HOST_MM), &STRUCTURED);
        foreign.credential_ref = Uuid::from_u128(0xBAD);
        assert_eq!(
            routes.provider_for(&foreign).err(),
            Some(CREDENTIAL_NOT_MAPPED)
        );
        assert_eq!((routes.built(), resolver.0.load(Ordering::SeqCst)), (0, 0));
    }

    /// T38 (ADR-0060 D-B step 2, D-L) — fault: check the recipient uuid only ⇒ another vendor's
    /// host is built.
    #[test]
    fn endpoint_host_must_belong_to_its_egress_recipient() {
        let resolver = Arc::new(CountingResolver::default());
        let routes = providers(&resolver);
        for (url, expected) in [
            (mm_url("api.other.example"), Err(EGRESS_HOST_NOT_ALLOWED)),
            (mm_url("xapi.mm.example"), Err(EGRESS_HOST_NOT_ALLOWED)),
        ] {
            let refused = routes.provider_for(&route(&url, &STRUCTURED)).map(|_| ());
            assert_eq!(refused, expected, "{url}");
        }
        assert_eq!(
            (routes.built(), resolver.0.load(Ordering::SeqCst)),
            (0, 0),
            "no DNS lookup, no build"
        );
        let mut unknown = route(&mm_url(HOST_MM), &STRUCTURED);
        unknown.egress_processor_id = ProcessorId(Uuid::from_u128(0xA2));
        assert_eq!(
            routes.provider_for(&unknown).err(),
            Some(EGRESS_PROCESSOR_NOT_ALLOWED)
        );
        let mut region = route(&mm_url(HOST_MM), &STRUCTURED);
        region.region = "region-b".to_owned();
        assert_eq!(routes.provider_for(&region).err(), Some(REGION_NOT_ALLOWED));
        assert!(
            routes
                .provider_for(&route(&mm_url(&format!("eu.{HOST_MM}")), &STRUCTURED))
                .is_ok(),
            "a subdomain of an allowed host"
        );
    }

    /// T16 (card 33 T23, ADR-0059 D-I; card fault 2) — fault: return the first key for every
    /// reference.
    #[test]
    fn credential_map_resolves_each_ref_to_its_own_key() {
        let (r1, r2, r3) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let (k1, k2) = (
            Uuid::new_v4().simple().to_string(),
            Uuid::new_v4().simple().to_string(),
        );
        let map = parse_credential_map(lookup(vec![
            (CREDENTIALS, format!("{r1}=HX33_KEY_ONE,{r2}=HX33_KEY_TWO")),
            ("HX33_KEY_ONE", k1.clone()),
            ("HX33_KEY_TWO", k2.clone()),
        ]))
        .expect("valid map");
        assert_eq!(map.refs(), BTreeSet::from([r1, r2]));
        // `assert!(a == b)`: a failure never prints a key.
        assert!(
            resolve(&map, r1).expect("r1 mapped").expose() == k1,
            "r1 must get k1"
        );
        assert!(
            resolve(&map, r2).expect("r2 mapped").expose() == k2,
            "r2 must get k2"
        );
        assert!(matches!(
            resolve(&map, r3),
            Err(ReasoningProviderError::WaitingKey { fingerprint: None })
        ));
    }

    /// ADR-0059 D-I (card 33 review P2): an unset right-hand side is reported by entry and
    /// credential_ref only. Fault: echo the right-hand side whatever it is.
    #[test]
    fn credential_map_unset_rhs_is_never_echoed() {
        let r = Uuid::new_v4();
        let pasted = Uuid::new_v4().simple().to_string().to_ascii_uppercase();
        let Err(unset) = parse_credential_map(lookup(vec![(CREDENTIALS, format!("{r}={pasted}"))]))
        else {
            panic!("an unset right-hand side is refused");
        };
        assert!(!unset.contains(&pasted), "unset rhs echoed: {unset}");
        assert!(unset.contains("<unset variable>"), "{unset}");
        assert!(
            unset.contains(&r.to_string()),
            "names the credential_ref: {unset}"
        );
        let Err(empty_named) = parse_credential_map(lookup(vec![
            (CREDENTIALS, format!("{r}=HX33_KEY")),
            ("HX33_KEY", String::new()),
        ])) else {
            panic!("an empty variable is refused");
        };
        assert!(
            empty_named.contains("HX33_KEY is empty") && empty_named.contains(&r.to_string()),
            "{empty_named}"
        );
    }

    /// Card 33 T24 + T35 (ADR-0060 D-J) — every boot refusal names a variable and never echoes a
    /// value; an explicitly empty map boots. Fault (T35): refuse an empty map.
    #[test]
    fn boot_accepts_explicitly_empty_credential_map() {
        let r = Uuid::new_v4();
        let key = Uuid::new_v4().simple().to_string();
        let ok_key = || ("HX33_KEY", key.clone());
        let cases: Vec<(&str, Vars, &str)> = vec![
            ("unset map", vec![ok_key()], CREDENTIALS),
            (
                "no =",
                vec![(CREDENTIALS, format!("{r}")), ok_key()],
                CREDENTIALS,
            ),
            (
                "two =",
                vec![(CREDENTIALS, format!("{r}=HX33_KEY=X")), ok_key()],
                CREDENTIALS,
            ),
            (
                "bad uuid",
                vec![(CREDENTIALS, "not-a-uuid=HX33_KEY".into()), ok_key()],
                CREDENTIALS,
            ),
            (
                "nil uuid",
                vec![(CREDENTIALS, format!("{}=HX33_KEY", Uuid::nil())), ok_key()],
                CREDENTIALS,
            ),
            (
                "bad name",
                vec![(CREDENTIALS, format!("{r}=hx33-key")), ok_key()],
                CREDENTIALS,
            ),
            (
                "empty name",
                vec![(CREDENTIALS, format!("{r}=")), ok_key()],
                CREDENTIALS,
            ),
            (
                "duplicate ref",
                vec![
                    (CREDENTIALS, format!("{r}=HX33_KEY,{r}=HX33_KEY")),
                    ok_key(),
                ],
                CREDENTIALS,
            ),
            (
                "unset key variable",
                vec![(CREDENTIALS, format!("{r}=HX33_KEY"))],
                "<unset variable>",
            ),
            (
                "empty key variable",
                vec![
                    (CREDENTIALS, format!("{r}=HX33_KEY")),
                    ("HX33_KEY", String::new()),
                ],
                "HX33_KEY",
            ),
        ];
        for (what, vars, named) in cases {
            let Err(error) = parse_credential_map(lookup(vars)) else {
                panic!("{what}: must be refused");
            };
            assert!(error.contains(named), "{what}: names {named}: {error}");
            assert!(
                !error.contains(&key),
                "{what}: the error must not carry a key value"
            );
        }
        let empty =
            parse_credential_map(lookup(vec![(CREDENTIALS, String::new())])).expect("empty map");
        assert!(empty.refs().is_empty());
    }

    /// T23 (ADR-0060 D-C / D-L) — fault: accept an unset variable as empty, or a host carrying a
    /// scheme.
    #[test]
    fn recipient_and_region_lists_parse() {
        let (u1, u2) = (Uuid::new_v4(), Uuid::new_v4());
        let ok = parse_recipients(lookup(vec![(
            EGRESS_RECIPIENTS,
            format!("{u1}=h1|h2,{u2}=h3"),
        )]))
        .expect("two recipients");
        assert_eq!(ok.len(), 2);
        assert_eq!(ok.values().map(Vec::len).sum::<usize>(), 3);
        assert!(
            parse_recipients(lookup(vec![(EGRESS_RECIPIENTS, String::new())]))
                .expect("explicitly empty")
                .is_empty()
        );
        let unset = parse_recipients(lookup(vec![])).expect_err("unset");
        assert!(unset.contains(EGRESS_RECIPIENTS), "{unset}");
        for bad in [
            format!("{u1}=h1,,{u2}=h3"),
            format!("{}=h1", Uuid::nil()),
            format!("{u1}=h1,{u1}=h2"),
            format!("{u1}="),
            format!("{u1}=https://h"),
            format!("{u1}=h:443"),
            format!("{u1}=h/x"),
            format!("{u1}=H1"),
            format!("{u1}=*.h"),
            "not-a-uuid=h".to_owned(),
        ] {
            let error =
                parse_recipients(lookup(vec![(EGRESS_RECIPIENTS, bad.clone())])).expect_err(&bad);
            assert!(error.contains(EGRESS_RECIPIENTS), "{bad}: {error}");
        }
        assert_eq!(
            parse_regions(lookup(vec![(REGIONS, "cn-a,cn-b".to_owned())]))
                .expect("regions")
                .len(),
            2
        );
        assert!(
            parse_regions(lookup(vec![(REGIONS, String::new())]))
                .expect("explicitly empty")
                .is_empty()
        );
        assert!(
            parse_regions(lookup(vec![]))
                .expect_err("unset")
                .contains(REGIONS)
        );
        for bad in ["cn-a,", "cn-a,cn-a", "cn a"] {
            assert!(
                parse_regions(lookup(vec![(REGIONS, bad.to_owned())]))
                    .expect_err(bad)
                    .contains(REGIONS)
            );
        }
    }

    /// T36 (ADR-0060 D-J) — fault: group by ENV_NAME only ⇒ the alias case boots.
    #[test]
    fn shared_secret_requires_one_vendor_account() {
        let (ra, rb) = (Uuid::from_u128(0xA), Uuid::from_u128(0xB));
        let digest = |v: &str| -> [u8; 32] { Sha256::digest(v).into() };
        let account = |r: Uuid, hash: u8| (r, "vendor".to_owned(), vec![hash; 32]);
        // Same variable, two vendor accounts.
        let one_env = [(ra, "K", digest("k")), (rb, "K", digest("k"))];
        let error = check_secret_groups(&one_env, &[account(ra, 1), account(rb, 2)])
            .expect_err("one secret, two accounts");
        assert!(
            error.contains(&ra.to_string())
                && error.contains(&rb.to_string())
                && error.contains("=K"),
            "{error}"
        );
        // Two variables holding one value (alias), two accounts.
        let alias = [(ra, "K1", digest("k")), (rb, "K2", digest("k"))];
        assert!(check_secret_groups(&alias, &[account(ra, 1), account(rb, 2)]).is_err());
        // Same vendor account: allowed.
        let lines =
            check_secret_groups(&alias, &[account(ra, 1), account(rb, 1)]).expect("one account");
        assert_eq!(lines.len(), 2);
        assert!(
            lines.iter().all(|l| l.contains("account_hash=01010101")),
            "{lines:?}"
        );
        // One ref bound to two accounts.
        let single = [(ra, "K", digest("k"))];
        assert!(check_secret_groups(&single, &[account(ra, 1), account(ra, 2)]).is_err());
        // An unregistered ref sharing a secret.
        assert!(check_secret_groups(&one_env, &[account(ra, 1)]).is_err());
        // An unregistered ref sharing nothing.
        let lines = check_secret_groups(
            &[(ra, "K1", digest("k1")), (rb, "K2", digest("k2"))],
            &[account(ra, 1)],
        )
        .expect("nothing shared");
        assert!(
            lines.iter().any(|l| l.contains("unregistered")),
            "{lines:?}"
        );
    }
}
