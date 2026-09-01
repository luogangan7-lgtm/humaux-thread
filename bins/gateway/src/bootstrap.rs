//! Process bootstrap for the one configured Gateway write stream.
//!
//! This module owns process configuration only. Tool arguments never select a
//! tenant, stream, credential verifier, listener, or rate policy.
//! `GuardSettings::tenant_network` is deliberately empty in this single-policy
//! process. A tenant-specific network-policy loader needs a separate approved
//! authorization and acceptance gate before this bootstrap can serve it.

use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use humaux_adapters::{postgres::RuntimeDbPool, quota_repo::RatePolicy};
use humaux_contracts::config_registry::{
    ConfigEntry, effective_config_fingerprint, resolve_effective_config,
};
use humaux_contracts::retrieval_config::{
    resolve_registered_retrieval_profile, retrieval_profile_registry,
};
use humaux_domain::{
    context::ContextBudget, dataclass::DataClass, identity::VisibilityClass, ids::TenantId,
};
use humaux_projection::stream::StreamKey;
use humaux_protocol::{
    edge::{Cidr, TrustedProxyConfig},
    mcp::{McpAdapter, McpHttpConfig},
    mcp_catalog::CanonicalCatalog,
};
use uuid::Uuid;

use crate::{
    context::ContextBootstrap,
    guard::{GatewayGuard, GuardRatePolicies, GuardSettings},
    mcp_application::GatewayMcpApplication,
    remember::{RememberEventKind, RememberPolicy},
};

const PREFIX: &str = "HUMAUX_GATEWAY_";

/// A fail-closed bootstrap error. It intentionally contains configuration keys,
/// never a DSN, pepper, or another secret value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapError {
    key: String,
    reason: &'static str,
}

impl BootstrapError {
    fn new(key: impl Into<String>, reason: &'static str) -> Self {
        Self {
            key: key.into(),
            reason,
        }
    }
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "gateway bootstrap configuration {}: {}",
            self.key, self.reason
        )
    }
}

impl std::error::Error for BootstrapError {}

/// Parsed process configuration. Secret fields stay private and this type does
/// not implement `Debug` so a normal startup error path cannot expose them.
pub struct GatewayBootstrap {
    bind_addr: SocketAddr,
    http: McpHttpConfig,
    pg_dsn: String,
    guard: GuardSettings,
    remember_policy: RememberPolicy,
    remember_event_kind: RememberEventKind,
    context_bootstrap: ContextBootstrap,
    config_fingerprint: String,
}

/// Ready-to-serve components for `main`: bind [`Self::bind_addr`], then serve
/// [`Self::adapter`] through axum's `into_make_service_with_connect_info`.
pub struct GatewayRuntime {
    bind_addr: SocketAddr,
    adapter: McpAdapter,
    config_fingerprint: String,
}

impl GatewayRuntime {
    #[must_use]
    pub const fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    #[must_use]
    pub fn adapter(&self) -> &McpAdapter {
        &self.adapter
    }

    /// Safe to record at startup: every secret was replaced with `<redacted>`.
    #[must_use]
    pub fn config_fingerprint(&self) -> &str {
        &self.config_fingerprint
    }
}

impl GatewayBootstrap {
    /// Loads every `HUMAUX_GATEWAY_*` variable through the declared typed
    /// registry. Unknown, missing, empty, and malformed configuration fails
    /// before any database connection or listener bind.
    pub fn load_from_env() -> Result<Self, BootstrapError> {
        let raw = std::env::vars_os()
            .filter(|(key, _)| key.to_string_lossy().starts_with(PREFIX))
            .map(|(key, value)| {
                let key = key
                    .into_string()
                    .map_err(|_| BootstrapError::new(PREFIX, "non-Unicode configuration key"))?;
                let value = value
                    .into_string()
                    .map_err(|_| BootstrapError::new(&key, "non-Unicode configuration value"))?;
                Ok((key, value))
            })
            .collect::<Result<BTreeMap<_, _>, BootstrapError>>()?;
        Self::from_raw(raw)
    }

    /// Connects the checked `role_gateway` pool, then constructs the existing
    /// Guard/application/adapter chain. No caller can inject an alternate pool
    /// or a per-request write policy.
    pub async fn build(self) -> Result<GatewayRuntime, BootstrapError> {
        let pool = RuntimeDbPool::connect(&self.pg_dsn)
            .await
            .map_err(|_| BootstrapError::new("HUMAUX_GATEWAY_PG_DSN", "connection rejected"))?;
        let guard = Arc::new(
            GatewayGuard::new(pool, self.guard)
                .map_err(|_| BootstrapError::new("gateway guard", "unsafe configuration"))?,
        );
        let catalog = CanonicalCatalog::load().map_err(|_| {
            BootstrapError::new("canonical MCP catalog", "invalid embedded contract")
        })?;
        let advertised = catalog.trusted_catalog().map_err(|_| {
            BootstrapError::new("canonical MCP catalog", "invalid embedded contract")
        })?;
        let application = Arc::new(GatewayMcpApplication::new(
            catalog,
            guard,
            self.remember_policy,
            self.remember_event_kind,
            self.context_bootstrap,
        ));
        Ok(GatewayRuntime {
            bind_addr: self.bind_addr,
            adapter: McpAdapter::new(application, advertised, self.http),
            config_fingerprint: self.config_fingerprint,
        })
    }

    fn from_raw(raw: BTreeMap<String, String>) -> Result<Self, BootstrapError> {
        let registry = registry();
        let effective = resolve_effective_config(&registry, &raw)
            .map_err(|error| BootstrapError::new(error.entry_name, "missing or undeclared"))?;
        let remember_policy = parse_remember_policy(&effective)?;
        let context_bootstrap = parse_context_bootstrap(&effective, &remember_policy)?;

        Ok(Self {
            bind_addr: parse_bind_addr(required(&effective, "HUMAUX_GATEWAY_BIND_ADDR")?)?,
            http: parse_http(&effective)?,
            pg_dsn: required(&effective, "HUMAUX_GATEWAY_PG_DSN")?.to_owned(),
            guard: parse_guard(&effective)?,
            remember_policy,
            remember_event_kind: parse_remember_event_kind(&effective)?,
            context_bootstrap,
            config_fingerprint: redacted_fingerprint(&registry, &effective),
        })
    }
}

fn parse_http(effective: &BTreeMap<String, String>) -> Result<McpHttpConfig, BootstrapError> {
    McpHttpConfig::new(
        csv_nonempty(
            required(effective, "HUMAUX_GATEWAY_ALLOWED_HOSTS")?,
            "HUMAUX_GATEWAY_ALLOWED_HOSTS",
        )?,
        csv_nonempty(
            required(effective, "HUMAUX_GATEWAY_ALLOWED_ORIGINS")?,
            "HUMAUX_GATEWAY_ALLOWED_ORIGINS",
        )?,
        parse_usize(
            required(effective, "HUMAUX_GATEWAY_MAX_REQUEST_BODY_BYTES")?,
            "HUMAUX_GATEWAY_MAX_REQUEST_BODY_BYTES",
        )?,
    )
    .map_err(|_| BootstrapError::new("HUMAUX_GATEWAY_MAX_REQUEST_BODY_BYTES", "invalid"))
}

fn parse_guard(effective: &BTreeMap<String, String>) -> Result<GuardSettings, BootstrapError> {
    let pepper = hex::decode(required(effective, "HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX")?)
        .map_err(|_| BootstrapError::new("HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX", "invalid hex"))?;
    if pepper.is_empty() {
        return Err(BootstrapError::new(
            "HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX",
            "must not be empty",
        ));
    }
    let guard = GuardSettings {
        credential_pepper: pepper,
        trusted_proxies: TrustedProxyConfig {
            trusted_proxy_cidrs: cidrs(
                present(effective, "HUMAUX_GATEWAY_TRUSTED_PROXY_CIDRS")?,
                "HUMAUX_GATEWAY_TRUSTED_PROXY_CIDRS",
            )?,
            max_forwarded_hops: parse_usize(
                required(effective, "HUMAUX_GATEWAY_MAX_FORWARDED_HOPS")?,
                "HUMAUX_GATEWAY_MAX_FORWARDED_HOPS",
            )?,
        },
        global_denylist: cidrs(
            present(effective, "HUMAUX_GATEWAY_GLOBAL_DENYLIST")?,
            "HUMAUX_GATEWAY_GLOBAL_DENYLIST",
        )?,
        global_emergency_allowlist: cidrs(
            present(effective, "HUMAUX_GATEWAY_GLOBAL_EMERGENCY_ALLOWLIST")?,
            "HUMAUX_GATEWAY_GLOBAL_EMERGENCY_ALLOWLIST",
        )?,
        tenant_network: BTreeMap::new(),
        rates: GuardRatePolicies {
            preauth_ip: rate(effective, "PREAUTH_IP")?,
            credential: rate(effective, "CREDENTIAL")?,
            user: rate(effective, "USER")?,
            tenant: rate(effective, "TENANT")?,
            operation: rate(effective, "OPERATION")?,
        },
        reservation_ttl: seconds(
            required(effective, "HUMAUX_GATEWAY_RESERVATION_TTL_SECONDS")?,
            "HUMAUX_GATEWAY_RESERVATION_TTL_SECONDS",
        )?,
        handler_timeout: seconds(
            required(effective, "HUMAUX_GATEWAY_HANDLER_TIMEOUT_SECONDS")?,
            "HUMAUX_GATEWAY_HANDLER_TIMEOUT_SECONDS",
        )?,
        finalize_timeout: seconds(
            required(effective, "HUMAUX_GATEWAY_FINALIZE_TIMEOUT_SECONDS")?,
            "HUMAUX_GATEWAY_FINALIZE_TIMEOUT_SECONDS",
        )?,
        replay_ttl: seconds(
            required(effective, "HUMAUX_GATEWAY_REPLAY_TTL_SECONDS")?,
            "HUMAUX_GATEWAY_REPLAY_TTL_SECONDS",
        )?,
    };
    validate_guard(&guard)?;
    Ok(guard)
}

fn parse_remember_policy(
    effective: &BTreeMap<String, String>,
) -> Result<RememberPolicy, BootstrapError> {
    let tenant = TenantId(uuid(
        required(effective, "HUMAUX_GATEWAY_REMEMBER_TENANT_ID")?,
        "HUMAUX_GATEWAY_REMEMBER_TENANT_ID",
    )?);
    let workspace = uuid(
        required(effective, "HUMAUX_GATEWAY_REMEMBER_WORKSPACE_ID")?,
        "HUMAUX_GATEWAY_REMEMBER_WORKSPACE_ID",
    )?;
    let scope_kind = required(effective, "HUMAUX_GATEWAY_REMEMBER_SCOPE_KIND")?;
    if scope_kind != "workspace" {
        return Err(BootstrapError::new(
            "HUMAUX_GATEWAY_REMEMBER_SCOPE_KIND",
            "only workspace is enabled",
        ));
    }
    RememberPolicy::new(
        StreamKey::new(
            tenant,
            scope_kind,
            workspace,
            nonempty(
                required(effective, "HUMAUX_GATEWAY_REMEMBER_DOMAIN")?,
                "HUMAUX_GATEWAY_REMEMBER_DOMAIN",
            )?,
            nonempty(
                required(effective, "HUMAUX_GATEWAY_REMEMBER_PROJECTION_KIND")?,
                "HUMAUX_GATEWAY_REMEMBER_PROJECTION_KIND",
            )?,
            nonempty(
                required(effective, "HUMAUX_GATEWAY_REMEMBER_PROJECTION_VERSION")?,
                "HUMAUX_GATEWAY_REMEMBER_PROJECTION_VERSION",
            )?,
        ),
        uuid(
            required(effective, "HUMAUX_GATEWAY_REMEMBER_REASONING_DOMAIN_ID")?,
            "HUMAUX_GATEWAY_REMEMBER_REASONING_DOMAIN_ID",
        )?,
        seconds(
            required(effective, "HUMAUX_GATEWAY_REMEMBER_TOKEN_TTL_SECONDS")?,
            "HUMAUX_GATEWAY_REMEMBER_TOKEN_TTL_SECONDS",
        )?,
        data_class(required(effective, "HUMAUX_GATEWAY_REMEMBER_DATA_CLASS")?)?,
        visibility(required(
            effective,
            "HUMAUX_GATEWAY_REMEMBER_VISIBILITY_CLASS",
        )?)?,
    )
    .map_err(|_| BootstrapError::new("HUMAUX_GATEWAY_REMEMBER_*", "invalid write policy"))
}

fn parse_remember_event_kind(
    effective: &BTreeMap<String, String>,
) -> Result<RememberEventKind, BootstrapError> {
    event_kind(required(effective, "HUMAUX_GATEWAY_REMEMBER_EVENT_KIND")?)
}

fn parse_context_bootstrap(
    effective: &BTreeMap<String, String>,
    write_policy: &RememberPolicy,
) -> Result<ContextBootstrap, BootstrapError> {
    let budget = ContextBudget::new(
        parse_u32(
            required(effective, "HUMAUX_GATEWAY_CONTEXT_TOTAL_TOKENS")?,
            "HUMAUX_GATEWAY_CONTEXT_TOTAL_TOKENS",
        )?,
        parse_u32(
            required(effective, "HUMAUX_GATEWAY_CONTEXT_MANDATORY_TOKENS")?,
            "HUMAUX_GATEWAY_CONTEXT_MANDATORY_TOKENS",
        )?,
    )
    .map_err(|_| BootstrapError::new("HUMAUX_GATEWAY_CONTEXT_*_TOKENS", "invalid budget"))?;
    let raw = retrieval_profile_registry()
        .into_iter()
        .map(|entry| {
            let value = required(effective, &retrieval_env_key(&entry.name))?.to_owned();
            Ok((entry.name, value))
        })
        .collect::<Result<BTreeMap<_, _>, BootstrapError>>()?;
    let profile = resolve_registered_retrieval_profile(&raw)
        .map_err(|_| BootstrapError::new("HUMAUX_GATEWAY_RETRIEVAL_*", "invalid profile"))?;
    ContextBootstrap::new(budget, profile, write_policy)
        .map_err(|_| BootstrapError::new("gateway binary", "provenance unavailable"))
}

fn retrieval_env_key(canonical_key: &str) -> String {
    format!(
        "{PREFIX}{}",
        canonical_key.replace('.', "_").to_ascii_uppercase()
    )
}

fn registry() -> Vec<ConfigEntry> {
    let mut entries = [
        ("BIND_ADDR", "socket_addr", false),
        ("ALLOWED_HOSTS", "csv", false),
        ("ALLOWED_ORIGINS", "csv", false),
        ("MAX_REQUEST_BODY_BYTES", "usize", false),
        ("PG_DSN", "dsn", true),
        ("CREDENTIAL_PEPPER_HEX", "hex", true),
        ("TRUSTED_PROXY_CIDRS", "csv-cidr", false),
        ("MAX_FORWARDED_HOPS", "usize", false),
        ("GLOBAL_DENYLIST", "csv-cidr", false),
        ("GLOBAL_EMERGENCY_ALLOWLIST", "csv-cidr", false),
        ("RESERVATION_TTL_SECONDS", "u64", false),
        ("HANDLER_TIMEOUT_SECONDS", "u64", false),
        ("FINALIZE_TIMEOUT_SECONDS", "u64", false),
        ("REPLAY_TTL_SECONDS", "u64", false),
        ("REMEMBER_TENANT_ID", "uuid", false),
        ("REMEMBER_WORKSPACE_ID", "uuid", false),
        ("REMEMBER_SCOPE_KIND", "enum:workspace", false),
        ("REMEMBER_DOMAIN", "string", false),
        ("REMEMBER_PROJECTION_KIND", "string", false),
        ("REMEMBER_PROJECTION_VERSION", "string", false),
        ("REMEMBER_REASONING_DOMAIN_ID", "uuid", false),
        ("REMEMBER_TOKEN_TTL_SECONDS", "u64", false),
        (
            "REMEMBER_DATA_CLASS",
            "enum:PUBLIC|INTERNAL|PRIVATE|SENSITIVE|SECRET_MATERIAL",
            false,
        ),
        (
            "REMEMBER_VISIBILITY_CLASS",
            "enum:USER_PRIVATE|WORKSPACE_SHARED",
            false,
        ),
        ("REMEMBER_EVENT_KIND", "enum", false),
        ("CONTEXT_TOTAL_TOKENS", "u32", false),
        ("CONTEXT_MANDATORY_TOKENS", "u32", false),
    ]
    .into_iter()
    .map(|(suffix, type_name, secret)| entry(&format!("{PREFIX}{suffix}"), type_name, secret))
    .collect::<Vec<_>>();
    for name in ["PREAUTH_IP", "CREDENTIAL", "USER", "TENANT", "OPERATION"] {
        entries.push(entry(
            &format!("HUMAUX_GATEWAY_RATE_{name}_CAPACITY"),
            "i64",
            false,
        ));
        entries.push(entry(
            &format!("HUMAUX_GATEWAY_RATE_{name}_REFILL_PER_SECOND"),
            "i64",
            false,
        ));
    }
    entries.extend(retrieval_profile_registry().into_iter().map(|mut entry| {
        entry.name = retrieval_env_key(&entry.name);
        entry
    }));
    entries
}

fn entry(name: &str, type_name: &str, secret: bool) -> ConfigEntry {
    ConfigEntry {
        name: name.to_owned(),
        type_name: type_name.to_owned(),
        default: None,
        scope: "process".to_owned(),
        secret,
        reloadability: "static".to_owned(),
        owner_module: "gateway.bootstrap".to_owned(),
    }
}

fn required<'a>(
    effective: &'a BTreeMap<String, String>,
    key: &str,
) -> Result<&'a str, BootstrapError> {
    effective
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .map(String::as_str)
        .ok_or_else(|| BootstrapError::new(key, "missing or empty"))
}

fn present<'a>(
    effective: &'a BTreeMap<String, String>,
    key: &str,
) -> Result<&'a str, BootstrapError> {
    effective
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| BootstrapError::new(key, "missing"))
}

fn nonempty<'a>(value: &'a str, key: &str) -> Result<&'a str, BootstrapError> {
    (!value.trim().is_empty())
        .then_some(value)
        .ok_or_else(|| BootstrapError::new(key, "missing or empty"))
}

fn parse_bind_addr(value: &str) -> Result<SocketAddr, BootstrapError> {
    let addr = value
        .parse::<SocketAddr>()
        .map_err(|_| BootstrapError::new("HUMAUX_GATEWAY_BIND_ADDR", "invalid socket address"))?;
    let unsafe_ip = match addr.ip() {
        IpAddr::V4(ip) => ip.is_unspecified() || ip.is_multicast() || ip.is_broadcast(),
        IpAddr::V6(ip) => ip.is_unspecified() || ip.is_multicast(),
    };
    if addr.port() == 0 || unsafe_ip {
        return Err(BootstrapError::new(
            "HUMAUX_GATEWAY_BIND_ADDR",
            "unsafe listener address",
        ));
    }
    Ok(addr)
}

fn parse_usize(value: &str, key: &str) -> Result<usize, BootstrapError> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| BootstrapError::new(key, "must be a positive integer"))
}

fn parse_u32(value: &str, key: &str) -> Result<u32, BootstrapError> {
    value
        .parse::<u32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| BootstrapError::new(key, "must be a positive integer"))
}

fn seconds(value: &str, key: &str) -> Result<Duration, BootstrapError> {
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .map(Duration::from_secs)
        .ok_or_else(|| BootstrapError::new(key, "must be positive seconds"))
}

fn uuid(value: &str, key: &str) -> Result<Uuid, BootstrapError> {
    Uuid::parse_str(value)
        .ok()
        .filter(|value| !value.is_nil())
        .ok_or_else(|| BootstrapError::new(key, "must be a non-nil UUID"))
}

fn csv_nonempty(value: &str, key: &str) -> Result<Vec<String>, BootstrapError> {
    let values = value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if values.is_empty() || value.split(',').any(|item| item.trim().is_empty()) {
        return Err(BootstrapError::new(key, "must be a nonempty CSV"));
    }
    Ok(values)
}

fn cidrs(value: &str, key: &str) -> Result<Vec<Cidr>, BootstrapError> {
    if value.is_empty() {
        return Ok(vec![]);
    }
    csv_nonempty(value, key)?
        .into_iter()
        .map(|cidr| {
            cidr.parse()
                .map_err(|_| BootstrapError::new(key, "invalid CIDR"))
        })
        .collect()
}

fn rate(effective: &BTreeMap<String, String>, name: &str) -> Result<RatePolicy, BootstrapError> {
    let capacity_key = format!("HUMAUX_GATEWAY_RATE_{name}_CAPACITY");
    let refill_key = format!("HUMAUX_GATEWAY_RATE_{name}_REFILL_PER_SECOND");
    let capacity = required(effective, &capacity_key)?
        .parse::<i64>()
        .map_err(|_| BootstrapError::new(&capacity_key, "invalid integer"))?;
    let refill = required(effective, &refill_key)?
        .parse::<i64>()
        .map_err(|_| BootstrapError::new(&refill_key, "invalid integer"))?;
    RatePolicy::new(capacity, refill).map_err(|_| BootstrapError::new(name, "invalid rate policy"))
}

fn validate_guard(guard: &GuardSettings) -> Result<(), BootstrapError> {
    let execution = guard
        .handler_timeout
        .checked_add(guard.finalize_timeout)
        .ok_or_else(|| BootstrapError::new("HUMAUX_GATEWAY_HANDLER_TIMEOUT_SECONDS", "overflow"))?;
    if guard.trusted_proxies.max_forwarded_hops == 0 || guard.reservation_ttl <= execution {
        return Err(BootstrapError::new(
            "gateway guard",
            "unsafe TTL or proxy policy",
        ));
    }
    Ok(())
}

fn data_class(value: &str) -> Result<DataClass, BootstrapError> {
    match value {
        "PUBLIC" => Ok(DataClass::Public),
        "INTERNAL" => Ok(DataClass::Internal),
        "PRIVATE" => Ok(DataClass::Private),
        "SENSITIVE" => Ok(DataClass::Sensitive),
        "SECRET_MATERIAL" => Ok(DataClass::SecretMaterial),
        _ => Err(BootstrapError::new(
            "HUMAUX_GATEWAY_REMEMBER_DATA_CLASS",
            "unknown closed enum value",
        )),
    }
}

fn visibility(value: &str) -> Result<VisibilityClass, BootstrapError> {
    match value {
        "USER_PRIVATE" => Ok(VisibilityClass::UserPrivate),
        "WORKSPACE_SHARED" => Ok(VisibilityClass::WorkspaceShared),
        _ => Err(BootstrapError::new(
            "HUMAUX_GATEWAY_REMEMBER_VISIBILITY_CLASS",
            "only USER_PRIVATE or WORKSPACE_SHARED is enabled",
        )),
    }
}

fn event_kind(value: &str) -> Result<RememberEventKind, BootstrapError> {
    match value {
        "USER_MESSAGE" => Ok(RememberEventKind::UserMessage),
        "ASSISTANT_MESSAGE" => Ok(RememberEventKind::AssistantMessage),
        "TOOL_CALL" => Ok(RememberEventKind::ToolCall),
        "TOOL_RESULT" => Ok(RememberEventKind::ToolResult),
        "MANUAL_NOTE" => Ok(RememberEventKind::ManualNote),
        "USER_CORRECTION" => Ok(RememberEventKind::UserCorrection),
        "TASK_EVENT" => Ok(RememberEventKind::TaskEvent),
        "GIT_EVENT" => Ok(RememberEventKind::GitEvent),
        "SYSTEM_IMPORT" => Ok(RememberEventKind::SystemImport),
        _ => Err(BootstrapError::new(
            "HUMAUX_GATEWAY_REMEMBER_EVENT_KIND",
            "unknown closed enum value",
        )),
    }
}

fn redacted_fingerprint(registry: &[ConfigEntry], effective: &BTreeMap<String, String>) -> String {
    let redacted = effective
        .iter()
        .map(|(key, value)| {
            let secret = registry
                .iter()
                .any(|entry| entry.name == *key && entry.secret);
            (
                key.clone(),
                if secret {
                    "<redacted>".to_owned()
                } else {
                    value.clone()
                },
            )
        })
        .collect();
    effective_config_fingerprint(&redacted)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw() -> BTreeMap<String, String> {
        let tenant = Uuid::now_v7();
        let workspace = Uuid::now_v7();
        let reasoning = Uuid::now_v7();
        let mut raw = BTreeMap::new();
        for entry in registry() {
            let value = match entry.name.as_str() {
                "HUMAUX_GATEWAY_BIND_ADDR" => "127.0.0.1:8080".into(),
                "HUMAUX_GATEWAY_ALLOWED_HOSTS" => "mcp.example.test".into(),
                "HUMAUX_GATEWAY_ALLOWED_ORIGINS" => "https://mcp.example.test".into(),
                "HUMAUX_GATEWAY_MAX_REQUEST_BODY_BYTES" => "65536".into(),
                "HUMAUX_GATEWAY_PG_DSN" => "postgres://redacted".into(),
                "HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX" => "00112233".into(),
                "HUMAUX_GATEWAY_TRUSTED_PROXY_CIDRS"
                | "HUMAUX_GATEWAY_GLOBAL_DENYLIST"
                | "HUMAUX_GATEWAY_GLOBAL_EMERGENCY_ALLOWLIST" => String::new(),
                "HUMAUX_GATEWAY_MAX_FORWARDED_HOPS" => "1".into(),
                "HUMAUX_GATEWAY_RESERVATION_TTL_SECONDS" => "30".into(),
                "HUMAUX_GATEWAY_HANDLER_TIMEOUT_SECONDS" => "5".into(),
                "HUMAUX_GATEWAY_FINALIZE_TIMEOUT_SECONDS" => "2".into(),
                "HUMAUX_GATEWAY_REPLAY_TTL_SECONDS" => "60".into(),
                "HUMAUX_GATEWAY_REMEMBER_TENANT_ID" => tenant.to_string(),
                "HUMAUX_GATEWAY_REMEMBER_WORKSPACE_ID" => workspace.to_string(),
                "HUMAUX_GATEWAY_REMEMBER_SCOPE_KIND" => "workspace".into(),
                "HUMAUX_GATEWAY_REMEMBER_DOMAIN" => "knowledge".into(),
                "HUMAUX_GATEWAY_REMEMBER_PROJECTION_KIND" => "ingest".into(),
                "HUMAUX_GATEWAY_REMEMBER_PROJECTION_VERSION" => "v1".into(),
                "HUMAUX_GATEWAY_REMEMBER_REASONING_DOMAIN_ID" => reasoning.to_string(),
                "HUMAUX_GATEWAY_REMEMBER_TOKEN_TTL_SECONDS" => "60".into(),
                "HUMAUX_GATEWAY_REMEMBER_DATA_CLASS" => "INTERNAL".into(),
                "HUMAUX_GATEWAY_REMEMBER_VISIBILITY_CLASS" => "WORKSPACE_SHARED".into(),
                "HUMAUX_GATEWAY_REMEMBER_EVENT_KIND" => "USER_MESSAGE".into(),
                "HUMAUX_GATEWAY_CONTEXT_TOTAL_TOKENS" => "2048".into(),
                "HUMAUX_GATEWAY_CONTEXT_MANDATORY_TOKENS" => "1024".into(),
                key if key.contains("_CAPACITY") || key.contains("_REFILL_PER_SECOND") => {
                    "100".into()
                }
                key => entry
                    .default
                    .clone()
                    .unwrap_or_else(|| panic!("unhandled gateway config key {key}")),
            };
            raw.insert(entry.name, value);
        }
        raw
    }

    #[test]
    fn bootstrap_rejects_unknown_missing_and_unsafe_listener_values() {
        let mut values = raw();
        values.insert("HUMAUX_GATEWAY_UNKNOWN".into(), "x".into());
        assert!(GatewayBootstrap::from_raw(values).is_err());

        let mut values = raw();
        values.remove("HUMAUX_GATEWAY_PG_DSN");
        assert!(GatewayBootstrap::from_raw(values).is_err());

        let mut values = raw();
        values.insert("HUMAUX_GATEWAY_BIND_ADDR".into(), "0.0.0.0:8080".into());
        assert!(GatewayBootstrap::from_raw(values).is_err());
    }

    #[test]
    fn fingerprint_changes_for_public_values_but_not_secret_values() {
        let values = raw();
        let first = GatewayBootstrap::from_raw(values.clone()).expect("valid fixture config");

        let mut changed_secrets = values.clone();
        changed_secrets.insert("HUMAUX_GATEWAY_PG_DSN".into(), "postgres://other".into());
        changed_secrets.insert(
            "HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX".into(),
            "aabbccdd".into(),
        );
        let secret_changed =
            GatewayBootstrap::from_raw(changed_secrets).expect("valid secret-changed config");
        assert_eq!(first.config_fingerprint, secret_changed.config_fingerprint);

        let mut changed = values;
        changed.insert("HUMAUX_GATEWAY_CONTEXT_TOTAL_TOKENS".into(), "4096".into());
        let second = GatewayBootstrap::from_raw(changed).expect("valid changed config");
        assert_ne!(first.config_fingerprint, second.config_fingerprint);
    }

    #[test]
    fn context_profile_uses_registered_validation_and_fingerprint() {
        let baseline = raw();
        let first =
            GatewayBootstrap::from_raw(baseline.clone()).expect("registered default profile");
        for (key, value) in [
            ("HUMAUX_GATEWAY_RETRIEVAL_PROFILE_TOP_K", "0"),
            (
                "HUMAUX_GATEWAY_RETRIEVAL_PROFILE_PRODUCTION_ENABLED",
                "false",
            ),
            (
                "HUMAUX_GATEWAY_RETRIEVAL_PROFILE_QUERY_TRANSFORM",
                "client-prompt",
            ),
        ] {
            let mut values = baseline.clone();
            values.insert(key.into(), value.into());
            assert!(
                GatewayBootstrap::from_raw(values).is_err(),
                "{key} must use the registered gate"
            );
        }
        let mut values = baseline;
        values.insert("HUMAUX_GATEWAY_RETRIEVAL_PROFILE_TOP_K".into(), "7".into());
        let changed = GatewayBootstrap::from_raw(values).expect("registered profile override");
        assert_ne!(first.config_fingerprint, changed.config_fingerprint);
    }
}
