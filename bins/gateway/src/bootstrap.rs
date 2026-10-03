//! `gateway::bootstrap` — Process bootstrap: the process-wide stream family `(scope_kind, domain, projection_kind,
//!   projection_version)` that the read routes AND `remember.put` attach to each request's own `(tenant, workspace)`
//!   (ADR-0031 D-A / ADR-0032 D-A, §34.0.1 Q9: one process serves every pair; no registry table, no per-process
//!   stream cache) — the same family the 14 governance / subject / affect writes attach to since ADR-0054, so
//!   the process holds no default write pair at all.
//! Depends-on: crates=[hex, humaux-adapters, humaux-application, humaux-contracts, humaux-domain, humaux-infra-cell,
//!   humaux-protocol, tokio,
//!   uuid]; services=[PostgreSQL(role_gateway)]; env=[HUMAUX_GATEWAY_ALLOWED_HOSTS, HUMAUX_GATEWAY_ALLOWED_ORIGINS,
//!   HUMAUX_GATEWAY_BIND_ADDR, HUMAUX_GATEWAY_CALLER_ID, HUMAUX_GATEWAY_CELL_ID,
//!   HUMAUX_GATEWAY_CONFIRM_TOKEN_TTL_SECONDS, HUMAUX_GATEWAY_CONTEXT_MANDATORY_TOKENS,
//!   HUMAUX_GATEWAY_CONTEXT_TOTAL_TOKENS, HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX,
//!   HUMAUX_GATEWAY_CREDENTIAL_PEPPER_PREVIOUS_HEX, HUMAUX_GATEWAY_EMBEDDING_DIMENSION,
//!   HUMAUX_GATEWAY_EMBEDDING_VERSION, HUMAUX_GATEWAY_FINALIZE_TIMEOUT_SECONDS, HUMAUX_GATEWAY_GLOBAL_DENYLIST,
//!   HUMAUX_GATEWAY_GLOBAL_EMERGENCY_ALLOWLIST, HUMAUX_GATEWAY_HANDLER_TIMEOUT_SECONDS,
//!   HUMAUX_GATEWAY_MAX_FORWARDED_HOPS, HUMAUX_GATEWAY_MAX_REQUEST_BODY_BYTES,
//!   HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS, HUMAUX_GATEWAY_PG_DSN, HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS,
//!   HUMAUX_GATEWAY_QDRANT_CIDR,
//!   HUMAUX_GATEWAY_QDRANT_HOST, HUMAUX_GATEWAY_QDRANT_PORT, HUMAUX_GATEWAY_QDRANT_TLS,
//!   HUMAUX_GATEWAY_REMEMBER_DATA_CLASS, HUMAUX_GATEWAY_REMEMBER_DOMAIN, HUMAUX_GATEWAY_REMEMBER_EVENT_KIND,
//!   HUMAUX_GATEWAY_REMEMBER_PROJECTION_KIND, HUMAUX_GATEWAY_REMEMBER_PROJECTION_VERSION,
//!   HUMAUX_GATEWAY_REMEMBER_REASONING_DOMAIN_ID, HUMAUX_GATEWAY_REMEMBER_SCOPE_KIND,
//!   HUMAUX_GATEWAY_REMEMBER_TENANT_ID, HUMAUX_GATEWAY_REMEMBER_TOKEN_TTL_SECONDS,
//!   HUMAUX_GATEWAY_REMEMBER_VISIBILITY_CLASS, HUMAUX_GATEWAY_REMEMBER_WORKSPACE_ID,
//!   HUMAUX_GATEWAY_REPLAY_TTL_SECONDS, HUMAUX_GATEWAY_RESERVATION_TTL_SECONDS,
//!   HUMAUX_GATEWAY_RETRIEVAL_PROFILE_PRODUCTION_ENABLED, HUMAUX_GATEWAY_RETRIEVAL_PROFILE_QUERY_TRANSFORM,
//!   HUMAUX_GATEWAY_RETRIEVAL_PROFILE_TOP_K, HUMAUX_GATEWAY_RETRIEVAL_RPC_PERMIT_TTL_SECONDS,
//!   HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH, HUMAUX_GATEWAY_TOKEN_HMAC_KEY, HUMAUX_GATEWAY_TOKEN_HMAC_KEY_PREVIOUS,
//!   HUMAUX_GATEWAY_TRUSTED_PROXY_CIDRS,
//!   HUMAUX_GATEWAY_UNDO_WINDOW_SECONDS, HUMAUX_GATEWAY_UNKNOWN]; modules=[adapters::postgres, adapters::quota_repo,
//!   adapters::retrieve,
//!   application::retrieval_embedding_port, contracts::config_registry, contracts::retrieval_config,
//!   domain::context, domain::dataclass, domain::identity, gateway::context, gateway::guard,
//!   gateway::mcp_application, gateway::recall, gateway::remember, gateway::retrieval_embedding_client,
//!   infra-cell::permit, infra-cell::resource, infra-cell::transport, protocol::edge, protocol::mcp, protocol::mcp_catalog]
//! Called-by: [gateway::main]
//! Invariants: [one process serves every (tenant, workspace) pair with no per-process stream cache or registry table; GuardSettings::tenant_network stays empty until a separate authorization approves a tenant-specific network policy]
//! Spec: Baseline §34.0.1; §78.1; ADR-0031; ADR-0032; ADR-0054
//!
//! This module owns process configuration only. Tool arguments never select a
//! stream version, credential verifier, listener, or rate policy; a tool argument selects a
//! workspace only inside the credential's already-authorized membership (`narrow`).
//! `GuardSettings::tenant_network` is deliberately empty in this single-policy
//! process. A tenant-specific network-policy loader needs a separate approved
//! authorization and acceptance gate before this bootstrap can serve it.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use humaux_adapters::{
    postgres::RuntimeDbPool,
    quota_repo::RatePolicy,
    retrieve::{TokenKeys, install_token_keys},
};
use humaux_application::retrieval_embedding_port::RetrievalEmbeddingPort;
use humaux_contracts::config_registry::{
    ConfigEntry, effective_config_fingerprint, resolve_effective_config,
};
use humaux_contracts::retrieval_config::{
    resolve_registered_retrieval_profile, retrieval_profile_registry,
};
use humaux_domain::{context::ContextBudget, dataclass::DataClass, identity::VisibilityClass};
use humaux_infra_cell::{
    CallerId, CellAccessMode, CellCidr, CellId, DEFAULT_MAX_RESPONSE_BYTES, HttpIntraCellTransport,
    IntraCellResource, IntraCellResourceRegistry, ResourceEntry,
};
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
    recall::{SemanticRecallRuntime, SemanticRecallVersions},
    remember::{self, ProcessFamily, RememberEventKind, RememberPolicy},
    retrieval_embedding_client::GatewayRetrievalEmbeddingClient,
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
    semantic_recall: Option<SemanticRecallConfig>,
    /// Copied out of `guard` before it moves into [`GatewayGuard::new`] (`build`) — the one
    /// piece of guard config the semantic-recall wiring also needs, for the Qdrant transport's
    /// own request timeout (never a literal, §78.1).
    handler_timeout: Duration,
    /// §33.10 rule 9 confirm-token lifetime (ADR-0018), `HUMAUX_GATEWAY_CONFIRM_TOKEN_TTL_SECONDS`.
    confirm_token_ttl: Duration,
    /// §78.1 memory.restore undo window (ADR-0020), `HUMAUX_GATEWAY_UNDO_WINDOW_SECONDS`.
    undo_window: Duration,
    /// §8.5.1 / ADR-0030 D-B frozen mood half-life policy (§78.1, no literal),
    /// `HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS`.
    mood_half_life: Duration,
    /// §15.5 / ADR-0059 D-G consistency-token MAC keys, installed process-wide in [`Self::build`].
    token_keys: TokenKeys,
}

/// Parsed `HUMAUX_GATEWAY_RETRIEVAL_RPC_*` / `HUMAUX_GATEWAY_EMBEDDING_*` /
/// `HUMAUX_GATEWAY_QDRANT_*` / `HUMAUX_GATEWAY_CELL_ID` / `HUMAUX_GATEWAY_CALLER_ID`
/// configuration — present only when
/// `HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH` is non-empty (`parse_semantic_recall`'s doc).
struct SemanticRecallConfig {
    socket_path: String,
    permit_ttl: Duration,
    embedding_dimension: u32,
    embedding_version: String,
    qdrant_host: String,
    qdrant_port: u16,
    qdrant_cidr: CellCidr,
    qdrant_tls: bool,
    cell_id: CellId,
    caller_id: CallerId,
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
        // ADR-0059 D-G: the one production install, before the pool, the guard and the listener.
        install_token_keys(self.token_keys).map_err(|_| {
            BootstrapError::new(
                "HUMAUX_GATEWAY_TOKEN_HMAC_KEY",
                "a different key set is already installed",
            )
        })?;
        // dep: PostgreSQL(role_gateway) — opens the role_gateway pool the rest of bootstrap wires into the app state
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
        let mut application = GatewayMcpApplication::new(
            catalog,
            guard.clone(),
            self.remember_policy,
            self.remember_event_kind,
            self.context_bootstrap,
        )
        .with_confirm_token_ttl(self.confirm_token_ttl)
        .map_err(|_| {
            BootstrapError::new(
                "HUMAUX_GATEWAY_CONFIRM_TOKEN_TTL_SECONDS",
                "must be positive seconds",
            )
        })?
        .with_undo_window(self.undo_window)
        .map_err(|_| {
            BootstrapError::new(
                "HUMAUX_GATEWAY_UNDO_WINDOW_SECONDS",
                "must be positive seconds",
            )
        })?
        .with_mood_half_life(self.mood_half_life)
        .map_err(|_| {
            BootstrapError::new(
                "HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS",
                "must be positive seconds",
            )
        })?;
        match self.semantic_recall {
            Some(config) => {
                let runtime = build_semantic_recall_runtime(
                    guard.runtime_pool(),
                    config,
                    self.handler_timeout,
                )?;
                application = application.with_semantic_recall(runtime);
            }
            None => {
                // §57.1: not_applicable prints the missing object's name, not a silent skip.
                eprintln!(
                    "gateway bootstrap: not_applicable: HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH \
                     (semantic recall stays disabled, recall.search keeps returning DependencyUnavailable)"
                );
            }
        }
        let application = Arc::new(application);
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
        let guard = parse_guard(&effective)?;
        let handler_timeout = guard.handler_timeout;
        let confirm_token_ttl = seconds(
            required(&effective, "HUMAUX_GATEWAY_CONFIRM_TOKEN_TTL_SECONDS")?,
            "HUMAUX_GATEWAY_CONFIRM_TOKEN_TTL_SECONDS",
        )?;
        let undo_window = seconds(
            required(&effective, "HUMAUX_GATEWAY_UNDO_WINDOW_SECONDS")?,
            "HUMAUX_GATEWAY_UNDO_WINDOW_SECONDS",
        )?;
        let mood_half_life = seconds(
            required(&effective, "HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS")?,
            "HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS",
        )?;
        let token_keys = parse_token_keys(&effective)?;

        Ok(Self {
            bind_addr: parse_bind_addr(required(&effective, "HUMAUX_GATEWAY_BIND_ADDR")?)?,
            http: parse_http(&effective)?,
            pg_dsn: required(&effective, "HUMAUX_GATEWAY_PG_DSN")?.to_owned(),
            guard,
            remember_policy,
            remember_event_kind: parse_remember_event_kind(&effective)?,
            context_bootstrap,
            config_fingerprint: redacted_fingerprint(&registry, &effective),
            semantic_recall: parse_semantic_recall(&effective)?,
            handler_timeout,
            confirm_token_ttl,
            undo_window,
            mood_half_life,
            token_keys,
        })
    }
}

/// ADR-0059 D-G: `HUMAUX_GATEWAY_TOKEN_HMAC_KEY` (required, no default) and the optional rotation
/// twin `…_PREVIOUS` (empty = window closed). Errors name the key, never a value.
fn parse_token_keys(effective: &BTreeMap<String, String>) -> Result<TokenKeys, BootstrapError> {
    const CURRENT: &str = "HUMAUX_GATEWAY_TOKEN_HMAC_KEY";
    const PREVIOUS: &str = "HUMAUX_GATEWAY_TOKEN_HMAC_KEY_PREVIOUS";
    let current = hex::decode(required(effective, CURRENT)?)
        .map_err(|_| BootstrapError::new(CURRENT, "invalid hex"))?;
    let previous = optional_hex(effective, PREVIOUS)?;
    // TokenKeys::new names the failing key ("current …" / "previous …") without its value.
    TokenKeys::new(current, previous).map_err(|why| {
        BootstrapError::new(
            if why.starts_with("current") {
                CURRENT
            } else {
                PREVIOUS
            },
            why,
        )
    })
}

/// An optional secret hex key registered with default `""`: empty = absent; otherwise valid hex.
fn optional_hex(
    effective: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<Vec<u8>>, BootstrapError> {
    let value = present(effective, key)?;
    if value.trim().is_empty() {
        return Ok(None);
    }
    hex::decode(value)
        .map(Some)
        .map_err(|_| BootstrapError::new(key, "invalid hex"))
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
    // ADR-0059 D-H: the rotation window is open exactly while this key is set (runbook Rotate).
    let pepper_previous = optional_hex(effective, "HUMAUX_GATEWAY_CREDENTIAL_PEPPER_PREVIOUS_HEX")?;
    if pepper_previous.as_ref() == Some(&pepper) {
        return Err(BootstrapError::new(
            "HUMAUX_GATEWAY_CREDENTIAL_PEPPER_PREVIOUS_HEX",
            "must differ from the current pepper",
        ));
    }
    let guard = GuardSettings {
        credential_pepper: pepper,
        credential_pepper_previous: pepper_previous,
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
    // ADR-0054 D-D: the former default write pair is optional and unused. A present value must
    // still be a UUID (fail closed on garbage); it only earns one startup line. The keys stay
    // registered because `xtask e2e-onboard` still passes them.
    for key in [
        "HUMAUX_GATEWAY_REMEMBER_TENANT_ID",
        "HUMAUX_GATEWAY_REMEMBER_WORKSPACE_ID",
    ] {
        let value = present(effective, key)?;
        if !value.trim().is_empty() {
            uuid(value, key)?;
            eprintln!(
                "gateway bootstrap: {key} ignored since ADR-0054 (writes derive (tenant, workspace) per request)"
            );
        }
    }
    let scope_kind = required(effective, "HUMAUX_GATEWAY_REMEMBER_SCOPE_KIND")?;
    if scope_kind != "workspace" {
        return Err(BootstrapError::new(
            "HUMAUX_GATEWAY_REMEMBER_SCOPE_KIND",
            "only workspace is enabled",
        ));
    }
    let family = ProcessFamily::new(
        scope_kind,
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
    )
    .map_err(|_| BootstrapError::new("HUMAUX_GATEWAY_REMEMBER_*", "invalid stream family"))?;
    RememberPolicy::new(
        family,
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
    let projection_lag = seconds(
        required(effective, "HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS")?,
        "HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS",
    )?;
    ContextBootstrap::new(budget, profile, write_policy, projection_lag)
        .map_err(|_| BootstrapError::new("gateway binary", "provenance unavailable"))
}

/// `HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH` gates the whole feature: empty (not merely
/// "unset" — see [`registry`]'s doc on why every declared key must literally be present, even
/// blank) means semantic recall stays disabled and every other `HUMAUX_GATEWAY_{RETRIEVAL_RPC,
/// EMBEDDING,QDRANT,CELL_ID,CALLER_ID}_*` key may itself be blank. A non-empty socket
/// path requires all of them filled in.
fn parse_semantic_recall(
    effective: &BTreeMap<String, String>,
) -> Result<Option<SemanticRecallConfig>, BootstrapError> {
    let socket_path = present(effective, "HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH")?;
    if socket_path.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(SemanticRecallConfig {
        socket_path: socket_path.to_owned(),
        permit_ttl: seconds(
            required(effective, "HUMAUX_GATEWAY_RETRIEVAL_RPC_PERMIT_TTL_SECONDS")?,
            "HUMAUX_GATEWAY_RETRIEVAL_RPC_PERMIT_TTL_SECONDS",
        )?,
        embedding_dimension: parse_u32(
            required(effective, "HUMAUX_GATEWAY_EMBEDDING_DIMENSION")?,
            "HUMAUX_GATEWAY_EMBEDDING_DIMENSION",
        )?,
        embedding_version: nonempty(
            required(effective, "HUMAUX_GATEWAY_EMBEDDING_VERSION")?,
            "HUMAUX_GATEWAY_EMBEDDING_VERSION",
        )?
        .to_owned(),
        qdrant_host: nonempty(
            required(effective, "HUMAUX_GATEWAY_QDRANT_HOST")?,
            "HUMAUX_GATEWAY_QDRANT_HOST",
        )?
        .to_owned(),
        qdrant_port: {
            let port = parse_u32(
                required(effective, "HUMAUX_GATEWAY_QDRANT_PORT")?,
                "HUMAUX_GATEWAY_QDRANT_PORT",
            )?;
            u16::try_from(port)
                .map_err(|_| BootstrapError::new("HUMAUX_GATEWAY_QDRANT_PORT", "must fit u16"))?
        },
        qdrant_cidr: required(effective, "HUMAUX_GATEWAY_QDRANT_CIDR")?
            .parse()
            .map_err(|_| BootstrapError::new("HUMAUX_GATEWAY_QDRANT_CIDR", "invalid CIDR"))?,
        qdrant_tls: match required(effective, "HUMAUX_GATEWAY_QDRANT_TLS")? {
            "true" => true,
            "false" => false,
            _ => {
                return Err(BootstrapError::new(
                    "HUMAUX_GATEWAY_QDRANT_TLS",
                    "must be true or false",
                ));
            }
        },
        cell_id: CellId(uuid(
            required(effective, "HUMAUX_GATEWAY_CELL_ID")?,
            "HUMAUX_GATEWAY_CELL_ID",
        )?),
        caller_id: CallerId(
            nonempty(
                required(effective, "HUMAUX_GATEWAY_CALLER_ID")?,
                "HUMAUX_GATEWAY_CALLER_ID",
            )?
            .to_owned(),
        ),
    }))
}

/// Builds the one native semantic-recall lane's runtime — `IntraCellResource::QDRANT_REST`
/// registered [`CellAccessMode::QdrantReadOnly`] (2026-08-30 ruling), never `ReadWrite`;
/// `xtask architecture-check`'s ADR-0014 gate statically asserts this file never constructs
/// the latter. `IntraCellResource::RETRIEVAL_EMBEDDING_RPC` shares the same registry/Cell/
/// caller identity (ADR-0012 §决定3) even though its actual transport bypasses
/// `IntraCellHttpTransport` entirely (`GatewayRetrievalEmbeddingClient`'s own doc).
fn build_semantic_recall_runtime(
    pool: Arc<RuntimeDbPool>,
    config: SemanticRecallConfig,
    handler_timeout: Duration,
) -> Result<SemanticRecallRuntime, BootstrapError> {
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            config.qdrant_host,
            config.qdrant_port,
            config.cell_id,
            vec![config.qdrant_cidr],
            BTreeSet::from([config.caller_id.clone()]),
            config.qdrant_tls,
        )
        .map_err(|_| BootstrapError::new("HUMAUX_GATEWAY_QDRANT_*", "invalid Qdrant resource"))?
        .with_access_mode(CellAccessMode::QdrantReadOnly),
    );
    entries.insert(
        IntraCellResource::RETRIEVAL_EMBEDDING_RPC,
        ResourceEntry::new(
            "unix-socket",
            0,
            config.cell_id,
            vec![],
            BTreeSet::from([config.caller_id.clone()]),
            false,
        )
        .map_err(|_| {
            BootstrapError::new("gateway semantic recall", "invalid RPC resource entry")
        })?,
    );
    let registry = IntraCellResourceRegistry::new(entries, config.cell_id, config.caller_id);

    let qdrant_transport = Arc::new(
        HttpIntraCellTransport::new(
            registry.clone(),
            handler_timeout,
            DEFAULT_MAX_RESPONSE_BYTES,
        )
        .map_err(|_| {
            BootstrapError::new(
                "gateway semantic recall",
                "could not construct Qdrant transport",
            )
        })?,
    );
    let embedding_port: Arc<dyn RetrievalEmbeddingPort> =
        Arc::new(GatewayRetrievalEmbeddingClient::new(
            pool,
            config.socket_path,
            registry.clone(),
            config.permit_ttl,
        ));
    // ADR-0056 D-C: no scanner here — the retrieval worker's `seal_query` is the one query seal.
    SemanticRecallRuntime::new(
        embedding_port,
        qdrant_transport,
        registry,
        SemanticRecallVersions {
            embedding_version: config.embedding_version,
            dimension: config.embedding_dimension,
        },
        handler_timeout,
    )
    .map_err(|_| BootstrapError::new("gateway semantic recall", "invalid runtime configuration"))
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
        // ADR-0059 D-G: no default — boot-fatal when absent.
        ("TOKEN_HMAC_KEY", "hex", true),
        ("TRUSTED_PROXY_CIDRS", "csv-cidr", false),
        ("MAX_FORWARDED_HOPS", "usize", false),
        ("GLOBAL_DENYLIST", "csv-cidr", false),
        ("GLOBAL_EMERGENCY_ALLOWLIST", "csv-cidr", false),
        ("RESERVATION_TTL_SECONDS", "u64", false),
        ("HANDLER_TIMEOUT_SECONDS", "u64", false),
        ("FINALIZE_TIMEOUT_SECONDS", "u64", false),
        ("REPLAY_TTL_SECONDS", "u64", false),
        ("CONFIRM_TOKEN_TTL_SECONDS", "u64", false),
        ("UNDO_WINDOW_SECONDS", "u64", false),
        ("MOOD_HALF_LIFE_SECONDS", "u64", false),
        // §78.1: no default — boot-fatal when absent (ADR-0057 D-F, §22.4 projection lag).
        ("PROJECTION_LAG_SECONDS", "u64", false),
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
    // ADR-0054 D-D: the former default write pair. No write compares against it and no put
    // falls back to it any more — every route derives (tenant, workspace) per request — so both
    // are optional (default "") and ignored; a present value must still parse as a UUID. Kept
    // registered only because `xtask e2e-onboard` still passes them; the follow-up that drops
    // them there deletes these two entries (their presence then becomes a boot error).
    // ADR-0059 D-G / D-H rotation twins: empty (the default) = rotation window closed.
    entries.extend(
        ["TOKEN_HMAC_KEY_PREVIOUS", "CREDENTIAL_PEPPER_PREVIOUS_HEX"]
            .into_iter()
            .map(|suffix| entry_with_default(&format!("{PREFIX}{suffix}"), "hex", true, "")),
    );
    entries.extend(
        ["REMEMBER_TENANT_ID", "REMEMBER_WORKSPACE_ID"]
            .into_iter()
            .map(|suffix| entry_with_default(&format!("{PREFIX}{suffix}"), "uuid", false, "")),
    );
    // Semantic-recall wiring keys: gated as a group by `RETRIEVAL_RPC_SOCKET_PATH`
    // (`parse_semantic_recall`'s doc) — absent is a valid, expected deployment shape (semantic
    // recall stays disabled), so each gets `default: ""` rather than `None`. `None` would make
    // `resolve_effective_config` hard-fail startup on any deployment that hasn't turned the
    // feature on yet, instead of reaching the `not_applicable` degrade path in `build`.
    entries.extend(
        [
            ("RETRIEVAL_RPC_SOCKET_PATH", "path", false),
            ("RETRIEVAL_RPC_PERMIT_TTL_SECONDS", "u64", false),
            ("EMBEDDING_DIMENSION", "u32", false),
            ("EMBEDDING_VERSION", "string", false),
            ("QDRANT_HOST", "string", false),
            ("QDRANT_PORT", "u16", false),
            ("QDRANT_CIDR", "cidr", false),
            ("QDRANT_TLS", "bool", false),
            ("CELL_ID", "uuid", false),
            ("CALLER_ID", "string", false),
        ]
        .into_iter()
        .map(|(suffix, type_name, secret)| {
            entry_with_default(&format!("{PREFIX}{suffix}"), type_name, secret, "")
        }),
    );
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

fn entry_with_default(name: &str, type_name: &str, secret: bool, default: &str) -> ConfigEntry {
    ConfigEntry {
        default: Some(default.to_owned()),
        ..entry(name, type_name, secret)
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

// The three closed-set parsers are `remember`'s (ADR-0032 D-B: one parser each for the env
// default and the per-call argument, so the two spellings can never drift apart).
fn data_class(value: &str) -> Result<DataClass, BootstrapError> {
    remember::parse_data_class(value).map_err(|_| {
        BootstrapError::new(
            "HUMAUX_GATEWAY_REMEMBER_DATA_CLASS",
            "unknown closed enum value",
        )
    })
}

/// The process DEFAULT may not be `TENANT_SHARED`: that class is reachable per call only,
/// behind the OWNER/ADMIN membership gate (ADR-0032 D-B).
fn visibility(value: &str) -> Result<VisibilityClass, BootstrapError> {
    match remember::parse_visibility_class(value) {
        Ok(class) if class != VisibilityClass::TenantShared => Ok(class),
        _ => Err(BootstrapError::new(
            "HUMAUX_GATEWAY_REMEMBER_VISIBILITY_CLASS",
            "only USER_PRIVATE or WORKSPACE_SHARED is enabled",
        )),
    }
}

fn event_kind(value: &str) -> Result<RememberEventKind, BootstrapError> {
    RememberEventKind::parse(value).map_err(|_| {
        BootstrapError::new(
            "HUMAUX_GATEWAY_REMEMBER_EVENT_KIND",
            "unknown closed enum value",
        )
    })
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
                "HUMAUX_GATEWAY_TOKEN_HMAC_KEY" => generated_key_hex(),
                "HUMAUX_GATEWAY_TRUSTED_PROXY_CIDRS"
                | "HUMAUX_GATEWAY_GLOBAL_DENYLIST"
                | "HUMAUX_GATEWAY_GLOBAL_EMERGENCY_ALLOWLIST" => String::new(),
                "HUMAUX_GATEWAY_MAX_FORWARDED_HOPS" => "1".into(),
                "HUMAUX_GATEWAY_RESERVATION_TTL_SECONDS" => "30".into(),
                "HUMAUX_GATEWAY_HANDLER_TIMEOUT_SECONDS" => "5".into(),
                "HUMAUX_GATEWAY_FINALIZE_TIMEOUT_SECONDS" => "2".into(),
                "HUMAUX_GATEWAY_REPLAY_TTL_SECONDS" => "60".into(),
                "HUMAUX_GATEWAY_CONFIRM_TOKEN_TTL_SECONDS" => "300".into(),
                "HUMAUX_GATEWAY_UNDO_WINDOW_SECONDS" => "86400".into(),
                "HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS" => "21600".into(),
                "HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS" => "60".into(),
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
                // Semantic recall stays disabled in this fixture (empty socket path) — every
                // other key in this group may legitimately be blank when it is.
                "HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH"
                | "HUMAUX_GATEWAY_RETRIEVAL_RPC_PERMIT_TTL_SECONDS"
                | "HUMAUX_GATEWAY_EMBEDDING_DIMENSION"
                | "HUMAUX_GATEWAY_EMBEDDING_VERSION"
                | "HUMAUX_GATEWAY_QDRANT_HOST"
                | "HUMAUX_GATEWAY_QDRANT_PORT"
                | "HUMAUX_GATEWAY_QDRANT_CIDR"
                | "HUMAUX_GATEWAY_QDRANT_TLS"
                | "HUMAUX_GATEWAY_CELL_ID"
                | "HUMAUX_GATEWAY_CALLER_ID" => String::new(),
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

    /// A per-call generated 32-byte key, hex-encoded (never a literal key).
    fn generated_key_hex() -> String {
        format!("{}{}", Uuid::now_v7().simple(), Uuid::now_v7().simple())
    }

    fn rejected_key(values: BTreeMap<String, String>) -> String {
        match GatewayBootstrap::from_raw(values) {
            Ok(_) => panic!("configuration must be rejected"),
            Err(error) => error.key,
        }
    }

    #[test]
    fn bootstrap_without_token_hmac_key_fails_naming_the_key() {
        let mut values = raw();
        values.remove("HUMAUX_GATEWAY_TOKEN_HMAC_KEY");
        assert_eq!(rejected_key(values), "HUMAUX_GATEWAY_TOKEN_HMAC_KEY");
        let mut values = raw();
        values.insert("HUMAUX_GATEWAY_TOKEN_HMAC_KEY".into(), String::new());
        assert_eq!(rejected_key(values), "HUMAUX_GATEWAY_TOKEN_HMAC_KEY");
    }

    #[test]
    fn bootstrap_rejects_short_or_equal_previous_token_key() {
        let mut values = raw();
        values.insert("HUMAUX_GATEWAY_TOKEN_HMAC_KEY".into(), "00".repeat(31));
        assert_eq!(rejected_key(values), "HUMAUX_GATEWAY_TOKEN_HMAC_KEY");
        let mut values = raw();
        values.insert(
            "HUMAUX_GATEWAY_TOKEN_HMAC_KEY_PREVIOUS".into(),
            "00".repeat(31),
        );
        assert_eq!(
            rejected_key(values),
            "HUMAUX_GATEWAY_TOKEN_HMAC_KEY_PREVIOUS"
        );
        let mut values = raw();
        let current = values["HUMAUX_GATEWAY_TOKEN_HMAC_KEY"].clone();
        values.insert("HUMAUX_GATEWAY_TOKEN_HMAC_KEY_PREVIOUS".into(), current);
        assert_eq!(
            rejected_key(values),
            "HUMAUX_GATEWAY_TOKEN_HMAC_KEY_PREVIOUS"
        );
        let mut values = raw();
        values.insert(
            "HUMAUX_GATEWAY_TOKEN_HMAC_KEY_PREVIOUS".into(),
            generated_key_hex(),
        );
        assert!(GatewayBootstrap::from_raw(values).is_ok());
    }

    #[test]
    fn bootstrap_rejects_previous_pepper_equal_to_current() {
        let mut values = raw();
        let current = values["HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX"].clone();
        values.insert(
            "HUMAUX_GATEWAY_CREDENTIAL_PEPPER_PREVIOUS_HEX".into(),
            current,
        );
        assert_eq!(
            rejected_key(values),
            "HUMAUX_GATEWAY_CREDENTIAL_PEPPER_PREVIOUS_HEX"
        );
        let mut values = raw();
        values.insert(
            "HUMAUX_GATEWAY_CREDENTIAL_PEPPER_PREVIOUS_HEX".into(),
            "not hex".into(),
        );
        assert_eq!(
            rejected_key(values),
            "HUMAUX_GATEWAY_CREDENTIAL_PEPPER_PREVIOUS_HEX"
        );
        let mut values = raw();
        values.insert(
            "HUMAUX_GATEWAY_CREDENTIAL_PEPPER_PREVIOUS_HEX".into(),
            generated_key_hex(),
        );
        assert!(GatewayBootstrap::from_raw(values).is_ok());
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

    /// ADR-0057 D-F (test 35): `PROJECTION_LAG_SECONDS` has no default — boot without it fails
    /// naming the key, and zero is refused. Fault: give the registry entry a default.
    #[test]
    fn bootstrap_without_projection_lag_seconds_fails_naming_the_key() {
        let key = "HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS";
        assert!(
            registry()
                .iter()
                .any(|entry| entry.name == key && entry.default.is_none()),
            "{key} must be declared without a default"
        );
        let mut values = raw();
        values.remove(key);
        let error = GatewayBootstrap::from_raw(values)
            .err()
            .expect("boot must fail without the lag key");
        assert!(error.to_string().contains(key), "{error}");

        let mut values = raw();
        values.insert(key.into(), "0".into());
        let error = GatewayBootstrap::from_raw(values)
            .err()
            .expect("a zero lag threshold must be refused");
        assert!(error.to_string().contains(key), "{error}");

        assert!(GatewayBootstrap::from_raw(raw()).is_ok());
    }

    /// ADR-0054 D-D: the former default write pair is optional — the process boots without
    /// both keys (every write derives its pair per request) — but a present value must still be
    /// a UUID, so a typo fails closed instead of being silently ignored.
    #[test]
    fn bootstrap_starts_without_the_default_write_pair() {
        let mut values = raw();
        values.remove("HUMAUX_GATEWAY_REMEMBER_TENANT_ID");
        values.remove("HUMAUX_GATEWAY_REMEMBER_WORKSPACE_ID");
        assert!(GatewayBootstrap::from_raw(values).is_ok());

        let mut values = raw();
        values.insert("HUMAUX_GATEWAY_REMEMBER_TENANT_ID".into(), String::new());
        values.insert("HUMAUX_GATEWAY_REMEMBER_WORKSPACE_ID".into(), String::new());
        assert!(GatewayBootstrap::from_raw(values).is_ok());

        for key in [
            "HUMAUX_GATEWAY_REMEMBER_TENANT_ID",
            "HUMAUX_GATEWAY_REMEMBER_WORKSPACE_ID",
        ] {
            let mut values = raw();
            values.insert(key.into(), "not-a-uuid".into());
            assert!(GatewayBootstrap::from_raw(values).is_err(), "{key}");
        }
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

    fn semantic_recall_enabled_values() -> BTreeMap<String, String> {
        let mut values = raw();
        let cell_id = Uuid::now_v7().to_string();
        for (key, value) in [
            (
                "HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH",
                "/tmp/hgb-test.sock",
            ),
            ("HUMAUX_GATEWAY_RETRIEVAL_RPC_PERMIT_TTL_SECONDS", "30"),
            ("HUMAUX_GATEWAY_EMBEDDING_DIMENSION", "4"),
            ("HUMAUX_GATEWAY_EMBEDDING_VERSION", "embed-v1"),
            ("HUMAUX_GATEWAY_QDRANT_HOST", "127.0.0.1"),
            ("HUMAUX_GATEWAY_QDRANT_PORT", "6333"),
            ("HUMAUX_GATEWAY_QDRANT_CIDR", "127.0.0.1/32"),
            ("HUMAUX_GATEWAY_QDRANT_TLS", "false"),
            ("HUMAUX_GATEWAY_CALLER_ID", "gateway"),
        ] {
            values.insert(key.into(), value.into());
        }
        values.insert("HUMAUX_GATEWAY_CELL_ID".into(), cell_id);
        values
    }

    /// A non-empty `HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH` requires every other
    /// `HUMAUX_GATEWAY_{RETRIEVAL_RPC,EMBEDDING,QDRANT,CELL_ID,CALLER_ID}_*` key —
    /// dropping any one of them must fail closed, never silently disable the lane.
    #[test]
    fn semantic_recall_requires_every_field_once_the_socket_path_is_set() {
        let values = semantic_recall_enabled_values();
        GatewayBootstrap::from_raw(values.clone()).expect("fully configured semantic recall");
        for key in [
            "HUMAUX_GATEWAY_EMBEDDING_DIMENSION",
            "HUMAUX_GATEWAY_EMBEDDING_VERSION",
            "HUMAUX_GATEWAY_QDRANT_HOST",
            "HUMAUX_GATEWAY_QDRANT_CIDR",
            "HUMAUX_GATEWAY_CELL_ID",
            "HUMAUX_GATEWAY_CALLER_ID",
        ] {
            let mut broken = values.clone();
            broken.insert(key.into(), String::new());
            assert!(
                GatewayBootstrap::from_raw(broken).is_err(),
                "{key} must be required once semantic recall is enabled"
            );
        }
    }

    /// The real build path (`GatewayBootstrap::build`'s `build_semantic_recall_runtime`) wires
    /// a working `SemanticRecallRuntime` — proven by constructing it directly here with the
    /// same parser this bootstrap uses, against a real `role_gateway` pool (§79.2: no mock
    /// PostgreSQL), rather than spinning up the whole HTTP-listener stack `build()` needs.
    #[tokio::test]
    async fn semantic_recall_config_builds_a_runtime() {
        let effective = resolve_effective_config(&registry(), &semantic_recall_enabled_values())
            .expect("effective config");
        let config = parse_semantic_recall(&effective)
            .expect("valid semantic recall config")
            .expect("socket path is non-empty");
        let handler_timeout = parse_guard(&effective)
            .expect("valid guard")
            .handler_timeout;
        let pool = Arc::new(
            // dep: PostgreSQL(role_gateway) — opens a fresh pool for the drain/shutdown health check
            RuntimeDbPool::connect(
                &std::env::var("HUMAUX_GATEWAY_PG_DSN")
                    .expect("semantic recall bootstrap test requires HUMAUX_GATEWAY_PG_DSN"),
            )
            .await
            .expect("real role_gateway pool"),
        );
        build_semantic_recall_runtime(pool, config, handler_timeout)
            .expect("semantic recall runtime builds from valid config");
    }
}
