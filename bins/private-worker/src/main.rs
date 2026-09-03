//! `humaux-private-worker` process entry (§4.2 minimal process set; §4.4 admin probe
//! contract; §11/§11.1 T4.4+T4.5; §11.8 ADR-0015 inference RPC).
//!
//! §11.1: "仅 humaux-private-worker 在最贴近 adapter 处解密" — this is the one process in the
//! workspace permitted to hold both DB write capability (`PrivateWorkerDbPool`,
//! `role_private_worker`) and BYOK decrypt capability at once (contrast
//! `humaux-consolidation-worker`, whose own Cargo.toml documents *not* having a path to
//! either `humaux_adapters::byok`'s decrypt trait or an OpenBao client, §11.8 — that binary
//! must go through this one over a UDS for any USER_REASONING call).
//!
//! Modes (same `required`/`parse` env scaffold every other `bins/*/src/main.rs` uses — no
//! config-loading infrastructure exists yet):
//! * no flag / `--probe-connection`: the original Phase 4 probe (typed role connection only).
//! * `--serve-rpc`: the §11.8 private inference listener
//!   ([`humaux_private_worker::inference_rpc`]) the consolidation worker's
//!   `HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH` dials — the same `bind_socket` + `serve`
//!   pair `bins/consolidation-worker/tests/consolidation_hop_e2e.rs` proves in-process, so
//!   the tests cover the accept loop production runs. Provider identity/endpoint/capabilities
//!   are configuration (§78.1: no literal model, endpoint, or dimension in code).

use std::env;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use humaux_adapters::byok::{
    CredentialDecryptor, CredentialRef, EgressHttpTransport, OpenAiCompatibleProvider,
    PlaintextApiKey, ReasoningCapability, ReasoningProviderDescriptor, ReasoningProviderError,
    ssrf,
};
use humaux_adapters::consolidation_reasoner::consolidation_prompt_contract;
use humaux_adapters::contribution_reasoner::ContributionReasonerConfig;
use humaux_adapters::disclosure::DeletionCapability;
use humaux_adapters::postgres::PrivateWorkerDbPool;
use humaux_domain::egress::ProcessorId;
use humaux_private_worker::inference_rpc::{RpcState, bind_socket, serve};
use uuid::Uuid;

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("missing required configuration: {name}"))
}

fn parse<T: std::str::FromStr>(name: &str) -> Result<T, String> {
    required(name)?
        .parse()
        .map_err(|_| format!("invalid configuration: {name}"))
}

fn usage() -> &'static str {
    "usage: humaux-private-worker (--probe-connection | --serve-rpc)"
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        None | Some("--probe-connection") => {
            probe_connection().await;
            ExitCode::SUCCESS
        }
        Some("--serve-rpc") => match serve_rpc().await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("humaux-private-worker: {error}");
                ExitCode::from(2)
            }
        },
        Some(_) => {
            eprintln!("humaux-private-worker: {}", usage());
            ExitCode::from(2)
        }
    }
}

/// Original Phase 4 scaffold probe, preserved as the argument-less default so an existing
/// deploy invocation with no flags keeps behaving exactly as before.
async fn probe_connection() {
    match env::var("PRIVATE_WORKER_PG_DSN") {
        Err(_) => {
            println!(
                "humaux-private-worker: PRIVATE_WORKER_PG_DSN not set, not wired yet (Phase 4 scaffold)"
            );
        }
        Ok(dsn) => match PrivateWorkerDbPool::connect(&dsn).await {
            Ok(_pool) => println!("humaux-private-worker: connected as role_private_worker"),
            Err(e) => eprintln!("humaux-private-worker: {e}"),
        },
    }
}

/// The one BYOK credential this process holds (§4.2 one process, one credential). Read from
/// `HUMAUX_PRIVATE_WORKER_PROVIDER_API_KEY` once at startup, handed out only as a
/// [`PlaintextApiKey`] (whose `Debug`/`Display` print a fingerprint, never the value) and read
/// only inside `OpenAiCompatibleProvider::build_openai_request` — the §11.1 "closest to the
/// adapter" point. Nothing in this file logs or formats it.
// ponytail: env-held key until `adapters::openbao` stops being a placeholder; the OpenBao
// decryptor replaces this impl and nothing else here changes. Written out as the desugared
// `async_trait` signature because `async-trait` is a dev-only dependency of this binary.
struct EnvCredential(String);

impl CredentialDecryptor for EnvCredential {
    fn resolve<'life0, 'async_trait>(
        &'life0 self,
        _credential_ref: CredentialRef,
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
        Box::pin(std::future::ready(Ok(PlaintextApiKey::new(self.0.clone()))))
    }
}

/// ADR-0015 `--serve-rpc`: the Unix-domain-socket private inference listener.
async fn serve_rpc() -> Result<(), String> {
    let dsn = required("PRIVATE_WORKER_PG_DSN")?;
    let socket_path = required("HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH")?;
    let consolidation_uid = parse::<u32>("HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID")?;

    let chat_url = required("HUMAUX_PRIVATE_WORKER_CHAT_URL")?;
    let descriptor = ReasoningProviderDescriptor {
        provider_id: required("HUMAUX_PRIVATE_WORKER_PROVIDER_ID")?,
        model_id: required("HUMAUX_PRIVATE_WORKER_MODEL_ID")?,
        model_revision: env::var("HUMAUX_PRIVATE_WORKER_MODEL_REVISION")
            .ok()
            .filter(|v| !v.is_empty()),
        // Not deployment config: this listener's only provider call is `complete_structured`,
        // and `UserReasoningProvider` refuses that before any network round trip unless the
        // descriptor declares the capability (§11.3) — a deployment that cannot promise it has
        // nothing this RPC can serve.
        capabilities: vec![ReasoningCapability::StructuredOutput],
        custom_endpoint: Some(chat_url.clone()),
    };
    let http_timeout =
        Duration::from_secs(parse::<u64>("HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS")?);
    // The NAME of the variable holding the key is configurable; the key itself is read once,
    // here, and only ever leaves this scope inside `EnvCredential`.
    let key_env = env::var("HUMAUX_PRIVATE_WORKER_KEY_ENV")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "MINIMAX_API_KEY".to_owned());
    let key = env::var(&key_env).map_err(|_| {
        format!("missing required configuration: {key_env} (HUMAUX_PRIVATE_WORKER_KEY_ENV)")
    })?;
    // §11.4 static DNS pins (optional): `host=ip[|ip],...` — for hosts whose system DNS answer
    // is not trustworthy on this node. The forbidden-range check still runs on the pins.
    let resolver: Box<dyn ssrf::DnsResolver> = match env::var("HUMAUX_PRIVATE_WORKER_DNS_PINS") {
        Ok(spec) if !spec.trim().is_empty() => {
            Box::new(ssrf::PinnedDnsResolver::parse(&spec).map_err(|e| {
                format!("invalid configuration: HUMAUX_PRIVATE_WORKER_DNS_PINS ({e:?})")
            })?)
        }
        _ => Box::new(ssrf::SystemDnsResolver),
    };
    let provider = OpenAiCompatibleProvider::new(
        descriptor,
        chat_url,
        EgressHttpTransport::new(http_timeout)
            .map_err(|_| "egress transport construction failed".to_owned())?,
        EnvCredential(key),
        ssrf::CustomEndpointPolicy::default(),
        resolver.as_ref(),
    )
    .map_err(|_| {
        "invalid configuration: HUMAUX_PRIVATE_WORKER_CHAT_URL rejected by the §11.4 SSRF choke point".to_owned()
    })?;

    // The Consolidate path takes prompt/schema/budget from the shared contract itself
    // (`ConsolidationReasoner`, ADR-0015 D2); these three fields only have to satisfy
    // `ContributionReasonerConfig::validate` for the config to be accepted at all.
    let contract = consolidation_prompt_contract();
    let config = ContributionReasonerConfig {
        allowed_egress_processor_id: ProcessorId(parse::<Uuid>(
            "HUMAUX_PRIVATE_WORKER_EGRESS_PROCESSOR_ID",
        )?),
        region: required("HUMAUX_PRIVATE_WORKER_REGION")?,
        permit_ttl: Duration::from_secs(parse::<u64>("HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS")?),
        // ponytail: no deployment has told us the endpoint's deletion semantics yet; make it
        // env-driven when a provider that does promise deletion is onboarded.
        deletion_capability: DeletionCapability::Unknown,
        system_prompt: contract.system_prompt.to_owned(),
        json_schema: contract.json_schema.to_owned(),
        max_output_tokens: contract.max_output_tokens,
    };
    config
        .validate()
        .map_err(|code| format!("invalid configuration: {code:?}"))?;

    let calls = PrivateWorkerDbPool::connect(&dsn)
        .await
        .map_err(|e| format!("private worker database role connection failed: {e}"))?;
    let state = Arc::new(RpcState {
        expected_consolidation_uid: consolidation_uid,
        calls,
        config,
        provider: Box::new(provider),
    });

    let listener = bind_socket(std::path::Path::new(&socket_path)).map_err(|error| {
        format!("failed to bind private inference RPC socket {socket_path}: {error}")
    })?;
    eprintln!("humaux-private-worker RPC listening on {socket_path}");
    serve(listener, state)
        .await
        .map_err(|error| format!("private inference RPC server failed: {error}"))
}
