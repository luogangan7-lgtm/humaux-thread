//! `private-worker::main` — `humaux-private-worker` process entry (§4.2 minimal process set; §4.4 admin probe
//!   contract; §11/§11.1 T4.4+T4.5; §11.8 ADR-0015 inference RPC).
//! Depends-on: crates=[humaux-adapters, humaux-domain, tokio, uuid]; services=[PostgreSQL(role_private_worker)]; env=[HUMAUX_PRIVATE_WORKER_CAPABILITIES, HUMAUX_PRIVATE_WORKER_CHAT_URL, HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID, HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_MAX_CALLS, HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_IN_FLIGHT, HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS, HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS, HUMAUX_PRIVATE_WORKER_DNS_PINS, HUMAUX_PRIVATE_WORKER_EGRESS_PROCESSOR_ID, HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS, HUMAUX_PRIVATE_WORKER_KEY_ENV, HUMAUX_PRIVATE_WORKER_MODEL_ID, HUMAUX_PRIVATE_WORKER_MODEL_REVISION, HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS, HUMAUX_PRIVATE_WORKER_PROVIDER_ID, HUMAUX_PRIVATE_WORKER_REGION, HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH, PRIVATE_WORKER_PG_DSN]; modules=[adapters::byok, adapters::byok::ssrf, adapters::consolidation_reasoner, adapters::contribution_reasoner, adapters::disclosure, adapters::jobs, adapters::postgres, domain::authority, domain::egress, private-worker::distill, private-worker::inference_rpc]
//! Called-by: [process(humaux-private-worker)]
//! Invariants: [the only process holding both role_private_worker DB write and BYOK decrypt capability (§11.1); a
//!   missing/invalid env value or unreachable DSN exits non-zero before serving]
//! Spec: Baseline §11.1; §11.8; §78.1; ADR-0037; ADR-0036; ADR-0016; ADR-0058
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
//! * `--readyz`: card 15 / ADR-0037 probe-based readiness — see [`readyz`]. Tenant-free
//!   (ADR-0036) and provider-free: it deliberately makes NO inference call, because a readiness
//!   probe that burns a paid provider round trip is a probe nobody dares to poll.
//! * `--distill-once` / `--distill-serve` (ADR-0016, cross-tenant since ADR-0036, seats since
//!   ADR-0058): IN_FLIGHT seats drain the backlog once
//!   ([`humaux_private_worker::distill::dispatch_pass`]) / stay resident
//!   ([`humaux_private_worker::distill::dispatch_serve`]) — same provider/config bootstrap as
//!   `--serve-rpc` ([`bootstrap`]). There is no tenant id or reasoning domain in the environment:
//!   both come from each `DERIVED_DISTILL` job claimed through the owner SECURITY DEFINER
//!   `ops.claim_derived_work_v2` (migration 0190), and everything after the claim runs under that
//!   job's own tenant context.

use std::env;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use humaux_adapters::byok::{
    CredentialDecryptor, CredentialRef, EgressHttpTransport, OpenAiCompatibleProvider,
    PlaintextApiKey, ReasoningCapability, ReasoningProviderDescriptor, ReasoningProviderError,
    ssrf,
};
use humaux_adapters::consolidation_reasoner::consolidation_prompt_contract;
use humaux_adapters::contribution_reasoner::ContributionReasonerConfig;
use humaux_adapters::disclosure::DeletionCapability;
use humaux_adapters::jobs;
use humaux_adapters::postgres::PrivateWorkerDbPool;
use humaux_domain::authority::AuthorityClass;
use humaux_domain::egress::ProcessorId;
use humaux_private_worker::distill::{self, DistillDispatchConfig};
use humaux_private_worker::inference_rpc::{RpcState, bind_socket, clone_config, serve};
use uuid::Uuid;

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("missing required configuration: {name}"))
}

fn parse<T: std::str::FromStr>(name: &str) -> Result<T, String> {
    required(name)?
        .parse()
        .map_err(|_| format!("invalid configuration: {name}"))
}

/// A required comma list from the §11.2 capability closed set ([`ReasoningCapability::parse`]);
/// empty, unknown or repeated values are a configuration error.
fn capabilities(name: &str) -> Result<Vec<ReasoningCapability>, String> {
    let mut out = Vec::new();
    for raw in required(name)?.split(',') {
        match ReasoningCapability::parse(raw.trim()) {
            Some(c) if !out.contains(&c) => out.push(c),
            _ => return Err(format!("invalid configuration: {name}")),
        }
    }
    Ok(out)
}

fn usage() -> &'static str {
    "usage: humaux-private-worker (--probe-connection | --readyz | --serve-rpc | --distill-once | --distill-serve)"
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        None | Some("--probe-connection") => {
            probe_connection().await;
            ExitCode::SUCCESS
        }
        Some("--readyz") => match readyz().await {
            Ok(()) => ExitCode::SUCCESS,
            Err(missing) => {
                eprintln!("humaux-private-worker: not ready — missing object: {missing}");
                ExitCode::from(2)
            }
        },
        Some(mode @ ("--serve-rpc" | "--distill-once" | "--distill-serve")) => {
            let outcome = match mode {
                "--serve-rpc" => serve_rpc().await,
                "--distill-once" => distill_mode(false).await,
                _ => distill_mode(true).await,
            };
            match outcome {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("humaux-private-worker: {error}");
                    ExitCode::from(2)
                }
            }
        }
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
        // dep: PostgreSQL(role_private_worker) — role-scoped pool call
        Ok(dsn) => match PrivateWorkerDbPool::connect(&dsn).await {
            Ok(_pool) => println!("humaux-private-worker: connected as role_private_worker"),
            Err(e) => eprintln!("humaux-private-worker: {e}"),
        },
    }
}

/// `--readyz`: one live round trip to the ONE dependency this process cannot work without —
/// `role_private_worker` connects AND `current_user` matches (§6.2.3 assertion E). A dependency
/// that is down is NAMED, never collapsed into a bare non-zero exit (§4.4 坑5 applied to
/// readiness).
///
/// Deliberately not probed here, each for a reason readiness cannot argue away:
/// * the provider endpoint — a readiness poll must not spend a BYOK inference call, and §11.4's
///   SSRF choke point already refuses a bad endpoint at `bootstrap()`, i.e. at start, not here;
/// * this process's own RPC socket — it is the SERVER of that socket (`--serve-rpc` binds it),
///   so "can I connect to it" is a statement about the previous process generation, not this
///   one. The consolidation worker's `--readyz` is what asserts that peer is up, from the side
///   that actually dials it.
async fn readyz() -> Result<(), String> {
    let dsn = required("PRIVATE_WORKER_PG_DSN")?;
    // dep: PostgreSQL(role_private_worker) — role-scoped pool call
    PrivateWorkerDbPool::connect(&dsn)
        .await
        .map_err(|e| format!("PostgreSQL as role_private_worker ({e})"))?;
    println!("humaux-private-worker: ready db=role_private_worker");
    Ok(())
}

/// SIGTERM/Ctrl-C, as one awaitable. Both handlers are installed HERE, before the first pass —
/// which is the whole point of the type. `tokio::signal::ctrl_c()` cannot be used for the SIGINT
/// half: it is an `async fn` whose body registers the handler on its FIRST POLL, and the first
/// poll only happens inside [`Self::recv`], i.e. after a pass has already returned. A Ctrl-C
/// during that first pass would then hit SIGINT's default disposition and kill the process
/// mid-pass, leaving exactly the `ops.jobs` row `PROCESSING` with a live lease this card
/// forbids. Registering `SignalKind::interrupt()` eagerly, next to `terminate`, is what makes
/// the doc claim true for both signals.
struct Shutdown {
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
}

impl Shutdown {
    fn install() -> Result<Self, String> {
        Ok(Self {
            #[cfg(unix)]
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|e| format!("cannot install the SIGTERM handler: {e}"))?,
            #[cfg(unix)]
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .map_err(|e| format!("cannot install the SIGINT handler: {e}"))?,
        })
    }

    async fn recv(&mut self) {
        #[cfg(unix)]
        tokio::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
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

/// The provider + deployment config + `role_private_worker` pool every inference-bearing mode
/// of this binary shares (`--serve-rpc`, `--distill-once`, `--distill-serve`) — one bootstrap,
/// so the Distill hop and the RPC listener can never drift on which provider/endpoint/key they
/// hold (§4.2 one process, one credential).
struct Bootstrap {
    pool: PrivateWorkerDbPool,
    config: ContributionReasonerConfig,
    provider: OpenAiCompatibleProvider<EgressHttpTransport, EnvCredential>,
    /// The provider transport timeout; the distill dispatcher sizes `ops.begin_call`'s window
    /// from it (ADR-0058 D-K).
    http_timeout: Duration,
}

async fn bootstrap() -> Result<Bootstrap, String> {
    let dsn = required("PRIVATE_WORKER_PG_DSN")?;
    let chat_url = required("HUMAUX_PRIVATE_WORKER_CHAT_URL")?;
    let descriptor = ReasoningProviderDescriptor {
        provider_id: required("HUMAUX_PRIVATE_WORKER_PROVIDER_ID")?,
        model_id: required("HUMAUX_PRIVATE_WORKER_MODEL_ID")?,
        model_revision: env::var("HUMAUX_PRIVATE_WORKER_MODEL_REVISION")
            .ok()
            .filter(|v| !v.is_empty()),
        // ADR-0058 D-M (main-line ruling 2026-10-02 10:35): what the bound endpoint can do is
        // deployment configuration, never a literal or a provider name — the distill output
        // channel and any provider-specific request field follow these declared capabilities.
        capabilities: capabilities("HUMAUX_PRIVATE_WORKER_CAPABILITIES")?,
        custom_endpoint: Some(chat_url.clone()),
    };
    let http_timeout =
        Duration::from_secs(parse::<u64>("HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS")?);
    // The NAME of the variable holding the key is configuration (required: a default would name a
    // provider in code, §78.1); the key itself is read once, here, and only ever leaves this scope
    // inside `EnvCredential`.
    let key_env = required("HUMAUX_PRIVATE_WORKER_KEY_ENV")?;
    let key = env::var(&key_env).map_err(|_| {
        format!("missing required configuration: {key_env} (HUMAUX_PRIVATE_WORKER_KEY_ENV)")
    })?;
    // §11.4 static DNS pins (optional): `host=ip[|ip],...` — for hosts whose system DNS answer
    // is not trustworthy on this node. Unpinned hosts fall through to the system resolver
    // inside `PinnedDnsResolver`, so an empty/absent spec is exactly the old default. The
    // forbidden-range check still runs on the pins.
    let resolver: Arc<dyn ssrf::DnsResolver> = Arc::new(
        ssrf::PinnedDnsResolver::parse(
            &env::var("HUMAUX_PRIVATE_WORKER_DNS_PINS").unwrap_or_default(),
        )
        .map_err(|e| format!("invalid configuration: HUMAUX_PRIVATE_WORKER_DNS_PINS ({e:?})"))?,
    );
    // ADR-0039 判据0: the resolver is handed over **once** — `with_egress_transport` derives
    // both the §11.4 check and the client's dial-time resolver from this one value. Building
    // the transport separately is what let the operator's pins reach only the check while the
    // dial kept using system DNS (card 17 review, P0).
    let provider = OpenAiCompatibleProvider::with_egress_transport(
        descriptor,
        chat_url,
        http_timeout,
        EnvCredential(key),
        ssrf::CustomEndpointPolicy::default(),
        Arc::clone(&resolver),
    )
    .map_err(|e| {
        format!(
            "invalid configuration: HUMAUX_PRIVATE_WORKER_CHAT_URL rejected by the §11.4 SSRF \
             choke point, or the egress transport could not be built ({e:?})"
        )
    })?;

    // The Consolidate path takes prompt/schema/budget from the shared contract itself
    // (`ConsolidationReasoner`, ADR-0015 D2); these three fields only have to satisfy
    // `ContributionReasonerConfig::validate` for the config to be accepted at all.
    // ADR-0058 D-O: any ceiling satisfies `validate`; the highest storable one is the widest menu.
    let contract = consolidation_prompt_contract(AuthorityClass::ProjectConstraint);
    let config = ContributionReasonerConfig {
        allowed_egress_processor_id: ProcessorId(parse::<Uuid>(
            "HUMAUX_PRIVATE_WORKER_EGRESS_PROCESSOR_ID",
        )?),
        region: required("HUMAUX_PRIVATE_WORKER_REGION")?,
        permit_ttl: Duration::from_secs(parse::<u64>("HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS")?),
        // ponytail: no deployment has told us the endpoint's deletion semantics yet; make it
        // env-driven when a provider that does promise deletion is onboarded.
        deletion_capability: DeletionCapability::Unknown,
        system_prompt: contract.system_prompt,
        json_schema: contract.json_schema,
        max_output_tokens: contract.max_output_tokens,
    };
    config
        .validate()
        .map_err(|code| format!("invalid configuration: {code:?}"))?;

    // dep: PostgreSQL(role_private_worker) — role-scoped pool call
    let pool = PrivateWorkerDbPool::connect(&dsn)
        .await
        .map_err(|e| format!("private worker database role connection failed: {e}"))?;
    Ok(Bootstrap {
        pool,
        config,
        provider,
        http_timeout,
    })
}

/// ADR-0015 `--serve-rpc`: the Unix-domain-socket private inference listener.
async fn serve_rpc() -> Result<(), String> {
    let socket_path = required("HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH")?;
    let consolidation_uid = parse::<u32>("HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID")?;
    let Bootstrap {
        pool,
        config,
        provider,
        ..
    } = bootstrap().await?;
    let state = Arc::new(RpcState {
        expected_consolidation_uid: consolidation_uid,
        calls: pool,
        config,
        provider: Box::new(provider),
    });

    let listener = bind_socket(std::path::Path::new(&socket_path)).map_err(|error| {
        format!("failed to bind private inference RPC socket {socket_path}: {error}")
    })?;
    eprintln!("humaux-private-worker RPC listening on {socket_path}");
    let mut shutdown = Shutdown::install()?;
    // This listener holds no lease of its own: a call cut short by shutdown fails the caller's
    // inference hop, and the consolidation worker settles that job's lease on its own side
    // (card 14) rather than leaving it PROCESSING. So dropping the accept loop IS the drain
    // here — there is no in-process state a longer wait would settle.
    tokio::select! {
        result = serve(listener, state) => {
            result.map_err(|error| format!("private inference RPC server failed: {error}"))
        }
        () = shutdown.recv() => {
            eprintln!("humaux-private-worker: signal received, RPC listener closing");
            Ok(())
        }
    }
}

/// ADR-0016 `--distill-once` (IN_FLIGHT seats drain the cross-tenant backlog, then exit — an empty
/// backlog exits zero promptly) / `--distill-serve` (the same seats, resident: a seat sleeps
/// `HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS` only after its own claim came back empty).
/// ADR-0058: jobs are claimed one at a time through `ops.claim_derived_work_v2` (four provider
/// slots, least-recently-served tenant first); the route binding is still resolved by
/// `(tenant, reasoning_domain, purpose = PRIVATE_DISTILL_TEXT)` per job.
async fn distill_mode(resident: bool) -> Result<(), String> {
    let poll_interval = if resident {
        Some(Duration::from_secs(parse::<u64>(
            "HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS",
        )?))
    } else {
        None
    };
    let Bootstrap {
        pool,
        config,
        provider,
        http_timeout,
    } = bootstrap().await?;
    let dispatch = DistillDispatchConfig {
        // Per-process owner: the job lease and the outbox row a job takes are both fenced on it,
        // together with the claim generation (ADR-0058 D-E).
        lease_owner: format!("humaux-private-worker/{}", Uuid::now_v7()),
        lease_seconds: parse::<u64>("HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS")? as f64,
        in_flight: parse::<u32>("HUMAUX_PRIVATE_WORKER_DISTILL_IN_FLIGHT")?,
        hard_deadline_seconds: parse::<u64>("HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS")?
            as f64,
        http_timeout_seconds: http_timeout.as_secs_f64(),
        not_ready_park_seconds: parse::<u64>("HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS")?
            as f64,
        max_attempts: parse::<i32>("HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS")?,
        budget: jobs::DistillCallBudget {
            window_seconds: parse::<u64>("HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS")?
                as f64,
            max_calls: parse::<i32>("HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_MAX_CALLS")?,
        },
    };
    dispatch.validate().map_err(|code| {
        format!(
            "invalid configuration: HUMAUX_PRIVATE_WORKER_DISTILL_* ({code:?}; HARD_DEADLINE_SECS \
             must be >= 2 x (HTTP_TIMEOUT_SECS + LEASE_SECS); BUDGET_WINDOW_SECS and \
             BUDGET_MAX_CALLS must be >= 1)"
        )
    })?;
    let mut shutdown = Shutdown::install()?;
    let Some(poll) = poll_interval else {
        let report = distill::dispatch_pass(&pool, &provider, clone_config(&config), &dispatch)
            .await
            .map_err(|error| format!("distill dispatch failed: {error}"))?;
        println!("{}", report.summary_line());
        return Ok(());
    };
    // Card 15 / ADR-0037: a signal stops every seat after the job it holds (each job settles or,
    // for a call whose outcome is unknown, is reconciled by its hard deadline), so exiting cannot
    // leave a CLAIMED job with a live lease. Shutdown latency is bounded by one job (at most the
    // HTTP timeout plus the post-call legs); the supervisor's grace period must exceed it
    // (docs/ops/supervision.md).
    let stop = AtomicBool::new(false);
    let serve = distill::dispatch_serve(
        &pool,
        &provider,
        clone_config(&config),
        &dispatch,
        poll,
        &stop,
    );
    tokio::pin!(serve);
    let report = tokio::select! {
        report = &mut serve => report,
        () = shutdown.recv() => {
            eprintln!("humaux-private-worker: signal received, seats finish their current job");
            stop.store(true, Ordering::SeqCst);
            serve.await
        }
    }
    .map_err(|error| format!("distill dispatch failed: {error}"))?;
    println!("{}", report.summary_line());
    Ok(())
}
