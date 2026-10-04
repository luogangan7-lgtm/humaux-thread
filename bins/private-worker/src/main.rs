//! `private-worker::main` — `humaux-private-worker` process entry (§4.2 minimal process set; §4.4 admin probe
//!   contract; §11/§11.1 T4.4+T4.5; §11.8 ADR-0015 inference RPC).
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-telemetry, tokio, uuid]; services=[HTTP(loopback), PostgreSQL(role_private_worker)]; env=[CARGO_PKG_VERSION, HUMAUX_BUILD_GIT_SHA, HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID, HUMAUX_PRIVATE_WORKER_CREDENTIALS, HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_MAX_CALLS, HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_IN_FLIGHT, HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS, HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_SERVE_METRICS_ADDR, HUMAUX_PRIVATE_WORKER_DNS_PINS, HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS, HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS, HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS, HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS, HUMAUX_PRIVATE_WORKER_REGIONS, HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH, HUMAUX_PRIVATE_WORKER_SERVE_RPC_METRICS_ADDR, PRIVATE_WORKER_PG_DSN, refused:HUMAUX_PRIVATE_WORKER_{CAPABILITIES, CHAT_URL, EGRESS_PROCESSOR_ID, KEY_ENV, MODEL_ID, MODEL_REVISION, PROVIDER_ID, REGION}]; modules=[adapters::byok::ssrf, adapters::consolidation_reasoner, adapters::contribution_reasoner, adapters::disclosure, adapters::distill_repo, adapters::jobs, adapters::model_call_ledger, adapters::postgres, adapters::reasoning_route_admission, domain::authority, private-worker::distill, private-worker::inference_rpc, private-worker::route_providers, telemetry::metrics]
//! Called-by: [process(humaux-private-worker)]
//! Invariants: [the only process holding both role_private_worker DB write and BYOK decrypt capability (§11.1); a
//!   missing/invalid env value or unreachable DSN exits non-zero before serving; no provider, model, endpoint,
//!   capability, recipient or region is process configuration — each call's comes from its admitted route
//!   (ADR-0060 D-B/D-C), and a removed process-level key that is set refuses boot (E7); one `RouteProviders`;
//!   each resident mode reads only its own *_METRICS_ADDR key (required, loopback) and the one-shot modes open no
//!   listener; --metrics-families reads no configuration]
//! Spec: Baseline §11.1; §11.8; §78.1; ADR-0037; ADR-0036; ADR-0016; ADR-0058; ADR-0059; ADR-0060 D-B; ADR-0060 D-C;
//!   ADR-0060 D-J; ADR-0060 E3; ADR-0061 D-B; ADR-0061 D-C; ADR-0061 E10; §41.2; §42
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
//!   come from each call's admitted route (ADR-0060 D-B), never from this process's
//!   environment (§78.1: no literal model, endpoint, or dimension in code).
//! * `--readyz`: card 15 / ADR-0037 probe-based readiness — see [`readyz`]. Tenant-free
//!   (ADR-0036) and provider-free: it deliberately makes NO inference call, because a readiness
//!   probe that burns a paid provider round trip is a probe nobody dares to poll.
//! * `--distill-once` / `--distill-serve` (ADR-0016, cross-tenant since ADR-0036, seats since
//!   ADR-0058): IN_FLIGHT seats drain the backlog once
//!   ([`humaux_private_worker::distill::dispatch_pass`]) / stay resident
//!   ([`humaux_private_worker::distill::dispatch_serve`]) — same route/config bootstrap as
//!   `--serve-rpc` ([`bootstrap`]). There is no tenant id or reasoning domain in the environment:
//!   both come from each `DERIVED_DISTILL` job claimed through the owner SECURITY DEFINER
//!   `ops.claim_derived_work_v2` (migration 0190), and everything after the claim runs under that
//!   job's own tenant context.
//!
//! Card 34 / ADR-0061 D-B: `--serve-rpc` and `--distill-serve` each open their own loopback ops listener
//! (`HUMAUX_PRIVATE_WORKER_SERVE_RPC_METRICS_ADDR` / `HUMAUX_PRIVATE_WORKER_DISTILL_SERVE_METRICS_ADDR`).
//! Metrics families emitted (card 34b, ADR-0061 D-C): `private_distill_runs_total`, `private_distill_outputs_total`
//! (§42 no-output stage) and `private_reasoning_usage_total` (§35 quota), all unlabeled and seeded at 0.

use std::env;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use humaux_adapters::byok::ssrf;
use humaux_adapters::consolidation_reasoner::consolidation_prompt_contract;
use humaux_adapters::contribution_reasoner::ContributionReasonerConfig;
use humaux_adapters::disclosure::DeletionCapability;
use humaux_adapters::distill_repo;
use humaux_adapters::jobs;
use humaux_adapters::model_call_ledger;
use humaux_adapters::postgres::PrivateWorkerDbPool;
use humaux_adapters::reasoning_route_admission::{ProviderFor, ReasoningAdmissionLocator};
use humaux_domain::authority::AuthorityClass;
use humaux_private_worker::distill::{self, DistillDispatchConfig};
use humaux_private_worker::inference_rpc::{RpcState, bind_socket, clone_config, serve};
use humaux_private_worker::route_providers::{self, RouteProviders};
use humaux_telemetry::metrics::{
    OpsListener, families, parse_ops_addr, process_routes, serve_loopback, write_family,
};
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
    "usage: humaux-private-worker (--probe-connection | --readyz | --serve-rpc | --distill-once | --distill-serve | --metrics-families)"
}

/// §78.1 / ADR-0061 D-B: `--serve-rpc`'s own ops listener address (loopback only, no default).
const SERVE_RPC_METRICS_ADDR: &str = "HUMAUX_PRIVATE_WORKER_SERVE_RPC_METRICS_ADDR";
/// §78.1 / ADR-0061 D-B: `--distill-serve`'s own ops listener address (loopback only, no default).
const DISTILL_SERVE_METRICS_ADDR: &str = "HUMAUX_PRIVATE_WORKER_DISTILL_SERVE_METRICS_ADDR";

/// The §41.2 families this process counts (ADR-0061 D-C, card 34b): the two distill counters and the private
/// reasoning usage, each read from its adapters emit. `/metrics` and `--metrics-families` both call this.
fn render_metrics(out: &mut String) {
    for (family, value) in [
        (
            &families::PRIVATE_DISTILL_RUNS_TOTAL,
            distill_repo::private_distill_runs_total(),
        ),
        (
            &families::PRIVATE_DISTILL_OUTPUTS_TOTAL,
            distill_repo::private_distill_outputs_total(),
        ),
        (
            &families::PRIVATE_REASONING_USAGE_TOTAL,
            model_call_ledger::private_reasoning_usage_total(),
        ),
    ] {
        write_family(out, family, &[(&[], value as f64)]);
    }
}

/// Binds `key`'s loopback ops listener for resident `mode` (ADR-0061 D-B). The handle must live as long as the
/// mode: dropping it closes the port.
fn ops_listener(key: &'static str, mode: &'static str) -> Result<OpsListener, String> {
    let addr = parse_ops_addr(&required(key)?)
        .map_err(|reason| format!("invalid configuration: {key}: {reason}"))?;
    let routes = process_routes(
        "humaux-private-worker",
        mode,
        env!("CARGO_PKG_VERSION"),
        option_env!("HUMAUX_BUILD_GIT_SHA"),
        render_metrics,
    );
    // dep: HTTP(loopback) — this mode's /metrics + /status listener
    serve_loopback(key, addr, routes).map_err(|e| e.to_string())
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        None | Some("--probe-connection") => {
            probe_connection().await;
            ExitCode::SUCCESS
        }
        // ADR-0061 D-C: before any configuration is read.
        Some("--metrics-families") => {
            let mut out = String::new();
            render_metrics(&mut out);
            print!("{out}");
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
/// * the provider endpoints — a readiness poll must not spend a BYOK inference call, and §11.4's
///   SSRF choke point refuses a bad endpoint when its route's instance is first built
///   (ADR-0060 D-B), with the route's NOT_READY class, not here;
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

/// Process-level keys removed by ADR-0059 D-I (the single key variable) and ADR-0060 D-C (the five
/// provider keys and the two singular deny-list keys): the provider, model, endpoint, capabilities,
/// recipient and region of a call come from its admitted route. Refused when set, so stale
/// configuration is never silently ignored (the card-16 P0 shape).
const REMOVED_KEYS: [(&str, &str); 8] = [
    (
        "HUMAUX_PRIVATE_WORKER_KEY_ENV",
        "ADR-0059 D-I; use HUMAUX_PRIVATE_WORKER_CREDENTIALS",
    ),
    ("HUMAUX_PRIVATE_WORKER_PROVIDER_ID", "ADR-0060 D-C"),
    ("HUMAUX_PRIVATE_WORKER_MODEL_ID", "ADR-0060 D-C"),
    ("HUMAUX_PRIVATE_WORKER_MODEL_REVISION", "ADR-0060 D-C"),
    ("HUMAUX_PRIVATE_WORKER_CHAT_URL", "ADR-0060 D-C"),
    ("HUMAUX_PRIVATE_WORKER_CAPABILITIES", "ADR-0060 D-C"),
    (
        "HUMAUX_PRIVATE_WORKER_EGRESS_PROCESSOR_ID",
        "ADR-0060 D-C; use HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS",
    ),
    (
        "HUMAUX_PRIVATE_WORKER_REGION",
        "ADR-0060 D-C; use HUMAUX_PRIVATE_WORKER_REGIONS",
    ),
];

/// Refuses the first [`REMOVED_KEYS`] entry `lookup` finds set, naming it (T22).
fn refuse_removed_keys(lookup: impl Fn(&str) -> Option<String>) -> Result<(), String> {
    match REMOVED_KEYS.iter().find(|(name, _)| lookup(name).is_some()) {
        Some((name, why)) => Err(format!(
            "invalid configuration: {name} was removed by {why}"
        )),
        None => Ok(()),
    }
}

/// The route→provider seam + deployment config + `role_private_worker` pool every
/// inference-bearing mode of this binary shares (`--serve-rpc`, `--distill-once`,
/// `--distill-serve`) — one bootstrap, so the Distill hop and the RPC listener can never drift on
/// which keys, recipients and regions they hold (§4.2 one process; ADR-0060 D-B).
struct Bootstrap {
    pool: PrivateWorkerDbPool,
    config: ContributionReasonerConfig,
    /// ADR-0060 D-B: admitted route → its Profile@version's instance ([`RouteProviders`]).
    providers: Box<ProviderFor>,
    /// The provider transport timeout; the distill dispatcher sizes `ops.begin_call`'s window
    /// from it (ADR-0058 D-K).
    http_timeout: Duration,
    /// Ruling E3: the validity of a worker-observed health renewal, and twice the remaining
    /// validity below which a SUCCEEDED call renews it.
    health_renew_seconds: i64,
}

async fn bootstrap() -> Result<Bootstrap, String> {
    let lookup = |name: &str| env::var(name).ok();
    refuse_removed_keys(lookup)?;
    let dsn = required("PRIVATE_WORKER_PG_DSN")?;
    let http_timeout =
        Duration::from_secs(parse::<u64>("HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS")?);
    // Ruling E3 (b): required, no default (§78.1) — it trades attestation lifetime against one
    // extra definer call per renewal window.
    let health_renew_seconds = Some(parse::<i64>("HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS")?)
        .filter(|secs| *secs > 0)
        .ok_or("invalid configuration: HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS")?;
    // ADR-0059 D-I / ADR-0060 D-J: the keys are read once, here, and only ever leave this scope
    // inside `EnvCredentialMap`. ADR-0060 D-C: the recipient ↔ host and region lists refuse a
    // route, never select one; explicitly empty lists are legal (every route parks).
    let credentials = route_providers::parse_credential_map(lookup)?;
    let recipients = route_providers::parse_recipients(lookup)?;
    let regions = route_providers::parse_regions(lookup)?;
    // §11.4 static DNS pins (optional): `host=ip[|ip],...` — for hosts whose system DNS answer
    // is not trustworthy on this node. Unpinned hosts fall through to the system resolver
    // inside `PinnedDnsResolver`, so an empty/absent spec is exactly the old default. The
    // forbidden-range check still runs on the pins. ADR-0039 判据0: this one resolver is handed
    // to every instance, which derives both its §11.4 check and its dial from it.
    let resolver: Arc<dyn ssrf::DnsResolver> = Arc::new(
        ssrf::PinnedDnsResolver::parse(
            &env::var("HUMAUX_PRIVATE_WORKER_DNS_PINS").unwrap_or_default(),
        )
        .map_err(|e| format!("invalid configuration: HUMAUX_PRIVATE_WORKER_DNS_PINS ({e:?})"))?,
    );

    // The Consolidate path takes prompt/schema/budget from the shared contract itself
    // (`ConsolidationReasoner`, ADR-0015 D2); these three fields only have to satisfy
    // `ContributionReasonerConfig::validate` for the config to be accepted at all.
    // ADR-0058 D-O: any ceiling satisfies `validate`; the highest storable one is the widest menu.
    let contract = consolidation_prompt_contract(AuthorityClass::ProjectConstraint);
    let config = ContributionReasonerConfig {
        permit_ttl: Duration::from_secs(parse::<u64>("HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS")?),
        // ponytail: no deployment has told us the endpoint's deletion semantics yet; make it
        // per-recipient when a provider that does promise deletion is onboarded (ADR-0060 L7).
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
    // ADR-0060 D-J: one secret serves one vendor account, or the worker does not start.
    for line in route_providers::verify_credential_accounts(&pool, &credentials).await? {
        eprintln!("humaux-private-worker: {line}");
    }
    eprintln!(
        "humaux-private-worker: routes credentials={} recipients={} regions={}",
        credentials.refs().len(),
        recipients.len(),
        regions.len()
    );
    let routes = Arc::new(RouteProviders::new(
        credentials,
        recipients,
        regions,
        resolver,
        http_timeout,
    ));
    Ok(Bootstrap {
        pool,
        config,
        providers: Box::new(move |route: &ReasoningAdmissionLocator| routes.provider_for(route)),
        http_timeout,
        health_renew_seconds,
    })
}

/// ADR-0015 `--serve-rpc`: the Unix-domain-socket private inference listener.
async fn serve_rpc() -> Result<(), String> {
    let _ops = ops_listener(SERVE_RPC_METRICS_ADDR, "serve-rpc")?;
    let socket_path = required("HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH")?;
    let consolidation_uid = parse::<u32>("HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID")?;
    let Bootstrap {
        pool,
        config,
        providers,
        health_renew_seconds,
        ..
    } = bootstrap().await?;
    let state = Arc::new(RpcState {
        expected_consolidation_uid: consolidation_uid,
        calls: pool,
        config,
        providers,
        health_renew_seconds,
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
/// slots, fewest-held-slots tenant first, ADR-0060 D-F); the route binding is resolved by
/// `(tenant, reasoning_domain, purpose = PRIVATE_DISTILL_TEXT)` per job and its admitted
/// Profile@version picks the instance (ADR-0060 D-B).
async fn distill_mode(resident: bool) -> Result<(), String> {
    // ADR-0061 D-B: only the resident mode is scraped; `--distill-once` opens no listener.
    let _ops = if resident {
        Some(ops_listener(DISTILL_SERVE_METRICS_ADDR, "distill-serve")?)
    } else {
        None
    };
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
        providers,
        http_timeout,
        health_renew_seconds,
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
        health_renew_seconds,
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
        let report = distill::dispatch_pass(&pool, &*providers, clone_config(&config), &dispatch)
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
        &*providers,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// T22 (ADR-0060 D-C, E7) — fault: drop one name from [`REMOVED_KEYS`] ⇒ that key boots.
    #[test]
    fn removed_provider_env_is_refused_at_boot() {
        assert_eq!(refuse_removed_keys(|_| None), Ok(()));
        for (name, _) in REMOVED_KEYS {
            let error =
                refuse_removed_keys(|n| (n == name).then(|| "x".to_owned())).expect_err(name);
            assert!(
                error.contains(name) && error.contains("removed by"),
                "{name}: {error}"
            );
        }
        let names: Vec<&str> = REMOVED_KEYS.iter().map(|(n, _)| *n).collect();
        for suffix in [
            "PROVIDER_ID",
            "MODEL_ID",
            "MODEL_REVISION",
            "CHAT_URL",
            "CAPABILITIES",
            "EGRESS_PROCESSOR_ID",
            "REGION",
            "KEY_ENV",
        ] {
            assert!(
                names.contains(&format!("HUMAUX_PRIVATE_WORKER_{suffix}").as_str()),
                "{suffix} is refused"
            );
        }
    }
}
