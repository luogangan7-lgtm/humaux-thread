//! `maintenance::main` — `humaux-maintenance`, the operator-write CLI (§4.2): onboarding, API keys, placement,
//!   activation, re-drive of DEAD distill jobs, role-password rotation, the deploy-check, opening/closing
//!   the API-key pepper rehash window and the reasoning-route doors (register / bind / attest-health /
//!   profile-state / status, ADR-0060 D-H), `sweep once` (one page of every maintenance task, ADR-0062 D-Q), the
//!   one-shot §48.1 retention executor `retention approve | create-partitions | execute` (ADR-0063 D-I), and the
//!   two resident modes `health serve` (ADR-0061 D-D) and `--serve`, the maintenance daemon (ADR-0062).
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-protocol, rand, serde, serde_json, time, tokio, uuid];
//!   services=[PostgreSQL(role_maintenance)]; env=[HUMAUX_MAINTENANCE_CREDENTIAL_PEPPER_HEX,
//!   HUMAUX_MAINTENANCE_EMBEDDING_DIMENSION, HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_MAINTENANCE_PRIVATE_MEMORY_COLLECTION,
//!   HUMAUX_MAINTENANCE_QDRANT_CIDR, HUMAUX_MAINTENANCE_QDRANT_HOST, HUMAUX_MAINTENANCE_QDRANT_PORT];
//!   modules=[adapters::byok, adapters::membership_repo, adapters::postgres, adapters::provisioning,
//!   adapters::quota_repo, adapters::reasoning_route_onboarding, adapters::role_hygiene,
//!   domain::identity, domain::ids, domain::ticket_family, maintenance::health_serve, maintenance::retention,
//!   maintenance::roles, maintenance::serve, protocol::edge]
//! Called-by: [process(humaux-maintenance)]
//! Invariants: [every subcommand but `health serve` and `--serve` is one-shot; one JSON receipt on stdout per run; exit 0 created/existing, 3 refused, 2 usage, 1
//!   infrastructure (PostgreSQL/Qdrant down); the wire key and generated role passwords are printed once on stdout
//!   before the receipt, never on stderr or in a receipt; no flag or env var has a literal default]
//! Spec: Baseline §4.2; §6.2.2; §11.2.3; §41.2; §48.1; §73.5; §77; §78.1; ADR-0053; ADR-0058; ADR-0059; ADR-0060;
//!   ADR-0061; ADR-0062; ADR-0063
//!
//! Subcommand mode (card 28). Two modes are resident: `health serve` (ADR-0061 D-D) samples the §41.2 health
//! gauges, and `--serve` (ADR-0062) runs the scheduled maintenance tasks; each runs until SIGTERM, answers
//! readiness on its own ops listener and prints its receipt on exit;
//! `--metrics-families` (`health serve`'s families) and `--serve --metrics-families` (the daemon's, ADR-0062 D-S)
//! print their zero-state exposition before any config is read. Every other subcommand is
//! one-shot, idempotent (a re-run writes nothing and answers `existing`; `apikey pepper-epoch advance`
//! is instead refused `rehash_window_open` while its window is open, ADR-0059 D-H), and prints exactly ONE
//! JSON receipt on stdout. Exit codes (ADR-0053 D-F): 0 created/existing, 3 refused (a named
//! reason, nothing written), 2 usage, 1 infrastructure (`sweep once` still prints its receipt then). `jobs requeue-dead` (ADR-0058 R4) answers
//! `requeued` (`nothing_requeued` when class mode skipped every match; class mode lists each skipped
//! DEAD job with `evidence_gone` / `outbox_settled`, exit 0); its re-run is refused `job_not_dead` /
//! `no_dead_job` (exit 3), never a second re-arm.
//!
//! Secrets: the pepper comes from the environment only. A newly minted API key is printed ONCE,
//! as the single line `Authorization: Bearer <prefix>.<secret>` on stdout before the JSON, only
//! when it was created — never on stderr, never inside a receipt, never on a re-run (a lost key
//! is revoked and reissued under a new `--key-name`). Receipts carry the log fingerprint only.
//! `reasoning register` (ADR-0060 D-H) takes `--account-ref` as text and sends only its sha256; it
//! never takes a key — the receipt names the `<credential_ref>=<ENV_NAME>` line the operator adds to
//! the private worker's `HUMAUX_PRIVATE_WORKER_CREDENTIALS` (D-J).
//! `roles rotate` (ADR-0059 D-E) prints each generated role password the same way, once, as
//! `HUMAUX_ROLE_PASSWORD_<SUFFIX>=<value>`; `deploy-check` (D-F) is read-only and prints names only.
//!
//! Every writing subcommand requires the §77 Sensitive-Admin-Action fields `--actor --reason
//! --ticket --step-up-auth` (`--trace-id` optional; minted and printed in the receipt otherwise).
//! No flag has a literal default (§78.1) except the two names `--workspace` / `--reasoning-domain`
//! (`default`, the name the seed always used).

mod health_serve;
mod resident;
mod retention;
mod roles;
mod serve;

use std::process::ExitCode;

use humaux_adapters::byok::ReasoningCapability;
use humaux_adapters::membership_repo::AdminAction;
use humaux_adapters::postgres::MaintenanceDbPool;
use humaux_adapters::provisioning::{
    self, NewApiKey, ProvisioningError, QdrantFace, RequeueTarget, TenantRequest,
    WorkspaceActivation,
};
use humaux_adapters::quota_repo;
use humaux_adapters::reasoning_route_onboarding::{self as routes, RegisterProfile};
use humaux_adapters::role_hygiene;
use humaux_domain::identity::MembershipRole;
use humaux_domain::ids::TenantId;
use humaux_domain::ticket_family::TicketFamily;
use humaux_protocol::edge::{api_key_log_fingerprint, compute_api_key_hash};
use rand::Rng;
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

const USAGE: &str = "usage: humaux-maintenance <deploy-init | onboard tenant|workspace|user | \
apikey issue|revoke | apikey pepper-epoch advance|close | placement ensure | collection ensure | activate | status | \
jobs requeue-dead --tenant ID (--job ID | --error-class CLASS) | \
reasoning register|bind|attest-health|profile-state|status --tenant ID | \
roles rotate --roles-sql PATH [--role ROLE]... [--create-missing] | deploy-check --roles-sql PATH | \
retention approve|create-partitions|execute | \
health serve | --serve | --serve --metrics-families | sweep once | --metrics-families> [flags]";

/// A failure before or outside the provisioning library.
enum Failure {
    Usage(String),
    Infra(String),
    Provisioning(ProvisioningError),
}

impl From<ProvisioningError> for Failure {
    fn from(error: ProvisioningError) -> Self {
        Self::Provisioning(error)
    }
}

type Result<T> = std::result::Result<T, Failure>;

struct Args(Vec<String>);

impl Args {
    fn get(&self, flag: &str) -> Option<String> {
        self.0
            .iter()
            .position(|a| a == flag)
            .and_then(|i| self.0.get(i + 1))
            .cloned()
    }

    fn required(&self, flag: &str) -> Result<String> {
        self.get(flag)
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| {
                Failure::Usage(format!("missing required flag {flag} (§78.1: no default)"))
            })
    }

    fn parsed<T: std::str::FromStr>(&self, flag: &str) -> Result<T> {
        self.required(flag)?
            .parse()
            .map_err(|_| Failure::Usage(format!("{flag}: not a valid value")))
    }
}

fn env(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| Failure::Usage(format!("missing environment variable {name}")))
}

fn env_parsed<T: std::str::FromStr>(name: &str) -> Result<T> {
    env(name)?
        .parse()
        .map_err(|_| Failure::Usage(format!("{name}: not a valid value")))
}

struct Admin {
    actor: String,
    reason: String,
    ticket: String,
    trace_id: String,
    step_up: String,
}

impl Admin {
    fn from(args: &Args) -> Result<Self> {
        Ok(Self {
            actor: args.required("--actor")?,
            reason: args.required("--reason")?,
            ticket: args.required("--ticket")?,
            trace_id: args
                .get("--trace-id")
                .unwrap_or_else(|| Uuid::now_v7().to_string()),
            step_up: args.required("--step-up-auth")?,
        })
    }

    fn action(&self) -> AdminAction<'_> {
        AdminAction {
            actor: &self.actor,
            reason: &self.reason,
            ticket: &self.ticket,
            trace_id: &self.trace_id,
            step_up_auth_context: &self.step_up,
        }
    }
}

/// The pepper (§73.5) — must equal the gateway's `HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX`.
fn pepper() -> Result<Vec<u8>> {
    let hex = env("HUMAUX_MAINTENANCE_CREDENTIAL_PEPPER_HEX")?;
    if !hex.len().is_multiple_of(2) {
        return Err(Failure::Usage("pepper hex has odd length".to_owned()));
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| Failure::Usage("pepper is not hex".to_owned()))
}

/// Mints `<prefix>.<secret>` for (tenant, key name): the prefix is deterministic
/// (`provisioning::api_key_prefix`), the 32-char secret random. Returns the key material for the
/// library and the wire key for the single stdout line.
fn mint(pepper: &[u8], tenant_id: Uuid, key_name: &str) -> (NewApiKey, String) {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    let secret: String = (0..32)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect();
    let prefix = provisioning::api_key_prefix(tenant_id, key_name);
    let wire = format!("{prefix}.{secret}");
    let key_hash = compute_api_key_hash(pepper, &wire);
    let fingerprint = api_key_log_fingerprint(&prefix, &key_hash);
    (
        NewApiKey {
            prefix,
            key_hash,
            fingerprint,
        },
        wire,
    )
}

/// The one stdout line that carries a newly minted key, when there is one.
fn bearer_line(wire: Option<String>) -> Vec<String> {
    wire.map(|w| format!("Authorization: Bearer {w}"))
        .into_iter()
        .collect()
}

fn scopes(args: &Args) -> Result<Vec<String>> {
    let scopes: Vec<String> = args
        .required("--scopes")?
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();
    if scopes.is_empty() {
        return Err(Failure::Usage("--scopes is empty".to_owned()));
    }
    Ok(scopes)
}

fn uuid_flag(args: &Args, flag: &str) -> Result<Uuid> {
    args.parsed::<Uuid>(flag)
}

async fn pool() -> Result<MaintenanceDbPool> {
    let dsn = env("HUMAUX_MAINTENANCE_PG_DSN")?;
    // dep: PostgreSQL(role_maintenance) — the operator-write pool (§4.2)
    MaintenanceDbPool::connect(&dsn)
        .await
        .map_err(|e| Failure::Infra(format!("connect HUMAUX_MAINTENANCE_PG_DSN: {e}")))
}

/// The Qdrant face and the collection/dimension every onboarding step uses.
struct Qdrant {
    face: QdrantFace,
    collection: String,
    dimension: u32,
}

fn qdrant() -> Result<Qdrant> {
    let face = QdrantFace::new(
        &env("HUMAUX_MAINTENANCE_QDRANT_HOST")?,
        env_parsed("HUMAUX_MAINTENANCE_QDRANT_PORT")?,
        &env("HUMAUX_MAINTENANCE_QDRANT_CIDR")?,
    )?;
    Ok(Qdrant {
        face,
        collection: env("HUMAUX_MAINTENANCE_PRIVATE_MEMORY_COLLECTION")?,
        dimension: env_parsed("HUMAUX_MAINTENANCE_EMBEDDING_DIMENSION")?,
    })
}

fn to_json<T: serde::Serialize>(value: &T) -> Result<Value> {
    serde_json::to_value(value).map_err(|e| Failure::Infra(format!("receipt: {e}")))
}

/// A receipt plus whether it reports a refusal (exit 3) and the secret lines printed once before it
/// (a minted `Authorization: Bearer` key, or generated `HUMAUX_ROLE_PASSWORD_<SUFFIX>=` values).
struct Output {
    receipt: Value,
    refused: bool,
    /// An infrastructure failure reported in the receipt itself (`sweep once`, ADR-0062 D-Q): exit 1.
    failed: bool,
    once: Vec<String>,
}

impl Output {
    fn ok(receipt: Value) -> Self {
        Self {
            receipt,
            refused: false,
            failed: false,
            once: Vec::new(),
        }
    }

    /// Everything printed on stdout: the once-lines, then the one JSON receipt.
    fn render(&self) -> String {
        self.once
            .iter()
            .map(String::as_str)
            .chain([self.receipt.to_string().as_str()])
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn activation(receipt: Value, activation: &WorkspaceActivation) -> Self {
        Self {
            receipt,
            refused: activation.refusal().is_some(),
            failed: false,
            once: Vec::new(),
        }
    }
}

async fn deploy_init(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let provider = args.required("--provider")?;
    let region = args.required("--region")?;
    let tpm: i64 = args.parsed("--tpm")?;
    let rpm: i64 = args.parsed("--rpm")?;
    let pool = pool().await?;
    let receipt =
        provisioning::deploy_init(&pool, &provider, &region, tpm, rpm, &admin.action()).await?;
    Ok(Output::ok(to_json(&receipt)?))
}

async fn onboard_tenant(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let name = args.required("--name")?;
    let owner_email = args.required("--owner-email")?;
    let workspace = args
        .get("--workspace")
        .unwrap_or_else(|| "default".to_owned());
    let reasoning_domain = args
        .get("--reasoning-domain")
        .unwrap_or_else(|| "default".to_owned());
    let plan_limit: i64 = args.parsed("--plan-limit")?;
    let period_end = OffsetDateTime::parse(&args.required("--period-end")?, &Rfc3339)
        .map_err(|_| Failure::Usage("--period-end must be RFC 3339".to_owned()))?;
    let scopes = scopes(args)?;
    let key_name = args.required("--key-name")?;
    let provider = args.required("--provider")?;
    let region = args.required("--region")?;
    let tenant_tpm: i64 = args.parsed("--tenant-tpm")?;
    let tenant_rpm: i64 = args.parsed("--tenant-rpm")?;
    let pepper = pepper()?;
    let qdrant = qdrant()?;
    let pool = pool().await?;

    let started = std::time::Instant::now();
    let request = TenantRequest {
        name: &name,
        owner_email: &owner_email,
        workspace_name: &workspace,
        reasoning_domain_name: &reasoning_domain,
        plan_limit,
        period_start: OffsetDateTime::now_utc() - time::Duration::seconds(1),
        period_end,
        provider_id: &provider,
        region: &region,
        tenant_tpm,
        tenant_rpm,
        scopes: &scopes,
        collection: &qdrant.collection,
    };
    let mut wire = None;
    let mut minter = |tenant_id: Uuid| {
        let (key, minted) = mint(&pepper, tenant_id, &key_name);
        wire = Some(minted);
        key
    };
    let tenant =
        provisioning::onboard_tenant(&pool, &request, &mut minter, &admin.action()).await?;
    quota_repo::issue_window(&pool, TenantId(tenant.tenant_id))
        .await
        .map_err(|e| Failure::Infra(format!("issue_window: {e:?}")))?;
    let collection =
        provisioning::ensure_collection(&qdrant.face, &qdrant.collection, qdrant.dimension).await?;
    let activation = provisioning::activate_workspace(
        &pool,
        &qdrant.face,
        tenant.tenant_id,
        tenant.workspace_id,
        qdrant.dimension,
        None,
        &admin.action(),
    )
    .await?;
    let mut receipt = to_json(&tenant)?;
    receipt["collection"] = to_json(&collection)?;
    receipt["activations"] = to_json(&activation.activations)?;
    receipt["lifecycle"] = json!(activation.lifecycle);
    receipt["trace_id"] = json!(admin.trace_id);
    receipt["wall_clock_ms"] = json!(started.elapsed().as_millis());
    let mut output = Output::activation(receipt, &activation);
    // Printed only when this run created the key (the tenant was created).
    output.once = bearer_line(tenant.api_key.as_ref().filter(|k| k.created).and(wire));
    Ok(output)
}

async fn onboard_workspace(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let tenant_id = uuid_flag(args, "--tenant")?;
    let name = args.required("--name")?;
    let owner = uuid_flag(args, "--owner")?;
    let qdrant = qdrant()?;
    let pool = pool().await?;
    let workspace =
        provisioning::onboard_workspace(&pool, tenant_id, &name, owner, &admin.action()).await?;
    let collection =
        provisioning::ensure_collection(&qdrant.face, &qdrant.collection, qdrant.dimension).await?;
    let activation = provisioning::activate_workspace(
        &pool,
        &qdrant.face,
        tenant_id,
        workspace.workspace_id,
        qdrant.dimension,
        None,
        &admin.action(),
    )
    .await?;
    let mut receipt = to_json(&workspace)?;
    receipt["collection"] = to_json(&collection)?;
    receipt["activations"] = to_json(&activation.activations)?;
    receipt["lifecycle"] = json!(activation.lifecycle);
    Ok(Output::activation(receipt, &activation))
}

async fn onboard_user(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let tenant_id = uuid_flag(args, "--tenant")?;
    let email = args.required("--email")?;
    let role = MembershipRole::from_db_str(&args.required("--role")?)
        .ok_or_else(|| Failure::Usage("--role must be OWNER, ADMIN or MEMBER".to_owned()))?;
    let workspace = match args.get("--workspace") {
        Some(_) => Some(uuid_flag(args, "--workspace")?),
        None => None,
    };
    let pool = pool().await?;
    let receipt =
        provisioning::onboard_user(&pool, tenant_id, &email, role, workspace, &admin.action())
            .await?;
    Ok(Output::ok(to_json(&receipt)?))
}

async fn apikey_issue(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let tenant_id = uuid_flag(args, "--tenant")?;
    let user_id = uuid_flag(args, "--user")?;
    let workspace_id = uuid_flag(args, "--workspace")?;
    let scopes = scopes(args)?;
    let key_name = args.required("--key-name")?;
    let pepper = pepper()?;
    let pool = pool().await?;
    let (key, wire) = mint(&pepper, tenant_id, &key_name);
    let receipt = provisioning::issue_api_key(
        &pool,
        tenant_id,
        user_id,
        workspace_id,
        &key,
        &scopes,
        &admin.action(),
    )
    .await?;
    let mut output = Output::ok(to_json(&receipt)?);
    output.receipt["outcome"] = json!(if receipt.created {
        "created"
    } else {
        "existing"
    });
    output.once = bearer_line(receipt.created.then_some(wire));
    Ok(output)
}

async fn apikey_revoke(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let tenant_id = uuid_flag(args, "--tenant")?;
    let key_name = args.required("--key-name")?;
    let pool = pool().await?;
    let prefix = provisioning::api_key_prefix(tenant_id, &key_name);
    let receipt = provisioning::revoke_api_key(&pool, tenant_id, &prefix, &admin.action()).await?;
    Ok(Output::ok(to_json(&receipt)?))
}

async fn placement_ensure(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let tenant_id = uuid_flag(args, "--tenant")?;
    let collection = env("HUMAUX_MAINTENANCE_PRIVATE_MEMORY_COLLECTION")?;
    let pool = pool().await?;
    let placements =
        provisioning::ensure_placement(&pool, tenant_id, &collection, &admin.action()).await?;
    let created = placements.iter().any(|p| p.created);
    Ok(Output::ok(json!({
        "outcome": if created { "created" } else { "existing" },
        "tenant_id": tenant_id,
        "placements": to_json(&placements)?,
    })))
}

async fn collection_ensure() -> Result<Output> {
    let qdrant = qdrant()?;
    let collection =
        provisioning::ensure_collection(&qdrant.face, &qdrant.collection, qdrant.dimension).await?;
    let mut receipt = to_json(&collection)?;
    receipt["outcome"] = json!(if collection.created {
        "created"
    } else {
        "existing"
    });
    Ok(Output::ok(receipt))
}

async fn activate(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let tenant_id = uuid_flag(args, "--tenant")?;
    let workspace_id = uuid_flag(args, "--workspace")?;
    // Absent triple = every family (TicketFamily::ALL); a partial triple is a usage error.
    let only = match (
        args.get("--domain"),
        args.get("--projection-kind"),
        args.get("--version"),
    ) {
        (None, None, None) => None,
        (Some(d), Some(k), Some(v)) => Some(
            TicketFamily::from_triple(&d, &k, &v)
                .ok_or_else(|| Failure::Usage("unknown family triple".to_owned()))?,
        ),
        _ => {
            return Err(Failure::Usage(
                "--domain, --projection-kind and --version go together".to_owned(),
            ));
        }
    };
    let qdrant = qdrant()?;
    let pool = pool().await?;
    let activation = provisioning::activate_workspace(
        &pool,
        &qdrant.face,
        tenant_id,
        workspace_id,
        qdrant.dimension,
        only,
        &admin.action(),
    )
    .await?;
    let mut receipt = to_json(&activation)?;
    receipt["outcome"] = json!(match activation.refusal() {
        Some(_) => "refused",
        None if activation
            .activations
            .iter()
            .any(|a| a.outcome == "activated") =>
            "created",
        None => "existing",
    });
    if let Some(reason) = activation.refusal() {
        receipt["reason"] = json!(reason);
    }
    Ok(Output::activation(receipt, &activation))
}

/// ADR-0058 R4: re-arms DEAD distill jobs (one `--job`, or every DEAD job of the tenant with one
/// exact `--error-class`) and prints the re-armed jobs in the receipt, and in class mode the
/// matching DEAD jobs it skipped with the reason (ADR-0058 ruling 2026-10-02 20:30, 0200).
async fn jobs_requeue_dead(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let tenant_id = uuid_flag(args, "--tenant")?;
    let class = args.get("--error-class");
    let target = match (args.get("--job"), class.as_deref()) {
        (Some(_), None) => RequeueTarget::Job(uuid_flag(args, "--job")?),
        (None, Some(class)) if !class.trim().is_empty() => RequeueTarget::ErrorClass(class),
        _ => {
            return Err(Failure::Usage(
                "jobs requeue-dead takes exactly one of --job or --error-class".to_owned(),
            ));
        }
    };
    let pool = pool().await?;
    let receipt =
        provisioning::requeue_dead_distill(&pool, tenant_id, target, &admin.action()).await?;
    Ok(Output::ok(to_json(&receipt)?))
}

/// `apikey pepper-epoch advance|close` (ADR-0059 D-H, runbook Rotate pepper phases 3 and 4):
/// `advance` opens the rehash-on-use window once every gateway replica runs with the new current
/// pepper; `close` shuts it, after which no key's verifier can be rewritten. `advance` is not
/// re-runnable inside one window: while it is open a second `advance` is refused
/// `rehash_window_open` (exit 3, epoch unchanged; migration 0205); `close` re-runs as a no-op.
async fn apikey_pepper_epoch(args: &Args, open: bool) -> Result<Output> {
    let admin = Admin::from(args)?;
    let pool = pool().await?;
    let epoch = role_hygiene::set_pepper_window(&pool, open).await?;
    // ponytail: receipt-only audit (control.audit_events needs a tenant; the epoch is cluster-wide),
    // a tenant-less ops audit stream is the upgrade path (ADR-0059 L3).
    Ok(Output::ok(json!({
        "command": if open { "apikey pepper-epoch advance" } else { "apikey pepper-epoch close" },
        "epoch": epoch,
        "rehash_open": open,
        "actor": admin.actor,
        "reason": admin.reason,
        "ticket": admin.ticket,
        "trace_id": admin.trace_id,
    })))
}

async fn status(args: &Args) -> Result<Output> {
    let tenant_id = uuid_flag(args, "--tenant")?;
    let pool = pool().await?;
    Ok(Output::ok(provisioning::status(&pool, tenant_id).await?))
}

/// `--profile ID --profile-version N`: one Profile@version (ADR-0060 D-H).
fn profile_flags(args: &Args) -> Result<(Uuid, i64)> {
    Ok((
        uuid_flag(args, "--profile")?,
        args.parsed("--profile-version")?,
    ))
}

/// `reasoning register` (ADR-0060 D-H 1): one Profile@version with its catalog row, vendor account,
/// endpoint and credential reference. Every provider-shaped value is a flag (§78.1: no default);
/// `--request-extras` is required too (pass `{}` for none), so no vendor field is ever implied.
async fn reasoning_register(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let capabilities = args
        .required("--capabilities")?
        .split(',')
        .map(|c| {
            ReasoningCapability::parse(c.trim()).ok_or_else(|| {
                Failure::Usage(format!(
                    "--capabilities: {c:?} is outside the §11.2 closed set"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let request_extras: Value = serde_json::from_str(&args.required("--request-extras")?)
        .map_err(|e| Failure::Usage(format!("--request-extras: not JSON ({e})")))?;
    let optional_uuid = |flag: &str| args.get(flag).map(|_| uuid_flag(args, flag)).transpose();
    let (processor_id, provider_model_id, account_ref) = (
        args.required("--provider-id")?,
        args.required("--provider-model-id")?,
        args.required("--account-ref")?,
    );
    let (endpoint_ref, region, service_tier) = (
        args.required("--endpoint-ref")?,
        args.required("--region")?,
        args.required("--service-tier")?,
    );
    let model_revision = args.get("--model-revision");
    let request = RegisterProfile {
        tenant_id: uuid_flag(args, "--tenant")?,
        owner_user_id: uuid_flag(args, "--owner-user")?,
        processor_id: &processor_id,
        provider_model_id: &provider_model_id,
        model_revision: model_revision.as_deref(),
        capabilities: &capabilities,
        request_extras: &request_extras,
        account_ref: &account_ref,
        endpoint_ref: &endpoint_ref,
        region: &region,
        service_tier: &service_tier,
        egress_processor_id: uuid_flag(args, "--egress-processor-id")?,
        credential_ref: optional_uuid("--credential-ref")?,
        successor_of: optional_uuid("--successor-of")?,
    };
    let pool = pool().await?;
    let receipt = routes::register_profile(&pool, &request, &admin.action()).await?;
    Ok(Output::ok(to_json(&receipt)?))
}

/// `reasoning bind` (ADR-0060 D-H 2): the R2 projection of one (domain, purpose) onto one
/// Profile@version; a rebind writes the successor Binding@version.
async fn reasoning_bind(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let purpose_text = args.required("--purpose")?;
    let purpose = routes::parse_purpose(&purpose_text).ok_or_else(|| {
        Failure::Usage(format!(
            "--purpose: {purpose_text:?} is not a §11.2.3 purpose"
        ))
    })?;
    let pool = pool().await?;
    let receipt = routes::bind_domain(
        &pool,
        uuid_flag(args, "--tenant")?,
        uuid_flag(args, "--domain")?,
        purpose,
        profile_flags(args)?,
        &admin.action(),
    )
    .await?;
    Ok(Output::ok(to_json(&receipt)?))
}

/// `reasoning attest-health` (ADR-0060 D-H 3, ruling E3 (a)): `--valid-for-secs` has no default.
async fn reasoning_attest_health(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let pool = pool().await?;
    let receipt = routes::attest_health(
        &pool,
        uuid_flag(args, "--tenant")?,
        profile_flags(args)?,
        args.parsed("--valid-for-secs")?,
        &admin.action(),
    )
    .await?;
    Ok(Output::ok(to_json(&receipt)?))
}

/// `reasoning profile-state --enabled true|false` (ADR-0060 D-H 4).
async fn reasoning_profile_state(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let pool = pool().await?;
    let receipt = routes::set_profile_enabled(
        &pool,
        uuid_flag(args, "--tenant")?,
        profile_flags(args)?,
        args.parsed("--enabled")?,
        &admin.action(),
    )
    .await?;
    Ok(Output::ok(to_json(&receipt)?))
}

/// `reasoning status --tenant` (ruling E3 (c)): read-only.
async fn reasoning_status(args: &Args) -> Result<Output> {
    let tenant_id = uuid_flag(args, "--tenant")?;
    let pool = pool().await?;
    Ok(Output::ok(routes::route_status(&pool, tenant_id).await?))
}

async fn run(args: Args) -> Result<Output> {
    let words: Vec<&str> = args.0.iter().take(2).map(String::as_str).collect();
    match words.as_slice() {
        ["deploy-init", ..] => deploy_init(&args).await,
        ["onboard", "tenant"] => onboard_tenant(&args).await,
        ["onboard", "workspace"] => onboard_workspace(&args).await,
        ["onboard", "user"] => onboard_user(&args).await,
        ["apikey", "issue"] => apikey_issue(&args).await,
        ["apikey", "revoke"] => apikey_revoke(&args).await,
        ["placement", "ensure"] => placement_ensure(&args).await,
        ["collection", "ensure"] => collection_ensure().await,
        ["activate", ..] => activate(&args).await,
        ["status", ..] => status(&args).await,
        ["jobs", "requeue-dead"] => jobs_requeue_dead(&args).await,
        ["reasoning", "register"] => reasoning_register(&args).await,
        ["reasoning", "bind"] => reasoning_bind(&args).await,
        ["reasoning", "attest-health"] => reasoning_attest_health(&args).await,
        ["reasoning", "profile-state"] => reasoning_profile_state(&args).await,
        ["reasoning", "status"] => reasoning_status(&args).await,
        ["apikey", "pepper-epoch"] => match args.0.get(2).map(String::as_str) {
            Some("advance") => apikey_pepper_epoch(&args, true).await,
            Some("close") => apikey_pepper_epoch(&args, false).await,
            _ => Err(Failure::Usage(USAGE.to_owned())),
        },
        ["roles", "rotate"] => roles::rotate(&args).await,
        ["deploy-check", ..] => roles::deploy_check(&args).await,
        ["retention", "approve"] => retention::approve(&args).await,
        ["retention", "create-partitions"] => retention::create_partitions(&args).await,
        ["retention", "execute"] => retention::execute(&args).await,
        ["health", "serve"] => health_serve::serve().await,
        ["--serve"] => serve::serve().await,
        ["sweep", "once"] => serve::sweep_once(&args).await,
        _ => Err(Failure::Usage(USAGE.to_owned())),
    }
}

fn main() -> ExitCode {
    let args = Args(std::env::args().skip(1).collect());
    // ADR-0061 D-C: the exposition this process serves, at zero state, before any config is read.
    if args.0.first().is_some_and(|a| a == "--metrics-families") {
        print!("{}", health_serve::metrics_families());
        return ExitCode::SUCCESS;
    }
    // ADR-0062 D-S: `--serve` is a second resident mode with its own families; its zero state, also before config.
    if args.0 == ["--serve", "--metrics-families"] {
        print!("{}", serve::render_metrics());
        return ExitCode::SUCCESS;
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("humaux-maintenance: runtime: {e}");
            return ExitCode::from(1);
        }
    };
    match runtime.block_on(run(args)) {
        Ok(output) => {
            println!("{}", output.render());
            ExitCode::from(match (output.refused, output.failed) {
                (true, _) => 3,
                (false, true) => 1,
                (false, false) => 0,
            })
        }
        Err(Failure::Usage(message)) => {
            eprintln!("humaux-maintenance: {message}");
            ExitCode::from(2)
        }
        Err(Failure::Infra(message)) => {
            eprintln!("humaux-maintenance: {message}");
            ExitCode::from(1)
        }
        Err(Failure::Provisioning(error)) => {
            let code = error.exit_code();
            if let ProvisioningError::Refused(reason) = &error {
                println!("{}", json!({ "outcome": "refused", "reason": reason }));
            } else {
                eprintln!("humaux-maintenance: {error}");
            }
            ExitCode::from(u8::try_from(code).unwrap_or(1))
        }
    }
}
