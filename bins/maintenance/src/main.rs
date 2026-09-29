//! `maintenance::main` — `humaux-maintenance`, the operator-write CLI (§4.2): onboarding, API keys, placement,
//!   activation.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-protocol, rand, serde, serde_json, time, tokio, uuid];
//!   services=[PostgreSQL(role_maintenance)]; env=[HUMAUX_MAINTENANCE_CREDENTIAL_PEPPER_HEX,
//!   HUMAUX_MAINTENANCE_EMBEDDING_DIMENSION, HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_MAINTENANCE_PRIVATE_MEMORY_COLLECTION,
//!   HUMAUX_MAINTENANCE_QDRANT_CIDR, HUMAUX_MAINTENANCE_QDRANT_HOST, HUMAUX_MAINTENANCE_QDRANT_PORT];
//!   modules=[adapters::membership_repo, adapters::postgres, adapters::provisioning, adapters::quota_repo,
//!   domain::identity, domain::ids, domain::ticket_family, protocol::edge]
//! Called-by: [process(humaux-maintenance)]
//! Invariants: [one-shot, one JSON receipt on stdout per run; exit 0 created/existing, 3 refused, 2 usage, 1
//!   infrastructure (PostgreSQL/Qdrant down); the wire key is printed once on stdout only when created, never on
//!   stderr or in a receipt; no flag or env var has a literal default]
//! Spec: Baseline §4.2; §6.2.2; §73.5; §77; §78.1; ADR-0053
//!
//! Subcommand mode (card 28; the resident `--serve` job is card 35). Every subcommand is
//! one-shot, idempotent (a re-run writes nothing and answers `existing`), and prints exactly ONE
//! JSON receipt on stdout. Exit codes (ADR-0053 D-F): 0 created/existing, 3 refused (a named
//! reason, nothing written), 2 usage, 1 infrastructure.
//!
//! Secrets: the pepper comes from the environment only. A newly minted API key is printed ONCE,
//! as the single line `Authorization: Bearer <prefix>.<secret>` on stdout before the JSON, only
//! when it was created — never on stderr, never inside a receipt, never on a re-run (a lost key
//! is revoked and reissued under a new `--key-name`). Receipts carry the log fingerprint only.
//!
//! Every writing subcommand requires the §77 Sensitive-Admin-Action fields `--actor --reason
//! --ticket --step-up-auth` (`--trace-id` optional; minted and printed in the receipt otherwise).
//! No flag has a literal default (§78.1) except the two names `--workspace` / `--reasoning-domain`
//! (`default`, the name the seed always used).

use std::process::ExitCode;

use humaux_adapters::membership_repo::AdminAction;
use humaux_adapters::postgres::MaintenanceDbPool;
use humaux_adapters::provisioning::{
    self, NewApiKey, ProvisioningError, QdrantFace, TenantRequest, WorkspaceActivation,
};
use humaux_adapters::quota_repo;
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
apikey issue|revoke | placement ensure | collection ensure | activate | status> [flags]";

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

/// A receipt plus whether it reports a refusal (exit 3) and the one-time wire line.
struct Output {
    receipt: Value,
    refused: bool,
    wire: Option<String>,
}

impl Output {
    fn ok(receipt: Value) -> Self {
        Self {
            receipt,
            refused: false,
            wire: None,
        }
    }

    fn activation(receipt: Value, activation: &WorkspaceActivation) -> Self {
        Self {
            receipt,
            refused: activation.refusal().is_some(),
            wire: None,
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
    output.wire = tenant.api_key.as_ref().filter(|k| k.created).and(wire);
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
    output.wire = receipt.created.then_some(wire);
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

async fn status(args: &Args) -> Result<Output> {
    let tenant_id = uuid_flag(args, "--tenant")?;
    let pool = pool().await?;
    Ok(Output::ok(provisioning::status(&pool, tenant_id).await?))
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
        _ => Err(Failure::Usage(USAGE.to_owned())),
    }
}

fn main() -> ExitCode {
    let args = Args(std::env::args().skip(1).collect());
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
            if let Some(wire) = output.wire {
                println!("Authorization: Bearer {wire}");
            }
            println!("{}", output.receipt);
            ExitCode::from(if output.refused { 3 } else { 0 })
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
