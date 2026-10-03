//! `xtask::e2e_seed` — persistent tenant/credential/quota seed for deployment-point rehearsals.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-protocol, postgres, rand, time, tokio, uuid];
//!   services=[PostgreSQL(any) w=[control.api_keys, control.audit_events, control.credentials,
//!   control.entitlement_snapshots, control.memberships, control.private_reasoning_domains, control.processor_models,
//!   control.provider_accounts, control.provider_endpoints, control.quota_windows,
//!   control.reasoning_credential_bindings, control.reasoning_profiles, control.reasoning_route_bindings,
//!   control.reasoning_route_candidates, control.reasoning_route_policies,
//!   control.retrieval_provider_admission_limits, control.tenants, control.user_emails, control.users,
//!   control.workspace_memberships, control.workspaces, ops.data_disclosure_sources, ops.data_disclosures, ops.jobs,
//!   ops.model_call_ledger, ops.outbox, ops.reasoning_account_health_observations,
//!   ops.reasoning_provider_health_observations, ops.retrieval_provider_budget_allocations,
//!   ops.retrieval_provider_budget_reservations, private.events, private.evidence_objects, private.memory_evidence,
//!   private.memory_records, private.processing_runs, private.retrieval_query_sources, projection.family_activations,
//!   projection.private_memory_points, projection.stream_checkpoints, projection.stream_log,
//!   projection.tenant_placements] x=[control.onboard_tenant, control.resolve_user_reasoning_admission],
//!   PostgreSQL(role_maintenance)]; env=[HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_TEST_PG_DSN];
//!   modules=[adapters::membership_repo, adapters::postgres, adapters::provisioning, adapters::quota_repo,
//!   domain::identity, domain::ids, domain::ticket_family, protocol::edge]
//! Called-by: [xtask::e2e_onboard, xtask::main]
//! Invariants: [a thin wrapper over adapters::provisioning (the same onboarding doors humaux-maintenance uses, no INSERT
//!   of its own for tenant/workspace/key/tier/placement/collection); refuses any DSN host but 127.0.0.1 and any database
//!   not named humaux_thread_*; --teardown removes seeded rows explicitly]
//! Spec: Baseline §73.5; ADR-0053; ADR-0059
//!
//! xtask `e2e-seed` — persistent tenant/credential/quota seed for deployment-point
//! rehearsals (an ops tool, not a test fixture: rows outlive the process, teardown is
//! explicit via `--teardown`).
//!
//! Reuses the real primitives instead of re-deriving them: `humaux_protocol::edge::
//! compute_api_key_hash` (§73.5 keyed hash: `HMAC-SHA256(pepper, "<prefix>.<secret>")`)
//! and `humaux_adapters::quota_repo::issue_window` (the only maintenance-role path that
//! may write `control.quota_windows`). The base six rows mirror
//! `crates/adapters/tests/support/operation_receipt_fixture.rs`'s `isolate()`; the
//! PRIVATE_CONSOLIDATE R3 admission lane mirrors `bins/consolidation-worker/tests/
//! consolidation_hop_e2e.rs::setup_db` verbatim (2026-09-03 addition — the second
//! rehearsal hop needs a resolvable admission lane, not just a bearer); the same profile
//! also gets a second policy/candidate/binding for purpose `PRIVATE_DISTILL_TEXT` (ADR-0016:
//! the private worker resolves it by purpose, so nothing new is printed). The 追加2 lane
//! (same date) provisions the semantic-recall placement the third/fourth rehearsal hop needs:
//! a Qdrant collection (mirrors `bins/gateway/tests/semantic_recall_wiring.rs::create_collection`,
//! reusing `humaux_adapters::qdrant`'s body constructors and the real
//! `IntraCellResource::QDRANT_REST` transport, never a hand-rolled HTTP client) plus its
//! `projection.tenant_placements` row — without both, `tenant_placement(...)` resolves to
//! `None` and semantic recall degrades closed with `DependencyUnavailable`.
//!
//! Card 27 (ADR-0052): `--workspaces <n>` (default 1) adds workspaces 2..=n to the seeded
//! tenant, each with its own membership and workspace-bound key, printed as `workspace_id_<k>:` /
//! `bearer_<k>:` lines (the rehearsal's 3 tenants × 2 workspaces). The retrieval worker's
//! tenant/scope/collection exports are gone: its `--serve` / `--run-once` claim tickets across
//! tenants and read each ticket's placement from the claim.
//!
//! Card 28 (ADR-0053 D-G): the tenant, workspaces, keys, tiers, placement, collection and the
//! VerifiedEmpty first activation now come from `humaux_adapters::provisioning` — the doors
//! `humaux-maintenance` uses — so a seeded workspace is READY and serving before any write, and
//! a later `xtask projection-serve` for the same version prints "already serving". Only the BYOK
//! distill lane (`seed_lane`, TEST health rows) stays seed-only (card 52). Printed lines are
//! unchanged.
//!
//! Card 32 (ADR-0058 M8 rehearsal twin): `--second-domain` adds a second user (through
//! `provisioning::onboard_user`, a member of the tenant and its base workspace), the reasoning
//! domain that user owns, that user's own seed lane and key (`bearer_d2:`), so remember.put lands
//! one tenant's Evidence in two domains. The domain row is the one seed-only INSERT besides the
//! lane (no provisioning door creates a second domain today).
//!
//! Card 33 (ADR-0059 D-I): `--credential-env <ENV_NAME>` (required) names the variable that holds
//! the rehearsal's provider key; the seed prints one `export HUMAUX_PRIVATE_WORKER_CREDENTIALS=`
//! line mapping every lane it created (the base lane and, with `--second-domain`, the second one)
//! to that name. Only names and references are printed, never a key.
//!
//! Refuses to run against anything but a local disposable database (binding rule): DSN
//! host must be `127.0.0.1` and the database name must start with `humaux_thread_`.

use std::fmt::Write as _;

use humaux_adapters::membership_repo::AdminAction;
use humaux_adapters::postgres::MaintenanceDbPool;
use humaux_adapters::provisioning::{self, NewApiKey, QdrantFace, TenantReceipt, TenantRequest};
use humaux_adapters::quota_repo;
use humaux_domain::identity::MembershipRole;
use humaux_domain::ids::TenantId;
use humaux_domain::ticket_family::TicketFamily;
use humaux_protocol::edge::{api_key_log_fingerprint, compute_api_key_hash};
use postgres::{Client, NoTls};
use rand::Rng;
use uuid::Uuid;

const DSN_ENV: &str = "HUMAUX_TEST_PG_DSN";
const MAINTENANCE_DSN_ENV: &str = "HUMAUX_MAINTENANCE_PG_DSN";
const DEFAULT_LIMIT: i64 = 1000;

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

/// `host`, `database name` out of a `postgres://user:pass@host:port/db?query` DSN. No `url`
/// crate in this workspace for this shape — the split is a few lines, not worth a dependency
/// (ponytail rung 3: stdlib string ops suffice).
fn parse_host_and_db(dsn: &str) -> Option<(String, String)> {
    let after_scheme = dsn.split_once("://")?.1;
    let after_at = after_scheme
        .rsplit_once('@')
        .map_or(after_scheme, |(_, r)| r);
    let (hostport, rest) = after_at.split_once('/')?;
    let host = hostport.split(':').next().unwrap_or(hostport).to_string();
    let db = rest.split(['?', '#']).next().unwrap_or(rest).to_string();
    Some((host, db))
}

/// Binding rule: never touch anything but a local disposable `humaux_thread_*` database.
/// Applied to every writing DSN this tool holds (owner AND maintenance — P0 fix: a
/// misconfigured maintenance DSN must refuse too, not just the owner one), so the env var
/// name is a parameter, not the `DSN_ENV` constant, or a maintenance-DSN failure would blame
/// the wrong variable in the error message.
fn guard_local_test_db(env_name: &str, dsn: &str) -> Result<(), String> {
    let (host, db) = parse_host_and_db(dsn)
        .ok_or_else(|| format!("cannot parse host/database out of ${env_name}"))?;
    if host != "127.0.0.1" {
        return Err(format!(
            "refusing: ${env_name} host is {host:?}, must be 127.0.0.1 (never production)"
        ));
    }
    if !db.starts_with("humaux_thread_") {
        return Err(format!(
            "refusing: ${env_name} database {db:?} does not start with \"humaux_thread_\" \
             (never production)"
        ));
    }
    Ok(())
}

/// 32 alphanumeric chars, drawn from the workspace's existing `rand` line (no hand-rolled
/// RNG — binding rule).
fn random_secret() -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..32)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect()
}

/// `Display` on `postgres::Error` can collapse to a bare "db error" — the server's actual
/// complaint lives in `DbError` (same shape `migrate.rs` already uses for this).
fn db_detail(e: &postgres::Error) -> String {
    e.as_db_error()
        .map(|db| format!("{} — {}", db.message(), db.detail().unwrap_or("")))
        .unwrap_or_else(|| format!("{e:?}"))
}

fn random_hex_suffix(rng: &mut impl Rng, n: usize) -> String {
    let mut out = String::with_capacity(n);
    for _ in 0..n {
        let _ = write!(out, "{:x}", rng.random_range(0..16u8));
    }
    out
}

/// Flags for the PRIVATE_CONSOLIDATE R3 admission lane (§78.1: no literal defaults —
/// every provider-shaped value comes from the CLI, none baked in).
pub(crate) struct LaneFlags {
    pub(crate) egress_processor_id: Uuid,
    pub(crate) region: String,
    pub(crate) service_tier: String,
    pub(crate) endpoint_ref: String,
    pub(crate) provider_id: String,
    pub(crate) provider_model_id: String,
    pub(crate) model_revision: String,
}

fn parse_lane_flags(args: &[String]) -> Result<LaneFlags, String> {
    let get = |flag: &str| {
        arg(args, flag).ok_or_else(|| format!("missing required flag {flag} (§78.1: no default)"))
    };
    let egress_processor_id = get("--processor-id")?
        .parse::<Uuid>()
        .map_err(|e| format!("--processor-id must be a uuid: {e}"))?;
    Ok(LaneFlags {
        egress_processor_id,
        region: get("--region")?,
        service_tier: get("--service-tier")?,
        endpoint_ref: get("--endpoint-ref")?,
        provider_id: get("--provider-id")?,
        provider_model_id: get("--provider-model-id")?,
        model_revision: get("--model-revision")?,
    })
}

/// The lane's own ids, printed alongside the base six + used to build the two paste-ready
/// env blocks the 2026-09-03 addition asks for.
pub(crate) struct LaneSeed {
    binding_id: Uuid,
    binding_version: i64,
    /// ADR-0016 D7: the `PRIVATE_DISTILL_TEXT` binding over the same profile — resolved by
    /// purpose at runtime, printed only for teardown bookkeeping.
    distill_binding_id: Uuid,
    /// The lane's credential reference — the key the private worker's credential map is keyed
    /// on (ADR-0059 D-I).
    pub(crate) credential_id: Uuid,
    provider_account_id: Uuid,
    processor_model_id: Uuid,
    endpoint_id: Uuid,
    profile_id: Uuid,
    policy_id: Uuid,
}

/// One route policy (pinned candidate over `profile_id`, promoted SHADOW → SERVING) + its
/// binding for `purpose` — the tail of the lane graph, shared by the PRIVATE_CONSOLIDATE and
/// PRIVATE_DISTILL_TEXT purposes (same profile, same provider). Returns
/// `(route_policy_id, binding_id)`.
fn seed_route(
    txn: &mut postgres::Transaction<'_>,
    tenant_id: Uuid,
    user_id: Uuid,
    reasoning_domain_id: Uuid,
    profile_id: Uuid,
    purpose: &str,
) -> Result<(Uuid, Uuid), String> {
    let policy_id: Uuid = txn
        .query_one(
            "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,$3) RETURNING route_policy_id",
            &[&tenant_id, &user_id, &purpose],
        )
        .map_err(|e| format!("insert route policy ({purpose}): {}", db_detail(&e)))?
        .get(0);
    txn.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) \
         VALUES($1,$2,1,$3,1,0)",
        &[&tenant_id, &policy_id, &profile_id],
    )
    .map_err(|e| format!("insert route candidate ({purpose}): {}", db_detail(&e)))?;
    txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
        &[&policy_id],
    )
    .map_err(|e| format!("promote policy to shadow ({purpose}): {}", db_detail(&e)))?;
    txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SERVING' WHERE route_policy_id=$1 AND policy_version=1",
        &[&policy_id],
    )
    .map_err(|e| format!("promote policy to serving ({purpose}): {}", db_detail(&e)))?;
    let binding_id: Uuid = txn
        .query_one(
            "INSERT INTO control.reasoning_route_bindings(tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) \
             VALUES($1,$2,$3,$4,1) RETURNING binding_id",
            &[&tenant_id, &reasoning_domain_id, &purpose, &policy_id],
        )
        .map_err(|e| format!("insert route binding ({purpose}): {}", db_detail(&e)))?
        .get(0);
    Ok((policy_id, binding_id))
}

/// Mirrors `bins/consolidation-worker/tests/consolidation_hop_e2e.rs::setup_db`'s R3
/// admission-lane graph verbatim (2026-09-03 card addition — 逐字镜像, not a rederivation).
#[allow(clippy::too_many_lines)]
pub(crate) fn seed_lane(
    client: &mut Client,
    tenant_id: Uuid,
    user_id: Uuid,
    reasoning_domain_id: Uuid,
    flags: &LaneFlags,
) -> Result<LaneSeed, String> {
    const PURPOSE: &str = "PRIVATE_CONSOLIDATE";
    const DISTILL_PURPOSE: &str = "PRIVATE_DISTILL_TEXT";
    let mut txn = client
        .transaction()
        .map_err(|e| format!("begin lane txn: {}", db_detail(&e)))?;

    let credential_id: Uuid = txn
        .query_one(
            "INSERT INTO control.credentials(tenant_id,purpose,openbao_ref) VALUES($1,'USER_REASONING',$2) RETURNING credential_id",
            &[&tenant_id, &format!("openbao://e2e-seed/{}", Uuid::new_v4())],
        )
        .map_err(|e| format!("insert credential: {}", db_detail(&e)))?
        .get(0);

    txn.execute(
        "INSERT INTO control.processor_models(processor_id,provider_model_id,model_revision,capabilities,status,catalog_observed_at) \
         VALUES($1,$2,$3,ARRAY['TEXT','STRUCTURED_OUTPUT'],'ACTIVE',clock_timestamp()) ON CONFLICT DO NOTHING",
        &[&flags.provider_id, &flags.provider_model_id, &flags.model_revision],
    )
    .map_err(|e| format!("insert processor model: {}", db_detail(&e)))?;
    let processor_model_id: Uuid = txn
        .query_one(
            "SELECT processor_model_id FROM control.processor_models \
             WHERE processor_id=$1 AND provider_model_id=$2 AND model_revision=$3 AND status='ACTIVE'",
            &[&flags.provider_id, &flags.provider_model_id, &flags.model_revision],
        )
        .map_err(|e| format!("select processor model: {}", db_detail(&e)))?
        .get(0);

    let account_hash: Vec<u8> = Uuid::new_v4().as_bytes().repeat(2);
    let provider_account_id: Uuid = txn
        .query_one(
            "INSERT INTO control.provider_accounts(tenant_id,owner_user_id,processor_id,external_account_ref_hash) \
             VALUES($1,$2,$3,$4) RETURNING provider_account_id",
            &[&tenant_id, &user_id, &flags.provider_id, &account_hash],
        )
        .map_err(|e| format!("insert provider account: {}", db_detail(&e)))?
        .get(0);
    txn.execute(
        "INSERT INTO control.reasoning_credential_bindings(credential_ref,tenant_id,owner_user_id,provider_account_id,processor_id) \
         VALUES($1,$2,$3,$4,$5)",
        &[&credential_id, &tenant_id, &user_id, &provider_account_id, &flags.provider_id],
    )
    .map_err(|e| format!("insert credential binding: {}", db_detail(&e)))?;
    let endpoint_id: Uuid = txn
        .query_one(
            "INSERT INTO control.provider_endpoints(tenant_id,provider_account_id,region,service_tier,endpoint_ref,egress_processor_id) \
             VALUES($1,$2,$3,$4,$5,$6) RETURNING endpoint_id",
            &[&tenant_id, &provider_account_id, &flags.region, &flags.service_tier, &flags.endpoint_ref, &flags.egress_processor_id],
        )
        .map_err(|e| format!("insert provider endpoint: {}", db_detail(&e)))?
        .get(0);
    let profile_id: Uuid = txn
        .query_one(
            "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,billing_account_id,default_billing_instrument_id,capabilities,processing_region) \
             VALUES($1,$2,$3,$4,$5,$6,NULL,NULL,ARRAY['TEXT'],$7) RETURNING profile_id",
            &[&tenant_id, &user_id, &provider_account_id, &endpoint_id, &processor_model_id, &credential_id, &flags.region],
        )
        .map_err(|e| format!("insert reasoning profile: {}", db_detail(&e)))?
        .get(0);
    let (policy_id, binding_id) = seed_route(
        &mut txn,
        tenant_id,
        user_id,
        reasoning_domain_id,
        profile_id,
        PURPOSE,
    )?;
    let (_, distill_binding_id) = seed_route(
        &mut txn,
        tenant_id,
        user_id,
        reasoning_domain_id,
        profile_id,
        DISTILL_PURPOSE,
    )?;
    txn.execute(
        "INSERT INTO ops.reasoning_provider_health_observations(tenant_id,processor_id,processor_model_id,provider_model_id,model_revision,provider_endpoint_id,endpoint_ref,region,service_tier,source_kind,reason_code,verdict,observed_at,valid_until) \
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'TEST',NULL,'HEALTHY',clock_timestamp()-interval '1 second',clock_timestamp()+interval '30 minutes')",
        &[&tenant_id, &flags.provider_id, &processor_model_id, &flags.provider_model_id, &flags.model_revision, &endpoint_id, &flags.endpoint_ref, &flags.region, &flags.service_tier],
    )
    .map_err(|e| format!("insert provider health observation: {}", db_detail(&e)))?;
    txn.execute(
        "INSERT INTO ops.reasoning_account_health_observations(tenant_id,provider_account_id,credential_ref,billing_account_id,billing_instrument_id,source_kind,reason_code,account_verdict,credential_verdict,billing_account_verdict,billing_instrument_verdict,observed_at,valid_until) \
         VALUES($1,$2,$3,NULL,NULL,'TEST',NULL,'HEALTHY','VALID',NULL,NULL,clock_timestamp()-interval '1 second',clock_timestamp()+interval '30 minutes')",
        &[&tenant_id, &provider_account_id, &credential_id],
    )
    .map_err(|e| format!("insert account health observation: {}", db_detail(&e)))?;

    txn.commit()
        .map_err(|e| format!("commit lane txn: {}", db_detail(&e)))?;

    Ok(LaneSeed {
        binding_id,
        binding_version: 1,
        distill_binding_id,
        credential_id,
        provider_account_id,
        processor_model_id,
        endpoint_id,
        profile_id,
        policy_id,
    })
}

/// What [`seed_second_domain`] provisioned: a second user of the seeded tenant, the ACTIVE
/// reasoning domain that user owns, and that user's key on the base workspace (`wire` is a secret,
/// printed only as `bearer_d2:`).
pub(crate) struct SecondDomain {
    user_id: Uuid,
    reasoning_domain_id: Uuid,
    wire: String,
    /// The second lane's credential reference (ADR-0059 D-I map entry).
    credential_id: Uuid,
}

/// ADR-0058 M8, rehearsal twin (card 32 `--second-domain`): one tenant whose Evidence lands in
/// TWO reasoning domains. §11.2.1 / `adapters::remember::resolve_reasoning_domain`: a put is
/// processed under the on-behalf-of user's own domain, so the second domain needs a second user
/// (member of the tenant and the base workspace, through the same `onboard_user` door
/// `humaux-maintenance` uses), that user's ACTIVE domain, its own admitted lane ([`seed_lane`],
/// owned by that user — the policy-owner check wants the domain owner) and its own key.
pub(crate) fn seed_second_domain(
    rt: &tokio::runtime::Runtime,
    maintenance: &MaintenanceDbPool,
    client: &mut Client,
    tenant: &TenantReceipt,
    flags: &LaneFlags,
    scopes: &[String],
    pepper: &[u8],
) -> Result<SecondDomain, String> {
    let user = rt
        .block_on(provisioning::onboard_user(
            maintenance,
            tenant.tenant_id,
            &format!("e2e-seed-d2-{}@e2e.invalid", Uuid::new_v4()),
            MembershipRole::Member,
            Some(tenant.workspace_id),
            &SEED_ADMIN,
        ))
        .map_err(|e| format!("onboard_user: {e}"))?;
    let reasoning_domain_id: Uuid = client
        .query_one(
            "INSERT INTO control.private_reasoning_domains(tenant_id,name,owner_user_id,status) \
             VALUES($1,'second',$2,'ACTIVE') RETURNING reasoning_domain_id",
            &[&tenant.tenant_id, &user.user_id],
        )
        .map_err(|e| format!("insert second reasoning domain: {}", db_detail(&e)))?
        .get(0);
    let lane = seed_lane(
        client,
        tenant.tenant_id,
        user.user_id,
        reasoning_domain_id,
        flags,
    )?;
    let (key, wire) = seed_key(pepper);
    rt.block_on(provisioning::issue_api_key(
        maintenance,
        tenant.tenant_id,
        user.user_id,
        tenant.workspace_id,
        &key,
        scopes,
        &SEED_ADMIN,
    ))
    .map_err(|e| format!("issue_api_key (second domain): {e}"))?;
    Ok(SecondDomain {
        user_id: user.user_id,
        reasoning_domain_id,
        wire,
        credential_id: lane.credential_id,
    })
}

/// ADR-0059 D-I: the private worker's `HUMAUX_PRIVATE_WORKER_CREDENTIALS` value for the seeded
/// lanes — every reference maps to `env_name`, the variable that holds the rehearsal's provider
/// key (a NAME, never a value; required, no default: a default would name a provider, §78.1).
fn credential_map_value(env_name: &str, refs: &[Uuid]) -> Result<String, String> {
    if env_name.is_empty()
        || !env_name
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err("--credential-env must name a variable matching [A-Z0-9_]+".to_owned());
    }
    Ok(refs
        .iter()
        .map(|r| format!("{r}={env_name}"))
        .collect::<Vec<_>>()
        .join(","))
}

/// Flags for the 追加2 semantic-recall placement lane (§78.1: dimension/collection/host/port
/// all come from the CLI, none baked in). `--qdrant-host` is restricted to `127.0.0.1` — same
/// binding rule as the two Postgres DSNs, this tool never dials a non-local Qdrant.
struct QdrantFlags {
    collection: String,
    dimension: u32,
    host: String,
    port: u16,
    /// §19 retrieval-provider admission is keyed by (provider_id, region, tenant, purpose):
    /// the embedding/rerank limit rows the projection/recall hops need (§78.1: from the CLI).
    embedding_provider: String,
    embedding_region: String,
}

fn parse_qdrant_flags(args: &[String]) -> Result<QdrantFlags, String> {
    let collection = arg(args, "--collection")
        .ok_or_else(|| "missing required flag --collection (§78.1: no default)".to_string())?;
    let dimension: u32 = arg(args, "--dimension")
        .ok_or_else(|| "missing required flag --dimension (§78.1: no default)".to_string())?
        .parse()
        .map_err(|e| format!("--dimension: {e}"))?;
    let host = arg(args, "--qdrant-host").unwrap_or_else(|| "127.0.0.1".to_string());
    if host != "127.0.0.1" {
        return Err(format!(
            "refusing: --qdrant-host {host:?} must be 127.0.0.1 (never production)"
        ));
    }
    let port: u16 = match arg(args, "--qdrant-port") {
        Some(v) => v.parse().map_err(|e| format!("--qdrant-port: {e}"))?,
        None => 6333,
    };
    let embedding_provider = arg(args, "--embedding-provider").ok_or_else(|| {
        "missing required flag --embedding-provider (§78.1: no default)".to_string()
    })?;
    let embedding_region = arg(args, "--embedding-region").ok_or_else(|| {
        "missing required flag --embedding-region (§78.1: no default)".to_string()
    })?;
    Ok(QdrantFlags {
        collection,
        dimension,
        host,
        port,
        embedding_provider,
        embedding_region,
    })
}

/// `--teardown --drop-collection <name>`: explicit-only deletion (default teardown never drops
/// the collection, per the card — it may be shared with another tenant's placement row).
fn drop_qdrant_collection(
    rt: &tokio::runtime::Runtime,
    host: &str,
    port: u16,
    collection: &str,
) -> Result<(), String> {
    let face = QdrantFace::new(host, port, &format!("{host}/32")).map_err(|e| e.to_string())?;
    rt.block_on(face.delete_collection(collection))
        .map_err(|e| e.to_string())
}

/// The seed's §77 operator identity (a local disposable database; the rows are torn down).
const SEED_ADMIN: AdminAction<'static> = AdminAction {
    actor: "xtask-e2e-seed",
    reason: "deployment-point rehearsal seed (local disposable database)",
    ticket: "e2e-seed",
    trace_id: "e2e-seed",
    step_up_auth_context: "local-127.0.0.1-only",
};

/// One `e2e…` API key: random prefix (the seed's printed shape), the §73.5 hash, the wire key.
fn seed_key(pepper: &[u8]) -> (NewApiKey, String) {
    let prefix = format!("e2e{}", random_hex_suffix(&mut rand::rng(), 12));
    let wire = format!("{prefix}.{}", random_secret());
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

/// What one seed run provisioned through the onboarding library (ADR-0053 D-G).
struct Seeded {
    tenant: TenantReceipt,
    api_key_id: Uuid,
    prefix: String,
    wire: String,
    /// Card 27 `--workspaces <n>`: `(workspace_id, wire)` of workspaces 2..=n.
    extra_workspaces: Vec<(Uuid, String)>,
}

/// The seed as a thin wrapper over `humaux_adapters::provisioning` — the same doors
/// `humaux-maintenance` uses, no INSERT of its own: deploy tiers (1e9, never torn down), the
/// tenant (random `e2e-seed-<uuid>` name, a throwaway `@e2e.invalid` owner), the quota window,
/// the extra workspaces with their own keys, the collection and the VerifiedEmpty activation of
/// every workspace (so the later `projection-serve` calls print "already serving").
#[allow(clippy::too_many_lines)] // the seed's onboarding sequence, one step per library call
async fn provision(
    maintenance: &MaintenanceDbPool,
    qdrant: &QdrantFlags,
    scopes: &[String],
    limit: i64,
    pepper: &[u8],
    workspaces: usize,
) -> Result<Seeded, String> {
    const SEED_TIER_LIMIT: i64 = 1_000_000_000;
    let run = Uuid::new_v4();
    provisioning::deploy_init(
        maintenance,
        &qdrant.embedding_provider,
        &qdrant.embedding_region,
        SEED_TIER_LIMIT,
        SEED_TIER_LIMIT,
        &SEED_ADMIN,
    )
    .await
    .map_err(|e| format!("deploy_init: {e}"))?;
    let now = time::OffsetDateTime::now_utc();
    let mut minted = None;
    let mut mint = |_tenant: Uuid| {
        let (key, wire) = seed_key(pepper);
        minted = Some((key.prefix.clone(), wire));
        key
    };
    let tenant = provisioning::onboard_tenant(
        maintenance,
        &TenantRequest {
            name: &format!("e2e-seed-{run}"),
            owner_email: &format!("e2e-seed-{run}@e2e.invalid"),
            workspace_name: "e2e-seed workspace",
            reasoning_domain_name: "default",
            plan_limit: limit,
            period_start: now - time::Duration::seconds(1),
            period_end: now + time::Duration::hours(1),
            provider_id: &qdrant.embedding_provider,
            region: &qdrant.embedding_region,
            tenant_tpm: SEED_TIER_LIMIT,
            tenant_rpm: SEED_TIER_LIMIT,
            scopes,
            collection: &qdrant.collection,
        },
        &mut mint,
        &SEED_ADMIN,
    )
    .await
    .map_err(|e| format!("onboard_tenant: {e}"))?;
    let api_key_id = tenant
        .api_key
        .as_ref()
        .map(|k| k.api_key_id)
        .ok_or("onboard_tenant created no key (tenant name collision?)")?;
    let (prefix, wire) = minted.ok_or("no key minted")?;
    quota_repo::issue_window(maintenance, TenantId(tenant.tenant_id))
        .await
        .map_err(|e| format!("issue_window: {e:?}"))?;

    let mut extra_workspaces = Vec::with_capacity(workspaces.saturating_sub(1));
    for k in 2..=workspaces {
        let workspace = provisioning::onboard_workspace(
            maintenance,
            tenant.tenant_id,
            &format!("e2e-seed workspace {k}"),
            tenant.owner_user_id,
            &SEED_ADMIN,
        )
        .await
        .map_err(|e| format!("onboard_workspace: {e}"))?;
        let (key, wire) = seed_key(pepper);
        provisioning::issue_api_key(
            maintenance,
            tenant.tenant_id,
            tenant.owner_user_id,
            workspace.workspace_id,
            &key,
            scopes,
            &SEED_ADMIN,
        )
        .await
        .map_err(|e| format!("issue_api_key: {e}"))?;
        extra_workspaces.push((workspace.workspace_id, wire));
    }

    let face = QdrantFace::new(&qdrant.host, qdrant.port, &format!("{}/32", qdrant.host))
        .map_err(|e| format!("qdrant face: {e}"))?;
    provisioning::ensure_collection(&face, &qdrant.collection, qdrant.dimension)
        .await
        .map_err(|e| format!("qdrant collection: {e}"))?;
    let workspace_ids = std::iter::once(tenant.workspace_id)
        .chain(extra_workspaces.iter().map(|(id, _)| *id))
        .collect::<Vec<_>>();
    for workspace_id in workspace_ids {
        let activation = provisioning::activate_workspace(
            maintenance,
            &face,
            tenant.tenant_id,
            workspace_id,
            qdrant.dimension,
            None,
            &SEED_ADMIN,
        )
        .await
        .map_err(|e| format!("activate: {e}"))?;
        if let Some(reason) = activation.refusal() {
            return Err(format!("activate {workspace_id}: refused {reason}"));
        }
    }
    Ok(Seeded {
        tenant,
        api_key_id,
        prefix,
        wire,
        extra_workspaces,
    })
}

/// Deletes one tenant's rows in dependency-reverse order — base fixture tables (mirrors
/// `operation_receipt_fixture.rs` Drop) then the lane (reverse of `seed_lane`, per the
/// 2026-09-03 addition's explicit order).
fn teardown(client: &mut Client, tenant_id: Uuid) -> Result<(), String> {
    let mut txn = client
        .transaction()
        .map_err(|e| format!("begin teardown txn: {}", db_detail(&e)))?;
    // `ops.reasoning_*_health_observations` are append-only by trigger (§11.2.4-11.2.5 R3).
    // Superuser test DSN can skip triggers for this row-forward teardown, same shape
    // `consolidation_hop_e2e.rs::Fixture::drop` uses for the same tables.
    txn.batch_execute("SET session_replication_role = replica")
        .map_err(|e| format!("set session_replication_role: {}", db_detail(&e)))?;

    // Captured before memberships are deleted below — the only way back to "which
    // users did this seed create" once the membership row is gone.
    let user_ids: Vec<Uuid> = txn
        .query(
            "SELECT user_id FROM control.memberships WHERE tenant_id=$1",
            &[&tenant_id],
        )
        .map_err(|e| format!("select seeded user ids: {}", db_detail(&e)))?
        .into_iter()
        .map(|r| r.get(0))
        .collect();

    // Captured before reasoning_profiles is deleted below — needed to tell whether this
    // tenant's processor_models row(s) go orphaned once its own lane is torn down (card's
    // "仅本次种的" clause: seed_lane's `ON CONFLICT DO NOTHING` means the row may be shared
    // with another tenant's seed run, so it is deleted only when no profile references it
    // any more, never unconditionally).
    let processor_model_ids: Vec<Uuid> = txn
        .query(
            "SELECT DISTINCT processor_model_id FROM control.reasoning_profiles WHERE tenant_id=$1",
            &[&tenant_id],
        )
        .map_err(|e| format!("select seeded processor_model ids: {}", db_detail(&e)))?
        .into_iter()
        .map(|r| r.get(0))
        .collect();

    // Lane, reverse dependency order. Every statement here takes exactly `$1 = tenant_id`.
    for sql in [
        "DELETE FROM ops.reasoning_account_health_observations WHERE tenant_id=$1",
        "DELETE FROM ops.reasoning_provider_health_observations WHERE tenant_id=$1",
        "DELETE FROM control.reasoning_route_bindings WHERE tenant_id=$1",
        "DELETE FROM control.reasoning_route_candidates WHERE tenant_id=$1",
        "DELETE FROM control.reasoning_route_policies WHERE tenant_id=$1",
        "DELETE FROM control.reasoning_profiles WHERE tenant_id=$1",
        "DELETE FROM control.provider_endpoints WHERE tenant_id=$1",
        "DELETE FROM control.reasoning_credential_bindings WHERE tenant_id=$1",
        "DELETE FROM control.provider_accounts WHERE tenant_id=$1",
        "DELETE FROM control.credentials WHERE tenant_id=$1",
    ] {
        txn.execute(sql, &[&tenant_id])
            .map_err(|e| format!("teardown ({sql}): {}", db_detail(&e)))?;
    }
    // control.processor_models has no tenant_id (global catalog, `ON CONFLICT DO NOTHING`
    // insert) — deleted here only for the rows this tenant's lane referenced AND that no
    // other tenant's reasoning_profiles row references any more (i.e. this run's own catalog
    // row going orphaned, never a shared one still in use elsewhere).
    txn.execute(
        "DELETE FROM control.processor_models pm WHERE pm.processor_model_id = ANY($1) \
         AND NOT EXISTS (SELECT 1 FROM control.reasoning_profiles rp WHERE rp.processor_model_id = pm.processor_model_id)",
        &[&processor_model_ids],
    )
    .map_err(|e| format!("teardown (processor_models): {}", db_detail(&e)))?;

    // Base fixture tables, reverse dependency order (mirrors operation_receipt_fixture.rs Drop).
    for sql in [
        "DELETE FROM projection.private_memory_points WHERE tenant_id=$1",
        "DELETE FROM projection.tenant_placements WHERE tenant_id=$1",
        "DELETE FROM projection.stream_log WHERE tenant_id=$1",
        // Card 28 (ADR-0053): the VerifiedEmpty receipts reference the checkpoint rows.
        "DELETE FROM projection.family_activations WHERE tenant_id=$1",
        "DELETE FROM projection.stream_checkpoints WHERE tenant_id=$1",
        // Distill-hop outputs (ADR-0016) the rehearsal wrote for this tenant after seeding:
        // disclosure receipts, memories + their PRIMARY links, processing runs, then the
        // outbox rows and the Evidence they announced.
        "DELETE FROM ops.data_disclosure_sources WHERE tenant_id=$1",
        "DELETE FROM ops.data_disclosures WHERE tenant_id=$1",
        "DELETE FROM ops.retrieval_provider_budget_allocations WHERE tenant_id=$1",
        "DELETE FROM ops.retrieval_provider_budget_reservations WHERE tenant_id=$1",
        "DELETE FROM ops.model_call_ledger WHERE tenant_id=$1",
        "DELETE FROM private.retrieval_query_sources WHERE tenant_id=$1",
        "DELETE FROM control.retrieval_provider_admission_limits WHERE tenant_id=$1",
        "DELETE FROM private.memory_evidence WHERE memory_id IN \
           (SELECT memory_id FROM private.memory_records WHERE tenant_id=$1)",
        "DELETE FROM private.memory_records WHERE tenant_id=$1",
        "DELETE FROM private.processing_runs WHERE tenant_id=$1",
        // Card 27: the 0164 enqueue triggers give every seeded tenant ops.jobs rows, and the
        // replica-mode delete below skips the ON DELETE CASCADE, so they were left orphaned.
        "DELETE FROM ops.jobs WHERE tenant_id=$1",
        "DELETE FROM ops.outbox WHERE tenant_id=$1",
        "DELETE FROM private.events WHERE event_id IN \
           (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id=$1)",
        "DELETE FROM private.evidence_objects WHERE tenant_id=$1",
        "DELETE FROM control.api_keys WHERE tenant_id=$1",
        "DELETE FROM control.entitlement_snapshots WHERE tenant_id=$1",
        "DELETE FROM control.quota_windows WHERE tenant_id=$1",
        // Card 28: replica mode skips the ON DELETE CASCADE from memberships/workspaces, and
        // onboarding now writes §77 audit rows for the tenant.
        "DELETE FROM control.workspace_memberships WHERE tenant_id=$1",
        "DELETE FROM control.audit_events WHERE tenant_id=$1",
        "DELETE FROM control.memberships WHERE tenant_id=$1",
        "DELETE FROM control.workspaces WHERE tenant_id=$1",
        "DELETE FROM control.private_reasoning_domains WHERE tenant_id=$1",
    ] {
        txn.execute(sql, &[&tenant_id])
            .map_err(|e| format!("teardown ({sql}): {}", db_detail(&e)))?;
    }
    txn.execute(
        "DELETE FROM control.user_emails WHERE user_id = ANY($1)",
        &[&user_ids],
    )
    .map_err(|e| format!("teardown user emails: {}", db_detail(&e)))?;
    txn.execute(
        "DELETE FROM control.users WHERE user_id = ANY($1)",
        &[&user_ids],
    )
    .map_err(|e| format!("teardown users: {}", db_detail(&e)))?;
    txn.execute(
        "DELETE FROM control.tenants WHERE tenant_id=$1",
        &[&tenant_id],
    )
    .map_err(|e| format!("teardown tenant: {}", db_detail(&e)))?;
    txn.batch_execute("SET session_replication_role = DEFAULT")
        .map_err(|e| format!("reset session_replication_role: {}", db_detail(&e)))?;

    txn.commit()
        .map_err(|e| format!("commit teardown txn: {}", db_detail(&e)))
}

/// `cargo xtask e2e-seed --pepper-hex <hex> --scopes <a,b> [--limit 1000] \
///   --processor-id <uuid> --region <s> --service-tier <s> --endpoint-ref <url> \
///   --provider-id <s> --provider-model-id <s> --model-revision <s> \
///   --collection <name> --dimension <u32> [--qdrant-host 127.0.0.1] [--qdrant-port 6333] \
///   --credential-env <ENV_NAME> [--workspaces <n>] [--second-domain]`
/// or `cargo xtask e2e-seed --teardown <tenant_id> [--drop-collection <name>] \
///   [--qdrant-host 127.0.0.1] [--qdrant-port 6333]`.
#[allow(clippy::too_many_lines)]
pub fn run(args: &[String]) -> i32 {
    let dsn = match std::env::var(DSN_ENV) {
        Ok(v) => v,
        Err(_) => {
            eprintln!("e2e-seed: fail (missing object: ${DSN_ENV} env var)");
            return 1;
        }
    };
    if let Err(e) = guard_local_test_db(DSN_ENV, &dsn) {
        eprintln!("e2e-seed: {e}");
        return 1;
    }

    // dep: PostgreSQL(any) — seed target database (HUMAUX_MAINTENANCE_PG_DSN/HUMAUX_TEST_PG_DSN)
    let mut client = match Client::connect(&dsn, NoTls) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("e2e-seed: fail (cannot connect to ${DSN_ENV}: {e})");
            return 1;
        }
    };

    if let Some(tenant_arg) = arg(args, "--teardown") {
        let tenant_id = match tenant_arg.parse::<Uuid>() {
            Ok(id) => id,
            Err(e) => {
                eprintln!("e2e-seed: fail (--teardown value not a uuid: {e})");
                return 1;
            }
        };
        if let Err(e) = teardown(&mut client, tenant_id) {
            eprintln!("e2e-seed: teardown fail ({e})");
            return 1;
        }
        // Explicit-only (default teardown never drops the collection — it may be shared with
        // another tenant's placement row).
        if let Some(collection) = arg(args, "--drop-collection") {
            let host = arg(args, "--qdrant-host").unwrap_or_else(|| "127.0.0.1".to_string());
            if host != "127.0.0.1" {
                eprintln!(
                    "e2e-seed: fail (refusing: --qdrant-host {host:?} must be 127.0.0.1 (never production))"
                );
                return 1;
            }
            let port: u16 = match arg(args, "--qdrant-port") {
                Some(v) => match v.parse() {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("e2e-seed: fail (--qdrant-port: {e})");
                        return 1;
                    }
                },
                None => 6333,
            };
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("e2e-seed: fail (tokio runtime: {e})");
                    return 1;
                }
            };
            if let Err(e) = drop_qdrant_collection(&rt, &host, port, &collection) {
                eprintln!("e2e-seed: teardown fail (drop-collection {collection}: {e})");
                return 1;
            }
        }
        eprintln!("e2e-seed: teardown pass ({tenant_id})");
        return 0;
    }

    let Some(pepper_hex) = arg(args, "--pepper-hex") else {
        eprintln!("e2e-seed: fail (missing required flag --pepper-hex — §78.1: no default)");
        return 1;
    };
    let pepper = match decode_hex(&pepper_hex) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("e2e-seed: fail (--pepper-hex: {e})");
            return 1;
        }
    };
    let Some(scopes) = arg(args, "--scopes") else {
        eprintln!("e2e-seed: fail (missing required flag --scopes — §78.1: no default)");
        return 1;
    };
    let limit: i64 = match arg(args, "--limit") {
        Some(v) => match v.parse() {
            Ok(n) => n,
            Err(e) => {
                eprintln!("e2e-seed: fail (--limit: {e})");
                return 1;
            }
        },
        None => DEFAULT_LIMIT,
    };
    let lane_flags = match parse_lane_flags(args) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("e2e-seed: fail ({e})");
            return 1;
        }
    };
    let qdrant_flags = match parse_qdrant_flags(args) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("e2e-seed: fail ({e})");
            return 1;
        }
    };
    let Some(credential_env) = arg(args, "--credential-env") else {
        eprintln!("e2e-seed: fail (missing required flag --credential-env — §78.1: no default)");
        return 1;
    };
    if let Err(e) = credential_map_value(&credential_env, &[]) {
        eprintln!("e2e-seed: fail ({e})");
        return 1;
    }

    // Card 27: `--workspaces <n>` (absent = 1, so every existing caller is unchanged).
    let workspaces = match arg(args, "--workspaces").map(|v| v.parse::<usize>()) {
        None => 1,
        Some(Ok(n)) if n >= 1 => n,
        Some(_) => {
            eprintln!("e2e-seed: fail (--workspaces must be a positive integer)");
            return 1;
        }
    };

    let maintenance_dsn = match std::env::var(MAINTENANCE_DSN_ENV) {
        Ok(v) => v,
        Err(_) => {
            eprintln!("e2e-seed: fail (missing object: ${MAINTENANCE_DSN_ENV} env var)");
            return 1;
        }
    };
    // Binding rule applies to every writing DSN this tool touches, not just the owner one
    // (P0: MaintenanceDbPool::connect only asserts current_user, not host/db-name).
    if let Err(e) = guard_local_test_db(MAINTENANCE_DSN_ENV, &maintenance_dsn) {
        eprintln!("e2e-seed: fail (${MAINTENANCE_DSN_ENV}: {e})");
        return 1;
    }
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("e2e-seed: fail (tokio runtime: {e})");
            return 1;
        }
    };
    // dep: PostgreSQL(role_maintenance) — seed target database (HUMAUX_MAINTENANCE_PG_DSN/HUMAUX_TEST_PG_DSN)
    let maintenance = match rt.block_on(MaintenanceDbPool::connect(&maintenance_dsn)) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("e2e-seed: fail (connect ${MAINTENANCE_DSN_ENV}: {e:?})");
            return 1;
        }
    };
    let scopes: Vec<String> = scopes
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();
    let seeded = match rt.block_on(provision(
        &maintenance,
        &qdrant_flags,
        &scopes,
        limit,
        &pepper,
        workspaces,
    )) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("e2e-seed: fail (onboarding: {e})");
            return 1;
        }
    };
    let base = &seeded.tenant;

    let lane = match seed_lane(
        &mut client,
        base.tenant_id,
        base.owner_user_id,
        base.reasoning_domain_id,
        &lane_flags,
    ) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("e2e-seed: fail (lane seed: {e})");
            return 1;
        }
    };

    // Card 32 `--second-domain`: the M8 rehearsal twin (one tenant, two reasoning domains).
    let second_domain = if args.iter().any(|a| a == "--second-domain") {
        match seed_second_domain(
            &rt,
            &maintenance,
            &mut client,
            base,
            &lane_flags,
            &scopes,
            &pepper,
        ) {
            Ok(d) => Some(d),
            Err(e) => {
                eprintln!("e2e-seed: fail (second domain: {e})");
                return 1;
            }
        }
    } else {
        None
    };

    println!("tenant_id: {}", base.tenant_id);
    println!("user_id: {}", base.owner_user_id);
    println!("workspace_id: {}", base.workspace_id);
    println!("reasoning_domain_id: {}", base.reasoning_domain_id);
    println!("api_key_id: {}", seeded.api_key_id);
    println!("api_key_prefix: {}", seeded.prefix);
    println!("Authorization: Bearer {}", seeded.wire);
    for (k, (workspace_id, wire)) in seeded.extra_workspaces.iter().enumerate() {
        println!("workspace_id_{}: {workspace_id}", k + 2);
        println!("bearer_{}: {wire}", k + 2);
    }
    if let Some(d) = &second_domain {
        println!("second_user_id: {}", d.user_id);
        println!("second_reasoning_domain_id: {}", d.reasoning_domain_id);
        println!("bearer_d2: {}", d.wire);
    }
    println!("binding_id: {}", lane.binding_id);
    println!("binding_version: {}", lane.binding_version);
    println!("distill_binding_id: {}", lane.distill_binding_id);
    println!("credential_id: {}", lane.credential_id);
    println!("provider_account_id: {}", lane.provider_account_id);
    println!("processor_model_id: {}", lane.processor_model_id);
    println!("endpoint_id: {}", lane.endpoint_id);
    println!("profile_id: {}", lane.profile_id);
    println!("policy_id: {}", lane.policy_id);
    println!("collection_name: {}", qdrant_flags.collection);
    println!("embedding_provider: {}", qdrant_flags.embedding_provider);
    println!("embedding_region: {}", qdrant_flags.embedding_region);
    println!("dimension: {}", qdrant_flags.dimension);
    println!();
    // ADR-0036: the consolidation worker no longer takes a (tenant, domain, binding) pair from
    // the environment — it claims work across tenants via `ops.claim_derived_work` and resolves
    // the route binding per claimed tenant. What is left is the dispatch knobs; the socket path,
    // TTLs and MAX_INPUTS stay with the deployment because the seed cannot know them.
    println!("export HUMAUX_CONSOLIDATION_WORKER_LEASE_SECS=120");
    println!("export HUMAUX_CONSOLIDATION_WORKER_BATCH=8");
    println!("export HUMAUX_CONSOLIDATION_WORKER_MAX_ATTEMPTS=5");
    println!();
    println!(
        "export HUMAUX_PRIVATE_WORKER_PROVIDER_ID={}",
        lane_flags.provider_id
    );
    println!(
        "export HUMAUX_PRIVATE_WORKER_MODEL_ID={}",
        lane_flags.provider_model_id
    );
    println!(
        "export HUMAUX_PRIVATE_WORKER_MODEL_REVISION={}",
        lane_flags.model_revision
    );
    println!(
        "export HUMAUX_PRIVATE_WORKER_CHAT_URL={}",
        lane_flags.endpoint_ref
    );
    println!("export HUMAUX_PRIVATE_WORKER_REGION={}", lane_flags.region);
    println!(
        "export HUMAUX_PRIVATE_WORKER_EGRESS_PROCESSOR_ID={}",
        lane_flags.egress_processor_id
    );
    let mut lane_refs = vec![lane.credential_id];
    lane_refs.extend(second_domain.as_ref().map(|d| d.credential_id));
    match credential_map_value(&credential_env, &lane_refs) {
        Ok(map) => println!("export HUMAUX_PRIVATE_WORKER_CREDENTIALS={map}"),
        Err(e) => {
            eprintln!("e2e-seed: fail ({e})");
            return 1;
        }
    }
    println!();
    println!(
        "export HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER={}",
        qdrant_flags.embedding_provider
    );
    println!(
        "export HUMAUX_RETRIEVAL_WORKER_REGION={}",
        qdrant_flags.embedding_region
    );
    println!(
        "export HUMAUX_RETRIEVAL_WORKER_DIMENSION={}",
        qdrant_flags.dimension
    );
    // Card 21, §7.4: the retrieval worker's own §7 egress identity. It used to build its
    // embedding provider with `ProcessorId(Uuid::nil())`, so every `ops.data_disclosures` row
    // it wrote named processor all-zeros. Emitted from the SAME `--processor-id` the private
    // worker's identity comes from, so the deployment has one value to set, not two.
    println!(
        "export HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID={}",
        lane_flags.egress_processor_id
    );
    println!(
        "export HUMAUX_GATEWAY_EMBEDDING_DIMENSION={}",
        qdrant_flags.dimension
    );
    println!();
    // Card 21, §78.1: the §15.1 ticket-family triple is EMITTED from the one closed set that
    // owns it (`domain::ticket_family::TicketFamily`), never typed into the rehearsal script.
    // The gateway is the issuer and the retrieval worker is the resolver; before this, the
    // script carried `DOMAIN=…; PKIND=…; PVER=…` by hand and the retrieval worker read three
    // env values of its own — three copies of a value that must be equal, with no runtime
    // signal when they are not (the worker just polls a stream nobody writes, forever). The
    // worker now derives its triple from `RetrievalFamily::PrivateMemoryV1`; the gateway's
    // write policy is deployment configuration (§78.1 keeps it a key), so the seed fills it
    // from the same source instead of leaving it to be hand-aligned.
    let ticket_family = TicketFamily::PrivateMemory;
    println!(
        "export HUMAUX_GATEWAY_REMEMBER_DOMAIN={}",
        ticket_family.domain()
    );
    println!(
        "export HUMAUX_GATEWAY_REMEMBER_PROJECTION_KIND={}",
        ticket_family.projection_kind()
    );
    println!(
        "export HUMAUX_GATEWAY_REMEMBER_PROJECTION_VERSION={}",
        ticket_family.projection_version()
    );
    println!();
    // ADR-0036: same for the distill hop — no tenant/domain in the environment, only the
    // dispatch knobs (ADR-0058 D-K). LEASE / HARD_DEADLINE remain deployment-side.
    println!("export HUMAUX_PRIVATE_WORKER_DISTILL_IN_FLIGHT=4");
    println!("export HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS=5");

    0
}

fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err("odd-length hex string".to_string());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        DSN_ENV, LaneFlags, MAINTENANCE_DSN_ENV, QdrantFlags, credential_map_value,
        guard_local_test_db, seed_lane, teardown,
    };
    use humaux_adapters::postgres::MaintenanceDbPool;
    use postgres::{Client, NoTls};
    use uuid::Uuid;

    /// ADR-0059 D-I: one `ref=NAME` entry per seeded lane; a lower-case, empty or punctuated name
    /// is refused. Fault: drop the name check ⇒ a map the worker refuses at boot is emitted.
    #[test]
    fn credential_map_value_names_each_lane_and_refuses_a_bad_name() {
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        assert_eq!(
            credential_map_value("HX33_KEY", &[a, b]).as_deref(),
            Ok(format!("{a}=HX33_KEY,{b}=HX33_KEY").as_str())
        );
        for bad in ["", "hx33_key", "HX33-KEY", "HX33=KEY"] {
            assert!(credential_map_value(bad, &[a]).is_err(), "{bad:?}");
        }
    }

    /// Card 28 fault (e): the guard stays — a hostname (even `localhost`) is refused, only the
    /// literal `127.0.0.1` and a `humaux_thread_*` database pass.
    #[test]
    fn guard_refuses_hostname_dsn() {
        assert!(
            guard_local_test_db(DSN_ENV, "postgres://u:p@localhost:5432/humaux_thread_x").is_err()
        );
        assert!(
            guard_local_test_db(DSN_ENV, "postgres://u:p@db.prod:5432/humaux_thread_x").is_err()
        );
        assert!(guard_local_test_db(DSN_ENV, "postgres://u:p@127.0.0.1:5432/prod").is_err());
        assert!(
            guard_local_test_db(DSN_ENV, "postgres://u:p@127.0.0.1:5432/humaux_thread_x").is_ok()
        );
    }

    /// Card 16 regression, seed side. Two `e2e-seed` invocations must produce two tenants that
    /// ONE private-worker process can serve: each with its OWN admitted `PRIVATE_DISTILL_TEXT`
    /// route and its OWN credential (no shared/global row standing in for a per-tenant one), and
    /// both under the SAME `egress_processor_id` — that field is the deployment's egress identity
    /// (`HUMAUX_PRIVATE_WORKER_EGRESS_PROCESSOR_ID`), and `provider_matches_admission` compares
    /// the two, so a second tenant seeded under a different `--processor-id` is admitted by
    /// nothing and its Distill hop defers forever.
    ///
    /// Card 28: the tenants come from the onboarding library over `HUMAUX_MAINTENANCE_PG_DSN`
    /// (the seed's own path), the lane from the owner DSN as before.
    ///
    /// Fault injection: pass a fresh `Uuid::new_v4()` as the second lane's `egress_processor_id`
    /// (what `rehearse.sh` did) and the last assertion goes red — which is exactly the soak's P0.
    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one linear fixture script (seed -> assert -> teardown); splitting it hides which teardown covers which seed"
    )]
    fn seeding_two_tenants_yields_two_independently_admitted_distill_routes() {
        let (Ok(dsn), Ok(maintenance_dsn)) =
            (std::env::var(DSN_ENV), std::env::var(MAINTENANCE_DSN_ENV))
        else {
            eprintln!(
                "e2e_seed test: not_applicable — {DSN_ENV} / {MAINTENANCE_DSN_ENV} unset, skipping"
            );
            return;
        };
        // dep: PostgreSQL(any) — seed target database (HUMAUX_MAINTENANCE_PG_DSN/HUMAUX_TEST_PG_DSN)
        let Ok(mut client) = Client::connect(&dsn, NoTls) else {
            eprintln!(
                "e2e_seed test: not_applicable — cannot reach Postgres at ${DSN_ENV}, skipping"
            );
            return;
        };
        if client
            .query_one(
                "SELECT to_regprocedure('control.onboard_tenant(text,uuid,text,text,bigint,timestamptz,timestamptz,text,text,bigint,bigint,text[])') IS NULL",
                &[],
            )
            .map(|row| row.get::<_, bool>(0))
            .unwrap_or(true)
        {
            eprintln!("e2e_seed test: not_applicable — migrations not applied, skipping");
            return;
        }
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        // dep: PostgreSQL(role_maintenance) — the seed's onboarding pool
        let maintenance = rt
            .block_on(MaintenanceDbPool::connect(&maintenance_dsn))
            .expect("maintenance pool");

        // One deployment: one egress processor, one endpoint. Unique per run so the assertions
        // read only this test's rows on a shared dev database.
        let egress = Uuid::new_v4();
        let run = Uuid::new_v4();
        let flags = |egress: Uuid| LaneFlags {
            egress_processor_id: egress,
            region: "cn-shanghai".to_string(),
            service_tier: "standard".to_string(),
            endpoint_ref: format!("https://xtask-e2e-seed-{run}.invalid/v1/chat/completions"),
            provider_id: format!("xtask-e2e-seed-{run}"),
            provider_model_id: "self-test-model".to_string(),
            model_revision: "self-test".to_string(),
        };
        // This test never touches Qdrant: it onboards through the PostgreSQL-only library steps
        // (`onboard_pg_only`); the Qdrant half of `provision` runs in `cargo xtask e2e-onboard`.
        let qdrant = QdrantFlags {
            collection: format!("xtask_e2e_seed_{run}"),
            dimension: 8,
            host: "127.0.0.1".to_string(),
            port: 1,
            embedding_provider: format!("xtask-e2e-seed-{run}"),
            embedding_region: "cn-shanghai".to_string(),
        };
        let scopes = vec!["memory:write".to_string()];

        let mut seeded = Vec::new();
        let mut lanes = Vec::new();
        for _ in 0..2 {
            let tenant = rt
                .block_on(onboard_pg_only(&maintenance, &qdrant, &scopes))
                .expect("onboard");
            let lane = seed_lane(&mut client, tenant.0, tenant.1, tenant.2, &flags(egress))
                .expect("seed lane");
            seeded.push(tenant);
            lanes.push(lane);
        }

        let verdict = (|| -> Result<(), String> {
            for ((tenant_id, _, reasoning_domain_id), lane) in seeded.iter().zip(&lanes) {
                // The resolver reads RLS-protected `control.*` rows: without the tenant context
                // the answer is an empty set for every tenant, which would make this assertion
                // vacuously red rather than a real verdict.
                client
                    .batch_execute(&format!("SET humaux.tenant_id = '{tenant_id}'"))
                    .map_err(|e| format!("set tenant context: {e}"))?;
                let binding_version: i64 = client
                    .query_one(
                        "SELECT binding_version FROM control.reasoning_route_bindings \
                         WHERE binding_id = $1",
                        &[&lane.distill_binding_id],
                    )
                    .map_err(|e| format!("read binding version: {e}"))?
                    .get(0);
                let rows: i64 = client
                    .query_one(
                        "SELECT count(*) FROM control.resolve_user_reasoning_admission($1,$2,$3,'PRIVATE_DISTILL_TEXT')",
                        &[&lane.distill_binding_id, &binding_version, reasoning_domain_id],
                    )
                    .map_err(|e| format!("resolve admission: {e}"))?
                    .get(0);
                if rows != 1 {
                    return Err(format!(
                        "tenant {tenant_id} has {rows} admitted PRIVATE_DISTILL_TEXT routes, want 1"
                    ));
                }
            }
            if lanes[0].distill_binding_id == lanes[1].distill_binding_id {
                return Err("the two tenants share one distill binding".to_string());
            }
            if lanes[0].credential_id == lanes[1].credential_id {
                return Err("the two tenants share one credential".to_string());
            }
            let egresses: Vec<Uuid> = client
                .query(
                    "SELECT DISTINCT egress_processor_id FROM control.provider_endpoints \
                     WHERE endpoint_id = ANY($1)",
                    &[&vec![lanes[0].endpoint_id, lanes[1].endpoint_id]],
                )
                .map_err(|e| format!("read egress processors: {e}"))?
                .into_iter()
                .map(|row| row.get(0))
                .collect();
            if egresses != vec![egress] {
                return Err(format!(
                    "the two tenants must share ONE deployment egress processor ({egress}), got \
                     {egresses:?} — a private worker holds a single \
                     HUMAUX_PRIVATE_WORKER_EGRESS_PROCESSOR_ID, so the odd one out is admitted by \
                     nothing and its distill defers forever (card 16)"
                ));
            }
            Ok(())
        })();

        for (tenant_id, _, _) in &seeded {
            if let Err(error) = teardown(&mut client, *tenant_id) {
                eprintln!("e2e_seed test teardown ({tenant_id}): {error}");
            }
        }
        // The deployment tiers of this run's unique provider are this test's own rows.
        let _ = client.execute(
            "DELETE FROM control.retrieval_provider_admission_limits WHERE provider_id = $1",
            &[&qdrant.embedding_provider],
        );
        verdict.expect("two seeded tenants must both be servable by one deployment");
    }

    /// The PostgreSQL half of [`provision`] (deploy tiers + one tenant), so the lane test above
    /// needs no Qdrant. Returns `(tenant, owner user, reasoning domain)`.
    async fn onboard_pg_only(
        maintenance: &MaintenanceDbPool,
        qdrant: &QdrantFlags,
        scopes: &[String],
    ) -> Result<(Uuid, Uuid, Uuid), String> {
        use humaux_adapters::provisioning::{self, TenantRequest};
        humaux_adapters::provisioning::deploy_init(
            maintenance,
            &qdrant.embedding_provider,
            &qdrant.embedding_region,
            1_000,
            1_000,
            &super::SEED_ADMIN,
        )
        .await
        .map_err(|e| e.to_string())?;
        let now = time::OffsetDateTime::now_utc();
        let run = Uuid::new_v4();
        let mut mint = |_| super::seed_key(&[7u8; 32]).0;
        let tenant = provisioning::onboard_tenant(
            maintenance,
            &TenantRequest {
                name: &format!("e2e-seed-{run}"),
                owner_email: &format!("e2e-seed-{run}@e2e.invalid"),
                workspace_name: "e2e-seed workspace",
                reasoning_domain_name: "default",
                plan_limit: 1000,
                period_start: now - time::Duration::seconds(1),
                period_end: now + time::Duration::hours(1),
                provider_id: &qdrant.embedding_provider,
                region: &qdrant.embedding_region,
                tenant_tpm: 1_000,
                tenant_rpm: 1_000,
                scopes,
                collection: &qdrant.collection,
            },
            &mut mint,
            &super::SEED_ADMIN,
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok((
            tenant.tenant_id,
            tenant.owner_user_id,
            tenant.reasoning_domain_id,
        ))
    }
}
