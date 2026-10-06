//! `xtask::e2e_seed` — persistent tenant/credential/quota seed for deployment-point rehearsals.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-protocol, humaux-testkit, postgres, rand, serde_json, time,
//!   tokio, uuid];
//!   services=[PostgreSQL(any) r=[control.audit_event_identity, control.audit_events, control.memberships,
//!   control.quota_windows, control.workspace_memberships, control.workspaces, ops.jobs, ops.model_call_identity,
//!   ops.schema_migrations, private.event_identity, public.c36_vanishing]
//!   w=[control.credentials, control.operation_receipts, control.private_reasoning_domains, control.processor_models,
//!   control.provider_accounts, control.provider_endpoints, control.reasoning_credential_bindings,
//!   control.reasoning_profiles, control.reasoning_route_bindings, control.reasoning_route_candidates,
//!   control.reasoning_route_policies, control.retrieval_provider_admission_limits, control.tenants,
//!   control.usage_reservations, control.user_emails, control.users, ops.model_call_ledger,
//!   ops.reasoning_account_health_observations, ops.reasoning_provider_health_observations,
//!   ops.selection_snapshot_items, ops.selection_snapshots, private.conversations, private.events,
//!   private.evidence_objects, private.memory_evidence, private.memory_records, private.messages]
//!   x=[control.onboard_tenant, control.resolve_user_reasoning_admission], PostgreSQL(role_maintenance)];
//!   env=[CARGO_MANIFEST_DIR, HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_TEST_PG_DSN];
//!   modules=[adapters::byok, adapters::byok::ssrf, adapters::membership_repo, adapters::postgres, adapters::provisioning, adapters::quota_repo,
//!   domain::identity, domain::ids, domain::ticket_family, protocol::edge, testkit::fixture_purge]
//! Called-by: [xtask::e2e_onboard, xtask::main]
//! Invariants: [a thin wrapper over adapters::provisioning (the same onboarding doors humaux-maintenance uses, no INSERT
//!   of its own for tenant/workspace/key/tier/placement/collection); refuses any DSN host but 127.0.0.1 and any database
//!   not named humaux_thread_*; --teardown is the one fixture purge (testkit::fixture_purge, ADR-0063 "Dev integrity
//!   finding") plus the seeded users with constraints enforced, so it leaves no FK orphan]
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
//! Card 33b (ADR-0060 D-A, E5): `--capabilities <CSV>` (required, §11.2 closed set) is what the
//! seeded Profile declares and what a catalog row this seed creates declares. An existing catalog
//! row narrower than that set is refused naming `--model-revision` (catalog rows are append-only, so
//! a wider set takes a new revision label).
//!
//! Card 33b (ADR-0060 D-J): `--account-ref <text>` (required) is the vendor account every lane of
//! this seed declares (`external_account_ref_hash = sha256(text)`): lanes whose references the
//! deployment maps to one key variable must name one vendor account or the private worker refuses
//! to boot. The seed no longer prints provider / model / endpoint / capability lines for the worker
//! (ADR-0060 D-C: those come from each route); it prints the deny-only
//! `HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS` (`<processor-id>=<endpoint host>`) and
//! `HUMAUX_PRIVATE_WORKER_REGIONS` lines its lanes need.
//!
//! Card 33b (ADR-0060 research amendment 1): `--request-extras <JSON object>` (required, `{}` for
//! none) is the Profile's vendor request fields; the adapter writes no vendor field itself.
//!
//! Card 33b (ADR-0060 D-H): `--no-lane` seeds the tenant, users, reasoning domains and keys only —
//! no catalog row, account, credential, endpoint, profile, policy, binding or health row, and no
//! private-worker export line. The routes then come from `humaux-maintenance reasoning register |
//! bind | attest-health`, the production doors (docs/ops/rehearse.sh does this).
//!
//! Refuses to run against anything but a local disposable database (binding rule): DSN
//! host must be `127.0.0.1` and the database name must start with `humaux_thread_`.

use std::fmt::Write as _;

use humaux_adapters::byok::ReasoningCapability;
use humaux_adapters::membership_repo::AdminAction;
use humaux_adapters::postgres::MaintenanceDbPool;
use humaux_adapters::provisioning::{self, NewApiKey, QdrantFace, TenantReceipt, TenantRequest};
use humaux_adapters::quota_repo;
use humaux_domain::identity::MembershipRole;
use humaux_domain::ids::TenantId;
use humaux_domain::ticket_family::TicketFamily;
use humaux_protocol::edge::{api_key_log_fingerprint, compute_api_key_hash};
use humaux_testkit::fixture_purge::purge_tenant_fixture_sql;
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
    /// §11.2: what the seeded Profile declares, and the catalog row's capabilities when this seed
    /// creates it (ADR-0060 E5: a wider set needs a new `--model-revision` label, the catalog is
    /// append-only).
    pub(crate) capabilities: Vec<ReasoningCapability>,
    /// ADR-0060 D-J: the vendor account every lane of this seed declares (its sha256 is the
    /// account's `external_account_ref_hash`).
    pub(crate) account_ref: String,
    /// ADR-0060 research amendment 1: the Profile's vendor request fields (`{}` for none); the
    /// adapter writes no vendor field itself, and the 0206 CHECK refuses an adapter-owned key.
    pub(crate) request_extras: serde_json::Map<String, serde_json::Value>,
}

/// `--capabilities TEXT,STRUCTURED_OUTPUT,...` over the §11.2 closed set (no default, §78.1).
fn parse_capabilities(raw: &str) -> Result<Vec<ReasoningCapability>, String> {
    let caps = raw
        .split(',')
        .map(|c| {
            ReasoningCapability::parse(c.trim())
                .ok_or_else(|| format!("--capabilities: {c:?} is outside the §11.2 closed set"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if caps.is_empty() {
        return Err("--capabilities must name at least one capability".to_owned());
    }
    Ok(caps)
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
        capabilities: parse_capabilities(&get("--capabilities")?)?,
        account_ref: Some(get("--account-ref")?)
            .filter(|r| !r.trim().is_empty())
            .ok_or("--account-ref must not be empty")?,
        request_extras: parse_request_extras(&get("--request-extras")?)?,
    })
}

/// `--request-extras <JSON object>` — required like `humaux-maintenance reasoning register`'s
/// (pass `{}` for none), so no vendor field is ever implied by its absence.
fn parse_request_extras(raw: &str) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    match serde_json::from_str(raw) {
        Ok(serde_json::Value::Object(map)) => Ok(map),
        Ok(_) => Err("--request-extras must be a JSON object".to_owned()),
        Err(e) => Err(format!("--request-extras: not JSON ({e})")),
    }
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

    let capabilities: Vec<&str> = flags.capabilities.iter().map(|c| c.as_str()).collect();
    txn.execute(
        "INSERT INTO control.processor_models(processor_id,provider_model_id,model_revision,capabilities,status,catalog_observed_at) \
         VALUES($1,$2,$3,$4,'ACTIVE',clock_timestamp()) ON CONFLICT DO NOTHING",
        &[&flags.provider_id, &flags.provider_model_id, &flags.model_revision, &capabilities],
    )
    .map_err(|e| format!("insert processor model: {}", db_detail(&e)))?;
    let (processor_model_id, covers): (Uuid, bool) = txn
        .query_one(
            "SELECT processor_model_id, $4::text[] <@ capabilities FROM control.processor_models \
             WHERE processor_id=$1 AND provider_model_id=$2 AND model_revision=$3 AND status='ACTIVE'",
            &[&flags.provider_id, &flags.provider_model_id, &flags.model_revision, &capabilities],
        )
        .map(|row| (row.get(0), row.get(1)))
        .map_err(|e| format!("select processor model: {}", db_detail(&e)))?;
    // ADR-0060 E5: catalog rows are append-only; an existing row narrower than --capabilities would
    // make the profile INSERT fail with a generic 0128 trigger error, so name the way out instead.
    if !covers {
        return Err(format!(
            "the catalog row ({}, {}, {}) declares fewer capabilities than --capabilities {}; \
             pass a new --model-revision label (e.g. caps-<sorted capabilities joined by .>)",
            flags.provider_id,
            flags.provider_model_id,
            flags.model_revision,
            capabilities.join(",")
        ));
    }

    // ADR-0060 D-J: one --account-ref = one vendor account across every lane that names it.
    let provider_account_id: Uuid = txn
        .query_one(
            "INSERT INTO control.provider_accounts(tenant_id,owner_user_id,processor_id,external_account_ref_hash) \
             VALUES($1,$2,$3,sha256(convert_to($4::text,'UTF8'))) RETURNING provider_account_id",
            &[&tenant_id, &user_id, &flags.provider_id, &flags.account_ref],
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
            "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,billing_account_id,default_billing_instrument_id,capabilities,processing_region,request_extras) \
             VALUES($1,$2,$3,$4,$5,$6,NULL,NULL,$7,$8,$9::text::jsonb) RETURNING profile_id",
            &[&tenant_id, &user_id, &provider_account_id, &endpoint_id, &processor_model_id, &credential_id, &capabilities, &flags.region, &serde_json::Value::Object(flags.request_extras.clone()).to_string()],
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
    /// The second lane's credential reference (ADR-0059 D-I map entry); `None` under `--no-lane`.
    credential_id: Option<Uuid>,
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
    flags: Option<&LaneFlags>,
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
    let lane = flags
        .map(|flags| {
            seed_lane(
                client,
                tenant.tenant_id,
                user.user_id,
                reasoning_domain_id,
                flags,
            )
        })
        .transpose()?;
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
        credential_id: lane.map(|l| l.credential_id),
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
            // The window must outlast the longest rehearsal that reuses this seed: the card-37 chain's rerun ran 67
            // minutes and every billable call after the 60th answered ENTITLEMENT_REQUIRED (ADR-0064 "Chain run 1");
            // the card-53 go-live soak is eight hours. A day; the plan limit, not the clock, is the quota under test.
            period_end: now + time::Duration::hours(24),
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

/// `--teardown`: the seeded tenant and every row that depends on it go through the one fixture purge
/// (`humaux_testkit::fixture_purge`, ADR-0063 "Dev integrity finding" — it replaced a hand-written replica-mode
/// DELETE list that left FK orphans on dev whenever a table was added). The users the seed onboarded are not tenant
/// rows: they are deleted afterwards in the same transaction with constraints enforced (the purge restores the
/// replication role), so a user still referenced elsewhere refuses instead of leaving an orphan. The global
/// `control.processor_models` catalog row of a lane stays (append-only, `processor_models_identity_immutable`).
fn teardown(client: &mut Client, tenant_id: Uuid) -> Result<(), String> {
    let purge = purge_tenant_fixture_sql(&tenant_id.to_string())?;
    let mut txn = client
        .transaction()
        .map_err(|e| format!("begin teardown txn: {}", db_detail(&e)))?;
    // Captured before the purge — the only way back to "which users did this seed create".
    let user_ids: Vec<Uuid> = txn
        .query(
            "SELECT user_id FROM control.memberships WHERE tenant_id=$1",
            &[&tenant_id],
        )
        .map_err(|e| format!("select seeded user ids: {}", db_detail(&e)))?
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    txn.batch_execute(&purge)
        .map_err(|e| format!("fixture purge: {}", db_detail(&e)))?;
    for sql in [
        "DELETE FROM control.user_emails WHERE user_id = ANY($1)",
        "DELETE FROM control.users WHERE user_id = ANY($1)",
    ] {
        txn.execute(sql, &[&user_ids])
            .map_err(|e| format!("teardown ({sql}): {}", db_detail(&e)))?;
    }
    txn.commit()
        .map_err(|e| format!("commit teardown txn: {}", db_detail(&e)))
}

/// `cargo xtask e2e-seed --pepper-hex <hex> --scopes <a,b> [--limit 1000] \
///   --processor-id <uuid> --region <s> --service-tier <s> --endpoint-ref <url> \
///   --provider-id <s> --provider-model-id <s> --model-revision <s> --capabilities <CSV> \
///   --account-ref <text> --request-extras <JSON object> --collection <name> --dimension <u32> [--qdrant-host 127.0.0.1] [--qdrant-port 6333] \
///   --credential-env <ENV_NAME> [--workspaces <n>] [--second-domain]`,
/// or the same with `--no-lane` (ADR-0060 D-H: only `--pepper-hex --scopes --processor-id` and the
/// Qdrant/collection flags; the lane flags and `--credential-env` are not read),
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
    // ADR-0060 D-H: `--no-lane` seeds tenant, users, domains and keys only; the reasoning routes are
    // then written by `humaux-maintenance reasoning register|bind|attest-health` (the rehearsal's way).
    let no_lane = args.iter().any(|a| a == "--no-lane");
    let lane_flags = if no_lane {
        None
    } else {
        match parse_lane_flags(args) {
            Ok(f) => Some(f),
            Err(e) => {
                eprintln!("e2e-seed: fail ({e})");
                return 1;
            }
        }
    };
    // Card 21: the retrieval worker's §7 egress identity is printed in both modes.
    let retrieval_processor_id = match arg(args, "--processor-id").map(|v| v.parse::<Uuid>()) {
        Some(Ok(id)) => id,
        Some(Err(e)) => {
            eprintln!("e2e-seed: fail (--processor-id must be a uuid: {e})");
            return 1;
        }
        None => {
            eprintln!("e2e-seed: fail (missing required flag --processor-id — §78.1: no default)");
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
    let credential_env = arg(args, "--credential-env");
    match (&lane_flags, &credential_env) {
        (Some(_), None) => {
            eprintln!(
                "e2e-seed: fail (missing required flag --credential-env — §78.1: no default)"
            );
            return 1;
        }
        (_, Some(env_name)) => {
            if let Err(e) = credential_map_value(env_name, &[]) {
                eprintln!("e2e-seed: fail ({e})");
                return 1;
            }
        }
        (None, None) => {}
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

    let lane = match lane_flags
        .as_ref()
        .map(|flags| {
            seed_lane(
                &mut client,
                base.tenant_id,
                base.owner_user_id,
                base.reasoning_domain_id,
                flags,
            )
        })
        .transpose()
    {
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
            lane_flags.as_ref(),
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
    if let Some(lane) = &lane {
        println!("binding_id: {}", lane.binding_id);
        println!("binding_version: {}", lane.binding_version);
        println!("distill_binding_id: {}", lane.distill_binding_id);
        println!("credential_id: {}", lane.credential_id);
        println!("provider_account_id: {}", lane.provider_account_id);
        println!("processor_model_id: {}", lane.processor_model_id);
        println!("endpoint_id: {}", lane.endpoint_id);
        println!("profile_id: {}", lane.profile_id);
        println!("policy_id: {}", lane.policy_id);
    }
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
    // ADR-0060 D-C / D-L: the deny-only lists the worker needs to serve these lanes; provider,
    // model, endpoint and capabilities come from each admitted route, never from the worker env.
    // Under `--no-lane` the deployment composes these from its `reasoning register` receipts.
    if let (Some(lane_flags), Some(lane), Some(credential_env)) =
        (&lane_flags, &lane, &credential_env)
    {
        match humaux_adapters::byok::ssrf::https_host(&lane_flags.endpoint_ref) {
            Ok(host) => println!(
                "export HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS={}={host}",
                lane_flags.egress_processor_id
            ),
            Err(e) => {
                eprintln!("e2e-seed: fail (--endpoint-ref host: {e})");
                return 1;
            }
        }
        println!("export HUMAUX_PRIVATE_WORKER_REGIONS={}", lane_flags.region);
        let mut lane_refs = vec![lane.credential_id];
        lane_refs.extend(second_domain.as_ref().and_then(|d| d.credential_id));
        match credential_map_value(credential_env, &lane_refs) {
            Ok(map) => println!("export HUMAUX_PRIVATE_WORKER_CREDENTIALS={map}"),
            Err(e) => {
                eprintln!("e2e-seed: fail ({e})");
                return 1;
            }
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
    println!("export HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID={retrieval_processor_id}");
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
    use humaux_adapters::byok::ReasoningCapability;
    use humaux_adapters::postgres::MaintenanceDbPool;
    use humaux_testkit::fixture_purge::{INTEGRITY_EDGES_SQL, purge_tenant_fixture_sql};
    use postgres::{Client, NoTls};
    use std::collections::BTreeMap;
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
    /// §11.2 closed set, no default (§78.1): an unknown or empty capability list is refused.
    #[test]
    fn capabilities_flag_parses_the_closed_set_only() {
        assert_eq!(
            super::parse_capabilities("TEXT, STRUCTURED_OUTPUT,JSON_OBJECT").unwrap(),
            vec![
                ReasoningCapability::Text,
                ReasoningCapability::StructuredOutput,
                ReasoningCapability::JsonObject
            ]
        );
        assert!(super::parse_capabilities("TEXT,VISIONS").is_err());
        assert!(super::parse_capabilities("").is_err());
    }

    /// ADR-0060 research amendment 1: `--request-extras` is a JSON object (`{}` for none); an
    /// array, a scalar or non-JSON is refused before any row is written.
    #[test]
    fn request_extras_flag_takes_a_json_object_only() {
        assert_eq!(
            super::parse_request_extras(r#"{"reasoning_split":true}"#).unwrap()["reasoning_split"],
            serde_json::Value::Bool(true)
        );
        assert!(super::parse_request_extras("{}").unwrap().is_empty());
        for bad in ["[]", "true", "", "{"] {
            assert!(super::parse_request_extras(bad).is_err(), "{bad:?}");
        }
    }

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
    /// both under the SAME `egress_processor_id` — that field is the recipient the deployment's
    /// `HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS` must list (ADR-0060 D-C), so a second tenant
    /// seeded under a different `--processor-id` is refused by a worker configured from the
    /// first seed's export line and its Distill hop parks.
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
            provider_id: format!("e2e-seed-{run}"),
            provider_model_id: "self-test-model".to_string(),
            model_revision: "self-test".to_string(),
            capabilities: vec![
                ReasoningCapability::Text,
                ReasoningCapability::StructuredOutput,
            ],
            account_ref: format!("e2e-seed-account-{run}"),
            request_extras: super::parse_request_extras(r#"{"xtask_seed_probe":true}"#)
                .expect("extras"),
        };
        // This test never touches Qdrant: it onboards through the PostgreSQL-only library steps
        // (`onboard_pg_only`); the Qdrant half of `provision` runs in `cargo xtask e2e-onboard`.
        let qdrant = QdrantFlags {
            collection: format!("xtask_e2e_seed_{run}"),
            dimension: 8,
            host: "127.0.0.1".to_string(),
            port: 1,
            embedding_provider: format!("e2e-seed-{run}"),
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
                // ADR-0060 research amendment 1: the Profile carries --request-extras verbatim.
                // Fault: drop the column from the INSERT ⇒ the profile stores `{}` and the
                // deployed worker sends no vendor field (e2e-onboard's MiniMax lane).
                let extras = client
                    .query_one(
                        "SELECT count(*), count(*) FILTER (WHERE request_extras <> \
                           '{\"xtask_seed_probe\":true}'::jsonb) \
                         FROM control.reasoning_profiles WHERE tenant_id = $1",
                        &[tenant_id],
                    )
                    .map_err(|e| format!("read profile extras: {e}"))?;
                let (profiles, wrong): (i64, i64) = (extras.get(0), extras.get(1));
                if profiles == 0 || wrong != 0 {
                    return Err(format!(
                        "tenant {tenant_id}: {wrong} of {profiles} profile(s) lack --request-extras"
                    ));
                }
            }
            // ADR-0060 E5: the catalog row this run created declares {TEXT, STRUCTURED_OUTPUT}; a
            // wider --capabilities on the same identity is refused naming --model-revision.
            // Fault: drop the `covers` refusal → the 0128 profile trigger's generic error instead.
            let wider = LaneFlags {
                capabilities: vec![
                    ReasoningCapability::Text,
                    ReasoningCapability::StructuredOutput,
                    ReasoningCapability::ToolCalls,
                ],
                ..flags(egress)
            };
            let (tenant_id, user_id, domain_id) = seeded[0];
            match seed_lane(&mut client, tenant_id, user_id, domain_id, &wider) {
                Err(error) if error.contains("--model-revision") => {}
                Err(error) => return Err(format!("wider caps refused without the flag: {error}")),
                Ok(_) => return Err("a narrower catalog row accepted a wider profile".to_string()),
            }
            if lanes[0].distill_binding_id == lanes[1].distill_binding_id {
                return Err("the two tenants share one distill binding".to_string());
            }
            if lanes[0].credential_id == lanes[1].credential_id {
                return Err("the two tenants share one credential".to_string());
            }
            // ADR-0060 D-J: one --account-ref names one vendor account in every lane, so lanes the
            // deployment maps to one key variable pass the worker's boot check. Fault: hash a
            // random value per lane (the pre-33b seed) ⇒ two hashes.
            let hashes: Vec<Vec<u8>> = client
                .query(
                    "SELECT DISTINCT external_account_ref_hash FROM control.provider_accounts \
                     WHERE provider_account_id = ANY($1)",
                    &[&vec![
                        lanes[0].provider_account_id,
                        lanes[1].provider_account_id,
                    ]],
                )
                .map_err(|e| format!("read account hashes: {e}"))?
                .into_iter()
                .map(|row| row.get(0))
                .collect();
            if hashes.len() != 1 {
                return Err(format!(
                    "one --account-ref must declare one vendor account, got {} hashes",
                    hashes.len()
                ));
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
                     {egresses:?} — the seed exports one recipient, so the odd one out is refused \
                     by the worker's deny-only list and its distill parks (card 16)"
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

    /// The catalog-driven integrity scan (`INTEGRITY_EDGES_SQL`, the generator `c36_dev_orphan_repair.sh scan` runs
    /// on dev): one `child|parent|constraint|n` or `identity|table|parent|n` line per violated edge; empty when the
    /// database is FK-consistent and holds no identity garbage.
    fn integrity_violations(client: &mut Client) -> Vec<String> {
        let statements: Vec<String> = client
            .query(
                &format!("SELECT scan_sql FROM ({INTEGRITY_EDGES_SQL}) g"),
                &[],
            )
            .expect("integrity edge generator")
            .iter()
            .map(|row| row.get(0))
            .collect();
        statements
            .iter()
            .flat_map(|sql| {
                client
                    .query(sql.as_str(), &[])
                    .expect("integrity scan statement")
            })
            .map(|row| row.get(0))
            .collect()
    }

    /// `table -> rows` with `tenant_id = tenant`, over every table that has the column (none named here).
    fn tenant_rows(client: &mut Client, tenant: Uuid) -> BTreeMap<String, i64> {
        let statements: Vec<(String, String)> = client
            .query(
                "SELECT c.oid::regclass::text, format('SELECT count(*) FROM %s WHERE tenant_id = $1', c.oid::regclass) \
                 FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'tenant_id' \
                   AND NOT a.attisdropped \
                 WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition",
                &[],
            )
            .expect("tenant_id tables")
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        statements
            .into_iter()
            .filter_map(|(table, sql)| {
                let n: i64 = client
                    .query_one(sql.as_str(), &[&tenant])
                    .expect("count")
                    .get(0);
                (n > 0).then_some((table, n))
            })
            .collect()
    }

    /// The runtime rows a rehearsal leaves after the seed (evidence + event and its identity row, memory + link,
    /// ledger row + identity, consumed reservation + operation receipt, selection snapshot + item, conversation +
    /// message). The receipt is written in replica mode (its 0128-style integrity trigger wants a whole committed
    /// request) and references only rows written here or by onboarding, so the fixture is FK-consistent.
    fn runtime_residue_sql(tenant: Uuid) -> String {
        format!(
            "WITH t AS (SELECT '{tenant}'::uuid AS tenant_id), \
             w AS (SELECT workspace_id FROM control.workspaces WHERE tenant_id = (SELECT tenant_id FROM t) LIMIT 1), \
             d AS (SELECT reasoning_domain_id FROM control.private_reasoning_domains \
                    WHERE tenant_id = (SELECT tenant_id FROM t) LIMIT 1), \
             q AS (SELECT entitlement_key, window_start FROM control.quota_windows \
                    WHERE tenant_id = (SELECT tenant_id FROM t) LIMIT 1), \
             u AS (SELECT user_id FROM control.memberships WHERE tenant_id = (SELECT tenant_id FROM t) LIMIT 1), \
             e AS (INSERT INTO private.evidence_objects (tenant_id, evidence_kind, payload_sha256, data_class, \
                     origin_class, visibility_class, reasoning_domain_id) \
                   SELECT (SELECT tenant_id FROM t), 'EVENT', sha256('purge'::bytea), 'PRIVATE', 'DirectUserInput', \
                     'TENANT_SHARED', reasoning_domain_id FROM d RETURNING evidence_id), \
             ev AS (INSERT INTO private.events (event_id, event_kind, payload) \
                    SELECT evidence_id, 'USER_MESSAGE', '{{}}'::jsonb FROM e RETURNING event_id), \
             m AS (INSERT INTO private.memory_records (tenant_id, memory_type, content, visibility_class, \
                     authority_class, confidence, status, asserted_at) \
                   SELECT tenant_id, 'NOTE', '{{}}'::jsonb, 'TENANT_SHARED', 'PrivateKnowledge', 0.9, 'active', now() \
                   FROM t RETURNING memory_id), \
             me AS (INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
                    SELECT m.memory_id, e.evidence_id, 'PRIMARY', 0 FROM m, e RETURNING 1), \
             l AS (INSERT INTO ops.model_call_ledger (tenant_id, provider, workspace_id) \
                   SELECT (SELECT tenant_id FROM t), 'purge-probe', workspace_id FROM w RETURNING 1), \
             r AS (INSERT INTO control.usage_reservations (request_id, tenant_id, principal_id, entitlement_key, \
                     window_start, operation, request_fingerprint, units, expires_at, status, finished_at) \
                   SELECT gen_random_uuid(), (SELECT tenant_id FROM t), u.user_id, q.entitlement_key, q.window_start, \
                     'memory.remember', encode(sha256('purge'::bytea), 'hex'), 1, now() + interval '1 hour', \
                     'CONSUMED', now() FROM q, u RETURNING 1), \
             s AS (INSERT INTO ops.selection_snapshots (tenant_id, query_fingerprint, expires_at) \
                   SELECT tenant_id, 'purge', now() + interval '1 hour' FROM t RETURNING selection_snapshot_id), \
             si AS (INSERT INTO ops.selection_snapshot_items (selection_snapshot_id, item_id, tenant_id, ordinal) \
                    SELECT s.selection_snapshot_id, gen_random_uuid(), (SELECT tenant_id FROM t), 0 FROM s RETURNING 1), \
             c AS (INSERT INTO private.conversations (tenant_id) SELECT tenant_id FROM t RETURNING conversation_id), \
             msg AS (INSERT INTO private.messages (tenant_id, conversation_id, event_id) \
                     SELECT (SELECT tenant_id FROM t), c.conversation_id, ev.event_id FROM c, ev RETURNING 1) \
             SELECT (SELECT count(*) FROM me) + (SELECT count(*) FROM l) + (SELECT count(*) FROM r) \
                  + (SELECT count(*) FROM si) + (SELECT count(*) FROM msg); \
             BEGIN; SET LOCAL session_replication_role = replica; \
             INSERT INTO control.operation_receipts (tenant_id, principal_id, operation, idempotency_key, \
               request_fingerprint, request_id, reservation_id, scope_kind, scope_id, domain, projection_kind, \
               projection_version, stream_seq, commit_seq, audit_event_id, replay_expires_at, evidence_id) \
             SELECT r.tenant_id, r.principal_id, r.operation, 'purge-probe', r.request_fingerprint, r.request_id, \
               r.reservation_id, 'tenant', r.tenant_id, 'private_memory', 'PRIVATE_MEMORY', 'v1', 1, 1, \
               (SELECT a.audit_event_id FROM control.audit_events a WHERE a.tenant_id = r.tenant_id LIMIT 1), \
               now() + interval '1 day', \
               (SELECT eo.evidence_id FROM private.evidence_objects eo WHERE eo.tenant_id = r.tenant_id LIMIT 1) \
             FROM control.usage_reservations r WHERE r.tenant_id = '{tenant}'; \
             COMMIT;"
        )
    }

    /// `private.event_identity` rows of `tenant`'s evidence (the identity table has no `tenant_id`).
    fn event_identity_rows(client: &mut Client, tenant: Uuid) -> i64 {
        client
            .query_one(
                "SELECT count(*) FROM private.event_identity i \
                 JOIN private.evidence_objects e ON e.evidence_id = i.event_id WHERE e.tenant_id = $1",
                &[&tenant],
            )
            .expect("event identity rows")
            .get(0)
    }

    /// Every migration body in file order on the throwaway `owner` is connected to, as
    /// `rls_check::tests::migrated_bodies` does (`migrate::run` reads a cwd-relative directory; the manifests are
    /// `migrate`'s own test).
    fn apply_every_migration(owner: &mut Client) {
        owner
            .batch_execute(
                "CREATE SCHEMA IF NOT EXISTS ops; CREATE TABLE IF NOT EXISTS ops.schema_migrations \
                 (migration_id text PRIMARY KEY, checksum text NOT NULL, \
                  applied_at timestamptz NOT NULL DEFAULT now())",
            )
            .expect("ledger bootstrap");
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../migrations");
        let mut bodies: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
            .expect("migrations dir")
            .map(|e| e.expect("entry").path())
            .filter(|p| p.extension().is_some_and(|x| x == "sql"))
            .collect();
        bodies.sort();
        for body in bodies {
            owner
                .batch_execute(&std::fs::read_to_string(&body).expect("migration body"))
                .unwrap_or_else(|e| panic!("apply {}: {e:?}", body.display()));
        }
    }

    /// A fixture tenant (`e2e-fixture …`, the purge's predicate) on the throwaway.
    fn fixture_tenant(owner: &mut Client, label: &str) -> Uuid {
        owner
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&format!("e2e-fixture purge {label}")],
            )
            .expect("fixture tenant")
            .get(0)
    }

    fn tenant_exists(owner: &mut Client, tenant: Uuid) -> bool {
        owner
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM control.tenants WHERE tenant_id = $1)",
                &[&tenant],
            )
            .expect("tenant read")
            .get(0)
    }

    /// `humaux_testkit::fixture_purge` sharp edges (2026-10-05 teardown sweep), on a throwaway migrated to head:
    /// (a) two purges in ONE transaction both commit (the purge drops its own scratch tables); (b) a `tenant_id` table
    /// dropped by another session while the purge waits on it is refused by name (`relation public.c36_vanishing
    /// (oid …) vanished while the purge ran`), nothing is purged, and the same purge succeeds afterwards.
    /// Faults: delete the purge's `DROP TABLE pg_temp.fixture_purge_edges, …` ⇒ (a) raises 42P07 ⇒ red; delete the
    /// purge's `EXCEPTION WHEN syntax_error …` handler ⇒ (b) raises a bare `relation … does not exist` ⇒ red.
    #[test]
    fn purges_share_a_transaction_and_a_vanished_table_is_named() {
        let Some((_db, dsn)) = crate::migrate::tests::throwaway(
            "purges_share_a_transaction_and_a_vanished_table_is_named",
            "purge_edges",
        ) else {
            return;
        };
        // dep: PostgreSQL(any) — superuser on the throwaway database (migrate, fixture tenants, purges)
        let mut owner = Client::connect(&dsn, NoTls).expect("owner connect");
        apply_every_migration(&mut owner);

        let (a, b) = (
            fixture_tenant(&mut owner, "a"),
            fixture_tenant(&mut owner, "b"),
        );
        let sql = |t: Uuid| purge_tenant_fixture_sql(&t.to_string()).expect("canonical");
        owner
            .batch_execute(&format!("BEGIN; {}; {}; COMMIT;", sql(a), sql(b)))
            .map_err(|e| super::db_detail(&e))
            .expect("two purges in one transaction");
        assert!(!tenant_exists(&mut owner, a) && !tenant_exists(&mut owner, b));

        let c = fixture_tenant(&mut owner, "c");
        owner
            .batch_execute("CREATE TABLE public.c36_vanishing (tenant_id uuid)")
            .expect("scratch tenant_id table");
        // dep: PostgreSQL(any) — a second superuser session that holds, then drops, the scratch table
        let mut dropper = Client::connect(&dsn, NoTls).expect("dropper connect");
        dropper
            .batch_execute("BEGIN; LOCK TABLE public.c36_vanishing IN ACCESS EXCLUSIVE MODE")
            .expect("hold the scratch table");
        let purge_dsn = dsn.clone();
        let purge_c = sql(c);
        let purging = std::thread::spawn(move || {
            // dep: PostgreSQL(any) — the purging session
            let mut purger = Client::connect(&purge_dsn, NoTls).expect("purger connect");
            purger
                .batch_execute(&purge_c)
                .map_err(|e| super::db_detail(&e))
        });
        let waited = (0..300).any(|_| {
            std::thread::sleep(std::time::Duration::from_millis(100));
            owner
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = 'public.c36_vanishing'::regclass \
                       AND NOT granted)",
                    &[],
                )
                .expect("lock read")
                .get::<_, bool>(0)
        });
        assert!(waited, "the purge never queued behind the scratch table");
        dropper
            .batch_execute("DROP TABLE public.c36_vanishing; COMMIT")
            .expect("drop while the purge waits");
        let refused = purging.join().expect("purge thread");
        match &refused {
            Err(e)
                if e.contains("public.c36_vanishing")
                    && e.contains("vanished while the purge ran") => {}
            other => panic!("a vanished table must be refused by name: {other:?}"),
        }
        assert!(
            tenant_exists(&mut owner, c),
            "a refused purge purges nothing"
        );
        owner
            .batch_execute(&sql(c))
            .map_err(|e| super::db_detail(&e))
            .expect("the same purge once nothing is dropped under it");
        assert!(!tenant_exists(&mut owner, c));
    }

    /// ADR-0063 "Dev integrity finding". On a throwaway migrated to head, a tenant seeded through the real e2e-seed
    /// path (`provision` with two workspaces, `seed_lane`, `seed_second_domain`) plus [`runtime_residue_sql`] is torn
    /// down by [`teardown`], next to a decoy fixture tenant and a non-fixture tenant. Afterwards: no row with the
    /// tenant's id in any `tenant_id` table, the whole-database integrity scan empty (no FK orphan, no identity
    /// garbage), the decoy's rows untouched, and the purge refuses the non-fixture tenant.
    /// Faults (ADR-0063): skip `ops.jobs` in the purge's delete loop ⇒ `ops.jobs|control.tenants|
    /// jobs_tenant_id_fkey|n`; skip the identity tables ⇒ `private.event_identity|private.evidence_objects|…` plus
    /// `identity|…` lines.
    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one linear fixture script (seed -> assert -> teardown -> assert); splitting it hides which seed each assertion reads"
    )]
    fn teardown_leaves_no_fk_orphan_and_no_identity_garbage() {
        let Some((_db, dsn)) = crate::migrate::tests::throwaway(
            "teardown_leaves_no_fk_orphan_and_no_identity_garbage",
            "purge",
        ) else {
            return;
        };
        // dep: PostgreSQL(any) — superuser on the throwaway database (migrate, seed residue, teardown, scan)
        let mut owner = Client::connect(&dsn, NoTls).expect("owner connect");
        apply_every_migration(&mut owner);
        let maintenance_dsn =
            humaux_testkit::role_login_dsn(&dsn, "role_maintenance", |n| std::env::var(n).ok())
                .unwrap_or_else(|missing| panic!("missing object: {missing} (ADR-0059 D-D)"));
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        // dep: PostgreSQL(role_maintenance) — the seed's onboarding pool on the throwaway
        let maintenance = rt
            .block_on(MaintenanceDbPool::connect(&maintenance_dsn))
            .expect("maintenance pool");
        let run = Uuid::new_v4();
        let qdrant = QdrantFlags {
            collection: format!("xtask_purge_{}", run.simple()),
            dimension: 8,
            host: "127.0.0.1".to_string(),
            port: 6333,
            embedding_provider: format!("e2e-seed-{run}"),
            embedding_region: "cn-shanghai".to_string(),
        };
        let scopes = vec!["memory:write".to_string()];
        let pepper = [7u8; 32];
        let flags = LaneFlags {
            egress_processor_id: Uuid::new_v4(),
            region: "cn-shanghai".to_string(),
            service_tier: "standard".to_string(),
            endpoint_ref: format!("https://xtask-purge-{run}.invalid/v1/chat/completions"),
            provider_id: format!("e2e-seed-{run}"),
            provider_model_id: "purge-model".to_string(),
            model_revision: "purge".to_string(),
            capabilities: vec![ReasoningCapability::Text],
            account_ref: format!("e2e-seed-account-{run}"),
            request_extras: super::parse_request_extras("{}").expect("extras"),
        };

        let seeded = rt
            .block_on(super::provision(
                &maintenance,
                &qdrant,
                &scopes,
                1000,
                &pepper,
                2,
            ))
            .expect("provision (the e2e-seed onboarding path)");
        let verdict = (|| -> Result<(), String> {
            let tenant = seeded.tenant.tenant_id;
            seed_lane(
                &mut owner,
                tenant,
                seeded.tenant.owner_user_id,
                seeded.tenant.reasoning_domain_id,
                &flags,
            )?;
            super::seed_second_domain(
                &rt,
                &maintenance,
                &mut owner,
                &seeded.tenant,
                Some(&flags),
                &scopes,
                &pepper,
            )?;
            owner
                .batch_execute(&runtime_residue_sql(tenant))
                .map_err(|e| format!("tenant residue: {e:?}"))?;
            let decoy = rt
                .block_on(onboard_pg_only(&maintenance, &qdrant, &scopes))?
                .0;
            owner
                .batch_execute(&runtime_residue_sql(decoy))
                .map_err(|e| format!("decoy residue: {e:?}"))?;
            let foreign: Uuid = owner
                .query_one(
                    "INSERT INTO control.tenants (name) VALUES ('acme purge probe') RETURNING tenant_id",
                    &[],
                )
                .map_err(|e| format!("foreign tenant: {e}"))?
                .get(0);
            if !integrity_violations(&mut owner).is_empty() {
                return Err("the seeded throwaway is not FK-consistent before teardown".to_string());
            }

            let before = tenant_rows(&mut owner, tenant);
            for table in [
                "control.audit_events",
                "control.audit_event_identity",
                "ops.jobs",
                "control.usage_reservations",
                "ops.selection_snapshots",
                "ops.selection_snapshot_items",
                "control.workspace_memberships",
                "control.operation_receipts",
                "ops.model_call_ledger",
                "ops.model_call_identity",
                "control.reasoning_profiles",
                "private.messages",
            ] {
                if !before.contains_key(table) {
                    return Err(format!("seed left no {table} row to tear down: {before:?}"));
                }
            }
            if event_identity_rows(&mut owner, tenant) == 0 {
                return Err("seed left no private.event_identity row".to_string());
            }
            let decoy_before = (
                tenant_rows(&mut owner, decoy),
                event_identity_rows(&mut owner, decoy),
            );
            eprintln!(
                "teardown purge: {} tenant tables seeded: {before:?}",
                before.len()
            );

            teardown(&mut owner, tenant)?;

            let violations = integrity_violations(&mut owner);
            let left = tenant_rows(&mut owner, tenant);
            if !violations.is_empty() || !left.is_empty() {
                return Err(format!(
                    "after teardown: integrity scan {violations:?}, tenant rows left {left:?}"
                ));
            }
            let decoy_after = (
                tenant_rows(&mut owner, decoy),
                event_identity_rows(&mut owner, decoy),
            );
            if decoy_after != decoy_before {
                return Err(format!(
                    "decoy touched: {decoy_before:?} -> {decoy_after:?}"
                ));
            }
            let refusal = purge_tenant_fixture_sql(&foreign.to_string())
                .and_then(|sql| owner.batch_execute(&sql).map_err(|e| super::db_detail(&e)));
            match refusal {
                Err(error) if error.contains("not a fixture tenant") => {}
                other => return Err(format!("non-fixture tenant not refused: {other:?}")),
            }
            if tenant_rows(&mut owner, foreign).get("control.tenants") != Some(&1) {
                return Err("the refused purge removed the non-fixture tenant".to_string());
            }
            teardown(&mut owner, decoy)
        })();
        let dropped = super::drop_qdrant_collection(&rt, "127.0.0.1", 6333, &qdrant.collection);
        verdict.expect("fixture teardown leaves a consistent database");
        dropped.expect("drop the run's Qdrant collection");
    }
}
