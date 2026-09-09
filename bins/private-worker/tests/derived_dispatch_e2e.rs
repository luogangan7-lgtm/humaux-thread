//! ADR-0036 (card 14) acceptance, distill side: cross-tenant pending-work discovery for
//! `humaux-private-worker --distill-once` / `--distill-serve`.
//!
//! The Distill hop's own behaviour (route admission, §16.1.1 fingerprint, §10.1 ceiling, the
//! lease-fenced write transaction) is `tests/distill_hop_e2e.rs`' subject and is NOT re-proved
//! here. What is new in this card, and what this file covers, is the layer above it:
//!
//! 1. the 0164 `ops.outbox` `EVIDENCE_ACCEPTED` trigger emits a `DERIVED_DISTILL` job carrying
//!    the Evidence's own reasoning domain — the pair the pass needs and no longer reads from the
//!    environment;
//! 2. one pass discovers work in BOTH tenants with no tenant id in its inputs;
//! 3. two workers racing claim each job exactly once (asserted on `ops.jobs` rows, not logs);
//! 4. a killed worker's lease expires and another process re-claims the job, and the dead
//!    worker's stale fencing token then settles nothing;
//! 5. the owner arm 0164 added to `jobs_tenant_isolation` does NOT make `ops.jobs` cross-tenant
//!    readable from a worker session — only from inside the SECURITY DEFINER claim (asserted
//!    under RLS);
//! 6. a pass with no input claims nothing and returns promptly (`--distill-once` exits on it).
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or the 0164 objects missing print a visible
//! SKIP naming what was missing.

use humaux_adapters::byok::{
    PrivateInferenceContext, ReasoningCapability, ReasoningProviderDescriptor,
    ReasoningProviderError, StructuredReasoningRequest, StructuredReasoningResponse, TokenUsage,
    UserReasoningProvider, VisionReasoningRequest, VisionReasoningResponse,
};
use humaux_adapters::contribution_reasoner::ContributionReasonerConfig;
use humaux_adapters::disclosure::DeletionCapability;
use humaux_adapters::jobs::{
    self, ClaimedJob, DerivedJobType, DerivedLease, DerivedWorkOutcome, JobStatus,
};
use humaux_adapters::postgres::PrivateWorkerDbPool;
use humaux_domain::egress::ProcessorId;
use humaux_private_worker::distill::{self, DistillDispatchConfig};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use std::sync::Mutex;
use std::time::Duration;
use uuid::Uuid;

/// This file's claim is deliberately CROSS-tenant, so its tests cannot run concurrently with each
/// other — a per-tenant fixture cannot isolate a query that is not per-tenant by construction.
static SERIAL_GUARD: Mutex<()> = Mutex::new(());

const REGION: &str = "cn-shanghai";
const SERVICE_TIER: &str = "standard";
const EGRESS_PROCESSOR_ID: Uuid = Uuid::from_u128(0x2001);
/// Route-graph fixture values, mirrored from `tests/distill_hop_e2e.rs` so the two files share
/// one catalog row in the global append-only `control.processor_models`.
const PROVIDER_ID: &str = "minimax";
const MODEL_ID: &str = "MiniMax-M3";
const ENDPOINT_REF: &str = "https://api.minimaxi.com/v1/chat/completions";
const PURPOSE_DB: &str = "PRIVATE_DISTILL_TEXT";
/// One §10.1-admissible distilled memory. `PrivateKnowledge` is at or under the
/// `DirectUserInput` origin ceiling, so the write leg admits it instead of rejecting it.
const ONE_MEMORY_REPLY: &str = r#"{"memories":[{"content":"Health endpoint before traffic.","memory_type":"Decision","class":"PrivateKnowledge","confidence":0.9}]}"#;

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let Some(rest) = admin_dsn
        .strip_prefix("postgres://")
        .or_else(|| admin_dsn.strip_prefix("postgresql://"))
    else {
        return admin_dsn.to_string();
    };
    let Some(at) = rest.find('@') else {
        return admin_dsn.to_string();
    };
    format!("postgres://{role}:devlocal_{role}@{}", &rest[at + 1..])
}

fn db_detail(error: &postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{error}"),
    }
}

/// Never called: these tenants have no admitted `PRIVATE_DISTILL_TEXT` binding, so every claimed
/// Evidence is handed back to PENDING before any provider round trip (ADR-0016 D5's
/// environmental-failure split). `unreachable!` is the assertion — a pass that reached the
/// provider would be spending BYOK budget this test never authorized.
struct NeverCalledProvider(ReasoningProviderDescriptor);

#[async_trait::async_trait]
impl UserReasoningProvider for NeverCalledProvider {
    fn descriptor(&self) -> &ReasoningProviderDescriptor {
        &self.0
    }

    fn endpoint_ref(&self) -> &str {
        "https://example.invalid/v1/chat/completions"
    }

    fn model_revision(&self) -> Option<&str> {
        None
    }

    async fn complete_structured(
        &self,
        _context: &PrivateInferenceContext,
        _request: StructuredReasoningRequest,
    ) -> Result<StructuredReasoningResponse, ReasoningProviderError> {
        unreachable!("no admitted distill route: the pass must defer before any provider call")
    }

    async fn analyze_vision(
        &self,
        _context: &PrivateInferenceContext,
        _request: VisionReasoningRequest,
    ) -> Result<VisionReasoningResponse, ReasoningProviderError> {
        unreachable!("distill never calls vision")
    }
}

fn provider() -> NeverCalledProvider {
    NeverCalledProvider(ReasoningProviderDescriptor {
        provider_id: "derived-dispatch-e2e".to_string(),
        model_id: "never-called".to_string(),
        model_revision: None,
        capabilities: vec![ReasoningCapability::StructuredOutput],
        custom_endpoint: Some("https://example.invalid/v1/chat/completions".to_string()),
    })
}

/// The provisioned tenants' provider: a canned reply, so the pass really runs the whole distill
/// write leg (§16.1.1 fingerprint, §10.1 ceiling, the lease-fenced write) without a network hop.
/// `calls` is how "no double distill" is asserted on the provider side; the row counts assert it
/// on the database side.
struct FakeProvider {
    descriptor: ReasoningProviderDescriptor,
    calls: std::sync::atomic::AtomicU32,
}

impl FakeProvider {
    fn new() -> Self {
        Self {
            descriptor: ReasoningProviderDescriptor {
                provider_id: PROVIDER_ID.to_string(),
                model_id: MODEL_ID.to_string(),
                model_revision: None,
                capabilities: vec![ReasoningCapability::StructuredOutput],
                custom_endpoint: Some(ENDPOINT_REF.to_string()),
            },
            calls: std::sync::atomic::AtomicU32::new(0),
        }
    }

    fn calls(&self) -> u32 {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl UserReasoningProvider for FakeProvider {
    fn descriptor(&self) -> &ReasoningProviderDescriptor {
        &self.descriptor
    }

    fn endpoint_ref(&self) -> &str {
        ENDPOINT_REF
    }

    fn model_revision(&self) -> Option<&str> {
        None
    }

    async fn complete_structured(
        &self,
        _context: &PrivateInferenceContext,
        _request: StructuredReasoningRequest,
    ) -> Result<StructuredReasoningResponse, ReasoningProviderError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(StructuredReasoningResponse {
            json: ONE_MEMORY_REPLY.to_string(),
            usage: TokenUsage::default(),
        })
    }

    async fn analyze_vision(
        &self,
        _context: &PrivateInferenceContext,
        _request: VisionReasoningRequest,
    ) -> Result<VisionReasoningResponse, ReasoningProviderError> {
        unreachable!("distill never calls vision")
    }
}

fn reasoner_config() -> ContributionReasonerConfig {
    ContributionReasonerConfig {
        allowed_egress_processor_id: ProcessorId(EGRESS_PROCESSOR_ID),
        region: REGION.to_string(),
        permit_ttl: Duration::from_secs(30),
        deletion_capability: DeletionCapability::Unknown,
        system_prompt: "s".to_string(),
        json_schema: "{}".to_string(),
        max_output_tokens: 64,
    }
}

struct SeededTenant {
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
}

/// Every seeded tenant shares one throwaway `control.users` row (the route policies' owner and
/// every reasoning domain's owner), deleted with the tenants in `Drop`.
struct SeedContext {
    user_id: Uuid,
}

struct Handle {
    rt: tokio::runtime::Runtime,
    private: PrivateWorkerDbPool,
    admin: Client,
    dsn: String,
    tenants: Vec<SeededTenant>,
    user_id: Uuid,
}

impl Drop for Handle {
    /// Same `session_replication_role = replica` sweep `tests/distill_hop_e2e.rs` and
    /// `bins/consolidation-worker/tests/consolidation_hop_e2e.rs` use — one pass over every
    /// `tenant_id`-carrying table (`ops.jobs` included) plus the children that key through a
    /// parent.
    fn drop(&mut self) {
        let Ok(rows) = self.admin.query(
            "SELECT table_schema, table_name FROM information_schema.columns \
             WHERE column_name = 'tenant_id' \
               AND table_schema IN ('control','private','ops','projection','staging') \
               AND table_name <> 'tenants' \
             ORDER BY table_schema, table_name",
            &[],
        ) else {
            return;
        };
        let tables: Vec<(String, String)> = rows
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        for tenant in &self.tenants {
            let tenant = tenant.tenant_id;
            let mut sql = String::from("SET session_replication_role = replica; ");
            sql.push_str(&format!(
                "DELETE FROM private.memory_evidence WHERE memory_id IN \
                   (SELECT memory_id FROM private.memory_records WHERE tenant_id = '{tenant}'); \
                 DELETE FROM private.events WHERE event_id IN \
                   (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{tenant}'); "
            ));
            for (schema, table) in &tables {
                sql.push_str(&format!(
                    "DELETE FROM {schema}.{table} WHERE tenant_id = '{tenant}'; "
                ));
            }
            sql.push_str(&format!(
                "DELETE FROM control.tenants WHERE tenant_id = '{tenant}'; \
                 SET session_replication_role = DEFAULT;"
            ));
            if let Err(error) = self.admin.batch_execute(&sql) {
                eprintln!("derived_dispatch_e2e teardown ({tenant}): {error}");
            }
        }
        let _ = self.admin.execute(
            "DELETE FROM control.users WHERE user_id = $1",
            &[&self.user_id],
        );
    }
}

struct DispatchFixture;

impl DbIntegrationFixture for DispatchFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let migrated: bool = admin
            .query_one(
                "SELECT to_regprocedure('ops.claim_derived_work(text[],text,double precision,bigint)') \
                 IS NOT NULL",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(db_detail(&e)))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "ops.claim_derived_work does not exist — run `cargo xtask migrate` against \
                 HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }

        // Start from a quiet queue: park any DERIVED_DISTILL row another test's throwaway tenant
        // left claimable. Updates nothing on a fresh database.
        admin
            .execute(
                "UPDATE ops.jobs SET status = 'DEAD', lease_owner = NULL, lease_expires_at = NULL \
                 WHERE job_type = 'DERIVED_DISTILL' AND status <> 'DEAD'",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(db_detail(&e)))?;

        let user_id: Uuid = admin
            .query_one(
                "INSERT INTO control.users (state) VALUES ('ACTIVE') RETURNING user_id",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(db_detail(&e)))?
            .get(0);
        let ctx = SeedContext { user_id };

        // A and B are provisioned with an admitted PRIVATE_DISTILL_TEXT route; C deliberately is
        // not — the "tenant onboarded before its route was admitted" case, whose job must be
        // released, never settled DONE (its Evidence would be stranded: 0164's enqueue key is
        // per evidence_id with ON CONFLICT DO NOTHING).
        let mut tenants = Vec::new();
        for (label, with_binding) in [
            ("private derived_dispatch_e2e tenant A", true),
            ("private derived_dispatch_e2e tenant B", true),
            ("private derived_dispatch_e2e tenant C (no route)", false),
        ] {
            tenants.push(
                seed_tenant(&mut admin, label, &ctx, with_binding)
                    .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(db_detail(&e)))?,
            );
        }

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let private = rt
            .block_on(PrivateWorkerDbPool::connect(&dsn_as_role(
                &dsn,
                "role_private_worker",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            private,
            admin,
            dsn,
            tenants,
            user_id,
        })
    }
}

fn seed_tenant(
    admin: &mut Client,
    label: &str,
    ctx: &SeedContext,
    with_binding: bool,
) -> Result<SeededTenant, postgres::Error> {
    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name, state) VALUES ($1, 'ACTIVE') RETURNING tenant_id",
            &[&label],
        )?
        .get(0);
    // `control.reasoning_route_policies_check_owner` refuses a policy whose owner has no ACTIVE
    // membership in the policy's own tenant (§6.3); the binding trigger additionally requires the
    // domain to name that same owner, and `distill::acting_user` installs it for the RLS reads.
    admin.execute(
        "INSERT INTO control.memberships (tenant_id, user_id, role, state) \
         VALUES ($1, $2, 'OWNER', 'ACTIVE')",
        &[&tenant_id, &ctx.user_id],
    )?;
    let reasoning_domain_id: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains \
               (tenant_id, name, owner_user_id, status) \
             VALUES ($1, $2, $3, 'ACTIVE') RETURNING reasoning_domain_id",
            &[&tenant_id, &label, &ctx.user_id],
        )?
        .get(0);
    if with_binding {
        seed_route_binding(admin, tenant_id, ctx.user_id, reasoning_domain_id, label)?;
    }
    Ok(SeededTenant {
        tenant_id,
        reasoning_domain_id,
    })
}

/// The admitted `PRIVATE_DISTILL_TEXT` binding the pass resolves per claimed tenant, mirrored
/// from `tests/distill_hop_e2e.rs::setup_db` — none of these values reaches a network: the
/// provider is a fake, and what is under test is that the WORKER resolves the route per claimed
/// tenant instead of reading one out of its environment.
fn seed_route_binding(
    admin: &mut Client,
    tenant_id: Uuid,
    user_id: Uuid,
    reasoning_domain_id: Uuid,
    label: &str,
) -> Result<(), postgres::Error> {
    let route = seed_reasoning_profile(admin, tenant_id, user_id, label)?;
    let policy: Uuid = admin
        .query_one(
            "INSERT INTO control.reasoning_route_policies \
               (tenant_id, policy_owner_user_id, purpose) \
             VALUES ($1, $2, $3) RETURNING route_policy_id",
            &[&tenant_id, &user_id, &PURPOSE_DB],
        )?
        .get(0);
    // A PINNED policy needs exactly one priority-0 candidate before it may leave DRAFT, and
    // DRAFT -> SHADOW -> SERVING is the only order the owner check accepts.
    admin.execute(
        "INSERT INTO control.reasoning_route_candidates \
           (tenant_id, route_policy_id, route_policy_version, profile_id, profile_version, \
            priority) \
         VALUES ($1, $2, 1, $3, 1, 0)",
        &[&tenant_id, &policy, &route.profile_id],
    )?;
    for state in ["SHADOW", "SERVING"] {
        admin.execute(
            "UPDATE control.reasoning_route_policies SET lifecycle_state = $2 \
             WHERE route_policy_id = $1 AND policy_version = 1",
            &[&policy, &state],
        )?;
    }
    admin.execute(
        "INSERT INTO control.reasoning_route_bindings \
           (tenant_id, reasoning_domain_id, purpose, route_policy_id, route_policy_version) \
         VALUES ($1, $2, $3, $4, 1)",
        &[&tenant_id, &reasoning_domain_id, &PURPOSE_DB, &policy],
    )?;
    admin.execute(
        "INSERT INTO ops.reasoning_provider_health_observations \
           (tenant_id, processor_id, processor_model_id, provider_model_id, model_revision, \
            provider_endpoint_id, endpoint_ref, region, service_tier, source_kind, reason_code, \
            verdict, observed_at, valid_until) \
         VALUES ($1, $2, $3, $4, NULL, $5, $6, $7, $8, 'TEST', NULL, 'HEALTHY', \
                 clock_timestamp() - interval '1 second', clock_timestamp() + interval '30 minutes')",
        &[
            &tenant_id,
            &PROVIDER_ID,
            &route.processor_model_id,
            &MODEL_ID,
            &route.endpoint_id,
            &ENDPOINT_REF,
            &REGION,
            &SERVICE_TIER,
        ],
    )?;
    admin.execute(
        "INSERT INTO ops.reasoning_account_health_observations \
           (tenant_id, provider_account_id, credential_ref, billing_account_id, \
            billing_instrument_id, source_kind, reason_code, account_verdict, \
            credential_verdict, billing_account_verdict, billing_instrument_verdict, \
            observed_at, valid_until) \
         VALUES ($1, $2, $3, NULL, NULL, 'TEST', NULL, 'HEALTHY', 'VALID', NULL, NULL, \
                 clock_timestamp() - interval '1 second', clock_timestamp() + interval '30 minutes')",
        &[&tenant_id, &route.provider_account_id, &route.credential_ref],
    )?;
    Ok(())
}

/// The route-graph tail `control.reasoning_route_candidates` needs, plus the ids the two health
/// observations key on.
struct SeededRoute {
    profile_id: Uuid,
    processor_model_id: Uuid,
    provider_account_id: Uuid,
    endpoint_id: Uuid,
    credential_ref: Uuid,
}

fn seed_reasoning_profile(
    admin: &mut Client,
    tenant_id: Uuid,
    user_id: Uuid,
    label: &str,
) -> Result<SeededRoute, postgres::Error> {
    let credential: Uuid = admin
        .query_one(
            "INSERT INTO control.credentials (tenant_id, purpose, openbao_ref) \
             VALUES ($1, 'USER_REASONING', $2) RETURNING credential_id",
            &[&tenant_id, &format!("openbao://derived-dispatch/{label}")],
        )?
        .get(0);
    // `control.processor_models` is global and append-only (0128 trigger): one catalog row per
    // (processor, model, revision) across every run of every test in this workspace.
    admin.execute(
        "INSERT INTO control.processor_models \
           (processor_id, provider_model_id, model_revision, capabilities, status, \
            catalog_observed_at) \
         VALUES ($1, $2, NULL, ARRAY['TEXT','STRUCTURED_OUTPUT'], 'ACTIVE', clock_timestamp()) \
         ON CONFLICT DO NOTHING",
        &[&PROVIDER_ID, &MODEL_ID],
    )?;
    let processor_model_id: Uuid = admin
        .query_one(
            "SELECT processor_model_id FROM control.processor_models \
             WHERE processor_id = $1 AND provider_model_id = $2 AND model_revision IS NULL \
               AND status = 'ACTIVE'",
            &[&PROVIDER_ID, &MODEL_ID],
        )?
        .get(0);
    let account: Uuid = admin
        .query_one(
            "INSERT INTO control.provider_accounts \
               (tenant_id, owner_user_id, processor_id, external_account_ref_hash) \
             VALUES ($1, $2, $3, sha256(convert_to(gen_random_uuid()::text, 'UTF8'))) \
             RETURNING provider_account_id",
            &[&tenant_id, &user_id, &PROVIDER_ID],
        )?
        .get(0);
    admin.execute(
        "INSERT INTO control.reasoning_credential_bindings \
           (credential_ref, tenant_id, owner_user_id, provider_account_id, processor_id) \
         VALUES ($1, $2, $3, $4, $5)",
        &[&credential, &tenant_id, &user_id, &account, &PROVIDER_ID],
    )?;
    let endpoint: Uuid = admin
        .query_one(
            "INSERT INTO control.provider_endpoints \
               (tenant_id, provider_account_id, region, service_tier, endpoint_ref, \
                egress_processor_id) \
             VALUES ($1, $2, $3, $4, $5, $6) RETURNING endpoint_id",
            &[
                &tenant_id,
                &account,
                &REGION,
                &SERVICE_TIER,
                &ENDPOINT_REF,
                &EGRESS_PROCESSOR_ID,
            ],
        )?
        .get(0);
    let profile: Uuid = admin
        .query_one(
            "INSERT INTO control.reasoning_profiles \
               (tenant_id, owner_user_id, provider_account_id, endpoint_id, processor_model_id, \
                credential_ref, billing_account_id, default_billing_instrument_id, capabilities, \
                processing_region) \
             VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, ARRAY['TEXT'], $7) RETURNING profile_id",
            &[
                &tenant_id,
                &user_id,
                &account,
                &endpoint,
                &processor_model_id,
                &credential,
                &REGION,
            ],
        )?
        .get(0);
    Ok(SeededRoute {
        profile_id: profile,
        processor_model_id,
        provider_account_id: account,
        endpoint_id: endpoint,
        credential_ref: credential,
    })
}

/// What `remember` writes for one accepted Evidence (§14): the Evidence row plus the
/// `EVIDENCE_ACCEPTED` outbox row. The outbox INSERT is what 0164's
/// `derived_distill_work_enqueue` trigger fires on — this test never writes an `ops.jobs` row.
fn accept_evidence(handle: &mut Handle, tenant: usize) -> Uuid {
    let tenant_id = handle.tenants[tenant].tenant_id;
    let reasoning_domain_id = handle.tenants[tenant].reasoning_domain_id;
    let mut txn = handle.admin.transaction().expect("begin remember txn");
    let evidence_id: Uuid = txn
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', sha256(convert_to(gen_random_uuid()::text, 'UTF8')), \
                     'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $2) \
             RETURNING evidence_id",
            &[&tenant_id, &reasoning_domain_id],
        )
        .expect("insert evidence")
        .get(0);
    txn.execute(
        "INSERT INTO private.events (event_id, event_kind, payload) \
         VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
        &[&evidence_id],
    )
    .expect("insert event");
    let commit_seq: i64 = txn
        .query_one("SELECT nextval('ops.commit_seq_seq')", &[])
        .expect("commit seq")
        .get(0);
    txn.execute(
        "INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id) \
         VALUES ($1, $2, 1, 'EVIDENCE_ACCEPTED', $3)",
        &[&tenant_id, &commit_seq, &evidence_id],
    )
    .expect("insert outbox");
    txn.commit().expect("commit remember txn");
    evidence_id
}

fn jobs_of(handle: &mut Handle, tenant: usize) -> Vec<(Uuid, String, serde_json::Value)> {
    let tenant_id = handle.tenants[tenant].tenant_id;
    handle
        .admin
        .query(
            "SELECT job_id, status, payload FROM ops.jobs \
             WHERE tenant_id = $1 AND job_type = 'DERIVED_DISTILL' ORDER BY created_at",
            &[&tenant_id],
        )
        .expect("read jobs")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

/// Distilled `private.memory_records` rows for one seeded tenant — the row count the acceptance
/// gate asks for ("assert row counts, not logs").
fn memory_count(handle: &mut Handle, tenant: usize) -> i64 {
    let tenant_id = handle.tenants[tenant].tenant_id;
    handle
        .admin
        .query_one(
            "SELECT count(*) FROM private.memory_records WHERE tenant_id = $1",
            &[&tenant_id],
        )
        .expect("count memories")
        .get(0)
}

fn dispatch_config(owner: &str, lease_seconds: f64) -> DistillDispatchConfig {
    DistillDispatchConfig {
        lease_owner: owner.to_string(),
        lease_seconds,
        job_batch: 16,
        batch: 8,
        max_attempts: 5,
    }
}

fn claim(handle: &Handle, owner: &str, lease_seconds: f64) -> Vec<ClaimedJob> {
    handle
        .rt
        .block_on(jobs::claim_derived_work_private(
            &handle.private,
            &[DerivedJobType::Distill],
            owner,
            lease_seconds,
            16,
        ))
        .expect("cross-tenant claim")
}

/// (1)+(2): the trigger carries the Evidence's reasoning domain into the payload, and ONE pass
/// discovers and COMPLETES work in both tenants with no tenant id in its inputs — proved by the
/// distilled `private.memory_records` row counts, not by the report alone. A second pass over the
/// same tenants then writes nothing new: no double distill.
#[test]
fn dispatch_discovers_pending_evidence_in_two_tenants() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "dispatch_discovers_pending_evidence_in_two_tenants",
        |mut handle| {
            accept_evidence(&mut handle, 0);
            accept_evidence(&mut handle, 1);

            for tenant in 0..2 {
                let jobs = jobs_of(&mut handle, tenant);
                assert_eq!(jobs.len(), 1, "one job per accepted Evidence");
                assert_eq!(jobs[0].1, "PENDING");
                assert_eq!(
                    jobs[0].2["reasoning_domain_id"].as_str(),
                    Some(
                        handle.tenants[tenant]
                            .reasoning_domain_id
                            .to_string()
                            .as_str()
                    ),
                    "the trigger must carry the Evidence's own reasoning domain"
                );
            }

            let provider = FakeProvider::new();
            let report = handle
                .rt
                .block_on(distill::dispatch_pass(
                    &handle.private,
                    &provider,
                    reasoner_config(),
                    &dispatch_config("private-worker-one", 60.0),
                ))
                .expect("dispatch pass");

            assert_eq!(
                report.claimed, 2,
                "one pass must find BOTH tenants: {report:?}"
            );
            assert_eq!(report.completed, 2, "{report:?}");
            assert_eq!(report.not_ready, 0, "{report:?}");
            assert_eq!(report.work.claimed, 2, "{report:?}");
            assert_eq!(report.work.done, 2, "{report:?}");
            assert_eq!(report.work.deferred, 0, "{report:?}");
            assert_eq!(report.work.memories, 2, "{report:?}");
            assert_eq!(provider.calls(), 2, "one provider call per tenant");

            for tenant in 0..2 {
                assert_eq!(
                    memory_count(&mut handle, tenant),
                    1,
                    "each tenant's Evidence became exactly one memory"
                );
                assert_eq!(jobs_of(&mut handle, tenant)[0].1, "DONE");
            }

            // No double distill: the job is DONE and the Evidence's outbox row is settled, so a
            // second pass claims nothing and no second memory row appears.
            let again = handle
                .rt
                .block_on(distill::dispatch_pass(
                    &handle.private,
                    &provider,
                    reasoner_config(),
                    &dispatch_config("private-worker-one", 60.0),
                ))
                .expect("second dispatch pass");
            assert_eq!(again.claimed, 0, "{again:?}");
            assert_eq!(provider.calls(), 2, "a second pass must not re-infer");
            for tenant in 0..2 {
                assert_eq!(
                    memory_count(&mut handle, tenant),
                    1,
                    "no double distill: the row count must not move"
                );
            }
        },
    );
}

/// A pass that distilled NOTHING must not settle its job `DONE`. Tenant C has no admitted
/// `PRIVATE_DISTILL_TEXT` route, so the per-tenant hop hands every claimed `ops.outbox` row back
/// to PENDING (ADR-0016 D5's environmental-failure split) and the job has finished no work.
/// Settling it `DONE` — which is what the code did before ADR-0036 D5 — stranded that Evidence
/// forever: 0164's enqueue key is `derived-work:DERIVED_DISTILL:<evidence_id>` with
/// `ON CONFLICT DO NOTHING`, so the job is never re-emitted, and only another Evidence in the
/// SAME tenant would ever sweep it up again (`distill_repo::claim_pending_evidence` sweeps by
/// tenant).
///
/// Fault injection: settle `Ok(pass)` as `Done` unconditionally in `distill::dispatch_pass` and
/// the "PENDING" assertion below goes red.
#[test]
fn a_pass_that_distilled_nothing_releases_the_job_instead_of_completing_it() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "a_pass_that_distilled_nothing_releases_the_job_instead_of_completing_it",
        |mut handle| {
            let evidence_id = accept_evidence(&mut handle, 2);

            let provider = provider(); // must never be reached: no admitted route
            let mut config = dispatch_config("unprovisioned-worker", 60.0);
            // The harshest retry budget there is: a `Retry` that spent an attempt would be DEAD
            // on the very next pass.
            config.max_attempts = 1;
            let report = handle
                .rt
                .block_on(distill::dispatch_pass(
                    &handle.private,
                    &provider,
                    reasoner_config(),
                    &config,
                ))
                .expect("dispatch pass");
            assert_eq!(report.claimed, 1, "{report:?}");
            assert_eq!(report.completed, 0, "nothing was completed: {report:?}");
            assert_eq!(report.not_ready, 1, "{report:?}");
            assert_eq!(report.dead, 0, "{report:?}");
            assert_eq!(report.work.deferred, 1, "{report:?}");
            assert_eq!(report.work.memories, 0, "{report:?}");
            assert_eq!(memory_count(&mut handle, 2), 0);

            let job = &jobs_of(&mut handle, 2)[0];
            assert_eq!(
                job.1, "PENDING",
                "a job whose pass distilled nothing must be released, not settled DONE"
            );
            let backed_off: bool = handle
                .admin
                .query_one(
                    "SELECT next_retry_at > clock_timestamp() FROM ops.jobs WHERE job_id = $1",
                    &[&job.0],
                )
                .expect("read next_retry_at")
                .get(0);
            assert!(
                backed_off,
                "the release must back off — otherwise every --serve poll burns one attempt \
                 with zero delay"
            );

            // The Evidence itself is still claimable, which is the property that matters: the
            // work was not lost.
            let pending: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.outbox \
                     WHERE evidence_id = $1 AND status = 'PENDING'",
                    &[&evidence_id],
                )
                .expect("read outbox")
                .get(0);
            assert_eq!(pending, 1, "the pending Evidence must not be stranded");
        },
    );
}

/// (3) Two workers racing on the same pending work: every job is claimed by exactly one of them.
#[test]
fn two_racing_claims_take_each_job_exactly_once() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "two_racing_claims_take_each_job_exactly_once",
        |mut handle| {
            accept_evidence(&mut handle, 0);
            accept_evidence(&mut handle, 1);

            let first = claim(&handle, "racer-one", 60.0);
            let second = claim(&handle, "racer-two", 60.0);
            assert_eq!(
                first.len() + second.len(),
                2,
                "two jobs, claimed once each — never twice"
            );
            assert!(
                second
                    .iter()
                    .all(|b| first.iter().all(|a| a.job_id != b.job_id)),
                "no job may appear in both claims"
            );
            for job in first.iter().chain(second.iter()) {
                assert_eq!(job.status, JobStatus::Processing);
                assert_eq!(job.attempt, 1, "attempt is the monotonic fencing token");
            }
            // A third pass sees nothing: both live leases are held.
            assert!(claim(&handle, "racer-three", 60.0).is_empty());
        },
    );
}

/// (4) Recovery: the killed worker's lease expires, another process re-claims the job with a NEW
/// fencing token, and the dead worker's stale token then settles nothing.
#[test]
fn expired_lease_is_reclaimed_and_the_stale_token_settles_nothing() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "expired_lease_is_reclaimed_and_the_stale_token_settles_nothing",
        |mut handle| {
            accept_evidence(&mut handle, 0);

            let dead = claim(&handle, "killed-worker", 0.05);
            assert_eq!(dead.len(), 1);
            std::thread::sleep(Duration::from_millis(200));

            let survivor = claim(&handle, "survivor", 60.0);
            assert_eq!(
                survivor.len(),
                1,
                "an expired lease must be re-claimable — this is the arm the ADR-0036 fault \
                 injection removes"
            );
            assert_eq!(survivor[0].job_id, dead[0].job_id);
            assert_eq!(survivor[0].attempt, 2, "the token must have moved");

            let stale = handle
                .rt
                .block_on(jobs::settle_derived_private(
                    &handle.private,
                    &DerivedLease::of(&dead[0], "killed-worker"),
                    DerivedWorkOutcome::Done,
                    60.0,
                ))
                .expect("stale settle");
            assert!(!stale, "a stale fencing token must settle nothing");

            let settled = handle
                .rt
                .block_on(jobs::settle_derived_private(
                    &handle.private,
                    &DerivedLease::of(&survivor[0], "survivor"),
                    DerivedWorkOutcome::Done,
                    60.0,
                ))
                .expect("survivor settle");
            assert!(settled);
            assert_eq!(jobs_of(&mut handle, 0)[0].1, "DONE");
        },
    );
}

/// (5) The owner arm 0164 added to `jobs_tenant_isolation` is reachable ONLY from inside the
/// SECURITY DEFINER claim: a `role_private_worker` session still sees exactly its own tenant.
#[test]
fn worker_session_still_sees_only_its_own_tenants_jobs() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "worker_session_still_sees_only_its_own_tenants_jobs",
        |mut handle| {
            accept_evidence(&mut handle, 0);
            accept_evidence(&mut handle, 1);
            let (a, b) = (handle.tenants[0].tenant_id, handle.tenants[1].tenant_id);

            let mut worker =
                Client::connect(&dsn_as_role(&handle.dsn, "role_private_worker"), NoTls)
                    .expect("connect as role_private_worker");
            let mut txn = worker.transaction().expect("begin");
            txn.batch_execute(&format!("SET LOCAL humaux.tenant_id = '{a}'"))
                .expect("install tenant A context");
            let own: i64 = txn
                .query_one(
                    "SELECT count(*) FROM ops.jobs WHERE tenant_id = $1 \
                       AND job_type = 'DERIVED_DISTILL'",
                    &[&a],
                )
                .expect("own jobs")
                .get(0);
            let other: i64 = txn
                .query_one(
                    "SELECT count(*) FROM ops.jobs WHERE tenant_id = $1 \
                       AND job_type = 'DERIVED_DISTILL'",
                    &[&b],
                )
                .expect("cross-tenant jobs")
                .get(0);
            assert_eq!(own, 1);
            assert_eq!(
                other, 0,
                "the owner arm must not widen a worker session's own view of ops.jobs"
            );
            txn.rollback().expect("rollback probe txn");
        },
    );
}

/// (6) `--distill-once` with no input: nothing claimed, returns promptly rather than polling.
#[test]
fn dispatch_pass_with_no_pending_work_claims_nothing() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "dispatch_pass_with_no_pending_work_claims_nothing",
        |mut handle| {
            let provider = provider();
            let started = std::time::Instant::now();
            let report = handle
                .rt
                .block_on(distill::dispatch_pass(
                    &handle.private,
                    &provider,
                    reasoner_config(),
                    &dispatch_config("idle-worker", 60.0),
                ))
                .expect("idle pass must succeed, not hang");
            assert_eq!(report.completed, 0, "{report:?}");
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "an empty pass must return promptly, not poll"
            );
            for tenant in 0..3 {
                assert!(jobs_of(&mut handle, tenant).is_empty());
            }
        },
    );
}

// ---------------------------------------------------------------------------
// Card 15 / ADR-0037 — graceful shutdown of the distill loop, asserted against the BINARY
// ---------------------------------------------------------------------------

/// macOS XProtect assesses a freshly linked binary on its first exec (~1 min, sometimes much
/// longer under load); pay it once, on a run that measures nothing.
fn warm_binary() {
    let _ = std::process::Command::new(env!("CARGO_BIN_EXE_humaux-private-worker"))
        .arg("--warm-up-not-a-mode")
        .output();
}

/// The `--distill-serve` environment, minus nothing: `bootstrap()` builds the real BYOK provider
/// before the loop starts, so every one of its keys has to be present even though this test's
/// pass never reaches an inference call.
///
/// The endpoint is a non-forbidden IP LITERAL, which `ssrf::validate_custom_endpoint` accepts
/// without any DNS round trip — and nothing ever dials it: the only seeded work belongs to
/// tenant C, whose route was never admitted, so the pass releases the job at the admission check
/// (the same path `a_pass_that_distilled_nothing_releases_the_job_instead_of_completing_it`
/// proves in process with a provider that panics if called).
fn distill_serve_command(dsn: &str) -> std::process::Command {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_humaux-private-worker"));
    cmd.env("PRIVATE_WORKER_PG_DSN", dsn)
        .env(
            "HUMAUX_PRIVATE_WORKER_CHAT_URL",
            "https://192.88.99.1/v1/chat/completions",
        )
        .env("HUMAUX_PRIVATE_WORKER_PROVIDER_ID", PROVIDER_ID)
        .env("HUMAUX_PRIVATE_WORKER_MODEL_ID", MODEL_ID)
        .env("HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS", "5")
        .env("HUMAUX_PRIVATE_WORKER_KEY_ENV", "HUMAUX_CARD15_TEST_SECRET")
        .env("HUMAUX_CARD15_TEST_SECRET", "unused-by-this-path")
        .env(
            "HUMAUX_PRIVATE_WORKER_EGRESS_PROCESSOR_ID",
            EGRESS_PROCESSOR_ID.to_string(),
        )
        .env("HUMAUX_PRIVATE_WORKER_REGION", REGION)
        .env("HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS", "30")
        .env("HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS", "120")
        .env("HUMAUX_PRIVATE_WORKER_DISTILL_JOB_BATCH", "16")
        .env("HUMAUX_PRIVATE_WORKER_DISTILL_BATCH", "8")
        .env("HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS", "5")
        .env("HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS", "1");
    cmd
}

/// ADR-0037 D3 for the OTHER lease-holding resident worker. The review that produced this test
/// found the acceptance item ("SIGTERM to each worker mid-poll drains and exits zero without
/// leaving a claimed job un-released") asserted for the consolidation worker only, while the
/// distill loop — which settles BOTH an `ops.jobs` lease and a per-tenant `ops.outbox` lease per
/// pass (ADR-0016 D5) — had no shutdown test at all.
///
/// The signal is delivered once the queue shows the loop has really run a pass, so it lands
/// either inside a pass or in the poll wait; the invariant asserted is the same in both cases
/// and is read from `ops.jobs`, not from a log.
///
/// 注错: move the `shutdown.recv()` arm from the poll wait INTO `distill::dispatch_pass`
/// (cancel a pass mid-flight) ⇒ a claimed job is left `PROCESSING` with a live lease and the
/// final assertion goes red naming its job id. Replacing the eagerly installed
/// `SignalKind::interrupt()` in `Shutdown` with `tokio::signal::ctrl_c()` reddens the
/// `sigint_...` twin below the same way.
#[test]
fn sigterm_mid_serve_drains_and_leaves_no_job_processing_with_a_live_lease() {
    distill_serve_drains_on(
        "TERM",
        "sigterm_mid_serve_drains_and_leaves_no_job_processing_with_a_live_lease",
    );
}

/// The Ctrl-C twin: `Shutdown`'s doc promised SIGINT was latched before the first pass, and until
/// card 15's review it was not — `tokio::signal::ctrl_c()` registers the handler on its first
/// poll, which happens only after a pass has returned.
#[test]
fn sigint_mid_serve_drains_and_leaves_no_job_processing_with_a_live_lease() {
    distill_serve_drains_on(
        "INT",
        "sigint_mid_serve_drains_and_leaves_no_job_processing_with_a_live_lease",
    );
}

fn distill_serve_drains_on(signal: &str, test_name: &'static str) {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(test_name, |mut handle| {
        warm_binary();
        // Tenant C: onboarded, no admitted PRIVATE_DISTILL_TEXT route. Its jobs are claimed and
        // released without an inference call, which is what keeps this test hermetic.
        for _ in 0..3 {
            accept_evidence(&mut handle, 2);
        }
        let tenant_id = handle.tenants[2].tenant_id;
        assert_eq!(jobs_of(&mut handle, 2).len(), 3, "three jobs to claim");

        let dsn = dsn_as_role(&handle.dsn, "role_private_worker");
        let mut child = distill_serve_command(&dsn)
            .arg("--distill-serve")
            .spawn()
            .expect("spawn humaux-private-worker --distill-serve");

        // Wait until the loop has demonstrably touched the queue (bounded, monotonic).
        let deadline = std::time::Instant::now() + Duration::from_secs(90);
        let mut ran = false;
        while std::time::Instant::now() < deadline {
            let touched: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.jobs \
                     WHERE tenant_id = $1 AND (attempt > 0 OR status <> 'PENDING')",
                    &[&tenant_id],
                )
                .expect("read job progress")
                .get(0);
            if touched > 0 {
                ran = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if !ran {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the distill loop never ran a pass — this test would assert nothing");
        }

        let signalled = std::process::Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(child.id().to_string())
            .status()
            .expect("send the termination signal");
        assert!(signalled.success(), "kill -{signal} failed");

        let exit_deadline = std::time::Instant::now() + Duration::from_secs(90);
        let status = loop {
            match child.try_wait().expect("poll the worker") {
                Some(status) => break status,
                None if std::time::Instant::now() >= exit_deadline => {
                    let _ = child.kill();
                    panic!("the distill worker did not exit within 90s of SIG{signal}");
                }
                None => std::thread::sleep(Duration::from_millis(100)),
            }
        };
        assert!(
            status.success(),
            "a worker drained by SIG{signal} must exit zero, got {status:?} — a non-zero status \
             means the signal hit its default disposition instead of a handler"
        );

        let stuck = handle
            .admin
            .query(
                "SELECT job_id FROM ops.jobs \
                 WHERE tenant_id = $1 AND status = 'PROCESSING' \
                   AND lease_expires_at > clock_timestamp()",
                &[&tenant_id],
            )
            .expect("read leases");
        let stuck_ids: Vec<Uuid> = stuck.iter().map(|r| r.get(0)).collect();
        assert!(
            stuck.is_empty(),
            "a drained distill worker left {} job(s) PROCESSING with a live lease only expiry \
             could free: {stuck_ids:?}",
            stuck.len()
        );
        // The per-tenant ADR-0016 D5 outbox lease must be settled too — the second lease this
        // loop holds, and the one a mid-pass cancellation would strand.
        let leased: i64 = handle
            .admin
            .query_one(
                "SELECT count(*) FROM ops.outbox \
                 WHERE tenant_id = $1 AND lease_expires_at > clock_timestamp()",
                &[&tenant_id],
            )
            .expect("read outbox leases")
            .get(0);
        assert_eq!(
            leased, 0,
            "a drained distill worker left a live ops.outbox lease behind"
        );
    });
}

/// ADR-0037 D3, the RPC-listener half of `bins/private-worker/src/main.rs`: `--serve-rpc` holds no
/// lease of its own, so its drain is just "stop accepting and return zero" — but nothing asserted
/// that it returns at all. Before the eagerly installed handlers landed, a Ctrl-C between `bind`
/// and the first poll of the `select!` killed this listener by default disposition.
///
/// 注错: drop the `() = shutdown.recv()` arm from `serve_rpc`'s `select!` ⇒ the process ignores
/// the signal and this test fails on the 60s exit deadline.
#[test]
fn sigterm_to_the_inference_rpc_listener_exits_zero() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "sigterm_to_the_inference_rpc_listener_exits_zero",
        |handle| {
            warm_binary();
            // Short `/tmp` path: macOS's temp dir plus a uuid overruns `sockaddr_un.sun_path`.
            let socket_path = format!("/tmp/hp15-rpc-{}.sock", Uuid::now_v7().simple());
            let _ = std::fs::remove_file(&socket_path);
            let dsn = dsn_as_role(&handle.dsn, "role_private_worker");
            let mut child = distill_serve_command(&dsn)
                .arg("--serve-rpc")
                .env("HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH", &socket_path)
                .env(
                    "HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID",
                    own_uid().to_string(),
                )
                .spawn()
                .expect("spawn humaux-private-worker --serve-rpc");

            let deadline = std::time::Instant::now() + Duration::from_secs(60);
            while !std::path::Path::new(&socket_path).exists() {
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("the RPC listener never bound {socket_path}");
                }
                std::thread::sleep(Duration::from_millis(100));
            }

            let signalled = std::process::Command::new("kill")
                .arg("-TERM")
                .arg(child.id().to_string())
                .status()
                .expect("send SIGTERM");
            assert!(signalled.success(), "kill -TERM failed");

            let exit_deadline = std::time::Instant::now() + Duration::from_secs(60);
            let status = loop {
                match child.try_wait().expect("poll the listener") {
                    Some(status) => break status,
                    None if std::time::Instant::now() >= exit_deadline => {
                        let _ = child.kill();
                        let _ = std::fs::remove_file(&socket_path);
                        panic!("the RPC listener did not exit within 60s of SIGTERM");
                    }
                    None => std::thread::sleep(Duration::from_millis(100)),
                }
            };
            let _ = std::fs::remove_file(&socket_path);
            assert!(
                status.success(),
                "a drained RPC listener must exit zero, got {status:?}"
            );
        },
    );
}

/// This process's uid, read from a file it just created — the peer-credential value
/// `--serve-rpc` expects its caller to have (§4.2 / ADR-0037 D6). This test never dials the
/// socket, so the value only has to parse; taking the real one keeps the environment honest
/// without linking `libc` for one call.
fn own_uid() -> u32 {
    use std::os::unix::fs::MetadataExt;
    let marker = std::env::temp_dir().join(format!("hp15-uid-{}", Uuid::now_v7().simple()));
    std::fs::write(&marker, b"").expect("write the uid marker file");
    let uid = std::fs::metadata(&marker)
        .expect("stat the uid marker file")
        .uid();
    let _ = std::fs::remove_file(&marker);
    uid
}
