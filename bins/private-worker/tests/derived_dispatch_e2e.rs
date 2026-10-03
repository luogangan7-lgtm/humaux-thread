//! `private-worker::tests::derived_dispatch_e2e` — ADR-0036 / ADR-0058 acceptance, distill side: the cross-tenant,
//!   slot-bounded, tenant-fair dispatcher behind `humaux-private-worker --distill-once` / `--distill-serve`.
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-domain, humaux-testkit, postgres, serde_json, tokio,
//!   uuid]; services=[PostgreSQL(owner) r=[control.audit_events, ops.claim_derived_work_v2, ops.commit_seq_seq, ops.distill_calls, ops.distill_tenant_scheduler, ops.model_call_ledger,
//!   ops.provider_arbiters, ops.provider_slots, private.memory_affects, private.memory_records, private.processing_runs]
//!   w=[control.credentials, control.memberships, control.private_reasoning_domains, control.processor_models,
//!   control.provider_accounts, control.provider_endpoints, control.reasoning_credential_bindings,
//!   control.reasoning_profiles, control.reasoning_route_bindings, control.reasoning_route_candidates,
//!   control.reasoning_route_policies, control.tenants, control.users, ops.jobs, ops.outbox, ops.provider_slots,
//!   ops.reasoning_account_health_observations, ops.reasoning_provider_health_observations, private.events,
//!   private.evidence_objects, private.memory_evidence], PostgreSQL(role_maintenance), PostgreSQL(role_private_worker) x=[ops.claim_derived_work_v2],
//!   subprocess(humaux-private-worker), subprocess(kill)]; env=[CARGO_BIN_EXE_humaux-private-worker,
//!   HUMAUX_CARD15_TEST_SECRET, HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS, HUMAUX_PRIVATE_WORKER_CAPABILITIES,
//!   HUMAUX_PRIVATE_WORKER_CHAT_URL, HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID, HUMAUX_PRIVATE_WORKER_CREDENTIALS,
//!   HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_MAX_CALLS,
//!   HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_IN_FLIGHT,
//!   HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS,
//!   HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS, HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS,
//!   HUMAUX_PRIVATE_WORKER_DNS_PINS, HUMAUX_PRIVATE_WORKER_EGRESS_PROCESSOR_ID, HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS,
//!   HUMAUX_PRIVATE_WORKER_KEY_ENV, HUMAUX_PRIVATE_WORKER_MODEL_ID, HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS,
//!   HUMAUX_PRIVATE_WORKER_PROVIDER_ID, HUMAUX_PRIVATE_WORKER_REGION, HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH,
//!   HUMAUX_TEST_PG_DSN, MINIMAX_API_KEY, PRIVATE_WORKER_PG_DSN]; modules=[adapters::byok, adapters::contribution_reasoner,
//!   adapters::disclosure, adapters::distill_reasoner, adapters::jobs, adapters::membership_repo, adapters::postgres, adapters::provisioning, domain::egress,
//!   domain::evidence, humaux-testkit,
//!   private-worker::distill, private-worker::tests::support::dispatch_fence,
//!   private-worker::tests::support::double_spend, private-worker::tests::support::live_minimax]
//! Called-by: [cargo-test]
//! Invariants: [only this file's tenants are ever claimed (foreign scheduler rows are fenced FOR UPDATE); every
//!   scenario's faults are named in its doc (ADR-0058 records the red→green runs); a fixture deletes its jobs, slots
//!   and data rows in one printed batch and its tenant rows in a separate best-effort batch]
//! Spec: Baseline §16.1.1; §10.1; §67.2; §11; §79.2; ADR-0058; ADR-0059
//!
//! The Distill hop's own behaviour (route admission, §16.1.1 fingerprint, §10.1 ceiling, the
//! fenced write transaction) is `tests/distill_hop_e2e.rs`' subject. This file covers the layer
//! above it: one job = one Evidence (P1-4), the four provider slots and the tenant rotation
//! (P1-16, debts 1 and 2), counted attempts and honest DEAD, WAITING_KEY parking, the generation
//! fence under two dispatchers, and the binary's resident loop.

use humaux_adapters::byok::{
    PrivateInferenceContext, ReasoningCapability, ReasoningProviderDescriptor,
    ReasoningProviderError, StructuredReasoningRequest, StructuredReasoningResponse, TokenUsage,
    UserReasoningProvider, VisionReasoningRequest, VisionReasoningResponse,
};
use humaux_adapters::contribution_reasoner::ContributionReasonerConfig;
use humaux_adapters::disclosure::DeletionCapability;
use humaux_adapters::jobs::{self, DistillLease};
use humaux_adapters::membership_repo::AdminAction;
use humaux_adapters::postgres::{MaintenanceDbPool, PrivateWorkerDbPool};
use humaux_adapters::provisioning::{
    self, ProvisioningError, RequeueReceipt, RequeueSkipReason, RequeueTarget, RequeuedJob,
    SkippedJob,
};
use humaux_domain::egress::ProcessorId;
use humaux_private_worker::distill::{self, DistillDispatchConfig, DistillDispatchReport};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use uuid::Uuid;

#[path = "support/dispatch_fence.rs"]
mod dispatch_fence;
#[path = "support/double_spend.rs"]
mod double_spend;
#[path = "support/live_minimax.rs"]
mod live_minimax;

/// Every test drives the ONE global slot set and the cross-tenant claim, so tests never overlap.
static SERIAL_GUARD: Mutex<()> = Mutex::new(());

/// ADR-0059 D-I: the worker's credential map as this file configures it — every credential
/// reference [`seed_reasoning_profile`] created (tests are serialized by [`SERIAL_GUARD`]). T25
/// takes one reference back out of a config.
static MAPPED_CREDENTIALS: Mutex<BTreeSet<Uuid>> = Mutex::new(BTreeSet::new());

fn mapped_credentials() -> BTreeSet<Uuid> {
    MAPPED_CREDENTIALS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

/// `HUMAUX_PRIVATE_WORKER_CREDENTIALS` for a spawned worker: every mapped reference names `var`.
fn credentials_spec(var: &str) -> String {
    mapped_credentials()
        .iter()
        .map(|r| format!("{r}={var}"))
        .collect::<Vec<_>>()
        .join(",")
}

const REGION: &str = "cn-shanghai";
const SERVICE_TIER: &str = "standard";
const EGRESS_PROCESSOR_ID: Uuid = Uuid::from_u128(0x2001);
/// A DIFFERENT deployment's egress processor: a tenant whose admitted route points at it can
/// never be served by THIS worker (card 16's P0).
const FOREIGN_EGRESS_PROCESSOR_ID: Uuid = Uuid::from_u128(0x2002);
/// The static class `PrivateReasoningError::class` carries for that failure.
const FOREIGN_EGRESS_REASON: &str = "configured provider does not match admitted route";
/// The NOT_READY class of a tenant with no admitted binding.
const NO_BINDING_REASON: &str = "no admitted PRIVATE_DISTILL_TEXT route binding";
/// Route-graph fixture values, mirrored from `tests/distill_hop_e2e.rs` so the two files share
/// one catalog row in the global append-only `control.processor_models`.
const PROVIDER_ID: &str = live_minimax::MINIMAX_PROVIDER;
const MODEL_ID: &str = live_minimax::MINIMAX_MODEL;
const ENDPOINT_REF: &str = live_minimax::MINIMAX_CHAT_URL;
const PURPOSE_DB: &str = "PRIVATE_DISTILL_TEXT";
/// The transport timeout the in-process tests size `ops.begin_call`'s window with.
const HTTP_SECS: f64 = 5.0;
/// ADR-0058 D-T: a §72.3 budget no test in this file reaches (the budget gate has its own test,
/// distill_dispatch_v2 T21).
const TEST_BUDGET: jobs::DistillCallBudget = jobs::DistillCallBudget {
    window_seconds: 60.0,
    max_calls: 10_000,
};
/// One §10.1-admissible distilled memory (`PrivateKnowledge` ≤ the `DirectUserInput` ceiling).
const ONE_MEMORY_REPLY: &str = r#"{"memories":[{"content":"Health endpoint before traffic.","memory_type":"Decision","class":"PrivateKnowledge","confidence":0.9}]}"#;

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    // ADR-0059 D-D: a real login as `role`, its password from HUMAUX_ROLE_PASSWORD_<SUFFIX>.
    humaux_testkit::role_login_dsn(admin_dsn, role, |name| std::env::var(name).ok())
        .unwrap_or_else(|missing| panic!("missing object: {missing} (ADR-0059 D-D)"))
}

fn db_detail(error: &postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{error}"),
    }
}

/// Never called: tenants with no admitted binding are settled NOT_READY before any provider
/// round trip. `unreachable!` is the assertion.
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
        unreachable!("no admitted distill route: the job must settle before any provider call")
    }

    async fn analyze_vision(
        &self,
        _context: &PrivateInferenceContext,
        _request: VisionReasoningRequest,
    ) -> Result<VisionReasoningResponse, ReasoningProviderError> {
        unreachable!("distill never calls vision")
    }
}

fn never_called() -> NeverCalledProvider {
    NeverCalledProvider(ReasoningProviderDescriptor {
        provider_id: "derived-dispatch-e2e".to_string(),
        model_id: "never-called".to_string(),
        model_revision: None,
        capabilities: vec![ReasoningCapability::StructuredOutput],
        custom_endpoint: Some("https://example.invalid/v1/chat/completions".to_string()),
    })
}

/// What a scripted call answers.
enum Reply {
    Json(String),
    RetryWait,
    WaitingKey,
}

type Script = dyn Fn(&str) -> (Duration, Reply) + Send + Sync;

/// The admitted tenants' provider: the script maps the request's user prompt (which carries the
/// Evidence payload, so a marker text selects a behaviour) to a delay and a reply. Counts calls
/// and the highest number of calls live at once.
struct StubProvider {
    descriptor: ReasoningProviderDescriptor,
    calls: AtomicU32,
    finished: AtomicU32,
    live: AtomicU32,
    max_live: AtomicU32,
    script: Box<Script>,
}

impl StubProvider {
    fn new(script: impl Fn(&str) -> (Duration, Reply) + Send + Sync + 'static) -> Self {
        Self {
            descriptor: live_minimax::descriptor(),
            calls: AtomicU32::new(0),
            finished: AtomicU32::new(0),
            live: AtomicU32::new(0),
            max_live: AtomicU32::new(0),
            script: Box::new(script),
        }
    }

    /// Answers every call with one admissible memory after `delay`.
    fn one_memory(delay: Duration) -> Self {
        Self::new(move |_| (delay, Reply::Json(ONE_MEMORY_REPLY.to_string())))
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }
}

struct LiveGuard<'a>(&'a AtomicU32);

impl Drop for LiveGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl UserReasoningProvider for StubProvider {
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
        request: StructuredReasoningRequest,
    ) -> Result<StructuredReasoningResponse, ReasoningProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_live.fetch_max(live, Ordering::SeqCst);
        let _live = LiveGuard(&self.live);
        let (delay, reply) = (self.script)(&request.user_prompt);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        self.finished.fetch_add(1, Ordering::SeqCst);
        match reply {
            Reply::Json(json) => Ok(StructuredReasoningResponse {
                json,
                usage: TokenUsage::default(),
                channel_fallback: false,
            }),
            Reply::RetryWait => Err(ReasoningProviderError::RetryWait { retry_after: None }),
            Reply::WaitingKey => Err(ReasoningProviderError::WaitingKey { fingerprint: None }),
        }
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

/// ADR-0058 D-K shape with the smallest admissible hard deadline for `lease_seconds`.
fn dispatch_config(owner: &str, lease_seconds: f64) -> DistillDispatchConfig {
    DistillDispatchConfig {
        lease_owner: owner.to_string(),
        lease_seconds,
        in_flight: 4,
        hard_deadline_seconds: 2.0 * (HTTP_SECS + lease_seconds),
        http_timeout_seconds: HTTP_SECS,
        not_ready_park_seconds: 600.0,
        max_attempts: 5,
        budget: TEST_BUDGET,
        credential_refs: mapped_credentials(),
    }
}

struct SeededTenant {
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    /// The tenant's `PRIVATE_DISTILL_TEXT` route policy, when it has one (a second domain binds it).
    route_policy_id: Option<Uuid>,
}

struct Handle {
    rt: tokio::runtime::Runtime,
    private: PrivateWorkerDbPool,
    admin: Client,
    /// Holds every pre-existing scheduler row FOR UPDATE (`dispatch_fence`).
    fence: Client,
    dsn: String,
    tenants: Vec<SeededTenant>,
    user_id: Uuid,
}

impl Drop for Handle {
    /// Card-31 lesson: the jobs (with the slots they hold) and every data row of this file's
    /// tenants go in ONE batch whose failure is printed; the tenant rows (referenced by
    /// append-only audit rows) and the shared user go in a separate best-effort batch.
    fn drop(&mut self) {
        let _ = self.fence.batch_execute("ROLLBACK");
        let ids = self
            .tenants
            .iter()
            .map(|t| format!("'{}'", t.tenant_id))
            .collect::<Vec<_>>()
            .join(",");
        if ids.is_empty() {
            return;
        }
        let tables: Vec<(String, String)> = match self.admin.query(
            "SELECT table_schema, table_name FROM information_schema.columns \
             WHERE column_name = 'tenant_id' \
               AND table_schema IN ('control','private','ops','projection','staging') \
               AND table_name <> 'tenants' \
             ORDER BY table_schema, table_name",
            &[],
        ) {
            Ok(rows) => rows.into_iter().map(|r| (r.get(0), r.get(1))).collect(),
            Err(e) => {
                eprintln!("derived_dispatch_e2e cleanup: table list failed: {e}");
                return;
            }
        };
        let mut sql = format!(
            "SET session_replication_role = replica; \
             UPDATE ops.provider_slots SET job_id = NULL, claim_generation = NULL, bound_until = NULL \
               WHERE job_id IN (SELECT job_id FROM ops.jobs WHERE tenant_id IN ({ids})); \
             DELETE FROM private.memory_evidence WHERE memory_id IN \
               (SELECT memory_id FROM private.memory_records WHERE tenant_id IN ({ids})); \
             DELETE FROM private.events WHERE event_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id IN ({ids})); "
        );
        for (schema, table) in &tables {
            sql.push_str(&format!(
                "DELETE FROM {schema}.{table} WHERE tenant_id IN ({ids}); "
            ));
        }
        sql.push_str("SET session_replication_role = DEFAULT;");
        if let Err(e) = self.admin.batch_execute(&sql) {
            eprintln!("derived_dispatch_e2e cleanup: jobs/data batch failed: {e}");
        }
        if let Err(e) = self.admin.batch_execute(&format!(
            "DELETE FROM control.tenants WHERE tenant_id IN ({ids}); \
             DELETE FROM control.users WHERE user_id = '{}';",
            self.user_id
        )) {
            eprintln!("derived_dispatch_e2e cleanup: tenant batch failed (best effort): {e}");
        }
    }
}

struct DispatchFixture;

impl DbIntegrationFixture for DispatchFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        // dep: PostgreSQL(owner) — admin connection for seeding, inspection and cleanup
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        let migrated: bool = admin
            .query_one(
                "SELECT to_regprocedure('ops.claim_derived_work_v2(text,double precision,double precision)') \
                 IS NOT NULL",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(db_detail(&e)))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "ops.claim_derived_work_v2 does not exist — run `cargo xtask migrate` (0190) \
                 against HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }
        let fence = dispatch_fence::open(&mut admin, &dsn)
            .map_err(DbFixtureSkipReason::IsolationSetupFailed)?;

        let user_id: Uuid = admin
            .query_one(
                "INSERT INTO control.users (state) VALUES ('ACTIVE') RETURNING user_id",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(db_detail(&e)))?
            .get(0);

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let private = rt
            // dep: PostgreSQL(role_private_worker) — role-scoped pool call
            .block_on(PrivateWorkerDbPool::connect(&dsn_as_role(
                &dsn,
                "role_private_worker",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let mut handle = Handle {
            rt,
            private,
            admin,
            fence,
            dsn,
            tenants: Vec::new(),
            user_id,
        };
        // A and B are provisioned with an admitted PRIVATE_DISTILL_TEXT route; C deliberately is
        // not ("onboarded before its route was admitted"); D has a COMPLETE, admitted route bound
        // to another deployment's egress processor (card 16).
        for (label, binding_egress) in [
            (
                "private derived_dispatch_e2e tenant A",
                Some(EGRESS_PROCESSOR_ID),
            ),
            (
                "private derived_dispatch_e2e tenant B",
                Some(EGRESS_PROCESSOR_ID),
            ),
            ("private derived_dispatch_e2e tenant C (no route)", None),
            (
                "private derived_dispatch_e2e tenant D (foreign egress)",
                Some(FOREIGN_EGRESS_PROCESSOR_ID),
            ),
        ] {
            handle
                .add_tenant(label, binding_egress)
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(db_detail(&e)))?;
        }
        Ok(handle)
    }
}

impl Handle {
    /// Seeds one more tenant (pushed onto `tenants`, cleaned up with them); returns its index.
    fn add_tenant(
        &mut self,
        label: &str,
        binding_egress: Option<Uuid>,
    ) -> Result<usize, postgres::Error> {
        let tenant = seed_tenant(
            &mut self.admin,
            label,
            self.user_id,
            binding_egress,
            ENDPOINT_REF,
        )?;
        self.tenants.push(tenant);
        Ok(self.tenants.len() - 1)
    }

    /// A second reasoning domain of tenant `idx`, bound to the tenant's own route policy.
    fn add_domain(&mut self, idx: usize, label: &str) -> Uuid {
        let tenant_id = self.tenants[idx].tenant_id;
        let policy = self.tenants[idx]
            .route_policy_id
            .expect("a second domain needs the tenant's route policy");
        let domain: Uuid = self
            .admin
            .query_one(
                "INSERT INTO control.private_reasoning_domains (tenant_id, name, owner_user_id, status) \
                 VALUES ($1, $2, $3, 'ACTIVE') RETURNING reasoning_domain_id",
                &[&tenant_id, &label, &self.user_id],
            )
            .expect("second reasoning domain")
            .get(0);
        self.admin
            .execute(
                "INSERT INTO control.reasoning_route_bindings \
                   (tenant_id, reasoning_domain_id, purpose, route_policy_id, route_policy_version) \
                 VALUES ($1, $2, $3, $4, 1)",
                &[&tenant_id, &domain, &PURPOSE_DB, &policy],
            )
            .expect("second domain binding");
        domain
    }

    fn pass(
        &self,
        provider: &dyn UserReasoningProvider,
        config: &DistillDispatchConfig,
    ) -> DistillDispatchReport {
        self.rt
            .block_on(distill::dispatch_pass(
                &self.private,
                provider,
                reasoner_config(),
                config,
            ))
            .expect("dispatch pass")
    }

    fn tenant_ids(&self) -> Vec<Uuid> {
        self.tenants.iter().map(|t| t.tenant_id).collect()
    }

    /// (status, attempt, last_error_class, claim_generation, abandoned_claims)
    fn job(&mut self, job_id: Uuid) -> (String, i32, Option<String>, i32, i32) {
        let r = self
            .admin
            .query_one(
                "SELECT status, attempt, last_error_class, claim_generation, abandoned_claims \
                 FROM ops.jobs WHERE job_id = $1",
                &[&job_id],
            )
            .expect("job row");
        (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4))
    }

    /// The job of one Evidence (0164's idempotency key).
    fn job_of(&mut self, evidence_id: Uuid) -> Uuid {
        self.admin
            .query_one(
                "SELECT job_id FROM ops.jobs WHERE idempotency_key = 'derived-work:DERIVED_DISTILL:' || $1::uuid::text",
                &[&evidence_id],
            )
            .expect("one job per Evidence")
            .get(0)
    }

    fn outbox_status(&mut self, evidence_id: Uuid) -> String {
        self.admin
            .query_one(
                "SELECT status FROM ops.outbox WHERE evidence_id = $1",
                &[&evidence_id],
            )
            .expect("outbox row")
            .get(0)
    }

    fn calls_of(&mut self, job_id: Uuid) -> i64 {
        self.admin
            .query_one(
                "SELECT count(*) FROM ops.distill_calls WHERE job_id = $1",
                &[&job_id],
            )
            .expect("distill_calls")
            .get(0)
    }

    fn sql(&mut self, sql: &str, id: Uuid) {
        self.admin.execute(sql, &[&id]).expect("admin statement");
    }

    /// Makes a backed-off or parked job claimable now.
    fn ready_now(&mut self, job_id: Uuid) {
        self.sql(
            "UPDATE ops.jobs SET next_retry_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            job_id,
        );
    }

    /// ADR-0058 §2.1 I-SLOT over the whole table.
    fn assert_i_slot(&mut self) {
        assert_eq!(i_slot_violations(&mut self.admin), 0, "I-SLOT");
    }
}

fn i_slot_violations(admin: &mut Client) -> i64 {
    admin
        .query_one(
            "SELECT count(*) FROM ops.jobs j \
             WHERE j.job_type = 'DERIVED_DISTILL' AND j.status = 'PROCESSING' \
               AND j.dispatch_state IS NOT NULL \
               AND (SELECT count(*) FROM ops.provider_slots s \
                    WHERE s.job_id = j.job_id AND s.claim_generation = j.claim_generation) <> 1",
            &[],
        )
        .expect("I-SLOT probe")
        .get(0)
}

/// `binding_egress`: `Some(id)` seeds an admitted `PRIVATE_DISTILL_TEXT` route whose endpoint
/// carries that egress processor; `None` seeds no route at all.
fn seed_tenant(
    admin: &mut Client,
    label: &str,
    user_id: Uuid,
    binding_egress: Option<Uuid>,
    endpoint_ref: &str,
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
        &[&tenant_id, &user_id],
    )?;
    let reasoning_domain_id: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains \
               (tenant_id, name, owner_user_id, status) \
             VALUES ($1, $2, $3, 'ACTIVE') RETURNING reasoning_domain_id",
            &[&tenant_id, &label, &user_id],
        )?
        .get(0);
    let route_policy_id = match binding_egress {
        Some(egress) => Some(seed_route_binding(
            admin,
            tenant_id,
            user_id,
            reasoning_domain_id,
            label,
            egress,
            endpoint_ref,
            MODEL_ID,
        )?),
        None => None,
    };
    Ok(SeededTenant {
        tenant_id,
        reasoning_domain_id,
        route_policy_id,
    })
}

/// The admitted `PRIVATE_DISTILL_TEXT` binding the dispatcher resolves per claimed job, mirrored
/// from `tests/distill_hop_e2e.rs::setup_db`. Returns the route policy (a second domain of the
/// tenant binds it too). `endpoint_ref` must equal the worker provider's endpoint for the route
/// to match (`provider_matches_admission`); so must `model_id`.
#[allow(clippy::too_many_arguments)] // one route graph's identity: tenant, owner, domain, lane
fn seed_route_binding(
    admin: &mut Client,
    tenant_id: Uuid,
    user_id: Uuid,
    reasoning_domain_id: Uuid,
    label: &str,
    egress_processor_id: Uuid,
    endpoint_ref: &str,
    model_id: &str,
) -> Result<Uuid, postgres::Error> {
    let route = seed_reasoning_profile(
        admin,
        tenant_id,
        user_id,
        label,
        egress_processor_id,
        endpoint_ref,
        model_id,
    )?;
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
            &model_id,
            &route.endpoint_id,
            &endpoint_ref,
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
    Ok(policy)
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
    egress_processor_id: Uuid,
    endpoint_ref: &str,
    model_id: &str,
) -> Result<SeededRoute, postgres::Error> {
    let credential: Uuid = admin
        .query_one(
            "INSERT INTO control.credentials (tenant_id, purpose, openbao_ref) \
             VALUES ($1, 'USER_REASONING', $2) RETURNING credential_id",
            &[&tenant_id, &format!("openbao://derived-dispatch/{label}")],
        )?
        .get(0);
    MAPPED_CREDENTIALS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(credential);
    // `control.processor_models` is global and append-only (0128 trigger): one catalog row per
    // (processor, model, revision) across every run of every test in this workspace.
    admin.execute(
        "INSERT INTO control.processor_models \
           (processor_id, provider_model_id, model_revision, capabilities, status, \
            catalog_observed_at) \
         VALUES ($1, $2, NULL, ARRAY['TEXT','STRUCTURED_OUTPUT'], 'ACTIVE', clock_timestamp()) \
         ON CONFLICT DO NOTHING",
        &[&PROVIDER_ID, &model_id],
    )?;
    let processor_model_id: Uuid = admin
        .query_one(
            "SELECT processor_model_id FROM control.processor_models \
             WHERE processor_id = $1 AND provider_model_id = $2 AND model_revision IS NULL \
               AND status = 'ACTIVE'",
            &[&PROVIDER_ID, &model_id],
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
                &endpoint_ref,
                &egress_processor_id,
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
/// `derived_distill_work_enqueue` trigger fires on — this file never writes an `ops.jobs` row.
/// `text` lands in the event payload, i.e. in the prompt the stub provider sees.
fn accept_evidence_in(handle: &mut Handle, tenant_id: Uuid, domain: Uuid, text: &str) -> Uuid {
    let mut txn = handle.admin.transaction().expect("begin remember txn");
    let evidence_id: Uuid = txn
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', sha256(convert_to(gen_random_uuid()::text, 'UTF8')), \
                     'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $2) \
             RETURNING evidence_id",
            &[&tenant_id, &domain],
        )
        .expect("insert evidence")
        .get(0);
    txn.execute(
        "INSERT INTO private.events (event_id, event_kind, payload) \
         VALUES ($1, 'USER_MESSAGE', jsonb_build_object('text', $2::text))",
        &[&evidence_id, &text],
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

/// One accepted Evidence in tenant `idx`'s first domain.
fn accept_evidence(handle: &mut Handle, idx: usize) -> Uuid {
    accept_evidence_marked(handle, idx, "")
}

fn accept_evidence_marked(handle: &mut Handle, idx: usize, text: &str) -> Uuid {
    let (tenant, domain) = (
        handle.tenants[idx].tenant_id,
        handle.tenants[idx].reasoning_domain_id,
    );
    accept_evidence_in(handle, tenant, domain, text)
}

fn jobs_of(handle: &mut Handle, idx: usize) -> Vec<(Uuid, String, serde_json::Value)> {
    let tenant_id = handle.tenants[idx].tenant_id;
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

/// Distilled `private.memory_records` rows of one seeded tenant ("assert row counts, not logs").
fn memory_count(handle: &mut Handle, idx: usize) -> i64 {
    let tenant_id = handle.tenants[idx].tenant_id;
    handle
        .admin
        .query_one(
            "SELECT count(*) FROM private.memory_records WHERE tenant_id = $1",
            &[&tenant_id],
        )
        .expect("count memories")
        .get(0)
}

fn run(name: &str, body: impl FnOnce(Handle)) {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(name, body);
}

/// One pass discovers and COMPLETES work in two tenants with no tenant id in its inputs — proved
/// by the distilled row counts. A second pass writes nothing new: no double distill.
#[test]
fn dispatch_discovers_pending_evidence_in_two_tenants() {
    run(
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
            let provider = StubProvider::one_memory(Duration::ZERO);
            let report = handle.pass(&provider, &dispatch_config("private-worker-one", 60.0));
            assert_eq!(
                report.claimed, 2,
                "one pass must find BOTH tenants: {report:?}"
            );
            assert_eq!(report.completed, 2, "{report:?}");
            assert_eq!(report.attempts, 2, "{report:?}");
            assert_eq!(report.memories, 2, "{report:?}");
            assert_eq!(provider.calls(), 2, "one provider call per Evidence");
            for tenant in 0..2 {
                assert_eq!(memory_count(&mut handle, tenant), 1);
                assert_eq!(jobs_of(&mut handle, tenant)[0].1, "DONE");
            }
            let again = handle.pass(&provider, &dispatch_config("private-worker-one", 60.0));
            assert_eq!(again.claimed, 0, "{again:?}");
            assert_eq!(provider.calls(), 2, "a second pass must not re-infer");
            handle.assert_i_slot();
        },
    );
}

/// E1 (debt 1d) — one hung provider call (3 s, then `RetryWait`) for ONE Evidence of tenant A
/// while tenant B has three rows; lease 1 s, four seats, a second claimer sweeping every 100 ms.
/// ⇒ every B row DONE before A's call ends, `lost_lease == 0`, `heartbeat_lost == 0`, the hung job
/// never seen EXECUTION_UNCERTAIN, then PENDING with attempt 1 and a backoff. Faults: (a) remove
/// the heartbeat future from the per-job select ⇒ the sweep turns the hung job UNCERTAIN (its
/// lease is gone; ADR-0058 D-G keeps the slot and accepts the same-generation settle, so nothing
/// else breaks); (b) seats forced to 1 ⇒ a B row waits behind the hang. Red on v1's serial pass
/// (card 30b evidence).
#[test]
fn one_hung_provider_call_does_not_cost_siblings_their_leases_or_stall_another_tenant() {
    run(
        "one_hung_provider_call_does_not_cost_siblings_their_leases_or_stall_another_tenant",
        |mut handle| {
            let hung = accept_evidence_marked(&mut handle, 0, "HANG");
            let b_rows: Vec<Uuid> = (0..3).map(|_| accept_evidence(&mut handle, 1)).collect();
            let provider = StubProvider::new(|prompt| {
                if prompt.contains("HANG") {
                    (Duration::from_secs(3), Reply::RetryWait)
                } else {
                    (Duration::ZERO, Reply::Json(ONE_MEMORY_REPLY.to_string()))
                }
            });
            let hung_job = handle.job_of(hung);
            // A second claimer sweeps every 100 ms while the pass runs (so an expired lease would
            // be acted on), and the sampler records the hung job's state: with its heartbeat the
            // job stays DISPATCH_INTENT for the whole hang; without it, T5 makes it UNCERTAIN.
            let stop = std::sync::Arc::new(AtomicBool::new(false));
            let sampler = {
                let (stop, dsn) = (stop.clone(), handle.dsn.clone());
                std::thread::spawn(move || {
                    // dep: PostgreSQL(owner) — state sampler
                    let mut admin = Client::connect(&dsn, NoTls).expect("sampler connection");
                    // dep: PostgreSQL(role_private_worker) — the sweeping claimer
                    let mut sweeper =
                        Client::connect(&dsn_as_role(&dsn, "role_private_worker"), NoTls)
                            .expect("sweeper connection");
                    std::thread::sleep(Duration::from_millis(300));
                    let mut uncertain = 0_u32;
                    while !stop.load(Ordering::SeqCst) {
                        let swept = sweeper
                            .query("SELECT job_id FROM ops.claim_derived_work_v2('c32-e1-sweeper', 1, 12)", &[])
                            .expect("sweep claim");
                        assert!(swept.is_empty(), "nothing else is READY");
                        let state: Option<String> = admin
                            .query_one(
                                "SELECT dispatch_state FROM ops.jobs WHERE job_id = $1",
                                &[&hung_job],
                            )
                            .expect("sample")
                            .get(0);
                        uncertain += u32::from(state.as_deref() == Some("EXECUTION_UNCERTAIN"));
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    uncertain
                })
            };
            let started = std::time::Instant::now();
            let report = handle.pass(&provider, &dispatch_config("c32-e1", 1.0));
            stop.store(true, Ordering::SeqCst);
            let uncertain_samples = sampler.join().expect("sampler");
            println!(
                "E1 report {report:?} wall {:?} uncertain_samples {uncertain_samples}",
                started.elapsed()
            );
            assert_eq!(
                uncertain_samples, 0,
                "the hung job's own heartbeat keeps its lease through the hang"
            );
            assert_eq!(report.lost_lease, 0, "{report:?}");
            assert_eq!(report.heartbeat_lost, 0, "{report:?}");
            assert_eq!(report.completed, 3, "{report:?}");
            assert_eq!(report.deferred, 1, "{report:?}");
            let hung_settled: bool = handle
                .admin
                .query_one(
                    "SELECT (SELECT max(o.processed_at) FROM ops.outbox o WHERE o.evidence_id = ANY($1)) \
                          < (SELECT l.called_at + make_interval(secs => l.latency_ms / 1000.0) \
                             FROM ops.distill_calls c JOIN ops.model_call_ledger l USING (model_call_id) \
                             WHERE c.job_id = $2)",
                    &[&b_rows, &hung_job],
                )
                .expect("ordering probe")
                .get(0);
            assert!(
                hung_settled,
                "every B row must be DONE before A's hung call ends"
            );
            for row in &b_rows {
                assert_eq!(handle.outbox_status(*row), "DONE");
            }
            let (status, attempt, class, ..) = handle.job(hung_job);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref()),
                ("PENDING", 1, Some("RETRY_WAIT"))
            );
            let backed_off: bool = handle
                .admin
                .query_one(
                    "SELECT next_retry_at > clock_timestamp() FROM ops.jobs WHERE job_id = $1",
                    &[&hung_job],
                )
                .unwrap()
                .get(0);
            assert!(
                backed_off,
                "a failing job backs off and does not head the next pass"
            );
            handle.assert_i_slot();
        },
    );
}

/// E2 (debt 2) — 20 never-ready tenants × 10 jobs, all older than the ready tenant B's one job.
/// B is claimed within the first 21 claims of the pass and completes; the never-ready jobs spend
/// no attempt. Fault: global-FIFO job pick (T2 fault) ⇒ B is claim #201. Red on v1's global FIFO.
#[test]
fn twenty_never_ready_tenants_cannot_delay_the_ready_tenant() {
    run(
        "twenty_never_ready_tenants_cannot_delay_the_ready_tenant",
        |mut handle| {
            let mut never_ready = Vec::new();
            for n in 0..20 {
                let idx = handle
                    .add_tenant(
                        &format!("private derived_dispatch_e2e never-ready tenant {n}"),
                        None,
                    )
                    .expect("never-ready tenant");
                for _ in 0..10 {
                    accept_evidence(&mut handle, idx);
                }
                never_ready.push(handle.tenants[idx].tenant_id);
            }
            let b = accept_evidence(&mut handle, 1);
            let start_turn: i64 = handle
                .admin
                .query_one(
                    "SELECT next_turn FROM ops.provider_arbiters WHERE budget = 'PRIVATE_REASONING'",
                    &[],
                )
                .unwrap()
                .get(0);
            let provider = StubProvider::one_memory(Duration::ZERO);
            let started = std::time::Instant::now();
            let report = handle.pass(&provider, &dispatch_config("c32-e2", 60.0));
            let wall = started.elapsed();
            let b_turn: i64 = handle
                .admin
                .query_one(
                    "SELECT last_served_turn FROM ops.distill_tenant_scheduler WHERE tenant_id = $1",
                    &[&handle.tenants[1].tenant_id],
                )
                .unwrap()
                .get(0);
            println!(
                "E2 report {report:?} wall {wall:?} start_turn {start_turn} b_turn {b_turn} (claim #{})",
                b_turn - start_turn + 1
            );
            assert_eq!(
                handle.outbox_status(b),
                "DONE",
                "the ready tenant completes"
            );
            assert!(
                b_turn - start_turn <= 21,
                "B must be served within the first 21 claims, was claim #{}",
                b_turn - start_turn + 1
            );
            let spent: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.jobs WHERE tenant_id = ANY($1) AND (attempt > 0 OR status = 'DEAD')",
                    &[&never_ready],
                )
                .unwrap()
                .get(0);
            assert_eq!(
                spent, 0,
                "a never-ready job spends no attempt and never dies"
            );
            assert_eq!(provider.calls(), 1, "only B reaches the provider");
            handle.assert_i_slot();
        },
    );
}

/// E3 (P1-4) — one tenant, two reasoning domains: both Evidence are distilled, none FAILED. The
/// second domain's Evidence is committed first and the first domain's job is forced to be
/// claimed first. Fault: take the tenant's oldest open outbox row instead of the job's own
/// `evidence_id` ⇒ the first job reads the other domain's Evidence (DOMAIN_MISMATCH, not done).
#[test]
fn one_tenant_two_reasoning_domains_distills_both_and_fails_none() {
    run(
        "one_tenant_two_reasoning_domains_distills_both_and_fails_none",
        |mut handle| {
            let d2 = handle.add_domain(0, "private derived_dispatch_e2e tenant A domain 2");
            let tenant = handle.tenants[0].tenant_id;
            let d1 = handle.tenants[0].reasoning_domain_id;
            let e2 = accept_evidence_in(&mut handle, tenant, d2, "second domain");
            let e1 = accept_evidence_in(&mut handle, tenant, d1, "first domain");
            let j1 = handle.job_of(e1);
            handle.sql(
                "UPDATE ops.jobs SET created_at = created_at - interval '1 hour' WHERE job_id = $1",
                j1,
            );
            let provider = StubProvider::one_memory(Duration::ZERO);
            let mut config = dispatch_config("c32-e3", 60.0);
            config.in_flight = 1;
            let report = handle.pass(&provider, &config);
            println!("E3 report {report:?}");
            assert_eq!(report.completed, 2, "{report:?}");
            assert_eq!(report.failed, 0, "{report:?}");
            assert_eq!(report.not_ready, 0, "{report:?}");
            for evidence in [e1, e2] {
                assert_eq!(handle.outbox_status(evidence), "DONE");
                let job = handle.job_of(evidence);
                assert_eq!(handle.job(job).0, "DONE");
            }
            assert_eq!(memory_count(&mut handle, 0), 2);
            handle.assert_i_slot();
        },
    );
}

/// E4 (ADR-0058 D-D) — a job whose payload names another (bound) domain than its Evidence goes
/// back to PENDING as NOT_READY `DOMAIN_MISMATCH`; nothing is processed, nothing FAILED. Fault:
/// remove the `evidence_domain` comparison ⇒ the other domain's binding is used and the Evidence
/// is settled FAILED.
#[test]
fn a_job_whose_domain_does_not_match_its_evidence_returns_to_pending_never_failed() {
    run(
        "a_job_whose_domain_does_not_match_its_evidence_returns_to_pending_never_failed",
        |mut handle| {
            let d2 = handle.add_domain(0, "private derived_dispatch_e2e tenant A domain 2");
            let evidence = accept_evidence(&mut handle, 0);
            let job = handle.job_of(evidence);
            handle
                .admin
                .execute(
                    "UPDATE ops.jobs SET payload = jsonb_set(payload, '{reasoning_domain_id}', to_jsonb($2::uuid::text)) \
                     WHERE job_id = $1",
                    &[&job, &d2],
                )
                .expect("rewrite the payload domain");
            let provider = StubProvider::one_memory(Duration::ZERO);
            let report = handle.pass(&provider, &dispatch_config("c32-e4", 60.0));
            assert_eq!(report.not_ready, 1, "{report:?}");
            assert_eq!(handle.outbox_status(evidence), "PENDING");
            let (status, attempt, class, ..) = handle.job(job);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref()),
                ("PENDING", 0, Some("DOMAIN_MISMATCH"))
            );
            let runs: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM private.processing_runs WHERE evidence_id = $1",
                    &[&evidence],
                )
                .unwrap()
                .get(0);
            assert_eq!(runs, 0, "nothing was processed");
            assert_eq!(provider.calls(), 0);
            handle.assert_i_slot();
        },
    );
}

/// E5 (scope 4) — a provider that always answers `RetryWait` for one Evidence: the job backs
/// off, then is DEAD with class `RETRY_WAIT` after `max_attempts` (2) admitted calls — attempt ==
/// ledger rows == `distill_calls` rows — and its outbox row is FAILED; the tenant's other
/// Evidence (another domain) is DONE. Fault: drop the `attempt >= max_attempts ⇒ DEAD` branch.
#[test]
fn a_failing_provider_backs_off_and_dies_after_max_attempts_with_its_class() {
    run(
        "a_failing_provider_backs_off_and_dies_after_max_attempts_with_its_class",
        |mut handle| {
            let d2 = handle.add_domain(0, "private derived_dispatch_e2e tenant A domain 2");
            let tenant = handle.tenants[0].tenant_id;
            let poison = accept_evidence_marked(&mut handle, 0, "FAIL");
            let good = accept_evidence_in(&mut handle, tenant, d2, "fine");
            let provider = StubProvider::new(|prompt| {
                if prompt.contains("FAIL") {
                    (Duration::ZERO, Reply::RetryWait)
                } else {
                    (Duration::ZERO, Reply::Json(ONE_MEMORY_REPLY.to_string()))
                }
            });
            let mut config = dispatch_config("c32-e5", 60.0);
            config.max_attempts = 2;
            let first = handle.pass(&provider, &config);
            assert_eq!((first.deferred, first.completed), (1, 1), "{first:?}");
            let job = handle.job_of(poison);
            assert_eq!(handle.job(job).0, "PENDING", "backed off, not dead yet");
            handle.ready_now(job);
            let second = handle.pass(&provider, &config);
            assert_eq!(second.dead, 1, "{second:?}");
            let (status, attempt, class, ..) = handle.job(job);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref()),
                ("DEAD", 2, Some("RETRY_WAIT"))
            );
            assert_eq!(handle.outbox_status(poison), "FAILED");
            assert_eq!(handle.outbox_status(good), "DONE");
            assert_eq!(handle.calls_of(job), 2);
            let failed_ledger: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id = $1 AND status = 'FAILED'",
                    &[&tenant],
                )
                .unwrap()
                .get(0);
            assert_eq!(
                failed_ledger, 2,
                "attempt == ledger rows == distill_calls rows"
            );
            let again = handle.pass(&provider, &config);
            assert_eq!(
                again.claimed, 0,
                "a DEAD job is never claimed again: {again:?}"
            );
            handle.assert_i_slot();
        },
    );
}

/// E6 (ADR-0058 D-H) — a tenant with no binding: NOT_READY spends no attempt and, once not ready
/// for the park age, parks as `WAITING_KEY` with its class. Fault: admit a provider request
/// (`ops.begin_call`) before the binding is resolved ⇒ the attempt is counted.
#[test]
fn not_ready_never_spends_an_attempt_and_parks_after_the_age() {
    run(
        "not_ready_never_spends_an_attempt_and_parks_after_the_age",
        |mut handle| {
            let evidence = accept_evidence(&mut handle, 2);
            let job = handle.job_of(evidence);
            let mut config = dispatch_config("c32-e6", 60.0);
            config.not_ready_park_seconds = 1.0;
            let provider = never_called();
            let first = handle.pass(&provider, &config);
            assert_eq!(first.not_ready, 1, "{first:?}");
            let (status, attempt, class, ..) = handle.job(job);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref()),
                ("PENDING", 0, Some(NO_BINDING_REASON))
            );
            assert_eq!(handle.calls_of(job), 0);
            std::thread::sleep(Duration::from_millis(1200));
            handle.ready_now(job);
            let second = handle.pass(&provider, &config);
            assert_eq!(second.parked, 1, "{second:?}");
            let (status, attempt, class, ..) = handle.job(job);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref()),
                ("WAITING_KEY", 0, Some(NO_BINDING_REASON)),
                "parked past the age, never DEAD, no attempt"
            );
            assert_eq!(handle.outbox_status(evidence), "PENDING");
            handle.assert_i_slot();
        },
    );
}

/// T25 (card-33 acceptance, ADR-0059 D-I) — tenant A's route is fully admitted, but its
/// credential reference is not in the worker's key map: the job is NOT_READY
/// `CREDENTIAL_NOT_MAPPED`, then parks `WAITING_KEY` past the age, with no attempt, no
/// `ops.model_call_ledger` / `ops.distill_calls` row and no provider call. Fault: remove the
/// `read_leg` pre-check ⇒ the request is reserved and sent (the stub answers) ⇒ red.
#[test]
fn foreign_credential_ref_parks_waiting_key_without_ledger_row() {
    run(
        "foreign_credential_ref_parks_waiting_key_without_ledger_row",
        |mut handle| {
            let tenant_id = handle.tenants[0].tenant_id;
            let credential_ref: Uuid = handle
                .admin
                .query_one(
                    "SELECT credential_ref FROM control.reasoning_profiles WHERE tenant_id = $1",
                    &[&tenant_id],
                )
                .expect("tenant A's route credential")
                .get(0);
            let evidence = accept_evidence(&mut handle, 0);
            let job = handle.job_of(evidence);
            let mut config = dispatch_config("c33-t25", 60.0);
            assert!(
                config.credential_refs.remove(&credential_ref),
                "A was mapped"
            );
            config.not_ready_park_seconds = 1.0;
            let provider = StubProvider::one_memory(Duration::ZERO);
            let first = handle.pass(&provider, &config);
            assert_eq!((first.not_ready, first.attempts), (1, 0), "{first:?}");
            let (status, attempt, class, ..) = handle.job(job);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref()),
                ("PENDING", 0, Some(distill::CREDENTIAL_NOT_MAPPED))
            );
            std::thread::sleep(Duration::from_millis(1200));
            handle.ready_now(job);
            let second = handle.pass(&provider, &config);
            assert_eq!(second.parked, 1, "{second:?}");
            let (status, attempt, class, ..) = handle.job(job);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref()),
                ("WAITING_KEY", 0, Some(distill::CREDENTIAL_NOT_MAPPED)),
                "parked with the named class, never DEAD, no attempt"
            );
            let ledger: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id = $1",
                    &[&tenant_id],
                )
                .expect("ledger count")
                .get(0);
            assert_eq!(
                (ledger, handle.calls_of(job), provider.calls()),
                (0, 0, 0),
                "no ledger row, no distill call, no provider call"
            );
            assert_eq!(handle.outbox_status(evidence), "PENDING", "not stranded");
            handle.assert_i_slot();
        },
    );
}

/// E7 (§11) — a provider that always answers 401: every round parks the job `WAITING_KEY` with
/// attempt 0 and the outbox row open, even with `max_attempts = 1`; the physical calls stay in
/// the ledger and in `ops.distill_calls`. Fault: drop the WAITING_KEY revert in
/// `ops.finish_derived_work_v2` ⇒ round 2 sees attempt 1 = max ⇒ DEAD.
#[test]
fn a_401_parks_waiting_key_and_never_dies() {
    run("a_401_parks_waiting_key_and_never_dies", |mut handle| {
        let evidence = accept_evidence(&mut handle, 0);
        let job = handle.job_of(evidence);
        let provider = StubProvider::new(|_| (Duration::ZERO, Reply::WaitingKey));
        let mut config = dispatch_config("c32-e7", 60.0);
        config.max_attempts = 1;
        config.not_ready_park_seconds = 1.0;
        for round in 1..=3 {
            let report = handle.pass(&provider, &config);
            assert_eq!(report.parked, 1, "round {round}: {report:?}");
            let (status, attempt, class, ..) = handle.job(job);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref()),
                ("WAITING_KEY", 0, Some("WAITING_KEY")),
                "round {round}"
            );
            assert_eq!(handle.outbox_status(evidence), "PENDING", "round {round}");
            handle.ready_now(job);
        }
        assert_eq!(handle.calls_of(job), 3);
        let failed: i64 = handle
            .admin
            .query_one(
                "SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id = $1 AND status = 'FAILED'",
                &[&handle.tenants[0].tenant_id],
            )
            .unwrap()
            .get(0);
        assert_eq!(failed, 3, "every 401 call is ledgered");
        handle.assert_i_slot();
    });
}

/// E8 — two in-process dispatchers (different owners, separate pools) over one backlog: every
/// Evidence is called once and distilled once; the double-spend report is clean. Fault: drop
/// `SKIP LOCKED` + the restated eligibility on the job pick (recorded in ADR-0058: the arbiter
/// lock serializes claims, so this pair is defence in depth).
#[test]
fn two_dispatchers_over_one_backlog_call_each_evidence_once() {
    run(
        "two_dispatchers_over_one_backlog_call_each_evidence_once",
        |mut handle| {
            let mut evidence = Vec::new();
            for _ in 0..6 {
                evidence.push(accept_evidence(&mut handle, 0));
                evidence.push(accept_evidence(&mut handle, 1));
            }
            let second_pool = handle
                .rt
                // dep: PostgreSQL(role_private_worker) — role-scoped pool call
                .block_on(PrivateWorkerDbPool::connect(&dsn_as_role(
                    &handle.dsn,
                    "role_private_worker",
                )))
                .expect("second pool");
            let provider = StubProvider::one_memory(Duration::from_millis(50));
            let (config_one, config_two) = (
                dispatch_config("c32-e8-one", 30.0),
                dispatch_config("c32-e8-two", 30.0),
            );
            let (one, two) = handle.rt.block_on(async {
                tokio::join!(
                    distill::dispatch_pass(
                        &handle.private,
                        &provider,
                        reasoner_config(),
                        &config_one,
                    ),
                    distill::dispatch_pass(&second_pool, &provider, reasoner_config(), &config_two)
                )
            });
            let (one, two) = (one.expect("dispatcher one"), two.expect("dispatcher two"));
            println!("E8 one {one:?} two {two:?}");
            assert_eq!(one.completed + two.completed, 12);
            assert_eq!(one.lost_lease + two.lost_lease, 0);
            assert_eq!(provider.calls(), 12, "each Evidence is called once");
            let tenants = handle.tenant_ids();
            let report = double_spend::report(
                &mut handle.admin,
                &tenants,
                3,
                config_one.hard_deadline_seconds,
            );
            println!("E8 {}", report.line());
            assert_eq!((report.duplicates, report.overlaps), (0, 0));
            assert_eq!(report.unattributed_succeeded, 0);
            for row in &evidence {
                assert_eq!(handle.outbox_status(*row), "DONE");
            }
            assert_eq!(
                memory_count(&mut handle, 0) + memory_count(&mut handle, 1),
                12
            );
            assert!(
                provider.max_live.load(Ordering::SeqCst) <= 4,
                "never more than four calls in flight across both dispatchers"
            );
            handle.assert_i_slot();
        },
    );
}

/// E13 (ADR-0058 D-E, card-32 review) — a DB error AFTER a counted call (here: the zero-candidate
/// re-ask's run-close transaction hits `lock_timeout` on a run row another transaction holds) is
/// settled by ONE fenced RETRY in a fresh transaction: the job is PENDING with its class, its slot
/// is free and nothing waits for `hard_deadline` or comes back as EXECUTION_UNCERTAIN. Fault: let
/// the error propagate with `?` from the call loop (no settle) → the job stays
/// PROCESSING/DISPATCH_INTENT holding its slot, reported `lost_lease`.
#[test]
fn a_db_error_after_a_counted_call_settles_one_fenced_retry() {
    run(
        "a_db_error_after_a_counted_call_settles_one_fenced_retry",
        |mut handle| {
            let tenant = handle.tenants[0].tenant_id;
            let evidence = accept_evidence(&mut handle, 0);
            let job = handle.job_of(evidence);
            let lock_holders: std::sync::Arc<Mutex<Vec<std::thread::JoinHandle<()>>>> =
                std::sync::Arc::default();
            let provider = {
                let (dsn, holders) = (handle.dsn.clone(), std::sync::Arc::clone(&lock_holders));
                StubProvider::new(move |_| {
                    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
                    let dsn = dsn.clone();
                    holders.lock().unwrap().push(std::thread::spawn(move || {
                        // dep: PostgreSQL(owner) — holds the open processing run row past the
                        // worker's lock_timeout, as a slow concurrent writer would
                        let mut holder = Client::connect(&dsn, NoTls).expect("lock holder");
                        holder.batch_execute("BEGIN").unwrap();
                        holder
                            .query(
                                "SELECT 1 FROM private.processing_runs \
                                 WHERE tenant_id = $1 AND completed_at IS NULL FOR UPDATE",
                                &[&tenant],
                            )
                            .unwrap();
                        locked_tx.send(()).unwrap();
                        std::thread::sleep(Duration::from_secs(4));
                        holder.batch_execute("ROLLBACK").unwrap();
                    }));
                    locked_rx.recv().expect("run row locked");
                    (
                        Duration::ZERO,
                        Reply::Json(r#"{"memories":[]}"#.to_string()),
                    )
                })
            };
            let short_lock_pool = handle
                .rt
                // dep: PostgreSQL(role_private_worker) — role-scoped pool call
                .block_on(PrivateWorkerDbPool::connect(&format!(
                    "{}{}options=-c%20lock_timeout%3D1000",
                    dsn_as_role(&handle.dsn, "role_private_worker"),
                    if handle.dsn.contains('?') { '&' } else { '?' }
                )))
                .expect("lock_timeout pool");
            let config = dispatch_config("c32-e13", 30.0);
            let report = handle
                .rt
                .block_on(distill::dispatch_pass(
                    &short_lock_pool,
                    &provider,
                    reasoner_config(),
                    &config,
                ))
                .expect("dispatch pass");
            for holder in lock_holders.lock().unwrap().drain(..) {
                holder.join().expect("lock holder");
            }
            println!("E13 {report:?}");
            assert_eq!(provider.calls(), 1);
            assert_eq!(
                (report.deferred, report.lost_lease, report.unknown),
                (1, 0, 0),
                "{report:?}"
            );
            let (status, attempt, class, generation, _) = handle.job(job);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref(), generation),
                ("PENDING", 1, Some("WORKER_DB_ERROR"), 1),
                "one fenced RETRY with the error's class, the call counted"
            );
            let slots: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.provider_slots WHERE job_id = $1",
                    &[&job],
                )
                .unwrap()
                .get(0);
            assert_eq!(slots, 0, "the slot is freed now, not at hard_deadline");
            assert_eq!(handle.calls_of(job), 1);
            assert_eq!(handle.outbox_status(evidence), "PENDING");
            handle.assert_i_slot();
        },
    );
}

/// E14 (ADR-0058 R3) — an error that escapes the job (here: taking the Evidence's outbox row hits
/// `lock_timeout` on a row another transaction holds, before anything could be settled) is counted
/// as `errors` with its class, never as a lost lease: the fence refused nothing. Fault: report an
/// escaped error as `LeaseLost` → `lost_lease = 1, errors = 0`.
#[test]
fn an_escaped_job_error_is_counted_as_an_error_not_a_lost_lease() {
    run(
        "an_escaped_job_error_is_counted_as_an_error_not_a_lost_lease",
        |mut handle| {
            let evidence = accept_evidence(&mut handle, 0);
            let job = handle.job_of(evidence);
            let (locked_tx, locked_rx) = std::sync::mpsc::channel();
            let holder = {
                let dsn = handle.dsn.clone();
                std::thread::spawn(move || {
                    // dep: PostgreSQL(owner) — holds the outbox row past the worker's lock_timeout
                    let mut holder = Client::connect(&dsn, NoTls).expect("lock holder");
                    holder.batch_execute("BEGIN").unwrap();
                    holder
                        .query(
                            "SELECT 1 FROM ops.outbox WHERE evidence_id = $1 FOR UPDATE",
                            &[&evidence],
                        )
                        .unwrap();
                    locked_tx.send(()).unwrap();
                    std::thread::sleep(Duration::from_secs(3));
                    holder.batch_execute("ROLLBACK").unwrap();
                })
            };
            locked_rx.recv().expect("outbox row locked");
            let short_lock_pool = handle
                .rt
                // dep: PostgreSQL(role_private_worker) — role-scoped pool call
                .block_on(PrivateWorkerDbPool::connect(&format!(
                    "{}{}options=-c%20lock_timeout%3D1000",
                    dsn_as_role(&handle.dsn, "role_private_worker"),
                    if handle.dsn.contains('?') { '&' } else { '?' }
                )))
                .expect("lock_timeout pool");
            let provider = StubProvider::one_memory(Duration::ZERO);
            let report = handle
                .rt
                .block_on(distill::dispatch_pass(
                    &short_lock_pool,
                    &provider,
                    reasoner_config(),
                    &dispatch_config("c32-e14", 30.0),
                ))
                .expect("dispatch pass");
            holder.join().expect("lock holder");
            println!("E14 {report:?}");
            assert_eq!(provider.calls(), 0, "nothing was sent");
            assert_eq!(
                (report.claimed, report.errors, report.lost_lease),
                (1, 1, 0),
                "{report:?}"
            );
            assert!(report.summary_line().contains(" errors=1 "), "{report:?}");
            let (status, attempt, _, _, _) = handle.job(job);
            assert_eq!(
                (status.as_str(), attempt),
                ("PROCESSING", 0),
                "unsettled: the claim reconciles through the T4 sweep"
            );
            handle.assert_i_slot();
        },
    );
}

/// ADR-0058 D-M (main-line ruling 2026-10-02 10:35, test 3) + §78.1: the endpoint's capabilities
/// and the credential map (ADR-0059 D-I, which replaced card 32's single key variable; the test
/// keeps its card-32 name, a gate greps it) are deployment configuration with no code default —
/// the worker refuses to start without either, naming the key, and refuses the removed
/// `HUMAUX_PRIVATE_WORKER_KEY_ENV`. Fault: a literal default for `HUMAUX_PRIVATE_WORKER_CAPABILITIES`
/// or `_CREDENTIALS` → the process gets past bootstrap and fails elsewhere (or not at all).
#[test]
fn the_worker_refuses_to_boot_without_capabilities_or_key_env() {
    for missing in [
        "HUMAUX_PRIVATE_WORKER_CAPABILITIES",
        "HUMAUX_PRIVATE_WORKER_CREDENTIALS",
    ] {
        // An unreachable DSN: a bootstrap that got past the configuration would fail on it with
        // a different message.
        let out = distill_serve_command("postgres://nobody@127.0.0.1:1/none")
            .env_remove(missing)
            .arg("--distill-once")
            .output()
            .expect("spawn humaux-private-worker");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(!out.status.success(), "{missing}: must not start");
        assert!(
            stderr.contains(&format!("missing required configuration: {missing}")),
            "{missing}: {stderr}"
        );
    }
    let out = distill_serve_command("postgres://nobody@127.0.0.1:1/none")
        .env("HUMAUX_PRIVATE_WORKER_KEY_ENV", "HUMAUX_CARD15_TEST_SECRET")
        .arg("--distill-once")
        .output()
        .expect("spawn humaux-private-worker");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains(
            "invalid configuration: HUMAUX_PRIVATE_WORKER_KEY_ENV was removed by ADR-0059 D-I"
        ),
        "the removed single-key variable is refused, never silently ignored"
    );
    let out = distill_serve_command("postgres://nobody@127.0.0.1:1/none")
        .env(
            "HUMAUX_PRIVATE_WORKER_CAPABILITIES",
            "STRUCTURED_OUTPUT,JSON_MODE",
        )
        .arg("--distill-once")
        .output()
        .expect("spawn humaux-private-worker");
    assert!(
        String::from_utf8_lossy(&out.stderr)
            .contains("invalid configuration: HUMAUX_PRIVATE_WORKER_CAPABILITIES"),
        "a value outside the §11.2 closed set is refused"
    );
}

/// E9 (ADR-0058 D-F) — claims that crash before any request (lease expiry while `CLAIMED`) are
/// counted in `abandoned_claims`; at the cap the job is DEAD `PRE_DISPATCH_ABANDONED` with its
/// outbox row FAILED and no call. Fault: the worker ignores `abandoned_claims`.
#[test]
fn abandoned_pre_dispatch_claims_reach_dead_at_the_cap() {
    run(
        "abandoned_pre_dispatch_claims_reach_dead_at_the_cap",
        |mut handle| {
            let evidence = accept_evidence(&mut handle, 0);
            let job = handle.job_of(evidence);
            let expire = |handle: &mut Handle| {
                handle.sql(
                    "UPDATE ops.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
                    job,
                )
            };
            for crash in 1..=2 {
                let claimed = handle
                    .rt
                    .block_on(jobs::claim_distill(
                        &handle.private,
                        "c32-e9-crash",
                        30.0,
                        300.0,
                    ))
                    .expect("raw claim")
                    .expect("the job is READY");
                assert_eq!(claimed.job_id, job);
                assert_eq!(claimed.abandoned_claims, crash - 1);
                expire(&mut handle);
            }
            let provider = never_called();
            let mut config = dispatch_config("c32-e9", 30.0);
            config.max_attempts = 2;
            let report = handle.pass(&provider, &config);
            assert_eq!(report.dead, 1, "{report:?}");
            let (status, attempt, class, _, abandoned) = handle.job(job);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref(), abandoned),
                ("DEAD", 0, Some("PRE_DISPATCH_ABANDONED"), 2)
            );
            assert_eq!(handle.outbox_status(evidence), "FAILED");
            assert_eq!(handle.calls_of(job), 0);
            handle.assert_i_slot();
        },
    );
}

/// E11 (ADR-0058 D-J, review finding 2) — a call admitted at the latest admissible moment whose
/// answer arrives just inside the HTTP window, followed by a write leg slowed past the HTTP
/// cutoff (but not past `hard_deadline`), is kept in generation 1 and never resent, while a
/// second dispatcher sweeps every 100 ms. Fault: put the deadline around the whole job future.
#[test]
#[allow(clippy::too_many_lines)]
fn a_success_at_the_end_of_the_http_window_is_kept_not_resent() {
    run(
        "a_success_at_the_end_of_the_http_window_is_kept_not_resent",
        |mut handle| {
            const HTTP: f64 = 3.0;
            const LEASE: f64 = 2.0;
            let evidence = accept_evidence(&mut handle, 0);
            let job = handle.job_of(evidence);
            let config = DistillDispatchConfig {
                lease_owner: "c32-e11".into(),
                lease_seconds: LEASE,
                in_flight: 1,
                hard_deadline_seconds: 2.0 * (HTTP + LEASE),
                http_timeout_seconds: HTTP,
                not_ready_park_seconds: 600.0,
                max_attempts: 5,
                budget: TEST_BUDGET,
                credential_refs: mapped_credentials(),
            };
            let replied = std::sync::Arc::new(AtomicBool::new(false));
            let called = std::sync::Arc::new(AtomicBool::new(false));
            let provider = {
                let (replied, called) = (replied.clone(), called.clone());
                StubProvider::new(move |_| {
                    called.store(true, Ordering::SeqCst);
                    // Answered 0.35 s before the HTTP window closes.
                    let delay = Duration::from_secs_f64(HTTP - 0.35);
                    let replied = replied.clone();
                    std::thread::spawn(move || {
                        std::thread::sleep(delay);
                        replied.store(true, Ordering::SeqCst);
                    });
                    (delay, Reply::Json(ONE_MEMORY_REPLY.to_string()))
                })
            };
            // The admin side: holds the Evidence row FOR UPDATE so the read leg's processing-run
            // INSERT (FK) waits until `hard_deadline - (http + lease) - 0.4 s` (begin_call then runs
            // at the latest admissible moment), then holds it again from the call until 1 s after
            // the answer, so the write leg's memory_evidence INSERT crosses the HTTP cutoff.
            let dsn = handle.dsn.clone();
            let (replied2, called2) = (replied.clone(), called.clone());
            let gate = std::thread::spawn(move || {
                // dep: PostgreSQL(owner) — lock-holder connection steering the job's timing
                let mut admin = Client::connect(&dsn, NoTls).expect("gate connection");
                let mut tx = admin.transaction().expect("begin");
                tx.query_one(
                    "SELECT 1 FROM private.evidence_objects WHERE evidence_id = $1 FOR UPDATE",
                    &[&evidence],
                )
                .expect("lock evidence");
                // dep: PostgreSQL(owner) — probe connection
                let mut probe = Client::connect(&dsn, NoTls).expect("probe connection");
                let wait: f64 = loop {
                    let row = probe
                        .query_one(
                            "SELECT status = 'PROCESSING', \
                                    extract(epoch FROM hard_deadline - make_interval(secs => $2) - clock_timestamp())::float8 \
                             FROM ops.jobs WHERE job_id = $1",
                            &[&job, &(HTTP + LEASE + 0.4)],
                        )
                        .expect("job probe");
                    if row.get::<_, bool>(0) {
                        break row.get::<_, Option<f64>>(1).unwrap_or(0.0);
                    }
                    std::thread::sleep(Duration::from_millis(10));
                };
                std::thread::sleep(Duration::from_secs_f64(wait.max(0.0)));
                tx.commit().expect("release the read leg");
                while !called2.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(5));
                }
                let mut tx = admin.transaction().expect("begin");
                tx.query_one(
                    "SELECT 1 FROM private.evidence_objects WHERE evidence_id = $1 FOR UPDATE",
                    &[&evidence],
                )
                .expect("lock evidence again");
                while !replied2.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(5));
                }
                std::thread::sleep(Duration::from_secs_f64(LEASE / 2.0));
                tx.commit().expect("release the write leg");
            });
            let sweeper_pool = handle
                .rt
                // dep: PostgreSQL(role_private_worker) — role-scoped pool call
                .block_on(PrivateWorkerDbPool::connect(&dsn_as_role(
                    &handle.dsn,
                    "role_private_worker",
                )))
                .expect("sweeper pool");
            let stop = AtomicBool::new(false);
            let (report, stolen) = handle.rt.block_on(async {
                let pass = async {
                    let r = distill::dispatch_pass(
                        &handle.private,
                        &provider,
                        reasoner_config(),
                        &config,
                    )
                    .await;
                    stop.store(true, Ordering::SeqCst);
                    r
                };
                let sweeper = async {
                    let mut stolen = 0;
                    while !stop.load(Ordering::SeqCst) {
                        if jobs::claim_distill(
                            &sweeper_pool,
                            "c32-e11-sweeper",
                            LEASE,
                            2.0 * (HTTP + LEASE),
                        )
                        .await
                        .expect("sweep claim")
                        .is_some()
                        {
                            stolen += 1;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    stolen
                };
                tokio::join!(pass, sweeper)
            });
            gate.join().expect("gate thread");
            let report = report.expect("dispatch pass");
            println!("E11 report {report:?} stolen {stolen}");
            let (status, attempt, _, generation, _) = handle.job(job);
            assert_eq!(
                (status.as_str(), generation, attempt),
                ("DONE", 1, 1),
                "kept in generation 1, never resent"
            );
            assert_eq!(stolen, 0);
            assert_eq!(handle.calls_of(job), 1);
            let succeeded: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id = $1 AND status = 'SUCCEEDED'",
                    &[&handle.tenants[0].tenant_id],
                )
                .unwrap()
                .get(0);
            assert_eq!(succeeded, 1);
            assert_eq!(report.unknown, 0, "{report:?}");
            handle.assert_i_slot();
        },
    );
}

/// E12 (ADR-0048 D-D, review finding 6) — a malformed reply whose re-ask cannot fit before
/// `hard_deadline` (the stub moves the deadline on its first call) fails closed once: one
/// `distill_calls` row, job DEAD `FAILED_OUTPUT_SCHEMA`, outbox FAILED, and a later pass sends
/// nothing. Fault: map a refused re-ask to NOT_READY (the original prompt would be sent again).
#[test]
fn a_malformed_reply_whose_reask_cannot_fit_fails_closed_once() {
    run(
        "a_malformed_reply_whose_reask_cannot_fit_fails_closed_once",
        |mut handle| {
            let evidence = accept_evidence(&mut handle, 0);
            let job = handle.job_of(evidence);
            let dsn = handle.dsn.clone();
            let provider = StubProvider::new(move |_| {
                let dsn = dsn.clone();
                std::thread::spawn(move || {
                    // dep: PostgreSQL(owner) — moves the claim's deadline under the running call
                    let mut admin = Client::connect(&dsn, NoTls).expect("deadline connection");
                    admin
                        .execute(
                            "UPDATE ops.jobs SET hard_deadline = clock_timestamp() + interval '2 seconds' \
                             WHERE job_id = $1",
                            &[&job],
                        )
                        .expect("move hard_deadline");
                })
                .join()
                .expect("deadline thread");
                (Duration::ZERO, Reply::Json("not json".to_string()))
            });
            let report = handle.pass(&provider, &dispatch_config("c32-e12", 30.0));
            println!("E12 report {report:?}");
            assert_eq!(report.dead, 1, "{report:?}");
            let (status, attempt, class, ..) = handle.job(job);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref()),
                ("DEAD", 1, Some("FAILED_OUTPUT_SCHEMA"))
            );
            assert_eq!(handle.outbox_status(evidence), "FAILED");
            assert_eq!(handle.calls_of(job), 1);
            let again = handle.pass(&provider, &dispatch_config("c32-e12", 30.0));
            assert_eq!(
                (again.claimed, provider.calls()),
                (0, 1),
                "nothing is sent again"
            );
            handle.assert_i_slot();
        },
    );
}

/// E15 (ADR-0058 R8 a) — a reply whose memory text carries U+0000 (a live reply did, card-32
/// M3) is refused by the parser as `nul_character`: no write is attempted, one re-ask, then DEAD
/// `FAILED_OUTPUT_SCHEMA` with the outbox FAILED, and a later pass sends nothing
/// (`double_spend duplicates=0`). Fault: drop the parser's NUL refusal ⇒ PostgreSQL refuses the
/// write (`WORKER_DB_ERROR`), the job settles RETRY and the next pass calls again (duplicates > 0).
#[test]
fn a_reply_carrying_u0000_is_malformed_never_written_and_never_resent() {
    run(
        "a_reply_carrying_u0000_is_malformed_never_written_and_never_resent",
        |mut handle| {
            let evidence = accept_evidence(&mut handle, 0);
            let job = handle.job_of(evidence);
            let tenants = handle.tenant_ids();
            let provider = StubProvider::new(|_| {
                (
                    Duration::ZERO,
                    Reply::Json(
                        ONE_MEMORY_REPLY.replace("Health endpoint", "Health\\u0000endpoint"),
                    ),
                )
            });
            let config = dispatch_config("c32-r8a", 30.0);
            let first = handle.pass(&provider, &config);
            println!("R8a first {first:?}");
            handle.ready_now(job);
            let again = handle.pass(&provider, &config);
            println!("R8a again {again:?}");
            let report =
                double_spend::report(&mut handle.admin, &tenants, 3, config.hard_deadline_seconds);
            println!("R8a {}", report.line());
            assert_eq!(report.duplicates, 0, "{}", report.line());
            assert_eq!(
                (
                    first.dead,
                    first.malformed_retries,
                    first.attempts,
                    first.errors
                ),
                (1, 1, 2, 0),
                "{first:?}"
            );
            let (status, attempt, class, ..) = handle.job(job);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref()),
                ("DEAD", 2, Some("FAILED_OUTPUT_SCHEMA"))
            );
            assert_eq!(handle.outbox_status(evidence), "FAILED");
            assert_eq!((handle.calls_of(job), provider.calls()), (2, 2));
            assert_eq!(memory_count(&mut handle, 0), 0, "nothing was written");
            handle.assert_i_slot();
        },
    );
}

/// A job that distilled NOTHING (tenant C has no admitted route) is settled NOT_READY with a
/// backoff — never DONE (its Evidence would be stranded: 0164's enqueue key is per evidence_id
/// with ON CONFLICT DO NOTHING), and its outbox row stays claimable. Fault: settle DONE.
#[test]
fn a_job_that_distilled_nothing_is_not_ready_instead_of_completed() {
    run(
        "a_job_that_distilled_nothing_is_not_ready_instead_of_completed",
        |mut handle| {
            let evidence = accept_evidence(&mut handle, 2);
            let provider = never_called();
            let mut config = dispatch_config("unprovisioned-worker", 60.0);
            config.max_attempts = 1;
            let report = handle.pass(&provider, &config);
            assert_eq!(
                (
                    report.claimed,
                    report.completed,
                    report.not_ready,
                    report.dead
                ),
                (1, 0, 1, 0),
                "{report:?}"
            );
            assert_eq!(memory_count(&mut handle, 2), 0);
            let job = handle.job_of(evidence);
            let (status, attempt, class, ..) = handle.job(job);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref()),
                ("PENDING", 0, Some(NO_BINDING_REASON)),
                "released with its class, never DONE"
            );
            let backed_off: bool = handle
                .admin
                .query_one(
                    "SELECT next_retry_at > clock_timestamp() FROM ops.jobs WHERE job_id = $1",
                    &[&job],
                )
                .unwrap()
                .get(0);
            assert!(backed_off, "the release must back off");
            assert_eq!(handle.outbox_status(evidence), "PENDING", "not stranded");
            handle.assert_i_slot();
        },
    );
}

/// Card 16's P0: tenant D's route is fully admitted but bound to another deployment's egress;
/// it is NOT_READY with that class (never DEAD, no attempt), and the ready tenant claimed in the
/// same pass still completes.
#[test]
fn a_tenant_the_deployment_cannot_admit_is_named_and_does_not_starve_the_ready_one() {
    run(
        "a_tenant_the_deployment_cannot_admit_is_named_and_does_not_starve_the_ready_one",
        |mut handle| {
            accept_evidence(&mut handle, 0);
            let foreign = accept_evidence(&mut handle, 3);
            let provider = StubProvider::one_memory(Duration::ZERO);
            let mut config = dispatch_config("mixed-readiness-worker", 60.0);
            config.max_attempts = 1;
            for pass in 1..=2 {
                let report = handle.pass(&provider, &config);
                assert_eq!(report.not_ready, 1, "pass {pass}: {report:?}");
                assert_eq!(report.dead, 0, "pass {pass}: {report:?}");
                let job = handle.job_of(foreign);
                let (status, attempt, class, ..) = handle.job(job);
                assert_eq!(
                    (status.as_str(), attempt, class.as_deref()),
                    ("PENDING", 0, Some(FOREIGN_EGRESS_REASON)),
                    "pass {pass}: named, released, never DEAD"
                );
                handle.ready_now(job);
            }
            assert_eq!(
                memory_count(&mut handle, 0),
                1,
                "the ready tenant is not starved"
            );
            assert_eq!(jobs_of(&mut handle, 0)[0].1, "DONE");
            assert_eq!(
                provider.calls(),
                1,
                "only the admitted tenant reaches the provider"
            );
            handle.assert_i_slot();
        },
    );
}

/// Two racing claims take each job exactly once (ADR-0058 T1 under two owners).
#[test]
fn two_racing_claims_take_each_job_exactly_once() {
    run(
        "two_racing_claims_take_each_job_exactly_once",
        |mut handle| {
            accept_evidence(&mut handle, 0);
            accept_evidence(&mut handle, 1);
            let claim = |owner: &str| {
                handle
                    .rt
                    .block_on(jobs::claim_distill(&handle.private, owner, 60.0, 300.0))
                    .expect("claim")
            };
            let (first, second, third) =
                (claim("racer-one"), claim("racer-two"), claim("racer-three"));
            let (first, second) = (first.expect("first job"), second.expect("second job"));
            assert_ne!(first.job_id, second.job_id, "no job may be claimed twice");
            assert!(third.is_none(), "both live claims are held");
            for c in [&first, &second] {
                assert_eq!((c.attempt, c.claim_generation), (0, 1));
            }
            handle.assert_i_slot();
        },
    );
}

/// Recovery: a crashed claim's lease expires, the job is READY again (T4), the next claim takes
/// it in generation 2, and the stale generation settles nothing.
#[test]
fn expired_lease_is_reclaimed_and_the_stale_generation_settles_nothing() {
    run(
        "expired_lease_is_reclaimed_and_the_stale_generation_settles_nothing",
        |mut handle| {
            let evidence = accept_evidence(&mut handle, 0);
            let job = handle.job_of(evidence);
            let dead = handle
                .rt
                .block_on(jobs::claim_distill(
                    &handle.private,
                    "killed-worker",
                    30.0,
                    300.0,
                ))
                .expect("claim")
                .expect("job");
            handle.sql(
                "UPDATE ops.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
                job,
            );
            let survivor = handle
                .rt
                .block_on(jobs::claim_distill(
                    &handle.private,
                    "survivor",
                    30.0,
                    300.0,
                ))
                .expect("claim")
                .expect("an expired CLAIMED job is READY again");
            assert_eq!(survivor.job_id, dead.job_id);
            assert_eq!(survivor.claim_generation, 2, "the generation moved");
            let stale = handle
                .rt
                .block_on(jobs::finish_distill(
                    &handle.private,
                    &DistillLease::of(&dead, "killed-worker"),
                    jobs::DistillFinish::Done,
                    None,
                    0.0,
                    60.0,
                ))
                .expect("stale finish");
            assert!(!stale, "a stale generation settles nothing");
            let settled = handle
                .rt
                .block_on(jobs::finish_distill(
                    &handle.private,
                    &DistillLease::of(&survivor, "survivor"),
                    jobs::DistillFinish::Done,
                    None,
                    0.0,
                    60.0,
                ))
                .expect("survivor finish");
            assert!(settled);
            assert_eq!(handle.job(job).0, "DONE");
            handle.assert_i_slot();
        },
    );
}

/// The owner arm 0164 added to `jobs_tenant_isolation` is reachable ONLY from inside the
/// SECURITY DEFINER doors: a `role_private_worker` session still sees exactly its own tenant.
#[test]
fn worker_session_still_sees_only_its_own_tenants_jobs() {
    run(
        "worker_session_still_sees_only_its_own_tenants_jobs",
        |mut handle| {
            accept_evidence(&mut handle, 0);
            accept_evidence(&mut handle, 1);
            let (a, b) = (handle.tenants[0].tenant_id, handle.tenants[1].tenant_id);
            let mut worker =
            // dep: PostgreSQL(role_private_worker) — role-scoped pool call
            Client::connect(&dsn_as_role(&handle.dsn, "role_private_worker"), NoTls)
                .expect("connect as role_private_worker");
            let mut txn = worker.transaction().expect("begin");
            txn.batch_execute(&format!("SET LOCAL humaux.tenant_id = '{a}'"))
                .expect("install tenant A context");
            let count = |txn: &mut postgres::Transaction<'_>, t: Uuid| -> i64 {
                txn.query_one(
                "SELECT count(*) FROM ops.jobs WHERE tenant_id = $1 AND job_type = 'DERIVED_DISTILL'",
                &[&t],
            )
            .expect("jobs")
            .get(0)
            };
            assert_eq!(count(&mut txn, a), 1);
            assert_eq!(
                count(&mut txn, b),
                0,
                "the owner arm must not widen a worker session"
            );
            txn.rollback().expect("rollback probe txn");
        },
    );
}

/// `--distill-once` with no input: nothing claimed, returns promptly rather than polling.
#[test]
fn dispatch_pass_with_no_pending_work_claims_nothing() {
    run(
        "dispatch_pass_with_no_pending_work_claims_nothing",
        |handle| {
            let started = std::time::Instant::now();
            let report = handle.pass(&never_called(), &dispatch_config("idle-worker", 60.0));
            assert_eq!(report.claimed, 0, "{report:?}");
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "an empty pass returns promptly"
            );
        },
    );
}

/// Every free provider slot bound to a job id no `ops.jobs` row carries (the T9 orphan shape,
/// `bound_until` in the future so no sweep frees it); released on drop.
struct SlotsHeld {
    admin: Client,
    fake_jobs: Vec<Uuid>,
}

impl SlotsHeld {
    fn bind_all_free(dsn: &str) -> Self {
        // dep: PostgreSQL(owner) — binds and releases the held provider slots
        let mut admin = Client::connect(dsn, NoTls).expect("slot-holder connection");
        let fake_jobs = admin
            .query(
                "UPDATE ops.provider_slots SET job_id = gen_random_uuid(), claim_generation = 1, \
                   bound_until = clock_timestamp() + interval '5 minutes' \
                 WHERE job_id IS NULL RETURNING job_id",
                &[],
            )
            .expect("bind free slots")
            .iter()
            .map(|r| r.get(0))
            .collect();
        Self { admin, fake_jobs }
    }
}

impl Drop for SlotsHeld {
    fn drop(&mut self) {
        if let Err(e) = self.admin.execute(
            "UPDATE ops.provider_slots SET job_id = NULL, claim_generation = NULL, bound_until = NULL \
             WHERE job_id = ANY($1)",
            &[&self.fake_jobs],
        ) {
            eprintln!("derived_dispatch_e2e: releasing held slots failed: {e}");
        }
    }
}

/// ADR-0058 R5: `--distill-once` names why it stopped. With a free slot and nothing READY it is
/// `no_work`; with READY work left behind every bound slot it is `no_slot`, and the summary line
/// says so. Fault: `ops.distill_slots_all_bound()` answers false (the drain cannot tell the two
/// apart) → red, the blocked pass reports `no_work` while its job is still PENDING.
#[test]
fn a_drain_names_why_it_stopped() {
    run("a_drain_names_why_it_stopped", |mut handle| {
        let config = dispatch_config("c32-r5", 60.0);
        let idle = handle.pass(&never_called(), &config);
        assert_eq!(
            (idle.claimed, idle.stopped),
            (0, Some(distill::DrainStop::NoWork)),
            "{idle:?}"
        );
        assert!(
            idle.summary_line().ends_with(" stopped=no_work"),
            "{idle:?}"
        );

        let evidence = accept_evidence(&mut handle, 0);
        let blocked = {
            let held = SlotsHeld::bind_all_free(&handle.dsn);
            let report = handle.pass(&never_called(), &config);
            drop(held);
            report
        };
        assert_eq!(
            (blocked.claimed, blocked.stopped),
            (0, Some(distill::DrainStop::NoSlot)),
            "{blocked:?}"
        );
        assert!(
            blocked.summary_line().ends_with(" stopped=no_slot"),
            "{blocked:?}"
        );
        let job = handle.job_of(evidence);
        assert_eq!(
            handle.job(job).0,
            "PENDING",
            "the READY job was left behind"
        );
        handle.assert_i_slot();
    });
}

// ---------------------------------------------------------------------------
// Operator re-drive: `humaux-maintenance jobs requeue-dead` (ADR-0058 R4, migration 0197)
// ---------------------------------------------------------------------------

fn operator() -> AdminAction<'static> {
    AdminAction {
        actor: "c32-r4-test",
        reason: "card 32 R4 requeue-dead test",
        ticket: "T-32-R4",
        trace_id: "c32-r4-trace",
        step_up_auth_context: "test-mfa",
    }
}

impl Handle {
    /// The library call behind `jobs requeue-dead`, as the operator role (0197 grants EXECUTE to
    /// role_maintenance only).
    fn requeue(
        &self,
        tenant_id: Uuid,
        target: RequeueTarget<'_>,
    ) -> Result<RequeueReceipt, ProvisioningError> {
        let pool = self
            .rt
            // dep: PostgreSQL(role_maintenance) — the operator pool of `jobs requeue-dead`
            .block_on(MaintenanceDbPool::connect(&dsn_as_role(
                &self.dsn,
                "role_maintenance",
            )))
            .expect("maintenance pool");
        self.rt.block_on(provisioning::requeue_dead_distill(
            &pool,
            tenant_id,
            target,
            &operator(),
        ))
    }

    /// Settles `evidence_id`'s job DEAD the way a DEAD settle does (job DEAD with its class,
    /// outbox FAILED), without a provider round trip.
    fn kill(&mut self, evidence_id: Uuid) -> Uuid {
        let job = self.job_of(evidence_id);
        self.sql(
            "UPDATE ops.jobs SET status = 'DEAD', attempt = 3, last_error_class = 'RETRY_WAIT' \
             WHERE job_id = $1",
            job,
        );
        self.sql(
            "UPDATE ops.outbox SET status = 'FAILED', processed_at = now() WHERE evidence_id = $1",
            evidence_id,
        );
        job
    }
}

fn refusal(result: Result<RequeueReceipt, ProvisioningError>) -> String {
    match result {
        Err(ProvisioningError::Refused(reason)) => reason,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// R4-a: a DEAD job and its FAILED outbox row re-armed together (PENDING, attempt 0, class kept)
/// are distilled by the next pass. Fault: the definer leaves the outbox row FAILED ⇒ red (the row
/// stays FAILED; the job's claim would find no open row and settle DONE with no memory).
#[test]
fn a_requeued_dead_job_is_distilled_by_the_next_pass() {
    run(
        "a_requeued_dead_job_is_distilled_by_the_next_pass",
        |mut handle| {
            let tenant = handle.tenants[0].tenant_id;
            let evidence = accept_evidence(&mut handle, 0);
            let failing = std::sync::Arc::new(AtomicBool::new(true));
            let script_failing = failing.clone();
            let provider = StubProvider::new(move |_| {
                if script_failing.load(Ordering::SeqCst) {
                    (Duration::ZERO, Reply::RetryWait)
                } else {
                    (Duration::ZERO, Reply::Json(ONE_MEMORY_REPLY.to_string()))
                }
            });
            let mut config = dispatch_config("c32-r4a", 60.0);
            config.max_attempts = 1;
            let first = handle.pass(&provider, &config);
            assert_eq!(first.dead, 1, "{first:?}");
            let job = handle.job_of(evidence);
            assert_eq!(handle.outbox_status(evidence), "FAILED");

            let receipt = handle
                .requeue(tenant, RequeueTarget::Job(job))
                .expect("requeue-dead");
            assert_eq!(
                receipt.requeued,
                vec![RequeuedJob {
                    job_id: job,
                    evidence_id: evidence,
                    last_error_class: Some("RETRY_WAIT".to_string()),
                    attempt_spent: 1,
                }]
            );
            let (status, attempt, class, ..) = handle.job(job);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref()),
                ("PENDING", 0, Some("RETRY_WAIT")),
                "re-armed with a full budget, class kept"
            );
            assert_eq!(handle.outbox_status(evidence), "PENDING");

            failing.store(false, Ordering::SeqCst);
            let second = handle.pass(&provider, &config);
            assert_eq!(second.completed, 1, "{second:?}");
            assert_eq!(handle.job(job).0, "DONE");
            assert_eq!(handle.outbox_status(evidence), "DONE");
            assert_eq!(
                memory_count(&mut handle, 0),
                1,
                "the re-armed Evidence was distilled"
            );
            handle.assert_i_slot();
        },
    );
}

/// R4-b: a job that is not DEAD is refused and untouched. Fault: drop the definer's
/// `status <> 'DEAD'` refusal ⇒ the PENDING job is "re-armed" (red).
#[test]
fn requeue_dead_refuses_a_job_that_is_not_dead() {
    run(
        "requeue_dead_refuses_a_job_that_is_not_dead",
        |mut handle| {
            let tenant = handle.tenants[0].tenant_id;
            let evidence = accept_evidence(&mut handle, 0);
            let job = handle.job_of(evidence);
            let reason = refusal(handle.requeue(tenant, RequeueTarget::Job(job)));
            assert_eq!(reason, "job_not_dead");
            assert_eq!(handle.job(job).0, "PENDING");
            assert_eq!(handle.outbox_status(evidence), "PENDING");
        },
    );
}

/// R4-c: the door is tenant-scoped — another tenant's DEAD job is not found. Fault: drop the
/// definer's `j.tenant_id = p_tenant_id` job filter ⇒ the job is selected (and refused with
/// another reason, or re-armed) — red on the exact reason.
#[test]
fn requeue_dead_refuses_another_tenants_job() {
    run("requeue_dead_refuses_another_tenants_job", |mut handle| {
        let tenant_a = handle.tenants[0].tenant_id;
        let evidence_b = accept_evidence(&mut handle, 1);
        let job_b = handle.kill(evidence_b);
        let reason = refusal(handle.requeue(tenant_a, RequeueTarget::Job(job_b)));
        assert_eq!(reason, "job_not_found");
        assert_eq!(handle.job(job_b).0, "DEAD");
        assert_eq!(handle.outbox_status(evidence_b), "FAILED");
    });
}

/// R4-d: a DEAD job whose Evidence is gone (no outbox row left) is refused — re-armed it could
/// only die again. Fault: drop the definer's `v_outbox IS NULL` refusal ⇒ re-armed (red).
#[test]
fn requeue_dead_refuses_a_job_whose_evidence_is_gone() {
    run(
        "requeue_dead_refuses_a_job_whose_evidence_is_gone",
        |mut handle| {
            let tenant = handle.tenants[0].tenant_id;
            let evidence = accept_evidence(&mut handle, 0);
            let job = handle.kill(evidence);
            handle.sql("DELETE FROM ops.outbox WHERE evidence_id = $1", evidence);
            let reason = refusal(handle.requeue(tenant, RequeueTarget::Job(job)));
            assert_eq!(reason, "evidence_gone");
            assert_eq!(handle.job(job).0, "DEAD");
        },
    );
}

/// R4-e (ADR-0058 ruling 2026-10-02 20:30, migration 0200): class mode re-arms what it can and
/// REPORTS every matching DEAD job it skipped — Evidence gone, outbox already DONE — with the
/// reason, in the receipt and the audit row; a class whose every match is skipped answers
/// `nothing_requeued`, not `no_dead_job`. Fault: the definer's skip branch `CONTINUE`s without
/// `RETURN NEXT` (0198's body) ⇒ red.
#[test]
fn requeue_dead_by_class_reports_the_dead_jobs_it_skipped() {
    run(
        "requeue_dead_by_class_reports_the_dead_jobs_it_skipped",
        |mut handle| {
            let tenant = handle.tenants[0].tenant_id;
            let live = accept_evidence(&mut handle, 0);
            let gone = accept_evidence(&mut handle, 0);
            let settled = accept_evidence(&mut handle, 0);
            let (j_live, j_gone, j_settled) =
                (handle.kill(live), handle.kill(gone), handle.kill(settled));
            handle.sql("DELETE FROM ops.outbox WHERE evidence_id = $1", gone);
            handle.sql(
                "UPDATE ops.outbox SET status = 'DONE' WHERE evidence_id = $1",
                settled,
            );
            let skip = |job_id, evidence_id, reason| SkippedJob {
                job_id,
                evidence_id: Some(evidence_id),
                last_error_class: Some("RETRY_WAIT".to_string()),
                attempt_spent: 3,
                reason,
            };
            let expected_skips = vec![
                skip(j_gone, gone, RequeueSkipReason::EvidenceGone),
                skip(j_settled, settled, RequeueSkipReason::OutboxSettled),
            ];

            let receipt = handle
                .requeue(tenant, RequeueTarget::ErrorClass("RETRY_WAIT"))
                .expect("requeue-dead");
            println!("R4-e receipt {receipt:?}");
            assert_eq!(receipt.skipped, expected_skips);
            assert_eq!(
                (
                    receipt.outcome,
                    receipt.requeued.len(),
                    receipt.requeued[0].job_id
                ),
                ("requeued", 1, j_live)
            );
            let audited: serde_json::Value = handle
                .admin
                .query_one(
                    "SELECT metadata FROM control.audit_events WHERE audit_event_id = $1",
                    &[&receipt.audit_event_id],
                )
                .expect("audit row")
                .get(0);
            assert_eq!(
                audited["skipped"],
                serde_json::json!([
                    {"job_id": j_gone, "evidence_id": gone, "last_error_class": "RETRY_WAIT",
                     "attempt_spent": 3, "reason": "evidence_gone"},
                    {"job_id": j_settled, "evidence_id": settled, "last_error_class": "RETRY_WAIT",
                     "attempt_spent": 3, "reason": "outbox_settled"},
                ]),
                "{audited}"
            );
            assert_eq!(
                (handle.job(j_gone).0, handle.job(j_settled).0),
                ("DEAD".to_string(), "DEAD".to_string())
            );

            let only_skips = handle
                .requeue(tenant, RequeueTarget::ErrorClass("RETRY_WAIT"))
                .expect("every match skipped is a receipt, not a refusal");
            assert_eq!(
                (
                    only_skips.outcome,
                    only_skips.requeued.len(),
                    only_skips.skipped
                ),
                ("nothing_requeued", 0, expected_skips)
            );
            handle.sql(
                "UPDATE ops.jobs SET status = 'DONE' WHERE job_id = $1",
                j_live,
            );
        },
    );
}

// ---------------------------------------------------------------------------
// The BINARY: graceful shutdown (card 15 / ADR-0037) and two resident workers (E10)
// ---------------------------------------------------------------------------

/// macOS XProtect assesses a freshly linked binary on its first exec; pay it once.
fn warm_binary() {
    // dep: subprocess(humaux-private-worker) — spawns the private-worker binary under test
    let _ = std::process::Command::new(env!("CARGO_BIN_EXE_humaux-private-worker"))
        .arg("--warm-up-not-a-mode")
        .output();
}

/// A non-forbidden IP LITERAL endpoint (`ssrf::validate_custom_endpoint` accepts it without a DNS
/// round trip; F25: no loopback stub is reachable from the subprocess).
const LITERAL_CHAT_URL: &str = "https://192.88.99.1/v1/chat/completions";

/// The `--distill-serve` environment: `bootstrap()` builds the real BYOK provider before the loop
/// starts, so every one of its keys has to be present.
fn distill_serve_command(dsn: &str) -> std::process::Command {
    // dep: subprocess(humaux-private-worker) — spawns the private-worker binary under test
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_humaux-private-worker"));
    cmd.env("PRIVATE_WORKER_PG_DSN", dsn)
        .env("HUMAUX_PRIVATE_WORKER_CHAT_URL", LITERAL_CHAT_URL)
        .env("HUMAUX_PRIVATE_WORKER_PROVIDER_ID", PROVIDER_ID)
        .env("HUMAUX_PRIVATE_WORKER_MODEL_ID", MODEL_ID)
        .env("HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS", "5")
        // ADR-0059 D-I: a generated throwaway key under every reference this file seeded; the
        // literal chat URL is never reached with it.
        .env_remove("HUMAUX_PRIVATE_WORKER_KEY_ENV")
        .env(
            "HUMAUX_PRIVATE_WORKER_CREDENTIALS",
            credentials_spec("HUMAUX_CARD15_TEST_SECRET"),
        )
        .env(
            "HUMAUX_CARD15_TEST_SECRET",
            Uuid::new_v4().simple().to_string(),
        )
        .env(
            "HUMAUX_PRIVATE_WORKER_EGRESS_PROCESSOR_ID",
            EGRESS_PROCESSOR_ID.to_string(),
        )
        .env("HUMAUX_PRIVATE_WORKER_REGION", REGION)
        .env("HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS", "30")
        .env("HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS", "120")
        .env("HUMAUX_PRIVATE_WORKER_DISTILL_IN_FLIGHT", "4")
        .env("HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS", "250")
        .env("HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS", "600")
        .env("HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS", "5")
        .env("HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS", "1")
        .env("HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS", "60")
        .env("HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_MAX_CALLS", "10000")
        .env(
            "HUMAUX_PRIVATE_WORKER_CAPABILITIES",
            live_minimax::REHEARSAL_CAPABILITIES
                .map(ReasoningCapability::as_str)
                .join(","),
        );
    cmd
}

fn wait_exit(
    child: &mut std::process::Child,
    within: Duration,
    what: &str,
) -> std::process::ExitStatus {
    let deadline = std::time::Instant::now() + within;
    loop {
        match child.try_wait().expect("poll the worker") {
            Some(status) => return status,
            None if std::time::Instant::now() >= deadline => {
                let _ = child.kill();
                panic!("{what} did not exit within {within:?}");
            }
            None => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

fn signal(pid: u32, sig: &str) {
    // dep: subprocess(kill) — spawns external process
    let status = std::process::Command::new("kill")
        .arg(format!("-{sig}"))
        .arg(pid.to_string())
        .status()
        .expect("send a signal");
    assert!(status.success(), "kill -{sig} {pid} failed");
}

/// ADR-0037 D3: SIGTERM to `--distill-serve` mid-serve — every seat finishes the job it holds and
/// the process exits zero; no job is left `PROCESSING` with a live lease, no outbox row leased.
/// 注错: make a seat stop mid-job on shutdown (drop its work future) ⇒ a claimed job is left
/// `PROCESSING` with a live lease.
#[test]
fn sigterm_mid_serve_drains_and_leaves_no_job_processing_with_a_live_lease() {
    distill_serve_drains_on(
        "TERM",
        "sigterm_mid_serve_drains_and_leaves_no_job_processing_with_a_live_lease",
    );
}

/// The Ctrl-C twin: SIGINT is latched before the first claim (`Shutdown::install`).
#[test]
fn sigint_mid_serve_drains_and_leaves_no_job_processing_with_a_live_lease() {
    distill_serve_drains_on(
        "INT",
        "sigint_mid_serve_drains_and_leaves_no_job_processing_with_a_live_lease",
    );
}

fn distill_serve_drains_on(sig: &str, test_name: &'static str) {
    run(test_name, |mut handle| {
        warm_binary();
        // Tenant C: no admitted route — its jobs are claimed and settled NOT_READY without an
        // inference call, which keeps this test hermetic.
        for _ in 0..3 {
            accept_evidence(&mut handle, 2);
        }
        let tenant_id = handle.tenants[2].tenant_id;
        let dsn = dsn_as_role(&handle.dsn, "role_private_worker");
        let mut child = distill_serve_command(&dsn)
            .arg("--distill-serve")
            .spawn()
            .expect("spawn humaux-private-worker --distill-serve");
        let deadline = std::time::Instant::now() + Duration::from_secs(90);
        loop {
            let touched: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.jobs \
                     WHERE tenant_id = $1 AND (not_ready_since IS NOT NULL OR status <> 'PENDING')",
                    &[&tenant_id],
                )
                .expect("read job progress")
                .get(0);
            if touched > 0 {
                break;
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("the distill loop never claimed a job — this test would assert nothing");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        signal(child.id(), sig);
        let status = wait_exit(&mut child, Duration::from_secs(90), "the distill worker");
        assert!(
            status.success(),
            "SIG{sig} must drain to exit zero, got {status:?}"
        );
        let stuck: i64 = handle
            .admin
            .query_one(
                "SELECT count(*) FROM ops.jobs WHERE tenant_id = $1 AND status = 'PROCESSING' \
                   AND lease_expires_at > clock_timestamp()",
                &[&tenant_id],
            )
            .expect("read leases")
            .get(0);
        assert_eq!(
            stuck, 0,
            "a drained worker left a job PROCESSING with a live lease"
        );
        let leased: i64 = handle
            .admin
            .query_one(
                "SELECT count(*) FROM ops.outbox WHERE tenant_id = $1 AND lease_expires_at > clock_timestamp()",
                &[&tenant_id],
            )
            .expect("read outbox leases")
            .get(0);
        assert_eq!(
            leased, 0,
            "a drained worker left a live ops.outbox lease behind"
        );
        handle.assert_i_slot();
    });
}

/// ADR-0037 D3, the RPC-listener half: `--serve-rpc` holds no lease, its drain is "stop
/// accepting and return zero". 注错: drop the `shutdown.recv()` arm from `serve_rpc`'s `select!`.
#[test]
fn sigterm_to_the_inference_rpc_listener_exits_zero() {
    run(
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
            signal(child.id(), "TERM");
            let status = wait_exit(&mut child, Duration::from_secs(60), "the RPC listener");
            let _ = std::fs::remove_file(&socket_path);
            assert!(
                status.success(),
                "a drained RPC listener must exit zero, got {status:?}"
            );
        },
    );
}

/// This process's uid, read from a file it just created (no `libc` for one call).
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

/// E10 (review finding 5, MiniMax-free M3) — two resident `--distill-serve` subprocesses over one
/// backlog whose provider endpoint never answers in time (the literal endpoint; HTTP 3 s, lease
/// 1 s, hard deadline 8 s, max 3 attempts). Once worker 1 holds a dispatched job it is stopped
/// (SIGSTOP) for 4 s: its leases expire while its calls are open. Sampled every 100 ms: bound
/// slots ≤ 4 and I-SLOT; at least one EXECUTION_UNCERTAIN is observed (else inconclusive); the
/// double-spend report has no overlap and no unattributed SUCCEEDED row; every job ends DEAD with
/// its outbox FAILED. Fault: T5 branch → READY (lease expiry frees the slot) ⇒ worker 2
/// re-dispatches a job while worker 1's stopped call is open ⇒ overlap.
#[test]
#[allow(clippy::too_many_lines)]
fn two_resident_workers_never_overlap_calls_for_one_evidence() {
    run(
        "two_resident_workers_never_overlap_calls_for_one_evidence",
        |mut handle| {
            warm_binary();
            let mut evidence = Vec::new();
            for n in 0..4 {
                let tenant = seed_tenant(
                    &mut handle.admin,
                    &format!("private derived_dispatch_e2e e10 tenant {n}"),
                    handle.user_id,
                    Some(EGRESS_PROCESSOR_ID),
                    LITERAL_CHAT_URL,
                )
                .expect("e10 tenant");
                handle.tenants.push(tenant);
                let idx = handle.tenants.len() - 1;
                for _ in 0..3 {
                    evidence.push(accept_evidence(&mut handle, idx));
                }
            }
            let dsn = dsn_as_role(&handle.dsn, "role_private_worker");
            let spawn = || {
                distill_serve_command(&dsn)
                    .env("HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS", "3")
                    .env("HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS", "1")
                    .env(
                        "HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS",
                        E10_HARD_DEADLINE_SECS.to_string(),
                    )
                    .env("HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS", "3")
                    .arg("--distill-serve")
                    .spawn()
                    .expect("spawn a resident worker")
            };
            let mut one = spawn();
            let tenants = handle.tenant_ids();
            let started = std::time::Instant::now();
            // Worker 1's owner: the first dispatched job's lease owner (worker 2 is not up yet).
            let owner_one: String = loop {
                let row = handle
                    .admin
                    .query_opt(
                        "SELECT lease_owner FROM ops.jobs WHERE tenant_id = ANY($1) \
                           AND dispatch_state = 'DISPATCH_INTENT' LIMIT 1",
                        &[&tenants],
                    )
                    .expect("probe");
                if let Some(row) = row {
                    break row.get(0);
                }
                assert!(
                    started.elapsed() < Duration::from_secs(60),
                    "worker 1 never dispatched"
                );
                std::thread::sleep(Duration::from_millis(20));
            };
            let mut two = spawn();
            signal(one.id(), "STOP");
            let stopped_at = std::time::Instant::now();
            let mut resumed = false;
            let (mut max_bound, mut uncertain_seen, mut samples) = (0_i64, false, 0_u32);
            let done = loop {
                if !resumed && stopped_at.elapsed() >= Duration::from_secs(4) {
                    signal(one.id(), "CONT");
                    resumed = true;
                }
                let bound: i64 = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FROM ops.provider_slots WHERE job_id IS NOT NULL",
                        &[],
                    )
                    .unwrap()
                    .get(0);
                max_bound = max_bound.max(bound);
                assert!(bound <= 4, "bound slots {bound} > 4");
                assert_eq!(
                    i_slot_violations(&mut handle.admin),
                    0,
                    "I-SLOT broke in a sample"
                );
                let row = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FILTER (WHERE dispatch_state = 'EXECUTION_UNCERTAIN'), \
                                count(*) FILTER (WHERE status IN ('DEAD', 'DONE')) \
                         FROM ops.jobs WHERE tenant_id = ANY($1)",
                        &[&tenants],
                    )
                    .unwrap();
                uncertain_seen |= row.get::<_, i64>(0) > 0;
                samples += 1;
                if resumed && row.get::<_, i64>(1) == evidence.len() as i64 {
                    break true;
                }
                if started.elapsed() > Duration::from_secs(240) {
                    break false;
                }
                std::thread::sleep(Duration::from_millis(100));
            };
            if !resumed {
                signal(one.id(), "CONT");
            }
            for child in [&mut one, &mut two] {
                signal(child.id(), "TERM");
                let status = wait_exit(child, Duration::from_secs(60), "a resident worker");
                assert!(status.success(), "{status:?}");
            }
            let report =
                double_spend::report(&mut handle.admin, &tenants, 3, E10_HARD_DEADLINE_SECS);
            let classes: Vec<(String, Option<String>, i64)> = handle
                .admin
                .query(
                    "SELECT status, last_error_class, count(*) FROM ops.jobs WHERE tenant_id = ANY($1) GROUP BY 1, 2 ORDER BY 1, 2",
                    &[&tenants],
                )
                .unwrap()
                .into_iter()
                .map(|r| (r.get(0), r.get(1), r.get(2)))
                .collect();
            println!(
                "E10 owner_one={owner_one} samples={samples} max_bound={max_bound} uncertain_seen={uncertain_seen} wall={:?} jobs={classes:?} {}",
                started.elapsed(),
                report.line()
            );
            assert!(done, "every job must settle within 240 s: {classes:?}");
            assert_eq!(report.overlaps, 0, "{}", report.line());
            assert_eq!(report.unattributed_succeeded, 0, "{}", report.line());
            assert!(
                uncertain_seen,
                "inconclusive: no EXECUTION_UNCERTAIN observed while worker 1 was stopped"
            );
            assert!(max_bound <= 4);
            for row in &evidence {
                assert_eq!(
                    handle.outbox_status(*row),
                    "FAILED",
                    "DEAD flips its outbox row"
                );
            }
            let not_dead: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.jobs WHERE tenant_id = ANY($1) AND status <> 'DEAD'",
                    &[&tenants],
                )
                .unwrap()
                .get(0);
            assert_eq!(not_dead, 0, "{classes:?}");
            handle.assert_i_slot();
        },
    );
}

// ---------------------------------------------------------------------------
// ADR-0058 §7: the card's live acceptance gate (MiniMax), M1..M8.
// ---------------------------------------------------------------------------

/// The live transport timeout; `live_config` sizes `ops.begin_call`'s window from it (D-K).
const LIVE_HTTP_SECS: u64 = 60;
/// Card gate: B's single row completes within this while A has 200 queued (card 32 acceptance).
const FAIRNESS_BOUND_SECS: f64 = 60.0;
/// A model id MiniMax does not serve: every call to it is a real provider refusal (M6).
const POISON_MODEL_ID: &str = "c32-no-such-model";
/// ADR-0048 per-claim budget: first call + malformed re-ask + empty retry (M3's per-gen bound).
const PER_GEN_CALL_BUDGET: i64 = 3;
/// E10's `HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS` (also the double-spend window, R6).
const E10_HARD_DEADLINE_SECS: f64 = 8.0;

/// ADR-0058 D-K shape for the live provider: `hard_deadline = 2 × (http + lease)`.
fn live_config(owner: &str, lease_seconds: f64, in_flight: u32) -> DistillDispatchConfig {
    let http = LIVE_HTTP_SECS as f64;
    DistillDispatchConfig {
        lease_owner: owner.to_string(),
        lease_seconds,
        in_flight,
        hard_deadline_seconds: 2.0 * (http + lease_seconds),
        http_timeout_seconds: http,
        not_ready_park_seconds: 600.0,
        max_attempts: 5,
        budget: TEST_BUDGET,
        credential_refs: mapped_credentials(),
    }
}

/// One distinct, uuid-free factual note per index (gitleaks' generic-key rule flags a uuid inside
/// a distilled memory, card 27 run 2026-09-29 #3).
fn live_note(set: &str, n: usize) -> String {
    const NOUNS: [&str; 10] = [
        "billing",
        "search",
        "ledger",
        "inventory",
        "payroll",
        "gateway",
        "audit",
        "catalog",
        "shipping",
        "pricing",
    ];
    const DAYS: [&str; 5] = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"];
    format!(
        "Card32 {set} note {n}: the {} service of team {} listens on port {} and ships every {}.",
        NOUNS[n % NOUNS.len()],
        n / NOUNS.len() + 1,
        7000 + n,
        DAYS[n % DAYS.len()]
    )
}

/// Outbox rows of `tenants` still open (`PENDING` / `PROCESSING`).
fn open_outbox(admin: &mut Client, tenants: &[Uuid]) -> i64 {
    admin
        .query_one(
            "SELECT count(*) FROM ops.outbox WHERE tenant_id = ANY($1) \
               AND event_type = 'EVIDENCE_ACCEPTED' AND status IN ('PENDING', 'PROCESSING')",
            &[&tenants],
        )
        .expect("open outbox")
        .get(0)
}

/// `(DONE, FAILED)` outbox rows of `tenants`.
fn outbox_done_failed(admin: &mut Client, tenants: &[Uuid]) -> (i64, i64) {
    let row = admin
        .query_one(
            "SELECT count(*) FILTER (WHERE status = 'DONE'), count(*) FILTER (WHERE status = 'FAILED') \
             FROM ops.outbox WHERE tenant_id = ANY($1) AND event_type = 'EVIDENCE_ACCEPTED'",
            &[&tenants],
        )
        .expect("outbox totals");
    (row.get(0), row.get(1))
}

/// Bound slots right now (M4 a).
fn bound_slots(admin: &mut Client) -> i64 {
    admin
        .query_one(
            "SELECT count(*) FROM ops.provider_slots WHERE job_id IS NOT NULL",
            &[],
        )
        .expect("bound slots")
        .get(0)
}

/// M4 (b): the most `distill_calls`-attributed ledger intervals `[called_at, called_at +
/// latency_ms]` of `tenants` open at one instant. Rows with no latency (RESERVED: killed or
/// unknown calls) are excluded and counted separately by the caller's double-spend line.
fn max_ledger_overlap(admin: &mut Client, tenants: &[Uuid]) -> i64 {
    admin
        .query_one(
            "WITH iv AS (SELECT l.called_at AS s, \
                    l.called_at + make_interval(secs => l.latency_ms / 1000.0) AS e \
                  FROM ops.distill_calls c JOIN ops.model_call_ledger l USING (model_call_id) \
                  WHERE c.tenant_id = ANY($1) AND l.latency_ms IS NOT NULL) \
             SELECT coalesce(max((SELECT count(*) FROM iv b WHERE b.s <= a.s AND b.e > a.s)), 0) \
             FROM iv a",
            &[&tenants],
        )
        .expect("ledger overlap")
        .get(0)
}

/// Evidence/s of one tenant set: n / (last outbox settle − first admitted call).
fn evidence_rate(admin: &mut Client, tenants: &[Uuid]) -> (i64, f64) {
    let row = admin
        .query_one(
            "SELECT (SELECT count(*) FROM ops.outbox WHERE tenant_id = ANY($1) AND status = 'DONE'), \
                    extract(epoch FROM (SELECT max(processed_at) FROM ops.outbox WHERE tenant_id = ANY($1)) \
                      - (SELECT min(begun_at) FROM ops.distill_calls WHERE tenant_id = ANY($1)))::float8",
            &[&tenants],
        )
        .expect("rate");
    let (done, secs): (i64, f64) = (row.get(0), row.get(1));
    (done, done as f64 / secs)
}

/// What [`serve_until`] observed while the dispatcher(s) ran.
struct Served {
    report: DistillDispatchReport,
    met: bool,
    wall: Duration,
    max_bound: i64,
    i_slot_breaks: u32,
    samples: u32,
}

/// One in-process `dispatch_serve` until `done` holds on an owner connection or `limit` passes;
/// a sampler records the bound-slot maximum and I-SLOT on every 100 ms tick (M4 a). One
/// dispatcher only: a process serves one provider (card 33b), so two dispatchers with different
/// providers over one queue race for each other's jobs (M6, `distill_poison_live`).
fn serve_until(
    handle: &Handle,
    provider: &dyn UserReasoningProvider,
    config: &DistillDispatchConfig,
    limit: Duration,
    done: impl Fn(&mut Client) -> bool + Send,
) -> Served {
    let stop = AtomicBool::new(false);
    let poll = Duration::from_millis(500);
    let started = std::time::Instant::now();
    let (dsn, stop_flag) = (handle.dsn.clone(), &stop);
    std::thread::scope(|scope| {
        let sampler = scope.spawn(move || {
            // dep: PostgreSQL(owner) — sampler connection
            let mut admin = Client::connect(&dsn, NoTls).expect("sampler connection");
            let (mut max_bound, mut breaks, mut samples) = (0_i64, 0_u32, 0_u32);
            let met = loop {
                max_bound = max_bound.max(bound_slots(&mut admin));
                breaks += u32::from(i_slot_violations(&mut admin) != 0);
                samples += 1;
                if done(&mut admin) {
                    break true;
                }
                if started.elapsed() > limit {
                    break false;
                }
                std::thread::sleep(Duration::from_millis(100));
            };
            stop_flag.store(true, Ordering::SeqCst);
            (met, max_bound, breaks, samples)
        });
        let report = handle.rt.block_on(distill::dispatch_serve(
            &handle.private,
            provider,
            reasoner_config(),
            config,
            poll,
            &stop,
        ));
        let (met, max_bound, i_slot_breaks, samples) = sampler.join().expect("sampler");
        Served {
            report: report.expect("dispatcher"),
            met,
            wall: started.elapsed(),
            max_bound,
            i_slot_breaks,
            samples,
        }
    })
}

/// A resident `--distill-serve` subprocess on the live MiniMax lane (the same keys the rehearsal
/// sets; the key reaches the child's environment only).
fn live_serve_command(dsn: &str, key: &str) -> std::process::Command {
    let mut cmd = distill_serve_command(dsn);
    cmd.env("HUMAUX_PRIVATE_WORKER_CHAT_URL", ENDPOINT_REF)
        .env("HUMAUX_PRIVATE_WORKER_DNS_PINS", live_minimax::dns_pins())
        .env(
            "HUMAUX_PRIVATE_WORKER_CREDENTIALS",
            credentials_spec("MINIMAX_API_KEY"),
        )
        .env("MINIMAX_API_KEY", key)
        .env("HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS", "604800")
        .env(
            "HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS",
            LIVE_HTTP_SECS.to_string(),
        )
        .env("HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS", "30")
        .env(
            "HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS",
            (2 * (LIVE_HTTP_SECS + 30)).to_string(),
        );
    cmd
}

/// Leftover open jobs of a measurement that stopped early are parked DEAD so the next
/// measurement's dispatcher never serves them (the fixture deletes them at drop).
fn retire_open_jobs(admin: &mut Client, tenants: &[Uuid]) -> u64 {
    admin
        .execute(
            "UPDATE ops.jobs SET status = 'DEAD', last_error_class = 'C32_LIVE_GATE_STOPPED', \
               next_retry_at = NULL, lease_owner = NULL, lease_expires_at = NULL \
             WHERE tenant_id = ANY($1) AND job_type = 'DERIVED_DISTILL' \
               AND status IN ('PENDING', 'RETRY_WAIT', 'WAITING_KEY')",
            &[&tenants],
        )
        .expect("retire leftover jobs")
}

/// ADR-0058 §7 M6 — a poison Evidence (its domain bound to a model MiniMax does not serve) dies
/// after `max_attempts` real provider refusals while the tenant's good domain is served. Phased so
/// no two dispatchers ever compete for one job (a process serves one provider until card 33b):
/// phase A runs only the poison-lane dispatcher until the poison job is DEAD — the good jobs it
/// claims are released NOT_READY and keep attempt 0; phase B makes the good jobs due and runs only
/// the good-lane dispatcher — every good job DONE at attempt 1, the DEAD job never claimed again.
#[test]
#[ignore = "lane(a:shared_db) live MiniMax acceptance gate (card 32 distill_poison_live): run with --include-ignored and HUMAUX_REQUIRE_MINIMAX=1"]
#[allow(clippy::too_many_lines)]
fn distill_poison_live() {
    let Some(key) = live_minimax::load_minimax_key() else {
        humaux_testkit::skip_or_fail(
            "distill_poison_live",
            "missing object: MINIMAX_API_KEY (env and /Volumes/data/viral-skill-eval/.env both empty)",
            humaux_testkit::ExternalDep::MiniMax,
        );
        return;
    };
    run("distill_poison_live", |mut handle| {
        // SAFETY: set while SERIAL_GUARD is held (see `distill_fairness_live`).
        unsafe {
            std::env::set_var("HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS", "604800");
        }
        let http = Duration::from_secs(LIVE_HTTP_SECS);
        let m6 = handle
            .add_tenant(
                "private derived_dispatch_e2e live m6 poison",
                Some(EGRESS_PROCESSOR_ID),
            )
            .expect("live tenant");
        let m6_tenant = handle.tenants[m6].tenant_id;
        let poison_domain: Uuid = handle
            .admin
            .query_one(
                "INSERT INTO control.private_reasoning_domains (tenant_id, name, owner_user_id, status) \
                 VALUES ($1, 'private derived_dispatch_e2e live m6 poison domain', $2, 'ACTIVE') \
                 RETURNING reasoning_domain_id",
                &[&m6_tenant, &handle.user_id],
            )
            .expect("poison domain")
            .get(0);
        // The poison domain's own lane names a model MiniMax does not serve: every admitted call
        // is a real provider refusal.
        seed_route_binding(
            &mut handle.admin,
            m6_tenant,
            handle.user_id,
            poison_domain,
            "private derived_dispatch_e2e live m6 poison lane",
            EGRESS_PROCESSOR_ID,
            ENDPOINT_REF,
            POISON_MODEL_ID,
        )
        .expect("poison lane");
        let good_rows: Vec<Uuid> = (0..2)
            .map(|n| accept_evidence_marked(&mut handle, m6, &live_note("m6good", n)))
            .collect();
        let good_jobs: Vec<Uuid> = good_rows.iter().map(|r| handle.job_of(*r)).collect();
        let poison_row = accept_evidence_in(
            &mut handle,
            m6_tenant,
            poison_domain,
            &live_note("m6poison", 0),
        );
        let poison_job = handle.job_of(poison_row);
        let mut poison_config = live_config("c32-m6-poison-lane", 5.0, 3);
        poison_config.max_attempts = 3;
        let mut good_config = live_config("c32-m6-good-lane", 5.0, 3);
        good_config.max_attempts = 3;

        // Phase A bound: every attempt's HTTP window + one lease of claim/settle work, plus the
        // backoffs between attempts, plus one more lease of poll slack.
        let backoffs: f64 = (1..poison_config.max_attempts)
            .map(|a| jobs::retry_backoff_seconds(poison_config.lease_seconds, a))
            .sum();
        let phase_a_bound = backoffs
            + f64::from(poison_config.max_attempts)
                * (poison_config.http_timeout_seconds + poison_config.lease_seconds)
            + poison_config.lease_seconds;
        let poison_provider = live_minimax::live_provider(key.clone(), POISON_MODEL_ID, http);
        let phase_a = serve_until(
            &handle,
            &poison_provider,
            &poison_config,
            Duration::from_secs_f64(phase_a_bound),
            |admin| {
                admin
                    .query_one(
                        "SELECT status FROM ops.jobs WHERE job_id = $1",
                        &[&poison_job],
                    )
                    .expect("poison status")
                    .get::<_, String>(0)
                    == "DEAD"
            },
        );
        let good_attempts: Vec<i32> = good_jobs.iter().map(|j| handle.job(*j).1).collect();
        let (_, _, _, poison_gen, _) = handle.job(poison_job);

        // Phase B bound: the good jobs are due now; one claim's full call budget per job (all in
        // flight at once) + one lease of poll slack.
        for job in &good_jobs {
            handle.ready_now(*job);
        }
        let phase_b_bound = PER_GEN_CALL_BUDGET as f64 * good_config.http_timeout_seconds
            + good_config.lease_seconds;
        let live = live_minimax::live_provider(key.clone(), MODEL_ID, http);
        let phase_b = serve_until(
            &handle,
            &live,
            &good_config,
            Duration::from_secs_f64(phase_b_bound),
            |admin| open_outbox(admin, &[m6_tenant]) == 0,
        );

        let (status, attempt, class, gen_after, _) = handle.job(poison_job);
        let row = handle
            .admin
            .query_one(
                "SELECT count(*), count(*) FILTER (WHERE l.status = 'FAILED') \
                 FROM ops.model_call_ledger l JOIN ops.distill_calls c USING (model_call_id) \
                 WHERE c.job_id = $1",
                &[&poison_job],
            )
            .expect("poison ledger");
        let (poison_ledger, poison_refused): (i64, i64) = (row.get(0), row.get(1));
        let poison_calls = handle.calls_of(poison_job);
        let good_final: Vec<(String, i32)> = good_jobs
            .iter()
            .map(|j| {
                let (s, a, ..) = handle.job(*j);
                (s, a)
            })
            .collect();
        let good_done = good_rows
            .iter()
            .filter(|r| handle.outbox_status(**r) == "DONE")
            .count();
        println!(
            "M6 poison status={status} attempt={attempt} class={class:?} calls={poison_calls} ledger_rows={poison_ledger} ledger_failed={poison_refused} outbox={} good_done={good_done}/{} good_attempts_after_poison_phase={good_attempts:?} good_final={good_final:?} poison_gen_after_a={poison_gen} poison_gen_after_b={gen_after} wall_s={:.1} phase_a_s={:.1}/{phase_a_bound:.0} phase_b_s={:.1}/{phase_b_bound:.0} good_lane={} poison_lane={}",
            handle.outbox_status(poison_row),
            good_rows.len(),
            (phase_a.wall + phase_b.wall).as_secs_f64(),
            phase_a.wall.as_secs_f64(),
            phase_b.wall.as_secs_f64(),
            phase_b.report.summary_line(),
            phase_a.report.summary_line(),
        );
        assert!(phase_a.met, "M6 phase A: the poison job never reached DEAD");
        assert_eq!(
            (status.as_str(), attempt),
            ("DEAD", poison_config.max_attempts)
        );
        assert_eq!(class.as_deref(), Some("PROVIDER_PERMANENT"));
        let max_calls = i64::from(poison_config.max_attempts);
        assert_eq!(
            (poison_calls, poison_ledger, poison_refused),
            (max_calls, max_calls, max_calls),
            "one FAILED ledger row per real call"
        );
        assert_eq!(handle.outbox_status(poison_row), "FAILED");
        assert!(
            good_attempts.iter().all(|a| *a == 0),
            "a poison lane never spends another job's budget"
        );
        assert!(phase_b.met, "M6 phase B: the good jobs did not settle");
        assert!(
            good_final.iter().all(|(s, a)| s == "DONE" && *a == 1),
            "every good job DONE at attempt 1"
        );
        assert_eq!(good_done, good_rows.len());
        assert_eq!(
            gen_after, poison_gen,
            "the DEAD poison job was claimed again"
        );
        assert_eq!(phase_a.i_slot_breaks + phase_b.i_slot_breaks, 0, "I-SLOT");
        handle.assert_i_slot();
    });
}

/// ADR-0058 §7 — the card's live acceptance gate on MiniMax (`HUMAUX_REQUIRE_MINIMAX=1`). Prints
/// one `M<n>` line per measurement and asserts the card's bounds: M1 Evidence/s at 1 vs 4
/// in-flight (n = 50 each); M2 tenant B's single row DONE within 60 s while A has 200 queued; M3
/// two resident subprocess workers over a 100-Evidence backlog — no duplicate, overlap, budget
/// overrun or unattributed SUCCEEDED call, one memory set per Evidence; M4 bound slots ≤ 4 in
/// every sample and ≤ 4 overlapping ledger intervals; M5 kill -9 mid-call → EXECUTION_UNCERTAIN
/// with the slot kept, then the classed T6 reconcile after `hard_deadline`, never a resend before
/// it; M7 inferred affect rows (origin DISTILL, confidence ≤ 5000); M8 one tenant with two
/// domains, FAILED = 0. Faults are carried by T2/T3/E1/E2/E3/E10/d5b; this test is the
/// measurement. M6 moved to [`distill_poison_live`]: here it ran a poison-lane and a good-lane
/// dispatcher concurrently over one queue, each can only release the other's jobs NOT_READY, so
/// who claims the poison job after a backoff is a race (main-line run: `M6 poison status=PENDING
/// attempt=2 ... good_lane: claimed=34 completed=2 not_ready=32`).
#[test]
#[ignore = "lane(a:shared_db) live MiniMax acceptance gate (card 32 distill_fairness_live): run with --include-ignored and HUMAUX_REQUIRE_MINIMAX=1"]
#[allow(clippy::too_many_lines)]
fn distill_fairness_live() {
    let Some(key) = live_minimax::load_minimax_key() else {
        humaux_testkit::skip_or_fail(
            "distill_fairness_live",
            "missing object: MINIMAX_API_KEY (env and /Volumes/data/viral-skill-eval/.env both empty)",
            humaux_testkit::ExternalDep::MiniMax,
        );
        return;
    };
    run("distill_fairness_live", |mut handle| {
        // ADR-0026 candidate TTL for the in-process dispatcher's rejection path (§78.1, no
        // literal in the worker).
        // SAFETY: set while SERIAL_GUARD is held; no other test thread of this binary reads the
        // environment until it acquires the guard.
        unsafe {
            std::env::set_var("HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS", "604800");
        }
        warm_binary();
        let http = Duration::from_secs(LIVE_HTTP_SECS);
        let live = live_minimax::live_provider(key.clone(), MODEL_ID, http);
        let new_tenant = |handle: &mut Handle, label: &str| {
            handle
                .add_tenant(
                    &format!("private derived_dispatch_e2e live {label}"),
                    Some(EGRESS_PROCESSOR_ID),
                )
                .expect("live tenant")
        };

        // ---- M1: Evidence/s, 1 vs 4 in-flight, n = 50 each ----
        let mut rates = Vec::new();
        for in_flight in [1_u32, 4] {
            let idx = new_tenant(&mut handle, &format!("m1 in_flight {in_flight}"));
            for n in 0..50 {
                accept_evidence_marked(&mut handle, idx, &live_note("m1", n));
            }
            let tenants = vec![handle.tenants[idx].tenant_id];
            let served = serve_until(
                &handle,
                &live,
                &live_config(&format!("c32-m1-{in_flight}"), 30.0, in_flight),
                Duration::from_secs(900),
                |admin| open_outbox(admin, &tenants) == 0,
            );
            let (done, rate) = evidence_rate(&mut handle.admin, &tenants);
            let failed = outbox_done_failed(&mut handle.admin, &tenants).1;
            println!(
                "M1 in_flight={in_flight} n=50 done={done} failed={failed} evidence_per_s={rate:.3} wall_s={:.1} max_bound={} report={}",
                served.wall.as_secs_f64(),
                served.max_bound,
                served.report.summary_line()
            );
            assert!(served.met, "M1 in_flight={in_flight}: backlog not drained");
            assert!(served.max_bound <= i64::from(in_flight));
            assert_eq!(served.i_slot_breaks, 0, "I-SLOT");
            rates.push(rate);
        }
        println!("M1 ratio in_flight4/in_flight1={:.2}", rates[1] / rates[0]);

        // ---- M2 + M4 (sampled): B's single row while A has 200 queued ----
        let a = new_tenant(&mut handle, "m2 tenant A");
        let b = new_tenant(&mut handle, "m2 tenant B");
        for n in 0..200 {
            accept_evidence_marked(&mut handle, a, &live_note("m2a", n));
        }
        let b_row = accept_evidence_marked(&mut handle, b, &live_note("m2b", 0));
        let (a_id, b_id) = (handle.tenants[a].tenant_id, handle.tenants[b].tenant_id);
        let a_depth_at_b = std::sync::Mutex::new(None::<i64>);
        let served = serve_until(
            &handle,
            &live,
            &live_config("c32-m2", 30.0, 4),
            Duration::from_secs(300),
            |admin| {
                let done = open_outbox(admin, &[b_id]) == 0;
                if done {
                    *a_depth_at_b.lock().expect("depth") = Some(open_outbox(admin, &[a_id]));
                }
                done
            },
        );
        let b_latency: f64 = handle
            .admin
            .query_one(
                "SELECT extract(epoch FROM processed_at - created_at)::float8 FROM ops.outbox WHERE evidence_id = $1",
                &[&b_row],
            )
            .expect("B latency")
            .get(0);
        let b_status = handle.outbox_status(b_row);
        let overlap = max_ledger_overlap(&mut handle.admin, &[a_id, b_id]);
        println!(
            "M2 tenantB_single_row_latency_s={b_latency:.1} bound_s={FAIRNESS_BOUND_SECS} b_status={b_status} a_queue_at_b_done={:?} wall_s={:.1}",
            a_depth_at_b.lock().expect("depth"),
            served.wall.as_secs_f64()
        );
        println!(
            "M4 m2 samples={} max_bound_slots={} i_slot_breaks={} max_ledger_overlap={overlap}",
            served.samples, served.max_bound, served.i_slot_breaks
        );
        assert!(served.met, "M2: B's row never completed");
        assert_eq!(b_status, "DONE");
        assert!(
            b_latency <= FAIRNESS_BOUND_SECS,
            "M2: B waited {b_latency:.1}s behind A's queue"
        );
        assert!(served.max_bound <= 4 && overlap <= 4);
        assert_eq!(served.i_slot_breaks, 0, "I-SLOT");
        retire_open_jobs(&mut handle.admin, &[a_id]);

        // ---- M3 + M4: two resident subprocess workers over one 100-Evidence backlog ----
        let m3: Vec<usize> = (0..2)
            .map(|t| new_tenant(&mut handle, &format!("m3 tenant {t}")))
            .collect();
        let mut m3_evidence = Vec::new();
        for (t, idx) in m3.iter().enumerate() {
            for n in 0..50 {
                m3_evidence.push(accept_evidence_marked(
                    &mut handle,
                    *idx,
                    &live_note(&format!("m3t{t}"), n),
                ));
            }
        }
        let m3_tenants: Vec<Uuid> = m3.iter().map(|i| handle.tenants[*i].tenant_id).collect();
        let worker_dsn = dsn_as_role(&handle.dsn, "role_private_worker");
        let mut workers: Vec<std::process::Child> = (0..2)
            .map(|_| {
                live_serve_command(&worker_dsn, &key)
                    .arg("--distill-serve")
                    .spawn()
                    .expect("spawn a live resident worker")
            })
            .collect();
        let started = std::time::Instant::now();
        let (mut max_bound, mut breaks, mut samples) = (0_i64, 0_u32, 0_u32);
        let drained = loop {
            max_bound = max_bound.max(bound_slots(&mut handle.admin));
            breaks += u32::from(i_slot_violations(&mut handle.admin) != 0);
            samples += 1;
            if open_outbox(&mut handle.admin, &m3_tenants) == 0 {
                break true;
            }
            if started.elapsed() > Duration::from_secs(900) {
                break false;
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        for child in &mut workers {
            signal(child.id(), "TERM");
            let status = wait_exit(child, Duration::from_secs(150), "a live resident worker");
            assert!(status.success(), "{status:?}");
        }
        let report = double_spend::report(
            &mut handle.admin,
            &m3_tenants,
            PER_GEN_CALL_BUDGET,
            (2 * (LIVE_HTTP_SECS + 30)) as f64,
        );
        let histogram: Vec<(i64, i64)> = handle
            .admin
            .query(
                "SELECT calls, count(*) FROM (SELECT j.job_id, count(c.model_call_id) AS calls \
                   FROM ops.jobs j LEFT JOIN ops.distill_calls c USING (job_id) \
                   WHERE j.tenant_id = ANY($1) AND j.job_type = 'DERIVED_DISTILL' GROUP BY 1) g \
                 GROUP BY 1 ORDER BY 1",
                &[&m3_tenants],
            )
            .expect("call histogram")
            .into_iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        let twice_processed: i64 = handle
            .admin
            .query_one(
                "SELECT count(*) FROM (SELECT evidence_id FROM private.processing_runs \
                   WHERE tenant_id = ANY($1) AND completed_at IS NOT NULL GROUP BY 1 HAVING count(*) > 1) g",
                &[&m3_tenants],
            )
            .expect("memory sets")
            .get(0);
        let (done, failed) = outbox_done_failed(&mut handle.admin, &m3_tenants);
        let overlap = max_ledger_overlap(&mut handle.admin, &m3_tenants);
        println!(
            "M3 workers=2 n={} done={done} failed={failed} wall_s={:.1} calls_per_evidence={histogram:?} evidence_with_two_memory_sets={twice_processed} {}",
            m3_evidence.len(),
            started.elapsed().as_secs_f64(),
            report.line()
        );
        println!(
            "M4 m3 samples={samples} max_bound_slots={max_bound} i_slot_breaks={breaks} max_ledger_overlap={overlap}"
        );
        assert!(drained, "M3: the backlog did not drain within 900 s");
        assert_eq!(
            (
                report.duplicates,
                report.overlaps,
                report.per_gen_over_budget,
                report.unattributed_succeeded
            ),
            (0, 0, 0, 0),
            "{}",
            report.line()
        );
        assert_eq!(twice_processed, 0, "one memory set per Evidence");
        assert_eq!(done + failed, m3_evidence.len() as i64);
        assert!(max_bound <= 4 && overlap <= 4, "M4: in-flight above 4");
        assert_eq!(breaks, 0, "I-SLOT");

        // ---- M5: kill -9 mid-call ----
        let m5 = new_tenant(&mut handle, "m5 kill9");
        let m5_row = accept_evidence_marked(&mut handle, m5, &live_note("m5", 0));
        let m5_job = handle.job_of(m5_row);
        let m5_env = |cmd: &mut std::process::Command| {
            cmd.env("HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS", "20")
                .env("HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS", "5")
                .env("HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS", "50")
                .arg("--distill-serve");
        };
        let mut cmd = live_serve_command(&worker_dsn, &key);
        m5_env(&mut cmd);
        let mut victim = cmd.spawn().expect("spawn the kill -9 victim");
        let started = std::time::Instant::now();
        let (uncertain_call, gen_one): (Uuid, i32) = loop {
            let row = handle
                .admin
                .query_opt(
                    "SELECT dispatch_model_call_id, claim_generation FROM ops.jobs \
                     WHERE job_id = $1 AND dispatch_state = 'DISPATCH_INTENT'",
                    &[&m5_job],
                )
                .expect("probe");
            if let Some(row) = row {
                break (row.get(0), row.get(1));
            }
            assert!(
                started.elapsed() < Duration::from_secs(120),
                "M5: the victim never dispatched"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        signal(victim.id(), "KILL");
        let _ = victim.wait();
        let killed_at = std::time::Instant::now();
        let mut cmd = live_serve_command(&worker_dsn, &key);
        m5_env(&mut cmd);
        let mut survivor = cmd.spawn().expect("spawn the surviving worker");
        let (mut uncertain_with_slot, mut t6) = (false, None::<(Duration, f64, i32)>);
        let m5_done = loop {
            let row = handle
                .admin
                .query_one(
                    "SELECT j.status, j.dispatch_state, j.last_error_class, j.attempt, \
                            extract(epoch FROM clock_timestamp())::float8, \
                            EXISTS (SELECT 1 FROM ops.provider_slots s \
                                    WHERE s.job_id = j.job_id AND s.claim_generation = $2) \
                     FROM ops.jobs j WHERE j.job_id = $1",
                    &[&m5_job, &gen_one],
                )
                .expect("M5 sample");
            let (status, state, class, attempt, now, slot): (
                String,
                Option<String>,
                Option<String>,
                i32,
                f64,
                bool,
            ) = (
                row.get(0),
                row.get(1),
                row.get(2),
                row.get(3),
                row.get(4),
                row.get(5),
            );
            uncertain_with_slot |= state.as_deref() == Some("EXECUTION_UNCERTAIN") && slot;
            if t6.is_none()
                && status != "PROCESSING"
                && class.as_deref() == Some("EXECUTION_UNCERTAIN")
            {
                t6 = Some((killed_at.elapsed(), now, attempt));
            }
            if status == "DONE" || status == "DEAD" {
                break status;
            }
            assert!(
                killed_at.elapsed() < Duration::from_secs(240),
                "M5: the job never settled after the kill ({status} {state:?} {class:?})"
            );
            std::thread::sleep(Duration::from_millis(200));
        };
        signal(survivor.id(), "TERM");
        let status = wait_exit(
            &mut survivor,
            Duration::from_secs(90),
            "the surviving worker",
        );
        assert!(status.success(), "{status:?}");
        let ledger_status: String = handle
            .admin
            .query_one(
                "SELECT status FROM ops.model_call_ledger WHERE model_call_id = $1",
                &[&uncertain_call],
            )
            .expect("uncertain ledger row")
            .get(0);
        let resend_after_t6: Option<bool> = t6.map(|(_, t6_db, _)| {
            handle
                .admin
                .query_one(
                    "SELECT bool_and(extract(epoch FROM begun_at)::float8 > $2) FROM ops.distill_calls \
                     WHERE job_id = $1 AND claim_generation > $3",
                    &[&m5_job, &t6_db, &gen_one],
                )
                .expect("resend order")
                .get::<_, Option<bool>>(0)
                .unwrap_or(false)
        });
        println!(
            "M5 uncertain_with_slot_kept={uncertain_with_slot} reconcile_delay_s={:?} attempt_at_t6={:?} uncertain_ledger_status={ledger_status} resend_after_t6={resend_after_t6:?} final={m5_done} calls={}",
            t6.map(|(d, _, _)| d.as_secs_f64()),
            t6.map(|(_, _, a)| a),
            handle.calls_of(m5_job)
        );
        assert!(uncertain_with_slot, "M5: T5 must keep the slot");
        let (_, _, attempt_at_t6) = t6.expect("M5: T6 reconcile never observed");
        assert_eq!(attempt_at_t6, 1, "the uncertain call stays counted");
        assert_eq!(
            ledger_status, "RESERVED",
            "the killed call's outcome is unknown"
        );
        assert_eq!(resend_after_t6, Some(true), "no resend before T6");
        assert_eq!(m5_done, "DONE");

        // ---- M7: inferred affect rows ----
        let m7 = new_tenant(&mut handle, "m7 affect");
        let feelings = [
            "I am thrilled: the migration finished two days early and the whole team celebrated together.",
            "Honestly I feel anxious about tomorrow's launch; the load tests kept failing all week.",
            "I was furious when the vendor cancelled our contract without any warning yesterday.",
            "I feel deeply grateful to Maria for staying late to fix the billing outage with me.",
            "Losing the Hamburg customer left me sad and exhausted after months of work on that account.",
        ];
        for text in feelings {
            accept_evidence_marked(&mut handle, m7, text);
        }
        let m7_tenant = handle.tenants[m7].tenant_id;
        let served = serve_until(
            &handle,
            &live,
            &live_config("c32-m7", 30.0, 4),
            Duration::from_secs(300),
            |admin| open_outbox(admin, &[m7_tenant]) == 0,
        );
        let row = handle
            .admin
            .query_one(
                "SELECT count(*) FILTER (WHERE origin = 'DISTILL'), \
                        count(*) FILTER (WHERE origin = 'DISTILL' AND confidence_bp > 5000), \
                        count(DISTINCT memory_id) FILTER (WHERE origin = 'DISTILL') \
                 FROM private.memory_affects WHERE tenant_id = $1",
                &[&m7_tenant],
            )
            .expect("affect rows");
        let (inferred, over_ceiling, memories): (i64, i64, i64) =
            (row.get(0), row.get(1), row.get(2));
        println!(
            "M7 evidence=5 inferred_affect_rows={inferred} memories_with_inferred_affect={memories} over_ceiling={over_ceiling} report={}",
            served.report.summary_line()
        );
        assert!(served.met, "M7: the affect Evidence did not settle");
        assert!(
            inferred > 0,
            "M7: no inferred affect row from 5 emotional Evidence"
        );
        assert_eq!(over_ceiling, 0, "inferred confidence ≤ 5000");

        // ---- M8: one tenant, two reasoning domains ----
        let m8 = new_tenant(&mut handle, "m8 two domains");
        let m8_tenant = handle.tenants[m8].tenant_id;
        let second = handle.add_domain(m8, "private derived_dispatch_e2e live m8 domain 2");
        let first = handle.tenants[m8].reasoning_domain_id;
        for n in 0..3 {
            accept_evidence_in(&mut handle, m8_tenant, second, &live_note("m8d2", n));
            accept_evidence_in(&mut handle, m8_tenant, first, &live_note("m8d1", n));
        }
        let served = serve_until(
            &handle,
            &live,
            &live_config("c32-m8", 30.0, 4),
            Duration::from_secs(300),
            |admin| open_outbox(admin, &[m8_tenant]) == 0,
        );
        let per_domain: Vec<(Uuid, String, i64)> = handle
            .admin
            .query(
                "SELECT e.reasoning_domain_id, o.status, count(*) FROM ops.outbox o \
                   JOIN private.evidence_objects e USING (evidence_id) \
                 WHERE o.tenant_id = $1 GROUP BY 1, 2 ORDER BY 1, 2",
                &[&m8_tenant],
            )
            .expect("per-domain outbox")
            .into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect();
        let (done, failed) = outbox_done_failed(&mut handle.admin, &[m8_tenant]);
        println!(
            "M8 domains=2 evidence=6 done={done} failed={failed} per_domain={per_domain:?} report={}",
            served.report.summary_line()
        );
        assert!(served.met, "M8: the two-domain tenant did not settle");
        assert_eq!((done, failed), (6, 0), "all Evidence distilled, FAILED = 0");
        handle.assert_i_slot();
    });
}

// ---------------------------------------------------------------------------
// ADR-0058 R10: the live channel A/B probe (a measurement, not an acceptance bound).
// ---------------------------------------------------------------------------

/// The A/B probe's Evidence count per channel (R10: n >= 100, the SAME texts on both channels).
const AB_N: usize = 100;

/// `AB_N` distinct realistic notes of the kinds the rehearsal and soak write: facts, preferences,
/// decisions, dated items, identifiers and emotional content. No `"` or `\`, so each text appears
/// verbatim in the request's user prompt (the recorder keys calls by it); no uuid.
fn ab_notes() -> Vec<String> {
    const TEMPLATES: [&str; 20] = [
        "We decided to move the {a} service to the new cluster before the end of {b}; the old nodes get drained one by one.",
        "I prefer short code reviews for {a} changes, ideally under {b} hundred lines, and I always want the tests in the same pull request.",
        "Fact: the {a} database is backed up every night at {b} am and the snapshots are kept for thirty days.",
        "Honestly I was really frustrated today, the {a} deploy failed three times and nobody answered on call until {b} pm.",
        "Meeting note from {b}: the team agreed that {a} owns the incident runbook from now on.",
        "Ticket OPS-{b}4 tracks the {a} latency regression; p95 went from 180 ms to 420 ms after release v2.{b}.0.",
        "Reminder for myself: renew the TLS certificate of the {a} gateway before {b} October, it expires at midnight UTC.",
        "I am relieved, the {a} migration finally finished without data loss after two weekends of work.",
        "The {a} team prefers Rust for new backend services because of memory safety, and Go only for small tools.",
        "Decision: from {b} onwards every {a} change needs a feature flag and a rollback plan written in the ticket.",
        "Customer feedback says the {a} page is confusing; {b} of the last ten support calls were about it.",
        "My manager asked me to take over the {a} roadmap next quarter and I feel nervous but also excited about it.",
        "The {a} API rate limit is {b}0 requests per second per tenant; anything above returns HTTP 429.",
        "We rejected the proposal to rewrite {a} in a new framework because the team has no time before {b}.",
        "Note: PR #{b}12 fixes the timezone bug in {a} where reports used local time instead of UTC.",
        "I like to start the day with the {a} dashboard and keep meetings after {b} am whenever possible.",
        "Postmortem summary: the {a} outage on {b} March lasted 47 minutes and was caused by an expired credential.",
        "The {a} contract with the vendor renews every year in {b}; legal needs sixty days notice to cancel.",
        "I am worried about the {a} on-call load, I was paged {b} times this week and slept badly.",
        "Architecture choice: {a} events go through the outbox table first, never straight to the queue, decided on {b} June.",
    ];
    const SERVICES: [&str; 5] = ["billing", "search", "payroll", "shipping", "catalog"];
    const SECONDS: [&str; 5] = ["3", "5", "7", "8", "9"];
    let mut notes = Vec::with_capacity(AB_N);
    for n in 0..AB_N {
        let template = TEMPLATES[n % TEMPLATES.len()];
        let variant = n / TEMPLATES.len();
        notes.push(
            template
                .replace("{a}", SERVICES[variant % SERVICES.len()])
                .replace("{b}", SECONDS[(variant + n) % SECONDS.len()]),
        );
    }
    notes
}

/// One provider call the [`RecordingProvider`] saw: which note it carried, how long it took and
/// what came back (`Ok((json, channel_fallback))` or the error's static class).
struct RecordedCall {
    note: Option<usize>,
    latency: Duration,
    outcome: Result<(String, bool), &'static str>,
}

/// Delegates every call to the live provider and records it (ADR-0058 R10: the probe measures the
/// first reply per Evidence, which the worker's counters fold away).
struct RecordingProvider<P> {
    inner: P,
    notes: Vec<String>,
    calls: Mutex<Vec<RecordedCall>>,
}

#[async_trait::async_trait]
impl<P: UserReasoningProvider> UserReasoningProvider for RecordingProvider<P> {
    fn descriptor(&self) -> &ReasoningProviderDescriptor {
        self.inner.descriptor()
    }

    fn endpoint_ref(&self) -> &str {
        self.inner.endpoint_ref()
    }

    fn model_revision(&self) -> Option<&str> {
        self.inner.model_revision()
    }

    async fn complete_structured(
        &self,
        context: &PrivateInferenceContext,
        request: StructuredReasoningRequest,
    ) -> Result<StructuredReasoningResponse, ReasoningProviderError> {
        let note = self
            .notes
            .iter()
            .position(|text| request.user_prompt.contains(text.as_str()));
        let started = std::time::Instant::now();
        let answer = self.inner.complete_structured(context, request).await;
        let outcome = match &answer {
            Ok(response) => Ok((response.json.clone(), response.channel_fallback)),
            Err(error) => Err(error.class()),
        };
        self.calls.lock().expect("calls").push(RecordedCall {
            note,
            latency: started.elapsed(),
            outcome,
        });
        answer
    }

    async fn analyze_vision(
        &self,
        _context: &PrivateInferenceContext,
        _request: VisionReasoningRequest,
    ) -> Result<VisionReasoningResponse, ReasoningProviderError> {
        unreachable!("distill never calls vision")
    }
}

/// Nearest-rank percentile of `sorted` in ms (`-` when empty).
fn percentile_ms(sorted: &[Duration], pct: usize) -> String {
    if sorted.is_empty() {
        return "-".to_owned();
    }
    let rank = (pct * sorted.len()).div_ceil(100).max(1) - 1;
    sorted[rank.min(sorted.len() - 1)].as_millis().to_string()
}

/// ADR-0058 R10 — the live A/B probe of the Distill output channel. The SAME [`AB_N`] Evidence
/// texts go once through the tool channel (with R9's `content` fallback) and once through the
/// content channel, same model, `in_flight` 4, each channel in its own tenant. One `AB` line per
/// channel: Evidence DONE / DEAD, first replies the ADR-0048 parser (or the tool-call shape) refused
/// and by which class, R9 fallbacks, dropped affects, provider calls and per-call latency p50/p95.
/// Asserts only that both runs finished: the comparison is a measurement, and R10's rule (tool
/// channel only if its DEAD count and first-reply malformed rate are not higher) is applied to the
/// rehearsal profile by hand and recorded in ADR-0058 D-M.
#[test]
#[ignore = "lane(a:shared_db) live MiniMax measurement (card 32 R10 distill_channel_ab_live): run with --include-ignored and HUMAUX_REQUIRE_MINIMAX=1"]
#[allow(clippy::too_many_lines)]
fn distill_channel_ab_live() {
    let Some(key) = live_minimax::load_minimax_key() else {
        humaux_testkit::skip_or_fail(
            "distill_channel_ab_live",
            "missing object: MINIMAX_API_KEY (env and /Volumes/data/viral-skill-eval/.env both empty)",
            humaux_testkit::ExternalDep::MiniMax,
        );
        return;
    };
    run("distill_channel_ab_live", |mut handle| {
        // SAFETY: set while SERIAL_GUARD is held (see `distill_fairness_live`).
        unsafe {
            std::env::set_var("HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS", "604800");
        }
        let http = Duration::from_secs(LIVE_HTTP_SECS);
        let notes = ab_notes();
        let distinct: std::collections::BTreeSet<&String> = notes.iter().collect();
        assert_eq!(distinct.len(), AB_N, "the probe's texts are distinct");
        let offer_affects = humaux_adapters::distill_reasoner::offers_affects(
            humaux_domain::evidence::EvidenceOriginClass::DirectUserInput,
        );
        let mut finished = Vec::new();
        for (channel, capabilities) in [
            ("tool", &live_minimax::REHEARSAL_CAPABILITIES[..]),
            ("content", &[ReasoningCapability::StructuredOutput][..]),
        ] {
            let idx = handle
                .add_tenant(
                    &format!("private derived_dispatch_e2e live ab {channel}"),
                    Some(EGRESS_PROCESSOR_ID),
                )
                .expect("live tenant");
            for text in &notes {
                accept_evidence_marked(&mut handle, idx, text);
            }
            let tenants = vec![handle.tenants[idx].tenant_id];
            let provider = RecordingProvider {
                inner: live_minimax::live_provider_declaring(
                    key.clone(),
                    MODEL_ID,
                    http,
                    capabilities,
                ),
                notes: notes.clone(),
                calls: Mutex::new(Vec::new()),
            };
            let served = serve_until(
                &handle,
                &provider,
                &live_config(&format!("c32-ab-{channel}"), 30.0, 4),
                Duration::from_secs(1800),
                |admin| open_outbox(admin, &tenants) == 0,
            );
            let (done, _) = outbox_done_failed(&mut handle.admin, &tenants);
            let dead: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.jobs WHERE tenant_id = ANY($1) \
                       AND job_type = 'DERIVED_DISTILL' AND status = 'DEAD'",
                    &[&tenants],
                )
                .expect("dead jobs")
                .get(0);
            let calls = provider.calls.into_inner().expect("calls");
            let mut first: Vec<Option<&RecordedCall>> = vec![None; AB_N];
            for call in &calls {
                if let Some(n) = call.note
                    && first[n].is_none()
                {
                    first[n] = Some(call);
                }
            }
            let mut by_class = std::collections::BTreeMap::<&str, u32>::new();
            for call in first.iter().flatten() {
                let class = match &call.outcome {
                    Err(class) if *class == ReasoningProviderError::FAILED_OUTPUT_SCHEMA_CLASS => {
                        Some("tool_call_shape")
                    }
                    Err(_) => None,
                    Ok((json, _)) => {
                        humaux_adapters::distill_reasoner::parse_distill_output_detailed(
                            json.as_bytes(),
                            offer_affects,
                        )
                        .err()
                        .map(humaux_adapters::distill_reasoner::DistillParseError::as_str)
                    }
                };
                if let Some(class) = class {
                    *by_class.entry(class).or_default() += 1;
                }
            }
            let mut latencies: Vec<Duration> = calls.iter().map(|c| c.latency).collect();
            latencies.sort();
            let classes = if by_class.is_empty() {
                "-".to_owned()
            } else {
                by_class
                    .iter()
                    .map(|(class, k)| format!("{class}:{k}"))
                    .collect::<Vec<_>>()
                    .join(",")
            };
            println!(
                "AB channel={channel} n={AB_N} done={done} dead={dead} first_reply_malformed={} by_class={classes} channel_fallback={} affects_dropped={} calls={} latency_p50_ms={} latency_p95_ms={}",
                by_class.values().sum::<u32>(),
                served.report.channel_fallback,
                served.report.affects_dropped,
                calls.len(),
                percentile_ms(&latencies, 50),
                percentile_ms(&latencies, 95),
            );
            println!(
                "AB channel={channel} unmatched_calls={} wall_s={:.1} report={}",
                calls.iter().filter(|c| c.note.is_none()).count(),
                served.wall.as_secs_f64(),
                served.report.summary_line()
            );
            if !served.met {
                retire_open_jobs(&mut handle.admin, &tenants);
            }
            finished.push((channel, served.met));
        }
        assert!(
            finished.iter().all(|(_, met)| *met),
            "both channels must finish their backlog: {finished:?}"
        );
    });
}
