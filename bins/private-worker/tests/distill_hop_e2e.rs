//! `private-worker::tests::distill_hop_e2e` — ADR-0016 Distill hop — accepted Evidence → 0..N
//!   `private.memory_records` by the private worker itself (`humaux_private_worker::distill::dispatch_pass`, the
//!   `--distill-once` code path, ADR-0058 slots + generation fence), then the remember-time `projection.stream_log`
//!   ticket resolved by the real `humaux_adapters::projection_worker::run_once`.
//! Depends-on: crates=[async-trait, hex, humaux-adapters, humaux-application, humaux-domain, humaux-infra-cell, humaux-local-secret-scan,
//!   humaux-projection, humaux-testkit, postgres, serde_json, sha2, tokio, uuid]; services=[PostgreSQL(role_gateway)
//!   r=[control.current_reasoning_route_binding, ops.claim_derived_work_v2, ops.commit_seq_seq, ops.data_disclosure_sources,
//!   ops.data_disclosures, ops.model_call_ledger, ops.private_inference_rpc_calls, private.distill_candidates,
//!   private.memory_affects, private.memory_evidence, private.memory_records, private.memory_subject_mentions, private.memory_subjects, private.processing_runs]
//!   w=[control.credentials, control.memberships, control.private_reasoning_domains, control.processor_models,
//!   control.provider_accounts, control.provider_endpoints, control.reasoning_credential_bindings,
//!   control.reasoning_profiles, control.reasoning_route_bindings, control.reasoning_route_candidates,
//!   control.reasoning_route_policies, control.tenants, control.users, control.workspace_memberships,
//!   control.workspaces, ops.outbox, ops.reasoning_account_health_observations,
//!   ops.reasoning_provider_health_observations, private.events, private.evidence_affects, private.evidence_objects, private.evidence_subjects,
//!   private.subject_keys, private.subjects, projection.stream_checkpoints,
//!   projection.stream_log, ops.jobs, ops.provider_slots] x=[control.current_reasoning_route_binding,
//!   ops.claim_derived_work_v2], PostgreSQL(role_private_worker), PostgreSQL(owner), PostgreSQL(role_maintenance),
//!   PostgreSQL(role_retrieval_worker), MiniMax, subprocess(gitleaks)]; env=[HUMAUX_MINIMAX_DNS_PINS,
//!   HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS, HUMAUX_TEST_GITLEAKS_BIN, HUMAUX_TEST_PG_DSN, MINIMAX_API_KEY];
//!   modules=[adapters::affect_repo, adapters::byok, adapters::byok::ssrf, adapters::contribution_reasoner,
//!   adapters::disclosure, adapters::distill_reasoner, adapters::distill_repo, adapters::jobs,
//!   adapters::membership_repo, adapters::model_call_ledger, adapters::postgres, adapters::projection_worker, adapters::provisioning,
//!   adapters::qdrant, adapters::reasoning_route_admission, application::affect, domain::affect, domain::authority,
//!   domain::egress, domain::error, domain::evidence, domain::identity, domain::ids, domain::ticket_family,
//!   humaux-local-secret-scan, humaux-testkit, infra-cell::permit, infra-cell::resource, infra-cell::transport,
//!   private-worker::distill, private-worker::tests::support::dispatch_fence,
//!   private-worker::tests::support::live_minimax, private-worker::tests::support::live_provider,
//!   projection::fingerprint, projection::serving, testkit::fixture_purge]
//! Called-by: [cargo-test]
//! Invariants: [no MINIMAX_API_KEY -> SKIP for the live test only; no DB (or not migrated to 0190) -> SKIP for all;
//!   HUMAUX_REQUIRE_MINIMAX/HUMAUX_REQUIRE_DB make either a panic via skip_or_fail; tests run one at a time and
//!   foreign scheduler rows are fenced, so the cross-tenant claim only serves the fixture tenant; cleanup deletes
//!   jobs, slots and data in one printed batch and the tenant row in a separate best-effort batch]
//! Spec: ADR-0005; ADR-0058; ADR-0059
//!
//! Four states, mirroring `bins/consolidation-worker/tests/consolidation_hop_e2e.rs`:
//! 1. `MINIMAX_API_KEY` missing (env + `.env` fallback) ⇒ visible SKIP for D1 only (D2–D6 use a
//!    fake provider and always run against a live DB).
//! 2. DB missing/unreachable/unmigrated (0147) ⇒ visible SKIP for every test in this file.
//! 3. `HUMAUX_REQUIRE_MINIMAX=1` / `HUMAUX_REQUIRE_DB=1` with the dependency missing ⇒ panic
//!    (ADR-0005), via `skip_or_fail`.
//! 4. Everything present ⇒ real assertions.
//!
//! Evidence is inserted the way `adapters::remember::remember_in_txn` writes it (its own SQL
//! mirrored statement by statement — `next_commit_seq` → `evidence_objects` → `events` →
//! `stream_checkpoints`/`stream_log` → `ops.outbox` PENDING); those functions are `pub(crate)`
//! and the pool they take is `role_gateway`'s, neither reachable from this crate.

use std::collections::{BTreeMap, BTreeSet};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use humaux_adapters::affect_repo;
use humaux_adapters::byok::{
    OpenAiCompatTransport, OpenAiCompatibleProvider, OpenAiHttpOutcome, OpenAiHttpRequest,
    PrivateInferenceContext, ReasoningProviderDescriptor, ReasoningProviderError,
    StructuredReasoningRequest, StructuredReasoningResponse, TokenUsage, UserReasoningProvider,
    VisionReasoningRequest, VisionReasoningResponse, ssrf,
};
use humaux_adapters::contribution_reasoner::ContributionReasonerConfig;
use humaux_adapters::disclosure::DeletionCapability;
use humaux_adapters::distill_reasoner::{DISTILL_PARSER_VERSION, distill_prompt_contract};
use humaux_adapters::distill_repo;
use humaux_adapters::jobs::{self, DistillLease};
use humaux_adapters::membership_repo::AdminAction;
use humaux_adapters::model_call_ledger;
use humaux_adapters::postgres::RuntimeDbPool;
use humaux_adapters::postgres::{MaintenanceDbPool, PrivateWorkerDbPool, RetrievalWorkerDbPool};
use humaux_adapters::projection_worker::{CardEmbedder, ProjectionWorkerDeps, RunOnceOutcome};
use humaux_adapters::provisioning::{self, RequeueTarget};
use humaux_adapters::qdrant::{
    PlacementClass, PromotionState, RetrievalFamily, TenantPlacementRow,
};
use humaux_adapters::reasoning_route_admission::ReasoningAdmissionLocator;
use humaux_application::affect::{ObservedAffect, memories_matching};
use humaux_domain::affect::{AffectFilter, EmotionLabel};
use humaux_domain::egress::ProcessorId;
use humaux_domain::error::ErrorCode;
use humaux_domain::evidence::payload_sha256;
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_infra_cell::{
    CallerId, CellAccessPermit, CellId, IntraCellError, IntraCellHttpTransport, IntraCellRequest,
    IntraCellResource, IntraCellResourceRegistry, IntraCellResponse, ResourceEntry,
    authorize_cell_access,
};
use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig, SealedRetrievalCard};
use humaux_private_worker::distill::{DistillDispatchConfig, DistillDispatchReport, dispatch_pass};
use humaux_projection::fingerprint::{ProcessingInputFingerprintInputs, source_hash};
use humaux_projection::serving::StreamFamily;
use humaux_testkit::fixture_purge::purge_tenant_fixture_sql;
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[path = "support/dispatch_fence.rs"]
mod dispatch_fence;
#[path = "support/live_minimax.rs"]
mod live_minimax;
// The first provider's `LiveProfile` lives here; this file dials it through `live_minimax` only.
#[allow(dead_code)]
#[path = "support/live_provider.rs"]
mod live_provider;

use live_minimax::{
    EnvKeyDecryptor, MINIMAX_CHAT_URL, MINIMAX_MODEL, descriptor, live_provider, load_minimax_key,
};

/// The claim is cross-tenant: tests of this file never overlap (one fixture at a time).
static SERIAL: Mutex<()> = Mutex::new(());

const NAME: &str = "distill_hop_e2e";
const EGRESS_PROCESSOR_ID: Uuid = Uuid::from_u128(0x2016);
const REGION: &str = "cn-shanghai";
const SERVICE_TIER: &str = "standard";
const PURPOSE_DB: &str = "PRIVATE_DISTILL_TEXT";
/// ADR-0058 D-T: a §72.3 budget no test in this file reaches (the budget gate has its own test,
/// distill_dispatch_v2 T21).
const TEST_BUDGET: jobs::DistillCallBudget = jobs::DistillCallBudget {
    window_seconds: 60.0,
    max_calls: 10_000,
};
const EVIDENCE_TEXT: &str =
    "New backend services must expose a health endpoint before any traffic is routed to them.";
/// The remember-side stream identity (`consolidate_repo::publish_rollup` / `projection_worker`
/// module doc): workspace-scoped `private_memory` / `PRIVATE_MEMORY` / `v1`.
const STREAM_DOMAIN: &str = "private_memory";
const STREAM_PROJECTION_KIND: &str = "PRIVATE_MEMORY";
const STREAM_PROJECTION_VERSION: &str = "v1";
const DIMENSION: u32 = 4;

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    // ADR-0059 D-D: a real login as `role`, its password from HUMAUX_ROLE_PASSWORD_<SUFFIX>.
    humaux_testkit::role_login_dsn(admin_dsn, role, |name| std::env::var(name).ok())
        .unwrap_or_else(|missing| panic!("missing object: {missing} (ADR-0059 D-D)"))
}

/// ADR-0060 D-B: every admitted route goes to `provider`, unless its recipient is not this
/// deployment's (the deny-only check the private worker's route mapping holds, §11.2.5).
fn routes<P: UserReasoningProvider + 'static>(
    provider: &Arc<P>,
) -> impl Fn(&ReasoningAdmissionLocator) -> Result<Arc<dyn UserReasoningProvider>, &'static str>
+ Send
+ Sync
+ use<P> {
    let provider = Arc::clone(provider);
    move |route| {
        if route.egress_processor_id.0 == EGRESS_PROCESSOR_ID {
            Ok(Arc::clone(&provider) as Arc<dyn UserReasoningProvider>)
        } else {
            Err("EGRESS_PROCESSOR_NOT_ALLOWED")
        }
    }
}

fn contribution_config() -> ContributionReasonerConfig {
    ContributionReasonerConfig {
        permit_ttl: Duration::from_secs(30),
        deletion_capability: DeletionCapability::Unknown,
        // Contribution-path prompt config only; Distill takes prompt/schema/budget from
        // `distill_prompt_contract()` and ignores these three.
        system_prompt: "s".to_string(),
        json_schema: "{}".to_string(),
        max_output_tokens: 64,
    }
}

/// Key-free provider for D2–D5: answers with canned JSON, matches the seeded admission lane
/// exactly (`provider_matches_admission` compares provider id / model / revision / endpoint).
/// A reply string starting with this prefix is returned as a provider `RetryWait` (429/5xx)
/// instead of a body.
const FAKE_RETRY_WAIT: &str = "ERR:retry_wait";

struct FakeProvider {
    descriptor: ReasoningProviderDescriptor,
    replies: Mutex<Vec<String>>,
    calls: Mutex<u32>,
    /// The `json_schema` of every request, in call order (the rendered menu the model saw).
    schemas: Mutex<Vec<String>>,
}

impl FakeProvider {
    fn new(replies: Vec<&str>) -> Arc<Self> {
        Arc::new(Self {
            descriptor: descriptor(),
            replies: Mutex::new(replies.into_iter().rev().map(str::to_owned).collect()),
            calls: Mutex::new(0),
            schemas: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> u32 {
        *self.calls.lock().expect("calls")
    }

    fn schemas(&self) -> Vec<String> {
        self.schemas.lock().expect("schemas").clone()
    }
}

#[async_trait]
impl UserReasoningProvider for FakeProvider {
    fn descriptor(&self) -> &ReasoningProviderDescriptor {
        &self.descriptor
    }

    fn endpoint_ref(&self) -> &str {
        MINIMAX_CHAT_URL
    }

    fn model_revision(&self) -> Option<&str> {
        self.descriptor.model_revision.as_deref()
    }

    async fn complete_structured(
        &self,
        _context: &PrivateInferenceContext,
        request: StructuredReasoningRequest,
    ) -> Result<StructuredReasoningResponse, ReasoningProviderError> {
        *self.calls.lock().expect("calls") += 1;
        self.schemas
            .lock()
            .expect("schemas")
            .push(request.json_schema);
        let json = self
            .replies
            .lock()
            .expect("replies")
            .pop()
            .expect("fake provider has a reply for every call");
        if json == FAKE_RETRY_WAIT {
            return Err(ReasoningProviderError::RetryWait { retry_after: None });
        }
        Ok(StructuredReasoningResponse {
            json,
            usage: TokenUsage::default(),
            channel_fallback: false,
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

struct Fixture {
    admin: Client,
    /// Holds every pre-existing scheduler row FOR UPDATE (`dispatch_fence`); released on drop.
    fence: Client,
    dsn: String,
    tenant_id: Uuid,
    user_id: Uuid,
    reasoning_domain_id: Uuid,
    workspace_id: Uuid,
    /// Dropped last: the next test's fixture starts only after this one is cleaned up.
    _serial: MutexGuard<'static, ()>,
}

impl Drop for Fixture {
    /// Throwaway-tenant cleanup through the one fixture purge (`humaux_testkit::fixture_purge`, ADR-0063 "Dev
    /// integrity finding"; the replica-mode DELETE list it replaces skipped RI and the identity release triggers and
    /// left orphans on dev). The slots the tenant's jobs hold are released first: `ops.provider_slots` rows are global
    /// and carry no FK, so the purge never reaches them. A failure is printed (the fixture tenant stays, nothing is
    /// half-deleted); the user is not a tenant row and goes last with constraints enforced, best effort.
    fn drop(&mut self) {
        let _ = self.fence.batch_execute("ROLLBACK");
        let tenant = self.tenant_id;
        if let Err(error) = self.admin.execute(
            "UPDATE ops.provider_slots SET job_id = NULL, claim_generation = NULL, bound_until = NULL \
             WHERE job_id IN (SELECT job_id FROM ops.jobs WHERE tenant_id = $1)",
            &[&tenant],
        ) {
            eprintln!("{NAME}: slot release for tenant {tenant} failed: {error}");
        }
        let purged = purge_tenant_fixture_sql(&tenant.to_string())
            .and_then(|sql| self.admin.batch_execute(&sql).map_err(|e| e.to_string()));
        if let Err(error) = purged {
            eprintln!("{NAME}: fixture cleanup for tenant {tenant} failed: {error}");
        }
        let _ = self.admin.execute(
            "DELETE FROM control.users WHERE user_id = $1",
            &[&self.user_id],
        );
    }
}

/// Seeds tenant + owner user + workspace + reasoning domain + the full R3 admission lane for
/// `PRIVATE_DISTILL_TEXT` over the MiniMax descriptor (mirrors consolidation_hop_e2e::setup_db).
#[allow(clippy::too_many_lines)]
fn setup_db(test_name: &str) -> Option<Fixture> {
    let serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // ADR-0026: the distill producer reads the candidate TTL from env (§78.1, no literal). Every
    // distill test that reaches a rejection needs it; set it here for all of them.
    // SAFETY: the mandated distill_hop_e2e run is --test-threads=1; every writer sets the same
    // value and nothing else reads this var, so there is no data race.
    unsafe {
        std::env::set_var("HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS", "604800");
    }
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(
            test_name,
            "missing object: HUMAUX_TEST_PG_DSN",
            ExternalDep::Postgres,
        );
        return None;
    };
    // dep: PostgreSQL(role_gateway) — role-scoped pool call
    let Ok(mut admin) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(
            test_name,
            "missing object: live Postgres",
            ExternalDep::Postgres,
        );
        return None;
    };
    let migrated: bool = admin
        .query_one(
            "SELECT to_regprocedure('control.current_reasoning_route_binding(uuid,text)') IS NOT NULL \
                AND to_regprocedure('ops.claim_derived_work_v2(text,double precision,double precision)') IS NOT NULL",
            &[],
        )
        .ok()?
        .get(0);
    if !migrated {
        skip_or_fail(
            test_name,
            "missing object: control.current_reasoning_route_binding / ops.claim_derived_work_v2 — run `cargo xtask migrate` (0190)",
            ExternalDep::Postgres,
        );
        return None;
    }
    let fence = match dispatch_fence::open(&mut admin, &dsn) {
        Ok(fence) => fence,
        Err(missing) => {
            skip_or_fail(
                test_name,
                &format!("missing object: {missing}"),
                ExternalDep::Postgres,
            );
            return None;
        }
    };

    let suffix = Uuid::now_v7();
    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name, state) VALUES ($1, 'ACTIVE') RETURNING tenant_id",
            &[&format!("e2e-fixture {NAME} throwaway tenant {suffix}")],
        )
        .expect("tenant")
        .get(0);
    let user_id: Uuid = admin
        .query_one(
            "INSERT INTO control.users(state) VALUES('ACTIVE') RETURNING user_id",
            &[],
        )
        .expect("user")
        .get(0);
    admin
        .execute(
            "INSERT INTO control.memberships(tenant_id,user_id,role,state) VALUES($1,$2,'OWNER','ACTIVE')",
            &[&tenant_id, &user_id],
        )
        .expect("membership");
    let reasoning_domain_id: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name, owner_user_id, status) \
             VALUES ($1, $2, $3, 'ACTIVE') RETURNING reasoning_domain_id",
            &[&tenant_id, &format!("{NAME} domain"), &user_id],
        )
        .expect("reasoning domain")
        .get(0);
    let workspace_id: Uuid = admin
        .query_one(
            "INSERT INTO control.workspaces (tenant_id, name) VALUES ($1, $2) \
             RETURNING workspace_id",
            &[&tenant_id, &format!("{NAME} workspace")],
        )
        .expect("workspace")
        .get(0);
    // ADR-0035 (card 13): 0163 re-points the WORKSPACE_SHARED visibility arm from
    // control.memberships (tenant membership) to an ACTIVE control.workspace_memberships row
    // for the row's OWN workspace. This fixture writes WORKSPACE_SHARED evidence/subjects under
    // `user_id` in `workspace_id`, so it needs an ACTIVE workspace membership, not just a tenant
    // membership, for role_gateway's WITH CHECK / USING to see them.
    admin
        .execute(
            "INSERT INTO control.workspace_memberships(tenant_id,workspace_id,user_id,role,state) \
             VALUES($1,$2,$3,'MEMBER','ACTIVE')",
            &[&tenant_id, &workspace_id, &user_id],
        )
        .expect("workspace membership");

    let credential: Uuid = admin
        .query_one(
            "INSERT INTO control.credentials(tenant_id,purpose,openbao_ref) VALUES($1,'USER_REASONING',$2) RETURNING credential_id",
            &[&tenant_id, &format!("openbao://{NAME}/{suffix}")],
        )
        .expect("credential locator")
        .get(0);
    let d = descriptor();
    // ADR-0060 D-C / E5: catalog row, Profile and provider declare one capability set; the
    // descriptor's revision is that set's label, so it never meets the frozen NULL-revision row.
    let caps: Vec<&str> = d.capabilities.iter().map(|c| c.as_str()).collect();
    admin
        .execute(
            "INSERT INTO control.processor_models(processor_id,provider_model_id,model_revision,capabilities,status,catalog_observed_at) \
             VALUES($1,$2,$3,$4,'ACTIVE',clock_timestamp()) ON CONFLICT DO NOTHING",
            &[&d.provider_id, &d.model_id, &d.model_revision, &caps],
        )
        .expect("processor model");
    let processor_model_id: Uuid = admin
        .query_one(
            "SELECT processor_model_id FROM control.processor_models \
             WHERE processor_id=$1 AND provider_model_id=$2 AND model_revision IS NOT DISTINCT FROM $3 AND status='ACTIVE'",
            &[&d.provider_id, &d.model_id, &d.model_revision],
        )
        .expect("processor model id")
        .get(0);
    let account_hash = suffix.as_bytes().repeat(2);
    let account: Uuid = admin
        .query_one(
            "INSERT INTO control.provider_accounts(tenant_id,owner_user_id,processor_id,external_account_ref_hash) VALUES($1,$2,$3,$4) RETURNING provider_account_id",
            &[&tenant_id, &user_id, &d.provider_id, &account_hash],
        )
        .expect("provider account")
        .get(0);
    admin
        .execute(
            "INSERT INTO control.reasoning_credential_bindings(credential_ref,tenant_id,owner_user_id,provider_account_id,processor_id) VALUES($1,$2,$3,$4,$5)",
            &[&credential, &tenant_id, &user_id, &account, &d.provider_id],
        )
        .expect("credential binding");
    let endpoint: Uuid = admin
        .query_one(
            "INSERT INTO control.provider_endpoints(tenant_id,provider_account_id,region,service_tier,endpoint_ref,egress_processor_id) VALUES($1,$2,$3,$4,$5,$6) RETURNING endpoint_id",
            &[&tenant_id, &account, &REGION, &SERVICE_TIER, &MINIMAX_CHAT_URL, &EGRESS_PROCESSOR_ID],
        )
        .expect("provider endpoint")
        .get(0);
    let profile: Uuid = admin
        .query_one(
            "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,billing_account_id,default_billing_instrument_id,capabilities,processing_region) VALUES($1,$2,$3,$4,$5,$6,NULL,NULL,$8,$7) RETURNING profile_id",
            &[&tenant_id, &user_id, &account, &endpoint, &processor_model_id, &credential, &REGION, &caps],
        )
        .expect("reasoning profile")
        .get(0);
    let policy: Uuid = admin
        .query_one(
            "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,$3) RETURNING route_policy_id",
            &[&tenant_id, &user_id, &PURPOSE_DB],
        )
        .expect("route policy")
        .get(0);
    admin
        .execute(
            "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,0)",
            &[&tenant_id, &policy, &profile],
        )
        .expect("pinned candidate");
    admin
        .execute(
            "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
            &[&policy],
        )
        .expect("shadow policy");
    admin
        .execute(
            "UPDATE control.reasoning_route_policies SET lifecycle_state='SERVING' WHERE route_policy_id=$1 AND policy_version=1",
            &[&policy],
        )
        .expect("serving policy");
    admin
        .execute(
            "INSERT INTO control.reasoning_route_bindings(tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES($1,$2,$3,$4,1)",
            &[&tenant_id, &reasoning_domain_id, &PURPOSE_DB, &policy],
        )
        .expect("route binding");
    admin
        .execute(
            "INSERT INTO ops.reasoning_provider_health_observations(tenant_id,processor_id,processor_model_id,provider_model_id,model_revision,provider_endpoint_id,endpoint_ref,region,service_tier,source_kind,reason_code,verdict,observed_at,valid_until) \
             VALUES($1,$2,$3,$4,$9,$5,$6,$7,$8,'TEST',NULL,'HEALTHY',clock_timestamp()-interval '1 second',clock_timestamp()+interval '30 minutes')",
            &[&tenant_id, &d.provider_id, &processor_model_id, &d.model_id, &endpoint, &MINIMAX_CHAT_URL, &REGION, &SERVICE_TIER, &d.model_revision],
        )
        .expect("provider health observation");
    admin
        .execute(
            "INSERT INTO ops.reasoning_account_health_observations(tenant_id,provider_account_id,credential_ref,billing_account_id,billing_instrument_id,source_kind,reason_code,account_verdict,credential_verdict,billing_account_verdict,billing_instrument_verdict,observed_at,valid_until) \
             VALUES($1,$2,$3,NULL,NULL,'TEST',NULL,'HEALTHY','VALID',NULL,NULL,clock_timestamp()-interval '1 second',clock_timestamp()+interval '30 minutes')",
            &[&tenant_id, &account, &credential],
        )
        .expect("account health observation");

    Some(Fixture {
        admin,
        fence,
        dsn,
        tenant_id,
        user_id,
        reasoning_domain_id,
        workspace_id,
        _serial: serial,
    })
}

/// One accepted Evidence exactly as `remember::remember_in_txn` writes it (see module doc):
/// AuthenticatedAgent origin, WORKSPACE_SHARED in the fixture workspace, `USER_MESSAGE` event
/// whose payload is `{"text": <text>}`. Returns `(evidence_id, commit_seq, stream_seq)`.
fn seed_evidence(f: &mut Fixture, text: &str) -> (Uuid, i64, i64) {
    seed_evidence_as(f, text, "AuthenticatedAgent")
}

/// [`seed_evidence`] with another `origin_class` (the DB CHECK literal).
fn seed_evidence_as(f: &mut Fixture, text: &str, origin_class: &str) -> (Uuid, i64, i64) {
    let payload = serde_json::json!({ "text": text });
    // `gateway::remember` hashes the request's raw JSON bytes without normalizing them
    // (`raw_json_bytes_are_hashed_without_normalization`); a pretty-printed body is what a
    // real client sends, and it is NOT the canonical jsonb rendering the worker re-reads.
    let raw = serde_json::to_vec_pretty(&payload).expect("payload bytes");
    let digest = payload_sha256(&raw);
    let mut txn = f.admin.transaction().expect("begin remember txn");
    let commit_seq: i64 = txn
        .query_one("SELECT nextval('ops.commit_seq_seq')", &[])
        .expect("commit_seq")
        .get(0);
    let evidence_id: Uuid = txn
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                origin_principal_id, visibility_class, visibility_workspace_id, reasoning_domain_id, occurred_at) \
             VALUES ($1, 'EVENT', decode($2, 'hex'), 'PRIVATE', $6, $3, \
                     'WORKSPACE_SHARED', $4, $5, now()) \
             RETURNING evidence_id",
            &[
                &f.tenant_id,
                &digest.to_hex(),
                &f.user_id,
                &f.workspace_id,
                &f.reasoning_domain_id,
                &origin_class,
            ],
        )
        .expect("insert evidence")
        .get(0);
    txn.execute(
        "INSERT INTO private.events (event_id, event_kind, payload) VALUES ($1, 'USER_MESSAGE', $2)",
        &[&evidence_id, &payload],
    )
    .expect("insert event");
    txn.execute(
        "INSERT INTO projection.stream_checkpoints \
           (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version) \
         VALUES ($1, 'workspace', $2, $3, $4, $5) \
         ON CONFLICT (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version) DO NOTHING",
        &[
            &f.tenant_id,
            &f.workspace_id,
            &STREAM_DOMAIN,
            &STREAM_PROJECTION_KIND,
            &STREAM_PROJECTION_VERSION,
        ],
    )
    .expect("checkpoint bootstrap");
    let stream_seq: i64 = txn
        .query_one(
            "UPDATE projection.stream_checkpoints SET issued_highwater = issued_highwater + 1 \
             WHERE tenant_id = $1 AND scope_kind = 'workspace' AND scope_id = $2 AND domain = $3 \
               AND projection_kind = $4 AND projection_version = $5 \
             RETURNING issued_highwater",
            &[
                &f.tenant_id,
                &f.workspace_id,
                &STREAM_DOMAIN,
                &STREAM_PROJECTION_KIND,
                &STREAM_PROJECTION_VERSION,
            ],
        )
        .expect("issue stream_seq")
        .get(0);
    txn.execute(
        "INSERT INTO projection.stream_log \
           (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, stream_seq, commit_seq) \
         VALUES ($1, 'workspace', $2, $3, $4, $5, $6, $7)",
        &[
            &f.tenant_id,
            &f.workspace_id,
            &STREAM_DOMAIN,
            &STREAM_PROJECTION_KIND,
            &STREAM_PROJECTION_VERSION,
            &stream_seq,
            &commit_seq,
        ],
    )
    .expect("insert stream_log ticket");
    txn.execute(
        "INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id) \
         VALUES ($1, $2, $3, 'EVIDENCE_ACCEPTED', $4)",
        &[&f.tenant_id, &commit_seq, &stream_seq, &evidence_id],
    )
    .expect("insert outbox");
    txn.commit().expect("commit remember txn");
    (evidence_id, commit_seq, stream_seq)
}

/// ADR-0058 D-K config with one seat (the fake provider serves replies in call order).
fn dispatch_config(lease_owner: &str, lease_seconds: f64) -> DistillDispatchConfig {
    DistillDispatchConfig {
        lease_owner: lease_owner.to_owned(),
        lease_seconds,
        in_flight: 1,
        hard_deadline_seconds: 2.0 * (5.0 + lease_seconds),
        http_timeout_seconds: 5.0,
        not_ready_park_seconds: 600.0,
        max_attempts: 5,
        budget: TEST_BUDGET,
        health_renew_seconds: 1800,
    }
}

fn private_pool(rt: &tokio::runtime::Runtime, f: &Fixture) -> PrivateWorkerDbPool {
    rt.block_on(
        // dep: PostgreSQL(role_private_worker) — role-scoped pool call
        PrivateWorkerDbPool::connect(&dsn_as_role(&f.dsn, "role_private_worker")),
    )
    .expect("private worker pool")
}

/// One `--distill-once` drain over the fixture tenant (foreign tenants are fenced).
fn run_pass<P: UserReasoningProvider + 'static>(
    rt: &tokio::runtime::Runtime,
    f: &Fixture,
    provider: &Arc<P>,
    lease_owner: &str,
) -> DistillDispatchReport {
    let pool = private_pool(rt, f);
    rt.block_on(dispatch_pass(
        &pool,
        &routes(provider),
        contribution_config(),
        &dispatch_config(lease_owner, 30.0),
    ))
    .expect("distill dispatch pass")
}

/// The Evidence's `DERIVED_DISTILL` job (0164's idempotency key).
fn job_of(f: &mut Fixture, evidence_id: Uuid) -> Uuid {
    f.admin
        .query_one(
            "SELECT job_id FROM ops.jobs WHERE idempotency_key = 'derived-work:DERIVED_DISTILL:' || $1::uuid::text",
            &[&evidence_id],
        )
        .expect("one job per Evidence")
        .get(0)
}

/// `(status, attempt, claim_generation, last_error_class)` of the Evidence's job.
fn job_state(f: &mut Fixture, evidence_id: Uuid) -> (String, i32, i32, Option<String>) {
    let job = job_of(f, evidence_id);
    let r = f
        .admin
        .query_one(
            "SELECT status, attempt, claim_generation, last_error_class FROM ops.jobs WHERE job_id = $1",
            &[&job],
        )
        .expect("job row");
    (r.get(0), r.get(1), r.get(2), r.get(3))
}

/// Makes the Evidence's backed-off job claimable now.
fn ready_now(f: &mut Fixture, evidence_id: Uuid) {
    let job = job_of(f, evidence_id);
    f.admin
        .execute(
            "UPDATE ops.jobs SET next_retry_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            &[&job],
        )
        .expect("ready now");
}

/// This tenant's `ops.model_call_ledger` rows on the distill purposes (§19.1), as `observe`
/// reads them back.
#[derive(Debug, Clone)]
struct LedgerRow {
    rows: i64,
    status: Option<String>,
    purpose: Option<String>,
    model: Option<String>,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
}

/// Everything D1–D5 read back about one Evidence.
struct Observed {
    memories: i64,
    classes: Vec<String>,
    visibility: Vec<(String, Option<Uuid>, Option<Uuid>)>,
    primary_links: i64,
    outbox_status: String,
    run_output_count: Option<i32>,
    run_completed: bool,
    run_source_hash_len: i32,
    run_prompt_hash: String,
    run_parser_version: String,
    disclosure_outcomes: Vec<String>,
    rpc_calls: i64,
    /// Card 20's primary acceptance gate: the §19.1 cost row that must exist ALONGSIDE the §7.4
    /// disclosure row, not instead of it. Deleting `distill_reasoner`'s `reserve_private_call_with_disclosure` /
    /// `finalize_private_call` makes this go red, which is what "the ledger covers this hop"
    /// has to mean.
    ledger: LedgerRow,
}

fn observe(f: &mut Fixture, evidence_id: Uuid) -> Observed {
    let row = f
        .admin
        .query_one(
            "SELECT \
               (SELECT count(*) FROM private.memory_evidence me WHERE me.evidence_id = $1) AS memories, \
               (SELECT count(*) FROM private.memory_evidence me WHERE me.evidence_id = $1 AND me.role = 'PRIMARY' AND me.ordinal = 0) AS primary_links, \
               (SELECT coalesce(array_agg(m.authority_class), ARRAY[]::text[]) FROM private.memory_records m JOIN private.memory_evidence me ON me.memory_id = m.memory_id WHERE me.evidence_id = $1) AS classes, \
               (SELECT status FROM ops.outbox WHERE evidence_id = $1 AND event_type = 'EVIDENCE_ACCEPTED') AS outbox_status, \
               (SELECT count(*) FROM ops.private_inference_rpc_calls c WHERE c.tenant_id = $2) AS rpc_calls, \
               (SELECT count(*) FROM ops.model_call_ledger l WHERE l.tenant_id = $2 \
                  AND l.purpose IN ('PRIVATE_DISTILL_TEXT','PRIVATE_DISTILL_VISION')) AS ledger_rows, \
               (SELECT l.status FROM ops.model_call_ledger l WHERE l.tenant_id = $2 \
                  AND l.purpose IN ('PRIVATE_DISTILL_TEXT','PRIVATE_DISTILL_VISION') \
                  ORDER BY l.called_at DESC LIMIT 1) AS ledger_status, \
               (SELECT l.purpose FROM ops.model_call_ledger l WHERE l.tenant_id = $2 \
                  AND l.purpose IN ('PRIVATE_DISTILL_TEXT','PRIVATE_DISTILL_VISION') \
                  ORDER BY l.called_at DESC LIMIT 1) AS ledger_purpose, \
               (SELECT l.model FROM ops.model_call_ledger l WHERE l.tenant_id = $2 \
                  AND l.purpose IN ('PRIVATE_DISTILL_TEXT','PRIVATE_DISTILL_VISION') \
                  ORDER BY l.called_at DESC LIMIT 1) AS ledger_model, \
               (SELECT l.input_tokens FROM ops.model_call_ledger l WHERE l.tenant_id = $2 \
                  AND l.purpose IN ('PRIVATE_DISTILL_TEXT','PRIVATE_DISTILL_VISION') \
                  ORDER BY l.called_at DESC LIMIT 1) AS ledger_input_tokens, \
               (SELECT l.output_tokens FROM ops.model_call_ledger l WHERE l.tenant_id = $2 \
                  AND l.purpose IN ('PRIVATE_DISTILL_TEXT','PRIVATE_DISTILL_VISION') \
                  ORDER BY l.called_at DESC LIMIT 1) AS ledger_output_tokens",
            &[&evidence_id, &f.tenant_id],
        )
        .expect("observe");
    let memories: i64 = row.get("memories");
    let primary_links: i64 = row.get("primary_links");
    let classes: Vec<String> = row.get("classes");
    let outbox_status: String = row.get("outbox_status");
    let rpc_calls: i64 = row.get("rpc_calls");
    let ledger = LedgerRow {
        rows: row.get("ledger_rows"),
        status: row.get("ledger_status"),
        purpose: row.get("ledger_purpose"),
        model: row.get("ledger_model"),
        input_tokens: row.get("ledger_input_tokens"),
        output_tokens: row.get("ledger_output_tokens"),
    };
    let visibility = f
        .admin
        .query(
            "SELECT m.visibility_class, m.visibility_user_id, m.visibility_workspace_id \
             FROM private.memory_records m JOIN private.memory_evidence me ON me.memory_id = m.memory_id \
             WHERE me.evidence_id = $1",
            &[&evidence_id],
        )
        .expect("visibility")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    let run = f
        .admin
        .query_one(
            "SELECT output_count, completed_at IS NOT NULL AS completed, length(source_hash) AS source_hash_len, \
                    prompt_hash, parser_version \
             FROM private.processing_runs WHERE evidence_id = $1 ORDER BY started_at DESC LIMIT 1",
            &[&evidence_id],
        )
        .expect("processing run row");
    let disclosure_outcomes = f
        .admin
        .query(
            "SELECT d.outcome FROM ops.data_disclosures d \
             JOIN ops.data_disclosure_sources s ON s.disclosure_id = d.disclosure_id \
             WHERE s.evidence_id = $1",
            &[&evidence_id],
        )
        .expect("disclosures")
        .into_iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    Observed {
        memories,
        classes,
        visibility,
        primary_links,
        outbox_status,
        run_output_count: run.get("output_count"),
        run_completed: run.get("completed"),
        run_source_hash_len: run.get("source_hash_len"),
        run_prompt_hash: run.get("prompt_hash"),
        run_parser_version: run.get("parser_version"),
        disclosure_outcomes,
        rpc_calls,
        ledger,
    }
}

// ----------------------------------------------------------------------------
// Projection leg (D1/D3): the real `projection_worker::run_once` over the remember-time ticket.
// ----------------------------------------------------------------------------

/// Same discovery `crates/adapters/tests/projection_worker.rs::discover_gitleaks` does.
fn discover_gitleaks() -> Option<(std::path::PathBuf, String, String)> {
    let candidates: Vec<std::path::PathBuf> =
        if let Ok(bin) = std::env::var("HUMAUX_TEST_GITLEAKS_BIN") {
            vec![bin.into()]
        } else {
            vec![
                "/private/tmp/gitleaks-8.30.1/gitleaks".into(),
                "/private/tmp/gitleaks-linux/gitleaks".into(),
            ]
        };
    for path in candidates {
        if !path.is_absolute() || !path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&path).ok()?;
        let sha256 = Sha256::digest(&bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        // dep: subprocess(gitleaks) — runs a candidate gitleaks binary to read its version
        let output = Command::new(&path)
            .arg("version")
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        if !output.status.success() {
            continue;
        }
        let version = String::from_utf8(output.stdout).ok()?.trim().to_owned();
        if version.is_empty() {
            continue;
        }
        return Some((path, version, sha256));
    }
    None
}

struct FixedEmbedder;

#[async_trait]
impl CardEmbedder for FixedEmbedder {
    async fn embed_cards(
        &self,
        _tenant_id: TenantId,
        dimension: u32,
        cards: &[SealedRetrievalCard],
        _memory_ids: &[Uuid],
    ) -> Result<Vec<Vec<f32>>, ErrorCode> {
        Ok(cards
            .iter()
            .map(|_| vec![0.25_f32; dimension as usize])
            .collect())
    }
}

/// Stub Qdrant: acknowledges every upsert and reports every id it was asked about as visible —
/// the projection leg under test is ticket resolution + settlement, not Qdrant itself
/// (`crates/adapters/tests/projection_worker.rs` owns the live-Qdrant proof).
struct AckTransport {
    upserted: Mutex<Vec<serde_json::Value>>,
}

#[async_trait]
impl IntraCellHttpTransport for AckTransport {
    async fn execute(
        &self,
        _permit: &CellAccessPermit,
        request: IntraCellRequest,
    ) -> Result<IntraCellResponse, IntraCellError> {
        if request.path.contains("/points/scroll") {
            let ids = request
                .json_body
                .as_ref()
                .and_then(|b| b.pointer("/filter/must/0/has_id").cloned())
                .unwrap_or_else(|| serde_json::json!([]));
            let points: Vec<serde_json::Value> = ids
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|id| serde_json::json!({ "id": id }))
                .collect();
            return Ok(IntraCellResponse {
                status: 200,
                json_body: Some(serde_json::json!({ "result": { "points": points } })),
            });
        }
        if let Some(body) = request.json_body {
            self.upserted.lock().expect("upserted").push(body);
        }
        Ok(IntraCellResponse {
            status: 200,
            json_body: Some(
                serde_json::json!({ "result": { "status": "acknowledged" }, "status": "ok" }),
            ),
        })
    }
}

fn qdrant_permit() -> CellAccessPermit {
    let cell = CellId(Uuid::new_v4());
    let caller = CallerId(format!("{NAME}-projection"));
    let cidr = "127.0.0.1/32".parse().expect("loopback cidr");
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            "127.0.0.1",
            6333,
            cell,
            vec![cidr],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .expect("qdrant resource entry"),
    );
    let registry = IntraCellResourceRegistry::new(entries, cell, caller);
    authorize_cell_access(
        &registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(60),
    )
    .expect("qdrant permit")
}

/// Runs one projection batch over the fixture's workspace stream. `None` (visible SKIP) when
/// no gitleaks binary is staged — the scanner is `SealedRetrievalCard`'s sole constructor path.
fn run_projection(
    rt: &tokio::runtime::Runtime,
    f: &Fixture,
    test_name: &str,
) -> Option<(RunOnceOutcome, usize)> {
    let Some((gitleaks_bin, gitleaks_version, gitleaks_sha256)) = discover_gitleaks() else {
        skip_or_fail(
            test_name,
            "missing object: gitleaks binary (HUMAUX_TEST_GITLEAKS_BIN unset, no staged fallback)",
            ExternalDep::Postgres,
        );
        return None;
    };
    let scanner = LocalSecretScanner::new(LocalSecretScannerConfig {
        executable: gitleaks_bin,
        expected_version: gitleaks_version,
        expected_executable_sha256: gitleaks_sha256,
        timeout: Duration::from_secs(5),
        max_payload_bytes: 64 * 1024,
        finding_exit_code: 1,
    })
    .expect("gitleaks-backed scanner");
    let transport = Arc::new(AckTransport {
        upserted: Mutex::new(Vec::new()),
    });
    let outcome = rt.block_on(async {
        // dep: PostgreSQL(role_retrieval_worker) — role-scoped pool call
        let pool = RetrievalWorkerDbPool::connect(&dsn_as_role(&f.dsn, "role_retrieval_worker"))
            .await
            .expect("retrieval worker pool");
        let deps = ProjectionWorkerDeps {
            pool,
            embedder: Arc::new(FixedEmbedder),
            scanner: Arc::new(scanner),
            transport: transport.clone(),
            permit: qdrant_permit(),
            placement: TenantPlacementRow {
                tenant_id: TenantId(f.tenant_id),
                projection_family: RetrievalFamily::PrivateMemoryV1,
                collection_name: format!("distill_hop_{}", f.tenant_id.simple()),
                shard_key: None,
                placement_class: PlacementClass::SharedFallback,
                point_count: 0,
                bytes_estimate: 0,
                promotion_state: PromotionState::Stable,
            },
            family: StreamFamily::new(
                TenantId(f.tenant_id),
                "workspace",
                f.workspace_id,
                STREAM_DOMAIN,
                STREAM_PROJECTION_KIND,
            ),
            embedding_version: "embed-v1".to_owned(),
            projection_version: STREAM_PROJECTION_VERSION.to_owned(),
            dimension: DIMENSION,
            // Card 21 fix pass: the §7.4 identity `advance_prefix` attributes this hop's
            // checkpoint to (migration 0171).
            processor_id: ProcessorId(Uuid::from_u128(0x0171_0002)),
        };
        humaux_adapters::projection_worker::run_once(&deps, 16)
            .await
            .expect("projection run_once")
    });
    let upserts = transport.upserted.lock().expect("upserted").len();
    Some((outcome, upserts))
}

fn ticket_state(f: &mut Fixture, stream_seq: i64) -> (String, Option<String>) {
    let row = f
        .admin
        .query_one(
            "SELECT state, error_class FROM projection.stream_log \
             WHERE tenant_id = $1 AND scope_kind = 'workspace' AND scope_id = $2 \
               AND domain = $3 AND projection_kind = $4 AND projection_version = $5 AND stream_seq = $6",
            &[
                &f.tenant_id,
                &f.workspace_id,
                &STREAM_DOMAIN,
                &STREAM_PROJECTION_KIND,
                &STREAM_PROJECTION_VERSION,
                &stream_seq,
            ],
        )
        .expect("ticket row");
    (row.get(0), row.get(1))
}

// ----------------------------------------------------------------------------
// D1 — live MiniMax, full chain, then the projection leg resolves the ticket.
// ----------------------------------------------------------------------------

#[test]
#[allow(clippy::too_many_lines)]
fn d1_live_distill_writes_memories_and_projection_resolves_ticket() {
    let test_name = "d1_live_distill_writes_memories_and_projection_resolves_ticket";
    let Some(key) = load_minimax_key() else {
        skip_or_fail(
            test_name,
            "missing object: MINIMAX_API_KEY (env and /Volumes/data/viral-skill-eval/.env both empty)",
            ExternalDep::MiniMax,
        );
        return;
    };
    let Some(mut f) = setup_db(test_name) else {
        return;
    };
    let (evidence_id, _commit_seq, stream_seq) = seed_evidence(&mut f, EVIDENCE_TEXT);
    let rt = tokio::runtime::Runtime::new().expect("rt");
    // ADR-0058 D-K: the dispatch window is sized from the provider's own transport timeout. The
    // fake-provider `dispatch_config` (5 s) would cut a live call — or a live malformed re-ask —
    // at `hard_deadline − lease` = 40 s and leave the job UNKNOWN instead of DONE.
    const LIVE_HTTP_SECS: u64 = 120;
    let provider = Arc::new(live_provider(
        key,
        MINIMAX_MODEL,
        Duration::from_secs(LIVE_HTTP_SECS),
    ));
    let live_config = DistillDispatchConfig {
        http_timeout_seconds: LIVE_HTTP_SECS as f64,
        hard_deadline_seconds: 2.0 * (LIVE_HTTP_SECS as f64 + 30.0),
        ..dispatch_config("d1-worker", 30.0)
    };
    // ADR-0058 D-F: a live provider transient settles RETRY with a backoff instead of failing the
    // pass (main-line chain 2026-10-02: one RETRY_WAIT after 3.9 s made a single-pass assertion
    // red). The product path is exercised as it is: a deferred job is made due and claimed again,
    // within the job's own attempt budget; only the wall-clock backoff is skipped.
    const LIVE_PASSES: u32 = 3;
    let mut report = None;
    for pass in 1..=LIVE_PASSES {
        let r = rt
            .block_on(dispatch_pass(
                &private_pool(&rt, &f),
                &routes(&provider),
                contribution_config(),
                &live_config,
            ))
            .expect("distill dispatch pass");
        assert_eq!(
            r.claimed, 1,
            "pass {pass}: one DERIVED_DISTILL job claimed: {r:?}"
        );
        let done = r.completed == 1;
        assert!(
            done || r.deferred == 1,
            "pass {pass}: the job is DONE or deferred by a provider transient, nothing else: {r:?}"
        );
        report = Some(r);
        if done {
            break;
        }
        ready_now(&mut f, evidence_id);
    }
    let report = report.expect("at least one pass ran");
    assert_eq!(
        report.completed, 1,
        "the live job must settle DONE within {LIVE_PASSES} passes: {report:?}"
    );

    let o = observe(&mut f, evidence_id);
    assert!(
        o.memories >= 1,
        "live MiniMax must distill at least one memory from {EVIDENCE_TEXT:?}: {report:?}"
    );
    assert_eq!(
        o.primary_links, o.memories,
        "every memory links PRIMARY/ordinal 0"
    );
    for class in &o.classes {
        assert!(
            class == "PublicKnowledge" || class == "PrivateKnowledge",
            "AuthenticatedAgent ceiling is PrivateKnowledge (§10.1), got {class}"
        );
    }
    for (visibility_class, user, workspace) in &o.visibility {
        assert_eq!(visibility_class, "WORKSPACE_SHARED");
        assert_eq!(*user, None);
        assert_eq!(*workspace, Some(f.workspace_id));
    }
    assert_eq!(o.outbox_status, "DONE");
    assert!(o.run_completed, "processing run must be completed");
    assert_eq!(o.run_output_count, Some(o.memories as i32));
    assert_eq!(o.run_source_hash_len, 32);
    assert_eq!(
        o.run_prompt_hash,
        hex::encode(
            // AuthenticatedAgent origin with no declared affect: the menu is offered (ADR-0058
            // D-P amendment); the channel is the one the live descriptor declares (D-M).
            distill_prompt_contract(
                humaux_domain::authority::AuthorityClass::PrivateKnowledge,
                true,
                humaux_adapters::distill_reasoner::distill_output_channel(&descriptor()),
            )
            .sha256
            .0
        ),
        "prompt_hash column is the contract sha256"
    );
    assert_eq!(o.run_parser_version, DISTILL_PARSER_VERSION);
    // ADR-0048: one round trip per attempt, and the ADR-0048 empty-retry is an attempt. The
    // expected count is derived from the report's own counter rather than hard-coded at 1, so
    // the §7.4 / §19.1 "every call leaves a receipt" invariant is still what is being tested —
    // a retry that left NO disclosure or NO ledger row still goes red.
    // …and so is a malformed-reply retry (ADR-0048 addendum D-D): the live model answered
    // `memory_type: "Requirement"` 8 of 29 times on 2026-09-26, and a retry that is not counted
    // here turns this test red for a fixed hop (card 24 review P1).
    let attempts = 1 + report.empty_retries as i64 + report.malformed_retries as i64;
    assert_eq!(
        o.disclosure_outcomes.len(),
        attempts as usize,
        "one §7.4 disclosure per attempt: {report:?} {:?}",
        o.disclosure_outcomes
    );
    assert!(
        o.disclosure_outcomes.iter().all(|o| o == "SUCCESS"),
        "{:?}",
        o.disclosure_outcomes
    );
    // Card 20 acceptance, the positive half: ONE §19.1 ledger row per provider call, with
    // the right purpose/model/status and real token usage — BOTH it and the §7.4 disclosure row
    // above, never either alone. Deleting `distill_reasoner`'s `reserve_private_call_with_disclosure` or
    // `finalize_private_call` turns this red.
    let l = &o.ledger;
    assert_eq!(
        l.rows, attempts,
        "one ops.model_call_ledger row per distill provider call: {report:?}"
    );
    assert_eq!(l.status.as_deref(), Some("SUCCEEDED"));
    assert_eq!(l.purpose.as_deref(), Some("PRIVATE_DISTILL_TEXT"));
    assert!(
        l.model.as_deref().is_some_and(|m| !m.is_empty()),
        "ledger row carries the admitted provider model, got {:?}",
        l.model
    );
    assert!(
        l.input_tokens.is_some_and(|v| v > 0),
        "input_tokens from the provider's usage block, got {:?}",
        l.input_tokens
    );
    assert!(
        l.output_tokens.is_some_and(|v| v > 0),
        "output_tokens (0168) from the provider's usage block — a generative call bills them \
         and they used to be parsed and discarded, got {:?}",
        l.output_tokens
    );
    assert_eq!(
        o.rpc_calls, 0,
        "rpc-free: no ops.private_inference_rpc_calls row"
    );
    assert_fingerprint_recomputes(&mut f, evidence_id);

    let Some((outcome, upserts)) = run_projection(&rt, &f, test_name) else {
        return;
    };
    assert_eq!(
        outcome.done, 1,
        "ticket resolves to the distilled memory: {outcome:?}"
    );
    assert_eq!(outcome.failed, 0);
    assert_eq!(upserts, 1);
    let (state, error_class) = ticket_state(&mut f, stream_seq);
    assert_eq!(state, "DONE");
    assert_eq!(error_class, None);
    assert_eq!(outcome.projection_highwater, stream_seq as u64);

    println!(
        "D1 ASSERTION LOG: evidence={evidence_id} report={report:?} memories={} classes={:?} \
         visibility=WORKSPACE_SHARED/{} outbox={} run(completed={},output_count={:?},source_hash_len={},parser_version={},source_hash_recomputes=true) \
         disclosures={:?} rpc_calls={} ledger={:?} projection(done={},failed={},highwater={}) ticket=({state},{error_class:?})",
        o.memories,
        o.classes,
        f.workspace_id,
        o.outbox_status,
        o.run_completed,
        o.run_output_count,
        o.run_source_hash_len,
        o.run_parser_version,
        o.disclosure_outcomes,
        o.rpc_calls,
        o.ledger,
        outcome.done,
        outcome.failed,
        outcome.projection_highwater,
    );
}

/// `(status, lease_owner)` of the Evidence's `EVIDENCE_ACCEPTED` row.
fn outbox_lease(f: &mut Fixture, evidence_id: Uuid) -> (String, Option<String>) {
    let row = f
        .admin
        .query_one(
            "SELECT status, lease_owner FROM ops.outbox \
             WHERE evidence_id = $1 AND event_type = 'EVIDENCE_ACCEPTED'",
            &[&evidence_id],
        )
        .expect("outbox row");
    (row.get(0), row.get(1))
}

/// Appends a fresh provider-health observation (same route identity as the seeded one) with
/// `verdict`, observed now — the resolver reads the newest one.
fn observe_provider_health(f: &mut Fixture, verdict: &str) {
    f.admin
        .execute(
            "INSERT INTO ops.reasoning_provider_health_observations \
               (tenant_id, processor_id, processor_model_id, provider_model_id, model_revision, \
                provider_endpoint_id, endpoint_ref, region, service_tier, source_kind, reason_code, \
                verdict, observed_at, valid_until) \
             SELECT tenant_id, processor_id, processor_model_id, provider_model_id, model_revision, \
                    provider_endpoint_id, endpoint_ref, region, service_tier, 'TEST', NULL, \
                    $2, clock_timestamp(), clock_timestamp() + interval '30 minutes' \
             FROM ops.reasoning_provider_health_observations \
             WHERE tenant_id = $1 ORDER BY observed_at DESC, observation_id DESC LIMIT 1",
            &[&f.tenant_id, &verdict],
        )
        .expect("append provider health observation");
}

/// `(rows, completed rows)` of `private.processing_runs` for one Evidence.
fn processing_runs(f: &mut Fixture, evidence_id: Uuid) -> (i64, i64) {
    let row = f
        .admin
        .query_one(
            "SELECT count(*), count(completed_at) FROM private.processing_runs WHERE evidence_id = $1",
            &[&evidence_id],
        )
        .expect("processing runs");
    (row.get(0), row.get(1))
}

/// §16.1.1 replay check on the latest run row of one Evidence: `source_hash` must equal
/// `humaux_projection::fingerprint::source_hash` over the persisted axis columns plus the
/// evidence axis, and `evidence_payload_sha256[]` must be the Evidence's own §8.1 anchor.
///
/// **Card 21 closes ADR-0016's registered deviation.** The evidence axis used to be
/// `payload_sha256(canonical jsonb of events.payload)` — a digest of a *re-rendering* — while
/// the run row's `evidence_payload_sha256[]` stored `evidence_objects.payload_sha256`, the
/// Evidence's own raw-bytes anchor. Two different values, so the fingerprint could not be
/// recomputed from the row: this helper had to go back to `private.events` to reproduce it.
/// `EvidencePayloadSha256::from_stored_digest` (a read-back, not a second hasher) removed that
/// need, and this check now does what §16.1.1 asks — **recompute from the run row alone**.
///
/// The seed still writes non-canonical raw bytes, so `anchor != canonical` remains provable
/// here; that inequality is what makes the assertion below meaningful rather than vacuous (with
/// the old code, feeding the stored anchor would have failed).
fn assert_fingerprint_recomputes(f: &mut Fixture, evidence_id: Uuid) {
    let run = f
        .admin
        .query_one(
            "SELECT processor_kind, processor_version, model_provider, model_id, model_revision, \
                    prompt_version, prompt_hash, parser_version, embedding_version, \
                    card_builder_version, context_snapshot_seq, evidence_payload_sha256, source_hash \
             FROM private.processing_runs WHERE evidence_id = $1 ORDER BY started_at DESC LIMIT 1",
            &[&evidence_id],
        )
        .expect("processing run row");
    let evidence = f
        .admin
        .query_one(
            "SELECT e.payload_sha256, ev.payload FROM private.evidence_objects e \
             JOIN private.events ev ON ev.event_id = e.evidence_id WHERE e.evidence_id = $1",
            &[&evidence_id],
        )
        .expect("evidence row");
    let anchor: Vec<u8> = evidence.get(0);
    let payload: serde_json::Value = evidence.get(1);
    let canonical = payload_sha256(&serde_json::to_vec(&payload).expect("canonical payload"));
    assert_ne!(
        hex::encode(&anchor),
        canonical.to_hex(),
        "seed must exercise the raw-bytes vs canonical-jsonb divergence"
    );
    let stored_axis: Vec<Vec<u8>> = run.get("evidence_payload_sha256");
    assert_eq!(
        stored_axis,
        vec![anchor.clone()],
        "evidence_payload_sha256[] is the Evidence's own payload_sha256 (0064 column contract)"
    );
    // §16.1.1 (card 21): the axis fed to the hash comes from THE ROW, read back — nothing here
    // reaches for `events.payload`. `anchor` above is only used to prove the row's array is the
    // Evidence's anchor; the recomputation below is a pure function of `run`.
    let axis: Vec<humaux_domain::evidence::EvidencePayloadSha256> = stored_axis
        .iter()
        .map(|d| {
            humaux_domain::evidence::EvidencePayloadSha256::from_stored_digest(d)
                .expect("persisted digest is 32 bytes")
        })
        .collect();
    let embedding_version: Option<String> = run.get("embedding_version");
    let card_builder_version: Option<String> = run.get("card_builder_version");
    let context_snapshot_seq: i64 = run.get("context_snapshot_seq");
    let recomputed = source_hash(&ProcessingInputFingerprintInputs {
        evidence_payload_sha256: &axis,
        processor_kind: run.get("processor_kind"),
        processor_version: run.get("processor_version"),
        model_provider: run.get("model_provider"),
        model_id: run.get("model_id"),
        model_revision: run.get("model_revision"),
        prompt_version: run.get("prompt_version"),
        prompt_hash: run.get("prompt_hash"),
        embedding_version: embedding_version.as_deref(),
        parser_version: run.get("parser_version"),
        card_builder_version: card_builder_version.as_deref(),
        context_snapshot_seq: u64::try_from(context_snapshot_seq).expect("non-negative seq"),
    });
    let stored: Vec<u8> = run.get("source_hash");
    assert_eq!(
        stored,
        recomputed.as_bytes().to_vec(),
        "§16.1.1: source_hash recomputes byte-for-byte from the persisted run row alone"
    );
    // Fault injection, run every time rather than described in a comment: perturb ONE persisted
    // axis (the one ADR-0016's deviation hid) and the recomputation must disagree. A check that
    // only ever sees the matching case cannot tell "recomputable" from "always equal".
    let mut tampered = stored_axis[0].clone();
    tampered[0] ^= 0xff;
    let tampered_axis = [
        humaux_domain::evidence::EvidencePayloadSha256::from_stored_digest(&tampered)
            .expect("still 32 bytes"),
    ];
    let divergent = source_hash(&ProcessingInputFingerprintInputs {
        evidence_payload_sha256: &tampered_axis,
        processor_kind: run.get("processor_kind"),
        processor_version: run.get("processor_version"),
        model_provider: run.get("model_provider"),
        model_id: run.get("model_id"),
        model_revision: run.get("model_revision"),
        prompt_version: run.get("prompt_version"),
        prompt_hash: run.get("prompt_hash"),
        embedding_version: embedding_version.as_deref(),
        parser_version: run.get("parser_version"),
        card_builder_version: card_builder_version.as_deref(),
        context_snapshot_seq: u64::try_from(context_snapshot_seq).expect("non-negative seq"),
    });
    assert_ne!(
        stored,
        divergent.as_bytes().to_vec(),
        "changing a persisted axis must change the fingerprint"
    );
    // §15.1/§78.1 (card 21): this file's fixture constants are the POSITIVE CONTROL for the
    // derived ticket family — they spell the triple, production derives it, and the two must
    // agree. Checked here so the fixture cannot drift away from what the workers actually use.
    let family = humaux_domain::ticket_family::TicketFamily::PrivateMemory;
    assert_eq!(STREAM_DOMAIN, family.domain());
    assert_eq!(STREAM_PROJECTION_KIND, family.projection_kind());
    assert_eq!(STREAM_PROJECTION_VERSION, family.projection_version());
}

// ----------------------------------------------------------------------------
// D2 — over-ceiling candidate is rejected, never downgraded.
// ----------------------------------------------------------------------------

#[test]
fn d2_over_ceiling_candidate_rejected_not_downgraded() {
    let Some(mut f) = setup_db("d2_over_ceiling_candidate_rejected_not_downgraded") else {
        return;
    };
    let (evidence_id, _, _) = seed_evidence(&mut f, EVIDENCE_TEXT);
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let provider = FakeProvider::new(vec![
        r#"{"memories":[{"content":"Health endpoint before traffic.","memory_type":"Decision","class":"UserCorrection","confidence":0.9}]}"#,
    ]);
    let report = run_pass(&rt, &f, &provider, "d2-worker");
    assert_eq!(provider.calls(), 1);
    assert_eq!(report.completed, 1, "{report:?}");
    assert_eq!(report.rejected, 1, "rejection counted: {report:?}");
    assert_eq!(report.memories, 0);
    let o = observe(&mut f, evidence_id);
    assert_eq!(o.memories, 0, "over-ceiling candidate must leave NO row");
    assert_eq!(o.outbox_status, "DONE");
    assert!(o.run_completed);
    assert_eq!(o.run_output_count, Some(0));

    // ADR-0026 (Card 6) D-B: the rejected candidate is persisted PENDING so a user can confirm
    // it. FAULT SENTINEL: this exact-count assertion goes red if the candidate INSERT is dropped
    // from the distill transaction (the card's required fault-injection check).
    let candidates = f
        .admin
        .query(
            "SELECT state, rejection_reason, requested_class, memory_type, confidence, \
                    confirmed_memory_id, candidate_body \
             FROM private.distill_candidates WHERE source_evidence_id = $1",
            &[&evidence_id],
        )
        .expect("read distill candidates");
    assert_eq!(
        candidates.len(),
        1,
        "a rejected distill output persists exactly one candidate"
    );
    let c = &candidates[0];
    let state: String = c.get(0);
    let reason: String = c.get(1);
    let requested_class: String = c.get(2);
    let memory_type: String = c.get(3);
    let confirmed: Option<Uuid> = c.get(5);
    let body: serde_json::Value = c.get(6);
    assert_eq!(state, "PENDING", "candidate is PENDING");
    assert_eq!(
        reason, "origin_authority_ceiling",
        "closed CandidateRejection reason is persisted verbatim"
    );
    assert_eq!(
        requested_class, "UserCorrection",
        "the rejected requested class is carried (not downgraded)"
    );
    assert_eq!(memory_type, "DECISION", "the parsed memory_type is carried");
    assert!(
        confirmed.is_none(),
        "a PENDING candidate names no memory yet"
    );
    assert_eq!(
        body["key_claim"], "Health endpoint before traffic.",
        "the parsed content is carried so a user can confirm it"
    );

    println!(
        "D2 ASSERTION LOG: report={report:?} memories={} outbox={} output_count={:?} \
         candidate=(state={state} reason={reason} requested={requested_class} type={memory_type})",
        o.memories, o.outbox_status, o.run_output_count
    );
}

// ----------------------------------------------------------------------------
// D3 — zero memories is a valid answer; the ticket settles as a no-op.
// ----------------------------------------------------------------------------

#[test]
fn d3_zero_memories_settles_outbox_and_ticket() {
    let test_name = "d3_zero_memories_settles_outbox_and_ticket";
    let Some(mut f) = setup_db(test_name) else {
        return;
    };
    let (evidence_id, _, stream_seq) = seed_evidence(&mut f, "hi there");
    let rt = tokio::runtime::Runtime::new().expect("rt");
    // ADR-0048 (card 24): an empty answer is re-asked ONCE before it settles, so the fixture
    // must hand out two replies and the pass must report exactly one `empty_retries`. This is
    // the observable that the 2026-09-19 / 2026-09-20 `done: 1, memories: 0` chains lacked.
    let provider = FakeProvider::new(vec![r#"{"memories":[]}"#, r#"{"memories":[]}"#]);
    let report = run_pass(&rt, &f, &provider, "d3-worker");
    assert_eq!(report.completed, 1, "{report:?}");
    assert_eq!(
        report.empty_retries, 1,
        "one bounded retry, then settle: {report:?}"
    );
    assert_eq!(provider.calls(), 2, "the retry is a real second round trip");
    let o = observe(&mut f, evidence_id);
    assert_eq!(o.memories, 0);
    assert_eq!(o.outbox_status, "DONE");
    assert_eq!(o.run_output_count, Some(0));
    // Card 24 review P1 sentinel: BOTH attempts are closed. If the abandoned first run is ever
    // left `completed_at IS NULL` again this reads (2, 1) — d6's deferred-provider-FAILURE
    // shape at :1607 — i.e. a succeeded-but-empty provider call recorded as a failed one.
    assert_eq!(
        processing_runs(&mut f, evidence_id),
        (2, 2),
        "empty-retry: two run rows, both completed (neither is a failure marker)"
    );

    // ADR-0026 (Card 6) D-B: a SKIPPED_BY_POLICY pass (0 outputs) is NOT a rejected candidate —
    // it creates no queue row. FAULT SENTINEL: goes red if a 0-output pass ever persists a
    // candidate (distinct from D2's ceiling-rejected case, which persists exactly one).
    let candidate_count: i64 = f
        .admin
        .query_one(
            "SELECT count(*) FROM private.distill_candidates WHERE source_evidence_id = $1",
            &[&evidence_id],
        )
        .expect("read distill candidate count")
        .get(0);
    assert_eq!(
        candidate_count, 0,
        "a zero-output (SKIPPED_BY_POLICY) distill pass persists no candidate"
    );

    let Some((outcome, upserts)) = run_projection(&rt, &f, test_name) else {
        return;
    };
    assert_eq!(upserts, 0, "nothing to index");
    assert_eq!(outcome.skipped_by_policy, 1, "{outcome:?}");
    assert_eq!(outcome.failed, 0);
    assert_eq!(outcome.pending, 0);
    let (state, error_class) = ticket_state(&mut f, stream_seq);
    assert_eq!(
        state, "SKIPPED_BY_POLICY",
        "0-memory ticket must not stay ISSUED"
    );
    assert_eq!(error_class.as_deref(), Some("no_memory_distilled"));
    assert_eq!(
        outcome.projection_highwater, stream_seq as u64,
        "a legitimately empty evidence is not an open gap (§15.7)"
    );
    println!(
        "D3 ASSERTION LOG: report={report:?} outbox={} ticket=({state},{error_class:?}) highwater={}",
        o.outbox_status, outcome.projection_highwater
    );
}

/// D3 companion: a ticket whose Evidence is still PENDING stays ISSUED (not FAILED) until the
/// hop runs — the projection worker never races the distiller into a permanent gap.
#[test]
fn d3b_undistilled_ticket_stays_issued() {
    let test_name = "d3b_undistilled_ticket_stays_issued";
    let Some(mut f) = setup_db(test_name) else {
        return;
    };
    let (_, _, stream_seq) = seed_evidence(&mut f, "not yet distilled");
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let Some((outcome, _)) = run_projection(&rt, &f, test_name) else {
        return;
    };
    assert_eq!(outcome.pending, 1, "{outcome:?}");
    assert_eq!(outcome.failed, 0);
    let (state, error_class) = ticket_state(&mut f, stream_seq);
    assert_eq!(state, "ISSUED");
    assert_eq!(error_class, None);
    assert_eq!(outcome.projection_highwater, 0);
}

// ----------------------------------------------------------------------------
// D4 — parser fail-closed: FAILED outbox, no rows, run left without completed_at.
// ----------------------------------------------------------------------------

#[test]
fn d4_parser_fail_closed_marks_outbox_failed() {
    let Some(mut f) = setup_db("d4_parser_fail_closed_marks_outbox_failed") else {
        return;
    };
    let (bad_enum, _, _) = seed_evidence(&mut f, "bad enum evidence");
    let (extra_key, _, _) = seed_evidence(&mut f, "extra key evidence");
    let rt = tokio::runtime::Runtime::new().expect("rt");
    // ADR-0048 addendum (card 24 soak): a refused reply is re-asked ONCE, so each row consumes
    // two replies (`FakeProvider::new` serves them in vec order; claim order is commit_seq
    // ascending). Both attempts of each row are malformed here, so both rows still fail closed
    // — with the retry counted.
    let provider = FakeProvider::new(vec![
        r#"{"memories":[{"content":"c","memory_type":"Constraint","class":"PrivateKnowledge","confidence":0.5}]}"#,
        r#"{"memories":[{"content":"c","memory_type":"Constraint","class":"PrivateKnowledge","confidence":0.5}]}"#,
        r#"{"memories":[{"content":"c","memory_type":"Fact","class":"PrivateKnowledge","confidence":0.5,"leak":"x"}]}"#,
        r#"{"memories":[{"content":"c","memory_type":"Fact","class":"PrivateKnowledge","confidence":0.5,"leak":"x"}]}"#,
    ]);
    let report = run_pass(&rt, &f, &provider, "d4-worker");
    assert_eq!(report.claimed, 2, "{report:?}");
    assert_eq!(report.failed, 2, "{report:?}");
    assert_eq!(report.dead, 2, "{report:?}");
    assert_eq!(report.completed, 0);
    assert_eq!(report.memories, 0);
    assert_eq!(
        report.malformed_retries, 2,
        "one bounded retry per refused row, then fail closed: {report:?}"
    );
    assert_eq!(
        provider.calls(),
        4,
        "each retry is a real second round trip"
    );
    for evidence_id in [bad_enum, extra_key] {
        // ADR-0048 D-D / ADR-0058 D-F: after its re-ask budget the job is DEAD with the class and
        // its outbox row FAILED in the same transaction (two counted calls, never re-claimed).
        let (status, attempt, _, class) = job_state(&mut f, evidence_id);
        assert_eq!(
            (status.as_str(), attempt, class.as_deref()),
            ("DEAD", 2, Some("FAILED_OUTPUT_SCHEMA"))
        );
        let o = observe(&mut f, evidence_id);
        assert_eq!(o.memories, 0, "fail-closed parse writes no memory row");
        assert_eq!(o.outbox_status, "FAILED");
        assert!(
            !o.run_completed,
            "failure marker (ADR-0016 D4): processing run keeps completed_at NULL"
        );
        assert_eq!(o.run_output_count, None);
        assert_eq!(
            o.run_source_hash_len, 32,
            "fingerprint recorded before the call"
        );
        assert_eq!(
            o.disclosure_outcomes,
            vec!["SUCCESS".to_owned(), "SUCCESS".to_owned()],
            "bytes did leave twice; the replies were the problem"
        );
    }
    // DEAD is terminal: a second pass claims nothing.
    let again = run_pass(&rt, &f, &FakeProvider::new(vec![]), "d4-worker-2");
    assert_eq!(again.claimed, 0);
    println!("D4 ASSERTION LOG: report={report:?} second_pass={again:?}");
}

/// ADR-0048 addendum (card 24 soak, 2026-09-26): a reply the parser refuses is re-asked ONCE.
/// The live model wrote `memory_type: "Requirement"` in 8 of 29 soak distills, and every one
/// became a permanent `FAILED` ticket that wedged the §15.4 prefix. The abandoned attempt keeps
/// ADR-0016 D4's failure marker — it WAS a failed attempt, unlike d3's succeeded-but-empty one.
#[test]
fn d4b_a_malformed_reply_is_re_asked_once_then_settles() {
    let Some(mut f) = setup_db("d4b_a_malformed_reply_is_re_asked_once_then_settles") else {
        return;
    };
    let (evidence_id, _, _) = seed_evidence(&mut f, "a rule the model first mistyped");
    let rt = tokio::runtime::Runtime::new().expect("rt");
    // Vec order is call order: the invented type first, the contract shape second.
    let provider = FakeProvider::new(vec![
        r#"{"memories":[{"content":"c","memory_type":"Requirement","class":"PrivateKnowledge","confidence":0.9}]}"#,
        r#"{"memories":[{"content":"c","memory_type":"Decision","class":"PrivateKnowledge","confidence":0.9}]}"#,
    ]);
    let report = run_pass(&rt, &f, &provider, "d4b-worker");
    assert_eq!(report.completed, 1, "{report:?}");
    assert_eq!(report.failed, 0, "{report:?}");
    assert_eq!(report.malformed_retries, 1, "one bounded retry: {report:?}");
    assert_eq!(provider.calls(), 2, "the retry is a real second round trip");
    let o = observe(&mut f, evidence_id);
    assert_eq!(o.memories, 1);
    assert_eq!(o.outbox_status, "DONE");
    assert!(
        o.run_completed,
        "the attempt that produced the memory is closed"
    );
    assert_eq!(
        processing_runs(&mut f, evidence_id),
        (2, 1),
        "two attempts: the refused one keeps D4's failure marker, the second completed"
    );
    assert_eq!(
        o.disclosure_outcomes.len(),
        2,
        "one §7.4 disclosure per attempt"
    );
    println!("D4B ASSERTION LOG: report={report:?}");
}

// ----------------------------------------------------------------------------
// D5 — idempotency under the ADR-0058 generation fence: a DONE job is never re-claimed; a claim
// that crashed after taking its outbox row is reclaimed (T4) and distilled once; a live claim is
// not stolen.
// ----------------------------------------------------------------------------

#[test]
fn d5_two_passes_and_expired_lease_never_duplicate_memories() {
    let Some(mut f) = setup_db("d5_two_passes_and_expired_lease_never_duplicate_memories") else {
        return;
    };
    let reply = r#"{"memories":[{"content":"Health endpoint before traffic.","memory_type":"Decision","class":"PrivateKnowledge","confidence":0.9}]}"#;
    let (evidence_a, _, _) = seed_evidence(&mut f, EVIDENCE_TEXT);
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let counted = distill_counters();
    let first = run_pass(&rt, &f, &FakeProvider::new(vec![reply]), "d5-worker");
    assert_eq!(first.completed, 1, "{first:?}");
    assert_eq!(observe(&mut f, evidence_a).memories, 1);
    // §41.2 R4 on the real path (card 34b): one committed write = exactly one run and one output; a deleted
    // or duplicated emit call is red here.
    assert_eq!(
        counters_since(counted),
        (1, 1),
        "(runs, outputs) of one committed distill write"
    );
    assert_fingerprint_recomputes(&mut f, evidence_a);
    let second = run_pass(&rt, &f, &FakeProvider::new(vec![]), "d5-worker");
    assert_eq!(
        second.claimed, 0,
        "a DONE job is never re-claimed: {second:?}"
    );
    assert_eq!(observe(&mut f, evidence_a).memories, 1);

    // A worker that crashed mid-flight: its claim took the outbox row, then its lease expired.
    let (evidence_b, _, _) = seed_evidence(&mut f, "crashed lease evidence");
    let pool = private_pool(&rt, &f);
    let crashed = rt
        .block_on(jobs::claim_distill(&pool, "dead-worker", 30.0, 70.0))
        .expect("claim")
        .expect("the job is READY");
    let taken = rt
        .block_on(distill_repo::take_outbox_row(
            &pool,
            &DistillLease::of(&crashed, "dead-worker"),
            evidence_b,
            crashed.hard_deadline,
        ))
        .expect("take");
    assert!(matches!(taken, distill_repo::TakenOutbox::Taken(_)));
    f.admin
        .execute(
            "UPDATE ops.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            &[&crashed.job_id],
        )
        .expect("simulate the crash");
    let retry = run_pass(&rt, &f, &FakeProvider::new(vec![reply]), "d5-worker-2");
    assert_eq!(
        retry.claimed, 1,
        "the expired claim is READY again (T4): {retry:?}"
    );
    assert_eq!(retry.completed, 1, "{retry:?}");
    assert_eq!(observe(&mut f, evidence_b).memories, 1);
    let (status, attempt, generation, _) = job_state(&mut f, evidence_b);
    assert_eq!((status.as_str(), attempt, generation), ("DONE", 1, 2));
    let after = run_pass(&rt, &f, &FakeProvider::new(vec![]), "d5-worker-3");
    assert_eq!(after.claimed, 0);
    assert_eq!(
        observe(&mut f, evidence_b).memories,
        1,
        "retry never duplicates"
    );
    assert_eq!(observe(&mut f, evidence_a).memories, 1);

    // A live claim is not stolen: a concurrent worker sees nothing.
    let (evidence_c, _, _) = seed_evidence(&mut f, "live lease evidence");
    let busy = rt
        .block_on(jobs::claim_distill(&pool, "busy-worker", 600.0, 600.0))
        .expect("claim")
        .expect("the job is READY");
    let contended = run_pass(&rt, &f, &FakeProvider::new(vec![]), "d5-worker-4");
    assert_eq!(
        contended.claimed, 0,
        "a live claim must not be stolen: {contended:?}"
    );
    assert_eq!(job_state(&mut f, evidence_c).2, busy.claim_generation);
    println!(
        "D5 ASSERTION LOG: first={first:?} second={second:?} retry={retry:?} after={after:?} contended={contended:?}"
    );
}

/// A provider whose first call blocks until `gate_one` opens and second call until `gate_two`
/// opens; both then answer one admissible memory.
struct GatedProvider {
    descriptor: ReasoningProviderDescriptor,
    calls: AtomicU32,
    gate_one: AtomicBool,
    gate_two: AtomicBool,
}

#[async_trait]
impl UserReasoningProvider for GatedProvider {
    fn descriptor(&self) -> &ReasoningProviderDescriptor {
        &self.descriptor
    }

    fn endpoint_ref(&self) -> &str {
        MINIMAX_CHAT_URL
    }

    fn model_revision(&self) -> Option<&str> {
        self.descriptor.model_revision.as_deref()
    }

    async fn complete_structured(
        &self,
        _context: &PrivateInferenceContext,
        _request: StructuredReasoningRequest,
    ) -> Result<StructuredReasoningResponse, ReasoningProviderError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let gate = if n == 1 {
            &self.gate_one
        } else {
            &self.gate_two
        };
        while !gate.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(StructuredReasoningResponse {
            json: r#"{"memories":[{"content":"Health endpoint before traffic.","memory_type":"Decision","class":"PrivateKnowledge","confidence":0.9}]}"#.to_string(),
            usage: TokenUsage::default(),
            channel_fallback: false,
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

/// One admin statement on its own thread (the sync `postgres` client must not run inside the
/// test's async runtime).
fn admin_exec(dsn: &str, sql: &'static str, id: Uuid) {
    let dsn = dsn.to_owned();
    std::thread::spawn(move || {
        // dep: PostgreSQL(owner) — admin statement from inside the async test body
        let mut admin = Client::connect(&dsn, NoTls).expect("admin connection");
        admin.batch_execute("SELECT 1").expect("ping");
        admin.execute(sql, &[&id]).expect("admin statement");
    })
    .join()
    .expect("admin thread");
}

/// This process's (`private_distill_runs_total`, `private_distill_outputs_total`,
/// `private_reasoning_usage_total`). Exact deltas hold because `SERIAL` runs this file's tests one at a time.
fn distill_counters() -> (u64, u64, u64) {
    (
        distill_repo::private_distill_runs_total(),
        distill_repo::private_distill_outputs_total(),
        model_call_ledger::private_reasoning_usage_total(),
    )
}

/// (runs, outputs) counted since `before`.
fn counters_since(before: (u64, u64, u64)) -> (u64, u64) {
    let now = distill_counters();
    (now.0 - before.0, now.1 - before.1)
}

/// Moves the Evidence's outbox lease to another owner while the job's own lease stays live.
const STEAL_OUTBOX_LEASE: &str = "UPDATE ops.outbox SET lease_owner = 'd5c-thief' \
     WHERE evidence_id = $1 AND event_type = 'EVIDENCE_ACCEPTED'";

/// Input / output tokens [`StealingProvider`] reports.
const D5C_USAGE: (u64, u64) = (1200, 34);

/// A provider that, inside its one call, takes the outbox row's lease away from the worker, then
/// answers one admissible memory with [`D5C_USAGE`].
struct StealingProvider {
    descriptor: ReasoningProviderDescriptor,
    dsn: String,
    evidence_id: Uuid,
}

#[async_trait]
impl UserReasoningProvider for StealingProvider {
    fn descriptor(&self) -> &ReasoningProviderDescriptor {
        &self.descriptor
    }

    fn endpoint_ref(&self) -> &str {
        MINIMAX_CHAT_URL
    }

    fn model_revision(&self) -> Option<&str> {
        self.descriptor.model_revision.as_deref()
    }

    async fn complete_structured(
        &self,
        _context: &PrivateInferenceContext,
        _request: StructuredReasoningRequest,
    ) -> Result<StructuredReasoningResponse, ReasoningProviderError> {
        admin_exec(&self.dsn, STEAL_OUTBOX_LEASE, self.evidence_id);
        Ok(StructuredReasoningResponse {
            json: r#"{"memories":[{"content":"Health endpoint before traffic.","memory_type":"Decision","class":"PrivateKnowledge","confidence":0.9}]}"#.to_string(),
            usage: TokenUsage {
                input_tokens: Some(D5C_USAGE.0),
                output_tokens: Some(D5C_USAGE.1),
                ..TokenUsage::default()
            },
            channel_fallback: false,
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

/// D5c (card 34b, ADR-0061; §41.2 / §42) — the distill counters count only what commits. The
/// provider takes the outbox lease away mid-call, so the write's outbox settle is refused AFTER its
/// memory insert and run finish, and the transaction rolls back: runs and outputs stay flat. The
/// finalized call's reported tokens were spent, so `private_reasoning_usage_total` adds them once.
/// Faults: count inside the write transaction (the card-34b review P0) ⇒ (runs, outputs) = (1, 1);
/// drop or duplicate the usage emit in `finalize_private_call` ⇒ a usage delta of 0 or twice the tokens.
#[test]
fn d5c_a_rolled_back_write_counts_nothing_and_a_finalized_call_counts_its_tokens_once() {
    let Some(mut f) = setup_db(
        "d5c_a_rolled_back_write_counts_nothing_and_a_finalized_call_counts_its_tokens_once",
    ) else {
        return;
    };
    let (evidence_id, _, _) = seed_evidence(&mut f, EVIDENCE_TEXT);
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let provider = Arc::new(StealingProvider {
        descriptor: descriptor(),
        dsn: f.dsn.clone(),
        evidence_id,
    });
    let counted = distill_counters();
    let report = run_pass(&rt, &f, &provider, "d5c-worker");
    let usage = model_call_ledger::private_reasoning_usage_total() - counted.2;
    println!("D5C ASSERTION LOG: report={report:?} usage={usage}");
    assert_eq!(
        report.memories, 0,
        "the refused settle rolled the write back: {report:?}"
    );
    let o = observe(&mut f, evidence_id);
    assert_eq!(o.memories, 0, "no memory row survives the rollback");
    assert!(!o.run_completed, "the run's finish rolled back with it");
    assert_eq!(
        counters_since(counted),
        (0, 0),
        "(runs, outputs) of a rolled-back distill write"
    );
    assert_eq!(
        (o.ledger.input_tokens, o.ledger.output_tokens),
        (Some(D5C_USAGE.0 as i64), Some(D5C_USAGE.1 as i64)),
        "the call is finalized with the provider's usage: {:?}",
        o.ledger
    );
    assert_eq!(
        usage,
        D5C_USAGE.0 + D5C_USAGE.1,
        "input + output tokens, counted once"
    );
}

/// D5b (card TH-3, ADR-0058 D-E/D-J) — a late worker. W1 is blocked inside its provider call;
/// the admin moves W1's lease and `hard_deadline` (and its slot's `bound_until`) into the past, a
/// sweep reconciles the claim (T6), and W2 — with the SAME `lease_owner` string — claims
/// generation 2 and enters its own call. W1 is released first: its heartbeat has found the lease
/// gone, its settle is refused by the generation fence, and its call is still ledgered. Then W2
/// finishes. ⇒ one memory, two finalized ledger rows, W1 `lost_lease == 1` and
/// `heartbeat_lost == 1`, job DONE in generation 2 with attempt 2.
/// Faults: (a) drop the generation predicate in `ops.finish_derived_work_v2` ⇒ W1's settle lands
/// (W1 `lost_lease == 0`); (b) drop the generation / hard-deadline predicates in
/// `ops.renew_lease` ⇒ W1's heartbeat renews generation 2's live lease (`heartbeat_lost == 0`).
#[test]
fn d5b_a_late_worker_loses_its_lease_writes_nothing_and_its_cost_is_ledgered() {
    let Some(mut f) =
        setup_db("d5b_a_late_worker_loses_its_lease_writes_nothing_and_its_cost_is_ledgered")
    else {
        return;
    };
    let (evidence, _, _) = seed_evidence(&mut f, EVIDENCE_TEXT);
    let job = job_of(&mut f, evidence);
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let (pool_one, pool_two, sweeper) = (
        private_pool(&rt, &f),
        private_pool(&rt, &f),
        private_pool(&rt, &f),
    );
    let provider = Arc::new(GatedProvider {
        descriptor: descriptor(),
        calls: AtomicU32::new(0),
        gate_one: AtomicBool::new(false),
        gate_two: AtomicBool::new(false),
    });
    let providers = routes(&provider);
    // Lease 6 s: a heartbeat every 2 s, so W1's next renew lands after generation 2 exists.
    let config = dispatch_config("d5b-worker", 6.0);
    let w1_done = AtomicBool::new(false);
    let dsn = f.dsn.clone();
    let (w1, w2) = rt.block_on(async {
        let w1 = async {
            let r = dispatch_pass(&pool_one, &providers, contribution_config(), &config).await;
            w1_done.store(true, Ordering::SeqCst);
            r
        };
        let controller = async {
            while provider.calls.load(Ordering::SeqCst) < 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            admin_exec(
                &dsn,
                "WITH j AS (UPDATE ops.jobs SET lease_expires_at = clock_timestamp() - interval '2 seconds', \
                                hard_deadline = clock_timestamp() - interval '1 second' \
                            WHERE job_id = $1 RETURNING job_id) \
                 UPDATE ops.provider_slots SET bound_until = clock_timestamp() - interval '1 second' \
                 WHERE job_id IN (SELECT job_id FROM j)",
                job,
            );
            let swept = jobs::claim_distill(&sweeper, "d5b-sweeper", 6.0, 22.0)
                .await
                .expect("sweep");
            assert!(swept.is_none(), "T6 re-queued the job with a backoff");
            admin_exec(
                &dsn,
                "UPDATE ops.jobs SET next_retry_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
                job,
            );
            let w2 = dispatch_pass(&pool_two, &providers, contribution_config(), &config);
            let release = async {
                while provider.calls.load(Ordering::SeqCst) < 2 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                // Two of W1's heartbeat periods while generation 2 holds the job.
                tokio::time::sleep(Duration::from_millis(4500)).await;
                provider.gate_one.store(true, Ordering::SeqCst);
                while !w1_done.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                provider.gate_two.store(true, Ordering::SeqCst);
            };
            tokio::join!(w2, release).0
        };
        tokio::join!(w1, controller)
    });
    let (w1, w2) = (w1.expect("W1 pass"), w2.expect("W2 pass"));
    println!("D5B ASSERTION LOG: w1={w1:?} w2={w2:?}");
    assert_eq!((w1.lost_lease, w1.heartbeat_lost), (1, 1), "W1: {w1:?}");
    assert_eq!(w1.memories, 0, "W1 wrote nothing: {w1:?}");
    assert_eq!((w2.completed, w2.memories), (1, 1), "W2: {w2:?}");
    let o = observe(&mut f, evidence);
    assert_eq!(o.memories, 1, "exactly one memory set");
    assert_eq!(o.outbox_status, "DONE");
    assert_eq!(
        o.ledger.rows, 2,
        "the late call's cost is still ledgered: {:?}",
        o.ledger
    );
    let finalized: i64 = f
        .admin
        .query_one(
            "SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id = $1 AND status = 'SUCCEEDED'",
            &[&f.tenant_id],
        )
        .expect("ledger")
        .get(0);
    assert_eq!(finalized, 2, "both calls finalized");
    let (status, attempt, generation, _) = job_state(&mut f, evidence);
    assert_eq!((status.as_str(), attempt, generation), ("DONE", 2, 2));
}

// ----------------------------------------------------------------------------
// D6 — retryable failures hand the row back: a lapsed admission (the shape of running
// `--distill-once` before the lane is seeded / the health observation expired) and a provider
// 429 both leave the row PENDING with no lease, and the next healthy pass distills it — never
// FAILED, which nothing re-claims and which would fail the ticket for good (§15.7).
// ----------------------------------------------------------------------------

#[test]
fn d6_retryable_failures_hand_the_row_back_for_a_later_pass() {
    let Some(mut f) = setup_db("d6_retryable_failures_hand_the_row_back_for_a_later_pass") else {
        return;
    };
    let reply = r#"{"memories":[{"content":"Health endpoint before traffic.","memory_type":"Decision","class":"PrivateKnowledge","confidence":0.9}]}"#;
    let (evidence_id, _, _) = seed_evidence(&mut f, EVIDENCE_TEXT);
    let rt = tokio::runtime::Runtime::new().expect("rt");

    // (1) Route not admitted: the latest provider-health observation (append-only ledger,
    // newest `observed_at` wins in the 0130 resolver) says UNAVAILABLE → no admission, no
    // run row.
    observe_provider_health(&mut f, "UNAVAILABLE");
    let unadmitted = run_pass(&rt, &f, &FakeProvider::new(vec![]), "d6-worker");
    assert_eq!(unadmitted.claimed, 1, "{unadmitted:?}");
    assert_eq!(unadmitted.not_ready, 1, "{unadmitted:?}");
    assert_eq!(unadmitted.failed, 0, "{unadmitted:?}");
    assert_eq!(
        outbox_lease(&mut f, evidence_id),
        ("PENDING".to_owned(), None),
        "row handed back with the lease cleared"
    );
    assert_eq!(processing_runs(&mut f, evidence_id), (0, 0));
    observe_provider_health(&mut f, "HEALTHY");
    ready_now(&mut f, evidence_id);

    // (2) Provider 429/5xx after the fingerprint was recorded: run row stays open, row PENDING.
    let throttled = run_pass(
        &rt,
        &f,
        &FakeProvider::new(vec![FAKE_RETRY_WAIT]),
        "d6-worker",
    );
    assert_eq!(throttled.claimed, 1, "{throttled:?}");
    assert_eq!(throttled.deferred, 1, "{throttled:?}");
    assert_eq!(throttled.failed, 0, "{throttled:?}");
    assert_eq!(
        outbox_lease(&mut f, evidence_id),
        ("PENDING".to_owned(), None)
    );
    assert_eq!(
        processing_runs(&mut f, evidence_id),
        (1, 0),
        "attempt marker: one run row, completed_at NULL"
    );

    // (3) The next healthy pass (after the RETRY backoff) distills it exactly once.
    ready_now(&mut f, evidence_id);
    let recovered = run_pass(&rt, &f, &FakeProvider::new(vec![reply]), "d6-worker");
    assert_eq!(recovered.claimed, 1, "{recovered:?}");
    assert_eq!(recovered.completed, 1, "{recovered:?}");
    let o = observe(&mut f, evidence_id);
    assert_eq!(o.memories, 1);
    assert_eq!(o.outbox_status, "DONE");
    assert_eq!(processing_runs(&mut f, evidence_id), (2, 1));
    let after = run_pass(&rt, &f, &FakeProvider::new(vec![]), "d6-worker-2");
    assert_eq!(after.claimed, 0, "{after:?}");
    println!(
        "D6 ASSERTION LOG: unadmitted={unadmitted:?} throttled={throttled:?} recovered={recovered:?} after={after:?}"
    );
}

// ----------------------------------------------------------------------------
// D7 — §6.1.3 / ADR-0028 (card 8): a remember-time subject declaration on the Evidence reaches
// the Distill-born memory through the ONE shared hook inside `distill_repo::insert_memory`
// (rule 3a: the PRIMARY Evidence's declaration → INHERITED, 10000 bp, ABOUT — the Evidence row
// keeps whether it was declared by id or by key), and every mention span indexes back into the
// exact stored revision. The Distill hop itself stays deterministic on this axis: nothing in the
// model output is consulted for linking.
// ----------------------------------------------------------------------------

/// Registers one Person (declared by id) and one Organisation (declared by exact CRM key) under
/// the fixture tenant and records both declarations on `evidence_id` the way the gateway's
/// `remember.put` does (`private.evidence_subjects` rows under `role_gateway`, in the Evidence's
/// own transaction — `adapters::remember::remember_in_txn`). Returns `(person, org)`.
fn seed_subject_declaration(f: &mut Fixture, evidence_id: Uuid) -> (Uuid, Uuid) {
    let person: Uuid = f
        .admin
        .query_one(
            "INSERT INTO private.subjects (tenant_id, kind, display_name) \
             VALUES ($1, 'PERSON', 'Ada Lovelace') RETURNING subject_id",
            &[&f.tenant_id],
        )
        .expect("seed person")
        .get(0);
    let org: Uuid = f
        .admin
        .query_one(
            "INSERT INTO private.subjects (tenant_id, kind, display_name) \
             VALUES ($1, 'ORGANISATION', 'Analytical Engines Ltd') RETURNING subject_id",
            &[&f.tenant_id],
        )
        .expect("seed org")
        .get(0);
    f.admin
        .execute(
            "INSERT INTO private.subject_keys (tenant_id, subject_id, key_kind, key_value) \
             VALUES ($1, $2, 'CRM', 'CRM-1001')",
            &[&f.tenant_id, &org],
        )
        .expect("seed crm key");
    // The gateway resolves the key to the org id under RLS before declaring; the declaration
    // itself carries the rule that produced it.
    let mut txn = f.admin.transaction().expect("begin declare");
    txn.batch_execute(&format!(
        // dep: PostgreSQL(role_gateway) — role-scoped pool call
        "SET LOCAL ROLE role_gateway; SET LOCAL humaux.tenant_id = '{}'; SET LOCAL humaux.user_id = '{}';",
        f.tenant_id, f.user_id
    ))
    .expect("gateway context");
    let written = txn
        .execute(
            "INSERT INTO private.evidence_subjects (tenant_id, evidence_id, subject_id, source_kind) \
             VALUES ($1, $2, $3, 'DECLARED'), ($1, $2, $4, 'EXTERNAL_KEY')",
            &[&f.tenant_id, &evidence_id, &person, &org],
        )
        .expect("declare under role_gateway");
    assert_eq!(written, 2, "two declaration rows on the Evidence");
    txn.commit().expect("commit declare");
    (person, org)
}

#[test]
fn d7_declared_subjects_reach_the_distilled_memory_with_spans() {
    let Some(mut f) = setup_db("d7_declared_subjects_reach_the_distilled_memory_with_spans") else {
        return;
    };
    let (evidence_id, _, _) = seed_evidence(
        &mut f,
        "Ada Lovelace (account CRM-1001) wants the renewal moved to Q4.",
    );
    let (person, org) = seed_subject_declaration(&mut f, evidence_id);
    let rt = tokio::runtime::Runtime::new().expect("rt");
    // The model output names no subject at all — linking must come from the declaration.
    let provider = FakeProvider::new(vec![
        r#"{"memories":[{"content":"Ada Lovelace wants the CRM-1001 renewal moved to Q4.","memory_type":"Decision","class":"PrivateKnowledge","confidence":0.9}]}"#,
    ]);
    let report = run_pass(&rt, &f, &provider, "d7-worker");
    assert_eq!(report.completed, 1, "{report:?}");
    assert_eq!(report.memories, 1, "{report:?}");
    let o = observe(&mut f, evidence_id);
    assert_eq!(o.memories, 1);

    // FAULT SENTINEL: exact link count per source_kind goes red if the hook call is dropped from
    // `distill_repo::insert_memory` (0 rows) or the declaration is not carried as INHERITED.
    let mut rows = f
        .admin
        .query(
            "SELECT ms.subject_id, ms.relation, ms.source_kind, ms.confidence_bp \
             FROM private.memory_subjects ms \
             JOIN private.memory_evidence me ON me.memory_id = ms.memory_id \
             WHERE me.evidence_id = $1",
            &[&evidence_id],
        )
        .expect("read links")
        .into_iter()
        .map(|r| {
            (
                r.get::<_, Uuid>(0),
                r.get::<_, String>(1),
                r.get::<_, String>(2),
                r.get::<_, i16>(3),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    let mut expected = vec![
        (person, "ABOUT".to_owned(), "INHERITED".to_owned(), 10_000),
        (org, "ABOUT".to_owned(), "INHERITED".to_owned(), 10_000),
    ];
    expected.sort();
    assert_eq!(
        rows, expected,
        "the distilled memory inherits both Evidence declarations (§6.1.3 rule 3 → INHERITED)"
    );

    // Mention spans: each subject's display_name / key value located in the exact stored
    // revision bytes, sha256-bound to that revision.
    let mentions = f
        .admin
        .query(
            "SELECT ms.subject_id, \
                    convert_from(substring(convert_to(m.content::text,'UTF8') \
                        FROM ms.span_start + 1 FOR ms.span_end - ms.span_start), 'UTF8'), \
                    ms.revision_sha256 = sha256(convert_to(m.content::text,'UTF8')) \
             FROM private.memory_subject_mentions ms \
             JOIN private.memory_records m ON m.memory_id = ms.memory_id \
             JOIN private.memory_evidence me ON me.memory_id = ms.memory_id \
             WHERE me.evidence_id = $1",
            &[&evidence_id],
        )
        .expect("read mentions")
        .into_iter()
        .map(|r| {
            (
                r.get::<_, Uuid>(0),
                r.get::<_, String>(1),
                r.get::<_, bool>(2),
            )
        })
        .collect::<Vec<_>>();
    assert!(
        mentions
            .iter()
            .any(|(id, text, bound)| *id == person && text == "Ada Lovelace" && *bound),
        "person span indexes the stored revision: {mentions:?}"
    );
    assert!(
        mentions
            .iter()
            .any(|(id, text, bound)| *id == org && text == "CRM-1001" && *bound),
        "org key span indexes the stored revision: {mentions:?}"
    );
    println!(
        "D7 ASSERTION LOG: report={report:?} links={rows:?} mentions={}",
        mentions.len()
    );
}

// ----------------------------------------------------------------------------
// D8 / D9 — card 32 slice 3 (ADR-0058 D-M / D-P).
// ----------------------------------------------------------------------------

/// A transport that answers with scripted chat envelopes and records every request body, so the
/// REAL `OpenAiCompatibleProvider` (tool body + tool-call parse) sits between the worker and it.
struct ScriptedTransport {
    bodies: Mutex<Vec<&'static str>>,
    sent: Arc<Mutex<Vec<serde_json::Value>>>,
}

#[async_trait]
impl OpenAiCompatTransport for ScriptedTransport {
    async fn send(
        &self,
        request: OpenAiHttpRequest,
        _policy: &ssrf::CustomEndpointPolicy,
    ) -> Result<OpenAiHttpOutcome, ReasoningProviderError> {
        self.sent
            .lock()
            .expect("sent")
            .push(serde_json::from_slice(&request.body).expect("the request body is JSON"));
        let body = self
            .bodies
            .lock()
            .expect("bodies")
            .pop()
            .expect("scripted transport has a reply for every send");
        Ok(OpenAiHttpOutcome {
            status: 200,
            retry_after: None,
            body: body.as_bytes().to_vec(),
        })
    }
}

/// The bodies this provider's transport was sent, in order.
type Sent = Arc<Mutex<Vec<serde_json::Value>>>;

fn scripted_provider(
    bodies: Vec<&'static str>,
) -> (
    Arc<OpenAiCompatibleProvider<ScriptedTransport, EnvKeyDecryptor>>,
    Sent,
) {
    let sent = Sent::default();
    // No socket is ever opened: the transport is scripted. The pin only lets the §11.4 choke
    // point accept the real endpoint string the admission lane names.
    let resolver = ssrf::PinnedDnsResolver::parse("api.minimaxi.com=93.184.216.34").expect("pin");
    let provider = OpenAiCompatibleProvider::new(
        descriptor(),
        MINIMAX_CHAT_URL.to_string(),
        ScriptedTransport {
            bodies: Mutex::new(bodies.into_iter().rev().collect()),
            sent: Arc::clone(&sent),
        },
        EnvKeyDecryptor {
            key_material: "scripted-not-a-key".to_owned(),
        },
        ssrf::CustomEndpointPolicy::default(),
        &resolver,
    )
    .expect("the endpoint passes the SSRF choke point");
    (Arc::new(provider), sent)
}

const TOOL_OVER_CEILING: &str = r#"{"choices":[{"finish_reason":"tool_calls","message":{"role":"assistant","tool_calls":[{"id":"c1","type":"function","function":{"name":"emit_distillation","arguments":"{\"memories\":[{\"content\":\"Health endpoint before traffic.\",\"memory_type\":\"Decision\",\"class\":\"ProjectConstraint\",\"confidence\":0.9}]}"}}]}}],"usage":{"prompt_tokens":10,"completion_tokens":5},"base_resp":{"status_code":0,"status_msg":""}}"#;
const TOOL_TWO_CALLS: &str = r#"{"choices":[{"finish_reason":"tool_calls","message":{"role":"assistant","tool_calls":[{"id":"c1","type":"function","function":{"name":"emit_distillation","arguments":"{\"memories\":[{\"content\":\"Health endpoint before traffic.\",\"memory_type\":\"Decision\",\"class\":\"PrivateKnowledge\",\"confidence\":0.9}]}"}},{"id":"c2","type":"function","function":{"name":"emit_distillation","arguments":"{\"memories\":[]}"}}]}}],"base_resp":{"status_code":0,"status_msg":""}}"#;

/// ADR-0058 D-M: the tool call is a transport, never the validation. (a) One well-formed call
/// whose arguments carry an over-ceiling class still meets §10.1's authorize — rejected PENDING,
/// never downgraded (d2's rule, through the real provider). (b) A reply with two calls is a
/// schema failure that spends the malformed re-ask (ADR-0048 D-D) and then fails closed: DEAD
/// `FAILED_OUTPUT_SCHEMA`, outbox FAILED, nothing written. Fault: accept the first of two tool
/// calls ⇒ (b) writes a memory.
#[test]
fn d8_tool_call_reply_goes_through_the_fail_closed_parser() {
    let Some(mut f) = setup_db("d8_tool_call_reply_goes_through_the_fail_closed_parser") else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().expect("rt");

    let (over, _, _) = seed_evidence(&mut f, EVIDENCE_TEXT);
    let (provider, sent) = scripted_provider(vec![TOOL_OVER_CEILING]);
    let report = run_pass(&rt, &f, &provider, "d8-worker-a");
    assert_eq!(report.rejected, 1, "{report:?}");
    let o = observe(&mut f, over);
    assert_eq!(
        o.memories, 0,
        "an over-ceiling class is rejected, never downgraded"
    );
    assert_eq!(o.outbox_status, "DONE");
    let requested: String = f
        .admin
        .query_one(
            "SELECT requested_class FROM private.distill_candidates WHERE source_evidence_id = $1",
            &[&over],
        )
        .expect("one PENDING candidate")
        .get(0);
    assert_eq!(requested, "ProjectConstraint");
    let sent = sent.lock().expect("sent").clone();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["tools"][0]["function"]["name"], "emit_distillation");
    // ADR-0060 research amendment 1: a vendor field comes only from the Profile's request_extras
    // (none here); REASONING_SPLIT selects nothing.
    assert!(sent[0].get("reasoning_split").is_none());
    assert!(sent[0].get("tool_choice").is_none(), "W1: never sent");

    let (two, _, _) = seed_evidence(&mut f, "two tool calls evidence");
    let (provider, sent) = scripted_provider(vec![TOOL_TWO_CALLS, TOOL_TWO_CALLS]);
    let report = run_pass(&rt, &f, &provider, "d8-worker-b");
    assert_eq!(
        sent.lock().expect("sent").len(),
        2,
        "one counted re-ask: {report:?}"
    );
    assert_eq!(report.malformed_retries, 1, "{report:?}");
    assert_eq!(report.memories, 0, "{report:?}");
    let (status, attempt, _, class) = job_state(&mut f, two);
    assert_eq!(
        (status.as_str(), attempt, class.as_deref()),
        ("DEAD", 2, Some("FAILED_OUTPUT_SCHEMA"))
    );
    let o = observe(&mut f, two);
    assert_eq!(o.memories, 0, "the first of two calls is never taken");
    assert_eq!(o.outbox_status, "FAILED");
    assert_eq!(
        o.ledger.status.as_deref(),
        Some("FAILED"),
        "the refused reply is a ledgered failed call"
    );
    println!(
        "D8 ASSERTION LOG: report={report:?} memories={} outbox={}",
        o.memories, o.outbox_status
    );
}

/// `(origin, label, confidence_bp)` of every affect row of the memories born from `evidence_id`.
fn affect_rows_of(f: &mut Fixture, evidence_id: Uuid) -> Vec<(String, Option<String>, i16)> {
    f.admin
        .query(
            "SELECT a.origin, a.label, a.confidence_bp FROM private.memory_affects a \
             JOIN private.memory_evidence me ON me.memory_id = a.memory_id \
             WHERE me.evidence_id = $1 ORDER BY a.created_at, a.affect_id",
            &[&evidence_id],
        )
        .expect("affect rows")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

/// ADR-0058 D-P (amended by the card-32 review): an Evidence of the gateway's own ingress origin
/// (`remember.put` stamps `AuthenticatedAgent`) is offered the affect menu, its inferred EMOTION is
/// written as `origin = 'DISTILL'` within the confidence ceiling, and the recall final gate's affect
/// re-check (`affect_repo::affects_for_memories` under role_gateway + `memories_matching`, the
/// composition `read_materialize::final_memory_ids_about_in_txn` runs) selects its memory by that
/// inferred label. An Evidence that DECLARED an affect is offered no menu, and its memory carries
/// only the declared (EXPLICIT) row the 0157 trigger copied. Faults: write inferred rows through
/// `insert_in_txn` (EXPLICIT); offer the menu to user origins only (the reply's `affects` is then an
/// extra key and the hop writes no inferred row).
#[test]
fn d9_inferred_affect_rows_carry_origin_distill_and_explicit_affects_suppress_them() {
    let Some(mut f) =
        setup_db("d9_inferred_affect_rows_carry_origin_distill_and_explicit_affects_suppress_them")
    else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let (inferred, _, _) = seed_evidence_as(
        &mut f,
        "Finally shipped the release, what a relief.",
        "AuthenticatedAgent",
    );
    let provider = FakeProvider::new(vec![
        r#"{"memories":[{"content":"The release shipped.","memory_type":"State","class":"PrivateKnowledge","confidence":0.9,"affects":[{"kind":"EMOTION","label":"RELIEF","valence":7000,"arousal":-2000,"intensity":6000,"confidence":4500}]}]}"#,
    ]);
    let report = run_pass(&rt, &f, &provider, "d9-worker-a");
    assert_eq!(report.memories, 1, "{report:?}");
    assert!(
        provider.schemas()[0].contains("\"affects\""),
        "an AuthenticatedAgent (remember.put) Evidence is offered the affect menu"
    );
    assert_eq!(
        affect_rows_of(&mut f, inferred),
        vec![("DISTILL".to_owned(), Some("RELIEF".to_owned()), 4500)]
    );
    let memory: Uuid = f
        .admin
        .query_one(
            "SELECT memory_id FROM private.memory_evidence WHERE evidence_id = $1",
            &[&inferred],
        )
        .expect("the inferred row's memory")
        .get(0);
    let gateway = rt
        // dep: PostgreSQL(role_gateway) — the recall path's role-scoped pool
        .block_on(RuntimeDbPool::connect(&dsn_as_role(&f.dsn, "role_gateway")))
        .expect("gateway pool");
    let auth = AuthorizationScope::new(
        TenantId(f.tenant_id),
        PrincipalId::new(),
        Some(UserId(f.user_id)),
        BoundedSet::new([WorkspaceId(f.workspace_id)]).expect("one workspace"),
    );
    let rows = rt
        .block_on(affect_repo::affects_for_memories(
            &gateway,
            &auth,
            &[memory],
        ))
        .expect("the recall gate's one affect read");
    // An EMOTION does not decay (ADR-0030 D-B): its effective intensity is the stored one.
    let observed: Vec<ObservedAffect> = rows
        .iter()
        .map(|row| ObservedAffect {
            memory_id: row.memory_id,
            annotation: row.annotation.clone(),
            effective_intensity: row.annotation.intensity,
        })
        .collect();
    let relief = AffectFilter {
        labels_any: vec![EmotionLabel::Relief],
        ..AffectFilter::default()
    };
    assert!(
        memories_matching(&relief, &observed).contains(&memory),
        "an affect-filtered recall selects the memory by its inferred (DISTILL) row"
    );

    let (declared, _, _) = seed_evidence_as(
        &mut f,
        "This flaky build again, I am furious.",
        "DirectUserInput",
    );
    f.admin
        .execute(
            "INSERT INTO private.evidence_affects \
               (tenant_id, affect_kind, label, valence_bp, intensity_bp, confidence_bp, evidence_id, observed_at) \
             VALUES ($1, 'EMOTION', 'FRUSTRATION', -8000, 9000, 9000, $2, now())",
            &[&f.tenant_id, &declared],
        )
        .expect("declare an affect on the Evidence (remember.put's carrier)");
    let provider = FakeProvider::new(vec![
        r#"{"memories":[{"content":"The build is flaky.","memory_type":"Issue","class":"PrivateKnowledge","confidence":0.9}]}"#,
    ]);
    let report = run_pass(&rt, &f, &provider, "d9-worker-b");
    assert_eq!(report.memories, 1, "{report:?}");
    assert!(
        !provider.schemas()[0].contains("\"affects\""),
        "an Evidence with a declared affect is offered no inference menu"
    );
    assert_eq!(
        affect_rows_of(&mut f, declared),
        vec![("EXPLICIT".to_owned(), Some("FRUSTRATION".to_owned()), 9000)],
        "only the declared row, copied by the PRIMARY trigger"
    );
    println!("D9 ASSERTION LOG: report={report:?}");
}

/// ADR-0058 R1: an inferred affect over the ceiling costs the Evidence nothing but its inferred
/// affects — the reply is accepted, the memory written with zero `origin = 'DISTILL'` rows, the
/// job DONE on its first call, nothing re-asked, `affects_dropped = 1`. Fault: treat the dropped
/// affects as a malformed reply → the hop re-asks (attempt 2, `malformed_retries = 1`) and, the
/// re-ask carrying the same affect, the job is DEAD with no memory.
#[test]
fn d10_an_invalid_inferred_affect_drops_the_affects_and_keeps_the_memory() {
    let Some(mut f) =
        setup_db("d10_an_invalid_inferred_affect_drops_the_affects_and_keeps_the_memory")
    else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let (evidence, _, _) = seed_evidence_as(
        &mut f,
        "Finally shipped the release, what a relief.",
        "AuthenticatedAgent",
    );
    const OVER_CEILING: &str = r#"{"memories":[{"content":"The release shipped.","memory_type":"State","class":"PrivateKnowledge","confidence":0.9,"affects":[{"kind":"EMOTION","label":"RELIEF","intensity":6000,"confidence":9000}]}]}"#;
    let provider = FakeProvider::new(vec![OVER_CEILING, OVER_CEILING]);
    let report = run_pass(&rt, &f, &provider, "d10-worker");
    assert_eq!(
        (
            report.memories,
            report.affects_dropped,
            report.malformed_retries,
            report.attempts
        ),
        (1, 1, 0, 1),
        "{report:?}"
    );
    assert!(
        report.summary_line().contains(" affects_dropped=1"),
        "{report:?}"
    );
    let (status, attempt, _, _) = job_state(&mut f, evidence);
    assert_eq!((status.as_str(), attempt), ("DONE", 1));
    let o = observe(&mut f, evidence);
    assert_eq!((o.memories, o.outbox_status.as_str()), (1, "DONE"));
    assert!(
        affect_rows_of(&mut f, evidence).is_empty(),
        "nothing invalid is stored and nothing is clamped"
    );
    println!("D10 ASSERTION LOG: report={report:?}");
}

/// The Evidence's successor tickets (`MEMORY_LIFECYCLE` carriers), `(stream_seq, state,
/// error_class)` in stream order.
fn successor_tickets(f: &mut Fixture, evidence_id: Uuid) -> Vec<(i64, String, Option<String>)> {
    f.admin
        .query(
            "SELECT sl.stream_seq, sl.state, sl.error_class FROM ops.outbox o \
             JOIN projection.stream_log sl ON sl.tenant_id = o.tenant_id AND sl.commit_seq = o.commit_seq \
             WHERE o.tenant_id = $1 AND o.evidence_id = $2 AND o.event_type = 'MEMORY_LIFECYCLE' \
             ORDER BY sl.stream_seq",
            &[&f.tenant_id, &evidence_id],
        )
        .expect("successor tickets")
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

/// ADR-0058 D-U (review P0 on ruling R4): a DEAD Evidence whose remember ticket already settled
/// FAILED `distill_failed` is re-driven by `jobs requeue-dead`, and its re-distilled memory reaches
/// the index through the successor ticket the definer issues — which waits while the re-armed job
/// is open instead of failing. Faults: the definer issues no successor (0197's body) → red at the
/// successor count; the projection worker reads the distill state by the ticket's own commit_seq
/// (the pre-fix lookup) → red, the successor fails `no_visible_memory_record` before the re-run.
#[test]
fn d11_a_requeued_dead_evidence_is_projected_by_a_successor_ticket() {
    let test_name = "d11_a_requeued_dead_evidence_is_projected_by_a_successor_ticket";
    let Some(mut f) = setup_db(test_name) else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let (evidence, _, stream_seq) = seed_evidence(&mut f, "Health endpoint before traffic.");
    const MALFORMED: &str = r#"{"memories":[{"content":"c","memory_type":"Constraint","class":"PrivateKnowledge","confidence":0.5}]}"#;
    const VALID: &str = r#"{"memories":[{"content":"Health endpoint before traffic.","memory_type":"Decision","class":"PrivateKnowledge","confidence":0.9}]}"#;
    let provider = FakeProvider::new(vec![MALFORMED, MALFORMED, VALID]);

    let report = run_pass(&rt, &f, &provider, "d11-worker");
    assert_eq!(report.dead, 1, "{report:?}");
    assert_eq!(observe(&mut f, evidence).outbox_status, "FAILED");
    let Some((outcome, _)) = run_projection(&rt, &f, test_name) else {
        return;
    };
    assert_eq!(outcome.failed, 1, "{outcome:?}");
    assert_eq!(
        ticket_state(&mut f, stream_seq),
        ("FAILED".to_owned(), Some("distill_failed".to_owned()))
    );

    let job = job_of(&mut f, evidence);
    let receipt = rt
        .block_on(async {
            // dep: PostgreSQL(role_maintenance) — the operator pool of `jobs requeue-dead`
            let pool = MaintenanceDbPool::connect(&dsn_as_role(&f.dsn, "role_maintenance"))
                .await
                .expect("maintenance pool");
            provisioning::requeue_dead_distill(
                &pool,
                f.tenant_id,
                RequeueTarget::Job(job),
                &AdminAction {
                    actor: "c32-d11-test",
                    reason: "card 32 D-U successor ticket test",
                    ticket: "T-32-DU",
                    trace_id: "c32-d11-trace",
                    step_up_auth_context: "test-mfa",
                },
            )
            .await
        })
        .expect("requeue-dead");
    assert_eq!(receipt.requeued.len(), 1, "{receipt:?}");
    let successors = successor_tickets(&mut f, evidence);
    assert_eq!(successors.len(), 1, "one successor ticket: {successors:?}");
    let (successor, state, _) = successors[0].clone();
    assert!(successor > stream_seq, "issued after the remember ticket");
    assert_eq!(state, "ISSUED");
    assert_eq!(
        ticket_state(&mut f, stream_seq).0,
        "FAILED",
        "the old row is left for the operator's audited retirement"
    );

    let Some((waiting, _)) = run_projection(&rt, &f, test_name) else {
        return;
    };
    assert_eq!((waiting.pending, waiting.failed), (1, 0), "{waiting:?}");
    assert_eq!(
        ticket_state(&mut f, successor),
        ("ISSUED".to_owned(), None),
        "the successor waits for the re-armed job"
    );

    let rerun = run_pass(&rt, &f, &provider, "d11-worker");
    assert_eq!((rerun.completed, rerun.memories), (1, 1), "{rerun:?}");
    let Some((indexed, upserts)) = run_projection(&rt, &f, test_name) else {
        return;
    };
    assert_eq!(
        upserts, 1,
        "the re-distilled memory reaches the index: {indexed:?}"
    );
    assert_eq!(ticket_state(&mut f, successor).0, "DONE");
    println!(
        "D11 ASSERTION LOG: dead={report:?} successor={successor} waiting={waiting:?} rerun={rerun:?} indexed={indexed:?} upserts={upserts}"
    );
}

/// A tool-channel reply with NO tool call whose `content` (reasoning block included) carries a
/// valid answer object (ADR-0058 R9).
const TOOL_CHANNEL_CONTENT_ANSWER: &str = r#"{"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"<think>one decision</think>{\"memories\":[{\"content\":\"Health endpoint before traffic.\",\"memory_type\":\"Decision\",\"class\":\"PrivateKnowledge\",\"confidence\":0.9}]}"}}],"usage":{"prompt_tokens":10,"completion_tokens":5},"base_resp":{"status_code":0,"status_msg":""}}"#;

/// ADR-0058 R9 (main-line ruling after chain run 2): on the tool channel, a reply with no tool call
/// whose `content` is a valid answer object goes through the same ADR-0048 parser and is accepted
/// on the first call — no re-ask, the memory written, `channel_fallback=1` on the dispatch line.
/// Fault: no fallback (zero tool calls is always a schema failure) → the hop re-asks
/// (`malformed_retries=1`, two sends) and the job dies DEAD `FAILED_OUTPUT_SCHEMA`.
#[test]
fn d12_a_tool_channel_reply_in_content_is_parsed_and_accepted_once() {
    let Some(mut f) = setup_db("d12_a_tool_channel_reply_in_content_is_parsed_and_accepted_once")
    else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let (evidence, _, _) = seed_evidence(&mut f, EVIDENCE_TEXT);
    let (provider, sent) = scripted_provider(vec![
        TOOL_CHANNEL_CONTENT_ANSWER,
        TOOL_CHANNEL_CONTENT_ANSWER,
    ]);
    let report = run_pass(&rt, &f, &provider, "d12-worker");
    let sends = sent.lock().expect("sent").len();
    let (status, attempt, _, class) = job_state(&mut f, evidence);
    let o = observe(&mut f, evidence);
    println!(
        "D12 ASSERTION LOG: sends={sends} status={status} attempt={attempt} class={class:?} memories={} outbox={} line={}",
        o.memories,
        o.outbox_status,
        report.summary_line()
    );
    assert_eq!(sends, 1, "accepted on the first call, never re-asked");
    assert_eq!(
        (report.malformed_retries, report.channel_fallback),
        (0, 1),
        "{report:?}"
    );
    assert!(
        report.summary_line().contains(" channel_fallback=1 "),
        "{}",
        report.summary_line()
    );
    assert_eq!((status.as_str(), attempt), ("DONE", 1));
    assert_eq!(o.memories, 1);
    assert_eq!(o.outbox_status, "DONE");
}
