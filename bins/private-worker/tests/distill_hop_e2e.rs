//! ADR-0016 Distill hop — accepted Evidence → 0..N `private.memory_records` by the private
//! worker itself (`humaux_private_worker::distill::run_once`, the `--distill-once` code path),
//! then the remember-time `projection.stream_log` ticket resolved by the real
//! `humaux_adapters::projection_worker::run_once`.
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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use humaux_adapters::byok::{
    CredentialDecryptor, CredentialRef, EgressHttpTransport, OpenAiCompatibleProvider,
    PlaintextApiKey, PrivateInferenceContext, ReasoningCapability, ReasoningProviderDescriptor,
    ReasoningProviderError, StructuredReasoningRequest, StructuredReasoningResponse, TokenUsage,
    UserReasoningProvider, VisionReasoningRequest, VisionReasoningResponse, ssrf,
};
use humaux_adapters::contribution_reasoner::ContributionReasonerConfig;
use humaux_adapters::disclosure::DeletionCapability;
use humaux_adapters::distill_reasoner::{DISTILL_PARSER_VERSION, distill_prompt_contract};
use humaux_adapters::postgres::{PrivateWorkerDbPool, RetrievalWorkerDbPool};
use humaux_adapters::projection_worker::{CardEmbedder, ProjectionWorkerDeps, RunOnceOutcome};
use humaux_adapters::qdrant::{
    PlacementClass, PromotionState, RetrievalFamily, TenantPlacementRow,
};
use humaux_domain::egress::ProcessorId;
use humaux_domain::error::ErrorCode;
use humaux_domain::evidence::payload_sha256;
use humaux_domain::ids::TenantId;
use humaux_infra_cell::{
    CallerId, CellAccessPermit, CellId, IntraCellError, IntraCellHttpTransport, IntraCellRequest,
    IntraCellResource, IntraCellResourceRegistry, IntraCellResponse, ResourceEntry,
    authorize_cell_access,
};
use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig, SealedRetrievalCard};
use humaux_private_worker::distill::{DistillConfig, DistillPassReport, run_once};
use humaux_projection::fingerprint::{ProcessingInputFingerprintInputs, source_hash};
use humaux_projection::serving::StreamFamily;
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const NAME: &str = "distill_hop_e2e";
const MINIMAX_CHAT_URL: &str = "https://api.minimaxi.com/v1/chat/completions";
const MINIMAX_MODEL: &str = "MiniMax-M3";
const EGRESS_PROCESSOR_ID: Uuid = Uuid::from_u128(0x2016);
const REGION: &str = "cn-shanghai";
const SERVICE_TIER: &str = "standard";
const PURPOSE_DB: &str = "PRIVATE_DISTILL_TEXT";
const EVIDENCE_TEXT: &str =
    "New backend services must expose a health endpoint before any traffic is routed to them.";
/// The remember-side stream identity (`consolidate_repo::publish_rollup` / `projection_worker`
/// module doc): workspace-scoped `private_memory` / `PRIVATE_MEMORY` / `v1`.
const STREAM_DOMAIN: &str = "private_memory";
const STREAM_PROJECTION_KIND: &str = "PRIVATE_MEMORY";
const STREAM_PROJECTION_VERSION: &str = "v1";
const DIMENSION: u32 = 4;

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

/// Same env + `/Volumes/data/viral-skill-eval/.env` fallback `minimax_live_smoke.rs` uses.
/// Zero println/panic-message exposure of the value.
fn load_minimax_key() -> Option<String> {
    if let Some(v) = std::env::var("MINIMAX_API_KEY")
        .ok()
        .filter(|v| !v.is_empty())
    {
        return Some(v);
    }
    let raw = std::fs::read_to_string("/Volumes/data/viral-skill-eval/.env").ok()?;
    for line in raw.lines() {
        let line = line.trim().strip_prefix("export ").unwrap_or(line.trim());
        if let Some(v) = line.strip_prefix("MINIMAX_API_KEY=") {
            let v = v.trim().trim_matches('"').trim_matches('\'');
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

struct EnvKeyDecryptor {
    key_material: String,
}

#[async_trait]
impl CredentialDecryptor for EnvKeyDecryptor {
    async fn resolve(
        &self,
        _credential_ref: CredentialRef,
    ) -> Result<PlaintextApiKey, ReasoningProviderError> {
        Ok(PlaintextApiKey::new(self.key_material.clone()))
    }
}

/// Same fake-IP-proxy environment fact `minimax_live_smoke.rs::PinnedPublicResolver` documents.
struct PinnedPublicResolver;
impl ssrf::DnsResolver for PinnedPublicResolver {
    fn resolve(&self, _host: &str) -> Result<Vec<std::net::IpAddr>, ssrf::SsrfError> {
        Ok(vec![std::net::IpAddr::V4(std::net::Ipv4Addr::new(
            93, 184, 216, 34,
        ))])
    }
}

fn descriptor() -> ReasoningProviderDescriptor {
    ReasoningProviderDescriptor {
        provider_id: "minimax".to_string(),
        model_id: MINIMAX_MODEL.to_string(),
        model_revision: None,
        capabilities: vec![ReasoningCapability::StructuredOutput],
        custom_endpoint: Some(MINIMAX_CHAT_URL.to_string()),
    }
}

fn contribution_config() -> ContributionReasonerConfig {
    ContributionReasonerConfig {
        allowed_egress_processor_id: ProcessorId(EGRESS_PROCESSOR_ID),
        region: REGION.to_string(),
        permit_ttl: Duration::from_secs(30),
        deletion_capability: DeletionCapability::Unknown,
        // Contribution-path prompt config only; Distill takes prompt/schema/budget from
        // `distill_prompt_contract()` and ignores these three.
        system_prompt: "s".to_string(),
        json_schema: "{}".to_string(),
        max_output_tokens: 64,
    }
}

fn live_provider(key: String) -> OpenAiCompatibleProvider<EgressHttpTransport, EnvKeyDecryptor> {
    OpenAiCompatibleProvider::new(
        descriptor(),
        MINIMAX_CHAT_URL.to_string(),
        EgressHttpTransport::new(Duration::from_secs(120)).expect("transport"),
        EnvKeyDecryptor { key_material: key },
        ssrf::CustomEndpointPolicy::default(),
        &PinnedPublicResolver,
    )
    .expect("SSRF choke point must accept the endpoint (resolver pinned)")
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
}

impl FakeProvider {
    fn new(replies: Vec<&str>) -> Self {
        Self {
            descriptor: descriptor(),
            replies: Mutex::new(replies.into_iter().rev().map(str::to_owned).collect()),
            calls: Mutex::new(0),
        }
    }

    fn calls(&self) -> u32 {
        *self.calls.lock().expect("calls")
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
        None
    }

    async fn complete_structured(
        &self,
        _context: &PrivateInferenceContext,
        _request: StructuredReasoningRequest,
    ) -> Result<StructuredReasoningResponse, ReasoningProviderError> {
        *self.calls.lock().expect("calls") += 1;
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
    dsn: String,
    tenant_id: Uuid,
    user_id: Uuid,
    reasoning_domain_id: Uuid,
    workspace_id: Uuid,
}

impl Drop for Fixture {
    /// Throwaway-tenant cleanup — same generic sweep `consolidation_hop_e2e.rs` uses, plus the
    /// child tables that key through a parent (memory_evidence, events).
    fn drop(&mut self) {
        let tenant = self.tenant_id;
        let Ok(rows) = self.admin.query(
            "SELECT table_schema, table_name FROM information_schema.columns \
             WHERE column_name = 'tenant_id' AND table_schema IN ('control','private','ops','projection','staging') \
               AND table_name <> 'tenants' \
             ORDER BY table_schema, table_name",
            &[],
        ) else {
            return;
        };
        let mut sql = String::from("SET session_replication_role = replica; ");
        sql.push_str(&format!(
            "DELETE FROM private.memory_evidence WHERE memory_id IN \
               (SELECT memory_id FROM private.memory_records WHERE tenant_id = '{tenant}'); \
             DELETE FROM private.events WHERE event_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{tenant}'); "
        ));
        for row in rows {
            let schema: String = row.get(0);
            let table: String = row.get(1);
            sql.push_str(&format!(
                "DELETE FROM {schema}.{table} WHERE tenant_id = '{tenant}'; "
            ));
        }
        sql.push_str(&format!(
            "DELETE FROM control.tenants WHERE tenant_id = '{tenant}'; \
             DELETE FROM control.users WHERE user_id = '{}'; \
             SET session_replication_role = DEFAULT;",
            self.user_id
        ));
        if let Err(error) = self.admin.batch_execute(&sql) {
            eprintln!("{NAME}: fixture cleanup for tenant {tenant} failed: {error}");
        }
    }
}

/// Seeds tenant + owner user + workspace + reasoning domain + the full R3 admission lane for
/// `PRIVATE_DISTILL_TEXT` over the MiniMax descriptor (mirrors consolidation_hop_e2e::setup_db).
#[allow(clippy::too_many_lines)]
fn setup_db(test_name: &str) -> Option<Fixture> {
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(
            test_name,
            "missing object: HUMAUX_TEST_PG_DSN",
            ExternalDep::Postgres,
        );
        return None;
    };
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
            "SELECT to_regprocedure('control.current_reasoning_route_binding(uuid,text)') IS NOT NULL",
            &[],
        )
        .ok()?
        .get(0);
    if !migrated {
        skip_or_fail(
            test_name,
            "missing object: control.current_reasoning_route_binding — run `cargo xtask migrate` (0147)",
            ExternalDep::Postgres,
        );
        return None;
    }

    let suffix = Uuid::now_v7();
    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name, state) VALUES ($1, 'ACTIVE') RETURNING tenant_id",
            &[&format!("{NAME} throwaway tenant {suffix}")],
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

    let credential: Uuid = admin
        .query_one(
            "INSERT INTO control.credentials(tenant_id,purpose,openbao_ref) VALUES($1,'USER_REASONING',$2) RETURNING credential_id",
            &[&tenant_id, &format!("openbao://{NAME}/{suffix}")],
        )
        .expect("credential locator")
        .get(0);
    let d = descriptor();
    admin
        .execute(
            "INSERT INTO control.processor_models(processor_id,provider_model_id,model_revision,capabilities,status,catalog_observed_at) \
             VALUES($1,$2,NULL,ARRAY['TEXT','STRUCTURED_OUTPUT'],'ACTIVE',clock_timestamp()) ON CONFLICT DO NOTHING",
            &[&d.provider_id, &d.model_id],
        )
        .expect("processor model");
    let processor_model_id: Uuid = admin
        .query_one(
            "SELECT processor_model_id FROM control.processor_models \
             WHERE processor_id=$1 AND provider_model_id=$2 AND model_revision IS NULL AND status='ACTIVE'",
            &[&d.provider_id, &d.model_id],
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
            "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,billing_account_id,default_billing_instrument_id,capabilities,processing_region) VALUES($1,$2,$3,$4,$5,$6,NULL,NULL,ARRAY['TEXT'],$7) RETURNING profile_id",
            &[&tenant_id, &user_id, &account, &endpoint, &processor_model_id, &credential, &REGION],
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
             VALUES($1,$2,$3,$4,NULL,$5,$6,$7,$8,'TEST',NULL,'HEALTHY',clock_timestamp()-interval '1 second',clock_timestamp()+interval '30 minutes')",
            &[&tenant_id, &d.provider_id, &processor_model_id, &d.model_id, &endpoint, &MINIMAX_CHAT_URL, &REGION, &SERVICE_TIER],
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
        dsn,
        tenant_id,
        user_id,
        reasoning_domain_id,
        workspace_id,
    })
}

/// One accepted Evidence exactly as `remember::remember_in_txn` writes it (see module doc):
/// AuthenticatedAgent origin, WORKSPACE_SHARED in the fixture workspace, `USER_MESSAGE` event
/// whose payload is `{"text": <text>}`. Returns `(evidence_id, commit_seq, stream_seq)`.
fn seed_evidence(f: &mut Fixture, text: &str) -> (Uuid, i64, i64) {
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
             VALUES ($1, 'EVENT', decode($2, 'hex'), 'PRIVATE', 'AuthenticatedAgent', $3, \
                     'WORKSPACE_SHARED', $4, $5, now()) \
             RETURNING evidence_id",
            &[
                &f.tenant_id,
                &digest.to_hex(),
                &f.user_id,
                &f.workspace_id,
                &f.reasoning_domain_id,
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

fn distill_config(f: &Fixture, lease_owner: &str) -> DistillConfig {
    DistillConfig {
        tenant_id: f.tenant_id,
        reasoning_domain_id: f.reasoning_domain_id,
        batch: 10,
        lease_seconds: 120.0,
        lease_owner: lease_owner.to_owned(),
    }
}

fn run_pass(
    rt: &tokio::runtime::Runtime,
    f: &Fixture,
    provider: &dyn UserReasoningProvider,
    lease_owner: &str,
) -> DistillPassReport {
    rt.block_on(async {
        let pool = PrivateWorkerDbPool::connect(&dsn_as_role(&f.dsn, "role_private_worker"))
            .await
            .expect("private worker pool");
        run_once(
            &pool,
            provider,
            contribution_config(),
            &distill_config(f, lease_owner),
        )
        .await
        .expect("distill pass")
    })
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
               (SELECT count(*) FROM ops.private_inference_rpc_calls c WHERE c.tenant_id = $2) AS rpc_calls",
            &[&evidence_id, &f.tenant_id],
        )
        .expect("observe");
    let memories: i64 = row.get("memories");
    let primary_links: i64 = row.get("primary_links");
    let classes: Vec<String> = row.get("classes");
    let outbox_status: String = row.get("outbox_status");
    let rpc_calls: i64 = row.get("rpc_calls");
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
    let provider = live_provider(key);
    let report = run_pass(&rt, &f, &provider, "d1-worker");
    assert_eq!(
        report.claimed, 1,
        "one PENDING outbox row claimed: {report:?}"
    );
    assert_eq!(report.done, 1, "live pass must settle DONE: {report:?}");

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
        hex::encode(distill_prompt_contract().sha256.0),
        "prompt_hash column is the contract sha256"
    );
    assert_eq!(o.run_parser_version, DISTILL_PARSER_VERSION);
    assert_eq!(o.disclosure_outcomes, vec!["SUCCESS".to_owned()]);
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
         disclosures={:?} rpc_calls={} projection(done={},failed={},highwater={}) ticket=({state},{error_class:?})",
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
/// Known deviation (ADR-0016 已知局限, review P1): the evidence axis the worker hashes is
/// `payload_sha256(canonical jsonb of events.payload)`, not `evidence_objects.payload_sha256`
/// (remember's raw-bytes digest) — `EvidencePayloadSha256` has no read-back from the stored
/// bytea (§48.0① sole constructor), so the persisted array cannot be fed to `source_hash`.
/// This check therefore recomputes the axis from `events.payload` and asserts the array holds
/// the anchor, so either half drifting fails here; the seed uses non-canonical raw bytes so the
/// two digests are provably different in this test.
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
        vec![anchor],
        "evidence_payload_sha256[] is the Evidence's own payload_sha256 (0064 column contract)"
    );
    let embedding_version: Option<String> = run.get("embedding_version");
    let card_builder_version: Option<String> = run.get("card_builder_version");
    let context_snapshot_seq: i64 = run.get("context_snapshot_seq");
    let recomputed = source_hash(&ProcessingInputFingerprintInputs {
        evidence_payload_sha256: &[canonical],
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
        "source_hash recomputes from the persisted axes + canonical evidence digest"
    );
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
    assert_eq!(report.done, 1, "{report:?}");
    assert_eq!(report.rejected, 1, "rejection counted: {report:?}");
    assert_eq!(report.memories, 0);
    let o = observe(&mut f, evidence_id);
    assert_eq!(o.memories, 0, "over-ceiling candidate must leave NO row");
    assert_eq!(o.outbox_status, "DONE");
    assert!(o.run_completed);
    assert_eq!(o.run_output_count, Some(0));
    println!(
        "D2 ASSERTION LOG: report={report:?} memories={} outbox={} output_count={:?}",
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
    let provider = FakeProvider::new(vec![r#"{"memories":[]}"#]);
    let report = run_pass(&rt, &f, &provider, "d3-worker");
    assert_eq!(report.done, 1, "{report:?}");
    let o = observe(&mut f, evidence_id);
    assert_eq!(o.memories, 0);
    assert_eq!(o.outbox_status, "DONE");
    assert_eq!(o.run_output_count, Some(0));

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
    // Claim order is commit_seq ascending, so the replies line up with the two rows.
    let provider = FakeProvider::new(vec![
        r#"{"memories":[{"content":"c","memory_type":"Constraint","class":"PrivateKnowledge","confidence":0.5}]}"#,
        r#"{"memories":[{"content":"c","memory_type":"Fact","class":"PrivateKnowledge","confidence":0.5,"leak":"x"}]}"#,
    ]);
    let report = run_pass(&rt, &f, &provider, "d4-worker");
    assert_eq!(report.claimed, 2, "{report:?}");
    assert_eq!(report.failed, 2, "{report:?}");
    assert_eq!(report.done, 0);
    assert_eq!(report.memories, 0);
    for evidence_id in [bad_enum, extra_key] {
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
            vec!["SUCCESS".to_owned()],
            "bytes did leave; the reply was the problem"
        );
    }
    // FAILED is terminal (input-bound rejection): a second pass claims nothing.
    let again = run_pass(&rt, &f, &FakeProvider::new(vec![]), "d4-worker-2");
    assert_eq!(again.claimed, 0);
    println!("D4 ASSERTION LOG: report={report:?} second_pass={again:?}");
}

// ----------------------------------------------------------------------------
// D5 — idempotency: DONE rows are not re-claimed; an expired PROCESSING lease is retried
// without duplicate rows.
// ----------------------------------------------------------------------------

#[test]
fn d5_two_passes_and_expired_lease_never_duplicate_memories() {
    let Some(mut f) = setup_db("d5_two_passes_and_expired_lease_never_duplicate_memories") else {
        return;
    };
    let reply = r#"{"memories":[{"content":"Health endpoint before traffic.","memory_type":"Decision","class":"PrivateKnowledge","confidence":0.9}]}"#;
    let (evidence_a, _, _) = seed_evidence(&mut f, EVIDENCE_TEXT);
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let first = run_pass(&rt, &f, &FakeProvider::new(vec![reply]), "d5-worker");
    assert_eq!(first.done, 1, "{first:?}");
    assert_eq!(observe(&mut f, evidence_a).memories, 1);
    assert_fingerprint_recomputes(&mut f, evidence_a);
    let second = run_pass(&rt, &f, &FakeProvider::new(vec![]), "d5-worker");
    assert_eq!(
        second.claimed, 0,
        "DONE rows are never re-claimed: {second:?}"
    );
    assert_eq!(observe(&mut f, evidence_a).memories, 1);

    // A worker that crashed mid-flight: the row sits PROCESSING under a dead lease.
    let (evidence_b, _, _) = seed_evidence(&mut f, "crashed lease evidence");
    f.admin
        .execute(
            "UPDATE ops.outbox SET status = 'PROCESSING', lease_owner = 'dead-worker', \
                    lease_expires_at = clock_timestamp() - interval '1 second' \
             WHERE evidence_id = $1",
            &[&evidence_b],
        )
        .expect("simulate expired lease");
    let retry = run_pass(&rt, &f, &FakeProvider::new(vec![reply]), "d5-worker-2");
    assert_eq!(
        retry.claimed, 1,
        "expired PROCESSING lease is reclaimable: {retry:?}"
    );
    assert_eq!(retry.done, 1);
    assert_eq!(observe(&mut f, evidence_b).memories, 1);
    let after = run_pass(&rt, &f, &FakeProvider::new(vec![]), "d5-worker-3");
    assert_eq!(after.claimed, 0);
    assert_eq!(
        observe(&mut f, evidence_b).memories,
        1,
        "retry never duplicates"
    );
    assert_eq!(observe(&mut f, evidence_a).memories, 1);

    // A live lease is not reclaimable: a concurrent worker sees nothing.
    let (evidence_c, _, _) = seed_evidence(&mut f, "live lease evidence");
    f.admin
        .execute(
            "UPDATE ops.outbox SET status = 'PROCESSING', lease_owner = 'busy-worker', \
                    lease_expires_at = clock_timestamp() + interval '10 minutes' \
             WHERE evidence_id = $1",
            &[&evidence_c],
        )
        .expect("simulate live lease");
    let contended = run_pass(&rt, &f, &FakeProvider::new(vec![]), "d5-worker-4");
    assert_eq!(
        contended.claimed, 0,
        "live lease must not be stolen: {contended:?}"
    );
    println!(
        "D5 ASSERTION LOG: first={first:?} second={second:?} retry={retry:?} after={after:?} contended={contended:?}"
    );
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
    assert_eq!(unadmitted.deferred, 1, "{unadmitted:?}");
    assert_eq!(unadmitted.failed, 0, "{unadmitted:?}");
    assert_eq!(
        outbox_lease(&mut f, evidence_id),
        ("PENDING".to_owned(), None),
        "row handed back with the lease cleared"
    );
    assert_eq!(processing_runs(&mut f, evidence_id), (0, 0));
    observe_provider_health(&mut f, "HEALTHY");

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

    // (3) The next healthy pass distills it exactly once.
    let recovered = run_pass(&rt, &f, &FakeProvider::new(vec![reply]), "d6-worker");
    assert_eq!(recovered.claimed, 1, "{recovered:?}");
    assert_eq!(recovered.done, 1, "{recovered:?}");
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
