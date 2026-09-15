//! §11.8 consolidation<->private-worker inference hop — spawns the real
//! `humaux-private-worker` `inference_rpc` axum app in-process on a temp UDS path (same shape
//! `bins/gateway/tests/query_embedding_rpc.rs` already uses for the sibling ADR-0012 RPC) and
//! drives `run_once_bound` through the real `humaux_consolidation_worker::inference_client::
//! UdsInferenceClient` against the real MiniMax provider (ADR-0015).
//!
//! Four states, mirroring `crates/adapters/tests/minimax_live_smoke.rs`'s own doc:
//! 1. `MINIMAX_API_KEY` missing (env + `.env` fallback) ⇒ visible SKIP for T1/T5 only (T2-T4
//!    and T6 need no key and always run against a live DB).
//! 2. DB missing/unreachable/unmigrated ⇒ visible SKIP for every test in this file.
//! 3. `HUMAUX_REQUIRE_MINIMAX=1`/`HUMAUX_REQUIRE_DB=1` with the dependency missing ⇒ panic
//!    (ADR-0005), via `skip_or_fail`.
//! 4. Everything present ⇒ real assertions.
//!
//! The fixture seeds the complete Phase 9 R3 admission lane for purpose `PRIVATE_CONSOLIDATE`
//! (same graph `crates/adapters/tests/reasoning_route_health_admission.rs::seed_lane` seeds
//! for `CONTRIBUTION_DEIDENTIFY`) so the private worker goes through the real
//! `control.resolve_user_reasoning_admission` resolver — no admission shortcut.

use std::sync::Arc;
use std::time::Duration;

use humaux_adapters::byok::{
    CredentialDecryptor, CredentialRef, OpenAiCompatibleProvider, PlaintextApiKey,
    ReasoningCapability, ReasoningProviderDescriptor, ReasoningProviderError, ssrf,
};
use humaux_adapters::consolidate_repo::{PublishOutcome, ROLLUP_TICKET_EVENT_TYPE};
use humaux_adapters::consolidation_reasoner::MANIFEST_MISMATCH;
use humaux_adapters::contribution_reasoner::ContributionReasonerConfig;
use humaux_adapters::disclosure::DeletionCapability;
use humaux_adapters::postgres::{ConsolidationDbPool, PrivateWorkerDbPool};
use humaux_adapters::private_inference_rpc::{
    ClaimOutcome, ConsolidationRegistrations, PrivateWorkerInferenceCalls, RegisterCall,
};
use humaux_application::consolidate::{
    PrivateReasoningError, PrivateReasoningPort, PrivateReasoningPurpose, PrivateReasoningResult,
    ReasoningRouteBindingId, ReasoningRouteBindingVersion, SealedPrivateReasoningRequest,
};
use humaux_consolidation_worker::inference_client::UdsInferenceClient;
use humaux_consolidation_worker::{RunOnceError, build_rollup, run_once, run_once_bound};
use humaux_domain::authority::AuthorityClass;
use humaux_domain::egress::ProcessorId;
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use tokio::net::{UnixListener, UnixStream};
use uuid::Uuid;

const NAME: &str = "consolidation_hop_e2e";
const MINIMAX_CHAT_URL: &str = "https://api.minimaxi.com/v1/chat/completions";
const MINIMAX_MODEL: &str = "MiniMax-M3";
/// One fixed egress processor id shared by the seeded `control.provider_endpoints` row and
/// the private worker's deny-only allowlist (`ContributionReasonerConfig
/// .allowed_egress_processor_id`) — the admission resolver and `provider_matches_admission`
/// both compare it exactly.
const EGRESS_PROCESSOR_ID: Uuid = Uuid::from_u128(0x2001);
const REGION: &str = "cn-shanghai";
const SERVICE_TIER: &str = "standard";
const PURPOSE_DB: &str = "PRIVATE_CONSOLIDATE";

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

#[async_trait::async_trait]
impl CredentialDecryptor for EnvKeyDecryptor {
    async fn resolve(
        &self,
        _credential_ref: CredentialRef,
    ) -> Result<PlaintextApiKey, ReasoningProviderError> {
        Ok(PlaintextApiKey::new(self.key_material.clone()))
    }
}

/// 卡 17 之后拨号也走这个 resolver（ADR-0039 判据0），所以这里再返回一个「随便挑的公网
/// 地址」就等于把 live 外呼指到别人家去——只有在代理接管解析时才碰巧还绿。改成与生产同一
/// 个机制：`HUMAUX_MINIMAX_DNS_PINS`（`host=ip[|ip],...`，与
/// `HUMAUX_PRIVATE_WORKER_DNS_PINS` 同格式）。没设 = 空 pin 集 = 全量落系统 DNS，也就是
/// CI / 无 fake-IP 环境的默认行为。
///
/// 这台开发机的 DNS 被本机代理 fake-IP 接管（`api.minimaxi.com` → `198.18.0.x`，RFC 2544
/// 保留段），`SystemDnsResolver` 在这里**必然**被 `ResolvedIpForbidden` 拒——那是闸的正确
/// 行为，不是 bug；跑本套件时给上真地址的 pin。`/tests/` 读 env 是 §78 boundary lint 的既有
/// 豁免面。
fn live_dns_resolver() -> Arc<dyn ssrf::DnsResolver> {
    Arc::new(
        ssrf::PinnedDnsResolver::parse(
            &std::env::var("HUMAUX_MINIMAX_DNS_PINS").unwrap_or_default(),
        )
        .expect("HUMAUX_MINIMAX_DNS_PINS must parse as host=ip[|ip],..."),
    )
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
        // Contribution-path prompt config only; the Consolidate path takes prompt/schema/budget
        // from `consolidation_prompt_contract()` and ignores these three.
        system_prompt: "s".to_string(),
        json_schema: "{}".to_string(),
        max_output_tokens: 64,
    }
}

fn temp_socket_path(tag: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("/tmp/hq-{tag}-{}.sock", Uuid::now_v7().simple()))
}

/// Learns this test process's own real uid — same technique
/// `bins/gateway/tests/query_embedding_rpc.rs::own_uid` uses, no `libc`/`nix` dependency.
async fn own_uid() -> u32 {
    let path = temp_socket_path("uidprobe");
    let listener = UnixListener::bind(&path).expect("bind uid probe socket");
    let client = UnixStream::connect(&path).await.expect("connect uid probe");
    let (server_side, _) = listener.accept().await.expect("accept uid probe");
    let uid = server_side.peer_cred().expect("peer credential").uid();
    drop(client);
    drop(server_side);
    let _ = std::fs::remove_file(&path);
    uid
}

async fn spawn_private_worker(
    socket_path: &std::path::Path,
    expected_consolidation_uid: u32,
    calls: PrivateWorkerDbPool,
    key: String,
) {
    let provider = OpenAiCompatibleProvider::with_egress_transport(
        descriptor(),
        MINIMAX_CHAT_URL.to_string(),
        Duration::from_secs(120),
        EnvKeyDecryptor { key_material: key },
        ssrf::CustomEndpointPolicy::default(),
        live_dns_resolver(),
    )
    .expect("SSRF choke point must accept the endpoint (see live_dns_resolver / HUMAUX_MINIMAX_DNS_PINS)");
    let state = Arc::new(humaux_private_worker::inference_rpc::RpcState {
        expected_consolidation_uid,
        calls,
        config: contribution_config(),
        provider: Box::new(provider),
    });
    // Same `bind_socket` + `serve` pair the binary's `--serve-rpc` mode runs — nothing here
    // reimplements the accept loop, so this harness proves the production listener.
    let listener = humaux_private_worker::inference_rpc::bind_socket(socket_path)
        .expect("bind private inference rpc socket");
    tokio::spawn(async move {
        let _ = humaux_private_worker::inference_rpc::serve(listener, state).await;
    });
    // Give the listener a moment to accept before the first dial.
    tokio::time::sleep(Duration::from_millis(50)).await;
}

struct Fixture {
    admin: Client,
    dsn: String,
    tenant_id: Uuid,
    user_id: Uuid,
    reasoning_domain_id: Uuid,
    workspace_id: Uuid,
    binding_id: Uuid,
    binding_version: i64,
}

impl Drop for Fixture {
    /// Throwaway-tenant cleanup. `session_replication_role = replica` (superuser test DSN)
    /// skips the 0128 append-only triggers on the seeded route graph and the FK/RI triggers,
    /// so one pass over every `tenant_id`-carrying table plus the four child tables that key
    /// through a parent is enough; nothing here is a production path.
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
            "DELETE FROM private.memory_rollup_sources WHERE rollup_id IN \
               (SELECT rollup_id FROM private.memory_rollups WHERE tenant_id = '{tenant}'); \
             DELETE FROM private.memory_consolidation_inputs WHERE run_id IN \
               (SELECT run_id FROM private.memory_consolidation_runs WHERE tenant_id = '{tenant}'); \
             DELETE FROM private.memory_evidence WHERE memory_id IN \
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
/// `PRIVATE_CONSOLIDATE` over the MiniMax descriptor this file's provider uses.
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
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
             WHERE table_schema='ops' AND table_name='private_inference_rpc_calls' \
               AND column_name='consolidation_run_id')",
            &[],
        )
        .ok()?
        .get(0);
    if !migrated {
        skip_or_fail(
            test_name,
            "missing object: ops.private_inference_rpc_calls.consolidation_run_id — run `cargo xtask migrate` (0145)",
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

    // ---- R3 admission lane (mirrors reasoning_route_health_admission.rs::seed_lane) ----
    let credential: Uuid = admin
        .query_one(
            "INSERT INTO control.credentials(tenant_id,purpose,openbao_ref) VALUES($1,'USER_REASONING',$2) RETURNING credential_id",
            &[&tenant_id, &format!("openbao://{NAME}/{suffix}")],
        )
        .expect("credential locator")
        .get(0);
    let d = descriptor();
    // `control.processor_models` is global (no tenant) and append-only (0128 trigger): keep one
    // catalog row per (processor, model, revision) across test runs.
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
    let binding_id: Uuid = admin
        .query_one(
            "INSERT INTO control.reasoning_route_bindings(tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES($1,$2,$3,$4,1) RETURNING binding_id",
            &[&tenant_id, &reasoning_domain_id, &PURPOSE_DB, &policy],
        )
        .expect("route binding")
        .get(0);
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
        binding_id,
        binding_version: 1,
    })
}

/// Seeds one active memory + its PRIMARY evidence, WORKSPACE_SHARED in the fixture workspace
/// (migration 0145's RLS widening is exactly what lets both headless consolidation roles see
/// it — before 0145 this shape was verified invisible, `PublishOutcome::NoOutput`).
fn seed_workspace_memory(f: &mut Fixture, text: &str) -> (Uuid, Uuid) {
    let workspace_id = f.workspace_id;
    seed_memory(f, text, Some(workspace_id))
}

/// Same shape, TENANT_SHARED (no workspace) — the only input class a tenant-scoped run
/// (`workspace_id = None`) may consume (T6).
fn seed_tenant_memory(f: &mut Fixture, text: &str) -> (Uuid, Uuid) {
    seed_memory(f, text, None)
}

fn seed_memory(f: &mut Fixture, text: &str, workspace_id: Option<Uuid>) -> (Uuid, Uuid) {
    let visibility_class = if workspace_id.is_some() {
        "WORKSPACE_SHARED"
    } else {
        "TENANT_SHARED"
    };
    let evidence_id: Uuid = f
        .admin
        .query_one(
            &format!(
                "INSERT INTO private.evidence_objects \
                   (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                    visibility_class, visibility_workspace_id, reasoning_domain_id) \
                 VALUES ($1, 'EVENT', $2, 'PRIVATE', 'DirectUserInput', '{visibility_class}', $3, $4) \
                 RETURNING evidence_id"
            ),
            &[
                &f.tenant_id,
                &vec![0u8; 32],
                &workspace_id,
                &f.reasoning_domain_id,
            ],
        )
        .expect("insert evidence")
        .get(0);
    f.admin
        .execute(
            "INSERT INTO private.events (event_id, event_kind, payload) \
             VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
            &[&evidence_id],
        )
        .expect("insert event");
    let content = serde_json::json!({ "text": text });
    let mut txn = f.admin.transaction().expect("begin seed txn");
    let memory_id: Uuid = txn
        .query_one(
            &format!(
                "INSERT INTO private.memory_records \
                   (tenant_id, memory_type, content, visibility_class, visibility_workspace_id, \
                    authority_class, confidence, status, asserted_at) \
                 VALUES ($1, 'NOTE', $2, '{visibility_class}', $3, \
                         'UserPreference', 0.8, 'active', now()) \
                 RETURNING memory_id"
            ),
            &[&f.tenant_id, &content, &workspace_id],
        )
        .expect("seed active memory")
        .get(0);
    txn.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) \
         VALUES ($1, $2, 'PRIMARY')",
        &[&memory_id, &evidence_id],
    )
    .expect("insert memory_evidence link");
    txn.commit().expect("commit seed txn");
    (memory_id, evidence_id)
}

/// Registration-only fixtures (T3) use a binding that never resolves; the private worker
/// never dispatches for them.
const UNADMITTED_BINDING_ID: Uuid = Uuid::from_u128(0x1001);

/// Key-free port for the tests that only need `run_once`'s selection/publish legs (T4, T6):
/// returns fixed bytes, never touches a provider.
struct FakePort;

#[async_trait::async_trait]
impl PrivateReasoningPort for FakePort {
    async fn infer(
        &self,
        _req: SealedPrivateReasoningRequest,
    ) -> Result<PrivateReasoningResult, PrivateReasoningError> {
        Ok(PrivateReasoningResult {
            output_bytes: b"fake port output".to_vec(),
            output_sha256: humaux_application::consolidate::ContentSha256([1u8; 32]),
            provider_trace: humaux_application::consolidate::ProviderTraceRef("fake-trace".into()),
            model_call_id: Uuid::from_u128(0x9001),
            binding_id: ReasoningRouteBindingId(UNADMITTED_BINDING_ID),
            binding_version: ReasoningRouteBindingVersion(1),
        })
    }
}

fn call_ttl() -> Duration {
    Duration::from_secs(180)
}

/// (T1) Full positive path: real MiniMax provider through the real UDS RPC hop, real R3
/// admission, WORKSPACE_SHARED inputs, workspace-scoped rollup + ticket.
#[test]
#[allow(clippy::too_many_lines)]
fn t1_full_inference_hop_publishes_ticket() {
    let Some(key) = load_minimax_key() else {
        skip_or_fail(
            "t1_full_inference_hop_publishes_ticket",
            "missing object: MINIMAX_API_KEY (env and /Volumes/data/viral-skill-eval/.env both empty)",
            ExternalDep::MiniMax,
        );
        return;
    };
    let Some(mut f) = setup_db("t1_full_inference_hop_publishes_ticket") else {
        return;
    };
    let (memory_a, evidence_a) = seed_workspace_memory(
        &mut f,
        "The user prefers Rust for backend services because of memory safety and predictable performance.",
    );
    let (memory_b, evidence_b) = seed_workspace_memory(
        &mut f,
        "The user asked that new services expose a health endpoint before any traffic is routed to them.",
    );

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let outcome = rt.block_on(async {
        let uid = own_uid().await;
        let socket_path = temp_socket_path("t1");
        let private_pool =
            PrivateWorkerDbPool::connect(&dsn_as_role(&f.dsn, "role_private_worker"))
                .await
                .expect("private worker pool");
        spawn_private_worker(&socket_path, uid, private_pool, key).await;

        let consolidation_pool =
            ConsolidationDbPool::connect(&dsn_as_role(&f.dsn, "role_consolidation_worker"))
                .await
                .expect("consolidation pool");
        run_once_bound(
            &consolidation_pool,
            |run_id| {
                UdsInferenceClient::new(
                    &consolidation_pool,
                    socket_path.to_string_lossy(),
                    f.tenant_id,
                    call_ttl(),
                    call_ttl(),
                    run_id,
                )
            },
            f.tenant_id,
            f.reasoning_domain_id,
            ReasoningRouteBindingId(f.binding_id),
            ReasoningRouteBindingVersion(f.binding_version),
            Some(f.workspace_id),
            10_000,
            build_rollup,
            None,
        )
        .await
    });

    let rollup_id = match outcome {
        Ok(PublishOutcome::Published { rollup_id }) => rollup_id,
        other => {
            // Diagnostic only: the port redacts its message (§11.8), so surface the registration
            // row's own outcome/failure text and the side-effect counts the way T5 does.
            let diag = f
                .admin
                .query_opt(
                    "SELECT c.outcome, c.response_failure_message, \
                            c.consolidation_run_id IS NOT NULL AS run_bound, \
                            (SELECT count(*) FROM ops.data_disclosures d WHERE d.tenant_id = c.tenant_id) AS disclosures, \
                            (SELECT count(*) FROM private.memory_rollups r WHERE r.tenant_id = c.tenant_id) AS rollups, \
                            left(convert_from(coalesce(c.response_output_bytes, ''::bytea), 'UTF8'), 1200) AS output_head \
                     FROM ops.private_inference_rpc_calls c WHERE c.tenant_id = $1 \
                     ORDER BY c.registered_at DESC LIMIT 1",
                    &[&f.tenant_id],
                )
                .expect("rpc diagnostic row")
                .map(|row| {
                    let outcome: String = row.get("outcome");
                    let failure: Option<String> = row.get("response_failure_message");
                    let run_bound: bool = row.get("run_bound");
                    let disclosures: i64 = row.get("disclosures");
                    let rollups: i64 = row.get("rollups");
                    let output_head: String = row.get("output_head");
                    format!(
                        "rpc_outcome={outcome} failure={failure:?} run_bound={run_bound} \
                         disclosures={disclosures} rollups={rollups} output_head={output_head:?}"
                    )
                });
            panic!(
                "run_once_bound must publish a rollup once a real inference result comes back \
                 over the UDS hop, got: {other:?}; rpc diag: {diag:?}"
            )
        }
    };

    // (a) rollup row: WORKSPACE_SHARED in the run's workspace, non-empty content, class from the
    //     parsed provider output (re-parsed here from the stored response bytes).
    let row = f
        .admin
        .query_one(
            "SELECT r.run_id, r.visibility_class, r.visibility_workspace_id, r.authority_class, \
                    length(r.content->>'content') AS content_len, \
                    (SELECT array_agg(s.memory_id) FROM private.memory_rollup_sources s WHERE s.rollup_id = r.rollup_id) AS source_memories, \
                    (SELECT array_agg(s.evidence_id) FROM private.memory_rollup_sources s WHERE s.rollup_id = r.rollup_id) AS source_evidence \
             FROM private.memory_rollups r WHERE r.rollup_id = $1",
            &[&rollup_id],
        )
        .expect("rollup row");
    let run_id: Uuid = row.get("run_id");
    let visibility_class: String = row.get("visibility_class");
    let visibility_workspace_id: Option<Uuid> = row.get("visibility_workspace_id");
    let authority_class: String = row.get("authority_class");
    let content_len: Option<i32> = row.get("content_len");
    let source_memories: Vec<Uuid> = row.get("source_memories");
    let source_evidence: Vec<Uuid> = row.get("source_evidence");
    assert_eq!(visibility_class, "WORKSPACE_SHARED");
    assert_eq!(visibility_workspace_id, Some(f.workspace_id));
    assert!(
        content_len.unwrap_or(0) > 0,
        "rollup content must be non-empty"
    );
    assert!(!source_memories.is_empty());
    for source in &source_memories {
        assert!(
            *source == memory_a || *source == memory_b,
            "rollup source {source} is not one of this run's inputs"
        );
    }
    for evidence in &source_evidence {
        assert!(*evidence == evidence_a || *evidence == evidence_b);
    }

    // (e) rpc_calls row: COMPLETED, bound to this run, sha matches bytes, receipt = a
    //     finalized SUCCESS disclosure row (ADR-0015 §"ledger leg").
    let rpc = f
        .admin
        .query_one(
            "SELECT c.outcome, c.consolidation_run_id, \
                    c.response_output_sha256 = sha256(c.response_output_bytes) AS sha_ok, \
                    convert_from(c.response_output_bytes, 'UTF8') AS output_json, \
                    c.response_model_call_id, \
                    (SELECT d.outcome FROM ops.data_disclosures d WHERE d.disclosure_id = c.response_model_call_id) AS disclosure_outcome, \
                    (SELECT count(*) FROM ops.data_disclosure_sources s WHERE s.disclosure_id = c.response_model_call_id) AS disclosure_sources \
             FROM ops.private_inference_rpc_calls c WHERE c.tenant_id = $1 AND c.purpose = 'Consolidate'",
            &[&f.tenant_id],
        )
        .expect("exactly one rpc row");
    let rpc_outcome: String = rpc.get("outcome");
    let rpc_run: Option<Uuid> = rpc.get("consolidation_run_id");
    let sha_ok: bool = rpc.get("sha_ok");
    let output_json: String = rpc.get("output_json");
    let receipt: Option<Uuid> = rpc.get("response_model_call_id");
    let disclosure_outcome: Option<String> = rpc.get("disclosure_outcome");
    let disclosure_sources: i64 = rpc.get("disclosure_sources");
    assert_eq!(rpc_outcome, "COMPLETED");
    assert_eq!(rpc_run, Some(run_id));
    assert!(
        sha_ok,
        "response_output_sha256 must be sha256(response_output_bytes)"
    );
    assert!(receipt.is_some());
    assert_eq!(disclosure_outcome.as_deref(), Some("SUCCESS"));
    assert_eq!(
        disclosure_sources, 2,
        "both inputs disclosed as Memory sources"
    );

    // (e2) Card 20's primary acceptance gate, positive half: the §19.1 cost row that must exist
    //      ALONGSIDE the §7.4 disclosure row just asserted, never instead of it. The only
    //      ledger assertion this suite had was the NEGATIVE one on the manifest-mismatch path
    //      (`ledger_rows == 0`), so both `consolidation_reasoner` ledger calls could be deleted
    //      with the suite still green. Deleting either now turns this red.
    let ledger = f
        .admin
        .query_one(
            "SELECT count(*) AS rows_n, \
                    max(l.status) AS status, \
                    max(l.model) AS model, \
                    max(l.input_tokens) AS input_tokens, \
                    max(l.output_tokens) AS output_tokens \
             FROM ops.model_call_ledger l \
             WHERE l.tenant_id = $1 AND l.purpose = 'PRIVATE_CONSOLIDATE'",
            &[&f.tenant_id],
        )
        .expect("consolidation ledger rows");
    let ledger_rows: i64 = ledger.get("rows_n");
    let ledger_status: Option<String> = ledger.get("status");
    let ledger_model: Option<String> = ledger.get("model");
    let ledger_input_tokens: Option<i64> = ledger.get("input_tokens");
    let ledger_output_tokens: Option<i64> = ledger.get("output_tokens");
    assert_eq!(
        ledger_rows, 1,
        "exactly one ops.model_call_ledger row with purpose PRIVATE_CONSOLIDATE"
    );
    assert_eq!(ledger_status.as_deref(), Some("SUCCEEDED"));
    assert!(
        ledger_model.as_deref().is_some_and(|m| !m.is_empty()),
        "ledger row carries the admitted provider model, got {ledger_model:?}"
    );
    assert!(
        ledger_input_tokens.is_some_and(|v| v > 0),
        "input_tokens from the provider's usage block, got {ledger_input_tokens:?}"
    );
    assert!(
        ledger_output_tokens.is_some_and(|v| v > 0),
        "output_tokens (0168) from the provider's usage block — for a generative hop the output \
         leg usually dominates the bill, got {ledger_output_tokens:?}"
    );
    println!(
        "CONSOLIDATION LEDGER: rows={ledger_rows} status={ledger_status:?} model={ledger_model:?} \
         input_tokens={ledger_input_tokens:?} output_tokens={ledger_output_tokens:?}"
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&output_json).expect("stored output is JSON");
    assert_eq!(
        parsed["class"].as_str(),
        Some(authority_class.as_str()),
        "rollup authority_class must be the class the provider output named"
    );

    // (b)+(c) workspace-scoped ISSUED ticket joined to its outbox row, event_type pinned,
    //         evidence ref = one of the rollup's own source evidence rows.
    let ticket = f
        .admin
        .query_one(
            "SELECT l.scope_kind, l.scope_id, l.state, o.event_type, o.evidence_id \
             FROM projection.stream_log l \
             JOIN ops.outbox o ON o.tenant_id = l.tenant_id AND o.commit_seq = l.commit_seq \
             WHERE l.tenant_id = $1 AND l.domain = 'private_memory'",
            &[&f.tenant_id],
        )
        .expect("exactly one ticket");
    let scope_kind: String = ticket.get("scope_kind");
    let scope_id: Uuid = ticket.get("scope_id");
    let state: String = ticket.get("state");
    let event_type: String = ticket.get("event_type");
    let ticket_evidence: Uuid = ticket.get("evidence_id");
    assert_eq!(scope_kind, "workspace");
    assert_eq!(scope_id, f.workspace_id);
    assert_eq!(state, "ISSUED");
    assert_eq!(event_type, ROLLUP_TICKET_EVENT_TYPE);
    assert!(source_evidence.contains(&ticket_evidence));

    // (d) the projection hop's read (0140 path): as role_retrieval_worker with ONLY
    //     humaux.tenant_id set, the memory + evidence the ticket points at are visible.
    let mut retrieval = Client::connect(&dsn_as_role(&f.dsn, "role_retrieval_worker"), NoTls)
        .expect("retrieval worker client");
    retrieval
        .execute(
            "SELECT set_config('humaux.tenant_id', $1, false)",
            &[&f.tenant_id.to_string()],
        )
        .expect("tenant GUC");
    let visible = retrieval
        .query_one(
            "SELECT (SELECT count(*) FROM private.evidence_objects WHERE evidence_id = $1) AS evidence_visible, \
                    (SELECT count(*) FROM private.memory_records m JOIN private.memory_evidence me ON me.memory_id = m.memory_id \
                      WHERE me.evidence_id = $1 AND m.visibility_class = 'WORKSPACE_SHARED') AS memory_visible, \
                    (SELECT count(*) FROM private.memory_rollups WHERE rollup_id = $2) AS rollup_visible",
            &[&ticket_evidence, &rollup_id],
        )
        .expect("retrieval worker visibility");
    let evidence_visible: i64 = visible.get("evidence_visible");
    let memory_visible: i64 = visible.get("memory_visible");
    let rollup_visible: i64 = visible.get("rollup_visible");
    // 0146: the rollup row itself is SELECT-visible to a tenant-only role_retrieval_worker
    // session (D5(d)); before 0146 this was only logged.
    assert_eq!(
        rollup_visible, 1,
        "rollup row must be visible to role_retrieval_worker (0146)"
    );
    assert_eq!(
        evidence_visible, 1,
        "0140: WORKSPACE_SHARED evidence visible to headless retrieval worker"
    );
    assert_eq!(
        memory_visible, 1,
        "0140: WORKSPACE_SHARED memory visible to headless retrieval worker"
    );

    println!(
        "T1 ASSERTION LOG: rollup_id={rollup_id} run_id={run_id} visibility={visibility_class}/{} \
         class={authority_class} content_chars={} sources={} ticket=({scope_kind},{scope_id},{state},{event_type}) \
         rpc=({rpc_outcome},run_bound={},sha_ok={sha_ok},receipt={},disclosure={:?},disclosure_sources={disclosure_sources}) \
         retrieval_worker_sees(evidence={evidence_visible},memory={memory_visible},rollup_row={rollup_visible})",
        f.workspace_id,
        content_len.unwrap_or(0),
        source_memories.len(),
        rpc_run.is_some(),
        receipt.map(|r| r.to_string()).unwrap_or_default(),
        disclosure_outcome,
    );
}

/// (T2) Wrong peer uid is rejected before the JSON body is ever parsed.
#[test]
fn t2_wrong_peer_uid_rejected_before_body() {
    let Some(f) = setup_db("t2_wrong_peer_uid_rejected_before_body") else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().expect("rt");
    rt.block_on(async {
        let uid = own_uid().await;
        let socket_path = temp_socket_path("t2");
        let private_pool =
            PrivateWorkerDbPool::connect(&dsn_as_role(&f.dsn, "role_private_worker"))
                .await
                .expect("private worker pool");
        // A key that never gets used — the peer-uid check runs before the body is parsed,
        // which is exactly what this test proves, so no MINIMAX_API_KEY is needed.
        spawn_private_worker(
            &socket_path,
            uid.wrapping_add(1),
            private_pool,
            String::new(),
        )
        .await;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = UnixStream::connect(&socket_path).await.expect("connect");
        let body = b"{}";
        let head = format!(
            "POST /internal/v1/private/infer HTTP/1.1\r\nHost: localhost\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.unwrap();
        let text = String::from_utf8_lossy(&raw);
        assert!(
            text.starts_with("HTTP/1.1 403"),
            "wrong peer uid must be rejected 403 before the body is touched, got: {text}"
        );
    });
    drop(f);
}

/// (T3) Same `call_id` twice: the provider is invoked at most once — proven at the
/// registration-table level (the row transitions `REGISTERED -> CLAIMED -> COMPLETED` exactly
/// once; a second claim on the same `call_id` sees `Replay`, never re-enters `CLAIMED`). This
/// does not require a live MINIMAX_API_KEY: the outcome (`COMPLETED`/`FAILED`) is irrelevant to
/// idempotency, only that the *same* stored outcome comes back both times with no second claim.
#[test]
fn t3_same_call_id_twice_claims_once() {
    let Some(mut f) = setup_db("t3_same_call_id_twice_claims_once") else {
        return;
    };
    let (_, evidence_id) = seed_workspace_memory(&mut f, "t3 fixture memory");
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let call_id = rt.block_on(async {
        let private_pool =
            PrivateWorkerDbPool::connect(&dsn_as_role(&f.dsn, "role_private_worker"))
                .await
                .expect("private worker pool");
        let consolidation_pool =
            ConsolidationDbPool::connect(&dsn_as_role(&f.dsn, "role_consolidation_worker"))
                .await
                .expect("consolidation pool");

        let call_id = Uuid::now_v7();
        ConsolidationRegistrations::new(&consolidation_pool)
            .register(&RegisterCall {
                call_id,
                tenant_id: f.tenant_id,
                reasoning_domain_id: f.reasoning_domain_id,
                binding_id: UNADMITTED_BINDING_ID,
                binding_version: 1,
                purpose: humaux_adapters::private_inference_rpc::purpose_db_str(
                    PrivateReasoningPurpose::Consolidate,
                ),
                input_manifest_hash: [7u8; 32],
                consolidation_run_id: None,
                ttl: Duration::from_secs(30),
            })
            .await
            .expect("register call");

        let repo = PrivateWorkerInferenceCalls::new(&private_pool);
        let first = repo
            .load_and_claim(call_id, f.tenant_id, "role_private_worker")
            .await
            .expect("first claim");
        assert!(matches!(first, ClaimOutcome::Claimed(_)));
        repo.finish(
            call_id,
            f.tenant_id,
            humaux_adapters::private_inference_rpc::FinishOutcome::Failed {
                message: "t3 fixture — provider not actually called".to_owned(),
            },
        )
        .await
        .expect("finish call");

        let second = repo
            .load_and_claim(call_id, f.tenant_id, "role_private_worker")
            .await
            .expect("second claim");
        assert!(
            matches!(second, ClaimOutcome::Replay(_)),
            "a second load_and_claim on the same call_id must replay, never re-claim"
        );
        call_id
    });

    // Same lesson `minimax_live_smoke.rs` documents: a synchronous `postgres::Client` call
    // spins up its own tokio runtime internally, so it must run outside `rt.block_on` (nested
    // runtimes panic).
    let claim_count: i64 = f
        .admin
        .query_one(
            "SELECT count(*) FROM ops.private_inference_rpc_calls WHERE call_id = $1",
            &[&call_id],
        )
        .expect("count rows")
        .get(0);
    assert_eq!(
        claim_count, 1,
        "idempotency by call_id: exactly one row, never a second registration"
    );
    let _ = evidence_id;
}

/// (T4) §11.6 guard: `build_rollup` has no DB capability (its signature takes only the
/// materialized id list and the inference result, never a pool/repository) — it structurally
/// cannot issue an `UPDATE` against `private.memory_records`. Proven by observation: publish a
/// rollup over the seeded memory via a `FakePort` (no live key needed) and assert the source
/// memory's own row is byte-identical before and after.
#[test]
fn t4_build_rollup_cannot_mutate_base_memory() {
    let Some(mut f) = setup_db("t4_build_rollup_cannot_mutate_base_memory") else {
        return;
    };
    let (memory_id, evidence_id) = seed_workspace_memory(&mut f, "t4 fixture memory");

    let before = read_memory_snapshot(&mut f.admin, memory_id);

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let consolidation_pool = rt.block_on(ConsolidationDbPool::connect(&dsn_as_role(
        &f.dsn,
        "role_consolidation_worker",
    )));
    let consolidation_pool = consolidation_pool.expect("consolidation pool");
    let outcome = rt.block_on(run_once(
        &consolidation_pool,
        &FakePort,
        f.tenant_id,
        f.reasoning_domain_id,
        ReasoningRouteBindingId(UNADMITTED_BINDING_ID),
        ReasoningRouteBindingVersion(1),
        Some(f.workspace_id),
        10_000,
        |ids, _result| {
            let content = serde_json::json!({"content": "PLEASE UPDATE THE BASE MEMORY BODY"});
            let sources = ids
                .iter()
                .map(|id| (*id, humaux_domain::authority::EvidenceId(evidence_id)))
                .collect();
            Ok((content, AuthorityClass::PrivateKnowledge, sources))
        },
    ));
    assert!(
        matches!(outcome, Ok(PublishOutcome::Published { .. })),
        "T4 expected a published rollup over the seeded memory, got: {outcome:?}"
    );

    let after = read_memory_snapshot(&mut f.admin, memory_id);
    assert_eq!(
        before, after,
        "§11.6: publish_rollup must never rewrite the source memory's own body/authority/status"
    );
}

/// Tampers the run's recorded input ordinals AFTER `run_once_bound` sealed the manifest hash
/// and BEFORE the private worker reads the rows (the tamper runs inside `infer`, ahead of the
/// registration + dial the inner client performs) — the exact window the §11.8 integrity gate
/// exists for.
struct TamperThenDial<'a> {
    inner: UdsInferenceClient<'a>,
    admin_dsn: String,
    run_id: Uuid,
}

#[async_trait::async_trait]
impl PrivateReasoningPort for TamperThenDial<'_> {
    async fn infer(
        &self,
        req: SealedPrivateReasoningRequest,
    ) -> Result<PrivateReasoningResult, PrivateReasoningError> {
        let dsn = self.admin_dsn.clone();
        let run_id = self.run_id;
        let tampered = tokio::task::spawn_blocking(move || {
            let mut admin = Client::connect(&dsn, NoTls).expect("admin client");
            admin
                .execute(
                    "UPDATE private.memory_consolidation_inputs SET ordinal = ordinal + 100 WHERE run_id = $1",
                    &[&run_id],
                )
                .expect("tamper ordinals")
        })
        .await
        .expect("join tamper");
        assert!(tampered >= 2, "tamper must touch every recorded input");
        self.inner.infer(req).await
    }
}

/// (T5) Manifest mismatch fails closed at the private worker: FAILED rpc row carrying the
/// exact integrity-gate message, and zero provider dispatch — no disclosure row and no ledger
/// row for the tenant (both are written before any bytes leave, so their absence proves the
/// provider was never reached).
#[test]
fn t5_manifest_mismatch_fails_closed_without_provider_call() {
    let Some(key) = load_minimax_key() else {
        skip_or_fail(
            "t5_manifest_mismatch_fails_closed_without_provider_call",
            "missing object: MINIMAX_API_KEY (env and /Volumes/data/viral-skill-eval/.env both empty)",
            ExternalDep::MiniMax,
        );
        return;
    };
    let Some(mut f) = setup_db("t5_manifest_mismatch_fails_closed_without_provider_call") else {
        return;
    };
    seed_workspace_memory(&mut f, "t5 fixture memory one");
    seed_workspace_memory(&mut f, "t5 fixture memory two");

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let outcome = rt.block_on(async {
        let uid = own_uid().await;
        let socket_path = temp_socket_path("t5");
        let private_pool =
            PrivateWorkerDbPool::connect(&dsn_as_role(&f.dsn, "role_private_worker"))
                .await
                .expect("private worker pool");
        spawn_private_worker(&socket_path, uid, private_pool, key).await;
        let consolidation_pool =
            ConsolidationDbPool::connect(&dsn_as_role(&f.dsn, "role_consolidation_worker"))
                .await
                .expect("consolidation pool");
        run_once_bound(
            &consolidation_pool,
            |run_id| TamperThenDial {
                inner: UdsInferenceClient::new(
                    &consolidation_pool,
                    socket_path.to_string_lossy(),
                    f.tenant_id,
                    call_ttl(),
                    call_ttl(),
                    run_id,
                ),
                admin_dsn: f.dsn.clone(),
                run_id,
            },
            f.tenant_id,
            f.reasoning_domain_id,
            ReasoningRouteBindingId(f.binding_id),
            ReasoningRouteBindingVersion(f.binding_version),
            Some(f.workspace_id),
            10_000,
            build_rollup,
            None,
        )
        .await
    });
    assert!(
        matches!(outcome, Err(RunOnceError::Reasoning(_))),
        "a tampered manifest must surface as an inference-hop failure, got: {outcome:?}"
    );

    let row = f
        .admin
        .query_one(
            "SELECT c.outcome, c.response_failure_message, c.consolidation_run_id IS NOT NULL AS run_bound, \
                    (SELECT count(*) FROM ops.data_disclosures d WHERE d.tenant_id = c.tenant_id) AS disclosures, \
                    (SELECT count(*) FROM ops.model_call_ledger l WHERE l.tenant_id = c.tenant_id) AS ledger_rows, \
                    (SELECT count(*) FROM private.memory_rollups r WHERE r.tenant_id = c.tenant_id) AS rollups \
             FROM ops.private_inference_rpc_calls c WHERE c.tenant_id = $1",
            &[&f.tenant_id],
        )
        .expect("exactly one rpc row");
    let outcome_db: String = row.get("outcome");
    let message: Option<String> = row.get("response_failure_message");
    let run_bound: bool = row.get("run_bound");
    let disclosures: i64 = row.get("disclosures");
    let ledger_rows: i64 = row.get("ledger_rows");
    let rollups: i64 = row.get("rollups");
    assert_eq!(outcome_db, "FAILED");
    assert!(run_bound);
    assert_eq!(
        message.as_deref(),
        Some(
            PrivateReasoningError::new(MANIFEST_MISMATCH)
                .to_string()
                .as_str()
        ),
        "failure must be the integrity gate, not some other error"
    );
    assert_eq!(
        disclosures, 0,
        "no §7.4 disclosure row ⇒ no bytes left the worker"
    );
    assert_eq!(ledger_rows, 0, "no ledger row ⇒ no provider attempt");
    assert_eq!(rollups, 0);
    println!(
        "T5 ASSERTION LOG: rpc_outcome={outcome_db} run_bound={run_bound} disclosures={disclosures} ledger_rows={ledger_rows} rollups={rollups}"
    );
}

/// (T6) A tenant-scoped run (`workspace_id = None`) publishes a TENANT_SHARED rollup, so it
/// must never consume WORKSPACE_SHARED inputs. Before 0145 that held only because RLS hid every
/// WORKSPACE_SHARED row from the headless role; after 0145 the role reads them for workspace
/// runs, so `select_and_materialize_inputs`'s own predicate is the sole guard against a
/// cross-workspace → tenant-wide widening. Seeds two WORKSPACE_SHARED memories + one
/// TENANT_SHARED, runs with `None` through `FakePort` (no live key), and asserts the run
/// materialized — and the rollup cites — exactly the TENANT_SHARED one.
#[test]
fn t6_tenant_scoped_run_never_consumes_workspace_shared_inputs() {
    let Some(mut f) = setup_db("t6_tenant_scoped_run_never_consumes_workspace_shared_inputs")
    else {
        return;
    };
    seed_workspace_memory(&mut f, "t6 workspace memory one");
    seed_workspace_memory(&mut f, "t6 workspace memory two");
    let (tenant_memory, tenant_evidence) = seed_tenant_memory(&mut f, "t6 tenant memory");

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let consolidation_pool = rt
        .block_on(ConsolidationDbPool::connect(&dsn_as_role(
            &f.dsn,
            "role_consolidation_worker",
        )))
        .expect("consolidation pool");
    let outcome = rt.block_on(run_once_bound(
        &consolidation_pool,
        |_run_id| FakePort,
        f.tenant_id,
        f.reasoning_domain_id,
        ReasoningRouteBindingId(UNADMITTED_BINDING_ID),
        ReasoningRouteBindingVersion(1),
        None,
        10_000,
        |inputs, _result| {
            let sources = inputs
                .iter()
                .map(|input| (input.memory_id, input.evidence_id))
                .collect();
            Ok((
                serde_json::json!({"content": "t6 tenant rollup"}),
                AuthorityClass::PrivateKnowledge,
                sources,
            ))
        },
        None,
    ));
    let rollup_id = match outcome {
        Ok(PublishOutcome::Published { rollup_id }) => rollup_id,
        other => panic!("T6 expected a published TENANT_SHARED rollup, got: {other:?}"),
    };

    let row = f
        .admin
        .query_one(
            "SELECT r.visibility_class, r.visibility_workspace_id, \
                    (SELECT array_agg(i.memory_id ORDER BY i.ordinal) FROM private.memory_consolidation_inputs i WHERE i.run_id = r.run_id) AS inputs, \
                    (SELECT array_agg(s.memory_id) FROM private.memory_rollup_sources s WHERE s.rollup_id = r.rollup_id) AS source_memories, \
                    (SELECT array_agg(s.evidence_id) FROM private.memory_rollup_sources s WHERE s.rollup_id = r.rollup_id) AS source_evidence, \
                    (SELECT l.scope_kind FROM projection.stream_log l WHERE l.tenant_id = r.tenant_id ORDER BY l.commit_seq DESC LIMIT 1) AS ticket_scope \
             FROM private.memory_rollups r WHERE r.rollup_id = $1",
            &[&rollup_id],
        )
        .expect("rollup row");
    let visibility_class: String = row.get("visibility_class");
    let visibility_workspace_id: Option<Uuid> = row.get("visibility_workspace_id");
    let inputs: Vec<Uuid> = row.get("inputs");
    let source_memories: Vec<Uuid> = row.get("source_memories");
    let source_evidence: Vec<Uuid> = row.get("source_evidence");
    let ticket_scope: Option<String> = row.get("ticket_scope");
    assert_eq!(visibility_class, "TENANT_SHARED");
    assert_eq!(visibility_workspace_id, None);
    assert_eq!(
        inputs,
        vec![tenant_memory],
        "a tenant-scoped run must materialize only TENANT_SHARED memories — a WORKSPACE_SHARED \
         input here would be republished tenant-wide"
    );
    assert_eq!(source_memories, vec![tenant_memory]);
    assert_eq!(source_evidence, vec![tenant_evidence]);
    assert_eq!(ticket_scope.as_deref(), Some("tenant"));
    println!(
        "T6 ASSERTION LOG: rollup={rollup_id} visibility={visibility_class} workspace={visibility_workspace_id:?} \
         inputs={} (only tenant memory {tenant_memory}) ticket_scope={ticket_scope:?}",
        inputs.len()
    );
}

fn read_memory_snapshot(
    admin: &mut Client,
    memory_id: Uuid,
) -> (serde_json::Value, String, f32, String, String) {
    let row = admin
        .query_one(
            "SELECT content, authority_class, confidence, status, updated_at::text \
             FROM private.memory_records WHERE memory_id = $1",
            &[&memory_id],
        )
        .expect("read memory snapshot");
    (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4))
}

/// (T7) §6.1.3 / ADR-0028 (card 8): a rollup published by the real consolidation path inherits
/// its source memories' subject links through the ONE shared hook (`publish_rollup` →
/// `private.link_rollup_subjects`). FAULT SENTINEL: removing the `link_rollup_in_txn` call from
/// `consolidate_repo::publish_rollup` leaves `memory_rollup_subjects` empty and this exact-count
/// assertion goes red — the proof that the rollup path is covered, not only the Distill path.
#[test]
fn t7_rollup_inherits_subject_links_through_publish_rollup() {
    let Some(mut f) = setup_db("t7_rollup_inherits_subject_links_through_publish_rollup") else {
        return;
    };
    let (memory_id, evidence_id) = seed_workspace_memory(&mut f, "t7 memory about Babbage & Co");
    let org: Uuid = f
        .admin
        .query_one(
            "INSERT INTO private.subjects (tenant_id, kind, display_name) \
             VALUES ($1, 'ORGANISATION', 'Babbage & Co') RETURNING subject_id",
            &[&f.tenant_id],
        )
        .expect("seed org")
        .get(0);
    f.admin
        .execute(
            "SELECT private.link_memory_subjects($1, $2, ARRAY[$3]::uuid[], ARRAY['DECLARED']::text[])",
            &[&f.tenant_id, &memory_id, &org],
        )
        .expect("link the source memory");

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let consolidation_pool = rt
        .block_on(ConsolidationDbPool::connect(&dsn_as_role(
            &f.dsn,
            "role_consolidation_worker",
        )))
        .expect("consolidation pool");
    let outcome = rt.block_on(run_once(
        &consolidation_pool,
        &FakePort,
        f.tenant_id,
        f.reasoning_domain_id,
        ReasoningRouteBindingId(UNADMITTED_BINDING_ID),
        ReasoningRouteBindingVersion(1),
        Some(f.workspace_id),
        10_000,
        |ids, _result| {
            let content = serde_json::json!({"content": "Babbage & Co: consolidated view"});
            let sources = ids
                .iter()
                .map(|id| (*id, humaux_domain::authority::EvidenceId(evidence_id)))
                .collect();
            Ok((content, AuthorityClass::PrivateKnowledge, sources))
        },
    ));
    let rollup_id = match outcome {
        Ok(PublishOutcome::Published { rollup_id }) => rollup_id,
        other => panic!("T7 expected a published rollup, got: {other:?}"),
    };
    let rows: Vec<(Uuid, String)> = f
        .admin
        .query(
            "SELECT subject_id, source_kind FROM private.memory_rollup_subjects WHERE rollup_id = $1",
            &[&rollup_id],
        )
        .expect("read rollup subjects")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        rows,
        vec![(org, "INHERITED".to_owned())],
        "the rollup inherits exactly its source memory's subject as INHERITED"
    );
}
