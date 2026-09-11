//! §11 BYOK 域的 live 冒烟：MiniMax M3 经 `OpenAiCompatibleProvider` + `EgressHttpTransport`
//! 真打一次，全链 = 铸 permit → 披露 reserve → 外呼 → 披露 finalize → 断言。
//!
//! 与 §19 的 `dashscope_live_smoke` 是姊妹：那条走 PlatformManaged 域
//! （`HttpExternalCall`，401 = `Unauthorized`），本条走 BYOK 域（明文 key 唯一展开点在
//! `build_openai_request`，401/1004 = `WaitingKey`，§11.3）。**刻意不复用**那边的
//! `EnvCredentialSource`/transport——复用会同时破坏 §11.1 的 key 单点与 §11.3 的语义。
//!
//! 四态：
//! 1. key 缺（env 与 .env 双探空）⇒ 可见 SKIP（`ExternalDep::MiniMax`）；
//! 2. key 在、库缺 ⇒ 可见 SKIP（披露半边需要真库）；
//! 3. `HUMAUX_REQUIRE_MINIMAX=1` / `HUMAUX_REQUIRE_DB=1` 而对应依赖缺 ⇒ panic（ADR-0005）；
//! 4. 全在 ⇒ 真打，Err 即 panic（fail-loud），Ok 则逐面断言。
//!
//! endpoint 用 `/v1/chat/completions`（OpenAI 兼容形状）——**实测**（2026-08-27 探针：
//! HTTP 200、choices[0].message.content 带 `<think>`、usage 带 completion_tokens_details），
//! 与 provider 发出的请求形状精确匹配；`chatcompletion_v2` 是 MiniMax 旧原生形状，不用。

use std::sync::Arc;
use std::time::Duration;

use humaux_adapters::byok::{
    CredentialDecryptor, CredentialRef, OpenAiCompatibleProvider, PlaintextApiKey,
    PrivateInferenceContext, ReasoningCapability, ReasoningDomainId, ReasoningProviderDescriptor,
    ReasoningProviderError, StructuredReasoningRequest, StructuredReasoningResponse,
    UserReasoningProvider, ssrf, structured_request_body,
};
use humaux_adapters::disclosure::{
    DeletionCapability, DisclosureOutcome, DisclosureSource, finalize_private, reserve_private,
};
use humaux_adapters::postgres::PrivateWorkerDbPool;
use humaux_domain::dataclass::DataClass;
use humaux_domain::egress::{
    AuthorizedEgressPayload, EgressPermit, PrivateDataPurpose, ProcessorId, authorize,
};
use humaux_domain::ids::{TenantId, UserId};
use humaux_testkit::{DISCLOSURE_LEDGER_ADVISORY_LOCK, ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use uuid::Uuid;

const NAME: &str = "minimax_live_smoke";
const MINIMAX_CHAT_URL: &str = "https://api.minimaxi.com/v1/chat/completions";
const MINIMAX_MODEL: &str = "MiniMax-M3";

/// ① `MINIMAX_API_KEY` 环境变量（空串视为缺失，同生产语义）；② 回退逐行解析
/// `/Volumes/data/viral-skill-eval/.env`：trim、容忍 `export ` 前缀、去引号。
/// **值零 println、零入 panic 消息**——密钥不进任何输出。
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

/// test-only 凭证解密器：持有 key 材料，`resolve` 原样交出。生产路径是 OpenBao
/// （`openbao.rs` 仍占位——那是这条链上唯一的 NA，缺失对象名就是它）。
/// **刻意不 derive Debug**：一个带 Debug 的持 key 结构就是一条泄漏通道。
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

/// 恒失败的 transport——G5（披露失败路径）用，不需要 key 也不出网。
struct AlwaysFailTransport;
#[async_trait::async_trait]
impl humaux_adapters::byok::OpenAiCompatTransport for AlwaysFailTransport {
    async fn send(
        &self,
        _request: humaux_adapters::byok::OpenAiHttpRequest,
        _policy: &ssrf::CustomEndpointPolicy,
    ) -> Result<humaux_adapters::byok::OpenAiHttpOutcome, ReasoningProviderError> {
        Err(ReasoningProviderError::Transport(
            "always-fail transport (G5 fixture)".to_string(),
        ))
    }
}

/// 固定公网 IP 的 resolver——**不是**在绕 SSRF 闸，是闸抓到了一个环境事实：
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

#[test]
fn dsn_as_role_accepts_both_postgres_uri_schemes() {
    let postgres = dsn_as_role(
        "postgres://postgres@127.0.0.1:54329/test",
        "role_private_worker",
    );
    let postgresql = dsn_as_role(
        "postgresql://postgres@127.0.0.1:54329/test",
        "role_private_worker",
    );

    assert_eq!(postgres, postgresql);
    assert!(postgres.starts_with("postgres://role_private_worker:"));
}

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

struct Fixture {
    admin: Client,
    tenant_id: Uuid,
    evidence_id: Uuid,
    dsn: String,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // `ops.data_disclosures` 是 append-only（§7.4），其 FK 的 tenants 行删不掉——
        // 与 disclosure_ledger.rs 自陈的 permanent-leak 形状一致，只清能清的。
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM private.events WHERE event_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{0}'); \
             DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
             DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}';",
            self.tenant_id
        ));
    }
}

/// 建 DB fixture。`None` = 已打印 SKIP（或声明模式下已 panic）。
fn setup_db() -> Option<Fixture> {
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(
            NAME,
            "missing object: HUMAUX_TEST_PG_DSN（披露半边需要真库）",
            ExternalDep::Postgres,
        );
        return None;
    };
    let Ok(mut admin) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(NAME, "missing object: live Postgres", ExternalDep::Postgres);
        return None;
    };
    // 与 disclosure_ledger.rs 的 TRUNCATE 排他互斥（40P01 实测教训）。
    admin
        .execute(
            "SELECT pg_advisory_lock_shared($1)",
            &[&DISCLOSURE_LEDGER_ADVISORY_LOCK],
        )
        .ok()?;

    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
            &[&"minimax_live_smoke throwaway tenant"],
        )
        .ok()?
        .get(0);
    let reasoning_domain_id: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
             VALUES ($1, 'minimax_live_smoke domain') RETURNING reasoning_domain_id",
            &[&tenant_id],
        )
        .ok()?
        .get(0);
    let evidence_id: Uuid = admin
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', $2, 'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $3) \
             RETURNING evidence_id",
            &[&tenant_id, &vec![0u8; 32], &reasoning_domain_id],
        )
        .ok()?
        .get(0);
    admin
        .execute(
            "INSERT INTO private.events (event_id, event_kind, payload) \
             VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
            &[&evidence_id],
        )
        .ok()?;

    Some(Fixture {
        admin,
        tenant_id,
        evidence_id,
        dsn,
    })
}

/// 铸 permit + 建 ctx。`EgressPermit` **刻意**不 Clone（单一所有权纪律，见它的 rustdoc），
/// 所以对同一份 payload 铸两张：一张移入 ctx（transport 侧对精确字节做 sha256 校验），
/// 一张交回给披露 reserve/finalize。两张的 tenant/purpose/payload 逐字相同，语义等价。
fn permit_and_ctx(
    tenant: TenantId,
    payload: &AuthorizedEgressPayload,
    trace: &str,
) -> (EgressPermit, PrivateInferenceContext) {
    let mint = || {
        authorize(
            tenant,
            ProcessorId(Uuid::now_v7()),
            PrivateDataPurpose::UserReasoning,
            DataClass::Private,
            payload,
            Duration::from_secs(30),
        )
        .expect("mint egress permit (§7.3)")
    };
    let ctx = PrivateInferenceContext::new(
        tenant,
        UserId::new(),
        ReasoningDomainId(Uuid::now_v7()),
        CredentialRef::new(Uuid::now_v7()),
        mint(),
        "minimax",
        MINIMAX_MODEL,
        1,
        trace.to_string(),
    )
    .expect("inference context");
    (mint(), ctx)
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

/// reserve → call → **无条件** finalize。live 路径与 G5 失败路径共用——
/// 「成败都落披露账」由这一个函数保证，不是两处各自记得。
async fn call_with_disclosure(
    provider: &dyn UserReasoningProvider,
    ctx: &PrivateInferenceContext,
    pool: &PrivateWorkerDbPool,
    permit: &EgressPermit,
    payload: &AuthorizedEgressPayload,
    evidence_id: Uuid,
    request: StructuredReasoningRequest,
) -> (
    Uuid,
    Result<StructuredReasoningResponse, ReasoningProviderError>,
) {
    let disclosure_id = reserve_private(
        pool,
        permit,
        "cn-shanghai",
        payload,
        None,
        &[DisclosureSource::Evidence(evidence_id)],
    )
    .await
    .expect("reserve disclosure before the external call (§7.4)");

    let result = provider.complete_structured(ctx, request).await;

    let outcome = if result.is_ok() {
        DisclosureOutcome::Success
    } else {
        DisclosureOutcome::Failed
    };
    finalize_private(
        pool,
        permit.tenant_id().0,
        disclosure_id,
        outcome,
        DeletionCapability::Unknown,
    )
    .await
    .expect("finalize disclosure after the external call — success or failure alike (§7.4)");

    (disclosure_id, result)
}

/// 主冒烟：全链真打。
#[test]
fn minimax_live_smoke() {
    let Some(key) = load_minimax_key() else {
        skip_or_fail(
            NAME,
            "missing object: MINIMAX_API_KEY",
            ExternalDep::MiniMax,
        );
        return;
    };
    let Some(f) = setup_db() else { return };

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let rt_result = rt.block_on(async {
        let pool = PrivateWorkerDbPool::connect(&dsn_as_role(&f.dsn, "role_private_worker"))
            .await
            .expect("private worker pool");

        let desc = descriptor();
        let request = StructuredReasoningRequest {
            system_prompt: "You are a JSON-only assistant. Reply with JSON only, no prose."
                .to_string(),
            user_prompt: "Reply with exactly this JSON object: {\"answer\": 4}".to_string(),
            json_schema: r#"{"type":"object","properties":{"answer":{"type":"integer"}}}"#
                .to_string(),
            // M3 的 reasoning 计入 completion 预算（实测：8 个 token 全被思考吃掉、
            // answer 为空而 HTTP 200）——给足余量。
            max_output_tokens: 2048,
        };

        // 铸 permit：与 provider 内部将要发送的字节**同源**（同一个 pub 函数算出来），
        // 手抄必漂移 → EgressPermitPayloadMismatch。
        let body = structured_request_body(&desc, &request);
        let payload = AuthorizedEgressPayload::new(body);
        let tenant = TenantId(f.tenant_id);
        let (permit, ctx) =
            permit_and_ctx(tenant, &payload, &format!("minimax-smoke-{}", Uuid::now_v7()));

        let provider = OpenAiCompatibleProvider::with_egress_transport(
            desc,
            MINIMAX_CHAT_URL.to_string(),
            Duration::from_secs(30),
            EnvKeyDecryptor { key_material: key },
            ssrf::CustomEndpointPolicy::default(),
            live_dns_resolver(),
        )
        .expect("SSRF choke point must accept the endpoint (see live_dns_resolver / HUMAUX_MINIMAX_DNS_PINS)");

        let (disclosure_id, result) = call_with_disclosure(
            &provider,
            &ctx,
            &pool,
            &permit,
            &payload,
            f.evidence_id,
            request,
        )
        .await;

        // fail-loud：走到这儿说明 key 与库都在，Err 是真回归（dashscope 同款纪律）。
        let resp = result.unwrap_or_else(|e| {
            panic!(
                "{NAME}: call failed ({e:?}) despite a live MINIMAX_API_KEY and a reachable \
                 DB — permit/disclosure sequence completed, so this is a real regression \
                 (endpoint, envelope parsing, or base_resp classification)"
            )
        });

        // 断言面：剥净、可解析、usage 真实。
        assert!(
            !resp.json.contains("<think>"),
            "结构化输出里不得残留思考块: {}",
            resp.json
        );
        assert!(!resp.json.trim().is_empty(), "剥后不得为空");
        let v: serde_json::Value = serde_json::from_str(&resp.json).expect("剥后必须是合法 JSON");
        assert_eq!(
            v.get("answer").and_then(serde_json::Value::as_i64),
            Some(4),
            "模型没按要求返回 {{\"answer\": 4}}: {}",
            resp.json
        );
        assert!(
            resp.usage.input_tokens.unwrap_or(0) > 0 && resp.usage.output_tokens.unwrap_or(0) > 0,
            "usage 必须来自真实响应而不是 default: {:?}",
            resp.usage
        );
        // reasoning/cached 两个字段随 MiniMax 版本可能变——log-only，不硬断。
        eprintln!(
            "{NAME}: reasoning_tokens={:?} cached_input_tokens={:?}",
            resp.usage.reasoning_tokens, resp.usage.cached_input_tokens
        );

        disclosure_id
    });

    // 披露行校验在 async 块**外**：同步 `postgres::Client` 内部自起 tokio runtime，
    // 放在 block_on 里是 runtime 套 runtime，当场 panic（实测踩过）。
    let disclosure_id = rt_result;
    let mut admin2 = Client::connect(&f.dsn, NoTls).expect("verify conn");
    let row = admin2
        .query_one(
            "SELECT finalized_at IS NOT NULL, outcome::text \
             FROM ops.data_disclosures WHERE disclosure_id = $1",
            &[&disclosure_id],
        )
        .expect("disclosure row must exist");
    let (finalized, outcome): (bool, String) = (row.get(0), row.get(1));
    assert!(finalized, "披露行必须已 finalize（§7.4「每一次，不采样」）");
    assert_eq!(outcome, "SUCCESS");
}

/// G5：**失败也落披露账**。恒失败 transport（不出网、不需 key），断言披露行
/// `outcome='FAILED'` 且已 finalize。
/// 注错：删掉 `call_with_disclosure` 的无条件 finalize ⇒ finalized_at NULL ⇒ 红。
#[test]
fn g5_a_failed_call_still_finalizes_its_disclosure_row() {
    let Some(f) = setup_db() else { return };

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let rt_result = rt.block_on(async {
        let pool = PrivateWorkerDbPool::connect(&dsn_as_role(&f.dsn, "role_private_worker"))
            .await
            .expect("private worker pool");

        let desc = descriptor();
        let request = StructuredReasoningRequest {
            system_prompt: "s".to_string(),
            user_prompt: "u".to_string(),
            json_schema: "{}".to_string(),
            max_output_tokens: 8,
        };
        let body = structured_request_body(&desc, &request);
        let payload = AuthorizedEgressPayload::new(body);
        let tenant = TenantId(f.tenant_id);
        let (permit, ctx) = permit_and_ctx(tenant, &payload, "g5-trace");

        let provider = OpenAiCompatibleProvider::new(
            desc,
            MINIMAX_CHAT_URL.to_string(),
            AlwaysFailTransport,
            EnvKeyDecryptor {
                key_material: "not-a-real-key-g5-fixture".to_string(),
            },
            ssrf::CustomEndpointPolicy::default(),
            live_dns_resolver().as_ref(),
        )
        .expect("provider");

        let (disclosure_id, result) = call_with_disclosure(
            &provider,
            &ctx,
            &pool,
            &permit,
            &payload,
            f.evidence_id,
            request,
        )
        .await;
        assert!(result.is_err(), "恒失败 transport 必须失败");
        disclosure_id
    });

    // 同主冒烟：同步 Client 必须在 block_on 之外。
    let disclosure_id = rt_result;
    let mut admin2 = Client::connect(&f.dsn, NoTls).expect("verify conn");
    let row = admin2
        .query_one(
            "SELECT finalized_at IS NOT NULL, outcome::text \
             FROM ops.data_disclosures WHERE disclosure_id = $1",
            &[&disclosure_id],
        )
        .expect("disclosure row must exist");
    let (finalized, outcome): (bool, String) = (row.get(0), row.get(1));
    assert!(
        finalized,
        "失败的调用同样必须 finalize——「成败都落账」不是注释是判据（§7.4）"
    );
    assert_eq!(outcome, "FAILED");
}
