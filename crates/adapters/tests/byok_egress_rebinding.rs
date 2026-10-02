//! `adapters::tests::byok_egress_rebinding` — ADR-0039 / §11.4 — BYOK 出网的「检查即拨号」验收：**同一套判定既跑在检查上也跑在拨号上**。
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-domain, humaux-infra-egress, tokio, uuid]; services=[]; env=[NO_PROXY, no_proxy]; modules=[adapters::byok, adapters::byok::ssrf, domain::dataclass, domain::egress, domain::ids, infra-egress::resolver]
//! Called-by: [cargo-test]
//! Invariants: [the rebinding resolver answers public first, loopback second; the listener on the forbidden address
//!   must receive zero connections, so the refusal happens before TCP connect, not in a log line]
//! Spec: Baseline §11.4
//!
//! 卡 17 之前的形态：`OpenAiCompatibleProvider::new` 用调用方注入的 `ssrf::DnsResolver` 跑
//! `validate_custom_endpoint`（检查），`EgressHttpTransport` 底下的 `RawHttpPost` 用**系统
//! DNS** 拨号——两次独立解析，答案可以不同。一个域名第一次解析给公网地址骗过检查、第二次
//! 解析改指 `127.0.0.1` / `169.254.169.254`，请求就会带着用户的 provider API key 打到那里。
//!
//! 本文件测的就是这个形态：`RebindingResolver` 第一次给公网地址（检查过），之后给回环地址
//! （§11.4 禁止段）。判据不是「日志里有一条拒绝」，而是**被拒地址上的真监听器一个连接都收
//! 不到**——拒绝必须发生在 TCP 建连之前。

use std::net::{IpAddr, Ipv4Addr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use humaux_adapters::byok::{
    CredentialDecryptor, CredentialRef, EgressHttpTransport, HeaderValue, OpenAiCompatTransport,
    OpenAiCompatibleProvider, OpenAiHttpRequest, OutputChannel, PlaintextApiKey,
    PrivateInferenceContext, ReasoningCapability, ReasoningDomainId, ReasoningProviderDescriptor,
    ReasoningProviderError, SsrfCheckedResolver, StructuredReasoningRequest, UserReasoningProvider,
    ssrf, structured_request_body,
};
use humaux_domain::dataclass::DataClass;
use humaux_domain::egress::{AuthorizedEgressPayload, PrivateDataPurpose, ProcessorId, authorize};
use humaux_domain::ids::{TenantId, UserId};
use humaux_infra_egress::resolver::CheckedDnsResolve;
use uuid::Uuid;

/// 一个公网地址：`is_forbidden_ip` 判它合法，所以「检查」这一跳会过。
const PUBLIC: IpAddr = IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34));

/// 会 rebinding 的 resolver：第 1 次（检查）给公网地址，第 2 次起（拨号）给回环地址。
struct RebindingResolver {
    calls: Arc<AtomicUsize>,
}

impl ssrf::DnsResolver for RebindingResolver {
    fn resolve(&self, _host: &str) -> Result<Vec<IpAddr>, ssrf::SsrfError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(vec![PUBLIC])
        } else {
            Ok(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        }
    }
}

/// 起一个真监听器：接到任何连接就把 flag 置位。被拒的地址上放这个，才能证明「拒绝」不是
/// 「连上之后才拒」。
fn listener_that_records(saw: Arc<AtomicBool>) -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind loopback");
    let port = listener.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            if stream.is_ok() {
                saw.store(true, Ordering::SeqCst);
            }
        }
    });
    port
}

/// 本进程配置的 egress 代理会不会拦下发往 `host` 的 HTTPS 请求。
///
/// 代理在场时解析发生在**代理侧**，client 的 resolver 不参与拨号（ADR-0039 记的残余天花板），
/// 所以下面那条测试的 typed-refusal 断言在这种环境里测不到——判据必须知道自己站在哪种环境里，
/// 而不是把环境事实当成实现缺陷。`/tests/` 目录读 env 是 §78 boundary lint 的既有豁免面。
fn env_proxy_intercepts(host: &str) -> bool {
    let proxy_set = ["HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"]
        .iter()
        .any(|k| std::env::var(k).is_ok_and(|v| !v.trim().is_empty()));
    if !proxy_set {
        return false;
    }
    let no_proxy = std::env::var("NO_PROXY")
        .or_else(|_| std::env::var("no_proxy"))
        .unwrap_or_default();
    let bypassed = no_proxy.split(',').map(str::trim).any(|e| {
        !e.is_empty()
            && (e == "*"
                || e.eq_ignore_ascii_case(host)
                || host.to_ascii_lowercase().ends_with(&e.to_ascii_lowercase()))
    });
    !bypassed
}

/// **卡 17 的验收判据**：检查这一跳解析到允许地址（provider 构造成功），拨号那一跳解析到
/// §11.4 禁止段 ⇒ 必须在建连之前被拒，且被拒地址上的监听器收不到任何连接。
#[tokio::test]
async fn a_rebound_endpoint_is_refused_before_the_connection_and_never_dialed() {
    let saw_connection = Arc::new(AtomicBool::new(false));
    let port = listener_that_records(saw_connection.clone());

    let calls = Arc::new(AtomicUsize::new(0));
    let resolver: Arc<dyn ssrf::DnsResolver> = Arc::new(RebindingResolver {
        calls: calls.clone(),
    });

    // 检查这一跳：§11.4 的 SSRF 闸拿到公网地址，端点被接受——provider 能被构造出来。
    let endpoint = format!("https://rebind.test:{port}/v1/chat/completions");
    let validated = ssrf::validate_custom_endpoint(&endpoint, resolver.as_ref())
        .expect("第一次解析给的是公网地址，检查必须过");
    assert_eq!(validated.resolved_ips, vec![PUBLIC]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // 拨号这一跳：同一个 resolver，第 2 次解析给回环地址。
    let transport =
        EgressHttpTransport::with_resolver(Duration::from_secs(5), resolver).expect("transport");
    let err = transport
        .send(
            OpenAiHttpRequest {
                url: endpoint,
                headers: vec![(
                    "Authorization".to_string(),
                    HeaderValue::new("Bearer not-a-real-key"),
                )],
                body: b"{}".to_vec(),
            },
            &ssrf::CustomEndpointPolicy::default(),
        )
        .await
        .expect_err("rebinding 到禁止段必须被拒");

    let ReasoningProviderError::Transport(msg) = &err else {
        panic!("期望 Transport 拒绝，实得 {err:?}");
    };
    if env_proxy_intercepts("rebind.test") {
        // 已知天花板（ADR-0039「Ceilings」/ `infra_egress::resolver` 模块 doc）：进程配了
        // egress 代理且目标不在 NO_PROXY 内时，`reqwest` 拨的是代理、`CONNECT` 的是主机名，
        // 解析发生在代理侧——本 client 的 resolver 根本不会被问到。这台机器就是这个形态。
        // 本条不静默放过：明确打印缺失对象，并且**仍然**断言被拒地址收不到连接。
        eprintln!(
            "PARTIAL a_rebound_endpoint_is_refused_before_the_connection_and_never_dialed: \
             missing object: 无代理的出网环境（HTTPS_PROXY/ALL_PROXY 已设且 rebind.test 不在 \
             NO_PROXY 内）——本环境下 resolver 不参与拨号，typed refusal 无法断言；\
             无代理环境下的同款断言见 humaux-infra-egress 的 \
             resolver::tests::a_rebound_second_answer_is_refused_before_any_connection。\
             实得: {msg}"
        );
    } else {
        assert!(
            msg.contains("egress policy refused"),
            "拒绝必须是一条能认回来的出网判定（不是被压平的网络错误）: {msg}"
        );
        assert!(
            msg.contains("127.0.0.1"),
            "理由里必须说清被拒的是哪个地址: {msg}"
        );
    }

    // `std::thread::sleep`（不是 `tokio::time`）：本 crate 的 dev-dependency 没开 tokio 的
    // "time" feature，而这里只需要让「如果真的连了」的那条连接有时间落到 accept 上。
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !saw_connection.load(Ordering::SeqCst),
        "REBOUND_ADDRESS_DIALED = true —— 回环地址上的监听器收到了连接，说明拒绝发生在建连之后"
    );
}

/// 检查与拨号跑的是**同一个** `is_forbidden_ip`：`SsrfCheckedResolver`（装进 client 的那个）
/// 对同一组地址给出的判定必须与 `validate_custom_endpoint` 逐条一致——两处判据一旦分叉，
/// 上面那条测试就只是碰巧还绿。
#[test]
fn the_dial_time_resolver_applies_the_same_policy_as_the_construction_time_check() {
    struct Fixed(Vec<IpAddr>);
    impl ssrf::DnsResolver for Fixed {
        fn resolve(&self, _host: &str) -> Result<Vec<IpAddr>, ssrf::SsrfError> {
            Ok(self.0.clone())
        }
    }

    for (addrs, allowed) in [
        (vec![PUBLIC], true),
        (vec![IpAddr::V4(Ipv4Addr::LOCALHOST)], false),
        (vec![IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254))], false),
        (vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7))], false),
        // 「有一个公网的就算过」是错的：任一地址被禁即整体拒，两处判据同款。
        (vec![PUBLIC, IpAddr::V4(Ipv4Addr::LOCALHOST)], false),
    ] {
        let inner: Arc<dyn ssrf::DnsResolver> = Arc::new(Fixed(addrs.clone()));
        let check = ssrf::validate_custom_endpoint("https://x.test/v1", inner.as_ref());
        let dial = SsrfCheckedResolver::new(inner).resolve_checked("x.test");
        assert_eq!(check.is_ok(), allowed, "检查侧对 {addrs:?} 判错了");
        assert_eq!(dial.is_ok(), allowed, "拨号侧对 {addrs:?} 判错了");
    }
}

/// 零地址不是「没地址所以随便连」——两处都必须拒。
#[test]
fn zero_addresses_is_refused_on_the_dial_path_too() {
    struct Empty;
    impl ssrf::DnsResolver for Empty {
        fn resolve(&self, _host: &str) -> Result<Vec<IpAddr>, ssrf::SsrfError> {
            Ok(Vec::new())
        }
    }
    let inner: Arc<dyn ssrf::DnsResolver> = Arc::new(Empty);
    assert!(ssrf::validate_custom_endpoint("https://x.test/v1", inner.as_ref()).is_err());
    assert!(
        SsrfCheckedResolver::new(inner)
            .resolve_checked("x.test")
            .is_err()
    );
}

/// 卡 17 复审的判据：**生产构造点自己**必须把同一个 resolver 同时交给检查腿和拨号腿。
///
/// 上面那条测试手工把 resolver 分别交给 `validate_custom_endpoint` 和
/// `EgressHttpTransport::with_resolver`——它证明的是「两处传同一个时机制成立」，证明不了
/// 「生产真的传了同一个」。复审抓到的 P0 恰恰长在这条缝里：`bins/private-worker` 把运维配的
/// DNS pin 只交给了检查，拨号仍走系统 DNS，判据1–4 全绿。这条测试走
/// [`OpenAiCompatibleProvider::with_egress_transport`]——生产唯一入口，resolver 只有一个实参
/// ——「传两个不同的」在这里无法表达；rebinding 仍必须在建连之前被拒。
#[tokio::test]
async fn the_production_constructor_hands_one_resolver_to_both_legs() {
    struct NoKey;
    #[async_trait::async_trait]
    impl CredentialDecryptor for NoKey {
        async fn resolve(
            &self,
            _credential_ref: CredentialRef,
        ) -> Result<PlaintextApiKey, ReasoningProviderError> {
            Ok(PlaintextApiKey::new("not-a-real-key".to_string()))
        }
    }

    let saw_connection = Arc::new(AtomicBool::new(false));
    let port = listener_that_records(saw_connection.clone());

    let calls = Arc::new(AtomicUsize::new(0));
    let resolver: Arc<dyn ssrf::DnsResolver> = Arc::new(RebindingResolver {
        calls: calls.clone(),
    });

    let descriptor = ReasoningProviderDescriptor {
        provider_id: "rebind".to_string(),
        model_id: "rebind-1".to_string(),
        model_revision: None,
        capabilities: vec![ReasoningCapability::StructuredOutput],
        custom_endpoint: Some(format!("https://rebind.test:{port}/v1/chat/completions")),
    };

    // resolver 只传一次。构造成功 = 检查腿用它解析到公网地址并放行。
    let provider = OpenAiCompatibleProvider::with_egress_transport(
        descriptor.clone(),
        format!("https://rebind.test:{port}/v1/chat/completions"),
        Duration::from_secs(5),
        NoKey,
        ssrf::CustomEndpointPolicy::default(),
        Arc::clone(&resolver),
    )
    .expect("第一次解析给的是公网地址，§11.4 检查必须过");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "构造期只该发生一次解析（检查腿）"
    );

    let request = StructuredReasoningRequest {
        system_prompt: "s".to_string(),
        user_prompt: "u".to_string(),
        json_schema: "{}".to_string(),
        max_output_tokens: 8,
        output: OutputChannel::Content,
    };
    // permit 与 provider 将要发送的字节同源（手抄必漂移 ⇒ EgressPermitPayloadMismatch，
    // 那会在拨号之前就返回，测不到这条判据）。
    let payload = AuthorizedEgressPayload::new(structured_request_body(&descriptor, &request));
    let tenant = TenantId(Uuid::now_v7());
    let mint = || {
        authorize(
            tenant,
            ProcessorId(Uuid::now_v7()),
            PrivateDataPurpose::UserReasoning,
            DataClass::Private,
            &payload,
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
        "rebind",
        "rebind-1",
        1,
        "card17-production-constructor".to_string(),
    )
    .expect("inference context");

    // 拨号腿：同一个 resolver 的第 2 次解析给回环地址 ⇒ 必须在建连之前被拒。
    let err = provider
        .complete_structured(&ctx, request)
        .await
        .expect_err("rebinding 到禁止段必须被拒——若 Ok，说明拨号走的不是被检查的那个 resolver");
    let ReasoningProviderError::Transport(msg) = &err else {
        panic!("期望 Transport 拒绝（permit/凭证类错误说明根本没走到拨号）: {err:?}");
    };
    if env_proxy_intercepts("rebind.test") {
        // 与上一条测试同款的环境天花板，同款不静默：仍然断言被拒地址收不到连接。
        eprintln!(
            "PARTIAL the_production_constructor_hands_one_resolver_to_both_legs: \
             missing object: 无代理的出网环境——resolver 不参与拨号，typed refusal 无法断言。\
             实得: {msg}"
        );
    } else {
        assert!(
            msg.contains("egress policy refused") && msg.contains("127.0.0.1"),
            "拒绝必须是一条能认回来的出网判定，并说清被拒地址: {msg}"
        );
    }

    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !saw_connection.load(Ordering::SeqCst),
        "REBOUND_ADDRESS_DIALED = true —— 生产构造点造出来的 client 连上了被 rebind 的地址"
    );
}
