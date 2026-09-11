//! `infra_egress::resolver` — Layer 1A 的「检查即拨号」解析器：**出网 client 的唯一构造点**。
//!
//! ## 这个模块存在的理由（ADR-0039 / §11.4 / §83.4 判据3）
//!
//! 在此之前，Layer 1A 的两条出口（[`crate::raw::RawHttpPost`] = BYOK 域、
//! [`crate::http::HttpExternalCall`] = PlatformManaged 域）都经 `build_client` 构造 client，
//! 也就是**用系统 DNS 拨号**；而 §11.4 的 SSRF 闸（`adapters::byok::ssrf::
//! validate_custom_endpoint`）用的是**调用方注入的 resolver**。两次解析、两个答案——
//! 检查通过的地址和真正连上去的地址之间没有任何结构性联系，这正是 OWASP SSRF Cheat Sheet
//! 里的 DNS rebinding / TOCTOU pinning bypass：第一次解析返回一个公网地址骗过检查，
//! 第二次（连接时那次）返回 `169.254.169.254`。而这条路上跑的是用户的 provider API key。
//!
//! 修法不是「在拨号前再检查一次」（那只是把窗口挪小），而是**让检查与拨号是同一次解析**：
//! 调用方的已检查 resolver 被装成 `reqwest::Client` 的**唯一** DNS resolver
//! （`humaux_infra_network::http::build_client_with_resolver`，Layer 1B 的
//! `infra-cell::transport` 早已是这么做的），client 的连接器不会再自己查一次名字。
//! 拒绝发生在**连接建立之前**，表现为一次 `reqwest` 连接错误，而不是一行日志。
//!
//! ## 纪律
//!
//! - `humaux_infra_network::http` 只导出 `build_client_with_resolver` 一个构造函数
//!   （无 resolver 的 `build_client` 已删除）——语言层面已经没有「不经检查的 client」
//!   这种东西可造；`xtask architecture-check` 的
//!   「outbound client dials only through the checked resolver」闸再把
//!   `build_client_with_resolver(` 的调用点集合钉死为
//!   `{crates/infra-egress/src/resolver.rs, crates/infra-cell/src/transport.rs}`。
//! - 本模块**不带任何地址策略**：允许/禁止哪些地址是调用方的语义（§11.4 的
//!   `is_forbidden_ip` 住在 `adapters::byok::ssrf`，Cell CIDR 住在 `infra-cell`），
//!   本模块只保证「调用方判过的那组地址，就是连上去的那组地址」。
//! - 天花板（诚实记账）：进程环境里配了 `HTTP_PROXY`/`HTTPS_PROXY` 且目标不在 `NO_PROXY`
//!   里时，**解析发生在代理侧**，`reqwest` 根本不会问本 resolver——这条 pin 在那种部署下
//!   不成立（见 `ClientConfig::trust_env_proxy` 自己的 doc）。Layer 1A 仍然默认尊重代理
//!   （组织的出网代理是合法路径），所以这是部署形态决定的残余面，不是本层能关掉的洞。

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use humaux_infra_network::http::{ClientConfig, build_client_with_resolver};
use humaux_infra_network::reqwest;

/// 调用方的「解析 + 判定」：返回**已经通过调用方出网策略**的地址集合。
///
/// 返回 `Err(reason)` 即拒绝——[`PinnedResolver`] 会把它变成一次连接前的失败，
/// `reason` 原样带到 [`EgressResolutionRefused`] 里（不含凭据：本 trait 只看主机名）。
///
/// 签名刻意只用 `std` 类型：`adapters` 侧实现它时完全不触 `infra-network` 的类型
/// （G80-3 `g80_3_infra_network_dependents_check` 把依赖方钉死为
/// `{infra-egress, infra-cell}`，`adapters` 加那条依赖当场红）。
pub trait CheckedDnsResolve: Send + Sync {
    /// 解析 `host`（不含端口）并对结果套用调用方自己的出网策略。
    ///
    /// # Errors
    /// 解析失败或任一地址被策略拒绝 ⇒ `Err(理由)`。
    fn resolve_checked(&self, host: &str) -> Result<Vec<IpAddr>, String>;
}

/// 无附加策略的系统解析器：`std::net::ToSocketAddrs`，与
/// `adapters::byok::ssrf::SystemDnsResolver` / `infra-cell::transport::SystemDnsResolve`
/// 同款 stdlib-only 做法。
///
/// 「无策略」是刻意的：[`crate::http::HttpExternalCall`] 打的是**运营方配置的** provider
/// 端点（DashScope 之类），不是用户提交的 URL——§11.4 的私网/保留段禁令是 BYOK 自定义端点
/// 的语义，套到平台端点上会把本 crate 自己的 loopback 测试夹具一起拒掉。它带来的收益是
/// pin 本身：一次解析，连上去的就是那次解析的答案。带策略的版本由调用方实现本 trait 提供
/// （BYOK 侧即 `adapters::byok::SsrfCheckedResolver`）。
pub struct SystemCheckedResolve;

impl CheckedDnsResolve for SystemCheckedResolve {
    fn resolve_checked(&self, host: &str) -> Result<Vec<IpAddr>, String> {
        // 端口 0 是占位——`ToSocketAddrs` 要端口才肯查，本调用只要 IP。
        (host, 0u16)
            .to_socket_addrs()
            .map(|it| it.map(|sa| sa.ip()).collect())
            .map_err(|_| format!("DNS resolution failed for {host}"))
    }
}

/// 连接前的拒绝，作为 `reqwest` 错误链上的 source 抛出。
///
/// [`crate::raw::RawHttpPost`] 用 [`refusal_in_error_chain`] 从 `reqwest::Error` 上把它捞
/// 回来，这样调用方拿到的是「出网策略拒了 `<host>`：`<reason>`」这句话，而不是被压平成
/// 一条看不出所以然的连接错误——「拒了」和「网络刚好不通」必须能分开。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EgressResolutionRefused {
    /// 被拒的主机名（小写化后的形式，与 resolver 实际查的键一致）。
    pub host: String,
    /// 调用方给的理由。
    pub reason: String,
}

impl std::fmt::Display for EgressResolutionRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "egress policy refused {}: {}", self.host, self.reason)
    }
}

impl std::error::Error for EgressResolutionRefused {}

/// 在 `reqwest::Error` 的 source 链上找回 [`PinnedResolver`] 抛出的判定——照抄
/// `infra-cell::transport::downcast_intra_cell_error` 的形状（同一个问题：`reqwest`
/// 把 resolver 的错误包在连接错误里）。
#[must_use]
pub fn refusal_in_error_chain(err: &reqwest::Error) -> Option<EgressResolutionRefused> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(err);
    while let Some(e) = source {
        if let Some(refused) = e.downcast_ref::<EgressResolutionRefused>() {
            return Some(refused.clone());
        }
        source = e.source();
    }
    None
}

/// §78.1：一个有名字、可覆写的旋钮，不是埋在构造里的字面量。
///
/// 60 秒 = 「检查过的答案」在一个 client 实例上可以复用多久。它同时是两件事的上限：
/// pin 的**新鲜度**（真实的 DNS 变更最迟 60s 后被看到）和**每请求一次 getaddrinfo 的
/// 成本**（card 17 的速度判据：pin 不许给每个请求加一次解析）。
pub const DEFAULT_PIN_TTL: Duration = Duration::from_secs(60);

/// 把调用方的 [`CheckedDnsResolve`] 装成 `reqwest` 的**唯一** DNS resolver。
///
/// 每个主机名在一个 TTL 窗口内只查一次：命中缓存的请求一次 `getaddrinfo` 都不做
/// （速度判据），过期后重查并重新过一遍调用方的策略（安全判据：pin 不是「永远相信第一次
/// 的答案」，rebinding 换到禁止地址会在下一次重查时被拒）。
/// 主机名 -> （这条 pin 是什么时候取的，取到的是哪些地址）。
type PinCache = Arc<Mutex<HashMap<String, (Instant, Vec<IpAddr>)>>>;

pub struct PinnedResolver {
    inner: Arc<dyn CheckedDnsResolve>,
    ttl: Duration,
    // ponytail: 无上限的 map，按主机名增长——一个 client 实例的目标主机数以个位数计
    // （每个 provider 实例一个端点）。真出现多租户共享一个 client 打上千个主机名时，
    // 升级路径是换成带容量上限的 LRU。
    cache: PinCache,
}

impl PinnedResolver {
    /// `ttl` 见 [`DEFAULT_PIN_TTL`]。`ttl` 为零表示每次连接都重新解析+重新判定。
    #[must_use]
    pub fn new(inner: Arc<dyn CheckedDnsResolve>, ttl: Duration) -> Self {
        Self {
            inner,
            ttl,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 未过期的 pin；`None` 表示要重查。
    fn cached(&self, host: &str, now: Instant) -> Option<Vec<IpAddr>> {
        let guard = self.cache.lock().ok()?;
        let (at, addrs) = guard.get(host)?;
        (now.duration_since(*at) < self.ttl).then(|| addrs.clone())
    }
}

fn refused(host: &str, reason: String) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(EgressResolutionRefused {
        host: host.to_string(),
        reason,
    })
}

fn as_addrs(addrs: Vec<IpAddr>) -> reqwest::dns::Addrs {
    // 端口 0 会被丢弃：`reqwest::dns::Resolve` 的契约是 URL 里的端口覆盖解析结果携带的端口
    // （`infra-cell::transport::ValidatingResolver` 同一条注释）。
    Box::new(addrs.into_iter().map(|ip| SocketAddr::new(ip, 0)))
}

impl reqwest::dns::Resolve for PinnedResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        // `url::Url` 在 `reqwest` 内部就把 authority 小写化了；这里再小写一次，缓存键与
        // 调用方看到的 host 才始终是同一个形式。
        let host = name.as_str().to_ascii_lowercase();
        if let Some(pinned) = self.cached(&host, Instant::now()) {
            // 命中 pin：不落 blocking 线程，一次系统调用都不做。
            return Box::pin(std::future::ready(Ok(as_addrs(pinned))));
        }
        let inner = self.inner.clone();
        let cache = self.cache.clone();
        Box::pin(async move {
            let lookup_host = host.clone();
            // 注入的 resolver 可能是阻塞的（`SystemCheckedResolve` 的 `ToSocketAddrs`、
            // `adapters::byok::ssrf::SystemDnsResolver` 同理）——`reqwest::dns::Resolve::
            // resolve` 是在调用方的 async runtime 上被 poll 的，阻塞系统调用放在那里会把它
            // 卡住。与 `infra-cell::transport::ValidatingResolver` 同款处理。
            let addrs = tokio::task::spawn_blocking(move || inner.resolve_checked(&lookup_host))
                .await
                .map_err(|e| refused(&host, format!("resolver task did not complete: {e}")))?
                .map_err(|reason| refused(&host, reason))?;
            if addrs.is_empty() {
                return Err(refused(&host, "resolved to zero addresses".to_string()));
            }
            if let Ok(mut guard) = cache.lock() {
                guard.insert(host, (Instant::now(), addrs.clone()));
            }
            Ok(as_addrs(addrs))
        })
    }
}

/// **Layer 1A 出网 client 的唯一构造点**：调用方的已检查 resolver 装进 client，
/// 检查过的地址就是连上去的地址。
///
/// # Errors
/// TLS 后端初始化失败（`build_client_with_resolver` 自己的唯一失败模式，进程启动期的
/// 配置错误，不是每次调用会遇到的）。
pub fn build_pinned_client(
    config: ClientConfig,
    resolver: Arc<dyn CheckedDnsResolve>,
    ttl: Duration,
) -> Result<reqwest::Client, reqwest::Error> {
    build_client_with_resolver(config, Arc::new(PinnedResolver::new(resolver, ttl)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// 一个「会 rebinding 的」测试 resolver：第一次查返回 `first`，之后返回 `then`
    /// （`Ok` 或 `Err`）。真实攻击就是这个形状：第一次的答案骗过检查，第二次的答案是目标。
    struct RebindingResolver {
        first: Vec<IpAddr>,
        then: Result<Vec<IpAddr>, String>,
        calls: Arc<AtomicUsize>,
    }

    impl CheckedDnsResolve for RebindingResolver {
        fn resolve_checked(&self, _host: &str) -> Result<Vec<IpAddr>, String> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(self.first.clone())
            } else {
                self.then.clone()
            }
        }
    }

    /// 在同一个端口上分别绑 `127.0.0.1` 与 `[::1]`——两个**不同地址**的真监听器。
    /// （必须同端口：`reqwest` 用 URL 里的端口覆盖解析结果的端口，所以「拨错地方」只能靠
    /// 地址区分，靠端口区分是测不出来的。）
    fn loopback_pair() -> Option<(std::net::TcpListener, std::net::TcpListener, u16)> {
        for _ in 0..64 {
            let v6 = std::net::TcpListener::bind("[::1]:0").ok()?;
            let port = v6.local_addr().ok()?.port();
            if let Ok(v4) = std::net::TcpListener::bind(("127.0.0.1", port)) {
                return Some((v4, v6, port));
            }
        }
        None
    }

    /// 接一次连接就把 flag 置位并回一个最小 200；用 std 监听器 + 阻塞线程，避免把测试
    /// 绑死在某个 runtime 形状上。
    fn serve_once(listener: std::net::TcpListener, saw: Arc<AtomicBool>) {
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                saw.store(true, Ordering::SeqCst);
                use std::io::{Read, Write};
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf);
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                );
            }
        });
    }

    fn client_for(resolver: Arc<dyn CheckedDnsResolve>, ttl: Duration) -> reqwest::Client {
        build_pinned_client(
            ClientConfig {
                request_timeout: Duration::from_secs(3),
                // 测试固定不走代理：代理在场时解析发生在代理侧，本 resolver 根本不会被问到
                // （模块 doc 的天花板那条），那样测的就不是本模块了。
                trust_env_proxy: false,
            },
            resolver,
            ttl,
        )
        .expect("client")
    }

    /// **本卡的验收判据**：检查这一跳解析到允许地址，拨号这一跳解析到被拒地址 ⇒
    /// 必须在**建连之前**被拒，而且被拒的那个地址上的真监听器**一个连接都收不到**。
    #[tokio::test]
    async fn a_rebound_second_answer_is_refused_before_any_connection() {
        let Some((v4, v6, port)) = loopback_pair() else {
            eprintln!(
                "SKIP a_rebound_second_answer_is_refused_before_any_connection: 无法在同一端口上同时绑 127.0.0.1 与 [::1]"
            );
            return;
        };
        let allowed_saw = Arc::new(AtomicBool::new(false));
        let forbidden_saw = Arc::new(AtomicBool::new(false));
        serve_once(v4, allowed_saw.clone());
        serve_once(v6, forbidden_saw.clone());

        let calls = Arc::new(AtomicUsize::new(0));
        let resolver = Arc::new(RebindingResolver {
            first: vec![IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)],
            // 第二次：调用方的策略判定这个地址不许连（BYOK 侧就是 `is_forbidden_ip`）。
            then: Err("::1 is private/reserved/loopback".to_string()),
            calls: calls.clone(),
        });

        // 检查这一跳（连接前的那次解析，第 1 次调用）。
        assert_eq!(
            resolver.resolve_checked("pinned.test").unwrap(),
            vec![IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)]
        );

        // ttl = 0：强制拨号这一跳重新解析，也就是拿到 rebinding 后的第 2 个答案。
        let client = client_for(resolver, Duration::ZERO);
        let err = client
            .get(format!("http://pinned.test:{port}/x"))
            .send()
            .await
            .expect_err("拨号必须被拒");

        let refusal = refusal_in_error_chain(&err)
            .expect("拒绝必须是一条能认回来的出网判定，不是被压平的连接错误");
        assert_eq!(refusal.host, "pinned.test");
        assert!(refusal.reason.contains("private/reserved/loopback"));
        assert!(err.is_connect() || err.is_request());

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !forbidden_saw.load(Ordering::SeqCst),
            "REBOUND_ADDRESS_DIALED = true —— 被拒地址上的监听器收到了连接，说明拒绝只是记了日志"
        );
        assert!(
            !allowed_saw.load(Ordering::SeqCst),
            "拒绝发生在建连之前，允许地址也不该有连接"
        );
    }

    /// 正对照：同一个 client，resolver 返回允许地址时**真的连得上**，而且连的就是解析出来
    /// 的那个地址（另一个地址的监听器无连接）——否则上面那条测的可能只是「这个 client
    /// 什么都连不上」。
    #[tokio::test]
    async fn the_checked_address_is_the_one_dialed() {
        let Some((v4, v6, port)) = loopback_pair() else {
            eprintln!(
                "SKIP the_checked_address_is_the_one_dialed: 无法在同一端口上同时绑 127.0.0.1 与 [::1]"
            );
            return;
        };
        let v4_saw = Arc::new(AtomicBool::new(false));
        let v6_saw = Arc::new(AtomicBool::new(false));
        serve_once(v4, v4_saw.clone());
        serve_once(v6, v6_saw.clone());

        let calls = Arc::new(AtomicUsize::new(0));
        let resolver = Arc::new(RebindingResolver {
            first: vec![IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)],
            then: Ok(vec![IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)]),
            calls: calls.clone(),
        });
        let client = client_for(resolver, DEFAULT_PIN_TTL);
        let response = client
            .get(format!("http://pinned.test:{port}/x"))
            .send()
            .await
            .expect("允许地址必须连得上");
        assert_eq!(response.status().as_u16(), 200);
        assert!(v4_saw.load(Ordering::SeqCst), "解析出来的地址必须真的被连");
        assert!(!v6_saw.load(Ordering::SeqCst), "没解析出来的地址不该被连");
    }

    /// 速度判据（card 17）：pin 不许给每个请求加一次解析——TTL 窗口内 N 个请求只解析 1 次。
    #[tokio::test]
    async fn the_checked_answer_is_reused_for_the_ttl_not_re_resolved_per_request() {
        let Some((v4, _v6, port)) = loopback_pair() else {
            eprintln!(
                "SKIP the_checked_answer_is_reused_for_the_ttl_not_re_resolved_per_request: 端口对绑失败"
            );
            return;
        };
        serve_once(v4, Arc::new(AtomicBool::new(false)));

        let calls = Arc::new(AtomicUsize::new(0));
        let resolver = Arc::new(RebindingResolver {
            first: vec![IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)],
            // 第 2 次起就拒——真被重查了这里必红，不需要靠计数器自证。
            then: Err("second lookup must not happen inside the TTL".to_string()),
            calls: calls.clone(),
        });
        let client = client_for(resolver, DEFAULT_PIN_TTL);
        // n = 30（card 17 的速度判据要求 n>=30）；顺带把每请求墙钟记下来，ADR-0039 的
        // latency 一节引用的就是这条测试打印的数字。
        const N: usize = 30;
        let mut samples_us = Vec::with_capacity(N);
        for _ in 0..N {
            let started = Instant::now();
            let response = client
                .get(format!("http://pinned.test:{port}/x"))
                .send()
                .await
                .expect("TTL 窗口内每次都该用 pin 住的答案");
            assert_eq!(response.status().as_u16(), 200);
            samples_us.push(started.elapsed().as_micros());
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "TTL 窗口内 {N} 个请求只该有 1 次解析"
        );
        samples_us.sort_unstable();
        eprintln!(
            "PIN_LATENCY n={N} p50={:.3}ms p95={:.3}ms (loopback, pin 命中路径)",
            samples_us[N / 2] as f64 / 1000.0,
            samples_us[(N * 95) / 100] as f64 / 1000.0,
        );
    }

    /// 解析器返回空集合 ⇒ 拒，不是「没有地址所以随便连」。
    #[tokio::test]
    async fn zero_addresses_is_a_refusal() {
        struct Empty;
        impl CheckedDnsResolve for Empty {
            fn resolve_checked(&self, _host: &str) -> Result<Vec<IpAddr>, String> {
                Ok(Vec::new())
            }
        }
        let client = client_for(Arc::new(Empty), DEFAULT_PIN_TTL);
        let err = client
            .get("http://nowhere.test:9/x")
            .send()
            .await
            .expect_err("零地址必须拒");
        let refusal = refusal_in_error_chain(&err).expect("必须是出网判定");
        assert_eq!(refusal.reason, "resolved to zero addresses");
    }

    /// `SystemCheckedResolve` 是真解析：loopback 名字能查出 loopback 地址（**不带策略**，
    /// 策略是调用方的事——见类型 doc）。
    #[test]
    fn system_checked_resolve_resolves_localhost() {
        let addrs = SystemCheckedResolve.resolve_checked("localhost").unwrap();
        assert!(!addrs.is_empty());
        assert!(addrs.iter().all(|ip| ip.is_loopback()));
    }

    // 没有「不可解析的名字必须返回 Err」这条测试：本机 DNS 被代理 fake-ip 接管时，
    // `no-such-host.invalid` 会拿到一条合成 A 记录（实测 198.18.0.48），NXDOMAIN 在
    // `getaddrinfo` 这一层根本看不出来——那条断言测的是运行环境，不是本模块。
    // 这是 OS 解析器信任边界的已知天花板（`infra-cell` 的 `cell_resources` 模块 doc 里
    // 记的同一条）。`resolve_checked` 的失败腿只有一个 `map_err` 格式化，无逻辑可测；
    // 有逻辑的那半（策略拒绝、零地址、pin）由上面几条真 socket 测试覆盖。
    // ponytail: 要真正区分 NXDOMAIN 与合成应答，得换成能读 wire RCODE 的 DNS client
    // （hickory-resolver）——真需要区分时再换。
}
