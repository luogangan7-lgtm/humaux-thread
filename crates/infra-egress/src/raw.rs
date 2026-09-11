//! `infra_egress::raw` — Layer 1A 的原始 HTTPS POST 出口，给 **BYOK 域**（§11）用。
//!
//! 为什么不是复用 [`crate::http::HttpExternalCall`]：那是 **PlatformManaged 域**（§19）的
//! 出口——它自己注 `Authorization`（凭据来自 `RetrievalCredentialSource`）、自己按
//! `status_classifier` 分类（401 = `Unauthorized`）。BYOK 域这两件事都必须发生在
//! `adapters::byok` 侧：明文 key 的唯一展开点是 `build_openai_request`（§11.1 单点纪律），
//! 401 的语义是 `WaitingKey` 而非平台故障（§11.3）。让 BYOK 走 `HttpExternalCall` 会同时
//! 破坏这两条。`byok.rs` 的模块 doc 早已点名这是「两条路」——本模块就是姊妹那条。
//!
//! 为什么住在本 crate 而不是 `adapters`：G80-3 的
//! `g80_3_infra_network_dependents_check` 把 `humaux-infra-network` 的依赖方 manifest 钉死为
//! `{infra-egress, infra-cell}`，`adapters` 加那条依赖当场红。所以 raw client 的构造留在
//! 这里，`adapters` 只经本类型间接触达——签名刻意只收 `std` 类型（`Duration`），
//! `adapters` 侧完全不触 `infra-network` 的类型。
//!
//! 纪律（与 [`crate::http`] 对齐，缺一即偏离 §83.4）：
//! - 不注 `Authorization`——凭据属于调用方的 `build_openai_request`；
//! - 不做状态分类——`classify_http_status` / `classify_base_resp` 独占；
//! - `https://` 强制（loopback `http://` 仅供测试），redirect 永不跟随（构造点内建）；
//! - 响应体流式读且有上限——不给坏 provider 无界缓冲的机会；
//! - **拨号只走调用方检查过的地址**（ADR-0039）：client 的唯一 DNS resolver 就是调用方注入的
//!   [`crate::resolver::CheckedDnsResolve`]，检查与拨号是同一次解析。在此之前本类型用系统 DNS
//!   拨号、而 §11.4 的 SSRF 闸用注入的 resolver 检查，两者可以给出不同答案——那是一个真实的
//!   DNS rebinding 窗口，而且开在带着用户 provider API key 的那条路上。

use std::sync::Arc;
use std::time::Duration;

use humaux_infra_network::http::ClientConfig;
use humaux_infra_network::reqwest;

use crate::resolver::{CheckedDnsResolve, DEFAULT_PIN_TTL, build_pinned_client};

/// 一次原始调用的结果。**只有事实，没有判断**：状态码怎么解释归调用方。
#[derive(Debug)]
pub struct RawOutcome {
    /// HTTP 状态码。
    pub status: u16,
    /// `Retry-After` 头的秒数形式；HTTP-date 形式不解析（与 `HttpExternalCall` 同款省略，
    /// ponytail: 真 provider 用到 date 形式再补）。
    pub retry_after: Option<Duration>,
    /// 响应体（已按上限截断校验，超限走 [`RawSendError::BodyTooLarge`] 而不是静默截断）。
    pub body: Vec<u8>,
}

/// 传输层失败。**不含任何业务分类**——`WaitingKey`/`RetryWait` 之类的语义由
/// `adapters::byok` 按 §11.3 翻译。
#[derive(Debug)]
pub enum RawSendError {
    /// 端点既不是 `https://` 也不是 loopback `http://`。
    NonHttpsEndpoint,
    /// 整请求超时（连接 + 发送 + 接收）。
    Timeout,
    /// 响应体超过调用方给的上限。
    BodyTooLarge {
        /// 调用方给的上限（字节）。
        limit: usize,
    },
    /// 其余网络层失败，携带不含凭据的描述。
    Network(String),
    /// ADR-0039：调用方注入的出网策略在**建连之前**拒了这个主机名解析出来的地址。
    /// 与 [`RawSendError::Network`] 分开是有意的——「策略拒了」和「网络刚好不通」必须能
    /// 分辨，否则一条被成功挡下的 rebinding 攻击看起来跟一次超时没有区别。
    EgressRefused {
        /// 被拒的主机名。
        host: String,
        /// 调用方给的理由（不含凭据：resolver 只看主机名）。
        reason: String,
    },
}

/// BYOK 域的原始 HTTPS POST。持有一个经 [`build_pinned_client`] 构造的 client——
/// 本 crate 自 Layer 0/1A 拆分起未再直呼 `Client::builder()`（ADR-0003），
/// 自 ADR-0039 起也不再存在「不带 resolver 的构造」这条路。
pub struct RawHttpPost {
    client: reqwest::Client,
}

impl RawHttpPost {
    /// 构造。`request_timeout` 是整请求墙钟（`reqwest` 默认无超时——没有它，一个收下连接
    /// 后永不响应的 provider 会把调用挂死到 §53 INV-3 的账本看门狗从外面发现为止）。
    ///
    /// `resolver` 是这个 client **唯一**的 DNS resolver：调用方在这里注入的就是它自己检查
    /// 端点时用的那套判定（BYOK 侧 = `adapters::byok::SsrfCheckedResolver`，即 §11.4 的
    /// `is_forbidden_ip`），所以检查过的地址就是连上去的地址。答案按
    /// [`DEFAULT_PIN_TTL`] pin 住——TTL 窗口内不会给每个请求多加一次解析（速度判据），
    /// 窗口过后重查并重新判定（rebinding 换到禁止地址会在重查时被拒）。
    ///
    /// # Errors
    /// [`RawSendError::Network`]：底层 client 构造失败（TLS 后端初始化之类，极罕见）。
    pub fn new(
        request_timeout: Duration,
        resolver: Arc<dyn CheckedDnsResolve>,
    ) -> Result<Self, RawSendError> {
        let client = build_pinned_client(
            ClientConfig {
                request_timeout,
                // 外部出境：组织的 egress 代理应当被尊重（与 crate::http 的 Layer 1A 语义一致；
                // 不尊重代理是 Layer 1B infra-cell 的语义）。天花板：代理在场时解析发生在代理
                // 侧，上面这条 pin 不成立——见 `resolver` 模块 doc 与 `ClientConfig::
                // trust_env_proxy` 自己的 doc。
                trust_env_proxy: true,
            },
            resolver,
            DEFAULT_PIN_TTL,
        )
        .map_err(|e| RawSendError::Network(e.to_string()))?;
        Ok(Self { client })
    }

    /// 发一次 POST。`headers` 由调用方给全（含 `Authorization`——见模块 doc 的纪律），
    /// `body` 是已经拼好的字节（permit 的 sha256 绑定在这些字节上，本函数一个字节不动）。
    ///
    /// # Errors
    /// 见 [`RawSendError`]。**非 2xx 不是本层的错误**——状态码原样交回，
    /// 解释权归调用方（HTTP 200 + `base_resp != 0` 的 MiniMax 形态正是解释权必须上收的
    /// 理由：本层看不懂 body，硬判就会把限流当成功）。
    pub async fn send(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
        max_response_bytes: usize,
    ) -> Result<RawOutcome, RawSendError> {
        if !scheme_allowed(url) {
            return Err(RawSendError::NonHttpsEndpoint);
        }
        let mut req = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .body(body);
        for (name, value) in headers {
            req = req.header(name.as_str(), value.as_str());
        }
        let mut response = req.send().await.map_err(classify_transport_error)?;

        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map(Duration::from_secs);

        // 有界读（§83.4）：按块累积并逐块校验上限，不用 `response.bytes()` 一次吞下。
        let mut out = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(classify_transport_error)? {
            if out.len() + chunk.len() > max_response_bytes {
                return Err(RawSendError::BodyTooLarge {
                    limit: max_response_bytes,
                });
            }
            out.extend_from_slice(&chunk);
        }

        Ok(RawOutcome {
            status,
            retry_after,
            body: out,
        })
    }
}

/// `reqwest::Error` -> [`RawSendError`]。ADR-0039：出网策略的拒绝先从错误链上捞回来
/// （[`crate::resolver::refusal_in_error_chain`]），否则它会被压成一条 `Network(...)`，
/// 「策略挡下了一次 rebinding」和「网络不通」就再也分不开了。
/// `reqwest` 的 `Display` 不含请求头，凭据不会经此泄漏。
fn classify_transport_error(e: reqwest::Error) -> RawSendError {
    if let Some(refused) = crate::resolver::refusal_in_error_chain(&e) {
        return RawSendError::EgressRefused {
            host: refused.host,
            reason: refused.reason,
        };
    }
    if e.is_timeout() {
        RawSendError::Timeout
    } else {
        RawSendError::Network(e.to_string())
    }
}

/// 与 [`crate::http`] 的 `endpoint_scheme_is_allowed` 同判据：`https://` 恒许，
/// `http://` 仅 loopback（本 crate 自己的 TcpListener 测试要用）。
fn scheme_allowed(endpoint: &str) -> bool {
    if let Some(rest) = endpoint.strip_prefix("https://") {
        return !rest.is_empty();
    }
    if let Some(rest) = endpoint.strip_prefix("http://") {
        let host = rest.split(['/', ':', '?']).next().unwrap_or("");
        return matches!(host, "127.0.0.1" | "localhost" | "::1" | "[::1]");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::SystemCheckedResolve;

    /// scheme 判据与 crate::http 同型：https 恒许、http 仅 loopback。
    #[test]
    fn scheme_gate_matches_the_layer_1a_rule() {
        assert!(scheme_allowed(
            "https://api.minimaxi.com/v1/chat/completions"
        ));
        assert!(scheme_allowed("http://127.0.0.1:9999/x"));
        assert!(scheme_allowed("http://localhost:9999/x"));
        assert!(!scheme_allowed(
            "http://api.minimaxi.com/v1/chat/completions"
        ));
        assert!(!scheme_allowed("ftp://api.minimaxi.com/x"));
        assert!(!scheme_allowed("https://"));
    }

    /// 非法 scheme 在建连之前就拒——不依赖网络。
    #[tokio::test]
    async fn non_https_is_rejected_before_any_connection() {
        let raw = RawHttpPost::new(Duration::from_secs(1), Arc::new(SystemCheckedResolve))
            .expect("client");
        let err = raw
            .send("http://api.minimaxi.com/v1/x", &[], vec![], 1024)
            .await
            .expect_err("非 loopback 的 http 必须拒");
        assert!(matches!(err, RawSendError::NonHttpsEndpoint));
    }

    /// 响应体超限 ⇒ BodyTooLarge，不静默截断。用本地真监听器。
    #[tokio::test]
    async fn an_oversized_body_is_rejected_not_truncated() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let big = "x".repeat(4096);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                big.len(),
                big
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });
        let raw = RawHttpPost::new(Duration::from_secs(5), Arc::new(SystemCheckedResolve))
            .expect("client");
        let err = raw
            .send(
                &format!("http://127.0.0.1:{}/x", addr.port()),
                &[],
                vec![],
                1024,
            )
            .await
            .expect_err("4096 字节的 body 必须撞上 1024 的上限");
        assert!(matches!(err, RawSendError::BodyTooLarge { limit: 1024 }));
    }
}
