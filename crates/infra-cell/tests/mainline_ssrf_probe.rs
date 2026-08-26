//! 主线独立复现：ADR-0003 二轮审查报告的 IP 字面量绕过是否真被堵住。
//!
//! 攻击面：`reqwest`/`hyper` 在 URL authority 是 IP 字面量时**不会**调用自定义
//! `dns_resolver`，所以 §83.4 判据3 若只挂在 resolver 上，对 `host="127.0.0.1"` 这种
//! 形态完全失效——而树里所有真实 registry 构造点用的正是 IP 字面量。
//! 「被拒」的标准是**零连接**：连上了再报网络错误不算拒绝。

use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use humaux_infra_cell::transport::{
    HttpIntraCellTransport, IntraCellHttpTransport, IntraCellMethod, IntraCellRequest,
};
use humaux_infra_cell::{
    CallerId, CellCidr, CellId, IntraCellResource, IntraCellResourceRegistry, ResourceEntry,
    authorize_cell_access,
};

/// 真监听器 + 是否收到连接的标记。
fn spy_listener() -> (SocketAddr, Arc<AtomicBool>) {
    let l = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = l.local_addr().unwrap();
    let hit = Arc::new(AtomicBool::new(false));
    let h = hit.clone();
    thread::spawn(move || {
        if l.accept().is_ok() {
            h.store(true, Ordering::SeqCst);
        }
    });
    (addr, hit)
}

fn registry_with(host: &str, port: u16, cidr: &str) -> IntraCellResourceRegistry {
    let cell = CellId(uuid::Uuid::nil());
    let caller = CallerId("mainline-probe".to_string());
    let entry = ResourceEntry::new(
        host.to_string(),
        port,
        cell,
        vec![cidr.parse::<CellCidr>().expect("cidr")],
        BTreeSet::from([caller.clone()]),
        false,
    )
    .expect("registry entry");
    let mut entries = BTreeMap::new();
    entries.insert(IntraCellResource::QDRANT_REST, entry);
    IntraCellResourceRegistry::new(entries, CellId(uuid::Uuid::nil()), caller)
}

async fn call(reg: IntraCellResourceRegistry, path: &str) -> Result<(), String> {
    let permit = authorize_cell_access(
        &reg,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(30),
    )
    .map_err(|e| format!("{e:?}"))?;
    let transport = HttpIntraCellTransport::new(reg, Duration::from_secs(5), 1024 * 1024)
        .map_err(|e| format!("{e:?}"))?;
    transport
        .execute(
            &permit,
            IntraCellRequest {
                method: IntraCellMethod::Get,
                path: path.to_string(),
                json_body: None,
                headers: vec![],
            },
        )
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

/// 攻击 1：IP 字面量指向一个**活的**监听器，但它不在注册 CIDR 内。
/// 必须拒绝，且一个连接都不许发出。
#[tokio::test]
async fn ip_literal_outside_registered_cidr_is_rejected_with_zero_connections() {
    let (addr, hit) = spy_listener();
    let reg = registry_with("127.0.0.1", addr.port(), "10.0.0.0/8");
    let res = call(reg, "/").await;
    assert!(
        res.is_err(),
        "out-of-CIDR IP literal must be rejected, got {res:?}"
    );
    assert!(
        !hit.load(Ordering::SeqCst),
        "REGRESSION: a real connection reached an out-of-Cell address (判据3 dead for IP literals)"
    );
}

/// 攻击 2：IP 字面量直指云 metadata 地址。必须在拨号前硬拒，
/// 且失败原因必须是判据3 的判定，而不是「连上以后网络错误」。
#[tokio::test]
async fn ip_literal_metadata_address_is_rejected_before_dialing() {
    let reg = registry_with("169.254.169.254", 80, "127.0.0.0/8");
    let err = call(reg, "/latest/meta-data/")
        .await
        .expect_err("metadata address must never be reachable through IntraCell transport");
    assert!(
        err.contains("Metadata") || err.contains("AddressNotInCell"),
        "must fail on the 判据3 judgment, not on a post-dial network error: {err}"
    );
}

/// 攻击 3：十进制/八进制/十六进制编码的 127.0.0.1（审查员点名的绕过形态）。
/// URL 解析器会把它们折叠成同一个地址，判定必须同样命中。
#[tokio::test]
async fn numeric_encoded_ip_literals_are_judged_the_same_as_dotted_quad() {
    for host in ["2130706433", "0177.0.0.1", "0x7f.0.0.1"] {
        let (addr, hit) = spy_listener();
        let reg = registry_with(host, addr.port(), "10.0.0.0/8");
        let res = call(reg, "/").await;
        assert!(
            res.is_err(),
            "encoded literal {host} must be rejected, got {res:?}"
        );
        assert!(
            !hit.load(Ordering::SeqCst),
            "REGRESSION: encoded literal {host} reached a live listener outside the Cell CIDR"
        );
    }
}

/// 攻击 4：IPv4-mapped IPv6（Baseline 2.9 §83.4 九条准入里点名的 `IPv4-mapped IPv6 规范化`）。
///
/// `[::ffff:169.254.169.254]` 经 WHATWG 解析后是 `url::Host::Ipv6(::ffff:169.254.169.254)`，
/// 若 metadata 判据只按 V6 语义看，`fe80::/10` 匹配不到它——metadata 地址就从 V6 这一侧漏过去。
/// 实现靠 `is_metadata_or_link_local` 开头的 `.to_canonical()` 把 mapped 形态展平回 V4 再判。
///
/// 断言必须钉**具体**错误而不是「拒了就行」：`address_in_cell` 会顺手接住同一个地址（它落在
/// registry CIDR 之外），一条 `Metadata || AddressNotInCell` 的松断言会让两道闸互相顶包，
/// 拆掉展平后仍然绿——本条上线首版即如此假绿。这里要求逐字命中 metadata 判据本身。
#[tokio::test]
async fn ipv4_mapped_ipv6_metadata_literal_is_flattened_before_judgment() {
    for host in ["[::ffff:169.254.169.254]", "[::ffff:a9fe:a9fe]"] {
        let reg = registry_with(host, 80, "127.0.0.0/8");
        let err = call(reg, "/latest/meta-data/").await.expect_err(
            "IPv4-mapped metadata literal must never be reachable through IntraCell transport",
        );
        assert!(
            err.contains("Metadata"),
            "{host} must be caught by the metadata/link-local judgment itself (i.e. the mapped \
             form was flattened), not merely by the Cell-CIDR judgment downstream: {err}"
        );
    }
}

/// 同一形态的 CIDR 侧对照：`[::ffff:127.0.0.1]` 不是 metadata，只是落在 Cell CIDR 之外，
/// 必须**零连接**地被拒——证明展平后的地址确实走到了 CIDR 判据。
///
/// ponytail: 这里不给 `CellCidr::contains` 的 `.to_canonical()` 配注错闸——去掉它会让
/// `(V4 net, V6 ip)` 落进 `_ => false`，判定只会更严不会更松（fail-closed），那是可用性回归
/// 不是安全回归，没有对应的攻击可复现。承重的是上面 metadata 那条。
#[tokio::test]
async fn ipv4_mapped_ipv6_outside_cell_cidr_reaches_no_listener() {
    let (addr, hit) = spy_listener();
    let reg = registry_with("[::ffff:127.0.0.1]", addr.port(), "10.0.0.0/8");
    let res = call(reg, "/").await;
    assert!(
        res.is_err(),
        "mapped literal outside the Cell CIDR must be rejected, got {res:?}"
    );
    assert!(
        !hit.load(Ordering::SeqCst),
        "REGRESSION: IPv4-mapped literal reached a live listener outside the Cell CIDR"
    );
}
