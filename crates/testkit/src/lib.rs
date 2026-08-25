//! `humaux-testkit` — 测试基建：DB fixture、注错（fault injection）夹具、正哨兵样本。
//!
//! 目录契约：`sentinels/`（§53.3 规则3 正哨兵）· `tests/metrics/`（§80.2 Metric Witness）
//! · `tests/fault/`（§53.4 每个 DegradeCode reason 一条注错）· `tests/pair/`（§52.4 G52-5 成对测试）。
//!
//! 本 crate 不依赖 domain/adapters，只定义测试基建的类型契约（`String` 等
//! 通用类型表达身份/凭证，不引入具体鉴权类型）；具体测试用例落在 `tests/`
//! 各子目录，按各自 README 命名规则实现。§78.3 Workspace Dependency Rule
//! 登记的是 domain crate 禁止依赖 axum/sqlx/qdrant client 等基础设施库，
//! 与本条反向约束是两回事，spec 未见专门条目登记后者，此处不挂错误的 § 号。

use std::fmt;

/// §79.2 DB integration fixture 未能连上真实数据库时的原因。
///
/// 一个不可达的 fixture 必须**带原因跳过**，绝不能静默通过：mock
/// PostgreSQL 无法证明事务本身正确（§79.2「不要用 mock PostgreSQL 证明事务正确」），
/// 所以「没有真 DB」和「测试成功」不是同一件事，调用方必须能区分。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbFixtureSkipReason {
    /// 未设置数据库连接环境变量。
    NoDatabaseUrl,
    /// 已配置连接串，但建立连接失败。
    ConnectFailed(String),
    /// 连接成功，但隔离 schema/transaction 建立失败。
    IsolationSetupFailed(String),
}

impl fmt::Display for DbFixtureSkipReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoDatabaseUrl => write!(f, "no database URL configured"),
            Self::ConnectFailed(msg) => write!(f, "connect failed: {msg}"),
            Self::IsolationSetupFailed(msg) => write!(f, "isolation setup failed: {msg}"),
        }
    }
}

/// §79.2 每个 T3 DB integration 测试必须实现的契约：为一次测试建立隔离
/// schema 或 transaction，对**真实** Postgres 运行，不是 mock 替身
/// （§79.2「不要用 mock PostgreSQL 证明事务正确」；RLS/并发配额预留/
/// SKIP LOCKED/fencing token/outbox 原子性/supersession/public revoke
/// closure 这类语义只有真库能证明）。
///
/// 实现方连接真实数据库；连不上时必须返回 `Err(DbFixtureSkipReason)`，
/// 让调用方走 `run_db_fixture` 输出可见的 SKIP，而不是悄悄放行。
pub trait DbIntegrationFixture: Sized {
    /// 隔离句柄（schema 名、transaction guard 等），由实现方自定义；
    /// `Drop` 时必须撤销隔离（DROP schema 或 ROLLBACK transaction）。
    type Handle;

    /// 建立本次测试的隔离 schema/transaction。§79.2：无法连上真库时必须
    /// 返回 `Err`，禁止返回一个「看起来能用」的默认 `Handle`。
    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason>;
}

/// 在 §79.2 DB fixture 上运行 `body`；fixture 不可达时打印带原因的 SKIP
/// 并直接返回，不把「跳过」伪装成「通过」。T3 测试体应统一走这个入口，
/// 而不是各自手写 `if let ... else return`——重点是 SKIP 必须**可见**。
pub fn run_db_fixture<F, R>(test_name: &str, body: impl FnOnce(F::Handle) -> R)
where
    F: DbIntegrationFixture,
{
    match F::isolate() {
        Ok(handle) => {
            body(handle);
        }
        Err(reason) => {
            eprintln!("SKIP {test_name}: {reason} (§79.2 — 跳过不等于通过)");
        }
    }
}

/// §79.3 cross-tenant 安全测试中的一个租户身份：租户 id、actor id、凭证
/// 材料全部用 `String` 表达，使本 crate 不依赖 domain/adapters 的具体
/// 鉴权类型（testkit 不反向依赖 domain；不是 §78.3——那条约束的是 domain
/// 禁止依赖 axum/sqlx 等基础设施库，方向相反）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantIdentity {
    pub tenant_id: String,
    pub actor_id: String,
    pub credential: String,
}

/// §79.3 CI 必须至少跑的 A/B 双身份对：`A memory ID supplied by B`、
/// `A artifact -> B inaccessible` 这类断言都需要两个**真实存在、互不相同**
/// 的身份，而不是同一身份对自己做断言。
pub struct TenantPair {
    pub a: TenantIdentity,
    pub b: TenantIdentity,
}

impl TenantPair {
    /// 由两个身份构造 A/B 对。§79.3：租户不同才谈得上跨租户隔离，同一
    /// 租户的「A/B 对」测不出任何东西，构造时直接拒绝。
    ///
    /// # Panics
    /// `a.tenant_id == b.tenant_id` 时 panic。
    pub fn new(a: TenantIdentity, b: TenantIdentity) -> Self {
        assert_ne!(
            a.tenant_id, b.tenant_id,
            "§79.3: A/B tenant pair 必须是两个不同租户"
        );
        Self { a, b }
    }
}

/// §53.4 一次注错测试观察到的结果：`DegradeCode` 变体名、注入前后的
/// `degrade_total` 计数、响应体 `completeness.degradations[]` 快照。
pub struct FaultObservation<'a> {
    /// `DegradeCode` 变体名，PascalCase 逐字（如 `ProjectionInvisibleLoss`）。
    /// §53.2 冻结 label 值（PascalCase，用于 `degrade_total{code}`）与线格式
    /// （SCREAMING_SNAKE，用于 `completeness.degradations[]`）两处不许互换；
    /// 本结构只接受变体名一个真源，两种形式由 `assert_fault_observed` 内部
    /// 机械推导，调用方不需要、也不能分别传两种大小写。
    pub degrade_variant: &'a str,
    pub degrade_total_before: u64,
    pub degrade_total_after: u64,
    pub completeness_degradations: &'a [String],
}

/// §53.2 fold(变体名)：在每个非首位大写字母前插 `_`，再整体大写，把
/// PascalCase 变体名机械转换成线格式（SCREAMING_SNAKE_CASE）。
fn fold_to_wire_format(variant: &str) -> String {
    let mut out = String::with_capacity(variant.len() + 4);
    for (i, c) in variant.chars().enumerate() {
        if i > 0 && c.is_ascii_uppercase() {
            out.push('_');
        }
        out.extend(c.to_uppercase());
    }
    out
}

/// §53.4 断言一条注错测试真的被观察到：① `degrade_total{code="<变体>"}`
/// 恰好 +1（label 值 = 变体名逐字，PascalCase）② `completeness.degradations[]`
/// 含该变体的线格式值（SCREAMING_SNAKE，§53.2 fold）。两个条件分开断言，
/// 红的时候能看出是计数没变还是响应体没体现，而不是笼统失败。
///
/// # Panics
/// 任一条件不满足时 panic，消息点名具体哪个条件失败。
pub fn assert_fault_observed(obs: &FaultObservation<'_>) {
    assert_eq!(
        obs.degrade_total_after,
        obs.degrade_total_before + 1,
        "§53.4: degrade_total{{code=\"{}\"}} 必须恰好 +1（before={}, after={}）",
        obs.degrade_variant,
        obs.degrade_total_before,
        obs.degrade_total_after
    );
    let wire = fold_to_wire_format(obs.degrade_variant);
    assert!(
        obs.completeness_degradations.contains(&wire),
        "§53.4: completeness.degradations[] 必须含 \"{}\"，实际 {:?}",
        wire,
        obs.completeness_degradations
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AlwaysSkip;

    impl DbIntegrationFixture for AlwaysSkip {
        type Handle = ();

        fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
            Err(DbFixtureSkipReason::NoDatabaseUrl)
        }
    }

    #[test]
    fn run_db_fixture_skips_without_running_body_on_unreachable_db() {
        // §79.2: 不可达时必须跳过而不是把 body 当成通过——body 根本不能跑。
        let mut ran = false;
        run_db_fixture::<AlwaysSkip, _>("smoke", |_handle| {
            ran = true;
        });
        assert!(!ran, "body must not run when the DB fixture is unreachable");
    }

    #[test]
    #[should_panic(expected = "两个不同租户")]
    fn tenant_pair_rejects_identical_tenant() {
        let identity = TenantIdentity {
            tenant_id: "t1".into(),
            actor_id: "a1".into(),
            credential: "c1".into(),
        };
        TenantPair::new(identity.clone(), identity);
    }

    #[test]
    fn assert_fault_observed_passes_on_matching_evidence() {
        // §53.2: label 值（PascalCase）与线格式（SCREAMING_SNAKE）是两种不同
        // 大小写，这里刻意分开写死，若 assert_fault_observed 内部把两者混用
        // 或互换，本用例会失败——不能像早前那样两处传同一个大小写掩盖问题。
        let degradations = vec!["RERANK_PROVIDER_TIMEOUT".to_string()];
        assert_fault_observed(&FaultObservation {
            degrade_variant: "RerankProviderTimeout",
            degrade_total_before: 0,
            degrade_total_after: 1,
            completeness_degradations: &degradations,
        });
    }

    #[test]
    #[should_panic(expected = "恰好 +1")]
    fn assert_fault_observed_fails_when_counter_did_not_move() {
        let degradations = vec!["RERANK_PROVIDER_TIMEOUT".to_string()];
        assert_fault_observed(&FaultObservation {
            degrade_variant: "RerankProviderTimeout",
            degrade_total_before: 0,
            degrade_total_after: 0,
            completeness_degradations: &degradations,
        });
    }

    #[test]
    #[should_panic(expected = "completeness.degradations")]
    fn assert_fault_observed_fails_when_degradations_carry_wrong_case() {
        // 响应体如果错误地塞进了 PascalCase（label 值）而不是线格式，必须红。
        let degradations = vec!["RerankProviderTimeout".to_string()];
        assert_fault_observed(&FaultObservation {
            degrade_variant: "RerankProviderTimeout",
            degrade_total_before: 0,
            degrade_total_after: 1,
            completeness_degradations: &degradations,
        });
    }

    #[test]
    fn fold_to_wire_format_matches_spec_example() {
        // §53.2 登记的机械映射例子：ProjectionInvisibleLoss -> PROJECTION_INVISIBLE_LOSS
        assert_eq!(
            fold_to_wire_format("ProjectionInvisibleLoss"),
            "PROJECTION_INVISIBLE_LOSS"
        );
    }
}

pub mod dod;
