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
            // 判定不在这里做——见 [`skip_or_fail`]：跳过与失败的分界全 workspace 只有一处。
            skip_or_fail(test_name, &reason.to_string(), ExternalDep::Postgres);
        }
    }
}

/// `ops.data_disclosures` / `ops.data_disclosure_sources` 这一对表的测试序列化键。
///
/// **为什么需要它**：§7.4 的 append-only 守卫要用 `TRUNCATE … CASCADE` 才能触发（不带
/// CASCADE 时 PostgreSQL 更早地以「cannot truncate a table referenced in a foreign key
/// constraint」拒绝，守卫触发器根本不会触发，测试就断言不到 §7.4 了）。而
/// **PostgreSQL 是先拿 `AccessExclusiveLock` 再触发 trigger**——即便 TRUNCATE 最终被拒，
/// 锁已经拿到手；CASCADE 又按自己的顺序锁上述两张表，与并发读者的加锁顺序交叉即成环。
/// 实测过一次真死锁（40P01，两个 relation 正是这两张表）。
///
/// 用法（最小序列化：读者之间仍并发，只有 TRUNCATE 那条独占）：
/// - 读侧在 fixture 建连后取 **共享**：`SELECT pg_advisory_lock_shared($K)`；
/// - TRUNCATE 那条先 `pg_advisory_unlock_shared($K)` 再 `pg_advisory_lock($K)`。
///
/// 键定义在这里而不是各文件各写一个数字：跨 crate 的两个测试 binary
/// （`adapters/tests/disclosure_ledger.rs` 与 `retrieval-provider/tests/dashscope_live_smoke.rs`）
/// 都要用同一个值，写两处迟早会有一处改漏——那时序列化静默失效，只剩偶发红。
///
/// 值是任取的固定 bigint，无语义；advisory lock 的命名空间与业务无关。
pub const DISCLOSURE_LEDGER_ADVISORY_LOCK: i64 = 0x0074_4C45_4447_5231;

/// 测试可以声明「本次运行确实有」的外部依赖。**闭集**，不是字符串。
///
/// 早先这里把变量名当 `&str` 参数收，被 §78 的 env 扫描闸当场抓住——不是实现细节，是
/// stringly-typed 的设计缺陷（仓库硬边界原文：禁止 stringly-typed）。闭集之后：新增一个可
/// 声明依赖必须在这里加变体，编译器会逼所有 `match` 跟着改；读取点也各自持有字面量，静态
/// 扫描看得见读的是什么。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalDep {
    /// 真 PostgreSQL（§79.2 说的「只有真库能证明」的那一类：RLS / SKIP LOCKED 并发 /
    /// fencing token / outbox 原子性 / 跨租户隔离 / read-your-writes）。
    Postgres,
    /// 真 Qdrant（向量投影契约往返）。
    Qdrant,
    /// 真 DashScope 出境（live egress 冒烟）。
    DashScope,
}

impl ExternalDep {
    /// 本次运行是否**声明**了自己有这个依赖。
    ///
    /// 每个分支读各自的字面量而不是收一个名字参数：读取点看得见读的是什么，静态扫描才判得
    /// 出这是测试控制量而非生产配置（见 xtask 的 `TEST_CONTROL_ENV_PREFIXES`）。
    #[must_use]
    pub fn declared(self) -> bool {
        let raw = match self {
            Self::Postgres => std::env::var("HUMAUX_REQUIRE_DB"),
            Self::Qdrant => std::env::var("HUMAUX_REQUIRE_QDRANT"),
            Self::DashScope => std::env::var("HUMAUX_REQUIRE_DASHSCOPE"),
        };
        raw.is_ok_and(|v| v == "1")
    }

    /// 报错时告诉人该设哪个变量。
    #[must_use]
    pub const fn env_var(self) -> &'static str {
        match self {
            Self::Postgres => "HUMAUX_REQUIRE_DB",
            Self::Qdrant => "HUMAUX_REQUIRE_QDRANT",
            Self::DashScope => "HUMAUX_REQUIRE_DASHSCOPE",
        }
    }
}

/// **「跳过还是失败」的唯一判定点。** 任何因为外部被测对象缺席而要跳过的测试都走这里，
/// 不要各自手写 `eprintln!("SKIP …"); return;`——散落的跳过点没法统一声明，也就没法统一兜底。
///
/// 语义：`dep` 被声明存在时，缺席即 **panic**；否则打印可见 SKIP 后正常返回（调用方随后
/// `return`）。
///
/// **为什么需要这个开关**：SKIP 分支本身是对的（本机没起依赖时不该红），但它有一个致命的
/// 副作用——**没有被测对象时它长得和通过一模一样**。CI 实测过这个后果：`ci.yml` 从来没有
/// 配过 Postgres service，于是全 workspace **99 个**依赖真库的测试（RLS、SKIP LOCKED 并发、
/// fencing token、outbox 原子性、跨租户隔离、read-your-writes）每次都走 SKIP 分支，而
/// `cargo test --workspace` 一路绿灯。§79.2 说「跳过不等于通过」，CI 却正好把跳过当成了通过。
///
/// 光在 `ci.yml` 里补 service 治不了根：哪天有人删掉那段 YAML，CI 会**悄悄**退回静默跳过，
/// 没有任何东西会红。所以由**声明**兜底：CI 声明「本次运行有真库」，此后任何一个 fixture
/// 拿不到连接都是环境故障，必须响。删 service 的那次提交当场变红，而不是三个月后有人发现
/// RLS 从来没被测过。
///
/// 每个外部依赖各自声明（见 [`ExternalDep`]）：它们是独立服务，合成一个开关会让「只起了
/// Postgres」的环境被迫在 Qdrant 测试上变红。
///
/// 本机开发不设这些变量，行为与从前逐字相同。
pub fn skip_or_fail(test_name: &str, missing_object: &str, dep: ExternalDep) {
    if dep.declared() {
        let var = dep.env_var();
        panic!(
            "{test_name}: {var} is set, so a skip here is a failure — {missing_object}\n\
             声明了「本次运行有这个依赖」却拿不到它：这是环境坏了，不是这条测试不适用。"
        );
    }
    eprintln!("SKIP {test_name}: {missing_object} (§79.2 — 跳过不等于通过)");
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
    fn run_db_fixture_never_runs_the_body_on_an_unreachable_db() {
        // §79.2 的不变量在**两种模式下都成立**，本条断言的就是这个交集：
        // 依赖不可达时 body 一行都不许跑。
        //
        // 两种模式的区别只在「之后怎么办」：未声明 ⇒ 打印可见 SKIP 后正常返回；
        // 已声明（CI 就是这个模式，见 ADR-0005）⇒ panic。本条不去操作环境变量来
        // 挑模式——env 是进程全局的，`cargo test` 并行跑时改它会污染同进程里的
        // 其它测试，那种测试自己就是不确定性的来源。改为：无论当前哪种模式，
        // 都断言 body 没跑；顺带断言 panic 与否恰好等于「有没有声明」。
        let ran = std::sync::atomic::AtomicBool::new(false);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_db_fixture::<AlwaysSkip, _>("smoke", |_handle| {
                ran.store(true, std::sync::atomic::Ordering::SeqCst);
            });
        }));

        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "body must not run when the DB fixture is unreachable"
        );
        assert_eq!(
            outcome.is_err(),
            ExternalDep::Postgres.declared(),
            "panic 与否必须恰好等于「本次运行有没有声明它有真库」——\
             声明了却拿不到是环境故障；没声明则是这条测试不适用"
        );
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
