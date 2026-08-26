//! `application::scheduler` — Tenant Fair Scheduler 三层限流骨架 + cost 模型（§32.0）。
//!
//! Leadership 与 exactly-once enqueue 的 DB 逻辑不在本模块：`RuntimeDbPool::pool()` 是
//! `pub(crate)`（`crates/adapters/src/postgres.rs`），Domain/Application 层拿不到裸
//! `sqlx::PgPool`（G80-40 会红）——那部分实现在 `adapters::scheduler`。本模块只负责纯内存、
//! 无 I/O 的准入控制（Global/Provider/Tenant 三层）与租户公平排序（DRR），供 job worker 在
//! claim 循环里调用，判断"现在可以再放行一个 job 吗"与"下一个该轮到哪个 tenant"。
//!
//! §78.1 冻结："禁止硬编码业务配置：模型名/价格/plan 判断/quota 数/TTL/限流阈值"——本模块
//! 的所有阈值（`AdmissionLimits`/`CostWeights`）都是调用方传入的运行时值，模块自身不定义
//! 任何默认阈值常量。

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};

/// §32.0 每类 Job 的 estimated cost 四项。字段顺序与 spec 代码块一致。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JobCost {
    pub private_llm_tokens: u64,
    pub embedding_tokens: u64,
    pub rerank_tokens: u64,
    pub document_pages: u64,
}

/// 四项成本→单一权重的换算系数。禁止在本 crate 硬编码任何具体数值（§78.1）——调用方从
/// 配置/DB 读取后传入。`1` 系数即"按 token/page 数原样计数"，调用方可以让某一项系数为
/// `0` 来完全忽略它。
#[derive(Debug, Clone, Copy)]
pub struct CostWeights {
    pub private_llm_tokens: u64,
    pub embedding_tokens: u64,
    pub rerank_tokens: u64,
    pub document_pages: u64,
}

impl JobCost {
    /// 加权成本 = Σ(该项数值 × 对应系数)。饱和加法/乘法，避免溢出 panic——一个错误配置的
    /// 超大系数应该让公平性退化（该 tenant 的 deficit 涨得更慢），不应该让 worker 进程崩溃。
    pub fn weighted(&self, weights: &CostWeights) -> u64 {
        self.private_llm_tokens
            .saturating_mul(weights.private_llm_tokens)
            .saturating_add(
                self.embedding_tokens
                    .saturating_mul(weights.embedding_tokens),
            )
            .saturating_add(self.rerank_tokens.saturating_mul(weights.rerank_tokens))
            .saturating_add(self.document_pages.saturating_mul(weights.document_pages))
    }
}

/// 三层准入控制的一层：一个 `max` 上限 + 一个原子计数器。
///
/// ponytail: 三层各自独立 `AtomicUsize`，`try_admit`/`release` 之间没有跨层事务性——高并发下
/// 理论上可能短暂超发（例如 global 刚好在 provider 检查和 global 检查之间被另一线程占满）。
/// 骨架阶段可接受；真正需要严格上限时把三层计数收进一把 `Mutex<Counters>` 一次性 CAS。
#[derive(Debug)]
struct Counter {
    max: usize,
    current: AtomicUsize,
}

impl Counter {
    fn new(max: usize) -> Self {
        Self {
            max,
            current: AtomicUsize::new(0),
        }
    }

    /// `current < max` 时 +1 返回 `true`；否则不变返回 `false`。用 `fetch_update` 一次
    /// compare-and-swap 完成，避免 `load` 之后再 `fetch_add` 之间的检查-再用竞态。
    fn try_admit(&self) -> bool {
        self.current
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                (cur < self.max).then_some(cur + 1)
            })
            .is_ok()
    }

    fn release(&self) {
        // 饱和减法：release 多调用一次不应该把计数器绕到 usize::MAX。
        let _ = self
            .current
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                Some(cur.saturating_sub(1))
            });
    }
}

/// 三层限流骨架（§32.0 "至少三层：Global semaphore / Provider semaphore·token bucket /
/// Tenant semaphore·credit"）。Token bucket 的时间维度补充（真正的令牌桶速率恢复）不在本
/// 骨架范围内——`Counter` 目前是纯并发数上限，不是速率限制；调用方需要速率限制时在这之上
/// 再包一层。
pub struct TieredLimiter {
    global: Counter,
    provider: HashMap<String, Counter>,
    tenant: HashMap<String, Counter>,
}

/// [`TieredLimiter::try_admit`] 成功后持有的许可证。`Drop` 时自动释放三层配额，调用方不需
/// 要记得手动 release——job 处理失败 panic 时也不会泄漏配额。
pub struct AdmitGuard<'a> {
    limiter: &'a TieredLimiter,
    provider: String,
    tenant: String,
}

impl Drop for AdmitGuard<'_> {
    fn drop(&mut self) {
        self.limiter.global.release();
        if let Some(c) = self.limiter.provider.get(&self.provider) {
            c.release();
        }
        if let Some(c) = self.limiter.tenant.get(&self.tenant) {
            c.release();
        }
    }
}

impl TieredLimiter {
    /// `provider_max`/`tenant_max` 键缺失的 provider/tenant 视为"无限流"（该层直接放行）——
    /// 调用方按需为已知 provider/tenant 建条目，不要求穷举全集。
    pub fn new(
        global_max: usize,
        provider_max: impl IntoIterator<Item = (String, usize)>,
        tenant_max: impl IntoIterator<Item = (String, usize)>,
    ) -> Self {
        Self {
            global: Counter::new(global_max),
            provider: provider_max
                .into_iter()
                .map(|(k, v)| (k, Counter::new(v)))
                .collect(),
            tenant: tenant_max
                .into_iter()
                .map(|(k, v)| (k, Counter::new(v)))
                .collect(),
        }
    }

    /// 三层都放行才返回 `Some`；任一层拒绝时，已经占用的更高层配额会被立即退回（不留悬空
    /// 占用），返回 `None`。顺序 global → provider → tenant，与 spec 代码块顺序一致。
    pub fn try_admit(&self, provider: &str, tenant: &str) -> Option<AdmitGuard<'_>> {
        if !self.global.try_admit() {
            return None;
        }
        if let Some(c) = self.provider.get(provider)
            && !c.try_admit()
        {
            self.global.release();
            return None;
        }
        if let Some(c) = self.tenant.get(tenant)
            && !c.try_admit()
        {
            self.global.release();
            if let Some(c) = self.provider.get(provider) {
                c.release();
            }
            return None;
        }
        Some(AdmitGuard {
            limiter: self,
            provider: provider.to_string(),
            tenant: tenant.to_string(),
        })
    }
}

/// [`TenantFairQueue`] 里排队的一件待办：调用方类型 `T`（通常是 job id 或完整 job 结构体）
/// 附带它的成本（用于 DRR 扣减 deficit）。
struct QueueItem<T> {
    item: T,
    cost: u64,
}

/// §32.0 "推荐 scheduler 采用 Deficit Round Robin 或 tenant round-robin + cost weight，而不
/// 是只按 created_at 排序"。本结构实现经典 DRR：每个 tenant 一条 FIFO 队列 + 一个 deficit
/// 计数器；`quantum` 每轮给每个非空队列的 tenant 加一次；deficit 够付队头成本才出队。
///
/// oldest-only（只按 created_at）排序的问题是单 tenant 灌满队列时其它 tenant 会被完全饿死；
/// DRR 保证每个非空 tenant 队列迟早轮到，且轮到的频率与其 job 成本成反比（§61 "生产版本还
/// 要在 application scheduler 上加 tenant fairness，不要让 oldest-only claim 成为唯一仲
/// 裁"）。
pub struct TenantFairQueue<T> {
    quantum: u64,
    queues: HashMap<String, VecDeque<QueueItem<T>>>,
    deficits: HashMap<String, u64>,
    /// 轮转顺序——`HashMap` 迭代顺序不确定，公平轮转需要一个稳定的 tenant 顺序。
    order: VecDeque<String>,
}

impl<T> TenantFairQueue<T> {
    pub fn new(quantum: u64) -> Self {
        Self {
            quantum,
            queues: HashMap::new(),
            deficits: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    /// 入队一个 tenant 的一件工作。首次见到该 tenant 时把它加入轮转顺序末尾。
    pub fn push(&mut self, tenant: &str, item: T, cost: u64) {
        if !self.queues.contains_key(tenant) {
            self.order.push_back(tenant.to_string());
            self.deficits.insert(tenant.to_string(), 0);
        }
        self.queues
            .entry(tenant.to_string())
            .or_default()
            .push_back(QueueItem { item, cost });
    }

    /// 取出下一件该被处理的工作（DRR 核心循环）。最多扫描 `order.len()` 轮——如果连一整
    /// 轮下来所有非空队列的队头成本都超过各自新增的 deficit，说明 `quantum` 相对 cost 太
    /// 小，直接返回 `None` 而不是死循环空转（调用方应加大 quantum 或降低单 job 成本上限）。
    pub fn next_ready(&mut self) -> Option<(String, T)> {
        let rounds = self.order.len();
        for _ in 0..rounds.max(1) {
            let tenant = self.order.pop_front()?;
            self.order.push_back(tenant.clone());

            let is_empty = self.queues.get(&tenant).is_none_or(|q| q.is_empty());
            if is_empty {
                continue;
            }

            let deficit = self.deficits.entry(tenant.clone()).or_insert(0);
            *deficit = deficit.saturating_add(self.quantum);

            let queue = self
                .queues
                .get_mut(&tenant)
                .expect("checked non-empty above");
            let head_cost = queue.front().expect("checked non-empty above").cost;
            if *deficit >= head_cost {
                *deficit -= head_cost;
                let popped = queue.pop_front().expect("checked non-empty above");
                return Some((tenant, popped.item));
            }
            // 这个 tenant 的 deficit 还不够付队头成本，让给下一个 tenant，下一轮再攒。
        }
        None
    }
}

#[cfg(test)]
mod tests {
    //! ponytail: 非平凡逻辑（三层准入的 CAS、DRR 的轮转+deficit）留一个可跑的自检，不是完整
    //! 套件——三层限流与 DRR 都是纯内存结构，不需要 DB fixture。

    use super::*;

    #[test]
    fn tiered_limiter_denies_at_each_layer_and_releases_on_drop() {
        let limiter = TieredLimiter::new(
            1,
            [("dashscope".to_string(), 1)],
            [("tenant-a".to_string(), 5)],
        );

        let first = limiter
            .try_admit("dashscope", "tenant-a")
            .expect("first admit under all three limits must succeed");
        // Global max=1 已被占满：即使 provider/tenant 都还有余量，第二个请求必须被拒绝。
        assert!(
            limiter.try_admit("dashscope", "tenant-a").is_none(),
            "global layer at capacity must deny regardless of provider/tenant headroom"
        );
        // 换一个不同 provider/tenant 仍然被 global 挡住——三层是 AND 不是 OR。
        assert!(limiter.try_admit("other-provider", "tenant-b").is_none());

        drop(first);
        // guard drop 后配额退回，global 重新可用。
        assert!(
            limiter.try_admit("other-provider", "tenant-b").is_some(),
            "dropping the first guard must release its global-layer slot"
        );
    }

    #[test]
    fn drr_gives_low_cost_tenant_more_turns_than_high_cost_tenant() {
        // tenant-heavy 每个 job 成本 10，tenant-light 每个成本 1。quantum=1：DRR 应该让
        // tenant-light 在同一段时间内被服务的次数远多于 tenant-heavy——这正是 oldest-only
        // (仅按 created_at) 排序做不到的公平性（同一 FIFO 里先到先得，重 job 不会少排）。
        let mut q: TenantFairQueue<u32> = TenantFairQueue::new(1);
        for i in 0..3 {
            q.push("tenant-heavy", i, 10);
        }
        for i in 0..3 {
            q.push("tenant-light", i, 1);
        }

        let mut served = HashMap::new();
        // `next_ready` returning `None` mid-cycle is not exhaustion — it means neither
        // tenant's deficit has reached its head cost *this round* (e.g. tenant-light's queue
        // is already empty but tenant-heavy hasn't accumulated 10 yet); a real caller keeps
        // calling on the next tick. So this loop runs a fixed number of times and only
        // records `Some`, it must not `break` on the first `None`.
        for _ in 0..60 {
            if let Some((tenant, _)) = q.next_ready() {
                *served.entry(tenant).or_insert(0u32) += 1;
            }
        }

        assert_eq!(served.get("tenant-heavy").copied().unwrap_or(0), 3);
        assert_eq!(served.get("tenant-light").copied().unwrap_or(0), 3);
        // 两边总数相等（都排空），但 tenant-light 应该先排空——用"发生顺序"证明不了（未记录
        // 顺序），这里改为断言 quantum 语义的直接后果：tenant-light 只需 1 次 quantum 就能
        // 出队一件，tenant-heavy 需要攒 10 次——即 light 出队第 3 件时 heavy 还一件没出。
        let mut q2: TenantFairQueue<u32> = TenantFairQueue::new(1);
        q2.push("tenant-heavy", 0, 10);
        q2.push("tenant-light", 0, 1);
        q2.push("tenant-light", 1, 1);
        let mut order = Vec::new();
        for _ in 0..5 {
            if let Some((t, _)) = q2.next_ready() {
                order.push(t);
            }
        }
        assert_eq!(
            order,
            vec!["tenant-light", "tenant-light"],
            "cheap tenant must clear its queue while the expensive tenant is still accumulating deficit"
        );
    }

    #[test]
    fn job_cost_weighted_uses_caller_supplied_weights_not_a_hardcoded_default() {
        let cost = JobCost {
            private_llm_tokens: 100,
            embedding_tokens: 50,
            rerank_tokens: 0,
            document_pages: 2,
        };
        let weights = CostWeights {
            private_llm_tokens: 3,
            embedding_tokens: 1,
            rerank_tokens: 1,
            document_pages: 10,
        };
        // rerank_tokens is 0 above and contributes nothing regardless of its weight — left
        // out of the expected sum rather than written as `0 * 1` (clippy::erasing_op); the
        // embedding term's `* 1` weight is likewise dropped (clippy::identity_op).
        assert_eq!(cost.weighted(&weights), 100 * 3 + 50 + 2 * 10);
    }
}
