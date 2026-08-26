//! `retrieval-provider::admission` — §19 Provider Admission Controller, §19.2 Provider Budget,
//! §19 Provider Tenant Fairness (T7.3).
//!
//! Three pieces, in the order a caller uses them:
//!
//! 1. [`decide`] — the hierarchical admission check (§19 "Global Provider Budget -> Region
//!    Budget -> Tenant Budget -> Purpose Budget"), pure and in-memory, mirroring the split
//!    `crates/application/src/scheduler.rs` already established for §32.0 ("Domain 纯内存，
//!    adapters 做真实 DB" — the DB-backed config loader for [`AdmissionBudgets`]'s Global/
//!    Region tiers, `control.retrieval_provider_admission_limits`, migrations/0091, is a
//!    later adapters-layer task).
//! 2. [`classify_provider_status`] / [`bounded_backoff`] — §19.2 "429：有界退避；401：根据
//!    provider/key domain 进入 invalid/waiting_key；5xx：transient retry", for a caller that
//!    already went ahead and made the call and now has to decide what a failure means.
//! 3. [`TenantFairScheduler`] — §19 "Tenant Fair Scheduler 必须针对 embedding token demand 与
//!    rerank token demand 分别进行公平调度", Deficit Round Robin with per-tenant plan
//!    overrides for weight/queue priority/maximum burst.
//!
//! §78.1 freeze: nothing in this module bakes in a specific token/RPM/TPM number, a plan
//! name, or a backoff duration default — every threshold is a caller-supplied field.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use humaux_domain::error::ErrorCode;
use humaux_domain::ids::TenantId;

// =============================================================================
// §19 Admission Controller: request/decision types
// =============================================================================

/// §19 Admission Controller output — five states, exhaustive, no other value is producible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDecision {
    /// Every tier had room and the provider is not congested/blocked: send now.
    Accept,
    /// A soft (window-refillable) tier is momentarily full or the provider is rate-limited/
    /// unavailable, but this tenant's priority and deadline still allow waiting.
    Queue,
    /// Same congestion as `Queue`, but this tenant's priority is `Low` or its deadline has
    /// already passed — waiting would not help, so the request is dropped under backpressure
    /// rather than queued forever.
    Shed,
    /// A hard ceiling (§19.2 "monthly plan quota") is exhausted. No amount of queueing fixes
    /// this — distinct from `Shed`, which is about *this window's* contention, not the plan's
    /// period-long allowance.
    RejectQuota,
    /// A policy gate refused the request outright: the provider circuit breaker reports
    /// `PolicyBlocked` (§19 "Provider Health / Circuit Breaker"), or the tenant's plan does
    /// not entitle this purpose at all (§19 SaaS "允许套餐影响...provider budget；不得影响
    /// ...authorization").
    RejectPolicy,
}

/// §19 Admission input field 6/7 ("provider health", "deadline") — `provider health`'s full
/// five-state form (§19 "Provider Health / Circuit Breaker"); that monitor itself is a
/// separate module's deliverable (`retrieval-provider::health`, not built by this task) — this
/// module only consumes its output as an admission signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderHealthState {
    Healthy,
    Degraded,
    RateLimited,
    Unavailable,
    PolicyBlocked,
}

/// §19 Admission input field 2 ("tenant priority"). Coarse three-level ordinal, not a numeric
/// score — §78.1 forbids baking a specific priority *number* in here; a caller that needs a
/// finer ranking maps its own scale down to these three before calling [`decide`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TenantPriority {
    Low,
    Normal,
    High,
}

/// One bounded token pool: a ceiling plus how much of it is already spent. Used for §19.2's
/// "monthly plan quota" (hard, non-refilling within the period) here; the four hierarchy
/// tiers below use [`TierBudget`] instead, which additionally tracks a request-rate ceiling
/// (§19.2 "provider RPM/TPM limiter").
#[derive(Debug, Clone, Copy, Default)]
pub struct TokenBudget {
    pub max_tokens: u64,
    pub used_tokens: u64,
}

impl TokenBudget {
    pub fn remaining(&self) -> u64 {
        self.max_tokens.saturating_sub(self.used_tokens)
    }

    /// §19 core frozen decision: the unit is tokens, not requests — this is where that shows
    /// up mechanically. A 5k-token request and a 200k-token request both cost exactly one
    /// call to this function, but ask for very different amounts of `remaining()`.
    pub fn has_room(&self, tokens: u64) -> bool {
        self.remaining() >= tokens
    }
}

/// One hierarchy tier's admission state (§19.2 "provider RPM/TPM limiter"): both a token
/// ceiling and a request-count ceiling for the current window, each already tracking this
/// window's usage ("current TPM"/"current RPM" in §19's admission input list — folded into
/// the tier itself here rather than kept as separate flat fields on [`AdmissionRequest`],
/// since a tier's current utilization is what "current TPM"/"current RPM" *means*; the four
/// tiers below are evaluated together, not just one flat pair).
#[derive(Debug, Clone, Copy, Default)]
pub struct TierBudget {
    pub tpm_limit: u64,
    pub current_tpm: u64,
    pub rpm_limit: u64,
    pub current_rpm: u64,
}

impl TierBudget {
    /// `true` if admitting `tokens` more would keep both the token and request ceilings
    /// within limit for this window. Saturating math: a caller-supplied ceiling of 0 (a
    /// misconfigured row) fails closed (never admits) rather than panicking.
    pub fn admits(&self, tokens: u64) -> bool {
        self.current_tpm.saturating_add(tokens) <= self.tpm_limit
            && self.current_rpm.saturating_add(1) <= self.rpm_limit
    }
}

/// §19 "Global Provider Budget -> Region Budget -> Tenant Budget -> Purpose Budget" — the
/// admission hierarchy, evaluated as a logical AND across all four tiers (every tier must
/// have room, same "AND not OR" shape `application::scheduler::TieredLimiter` already
/// documents for its own three-layer check, §32.0).
#[derive(Debug, Clone, Copy, Default)]
pub struct AdmissionBudgets {
    pub global: TierBudget,
    pub region: TierBudget,
    pub tenant: TierBudget,
    pub purpose: TierBudget,
}

impl AdmissionBudgets {
    fn admits(&self, tokens: u64) -> bool {
        self.global.admits(tokens)
            && self.region.admits(tokens)
            && self.tenant.admits(tokens)
            && self.purpose.admits(tokens)
    }
}

/// §19 Admission input field 3 ("plan entitlement"). Every number here is caller-supplied
/// (§78.1) — this module defines no plan names, no default weight, no default quota.
#[derive(Debug, Clone, Copy)]
pub struct PlanEntitlement {
    /// §19 SaaS "不得影响...authorization": a plan that does not grant this call's purpose at
    /// all is a policy rejection (`RejectPolicy`), never a quota one.
    pub purpose_allowed: bool,
    /// §19.2 "monthly plan quota" — the hard, non-refilling ceiling for the current billing
    /// period.
    pub monthly_quota: TokenBudget,
    /// §19 SaaS "套餐可以改变...weight" — this tenant's DRR quantum, consumed via
    /// [`TenantFairQueue::push_with_plan`].
    pub fair_weight: u64,
    /// §19 SaaS "套餐可以改变...maximum burst" — caps how large this tenant's DRR deficit may
    /// grow while its head item waits, so a tenant cannot bank enough deficit to dominate.
    /// Consumed via [`TenantFairQueue::push_with_plan`].
    pub max_burst_tokens: u64,
}

/// §19 Admission Controller's full input list, fields 1/4/5 folded into [`AdmissionBudgets`]
/// (see [`TierBudget`]'s doc) — this struct carries the rest: field 1 (`estimated_input_tokens`),
/// field 2 (`tenant_priority`), field 3 (`plan_entitlement`), field 6 (`provider_health`),
/// field 7 (`deadline`).
#[derive(Debug, Clone, Copy)]
pub struct AdmissionRequest {
    pub estimated_input_tokens: u64,
    /// §19 SaaS "套餐可以改变...queue priority": this is already the plan-derived value — the
    /// caller maps its plan's queue priority onto [`TenantPriority`] before calling [`decide`]
    /// (same "caller-supplied, not a second parallel field on `PlanEntitlement`" shape
    /// `estimated_input_tokens`/`deadline` already use for their own inputs).
    pub tenant_priority: TenantPriority,
    pub plan_entitlement: PlanEntitlement,
    pub provider_health: ProviderHealthState,
    /// Time remaining until the caller's deadline, `None` = no deadline. `Some(Duration::ZERO)`
    /// (or already past) means the deadline is exceeded *now* — checked as `is_zero()`, not
    /// compared against a wall clock, so this module stays free of any time-source dependency
    /// (§3/§78.3 "Domain 永不 import...ENV" — no `SystemTime::now()` call lives here; the
    /// caller computes "time remaining" itself and passes the result in).
    pub deadline: Option<Duration>,
}

/// §19 Provider Admission Controller's decision function. Order matches the spec's own
/// framing: policy gates first (no budget math is meaningful if the provider or the plan
/// already refuses outright), then the hard monthly ceiling (`RejectQuota` — queueing cannot
/// fix an exhausted period), then the four soft hierarchy tiers plus provider congestion
/// (`Queue` vs `Shed`, decided by priority/deadline), and only then `Accept`.
pub fn decide(req: &AdmissionRequest, budgets: &AdmissionBudgets) -> AdmissionDecision {
    if req.provider_health == ProviderHealthState::PolicyBlocked {
        return AdmissionDecision::RejectPolicy;
    }
    if !req.plan_entitlement.purpose_allowed {
        return AdmissionDecision::RejectPolicy;
    }

    if !req
        .plan_entitlement
        .monthly_quota
        .has_room(req.estimated_input_tokens)
    {
        return AdmissionDecision::RejectQuota;
    }

    let tiers_have_room = budgets.admits(req.estimated_input_tokens);
    let provider_congested = matches!(
        req.provider_health,
        ProviderHealthState::RateLimited | ProviderHealthState::Unavailable
    );

    // §19 "Global Provider Budget -> Region Budget -> Tenant Budget -> Purpose Budget" plus
    // §72.5 `COST_BUDGET_EXCEEDED`: a request whose token cost structurally exceeds any one
    // tier's window ceiling can never fit that tier no matter how empty the window is —
    // `Queue`/`Shed` are for *this window's contention*, not for a request the ceiling can
    // never admit at all. Caught before the Queue/Shed split so it terminates instead of
    // looping forever.
    let unsatisfiable_by_construction = req.estimated_input_tokens > budgets.global.tpm_limit
        || req.estimated_input_tokens > budgets.region.tpm_limit
        || req.estimated_input_tokens > budgets.tenant.tpm_limit
        || req.estimated_input_tokens > budgets.purpose.tpm_limit;
    if unsatisfiable_by_construction {
        return AdmissionDecision::RejectQuota;
    }

    if !tiers_have_room || provider_congested {
        let deadline_exceeded = req.deadline.is_some_and(|d| d.is_zero());
        return if req.tenant_priority == TenantPriority::Low || deadline_exceeded {
            AdmissionDecision::Shed
        } else {
            AdmissionDecision::Queue
        };
    }

    AdmissionDecision::Accept
}

// =============================================================================
// §19.2 error handling: 429 有界退避 / 401 invalid|waiting_key / 5xx transient retry
// =============================================================================

/// §19 "Provider Credential 边界": which trust domain a provider credential belongs to
/// decides what a 401 means. Not the same axis as [`ProviderHealthState`] — health is what
/// the breaker currently believes about the provider; this is which credential a given call
/// used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKeyDomain {
    /// Humaux's own platform-managed Alibaba/DashScope credential (§19 "平台 Retrieval
    /// Credential...完全独立" from USER_REASONING/PLATFORM_PUBLIC). A 401 here is an ops
    /// misconfiguration or a credential that needs rotation — nothing to wait on.
    PlatformManaged,
    /// §19 "未来...CUSTOMER_RETRIEVAL_BYOK" — not yet a real trust domain, but the spec names
    /// it explicitly as the other half of the invalid/waiting_key split. A 401 here mirrors
    /// `adapters::byok`'s existing USER_REASONING convention (`ReasoningProviderError::
    /// WaitingKey`): the tenant simply has not supplied a working key yet, pending, not
    /// failed.
    CustomerRetrievalByok,
}

/// §19.2's HTTP-status half of the error-handling rule, as a pure function — same shape as
/// `adapters::byok::classify_http_status` (different crate: USER_REASONING's 401 is always
/// `WaitingKey` there because every USER_REASONING credential is inherently a per-tenant BYOK
/// key; this Plane's credential is platform-managed by default, so the 401 split needs the
/// extra `key_domain` parameter that function does not have).
///
/// Maps onto the existing `ErrorCode` closed set (§52.1) rather than a new error type —
/// `ProviderRateLimited`/`WaitingKey`/`Unauthorized`/`ProviderTransient`/`ProviderPermanent`
/// already cover every case this function needs.
pub fn classify_provider_status(status: u16, key_domain: ProviderKeyDomain) -> Option<ErrorCode> {
    match status {
        200..=299 => None,
        401 => Some(match key_domain {
            ProviderKeyDomain::PlatformManaged => ErrorCode::Unauthorized,
            ProviderKeyDomain::CustomerRetrievalByok => ErrorCode::WaitingKey,
        }),
        403 => Some(ErrorCode::ProviderPermanent),
        429 => Some(ErrorCode::ProviderRateLimited),
        // §11.3 groups "timeout" with 5xx as transient, not permanent — same as the sibling
        // classifier this function's doc claims to mirror, `adapters::byok::
        // classify_http_status` (408 | 425 => RetryWait).
        408 | 425 => Some(ErrorCode::ProviderTransient),
        500..=599 => Some(ErrorCode::ProviderTransient),
        _ => Some(ErrorCode::ProviderPermanent),
    }
}

/// §19.2 "429：有界退避". `retry_after` (an honored `Retry-After` header) takes precedence but
/// is still clamped to `max` — an unclamped provider-supplied value would make "bounded"
/// depend on a caller that does not exist yet, not on this function. Absent that, exponential
/// off `base` with a fixed cap — `attempt.min(4)` bounds growth at 16x `base`, so a runaway
/// attempt counter can never produce an unbounded sleep. Same shape `adapters::byok::
/// backoff_for_attempt` already uses (different crate, not reusable directly — the formula is
/// cheaper to repeat than to add a cross-crate dependency for); `base`/`max` are caller-
/// supplied per §78.1 rather than baked in here, unlike that sibling's fixed 200ms/16x.
pub fn bounded_backoff(
    attempt: u32,
    retry_after: Option<Duration>,
    base: Duration,
    max: Duration,
) -> Duration {
    let computed = retry_after.unwrap_or_else(|| base * 2u32.pow(attempt.min(4)));
    computed.min(max)
}

// =============================================================================
// §19 Provider Tenant Fairness: per-purpose Deficit Round Robin
// =============================================================================

/// The Retrieval Provider Plane's only two purposes (§19 "Retrieval Provider Plane...职责只
/// 针对...dense embedding, rerank"). Deliberately not `humaux_domain::egress::
/// PrivateDataPurpose` directly: that enum's third variant (`UserReasoning`) is outside this
/// Plane's scope by the same freeze ("不扩展成通用聊天 LLM Gateway") — accepting it here would
/// let a non-retrieval purpose flow into a fair scheduler that spec explicitly narrows to
/// embedding/rerank. [`From`] below is the one-way bridge back to the egress type for the
/// (separate, later) `EgressPermit` call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RetrievalPurpose {
    Embedding,
    Rerank,
}

impl From<RetrievalPurpose> for humaux_domain::egress::PrivateDataPurpose {
    fn from(purpose: RetrievalPurpose) -> Self {
        match purpose {
            RetrievalPurpose::Embedding => Self::RetrievalEmbedding,
            RetrievalPurpose::Rerank => Self::RetrievalRerank,
        }
    }
}

struct QueueItem<T> {
    item: T,
    cost: u64,
}

/// §19 "Deficit Round Robin / weighted fair scheduling...而不是 first tenant fills queue
/// first" for one purpose's demand queue. Classic DRR (same algorithm as `application::
/// scheduler::TenantFairQueue`, §32.0) extended with per-tenant plan overrides: `weight` sets
/// this tenant's quantum (§19 SaaS "套餐可以改变weight"), `max_burst` caps how much deficit it
/// can bank while idle (§19 SaaS "...maximum burst"). Neither override can drop, duplicate, or
/// leak another tenant's items — see this module's tests
/// `weight_does_not_affect_completeness_or_isolation` for the frozen guarantee (§19 SaaS "不
/// 得影响...exact enumeration correctness...authorization").
///
/// ponytail: `order`/`queues`/`deficits`/`quanta`/`burst_caps` never drop a tenant once seen,
/// so both per-call scan cost and memory grow with tenants-ever-seen rather than
/// tenants-currently-active — same ceiling `application::scheduler::TenantFairQueue` already
/// carries. Upgrade path: drop a tenant's entries from all five maps/`order` when its queue
/// drains, once a long-lived process on this Plane's path makes tenant churn measurable.
pub struct TenantFairQueue<T> {
    default_quantum: u64,
    quanta: HashMap<TenantId, u64>,
    burst_caps: HashMap<TenantId, u64>,
    queues: HashMap<TenantId, VecDeque<QueueItem<T>>>,
    deficits: HashMap<TenantId, u64>,
    /// Stable rotation order — `HashMap` iteration order is not guaranteed, and fair rotation
    /// needs one.
    order: VecDeque<TenantId>,
}

impl<T> TenantFairQueue<T> {
    pub fn new(default_quantum: u64) -> Self {
        Self {
            default_quantum,
            quanta: HashMap::new(),
            burst_caps: HashMap::new(),
            queues: HashMap::new(),
            deficits: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    /// Enqueues one item for `tenant`. `weight`/`max_burst` register this tenant's plan
    /// overrides on its *first* appearance only — a plan change for a tenant already mid-queue
    /// is a future task's concern (`Some` on a later `push` for the same tenant is silently
    /// ignored, not applied).
    pub fn push(
        &mut self,
        tenant: TenantId,
        item: T,
        cost: u64,
        weight: Option<u64>,
        max_burst: Option<u64>,
    ) {
        if !self.queues.contains_key(&tenant) {
            self.order.push_back(tenant);
            self.deficits.insert(tenant, 0);
            if let Some(w) = weight {
                self.quanta.insert(tenant, w);
            }
            if let Some(b) = max_burst {
                self.burst_caps.insert(tenant, b);
            }
        }
        self.queues
            .entry(tenant)
            .or_default()
            .push_back(QueueItem { item, cost });
    }

    /// [`push`](Self::push) with `weight`/`max_burst` derived from a [`PlanEntitlement`]
    /// instead of passed as loose `Option<u64>` literals — the one intended call shape for a
    /// real caller (§19 SaaS "套餐可以改变weight/...maximum burst"), so `fair_weight`/
    /// `max_burst_tokens` have exactly one reader each rather than sitting on the struct
    /// unread.
    pub fn push_with_plan(&mut self, tenant: TenantId, item: T, cost: u64, plan: &PlanEntitlement) {
        self.push(
            tenant,
            item,
            cost,
            Some(plan.fair_weight),
            Some(plan.max_burst_tokens),
        );
    }

    /// DRR core loop — see `application::scheduler::TenantFairQueue::next_ready`'s doc for the
    /// bounded-scan rationale (identical here): at most one full rotation per call, `None` if
    /// no tenant's deficit covers its head cost yet this round (not exhaustion — the caller
    /// keeps calling on the next tick).
    pub fn next_ready(&mut self) -> Option<(TenantId, T)> {
        let rounds = self.order.len();
        for _ in 0..rounds.max(1) {
            let tenant = self.order.pop_front()?;
            self.order.push_back(tenant);

            let is_empty = self.queues.get(&tenant).is_none_or(|q| q.is_empty());
            if is_empty {
                continue;
            }

            let quantum = self
                .quanta
                .get(&tenant)
                .copied()
                .unwrap_or(self.default_quantum);
            let cap = self.burst_caps.get(&tenant).copied().unwrap_or(u64::MAX);
            let queue = self
                .queues
                .get_mut(&tenant)
                .expect("checked non-empty above");
            let head_cost = queue.front().expect("checked non-empty above").cost;
            let deficit = self.deficits.entry(tenant).or_insert(0);
            // §19 SaaS "maximum burst 不得影响...exact enumeration correctness": `cap` bounds
            // how much deficit an *idle* tenant may bank, but must never sit below the head
            // item's own cost — otherwise an item costlier than `cap` could never clear the
            // gate at all (an infinite, silent stall, not a burst limit).
            *deficit = deficit.saturating_add(quantum).min(cap.max(head_cost));
            if *deficit >= head_cost {
                *deficit -= head_cost;
                let popped = queue.pop_front().expect("checked non-empty above");
                return Some((tenant, popped.item));
            }
        }
        None
    }
}

/// §19 "对 embedding token demand 与 rerank token demand 分别进行公平调度" — two independent
/// [`TenantFairQueue`]s, one per [`RetrievalPurpose`]. A tenant flooding the embedding queue
/// has no effect on the rerank queue's fairness and vice versa.
pub struct TenantFairScheduler<T> {
    pub embedding: TenantFairQueue<T>,
    pub rerank: TenantFairQueue<T>,
}

impl<T> TenantFairScheduler<T> {
    pub fn new(default_quantum: u64) -> Self {
        Self {
            embedding: TenantFairQueue::new(default_quantum),
            rerank: TenantFairQueue::new(default_quantum),
        }
    }

    fn queue_mut(&mut self, purpose: RetrievalPurpose) -> &mut TenantFairQueue<T> {
        match purpose {
            RetrievalPurpose::Embedding => &mut self.embedding,
            RetrievalPurpose::Rerank => &mut self.rerank,
        }
    }

    pub fn push(
        &mut self,
        purpose: RetrievalPurpose,
        tenant: TenantId,
        item: T,
        cost: u64,
        weight: Option<u64>,
        max_burst: Option<u64>,
    ) {
        self.queue_mut(purpose)
            .push(tenant, item, cost, weight, max_burst);
    }

    /// See [`TenantFairQueue::push_with_plan`].
    pub fn push_with_plan(
        &mut self,
        purpose: RetrievalPurpose,
        tenant: TenantId,
        item: T,
        cost: u64,
        plan: &PlanEntitlement,
    ) {
        self.queue_mut(purpose)
            .push_with_plan(tenant, item, cost, plan);
    }

    pub fn next_ready(&mut self, purpose: RetrievalPurpose) -> Option<(TenantId, T)> {
        self.queue_mut(purpose).next_ready()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------------------
    // §19 core frozen decision: budget unit is tokens, not requests.
    // -------------------------------------------------------------------------------------

    fn full_room_budgets() -> AdmissionBudgets {
        let tier = TierBudget {
            tpm_limit: 10_000,
            current_tpm: 0,
            rpm_limit: 100,
            current_rpm: 0,
        };
        AdmissionBudgets {
            global: tier,
            region: tier,
            tenant: tier,
            purpose: tier,
        }
    }

    fn base_request(tokens: u64) -> AdmissionRequest {
        AdmissionRequest {
            estimated_input_tokens: tokens,
            tenant_priority: TenantPriority::Normal,
            plan_entitlement: PlanEntitlement {
                purpose_allowed: true,
                monthly_quota: TokenBudget {
                    max_tokens: 1_000_000,
                    used_tokens: 0,
                },
                fair_weight: 1,
                max_burst_tokens: u64::MAX,
            },
            provider_health: ProviderHealthState::Healthy,
            deadline: None,
        }
    }

    #[test]
    fn admission_budget_is_measured_in_tokens_not_request_count() {
        // §19 frozen: "1 embedding request with 5k tokens != 1 embedding request with 200k
        // tokens". Same tier ceiling (10k TPM), same RPM headroom (both are exactly 1
        // request) — only the token count differs, and only the token count may flip the
        // decision.
        let small = base_request(5_000);
        let large = base_request(200_000);

        assert_eq!(
            decide(&small, &full_room_budgets()),
            AdmissionDecision::Accept
        );
        assert_eq!(
            decide(&large, &full_room_budgets()),
            AdmissionDecision::RejectQuota,
            "a 200k-token request against a 10k TPM tier can never fit any window of that \
             tier — this must terminate as RejectQuota, not sit in Queue forever just because \
             both are 'one request' (the budget unit is tokens, §19, not requests)"
        );
    }

    // -------------------------------------------------------------------------------------
    // Five-state coverage.
    // -------------------------------------------------------------------------------------

    #[test]
    fn accept_when_every_tier_has_room_and_provider_is_healthy() {
        assert_eq!(
            decide(&base_request(100), &full_room_budgets()),
            AdmissionDecision::Accept
        );
    }

    #[test]
    fn queue_when_a_tier_is_tight_but_priority_and_deadline_allow_waiting() {
        let mut req = base_request(9_000);
        req.tenant_priority = TenantPriority::High;
        req.deadline = Some(Duration::from_secs(30));
        let mut budgets = full_room_budgets();
        budgets.tenant.current_tpm = 5_000; // only 5k of 10k TPM left this window
        assert_eq!(decide(&req, &budgets), AdmissionDecision::Queue);
    }

    #[test]
    fn shed_when_tier_is_tight_and_priority_is_low() {
        let mut req = base_request(9_000);
        req.tenant_priority = TenantPriority::Low;
        let mut budgets = full_room_budgets();
        budgets.tenant.current_tpm = 5_000;
        assert_eq!(decide(&req, &budgets), AdmissionDecision::Shed);
    }

    #[test]
    fn shed_when_tier_is_tight_and_deadline_already_exceeded() {
        let mut req = base_request(9_000);
        req.tenant_priority = TenantPriority::High; // priority alone does not save a blown deadline
        req.deadline = Some(Duration::ZERO);
        let mut budgets = full_room_budgets();
        budgets.tenant.current_tpm = 5_000;
        assert_eq!(decide(&req, &budgets), AdmissionDecision::Shed);
    }

    #[test]
    fn reject_quota_when_request_structurally_exceeds_a_tier_ceiling() {
        // §19/§72.5: a request that can never fit *any* window of a tier (not just this
        // window's current usage) must terminate rather than sit in `Queue` forever. High
        // priority + no deadline would otherwise pick `Queue` — the ceiling breach must win.
        let mut req = base_request(200_000);
        req.tenant_priority = TenantPriority::High;
        req.deadline = None;
        assert_eq!(
            decide(&req, &full_room_budgets()), // every tier's tpm_limit is 10_000
            AdmissionDecision::RejectQuota
        );
    }

    #[test]
    fn reject_quota_when_monthly_plan_ceiling_is_exhausted() {
        let mut req = base_request(100);
        req.plan_entitlement.monthly_quota = TokenBudget {
            max_tokens: 50,
            used_tokens: 50,
        };
        // Window tiers are wide open — only the hard monthly quota is the problem, and no
        // amount of queueing fixes that, unlike the soft-tier cases above.
        assert_eq!(
            decide(&req, &full_room_budgets()),
            AdmissionDecision::RejectQuota
        );
    }

    #[test]
    fn reject_policy_when_provider_circuit_breaker_reports_policy_blocked() {
        let mut req = base_request(100);
        req.provider_health = ProviderHealthState::PolicyBlocked;
        assert_eq!(
            decide(&req, &full_room_budgets()),
            AdmissionDecision::RejectPolicy
        );
    }

    #[test]
    fn reject_policy_when_plan_does_not_entitle_this_purpose() {
        let mut req = base_request(100);
        req.plan_entitlement.purpose_allowed = false;
        assert_eq!(
            decide(&req, &full_room_budgets()),
            AdmissionDecision::RejectPolicy
        );
    }

    #[test]
    fn degraded_health_alone_still_admits_when_tiers_have_room() {
        // §19 "Circuit Breaker 只影响 new admission" — Degraded is a signal, not a stop; only
        // RateLimited/Unavailable/PolicyBlocked change the outcome here.
        let mut req = base_request(100);
        req.provider_health = ProviderHealthState::Degraded;
        assert_eq!(
            decide(&req, &full_room_budgets()),
            AdmissionDecision::Accept
        );
    }

    // -------------------------------------------------------------------------------------
    // §19.2 error handling: 429 / 401 invalid|waiting_key / 5xx.
    // -------------------------------------------------------------------------------------

    #[test]
    fn status_429_maps_to_provider_rate_limited_with_bounded_backoff() {
        assert_eq!(
            classify_provider_status(429, ProviderKeyDomain::PlatformManaged),
            Some(ErrorCode::ProviderRateLimited)
        );
        // "有界": four sequential attempt numbers never produce an unbounded sleep.
        let base = Duration::from_millis(200);
        let max = Duration::from_secs(10);
        let d0 = bounded_backoff(0, None, base, max);
        let d4 = bounded_backoff(4, None, base, max);
        let d9 = bounded_backoff(9, None, base, max); // way past the min(4) cap
        assert!(d0 < d4);
        assert_eq!(
            d4, d9,
            "growth must be capped at attempt.min(4), not keep doubling"
        );
        assert!(
            d9 <= max,
            "backoff must stay bounded even for a runaway attempt count"
        );
    }

    #[test]
    fn status_429_honors_retry_after_over_the_computed_backoff() {
        let base = Duration::from_millis(200);
        let max = Duration::from_secs(10);
        let honored = bounded_backoff(0, Some(Duration::from_secs(3)), base, max);
        assert_eq!(honored, Duration::from_secs(3));
    }

    #[test]
    fn status_429_clamps_an_absurd_retry_after_to_max() {
        // The provider-supplied `Retry-After` is untrusted input — "bounded" must not depend
        // on the provider being reasonable.
        let base = Duration::from_millis(200);
        let max = Duration::from_secs(10);
        let clamped = bounded_backoff(0, Some(Duration::from_secs(86_400)), base, max);
        assert_eq!(clamped, max);
    }

    #[test]
    fn status_401_splits_by_key_domain_invalid_vs_waiting_key() {
        assert_eq!(
            classify_provider_status(401, ProviderKeyDomain::PlatformManaged),
            Some(ErrorCode::Unauthorized),
            "platform-managed credential 401 is an ops misconfiguration, not a wait state"
        );
        assert_eq!(
            classify_provider_status(401, ProviderKeyDomain::CustomerRetrievalByok),
            Some(ErrorCode::WaitingKey),
            "customer BYOK credential 401 is pending, same convention as USER_REASONING"
        );
    }

    #[test]
    fn status_5xx_maps_to_transient_retry() {
        for status in [500u16, 502, 503, 599] {
            assert_eq!(
                classify_provider_status(status, ProviderKeyDomain::PlatformManaged),
                Some(ErrorCode::ProviderTransient),
                "status {status} must be transient-retryable"
            );
        }
    }

    #[test]
    fn status_408_and_425_map_to_transient_retry_not_permanent() {
        // §11.3 groups "timeout" with 5xx as transient — same table `adapters::byok::
        // classify_http_status` implements for 408/425.
        for status in [408u16, 425] {
            assert_eq!(
                classify_provider_status(status, ProviderKeyDomain::PlatformManaged),
                Some(ErrorCode::ProviderTransient),
                "status {status} (timeout) must be transient-retryable, not permanent"
            );
        }
    }

    #[test]
    fn status_2xx_is_not_an_error() {
        assert_eq!(
            classify_provider_status(200, ProviderKeyDomain::PlatformManaged),
            None
        );
    }

    // -------------------------------------------------------------------------------------
    // §19 Tenant Fair Scheduler: DRR, per-purpose, weight/burst plan overrides.
    // -------------------------------------------------------------------------------------

    #[test]
    fn embedding_and_rerank_demand_are_scheduled_independently() {
        let mut sched: TenantFairScheduler<&'static str> = TenantFairScheduler::new(1);
        let tenant = TenantId::new();
        // Flood only the embedding queue; the rerank queue stays empty.
        for _ in 0..5 {
            sched.push(RetrievalPurpose::Embedding, tenant, "e", 1, None, None);
        }
        assert!(
            sched.next_ready(RetrievalPurpose::Rerank).is_none(),
            "an empty rerank queue must stay empty regardless of embedding-queue depth"
        );
        assert!(sched.next_ready(RetrievalPurpose::Embedding).is_some());
    }

    #[test]
    fn flooding_tenant_does_not_starve_a_lower_cost_tenant() {
        let mut q: TenantFairQueue<u32> = TenantFairQueue::new(1);
        let heavy = TenantId::new();
        let light = TenantId::new();
        for i in 0..3 {
            q.push(heavy, i, 10, None, None); // default weight, expensive items
        }
        for i in 0..3 {
            q.push(light, i, 1, None, None); // default weight, cheap items
        }

        let mut served: HashMap<TenantId, u32> = HashMap::new();
        for _ in 0..60 {
            if let Some((t, _)) = q.next_ready() {
                *served.entry(t).or_insert(0) += 1;
            }
        }
        let heavy_served = served.get(&heavy).copied().unwrap_or(0);
        let light_served = served.get(&light).copied().unwrap_or(0);
        eprintln!(
            "throughput comparison over 60 ticks: heavy(cost=10)={heavy_served} \
             light(cost=1)={light_served}"
        );
        assert_eq!(
            heavy_served, 3,
            "the flooding tenant must still fully drain, not be cut off"
        );
        assert_eq!(
            light_served, 3,
            "the cheap tenant must not be starved by the expensive one"
        );
    }

    #[test]
    fn push_with_plan_derives_weight_and_burst_from_the_plan_entitlement() {
        // §19 SaaS "套餐可以改变weight/...maximum burst": `push_with_plan` must produce the
        // same admission behaviour as calling `push` with those two fields extracted by hand
        // — the derivation is the only thing this method adds.
        let mut via_plan: TenantFairQueue<u32> = TenantFairQueue::new(1);
        let mut via_literals: TenantFairQueue<u32> = TenantFairQueue::new(1);
        let tenant = TenantId::new();
        let plan = PlanEntitlement {
            purpose_allowed: true,
            monthly_quota: TokenBudget::default(),
            fair_weight: 4,
            max_burst_tokens: 4,
        };
        via_plan.push_with_plan(tenant, 0, 4, &plan);
        via_literals.push(
            tenant,
            0,
            4,
            Some(plan.fair_weight),
            Some(plan.max_burst_tokens),
        );
        assert_eq!(
            via_plan.next_ready().map(|(t, _)| t),
            via_literals.next_ready().map(|(t, _)| t),
            "push_with_plan must be equivalent to push with the plan's own weight/max_burst"
        );
    }

    #[test]
    fn plan_weight_gives_higher_throughput_over_many_ticks() {
        // §19 SaaS "套餐可以改变weight": with items too expensive for the default quantum to
        // clear every visit (cost=4 > default quantum=1), a 4x-weight tenant's deficit covers
        // its head cost on every visit while a default-weight tenant only covers it on every
        // 4th — so over enough ticks the weighted tenant's served count must come out ahead,
        // without starving the default tenant to zero.
        let mut q: TenantFairQueue<u32> = TenantFairQueue::new(1);
        let weighted = TenantId::new();
        let default_tenant = TenantId::new();
        for i in 0..20 {
            q.push(weighted, i, 4, Some(4), None); // 4x quantum, matches cost exactly
        }
        for i in 0..20 {
            q.push(default_tenant, i, 4, None, None); // default quantum = 1, cost = 4
        }

        let mut served: HashMap<TenantId, u32> = HashMap::new();
        for _ in 0..40 {
            if let Some((t, _)) = q.next_ready() {
                *served.entry(t).or_insert(0) += 1;
            }
        }
        let weighted_served = served.get(&weighted).copied().unwrap_or(0);
        let default_served = served.get(&default_tenant).copied().unwrap_or(0);
        eprintln!(
            "throughput comparison over 40 ticks: weighted(x4 quantum)={weighted_served} \
             default(x1 quantum)={default_served}"
        );
        assert!(
            weighted_served > default_served,
            "a 4x-weight tenant must be served more often than a default-weight tenant \
             against the same item cost: weighted={weighted_served} default={default_served}"
        );
        assert!(
            default_served > 0,
            "the default-weight tenant must still make progress, not be starved to zero \
             just because another tenant has a higher weight"
        );
    }

    #[test]
    fn weight_does_not_affect_completeness_or_isolation() {
        // §19 SaaS "套餐可以改变weight/queue priority/maximum burst，但不得影响...exact
        // enumeration correctness...authorization...deletion": every pushed item must come
        // back exactly once, attributed to its own tenant, no matter the weight skew — and,
        // critically, no matter how small `max_burst` is relative to an item's own cost. A
        // `max_burst` below every item's cost (here 1 < cost 3) must never leave items stuck
        // forever — completeness must win over the idle-burst cap, not the other way round.
        let mut q: TenantFairQueue<u32> = TenantFairQueue::new(1);
        let heavy_weight = TenantId::new();
        let default_tenant = TenantId::new();
        for i in 0..7 {
            q.push(heavy_weight, i, 3, Some(9), Some(1));
        }
        for i in 0..7 {
            q.push(default_tenant, i, 3, None, None);
        }

        let mut served_by_tenant: HashMap<TenantId, Vec<u32>> = HashMap::new();
        for _ in 0..500 {
            if let Some((t, item)) = q.next_ready() {
                served_by_tenant.entry(t).or_default().push(item);
            }
        }
        let mut heavy_items = served_by_tenant.remove(&heavy_weight).unwrap_or_default();
        let mut default_items = served_by_tenant.remove(&default_tenant).unwrap_or_default();
        heavy_items.sort_unstable();
        default_items.sort_unstable();
        let expected: Vec<u32> = (0..7).collect();
        assert_eq!(
            heavy_items, expected,
            "no drop/duplicate for the heavy-weight tenant"
        );
        assert_eq!(
            default_items, expected,
            "no drop/duplicate leaked onto the other tenant"
        );
    }

    #[test]
    fn burst_cap_bounds_deficit_growth_while_the_head_item_waits() {
        // §19 SaaS "maximum burst": a tenant whose head item needs several rounds to
        // accumulate enough deficit must not bank *more* than `cap` while waiting — quantum=2
        // would reach 6 on the 3rd round uncapped, but cap=5 must hold it at exactly 5.
        let mut q: TenantFairQueue<u32> = TenantFairQueue::new(2);
        let capped = TenantId::new();
        q.push(capped, 0, 5, None, Some(5)); // burst cap = 5 = this item's own cost
        assert!(
            q.next_ready().is_none(),
            "deficit=2 < cost=5, must not fire yet"
        );
        assert!(
            q.next_ready().is_none(),
            "deficit=4 < cost=5, must not fire yet"
        );
        assert_eq!(
            q.next_ready().map(|(t, _)| t),
            Some(capped),
            "deficit reaches exactly cap=5 on the 3rd round and must fire then, not be held \
             below its own cost by an off-by-something in the cap"
        );
    }

    #[test]
    fn burst_cap_is_floored_at_head_cost_so_an_expensive_item_still_clears() {
        // §19 blocker regression: `max_burst` (here 3) may be smaller than a queued item's
        // cost (100) — the cap must floor at the head item's own cost so the item still
        // dequeues within a bounded number of ticks, never stalls forever. Before the fix,
        // capping the *working* deficit at `cap` (ignoring `head_cost`) made this item
        // undequeuable for any number of ticks — a plan setting (max_burst) silently
        // destroying completeness, which §19 SaaS freezes as forbidden.
        let mut q: TenantFairQueue<u32> = TenantFairQueue::new(10);
        let capped = TenantId::new();
        q.push(capped, 0, 100, None, Some(3));
        let mut dequeued = false;
        for _ in 0..20 {
            if q.next_ready().map(|(t, _)| t) == Some(capped) {
                dequeued = true;
                break;
            }
        }
        assert!(
            dequeued,
            "cost=100 item with max_burst=3 must dequeue within a bounded number of ticks, \
             not stall forever"
        );
    }
}
