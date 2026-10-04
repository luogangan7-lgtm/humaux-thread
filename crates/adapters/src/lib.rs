//! `humaux-adapters` — Humaux Thread workspace crate（布局见 §58）。
//! Depends-on: crates=[]; services=[]; env=[]; modules=[]
//! Called-by: [adapters::distill_repo, adapters::model_call_ledger, crate(humaux-admin), crate(humaux-consolidation-worker), crate(humaux-gateway), crate(humaux-maintenance), crate(humaux-private-worker), crate(humaux-public-worker), crate(humaux-retrieval-provider), crate(humaux-retrieval-worker), crate(xtask)]
//! Invariants: [crate root: the module list plus [`Counter`], the process-local cell of the three §41.2 private-plane
//!   counters (ADR-0061 card 34b); Domain/Application never import this crate's HTTP/SQL/Qdrant drivers (§3/§78.3)]
//! Spec: Baseline §3; §78.3; §41.2; ADR-0061 D-A; ADR-0061 D-C
//!
//! 本 crate 的职责边界与依赖规则以 docs/architecture/Baseline_2.9.md 为唯一规范真源；
//! Domain 永不 import HTTP / SQLx / Qdrant / Provider SDK / ENV（§3 / §78.3）。

pub mod affect_repo;
pub mod audit_sink;
pub mod batch;
pub mod byok;
pub mod confirm_token_repo;
pub mod consolidate_repo;
pub mod consolidation_reasoner;
pub mod context_repo;
pub mod continuity_read;
pub mod continuity_repo;
pub mod contribution_entry_repo;
pub mod contribution_execution_ingress;
pub mod contribution_execution_repo;
pub mod contribution_reasoner;
pub mod contribution_repo;
pub mod contribution_scan;
pub mod credential_repo;
pub mod dashscope;
pub mod disclosure;
pub mod distill_reasoner;
pub mod distill_repo;
pub mod email;
pub mod exact_census;
pub mod forget_repo;
pub mod health;
pub mod jobs;
pub mod mechanism_observation;
pub mod membership_repo;
pub mod memory_governance_repo;
pub mod model_call_ledger;
pub mod openbao;
pub mod operation_receipt;
pub mod placement_repo;
pub mod postgres;
pub mod private_inference_rpc;
pub mod private_projection_registry;
pub mod projection_worker;
pub mod provider_budget;
pub mod provisioning;
pub mod public_projection;
pub mod public_provenance;
pub mod public_repo;
pub mod qdrant;
pub mod quota_repo;
pub mod read_materialize;
pub mod reasoning_route_admission;
pub mod reasoning_route_onboarding;
pub mod remember;
pub mod request_guard_repo;
pub mod retrieval_embedding_rpc;
pub mod retrieval_query_source;
pub mod retrieve;
pub mod role_hygiene;
pub mod s3;
pub mod scheduler;
pub mod selection_repo;
pub mod serving_repo;
pub mod stream_repo;
pub mod subject_repo;
pub mod valkey;

use std::sync::atomic::{AtomicU64, Ordering};

/// One unlabeled, process-local §41.2 counter (ADR-0061 D-A: no metrics crate is linked). This crate has no
/// telemetry dependency, so the process that reaches the emit renders the value through `telemetry::metrics`
/// (ADR-0061 D-C); each static's `.inc(` is its family's one emit site (§41.2 R4).
pub struct Counter(AtomicU64);

impl Counter {
    /// A counter at 0.
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Adds `by`; resets only on process restart.
    pub fn inc(&self, by: u64) {
        self.0.fetch_add(by, Ordering::Relaxed);
    }

    /// The current value.
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}
