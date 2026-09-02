//! `humaux-adapters` — Humaux Thread workspace crate（布局见 §58）。
//!
//! 本 crate 的职责边界与依赖规则以 docs/architecture/Baseline_2.9.md 为唯一规范真源；
//! Domain 永不 import HTTP / SQLx / Qdrant / Provider SDK / ENV（§3 / §78.3）。

pub mod audit_sink;
pub mod batch;
pub mod byok;
pub mod consolidate_repo;
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
pub mod email;
pub mod exact_census;
pub mod forget_repo;
pub mod jobs;
pub mod mechanism_observation;
pub mod model_call_ledger;
pub mod openbao;
pub mod operation_receipt;
pub mod postgres;
pub mod private_projection_registry;
pub mod projection_worker;
pub mod provider_budget;
pub mod public_projection;
pub mod public_provenance;
pub mod public_repo;
pub mod qdrant;
pub mod quota_repo;
pub mod read_materialize;
pub mod reasoning_route_admission;
pub mod remember;
pub mod request_guard_repo;
pub mod retrieval_query_source;
pub mod retrieve;
pub mod s3;
pub mod scheduler;
pub mod selection_repo;
pub mod serving_repo;
pub mod stream_repo;
pub mod valkey;
