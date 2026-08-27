//! `humaux-adapters` — Humaux Thread workspace crate（布局见 §58）。
//!
//! 本 crate 的职责边界与依赖规则以 docs/architecture/Baseline_2.9.md 为唯一规范真源；
//! Domain 永不 import HTTP / SQLx / Qdrant / Provider SDK / ENV（§3 / §78.3）。

pub mod audit_sink;
pub mod batch;
pub mod byok;
pub mod consolidate_repo;
pub mod context_repo;
pub mod dashscope;
pub mod disclosure;
pub mod email;
pub mod exact_census;
pub mod forget_repo;
pub mod jobs;
pub mod model_call_ledger;
pub mod openbao;
pub mod postgres;
pub mod qdrant;
pub mod remember;
pub mod retrieve;
pub mod s3;
pub mod scheduler;
pub mod selection_repo;
pub mod serving_repo;
pub mod stream_repo;
pub mod valkey;
