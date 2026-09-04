//! `humaux-application` — Humaux Thread workspace crate（布局见 §58）。
//!
//! 本 crate 的职责边界与依赖规则以 docs/architecture/Baseline_2.9.md 为唯一规范真源；
//! Domain 永不 import HTTP / SQLx / Qdrant / Provider SDK / ENV（§3 / §78.3）。

pub mod auth;
pub mod consolidate;
pub mod continuity;
pub mod contribute;
pub mod contribution_execution;
pub mod correct;
pub mod distill;
pub mod entitlement;
pub mod forget;
pub mod ingest;
pub mod notify;
pub mod pin;
pub mod public_evolve;
pub mod referral;
pub mod remember;
pub mod retrieval_embedding_port;
pub mod retrieve;
pub mod scheduler;
pub mod supersede;
