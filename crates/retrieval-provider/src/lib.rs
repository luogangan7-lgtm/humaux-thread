//! `humaux-retrieval-provider` — Humaux Thread workspace crate（布局见 §58）。
//!
//! 本 crate 的职责边界与依赖规则以 docs/architecture/Baseline_2.8.md 为唯一规范真源；
//! Domain 永不 import HTTP / SQLx / Qdrant / Provider SDK / ENV（§3 / §78.3）。

pub mod adapters;
pub mod admission;
pub mod contract;
pub mod cost;
pub mod failover;
pub mod health;
pub mod pricing;
pub mod router;
