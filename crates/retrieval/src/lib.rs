//! `humaux-retrieval` — Humaux Thread workspace crate（布局见 §58）。
//!
//! 本 crate 的职责边界与依赖规则以 docs/architecture/Baseline_2.8.md 为唯一规范真源；
//! Domain 永不 import HTTP / SQLx / Qdrant / Provider SDK / ENV（§3 / §78.3）。

pub mod planner;
pub mod candidate;
pub mod fusion;
pub mod rerank;
pub mod completeness;
pub mod compiler;
