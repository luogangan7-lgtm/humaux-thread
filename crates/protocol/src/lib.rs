//! `humaux-protocol` — Humaux Thread workspace crate（布局见 §58）。
//! Depends-on: crates=[]; services=[];
//!   env=[]; modules=[]
//! Called-by: [crate(humaux-gateway), crate(xtask)]
//! Invariants: []
//! Spec: §58; §3; §78.3
//!
//! 本 crate 的职责边界与依赖规则以 docs/architecture/Baseline_2.9.md 为唯一规范真源；
//! Domain 永不 import HTTP / SQLx / Qdrant / Provider SDK / ENV（§3 / §78.3）。

pub mod edge;
pub mod error_map;
pub mod mcp;
pub mod mcp_catalog;
pub mod rest;
