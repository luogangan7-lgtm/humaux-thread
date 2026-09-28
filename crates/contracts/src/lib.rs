//! `humaux-contracts` — Humaux Thread workspace crate（布局见 §58）。
//! Depends-on: crates=[]; services=[]; env=[]; modules=[]
//! Called-by: [crate(humaux-adapters), crate(humaux-admin), crate(humaux-gateway), crate(humaux-infra-egress), crate(humaux-retrieval), crate(humaux-retrieval-provider), crate(xtask)]
//! Invariants: []
//! Spec: Baseline §3; §58; §78.3
//!
//! 本 crate 的职责边界与依赖规则以 docs/architecture/Baseline_2.9.md 为唯一规范真源；
//! Domain 永不 import HTTP / SQLx / Qdrant / Provider SDK / ENV（§3 / §78.3）。

pub mod config_registry;
pub mod feature_registry;
pub mod mechanism_registry;
pub mod retrieval_config;
