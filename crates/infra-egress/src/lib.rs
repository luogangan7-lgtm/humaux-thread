//! `humaux-infra-egress` — Humaux Thread workspace crate（布局见 §58）。
//! Depends-on: crates=[]; services=[]; env=[]; modules=[]
//! Called-by: [crate(humaux-adapters), crate(humaux-retrieval-provider)]
//! Invariants: [crate root: external egress reaches reqwest only through humaux-infra-network and every external call
//!   requires an EgressPermit (§7.4)]
//! Spec: Baseline §3; §78.3
//!
//! 本 crate 的职责边界与依赖规则以 docs/architecture/Baseline_2.9.md 为唯一规范真源；
//! Domain 永不 import HTTP / SQLx / Qdrant / Provider SDK / ENV（§3 / §78.3）。

pub mod http;
pub mod raw;
pub mod resolver;
