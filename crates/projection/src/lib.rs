//! `humaux-projection` — Humaux Thread workspace crate（布局见 §58）。
//! Depends-on: crates=[]; services=[]; env=[]; modules=[]
//! Called-by: [crate(humaux-adapters), crate(humaux-gateway), crate(humaux-local-secret-scan), crate(humaux-private-worker), crate(humaux-retrieval-provider), crate(xtask)]
//! Invariants: [crate root: projection logic stays pure (no HTTP/SQLx/Qdrant/provider SDK, §3/§78.3); IO lives in
//!   humaux-adapters]
//! Spec: Baseline §3; §78.3
//!
//! 本 crate 的职责边界与依赖规则以 docs/architecture/Baseline_2.9.md 为唯一规范真源；
//! Domain 永不 import HTTP / SQLx / Qdrant / Provider SDK / ENV（§3 / §78.3）。

pub mod card;
pub mod code;
pub mod dense;
pub mod fingerprint;
pub mod graph;
pub mod serving;
pub mod sparse;
pub mod stream;
