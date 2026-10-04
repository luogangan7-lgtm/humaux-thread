//! `humaux-domain` — Humaux Thread workspace crate（布局见 §58）。
//! Depends-on: crates=[]; services=[]; env=[]; modules=[]
//! Called-by: [crate(humaux-adapters), crate(humaux-admin), crate(humaux-application), crate(humaux-consolidation-worker), crate(humaux-gateway), crate(humaux-infra-cell), crate(humaux-infra-egress), crate(humaux-local-secret-scan), crate(humaux-maintenance), crate(humaux-private-worker), crate(humaux-projection), crate(humaux-protocol), crate(humaux-public-worker), crate(humaux-retrieval), crate(humaux-retrieval-provider), crate(humaux-retrieval-worker), crate(humaux-telemetry), crate(humaux-testkit), crate(xtask)]
//! Invariants: []
//! Spec: Baseline §3; §58; §78.3
//!
//! 本 crate 的职责边界与依赖规则以 docs/architecture/Baseline_2.9.md 为唯一规范真源；
//! Domain 永不 import HTTP / SQLx / Qdrant / Provider SDK / ENV（§3 / §78.3）。

pub mod affect;
pub mod audit;
pub mod authority;
pub mod boundary;
pub mod code;
pub mod confirm;
pub mod consolidate;
pub mod context;
pub mod continuity;
pub mod coordination;
pub mod dataclass;
pub mod egress;
pub mod error;
pub mod evidence;
pub mod grounding;
pub mod identity;
pub mod ids;
pub mod knowledge;
pub mod ledger;
pub mod lifecycle;
pub mod memory;
pub mod policy;
pub mod public;
pub mod selection;
pub mod subject;
pub mod temporal;
pub mod ticket_family;
