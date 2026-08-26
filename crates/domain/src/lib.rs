//! `humaux-domain` — Humaux Thread workspace crate（布局见 §58）。
//!
//! 本 crate 的职责边界与依赖规则以 docs/architecture/Baseline_2.8.md 为唯一规范真源；
//! Domain 永不 import HTTP / SQLx / Qdrant / Provider SDK / ENV（§3 / §78.3）。

pub mod audit;
pub mod authority;
pub mod boundary;
pub mod code;
pub mod consolidate;
pub mod context;
pub mod coordination;
pub mod dataclass;
pub mod egress;
pub mod error;
pub mod evidence;
pub mod identity;
pub mod ids;
pub mod knowledge;
pub mod memory;
pub mod policy;
pub mod public;
pub mod temporal;
