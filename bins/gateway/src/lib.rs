//! `humaux-gateway` — Trusted gateway entry points shared by HTTP and MCP transports.
//! Depends-on: crates=[]; services=[]; env=[]; modules=[]
//! Called-by: []
//! Invariants: []
//! Spec: none

pub mod auth;
pub mod bootstrap;
pub mod context;
pub mod continuity;
pub mod guard;
pub mod mcp_application;
mod memory;
pub mod recall;
pub mod remember;
pub mod retrieval_embedding_client;
