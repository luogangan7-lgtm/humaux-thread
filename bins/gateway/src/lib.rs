//! Trusted gateway entry points shared by HTTP and MCP transports.

pub mod auth;
pub mod bootstrap;
pub mod context;
pub mod continuity;
pub mod guard;
pub mod mcp_application;
mod memory;
pub mod recall;
pub mod remember;
