//! `humaux-retrieval-worker` library facet — exists solely so `bins/gateway`'s
//! `tests/query_embedding_rpc.rs` can spawn the real ADR-0012 RPC app (`rpc::router`,
//! `rpc::RpcState`, `rpc::PeerIdentity`) in-process against a temporary Unix domain socket,
//! instead of re-implementing (and thereby drifting from) the worker's own handler. The
//! process entry point stays `src/main.rs`; Cargo builds both targets from this one package
//! automatically once both `src/lib.rs` and `src/main.rs` exist.

pub mod rpc;
