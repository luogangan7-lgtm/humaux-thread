// ADR-0019 D-A: a `ConfirmedUserActor` is minted only from a consumed pin/unpin confirmation.
// Consolidation / retention / private-worker code holding a `BindingRequest` has no literal,
// no `Default`, no `new()` — the field is private to `humaux_domain::context`. This must fail
// to *compile*, not fail an assertion at runtime (same technique as
// `fail_bound_to_auto_mutable.rs`).
use humaux_domain::authority::MemoryId;
use humaux_domain::context::{BindingMode, BindingRequest, ConfirmedUserActor, ScopeKind, authorize_pinned};

fn main() {
    let req = BindingRequest {
        mode: BindingMode::Pinned,
        scope_kind: ScopeKind::Tenant,
        scope_id: None,
        memory_id: MemoryId::new(),
    };
    let forged = ConfirmedUserActor { _priv: () };
    let _grant = authorize_pinned(Some(&forged), req);
}
