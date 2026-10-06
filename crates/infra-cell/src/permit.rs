//! `infra-cell::permit` — ADR-0003 / §83.4 Layer 1B's sole authorization token.
//! Depends-on: crates=[uuid]; services=[]; env=[]; modules=[infra-cell::resource]
//! Called-by: [adapters::projection_worker, adapters::provisioning, adapters::public_projection, adapters::qdrant, adapters::rebuild, adapters::retrieve, admin::cell_resources, gateway::bootstrap, gateway::recall, gateway::retrieval_embedding_client, gateway::status, infra-cell::resource, infra-cell::transport, public-worker::main, retrieval-worker::main, tests, xtask::switch_visible]
//! Invariants: [CellAccessPermit has no payload/data-class field and no conversion to EgressPermit, so minting one
//!   cannot reserve a disclosure row; an unknown caller or wrong Cell is a PermitError]
//! Spec: Baseline §7.4; §53.5; §83.4
//!
//! [`CellAccessPermit`] is deliberately **not** an [`EgressPermit`](https://docs.rs) (this
//! crate does not depend on `humaux-domain::egress` at all): it carries no
//! `payload_sha256`/`data_class` field, and there is no conversion from it to an
//! `EgressPermit` anywhere in the workspace. Minting one does not, and structurally cannot,
//! reserve an `ops.data_disclosures` row (§7.4) — that table is Layer 1A's ledger, and Qdrant
//! traffic must never post to it (see `docs/adr/0003-…` for the full argument: PG is Authority,
//! Qdrant is a rebuildable Projection, and `data_disclosures_finalized_total`/
//! `data_disclosures_reserved_unfinalized` are §53.5 INV-2/INV-3's real denominators — mixing
//! in same-Cell Projection/Search traffic would change what those two numbers measure).
//!
//! Every field on [`CellAccessPermit`] is private; the only way to obtain one is
//! [`authorize_cell_access`] — same "topology, not discipline" shape
//! `domain::egress::EgressPermit`/[`authorize`](humaux_domain::egress::authorize) already
//! establishes for Layer 1A (T4.1).
//!
//! §83.4 判据2 (same cell) / 判据6 (caller allowlist) are checked against
//! [`IntraCellResourceRegistry::local_cell_id`](crate::resource::IntraCellResourceRegistry)/
//! `local_caller_id` — this Cell process's own deploy-time identity, baked into the registry
//! once at bootstrap — **not** against parameters `authorize_cell_access` itself accepts. An
//! earlier shape took `source_cell_id: CellId` and `caller: CallerId` as call-site arguments:
//! any code holding a `&IntraCellResourceRegistry` (which many read-only call sites legitimately
//! do, to `resolve()` an entry) could then mint a permit for whatever Cell/caller identity it
//! chose to spell, including `registry.resolve(r).unwrap().cell_id()` echoed straight back —
//! both criteria were self-attested, not enforced. Removing both parameters means the only way
//! to change what identity a permit is minted for is to construct a *new* registry, which is
//! `IntraCellResourceRegistry::new`'s job, not any of this module's callers'.

use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::resource::{CellAccessMode, IntraCellResource, IntraCellResourceRegistry};

/// Deployment-Cell identity (§17.3 `projection.tenant_placements.cell_id` /
/// `ops.mechanism_observations.cell_id`'s Rust analogue, both `uuid` columns). Kept local to
/// this crate rather than added to `domain::ids`'s seven frozen newtypes — same precedent
/// `domain::egress::ProcessorId` already set for T4.1 (that module's doc: "ids.rs 模块文档明确
/// 只列七个").
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CellId(pub Uuid);

/// §83.4 判据6 caller-allowlist key: the calling **process/binary identity** a deploy-time
/// registry entry names (e.g. `"retrieval-worker"`), not a request-time self-reported claim —
/// [`authorize_cell_access`]'s caller checks against
/// [`ResourceEntry::caller_allowed`](crate::resource::ResourceEntry::caller_allowed), which
/// only the deploy-time [`IntraCellResourceRegistry`] populates.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CallerId(pub String);

/// [`authorize_cell_access`] failure — refusal, not a network condition (nothing has been
/// dialed yet at this point; that happens only once a permit exists, in
/// `crate::transport::IntraCellHttpTransport`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermitError {
    /// §83.4 判据1: the resource is not in [`IntraCellResourceRegistry`] at all.
    UnregisteredResource,
    /// §83.4 判据6: `caller` is not on this resource's deploy-time allowlist.
    UnknownCaller,
    /// §83.4 判据2: the registry's own Cell does not match the resource's Cell.
    WrongCell { source: CellId, target: CellId },
}

/// §83.4 Layer 1B's sole out-bound authorization token for same-Cell resource access — see
/// module doc for why this is not, and must never become, an `EgressPermit`.
#[derive(Debug)]
pub struct CellAccessPermit {
    grant_id: Uuid,
    resource: IntraCellResource,
    caller: CallerId,
    expires_at: Instant,
    access_mode: CellAccessMode,
}

impl CellAccessPermit {
    /// Not `pub`, not even `pub(crate)` beyond this module: only [`authorize_cell_access`],
    /// which lives here, may name it. `access_mode` is the registered [`ResourceEntry`]'s own
    /// mode at mint time (crate::resource::ResourceEntry::access_mode) — a permit carries it so
    /// `crate::transport::HttpIntraCellTransport::execute` can enforce §83.4's read-only ruling
    /// without re-resolving the registry entry per request.
    fn issue(
        resource: IntraCellResource,
        caller: CallerId,
        ttl: Duration,
        access_mode: CellAccessMode,
    ) -> Self {
        Self {
            grant_id: Uuid::now_v7(),
            resource,
            caller,
            expires_at: Instant::now() + ttl,
            access_mode,
        }
    }

    pub fn access_mode(&self) -> CellAccessMode {
        self.access_mode
    }

    pub fn grant_id(&self) -> Uuid {
        self.grant_id
    }

    pub fn resource(&self) -> IntraCellResource {
        self.resource
    }

    pub fn caller(&self) -> &CallerId {
        &self.caller
    }

    pub fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at
    }
}

/// §83.4's sole `pub` entry point for [`CellAccessPermit`]: enforces 判据2 (same-Cell) and
/// 判据6 (caller allowlist) at mint time, both checked against `registry`'s own baked-in
/// `local_cell_id`/`local_caller_id` (see module doc — neither is a parameter here, precisely
/// so neither can be caller-supplied). 判据1 (registry membership, no raw URL) is enforced by
/// [`IntraCellResource`]'s closed-enum shape before this function is ever reached; 判据3
/// (resolved-address-in-Cell) is enforced later, at call time, by
/// `crate::transport::IntraCellHttpTransport` — DNS resolution has not happened yet here.
pub fn authorize_cell_access(
    registry: &IntraCellResourceRegistry,
    resource: IntraCellResource,
    ttl: Duration,
) -> Result<CellAccessPermit, PermitError> {
    let entry = registry
        .resolve(resource)
        .ok_or(PermitError::UnregisteredResource)?;
    let target_cell_id = entry.cell_id();
    let source_cell_id = registry.local_cell_id();
    if target_cell_id != source_cell_id {
        return Err(PermitError::WrongCell {
            source: source_cell_id,
            target: target_cell_id,
        });
    }
    let caller = registry.local_caller_id();
    if !entry.caller_allowed(caller) {
        return Err(PermitError::UnknownCaller);
    }
    Ok(CellAccessPermit::issue(
        resource,
        caller.clone(),
        ttl,
        entry.access_mode(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::ResourceEntry;
    use std::collections::{BTreeMap, BTreeSet};

    /// `resource_cell_id` is the Cell the registered resource itself lives in;
    /// `local_cell_id`/`local_caller_id` are the registry's own baked-in process identity —
    /// kept as separate parameters so cross-Cell/unknown-caller tests below can make them
    /// deliberately mismatch.
    fn registry_with(
        resource_cell_id: CellId,
        callers: BTreeSet<CallerId>,
        local_cell_id: CellId,
        local_caller_id: CallerId,
    ) -> IntraCellResourceRegistry {
        let mut entries = BTreeMap::new();
        entries.insert(
            IntraCellResource::QDRANT_REST,
            ResourceEntry::new(
                "qdrant.internal",
                6333,
                resource_cell_id,
                vec!["10.0.0.0/8".parse().unwrap()],
                callers,
                true,
            )
            .unwrap(),
        );
        IntraCellResourceRegistry::new(entries, local_cell_id, local_caller_id)
    }

    #[test]
    fn authorize_mints_for_registered_resource_same_cell_allowed_caller() {
        let cell = CellId(Uuid::now_v7());
        let caller = CallerId("retrieval-worker".to_string());
        let registry = registry_with(cell, BTreeSet::from([caller.clone()]), cell, caller);
        let permit = authorize_cell_access(
            &registry,
            IntraCellResource::QDRANT_REST,
            Duration::from_secs(30),
        );
        assert!(permit.is_ok());
    }

    #[test]
    fn authorize_refuses_unregistered_resource() {
        let cell = CellId(Uuid::now_v7());
        let registry =
            IntraCellResourceRegistry::new(BTreeMap::new(), cell, CallerId("x".to_string()));
        let result = authorize_cell_access(
            &registry,
            IntraCellResource::QDRANT_REST,
            Duration::from_secs(30),
        );
        assert_eq!(result.err(), Some(PermitError::UnregisteredResource));
    }

    #[test]
    fn authorize_refuses_cross_cell_caller() {
        let resource_cell = CellId(Uuid::now_v7());
        let local_cell = CellId(Uuid::now_v7());
        let caller = CallerId("retrieval-worker".to_string());
        let registry = registry_with(
            resource_cell,
            BTreeSet::from([caller.clone()]),
            local_cell,
            caller,
        );
        let result = authorize_cell_access(
            &registry,
            IntraCellResource::QDRANT_REST,
            Duration::from_secs(30),
        );
        assert_eq!(
            result.err(),
            Some(PermitError::WrongCell {
                source: local_cell,
                target: resource_cell,
            })
        );
    }

    #[test]
    fn authorize_refuses_caller_not_on_allowlist() {
        let cell = CellId(Uuid::now_v7());
        let registry = registry_with(
            cell,
            BTreeSet::new(),
            cell,
            CallerId("uninvited-process".to_string()),
        );
        let result = authorize_cell_access(
            &registry,
            IntraCellResource::QDRANT_REST,
            Duration::from_secs(30),
        );
        assert_eq!(result.err(), Some(PermitError::UnknownCaller));
    }

    #[test]
    fn expiry_is_observable() {
        let cell = CellId(Uuid::now_v7());
        let caller = CallerId("retrieval-worker".to_string());
        let registry = registry_with(cell, BTreeSet::from([caller.clone()]), cell, caller);
        let permit = authorize_cell_access(
            &registry,
            IntraCellResource::QDRANT_REST,
            Duration::from_millis(0),
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(1));
        assert!(permit.is_expired(Instant::now()));
    }
}
