//! `domain::boundary` — ADR-0003 second-round correction: the disclosure-ledger judgment is a
//! legal/organizational-entity question, not a network-topology one.
//!
//! §83.4/ADR-0003's first round (Layer 0 protocol choke point / Layer 1A external egress /
//! Layer 1B intra-Cell access) split the *code topology* correctly, but its own prose risked
//! being read as "private IP ⇒ not a disclosure". EDPB Guidelines 07/2020 says otherwise: the
//! first condition for becoming a GDPR Art.28 processor is "being a separate entity in relation
//! to the controller" — same-entity departments processing data with their own resources
//! "within its organisation … this is not a processor situation" (GDPR Art.4(8)/Art.28). Network
//! privacy is not part of that test at all. The reverse trap this module exists to make
//! impossible: a request to AWS Bedrock over AWS PrivateLink never touches the public Internet
//! — fully private, by [`NetworkRouteClass`]'s own definition — but AWS is a separate legal
//! entity acting as a processor, so [`RecipientClass::ExternalProcessor`] still applies and
//! [`requires_disclosure_record`] still returns `true`. Conversely, a self-hosted Qdrant
//! instance inside this same Humaux Data Cell (§83.4 Layer 1B's `IntraCellResource`) is
//! [`RecipientClass::SameEntityResource`] regardless of the fact that its traffic happens to be
//! plain HTTP: `requires_disclosure_record` returns `false` for it not because the route is
//! intra-Cell, but because Humaux is the only entity that ever touches the bytes.
//!
//! [`NetworkRouteClass`] answers "where did the bytes travel" (topology). [`RecipientClass`]
//! answers "who is processing them" (legal/organizational boundary). They are deliberately
//! **not interconvertible** — no `From`/`Into` between them anywhere in this crate, and none
//! should ever be added elsewhere in the workspace — because conflating them is exactly the
//! mistake this module exists to make impossible. [`requires_disclosure_record`] is the
//! *only* function in this workspace that may decide whether a transfer must produce an
//! `ops.data_disclosures` row (§7.4); it takes a [`RecipientClass`] and nothing else. No
//! caller anywhere may use "is this IntraCell", "is this a private IP", or "is this plain HTTP"
//! as a substitute for calling it.

/// Where the bytes physically travelled — network topology, and *only* network topology. On
/// its own this is never a legitimate input to a disclosure decision (see module doc) — named
/// here anyway because §83.4's Layer 0/1A/1B split needs a name for this axis too, and giving
/// it one that cannot silently masquerade as [`RecipientClass`] is the whole point of keeping
/// the two enums separate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkRouteClass {
    /// No network hop at all — an in-process call.
    SameProcess,
    /// §83.4 Layer 1B: resolved inside this Cell's own CIDR/service-IP set
    /// (`humaux_infra_cell::resource::IntraCellResource`'s traffic).
    IntraCell,
    /// Crosses a Cell boundary but stays inside Humaux-operated infrastructure (e.g. a sibling
    /// Cell in the same deployment). Not minted by any caller in this workspace yet — named so
    /// a future cross-Cell route has a place to declare itself instead of being forced into
    /// [`Self::ExternalNetwork`].
    InterCell,
    /// The public Internet, a VPN, PrivateLink, or any other route leaving Humaux-operated
    /// infrastructure. **Not, by itself, a disclosure trigger** — see module doc's
    /// PrivateLink/Bedrock reverse trap.
    ExternalNetwork,
}

/// Who is actually processing the bytes — legal/organizational identity, per EDPB Guidelines
/// 07/2020's processor test (module doc). This is the *only* axis [`requires_disclosure_record`]
/// consults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecipientClass {
    /// The same legal entity operating this workspace, processing with its own resources
    /// within its own organisation (EDPB 07/2020: "this is not a processor situation") — e.g.
    /// a self-hosted Qdrant instance inside this Humaux Data Cell. §83.4 Layer 1B's
    /// `IntraCellResource` is always this class.
    SameEntityResource,
    /// A resource whose processing terms the *customer* (not Humaux) controls — reserved for a
    /// future BYO-infrastructure shape; not minted by any caller in this workspace yet.
    CustomerControlledResource,
    /// A separate legal entity acting as a GDPR Art.28 processor on Humaux's instructions —
    /// §7.3/§7.4's existing `EgressPermit`/`ops.data_disclosures` machinery already models
    /// exactly this recipient (every `domain::egress::PrivateDataPurpose` variant).
    ExternalProcessor,
    /// A separate legal entity receiving data as an independent controller/recipient, not
    /// under Humaux's processing instructions (e.g. a regulator, a user's own downstream
    /// export target). Kept distinct from [`Self::ExternalProcessor`] because §7.4's deletion-
    /// propagation semantics (does Humaux *instruct* the recipient to delete on request) differ
    /// between the two, even though both require a disclosure record.
    ExternalIndependentRecipient,
}

/// §7.4's *only* legitimate judgment point: whether a transfer to `recipient` must produce an
/// `ops.data_disclosures` row. **Never** derive `recipient` from a [`NetworkRouteClass`] — see
/// module doc's PrivateLink/Bedrock reverse trap. `route == ExternalNetwork` does not imply
/// `true`, and `route == IntraCell` does not imply `false`: the two axes are independent by
/// construction (there is no conversion function between them for a caller to reach for by
/// mistake), not merely by convention.
pub fn requires_disclosure_record(recipient: RecipientClass) -> bool {
    matches!(
        recipient,
        RecipientClass::ExternalProcessor | RecipientClass::ExternalIndependentRecipient
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_entity_resource_does_not_require_disclosure() {
        assert!(!requires_disclosure_record(
            RecipientClass::SameEntityResource
        ));
    }

    #[test]
    fn customer_controlled_resource_does_not_require_disclosure() {
        assert!(!requires_disclosure_record(
            RecipientClass::CustomerControlledResource
        ));
    }

    #[test]
    fn external_processor_requires_disclosure() {
        assert!(requires_disclosure_record(
            RecipientClass::ExternalProcessor
        ));
    }

    #[test]
    fn external_independent_recipient_requires_disclosure() {
        assert!(requires_disclosure_record(
            RecipientClass::ExternalIndependentRecipient
        ));
    }

    /// Reverse trap, ADR-0003 second-round correction: an `ExternalProcessor` reached over a
    /// route that *looks* intra-Cell must still require a disclosure record —
    /// `requires_disclosure_record` takes no `NetworkRouteClass` parameter at all, so this is
    /// byte-identical to [`external_processor_requires_disclosure`] at the type level: there is
    /// no way to *pass* a route into the function under test, which is itself the trap this
    /// test documents (a `NetworkRouteClass::IntraCell` value cannot silently change the
    /// answer below because it cannot reach this call at all). The real runtime trap-closing
    /// assertion is `adapters::disclosure::reserve_in_txn`'s own guard plus its
    /// `test-support`-feature fault-injection test — see that module's doc.
    #[test]
    fn external_processor_over_an_intra_cell_shaped_route_still_requires_disclosure() {
        assert!(requires_disclosure_record(
            RecipientClass::ExternalProcessor
        ));
    }

    /// Mirror of the trap above, same caveat: byte-identical to
    /// [`same_entity_resource_does_not_require_disclosure`] because `requires_disclosure_record`
    /// has no route parameter to vary.
    #[test]
    fn same_entity_resource_over_an_external_network_shaped_route_still_skips_disclosure() {
        assert!(!requires_disclosure_record(
            RecipientClass::SameEntityResource
        ));
    }
}
