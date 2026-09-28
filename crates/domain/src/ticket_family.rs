//! `domain::ticket_family` — the §15.1 stream-ticket family triple, in one place.
//! Depends-on: crates=[]; services=[]; env=[]; modules=[]
//! Called-by: [adapters::consolidate_repo, adapters::qdrant, tests, xtask::e2e_seed]
//! Invariants: []
//! Spec: Baseline §3; §15.1; §16.2
//!
//! A `projection.stream_log` row is addressed by six columns (§15.1); three of them —
//! `domain` / `projection_kind` / `projection_version` — identify the *family* rather than the
//! scope, and every process on the ticket path has to agree on them: the gateway's
//! `remember.put` issuer, `consolidate_repo::publish_rollup`'s rollup re-signal, and the
//! retrieval worker that resolves the tickets. Before this module they agreed by hand — three
//! literals in `consolidate_repo`, three `HUMAUX_RETRIEVAL_WORKER_{DOMAIN,PROJECTION_KIND,
//! PROJECTION_VERSION}` env values, and a `DOMAIN=…; PKIND=…; PVER=…` line in the rehearsal
//! script. Three copies of a value that must be equal is not configuration, it is a latent
//! outage: mis-set one and the worker polls a stream nobody writes, forever, with no error
//! (§78.1's "禁止字面量" is exactly this failure mode).
//!
//! §78.2: a closed enum, no `Other`. Adding a family is a compile-time act here, and
//! [`TicketFamily::collection_name`] is derived from the same two parts the ticket triple is
//! built from, so a family can never end up indexing into one Qdrant collection while
//! ticketing under another — `adapters::qdrant`'s `retrieval_family_matches_ticket_family`
//! pins that equality against §17's own collection names.
//!
//! §3/§78.3: Domain, so no env and no I/O — this is the *identity*, not the deployment's
//! choice of which families to run.

/// The §15.1 ticket families this deployment issues and resolves. One variant today
/// (§15.1's private-memory ingest path); `ALL` exists so a future second family cannot be
/// added without every enumerating consumer seeing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TicketFamily {
    /// `private_memory` / `PRIVATE_MEMORY` / `v1` — the Evidence→Memory ingest stream the
    /// gateway issues, the consolidation worker re-signals, and the retrieval worker projects.
    PrivateMemory,
}

impl TicketFamily {
    /// Every family, for callers that must enumerate rather than pick (§78.2).
    pub const ALL: [TicketFamily; 1] = [Self::PrivateMemory];

    /// `projection.stream_log.domain`.
    pub fn domain(self) -> &'static str {
        match self {
            Self::PrivateMemory => "private_memory",
        }
    }

    /// `projection.stream_log.projection_kind`.
    pub fn projection_kind(self) -> &'static str {
        match self {
            Self::PrivateMemory => "PRIVATE_MEMORY",
        }
    }

    /// `projection.stream_log.projection_version` — the version this build issues and
    /// resolves. §16.2's blue/green switch reads the *serving* version out of
    /// `projection.stream_checkpoints`; this is the family's own build-time identity, which is
    /// what a ticket is stamped with at issue time.
    pub fn projection_version(self) -> &'static str {
        match self {
            Self::PrivateMemory => "v1",
        }
    }

    /// The §17 Qdrant collection this family's cards land in, **derived** from the same two
    /// parts the ticket triple carries rather than spelled a second time. `adapters::qdrant::
    /// RetrievalFamily::collection_name` is §17's own control point for that name; the test
    /// `retrieval_family_matches_ticket_family` there asserts the two agree, so the derivation
    /// below is a checked identity, not a coincidence.
    pub fn collection_name(self) -> String {
        format!("{}_{}", self.domain(), self.projection_version())
    }

    /// Reverse lookup for a triple read back out of the database or off a command line — the
    /// fail-closed direction (§78.2: an unknown triple is `None`, never a synthesized family).
    pub fn from_triple(
        domain: &str,
        projection_kind: &str,
        projection_version: &str,
    ) -> Option<Self> {
        Self::ALL.into_iter().find(|f| {
            f.domain() == domain
                && f.projection_kind() == projection_kind
                && f.projection_version() == projection_version
        })
    }
}

#[cfg(test)]
mod tests {
    use super::TicketFamily;

    /// §17 × §15.1: the collection name is derived from the ticket triple, so the two can
    /// never drift. Fault injection: change `projection_version` to `v2` without touching
    /// §17's collection name and `adapters::qdrant::retrieval_family_matches_ticket_family`
    /// goes red.
    #[test]
    fn collection_name_is_derived_from_the_ticket_triple() {
        let f = TicketFamily::PrivateMemory;
        assert_eq!(
            f.collection_name(),
            format!("{}_{}", f.domain(), f.projection_version())
        );
        assert_eq!(f.collection_name(), "private_memory_v1");
    }

    #[test]
    fn from_triple_round_trips_and_is_fail_closed() {
        for f in TicketFamily::ALL {
            assert_eq!(
                TicketFamily::from_triple(f.domain(), f.projection_kind(), f.projection_version()),
                Some(f)
            );
        }
        assert_eq!(
            TicketFamily::from_triple("private_memory", "PRIVATE_MEMORY", "v2"),
            None
        );
        assert_eq!(TicketFamily::from_triple("knowledge", "ingest", "v1"), None);
    }

    /// §78.2: every variant answers all three columns with a non-empty value — a new variant
    /// that forgets one arm cannot compile, but one that returns `""` would only show up here.
    #[test]
    fn every_family_answers_all_three_columns() {
        assert_eq!(TicketFamily::ALL.len(), 1);
        for f in TicketFamily::ALL {
            assert!(!f.domain().is_empty());
            assert!(!f.projection_kind().is_empty());
            assert!(!f.projection_version().is_empty());
        }
    }
}
