//! `domain::subject` — the §6.1.3 Subject / Aboutness axis domain types (ADR-0027, card 7).
//!
//! Visibility (§6.1.1, [`crate::identity`]) answers "who may see this memory"; the Subject axis
//! answers the orthogonal question "who is this memory *about*". The two never substitute for one
//! another — a `TENANT_SHARED` memory can be about a customer, so can a `USER_PRIVATE` one.
//!
//! This module lands the registry-backed closed sets only — the three that map directly to a
//! `text` CHECK column created by `migrations/0153_subject_registry_and_visibility_fn.sql` and are
//! therefore §78.2-contract-testable against the live DB:
//!
//! | Rust                        | column                       | wire set              |
//! |-----------------------------|------------------------------|-----------------------|
//! | [`SubjectKind`]             | `private.subjects.kind`      | `PERSON`/`ORGANISATION`|
//! | [`SubjectKeyKind`]          | `private.subject_keys.key_kind` | `CRM`              |
//! | [`SubjectRole`]             | `private.subject_roles.role` | `CUSTOMER`            |
//!
//! Card 8 (ADR-0028, migration 0154) adds the linkage closed sets — the two `text` CHECK columns
//! of `private.memory_subjects` — and the wire-side declaration a write may carry:
//!
//! | Rust                        | column                                 | wire set                          |
//! |-----------------------------|----------------------------------------|-----------------------------------|
//! | [`SubjectLinkRelation`]     | `private.memory_subjects.relation`     | `ABOUT`/`MENTIONS`                |
//! | [`SubjectLinkSource`]       | `private.memory_subjects.source_kind`  | `DECLARED`/`EXTERNAL_KEY`/`INHERITED` |
//!
//! [`SubjectDeclaration`] is the explicit `subject_ids` / `subject_keys` argument of
//! `remember.put` / `memory.confirm` / `memory.correct` (§6.1.3 resolution rules 1 and 2). The
//! byte-span mention itself has no Rust type: it is computed and read in SQL only
//! (`private.link_memory_subjects`), nothing in Rust constructs one.

use crate::error::ErrorCode;
use uuid::Uuid;

/// Subject id (§6.1.3). UUIDv7, minted like every other §49 id. Deliberately *not* in
/// [`crate::ids`]: that module is frozen at the seven `Scope` ids (§59); a subject is aboutness,
/// not scope, and lives on its own axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubjectId(pub Uuid);

#[allow(clippy::new_without_default)]
impl SubjectId {
    /// Mints a new subject id (UUIDv7, §49).
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Parses an existing UUID string. Malformed input is `INVALID_INPUT` (§52.1).
    pub fn parse(s: &str) -> Result<Self, ErrorCode> {
        Uuid::parse_str(s)
            .map(Self)
            .map_err(|_| ErrorCode::InvalidInput)
    }
}

/// The kind of subject a memory can be about (§6.1.3). Frozen closed set — no `Other`,
/// no `#[non_exhaustive]`. Wire form = `private.subjects.kind`'s `text` CHECK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubjectKind {
    /// A natural person.
    Person,
    /// An organisation / company.
    Organisation,
}

impl SubjectKind {
    /// All variants — derived list, the §78.2 contract-test and exhaustiveness surface.
    pub const ALL: [SubjectKind; 2] = [SubjectKind::Person, SubjectKind::Organisation];

    /// Wire string frozen on `private.subjects.kind`.
    pub fn as_str(self) -> &'static str {
        match self {
            SubjectKind::Person => "PERSON",
            SubjectKind::Organisation => "ORGANISATION",
        }
    }

    /// Parse a wire string; unknown/lowercase input is `None` (caller maps to its own error).
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.as_str() == s)
    }
}

/// The role a subject plays (§6.1.3). Orthogonal to `ScopeKind`: `Customer` is a role a subject
/// *is*, not a scope a memory is *filed under*. Frozen closed set. Wire form =
/// `private.subject_roles.role`'s `text` CHECK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubjectRole {
    /// The subject is a customer.
    Customer,
}

impl SubjectRole {
    /// All variants — the §78.2 contract-test and exhaustiveness surface.
    pub const ALL: [SubjectRole; 1] = [SubjectRole::Customer];

    /// Wire string frozen on `private.subject_roles.role`.
    pub fn as_str(self) -> &'static str {
        match self {
            SubjectRole::Customer => "CUSTOMER",
        }
    }

    /// Parse a wire string; unknown/lowercase input is `None`.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.as_str() == s)
    }
}

/// The kind of an external-identity dedup key (§6.1.3). Frozen closed set (later cards extend).
/// Wire form = `private.subject_keys.key_kind`'s `text` CHECK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubjectKeyKind {
    /// A CRM external id.
    Crm,
}

impl SubjectKeyKind {
    /// All variants — the §78.2 contract-test and exhaustiveness surface.
    pub const ALL: [SubjectKeyKind; 1] = [SubjectKeyKind::Crm];

    /// Wire string frozen on `private.subject_keys.key_kind`.
    pub fn as_str(self) -> &'static str {
        match self {
            SubjectKeyKind::Crm => "CRM",
        }
    }

    /// Parse a wire string; unknown/lowercase input is `None`.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.as_str() == s)
    }
}

/// An external-identity dedup key (§6.1.3): a `(kind, value)` pair, tenant-scoped at the DB layer
/// (`UNIQUE(tenant_id, key_kind, key_value)`, never globally unique). The `value` is opaque here
/// (a CRM id, etc.); its meaning is `kind`'s.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubjectKey {
    /// The kind of external identity this key is.
    pub kind: SubjectKeyKind,
    /// The external key value, verbatim.
    pub value: String,
}

impl SubjectKey {
    /// Constructs a key. `value` must be non-empty (a blank external id is malformed input, §52.1).
    pub fn new(kind: SubjectKeyKind, value: impl Into<String>) -> Result<Self, ErrorCode> {
        let value = value.into();
        if value.is_empty() {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self { kind, value })
    }
}

/// How a memory relates to a subject (§6.1.3). Frozen closed set. Wire form =
/// `private.memory_subjects.relation`'s `text` CHECK. Every deterministic rule of card 8 writes
/// `About`; `Mentions` is reserved for a later extractor that finds a subject the memory is not
/// primarily about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubjectLinkRelation {
    /// The memory is about the subject.
    About,
    /// The memory merely mentions the subject.
    Mentions,
}

impl SubjectLinkRelation {
    /// All variants — the §78.2 contract-test and exhaustiveness surface.
    pub const ALL: [SubjectLinkRelation; 2] =
        [SubjectLinkRelation::About, SubjectLinkRelation::Mentions];

    /// Wire string frozen on `private.memory_subjects.relation`.
    pub fn as_str(self) -> &'static str {
        match self {
            SubjectLinkRelation::About => "ABOUT",
            SubjectLinkRelation::Mentions => "MENTIONS",
        }
    }

    /// Parse a wire string; unknown/lowercase input is `None`.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.as_str() == s)
    }
}

/// Which §6.1.3 resolution rule produced a memory↔subject link. Frozen closed set — the resolve
/// order is deterministic and this is its audit trail (no `ModelExtracted`: nothing a model emits
/// is trusted for linking). Wire form = `private.memory_subjects.source_kind`'s `text` CHECK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubjectLinkSource {
    /// Rule 1: an explicit `SubjectId` on the write.
    Declared,
    /// Rule 2: an exact trusted external key resolved through `private.subject_keys`.
    ExternalKey,
    /// Rule 3: inherited from the PRIMARY Evidence's declaration, a corrected predecessor, or a
    /// rollup's source memories.
    Inherited,
}

impl SubjectLinkSource {
    /// All variants — the §78.2 contract-test and exhaustiveness surface.
    pub const ALL: [SubjectLinkSource; 3] = [
        SubjectLinkSource::Declared,
        SubjectLinkSource::ExternalKey,
        SubjectLinkSource::Inherited,
    ];

    /// Wire string frozen on `private.memory_subjects.source_kind`.
    pub fn as_str(self) -> &'static str {
        match self {
            SubjectLinkSource::Declared => "DECLARED",
            SubjectLinkSource::ExternalKey => "EXTERNAL_KEY",
            SubjectLinkSource::Inherited => "INHERITED",
        }
    }

    /// Parse a wire string; unknown/lowercase input is `None`.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.as_str() == s)
    }
}

/// The two non-destructive registry writes the `memory` tool exposes (§6.1.3, ADR-0028 D-F,
/// card 7 D-E1): `memory.subject_register` creates a subject (kind + display name + roles) under
/// the request's authenticated tenant; `memory.subject_link_key` attaches an exact external key
/// to a registered subject. Closed set, same shape as [`crate::confirm::DestructiveOp`] so the
/// gateway dispatch and guard match on it instead of a second literal (§78.2). Neither is
/// confirm-gated: nothing is deleted, superseded or made invisible by either.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubjectWriteOp {
    Register,
    LinkKey,
}

impl SubjectWriteOp {
    /// All variants — the exhaustiveness surface for dispatch tables.
    pub const ALL: [SubjectWriteOp; 2] = [SubjectWriteOp::Register, SubjectWriteOp::LinkKey];

    /// The catalog `operation_key` (contracts/mcp/memory.schema.json `x-humaux-operation`).
    pub const fn operation_key(self) -> &'static str {
        match self {
            SubjectWriteOp::Register => "memory.subject_register",
            SubjectWriteOp::LinkKey => "memory.subject_link_key",
        }
    }

    /// Inverse of [`Self::operation_key`]; any other key is `None`.
    pub fn parse_operation_key(key: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|op| op.operation_key() == key)
    }
}

/// An explicit subject declaration carried by a write (`remember.put` / `memory.confirm` /
/// `memory.correct`): registered ids (rule 1) and/or exact external keys (rule 2). Resolution is
/// the adapter's job under the caller's tenant RLS; an unknown id or key is `INVALID_INPUT`,
/// never an auto-registration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubjectDeclaration {
    /// Rule 1 — explicit registered subject ids.
    pub ids: Vec<SubjectId>,
    /// Rule 2 — exact trusted external keys.
    pub keys: Vec<SubjectKey>,
}

impl SubjectDeclaration {
    /// Upper bound on ids + keys one write may declare — a bound on request shape, not a
    /// business quota (a memory is about a handful of subjects; a longer list is malformed input).
    pub const MAX_ENTRIES: usize = 16;

    /// Builds a declaration, rejecting an over-long or duplicated one as `INVALID_INPUT` (§52.1).
    pub fn new(ids: Vec<SubjectId>, keys: Vec<SubjectKey>) -> Result<Self, ErrorCode> {
        if ids.len() + keys.len() > Self::MAX_ENTRIES {
            return Err(ErrorCode::InvalidInput);
        }
        for (i, id) in ids.iter().enumerate() {
            if ids[..i].contains(id) {
                return Err(ErrorCode::InvalidInput);
            }
        }
        for (i, key) in keys.iter().enumerate() {
            if keys[..i].contains(key) {
                return Err(ErrorCode::InvalidInput);
            }
        }
        Ok(Self { ids, keys })
    }

    /// `true` when the write declares nothing (rules 1/2 skipped; rule 3 still runs).
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty() && self.keys.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_relation_and_source_wire_round_trip() {
        for r in SubjectLinkRelation::ALL {
            assert_eq!(SubjectLinkRelation::parse(r.as_str()), Some(r));
        }
        for s in SubjectLinkSource::ALL {
            assert_eq!(SubjectLinkSource::parse(s.as_str()), Some(s));
        }
        assert_eq!(SubjectLinkSource::ExternalKey.as_str(), "EXTERNAL_KEY");
        assert_eq!(SubjectLinkSource::parse("declared"), None);
        assert_eq!(SubjectLinkRelation::parse("MENTIONED"), None);
    }

    #[test]
    fn subject_write_op_keys_round_trip_and_stay_in_the_memory_tool() {
        for op in SubjectWriteOp::ALL {
            assert_eq!(
                SubjectWriteOp::parse_operation_key(op.operation_key()),
                Some(op)
            );
            assert!(op.operation_key().starts_with("memory.subject_"));
        }
        assert_eq!(SubjectWriteOp::parse_operation_key("memory.get"), None);
    }

    #[test]
    fn declaration_rejects_duplicates_and_overflow() {
        let id = SubjectId::new();
        assert!(SubjectDeclaration::new(vec![id, id], vec![]).is_err());
        let key = SubjectKey::new(SubjectKeyKind::Crm, "CRM-1").unwrap();
        assert!(SubjectDeclaration::new(vec![], vec![key.clone(), key.clone()]).is_err());
        let too_many: Vec<SubjectId> = (0..=SubjectDeclaration::MAX_ENTRIES)
            .map(|_| SubjectId::new())
            .collect();
        assert!(SubjectDeclaration::new(too_many, vec![]).is_err());
        let ok = SubjectDeclaration::new(vec![id], vec![key]).unwrap();
        assert!(!ok.is_empty());
        assert!(SubjectDeclaration::default().is_empty());
    }

    #[test]
    fn kind_wire_round_trips_and_is_exhaustive() {
        assert_eq!(SubjectKind::ALL.len(), 2);
        for k in SubjectKind::ALL {
            assert_eq!(SubjectKind::parse(k.as_str()), Some(k));
        }
        assert_eq!(SubjectKind::Person.as_str(), "PERSON");
        assert_eq!(SubjectKind::Organisation.as_str(), "ORGANISATION");
        assert_eq!(SubjectKind::parse("person"), None); // case-sensitive, fails closed
        assert_eq!(SubjectKind::parse("COMPANY"), None);
    }

    #[test]
    fn role_wire_round_trips() {
        for r in SubjectRole::ALL {
            assert_eq!(SubjectRole::parse(r.as_str()), Some(r));
        }
        assert_eq!(SubjectRole::Customer.as_str(), "CUSTOMER");
        assert_eq!(SubjectRole::parse("customer"), None);
    }

    #[test]
    fn key_kind_wire_round_trips() {
        for k in SubjectKeyKind::ALL {
            assert_eq!(SubjectKeyKind::parse(k.as_str()), Some(k));
        }
        assert_eq!(SubjectKeyKind::Crm.as_str(), "CRM");
        assert_eq!(SubjectKeyKind::parse("crm"), None);
    }

    #[test]
    fn subject_key_rejects_empty_value() {
        assert!(SubjectKey::new(SubjectKeyKind::Crm, "").is_err());
        let k = SubjectKey::new(SubjectKeyKind::Crm, "ACME-42").unwrap();
        assert_eq!(k.value, "ACME-42");
        assert_eq!(k.kind, SubjectKeyKind::Crm);
    }

    #[test]
    fn subject_id_parse_rejects_garbage() {
        assert!(SubjectId::parse("not-a-uuid").is_err());
        let id = SubjectId::new();
        assert_eq!(SubjectId::parse(&id.0.to_string()).unwrap(), id);
    }
}
