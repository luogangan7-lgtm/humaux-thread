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
//! Mention-reference types (`SubjectRef` / `ByteSpan` / `SubjectRefSource`) named in the card's
//! Scope narrative are deliberately NOT landed here: their backing table (`memory_subject_mentions`)
//! and their only consumer (card 8's resolve hook) do not exist yet, so shipping them now would be
//! scaffolding for later with speculative variants no DB CHECK can pin (§78.2 has nothing to test
//! them against). They land with the mentions table in the card that creates it. See ADR-0027.

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

#[cfg(test)]
mod tests {
    use super::*;

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
