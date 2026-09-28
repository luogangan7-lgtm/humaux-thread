//! `domain::dataclass` — `DataClass`, the monotonic safety lattice that cuts across Evidence / Observation / Memory /
//!   MemoryRollup / RetrievalCard / Query (§7.5.1), and its sole combinator `join_data_class` (§7.5.1 "唯一组合函数").
//! Depends-on: crates=[]; services=[]; env=[]; modules=[]
//! Called-by: [adapters::consolidation_reasoner, adapters::contribution_execution_repo, adapters::contribution_reasoner, adapters::distill_reasoner, adapters::projection_worker, adapters::qdrant, adapters::retrieval_query_source, domain::egress, gateway::bootstrap, gateway::mcp_application, gateway::remember, humaux-local-secret-scan, private-worker::distill, projection::card, retrieval-provider::adapters, tests]
//! Invariants: []
//! Spec: Baseline §7.5; §7.5.1; §78.2
//!
//! Closed set, five variants, **no** `Unknown` / `Other` / `#[non_exhaustive]` — §7.5.1's own
//! text: "判不出来的结果是 `SECRET_MATERIAL`，不是放行". A caller that cannot classify
//! something must pick [`DataClass::SecretMaterial`] itself; this type gives it nowhere else
//! to park the uncertainty.
//!
//! Wire form is the DB-side `text` CHECK constraint already frozen on
//! `private.evidence_objects.data_class` (`migrations/0004_private_evidence_memory.sql`):
//! `'PUBLIC' | 'INTERNAL' | 'PRIVATE' | 'SENSITIVE' | 'SECRET_MATERIAL'` — [`DataClass::as_str`]
//! / [`DataClass::parse`] are the sole §78.2 contract-test surface for that column, mirroring
//! `error::ErrorCode`'s `Variant => "WIRE_STRING"` shape (`domain::jobs`-style `as_db_str`
//! elsewhere in this workspace follows the same split between a `PascalCase` Rust identifier
//! and its `SCREAMING_SNAKE` wire string).
//!
//! ## No downgrade API (§7.5.1 "不允许派生时洗白")
//!
//! Every derived object's `DataClass` is computed by [`join_data_class`], which is a plain
//! `max` over its sources — structurally incapable of producing a value below any input. This
//! module exposes no function, method, or trait impl anywhere that maps a higher `DataClass`
//! to a lower one; the *only* legitimate way to reduce sensitivity is §7.5.1's own escape
//! hatch — building a brand-new object with its own provenance (`Public Release` in the
//! §7.5.1 derivation table) — which is a different object, not a mutation of this one. That
//! escape hatch is therefore not a function in this file to call; it is the *absence* of one.

use std::fmt;

/// §7.5.1 frozen lattice order: `PUBLIC < INTERNAL < PRIVATE < SENSITIVE < SECRET_MATERIAL`.
/// Discriminants double as that order so `#[derive(Ord)]` — and therefore `Iterator::max` in
/// [`join_data_class`] — *is* the join operator, not an approximation of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DataClass {
    /// Freely publishable; no tenant/user confidentiality claim (§7.5).
    Public = 0,
    /// Internal-only, not secret, not tied to one tenant's private content (§7.5).
    Internal = 1,
    /// Ordinary private tenant/user content (§7.5, the default "所有主流程 memory" grade).
    Private = 2,
    /// Elevated sensitivity within private content (§7.5).
    Sensitive = 3,
    /// Credential/secret-shaped material, or anything the classifier could not resolve
    /// (§7.5.1: "判不出来的结果是 `SECRET_MATERIAL`，不是放行"). Must never reach external
    /// embedding/rerank (§7.5).
    SecretMaterial = 4,
}

impl DataClass {
    /// All five variants, lattice order low to high (mirrors `error::ErrorCode::ALL`'s
    /// "derived list, not hand-recounted" shape — used by this module's own exhaustiveness
    /// tests and available to any future §78.2 contract test that walks the full set).
    pub const ALL: [DataClass; 5] = [
        DataClass::Public,
        DataClass::Internal,
        DataClass::Private,
        DataClass::Sensitive,
        DataClass::SecretMaterial,
    ];

    /// The frozen `SCREAMING_SNAKE` wire form — verbatim the five literals in
    /// `private.evidence_objects.data_class`'s CHECK constraint (§7.5, §78.2).
    pub const fn as_str(self) -> &'static str {
        match self {
            DataClass::Public => "PUBLIC",
            DataClass::Internal => "INTERNAL",
            DataClass::Private => "PRIVATE",
            DataClass::Sensitive => "SENSITIVE",
            DataClass::SecretMaterial => "SECRET_MATERIAL",
        }
    }

    /// Parses the DB wire form, recognized literals only — `pub(crate)` because its `None` is
    /// exactly the uncertainty §7.5.1 forbids parking anywhere but `SecretMaterial`; the one
    /// recommended entry point for any caller outside this module is [`Self::parse_or_secret`].
    pub(crate) fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.as_str() == s)
    }

    /// §7.5.1 "判不出来的结果是 `SECRET_MATERIAL`，不是放行" — the fail-closed parse every
    /// caller outside this module must use: an unrecognized wire string resolves to
    /// [`DataClass::SecretMaterial`], not a lower default and not a panic. [`Self::parse`]
    /// still exists (as `pub(crate)`) for this function's own use and this module's tests, but
    /// is not itself a spec-conformant entry point — `s.parse().unwrap_or(DataClass::Public)`
    /// is exactly the one-call whitewash this split exists to make unreachable from outside.
    pub fn parse_or_secret(s: &str) -> Self {
        Self::parse(s).unwrap_or(DataClass::SecretMaterial)
    }
}

impl fmt::Display for DataClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// §7.5.1's sole combinator: "语义 = 取最严格值". An Observation joins its source Evidence, a
/// Memory joins its bound Evidence, a MemoryRollup joins its source Memory — every one of
/// those derivations in the §7.5.1 table is this same function, never a bespoke recomputation.
///
/// An empty `xs` cannot arise from a real derivation (every derived object in the §7.5.1 table
/// has a non-empty basis by construction — same invariant `authority::NonEmptyVec` encodes
/// elsewhere in this crate for Authority's basis), but this function still must not silently
/// pick the *lowest* class for "nothing to join" — that would be exactly the "判不出来 = 放行"
/// shape §7.5.1 forbids. Falling back to [`DataClass::SecretMaterial`] keeps the empty case
/// fail-closed like every other undeterminable case, rather than introducing a `Result`/panic
/// this function's callers do not need for a case that cannot occur.
pub fn join_data_class<I: IntoIterator<Item = DataClass>>(xs: I) -> DataClass {
    xs.into_iter().max().unwrap_or(DataClass::SecretMaterial)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// §7.5.1 order, pinned explicitly (the `#[derive(Ord)]` above already encodes it via
    /// discriminant value; this additionally locks the *comparison outcomes* so a future
    /// reordering of the enum body must fail this test, not just silently reshuffle the
    /// lattice).
    #[test]
    fn lattice_order_matches_spec() {
        use DataClass::*;
        let ascending = [Public, Internal, Private, Sensitive, SecretMaterial];
        for w in ascending.windows(2) {
            assert!(w[0] < w[1], "{:?} must be strictly below {:?}", w[0], w[1]);
        }
    }

    #[test]
    fn as_str_round_trips_through_parse() {
        for class in DataClass::ALL {
            assert_eq!(DataClass::parse(class.as_str()), Some(class));
        }
    }

    /// Pins the literal spelling only — this is a Rust-vs-Rust comparison (both sides are
    /// compiled into this crate) and therefore cannot detect the DB CHECK constraint drifting
    /// out from under it. §78.2's actual DB-vs-Rust contract test lives in
    /// `crates/adapters/tests/disclosure_ledger.rs`
    /// (`data_class_matches_evidence_objects_check_constraint`), which has the live Postgres
    /// connection this assertion does not.
    #[test]
    fn as_str_literals_are_stable() {
        let wire: Vec<&str> = DataClass::ALL.iter().map(|c| c.as_str()).collect();
        assert_eq!(
            wire,
            vec![
                "PUBLIC",
                "INTERNAL",
                "PRIVATE",
                "SENSITIVE",
                "SECRET_MATERIAL"
            ]
        );
    }

    /// §7.5.1 fail-closed entry point: unrecognized/malformed wire forms resolve to
    /// `SecretMaterial`, never a lower default.
    #[test]
    fn parse_or_secret_fails_closed_on_unrecognized_input() {
        assert_eq!(
            DataClass::parse_or_secret("nonsense"),
            DataClass::SecretMaterial
        );
        assert_eq!(DataClass::parse_or_secret(""), DataClass::SecretMaterial);
        for class in DataClass::ALL {
            assert_eq!(DataClass::parse_or_secret(class.as_str()), class);
        }
    }

    #[test]
    fn parse_rejects_unknown_and_lowercase() {
        assert_eq!(DataClass::parse("Public"), None);
        assert_eq!(DataClass::parse("unknown"), None);
        assert_eq!(DataClass::parse(""), None);
    }

    /// §7.5.1 "唯一组合函数...语义 = 取最严格值": for any combination, the join result is
    /// `>=` every individual input — the defining monotonicity property, checked over every
    /// pair and every combination-of-all-five, not just one hand-picked example.
    #[test]
    fn join_is_monotone_over_every_pair() {
        for a in DataClass::ALL {
            for b in DataClass::ALL {
                let joined = join_data_class([a, b]);
                assert!(
                    joined >= a && joined >= b,
                    "join({a:?}, {b:?}) = {joined:?}"
                );
                assert_eq!(joined, a.max(b));
            }
        }
    }

    #[test]
    fn join_of_all_five_is_secret_material() {
        assert_eq!(join_data_class(DataClass::ALL), DataClass::SecretMaterial);
    }

    #[test]
    fn join_single_element_is_identity() {
        for class in DataClass::ALL {
            assert_eq!(join_data_class([class]), class);
        }
    }

    /// §7.5.1 "判不出来...不是放行": an empty basis must not resolve to the lowest class.
    #[test]
    fn join_of_empty_is_fail_closed_not_public() {
        assert_eq!(
            join_data_class(std::iter::empty()),
            DataClass::SecretMaterial
        );
    }

    /// The type-level half of "SECRET_MATERIAL 不可被派生降级": once `SecretMaterial` is one
    /// of the sources, joining it with *any* other class — including the lowest,
    /// [`DataClass::Public`] — can never produce anything but `SecretMaterial`. Combined with
    /// this module's doc comment (no downgrade API exists anywhere in this file), this pins
    /// the only route by which a derived object's class is computed: there is no reachable
    /// call sequence through `join_data_class` that launders a `SecretMaterial` source down to
    /// a lower grade.
    #[test]
    fn secret_material_is_unreachable_via_join() {
        for other in DataClass::ALL {
            assert_eq!(
                join_data_class([DataClass::SecretMaterial, other]),
                DataClass::SecretMaterial,
                "joining SecretMaterial with {other:?} must not downgrade it"
            );
            // Order independence: `join` must not be sensitive to argument position — a
            // caller composing sources in a different `Vec` order (as `join_data_class`'s own
            // callers do — Evidence/Memory basis ordering is not spec-frozen) must still fail
            // closed.
            assert_eq!(
                join_data_class([other, DataClass::SecretMaterial]),
                DataClass::SecretMaterial
            );
        }
    }
}
