//! `domain::authority` — `Authority` value object, its four-tier adjudication order, and the
//! `AuthorityClass` / `AuthorityStatus` / `Confidence` types it is built from (§59 / §59.1).
//!
//! §59.1 freezes: this section promotes §10's "recommended" `authority_class`/`confidence`/
//! `status` to required. §10's priority chain itself is unchanged, it merely gains typed
//! expression — `AuthorityClass`'s discriminants 0→6 map item-for-item, low to high, onto
//! §10's table.
//!
//! `asserted_at` uses `std::time::SystemTime` rather than the §59.1 frozen field table's
//! `DateTime<Utc>` — no crate in this workspace depends on `chrono`/`time` yet. `SystemTime`
//! is stdlib and totally ordered (`Ord`), satisfying I1's "asserted_at 晚者优先" tier, but is
//! unsuitable as `TIMESTAMPTZ`'s wire type (no defined line format, can underflow
//! `UNIX_EPOCH` on some platforms). ADR for this deviation is pending (`docs/adr/` is outside
//! this task's assigned file scope); whichever task wires §59.1 G59-5's Postgres column must
//! either write it or pick the wire conversion.
//!
//! `MemoryId` / `EvidenceId` are minted here rather than in `domain::ids`: they are not among
//! that module's fixed seven §59 core types, and this module is their first consumer
//! (`Authority::superseded_by`, `Authority::evidence`, and G59-1's adjudication tuple). Their
//! mint/parse shape hand-matches `ids::uuid_newtype!` rather than reusing it — that macro is
//! private to `ids.rs` and exporting it is a change to a file outside this task's assigned
//! scope (`crates/domain/src/authority.rs`, `crates/domain/Cargo.toml`); flagged for the
//! orchestrating task to fold into `ids::uuid_newtype!` or relocate these two ids.

use crate::error::ErrorCode;
use std::cmp::Ordering;
use std::time::SystemTime;
use uuid::Uuid;

/// Typed expression of §10's priority chain, a closed set of 7 variants (§59).
///
/// The discriminant *is* the priority — larger outranks smaller, mapping item-for-item onto
/// §10's priority chain (§59.1 I1, first tier of the adjudication total order). No
/// `#[non_exhaustive]`, no `Other`/`Unknown`/`Custom(String)` (§59 frozen).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AuthorityClass {
    /// General public knowledge, no personal or project context (§10, lowest priority).
    PublicKnowledge = 0,
    /// Private knowledge specific to this tenant/user (§10).
    PrivateKnowledge = 1,
    /// A stated user preference (§10).
    UserPreference = 2,
    /// A recorded project decision (§10).
    ProjectDecision = 3,
    /// The user explicitly correcting prior output (§10).
    UserCorrection = 4,
    /// A project-level constraint that must hold (§10).
    ProjectConstraint = 5,
    /// Context explicitly supplied for the current task (§10, highest priority).
    ExplicitTaskContext = 6,
}

/// Authority lifecycle state, a closed set of 4 variants (§59).
///
/// A non-`Active` row does not participate in adjudication (§59.1 I3), but still counts
/// toward §23's `visible` and `done` as usual — completeness measures "what you cannot see",
/// not "which row won adjudication"; the two must never be conflated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityStatus {
    /// Currently in force and eligible for adjudication (§59).
    Active,
    /// Replaced by a newer Authority row; kept for history (§59).
    Superseded,
    /// Explicitly withdrawn (§59).
    Revoked,
    /// Past its validity window (§59).
    Expired,
}

/// Confidence in the closed `[0.0, 1.0]` interval (§59).
///
/// Out-of-range values (including `NaN` / `Inf`) return `Err(ErrorCode::InvalidInput)` at
/// `new` — clamping back into `[0,1]` is forbidden: garbage fails loud (§50), it is never
/// silently corrected. Used only for tie-breaks **within the same class**, never in effect
/// across `AuthorityClass` boundaries (§59.1).
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Confidence(f32);

impl Confidence {
    /// Construction guard: `v` must be a finite float within the closed `[0.0, 1.0]`
    /// interval, otherwise returns `Err(ErrorCode::InvalidInput)` (§52 `INVALID_INPUT`).
    /// Clamping is forbidden.
    pub fn new(v: f32) -> Result<Self, ErrorCode> {
        if v.is_finite() && (0.0..=1.0).contains(&v) {
            Ok(Self(v))
        } else {
            Err(ErrorCode::InvalidInput)
        }
    }

    /// Reads out the inner `f32` value.
    pub fn get(self) -> f32 {
        self.0
    }
}

/// Memory row id, UUIDv7 (§49). See module doc for why it is minted here instead of
/// `domain::ids`; `parse` and the mint/parse split otherwise match `ids::uuid_newtype!`
/// exactly (§49: "exactly two entry points, mint and parse").
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemoryId(pub Uuid);

impl MemoryId {
    // See `domain::ids`'s identical rationale: `Default` would silently mint a fresh random
    // id, a bigger trap than one extra `::new()` call.
    #[allow(clippy::new_without_default)]
    /// Mints a new id (UUIDv7, §49).
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Parses an existing UUID string (§49's second entry point; no legacy uuid5/hash
    /// formula compatibility).
    pub fn parse(s: &str) -> Result<Self, ErrorCode> {
        Uuid::parse_str(s)
            .map(Self)
            .map_err(|_| ErrorCode::InvalidInput)
    }
}

/// Evidence row id, UUIDv7 (§49). See module doc for why it is minted here instead of
/// `domain::ids`; not ordered — I1 tier 4 orders `MemoryId`, never `EvidenceId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EvidenceId(pub Uuid);

impl EvidenceId {
    #[allow(clippy::new_without_default)]
    /// Mints a new id (UUIDv7, §49).
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Parses an existing UUID string (§49's second entry point; no legacy uuid5/hash
    /// formula compatibility).
    pub fn parse(s: &str) -> Result<Self, ErrorCode> {
        Uuid::parse_str(s)
            .map(Self)
            .map_err(|_| ErrorCode::InvalidInput)
    }
}

#[cfg(test)]
mod memory_id_and_evidence_id_mint_parse {
    use super::*;

    #[test]
    fn memory_id_new_ids_round_trip_through_parse() {
        let id = MemoryId::new();
        let parsed = MemoryId::parse(&id.0.to_string()).expect("valid uuid string parses");
        assert_eq!(id, parsed);
    }

    #[test]
    fn memory_id_new_ids_are_v7() {
        assert_eq!(MemoryId::new().0.get_version_num(), 7);
    }

    #[test]
    fn memory_id_parse_rejects_garbage() {
        assert_eq!(
            MemoryId::parse("not-a-uuid").unwrap_err(),
            ErrorCode::InvalidInput
        );
    }

    #[test]
    fn evidence_id_new_ids_round_trip_through_parse() {
        let id = EvidenceId::new();
        let parsed = EvidenceId::parse(&id.0.to_string()).expect("valid uuid string parses");
        assert_eq!(id, parsed);
    }

    #[test]
    fn evidence_id_new_ids_are_v7() {
        assert_eq!(EvidenceId::new().0.get_version_num(), 7);
    }

    #[test]
    fn evidence_id_parse_rejects_garbage() {
        assert_eq!(
            EvidenceId::parse("not-a-uuid").unwrap_err(),
            ErrorCode::InvalidInput
        );
    }
}

/// A `Vec<T>` guaranteed non-empty by construction (§59.1 I6: `Authority.evidence` must carry
/// at least one `EvidenceId`; reused by `AuthorityPolicy::authorize`'s `basis` below for the
/// identical §10.1 requirement — one guard, not two copies of it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonEmptyVec<T>(Vec<T>);

impl<T> NonEmptyVec<T> {
    /// Rejects an empty `items` with `ErrorCode::InvalidInput` (§59.1 I6); never silently
    /// substitutes a default element.
    pub fn new(items: Vec<T>) -> Result<Self, ErrorCode> {
        if items.is_empty() {
            Err(ErrorCode::InvalidInput)
        } else {
            Ok(Self(items))
        }
    }

    /// Reads out the wrapped elements.
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }
}

/// Authority value object (§59.1). Fields all private, no `Default`, no `pub` setter —
/// `Authority::new` is the sole construction entry point workspace-wide (§59.1 G59-3: exactly
/// one `Authority { .. }` struct literal may exist).
#[derive(Debug, Clone, PartialEq)]
pub struct Authority {
    class: AuthorityClass,
    confidence: Confidence,
    status: AuthorityStatus,
    asserted_at: SystemTime,
    evidence: NonEmptyVec<EvidenceId>,
    superseded_by: Option<MemoryId>,
}

impl Authority {
    /// Validates §59.1's I4/I5/I6 before constructing; returns `Err(ErrorCode::InvalidInput)`
    /// rather than silently repairing a bad combination (§50 fail-loud).
    ///
    /// - **I4**: `status == Superseded` iff `superseded_by.is_some()` — checked below, both
    ///   directions.
    /// - **I5**: delegated entirely to `Confidence::new`'s own invariant — by the time a
    ///   `Confidence` value reaches this function it is already finite and in `[0,1]`, so
    ///   there is nothing left here to re-check.
    /// - **I6**: the non-empty half is enforced by `NonEmptyVec::new` before this function is
    ///   ever reachable with an empty list. I6's tenant-locality clause ("class >
    ///   PrivateKnowledge 的行还必须至少一条 evidence 落在本 tenant 内") needs a
    ///   tenant-carrying Evidence lookup this constructor does not have — `EvidenceId` here is
    ///   an opaque id, not a tenant-scoped record. That half belongs at the
    ///   `AuthorityPolicy::authorize` boundary below, which does receive a `Scope`.
    pub fn new(
        class: AuthorityClass,
        confidence: Confidence,
        status: AuthorityStatus,
        asserted_at: SystemTime,
        evidence: NonEmptyVec<EvidenceId>,
        superseded_by: Option<MemoryId>,
    ) -> Result<Self, ErrorCode> {
        if (status == AuthorityStatus::Superseded) != superseded_by.is_some() {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Authority {
            class,
            confidence,
            status,
            asserted_at,
            evidence,
            superseded_by,
        })
    }

    /// This row's `AuthorityClass` (§59.1 I1 first tier).
    pub fn class(&self) -> AuthorityClass {
        self.class
    }

    /// `status == Active` (§59.1 I3: a non-Active row does not participate in adjudication,
    /// but still counts toward §23's `visible`/`done` — that bookkeeping is not this module).
    pub fn is_decisive(&self) -> bool {
        self.status == AuthorityStatus::Active
    }
}

/// §59.1 I1: the four-tier adjudication total order over `(MemoryId, Authority)` pairs —
/// class discriminant, then `asserted_at`, then `confidence`, then `memory_id`, each tier only
/// consulted when every earlier tier ties. `Ordering::Greater` means the left pair outranks
/// the right. Pure comparator: it does not look at `status` — §59.1 I3's exclusion of
/// non-Active rows from adjudication is `adjudicate`'s job below, not this function's (I1 and
/// I3 are separate invariants, and conflating "who wins" with "who is even eligible" is the
/// exact confusion §59.1 I3 warns against for §23's `skipped`).
///
/// I2 ("Public 只补充不覆盖") has no code of its own here: §59.1 states it is a pure
/// consequence of I1's first tier, not an independently checkable rule — see §59.1's own I2
/// text for why encoding it separately would be a vacuously-true assertion.
pub fn resolve(a: &(MemoryId, Authority), b: &(MemoryId, Authority)) -> Ordering {
    let (id_a, auth_a) = a;
    let (id_b, auth_b) = b;
    auth_a
        .class
        .cmp(&auth_b.class)
        .then_with(|| auth_a.asserted_at.cmp(&auth_b.asserted_at))
        .then_with(|| {
            auth_a
                .confidence
                .get()
                .partial_cmp(&auth_b.confidence.get())
                .expect("Confidence is constructor-guaranteed finite, §59.1 I5")
        })
        .then_with(|| id_a.0.cmp(&id_b.0))
}

/// §59.1 I1 + I3 together: the winning candidate among `Active` rows only. Non-Active rows
/// are excluded from adjudication here (I3's adjudication half); they still count toward
/// §23's `visible`/`done` ledger, which this module does not implement (§59.1 I3 — that
/// bookkeeping lives with `ledger::close`, a different task).
pub fn adjudicate(candidates: &[(MemoryId, Authority)]) -> Option<&(MemoryId, Authority)> {
    candidates
        .iter()
        .filter(|(_, a)| a.is_decisive())
        .max_by(|a, b| resolve(a, b))
}

/// §10.1 I7 skeleton: validates a requested `AuthorityClass` against the origin-bound ceiling
/// (§10.1's table) before an `Authority` is ever constructed at that class — the
/// implementation lands in Phase 1 T1.8, this only fixes the contract shape so it can be
/// depended on today. `basis` reuses `evidence::EvidenceOriginClass` (already §8.7-frozen) for
/// spec pseudocode's `EvidenceOrigin`, which no task has defined a richer type for yet.
pub trait AuthorityPolicy {
    /// Returns the authorized class, or a `CandidateRejection` — §10.1 rule 3: an
    /// over-ceiling request is rejected outright, never silently downgraded.
    fn authorize(
        &self,
        requested: AuthorityClass,
        memory_type: crate::memory::MemoryType,
        basis: NonEmptyVec<crate::evidence::EvidenceOriginClass>,
        scope: &crate::ids::Scope,
    ) -> Result<AuthorizedAuthority, CandidateRejection>;
}

/// §10.1 I7 success value: the `AuthorityClass` actually authorized. Full shape (beyond the
/// class itself) is for the T1.8 implementation to decide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorizedAuthority(pub AuthorityClass);

/// §10.1's rejection-reason closed set, "at least" these four named there explicitly; the
/// T1.8 implementation may need to extend this list, not this task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateRejection {
    /// Requested class exceeds what `basis`'s origin(s) are allowed to reach (§10.1 table).
    OriginAuthorityCeiling,
    /// `basis` carries instruction-like text that must stay `DATA_ONLY` (§10.1).
    UntrustedInstruction,
    /// `basis` includes Evidence outside `scope`'s tenant (§59.1 I6, §52 `TENANT_BOUNDARY`).
    CrossTenantEvidence,
    /// `requested` needs a `UserConfirmed` basis that isn't present (§10.1).
    MissingConfirmation,
}

impl CandidateRejection {
    /// The closed label persisted in `private.distill_candidates.rejection_reason` and emitted
    /// as the `memory_candidate_rejections_total{reason}` metric value (§78.2 DB<->Rust — one
    /// mapping, never a parallel string in the distill hop or the migration).
    #[must_use]
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::OriginAuthorityCeiling => "origin_authority_ceiling",
            Self::UntrustedInstruction => "untrusted_instruction",
            Self::CrossTenantEvidence => "cross_tenant_evidence",
            Self::MissingConfirmation => "missing_confirmation",
        }
    }

    /// Inverse of [`Self::as_db_str`]; `None` for any value outside the closed set.
    #[must_use]
    pub fn from_db_str(s: &str) -> Option<Self> {
        Some(match s {
            "origin_authority_ceiling" => Self::OriginAuthorityCeiling,
            "untrusted_instruction" => Self::UntrustedInstruction,
            "cross_tenant_evidence" => Self::CrossTenantEvidence,
            "missing_confirmation" => Self::MissingConfirmation,
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confidence_rejects_nan() {
        assert!(Confidence::new(f32::NAN).is_err());
    }

    #[test]
    fn confidence_rejects_positive_infinity() {
        assert!(Confidence::new(f32::INFINITY).is_err());
    }

    #[test]
    fn confidence_rejects_above_upper_bound() {
        assert!(Confidence::new(1.5).is_err());
    }

    #[test]
    fn confidence_rejects_below_lower_bound() {
        assert!(Confidence::new(-0.5).is_err());
    }

    #[test]
    fn confidence_accepts_lower_closed_bound() {
        assert_eq!(Confidence::new(0.0).expect("0.0 is in range").get(), 0.0);
    }

    #[test]
    fn confidence_accepts_upper_closed_bound() {
        assert_eq!(Confidence::new(1.0).expect("1.0 is in range").get(), 1.0);
    }

    #[test]
    fn authority_class_discriminants_are_0_through_6_in_priority_order() {
        // §59.1 I1's first tier depends on the discriminant being the priority; this locks
        // the exact values and order — discriminant drift would silently change the meaning
        // of §10's priority chain at the type level.
        assert_eq!(AuthorityClass::PublicKnowledge as i32, 0);
        assert_eq!(AuthorityClass::PrivateKnowledge as i32, 1);
        assert_eq!(AuthorityClass::UserPreference as i32, 2);
        assert_eq!(AuthorityClass::ProjectDecision as i32, 3);
        assert_eq!(AuthorityClass::UserCorrection as i32, 4);
        assert_eq!(AuthorityClass::ProjectConstraint as i32, 5);
        assert_eq!(AuthorityClass::ExplicitTaskContext as i32, 6);

        assert!(AuthorityClass::PublicKnowledge < AuthorityClass::PrivateKnowledge);
        assert!(AuthorityClass::PrivateKnowledge < AuthorityClass::UserPreference);
        assert!(AuthorityClass::UserPreference < AuthorityClass::ProjectDecision);
        assert!(AuthorityClass::ProjectDecision < AuthorityClass::UserCorrection);
        assert!(AuthorityClass::UserCorrection < AuthorityClass::ProjectConstraint);
        assert!(AuthorityClass::ProjectConstraint < AuthorityClass::ExplicitTaskContext);
    }
}

#[cfg(test)]
mod non_empty_vec_tests {
    use super::*;

    #[test]
    fn rejects_empty() {
        // §59.1 I6, non-empty half.
        let result: Result<NonEmptyVec<EvidenceId>, ErrorCode> = NonEmptyVec::new(vec![]);
        assert_eq!(result.unwrap_err(), ErrorCode::InvalidInput);
    }

    #[test]
    fn accepts_one_element() {
        let items = vec![EvidenceId::new()];
        assert_eq!(
            NonEmptyVec::new(items).expect("non-empty").as_slice().len(),
            1
        );
    }
}

#[cfg(test)]
mod i4_status_superseded_by_consistency {
    use super::*;

    fn one_evidence() -> NonEmptyVec<EvidenceId> {
        NonEmptyVec::new(vec![EvidenceId::new()]).expect("one element is non-empty")
    }

    #[test]
    fn superseded_without_superseded_by_is_err() {
        // §59.1 I4, direction 1: Superseded requires Some(superseded_by).
        let result = Authority::new(
            AuthorityClass::PrivateKnowledge,
            Confidence::new(0.5).expect("0.5 is in range"),
            AuthorityStatus::Superseded,
            SystemTime::UNIX_EPOCH,
            one_evidence(),
            None,
        );
        assert_eq!(result.unwrap_err(), ErrorCode::InvalidInput);
    }

    #[test]
    fn active_with_superseded_by_is_err() {
        // §59.1 I4, direction 2: Active must have superseded_by == None.
        let result = Authority::new(
            AuthorityClass::PrivateKnowledge,
            Confidence::new(0.5).expect("0.5 is in range"),
            AuthorityStatus::Active,
            SystemTime::UNIX_EPOCH,
            one_evidence(),
            Some(MemoryId::new()),
        );
        assert_eq!(result.unwrap_err(), ErrorCode::InvalidInput);
    }

    #[test]
    fn superseded_with_superseded_by_is_ok() {
        // Positive control: I4 must not reject the combination it exists to allow.
        let result = Authority::new(
            AuthorityClass::PrivateKnowledge,
            Confidence::new(0.5).expect("0.5 is in range"),
            AuthorityStatus::Superseded,
            SystemTime::UNIX_EPOCH,
            one_evidence(),
            Some(MemoryId::new()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn active_without_superseded_by_is_ok() {
        let result = Authority::new(
            AuthorityClass::PrivateKnowledge,
            Confidence::new(0.5).expect("0.5 is in range"),
            AuthorityStatus::Active,
            SystemTime::UNIX_EPOCH,
            one_evidence(),
            None,
        );
        assert!(result.is_ok());
    }
}

/// §59.1 G59-1: full-order property test over a deliberately small, guaranteed-colliding
/// domain. Deterministic enumeration, not `proptest`: cycling through the fixed 63-value
/// domain to fill 200 samples *guarantees* (rather than merely makes likely) the same
/// pigeonhole collision spec pseudocode's random generator relies on, so this needs no
/// dependency `humaux-domain` doesn't already have. `wide_domain_corpus_has_no_collision`
/// below is the 反证 half: the same generator restored to a wide (non-colliding) domain.
#[cfg(test)]
mod g59_1_resolve_total_order {
    use super::*;
    use std::time::Duration;

    const CLASSES: [AuthorityClass; 7] = [
        AuthorityClass::PublicKnowledge,
        AuthorityClass::PrivateKnowledge,
        AuthorityClass::UserPreference,
        AuthorityClass::ProjectDecision,
        AuthorityClass::UserCorrection,
        AuthorityClass::ProjectConstraint,
        AuthorityClass::ExplicitTaskContext,
    ];
    const TIMES_SECS: [u64; 3] = [0, 1, 2];
    const CONFIDENCES: [f32; 3] = [0.1, 0.5, 0.9];

    /// The 7×3×3 = 63-value domain, as (class, asserted_at, confidence) triples.
    fn combos() -> Vec<(AuthorityClass, SystemTime, f32)> {
        let mut out = Vec::with_capacity(63);
        for &class in &CLASSES {
            for &secs in &TIMES_SECS {
                for &conf in &CONFIDENCES {
                    out.push((
                        class,
                        SystemTime::UNIX_EPOCH + Duration::from_secs(secs),
                        conf,
                    ));
                }
            }
        }
        out
    }

    /// 200 `(MemoryId, Authority)` samples cycling through the 63-value domain — each sample
    /// gets its own fresh `MemoryId`, so I1's 4th tier is the only thing that can ever break a
    /// tie among same-triple samples.
    fn corpus_200() -> Vec<(MemoryId, Authority)> {
        let combos = combos();
        assert_eq!(
            combos.len(),
            63,
            "generator domain must be 7*3*3=63 (§59.1 G59-1)"
        );
        (0..200)
            .map(|i| {
                let (class, asserted_at, conf) = combos[i % combos.len()];
                let authority = Authority::new(
                    class,
                    Confidence::new(conf).expect("fixed sample is in [0,1]"),
                    AuthorityStatus::Active,
                    asserted_at,
                    NonEmptyVec::new(vec![EvidenceId::new()]).expect("one element is non-empty"),
                    None,
                )
                .expect("valid Active authority with no superseded_by");
                (MemoryId::new(), authority)
            })
            .collect()
    }

    fn has_three_way_collision(corpus: &[(MemoryId, Authority)]) -> bool {
        for i in 0..corpus.len() {
            for j in (i + 1)..corpus.len() {
                let (_, a) = &corpus[i];
                let (_, b) = &corpus[j];
                if a.class == b.class
                    && a.asserted_at == b.asserted_at
                    && a.confidence.get() == b.confidence.get()
                {
                    return true;
                }
            }
        }
        false
    }

    #[test]
    fn positive_control_has_three_way_collision() {
        // Must run — and pass — before the main assertion means anything: 200 samples over a
        // 63-value domain must pigeonhole-collide, and if this ever stops being true the main
        // assertion below would be silently vacuous (§53.3 规则 3).
        assert!(
            has_three_way_collision(&corpus_200()),
            "positive control failed: no 3-way (class, asserted_at, confidence) collision in \
             the 200-sample corpus — the domain size or sample count changed"
        );
    }

    /// §59.1 G59-1 反证 half: with the generator restored to a wide domain (distinct
    /// `asserted_at` per sample, spec pseudocode's default), the positive control's
    /// collision must vanish — proving `has_three_way_collision` actually scans rather than
    /// being vacuously true (§53.3 规则 3's "observe the gate fail" requirement).
    #[test]
    fn wide_domain_corpus_has_no_collision() {
        let wide: Vec<(MemoryId, Authority)> = (0..200)
            .map(|i| {
                let authority = Authority::new(
                    AuthorityClass::PrivateKnowledge,
                    Confidence::new(i as f32 / 200.0).expect("i/200 is in [0,1) for i < 200"),
                    AuthorityStatus::Active,
                    SystemTime::UNIX_EPOCH + Duration::from_nanos(i as u64),
                    NonEmptyVec::new(vec![EvidenceId::new()]).expect("one element is non-empty"),
                    None,
                )
                .expect("valid Active authority with no superseded_by");
                (MemoryId::new(), authority)
            })
            .collect();
        assert!(
            !has_three_way_collision(&wide),
            "wide-domain corpus (distinct asserted_at per sample) must never collide — if it \
             does, has_three_way_collision is not actually comparing fields"
        );
    }

    #[test]
    fn resolve_matches_i1_key_tuple_on_every_pair() {
        let corpus = corpus_200();
        assert!(
            has_three_way_collision(&corpus),
            "corpus must still collide (see above test)"
        );

        // A std tuple's `Ord` is provably total, antisymmetric, and transitive by
        // construction; matching `resolve()` against it on every pair is therefore sufficient
        // evidence `resolve()` itself has those same three properties (§59.1 G59-1's "反对称 +
        // 传递 + 无平局"), and additionally proves there is no tie among distinct memory_ids —
        // `to_bits()` preserves order for non-negative finite floats (§59.1 I5 guarantees
        // Confidence is always finite and >= 0).
        let key = |(id, a): &(MemoryId, Authority)| {
            (
                a.class as i32,
                a.asserted_at,
                a.confidence.get().to_bits(),
                id.0,
            )
        };
        for i in 0..corpus.len() {
            for j in 0..corpus.len() {
                let expected = key(&corpus[i]).cmp(&key(&corpus[j]));
                let actual = resolve(&corpus[i], &corpus[j]);
                assert_eq!(
                    actual, expected,
                    "resolve() disagrees with the I1 key tuple at ({i}, {j})"
                );
                if i != j {
                    assert_ne!(
                        actual,
                        Ordering::Equal,
                        "distinct memory_id must never tie at ({i}, {j})"
                    );
                }
            }
        }
    }

    /// Red-then-green evidence (§80.1): a mutant lacking I1's 4th tier must reproduce a tie on
    /// the guaranteed collision — proving the real `resolve()` (which keeps the 4th tier) is
    /// what stands between green and this failure, not an untested code path.
    #[test]
    fn injected_fault_deleting_tier4_ties_on_collision() {
        fn resolve_without_tier4(a: &(MemoryId, Authority), b: &(MemoryId, Authority)) -> Ordering {
            let (_, auth_a) = a;
            let (_, auth_b) = b;
            // injected fault (§59.1 I1 tier 4, §80.1 red-then-green): deliberately missing
            // the memory_id tier — not a shortcut left in real code.
            auth_a
                .class
                .cmp(&auth_b.class)
                .then_with(|| auth_a.asserted_at.cmp(&auth_b.asserted_at))
                .then_with(|| {
                    auth_a
                        .confidence
                        .get()
                        .partial_cmp(&auth_b.confidence.get())
                        .unwrap()
                })
        }

        let corpus = corpus_200();
        let mut found_tie = false;
        'outer: for i in 0..corpus.len() {
            for j in (i + 1)..corpus.len() {
                if resolve_without_tier4(&corpus[i], &corpus[j]) == Ordering::Equal {
                    found_tie = true;
                    break 'outer;
                }
            }
        }
        assert!(
            found_tie,
            "mutant lacking I1's 4th tier must tie on the guaranteed collision pair — if it \
             doesn't, the corpus stopped guaranteeing a collision and this gate is vacuous"
        );

        // Green half: the real resolve() must not tie on the same corpus.
        for i in 0..corpus.len() {
            for j in (i + 1)..corpus.len() {
                assert_ne!(resolve(&corpus[i], &corpus[j]), Ordering::Equal);
            }
        }
    }
}

/// §59.1 I3: a non-`Active` row must not participate in adjudication, even when it would
/// otherwise win on class alone — the `filter(is_decisive)` in `adjudicate` is this
/// invariant's only code, and it was previously observed to be a dead condition: every
/// fixture in this file used `AuthorityStatus::Active`, so deleting the filter left `cargo
/// test` green (§53.3 规则 3 — a gate with no injected-fault evidence does not exist).
#[cfg(test)]
mod i3_non_active_excluded_from_adjudication {
    use super::*;

    fn authority(class: AuthorityClass, status: AuthorityStatus) -> (MemoryId, Authority) {
        let superseded_by = if status == AuthorityStatus::Superseded {
            Some(MemoryId::new())
        } else {
            None
        };
        let authority = Authority::new(
            class,
            Confidence::new(0.5).expect("0.5 is in range"),
            status,
            SystemTime::UNIX_EPOCH,
            NonEmptyVec::new(vec![EvidenceId::new()]).expect("one element is non-empty"),
            superseded_by,
        )
        .expect("valid fixture for the given status");
        (MemoryId::new(), authority)
    }

    #[test]
    fn higher_class_superseded_loses_to_lower_class_active() {
        // Superseded outranks Active on class alone (I1 tier 1); if I3's exclusion were
        // missing, `adjudicate` would still pick the higher class here via `resolve()` and
        // this assertion would fail — a test that is sensitive to the filter, not just to
        // class ordering.
        let superseded = authority(
            AuthorityClass::ExplicitTaskContext,
            AuthorityStatus::Superseded,
        );
        let active = authority(AuthorityClass::PublicKnowledge, AuthorityStatus::Active);
        let candidates = [superseded, active];
        let winner = adjudicate(&candidates).expect("one Active candidate is present");
        assert_eq!(winner.1.class(), AuthorityClass::PublicKnowledge);
        assert_eq!(winner.1.status, AuthorityStatus::Active);
    }

    #[test]
    fn all_non_active_candidates_yield_none() {
        let candidates = [
            authority(
                AuthorityClass::ExplicitTaskContext,
                AuthorityStatus::Superseded,
            ),
            authority(AuthorityClass::ProjectConstraint, AuthorityStatus::Revoked),
            authority(AuthorityClass::UserCorrection, AuthorityStatus::Expired),
        ];
        assert!(adjudicate(&candidates).is_none());
    }

    /// Red-then-green evidence (§53.3 规则 3): a mutant `adjudicate` without the I3 filter
    /// must pick the Superseded candidate on the fixture above — proving the real
    /// `adjudicate`'s filter is what stands between green and this failure.
    #[test]
    fn injected_fault_removing_filter_flips_the_winner() {
        fn adjudicate_without_filter(
            candidates: &[(MemoryId, Authority)],
        ) -> Option<&(MemoryId, Authority)> {
            // injected fault (§59.1 I3, §80.1 red-then-green): deliberately missing the
            // is_decisive filter — not a shortcut left in real code.
            candidates.iter().max_by(|a, b| resolve(a, b))
        }

        let superseded = authority(
            AuthorityClass::ExplicitTaskContext,
            AuthorityStatus::Superseded,
        );
        let active = authority(AuthorityClass::PublicKnowledge, AuthorityStatus::Active);
        let candidates = [superseded, active];

        let red = adjudicate_without_filter(&candidates).expect("non-empty");
        assert_eq!(red.1.status, AuthorityStatus::Superseded);

        let green = adjudicate(&candidates).expect("one Active candidate is present");
        assert_eq!(green.1.status, AuthorityStatus::Active);
    }
}

/// §59.1 G59-2: Public Knowledge never overrides, tested at two adjacent-in-priority pairs.
#[cfg(test)]
mod g59_2_public_does_not_override {
    use super::*;

    fn active_authority(class: AuthorityClass, confidence: f32) -> (MemoryId, Authority) {
        let authority = Authority::new(
            class,
            Confidence::new(confidence).expect("fixture confidence is in range"),
            // Must be Active: a Superseded fixture would be excluded by I3 before the
            // judgment-order tier is ever reached, testing I3 instead of I1 (§59.1 G59-2).
            AuthorityStatus::Active,
            SystemTime::UNIX_EPOCH,
            NonEmptyVec::new(vec![EvidenceId::new()]).expect("one element is non-empty"),
            None,
        )
        .expect("valid Active fixture");
        (MemoryId::new(), authority)
    }

    #[test]
    fn public_knowledge_loses_to_project_constraint() {
        let public = active_authority(AuthorityClass::PublicKnowledge, 0.99);
        let constraint = active_authority(AuthorityClass::ProjectConstraint, 0.10);
        let candidates = [public, constraint];
        let winner = adjudicate(&candidates).expect("both fixtures are Active");
        assert_eq!(winner.1.class(), AuthorityClass::ProjectConstraint);
    }

    #[test]
    fn public_knowledge_loses_to_private_knowledge() {
        // Adjacent-tier boundary (0 vs 1): the fixture above only exercises 0 vs 5 and would
        // stay green under a thresholded fake (e.g. `class as u8 >= 3`); this one does not —
        // it falls back to comparing confidence unless the class tier is genuinely first
        // (§59.1 G59-2 comment on this gate).
        let public = active_authority(AuthorityClass::PublicKnowledge, 0.99);
        let private = active_authority(AuthorityClass::PrivateKnowledge, 0.10);
        let candidates = [public, private];
        let winner = adjudicate(&candidates).expect("both fixtures are Active");
        assert_eq!(winner.1.class(), AuthorityClass::PrivateKnowledge);
    }

    /// Red-then-green evidence: a mutant that compares confidence before class must return
    /// `PublicKnowledge` on both fixtures above — proving the real `resolve()` (class-first)
    /// is what keeps this gate green.
    #[test]
    fn injected_fault_confidence_before_class_flips_both_fixtures() {
        fn resolve_confidence_first(
            a: &(MemoryId, Authority),
            b: &(MemoryId, Authority),
        ) -> Ordering {
            let (id_a, auth_a) = a;
            let (id_b, auth_b) = b;
            // injected fault (§59.1 G59-2, §80.1 red-then-green): deliberately wrong tier
            // order — not a shortcut left in real code.
            auth_a
                .confidence
                .get()
                .partial_cmp(&auth_b.confidence.get())
                .unwrap()
                .then_with(|| auth_a.class.cmp(&auth_b.class))
                .then_with(|| auth_a.asserted_at.cmp(&auth_b.asserted_at))
                .then_with(|| id_a.0.cmp(&id_b.0))
        }

        let public_a = active_authority(AuthorityClass::PublicKnowledge, 0.99);
        let constraint = active_authority(AuthorityClass::ProjectConstraint, 0.10);
        let public_b = active_authority(AuthorityClass::PublicKnowledge, 0.99);
        let private = active_authority(AuthorityClass::PrivateKnowledge, 0.10);

        let winner_1 = [public_a, constraint]
            .into_iter()
            .max_by(resolve_confidence_first)
            .expect("non-empty");
        let winner_2 = [public_b, private]
            .into_iter()
            .max_by(resolve_confidence_first)
            .expect("non-empty");

        assert_eq!(winner_1.1.class(), AuthorityClass::PublicKnowledge);
        assert_eq!(winner_2.1.class(), AuthorityClass::PublicKnowledge);
    }
}
