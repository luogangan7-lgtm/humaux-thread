//! `domain::affect` — the §8.5.1 affect annotation axis (ADR-0030, card E1).
//!
//! A Memory says *what* (`MemoryType`), is *about* someone (`subject`), is believed *because*
//! (Authority / Evidence) and is *currently valid or not* (lifecycle). This module adds the fourth
//! orthogonal axis: *what emotional state was observed when it was formed*. `MemoryType` is NOT
//! extended — there is no `Emotion` memory type (it would pollute the type axis into
//! `EmotionFact` / `EmotionDecision` / …). An affect is a measurement attached to any memory.
//!
//! Representation (research ruling): VAD (Russell circumplex + Mehrabian dominance) as the
//! computable primary — normalised basis points in `[-10000, +10000]`, never floats — plus an
//! optional closed [`EmotionLabel`] for UI, explicit filters and human explanation (Scherer:
//! a label is an appraisal-level category, not a more real atomic fact). `intensity` is stored
//! separately from arousal (high-intensity sadness is low-arousal).
//!
//! [`AffectKind::Emotion`] is an event-bound historical observation — its intensity never decays
//! ("five years later you were still angry yesterday"). [`AffectKind::Mood`] is a diffuse state:
//! [`effective_intensity`] derives `raw · 2^(−Δt/half_life)` at read time from the immutable raw
//! row. No literature supports a universal human mood half-life, so the half-life is a frozen
//! product/calibration policy (§78.1, `HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS`) captured at write
//! time and stored per row — never a constant in this crate.
//!
//! Every closed set here maps 1:1 onto a `text` CHECK column of `private.memory_affects`
//! (migration 0156) and is §78.2-contract-tested against the live DB:
//!
//! | Rust                       | column                                     |
//! |----------------------------|--------------------------------------------|
//! | [`AffectKind`]             | `affect_kind` (`EMOTION` / `MOOD`)         |
//! | [`EmotionLabel`]           | `label` (12 values)                        |
//! | [`AffectTargetScopeKind`]  | `target_scope_kind` (5 `§59` scope layers) |
//! | [`BasisPoints`]            | `*_bp` smallint range CHECKs               |

use std::time::Duration;

use crate::error::ErrorCode;
use crate::subject::SubjectId;
use uuid::Uuid;

/// One basis point = 1/10000. Signed axes (VAD) span `[-10000, +10000]`; unit axes (intensity,
/// confidence) span `[0, 10000]`. Integer on purpose: no NaN, no rounding, exact equality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BasisPoints(i16);

impl BasisPoints {
    /// `-1.0` on a signed axis.
    pub const MIN_SIGNED: BasisPoints = BasisPoints(-10_000);
    /// `0.0`.
    pub const ZERO: BasisPoints = BasisPoints(0);
    /// `+1.0` / full intensity / certain.
    pub const MAX: BasisPoints = BasisPoints(10_000);

    /// A signed-axis value (valence / arousal / dominance): `-10000..=10000`, else `INVALID_INPUT`.
    pub fn signed(value: i16) -> Result<Self, ErrorCode> {
        (Self::MIN_SIGNED.0..=Self::MAX.0)
            .contains(&value)
            .then_some(Self(value))
            .ok_or(ErrorCode::InvalidInput)
    }

    /// A unit-axis value (intensity / confidence / effective intensity): `0..=10000`, else
    /// `INVALID_INPUT`.
    pub fn unit(value: i16) -> Result<Self, ErrorCode> {
        (0..=Self::MAX.0)
            .contains(&value)
            .then_some(Self(value))
            .ok_or(ErrorCode::InvalidInput)
    }

    /// The raw basis-point value.
    pub fn get(self) -> i16 {
        self.0
    }
}

/// `EMOTION` (event-bound, never decays) vs `MOOD` (diffuse, decays at read time). Frozen closed
/// set. Wire form = `private.memory_affects.affect_kind`'s CHECK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AffectKind {
    /// A historical observation bound to an event; its intensity is a fact and never decays.
    Emotion,
    /// A diffuse, current-ish state; its effective intensity decays by the stored half-life.
    Mood,
}

impl AffectKind {
    /// All variants — the §78.2 contract-test and exhaustiveness surface.
    pub const ALL: [AffectKind; 2] = [AffectKind::Emotion, AffectKind::Mood];

    /// Wire string frozen on `private.memory_affects.affect_kind`.
    pub fn as_str(self) -> &'static str {
        match self {
            AffectKind::Emotion => "EMOTION",
            AffectKind::Mood => "MOOD",
        }
    }

    /// Parse a wire string; unknown/lowercase input is `None`.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.as_str() == s)
    }
}

/// The optional discrete label. Auxiliary only — UI, explicit filters, human-readable
/// explanation. It never decides Authority and is not a medical/psychological diagnosis.
/// Frozen closed set (12). Wire form = `private.memory_affects.label`'s CHECK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EmotionLabel {
    Joy,
    Sadness,
    Anger,
    Fear,
    Disgust,
    Surprise,
    Affection,
    Anxiety,
    Frustration,
    Calm,
    Excitement,
    Relief,
}

impl EmotionLabel {
    /// All variants — the §78.2 contract-test and exhaustiveness surface.
    pub const ALL: [EmotionLabel; 12] = [
        EmotionLabel::Joy,
        EmotionLabel::Sadness,
        EmotionLabel::Anger,
        EmotionLabel::Fear,
        EmotionLabel::Disgust,
        EmotionLabel::Surprise,
        EmotionLabel::Affection,
        EmotionLabel::Anxiety,
        EmotionLabel::Frustration,
        EmotionLabel::Calm,
        EmotionLabel::Excitement,
        EmotionLabel::Relief,
    ];

    /// Wire string frozen on `private.memory_affects.label`.
    pub fn as_str(self) -> &'static str {
        match self {
            EmotionLabel::Joy => "JOY",
            EmotionLabel::Sadness => "SADNESS",
            EmotionLabel::Anger => "ANGER",
            EmotionLabel::Fear => "FEAR",
            EmotionLabel::Disgust => "DISGUST",
            EmotionLabel::Surprise => "SURPRISE",
            EmotionLabel::Affection => "AFFECTION",
            EmotionLabel::Anxiety => "ANXIETY",
            EmotionLabel::Frustration => "FRUSTRATION",
            EmotionLabel::Calm => "CALM",
            EmotionLabel::Excitement => "EXCITEMENT",
            EmotionLabel::Relief => "RELIEF",
        }
    }

    /// Parse a wire string; unknown/lowercase input is `None`.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.as_str() == s)
    }
}

/// The closed kinds of an optional typed ScopeRef an affect may point at — the five §59 `Scope`
/// layers below tenant/user (`crate::ids::Scope`). Wire form =
/// `private.memory_affects.target_scope_kind`'s CHECK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AffectTargetScopeKind {
    Workspace,
    Repository,
    Task,
    Run,
    Agent,
}

impl AffectTargetScopeKind {
    /// All variants — the §78.2 contract-test and exhaustiveness surface.
    pub const ALL: [AffectTargetScopeKind; 5] = [
        AffectTargetScopeKind::Workspace,
        AffectTargetScopeKind::Repository,
        AffectTargetScopeKind::Task,
        AffectTargetScopeKind::Run,
        AffectTargetScopeKind::Agent,
    ];

    /// Wire string frozen on `private.memory_affects.target_scope_kind`.
    pub fn as_str(self) -> &'static str {
        match self {
            AffectTargetScopeKind::Workspace => "WORKSPACE",
            AffectTargetScopeKind::Repository => "REPOSITORY",
            AffectTargetScopeKind::Task => "TASK",
            AffectTargetScopeKind::Run => "RUN",
            AffectTargetScopeKind::Agent => "AGENT",
        }
    }

    /// Parse a wire string; unknown/lowercase input is `None`.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.as_str() == s)
    }
}

/// A typed ScopeRef target (`target_scope_kind` + `target_scope_id`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AffectTargetScope {
    pub kind: AffectTargetScopeKind,
    pub id: Uuid,
}

/// The frozen mood half-life policy (`HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS`), non-zero by
/// construction so [`effective_intensity`] can never divide by zero. Stored per MOOD row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MoodHalfLife(Duration);

impl MoodHalfLife {
    /// A zero half-life is refused (`INVALID_INPUT`): it would make every mood vanish instantly.
    pub fn new(half_life: Duration) -> Result<Self, ErrorCode> {
        if half_life.is_zero() {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self(half_life))
    }

    pub fn duration(self) -> Duration {
        self.0
    }
}

/// One affect annotation as written and read (the immutable row minus its ids/timestamps).
/// VAD are optional-but-preferred; `intensity`/`confidence` are mandatory unit values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffectAnnotation {
    pub kind: AffectKind,
    pub label: Option<EmotionLabel>,
    pub valence: Option<BasisPoints>,
    pub arousal: Option<BasisPoints>,
    pub dominance: Option<BasisPoints>,
    pub intensity: BasisPoints,
    pub confidence: BasisPoints,
    pub target_subject: Option<SubjectId>,
    pub target_scope: Option<AffectTargetScope>,
}

/// Read-time derived intensity (ADR-0030 D-B). `half_life` is `None` for an EMOTION (raw is a
/// historical fact and is returned unchanged however much time passed) and the row's stored
/// policy for a MOOD: `raw · 2^(−elapsed/half_life)`, rounded, saturating into `[0, raw]` —
/// never negative, never above raw. Pure: the caller supplies the elapsed time.
pub fn effective_intensity(
    raw: BasisPoints,
    elapsed: Duration,
    half_life: Option<MoodHalfLife>,
) -> BasisPoints {
    let Some(half_life) = half_life else {
        return raw;
    };
    let periods = elapsed.as_secs_f64() / half_life.duration().as_secs_f64();
    // f64 -> i16 via `as` saturates and truncates; the value is already rounded and clamped to
    // [0, raw] so the cast is exact for every reachable input.
    #[allow(clippy::cast_possible_truncation)]
    let decayed = (f64::from(raw.get()) * (-periods).exp2())
        .round()
        .clamp(0.0, f64::from(raw.get())) as i16;
    BasisPoints(decayed)
}

/// An inclusive basis-point interval `[lo, hi]` on a signed axis; `lo > hi` is `INVALID_INPUT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BasisPointRange {
    pub lo: BasisPoints,
    pub hi: BasisPoints,
}

impl BasisPointRange {
    pub fn new(lo: BasisPoints, hi: BasisPoints) -> Result<Self, ErrorCode> {
        (lo <= hi)
            .then_some(Self { lo, hi })
            .ok_or(ErrorCode::InvalidInput)
    }

    pub fn contains(self, value: BasisPoints) -> bool {
        (self.lo..=self.hi).contains(&value)
    }
}

/// `recall.search.affect` — the explicit structured affect query (ADR-0030 D-D). Every clause
/// is ANDed; a memory matches when at least ONE of its annotations satisfies all clauses. A
/// range on an axis the annotation did not record (`None`) does not match — absence is never
/// treated as zero.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AffectFilter {
    pub kinds: Vec<AffectKind>,
    pub labels_any: Vec<EmotionLabel>,
    pub valence: Option<BasisPointRange>,
    pub arousal: Option<BasisPointRange>,
    pub dominance: Option<BasisPointRange>,
    pub min_effective_intensity: Option<BasisPoints>,
}

impl AffectFilter {
    /// No clause at all — the unscoped read.
    pub fn is_empty(&self) -> bool {
        self.kinds.is_empty()
            && self.labels_any.is_empty()
            && self.valence.is_none()
            && self.arousal.is_none()
            && self.dominance.is_none()
            && self.min_effective_intensity.is_none()
    }

    /// Whether one annotation (with its read-time `effective` intensity) satisfies every clause.
    pub fn matches(&self, annotation: &AffectAnnotation, effective: BasisPoints) -> bool {
        let axis = |range: Option<BasisPointRange>, value: Option<BasisPoints>| match range {
            None => true,
            Some(range) => value.is_some_and(|v| range.contains(v)),
        };
        (self.kinds.is_empty() || self.kinds.contains(&annotation.kind))
            && (self.labels_any.is_empty()
                || annotation
                    .label
                    .is_some_and(|label| self.labels_any.contains(&label)))
            && axis(self.valence, annotation.valence)
            && axis(self.arousal, annotation.arousal)
            && axis(self.dominance, annotation.dominance)
            && self
                .min_effective_intensity
                .is_none_or(|min| effective >= min)
    }
}

/// `recall.search.mood_congruence` — the reader's current mood as a (valence, arousal) point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MoodPoint {
    pub valence: BasisPoints,
    pub arousal: BasisPoints,
}

/// Congruence a memory with NO usable affect data receives in a mood-congruent rerank — the
/// midpoint, so an annotated match ranks above it and an annotated mismatch below it, and
/// un-annotated memories are neither promoted nor buried.
pub const NEUTRAL_CONGRUENCE: BasisPoints = BasisPoints(5_000);

/// Mood congruence of one annotation with the reader's mood, in `[0, 10000]`: `10000 −
/// (|Δvalence| + |Δarousal|) / 4` (Manhattan distance on the circumplex, max 40000 → 0). `None`
/// when the annotation recorded no valence or no arousal.
pub fn mood_congruence(mood: MoodPoint, annotation: &AffectAnnotation) -> Option<BasisPoints> {
    let (Some(v), Some(a)) = (annotation.valence, annotation.arousal) else {
        return None;
    };
    let distance = (i32::from(v.get()) - i32::from(mood.valence.get())).abs()
        + (i32::from(a.get()) - i32::from(mood.arousal.get())).abs();
    let score = i32::from(BasisPoints::MAX.get()) - distance / 4;
    // 0 <= score <= 10000 by construction (distance <= 40000).
    #[allow(clippy::cast_possible_truncation)]
    Some(BasisPoints(
        score.clamp(0, i32::from(BasisPoints::MAX.get())) as i16,
    ))
}

/// A memory's congruence = the best of its annotations, or [`NEUTRAL_CONGRUENCE`] when none
/// carries a (valence, arousal) pair.
pub fn memory_congruence<'a>(
    mood: MoodPoint,
    annotations: impl IntoIterator<Item = &'a AffectAnnotation>,
) -> BasisPoints {
    annotations
        .into_iter()
        .filter_map(|a| mood_congruence(mood, a))
        .max()
        .unwrap_or(NEUTRAL_CONGRUENCE)
}

/// The non-destructive affect write (ADR-0030 D-C): `memory.annotate_affect` appends affect
/// rows to an existing visible memory. Ordinary admitted write, NO confirm gate (§33.10 gates
/// destructive ops only; annotating deletes/hides nothing). Frozen closed set, same shape as
/// `subject::SubjectWriteOp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AffectWriteOp {
    Annotate,
}

impl AffectWriteOp {
    /// All variants.
    pub const ALL: [AffectWriteOp; 1] = [AffectWriteOp::Annotate];

    /// The MCP operation key (`contracts/mcp/memory.schema.json`'s `x-humaux-operation`).
    pub const fn operation_key(self) -> &'static str {
        match self {
            AffectWriteOp::Annotate => "memory.annotate_affect",
        }
    }

    /// Reverse lookup for the gateway dispatcher; a non-affect key is `None`.
    pub fn parse_operation_key(key: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|op| op.operation_key() == key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bp(v: i16) -> BasisPoints {
        BasisPoints::signed(v).expect("in range")
    }

    fn annotation(
        kind: AffectKind,
        label: Option<EmotionLabel>,
        v: i16,
        a: i16,
    ) -> AffectAnnotation {
        AffectAnnotation {
            kind,
            label,
            valence: Some(bp(v)),
            arousal: Some(bp(a)),
            dominance: None,
            intensity: BasisPoints::unit(8_200).expect("unit"),
            confidence: BasisPoints::MAX,
            target_subject: None,
            target_scope: None,
        }
    }

    #[test]
    fn basis_point_boundaries_are_exact() {
        assert!(BasisPoints::signed(-10_000).is_ok());
        assert!(BasisPoints::signed(10_000).is_ok());
        assert_eq!(BasisPoints::signed(10_001), Err(ErrorCode::InvalidInput));
        assert_eq!(BasisPoints::signed(-10_001), Err(ErrorCode::InvalidInput));
        assert!(BasisPoints::unit(0).is_ok());
        assert!(BasisPoints::unit(10_000).is_ok());
        assert_eq!(BasisPoints::unit(-1), Err(ErrorCode::InvalidInput));
        assert_eq!(BasisPoints::unit(10_001), Err(ErrorCode::InvalidInput));
        assert_eq!(
            BasisPointRange::new(bp(5), bp(4)),
            Err(ErrorCode::InvalidInput)
        );
        assert!(
            BasisPointRange::new(bp(-1), bp(-1))
                .expect("point range")
                .contains(bp(-1))
        );
    }

    #[test]
    fn emotion_never_decays_and_mood_halves_per_half_life() {
        let raw = BasisPoints::unit(8_200).expect("unit");
        let five_years = Duration::from_secs(5 * 365 * 24 * 3600);
        assert_eq!(effective_intensity(raw, five_years, None), raw);
        let h = MoodHalfLife::new(Duration::from_secs(3_600)).expect("half-life");
        assert_eq!(effective_intensity(raw, Duration::ZERO, Some(h)), raw);
        assert_eq!(
            effective_intensity(raw, Duration::from_secs(3_600), Some(h)).get(),
            4_100
        );
        assert_eq!(
            effective_intensity(raw, Duration::from_secs(7_200), Some(h)).get(),
            2_050
        );
        // Saturating: never negative, never above raw, even at absurd elapsed times.
        assert_eq!(
            effective_intensity(raw, five_years, Some(h)),
            BasisPoints::ZERO
        );
        assert_eq!(
            effective_intensity(BasisPoints::ZERO, Duration::ZERO, Some(h)),
            BasisPoints::ZERO
        );
        assert_eq!(
            MoodHalfLife::new(Duration::ZERO),
            Err(ErrorCode::InvalidInput)
        );
    }

    #[test]
    fn closed_sets_round_trip_and_reject_lowercase() {
        for k in AffectKind::ALL {
            assert_eq!(AffectKind::parse(k.as_str()), Some(k));
            assert_eq!(AffectKind::parse(&k.as_str().to_lowercase()), None);
        }
        assert_eq!(EmotionLabel::ALL.len(), 12);
        for l in EmotionLabel::ALL {
            assert_eq!(EmotionLabel::parse(l.as_str()), Some(l));
        }
        assert_eq!(EmotionLabel::parse("frustration"), None);
        for s in AffectTargetScopeKind::ALL {
            assert_eq!(AffectTargetScopeKind::parse(s.as_str()), Some(s));
        }
        assert_eq!(
            AffectWriteOp::parse_operation_key("memory.annotate_affect"),
            Some(AffectWriteOp::Annotate)
        );
        assert_eq!(AffectWriteOp::parse_operation_key("memory.get"), None);
    }

    #[test]
    fn filter_ands_clauses_and_never_treats_absent_axis_as_zero() {
        let frustrated = annotation(
            AffectKind::Emotion,
            Some(EmotionLabel::Frustration),
            -8_000,
            5_000,
        );
        let calm = annotation(AffectKind::Mood, Some(EmotionLabel::Calm), 6_000, -4_000);
        let unmeasured = AffectAnnotation {
            valence: None,
            arousal: None,
            ..frustrated.clone()
        };
        let negative = AffectFilter {
            valence: Some(BasisPointRange::new(bp(-10_000), bp(-1)).expect("range")),
            ..AffectFilter::default()
        };
        assert!(AffectFilter::default().is_empty());
        assert!(negative.matches(&frustrated, BasisPoints::MAX));
        assert!(!negative.matches(&calm, BasisPoints::MAX));
        assert!(
            !negative.matches(&unmeasured, BasisPoints::MAX),
            "absent axis never matches a range"
        );
        let labelled = AffectFilter {
            labels_any: vec![EmotionLabel::Calm, EmotionLabel::Joy],
            kinds: vec![AffectKind::Mood],
            min_effective_intensity: Some(BasisPoints::unit(5_000).expect("unit")),
            ..AffectFilter::default()
        };
        assert!(labelled.matches(&calm, BasisPoints::unit(5_000).expect("unit")));
        assert!(
            !labelled.matches(&calm, BasisPoints::unit(4_999).expect("unit")),
            "decayed below min"
        );
        assert!(!labelled.matches(&frustrated, BasisPoints::MAX));
    }

    #[test]
    fn mood_congruence_is_bounded_and_neutral_without_data() {
        let mood = MoodPoint {
            valence: bp(-8_000),
            arousal: bp(5_000),
        };
        let same = annotation(AffectKind::Emotion, None, -8_000, 5_000);
        let opposite = annotation(AffectKind::Emotion, None, 10_000, -10_000);
        assert_eq!(mood_congruence(mood, &same), Some(BasisPoints::MAX));
        let far = mood_congruence(mood, &opposite).expect("measured");
        assert!(far < NEUTRAL_CONGRUENCE && far >= BasisPoints::ZERO);
        let unmeasured = AffectAnnotation {
            valence: None,
            ..same.clone()
        };
        assert_eq!(mood_congruence(mood, &unmeasured), None);
        assert_eq!(memory_congruence(mood, []), NEUTRAL_CONGRUENCE);
        assert_eq!(
            memory_congruence(mood, [&opposite, &same]),
            BasisPoints::MAX
        );
    }
}
