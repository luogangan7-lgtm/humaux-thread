//! `application::affect` — pure read-side policy of the §8.5.1 affect axis (ADR-0030 D-D): evaluating an explicit
//!   [`AffectFilter`] over observed annotations and the bounded mood-congruent late rerank.
//! Depends-on: crates=[humaux-domain, uuid]; services=[]; env=[]; modules=[domain::affect]
//! Called-by: [adapters::affect_repo, adapters::read_materialize, gateway::recall]
//! Invariants: []
//! Spec: §8.5.1; ADR-0030
//!
//! No I/O, no clock: callers hand in `now` and the rows.
//!
//! Two invariants live here so every caller (the PG hydrate re-check in
//! `adapters::read_materialize`, the gateway's `recall.search` rerank) shares one definition:
//!
//! * a memory MATCHES a filter when at least one of its annotations satisfies every clause,
//!   judged on the read-time [`effective_intensity`] — never on the raw row for a MOOD;
//! * a mood-congruent rerank is a PERMUTATION of the already-visible set: it never adds or
//!   drops a memory, and ties keep the incoming (dense-score) order (stable sort).

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use humaux_domain::affect::{
    AffectAnnotation, AffectFilter, BasisPoints, MoodHalfLife, MoodPoint, effective_intensity,
    memory_congruence,
};
use uuid::Uuid;

/// One stored annotation as the read side sees it: the immutable row plus its derived,
/// never-persisted `effective_intensity`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedAffect {
    pub memory_id: Uuid,
    pub annotation: AffectAnnotation,
    pub effective_intensity: BasisPoints,
}

/// `effective_intensity(now)` for one row from unix-second timestamps: elapsed saturates at zero
/// (a row observed "in the future" by clock skew reads as fresh, never as negative decay).
pub fn effective_intensity_at(
    raw: BasisPoints,
    observed_at_unix: i64,
    now_unix: i64,
    half_life: Option<MoodHalfLife>,
) -> BasisPoints {
    let elapsed = Duration::from_secs(
        now_unix
            .saturating_sub(observed_at_unix)
            .max(0)
            .unsigned_abs(),
    );
    effective_intensity(raw, elapsed, half_life)
}

/// The memory ids among `observed` with at least one annotation satisfying `filter`. An empty
/// filter matches every memory that has an annotation — callers treat an empty filter as
/// "no affect clause" before reaching here.
pub fn memories_matching(filter: &AffectFilter, observed: &[ObservedAffect]) -> HashSet<Uuid> {
    observed
        .iter()
        .filter(|o| filter.matches(&o.annotation, o.effective_intensity))
        .map(|o| o.memory_id)
        .collect()
}

/// ADR-0030 D-D bounded late rerank: reorders `order` (the visible memory ids, dense order) by
/// descending [`memory_congruence`] with `mood`; memories without usable affect data score the
/// neutral midpoint; equal scores keep their incoming order. Same length, same set — always.
pub fn rerank_by_mood(order: Vec<Uuid>, mood: MoodPoint, observed: &[ObservedAffect]) -> Vec<Uuid> {
    let mut by_memory: HashMap<Uuid, Vec<&AffectAnnotation>> = HashMap::new();
    for o in observed {
        by_memory
            .entry(o.memory_id)
            .or_default()
            .push(&o.annotation);
    }
    let mut ranked = order;
    ranked.sort_by_key(|id| {
        std::cmp::Reverse(memory_congruence(
            mood,
            by_memory.get(id).into_iter().flatten().copied(),
        ))
    });
    ranked
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_domain::affect::{AffectKind, BasisPointRange, EmotionLabel, NEUTRAL_CONGRUENCE};

    fn bp(v: i16) -> BasisPoints {
        BasisPoints::signed(v).expect("range")
    }

    fn observed(
        memory_id: Uuid,
        kind: AffectKind,
        v: i16,
        a: i16,
        effective: i16,
    ) -> ObservedAffect {
        ObservedAffect {
            memory_id,
            annotation: AffectAnnotation {
                kind,
                label: Some(EmotionLabel::Frustration),
                valence: Some(bp(v)),
                arousal: Some(bp(a)),
                dominance: None,
                intensity: BasisPoints::unit(8_200).expect("unit"),
                confidence: BasisPoints::MAX,
                target_subject: None,
                target_scope: None,
            },
            effective_intensity: BasisPoints::unit(effective).expect("unit"),
        }
    }

    #[test]
    fn effective_intensity_at_saturates_negative_elapsed() {
        let raw = BasisPoints::unit(8_200).expect("unit");
        let h = MoodHalfLife::new(Duration::from_secs(3_600)).expect("h");
        assert_eq!(
            effective_intensity_at(raw, 1_000, 1_000 + 3_600, Some(h)).get(),
            4_100
        );
        assert_eq!(
            effective_intensity_at(raw, 5_000, 1_000, Some(h)),
            raw,
            "future row = fresh"
        );
        assert_eq!(
            effective_intensity_at(raw, 0, i64::MAX, None),
            raw,
            "emotion never decays"
        );
    }

    #[test]
    fn matching_uses_effective_not_raw_and_any_annotation_suffices() {
        let m1 = Uuid::now_v7();
        let m2 = Uuid::now_v7();
        let rows = vec![
            observed(m1, AffectKind::Mood, -8_000, 5_000, 2_050),
            observed(m2, AffectKind::Emotion, 6_000, 0, 8_200),
            observed(m2, AffectKind::Emotion, -9_000, 0, 8_200),
        ];
        let strong = AffectFilter {
            min_effective_intensity: Some(BasisPoints::unit(5_000).expect("unit")),
            ..AffectFilter::default()
        };
        assert_eq!(memories_matching(&strong, &rows), HashSet::from([m2]));
        let negative = AffectFilter {
            valence: Some(BasisPointRange::new(bp(-10_000), bp(-1)).expect("range")),
            ..AffectFilter::default()
        };
        assert_eq!(memories_matching(&negative, &rows), HashSet::from([m1, m2]));
    }

    #[test]
    fn rerank_is_a_stable_permutation() {
        let a = Uuid::now_v7();
        let b = Uuid::now_v7();
        let c = Uuid::now_v7();
        let mood = MoodPoint {
            valence: bp(-8_000),
            arousal: bp(5_000),
        };
        let rows = vec![
            observed(b, AffectKind::Emotion, -8_000, 5_000, 8_200), // perfect match
            observed(c, AffectKind::Emotion, 10_000, -10_000, 8_200), // far
        ];
        // a has no data => neutral: below the match, above the mismatch; c is buried last.
        assert_eq!(rerank_by_mood(vec![a, b, c], mood, &rows), vec![b, a, c]);
        // No data at all: every memory is neutral, the incoming order is kept verbatim.
        assert_eq!(rerank_by_mood(vec![c, a, b], mood, &[]), vec![c, a, b]);
        assert_eq!(memory_congruence(mood, []), NEUTRAL_CONGRUENCE);
    }
}
