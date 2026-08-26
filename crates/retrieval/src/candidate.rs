//! `retrieval::candidate` — §24 Candidate Builder + §63 rerank token-budget pack.
//!
//! §24: "不要直接取 RRF top-N" — coverage-aware allocation reserves budget per facet *before*
//! ranking, so one loud recall lane (typically `dense`) cannot flood out every other facet's
//! candidates before dedup/packing even runs.

use std::collections::BTreeSet;

/// A retrieval facet — §24's own Continuity table, verbatim: four `reserve` facets (each
/// guaranteed a minimum slice regardless of fusion score) and five `budget` facets (compete
/// for whatever remains). Closed set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Facet {
    // reserve
    State,
    Constraints,
    Decisions,
    Issues,
    // budget
    Recent,
    Dense,
    Sparse,
    Code,
    Association,
}

impl Facet {
    /// §24's `reserve` column, in table order.
    pub const RESERVE: [Facet; 4] = [
        Facet::State,
        Facet::Constraints,
        Facet::Decisions,
        Facet::Issues,
    ];
    /// §24's `budget` column, in table order.
    pub const BUDGET: [Facet; 5] = [
        Facet::Recent,
        Facet::Dense,
        Facet::Sparse,
        Facet::Code,
        Facet::Association,
    ];

    pub fn is_reserved(self) -> bool {
        Self::RESERVE.contains(&self)
    }
}

/// One retrieval candidate — just what allocation/dedup/pack need, not a full
/// `RetrievalCard` (§17's projection payload; assembling one is out of this crate's scope).
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub id: String,
    pub facet: Facet,
    pub fusion_score: f32,
    pub estimated_rerank_tokens: u32,
}

/// §63's `pack_by_token_budget`: greedy left-to-right over an already-ordered list. By the
/// time [`build_candidates`] calls this, facet reserve has already put reserved-facet picks
/// first and budget-facet fill after (§24), so a caller that wants "facet reserve, then pack
/// by fusion score/cost" gets it by ordering the input before this call — the packing
/// arithmetic itself is §63's reference implementation verbatim, no facet awareness of its
/// own.
pub fn pack_by_token_budget(
    candidates: impl IntoIterator<Item = Candidate>,
    budget: u32,
) -> Vec<Candidate> {
    let mut used = 0u32;
    let mut out = Vec::new();

    for candidate in candidates {
        let cost = candidate.estimated_rerank_tokens;
        if used.saturating_add(cost) > budget {
            continue;
        }
        used += cost;
        out.push(candidate);
    }

    out
}

/// §24 coverage-aware candidate allocation, in one call: facet reserve → budget fill → dedup
/// → §63 token-budget pack.
///
/// 1. **Facet reserve**: each of [`Facet::RESERVE`]'s four facets gets up to
///    `reserve_per_facet` slots, filled by descending `fusion_score` *within that facet only*
///    — before anything from a budget facet is considered at all. This is the actual fix for
///    §24's "某一路召回把其他 facet 全淹没": a `state` candidate that scored below a flood of
///    `dense` hits still gets its own guaranteed slice, because the flood is never compared
///    against it in this pass.
/// 2. **Budget fill**: whatever slots remain, up to `total_candidates`, are filled from the
///    whole remaining pool (any facet, reserved or budget) ranked by `fusion_score` — a
///    reserved-facet candidate that missed its own reserve slice still competes here.
/// 3. **Dedup**: by `id`; first occurrence wins. The reserve pass runs first, so a candidate
///    that already qualified for its facet's reserve can never be displaced by a later,
///    lower-priority appearance of the same id during budget fill.
/// 4. **Pack**: [`pack_by_token_budget`] over the deduped, priority-ordered list — reserved
///    picks are packed first, so the reserve step is not undone by a later `dense` hit
///    consuming the token budget before a reserved facet's own slice is packed.
pub fn build_candidates(
    mut pool: Vec<Candidate>,
    reserve_per_facet: usize,
    total_candidates: usize,
    max_rerank_tokens: u32,
) -> Vec<Candidate> {
    // `total_cmp` (not `partial_cmp().unwrap_or(Equal)`): a NaN `fusion_score` needs a defined
    // total order, not "compares Equal to everything", which would otherwise scatter it to an
    // arbitrary position instead of a deterministic one.
    pool.sort_by(|a, b| b.fusion_score.total_cmp(&a.fusion_score));

    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut ordered: Vec<Candidate> = Vec::new();

    'reserve: for facet in Facet::RESERVE {
        if ordered.len() >= total_candidates {
            break 'reserve;
        }
        let mut taken = 0usize;
        for c in pool.iter().filter(|c| c.facet == facet) {
            if taken >= reserve_per_facet || ordered.len() >= total_candidates {
                break;
            }
            if seen.insert(c.id.clone()) {
                ordered.push(c.clone());
                taken += 1;
            }
        }
    }

    for c in pool.iter() {
        if ordered.len() >= total_candidates {
            break;
        }
        if seen.insert(c.id.clone()) {
            ordered.push(c.clone());
        }
    }

    pack_by_token_budget(ordered, max_rerank_tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: &str, facet: Facet, score: f32, tokens: u32) -> Candidate {
        Candidate {
            id: id.to_string(),
            facet,
            fusion_score: score,
            estimated_rerank_tokens: tokens,
        }
    }

    /// §24's own worked concern, verbatim: "避免某一路召回把其他 facet 全淹没" — 40 `dense`
    /// hits all outscore the single `state` candidate; without a reserve, a `total_candidates`
    /// cutoff of 10 drops `state` entirely.
    #[test]
    fn flooded_facet_does_not_drown_out_a_reserved_facet() {
        let mut pool = vec![candidate("state-1", Facet::State, 0.1, 50)];
        for i in 0..40 {
            pool.push(candidate(&format!("dense-{i}"), Facet::Dense, 0.9, 50));
        }
        let out = build_candidates(pool, 1, 10, 100_000);
        assert!(
            out.iter().any(|c| c.id == "state-1"),
            "reserved facet must survive even when every budget-facet score outranks it"
        );
    }

    #[test]
    fn reserve_caps_at_reserve_per_facet_even_with_more_candidates_in_that_facet() {
        let pool = vec![
            candidate("state-1", Facet::State, 0.9, 10),
            candidate("state-2", Facet::State, 0.8, 10),
            candidate("state-3", Facet::State, 0.7, 10),
        ];
        let out = build_candidates(pool, 2, 2, 100_000);
        // reserve_per_facet=2 and total_candidates=2 together cap the whole result at 2.
        assert_eq!(out.len(), 2);
        assert!(out.iter().any(|c| c.id == "state-1"));
        assert!(out.iter().any(|c| c.id == "state-2"));
    }

    #[test]
    fn dedup_keeps_first_occurrence_from_reserve_pass() {
        // Same id cannot literally appear twice with two different facets in real data, but
        // the dedup mechanism itself (seen-set keyed by id) is what this proves: a reserve-
        // pass pick is never re-pushed during budget fill.
        let pool = vec![candidate("a", Facet::State, 0.5, 10)];
        let out = build_candidates(pool, 5, 5, 100_000);
        assert_eq!(out.iter().filter(|c| c.id == "a").count(), 1);
    }

    // ---- §63 pack_by_token_budget ----

    #[test]
    fn pack_never_exceeds_max_rerank_tokens() {
        let candidates = vec![
            candidate("a", Facet::Dense, 0.9, 40),
            candidate("b", Facet::Dense, 0.8, 40),
            candidate("c", Facet::Dense, 0.7, 40),
        ];
        let out = pack_by_token_budget(candidates, 90);
        let total: u32 = out.iter().map(|c| c.estimated_rerank_tokens).sum();
        assert!(total <= 90, "packed total {total} exceeds budget 90");
        assert_eq!(
            out.len(),
            2,
            "third candidate (40) would push 120 > 90, skipped"
        );
    }

    #[test]
    fn build_candidates_respects_token_budget_after_allocation() {
        let mut pool = vec![candidate("state-1", Facet::State, 0.5, 40)];
        for i in 0..5 {
            pool.push(candidate(&format!("dense-{i}"), Facet::Dense, 0.9, 40));
        }
        let out = build_candidates(pool, 1, 10, 80);
        let total: u32 = out.iter().map(|c| c.estimated_rerank_tokens).sum();
        assert!(total <= 80);
    }

    /// §23.3 `candidate_count == profile.cand_k` requires `build_candidates` itself to respect
    /// `total_candidates`, not just the token-budget pack downstream of it. Before the fix,
    /// `reserve_per_facet=5` over all 4 reserve facets (20 slots) blew straight past
    /// `total_candidates=10` because the reserve pass never checked the cap.
    #[test]
    fn reserve_pass_never_exceeds_total_candidates() {
        let mut pool = Vec::new();
        for facet in Facet::RESERVE {
            for i in 0..5 {
                pool.push(candidate(&format!("{facet:?}-{i}"), facet, 0.9, 10));
            }
        }
        let out = build_candidates(pool, 5, 10, 100_000);
        assert!(
            out.len() <= 10,
            "reserve pass alone produced {} candidates, over total_candidates=10",
            out.len()
        );
    }

    /// A `NaN` `fusion_score` must not scatter to an arbitrary position (`partial_cmp`
    /// returning `None` compares `Equal` to everything) — `total_cmp` gives it one, so the
    /// call is deterministic and does not panic.
    #[test]
    fn nan_fusion_score_sorts_deterministically_without_panicking() {
        let pool = vec![
            candidate("a", Facet::Dense, f32::NAN, 10),
            candidate("b", Facet::Dense, 0.5, 10),
            candidate("c", Facet::Dense, 0.9, 10),
        ];
        let out = build_candidates(pool, 0, 3, 100_000);
        assert_eq!(out.len(), 3);
    }
}
