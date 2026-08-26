//! `application::retrieve` — Phase 6 T6.2 (§20.0 / §20#G20-2 / G80-39): the online
//! recall/context/continuity orchestration entry point that resolves *which* query-transform
//! a call runs under, before any adapter ever touches PostgreSQL/Qdrant/a reasoning provider.
//!
//! Deliberately pure — no I/O, no provider SDK (§3/§78.3): the real DB-backed recall path
//! lives in `adapters::retrieve` (`recall_with_overlay` et al. — see that module's own doc
//! comment for why the real implementation goes there and not here), and `context`/
//! `continuity`'s DB legs will follow the same precedent once built. This module's only job
//! is the seam §20.0 freezes: recall/context/continuity MUST NOT *implicitly* reach a
//! `UserReasoningProvider` or `PublicReasoningProvider` — so this module, on purpose, never
//! names either type and never imports `humaux_adapters::byok` or anything with a provider
//! SDK dependency. `xtask architecture-check`'s G20-2/G80-39 sub-check enforces that as a
//! standing zero-count invariant over this file (+ its online-lane siblings), not a one-time
//! review note — see that check's own doc comment for the injected-fault proof.
//!
//! A generative query transform can only run under an explicitly-registered
//! [`RetrievalProfile`] whose `query_transform` is [`QueryTransform::Generative`] — never
//! inferred from request shape, never a bare bool flag. §55.6 additionally forbids an
//! eval-only profile ("不存在只给评测用的 profile"): every profile this registry could ever
//! hold must be able to run in production too, hence `production_enabled` has no `Default`
//! and the only public constructor below always sets it explicitly.
//!
//! Not yet wired to a caller: `resolve_profile`/[`RetrievalProfile`]/[`QueryTransform`] have
//! no callers anywhere in this workspace outside this module's own tests today (`adapters::
//! retrieve`'s `recall_with_overlay` does not call in here yet). This module ships the T6.2
//! seam type ahead of that wiring so `xtask architecture-check`'s G20-2/G80-39 scan domain
//! has a real file to enforce the zero-provider-dependency invariant over from day one;
//! landing the `adapters::retrieve` call site is separate follow-up work, not this module's.

/// §20.0: how a query is prepared *before* the deterministic Planner (§20) ever runs.
/// `Deterministic` is the only variant the default profile may select; `Generative` requires
/// an explicit, named profile version — never a bare bool, so every generative call site is
/// always traceable to one [`RetrievalProfile`] row (§55.6 `profile_fingerprint`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryTransform {
    Deterministic,
    /// §20.0's own required field list for a generative profile (provider/purpose/token
    /// budget/timeout/fallback direction/`profile_fingerprint`/benchmark manifest) does NOT
    /// yet live anywhere — not on this variant (a bare `&'static str` version marker cannot
    /// carry them) and not on the owning [`RetrievalProfile`] either, which today has no
    /// fields beyond `profile_id`/`query_transform`/`production_enabled`. A `Generative`
    /// profile can currently be constructed with none of §20.0's required fields present and
    /// nothing rejects it; the full field list is deferred to the §50 typed config registry
    /// delivery this profile type is meant to move into (§80.1.1 tracks the debt).
    Generative {
        version: &'static str,
    },
}

/// §20.0/§55.6: one registered retrieval profile. `production_enabled` is not optional and
/// has no `Default` — §55.6 freezes "不存在只给评测用的 profile"; a profile that cannot run
/// in production has no legal reason to exist in this registry at all.
///
/// This is a Rust-side stand-in, not the frozen §55.6 profile registry itself: §55.6 defines
/// "retrieval profile" as a row in the §50 typed config registry (top_k/cand_k depth,
/// garbage-value fail-loud, an effective-config fingerprint) keyed to a `retrieval::
/// build_request` (§55.1) request — neither of which exists yet. Do not read the three fields
/// below as that registry; when `build_request` lands, this type should either move into
/// `config/` alongside it or be deleted in favor of it, not grow into a second definition of
/// "retrieval profile" living in parallel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalProfile {
    pub profile_id: &'static str,
    pub query_transform: QueryTransform,
    pub production_enabled: bool,
}

impl RetrievalProfile {
    /// §20.0: "默认 profile 仍 `query_transform = deterministic`" — the only profile the
    /// online recall/context/continuity entry point may fall back to when a caller names
    /// none.
    pub const fn default_profile() -> Self {
        Self {
            profile_id: "default_deterministic",
            query_transform: QueryTransform::Deterministic,
            production_enabled: true,
        }
    }
}

/// The online orchestration entry point itself: resolves which profile a
/// recall/context/continuity call runs under. No implicit "generative if X" branch exists
/// anywhere in this function — an explicit profile in, or the frozen default out. This is the
/// only supported way to opt a call into `QueryTransform::Generative`; there is no second
/// path (§55.1-style "collapse construction to one place" discipline extended to profile
/// selection).
pub fn resolve_profile(explicit: Option<RetrievalProfile>) -> RetrievalProfile {
    explicit.unwrap_or_else(RetrievalProfile::default_profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_explicit_profile_falls_back_to_deterministic_default() {
        let resolved = resolve_profile(None);
        assert_eq!(resolved.query_transform, QueryTransform::Deterministic);
        assert!(resolved.production_enabled);
        assert_eq!(resolved.profile_id, "default_deterministic");
    }

    #[test]
    fn explicit_profile_is_honored_verbatim() {
        let generative = RetrievalProfile {
            profile_id: "generative_v1",
            query_transform: QueryTransform::Generative { version: "v1" },
            production_enabled: true,
        };
        let resolved = resolve_profile(Some(generative.clone()));
        assert_eq!(resolved, generative);
    }
}
