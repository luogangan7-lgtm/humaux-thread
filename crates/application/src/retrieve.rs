//! `application::retrieve` — pure handoff from bootstrap-resolved config to the sole request constructor.
//! Depends-on: crates=[humaux-retrieval]; services=[];
//!   env=[]; modules=[retrieval::request]
//! Called-by: [gateway::recall]
//! Invariants: []
//! Spec: none

pub use humaux_retrieval::request::{
    QueryTransform, RegisteredRetrievalProfile, RequestBuildError, RetrievalIntent,
    RetrievalRequest,
};

/// Application seam for future authenticated lanes. It has no provider or database dependency:
/// those callers supply already-resolved planner inputs and must use the same request builder.
pub fn prepare_request(
    intent: RetrievalIntent,
    profile: &RegisteredRetrievalProfile,
) -> Result<RetrievalRequest, RequestBuildError> {
    humaux_retrieval::request::build_request(intent, profile)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    fn profile() -> RegisteredRetrievalProfile {
        humaux_retrieval::request::resolve_registered_retrieval_profile(&BTreeMap::new())
            .expect("registered defaults")
    }

    #[test]
    fn application_seam_forwards_a_real_intent_to_the_unique_constructor() {
        let intent = RetrievalIntent::new(
            "find \"frozen contract\"".to_string(),
            vec![],
            BTreeSet::new(),
            BTreeSet::new(),
        )
        .expect("intent");
        let profile = profile();
        let request = prepare_request(intent, &profile).expect("request");
        assert_eq!(request.query_transform(), QueryTransform::Deterministic);
        assert_eq!(request.cand_k(), 25);
    }

    #[test]
    fn application_cannot_change_registered_profile_depth() {
        let profile = profile();
        assert_eq!(profile.top_k(), 5);
        assert_eq!(profile.query_transform(), QueryTransform::Deterministic);
    }
}
