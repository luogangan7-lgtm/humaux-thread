// Query provenance is derived inside the typed reserve path from a sealed request. Callers
// cannot construct a context carrying free profile/classifier/digest/byte metadata.
use humaux_retrieval_provider::contract::RetrievalQueryCallContext;

fn main() {
    let _ = RetrievalQueryCallContext {
        profile_fingerprint: "caller-profile",
        classifier_revision: "caller-classifier",
        query_sha256: [0_u8; 32],
        query_bytes: 7,
    };
}
