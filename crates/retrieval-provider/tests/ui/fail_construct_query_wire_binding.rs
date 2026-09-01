// The reserve capability binds the sealed query slice to the exact serialized payload. Callers
// may mint the sanctioned binding but cannot pair sealed query A with an unrelated payload B.
use humaux_adapters::retrieval_query_source::SerializedRetrievalQueryBatch;
use humaux_domain::egress::AuthorizedEgressPayload;
use humaux_local_secret_scan::SealedRetrievalQuery;

fn main() {
    let queries: &[SealedRetrievalQuery] = &[];
    let _ = SerializedRetrievalQueryBatch {
        queries,
        payload: AuthorizedEgressPayload::new(b"unrelated wire".to_vec()),
    };
}
