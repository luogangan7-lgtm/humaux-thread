// Positive counterpart to `fail_embed_queries_raw_string.rs`: the one sanctioned path —
// `SealedRetrievalQuery::seal` first — compiles fine against the same trait method.
use humaux_domain::ids::TenantId;
use humaux_retrieval_provider::contract::{EmbeddingProvider, SealedRetrievalQuery};

async fn call_with_sealed(provider: &dyn EmbeddingProvider) {
    let sealed = vec![SealedRetrievalQuery::seal("hello")];
    let _ = provider.embed_queries(TenantId::new(), 256, &sealed).await;
}

fn main() {}
