// §1.2.3/§41.4 (`fail_embed_queries_raw_string`): `EmbeddingProvider::embed_queries` narrows
// its input to `&[SealedRetrievalQuery]` — a plain `Vec<String>` (the raw private content type
// §1.2.3 forbids passing directly to a provider) must not type-check against it.
use humaux_domain::ids::TenantId;
use humaux_retrieval_provider::contract::EmbeddingProvider;

async fn call_with_raw_strings(provider: &dyn EmbeddingProvider) {
    let raw: Vec<String> = vec!["hello".to_string()];
    let _ = provider.embed_queries(TenantId::new(), 256, &raw).await;
}

fn main() {}
