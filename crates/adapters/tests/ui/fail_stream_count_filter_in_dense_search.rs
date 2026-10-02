// ADR-0057 D-C (§17.1): the count-only `StreamCountFilter` (no visibility disjunction) must never
// reach a search / scroll filter — no conversion into `DenseQueryFilter` exists, so handing one
// to the dense filter serializer must not type-check.
use humaux_adapters::qdrant::condition_to_filter;
use humaux_domain::ids::{TenantId, WorkspaceId};
use humaux_projection::dense::{DenseQueryFilter, build_stream_count_filter};

fn main() {
    let count_only = build_stream_count_filter(TenantId::new(), WorkspaceId::new(), "v1").unwrap();
    let search: DenseQueryFilter = count_only.into();
    let _wire = condition_to_filter(&search);
}
