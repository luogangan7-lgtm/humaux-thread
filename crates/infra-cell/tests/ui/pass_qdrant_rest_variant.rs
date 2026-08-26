// §83.4 判据1 (`pass_qdrant_rest_variant`): the one sanctioned way to name an
// `IntraCellResource` — the enum variant itself — works fine from outside the crate, proving
// `fail_resource_from_raw_url_string` is red because of the specific (URL-shaped) construction
// it tries, not because the type is unreachable altogether.
use humaux_infra_cell::IntraCellResource;

fn main() {
    let resource = IntraCellResource::QDRANT_REST;
    assert_eq!(resource, IntraCellResource::QDRANT_REST);
}
