// §83.4 判据1 (`fail_resource_from_raw_url_string`): a raw URL literal does not coerce into
// `IntraCellResource` — this fixture proves only that direct-literal shape. It does NOT, by
// itself, prove there is no path at all from a string to `IntraCellResource`: an
// arbitrarily-named associated fn (`from_url(&str) -> Self`, say) is neither `From::from` nor
// `FromStr::parse`, so no fixed-name compile-fail fixture can ever exercise it — that general
// property is `xtask architecture-check`'s G80-3 source scan
// (`g80_3_no_string_to_resource_constructor_fn`, scoped to this enum's own `impl` blocks), not
// this file. The endpoint is resolved entirely inside `IntraCellResourceRegistry`, never
// supplied by the caller.
use humaux_infra_cell::IntraCellResource;

fn main() {
    let _resource: IntraCellResource = "http://internal-service:1337/evil";
}
