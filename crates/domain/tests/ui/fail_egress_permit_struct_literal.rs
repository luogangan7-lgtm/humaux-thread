// §7.3 (`fail_egress_permit_struct_literal`): `EgressPermit`'s fields are all private — no
// code outside `crates/domain/src/egress.rs` can name a struct literal for it, even to fill in
// every field, because none of the field names are visible here to write.
use humaux_domain::egress::EgressPermit;

fn main() {
    let _permit: EgressPermit = EgressPermit {};
}
