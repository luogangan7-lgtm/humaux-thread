// §7.3 (`fail_egress_permit_issue_call`): `EgressPermit::issue` is module-private (not even
// `pub(crate)`) — from outside `humaux_domain`, it is invisible, not merely access-denied.
use humaux_domain::egress::EgressPermit;

fn main() {
    let _permit = EgressPermit::issue();
}
