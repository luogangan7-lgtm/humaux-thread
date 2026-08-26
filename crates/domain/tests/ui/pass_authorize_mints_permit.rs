// §7.3 (`pass_authorize_mints_permit`): the one sanctioned path — `egress::authorize` — works
// from outside the crate, proving the fail_* fixtures are red because of the specific
// construction path they try, not because `EgressPermit` is unreachable altogether.
use std::time::Duration;
use uuid::Uuid;

use humaux_domain::dataclass::DataClass;
use humaux_domain::egress::{AuthorizedEgressPayload, ProcessorId, PrivateDataPurpose, authorize};
use humaux_domain::ids::TenantId;

fn main() {
    let payload = AuthorizedEgressPayload::new(b"hello".to_vec());
    let permit = authorize(
        TenantId::new(),
        ProcessorId(Uuid::now_v7()),
        PrivateDataPurpose::UserReasoning,
        DataClass::Private,
        &payload,
        Duration::from_secs(60),
    )
    .expect("UserReasoning + Private must mint");
    assert_eq!(permit.payload_sha256(), payload.sha256());
}
