// §11.8 assertion: `UnboundMemoryId` (the `classify` branch for "no active context binding")
// converts to `AutoMutableMemoryId` via `From`.
use humaux_domain::authority::MemoryId;
use humaux_domain::consolidate::{classify, AutoMutableMemoryId, ClassifiedMemoryId};

fn main() {
    let id = MemoryId::new();
    let auto: AutoMutableMemoryId = match classify(id, false) {
        ClassifiedMemoryId::Unbound(u) => u.into(),
        ClassifiedMemoryId::Bound(_) => unreachable!(),
    };
    let _ = auto.into_inner();
}
