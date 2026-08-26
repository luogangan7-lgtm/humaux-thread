// §11.8 (T4.6/T4.7): "Pinned/Mandatory ID 无法构造成 AutoMutableMemoryId" — a `BoundMemoryId`
// (the `classify` branch standing in for an active MANDATORY/PINNED `context_bindings` row,
// see `domain::consolidate`'s module doc for why the two collapse into one type today) has no
// `From`/`Into` path to `AutoMutableMemoryId` anywhere in the crate. This must fail to
// *compile*, not fail an assertion at runtime.
use humaux_domain::authority::MemoryId;
use humaux_domain::consolidate::{classify, AutoMutableMemoryId, ClassifiedMemoryId};

fn main() {
    let id = MemoryId::new();
    let bound = match classify(id, true) {
        ClassifiedMemoryId::Bound(b) => b,
        ClassifiedMemoryId::Unbound(_) => unreachable!(),
    };
    let _auto: AutoMutableMemoryId = bound.into();
}
