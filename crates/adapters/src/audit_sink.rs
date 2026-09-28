//! `adapters::audit_sink` — §77 Immutable Audit Sink export path.
//! Depends-on: crates=[humaux-domain]; services=[]; env=[]; modules=[domain::audit]
//! Called-by: []
//! Invariants: [pure port: no object-store client ships here yet; the export call site writes through ObjectStore
//!   only, so no audit bytes are placed anywhere until a real WORM adapter implements it]
//! Spec: Baseline §3; §78.3
//!
//! The hash-chain itself (`AuditBatch`, `batch_hash`, `verify_chain`) is pure logic and
//! lives in `humaux_domain::audit` — Domain never touches an object store (§3/§78.3). This
//! module is the one place that will eventually call a real WORM/immutable object store to
//! place exported batch bytes; **no such adapter ships in this task** (tracked for Phase 13
//! — `crates/adapters/src/s3.rs` is the existing sibling placeholder for that client). What
//! exists here now is the port itself, [`ObjectStore`], so the export call site can be
//! written and tested against an in-memory fake today and swapped for the real client later
//! without touching callers.

use humaux_domain::audit::{self, AuditBatch};

/// Where an exported batch's payload bytes land — write-once, external to PostgreSQL (§77
/// "Immutable Audit Sink ... WORM/immutable object storage"). No implementation ships with
/// this task; Phase 13 provides the real S3/WORM adapter. Write-once semantics is the real
/// adapter's responsibility — this port does not itself enforce it.
pub trait ObjectStore {
    /// Failure writing `bytes` at `object_key` (network/auth/store-specific — adapter-defined).
    type Error: std::error::Error;

    /// Writes `bytes` at `object_key`, returning the reference to record as
    /// [`AuditBatch::exported_object`].
    fn put(&self, object_key: &str, bytes: &[u8]) -> Result<String, Self::Error>;
}

/// Builds the next batch in a lineage and writes its payload through `store` (§77 Audit
/// Batch export path). `previous` is `None` only for the very first batch in a lineage
/// (chains from [`humaux_domain::audit::GENESIS_BATCH_HASH`]).
pub fn export_batch<S: ObjectStore>(
    store: &S,
    previous: Option<&AuditBatch>,
    seq_start: i64,
    seq_end: i64,
    payload: &[u8],
    object_key: &str,
    created_at: std::time::SystemTime,
) -> Result<AuditBatch, S::Error> {
    let previous_batch_hash = previous
        .map(audit::batch_hash)
        .unwrap_or(audit::GENESIS_BATCH_HASH);
    let exported_object = store.put(object_key, payload)?;
    Ok(AuditBatch {
        seq_start,
        seq_end,
        previous_batch_hash,
        payload_hash: audit::payload_hash(payload),
        exported_object,
        created_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::fmt;

    #[derive(Debug)]
    struct FakeStoreError;
    impl fmt::Display for FakeStoreError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "fake store error")
        }
    }
    impl std::error::Error for FakeStoreError {}

    /// In-memory `ObjectStore` fake — the one this module's own doc promises callers can
    /// test the export call site against before Phase 13's real client exists.
    #[derive(Default)]
    struct InMemoryObjectStore {
        objects: RefCell<Vec<(String, Vec<u8>)>>,
    }

    impl ObjectStore for InMemoryObjectStore {
        type Error = FakeStoreError;

        fn put(&self, object_key: &str, bytes: &[u8]) -> Result<String, Self::Error> {
            self.objects
                .borrow_mut()
                .push((object_key.to_string(), bytes.to_vec()));
            Ok(format!("memory://{object_key}"))
        }
    }

    #[test]
    fn export_batch_chains_from_genesis_when_no_previous() {
        let store = InMemoryObjectStore::default();
        let batch = export_batch(
            &store,
            None,
            1,
            100,
            b"payload-1",
            "batch-1",
            std::time::SystemTime::UNIX_EPOCH,
        )
        .expect("fake store never fails");
        assert_eq!(batch.previous_batch_hash, audit::GENESIS_BATCH_HASH);
        assert_eq!(batch.exported_object, "memory://batch-1");
        assert_eq!(batch.payload_hash, audit::payload_hash(b"payload-1"));
    }

    #[test]
    fn export_batch_chains_from_previous_batch_hash() {
        let store = InMemoryObjectStore::default();
        let first = export_batch(
            &store,
            None,
            1,
            100,
            b"payload-1",
            "batch-1",
            std::time::SystemTime::UNIX_EPOCH,
        )
        .unwrap();
        let second = export_batch(
            &store,
            Some(&first),
            101,
            200,
            b"payload-2",
            "batch-2",
            std::time::SystemTime::UNIX_EPOCH,
        )
        .unwrap();
        assert_eq!(second.previous_batch_hash, audit::batch_hash(&first));
        assert!(audit::verify_chain(&[first, second]).is_ok());
    }
}
