//! `maintenance::tests::rebuild_cli` — the `projection rebuild` arm of `humaux-maintenance` (ADR-0064 D-E): every
//!   required flag is refused when missing (exit 2, named, before anything is read), and a run over a throwaway
//!   database and a scratch Qdrant prints one receipt object per stream.
//! Depends-on: crates=[serde_json, uuid]; services=[PostgreSQL(owner)
//!   w=[control.tenants, projection.embedding_fingerprints, projection.stream_checkpoints,
//!   projection.tenant_placements]];
//!   env=[HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_MAINTENANCE_QDRANT_CIDR, HUMAUX_MAINTENANCE_QDRANT_HOST,
//!   HUMAUX_MAINTENANCE_QDRANT_PORT, HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION, HUMAUX_RETRIEVAL_WORKER_PG_DSN];
//!   modules=[adapters::tests::support::scratch_qdrant, maintenance::tests::support::throwaway]
//! Called-by: [cargo-test]
//! Invariants: [the database is humaux_thread_c37_cli_<pid>_<n> and the Qdrant is humaux-c37-qdrant-<pid>-<n>, both
//!   created and removed by their guards (also on panic); the shared dev database and containers are never touched;
//!   each refusal is asserted by exit code and the flag named on stderr]
//! Spec: Baseline §78.1; §79.2; ADR-0053 D-F; ADR-0064 D-E

#[path = "support/throwaway.rs"]
#[allow(dead_code)]
mod throwaway;

#[path = "../../../crates/adapters/tests/support/scratch_qdrant.rs"]
mod scratch_qdrant;

use serde_json::Value;
use throwaway::{run, with_db};
use uuid::Uuid;

/// CLI (ADR-0064 D-E, §78.1): removing any one required flag of `projection rebuild` exits 2 naming it, before any
/// environment is read; with every flag, two serving streams of a tenant yield two receipt objects, each
/// `equivalent` and closed. Fault: a default for `--batch` → the run proceeds to the environment and stderr no
/// longer names `--batch` → red.
#[test]
fn projection_rebuild_requires_every_flag_and_prints_one_object_per_stream() {
    let tenant = Uuid::now_v7().to_string();
    let full = [
        "projection",
        "rebuild",
        "--tenant",
        &tenant,
        "--batch",
        "10",
        "--wait-seconds",
        "5",
    ];
    for (flag, named) in [
        ("--tenant", "--tenant"),
        ("--batch", "--batch"),
        ("--wait-seconds", "--wait-seconds"),
    ] {
        let args: Vec<&str> = full
            .chunks(2)
            .filter(|pair| pair[0] != flag)
            .flatten()
            .copied()
            .collect();
        let out = run(&args, &[]);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(2), "{args:?}: {stderr}");
        assert!(
            stderr.contains(named),
            "{args:?} must name {named}: {stderr}"
        );
        assert!(out.stdout.is_empty(), "nothing printed before the refusal");
    }

    let test = "projection_rebuild_requires_every_flag_and_prints_one_object_per_stream";
    let Some(mut db) = throwaway::db(test, "c37_cli") else {
        return;
    };
    let qdrant = scratch_qdrant::ScratchQdrant::start("qdrant").expect("scratch Qdrant");
    let worker_dsn = std::env::var("HUMAUX_RETRIEVAL_WORKER_PG_DSN")
        .expect("HUMAUX_RETRIEVAL_WORKER_PG_DSN (§79.2: required with the DB)");
    let label = "c37-cli-embed@r1";
    let tenant = seed(&mut db, label);
    let tenant = tenant.to_string();
    let args = [
        "projection",
        "rebuild",
        "--tenant",
        &tenant,
        "--batch",
        "10",
        "--wait-seconds",
        "5",
    ];
    let out = run(
        &args,
        &[
            ("HUMAUX_MAINTENANCE_PG_DSN", db.maintenance_dsn.clone()),
            (
                "HUMAUX_RETRIEVAL_WORKER_PG_DSN",
                with_db(&worker_dsn, &db.name),
            ),
            (
                "HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION",
                label.to_owned(),
            ),
            ("HUMAUX_MAINTENANCE_QDRANT_HOST", "127.0.0.1".to_owned()),
            ("HUMAUX_MAINTENANCE_QDRANT_PORT", qdrant.port.to_string()),
            ("HUMAUX_MAINTENANCE_QDRANT_CIDR", "127.0.0.1/32".to_owned()),
        ],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{stdout} {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let receipt: Value = serde_json::from_str(stdout.trim()).expect("one JSON receipt");
    let streams = receipt["streams"].as_array().expect("streams");
    assert_eq!(streams.len(), 2, "one object per stream: {receipt}");
    for stream in streams {
        assert_eq!(stream["verdict"], "equivalent", "{stream}");
        assert_eq!(stream["closed"], true, "{stream}");
    }
    drop(qdrant);
}

/// One tenant with a bound label, a placement and two serving (empty) workspace streams.
fn seed(db: &mut throwaway::Db, label: &str) -> Uuid {
    let tenant: Uuid = db
        .client()
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ('c37 cli') RETURNING tenant_id",
            &[],
        )
        .expect("tenant")
        .get(0);
    db.client()
        .execute(
            "INSERT INTO projection.embedding_fingerprints (fingerprint_sha256, embedding_version, provider, \
               model_id, model_revision, dimension, task_type, preprocessing_version, projection_contract_version, \
               dtype, normalization, distance) \
             VALUES (sha256('c37 cli'::bytea), $1, 'c37-test', 'c37-model', 'r1', 4, 'document', 'v1-test', 'v1', \
               'float32', 'provider-raw', 'Cosine')",
            &[&label],
        )
        .expect("label binding");
    db.client()
        .execute(
            "INSERT INTO projection.tenant_placements \
               (tenant_id, projection_family, collection_name, placement_class) \
             VALUES ($1, 'private_memory_v1', $2, 'SHARED_FALLBACK')",
            &[&tenant, &format!("c37_cli_{}", Uuid::new_v4().simple())],
        )
        .expect("placement");
    for _ in 0..2 {
        db.client()
            .execute(
                "INSERT INTO projection.stream_checkpoints (tenant_id, scope_kind, scope_id, domain, \
                   projection_kind, projection_version, issued_highwater, serving) \
                 VALUES ($1, 'workspace', gen_random_uuid(), 'private_memory', 'PRIVATE_MEMORY', 'v1', 0, true)",
                &[&tenant],
            )
            .expect("serving stream");
    }
    tenant
}
