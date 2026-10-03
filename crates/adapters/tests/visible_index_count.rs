//! `adapters::tests::visible_index_count` — Card 18 / §23.1② — `retrieve::visible_index_count`, the one producer of
//!   the live Qdrant `visible` number the three read routes hand to `envelope::build_projection_block`.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-infra-cell, humaux-projection, humaux-retrieval,
//!   humaux-telemetry, humaux-testkit, postgres, serde_json, sqlx, tokio, uuid]; services=[PostgreSQL(owner)
//!   r=[projection.processing_gaps] w=[control.tenants, projection.stream_checkpoints, projection.stream_log],
//!   PostgreSQL(role_gateway), PostgreSQL(role_maintenance), Qdrant(*)]; env=[HUMAUX_TEST_PG_DSN,
//!   HUMAUX_TEST_QDRANT_PORT]; modules=[adapters::forget_repo, adapters::postgres, adapters::qdrant,
//!   adapters::retrieve, domain::authority, domain::dataclass, domain::identity, domain::ids, domain::memory,
//!   humaux-testkit, infra-cell::permit, infra-cell::resource, infra-cell::transport, projection::card,
//!   projection::stream, retrieval::completeness, retrieval::envelope, telemetry::degrade]
//! Called-by: [cargo-test]
//! Invariants: [PostgreSQL unreachable or role denied -> the call fails and surfaces the error to the caller; no silent fallback; Qdrant unreachable -> QdrantTransportError to the caller, no fallback search]
//! Spec: Baseline §4.4; §15.2; §16.2; ADR-0031
//!
//! Against a **real** Postgres ledger and a **real** Qdrant index (both required; three-state
//! skip, §79.2/§57.1, prints which object is missing rather than passing silently). The
//! function under test is the production one, unmodified — only the fixture IO is test-local,
//! same discipline as `recall_envelope_g23.rs` (whose fixture shape this file deliberately
//! mirrors so the two can be read side by side).
//!
//! Six legs, each of which fails for a different reason if the wiring regresses:
//! 1. **healthy** — 100 `DONE`, 100 indexed: `visible = 100`, ratio `1.0`, `current`, no
//!    degradation. Before this card every read route passed `None` here and the envelope said
//!    `cannot_establish` / `index_count_unavailable` on a perfectly healthy projection.
//! 2. **legal tombstone before physical purge** — 10 real `TOMBSTONED` writes
//!    (`forget_repo::tombstone`, §37 step 1) with the 10 points still in the index (§37 step 5
//!    not run, and on this deployment not wired at all): `visible = 90`, A2 closed, ratio still
//!    `1.0`. This is the leg the tombstone overlay exists for.
//! 3. **fault injection / reverse falsification** — the identical filter counted through the
//!    raw `qdrant::count` (i.e. the overlay dropped, which is what "return a raw Qdrant count
//!    without subtracting the tombstone overlay" means at the call site) reads 100, and
//!    `build_projection_block` then produces `completeness_ratio: None` instead of `1.0`. Leg 2's
//!    assertion is therefore observably falsifiable, not decorative.
//! 4. **A2 InvisibleLoss** — 5 points deleted straight out of Qdrant with no PG write at all:
//!    ratio drops below `1.0` (and is **not** null), `current` clears, and
//!    `PROJECTION_INVISIBLE_LOSS` fires.
//! 5. **no serving version** — `None` in, `None` out. Explicitly asserted to not be `Some(0)`
//!    (§16.2/§57.1/§4.4 坑5: with no serving version the ratio has no denominator, which is
//!    `cannot_establish`, not "the index is empty").
//! 6. **serving version != the ledger key's version** — the mid-switch shape. `None`, because
//!    A2 compares `visible` with a `done` read at `key.projection_version`; counting the other
//!    face would fabricate a loss on a healthy projection. Paired with a control call at the
//!    matching version that still returns a number.
//!
//! Plus the card's speed reading: p50/p95 of the count call itself over n = 40.

use std::collections::{BTreeMap, BTreeSet};
use std::net::TcpStream;
use std::time::{Duration, Instant, SystemTime};

use humaux_adapters::forget_repo;
use humaux_adapters::postgres::{MaintenanceDbPool, RuntimeDbPool};
use humaux_adapters::qdrant::{
    Distance, IndexablePayload, PointId, QdrantOperation, QdrantPointPayload, ShardingMethod,
    VisibleCountFilter, count, create_collection_body, ha_profile_for, tenant_index_body, upsert,
};
use humaux_adapters::retrieve::{IndexFace, visible_index_count};
use humaux_domain::authority::{AuthorityClass, AuthorityStatus};
use humaux_domain::dataclass::DataClass;
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId, VisibilityClass};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_domain::memory::MemoryType;
use humaux_infra_cell::{
    CallerId, CellId, HttpIntraCellTransport, IntraCellHttpTransport, IntraCellMethod,
    IntraCellRequest, IntraCellResource, IntraCellResourceRegistry, ResourceEntry,
    authorize_cell_access,
};
use humaux_projection::card::EgressDisposition;
use humaux_projection::stream::StreamKey;
use humaux_retrieval::completeness::LedgerClosure;
use humaux_retrieval::completeness::ledger::{self, LedgerReads};
use humaux_retrieval::envelope::build_projection_block;
use humaux_telemetry::degrade::DegradeCode;
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

const NAME: &str = "visible_index_count_is_the_live_denominator_input";
const VERSION: &str = "c18";
const TOTAL: i64 = 100;
const TOMBSTONED: i64 = 10;
const RAW_DELETED: i64 = 5;
const SPEED_SAMPLES: usize = 40;

const DEFAULT_QDRANT_PORT: u16 = 6333;

fn qdrant_port() -> u16 {
    std::env::var("HUMAUX_TEST_QDRANT_PORT")
        .map(|raw| {
            let port: u16 = raw.parse().expect("HUMAUX_TEST_QDRANT_PORT must be a u16");
            assert_ne!(port, 0, "HUMAUX_TEST_QDRANT_PORT must be nonzero");
            port
        })
        .unwrap_or(DEFAULT_QDRANT_PORT)
}

fn qdrant_addr() -> String {
    format!("127.0.0.1:{}", qdrant_port())
}

/// Same two-dependency three-state skip as `recall_envelope_g23.rs`: each missing object is
/// declared by name through `testkit::skip_or_fail`, never by a silent `return`.
fn skip_unless_both_reachable() -> Option<(String, Client)> {
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(
            NAME,
            "missing object: Postgres DSN (HUMAUX_TEST_PG_DSN not set)",
            ExternalDep::Postgres,
        );
        return None;
    };
    // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
    let admin = match Client::connect(&dsn, NoTls) {
        Ok(c) => c,
        Err(e) => {
            skip_or_fail(
                NAME,
                &format!("missing object: live Postgres (connect failed: {e})"),
                ExternalDep::Postgres,
            );
            return None;
        }
    };
    // dep: Qdrant(*) — reachability probe before running the Qdrant-dependent test
    if TcpStream::connect_timeout(&qdrant_addr().parse().unwrap(), Duration::from_millis(500))
        .is_err()
    {
        skip_or_fail(
            NAME,
            &format!("missing object: live Qdrant server at {}", qdrant_addr()),
            ExternalDep::Qdrant,
        );
        return None;
    }
    Some((dsn, admin))
}

fn dsn_as_role(dsn: &str, role: &str) -> String {
    // ADR-0059 D-D: a real login as `role`, its password from HUMAUX_ROLE_PASSWORD_<SUFFIX>.
    humaux_testkit::role_login_dsn(dsn, role, |name| std::env::var(name).ok())
        .unwrap_or_else(|missing| panic!("missing object: {missing} (ADR-0059 D-D)"))
}

fn key(tenant_id: Uuid, scope_id: Uuid) -> StreamKey {
    StreamKey::new(
        TenantId(tenant_id),
        "workspace",
        scope_id,
        "code",
        "retrieval_card",
        VERSION,
    )
}

fn seed_log_row(admin: &mut Client, k: &StreamKey, seq: i64, state: &str) {
    let now = SystemTime::now();
    let settled_at: Option<SystemTime> = matches!(state, "DONE").then_some(now);
    admin
        .execute(
            "INSERT INTO projection.stream_log \
               (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                stream_seq, commit_seq, state, issued_at, settled_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$7,$8,$9,$10)",
            &[
                &k.tenant_id.0,
                &k.scope_kind,
                &k.scope_id,
                &k.domain,
                &k.projection_kind,
                &k.projection_version,
                &seq,
                &state,
                &now,
                &settled_at,
            ],
        )
        .expect("seed stream_log row");
}

/// The §23.1② precondition every leg starts from: `TOTAL` rows, all `DONE`, serving.
fn seed_serving_stream(admin: &mut Client, k: &StreamKey) {
    for seq in 1..=TOTAL {
        seed_log_row(admin, k, seq, "DONE");
    }
    admin
        .execute(
            "INSERT INTO projection.stream_checkpoints \
               (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                issued_highwater, serving) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,true)",
            &[
                &k.tenant_id.0,
                &k.scope_kind,
                &k.scope_id,
                &k.domain,
                &k.projection_kind,
                &k.projection_version,
                &TOTAL,
            ],
        )
        .expect("seed serving stream_checkpoints row");
}

/// The six ledger numbers, read as plain SQL so the judgment under test stays exactly
/// `humaux_retrieval::completeness::ledger::close`.
fn read_ledger(admin: &mut Client, k: &StreamKey) -> LedgerClosure {
    let expected: i64 = admin
        .query_one(
            "SELECT issued_highwater FROM projection.stream_checkpoints \
             WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
               AND projection_kind=$5 AND projection_version=$6",
            &[
                &k.tenant_id.0,
                &k.scope_kind,
                &k.scope_id,
                &k.domain,
                &k.projection_kind,
                &k.projection_version,
            ],
        )
        .expect("checkpoint row")
        .get(0);
    let row = admin
        .query_one(
            "SELECT \
               count(*) FILTER (WHERE state IN ('DONE','SKIPPED_BY_POLICY','TOMBSTONED')) AS done, \
               count(*) FILTER (WHERE state = 'TOMBSTONED') AS deleted, \
               count(*) FILTER (WHERE state = 'SKIPPED_BY_POLICY') AS skipped, \
               count(*) FILTER (WHERE state IN ('ISSUED','PROCESSING','WAITING_KEY','RETRY_WAIT')) AS pending \
             FROM projection.stream_log \
             WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
               AND projection_kind=$5 AND projection_version=$6",
            &[&k.tenant_id.0, &k.scope_kind, &k.scope_id, &k.domain, &k.projection_kind, &k.projection_version],
        )
        .expect("stream_log aggregate");
    let open_gaps: i64 = admin
        .query_one(
            "SELECT count(*) FROM projection.processing_gaps \
             WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
               AND projection_kind=$5 AND projection_version=$6",
            &[
                &k.tenant_id.0,
                &k.scope_kind,
                &k.scope_id,
                &k.domain,
                &k.projection_kind,
                &k.projection_version,
            ],
        )
        .expect("processing_gaps count")
        .get(0);
    let reads = LedgerReads {
        expected: expected as u64,
        done: row.get::<_, i64>(0) as u64,
        deleted: row.get::<_, i64>(1) as u64,
        skipped: row.get::<_, i64>(2) as u64,
        open_gaps: open_gaps as u64,
        pending: row.get::<_, i64>(3) as u64,
    };
    // ADR-0057 D-A: A2 compares points with points. This fixture writes one synthetic point per
    // ticket and no memory rows, so its point reading is the ticket terms one-for-one (the
    // definer itself is exercised against real memories in `a2_point_identity.rs`).
    ledger::close(
        reads,
        ledger::ProjectionReads {
            points_expected: reads.expected - reads.deleted,
            points_settled: reads.done - reads.deleted - reads.skipped,
            points_in_flight: reads.pending,
            points_unsettled: 0,
            oldest_pending_age_secs: None,
        },
    )
}

fn cleanup(admin: &mut Client, tenant_id: Uuid) {
    let _ = admin.execute(
        "DELETE FROM projection.stream_log WHERE tenant_id=$1",
        &[&tenant_id],
    );
    let _ = admin.execute(
        "DELETE FROM projection.stream_checkpoints WHERE tenant_id=$1",
        &[&tenant_id],
    );
    let _ = admin.execute(
        "DELETE FROM control.tenants WHERE tenant_id=$1",
        &[&tenant_id],
    );
}

fn registry(cell: CellId, caller: CallerId) -> IntraCellResourceRegistry {
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            "127.0.0.1",
            qdrant_port(),
            cell,
            vec!["127.0.0.1/32".parse().unwrap()],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .expect("127.0.0.1/32 is a reserved/loopback CIDR"),
    );
    IntraCellResourceRegistry::new(entries, cell, caller)
}

fn payload(
    tenant_id: TenantId,
    workspace_id: WorkspaceId,
    user_id: UserId,
    seq: i64,
) -> QdrantPointPayload {
    QdrantPointPayload {
        tenant_id,
        workspace_id,
        visibility_class: VisibilityClass::UserPrivate,
        visibility_user_id: Some(user_id),
        visibility_workspace_id: None,
        object_type: "memory_record".to_string(),
        memory_type: MemoryType::Fact,
        status: AuthorityStatus::Active,
        authority: AuthorityClass::PrivateKnowledge,
        created_at: OffsetDateTime::now_utc(),
        effective_at: OffsetDateTime::now_utc(),
        embedding_version: "embed-v1".to_string(),
        projection_version: VERSION.to_string(),
        // The overlay key under test: the seq this point was projected from (§37).
        source_stream_seq: seq,
        data_class: DataClass::Private,
        egress_disposition: EgressDisposition::Forbidden,
    }
}

fn point_id_json(id: PointId) -> serde_json::Value {
    match id {
        PointId::Num(n) => serde_json::json!(n),
        PointId::Uuid(u) => serde_json::json!(u.to_string()),
    }
}

fn percentile(sorted: &[Duration], p: f64) -> u128 {
    let idx = (((sorted.len() - 1) as f64) * p).round() as usize;
    sorted[idx].as_micros()
}

// `#[test]` + a hand-driven `Runtime` rather than `#[tokio::test]`: the blocking
// `postgres::Client` used for every ledger read panics ("Cannot start a runtime from within a
// runtime") if it runs while a `#[tokio::test]` runtime is active on the thread. Same shape and
// same reason as `recall_envelope_g23.rs`.
#[test]
#[allow(clippy::too_many_lines)] // One causal chain: seed -> healthy -> tombstone -> fault -> loss.
fn visible_index_count_is_the_live_denominator_input() {
    let Some((dsn, mut admin)) = skip_unless_both_reachable() else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
            &[&"card 18 visible index count"],
        )
        .expect("insert tenant")
        .get(0);
    let workspace_id = WorkspaceId::new();
    let user_id = UserId::new();
    let k = key(tenant_id, workspace_id.0);
    seed_serving_stream(&mut admin, &k);

    let cell = CellId(Uuid::now_v7());
    let caller = CallerId(format!("card18-{}", Uuid::now_v7()));
    let cell_registry = registry(cell, caller);
    let transport = HttpIntraCellTransport::new(
        cell_registry.clone(),
        Duration::from_secs(10),
        humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
    )
    .expect("intra-cell transport");
    let permit = authorize_cell_access(
        &cell_registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(300),
    )
    .expect("same-cell, allowlisted caller must mint a permit");
    let collection = format!("card18_{}", Uuid::now_v7().simple());
    let scope = AuthorizationScope::new(
        TenantId(tenant_id),
        PrincipalId::new(),
        Some(user_id),
        BoundedSet::new(vec![workspace_id]).expect("one workspace is a valid bounded set"),
    );

    let ids: Vec<PointId> = (1..=TOTAL).map(|_| PointId::Uuid(Uuid::now_v7())).collect();
    let pool = rt.block_on(async {
        transport
            .execute(
                &permit,
                // dep: Qdrant(*) — Qdrant REST call for fixture setup/assertion
                IntraCellRequest {
                    method: IntraCellMethod::Put,
                    path: format!("/collections/{collection}"),
                    json_body: Some(create_collection_body(
                        4,
                        Distance::Cosine,
                        1,
                        1,
                        1,
                        ShardingMethod::Auto,
                    )),
                    headers: Vec::new(),
                },
            )
            .await
            .expect("create collection");
        transport
            .execute(
                &permit,
                // dep: Qdrant(*) — Qdrant REST call for fixture setup/assertion
                IntraCellRequest {
                    method: IntraCellMethod::Put,
                    path: format!("/collections/{collection}/index"),
                    json_body: Some(tenant_index_body()),
                    headers: Vec::new(),
                },
            )
            .await
            .expect("tenant index");
        for (i, id) in ids.iter().enumerate() {
            let seq = i as i64 + 1;
            let indexable: IndexablePayload =
                payload(TenantId(tenant_id), workspace_id, user_id, seq)
                    .into_indexable()
                    .expect("a Private, non-secret payload is indexable");
            upsert(
                &transport,
                &permit,
                &collection,
                &[(*id, &indexable, vec![0.1, 0.2, 0.3, 0.4])],
                ha_profile_for(QdrantOperation::NormalImmutableUpsert),
            )
            .await
            .expect("upsert");
        }
        // dep: PostgreSQL(role_gateway) — test opens a direct PG connection for setup/verification
        RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway"))
            .await
            .expect("role_gateway runtime pool")
    });

    fn face<'a>(
        t: &'a HttpIntraCellTransport,
        permit: &'a humaux_infra_cell::CellAccessPermit,
        collection: &'a str,
    ) -> IndexFace<'a> {
        IndexFace {
            transport: t as &dyn IntraCellHttpTransport,
            permit,
            collection,
        }
    }

    // ---- Leg 1: healthy. ------------------------------------------------------------------
    let closure = read_ledger(&mut admin, &k);
    assert!(closure.is_closed(), "A1 must hold on the seeded fixture");
    let healthy = rt.block_on(async {
        // Qdrant indexing is asynchronous: retry until the whole seeded set is countable, so a
        // slow index does not read as a completeness fault.
        for _ in 0..40 {
            let n = visible_index_count(
                &pool,
                &scope,
                face(&transport, &permit, &collection),
                &k,
                Some(VERSION),
                &closure,
            )
            .await;
            if n == Some(TOTAL as u64) {
                return n;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        visible_index_count(
            &pool,
            &scope,
            face(&transport, &permit, &collection),
            &k,
            Some(VERSION),
            &closure,
        )
        .await
    });
    assert_eq!(
        healthy,
        Some(TOTAL as u64),
        "a healthy serving projection must report a real count, not None"
    );
    let out = build_projection_block(&closure, healthy, std::time::Duration::from_secs(60));
    assert_eq!(out.value.visible, Some(TOTAL as u64));
    assert_eq!(
        out.value.completeness_ratio,
        Some(1.0),
        "this is the number every read route reported as null before card 18"
    );
    assert!(out.value.current);
    assert!(out.degradations.is_empty());

    // ---- Leg 5 (taken here, it needs no further state): no serving version. ----------------
    let no_serving = rt.block_on(visible_index_count(
        &pool,
        &scope,
        face(&transport, &permit, &collection),
        &k,
        None,
        &closure,
    ));
    assert_eq!(
        no_serving, None,
        "§16.2/§57.1: no serving version means no denominator — cannot_establish"
    );
    assert_ne!(
        no_serving,
        Some(0),
        "§4.4 坑5: 0 would claim the index is empty, which is a different (false) statement"
    );
    assert!(
        build_projection_block(&closure, no_serving, std::time::Duration::from_secs(60))
            .value
            .completeness_ratio
            .is_none(),
        "and it must reach the envelope as cannot_establish, not as a 0.0 ratio"
    );
    assert!(
        VisibleCountFilter::family_probe(&scope, workspace_id, "").is_none(),
        "an empty version string must not stand in for \"no version filter\" either"
    );

    // ---- Leg 6: serving version != the version the ledger was closed at. -------------------
    // `ledger` here describes `k.projection_version`. A count taken against any OTHER face is
    // not comparable with it: A2 would subtract one version's `done` from another version's
    // `visible` and report a fabricated PROJECTION_INVISIBLE_LOSS (or an A2 overshoot) on a
    // healthy projection. This is the real mid-switch shape on `memory.*` / `context.assemble`,
    // where the ledger key carries the process-configured version (ADR-0031 Q9) while the
    // serving row is read separately. The only honest answer is "no measurement".
    let cross_version = rt.block_on(visible_index_count(
        &pool,
        &scope,
        face(&transport, &permit, &collection),
        &k,
        Some("v-not-the-ledger-version"),
        &closure,
    ));
    assert_eq!(
        cross_version, None,
        "a serving version other than the ledger key's is not a comparable denominator"
    );
    // Falsifiable: the same call with the matching string does return a number, so leg 6 is
    // about the version mismatch and not about some unrelated failure on this fixture.
    assert_eq!(
        rt.block_on(visible_index_count(
            &pool,
            &scope,
            face(&transport, &permit, &collection),
            &k,
            Some(VERSION),
            &closure,
        )),
        Some(TOTAL as u64),
        "control: the same call at the ledger's own version still counts"
    );

    // ---- Leg 2: §37 step 1 (tombstone) with step 5 (physical purge) not run. ---------------
    let maintenance = rt.block_on(async {
        // dep: PostgreSQL(role_maintenance) — test opens a direct PG connection for setup/verification
        let maintenance = MaintenanceDbPool::connect(&dsn_as_role(&dsn, "role_maintenance"))
            .await
            .expect("maintenance pool");
        for seq in 1..=TOMBSTONED as u64 {
            assert!(
                forget_repo::tombstone(&maintenance, &k, seq)
                    .await
                    .expect("tombstone must not error"),
                "seq {seq} must transition to TOMBSTONED exactly once"
            );
        }
        maintenance
    });
    drop(maintenance);
    let after_tombstone = read_ledger(&mut admin, &k);
    assert_eq!(after_tombstone.counts().deleted(), TOMBSTONED as u64);
    assert_eq!(
        after_tombstone.counts().done(),
        TOTAL as u64,
        "TOMBSTONED still counts inside the §15.2 SETTLED_OK union"
    );
    let overlaid = rt
        .block_on(visible_index_count(
            &pool,
            &scope,
            face(&transport, &permit, &collection),
            &k,
            Some(VERSION),
            &after_tombstone,
        ))
        .expect("count must succeed");
    assert_eq!(
        overlaid,
        (TOTAL - TOMBSTONED) as u64,
        "the overlay must exclude the 10 tombstoned points that are still physically indexed"
    );
    let out = build_projection_block(
        &after_tombstone,
        Some(overlaid),
        std::time::Duration::from_secs(60),
    );
    assert_eq!(out.value.completeness_ratio, Some(1.0));
    assert!(out.value.current);
    assert!(
        out.degradations.is_empty(),
        "a legal deletion is not an invisible loss"
    );

    // ---- Leg 3: fault injection — same filter, overlay dropped. ----------------------------
    // This is the card's named fault ("return a raw Qdrant count without subtracting the
    // tombstone overlay"): the only difference from leg 2 is `count` instead of
    // `count_visible_excluding_seqs`, and it must break leg 2's ratio assertion.
    let filter =
        VisibleCountFilter::family_probe(&scope, workspace_id, VERSION).expect("non-empty version");
    let raw = rt
        .block_on(count(&transport, &permit, &collection, &filter))
        .expect("raw count");
    assert_eq!(
        raw, TOTAL as u64,
        "without the overlay the tombstoned points are still counted"
    );
    let faulted = build_projection_block(
        &after_tombstone,
        Some(raw),
        std::time::Duration::from_secs(60),
    );
    assert_ne!(
        faulted.value.completeness_ratio,
        Some(1.0),
        "the overlay assertion in leg 2 must be falsifiable, not decorative"
    );
    assert_eq!(
        faulted.value.completeness_ratio, None,
        "A2's `>` side overshoots `pending` by exactly `deleted` ⇒ cannot_establish"
    );

    // ---- Leg 4: A2 InvisibleLoss — points gone with no PG write at all. --------------------
    let doomed: Vec<serde_json::Value> = ids
        [(TOMBSTONED as usize)..(TOMBSTONED + RAW_DELETED) as usize]
        .iter()
        .map(|id| point_id_json(*id))
        .collect();
    rt.block_on(transport.execute(
        &permit,
        // dep: Qdrant(*) — Qdrant REST call for fixture setup/assertion
        IntraCellRequest {
            method: IntraCellMethod::Post,
            path: format!("/collections/{collection}/points/delete?wait=true"),
            json_body: Some(serde_json::json!({ "points": doomed })),
            headers: Vec::new(),
        },
    ))
    .expect("raw delete must not fail transport-side");
    let lossy = rt
        .block_on(visible_index_count(
            &pool,
            &scope,
            face(&transport, &permit, &collection),
            &k,
            Some(VERSION),
            &after_tombstone,
        ))
        .expect("count must still succeed");
    assert_eq!(lossy, (TOTAL - TOMBSTONED - RAW_DELETED) as u64);
    let out = build_projection_block(
        &after_tombstone,
        Some(lossy),
        std::time::Duration::from_secs(60),
    );
    let ratio = out
        .value
        .completeness_ratio
        .expect("an invisible loss is measured, never null");
    assert!(
        ratio < 1.0,
        "PROJECTION_INVISIBLE_LOSS must come with a real ratio below 1.0, got {ratio}"
    );
    assert!(!out.value.current);
    assert_eq!(
        out.degradations.as_slice(),
        &[DegradeCode::ProjectionInvisibleLoss]
    );

    // ---- Speed (card acceptance goal): the count call itself. ------------------------------
    let mut samples = Vec::with_capacity(SPEED_SAMPLES);
    for _ in 0..SPEED_SAMPLES {
        let started = Instant::now();
        let seen = rt.block_on(visible_index_count(
            &pool,
            &scope,
            face(&transport, &permit, &collection),
            &k,
            Some(VERSION),
            &after_tombstone,
        ));
        samples.push(started.elapsed());
        assert!(seen.is_some());
    }
    samples.sort_unstable();
    eprintln!(
        "{NAME}: visible_index_count over {TOTAL} points (deleted={TOMBSTONED}, so one extra PG \
         read + one exact Qdrant count per call): p50 = {} us, p95 = {} us, n = {SPEED_SAMPLES}",
        percentile(&samples, 0.50),
        percentile(&samples, 0.95),
    );

    rt.block_on(async {
        let _ = transport
            .execute(
                &permit,
                // dep: Qdrant(*) — Qdrant REST call for fixture setup/assertion
                IntraCellRequest {
                    method: IntraCellMethod::Delete,
                    path: format!("/collections/{collection}"),
                    json_body: None,
                    headers: Vec::new(),
                },
            )
            .await;
    });
    cleanup(&mut admin, tenant_id);
}
