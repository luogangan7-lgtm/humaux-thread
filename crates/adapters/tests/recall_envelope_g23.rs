//! §23.4 G23-2 — "分子取 `visible`" fault injection, against a **real** Postgres ledger and a
//! **real** Qdrant index (both required; three-state skip, §79.2/§57.1, prints which object is
//! missing rather than silently passing).
//!
//! Every ledger read below is independent, hand-rolled SQL against the admin connection —
//! mirroring `stream_repo.rs`'s own `seed_log_row`/`seed_checkpoint`/`log_state` helpers,
//! never the crate's `pub(crate)` pool internals (this file is a separate integration-test
//! crate; only `humaux_adapters`'s public API is visible to it). The judgment itself
//! (`humaux_retrieval::completeness::ledger::close` + `humaux_retrieval::envelope::
//! build_projection_block`) is the real production code path under test — only the IO is
//! test-local.
//!
//! Four cases, one shared fixture shape (100 `DONE`, 0 `TOMBSTONED`, 0 `SKIPPED_BY_POLICY`, 0
//! gaps, 0 pending — "100 条全部提交且 search-visible 已确认", §23.4's own precondition):
//! 1. **Injection 1**: bypass `retention::tombstone`, delete 10 Qdrant points directly.
//! 2. **Injection 2**: only 93 of 100 points ever make it into the index (an adapter that acks
//!    before search-visible confirmation, §17.4).
//! 3. **Reverse falsification**: the old formula `(done-deleted)/(expected-deleted)` must stay
//!    `1.0` on both injections above — proof only the new formula can observe the fault.
//! 4. **Legal deletion contrast**: a real `TOMBSTONED` write (via `forget_repo::tombstone`)
//!    plus a real Qdrant delete of the same 10 points — `completeness_ratio` stays `1.0`,
//!    `current` stays `true`, `PROJECTION_INVISIBLE_LOSS` never fires, and — sampled through
//!    two Qdrant-side scrolls built by the production `qdrant::overlay_filter` (one over an
//!    id-exclusion filter standing in for the dense lane, one over `build_sparse_lane`'s own
//!    filter construction, §17.6: sparse queries the *same* collection/points as dense, just
//!    with different scoring) plus two PostgreSQL reads built by the production
//!    `forget_repo::count_excluding_tombstoned`/`forget_repo::state_of` (standing in for the
//!    EXACT channel, §22.1, and the literal lane) — none of the four ever surfaces the deleted
//!    ids. The boundary-B/boundary-C `visible` counts both go through the production
//!    `qdrant::count_visible`, the *same* call before and after §37 step 5's physical purge —
//!    proving it purge-order-independent rather than asserted so.

use std::collections::{BTreeMap, BTreeSet};
use std::net::TcpStream;
use std::time::{Duration, SystemTime};

use humaux_adapters::forget_repo;
use humaux_adapters::postgres::MaintenanceDbPool;
use humaux_adapters::qdrant::{
    Distance, IndexablePayload, PointId, QdrantOperation, QdrantPointPayload, ShardingMethod,
    VisibleCountFilter, condition_to_filter, count, count_visible, create_collection_body,
    ha_profile_for, overlay_filter, tenant_index_body, upsert,
};
use humaux_domain::authority::{AuthorityClass, AuthorityStatus};
use humaux_domain::dataclass::DataClass;
use humaux_domain::identity::{
    AuthorizationScope, BoundedSet, PrincipalId, VisibilityClass, VisibilityDescriptor,
};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_domain::memory::MemoryType;
use humaux_infra_cell::{
    CallerId, CellId, HttpIntraCellTransport, IntraCellHttpTransport, IntraCellMethod,
    IntraCellRequest, IntraCellResource, IntraCellResourceRegistry, ResourceEntry,
    authorize_cell_access,
};
use humaux_projection::card::EgressDisposition;
use humaux_projection::sparse::{CorpusDocument, build_sparse_lane};
use humaux_projection::stream::StreamKey;
use humaux_retrieval::completeness::ledger::{self, LedgerReads};
use humaux_retrieval::envelope::build_projection_block;
use humaux_telemetry::degrade::DegradeCode;
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

/// `PointId::to_json` is adapters-crate-private; this file is a separate compilation unit
/// (an integration test only sees `humaux_adapters`'s public API) so it needs its own copy
/// of the same two-variant wire mapping to build raw delete/scroll request bodies.
fn point_id_json(id: PointId) -> serde_json::Value {
    match id {
        PointId::Num(n) => serde_json::json!(n),
        PointId::Uuid(u) => serde_json::json!(u.to_string()),
    }
}

const QDRANT_ADDR: &str = "127.0.0.1:6333";

fn qdrant_reachable() -> bool {
    TcpStream::connect_timeout(&QDRANT_ADDR.parse().unwrap(), Duration::from_millis(500)).is_ok()
}

/// Combined three-state skip (§57.1/§79.2): both a live Postgres and a live Qdrant are
/// required. Returns `None` (and has already printed why) when either is missing.
fn skip_unless_both_reachable(test_name: &str) -> Option<(String, Client)> {
    // 跳过与失败的分界不在这里做——`testkit::skip_or_fail` 是全 workspace 唯一判定点
    // （见它的 doc：散落的手写跳过没法统一声明，CI 里 99 个测试就是这样静默跳过的）。
    // 两个依赖各报各的声明变量：只起了 Postgres 的环境不该被 Qdrant 的缺席拖红。
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(
            test_name,
            "missing object: Postgres DSN (HUMAUX_TEST_PG_DSN not set)",
            ExternalDep::Postgres,
        );
        return None;
    };
    let admin = match Client::connect(&dsn, NoTls) {
        Ok(c) => c,
        Err(e) => {
            skip_or_fail(
                test_name,
                &format!("missing object: live Postgres (connect failed: {e})"),
                ExternalDep::Postgres,
            );
            return None;
        }
    };
    if !qdrant_reachable() {
        skip_or_fail(
            test_name,
            &format!("missing object: live Qdrant server at {QDRANT_ADDR}"),
            ExternalDep::Qdrant,
        );
        return None;
    }
    Some((dsn, admin))
}

fn key(tenant_id: Uuid) -> StreamKey {
    StreamKey::new(
        TenantId(tenant_id),
        "workspace",
        Uuid::new_v4(),
        "code",
        "retrieval_card",
        "g23_2",
    )
}

fn seed_checkpoint(admin: &mut Client, k: &StreamKey, issued_highwater: i64) {
    admin
        .execute(
            "INSERT INTO projection.stream_checkpoints \
               (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                issued_highwater) \
             VALUES ($1,$2,$3,$4,$5,$6,$7)",
            &[
                &k.tenant_id.0,
                &k.scope_kind,
                &k.scope_id,
                &k.domain,
                &k.projection_kind,
                &k.projection_version,
                &issued_highwater,
            ],
        )
        .expect("seed stream_checkpoints row");
}

fn seed_log_row(admin: &mut Client, k: &StreamKey, seq: i64, state: &str) {
    let now = SystemTime::now();
    let settled_at: Option<SystemTime> = matches!(
        state,
        "DONE" | "SKIPPED_BY_POLICY" | "FAILED" | "TOMBSTONED"
    )
    .then_some(now);
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

/// §23.4's own precondition for every G23-2 case: 100 rows, all `DONE`, none `TOMBSTONED`/
/// `SKIPPED_BY_POLICY`, no gaps, no pending.
fn seed_100_done(admin: &mut Client, k: &StreamKey) {
    for seq in 1..=100i64 {
        seed_log_row(admin, k, seq, "DONE");
    }
    seed_checkpoint(admin, k, 100);
}

/// The three independent PG reads §22.5's `ledger::close` needs, done here as plain SQL (this
/// file's own IO, not `adapters`-internal — see module doc) so the *judgment* under test is
/// exactly `humaux_retrieval::completeness::ledger::close`, unmodified.
fn read_ledger(admin: &mut Client, k: &StreamKey) -> LedgerReads {
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
    let done: i64 = row.get(0);
    let deleted: i64 = row.get(1);
    let skipped: i64 = row.get(2);
    let pending: i64 = row.get(3);

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

    LedgerReads {
        expected: expected as u64,
        done: done as u64,
        deleted: deleted as u64,
        skipped: skipped as u64,
        open_gaps: open_gaps as u64,
        pending: pending as u64,
    }
}

fn cleanup(admin: &mut Client, k: &StreamKey) {
    let _ = admin.execute(
        "DELETE FROM projection.stream_log WHERE tenant_id=$1",
        &[&k.tenant_id.0],
    );
    let _ = admin.execute(
        "DELETE FROM projection.stream_checkpoints WHERE tenant_id=$1",
        &[&k.tenant_id.0],
    );
    let _ = admin.execute(
        "DELETE FROM control.tenants WHERE tenant_id=$1",
        &[&k.tenant_id.0],
    );
}

fn registry(cell: CellId, caller: CallerId) -> IntraCellResourceRegistry {
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            "127.0.0.1",
            6333,
            cell,
            vec!["127.0.0.1/32".parse().unwrap()],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .expect("127.0.0.1/32 is a reserved/loopback CIDR"),
    );
    IntraCellResourceRegistry::new(entries, cell, caller)
}

fn payload(tenant_id: TenantId, user_id: UserId, seq: i64) -> QdrantPointPayload {
    QdrantPointPayload {
        tenant_id,
        workspace_id: WorkspaceId::new(),
        visibility_class: VisibilityClass::UserPrivate,
        visibility_user_id: Some(user_id),
        visibility_workspace_id: None,
        object_type: "memory_record".to_string(),
        memory_type: MemoryType::Fact,
        status: AuthorityStatus::Active,
        authority: AuthorityClass::PrivateKnowledge,
        created_at: OffsetDateTime::now_utc(),
        effective_at: OffsetDateTime::now_utc(),
        embedding_version: "v1".to_string(),
        projection_version: "g23_2".to_string(),
        source_stream_seq: seq,
        data_class: DataClass::Private,
        egress_disposition: EgressDisposition::Forbidden,
    }
}

/// Creates a throwaway collection and upserts `upsert_count` of `total` deterministic points
/// (`PointId::Uuid`, seq `1..=total`), then retries `count()` until it observes `upsert_count`
/// visible (Qdrant indexing is async) or gives up. Returns `(collection, all point ids in seq
/// order, transport, permit, scope)`.
#[allow(clippy::too_many_arguments)]
async fn setup_collection(
    tenant_id: TenantId,
    user_id: UserId,
    total: i64,
    upsert_count: i64,
) -> (
    String,
    Vec<PointId>,
    HttpIntraCellTransport,
    humaux_infra_cell::CellAccessPermit,
    AuthorizationScope,
) {
    let cell = CellId(Uuid::now_v7());
    let caller = CallerId(format!("g23-2-{}", Uuid::now_v7()));
    let registry = registry(cell, caller);
    let transport = HttpIntraCellTransport::new(
        registry.clone(),
        Duration::from_secs(10),
        humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
    )
    .expect("client builds");
    let permit = authorize_cell_access(
        &registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(120),
    )
    .expect("same-cell, allowlisted caller must mint");

    let collection = format!("g23_2_{}", Uuid::now_v7().simple());
    let create_body = create_collection_body(4, Distance::Cosine, 1, 1, 1, ShardingMethod::Auto);
    transport
        .execute(
            &permit,
            IntraCellRequest {
                method: IntraCellMethod::Put,
                path: format!("/collections/{collection}"),
                json_body: Some(create_body),
                headers: Vec::new(),
            },
        )
        .await
        .expect("create collection");
    transport
        .execute(
            &permit,
            IntraCellRequest {
                method: IntraCellMethod::Put,
                path: format!("/collections/{collection}/index"),
                json_body: Some(tenant_index_body()),
                headers: Vec::new(),
            },
        )
        .await
        .expect("tenant index");

    let ids: Vec<PointId> = (1..=total).map(|_| PointId::Uuid(Uuid::now_v7())).collect();
    let ha = ha_profile_for(QdrantOperation::NormalImmutableUpsert);
    for (i, id) in ids.iter().enumerate().take(upsert_count as usize) {
        let seq = i as i64 + 1;
        let p = payload(tenant_id, user_id, seq);
        let indexable: IndexablePayload = p.into_indexable().expect("Private is indexable");
        upsert(
            &transport,
            &permit,
            &collection,
            &[(*id, &indexable, vec![0.1, 0.2, 0.3, 0.4])],
            ha,
        )
        .await
        .expect("upsert");
    }

    let scope = AuthorizationScope::new(
        tenant_id,
        PrincipalId::new(),
        Some(user_id),
        BoundedSet::new(Vec::<WorkspaceId>::new()).expect("empty workspace set is valid"),
    );

    // Wait for the upserted subset to become search-visible before returning.
    let expect_visible = upsert_count.min(total) as u64;
    for _ in 0..40 {
        let filter = VisibleCountFilter::new(&scope, "g23_2").expect("non-empty version");
        if let Ok(n) = count(&transport, &permit, &collection, &filter).await
            && n >= expect_visible
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    (collection, ids, transport, permit, scope)
}

async fn teardown_collection(
    transport: &HttpIntraCellTransport,
    permit: &humaux_infra_cell::CellAccessPermit,
    collection: &str,
) {
    let _ = transport
        .execute(
            permit,
            IntraCellRequest {
                method: IntraCellMethod::Delete,
                path: format!("/collections/{collection}"),
                json_body: None,
                headers: Vec::new(),
            },
        )
        .await;
}

/// Old (pre-§23.1②) formula — kept only to prove it stays pinned at `1.0` while the new
/// formula moves (reverse falsification, §23.4).
fn old_ratio(done: u64, deleted: u64, expected: u64) -> f64 {
    (done - deleted) as f64 / (expected - deleted) as f64
}

// ============================================================================
// Injection 1: bypass retention::tombstone, delete 10 Qdrant points directly.
// ============================================================================

// `#[test]` + a hand-driven `Runtime`, not `#[tokio::test]`: the blocking `postgres::Client`
// (used for every ledger read/seed below) panics ("Cannot start a runtime from within a
// runtime") if invoked while a `#[tokio::test]`'s own runtime is already active on this
// thread. `stream_repo.rs`'s DB fixture uses the identical shape for the identical reason.
#[test]
fn g23_2_injection_1_bypass_tombstone_direct_delete() {
    let Some((_, mut admin)) =
        skip_unless_both_reachable("g23_2_injection_1_bypass_tombstone_direct_delete")
    else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
            &[&"g23_2 injection1"],
        )
        .expect("insert tenant")
        .get(0);
    let k = key(tenant_id);
    seed_100_done(&mut admin, &k);

    let (collection, ids, transport, permit, scope) =
        rt.block_on(setup_collection(k.tenant_id, UserId::new(), 100, 100));

    // §23.4 precondition check: all 100 confirmed search-visible before the injection runs.
    let filter = VisibleCountFilter::new(&scope, "g23_2").unwrap();
    let before = rt
        .block_on(count(&transport, &permit, &collection, &filter))
        .expect("count before");
    assert_eq!(
        before, 100,
        "precondition: 100 must be search-visible before the injection"
    );

    // Injection: raw delete, bypassing `retention::tombstone` entirely — no PG write happens.
    let to_delete: Vec<serde_json::Value> =
        ids[0..10].iter().map(|id| point_id_json(*id)).collect();
    rt.block_on(transport.execute(
        &permit,
        IntraCellRequest {
            method: IntraCellMethod::Post,
            path: format!("/collections/{collection}/points/delete?wait=true"),
            json_body: Some(serde_json::json!({ "points": to_delete })),
            headers: Vec::new(),
        },
    ))
    .expect("raw delete request must not fail transport-side");

    let after = rt
        .block_on(count(&transport, &permit, &collection, &filter))
        .expect("count after");
    assert_eq!(
        after, 90,
        "visible must drop 100 -> 90 after the bypass delete"
    );

    let reads = read_ledger(&mut admin, &k);
    assert_eq!(
        reads.done, 100,
        "ledger done must stay 100 — no PG write happened"
    );
    assert_eq!(
        reads.deleted, 0,
        "ledger deleted must stay 0 — retention::tombstone was bypassed"
    );
    let closure = ledger::close(reads);
    assert!(closure.is_closed());

    let out = build_projection_block(&closure, Some(after));
    assert_eq!(
        out.value.completeness_ratio,
        Some(0.90),
        "ratio must move 1.0 -> 0.90"
    );
    assert!(!out.value.current);
    assert_eq!(
        out.degradations.as_slice(),
        &[DegradeCode::ProjectionInvisibleLoss]
    );

    // Reverse falsification (§23.4, "not optional"): the old formula is blind to this fault.
    assert_eq!(old_ratio(reads.done, reads.deleted, reads.expected), 1.0);

    rt.block_on(teardown_collection(&transport, &permit, &collection));
    cleanup(&mut admin, &k);
}

// ============================================================================
// Injection 2: adapter acks without visibility verification — 7 of 100 never indexed.
// ============================================================================

#[test]
fn g23_2_injection_2_adapter_acks_without_verify_loses_seven() {
    let Some((_, mut admin)) =
        skip_unless_both_reachable("g23_2_injection_2_adapter_acks_without_verify_loses_seven")
    else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
            &[&"g23_2 injection2"],
        )
        .expect("insert tenant")
        .get(0);
    let k = key(tenant_id);
    // §17.4's fault shape: the ledger settles all 100 as DONE (the adapter acked every write)
    // even though only 93 ever became search-visible — an adapter that skipped
    // `verify_visible`/§17.4's confirmation contract, not a partial write to PostgreSQL.
    seed_100_done(&mut admin, &k);

    let (collection, _ids, transport, permit, scope) =
        rt.block_on(setup_collection(k.tenant_id, UserId::new(), 100, 93));

    let filter = VisibleCountFilter::new(&scope, "g23_2").unwrap();
    let visible = rt
        .block_on(count(&transport, &permit, &collection, &filter))
        .expect("count");
    assert_eq!(
        visible, 93,
        "only the 93 actually-upserted points are search-visible"
    );

    let reads = read_ledger(&mut admin, &k);
    assert_eq!(reads.done, 100);
    assert_eq!(reads.deleted, 0);

    let closure = ledger::close(reads);
    let out = build_projection_block(&closure, Some(visible));
    assert_eq!(
        out.value.completeness_ratio,
        Some(0.93),
        "ratio must move 1.0 -> 0.93"
    );
    assert!(!out.value.current, "A2 must be red");
    assert_eq!(
        out.degradations.as_slice(),
        &[DegradeCode::ProjectionInvisibleLoss]
    );

    assert_eq!(
        old_ratio(reads.done, reads.deleted, reads.expected),
        1.0,
        "old formula stays blind"
    );

    rt.block_on(teardown_collection(&transport, &permit, &collection));
    cleanup(&mut admin, &k);
}

// ============================================================================
// Legal deletion contrast: real tombstone + real Qdrant delete, four-lane sampling.
// ============================================================================

// Single sequential e2e narrative (setup -> boundary A -> tombstone -> boundary B ->
// four-lane sampling -> purge -> boundary C), same shape and same allow as
// `qdrant_live.rs`'s own `upsert_then_search_visible_round_trips_over_real_qdrant`.
#[allow(clippy::too_many_lines)]
#[test]
fn g23_2_legal_deletion_contrast_stays_closed_across_four_lanes() {
    let Some((dsn, mut admin)) =
        skip_unless_both_reachable("g23_2_legal_deletion_contrast_stays_closed_across_four_lanes")
    else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
            &[&"g23_2 legal deletion"],
        )
        .expect("insert tenant")
        .get(0);
    let k = key(tenant_id);
    seed_100_done(&mut admin, &k);

    let user_id = UserId::new();
    let (collection, ids, transport, permit, scope) =
        rt.block_on(setup_collection(k.tenant_id, user_id, 100, 100));
    let filter = VisibleCountFilter::new(&scope, "g23_2").unwrap();

    // Boundary A: before any deletion — ratio 1.0, current true.
    let raw_a = rt
        .block_on(count(&transport, &permit, &collection, &filter))
        .unwrap();
    let reads_a = read_ledger(&mut admin, &k);
    let closure_a = ledger::close(reads_a);
    let out_a = build_projection_block(&closure_a, Some(raw_a));
    assert_eq!(out_a.value.completeness_ratio, Some(1.0));
    assert!(out_a.value.current);
    assert!(out_a.degradations.is_empty());

    // §37 DeletionPlan step 1: tombstone-first — real PG write via `forget_repo::tombstone`,
    // no Qdrant point removed yet (steps 1..5 boundary, before physical purge). `maintenance`
    // is kept open past this block: lanes 3/4 below reuse it, as does boundary C's own
    // production tombstone-write path check.
    let deleted_ids = ids[0..10].to_vec();
    let maintenance = rt.block_on(async {
        let maintenance = MaintenanceDbPool::connect(&dsn_as_role(&dsn, "role_maintenance"))
            .await
            .expect("maintenance pool");
        for seq in 1..=10u64 {
            let ok = forget_repo::tombstone(&maintenance, &k, seq)
                .await
                .expect("tombstone must not error");
            assert!(ok, "seq {seq} must transition to TOMBSTONED exactly once");
        }
        maintenance
    });

    // Boundary B: step 1 done, step 5 (physical purge) not yet run. Ledger side: `deleted`
    // must now read 10 (real PG state), `done` unchanged (TOMBSTONED still counts toward the
    // SETTLED_OK union, §15.2). Qdrant side: the 10 points are still physically present (no
    // Qdrant delete has happened) — `visible` must still read 90 via `count_visible`'s
    // query-time overlay (§23.1②: raw Qdrant `count()` alone would still read 100 at this
    // exact boundary, which is why the overlay is load-bearing here — same production call
    // as boundary C below, proving it purge-order-independent rather than two different
    // "correct" shapes depending on when it runs).
    let reads_b = read_ledger(&mut admin, &k);
    assert_eq!(reads_b.done, 100);
    assert_eq!(
        reads_b.deleted, 10,
        "real TOMBSTONED write must be visible in PG"
    );
    let raw_b = rt
        .block_on(count(&transport, &permit, &collection, &filter))
        .unwrap();
    assert_eq!(
        raw_b, 100,
        "physical points still present — purge has not run yet"
    );
    let visible_b = rt
        .block_on(count_visible(
            &transport,
            &permit,
            &collection,
            &filter,
            &deleted_ids,
        ))
        .expect("count_visible pre-purge");
    assert_eq!(visible_b, 90);
    let closure_b = ledger::close(reads_b);
    let out_b = build_projection_block(&closure_b, Some(visible_b));
    assert_eq!(
        out_b.value.completeness_ratio,
        Some(1.0),
        "overlay keeps the ratio at 1.0 pre-purge"
    );
    assert!(out_b.value.current);
    assert!(
        out_b.degradations.is_empty(),
        "legal deletion must never raise PROJECTION_INVISIBLE_LOSS"
    );

    // Four-lane sampling at boundary B — the 10 tombstoned ids must not surface via any of
    // them, *including* while the physical points are still present (§23.4's own text:
    // "包括第 5 步物理 purge 还没跑的那些采样点"). Every lane below calls a production entry
    // point (`qdrant::overlay_filter` / `forget_repo::count_excluding_tombstoned` /
    // `forget_repo::state_of`) rather than hand-writing its own predicate.

    // Lane 1 (dense): `qdrant::overlay_filter` applied to an otherwise-empty filter — the real
    // predicate builder any dense-lane search must route through, not just `count()`.
    let dense_body = serde_json::json!({
        "filter": overlay_filter(serde_json::json!({}), &deleted_ids),
        "limit": 100,
        "with_payload": false,
        "with_vector": false,
    });
    let dense_result = rt
        .block_on(transport.execute(
            &permit,
            IntraCellRequest {
                method: IntraCellMethod::Post,
                path: format!("/collections/{collection}/points/scroll"),
                json_body: Some(dense_body),
                headers: Vec::new(),
            },
        ))
        .expect("overlay scroll");
    let dense_points = dense_result
        .json_body
        .as_ref()
        .and_then(|b| b.get("result"))
        .and_then(|r| r.get("points"))
        .and_then(|p| p.as_array())
        .cloned()
        .unwrap_or_default();
    for id in &deleted_ids {
        let wire = point_id_json(*id);
        assert!(
            !dense_points.iter().any(|p| p.get("id") == Some(&wire)),
            "dense lane must never surface a tombstoned id"
        );
    }

    // Lane 2 (sparse): same collection/points (§17.6), scored differently — `build_sparse_lane`
    // supplies the real tenant/visibility filter, and the identical production
    // `qdrant::overlay_filter` folds the tombstone predicate into it, executed for real against
    // the same collection.
    let sparse_lane = build_sparse_lane(
        &scope,
        &[],
        &[CorpusDocument {
            tenant_id: k.tenant_id,
            visibility: VisibilityDescriptor {
                class: VisibilityClass::UserPrivate,
                user_id: Some(user_id),
                workspace_id: None,
            },
            terms: vec!["placeholder".to_string()],
        }],
        &["placeholder".to_string()],
        true,
    );
    let humaux_projection::sparse::SparseLane::Executed {
        filter: sparse_filter,
        ..
    } = &sparse_lane
    else {
        panic!("sparse lane must actually run, not skip");
    };
    let sparse_body = serde_json::json!({
        "filter": overlay_filter(condition_to_filter(sparse_filter), &deleted_ids),
        "limit": 100, "with_payload": false, "with_vector": false,
    });
    let sparse_result = rt
        .block_on(transport.execute(
            &permit,
            IntraCellRequest {
                method: IntraCellMethod::Post,
                path: format!("/collections/{collection}/points/scroll"),
                json_body: Some(sparse_body),
                headers: Vec::new(),
            },
        ))
        .expect("sparse-lane overlay scroll");
    let sparse_points = sparse_result
        .json_body
        .as_ref()
        .and_then(|b| b.get("result"))
        .and_then(|r| r.get("points"))
        .and_then(|p| p.as_array())
        .cloned()
        .unwrap_or_default();
    for id in &deleted_ids {
        let wire = point_id_json(*id);
        assert!(
            !sparse_points.iter().any(|p| p.get("id") == Some(&wire)),
            "sparse lane must never surface a tombstoned id"
        );
    }

    // Lane 3 (§22.1 PostgreSQL EXACT channel): `forget_repo::count_excluding_tombstoned`, the
    // production predicate shared with lane 4 and with `tombstone`'s own key shape.
    let exact_count = rt
        .block_on(forget_repo::count_excluding_tombstoned(
            &maintenance,
            &k,
            1,
            10,
        ))
        .expect("exact channel count");
    assert_eq!(
        exact_count, 0,
        "PostgreSQL EXACT channel must exclude all 10 tombstoned seqs"
    );

    // Lane 4 (literal): `forget_repo::state_of` per-row lookup — every one of the 10 rows
    // reports TOMBSTONED, never DONE (the state a literal id-lookup lane would otherwise
    // surface as "present").
    for seq in 1..=10u64 {
        let state = rt
            .block_on(forget_repo::state_of(&maintenance, &k, seq))
            .expect("literal-lane row lookup")
            .expect("row must exist");
        assert_eq!(
            state, "TOMBSTONED",
            "literal lane must observe seq {seq} as TOMBSTONED, not DONE"
        );
    }

    // §37 step 5: physical purge — the delete `retention::tombstone` deferred. After this,
    // ratio/current are unchanged (the overlay already accounted for these 10); this only
    // proves purge itself does not additionally move the numbers.
    let to_delete: Vec<serde_json::Value> =
        deleted_ids.iter().map(|id| point_id_json(*id)).collect();
    rt.block_on(transport.execute(
        &permit,
        IntraCellRequest {
            method: IntraCellMethod::Post,
            path: format!("/collections/{collection}/points/delete?wait=true"),
            json_body: Some(serde_json::json!({ "points": to_delete })),
            headers: Vec::new(),
        },
    ))
    .expect("physical purge delete");
    let raw_c = rt
        .block_on(count(&transport, &permit, &collection, &filter))
        .unwrap();
    assert_eq!(raw_c, 90, "purge must physically remove the 10 points");
    // Boundary-C assertion (major finding fix): the *same* `count_visible` call, same
    // tombstoned-id argument, unchanged across purge — the 10 ids are now physically absent
    // (so `has_id` simply matches nothing further) rather than needing a second, different
    // "post-purge" call shape.
    let visible_c = rt
        .block_on(count_visible(
            &transport,
            &permit,
            &collection,
            &filter,
            &deleted_ids,
        ))
        .expect("count_visible post-purge");
    assert_eq!(
        visible_c, visible_b,
        "count_visible's answer must not move across purge"
    );
    let reads_c = read_ledger(&mut admin, &k);
    let closure_c = ledger::close(reads_c);
    let out_c = build_projection_block(&closure_c, Some(visible_c));
    assert_eq!(out_c.value.completeness_ratio, Some(1.0));
    assert!(out_c.value.current);
    assert!(out_c.degradations.is_empty());

    rt.block_on(teardown_collection(&transport, &permit, &collection));
    cleanup(&mut admin, &k);
}

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}
