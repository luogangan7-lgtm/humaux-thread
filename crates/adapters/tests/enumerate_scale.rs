//! `adapters::tests::enumerate_scale` — P1-14 measurement (ADR-0062 D-K, M-1): `memory.enumerate`'s first page on a
//!   50k-memory workspace, on its own throwaway database.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-projection, humaux-testkit, postgres, sqlx, tokio];
//!   services=[PostgreSQL(owner) r=[ops.selection_snapshot_items] w=[control.private_reasoning_domains,
//!   control.tenants, control.workspaces, private.evidence_objects, private.memory_evidence,
//!   private.memory_records, projection.stream_checkpoints], PostgreSQL(role_gateway) r=[private.memory_records]];
//!   env=[CARGO_MANIFEST_DIR, HUMAUX_TEST_PG_DSN]; modules=[adapters::context_repo, adapters::postgres,
//!   adapters::tests::support::throwaway_db, domain::identity, domain::ids, humaux-testkit, projection::serving]
//! Called-by: [cargo-test]
//! Invariants: [the 50k seed and every manifest it mints live in humaux_thread_c35_enum_<pid>_<n>, dropped WITH
//!   (FORCE) even on panic, never in the shared dev database (ruling E8); bar (a) reads the checked-in
//!   baseline B (Step 1, measured before any S7 edit), never a value from the same run; a missing baseline fails
//!   naming the file, never skips]
//! Spec: Baseline §20.4; §22.1; ADR-0062 D-K, Known limits (ruling E12)
//!
//! Regression bars (main-line rulings E12, 2026-10-04 21:50, and E12-b, 2026-10-05 20:05), each printing its
//! operands; n = 30 timed runs after 3 warm-ups, ms, dev host, debug test build, throwaway DB:
//! (a) `ratio_after ≤ 0.6 × ratio_B` where ratio = first_p95 / floor_p95 of the SAME run; (c) `later_p95_ms < 300`;
//! (d) `manifest_rows == cap`. E12-b: no bar compares an absolute first-page time with the baseline — host load moves
//! first_p95 and the bare floor scan together (card 36 verification: 16.6 s under load, 12.9 s idle on a slower day,
//! 9.4 s the day before, ratio 13.8–17.3 throughout), so only the ratio is load-independent; it also subsumes the
//! former weaker bar `ratio × 1.5 ≤ ratio_B`. The card's 300 ms first-page bar is neither relaxed nor met: it moved
//! verbatim to card 35b and is printed as `target_300ms=` on the `ENUM` line only.

use std::time::{Duration, Instant};

use humaux_adapters::context_repo::{MemoryEnumerationParams, materialize_memory_enumeration};
use humaux_adapters::postgres::RuntimeDbPool;
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{Scope, TenantId, WorkspaceId};
use humaux_projection::serving::StreamFamily;
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

#[path = "support/throwaway_db.rs"]
#[allow(dead_code)]
mod throwaway_db;

const NAME: &str = "enumerate_first_page_scales_with_a_capped_manifest";
const N: i64 = 50_000;
const RUNS: usize = 30;
const WARMUPS: usize = 3;
const PAGE: u16 = 50;
const MAC_KEY: &[u8] = b"c35 enumerate scale test mac key";
/// The manifest cap this measurement runs under (the dev launchers' `HUMAUX_GATEWAY_ENUMERATION_MANIFEST_CAP`).
const CAP: usize = 1000;
/// The card's first-page target, ms (ruling E12: reported as `target_300ms=`, not asserted until card 35b).
// ponytail: first page measured ≈ 9.4 s p95 on 50k memories because RLS policy memory_records_subject_visibility
// calls private.memory_subject_visibility_ok per row; card 35b (set-based visibility) restores `first_p95 < 300`.
const FIRST_P95_TARGET_MS: f64 = 300.0;
/// Bar (c): the pages-2..n promise, ms (ruling E12).
const LATER_P95_BAR_MS: f64 = 300.0;
const BASELINE: &str = "tests/data/enumerate_scale_baseline.txt";

/// The numeric field `<name>=` of the checked-in Step-1 `ENUM` line (baseline B).
fn baseline_field(name: &str) -> f64 {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(BASELINE);
    let line = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "baseline B {} is required (D-K M-1 Step 1): {e}",
            path.display()
        )
    });
    assert!(
        line.starts_with(&format!("ENUM n={N} runs={RUNS} ")),
        "baseline B {} was measured at another scale: {line}",
        path.display()
    );
    line.split_whitespace()
        .find_map(|field| field.strip_prefix(name)?.strip_prefix('='))
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("baseline B {} has no {name}=: {line}", path.display()))
}

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

/// Nearest-rank percentile of `samples` in ms.
fn pct(samples: &mut [f64], p: f64) -> f64 {
    samples.sort_by(f64::total_cmp);
    let rank = ((p * samples.len() as f64).ceil() as usize).max(1);
    samples[rank - 1]
}

fn ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

/// The 50k workspace: one owner statement per table (memory and link commit together, §8.6 deferred orphan check),
/// plus the serving checkpoint of its stream. Returns `(tenant, workspace)`.
fn seed(admin: &mut Client) -> (Uuid, Uuid) {
    let tenant: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ('c35 enumerate scale') RETURNING tenant_id",
            &[],
        )
        .expect("tenant")
        .get(0);
    let domain: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
             VALUES ($1, 'c35 scale') RETURNING reasoning_domain_id",
            &[&tenant],
        )
        .expect("reasoning domain")
        .get(0);
    let workspace: Uuid = admin
        .query_one(
            "INSERT INTO control.workspaces (tenant_id, name) VALUES ($1, 'c35 scale') \
             RETURNING workspace_id",
            &[&tenant],
        )
        .expect("workspace")
        .get(0);
    // One statement per table; memory and link commit together (§8.6 deferred orphan check).
    admin
        .batch_execute(&format!(
            "BEGIN; \
             CREATE TEMP TABLE c35_seed AS \
               SELECT g, uuidv7() AS evidence_id, uuidv7() AS memory_id FROM generate_series(1, {N}) g; \
             INSERT INTO private.evidence_objects (evidence_id, tenant_id, evidence_kind, payload_sha256, \
               data_class, origin_class, visibility_class, reasoning_domain_id) \
               SELECT evidence_id, '{tenant}', 'EVENT', sha256(g::text::bytea), 'INTERNAL', \
                      'DirectUserInput', 'TENANT_SHARED', '{domain}' FROM c35_seed; \
             INSERT INTO private.memory_records (memory_id, tenant_id, memory_type, content, visibility_class, \
               authority_class, confidence, status, asserted_at) \
               SELECT memory_id, '{tenant}', 'NOTE', jsonb_build_object('title', 'scale ' || g), \
                      'TENANT_SHARED', 'PrivateKnowledge', 0.9, 'active', now() FROM c35_seed; \
             INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
               SELECT memory_id, evidence_id, 'PRIMARY', 0 FROM c35_seed; \
             INSERT INTO projection.stream_checkpoints (tenant_id, scope_kind, scope_id, domain, \
               projection_kind, projection_version, issued_highwater) \
               VALUES ('{tenant}', 'workspace', '{workspace}', 'private_memory', 'PRIVATE_MEMORY', 'v1', 0); \
             COMMIT; \
             ANALYZE private.evidence_objects; ANALYZE private.memory_records; ANALYZE private.memory_evidence;"
        ))
        .expect("seed 50k memories");
    (tenant, workspace)
}

/// The authorization, scope and stream family of a workspace-scoped `memory.enumerate` (so page 1 runs its census).
fn enumeration_inputs(tenant: Uuid, workspace: Uuid) -> (AuthorizationScope, Scope, StreamFamily) {
    let ws = WorkspaceId(workspace);
    let authorization = AuthorizationScope::new(
        TenantId(tenant),
        PrincipalId::new(),
        None,
        BoundedSet::new([ws]).expect("workspace grant"),
    );
    let scope = Scope {
        tenant_id: TenantId(tenant),
        user_id: None,
        workspace_id: Some(ws),
        repository_id: None,
        task_id: None,
        run_id: None,
        agent_id: None,
    };
    let family = StreamFamily::new(
        TenantId(tenant),
        "workspace",
        workspace,
        "private_memory",
        "PRIVATE_MEMORY",
    );
    (authorization, scope, family)
}

/// One bare O(N) id scan of ENUMERATION_SCOPE + ENUMERATION_PREDICATE (D-K M-1 floor), under role_gateway's RLS with
/// the GUCs the gateway sets; its wall time in ms.
fn floor_ms_once(admin: &mut Client, tenant: Uuid, workspace: Uuid) -> f64 {
    let start = Instant::now();
    // dep: PostgreSQL(role_gateway) — the floor scan, SET LOCAL ROLE on the owner connection
    let mut txn = admin.transaction().expect("floor txn");
    txn.batch_execute(&format!(
        "SET LOCAL ROLE role_gateway; SET LOCAL humaux.tenant_id = '{tenant}'; \
         SELECT set_config('humaux.user_id', '{}', true)",
        Uuid::nil()
    ))
    .expect("floor GUCs");
    let ids = txn
        .query(
            "SELECT memory_id FROM private.memory_records \
             WHERE memory_records.tenant_id = $1 \
               AND (memory_records.visibility_workspace_id IS NULL \
                    OR memory_records.visibility_workspace_id = $2) \
               AND memory_records.archived_at IS NULL ORDER BY memory_id DESC",
            &[&tenant, &workspace],
        )
        .expect("floor scan");
    txn.commit().expect("floor commit");
    assert_eq!(ids.len() as i64, N, "floor sees the whole seed");
    ms(start)
}

/// Rulings E12 / E12-b bars (a), (c), (d), each printing its operands; called after the `ENUM` line so a red run
/// still prints the numbers it was judged on. `ratio_b` comes from baseline B.
fn judge(ratio: f64, later_p95: f64, manifest_rows: i64, ratio_b: f64) {
    assert_eq!(
        manifest_rows as usize, CAP,
        "(d) manifest_rows={manifest_rows} must be == cap={CAP}"
    );
    assert!(
        ratio <= 0.6 * ratio_b,
        "(a) ratio_after={ratio:.2} must be <= 0.6 x ratio_B={ratio_b:.2} (= {:.2}; from {BASELINE})",
        0.6 * ratio_b
    );
    assert!(
        later_p95 < LATER_P95_BAR_MS,
        "(c) later_p95_ms={later_p95:.1} must be < {LATER_P95_BAR_MS} (n={RUNS})"
    );
}

#[test]
#[ignore = "lane(b) timing-sensitive: seeds 50k memories on its own throwaway database (ADR-0062 D-K M-1)"]
fn enumerate_first_page_scales_with_a_capped_manifest() {
    let ratio_b = baseline_field("ratio");
    let db = match throwaway_db::create("c35_enum") {
        Ok(db) => db,
        Err(reason) => {
            skip_or_fail(NAME, &reason.to_string(), ExternalDep::Postgres);
            return;
        }
    };
    let dsn = db.dsn();
    // dep: PostgreSQL(owner) — seeds the 50k workspace on the throwaway database
    let mut admin = Client::connect(&dsn, NoTls).expect("owner connection");
    let (tenant, workspace) = seed(&mut admin);

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    // dep: PostgreSQL(role_gateway) — the production enumerate path's pool
    let pool = rt
        .block_on(RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")))
        .expect("role_gateway pool");
    let (authorization, scope, family) = enumeration_inputs(tenant, workspace);
    let key = family.with_version("v1");
    let page = |cursor: Option<&str>| {
        rt.block_on(materialize_memory_enumeration(
            &pool,
            &authorization,
            &scope,
            &family,
            &key,
            MemoryEnumerationParams {
                cursor,
                page_size: PAGE,
                ttl: Duration::from_secs(900),
                manifest_cap: CAP,
                mac_key: MAC_KEY,
                subject_id: None,
            },
        ))
        .unwrap_or_else(|e| panic!("enumerate page: {e:?}"))
    };
    for _ in 0..WARMUPS {
        let first = page(None);
        page(first.next_cursor.as_deref());
        floor_ms_once(&mut admin, tenant, workspace);
    }
    let (mut first_ms, mut later_ms, mut floor_ms) = (Vec::new(), Vec::new(), Vec::new());
    let mut last_snapshot = Uuid::nil();
    for _ in 0..RUNS {
        let start = Instant::now();
        let first = page(None);
        first_ms.push(ms(start));
        assert_eq!(first.memory.bodies.items.len(), usize::from(PAGE));
        let census = first.census.as_ref().expect("workspace census");
        assert_eq!(
            census.census.enumeration().map(|e| e.total()),
            Some(N as u64),
            "page 1's census counts the whole workspace"
        );
        last_snapshot = first.snapshot_id;
        let start = Instant::now();
        page(first.next_cursor.as_deref());
        later_ms.push(ms(start));
        floor_ms.push(floor_ms_once(&mut admin, tenant, workspace));
    }
    let manifest_rows: i64 = admin
        .query_one(
            "SELECT count(*) FROM ops.selection_snapshot_items WHERE selection_snapshot_id = $1",
            &[&last_snapshot],
        )
        .expect("manifest rows")
        .get(0);
    let first_p50 = pct(&mut first_ms, 0.50);
    let first_p95 = pct(&mut first_ms, 0.95);
    let later_p95 = pct(&mut later_ms, 0.95);
    let floor_p95 = pct(&mut floor_ms, 0.95);
    let ratio = first_p95 / floor_p95;
    let target = if first_p95 < FIRST_P95_TARGET_MS {
        "met"
    } else {
        "not_met"
    };
    println!(
        "ENUM n={N} runs={RUNS} first_p50_ms={first_p50:.1} first_p95_ms={first_p95:.1} \
         later_p95_ms={later_p95:.1} floor_p95_ms={floor_p95:.1} ratio={ratio:.2} manifest_rows={manifest_rows} \
         target_300ms={target}"
    );
    judge(ratio, later_p95, manifest_rows, ratio_b);
    drop(pool);
    drop(rt);
    drop(admin);
    drop(db);
}
