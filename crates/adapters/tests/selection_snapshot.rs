//! T6.3 integration test — `selection_repo` (§20.4) against a real Postgres.
//!
//! **G20-1 / G80-32 "Stable Selection Snapshot Integrity"** (§20.4's own anchor for both gate
//! numbers, `§20#G20-1`): seed 30 eligible memories, page through a §20.4 Mode B snapshot at
//! `page_size = 7`, concurrently inserting 20 "higher-ranked" rows (newer UUIDv7, so they sort
//! first under this module's `ORDER BY memory_id DESC`, same technique
//! `consolidate_snapshot.rs`'s G11-1 test uses) — first mid-flight during the snapshot's own
//! materializing transaction, then again after page 1 has already been returned to the caller.
//! Every page read back afterward must be duplicate-free, must not miss anything from the
//! base 30, and must not contain any of the 20 concurrently-inserted rows.
//!
//! **Red-then-green fault injection (§80.1 "没有注错红转绿记录就不算存在"):** with
//! `selection_repo::begin_enumeration_snapshot`/`fetch_enumeration_page` locally replaced by a
//! naive multi-page `OFFSET`/`LIMIT` loop over `private.memory_records` directly (one
//! autocommit `SELECT ... ORDER BY memory_id DESC LIMIT $page_size OFFSET $n` per page, no
//! `ops.selection_snapshot_items` manifest, no shared transaction — i.e. exactly §20.4's
//! banned "OFFSET pagination over live mutable set"), [`snapshot_pagination_is_stable_under_
//! concurrent_inserts`] below went RED: the concurrent inserts shifted every later page's
//! `OFFSET` window, producing both duplicate ids (a row already returned on an earlier page
//! reappearing after the window shifted back over it) and missed ones (a row pushed past the
//! last page's boundary). Reverting to this crate's manifest-then-page implementation turned
//! the same test back GREEN. The buggy variant itself is intentionally not committed anywhere
//! in this workspace (§46 "main 不允许红") — this comment is the "红转绿" record §80.1 asks
//! for.
//!
//! Cursor MAC/cross-tenant checks are also exercised end-to-end here (through real DB pages,
//! not just `humaux_domain::selection`'s own pure unit tests): a page-1 cursor replayed under
//! a second, unrelated tenant is rejected, and a page-1 cursor with `last_ordinal` tampered
//! after signing is rejected.
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or the migration missing all print a
//! visible SKIP and return.

use std::sync::Mutex;

use humaux_adapters::postgres::RetrievalWorkerDbPool;
use humaux_adapters::selection_repo::{self, SelectionRepoError};
use humaux_domain::selection::CursorError;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

/// Same reasoning as `consolidate_snapshot.rs`'s `SERIAL_GUARD`: each test seeds its own
/// throwaway tenant, but the concurrency test drives real concurrent connections against
/// shared tables and asserts on exact counts.
static SERIAL_GUARD: Mutex<()> = Mutex::new(());

const MAC_KEY: &[u8] = b"selection_snapshot.rs test MAC key - not a real secret";

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    // Same libpq `options=-c role=X` form `consolidate_snapshot.rs::dsn_as_role` uses —
    // verified working against both drivers there.
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

struct Handle {
    rt: tokio::runtime::Runtime,
    retrieval: RetrievalWorkerDbPool,
    admin: Client,
    tenant_id: Uuid,
    other_tenant_id: Uuid,
    evidence_id: Uuid,
    admin_dsn: String,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup (CLAUDE.md hard rule ④: this file never touches a schema/table
        // of its own, only rows it created under its own throwaway tenants).
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM ops.selection_snapshot_items WHERE tenant_id IN ('{0}','{1}'); \
             DELETE FROM ops.selection_snapshots WHERE tenant_id IN ('{0}','{1}'); \
             DELETE FROM private.memory_evidence WHERE memory_id IN \
               (SELECT memory_id FROM private.memory_records WHERE tenant_id IN ('{0}','{1}')); \
             DELETE FROM private.memory_records WHERE tenant_id IN ('{0}','{1}'); \
             DELETE FROM private.events WHERE event_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id IN ('{0}','{1}')); \
             DELETE FROM private.evidence_objects WHERE tenant_id IN ('{0}','{1}'); \
             DELETE FROM control.tenants WHERE tenant_id IN ('{0}','{1}');",
            self.tenant_id, self.other_tenant_id
        ));
    }
}

struct SelectionFixture;

impl DbIntegrationFixture for SelectionFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let migrated: bool = admin
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = 'ops' AND table_name = 'selection_snapshot_items' \
                   AND column_name = 'tenant_id')",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "ops.selection_snapshot_items.tenant_id does not exist — run `cargo xtask \
                 migrate` against HUMAUX_TEST_PG_DSN first (migrations 0083/0084)"
                    .to_string(),
            ));
        }

        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"selection_snapshot.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        let other_tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"selection_snapshot.rs throwaway tenant B (cross-tenant cursor check)"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        let reasoning_domain_id: Uuid = admin
            .query_one(
                "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
                 VALUES ($1, 'selection_snapshot.rs domain') RETURNING reasoning_domain_id",
                &[&tenant_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        // §8.1: `evidence_kind='EVENT'` needs a matching `private.events` row — same minimal
        // recipe `consolidate_snapshot.rs::isolate` uses.
        let evidence_id: Uuid = admin
            .query_one(
                "INSERT INTO private.evidence_objects \
                   (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                    visibility_class, reasoning_domain_id) \
                 VALUES ($1, 'EVENT', $2, 'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $3) \
                 RETURNING evidence_id",
                &[&tenant_id, &vec![0u8; 32], &reasoning_domain_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        admin
            .execute(
                "INSERT INTO private.events (event_id, event_kind, payload) \
                 VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
                &[&evidence_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        let retrieval_dsn = dsn_as_role(&dsn, "role_retrieval_worker");
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let retrieval = rt
            .block_on(RetrievalWorkerDbPool::connect(&retrieval_dsn))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            retrieval,
            admin,
            tenant_id,
            other_tenant_id,
            evidence_id,
            admin_dsn: dsn,
        })
    }
}

/// Seeds `count` `active`/`TENANT_SHARED` memory rows for `tenant_id`, each linked to
/// `evidence_id` in the same transaction (§8.6's orphan-Memory DEFERRABLE trigger only fires
/// at COMMIT, but the link must exist by then) — same recipe as
/// `consolidate_snapshot.rs::seed_active_memories`.
fn seed_active_memories(
    admin: &mut Client,
    tenant_id: Uuid,
    evidence_id: Uuid,
    count: usize,
) -> Vec<Uuid> {
    let mut txn = admin.transaction().expect("begin seed txn");
    let ids: Vec<Uuid> = (0..count)
        .map(|i| {
            let memory_id: Uuid = txn
                .query_one(
                    "INSERT INTO private.memory_records \
                       (tenant_id, memory_type, content, visibility_class, \
                        authority_class, confidence, status, asserted_at) \
                     VALUES ($1, 'NOTE', $2, 'TENANT_SHARED', 'PrivateKnowledge', 0.5, \
                             'active', now()) \
                     RETURNING memory_id",
                    &[&tenant_id, &serde_json::json!({"seed": i})],
                )
                .expect("seed active memory")
                .get(0);
            txn.execute(
                "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) \
                 VALUES ($1, $2, 'PRIMARY')",
                &[&memory_id, &evidence_id],
            )
            .expect("insert memory_evidence link");
            memory_id
        })
        .collect();
    txn.commit().expect("commit seed txn");
    ids
}

/// Inserts `count` more "concurrent" active memory rows on an independent blocking connection
/// — newer UUIDv7 than anything seeded before it, so they sort first under
/// `ORDER BY memory_id DESC` (the exact "更靠前的行" shape §20.4's G20-1 fault injection
/// describes).
/// The actual insert loop, connection-agnostic — used both from inside a `spawn_blocking`
/// (racing the snapshot's own materializing transaction, see
/// [`spawn_concurrent_inserter`]) and called directly (the post-page-1 wave, which needs no
/// race, just to exist before the remaining pages are fetched).
fn insert_concurrent_batch(
    client: &mut Client,
    tenant_id: Uuid,
    evidence_id: Uuid,
    count: usize,
    tag: &str,
) {
    for i in 0..count {
        let mut txn = client.transaction().expect("begin concurrent-insert txn");
        let memory_id: Uuid = txn
            .query_one(
                "INSERT INTO private.memory_records \
                   (tenant_id, memory_type, content, visibility_class, \
                    authority_class, confidence, status, asserted_at) \
                 VALUES ($1, 'NOTE', $2, 'TENANT_SHARED', 'PrivateKnowledge', 0.9, \
                         'active', now()) \
                 RETURNING memory_id",
                &[&tenant_id, &serde_json::json!({"concurrent": tag, "i": i})],
            )
            .expect("concurrent insert must succeed")
            .get(0);
        txn.execute(
            "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) \
             VALUES ($1, $2, 'PRIMARY')",
            &[&memory_id, &evidence_id],
        )
        .expect("concurrent memory_evidence link must succeed");
        txn.commit().expect("commit concurrent-insert txn");
    }
}

/// Inserts `count` more "concurrent" active memory rows on an independent blocking connection
/// — newer UUIDv7 than anything seeded before it, so they sort first under
/// `ORDER BY memory_id DESC` (the exact "更靠前的行" shape §20.4's G20-1 fault injection
/// describes). Must be called from inside an active Tokio runtime (`spawn_blocking` needs a
/// reactor at call time, not just when awaited).
fn spawn_concurrent_inserter(
    admin_dsn: String,
    tenant_id: Uuid,
    evidence_id: Uuid,
    count: usize,
    tag: &'static str,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        let mut client = Client::connect(&admin_dsn, NoTls).expect("concurrent inserter connects");
        insert_concurrent_batch(&mut client, tenant_id, evidence_id, count, tag);
    })
}

/// G20-1 / G80-32: base 30 + 20 concurrent-during-materialization + 15 concurrent-after-page-1
/// — every page collected afterward must equal exactly the base 30, no more, no less.
#[test]
fn snapshot_pagination_is_stable_under_concurrent_inserts() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<SelectionFixture, _>(
        "snapshot_pagination_is_stable_under_concurrent_inserts",
        |mut handle| {
            let base_ids =
                seed_active_memories(&mut handle.admin, handle.tenant_id, handle.evidence_id, 30);
            let mut base_sorted = base_ids.clone();
            base_sorted.sort();

            let tenant_id = handle.tenant_id;
            let evidence_id = handle.evidence_id;
            let admin_dsn = handle.admin_dsn.clone();
            let retrieval = &handle.retrieval;

            // Race 20 "higher-ranked" inserts against the snapshot's own materializing
            // transaction — mirrors `consolidate_snapshot.rs`'s G11-1 race exactly.
            let first_page = handle.rt.block_on(async {
                let snapshot = selection_repo::begin_enumeration_snapshot(
                    retrieval, tenant_id, 300.0, 7, MAC_KEY,
                );
                let inserter = spawn_concurrent_inserter(
                    admin_dsn.clone(),
                    tenant_id,
                    evidence_id,
                    20,
                    "during",
                );
                let (snapshot, _) = tokio::join!(snapshot, inserter);
                snapshot.expect("begin_enumeration_snapshot must not error")
            });

            // Now insert a *second* wave after page 1 has already been returned to the caller
            // — the "page 1 后插入排序更靠前的 20 行" shape from §20.4's own G20-1 wording,
            // this time strictly after materialization committed. No race needed here (the
            // manifest is already immutable), so this runs synchronously on the admin
            // connection, no Tokio runtime required.
            insert_concurrent_batch(&mut handle.admin, tenant_id, evidence_id, 15, "after");
            let _ = admin_dsn; // kept alive above only for the "during" race's inserter

            let mut collected = first_page.items.clone();
            let mut cursor = first_page.next_cursor.clone();
            let mut pages = 1;
            while let Some(c) = cursor {
                let page = handle
                    .rt
                    .block_on(selection_repo::fetch_enumeration_page(
                        &handle.retrieval,
                        tenant_id,
                        &c,
                        7,
                        MAC_KEY,
                    ))
                    .expect("fetch_enumeration_page must not error");
                collected.extend(page.items.clone());
                cursor = page.next_cursor;
                pages += 1;
                assert!(
                    pages <= 10,
                    "runaway pagination — next_cursor never became None"
                );
            }

            let mut collected_sorted = collected.clone();
            collected_sorted.sort();
            let mut dedup = collected_sorted.clone();
            dedup.dedup();
            assert_eq!(
                dedup.len(),
                collected_sorted.len(),
                "G20-1: pages must not repeat any item — collected {} with {} unique",
                collected_sorted.len(),
                dedup.len()
            );
            assert_eq!(
                collected_sorted, base_sorted,
                "G20-1: paginated set must equal exactly the base 30 — no misses from the \
                 snapshot universe, and none of the 35 concurrently-inserted rows leaking in"
            );
        },
    );
}

/// A page-1 cursor is only ever valid for the tenant it was issued under — §20.4 "禁止 ...
/// 跨 tenant 复用 cursor".
#[test]
fn cursor_rejects_cross_tenant_reuse() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<SelectionFixture, _>("cursor_rejects_cross_tenant_reuse", |mut handle| {
        seed_active_memories(&mut handle.admin, handle.tenant_id, handle.evidence_id, 3);
        let tenant_id = handle.tenant_id;
        let other_tenant_id = handle.other_tenant_id;

        let page1 = handle
            .rt
            .block_on(selection_repo::begin_enumeration_snapshot(
                &handle.retrieval,
                tenant_id,
                300.0,
                1,
                MAC_KEY,
            ))
            .expect("begin_enumeration_snapshot must not error");
        let cursor = page1
            .next_cursor
            .expect("3 items at page_size=1 must yield a next_cursor");

        let result = handle.rt.block_on(selection_repo::fetch_enumeration_page(
            &handle.retrieval,
            other_tenant_id,
            &cursor,
            1,
            MAC_KEY,
        ));
        assert!(
            matches!(
                result,
                Err(SelectionRepoError::Cursor(CursorError::CrossTenant))
            ),
            "expected CrossTenant rejection, got {result:?}"
        );
    });
}

/// A tampered `last_ordinal` (the client-visible pagination position) must invalidate the
/// MAC — §20.4 "禁止 ... 客户端可伪造 snapshot_upper_bound".
#[test]
fn cursor_rejects_forged_ordinal() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<SelectionFixture, _>("cursor_rejects_forged_ordinal", |mut handle| {
        seed_active_memories(&mut handle.admin, handle.tenant_id, handle.evidence_id, 5);
        let tenant_id = handle.tenant_id;

        let page1 = handle
            .rt
            .block_on(selection_repo::begin_enumeration_snapshot(
                &handle.retrieval,
                tenant_id,
                300.0,
                1,
                MAC_KEY,
            ))
            .expect("begin_enumeration_snapshot must not error");
        let mut forged = page1
            .next_cursor
            .expect("5 items at page_size=1 must yield a next_cursor");
        forged.last_ordinal += 3; // try to skip ahead without paying for the pages in between

        let result = handle.rt.block_on(selection_repo::fetch_enumeration_page(
            &handle.retrieval,
            tenant_id,
            &forged,
            1,
            MAC_KEY,
        ));
        assert!(
            matches!(
                result,
                Err(SelectionRepoError::Cursor(CursorError::InvalidMac))
            ),
            "expected InvalidMac rejection, got {result:?}"
        );
    });
}
