//! T3.8 (§15.5) integration test — `retrieve::recall_with_overlay` against a real Postgres.
//!
//! `crate::remember::remember` (T3.2) has landed since this task started, but it signs its
//! own **placeholder** `consistency_token` (that module's own doc: "T3.8's job... may change
//! this format entirely") rather than [`retrieve::issue_consistency_token`] — the two are not
//! interoperable yet (see `retrieve`'s module doc). Calling the real `remember()` here would
//! therefore produce a token this file's own `retrieve::decode_consistency_token` cannot
//! read, so this file seeds `private.evidence_objects` / `private.events` /
//! `projection.stream_log` / `ops.outbox` directly with SQL through the admin connection
//! instead — exactly the task brief's documented fallback, and every seeded row matches the
//! shape `remember()` (§60) actually produces (verified against `crates/adapters/src/
//! remember.rs` directly, not guessed), so the assertions below exercise the real read path
//! even though the write path is simulated.
//!
//! Three-state skip (§79.2): no `HUMAUX_TEST_PG_DSN`, an unreachable DB, or the migrations
//! through `0046_stream_log_evidence_id_via_outbox.sql` not yet applied all print a visible
//! SKIP and return, never a false pass.

use humaux_adapters::postgres::RuntimeDbPool;
use humaux_adapters::retrieve::{self, ProcessingState, RetrieveError, TokenClaims};
use humaux_domain::ids::TenantId;
use humaux_projection::stream::StreamKey;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

/// `options[role]=...` post-connect `SET ROLE` — same technique as `postgres.rs`'s own
/// `#[cfg(test)]` fixtures and `tests/email_outbox.rs` (reproduced here since the helper is
/// private to its defining modules).
fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    // libpq-standard `options=-c role=X` (URL-encoded). The older `options[role]=X` form
    // is silently tolerated by sqlx but rejected outright by rust-postgres ("invalid
    // connection string") — which made every `Client::connect` role fixture skip, and a
    // skip is not a pass (§79.2). This form is verified working on both drivers.
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

const SCOPE_KIND: &str = "tenant";
const DOMAIN: &str = "knowledge";
const PROJECTION_KIND: &str = "ingest";
const PROJECTION_VERSION: &str = "v1";

struct Handle {
    rt: tokio::runtime::Runtime,
    gateway: RuntimeDbPool,
    admin: Client,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup of everything FK-rooted at this run's throwaway tenant (repo
        // CLAUDE.md hard rule ④ — this file never touches a schema/table of its own, only
        // rows it created).
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM private.memory_evidence WHERE evidence_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{0}'); \
             DELETE FROM private.memory_records WHERE tenant_id = '{0}'; \
             DELETE FROM ops.outbox WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_log WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_checkpoints WHERE tenant_id = '{0}'; \
             DELETE FROM private.events WHERE event_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{0}'); \
             DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
             DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}'; \
             DELETE FROM control.tenants WHERE tenant_id = '{0}';",
            self.tenant_id
        ));
    }
}

struct RetrieveFixture;

impl DbIntegrationFixture for RetrieveFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let migrated: bool = admin
            .query_one(
                "SELECT to_regclass('projection.stream_log') IS NOT NULL \
                   AND to_regclass('ops.commit_seq_seq') IS NOT NULL \
                   AND has_table_privilege('role_gateway', 'ops.outbox', 'SELECT')",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "projection.stream_log / ops.commit_seq_seq / role_gateway's SELECT on \
                 ops.outbox not all present — run `cargo xtask migrate` (migrations through \
                 0046_stream_log_evidence_id_via_outbox.sql) against HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }

        let tenant_id = Uuid::new_v4();
        admin
            .execute(
                "INSERT INTO control.tenants (tenant_id, name, state) VALUES ($1, $2, 'ACTIVE')",
                &[&tenant_id, &format!("t3.8-test-{tenant_id}")],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        let reasoning_domain_id = Uuid::new_v4();
        admin
            .execute(
                "INSERT INTO control.private_reasoning_domains (reasoning_domain_id, tenant_id, name) \
                 VALUES ($1, $2, 'default')",
                &[&reasoning_domain_id, &tenant_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let gateway = rt
            .block_on(RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            gateway,
            admin,
            tenant_id,
            reasoning_domain_id,
        })
    }
}

/// Seeds one Evidence (`evidence_objects` + `events`), its `projection.stream_log` row at
/// `stream_seq`, and the matching `ops.outbox` row — the direct-SQL stand-in for
/// `remember()`'s transaction B (see file header). `commit_seq` is drawn from the real
/// `ops.commit_seq_seq` (§60/§15.1's sole generator, `migrations/0043_commit_sequence.sql`),
/// same as `remember::next_commit_seq` — `retrieve::pg_delta_overlay` joins `stream_log` to
/// `outbox` on `(tenant_id, commit_seq)` (see that function's doc for why it is `commit_seq`,
/// not `stream_seq`, that must be genuinely unique here).
fn seed_evidence_and_stream_row(handle: &mut Handle, stream_seq: i64, state: &str) -> Uuid {
    let evidence_id = Uuid::new_v4();
    handle
        .admin
        .execute(
            "INSERT INTO private.evidence_objects \
               (evidence_id, tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, $2, 'EVENT', $3, 'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $4)",
            &[
                &evidence_id,
                &handle.tenant_id,
                &vec![0u8; 32],
                &handle.reasoning_domain_id,
            ],
        )
        .expect("insert evidence_objects");
    handle
        .admin
        .execute(
            "INSERT INTO private.events (event_id, event_kind, payload) \
             VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
            &[&evidence_id],
        )
        .expect("insert events");

    let commit_seq: i64 = handle
        .admin
        .query_one("SELECT nextval('ops.commit_seq_seq')", &[])
        .expect("next commit_seq")
        .get(0);

    let settled = matches!(
        state,
        "DONE" | "SKIPPED_BY_POLICY" | "FAILED" | "TOMBSTONED"
    );
    handle
        .admin
        .execute(
            &format!(
                "INSERT INTO projection.stream_log \
                   (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                    stream_seq, commit_seq, state{settled_col}) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9{settled_val})",
                settled_col = if settled { ", settled_at" } else { "" },
                settled_val = if settled { ", now()" } else { "" },
            ),
            &[
                &handle.tenant_id,
                &SCOPE_KIND,
                &handle.tenant_id, // scope_id: tenant-level scope, scope_id = tenant_id
                &DOMAIN,
                &PROJECTION_KIND,
                &PROJECTION_VERSION,
                &stream_seq,
                &commit_seq,
                &state,
            ],
        )
        .expect("insert stream_log row");

    handle
        .admin
        .execute(
            "INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id) \
             VALUES ($1, $2, $3, 'EVIDENCE_ACCEPTED', $4)",
            &[&handle.tenant_id, &commit_seq, &stream_seq, &evidence_id],
        )
        .expect("insert outbox row");

    evidence_id
}

fn token_claims(handle: &Handle, stream_seq: i64) -> TokenClaims {
    let now = OffsetDateTime::now_utc();
    TokenClaims {
        tenant_id: handle.tenant_id,
        workspace_id: None,
        scope_kind: SCOPE_KIND.to_string(),
        scope_id: handle.tenant_id,
        domain: DOMAIN.to_string(),
        projection_kind: PROJECTION_KIND.to_string(),
        projection_version: PROJECTION_VERSION.to_string(),
        stream_seq,
        commit_seq: stream_seq,
        issued_at: now,
        expires_at: now + std::time::Duration::from_secs(3600),
    }
}

/// §15.5 core acceptance behavior: a `remember()`-shaped write (simulated) that has not yet
/// finished distillation must still show up on the very next `recall` — with
/// `processing_state` set, never with a `memory_ids` entry it does not actually have.
#[test]
fn remember_then_recall_sees_evidence_with_processing_state_not_memory() {
    run_db_fixture::<RetrieveFixture, _>(
        "remember_then_recall_sees_evidence_with_processing_state_not_memory",
        |mut handle| {
            let evidence_id = seed_evidence_and_stream_row(&mut handle, 1, "PROCESSING");
            let claims = token_claims(&handle, 1);
            let token = retrieve::issue_consistency_token(&claims);

            let envelope = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    handle.tenant_id,
                    None,
                ))
                .expect("recall_with_overlay must succeed for a same-tenant token");

            assert!(
                !envelope.served_by_projection,
                "serving has not caught up (no stream_checkpoints row) — overlay must engage"
            );
            assert_eq!(envelope.overlay.len(), 1, "exactly the one seeded Evidence");
            let candidate = &envelope.overlay[0];
            assert_eq!(candidate.evidence_id, evidence_id);
            assert_eq!(candidate.processing_state, ProcessingState::Processing);
            assert!(
                candidate.memory_ids.is_empty(),
                "undistilled Evidence must never masquerade as a finished Memory (§15.5)"
            );
        },
    );
}

/// Same shape, but the ledger row is already `SETTLED_OK` (`DONE`) with no `memory_evidence`
/// link ever created (e.g. distillation legitimately produced zero Memory candidates, §15.5
/// "一次 Evidence 可能产生 0/1/N 条 Memory") — `memory_ids` must stay empty, not be invented.
#[test]
fn settled_evidence_without_memory_link_reports_none_not_fabricated_id() {
    run_db_fixture::<RetrieveFixture, _>(
        "settled_evidence_without_memory_link_reports_none_not_fabricated_id",
        |mut handle| {
            let evidence_id = seed_evidence_and_stream_row(&mut handle, 1, "DONE");
            let claims = token_claims(&handle, 1);
            let token = retrieve::issue_consistency_token(&claims);

            let envelope = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    handle.tenant_id,
                    None,
                ))
                .expect("recall_with_overlay must succeed");

            assert_eq!(envelope.overlay.len(), 1);
            let candidate = &envelope.overlay[0];
            assert_eq!(candidate.evidence_id, evidence_id);
            assert_eq!(candidate.processing_state, ProcessingState::Done);
            assert!(candidate.processing_state.is_settled_ok());
            assert!(candidate.memory_ids.is_empty());
            assert_eq!(
                envelope.contiguous_done_prefix, 1,
                "the one settled row is the whole contiguous prefix"
            );
        },
    );
}

/// §15.5 "不可跨 tenant/workspace 使用" — a token minted for tenant A used against tenant B's
/// request context must be rejected outright (`Err`), and must reach zero rows: the rejection
/// happens before `recall_with_overlay` issues a single query.
#[test]
fn token_used_across_tenant_is_rejected() {
    run_db_fixture::<RetrieveFixture, _>("token_used_across_tenant_is_rejected", |mut handle| {
        seed_evidence_and_stream_row(&mut handle, 1, "PROCESSING");
        let claims = token_claims(&handle, 1);
        let token = retrieve::issue_consistency_token(&claims);

        let other_tenant_id = Uuid::new_v4(); // never inserted into control.tenants — proves
        // the rejection is a pure claims comparison, not something that happened to fail
        // because the other tenant doesn't exist.
        let result = handle.rt.block_on(retrieve::recall_with_overlay(
            &handle.gateway,
            &token,
            other_tenant_id,
            None,
        ));

        assert!(
            matches!(result, Err(RetrieveError::CrossTenant)),
            "expected CrossTenant rejection, got {result:?}"
        );
    });
}

/// Same rule, workspace half: a tenant-shared token (`workspace_id: None`) used against a
/// workspace-scoped request must also reject — not just two different `Some` workspace ids.
#[test]
fn token_used_across_workspace_is_rejected() {
    run_db_fixture::<RetrieveFixture, _>(
        "token_used_across_workspace_is_rejected",
        |mut handle| {
            seed_evidence_and_stream_row(&mut handle, 1, "PROCESSING");
            let claims = token_claims(&handle, 1);
            let token = retrieve::issue_consistency_token(&claims);

            let result = handle.rt.block_on(retrieve::recall_with_overlay(
                &handle.gateway,
                &token,
                handle.tenant_id,
                Some(Uuid::new_v4()),
            ));

            assert!(
                matches!(result, Err(RetrieveError::CrossWorkspace)),
                "expected CrossWorkspace rejection, got {result:?}"
            );
        },
    );
}

/// A malformed/garbage token must never panic and must never be treated as "no overlay
/// needed" — it is a hard decode error.
#[test]
fn garbage_token_is_rejected_not_silently_ignored() {
    run_db_fixture::<RetrieveFixture, _>(
        "garbage_token_is_rejected_not_silently_ignored",
        |handle| {
            let result = handle.rt.block_on(retrieve::recall_with_overlay(
                &handle.gateway,
                "not-a-real-token",
                handle.tenant_id,
                None,
            ));
            assert!(matches!(result, Err(RetrieveError::TokenMalformed(_))));
        },
    );
}

/// Seeds N `private.memory_records` rows and links every one of them to `evidence_id` via
/// `private.memory_evidence` (role `PRIMARY` for the first, `SUPPORTING` for the rest — any
/// valid, non-colliding roles, since the PK is `(memory_id, evidence_id, role)`).
fn link_memories_to_evidence(handle: &mut Handle, evidence_id: Uuid, count: usize) -> Vec<Uuid> {
    let roles = ["PRIMARY", "SUPPORTING", "SUPPORTING", "SUPPORTING"];
    // §8.6's orphan-Memory check is a DEFERRABLE INITIALLY DEFERRED constraint trigger — it
    // only fires at COMMIT, but each memory_records INSERT still needs its memory_evidence
    // link to land in the *same* transaction, or an implicit per-statement autocommit (the
    // `postgres` crate's default outside an explicit `transaction()`) commits the orphan
    // Memory row before its link exists and the trigger rejects it.
    let mut txn = handle.admin.transaction().expect("begin txn");
    let ids: Vec<Uuid> = (0..count)
        .map(|i| {
            let memory_id = Uuid::new_v4();
            txn.execute(
                "INSERT INTO private.memory_records \
                   (memory_id, tenant_id, memory_type, content, visibility_class, \
                    authority_class, confidence, status, asserted_at) \
                 VALUES ($1, $2, 'FACT', '{}'::jsonb, 'TENANT_SHARED', \
                         'PrivateKnowledge', 0.9, 'active', now())",
                &[&memory_id, &handle.tenant_id],
            )
            .expect("insert memory_records");
            txn.execute(
                "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) \
                 VALUES ($1, $2, $3)",
                &[&memory_id, &evidence_id, &roles[i]],
            )
            .expect("insert memory_evidence link");
            memory_id
        })
        .collect();
    txn.commit().expect("commit txn");
    ids
}

/// §15.5 "一次 Evidence 可能产生 0/1/N 条 Memory" — the N branch. Two distinct Memories linked
/// to the same Evidence must surface as ONE `OverlayCandidate` carrying both `memory_id`s, not
/// as two candidates duplicating the same `stream_seq`/`evidence_id` (regression test for the
/// `LEFT JOIN private.memory_evidence ... ON evidence_id` fan-out bug: that join's PK is
/// `(memory_id, evidence_id, role)`, so an ungrouped join returns one row per link).
#[test]
fn evidence_with_two_memories_reports_one_candidate_with_both_ids() {
    run_db_fixture::<RetrieveFixture, _>(
        "evidence_with_two_memories_reports_one_candidate_with_both_ids",
        |mut handle| {
            let evidence_id = seed_evidence_and_stream_row(&mut handle, 1, "DONE");
            let memory_ids = link_memories_to_evidence(&mut handle, evidence_id, 2);
            let claims = token_claims(&handle, 1);
            let token = retrieve::issue_consistency_token(&claims);

            let envelope = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    handle.tenant_id,
                    None,
                ))
                .expect("recall_with_overlay must succeed");

            assert_eq!(
                envelope.overlay.len(),
                1,
                "one Evidence with N memory_evidence links must still be one candidate, not N \
                 duplicates of the same stream_seq/evidence_id"
            );
            let candidate = &envelope.overlay[0];
            assert_eq!(candidate.evidence_id, evidence_id);
            let mut got = candidate.memory_ids.clone();
            got.sort();
            let mut want = memory_ids;
            want.sort();
            assert_eq!(
                got, want,
                "both linked memory_ids must be present on the one candidate"
            );
        },
    );
}

/// Sanity check on [`StreamKey`]/[`TenantId`] reuse (T3.3's `humaux_projection::stream`
/// types) — `TokenClaims::stream_key()` must resolve to the same six-tuple the seeded
/// `stream_log` row carries, not a coincidentally-matching one.
#[test]
fn token_claims_resolve_to_the_seeded_stream_key() {
    run_db_fixture::<RetrieveFixture, _>(
        "token_claims_resolve_to_the_seeded_stream_key",
        |handle| {
            let claims = token_claims(&handle, 5);
            let key: StreamKey = claims.stream_key();
            assert_eq!(key.tenant_id, TenantId(handle.tenant_id));
            assert_eq!(key.scope_kind, SCOPE_KIND);
            assert_eq!(key.scope_id, handle.tenant_id);
            assert_eq!(key.domain, DOMAIN);
            assert_eq!(key.projection_kind, PROJECTION_KIND);
            assert_eq!(key.projection_version, PROJECTION_VERSION);
        },
    );
}
