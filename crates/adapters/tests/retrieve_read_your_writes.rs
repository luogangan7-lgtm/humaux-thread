//! T3.8 (§15.5) integration test — `retrieve::recall_with_overlay` against a real Postgres.
//!
//! Some focused read-side cases seed `private.evidence_objects` / `private.events` /
//! `projection.stream_log` / `ops.outbox` directly with SQL through the admin connection
//! instead — exactly the task brief's documented fallback, and every seeded row matches the
//! shape `remember()` (§60) actually produces (verified against `crates/adapters/src/
//! remember.rs` directly, not guessed), so the assertions below exercise the real read path
//! even though the write path is simulated.
//!
//! Three-state skip (§79.2): no `HUMAUX_TEST_PG_DSN`, an unreachable DB, or the migrations
//! through `0046_stream_log_evidence_id_via_outbox.sql` not yet applied all print a visible
//! SKIP and return, never a false pass.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use humaux_adapters::forget_repo;
use humaux_adapters::postgres::{MaintenanceDbPool, RuntimeDbPool};
use humaux_adapters::read_materialize::{self, MaterializedItem};
use humaux_adapters::remember::{self, RememberCommand};
use humaux_adapters::retrieve::{self, ProcessingState, RetrieveError, TokenClaims};
use humaux_domain::evidence::{EvidenceOriginClass, payload_sha256};
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_projection::serving::StreamFamily;
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

fn dsn_with_application_name(role_dsn: &str, application_name: &str) -> String {
    format!("{role_dsn}&application_name={application_name}")
}

fn dsn_with_application_name_and_statement_timeout(
    role_dsn: &str,
    application_name: &str,
) -> String {
    // `dsn_as_role` ends its libpq options value with role_gateway. Keep that role and add a
    // server-enforced bound, so the test never waits forever before joining its actor thread.
    format!(
        "{role_dsn}%20-c%20statement_timeout%3D3000&connect_timeout=2&application_name={application_name}"
    )
}

fn verified_maintenance_pool(
    rt: &tokio::runtime::Runtime,
) -> Result<MaintenanceDbPool, DbFixtureSkipReason> {
    let dsn = std::env::var("HUMAUX_MAINTENANCE_PG_DSN")
        .map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
    let mut probe = Client::connect(&dsn, NoTls)
        .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
    let role_ok: bool = probe
        .query_one(
            "SELECT current_user='role_maintenance' AND session_user='role_maintenance' \
             AND NOT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user)",
            &[],
        )
        .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
        .get(0);
    if !role_ok {
        return Err(DbFixtureSkipReason::IsolationSetupFailed(
            "HUMAUX_MAINTENANCE_PG_DSN is not a non-bypass role_maintenance LOGIN".to_string(),
        ));
    }
    rt.block_on(MaintenanceDbPool::connect(&dsn))
        .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))
}

const SCOPE_KIND: &str = "tenant";
const DOMAIN: &str = "knowledge";
const PROJECTION_KIND: &str = "ingest";
const PROJECTION_VERSION: &str = "v1";

struct Handle {
    rt: tokio::runtime::Runtime,
    gateway: RuntimeDbPool,
    maintenance: MaintenanceDbPool,
    gateway_dsn: String,
    admin: Client,
    tenant_id: Uuid,
    user_id: Uuid,
    auth: AuthorizationScope,
    family: StreamFamily,
    reasoning_domain_id: Uuid,
    extra_user_ids: Vec<Uuid>,
    workspace_ids: Vec<Uuid>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            if std::thread::panicking() {
                eprintln!("RYW fixture cleanup failed during the original panic: {error}");
            } else {
                panic!("RYW fixture cleanup must succeed: {error}");
            }
        }
    }
}

impl Handle {
    fn cleanup(&mut self) -> Result<(), postgres::Error> {
        // A rejected worker transition may leave the owner's explicit transaction aborted.
        // Roll it back before deleting only this fixture's recorded identities and rows.
        self.admin.batch_execute(&format!(
            "ROLLBACK; \
             DELETE FROM private.artifacts WHERE artifact_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{0}'); \
             DELETE FROM private.memory_evidence WHERE evidence_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{0}'); \
             DELETE FROM private.memory_records WHERE tenant_id = '{0}'; \
             DELETE FROM ops.outbox WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_log WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_checkpoints WHERE tenant_id = '{0}'; \
             DELETE FROM private.events WHERE event_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{0}'); \
             DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
             DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}'; \
             DELETE FROM control.memberships WHERE tenant_id = '{0}';",
            self.tenant_id
        ))?;
        for workspace_id in &self.workspace_ids {
            self.admin.execute(
                "DELETE FROM control.workspaces WHERE workspace_id=$1",
                &[workspace_id],
            )?;
        }
        for user_id in &self.extra_user_ids {
            self.admin
                .execute("DELETE FROM control.users WHERE user_id=$1", &[user_id])?;
        }
        self.admin.execute(
            "DELETE FROM control.users WHERE user_id=$1",
            &[&self.user_id],
        )?;
        self.admin.execute(
            "DELETE FROM control.tenants WHERE tenant_id=$1",
            &[&self.tenant_id],
        )?;
        Ok(())
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

        let user_id: Uuid = admin
            .query_one(
                "INSERT INTO control.users(state) VALUES ('ACTIVE') RETURNING user_id",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        admin
            .execute(
                "INSERT INTO control.memberships(tenant_id,user_id,role,state) \
                 VALUES($1,$2,'member','ACTIVE')",
                &[&tenant_id, &user_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let auth = AuthorizationScope::new(
            TenantId(tenant_id),
            PrincipalId(user_id),
            Some(UserId(user_id)),
            BoundedSet::<WorkspaceId>::new([]).map_err(|e| {
                DbFixtureSkipReason::IsolationSetupFailed(format!("auth scope: {e:?}"))
            })?,
        );
        let family = StreamFamily::new(
            TenantId(tenant_id),
            SCOPE_KIND,
            tenant_id,
            DOMAIN,
            PROJECTION_KIND,
        );

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
        let gateway_dsn = dsn_as_role(&dsn, "role_gateway");
        let gateway = rt
            .block_on(RuntimeDbPool::connect(&gateway_dsn))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let maintenance = verified_maintenance_pool(&rt)?;

        Ok(Handle {
            rt,
            gateway,
            maintenance,
            gateway_dsn,
            admin,
            tenant_id,
            user_id,
            auth,
            family,
            reasoning_domain_id,
            extra_user_ids: Vec::new(),
            workspace_ids: Vec::new(),
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

    handle
        .admin
        .execute(
            "INSERT INTO projection.stream_checkpoints \
               (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version) \
             VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",
            &[
                &handle.tenant_id,
                &SCOPE_KIND,
                &handle.tenant_id,
                &DOMAIN,
                &PROJECTION_KIND,
                &PROJECTION_VERSION,
            ],
        )
        .expect("register seed stream version");

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

fn issued_token_claims(handle: &mut Handle, stream_seq: i64) -> TokenClaims {
    let mut claims = token_claims(handle, stream_seq);
    claims.commit_seq = handle
        .admin
        .query_one(
            "SELECT commit_seq FROM projection.stream_log \
             WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
               AND projection_kind=$5 AND projection_version=$6 AND stream_seq=$7",
            &[
                &handle.tenant_id,
                &SCOPE_KIND,
                &handle.tenant_id,
                &DOMAIN,
                &PROJECTION_KIND,
                &PROJECTION_VERSION,
                &stream_seq,
            ],
        )
        .expect("read exact issued commit_seq")
        .get(0);
    claims
}

fn remember_command(
    handle: &Handle,
    content: &str,
    consistency_token_expires_at: OffsetDateTime,
) -> RememberCommand {
    RememberCommand {
        tenant_id: handle.tenant_id,
        authorization_user_id: Some(handle.user_id),
        scope_kind: SCOPE_KIND.to_string(),
        scope_id: handle.tenant_id,
        domain: DOMAIN.to_string(),
        projection_kind: PROJECTION_KIND.to_string(),
        projection_version: PROJECTION_VERSION.to_string(),
        // Test policy input, not a production adapter default.
        consistency_token_expires_at,
        batch_id: None,
        payload_sha256: payload_sha256(content.as_bytes()),
        data_class: "INTERNAL".to_string(),
        origin_class: EvidenceOriginClass::DirectUserInput,
        origin_principal_id: None,
        origin_connector_id: None,
        visibility_class: "TENANT_SHARED".to_string(),
        visibility_user_id: None,
        visibility_workspace_id: None,
        reasoning_domain_id: handle.reasoning_domain_id,
        occurred_at: None,
        event_kind: "USER_MESSAGE".to_string(),
        event_payload: serde_json::json!({ "content": content }),
    }
}

fn remember_durable_counts(handle: &mut Handle) -> (i64, i64, i64, i64) {
    let row = handle
        .admin
        .query_one(
            "SELECT \
               (SELECT count(*) FROM private.evidence_objects WHERE tenant_id = $1), \
               (SELECT count(*) FROM private.events AS event \
                  JOIN private.evidence_objects AS evidence ON evidence.evidence_id = event.event_id \
                 WHERE evidence.tenant_id = $1), \
               (SELECT count(*) FROM projection.stream_log WHERE tenant_id = $1), \
               (SELECT count(*) FROM ops.outbox WHERE tenant_id = $1)",
            &[&handle.tenant_id],
        )
        .expect("count durable remember rows");
    (row.get(0), row.get(1), row.get(2), row.get(3))
}

fn wait_for_checkpoint_lock_wait(
    admin: &mut Client,
    application_name: &str,
    holder_backend_pid: i32,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let waiting: bool = admin
            .query_one(
                "SELECT EXISTS ( \
                   SELECT 1 \
                     FROM pg_stat_activity AS actor \
                     JOIN pg_locks AS actor_wait \
                       ON actor_wait.pid = actor.pid \
                      AND actor_wait.locktype = 'transactionid' \
                      AND NOT actor_wait.granted \
                     JOIN pg_locks AS holder_xid \
                       ON holder_xid.pid = $2 \
                      AND holder_xid.locktype = 'transactionid' \
                      AND holder_xid.granted \
                      AND holder_xid.transactionid = actor_wait.transactionid \
                    WHERE actor.application_name = $1 \
                      AND actor.wait_event_type = 'Lock' \
                      AND actor.wait_event = 'transactionid' \
                 )",
                &[&application_name, &holder_backend_pid],
            )
            .map_err(|error| format!("inspect remember checkpoint wait: {error}"))?
            .get(0);
        if waiting {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "remember actor {application_name} never waited for the held checkpoint row within 5s"
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn mark_stream_done_as_retrieval_worker(handle: &mut Handle, evidence_id: Uuid) {
    handle
        .admin
        .batch_execute(&format!(
            "BEGIN; \
             SET LOCAL ROLE role_retrieval_worker; \
             SET LOCAL humaux.tenant_id = '{tenant}'; \
             UPDATE projection.stream_log SET state = 'DONE' \
              WHERE tenant_id = '{tenant}' AND commit_seq = ( \
                SELECT commit_seq FROM ops.outbox WHERE evidence_id = '{evidence_id}'); \
             COMMIT;",
            tenant = handle.tenant_id,
        ))
        .expect("production worker role must make its legal state transition");
}

fn wait_for_outbox_relation_lock_wait(
    admin: &mut Client,
    application_name: &str,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let waiting: bool = admin
            .query_one(
                "SELECT EXISTS ( \
                   SELECT 1 FROM pg_stat_activity AS actor \
                   JOIN pg_locks AS lock ON lock.pid = actor.pid \
                    WHERE actor.application_name = $1 \
                      AND actor.wait_event_type = 'Lock' \
                      AND lock.locktype = 'relation' \
                      AND lock.relation = 'ops.outbox'::regclass \
                      AND NOT lock.granted
                 )",
                &[&application_name],
            )
            .map_err(|error| format!("inspect recall outbox relation wait: {error}"))?
            .get(0);
        if waiting {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "recall actor {application_name} never waited on ops.outbox within 5s"
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn advance_stream_highwater_as_retrieval_worker(handle: &mut Handle, commit_seq: i64) {
    handle
        .admin
        .batch_execute(&format!(
            "BEGIN; \
             SET LOCAL ROLE role_retrieval_worker; \
             SET LOCAL humaux.tenant_id = '{tenant}'; \
             UPDATE projection.stream_log SET state = 'DONE' \
              WHERE tenant_id = '{tenant}' AND commit_seq = {commit_seq}; \
             UPDATE projection.stream_checkpoints SET projection_highwater = 1 \
              WHERE tenant_id = '{tenant}' AND scope_kind = '{scope_kind}' \
                AND scope_id = '{tenant}' AND domain = '{domain}' \
                AND projection_kind = '{projection_kind}' AND projection_version = '{version}'; \
             COMMIT;",
            tenant = handle.tenant_id,
            scope_kind = SCOPE_KIND,
            domain = DOMAIN,
            projection_kind = PROJECTION_KIND,
            version = PROJECTION_VERSION,
        ))
        .expect("production retrieval worker must advance stream state and highwater");
}

fn tombstone_stream(handle: &Handle, key: &StreamKey, stream_seq: i64) {
    let seq = u64::try_from(stream_seq).expect("fixture stream sequence is nonnegative");
    let tombstoned = handle
        .rt
        .block_on(forget_repo::tombstone(&handle.maintenance, key, seq))
        .expect("role_maintenance tombstone adapter call");
    assert!(tombstoned, "the seeded stream row must transition once");
}

/// Regression: `remember()` must return the exact opaque token `recall_with_overlay` accepts;
/// no test-created token is permitted on this path.
#[test]
fn remember_returned_token_is_accepted_by_recall() {
    run_db_fixture::<RetrieveFixture, _>(
        "remember_returned_token_is_accepted_by_recall",
        |mut handle| {
            let expires_at = OffsetDateTime::now_utc() + std::time::Duration::from_secs(300);
            let accepted = handle
                .rt
                .block_on(remember::remember(
                    &handle.gateway,
                    remember_command(&handle, "remember-to-recall token interop", expires_at),
                ))
                .expect("real remember must accept the Evidence");

            let envelope = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &accepted.consistency_token,
                    &handle.auth,
                    &handle.family,
                ))
                .expect("the token returned by real remember must be accepted by recall");

            assert!(!envelope.served_by_projection);
            assert_eq!(envelope.overlay.len(), 1);
            assert_eq!(envelope.overlay[0].evidence_id, accepted.evidence_id);
            assert_eq!(
                envelope.overlay[0].processing_state,
                ProcessingState::Issued
            );

            mark_stream_done_as_retrieval_worker(&mut handle, accepted.evidence_id);
            let processed = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &accepted.consistency_token,
                    &handle.auth,
                    &handle.family,
                ))
                .expect("same returned token remains valid after processing");
            assert_eq!(processed.overlay[0].processing_state, ProcessingState::Done);
            assert_eq!(processed.contiguous_done_prefix, 1);
        },
    );
}

fn workspace_stream(handle: &mut Handle, name: &str) -> (Uuid, AuthorizationScope, StreamFamily) {
    let workspace_id: Uuid = handle
        .admin
        .query_one(
            "INSERT INTO control.workspaces(tenant_id,name) VALUES($1,$2) RETURNING workspace_id",
            &[&handle.tenant_id, &name],
        )
        .expect("create real workspace")
        .get(0);
    handle.workspace_ids.push(workspace_id);
    let authorization = auth_for(handle.tenant_id, handle.user_id, [workspace_id]);
    let family = StreamFamily::new(
        TenantId(handle.tenant_id),
        "workspace",
        workspace_id,
        DOMAIN,
        PROJECTION_KIND,
    );
    (workspace_id, authorization, family)
}

fn add_active_tenant_member(handle: &mut Handle) -> Uuid {
    let user_id: Uuid = handle
        .admin
        .query_one(
            "INSERT INTO control.users(state) VALUES ('ACTIVE') RETURNING user_id",
            &[],
        )
        .expect("create other active user")
        .get(0);
    handle.extra_user_ids.push(user_id);
    handle
        .admin
        .execute(
            "INSERT INTO control.memberships(tenant_id,user_id,role,state) VALUES($1,$2,'member','ACTIVE')",
            &[&handle.tenant_id, &user_id],
        )
        .expect("make other user an active tenant member");
    user_id
}

fn make_artifact_without_event(handle: &mut Handle, evidence_id: Uuid) {
    handle
        .admin
        .execute(
            "DELETE FROM private.events WHERE event_id=$1",
            &[&evidence_id],
        )
        .expect("remove event body from artifact fixture");
    handle
        .admin
        .execute(
            "UPDATE private.evidence_objects SET data_class='INTERNAL', evidence_kind='ARTIFACT' WHERE evidence_id=$1",
            &[&evidence_id],
        )
        .expect("convert isolated fixture Evidence to artifact identity");
    handle
        .admin
        .execute(
            "INSERT INTO private.artifacts (artifact_id, artifact_kind, object_locator) VALUES ($1, 'TEXT', 's3://must-not-leak')",
            &[&evidence_id],
        )
        .expect("insert artifact locator only in owner fixture");
}

/// A workspace stream's token carries the workspace routing identity even for USER_PRIVATE
/// Evidence, whose stored visibility workspace must stay NULL. The other active workspace
/// member proves that the token mapping does not replace Evidence visibility enforcement.
#[test]
fn workspace_stream_private_evidence_token_routes_and_stays_private() {
    run_db_fixture::<RetrieveFixture, _>(
        "workspace_stream_private_evidence_token_routes_and_stays_private",
        |mut handle| {
            let (workspace_id, authorization, family) =
                workspace_stream(&mut handle, "remember-private");
            let mut command = remember_command(
                &handle,
                "workspace private token interop",
                OffsetDateTime::now_utc() + std::time::Duration::from_secs(300),
            );
            command.scope_kind = "workspace".to_string();
            command.scope_id = workspace_id;
            command.visibility_class = "USER_PRIVATE".to_string();
            command.visibility_user_id = Some(handle.user_id);

            let accepted = handle
                .rt
                .block_on(remember::remember(&handle.gateway, command))
                .expect("authenticated owner may write private Evidence to workspace stream");
            let claims = retrieve::decode_consistency_token(&accepted.consistency_token)
                .expect("returned token decodes");
            assert_eq!(claims.scope_kind, "workspace");
            assert_eq!(claims.scope_id, workspace_id);
            assert_eq!(claims.workspace_id, Some(workspace_id));

            let row = handle
                .admin
                .query_one(
                    "SELECT visibility_class, visibility_user_id, visibility_workspace_id \
                     FROM private.evidence_objects WHERE evidence_id=$1",
                    &[&accepted.evidence_id],
                )
                .expect("read stored Evidence visibility");
            let class: String = row.get(0);
            let user_id: Option<Uuid> = row.get(1);
            let visibility_workspace_id: Option<Uuid> = row.get(2);
            assert_eq!(class, "USER_PRIVATE");
            assert_eq!(user_id, Some(handle.user_id));
            assert_eq!(visibility_workspace_id, None);

            let owner = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &accepted.consistency_token,
                    &authorization,
                    &family,
                ))
                .expect("workspace token returned to owner must recall");
            assert_eq!(owner.overlay.len(), 1);
            assert_eq!(owner.overlay[0].evidence_id, accepted.evidence_id);
            let owner_bodies = handle
                .rt
                .block_on(read_materialize::materialize_final_bodies(
                    &handle.gateway,
                    &authorization,
                    &family,
                    owner.validated_stream_key(),
                    &[],
                    &owner.overlay,
                ))
                .expect("owner materializes the workspace-routed private event");
            assert!(owner_bodies.items.iter().any(|item| matches!(
                item,
                MaterializedItem::TemporaryEvidence { evidence_id, .. }
                    if *evidence_id == accepted.evidence_id
            )));

            let other_user = add_active_tenant_member(&mut handle);
            let other_authorization = auth_for(handle.tenant_id, other_user, [workspace_id]);
            let other = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &accepted.consistency_token,
                    &other_authorization,
                    &family,
                ))
                .expect("authorized workspace route is valid for another tenant member");
            assert!(
                other.overlay.is_empty(),
                "USER_PRIVATE Evidence must stay hidden after valid workspace token routing"
            );
            let other_bodies = handle
                .rt
                .block_on(read_materialize::materialize_final_bodies(
                    &handle.gateway,
                    &other_authorization,
                    &family,
                    owner.validated_stream_key(),
                    &[],
                    &owner.overlay,
                ))
                .expect("the narrower workspace route must not widen private body access");
            assert!(other_bodies.items.is_empty());
        },
    );
}

#[test]
fn remember_rejects_expired_policy_without_publishing_a_token() {
    run_db_fixture::<RetrieveFixture, _>(
        "remember_rejects_expired_policy_without_publishing_a_token",
        |mut handle| {
            let before: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM private.evidence_objects WHERE tenant_id = $1",
                    &[&handle.tenant_id],
                )
                .expect("count before")
                .get(0);
            let err = handle
                .rt
                .block_on(remember::remember(
                    &handle.gateway,
                    remember_command(
                        &handle,
                        "must not publish",
                        OffsetDateTime::now_utc() - std::time::Duration::from_secs(1),
                    ),
                ))
                .expect_err("expired policy must reject before a token can be returned");
            assert!(matches!(
                err,
                remember::RememberError::ConsistencyTokenExpiryNotFuture
            ));
            let after: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM private.evidence_objects WHERE tenant_id = $1",
                    &[&handle.tenant_id],
                )
                .expect("count after")
                .get(0);
            assert_eq!(
                after, before,
                "rejected remember has no durable side effects"
            );
        },
    );
}

/// A policy deadline can pass after the fast check while `remember()` is blocked on its real
/// checkpoint write. The late issuance check must then roll back every prior transaction write.
#[test]
fn remember_lock_wait_past_deadline_rolls_back_all_writes() {
    run_db_fixture::<RetrieveFixture, _>(
        "remember_lock_wait_past_deadline_rolls_back_all_writes",
        |mut handle| {
            handle
                .admin
                .execute(
                    "INSERT INTO projection.stream_checkpoints \
                       (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version) \
                     VALUES ($1, $2, $3, $4, $5, $6)",
                    &[
                        &handle.tenant_id,
                        &SCOPE_KIND,
                        &handle.tenant_id,
                        &DOMAIN,
                        &PROJECTION_KIND,
                        &PROJECTION_VERSION,
                    ],
                )
                .expect("seed checkpoint row for the real row-lock barrier");
            let before = remember_durable_counts(&mut handle);

            let expires_at = OffsetDateTime::now_utc() + std::time::Duration::from_secs(2);
            let command = remember_command(&handle, "late checkpoint write", expires_at);
            let application_name = format!("remember-expiry-{}", Uuid::new_v4().simple());
            let actor_dsn = dsn_with_application_name(&handle.gateway_dsn, &application_name);

            let mut holder = Client::connect(&handle.gateway_dsn, NoTls)
                .expect("connect role_gateway checkpoint lock holder");
            let holder_backend_pid: i32 = holder
                .query_one("SELECT pg_backend_pid()", &[])
                .expect("read checkpoint holder backend pid")
                .get(0);
            let mut holder = holder.transaction().expect("begin checkpoint lock holder");
            holder
                .batch_execute(&format!(
                    "SET LOCAL humaux.tenant_id = '{}'",
                    handle.tenant_id
                ))
                .expect("bind checkpoint holder to this tenant");
            holder
                .query_one(
                    "SELECT 1 FROM projection.stream_checkpoints \
                      WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
                        AND projection_kind = $5 AND projection_version = $6 \
                      FOR UPDATE",
                    &[
                        &handle.tenant_id,
                        &SCOPE_KIND,
                        &handle.tenant_id,
                        &DOMAIN,
                        &PROJECTION_KIND,
                        &PROJECTION_VERSION,
                    ],
                )
                .expect("hold the exact checkpoint row");

            let actor = std::thread::spawn(move || -> Result<_, String> {
                let runtime = tokio::runtime::Runtime::new()
                    .map_err(|error| format!("create remember actor runtime: {error}"))?;
                let pool = runtime
                    .block_on(RuntimeDbPool::connect(&actor_dsn))
                    .map_err(|error| format!("connect remember actor pool: {error}"))?;
                Ok(runtime.block_on(remember::remember(&pool, command)))
            });

            let wait_result = wait_for_checkpoint_lock_wait(
                &mut handle.admin,
                &application_name,
                holder_backend_pid,
            );
            while OffsetDateTime::now_utc() < expires_at {
                std::thread::sleep(Duration::from_millis(10));
            }
            let release_result = holder.rollback();
            let actor_result = actor.join();

            assert!(wait_result.is_ok(), "{wait_result:?}");
            release_result.expect("release checkpoint row after the deadline");
            let result = actor_result
                .expect("remember actor must join")
                .expect("remember actor setup must succeed");
            assert!(matches!(
                result,
                Err(remember::RememberError::ConsistencyTokenExpiryNotFuture)
            ));
            assert_eq!(
                remember_durable_counts(&mut handle),
                before,
                "the late rejection must commit no Evidence, event, stream row, or outbox row"
            );
        },
    );
}

#[test]
fn fixture_cleanup_rolls_back_an_aborted_owner_transaction() {
    run_db_fixture::<RetrieveFixture, _>(
        "fixture_cleanup_rolls_back_an_aborted_owner_transaction",
        |mut handle| {
            seed_evidence_and_stream_row(&mut handle, 1, "ISSUED");
            handle
                .admin
                .batch_execute("BEGIN; SELECT 1 / 0")
                .expect_err("the fixture owner transaction must be aborted");
            handle
                .cleanup()
                .expect("cleanup must recover the transaction");
            let remaining: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM control.tenants WHERE tenant_id=$1",
                    &[&handle.tenant_id],
                )
                .expect("owner remains usable after cleanup")
                .get(0);
            assert_eq!(remaining, 0, "the fixture tenant must actually be removed");
        },
    );
}

/// The relation lock sits only on the final overlay query: before the actor waits it has
/// already validated the issued token and read serving/highwater/prefix in its RR transaction.
/// A second owner connection then advances state/highwater. The released actor must retain its
/// old snapshot while a fresh request observes the advanced projection.
#[test]
fn recall_overlay_decision_keeps_token_and_projection_reads_in_one_rr_snapshot() {
    run_db_fixture::<RetrieveFixture, _>(
        "recall_overlay_decision_keeps_token_and_projection_reads_in_one_rr_snapshot",
        |mut handle| {
            let evidence_id = seed_evidence_and_stream_row(&mut handle, 1, "ISSUED");
            handle
                .admin
                .execute(
                    "UPDATE projection.stream_checkpoints SET serving = true \
                      WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 \
                        AND domain = $4 AND projection_kind = $5 AND projection_version = $6",
                    &[
                        &handle.tenant_id,
                        &SCOPE_KIND,
                        &handle.tenant_id,
                        &DOMAIN,
                        &PROJECTION_KIND,
                        &PROJECTION_VERSION,
                    ],
                )
                .expect("activate seeded serving version at highwater zero");
            let claims = issued_token_claims(&mut handle, 1);
            let commit_seq = claims.commit_seq;
            let token = retrieve::issue_consistency_token(&claims);
            let actor_token = token.clone();
            let application_name = format!("recall-rr-{}", Uuid::new_v4().simple());
            let actor_dsn = dsn_with_application_name_and_statement_timeout(
                &handle.gateway_dsn,
                &application_name,
            );
            let auth = handle.auth.clone();
            let family = handle.family.clone();
            let (done_tx, done_rx) = mpsc::sync_channel(1);
            let mut holder = Client::connect(
                &std::env::var("HUMAUX_TEST_PG_DSN").expect("test owner DSN is present"),
                NoTls,
            )
            .expect("connect owner outbox lock holder");
            let mut holder_txn = holder
                .transaction()
                .expect("begin owner outbox lock holder");
            holder_txn
                .batch_execute("LOCK TABLE ops.outbox IN ACCESS EXCLUSIVE MODE")
                .expect("hold only final overlay relation");
            let actor = std::thread::spawn(move || {
                let outcome = (|| -> Result<_, String> {
                    let runtime = tokio::runtime::Runtime::new()
                        .map_err(|error| format!("create recall actor runtime: {error}"))?;
                    let pool = runtime
                        .block_on(RuntimeDbPool::connect(&actor_dsn))
                        .map_err(|error| format!("connect recall actor pool: {error}"))?;
                    Ok(runtime.block_on(retrieve::recall_with_overlay(
                        &pool,
                        &actor_token,
                        &auth,
                        &family,
                    )))
                })();
                let _ = done_tx.send(outcome);
            });

            let wait = wait_for_outbox_relation_lock_wait(&mut handle.admin, &application_name);
            advance_stream_highwater_as_retrieval_worker(&mut handle, commit_seq);
            let release = holder_txn.rollback();
            let completion = done_rx.recv_timeout(Duration::from_secs(5));
            let joined = actor.join();

            assert!(wait.is_ok(), "{wait:?}");
            assert!(
                release.is_ok(),
                "release final-overlay relation lock: {release:?}"
            );
            assert!(joined.is_ok(), "recall actor must not panic");
            let old = completion
                .expect("recall actor must finish after lock release")
                .expect("recall actor setup must succeed")
                .expect("snapshot recall must succeed");
            assert!(!old.served_by_projection);
            assert_eq!(old.contiguous_done_prefix, 0);
            assert_eq!(old.overlay.len(), 1);
            assert_eq!(old.overlay[0].evidence_id, evidence_id);
            assert_eq!(old.overlay[0].processing_state, ProcessingState::Issued);

            let fresh = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    &handle.auth,
                    &handle.family,
                ))
                .expect("fresh recall after worker advance must succeed");
            assert!(fresh.served_by_projection);
            assert!(fresh.overlay.is_empty());
            assert_eq!(fresh.contiguous_done_prefix, 1);
        },
    );
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
            let claims = issued_token_claims(&mut handle, 1);
            let token = retrieve::issue_consistency_token(&claims);

            let envelope = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    &handle.auth,
                    &handle.family,
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
            let claims = issued_token_claims(&mut handle, 1);
            let token = retrieve::issue_consistency_token(&claims);

            let envelope = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    &handle.auth,
                    &handle.family,
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
    run_db_fixture::<RetrieveFixture, _>("token_used_across_tenant_is_rejected", |handle| {
        let accepted = handle
            .rt
            .block_on(remember::remember(
                &handle.gateway,
                remember_command(
                    &handle,
                    "tenant isolation",
                    OffsetDateTime::now_utc() + std::time::Duration::from_secs(300),
                ),
            ))
            .expect("real remember must accept the Evidence");

        let other_tenant_id = Uuid::new_v4(); // need not exist: the typed auth comparison is first.
        let other_auth = AuthorizationScope::new(
            TenantId(other_tenant_id),
            PrincipalId(handle.user_id),
            Some(UserId(handle.user_id)),
            BoundedSet::<WorkspaceId>::new([]).expect("empty scope"),
        );
        let result = handle.rt.block_on(retrieve::recall_with_overlay(
            &handle.gateway,
            &accepted.consistency_token,
            &other_auth,
            &handle.family,
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
    run_db_fixture::<RetrieveFixture, _>("token_used_across_workspace_is_rejected", |handle| {
        let accepted = handle
            .rt
            .block_on(remember::remember(
                &handle.gateway,
                remember_command(
                    &handle,
                    "workspace isolation",
                    OffsetDateTime::now_utc() + std::time::Duration::from_secs(300),
                ),
            ))
            .expect("real remember must accept the Evidence");

        let mut claims = retrieve::decode_consistency_token(&accepted.consistency_token)
            .expect("real token decodes");
        claims.workspace_id = Some(Uuid::new_v4());
        let malformed_scope_token = retrieve::issue_consistency_token(&claims);
        let result = handle.rt.block_on(retrieve::recall_with_overlay(
            &handle.gateway,
            &malformed_scope_token,
            &handle.auth,
            &handle.family,
        ));

        assert!(
            matches!(result, Err(RetrieveError::CrossWorkspace)),
            "expected CrossWorkspace rejection, got {result:?}"
        );
    });
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
                &handle.auth,
                &handle.family,
            ));
            assert!(matches!(result, Err(RetrieveError::TokenMalformed(_))));
        },
    );
}

/// A syntactically valid token past its explicit policy expiry is rejected before any overlay
/// query. `remember`'s corresponding pre-write rejection is covered above.
#[test]
fn expired_token_is_rejected() {
    run_db_fixture::<RetrieveFixture, _>("expired_token_is_rejected", |handle| {
        let mut claims = token_claims(&handle, 1);
        claims.expires_at = OffsetDateTime::now_utc() - std::time::Duration::from_secs(1);
        let token = retrieve::issue_consistency_token(&claims);
        let result = handle.rt.block_on(retrieve::recall_with_overlay(
            &handle.gateway,
            &token,
            &handle.auth,
            &handle.family,
        ));
        assert!(matches!(result, Err(RetrieveError::TokenExpired)));
    });
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
            let claims = issued_token_claims(&mut handle, 1);
            let token = retrieve::issue_consistency_token(&claims);

            let envelope = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    &handle.auth,
                    &handle.family,
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

fn auth_for(
    tenant_id: Uuid,
    user_id: Uuid,
    workspaces: impl IntoIterator<Item = Uuid>,
) -> AuthorizationScope {
    AuthorizationScope::new(
        TenantId(tenant_id),
        PrincipalId(user_id),
        Some(UserId(user_id)),
        BoundedSet::new(workspaces.into_iter().map(WorkspaceId)).expect("bounded test scope"),
    )
}

/// The same valid token and stream remain usable by another active tenant member, but an
/// Evidence whose real row is USER_PRIVATE must disappear at the Evidence join — RLS and the
/// request's narrower AuthorizationScope agree, rather than the old stream/outbox-only overlay
/// leaking the id to every same-tenant caller.
#[test]
fn same_tenant_other_user_cannot_read_private_overlay_evidence() {
    run_db_fixture::<RetrieveFixture, _>(
        "same_tenant_other_user_cannot_read_private_overlay_evidence",
        |mut handle| {
            let evidence_id = seed_evidence_and_stream_row(&mut handle, 1, "PROCESSING");
            handle
                .admin
                .execute(
                    "UPDATE private.evidence_objects \
                       SET visibility_class='USER_PRIVATE', visibility_user_id=$2 \
                     WHERE evidence_id=$1",
                    &[&evidence_id, &handle.user_id],
                )
                .expect("make seed private to the authenticated user");
            let token = retrieve::issue_consistency_token(&issued_token_claims(&mut handle, 1));
            let owner = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    &handle.auth,
                    &handle.family,
                ))
                .expect("owner may read own private evidence");
            assert_eq!(owner.overlay.len(), 1);

            let other_user: Uuid = handle
                .admin
                .query_one(
                    "INSERT INTO control.users(state) VALUES ('ACTIVE') RETURNING user_id",
                    &[],
                )
                .expect("create other user")
                .get(0);
            handle.extra_user_ids.push(other_user);
            handle
                .admin
                .execute(
                    "INSERT INTO control.memberships(tenant_id,user_id,role,state) \
                     VALUES($1,$2,'member','ACTIVE')",
                    &[&handle.tenant_id, &other_user],
                )
                .expect("same tenant member");
            let other_auth = auth_for(handle.tenant_id, other_user, []);
            let other = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    &other_auth,
                    &handle.family,
                ))
                .expect("same-tenant request itself is valid");
            assert!(
                other.overlay.is_empty(),
                "USER_PRIVATE evidence must not leak"
            );
        },
    );
}

/// A token's `workspace` mapping can only call `AuthorizationScope::narrow`; a real tenant
/// workspace that the authenticated user lacks is rejected before its arbitrary token fields
/// can cause a route or an overlay read.
#[test]
fn unauthorized_workspace_token_is_rejected_by_scope_narrowing() {
    run_db_fixture::<RetrieveFixture, _>(
        "unauthorized_workspace_token_is_rejected_by_scope_narrowing",
        |mut handle| {
            let workspace_id: Uuid = handle
                .admin
                .query_one(
                    "INSERT INTO control.workspaces(tenant_id,name) VALUES($1,'recall-private') \
                     RETURNING workspace_id",
                    &[&handle.tenant_id],
                )
                .expect("create real workspace")
                .get(0);
            handle.workspace_ids.push(workspace_id);
            let mut claims = token_claims(&handle, 1);
            claims.scope_kind = "workspace".to_string();
            claims.scope_id = workspace_id;
            claims.workspace_id = Some(workspace_id);
            let family = StreamFamily::new(
                TenantId(handle.tenant_id),
                "workspace",
                workspace_id,
                DOMAIN,
                PROJECTION_KIND,
            );
            let result = handle.rt.block_on(retrieve::recall_with_overlay(
                &handle.gateway,
                &retrieve::issue_consistency_token(&claims),
                &handle.auth,
                &family,
            ));
            assert!(matches!(result, Err(RetrieveError::CrossWorkspace)));
        },
    );
}

#[test]
fn token_cannot_forge_a_different_trusted_stream_family() {
    run_db_fixture::<RetrieveFixture, _>(
        "token_cannot_forge_a_different_trusted_stream_family",
        |mut handle| {
            let evidence_id = seed_evidence_and_stream_row(&mut handle, 1, "ISSUED");
            let token = retrieve::issue_consistency_token(&issued_token_claims(&mut handle, 1));
            let forged_family = StreamFamily::new(
                TenantId(handle.tenant_id),
                SCOPE_KIND,
                handle.tenant_id,
                DOMAIN,
                "different-projection-kind",
            );
            let result = handle.rt.block_on(retrieve::recall_with_overlay(
                &handle.gateway,
                &token,
                &handle.auth,
                &forged_family,
            ));
            assert!(matches!(result, Err(RetrieveError::UntrustedStreamFamily)));
            let forged_domain_family = StreamFamily::new(
                TenantId(handle.tenant_id),
                SCOPE_KIND,
                handle.tenant_id,
                "different-domain",
                PROJECTION_KIND,
            );
            let domain_result = handle.rt.block_on(retrieve::recall_with_overlay(
                &handle.gateway,
                &token,
                &handle.auth,
                &forged_domain_family,
            ));
            assert!(matches!(
                domain_result,
                Err(RetrieveError::UntrustedStreamFamily)
            ));
            let exists: bool = handle
                .admin
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM private.evidence_objects WHERE evidence_id=$1)",
                    &[&evidence_id],
                )
                .expect("verify fixture still exists")
                .get(0);
            assert!(
                exists,
                "rejection is read-only and cannot alter the evidence"
            );
        },
    );
}

/// Stream sequence and checkpoint registration are independent token checks: a token cannot
/// point at a non-issued sequence or a row whose version was never registered.
#[test]
fn forged_sequence_or_unregistered_version_is_rejected() {
    run_db_fixture::<RetrieveFixture, _>(
        "forged_sequence_or_unregistered_version_is_rejected",
        |mut handle| {
            let evidence_id = seed_evidence_and_stream_row(&mut handle, 1, "ISSUED");
            let issued_claims = issued_token_claims(&mut handle, 1);
            let mut sequence_claims = issued_claims.clone();
            sequence_claims.stream_seq = 2;
            let sequence_result = handle.rt.block_on(retrieve::recall_with_overlay(
                &handle.gateway,
                &retrieve::issue_consistency_token(&sequence_claims),
                &handle.auth,
                &handle.family,
            ));
            assert!(matches!(
                sequence_result,
                Err(RetrieveError::TokenNotIssued)
            ));

            handle
                .admin
                .execute(
                    "UPDATE projection.stream_log SET projection_version='unregistered-v2' \
                     WHERE tenant_id=$1 AND stream_seq=1",
                    &[&handle.tenant_id],
                )
                .expect("make an issued but unregistered-version row");
            let mut version_claims = issued_claims;
            version_claims.projection_version = "unregistered-v2".to_string();
            let version_result = handle.rt.block_on(retrieve::recall_with_overlay(
                &handle.gateway,
                &retrieve::issue_consistency_token(&version_claims),
                &handle.auth,
                &handle.family,
            ));
            assert!(matches!(version_result, Err(RetrieveError::TokenNotIssued)));
            let _ = evidence_id;
        },
    );
}

/// A token for a registered retired version still proves its exact issued row, but must not
/// borrow a newer serving version's highwater: it takes the full PG overlay instead.
#[test]
fn registered_nonserving_old_version_uses_full_pg_overlay() {
    run_db_fixture::<RetrieveFixture, _>(
        "registered_nonserving_old_version_uses_full_pg_overlay",
        |mut handle| {
            let evidence_id = seed_evidence_and_stream_row(&mut handle, 1, "PROCESSING");
            let mut claims = issued_token_claims(&mut handle, 1);
            handle
                .admin
                .execute(
                    "UPDATE projection.stream_checkpoints \
                       SET projection_version='legacy-v0', serving=false, projection_highwater=99 \
                     WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
                       AND projection_kind=$5 AND projection_version=$6",
                    &[
                        &handle.tenant_id,
                        &SCOPE_KIND,
                        &handle.tenant_id,
                        &DOMAIN,
                        &PROJECTION_KIND,
                        &PROJECTION_VERSION,
                    ],
                )
                .expect("retain the issued legacy checkpoint as non-serving");
            handle
                .admin
                .execute(
                    "UPDATE projection.stream_log SET projection_version='legacy-v0' \
                     WHERE tenant_id=$1 AND stream_seq=1",
                    &[&handle.tenant_id],
                )
                .expect("keep the issued row on its registered legacy version");
            handle
                .admin
                .execute(
                    "INSERT INTO projection.stream_checkpoints \
                       (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                        projection_highwater, serving) \
                     VALUES ($1,$2,$3,$4,$5,'v2',1,true)",
                    &[
                        &handle.tenant_id,
                        &SCOPE_KIND,
                        &handle.tenant_id,
                        &DOMAIN,
                        &PROJECTION_KIND,
                    ],
                )
                .expect("register a distinct newer serving version");
            claims.projection_version = "legacy-v0".to_string();

            let envelope = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &retrieve::issue_consistency_token(&claims),
                    &handle.auth,
                    &handle.family,
                ))
                .expect("registered old version remains recallable");
            assert!(!envelope.served_by_projection);
            assert_eq!(envelope.serving_version.as_deref(), Some("v2"));
            assert_eq!(envelope.overlay.len(), 1);
            assert_eq!(envelope.overlay[0].evidence_id, evidence_id);
        },
    );
}

/// A revoked Memory can remain historically linked for provenance, but the fresh overlay must
/// not surface it. A tombstoned stream row likewise remains in the ledger but not in recall.
#[test]
fn revoked_memory_and_tombstoned_evidence_are_finally_filtered() {
    run_db_fixture::<RetrieveFixture, _>(
        "revoked_memory_and_tombstoned_evidence_are_finally_filtered",
        |mut handle| {
            let evidence_id = seed_evidence_and_stream_row(&mut handle, 1, "DONE");
            let memory_id = link_memories_to_evidence(&mut handle, evidence_id, 1)[0];
            let token = retrieve::issue_consistency_token(&issued_token_claims(&mut handle, 1));
            let before_revoke = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    &handle.auth,
                    &handle.family,
                ))
                .expect("capture the originally visible final overlay");
            handle
                .admin
                .execute(
                    "UPDATE private.memory_records SET status='revoked' WHERE memory_id=$1",
                    &[&memory_id],
                )
                .expect("revoke Memory without deleting provenance");
            let after_revoke = handle
                .rt
                .block_on(read_materialize::materialize_final_bodies(
                    &handle.gateway,
                    &handle.auth,
                    &handle.family,
                    before_revoke.validated_stream_key(),
                    &[memory_id],
                    &before_revoke.overlay,
                ))
                .expect("revoked Memory is omitted on final body recheck");
            assert!(after_revoke.items.iter().all(|item| !matches!(
                item,
                MaterializedItem::Memory { memory_id: id, .. } if *id == memory_id
            )));
            assert!(after_revoke.items.iter().any(|item| matches!(
                item,
                MaterializedItem::TemporaryEvidence { evidence_id: id, linked_memory_ids, .. }
                    if *id == evidence_id && linked_memory_ids.is_empty()
            )));

            tombstone_stream(
                &handle,
                before_revoke.validated_stream_key(),
                before_revoke.overlay[0].stream_seq,
            );
            let after_tombstone = handle
                .rt
                .block_on(read_materialize::materialize_final_bodies(
                    &handle.gateway,
                    &handle.auth,
                    &handle.family,
                    before_revoke.validated_stream_key(),
                    &[memory_id],
                    &before_revoke.overlay,
                ))
                .expect("tombstoned evidence is omitted rather than exposed from stale input");
            assert!(after_tombstone.items.is_empty());
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

#[test]
fn final_materialize_returns_real_memory_and_processing_event_bodies() {
    run_db_fixture::<RetrieveFixture, _>(
        "final_materialize_returns_real_memory_and_processing_event_bodies",
        |mut handle| {
            let evidence_id = seed_evidence_and_stream_row(&mut handle, 1, "PROCESSING");
            let memory_id = link_memories_to_evidence(&mut handle, evidence_id, 1)[0];
            let token = retrieve::issue_consistency_token(&issued_token_claims(&mut handle, 1));
            let envelope = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    &handle.auth,
                    &handle.family,
                ))
                .expect("issued row supplies the trusted key and overlay");
            let bodies = handle
                .rt
                .block_on(read_materialize::materialize_final_bodies(
                    &handle.gateway,
                    &handle.auth,
                    &handle.family,
                    envelope.validated_stream_key(),
                    &[memory_id, memory_id],
                    &envelope.overlay,
                ))
                .expect("visible bodies materialize");
            assert!(bodies.snapshot.context_snapshot_seq > 0);
            assert_eq!(bodies.snapshot.snapshot_token_sha256.len(), 64);
            assert!(bodies.items.iter().any(|item| matches!(
                item,
                MaterializedItem::Memory { memory_id: id, content }
                    if *id == memory_id && *content == serde_json::json!({})
            )));
            assert!(bodies.items.iter().any(|item| matches!(
                item,
                MaterializedItem::TemporaryEvidence {
                    evidence_id: id,
                    stream_seq: 1,
                    processing_state: ProcessingState::Processing,
                    payload,
                    linked_memory_ids,
                } if *id == evidence_id
                    && *payload == serde_json::json!({})
                    && linked_memory_ids == &vec![memory_id]
            )));
        },
    );
}

#[test]
fn final_materialize_rechecks_private_evidence_for_the_current_user() {
    run_db_fixture::<RetrieveFixture, _>(
        "final_materialize_rechecks_private_evidence_for_the_current_user",
        |mut handle| {
            let evidence_id = seed_evidence_and_stream_row(&mut handle, 1, "PROCESSING");
            handle
                .admin
                .execute(
                    "UPDATE private.evidence_objects                      SET visibility_class='USER_PRIVATE', visibility_user_id=$2                      WHERE evidence_id=$1",
                    &[&evidence_id, &handle.user_id],
                )
                .expect("make Evidence private");
            let token = retrieve::issue_consistency_token(&issued_token_claims(&mut handle, 1));
            let owner = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    &handle.auth,
                    &handle.family,
                ))
                .expect("owner reads the private overlay");
            let other_user: Uuid = handle
                .admin
                .query_one(
                    "INSERT INTO control.users(state) VALUES ('ACTIVE') RETURNING user_id",
                    &[],
                )
                .expect("create second user")
                .get(0);
            handle.extra_user_ids.push(other_user);
            handle
                .admin
                .execute(
                    "INSERT INTO control.memberships(tenant_id,user_id,role,state)                      VALUES($1,$2,'member','ACTIVE')",
                    &[&handle.tenant_id, &other_user],
                )
                .expect("other active member");
            let other_auth = auth_for(handle.tenant_id, other_user, []);
            let bodies = handle
                .rt
                .block_on(read_materialize::materialize_final_bodies(
                    &handle.gateway,
                    &other_auth,
                    &handle.family,
                    owner.validated_stream_key(),
                    &[],
                    &owner.overlay,
                ))
                .expect("hidden and absent candidates are intentionally indistinguishable");
            assert!(
                bodies.items.is_empty(),
                "a same-tenant peer cannot materialize a private event body"
            );
        },
    );
}

#[test]
fn final_materialize_drops_memory_with_a_hidden_linked_source() {
    run_db_fixture::<RetrieveFixture, _>(
        "final_materialize_drops_memory_with_a_hidden_linked_source",
        |mut handle| {
            let visible_evidence = seed_evidence_and_stream_row(&mut handle, 1, "PROCESSING");
            let memory_id = link_memories_to_evidence(&mut handle, visible_evidence, 1)[0];
            let token = retrieve::issue_consistency_token(&issued_token_claims(&mut handle, 1));
            let envelope = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    &handle.auth,
                    &handle.family,
                ))
                .expect("capture the visible overlay before a second source becomes private");

            let hidden_evidence = seed_evidence_and_stream_row(&mut handle, 2, "PROCESSING");
            let other_user: Uuid = handle
                .admin
                .query_one(
                    "INSERT INTO control.users(state) VALUES ('ACTIVE') RETURNING user_id",
                    &[],
                )
                .expect("create unrelated private owner")
                .get(0);
            handle.extra_user_ids.push(other_user);
            let mut txn = handle
                .admin
                .transaction()
                .expect("begin linked-source mutation");
            txn.execute(
                "UPDATE private.evidence_objects \
                 SET visibility_class='USER_PRIVATE', visibility_user_id=$2, visibility_workspace_id=NULL \
                 WHERE evidence_id=$1",
                &[&hidden_evidence, &other_user],
            )
            .expect("make the second source private to another user");
            txn.execute(
                "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) \
                 VALUES ($1, $2, 'SUPPORTING')",
                &[&memory_id, &hidden_evidence],
            )
            .expect("link the hidden source to the existing Memory");
            txn.commit().expect("commit linked-source mutation");

            let bodies = handle
                .rt
                .block_on(read_materialize::materialize_final_bodies(
                    &handle.gateway,
                    &handle.auth,
                    &handle.family,
                    envelope.validated_stream_key(),
                    &[memory_id],
                    &envelope.overlay,
                ))
                .expect("hidden sources omit their Memory instead of exposing a partial body");
            assert!(bodies.items.iter().all(|item| !matches!(
                item,
                MaterializedItem::Memory { memory_id: id, .. } if *id == memory_id
            )));
            assert!(bodies.items.iter().any(|item| matches!(
                item,
                MaterializedItem::TemporaryEvidence { evidence_id, linked_memory_ids, .. }
                    if *evidence_id == visible_evidence && linked_memory_ids.is_empty()
            )));
        },
    );
}

#[test]
fn final_materialize_binds_overlay_to_the_validated_projection_version() {
    run_db_fixture::<RetrieveFixture, _>(
        "final_materialize_binds_overlay_to_the_validated_projection_version",
        |mut handle| {
            let evidence_id = seed_evidence_and_stream_row(&mut handle, 1, "PROCESSING");
            let token = retrieve::issue_consistency_token(&issued_token_claims(&mut handle, 1));
            let envelope = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    &handle.auth,
                    &handle.family,
                ))
                .expect("issued row supplies a valid overlay");
            let different_version = StreamKey::new(
                TenantId(handle.tenant_id),
                SCOPE_KIND,
                handle.tenant_id,
                DOMAIN,
                PROJECTION_KIND,
                "different-version",
            );
            let bodies = handle
                .rt
                .block_on(read_materialize::materialize_final_bodies(
                    &handle.gateway,
                    &handle.auth,
                    &handle.family,
                    &different_version,
                    &[],
                    &envelope.overlay,
                ))
                .expect("a trusted-family key with another version reveals nothing");
            assert!(
                bodies.items.is_empty(),
                "the same sequence number in a different projection version is not a match"
            );
            assert_eq!(envelope.overlay[0].evidence_id, evidence_id);
        },
    );
}

#[test]
fn final_materialize_excludes_secret_memory_source_and_marks_artifact_unavailable() {
    run_db_fixture::<RetrieveFixture, _>(
        "final_materialize_excludes_secret_memory_source_and_marks_artifact_unavailable",
        |mut handle| {
            let evidence_id = seed_evidence_and_stream_row(&mut handle, 1, "PROCESSING");
            let memory_id = link_memories_to_evidence(&mut handle, evidence_id, 1)[0];
            let token = retrieve::issue_consistency_token(&issued_token_claims(&mut handle, 1));
            let envelope = handle
                .rt
                .block_on(retrieve::recall_with_overlay(
                    &handle.gateway,
                    &token,
                    &handle.auth,
                    &handle.family,
                ))
                .expect("capture visible event before lifecycle changes");

            handle
                .admin
                .execute(
                    "UPDATE private.evidence_objects SET data_class='SECRET_MATERIAL' \
                     WHERE evidence_id=$1",
                    &[&evidence_id],
                )
                .expect("mark source secret");
            let secret = handle
                .rt
                .block_on(read_materialize::materialize_final_bodies(
                    &handle.gateway,
                    &handle.auth,
                    &handle.family,
                    envelope.validated_stream_key(),
                    &[memory_id],
                    &envelope.overlay,
                ))
                .expect("secret source is silently omitted");
            assert!(secret.items.is_empty());

            make_artifact_without_event(&mut handle, evidence_id);
            let artifact = handle
                .rt
                .block_on(read_materialize::materialize_final_bodies(
                    &handle.gateway,
                    &handle.auth,
                    &handle.family,
                    envelope.validated_stream_key(),
                    &[],
                    &envelope.overlay,
                ))
                .expect("artifact has a deliberate unavailable result");
            assert!(artifact.items.iter().any(|item| matches!(
                item,
                MaterializedItem::ArtifactUnavailable { evidence_id: id, .. } if *id == evidence_id
            )));

            let missing_event_id = seed_evidence_and_stream_row(&mut handle, 2, "PROCESSING");
            handle
                .admin
                .execute(
                    "DELETE FROM private.events WHERE event_id=$1",
                    &[&missing_event_id],
                )
                .expect("make a malformed EVENT fixture without a payload row");
            let missing_event = retrieve::OverlayCandidate {
                stream_seq: 2,
                evidence_id: missing_event_id,
                processing_state: ProcessingState::Processing,
                memory_ids: Vec::new(),
            };
            let error = handle
                .rt
                .block_on(read_materialize::materialize_final_bodies(
                    &handle.gateway,
                    &handle.auth,
                    &handle.family,
                    envelope.validated_stream_key(),
                    &[],
                    &[missing_event],
                ));
            assert!(matches!(
                error,
                Err(humaux_domain::error::ErrorCode::Internal)
            ));
        },
    );
}
