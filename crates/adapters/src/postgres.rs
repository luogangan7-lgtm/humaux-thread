//! `adapters::postgres` — the sole encapsulation point for `sqlx::PgPool` (§6.2.3).
//! Depends-on: crates=[humaux-domain, humaux-testkit, postgres, sqlx, tokio]; services=[PostgreSQL(any)
//!   r=[private.events, private.ingest_tickets, private.memory_consolidation_inputs, private.memory_evidence,
//!   private.memory_records, projection.stream_checkpoints, projection.stream_log], PostgreSQL(role_batch_issuer),
//!   PostgreSQL(role_consolidation_worker), PostgreSQL(role_gateway), PostgreSQL(role_maintenance),
//!   PostgreSQL(role_private_worker), PostgreSQL(role_public_worker), PostgreSQL(role_retrieval_worker),
//!   PostgreSQL(owner)];
//!   env=[HUMAUX_TEST_PG_DSN]; modules=[domain::error]
//! Called-by: [adapters::affect_repo, adapters::batch, adapters::confirm_token_repo, adapters::consolidate_repo, adapters::consolidation_reasoner, adapters::context_repo, adapters::continuity_read, adapters::continuity_repo, adapters::contribution_entry_repo, adapters::contribution_execution_ingress, adapters::contribution_execution_repo, adapters::contribution_reasoner, adapters::contribution_repo, adapters::credential_repo, adapters::disclosure, adapters::distill_reasoner, adapters::distill_repo, adapters::email::outbox, adapters::exact_census, adapters::forget_repo, adapters::health, adapters::jobs, adapters::maintenance_repo, adapters::mechanism_observation, adapters::membership_repo, adapters::memory_governance_repo, adapters::model_call_ledger, adapters::operation_receipt, adapters::placement_repo, adapters::private_inference_rpc, adapters::private_projection_registry, adapters::projection_worker, adapters::provider_budget, adapters::provisioning, adapters::public_provenance, adapters::public_repo, adapters::quota_repo, adapters::read_materialize, adapters::reasoning_route_admission, adapters::reasoning_route_onboarding, adapters::rebuild, adapters::remember, adapters::request_guard_repo, adapters::retrieval_embedding_rpc, adapters::retrieval_query_source, adapters::retrieve, adapters::role_hygiene, adapters::scheduler, adapters::selection_repo, adapters::serving_repo, adapters::stream_repo, adapters::subject_repo, admin::mechanism, admin::probe, consolidation-worker::inference_client, consolidation-worker::main, gateway::auth, gateway::bootstrap, gateway::context, gateway::continuity, gateway::guard, gateway::mcp_application, gateway::memory, gateway::recall, gateway::retrieval_embedding_client, gateway::status, humaux-consolidation-worker, humaux-private-worker, maintenance::backup, maintenance::drill, maintenance::health_serve, maintenance::main, maintenance::rebuild_cli, maintenance::retention, maintenance::serve, maintenance::roles, private-worker::distill, private-worker::inference_rpc, private-worker::main, private-worker::route_providers, public-worker::main, retrieval-provider::adapters, retrieval-worker::main, retrieval-worker::rpc, tests, xtask::e2e_seed, xtask::mechanism_registry, xtask::member, xtask::projection_serve]
//! Invariants: [the only file that names sqlx::PgPool: eight typed pools, one per role (§6.2.3), each connect checks
//!   current_user and fails with PoolInitError::RoleMismatch on a wrong role; no raw-pool accessor leaves the crate;
//!   the ninth, MigratorDbPool, refuses every §6.2.0 role and any principal without CREATEROLE (ADR-0059 D-E);
//!   RetentionExecutor wraps it and also refuses a non-SUPERUSER session_user (ADR-0063 D-H)]
//! Spec: Baseline §58; §6.2.3; §15.4; §15.2; §6.2.2; ADR-0059; ADR-0063 D-H
//!
//! Spec canonical path is `crates/adapters/postgres/src/pools.rs`; this repo's crate layout
//! (§58) flattens adapters into `crates/adapters/src/<name>.rs` modules, so **this file IS
//! meant to be** that sole encapsulation point — no other file in this workspace is meant to
//! name `sqlx::PgPool` (G6-DB1 assertion B).
//!
//! G80-40's static topology scan is implemented in
//! `xtask/src/architecture_check.rs`; the runtime half remains the `current_user` check and
//! SQL fixtures below. Adding a bare `PgPool` field anywhere outside this file is therefore
//! covered by the architecture gate as well as the compile-time sentinels.
//!
//! Closed set (§6.2.3): eight newtypes, one per role that holds a standing connection pool
//! (four landed with T1.4; T3.3+T3.4 adds `RetrievalWorkerDbPool` / `MaintenanceDbPool`, and
//! Phase 9 adds `PublicWorkerDbPool`; operational observations add read-only `AdminDbPool` —
//! §15.4 `advance_prefix`'s `projection_highwater` write and §15.2's `ISSUED -> LOST` sweep
//! are each the *only* legal writer of their respective column/transition per the §6.2.2
//! grant matrix and `stream_log_guard_state_transition`'s per-`current_user` transition set
//! in `0011_roles_and_grants.sql`, so each needs its own role-scoped pool rather than reusing
//! one of the original four). `inner` is private to this module on every wrapper; none of
//! them implement `From`/`Into` of one another, `Deref<Target = PgPool>`, or `Clone` — a
//! leaked `PgPool` (via any of those) would let a caller run SQL under the wrong role's
//! grants, defeating the entire point of the closed set. Application ports must take
//! `&RuntimeDbPool` / `&BatchIssuerDbPool` / `&ConsolidationDbPool` / `&PrivateWorkerDbPool` /
//! `&RetrievalWorkerDbPool` / `&MaintenanceDbPool` / `&PublicWorkerDbPool` / `&AdminDbPool` by name, never a bare `PgPool` (G6-DB1
//! `tests/ui/pass_*` / `fail_*` fixtures cover the five wrappers currently exposed by typed
//! compile-pass ports; all eight wrappers use the same `connect_checked` construction path.

use std::fmt;

use humaux_domain::error::ErrorCode;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

/// Literal `current_user` a [`RuntimeDbPool`] connection must report (§6.2.3 assertion E).
pub const ROLE_GATEWAY: &str = "role_gateway";
/// Literal `current_user` a [`BatchIssuerDbPool`] connection must report (§6.2.3 assertion E).
pub const ROLE_BATCH_ISSUER: &str = "role_batch_issuer";
/// Literal `current_user` a [`ConsolidationDbPool`] connection must report (§6.2.3 assertion E).
pub const ROLE_CONSOLIDATION_WORKER: &str = "role_consolidation_worker";
/// Literal `current_user` a [`PrivateWorkerDbPool`] connection must report (§6.2.3 assertion E).
pub const ROLE_PRIVATE_WORKER: &str = "role_private_worker";
/// Literal `current_user` a [`RetrievalWorkerDbPool`] connection must report (§6.2.3 assertion E).
pub const ROLE_RETRIEVAL_WORKER: &str = "role_retrieval_worker";
/// Literal `current_user` a [`MaintenanceDbPool`] connection must report (§6.2.3 assertion E).
pub const ROLE_MAINTENANCE: &str = "role_maintenance";
/// Literal `current_user` a [`PublicWorkerDbPool`] connection must report (§6.2.3 assertion E).
pub const ROLE_PUBLIC_WORKER: &str = "role_public_worker";
/// §6.2 read-only operational observation identity (ADR-0011).
pub const ROLE_ADMIN: &str = "role_admin";

/// Construction-time failure for any typed pool (§6.2.3: "不符返回 Err").
#[derive(Debug)]
pub enum PoolInitError {
    /// The DSN could not be reached / authenticated at all.
    Connect(sqlx::Error),
    /// The connection succeeded, but `SELECT current_user` did not match the wrapper's role
    /// literally (§6.2.3 assertion E). Carries both sides so callers can log without a second
    /// round trip.
    RoleMismatch {
        expected: &'static str,
        actual: String,
    },
}

impl fmt::Display for PoolInitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PoolInitError::Connect(e) => write!(f, "postgres pool connect failed: {e}"),
            PoolInitError::RoleMismatch { expected, actual } => write!(
                f,
                "§6.2.3 role mismatch: expected current_user = {expected:?}, got {actual:?}"
            ),
        }
    }
}

impl std::error::Error for PoolInitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PoolInitError::Connect(e) => Some(e),
            PoolInitError::RoleMismatch { .. } => None,
        }
    }
}

/// The *only* place in the crate that constructs a bare [`PgPool`]; every wrapper's `connect`
/// calls through here and then checks the connected identity before handing the pool out.
async fn open(dsn: &str) -> Result<PgPool, PoolInitError> {
    // dep: PostgreSQL(any) — connects to PostgreSQL
    PgPoolOptions::new()
        .connect(dsn)
        .await
        .map_err(PoolInitError::Connect)
}

/// Connects `dsn` and asserts `SELECT current_user` equals `expected_role` literally before
/// handing back a pool (§6.2.3 assertion E).
async fn connect_checked(dsn: &str, expected_role: &'static str) -> Result<PgPool, PoolInitError> {
    let pool = open(dsn).await?;
    let row = sqlx::query("SELECT current_user")
        // dep: PostgreSQL(any) — executes a query against the pool
        .fetch_one(&pool)
        .await
        .map_err(PoolInitError::Connect)?;
    let actual: String = row.try_get(0).map_err(PoolInitError::Connect)?;
    if actual != expected_role {
        return Err(PoolInitError::RoleMismatch {
            expected: expected_role,
            actual,
        });
    }
    Ok(pool)
}

/// `role_gateway`'s standing request-path pool (§6.2.1 row `role_gateway`, 连接池=`request`).
pub struct RuntimeDbPool(PgPool);

impl RuntimeDbPool {
    /// §6.2.3 assertion E: connects and verifies `current_user == "role_gateway"`.
    pub async fn connect(dsn: &str) -> Result<Self, PoolInitError> {
        connect_checked(dsn, ROLE_GATEWAY).await.map(Self)
    }

    /// ADR-0061 D-F readiness: one `SELECT 1` through the checked pool. A pool that can no longer open or
    /// reuse a connection answers the driver's error, which the gateway's `/status` names under `pg`.
    ///
    /// # Errors
    /// The `sqlx` error of the round trip (unreachable server, refused connection, closed pool).
    pub async fn ping(&self) -> Result<(), sqlx::Error> {
        // dep: PostgreSQL(role_gateway) — readiness round trip, no table read
        sqlx::query("SELECT 1").execute(&self.0).await.map(|_| ())
    }

    /// H3 (§74.6) `adapters::email::outbox::enqueue`'s first real reader of `.0` — the
    /// ponytail note this replaces asked for exactly this ("delete this allow once a query
    /// method... reads it"). `pub(crate)` on purpose: only code inside this crate may run
    /// queries through the checked pool; nothing outside `humaux-adapters` gets a `&PgPool`
    /// at all, so G6-DB1's closed set (no cross-role construction/`Deref`/`From`) is
    /// unchanged — this adds a query surface, not a leak of the wrapped value itself.
    pub(crate) fn pool(&self) -> &PgPool {
        &self.0
    }
}

/// `role_batch_issuer`'s independent single-purpose pool (§6.2.0: must not share a pool with
/// any runtime role — sharing would hand request-path connections invoice-issuing power).
pub struct BatchIssuerDbPool(PgPool);

impl BatchIssuerDbPool {
    /// §6.2.3 assertion E: connects and verifies `current_user == "role_batch_issuer"`.
    pub async fn connect(dsn: &str) -> Result<Self, PoolInitError> {
        connect_checked(dsn, ROLE_BATCH_ISSUER).await.map(Self)
    }

    /// T3.2 `batch::begin_batch`'s pool accessor — see [`RuntimeDbPool::pool`]'s doc for why
    /// `pub(crate)` keeps G6-DB1's closed set intact. The `#[allow(dead_code)]` that used to
    /// sit on the struct above is gone: `batch.rs` is that first real reader of `.0`.
    pub(crate) fn pool(&self) -> &PgPool {
        &self.0
    }
}

/// `role_consolidation_worker`'s independent-process pool (§6.2.1 row
/// `role_consolidation_worker`, 连接池=`consolidation（独立进程/独立池）`).
pub struct ConsolidationDbPool(PgPool);

impl ConsolidationDbPool {
    /// §6.2.3 assertion E: connects and verifies `current_user == "role_consolidation_worker"`.
    pub async fn connect(dsn: &str) -> Result<Self, PoolInitError> {
        connect_checked(dsn, ROLE_CONSOLIDATION_WORKER)
            .await
            .map(Self)
    }

    /// T4.6/T4.7 `adapters::consolidate_repo`'s pool accessor — see [`RuntimeDbPool::pool`]'s
    /// doc for why `pub(crate)` keeps G6-DB1's closed set intact. `consolidate_repo.rs` is
    /// this wrapper's first real reader of `.0`.
    pub(crate) fn pool(&self) -> &PgPool {
        &self.0
    }
}

/// `role_private_worker`'s worker pool (§6.2.1 row `role_private_worker`).
pub struct PrivateWorkerDbPool(PgPool);

impl PrivateWorkerDbPool {
    /// §6.2.3 assertion E: connects and verifies `current_user == "role_private_worker"`.
    pub async fn connect(dsn: &str) -> Result<Self, PoolInitError> {
        connect_checked(dsn, ROLE_PRIVATE_WORKER).await.map(Self)
    }

    /// H3 (§74.6) outbox worker's pool accessor — see [`RuntimeDbPool::pool`]'s doc for why
    /// `pub(crate)` keeps G6-DB1's closed set intact.
    pub(crate) fn pool(&self) -> &PgPool {
        &self.0
    }
}

/// `role_retrieval_worker`'s pool (§6.2.1 row `role_retrieval_worker`). Sole legal writer of
/// `projection.stream_checkpoints.{evidence,knowledge,projection}_highwater` and of
/// `projection.stream_log`'s `ISSUED -> {DONE,SKIPPED_BY_POLICY,FAILED}` terminal edges
/// (§6.2.2 grant matrix; `stream_log_guard_state_transition` enforces the latter by
/// `current_user`) — T3.3's `advance_prefix` connects through this wrapper, never
/// `RuntimeDbPool` (which lacks the `projection_highwater` column grant) or
/// `MaintenanceDbPool` (whose transition set doesn't include this edge).
pub struct RetrievalWorkerDbPool(PgPool);

impl RetrievalWorkerDbPool {
    /// §6.2.3 assertion E: connects and verifies `current_user == "role_retrieval_worker"`.
    pub async fn connect(dsn: &str) -> Result<Self, PoolInitError> {
        connect_checked(dsn, ROLE_RETRIEVAL_WORKER).await.map(Self)
    }

    /// T3.3 `humaux_adapters::stream_repo`'s pool accessor — see [`RuntimeDbPool::pool`]'s
    /// doc for why `pub(crate)` keeps G6-DB1's closed set intact.
    pub(crate) fn pool(&self) -> &PgPool {
        &self.0
    }
}

/// `role_maintenance`'s ops-pool (§6.2.1 row `role_maintenance`, 连接池=`ops（repair job）`).
/// Sole legal writer of `projection.stream_log`'s `ISSUED -> LOST` sweep edge (§15.2) and of
/// `retention::tombstone`'s `* -> TOMBSTONED` edge (§37.2) — T3.4's LOST patrol connects
/// through this wrapper. Not a runtime role (§6.2.1's five-name enum excludes it), so it is
/// never handed to a request-path handler.
pub struct MaintenanceDbPool(PgPool);

/// The read-only §4.2 admin identity. It has only the explicit observation SELECT
/// grants in §6.2.2 and cannot inherit a maintenance or runtime writer connection.
pub struct AdminDbPool(PgPool);

impl AdminDbPool {
    /// Connect and verify the actual database role before exposing any admin query.
    pub async fn connect(dsn: &str) -> Result<Self, PoolInitError> {
        connect_checked(dsn, ROLE_ADMIN).await.map(Self)
    }

    /// Internal access only; callers cannot use an admin handle as a writer pool.
    pub(crate) fn pool(&self) -> &PgPool {
        &self.0
    }
}

impl MaintenanceDbPool {
    /// §6.2.3 assertion E: connects and verifies `current_user == "role_maintenance"`.
    pub async fn connect(dsn: &str) -> Result<Self, PoolInitError> {
        connect_checked(dsn, ROLE_MAINTENANCE).await.map(Self)
    }

    /// T3.4 `humaux_adapters::stream_repo`'s pool accessor — see [`RuntimeDbPool::pool`]'s
    /// doc for why `pub(crate)` keeps G6-DB1's closed set intact.
    pub(crate) fn pool(&self) -> &PgPool {
        &self.0
    }
}

/// `role_public_worker`'s independent public-contribution worker pool (§6.2.1).
pub struct PublicWorkerDbPool(PgPool);

impl PublicWorkerDbPool {
    /// §6.2.3 assertion E: connects and verifies `current_user == "role_public_worker"`.
    pub async fn connect(dsn: &str) -> Result<Self, PoolInitError> {
        connect_checked(dsn, ROLE_PUBLIC_WORKER).await.map(Self)
    }

    /// Public contribution worker queries stay inside this crate so the checked role wrapper
    /// cannot be replaced by a pool belonging to another role.
    pub(crate) fn pool(&self) -> &PgPool {
        &self.0
    }
}

/// `role_migration_owner`: NOLOGIN, never a pool identity (ADR-0059 D-A); named here so the
/// migrator check can refuse every member of the frozen role set (§6.2.0).
pub const ROLE_MIGRATION_OWNER: &str = "role_migration_owner";

/// The principal that runs `migrate` (ADR-0059 D-E): the only pool that may `ALTER ROLE`. Opened
/// for `humaux-maintenance roles rotate` from `HUMAUX_MIGRATOR_PG_DSN`, one connection, never a
/// standing pool of any service.
pub struct MigratorDbPool(PgPool);

impl MigratorDbPool {
    /// §6.2.3-style connect check (ADR-0059 D-E): `current_user` is none of the nine §6.2.0 roles
    /// and is a superuser or holds CREATEROLE. Anything else is [`PoolInitError::RoleMismatch`].
    pub async fn connect(dsn: &str) -> Result<Self, PoolInitError> {
        const EXPECTED: &str = "a superuser or CREATEROLE principal outside the §6.2.0 role set";
        let pool = open(dsn).await?;
        let row = sqlx::query(
            "SELECT current_user::text, rolsuper OR rolcreaterole FROM pg_roles \
             WHERE rolname = current_user",
        )
        // dep: PostgreSQL(owner) — the connect check of the role-rotation principal
        .fetch_one(&pool)
        .await
        .map_err(PoolInitError::Connect)?;
        let actual: String = row.try_get(0).map_err(PoolInitError::Connect)?;
        let may_alter_roles: bool = row.try_get(1).map_err(PoolInitError::Connect)?;
        let frozen = [
            ROLE_GATEWAY,
            ROLE_BATCH_ISSUER,
            ROLE_CONSOLIDATION_WORKER,
            ROLE_PRIVATE_WORKER,
            ROLE_RETRIEVAL_WORKER,
            ROLE_MAINTENANCE,
            ROLE_PUBLIC_WORKER,
            ROLE_ADMIN,
            ROLE_MIGRATION_OWNER,
        ];
        if frozen.contains(&actual.as_str()) || !may_alter_roles {
            return Err(PoolInitError::RoleMismatch {
                expected: EXPECTED,
                actual,
            });
        }
        Ok(Self(pool))
    }

    /// `adapters::role_hygiene`'s accessor — see [`RuntimeDbPool::pool`]'s doc for why `pub(crate)`.
    pub(crate) fn pool(&self) -> &PgPool {
        &self.0
    }
}

/// The `humaux-maintenance retention …` principal (ADR-0063 D-H): the [`MigratorDbPool`] principal, and it must be a
/// SUPERUSER. LOCK SHARE, COPY and count of a leaf need table privileges no non-owner role may hold on a leaf (rls-check
/// partition arm), and the cross-tenant hold reads need to bypass FORCE RLS; a CREATEROLE-only migrator can do neither.
pub struct RetentionExecutor(MigratorDbPool);

impl RetentionExecutor {
    /// [`MigratorDbPool::connect`]'s check, then `rolsuper` of `session_user`; anything else is
    /// [`PoolInitError::RoleMismatch`] naming SUPERUSER, before any other statement.
    pub async fn connect(dsn: &str) -> Result<Self, PoolInitError> {
        // dep: PostgreSQL(owner) — the checked migrator connect this executor narrows to SUPERUSER
        let migrator = MigratorDbPool::connect(dsn).await?;
        let row = sqlx::query(
            "SELECT session_user::text, rolsuper FROM pg_roles WHERE rolname = session_user",
        )
        // dep: PostgreSQL(owner) — the connect check of the retention executor principal
        .fetch_one(migrator.pool())
        .await
        .map_err(PoolInitError::Connect)?;
        let actual: String = row.try_get(0).map_err(PoolInitError::Connect)?;
        if !row.try_get::<bool, _>(1).map_err(PoolInitError::Connect)? {
            return Err(PoolInitError::RoleMismatch {
                expected: "a SUPERUSER principal (ADR-0063 D-H)",
                actual,
            });
        }
        Ok(Self(migrator))
    }

    /// `adapters::maintenance_repo`'s executor accessor — see [`RuntimeDbPool::pool`]'s doc for why `pub(crate)`.
    pub(crate) fn pool(&self) -> &PgPool {
        self.0.pool()
    }
}

/// Binds observation writes to the public business pool's actual database identity.
/// Raw pool handling stays in the sole PostgreSQL encapsulation module; the caller
/// supplies only the two role-specific handles and never a request-provided target.
pub(crate) async fn public_observation_database_matches(
    writer: &MaintenanceDbPool,
    business: &PublicWorkerDbPool,
) -> Result<bool, ErrorCode> {
    Ok(database_identity(&writer.0).await? == database_identity(&business.0).await?)
}

async fn database_identity(pool: &PgPool) -> Result<(String, String, i32), ErrorCode> {
    let row = sqlx::query("SELECT current_database() AS db, inet_server_addr()::text AS addr, inet_server_port() AS port")
        // dep: PostgreSQL(any) — executes a query against the pool
        .fetch_one(pool).await.map_err(|_| ErrorCode::Internal)?;
    let db: String = row.try_get("db").map_err(|_| ErrorCode::Internal)?;
    let addr: Option<String> = row.try_get("addr").map_err(|_| ErrorCode::Internal)?;
    let port: Option<i32> = row.try_get("port").map_err(|_| ErrorCode::Internal)?;
    match (addr, port) {
        (Some(addr), Some(port)) if !db.is_empty() && !addr.is_empty() && port > 0 => {
            Ok((db, addr, port))
        }
        _ => Err(ErrorCode::InvalidInput),
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests here (not `tests/`) so `.0` stays reachable without widening the field's
    //! visibility past this module (§6.2.3 "inner 字段 crate-private").
    //!
    //! Reuses the repo's §79.2 three-state DB fixture contract
    //! (`humaux_testkit::{run_db_fixture, DbIntegrationFixture}`, already established by the
    //! sibling T1.5 `tests/auth_scope_rls.rs`) instead of a bespoke skip mechanism — `isolate`
    //! opens the admin connection and is the single place a missing `HUMAUX_TEST_PG_DSN` or
    //! unreachable DB turns into a printed `SKIP`, never a silent pass.
    use super::*;
    use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
    use postgres::{Client, NoTls};

    /// Builds a role-scoped DSN from the shared admin DSN via sqlx's `options[role]=...` query
    /// param (`sqlx-postgres/src/options/parse.rs`: `k if k.starts_with("options[")` — becomes
    /// the startup GUC `-c role=<role>`, i.e. a post-connect `SET ROLE`). No separate per-role
    /// credentials needed — a superuser DSN can `SET ROLE` to any existing role for free.
    /// Test-only: production wrappers connect with a DSN that already authenticates as the
    /// target role directly.
    fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
        // libpq-standard `options=-c role=X` (URL-encoded). The older `options[role]=X` form
        // is silently tolerated by sqlx but rejected outright by rust-postgres ("invalid
        // connection string") — which made every `Client::connect` role fixture skip, and a
        // skip is not a pass (§79.2). This form is verified working on both drivers.
        let sep = if admin_dsn.contains('?') { '&' } else { '?' };
        format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
    }

    struct AdminConn;

    impl DbIntegrationFixture for AdminConn {
        type Handle = (String, Client);

        fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
            let dsn = std::env::var("HUMAUX_TEST_PG_DSN")
                .map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
            // dep: PostgreSQL(any) — connects to PostgreSQL
            let client = Client::connect(&dsn, NoTls)
                .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
            Ok((dsn, client))
        }
    }

    /// §6.2.3 assertion E, negative branch: connecting as the wrong role must return
    /// [`PoolInitError::RoleMismatch`], not silently succeed. Connects as `role_batch_issuer`
    /// (via `dsn_as_role`, a real `SET ROLE`) but checks against `ROLE_GATEWAY` — `actual` is
    /// pinned to a specific known-wrong value, unlike comparing against the admin DSN's
    /// `current_user` (typically the superuser), where `actual != ROLE_GATEWAY` is true for
    /// nearly any DSN and the assertion would still pass even if `connect_checked`'s
    /// comparison were broken into an always-true check.
    #[test]
    fn role_mismatch_is_rejected() {
        run_db_fixture::<AdminConn, _>("role_mismatch_is_rejected", |(dsn, mut client)| {
            let Some(()) = require(
                &mut client,
                "role_mismatch_is_rejected",
                &[
                    ROLE_BATCH_ISSUER,
                    ROLE_PUBLIC_WORKER,
                    ROLE_GATEWAY,
                    ROLE_PRIVATE_WORKER,
                ],
                &[],
            ) else {
                return;
            };
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime for sqlx connect");
            for wrong_role in [ROLE_GATEWAY, ROLE_PRIVATE_WORKER] {
                let mismatch = rt
                    // dep: PostgreSQL(role_public_worker) — connects to PostgreSQL
                    .block_on(PublicWorkerDbPool::connect(&dsn_as_role(&dsn, wrong_role)))
                    .err()
                    .unwrap_or_else(|| panic!("public worker must reject {wrong_role} DSN"));
                match mismatch {
                    PoolInitError::RoleMismatch { expected, actual } => {
                        assert_eq!(expected, ROLE_PUBLIC_WORKER);
                        assert_eq!(actual, wrong_role);
                    }
                    PoolInitError::Connect(e) => {
                        panic!("expected RoleMismatch, got Connect({e})")
                    }
                }
            }
            for wrong_role in [ROLE_BATCH_ISSUER, ROLE_PUBLIC_WORKER] {
                rt.block_on(async {
                    let scoped = dsn_as_role(&dsn, wrong_role);
                    let err = connect_checked(&scoped, ROLE_GATEWAY).await.expect_err(
                        "a non-gateway current_user must not literally be role_gateway",
                    );
                    match err {
                        PoolInitError::RoleMismatch { expected, actual } => {
                            assert_eq!(expected, ROLE_GATEWAY);
                            assert_eq!(actual, wrong_role);
                        }
                        PoolInitError::Connect(e) => {
                            panic!("expected RoleMismatch, got Connect({e})")
                        }
                    }
                });
            }
        });
    }

    /// §6.2.3 assertion E, positive branch, for all seven roles: once `roles.sql` has created
    /// the role, `SET ROLE` (via `dsn_as_role`) makes `current_user` match literally and
    /// `connect_checked` must return `Ok`. Per-role existence is a separate, narrower
    /// precondition than "DB reachable" — printed as its own `SKIP` line (§79 三态: 未就绪就
    /// skip 并打印原因, 不静默通过) rather than folded into the fixture-level skip.
    #[test]
    fn role_match_succeeds_once_role_exists() {
        run_db_fixture::<AdminConn, _>(
            "role_match_succeeds_once_role_exists",
            |(dsn, mut client)| {
                let rt = tokio::runtime::Runtime::new().expect("tokio runtime for sqlx connect");
                if require(
                    &mut client,
                    "public_worker_direct_connect",
                    &[ROLE_PUBLIC_WORKER],
                    &[],
                )
                .is_some()
                {
                    let pool = rt
                        // dep: PostgreSQL(role_public_worker) — connects to PostgreSQL
                        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
                            &dsn,
                            ROLE_PUBLIC_WORKER,
                        )))
                        .unwrap_or_else(|e| panic!("public worker wrapper connect failed: {e}"));
                    rt.block_on(pool.pool().close());
                }
                for role in [
                    ROLE_GATEWAY,
                    ROLE_BATCH_ISSUER,
                    ROLE_CONSOLIDATION_WORKER,
                    ROLE_PRIVATE_WORKER,
                    ROLE_RETRIEVAL_WORKER,
                    ROLE_MAINTENANCE,
                    ROLE_PUBLIC_WORKER,
                ] {
                    // A failed existence query means the admin fixture itself is broken
                    // (connection dropped, grants changed, syntax regression) — that is a
                    // fail, not the "object not present" case §79.2's SKIP exists for.
                    // Folding both into `false` would downgrade a real failure into a
                    // silent not_applicable, the direction §79.2 forbids.
                    let exists: bool = client
                        .query_one(
                            "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)",
                            &[&role],
                        )
                        .unwrap_or_else(|e| {
                            panic!("pg_roles existence query failed for {role}: {e}")
                        })
                        .get(0);
                    if !exists {
                        humaux_testkit::skip_or_fail(
                            &format!("role_match_succeeds_once_role_exists[{role}]"),
                            &format!(
                                "missing object: role {role} not present in pg_roles \
                                 (roles.sql not applied to this DB yet)"
                            ),
                            humaux_testkit::ExternalDep::Postgres,
                        );
                        continue;
                    }
                    let scoped = dsn_as_role(&dsn, role);
                    rt.block_on(async {
                        let pool = connect_checked(&scoped, role).await.unwrap_or_else(|e| {
                            panic!("role {role} exists but connect_checked failed: {e}")
                        });
                        pool.close().await;
                    });
                }
            },
        );
    }

    /// Checks that `roles` and `tables` all exist; prints one `SKIP {test_name}: ...` and
    /// returns `None` at the first unmet precondition. §48 DDL and roles.sql are separate
    /// tasks from T1.4, so on a DB where they haven't landed yet every G6-DB2 case below
    /// skips here — that is the correct three-state answer, not a failure to wire up.
    fn require(
        client: &mut Client,
        test_name: &str,
        roles: &[&str],
        tables: &[&str],
    ) -> Option<()> {
        // Both loops below panic on a query *error* and only SKIP on a query result of
        // `false` — collapsing "the existence query itself failed" into "not present" would
        // let a broken admin fixture (dropped connection, revoked grants, syntax regression)
        // masquerade as the legitimate §79.2 not_applicable case instead of failing loudly.
        for role in roles {
            let exists: bool = client
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)",
                    &[role],
                )
                .unwrap_or_else(|e| panic!("pg_roles existence query failed for {role:?}: {e}"))
                .get(0);
            if !exists {
                humaux_testkit::skip_or_fail(
                    test_name,
                    &format!(
                        "missing object: role {role:?} not present in pg_roles \
                         (roles.sql not applied to this DB yet)"
                    ),
                    humaux_testkit::ExternalDep::Postgres,
                );
                return None;
            }
        }
        for table in tables {
            let exists: bool = client
                .query_one("SELECT to_regclass($1) IS NOT NULL", &[table])
                .unwrap_or_else(|e| panic!("to_regclass query failed for {table:?}: {e}"))
                .get(0);
            if !exists {
                humaux_testkit::skip_or_fail(
                    test_name,
                    &format!(
                        "missing object: table {table:?} does not exist \
                         (§48 DDL not applied to this DB yet)"
                    ),
                    humaux_testkit::ExternalDep::Postgres,
                );
                return None;
            }
        }
        Some(())
    }

    /// The table's **insertable** columns, comma-separated.
    ///
    /// The probes below used a bare `SELECT *`, which stopped being schema-agnostic the moment
    /// a table gained a `GENERATED ALWAYS ... STORED` column (`private.memory_records.facet`,
    /// migration 0172): `*` expands to include it and PostgreSQL refuses "cannot insert a
    /// non-DEFAULT value into column" — a shape error that has nothing to do with the
    /// permission these probes exist to measure. Naming the non-generated columns keeps the
    /// probe schema-agnostic for real and keeps the failure mode pinned on permissions.
    async fn insertable_columns(pool: &PgPool, table: &str) -> String {
        let columns: Vec<String> = sqlx::query_scalar(
            "SELECT quote_ident(a.attname) FROM pg_attribute a               WHERE a.attrelid = $1::regclass AND a.attnum > 0                 AND NOT a.attisdropped AND a.attgenerated = ''               ORDER BY a.attnum",
        )
        .bind(table)
        // dep: PostgreSQL(any) — executes a query against the pool
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("column list for {table}: {e}"));
        assert!(!columns.is_empty(), "{table} has no insertable column");
        columns.join(", ")
    }

    /// `INSERT INTO t (cols) SELECT cols FROM t WHERE false` inserts zero rows either way — it
    /// isolates the *permission* check (which PostgreSQL performs before executing the query)
    /// from having to know the table's real column values, so this file needs no dependency on
    /// the §48 DDL task's exact row shapes.
    async fn insert_probe_ok(pool: &PgPool, table: &str) {
        let columns = insertable_columns(pool, table).await;
        sqlx::query(&format!(
            "INSERT INTO {table} ({columns}) SELECT {columns} FROM {table} WHERE false"
        ))
        // dep: PostgreSQL(any) — executes a query against the pool
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("expected INSERT on {table} to succeed (0-row probe): {e}"));
    }

    /// Same 0-row probe as [`insert_probe_ok`], asserting PostgreSQL rejects it with
    /// `permission denied` rather than any other error (e.g. a typo would "succeed" by
    /// failing for the wrong reason).
    async fn insert_probe_denied(pool: &PgPool, table: &str) {
        // `SELECT * FROM {table} WHERE false` needs SELECT as well as INSERT to run at all —
        // on a role with no SELECT grant either, the probe below "passes" on a permission
        // denied that could be caused by the missing SELECT alone, leaving an over-granted
        // INSERT unobservable (e.g. `GRANT INSERT ON {table} TO role`). Assert the specific
        // privilege directly first so a fault-injected extra INSERT grant is caught here.
        let has_insert: bool =
            sqlx::query_scalar("SELECT has_table_privilege(current_user, $1, 'INSERT')")
                .bind(table)
                // dep: PostgreSQL(any) — executes a query against the pool
                .fetch_one(pool)
                .await
                .unwrap_or_else(|e| panic!("has_table_privilege check failed for {table}: {e}"));
        assert!(
            !has_insert,
            "expected no INSERT privilege on {table}, but has_table_privilege(current_user, \
             ..) reports true"
        );

        let columns = insertable_columns(pool, table).await;
        let err = sqlx::query(&format!(
            "INSERT INTO {table} ({columns}) SELECT {columns} FROM {table} WHERE false"
        ))
        // dep: PostgreSQL(any) — executes a query against the pool
        .execute(pool)
        .await
        .expect_err(&format!(
            "expected INSERT on {table} to be permission-denied"
        ));
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("permission denied"),
            "expected permission denied for {table}, got: {msg}"
        );
    }

    /// One real column name for `schema.table`, needed because `UPDATE t SET col = col`
    /// (unlike `INSERT ... SELECT *`) has no schema-agnostic form — SQL has no "set every
    /// column to itself" wildcard.
    fn any_column(client: &mut Client, schema: &str, table: &str) -> String {
        client
            .query_one(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position LIMIT 1",
                &[&schema, &table],
            )
            .map(|row| row.get(0))
            .unwrap_or_else(|e| panic!("expected {schema}.{table} to have >=1 column: {e}"))
    }

    async fn update_probe_denied(pool: &PgPool, table: &str, col: &str) {
        // Assert zero UPDATE grants across ALL columns of `table`, not just `col` (the PK
        // `any_column` happens to pick). §6.2.1 grants are frequently column-scoped (e.g.
        // `GRANT UPDATE(status) ON table TO role`); a probe that only ever touches the PK
        // column would stay "denied" and green even if such a column grant were mistakenly
        // added on a different column, since the PK itself is never independently granted.
        let (schema, bare_table) = table
            .split_once('.')
            .unwrap_or_else(|| panic!("expected schema-qualified table name, got {table:?}"));
        let bad_grants: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM information_schema.column_privileges \
             WHERE grantee = current_user AND table_schema = $1 AND table_name = $2 \
             AND privilege_type = 'UPDATE'",
        )
        .bind(schema)
        .bind(bare_table)
        // dep: PostgreSQL(any) — executes a query against the pool
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("column_privileges query failed for {table}: {e}"));
        assert_eq!(
            bad_grants, 0,
            "expected zero UPDATE column grants on {table} for current role, found {bad_grants}"
        );

        let err = sqlx::query(&format!("UPDATE {table} SET {col} = {col} WHERE false"))
            // dep: PostgreSQL(any) — executes a query against the pool
            .execute(pool)
            .await
            .expect_err(&format!(
                "expected UPDATE on {table} to be permission-denied"
            ));
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("permission denied"),
            "expected permission denied for {table}, got: {msg}"
        );
    }

    /// §6.2.3 G6-DB2 — `BatchIssuerDbPool`: it alone may `INSERT private.ingest_tickets`
    /// (§60.1 self-invoice lockout / G23-1c); it has no grant at all on `private.events`.
    #[test]
    fn g6_db2_batch_issuer_pool() {
        run_db_fixture::<AdminConn, _>("g6_db2_batch_issuer_pool", |(dsn, mut client)| {
            let Some(()) = require(
                &mut client,
                "g6_db2_batch_issuer_pool",
                &[ROLE_BATCH_ISSUER],
                &["private.ingest_tickets", "private.events"],
            ) else {
                return;
            };
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime for sqlx connect");
            rt.block_on(async {
                // dep: PostgreSQL(role_batch_issuer) — connects to PostgreSQL
                let pool = BatchIssuerDbPool::connect(&dsn_as_role(&dsn, ROLE_BATCH_ISSUER))
                    .await
                    .expect("connect as role_batch_issuer");
                insert_probe_ok(&pool.0, "private.ingest_tickets").await;
                insert_probe_denied(&pool.0, "private.events").await;
            });
        });
    }

    /// §6.2.3 G6-DB2 — `RuntimeDbPool`: the mirror image of the batch-issuer case (§60.1
    /// "反向同时封死" — runtime role must NOT be able to write its own ingest ticket).
    #[test]
    fn g6_db2_runtime_pool() {
        run_db_fixture::<AdminConn, _>("g6_db2_runtime_pool", |(dsn, mut client)| {
            let Some(()) = require(
                &mut client,
                "g6_db2_runtime_pool",
                &[ROLE_GATEWAY],
                &["private.events", "private.ingest_tickets"],
            ) else {
                return;
            };
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime for sqlx connect");
            rt.block_on(async {
                // dep: PostgreSQL(role_gateway) — connects to PostgreSQL
                let pool = RuntimeDbPool::connect(&dsn_as_role(&dsn, ROLE_GATEWAY))
                    .await
                    .expect("connect as role_gateway");
                insert_probe_ok(&pool.0, "private.events").await;
                insert_probe_denied(&pool.0, "private.ingest_tickets").await;
            });
        });
    }

    /// §6.2.3 G6-DB2 — `ConsolidationDbPool`: may `INSERT private.memory_consolidation_inputs`
    /// but has no `UPDATE` on `private.memory_records` / `private.memory_evidence` (§6.2.2 —
    /// those two stay `SELECT`-only for the consolidation worker).
    #[test]
    fn g6_db2_consolidation_pool() {
        run_db_fixture::<AdminConn, _>("g6_db2_consolidation_pool", |(dsn, mut client)| {
            let Some(()) = require(
                &mut client,
                "g6_db2_consolidation_pool",
                &[ROLE_CONSOLIDATION_WORKER],
                &[
                    "private.memory_consolidation_inputs",
                    "private.memory_records",
                    "private.memory_evidence",
                ],
            ) else {
                return;
            };
            let records_col = any_column(&mut client, "private", "memory_records");
            let evidence_col = any_column(&mut client, "private", "memory_evidence");
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime for sqlx connect");
            rt.block_on(async {
                let pool =
                    // dep: PostgreSQL(role_consolidation_worker) — connects to PostgreSQL
                    ConsolidationDbPool::connect(&dsn_as_role(&dsn, ROLE_CONSOLIDATION_WORKER))
                        .await
                        .expect("connect as role_consolidation_worker");
                insert_probe_ok(&pool.0, "private.memory_consolidation_inputs").await;
                update_probe_denied(&pool.0, "private.memory_records", &records_col).await;
                update_probe_denied(&pool.0, "private.memory_evidence", &evidence_col).await;
            });
        });
    }

    /// §6.2.3 G6-DB2 — `PrivateWorkerDbPool`: may `INSERT private.memory_records` but not
    /// `private.memory_consolidation_inputs` (that table belongs to the consolidation lane).
    #[test]
    fn g6_db2_private_worker_pool() {
        run_db_fixture::<AdminConn, _>("g6_db2_private_worker_pool", |(dsn, mut client)| {
            let Some(()) = require(
                &mut client,
                "g6_db2_private_worker_pool",
                &[ROLE_PRIVATE_WORKER],
                &[
                    "private.memory_records",
                    "private.memory_consolidation_inputs",
                ],
            ) else {
                return;
            };
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime for sqlx connect");
            rt.block_on(async {
                // dep: PostgreSQL(role_private_worker) — connects to PostgreSQL
                let pool = PrivateWorkerDbPool::connect(&dsn_as_role(&dsn, ROLE_PRIVATE_WORKER))
                    .await
                    .expect("connect as role_private_worker");
                insert_probe_ok(&pool.0, "private.memory_records").await;
                insert_probe_denied(&pool.0, "private.memory_consolidation_inputs").await;
            });
        });
    }

    /// Column-scoped positive probe: `col` succeeds on a 0-row `UPDATE`. Unlike
    /// [`update_probe_denied`] this does not assert anything about *other* columns — it is
    /// paired with [`update_column_probe_denied`] on a *different* column of the same table
    /// (§6.2.2's column-limited grants put both a grantee and a non-grantee column on one
    /// role, e.g. `role_retrieval_worker` has `projection_highwater` but not
    /// `issued_highwater`), which `update_probe_denied`'s "zero grants across the whole
    /// table" assertion cannot express.
    async fn update_column_probe_ok(pool: &PgPool, table: &str, col: &str) {
        sqlx::query(&format!("UPDATE {table} SET {col} = {col} WHERE false"))
            // dep: PostgreSQL(any) — executes a query against the pool
            .execute(pool)
            .await
            .unwrap_or_else(|e| {
                panic!("expected UPDATE {table}.{col} to succeed (0-row probe): {e}")
            });
    }

    /// Column-scoped negative probe — see [`update_column_probe_ok`] for why this checks one
    /// named column instead of every column on `table`.
    async fn update_column_probe_denied(pool: &PgPool, table: &str, col: &str) {
        let has: bool = sqlx::query_scalar(
            "SELECT has_column_privilege(current_user, $1::regclass, $2, 'UPDATE')",
        )
        .bind(table)
        .bind(col)
        // dep: PostgreSQL(any) — executes a query against the pool
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("has_column_privilege check failed for {table}.{col}: {e}"));
        assert!(
            !has,
            "expected no UPDATE privilege on {table}.{col}, but has_column_privilege reports true"
        );

        let err = sqlx::query(&format!("UPDATE {table} SET {col} = {col} WHERE false"))
            // dep: PostgreSQL(any) — executes a query against the pool
            .execute(pool)
            .await
            .expect_err(&format!(
                "expected UPDATE {table}.{col} to be permission-denied"
            ));
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("permission denied"),
            "expected permission denied for {table}.{col}, got: {msg}"
        );
    }

    /// §6.2.3 G6-DB2 — `RetrievalWorkerDbPool`: may `UPDATE
    /// projection.stream_checkpoints.projection_highwater` (T3.3 `advance_prefix`'s write)
    /// but not `.issued_highwater` (that column is `role_gateway`-only, §15.1 seq issuance).
    #[test]
    fn g6_db2_retrieval_worker_pool() {
        run_db_fixture::<AdminConn, _>("g6_db2_retrieval_worker_pool", |(dsn, mut client)| {
            let Some(()) = require(
                &mut client,
                "g6_db2_retrieval_worker_pool",
                &[ROLE_RETRIEVAL_WORKER],
                &["projection.stream_checkpoints"],
            ) else {
                return;
            };
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime for sqlx connect");
            rt.block_on(async {
                let pool =
                    // dep: PostgreSQL(role_retrieval_worker) — connects to PostgreSQL
                    RetrievalWorkerDbPool::connect(&dsn_as_role(&dsn, ROLE_RETRIEVAL_WORKER))
                        .await
                        .expect("connect as role_retrieval_worker");
                update_column_probe_ok(
                    &pool.0,
                    "projection.stream_checkpoints",
                    "projection_highwater",
                )
                .await;
                update_column_probe_denied(
                    &pool.0,
                    "projection.stream_checkpoints",
                    "issued_highwater",
                )
                .await;
            });
        });
    }

    /// §6.2.3 G6-DB2 — `MaintenanceDbPool`: may `UPDATE projection.stream_log.state` (T3.4's
    /// `ISSUED -> LOST` sweep) but has no `INSERT` on it (§15.2 — only `role_gateway` mints
    /// new `stream_log` rows, §60 `issue_stream_log_row`).
    #[test]
    fn g6_db2_maintenance_pool() {
        run_db_fixture::<AdminConn, _>("g6_db2_maintenance_pool", |(dsn, mut client)| {
            let Some(()) = require(
                &mut client,
                "g6_db2_maintenance_pool",
                &[ROLE_MAINTENANCE],
                &["projection.stream_log"],
            ) else {
                return;
            };
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime for sqlx connect");
            rt.block_on(async {
                // dep: PostgreSQL(role_maintenance) — connects to PostgreSQL
                let pool = MaintenanceDbPool::connect(&dsn_as_role(&dsn, ROLE_MAINTENANCE))
                    .await
                    .expect("connect as role_maintenance");
                update_column_probe_ok(&pool.0, "projection.stream_log", "state").await;
                insert_probe_denied(&pool.0, "projection.stream_log").await;
            });
        });
    }
}
