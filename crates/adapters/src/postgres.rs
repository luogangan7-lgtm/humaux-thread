//! `adapters::postgres` — the sole encapsulation point for `sqlx::PgPool` (§6.2.3).
//!
//! Spec canonical path is `crates/adapters/postgres/src/pools.rs`; this repo's crate layout
//! (§58) flattens adapters into `crates/adapters/src/<name>.rs` modules, so **this file IS
//! meant to be** that sole encapsulation point — no other file in this workspace is meant to
//! name `sqlx::PgPool` (G6-DB1 assertion B).
//!
//! **Assertion B has no execution body yet — this is a known gap, not a passing check.**
//! `xtask/src/architecture_check.rs` implements `rule1_forbidden_fallback` /
//! `rule2_degrade_fault_parity` / `rule3_positive_sentinels` and the §78.3
//! `DOMAIN_FORBIDDEN_DEPS` scan, but that scan only runs against `humaux-domain` (the source
//! explicitly notes `humaux-application` "is deliberately not checked here"). No
//! `db-pool-topology-check` (G80-40) subcommand exists in `xtask` — the name only appears in
//! `xtask/src/contract_impact.rs`'s test fixtures as a hypothetical `CheckerKind::Implemented`
//! for a scenario that "assumes G80-26/G80-40 are already implemented (unlike this repo's
//! actual state)". Adding a bare `PgPool` field anywhere outside this file — including on
//! `humaux-application` types — will not be caught by any current gate. Tracked on the shared
//! task canvas; land a real static scan (or equivalent) before G80-40 / G6-DB1 assertion B is
//! treated as closed.
//!
//! Closed set (§6.2.3): four newtypes, one per role that holds a standing connection pool.
//! `inner` is private to this module on every wrapper; none of them implement `From`/`Into`
//! of one another, `Deref<Target = PgPool>`, or `Clone` — a leaked `PgPool` (via any of
//! those) would let a caller run SQL under the wrong role's grants, defeating the entire
//! point of the closed set. Application ports must take `&RuntimeDbPool` /
//! `&BatchIssuerDbPool` / `&ConsolidationDbPool` / `&PrivateWorkerDbPool` by name, never a
//! bare `PgPool` (G6-DB1 `tests/ui/pass_*` / `fail_*` fixtures prove both directions).

use std::fmt;

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

/// Construction-time failure for any of the four typed pools (§6.2.3: "不符返回 Err").
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

/// Connects `dsn` and asserts `SELECT current_user` equals `expected_role` literally before
/// handing back a pool (§6.2.3 assertion E). This is the *only* place in the crate that
/// constructs a bare [`PgPool`] — every wrapper's `connect` calls through here.
async fn connect_checked(dsn: &str, expected_role: &'static str) -> Result<PgPool, PoolInitError> {
    let pool = PgPoolOptions::new()
        .connect(dsn)
        .await
        .map_err(PoolInitError::Connect)?;
    let row = sqlx::query("SELECT current_user")
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

// ponytail: no Application-layer consumer yet reads `.0` (that layer is a later task).
#[allow(dead_code)]
/// `role_batch_issuer`'s independent single-purpose pool (§6.2.0: must not share a pool with
/// any runtime role — sharing would hand request-path connections invoice-issuing power).
pub struct BatchIssuerDbPool(PgPool);

impl BatchIssuerDbPool {
    /// §6.2.3 assertion E: connects and verifies `current_user == "role_batch_issuer"`.
    pub async fn connect(dsn: &str) -> Result<Self, PoolInitError> {
        connect_checked(dsn, ROLE_BATCH_ISSUER).await.map(Self)
    }
}

// ponytail: no Application-layer consumer yet reads `.0` (that layer is a later task).
#[allow(dead_code)]
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
        let sep = if admin_dsn.contains('?') { '&' } else { '?' };
        format!("{admin_dsn}{sep}options[role]={role}")
    }

    struct AdminConn;

    impl DbIntegrationFixture for AdminConn {
        type Handle = (String, Client);

        fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
            let dsn = std::env::var("HUMAUX_TEST_PG_DSN")
                .map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
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
                &[ROLE_BATCH_ISSUER],
                &[],
            ) else {
                return;
            };
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime for sqlx connect");
            rt.block_on(async {
                let scoped = dsn_as_role(&dsn, ROLE_BATCH_ISSUER);
                let err = connect_checked(&scoped, ROLE_GATEWAY).await.expect_err(
                    "role_batch_issuer's current_user must not literally be role_gateway",
                );
                match err {
                    PoolInitError::RoleMismatch { expected, actual } => {
                        assert_eq!(expected, ROLE_GATEWAY);
                        assert_eq!(actual, ROLE_BATCH_ISSUER);
                    }
                    PoolInitError::Connect(e) => panic!("expected RoleMismatch, got Connect({e})"),
                }
            });
        });
    }

    /// §6.2.3 assertion E, positive branch, for all four roles: once `roles.sql` has created
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
                for role in [
                    ROLE_GATEWAY,
                    ROLE_BATCH_ISSUER,
                    ROLE_CONSOLIDATION_WORKER,
                    ROLE_PRIVATE_WORKER,
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
                        eprintln!(
                            "SKIP role_match_succeeds_once_role_exists[{role}]: role not present in \
                         pg_roles — roles.sql not applied to this DB yet (§79.2 — 跳过不等于通过)"
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
                eprintln!(
                    "SKIP {test_name}: role {role:?} not present in pg_roles — roles.sql not \
                     applied to this DB yet (§79.2 — 跳过不等于通过)"
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
                eprintln!(
                    "SKIP {test_name}: table {table:?} does not exist — §48 DDL not applied to \
                     this DB yet (§79.2 — 跳过不等于通过)"
                );
                return None;
            }
        }
        Some(())
    }

    /// `INSERT INTO t SELECT * FROM t WHERE false` is schema-agnostic (any column list) and
    /// inserts zero rows either way — it isolates the *permission* check (which PostgreSQL
    /// performs before executing the query) from having to know the table's real column
    /// values, so this file needs no dependency on the §48 DDL task's exact row shapes.
    async fn insert_probe_ok(pool: &PgPool, table: &str) {
        sqlx::query(&format!(
            "INSERT INTO {table} SELECT * FROM {table} WHERE false"
        ))
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
                .fetch_one(pool)
                .await
                .unwrap_or_else(|e| panic!("has_table_privilege check failed for {table}: {e}"));
        assert!(
            !has_insert,
            "expected no INSERT privilege on {table}, but has_table_privilege(current_user, \
             ..) reports true"
        );

        let err = sqlx::query(&format!(
            "INSERT INTO {table} SELECT * FROM {table} WHERE false"
        ))
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
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("column_privileges query failed for {table}: {e}"));
        assert_eq!(
            bad_grants, 0,
            "expected zero UPDATE column grants on {table} for current role, found {bad_grants}"
        );

        let err = sqlx::query(&format!("UPDATE {table} SET {col} = {col} WHERE false"))
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
                let pool = PrivateWorkerDbPool::connect(&dsn_as_role(&dsn, ROLE_PRIVATE_WORKER))
                    .await
                    .expect("connect as role_private_worker");
                insert_probe_ok(&pool.0, "private.memory_records").await;
                insert_probe_denied(&pool.0, "private.memory_consolidation_inputs").await;
            });
        });
    }
}
