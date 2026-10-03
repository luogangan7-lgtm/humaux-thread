//! `adapters::tests::g80_31_handoff` — G80-31（§57.1 Phase 8 出场判据）端到端：**同一快照两次装配 handoff 逐字节相同**， 且 Mandatory
//!   的选取是机械规则、撤销立即生效、pinned 的「钉 3 带 2」可观测。
//! Depends-on: crates=[humaux-adapters, humaux-application, humaux-domain, humaux-projection, humaux-retrieval,
//!   humaux-testkit, postgres, serde_json, sqlx, tokio, uuid]; services=[PostgreSQL(any)
//!   w=[control.private_reasoning_domains, control.tenants, ops.jobs, ops.selection_snapshot_items,
//!   ops.selection_snapshots, private.context_bindings, private.events, private.evidence_objects,
//!   private.memory_evidence, private.memory_records, projection.stream_checkpoints, projection.stream_log],
//!   PostgreSQL(owner),
//!   PostgreSQL(role_gateway)]; env=[HUMAUX_GATEWAY_PG_DSN, HUMAUX_TEST_PG_DSN]; modules=[adapters::context_repo,
//!   adapters::postgres, adapters::read_materialize, application::continuity, domain::authority, domain::context,
//!   domain::error, domain::identity, domain::ids, humaux-testkit, projection::serving, projection::stream,
//!   retrieval::compiler, retrieval::handoff]
//! Called-by: [cargo-test]
//! Invariants: [two assemblies of one snapshot must be byte-identical, with snapshot-token equality asserted first (a
//!   concurrent writer is a precondition failure, not a regression); skip_or_fail turns a missing DB into a failure
//!   under HUMAUX_REQUIRE_DB]
//! Spec: ADR-0047; ADR-0050; ADR-0006
//!
//! 快照身份语义（`FrozenReads` doc）：两次装配各开各的 REPEATABLE READ 事务——静默
//! fixture 库（本 harness 独占本租户的写权）里两个事务看到同一世界。**前置显式断言
//! snapshot token 相等**（不是假设）：token 不等说明有并发写者，此时字节不同不是回归，
//! 是前置没满足——判据与环境噪声分开。
//!
//! 三态照 `testkit::skip_or_fail`（CI 声明 HUMAUX_REQUIRE_DB=1 时跳过即失败）。
//!
//! depends-on: Postgres at `HUMAUX_TEST_PG_DSN` (owner; any loopback port/database — ADR-0047
//! D-D, ADR-0050 D-B) + `HUMAUX_GATEWAY_PG_DSN` (role_gateway, same port/database as the owner
//! or the legacy `61719 / humaux_thread_request_guard_20260828` pair); tables control.tenants,
//! private.memory_records/evidence/context_bindings, ops.selection_snapshots, projection.stream_*.
//! called-by: `cargo test -p humaux-adapters --test g80_31_handoff` (gate chain `adapters_tests`).
//! invariants: owner DDL (a tenant-scoped RESTRICTIVE policy) is dropped in `Fixture::drop`.
//! NA 的唯一主语是 probe 探测的具名缺失对象；字节不同 / mandatory 被淘汰 /
//! needs_verification 空 **永远是红**，与 NA 零重合（ADR-0006）。

use humaux_adapters::context_repo::{
    ContextReadAdapter, MaterializedContext, MaterializedMemory, MemoryEnumerationParams,
    assemble_materialized, materialize_memory_enumeration, materialize_memory_get,
};
use humaux_adapters::postgres::RuntimeDbPool;
use humaux_adapters::read_materialize::MaterializedItem;
use humaux_application::continuity::assemble_handoff;
use humaux_domain::authority::MemoryId;
use humaux_domain::context::ContextBudget;
use humaux_domain::error::ErrorCode;
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{Scope, TenantId, WorkspaceId};
use humaux_projection::serving::StreamFamily;
use humaux_projection::stream::StreamKey;
use humaux_retrieval::compiler::ContextOutcome;
use humaux_retrieval::handoff::Handoff;
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use sqlx::postgres::PgConnectOptions;
use std::str::FromStr;
use std::time::{Duration, Instant};
use uuid::Uuid;

const NAME: &str = "g80_31_handoff";
const REQUEST_GUARD_DB: &str = "humaux_thread_request_guard_20260828";
const ENUMERATION_MAC_KEY: &[u8] = b"g80_31 enumeration test key, not a production secret";

/// Uses the isolated fixture's real gateway LOGIN, never a privileged owner connection that
/// changes role after connecting. Invalid/missing configuration is a testkit skip-or-fail.
fn gateway_dsn() -> Option<String> {
    let Ok(dsn) = std::env::var("HUMAUX_GATEWAY_PG_DSN") else {
        skip_or_fail(
            NAME,
            "missing object: role_gateway PostgreSQL DSN",
            ExternalDep::Postgres,
        );
        return None;
    };
    let Ok(options) = PgConnectOptions::from_str(&dsn) else {
        skip_or_fail(
            NAME,
            "invalid object: role_gateway PostgreSQL DSN",
            ExternalDep::Postgres,
        );
        return None;
    };
    // card 23 (serial lane, class (a)): the target is whatever `HUMAUX_TEST_PG_DSN` names —
    // the repo-wide isolated test database (§79.2) — with the machine-local `61719 /
    // humaux_thread_request_guard_20260828` pair kept as an accepted legacy target. Pinning
    // only that pair made all 11 tests in this file skip on every standard environment while
    // `skip_or_fail` printed SKIP, i.e. a whole file of false green; the identical fix already
    // landed in `support/operation_receipt_fixture.rs` on 2026-09-03 and this is a read of the
    // same rule, not a second one.
    let legacy_target =
        options.get_port() == 61719 && options.get_database() == Some(REQUEST_GUARD_DB);
    let shared_target = std::env::var("HUMAUX_TEST_PG_DSN")
        .ok()
        .and_then(|admin| PgConnectOptions::from_str(&admin).ok())
        .is_some_and(|admin| {
            options.get_port() == admin.get_port() && options.get_database() == admin.get_database()
        });
    if options.get_username() != "role_gateway"
        || options.get_host() != "127.0.0.1"
        || !(legacy_target || shared_target)
        || dsn.contains(['?', '#'])
    {
        skip_or_fail(
            NAME,
            "invalid object: isolated role_gateway fixture DSN",
            ExternalDep::Postgres,
        );
        return None;
    }
    // dep: PostgreSQL(role_gateway) — HUMAUX_GATEWAY_PG_DSN: identity probe: LOGIN, non-superuser, non-bypassrls.
    // dep: PostgreSQL(any) — opens the role-scoped connection for `gateway_dsn`
    let Ok(mut gateway) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(
            NAME,
            "missing object: role_gateway PostgreSQL login",
            ExternalDep::Postgres,
        );
        return None;
    };
    let Ok(role_ok) = gateway.query_one(
        "SELECT current_user='role_gateway' AND session_user='role_gateway' \
         AND NOT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user)",
        &[],
    ) else {
        skip_or_fail(
            NAME,
            "missing object: role_gateway session identity probe",
            ExternalDep::Postgres,
        );
        return None;
    };
    if !role_ok.get::<_, bool>(0) {
        skip_or_fail(
            NAME,
            "invalid object: role_gateway must be LOGIN, non-superuser, non-bypassrls",
            ExternalDep::Postgres,
        );
        return None;
    }
    Some(dsn)
}

struct Fixture {
    admin: Client,
    tenant_id: Uuid,
    evidence_id: Uuid,
    dsn: String,
    direct_get_barrier: Option<String>,
    /// Held for the fixture's whole life (dropped after `Drop::drop` cleans up). The snapshot
    /// token is the cluster-wide `pg_current_snapshot()`, so the byte-identity precondition
    /// needs a quiet cluster: sibling tests of this binary must not commit between the two
    /// assemblies. On the dedicated 61719 container the binary never ran in parallel with
    /// anything that mattered; on the shared `HUMAUX_TEST_PG_DSN` target (ADR-0050 D-B) this
    /// lock restores the "本 harness 独占" precondition the module doc states.
    _quiet: std::sync::MutexGuard<'static, ()>,
}

/// One fixture at a time inside this binary — see [`Fixture::_quiet`].
static QUIET: std::sync::Mutex<()> = std::sync::Mutex::new(());

impl Drop for Fixture {
    fn drop(&mut self) {
        // Card-31 pattern (card 33 leak fix): each seeded PRIMARY memory_evidence link enqueued a
        // DERIVED_CONSOLIDATE job (0164 trigger). They are deleted in their own autocommit
        // statement first, so a failure in the transaction below (which ends in the tenant delete)
        // cannot roll the job delete back.
        if let Err(error) = self.admin.execute(
            "DELETE FROM ops.jobs WHERE tenant_id = $1",
            &[&self.tenant_id],
        ) {
            eprintln!(
                "g80 handoff fixture job cleanup failed for tenant {}: {error}",
                self.tenant_id
            );
        }
        let cleanup = (|| -> Result<(), postgres::Error> {
            let mut txn = self.admin.transaction()?;
            if let Some(name) = self.direct_get_barrier.take() {
                txn.batch_execute(&format!(
                    "DROP POLICY IF EXISTS {name} ON private.memory_records; \
                     DROP FUNCTION IF EXISTS private.{name}()"
                ))?;
            }
            txn.batch_execute(&format!(
                "DELETE FROM private.context_bindings WHERE tenant_id = '{0}'; \
             DELETE FROM ops.selection_snapshot_items WHERE tenant_id = '{0}'; \
             DELETE FROM ops.selection_snapshots WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_log WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_checkpoints WHERE tenant_id = '{0}'; \
             DELETE FROM private.memory_evidence WHERE memory_id IN \
               (SELECT memory_id FROM private.memory_records WHERE tenant_id = '{0}'); \
             DELETE FROM private.memory_records WHERE tenant_id = '{0}'; \
             DELETE FROM private.events WHERE event_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{0}'); \
             DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
             DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}'; \
             DELETE FROM control.tenants WHERE tenant_id = '{0}';",
                self.tenant_id
            ))?;
            txn.commit()
        })();
        if let Err(error) = cleanup {
            if std::thread::panicking() {
                eprintln!("g80 handoff fixture cleanup failed: {error}");
            } else {
                panic!("g80 handoff fixture cleanup failed: {error}");
            }
        }
    }
}

fn setup() -> Option<Fixture> {
    // A test that panicked while holding the lock poisons it; the next fixture is still valid.
    let quiet = QUIET
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(NAME, "missing object: Postgres DSN", ExternalDep::Postgres);
        return None;
    };
    let Ok(options) = PgConnectOptions::from_str(&dsn) else {
        skip_or_fail(
            NAME,
            "invalid object: owner PostgreSQL DSN",
            ExternalDep::Postgres,
        );
        return None;
    };
    // ADR-0047 D-D / ADR-0050 D-B: the owner DSN *defines* the fixture target (any loopback
    // port/database); only loopback and a query-free DSN are enforced here, because this
    // fixture installs a RESTRICTIVE policy as the owner. Role DSNs must then name the same
    // target (`gateway_dsn`).
    if options.get_host() != "127.0.0.1" || dsn.contains(['?', '#']) {
        skip_or_fail(
            NAME,
            "invalid object: loopback owner fixture DSN",
            ExternalDep::Postgres,
        );
        return None;
    }
    // dep: PostgreSQL(owner) — HUMAUX_TEST_PG_DSN: seeds/cleans the fixture tenant and installs the barrier policy.
    // dep: PostgreSQL(any) — opens the role-scoped connection for `setup`
    let Ok(mut admin) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(NAME, "missing object: live Postgres", ExternalDep::Postgres);
        return None;
    };
    let gateway_dsn = gateway_dsn()?;
    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
            &[&"g80_31_handoff throwaway tenant"],
        )
        .expect("seed tenant")
        .get(0);
    let reasoning_domain_id: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
             VALUES ($1, 'g80_31 domain') RETURNING reasoning_domain_id",
            &[&tenant_id],
        )
        .expect("seed reasoning domain")
        .get(0);
    let evidence_id: Uuid = admin
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', $2, 'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $3) \
             RETURNING evidence_id",
            &[&tenant_id, &vec![0u8; 32], &reasoning_domain_id],
        )
        .expect("seed evidence")
        .get(0);
    admin
        .execute(
            "INSERT INTO private.events (event_id, event_kind, payload) \
             VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
            &[&evidence_id],
        )
        .expect("seed event");
    Some(Fixture {
        admin,
        tenant_id,
        evidence_id,
        dsn: gateway_dsn,
        direct_get_barrier: None,
        _quiet: quiet,
    })
}

/// 批量播 memory（SNAPSHOT 档 evidence，进得了 lane）。返回 id。
fn seed_many(f: &mut Fixture, authority: &str, n: usize) -> Vec<Uuid> {
    let confidence: f32 = 0.9;
    let mut out = Vec::with_capacity(n);
    let mut txn = f.admin.transaction().expect("begin");
    for _ in 0..n {
        let id: Uuid = txn
            .query_one(
                "INSERT INTO private.memory_records \
                   (tenant_id, memory_type, content, visibility_class, \
                    authority_class, confidence, status, asserted_at) \
                 VALUES ($1, 'NOTE', $2, 'TENANT_SHARED', $3, $4, 'active', now()) \
                 RETURNING memory_id",
                &[
                    &f.tenant_id,
                    &serde_json::json!({"fixture": NAME}),
                    &authority,
                    &confidence,
                ],
            )
            .expect("insert memory")
            .get(0);
        // dep: PostgreSQL(any) — pool/txn query execution
        txn.execute(
            "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, grounding_mode) \
             VALUES ($1, $2, 'PRIMARY', 'SNAPSHOT')",
            &[&id, &f.evidence_id],
        )
        .expect("link evidence");
        out.push(id);
    }
    txn.commit().expect("commit");
    out
}

fn scope_for(tenant_id: Uuid) -> Scope {
    Scope {
        tenant_id: TenantId(tenant_id),
        user_id: None,
        workspace_id: None,
        repository_id: None,
        task_id: None,
        run_id: None,
        agent_id: None,
    }
}

fn authorization_for(tenant_id: Uuid) -> AuthorizationScope {
    AuthorizationScope::new(
        TenantId(tenant_id),
        PrincipalId::new(),
        None,
        BoundedSet::new([]).expect("empty workspace grant set"),
    )
}

fn budget() -> ContextBudget {
    // 200 条噪声不进 mandatory（authority 不够），预算给足 constraint。
    ContextBudget::new(1_000_000, 500_000).expect("budget")
}

/// 每次装配自己连一个 pool：typed pool 刻意无 `Clone`（§6.2.3 闭集），测试迁就它
/// 而不是撬开它——两次装配本来就该是两个独立事务。
async fn one_handoff(dsn: &str, tenant: Uuid) -> Handoff {
    // dep: PostgreSQL(role_gateway) — RuntimeDbPool under test, the runtime role's real pool.
    // dep: PostgreSQL(role_gateway) — opens the role-scoped connection for `one_handoff`
    let pool = RuntimeDbPool::connect(dsn).await.expect("pool");
    let adapter = ContextReadAdapter::new(pool, authorization_for(tenant));
    assemble_handoff(&adapter, &scope_for(tenant), budget())
        .await
        .expect("assemble handoff")
}

fn stream_family_for(tenant: Uuid) -> StreamFamily {
    StreamFamily::new(
        TenantId(tenant),
        "tenant",
        tenant,
        "private_reasoning",
        "context",
    )
}

fn seed_stream_ledger(fixture: &mut Fixture, key: &StreamKey, highwater: i64, states: &[&str]) {
    fixture
        .admin
        .execute(
            "INSERT INTO projection.stream_checkpoints \
               (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                issued_highwater) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &key.tenant_id.0,
                &key.scope_kind,
                &key.scope_id,
                &key.domain,
                &key.projection_kind,
                &key.projection_version,
                &highwater,
            ],
        )
        .expect("seed stream checkpoint");
    for (index, state) in states.iter().enumerate() {
        let state = *state;
        let stream_seq = i64::try_from(index + 1).expect("fixture stream sequence");
        let settled = matches!(
            state,
            "DONE" | "SKIPPED_BY_POLICY" | "TOMBSTONED" | "FAILED"
        );
        fixture
            .admin
            .execute(
                "INSERT INTO projection.stream_log \
                   (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                    stream_seq, commit_seq, state, settled_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, \
                         CASE WHEN $10 THEN now() ELSE NULL END)",
                &[
                    &key.tenant_id.0,
                    &key.scope_kind,
                    &key.scope_id,
                    &key.domain,
                    &key.projection_kind,
                    &key.projection_version,
                    &stream_seq,
                    &stream_seq,
                    &state,
                    &settled,
                ],
            )
            .expect("seed stream log row");
    }
}

async fn one_materialized(
    dsn: &str,
    authorization: &AuthorizationScope,
    scope: &Scope,
    budget: ContextBudget,
    family: &StreamFamily,
) -> Result<MaterializedContext, ErrorCode> {
    // dep: PostgreSQL(role_gateway) — RuntimeDbPool under test, the runtime role's real pool.
    // dep: PostgreSQL(role_gateway) — opens the role-scoped connection for `one_materialized`
    let pool = RuntimeDbPool::connect(dsn).await.expect("pool");
    let key = family.with_version("v1");
    assemble_materialized(&pool, authorization, scope, budget, family, &key).await
}

async fn one_direct_memory(
    dsn: &str,
    authorization: &AuthorizationScope,
    scope: &Scope,
    family: &StreamFamily,
    memory_id: Uuid,
) -> Result<MaterializedMemory, ErrorCode> {
    // dep: PostgreSQL(role_gateway) — RuntimeDbPool under test, the runtime role's real pool.
    // dep: PostgreSQL(role_gateway) — opens the role-scoped connection for `one_direct_memory`
    let pool = RuntimeDbPool::connect(dsn).await.expect("pool");
    let key = family.with_version("v1");
    materialize_memory_get(
        &pool,
        authorization,
        scope,
        family,
        &key,
        MemoryId(memory_id),
    )
    .await
}

async fn one_enumerated(
    dsn: &str,
    authorization: &AuthorizationScope,
    scope: &Scope,
    family: &StreamFamily,
    cursor: Option<&str>,
    page_size: u16,
) -> Result<humaux_adapters::context_repo::MaterializedMemoryPage, ErrorCode> {
    // dep: PostgreSQL(role_gateway) — RuntimeDbPool under test, the runtime role's real pool.
    // dep: PostgreSQL(role_gateway) — opens the role-scoped connection for `one_enumerated`
    let pool = RuntimeDbPool::connect(dsn).await.expect("pool");
    let key = family.with_version("v1");
    materialize_memory_enumeration(
        &pool,
        authorization,
        scope,
        family,
        &key,
        MemoryEnumerationParams {
            cursor,
            page_size,
            ttl: Duration::from_secs(30),
            mac_key: ENUMERATION_MAC_KEY,
            subject_id: None,
        },
    )
    .await
}

fn install_direct_get_snapshot_barrier(fixture: &mut Fixture) -> (i32, i32) {
    let name = format!("g80_direct_get_{}", fixture.tenant_id.simple());
    let lock_a = i32::from_be_bytes(fixture.tenant_id.as_bytes()[..4].try_into().expect("uuid"));
    let lock_b = 8031;
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE FUNCTION private.{name}() RETURNS boolean LANGUAGE plpgsql AS $$ \
             BEGIN PERFORM pg_advisory_xact_lock({lock_a}, {lock_b}); RETURN true; END; $$; \
             CREATE POLICY {name} ON private.memory_records AS RESTRICTIVE FOR SELECT \
             TO role_gateway USING (CASE WHEN tenant_id = '{tenant_id}'::uuid \
                 THEN private.{name}() ELSE true END)",
            tenant_id = fixture.tenant_id,
        ))
        .expect("install isolated direct-get RLS lock barrier");
    fixture.direct_get_barrier = Some(name);
    (lock_a, lock_b)
}

fn waits_on_advisory_lock(
    admin: &mut Client,
    application_name: &str,
    holder_pid: i32,
) -> Result<bool, String> {
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        let waiting: bool = admin
            .query_one(
                "SELECT EXISTS ( \
                   SELECT 1 FROM pg_locks waiter \
                   JOIN pg_stat_activity actor ON actor.pid = waiter.pid \
                   JOIN pg_locks holder \
                     ON holder.locktype = waiter.locktype \
                    AND holder.classid = waiter.classid \
                    AND holder.objid = waiter.objid \
                    AND holder.objsubid = waiter.objsubid \
                   WHERE actor.application_name = $1 AND waiter.locktype = 'advisory' \
                     AND NOT waiter.granted AND holder.granted AND holder.pid = $2 \
                 )",
                &[&application_name, &holder_pid],
            )
            .map_err(|error| format!("inspect advisory-lock waiter: {error}"))?
            .get(0);
        if waiting {
            return Ok(true);
        }
        std::thread::yield_now();
    }
    Ok(false)
}

fn mutate_direct_get_facts(
    fixture: &mut Fixture,
    key: &StreamKey,
    memory_id: Uuid,
) -> Result<(), String> {
    let mut mutation = fixture
        .admin
        .transaction()
        .map_err(|error| format!("begin concurrent mutation: {error}"))?;
    mutation
        .execute(
            "UPDATE private.memory_records SET content=$2 WHERE memory_id=$1",
            &[&memory_id, &serde_json::json!({"fixture": "after"})],
        )
        .map_err(|error| format!("mutate body after snapshot begins: {error}"))?;
    mutation
        .execute(
            "UPDATE private.memory_evidence \
             SET grounding_mode='LIVE', recorded_version=NULL WHERE memory_id=$1",
            &[&memory_id],
        )
        .map_err(|error| format!("mutate grounding after snapshot begins: {error}"))?;
    mutation
        .execute(
            "UPDATE projection.stream_checkpoints SET issued_highwater=2 \
             WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
               AND projection_kind=$5 AND projection_version=$6",
            &[
                &fixture.tenant_id,
                &key.scope_kind,
                &key.scope_id,
                &key.domain,
                &key.projection_kind,
                &key.projection_version,
            ],
        )
        .map_err(|error| format!("mutate ledger after snapshot begins: {error}"))?;
    mutation
        .commit()
        .map_err(|error| format!("commit concurrent mutation: {error}"))?;
    Ok(())
}

fn link_secret_source(fixture: &mut Fixture, memory_id: Uuid) {
    let secret_evidence_id: Uuid = fixture
        .admin
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             SELECT tenant_id, 'EVENT', $2, 'SECRET_MATERIAL', 'DirectUserInput', \
                    'TENANT_SHARED', reasoning_domain_id \
             FROM private.evidence_objects WHERE evidence_id = $1 RETURNING evidence_id",
            &[&fixture.evidence_id, &vec![1u8; 32]],
        )
        .expect("seed secret source")
        .get(0);
    fixture
        .admin
        .execute(
            "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, grounding_mode) \
             VALUES ($1, $2, 'SUPPORTING', 'SNAPSHOT')",
            &[&memory_id, &secret_evidence_id],
        )
        .expect("link secret source");
}

/// 判据 (a)+(b)：1 条 ProjectConstraint + 200 条噪声，两次独立装配（两个事务）——
/// token 相等（前置）∧ 逐字节相同 ∧ constraint 在 mandatory ∧ 噪声不进。
#[test]
fn same_snapshot_assembles_byte_identical_handoffs_with_the_constraint_present() {
    let Some(mut f) = setup() else { return };
    let constraint = seed_many(&mut f, "ProjectConstraint", 1)[0];
    let noise = seed_many(&mut f, "PrivateKnowledge", 200);

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let (h1, h2) = rt.block_on(async {
        let h1 = one_handoff(&f.dsn, f.tenant_id).await;
        let h2 = one_handoff(&f.dsn, f.tenant_id).await;
        (h1, h2)
    });

    // 前置：同一世界。token 不等 ⇒ 有并发写者 ⇒ 前置没满足（显式可观测，不是假设）。
    assert_eq!(
        h1.snapshot_token_sha256, h2.snapshot_token_sha256,
        "前置不满足：两次装配读到了不同快照（fixture 库应当静默）"
    );

    // §57.1 Phase 8 前半：逐字节相同。
    assert_eq!(
        h1.canonical_bytes(),
        h2.canonical_bytes(),
        "同一快照两次装配必须逐字节相同（G80-31）"
    );
    assert_eq!(h1.sha256(), h2.sha256());

    // 后半（G25-1 的 e2e 面）：constraint 在，200 噪声一条都不进。
    let ids: Vec<String> = h1.mandatory.iter().map(|i| i.memory_id.clone()).collect();
    assert!(
        ids.contains(&constraint.to_string()),
        "ProjectConstraint 必须在 mandatory 里"
    );
    for n in &noise {
        assert!(
            !ids.contains(&n.to_string()),
            "PrivateKnowledge 噪声不得进 mandatory（选取是机械 authority 规则）"
        );
    }
    assert_eq!(h1.counts.mandatory_returned, 1);
    assert!(!h1.counts.overflow);
}

/// 判据 (c)：supersede 之后重装配必须消失——「不可淘汰」的反向对照，防「永不丢」过拟合。
#[test]
fn a_superseded_constraint_disappears_on_reassembly() {
    let Some(mut f) = setup() else { return };
    let constraint = seed_many(&mut f, "ProjectConstraint", 1)[0];

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let before = rt.block_on(one_handoff(&f.dsn, f.tenant_id));
    // supersede：同步 Client 在 block_on **之外**（runtime 套 runtime 的教训），
    // 也不需要撬 typed pool 的私有 `pool()`。
    f.admin
        .execute(
            // G59-4 CHECK：superseded_by 非 NULL ⇔ status='superseded'，两列一起改
            // （第一次只改一列被 CHECK 顶回来——闸在工作）。
            "UPDATE private.memory_records \
             SET superseded_by = memory_id, status = 'superseded' WHERE memory_id = $1",
            &[&constraint],
        )
        .expect("supersede");
    let after = rt.block_on(one_handoff(&f.dsn, f.tenant_id));

    assert!(
        before
            .mandatory
            .iter()
            .any(|i| i.memory_id == constraint.to_string()),
        "supersede 前必须在"
    );
    assert!(
        !after
            .mandatory
            .iter()
            .any(|i| i.memory_id == constraint.to_string()),
        "supersede 后必须消失——否则「不可淘汰」是过拟合出来的「永不丢」"
    );
    assert_ne!(
        before.canonical_bytes(),
        after.canonical_bytes(),
        "世界变了，字节必须变"
    );
}

/// 判据 (e)：钉 3 条、1 条低 authority——pinned.expected==3、returned==2、excluded 具名。
/// **oracle 是测试播种的外部真值**，不是内部守恒式（守恒式在 expected 内生时恒真）。
#[test]
fn pinned_three_with_one_low_authority_reports_expected_three_returned_two() {
    let Some(mut f) = setup() else { return };
    // card 22c (ADR-0046, §25.4.B(6)): these two used to be seeded at `ExplicitTaskContext`
    // precisely because it cleared the pinned floor WITHOUT matching `project_active_constraints_v1`.
    // A stored 6 is now refused outright (I-STORE), and `ProjectConstraint` — the only class
    // left at or above that floor — is claimed by the constraints selector unconditionally.
    // So "pinned-only delivery above the floor" no longer exists, and the judgment this test
    // carries (§25.5: 钉了的每一条的去向都必须可观测，没有一条静默消失) is asserted in the
    // shape it now has: 1 below the floor + 2 taken over by Mandatory = 3 named exclusions.
    let high = seed_many(&mut f, "ProjectConstraint", 2);
    let low = seed_many(&mut f, "PrivateKnowledge", 1)[0];

    for id in high.iter().chain(std::iter::once(&low)) {
        f.admin
            .execute(
                "INSERT INTO private.context_bindings \
                   (tenant_id, memory_id, mode, scope_kind, created_by) \
                 VALUES ($1, $2, 'PINNED', 'TENANT', $1)",
                &[&f.tenant_id, id],
            )
            .expect("pin");
    }

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let h = rt.block_on(one_handoff(&f.dsn, f.tenant_id));

    // The two high rows are delivered — by the Mandatory lane, which takes precedence for a
    // memory that both lanes reach (`PinnedLane::excluding_mandatory`). Asserting this first
    // is what keeps the exclusion count below from reading as "two pins were dropped".
    assert_eq!(
        h.mandatory.len(),
        2,
        "两条 ProjectConstraint 由 Mandatory 承载，不是消失了"
    );
    assert_eq!(h.counts.pinned_expected, 3, "外部真值：钉了 3 条");
    assert_eq!(
        h.counts.pinned_returned, 0,
        "钉住的三条里，两条被 Mandatory 承载、一条低于下限——独立 Pinned 行为 0"
    );
    assert_eq!(
        h.counts.pinned_excluded, 3,
        "被排除的必须逐条计数——「钉 3 带 0」不可观测就是静默截断"
    );
}

/// The manifest and final bodies are read in the same repeatable-read snapshot.
#[test]
#[allow(clippy::too_many_lines)] // One fixture keeps manifest/body snapshot setup and its paired mutation oracle together.
fn materialized_context_reads_manifest_and_body_from_one_snapshot() {
    let Some(mut fixture) = setup() else { return };
    let memory_ids = seed_many(&mut fixture, "ProjectConstraint", 2);
    fixture
        .admin
        .execute(
            "UPDATE private.memory_evidence \
             SET grounding_mode='LIVE', recorded_version='g80-31-live-v1' \
             WHERE memory_id=ANY($1::uuid[])",
            &[&memory_ids],
        )
        .expect("versioned LIVE edges require a fresh observation and remain NotJudged");
    fixture
        .admin
        .execute(
            "INSERT INTO private.context_bindings \
               (tenant_id, memory_id, mode, scope_kind, created_by) \
             VALUES ($1, $2, 'MANDATORY', 'TENANT', $1)",
            &[&fixture.tenant_id, &memory_ids[0]],
        )
        .expect("same memory selected by constraint and explicit mandatory selectors");
    fixture
        .admin
        .execute(
            "INSERT INTO private.context_bindings \
               (tenant_id, memory_id, mode, scope_kind, created_by) \
             VALUES ($1, $2, 'PINNED', 'TENANT', $1)",
            &[&fixture.tenant_id, &memory_ids[0]],
        )
        .expect("seed cross-lane pinned duplicate");
    let family = stream_family_for(fixture.tenant_id);
    let primary_key = family.with_version("v1");
    seed_stream_ledger(
        &mut fixture,
        &primary_key,
        6,
        &[
            "DONE",
            "SKIPPED_BY_POLICY",
            "TOMBSTONED",
            "FAILED",
            "PROCESSING",
            "DONE",
        ],
    );
    seed_stream_ledger(
        &mut fixture,
        &family.with_version("v2"),
        99,
        &["PROCESSING"],
    );
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let result = rt.block_on(one_materialized(
        &fixture.dsn,
        &authorization_for(fixture.tenant_id),
        &scope_for(fixture.tenant_id),
        budget(),
        &family,
    ));
    let context = result.expect("materialize context");

    assert!(matches!(context.outcome, ContextOutcome::Compiled(_)));
    assert!(context.ledger.is_closed());
    let counts = context.ledger.counts();
    assert_eq!(counts.expected(), 6);
    assert_eq!(counts.done(), 4);
    assert_eq!(counts.deleted(), 1);
    assert_eq!(counts.skipped(), 1);
    assert_eq!(counts.open_gaps(), 1);
    assert_eq!(counts.pending(), 1);
    assert_eq!(context.grounding.current, 0);
    assert_eq!(context.grounding.not_judged, 2);
    assert_eq!(context.handoff.counts.mandatory_expected, 2);
    assert_eq!(context.handoff.counts.mandatory_returned, 2);
    assert_eq!(context.handoff.counts.mandatory_missing, 0);
    assert_eq!(context.handoff.counts.pinned_expected, 1);
    assert_eq!(context.handoff.counts.pinned_returned, 0);
    assert_eq!(context.handoff.counts.pinned_excluded, 1);
    assert_eq!(
        context.bodies.snapshot.context_snapshot_seq,
        context.handoff.context_snapshot_seq
    );
    assert_eq!(
        context.bodies.snapshot.snapshot_token_sha256,
        context.handoff.snapshot_token_sha256
    );
    let emitted_ids = context
        .handoff
        .mandatory
        .iter()
        .chain(&context.handoff.pinned)
        .map(|item| Uuid::parse_str(&item.memory_id).expect("handoff UUID"))
        .collect::<Vec<_>>();
    let body_ids = context
        .bodies
        .items
        .iter()
        .filter_map(|item| match item {
            MaterializedItem::Memory { memory_id, .. } => Some(*memory_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        body_ids, emitted_ids,
        "body order follows compiled manifest order"
    );
    let mut unique_emitted_ids = emitted_ids.clone();
    unique_emitted_ids.sort_unstable();
    unique_emitted_ids.dedup();
    assert_eq!(
        unique_emitted_ids.len(),
        emitted_ids.len(),
        "multi-selector and cross-lane duplicates must each materialize once"
    );
    assert_eq!(
        context.grounding.not_judged,
        u32::try_from(emitted_ids.len()).expect("fixture count"),
        "grounding tallies physical Context items once"
    );
    for memory_id in memory_ids {
        assert!(emitted_ids.contains(&memory_id));
        assert!(context.bodies.items.iter().any(|item| {
            matches!(item, MaterializedItem::Memory { memory_id: id, content }
                if *id == memory_id && *content == serde_json::json!({"fixture": NAME}))
        }));
    }
}

/// Mandatory overflow still returns a manifest, but never body data.
#[test]
fn materialized_context_overflow_has_no_bodies() {
    let Some(mut fixture) = setup() else { return };
    seed_many(&mut fixture, "ProjectConstraint", 1);
    let zero_budget = ContextBudget::new(0, 0).expect("zero budget");
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let context = rt
        .block_on(one_materialized(
            &fixture.dsn,
            &authorization_for(fixture.tenant_id),
            &scope_for(fixture.tenant_id),
            zero_budget,
            &stream_family_for(fixture.tenant_id),
        ))
        .expect("overflow is a handoff outcome");

    assert!(matches!(context.outcome, ContextOutcome::Overflow(_)));
    assert!(context.handoff.counts.overflow);
    assert!(context.bodies.items.is_empty());
    assert_eq!(
        context.bodies.snapshot.snapshot_token_sha256,
        context.handoff.snapshot_token_sha256
    );
}

/// A key cannot route a request to a different authorized workspace than its requested scope.
#[test]
fn materialized_context_rejects_authorized_but_mismatched_workspace_key() {
    let Some(fixture) = setup() else { return };
    let requested_workspace = WorkspaceId(Uuid::new_v4());
    let routed_workspace = WorkspaceId(Uuid::new_v4());
    let authorization = AuthorizationScope::new(
        TenantId(fixture.tenant_id),
        PrincipalId::new(),
        None,
        BoundedSet::new([requested_workspace, routed_workspace]).expect("workspace grants"),
    );
    let scope = Scope {
        workspace_id: Some(requested_workspace),
        ..scope_for(fixture.tenant_id)
    };
    let family = StreamFamily::new(
        TenantId(fixture.tenant_id),
        "workspace",
        routed_workspace.0,
        "private_reasoning",
        "context",
    );
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let result = rt.block_on(one_materialized(
        &fixture.dsn,
        &authorization,
        &scope,
        budget(),
        &family,
    ));

    assert!(matches!(result, Err(ErrorCode::Forbidden)));
}

/// A compiled manifest must fail closed when final body policy hides one of its sources.
#[test]
fn materialized_context_fails_closed_when_manifest_memory_has_secret_source() {
    let Some(mut fixture) = setup() else { return };
    let memory_id = seed_many(&mut fixture, "ProjectConstraint", 1)[0];
    link_secret_source(&mut fixture, memory_id);
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let result = rt.block_on(one_materialized(
        &fixture.dsn,
        &authorization_for(fixture.tenant_id),
        &scope_for(fixture.tenant_id),
        budget(),
        &stream_family_for(fixture.tenant_id),
    ));

    assert!(matches!(result, Err(ErrorCode::DependencyUnavailable)));
}

/// DirectGet's body, grounding and ledger must stay in one RR snapshot even when a real
/// role_gateway actor is held in its first RLS-qualified Memory read and an owner mutates all
/// three facts before it proceeds. The advisory lock and pg_locks witness are the concurrency
/// evidence; no time delay decides the ordering.
#[test]
#[allow(clippy::too_many_lines)] // Keep the real barrier, cleanup and independent fresh-read control together.
fn direct_get_keeps_body_grounding_and_ledger_on_one_snapshot() {
    let Some(mut fixture) = setup() else { return };
    let memory_id = seed_many(&mut fixture, "ProjectConstraint", 1)[0];
    let family = stream_family_for(fixture.tenant_id);
    let key = family.with_version("v1");
    seed_stream_ledger(&mut fixture, &key, 1, &["DONE"]);
    let (lock_a, lock_b) = install_direct_get_snapshot_barrier(&mut fixture);
    let application_name = format!("direct-get-{}", Uuid::new_v4().simple());
    let actor_dsn = format!("{}?application_name={application_name}", fixture.dsn);
    let authorization = authorization_for(fixture.tenant_id);
    let scope = scope_for(fixture.tenant_id);
    let actor_family = family.clone();

    // dep: PostgreSQL(role_gateway) — holds the advisory barrier the actor blocks on.
    // dep: PostgreSQL(any) — opens the role-scoped connection for `direct_get_keeps_body_grounding_and_ledger_on_one_snapshot`
    let mut holder = Client::connect(&fixture.dsn, NoTls).expect("gateway lock holder");
    let holder_pid: i32 = holder
        .query_one("SELECT pg_backend_pid()", &[])
        .expect("holder pid")
        .get(0);
    holder
        .query_one("SELECT pg_advisory_lock($1, $2)", &[&lock_a, &lock_b])
        .expect("hold direct-get barrier");
    let actor = std::thread::spawn(move || -> Result<_, String> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|error| format!("direct-get actor runtime: {error}"))?;
        // dep: PostgreSQL(role_gateway) — RuntimeDbPool under test, the runtime role's real pool.
        // dep: PostgreSQL(role_gateway) — opens the role-scoped connection for `direct_get_keeps_body_grounding_and_ledger_on_one_snapshot`
        let pool = runtime
            .block_on(RuntimeDbPool::connect(&actor_dsn))
            .map_err(|error| format!("direct-get actor pool: {error}"))?;
        let actor_key = actor_family.with_version("v1");
        runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(8),
                materialize_memory_get(
                    &pool,
                    &authorization,
                    &scope,
                    &actor_family,
                    &actor_key,
                    MemoryId(memory_id),
                ),
            )
            .await
            .map_err(|_| "direct-get actor exceeded bounded timeout".to_owned())
        })
    });

    let observed_wait = waits_on_advisory_lock(&mut fixture.admin, &application_name, holder_pid);
    let mutation = match observed_wait {
        Ok(true) => mutate_direct_get_facts(&mut fixture, &key, memory_id),
        Ok(false) | Err(_) => Ok(()),
    };
    let unlock = holder.query_one("SELECT pg_advisory_unlock($1, $2)", &[&lock_a, &lock_b]);
    let result = actor
        .join()
        .map_err(|_| "direct-get actor panicked".to_string());

    assert!(
        matches!(observed_wait, Ok(true)),
        "actor must wait on exact lock: {observed_wait:?}"
    );
    assert!(mutation.is_ok(), "snapshot mutation failed: {mutation:?}");
    assert!(
        unlock.is_ok(),
        "release direct-get barrier failed: {unlock:?}"
    );
    let direct = result
        .expect("actor thread joined")
        .expect("actor setup")
        .expect("DirectGet completes after barrier release");
    assert!(matches!(
        direct.bodies.items.as_slice(),
        [MaterializedItem::Memory { memory_id: id, content }]
            if *id == memory_id && *content == serde_json::json!({"fixture": NAME})
    ));
    assert!(direct.bodies.snapshot.context_snapshot_seq > 0);
    assert_eq!(direct.bodies.snapshot.snapshot_token_sha256.len(), 64);
    assert_eq!(direct.grounding.current, 1);
    assert_eq!(direct.grounding.recheck_required, 0);
    assert_eq!(direct.ledger.counts().expected(), 1);
    assert_eq!(direct.ledger.counts().done(), 1);
    assert!(direct.ledger.is_closed());
    let after: serde_json::Value = fixture
        .admin
        .query_one(
            "SELECT content FROM private.memory_records WHERE memory_id=$1",
            &[&memory_id],
        )
        .expect("read committed mutation")
        .get(0);
    assert_eq!(after, serde_json::json!({"fixture": "after"}));
    let runtime = tokio::runtime::Runtime::new().expect("fresh-read runtime");
    let fresh = runtime
        .block_on(one_direct_memory(
            &fixture.dsn,
            &authorization_for(fixture.tenant_id),
            &scope_for(fixture.tenant_id),
            &family,
            memory_id,
        ))
        .expect("fresh read observes the committed changes");
    assert!(matches!(
        fresh.bodies.items.as_slice(),
        [MaterializedItem::Memory { memory_id: id, content }]
            if *id == memory_id && *content == after
    ));
    assert_eq!(fresh.grounding.recheck_required, 1);
    assert_eq!(fresh.ledger.counts().expected(), 2);
    assert!(!fresh.ledger.is_closed());
}

/// DirectGet preserves the pre-lookup scope error, while lifecycle or source filtering is the
/// single object-level NotFound result.
#[test]
fn direct_get_rejects_wrong_key_and_hidden_lifecycle_sources_as_not_found() {
    let Some(mut fixture) = setup() else { return };
    let memory_id = seed_many(&mut fixture, "ProjectConstraint", 1)[0];
    let family = stream_family_for(fixture.tenant_id);
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let wrong_family = StreamFamily::new(
        TenantId(fixture.tenant_id),
        "workspace",
        Uuid::new_v4(),
        "private_reasoning",
        "context",
    );
    let forbidden = rt.block_on(one_direct_memory(
        &fixture.dsn,
        &authorization_for(fixture.tenant_id),
        &scope_for(fixture.tenant_id),
        &wrong_family,
        memory_id,
    ));
    assert!(matches!(forbidden, Err(ErrorCode::Forbidden)));

    fixture
        .admin
        .execute(
            "UPDATE private.memory_records SET status='revoked' WHERE memory_id=$1",
            &[&memory_id],
        )
        .expect("revoke direct-get memory");
    let revoked = rt.block_on(one_direct_memory(
        &fixture.dsn,
        &authorization_for(fixture.tenant_id),
        &scope_for(fixture.tenant_id),
        &family,
        memory_id,
    ));
    assert!(matches!(revoked, Err(ErrorCode::NotFound)));

    fixture
        .admin
        .execute(
            "UPDATE private.memory_records SET status='active' WHERE memory_id=$1",
            &[&memory_id],
        )
        .expect("restore lifecycle for source filter");
    link_secret_source(&mut fixture, memory_id);
    let secret = rt.block_on(one_direct_memory(
        &fixture.dsn,
        &authorization_for(fixture.tenant_id),
        &scope_for(fixture.tenant_id),
        &family,
        memory_id,
    ));
    assert!(matches!(secret, Err(ErrorCode::NotFound)));
}

/// Enumeration's page-1 manifest, final body, grounding and ledger read from one RR snapshot.
/// The actor blocks on its first RLS-qualified Memory query; an owner changes all three facts
/// only after `pg_locks` proves the actor is waiting, so no sleep establishes the ordering.
#[test]
#[allow(clippy::too_many_lines)]
fn enumeration_keeps_body_grounding_and_ledger_on_one_snapshot() {
    let Some(mut fixture) = setup() else { return };
    let memory_id = seed_many(&mut fixture, "ProjectConstraint", 1)[0];
    let family = stream_family_for(fixture.tenant_id);
    let key = family.with_version("v1");
    seed_stream_ledger(&mut fixture, &key, 1, &["DONE"]);
    let (lock_a, lock_b) = install_direct_get_snapshot_barrier(&mut fixture);
    let application_name = format!("enumeration-{}", Uuid::new_v4().simple());
    let actor_dsn = format!("{}?application_name={application_name}", fixture.dsn);
    let authorization = authorization_for(fixture.tenant_id);
    let scope = scope_for(fixture.tenant_id);
    let actor_family = family.clone();

    // dep: PostgreSQL(role_gateway) — holds the advisory barrier the actor blocks on.
    // dep: PostgreSQL(any) — opens the role-scoped connection for `enumeration_keeps_body_grounding_and_ledger_on_one_snapshot`
    let mut holder = Client::connect(&fixture.dsn, NoTls).expect("gateway lock holder");
    let holder_pid: i32 = holder
        .query_one("SELECT pg_backend_pid()", &[])
        .expect("holder pid")
        .get(0);
    holder
        .query_one("SELECT pg_advisory_lock($1, $2)", &[&lock_a, &lock_b])
        .expect("hold enumeration barrier");
    let actor = std::thread::spawn(move || -> Result<_, String> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|error| format!("enumeration actor runtime: {error}"))?;
        // dep: PostgreSQL(role_gateway) — RuntimeDbPool under test, the runtime role's real pool.
        // dep: PostgreSQL(role_gateway) — opens the role-scoped connection for `enumeration_keeps_body_grounding_and_ledger_on_one_snapshot`
        let pool = runtime
            .block_on(RuntimeDbPool::connect(&actor_dsn))
            .map_err(|error| format!("enumeration actor pool: {error}"))?;
        let actor_key = actor_family.with_version("v1");
        runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(8),
                materialize_memory_enumeration(
                    &pool,
                    &authorization,
                    &scope,
                    &actor_family,
                    &actor_key,
                    MemoryEnumerationParams {
                        cursor: None,
                        page_size: 1,
                        ttl: Duration::from_secs(30),
                        mac_key: ENUMERATION_MAC_KEY,
                        subject_id: None,
                    },
                ),
            )
            .await
            .map_err(|_| "enumeration actor exceeded bounded timeout".to_owned())
        })
    });

    let observed_wait = waits_on_advisory_lock(&mut fixture.admin, &application_name, holder_pid);
    let mutation = match observed_wait {
        Ok(true) => mutate_direct_get_facts(&mut fixture, &key, memory_id),
        Ok(false) | Err(_) => Ok(()),
    };
    let unlock = holder.query_one("SELECT pg_advisory_unlock($1, $2)", &[&lock_a, &lock_b]);
    let result = actor
        .join()
        .map_err(|_| "enumeration actor panicked".to_string());

    assert!(
        matches!(observed_wait, Ok(true)),
        "actor lock witness: {observed_wait:?}"
    );
    assert!(mutation.is_ok(), "snapshot mutation failed: {mutation:?}");
    assert!(
        unlock.is_ok(),
        "release enumeration barrier failed: {unlock:?}"
    );
    let page = result
        .expect("actor thread joined")
        .expect("actor setup")
        .expect("enumeration completes after barrier release");
    assert!(matches!(
        page.memory.bodies.items.as_slice(),
        [MaterializedItem::Memory { memory_id: id, content }]
            if *id == memory_id && *content == serde_json::json!({"fixture": NAME})
    ));
    assert_eq!(page.memory.grounding.current, 1);
    assert_eq!(page.memory.grounding.recheck_required, 0);
    assert_eq!(page.memory.ledger.counts().expected(), 1);
    assert_eq!(page.memory.ledger.counts().done(), 1);
    assert!(page.memory.ledger.is_closed());

    let runtime = tokio::runtime::Runtime::new().expect("fresh enumeration runtime");
    let fresh = runtime
        .block_on(one_enumerated(
            &fixture.dsn,
            &authorization_for(fixture.tenant_id),
            &scope_for(fixture.tenant_id),
            &family,
            None,
            1,
        ))
        .expect("fresh enumeration observes committed facts");
    assert!(matches!(
        fresh.memory.bodies.items.as_slice(),
        [MaterializedItem::Memory { memory_id: id, content }]
            if *id == memory_id && *content == serde_json::json!({"fixture": "after"})
    ));
    assert_eq!(fresh.memory.grounding.current, 0);
    assert_eq!(fresh.memory.grounding.recheck_required, 1);
    assert_eq!(fresh.memory.ledger.counts().expected(), 2);
    assert!(!fresh.memory.ledger.is_closed());
}

/// Cursor MACs bind the authenticated subject; invalid page bounds and empty wire cursors fail
/// before any selection read. A workspace scope outside the authenticated grant fails precheck.
#[test]
fn enumeration_rejects_cursor_subject_scope_and_parameter_mismatch() {
    let Some(mut fixture) = setup() else { return };
    seed_many(&mut fixture, "ProjectConstraint", 2);
    let family = stream_family_for(fixture.tenant_id);
    let authorization = authorization_for(fixture.tenant_id);
    let scope = scope_for(fixture.tenant_id);
    let runtime = tokio::runtime::Runtime::new().expect("enumeration bounds runtime");
    let first = runtime
        .block_on(one_enumerated(
            &fixture.dsn,
            &authorization,
            &scope,
            &family,
            None,
            1,
        ))
        .expect("first enumeration page");
    let cursor = first
        .next_cursor
        .as_deref()
        .expect("second page cursor")
        .to_owned();
    let different_principal = authorization_for(fixture.tenant_id);
    let mismatched_principal = runtime.block_on(one_enumerated(
        &fixture.dsn,
        &different_principal,
        &scope,
        &family,
        Some(&cursor),
        1,
    ));
    assert!(matches!(mismatched_principal, Err(ErrorCode::NotFound)));
    for (cursor, page_size) in [(None, 0), (None, 101), (Some(""), 1)] {
        let result = runtime.block_on(one_enumerated(
            &fixture.dsn,
            &authorization,
            &scope,
            &family,
            cursor,
            page_size,
        ));
        assert!(matches!(result, Err(ErrorCode::InvalidInput)));
    }
    let workspace = WorkspaceId(Uuid::new_v4());
    let workspace_scope = Scope {
        workspace_id: Some(workspace),
        ..scope
    };
    let workspace_family = StreamFamily::new(
        TenantId(fixture.tenant_id),
        "workspace",
        workspace.0,
        "private_reasoning",
        "context",
    );
    let workspace_result = runtime.block_on(one_enumerated(
        &fixture.dsn,
        &authorization,
        &workspace_scope,
        &workspace_family,
        Some(&cursor),
        1,
    ));
    assert!(matches!(workspace_result, Err(ErrorCode::Forbidden)));
}
