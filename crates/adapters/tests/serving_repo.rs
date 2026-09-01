//! T5.2+T5.3 integration test — `serving_repo` (§16.2/§16.3) against a real Postgres. Same
//! convention as `stream_repo.rs`: shared tables (`0007_projection.sql`, migration `0065`'s
//! `ux_serving_one`), not a scratch schema — every test scopes rows to a throwaway
//! `control.tenants` row this file owns and cleans up on `Drop`.
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or `ux_serving_one` missing all print a
//! visible SKIP and return.

use std::time::SystemTime;

use humaux_adapters::postgres::{MaintenanceDbPool, RuntimeDbPool};
use humaux_adapters::serving_repo::{self, SwitchOutcome};
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_projection::serving::{ContinuationVerdict, StreamFamily};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::error::SqlState;
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    // See `stream_repo.rs`'s sibling helper for why this exact `options=-c%20role%3D<role>`
    // form (URL-encoded libpq `options`) is used over the `options[role]=X` form.
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

struct Handle {
    rt: tokio::runtime::Runtime,
    /// §16.2 读路由现在收 [`RuntimeDbPool`]（`role_gateway`）——见 `serving_repo::
    /// serving_version` 的 rustdoc。**换掉而不是并存**：多留一个用不到的 pool 会在
    /// `--all-targets -D warnings` 下因 dead_code 变红，而且这两条断言的价值恰恰在于
    /// 实证「gateway 这个角色真能穿 RLS 读到 `serving` 列」。
    runtime: RuntimeDbPool,
    maintenance: MaintenanceDbPool,
    admin: Client,
    tenant_id: Uuid,
    user_id: Uuid,
    auth: AuthorizationScope,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup (repo CLAUDE.md hard rule ④: this file never touches a
        // schema/table of its own, only rows it created under its own throwaway tenant).
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM projection.stream_log WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_checkpoints WHERE tenant_id = '{0}'; \
             DELETE FROM control.memberships WHERE tenant_id = '{0}'; \
             DELETE FROM control.users WHERE user_id = '{1}'; \
             DELETE FROM control.tenants WHERE tenant_id = '{0}';",
            self.tenant_id, self.user_id
        ));
    }
}

struct ServingFixture;

impl DbIntegrationFixture for ServingFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        for table in [
            "projection.stream_log",
            "projection.stream_checkpoints",
            "projection.processing_gaps",
        ] {
            let exists: bool = admin
                .query_one("SELECT to_regclass($1) IS NOT NULL", &[&table])
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
                .get(0);
            if !exists {
                return Err(DbFixtureSkipReason::IsolationSetupFailed(format!(
                    "{table} does not exist — run `cargo xtask migrate` first"
                )));
            }
        }

        let ux_serving_one_exists: bool = admin
            .query_one(
                "SELECT to_regclass('projection.ux_serving_one') IS NOT NULL",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !ux_serving_one_exists {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "projection.ux_serving_one does not exist — run `cargo xtask migrate` \
                 (migration 0065) first"
                    .to_string(),
            ));
        }

        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"serving_repo.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        let user_id: Uuid = admin
            .query_one(
                "INSERT INTO control.users(state) VALUES ('ACTIVE') RETURNING user_id",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        admin
            .execute(
                "INSERT INTO control.memberships(tenant_id, user_id, role, state) \
                 VALUES ($1, $2, 'member', 'ACTIVE')",
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

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let runtime = rt
            .block_on(RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let maintenance = rt
            .block_on(MaintenanceDbPool::connect(&dsn_as_role(
                &dsn,
                "role_maintenance",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            runtime,
            maintenance,
            admin,
            tenant_id,
            user_id,
            auth,
        })
    }
}

fn family(handle: &Handle) -> StreamFamily {
    StreamFamily::new(
        TenantId(handle.tenant_id),
        "workspace",
        Uuid::new_v4(),
        "code",
        "retrieval_card",
    )
}

fn seed_checkpoint(
    admin: &mut Client,
    family: &StreamFamily,
    version: &str,
    serving: bool,
    shadow: bool,
) {
    admin
        .execute(
            "INSERT INTO projection.stream_checkpoints \
               (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                serving, shadow) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            &[
                &family.tenant_id.0,
                &family.scope_kind,
                &family.scope_id,
                &family.domain,
                &family.projection_kind,
                &version,
                &serving,
                &shadow,
            ],
        )
        .expect("seed stream_checkpoints row");
}

/// One `FAILED` `stream_log` row — the cheapest way to make `processing_gaps` (a view over
/// `FAILED | LOST`) report a non-zero `open_gaps` for a given key, same technique
/// `stream_repo.rs`'s own tests use.
fn seed_gap_row(admin: &mut Client, family: &StreamFamily, version: &str, seq: i64) {
    let now = SystemTime::now();
    admin
        .execute(
            "INSERT INTO projection.stream_log \
               (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                stream_seq, commit_seq, state, issued_at, settled_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$7,'FAILED',$8,$8)",
            &[
                &family.tenant_id.0,
                &family.scope_kind,
                &family.scope_id,
                &family.domain,
                &family.projection_kind,
                &version,
                &seq,
                &now,
            ],
        )
        .expect("seed FAILED stream_log row (open gap)");
}

fn read_serving_flags(admin: &mut Client, family: &StreamFamily, version: &str) -> (bool, bool) {
    let row = admin
        .query_one(
            "SELECT serving, shadow FROM projection.stream_checkpoints \
             WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
               AND projection_kind=$5 AND projection_version=$6",
            &[
                &family.tenant_id.0,
                &family.scope_kind,
                &family.scope_id,
                &family.domain,
                &family.projection_kind,
                &version,
            ],
        )
        .expect("row must exist");
    (row.get(0), row.get(1))
}

/// §16.2 DB-layer constraint (migration `0065`): at most one `serving = true` row per stream
/// family, enforced by `ux_serving_one` — not by application discipline. A second `INSERT`
/// with `serving = true` for the same 5-column family must fail with a real
/// `unique_violation`, not merely be discouraged by convention.
#[test]
fn ux_serving_one_rejects_second_serving_row_for_same_family() {
    run_db_fixture::<ServingFixture, _>(
        "ux_serving_one_rejects_second_serving_row_for_same_family",
        |mut handle| {
            let f = family(&handle);
            seed_checkpoint(&mut handle.admin, &f, "v1", true, false);

            let err = handle
                .admin
                .execute(
                    "INSERT INTO projection.stream_checkpoints \
                       (tenant_id, scope_kind, scope_id, domain, projection_kind, \
                        projection_version, serving, shadow) \
                     VALUES ($1,$2,$3,$4,$5,'v2',true,false)",
                    &[
                        &f.tenant_id.0,
                        &f.scope_kind,
                        &f.scope_id,
                        &f.domain,
                        &f.projection_kind,
                    ],
                )
                .expect_err(
                    "a second `serving = true` row for the same family must be rejected by \
                     ux_serving_one, not merely discouraged",
                );
            assert_eq!(
                err.code(),
                Some(&SqlState::UNIQUE_VIOLATION),
                "must fail as a real unique_violation (23505), not some other error class"
            );
        },
    );
}

/// §16.2's sole read-routing entry point: a family with one `serving` row and one `shadow`
/// row (same family, different `projection_version`) — `serving_version` must return the
/// `serving` version and only it. This is "shadow 行不出现在任何读路由结果里" pinned directly
/// at the one function retrieval is meant to call through.
#[test]
fn serving_version_never_returns_a_shadow_only_version() {
    run_db_fixture::<ServingFixture, _>(
        "serving_version_never_returns_a_shadow_only_version",
        |mut handle| {
            let f = family(&handle);
            seed_checkpoint(&mut handle.admin, &f, "v1", true, false);
            seed_checkpoint(&mut handle.admin, &f, "v2", false, true);

            let version = handle
                .rt
                .block_on(serving_repo::serving_version(
                    &handle.runtime,
                    &handle.auth,
                    &f,
                ))
                .expect("read must succeed")
                .expect("a serving row exists");
            assert_eq!(
                version, "v1",
                "must read the serving row, never the shadow row"
            );
        },
    );
}

/// `serving_version` on a family with no `serving = true` row yet (only shadow backfilling)
/// returns `None`, not an error and not the shadow version as a fallback.
#[test]
fn serving_version_is_none_when_only_a_shadow_row_exists() {
    run_db_fixture::<ServingFixture, _>(
        "serving_version_is_none_when_only_a_shadow_row_exists",
        |mut handle| {
            let f = family(&handle);
            seed_checkpoint(&mut handle.admin, &f, "v2", false, true);

            let version = handle
                .rt
                .block_on(serving_repo::serving_version(
                    &handle.runtime,
                    &handle.auth,
                    &f,
                ))
                .expect("read must succeed");
            assert_eq!(version, None);
        },
    );
}

/// 正对照 (positive control): all three §16.3 criteria genuinely true — `visible(shadow) ==
/// visible(serving)`, zero open gaps on the shadow version, `ContinuationVerdict::Pass` —
/// `switch_projection_version` must commit the atomic switch (`SwitchOutcome::Switched`), the
/// old `v1` row must still exist with `serving = false` (rollback target, §16.3), and the new
/// `v2` row must end up `serving = true, shadow = false`.
#[test]
fn switch_projection_version_succeeds_when_all_three_criteria_hold_and_old_row_survives() {
    run_db_fixture::<ServingFixture, _>(
        "switch_projection_version_succeeds_when_all_three_criteria_hold_and_old_row_survives",
        |mut handle| {
            let f = family(&handle);
            seed_checkpoint(&mut handle.admin, &f, "v1", true, false);
            seed_checkpoint(&mut handle.admin, &f, "v2", false, true);
            // v2 has zero FAILED/LOST rows ⇒ shadow_open_gaps == 0.

            let outcome = handle
                .rt
                .block_on(serving_repo::switch_projection_version(
                    &handle.maintenance,
                    &f,
                    "v2",
                    Some(("v2".to_string(), 10)),
                    Some(("v1".to_string(), 10)),
                    ContinuationVerdict::Pass,
                ))
                .expect("switch call must not error");
            assert_eq!(outcome, SwitchOutcome::Switched);

            let (v1_serving, v1_shadow) = read_serving_flags(&mut handle.admin, &f, "v1");
            assert!(!v1_serving, "old version must be retired");
            assert!(!v1_shadow, "old version was never shadow to begin with");
            let (v2_serving, v2_shadow) = read_serving_flags(&mut handle.admin, &f, "v2");
            assert!(v2_serving, "new version must now be serving");
            assert!(!v2_shadow, "new version must no longer be flagged shadow");

            // §16.3 "旧 version 行留作回滚目标" — the v1 row must still be present, not deleted.
            let v1_still_exists: bool = handle
                .admin
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM projection.stream_checkpoints \
                     WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
                       AND projection_kind=$5 AND projection_version='v1')",
                    &[
                        &f.tenant_id.0,
                        &f.scope_kind,
                        &f.scope_id,
                        &f.domain,
                        &f.projection_kind,
                    ],
                )
                .expect("query must succeed")
                .get(0);
            assert!(
                v1_still_exists,
                "old version row must survive as a rollback target"
            );
        },
    );
}

/// §16.3 gate / G80-28 恒真闸检测, exercised through the real DB-backed switch function (the
/// pure-logic form lives in `humaux_projection::serving`'s own tests): baseline has
/// `visible_shadow == visible_serving` and would switch; injecting "shadow 少回填 1 个点"
/// (`visible_shadow` one less, all else — open_gaps, continuation — held identical) must flip
/// the outcome from `Switched` to `Rejected`, and the DB must show **no** state change at all
/// (not even the first of the two `UPDATE`s) — proving the rejection happens strictly before
/// any write, not as a half-applied switch.
#[test]
fn switch_projection_version_rejects_and_writes_nothing_when_shadow_missing_one_visible_point() {
    run_db_fixture::<ServingFixture, _>(
        "switch_projection_version_rejects_and_writes_nothing_when_shadow_missing_one_visible_point",
        |mut handle| {
            let f = family(&handle);
            seed_checkpoint(&mut handle.admin, &f, "v1", true, false);
            seed_checkpoint(&mut handle.admin, &f, "v2", false, true);

            let outcome = handle
                .rt
                .block_on(serving_repo::switch_projection_version(
                    &handle.maintenance,
                    &f,
                    "v2",
                    Some(("v2".to_string(), 9)), // one short of visible_serving — the injected shortfall
                    Some(("v1".to_string(), 10)),
                    ContinuationVerdict::Pass,
                ))
                .expect("switch call must not error even when rejected");
            assert!(
                matches!(outcome, SwitchOutcome::Rejected(_)),
                "a one-point shadow shortfall must be rejected, not silently switched \
                 (if this stays Switched the criterion is a 恒真闸)"
            );

            let (v1_serving, _) = read_serving_flags(&mut handle.admin, &f, "v1");
            let (v2_serving, v2_shadow) = read_serving_flags(&mut handle.admin, &f, "v2");
            assert!(
                v1_serving,
                "rejection must leave the old serving row untouched"
            );
            assert!(
                !v2_serving,
                "rejection must never flip the new row to serving"
            );
            assert!(v2_shadow, "rejection must leave the shadow flag untouched");
        },
    );
}

/// §16.3 criterion ②: a `FAILED` row on the shadow version (`open_gaps == 1`) must reject the
/// switch even though criteria ① and ③ both hold.
#[test]
fn switch_projection_version_rejects_when_shadow_has_an_open_gap() {
    run_db_fixture::<ServingFixture, _>(
        "switch_projection_version_rejects_when_shadow_has_an_open_gap",
        |mut handle| {
            let f = family(&handle);
            seed_checkpoint(&mut handle.admin, &f, "v1", true, false);
            seed_checkpoint(&mut handle.admin, &f, "v2", false, true);
            seed_gap_row(&mut handle.admin, &f, "v2", 1);

            let outcome = handle
                .rt
                .block_on(serving_repo::switch_projection_version(
                    &handle.maintenance,
                    &f,
                    "v2",
                    Some(("v2".to_string(), 10)),
                    Some(("v1".to_string(), 10)),
                    ContinuationVerdict::Pass,
                ))
                .expect("switch call must not error");
            assert!(matches!(outcome, SwitchOutcome::Rejected(_)));

            let (v1_serving, _) = read_serving_flags(&mut handle.admin, &f, "v1");
            assert!(
                v1_serving,
                "rejection must leave the old serving row untouched"
            );
        },
    );
}

/// Blocker regression (§16.3 "旧 version 行留作回滚目标"): `new_version` names no row in
/// `stream_checkpoints` at all — the exact real-PG repro from the review (typo'd version
/// string / a shadow row that was never created / one deleted concurrently). Criteria ①②③ all
/// genuinely hold given the caller's honest inputs (zero `processing_gaps` rows for a
/// nonexistent version really do count to zero, so criterion ② alone can't catch this). Before
/// the fix, the clear-`UPDATE` on `v1` committed while the set-`UPDATE` on the missing `v2`
/// matched zero rows, leaving the family with **zero** `serving` rows while still reporting
/// `Switched`. Must now report `SwitchOutcome::TargetVersionMissing` and leave `v1` untouched.
#[test]
fn switch_projection_version_reports_target_missing_and_writes_nothing_when_new_version_row_absent()
{
    run_db_fixture::<ServingFixture, _>(
        "switch_projection_version_reports_target_missing_and_writes_nothing_when_new_version_row_absent",
        |mut handle| {
            let f = family(&handle);
            seed_checkpoint(&mut handle.admin, &f, "v1", true, false);
            // Deliberately no "v2" row seeded — this is the whole point of the regression.

            let outcome = handle
                .rt
                .block_on(serving_repo::switch_projection_version(
                    &handle.maintenance,
                    &f,
                    "v2",
                    Some(("v2".to_string(), 10)),
                    Some(("v1".to_string(), 10)),
                    ContinuationVerdict::Pass,
                ))
                .expect("switch call must not error");
            assert_eq!(
                outcome,
                SwitchOutcome::TargetVersionMissing,
                "a missing target row must be its own distinguishable outcome, never silently \
                 reported as Switched"
            );

            let (v1_serving, _) = read_serving_flags(&mut handle.admin, &f, "v1");
            assert!(
                v1_serving,
                "the old serving row must survive untouched — the family must never end up \
                 with zero serving rows"
            );
        },
    );
}

/// §16.2/§16.3 version-consistency check: a `visible_serving` count tagged with a version that
/// does not match the family's actual current `serving` version must be rejected
/// (`VisibleVersionMismatch`), never silently trusted as if it were a real comparison.
#[test]
fn switch_projection_version_rejects_when_declared_serving_version_does_not_match_db() {
    run_db_fixture::<ServingFixture, _>(
        "switch_projection_version_rejects_when_declared_serving_version_does_not_match_db",
        |mut handle| {
            let f = family(&handle);
            seed_checkpoint(&mut handle.admin, &f, "v1", true, false);
            seed_checkpoint(&mut handle.admin, &f, "v2", false, true);

            let outcome = handle
                .rt
                .block_on(serving_repo::switch_projection_version(
                    &handle.maintenance,
                    &f,
                    "v2",
                    Some(("v2".to_string(), 10)),
                    // Declares "v0" — nothing in the DB is serving "v0"; the real serving
                    // version is "v1". Must be rejected, not silently trusted at face value.
                    Some(("v0".to_string(), 10)),
                    ContinuationVerdict::Pass,
                ))
                .expect("switch call must not error");
            assert!(
                matches!(outcome, SwitchOutcome::Rejected(_)),
                "a declared version that does not match the DB's actual serving version must \
                 reject the switch"
            );

            let (v1_serving, _) = read_serving_flags(&mut handle.admin, &f, "v1");
            assert!(
                v1_serving,
                "rejection must leave the old serving row untouched"
            );
        },
    );
}

/// The read route is bound to the authenticated tenant before its own transaction starts; a
/// caller cannot pass a family from another tenant even though the gateway pool is reusable.
#[test]
fn serving_version_rejects_family_outside_authenticated_tenant() {
    run_db_fixture::<ServingFixture, _>(
        "serving_version_rejects_family_outside_authenticated_tenant",
        |handle| {
            let foreign = StreamFamily::new(
                TenantId(Uuid::new_v4()),
                "workspace",
                Uuid::new_v4(),
                "code",
                "retrieval_card",
            );
            let result = handle.rt.block_on(serving_repo::serving_version(
                &handle.runtime,
                &handle.auth,
                &foreign,
            ));
            assert!(matches!(
                result,
                Err(serving_repo::ServingRepoError::CrossTenant)
            ));
        },
    );
}
