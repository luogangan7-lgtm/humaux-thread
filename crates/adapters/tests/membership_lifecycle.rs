//! `adapters::tests::membership_lifecycle` — §6.3 membership lifecycle admin path (ADR-0033, card 12): live-DB
//!   acceptance for `adapters::membership_repo` under the real `role_maintenance` login.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-testkit, postgres, serde_json, uuid];
//!   services=[PostgreSQL(any) r=[control.audit_events, public.humaux_test_membership_fault_] w=[control.memberships,
//!   control.users]]; env=[HUMAUX_TEST_PG_DSN]; modules=[adapters::membership_repo,
//!   adapters::tests::support::operation_receipt_fixture, domain::identity, domain::ids, humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [removing the last ACTIVE OWNER is Conflict(LastOwner) with only a DENIED row; the state change and
//!   security-epoch bump commit together (injected trigger faults roll both back); every request writes its §77 audit
//!   row]
//! Spec: Baseline §77
//!
//! - last-OWNER rule: a REMOVE / SUSPEND / demotion that would leave the tenant with zero
//!   ACTIVE OWNER is refused (`Conflict(LastOwner)`) and writes nothing but its DENIED row;
//! - atomicity: the state change and the user security-epoch bump commit together or not at
//!   all — a failing trigger injected on the epoch write (and, separately, on the audit
//!   insert) rolls the state change back too; moving the bump into a second transaction
//!   makes this red (card 12 fault-injection acceptance);
//! - illegal edges are refused by the domain type before any SQL (`REMOVED → ACTIVE` leaves
//!   `updated_at` untouched);
//! - the DB CHECK constraints and the Rust closed sets agree (both deparse forms accepted);
//! - §77: every applied request writes a `SUCCESS` audit row and every refused one
//!   (CONFLICT / NOT_FOUND) writes a `DENIED` row naming the refusal, both carrying the
//!   Sensitive-Admin-Action fields (actor / reason / ticket→request_id / trace_id /
//!   step_up_auth_context); an empty field is `InvalidInput` and writes nothing.

use humaux_adapters::membership_repo::{self, AdminAction, MembershipRepoError, MembershipRequest};
use humaux_domain::identity::{
    MembershipConflict, MembershipMutation, MembershipRole, MembershipState,
};
use humaux_domain::ids::{TenantId, UserId};
use humaux_testkit::run_db_fixture;
use uuid::Uuid;

#[allow(dead_code)]
#[path = "support/operation_receipt_fixture.rs"]
mod operation_receipt_fixture;
use operation_receipt_fixture::{Fixture, Handle};

fn epoch(handle: &mut Handle, user_id: Uuid) -> i64 {
    handle
        .admin
        .query_one(
            "SELECT security_epoch FROM control.users WHERE user_id=$1",
            &[&user_id],
        )
        .expect("owner reads user epoch")
        .get(0)
}

fn row(handle: &mut Handle, user_id: Uuid) -> Option<(String, String, String)> {
    handle
        .admin
        .query_opt(
            "SELECT state, role, updated_at::text FROM control.memberships WHERE tenant_id=$1 AND user_id=$2",
            &[&handle.tenant_id, &user_id],
        )
        .expect("owner reads membership")
        .map(|r| (r.get(0), r.get(1), r.get(2)))
}

const ADMIN: AdminAction<'static> = AdminAction {
    actor: "card12-test",
    reason: "membership_lifecycle acceptance",
    ticket: "OPS-12",
    trace_id: "trace-card12-test",
    step_up_auth_context: "test-fixture:maintenance-dsn",
};

/// `(SUCCESS rows, DENIED rows)` for this tenant's membership audit trail.
fn audit_counts(handle: &mut Handle) -> (i64, i64) {
    let row = handle
        .admin
        .query_one(
            "SELECT count(*) FILTER (WHERE result='SUCCESS'), \
                    count(*) FILTER (WHERE result='DENIED'), \
                    count(*) FROM control.audit_events \
             WHERE tenant_id=$1 AND action LIKE 'MEMBERSHIP_%' AND actor_type='ADMIN'",
            &[&handle.tenant_id],
        )
        .expect("owner counts audit rows");
    let (success, denied, total): (i64, i64, i64) = (row.get(0), row.get(1), row.get(2));
    assert_eq!(
        success + denied,
        total,
        "every membership audit row is SUCCESS or DENIED"
    );
    (success, denied)
}

/// The newest membership audit row: `(action, result, resource_id, request_id, trace_id,
/// before_fingerprint, after_fingerprint, metadata)`.
#[allow(clippy::type_complexity)]
fn latest_audit(
    handle: &mut Handle,
) -> (
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    serde_json::Value,
) {
    let row = handle
        .admin
        .query_one(
            "SELECT action, result, resource_id, request_id, trace_id, before_fingerprint, \
                    after_fingerprint, metadata FROM control.audit_events \
             WHERE tenant_id=$1 AND action LIKE 'MEMBERSHIP_%' \
             ORDER BY audit_seq DESC LIMIT 1",
            &[&handle.tenant_id],
        )
        .expect("owner reads newest audit row");
    (
        row.get(0),
        row.get(1),
        row.get(2),
        row.get(3),
        row.get(4),
        row.get(5),
        row.get(6),
        row.get(7),
    )
}

/// §77 Sensitive-Admin-Action fields every row of this path must carry.
fn assert_admin_fields(request_id: &str, trace_id: &str, metadata: &serde_json::Value) {
    assert_eq!(request_id, ADMIN.ticket, "request_id is the ticket");
    assert_eq!(trace_id, ADMIN.trace_id);
    assert_eq!(metadata["reason"], ADMIN.reason);
    assert_eq!(metadata["ticket"], ADMIN.ticket);
    assert_eq!(metadata["step_up_auth_context"], ADMIN.step_up_auth_context);
}

fn apply(
    handle: &mut Handle,
    user_id: Uuid,
    request: MembershipRequest,
) -> Result<membership_repo::MembershipOutcome, MembershipRepoError> {
    let tenant = TenantId(handle.tenant_id);
    handle.rt.block_on(membership_repo::apply(
        &handle.maintenance,
        tenant,
        UserId(user_id),
        request,
        ADMIN,
    ))
}

fn mutate(
    handle: &mut Handle,
    user_id: Uuid,
    mutation: MembershipMutation,
) -> Result<membership_repo::MembershipOutcome, MembershipRepoError> {
    apply(handle, user_id, MembershipRequest::Mutate(mutation))
}

fn conflict(
    result: Result<membership_repo::MembershipOutcome, MembershipRepoError>,
) -> MembershipConflict {
    match result {
        Err(MembershipRepoError::Conflict(c)) => c,
        other => panic!("expected CONFLICT, got {other:?}"),
    }
}

/// Owner-installed failing trigger; dropped on `Drop` so a failing assertion never leaves it.
struct FaultTrigger {
    admin_dsn: String,
    drop_sql: String,
}

impl FaultTrigger {
    fn install(handle: &mut Handle, table: &str, event: &str) -> Self {
        let tag = Uuid::now_v7().simple().to_string();
        let function = format!("public.humaux_test_membership_fault_{tag}");
        let trigger = format!("humaux_test_membership_fault_{tag}");
        handle
            .admin
            .batch_execute(&format!(
                "CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ \
                 BEGIN RAISE EXCEPTION 'injected membership fault' USING ERRCODE='23514'; END $$; \
                 CREATE TRIGGER {trigger} BEFORE {event} ON {table} \
                 FOR EACH ROW EXECUTE FUNCTION {function}()"
            ))
            .expect("owner installs fault trigger");
        Self {
            // The fixture's own owner DSN (it keeps the field private); read the same env.
            admin_dsn: std::env::var("HUMAUX_TEST_PG_DSN").expect("fixture ran, DSN is set"),
            drop_sql: format!(
                "DROP TRIGGER IF EXISTS {trigger} ON {table}; DROP FUNCTION IF EXISTS {function}()"
            ),
        }
    }
}

impl Drop for FaultTrigger {
    fn drop(&mut self) {
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
        if let Ok(mut client) = postgres::Client::connect(&self.admin_dsn, postgres::NoTls) {
            let _ = client.batch_execute(&self.drop_sql);
        }
    }
}

fn check_def(handle: &mut Handle, conname: &str) -> String {
    handle
        .admin
        .query_one(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
             WHERE conrelid='control.memberships'::regclass AND conname=$1",
            &[&conname],
        )
        .expect("constraint exists")
        .get(0)
}

#[test]
#[allow(clippy::too_many_lines)] // one fixture, one serialized story
fn membership_lifecycle_last_owner_atomic_epoch_bump_and_typed_edges() {
    run_db_fixture::<Fixture, _>(
        "membership_lifecycle_last_owner_atomic_epoch_bump_and_typed_edges",
        |mut handle| {
            // ---- closed sets: DB CHECK ⇔ Rust (both `IN (` and `= ANY (ARRAY[` deparse forms).
            let state_check = check_def(&mut handle, "memberships_state_check");
            let role_check = check_def(&mut handle, "memberships_role_known");
            for s in MembershipState::ALL {
                assert!(
                    state_check.contains(&format!("'{}'", s.as_db_str())),
                    "{state_check}"
                );
            }
            for r in MembershipRole::ALL {
                assert!(
                    role_check.contains(&format!("'{}'", r.as_db_str())),
                    "{role_check}"
                );
            }
            assert!(state_check.contains("IN (") || state_check.contains("= ANY (ARRAY["));

            let owner = handle.user_id; // seeded as ACTIVE 'member' (0160 folds it to MEMBER)
            let peer = handle.seed_peer_user();
            let tenant = handle.tenant_id;
            assert_eq!(audit_counts(&mut handle), (0, 0));

            // ---- §77: an empty Sensitive-Admin-Action field is refused before any SQL.
            for admin in [
                AdminAction {
                    actor: " ",
                    ..ADMIN
                },
                AdminAction {
                    reason: "",
                    ..ADMIN
                },
                AdminAction {
                    ticket: "",
                    ..ADMIN
                },
                AdminAction {
                    trace_id: "",
                    ..ADMIN
                },
                AdminAction {
                    step_up_auth_context: "",
                    ..ADMIN
                },
            ] {
                let refused = handle.rt.block_on(membership_repo::apply(
                    &handle.maintenance,
                    TenantId(tenant),
                    UserId(owner),
                    MembershipRequest::Mutate(MembershipMutation::ChangeRole(
                        MembershipRole::Owner,
                    )),
                    admin,
                ));
                assert!(
                    matches!(refused, Err(MembershipRepoError::InvalidInput)),
                    "{admin:?}: {refused:?}"
                );
            }
            assert_eq!(audit_counts(&mut handle), (0, 0));
            assert_eq!(row(&mut handle, owner).map(|r| r.1), Some("MEMBER".into()));

            // ---- set-role MEMBER → OWNER bumps the epoch and audits.
            let e0 = epoch(&mut handle, owner);
            let promoted = mutate(
                &mut handle,
                owner,
                MembershipMutation::ChangeRole(MembershipRole::Owner),
            )
            .expect("promote to OWNER");
            assert_eq!(
                (promoted.state, promoted.role),
                (MembershipState::Active, MembershipRole::Owner)
            );
            assert_eq!(promoted.user_security_epoch, Some(e0 + 1));
            assert_eq!(epoch(&mut handle, owner), e0 + 1);
            assert_eq!(audit_counts(&mut handle), (1, 0));
            {
                let (action, result, resource_id, request_id, trace_id, before, after, meta) =
                    latest_audit(&mut handle);
                assert_eq!(
                    (action.as_str(), result.as_str(), resource_id),
                    (
                        "MEMBERSHIP_CHANGE_ROLE",
                        "SUCCESS",
                        promoted.membership_id.to_string()
                    )
                );
                assert_eq!(
                    (before.as_deref(), after.as_deref()),
                    (Some("ACTIVE/MEMBER"), Some("ACTIVE/OWNER"))
                );
                assert_eq!(meta["user_id"], owner.to_string());
                assert_eq!(meta["user_security_epoch"], e0 + 1);
                assert_eq!(meta["refusal"], serde_json::Value::Null);
                assert_admin_fields(&request_id, &trace_id, &meta);
            }
            assert_eq!(
                conflict(mutate(
                    &mut handle,
                    owner,
                    MembershipMutation::ChangeRole(MembershipRole::Owner)
                )),
                MembershipConflict::AlreadyInState
            );
            assert_eq!(audit_counts(&mut handle), (1, 1));
            {
                let (action, result, resource_id, request_id, trace_id, before, after, meta) =
                    latest_audit(&mut handle);
                assert_eq!(
                    (action.as_str(), result.as_str(), resource_id),
                    (
                        "MEMBERSHIP_CHANGE_ROLE",
                        "DENIED",
                        promoted.membership_id.to_string()
                    )
                );
                assert_eq!((before.as_deref(), after), (Some("ACTIVE/OWNER"), None));
                assert_eq!(meta["refusal"], "ALREADY_IN_STATE");
                assert_eq!(meta["requested_role"], "OWNER");
                assert_eq!(meta["to_state"], serde_json::Value::Null);
                assert_admin_fields(&request_id, &trace_id, &meta);
            }

            // ---- last OWNER: remove / suspend / demote all refused, nothing written.
            let before = row(&mut handle, owner).expect("owner row");
            for m in [
                MembershipMutation::Remove,
                MembershipMutation::Suspend,
                MembershipMutation::ChangeRole(MembershipRole::Member),
            ] {
                assert_eq!(
                    conflict(mutate(&mut handle, owner, m)),
                    MembershipConflict::LastOwner,
                    "{m:?}"
                );
            }
            assert_eq!(row(&mut handle, owner), Some(before.clone()));
            assert_eq!(epoch(&mut handle, owner), e0 + 1);
            // ...but each refusal is itself audited (§77 "全部审计"): DENIED, naming LAST_OWNER.
            assert_eq!(audit_counts(&mut handle), (1, 4));
            {
                let (action, result, resource_id, request_id, trace_id, before, after, meta) =
                    latest_audit(&mut handle);
                assert_eq!(
                    (action.as_str(), result.as_str(), resource_id),
                    (
                        "MEMBERSHIP_CHANGE_ROLE",
                        "DENIED",
                        promoted.membership_id.to_string()
                    )
                );
                assert_eq!((before.as_deref(), after), (Some("ACTIVE/OWNER"), None));
                assert_eq!(meta["refusal"], "LAST_OWNER");
                assert_eq!(meta["requested_role"], "MEMBER");
                assert_admin_fields(&request_id, &trace_id, &meta);
            }

            // ---- invite → activate (no bump) → activate again (ALREADY_IN_STATE).
            let invitee = Uuid::new_v4();
            handle
                .admin
                .execute(
                    "INSERT INTO control.users(user_id,state) VALUES($1,'ACTIVE')",
                    &[&invitee],
                )
                .expect("owner mints invitee user");
            let ghost = Uuid::new_v4();
            assert!(matches!(
                apply(
                    &mut handle,
                    ghost,
                    MembershipRequest::Invite(MembershipRole::Member)
                ),
                Err(MembershipRepoError::NotFound)
            ));
            // NOT_FOUND is audited too: no resource row, subject named in metadata.
            assert_eq!(audit_counts(&mut handle), (1, 5));
            {
                let (action, result, resource_id, request_id, trace_id, before, after, meta) =
                    latest_audit(&mut handle);
                assert_eq!(
                    (action.as_str(), result.as_str(), resource_id.as_str()),
                    ("MEMBERSHIP_INVITE", "DENIED", "")
                );
                assert_eq!((before, after), (None, None));
                assert_eq!(meta["refusal"], "NOT_FOUND");
                assert_eq!(meta["user_id"], ghost.to_string());
                assert_admin_fields(&request_id, &trace_id, &meta);
            }
            let invited = apply(
                &mut handle,
                invitee,
                MembershipRequest::Invite(MembershipRole::Member),
            )
            .expect("invite");
            assert_eq!(
                (invited.state, invited.user_security_epoch),
                (MembershipState::Invited, None)
            );
            assert_eq!(
                conflict(apply(
                    &mut handle,
                    invitee,
                    MembershipRequest::Invite(MembershipRole::Admin)
                )),
                MembershipConflict::AlreadyInState
            );
            // A refused re-invite names the existing row.
            assert_eq!(audit_counts(&mut handle), (2, 6));
            {
                let (action, result, resource_id, _, _, _, _, meta) = latest_audit(&mut handle);
                assert_eq!(
                    (action.as_str(), result.as_str(), resource_id),
                    (
                        "MEMBERSHIP_INVITE",
                        "DENIED",
                        invited.membership_id.to_string()
                    )
                );
                assert_eq!(meta["refusal"], "ALREADY_IN_STATE");
                assert_eq!(meta["requested_role"], "ADMIN");
            }
            assert_eq!(
                conflict(mutate(&mut handle, invitee, MembershipMutation::Suspend)),
                MembershipConflict::TransitionNotAllowed
            );
            assert_eq!(audit_counts(&mut handle), (2, 7));
            assert_eq!(
                latest_audit(&mut handle).7["refusal"],
                "TRANSITION_NOT_ALLOWED"
            );
            let activated =
                mutate(&mut handle, invitee, MembershipMutation::Activate).expect("activate");
            assert_eq!(
                (activated.state, activated.user_security_epoch),
                (MembershipState::Active, None)
            );
            assert_eq!(epoch(&mut handle, invitee), 0, "activation never bumps");
            assert_eq!(
                conflict(mutate(&mut handle, invitee, MembershipMutation::Activate)),
                MembershipConflict::AlreadyInState
            );
            assert_eq!(audit_counts(&mut handle), (3, 8));

            // ---- suspend bumps.
            let suspended =
                mutate(&mut handle, peer, MembershipMutation::Suspend).expect("suspend peer");
            assert_eq!(
                (suspended.state, suspended.user_security_epoch),
                (MembershipState::Suspended, Some(1))
            );
            assert_eq!(epoch(&mut handle, peer), 1);
            assert_eq!(audit_counts(&mut handle), (4, 8));

            // ---- atomicity, both directions (card 12 fault-injection acceptance).
            // (a) epoch write fails ⇒ the state change rolls back with it.
            {
                let _fault =
                    FaultTrigger::install(&mut handle, "control.users", "UPDATE OF security_epoch");
                let removed = mutate(&mut handle, peer, MembershipMutation::Remove);
                assert!(
                    matches!(removed, Err(MembershipRepoError::Db(_))),
                    "{removed:?}"
                );
                assert_eq!(
                    row(&mut handle, peer).map(|r| r.0),
                    Some("SUSPENDED".into())
                );
                assert_eq!(epoch(&mut handle, peer), 1);
                assert_eq!(audit_counts(&mut handle), (4, 8));
            }
            // (b) audit insert fails ⇒ state change AND epoch bump roll back (neither).
            {
                let _fault = FaultTrigger::install(&mut handle, "control.audit_events", "INSERT");
                let removed = mutate(&mut handle, peer, MembershipMutation::Remove);
                assert!(
                    matches!(removed, Err(MembershipRepoError::Db(_))),
                    "{removed:?}"
                );
                assert_eq!(
                    row(&mut handle, peer).map(|r| r.0),
                    Some("SUSPENDED".into())
                );
                assert_eq!(
                    epoch(&mut handle, peer),
                    1,
                    "epoch bump must not outlive the rolled-back state change"
                );
                assert_eq!(audit_counts(&mut handle), (4, 8));
            }
            // (c) faults gone ⇒ remove commits both.
            let removed =
                mutate(&mut handle, peer, MembershipMutation::Remove).expect("remove peer");
            assert_eq!(
                (removed.state, removed.user_security_epoch),
                (MembershipState::Removed, Some(2))
            );
            assert_eq!(epoch(&mut handle, peer), 2);
            assert_eq!(audit_counts(&mut handle), (5, 8));

            // ---- REMOVED is terminal: refused by the type, DB untouched.
            let terminal = row(&mut handle, peer).expect("removed row");
            for m in [
                MembershipMutation::Activate,
                MembershipMutation::Suspend,
                MembershipMutation::Remove,
                MembershipMutation::ChangeRole(MembershipRole::Owner),
            ] {
                assert_eq!(
                    conflict(mutate(&mut handle, peer, m)),
                    MembershipConflict::TransitionNotAllowed,
                    "{m:?}"
                );
            }
            assert_eq!(row(&mut handle, peer), Some(terminal));
            assert_eq!(epoch(&mut handle, peer), 2);
            assert_eq!(audit_counts(&mut handle), (5, 12));
            {
                let (_, result, _, _, _, before, after, meta) = latest_audit(&mut handle);
                assert_eq!(result, "DENIED");
                assert_eq!((before.as_deref(), after), (Some("REMOVED/MEMBER"), None));
                assert_eq!(meta["refusal"], "TRANSITION_NOT_ALLOWED");
            }
            assert_eq!(
                conflict(apply(
                    &mut handle,
                    peer,
                    MembershipRequest::Invite(MembershipRole::Member)
                )),
                MembershipConflict::AlreadyInState,
                "UNIQUE (tenant_id, user_id): a removed membership is not re-invited as a second row"
            );

            assert_eq!(audit_counts(&mut handle), (5, 13));

            // ---- unknown membership ⇒ NOT_FOUND before any write, still audited.
            assert!(matches!(
                mutate(&mut handle, Uuid::new_v4(), MembershipMutation::Suspend),
                Err(MembershipRepoError::NotFound)
            ));
            assert_eq!(audit_counts(&mut handle), (5, 14));
            assert_eq!(latest_audit(&mut handle).7["refusal"], "NOT_FOUND");

            // ---- transfer ownership: promote the invitee, then the old owner may leave.
            mutate(
                &mut handle,
                invitee,
                MembershipMutation::ChangeRole(MembershipRole::Owner),
            )
            .expect("second owner");
            let left =
                mutate(&mut handle, owner, MembershipMutation::Remove).expect("old owner removed");
            assert_eq!(
                (left.state, left.user_security_epoch),
                (MembershipState::Removed, Some(e0 + 2))
            );
            // ...and the new sole owner is now protected.
            assert_eq!(
                conflict(mutate(&mut handle, invitee, MembershipMutation::Suspend)),
                MembershipConflict::LastOwner
            );
            assert_eq!(audit_counts(&mut handle), (7, 15));

            // The invitee is not a fixture-tracked user: remove its rows before teardown.
            handle
                .admin
                .execute(
                    "DELETE FROM control.memberships WHERE tenant_id=$1 AND user_id=$2",
                    &[&tenant, &invitee],
                )
                .expect("cleanup invitee membership");
            handle
                .admin
                .execute("DELETE FROM control.users WHERE user_id=$1", &[&invitee])
                .expect("cleanup invitee user");
        },
    );
}
