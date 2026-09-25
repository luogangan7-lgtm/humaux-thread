//! §12.1.1 real PG/Gitleaks acceptance. The inference fixture is deliberately offline;
//! these tests do not claim a live model call. Run ignored tests with the isolated runner.

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use contribution_fixture::{ContributionFixture as Fixture, OfflineReasoner};
use humaux_adapters::{
    contribution_entry_repo::{self, ContributionEntryRepo},
    contribution_repo,
};
use humaux_application::{
    consolidate::ProviderTraceRef,
    contribute::{self, ContributionCandidatePort, StoredCandidate},
};
use humaux_domain::{
    error::ErrorCode,
    evidence::payload_sha256,
    identity::{AuthorizationScope, BoundedSet, PrincipalId},
    ids::UserId,
};
use postgres::{Client, NoTls};
use std::{sync::Mutex, time::Duration};
use uuid::Uuid;

static SERIAL: Mutex<()> = Mutex::new(());

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL and pinned real Gitleaks"]
#[allow(
    deprecated,
    reason = "test fixture intentionally exercises the legacy prepare path"
)]
fn manual_exact_confirmation_release_is_atomic_idempotent_and_not_reactivated() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut f = Fixture::new();
    let candidate = f.prepare();
    assert_eq!(f.counts(), (1, 0, 0));
    let confirmed = f.confirm(candidate);
    let repo = ContributionEntryRepo::new(&f.private);
    let first =
        f.rt.block_on(repo.finalize_confirmed(confirmed.clone()))
            .unwrap();
    assert_eq!(
        first,
        f.rt.block_on(repo.finalize_confirmed(confirmed.clone()))
            .unwrap()
    );
    assert_eq!(f.counts(), (1, 1, 1));
    let row=f.admin.query_one("SELECT r.disclosed_payload_sha256=c.disclosed_payload_sha256,r.scan_receipt=c.scan_receipt, \
      (SELECT count(*) FROM staging.contribution_release_sources s WHERE s.contribution_release_id=r.contribution_release_id), \
      ROW(r.rights_basis,r.source_license,r.publisher,r.contributor_attestation,r.redistribution_policy) \
        IS NOT DISTINCT FROM ROW(c.rights_basis,c.source_license,c.publisher,c.contributor_attestation,c.redistribution_policy), \
      r.privacy_scan_outcome='PASSED' AND r.secret_scan_outcome='PASSED' \
      FROM staging.contribution_releases r JOIN staging.contribution_candidates c USING(candidate_id) WHERE r.contribution_release_id=$1",&[&first.0]).unwrap();
    assert!(row.get::<_, bool>(0));
    assert!(row.get::<_, bool>(1));
    assert_eq!(row.get::<_, i64>(2), 1);
    assert!(
        row.get::<_, bool>(3),
        "all five rights fields match the sealed candidate"
    );
    assert!(
        row.get::<_, bool>(4),
        "both scan gates passed for the disclosed bytes"
    );
    assert!(
        f.rt.block_on(contribution_repo::revoke_release(
            &f.private,
            f.auth.tenant_id(),
            first.0
        ))
        .unwrap()
    );
    assert_eq!(
        first,
        f.rt.block_on(ContributionEntryRepo::new(&f.private).finalize_confirmed(confirmed))
            .unwrap()
    );
    assert_eq!(
        f.admin
            .query_one(
                "SELECT state FROM staging.contribution_releases WHERE contribution_release_id=$1",
                &[&first.0]
            )
            .unwrap()
            .get::<_, String>(0),
        "REVOKED"
    );
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL and pinned real Gitleaks"]
#[allow(
    deprecated,
    reason = "this failure fixture intentionally exercises legacy contribute::prepare"
)]
fn scan_failure_or_changed_payload_creates_no_candidate_or_release() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut f = Fixture::new();
    for payload in [
        b"a@example.test".to_vec(),
        b"xoxb-123456789012-123456789012-abcdefghijklmnopqrstuvwx\n".to_vec(),
    ] {
        assert!(
            f.rt.block_on(contribute::prepare(
                f.request(),
                &OfflineReasoner(payload),
                &f.scanner(),
                &ContributionEntryRepo::new(&f.private)
            ))
            .is_err()
        );
        assert_eq!(f.counts(), (0, 0, 0));
    }
    let repo = ContributionEntryRepo::new(&f.private);
    let saved = f.rt.block_on(repo.load_preparation(&f.request())).unwrap();
    let clean = b"Public generalized statement.";
    let scan = f.scanner().scan(clean).unwrap();
    let trace = ProviderTraceRef(saved.reasoning.binding_id.0.to_string());
    assert_eq!(
        f.rt.block_on(repo.store_prepared(StoredCandidate {
            preparation: &saved,
            disclosed_bytes: b"Different bytes.",
            payload_sha256: payload_sha256(b"Different bytes."),
            provider_trace: &trace,
            model_call_id: saved.reasoning.binding_id.0,
            scan_receipt: &scan
        })),
        Err(ErrorCode::Conflict)
    );
    assert_eq!(f.counts(), (0, 0, 0));
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL and pinned real Gitleaks"]
fn stale_inputs_fail_while_generic_grants_do_not_control_self_principal_finalize() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for change in [
        "source",
        "policy",
        "rights",
        "membership",
        "grant",
        "grant_expiry",
        "grant_purpose",
        "grant_workspace",
        "grant_revoked",
        "domain_status",
    ] {
        let mut f = Fixture::new();
        let id = f.prepare();
        let confirmation = f.confirm(id);
        match change {
            "source" => {
                f.admin.execute("UPDATE private.memory_records SET content='{\"changed\":true}' WHERE memory_id=$1",&[&f.memory]).unwrap();
            }
            "policy" => {
                f.admin.query_one("SELECT (control.append_contribution_policy_successor(tenant_id,policy_id,policy_version,false,'DISABLED',rights_basis,source_license,publisher,contributor_attestation,redistribution_policy)).policy_version FROM control.contribution_policies WHERE tenant_id=$1 AND effective_to IS NULL",&[&f.auth.tenant_id().0]).unwrap();
            }
            "rights" => {
                f.admin.query_one("SELECT (control.append_contribution_policy_successor(tenant_id,policy_id,policy_version,allow_public_contribution,contribution_mode,rights_basis,'changed after preview',publisher,contributor_attestation,redistribution_policy)).policy_version FROM control.contribution_policies WHERE tenant_id=$1 AND effective_to IS NULL",&[&f.auth.tenant_id().0]).unwrap();
            }
            "membership" => {
                f.admin
                    .execute(
                        "UPDATE control.memberships SET state='SUSPENDED' WHERE tenant_id=$1",
                        &[&f.auth.tenant_id().0],
                    )
                    .unwrap();
            }
            "grant" => {
                f.admin
                    .execute(
                        "DELETE FROM control.reasoning_domain_grants WHERE reasoning_domain_id=$1",
                        &[&f.domain],
                    )
                    .unwrap();
            }
            "grant_expiry" => {
                f.admin.execute("UPDATE control.reasoning_domain_grants SET expires_at=clock_timestamp()-interval '1 second' WHERE reasoning_domain_id=$1",&[&f.domain]).unwrap();
            }
            "grant_purpose" => {
                f.admin.execute("UPDATE control.reasoning_domain_grants SET purposes=ARRAY['RETRIEVAL_EMBEDDING'] WHERE reasoning_domain_id=$1",&[&f.domain]).unwrap();
            }
            "grant_revoked" => {
                f.admin.execute("UPDATE control.reasoning_domain_grants SET revoked_at=clock_timestamp() WHERE reasoning_domain_id=$1",&[&f.domain]).unwrap();
            }
            "grant_workspace" => {
                let ws:Uuid=f.admin.query_one("INSERT INTO control.workspaces(tenant_id,name) VALUES($1,'restricted') RETURNING workspace_id",&[&f.auth.tenant_id().0]).unwrap().get(0);
                f.admin.execute("UPDATE control.reasoning_domain_grants SET workspace_id=$2 WHERE reasoning_domain_id=$1",&[&f.domain,&ws]).unwrap();
            }
            "domain_status" => {
                f.admin.execute("UPDATE control.private_reasoning_domains SET status='SUSPENDED' WHERE reasoning_domain_id=$1",&[&f.domain]).unwrap();
            }
            _ => unreachable!(),
        }
        let result =
            f.rt.block_on(ContributionEntryRepo::new(&f.private).finalize_confirmed(confirmation));
        if change.starts_with("grant") {
            result.expect(
                "generic USER_REASONING grants do not authorize self-principal contribution",
            );
            assert_eq!(f.counts(), (1, 1, 1), "{change}");
        } else {
            assert!(result.is_err(), "{change}");
            assert_eq!(f.counts(), (1, 0, 0), "{change}");
        }
    }
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL and pinned real Gitleaks"]
fn exact_binding_candidate_does_not_reconsult_mutable_legacy_profile() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for change in ["profile_disabled", "domain_profile_removed"] {
        let mut f = Fixture::new();
        let id = f.prepare();
        let confirmation = f.confirm(id);
        match change {
            "profile_disabled" => {
                f.admin
                    .execute(
                        "UPDATE control.user_reasoning_profiles SET enabled=false WHERE tenant_id=$1",
                        &[&f.auth.tenant_id().0],
                    )
                    .unwrap();
            }
            "domain_profile_removed" => {
                f.admin
                    .execute(
                        "UPDATE control.private_reasoning_domains \
                         SET user_reasoning_profile_id=NULL WHERE reasoning_domain_id=$1",
                        &[&f.domain],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        f.rt.block_on(ContributionEntryRepo::new(&f.private).finalize_confirmed(confirmation))
            .expect("R3 exact call receipt, not the legacy profile pointer, is authoritative");
        assert_eq!(f.counts(), (1, 1, 1), "{change}");
    }
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL and pinned real Gitleaks"]
fn wrong_hash_other_user_and_source_append_fail_closed() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut f = Fixture::new();
    let id = f.prepare();
    let wrong = f.rt.block_on(contribution_entry_repo::record_confirmation(
        &f.gateway,
        &f.auth,
        id,
        payload_sha256(b"not the candidate"),
        1,
        Duration::from_secs(300),
    ));
    assert!(wrong.is_err());
    let other: Uuid = f
        .admin
        .query_one(
            "INSERT INTO control.users(state) VALUES('ACTIVE') RETURNING user_id",
            &[],
        )
        .unwrap()
        .get(0);
    f.admin.execute("INSERT INTO control.memberships(tenant_id,user_id,role,state) VALUES($1,$2,'member','ACTIVE')",&[&f.auth.tenant_id().0,&other]).unwrap();
    let other_auth = AuthorizationScope::new(
        f.auth.tenant_id(),
        PrincipalId(other),
        Some(UserId(other)),
        BoundedSet::new([]).unwrap(),
    );
    assert!(
        f.rt.block_on(contribution_entry_repo::preview(
            &f.private,
            &other_auth,
            id
        ))
        .is_err()
    );
    // A distinct, valid Evidence bypasses duplicate-key checks: the deferred manifest seal
    // itself must reject it before confirmation, and the insert guard after confirmation.
    let second:Uuid=f.admin.query_one("INSERT INTO private.evidence_objects(tenant_id,evidence_kind,payload_sha256,data_class,origin_class,visibility_class,reasoning_domain_id) \
        VALUES($1,'EVENT',sha256(convert_to('{}','UTF8')),'INTERNAL','DirectUserInput','TENANT_SHARED',$2) RETURNING evidence_id",&[&f.auth.tenant_id().0,&f.domain]).unwrap().get(0);
    f.admin.execute("INSERT INTO private.events(event_id,event_kind,payload) VALUES($1,'USER_MESSAGE','{}')",&[&second]).unwrap();
    for confirmed in [false, true] {
        if confirmed {
            let _ = f.confirm(id);
        }
        let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG");
        let mut private = Client::connect(&dsn, NoTls).expect("admin fixture client");
        private
            .batch_execute(&format!(
                "BEGIN; SET LOCAL ROLE role_private_worker; \
                 SELECT set_config('humaux.tenant_id','{}',true), \
                 set_config('humaux.user_id','{}',true);",
                f.auth.tenant_id().0,
                f.auth.user_id().expect("user scope").0
            ))
            .expect("tenant context");
        let err = match private.execute(
            "INSERT INTO staging.contribution_candidate_sources(tenant_id,candidate_id,evidence_id,source_hash,ordinal) \
             VALUES($1,$2,$3,sha256(convert_to('{}','UTF8')),100)",
            &[&f.auth.tenant_id().0, &id.0, &second],
        ) {
            Ok(_) => private.batch_execute("COMMIT").expect_err("manifest seal must reject"),
            Err(error) => {
                private.batch_execute("ROLLBACK").expect("rollback failed insert");
                error
            }
        };
        assert_eq!(
            err.as_db_error().map(|e| e.code().code()),
            Some("23514"),
            "confirmed={confirmed}: {err}"
        );
    }
    assert_eq!(f.counts(), (1, 0, 0));
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL and pinned real Gitleaks"]
fn outbox_failure_rolls_back_finalized_release_and_source_rows() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut f = Fixture::new();
    let id = f.prepare();
    let confirmation = f.confirm(id);
    // Fault is scoped to this disposable tenant; no other agent/test's event can be intercepted.
    let trigger_name = format!("p9_fail_{}", Uuid::new_v4().simple());
    f.admin.batch_execute(&format!("CREATE FUNCTION ops.{trigger_name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
      IF NEW.tenant_id='{}'::uuid AND NEW.event_type='PUBLIC_RELEASE' THEN RAISE EXCEPTION 'isolated outbox fault'; END IF; RETURN NEW; END $$; \
      CREATE TRIGGER {trigger_name} BEFORE INSERT ON ops.outbox FOR EACH ROW EXECUTE FUNCTION ops.{trigger_name}();",f.auth.tenant_id().0)).unwrap();
    let result =
        f.rt.block_on(ContributionEntryRepo::new(&f.private).finalize_confirmed(confirmation));
    f.admin
        .batch_execute(&format!(
            "DROP TRIGGER {trigger_name} ON ops.outbox; DROP FUNCTION ops.{trigger_name}();"
        ))
        .unwrap();
    assert!(result.is_err());
    assert_eq!(f.counts(), (1, 0, 0));
    assert_eq!(
        f.admin
            .query_one(
                "SELECT count(*) FROM staging.contribution_release_sources WHERE tenant_id=$1",
                &[&f.auth.tenant_id().0]
            )
            .unwrap()
            .get::<_, i64>(0),
        0
    );
}
