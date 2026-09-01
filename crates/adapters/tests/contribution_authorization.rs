//! Real PostgreSQL races for §12.1.1. Input mutation must serialize with finalization;
//! generic reasoning grants never authorize or revoke a self-principal contribution.

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use contribution_fixture::ContributionFixture;
use humaux_adapters::{
    contribution_entry_repo::{self, ContributionEntryRepo},
    postgres::PrivateWorkerDbPool,
};
use humaux_application::contribute::ContributionCandidatePort;
use humaux_domain::identity::{AuthorizationScope, PrincipalId};
use postgres::{Client, NoTls};
use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

fn race_input_change(change: &str) {
    let mut f = ContributionFixture::new();
    let candidate = f.prepare();
    let confirmation = f.confirm(candidate);
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL fixture");
    let mut writer = Client::connect(&dsn, NoTls).unwrap();
    let bad_evidence = if change == "backing" {
        let other_domain:Uuid=writer.query_one("INSERT INTO control.private_reasoning_domains(tenant_id,name) VALUES($1,'other-domain') RETURNING reasoning_domain_id",&[&f.auth.tenant_id().0]).unwrap().get(0);
        let evidence:Uuid=writer.query_one("INSERT INTO private.evidence_objects(tenant_id,evidence_kind,payload_sha256,data_class,origin_class,visibility_class,reasoning_domain_id) \
            VALUES($1,'EVENT',sha256(convert_to('{}','UTF8')),'INTERNAL','DirectUserInput','TENANT_SHARED',$2) RETURNING evidence_id",&[&f.auth.tenant_id().0,&other_domain]).unwrap().get(0);
        writer.execute("INSERT INTO private.events(event_id,event_kind,payload) VALUES($1,'USER_MESSAGE','{}')",&[&evidence]).unwrap();
        Some(evidence)
    } else {
        None
    };
    let mut pending_change = writer.transaction().unwrap();
    if let Some(evidence) = bad_evidence {
        pending_change.execute("INSERT INTO private.memory_evidence(memory_id,evidence_id,role) VALUES($1,$2,'SUPPORTING')",&[&f.memory,&evidence]).unwrap();
    } else {
        pending_change.execute("UPDATE control.reasoning_domain_grants SET revoked_at=clock_timestamp() WHERE reasoning_domain_id=$1",&[&f.domain]).unwrap();
    }
    let application_name = format!("contribution-race-{}", Uuid::new_v4().simple());
    let role_dsn = format!(
        "{dsn}{}options=-c%20role%3Drole_private_worker&application_name={application_name}",
        if dsn.contains('?') { '&' } else { '?' }
    );
    let (ready, started) = mpsc::channel();
    let worker = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let pool = PrivateWorkerDbPool::connect(&role_dsn).await.unwrap();
            ready.send(()).unwrap();
            ContributionEntryRepo::new(&pool)
                .finalize_confirmed(confirmation)
                .await
        })
    });
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut saw_wait = false;
    while Instant::now() < deadline {
        saw_wait=f.admin.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND wait_event='advisory')",&[&application_name]).unwrap().get(0);
        if saw_wait {
            break;
        }
        if worker.is_finished() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    // Always release the real lock before assertions, including mutation-red runs.
    pending_change.commit().unwrap();
    let result = worker.join().unwrap();
    assert!(
        saw_wait,
        "{change}: finalizer did not wait for the input-change lock"
    );
    if change == "backing" {
        assert!(
            result.is_err(),
            "{change}: stale input authorized a release"
        );
        assert_eq!(f.counts(), (1, 0, 0));
    } else {
        result.expect("generic grant revocation cannot revoke self-principal authority");
        assert_eq!(f.counts(), (1, 1, 1));
    }
}

#[test]
#[ignore = "requires isolated PostgreSQL and pinned real Gitleaks"]
fn concurrent_generic_grant_revocation_does_not_revoke_self_principal_finalize() {
    race_input_change("grant");
}

#[test]
#[ignore = "requires isolated PostgreSQL and pinned real Gitleaks"]
fn concurrent_backing_link_change_prevents_finalize() {
    race_input_change("backing");
}

#[test]
#[ignore = "requires isolated PostgreSQL and pinned real Gitleaks"]
fn direct_domain_owner_needs_no_headless_grant() {
    let mut f = ContributionFixture::new();
    f.admin
        .execute(
            "DELETE FROM control.reasoning_domain_grants WHERE reasoning_domain_id=$1",
            &[&f.domain],
        )
        .unwrap();
    let _ = f.finalize_release();
    assert_eq!(f.counts(), (1, 1, 1));
}

#[test]
#[ignore = "requires isolated PostgreSQL and pinned real Gitleaks"]
fn valid_generic_grant_never_authorizes_delegated_contribution_entrypoints() {
    let mut f = ContributionFixture::new();
    let candidate = f.prepare();
    let preview =
        f.rt.block_on(contribution_entry_repo::preview(
            &f.private, &f.auth, candidate,
        ))
        .expect("self-principal preview");
    let confirmation = f.confirm(candidate);
    let delegated_principal: Uuid = f
        .admin
        .query_one(
            "SELECT principal_id FROM control.reasoning_domain_grants WHERE reasoning_domain_id=$1",
            &[&f.domain],
        )
        .expect("valid generic grant")
        .get(0);
    let delegated = AuthorizationScope::new(
        f.auth.tenant_id(),
        PrincipalId(delegated_principal),
        f.auth.user_id(),
        f.auth.allowed_workspace_ids().clone(),
    );
    let mut delegated_request = f.request();
    delegated_request.authorization = delegated.clone();
    assert!(
        f.rt.block_on(ContributionEntryRepo::new(&f.private).load_preparation(&delegated_request))
            .is_err(),
        "delegated principal created a contribution preparation"
    );
    assert!(
        f.rt.block_on(contribution_entry_repo::preview(
            &f.private, &delegated, candidate
        ))
        .is_err(),
        "delegated principal previewed a candidate"
    );
    assert!(
        f.rt.block_on(contribution_entry_repo::record_confirmation(
            &f.gateway,
            &delegated,
            candidate,
            preview.payload_sha256,
            preview.policy_version,
            Duration::from_secs(300),
        ))
        .is_err(),
        "delegated principal confirmed a candidate"
    );
    let delegated_confirmation = humaux_application::contribute::ConfirmContribution {
        authorization: delegated,
        ..confirmation.clone()
    };
    assert!(
        f.rt.block_on(
            ContributionEntryRepo::new(&f.private)
                .finalize_confirmed(delegated_confirmation.clone())
        )
        .is_err(),
        "delegated principal finalized a candidate"
    );
    assert_eq!(f.counts(), (1, 0, 0));
    f.rt.block_on(ContributionEntryRepo::new(&f.private).finalize_confirmed(confirmation))
        .expect("self-principal finalize");
    assert!(
        f.rt.block_on(
            ContributionEntryRepo::new(&f.private).finalize_confirmed(delegated_confirmation)
        )
        .is_err(),
        "delegated principal replayed a finalized candidate"
    );
    assert_eq!(f.counts(), (1, 1, 1));
}
