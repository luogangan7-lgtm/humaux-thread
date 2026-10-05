//! `adapters::tests::contribution_execution_ingress_0131` — Focused PostgreSQL 18 gate for the real Phase 9
//!   production-core start command.
//! Depends-on: crates=[humaux-adapters, humaux-application, humaux-testkit, postgres, tokio, uuid]; services=[PostgreSQL(any) r=[ops.contribution_execution_job_links, ops.jobs, private.contribution_execution_sources, private.contribution_executions] w=[control.contribution_policies]];
//!   env=[HUMAUX_TEST_PG_DSN]; modules=[adapters::contribution_entry_repo, adapters::contribution_execution_ingress, adapters::contribution_execution_repo, adapters::contribution_reasoner, adapters::tests::support::contribution_fixture, application::consolidate, application::contribute, application::contribution_execution, humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [enters through ContributionExecutionIngress::start_manual only (one direct enqueue seeds a legacy-v1
//!   root); no provider runs; without a DB it SKIPs unless HUMAUX_REQUIRE_DB, then panics]
//! Spec: none
//!
//! The test enters through `ContributionExecutionIngress::start_manual`; direct repository
//! enqueue is used once only to seed a legacy-v1 compatibility root. No provider or Phase 10
//! component participates.

#[allow(deprecated)] // Shared fixture retains one explicit legacy compatibility helper.
#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use contribution_fixture::ContributionFixture;
use humaux_adapters::{
    contribution_entry_repo::ContributionEntryRepo,
    contribution_execution_ingress::{
        ContributionExecutionIngress, ContributionExecutionIngressError,
    },
    contribution_execution_repo::{ContributionExecutionRepo, EnqueuedContributionExecution},
    contribution_reasoner::{assessment_prompt_contract, coverage_prompt_contract},
};
use humaux_application::{
    consolidate::ContentSha256,
    contribute::{ContributionPreparationInput, prepare_assessed_input},
    contribution_execution::{ContributionExecutionEnqueueInput, ContributionPromptContract},
};
use postgres::Client;
use uuid::Uuid;

/// Env probe only; the skip goes through `humaux_testkit::skip_or_fail`, which turns a
/// missing DSN into a failure under `HUMAUX_REQUIRE_DB=1` (§79.2, ADR-0051 D-K).
fn require_db() -> bool {
    std::env::var("HUMAUX_TEST_PG_DSN").is_ok()
}

fn assert_same_ids(expected: EnqueuedContributionExecution, actual: EnqueuedContributionExecution) {
    assert_eq!(actual.execution_id, expected.execution_id);
    assert_eq!(actual.job_id, expected.job_id);
    assert_eq!(actual.logical_call_ids, expected.logical_call_ids);
    assert_eq!(actual.candidate_id, expected.candidate_id);
}

fn counts(admin: &mut Client, tenant_id: Uuid, key: &str) -> (i64, i64, i64, i64) {
    let row = admin
        .query_one(
            "SELECT \
             (SELECT count(*) FROM private.contribution_executions \
              WHERE tenant_id=$1 AND enqueue_idempotency_key=$2), \
             (SELECT count(*) FROM private.contribution_execution_sources source \
              JOIN private.contribution_executions root \
                ON (root.tenant_id,root.execution_id)=(source.tenant_id,source.execution_id) \
              WHERE root.tenant_id=$1 AND root.enqueue_idempotency_key=$2), \
             (SELECT count(*) FROM ops.jobs WHERE idempotency_key=$2), \
             (SELECT count(*) FROM ops.contribution_execution_job_links link \
              JOIN private.contribution_executions root \
                ON (root.tenant_id,root.execution_id)=(link.tenant_id,link.execution_id) \
              WHERE root.tenant_id=$1 AND root.enqueue_idempotency_key=$2)",
            &[&tenant_id, &key],
        )
        .expect("canonical ingress counts");
    (row.get(0), row.get(1), row.get(2), row.get(3))
}

/// Admin-only fault injection that bypasses the production append-only lifecycle. Production
/// rejects same-version semantic updates; replica mode is used only to prove the 0131 fingerprint
/// still fails closed if storage is corrupted without changing the referenced policy key.
fn force_same_version_policy_update(admin: &mut Client, tenant_id: Uuid, statement: &str) {
    let mut txn = admin.transaction().expect("policy fault transaction");
    // replica-mode: fault setup, fixture purged at the end (the test ends with each fixture's `purge()`)
    txn.batch_execute("SET LOCAL session_replication_role='replica'")
        .expect("suppress policy version trigger for bounded fault injection");
    txn.execute(statement, &[&tenant_id])
        .expect("mutate one same-version rights axis");
    txn.commit().expect("commit bounded policy fault");
}

fn assert_conflict(
    result: Result<EnqueuedContributionExecution, ContributionExecutionIngressError>,
) {
    let error = result.expect_err("same-key frozen-axis drift must conflict");
    assert!(
        error.is_idempotency_conflict(),
        "0131 must remain the canonical conflict arbiter: {error}"
    );
}

#[test]
#[ignore = "lane(a:shared_db) requires disposable PostgreSQL 18 migrated through 0131"]
#[allow(clippy::too_many_lines)]
fn production_core_is_atomic_v2_complete_and_v1_compatible() {
    if !require_db() {
        humaux_testkit::skip_or_fail(
            "production_core_is_atomic_v2_complete_and_v1_compatible",
            "HUMAUX_TEST_PG_DSN",
            humaux_testkit::ExternalDep::Postgres,
        );
        return;
    }

    // Start with no root. The same-key lock must serialize all four first creators before each
    // READ COMMITTED preparation snapshot: exactly one creates, and every waiter sees its IDs.
    let mut race = ContributionFixture::new();
    let race_ingress = ContributionExecutionIngress::new(&race.private);
    let race_request: ContributionPreparationInput = (&race.request()).into();
    let race_tenant_id = race.auth.tenant_id().0;
    let race_key = format!("production-ingress-race-{}", Uuid::new_v4());
    let race_coverage = coverage_prompt_contract();
    let race_assessment = assessment_prompt_contract();
    let race_outcomes = race.rt.block_on(async {
        tokio::join!(
            race_ingress.start_manual(
                race_request.clone(),
                race_key.clone(),
                race_coverage,
                race_assessment,
            ),
            race_ingress.start_manual(
                race_request.clone(),
                race_key.clone(),
                race_coverage,
                race_assessment,
            ),
            race_ingress.start_manual(
                race_request.clone(),
                race_key.clone(),
                race_coverage,
                race_assessment,
            ),
            race_ingress.start_manual(
                race_request,
                race_key.clone(),
                race_coverage,
                race_assessment,
            ),
        )
    });
    let race_outcomes = [
        race_outcomes.0.expect("concurrent first creator 1"),
        race_outcomes.1.expect("concurrent first creator 2"),
        race_outcomes.2.expect("concurrent first creator 3"),
        race_outcomes.3.expect("concurrent first creator 4"),
    ];
    assert_eq!(
        race_outcomes
            .iter()
            .filter(|outcome| outcome.created)
            .count(),
        1,
        "exactly one concurrent first creator must mint the root"
    );
    let canonical_race_outcome = race_outcomes[0];
    for outcome in race_outcomes {
        assert_same_ids(canonical_race_outcome, outcome);
    }
    assert_eq!(
        counts(&mut race.admin, race_tenant_id, &race_key),
        (1, 1, 1, 1)
    );

    let mut fixture = ContributionFixture::new();
    let ingress = ContributionExecutionIngress::new(&fixture.private);
    let request: ContributionPreparationInput = (&fixture.request()).into();
    let tenant_id = fixture.auth.tenant_id().0;
    let key = format!("production-ingress-{}", Uuid::new_v4());
    let coverage = coverage_prompt_contract();
    let assessment = assessment_prompt_contract();

    let first = fixture
        .rt
        .block_on(ingress.start_manual(request.clone(), key.clone(), coverage, assessment))
        .expect("production-core start");
    assert!(first.created);
    assert_eq!(counts(&mut fixture.admin, tenant_id, &key), (1, 1, 1, 1));

    let retry = fixture
        .rt
        .block_on(ingress.start_manual(request.clone(), key.clone(), coverage, assessment))
        .expect("exact production retry");
    assert!(!retry.created);
    assert_same_ids(first, retry);

    // Four concurrent production-core retries serialize on the same pre-snapshot advisory key.
    let concurrent = fixture.rt.block_on(async {
        tokio::join!(
            ingress.start_manual(request.clone(), key.clone(), coverage, assessment),
            ingress.start_manual(request.clone(), key.clone(), coverage, assessment),
            ingress.start_manual(request.clone(), key.clone(), coverage, assessment),
            ingress.start_manual(request.clone(), key.clone(), coverage, assessment),
        )
    });
    for result in [concurrent.0, concurrent.1, concurrent.2, concurrent.3] {
        let replay = result.expect("concurrent exact retry");
        assert!(!replay.created);
        assert_same_ids(first, replay);
    }
    assert_eq!(counts(&mut fixture.admin, tenant_id, &key), (1, 1, 1, 1));

    let changed_coverage =
        ContributionPromptContract::try_new(coverage.version(), ContentSha256([0xc1; 32]))
            .expect("changed coverage SHA");
    assert_conflict(fixture.rt.block_on(ingress.start_manual(
        request.clone(),
        key.clone(),
        changed_coverage,
        assessment,
    )));
    let changed_assessment =
        ContributionPromptContract::try_new(assessment.version(), ContentSha256([0xa2; 32]))
            .expect("changed assessment SHA");
    assert_conflict(fixture.rt.block_on(ingress.start_manual(
        request.clone(),
        key.clone(),
        coverage,
        changed_assessment,
    )));
    assert_eq!(counts(&mut fixture.admin, tenant_id, &key), (1, 1, 1, 1));

    // Every persisted rights-provenance field is an independent v2 fingerprint axis.
    let rights_mutations = [
        (
            "UPDATE control.contribution_policies SET rights_basis='changed rights basis' WHERE tenant_id=$1",
            "UPDATE control.contribution_policies SET rights_basis='explicit fixture redistribution rights' WHERE tenant_id=$1",
        ),
        (
            "UPDATE control.contribution_policies SET source_license='changed-license' WHERE tenant_id=$1",
            "UPDATE control.contribution_policies SET source_license=NULL WHERE tenant_id=$1",
        ),
        (
            "UPDATE control.contribution_policies SET publisher='changed-publisher' WHERE tenant_id=$1",
            "UPDATE control.contribution_policies SET publisher=NULL WHERE tenant_id=$1",
        ),
        (
            "UPDATE control.contribution_policies SET contributor_attestation='changed-attestation' WHERE tenant_id=$1",
            "UPDATE control.contribution_policies SET contributor_attestation=NULL WHERE tenant_id=$1",
        ),
        (
            "UPDATE control.contribution_policies SET redistribution_policy='changed-redistribution' WHERE tenant_id=$1",
            "UPDATE control.contribution_policies SET redistribution_policy=NULL WHERE tenant_id=$1",
        ),
    ];
    for (mutate, restore) in rights_mutations {
        force_same_version_policy_update(&mut fixture.admin, tenant_id, mutate);
        assert_conflict(fixture.rt.block_on(ingress.start_manual(
            request.clone(),
            key.clone(),
            coverage,
            assessment,
        )));
        assert_eq!(counts(&mut fixture.admin, tenant_id, &key), (1, 1, 1, 1));
        force_same_version_policy_update(&mut fixture.admin, tenant_id, restore);
        let restored = fixture
            .rt
            .block_on(ingress.start_manual(request.clone(), key.clone(), coverage, assessment))
            .expect("restored exact retry");
        assert_same_ids(first, restored);
    }

    // Seed exactly one legacy v1 root through the accepted compatibility wrapper, then prove the
    // production command reuses it only while every formerly omitted axis still matches.
    let mut legacy = ContributionFixture::new();
    let legacy_request: ContributionPreparationInput = (&legacy.request()).into();
    let legacy_tenant_id = legacy.auth.tenant_id().0;
    let legacy_key = format!("legacy-v1-{}", Uuid::new_v4());
    let prepared = legacy
        .rt
        .block_on(prepare_assessed_input(
            legacy_request.clone(),
            &ContributionEntryRepo::new(&legacy.private),
        ))
        .expect("legacy trusted preparation");
    let legacy_input = ContributionExecutionEnqueueInput::try_new(
        legacy_key.clone(),
        coverage,
        assessment,
        prepared,
    )
    .expect("legacy enqueue input");
    let legacy_root = legacy
        .rt
        .block_on(ContributionExecutionRepo::new(&legacy.private).enqueue(&legacy_input))
        .expect("seed legacy v1 root");
    assert!(legacy_root.created);

    let legacy_ingress = ContributionExecutionIngress::new(&legacy.private);
    let legacy_retry = legacy
        .rt
        .block_on(legacy_ingress.start_manual(
            legacy_request.clone(),
            legacy_key.clone(),
            coverage,
            assessment,
        ))
        .expect("production command replays exact legacy root");
    assert!(!legacy_retry.created);
    assert_same_ids(legacy_root, legacy_retry);
    assert_eq!(
        counts(&mut legacy.admin, legacy_tenant_id, &legacy_key),
        (1, 1, 1, 1)
    );

    let changed_legacy_coverage =
        ContributionPromptContract::try_new(coverage.version(), ContentSha256([0x1e; 32]))
            .expect("changed legacy coverage SHA");
    assert_conflict(legacy.rt.block_on(legacy_ingress.start_manual(
        legacy_request.clone(),
        legacy_key.clone(),
        changed_legacy_coverage,
        assessment,
    )));
    assert_eq!(
        counts(&mut legacy.admin, legacy_tenant_id, &legacy_key),
        (1, 1, 1, 1)
    );

    force_same_version_policy_update(
        &mut legacy.admin,
        legacy_tenant_id,
        "UPDATE control.contribution_policies SET source_license='legacy nullable drift' WHERE tenant_id=$1",
    );
    assert_conflict(legacy.rt.block_on(legacy_ingress.start_manual(
        legacy_request.clone(),
        legacy_key.clone(),
        coverage,
        assessment,
    )));
    assert_eq!(
        counts(&mut legacy.admin, legacy_tenant_id, &legacy_key),
        (1, 1, 1, 1)
    );
    force_same_version_policy_update(
        &mut legacy.admin,
        legacy_tenant_id,
        "UPDATE control.contribution_policies SET source_license=NULL WHERE tenant_id=$1",
    );
    let restored_legacy = legacy
        .rt
        .block_on(legacy_ingress.start_manual(
            legacy_request.clone(),
            legacy_key.clone(),
            coverage,
            assessment,
        ))
        .expect("restored legacy omitted axes replay");
    assert_same_ids(legacy_root, restored_legacy);

    force_same_version_policy_update(
        &mut legacy.admin,
        legacy_tenant_id,
        "UPDATE control.contribution_policies SET rights_basis='legacy drift' WHERE tenant_id=$1",
    );
    assert_conflict(legacy.rt.block_on(legacy_ingress.start_manual(
        legacy_request,
        legacy_key.clone(),
        coverage,
        assessment,
    )));
    assert_eq!(
        counts(&mut legacy.admin, legacy_tenant_id, &legacy_key),
        (1, 1, 1, 1)
    );
    // The fault rows above are fixture rows of these three tenants: nothing this test planted survives it.
    race.purge();
    fixture.purge();
    legacy.purge();
}
