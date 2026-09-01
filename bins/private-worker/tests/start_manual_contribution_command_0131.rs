//! PostgreSQL 18 gate for the private-worker-owned MANUAL contribution command.
//!
//! This test enters through `StartManualContributionCommand`, proving that the production-core
//! owner supplies the canonical prompt contracts and that concurrent first creators share the
//! same migration-0131 root. No provider, scanner, external transport, or Phase 10 component runs.

#![allow(deprecated)] // Shared fixture retains one explicit legacy compatibility helper.

#[path = "../../../crates/adapters/tests/support/contribution_fixture.rs"]
mod contribution_fixture;

use std::sync::Mutex;

use contribution_fixture::ContributionFixture;
use humaux_adapters::{
    contribution_execution_repo::EnqueuedContributionExecution,
    contribution_reasoner::{assessment_prompt_contract, coverage_prompt_contract},
};
use humaux_application::contribute::ContributionPreparationInput;
use humaux_private_worker::StartManualContributionCommand;
use postgres::Client;
use uuid::Uuid;

static SERIAL: Mutex<()> = Mutex::new(());

fn require_db() -> bool {
    match std::env::var("HUMAUX_TEST_PG_DSN") {
        Ok(_) => true,
        Err(_) if std::env::var("HUMAUX_REQUIRE_DB").as_deref() == Ok("1") => {
            panic!("HUMAUX_REQUIRE_DB=1 requires HUMAUX_TEST_PG_DSN")
        }
        Err(_) => false,
    }
}

fn assert_same_ids(expected: EnqueuedContributionExecution, actual: EnqueuedContributionExecution) {
    assert_eq!(actual.execution_id, expected.execution_id);
    assert_eq!(actual.job_id, expected.job_id);
    assert_eq!(actual.logical_call_ids, expected.logical_call_ids);
    assert_eq!(actual.candidate_id, expected.candidate_id);
}

fn counts(admin: &mut Client, tenant_id: Uuid, key: &str) -> (i64, i64, i64) {
    let row = admin
        .query_one(
            "SELECT \
             (SELECT count(*) FROM private.contribution_executions \
              WHERE tenant_id=$1 AND enqueue_idempotency_key=$2), \
             (SELECT count(*) FROM ops.jobs WHERE idempotency_key=$2), \
             (SELECT count(*) FROM ops.contribution_execution_job_links link \
              JOIN private.contribution_executions root \
                ON (root.tenant_id,root.execution_id)=(link.tenant_id,link.execution_id) \
              WHERE root.tenant_id=$1 AND root.enqueue_idempotency_key=$2)",
            &[&tenant_id, &key],
        )
        .expect("command durable counts");
    (row.get(0), row.get(1), row.get(2))
}

#[test]
#[ignore = "requires disposable PostgreSQL 18 migrated through 0131"]
fn command_owns_contracts_and_serializes_concurrent_first_creators() {
    if !require_db() {
        eprintln!("SKIP: HUMAUX_TEST_PG_DSN is not set");
        return;
    }
    let _serial = SERIAL.lock().expect("serialize private-worker DB gate");

    let mut fixture = ContributionFixture::new();
    let command = StartManualContributionCommand::new(&fixture.private);
    let request: ContributionPreparationInput = (&fixture.request()).into();
    let tenant_id = fixture.auth.tenant_id().0;
    let key = format!("private-worker-command-{}", Uuid::new_v4());

    let outcomes = fixture.rt.block_on(async {
        tokio::join!(
            command.execute(request.clone(), key.clone()),
            command.execute(request.clone(), key.clone()),
            command.execute(request.clone(), key.clone()),
            command.execute(request, key.clone()),
        )
    });
    let outcomes = [
        outcomes.0.expect("command first creator 1"),
        outcomes.1.expect("command first creator 2"),
        outcomes.2.expect("command first creator 3"),
        outcomes.3.expect("command first creator 4"),
    ];
    assert_eq!(
        outcomes.iter().filter(|outcome| outcome.created).count(),
        1,
        "exactly one command invocation must create the durable root"
    );
    let canonical = outcomes[0];
    for outcome in outcomes {
        assert_same_ids(canonical, outcome);
    }
    assert_eq!(counts(&mut fixture.admin, tenant_id, &key), (1, 1, 1));

    let row = fixture
        .admin
        .query_one(
            "SELECT coverage_contract_version,coverage_prompt_contract_sha256,\
                    assessment_contract_version,assessment_prompt_contract_sha256 \
             FROM private.contribution_executions \
             WHERE tenant_id=$1 AND enqueue_idempotency_key=$2",
            &[&tenant_id, &key],
        )
        .expect("command-owned contract snapshot");
    let coverage = coverage_prompt_contract();
    let assessment = assessment_prompt_contract();
    assert_eq!(row.get::<_, i64>(0), coverage.version());
    assert_eq!(row.get::<_, Vec<u8>>(1), coverage.sha256().0.to_vec());
    assert_eq!(row.get::<_, i64>(2), assessment.version());
    assert_eq!(row.get::<_, Vec<u8>>(3), assessment.sha256().0.to_vec());
}
