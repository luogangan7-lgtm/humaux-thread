//! PostgreSQL 18 acceptance for the Phase 9 contribution-policy append-only lifecycle.

#[allow(deprecated)]
#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use std::{
    sync::{Arc, Barrier, Mutex},
    thread,
    time::Duration,
};

use contribution_fixture::ContributionFixture;
use humaux_adapters::{
    contribution_entry_repo::ContributionEntryRepo,
    contribution_execution_ingress::{
        ContributionExecutionIngress, ContributionExecutionIngressError,
    },
    contribution_execution_repo::EnqueuedContributionExecution,
    contribution_reasoner::{assessment_prompt_contract, coverage_prompt_contract},
};
use humaux_application::contribute::{ContributionCandidatePort, ContributionPreparationInput};
use postgres::{Client, NoTls};
use serde_json::json;
use uuid::Uuid;

static SERIAL: Mutex<()> = Mutex::new(());

fn require_db(gate_mode: &str) -> Option<String> {
    let required = std::env::var("HUMAUX_REQUIRE_DB").as_deref() == Ok("1");
    let dsn = match std::env::var("HUMAUX_TEST_PG_DSN") {
        Ok(dsn) => dsn,
        Err(_) if required => {
            panic!("HUMAUX_REQUIRE_DB=1 requires HUMAUX_TEST_PG_DSN")
        }
        Err(_) => return None,
    };
    match std::env::var("HUMAUX_0132_GATE_MODE") {
        Ok(actual) if actual == gate_mode => Some(dsn),
        Ok(actual) if required => {
            panic!("HUMAUX_REQUIRE_DB=1 requires HUMAUX_0132_GATE_MODE={gate_mode}; got {actual}")
        }
        Ok(actual) => {
            eprintln!("SKIP: HUMAUX_0132_GATE_MODE={actual}, expected {gate_mode}");
            None
        }
        Err(_) if required => {
            panic!("HUMAUX_REQUIRE_DB=1 requires HUMAUX_0132_GATE_MODE={gate_mode}")
        }
        Err(_) => {
            eprintln!("SKIP: HUMAUX_0132_GATE_MODE is not set; expected {gate_mode}");
            None
        }
    }
}

fn db_code(error: &postgres::Error) -> Option<&str> {
    error.as_db_error().map(|db| db.code().code())
}

fn assert_rejected(result: Result<u64, postgres::Error>, context: &str) {
    let error = result.expect_err(context);
    assert_eq!(db_code(&error), Some("23514"), "{context}: {error}");
}

fn append_v2_race(
    dsn: &str,
    tenant_id: Uuid,
    policy_id: Uuid,
    designated_first: usize,
) -> Vec<Result<i64, String>> {
    assert!(designated_first < 2);
    let clients = [
        Client::connect(dsn, NoTls).expect("first successor connection"),
        Client::connect(dsn, NoTls).expect("second successor connection"),
    ];
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = Vec::new();
    for (index, mut client) in clients.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            if index != designated_first {
                thread::sleep(Duration::from_millis(150));
            }
            println!("0132 successor race designated_first={designated_first} arrival={index}");
            client
                .query_one(
                    "SELECT (control.append_contribution_policy_successor(\
                       $1,$2,1,true,'MANUAL','explicit fixture redistribution rights',\
                       'v2-license',NULL,NULL,NULL)).policy_version",
                    &[&tenant_id, &policy_id],
                )
                .map(|row| row.get(0))
                .map_err(|error| {
                    let message = error
                        .as_db_error()
                        .map_or_else(|| error.to_string(), |db| db.message().to_owned());
                    format!("{}:{message}", db_code(&error).unwrap_or("none"))
                })
        }));
    }
    barrier.wait();
    handles
        .into_iter()
        .map(|handle| handle.join().expect("successor thread"))
        .collect()
}

fn migration_material() -> (toml::Value, String) {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let manifest =
        std::fs::read_to_string(root.join("0132_contribution_policy_lifecycle.manifest.toml"))
            .expect("read 0132 manifest");
    let sql = std::fs::read_to_string(root.join("0132_contribution_policy_lifecycle.sql"))
        .expect("read exact 0132 SQL");
    (toml::from_str(&manifest).expect("parse 0132 manifest"), sql)
}

fn assert_one_boolean(db: &mut Client, sql: &str, expected: bool, context: &str) {
    let rows = db.query(sql, &[]).expect(context);
    assert_eq!(rows.len(), 1, "{context}: one row");
    assert_eq!(rows[0].columns().len(), 1, "{context}: one column");
    assert_eq!(
        *rows[0].columns()[0].type_(),
        postgres::types::Type::BOOL,
        "{context}: boolean type"
    );
    assert_eq!(rows[0].get::<_, bool>(0), expected, "{context}: value");
}

fn assert_db_error<T>(
    result: Result<T, postgres::Error>,
    expected_code: &str,
    expected_message: &str,
    context: &str,
) {
    let error = match result {
        Ok(_) => panic!("{context}"),
        Err(error) => error,
    };
    let db = error.as_db_error().expect("database error");
    assert_eq!(db.code().code(), expected_code, "{context}: {db}");
    assert_eq!(db.message(), expected_message, "{context}: {db}");
}

#[derive(Clone)]
struct LegacySeed {
    tenant_id: Uuid,
    user_id: Uuid,
    policy_id: Uuid,
    domain_id: Uuid,
    binding_id: Uuid,
    memory_id: Uuid,
    source_hash: Vec<u8>,
    manifest_hash: Vec<u8>,
}

fn legacy_seed(fixture: &mut ContributionFixture) -> LegacySeed {
    let tenant_id = fixture.auth.tenant_id().0;
    let user_id = fixture
        .admin
        .query_one(
            "SELECT owner_user_id FROM control.private_reasoning_domains \
             WHERE tenant_id=$1 AND reasoning_domain_id=$2",
            &[&tenant_id, &fixture.domain],
        )
        .expect("fixture user")
        .get(0);
    let policy_id = fixture
        .admin
        .query_one(
            "SELECT policy_id FROM control.contribution_policies WHERE tenant_id=$1",
            &[&tenant_id],
        )
        .expect("fixture policy")
        .get(0);
    let source_hash: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to(content::text,'UTF8')) \
             FROM private.memory_records WHERE tenant_id=$1 AND memory_id=$2",
            &[&tenant_id, &fixture.memory],
        )
        .expect("fixture source hash")
        .get(0);
    let manifest_hash: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to('m:'||$1::uuid::text||':'||encode($2::bytea,'hex'),'UTF8'))",
            &[&fixture.memory, &source_hash],
        )
        .expect("canonical source manifest")
        .get(0);
    LegacySeed {
        tenant_id,
        user_id,
        policy_id,
        domain_id: fixture.domain,
        binding_id: fixture.binding,
        memory_id: fixture.memory,
        source_hash,
        manifest_hash,
    }
}

fn seed_unresolved_legacy_candidate(db: &mut Client, seed: &LegacySeed) -> Uuid {
    let candidate_id = Uuid::new_v4();
    let payload = b"deidentified candidate fixture".to_vec();
    let policy_snapshot = json!({
        "policy": "MANUAL",
        "principal_id": seed.user_id.to_string(),
        "allowed_workspace_ids": []
    });
    let scan_receipt = json!({
        "privacy_rules_version": "0132-gate",
        "privacy_rules_digest": "0132-gate",
        "gitleaks_version": "8.30.1",
        "gitleaks_binary_sha256": "00e91bbe655bd7c47753e8cfe61cb76ea1a5d7e7702fe161ee40102b46b3823b"
    });
    let mut txn = db.transaction().expect("legacy candidate transaction");
    txn.execute(
        "INSERT INTO staging.contribution_candidates(\
           candidate_id,tenant_id,user_id,policy_id,policy_version,policy_snapshot,\
           reasoning_domain_id,profile_version,source_manifest_hash,source_count,\
           disclosed_payload,disclosed_payload_sha256,provider_trace,scan_receipt,rights_basis) \
         VALUES($1,$2,$3,$4,999,$5,$6,1,$7,1,$8,sha256($8),$9,$10,$11)",
        &[
            &candidate_id,
            &seed.tenant_id,
            &seed.user_id,
            &seed.policy_id,
            &policy_snapshot,
            &seed.domain_id,
            &seed.manifest_hash,
            &payload,
            &"pre-0132-legacy-candidate",
            &scan_receipt,
            &"USER_CONSENT",
        ],
    )
    .expect("raw legacy candidate");
    txn.execute(
        "INSERT INTO staging.contribution_candidate_sources(\
           tenant_id,candidate_id,memory_id,source_hash,ordinal) VALUES($1,$2,$3,$4,0)",
        &[
            &seed.tenant_id,
            &candidate_id,
            &seed.memory_id,
            &seed.source_hash,
        ],
    )
    .expect("raw legacy candidate source");
    txn.commit().expect("commit legacy candidate");
    candidate_id
}

fn seed_unresolved_legacy_execution(db: &mut Client, seed: &LegacySeed) -> Uuid {
    let execution_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();
    let coverage_request_id = Uuid::new_v4();
    let assessment_request_id = Uuid::new_v4();
    let candidate_id = Uuid::new_v4();
    let key = format!("pre-0132-execution-{}", Uuid::new_v4());
    let fingerprint = vec![0x61_u8; 32];
    let policy_snapshot = json!({
        "policy": "MANUAL",
        "principal_id": seed.user_id.to_string(),
        "allowed_workspace_ids": []
    });
    let row = db
        .query_one(
            "SELECT * FROM private.enqueue_contribution_execution(\
             $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,1,$13,$14,NULL,NULL,NULL,NULL,\
             $15,1,1,1,$16,$17,$18,$19,$20)",
            &[
                &seed.tenant_id,
                &execution_id,
                &job_id,
                &coverage_request_id,
                &assessment_request_id,
                &candidate_id,
                &key,
                &fingerprint,
                &seed.user_id,
                &seed.domain_id,
                &seed.manifest_hash,
                &seed.policy_id,
                &policy_snapshot,
                &"USER_CONSENT",
                &seed.binding_id,
                &vec![0x71_u8; 32],
                &vec![0x72_u8; 32],
                &vec!["MEMORY".to_owned()],
                &vec![seed.memory_id],
                &vec![seed.source_hash.clone()],
            ],
        )
        .expect("seed valid v1 execution");
    assert!(row.get::<_, bool>(5), "legacy execution must be created");
    db.batch_execute("BEGIN; SET LOCAL session_replication_role='replica'")
        .expect("begin bounded execution fault");
    db.execute(
        "UPDATE private.contribution_executions SET policy_version=999 \
         WHERE tenant_id=$1 AND execution_id=$2",
        &[&seed.tenant_id, &execution_id],
    )
    .expect("corrupt only the execution policy version");
    db.batch_execute("COMMIT")
        .expect("commit bounded execution fault");
    execution_id
}

fn assert_pre0132_catalog_unchanged(db: &mut Client) {
    let row = db
        .query_one(
            "SELECT \
             NOT EXISTS(SELECT 1 FROM pg_attribute \
               WHERE attrelid='control.contribution_policies'::regclass \
                 AND attname IN('effective_from','effective_to') AND NOT attisdropped), \
             EXISTS(SELECT 1 FROM pg_constraint \
               WHERE conrelid='control.contribution_policies'::regclass \
                 AND conname='contribution_policies_pkey' \
                 AND pg_get_constraintdef(oid)='PRIMARY KEY (policy_id)'), \
             EXISTS(SELECT 1 FROM pg_constraint \
               WHERE conrelid='staging.contribution_candidates'::regclass \
                 AND conname='contribution_candidates_tenant_id_policy_id_fkey' \
                 AND pg_get_constraintdef(oid)=\
                   'FOREIGN KEY (tenant_id, policy_id) REFERENCES control.contribution_policies(tenant_id, policy_id)'), \
             NOT EXISTS(SELECT 1 FROM pg_constraint \
               WHERE conrelid='staging.contribution_candidates'::regclass \
                 AND conname='contribution_candidates_policy_version_fk'), \
             to_regprocedure('control.append_contribution_policy_successor(uuid,uuid,bigint,boolean,text,text,text,text,text,text)') IS NULL, \
             to_regclass('control.contribution_policies_one_open_head_per_tenant') IS NULL, \
             NOT EXISTS(SELECT 1 FROM pg_trigger \
               WHERE tgrelid='control.contribution_policies'::regclass \
                 AND tgname='contribution_policy_lifecycle' AND NOT tgisinternal), \
             NOT EXISTS(SELECT 1 FROM ops.schema_migrations \
               WHERE migration_id='0132_contribution_policy_lifecycle')",
            &[],
        )
        .expect("pre-0132 catalog shape");
    for index in 0..8 {
        assert!(
            row.get::<_, bool>(index),
            "pre-0132 catalog invariant {index}"
        );
    }
}

#[test]
#[ignore = "requires isolated PostgreSQL 18 migrated only through 0131"]
fn pre_0132_unresolved_triples_hard_stop() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let Some(_dsn) = require_db("pre0132") else {
        return;
    };
    let (manifest, migration_sql) = migration_material();
    let precheck = manifest
        .get("precheck")
        .and_then(toml::Value::as_str)
        .expect("0132 precheck");
    let mut fixture = ContributionFixture::new();
    let seed = legacy_seed(&mut fixture);
    let candidate_id = seed_unresolved_legacy_candidate(&mut fixture.admin, &seed);

    assert_one_boolean(&mut fixture.admin, precheck, false, "candidate precheck");
    assert_db_error(
        fixture.admin.batch_execute(&migration_sql),
        "55000",
        "0132 hard stop: a candidate policy version is not exactly resolvable",
        "candidate hard stop",
    );
    assert_pre0132_catalog_unchanged(&mut fixture.admin);
    let candidate_row = fixture
        .admin
        .query_one(
            "SELECT policy_version, \
             NOT EXISTS(SELECT 1 FROM control.contribution_policies \
               WHERE tenant_id=$1 AND policy_id=$2 AND policy_version=999) \
             FROM staging.contribution_candidates WHERE candidate_id=$3",
            &[&seed.tenant_id, &seed.policy_id, &candidate_id],
        )
        .expect("unresolved candidate retained");
    assert_eq!(candidate_row.get::<_, i64>(0), 999);
    assert!(
        candidate_row.get::<_, bool>(1),
        "must not invent policy 999"
    );

    fixture
        .admin
        .batch_execute("BEGIN; SET LOCAL session_replication_role='replica'")
        .expect("begin bounded candidate repair");
    fixture
        .admin
        .execute(
            "UPDATE staging.contribution_candidates SET policy_version=1 WHERE candidate_id=$1",
            &[&candidate_id],
        )
        .expect("repair candidate only to expose execution branch");
    fixture
        .admin
        .batch_execute("COMMIT")
        .expect("commit bounded candidate repair");
    assert_one_boolean(&mut fixture.admin, precheck, true, "repaired precheck");

    let execution_id = seed_unresolved_legacy_execution(&mut fixture.admin, &seed);
    assert_one_boolean(&mut fixture.admin, precheck, false, "execution precheck");
    assert_db_error(
        fixture.admin.batch_execute(&migration_sql),
        "55000",
        "0132 hard stop: an execution policy version is not exactly resolvable",
        "execution hard stop",
    );
    assert_pre0132_catalog_unchanged(&mut fixture.admin);
    let execution_row = fixture
        .admin
        .query_one(
            "SELECT policy_version, \
             NOT EXISTS(SELECT 1 FROM control.contribution_policies \
               WHERE tenant_id=$1 AND policy_id=$2 AND policy_version=999) \
             FROM private.contribution_executions WHERE execution_id=$3",
            &[&seed.tenant_id, &seed.policy_id, &execution_id],
        )
        .expect("unresolved execution retained");
    assert_eq!(execution_row.get::<_, i64>(0), 999);
    assert!(
        execution_row.get::<_, bool>(1),
        "must not invent policy 999"
    );
}

#[test]
#[ignore = "requires isolated PostgreSQL 18 migrated through 0132"]
fn manifest_postcheck_is_one_boolean_true() {
    let Some(dsn) = require_db("post0132") else {
        return;
    };
    let (manifest, _) = migration_material();
    let postcheck = manifest
        .get("postcheck")
        .and_then(toml::Value::as_str)
        .expect("0132 postcheck string");
    let mut db = Client::connect(&dsn, NoTls).expect("isolated PostgreSQL 18");
    assert_one_boolean(&mut db, postcheck, true, "exact 0132 postcheck");
}

fn fixture_policy(fixture: &mut ContributionFixture) -> (Uuid, Uuid) {
    let tenant_id = fixture.auth.tenant_id().0;
    let policy_id = fixture
        .admin
        .query_one(
            "SELECT policy_id FROM control.contribution_policies WHERE tenant_id=$1",
            &[&tenant_id],
        )
        .expect("fixture policy identity")
        .get(0);
    (tenant_id, policy_id)
}

fn append_v2(db: &mut Client, tenant_id: Uuid, policy_id: Uuid) -> i64 {
    db.query_one(
        "SELECT (control.append_contribution_policy_successor(\
           $1,$2,1,true,'MANUAL','explicit fixture redistribution rights',\
           'v2-license',NULL,NULL,NULL)).policy_version",
        &[&tenant_id, &policy_id],
    )
    .expect("append policy v2")
    .get(0)
}

fn assert_race_outcome(
    db: &mut Client,
    tenant_id: Uuid,
    policy_id: Uuid,
    outcomes: &[Result<i64, String>],
) {
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| result.as_ref() == Ok(&2))
            .count(),
        1,
        "one successor must win: {outcomes:?}"
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| result.as_ref().is_err_and(|error| {
                error == "40001:stale contribution policy successor expectation"
            }))
            .count(),
        1,
        "one exact stale loser is required: {outcomes:?}"
    );
    let history = db
        .query_one(
            "SELECT \
               count(*) FILTER(WHERE policy_version=1 AND effective_to IS NOT NULL)=1, \
               count(*) FILTER(WHERE policy_version=2 AND effective_to IS NULL)=1, \
               count(*) FILTER(WHERE effective_to IS NULL)=1, \
               count(*) FILTER(WHERE policy_version=3)=0, count(*)=2 \
             FROM control.contribution_policies WHERE tenant_id=$1 AND policy_id=$2",
            &[&tenant_id, &policy_id],
        )
        .expect("race policy history");
    for index in 0..5 {
        assert!(
            history.get::<_, bool>(index),
            "race history invariant {index}"
        );
    }
}

fn exact_execution_counts(db: &mut Client, tenant_id: Uuid, key: &str) -> (i64, i64, i64, i64) {
    let row = db
        .query_one(
            "SELECT \
             (SELECT count(*) FROM private.contribution_executions \
              WHERE tenant_id=$1 AND enqueue_idempotency_key=$2), \
             (SELECT count(*) FROM private.contribution_execution_sources source \
              JOIN private.contribution_executions root \
                ON (root.tenant_id,root.execution_id)=(source.tenant_id,source.execution_id) \
              WHERE root.tenant_id=$1 AND root.enqueue_idempotency_key=$2), \
             (SELECT count(*) FROM ops.jobs WHERE tenant_id=$1 AND idempotency_key=$2), \
             (SELECT count(*) FROM ops.contribution_execution_job_links link \
              JOIN private.contribution_executions root \
                ON (root.tenant_id,root.execution_id)=(link.tenant_id,link.execution_id) \
              WHERE root.tenant_id=$1 AND root.enqueue_idempotency_key=$2)",
            &[&tenant_id, &key],
        )
        .expect("exact durable execution counts");
    (row.get(0), row.get(1), row.get(2), row.get(3))
}

fn assert_same_execution_ids(
    expected: EnqueuedContributionExecution,
    actual: EnqueuedContributionExecution,
) {
    assert_eq!(actual.execution_id, expected.execution_id);
    assert_eq!(actual.job_id, expected.job_id);
    assert_eq!(actual.logical_call_ids, expected.logical_call_ids);
    assert_eq!(actual.candidate_id, expected.candidate_id);
}

fn assert_idempotency_conflict(
    result: Result<EnqueuedContributionExecution, ContributionExecutionIngressError>,
) {
    let error = result.expect_err("closed-policy replay must conflict");
    assert!(
        error.is_idempotency_conflict(),
        "0131 fingerprint remains the conflict authority: {error}"
    );
}

#[test]
#[ignore = "requires isolated PostgreSQL 18 migrated through 0132"]
fn same_key_replay_freezes_policy_authority() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let Some(_dsn) = require_db("post0132") else {
        return;
    };
    let mut fixture = ContributionFixture::new();
    let (tenant_id, policy_id) = fixture_policy(&mut fixture);
    let ingress = ContributionExecutionIngress::new(&fixture.private);
    let request: ContributionPreparationInput = (&fixture.request()).into();
    let key = format!("policy-replay-{}", Uuid::new_v4());

    let first = fixture
        .rt
        .block_on(ingress.start_manual(
            request.clone(),
            key.clone(),
            coverage_prompt_contract(),
            assessment_prompt_contract(),
        ))
        .expect("first v1 ingress");
    assert!(first.created);
    let replay = fixture
        .rt
        .block_on(ingress.start_manual(
            request.clone(),
            key.clone(),
            coverage_prompt_contract(),
            assessment_prompt_contract(),
        ))
        .expect("exact v1 replay");
    assert!(!replay.created);
    assert_same_execution_ids(first, replay);
    assert_eq!(
        exact_execution_counts(&mut fixture.admin, tenant_id, &key),
        (1, 1, 1, 1)
    );

    assert_eq!(append_v2(&mut fixture.admin, tenant_id, policy_id), 2);
    assert_idempotency_conflict(fixture.rt.block_on(ingress.start_manual(
        request,
        key.clone(),
        coverage_prompt_contract(),
        assessment_prompt_contract(),
    )));
    assert_eq!(
        exact_execution_counts(&mut fixture.admin, tenant_id, &key),
        (1, 1, 1, 1),
        "closed-policy replay must create no durable row"
    );
}

fn policy_history_json(db: &mut Client, tenant_id: Uuid, policy_id: Uuid) -> String {
    db.query_one(
        "SELECT jsonb_agg(to_jsonb(p) ORDER BY policy_version)::text \
         FROM control.contribution_policies p WHERE tenant_id=$1 AND policy_id=$2",
        &[&tenant_id, &policy_id],
    )
    .expect("policy history snapshot")
    .get(0)
}

fn denied_role_probe(
    txn: &mut postgres::Transaction<'_>,
    sql: &str,
    params: &[&(dyn postgres::types::ToSql + Sync)],
    context: &str,
) {
    txn.batch_execute("SAVEPOINT denied_probe")
        .expect("role probe savepoint");
    let error = txn.execute(sql, params).expect_err(context);
    assert_eq!(db_code(&error), Some("42501"), "{context}: {error}");
    txn.batch_execute("ROLLBACK TO SAVEPOINT denied_probe; RELEASE SAVEPOINT denied_probe")
        .expect("recover rejected role probe");
    assert_eq!(
        txn.query_one("SELECT 1", &[])
            .expect("transaction remains usable")
            .get::<_, i32>(0),
        1
    );
}

#[test]
#[ignore = "requires isolated PostgreSQL 18 migrated through 0132"]
fn runtime_roles_are_actually_denied_policy_mutation() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let Some(_dsn) = require_db("post0132") else {
        return;
    };
    let mut fixture = ContributionFixture::new();
    let (tenant_id, policy_id) = fixture_policy(&mut fixture);
    let before = policy_history_json(&mut fixture.admin, tenant_id, policy_id);
    let roles = [
        "role_admin",
        "role_gateway",
        "role_private_worker",
        "role_consolidation_worker",
        "role_public_worker",
        "role_retrieval_worker",
        "role_batch_issuer",
        "role_maintenance",
    ];
    for role in roles {
        let mut txn = fixture.admin.transaction().expect("role probe transaction");
        txn.batch_execute(&format!("SET LOCAL ROLE {role}"))
            .expect("assume exact runtime role");
        denied_role_probe(
            &mut txn,
            "SELECT (control.append_contribution_policy_successor(\
               $1,$2,1,true,'MANUAL','probe','probe',NULL,NULL,NULL)).policy_version",
            &[&tenant_id, &policy_id],
            &format!("{role} successor"),
        );
        let new_policy_id = Uuid::new_v4();
        denied_role_probe(
            &mut txn,
            "INSERT INTO control.contribution_policies(\
               policy_id,tenant_id,allow_public_contribution,policy_version,\
               contribution_mode,rights_basis,effective_from) \
             VALUES($1,$2,true,1,'MANUAL','probe',clock_timestamp())",
            &[&new_policy_id, &tenant_id],
            &format!("{role} insert"),
        );
        denied_role_probe(
            &mut txn,
            "UPDATE control.contribution_policies SET rights_basis='probe' \
             WHERE tenant_id=$1 AND policy_id=$2",
            &[&tenant_id, &policy_id],
            &format!("{role} update"),
        );
        denied_role_probe(
            &mut txn,
            "DELETE FROM control.contribution_policies WHERE tenant_id=$1 AND policy_id=$2",
            &[&tenant_id, &policy_id],
            &format!("{role} delete"),
        );
        denied_role_probe(
            &mut txn,
            "TRUNCATE control.contribution_policies",
            &[],
            &format!("{role} truncate"),
        );
        txn.rollback().expect("rollback role probe transaction");
    }
    assert_eq!(
        policy_history_json(&mut fixture.admin, tenant_id, policy_id),
        before,
        "runtime role probes must leave history byte-identical"
    );
}

fn assert_validated_exact_policy_fk(db: &mut Client, relation: &str, constraint: &str) {
    let row = db
        .query_one(
            "SELECT convalidated,pg_get_constraintdef(oid) FROM pg_constraint \
             WHERE conrelid=$1::text::regclass AND conname=$2",
            &[&relation, &constraint],
        )
        .expect("exact policy FK catalog row");
    assert!(row.get::<_, bool>(0), "{constraint} must be validated");
    assert_eq!(
        row.get::<_, String>(1),
        "FOREIGN KEY (tenant_id, policy_id, policy_version) REFERENCES control.contribution_policies(tenant_id, policy_id, policy_version)",
        "{constraint} exact triple definition"
    );
}

#[test]
#[ignore = "requires isolated PostgreSQL 18 migrated through 0132 and pinned Gitleaks"]
#[allow(clippy::too_many_lines)]
fn exact_history_successor_concurrency_and_head_admission_hold() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let Some(dsn) = require_db("post0132") else {
        return;
    };
    let mut fixture = ContributionFixture::new();
    let tenant_id = fixture.auth.tenant_id().0;
    let policy_row = fixture
        .admin
        .query_one(
            "SELECT policy_id,policy_version,effective_from IS NOT NULL,effective_to IS NULL \
             FROM control.contribution_policies WHERE tenant_id=$1",
            &[&tenant_id],
        )
        .expect("fixture v1 policy");
    let policy_id: Uuid = policy_row.get(0);
    assert_eq!(policy_row.get::<_, i64>(1), 1);
    assert!(
        policy_row.get::<_, bool>(2),
        "omitted effective_from is filled"
    );
    assert!(policy_row.get::<_, bool>(3), "fixture v1 starts open");

    let old_candidate = fixture.prepare();
    let old_confirmation = fixture.confirm(old_candidate);
    let ingress = ContributionExecutionIngress::new(&fixture.private);
    let request: ContributionPreparationInput = (&fixture.request()).into();
    let v1_execution = fixture
        .rt
        .block_on(ingress.start_manual(
            request.clone(),
            format!("policy-v1-{}", Uuid::new_v4()),
            coverage_prompt_contract(),
            assessment_prompt_contract(),
        ))
        .expect("v1 production preparation");
    assert_eq!(
        fixture
            .admin
            .query_one(
                "SELECT policy_version FROM private.contribution_executions \
                 WHERE tenant_id=$1 AND execution_id=$2",
                &[&tenant_id, &v1_execution.execution_id.as_uuid()],
            )
            .expect("v1 execution")
            .get::<_, i64>(0),
        1
    );

    let outcomes = append_v2_race(&dsn, tenant_id, policy_id, 0);
    assert_race_outcome(&mut fixture.admin, tenant_id, policy_id, &outcomes);
    let history = fixture
        .admin
        .query_one(
            "SELECT count(*)=2, \
               count(*) FILTER(WHERE policy_version=1 AND effective_to IS NOT NULL)=1, \
               count(*) FILTER(WHERE policy_version=2 AND effective_to IS NULL)=1, \
               count(*) FILTER(WHERE effective_to IS NULL)=1 \
             FROM control.contribution_policies WHERE tenant_id=$1 AND policy_id=$2",
            &[&tenant_id, &policy_id],
        )
        .expect("two-version history");
    for index in 0..4 {
        assert!(history.get::<_, bool>(index), "history invariant {index}");
    }
    assert_validated_exact_policy_fk(
        &mut fixture.admin,
        "staging.contribution_candidates",
        "contribution_candidates_policy_version_fk",
    );
    assert_validated_exact_policy_fk(
        &mut fixture.admin,
        "private.contribution_executions",
        "contribution_executions_policy_fk",
    );
    assert_eq!(
        fixture
            .admin
            .query_one(
                "SELECT policy_version FROM staging.contribution_candidates WHERE candidate_id=$1",
                &[&old_candidate.0],
            )
            .expect("v1 candidate remains")
            .get::<_, i64>(0),
        1
    );
    assert_eq!(
        fixture
            .admin
            .query_one(
                "SELECT policy_version FROM private.contribution_executions \
                 WHERE execution_id=$1",
                &[&v1_execution.execution_id.as_uuid()],
            )
            .expect("v1 execution remains")
            .get::<_, i64>(0),
        1
    );

    let stale = fixture.admin.query_one(
        "SELECT (control.append_contribution_policy_successor($1,$2,1,true,'MANUAL',\
          'stale',NULL,NULL,NULL,NULL)).policy_version",
        &[&tenant_id, &policy_id],
    );
    assert_eq!(
        stale
            .expect_err("stale expected version must fail")
            .as_db_error()
            .map(|error| error.code().code()),
        Some("40001")
    );

    assert_rejected(
        fixture.admin.execute(
            "UPDATE control.contribution_policies SET rights_basis='illegal rewrite' \
             WHERE tenant_id=$1 AND effective_to IS NULL",
            &[&tenant_id],
        ),
        "semantic update",
    );
    assert_rejected(
        fixture.admin.execute(
            "UPDATE control.contribution_policies SET effective_to=NULL \
             WHERE tenant_id=$1 AND policy_version=1",
            &[&tenant_id],
        ),
        "reopen closed v1",
    );
    assert_rejected(
        fixture.admin.execute(
            "DELETE FROM control.contribution_policies \
             WHERE tenant_id=$1 AND policy_version=1",
            &[&tenant_id],
        ),
        "delete historical v1",
    );
    assert_rejected(
        fixture.admin.execute(
            "INSERT INTO control.contribution_policies(\
               policy_id,tenant_id,allow_public_contribution,policy_version,contribution_mode,\
               rights_basis,effective_from) \
             VALUES($1,$2,true,4,'MANUAL','jump',clock_timestamp())",
            &[&policy_id, &tenant_id],
        ),
        "version jump",
    );
    assert_rejected(
        fixture.admin.execute(
            "INSERT INTO control.contribution_policies(\
               policy_id,tenant_id,allow_public_contribution,policy_version,contribution_mode,\
               rights_basis,effective_from) \
             VALUES($1,$2,true,3,'MANUAL','fork',clock_timestamp())",
            &[&policy_id, &tenant_id],
        ),
        "fork while v2 is open",
    );
    assert_rejected(
        fixture.admin.execute(
            "INSERT INTO control.contribution_policies(\
               policy_id,tenant_id,allow_public_contribution,rights_basis) \
             VALUES($1,$2,true,'identity drift')",
            &[&Uuid::new_v4(), &tenant_id],
        ),
        "new policy identity after tenant history exists",
    );
    let other_tenant: Uuid = fixture
        .admin
        .query_one(
            "INSERT INTO control.tenants(name,state) VALUES($1,'ACTIVE') RETURNING tenant_id",
            &[&format!("policy-drift-{}", Uuid::new_v4())],
        )
        .expect("other tenant")
        .get(0);
    assert_rejected(
        fixture.admin.execute(
            "UPDATE control.contribution_policies SET tenant_id=$2 \
             WHERE tenant_id=$1 AND policy_version=2",
            &[&tenant_id, &other_tenant],
        ),
        "tenant drift",
    );

    let v2_execution = fixture
        .rt
        .block_on(ingress.start_manual(
            request,
            format!("policy-v2-{}", Uuid::new_v4()),
            coverage_prompt_contract(),
            assessment_prompt_contract(),
        ))
        .expect("new production preparation reads v2");
    assert_eq!(
        fixture
            .admin
            .query_one(
                "SELECT policy_version FROM private.contribution_executions \
                 WHERE execution_id=$1",
                &[&v2_execution.execution_id.as_uuid()],
            )
            .expect("v2 execution")
            .get::<_, i64>(0),
        2
    );

    assert!(
        fixture
            .rt
            .block_on(
                ContributionEntryRepo::new(&fixture.private)
                    .finalize_confirmed(old_confirmation.clone())
            )
            .is_err(),
        "application release must reject the closed v1 head"
    );
    let direct_release = fixture.admin.execute(
        "INSERT INTO staging.contribution_releases(\
           tenant_id,policy_snapshot,privacy_scan_outcome,secret_scan_outcome,rights_basis,\
           source_license,publisher,contributor_attestation,redistribution_policy,candidate_id,\
           confirmation_id,disclosed_payload,disclosed_payload_sha256,scan_receipt) \
         SELECT tenant_id,policy_snapshot,'PASSED','PASSED',rights_basis,source_license,publisher,\
           contributor_attestation,redistribution_policy,candidate_id,$2,disclosed_payload,\
           disclosed_payload_sha256,scan_receipt \
         FROM staging.contribution_candidates WHERE candidate_id=$1",
        &[&old_candidate.0, &old_confirmation.confirmation_id],
    );
    let direct_error = direct_release.expect_err("DB release guard must reject closed v1");
    assert_eq!(db_code(&direct_error), Some("42501"));

    let v2_candidate = fixture.prepare();
    assert_eq!(
        fixture
            .admin
            .query_one(
                "SELECT policy_version FROM staging.contribution_candidates WHERE candidate_id=$1",
                &[&v2_candidate.0],
            )
            .expect("v2 candidate")
            .get::<_, i64>(0),
        2
    );
    let v2_confirmation = fixture.confirm(v2_candidate);
    fixture
        .rt
        .block_on(ContributionEntryRepo::new(&fixture.private).finalize_confirmed(v2_confirmation))
        .expect("open enabled v2 releases normally");

    let mut reverse_fixture = ContributionFixture::new();
    let (reverse_tenant, reverse_policy) = fixture_policy(&mut reverse_fixture);
    let reverse_outcomes = append_v2_race(&dsn, reverse_tenant, reverse_policy, 1);
    assert_race_outcome(
        &mut reverse_fixture.admin,
        reverse_tenant,
        reverse_policy,
        &reverse_outcomes,
    );
}
