//! Phase 9 R4-A durable SQL authority gates.

#![allow(deprecated)] // Shared fixture intentionally covers the legacy preparation boundary.

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Barrier},
};

use contribution_fixture::ContributionFixture;
use postgres::{Client, NoTls, Row};
use serde_json::json;
use uuid::Uuid;

#[derive(Clone)]
struct Execution {
    tenant: Uuid,
    user: Uuid,
    domain: Uuid,
    binding: Uuid,
    policy: Uuid,
    source: Uuid,
    source_hash: Vec<u8>,
    manifest_hash: Vec<u8>,
    execution: Uuid,
    job: Uuid,
    coverage_request: Uuid,
    assessment_request: Uuid,
    candidate: Uuid,
    fingerprint: Vec<u8>,
    key: String,
}

type FaultKey = (&'static str, &'static str);

const SQL_ATOMIC_FAULT_KEYS: &[FaultKey] = &[
    ("R4-FG-01", "BASE"),
    ("R4-FG-02", "BASE"),
    ("R4-FG-03", "BASE"),
    ("R4-FG-04", "BASE"),
    ("R4-FG-06", "BASE"),
    ("R4-FG-07", "BASE"),
    ("R4-FG-08", "BASE"),
    ("R4-FG-11", "BASE"),
    ("R4-FG-12", "BASE"),
    ("R4-FG-13", "BASE"),
    ("R4-FG-14", "BASE"),
    ("R4-FG-16", "BASE"),
    ("R4-FG-17", "BASE"),
];

const SQL_OUTCOME_FAULT_KEYS: &[FaultKey] = &[
    ("R4-FG-09", "BASE"),
    ("R4-FG-10", "BASE"),
    ("R4-FG-15", "BASE"),
    ("R4-FG-18", "B_GATE_FAIL"),
    ("R4-FG-18", "SCAN_REJECT"),
    ("R4-FG-18", "PROVIDER_DEFINITE_FAILURE"),
    ("R4-FG-18", "TIMEOUT"),
];

#[derive(Default)]
struct SqlDurableEvidence {
    expected: BTreeSet<(String, String)>,
    executed: BTreeSet<(String, String)>,
}

impl SqlDurableEvidence {
    fn new(expected: &[FaultKey]) -> Self {
        assert_manifest_sql_subset_is_exact();
        Self {
            expected: expected
                .iter()
                .map(|(gate_id, variant)| ((*gate_id).to_owned(), (*variant).to_owned()))
                .collect(),
            executed: BTreeSet::new(),
        }
    }

    fn observe<F>(&mut self, key: FaultKey, assertion: F)
    where
        F: FnOnce(&mut Client),
    {
        let owned_key = (key.0.to_owned(), key.1.to_owned());
        assert!(
            self.expected.contains(&owned_key),
            "undeclared SQL fault evidence: {}/{}",
            key.0,
            key.1
        );
        let mut db = Client::connect(&dsn(), NoTls).expect("fresh durable-observation PG");
        assertion(&mut db);
        println!(
            "R4_FAULT_EVIDENCE {}",
            json!({
                "gate_id": key.0,
                "variant": key.1,
                "sql_case_id": deterministic_sql_case_id(key.0, key.1),
                "durable_observation_connection": "reconnected",
                "probe_axes": ["execution", "job", "ledger", "disclosure", "candidate"],
                "provider": "not_applicable",
                "phase10": false,
            })
        );
        assert!(
            self.executed.insert(owned_key),
            "duplicate SQL fault evidence: {}/{}",
            key.0,
            key.1
        );
    }

    fn finish(self) {
        assert_eq!(
            self.executed, self.expected,
            "every declared SQL fault case must execute exactly once"
        );
    }
}

fn deterministic_sql_case_id(gate_id: &str, variant: &str) -> String {
    format!(
        "sql_{}_{}",
        gate_id.to_ascii_lowercase().replace('-', "_"),
        variant.to_ascii_lowercase()
    )
}

fn manifest_sql_cases() -> BTreeMap<(String, String), String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../contracts/r4_fault_manifest.toml");
    let raw = std::fs::read_to_string(path).expect("read canonical R4 fault manifest");
    let manifest: toml::Value = toml::from_str(&raw).expect("parse canonical R4 fault manifest");
    let cases = manifest
        .get("fault_case")
        .and_then(toml::Value::as_array)
        .expect("R4 fault_case array");
    let mut sql_cases = BTreeMap::new();
    let mut sql_case_ids = BTreeSet::new();

    for case in cases {
        let table = case.as_table().expect("R4 fault case table");
        let sql_case_id = table
            .get("sql_case_id")
            .and_then(toml::Value::as_str)
            .expect("R4 sql_case_id");
        if sql_case_id == "NONE" {
            continue;
        }
        let gate_id = table
            .get("gate_id")
            .and_then(toml::Value::as_str)
            .expect("R4 gate_id");
        let variant = table
            .get("variant")
            .and_then(toml::Value::as_str)
            .expect("R4 variant");
        assert_eq!(
            table
                .get("requires_fresh_connection")
                .and_then(toml::Value::as_bool),
            Some(true),
            "SQL evidence must require a fresh connection for {gate_id}/{variant}"
        );
        let expected_case_id = deterministic_sql_case_id(gate_id, variant);
        assert_eq!(
            sql_case_id, expected_case_id,
            "SQL case id must be deterministic for {gate_id}/{variant}"
        );
        assert!(
            sql_case_ids.insert(sql_case_id.to_owned()),
            "duplicate SQL case id: {sql_case_id}"
        );
        assert!(
            sql_cases
                .insert(
                    (gate_id.to_owned(), variant.to_owned()),
                    sql_case_id.to_owned(),
                )
                .is_none(),
            "duplicate SQL fault key: {gate_id}/{variant}"
        );
    }
    sql_cases
}

fn assert_manifest_sql_subset_is_exact() {
    let declared = manifest_sql_cases();
    let expected = SQL_ATOMIC_FAULT_KEYS
        .iter()
        .chain(SQL_OUTCOME_FAULT_KEYS)
        .map(|(gate_id, variant)| ((*gate_id).to_owned(), (*variant).to_owned()))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        declared.keys().cloned().collect::<BTreeSet<_>>(),
        expected,
        "canonical manifest SQL subset drifted from executable reconnect evidence"
    );
}

#[test]
fn r4_sql_durable_evidence_subset_matches_manifest() {
    assert_manifest_sql_subset_is_exact();
}

fn dsn() -> String {
    std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL 18 DSN")
}

fn database_dsn(base: &str, database: &str) -> String {
    let (without_query, query) = base
        .split_once('?')
        .map_or((base, None), |(head, tail)| (head, Some(tail)));
    let slash = without_query.rfind('/').expect("database path in test DSN");
    format!(
        "{}{database}{}",
        &without_query[..=slash],
        query.map_or(String::new(), |tail| format!("?{tail}"))
    )
}

fn execute_manifest_postcheck(db: &mut Client, migration: &str) -> bool {
    let manifest_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../../migrations/{migration}.manifest.toml"));
    let manifest = std::fs::read_to_string(manifest_path).expect("read migration manifest");
    let manifest: toml::Value = toml::from_str(&manifest).expect("parse migration manifest");
    let postcheck = manifest
        .get("postcheck")
        .and_then(toml::Value::as_str)
        .expect("manifest postcheck string");
    let rows = db.query(postcheck, &[]).expect("execute exact postcheck");
    assert_eq!(rows.len(), 1, "postcheck must return exactly one row");
    assert_eq!(
        rows[0].columns().len(),
        1,
        "postcheck must return exactly one column"
    );
    assert_eq!(
        *rows[0].columns()[0].type_(),
        postgres::types::Type::BOOL,
        "postcheck result must be boolean"
    );
    rows[0].get(0)
}

#[test]
#[ignore = "lane(a:disposable) creates and removes a disposable PostgreSQL 18 database"]
fn r4_manifest_postcheck_supersession_is_explicit_after_0133() {
    let base_dsn = dsn();
    let database = format!("humaux_0131_postcheck_{}", Uuid::new_v4().simple());
    let test_dsn = database_dsn(&base_dsn, &database);
    let mut admin = Client::connect(&base_dsn, NoTls).expect("admin DB");
    admin
        .batch_execute(&format!("CREATE DATABASE {database}"))
        .expect("create disposable postcheck DB");

    let (postcheck_0131, postcheck_0133) = {
        let mut db = Client::connect(&test_dsn, NoTls).expect("postcheck DB");
        let migration_dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut paths = std::fs::read_dir(&migration_dir)
            .expect("migration directory")
            .map(|entry| entry.expect("migration entry").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "sql"))
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.get(..4))
                    .and_then(|prefix| prefix.parse::<u16>().ok())
                    .is_some_and(|number| number <= 133)
            })
            .collect::<Vec<_>>();
        paths.sort();
        for path in paths {
            db.batch_execute(&std::fs::read_to_string(&path).expect("read migration"))
                .unwrap_or_else(|error| panic!("apply {}: {error}", path.display()));
        }
        (
            execute_manifest_postcheck(&mut db, "0131_contribution_execution"),
            execute_manifest_postcheck(
                &mut db,
                "0133_contribution_self_principal_reservation_authority",
            ),
        )
    };

    admin
        .batch_execute(&format!("DROP DATABASE {database} WITH (FORCE)"))
        .expect("drop disposable postcheck DB");
    assert!(
        !postcheck_0131,
        "0133 intentionally revokes the 0131 internal 14-argument reservation ACL"
    );
    assert!(
        postcheck_0133,
        "the successor manifest must be the exact current postcheck"
    );
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL 18 migrated through 0131"]
fn r4_security_definer_function_matrix_is_exact() {
    let mut db = Client::connect(&dsn(), NoTls).expect("isolated PG");
    let row = db
        .query_one(
            r#"
            WITH exposed(oid) AS (SELECT unnest(ARRAY[
              'private.enqueue_contribution_execution(uuid,uuid,uuid,uuid,uuid,uuid,text,bytea,uuid,uuid,bytea,uuid,bigint,jsonb,text,text,text,text,text,uuid,bigint,bigint,bigint,bytea,bytea,text[],uuid[],bytea[])'::regprocedure,
              'private.reserve_contribution_a(uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint,jsonb)'::regprocedure,
              'private.reserve_contribution_b(uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint,jsonb)'::regprocedure,
              'private.complete_contribution_a_exact(uuid,uuid,uuid,uuid,bytea,uuid,text,bytea,uuid,integer,bytea,bytea,jsonb,bytea,text,text,text,uuid,text,integer)'::regprocedure,
              'private.complete_contribution_b_exact(uuid,uuid,uuid,uuid,bytea,uuid,text,bytea,bytea,text,text,text,text,bytea,bytea,jsonb,bytea,text,text,text,uuid,text,integer)'::regprocedure,
              'private.commit_contribution_candidate(uuid,uuid,uuid,text,integer)'::regprocedure,
              'private.settle_contribution_terminal_job(uuid,uuid,uuid,text,integer)'::regprocedure,
              'private.mark_contribution_reconciliation_required(uuid,uuid,uuid,text,integer)'::regprocedure
            ])), internal(oid) AS (SELECT unnest(ARRAY[
              'private.require_contribution_execution_lease(uuid,uuid,uuid,text,integer)'::regprocedure,
              'private.reserve_contribution_execution_call(uuid,uuid,uuid,text,integer,text,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint)'::regprocedure,
              'private.reserve_contribution_a(uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint)'::regprocedure,
              'private.reserve_contribution_b(uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint)'::regprocedure,
              'private.settle_contribution_job_if_live(uuid,uuid,uuid,text,integer,text,text)'::regprocedure
            ])), all_functions(oid) AS (
              SELECT oid FROM exposed UNION ALL SELECT oid FROM internal
            )
            SELECT
              (SELECT count(*)=13 AND bool_and(p.prosecdef
                AND p.proowner='role_migration_owner'::regrole
                AND p.proconfig=ARRAY['search_path=pg_catalog']::text[])
               FROM pg_proc p JOIN all_functions f USING(oid)),
              (SELECT count(*)=8 AND bool_and(
                 has_function_privilege('role_private_worker',p.oid,'EXECUTE')
                 AND NOT has_function_privilege(0,p.oid,'EXECUTE')
                 AND NOT EXISTS (
                   SELECT 1 FROM unnest(ARRAY[
                     'role_admin','role_gateway','role_consolidation_worker','role_public_worker',
                     'role_retrieval_worker','role_batch_issuer','role_maintenance'
                   ]) role_name WHERE has_function_privilege(role_name,p.oid,'EXECUTE')
                 )) FROM pg_proc p JOIN exposed e USING(oid)),
              (SELECT count(*)=5 AND bool_and(
                 NOT has_function_privilege(0,p.oid,'EXECUTE')
                 AND NOT EXISTS (
                   SELECT 1 FROM unnest(ARRAY[
                     'role_admin','role_gateway','role_private_worker','role_consolidation_worker',
                     'role_public_worker','role_retrieval_worker','role_batch_issuer','role_maintenance'
                   ]) role_name WHERE has_function_privilege(role_name,p.oid,'EXECUTE')
                 )) FROM pg_proc p JOIN internal i USING(oid))
            "#,
            &[],
        )
        .expect("exact SECURITY DEFINER matrix");
    assert!(row.get::<_, bool>(0), "all 13 owner/definer/search_path");
    assert!(
        row.get::<_, bool>(1),
        "only private worker executes exposed 8"
    );
    assert!(
        row.get::<_, bool>(2),
        "no non-owner/PUBLIC executes internal 5"
    );
}

fn prepare_execution(fixture: &mut ContributionFixture, label: &str) -> Execution {
    let tenant: Uuid = fixture
        .admin
        .query_one(
            "SELECT tenant_id FROM private.memory_records WHERE memory_id=$1",
            &[&fixture.memory],
        )
        .expect("fixture tenant")
        .get(0);
    let user: Uuid = fixture
        .admin
        .query_one(
            "SELECT owner_user_id FROM control.private_reasoning_domains WHERE reasoning_domain_id=$1",
            &[&fixture.domain],
        )
        .expect("fixture user")
        .get(0);
    let policy: Uuid = fixture
        .admin
        .query_one(
            "SELECT policy_id FROM control.contribution_policies WHERE tenant_id=$1",
            &[&tenant],
        )
        .expect("fixture policy")
        .get(0);
    let source_hash: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to(content::text,'UTF8')) \
             FROM private.memory_records WHERE memory_id=$1",
            &[&fixture.memory],
        )
        .expect("current memory source hash")
        .get(0);
    let manifest_hash: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to('m:'||$1::uuid::text||':'||encode($2::bytea,'hex'),'UTF8'))",
            &[&fixture.memory, &source_hash],
        )
        .expect("canonical input manifest")
        .get(0);
    Execution {
        tenant,
        user,
        domain: fixture.domain,
        binding: fixture.binding,
        policy,
        source: fixture.memory,
        source_hash,
        manifest_hash,
        execution: Uuid::new_v4(),
        job: Uuid::new_v4(),
        coverage_request: Uuid::new_v4(),
        assessment_request: Uuid::new_v4(),
        candidate: Uuid::new_v4(),
        fingerprint: vec![0x61; 32],
        key: format!("r4-{label}-{}", Uuid::new_v4()),
    }
}

fn seed_execution(fixture: &mut ContributionFixture, label: &str) -> Execution {
    let execution = prepare_execution(fixture, label);
    let row =
        enqueue(&mut fixture.admin, &execution, &execution.fingerprint).expect("enqueue execution");
    assert!(row.get::<_, bool>(5));
    execution
}

fn enqueue(
    db: &mut Client,
    execution: &Execution,
    fingerprint: &[u8],
) -> Result<Row, postgres::Error> {
    let policy_snapshot = json!({
        "policy": "MANUAL",
        "principal_id": execution.user.to_string(),
        "allowed_workspace_ids": []
    });
    db.query_one(
        "SELECT * FROM private.enqueue_contribution_execution(\
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,1,$13,$14,NULL,NULL,NULL,NULL,\
         $15,1,1,1,$16,$17,$18,$19,$20)",
        &[
            &execution.tenant,
            &execution.execution,
            &execution.job,
            &execution.coverage_request,
            &execution.assessment_request,
            &execution.candidate,
            &execution.key,
            &fingerprint,
            &execution.user,
            &execution.domain,
            &execution.manifest_hash,
            &execution.policy,
            &policy_snapshot,
            &"USER_CONSENT",
            &execution.binding,
            &vec![0x71_u8; 32],
            &vec![0x72_u8; 32],
            &vec!["MEMORY".to_owned()],
            &vec![execution.source],
            &vec![execution.source_hash.clone()],
        ],
    )
}

fn claim(db: &mut Client, execution: &Execution, owner: &str) -> i32 {
    db.query_one(
        "UPDATE ops.jobs SET status='PROCESSING',attempt=attempt+1,lease_owner=$2,\
         lease_expires_at=clock_timestamp()+interval '10 minutes'\
         WHERE job_id=$1 RETURNING attempt",
        &[&execution.job, &owner],
    )
    .expect("fresh job lease")
    .get(0)
}

fn prepared_route(db: &mut Client, execution: &Execution) -> serde_json::Value {
    db.query_one(
        "SELECT jsonb_build_object(\
         'schema_version',1,'tenant_id',admission.tenant_id,'binding_id',admission.binding_id,\
         'binding_version',admission.binding_version,'reasoning_domain_id',admission.reasoning_domain_id,\
         'purpose',admission.purpose,'route_policy_id',admission.route_policy_id,\
         'route_policy_version',admission.route_policy_version,'profile_id',admission.profile_id,\
         'profile_version',admission.profile_version,'provider_account_id',admission.provider_account_id,\
         'processor_id',admission.processor_id,'processor_model_id',admission.processor_model_id,\
         'provider_model_id',admission.provider_model_id,'model_revision',admission.model_revision,\
         'provider_endpoint_id',admission.provider_endpoint_id,'egress_processor_id',admission.egress_processor_id,\
         'endpoint_ref',admission.endpoint_ref,'region',admission.region,'service_tier',admission.service_tier,\
         'credential_ref',admission.credential_ref,'billing_account_id',admission.billing_account_id,\
         'billing_instrument_id',admission.billing_instrument_id) \
         FROM control.resolve_user_reasoning_admission($1,1,$2,'CONTRIBUTION_DEIDENTIFY') admission",
        &[&execution.binding, &execution.domain],
    )
    .expect("current route expectation")
    .get(0)
}

fn reserve_a(
    db: &mut Client,
    execution: &Execution,
    owner: &str,
    attempt: i32,
    intent: &[u8],
) -> (Uuid, Uuid) {
    let model_call = Uuid::new_v4();
    let disclosure = Uuid::new_v4();
    let route = prepared_route(db, execution);
    let row = db
        .query_one(
            "SELECT * FROM private.reserve_contribution_a(\
             $1,$2,$3,$4,$5,$6,$7,$8,$9,NULL,NULL,'PRIVATE',$10,17,$11)",
            &[
                &execution.tenant,
                &execution.execution,
                &execution.job,
                &owner,
                &attempt,
                &model_call,
                &disclosure,
                &intent,
                &Uuid::new_v4(),
                &vec![0x81_u8; 32],
                &route,
            ],
        )
        .expect("reserve A");
    assert!(
        row.get::<_, bool>(8),
        "new reserve is the only dispatch permit"
    );
    (row.get(0), row.get(1))
}

fn reserve_b(
    db: &mut Client,
    execution: &Execution,
    owner: &str,
    attempt: i32,
    intent: &[u8],
) -> (Uuid, Uuid) {
    let route = prepared_route(db, execution);
    let row = db
        .query_one(
            "SELECT * FROM private.reserve_contribution_b(\
             $1,$2,$3,$4,$5,$6,$7,$8,$9,NULL,NULL,'PRIVATE',$10,23,$11)",
            &[
                &execution.tenant,
                &execution.execution,
                &execution.job,
                &owner,
                &attempt,
                &Uuid::new_v4(),
                &Uuid::new_v4(),
                &intent,
                &Uuid::new_v4(),
                &vec![0x82_u8; 32],
                &route,
            ],
        )
        .expect("reserve B");
    assert!(row.get::<_, bool>(8));
    (row.get(0), row.get(1))
}

fn scanner_receipt() -> serde_json::Value {
    json!({
        "privacy_rules_version": "r4",
        "privacy_rules_digest": "r4",
        "gitleaks_version": "r4",
        "gitleaks_binary_sha256": "a".repeat(64)
    })
}

fn receipt_hash(db: &mut Client, receipt: &serde_json::Value) -> Vec<u8> {
    db.query_one(
        "SELECT sha256(convert_to($1::jsonb::text,'UTF8'))",
        &[receipt],
    )
    .expect("receipt hash")
    .get(0)
}

struct BReservation {
    attempt: i32,
    call: Uuid,
    disclosure: Uuid,
    intent: Vec<u8>,
}

fn advance_to_b_reserved(
    fixture: &mut ContributionFixture,
    execution: &Execution,
    owner: &str,
) -> BReservation {
    let attempt = claim(&mut fixture.admin, execution, owner);
    let a_intent = vec![0xc1; 32];
    let (a_call, a_disclosure) =
        reserve_a(&mut fixture.admin, execution, owner, attempt, &a_intent);
    let receipt = scanner_receipt();
    let receipt_digest = receipt_hash(&mut fixture.admin, &receipt);
    fixture
        .admin
        .query_one(
            "SELECT private.complete_contribution_a_exact(\
             $1,$2,$3,$4,$5,$6,'USABLE',$7,$8,1,$9,$10,$11,$12,'a-trace',\
             'provider-a',NULL,NULL,NULL,NULL)",
            &[
                &execution.tenant,
                &execution.execution,
                &a_call,
                &execution.coverage_request,
                &a_intent,
                &a_disclosure,
                &vec![0xc2_u8; 32],
                &Uuid::new_v4(),
                &b"coverage".to_vec(),
                &vec![0xc3_u8; 32],
                &receipt,
                &receipt_digest,
            ],
        )
        .expect("complete A before B variant");
    let intent = vec![0xd1; 32];
    let (call, disclosure) = reserve_b(&mut fixture.admin, execution, owner, attempt, &intent);
    BReservation {
        attempt,
        call,
        disclosure,
        intent,
    }
}

fn outcome_a(db: &mut Client, execution: &Execution) -> (String, String, Option<Uuid>) {
    let row = db
        .query_one(
            "SELECT e.state::text,l.status,e.coverage_snapshot_id \
             FROM private.contribution_executions e \
             JOIN ops.model_call_ledger l ON l.model_call_id=e.coverage_model_call_id \
             WHERE e.execution_id=$1",
            &[&execution.execution],
        )
        .expect("durable A outcome");
    (row.get(0), row.get(1), row.get(2))
}

fn outcome_row(db: &mut Client, execution: &Execution) -> (String, String, String, Option<String>) {
    let row = db
        .query_one(
            "SELECT e.state::text,l.status,j.status,j.last_error_class FROM private.contribution_executions e JOIN ops.model_call_ledger l ON l.model_call_id=e.assessment_model_call_id JOIN ops.contribution_execution_job_links link USING(execution_id) JOIN ops.jobs j USING(job_id) WHERE e.execution_id=$1",
            &[&execution.execution],
        )
        .expect("durable outcome row");
    (row.get(0), row.get(1), row.get(2), row.get(3))
}

fn candidate_count(db: &mut Client, execution: &Execution) -> i64 {
    db.query_one(
        "SELECT count(*) FROM staging.contribution_candidates WHERE contribution_execution_id=$1",
        &[&execution.execution],
    )
    .expect("candidate count")
    .get(0)
}

fn durable_snapshot(db: &mut Client, execution: &Execution) -> serde_json::Value {
    db.query_one(
        r#"
        SELECT jsonb_build_object(
          'execution_state', e.state::text,
          'job_status', j.status,
          'last_error_class', j.last_error_class,
          'coverage_ledger_status', coverage_call.status,
          'coverage_disclosure_outcome', coverage_disclosure.outcome,
          'assessment_ledger_status', assessment_call.status,
          'assessment_disclosure_outcome', assessment_disclosure.outcome,
          'candidate_count', (
            SELECT count(*)
            FROM staging.contribution_candidates candidate
            WHERE candidate.contribution_execution_id=e.execution_id
          )
        )
        FROM private.contribution_executions e
        JOIN ops.contribution_execution_job_links link USING(execution_id)
        JOIN ops.jobs j USING(job_id)
        LEFT JOIN ops.model_call_ledger coverage_call
          ON coverage_call.tenant_id=e.tenant_id
         AND coverage_call.model_call_id=e.coverage_model_call_id
        LEFT JOIN ops.data_disclosures coverage_disclosure
          ON coverage_disclosure.tenant_id=e.tenant_id
         AND coverage_disclosure.disclosure_id=e.coverage_disclosure_id
        LEFT JOIN ops.model_call_ledger assessment_call
          ON assessment_call.tenant_id=e.tenant_id
         AND assessment_call.model_call_id=e.assessment_model_call_id
        LEFT JOIN ops.data_disclosures assessment_disclosure
          ON assessment_disclosure.tenant_id=e.tenant_id
         AND assessment_disclosure.disclosure_id=e.assessment_disclosure_id
        WHERE e.execution_id=$1
        "#,
        &[&execution.execution],
    )
    .expect("fresh durable snapshot")
    .get(0)
}

fn expected_snapshot(
    execution_state: &str,
    job_status: &str,
    last_error_class: Option<&str>,
    coverage: Option<(&str, Option<&str>)>,
    assessment: Option<(&str, Option<&str>)>,
    candidate_count: i64,
) -> serde_json::Value {
    let (coverage_ledger_status, coverage_disclosure_outcome) =
        coverage.map_or((None, None), |(status, outcome)| (Some(status), outcome));
    let (assessment_ledger_status, assessment_disclosure_outcome) =
        assessment.map_or((None, None), |(status, outcome)| (Some(status), outcome));
    json!({
        "execution_state": execution_state,
        "job_status": job_status,
        "last_error_class": last_error_class,
        "coverage_ledger_status": coverage_ledger_status,
        "coverage_disclosure_outcome": coverage_disclosure_outcome,
        "assessment_ledger_status": assessment_ledger_status,
        "assessment_disclosure_outcome": assessment_disclosure_outcome,
        "candidate_count": candidate_count,
    })
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL 18 migrated through 0131"]
#[allow(clippy::too_many_lines)] // One DB connection verifies the coupled durable bundle after every injected fault.
fn r4_sql_authority_exact_late_and_atomic_candidate() {
    let mut fixture = ContributionFixture::new();
    let mut evidence = SqlDurableEvidence::new(SQL_ATOMIC_FAULT_KEYS);
    let execution = prepare_execution(&mut fixture, "atomic");

    // R4-FG-01/BASE: two first enqueue calls race behind the tenant/key lock. Exactly one
    // creates the root bundle and both return the same pre-minted identities.
    let barrier = Arc::new(Barrier::new(2));
    let concurrent = std::thread::scope(|scope| {
        let handles = (0..2)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                let execution = execution.clone();
                scope.spawn(move || {
                    let mut db = Client::connect(&dsn(), NoTls).expect("concurrent enqueue DB");
                    barrier.wait();
                    let row = enqueue(&mut db, &execution, &execution.fingerprint)
                        .expect("concurrent same-fingerprint enqueue");
                    (
                        row.get::<_, Uuid>(0),
                        row.get::<_, Uuid>(2),
                        row.get::<_, Uuid>(3),
                        row.get::<_, Uuid>(4),
                        row.get::<_, bool>(5),
                    )
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("enqueue thread"))
            .collect::<Vec<_>>()
    });
    assert_eq!(concurrent.iter().filter(|row| row.4).count(), 1);
    assert!(concurrent.iter().all(|row| {
        row.0 == execution.execution
            && row.1 == execution.coverage_request
            && row.2 == execution.assessment_request
            && row.3 == execution.candidate
    }));
    let bundle_counts = fixture
        .admin
        .query_one(
            "SELECT (SELECT count(*) FROM ops.jobs WHERE job_id=$1),\
             (SELECT count(*) FROM private.contribution_executions WHERE execution_id=$2),\
             (SELECT count(*) FROM private.contribution_execution_sources WHERE execution_id=$2)",
            &[&execution.job, &execution.execution],
        )
        .expect("one concurrent root bundle");
    assert_eq!(bundle_counts.get::<_, i64>(0), 1);
    assert_eq!(bundle_counts.get::<_, i64>(1), 1);
    assert_eq!(bundle_counts.get::<_, i64>(2), 1);
    evidence.observe(("R4-FG-01", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &execution),
            expected_snapshot("READY_A", "PENDING", None, None, None, 0)
        );
        let counts = db
            .query_one(
                "SELECT (SELECT count(*) FROM ops.jobs WHERE job_id=$1),\
                 (SELECT count(*) FROM private.contribution_executions WHERE execution_id=$2),\
                 (SELECT count(*) FROM private.contribution_execution_sources WHERE execution_id=$2)",
                &[&execution.job, &execution.execution],
            )
            .expect("fresh concurrent root counts");
        assert_eq!(counts.get::<_, i64>(0), 1);
        assert_eq!(counts.get::<_, i64>(1), 1);
        assert_eq!(counts.get::<_, i64>(2), 1);
    });

    // R4-FG-02/BASE: stable idempotency and conflict-without-additional-rows.
    let same = enqueue(&mut fixture.admin, &execution, &execution.fingerprint).expect("same retry");
    assert!(!same.get::<_, bool>(5));
    assert_eq!(same.get::<_, Uuid>(0), execution.execution);
    let count_before: i64 = fixture
        .admin
        .query_one("SELECT count(*) FROM private.contribution_executions", &[])
        .expect("count")
        .get(0);
    assert!(enqueue(&mut fixture.admin, &execution, &[0x62; 32]).is_err());
    let count_after: i64 = fixture
        .admin
        .query_one("SELECT count(*) FROM private.contribution_executions", &[])
        .expect("count")
        .get(0);
    assert_eq!(count_before, count_after);
    evidence.observe(("R4-FG-02", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &execution),
            expected_snapshot("READY_A", "PENDING", None, None, None, 0)
        );
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM private.contribution_executions WHERE tenant_id=$1 AND enqueue_idempotency_key=$2",
                &[&execution.tenant, &execution.key],
            )
            .expect("fresh idempotency-key count")
            .get::<_, i64>(0),
            1
        );
    });

    // R4-FG-03/BASE: a committed claim without an A reservation leaves READY_A and a
    // later live lease can reserve the pre-minted A request.
    let owner = "r4-worker";
    let attempt = claim(&mut fixture.admin, &execution, owner);
    let ready_a: String = fixture
        .admin
        .query_one(
            "SELECT state::text FROM private.contribution_executions WHERE execution_id=$1",
            &[&execution.execution],
        )
        .expect("claim-before-A durable state")
        .get(0);
    assert_eq!(ready_a, "READY_A");
    evidence.observe(("R4-FG-03", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &execution),
            expected_snapshot("READY_A", "PROCESSING", None, None, None, 0)
        );
    });
    let a_intent = vec![0xa1; 32];
    let (a_call, a_disclosure) =
        reserve_a(&mut fixture.admin, &execution, owner, attempt, &a_intent);

    // R4-FG-04/BASE durable half: the A reserve is committed before external I/O and no
    // provider result exists in the database yet.
    let a_reserved: String = fixture
        .admin
        .query_one(
            "SELECT state::text FROM private.contribution_executions WHERE execution_id=$1",
            &[&execution.execution],
        )
        .expect("A reservation durable before I/O")
        .get(0);
    assert_eq!(a_reserved, "A_RESERVED");
    evidence.observe(("R4-FG-04", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &execution),
            expected_snapshot(
                "A_RESERVED",
                "PROCESSING",
                None,
                Some(("RESERVED", None)),
                None,
                0,
            )
        );
    });

    fixture
        .admin
        .execute(
            "UPDATE ops.jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1",
            &[&execution.job],
        )
        .expect("expire original claimant");

    // R4-FG-14/BASE: a stale exact-hash mismatch mutates nothing.
    let scan = scanner_receipt();
    let scan_hash = receipt_hash(&mut fixture.admin, &scan);
    assert!(
        fixture
            .admin
            .query_one(
                "SELECT private.complete_contribution_a_exact(\
             $1,$2,$3,$4,$5,$6,'USABLE',$7,$8,1,$9,$10,$11,$12,'a-trace',\
             'provider-a',NULL,NULL,NULL,NULL)",
                &[
                    &execution.tenant,
                    &execution.execution,
                    &a_call,
                    &execution.coverage_request,
                    &vec![0xff_u8; 32],
                    &a_disclosure,
                    &vec![0xa2_u8; 32],
                    &Uuid::new_v4(),
                    &b"coverage".to_vec(),
                    &vec![0xa3_u8; 32],
                    &scan,
                    &scan_hash,
                ],
            )
            .is_err()
    );
    let unchanged: (String, String) = {
        let row = fixture
            .admin
            .query_one(
                "SELECT e.state::text,l.status FROM private.contribution_executions e JOIN ops.model_call_ledger l ON l.model_call_id=e.coverage_model_call_id WHERE e.execution_id=$1",
                &[&execution.execution],
            )
            .expect("unchanged A reservation");
        (row.get(0), row.get(1))
    };
    assert_eq!(unchanged, ("A_RESERVED".to_owned(), "RESERVED".to_owned()));
    evidence.observe(("R4-FG-14", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &execution),
            expected_snapshot(
                "A_RESERVED",
                "PROCESSING",
                None,
                Some(("RESERVED", None)),
                None,
                0,
            )
        );
    });

    // R4-FG-06/BASE: a successful A coverage result in an aborted completion transaction
    // leaves the root, ledger, and disclosure reservation unchanged.
    let coverage_snapshot = Uuid::new_v4();
    fixture.admin.batch_execute("BEGIN").expect("begin A abort");
    fixture
        .admin
        .query_one(
            "SELECT private.complete_contribution_a_exact(\
             $1,$2,$3,$4,$5,$6,'USABLE',$7,$8,1,$9,$10,$11,$12,'a-trace',\
             'provider-a',NULL,NULL,NULL,NULL)",
            &[
                &execution.tenant,
                &execution.execution,
                &a_call,
                &execution.coverage_request,
                &a_intent,
                &a_disclosure,
                &vec![0xa2_u8; 32],
                &coverage_snapshot,
                &b"coverage".to_vec(),
                &vec![0xa3_u8; 32],
                &scan,
                &scan_hash,
            ],
        )
        .expect("A completion before injected abort");
    fixture
        .admin
        .batch_execute("ROLLBACK")
        .expect("inject A abort");
    assert_eq!(
        outcome_a(&mut fixture.admin, &execution),
        ("A_RESERVED".to_owned(), "RESERVED".to_owned(), None)
    );
    evidence.observe(("R4-FG-06", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &execution),
            expected_snapshot(
                "A_RESERVED",
                "PROCESSING",
                None,
                Some(("RESERVED", None)),
                None,
                0,
            )
        );
    });

    // R4-FG-13/BASE: exact late completion is accepted after the claimant's lease expires.
    fixture
        .admin
        .query_one(
            "SELECT private.complete_contribution_a_exact(\
             $1,$2,$3,$4,$5,$6,'USABLE',$7,$8,1,$9,$10,$11,$12,'a-trace',\
             'provider-a',NULL,NULL,NULL,NULL)",
            &[
                &execution.tenant,
                &execution.execution,
                &a_call,
                &execution.coverage_request,
                &a_intent,
                &a_disclosure,
                &vec![0xa2_u8; 32],
                &coverage_snapshot,
                &b"coverage".to_vec(),
                &vec![0xa3_u8; 32],
                &scan,
                &scan_hash,
            ],
        )
        .expect("exact late A completion");
    assert_eq!(
        outcome_a(&mut fixture.admin, &execution),
        (
            "READY_B".to_owned(),
            "SUCCEEDED".to_owned(),
            Some(coverage_snapshot)
        )
    );
    evidence.observe(("R4-FG-13", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &execution),
            expected_snapshot(
                "READY_B",
                "PROCESSING",
                None,
                Some(("SUCCEEDED", Some("SUCCESS"))),
                None,
                0,
            )
        );
    });

    // R4-FG-07/BASE: the committed A receipt is sufficient to resume at B without A replay.
    evidence.observe(("R4-FG-07", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &execution),
            expected_snapshot(
                "READY_B",
                "PROCESSING",
                None,
                Some(("SUCCEEDED", Some("SUCCESS"))),
                None,
                0,
            )
        );
    });
    let owner = "r4-worker-after-late-A";
    let attempt = claim(&mut fixture.admin, &execution, owner);

    let b_intent = vec![0xb1; 32];
    let (b_call, b_disclosure) =
        reserve_b(&mut fixture.admin, &execution, owner, attempt, &b_intent);
    // R4-FG-08/BASE: B reservation is independently durable before provider/scanner work.
    assert_eq!(outcome_row(&mut fixture.admin, &execution).0, "B_RESERVED");
    evidence.observe(("R4-FG-08", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &execution),
            expected_snapshot(
                "B_RESERVED",
                "PROCESSING",
                None,
                Some(("SUCCEEDED", Some("SUCCESS"))),
                Some(("RESERVED", None)),
                0,
            )
        );
    });
    let candidate_body = b"deidentified r4 candidate".to_vec();
    let candidate_hash: Vec<u8> = fixture
        .admin
        .query_one("SELECT sha256($1::bytea)", &[&candidate_body])
        .expect("candidate hash")
        .get(0);
    let assessment = b"canonical typed assessment".to_vec();
    let assessment_hash: Vec<u8> = fixture
        .admin
        .query_one("SELECT sha256($1::bytea)", &[&assessment])
        .expect("assessment hash")
        .get(0);

    // R4-FG-11/BASE: a committed B receipt stops at READY_CANDIDATE; candidate settlement
    // resumes from durable facts without any provider/scanner input.
    fixture
        .admin
        .query_one(
            "SELECT private.complete_contribution_b_exact(\
             $1,$2,$3,$4,$5,$6,'READY_CANDIDATE',$7,$8,'PASS','PASS','PASS','PASS',\
             $9,$10,$11,$12,'b-trace','provider-b',NULL,NULL,NULL,NULL)",
            &[
                &execution.tenant,
                &execution.execution,
                &b_call,
                &execution.assessment_request,
                &b_intent,
                &b_disclosure,
                &assessment,
                &assessment_hash,
                &candidate_body,
                &candidate_hash,
                &scan,
                &scan_hash,
            ],
        )
        .expect("exact late B completion");
    let job_status: String = fixture
        .admin
        .query_one(
            "SELECT status FROM ops.jobs WHERE job_id=$1",
            &[&execution.job],
        )
        .expect("job state")
        .get(0);
    assert_eq!(job_status, "PROCESSING");
    evidence.observe(("R4-FG-11", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &execution),
            expected_snapshot(
                "READY_CANDIDATE",
                "PROCESSING",
                None,
                Some(("SUCCEEDED", Some("SUCCESS"))),
                Some(("SUCCEEDED", Some("SUCCESS"))),
                0,
            )
        );
    });

    // R4-FG-16/BASE: the candidate trigger rejects an A-ledger reference even when every
    // other frozen candidate field is copied from the READY_CANDIDATE root.
    assert!(fixture
        .admin
        .execute(
            "INSERT INTO staging.contribution_candidates(\
             candidate_id,tenant_id,user_id,policy_id,policy_version,policy_snapshot,\
             reasoning_domain_id,profile_version,source_manifest_hash,source_count,\
             disclosed_payload,disclosed_payload_sha256,provider_trace,scan_receipt,\
             rights_basis,source_license,publisher,contributor_attestation,redistribution_policy,\
             binding_id,binding_version,model_call_id,contribution_execution_id) \
             SELECT e.candidate_id,e.tenant_id,e.user_id,e.policy_id,e.policy_version,e.policy_snapshot,\
             e.reasoning_domain_id,l.profile_version,e.input_manifest_hash,e.source_count,\
             e.candidate_body,e.candidate_sha256,e.assessment_provider_trace,e.candidate_scan_receipt,\
             e.rights_basis,e.source_license,e.publisher,e.contributor_attestation,e.redistribution_policy,\
             e.binding_id,e.binding_version,e.coverage_model_call_id,e.execution_id \
             FROM private.contribution_executions e \
             JOIN ops.model_call_ledger l ON l.model_call_id=e.assessment_model_call_id \
             WHERE e.execution_id=$1",
            &[&execution.execution],
        )
        .is_err());
    evidence.observe(("R4-FG-16", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &execution),
            expected_snapshot(
                "READY_CANDIDATE",
                "PROCESSING",
                None,
                Some(("SUCCEEDED", Some("SUCCESS"))),
                Some(("SUCCEEDED", Some("SUCCESS"))),
                0,
            )
        );
    });

    // R4-FG-12/BASE: an injected candidate transaction abort leaves candidate, execution,
    // and job unsettled together.
    fixture
        .admin
        .batch_execute("BEGIN")
        .expect("begin candidate abort");
    fixture
        .admin
        .query_one(
            "SELECT private.commit_contribution_candidate($1,$2,$3,$4,$5)",
            &[
                &execution.tenant,
                &execution.execution,
                &execution.job,
                &owner,
                &attempt,
            ],
        )
        .expect("candidate settlement before injected abort");
    fixture
        .admin
        .batch_execute("ROLLBACK")
        .expect("inject candidate abort");
    let after_candidate_abort = fixture
        .admin
        .query_one(
            "SELECT e.state::text,j.status,(SELECT count(*) FROM staging.contribution_candidates c WHERE c.contribution_execution_id=e.execution_id) \
             FROM private.contribution_executions e \
             JOIN ops.contribution_execution_job_links link USING(execution_id) \
             JOIN ops.jobs j USING(job_id) WHERE e.execution_id=$1",
            &[&execution.execution],
        )
        .expect("candidate abort durable observation");
    assert_eq!(after_candidate_abort.get::<_, String>(0), "READY_CANDIDATE");
    assert_eq!(after_candidate_abort.get::<_, String>(1), "PROCESSING");
    assert_eq!(after_candidate_abort.get::<_, i64>(2), 0);
    evidence.observe(("R4-FG-12", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &execution),
            expected_snapshot(
                "READY_CANDIDATE",
                "PROCESSING",
                None,
                Some(("SUCCEEDED", Some("SUCCESS"))),
                Some(("SUCCEEDED", Some("SUCCESS"))),
                0,
            )
        );
    });

    // R4-FG-17/BASE late-candidate branch: candidate + execution DONE + job DONE commit as
    // one fresh-lease settlement after the late B receipt left the job PROCESSING.
    let committed: Uuid = fixture
        .admin
        .query_one(
            "SELECT private.commit_contribution_candidate($1,$2,$3,$4,$5)",
            &[
                &execution.tenant,
                &execution.execution,
                &execution.job,
                &owner,
                &attempt,
            ],
        )
        .expect("atomic candidate commit")
        .get(0);
    assert_eq!(committed, execution.candidate);
    let terminal = fixture
        .admin
        .query_one(
            "SELECT e.state::text,j.status,(SELECT count(*) FROM staging.contribution_candidates c WHERE c.contribution_execution_id=e.execution_id) FROM private.contribution_executions e JOIN ops.contribution_execution_job_links link USING(execution_id) JOIN ops.jobs j USING(job_id) WHERE e.execution_id=$1",
            &[&execution.execution],
        )
        .expect("atomic terminal observation");
    assert_eq!(terminal.get::<_, String>(0), "DONE");
    assert_eq!(terminal.get::<_, String>(1), "DONE");
    assert_eq!(terminal.get::<_, i64>(2), 1);
    evidence.observe(("R4-FG-17", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &execution),
            expected_snapshot(
                "DONE",
                "DONE",
                None,
                Some(("SUCCEEDED", Some("SUCCESS"))),
                Some(("SUCCEEDED", Some("SUCCESS"))),
                1,
            )
        );
    });

    // R4-FG-07/BASE: direct legal-looking mutation is unavailable to runtime.
    let mut worker = Client::connect(
        &format!("{}?options=-c%20role%3Drole_private_worker", dsn()),
        NoTls,
    )
    .expect("private worker role");
    worker
        .query_one(
            "SELECT set_config('humaux.tenant_id',$1,false)",
            &[&execution.tenant.to_string()],
        )
        .expect("worker tenant");
    assert!(worker
        .execute(
            "UPDATE private.contribution_executions SET state='FAILED_TERMINAL' WHERE execution_id=$1",
            &[&execution.execution],
        )
        .is_err());
    evidence.finish();
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL 18 migrated through 0131"]
#[allow(clippy::too_many_lines)] // The four frozen outcome variants share one isolated fixture and assertion vocabulary.
fn r4_fg_18_business_outcome_variants_and_abort_invariants() {
    let mut fixture = ContributionFixture::new();
    let mut evidence = SqlDurableEvidence::new(SQL_OUTCOME_FAULT_KEYS);
    let owner = "r4-outcome-worker";
    let assessment = b"canonical typed assessment".to_vec();
    let assessment_hash: Vec<u8> = fixture
        .admin
        .query_one("SELECT sha256($1::bytea)", &[&assessment])
        .expect("assessment hash")
        .get(0);

    // R4-FG-18/B_GATE_FAIL: a normal no-candidate result succeeds its provider receipt
    // and atomically completes the live job.
    let gate_fail = seed_execution(&mut fixture, "gate-fail");
    let b = advance_to_b_reserved(&mut fixture, &gate_fail, owner);
    fixture
        .admin
        .query_one(
            "SELECT private.complete_contribution_b_exact(\
             $1,$2,$3,$4,$5,$6,'NOT_CONTRIBUTABLE',$7,$8,'FAIL','PASS','PASS','PASS',\
             NULL,NULL,NULL,NULL,'b-trace','provider-b',NULL,$9,$10,$11)",
            &[
                &gate_fail.tenant,
                &gate_fail.execution,
                &b.call,
                &gate_fail.assessment_request,
                &b.intent,
                &b.disclosure,
                &assessment,
                &assessment_hash,
                &gate_fail.job,
                &owner,
                &b.attempt,
            ],
        )
        .expect("B gate fail bundle");
    assert_eq!(
        outcome_row(&mut fixture.admin, &gate_fail),
        (
            "NOT_CONTRIBUTABLE".to_owned(),
            "SUCCEEDED".to_owned(),
            "DONE".to_owned(),
            None,
        )
    );
    assert_eq!(candidate_count(&mut fixture.admin, &gate_fail), 0);
    evidence.observe(("R4-FG-18", "B_GATE_FAIL"), |db| {
        assert_eq!(
            durable_snapshot(db, &gate_fail),
            expected_snapshot(
                "NOT_CONTRIBUTABLE",
                "DONE",
                None,
                Some(("SUCCEEDED", Some("SUCCESS"))),
                Some(("SUCCEEDED", Some("SUCCESS"))),
                0,
            )
        );
    });

    // R4-FG-18/SCAN_REJECT: scanner rejection is also a successful provider receipt.
    let scan_reject = seed_execution(&mut fixture, "scan-reject");
    let b = advance_to_b_reserved(&mut fixture, &scan_reject, owner);
    let candidate = b"rejected candidate".to_vec();
    let candidate_hash: Vec<u8> = fixture
        .admin
        .query_one("SELECT sha256($1::bytea)", &[&candidate])
        .expect("candidate hash")
        .get(0);
    let receipt = scanner_receipt();
    let receipt_digest = receipt_hash(&mut fixture.admin, &receipt);
    fixture
        .admin
        .query_one(
            "SELECT private.complete_contribution_b_exact(\
             $1,$2,$3,$4,$5,$6,'REJECTED_SAFETY',$7,$8,'PASS','PASS','PASS','PASS',\
             $9,$10,$11,$12,'b-trace','provider-b',NULL,$13,$14,$15)",
            &[
                &scan_reject.tenant,
                &scan_reject.execution,
                &b.call,
                &scan_reject.assessment_request,
                &b.intent,
                &b.disclosure,
                &assessment,
                &assessment_hash,
                &candidate,
                &candidate_hash,
                &receipt,
                &receipt_digest,
                &scan_reject.job,
                &owner,
                &b.attempt,
            ],
        )
        .expect("B scan rejection bundle");
    assert_eq!(
        outcome_row(&mut fixture.admin, &scan_reject),
        (
            "REJECTED_SAFETY".to_owned(),
            "SUCCEEDED".to_owned(),
            "DONE".to_owned(),
            None,
        )
    );
    assert_eq!(candidate_count(&mut fixture.admin, &scan_reject), 0);
    evidence.observe(("R4-FG-18", "SCAN_REJECT"), |db| {
        assert_eq!(
            durable_snapshot(db, &scan_reject),
            expected_snapshot(
                "REJECTED_SAFETY",
                "DONE",
                None,
                Some(("SUCCEEDED", Some("SUCCESS"))),
                Some(("SUCCEEDED", Some("SUCCESS"))),
                0,
            )
        );
    });

    // R4-FG-18/PROVIDER_DEFINITE_FAILURE: only a definite failure maps all three
    // ledger/disclosure/job authorities to FAILED.
    let provider_fail = seed_execution(&mut fixture, "provider-fail");
    let b = advance_to_b_reserved(&mut fixture, &provider_fail, owner);
    fixture
        .admin
        .query_one(
            "SELECT private.complete_contribution_b_exact(\
             $1,$2,$3,$4,$5,$6,'FAILED_TERMINAL',NULL,NULL,NULL,NULL,NULL,NULL,\
             NULL,NULL,NULL,NULL,NULL,'provider-b','PROVIDER_DEFINITE',$7,$8,$9)",
            &[
                &provider_fail.tenant,
                &provider_fail.execution,
                &b.call,
                &provider_fail.assessment_request,
                &b.intent,
                &b.disclosure,
                &provider_fail.job,
                &owner,
                &b.attempt,
            ],
        )
        .expect("definite provider failure bundle");
    assert_eq!(
        outcome_row(&mut fixture.admin, &provider_fail),
        (
            "FAILED_TERMINAL".to_owned(),
            "FAILED".to_owned(),
            "FAILED".to_owned(),
            Some("PROVIDER_DEFINITE".to_owned()),
        )
    );
    assert_eq!(candidate_count(&mut fixture.admin, &provider_fail), 0);
    evidence.observe(("R4-FG-18", "PROVIDER_DEFINITE_FAILURE"), |db| {
        assert_eq!(
            durable_snapshot(db, &provider_fail),
            expected_snapshot(
                "FAILED_TERMINAL",
                "FAILED",
                Some("PROVIDER_DEFINITE"),
                Some(("SUCCEEDED", Some("SUCCESS"))),
                Some(("FAILED", Some("FAILED"))),
                0,
            )
        );
    });

    // R4-FG-09/BASE: an unknown scanner outcome invokes no completion command, so the
    // separately committed B reservation remains the only durable business fact.
    let scanner_unknown = seed_execution(&mut fixture, "scanner-unknown");
    let _b = advance_to_b_reserved(&mut fixture, &scanner_unknown, owner);
    evidence.observe(("R4-FG-09", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &scanner_unknown),
            expected_snapshot(
                "B_RESERVED",
                "PROCESSING",
                None,
                Some(("SUCCEEDED", Some("SUCCESS"))),
                Some(("RESERVED", None)),
                0,
            )
        );
    });

    // R4-FG-10/BASE: aborting an exact B completion leaves the full reservation.
    let b_abort = seed_execution(&mut fixture, "b-completion-abort");
    let b = advance_to_b_reserved(&mut fixture, &b_abort, owner);
    fixture
        .admin
        .batch_execute("BEGIN")
        .expect("begin fault transaction");
    fixture
        .admin
        .query_one(
            "SELECT private.complete_contribution_b_exact(\
             $1,$2,$3,$4,$5,$6,'NOT_CONTRIBUTABLE',$7,$8,'FAIL','PASS','PASS','PASS',\
             NULL,NULL,NULL,NULL,'b-trace','provider-b',NULL,NULL,NULL,NULL)",
            &[
                &b_abort.tenant,
                &b_abort.execution,
                &b.call,
                &b_abort.assessment_request,
                &b.intent,
                &b.disclosure,
                &assessment,
                &assessment_hash,
            ],
        )
        .expect("completion before injected abort");
    fixture
        .admin
        .batch_execute("ROLLBACK")
        .expect("inject abort");
    let after_abort = outcome_row(&mut fixture.admin, &b_abort);
    assert_eq!(after_abort.0, "B_RESERVED");
    assert_eq!(after_abort.1, "RESERVED");
    assert_eq!(after_abort.2, "PROCESSING");
    assert_eq!(candidate_count(&mut fixture.admin, &b_abort), 0);
    evidence.observe(("R4-FG-10", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &b_abort),
            expected_snapshot(
                "B_RESERVED",
                "PROCESSING",
                None,
                Some(("SUCCEEDED", Some("SUCCESS"))),
                Some(("RESERVED", None)),
                0,
            )
        );
    });

    // R4-FG-15/BASE: a fresh claimant observing the reservation can only record the
    // reconciliation-required job disposition; the execution and call stay reserved.
    // R4-FG-18/TIMEOUT shares that durable disposition after the B attempt becomes
    // MAY_HAVE_REACHED; the runner-owned case separately proves zero redispatch.
    let timeout = seed_execution(&mut fixture, "timeout");
    let _b = advance_to_b_reserved(&mut fixture, &timeout, owner);
    let reconciliation_owner = "r4-reconciliation-worker";
    let reconciliation_attempt = claim(&mut fixture.admin, &timeout, reconciliation_owner);
    fixture
        .admin
        .query_one(
            "SELECT private.mark_contribution_reconciliation_required($1,$2,$3,$4,$5)",
            &[
                &timeout.tenant,
                &timeout.execution,
                &timeout.job,
                &reconciliation_owner,
                &reconciliation_attempt,
            ],
        )
        .expect("fresh reserved claimant disposition");
    assert_eq!(
        outcome_row(&mut fixture.admin, &timeout),
        (
            "B_RESERVED".to_owned(),
            "RESERVED".to_owned(),
            "FAILED".to_owned(),
            Some("RECONCILIATION_REQUIRED".to_owned()),
        )
    );
    assert_eq!(candidate_count(&mut fixture.admin, &timeout), 0);
    evidence.observe(("R4-FG-15", "BASE"), |db| {
        assert_eq!(
            durable_snapshot(db, &timeout),
            expected_snapshot(
                "B_RESERVED",
                "FAILED",
                Some("RECONCILIATION_REQUIRED"),
                Some(("SUCCEEDED", Some("SUCCESS"))),
                Some(("RESERVED", None)),
                0,
            )
        );
    });
    evidence.observe(("R4-FG-18", "TIMEOUT"), |db| {
        assert_eq!(
            durable_snapshot(db, &timeout),
            expected_snapshot(
                "B_RESERVED",
                "FAILED",
                Some("RECONCILIATION_REQUIRED"),
                Some(("SUCCEEDED", Some("SUCCESS"))),
                Some(("RESERVED", None)),
                0,
            )
        );
    });
    evidence.finish();
}
