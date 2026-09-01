//! §1.14 actual database reads, least-privilege admin and real reviewed-public execution.
//! Seeded observations test the verifier; the separate review test exercises the writer.

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use contribution_fixture::{ContributionFixture, OfflineReasoner};
use humaux_adapters::{
    contribution_entry_repo::ContributionEntryRepo,
    mechanism_observation::{PublicObservationRecorder, read_target},
    postgres::{AdminDbPool, MaintenanceDbPool, PublicWorkerDbPool},
    public_repo::{self, EvaluateClaim},
};
use humaux_application::contribute;
use humaux_contracts::mechanism_registry::{
    MechanismSpec, MechanismStatus, ObservationTarget, parse_registry,
};
use humaux_domain::public::ModerationState;
use humaux_infra_cell::{CallerId, CellId, IntraCellResourceRegistry};
use postgres::{Client, NoTls};
use std::{path::Path, process::Command, sync::Mutex};
use uuid::Uuid;

static SERIAL: Mutex<()> = Mutex::new(());
const SPEC: &str = include_str!("../../../docs/architecture/Baseline_2.9.md");

fn db() -> Client {
    assert_eq!(
        std::env::var("HUMAUX_MECHANISM_FIXTURE").as_deref(),
        Ok("humaux_thread_stable_observations"),
        "this suite only writes the dedicated mechanism fixture"
    );
    Client::connect(
        &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG DSN"),
        NoTls,
    )
    .expect("isolated PG")
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

fn target() -> ObservationTarget {
    ObservationTarget {
        deployment_id: Uuid::new_v4().to_string(),
        cell_id: Uuid::new_v4().to_string(),
    }
}

fn registry(cell_id: Uuid) -> IntraCellResourceRegistry {
    IntraCellResourceRegistry::new(
        std::collections::BTreeMap::new(),
        CellId(cell_id),
        CallerId("mechanism-fixture".into()),
    )
}

fn spec(ch: u32) -> MechanismSpec {
    parse_registry(SPEC)
        .expect("canonical registry")
        .into_iter()
        .find(|s| s.ch == ch)
        .expect("chapter")
}

fn admin_pool(rt: &tokio::runtime::Runtime) -> AdminDbPool {
    rt.block_on(AdminDbPool::connect(
        &std::env::var("HUMAUX_ADMIN_PG_DSN").expect("real read-only credentials"),
    ))
    .expect("read-only admin role")
}

fn insert_observation(
    client: &mut Client,
    target: &ObservationTarget,
    spec: &MechanismSpec,
    value: i64,
    scanned_n: Option<i64>,
    age_seconds: i64,
    status: &str,
) -> Uuid {
    client.query_one("INSERT INTO ops.mechanism_observations(deployment_id,cell_id,mechanism_id,value,scanned_n,measured_at,derived_status,probe_version,binary_build,scope_hash) \
        VALUES($1,$2,$3,$4,$5,clock_timestamp()-($6::bigint)*interval '1 second',$7,'fixture-probe@1','fixture-build',$8) RETURNING observation_id",
        &[&Uuid::parse_str(&target.deployment_id).unwrap(), &Uuid::parse_str(&target.cell_id).unwrap(), &spec.id(),
          &value,&scanned_n,&age_seconds,&status,&format!("sha256:{}","a".repeat(64))]).expect("fixture observation").get(0)
}

fn link_run(client: &mut Client, before: Uuid, after: Uuid) -> Uuid {
    client.query_one("INSERT INTO ops.mechanism_e2e_runs(deployment_id,cell_id,mechanism_id,before_observation_id,after_observation_id,scope_hash,probe_version,binary_build,started_at,completed_at) \
       SELECT a.deployment_id,a.cell_id,a.mechanism_id,b.observation_id,a.observation_id,a.scope_hash,a.probe_version,a.binary_build, \
         b.measured_at-interval '1 second',clock_timestamp() FROM ops.mechanism_observations b,ops.mechanism_observations a \
       WHERE b.observation_id=$1 AND a.observation_id=$2 RETURNING run_id", &[&before,&after]).expect("fixture paired run").get(0)
}

fn assert_admin_cli_status(
    target: &ObservationTarget,
    actual: &humaux_adapters::mechanism_observation::RuntimeObservations,
    expected_active: bool,
) {
    let binary = std::env::var("HUMAUX_MECHANISM_ADMIN_BIN")
        .expect("HUMAUX_MECHANISM_ADMIN_BIN is required for the real CLI test");
    assert!(
        Path::new(&binary).is_file(),
        "HUMAUX_MECHANISM_ADMIN_BIN must name an existing binary"
    );
    let admin_dsn = std::env::var("HUMAUX_ADMIN_PG_DSN")
        .expect("real CLI test requires the existing role_admin DSN");
    let output = Command::new(binary)
        .env_clear()
        .env("HUMAUX_ADMIN_PG_DSN", admin_dsn)
        .args([
            "mechanism",
            "status",
            "--deployment",
            &target.deployment_id,
            "--cell",
            &target.cell_id,
        ])
        .output()
        .expect("run actual humaux-admin mechanism status");
    assert_eq!(
        output.status.code(),
        Some(1),
        "other unobserved mechanisms keep the aggregate command nonzero"
    );
    let rendered: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("CLI must return its real JSON status");
    assert_eq!(
        rendered["deployment_id"].as_str(),
        Some(target.deployment_id.as_str())
    );
    assert_eq!(rendered["cell_id"].as_str(), Some(target.cell_id.as_str()));
    for ch in [12, 21] {
        let id = spec(ch).id();
        let row = rendered["mechanisms"]
            .as_array()
            .and_then(|rows| {
                rows.iter()
                    .find(|row| row["mechanism_id"].as_str() == Some(id.as_str()))
            })
            .expect("CLI must render the reviewed mechanism");
        let observed_id = actual.latest[&id].observation_id.to_string();
        assert_eq!(
            row["observation"]["observation_id"].as_str(),
            Some(observed_id.as_str())
        );
        if expected_active {
            assert_eq!(row["status"].as_str(), Some("ACTIVE"));
        } else {
            assert_ne!(row["status"].as_str(), Some("ACTIVE"));
        }
    }
    println!("{}", String::from_utf8_lossy(&output.stdout));
}

#[test]
#[ignore = "requires dedicated PostgreSQL78 mechanism fixture and real read-only credentials"]
fn runtime_reader_rejects_forged_active_unlinked_delta_and_cross_target_fallback() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut client = db();
    let rt = runtime();
    let reader = admin_pool(&rt);
    let target = target();
    let spec = spec(12);
    let empty = rt
        .block_on(read_target(&reader, &target))
        .expect("actual empty read");
    assert_eq!(empty.status(&spec).reason, "no_data");
    let before = insert_observation(
        &mut client,
        &target,
        &spec,
        0,
        Some(4),
        5,
        "NOT_APPLICABLE_YET",
    );
    let after = insert_observation(&mut client, &target, &spec, 1, Some(4), 1, "ACTIVE");
    let unlinked = rt.block_on(read_target(&reader, &target)).unwrap();
    assert_eq!(
        unlinked.status(&spec).reason,
        "no_e2e_evidence",
        "stored ACTIVE + adjacent delta is not a run"
    );
    link_run(&mut client, before, after);
    let linked = rt.block_on(read_target(&reader, &target)).unwrap();
    assert_eq!(linked.status(&spec).status, Some(MechanismStatus::Active));
    for wrong in [
        ObservationTarget {
            cell_id: Uuid::new_v4().to_string(),
            ..target.clone()
        },
        ObservationTarget {
            deployment_id: Uuid::new_v4().to_string(),
            ..target.clone()
        },
    ] {
        let absent = rt.block_on(read_target(&reader, &wrong)).unwrap();
        assert!(absent.latest.is_empty());
        assert_eq!(absent.status(&spec).status, Some(MechanismStatus::Stale));
    }
    // The latest failed attempt dominates the older successful receipt.
    insert_observation(&mut client, &target, &spec, 1, Some(4), 0, "ACTIVE");
    assert_eq!(
        rt.block_on(read_target(&reader, &target))
            .unwrap()
            .status(&spec)
            .reason,
        "no_e2e_evidence"
    );
}

#[test]
#[ignore = "requires dedicated PostgreSQL78 mechanism fixture"]
fn real_observation_freshness_and_denominator_never_use_bootstrap() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut client = db();
    let rt = runtime();
    let reader = admin_pool(&rt);
    let mut spec = spec(12);
    spec.bootstrap_value = "999".into();
    for (value, scanned, age, expected) in [
        (0, Some(4), 0, MechanismStatus::NotApplicableYet),
        (0, Some(0), 0, MechanismStatus::Stale),
        (1, None, 0, MechanismStatus::Stale),
        (1, Some(4), 91 * 24 * 3600, MechanismStatus::Stale),
        (1, Some(4), -60, MechanismStatus::Stale),
    ] {
        let target = target();
        insert_observation(&mut client, &target, &spec, value, scanned, age, "ACTIVE");
        let read = rt.block_on(read_target(&reader, &target)).unwrap();
        assert_eq!(read.status(&spec).status, Some(expected));
    }
    let target = target();
    let before = insert_observation(&mut client, &target, &spec, 1, Some(4), 5, "ACTIVE");
    let after = insert_observation(&mut client, &target, &spec, 1, Some(4), 1, "ACTIVE");
    link_run(&mut client, before, after);
    assert_eq!(
        rt.block_on(read_target(&reader, &target))
            .unwrap()
            .status(&spec)
            .reason,
        "no_positive_e2e_delta"
    );
}

#[test]
#[ignore = "requires dedicated PostgreSQL78 mechanism fixture"]
fn e2e_receipts_reject_cross_scope_and_preserve_immutable_measurements() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut client = db();
    let target = target();
    let spec = spec(12);
    let before = insert_observation(&mut client, &target, &spec, 0, Some(4), 5, "STALE");
    let after = insert_observation(&mut client, &target, &spec, 1, Some(4), 1, "ACTIVE");
    let wrong_cell = Uuid::new_v4();
    let bad=client.execute("INSERT INTO ops.mechanism_e2e_runs(deployment_id,cell_id,mechanism_id,before_observation_id,after_observation_id,scope_hash,probe_version,binary_build,started_at,completed_at) \
       SELECT a.deployment_id,$3,a.mechanism_id,b.observation_id,a.observation_id,a.scope_hash,a.probe_version,a.binary_build, \
         b.measured_at-interval '1 second',clock_timestamp() FROM ops.mechanism_observations b,ops.mechanism_observations a \
       WHERE b.observation_id=$1 AND a.observation_id=$2", &[&before,&after,&wrong_cell]);
    assert!(
        bad.is_err(),
        "cross-cell run must be rejected by actual database trigger"
    );
    let run = link_run(&mut client, before, after);
    assert!(
        client
            .execute(
                "UPDATE ops.mechanism_observations SET value=999 WHERE observation_id=$1",
                &[&after]
            )
            .is_err()
    );
    assert!(
        client
            .execute(
                "DELETE FROM ops.mechanism_e2e_runs WHERE run_id=$1",
                &[&run]
            )
            .is_err()
    );
    assert!(
        client
            .batch_execute("TRUNCATE ops.mechanism_e2e_runs")
            .is_err()
    );
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM ops.mechanism_e2e_runs WHERE run_id=$1",
                &[&run]
            )
            .unwrap()
            .get::<_, i64>(0),
        1
    );
}

#[test]
#[ignore = "requires actual role_admin and role_maintenance login credentials"]
fn readonly_admin_is_not_a_writer_or_private_reader() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let _fixture = db();
    let mut admin = Client::connect(&std::env::var("HUMAUX_ADMIN_PG_DSN").unwrap(), NoTls)
        .expect("real admin login");
    let role = admin
        .query_one("SELECT current_user::text,session_user::text", &[])
        .unwrap();
    assert_eq!(role.get::<_, String>(0), "role_admin");
    assert_eq!(role.get::<_, String>(1), "role_admin");
    admin
        .query(
            "SELECT observation_id FROM ops.mechanism_observations LIMIT 1",
            &[],
        )
        .expect("allowed SELECT");
    for query in [
        "UPDATE ops.mechanism_observations SET value=value",
        "DELETE FROM ops.mechanism_e2e_runs",
        "SELECT * FROM private.memory_records",
        "SELECT * FROM public.claims",
        "SET ROLE role_maintenance",
    ] {
        assert!(
            admin.batch_execute(query).is_err(),
            "admin must not acquire writer/private capability"
        );
    }
    let allowed:bool=admin.query_one("SELECT has_function_privilege(current_user,'ops.audit_batch_insert(bigint,bigint,bytea,bytea,text)','EXECUTE')",&[]).unwrap().get(0);
    assert!(
        !allowed,
        "read-only role must not write through SECURITY DEFINER"
    );
    let rt = runtime();
    assert!(
        rt.block_on(AdminDbPool::connect(
            &std::env::var("HUMAUX_MAINTENANCE_PG_DSN").unwrap()
        ))
        .is_err()
    );
}

#[allow(clippy::too_many_lines)] // one controlled experiment: authenticated releases, review, actual scans and durable receipts.
fn exercise_real_public_review_records_before_after_and_quarantine(verify_cli: bool) {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let _fixture = db();
    let mut fixture = ContributionFixture::new();
    let super_dsn = std::env::var("HUMAUX_TEST_PG_DSN").unwrap();
    let public = fixture
        .rt
        .block_on(PublicWorkerDbPool::connect(&format!(
            "{super_dsn}?options=-c%20role%3Drole_public_worker"
        )))
        .unwrap();
    let writer = fixture
        .rt
        .block_on(MaintenanceDbPool::connect(
            &std::env::var("HUMAUX_MAINTENANCE_PG_DSN").unwrap(),
        ))
        .unwrap();
    let reader = admin_pool(&fixture.rt);
    let target = target();
    let mut admitted = Vec::new();
    let resources = registry(Uuid::parse_str(&target.cell_id).unwrap());
    let recorder = fixture
        .rt
        .block_on(PublicObservationRecorder::new(
            &writer,
            &public,
            &resources,
            &resources,
            Uuid::parse_str(&target.deployment_id).unwrap(),
            "fixture-immutable-build",
        ))
        .expect("bind actual pools and trusted fixture configuration");
    assert_eq!(recorder.target(), &target);
    // Alphabetic fixture identity: a raw random UUID can contain seven adjacent
    // digits and correctly trip the real privacy scanner's phone-number rule.
    let run_tag: String = Uuid::new_v4()
        .as_bytes()
        .iter()
        .flat_map(|byte| {
            [
                char::from(b'a' + (byte >> 4)),
                char::from(b'a' + (byte & 15)),
            ]
        })
        .collect();
    // Independent identities here are explicit test fixtures, not a live identity-verification claim.
    // Each release still goes through real prepare, scanner, confirmation and admission.
    for n in 0..4 {
        #[allow(
            deprecated,
            reason = "this acceptance fixture intentionally exercises legacy contribute::prepare"
        )]
        let candidate=fixture.rt.block_on(contribute::prepare(fixture.request(),
            &OfflineReasoner(format!("Public test principle {run_tag} number {n}: independently review source evidence.").into_bytes()),
            &fixture.scanner(),&ContributionEntryRepo::new(&fixture.private))).expect("actual prepare");
        let release = fixture
            .rt
            .block_on(contribute::finalize(
                fixture.confirm(candidate),
                &ContributionEntryRepo::new(&fixture.private),
            ))
            .unwrap()
            .0;
        let admitted_release = fixture
            .rt
            .block_on(public_repo::admit_release(
                &public,
                fixture.auth.tenant_id(),
                release,
            ))
            .unwrap();
        fixture.admin.execute("UPDATE public.sources SET verified_organization=$2,identity_policy_version='identity-fixture-v1' WHERE source_id=$1",
            &[&admitted_release.source_id,&format!("fixture-organization-{n}-{}",Uuid::new_v4())]).unwrap();
        admitted.push(admitted_release);
    }
    let claim = admitted[0].claim_id;
    for root in &admitted[1..] {
        fixture
            .admin
            .execute(
                "INSERT INTO public.provenance_edges(claim_id,source_id) VALUES($1,$2)",
                &[&claim, &root.source_id],
            )
            .unwrap();
        fixture.admin.execute("INSERT INTO public.source_closure(claim_id,root_source_id,depth,is_current) VALUES($1,$2,1,true)",&[&claim,&root.source_id]).unwrap();
    }
    fixture.admin.execute("INSERT INTO control.public_moderator_grants(user_id,grant_version,enabled) VALUES($1,1,true)",
        &[&fixture.auth.user_id().unwrap().0]).unwrap();
    let body: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to(content::text,'UTF8')) FROM public.claims WHERE claim_id=$1",
            &[&claim],
        )
        .unwrap()
        .get(0);
    let input = EvaluateClaim {
        claim_id: claim,
        expected_revision: admitted[0].object_revision,
        expected_body_sha256: &body,
        policy_version: "mechanism-review-fixture-v1",
        rationale: "controlled real review and observed counts",
        target_state: ModerationState::Supported,
    };
    let reviewed = fixture
        .rt
        .block_on(recorder.review(&fixture.auth, &input))
        .expect("real review with scans");
    let actual = fixture
        .rt
        .block_on(read_target(&reader, &target))
        .expect("actual receipts read by readonly role");
    for ch in [12, 21] {
        let spec = spec(ch);
        assert_eq!(actual.status(&spec).status, Some(MechanismStatus::Active));
        let proof = &actual.evidence[&spec.id()];
        let latest = &actual.latest[&spec.id()];
        assert_eq!(
            latest.scope_hash.as_deref(),
            Some(proof.scope_hash.as_str())
        );
        assert_eq!(latest.value - proof.before.value, 1);
        assert!(latest.scanned_n.unwrap() >= 4);
    }
    if verify_cli {
        assert_admin_cli_status(&target, &actual, true);
    }
    // A real quarantine review has no positive delta: it cannot reuse the older
    // successful receipt to claim that this execution was ACTIVE.
    let quarantine = EvaluateClaim {
        expected_revision: reviewed.object_revision,
        target_state: ModerationState::Quarantined,
        ..input
    };
    fixture
        .rt
        .block_on(recorder.review(&fixture.auth, &quarantine))
        .unwrap();
    let stopped = fixture.rt.block_on(read_target(&reader, &target)).unwrap();
    for ch in [12, 21] {
        assert_ne!(
            stopped.status(&spec(ch)).status,
            Some(MechanismStatus::Active)
        );
    }
    if verify_cli {
        assert_admin_cli_status(&target, &stopped, false);
    }
}

#[test]
#[ignore = "requires PostgreSQL78, actual maintenance/admin credentials and pinned Gitleaks"]
fn real_public_review_records_before_after_and_quarantine_remains_visible() {
    exercise_real_public_review_records_before_after_and_quarantine(false);
}

#[test]
#[ignore = "requires PostgreSQL78, real admin binary and actual maintenance/admin credentials"]
fn admin_cli_reports_real_supported_then_quarantined_review() {
    exercise_real_public_review_records_before_after_and_quarantine(true);
}

#[test]
#[ignore = "requires dedicated PostgreSQL78 fixture and real maintenance credentials"]
fn recorder_rejects_other_cell_or_database_before_any_observation() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut admin = db();
    let rt = runtime();
    let target = target();
    let super_dsn = std::env::var("HUMAUX_TEST_PG_DSN").unwrap();
    let public = rt
        .block_on(PublicWorkerDbPool::connect(&format!(
            "{super_dsn}?options=-c%20role%3Drole_public_worker"
        )))
        .unwrap();
    let writer = rt
        .block_on(MaintenanceDbPool::connect(
            &std::env::var("HUMAUX_MAINTENANCE_PG_DSN").unwrap(),
        ))
        .unwrap();
    let local = registry(Uuid::parse_str(&target.cell_id).unwrap());
    let other = registry(Uuid::new_v4());
    let deployment = Uuid::parse_str(&target.deployment_id).unwrap();
    assert!(
        rt.block_on(PublicObservationRecorder::new(
            &writer,
            &public,
            &local,
            &other,
            deployment,
            "fixture-build"
        ))
        .is_err()
    );
    // This second database is on the same disposable cluster; only its connection
    // identity is queried. No schema or data is written to it.
    let base = super_dsn.rsplit_once('/').expect("validated fixture URI").0;
    let wrong_database = rt
        .block_on(PublicWorkerDbPool::connect(&format!(
            "{base}/postgres?options=-c%20role%3Drole_public_worker"
        )))
        .unwrap();
    assert!(
        rt.block_on(PublicObservationRecorder::new(
            &writer,
            &wrong_database,
            &local,
            &local,
            deployment,
            "fixture-build"
        ))
        .is_err()
    );
    assert!(
        rt.block_on(PublicObservationRecorder::new(
            &writer,
            &public,
            &local,
            &local,
            Uuid::nil(),
            "fixture-build"
        ))
        .is_err()
    );
    let rows: i64 = admin
        .query_one(
            "SELECT count(*) FROM ops.mechanism_observations WHERE deployment_id=$1",
            &[&deployment],
        )
        .unwrap()
        .get(0);
    assert_eq!(rows, 0);
}
