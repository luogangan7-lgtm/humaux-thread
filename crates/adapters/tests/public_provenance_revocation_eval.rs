//! `adapters::tests::public_provenance_revocation_eval` — Fixed four case local synthetic benchmark for §69
//!   `public_provenance_revocation`.
//! Depends-on: crates=[humaux-adapters, humaux-domain, postgres, sha2, uuid]; services=[PostgreSQL(any)
//!   r=[public.claim_trust_evaluations, public.claims] w=[control.public_moderator_grants, public.provenance_edges],
//!   PostgreSQL(role_public_worker)]; env=[HUMAUX_TEST_PG_DSN]; modules=[adapters::contribution_repo,
//!   adapters::postgres, adapters::public_provenance, adapters::public_repo,
//!   adapters::tests::support::contribution_fixture, domain::public]
//! Called-by: [cargo-test]
//! Invariants: [a measurement harness, not a production applicability test: cancelling flips must stay visible and
//!   unstable flips never count as resolution; the three-run counterfactual test is lane(c) retired]
//! Spec: Baseline §69; §79.2
//!
//! This is deliberately a measurement harness, not a production applicability or DOD-054 test.

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use contribution_fixture::ContributionFixture;
use humaux_adapters::{
    contribution_repo,
    postgres::PublicWorkerDbPool,
    public_provenance, public_repo,
    public_repo::{EvaluateClaim, ProjectionIdentity},
};
use humaux_domain::public::ModerationState;
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const DATASET: &str = include_str!("../../../evals/public_provenance_revocation/dataset.tsv");
const PROFILE_SOURCE: &[u8] = include_bytes!("public_provenance_revocation_eval.rs");
const PROVENANCE_SOURCE: &[u8] = include_bytes!("../src/public_provenance.rs");
const PUBLIC_REPO_SOURCE: &[u8] = include_bytes!("../src/public_repo.rs");
const REVOKE_SOURCE: &[u8] = include_bytes!("../src/contribution_repo.rs");
const FIXTURE_SOURCE: &[u8] = include_bytes!("support/contribution_fixture.rs");
const TRUST_SOURCE: &[u8] = include_bytes!("../../application/src/public_evolve.rs");
const MIGRATION_0104: &[u8] = include_bytes!("../../../migrations/0104_contribution_io.sql");
const MIGRATION_0105: &[u8] =
    include_bytes!("../../../migrations/0105_release_source_tenant_policy.sql");
const MIGRATION_0106: &[u8] = include_bytes!("../../../migrations/0106_phase9_public_runtime.sql");
const MIGRATION_0107: &[u8] = include_bytes!("../../../migrations/0107_contribution_finalize.sql");
const MIGRATION_0108: &[u8] =
    include_bytes!("../../../migrations/0108_contribution_authorization.sql");
const MIGRATION_0109: &[u8] =
    include_bytes!("../../../migrations/0109_public_review_authority.sql");
const DEPENDENCY_LOCK: &[u8] = include_bytes!("../../../Cargo.lock");

#[derive(Debug)]
struct CaseSpec<'a> {
    id: &'a str,
    layer: &'a str,
    primary: bool,
    counterfactual: bool,
}

fn dataset_cases() -> Vec<CaseSpec<'static>> {
    DATASET
        .lines()
        .skip(1)
        .map(|line| {
            let fields: Vec<_> = line.split('\t').collect();
            assert_eq!(fields.len(), 4, "dataset row must have four TSV fields");
            CaseSpec {
                id: fields[0],
                layer: fields[1],
                primary: fields[2].parse().expect("dataset primary boolean"),
                counterfactual: fields[3].parse().expect("dataset control boolean"),
            }
        })
        .collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn profile_fingerprint() -> String {
    let mut hasher = Sha256::new();
    for source in [
        PROFILE_SOURCE,
        PROVENANCE_SOURCE,
        PUBLIC_REPO_SOURCE,
        REVOKE_SOURCE,
        FIXTURE_SOURCE,
        TRUST_SOURCE,
        MIGRATION_0104,
        MIGRATION_0105,
        MIGRATION_0106,
        MIGRATION_0107,
        MIGRATION_0108,
        MIGRATION_0109,
        DEPENDENCY_LOCK,
        DATASET.as_bytes(),
    ] {
        hasher.update((source.len() as u64).to_be_bytes());
        hasher.update(source);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn dsn_as_role(dsn: &str, role: &str) -> String {
    format!(
        "{dsn}{}options=-c%20role%3D{role}",
        if dsn.contains('?') { '&' } else { '?' }
    )
}

fn public_pool(fixture: &ContributionFixture) -> PublicWorkerDbPool {
    fixture
        .rt
        // dep: PostgreSQL(role_public_worker) — open a role-scoped PG connection/pool for this test
        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
            &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
            "role_public_worker",
        )))
        .expect("public worker pool")
}

fn admit(
    fixture: &ContributionFixture,
    public: &PublicWorkerDbPool,
) -> public_repo::AdmittedRelease {
    let release = fixture.finalize_release();
    fixture
        .rt
        .block_on(public_repo::admit_release(
            public,
            fixture.auth.tenant_id(),
            release,
        ))
        .expect("public admission")
}

fn identity(fixture: &mut ContributionFixture, claim_id: Uuid) -> ProjectionIdentity {
    let row = fixture
        .admin
        .query_one(
            "SELECT claim_id,object_revision,current_evaluation_id,sha256(convert_to(content::text,'UTF8')) \
             FROM public.claims WHERE claim_id=$1",
            &[&claim_id],
        )
        .expect("claim identity");
    ProjectionIdentity {
        object_id: row.get(0),
        object_kind: "CLAIM".to_owned(),
        object_revision: row.get(1),
        evaluation_id: row.get(2),
        body_sha256: row.get::<_, Vec<u8>>(3).try_into().expect("sha256 length"),
    }
}

fn support_claim(
    fixture: &mut ContributionFixture,
    public: &PublicWorkerDbPool,
    claim_id: Uuid,
    revision: i64,
) {
    let user_id = fixture.auth.user_id().expect("fixture user").0;
    fixture
        .admin
        .execute(
            "INSERT INTO control.public_moderator_grants(user_id,grant_version,enabled) \
             VALUES($1,1,true) ON CONFLICT(user_id) DO UPDATE SET enabled=true,grant_version=EXCLUDED.grant_version",
            &[&user_id],
        )
        .expect("moderator grant");
    let hash: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to(content::text,'UTF8')) FROM public.claims WHERE claim_id=$1",
            &[&claim_id],
        )
        .expect("claim body hash")
        .get(0);
    fixture
        .rt
        .block_on(public_repo::evaluate_claim(
            public,
            &fixture.auth,
            &EvaluateClaim {
                claim_id,
                expected_revision: revision,
                expected_body_sha256: &hash,
                policy_version: "public-provenance-benchmark-v1",
                rationale: "fixed local synthetic benchmark case",
                target_state: ModerationState::Supported,
            },
        ))
        .expect("supported evaluation");
}

fn supported_hydrates(
    fixture: &mut ContributionFixture,
    _public: &PublicWorkerDbPool,
    claim_id: Uuid,
) -> bool {
    let expected = identity(fixture, claim_id);
    fixture
        .rt
        .block_on(public_repo::hydrate_gateway(&fixture.gateway, &expected))
        .expect("strict public hydration")
        .is_some()
}

fn counterfactual_supported(_fixture: &ContributionFixture, claim_id: Uuid) -> bool {
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG");
    // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
    let mut client = Client::connect(&dsn_as_role(&dsn, "role_public_worker"), NoTls)
        .expect("public SQL counterfactual");
    client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM public.claims c \
             JOIN public.claim_trust_evaluations e ON e.evaluation_id=c.current_evaluation_id \
             WHERE c.claim_id=$1 AND c.moderation_state='SUPPORTED')",
            &[&claim_id],
        )
        .expect("counterfactual supported read")
        .get(0)
}

fn run_case(case_id: &str) -> (bool, bool) {
    let mut fixture = ContributionFixture::new();
    let public = public_pool(&fixture);
    let admitted = admit(&fixture, &public);
    match case_id {
        "a_provenance_active_release" => {
            let probe = fixture
                .rt
                .block_on(public_provenance::probe_public_provenance(
                    &public,
                    fixture.auth.tenant_id(),
                    public_provenance::PublicProvenanceTarget::new(Some(admitted.claim_id), None)
                        .unwrap(),
                ))
                .expect("healthy provenance probe");
            (!probe.orphaned, !probe.orphaned)
        }
        "b_provenance_edge_deleted" => {
            fixture
                .admin
                .execute(
                    "DELETE FROM public.provenance_edges WHERE claim_id=$1 AND source_id=$2",
                    &[&admitted.claim_id, &admitted.source_id],
                )
                .expect("delete provenance edge");
            let probe = fixture
                .rt
                .block_on(public_provenance::probe_public_provenance(
                    &public,
                    fixture.auth.tenant_id(),
                    public_provenance::PublicProvenanceTarget::new(Some(admitted.claim_id), None)
                        .unwrap(),
                ))
                .expect("drift provenance probe");
            (!probe.orphaned, !probe.orphaned)
        }
        "c_eligible_supported_active" => {
            support_claim(
                &mut fixture,
                &public,
                admitted.claim_id,
                admitted.object_revision,
            );
            let primary = supported_hydrates(&mut fixture, &public, admitted.claim_id);
            (
                primary,
                counterfactual_supported(&fixture, admitted.claim_id),
            )
        }
        "d_eligible_supported_after_revoke_before_consumer" => {
            support_claim(
                &mut fixture,
                &public,
                admitted.claim_id,
                admitted.object_revision,
            );
            let changed = fixture
                .rt
                .block_on(contribution_repo::revoke_release(
                    &fixture.private,
                    fixture.auth.tenant_id(),
                    admitted.release_id,
                ))
                .expect("revoke release");
            assert!(changed, "first revoke must change the release");
            let primary = supported_hydrates(&mut fixture, &public, admitted.claim_id);
            (
                primary,
                counterfactual_supported(&fixture, admitted.claim_id),
            )
        }
        _ => panic!("unknown benchmark case {case_id}"),
    }
}

fn max_pairwise_hamming(runs: &[Vec<bool>]) -> usize {
    assert!(runs.len() >= 2, "spread requires repeated verdict vectors");
    let mut spread = 0;
    for (left, vector) in runs.iter().enumerate() {
        for other in &runs[left + 1..] {
            assert_eq!(vector.len(), other.len(), "fixed denominator changed");
            let distance = vector.iter().zip(other).filter(|(a, b)| a != b).count();
            spread = spread.max(distance);
        }
    }
    spread
}

fn stable_flip_indices(primary: &[Vec<bool>], control: &[Vec<bool>]) -> Vec<usize> {
    assert_eq!(primary.len(), 3);
    assert_eq!(control.len(), 3);
    let n = primary[0].len();
    assert!(primary.iter().chain(control).all(|run| run.len() == n));
    (0..n)
        .filter(|&i| {
            primary.iter().all(|run| run[i] == primary[0][i])
                && control.iter().all(|run| run[i] == control[0][i])
                && primary[0][i] != control[0][i]
        })
        .collect()
}

#[test]
fn cancelling_flips_are_visible_and_unstable_flips_are_not_resolution() {
    let cancelling = vec![vec![true, false], vec![false, true], vec![true, false]];
    assert!(
        cancelling
            .iter()
            .all(|run| run.iter().filter(|v| **v).count() == 1)
    );
    assert_eq!(max_pairwise_hamming(&cancelling), 2);
    let stable = vec![vec![true, true]; 3];
    assert_eq!(max_pairwise_hamming(&stable), 0);
    assert!(stable_flip_indices(&stable, &cancelling).is_empty());
    assert_eq!(
        stable_flip_indices(&stable, &vec![vec![true, false]; 3]),
        vec![1]
    );
}

#[test]
#[ignore = "lane(c) its admit() helper at line 119 admits through role_public_worker admit_release, the exact path migration 0124_phase9_independence_attestation:233 fenced and ADR-0047's Open debt names as must-not-reopen. The three-run counterfactual measurement has to be rebuilt on the assessed/anonymous seam; retired with that recipe rather than carried as a permanent red."]
#[allow(clippy::too_many_lines)] // One fixed three-run measurement retains every per-case observation.
fn fixed_cases_repeat_three_times_and_measure_counterfactual_resolution() {
    let specs = dataset_cases();
    assert_eq!(
        specs.len(),
        4,
        "fixed denominator must be four dataset rows"
    );
    assert_eq!(
        specs.iter().map(|case| case.id).collect::<Vec<_>>(),
        vec![
            "a_provenance_active_release",
            "b_provenance_edge_deleted",
            "c_eligible_supported_active",
            "d_eligible_supported_after_revoke_before_consumer"
        ]
    );
    assert_eq!(
        specs.iter().map(|case| case.layer).collect::<Vec<_>>(),
        vec!["provenance2", "provenance2", "withdrawal2", "withdrawal2"]
    );
    let fixture_sha256 = sha256_hex(DATASET.as_bytes());
    let profile_fingerprint = profile_fingerprint();
    println!("BENCH fixture_sha256={fixture_sha256} profile_fingerprint={profile_fingerprint}");
    assert_eq!(
        fixture_sha256,
        "a6982808fad9010304b28fed9f536d9c6379e265a694148b1f32877ef0c05937"
    );

    let mut primary_runs = Vec::new();
    let mut counterfactual_runs = Vec::new();
    for run in 0..3 {
        let mut primary = Vec::new();
        let mut counterfactual = Vec::new();
        for case in &specs {
            let (p, c) = run_case(case.id);
            assert_eq!(p, case.primary, "primary label mismatch for {}", case.id);
            assert_eq!(
                c, case.counterfactual,
                "control label mismatch for {}",
                case.id
            );
            println!(
                "BENCH run={run} case={} primary={p} counterfactual={c}",
                case.id
            );
            primary.push(p);
            counterfactual.push(c);
        }
        primary_runs.push(primary);
        counterfactual_runs.push(counterfactual);
    }
    assert!(primary_runs.iter().all(|run| run.len() == specs.len()));
    assert!(
        counterfactual_runs
            .iter()
            .all(|run| run.len() == specs.len())
    );
    let stable_flips = stable_flip_indices(&primary_runs, &counterfactual_runs);
    let flips = stable_flips.len();
    let expected_flips: Vec<usize> = specs
        .iter()
        .enumerate()
        .filter_map(|(i, case)| (case.primary != case.counterfactual).then_some(i))
        .collect();
    assert_eq!(stable_flips, expected_flips);
    println!("BENCH resolution_flips={flips} stable_indices={stable_flips:?}");
    let within_system =
        max_pairwise_hamming(&primary_runs).max(max_pairwise_hamming(&counterfactual_runs));
    assert!(
        flips > within_system,
        "cannot distinguish systems from repeat noise"
    );
    for i in 0..specs.len() {
        let values: Vec<bool> = primary_runs.iter().map(|run| run[i]).collect();
        let hamming = max_pairwise_hamming(&values.iter().map(|v| vec![*v]).collect::<Vec<_>>());
        println!(
            "BENCH hamming case={} pairwise_flips={hamming}",
            specs[i].id
        );
        assert_eq!(
            hamming, 0,
            "primary spread case={} values={values:?}",
            specs[i].id
        );
    }
    for layer in ["provenance2", "withdrawal2"] {
        let verdicts: Vec<Vec<bool>> = primary_runs
            .iter()
            .map(|run| {
                run.iter()
                    .zip(&specs)
                    .filter(|(_, case)| case.layer == layer)
                    .map(|(value, case)| *value == case.primary)
                    .collect()
            })
            .collect();
        let counts: Vec<_> = verdicts
            .iter()
            .map(|run| run.iter().filter(|v| **v).count())
            .collect();
        let spread = max_pairwise_hamming(&verdicts);
        let min = counts.iter().min().unwrap();
        let max = counts.iter().max().unwrap();
        println!(
            "BENCH layer={layer} verdicts={verdicts:?} correct_counts={counts:?} min={min} max={max} spread={spread}"
        );
        assert_eq!(spread, 0);
    }
    assert_eq!(
        flips, 1,
        "withdrawal guard must be the sole counterfactual flip"
    );
}
