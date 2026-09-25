#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Mutex;

use contribution_fixture::ContributionFixture;
use humaux_adapters::{
    contribution_repo,
    postgres::PublicWorkerDbPool,
    public_provenance::{PublicProvenanceTarget, probe_public_provenance},
    public_repo,
};
use humaux_domain::{ids::TenantId, public::ModerationState};
use postgres::{Client, NoTls};
use uuid::Uuid;

static SERIAL: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Target {
    Claim(Uuid),
    Synthesis(Uuid),
}

impl Target {
    fn probe_target(self) -> PublicProvenanceTarget {
        match self {
            Self::Claim(id) => PublicProvenanceTarget::new(Some(id), None),
            Self::Synthesis(id) => PublicProvenanceTarget::new(None, Some(id)),
        }
        .expect("valid target")
    }
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
        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
            &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL"),
            "role_public_worker",
        )))
        .expect("public worker pool")
}

fn authority_roots(admin: &mut Client, target: Target) -> BTreeSet<Uuid> {
    let (claim_id, synthesis_id) = match target {
        Target::Claim(id) => (Some(id), None),
        Target::Synthesis(id) => (None, Some(id)),
    };
    admin
        .query(
            "SELECT root_source_id FROM public.current_public_roots($1,$2) ORDER BY root_source_id",
            &[&claim_id, &synthesis_id],
        )
        .expect("authoritative roots")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

fn closure_roots(admin: &mut Client, target: Target) -> BTreeMap<Uuid, i32> {
    let (claim_id, synthesis_id) = match target {
        Target::Claim(id) => (Some(id), None),
        Target::Synthesis(id) => (None, Some(id)),
    };
    admin
        .query(
            "SELECT root_source_id,depth FROM public.source_closure \
             WHERE is_current AND claim_id IS NOT DISTINCT FROM $1 \
               AND synthesis_id IS NOT DISTINCT FROM $2 ORDER BY root_source_id",
            &[&claim_id, &synthesis_id],
        )
        .expect("closure roots")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// Test-only independent graph walk. It is an oracle for one fixture target, never persisted.
fn walk_roots(
    admin: &mut Client,
    target: Target,
    next_root_depth: i32,
    path: &mut HashSet<Target>,
    roots: &mut BTreeMap<Uuid, i32>,
) -> Result<(), &'static str> {
    if !path.insert(target) {
        return Err("cycle");
    }
    let result = match target {
        Target::Claim(claim_id) => {
            let rows = admin
                .query(
                    "SELECT source_id FROM public.provenance_edges WHERE claim_id=$1",
                    &[&claim_id],
                )
                .map_err(|_| "claim edge query")?;
            if rows.is_empty() {
                Err("unrooted claim")
            } else {
                for row in rows {
                    let source_id: Uuid = row.get(0);
                    roots
                        .entry(source_id)
                        .and_modify(|depth| *depth = (*depth).min(next_root_depth))
                        .or_insert(next_root_depth);
                }
                Ok(())
            }
        }
        Target::Synthesis(synthesis_id) => {
            let rows = admin
                .query(
                    "SELECT claim_id,input_synthesis_id FROM public.synthesis_inputs \
                     WHERE synthesis_id=$1 ORDER BY ordinal",
                    &[&synthesis_id],
                )
                .map_err(|_| "synthesis input query")?;
            if rows.is_empty() {
                Err("unrooted synthesis")
            } else {
                for row in rows {
                    let claim_id: Option<Uuid> = row.get(0);
                    let child_synthesis_id: Option<Uuid> = row.get(1);
                    match (claim_id, child_synthesis_id) {
                        (Some(claim_id), None) => walk_roots(
                            admin,
                            Target::Claim(claim_id),
                            next_root_depth,
                            path,
                            roots,
                        )?,
                        (None, Some(synthesis_id)) => walk_roots(
                            admin,
                            Target::Synthesis(synthesis_id),
                            next_root_depth + 1,
                            path,
                            roots,
                        )?,
                        _ => return Err("invalid synthesis input"),
                    }
                }
                Ok(())
            }
        }
    };
    path.remove(&target);
    result
}

fn direct_graph_roots(
    admin: &mut Client,
    target: Target,
) -> Result<BTreeMap<Uuid, i32>, &'static str> {
    let mut roots = BTreeMap::new();
    let mut path = HashSet::new();
    let depth = match target {
        Target::Claim(_) => 1,
        Target::Synthesis(_) => 2,
    };
    walk_roots(admin, target, depth, &mut path, &mut roots)?;
    if roots.is_empty() {
        Err("empty root set")
    } else {
        Ok(roots)
    }
}

fn private_release_exists(dsn: &str, tenant_id: TenantId, release_id: Uuid) -> bool {
    let mut client = Client::connect(dsn, NoTls).expect("private role client");
    let mut txn = client.transaction().expect("private transaction");
    txn.batch_execute("SET LOCAL ROLE role_private_worker")
        .expect("private worker role");
    txn.execute(
        "SELECT set_config('humaux.tenant_id',$1,true)",
        &[&tenant_id.0.to_string()],
    )
    .expect("private tenant context");
    let exists: bool = txn
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM staging.contribution_releases \
             WHERE tenant_id=$1 AND contribution_release_id=$2)",
            &[&tenant_id.0, &release_id],
        )
        .expect("private release visibility")
        .get(0);
    txn.commit().expect("private commit");
    exists
}

fn assert_target_contract(
    fixture: &mut ContributionFixture,
    public: &PublicWorkerDbPool,
    target: Target,
) {
    let expected = direct_graph_roots(&mut fixture.admin, target).expect("independent graph roots");
    let authority = authority_roots(&mut fixture.admin, target);
    let closure = closure_roots(&mut fixture.admin, target);
    assert_eq!(authority, expected.keys().copied().collect());
    assert_eq!(
        closure, expected,
        "closure must retain exact shortest-depth root pairs"
    );

    let probe = fixture
        .rt
        .block_on(probe_public_provenance(
            public,
            fixture.auth.tenant_id(),
            target.probe_target(),
        ))
        .expect("typed provenance probe");
    assert!(!probe.orphaned);
    assert!(!probe.closure_depth_drift);
    assert!(!probe.typed_root_invalid);
    assert_eq!(probe.direct_root_count, authority.len() as u64);
    assert_eq!(probe.closure_root_count, closure.len() as u64);

    for source_id in authority {
        let row = fixture
            .admin
            .query_one(
                "SELECT source_type,contribution_release_id FROM public.sources WHERE source_id=$1",
                &[&source_id],
            )
            .expect("discovered root identity");
        let source_type: String = row.get(0);
        let release_id: Option<Uuid> = row.get(1);
        if source_type == "USER_CONTRIBUTION" {
            assert!(
                private_release_exists(
                    &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL"),
                    fixture.auth.tenant_id(),
                    release_id.expect("user root release id"),
                ),
                "only the private typed pool resolves a discovered User root to its release"
            );
        } else {
            assert!(
                release_id.is_none(),
                "non-user root remains a PublicSource root"
            );
        }
    }
}

fn grant_moderator(fixture: &mut ContributionFixture) {
    fixture
        .admin
        .execute(
            "INSERT INTO control.public_moderator_grants(user_id,grant_version,enabled) \
             VALUES($1,1,true) ON CONFLICT(user_id) DO UPDATE \
             SET grant_version=EXCLUDED.grant_version,enabled=EXCLUDED.enabled",
            &[&fixture.auth.user_id().expect("fixture user").0],
        )
        .expect("global moderator grant");
}

fn evaluate_supported(
    fixture: &mut ContributionFixture,
    public: &PublicWorkerDbPool,
    claim_id: Uuid,
    expected_revision: i64,
) -> public_repo::EvaluationResult {
    grant_moderator(fixture);
    let body_sha256: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to(content::text,'UTF8')) FROM public.claims WHERE claim_id=$1",
            &[&claim_id],
        )
        .expect("body hash")
        .get(0);
    fixture
        .rt
        .block_on(public_repo::evaluate_claim(
            public,
            &fixture.auth,
            &public_repo::EvaluateClaim {
                claim_id,
                expected_revision,
                expected_body_sha256: &body_sha256,
                policy_version: "public-provenance-test-v1",
                rationale: "authenticated moderator typed root evaluation",
                target_state: ModerationState::Supported,
            },
        ))
        .expect("supported evaluation")
}

/// Isolated-database-only damage helper. It restores the exact constraint definition even when
/// a test assertion unwinds, so it never leaves the dedicated mutation database weakened.
struct UserRootFault<'a> {
    admin: &'a mut Client,
    constraint_definition: String,
    source_id: Option<Uuid>,
    claim_id: Option<Uuid>,
    restored: bool,
}

impl<'a> UserRootFault<'a> {
    fn new(admin: &'a mut Client) -> Self {
        let constraint_definition: String = admin
            .query_one(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
                 WHERE conrelid='public.sources'::regclass AND conname='sources_release_type'",
                &[],
            )
            .expect("source release constraint backup")
            .get(0);
        admin
            .batch_execute(
                "ALTER TABLE public.sources DROP CONSTRAINT sources_release_type; \
                 ALTER TABLE public.sources DISABLE TRIGGER public_source_contribution_guard",
            )
            .expect("isolated fault enable");
        Self {
            admin,
            constraint_definition,
            source_id: None,
            claim_id: None,
            restored: false,
        }
    }

    fn insert_damaged_root(&mut self) -> Target {
        let source_id: Uuid = self
            .admin
            .query_one(
                "INSERT INTO public.sources(source_type,content_hash,rights_basis,trust_class) \
                 VALUES('USER_CONTRIBUTION',$1,'fixture rights','UNDER_REVIEW') RETURNING source_id",
                &[&"d".repeat(64)],
            )
            .expect("damaged User source")
            .get(0);
        let claim_id: Uuid = self
            .admin
            .query_one(
                "INSERT INTO public.claims(content) VALUES('{}') RETURNING claim_id",
                &[],
            )
            .expect("damaged root claim")
            .get(0);
        self.admin
            .execute(
                "INSERT INTO public.provenance_edges(claim_id,source_id) VALUES($1,$2)",
                &[&claim_id, &source_id],
            )
            .expect("damaged root edge");
        self.admin
            .execute(
                "INSERT INTO public.source_closure(claim_id,root_source_id,depth,is_current) \
                 VALUES($1,$2,1,true)",
                &[&claim_id, &source_id],
            )
            .expect("damaged root closure");
        self.source_id = Some(source_id);
        self.claim_id = Some(claim_id);
        Target::Claim(claim_id)
    }

    fn restore(&mut self) {
        if self.restored {
            return;
        }
        if let Some(claim_id) = self.claim_id {
            self.admin
                .execute(
                    "DELETE FROM public.source_closure WHERE claim_id=$1",
                    &[&claim_id],
                )
                .expect("remove damaged closure");
            self.admin
                .execute(
                    "DELETE FROM public.provenance_edges WHERE claim_id=$1",
                    &[&claim_id],
                )
                .expect("remove damaged edge");
            self.admin
                .execute("DELETE FROM public.claims WHERE claim_id=$1", &[&claim_id])
                .expect("remove damaged claim");
        }
        if let Some(source_id) = self.source_id {
            self.admin
                .execute(
                    "DELETE FROM public.sources WHERE source_id=$1",
                    &[&source_id],
                )
                .expect("remove damaged source");
        }
        self.admin
            .batch_execute(&format!(
                "ALTER TABLE public.sources ADD CONSTRAINT sources_release_type {}; \
                 ALTER TABLE public.sources ENABLE TRIGGER public_source_contribution_guard",
                self.constraint_definition
            ))
            .expect("restore isolated source guard");
        self.restored = true;
    }
}

impl Drop for UserRootFault<'_> {
    fn drop(&mut self) {
        self.restore();
    }
}

#[test]
#[ignore = "lane(c) pins role_public_worker admit_release for an identity-bearing User root. Migration 0124_phase9_independence_attestation:233 REVOKEs SELECT ON staging.contribution_releases from role_public_worker on purpose, public_trust.rs pins that the same call must answer Forbidden (and passes), and ADR-0047's Open debt forbids re-opening it — the 0165 attempt was reverted as a lineage leak. Rebuild the typed-root / closure-depth / revocation oracle on admit_assessed_release + run_anonymous_once before restoring it."]
#[allow(clippy::too_many_lines)] // One real-PG fixture establishes User, non-user, recursive, and fault boundaries.
fn typed_roots_closure_depths_and_revocation_are_target_bound() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let public = public_pool(&fixture);
    let release = fixture.finalize_release();
    let admitted = fixture
        .rt
        .block_on(public_repo::admit_release(
            &public,
            fixture.auth.tenant_id(),
            release,
        ))
        .expect("authenticated User root admission");
    assert_target_contract(&mut fixture, &public, Target::Claim(admitted.claim_id));

    let invalid_user_source = fixture.admin.execute(
        "INSERT INTO public.sources(source_type,content_hash,rights_basis,trust_class) \
         VALUES('USER_CONTRIBUTION',$1,'fixture rights','UNDER_REVIEW')",
        &[&"u".repeat(64)],
    );
    assert!(
        invalid_user_source.is_err(),
        "a User root without a release cannot become a damaged persistent row"
    );

    let official_source: Uuid = fixture
        .admin
        .query_one(
            "INSERT INTO public.sources(source_type,content_hash,rights_basis,trust_class) \
             VALUES('OFFICIAL_DOCUMENT',$1,'fixture rights','UNDER_REVIEW') RETURNING source_id",
            &[&"o".repeat(64)],
        )
        .expect("controlled non-user root fixture")
        .get(0);
    let official_claim: Uuid = fixture
        .admin
        .query_one(
            "INSERT INTO public.claims(content) VALUES('{\"text\":\"official fixture\"}') RETURNING claim_id",
            &[],
        )
        .expect("official claim")
        .get(0);
    fixture
        .admin
        .execute(
            "INSERT INTO public.provenance_edges(claim_id,source_id) VALUES($1,$2)",
            &[&official_claim, &official_source],
        )
        .expect("official edge");
    fixture
        .admin
        .execute(
            "INSERT INTO public.source_closure(claim_id,root_source_id,depth,is_current) VALUES($1,$2,1,true)",
            &[&official_claim, &official_source],
        )
        .expect("official closure");
    assert_target_contract(&mut fixture, &public, Target::Claim(official_claim));

    let first_synthesis: Uuid = fixture
        .admin
        .query_one(
            "INSERT INTO public.syntheses(content) VALUES('{}') RETURNING synthesis_id",
            &[],
        )
        .expect("first synthesis")
        .get(0);
    fixture
        .admin
        .execute(
            "INSERT INTO public.synthesis_inputs(synthesis_id,claim_id,ordinal) VALUES($1,$2,0),($1,$3,1)",
            &[&first_synthesis, &admitted.claim_id, &official_claim],
        )
        .expect("mixed first inputs");
    assert_eq!(
        fixture
            .rt
            .block_on(contribution_repo::recompute_source_closure(
                &public,
                first_synthesis
            ))
            .expect("mixed closure"),
        2
    );
    assert_target_contract(&mut fixture, &public, Target::Synthesis(first_synthesis));

    let second_synthesis: Uuid = fixture
        .admin
        .query_one(
            "INSERT INTO public.syntheses(content) VALUES('{}') RETURNING synthesis_id",
            &[],
        )
        .expect("second synthesis")
        .get(0);
    fixture
        .admin
        .execute(
            "INSERT INTO public.synthesis_inputs(synthesis_id,input_synthesis_id,ordinal) VALUES($1,$2,0)",
            &[&second_synthesis, &first_synthesis],
        )
        .expect("recursive input");
    assert_eq!(
        fixture
            .rt
            .block_on(contribution_repo::recompute_source_closure(
                &public,
                second_synthesis
            ))
            .expect("depth-three closure"),
        2
    );
    assert_target_contract(&mut fixture, &public, Target::Synthesis(second_synthesis));
    let depths = closure_roots(&mut fixture.admin, Target::Synthesis(second_synthesis));
    assert_eq!(depths.get(&admitted.source_id), Some(&3));
    assert_eq!(depths.get(&official_source), Some(&3));

    fixture
        .admin
        .execute(
            "UPDATE public.source_closure SET depth=4 WHERE synthesis_id=$1 AND root_source_id=$2",
            &[&second_synthesis, &admitted.source_id],
        )
        .expect("faulty depth");
    let depth_fault = fixture
        .rt
        .block_on(probe_public_provenance(
            &public,
            fixture.auth.tenant_id(),
            Target::Synthesis(second_synthesis).probe_target(),
        ))
        .expect("depth drift probe");
    assert!(depth_fault.orphaned);
    assert!(depth_fault.closure_depth_drift);
    fixture
        .admin
        .execute(
            "UPDATE public.source_closure SET depth=3 WHERE synthesis_id=$1 AND root_source_id=$2",
            &[&second_synthesis, &admitted.source_id],
        )
        .expect("restore depth");

    let bad_claim: Uuid = fixture
        .admin
        .query_one(
            "INSERT INTO public.claims(content) VALUES('{}') RETURNING claim_id",
            &[],
        )
        .expect("fault claim")
        .get(0);
    assert!(direct_graph_roots(&mut fixture.admin, Target::Claim(bad_claim)).is_err());
    assert!(
        fixture
            .rt
            .block_on(probe_public_provenance(
                &public,
                fixture.auth.tenant_id(),
                Target::Claim(bad_claim).probe_target(),
            ))
            .expect("rootless claim probe")
            .orphaned
    );
    let bad_synthesis: Uuid = fixture
        .admin
        .query_one(
            "INSERT INTO public.syntheses(content) VALUES('{}') RETURNING synthesis_id",
            &[],
        )
        .expect("fault synthesis")
        .get(0);
    fixture
        .admin
        .execute(
            "INSERT INTO public.synthesis_inputs(synthesis_id,claim_id,ordinal) VALUES($1,$2,0)",
            &[&bad_synthesis, &bad_claim],
        )
        .expect("unrooted branch");
    assert!(
        fixture
            .rt
            .block_on(contribution_repo::recompute_source_closure(
                &public,
                bad_synthesis
            ))
            .is_err()
    );
    assert!(direct_graph_roots(&mut fixture.admin, Target::Synthesis(bad_synthesis)).is_err());

    let cycle_left: Uuid = fixture
        .admin
        .query_one(
            "INSERT INTO public.syntheses(content) VALUES('{}') RETURNING synthesis_id",
            &[],
        )
        .expect("cycle left")
        .get(0);
    let cycle_right: Uuid = fixture
        .admin
        .query_one(
            "INSERT INTO public.syntheses(content) VALUES('{}') RETURNING synthesis_id",
            &[],
        )
        .expect("cycle right")
        .get(0);
    fixture
        .admin
        .execute(
            "INSERT INTO public.synthesis_inputs(synthesis_id,input_synthesis_id,ordinal) VALUES($1,$2,0),($2,$1,0)",
            &[&cycle_left, &cycle_right],
        )
        .expect("cycle inputs");
    assert!(
        fixture
            .rt
            .block_on(contribution_repo::recompute_source_closure(
                &public, cycle_left
            ))
            .is_err()
    );
    assert!(direct_graph_roots(&mut fixture.admin, Target::Synthesis(cycle_left)).is_err());

    let evaluation = evaluate_supported(
        &mut fixture,
        &public,
        admitted.claim_id,
        admitted.object_revision,
    );
    let hash: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to(content::text,'UTF8')) FROM public.claims WHERE claim_id=$1",
            &[&admitted.claim_id],
        )
        .expect("projection hash")
        .get(0);
    let identity = public_repo::ProjectionIdentity {
        object_id: admitted.claim_id,
        object_kind: "CLAIM".into(),
        object_revision: evaluation.object_revision,
        evaluation_id: evaluation.evaluation_id,
        body_sha256: hash.try_into().expect("sha256 length"),
    };
    assert!(
        fixture
            .rt
            .block_on(public_repo::hydrate_gateway(&fixture.gateway, &identity))
            .expect("pre-revoke hydrate")
            .is_some()
    );
    assert!(
        fixture
            .rt
            .block_on(contribution_repo::revoke_release(
                &fixture.private,
                fixture.auth.tenant_id(),
                release,
            ))
            .expect("revoke release")
    );
    assert_target_contract(&mut fixture, &public, Target::Claim(admitted.claim_id));
    assert!(
        fixture
            .rt
            .block_on(public_repo::hydrate_gateway(&fixture.gateway, &identity))
            .expect("post-revoke hydrate")
            .is_none()
    );
}

#[test]
#[ignore = "lane(a:provenance_mutation) requires the dedicated isolated provenance mutation database"]
fn damaged_user_root_without_release_is_detected_and_guard_restored() {
    assert_eq!(
        std::env::var("HUMAUX_PUBLIC_PROVENANCE_FAULT_DB").as_deref(),
        Ok("1"),
        "refusing to alter source guards outside the dedicated mutation database"
    );
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let public = public_pool(&fixture);
    let mut fault = UserRootFault::new(&mut fixture.admin);
    let target = fault.insert_damaged_root();
    let probe = fixture
        .rt
        .block_on(probe_public_provenance(
            &public,
            fixture.auth.tenant_id(),
            target.probe_target(),
        ))
        .expect("damaged root probe");
    let detected = probe.orphaned && probe.typed_root_invalid;
    fault.restore();
    assert!(
        detected,
        "typed-root probe must reject the damaged persisted User root"
    );
}

#[test]
fn target_requires_exactly_one_non_nil_identity() {
    assert!(PublicProvenanceTarget::new(None, None).is_err());
    assert!(PublicProvenanceTarget::new(Some(uuid::Uuid::nil()), None).is_err());
    assert!(
        PublicProvenanceTarget::new(Some(uuid::Uuid::now_v7()), Some(uuid::Uuid::now_v7()))
            .is_err()
    );
}
