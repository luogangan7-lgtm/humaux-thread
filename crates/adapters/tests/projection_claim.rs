//! `adapters::tests::projection_claim` — ADR-0052 (card 27) real-PostgreSQL tests of the cross-tenant projection
//!   ticket claim, its lease fence, the retry/backoff/exhaustion transitions and the 0176 grant boundary.
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-domain, humaux-infra-cell, humaux-local-secret-scan,
//!   humaux-retrieval-provider, humaux-testkit, postgres, serde_json, sqlx, time, tokio];
//!   services=[PostgreSQL(any) r=[projection.stream_log] w=[control.private_reasoning_domains, control.tenants,
//!   ops.jobs, ops.outbox, private.evidence_objects, private.events, private.memory_evidence,
//!   private.memory_records, projection.private_memory_points, projection.stream_checkpoints,
//!   projection.stream_log, projection.tenant_placements]
//!   x=[projection.claim_issued_tickets, projection.retire_failed_ticket, projection.unplaced_issued_tickets],
//!   PostgreSQL(role_gateway), PostgreSQL(role_maintenance), PostgreSQL(role_retrieval_worker),
//!   subprocess(gitleaks)];
//!   env=[HUMAUX_TEST_GITLEAKS_BIN, HUMAUX_TEST_GITLEAKS_SHA256, HUMAUX_TEST_GITLEAKS_VERSION, HUMAUX_TEST_PG_DSN];
//!   modules=[adapters::postgres, adapters::projection_worker, adapters::qdrant, adapters::remember,
//!   adapters::stream_repo, domain::egress, domain::error, domain::evidence, domain::ids, domain::subject,
//!   humaux-local-secret-scan, humaux-testkit, infra-cell::permit, infra-cell::resource, infra-cell::transport,
//!   retrieval-provider::adapters, retrieval-provider::contract]
//! Called-by: [cargo-test]
//! Invariants: [every test uses throwaway tenants and its own projection_version ('card27-test-<uuid>'), so no dev
//!   ticket ever matches its claims; all rows are deleted on Drop; no DSN is a visible SKIP and HUMAUX_REQUIRE_DB=1
//!   turns it into a failure]
//! Spec: Baseline §15.2; §31; §6.2.2; §79.2; ADR-0036; ADR-0043; ADR-0052
//!
//! The claim is cross-tenant, so these tests cannot scope it by tenant: they scope it by family.
//! Each test claims `private_memory / PRIVATE_MEMORY / card27-test-<uuid>` — a
//! `projection_version` nothing else writes — through the same `stream_repo` functions the
//! resident runner calls. Tickets are issued by the real `remember::remember` (role_gateway), so
//! each one has its Evidence, its `EVIDENCE_ACCEPTED` outbox row and its checkpoint; a test that
//! wants a ticket claimable closes that outbox row (`close_distill`), exactly what a finished
//! distill does.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use humaux_adapters::postgres::{RetrievalWorkerDbPool, RuntimeDbPool};
use humaux_adapters::projection_worker::{
    CardEmbedder, PassConfig, SharedProjectionDeps, run_claimed_pass,
};
use humaux_adapters::qdrant::RetrievalFamily;
use humaux_adapters::remember::{self, RememberCommand};
use humaux_adapters::stream_repo::{
    self, Backoff, ClaimFamily, ClaimedTicket, RETRY_PREDICATE, TicketFence,
};
use humaux_domain::egress::ProcessorId;
use humaux_domain::error::ErrorCode;
use humaux_domain::evidence::{EvidenceOriginClass, payload_sha256};
use humaux_domain::ids::TenantId;
use humaux_infra_cell::{
    CallerId, CellId, HttpIntraCellTransport, IntraCellResource, IntraCellResourceRegistry,
    ResourceEntry,
};
use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig, SealedRetrievalCard};
use humaux_retrieval_provider::adapters::TestDoubleProvider;
use humaux_retrieval_provider::contract::{
    CalibrationProfileId, EmbeddingModelDescriptor, EmbeddingProvider, ModelId,
    RerankModelDescriptor, RerankScoreSemantics,
};
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

const OWNER_A: &str = "humaux-retrieval-worker/card27-test-a";
const OWNER_B: &str = "humaux-retrieval-worker/card27-test-b";

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

/// One test's world: an owner client for setup/inspection, the two runtime pools the code under
/// test uses, and the throwaway tenants/version it created.
struct Env {
    rt: tokio::runtime::Runtime,
    admin: Client,
    admin_dsn: String,
    worker: RetrievalWorkerDbPool,
    gateway: RuntimeDbPool,
    family: ClaimFamily,
    tenants: Vec<Uuid>,
    domains: HashMap<Uuid, Uuid>,
}

impl Env {
    /// `None` = visible SKIP (or a failure under `HUMAUX_REQUIRE_DB=1`).
    fn new(test: &str) -> Option<Self> {
        let Ok(admin_dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
            skip_or_fail(
                test,
                "missing object: HUMAUX_TEST_PG_DSN",
                ExternalDep::Postgres,
            );
            return None;
        };
        // dep: PostgreSQL(any) — the owner client for fixture setup and inspection
        let admin = match Client::connect(&admin_dsn, NoTls) {
            Ok(client) => client,
            Err(e) => {
                skip_or_fail(
                    test,
                    &format!("missing object: live Postgres ({e})"),
                    ExternalDep::Postgres,
                );
                return None;
            }
        };
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let worker = rt
            // dep: PostgreSQL(role_retrieval_worker) — the pool the claim/lease code runs under
            .block_on(RetrievalWorkerDbPool::connect(&dsn_as_role(
                &admin_dsn,
                "role_retrieval_worker",
            )))
            .expect("role_retrieval_worker connects");
        let gateway = rt
            // dep: PostgreSQL(role_gateway) — remember() issues the tickets under test
            .block_on(RuntimeDbPool::connect(&dsn_as_role(
                &admin_dsn,
                "role_gateway",
            )))
            .expect("role_gateway connects");
        Some(Self {
            rt,
            admin,
            admin_dsn,
            worker,
            gateway,
            family: ClaimFamily {
                domain: "private_memory".to_owned(),
                projection_kind: "PRIVATE_MEMORY".to_owned(),
                projection_version: format!("card27-test-{}", Uuid::new_v4().simple()),
                placement: RetrievalFamily::PrivateMemoryV1,
            },
            tenants: Vec::new(),
            domains: HashMap::new(),
        })
    }

    /// A throwaway tenant (with a reasoning domain), placed or not.
    fn tenant(&mut self, placed: bool) -> Uuid {
        let tenant: Uuid = self
            .admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ('card27 projection_claim') RETURNING tenant_id",
                &[],
            )
            .expect("throwaway tenant")
            .get(0);
        let domain: Uuid = self
            .admin
            .query_one(
                "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
                 VALUES ($1, 'card27') RETURNING reasoning_domain_id",
                &[&tenant],
            )
            .expect("reasoning domain")
            .get(0);
        if placed {
            self.admin
                .execute(
                    "INSERT INTO projection.tenant_placements \
                       (tenant_id, projection_family, collection_name, placement_class) \
                     VALUES ($1, 'private_memory_v1', 'card27_claim_test', 'SHARED_FALLBACK')",
                    &[&tenant],
                )
                .expect("placement");
        }
        self.tenants.push(tenant);
        self.domains.insert(tenant, domain);
        tenant
    }

    /// One ticket of this test's family in `(tenant, scope)`, issued by the real `remember`.
    /// Returns `(stream_seq, evidence_id)`.
    fn ticket(&mut self, tenant: Uuid, scope: Uuid) -> (i64, Uuid) {
        let content = format!("card27 claim fixture {}", Uuid::new_v4());
        let cmd = RememberCommand {
            tenant_id: tenant,
            authorization_user_id: None,
            scope_kind: "workspace".to_owned(),
            scope_id: scope,
            domain: self.family.domain.clone(),
            projection_kind: self.family.projection_kind.clone(),
            projection_version: self.family.projection_version.clone(),
            consistency_token_expires_at: time::OffsetDateTime::now_utc()
                + std::time::Duration::from_secs(3600),
            batch_id: None,
            payload_sha256: payload_sha256(content.as_bytes()),
            data_class: "INTERNAL".to_owned(),
            origin_class: EvidenceOriginClass::DirectUserInput,
            origin_principal_id: None,
            origin_connector_id: None,
            visibility_class: "TENANT_SHARED".to_owned(),
            visibility_user_id: None,
            visibility_workspace_id: None,
            reasoning_domain_id: self.domains[&tenant],
            occurred_at: None,
            event_kind: "MANUAL_NOTE".to_owned(),
            event_payload: serde_json::json!({ "content": content }),
            subjects: humaux_domain::subject::SubjectDeclaration::default(),
            affects: Vec::new(),
            mood_half_life: None,
        };
        let accepted = self
            .rt
            .block_on(remember::remember(&self.gateway, cmd))
            .expect("remember issues the ticket");
        let seq: i64 = self
            .admin
            .query_one(
                "SELECT sl.stream_seq FROM projection.stream_log sl \
                 JOIN ops.outbox ob ON ob.tenant_id = sl.tenant_id AND ob.commit_seq = sl.commit_seq \
                 WHERE ob.evidence_id = $1",
                &[&accepted.evidence_id],
            )
            .expect("the ticket remember issued")
            .get(0);
        (seq, accepted.evidence_id)
    }

    /// The Evidence's distill finished: its EVIDENCE_ACCEPTED row is DONE.
    fn close_distill(&mut self, tenant: Uuid) {
        self.admin
            .execute(
                "UPDATE ops.outbox SET status = 'DONE', processed_at = now() \
                 WHERE tenant_id = $1 AND event_type = 'EVIDENCE_ACCEPTED'",
                &[&tenant],
            )
            .expect("close distill");
    }

    /// Settles one ticket as `role_retrieval_worker` itself (the 0011/0167 guard admits
    /// `ISSUED -> {DONE,FAILED}` for that role only), clearing its lease.
    fn settle_as_worker(
        &self,
        tenant: Uuid,
        scope: Uuid,
        seq: i64,
        state: &str,
        class: Option<&str>,
    ) {
        // dep: PostgreSQL(role_retrieval_worker) — the one role the transition guard lets settle
        let mut worker = Client::connect(
            &dsn_as_role(&self.admin_dsn, "role_retrieval_worker"),
            NoTls,
        )
        .expect("role_retrieval_worker connects");
        let mut tx = worker.transaction().expect("txn");
        tx.batch_execute(&format!("SET LOCAL humaux.tenant_id = '{tenant}'"))
            .expect("tenant GUC");
        let n = tx
            .execute(
                "UPDATE projection.stream_log \
                    SET state = $1, error_class = $2, lease_owner = NULL, lease_expires_at = NULL \
                  WHERE tenant_id = $3 AND scope_kind = 'workspace' AND scope_id = $4 \
                    AND projection_version = $5 AND stream_seq = $6 AND state = 'ISSUED'",
                &[
                    &state,
                    &class,
                    &tenant,
                    &scope,
                    &self.family.projection_version,
                    &seq,
                ],
            )
            .expect("settle");
        assert_eq!(n, 1, "settled exactly one ticket");
        tx.commit().expect("commit");
    }

    fn claim(&self, owner: &str, lease: f64, limit: i64, cap: i64) -> Vec<ClaimedTicket> {
        self.rt
            .block_on(stream_repo::claim_issued(
                &self.worker,
                &self.family,
                owner,
                lease,
                limit,
                cap,
            ))
            .expect("claim")
            .tickets
    }

    fn row(&mut self, tenant: Uuid, scope: Uuid, seq: i64) -> TicketRow {
        let r = self
            .admin
            .query_one(
                &format!(
                    "SELECT state, attempts, lease_owner, error_class, \
                            next_attempt_at IS NOT NULL AND next_attempt_at > now() AS backing_off, \
                            EXTRACT(EPOCH FROM (next_attempt_at - now()))::float8 AS backoff_secs, \
                            ({RETRY_PREDICATE}) AS retry \
                     FROM projection.stream_log \
                     WHERE tenant_id = $1 AND scope_kind = 'workspace' AND scope_id = $2 \
                       AND projection_version = $3 AND stream_seq = $4"
                ),
                &[&tenant, &scope, &self.family.projection_version, &seq],
            )
            .expect("ticket row");
        TicketRow {
            state: r.get(0),
            attempts: r.get(1),
            lease_owner: r.get(2),
            error_class: r.get(3),
            backing_off: r.get::<_, Option<bool>>(4).unwrap_or(false),
            backoff_secs: r.get(5),
            retry: r.get::<_, Option<bool>>(6).unwrap_or(false),
        }
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        for tenant in &self.tenants {
            let _ = self.admin.batch_execute(&format!(
                "DELETE FROM projection.private_memory_points WHERE tenant_id = '{0}'; \
                 DELETE FROM ops.jobs WHERE tenant_id = '{0}'; \
                 DELETE FROM ops.outbox WHERE tenant_id = '{0}'; \
                 DELETE FROM projection.stream_log WHERE tenant_id = '{0}'; \
                 DELETE FROM projection.stream_checkpoints WHERE tenant_id = '{0}'; \
                 DELETE FROM projection.tenant_placements WHERE tenant_id = '{0}'; \
                 DELETE FROM private.memory_evidence USING private.memory_records m \
                   WHERE memory_evidence.memory_id = m.memory_id AND m.tenant_id = '{0}'; \
                 DELETE FROM private.memory_records WHERE tenant_id = '{0}'; \
                 DELETE FROM private.events USING private.evidence_objects eo \
                   WHERE events.event_id = eo.evidence_id AND eo.tenant_id = '{0}'; \
                 DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
                 DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}'; \
                 DELETE FROM control.tenants WHERE tenant_id = '{0}';",
                tenant
            ));
        }
    }
}

#[derive(Debug)]
struct TicketRow {
    state: String,
    attempts: i32,
    lease_owner: Option<String>,
    error_class: Option<String>,
    backing_off: bool,
    backoff_secs: Option<f64>,
    retry: bool,
}

fn seqs_of(tickets: &[ClaimedTicket], tenant: Uuid, scope: Uuid) -> Vec<i64> {
    let mut seqs: Vec<i64> = tickets
        .iter()
        .filter(|t| t.key.tenant_id.0 == tenant && t.key.scope_id == scope)
        .map(|t| t.stream_seq)
        .collect();
    seqs.sort_unstable();
    seqs
}

fn fence(owner: Option<&str>, attempts: i32) -> TicketFence<'_> {
    TicketFence {
        lease_owner: owner,
        attempts,
    }
}

/// Four claimers race over 3 tenants × 2 families × 3 tickets; every ticket is claimed exactly
/// once and no family is split across two claimers. Fault injection: drop `SKIP LOCKED` and the
/// restated eligibility predicate on the UPDATE ⇒ duplicates.
#[test]
fn claim_takes_each_ticket_exactly_once_across_concurrent_claimers() {
    let Some(mut env) = Env::new("claim_takes_each_ticket_exactly_once_across_concurrent_claimers")
    else {
        return;
    };
    let mut all = BTreeSet::new();
    for _ in 0..3 {
        let t = env.tenant(true);
        for _ in 0..2 {
            let scope = Uuid::new_v4();
            for _ in 0..3 {
                let (seq, _) = env.ticket(t, scope);
                all.insert((t, scope, seq));
            }
        }
        env.close_distill(t);
    }
    let pool = Arc::new(
        env.rt
            // dep: PostgreSQL(role_retrieval_worker) — the racing claimers' shared pool
            .block_on(RetrievalWorkerDbPool::connect(&dsn_as_role(
                &env.admin_dsn,
                "role_retrieval_worker",
            )))
            .expect("racing pool"),
    );
    // Rounds: four claimers race once each (limit 4, cap 2), then every claimed ticket is settled
    // DONE — the only way the rest of a held family becomes claimable (D-A) — until a round finds
    // nothing.
    let mut seen = BTreeSet::new();
    for _round in 0..20 {
        let results: Vec<Vec<ClaimedTicket>> = env.rt.block_on(async {
            let claimers: Vec<_> = (0..4)
                .map(|i| {
                    let pool = Arc::clone(&pool);
                    let family = env.family.clone();
                    tokio::spawn(async move {
                        let owner = format!("humaux-retrieval-worker/card27-race-{i}");
                        stream_repo::claim_issued(&pool, &family, &owner, 60.0, 4, 2)
                            .await
                            .expect("claim")
                            .tickets
                    })
                })
                .collect();
            let mut out = Vec::new();
            for claimer in claimers {
                out.push(claimer.await.expect("claimer task"));
            }
            out
        });
        if results.iter().all(Vec::is_empty) {
            break;
        }
        let mut family_owner: BTreeMap<(Uuid, Uuid), usize> = BTreeMap::new();
        for (claimer, got) in results.iter().enumerate() {
            for t in got {
                assert!(
                    seen.insert((t.key.tenant_id.0, t.key.scope_id, t.stream_seq)),
                    "ticket claimed twice: {:?}",
                    t.stream_seq
                );
                let owner = family_owner
                    .entry((t.key.tenant_id.0, t.key.scope_id))
                    .or_insert(claimer);
                assert_eq!(
                    *owner, claimer,
                    "a family was split across two live claimers"
                );
            }
        }
        for t in results.iter().flatten() {
            env.settle_as_worker(
                t.key.tenant_id.0,
                t.key.scope_id,
                t.stream_seq,
                "DONE",
                None,
            );
        }
    }
    assert_eq!(seen, all, "every ticket claimed exactly once");
}

/// Fairness: 3 tenants × 5 tickets, cap 2, limit 5 → rn 1 of every tenant, then rn 2 of two
/// tenants; no tenant above its cap. Fault injection: drop `rn <= p_per_tenant_cap` ⇒ one tenant
/// takes all five.
#[test]
fn claim_caps_each_tenant_per_call_and_round_robins_tenants() {
    let Some(mut env) = Env::new("claim_caps_each_tenant_per_call_and_round_robins_tenants") else {
        return;
    };
    let mut scopes = Vec::new();
    for _ in 0..3 {
        let t = env.tenant(true);
        let scope = Uuid::new_v4();
        for _ in 0..5 {
            env.ticket(t, scope);
        }
        env.close_distill(t);
        scopes.push((t, scope));
    }
    let claimed = env.claim(OWNER_A, 60.0, 5, 2);
    assert_eq!(claimed.len(), 5);
    let mut per_tenant: Vec<usize> = scopes
        .iter()
        .map(|(t, s)| seqs_of(&claimed, *t, *s).len())
        .collect();
    per_tenant.sort_unstable();
    assert_eq!(per_tenant, vec![1, 2, 2], "round robin under a cap of 2");
    for (t, s) in &scopes {
        let seqs = seqs_of(&claimed, *t, *s);
        assert_eq!(
            seqs,
            (1..=seqs.len() as i64).collect::<Vec<_>>(),
            "stream_seq order within a family"
        );
    }
}

/// A killed worker's ticket comes back: once its lease expires, another owner claims it and the
/// attempt counter moves to 2. Fault injection: delete the `lease_expires_at < clock_timestamp()`
/// arm ⇒ the ticket is never reclaimed.
#[test]
fn claim_reclaims_a_ticket_whose_lease_expired_and_bumps_attempts() {
    let Some(mut env) = Env::new("claim_reclaims_a_ticket_whose_lease_expired_and_bumps_attempts")
    else {
        return;
    };
    let t = env.tenant(true);
    let scope = Uuid::new_v4();
    let (seq, _) = env.ticket(t, scope);
    env.close_distill(t);
    // Leases here are whole seconds: under a loaded chain a sub-second lease expired before the
    // "still live" probe ran (chain 2026-09-29).
    let first = env.claim(OWNER_A, 3.0, 10, 10);
    assert_eq!(seqs_of(&first, t, scope), vec![seq]);
    assert_eq!(first[0].attempts, 1);
    assert!(
        env.claim(OWNER_B, 60.0, 10, 10).is_empty(),
        "live lease is not reclaimable"
    );
    std::thread::sleep(Duration::from_millis(3500));
    let second = env.claim(OWNER_B, 60.0, 10, 10);
    assert_eq!(seqs_of(&second, t, scope), vec![seq]);
    assert_eq!(second[0].attempts, 2);
    let row = env.row(t, scope, seq);
    assert_eq!(row.lease_owner.as_deref(), Some(OWNER_B));
    assert_eq!(row.state, "ISSUED");
}

/// D-A: while owner A holds a live lease on any ticket of a family, owner B gets none of that
/// family — not even the ones A did not take. Fault injection: drop the family-idle `NOT EXISTS`
/// ⇒ B gets seq 3/4 and two workers process one family concurrently.
#[test]
fn claim_never_hands_one_family_to_two_workers_while_a_lease_is_live() {
    let Some(mut env) =
        Env::new("claim_never_hands_one_family_to_two_workers_while_a_lease_is_live")
    else {
        return;
    };
    let t = env.tenant(true);
    // F sorts before G, so the tenant's first two tickets (rn 1, 2) are F's.
    let (x, y) = (Uuid::new_v4(), Uuid::new_v4());
    let (f, g) = (x.min(y), x.max(y));
    for _ in 0..4 {
        env.ticket(t, f);
    }
    env.ticket(t, g);
    env.close_distill(t);
    // cap 2: A gets the first two tickets of the tenant (family F, seq 1..2)
    let a = env.claim(OWNER_A, 60.0, 2, 2);
    assert_eq!(seqs_of(&a, t, f), vec![1, 2]);
    let b = env.claim(OWNER_B, 60.0, 10, 10);
    assert!(
        seqs_of(&b, t, f).is_empty(),
        "family F is held by A: B must get none of it, got {:?}",
        seqs_of(&b, t, f)
    );
    assert_eq!(seqs_of(&b, t, g), vec![1], "another family is free");
}

/// D-A's global lock: claimer X holds the claim lock inside an open transaction (it has leased
/// F's seq 1 but not committed); claimer Y starts meanwhile and must wait, then see X's lease and
/// take nothing of F. Fault injection: remove `pg_advisory_xact_lock` ⇒ Y's snapshot predates X's
/// commit, F looks idle, SKIP LOCKED passes over seq 1 and Y leases seq 2 — two workers, one
/// family.
#[test]
fn claim_serializes_racing_claimers_so_a_family_committed_meanwhile_stays_exclusive() {
    let Some(mut env) = Env::new(
        "claim_serializes_racing_claimers_so_a_family_committed_meanwhile_stays_exclusive",
    ) else {
        return;
    };
    let t = env.tenant(true);
    let f = Uuid::new_v4();
    for _ in 0..3 {
        env.ticket(t, f);
    }
    env.close_distill(t);
    let x_dsn = dsn_as_role(&env.admin_dsn, "role_retrieval_worker");
    let family = env.family.clone();
    // X runs on its own OS thread with a sync client: it claims inside an open transaction,
    // reports what it leased, and commits only when told to.
    let (claimed_tx, claimed_rx) = std::sync::mpsc::channel::<Vec<i64>>();
    let (commit_tx, commit_rx) = std::sync::mpsc::channel::<()>();
    let x_family = family.clone();
    let x = std::thread::spawn(move || {
        // dep: PostgreSQL(role_retrieval_worker) — claimer X, holding its claim transaction open
        let mut client = Client::connect(&x_dsn, NoTls).expect("X connects");
        let mut txn = client.transaction().expect("X begins");
        let seqs: Vec<i64> = txn
            .query(
                "SELECT stream_seq FROM projection.claim_issued_tickets($1,$2,$3,$4,$5,60,1,1)",
                &[
                    &x_family.domain,
                    &x_family.projection_kind,
                    &x_family.projection_version,
                    &x_family.placement.as_db_str(),
                    &OWNER_A,
                ],
            )
            .expect("X claims inside its transaction")
            .iter()
            .map(|row| row.get(0))
            .collect();
        claimed_tx.send(seqs).expect("report X's claim");
        commit_rx.recv().expect("commit signal");
        txn.commit().expect("X commits");
    });
    let x_seqs = claimed_rx.recv().expect("X claimed");
    let y_seqs = env.rt.block_on(async {
        let y = tokio::spawn({
            let family = family.clone();
            let y_dsn = dsn_as_role(&env.admin_dsn, "role_retrieval_worker");
            async move {
                // dep: PostgreSQL(role_retrieval_worker) — claimer Y, racing X
                let y_pool = RetrievalWorkerDbPool::connect(&y_dsn)
                    .await
                    .expect("Y connects");
                stream_repo::claim_issued(&y_pool, &family, OWNER_B, 60.0, 10, 10)
                    .await
                    .expect("Y claims")
                    .tickets
            }
        });
        tokio::time::sleep(Duration::from_millis(700)).await;
        commit_tx.send(()).expect("tell X to commit");
        y.await.expect("Y task")
    });
    x.join().expect("X thread");
    assert_eq!(x_seqs, vec![1]);
    assert!(
        seqs_of(&y_seqs, t, f).is_empty(),
        "Y claimed {:?} of a family X held",
        seqs_of(&y_seqs, t, f)
    );
}

/// A tenant with no placement row is never claimed and is counted by the unplaced function.
/// Fault injection: turn the placement JOIN into a LEFT JOIN ⇒ the unplaced ticket is claimed.
#[test]
fn claim_skips_tenants_without_a_placement_and_counts_them() {
    let Some(mut env) = Env::new("claim_skips_tenants_without_a_placement_and_counts_them") else {
        return;
    };
    let placed = env.tenant(true);
    let unplaced = env.tenant(false);
    let (ps, us) = (Uuid::new_v4(), Uuid::new_v4());
    env.ticket(placed, ps);
    env.ticket(unplaced, us);
    env.ticket(unplaced, us);
    env.close_distill(placed);
    env.close_distill(unplaced);
    let claimed = env.claim(OWNER_A, 60.0, 10, 10);
    assert_eq!(seqs_of(&claimed, placed, ps), vec![1]);
    assert!(seqs_of(&claimed, unplaced, us).is_empty());
    let counts = env
        .rt
        .block_on(stream_repo::unplaced_issued(&env.worker, &env.family))
        .expect("unplaced count");
    assert_eq!(counts, vec![(unplaced, 2)]);
}

/// ADR-0016 D6 in the claim: a ticket whose EVIDENCE_ACCEPTED row is still PENDING is not
/// claimed; once the distill closes it is. Fault injection: drop the distill-closed `NOT EXISTS`
/// ⇒ the open ticket is claimed and released as Pending on every poll.
#[test]
fn claim_skips_tickets_whose_distill_is_still_open() {
    let Some(mut env) = Env::new("claim_skips_tickets_whose_distill_is_still_open") else {
        return;
    };
    let t = env.tenant(true);
    let scope = Uuid::new_v4();
    let (seq, _) = env.ticket(t, scope);
    assert!(
        env.claim(OWNER_A, 60.0, 10, 10).is_empty(),
        "distill still open"
    );
    env.close_distill(t);
    assert_eq!(
        seqs_of(&env.claim(OWNER_A, 60.0, 10, 10), t, scope),
        vec![seq]
    );
}

/// The claim returns the tenant's placement row, parsed by the one existing parser.
#[test]
fn claim_returns_the_tenants_placement_row() {
    let Some(mut env) = Env::new("claim_returns_the_tenants_placement_row") else {
        return;
    };
    let t = env.tenant(true);
    let scope = Uuid::new_v4();
    env.ticket(t, scope);
    env.close_distill(t);
    let claimed = env.claim(OWNER_A, 60.0, 10, 10);
    assert_eq!(claimed.len(), 1);
    let p = &claimed[0].placement;
    assert_eq!(p.tenant_id, TenantId(t));
    assert_eq!(p.projection_family, RetrievalFamily::PrivateMemoryV1);
    assert_eq!(p.collection_name, "card27_claim_test");
    assert_eq!(p.shard_key, None);
    assert_eq!(
        claimed[0].key.projection_version,
        env.family.projection_version
    );
}

/// D-D retry: a transient failure returns the ticket to ISSUED with its lease cleared, the class
/// kept, and `next_attempt_at = now + base` for attempt 1 — the RETRY predicate — and not FAILED.
#[test]
fn transient_failure_returns_ticket_to_issued_with_backoff_and_is_not_failed() {
    let Some(mut env) =
        Env::new("transient_failure_returns_ticket_to_issued_with_backoff_and_is_not_failed")
    else {
        return;
    };
    let t = env.tenant(true);
    let scope = Uuid::new_v4();
    let (seq, _) = env.ticket(t, scope);
    env.close_distill(t);
    let claimed = env.claim(OWNER_A, 60.0, 10, 10);
    let ok = env
        .rt
        .block_on(stream_repo::release_for_retry(
            &env.worker,
            &claimed[0].key,
            seq,
            fence(Some(OWNER_A), 1),
            "qdrant_upsert_failed",
            Some(Backoff {
                base_secs: 30.0,
                max_secs: 300.0,
            }),
            true,
        ))
        .expect("release");
    assert!(ok);
    let row = env.row(t, scope, seq);
    assert_eq!(row.state, "ISSUED");
    assert_eq!(row.attempts, 1);
    assert_eq!(row.lease_owner, None);
    assert_eq!(row.error_class.as_deref(), Some("qdrant_upsert_failed"));
    assert!(row.retry && row.backing_off, "{row:?}");
    let secs = row.backoff_secs.expect("next_attempt_at set");
    assert!(
        (25.0..=30.5).contains(&secs),
        "attempt 1 waits base seconds, got {secs}"
    );
}

/// A backing-off ticket is not claimable before `next_attempt_at`; after it, it is, with
/// attempts 2, and the next backoff doubles. Fault injection: drop the `next_attempt_at` arm ⇒
/// the ticket is re-claimed at once (the ADR-0048 hot retry loop).
#[test]
fn retried_ticket_is_not_reclaimed_before_next_attempt_at() {
    let Some(mut env) = Env::new("retried_ticket_is_not_reclaimed_before_next_attempt_at") else {
        return;
    };
    let t = env.tenant(true);
    let scope = Uuid::new_v4();
    let (seq, _) = env.ticket(t, scope);
    env.close_distill(t);
    let backoff = Some(Backoff {
        base_secs: 30.0,
        max_secs: 300.0,
    });
    let key = env.claim(OWNER_A, 60.0, 10, 10)[0].key.clone();
    assert!(
        env.rt
            .block_on(stream_repo::release_for_retry(
                &env.worker,
                &key,
                seq,
                fence(Some(OWNER_A), 1),
                "embedding_failed",
                backoff,
                true,
            ))
            .expect("release")
    );
    assert!(
        env.claim(OWNER_B, 60.0, 10, 10).is_empty(),
        "still backing off"
    );
    env.admin
        .execute(
            "UPDATE projection.stream_log SET next_attempt_at = now() - interval '1 second' \
             WHERE tenant_id = $1 AND stream_seq = $2 AND projection_version = $3",
            &[&t, &seq, &env.family.projection_version],
        )
        .expect("elapse the backoff");
    let again = env.claim(OWNER_B, 60.0, 10, 10);
    assert_eq!(seqs_of(&again, t, scope), vec![seq]);
    assert_eq!(again[0].attempts, 2);
    assert!(
        env.rt
            .block_on(stream_repo::release_for_retry(
                &env.worker,
                &key,
                seq,
                fence(Some(OWNER_B), 2),
                "embedding_failed",
                backoff,
                true,
            ))
            .expect("release")
    );
    let secs = env.row(t, scope, seq).backoff_secs.expect("backoff");
    assert!(
        (55.0..=60.5).contains(&secs),
        "attempt 2 waits 2 × base, got {secs}"
    );
}

/// The pass's exhaustion rules, end to end through [`run_claimed_pass`]: (a) a transient failure
/// on the attempt that reaches `max_attempts` settles FAILED `transient_exhausted`; (b) a ticket
/// claimed past `max_attempts` (a worker that dies on it every time) is settled the same way
/// before any processing. Fault injection: drop the `attempts >= max_attempts` arm ⇒ (a) stays
/// ISSUED with a backoff forever.
#[test]
fn transient_at_max_attempts_settles_failed_transient_exhausted() {
    let Some(mut env) = Env::new("transient_at_max_attempts_settles_failed_transient_exhausted")
    else {
        return;
    };
    let Some(shared) = pass_deps(
        &env,
        "transient_at_max_attempts_settles_failed_transient_exhausted",
    ) else {
        return;
    };
    let t = env.tenant(true);
    let (a_scope, b_scope) = (Uuid::new_v4(), Uuid::new_v4());
    let (a, a_ev) = env.ticket(t, a_scope);
    let (b, _) = env.ticket(t, b_scope);
    attach_memory(&mut env, t, a_ev);
    env.close_distill(t);
    // (a) will be claimed at attempts 3 = max; (b) at 4 > max.
    env.admin
        .execute(
            "UPDATE projection.stream_log SET attempts = CASE scope_id WHEN $2 THEN 2 ELSE 3 END \
             WHERE tenant_id = $1 AND projection_version = $3",
            &[&t, &a_scope, &env.family.projection_version],
        )
        .expect("spend attempts");
    shared
        .provider
        .force_next_error(ErrorCode::ProviderTransient);
    let outcome = env
        .rt
        .block_on(run_claimed_pass(&shared.deps, &pass_config(&env, 3)))
        .expect("pass");
    assert_eq!((outcome.claimed, outcome.failed), (2, 2), "{outcome:?}");
    for (scope, seq) in [(a_scope, a), (b_scope, b)] {
        let row = env.row(t, scope, seq);
        assert_eq!(row.state, "FAILED", "{row:?}");
        assert_eq!(row.error_class.as_deref(), Some("transient_exhausted"));
        assert_eq!(row.lease_owner, None);
    }
}

/// Review 2026-09-29 P0: a family processed after `lease_secs` into the pass keeps its lease.
/// Family F (sorts first) holds one ticket whose embed call takes 3 s (then fails permanently);
/// family G's ticket waits behind it under a 1.5 s lease. The pass's background heartbeat renews
/// G while F is worked, so G is processed (its Evidence distilled to nothing ⇒ SKIPPED) and
/// nothing is lost. Fault injection: drop the heartbeat ⇒ G's per-ticket renew finds the lease
/// expired, `lost_lease = 1`, and G is left leased to expire and be re-claimed (attempts 2) —
/// the path that burned the tail of every slow batch to `transient_exhausted`.
#[test]
fn a_family_waiting_behind_a_slow_one_keeps_its_lease() {
    const TEST: &str = "a_family_waiting_behind_a_slow_one_keeps_its_lease";
    let Some(mut env) = Env::new(TEST) else {
        return;
    };
    let Some(shared) = pass_deps(&env, TEST) else {
        return;
    };
    let t = env.tenant(true);
    let (x, y) = (Uuid::new_v4(), Uuid::new_v4());
    let (f, g) = (x.min(y), x.max(y));
    let (f_seq, f_ev) = env.ticket(t, f);
    let (g_seq, _) = env.ticket(t, g);
    attach_memory(&mut env, t, f_ev);
    env.close_distill(t);
    shared.embed_delay_ms.store(3_000, Ordering::Relaxed);
    shared
        .provider
        .force_next_error(ErrorCode::ProviderPermanent);
    let outcome = env
        .rt
        .block_on(run_claimed_pass(
            &shared.deps,
            &pass_config_leased(&env, 6, 1.5),
        ))
        .expect("pass");
    assert_eq!(
        (
            outcome.claimed,
            outcome.failed,
            outcome.skipped,
            outcome.lost_lease
        ),
        (2, 1, 1, 0),
        "{outcome:?}"
    );
    let f_row = env.row(t, f, f_seq);
    assert_eq!(f_row.state, "FAILED", "{f_row:?}");
    let g_row = env.row(t, g, g_seq);
    assert_eq!(
        (
            g_row.state.as_str(),
            g_row.attempts,
            g_row.lease_owner.as_deref()
        ),
        ("SKIPPED_BY_POLICY", 1, None),
        "{g_row:?}"
    );
}

/// Review 2026-09-29 P1 (dependency-level breaker): with Qdrant down (a closed loopback port),
/// the first outage failure is charged (attempts 1) and every further one while no ticket has
/// reached DONE is not — a ticket that arrives at `max_attempts` during the outage stays ISSUED,
/// backing off, instead of `transient_exhausted`. Fault injection: charge every outage failure
/// (drop the `refund` arm) ⇒ the second ticket is FAILED `transient_exhausted`.
#[test]
fn outage_failures_after_the_first_spend_no_attempt() {
    const TEST: &str = "outage_failures_after_the_first_spend_no_attempt";
    let Some(mut env) = Env::new(TEST) else {
        return;
    };
    let Some(shared) = pass_deps(&env, TEST) else {
        return;
    };
    let t = env.tenant(true);
    let (x, y) = (Uuid::new_v4(), Uuid::new_v4());
    let (first, second) = (x.min(y), x.max(y));
    let (a, a_ev) = env.ticket(t, first);
    let (b, b_ev) = env.ticket(t, second);
    attach_memory(&mut env, t, a_ev);
    attach_memory(&mut env, t, b_ev);
    env.close_distill(t);
    // The second family's ticket is claimed at attempts 6 = max_attempts.
    env.admin
        .execute(
            "UPDATE projection.stream_log SET attempts = 5 \
             WHERE tenant_id = $1 AND scope_id = $2 AND projection_version = $3",
            &[&t, &second, &env.family.projection_version],
        )
        .expect("spend attempts");
    let outcome = env
        .rt
        .block_on(run_claimed_pass(&shared.deps, &pass_config(&env, 6)))
        .expect("pass");
    assert_eq!(
        (
            outcome.claimed,
            outcome.retried,
            outcome.refunded,
            outcome.failed
        ),
        (2, 2, 1, 0),
        "{outcome:?}"
    );
    let a_row = env.row(t, first, a);
    assert_eq!(
        (a_row.state.as_str(), a_row.attempts),
        ("ISSUED", 1),
        "{a_row:?}"
    );
    assert!(a_row.retry, "{a_row:?}");
    let b_row = env.row(t, second, b);
    assert_eq!(
        (b_row.state.as_str(), b_row.attempts),
        ("ISSUED", 5),
        "{b_row:?}"
    );
    assert_eq!(b_row.error_class.as_deref(), Some("qdrant_upsert_failed"));
    assert!(b_row.retry && b_row.backing_off, "{b_row:?}");
    assert!(shared.deps.dependency_down.load(Ordering::Relaxed));
}

/// A permanent failure settles FAILED with its own class on the first attempt, through the pass.
#[test]
fn permanent_failure_settles_failed_immediately_with_its_class() {
    let Some(mut env) = Env::new("permanent_failure_settles_failed_immediately_with_its_class")
    else {
        return;
    };
    let Some(shared) = pass_deps(
        &env,
        "permanent_failure_settles_failed_immediately_with_its_class",
    ) else {
        return;
    };
    let t = env.tenant(true);
    let scope = Uuid::new_v4();
    let (seq, ev) = env.ticket(t, scope);
    attach_memory(&mut env, t, ev);
    env.close_distill(t);
    shared
        .provider
        .force_next_error(ErrorCode::ProviderPermanent);
    let outcome = env
        .rt
        .block_on(run_claimed_pass(&shared.deps, &pass_config(&env, 6)))
        .expect("pass");
    assert_eq!(
        (outcome.claimed, outcome.failed, outcome.retried),
        (1, 1, 0)
    );
    let row = env.row(t, scope, seq);
    assert_eq!(row.state, "FAILED");
    assert_eq!(row.attempts, 1);
    assert_eq!(row.error_class.as_deref(), Some("embedding_rejected"));
}

/// D-D pending release: the lease is cleared and the claim's attempt is given back.
#[test]
fn pending_release_does_not_consume_an_attempt() {
    let Some(mut env) = Env::new("pending_release_does_not_consume_an_attempt") else {
        return;
    };
    let t = env.tenant(true);
    let scope = Uuid::new_v4();
    let (seq, _) = env.ticket(t, scope);
    env.close_distill(t);
    let key = env.claim(OWNER_A, 60.0, 10, 10)[0].key.clone();
    assert_eq!(env.row(t, scope, seq).attempts, 1);
    assert!(
        env.rt
            .block_on(stream_repo::release_pending(
                &env.worker,
                &key,
                seq,
                fence(Some(OWNER_A), 1)
            ))
            .expect("release")
    );
    let row = env.row(t, scope, seq);
    assert_eq!(
        (row.attempts, row.lease_owner, row.backing_off),
        (0, None, false)
    );
}

/// D-D fence: once A's lease expired and B re-claimed (attempts 2), every write A still makes with
/// its old fence matches nothing — no retry, no release, no heartbeat — and B's lease survives.
/// Fault injection: drop `attempts = $n` from the fence ⇒ A's stale retry clears B's lease.
#[test]
fn settle_after_the_lease_was_reclaimed_is_a_no_op() {
    let Some(mut env) = Env::new("settle_after_the_lease_was_reclaimed_is_a_no_op") else {
        return;
    };
    let t = env.tenant(true);
    let scope = Uuid::new_v4();
    let (seq, _) = env.ticket(t, scope);
    env.close_distill(t);
    let key = env.claim(OWNER_A, 0.5, 10, 10)[0].key.clone();
    std::thread::sleep(Duration::from_millis(800));
    assert_eq!(env.claim(OWNER_B, 60.0, 10, 10)[0].attempts, 2);
    let stale = fence(Some(OWNER_A), 1);
    let retried = env
        .rt
        .block_on(stream_repo::release_for_retry(
            &env.worker,
            &key,
            seq,
            stale,
            "qdrant_upsert_failed",
            None,
            true,
        ))
        .expect("stale retry");
    let released = env
        .rt
        .block_on(stream_repo::release_pending(&env.worker, &key, seq, stale))
        .expect("stale release");
    let renewed = env
        .rt
        .block_on(stream_repo::renew_family_leases(
            &env.worker,
            &key,
            OWNER_A,
            60.0,
        ))
        .expect("stale heartbeat");
    assert!(!retried && !released && renewed.is_empty());
    let row = env.row(t, scope, seq);
    assert_eq!(row.lease_owner.as_deref(), Some(OWNER_B));
    assert_eq!((row.attempts, row.state.as_str()), (2, "ISSUED"));
}

/// ADR-0043 heartbeat: renews every live lease the owner holds on that family, and nothing of
/// another owner or another family, and nothing once expired.
#[test]
fn heartbeat_renews_only_the_owners_live_family_leases() {
    let Some(mut env) = Env::new("heartbeat_renews_only_the_owners_live_family_leases") else {
        return;
    };
    let t = env.tenant(true);
    // F sorts before G, so the claim's rn 1, 2 are F's two tickets.
    let (x, y) = (Uuid::new_v4(), Uuid::new_v4());
    let (f, g) = (x.min(y), x.max(y));
    env.ticket(t, f);
    env.ticket(t, f);
    env.ticket(t, g);
    env.close_distill(t);
    let a = env.claim(OWNER_A, 3.0, 2, 2);
    let f_key = a[0].key.clone();
    assert_eq!(seqs_of(&a, t, f), vec![1, 2]);
    let b = env.claim(OWNER_B, 60.0, 10, 10);
    let g_key = b[0].key.clone();
    let renew = |key, owner| {
        env.rt
            .block_on(stream_repo::renew_family_leases(
                &env.worker,
                key,
                owner,
                2.0,
            ))
            .expect("renew")
    };
    let mut renewed = renew(&f_key, OWNER_A);
    renewed.sort_unstable();
    assert_eq!(renewed, vec![1, 2]);
    assert!(renew(&f_key, OWNER_B).is_empty(), "B holds nothing of F");
    assert!(renew(&g_key, OWNER_A).is_empty(), "A holds nothing of G");
    std::thread::sleep(Duration::from_millis(2500));
    assert!(
        renew(&f_key, OWNER_A).is_empty(),
        "an expired lease is not renewed"
    );
}

/// 0176's consequence, pinned: with the owner arm on stream_log, retire_failed_ticket must still
/// refuse a tenant other than the one the caller installed. Fault injection: drop the explicit
/// tenant predicate 0176 added ⇒ the tenant-A session retires tenant B's ticket.
#[test]
fn retire_failed_ticket_still_refuses_a_tenant_other_than_the_installed_one() {
    let Some(mut env) =
        Env::new("retire_failed_ticket_still_refuses_a_tenant_other_than_the_installed_one")
    else {
        return;
    };
    let a = env.tenant(true);
    let b = env.tenant(true);
    let scope = Uuid::new_v4();
    let (seq, _) = env.ticket(b, scope);
    env.settle_as_worker(b, scope, seq, "FAILED", Some("distill_failed"));
    // dep: PostgreSQL(role_maintenance) — the retirement door's only executor
    let mut maint = Client::connect(&dsn_as_role(&env.admin_dsn, "role_maintenance"), NoTls)
        .expect("role_maintenance connects");
    let version = env.family.projection_version.clone();
    let retire = |client: &mut Client, installed: Uuid| -> i64 {
        let mut tx = client.transaction().expect("txn");
        tx.execute(&format!("SET LOCAL humaux.tenant_id = '{installed}'"), &[])
            .expect("install tenant");
        let n: i64 = tx
            .query_one(
                "SELECT projection.retire_failed_ticket($1,'workspace',$2,'private_memory', \
                   'PRIVATE_MEMORY',$3,$4,'distill_failed')",
                &[&b, &scope, &version, &seq],
            )
            .expect("call")
            .get(0);
        tx.commit().expect("commit");
        n
    };
    assert_eq!(
        retire(&mut maint, a),
        0,
        "a tenant-A session retires nothing of tenant B"
    );
    assert_eq!(env.row(b, scope, seq).state, "FAILED");
    assert_eq!(retire(&mut maint, b), 1, "the installed tenant still can");
    assert_eq!(env.row(b, scope, seq).state, "RETIRED_FAILED");
}

/// §6.2.2: EXECUTE on both 0176 definers is role_retrieval_worker's alone, and a runtime role
/// without it is refused (42501) by the server, not by convention.
#[test]
fn claim_execute_is_granted_to_role_retrieval_worker_only() {
    let Some(mut env) = Env::new("claim_execute_is_granted_to_role_retrieval_worker_only") else {
        return;
    };
    for f in [
        "projection.claim_issued_tickets(text,text,text,text,text,double precision,bigint,bigint)",
        "projection.unplaced_issued_tickets(text,text,text,text)",
    ] {
        for role in [
            "role_gateway",
            "role_private_worker",
            "role_consolidation_worker",
            "role_public_worker",
            "role_retrieval_worker",
            "role_batch_issuer",
            "role_maintenance",
            "role_admin",
        ] {
            let can: bool = env
                .admin
                .query_one(
                    "SELECT has_function_privilege($1, $2, 'EXECUTE')",
                    &[&role, &f],
                )
                .expect("privilege")
                .get(0);
            assert_eq!(can, role == "role_retrieval_worker", "{role} on {f}");
        }
    }
    // dep: PostgreSQL(role_gateway) — a runtime role WITHOUT the grant tries the claim
    let mut gateway = Client::connect(&dsn_as_role(&env.admin_dsn, "role_gateway"), NoTls)
        .expect("role_gateway connects");
    let err = gateway
        .query(
            "SELECT * FROM projection.claim_issued_tickets('private_memory','PRIVATE_MEMORY',$1, \
               'private_memory_v1','x',60,1,1)",
            &[&env.family.projection_version],
        )
        .expect_err("role_gateway has no EXECUTE");
    assert_eq!(err.code().map(|c| c.code()), Some("42501"), "{err}");
}

/// Argument validation (22023) and the READ COMMITTED requirement (25000) — the latter is what
/// keeps the global lock's fresh-snapshot argument true.
#[test]
fn claim_refuses_invalid_arguments_and_non_read_committed() {
    let Some(env) = Env::new("claim_refuses_invalid_arguments_and_non_read_committed") else {
        return;
    };
    // dep: PostgreSQL(role_retrieval_worker) — direct calls of the claim with bad arguments
    let mut worker = Client::connect(&dsn_as_role(&env.admin_dsn, "role_retrieval_worker"), NoTls)
        .expect("role_retrieval_worker connects");
    let v = env.family.projection_version.clone();
    for (owner, lease, limit, cap) in [
        ("x", 60.0_f64, 0_i64, 1_i64),
        ("x", 60.0, 1, 0),
        ("x", 0.0, 1, 1),
        ("  ", 60.0, 1, 1),
    ] {
        let err = worker
            .query(
                "SELECT * FROM projection.claim_issued_tickets('private_memory','PRIVATE_MEMORY',$1, \
                   'private_memory_v1',$2,$3,$4,$5)",
                &[&v, &owner, &lease, &limit, &cap],
            )
            .expect_err("invalid argument");
        assert_eq!(
            err.code().map(|c| c.code()),
            Some("22023"),
            "{owner:?} {lease} {limit} {cap}: {err}"
        );
    }
    // dep: PostgreSQL(role_retrieval_worker) — the claim inside a REPEATABLE READ transaction
    let mut tx = worker
        .build_transaction()
        .isolation_level(postgres::IsolationLevel::RepeatableRead)
        .start()
        .expect("repeatable read txn");
    let err = tx
        .query(
            "SELECT * FROM projection.claim_issued_tickets('private_memory','PRIVATE_MEMORY',$1, \
               'private_memory_v1','x',60,1,1)",
            &[&v],
        )
        .expect_err("REPEATABLE READ is refused");
    assert_eq!(err.code().map(|c| c.code()), Some("25000"), "{err}");
}

// ---- run_claimed_pass fixture -------------------------------------------------------------

/// Forwards to a [`TestDoubleProvider`] so a test can force its next error, after an optional
/// delay (ms) a test sets to make one ticket slow.
struct Embedder(Arc<TestDoubleProvider>, Arc<AtomicU64>);

#[async_trait]
impl CardEmbedder for Embedder {
    async fn embed_cards(
        &self,
        tenant_id: TenantId,
        dimension: u32,
        cards: &[SealedRetrievalCard],
        _memory_ids: &[Uuid],
    ) -> Result<Vec<Vec<f32>>, ErrorCode> {
        let delay = self.1.load(Ordering::Relaxed);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        Ok(self
            .0
            .embed_cards(tenant_id, dimension, cards)
            .await?
            .vectors)
    }
}

struct PassDeps {
    deps: SharedProjectionDeps,
    provider: Arc<TestDoubleProvider>,
    /// Milliseconds every embed call waits before answering.
    embed_delay_ms: Arc<AtomicU64>,
}

/// Shared deps for a pass whose tickets fail before Qdrant: the embedder is a test double whose
/// next error the test forces, and the Qdrant registry points at a closed loopback port. The
/// scanner needs the pinned gitleaks triple (visible SKIP without it).
fn pass_deps(env: &Env, test: &str) -> Option<PassDeps> {
    let (Ok(bin), Ok(version), Ok(sha)) = (
        std::env::var("HUMAUX_TEST_GITLEAKS_BIN"),
        std::env::var("HUMAUX_TEST_GITLEAKS_VERSION"),
        std::env::var("HUMAUX_TEST_GITLEAKS_SHA256"),
    ) else {
        skip_or_fail(
            test,
            "missing object: HUMAUX_TEST_GITLEAKS_{BIN,VERSION,SHA256}",
            ExternalDep::Postgres,
        );
        return None;
    };
    // dep: subprocess(gitleaks) — the scanner verifies the pinned binary at construction
    let scanner = LocalSecretScanner::new(LocalSecretScannerConfig {
        executable: bin.into(),
        expected_version: version,
        expected_executable_sha256: sha,
        timeout: Duration::from_secs(5),
        max_payload_bytes: 64 * 1024,
        finding_exit_code: 1,
    })
    .expect("pinned gitleaks");
    let cell = CellId(Uuid::new_v4());
    let caller = CallerId("projection_claim.rs".to_owned());
    let dead = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("reserve a port")
        .port();
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            "127.0.0.1",
            dead,
            cell,
            vec!["127.0.0.1/32".parse().expect("cidr")],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .expect("entry"),
    );
    let registry = IntraCellResourceRegistry::new(entries, cell, caller);
    let transport = Arc::new(
        HttpIntraCellTransport::new(
            registry.clone(),
            Duration::from_secs(2),
            humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
        )
        .expect("transport"),
    );
    let provider = Arc::new(TestDoubleProvider::new(
        EmbeddingModelDescriptor {
            model_id: ModelId("card27-claim-test".to_owned()),
            model_revision: "r1".to_owned(),
            dimension_options: vec![4],
            max_input_tokens: 4_096,
            batch_supported: true,
            dense_supported: true,
            sparse_supported: false,
        },
        RerankModelDescriptor {
            model_id: ModelId("card27-unused".to_owned()),
            model_revision: "r1".to_owned(),
            max_documents: 5,
            max_input_tokens: 4_096,
            score_semantics: RerankScoreSemantics::RawLogit,
            calibration_profile: CalibrationProfileId("unused".to_owned()),
        },
    ));
    let pool = env
        .rt
        // dep: PostgreSQL(role_retrieval_worker) — the pass's own pool
        .block_on(RetrievalWorkerDbPool::connect(&dsn_as_role(
            &env.admin_dsn,
            "role_retrieval_worker",
        )))
        .expect("pass pool");
    let embed_delay_ms = Arc::new(AtomicU64::new(0));
    Some(PassDeps {
        deps: SharedProjectionDeps {
            pool,
            embedder: Arc::new(Embedder(provider.clone(), embed_delay_ms.clone())),
            scanner: Arc::new(scanner),
            transport,
            mint_permit: Arc::new(move || {
                humaux_infra_cell::authorize_cell_access(
                    &registry,
                    IntraCellResource::QDRANT_REST,
                    Duration::from_secs(30),
                )
                .ok()
            }),
            embedding_version: "card27-test@r1".to_owned(),
            dimension: 4,
            processor_id: ProcessorId(Uuid::from_u128(0x0c27)),
            sleep: Arc::new(|period| Box::pin(tokio::time::sleep(period))),
            dependency_down: AtomicBool::new(false),
        },
        provider,
        embed_delay_ms,
    })
}

fn pass_config(env: &Env, max_attempts: i32) -> PassConfig {
    pass_config_leased(env, max_attempts, 60.0)
}

fn pass_config_leased(env: &Env, max_attempts: i32, lease_secs: f64) -> PassConfig {
    PassConfig {
        claim: env.family.clone(),
        lease_owner: OWNER_A.to_owned(),
        lease_secs,
        batch: 16,
        per_tenant_cap: 8,
        max_attempts,
        backoff: Backoff {
            base_secs: 30.0,
            max_secs: 300.0,
        },
    }
}

/// A live memory on the ticket's Evidence, so the pass reaches the embedding call.
fn attach_memory(env: &mut Env, tenant: Uuid, evidence: Uuid) {
    let mut txn = env.admin.transaction().expect("txn");
    let memory: Uuid = txn
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id, memory_type, content, visibility_class, authority_class, confidence, \
                status, asserted_at) \
             VALUES ($1,'NOTE',$2,'TENANT_SHARED','PrivateKnowledge',0.9,'active',now()) \
             RETURNING memory_id",
            &[
                &tenant,
                &serde_json::json!({
                    "title": "card27 claim fixture",
                    "key_claim": "a claimed ticket reaches the embedder",
                    "evidence_excerpt": "card27 claim fixture",
                }),
            ],
        )
        .expect("memory")
        .get(0);
    // dep: PostgreSQL(any) — link the fixture memory to its Evidence
    txn.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
         VALUES ($1, $2, 'PRIMARY', 0)",
        &[&memory, &evidence],
    )
    .expect("memory_evidence");
    txn.commit().expect("commit");
}
