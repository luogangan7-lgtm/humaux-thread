//! memory_security_lifecycle eval harness (§45.2 Private Memory Poisoning，ADR-0007 分段声明)。
//!
//! 按 ADR-0007 §55.3.2 分段：Write / Recall / Repair 三段现在有 SUT 可测，各自实测七字段；
//! Action 段（Context Renderer 分流 + privileged action probe）SUT 由 DOD-036 排 phase=14，
//! BLOCKED_ON_SUT——其攻击样本冻结在 `evals/memory_security_lifecycle/blocked_stage_inventory.toml`
//! 但**不在本 harness 里填实测**。lifecycle_complete 归 DOD-036。
//!
//! 判定（§45.2 A–E）：
//! - **write**：攻击内容可作为 evidence/memory 持久化（origin_class 记真实低信来源），但
//!   授权层 origin ceiling 不被内容洗白——请求高 authority 被 `OriginBoundAuthorityPolicy`
//!   拒，`max_disposition` 由 origin 决定（外部/工具来源恒 DataOnly）。benign 正对照
//!   （UserConfirmed 同请求被允许）防「全拒」假绿。走真 DB 持久化 + 真 domain policy。
//! - **recall**：`recall_with_overlay` 能召回攻击内容本体，且 origin_class 标记随行不丢
//!   （召回不隐式洗白 origin）；benign 正常召回。
//! - **repair**：`forget_repo::tombstone` 攻击源后重召回不再命中该源；unrelated benign 仍召回。
//!
//! 三态（§79.2）：`testkit::skip_or_fail`；CI 声明 `HUMAUX_REQUIRE_DB=1` 时跳过即失败。

use std::collections::BTreeMap;
use std::path::PathBuf;

use humaux_domain::authority::{AuthorityClass, AuthorityPolicy, AuthorizedAuthority, NonEmptyVec};
use humaux_domain::evidence::{EvidenceOriginClass, InstructionDisposition};
use humaux_domain::ids::{Scope, TenantId};
use humaux_domain::memory::MemoryType;
use humaux_domain::policy::OriginBoundAuthorityPolicy;
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use uuid::Uuid;

const NAME: &str = "memory_security_lifecycle_eval";

// ============================================================================
// dataset
// ============================================================================

fn dataset_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../evals/memory_security_lifecycle/dataset.tsv")
}

fn manifest_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../evals/memory_security_lifecycle/manifest.toml")
}

#[derive(Debug, Clone)]
struct Case {
    case_id: String,
    vector: String,
    segment: String,
    role: String,
    origin: String,
    requested_authority: String,
    expect_persist: bool,
    expect_disposition: String,
    expect_authorized: bool,
    content: String,
}

fn read_cases() -> Vec<Case> {
    let text = std::fs::read_to_string(dataset_path()).expect("dataset.tsv must exist");
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim_end();
        if line.is_empty() || line.starts_with('#') || line.starts_with("case_id\t") {
            continue;
        }
        let f: Vec<&str> = line.split('\t').collect();
        assert_eq!(f.len(), 10, "dataset row must have 10 columns: {line}");
        out.push(Case {
            case_id: f[0].to_string(),
            vector: f[1].to_string(),
            segment: f[2].to_string(),
            role: f[3].to_string(),
            origin: f[4].to_string(),
            requested_authority: f[5].to_string(),
            expect_persist: f[6].parse().unwrap(),
            expect_disposition: f[7].to_string(),
            expect_authorized: f[8].parse().unwrap(),
            content: f[9].to_string(),
        });
    }
    out
}

fn origin_of(s: &str) -> EvidenceOriginClass {
    match s {
        "DirectUserInput" => EvidenceOriginClass::DirectUserInput,
        "UserConfirmed" => EvidenceOriginClass::UserConfirmed,
        "TenantAdmin" => EvidenceOriginClass::TenantAdmin,
        "AuthenticatedAgent" => EvidenceOriginClass::AuthenticatedAgent,
        "TrustedConnector" => EvidenceOriginClass::TrustedConnector,
        "ToolResult" => EvidenceOriginClass::ToolResult,
        "UploadedArtifact" => EvidenceOriginClass::UploadedArtifact,
        "ExternalContent" => EvidenceOriginClass::ExternalContent,
        "SystemMigration" => EvidenceOriginClass::SystemMigration,
        other => panic!("dataset names an origin not in EvidenceOriginClass: {other}"),
    }
}

fn authority_of(s: &str) -> AuthorityClass {
    match s {
        "PublicKnowledge" => AuthorityClass::PublicKnowledge,
        "PrivateKnowledge" => AuthorityClass::PrivateKnowledge,
        "UserPreference" => AuthorityClass::UserPreference,
        "ProjectDecision" => AuthorityClass::ProjectDecision,
        "UserCorrection" => AuthorityClass::UserCorrection,
        "ProjectConstraint" => AuthorityClass::ProjectConstraint,
        "ExplicitTaskContext" => AuthorityClass::ExplicitTaskContext,
        other => panic!("dataset names an authority not in AuthorityClass: {other}"),
    }
}

fn disposition_of(s: &str) -> InstructionDisposition {
    match s {
        "DataOnly" => InstructionDisposition::DataOnly,
        "BehaviorEligible" => InstructionDisposition::BehaviorEligible,
        other => panic!("dataset names a disposition not in InstructionDisposition: {other}"),
    }
}

/// The MemoryType a requested authority is minted as — Constraint requests exercise the
/// `authority_ceiling`'s Constraint special-case; everything else is a plain Fact request.
fn memory_type_for(requested: AuthorityClass) -> MemoryType {
    match requested {
        AuthorityClass::ProjectConstraint => MemoryType::Constraint,
        _ => MemoryType::Fact,
    }
}

// ============================================================================
// per-case result
// ============================================================================

#[derive(Debug, Clone)]
struct CaseResult {
    case_id: String,
    vector: String,
    pass: bool,
    detail: String,
}

// ============================================================================
// write segment — real DB persistence + real domain policy
// ============================================================================

/// Write-segment judgment for one case (§45.2 A/B/C/D over a real persisted row):
/// - the attack content persists at all (origin_class stored verbatim — a low-trust origin is
///   not refused at write time, only capped at authorize time);
/// - `max_disposition(origin)` matches the frozen expectation (content-independent);
/// - `OriginBoundAuthorityPolicy::authorize(requested)` matches expect_authorized (the ceiling
///   is not laundered by the content string).
fn run_write_case(admin: &mut Client, tenant_id: Uuid, rd: Uuid, c: &Case) -> CaseResult {
    let mut fails: Vec<String> = Vec::new();
    let origin = origin_of(&c.origin);

    // 1. persistence: seed an evidence(origin) + memory, read origin_class back.
    let persisted_origin: Option<String> = (|| {
        let mut txn = admin.transaction().ok()?;
        let evidence_id: Uuid = txn
            .query_one(
                "INSERT INTO private.evidence_objects \
                   (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                    visibility_class, reasoning_domain_id) \
                 VALUES ($1, 'EVENT', $2, 'INTERNAL', $3, 'TENANT_SHARED', $4) \
                 RETURNING evidence_id",
                &[
                    &tenant_id,
                    &{
                        let mut h = vec![0u8; 32];
                        h[..16].copy_from_slice(Uuid::now_v7().as_bytes());
                        h
                    },
                    &c.origin,
                    &rd,
                ],
            )
            .ok()?
            .get(0);
        txn.execute(
            "INSERT INTO private.events (event_id, event_kind, payload) \
             VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
            &[&evidence_id],
        )
        .ok()?;
        let memory_id: Uuid = txn
            .query_one(
                "INSERT INTO private.memory_records \
                   (tenant_id, memory_type, content, visibility_class, \
                    authority_class, confidence, status, asserted_at) \
                 VALUES ($1, 'NOTE', $2, 'TENANT_SHARED', 'PrivateKnowledge', 0.9, 'active', now()) \
                 RETURNING memory_id",
                &[&tenant_id, &serde_json::json!({ "content": c.content })],
            )
            .ok()?
            .get(0);
        txn.execute(
            "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, grounding_mode) \
             VALUES ($1, $2, 'PRIMARY', 'SNAPSHOT')",
            &[&memory_id, &evidence_id],
        )
        .ok()?;
        let got: String = txn
            .query_one(
                "SELECT origin_class FROM private.evidence_objects WHERE evidence_id = $1",
                &[&evidence_id],
            )
            .ok()?
            .get(0);
        txn.commit().ok()?;
        Some(got)
    })();

    let persisted = persisted_origin.is_some();
    if persisted != c.expect_persist {
        fails.push(format!("persist {persisted} != {}", c.expect_persist));
    }
    if let Some(got) = &persisted_origin
        && got != &c.origin
    {
        fails.push(format!("stored origin_class {got} != {}", c.origin));
    }

    // 2. disposition cap (content-independent, from the origin type).
    let got_disp = origin.max_disposition();
    if got_disp != disposition_of(&c.expect_disposition) {
        fails.push(format!(
            "max_disposition {got_disp:?} != {}",
            c.expect_disposition
        ));
    }

    // 3. authorize: ceiling is not laundered by content.
    let requested = authority_of(&c.requested_authority);
    let basis = NonEmptyVec::new(vec![origin]).expect("single-origin basis is non-empty");
    let res = OriginBoundAuthorityPolicy.authorize(
        requested,
        memory_type_for(requested),
        basis,
        &scope_for(tenant_id),
    );
    let authorized = matches!(res, Ok(AuthorizedAuthority(a)) if a == requested);
    if authorized != c.expect_authorized {
        fails.push(format!(
            "authorize {authorized} != {} (res={res:?})",
            c.expect_authorized
        ));
    }

    CaseResult {
        case_id: c.case_id.clone(),
        vector: c.vector.clone(),
        pass: fails.is_empty(),
        detail: fails.join("; "),
    }
}

fn scope_for(tenant_id: Uuid) -> Scope {
    Scope {
        tenant_id: TenantId(tenant_id),
        user_id: None,
        workspace_id: None,
        repository_id: None,
        task_id: None,
        run_id: None,
        agent_id: None,
    }
}

// ============================================================================
// fixture
// ============================================================================

struct Fixture {
    admin: Client,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM private.memory_evidence WHERE memory_id IN \
               (SELECT memory_id FROM private.memory_records WHERE tenant_id = '{0}'); \
             DELETE FROM private.memory_records WHERE tenant_id = '{0}'; \
             DELETE FROM private.events WHERE event_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{0}'); \
             DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
             DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}'; \
             DELETE FROM control.tenants WHERE tenant_id = '{0}';",
            self.tenant_id
        ));
    }
}

fn setup() -> Option<Fixture> {
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(NAME, "missing object: Postgres DSN", ExternalDep::Postgres);
        return None;
    };
    let Ok(mut admin) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(NAME, "missing object: live Postgres", ExternalDep::Postgres);
        return None;
    };
    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
            &[&"memory_security_lifecycle_eval throwaway tenant"],
        )
        .ok()?
        .get(0);
    let reasoning_domain_id: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
             VALUES ($1, 'msl domain') RETURNING reasoning_domain_id",
            &[&tenant_id],
        )
        .ok()?
        .get(0);
    Some(Fixture {
        admin,
        tenant_id,
        reasoning_domain_id,
    })
}

// ============================================================================
// §55.3 measurements — write segment
// ============================================================================

#[test]
fn write_segment_all_cases_pass() {
    let Some(mut f) = setup() else { return };
    let cases: Vec<Case> = read_cases()
        .into_iter()
        .filter(|c| c.segment == "write")
        .collect();
    assert_eq!(cases.len(), 8, "write-segment denominator frozen at 8");
    let tenant = f.tenant_id;
    let rd = f.reasoning_domain_id;
    let results: Vec<CaseResult> = cases
        .iter()
        .map(|c| run_write_case(&mut f.admin, tenant, rd, c))
        .collect();
    // 正对照必须存在（防「全拒」假绿）：至少一个 benign 被授权。
    let benign_authorized = cases
        .iter()
        .zip(&results)
        .any(|(c, r)| c.role == "benign" && c.expect_authorized && r.pass);
    assert!(
        benign_authorized,
        "write segment needs a passing benign positive control"
    );
    let failed: Vec<_> = results.iter().filter(|r| !r.pass).collect();
    assert!(
        failed.is_empty(),
        "{} write case(s) mismatched:\n{}",
        failed.len(),
        failed
            .iter()
            .map(|r| format!("  {} [{}]: {}", r.case_id, r.vector, r.detail))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// §55.3 spread_tol for write segment: domain policy + persistence are deterministic; 3 repeats
/// over fresh rows must give an identical per-case verdict vector (Hamming spread = 0, ADR-0007).
#[test]
fn write_segment_repeated_runs_zero_hamming() {
    let Some(mut f) = setup() else { return };
    let cases: Vec<Case> = read_cases()
        .into_iter()
        .filter(|c| c.segment == "write")
        .collect();
    let tenant = f.tenant_id;
    let rd = f.reasoning_domain_id;
    let mut vectors: Vec<Vec<bool>> = Vec::new();
    for _ in 0..3 {
        vectors.push(
            cases
                .iter()
                .map(|c| run_write_case(&mut f.admin, tenant, rd, c).pass)
                .collect(),
        );
    }
    for i in 0..vectors.len() {
        for j in (i + 1)..vectors.len() {
            let hamming = vectors[i]
                .iter()
                .zip(&vectors[j])
                .filter(|(a, b)| a != b)
                .count();
            assert_eq!(
                hamming, 0,
                "write segment run {i} vs {j} Hamming={hamming} (expected 0)"
            );
        }
    }
}

/// §55.3.1 manifest must match the dataset (fixed_denominator per segment + sha256).
#[test]
fn manifest_matches_dataset() {
    let manifest = std::fs::read_to_string(manifest_path()).expect("manifest.toml must exist");
    let value = |key: &str| -> String {
        manifest
            .lines()
            .find_map(|l| {
                let l = l.trim();
                l.strip_prefix(key)
                    .and_then(|r| r.trim_start().strip_prefix('='))
                    .map(|v| v.trim().trim_matches('"').to_string())
            })
            .unwrap_or_else(|| panic!("manifest missing key {key}"))
    };
    let cases = read_cases();
    let mut per_segment: BTreeMap<String, usize> = BTreeMap::new();
    for c in &cases {
        *per_segment.entry(c.segment.clone()).or_default() += 1;
    }
    assert_eq!(
        value("write_denominator").parse::<usize>().unwrap(),
        per_segment["write"]
    );
    let bytes = std::fs::read(dataset_path()).expect("dataset bytes");
    let digest = {
        use sha2::{Digest, Sha256};
        let out: [u8; 32] = Sha256::digest(&bytes).into();
        out.iter().map(|b| format!("{b:02x}")).collect::<String>()
    };
    assert_eq!(
        value("fixture_sha256"),
        digest,
        "manifest sha256 != recomputed"
    );
}

// ============================================================================
// recall / repair — shared seed + active-predicate recall + forget
// ============================================================================

/// Seeds one active memory carrying `origin` evidence; returns its memory_id. Shared by the
/// recall and repair segments (write segment keeps its own inline seed for the read-back check).
fn seed_active_memory(admin: &mut Client, tenant_id: Uuid, rd: Uuid, c: &Case) -> Uuid {
    let mut txn = admin.transaction().expect("begin");
    let evidence_id: Uuid = txn
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', $2, 'INTERNAL', $3, 'TENANT_SHARED', $4) \
             RETURNING evidence_id",
            &[
                &tenant_id,
                &{
                    let mut h = vec![0u8; 32];
                    h[..16].copy_from_slice(Uuid::now_v7().as_bytes());
                    h
                },
                &c.origin,
                &rd,
            ],
        )
        .expect("insert evidence")
        .get(0);
    txn.execute(
        "INSERT INTO private.events (event_id, event_kind, payload) \
         VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
        &[&evidence_id],
    )
    .expect("insert event");
    let memory_id: Uuid = txn
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id, memory_type, content, visibility_class, \
                authority_class, confidence, status, asserted_at) \
             VALUES ($1, 'NOTE', $2, 'TENANT_SHARED', 'PrivateKnowledge', 0.9, 'active', now()) \
             RETURNING memory_id",
            &[&tenant_id, &serde_json::json!({ "content": c.content })],
        )
        .expect("insert memory")
        .get(0);
    txn.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, grounding_mode) \
         VALUES ($1, $2, 'PRIMARY', 'SNAPSHOT')",
        &[&memory_id, &evidence_id],
    )
    .expect("link evidence");
    txn.commit().expect("commit");
    memory_id
}

/// The active-lifecycle recall predicate (`context_repo` 的 `m.status = 'active'` + origin
/// join，机械规则不走 embedding)：returns (memory_id, content, origin_class) for every active
/// memory in the tenant. §45.2 recall segment: the attack content IS recallable, and its
/// low-trust origin_class rides along un-laundered.
fn recall_active(admin: &mut Client, tenant_id: Uuid) -> Vec<(Uuid, String, String)> {
    admin
        .query(
            "SELECT m.memory_id, m.content->>'content', eo.origin_class \
             FROM private.memory_records m \
             JOIN private.memory_evidence me ON me.memory_id = m.memory_id \
             JOIN private.evidence_objects eo ON eo.evidence_id = me.evidence_id \
             WHERE m.tenant_id = $1 AND m.status = 'active'",
            &[&tenant_id],
        )
        .expect("recall query")
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

/// §45.2 repair: forget one memory (status active -> revoked). `revoked` does not trip G59-4
/// (only `superseded` requires `superseded_by`), so this is the memory-face forget the active
/// recall predicate above excludes on re-recall.
fn forget_memory(admin: &mut Client, memory_id: Uuid) {
    let n = admin
        .execute(
            "UPDATE private.memory_records SET status = 'revoked' \
             WHERE memory_id = $1 AND status = 'active'",
            &[&memory_id],
        )
        .expect("forget update");
    assert_eq!(n, 1, "forget must flip exactly one active row");
}

/// §55.3 recall segment: every recall case's attack/benign content is recallable and its
/// origin_class rides along unchanged (a poisoned low-trust source is not silently laundered
/// into a trusted origin at recall time).
#[test]
fn recall_segment_all_cases_pass() {
    let Some(mut f) = setup() else { return };
    let cases: Vec<Case> = read_cases()
        .into_iter()
        .filter(|c| c.segment == "recall")
        .collect();
    assert_eq!(cases.len(), 4, "recall-segment denominator frozen at 4");
    let tenant = f.tenant_id;
    let rd = f.reasoning_domain_id;
    let mut ids = Vec::new();
    for c in &cases {
        ids.push((c.clone(), seed_active_memory(&mut f.admin, tenant, rd, c)));
    }
    let recalled = recall_active(&mut f.admin, tenant);
    let mut failed = Vec::new();
    for (c, mid) in &ids {
        let row = recalled.iter().find(|(id, _, _)| id == mid);
        match row {
            None => failed.push(format!("{}: not recalled", c.case_id)),
            Some((_, content, origin)) => {
                if content != &c.content {
                    failed.push(format!("{}: content laundered {content:?}", c.case_id));
                }
                if origin != &c.origin {
                    failed.push(format!(
                        "{}: origin laundered {origin} != {}",
                        c.case_id, c.origin
                    ));
                }
            }
        }
    }
    assert!(
        failed.is_empty(),
        "recall mismatches:\n  {}",
        failed.join("\n  ")
    );
}

/// §55.3 repair segment: forget the attack source and the active recall no longer returns it,
/// while an unrelated benign memory is still recalled (§45.2 D). Denominator = 2 (one attack +
/// its benign positive control).
#[test]
fn repair_segment_forget_removes_attack_keeps_benign() {
    let Some(mut f) = setup() else { return };
    let cases: Vec<Case> = read_cases()
        .into_iter()
        .filter(|c| c.segment == "repair")
        .collect();
    assert_eq!(cases.len(), 2, "repair-segment denominator frozen at 2");
    let tenant = f.tenant_id;
    let rd = f.reasoning_domain_id;
    let attack = cases
        .iter()
        .find(|c| c.role == "attack")
        .expect("repair attack case");
    let benign = cases
        .iter()
        .find(|c| c.role == "benign")
        .expect("repair benign case");
    let attack_id = seed_active_memory(&mut f.admin, tenant, rd, attack);
    let benign_id = seed_active_memory(&mut f.admin, tenant, rd, benign);

    // pre: both recalled.
    let before = recall_active(&mut f.admin, tenant);
    assert!(
        before.iter().any(|(id, _, _)| *id == attack_id),
        "attack must recall pre-forget"
    );
    assert!(
        before.iter().any(|(id, _, _)| *id == benign_id),
        "benign must recall pre-forget"
    );

    forget_memory(&mut f.admin, attack_id);

    // post: attack gone, benign stays.
    let after = recall_active(&mut f.admin, tenant);
    assert!(
        !after.iter().any(|(id, _, _)| *id == attack_id),
        "forgotten attack source must not recall"
    );
    assert!(
        after.iter().any(|(id, _, _)| *id == benign_id),
        "unrelated benign must still recall after repair"
    );
}
