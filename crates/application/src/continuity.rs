//! `application::continuity` — §25.3 continuity 产品的装配入口（G80-31 的被测调用路径）。
//!
//! 端口反转的原因是依赖方向：`adapters` 依赖本 crate（不能反过来 import
//! `adapters::context_repo`），所以取数以 [`ContextReadPort`] 注入——`adapters` 侧实现它、
//! 委托 `context_repo::fetch_frozen`。先例：[`crate::consolidate::PrivateReasoningPort`]。
//!
//! 装配本体是纯函数（[`humaux_retrieval::handoff::assemble`]）：本函数只做「取冻结读数 →
//! 装配」两步，**收不到时钟、收不到连接**——G80-31「同一快照两次装配逐字节相同」的
//! application 侧保证就是这个签名形状。

use humaux_domain::context::{ContextBudget, FrozenReads};
use humaux_domain::continuity::{ContinuityFacetKind, ProjectId};
use humaux_domain::error::ErrorCode;
use humaux_domain::identity::AuthorizationScope;
use humaux_domain::ids::{Scope, WorkspaceId};
use humaux_retrieval::handoff::{Handoff, assemble};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// §25.4 冻结读数的取数端口。实现方：`adapters`（`context_repo::fetch_frozen`，
/// 单 REPEATABLE READ 事务）。
#[async_trait::async_trait]
pub trait ContextReadPort: Send + Sync {
    /// 单快照取齐 §25.4 步骤 2/5 的全部读数与快照身份。
    async fn fetch_frozen(&self, scope: &Scope) -> Result<FrozenReads, ErrorCode>;
}

/// §25.3：装配一份 continuity handoff。
///
/// `budget` 由调用方从上层配置传入（§78.1：domain/application 不内置默认值——
/// 默认值就是第二真源）。
///
/// # Errors
/// 取数失败原样上抛；装配本体不失败（溢出是 [`Handoff`] 里的一个如实状态，不是错误）。
pub async fn assemble_handoff(
    port: &dyn ContextReadPort,
    scope: &Scope,
    budget: ContextBudget,
) -> Result<Handoff, ErrorCode> {
    let frozen = port.fetch_frozen(scope).await?;
    Ok(assemble(frozen, budget))
}

/// W2 adapter port. The implementation owns the single read-only RR transaction and returns
/// only a closed snapshot; application assembly cannot perform another read.
#[async_trait::async_trait]
pub trait ContinuityReadPort: Send + Sync {
    async fn read_snapshot(
        &self,
        authorization: &AuthorizationScope,
        project_id: ProjectId,
        requested_workspace: Option<WorkspaceId>,
        budget: ContextBudget,
    ) -> Result<ClosedContinuitySnapshot, ErrorCode>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContinuityStatus {
    Current,
    Missing,
    Unavailable,
    Stale,
    Conflicted,
}

impl ContinuityStatus {
    const fn tag(self) -> u8 {
        match self {
            Self::Current => 1,
            Self::Missing => 2,
            Self::Unavailable => 3,
            Self::Stale => 4,
            Self::Conflicted => 5,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DiagnosticCode {
    NoCurrentVersion,
    AuthoritativeSourceUnavailable,
    VisibilityRevalidationFailed,
    SourceRevalidationFailed,
    StoredStale,
    UnresolvedSemanticConflict,
}

impl DiagnosticCode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NoCurrentVersion => "NO_CURRENT_VERSION",
            Self::AuthoritativeSourceUnavailable => "AUTHORITATIVE_SOURCE_UNAVAILABLE",
            Self::VisibilityRevalidationFailed => "VISIBILITY_REVALIDATION_FAILED",
            Self::SourceRevalidationFailed => "SOURCE_REVALIDATION_FAILED",
            Self::StoredStale => "STORED_STALE",
            Self::UnresolvedSemanticConflict => "UNRESOLVED_SEMANTIC_CONFLICT",
        }
    }
}

/// A CURRENT payload whose body/hash/source closure was verified inside the adapter's RR.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedCurrentFacet {
    facet_version_id: Uuid,
    facet_version: u64,
    body: Value,
    body_sha256: [u8; 32],
    memory_source_ids: Vec<Uuid>,
    evidence_source_ids: Vec<Uuid>,
}

impl VerifiedCurrentFacet {
    /// Constructs a current facet only after binding the exact PostgreSQL `jsonb::text` bytes
    /// to the stored database digest. Parsing happens after that check, so no caller can pair an
    /// arbitrary [`Value`] with an unrelated hash or redefine the database canonical bytes.
    pub fn from_authoritative_jsonb_text(
        facet_version_id: Uuid,
        facet_version: i64,
        body_text: String,
        body_sha256: [u8; 32],
        memory_source_ids: Vec<Uuid>,
        evidence_source_ids: Vec<Uuid>,
    ) -> Result<Self, ErrorCode> {
        let facet_version = u64::try_from(facet_version)
            .ok()
            .filter(|version| *version > 0)
            .ok_or(ErrorCode::CannotEstablishCompleteness)?;
        if facet_version_id.is_nil()
            || memory_source_ids.iter().any(Uuid::is_nil)
            || evidence_source_ids.iter().any(Uuid::is_nil)
        {
            return Err(ErrorCode::CannotEstablishCompleteness);
        }
        let computed_body_sha256: [u8; 32] = Sha256::digest(body_text.as_bytes()).into();
        if computed_body_sha256 != body_sha256 {
            return Err(ErrorCode::CannotEstablishCompleteness);
        }
        let body =
            serde_json::from_str(&body_text).map_err(|_| ErrorCode::CannotEstablishCompleteness)?;
        if memory_source_ids
            .windows(2)
            .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
            || evidence_source_ids
                .windows(2)
                .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
        {
            return Err(ErrorCode::CannotEstablishCompleteness);
        }
        Ok(Self {
            facet_version_id,
            facet_version,
            body,
            body_sha256,
            memory_source_ids,
            evidence_source_ids,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SourceFacetState {
    Missing,
    Current(VerifiedCurrentFacet),
    Stale(DiagnosticCode),
    Conflicted,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceFacetInput {
    pub kind: ContinuityFacetKind,
    pub state: SourceFacetState,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClosedContinuitySnapshot {
    pub project_id: ProjectId,
    pub facets: [SourceFacetInput; 15],
    pub handoff: Handoff,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CurrentFacetWire {
    facet_version_id: String,
    facet_version: u64,
    body: Value,
    body_sha256: String,
    memory_source_ids: Vec<String>,
    evidence_source_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CurrentSourceFacetWire {
    kind: &'static str,
    mode: &'static str,
    status: ContinuityStatus,
    current: CurrentFacetWire,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NonCurrentSourceFacetWire {
    kind: &'static str,
    mode: &'static str,
    status: ContinuityStatus,
    diagnostic_code: DiagnosticCode,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DerivedFacetWire {
    kind: &'static str,
    mode: &'static str,
    status: ContinuityStatus,
    payload_ref: &'static str,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ContinuityFacetWire {
    Current(CurrentSourceFacetWire),
    NonCurrent(NonCurrentSourceFacetWire),
    Derived(DerivedFacetWire),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompletenessClass {
    FacetComplete,
    CannotEstablish,
}

impl CompletenessClass {
    const fn tag(&self) -> u8 {
        match self {
            Self::FacetComplete => 1,
            Self::CannotEstablish => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ContinuityCoverage {
    required: u8,
    current: u8,
    missing: u8,
    unavailable: u8,
    stale: u8,
    conflicted: u8,
    ratio: f64,
    completeness_class: CompletenessClass,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContinuitySnapshotWire {
    context_snapshot_seq: i64,
    snapshot_token_sha256: String,
    result_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ContinuityResult {
    contract_version: &'static str,
    project_id: String,
    snapshot: ContinuitySnapshotWire,
    facets: Vec<ContinuityFacetWire>,
    handoff: Handoff,
    coverage: ContinuityCoverage,
}

/// Runs the application use case and builds the exact W0 wire result after the adapter has
/// committed its one closed RR snapshot.
pub async fn read_project_continuity(
    port: &dyn ContinuityReadPort,
    authorization: &AuthorizationScope,
    project_id: ProjectId,
    requested_workspace: Option<WorkspaceId>,
    budget: ContextBudget,
) -> Result<ContinuityResult, ErrorCode> {
    let closed = port
        .read_snapshot(authorization, project_id, requested_workspace, budget)
        .await?;
    assemble_continuity_result(closed)
}

pub const W2_FORCED_UNAVAILABLE: [ContinuityFacetKind; 6] = [
    ContinuityFacetKind::ActiveTasks,
    ContinuityFacetKind::RecentChanges,
    ContinuityFacetKind::Code,
    ContinuityFacetKind::Tests,
    ContinuityFacetKind::Config,
    ContinuityFacetKind::Migrations,
];

fn unavailable_in_w2(kind: ContinuityFacetKind) -> bool {
    W2_FORCED_UNAVAILABLE.contains(&kind)
}

fn source_wire(input: SourceFacetInput) -> ContinuityFacetWire {
    let kind = input.kind.as_str();
    if unavailable_in_w2(input.kind) {
        return ContinuityFacetWire::NonCurrent(NonCurrentSourceFacetWire {
            kind,
            mode: "SOURCE_BACKED",
            status: ContinuityStatus::Unavailable,
            diagnostic_code: DiagnosticCode::AuthoritativeSourceUnavailable,
        });
    }
    match input.state {
        SourceFacetState::Missing => ContinuityFacetWire::NonCurrent(NonCurrentSourceFacetWire {
            kind,
            mode: "SOURCE_BACKED",
            status: ContinuityStatus::Missing,
            diagnostic_code: DiagnosticCode::NoCurrentVersion,
        }),
        SourceFacetState::Stale(diagnostic_code) => {
            ContinuityFacetWire::NonCurrent(NonCurrentSourceFacetWire {
                kind,
                mode: "SOURCE_BACKED",
                status: ContinuityStatus::Stale,
                diagnostic_code,
            })
        }
        SourceFacetState::Conflicted => {
            ContinuityFacetWire::NonCurrent(NonCurrentSourceFacetWire {
                kind,
                mode: "SOURCE_BACKED",
                status: ContinuityStatus::Conflicted,
                diagnostic_code: DiagnosticCode::UnresolvedSemanticConflict,
            })
        }
        SourceFacetState::Current(current) => {
            ContinuityFacetWire::Current(CurrentSourceFacetWire {
                kind,
                mode: "SOURCE_BACKED",
                status: ContinuityStatus::Current,
                current: CurrentFacetWire {
                    facet_version_id: current.facet_version_id.to_string(),
                    facet_version: current.facet_version,
                    body: current.body,
                    body_sha256: hex::encode(current.body_sha256),
                    memory_source_ids: current
                        .memory_source_ids
                        .into_iter()
                        .map(|id| id.to_string())
                        .collect(),
                    evidence_source_ids: current
                        .evidence_source_ids
                        .into_iter()
                        .map(|id| id.to_string())
                        .collect(),
                },
            })
        }
    }
}

fn status_of(facet: &ContinuityFacetWire) -> ContinuityStatus {
    match facet {
        ContinuityFacetWire::Current(value) => value.status,
        ContinuityFacetWire::NonCurrent(value) => value.status,
        ContinuityFacetWire::Derived(value) => value.status,
    }
}

fn assemble_continuity_result(
    closed: ClosedContinuitySnapshot,
) -> Result<ContinuityResult, ErrorCode> {
    if closed.project_id.0.is_nil()
        || closed
            .facets
            .iter()
            .zip(ContinuityFacetKind::ALL)
            .any(|(input, expected)| input.kind != expected)
        || closed.handoff.context_snapshot_seq < 0
    {
        return Err(ErrorCode::CannotEstablishCompleteness);
    }
    let mut source = closed.facets.into_iter().map(source_wire);
    let mut facets = Vec::with_capacity(17);
    facets.extend(source.by_ref().take(13));
    facets.push(ContinuityFacetWire::Derived(DerivedFacetWire {
        kind: "HANDOFF",
        mode: "SYSTEM_DERIVED",
        status: ContinuityStatus::Current,
        payload_ref: "#/handoff",
    }));
    facets.extend(source);

    let mut counts = [0u8; 5];
    for facet in &facets {
        let index = usize::from(status_of(facet).tag() - 1);
        counts[index] = counts[index].checked_add(1).ok_or(ErrorCode::Internal)?;
    }
    // COVERAGE is itself a successfully derived CURRENT facet.
    counts[0] = counts[0].checked_add(1).ok_or(ErrorCode::Internal)?;
    let accounted = counts
        .iter()
        .try_fold(0u8, |sum, value| sum.checked_add(*value))
        .ok_or(ErrorCode::Internal)?;
    if accounted != 17 {
        return Err(ErrorCode::CannotEstablishCompleteness);
    }
    let completeness_class = if counts[0] == 17 && !closed.handoff.counts.overflow {
        CompletenessClass::FacetComplete
    } else {
        CompletenessClass::CannotEstablish
    };
    let coverage = ContinuityCoverage {
        required: 17,
        current: counts[0],
        missing: counts[1],
        unavailable: counts[2],
        stale: counts[3],
        conflicted: counts[4],
        ratio: f64::from(counts[0]) / 17.0,
        completeness_class,
    };
    facets.push(ContinuityFacetWire::Derived(DerivedFacetWire {
        kind: "COVERAGE",
        mode: "SYSTEM_DERIVED",
        status: ContinuityStatus::Current,
        payload_ref: "#/coverage",
    }));

    let snapshot_token = parse_hex_digest(&closed.handoff.snapshot_token_sha256)?;
    let result_sha256 = continuity_result_sha256(
        closed.project_id,
        closed.handoff.context_snapshot_seq,
        snapshot_token,
        &facets,
        closed.handoff.sha256(),
        &coverage,
    )?;
    Ok(ContinuityResult {
        contract_version: "1",
        project_id: closed.project_id.0.to_string(),
        snapshot: ContinuitySnapshotWire {
            context_snapshot_seq: closed.handoff.context_snapshot_seq,
            snapshot_token_sha256: closed.handoff.snapshot_token_sha256.clone(),
            result_sha256: hex::encode(result_sha256),
        },
        facets,
        handoff: closed.handoff,
        coverage,
    })
}

fn parse_hex_digest(value: &str) -> Result<[u8; 32], ErrorCode> {
    let bytes = hex::decode(value).map_err(|_| ErrorCode::Internal)?;
    bytes.try_into().map_err(|_| ErrorCode::Internal)
}

fn push_diagnostic(
    bytes: &mut Vec<u8>,
    diagnostic: Option<DiagnosticCode>,
) -> Result<(), ErrorCode> {
    let Some(diagnostic) = diagnostic else {
        bytes.push(0);
        return Ok(());
    };
    let diagnostic = diagnostic.as_str().as_bytes();
    let length = u16::try_from(diagnostic.len()).map_err(|_| ErrorCode::Internal)?;
    bytes.push(1);
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(diagnostic);
    Ok(())
}

fn push_ids(bytes: &mut Vec<u8>, ids: &[String]) -> Result<(), ErrorCode> {
    let count = u32::try_from(ids.len()).map_err(|_| ErrorCode::Internal)?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for id in ids {
        let id = Uuid::parse_str(id).map_err(|_| ErrorCode::Internal)?;
        bytes.extend_from_slice(id.as_bytes());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn continuity_result_sha256(
    project_id: ProjectId,
    context_snapshot_seq: i64,
    snapshot_token_sha256: [u8; 32],
    facets: &[ContinuityFacetWire],
    handoff_sha256: [u8; 32],
    coverage: &ContinuityCoverage,
) -> Result<[u8; 32], ErrorCode> {
    let seq = u64::try_from(context_snapshot_seq).map_err(|_| ErrorCode::Internal)?;
    if facets.len() != 17 {
        return Err(ErrorCode::Internal);
    }
    let mut bytes = b"humaux.continuity.result.v1\0".to_vec();
    bytes.extend_from_slice(project_id.0.as_bytes());
    bytes.extend_from_slice(&seq.to_be_bytes());
    bytes.extend_from_slice(&snapshot_token_sha256);
    for (index, facet) in facets.iter().enumerate() {
        bytes.push(u8::try_from(index + 1).map_err(|_| ErrorCode::Internal)?);
        match facet {
            ContinuityFacetWire::Current(value) => {
                bytes.extend_from_slice(&[1, ContinuityStatus::Current.tag()]);
                push_diagnostic(&mut bytes, None)?;
                let version_id = Uuid::parse_str(&value.current.facet_version_id)
                    .map_err(|_| ErrorCode::Internal)?;
                bytes.extend_from_slice(version_id.as_bytes());
                bytes.extend_from_slice(&value.current.facet_version.to_be_bytes());
                bytes.extend_from_slice(&parse_hex_digest(&value.current.body_sha256)?);
                push_ids(&mut bytes, &value.current.memory_source_ids)?;
                push_ids(&mut bytes, &value.current.evidence_source_ids)?;
            }
            ContinuityFacetWire::NonCurrent(value) => {
                bytes.extend_from_slice(&[1, value.status.tag()]);
                push_diagnostic(&mut bytes, Some(value.diagnostic_code))?;
            }
            ContinuityFacetWire::Derived(value) => {
                bytes.extend_from_slice(&[2, value.status.tag()]);
                push_diagnostic(&mut bytes, None)?;
                bytes.push(match value.payload_ref {
                    "#/handoff" => 1,
                    "#/coverage" => 2,
                    _ => return Err(ErrorCode::Internal),
                });
            }
        }
    }
    bytes.extend_from_slice(&handoff_sha256);
    bytes.extend_from_slice(&[
        coverage.required,
        coverage.current,
        coverage.missing,
        coverage.unavailable,
        coverage.stale,
        coverage.conflicted,
        coverage.completeness_class.tag(),
    ]);
    Ok(Sha256::digest(bytes).into())
}

#[cfg(test)]
mod w2_tests {
    use super::*;
    use humaux_retrieval::handoff::{HandoffCounts, HandoffItem};

    fn id(value: &str) -> Uuid {
        Uuid::parse_str(value).expect("fixed UUID")
    }

    fn handoff() -> Handoff {
        Handoff {
            context_snapshot_seq: 42,
            snapshot_token_sha256:
                "1111111111111111111111111111111111111111111111111111111111111111".into(),
            mandatory: vec![HandoffItem {
                memory_id: "018f0000-0000-7000-8000-000000000010".into(),
                selector: "project_active_constraints_v1".into(),
                authority: "ProjectConstraint".into(),
            }],
            pinned: Vec::new(),
            needs_verification: Vec::new(),
            not_judged: Vec::new(),
            unavailable_selectors: Vec::new(),
            overflow_manifest: Vec::new(),
            counts: HandoffCounts {
                mandatory_expected: 1,
                mandatory_returned: 1,
                mandatory_missing: 0,
                pinned_expected: 0,
                pinned_returned: 0,
                pinned_excluded: 0,
                overflow: false,
            },
        }
    }

    fn closed_snapshot() -> ClosedContinuitySnapshot {
        let body = r#"{"a": 1, "b": [2, 3]}"#.to_owned();
        let body_sha256 = Sha256::digest(body.as_bytes()).into();
        let current = VerifiedCurrentFacet::from_authoritative_jsonb_text(
            id("018f0000-0000-7000-8000-000000000020"),
            1,
            body,
            body_sha256,
            vec![id("018f0000-0000-7000-8000-000000000030")],
            vec![id("018f0000-0000-7000-8000-000000000040")],
        )
        .expect("closed current facet");
        let mut current = Some(current);
        ClosedContinuitySnapshot {
            project_id: ProjectId(id("018f0000-0000-7000-8000-000000000001")),
            facets: std::array::from_fn(|index| SourceFacetInput {
                kind: ContinuityFacetKind::ALL[index],
                state: if index == 0 {
                    SourceFacetState::Current(current.take().expect("one current facet"))
                } else {
                    SourceFacetState::Missing
                },
            }),
            handoff: handoff(),
        }
    }

    #[test]
    fn authoritative_body_and_order_are_unforgeable() {
        let body = r#"{"value": 1}"#.to_owned();
        let hash: [u8; 32] = Sha256::digest(body.as_bytes()).into();
        assert!(
            VerifiedCurrentFacet::from_authoritative_jsonb_text(
                id("018f0000-0000-7000-8000-000000000020"),
                1,
                body.clone(),
                [0; 32],
                Vec::new(),
                Vec::new(),
            )
            .is_err()
        );
        assert!(
            VerifiedCurrentFacet::from_authoritative_jsonb_text(
                id("018f0000-0000-7000-8000-000000000020"),
                1,
                body,
                hash,
                vec![
                    id("018f0000-0000-7000-8000-000000000032"),
                    id("018f0000-0000-7000-8000-000000000031"),
                ],
                Vec::new(),
            )
            .is_err()
        );
    }

    #[test]
    fn fixed_result_has_exact_order_partition_and_zero_leak() {
        let result = assemble_continuity_result(closed_snapshot()).expect("closed result");
        let kinds = result
            .facets
            .iter()
            .map(|facet| match facet {
                ContinuityFacetWire::Current(value) => value.kind,
                ContinuityFacetWire::NonCurrent(value) => value.kind,
                ContinuityFacetWire::Derived(value) => value.kind,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                "GOAL",
                "CURRENT_STATE",
                "DECISIONS",
                "REJECTIONS",
                "CONSTRAINTS",
                "KNOWN_ISSUES",
                "NEXT_ACTIONS",
                "ACTIVE_TASKS",
                "RECENT_CHANGES",
                "CODE",
                "TESTS",
                "CONFIG",
                "MIGRATIONS",
                "HANDOFF",
                "PROCEDURES",
                "OUTCOMES",
                "COVERAGE",
            ]
        );
        assert_eq!(result.coverage.current, 3);
        assert_eq!(result.coverage.missing, 8);
        assert_eq!(result.coverage.unavailable, 6);
        assert_eq!(result.coverage.stale, 0);
        assert_eq!(result.coverage.conflicted, 0);
        assert_eq!(
            result.coverage.completeness_class,
            CompletenessClass::CannotEstablish
        );
        let wire = serde_json::to_value(&result).expect("serializable result");
        for facet in wire["facets"].as_array().expect("facet array") {
            if facet["status"] != "CURRENT" {
                assert!(facet.get("current").is_none());
                assert!(facet.get("body").is_none());
                assert!(facet.get("body_sha256").is_none());
                assert!(facet.get("memory_source_ids").is_none());
                assert!(facet.get("evidence_source_ids").is_none());
            }
        }
    }

    fn recompute(result: &ContinuityResult) -> String {
        hex::encode(
            continuity_result_sha256(
                ProjectId(id(&result.project_id)),
                result.snapshot.context_snapshot_seq,
                parse_hex_digest(&result.snapshot.snapshot_token_sha256).expect("snapshot digest"),
                &result.facets,
                result.handoff.sha256(),
                &result.coverage,
            )
            .expect("hash view"),
        )
    }

    #[test]
    fn result_hash_v1_golden_and_self_omission() {
        let mut result = assemble_continuity_result(closed_snapshot()).expect("closed result");
        assert_eq!(
            result.snapshot.result_sha256,
            "61b906b7b32b6c5bbfae20e73706ab2be506230e5a24f9e6955e0085e47e879c"
        );
        let original = result.snapshot.result_sha256.clone();
        result.snapshot.result_sha256 =
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".into();
        assert_eq!(recompute(&result), original);
    }

    #[test]
    fn every_hash_view_semantic_mutation_changes_digest() {
        type Mutation = (&'static str, fn(&mut ContinuityResult));
        let mutations: [Mutation; 20] = [
            ("project", |result| {
                result.project_id = "018f0000-0000-7000-8000-000000000002".into();
            }),
            ("sequence", |result| {
                result.snapshot.context_snapshot_seq += 1
            }),
            ("snapshot token", |result| {
                result.snapshot.snapshot_token_sha256 = "22".repeat(32);
            }),
            ("facet ordinal", |result| result.facets.swap(0, 1)),
            ("mode", |result| {
                result.facets[1] = ContinuityFacetWire::Derived(DerivedFacetWire {
                    kind: "CURRENT_STATE",
                    mode: "SYSTEM_DERIVED",
                    status: ContinuityStatus::Current,
                    payload_ref: "#/handoff",
                });
            }),
            ("status", |result| {
                let ContinuityFacetWire::NonCurrent(facet) = &mut result.facets[1] else {
                    panic!("fixed non-current facet")
                };
                facet.status = ContinuityStatus::Stale;
            }),
            ("diagnostic", |result| {
                let ContinuityFacetWire::NonCurrent(facet) = &mut result.facets[1] else {
                    panic!("fixed non-current facet")
                };
                facet.diagnostic_code = DiagnosticCode::StoredStale;
            }),
            ("version id", |result| {
                let ContinuityFacetWire::Current(facet) = &mut result.facets[0] else {
                    panic!("fixed current facet")
                };
                facet.current.facet_version_id = "018f0000-0000-7000-8000-000000000021".into();
            }),
            ("version", |result| {
                let ContinuityFacetWire::Current(facet) = &mut result.facets[0] else {
                    panic!("fixed current facet")
                };
                facet.current.facet_version += 1;
            }),
            ("body hash", |result| {
                let ContinuityFacetWire::Current(facet) = &mut result.facets[0] else {
                    panic!("fixed current facet")
                };
                facet.current.body_sha256 = "33".repeat(32);
            }),
            ("memory source ids", |result| {
                let ContinuityFacetWire::Current(facet) = &mut result.facets[0] else {
                    panic!("fixed current facet")
                };
                facet.current.memory_source_ids =
                    vec!["018f0000-0000-7000-8000-000000000031".into()];
            }),
            ("evidence source ids", |result| {
                let ContinuityFacetWire::Current(facet) = &mut result.facets[0] else {
                    panic!("fixed current facet")
                };
                facet.current.evidence_source_ids =
                    vec!["018f0000-0000-7000-8000-000000000041".into()];
            }),
            ("handoff", |result| {
                result.handoff.mandatory[0].memory_id =
                    "018f0000-0000-7000-8000-000000000011".into();
            }),
            ("coverage required", |result| result.coverage.required -= 1),
            ("coverage current", |result| result.coverage.current += 1),
            ("coverage missing", |result| result.coverage.missing -= 1),
            ("coverage unavailable", |result| {
                result.coverage.unavailable -= 1;
            }),
            ("coverage stale", |result| result.coverage.stale += 1),
            ("coverage conflicted", |result| {
                result.coverage.conflicted += 1
            }),
            ("coverage class", |result| {
                result.coverage.completeness_class = CompletenessClass::FacetComplete;
            }),
        ];
        let baseline = assemble_continuity_result(closed_snapshot()).expect("closed result");
        let baseline_hash = recompute(&baseline);
        for (label, mutate) in mutations {
            let mut candidate = baseline.clone();
            mutate(&mut candidate);
            assert_ne!(recompute(&candidate), baseline_hash, "mutation: {label}");
        }
    }
}
