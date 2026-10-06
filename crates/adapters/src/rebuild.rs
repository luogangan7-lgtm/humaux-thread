//! `adapters::rebuild` — the Qdrant rebuild from PostgreSQL as generation g+1 on the same stream (ADR-0064 D-A,
//!   D-E), its equivalence verifier against Project(PG@H2) (D-F E1–E5, orphan deletion under the quiescent rule),
//!   the embedding-fingerprint precheck (D-G), and the closed no-provider deps of the restore drill (D-M).
//! Depends-on: crates=[async-trait, hex, humaux-domain, humaux-infra-cell, humaux-local-secret-scan,
//!   humaux-projection, serde_json, sha2, sqlx];
//!   services=[PostgreSQL(role_maintenance) r=[projection.embedding_fingerprints,
//!   projection.stream_checkpoints, projection.tenant_placements] x=[projection.issue_rebuild_tickets,
//!   projection.rebuild_close, projection.rebuild_open],
//!   PostgreSQL(role_retrieval_worker) r=[ops.outbox, private.evidence_objects, private.memory_evidence,
//!   private.memory_records, projection.memory_vectors, projection.private_memory_points,
//!   projection.rebuild_tickets, projection.stream_checkpoints, projection.stream_log], Qdrant(*)]; env=[];
//!   modules=[adapters::maintenance_repo, adapters::postgres, adapters::projection_worker, adapters::provisioning,
//!   adapters::qdrant, domain::egress, domain::error, domain::ids, domain::ticket_family, humaux-local-secret-scan,
//!   infra-cell::permit, infra-cell::resource, infra-cell::transport, projection::dense, projection::serving, projection::stream]
//! Called-by: [maintenance::drill, maintenance::rebuild_cli, tests]
//! Invariants: [every write is an owner definer call as role_maintenance under the tenant GUC (rebuild_open,
//!   issue_rebuild_tickets, rebuild_close), none is a table write; the verifier reads PostgreSQL in READ ONLY
//!   transactions as the projector (role_retrieval_worker), so X and the E4 payloads are what the projector writes;
//!   verify_stream never deletes, upserts or writes; orphans are deleted only when, in one snapshot after the
//!   scroll, no ticket of the stream at or below H2 is in flight, and only ids absent from that snapshot's registry,
//!   fenced at H2; a fingerprint other than the worker label's refuses before any write; NoProviderEmbedder can
//!   only refuse and count, and drill_projection_deps takes no embedder; drill_closed_deps can reach only the
//!   drill's own Qdrant on 127.0.0.1; with require_stored_vector (the drill) only generation tickets count as in
//!   flight for the stream-quiescence verdict, restored non-generation tickets being frozen quarantine state]
//! Spec: Baseline §16.2; §44; ADR-0057; ADR-0064 D-A; ADR-0064 D-E; ADR-0064 D-F; ADR-0064 D-G; ADR-0064 D-M;
//!   ADR-0064 D-N
//!
//! ## One stream, in order (D-E)
//!
//! [`precheck`] (D-G) → [`open_run`] → `ensure_collection` → [`issue_tickets`] until 0 → [`wait_generation`]
//! (the resident retrieval worker, or the caller's in-process pump, projects the tickets) → [`verify_run`]
//! at H2 with the orphan step → [`close_run`]. [`rebuild_stream`] runs them; the tests drive the steps.
//!
//! ## Why the verifier reads as the projector
//!
//! `role_maintenance`'s SELECT on `private.memory_records` is narrowed by the 0012/0155 visibility and subject
//! policies, so a USER_PRIVATE or WORKSPACE_SHARED memory is invisible to it; X and E4 must be what the projector
//! builds, which only `role_retrieval_worker`'s 0140 arm sees. The reads therefore take a
//! [`RetrievalWorkerDbPool`]; the definers take the [`MaintenanceDbPool`].

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use humaux_domain::egress::ProcessorId;
use humaux_domain::error::ErrorCode;
use humaux_domain::ids::{TenantId, WorkspaceId};
use humaux_domain::ticket_family::TicketFamily;
use humaux_infra_cell::{
    CallerId, CellAccessPermit, CellId, HttpIntraCellTransport, IntraCellHttpTransport,
    IntraCellMethod, IntraCellRequest, IntraCellResource, IntraCellResourceRegistry, ResourceEntry,
    authorize_cell_access,
};
use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig, SealedRetrievalCard};
use humaux_projection::dense::build_stream_count_filter;
use humaux_projection::serving::StreamFamily;
use humaux_projection::stream::StreamKey;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use sqlx::types::Uuid;

use crate::maintenance_repo;
use crate::postgres::{MaintenanceDbPool, RetrievalWorkerDbPool};
use crate::projection_worker::{
    self, CardEmbedder, ExpectedPoint, SharedProjectionDeps, Sleep, begin_projector_read,
};
use crate::provisioning::{self, ProvisioningError, QdrantFace};
use crate::qdrant::{self, PointId, QdrantOperation, RetrievalFamily, ScrolledPoint};

type Result<T> = std::result::Result<T, ProvisioningError>;
type Txn = sqlx::Transaction<'static, sqlx::Postgres>;

/// ADR-0064 D-F E5: the frozen per-component tolerance between the stored vector, normalised in f32 as Qdrant
/// does for Cosine, and Qdrant's copy. A contract constant, not configuration.
pub const VECTOR_TOLERANCE: f32 = 1e-6;

/// ADR-0064 D-F E4: how many differing ids a report names.
const DIFF_REPORT_LIMIT: usize = 10;

/// Points per verifier scroll page.
// ponytail: fixed page, the whole stream is held in memory for E3–E5 (one map per stream); stream the
// comparison by id range if one stream's points outgrow the operator host.
const SCROLL_PAGE: u32 = 256;

/// The ticket states that are not settled (0167's in-flight set; D-A completion term 1).
const IN_FLIGHT: &str = "('ISSUED', 'PROCESSING', 'WAITING_KEY', 'RETRY_WAIT')";

/// D-F E2 / card 35 D-N: the deterministic FAILED classes a generation ticket may end in (an exclusion by outcome).
const DETERMINISTIC_CLASSES: [&str; 3] = [
    "card_unbuildable",
    "secret_scan_rejected",
    "embedding_dimension_mismatch",
];

/// `projection.rebuild_runs.verdict`, the closed set of the 0233 CHECK (reconciled by a contract test).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Equivalent,
    NotEquivalent,
    ReEmbedRequired,
    CannotEstablish,
}

impl Verdict {
    /// Every variant, for the DB contract test.
    pub const ALL: [Verdict; 4] = [
        Self::Equivalent,
        Self::NotEquivalent,
        Self::ReEmbedRequired,
        Self::CannotEstablish,
    ];

    /// The wire / DB value.
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Equivalent => "equivalent",
            Self::NotEquivalent => "not_equivalent",
            Self::ReEmbedRequired => "re_embed_required",
            Self::CannotEstablish => "cannot_establish",
        }
    }
}

/// The worker label's binding (ADR-0064 D-C): the label, its fingerprint and the dimension the fingerprint names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerLabel {
    pub label: String,
    pub fingerprint: [u8; 32],
    pub dimension: u32,
}

/// ADR-0064 D-E step 1: the binding of `label` (the worker's `HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION`), refused
/// `label_unbound:<label>` when no worker ever bound it.
pub async fn worker_label(pool: &MaintenanceDbPool, label: &str) -> Result<WorkerLabel> {
    // dep: PostgreSQL(role_maintenance) — read the label's fingerprint row (0232)
    let row = sqlx::query(
        "SELECT fingerprint_sha256, dimension FROM projection.embedding_fingerprints \
          WHERE embedding_version = $1",
    )
    .bind(label)
    .fetch_optional(pool.pool())
    .await?
    .ok_or_else(|| ProvisioningError::Refused(format!("label_unbound:{label}")))?;
    let fingerprint: Vec<u8> = row.try_get("fingerprint_sha256")?;
    let dimension: i32 = row.try_get("dimension")?;
    Ok(WorkerLabel {
        label: label.to_owned(),
        fingerprint: fingerprint
            .try_into()
            .map_err(|_| ProvisioningError::InvalidInput("fingerprint length".to_owned()))?,
        dimension: u32::try_from(dimension)
            .map_err(|_| ProvisioningError::InvalidInput("dimension".to_owned()))?,
    })
}

/// What every step needs: the definer pool, the projector-view reader, the Qdrant face and the worker label.
pub struct RebuildDeps<'a> {
    pub maintenance: &'a MaintenanceDbPool,
    pub reader: &'a RetrievalWorkerDbPool,
    pub qdrant: &'a QdrantFace,
    pub worker: &'a WorkerLabel,
}

/// One stream to rebuild: its key and the collection its tenant's placement names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stream {
    pub key: StreamKey,
    pub collection: String,
}

impl Stream {
    fn family(&self) -> StreamFamily {
        StreamFamily::new(
            self.key.tenant_id,
            self.key.scope_kind.clone(),
            self.key.scope_id,
            self.key.domain.clone(),
            self.key.projection_kind.clone(),
        )
    }

    fn receipt_head(&self) -> Value {
        json!({
            "tenant_id": self.key.tenant_id.0,
            "workspace_id": self.key.scope_id,
            "domain": self.key.domain,
            "projection_kind": self.key.projection_kind,
            "projection_version": self.key.projection_version,
            "collection": self.collection,
        })
    }
}

/// ADR-0064 D-E: the serving workspace streams of `tenant` (optionally one workspace) for each `TicketFamily::ALL`
/// member, each with its tenant's placement collection. A family without a placement row is skipped (nothing was
/// ever projected for it).
pub async fn streams(
    pool: &MaintenanceDbPool,
    tenant: Uuid,
    workspace: Option<Uuid>,
) -> Result<Vec<Stream>> {
    let mut out = Vec::new();
    // dep: PostgreSQL(role_maintenance) — transaction entry for `streams`
    let mut txn = pool.pool().begin().await?;
    provisioning::set_tenant(&mut txn, tenant).await?;
    for family in TicketFamily::ALL {
        let Some(placement) = RetrievalFamily::ALL
            .into_iter()
            .find(|f| f.ticket_family() == Some(family))
        else {
            continue;
        };
        let rows = sqlx::query(
            "SELECT k.scope_id, p.collection_name FROM projection.stream_checkpoints k \
               JOIN projection.tenant_placements p \
                 ON p.tenant_id = k.tenant_id AND p.projection_family = $5 \
              WHERE k.tenant_id = $1 AND k.scope_kind = 'workspace' AND k.serving \
                AND k.domain = $2 AND k.projection_kind = $3 AND k.projection_version = $4 \
                AND ($6::uuid IS NULL OR k.scope_id = $6) \
              ORDER BY k.scope_id",
        )
        .bind(tenant)
        .bind(family.domain())
        .bind(family.projection_kind())
        .bind(family.projection_version())
        .bind(placement.as_db_str())
        .bind(workspace)
        .fetch_all(&mut *txn)
        .await?;
        for row in rows {
            out.push(Stream {
                key: StreamKey::new(
                    TenantId(tenant),
                    "workspace",
                    row.try_get::<Uuid, _>("scope_id")?,
                    family.domain(),
                    family.projection_kind(),
                    family.projection_version(),
                ),
                collection: row.try_get("collection_name")?,
            });
        }
    }
    txn.commit().await?;
    Ok(out)
}

/// `--all`: every tenant id, through card 35's tenant page definer (ADR-0062 D-D).
// ponytail: one page holding every tenant id; walk the definer's `p_after` cursor if the tenant list outgrows one
// result set.
pub async fn all_tenants(pool: &MaintenanceDbPool) -> Result<Vec<Uuid>> {
    Ok(maintenance_repo::tenant_page(pool, None, i32::MAX).await?)
}

// ============================================================================
// D-G precheck
// ============================================================================

/// ADR-0064 D-G: the stream's live registry rows (worker label) grouped by fingerprint. `Ok(legacy)` = rebuild may
/// proceed and `legacy` rows will be re-embedded under consent; `Err(receipt)` = refused `re_embed_required`, nothing
/// issued, written or called.
pub async fn precheck(
    deps: &RebuildDeps<'_>,
    stream: &Stream,
    allow_reembed: Option<u64>,
) -> Result<std::result::Result<i64, Value>> {
    let mut txn = begin_projector_read(deps.reader, stream.key.tenant_id.0).await?;
    let row = key_query(
        "SELECT count(*) FILTER (WHERE fingerprint_sha256 IS NULL) AS legacy, \
                count(*) FILTER (WHERE fingerprint_sha256 IS NOT NULL AND fingerprint_sha256 <> $8) AS other \
           FROM projection.private_memory_points \
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
            AND projection_kind = $5 AND projection_version = $6 AND embedding_version = $7 \
            AND projection_live",
        &stream.key,
    )
    .bind(&deps.worker.label)
    .bind(&deps.worker.fingerprint[..])
    .fetch_one(&mut *txn)
    .await?;
    txn.commit().await?;
    let legacy: i64 = row.try_get("legacy")?;
    let other: i64 = row.try_get("other")?;
    let mut receipt = stream.receipt_head();
    receipt["verdict"] = json!(Verdict::ReEmbedRequired.as_db_str());
    if other > 0 {
        // Reachable only by tampering or a revoked binding: a model change takes a new label (37b).
        receipt["points_other_fingerprint"] = json!(other);
        return Ok(Err(receipt));
    }
    let consented = allow_reembed.is_some_and(|n| u64::try_from(legacy).is_ok_and(|l| l <= n));
    if legacy > 0 && !consented {
        receipt["points_without_vector"] = json!(legacy);
        receipt["embedding_version"] = json!(deps.worker.label);
        receipt["run"] = json!(format!(
            "humaux-maintenance projection rebuild --tenant {} --workspace {} --batch <n> --wait-seconds <n> \
             --allow-reembed {legacy}",
            stream.key.tenant_id.0, stream.key.scope_id
        ));
        return Ok(Err(receipt));
    }
    Ok(Ok(legacy))
}

// ============================================================================
// D-E definers
// ============================================================================

/// An open run of one stream (D-E step 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Run {
    pub run_id: Uuid,
    pub generation: i32,
    pub boundary_seq: i64,
    pub resumed: bool,
}

async fn maintenance_txn(pool: &MaintenanceDbPool, tenant: Uuid) -> Result<Txn> {
    // dep: PostgreSQL(role_maintenance) — one definer call under the tenant GUC
    let mut txn = pool.pool().begin().await?;
    provisioning::set_tenant(&mut txn, tenant).await?;
    Ok(txn)
}

/// D-E step 2: `projection.rebuild_open` (H = issued_highwater under the checkpoint row lock; resumes an open run
/// with the same fingerprint, refuses `rebuild_open_with_other_fingerprint` otherwise).
pub async fn open_run(deps: &RebuildDeps<'_>, stream: &Stream) -> Result<Run> {
    let k = &stream.key;
    let mut txn = maintenance_txn(deps.maintenance, k.tenant_id.0).await?;
    // dep: PostgreSQL(role_maintenance) — projection.rebuild_open (0233 owner definer)
    let row = sqlx::query("SELECT * FROM projection.rebuild_open($1, $2, $3, $4, $5, $6, $7)")
        .bind(k.tenant_id.0)
        .bind(&k.scope_kind)
        .bind(k.scope_id)
        .bind(&k.domain)
        .bind(&k.projection_kind)
        .bind(&k.projection_version)
        .bind(&deps.worker.fingerprint[..])
        .fetch_one(&mut *txn)
        .await?;
    txn.commit().await?;
    Ok(Run {
        run_id: row.try_get("run_id")?,
        generation: row.try_get("generation")?,
        boundary_seq: row.try_get("boundary_seq")?,
        resumed: row.try_get("resumed")?,
    })
}

/// D-E step 4: `projection.issue_rebuild_tickets` in batches of `batch` until it issues 0; returns the total.
pub async fn issue_tickets(
    deps: &RebuildDeps<'_>,
    stream: &Stream,
    run: &Run,
    batch: i32,
    require_stored_vector: bool,
) -> Result<i64> {
    let mut total = 0;
    loop {
        let mut txn = maintenance_txn(deps.maintenance, stream.key.tenant_id.0).await?;
        // dep: PostgreSQL(role_maintenance) — projection.issue_rebuild_tickets (0233 owner definer)
        let issued: i64 = sqlx::query_scalar("SELECT projection.issue_rebuild_tickets($1, $2, $3)")
            .bind(run.run_id)
            .bind(batch)
            .bind(require_stored_vector)
            .fetch_one(&mut *txn)
            .await?;
        txn.commit().await?;
        total += issued;
        if issued == 0 {
            return Ok(total);
        }
    }
}

/// D-E step 7: `projection.rebuild_close` (refuses `generation_in_flight`, `boundary_moved`, `catch_up_in_flight`).
pub async fn close_run(
    deps: &RebuildDeps<'_>,
    stream: &Stream,
    run: &Run,
    report: &Report,
) -> Result<()> {
    let mut txn = maintenance_txn(deps.maintenance, stream.key.tenant_id.0).await?;
    // dep: PostgreSQL(role_maintenance) — projection.rebuild_close (0233 owner definer)
    sqlx::query("SELECT projection.rebuild_close($1, $2, $3, $4, $5, $6)")
        .bind(run.run_id)
        .bind(report.h2)
        .bind(report.verdict.as_db_str())
        .bind(report.points)
        .bind(&report.merkle_root[..])
        .bind(&report.json)
        .execute(&mut *txn)
        .await?;
    txn.commit().await?;
    Ok(())
}

/// The caller's step while generation tickets are in flight: a sleep while the resident worker projects (production),
/// or an in-process scoped pass (the drill and the tests).
pub type Pump<'a> = dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> + Sync + 'a;

/// D-E step 5: pumps until no generation ticket of `run` is in flight (`true`) or `wait` has passed (`false`).
pub async fn wait_generation(
    deps: &RebuildDeps<'_>,
    stream: &Stream,
    run: &Run,
    wait: Duration,
    pump: &Pump<'_>,
) -> Result<bool> {
    let started = Instant::now();
    loop {
        let mut txn = begin_projector_read(deps.reader, stream.key.tenant_id.0).await?;
        let in_flight: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS (SELECT 1 FROM projection.rebuild_tickets rt \
               JOIN projection.stream_log sl \
                 ON sl.tenant_id = rt.tenant_id AND sl.scope_kind = rt.scope_kind \
                AND sl.scope_id = rt.scope_id AND sl.domain = rt.domain \
                AND sl.projection_kind = rt.projection_kind \
                AND sl.projection_version = rt.projection_version AND sl.stream_seq = rt.stream_seq \
              WHERE rt.run_id = $1 AND sl.state IN {IN_FLIGHT})"
        ))
        .bind(run.run_id)
        .fetch_one(&mut *txn)
        .await?;
        txn.commit().await?;
        if !in_flight {
            return Ok(true);
        }
        if started.elapsed() >= wait {
            return Ok(false);
        }
        pump().await;
    }
}

// ============================================================================
// D-F verifier
// ============================================================================

/// One verifier result (D-F): the verdict, the stream's H2, the compared point count, the PG-side payload Merkle
/// root, the report object, and the label points absent from the registry (the orphan candidates).
#[derive(Debug, Clone)]
pub struct Report {
    pub verdict: Verdict,
    pub h2: i64,
    pub points: i64,
    pub merkle_root: [u8; 32],
    pub json: Value,
    pub orphans: Vec<Uuid>,
}

/// Every point of one stream from Qdrant (D-F E3): the worker label's points by id, the count of other labels'
/// points (never compared, never deleted), points whose id is not a uuid, and the exact count of the same filter.
#[derive(Debug, Clone, Default)]
pub struct Scrolled {
    pub label: BTreeMap<Uuid, ScrolledPoint>,
    pub other_label: u64,
    pub numeric_ids: u64,
    pub count_exact: u64,
}

/// D-F E3: scrolls every point of `stream` (payload and vector) and counts the same filter exactly.
pub async fn scroll_stream(deps: &RebuildDeps<'_>, stream: &Stream) -> Result<Scrolled> {
    let filter = build_stream_count_filter(
        stream.key.tenant_id,
        WorkspaceId(stream.key.scope_id),
        &stream.key.projection_version,
    )
    .ok_or_else(|| ProvisioningError::InvalidInput("empty projection_version".to_owned()))?;
    let (transport, permit) = deps.qdrant.wire()?;
    let qerr = |e: qdrant::QdrantTransportError| ProvisioningError::Qdrant(e.to_string());
    let mut scrolled = Scrolled::default();
    let mut offset = None;
    loop {
        // dep: Qdrant(*) — one page of the stream's points (payload and vector)
        let page = qdrant::scroll_stream_points(
            transport,
            &permit,
            &stream.collection,
            &filter,
            offset,
            SCROLL_PAGE,
            true,
        )
        .await
        .map_err(qerr)?;
        for point in page.points {
            let label = point
                .payload
                .get("embedding_version")
                .and_then(Value::as_str);
            match (point.id, label == Some(deps.worker.label.as_str())) {
                (PointId::Uuid(id), true) => {
                    scrolled.label.insert(id, point);
                }
                (PointId::Num(_), _) => scrolled.numeric_ids += 1,
                (PointId::Uuid(_), false) => scrolled.other_label += 1,
            }
        }
        offset = page.next;
        if offset.is_none() {
            break;
        }
    }
    scrolled.count_exact = qdrant::count(
        transport,
        &permit,
        &stream.collection,
        &qdrant::VisibleCountFilter::from_stream(filter),
    )
    .await
    .map_err(qerr)?;
    Ok(scrolled)
}

/// One live registry row of the stream under the worker label (R), with its vector row.
struct Registered {
    memory_id: Uuid,
    fingerprint: Option<Vec<u8>>,
    vector: Option<Vec<f32>>,
}

/// One input (Evidence) of the stream at or below H2, as D-E step 4 classifies it.
struct Input {
    /// The input has a generation ticket of the judged run (an exclusion found later is an outcome, not a skip).
    in_run: bool,
    home_commit: i64,
    excluded: Option<String>,
    latest_state: String,
    latest_class: Option<String>,
}

/// Everything the verifier reads from PostgreSQL, in one READ ONLY snapshot.
struct PgSide {
    h2: i64,
    stream_in_flight: bool,
    registry: BTreeMap<Uuid, Registered>,
    tombstoned_memories: BTreeSet<Uuid>,
    expected: BTreeMap<Uuid, (ExpectedPoint, bool)>,
    excluded: BTreeMap<String, i64>,
    generation: Vec<(String, Option<String>, Option<String>, bool)>,
}

async fn read_inputs(
    txn: &mut Txn,
    key: &StreamKey,
    h2: i64,
    fingerprint: &[u8],
    require_stored_vector: bool,
    run: Option<Uuid>,
) -> Result<Vec<Input>> {
    // Mirrors 0233 issue_rebuild_tickets' four exclusions to LABEL what the definer left out; the definer decides.
    let rows = key_query(
        "WITH tickets AS ( \
           SELECT sl.stream_seq, sl.commit_seq, sl.state, sl.error_class, o.evidence_id, \
                  EXISTS (SELECT 1 FROM projection.rebuild_tickets rt \
                           WHERE rt.tenant_id = sl.tenant_id AND rt.scope_kind = sl.scope_kind \
                             AND rt.scope_id = sl.scope_id AND rt.domain = sl.domain \
                             AND rt.projection_kind = sl.projection_kind \
                             AND rt.projection_version = sl.projection_version \
                             AND rt.stream_seq = sl.stream_seq) AS generation \
             FROM projection.stream_log sl \
             JOIN ops.outbox o ON o.tenant_id = sl.tenant_id AND o.commit_seq = sl.commit_seq \
            WHERE sl.tenant_id = $1 AND sl.scope_kind = $2 AND sl.scope_id = $3 AND sl.domain = $4 \
              AND sl.projection_kind = $5 AND sl.projection_version = $6 AND sl.stream_seq <= $7 \
              AND o.evidence_id IS NOT NULL \
         ), inputs AS ( \
           SELECT evidence_id, min(commit_seq) FILTER (WHERE NOT generation) AS home_commit, \
                  (array_agg(state ORDER BY stream_seq DESC) FILTER (WHERE NOT generation))[1] AS latest_g1, \
                  (array_agg(state ORDER BY stream_seq DESC))[1] AS latest_state, \
                  (array_agg(error_class ORDER BY stream_seq DESC))[1] AS latest_class \
             FROM tickets GROUP BY evidence_id \
         ) \
         SELECT i.home_commit, i.latest_state, i.latest_class, \
                EXISTS (SELECT 1 FROM projection.rebuild_tickets rt \
                         WHERE rt.run_id = $10 AND rt.commit_seq = i.home_commit) AS in_run, \
                CASE \
                  WHEN EXISTS (SELECT 1 FROM ops.outbox ob \
                                 JOIN projection.stream_log ts \
                                   ON ts.tenant_id = ob.tenant_id AND ts.commit_seq = ob.commit_seq \
                                WHERE ob.tenant_id = $1 AND ob.evidence_id = i.evidence_id \
                                  AND ts.state = 'TOMBSTONED') THEN 'tombstoned' \
                  WHEN i.latest_g1 = 'RETIRED_FAILED' THEN 'retired_by_operator' \
                  WHEN NOT EXISTS (SELECT 1 FROM private.memory_evidence me WHERE me.evidence_id = i.evidence_id) \
                   AND EXISTS (SELECT 1 FROM ops.outbox ea WHERE ea.tenant_id = $1 \
                                  AND ea.evidence_id = i.evidence_id AND ea.event_type = 'EVIDENCE_ACCEPTED' \
                                  AND ea.status IN ('PENDING', 'PROCESSING')) THEN 'distill_pending' \
                  WHEN $9 AND EXISTS (SELECT 1 FROM private.memory_evidence me \
                                        JOIN private.memory_records m \
                                          ON m.memory_id = me.memory_id AND m.tenant_id = $1 \
                                       WHERE me.evidence_id = i.evidence_id AND m.status = 'active' \
                                         AND NOT EXISTS (SELECT 1 FROM projection.memory_vectors v \
                                                          WHERE v.tenant_id = $1 AND v.memory_id = m.memory_id \
                                                            AND v.fingerprint_sha256 = $8 \
                                                            AND v.vector IS NOT NULL)) \
                    THEN 'without_stored_vector' \
                END AS excluded \
           FROM inputs i WHERE i.home_commit IS NOT NULL ORDER BY i.home_commit",
        key,
    )
    .bind(h2)
    .bind(fingerprint)
    .bind(require_stored_vector)
    .bind(run)
    .fetch_all(&mut **txn)
    .await?;
    rows.iter()
        .map(|row| {
            Ok(Input {
                in_run: row.try_get("in_run")?,
                home_commit: row.try_get("home_commit")?,
                excluded: row.try_get("excluded")?,
                latest_state: row.try_get("latest_state")?,
                latest_class: row.try_get("latest_class")?,
            })
        })
        .collect()
}

/// `(state, error_class, EVIDENCE_ACCEPTED status, evidence present)` of every generation ticket of `run`.
async fn read_generation(
    txn: &mut Txn,
    run: Uuid,
) -> Result<Vec<(String, Option<String>, Option<String>, bool)>> {
    let rows = sqlx::query(
        "SELECT sl.state, sl.error_class, \
                (SELECT ea.status FROM ops.outbox ea \
                  WHERE ea.tenant_id = sl.tenant_id AND ea.evidence_id = o.evidence_id \
                    AND ea.event_type = 'EVIDENCE_ACCEPTED' ORDER BY ea.created_at DESC LIMIT 1) AS ea_status, \
                EXISTS (SELECT 1 FROM private.evidence_objects eo WHERE eo.evidence_id = o.evidence_id) AS present \
           FROM projection.rebuild_tickets rt \
           JOIN projection.stream_log sl \
             ON sl.tenant_id = rt.tenant_id AND sl.scope_kind = rt.scope_kind AND sl.scope_id = rt.scope_id \
            AND sl.domain = rt.domain AND sl.projection_kind = rt.projection_kind \
            AND sl.projection_version = rt.projection_version AND sl.stream_seq = rt.stream_seq \
           LEFT JOIN ops.outbox o ON o.tenant_id = sl.tenant_id AND o.commit_seq = sl.commit_seq \
          WHERE rt.run_id = $1",
    )
    .bind(run)
    .fetch_all(&mut **txn)
    .await?;
    rows.iter()
        .map(|row| {
            Ok((
                row.try_get("state")?,
                row.try_get("error_class")?,
                row.try_get("ea_status")?,
                row.try_get("present")?,
            ))
        })
        .collect()
}

async fn read_registry(
    txn: &mut Txn,
    key: &StreamKey,
    label: &str,
) -> Result<BTreeMap<Uuid, Registered>> {
    let rows = key_query(
        "SELECT p.point_id, p.memory_id, p.fingerprint_sha256, v.vector \
           FROM projection.private_memory_points p \
           LEFT JOIN projection.memory_vectors v \
             ON v.tenant_id = p.tenant_id AND v.memory_id = p.memory_id \
            AND v.fingerprint_sha256 = p.fingerprint_sha256 AND v.input_sha256 = p.input_sha256 \
          WHERE p.tenant_id = $1 AND p.scope_kind = $2 AND p.scope_id = $3 AND p.domain = $4 \
            AND p.projection_kind = $5 AND p.projection_version = $6 AND p.embedding_version = $7 \
            AND p.projection_live",
        key,
    )
    .bind(label)
    .fetch_all(&mut **txn)
    .await?;
    let mut registry = BTreeMap::new();
    for row in &rows {
        registry.insert(
            row.try_get::<Uuid, _>("point_id")?,
            Registered {
                memory_id: row.try_get("memory_id")?,
                fingerprint: row.try_get("fingerprint_sha256")?,
                vector: row.try_get("vector")?,
            },
        );
    }
    Ok(registry)
}

/// The memories among `memories` that fail read_materialize's §37 hydrate gate (any TOMBSTONED ticket reached
/// through memory_evidence → outbox → stream_log): T of D-F E3, reused, not re-derived (§23.1②).
async fn read_tombstoned(txn: &mut Txn, tenant: Uuid, memories: &[Uuid]) -> Result<BTreeSet<Uuid>> {
    let rows: Vec<Uuid> = sqlx::query_scalar(
        "SELECT m FROM unnest($2::uuid[]) AS m \
          WHERE EXISTS (SELECT 1 FROM private.memory_evidence me \
                          JOIN ops.outbox ob ON ob.tenant_id = $1 AND ob.evidence_id = me.evidence_id \
                          JOIN projection.stream_log sl \
                            ON sl.tenant_id = ob.tenant_id AND sl.commit_seq = ob.commit_seq \
                         WHERE me.memory_id = m AND sl.state = 'TOMBSTONED')",
    )
    .bind(tenant)
    .bind(memories)
    .fetch_all(&mut **txn)
    .await?;
    Ok(rows.into_iter().collect())
}

fn key_query<'q>(
    sql: &'q str,
    key: &'q StreamKey,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    sqlx::query(sql)
        .bind(key.tenant_id.0)
        .bind(&key.scope_kind)
        .bind(key.scope_id)
        .bind(&key.domain)
        .bind(&key.projection_kind)
        .bind(&key.projection_version)
}

/// The deterministic FAILED outcomes PG implies for an input (D-F E2's allowed set): their memories may be in X
/// and absent from R.
fn outcome_excluded(state: &str, class: Option<&str>) -> bool {
    state == "FAILED"
        && class.is_some_and(|c| {
            DETERMINISTIC_CLASSES.contains(&c)
                || c == "distill_failed"
                || c == "no_visible_memory_record"
        })
}

async fn read_pg_side(
    deps: &RebuildDeps<'_>,
    stream: &Stream,
    run: Option<Uuid>,
    require_stored_vector: bool,
) -> Result<PgSide> {
    let key = &stream.key;
    let mut txn = begin_projector_read(deps.reader, key.tenant_id.0).await?;
    let h2: i64 = key_query(
        "SELECT issued_highwater FROM projection.stream_checkpoints \
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
            AND projection_kind = $5 AND projection_version = $6",
        key,
    )
    .fetch_one(&mut *txn)
    .await?
    .try_get("issued_highwater")?;
    // ADR-0064 D-N(g) / finding 18: in the drill (`require_stored_vector`) the only claimer is the run-scoped pass,
    // so a restored non-generation ticket is frozen quarantine state that can never write (its input is excluded
    // `without_stored_vector` or re-projected by the run); only the generation's own tickets count as in flight.
    let stream_in_flight: bool = key_query(
        &format!(
            "SELECT EXISTS (SELECT 1 FROM projection.stream_log sl \
              WHERE sl.tenant_id = $1 AND sl.scope_kind = $2 AND sl.scope_id = $3 AND sl.domain = $4 \
                AND sl.projection_kind = $5 AND sl.projection_version = $6 AND sl.stream_seq <= $7 \
                AND sl.state IN {IN_FLIGHT} \
                AND (NOT $8 OR EXISTS (SELECT 1 FROM projection.rebuild_tickets rt \
                       WHERE rt.tenant_id = sl.tenant_id AND rt.scope_kind = sl.scope_kind \
                         AND rt.scope_id = sl.scope_id AND rt.domain = sl.domain \
                         AND rt.projection_kind = sl.projection_kind \
                         AND rt.projection_version = sl.projection_version AND rt.stream_seq = sl.stream_seq)))"
        ),
        key,
    )
    .bind(h2)
    .bind(require_stored_vector)
    .fetch_one(&mut *txn)
    .await?
    .try_get(0)?;
    let registry = read_registry(&mut txn, key, &deps.worker.label).await?;
    let inputs = read_inputs(
        &mut txn,
        key,
        h2,
        &deps.worker.fingerprint,
        require_stored_vector,
        run,
    )
    .await?;
    let family = stream.family();
    let mut excluded = BTreeMap::new();
    let mut expected = BTreeMap::new();
    for input in &inputs {
        if let Some(class) = &input.excluded {
            // D-E step 4's count: what the definer left out. A forget after issue is the ticket's outcome (E2).
            if !input.in_run {
                *excluded.entry(class.clone()).or_insert(0) += 1;
            }
            continue;
        }
        let by_outcome = outcome_excluded(&input.latest_state, input.latest_class.as_deref());
        for point in projection_worker::expected_points(
            &mut txn,
            &family,
            &deps.worker.label,
            &key.projection_version,
            input.home_commit,
        )
        .await?
        {
            expected.insert(point.point_id, (point, by_outcome));
        }
    }
    let mut memories: Vec<Uuid> = registry.values().map(|r| r.memory_id).collect();
    memories.extend(expected.values().map(|(p, _)| p.memory_id));
    let tombstoned_memories = read_tombstoned(&mut txn, key.tenant_id.0, &memories).await?;
    let generation = match run {
        Some(run) => read_generation(&mut txn, run).await?,
        None => Vec::new(),
    };
    txn.commit().await?;
    Ok(PgSide {
        h2,
        stream_in_flight,
        registry,
        tombstoned_memories,
        expected,
        excluded,
        generation,
    })
}

/// D-F E2: the terminal `(state, class)` of every generation ticket must be one PG implies for its input.
fn judge_generation(
    generation: &[(String, Option<String>, Option<String>, bool)],
) -> (BTreeMap<String, i64>, BTreeMap<String, i64>, i64) {
    let mut by_outcome = BTreeMap::new();
    let mut failed = BTreeMap::new();
    let mut in_flight = 0;
    for (state, class, ea_status, present) in generation {
        let class_str = class.as_deref().unwrap_or("");
        let allowed = match state.as_str() {
            "ISSUED" | "PROCESSING" | "WAITING_KEY" | "RETRY_WAIT" => {
                in_flight += 1;
                continue;
            }
            "DONE" | "SKIPPED_BY_POLICY" => continue,
            // A forget during the run (the 0233 follow trigger): its memories are in T on both sides.
            "TOMBSTONED" => Some("tombstoned"),
            "FAILED" if DETERMINISTIC_CLASSES.contains(&class_str) => Some(class_str),
            "FAILED" if class_str == "distill_failed" && ea_status.as_deref() == Some("FAILED") => {
                Some(class_str)
            }
            "FAILED"
                if class_str == "no_visible_memory_record" && (ea_status.is_none() || !present) =>
            {
                Some(class_str)
            }
            _ => None,
        };
        match allowed {
            Some(c) => *by_outcome.entry(c.to_owned()).or_insert(0) += 1,
            None => {
                let label = if state == "FAILED" {
                    class_str
                } else {
                    state.as_str()
                };
                *failed.entry(label.to_owned()).or_insert(0) += 1;
            }
        }
    }
    (by_outcome, failed, in_flight)
}

/// D-F E4 leaf: `sha256(point_id bytes ‖ sha256(canonical JSON))`; serde_json's map is sorted, arrays keep the
/// builder order, `source_stream_seq` is removed by the caller.
fn leaf(id: Uuid, payload: &Map<String, Value>) -> [u8; 32] {
    let canonical = serde_json::to_vec(payload).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(id.as_bytes());
    hasher.update(Sha256::digest(&canonical));
    hasher.finalize().into()
}

/// D-F E5: Qdrant normalises a Cosine vector on upload; the stored provider vector is normalised the same way in f32.
fn vector_matches(stored: &[f32], qdrant: &[f32]) -> bool {
    let norm = stored.iter().map(|v| v * v).sum::<f32>().sqrt();
    norm > 0.0
        && stored.len() == qdrant.len()
        && stored
            .iter()
            .zip(qdrant)
            .all(|(s, q)| (s / norm - q).abs() <= VECTOR_TOLERANCE)
}

fn first_ids(ids: impl IntoIterator<Item = Uuid>) -> Vec<Uuid> {
    ids.into_iter().take(DIFF_REPORT_LIMIT).collect()
}

/// D-F E1: the collection's canonical config digest (dimension from the fingerprint row, both payload indexes)
/// and status `green`. Returns the failed terms.
async fn judge_collection(deps: &RebuildDeps<'_>, stream: &Stream) -> Result<Vec<String>> {
    let mut reasons = Vec::new();
    match provisioning::collection_generation(
        deps.qdrant,
        &stream.collection,
        deps.worker.dimension,
    )
    .await
    {
        Ok(_) => {}
        Err(ProvisioningError::Refused(reason)) => reasons.push(format!("e1:{reason}")),
        Err(other) => return Err(other),
    }
    let (transport, permit) = deps.qdrant.wire()?;
    // dep: Qdrant(*) — the collection's status (E1)
    let status = qdrant::collection_status(transport, &permit, &stream.collection)
        .await
        .map_err(|e| ProvisioningError::Qdrant(e.to_string()))?;
    if status != "green" {
        reasons.push(format!("e1:collection_status={status}"));
    }
    Ok(reasons)
}

/// D-F E4 + E5 over `r` (R \ T, sorted): the PG-side payload Merkle root, the ids whose payload leaf differs and
/// the ids whose fingerprint or vector does not match.
fn judge_points(
    deps: &RebuildDeps<'_>,
    pg: &PgSide,
    scrolled: &Scrolled,
    r: &BTreeSet<Uuid>,
) -> ([u8; 32], Vec<Uuid>, Vec<Uuid>) {
    let mut pg_hasher = Sha256::new();
    let mut payload_diff = Vec::new();
    let mut vector_diff = Vec::new();
    for id in r {
        let pg_leaf = pg.expected.get(id).map(|(p, _)| leaf(*id, &p.payload));
        let q_leaf = scrolled.label.get(id).map(|p| {
            let mut payload = p.payload.clone();
            payload.remove(qdrant::SOURCE_STREAM_SEQ_FIELD);
            leaf(*id, &payload)
        });
        if let Some(l) = pg_leaf {
            pg_hasher.update(l);
        }
        if pg_leaf.is_none() || pg_leaf != q_leaf {
            payload_diff.push(*id);
        }
        let ok = pg.registry.get(id).is_some_and(|registered| {
            registered.fingerprint.as_deref() == Some(&deps.worker.fingerprint[..])
                && match (
                    &registered.vector,
                    scrolled.label.get(id).and_then(|p| p.vector.as_ref()),
                ) {
                    (Some(stored), Some(in_qdrant)) => vector_matches(stored, in_qdrant),
                    _ => false,
                }
        });
        if !ok {
            vector_diff.push(*id);
        }
    }
    (pg_hasher.finalize().into(), payload_diff, vector_diff)
}

/// D-F E3's sets: T (registry points whose memory fails the hydrate gate), R \ T, and the four differences.
struct IdSets {
    t_points: BTreeSet<Uuid>,
    r: BTreeSet<Uuid>,
    missing: Vec<Uuid>,
    orphans: Vec<Uuid>,
    unexpected: Vec<Uuid>,
    unprojected: Vec<Uuid>,
}

fn id_sets(pg: &PgSide, scrolled: &Scrolled) -> IdSets {
    let t_points: BTreeSet<Uuid> = pg
        .registry
        .iter()
        .filter(|(_, r)| pg.tombstoned_memories.contains(&r.memory_id))
        .map(|(id, _)| *id)
        .collect();
    let q: BTreeSet<Uuid> = scrolled
        .label
        .keys()
        .copied()
        .filter(|id| !t_points.contains(id))
        .collect();
    let r: BTreeSet<Uuid> = pg
        .registry
        .keys()
        .copied()
        .filter(|id| !t_points.contains(id))
        .collect();
    let x: BTreeSet<Uuid> = pg
        .expected
        .iter()
        .filter(|(_, (p, _))| !pg.tombstoned_memories.contains(&p.memory_id))
        .map(|(id, _)| *id)
        .collect();
    let missing: Vec<Uuid> = r.difference(&q).copied().collect();
    let orphans: Vec<Uuid> = q.difference(&r).copied().collect();
    let unexpected: Vec<Uuid> = r.difference(&x).copied().collect();
    let unprojected: Vec<Uuid> = x
        .difference(&r)
        .copied()
        .filter(|id| {
            !pg.expected
                .get(id)
                .is_some_and(|(_, by_outcome)| *by_outcome)
        })
        .collect();
    IdSets {
        t_points,
        r,
        missing,
        orphans,
        unexpected,
        unprojected,
    }
}

/// D-F: E1–E5 of `stream` against Project(PG@H2) from one PG snapshot and one Qdrant scroll. Never writes.
async fn judge(
    deps: &RebuildDeps<'_>,
    stream: &Stream,
    pg: &PgSide,
    scrolled: &Scrolled,
) -> Result<Report> {
    let mut reasons = judge_collection(deps, stream).await?;
    // E2
    let (by_outcome, failed, in_flight) = judge_generation(&pg.generation);
    for (class, n) in &failed {
        reasons.push(format!("rebuild_tickets_failed:{class}={n}"));
    }
    // E3
    let IdSets {
        t_points,
        r,
        missing,
        orphans,
        unexpected,
        unprojected,
    } = id_sets(pg, scrolled);
    let total = scrolled.label.len() as u64 + scrolled.other_label + scrolled.numeric_ids;
    if scrolled.count_exact != total {
        reasons.push(format!(
            "e3:count_exact={} scrolled={total}",
            scrolled.count_exact
        ));
    }
    for (name, ids) in [
        ("e3:registered_not_in_qdrant", &missing),
        ("e3:qdrant_not_registered", &orphans),
        ("e3:registered_not_in_pg_projection", &unexpected),
        ("e3:pg_projection_not_registered", &unprojected),
    ] {
        if !ids.is_empty() {
            reasons.push(format!("{name}={}", ids.len()));
        }
    }
    if scrolled.numeric_ids > 0 {
        reasons.push(format!("e3:numeric_ids={}", scrolled.numeric_ids));
    }
    // E4 + E5 over R \ T
    let (merkle_root, payload_diff, vector_diff) = judge_points(deps, pg, scrolled, &r);
    if !payload_diff.is_empty() {
        reasons.push(format!("e4:payload_merkle_differs={}", payload_diff.len()));
    }
    if !vector_diff.is_empty() {
        reasons.push(format!("e5:vectors_differ={}", vector_diff.len()));
    }
    let verdict = if in_flight > 0 || pg.stream_in_flight {
        reasons.insert(0, "generation_in_flight".to_owned());
        Verdict::CannotEstablish
    } else if reasons.is_empty() {
        Verdict::Equivalent
    } else {
        Verdict::NotEquivalent
    };
    let legacy = pg
        .registry
        .values()
        .filter(|r| r.fingerprint.is_none())
        .count();
    let mut json = stream.receipt_head();
    json["verdict"] = json!(verdict.as_db_str());
    json["reasons"] = json!(reasons);
    json["h2"] = json!(pg.h2);
    json["points"] = json!(r.len());
    json["merkle_root"] = json!(hex::encode(merkle_root));
    json["other_label_points"] = json!(scrolled.other_label);
    json["tombstoned_unpurged_points"] = json!(t_points.len());
    json["excluded"] = json!(pg.excluded);
    json["excluded_by_outcome"] = json!(by_outcome);
    json["legacy_points_without_vector"] = json!(legacy);
    json["differing_ids"] = json!({
        "registered_not_in_qdrant": first_ids(missing),
        "qdrant_not_registered": first_ids(orphans.iter().copied()),
        "registered_not_in_pg_projection": first_ids(unexpected),
        "pg_projection_not_registered": first_ids(unprojected),
        "payload": first_ids(payload_diff),
        "vector": first_ids(vector_diff),
    });
    Ok(Report {
        verdict,
        h2: pg.h2,
        points: i64::try_from(r.len()).unwrap_or(i64::MAX),
        merkle_root,
        json,
        orphans,
    })
}

/// D-F read-only verify of one stream (`projection verify`): E1–E5 against Project(PG@H2); E2 judges the newest
/// rebuild run's tickets when `run` is given. No PostgreSQL write (READ ONLY transactions), no Qdrant write; a
/// moved `issued_highwater` between the two PG reads is `cannot_establish: boundary_moved`.
pub async fn verify_stream(
    deps: &RebuildDeps<'_>,
    stream: &Stream,
    run: Option<Uuid>,
    require_stored_vector: bool,
) -> Result<Report> {
    let pg = read_pg_side(deps, stream, run, require_stored_vector).await?;
    let scrolled = scroll_stream(deps, stream).await?;
    let mut report = judge(deps, stream, &pg, &scrolled).await?;
    let h2_after = read_h2(deps, stream).await?;
    if h2_after != pg.h2 {
        report.verdict = Verdict::CannotEstablish;
        report.json["verdict"] = json!(Verdict::CannotEstablish.as_db_str());
        report.json["reasons"] = json!(["boundary_moved"]);
    }
    Ok(report)
}

async fn read_h2(deps: &RebuildDeps<'_>, stream: &Stream) -> Result<i64> {
    let mut txn = begin_projector_read(deps.reader, stream.key.tenant_id.0).await?;
    let h2: i64 = key_query(
        "SELECT issued_highwater FROM projection.stream_checkpoints \
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
            AND projection_kind = $5 AND projection_version = $6",
        &stream.key,
    )
    .fetch_one(&mut *txn)
    .await?
    .try_get(0)?;
    txn.commit().await?;
    Ok(h2)
}

/// What the orphan step did (D-F E3, rebuild mode only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrphanStep {
    /// These ids were deleted, fenced at H2.
    Deleted(Vec<Uuid>),
    /// A ticket of the stream at or below H2 was in flight: nothing deleted (`cannot_establish: generation_in_flight`).
    InFlight,
}

/// D-F orphan deletion under the quiescent rule (review finding 15): AFTER the scroll, ONE statement reads the
/// registry `R_after` and whether any ticket of the stream at or below `h2` is in flight, in the same snapshot.
/// In flight → nothing is deleted. Otherwise the scrolled label points absent from `R_after` are deleted with the
/// seq fence at `h2` (a point of a later ticket carries `source_stream_seq > h2` and the fence spares it).
pub async fn delete_orphans(
    deps: &RebuildDeps<'_>,
    stream: &Stream,
    h2: i64,
    scrolled: &Scrolled,
) -> Result<OrphanStep> {
    let mut txn = begin_projector_read(deps.reader, stream.key.tenant_id.0).await?;
    let row = key_query(
        &format!(
            "SELECT ARRAY(SELECT point_id FROM projection.private_memory_points \
                           WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
                             AND projection_kind = $5 AND projection_version = $6 \
                             AND embedding_version = $7 AND projection_live) AS r_after, \
                    EXISTS (SELECT 1 FROM projection.stream_log \
                             WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
                               AND projection_kind = $5 AND projection_version = $6 \
                               AND stream_seq <= $8 AND state IN {IN_FLIGHT}) AS in_flight"
        ),
        &stream.key,
    )
    .bind(&deps.worker.label)
    .bind(h2)
    .fetch_one(&mut *txn)
    .await?;
    txn.commit().await?;
    if row.try_get::<bool, _>("in_flight")? {
        return Ok(OrphanStep::InFlight);
    }
    let r_after: BTreeSet<Uuid> = row
        .try_get::<Vec<Uuid>, _>("r_after")?
        .into_iter()
        .collect();
    let orphans: Vec<Uuid> = scrolled
        .label
        .keys()
        .copied()
        .filter(|id| !r_after.contains(id))
        .collect();
    if !orphans.is_empty() {
        let (transport, permit) = deps.qdrant.wire()?;
        let ids: Vec<PointId> = orphans.iter().map(|id| PointId::Uuid(*id)).collect();
        // dep: Qdrant(*) — delete the orphans, fenced at H2
        qdrant::delete_points(
            transport,
            &permit,
            &stream.collection,
            &ids,
            h2,
            qdrant::ha_profile_for(QdrantOperation::CorrectionDeleteSupersede),
        )
        .await
        .map_err(|e| ProvisioningError::Qdrant(e.to_string()))?;
    }
    Ok(OrphanStep::Deleted(orphans))
}

/// D-E step 6: verify the run at H2; when the only defect is label points absent from the registry, run the
/// orphan step and verify once more.
pub async fn verify_run(
    deps: &RebuildDeps<'_>,
    stream: &Stream,
    run: &Run,
    require_stored_vector: bool,
) -> Result<(Report, u64)> {
    let report = verify_stream(deps, stream, Some(run.run_id), require_stored_vector).await?;
    if report.orphans.is_empty() || report.verdict == Verdict::CannotEstablish {
        return Ok((report, 0));
    }
    let scrolled = scroll_stream(deps, stream).await?;
    match delete_orphans(deps, stream, report.h2, &scrolled).await? {
        OrphanStep::InFlight => {
            let mut report = report;
            report.verdict = Verdict::CannotEstablish;
            report.json["verdict"] = json!(Verdict::CannotEstablish.as_db_str());
            report.json["reasons"] = json!(["generation_in_flight"]);
            Ok((report, 0))
        }
        OrphanStep::Deleted(ids) => {
            let again =
                verify_stream(deps, stream, Some(run.run_id), require_stored_vector).await?;
            Ok((again, ids.len() as u64))
        }
    }
}

// ============================================================================
// The one-stream orchestration
// ============================================================================

/// One stream's knobs (every value from a required CLI flag; §78.1: none has a default).
pub struct RebuildOptions<'p, 'a> {
    pub batch: i32,
    pub wait: Duration,
    /// D-G consent: the most legacy (NULL-fingerprint) points this run may re-embed.
    pub allow_reembed: Option<u64>,
    /// The drill (D-E step 4): inputs without a stored vector are excluded and reported, never embedded.
    pub require_stored_vector: bool,
    pub pump: &'p Pump<'a>,
}

/// One stream's outcome: the verdict, whether it was a refusal (exit 3), and its receipt object.
#[derive(Debug, Clone)]
pub struct StreamOutcome {
    pub verdict: Verdict,
    pub refused: bool,
    pub receipt: Value,
}

/// ADR-0064 D-E steps 1–7 for one stream. A precheck or `rebuild_open` refusal is `refused` (nothing issued); a
/// generation still in flight after `wait` leaves the run open (`cannot_establish`, the next call resumes it).
pub async fn rebuild_stream(
    deps: &RebuildDeps<'_>,
    stream: &Stream,
    opts: &RebuildOptions<'_, '_>,
) -> Result<StreamOutcome> {
    let refused = |receipt: Value| StreamOutcome {
        verdict: Verdict::ReEmbedRequired,
        refused: true,
        receipt,
    };
    let legacy = match precheck(deps, stream, opts.allow_reembed).await? {
        Ok(legacy) => legacy,
        Err(receipt) => return Ok(refused(receipt)),
    };
    let run = match open_run(deps, stream).await {
        Ok(run) => run,
        Err(ProvisioningError::Refused(reason)) => {
            let mut receipt = stream.receipt_head();
            receipt["verdict"] = json!("refused");
            receipt["reason"] = json!(reason);
            return Ok(StreamOutcome {
                verdict: Verdict::CannotEstablish,
                refused: true,
                receipt,
            });
        }
        Err(other) => return Err(other),
    };
    provisioning::ensure_collection(deps.qdrant, &stream.collection, deps.worker.dimension).await?;
    let issued = issue_tickets(deps, stream, &run, opts.batch, opts.require_stored_vector).await?;
    let settled = wait_generation(deps, stream, &run, opts.wait, opts.pump).await?;
    let (mut report, orphans_deleted) = if settled {
        verify_run(deps, stream, &run, opts.require_stored_vector).await?
    } else {
        let mut report =
            verify_stream(deps, stream, Some(run.run_id), opts.require_stored_vector).await?;
        report.verdict = Verdict::CannotEstablish;
        report.json["verdict"] = json!(Verdict::CannotEstablish.as_db_str());
        (report, 0)
    };
    report.json["run_id"] = json!(run.run_id);
    report.json["generation"] = json!(run.generation);
    report.json["boundary_seq"] = json!(run.boundary_seq);
    report.json["resumed"] = json!(run.resumed);
    report.json["issued"] = json!(issued);
    report.json["orphans_deleted"] = json!(orphans_deleted);
    // D-G: the consent is recorded in the report (0233's rebuild_open takes no consent parameter).
    report.json["reembed_allowed"] = json!(opts.allow_reembed.unwrap_or(0));
    report.json["re_embed"] = json!(legacy > 0);
    if report.verdict != Verdict::CannotEstablish {
        match close_run(deps, stream, &run, &report).await {
            Ok(()) => report.json["closed"] = json!(true),
            Err(ProvisioningError::Refused(reason)) => {
                report.verdict = Verdict::CannotEstablish;
                report.json["verdict"] = json!(Verdict::CannotEstablish.as_db_str());
                report.json["close_refused"] = json!(reason);
            }
            Err(other) => return Err(other),
        }
    }
    Ok(StreamOutcome {
        verdict: report.verdict,
        refused: false,
        receipt: report.json,
    })
}

// ============================================================================
// D-M closed no-provider deps
// ============================================================================

/// ADR-0064 D-M: the drill's only embedder. It never reaches a provider: every call counts one attempt and refuses
/// `DependencyUnavailable`, so `attempts() > 0` means the stored-vector path was missed.
#[derive(Debug, Default)]
pub struct NoProviderEmbedder {
    attempts: AtomicU64,
}

impl NoProviderEmbedder {
    /// Calls refused so far.
    pub fn attempts(&self) -> u64 {
        self.attempts.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl CardEmbedder for NoProviderEmbedder {
    async fn embed_cards(
        &self,
        _tenant_id: TenantId,
        _dimension: u32,
        _cards: &[SealedRetrievalCard],
        _memory_ids: &[Uuid],
    ) -> std::result::Result<Vec<Vec<f32>>, ErrorCode> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(ErrorCode::DependencyUnavailable)
    }
}

/// Every part of [`SharedProjectionDeps`] except the embedder, which [`drill_projection_deps`] supplies itself.
pub struct ClosedDeps {
    pub pool: RetrievalWorkerDbPool,
    pub scanner: Arc<LocalSecretScanner>,
    pub transport: Arc<dyn IntraCellHttpTransport>,
    pub mint_permit: Arc<dyn Fn() -> Option<CellAccessPermit> + Send + Sync>,
    pub embedding_version: String,
    pub dimension: u32,
    pub processor_id: ProcessorId,
    pub sleep: Arc<dyn Fn(Duration) -> Sleep + Send + Sync>,
}

/// ADR-0064 D-M: the closed constructor of the drill's projection deps. It takes no embedder: the only one it can
/// hold is the [`NoProviderEmbedder`] it builds, returned beside the deps so the caller can read its count.
pub fn drill_projection_deps(parts: ClosedDeps) -> (SharedProjectionDeps, Arc<NoProviderEmbedder>) {
    let refusing = Arc::new(NoProviderEmbedder::default());
    let deps = SharedProjectionDeps {
        pool: parts.pool,
        embedder: refusing.clone(),
        scanner: parts.scanner,
        transport: parts.transport,
        mint_permit: parts.mint_permit,
        embedding_version: parts.embedding_version,
        dimension: parts.dimension,
        processor_id: parts.processor_id,
        sleep: parts.sleep,
        dependency_down: AtomicBool::new(false),
    };
    (deps, refusing)
}

/// ADR-0064 D-N(g): the collection names the Qdrant behind `face` holds. The drill writes only into a Qdrant that
/// answers none (`drill_qdrant_not_fresh` otherwise): a live Qdrant always has collections.
pub async fn collection_names(face: &QdrantFace) -> Result<Vec<String>> {
    let (transport, permit) = face.wire()?;
    // dep: Qdrant(*) — GET /collections on the drill's own Qdrant
    let response = transport
        .execute(
            &permit,
            IntraCellRequest {
                method: IntraCellMethod::Get,
                path: "/collections".to_owned(),
                json_body: None,
                headers: Vec::new(),
            },
        )
        .await
        .map_err(|e| ProvisioningError::Qdrant(format!("{e:?}")))?;
    if response.status != 200 {
        return Err(ProvisioningError::Qdrant(format!(
            "GET /collections: status {}",
            response.status
        )));
    }
    Ok(response
        .json_body
        .as_ref()
        .and_then(|b| b["result"]["collections"].as_array().cloned())
        .unwrap_or_default()
        .iter()
        .filter_map(|c| c["name"].as_str().map(str::to_owned))
        .collect())
}

/// The pinned gitleaks binary the projector's scanner checks (the retrieval worker's three keys, read by the caller
/// under the worker's own names).
pub struct ScannerPin {
    pub executable: std::path::PathBuf,
    pub version: String,
    pub sha256: String,
}

/// ADR-0064 D-M / D-N(g): every part of [`ClosedDeps`] but the pool, built for the DRILL's own Qdrant at
/// `127.0.0.1:qdrant_port` (from `docker compose port`, CIDR the loopback /32): a fresh transport and permit minter
/// that can reach nothing else, and the pinned scanner (the retrieval worker's literals: 5 s, 64 KiB, exit 1). The
/// stored-vector path never seals a card, so the scanner is held, not called.
pub fn drill_closed_deps(
    pool: RetrievalWorkerDbPool,
    qdrant_port: u16,
    scanner: &ScannerPin,
    embedding_version: &str,
    dimension: u32,
    processor_id: ProcessorId,
    sleep: Arc<dyn Fn(Duration) -> Sleep + Send + Sync>,
) -> Result<ClosedDeps> {
    let cell = CellId(Uuid::now_v7());
    let caller = CallerId("humaux-maintenance restore drill".to_owned());
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            "127.0.0.1",
            qdrant_port,
            cell,
            vec![
                "127.0.0.1/32"
                    .parse()
                    .map_err(|_| ProvisioningError::InvalidInput("loopback cidr".to_owned()))?,
            ],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .map_err(|e| ProvisioningError::InvalidInput(format!("drill qdrant entry: {e}")))?,
    );
    let registry = IntraCellResourceRegistry::new(entries, cell, caller);
    let transport = HttpIntraCellTransport::new(
        registry.clone(),
        Duration::from_secs(10),
        humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
    )
    .map_err(|e| ProvisioningError::Qdrant(format!("drill transport: {e}")))?;
    let scanner = LocalSecretScanner::new(LocalSecretScannerConfig {
        executable: scanner.executable.clone(),
        expected_version: scanner.version.clone(),
        expected_executable_sha256: scanner.sha256.clone(),
        timeout: Duration::from_secs(5),
        max_payload_bytes: 64 * 1024,
        finding_exit_code: 1,
    })
    .map_err(|e| ProvisioningError::InvalidInput(format!("scanner pin: {e:?}")))?;
    Ok(ClosedDeps {
        pool,
        scanner: Arc::new(scanner),
        transport: Arc::new(transport),
        mint_permit: Arc::new(move || {
            authorize_cell_access(
                &registry,
                IntraCellResource::QDRANT_REST,
                Duration::from_secs(300),
            )
            .ok()
        }),
        embedding_version: embedding_version.to_owned(),
        dimension,
        processor_id,
        sleep,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// D-F E5: a stored vector normalised in f32 matches Qdrant's copy within the tolerance; a 1e-3 nudge does not.
    #[test]
    fn vectors_compare_after_cosine_normalisation() {
        let stored = [3.0_f32, 4.0, 0.0, 0.0];
        assert!(vector_matches(&stored, &[0.6, 0.8, 0.0, 0.0]));
        assert!(!vector_matches(&stored, &[0.601, 0.8, 0.0, 0.0]));
        assert!(!vector_matches(&stored, &[0.6, 0.8, 0.0]));
        assert!(!vector_matches(&[0.0; 4], &[0.0; 4]));
    }

    /// D-F E2: the allowed terminal outcomes are exactly the ones PG implies; anything else names its class.
    #[test]
    fn generation_outcomes_are_judged_against_pg() {
        let t = |s: &str, c: Option<&str>, ea: Option<&str>, present: bool| {
            (
                s.to_owned(),
                c.map(str::to_owned),
                ea.map(str::to_owned),
                present,
            )
        };
        let (ok, failed, in_flight) = judge_generation(&[
            t("DONE", None, Some("DONE"), true),
            t("FAILED", Some("card_unbuildable"), Some("DONE"), true),
            t("FAILED", Some("distill_failed"), Some("FAILED"), true),
            t("FAILED", Some("distill_failed"), Some("DONE"), true),
            t("FAILED", Some("no_visible_memory_record"), None, true),
            t("FAILED", Some("transient_exhausted"), Some("DONE"), true),
            t("LOST", None, Some("DONE"), true),
            t("ISSUED", None, Some("DONE"), true),
        ]);
        assert_eq!(ok.get("card_unbuildable"), Some(&1));
        assert_eq!(ok.get("distill_failed"), Some(&1));
        assert_eq!(ok.get("no_visible_memory_record"), Some(&1));
        assert_eq!(failed.get("distill_failed"), Some(&1));
        assert_eq!(failed.get("transient_exhausted"), Some(&1));
        assert_eq!(failed.get("LOST"), Some(&1));
        assert_eq!(in_flight, 1);
    }
}
