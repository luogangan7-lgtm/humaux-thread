//! `adapters::qdrant` — Qdrant multitenant placement adapter (§17, T5.4+T5.6).
//!
//! **Build status:** implements every part of §17 that does not require an HTTP client —
//! collection/index request-body shaping, payload encoding, the
//! [`Condition`](humaux_projection::dense::Condition) → Qdrant filter JSON translation, the
//! §17.5 HA consistency profile, and the §17.4 search-visible confirmation contract
//! ([`verify_visible`]/[`VisibilityConfirmation`]). It does not send any of these bodies over
//! the wire: `crates/adapters/Cargo.toml` has no HTTP client dependency.
//!
//! §83.4 G80-3 (`xtask/src/architecture_check.rs:1354`) requires the reqwest/hyper-dependent
//! manifest set to equal exactly `{crates/infra-egress/Cargo.toml}` and the raw-client
//! construction-point set to equal exactly `{crates/infra-egress/src/http.rs}` — there is no
//! "local Data Cell resource" exemption in that check. Adding `reqwest` to this crate's
//! `Cargo.toml` would turn G80-3 from pass to fail today. Wiring the HTTP layer therefore needs
//! one of, decided by ADR (§78.6) before any manifest changes:
//!   (a) route through `infra-egress::http::ExternalHttpTransport` behind a new intra-cell
//!       `OutboundPurpose`, kept in lockstep with the external-egress registry (§83.4 判据2);
//!   (b) an ADR opening a "Data Cell internal resource" layer in §83.4, with
//!       `g80_3_manifest_dependency_check`/`g80_3_transport_and_registry_check` updated to
//!       match and its own fault-injection test added;
//!   (c) go through the existing `crates/infra-egress/src/http.rs` client as-is.
//! Not this module's call to make unilaterally. Once decided, `upsert`/`search`/`count`/the
//! real body of `verify_visible`'s `check_visible` callback are thin wrappers around the
//! functions already in this file.
//!
//! §3/§78.3: this crate sits below Domain in the dependency direction (adapters wraps HTTP/
//! SQL, Domain never imports either) — not a boundary violation.

use humaux_domain::authority::{AuthorityClass, AuthorityStatus};
use humaux_domain::dataclass::DataClass;
use humaux_domain::identity::{AuthorizationScope, VisibilityClass};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_domain::memory::MemoryType;
use humaux_projection::card::EgressDisposition;
use humaux_projection::dense::{Condition, DenseQueryFilter, FieldMatch, build_dense_filter};
use serde_json::{Value, json};
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

// ============================================================================
// §17 — three retrieval families
// ============================================================================

/// The three logical retrieval families §17 names — a closed set, no `Other`. This enum plus
/// [`RetrievalFamily::collection_name`] is the single control point for spelling a collection
/// name (§17: "collection/shard placement 必须由单独控制面管理，不能散落在业务代码").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RetrievalFamily {
    PrivateMemoryV1,
    PublicKnowledgeV1,
    CodeV1,
}

impl RetrievalFamily {
    /// Qdrant collection name for this family — §17's three names, verbatim.
    pub fn collection_name(self) -> &'static str {
        match self {
            Self::PrivateMemoryV1 => "private_memory_v1",
            Self::PublicKnowledgeV1 => "public_knowledge_v1",
            Self::CodeV1 => "code_v1",
        }
    }

    /// All three families, for callers that need to provision/inspect every collection
    /// (e.g. the tenant index bootstrap, §17.1).
    pub const ALL: [RetrievalFamily; 3] =
        [Self::PrivateMemoryV1, Self::PublicKnowledgeV1, Self::CodeV1];

    /// `projection.tenant_placements.projection_family`'s DB wire value (migration 0068's
    /// CHECK constraint). Same spelling as [`Self::collection_name`] today — a promoted
    /// tenant's `collection_name` can diverge from its `projection_family` once a dedicated
    /// collection gets its own name (§17.3's promotion path), so this stays a separate method
    /// rather than a type alias, even though nothing has diverged them yet.
    pub fn as_db_str(self) -> &'static str {
        self.collection_name()
    }
}

/// Qdrant's own distance-metric enum (not a Humaux type) — the three metrics Qdrant 1.19
/// documents for dense vectors. `vector_size` is a required parameter, never hardcoded here:
/// it is embedding-model-dependent business configuration (§78.1) and must come from the
/// caller's model config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Distance {
    Cosine,
    Dot,
    Euclid,
}

impl Distance {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cosine => "Cosine",
            Self::Dot => "Dot",
            Self::Euclid => "Euclid",
        }
    }
}

/// Qdrant's collection-level sharding mode. `Custom` is required for §17.3
/// `PlacementClass::Dedicated` tenants: it lets a later [`shard_key_body`] call
/// (`PUT /collections/{name}/shards`) create a named shard key that an upsert can target — the
/// default `Auto` mode rejects any point that carries a `shard_key`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardingMethod {
    Auto,
    Custom,
}

/// `PUT /collections/{name}` request body for one [`RetrievalFamily`] — dense-vector config
/// plus the §17.3/§17.5 collection-level placement/consistency parameters. None of
/// `shard_number`/`replication_factor`/`write_consistency_factor` are hardcoded (§78.1):
/// §17.5's own closing line is "具体值通过故障注入和压测冻结", so the caller's config supplies
/// them. §17.6's sparse/BM25 collection config is a separate, not-yet-frozen piece of this
/// same request body and is deliberately left out.
pub fn create_collection_body(
    vector_size: u64,
    distance: Distance,
    shard_number: u32,
    replication_factor: u32,
    write_consistency_factor: u32,
    sharding_method: ShardingMethod,
) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "vectors".into(),
        json!({ "size": vector_size, "distance": distance.as_str() }),
    );
    obj.insert("shard_number".into(), json!(shard_number));
    obj.insert("replication_factor".into(), json!(replication_factor));
    obj.insert(
        "write_consistency_factor".into(),
        json!(write_consistency_factor),
    );
    if sharding_method == ShardingMethod::Custom {
        obj.insert("sharding_method".into(), json!("custom"));
    }
    Value::Object(obj)
}

/// `PUT /collections/{name}/shards` request body — required once per shard key before any
/// point carrying that `shard_key` can be upserted into a [`ShardingMethod::Custom`]
/// collection (§17.3 `PlacementClass::Dedicated`; a bare `create_collection_body` alone leaves
/// `TenantPlacementRow.shard_key` write-only-but-unusable, Qdrant rejects the upsert with
/// "Shard key not found" until this call has run).
pub fn shard_key_body(shard_key: &str) -> Value {
    json!({ "shard_key": shard_key })
}

/// `PUT /collections/{name}/index` request body for the §17.1 hard constraint: `tenant_id`
/// must carry a keyword tenant index with `is_tenant = true`. One call per family collection
/// (§17.1 applies uniformly to all three; nothing in §17 carves out an exception).
pub fn tenant_index_body() -> Value {
    json!({
        "field_name": "tenant_id",
        "field_schema": {
            "type": "keyword",
            "is_tenant": true,
        }
    })
}

// ============================================================================
// §17 — payload schema (§17's own text block: "at least" 14 fields, plus §18.2's mandatory
// data_class/egress_disposition)
// ============================================================================

/// One Qdrant point's payload. Enum-typed fields reuse the existing Domain types rather than
/// re-deriving a second wire vocabulary (`VisibilityClass`/`AuthorityClass`/`AuthorityStatus`/
/// `MemoryType` are already the canonical Rust representation of the exact CHECK-constrained
/// columns `private.memory_records` persists).
///
/// `object_type` stays `String`: §17 does not freeze a closed set for it anywhere in the spec.
/// Upgrade to a typed enum once a §17-adjacent section freezes the set.
///
/// `embedding_version`/`projection_version` are `String`, matching the established convention
/// for the same two concepts elsewhere (`projection::stream::StreamKey::projection_version`,
/// `adapters::remember`/`retrieve`'s `RememberCommand::projection_version`).
///
/// §17's own frozen warning, restated because it is easy to get backwards: **`embedding_version`
/// does not stand in for `projection_version`** — a card-template change bumps
/// `projection_version` without touching `embedding_version` (§18.2), so `visible` filtering
/// (§23.1②) must key off `projection_version`, never fall back to `embedding_version`.
///
/// `data_class`/`egress_disposition` are §18.2's mandatory pair: "没有 data_class/
/// egress_disposition，SECRET_MATERIAL 在出境这一侧不可判定". A payload alone cannot prove it is
/// safe to index — see [`Self::into_indexable`], the type that does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QdrantPointPayload {
    pub tenant_id: TenantId,
    pub workspace_id: WorkspaceId,
    pub visibility_class: VisibilityClass,
    pub visibility_user_id: Option<UserId>,
    pub visibility_workspace_id: Option<WorkspaceId>,
    pub object_type: String,
    pub memory_type: MemoryType,
    pub status: AuthorityStatus,
    pub authority: AuthorityClass,
    pub created_at: OffsetDateTime,
    pub effective_at: OffsetDateTime,
    pub embedding_version: String,
    pub projection_version: String,
    /// §37 tombstone overlay scans by this field; point id may also be derived from it (§17:
    /// "point id 由它派生亦可，二选一" — this adapter does not itself pick which of the two,
    /// that is the Projection Worker's call at upsert time).
    pub source_stream_seq: i64,
    pub data_class: DataClass,
    pub egress_disposition: EgressDisposition,
}

/// §6.1.1 wire form, UPPER_SNAKE — the exact strings `private.memory_records.visibility_class`'s
/// CHECK constraint accepts (`migrations/0004_private_evidence_memory.sql`) and
/// `projection::dense::build_dense_filter`'s `Condition` tree already emits for its
/// `visibility_class`/`visibility_user_id`/`visibility_workspace_id` clauses — payload writes
/// and query filters must agree on this spelling or filtering silently returns nothing.
fn visibility_class_wire(c: VisibilityClass) -> &'static str {
    match c {
        VisibilityClass::UserPrivate => "USER_PRIVATE",
        VisibilityClass::WorkspaceShared => "WORKSPACE_SHARED",
        VisibilityClass::TenantShared => "TENANT_SHARED",
    }
}

/// §8.5 wire form, UPPER_SNAKE — the exact 12-value CHECK list on
/// `private.memory_records.memory_type` (`migrations/0004_private_evidence_memory.sql`).
fn memory_type_wire(t: MemoryType) -> &'static str {
    match t {
        MemoryType::Fact => "FACT",
        MemoryType::Preference => "PREFERENCE",
        MemoryType::Decision => "DECISION",
        MemoryType::Rejection => "REJECTION",
        MemoryType::State => "STATE",
        MemoryType::Issue => "ISSUE",
        MemoryType::Lesson => "LESSON",
        MemoryType::Constraint => "CONSTRAINT",
        MemoryType::Procedure => "PROCEDURE",
        MemoryType::Outcome => "OUTCOME",
        MemoryType::Reference => "REFERENCE",
        MemoryType::Note => "NOTE",
    }
}

/// §59.1 wire form — PascalCase, the variant name verbatim (repo convention §53.2: "label
/// 值 = PascalCase 变体名逐字"), matching `private.memory_records.authority_class`'s CHECK
/// list exactly (`migrations/0004_private_evidence_memory.sql`).
fn authority_class_wire(a: AuthorityClass) -> &'static str {
    match a {
        AuthorityClass::PublicKnowledge => "PublicKnowledge",
        AuthorityClass::PrivateKnowledge => "PrivateKnowledge",
        AuthorityClass::UserPreference => "UserPreference",
        AuthorityClass::ProjectDecision => "ProjectDecision",
        AuthorityClass::UserCorrection => "UserCorrection",
        AuthorityClass::ProjectConstraint => "ProjectConstraint",
        AuthorityClass::ExplicitTaskContext => "ExplicitTaskContext",
    }
}

/// §59.1 wire form — lowercase, matching `private.memory_records.status`'s CHECK list exactly
/// (that column's own `COMMENT`: "DB wire values lowercase to match the G59-4 CHECK quoted
/// from §59.1 verbatim").
fn authority_status_wire(s: AuthorityStatus) -> &'static str {
    match s {
        AuthorityStatus::Active => "active",
        AuthorityStatus::Superseded => "superseded",
        AuthorityStatus::Revoked => "revoked",
        AuthorityStatus::Expired => "expired",
    }
}

impl QdrantPointPayload {
    /// Encodes this payload as the flat JSON object Qdrant's `points.upsert` REST body expects
    /// under `payload`. `created_at`/`effective_at` are Unix-epoch seconds
    /// (`OffsetDateTime::unix_timestamp`, always available with no extra `time` crate feature)
    /// rather than an RFC3339 string.
    // ponytail: RFC3339 would be the more Qdrant-idiomatic datetime-range encoding, but needs
    // the `time` crate's `formatting` feature, not currently a dependency of this crate —
    // epoch-seconds needs no new feature and is still exact and filterable/sortable. Upgrade
    // alongside the HTTP client.
    pub fn to_json(&self) -> Value {
        let mut obj = serde_json::Map::with_capacity(16);
        obj.insert("tenant_id".into(), json!(self.tenant_id.0.to_string()));
        obj.insert(
            "workspace_id".into(),
            json!(self.workspace_id.0.to_string()),
        );
        obj.insert(
            "visibility_class".into(),
            json!(visibility_class_wire(self.visibility_class)),
        );
        obj.insert(
            "visibility_user_id".into(),
            match self.visibility_user_id {
                Some(u) => json!(u.0.to_string()),
                None => Value::Null,
            },
        );
        obj.insert(
            "visibility_workspace_id".into(),
            match self.visibility_workspace_id {
                Some(w) => json!(w.0.to_string()),
                None => Value::Null,
            },
        );
        obj.insert("object_type".into(), json!(self.object_type));
        obj.insert(
            "memory_type".into(),
            json!(memory_type_wire(self.memory_type)),
        );
        obj.insert("status".into(), json!(authority_status_wire(self.status)));
        obj.insert(
            "authority".into(),
            json!(authority_class_wire(self.authority)),
        );
        obj.insert("created_at".into(), json!(self.created_at.unix_timestamp()));
        obj.insert(
            "effective_at".into(),
            json!(self.effective_at.unix_timestamp()),
        );
        obj.insert("embedding_version".into(), json!(self.embedding_version));
        obj.insert("projection_version".into(), json!(self.projection_version));
        obj.insert("source_stream_seq".into(), json!(self.source_stream_seq));
        obj.insert("data_class".into(), json!(self.data_class.as_str()));
        obj.insert(
            "egress_disposition".into(),
            json!(self.egress_disposition.as_str()),
        );
        Value::Object(obj)
    }

    /// §18.2's index-write gate: `None` iff `self.data_class == DataClass::SecretMaterial` —
    /// mirrors `projection::card::build_card`'s identical check on the card-build path
    /// (`crates/projection/src/card.rs`), this module's equivalent for the Qdrant index-write
    /// path (§18.2: "不生成卡、不进 PLATFORM_RETRIEVAL 索引" names both). This is the only
    /// place that check runs — [`upsert_point_body`] takes an [`IndexablePayload`], not a bare
    /// [`QdrantPointPayload`], so a caller cannot reach the index-write body constructor
    /// without going through this gate first.
    pub fn into_indexable(self) -> Option<IndexablePayload> {
        (self.data_class != DataClass::SecretMaterial).then_some(IndexablePayload(self))
    }
}

/// A [`QdrantPointPayload`] proven not to be [`DataClass::SecretMaterial`] — see
/// [`QdrantPointPayload::into_indexable`], the sole constructor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexablePayload(QdrantPointPayload);

// ============================================================================
// Qdrant point id — Qdrant's own two accepted wire forms (unsigned int or UUID)
// ============================================================================

/// Qdrant point id, either of the two forms the Qdrant wire protocol accepts. Not a Humaux
/// domain id — this is Qdrant's own id space, distinct from any `*Id` newtype in
/// `humaux_domain::ids`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PointId {
    Num(u64),
    Uuid(Uuid),
}

impl PointId {
    fn to_json(self) -> Value {
        match self {
            Self::Num(n) => json!(n),
            Self::Uuid(u) => json!(u.to_string()),
        }
    }
}

/// One entry of a `points.upsert` request body's `points` array — id + payload, dense vector
/// omitted (embedding production is a separate concern from this adapter's payload/placement
/// responsibilities). Takes [`IndexablePayload`], not a bare [`QdrantPointPayload`] — §18.2's
/// `SECRET_MATERIAL` gate (see [`QdrantPointPayload::into_indexable`]) is therefore not
/// bypassable by calling this function directly. The caller assembles the full request
/// (`{"points": [...]}`, plus `ha_profile.write_params_json()` for the `?ordering=` query
/// param, §17.5) once an HTTP client lands — see module doc.
pub fn upsert_point_body(id: PointId, payload: &IndexablePayload) -> Value {
    json!({ "id": id.to_json(), "payload": payload.0.to_json() })
}

// ============================================================================
// §6.1.2 / §17.1 — Condition → Qdrant filter JSON
// ============================================================================

/// Translates a [`DenseQueryFilter`] (`projection::dense::build_dense_filter`'s output, the
/// only way to obtain one — its tenant+visibility injection is therefore already guaranteed,
/// §17.1) into a Qdrant REST `Filter` JSON object. Takes `&DenseQueryFilter`, not `&Condition`:
/// a bare `&Condition` parameter would let any caller hand-assemble a tree with no tenant
/// clause and get a valid-looking Qdrant filter out of it, exactly the "业务层不得手写可选
/// filter" bypass §17.1 forbids. This is the one place in the workspace that knows Qdrant's
/// `must`/`should`/`key`+`match` wire shape — `dense.rs`/`sparse.rs` deliberately do not import
/// `serde_json`/Qdrant.
pub fn condition_to_filter(filter: &DenseQueryFilter) -> Value {
    condition_tree_to_filter(filter.as_condition())
}

fn condition_tree_to_filter(condition: &Condition) -> Value {
    match condition {
        Condition::And(clauses) => {
            json!({ "must": clauses.iter().map(condition_to_wire).collect::<Vec<_>>() })
        }
        Condition::Or(clauses) => {
            json!({ "should": clauses.iter().map(condition_to_wire).collect::<Vec<_>>() })
        }
        // A bare Eq/In at the top (no caller currently produces this — `build_dense_filter`
        // always wraps in `And` — but the type permits it) is still a well-formed
        // single-condition filter.
        other => json!({ "must": [condition_to_wire(other)] }),
    }
}

/// One `Condition` node as a Qdrant `Condition` JSON value (usable inside a `must`/`should`
/// array) — `Eq`/`In` become a `FieldCondition`, `And`/`Or` become a nested `Filter` object
/// (Qdrant's filter DSL accepts a nested Filter anywhere a Condition is expected).
fn condition_to_wire(condition: &Condition) -> Value {
    match condition {
        Condition::Eq { field, value } => json!({ "key": field, "match": { "value": value } }),
        Condition::In { field, values } => json!({ "key": field, "match": { "any": values } }),
        Condition::And(clauses) => {
            json!({ "must": clauses.iter().map(condition_to_wire).collect::<Vec<_>>() })
        }
        Condition::Or(clauses) => {
            json!({ "should": clauses.iter().map(condition_to_wire).collect::<Vec<_>>() })
        }
    }
}

// ============================================================================
// §16.3/§23.1② — visible count filter (tenant+visibility+projection_version)
// ============================================================================

/// §16.3/§23.1②'s `visible` count filter — the only shape [`count_body`] accepts. §16.3's own
/// warning: counting by tenant+scope alone (no `projection_version` tag) makes criterion ①
/// compare a number to itself (恒真闸, `projection::serving`'s own G80-28 doc names this exact
/// failure mode). The sole constructor, [`Self::new`], routes through
/// `projection::dense::build_dense_filter` with `projection_version` as a mandatory `narrow_by`
/// term — §17.1's tenant/visibility injection stays intact, this only narrows further — and
/// refuses an empty version string, so a blank string cannot silently stand in for "no version
/// filter".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleCountFilter(DenseQueryFilter);

impl VisibleCountFilter {
    pub fn new(scope: &AuthorizationScope, projection_version: &str) -> Option<Self> {
        if projection_version.is_empty() {
            return None;
        }
        Some(Self(build_dense_filter(
            scope,
            &[FieldMatch {
                field: "projection_version",
                value: projection_version.to_string(),
            }],
        )))
    }
}

/// `POST /collections/{name}/points/count` request body. `exact: true` always — §23.1② needs a
/// real count, not Qdrant's approximate fast-count path.
pub fn count_body(filter: &VisibleCountFilter) -> Value {
    json!({ "filter": condition_to_filter(&filter.0), "exact": true })
}

/// §17/§37: `visible` is the raw Qdrant count minus the tombstone overlay — the missing half
/// that made `visible`/tombstone bookkeeping otherwise unimplementable in this crate (§17's own
/// text names `source_stream_seq` as existing precisely so a tombstone overlay can scan by it).
/// Pure arithmetic, not a query: `raw_count` is [`count_body`]'s response, `tombstone_count`
/// comes from wherever §37's tombstone rows are counted (Postgres, out of this Qdrant adapter's
/// scope). Saturates at 0 instead of underflowing if a stale tombstone outnumbers the raw count
/// (a tombstone recorded before the point was ever indexed).
pub fn visible_count(raw_count: u64, tombstone_count: u64) -> u64 {
    raw_count.saturating_sub(tombstone_count)
}

// ============================================================================
// §17.5 — HA Consistency Profile
// ============================================================================

/// Qdrant's own write-ordering enum (wire values, not a Humaux type).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOrdering {
    Weak,
    Medium,
    Strong,
}

impl WriteOrdering {
    fn as_str(self) -> &'static str {
        match self {
            Self::Weak => "weak",
            Self::Medium => "medium",
            Self::Strong => "strong",
        }
    }
}

/// Qdrant's own read-consistency enum (wire values, not a Humaux type). Qdrant's `consistency`
/// read parameter accepts either a replica-count integer or one of `quorum`/`majority`/`all` —
/// typed here (not a bare `u32`) so "ask more than one replica" is a variant a caller can name,
/// not an integer whose meaning has to be remembered (Qdrant's own default with no `consistency`
/// param at all is equivalent to querying exactly 1 replica, i.e. no cross-replica check).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadConsistency {
    Quorum,
    Majority,
    All,
    Factor(u32),
}

impl ReadConsistency {
    fn as_json(self) -> Value {
        match self {
            Self::Quorum => json!("quorum"),
            Self::Majority => json!("majority"),
            Self::All => json!("all"),
            Self::Factor(n) => json!(n),
        }
    }
}

/// The three §17.5 operation categories — closed set, exactly the three phrases §17.5's own
/// text names ("normal immutable projection upsert" / "correction/delete/supersede" /
/// "read-your-write strict path").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QdrantOperation {
    NormalImmutableUpsert,
    CorrectionDeleteSupersede,
    ReadYourWriteStrict,
}

/// §17.5's per-operation adapter knobs. `read_consistency` is `Option` rather than always-set:
/// §17.5's own last line is "具体值通过故障注入和压测冻结" (exact values pending fault-injection/
/// load-test results) — only `write_ordering` is a categorical decision §17.5 already makes,
/// and only `ReadYourWriteStrict` needs any cross-replica read check at all.
///
/// `write_consistency_factor` is deliberately not a field here — it is a collection-level
/// parameter (Qdrant's `PUT /collections/{name}` body, see [`create_collection_body`]), not a
/// per-request one; `write_params_json` below only ever emits per-request `ordering`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HaConsistencyProfile {
    pub write_ordering: WriteOrdering,
    pub read_consistency: Option<ReadConsistency>,
}

/// §17.5's per-operation profile. Throughput-oriented normal upserts get weak ordering;
/// correction/delete/supersede (the write class that must not silently lose to a concurrent
/// normal write) gets strong ordering; the read-your-write strict path (§17.5's third bullet:
/// "verified visibility or delta overlay") gets `Quorum` read consistency — more than the
/// single-replica default Qdrant otherwise uses — until benchmarking picks a tighter factor.
pub fn ha_profile_for(op: QdrantOperation) -> HaConsistencyProfile {
    match op {
        QdrantOperation::NormalImmutableUpsert => HaConsistencyProfile {
            write_ordering: WriteOrdering::Weak,
            read_consistency: None,
        },
        QdrantOperation::CorrectionDeleteSupersede => HaConsistencyProfile {
            write_ordering: WriteOrdering::Strong,
            read_consistency: None,
        },
        QdrantOperation::ReadYourWriteStrict => HaConsistencyProfile {
            write_ordering: WriteOrdering::Medium,
            read_consistency: Some(ReadConsistency::Quorum),
        },
    }
}

impl HaConsistencyProfile {
    /// Wire fragment for a Qdrant write request's query-string/body ordering param. Never
    /// carries `consistency` — that is a read parameter (see [`Self::read_params_json`]);
    /// putting it on a write request silently no-ops on Qdrant's side, which previously masked
    /// this exact mistake.
    pub fn write_params_json(&self) -> Value {
        json!({ "ordering": self.write_ordering.as_str() })
    }

    /// Wire fragment for a Qdrant read (search/scroll/count) request's `consistency` param.
    /// Empty object when `read_consistency` is `None` — Qdrant's own single-replica default.
    pub fn read_params_json(&self) -> Value {
        match self.read_consistency {
            Some(rc) => json!({ "consistency": rc.as_json() }),
            None => json!({}),
        }
    }
}

// ============================================================================
// §17.4 — Search-visible confirmation contract
// ============================================================================

/// Proof that every id in a batch is currently search-visible in Qdrant (§17.4). The only
/// producer is [`verify_visible`] — there is no public constructor that takes an
/// `acknowledged: bool` from a bare upsert response, because §17.4's entire point is that
/// `acknowledged` (or `wait=false`) does **not** imply search-visible. A caller that wants to
/// advance a stream checkpoint must go through this type; there is no second, cheaper path
/// that skips the confirmation (mirrors `projection::dense::DenseQueryFilter`'s own
/// single-construction-point pattern for the identical reason — a type-level lock, not a
/// convention someone has to remember).
///
/// Not a third `ErrorCode`/`DegradeCode` variant (CLAUDE.md hard rule) — same reasoning
/// `projection::sparse::SparseLane` already gives for its own non-error status descriptor:
/// "not visible yet" is neither a terminal error nor a quality degradation, it is Projection
/// Worker's normal not-ready-to-commit state, handled by retrying the upsert→verify step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibilityConfirmation {
    confirmed_ids: Vec<PointId>,
}

impl VisibilityConfirmation {
    /// The only legal construction path (crate-visible, not `pub`): [`verify_visible`] calls
    /// this after it has itself checked that every expected id came back from a real search.
    fn from_verified_search(confirmed_ids: Vec<PointId>) -> Self {
        Self { confirmed_ids }
    }

    /// Whether `id` was among the ids this confirmation actually observed as visible.
    pub fn contains(&self, id: &PointId) -> bool {
        self.confirmed_ids.contains(id)
    }
}

/// §17.4's contract, implemented as a pure decision function over a caller-supplied
/// visibility check: `upsert -> verify_visible -> commit checkpoint`, never
/// `upsert -> commit checkpoint`.
///
/// `check_visible` stands in for "ask Qdrant which of these ids a real search call returns
/// right now" — in production that is an HTTP search/scroll request (`adapters::qdrant`'s HTTP
/// layer, not implemented in this build, see module doc); in tests it is a plain closure. The
/// §23.4 injection-2 fault case is `tests/qdrant_contract.rs`'s
/// `verify_visible_withholds_confirmation_when_some_ids_are_not_yet_visible`: deleting this
/// function's `all_confirmed` check turns that test from green to red — see its doc for why
/// that, not a separately-named "fault" test with an identical closure, is the record.
///
/// Returns `None` (not an error) when confirmation is incomplete — §17.4: "adapter 必须通过
/// operation/update-queue/visibility verification 实现等价确认，而不是提前推进 checkpoint".
/// A `None` means "retry the verify step later", not "this upsert failed".
pub fn verify_visible(
    point_ids: &[PointId],
    check_visible: impl Fn(&[PointId]) -> Vec<PointId>,
) -> Option<VisibilityConfirmation> {
    let observed = check_visible(point_ids);
    let observed_set: std::collections::HashSet<PointId> = observed.iter().copied().collect();
    let all_confirmed = point_ids.iter().all(|id| observed_set.contains(id));
    // Confirmation carries exactly the batch that was requested and fully round-tripped, never
    // `observed` verbatim — `check_visible` may legitimately return a superset (e.g. a scroll
    // page touching ids outside this batch); advancing a checkpoint on ids nobody asked to
    // verify would defeat §17.4 just as surely as skipping the check entirely.
    all_confirmed.then(|| VisibilityConfirmation::from_verified_search(point_ids.to_vec()))
}

/// §17.4's gate for the Projection Worker's checkpoint-advance step: only proceed once
/// `confirmation` actually contains every id in `expected` (defense in depth alongside
/// [`verify_visible`] already refusing to construct a confirmation for a partial batch —
/// this is the check a caller holding an already-built `VisibilityConfirmation` re-runs
/// against a *different* expected set, e.g. a retried subset).
pub fn may_advance_checkpoint(confirmation: &VisibilityConfirmation, expected: &[PointId]) -> bool {
    expected.iter().all(|id| confirmation.contains(id))
}

// ============================================================================
// §17.3 — Tiered Multitenancy placement row (mirrors `projection.tenant_placements`,
// migrations 0068+)
// ============================================================================

/// Closed set for `projection.tenant_placements.placement_class` (migration 0068's CHECK
/// constraint) — §17.3: "small tenants -> shared fallback shard; large tenants -> dedicated
/// shard".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementClass {
    SharedFallback,
    Dedicated,
}

impl PlacementClass {
    /// `SHARED_FALLBACK` / `DEDICATED` — migration 0068's CHECK constraint values, verbatim.
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::SharedFallback => "SHARED_FALLBACK",
            Self::Dedicated => "DEDICATED",
        }
    }

    /// All variants, in the exact order/spelling migration 0068's CHECK constraint lists —
    /// `tests/qdrant_contract.rs`'s `placement_class_matches_migration_0068_check` (a real-DB
    /// contract test, §78.2) asserts this against `pg_get_constraintdef` so the two can never
    /// drift silently.
    pub const ALL: [PlacementClass; 2] = [Self::SharedFallback, Self::Dedicated];
}

/// Closed set for `projection.tenant_placements.promotion_state` (migration 0068's CHECK
/// constraint). Not spec-frozen anywhere in §17 (§17.3 lists the *column*, not its value set)
/// — this is a first-pass workflow closed set (stable / a promotion is in flight / already
/// promoted), narrower sets that later prove wrong are a plain `ALTER` away since no
/// production row exists yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotionState {
    Stable,
    PromotionPending,
    Promoted,
}

impl PromotionState {
    /// `STABLE` / `PROMOTION_PENDING` / `PROMOTED` — migration 0068's CHECK constraint values,
    /// verbatim.
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Stable => "STABLE",
            Self::PromotionPending => "PROMOTION_PENDING",
            Self::Promoted => "PROMOTED",
        }
    }

    /// All variants, in the exact order/spelling migration 0068's CHECK constraint lists (see
    /// [`PlacementClass::ALL`] for the same pattern on the sibling column).
    pub const ALL: [PromotionState; 3] = [Self::Stable, Self::PromotionPending, Self::Promoted];
}

/// Rust mirror of one `projection.tenant_placements` row (§17.3 field list, migration 0068).
/// A pure data-carrier type — no SQL lives on it. §17.3's control-plane-only requirement
/// ("placement 变更只经控制面（业务代码无散落逻辑）") is a statement about *who is allowed to
/// write this table*, not about this struct: the table's own GRANT matrix already enforces
/// the mechanical half of it (§6.2.1 domain default — this table has no
/// `projection.tenant_placements` row in §6.2.2's per-table override matrix, so it falls
/// through to the domain default: only `role_gateway` (read-only `R`) and
/// `role_retrieval_worker` (`R+W`) can touch it at all). The Rust-side half — no second module
/// in this workspace constructing an `UPDATE projection.tenant_placements` statement — is this
/// module's job once the write path lands (blocked on the same missing HTTP client the module
/// doc explains, since a placement write needs a live Qdrant `point_count`/collection read to
/// be honest, not a value invented at insert time).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantPlacementRow {
    pub tenant_id: TenantId,
    pub projection_family: RetrievalFamily,
    pub collection_name: String,
    pub shard_key: Option<String>,
    pub placement_class: PlacementClass,
    pub point_count: i64,
    pub bytes_estimate: i64,
    pub promotion_state: PromotionState,
}
