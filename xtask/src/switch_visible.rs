//! `xtask::switch_visible` — §16.2 serve switch: live visible_* counts taken from Qdrant/Postgres.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-infra-cell, humaux-projection, postgres, tokio, uuid]; services=[PostgreSQL(any) r=[private.memory_records, projection.private_memory_points, projection.stream_checkpoints, projection.stream_log, projection.tenant_placements]]; env=[]; modules=[adapters::qdrant, adapters::retrieve, domain::identity, domain::ids, infra-cell::permit, infra-cell::resource, infra-cell::transport, projection::serving]
//! Called-by: [xtask::projection_serve, xtask::soak]
//! Invariants: [visible_serving/visible_shadow are real Qdrant/Postgres counts, never operator-supplied numbers]
//! Spec: Baseline §16.2; §16.3; §23.1②
//!
//! §16.2 serve switch — the two `visible_*` counts, taken live from Qdrant.
//!
//! Both ops-side callers of `projection::serving::evaluate_switch`
//! (`xtask::projection_serve` → `adapters::serving_repo::switch_projection_version`, and
//! `xtask::soak`'s promotion report) used to hand the evaluator a number nobody had measured:
//! `projection-serve` took `--visible-shadow <n>` from the operator's command line (the
//! rehearsal filled it with `count(*) FROM projection.private_memory_points`, a PostgreSQL row
//! count, not §23.1②'s Qdrant index count) and passed `visible_serving = None`; the soak's
//! `switch_rejections` passed `None` for **both** sides. `None` on either side is
//! `SwitchRejection::VisibleUnavailable`, so every candidate in every soak witness was refused
//! for a reason that described the harness, not the deployment — card 18 wired the live count
//! into the three read routes and the tally did not move by one.
//!
//! This module is the serve-side half of that wiring, and it holds **no filter of its own**:
//! the count is `adapters::retrieve::visible_count_of_version`, the same producer
//! `visible_index_count` (the read routes) delegates to, so the §17.1 tenant clause + §6.1.2
//! visibility disjunction (`projection::dense::build_dense_filter`, the sole `Condition`
//! constructor — `crates/projection/tests/no_handwritten_filter_scan.rs`) and the §37 tombstone
//! overlay are injected identically on the read side and the switch side.
//!
//! **Which version each side counts** (ADR-0040): `visible_shadow` counts the **candidate**
//! version being promoted, `visible_serving` counts the family's current `serving` version.
//! `visible_index_count`'s `serving_version != key.projection_version ⇒ None` guard is
//! deliberately *not* reproduced here — that guard protects an A2 comparison against a
//! `LedgerClosure` closed at one version, and §16.3's criterion ① compares one count against
//! another count whose whole point is that the two versions differ. Reproducing it would return
//! `None` for every genuine (candidate ≠ serving) promotion, i.e. `VisibleUnavailable` forever —
//! the bug this module exists to remove.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use humaux_adapters::qdrant::RetrievalFamily;
use humaux_adapters::retrieve::{IndexFace, visible_count_of_version};
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{TenantId, WorkspaceId};
use humaux_infra_cell::{
    CallerId, CellId, DEFAULT_MAX_RESPONSE_BYTES, HttpIntraCellTransport, IntraCellResource,
    IntraCellResourceRegistry, ResourceEntry, authorize_cell_access,
};
use humaux_projection::serving::StreamFamily;
use postgres::Client;
use uuid::Uuid;

/// §17.3: the tenant's own collection, never a constant and never another tenant's — the same
/// row `adapters::placement_repo::tenant_placement` reads on the request path. `$1` = tenant.
pub const COLLECTION_SQL: &str = "SELECT collection_name FROM projection.tenant_placements \
     WHERE tenant_id = $1 AND projection_family = $2";

/// §37's tombstone overlay input for one full stream key — the same rows
/// `adapters::retrieve::tombstoned_source_seqs` reads for the read routes.
/// `$1..$6` = the six-column stream key.
pub const TOMBSTONED_SEQS_SQL: &str = "SELECT stream_seq FROM projection.stream_log \
     WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
       AND projection_kind = $5 AND projection_version = $6 AND state = 'TOMBSTONED' \
     ORDER BY stream_seq";

/// The blind-spot probe, see [`Candidate::user_private_points`]. Deliberately **not** narrowed
/// to one `projection_version`: a user-private point on either side of the switch is enough to
/// make the pair an undercount, and "any live user-private point in this tenant" is the cheap
/// conservative form (over-refusal, never a false pass). No bind parameters — RLS scopes it.
pub const USER_PRIVATE_PROBE_SQL: &str = "SELECT EXISTS ( \
       SELECT 1 FROM projection.private_memory_points p \
       JOIN private.memory_records m \
         ON m.tenant_id = p.tenant_id AND m.memory_id = p.memory_id \
       WHERE p.projection_live AND m.visibility_class = 'USER_PRIVATE')";

/// §16.2's family-scoped `serving` row, for the ops callers that must know **which** version to
/// count on the `visible_serving` side before the switch transaction opens.
/// `adapters::serving_repo`'s own entry point cannot be reused here: it requires an
/// authenticated end user (`MissingAuthenticatedUser`) and a `RuntimeDbPool`, and an ops switch
/// has neither. `ux_serving_one` guarantees at most one such row. `$1..$5` = the family.
pub const SERVING_ROW_VERSION_SQL: &str = "SELECT projection_version \
     FROM projection.stream_checkpoints \
     WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
       AND projection_kind = $5 AND serving";

/// The `projection_family` value [`COLLECTION_SQL`] binds — private memory is the only family
/// this deployment projects.
pub const PRIVATE_MEMORY_FAMILY: RetrievalFamily = RetrievalFamily::PrivateMemoryV1;

/// One live Qdrant face for this ops process: the one-entry `IntraCellResource::QDRANT_REST`
/// registry plus its transport (same shape as `xtask::e2e_seed::qdrant_registry`, ADR-0003 —
/// Qdrant is same-Cell, not egress). Built once and reused for every count so the two sides of
/// §16.3's criterion ① go over one connection pool rather than two.
pub struct VisibleFace {
    registry: IntraCellResourceRegistry,
    transport: HttpIntraCellTransport,
}

impl VisibleFace {
    /// `host` is restricted to `127.0.0.1` — the same binding rule `xtask::e2e_seed` applies:
    /// no ops tool in this workspace dials a non-local Qdrant.
    pub fn connect(host: &str, port: u16) -> Result<Self, String> {
        if host != "127.0.0.1" {
            return Err(format!(
                "refusing: --qdrant-host {host:?} must be 127.0.0.1 (never production)"
            ));
        }
        let cell = CellId(Uuid::new_v4());
        let caller = CallerId("xtask-switch-visible".to_string());
        let cidr = format!("{host}/32")
            .parse()
            .map_err(|e| format!("qdrant cidr {host}/32: {e:?}"))?;
        let mut entries = BTreeMap::new();
        entries.insert(
            IntraCellResource::QDRANT_REST,
            ResourceEntry::new(
                host,
                port,
                cell,
                vec![cidr],
                BTreeSet::from([caller.clone()]),
                false,
            )
            .map_err(|e| format!("qdrant resource entry: {e}"))?,
        );
        let registry = IntraCellResourceRegistry::new(entries, cell, caller);
        let transport = HttpIntraCellTransport::new(
            registry.clone(),
            Duration::from_secs(10),
            DEFAULT_MAX_RESPONSE_BYTES,
        )
        .map_err(|e| format!("qdrant transport: {e}"))?;
        Ok(Self {
            registry,
            transport,
        })
    }

    /// One §23.1② count. `None` on an unreachable/erroring Qdrant, which stays
    /// `VisibleUnavailable` — the honest refusal, never a `0`.
    async fn count(
        &self,
        scope: &AuthorizationScope,
        collection: &str,
        version: &str,
        tombstoned_seqs: &[i64],
    ) -> Option<u64> {
        let permit = authorize_cell_access(
            &self.registry,
            IntraCellResource::QDRANT_REST,
            Duration::from_secs(60),
        )
        .ok()?;
        visible_count_of_version(
            &IndexFace {
                transport: &self.transport,
                permit: &permit,
                collection,
            },
            scope,
            version,
            tombstoned_seqs,
        )
        .await
    }
}

/// The ops-side [`AuthorizationScope`] both counts run under: the candidate's own tenant, a
/// fresh service principal, **no** on-behalf-of user, and the family's single workspace.
///
/// `user_id = None` is the honest shape for a switch — there is no end user acting — and it is
/// also this scope's one ceiling: `projection::dense`'s §6.1.2 disjunction then carries no
/// `USER_PRIVATE` arm, so points of that class are counted on **neither** side. Equal-by-absence
/// is exactly the 恒真闸 §16.3 warns about, so [`Candidate::user_private_points`] refuses the
/// count outright rather than letting the pair agree vacuously.
pub fn ops_scope(tenant_id: Uuid, workspace_id: Uuid) -> Result<AuthorizationScope, String> {
    Ok(AuthorizationScope::new(
        TenantId(tenant_id),
        PrincipalId::new(),
        None,
        BoundedSet::new([WorkspaceId(workspace_id)])
            .map_err(|e| format!("ops scope workspace set: {e:?}"))?,
    ))
}

/// Everything about one promotion candidate that only PostgreSQL can answer, read by the
/// caller with its own handle ([`COLLECTION_SQL`], [`TOMBSTONED_SEQS_SQL`],
/// [`USER_PRIVATE_PROBE_SQL`]) — this module owns the Qdrant half only.
pub struct Candidate<'a> {
    pub scope: &'a AuthorizationScope,
    pub collection: &'a str,
    /// The version being promoted — §16.3's `visible(shadow)` side.
    pub candidate_version: &'a str,
    pub candidate_tombstoned: &'a [i64],
    /// The family's current `serving` version, or `None` for an ADR-0017 first activation
    /// (nothing to compare against; `evaluate_switch` accepts a lone shadow read-back there).
    pub serving_version: Option<&'a str>,
    pub serving_tombstoned: &'a [i64],
    /// `true` when this tenant has at least one live projected point whose memory is
    /// `USER_PRIVATE` — see [`ops_scope`]. Both counts then return `None`
    /// (`VisibleUnavailable`, a refusal) instead of a pair that silently omits those points.
    pub user_private_points: bool,
}

/// §16.3 criterion ①'s `(visible_shadow, visible_serving)` pair, each side tagged with the
/// `projection_version` it was counted against — the exact shape
/// `projection::serving::SwitchCriteria` takes.
pub type VisiblePair = (Option<(String, u64)>, Option<(String, u64)>);

/// §16.3 criterion ①'s two inputs, tagged with the version each was counted against —
/// the exact `(visible_shadow, visible_serving)` pair
/// `adapters::serving_repo::switch_projection_version` and `xtask::soak`'s report both need.
pub async fn visible_pair(face: &VisibleFace, c: &Candidate<'_>) -> VisiblePair {
    if c.user_private_points {
        return (None, None);
    }
    let shadow = face
        .count(
            c.scope,
            c.collection,
            c.candidate_version,
            c.candidate_tombstoned,
        )
        .await
        .map(|n| (c.candidate_version.to_string(), n));
    let serving = match c.serving_version {
        Some(v) => face
            .count(c.scope, c.collection, v, c.serving_tombstoned)
            .await
            .map(|n| (v.to_string(), n)),
        None => None,
    };
    (shadow, serving)
}

/// `--qdrant-host` / `--qdrant-port` for the two ops subcommands. Same two defaults
/// `xtask::e2e_seed` already uses for the same local Qdrant; every other input of the switch
/// stays §78.1 required-no-default.
pub fn qdrant_endpoint(args: &[String]) -> Result<(String, u16), String> {
    let host = args
        .iter()
        .position(|a| a == "--qdrant-host")
        .and_then(|i| args.get(i + 1).cloned())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let port: u16 = match args
        .iter()
        .position(|a| a == "--qdrant-port")
        .and_then(|i| args.get(i + 1).cloned())
    {
        Some(v) => v.parse().map_err(|e| format!("--qdrant-port: {e}"))?,
        None => 6333,
    };
    Ok((host, port))
}

/// The PostgreSQL half of one candidate's [`Candidate`] inputs. Read here, once, so the two ops
/// callers do not grow two copies of the same three queries — both already hold a
/// `role_maintenance` blocking client, and both must have installed `humaux.tenant_id` (RLS)
/// before calling.
pub struct CandidateFacts {
    pub collection: String,
    pub candidate_tombstoned: Vec<i64>,
    pub serving_tombstoned: Vec<i64>,
    pub user_private_points: bool,
}

/// `Ok(None)` — not an error — when this tenant has no §17.3 placement row for the private
/// memory family: there is no collection to count, so the switch's `visible_*` stay `None`
/// (`VisibleUnavailable`, the honest refusal) rather than borrowing another tenant's.
pub fn read_candidate_facts(
    db: &mut Client,
    family: &StreamFamily,
    candidate_version: &str,
    serving_version: Option<&str>,
) -> Result<Option<CandidateFacts>, String> {
    let projection_family = PRIVATE_MEMORY_FAMILY.as_db_str();
    let row = db
        .query_opt(COLLECTION_SQL, &[&family.tenant_id.0, &projection_family])
        .map_err(|e| format!("tenant placement: {e}"))?;
    let Some(row) = row else { return Ok(None) };
    let candidate_tombstoned = tombstoned_seqs(db, family, candidate_version)?;
    let serving_tombstoned = match serving_version {
        Some(v) => tombstoned_seqs(db, family, v)?,
        None => Vec::new(),
    };
    Ok(Some(CandidateFacts {
        collection: row
            .try_get::<_, String>("collection_name")
            .map_err(|e| format!("collection_name: {e}"))?,
        candidate_tombstoned,
        serving_tombstoned,
        user_private_points: db
            .query_one(USER_PRIVATE_PROBE_SQL, &[])
            .map_err(|e| format!("user-private probe: {e}"))?
            .get::<_, bool>(0),
    }))
}

fn tombstoned_seqs(
    db: &mut Client,
    family: &StreamFamily,
    version: &str,
) -> Result<Vec<i64>, String> {
    db.query(
        TOMBSTONED_SEQS_SQL,
        &[
            &family.tenant_id.0,
            &family.scope_kind,
            &family.scope_id,
            &family.domain,
            &family.projection_kind,
            &version,
        ],
    )
    .map_err(|e| format!("tombstoned seqs: {e}"))
    .map(|rows| rows.iter().map(|r| r.get::<_, i64>(0)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_non_loopback_qdrant_host_is_refused() {
        assert!(VisibleFace::connect("10.0.0.4", 6333).is_err());
        assert!(VisibleFace::connect("127.0.0.1", 6333).is_ok());
    }

    /// The blind-spot refusal: a tenant carrying live `USER_PRIVATE` points gets `(None, None)`
    /// — `VisibleUnavailable` — instead of a pair counted under a scope that cannot see them.
    /// Without this the two sides would agree at whatever the visible subset happens to be, and
    /// a shadow face missing every user-private point would promote.
    #[test]
    fn user_private_points_refuse_the_count_instead_of_agreeing_vacuously() {
        let scope = ops_scope(Uuid::now_v7(), Uuid::now_v7()).expect("scope");
        let face = VisibleFace::connect("127.0.0.1", 6333).expect("face");
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let pair = rt.block_on(visible_pair(
            &face,
            &Candidate {
                scope: &scope,
                collection: "humaux_private_memory_v1_e2e",
                candidate_version: "v2",
                candidate_tombstoned: &[],
                serving_version: Some("v1"),
                serving_tombstoned: &[],
                user_private_points: true,
            },
        ));
        assert_eq!(pair, (None, None));
    }

    #[test]
    fn qdrant_endpoint_defaults_to_the_local_qdrant() {
        assert_eq!(
            qdrant_endpoint(&[]).expect("defaults"),
            ("127.0.0.1".to_string(), 6333)
        );
        let args = vec![
            "--qdrant-port".to_string(),
            "7333".to_string(),
            "--qdrant-host".to_string(),
            "127.0.0.1".to_string(),
        ];
        assert_eq!(
            qdrant_endpoint(&args).expect("flags"),
            ("127.0.0.1".to_string(), 7333)
        );
    }
}
