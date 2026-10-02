//! `xtask::switch_visible` — §16.2 serve switch: live visible_* counts taken from Qdrant/Postgres.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-infra-cell, humaux-projection, humaux-testkit, postgres, serde_json, tokio, uuid]; services=[PostgreSQL(any) r=[projection.stream_checkpoints, projection.stream_log, projection.tenant_placements], PostgreSQL(owner) w=[control.private_reasoning_domains, control.tenants, control.users, control.workspaces, private.events, private.evidence_objects, private.memory_evidence, private.memory_records, projection.private_memory_points, projection.stream_checkpoints, projection.tenant_placements], PostgreSQL(role_maintenance), Qdrant(*)]; env=[HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_TEST_PG_DSN, HUMAUX_TEST_QDRANT_URL]; modules=[adapters::qdrant, adapters::retrieve, domain::ids, infra-cell::permit, infra-cell::resource, infra-cell::transport, projection::serving]
//! Called-by: [xtask::projection_serve, xtask::soak]
//! Invariants: [visible_serving/visible_shadow are real Qdrant/Postgres counts, never operator-supplied numbers;
//!   both count every point of the stream whatever its visibility class (ADR-0057 D-C)]
//! Spec: Baseline §16.2; §16.3; §23.1②; ADR-0057
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
//! the count is `adapters::retrieve::stream_count_of_version` (ADR-0057 D-C), the ops producer
//! over `projection::dense::build_stream_count_filter` — tenant + workspace + version, every
//! visibility class, so the switch sees every user's private points (ADR-0040 D-J's
//! USER_PRIVATE refusal is superseded) — with the same §37 tombstone overlay the read routes'
//! caller-scoped count uses. It returns a number, never ids or bodies (§17.1's count-only
//! exception).
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
use humaux_adapters::retrieve::{IndexFace, stream_count_of_version};
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
        c: &Candidate<'_>,
        version: &str,
        tombstoned_seqs: &[i64],
    ) -> Option<u64> {
        let permit = authorize_cell_access(
            &self.registry,
            IntraCellResource::QDRANT_REST,
            Duration::from_secs(60),
        )
        .ok()?;
        stream_count_of_version(
            &IndexFace {
                transport: &self.transport,
                permit: &permit,
                collection: c.collection,
            },
            c.tenant,
            c.workspace,
            version,
            tombstoned_seqs,
        )
        .await
    }
}

/// Everything about one promotion candidate that only PostgreSQL can answer, read by the
/// caller with its own handle ([`COLLECTION_SQL`], [`TOMBSTONED_SEQS_SQL`]) — this module owns
/// the Qdrant half only.
pub struct Candidate<'a> {
    /// The candidate family's tenant.
    pub tenant: TenantId,
    /// The candidate family's workspace (`scope_id`) — the worker stamps it on every point.
    pub workspace: WorkspaceId,
    /// The tenant's §17.3 collection.
    pub collection: &'a str,
    /// The version being promoted — §16.3's `visible(shadow)` side.
    pub candidate_version: &'a str,
    pub candidate_tombstoned: &'a [i64],
    /// The family's current `serving` version, or `None` for an ADR-0017 first activation
    /// (nothing to compare against; `evaluate_switch` accepts a lone shadow read-back there).
    pub serving_version: Option<&'a str>,
    pub serving_tombstoned: &'a [i64],
}

/// §16.3 criterion ①'s `(visible_shadow, visible_serving)` pair, each side tagged with the
/// `projection_version` it was counted against — the exact shape
/// `projection::serving::SwitchCriteria` takes.
pub type VisiblePair = (Option<(String, u64)>, Option<(String, u64)>);

/// §16.3 criterion ①'s two inputs, tagged with the version each was counted against —
/// the exact `(visible_shadow, visible_serving)` pair
/// `adapters::serving_repo::switch_projection_version` and `xtask::soak`'s report both need.
pub async fn visible_pair(face: &VisibleFace, c: &Candidate<'_>) -> VisiblePair {
    let shadow = face
        .count(c, c.candidate_version, c.candidate_tombstoned)
        .await
        .map(|n| (c.candidate_version.to_string(), n));
    let serving = match c.serving_version {
        Some(v) => face
            .count(c, v, c.serving_tombstoned)
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
    use humaux_adapters::qdrant::{
        Distance, ShardingMethod, create_collection_body, tenant_index_body,
    };
    use humaux_infra_cell::{IntraCellHttpTransport, IntraCellMethod, IntraCellRequest};
    use humaux_testkit::{ExternalDep, skip_or_fail};
    use postgres::NoTls;

    /// One throwaway tenant whose `v1` workspace stream holds two live points — a TENANT_SHARED
    /// memory and ANOTHER user's USER_PRIVATE memory — in PostgreSQL (memory rows + live
    /// registry rows, the facts the deleted ADR-0040 D-J probe read) and in a fresh Qdrant
    /// collection (payloads shaped like the worker's), with a placement row and an unserved
    /// checkpoint: an ADR-0017 first activation. Rows and collection are removed on Drop.
    struct UserPrivateStream {
        db: Client,
        face: VisibleFace,
        tenant: Uuid,
        workspace: Uuid,
        users: [Uuid; 2],
        collection: String,
    }

    impl UserPrivateStream {
        fn seed(mut db: Client, face: VisibleFace) -> Self {
            let one = |db: &mut Client,
                       sql: &str,
                       params: &[&(dyn postgres::types::ToSql + Sync)]|
             -> Uuid { db.query_one(sql, params).expect(sql).get(0) };
            let tenant = one(
                &mut db,
                "INSERT INTO control.tenants (name) VALUES ('switch_visible.rs throwaway') \
                 RETURNING tenant_id",
                &[],
            );
            let collection = format!("test_switch_up_{}", Uuid::new_v4().simple());
            let mut seeded = Self {
                db,
                face,
                tenant,
                workspace: Uuid::nil(),
                users: [Uuid::nil(); 2],
                collection,
            };
            let db = &mut seeded.db;
            let workspace = one(
                db,
                "INSERT INTO control.workspaces (tenant_id, name) VALUES ($1, 'switch') \
                 RETURNING workspace_id",
                &[&tenant],
            );
            let users = [0, 1].map(|_| {
                one(
                    db,
                    "INSERT INTO control.users (state) VALUES ('ACTIVE') RETURNING user_id",
                    &[],
                )
            });
            seeded.workspace = workspace;
            seeded.users = users;
            let domain = one(
                db,
                "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
                 VALUES ($1, 'switch') RETURNING reasoning_domain_id",
                &[&tenant],
            );
            let evidence = one(
                db,
                "INSERT INTO private.evidence_objects (tenant_id, evidence_kind, payload_sha256, \
                   data_class, origin_class, visibility_class, reasoning_domain_id) \
                 VALUES ($1, 'EVENT', $2, 'PRIVATE', 'DirectUserInput', 'TENANT_SHARED', $3) \
                 RETURNING evidence_id",
                &[&tenant, &vec![0u8; 32], &domain],
            );
            db.execute(
                "INSERT INTO private.events (event_id, event_kind, payload) \
                 VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
                &[&evidence],
            )
            .expect("event");
            db.execute(
                "INSERT INTO projection.tenant_placements \
                   (tenant_id, projection_family, collection_name) VALUES ($1, $2, $3)",
                &[
                    &tenant,
                    &PRIVATE_MEMORY_FAMILY.as_db_str(),
                    &seeded.collection,
                ],
            )
            .expect("placement");
            db.execute(
                "INSERT INTO projection.stream_checkpoints (tenant_id, scope_kind, scope_id, \
                   domain, projection_kind, projection_version) \
                 VALUES ($1, 'workspace', $2, 'private_memory', 'PRIVATE_MEMORY', 'v1')",
                &[&tenant, &workspace],
            )
            .expect("checkpoint");
            let points: Vec<serde_json::Value> =
                [("TENANT_SHARED", None), ("USER_PRIVATE", Some(users[1]))]
                    .into_iter()
                    .map(|(class, user)| seed_point(db, tenant, workspace, evidence, class, user))
                    .collect();
            let c = &seeded.collection;
            seeded.qdrant(
                IntraCellMethod::Put,
                format!("/collections/{c}"),
                create_collection_body(4, Distance::Cosine, 1, 1, 1, ShardingMethod::Auto),
            );
            seeded.qdrant(
                IntraCellMethod::Put,
                format!("/collections/{c}/index"),
                tenant_index_body(),
            );
            seeded.qdrant(
                IntraCellMethod::Put,
                format!("/collections/{c}/points?wait=true"),
                serde_json::json!({ "points": points }),
            );
            seeded
        }

        fn qdrant(&self, method: IntraCellMethod, path: String, body: serde_json::Value) {
            let permit = authorize_cell_access(
                &self.face.registry,
                IntraCellResource::QDRANT_REST,
                Duration::from_secs(60),
            )
            .expect("Qdrant permit");
            let rt = tokio::runtime::Runtime::new().expect("runtime");
            // dep: Qdrant(*) — fixture collection and points for the switch test
            rt.block_on(self.face.transport.execute(
                &permit,
                IntraCellRequest {
                    method,
                    path: path.clone(),
                    json_body: Some(body),
                    headers: Vec::new(),
                },
            ))
            .unwrap_or_else(|e| panic!("Qdrant {path}: {e:?}"));
        }
    }

    /// One memory of `evidence` with its live registry row, and the Qdrant point the worker
    /// would have written for it.
    fn seed_point(
        db: &mut Client,
        tenant: Uuid,
        workspace: Uuid,
        evidence: Uuid,
        class: &str,
        user: Option<Uuid>,
    ) -> serde_json::Value {
        // §8.6: the memory and its evidence link commit together (deferred orphan check).
        let mut txn = db.transaction().expect("memory txn");
        let memory: Uuid = txn
            .query_one(
                "INSERT INTO private.memory_records (tenant_id, memory_type, content, \
                   visibility_class, visibility_user_id, authority_class, confidence, \
                   status, asserted_at) \
                 VALUES ($1, 'NOTE', '{}'::jsonb, $2, $3, 'PrivateKnowledge', 0.9, \
                         'active', now()) RETURNING memory_id",
                &[&tenant, &class, &user],
            )
            .expect("memory")
            .get(0);
        txn.execute(
            "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
             VALUES ($1, $2, 'PRIMARY', 0)",
            &[&memory, &evidence],
        )
        .expect("memory evidence");
        txn.commit().expect("memory commit");
        let point = Uuid::new_v4();
        db.execute(
            "INSERT INTO projection.private_memory_points (point_id, tenant_id, \
               scope_kind, scope_id, domain, projection_kind, projection_version, \
               embedding_version, memory_id, source_updated_at, body_sha256) \
             VALUES ($1, $2, 'workspace', $3, 'private_memory', 'PRIVATE_MEMORY', 'v1', \
                     'embed-v1', $4, now(), $5)",
            &[&point, &tenant, &workspace, &memory, &vec![1u8; 32]],
        )
        .expect("registry row");
        serde_json::json!({
            "id": point.to_string(),
            "vector": [0.1, 0.2, 0.3, 0.4],
            "payload": {
                "tenant_id": tenant.to_string(),
                "workspace_id": workspace.to_string(),
                "projection_version": "v1",
                "visibility_class": class,
                "visibility_user_id": user.map(|u| u.to_string()),
            },
        })
    }

    impl Drop for UserPrivateStream {
        fn drop(&mut self) {
            let c = self.collection.clone();
            let permit = authorize_cell_access(
                &self.face.registry,
                IntraCellResource::QDRANT_REST,
                Duration::from_secs(60),
            );
            if let (Ok(permit), Ok(rt)) = (permit, tokio::runtime::Runtime::new()) {
                // dep: Qdrant(*) — drops the fixture collection
                let _ = rt.block_on(self.face.transport.execute(
                    &permit,
                    IntraCellRequest {
                        method: IntraCellMethod::Delete,
                        path: format!("/collections/{c}"),
                        json_body: None,
                        headers: Vec::new(),
                    },
                ));
            }
            let t = self.tenant;
            // Rows first in one batch (failure printed); the tenant row separately, best effort.
            if let Err(e) = self.db.batch_execute(&format!(
                "DELETE FROM projection.private_memory_points WHERE tenant_id = '{t}'; \
                 DELETE FROM projection.stream_checkpoints WHERE tenant_id = '{t}'; \
                 DELETE FROM projection.tenant_placements WHERE tenant_id = '{t}'; \
                 DELETE FROM private.memory_evidence USING private.memory_records m \
                   WHERE memory_evidence.memory_id = m.memory_id AND m.tenant_id = '{t}'; \
                 DELETE FROM private.memory_records WHERE tenant_id = '{t}'; \
                 DELETE FROM private.events USING private.evidence_objects eo \
                   WHERE events.event_id = eo.evidence_id AND eo.tenant_id = '{t}'; \
                 DELETE FROM private.evidence_objects WHERE tenant_id = '{t}'; \
                 DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{t}'; \
                 DELETE FROM control.workspaces WHERE tenant_id = '{t}'; \
                 DELETE FROM control.users WHERE user_id IN ('{}', '{}');",
                self.users[0], self.users[1]
            )) {
                eprintln!("switch_visible fixture cleanup failed for tenant {t}: {e}");
            }
            let _ = self.db.batch_execute(&format!(
                "DELETE FROM control.tenants WHERE tenant_id = '{t}';"
            ));
        }
    }

    fn qdrant_port() -> u16 {
        std::env::var("HUMAUX_TEST_QDRANT_URL")
            .ok()
            .and_then(|url| url.rsplit(':').next().and_then(|p| p.parse().ok()))
            .unwrap_or(6333)
    }

    /// test 26 on the production wiring (ADR-0057 D-C; review P1): the real
    /// `cargo xtask projection-serve` entry point — `visible_inputs` → [`read_candidate_facts`]
    /// → [`visible_pair`] → `serving_repo::switch_projection_version` — promotes a first
    /// activation whose stream holds another user's live USER_PRIVATE point, and the pair it
    /// hands the switch counts both points. Fault: re-add a USER_PRIVATE refusal anywhere on that
    /// path (a registry probe in `read_candidate_facts`, a short-circuit in `visible_pair`, or a
    /// `user_id = None` count) ⇒ `(None, None)` / a count of 1 ⇒ red.
    #[test]
    fn projection_serve_promotes_a_stream_holding_user_private_memories() {
        let name = "projection_serve_promotes_a_stream_holding_user_private_memories";
        let (Ok(owner_dsn), Ok(maintenance_dsn)) = (
            std::env::var("HUMAUX_TEST_PG_DSN"),
            std::env::var("HUMAUX_MAINTENANCE_PG_DSN"),
        ) else {
            skip_or_fail(
                name,
                "missing object: HUMAUX_TEST_PG_DSN / HUMAUX_MAINTENANCE_PG_DSN",
                ExternalDep::Postgres,
            );
            return;
        };
        // dep: PostgreSQL(owner) — seeds the throwaway tenant
        let Ok(owner) = Client::connect(&owner_dsn, NoTls) else {
            skip_or_fail(name, "missing object: live Postgres", ExternalDep::Postgres);
            return;
        };
        let port = qdrant_port();
        let face = VisibleFace::connect("127.0.0.1", port).expect("Qdrant face");
        let s = UserPrivateStream::seed(owner, face);
        let family = StreamFamily::new(
            TenantId(s.tenant),
            "workspace",
            s.workspace,
            "private_memory",
            "PRIVATE_MEMORY",
        );

        // dep: PostgreSQL(role_maintenance) — the candidate facts the ops callers read
        let mut maintenance = Client::connect(&maintenance_dsn, NoTls).expect("role_maintenance");
        maintenance
            .batch_execute(&format!("SET humaux.tenant_id = '{}'", s.tenant))
            .expect("tenant GUC");
        let facts = read_candidate_facts(&mut maintenance, &family, "v1", None)
            .expect("candidate facts")
            .expect("placement row");
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let pair = rt.block_on(visible_pair(
            &s.face,
            &Candidate {
                tenant: TenantId(s.tenant),
                workspace: WorkspaceId(s.workspace),
                collection: &facts.collection,
                candidate_version: "v1",
                candidate_tombstoned: &facts.candidate_tombstoned,
                serving_version: None,
                serving_tombstoned: &facts.serving_tombstoned,
            },
        ));
        assert_eq!(
            pair,
            (Some(("v1".to_owned(), 2)), None),
            "both points count"
        );

        let args: Vec<String> = [
            "--tenant",
            &s.tenant.to_string(),
            "--workspace",
            &s.workspace.to_string(),
            "--domain",
            "private_memory",
            "--projection-kind",
            "PRIVATE_MEMORY",
            "--version",
            "v1",
            "--continuation",
            "pass",
            "--qdrant-port",
            &port.to_string(),
        ]
        .iter()
        .map(|a| a.to_string())
        .collect();
        assert_eq!(
            crate::projection_serve::run(&args),
            0,
            "projection-serve switched"
        );
        let mut s = s;
        let serving: bool =
            s.db.query_one(
                "SELECT serving FROM projection.stream_checkpoints \
                 WHERE tenant_id = $1 AND scope_id = $2 AND projection_version = 'v1'",
                &[&s.tenant, &s.workspace],
            )
            .expect("checkpoint")
            .get(0);
        assert!(serving, "v1 is serving after the first activation");
    }

    #[test]
    fn a_non_loopback_qdrant_host_is_refused() {
        assert!(VisibleFace::connect("10.0.0.4", 6333).is_err());
        assert!(VisibleFace::connect("127.0.0.1", 6333).is_ok());
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
