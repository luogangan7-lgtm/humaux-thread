//! Fixer-review G11-R1/§79.2 integration coverage for `run_once` — before this review, `grep`
//! over the whole workspace for `run_once` found no caller and no test, and the crate's own
//! "positive sentinel" only asserted a fact about its own `FakePort` fixture 15 lines above it
//! (a change to any production file could never turn it red). This drives the real call site
//! against a real `ConsolidationDbPool`: empty-input skip (no wasted BYOK call, §11.5.1) and
//! the full inference->publish path (§11.7/§11.8/§11.9's persisted side effects).
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or the migration missing all print a
//! visible SKIP and return — same fixture shape as `crates/adapters/tests/consolidate_snapshot.rs`.

use humaux_adapters::postgres::ConsolidationDbPool;
use humaux_application::consolidate::{
    ContentSha256, PrivateReasoningError, PrivateReasoningPort, PrivateReasoningResult,
    ProviderTraceRef, ReasoningRouteBindingId, ReasoningRouteBindingVersion,
    SealedPrivateReasoningRequest,
};
use humaux_consolidation_worker::{RunOnceError, run_once};
use humaux_domain::authority::{AuthorityClass, EvidenceId};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

/// Serializes this file's tests — each seeds its own throwaway tenant, but keeping them
/// sequential avoids any question about shared-connection-pool exhaustion across two live-DB
/// tests running at once (same conservative stance `consolidate_snapshot.rs` takes).
static SERIAL_GUARD: Mutex<()> = Mutex::new(());

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options[role]={role}")
}

/// Records whether `infer` was ever called — the `SkipToNoOutput` test's whole point (§11.5.1:
/// an empty input set must never burn a BYOK request) is unobservable without this.
struct FakePort {
    calls: AtomicUsize,
    reply: PrivateReasoningResult,
}

#[async_trait::async_trait]
impl PrivateReasoningPort for FakePort {
    async fn infer(
        &self,
        _req: SealedPrivateReasoningRequest,
    ) -> Result<PrivateReasoningResult, PrivateReasoningError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.reply.clone())
    }
}

struct Handle {
    rt: tokio::runtime::Runtime,
    consolidation: ConsolidationDbPool,
    admin: Client,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    evidence_id: Uuid,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup, same shape as `consolidate_snapshot.rs::Handle::drop` — this
        // file never touches a schema/table of its own, only rows it created under its own
        // throwaway tenant.
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM private.memory_rollup_sources WHERE rollup_id IN \
               (SELECT rollup_id FROM private.memory_rollups WHERE tenant_id = '{0}'); \
             DELETE FROM private.memory_rollups WHERE tenant_id = '{0}'; \
             DELETE FROM private.memory_consolidation_inputs WHERE run_id IN \
               (SELECT run_id FROM private.memory_consolidation_runs WHERE tenant_id = '{0}'); \
             DELETE FROM private.memory_consolidation_runs WHERE tenant_id = '{0}'; \
             DELETE FROM private.memory_evidence WHERE memory_id IN \
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

struct RunOnceFixture;

impl DbIntegrationFixture for RunOnceFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let migrated: bool = admin
            .query_one(
                "SELECT to_regclass('private.memory_consolidation_inputs') IS NOT NULL",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "private.memory_consolidation_inputs does not exist — run `cargo xtask migrate` \
                 against HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }

        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"run_once_e2e.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        let reasoning_domain_id: Uuid = admin
            .query_one(
                "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
                 VALUES ($1, 'run_once_e2e.rs domain') RETURNING reasoning_domain_id",
                &[&tenant_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        let evidence_id: Uuid = admin
            .query_one(
                "INSERT INTO private.evidence_objects \
                   (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                    visibility_class, reasoning_domain_id) \
                 VALUES ($1, 'EVENT', $2, 'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $3) \
                 RETURNING evidence_id",
                &[&tenant_id, &vec![0u8; 32], &reasoning_domain_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        admin
            .execute(
                "INSERT INTO private.events (event_id, event_kind, payload) \
                 VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
                &[&evidence_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        let consolidation_dsn = dsn_as_role(&dsn, "role_consolidation_worker");
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let consolidation = rt
            .block_on(ConsolidationDbPool::connect(&consolidation_dsn))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            consolidation,
            admin,
            tenant_id,
            reasoning_domain_id,
            evidence_id,
        })
    }
}

fn seed_active_memory(handle: &mut Handle) -> Uuid {
    let mut txn = handle.admin.transaction().expect("begin seed txn");
    let memory_id: Uuid = txn
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id, memory_type, content, visibility_class, \
                authority_class, confidence, status, asserted_at) \
             VALUES ($1, 'NOTE', '{}'::jsonb, 'TENANT_SHARED', \
                     'PrivateKnowledge', 0.7, 'active', now()) \
             RETURNING memory_id",
            &[&handle.tenant_id],
        )
        .expect("seed active memory")
        .get(0);
    txn.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) \
         VALUES ($1, $2, 'PRIMARY')",
        &[&memory_id, &handle.evidence_id],
    )
    .expect("insert memory_evidence link");
    txn.commit().expect("commit seed txn");
    memory_id
}

/// Empty tenant, no eligible memories: `run_once` must reach `SUCCEEDED_NO_OUTPUT` without ever
/// calling `PrivateReasoningPort::infer` (§11.5.1 — no BYOK spend for a guaranteed-empty run).
#[test]
fn run_once_skips_inference_when_no_inputs() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<RunOnceFixture, _>("run_once_skips_inference_when_no_inputs", |handle| {
        let port = FakePort {
            calls: AtomicUsize::new(0),
            reply: PrivateReasoningResult {
                output_bytes: b"unused".to_vec(),
                output_sha256: ContentSha256([0u8; 32]),
                provider_trace: ProviderTraceRef("unused".into()),
                model_call_id: Uuid::from_u128(0x2001),
                binding_id: ReasoningRouteBindingId(Uuid::from_u128(0x1001)),
                binding_version: ReasoningRouteBindingVersion(1),
            },
        };

        let outcome = handle.rt.block_on(run_once(
            &handle.consolidation,
            &port,
            handle.tenant_id,
            handle.reasoning_domain_id,
            ReasoningRouteBindingId(Uuid::from_u128(0x1001)),
            ReasoningRouteBindingVersion(1),
            None,
            10_000,
            |_ids, _result| panic!("build_rollup must not be called on the empty-input path"),
        ));

        assert_eq!(
            outcome.map_err(|e| e.to_string()),
            Ok(humaux_adapters::consolidate_repo::PublishOutcome::NoOutput)
        );
        assert_eq!(
            port.calls.load(Ordering::SeqCst),
            0,
            "PrivateReasoningPort::infer must not be called for an empty input set"
        );
    });
}

/// One eligible memory: `run_once` must call `infer` exactly once, publish a rollup whose
/// content actually comes from the returned `PrivateReasoningResult` (not silently discarded —
/// the blocker this test's own review found), and persist a non-zero `manifest_hash` +
/// `authority_class`/`visibility_class` on the written rows.
#[test]
fn run_once_publishes_rollup_from_inference_result() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<RunOnceFixture, _>(
        "run_once_publishes_rollup_from_inference_result",
        |mut handle| {
            let memory_id = seed_active_memory(&mut handle);
            let evidence_id = handle.evidence_id;

            let port = FakePort {
                calls: AtomicUsize::new(0),
                reply: PrivateReasoningResult {
                    output_bytes: b"the actual inference output".to_vec(),
                    output_sha256: ContentSha256([9u8; 32]),
                    provider_trace: ProviderTraceRef("trace-e2e".into()),
                    model_call_id: Uuid::from_u128(0x2001),
                    binding_id: ReasoningRouteBindingId(Uuid::from_u128(0x1001)),
                    binding_version: ReasoningRouteBindingVersion(1),
                },
            };

            let outcome = handle
                .rt
                .block_on(run_once(
                    &handle.consolidation,
                    &port,
                    handle.tenant_id,
                    handle.reasoning_domain_id,
                    // This fake-port E2E proves exact Binding pair propagation only. It does not
                    // exercise or claim the private-worker database admission resolver.
                    ReasoningRouteBindingId(Uuid::from_u128(0x1001)),
                    ReasoningRouteBindingVersion(1),
                    None,
                    10_000,
                    |ids, result| {
                        // Proves the result reaches the caller instead of being dropped, and
                        // that `sources` can only be built from the ids this run itself
                        // materialized (§11.8 typestate).
                        assert_eq!(result.output_bytes, b"the actual inference output");
                        let content = serde_json::json!({
                            "summary_of": result.output_sha256.0.len(),
                        });
                        let sources = ids
                            .iter()
                            .map(|id| (*id, EvidenceId(evidence_id)))
                            .collect();
                        Ok((content, AuthorityClass::PrivateKnowledge, sources))
                    },
                ))
                .expect("run_once must succeed");

            assert_eq!(port.calls.load(Ordering::SeqCst), 1);
            let humaux_adapters::consolidate_repo::PublishOutcome::Published { rollup_id } =
                outcome
            else {
                panic!("expected Published, got {outcome:?}");
            };

            let row = handle
                .admin
                .query_one(
                    "SELECT authority_class, visibility_class, visibility_workspace_id \
                     FROM private.memory_rollups WHERE rollup_id = $1",
                    &[&rollup_id],
                )
                .expect("read back rollup row");
            let authority_class: String = row.get(0);
            let visibility_class: String = row.get(1);
            let visibility_workspace_id: Option<Uuid> = row.get(2);
            assert_eq!(authority_class, "PrivateKnowledge");
            assert_eq!(visibility_class, "TENANT_SHARED");
            assert!(visibility_workspace_id.is_none());

            let source_memory_ids: Vec<Uuid> = handle
                .admin
                .query(
                    "SELECT memory_id FROM private.memory_rollup_sources WHERE rollup_id = $1",
                    &[&rollup_id],
                )
                .expect("read back rollup sources")
                .into_iter()
                .map(|r| r.get(0))
                .collect();
            assert_eq!(source_memory_ids, vec![memory_id]);

            let run_row = handle
                .admin
                .query_one(
                    "SELECT status, manifest_hash, input_snapshot_seq, output_digest \
                     FROM private.memory_consolidation_runs \
                     WHERE run_id = (SELECT run_id FROM private.memory_rollups WHERE rollup_id = $1)",
                    &[&rollup_id],
                )
                .expect("read back run row");
            let status: String = run_row.get(0);
            let manifest_hash: Option<Vec<u8>> = run_row.get(1);
            let input_snapshot_seq: Option<i64> = run_row.get(2);
            let output_digest: Option<Vec<u8>> = run_row.get(3);
            assert_eq!(status, "SUCCEEDED");
            assert!(
                manifest_hash
                    .as_deref()
                    .is_some_and(|h| h.iter().any(|b| *b != 0)),
                "§11.8 input_manifest_hash must be a real, non-zero digest, not the old \
                 hardcoded [0u8; 32] placeholder"
            );
            assert!(input_snapshot_seq.is_some());
            assert!(output_digest.is_some());
        },
    );
}

/// Compile-time sanity: `RunOnceError` must still implement `Display` after the §11.8 redaction
/// fix — this is not a redaction test itself (that lives in `humaux_application::consolidate`'s
/// own unit tests via `Debug`/`Display` on `PrivateReasoningError`), just proof the two crates'
/// error types still compose.
#[test]
fn run_once_error_displays() {
    let e = RunOnceError::Reasoning(PrivateReasoningError::new("plain marker, no secret"));
    assert!(!format!("{e}").is_empty());
}
