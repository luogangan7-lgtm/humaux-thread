//! T7.1 real-network smoke test — §19 production embedding model is `text-embedding-v4`
//! (1024-dim default). `infra-egress::http` now injects `Authorization: Bearer <key>` +
//! `Content-Type: application/json` on every call (`crates/infra-egress/src/http.rs` module
//! doc, "Credential injection") via `EnvCredentialSource` reading this same
//! `DASHSCOPE_API_KEY` variable, so state 3 below is a real, network-verified call, not a
//! structurally-correct-but-untested branch.
//!
//! Three-state, per the task brief (never the usual §79.2 `run_db_fixture` three-state, since
//! the primary gate here is the API key, not the database):
//! 1. `DASHSCOPE_API_KEY` unset → SKIP, prints `missing object: DASHSCOPE_API_KEY` verbatim.
//! 2. Key set but `HUMAUX_TEST_PG_DSN` unset/unreachable → SKIP via `run_db_fixture` (§79.2 —
//!    the real DashScope adapter's disclosure-ledger write, hard rule ⑤, needs a live
//!    `RetrievalWorkerDbPool`, so this half cannot be waived independently of the key check).
//! 3. Both present → real call, asserts `dimension == 1024` and **panics** on any `Err` (an
//!    operator supplied a real key and a real DB; a failure here is a genuine regression, not
//!    an expected gap — unlike the pre-header-injection version of this test, which could only
//!    log the failure and had no way to distinguish "expected" from "broken").

use humaux_adapters::disclosure::DisclosureSource;
use humaux_adapters::postgres::RetrievalWorkerDbPool;
use humaux_domain::egress::ProcessorId;
use humaux_domain::ids::TenantId;
use humaux_retrieval_provider::adapters::DashscopeEmbeddingProvider;
use humaux_retrieval_provider::admission::RetrievalPurpose;
use humaux_retrieval_provider::contract::{
    EmbeddingModelDescriptor, EmbeddingProvider, ModelId, SealedRetrievalQuery,
};
use humaux_retrieval_provider::metrics::{
    Provider, Region, retrieval_provider_requests_total_count,
    retrieval_provider_tokens_total_count,
};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use uuid::Uuid;

/// Same `SET ROLE` trick `crates/adapters/src/postgres.rs`'s own (private, test-only) helper
/// uses — reproduced here rather than imported (that helper is not `pub`, module doc: "Test-
/// only: production wrappers connect with a DSN that already authenticates as the target role
/// directly").
fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

fn text_embedding_v4() -> EmbeddingModelDescriptor {
    EmbeddingModelDescriptor {
        model_id: ModelId("text-embedding-v4".to_string()),
        model_revision: "2026-08".to_string(),
        // §19 production default — DashScope's real Matryoshka option set for this model.
        dimension_options: vec![64, 128, 256, 512, 768, 1024, 1536, 2048],
        max_input_tokens: 8192,
        batch_supported: true,
        dense_supported: true,
        sparse_supported: false,
    }
}

struct Handle {
    rt: tokio::runtime::Runtime,
    // `Option` so `DashscopeEmbeddingProvider::new` (which takes the pool by value) can
    // `.take()` it out — `Handle` implements `Drop`, so a plain field cannot be moved out of.
    pool: Option<RetrievalWorkerDbPool>,
    admin: Client,
    tenant_id: Uuid,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup (repo CLAUDE.md hard rule ④) — `ops.data_disclosures` itself is
        // append-only (§7.4) and its FK'd `control.tenants` row can therefore never be deleted
        // either, same permanent-leak shape `disclosure_ledger.rs`'s own Drop documents.
        let _ = self.admin.execute(
            "DELETE FROM private.evidence_objects WHERE tenant_id = $1",
            &[&self.tenant_id],
        );
        let _ = self.admin.execute(
            "DELETE FROM control.private_reasoning_domains WHERE tenant_id = $1",
            &[&self.tenant_id],
        );
    }
}

struct SmokeFixture;

impl DbIntegrationFixture for SmokeFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        // 与 `adapters/tests/disclosure_ledger.rs::guard_trigger_rejects_truncate` 的
        // `TRUNCATE … CASCADE` 互斥：本条走真出境路径，会写 `ops.data_disclosures`，
        // 而那条测试必须拿这两张表的 AccessExclusiveLock（见
        // `humaux_testkit::DISCLOSURE_LEDGER_ADVISORY_LOCK` 的 doc）。两个 binary 并行跑，
        // 交叉加锁即成环——实测过一次真 40P01。取共享锁，只在那一条测试跑时让路。
        admin
            .execute(
                "SELECT pg_advisory_lock_shared($1)",
                &[&humaux_testkit::DISCLOSURE_LEDGER_ADVISORY_LOCK],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        let role_dsn = dsn_as_role(&dsn, "role_retrieval_worker");
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let pool = rt
            .block_on(RetrievalWorkerDbPool::connect(&role_dsn))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        let mut admin = admin;
        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"dashscope_live_smoke throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        Ok(Handle {
            rt,
            pool: Some(pool),
            admin,
            tenant_id,
        })
    }
}

fn seed_evidence(handle: &mut Handle) -> Uuid {
    let reasoning_domain_id: Uuid = handle
        .admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
             VALUES ($1, 'dashscope_live_smoke throwaway domain') \
             RETURNING reasoning_domain_id",
            &[&handle.tenant_id],
        )
        .expect("seed reasoning domain row")
        .get(0);

    handle
        .admin
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', $2, 'PRIVATE', 'DirectUserInput', 'TENANT_SHARED', $3) \
             RETURNING evidence_id",
            &[&handle.tenant_id, &vec![7u8; 32], &reasoning_domain_id],
        )
        .expect("seed evidence row")
        .get(0)
}

#[test]
fn dashscope_live_smoke() {
    // Same predicate `EnvCredentialSource::credential` actually uses (infra-egress/src/http.rs)
    // — a plain `std::env::var(..).is_ok()` here would treat `DASHSCOPE_API_KEY=""` as "present"
    // while the production path treats it as absent (`read_env_var` filters empty), which used
    // to send this test into state 3 with a message that claimed state 1's condition.
    if humaux_contracts::config_registry::read_env_var("DASHSCOPE_API_KEY").is_none() {
        // 唯一判定点。`HUMAUX_REQUIRE_DASHSCOPE=1` 把这条从「静默跳过」变成「必须真打出去」——
        // 本会话就吃过亏：live smoke 到底跑没跑过，只能靠人翻输出确认。
        humaux_testkit::skip_or_fail(
            "dashscope_live_smoke",
            "missing object: DASHSCOPE_API_KEY",
            humaux_testkit::ExternalDep::DashScope,
        );
        return;
    }

    run_db_fixture::<SmokeFixture, _>("dashscope_live_smoke", |mut handle| {
        let evidence_id = seed_evidence(&mut handle);
        let tenant_id = TenantId(handle.tenant_id);
        let processor = ProcessorId(Uuid::now_v7());

        let provider = DashscopeEmbeddingProvider::new(
            handle
                .pool
                .take()
                .expect("pool set by SmokeFixture::isolate"),
            processor,
            text_embedding_v4(),
            "cn-beijing",
            DisclosureSource::Evidence(evidence_id),
        )
        .expect("DashscopeEmbeddingProvider::new must build its HttpExternalCall");

        // §80.2: the adapter->metrics wiring (`record_call_outcome`'s
        // `metrics::record_provider_call` call, adapters.rs) has no witness that goes through
        // the real adapter rather than calling the metrics emit function directly — every
        // `crates/testkit/tests/metrics/retrieval_provider_*.rs` witness does the latter. This
        // is the one in-process real-provider call site that can assert a delta through the
        // actual adapter instead.
        let before_requests = retrieval_provider_requests_total_count(
            Provider::DashScope,
            RetrievalPurpose::Embedding,
            Region::CnBeijing,
            "ok",
        );
        let before_tokens =
            retrieval_provider_tokens_total_count(Provider::DashScope, RetrievalPurpose::Embedding);

        let query = SealedRetrievalQuery::seal("Humaux Thread retrieval provider smoke test");
        let result = handle.rt.block_on(provider.embed_queries(
            tenant_id,
            1024,
            std::slice::from_ref(&query),
        ));

        // §19: production text-embedding-v4 default dimension is 1024. An operator who reaches
        // this branch supplied a real `DASHSCOPE_API_KEY` and a real DB — a failure here is a
        // genuine regression (header injection broken, endpoint/model drifted, response shape
        // changed), not an expected gap, so it must fail the test, not merely log.
        match result {
            Ok(batch) => {
                assert_eq!(batch.dimension, 1024);
                assert_eq!(batch.vectors.len(), 1);
                assert_eq!(batch.vectors[0].len(), 1024);

                let after_requests = retrieval_provider_requests_total_count(
                    Provider::DashScope,
                    RetrievalPurpose::Embedding,
                    Region::CnBeijing,
                    "ok",
                );
                let after_tokens = retrieval_provider_tokens_total_count(
                    Provider::DashScope,
                    RetrievalPurpose::Embedding,
                );
                assert_eq!(
                    after_requests,
                    before_requests + 1,
                    "one real embed_queries call must move \
                     retrieval_provider_requests_total{{result=\"ok\"}} by exactly +1"
                );
                assert_eq!(
                    after_tokens,
                    before_tokens + batch.input_tokens,
                    "retrieval_provider_tokens_total must move by exactly the call's own \
                     input_tokens"
                );
            }
            Err(e) => {
                panic!(
                    "dashscope_live_smoke: call failed ({e:?}) despite a live \
                     DASHSCOPE_API_KEY and a reachable DB — the authorize/reserve/finalize \
                     sequence ran to completion, so this is a real regression (auth header, \
                     endpoint, or response shape), not the pre-header-injection gap this test \
                     used to document"
                );
            }
        }
    });
}
