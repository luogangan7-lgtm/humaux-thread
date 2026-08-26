//! T5.1 integration test — §16.1.1 "同一 fingerprint 可以有多次 processing run / 不同 output
//! digest；旧输出不被覆盖" against a real Postgres, on `migrations/0062_processing_runs_
//! fingerprint_fields.sql`'s real `private.processing_runs` columns.
//!
//! This is a DB-layer assertion, not a re-test of `humaux_projection::fingerprint::source_hash`
//! itself (that function's per-axis sensitivity is covered by `crates/projection/src/
//! fingerprint.rs`'s own `#[cfg(test)]` module, G16-4/G80-34) — this file computes one real
//! `SourceHash` via that function and then proves the *table* never collapses two runs sharing
//! it into one row: two `INSERT`s under the identical `source_hash` must each keep its own
//! `processing_run_id` and its own `output_digest`, and neither becomes visible to a later read
//! only under one of the two — i.e. the second `INSERT` is not an `UPDATE` in disguise.
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or the migration missing all print a
//! visible SKIP and return.

use humaux_domain::evidence::payload_sha256;
use humaux_projection::fingerprint::{ProcessingInputFingerprintInputs, source_hash};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

struct Handle {
    admin: Client,
    tenant_id: Uuid,
    evidence_id: Uuid,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup (repo CLAUDE.md hard rule ④), same shape as
        // `consolidate_snapshot.rs`'s `Handle::drop`.
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM private.observations WHERE tenant_id = '{0}'; \
             DELETE FROM private.processing_runs WHERE tenant_id = '{0}'; \
             DELETE FROM private.events WHERE event_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{0}'); \
             DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
             DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}'; \
             DELETE FROM control.tenants WHERE tenant_id = '{0}';",
            self.tenant_id
        ));
    }
}

struct ProcessingRunsFixture;

impl DbIntegrationFixture for ProcessingRunsFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        // §16.1.1 columns this migration adds — `output_count`/`provider_request_id`/
        // `completed_at` did not exist before 0062, this is the "migration missing" leg of
        // the three-state skip.
        let migrated: bool = admin
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                   WHERE table_schema = 'private' AND table_name = 'processing_runs' \
                     AND column_name = 'output_count')",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "private.processing_runs.output_count does not exist — run `cargo xtask \
                 migrate` against HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }

        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"processing_runs_fingerprint_rerun.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        let reasoning_domain_id: Uuid = admin
            .query_one(
                "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
                 VALUES ($1, 'processing_runs_fingerprint_rerun.rs domain') \
                 RETURNING reasoning_domain_id",
                &[&tenant_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        // Same minimal EVENT-kind evidence recipe as `consolidate_snapshot.rs`.
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

        Ok(Handle {
            admin,
            tenant_id,
            evidence_id,
        })
    }
}

/// 插入一条 processing run，**十二轴全给值**（migration 0064 把它们设成 NOT NULL：§1.3 缺任
/// 一项，「回放 / 模型对比 / 精准重建 / 为什么同一输入结果变了」四问就答不了）。
/// 抽成函数只为让调用方保持在 clippy 的行数闸内，语义与内联时逐字相同。
fn insert_run(
    admin: &mut Client,
    tenant_id: &Uuid,
    evidence_id: &Uuid,
    fingerprint_bytes: &[u8],
    output_digest: &[u8],
    output_count: i32,
    payload_sha: &Vec<u8>,
) -> Uuid {
    admin
        .query_one(
            "INSERT INTO private.processing_runs \
               (tenant_id, evidence_id, processor_kind, processor_version, \
                model_provider, model_id, model_revision, prompt_version, \
                prompt_hash, parser_version, evidence_payload_sha256, \
                source_hash, output_digest, output_count, context_snapshot_seq) \
             VALUES ($1, $2, 'distill', '1', 'alibaba', 'qwen3-max', '2026-08-01', '1', \
                     'ph-1', 'pv-1', ARRAY[$5::bytea], $3, $4, $6, 42) \
             RETURNING processing_run_id",
            &[
                tenant_id,
                evidence_id,
                &fingerprint_bytes,
                &output_digest,
                payload_sha,
                &output_count,
            ],
        )
        .expect("insert processing run")
        .get(0)
}

#[test]
fn same_fingerprint_rerun_keeps_independent_rows_never_overwritten() {
    run_db_fixture::<ProcessingRunsFixture, _>(
        "same_fingerprint_rerun_keeps_independent_rows_never_overwritten",
        |mut handle| {
            let ev = [payload_sha256(b"one evidence payload")];
            let inputs = ProcessingInputFingerprintInputs {
                evidence_payload_sha256: &ev,
                processor_kind: "distill",
                processor_version: "1",
                model_provider: "alibaba",
                model_id: "qwen3-max",
                model_revision: "2026-08-01",
                prompt_version: "1",
                prompt_hash: "prompt-hash-fixed",
                embedding_version: None,
                parser_version: "1",
                card_builder_version: None,
                context_snapshot_seq: 42,
            };
            let fingerprint = source_hash(&inputs);
            let fingerprint_bytes = fingerprint.as_bytes().to_vec();

            // One 32-byte payload hash standing in for the evidence set the fingerprint
            // was computed over (§16.1.1: the run row must be able to re-derive its own
            // source_hash, so the axis columns are NOT NULL — see migration 0064).
            let payload_sha: Vec<u8> = vec![7u8; 32];
            let run_1_output = vec![1u8; 32];
            let run_1_id: Uuid = insert_run(
                &mut handle.admin,
                &handle.tenant_id,
                &handle.evidence_id,
                &fingerprint_bytes,
                &run_1_output,
                3,
                &payload_sha,
            );

            // Second run under the *same* source_hash — §16.1.1's own words: "重复 run 不覆盖
            // 前一 run". A plain second INSERT (not an UPSERT/ON CONFLICT) is the correct
            // shape precisely because `processing_run_id` is a fresh UUIDv7 per row, not a
            // natural key derived from `source_hash` — there is no conflict target for
            // `source_hash` to collide on.
            let run_2_output = vec![2u8; 32];
            let run_2_id: Uuid = insert_run(
                &mut handle.admin,
                &handle.tenant_id,
                &handle.evidence_id,
                &fingerprint_bytes,
                &run_2_output,
                5,
                &payload_sha,
            );

            assert_ne!(
                run_1_id, run_2_id,
                "same source_hash must not collapse two runs into one processing_run_id"
            );

            let rows = handle
                .admin
                .query(
                    "SELECT processing_run_id, output_digest, output_count \
                     FROM private.processing_runs \
                     WHERE tenant_id = $1 AND source_hash = $2 \
                     ORDER BY output_count",
                    &[&handle.tenant_id, &fingerprint_bytes],
                )
                .expect("select both runs by shared fingerprint");

            assert_eq!(
                rows.len(),
                2,
                "both runs for this fingerprint must still be present, not merged into one row"
            );

            let (first_id, first_digest, first_count): (Uuid, Vec<u8>, i32) =
                (rows[0].get(0), rows[0].get(1), rows[0].get(2));
            let (second_id, second_digest, second_count): (Uuid, Vec<u8>, i32) =
                (rows[1].get(0), rows[1].get(1), rows[1].get(2));

            assert_eq!(first_id, run_1_id);
            assert_eq!(
                first_digest, run_1_output,
                "run 1's own output_digest, untouched"
            );
            assert_eq!(first_count, 3);

            assert_eq!(second_id, run_2_id);
            assert_eq!(
                second_digest, run_2_output,
                "run 2's own output_digest — a real second row, not run 1 UPDATEd in place"
            );
            assert_eq!(second_count, 5);
        },
    );
}
