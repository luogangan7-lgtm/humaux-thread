//! `retrieval-provider::tests::contract_tests` — §19 "Provider Plane 测试 · Contract Tests" (spec lines ~4120-4131),
//!   run against [`TestDoubleProvider`] — network-free, deterministic, must stay green independent of any live
//!   credential (task brief: "Contract/Failure/Cost/Migration 四类测试全部用 test double").
//! Depends-on: crates=[humaux-domain, humaux-local-secret-scan, humaux-projection, humaux-retrieval, tokio, uuid];
//!   services=[]; env=[CARGO_MANIFEST_DIR]; modules=[domain::authority, domain::dataclass, domain::error,
//!   domain::evidence, domain::identity, domain::ids, domain::memory, humaux-local-secret-scan, projection::card,
//!   retrieval-provider::adapters, retrieval-provider::contract, retrieval::request]
//! Called-by: [cargo-test]
//! Invariants: [network-free and credential-free; CARGO_MANIFEST_DIR only locates repo fixtures, a missing fixture fails the test]
//! Spec: §19
//!
//! The eight bullets are the spec's own list, verbatim: embedding input/output · dimension ·
//! batch semantics · empty input · Unicode · max token · rerank ordering · provider error
//! mapping. Each gets its own `#[tokio::test]` below, in that order.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    time::Duration,
};

use humaux_domain::authority::MemoryId;
use humaux_domain::dataclass::DataClass;
use humaux_domain::error::ErrorCode;
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_domain::memory::MemoryType;
use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig};
use humaux_local_secret_scan::{SealedRetrievalCard, SealedRetrievalQuery};
use humaux_projection::card::{
    CardBudget, CardBuildOutcome, CardInput, EgressDisposition, build_card,
};
use humaux_retrieval::request::{RetrievalIntent, build_request};
use humaux_retrieval_provider::adapters::TestDoubleProvider;
use humaux_retrieval_provider::contract::{
    CalibrationProfileId, EmbeddingModelDescriptor, EmbeddingProvider, ModelId,
    RerankModelDescriptor, RerankProvider, RerankScoreSemantics, RetrievalQueryCallContext,
};
use uuid::Uuid;

fn test_scanner_config() -> LocalSecretScannerConfig {
    let executable =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gitleaks-fixture.sh");
    LocalSecretScannerConfig {
        expected_executable_sha256: humaux_domain::evidence::payload_sha256(
            &fs::read(&executable).expect("read isolated scanner executable"),
        )
        .to_hex(),
        executable,
        expected_version: "retrieval-provider-contract-fixture".into(),
        timeout: Duration::from_secs(10),
        max_payload_bytes: 64 * 1024,
        finding_exit_code: 1,
    }
}

fn with_scanner<T>(run: impl FnOnce(&LocalSecretScanner) -> T) -> T {
    let scanner = LocalSecretScanner::new(test_scanner_config()).expect("pinned test scanner");
    run(&scanner)
}

fn query(text: impl Into<String>) -> SealedRetrievalQuery {
    let intent = RetrievalIntent::new(text.into(), vec![], BTreeSet::new(), BTreeSet::new())
        .expect("nonempty query fixture");
    let profile = humaux_retrieval::request::resolve_registered_retrieval_profile(&BTreeMap::new())
        .expect("registered profile");
    let request = build_request(intent, &profile).expect("request");
    with_scanner(|scanner| {
        scanner
            .seal_query(&request.trusted_query().expect("text request"))
            .expect("clean pinned scan")
    })
}

fn card(text: impl Into<String>) -> SealedRetrievalCard {
    let text = text.into();
    let title_chars = text.chars().count();
    let CardBuildOutcome::Card(card) = build_card(
        CardInput {
            memory_id: MemoryId::new(),
            memory_type: MemoryType::Fact,
            data_class: DataClass::Private,
            egress_disposition: EgressDisposition::PolicyGated,
            workspace_id: None,
            topic: None,
            effective_from: std::time::SystemTime::UNIX_EPOCH,
            title: text,
            key_claim: Some("fixture claim".to_owned()),
            entities: vec![],
            evidence_excerpt: Some("fixture evidence".to_owned()),
        },
        // Keep this fixture's sealed body to the requested title plus the canonical
        // template separators, so token-boundary tests assert the actual card bytes.
        CardBudget {
            max_chars: title_chars,
        },
    ) else {
        panic!("private fixture builds a card")
    };
    with_scanner(|scanner| scanner.seal_card(&card).expect("clean pinned scan"))
}

/// `text-embedding-v4`-shaped fixture (§19's own production model): three Matryoshka
/// dimensions, batching on, a small `max_input_tokens` so the "max token" test doesn't need a
/// multi-KB fixture string.
fn embedding_model() -> EmbeddingModelDescriptor {
    EmbeddingModelDescriptor {
        model_id: ModelId("text-embedding-v4".to_string()),
        model_revision: "2026-08".to_string(),
        dimension_options: vec![256, 512, 1024],
        max_input_tokens: 64,
        batch_supported: true,
        dense_supported: true,
        sparse_supported: false,
    }
}

/// `qwen3-rerank`-shaped fixture (§19's own production model).
fn rerank_model() -> RerankModelDescriptor {
    RerankModelDescriptor {
        model_id: ModelId("qwen3-rerank".to_string()),
        model_revision: "2026-08".to_string(),
        max_documents: 4,
        max_input_tokens: 200,
        score_semantics: RerankScoreSemantics::RawLogit,
        calibration_profile: CalibrationProfileId("qwen3-rerank-2026-08".to_string()),
    }
}

fn provider() -> TestDoubleProvider {
    TestDoubleProvider::new(embedding_model(), rerank_model())
}

async fn embed_queries(
    provider: &impl EmbeddingProvider,
    dimension: u32,
    queries: &[SealedRetrievalQuery],
) -> Result<humaux_retrieval_provider::contract::EmbeddingBatch, ErrorCode> {
    let tenant_id = TenantId::new();
    let workspace_id = WorkspaceId::new();
    let authorization = AuthorizationScope::new(
        tenant_id,
        PrincipalId::new(),
        Some(UserId::new()),
        BoundedSet::new([workspace_id]).expect("bounded workspace fixture"),
    );
    let context = RetrievalQueryCallContext::new(
        &authorization,
        workspace_id,
        Uuid::now_v7(),
        Uuid::now_v7(),
        1,
    )
    .expect("valid query call context");
    provider.embed_queries(&context, dimension, queries).await
}

// ============================================================================
// 1. embedding input/output
// ============================================================================

/// Basic shape: N sealed queries in, N vectors out, in the same order, each at the requested
/// dimension.
#[tokio::test]
async fn embedding_input_output_shape() {
    let provider = provider();
    let queries = vec![
        query("first query"),
        query("second query"),
        query("third query"),
    ];

    let batch = embed_queries(&provider, 256, &queries)
        .await
        .expect("test double never fails a well-formed request");

    assert_eq!(batch.vectors.len(), queries.len());
    for vector in &batch.vectors {
        assert_eq!(vector.len(), 256);
    }
    // Different input text must not collapse to identical output (a no-op provider would be
    // useless but would still pass every other assertion in this test).
    assert_ne!(batch.vectors[0], batch.vectors[1]);
    assert_ne!(batch.vectors[1], batch.vectors[2]);
}

/// Query and card wrappers use their canonical seal paths; both must reach the provider
/// with their requested vector shape after the real pinned scan.
#[tokio::test]
async fn embedding_card_and_query_paths_agree_on_equal_text() {
    let provider = provider();
    let query_batch = embed_queries(&provider, 512, &[query("same text")])
        .await
        .unwrap();
    let card_batch = provider
        .embed_cards(TenantId::new(), 512, &[card("same text")])
        .await
        .unwrap();
    assert_eq!(query_batch.vectors[0].len(), card_batch.vectors[0].len());
}

// ============================================================================
// 2. dimension
// ============================================================================

/// A `dimension` not in `dimension_options` must be rejected before any "call" happens (§19
/// "Incompatible Model Change": dimension is never a free per-request choice).
#[tokio::test]
async fn dimension_outside_model_options_is_rejected() {
    let provider = provider();
    let result = embed_queries(&provider, 999, &[query("x")]).await;
    assert_eq!(result.err(), Some(ErrorCode::InvalidInput));
}

/// Each of the model's declared `dimension_options` must actually work and produce a vector of
/// that exact length.
#[tokio::test]
async fn each_declared_dimension_option_produces_a_vector_of_that_length() {
    let provider = provider();
    for &dimension in &embedding_model().dimension_options {
        let batch = embed_queries(&provider, dimension, &[query("x")])
            .await
            .unwrap();
        assert_eq!(batch.vectors[0].len(), dimension as usize);
    }
}

// ============================================================================
// 3. batch semantics
// ============================================================================

/// `batch_supported = true`: N items in one call must return N vectors, not just the first.
#[tokio::test]
async fn batch_supported_model_accepts_multiple_items_in_one_call() {
    let provider = provider();
    let queries: Vec<SealedRetrievalQuery> = (0..5).map(|i| query(format!("item {i}"))).collect();
    let batch = embed_queries(&provider, 256, &queries).await.unwrap();
    assert_eq!(batch.vectors.len(), 5);
}

/// `batch_supported = false`: more than one item in a single call must be rejected — the
/// caller has to split into per-item calls instead.
#[tokio::test]
async fn batch_unsupported_model_rejects_more_than_one_item() {
    let mut model = embedding_model();
    model.batch_supported = false;
    let provider = TestDoubleProvider::new(model, rerank_model());

    let one = embed_queries(&provider, 256, &[query("solo")]).await;
    assert!(one.is_ok(), "a single item must still work");

    let two = embed_queries(&provider, 256, &[query("a"), query("b")]).await;
    assert_eq!(two.err(), Some(ErrorCode::InvalidInput));
}

// ============================================================================
// 4. empty input
// ============================================================================

/// Zero items is not an error — it's a free no-op that returns an empty batch without ever
/// reaching the provider's normal validation/embedding path (contract.rs's own doc: never
/// worth a round trip).
#[tokio::test]
async fn empty_input_is_a_free_no_op() {
    let provider = provider();
    let batch = embed_queries(&provider, 256, &[])
        .await
        .expect("empty input must never error");
    assert!(batch.vectors.is_empty());
    assert_eq!(batch.input_tokens, 0);

    let rerank = provider
        .rerank(TenantId::new(), &query("q"), &[])
        .await
        .expect("empty candidates must never error");
    assert!(rerank.items.is_empty());
}

// ============================================================================
// 5. Unicode
// ============================================================================

/// Multi-byte UTF-8 text must round-trip through the whole pipeline without being mangled or
/// mis-counted — token counting is a Unicode *scalar* count (`chars().count()`), not a UTF-8
/// byte count, so a string that is short in characters but long in bytes must not be rejected
/// by `max_input_tokens` on its byte length.
#[tokio::test]
async fn unicode_text_is_not_mangled_or_byte_miscounted() {
    let provider = provider();
    // 12 Unicode scalars (well under max_input_tokens=32), but 30+ UTF-8 bytes — would wrongly
    // trip a byte-length-based ceiling.
    let text = "北京时间检索系统测试🎉🚀";
    assert!(text.chars().count() <= 32);
    assert!(
        text.len() > 32,
        "fixture must actually be byte-longer than the char ceiling"
    );

    let batch = embed_queries(&provider, 256, &[query(text)])
        .await
        .expect("Unicode text within the char-count ceiling must not be rejected");
    assert_eq!(batch.vectors.len(), 1);
    assert_eq!(batch.input_tokens, text.chars().count() as u64);
}

#[test]
fn sealing_rejects_query_and_card_size_before_egress() {
    let intent = RetrievalIntent::new("a".repeat(4_097), vec![], BTreeSet::new(), BTreeSet::new())
        .expect("schema permits construction so sealing owns its stricter ceiling");
    let profile = humaux_retrieval::request::resolve_registered_retrieval_profile(&BTreeMap::new())
        .expect("registered profile");
    let request = build_request(intent, &profile).expect("request");
    assert_eq!(
        with_scanner(|scanner| scanner.seal_query(&request.trusted_query().expect("text request"))),
        Err(ErrorCode::InvalidInput)
    );

    let title = "b".repeat(16 * 1024 + 1);
    let CardBuildOutcome::Card(oversize_card) = build_card(
        CardInput {
            memory_id: MemoryId::new(),
            memory_type: MemoryType::Fact,
            data_class: DataClass::Private,
            egress_disposition: EgressDisposition::PolicyGated,
            workspace_id: None,
            topic: None,
            effective_from: std::time::SystemTime::UNIX_EPOCH,
            title,
            key_claim: Some("oversize fixture".to_owned()),
            entities: vec![],
            evidence_excerpt: Some("oversize fixture".to_owned()),
        },
        CardBudget {
            max_chars: 16 * 1024 + 4,
        },
    ) else {
        panic!("oversize private card is structurally buildable")
    };
    assert_eq!(
        with_scanner(|scanner| scanner.seal_card(&oversize_card)),
        Err(ErrorCode::InvalidInput)
    );
    assert_eq!(
        with_scanner(|scanner| scanner.scan(b"scanner-fixture-secret")),
        Err(ErrorCode::Forbidden),
        "a scanner finding must fail closed"
    );

    let mut mismatch = test_scanner_config();
    mismatch.expected_version.push_str("-mismatch");
    assert!(matches!(
        LocalSecretScanner::new(mismatch),
        Err(ErrorCode::Conflict)
    ));
}

#[test]
fn sealed_debug_redacts_query_and_card_bodies() {
    let query_text = "私有 query body 🔒";
    let card_text = "私有 card body 🗂️";
    let query_debug = format!("{:?}", query(query_text));
    let card_debug = format!("{:?}", card(card_text));
    assert!(!query_debug.contains(query_text));
    assert!(!card_debug.contains(card_text));
}

// ============================================================================
// 6. max token
// ============================================================================

/// A query longer than `max_input_tokens` (in Unicode scalars) must be rejected.
#[tokio::test]
async fn text_over_max_input_tokens_is_rejected() {
    let provider = provider();
    let too_long: String = "a".repeat(embedding_model().max_input_tokens as usize + 1);
    let result = embed_queries(&provider, 256, &[query(too_long)]).await;
    assert_eq!(result.err(), Some(ErrorCode::InvalidInput));
}

/// Exactly at the ceiling must still succeed — the check is `>`, not `>=`.
#[tokio::test]
async fn text_exactly_at_max_input_tokens_is_accepted() {
    let provider = provider();
    let exact: String = "a".repeat(embedding_model().max_input_tokens as usize);
    let result = embed_queries(&provider, 256, &[query(exact)]).await;
    assert!(result.is_ok());
}

/// §19's own rerank formula: `query_tokens * document_count + sum(document_tokens)` against
/// `max_input_tokens` (rerank_model's is 200) — not `max_docs` alone.
#[tokio::test]
async fn rerank_max_token_uses_the_query_times_docs_plus_sum_formula() {
    let provider = provider();
    let query = query("q".repeat(10)); // 10 tokens
    // Each 10-char title becomes a 13-char canonical card (three separators):
    // 10 * 3 + 13 * 3 = 69, under 200 -> ok.
    let candidates: Vec<SealedRetrievalCard> = (0..3).map(|_| card("c".repeat(10))).collect();
    let ok = provider.rerank(TenantId::new(), &query, &candidates).await;
    assert!(ok.is_ok());

    // Each canonical card keeps three template separators: title 37 + 3 = 40 tokens.
    // 4 candidates x 40 tokens = 160; 10 * 4 + 160 = 200 == ceiling -> ok.
    let at_ceiling: Vec<SealedRetrievalCard> = (0..4).map(|_| card("c".repeat(37))).collect();
    let at_ceiling_result = provider.rerank(TenantId::new(), &query, &at_ceiling).await;
    assert!(at_ceiling_result.is_ok());

    // One more token anywhere pushes it over.
    let over: Vec<SealedRetrievalCard> = (0..4).map(|_| card("c".repeat(38))).collect();
    let over_result = provider.rerank(TenantId::new(), &query, &over).await;
    assert_eq!(over_result.err(), Some(ErrorCode::InvalidInput));
}

// ============================================================================
// 7. rerank ordering
// ============================================================================

/// `RerankBatch::items` must come back sorted descending by score, and `candidate_index` must
/// still point at the *original* position in the caller's `candidates` slice (not the sorted
/// position) so the caller can map a reranked item back to the candidate it came from.
#[tokio::test]
async fn rerank_orders_descending_by_score_and_preserves_original_index() {
    let provider = provider();
    let query = query("alpha beta gamma");
    let candidates = vec![
        card("no overlap at all"), // index 0: low overlap
        card("alpha beta gamma"),  // index 1: full overlap
        card("alpha only"),        // index 2: partial overlap
    ];

    let batch = provider
        .rerank(TenantId::new(), &query, &candidates)
        .await
        .unwrap();

    assert_eq!(batch.items.len(), 3);
    for window in batch.items.windows(2) {
        assert!(
            window[0].score >= window[1].score,
            "items must be sorted descending by score"
        );
    }
    // The highest-overlap candidate (original index 1) must rank first.
    assert_eq!(batch.items[0].candidate_index, 1);
    // Every original index must appear exactly once — reordering, not filtering.
    let mut indices: Vec<usize> = batch.items.iter().map(|i| i.candidate_index).collect();
    indices.sort_unstable();
    assert_eq!(indices, vec![0, 1, 2]);
}

// ============================================================================
// 8. provider error mapping
// ============================================================================

/// `TestDoubleProvider::force_next_error` proves the trait's `Result<_, ErrorCode>` plumbing
/// carries a provider-side failure through unaltered — every `ErrorCode` a real HTTP status
/// mapping could plausibly produce (§19.2: "429：有界退避；401：...；5xx：transient retry").
#[tokio::test]
async fn provider_error_is_mapped_through_unaltered() {
    for code in [
        ErrorCode::ProviderRateLimited, // 429
        ErrorCode::WaitingKey,          // 401 (this task's own §19.2 mapping choice)
        ErrorCode::ProviderTransient,   // 5xx / timeout
        ErrorCode::ProviderPermanent,   // 403 / malformed response
    ] {
        let provider = provider();
        provider.force_next_error(code);
        let result = embed_queries(&provider, 256, &[query("x")]).await;
        assert_eq!(
            result.err(),
            Some(code),
            "embed_queries must surface {code:?} unaltered"
        );
    }

    for code in [ErrorCode::ProviderRateLimited, ErrorCode::ProviderPermanent] {
        let provider = provider();
        provider.force_next_error(code);
        let result = provider
            .rerank(TenantId::new(), &query("q"), &[card("c")])
            .await;
        assert_eq!(
            result.err(),
            Some(code),
            "rerank must surface {code:?} unaltered"
        );
    }
}

/// `force_next_error` is consumed by exactly one call, not sticky — the call after a forced
/// failure must run normally again.
#[tokio::test]
async fn forced_error_does_not_persist_past_one_call() {
    let provider = provider();
    provider.force_next_error(ErrorCode::ProviderTransient);
    let first = embed_queries(&provider, 256, &[query("x")]).await;
    assert_eq!(first.err(), Some(ErrorCode::ProviderTransient));

    let second = embed_queries(&provider, 256, &[query("x")]).await;
    assert!(
        second.is_ok(),
        "the error must not persist past the one call it was forced onto"
    );
}

/// `humaux_retrieval_provider::adapters::map_dashscope_status` (unit-tested in `adapters.rs`
/// itself) is this crate's real HTTP-status half of "provider error mapping" — this
/// integration test only covers the trait-level pass-through half, deliberately, since the
/// real DashScope adapter has no live credential to exercise a real HTTP response against
/// (see `dashscope_live_smoke`).
#[test]
fn provider_error_mapping_has_a_real_http_status_half_too() {
    use humaux_retrieval_provider::adapters::map_dashscope_status;
    assert_eq!(map_dashscope_status(429), ErrorCode::ProviderRateLimited);
}
