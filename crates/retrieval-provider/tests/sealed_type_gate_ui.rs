//! §1.2.3/§41.4 compile-time sentinel: `EmbeddingProvider::embed_queries`'s `&[SealedRetrievalQuery]`
//! parameter rejects a raw `Vec<String>` at compile time, and accepts the sanctioned
//! `SealedRetrievalQuery::seal(..)` path fine — same `trybuild` pattern
//! `crates/domain/tests/egress_topology_ui.rs` already uses for `EgressPermit`'s own topology.

#[test]
fn sealed_retrieval_query_type_narrowing() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_seal_then_embed_queries.rs");
    t.compile_fail("tests/ui/fail_embed_queries_raw_string.rs");
}
