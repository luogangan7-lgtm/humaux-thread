//! `retrieval-provider::tests::sealed_type_gate_ui` — §1.2.3/§41.4 compile-time sentinel:
//!   `EmbeddingProvider::embed_queries`'s `&[SealedRetrievalQuery]` parameter rejects a raw `Vec<String>` at compile
//!   time.
//! Depends-on: crates=[trybuild]; services=[];
//!   env=[]; modules=[]
//! Called-by: [cargo-test]
//! Invariants: []
//! Spec: §1.2.3; §41.4
//!
//! Construction belongs only to the
//! canonical pinned scanner path, so this UI gate deliberately has no positive constructor case.
//! `crates/domain/tests/egress_topology_ui.rs` already uses for `EgressPermit`'s own topology.

#[test]
fn sealed_retrieval_query_type_narrowing() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/fail_embed_queries_raw_string.rs");
    t.compile_fail("tests/ui/fail_construct_sealed_query.rs");
    t.compile_fail("tests/ui/fail_construct_query_provenance.rs");
    t.compile_fail("tests/ui/fail_construct_query_wire_binding.rs");
}
