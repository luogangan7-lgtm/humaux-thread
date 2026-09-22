//! §78.2 DB<->Rust contract for the §25.4.A(1)/(2) Mandatory facet projection, plus the
//! §25.4.A(11) registry-dependency contract (card 22b, ADR-0045).
//!
//! Three things are pinned here, each with a different failure mode:
//!
//! 1. **The live generated expression agrees with the Rust mapping** — evaluated by reading
//!    `pg_get_expr` out of the catalog and running THAT text over the 12 labels, so the test
//!    judges the expression the database actually computes with, not a copy of it.
//! 2. **The two closed label sets round-trip** — DB `memory_type` CHECK labels <->
//!    `MemoryType::ALL`, and the `facet` CHECK value domain <-> `MandatoryContextFacet::ALL`.
//!    Set equality with per-item round-trip, never `count == 12` (a swapped pair keeps the
//!    count).
//! 3. **`REGISTRY.required_columns` names real relations and the generated-column contract** —
//!    §25.4.A(11) forbids probing `memory_records.task_id`, and existence alone is not the
//!    facet contract: `pg_attribute.attgenerated` must be `'s'`.
//!
//! The GOLDEN below is a §78.1 registered test constant, written out by hand on purpose
//! (ruling §五.2): it is NOT computed from `humaux_domain::memory::facet_for`, because a test
//! that calls the production mapping cannot notice the production mapping changing. Mutant
//! `Rejection -> NULL` in either half turns this file red.
//!
//! Read-only: this suite creates nothing and deletes nothing. Three-state per §79.2.

use humaux_domain::context::{REGISTRY, SelectorId};
use humaux_domain::memory::{MandatoryContextFacet, MemoryType};
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use std::collections::{BTreeMap, BTreeSet};

const NAME: &str = "facet_contract";

/// §78.1 registered GOLDEN — the 12 (MemoryType, facet) pairs of §25.4.A(2), verbatim.
const GOLDEN: [(&str, Option<&str>); 12] = [
    ("FACT", None),
    ("PREFERENCE", None),
    ("DECISION", Some("decisions")),
    ("REJECTION", Some("decisions")),
    ("STATE", Some("state")),
    ("ISSUE", Some("issues")),
    ("LESSON", None),
    ("CONSTRAINT", Some("constraints")),
    ("PROCEDURE", None),
    ("OUTCOME", None),
    ("REFERENCE", None),
    ("NOTE", None),
];

fn client() -> Option<Client> {
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(NAME, "missing object: Postgres DSN", ExternalDep::Postgres);
        return None;
    };
    let Ok(client) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(NAME, "missing object: live Postgres", ExternalDep::Postgres);
        return None;
    };
    Some(client)
}

/// The quoted literals of a deparsed CHECK, in order.
///
/// PostgreSQL deparses `CHECK (col IN ('A','B'))` as `col = ANY (ARRAY['A'::text, 'B'::text])`,
/// so a contract test that looks for `IN (` alone is a false red. Splitting on the quote
/// character reads both spellings without caring which one the server chose.
fn quoted_literals(def: &str) -> Vec<String> {
    def.split('\'')
        .skip(1)
        .step_by(2)
        .map(str::to_owned)
        .collect()
}

fn constraint_def(client: &mut Client, conname: &str) -> Option<String> {
    let rows = client
        .query(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
             WHERE conrelid = 'private.memory_records'::regclass AND conname = $1",
            &[&conname],
        )
        .expect("constraint lookup");
    rows.first().map(|row| row.get(0))
}

/// The generated column exists as a STORED generated column, its value domain is the closed
/// facet set, and the expression the server computes with agrees with the GOLDEN.
#[test]
fn facet_column_is_stored_generated_and_matches_the_golden() {
    let Some(mut client) = client() else { return };

    let attr = client
        .query(
            "SELECT a.attgenerated::text, a.atttypid = 'text'::regtype, a.attnotnull \
             FROM pg_attribute a \
             WHERE a.attrelid = 'private.memory_records'::regclass \
               AND a.attname = 'facet' AND NOT a.attisdropped",
            &[],
        )
        .expect("facet attribute lookup");
    let Some(attr) = attr.first() else {
        skip_or_fail(
            NAME,
            "missing object: private.memory_records.facet — run `cargo xtask migrate` \
             (migrations/0172_memory_records_mandatory_facet.sql)",
            ExternalDep::Postgres,
        );
        return;
    };
    // §25.4.A(3)/(11): existence is NOT the contract. A writable `facet` would be the second
    // write entry the clause forbids, so the storage kind is asserted, not assumed.
    assert_eq!(
        attr.get::<_, String>(0),
        "s",
        "facet must be GENERATED ALWAYS ... STORED (pg_attribute.attgenerated = 's')"
    );
    assert!(attr.get::<_, bool>(1), "facet must be text");
    assert!(!attr.get::<_, bool>(2), "facet must stay nullable");

    // Value domain == the Rust enum's serialization, set-equal and round-tripped.
    let def = constraint_def(&mut client, "memory_records_facet_v1_check")
        .expect("memory_records_facet_v1_check must exist");
    assert!(
        def.contains("IN (") || def.contains("= ANY (ARRAY["),
        "unexpected CHECK spelling: {def}"
    );
    let db_facets: BTreeSet<String> = quoted_literals(&def).into_iter().collect();
    let rust_facets: BTreeSet<String> = MandatoryContextFacet::ALL
        .iter()
        .map(|facet| facet.wire().to_owned())
        .collect();
    assert_eq!(db_facets, rust_facets, "facet value domain drifted: {def}");
    for label in &db_facets {
        assert!(
            MandatoryContextFacet::parse_wire(label).is_some(),
            "DB facet label {label} does not round-trip into the Rust enum"
        );
    }
}

/// The generation expression the server actually computes with, read out of the catalog and
/// evaluated over the 12 labels — and the stored values it already produced.
#[test]
fn facet_expression_and_stored_values_match_the_golden() {
    let Some(mut client) = client() else { return };
    if constraint_def(&mut client, "memory_records_facet_v1_check").is_none() {
        skip_or_fail(
            NAME,
            "missing object: private.memory_records.facet — run `cargo xtask migrate`",
            ExternalDep::Postgres,
        );
        return;
    }

    // The expression the server computes with, read out of the catalog and evaluated over the
    // 12 labels. If it ever referenced a second column, this query errors — which is the right
    // answer: §25.4.A(2) says facet is a function of `memory_type` alone.
    let expr: String = client
        .query_one(
            "SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d \
               JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum \
              WHERE d.adrelid = 'private.memory_records'::regclass AND a.attname = 'facet'",
            &[],
        )
        .expect("facet generation expression")
        .get(0);
    let values = GOLDEN
        .iter()
        .map(|(label, _)| format!("('{label}'::text)"))
        .collect::<Vec<_>>()
        .join(",");
    let rows = client
        .query(
            &format!(
                "WITH t(memory_type) AS (VALUES {values}) \
                 SELECT memory_type, ({expr}) AS facet FROM t"
            ),
            &[],
        )
        .expect("live facet expression evaluation");
    let computed: BTreeMap<String, Option<String>> = rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, Option<String>>(1)))
        .collect();
    for (label, expected) in GOLDEN {
        assert_eq!(
            computed.get(label).cloned(),
            Some(expected.map(str::to_owned)),
            "live generated expression disagrees with the §25.4.A(2) GOLDEN for {label}"
        );
        // The Rust half of the same pair. Written as a separate assertion so a drift in either
        // side is attributable: the DB may disagree with the golden, or Rust may.
        let rust = MemoryType::parse_wire(label)
            .map(humaux_domain::memory::facet_for)
            .expect("GOLDEN label must be a MemoryType");
        assert_eq!(
            rust.map(MandatoryContextFacet::wire),
            expected,
            "domain::memory::facet_for disagrees with the GOLDEN for {label}"
        );
    }

    // Stored values, not just the expression: every real row already in the table agrees.
    // This is what catches a rewrite that skipped rows (or a later SET EXPRESSION).
    let stored = client
        .query(
            "SELECT DISTINCT memory_type, facet FROM private.memory_records ORDER BY 1",
            &[],
        )
        .expect("stored facet readback");
    let golden: BTreeMap<&str, Option<&str>> = GOLDEN.into_iter().collect();
    for row in &stored {
        let memory_type: String = row.get(0);
        let facet: Option<String> = row.get(1);
        assert_eq!(
            facet.as_deref(),
            *golden
                .get(memory_type.as_str())
                .expect("stored memory_type outside the closed set"),
            "stored facet for {memory_type} disagrees with the GOLDEN"
        );
    }
}

/// §78.2: the DB's `memory_type` label set and the Rust enum are the same set, item by item.
#[test]
fn memory_type_labels_round_trip_between_db_and_rust() {
    let Some(mut client) = client() else { return };
    let Some(def) = constraint_def(&mut client, "memory_records_memory_type_check") else {
        skip_or_fail(
            NAME,
            "missing object: memory_records_memory_type_check",
            ExternalDep::Postgres,
        );
        return;
    };
    assert!(
        def.contains("IN (") || def.contains("= ANY (ARRAY["),
        "unexpected CHECK spelling: {def}"
    );
    let db: BTreeSet<String> = quoted_literals(&def).into_iter().collect();
    let rust: BTreeSet<String> = MemoryType::ALL
        .iter()
        .map(|t| t.wire().to_owned())
        .collect();
    // Set equality, not `len() == 12`: swapping two labels keeps every count identical.
    assert_eq!(db, rust, "memory_type label sets drifted: {def}");
    for label in &db {
        let parsed = MemoryType::parse_wire(label).expect("DB label must parse");
        assert_eq!(parsed.wire(), label, "round-trip changed {label}");
    }
}

/// §25.4.A(11): every REGISTRY dependency names a real relation/column, the task selector
/// reads `private.context_bindings` (never `memory_records.task_id`), and the facet dependency
/// carries the stored-generated contract.
#[test]
fn registry_required_columns_name_real_relations() {
    let Some(mut client) = client() else { return };

    let task = &REGISTRY[0];
    assert_eq!(task.id, SelectorId::TaskExplicitContextV1);
    // card 22c (ADR-0046): v2 reads the obligation from `private.context_bindings` AND its
    // authorization from `private.task_binding_grants`. Those two relations, and no third —
    // the §25.4.A(11) ban below is the half that matters, and a wildcard here would let a
    // future dependency on `memory_records` slip in under a different column name.
    let task_relations: std::collections::BTreeSet<(&str, &str)> = task
        .required_columns
        .iter()
        .map(|(schema, table, _, _)| (*schema, *table))
        .collect();
    assert_eq!(
        task_relations,
        [
            ("private", "context_bindings"),
            ("private", "task_binding_grants")
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<(&str, &str)>>(),
        "§25.4.A(11): the task selector reads the binding obligation and its authorization"
    );
    for column in ["scope_kind", "scope_id", "mode", "revoked_at"] {
        assert!(
            task.required_columns
                .iter()
                .any(|(_, _, name, _)| *name == column),
            "task selector must declare private.context_bindings.{column}"
        );
    }
    assert!(
        REGISTRY
            .iter()
            .all(|spec| !spec
                .required_columns
                .iter()
                .any(|(schema, table, column, _)| {
                    *schema == "private" && *table == "memory_records" && *column == "task_id"
                })),
        "§25.4.A(11) forbids probing private.memory_records.task_id"
    );

    let facets = &REGISTRY[3];
    assert_eq!(facets.id, SelectorId::RequiredCurrentStateFacetsV1);
    assert_eq!(
        facets.required_columns,
        &[("private", "memory_records", "facet", true)],
        "the facets selector must require the STORED generated facet column"
    );

    // Every declared dependency resolves against the live catalog, with its generated contract.
    for spec in &REGISTRY {
        for (schema, table, column, stored_generated) in spec.required_columns {
            let rows = client
                .query(
                    "SELECT a.attgenerated::text FROM pg_attribute a \
                       JOIN pg_class c ON c.oid = a.attrelid \
                       JOIN pg_namespace n ON n.oid = c.relnamespace \
                      WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3 \
                        AND a.attnum > 0 AND NOT a.attisdropped",
                    &[schema, table, column],
                )
                .expect("registry dependency lookup");
            let Some(row) = rows.first() else {
                skip_or_fail(
                    NAME,
                    &format!("missing object: {schema}.{table}.{column}"),
                    ExternalDep::Postgres,
                );
                return;
            };
            if *stored_generated {
                assert_eq!(
                    row.get::<_, String>(0),
                    "s",
                    "{schema}.{table}.{column} must be STORED generated (§25.4.A(11))"
                );
            }
        }
    }
}
