//! `retrieval::planner` — deterministic Retrieval Planner (§20).
//! Depends-on: crates=[]; services=[];
//!   env=[]; modules=[retrieval::predicate_registry]
//! Called-by: [gateway::recall, retrieval::completeness, retrieval::envelope, retrieval::predicate_eval, retrieval::request, tests]
//! Invariants: []
//! Spec: §20; §20.0; §20.2; §20.1; §22.5
//!
//! §20.0: online recall/context/continuity must never implicitly call an LLM/VLM
//! (USER_REASONING · PLATFORM_PUBLIC · private consolidation · HyDE/generative rewrite ·
//! session-turn summarizer all sit on the MUST-NOT list; default profile is
//! `query_transform = deterministic`). This module has no HTTP/provider-SDK dependency at
//! all — [`decide`] and everything it calls are pure functions over `&str` and pre-fetched
//! [`PredicateEntry`]/index-catalog inputs, so it structurally cannot join the online call
//! graph G20-2/G80-39 scans for (a live-query variant of that architecture-check is a later
//! wiring task's job; this module cannot fail it because it makes zero calls of any kind).
//!
//! §20.2's 5-step short-circuit sequence is [`decide`]'s body, in order:
//! 1. an explicit `memory_id`/sha256 locator in the query text ⇒ [`PlannerDecision::DirectGet`];
//! 2. a `surface_patterns` hit **and** quantifier word present ⇒ a candidate predicate;
//! 3. the candidate's `required_columns` all indexed **and** its `enumerable_scope` confirmed
//!    enumerable in the current workspace ⇒ [`PlannerDecision::Enumerate`] (§20.1's "EXACT"
//!    completeness eligibility — see the module-level adjudication note below for why this
//!    crate spells that outcome `Enumerate` rather than a bare `Exact` variant);
//! 4. step 3 fails ⇒ [`PlannerDecision::CannotEstablish`] — **never** downgraded to
//!    `Class(Semantic)` (§20.2/§22.5 frozen: "不降到 SEMANTIC，直接 CANNOT_ESTABLISH");
//! 5. no candidate at all ⇒ [`PlannerDecision::Class`], routed by the deterministic keyword
//!    metadata rules in [`route_by_metadata`].
//!
//! Spec adjudication (recorded here, not silently guessed): §20's opening fence names 9
//! classes — `DIRECT_GET/LITERAL/ENUMERATE/STATE/SEMANTIC/TEMPORAL/ASSOCIATION/CODE/
//! CONTINUITY` — as one flat closed set, but §20.2's own rule 5 only names 6 of them
//! (`SEMANTIC/TEMPORAL/ASSOCIATION/CODE/STATE/CONTINUITY`) as the no-candidate fallback
//! targets, explicitly excluding `DIRECT_GET` and `ENUMERATE` (those two are only reachable
//! via rules 1 and 3, never rule 5) and saying nothing about where `LITERAL` is routed. This
//! module keeps [`QueryClass`] as the literal 9-member wire vocabulary (so a caller logging
//! `wire_class()` always emits one of the 9 spec names), but the Rust decision type
//! ([`PlannerDecision`]) gives `DirectGet` and `Enumerate` their own variants with the extra
//! payload rule 1/3 each produce (a locator, a `predicate_id`) that a bare `Class(_)` cannot
//! carry — `Class(_)` is therefore only ever constructed with one of the 7 remaining members
//! (`Literal` folded into rule 5's routing alongside the 6 named there, on the reading that
//! §20.2's list is "the classes rule 5 was written to cover", not "the total set anything can
//! route through": leaving `Literal` structurally unreachable would make it dead code in a
//! 9-member enum this module is supposed to classify into).

use crate::predicate_registry::PredicateEntry;
use std::collections::BTreeSet;

/// §20 opening fence — 9-member closed set, wire vocabulary in `as_wire_name`'s literal
/// spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryClass {
    DirectGet,
    Literal,
    Enumerate,
    State,
    Semantic,
    Temporal,
    Association,
    Code,
    Continuity,
}

impl QueryClass {
    /// §20 opening fence's literal `SCREAMING_SNAKE_CASE` spelling (wire/telemetry name).
    pub fn as_wire_name(self) -> &'static str {
        match self {
            QueryClass::DirectGet => "DIRECT_GET",
            QueryClass::Literal => "LITERAL",
            QueryClass::Enumerate => "ENUMERATE",
            QueryClass::State => "STATE",
            QueryClass::Semantic => "SEMANTIC",
            QueryClass::Temporal => "TEMPORAL",
            QueryClass::Association => "ASSOCIATION",
            QueryClass::Code => "CODE",
            QueryClass::Continuity => "CONTINUITY",
        }
    }
}

/// §20.2 rule 1 locator kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectGetLocator {
    MemoryId(String),
    Sha256(String),
}

/// Planner output (§20.2). See the module-level adjudication note for why `DirectGet` /
/// `Enumerate` are their own variants rather than `Class(QueryClass::DirectGet)` /
/// `Class(QueryClass::Enumerate)`, and why `CannotEstablish` carries no `QueryClass` at all
/// (rule 4: it is a completeness-eligibility dead end, not one of the 9 recall-lane classes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerDecision {
    DirectGet(DirectGetLocator),
    Enumerate { predicate_id: String },
    CannotEstablish,
    Class(QueryClass),
}

impl PlannerDecision {
    /// This decision's §20 wire class, or `None` for `CannotEstablish` (rule 4's outcome is
    /// explicitly not one of the 9 — see module-level note).
    pub fn wire_class(&self) -> Option<QueryClass> {
        match self {
            PlannerDecision::DirectGet(_) => Some(QueryClass::DirectGet),
            PlannerDecision::Enumerate { .. } => Some(QueryClass::Enumerate),
            PlannerDecision::CannotEstablish => None,
            PlannerDecision::Class(c) => Some(*c),
        }
    }

    /// §20.3 confusion-matrix scoring key: the `predicate_id` this decision established, or
    /// `None` for every variant except `Enumerate` — `DirectGet`/`CannotEstablish`/`Class(_)`
    /// all score as "no predicate" against an eval row's expected `predicate_id`.
    pub fn predicate_id(&self) -> Option<&str> {
        match self {
            PlannerDecision::Enumerate { predicate_id } => Some(predicate_id.as_str()),
            _ => None,
        }
    }
}

/// §20.2 rule 2's frozen quantifier word-face list. Literal — a paraphrase that doesn't use
/// one of these words is not a quantifier hit, by design ("量词词面存在是 EXACT 硬条件，宁可
/// 漏判").
const QUANTIFIERS: &[&str] = &["所有", "全部", "每一个", "哪些", "all", "every", "list"];

fn has_quantifier(query_lower: &str) -> bool {
    QUANTIFIERS.iter().any(|w| query_lower.contains(w))
}

fn is_ascii_hex(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// UUID-shaped token (8-4-4-4-12 hex groups) — the `memory_id` half of §20.2 rule 1.
fn looks_like_memory_id(tok: &str) -> bool {
    let parts: Vec<&str> = tok.split('-').collect();
    parts.len() == 5
        && [8usize, 4, 4, 4, 12]
            .iter()
            .zip(parts.iter())
            .all(|(&len, p)| p.len() == len && is_ascii_hex(p))
}

/// 64 hex chars — the sha256 half of §20.2 rule 1.
fn looks_like_sha256(tok: &str) -> bool {
    tok.len() == 64 && is_ascii_hex(tok)
}

fn find_direct_get_locator(query: &str) -> Option<DirectGetLocator> {
    for raw in query.split_whitespace() {
        let tok = raw.trim_matches(|c: char| !(c.is_ascii_alphanumeric() || c == '-'));
        if looks_like_memory_id(tok) {
            return Some(DirectGetLocator::MemoryId(tok.to_string()));
        }
        if looks_like_sha256(tok) {
            return Some(DirectGetLocator::Sha256(tok.to_string()));
        }
    }
    None
}

/// §20.2 rule 2: a candidate exists only when a quantifier word is present **and** some
/// registry entry's `surface_patterns` substring-matches the query. Both conditions are a
/// hard AND — dropping either turns this into the exact over-match §20.2's closing sentence
/// forbids (see `planner_predicate` eval negative examples for the two ways to break it).
fn find_candidate<'a>(
    query_lower: &str,
    registry: &'a [PredicateEntry],
) -> Option<&'a PredicateEntry> {
    if !has_quantifier(query_lower) {
        return None;
    }
    registry.iter().find(|p| {
        p.surface_patterns()
            .iter()
            .any(|pat| query_lower.contains(&pat.to_lowercase()))
    })
}

/// §20.2 rule 3, both conjuncts: every `required_columns` entry must be present in the live
/// index catalog **and** the candidate's `enumerable_scope` must be confirmed enumerable in the
/// current workspace ("required_columns 全部有索引，且 enumerable_scope 在当前 workspace 内可
/// 枚举"). `indexed_columns`/`enumerable_scopes` are both caller-supplied (cheap,
/// infrequently-refreshed snapshots — pg_index/information_schema derived, not a per-query
/// round trip) — this function itself stays IO-free like the rest of the module. Dropping
/// either conjunct reopens the exact false-EXACT gap §20.1/§22.1 exist to close: a scope that
/// cannot actually be enumerated in this workspace would still report `total` from
/// `SELECT count(*) FROM <enumerable_scope> AND <sql_predicate>` landing on a face that isn't
/// real, with coverage still claiming 1.0.
fn is_established(
    candidate: &PredicateEntry,
    indexed_columns: &BTreeSet<String>,
    enumerable_scopes: &BTreeSet<String>,
) -> bool {
    candidate
        .required_columns()
        .iter()
        .all(|c| indexed_columns.contains(c))
        && enumerable_scopes.contains(candidate.enumerable_scope())
}

// ponytail: keyword-list heuristic, not an NLP classifier — ceiling is it misses any
// paraphrase outside these fixed marker lists (e.g. "how's it going" won't hit `State`).
// Upgrade path: replace with a per-class §50 typed config metadata rule set once product
// defines one; §20 itself calls this "第一版 Planner", so a fixed rule table is the deliberate
// starting shape, not an oversight.
const CODE_MARKERS: &[&str] = &[
    "fn ",
    "function ",
    "class ",
    "()",
    "::",
    ".rs",
    ".py",
    ".ts",
    "代码",
    "函数",
    "symbol",
];
const TEMPORAL_MARKERS: &[&str] = &[
    "昨天",
    "上周",
    "上个月",
    "最近",
    "yesterday",
    "last week",
    "last month",
    "recently",
];
const ASSOCIATION_MARKERS: &[&str] = &[
    "关联",
    "相关",
    "关系",
    "related to",
    "connected to",
    "linked to",
];
const STATE_MARKERS: &[&str] = &[
    "现在",
    "目前",
    "进度",
    "状态",
    "current status",
    "current state",
    "how is it going",
];
const CONTINUITY_MARKERS: &[&str] = &[
    "接着",
    "继续",
    "上次聊到",
    "continue from",
    "pick up where",
    "last session",
];

/// §20.2 rule 5 fallback routing for the no-candidate path (§21.5 also names `STATE` as
/// needing its own freshness handling downstream of this classification, not decided here).
fn route_by_metadata(query: &str, query_lower: &str) -> QueryClass {
    if query.contains('"') || query.contains('\u{201c}') || query.contains('\u{201d}') {
        return QueryClass::Literal;
    }
    if CODE_MARKERS.iter().any(|m| query_lower.contains(m)) {
        return QueryClass::Code;
    }
    if TEMPORAL_MARKERS.iter().any(|m| query_lower.contains(m)) {
        return QueryClass::Temporal;
    }
    if ASSOCIATION_MARKERS.iter().any(|m| query_lower.contains(m)) {
        return QueryClass::Association;
    }
    if STATE_MARKERS.iter().any(|m| query_lower.contains(m)) {
        return QueryClass::State;
    }
    if CONTINUITY_MARKERS.iter().any(|m| query_lower.contains(m)) {
        return QueryClass::Continuity;
    }
    QueryClass::Semantic
}

/// §20.2's full 5-step short-circuit decision. Pure/deterministic (§20.0): no IO, no
/// randomness, same inputs always produce the same [`PlannerDecision`] — this is the property
/// the `planner_predicate` eval's `spread_tol=0` measurement (§55.3) relies on.
pub fn decide(
    query: &str,
    registry: &[PredicateEntry],
    indexed_columns: &BTreeSet<String>,
    enumerable_scopes: &BTreeSet<String>,
) -> PlannerDecision {
    if let Some(locator) = find_direct_get_locator(query) {
        return PlannerDecision::DirectGet(locator);
    }
    let query_lower = query.to_lowercase();
    if let Some(candidate) = find_candidate(&query_lower, registry) {
        return if is_established(candidate, indexed_columns, enumerable_scopes) {
            PlannerDecision::Enumerate {
                predicate_id: candidate.predicate_id().to_string(),
            }
        } else {
            PlannerDecision::CannotEstablish
        };
    }
    PlannerDecision::Class(route_by_metadata(query, &query_lower))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate_registry::{PredicateRow, load_registry};

    /// The real scope string, post migration 0079's tenant-filter fix (`enumerable_scope`
    /// column value; see that migration's header note).
    const SCOPE: &str =
        // dep-map: allow table-undeclared — predicate scope metadata; SQL is executed by adapters::exact_census
        "private.memory_records WHERE tenant_id = $1 AND visibility_workspace_id = $2";

    /// Built through [`load_registry`] rather than a `PredicateEntry { .. }` struct literal —
    /// `PredicateEntry`'s fields are private precisely so a fixture like this one cannot
    /// construct an "already validated" value without actually going through validation (§50
    /// fail-loud; see `predicate_registry.rs`'s type doc).
    fn rejected_decisions_predicate() -> PredicateEntry {
        let row = PredicateRow {
            predicate_id: "rejected_decisions_v1".to_string(),
            sql_predicate: "memory_type='REJECTION' AND superseded_at IS NULL".to_string(),
            required_columns: vec![
                "memory_type".to_string(),
                "superseded_at".to_string(),
                "visibility_workspace_id".to_string(),
            ],
            enumerable_scope: SCOPE.to_string(),
            surface_patterns: vec![
                "所有被否决".to_string(),
                "否掉过哪些".to_string(),
                "all rejected".to_string(),
            ],
            owner_module: "retrieval::planner".to_string(),
        };
        load_registry(vec![row]).expect("fixture row must pass §50 validation")[0].clone()
    }

    fn fully_indexed() -> BTreeSet<String> {
        ["memory_type", "superseded_at", "visibility_workspace_id"]
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    /// §20.2 rule 3's second conjunct input: scopes confirmed enumerable in the current
    /// workspace. Contains exactly `SCOPE` — the positive-path fixture.
    fn established_scopes() -> BTreeSet<String> {
        BTreeSet::from([SCOPE.to_string()])
    }

    #[test]
    fn query_class_wire_names_cover_all_9_variants() {
        fn assert_exhaustive(c: QueryClass) -> &'static str {
            match c {
                QueryClass::DirectGet => c.as_wire_name(),
                QueryClass::Literal => c.as_wire_name(),
                QueryClass::Enumerate => c.as_wire_name(),
                QueryClass::State => c.as_wire_name(),
                QueryClass::Semantic => c.as_wire_name(),
                QueryClass::Temporal => c.as_wire_name(),
                QueryClass::Association => c.as_wire_name(),
                QueryClass::Code => c.as_wire_name(),
                QueryClass::Continuity => c.as_wire_name(),
            }
        }
        // Table-driven over all 9 variants (previously only 2 of 9 were pinned) — each of the
        // 9 spec-literal wire names asserted verbatim so no single variant can drift silently.
        let cases = [
            (QueryClass::DirectGet, "DIRECT_GET"),
            (QueryClass::Literal, "LITERAL"),
            (QueryClass::Enumerate, "ENUMERATE"),
            (QueryClass::State, "STATE"),
            (QueryClass::Semantic, "SEMANTIC"),
            (QueryClass::Temporal, "TEMPORAL"),
            (QueryClass::Association, "ASSOCIATION"),
            (QueryClass::Code, "CODE"),
            (QueryClass::Continuity, "CONTINUITY"),
        ];
        for (class, wire_name) in cases {
            assert_eq!(assert_exhaustive(class), wire_name);
        }
    }

    #[test]
    fn rule1_memory_id_short_circuits_to_direct_get() {
        let decision = decide(
            "帮我看看 memory_id f47ac10b-58cc-4372-a567-0e02b2c3d479 的详情",
            &[],
            &BTreeSet::new(),
            &BTreeSet::new(),
        );
        assert_eq!(decision.wire_class(), Some(QueryClass::DirectGet));
        assert!(matches!(
            decision,
            PlannerDecision::DirectGet(DirectGetLocator::MemoryId(_))
        ));
    }

    #[test]
    fn rule1_sha256_short_circuits_to_direct_get() {
        let sha = "72c49ad07889405f8b9297966a52ba6ea2cd773d94d4816a6f5410e008dfb5a8"; // 64 hex
        let decision = decide(
            &format!("locate evidence {sha}"),
            &[],
            &BTreeSet::new(),
            &BTreeSet::new(),
        );
        assert!(matches!(
            decision,
            PlannerDecision::DirectGet(DirectGetLocator::Sha256(_))
        ));
    }

    #[test]
    fn rule3_established_candidate_is_enumerate_exact() {
        let registry = vec![rejected_decisions_predicate()];
        let decision = decide(
            "所有被否决的决策都列出来",
            &registry,
            &fully_indexed(),
            &established_scopes(),
        );
        assert_eq!(
            decision,
            PlannerDecision::Enumerate {
                predicate_id: "rejected_decisions_v1".to_string()
            }
        );
        assert_eq!(decision.predicate_id(), Some("rejected_decisions_v1"));
        assert_eq!(decision.wire_class(), Some(QueryClass::Enumerate));
    }

    /// §20.2 rule 4, frozen: an unindexed required_columns entry must land on
    /// `CannotEstablish`, never a silent downgrade to `Class(Semantic)`.
    #[test]
    fn rule4_unindexed_required_column_is_cannot_establish_not_semantic() {
        let registry = vec![rejected_decisions_predicate()];
        let mut partial = fully_indexed();
        partial.remove("superseded_at");
        let decision = decide(
            "所有被否决的决策都列出来",
            &registry,
            &partial,
            &established_scopes(),
        );
        assert_eq!(decision, PlannerDecision::CannotEstablish);
        assert_eq!(decision.wire_class(), None);
        assert_eq!(decision.predicate_id(), None);
    }

    /// §20.2 rule 3's second conjunct, frozen: `required_columns` all indexed is not
    /// sufficient on its own — an `enumerable_scope` not confirmed enumerable in the current
    /// workspace must also land on `CannotEstablish`. Reverse-direction pin of
    /// `rule4_unindexed_required_column_is_cannot_establish_not_semantic` above: this one
    /// holds `indexed_columns` fully satisfied and instead empties `enumerable_scopes`, so it
    /// fails only if `is_established` drops the scope conjunct (e.g. reverts to only checking
    /// `required_columns`, which is exactly the gap this rule exists to close).
    #[test]
    fn rule3_columns_indexed_but_scope_not_enumerable_is_cannot_establish() {
        let registry = vec![rejected_decisions_predicate()];
        let decision = decide(
            "所有被否决的决策都列出来",
            &registry,
            &fully_indexed(),
            &BTreeSet::new(), // scope not confirmed enumerable in this workspace
        );
        assert_eq!(decision, PlannerDecision::CannotEstablish);
        assert_eq!(decision.wire_class(), None);
        assert_eq!(decision.predicate_id(), None);
    }

    /// §20.2 rule 2, frozen half A: a surface-pattern hit with **no** quantifier word must
    /// never produce a candidate (falls through to rule 5, not EXACT). Pins the AND against
    /// an accidental OR mutation.
    #[test]
    fn rule2_surface_match_without_quantifier_never_reaches_exact() {
        let registry = vec![rejected_decisions_predicate()];
        let decision = decide(
            "为什么这个方案被否决了",
            &registry,
            &fully_indexed(),
            &established_scopes(),
        );
        assert_eq!(decision.predicate_id(), None);
        assert!(!matches!(decision, PlannerDecision::Enumerate { .. }));
        assert!(!matches!(decision, PlannerDecision::CannotEstablish));
    }

    /// §20.2 rule 2, frozen half B: a quantifier word with **no** surface-pattern hit must
    /// also never produce a candidate.
    #[test]
    fn rule2_quantifier_without_surface_match_never_reaches_exact() {
        let registry = vec![rejected_decisions_predicate()];
        let decision = decide(
            "每一个 API 端点都有测试吗",
            &registry,
            &fully_indexed(),
            &established_scopes(),
        );
        assert_eq!(decision.predicate_id(), None);
        assert!(!matches!(decision, PlannerDecision::Enumerate { .. }));
        assert!(!matches!(decision, PlannerDecision::CannotEstablish));
    }

    #[test]
    fn empty_registry_never_reaches_exact_or_cannot_establish() {
        let decision = decide(
            "所有被否决的决策都列出来",
            &[],
            &BTreeSet::new(),
            &BTreeSet::new(),
        );
        assert!(matches!(decision, PlannerDecision::Class(_)));
    }

    #[test]
    fn decide_is_deterministic_across_repeated_calls() {
        let registry = vec![rejected_decisions_predicate()];
        let catalog = fully_indexed();
        let scopes = established_scopes();
        let first = decide("所有被否决的决策都列出来", &registry, &catalog, &scopes);
        for _ in 0..2 {
            assert_eq!(
                decide("所有被否决的决策都列出来", &registry, &catalog, &scopes),
                first
            );
        }
    }

    /// §20.2 rule 5, one assertion per class this module's own test suite never pinned before
    /// (only `DirectGet`/`Enumerate` were checked via `wire_class()` elsewhere): swapping
    /// `CODE_MARKERS`/`TEMPORAL_MARKERS`, deleting a branch, or flipping `Literal`'s quote
    /// check would previously pass every test in this file. `Literal` gets its own case (the
    /// quote-detection branch runs before the marker-list branches in `route_by_metadata`).
    #[test]
    fn rule5_each_no_candidate_class_is_reachable_and_correctly_routed() {
        let cases: &[(&str, QueryClass)] = &[
            (
                "查找包含 \"deterministic planner\" 这句话的笔记",
                QueryClass::Literal,
            ),
            ("搜索一下代码里 payload_sha256 这个符号", QueryClass::Code),
            ("上周我们讨论过的部署方案是什么", QueryClass::Temporal),
            ("这个功能和登录模块有什么关联", QueryClass::Association),
            ("现在项目的进度状态是什么", QueryClass::State),
            ("接着上次聊到的地方继续", QueryClass::Continuity),
            ("帮我总结一下这个项目的技术栈", QueryClass::Semantic),
        ];
        for (query, expected) in cases {
            let decision = decide(query, &[], &BTreeSet::new(), &BTreeSet::new());
            assert_eq!(
                decision,
                PlannerDecision::Class(*expected),
                "query {query:?} expected {expected:?}, got {decision:?}"
            );
        }
    }

    /// ADR-0055 D-C: the card-30 everyday queries keep their planner class — recall records it
    /// (`provenance.planner_class`) and answers with dense + `LANE_SUBSTITUTED` instead of
    /// refusing, so the class itself must stay stable for that record to mean anything.
    #[test]
    fn everyday_queries_keep_their_planner_class() {
        let cases: &[(&str, QueryClass)] = &[
            ("目前项目进度", QueryClass::State),
            ("客户张三最近的情绪怎么样", QueryClass::Temporal),
            ("和支付相关的决定", QueryClass::Association),
            ("the \"frozen contract\" decision", QueryClass::Literal),
            (
                "0190f7a8-0000-7000-8000-000000000001",
                QueryClass::DirectGet,
            ),
        ];
        for (query, expected) in cases {
            let decision = decide(query, &[], &BTreeSet::new(), &BTreeSet::new());
            assert_eq!(
                decision.wire_class(),
                Some(*expected),
                "query {query:?} expected {expected:?}, got {decision:?}"
            );
        }
    }
}
