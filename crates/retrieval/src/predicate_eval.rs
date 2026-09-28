//! `retrieval::predicate_eval` — §20.3 confusion-matrix calculator for the `planner_predicate` eval set
//!   (`evals/planner_predicate/dataset.tsv`).
//! Depends-on: crates=[]; services=[];
//!   env=[]; modules=[retrieval::planner, retrieval::predicate_registry]
//! Called-by: [tests]
//! Invariants: []
//! Spec: §20.3
//!
//! §20.3 freezes the judgement as asymmetric:
//! - **漏判 (miss)**: expected a `predicate_id`, [`crate::planner::decide`] returned none —
//!   tolerated up to 15% of the fixed denominator.
//! - **误判 (wrong)**: the decided `predicate_id` differs from expected in any other way,
//!   including a false positive (expected none, got one) — must be exactly 0.

use crate::planner::decide;
use crate::predicate_registry::PredicateEntry;
use std::collections::BTreeSet;

/// One `(question, expected predicate_id | null)` row (§20.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvalCase {
    pub query: String,
    pub expected_predicate_id: Option<String>,
}

/// §20.3 confusion-matrix tally over a fixed-denominator eval set (§55.3: `fixed_denominator`
/// = question count = `total`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConfusionMatrix {
    pub total: usize,
    pub correct: usize,
    pub miss: usize,
    pub wrong: usize,
}

impl ConfusionMatrix {
    /// §20.3: 漏判上限 15%（`miss / total`）。
    pub fn miss_rate(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        self.miss as f64 / self.total as f64
    }
}

/// Run every case through [`decide`] and tally the §20.3 confusion matrix.
pub fn run(
    cases: &[EvalCase],
    registry: &[PredicateEntry],
    indexed_columns: &BTreeSet<String>,
    enumerable_scopes: &BTreeSet<String>,
) -> ConfusionMatrix {
    let mut m = ConfusionMatrix {
        total: cases.len(),
        ..Default::default()
    };
    for case in cases {
        let decision = decide(&case.query, registry, indexed_columns, enumerable_scopes);
        let actual = decision.predicate_id();
        match (actual, case.expected_predicate_id.as_deref()) {
            (None, None) => m.correct += 1,
            (Some(a), Some(e)) if a == e => m.correct += 1,
            (None, Some(_)) => m.miss += 1,
            _ => m.wrong += 1,
        }
    }
    m
}

/// One `evals/planner_predicate/dataset.tsv` data row: `query \t expected \t negative_for`.
/// `negative_for` is not consumed by [`run`] — it only records, per §20.3's closing sentence
/// ("注册表新增一行必须同时新增 ≥5 条问法，其中 ≥2 条是近义但不该命中的负例"), which
/// `predicate_id` a blank-expected row exists to guard against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetRow {
    pub case: EvalCase,
    pub negative_for: Option<String>,
}

/// Parse `evals/planner_predicate/dataset.tsv`. Blank lines and lines starting with `#` are
/// skipped (header/comment rows); every other line must have exactly 3 tab-separated fields.
pub fn parse_dataset_tsv(raw: &str) -> Vec<DatasetRow> {
    raw.lines()
        .map(str::trim_end)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| {
            let mut cols = l.split('\t');
            let query = cols.next().unwrap_or_default().to_string();
            let expected = cols.next().unwrap_or_default().trim();
            let negative_for = cols.next().unwrap_or_default().trim();
            DatasetRow {
                case: EvalCase {
                    query,
                    expected_predicate_id: (!expected.is_empty()).then(|| expected.to_string()),
                },
                negative_for: (!negative_for.is_empty()).then(|| negative_for.to_string()),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate_registry::{PredicateEntry, PredicateRow, load_registry};

    const SCOPE: &str =
        // dep-map: allow table-undeclared — predicate scope metadata; SQL is executed by adapters::exact_census
        "private.memory_records WHERE tenant_id = $1 AND visibility_workspace_id = $2";

    /// Built through [`load_registry`] rather than a `PredicateEntry { .. }` struct literal —
    /// see `predicate_registry.rs`'s type doc for why its fields are private.
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

    fn established_scopes() -> BTreeSet<String> {
        BTreeSet::from([SCOPE.to_string()])
    }

    #[test]
    fn parse_dataset_tsv_skips_comments_and_blanks() {
        let raw = "# header\n\nq1\texpected\tneg\nq2\t\t\n";
        let rows = parse_dataset_tsv(raw);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].case.expected_predicate_id.as_deref(),
            Some("expected")
        );
        assert_eq!(rows[0].negative_for.as_deref(), Some("neg"));
        assert_eq!(rows[1].case.expected_predicate_id, None);
        assert_eq!(rows[1].negative_for, None);
    }

    #[test]
    fn run_scores_correct_miss_and_wrong() {
        let registry = vec![rejected_decisions_predicate()];
        let catalog = fully_indexed();
        let scopes = established_scopes();
        let cases = vec![
            // correct: expected match.
            EvalCase {
                query: "所有被否决的决策都列出来".to_string(),
                expected_predicate_id: Some("rejected_decisions_v1".to_string()),
            },
            // correct: both agree "no predicate".
            EvalCase {
                query: "帮我总结一下这个项目的技术栈".to_string(),
                expected_predicate_id: None,
            },
            // miss: expected a predicate, quantifier present but no surface match ⇒ none.
            EvalCase {
                query: "每一个 API 端点都有测试吗".to_string(),
                expected_predicate_id: Some("rejected_decisions_v1".to_string()),
            },
            // wrong (false positive): expected none, but this row would actually match.
            EvalCase {
                query: "所有被否决的决策都列出来".to_string(),
                expected_predicate_id: None,
            },
        ];
        let m = run(&cases, &registry, &catalog, &scopes);
        assert_eq!(m.total, 4);
        assert_eq!(m.correct, 2);
        assert_eq!(m.miss, 1);
        assert_eq!(m.wrong, 1);
        assert!((m.miss_rate() - 0.25).abs() < f64::EPSILON);
    }
}
