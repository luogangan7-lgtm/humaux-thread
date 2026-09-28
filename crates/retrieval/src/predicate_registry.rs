//! `retrieval::predicate_registry` — §20.1 Predicate Registry types + fail-loud loader (§50).
//! Depends-on: crates=[]; services=[];
//!   env=[]; modules=[]
//! Called-by: [adapters::exact_census, retrieval::planner, retrieval::predicate_eval, retrieval::request, tests]
//! Invariants: []
//! Spec: §20.1; §50; §48
//!
//! Storage: `control.retrieval_predicates` (migrations 0077/0078; see 0077's header note for
//! why this deviates from §20.1's literal dotted example `retrieval.predicates` — no
//! `retrieval` schema exists in this workspace's canonical §48 seven-schema set). This module
//! only turns already-fetched rows into validated [`PredicateEntry`] values — it does not
//! import sqlx or issue any query itself, matching `contracts::feature_registry`'s split
//! between parsing/validation (here) and IO (a later adapters-crate task).
//!
//! §20.1: "谓词注册表... 属 §50 typed config registry，垃圾值 fail-loud" — [`load_registry`]
//! is the one place that enforces it: any row missing a required field, or any duplicate
//! `predicate_id`, is rejected with `Err`, never silently dropped or defaulted.

use std::collections::BTreeSet;
use std::fmt;

/// One `control.retrieval_predicates` row, already fetched from the DB — mirrors §20.1's
/// field table column-for-column (plus nothing else; `tenant_id`/`created_at` are storage
/// bookkeeping the planner never reads, so they are not part of this type).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredicateRow {
    pub predicate_id: String,
    pub sql_predicate: String,
    pub required_columns: Vec<String>,
    pub enumerable_scope: String,
    pub surface_patterns: Vec<String>,
    pub owner_module: String,
}

/// A [`PredicateRow`] that has passed [`load_registry`]'s fail-loud validation — the only
/// shape [`crate::planner::decide`] accepts as candidate input.
///
/// Fields are private (accessors below): [`PredicateRow`]'s fields are all `pub` because it is
/// the raw, not-yet-validated shape a caller is expected to construct directly (from a DB row,
/// a fixture, a future TOML mirror). `PredicateEntry` claims to already have passed
/// [`load_registry`]'s checks — if its fields were also `pub`, any caller outside this module
/// could construct one with a struct literal and skip validation entirely, which is exactly the
/// "already validated" guarantee this type exists to make (§50 fail-loud).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredicateEntry {
    predicate_id: String,
    sql_predicate: String,
    required_columns: Vec<String>,
    enumerable_scope: String,
    surface_patterns: Vec<String>,
    owner_module: String,
}

impl PredicateEntry {
    pub fn predicate_id(&self) -> &str {
        &self.predicate_id
    }
    pub fn sql_predicate(&self) -> &str {
        &self.sql_predicate
    }
    pub fn required_columns(&self) -> &[String] {
        &self.required_columns
    }
    pub fn enumerable_scope(&self) -> &str {
        &self.enumerable_scope
    }
    pub fn surface_patterns(&self) -> &[String] {
        &self.surface_patterns
    }
    pub fn owner_module(&self) -> &str {
        &self.owner_module
    }
}

/// §50 fail-loud parse/validation failure — a build-time/load-time local error, not the §52
/// `ErrorCode`/`DegradeCode` runtime pair (same carve-out `contracts::feature_registry`'s
/// `FeatureRegistryError` documents).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredicateRegistryError(pub String);

impl fmt::Display for PredicateRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for PredicateRegistryError {}

/// Validate and load a batch of raw rows (§50 fail-loud, §20.1 field table).
///
/// Rejects (each with the offending `predicate_id` named in the error, `total_rows_scanned`
/// discipline the same 0077 CHECK constraints already enforce at the DB layer, restated here
/// so a caller that bypasses SQL — a fixture, a future TOML mirror — cannot smuggle a garbage
/// row past this crate):
/// - blank `predicate_id`, or a `predicate_id` repeated across rows;
/// - blank `sql_predicate` / `enumerable_scope` / `owner_module`;
/// - empty `required_columns` — §20.2 rule 3 can never be evaluated against zero columns;
/// - empty `surface_patterns`, or any blank entry in it — §20.2 rule 2 can never match.
pub fn load_registry(
    rows: Vec<PredicateRow>,
) -> Result<Vec<PredicateEntry>, PredicateRegistryError> {
    let mut seen_ids: BTreeSet<String> = BTreeSet::new();
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if row.predicate_id.trim().is_empty() {
            return Err(PredicateRegistryError(
                "predicate_id must not be blank (§20.1 field table)".to_string(),
            ));
        }
        if !seen_ids.insert(row.predicate_id.clone()) {
            return Err(PredicateRegistryError(format!(
                "{}: duplicate predicate_id (§50 fail-loud)",
                row.predicate_id
            )));
        }
        if row.sql_predicate.trim().is_empty() {
            return Err(PredicateRegistryError(format!(
                "{}: sql_predicate must not be blank",
                row.predicate_id
            )));
        }
        if row.required_columns.is_empty() {
            return Err(PredicateRegistryError(format!(
                "{}: required_columns must not be empty (§20.2 rule 3 has nothing to check)",
                row.predicate_id
            )));
        }
        if row.enumerable_scope.trim().is_empty() {
            return Err(PredicateRegistryError(format!(
                "{}: enumerable_scope must not be blank (§20.1 — no scope means no real denominator)",
                row.predicate_id
            )));
        }
        if row.surface_patterns.is_empty()
            || row.surface_patterns.iter().any(|p| p.trim().is_empty())
        {
            return Err(PredicateRegistryError(format!(
                "{}: surface_patterns must be non-empty and contain no blank entry (§20.2 rule 2)",
                row.predicate_id
            )));
        }
        if row.owner_module.trim().is_empty() {
            return Err(PredicateRegistryError(format!(
                "{}: owner_module must not be blank (§50 owner)",
                row.predicate_id
            )));
        }
        out.push(PredicateEntry {
            predicate_id: row.predicate_id,
            sql_predicate: row.sql_predicate,
            required_columns: row.required_columns,
            enumerable_scope: row.enumerable_scope,
            surface_patterns: row.surface_patterns,
            owner_module: row.owner_module,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(predicate_id: &str) -> PredicateRow {
        PredicateRow {
            predicate_id: predicate_id.to_string(),
            sql_predicate: "memory_type='REJECTION' AND superseded_at IS NULL".to_string(),
            required_columns: vec![
                "memory_type".to_string(),
                "superseded_at".to_string(),
                "visibility_workspace_id".to_string(),
            ],
            // dep-map: allow table-undeclared — predicate scope metadata; SQL is executed by adapters::exact_census
            enumerable_scope: "private.memory_records WHERE visibility_workspace_id = $1"
                .to_string(),
            surface_patterns: vec![
                "所有被否决".to_string(),
                "否掉过哪些".to_string(),
                "all rejected".to_string(),
            ],
            owner_module: "retrieval::planner".to_string(),
        }
    }

    #[test]
    fn valid_row_loads() {
        let loaded = load_registry(vec![row("rejected_decisions_v1")]).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].predicate_id, "rejected_decisions_v1");
    }

    #[test]
    fn fault_blank_predicate_id_is_err() {
        assert!(load_registry(vec![row("")]).is_err());
    }

    #[test]
    fn fault_duplicate_predicate_id_is_err() {
        let err = load_registry(vec![row("x"), row("x")]).unwrap_err();
        assert!(err.0.contains("duplicate"));
    }

    #[test]
    fn fault_empty_required_columns_is_err() {
        let mut r = row("x");
        r.required_columns.clear();
        assert!(load_registry(vec![r]).is_err());
    }

    #[test]
    fn fault_empty_enumerable_scope_is_err() {
        let mut r = row("x");
        r.enumerable_scope = "  ".to_string();
        assert!(load_registry(vec![r]).is_err());
    }

    #[test]
    fn fault_empty_surface_patterns_is_err() {
        let mut r = row("x");
        r.surface_patterns.clear();
        assert!(load_registry(vec![r]).is_err());
    }

    #[test]
    fn fault_blank_entry_in_surface_patterns_is_err() {
        let mut r = row("x");
        r.surface_patterns.push("   ".to_string());
        assert!(load_registry(vec![r]).is_err());
    }

    #[test]
    fn fault_blank_owner_module_is_err() {
        let mut r = row("x");
        r.owner_module = "".to_string();
        assert!(load_registry(vec![r]).is_err());
    }
}
