//! `maintenance::retention` — the one-shot §48.1 retention executor arms `retention approve | create-partitions |
//!   execute` (ADR-0063 D-H, D-I): flag parsing, the superuser connect and the JSON receipt. Every drop predicate
//!   (policy, cutoff, newest leaf, registry = catalog by name and bounds, proposal, holds, row count) lives in
//!   `control.partition_drop_check`; the SQL calls live in `adapters::maintenance_repo`.
//! Depends-on: crates=[humaux-adapters, serde_json, time]; services=[PostgreSQL(owner)];
//!   env=[HUMAUX_MIGRATOR_PG_DSN]; modules=[adapters::maintenance_repo, adapters::postgres, maintenance::main]
//! Called-by: [maintenance::main]
//! Invariants: [never resident: each arm opens the superuser migrator principal for one transaction from the
//!   operator's shell and exits (both resident modes refuse to boot with that key, ADR-0063 D-H); a non-superuser or
//!   §6.2.0 principal is refused before any other statement (exit 2); ids are the only selectors, never a table or
//!   leaf name; a real `execute` without `--registry-id` is refused before connecting (exit 2); every flag is
//!   required with no default (§78.1); refusals exit 3 with `{"outcome":"refused","reason":<code>}` (ADR-0053 D-F)]
//! Spec: Baseline §48.1; §77; §78.1; ADR-0053 D-F; ADR-0059 D-E; ADR-0063 D-F; ADR-0063 D-H; ADR-0063 D-I;
//!   ADR-0063 D-J

use std::path::PathBuf;
use std::time::Duration;

use humaux_adapters::maintenance_repo::{self as repo, DropOutcome, DropRequest, PartitionTable};
use humaux_adapters::postgres::{PoolInitError, RetentionExecutor};
use serde_json::json;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::{Admin, Args, Failure, Output, Result, env, to_json, uuid_flag};

/// ADR-0063 D-H: the superuser principal, supplied only in the operator's shell for one command.
const MIGRATOR_DSN: &str = "HUMAUX_MIGRATOR_PG_DSN";

/// ADR-0063 D-H: the checked superuser connect. A role mismatch (a §6.2.0 role such as the maintenance DSN, or a
/// non-superuser) is a configuration refusal, exit 2; an unreachable database is infrastructure, exit 1.
async fn executor() -> Result<RetentionExecutor> {
    let dsn = env(MIGRATOR_DSN)?;
    // dep: PostgreSQL(owner) — the superuser executor principal, one command, never a standing pool
    RetentionExecutor::connect(&dsn).await.map_err(|e| match e {
        PoolInitError::RoleMismatch { .. } => {
            Failure::Usage(format!("connect {MIGRATOR_DSN}: {e}"))
        }
        PoolInitError::Connect(_) => Failure::Infra(format!("connect {MIGRATOR_DSN}: {e}")),
    })
}

/// `--lock-timeout-ms`: how long any lock of the run may queue before the run refuses (55P03, exit 1).
fn lock_timeout(args: &Args) -> Result<Duration> {
    let ms: u64 = args.parsed("--lock-timeout-ms")?;
    if ms == 0 {
        return Err(Failure::Usage(
            "--lock-timeout-ms must be > 0 (0 waits forever)".to_owned(),
        ));
    }
    Ok(Duration::from_millis(ms))
}

/// `retention approve --table KEY --months <n|forever> --effective-at <rfc3339> --lock-timeout-ms MS`.
pub(crate) async fn approve(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let key = args.required("--table")?;
    let table = PartitionTable::parse(&key)
        .ok_or_else(|| Failure::Usage(format!("--table: {key:?} is not a §48.1 table_key")))?;
    let months = match args.required("--months")?.as_str() {
        "forever" => None,
        n => Some(
            n.parse::<i32>()
                .ok()
                .filter(|m| *m >= 1)
                .ok_or_else(|| Failure::Usage("--months must be >= 1 or forever".to_owned()))?,
        ),
    };
    let effective_at = OffsetDateTime::parse(&args.required("--effective-at")?, &Rfc3339)
        .map_err(|_| Failure::Usage("--effective-at must be RFC 3339".to_owned()))?;
    let timeout = lock_timeout(args)?;
    let exec = executor().await?;
    let receipt =
        repo::retention_approve(&exec, table, months, effective_at, timeout, &admin.action())
            .await?;
    let mut out = to_json(&receipt)?;
    out["command"] = json!("retention approve");
    out["outcome"] = json!("created");
    out["table_key"] = json!(table.key());
    out["retention_months"] = json!(months);
    out["trace_id"] = json!(admin.trace_id);
    Ok(Output::ok(out))
}

/// ADR-0063 D-F: the closed `--months-ahead` range. Below 2 leaves no alert margin before the outage; above 24 is a
/// typo, not a plan (the design horizon is 3): each month is one leaf per table with its indexes, policies and
/// triggers, all in one transaction, so `3000` would bloat the catalog and the WAL.
const MONTHS_AHEAD: std::ops::RangeInclusive<i32> = 2..=24;

/// `retention create-partitions --months-ahead N --lock-timeout-ms MS` (N in [`MONTHS_AHEAD`]; the runbook runs 3
/// monthly). The range is checked before any connection is made.
pub(crate) async fn create_partitions(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let months_ahead: i32 = args.parsed("--months-ahead")?;
    if !MONTHS_AHEAD.contains(&months_ahead) {
        return Err(Failure::Usage(format!(
            "--months-ahead must be in {}..={} (ADR-0063 D-F), got {months_ahead}",
            MONTHS_AHEAD.start(),
            MONTHS_AHEAD.end()
        )));
    }
    let timeout = lock_timeout(args)?;
    let exec = executor().await?;
    let receipt = repo::create_partitions(&exec, months_ahead, timeout, &admin.action()).await?;
    let mut out = to_json(&receipt)?;
    out["command"] = json!("retention create-partitions");
    out["outcome"] = json!(if receipt.created.is_empty() {
        "existing"
    } else {
        "created"
    });
    out["trace_id"] = json!(admin.trace_id);
    Ok(Output::ok(out))
}

/// `retention execute --policy ID --registry-id ID --export-dir DIR --lock-timeout-ms MS [--dry-run]`; `--dry-run`
/// without `--registry-id` lists the due leaves with their verdicts.
pub(crate) async fn execute(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let policy_id = uuid_flag(args, "--policy")?;
    let dry_run = args.0.iter().any(|a| a == "--dry-run");
    let registry_id = match args.get("--registry-id") {
        Some(_) => Some(uuid_flag(args, "--registry-id")?),
        // ADR-0063 D-I: a real run names its one leaf; refused before connecting.
        None if !dry_run => return Err(Failure::Usage("registry_id_required".to_owned())),
        None => None,
    };
    let export_dir = PathBuf::from(args.required("--export-dir")?);
    if !export_dir.is_dir() {
        return Err(Failure::Usage(format!(
            "--export-dir {} is not a directory",
            export_dir.display()
        )));
    }
    let request = DropRequest {
        policy_id,
        registry_id,
        export_dir: &export_dir,
        lock_timeout: lock_timeout(args)?,
        dry_run,
    };
    let exec = executor().await?;
    let mut out = match repo::retention_execute(&exec, &request, &admin.action()).await? {
        DropOutcome::Listed(due) => json!({ "outcome": "listed", "due": to_json(&due)? }),
        DropOutcome::DryRun {
            leaf,
            statements,
            export_path,
        } => {
            let mut out = to_json(&leaf)?;
            out["outcome"] = json!("dry_run");
            out["statements"] = json!(statements);
            out["export_path"] = json!(export_path);
            out
        }
        DropOutcome::Dropped(receipt) => {
            let mut out = to_json(&receipt)?;
            out["outcome"] = json!("dropped");
            out
        }
        DropOutcome::AlreadyDropped(receipt) => {
            let mut out = to_json(&receipt)?;
            out["outcome"] = json!("already_dropped");
            out
        }
    };
    out["command"] = json!("retention execute");
    out["policy_id"] = json!(policy_id);
    out["trace_id"] = json!(admin.trace_id);
    Ok(Output::ok(out))
}
