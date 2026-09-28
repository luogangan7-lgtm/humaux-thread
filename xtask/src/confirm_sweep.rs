//! `xtask::confirm_sweep` — operator door for control.sweep_confirm_tokens retention sweep (migration 0169/0170).
//! Depends-on: crates=[humaux-adapters, postgres, tokio, uuid]; services=[PostgreSQL(any) r=[control.tenants],
//!   PostgreSQL(role_maintenance)]; env=[HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_TEST_PG_DSN];
//!   modules=[adapters::confirm_token_repo, adapters::postgres]
//! Called-by: [xtask::main]
//! Invariants: [runs the owner SECURITY DEFINER sweep function; retention predicate stays inside that function, never re-typed here]
//! Spec: Baseline §33.10 rule 9; migration 0169; 0170
//!
//! `cargo xtask sweep-confirm-tokens` — the operator door to `control.sweep_confirm_tokens`
//! (migration 0169, forward-fixed by 0170; §33.10 rule 9, card 1 review P2 folded into card 21).
//!
//! Why this exists: the sweep function and its retention predicate landed with no caller at all
//! — no scheduler, no CLI, no runbook line — so `control.confirm_tokens` still grew without
//! bound on every running deployment and the predicate was exercised only by hand, once. This
//! is the runnable door (`cron`/`launchd` calls it; the predicate itself stays inside the owner
//! SECURITY DEFINER function, never re-typed here).
//!
//! Runs under `HUMAUX_MAINTENANCE_PG_DSN` — `role_maintenance` holds EXECUTE on the function and
//! nothing else on the table (§6.2.1: no non-owner role holds DELETE anywhere). The table FORCEs
//! RLS, so the sweep is per-tenant by construction and this command loops the tenants it is
//! given: `--tenant <uuid>` (repeatable), or `--all-tenants`, which enumerates `control.tenants`
//! through `HUMAUX_TEST_PG_DSN` (the only DSN here that can see across tenants) and then sweeps
//! each one through the maintenance role.
//!
//! Retention is the deployment's policy and has no default (§78.1): `--consumed-retention-secs`
//! is required. A consumed token is kept for that long so the §9 audit answer "which confirm
//! token authorized this destructive call" outlives the call; an unconsumed one is deletable the
//! moment it expires.

use humaux_adapters::confirm_token_repo;
use humaux_adapters::postgres::MaintenanceDbPool;
use postgres::{Client, NoTls};
use uuid::Uuid;

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn parse(args: &[String]) -> Result<(Vec<Uuid>, std::time::Duration), String> {
    let secs: f64 = arg(args, "--consumed-retention-secs")
        .ok_or_else(|| {
            "missing required flag --consumed-retention-secs (§78.1: retention is deployment \
             policy, there is no default)"
                .to_owned()
        })?
        .parse()
        .map_err(|e| format!("--consumed-retention-secs must be a number: {e}"))?;
    if !secs.is_finite() || secs < 0.0 {
        return Err("--consumed-retention-secs must be a non-negative number".to_owned());
    }
    let mut tenants = Vec::new();
    for (i, a) in args.iter().enumerate() {
        if a == "--tenant" {
            let raw = args
                .get(i + 1)
                .ok_or_else(|| "--tenant needs a uuid".to_owned())?;
            tenants.push(
                raw.parse::<Uuid>()
                    .map_err(|e| format!("--tenant must be a uuid: {e}"))?,
            );
        }
    }
    let all = args.iter().any(|a| a == "--all-tenants");
    if all == !tenants.is_empty() {
        return Err(
            "pass either --all-tenants or one or more --tenant <uuid>, not both and not neither"
                .to_owned(),
        );
    }
    Ok((tenants, std::time::Duration::from_secs_f64(secs)))
}

/// Every `control.tenants` row, read through the cross-tenant DSN — `role_maintenance` cannot
/// enumerate them (RLS has no cross-tenant carve-out for it; see `stream_repo`'s module doc).
fn all_tenants() -> Result<Vec<Uuid>, String> {
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN")
        .map_err(|_| "--all-tenants needs HUMAUX_TEST_PG_DSN to enumerate control.tenants")?;
    // dep: PostgreSQL(any) — sweep target database (--dsn or HUMAUX_MAINTENANCE_PG_DSN/HUMAUX_TEST_PG_DSN)
    let mut client = Client::connect(&dsn, NoTls).map_err(|e| format!("connect: {e}"))?;
    let rows = client
        .query(
            "SELECT tenant_id FROM control.tenants ORDER BY tenant_id",
            &[],
        )
        .map_err(|e| format!("enumerate tenants: {e}"))?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

pub fn run(args: &[String]) -> i32 {
    let (tenants, retention) = match parse(args) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("sweep-confirm-tokens: {e}");
            return 2;
        }
    };
    let tenants = if tenants.is_empty() {
        match all_tenants() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("sweep-confirm-tokens: {e}");
                return 2;
            }
        }
    } else {
        tenants
    };
    let dsn = match std::env::var("HUMAUX_MAINTENANCE_PG_DSN") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("sweep-confirm-tokens: HUMAUX_MAINTENANCE_PG_DSN is required");
            return 2;
        }
    };
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("sweep-confirm-tokens: runtime: {e}");
            return 1;
        }
    };
    let result = rt.block_on(async {
        // dep: PostgreSQL(role_maintenance) — sweep target database (--dsn or HUMAUX_MAINTENANCE_PG_DSN/HUMAUX_TEST_PG_DSN)
        let pool = MaintenanceDbPool::connect(&dsn)
            .await
            .map_err(|e| format!("maintenance pool: {e}"))?;
        let mut total = 0i64;
        for tenant in &tenants {
            let deleted = confirm_token_repo::sweep_expired(&pool, *tenant, retention)
                .await
                .map_err(|e| format!("tenant {tenant}: sweep failed: {e:?}"))?;
            println!("sweep-confirm-tokens: tenant={tenant} deleted={deleted}");
            total += deleted;
        }
        Ok::<_, String>(total)
    });
    match result {
        Ok(total) => {
            println!(
                "sweep-confirm-tokens: {} tenant(s), {total} row(s) deleted, retention={}s",
                tenants.len(),
                retention.as_secs_f64()
            );
            0
        }
        Err(e) => {
            eprintln!("sweep-confirm-tokens: {e}");
            1
        }
    }
}
