//! `cargo xtask projection-serve` — the §16.2 blue/green switch that makes a projection
//! version the one `recall` reads (`projection.stream_checkpoints.serving`). Ops action, not a
//! test fixture: the projection worker only advances highwaters, it never flips `serving`;
//! without this step every recall answers `DEPENDENCY_UNAVAILABLE` (deployment rehearsal
//! 2026-09-03). Runs under `HUMAUX_MAINTENANCE_PG_DSN` — the only role with
//! `UPDATE(serving, shadow)` on the checkpoint row (§6.2.2).

use humaux_adapters::postgres::MaintenanceDbPool;
use humaux_adapters::serving_repo::{SwitchOutcome, switch_projection_version};
use humaux_domain::ids::TenantId;
use humaux_projection::serving::ContinuationVerdict;
use humaux_projection::serving::StreamFamily;
use uuid::Uuid;

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn required(args: &[String], flag: &str) -> Result<String, String> {
    arg(args, flag).ok_or_else(|| format!("missing required flag {flag} (§78.1: no default)"))
}

/// Entry point. Flags: `--tenant <uuid> --workspace <uuid> --domain <s> --projection-kind <s>
/// --version <s> --visible-shadow <n>`; `--continuation pass|fail|inconclusive` (default
/// `pass` is the operator's attestation that the shadow read-back was verified).
pub fn run(args: &[String]) -> i32 {
    let parsed = (|| -> Result<(StreamFamily, String, u64, ContinuationVerdict), String> {
        let tenant: Uuid = required(args, "--tenant")?
            .parse()
            .map_err(|e| format!("--tenant must be a uuid: {e}"))?;
        let workspace: Uuid = required(args, "--workspace")?
            .parse()
            .map_err(|e| format!("--workspace must be a uuid: {e}"))?;
        let domain = required(args, "--domain")?;
        let kind = required(args, "--projection-kind")?;
        let version = required(args, "--version")?;
        let visible: u64 = required(args, "--visible-shadow")?
            .parse()
            .map_err(|e| format!("--visible-shadow must be an integer: {e}"))?;
        let continuation = match arg(args, "--continuation").as_deref() {
            None | Some("pass") => ContinuationVerdict::Pass,
            Some("fail") => ContinuationVerdict::Fail,
            Some("inconclusive") => ContinuationVerdict::Inconclusive,
            Some(other) => return Err(format!("--continuation {other:?}: pass|fail|inconclusive")),
        };
        let family = StreamFamily::new(TenantId(tenant), "workspace", workspace, domain, kind);
        Ok((family, version, visible, continuation))
    })();
    let (family, version, visible, continuation) = match parsed {
        Ok(v) => v,
        Err(e) => {
            eprintln!("projection-serve: {e}");
            return 2;
        }
    };
    let dsn = match std::env::var("HUMAUX_MAINTENANCE_PG_DSN") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("projection-serve: HUMAUX_MAINTENANCE_PG_DSN is required");
            return 2;
        }
    };
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("projection-serve: runtime: {e}");
            return 1;
        }
    };
    let pool = match rt.block_on(MaintenanceDbPool::connect(&dsn)) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("projection-serve: maintenance pool: {e:?}");
            return 1;
        }
    };
    let outcome = rt.block_on(switch_projection_version(
        &pool,
        &family,
        &version,
        Some((version.clone(), visible)),
        None,
        continuation,
    ));
    match outcome {
        Ok(SwitchOutcome::Switched) => {
            println!("projection-serve: switched {version} to serving");
            0
        }
        Ok(SwitchOutcome::Rejected(reasons)) => {
            eprintln!("projection-serve: rejected: {reasons:?}");
            3
        }
        Ok(other) => {
            eprintln!("projection-serve: outcome {other:?}");
            3
        }
        Err(e) => {
            eprintln!("projection-serve: {e:?}");
            1
        }
    }
}
