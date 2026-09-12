//! `cargo xtask projection-serve` — the §16.2 blue/green switch that makes a projection
//! version the one `recall` reads (`projection.stream_checkpoints.serving`). Ops action, not a
//! test fixture: the projection worker only advances highwaters, it never flips `serving`;
//! without this step every recall answers `DEPENDENCY_UNAVAILABLE` (deployment rehearsal
//! 2026-09-03). Runs under `HUMAUX_MAINTENANCE_PG_DSN` — the only role with
//! `UPDATE(serving, shadow)` on the checkpoint row (§6.2.2).
//!
//! §16.3 criterion ①'s two `visible_*` counts come from [`crate::switch_visible`] (card 18's
//! shared producer, live against Qdrant), **not** from a number typed on the command line. The
//! old `--visible-shadow <n>` flag is accepted and ignored: the rehearsal filled it with
//! `count(*) FROM projection.private_memory_points`, a PostgreSQL row count that had never been
//! compared against the index it claimed to describe, and `visible_serving` was hard-`None`, so
//! every switch after the first activation was refused `VisibleUnavailable` (ADR-0040).

use humaux_adapters::postgres::MaintenanceDbPool;
use humaux_adapters::serving_repo::{SwitchOutcome, switch_projection_version};
use humaux_domain::ids::TenantId;
use humaux_projection::serving::ContinuationVerdict;
use humaux_projection::serving::StreamFamily;
use postgres::{Client, NoTls};
use uuid::Uuid;

use crate::switch_visible::{
    Candidate, SERVING_ROW_VERSION_SQL, VisibleFace, VisiblePair, ops_scope, qdrant_endpoint,
    read_candidate_facts, visible_pair,
};

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn required(args: &[String], flag: &str) -> Result<String, String> {
    arg(args, flag).ok_or_else(|| format!("missing required flag {flag} (§78.1: no default)"))
}

/// Everything the CLI contributes, parsed before any connection is opened.
struct Flags {
    family: StreamFamily,
    workspace: Uuid,
    version: String,
    continuation: ContinuationVerdict,
    qdrant_host: String,
    qdrant_port: u16,
}

fn parse(args: &[String]) -> Result<Flags, String> {
    let tenant: Uuid = required(args, "--tenant")?
        .parse()
        .map_err(|e| format!("--tenant must be a uuid: {e}"))?;
    let workspace: Uuid = required(args, "--workspace")?
        .parse()
        .map_err(|e| format!("--workspace must be a uuid: {e}"))?;
    let domain = required(args, "--domain")?;
    let kind = required(args, "--projection-kind")?;
    let version = required(args, "--version")?;
    let continuation = match arg(args, "--continuation").as_deref() {
        None | Some("pass") => ContinuationVerdict::Pass,
        Some("fail") => ContinuationVerdict::Fail,
        Some("inconclusive") => ContinuationVerdict::Inconclusive,
        Some(other) => return Err(format!("--continuation {other:?}: pass|fail|inconclusive")),
    };
    let (qdrant_host, qdrant_port) = qdrant_endpoint(args)?;
    Ok(Flags {
        family: StreamFamily::new(TenantId(tenant), "workspace", workspace, domain, kind),
        workspace,
        version,
        continuation,
        qdrant_host,
        qdrant_port,
    })
}

/// Entry point. Flags: `--tenant <uuid> --workspace <uuid> --domain <s> --projection-kind <s>
/// --version <s>`; `--continuation pass|fail|inconclusive` (default `pass` is the operator's
/// attestation that the §69 gate was consulted); `--qdrant-host`/`--qdrant-port` (local
/// defaults). `--visible-shadow` is obsolete — see the module doc.
pub fn run(args: &[String]) -> i32 {
    let flags = match parse(args) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("projection-serve: {e}");
            return 2;
        }
    };
    if arg(args, "--visible-shadow").is_some() {
        eprintln!(
            "projection-serve: ignoring --visible-shadow (obsolete): §23.1②'s visible counts are \
             now taken live from Qdrant for both the candidate and the serving version"
        );
    }
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
    let (shadow, serving) = match visible_inputs(&dsn, &flags, &rt) {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("projection-serve: {e}");
            return 1;
        }
    };
    println!("projection-serve: visible shadow={shadow:?} serving={serving:?}");

    let pool = match rt.block_on(MaintenanceDbPool::connect(&dsn)) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("projection-serve: maintenance pool: {e:?}");
            return 1;
        }
    };
    let outcome = rt.block_on(switch_projection_version(
        &pool,
        &flags.family,
        &flags.version,
        shadow,
        serving,
        flags.continuation,
    ));
    match outcome {
        Ok(SwitchOutcome::Switched) => {
            println!("projection-serve: switched {} to serving", flags.version);
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

/// §16.3 criterion ①'s pair, taken before the switch transaction opens.
///
/// The `serving` row is read here rather than reused from inside
/// [`switch_projection_version`]'s advisory-locked transaction, so a concurrent switch could in
/// principle move it between this read and that one. That race is already caught and does not
/// need a second guard: the counts carry the `projection_version` they were taken against, and
/// the switch transaction cross-checks that tag against the version it itself reads under the
/// lock — a stale tag is `SwitchRejection::VisibleVersionMismatch`, never a trusted count.
fn visible_inputs(
    dsn: &str,
    flags: &Flags,
    rt: &tokio::runtime::Runtime,
) -> Result<VisiblePair, String> {
    let mut db = Client::connect(dsn, NoTls).map_err(|e| format!("role_maintenance: {e}"))?;
    let family = &flags.family;
    db.batch_execute(&format!("SET humaux.tenant_id = '{}'", family.tenant_id.0))
        .map_err(|e| format!("SET humaux.tenant_id: {e}"))?;
    let serving = db
        .query_opt(
            SERVING_ROW_VERSION_SQL,
            &[
                &family.tenant_id.0,
                &family.scope_kind,
                &family.scope_id,
                &family.domain,
                &family.projection_kind,
            ],
        )
        .map_err(|e| format!("serving row: {e}"))?
        .map(|row| row.get::<_, String>(0));
    let Some(facts) = read_candidate_facts(&mut db, family, &flags.version, serving.as_deref())?
    else {
        // No §17.3 placement row ⇒ no collection ⇒ no honest count. `None` on both sides is
        // `VisibleUnavailable`, which is the correct refusal, not a reason to invent a number.
        eprintln!(
            "projection-serve: tenant has no private-memory placement row; visible counts \
             unavailable (§23.1②: never backfilled)"
        );
        return Ok((None, None));
    };
    let face = VisibleFace::connect(&flags.qdrant_host, flags.qdrant_port)?;
    let scope = ops_scope(family.tenant_id.0, flags.workspace)?;
    Ok(rt.block_on(visible_pair(
        &face,
        &Candidate {
            scope: &scope,
            collection: &facts.collection,
            candidate_version: &flags.version,
            candidate_tombstoned: &facts.candidate_tombstoned,
            serving_version: serving.as_deref(),
            serving_tombstoned: &facts.serving_tombstoned,
            user_private_points: facts.user_private_points,
        },
    )))
}
