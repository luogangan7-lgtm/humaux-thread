//! `xtask::member` — §6.3 membership lifecycle admin path (invite/activate/suspend/remove/set-role).
//! Depends-on: crates=[humaux-adapters, humaux-domain, tokio, uuid]; services=[PostgreSQL(role_maintenance)];
//!   env=[HUMAUX_MAINTENANCE_PG_DSN]; modules=[adapters::membership_repo, adapters::postgres, domain::identity,
//!   domain::ids]
//! Called-by: [xtask::main]
//! Invariants: [domain refuses illegal role edges and the last-OWNER rule; every action (including a refusal) appends its §77 audit row in the same transaction]
//! Spec: Baseline §6.3; §77; §78.1; ADR-0033
//!
//! xtask `member` — the §6.3 membership lifecycle admin path (ADR-0033, card 12): invite /
//! activate / suspend / remove / set-role for one `(tenant, user)` through
//! `humaux_adapters::membership_repo` under `role_maintenance` (`HUMAUX_MAINTENANCE_PG_DSN`).
//! Tenant onboarding stops being raw owner SQL: the domain machine refuses illegal edges and
//! the last-OWNER rule, the adapter bumps the user's security epoch and appends the §77 audit
//! row in the same transaction — a refusal appends its own `DENIED` row. §77 "Sensitive Admin
//! Action" makes `--actor --reason --ticket --step-up-auth` mandatory (no default identity or
//! reason, §78.1); `--trace-id` is optional and minted per invocation when absent, and is
//! printed either way. Prints one `member: pass (...)` / `member: fail (...)` line.

use humaux_adapters::membership_repo::{self, AdminAction, MembershipRepoError, MembershipRequest};
use humaux_adapters::postgres::MaintenanceDbPool;
use humaux_domain::identity::{MembershipMutation, MembershipRole};
use humaux_domain::ids::{TenantId, UserId};
use uuid::Uuid;

const MAINTENANCE_DSN_ENV: &str = "HUMAUX_MAINTENANCE_PG_DSN";
const USAGE: &str = "usage: cargo xtask member <invite|activate|suspend|remove|set-role> \
                     --tenant <uuid> --user <uuid> --actor <who> --reason <why> \
                     --ticket <request/ticket ref> --step-up-auth <context> \
                     [--trace-id <id>] [--role OWNER|ADMIN|MEMBER]";

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn required(args: &[String], flag: &str) -> Result<String, String> {
    arg(args, flag)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing {flag}\n{USAGE}"))
}

fn uuid_arg(args: &[String], flag: &str) -> Result<Uuid, String> {
    required(args, flag)?
        .parse()
        .map_err(|e| format!("{flag}: not a uuid ({e})"))
}

fn role_arg(args: &[String]) -> Result<MembershipRole, String> {
    let raw = arg(args, "--role").ok_or_else(|| format!("missing --role\n{USAGE}"))?;
    MembershipRole::from_db_str(&raw).ok_or_else(|| {
        format!(
            "--role {raw:?} not in {:?}",
            MembershipRole::ALL.map(MembershipRole::as_db_str)
        )
    })
}

fn request(args: &[String]) -> Result<MembershipRequest, String> {
    match args.first().map(String::as_str) {
        Some("invite") => Ok(MembershipRequest::Invite(role_arg(args)?)),
        Some("activate") => Ok(MembershipRequest::Mutate(MembershipMutation::Activate)),
        Some("suspend") => Ok(MembershipRequest::Mutate(MembershipMutation::Suspend)),
        Some("remove") => Ok(MembershipRequest::Mutate(MembershipMutation::Remove)),
        Some("set-role") => Ok(MembershipRequest::Mutate(MembershipMutation::ChangeRole(
            role_arg(args)?,
        ))),
        _ => Err(USAGE.to_string()),
    }
}

pub fn run(args: &[String]) -> i32 {
    match run_inner(args) {
        Ok(line) => {
            println!("member: pass ({line})");
            0
        }
        Err(e) => {
            eprintln!("member: fail ({e})");
            1
        }
    }
}

fn run_inner(args: &[String]) -> Result<String, String> {
    let request = request(args)?;
    let tenant_id = TenantId(uuid_arg(args, "--tenant")?);
    let user_id = UserId(uuid_arg(args, "--user")?);
    // §77 Sensitive Admin Action: actor / reason / ticket / step-up context all come from the
    // operator; no default identity or reason (§78.1). The trace id correlates this
    // invocation's audit row(s); minted here only when the operator has none to pass.
    let actor = required(args, "--actor")?;
    let reason = required(args, "--reason")?;
    let ticket = required(args, "--ticket")?;
    let step_up_auth_context = required(args, "--step-up-auth")?;
    let trace_id = arg(args, "--trace-id").unwrap_or_else(|| Uuid::now_v7().to_string());
    let admin = AdminAction {
        actor: &actor,
        reason: &reason,
        ticket: &ticket,
        trace_id: &trace_id,
        step_up_auth_context: &step_up_auth_context,
    };
    let dsn = std::env::var(MAINTENANCE_DSN_ENV)
        .map_err(|_| format!("missing object: ${MAINTENANCE_DSN_ENV} env var"))?;
    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("tokio runtime: {e}"))?;
    rt.block_on(async {
        // dep: PostgreSQL(role_maintenance) — HUMAUX_MAINTENANCE_PG_DSN, membership admin path
        let pool = MaintenanceDbPool::connect(&dsn)
            .await
            .map_err(|e| format!("cannot connect to ${MAINTENANCE_DSN_ENV}: {e}"))?;
        let outcome = membership_repo::apply(&pool, tenant_id, user_id, request, admin)
            .await
            .map_err(|e| match e {
                // Display already carries the §52 code for CONFLICT / NOT_FOUND / INVALID_INPUT.
                MembershipRepoError::Db(_) => {
                    format!("{} — {e} trace_id={trace_id}", e.error_code().as_str())
                }
                other => format!("{other} trace_id={trace_id}"),
            })?;
        Ok(format!(
            "membership_id={} state={} role={} user_security_epoch={} audit_event_id={} trace_id={trace_id}",
            outcome.membership_id,
            outcome.state.as_db_str(),
            outcome.role.as_db_str(),
            outcome
                .user_security_epoch
                .map_or_else(|| "unchanged".to_string(), |e| e.to_string()),
            outcome.audit_event_id
        ))
    })
}
