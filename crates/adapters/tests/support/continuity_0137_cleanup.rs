//! `adapters::tests::support::continuity_0137_cleanup` — Cleanup owner for the 0137 continuity fixtures: purges every
//!   registered fixture tenant through the one fixture purge, then the registered users.
//! Depends-on: crates=[humaux-testkit, postgres, serde_json, uuid]; services=[PostgreSQL(any) r=[control.memberships, control.private_reasoning_domains, control.tenants, control.workspaces, ops.outbox, private.continuity_facet_evidence_links, private.continuity_facet_memory_links, private.continuity_facet_slots, private.continuity_facet_versions, private.continuity_projects, private.evidence_objects, private.memory_evidence, private.memory_records, projection.stream_log] w=[control.users, ops.jobs]]; env=[]; modules=[testkit::fixture_purge]
//! Called-by: [adapters::tests::project_continuity_read_0137, adapters::tests::support::continuity_0137_fixture]
//! Invariants: [touches only the tenants and users the fixture registered: the purge refuses a tenant whose name is
//!   not `e2e-…` or whose closure reaches another tenant's rows, so a failed test never erases another tenant's
//!   continuity data; each purge is atomic per tenant and runs under the 0137 DDL advisory lock]
//! Spec: Baseline §25.3.1; ADR-0063 ("Dev integrity finding")
//!
#![allow(dead_code)]

use humaux_testkit::fixture_purge::purge_tenant_fixture_sql;
use postgres::{Client, NoTls};
use serde_json::{Value, json};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use uuid::Uuid;

#[derive(Clone, Debug, Default)]
struct Ledger {
    tenants: Vec<Uuid>,
    users: Vec<Uuid>,
    workspaces: Vec<Uuid>,
    domains: Vec<Uuid>,
    projects: Vec<Uuid>,
    memories: Vec<Uuid>,
    evidences: Vec<Uuid>,
}

impl Ledger {
    fn push_unique(values: &mut Vec<Uuid>, value: Uuid) {
        if !values.contains(&value) {
            values.push(value);
        }
    }

    fn to_json(&self) -> Value {
        let uuids = |values: &[Uuid]| {
            Value::Array(
                values
                    .iter()
                    .map(|value| Value::String(value.to_string()))
                    .collect(),
            )
        };
        json!({
            "tenants": uuids(&self.tenants),
            "users": uuids(&self.users),
            "workspaces": uuids(&self.workspaces),
            "domains": uuids(&self.domains),
            "projects": uuids(&self.projects),
            "memories": uuids(&self.memories),
            "evidences": uuids(&self.evidences),
        })
    }

    fn from_json(value: &Value) -> Result<Self, String> {
        let list = |name: &str| {
            value[name]
                .as_array()
                .ok_or_else(|| format!("cleanup ledger missing {name}"))?
                .iter()
                .map(|value| {
                    Uuid::parse_str(
                        value
                            .as_str()
                            .ok_or_else(|| format!("cleanup ledger {name} has non-string UUID"))?,
                    )
                    .map_err(|error| format!("cleanup ledger {name} has invalid UUID: {error}"))
                })
                .collect::<Result<Vec<_>, _>>()
        };
        Ok(Self {
            tenants: list("tenants")?,
            users: list("users")?,
            workspaces: list("workspaces")?,
            domains: list("domains")?,
            projects: list("projects")?,
            memories: list("memories")?,
            evidences: list("evidences")?,
        })
    }
}

/// The advisory lock key the 0137 acceptance guards hold around their DDL; the cleanup holds it around its purges.
pub const POLICY_DDL_LOCK_KEY: i64 = 13_720_260_831;

/// Exact owner for all rows created by one continuity fixture and its clones.
/// ponytail: one mutex + one last-owner Drop keeps the test-only fix local and avoids a
/// second cleanup abstraction or a global database sweep.
pub struct CleanupOwner {
    admin_dsn: String,
    ledger: Mutex<Ledger>,
    cleanup_lock: Mutex<()>,
    cleaned: AtomicBool,
    disarmed: AtomicBool,
}

impl std::fmt::Debug for CleanupOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CleanupOwner")
            .field("admin_dsn", &"<redacted>")
            .field("cleaned", &self.cleaned.load(Ordering::Acquire))
            .field("disarmed", &self.disarmed.load(Ordering::Acquire))
            .finish()
    }
}

impl CleanupOwner {
    pub fn new(admin_dsn: String) -> Self {
        Self {
            admin_dsn,
            ledger: Mutex::new(Ledger::default()),
            cleanup_lock: Mutex::new(()),
            cleaned: AtomicBool::new(false),
            disarmed: AtomicBool::new(false),
        }
    }

    pub fn from_state(admin_dsn: String, value: &Value) -> Result<Self, String> {
        Ok(Self {
            admin_dsn,
            ledger: Mutex::new(Ledger::from_json(value)?),
            cleanup_lock: Mutex::new(()),
            cleaned: AtomicBool::new(false),
            disarmed: AtomicBool::new(false),
        })
    }

    pub fn ledger_json(&self) -> Value {
        self.ledger.lock().expect("cleanup ledger mutex").to_json()
    }

    pub fn register_tenant(&self, id: Uuid) {
        Ledger::push_unique(
            &mut self.ledger.lock().expect("cleanup ledger mutex").tenants,
            id,
        );
    }

    pub fn register_user(&self, id: Uuid) {
        Ledger::push_unique(
            &mut self.ledger.lock().expect("cleanup ledger mutex").users,
            id,
        );
    }

    pub fn register_workspace(&self, id: Uuid) {
        Ledger::push_unique(
            &mut self.ledger.lock().expect("cleanup ledger mutex").workspaces,
            id,
        );
    }

    pub fn register_domain(&self, id: Uuid) {
        Ledger::push_unique(
            &mut self.ledger.lock().expect("cleanup ledger mutex").domains,
            id,
        );
    }

    pub fn register_project(&self, id: Uuid) {
        Ledger::push_unique(
            &mut self.ledger.lock().expect("cleanup ledger mutex").projects,
            id,
        );
    }

    pub fn register_memory(&self, id: Uuid) {
        Ledger::push_unique(
            &mut self.ledger.lock().expect("cleanup ledger mutex").memories,
            id,
        );
    }

    pub fn register_evidence(&self, id: Uuid) {
        Ledger::push_unique(
            &mut self.ledger.lock().expect("cleanup ledger mutex").evidences,
            id,
        );
    }

    pub fn disarm(&self) {
        self.disarmed.store(true, Ordering::Release);
    }

    pub fn rearm(&self) {
        self.disarmed.store(false, Ordering::Release);
        self.cleaned.store(false, Ordering::Release);
    }

    pub fn cleanup_now(&self) -> Result<(), String> {
        let _cleanup_lock = self.cleanup_lock.lock().expect("cleanup mutex");
        if self.disarmed.load(Ordering::Acquire) || self.cleaned.load(Ordering::Acquire) {
            return Ok(());
        }
        let ledger = self.ledger.lock().expect("cleanup ledger mutex").clone();
        // dep: PostgreSQL(any) — test opens a direct PG connection for setup/verification
        let mut admin = Client::connect(&self.admin_dsn, NoTls)
            .map_err(|error| format!("connect admin for continuity cleanup: {error}"))?;
        let cleanup = purge_rows(&mut admin, &ledger);
        if cleanup.is_ok() {
            self.cleaned.store(true, Ordering::Release);
        }
        cleanup
    }

    pub fn counts(&self) -> Result<Vec<i64>, String> {
        let ledger = self.ledger.lock().expect("cleanup ledger mutex").clone();
        // dep: PostgreSQL(any) — test opens a direct PG connection for setup/verification
        let mut admin = Client::connect(&self.admin_dsn, NoTls)
            .map_err(|error| format!("connect admin for continuity census: {error}"))?;
        let row = admin
            .query_one(
                "SELECT
                   (SELECT count(*) FROM private.continuity_projects WHERE tenant_id=ANY($1) AND project_id=ANY($2)),
                   (SELECT count(*) FROM private.continuity_facet_versions WHERE tenant_id=ANY($1) AND project_id=ANY($2)),
                   (SELECT count(*) FROM private.continuity_facet_memory_links WHERE tenant_id=ANY($1) AND project_id=ANY($2)),
                   (SELECT count(*) FROM private.continuity_facet_evidence_links WHERE tenant_id=ANY($1) AND project_id=ANY($2)),
                   (SELECT count(*) FROM private.continuity_facet_slots WHERE tenant_id=ANY($1) AND project_id=ANY($2)),
                   (SELECT count(*) FROM private.memory_records WHERE tenant_id=ANY($1) AND memory_id=ANY($3)),
                   (SELECT count(*) FROM private.memory_evidence WHERE memory_id=ANY($3) AND evidence_id=ANY($4)),
                   (SELECT count(*) FROM private.evidence_objects WHERE tenant_id=ANY($1) AND evidence_id=ANY($4)),
                   (SELECT count(*) FROM ops.outbox WHERE tenant_id=ANY($1) AND evidence_id=ANY($4)),
                   (SELECT count(*) FROM projection.stream_log WHERE tenant_id=ANY($1) AND scope_kind='tenant' AND scope_id=ANY($1) AND domain='knowledge' AND projection_kind='continuity-w2' AND projection_version='v1'),
                   (SELECT count(*) FROM control.tenants WHERE tenant_id=ANY($1)),
                   (SELECT count(*) FROM control.users WHERE user_id=ANY($5)),
                   (SELECT count(*) FROM control.workspaces WHERE tenant_id=ANY($1) AND workspace_id=ANY($6)),
                   (SELECT count(*) FROM control.memberships WHERE tenant_id=ANY($1) AND user_id=ANY($5)),
                   (SELECT count(*) FROM control.private_reasoning_domains WHERE tenant_id=ANY($1) AND reasoning_domain_id=ANY($7))",
                &[
                    &ledger.tenants,
                    &ledger.projects,
                    &ledger.memories,
                    &ledger.evidences,
                    &ledger.users,
                    &ledger.workspaces,
                    &ledger.domains,
                ],
            )
            .map_err(|error| format!("continuity cleanup census: {error}"))?;
        Ok((0..15).map(|index| row.get(index)).collect())
    }
}

fn db_detail(error: &postgres::Error) -> String {
    error.as_db_error().map_or_else(
        || error.to_string(),
        |d| format!("{}: {}", d.code().code(), d.message()),
    )
}

/// ADR-0063 "Dev integrity finding": the replica-mode DELETE list this replaces skipped RI and user triggers and left
/// orphans on dev (e.g. `control.workspace_memberships` was never listed). Every registered tenant still present goes
/// through the one fixture purge — all its rows, everything referencing them, the identity rows — and the users,
/// which are not tenant rows, go afterwards with constraints enforced. The `ops.jobs` delete reaches rows the purge
/// reaches too; it stays because gate `fixture_jobs_cleanup` pins that statement in this file (card 33: each seeded
/// EVIDENCE_ACCEPTED row enqueues a DERIVED_DISTILL job through the 0164 trigger).
///
/// Each statement is its own transaction (one purge per transaction: its temp tables drop on commit); a purge is
/// atomic per tenant, so a failure leaves whole tenants, never half of one. The purge reads the catalog at run time,
/// so it holds [`POLICY_DDL_LOCK_KEY`] — the lock the acceptance file's guards take around their DDL — and never sees
/// a guard's `private.continuity_w2_barrier_*` table (it has a `tenant_id`) created or dropped under it.
fn purge_rows(admin: &mut Client, ledger: &Ledger) -> Result<(), String> {
    let run = |admin: &mut Client,
               what: &str,
               sql: &str,
               params: &[&(dyn postgres::types::ToSql + Sync)]| {
        admin
            .execute(sql, params)
            .map(|_| ())
            .map_err(|error| format!("continuity cleanup {what}: {}", db_detail(&error)))
    };
    run(
        admin,
        "ddl lock",
        "SELECT pg_advisory_lock($1)",
        &[&POLICY_DDL_LOCK_KEY],
    )?;
    run(
        admin,
        "jobs",
        "DELETE FROM ops.jobs WHERE tenant_id=ANY($1)",
        &[&ledger.tenants],
    )?;
    let present: Vec<Uuid> = admin
        .query(
            "SELECT tenant_id FROM control.tenants WHERE tenant_id=ANY($1)",
            &[&ledger.tenants],
        )
        .map_err(|error| format!("continuity cleanup tenant census: {}", db_detail(&error)))?
        .iter()
        .map(|row| row.get(0))
        .collect();
    // Last registered first: a decoy tenant's planted `ops.outbox` row references the fixture tenant's evidence, never
    // the reverse, and the purge refuses a tenant whose closure reaches another tenant's rows.
    for &tenant in ledger.tenants.iter().rev().filter(|t| present.contains(t)) {
        let purge = purge_tenant_fixture_sql(&tenant.to_string())?;
        admin.batch_execute(&purge).map_err(|error| {
            format!(
                "continuity cleanup purge of tenant {tenant}: {}",
                db_detail(&error)
            )
        })?;
    }
    run(
        admin,
        "users",
        "DELETE FROM control.users WHERE user_id=ANY($1)",
        &[&ledger.users],
    )?;
    run(
        admin,
        "ddl unlock",
        "SELECT pg_advisory_unlock($1)",
        &[&POLICY_DDL_LOCK_KEY],
    )
}

impl Drop for CleanupOwner {
    fn drop(&mut self) {
        if self.disarmed.load(Ordering::Acquire) || self.cleaned.load(Ordering::Acquire) {
            return;
        }
        if let Err(error) = self.cleanup_now() {
            if std::thread::panicking() {
                eprintln!("continuity fixture cleanup failed during unwind: {error}");
            } else {
                panic!("continuity fixture cleanup failed: {error}");
            }
        }
    }
}
