#![allow(dead_code)]

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
        let mut admin = Client::connect(&self.admin_dsn, NoTls)
            .map_err(|error| format!("connect admin for continuity cleanup: {error}"))?;
        let cleanup = (|| {
            let mut transaction = admin
                .transaction()
                .map_err(|error| format!("begin continuity cleanup: {error}"))?;
            transaction
                .batch_execute("SET LOCAL session_replication_role = replica")
                .map_err(|error| format!("set local replica cleanup mode: {error}"))?;
            delete_event_rows(&mut transaction, &ledger)?;
            delete_continuity_rows(&mut transaction, &ledger)?;
            delete_source_rows(&mut transaction, &ledger)?;
            delete_control_rows(&mut transaction, &ledger)?;
            transaction
                .batch_execute("SET CONSTRAINTS ALL IMMEDIATE")
                .map_err(|error| format!("finish continuity cleanup constraints: {error}"))?;
            transaction
                .commit()
                .map_err(|error| format!("commit continuity cleanup: {error}"))?;
            Ok::<(), String>(())
        })();
        if cleanup.is_ok() {
            self.cleaned.store(true, Ordering::Release);
        }
        cleanup
    }

    pub fn counts(&self) -> Result<Vec<i64>, String> {
        let ledger = self.ledger.lock().expect("cleanup ledger mutex").clone();
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

fn execute_delete(
    transaction: &mut postgres::Transaction<'_>,
    sql: &str,
    params: &[&(dyn postgres::types::ToSql + Sync)],
) -> Result<(), String> {
    transaction
        .execute(sql, params)
        .map(|_| ())
        .map_err(|error| format!("continuity cleanup `{sql}`: {error}"))
}

fn delete_event_rows(
    transaction: &mut postgres::Transaction<'_>,
    ledger: &Ledger,
) -> Result<(), String> {
    execute_delete(
        transaction,
        "DELETE FROM ops.outbox WHERE tenant_id=ANY($1) AND evidence_id=ANY($2)",
        &[&ledger.tenants, &ledger.evidences],
    )?;
    execute_delete(
        transaction,
        "DELETE FROM projection.stream_log \
         WHERE tenant_id=ANY($1) AND scope_kind='tenant' AND scope_id=ANY($1) \
           AND domain='knowledge' AND projection_kind='continuity-w2' AND projection_version='v1'",
        &[&ledger.tenants],
    )
}

fn delete_continuity_rows(
    transaction: &mut postgres::Transaction<'_>,
    ledger: &Ledger,
) -> Result<(), String> {
    for table in [
        "private.continuity_facet_evidence_links",
        "private.continuity_facet_memory_links",
        "private.continuity_facet_slots",
        "private.continuity_facet_versions",
        "private.continuity_projects",
    ] {
        execute_delete(
            transaction,
            &format!("DELETE FROM {table} WHERE tenant_id=ANY($1) AND project_id=ANY($2)"),
            &[&ledger.tenants, &ledger.projects],
        )?;
    }
    Ok(())
}

fn delete_source_rows(
    transaction: &mut postgres::Transaction<'_>,
    ledger: &Ledger,
) -> Result<(), String> {
    execute_delete(
        transaction,
        "DELETE FROM private.memory_evidence WHERE memory_id=ANY($1) AND evidence_id=ANY($2)",
        &[&ledger.memories, &ledger.evidences],
    )?;
    execute_delete(
        transaction,
        "DELETE FROM private.memory_records WHERE tenant_id=ANY($1) AND memory_id=ANY($2)",
        &[&ledger.tenants, &ledger.memories],
    )?;
    execute_delete(
        transaction,
        "DELETE FROM private.evidence_objects WHERE tenant_id=ANY($1) AND evidence_id=ANY($2)",
        &[&ledger.tenants, &ledger.evidences],
    )
}

fn delete_control_rows(
    transaction: &mut postgres::Transaction<'_>,
    ledger: &Ledger,
) -> Result<(), String> {
    execute_delete(
        transaction,
        "DELETE FROM control.memberships WHERE tenant_id=ANY($1) AND user_id=ANY($2)",
        &[&ledger.tenants, &ledger.users],
    )?;
    execute_delete(
        transaction,
        "DELETE FROM control.workspaces WHERE tenant_id=ANY($1) AND workspace_id=ANY($2)",
        &[&ledger.tenants, &ledger.workspaces],
    )?;
    execute_delete(
        transaction,
        "DELETE FROM control.private_reasoning_domains \
         WHERE tenant_id=ANY($1) AND reasoning_domain_id=ANY($2)",
        &[&ledger.tenants, &ledger.domains],
    )?;
    execute_delete(
        transaction,
        "DELETE FROM control.users WHERE user_id=ANY($1)",
        &[&ledger.users],
    )?;
    execute_delete(
        transaction,
        "DELETE FROM control.tenants WHERE tenant_id=ANY($1)",
        &[&ledger.tenants],
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
