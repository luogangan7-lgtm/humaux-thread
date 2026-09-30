//! `adapters::subject_repo` — SQL half of the §6.1.3 Subject / Aboutness axis (ADR-0028, card 8): resolving an
//!   explicit [`SubjectDeclaration`] under the caller's tenant RLS, and the Rust face of the ONE deterministic
//!   resolve hook (`private.link_memory_subjects` / `private.link_rollup_subjects`, migration 0154).
//! Depends-on: crates=[humaux-domain, serde_json, sqlx]; services=[PostgreSQL(role_gateway)
//!   r=[private.memory_subjects] w=[private.evidence_subjects, private.subject_keys, private.subject_roles,
//!   private.subjects] x=[private.link_memory_subjects, private.link_rollup_subjects]]; env=[];
//!   modules=[adapters::confirm_token_repo, adapters::postgres, domain::error, domain::identity, domain::subject]
//! Called-by: [adapters::affect_repo, adapters::consolidate_repo, adapters::distill_repo, adapters::memory_governance_repo, adapters::remember, gateway::mcp_application, gateway::memory, tests]
//! Invariants: [the only Rust caller of the SQL subject-link hook; declarations commit with the write they belong to
//!   or not at all; resolution never calls a model and an unknown id/key is InvalidInput]
//! Spec: Baseline §6.1.3; ADR-0028
//!
//! The hook has exactly one implementation (SQL) and this module is its only Rust caller. The
//! production memory writers all reach it through [`crate::distill_repo::insert_memory`] (the
//! single `INSERT INTO private.memory_records` — Distill hop, `memory.confirm`, `memory.correct`)
//! and [`crate::consolidate_repo::publish_rollup`] (rollups). A `remember.put` declaration is
//! resolved and recorded on the Evidence inside [`crate::remember::remember_in_txn`]
//! ([`resolve_declaration_in_txn`] + [`declare_evidence_in_txn`]); `memory.confirm` /
//! `memory.correct` resolve theirs inside their own confirmed transaction and apply them through
//! [`link_memory_in_txn`]. There is no post-commit declaration path: every declaration commits
//! with the write it belongs to or not at all. Nothing here calls a model: resolution is explicit
//! id → exact trusted key → structured inheritance, and an unknown id/key is `INVALID_INPUT` —
//! never an auto-registration (§6.1.3).
//!
//! The registry writes (`memory.subject_register` / `memory.subject_link_key`, ADR-0028 D-F) live
//! here too: [`register_subject`] and [`link_key`], plain `role_gateway` INSERTs under RLS.
//!
//! Every statement runs under the caller's role + RLS (the hook functions are SECURITY INVOKER):
//! a subject that belongs to another tenant is simply not there, so cross-tenant linking fails
//! exactly like an unknown id.

use humaux_domain::error::ErrorCode;
use humaux_domain::identity::AuthorizationScope;
use humaux_domain::subject::{
    SubjectDeclaration, SubjectId, SubjectKey, SubjectKeyKind, SubjectKind, SubjectLinkSource,
    SubjectRole,
};
use sqlx::Row;
use sqlx::types::Uuid;

use crate::confirm_token_repo;
use crate::postgres::RuntimeDbPool;

type Txn<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

/// One resolved link input: a registered subject id and the §6.1.3 rule that produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedSubject {
    pub subject_id: Uuid,
    pub source: SubjectLinkSource,
}

/// One registered subject as `memory.enumerate {subjects:true}` lists it and as the two registry
/// writes return it.
#[derive(Debug, Clone)]
pub struct SubjectListing {
    pub subject_id: Uuid,
    pub kind: SubjectKind,
    pub display_name: String,
    /// `(key_kind, key_value)` pairs registered for this subject.
    pub keys: Vec<(SubjectKeyKind, String)>,
    pub roles: Vec<SubjectRole>,
}

fn db_error(error: sqlx::Error) -> ErrorCode {
    match error {
        sqlx::Error::RowNotFound => ErrorCode::NotFound,
        sqlx::Error::Database(ref db) => match db.code().as_deref() {
            Some("42501") => ErrorCode::Forbidden,
            Some("23503") => ErrorCode::TenantBoundary,
            Some("23505" | "40001" | "40P01" | "55P03" | "23514") => ErrorCode::Conflict,
            Some("22023" | "22P02" | "22003") => ErrorCode::InvalidInput,
            _ => ErrorCode::Internal,
        },
        _ => ErrorCode::DependencyUnavailable,
    }
}

/// Rules 1 + 2 under the transaction's tenant RLS: every declared id must be a live (unmerged)
/// registered subject and every declared key must resolve through `private.subject_keys`.
/// Any miss — unknown, merged-away, or another tenant's — is `INVALID_INPUT` (§52.1); the
/// caller never learns which. Duplicates across ids/keys collapse to one link.
pub async fn resolve_declaration_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    declaration: &SubjectDeclaration,
) -> Result<Vec<ResolvedSubject>, ErrorCode> {
    let mut out: Vec<ResolvedSubject> = Vec::new();
    if !declaration.ids.is_empty() {
        let ids: Vec<Uuid> = declaration.ids.iter().map(|id| id.0).collect();
        let found: Vec<Uuid> = sqlx::query_scalar(
            "SELECT subject_id FROM private.subjects \
             WHERE tenant_id = $1 AND subject_id = ANY($2) AND merged_into IS NULL",
        )
        .bind(tenant_id)
        .bind(&ids)
        .fetch_all(&mut **txn)
        .await
        .map_err(db_error)?;
        if found.len() != ids.len() {
            return Err(ErrorCode::InvalidInput);
        }
        out.extend(ids.into_iter().map(|subject_id| ResolvedSubject {
            subject_id,
            source: SubjectLinkSource::Declared,
        }));
    }
    for key in &declaration.keys {
        let subject_id: Option<Uuid> = sqlx::query_scalar(
            "SELECT k.subject_id FROM private.subject_keys k \
             JOIN private.subjects s ON s.tenant_id = k.tenant_id AND s.subject_id = k.subject_id \
             WHERE k.tenant_id = $1 AND k.key_kind = $2 AND k.key_value = $3 \
               AND s.merged_into IS NULL",
        )
        .bind(tenant_id)
        .bind(key.kind.as_str())
        .bind(&key.value)
        .fetch_optional(&mut **txn)
        .await
        .map_err(db_error)?;
        let subject_id = subject_id.ok_or(ErrorCode::InvalidInput)?;
        if !out.iter().any(|r| r.subject_id == subject_id) {
            out.push(ResolvedSubject {
                subject_id,
                source: SubjectLinkSource::ExternalKey,
            });
        }
    }
    Ok(out)
}

fn split(resolved: &[ResolvedSubject]) -> (Vec<Uuid>, Vec<&'static str>) {
    resolved
        .iter()
        .map(|r| (r.subject_id, r.source.as_str()))
        .unzip()
}

/// The hook: `private.link_memory_subjects` for one memory in the caller's transaction, with the
/// caller's already-resolved explicit links (empty ⇒ rules 1/2 skipped, rule 3 still runs).
/// Returns the number of new link rows. Called by `distill_repo::insert_memory` for every memory.
pub async fn link_memory_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    memory_id: Uuid,
    resolved: &[ResolvedSubject],
) -> Result<i32, sqlx::Error> {
    let (ids, kinds) = split(resolved);
    let explicit = !ids.is_empty();
    sqlx::query_scalar("SELECT private.link_memory_subjects($1, $2, $3, $4)")
        .bind(tenant_id)
        .bind(memory_id)
        .bind(explicit.then_some(ids))
        .bind(explicit.then_some(kinds))
        .fetch_one(&mut **txn)
        .await
}

/// The hook's rollup face: `private.link_rollup_subjects` in the publish transaction. Called by
/// `consolidate_repo::publish_rollup` after the `memory_rollup_sources` rows exist.
pub async fn link_rollup_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    rollup_id: Uuid,
) -> Result<i32, sqlx::Error> {
    sqlx::query_scalar("SELECT private.link_rollup_subjects($1, $2)")
        .bind(tenant_id)
        .bind(rollup_id)
        .fetch_one(&mut **txn)
        .await
}

/// `remember.put`'s declaration carrier, in the Evidence's own transaction: one
/// `private.evidence_subjects` row per resolved subject with the rule that produced it (so the
/// Distill-born memory inherits it through the hook's rule 3a). Called by
/// [`crate::remember::remember_in_txn`] right after the Evidence row exists; empty ⇒ no-op.
pub async fn declare_evidence_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    evidence_id: Uuid,
    resolved: &[ResolvedSubject],
) -> Result<u64, sqlx::Error> {
    if resolved.is_empty() {
        return Ok(0);
    }
    let (ids, kinds) = split(resolved);
    sqlx::query(
        "INSERT INTO private.evidence_subjects (tenant_id, evidence_id, subject_id, source_kind) \
         SELECT $1, $2, s.id, s.kind FROM unnest($3::uuid[], $4::text[]) AS s(id, kind) \
         ON CONFLICT DO NOTHING",
    )
    .bind(tenant_id)
    .bind(evidence_id)
    .bind(&ids)
    .bind(&kinds)
    .execute(&mut **txn)
    .await
    .map(|r| r.rows_affected())
}

/// Copies `from_evidence`'s declarations onto `to_evidence` (same tenant). `memory.confirm`
/// materialises M from a NEW UserConfirmed Evidence E2 whose provenance is the candidate's
/// source Evidence; carrying the source's declaration onto E2 lets the ordinary hook (rule 3a)
/// link M without a second write path.
pub async fn inherit_evidence_declarations_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    from_evidence: Uuid,
    to_evidence: Uuid,
) -> Result<u64, sqlx::Error> {
    sqlx::query(
        "INSERT INTO private.evidence_subjects (tenant_id, evidence_id, subject_id, source_kind) \
         SELECT tenant_id, $3, subject_id, source_kind FROM private.evidence_subjects \
         WHERE tenant_id = $1 AND evidence_id = $2 \
         ON CONFLICT DO NOTHING",
    )
    .bind(tenant_id)
    .bind(from_evidence)
    .bind(to_evidence)
    .execute(&mut **txn)
    .await
    .map(|r| r.rows_affected())
}

/// The subject ids currently linked to one memory (ABOUT + MENTIONS), in link order.
pub async fn memory_subject_ids_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    memory_id: Uuid,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT subject_id FROM private.memory_subjects \
         WHERE tenant_id = $1 AND memory_id = $2 ORDER BY created_at, subject_id",
    )
    .bind(tenant_id)
    .bind(memory_id)
    .fetch_all(&mut **txn)
    .await
}

/// Live (unmerged) subjects of `tenant_id` with their keys and roles, newest first, under the
/// transaction's RLS; `only` narrows to one subject (the shape both registry writes return).
async fn select_subjects_in_txn(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    only: Option<Uuid>,
    limit: i64,
) -> Result<Vec<SubjectListing>, ErrorCode> {
    let rows = sqlx::query(
        "SELECT s.subject_id, s.kind, s.display_name, \
                coalesce((SELECT array_agg(k.key_kind || ':' || k.key_value ORDER BY k.created_at) \
                          FROM private.subject_keys k \
                          WHERE k.tenant_id = s.tenant_id AND k.subject_id = s.subject_id), '{}') AS keys, \
                coalesce((SELECT array_agg(r.role ORDER BY r.created_at) \
                          FROM private.subject_roles r \
                          WHERE r.tenant_id = s.tenant_id AND r.subject_id = s.subject_id), '{}') AS roles \
         FROM private.subjects s \
         WHERE s.tenant_id = $1 AND s.merged_into IS NULL \
           AND ($2::uuid IS NULL OR s.subject_id = $2) \
         ORDER BY s.created_at DESC, s.subject_id DESC LIMIT $3",
    )
    .bind(tenant_id)
    .bind(only)
    .bind(limit)
    .fetch_all(&mut **txn)
    .await
    .map_err(db_error)?;
    rows.into_iter()
        .map(|row| {
            let kind: String = row.try_get("kind").map_err(|_| ErrorCode::Internal)?;
            let keys: Vec<String> = row.try_get("keys").map_err(|_| ErrorCode::Internal)?;
            let roles: Vec<String> = row.try_get("roles").map_err(|_| ErrorCode::Internal)?;
            Ok(SubjectListing {
                subject_id: row.try_get("subject_id").map_err(|_| ErrorCode::Internal)?,
                kind: SubjectKind::parse(&kind).ok_or(ErrorCode::Internal)?,
                display_name: row
                    .try_get("display_name")
                    .map_err(|_| ErrorCode::Internal)?,
                keys: keys
                    .iter()
                    .map(|k| {
                        let (kind, value) = k.split_once(':').ok_or(ErrorCode::Internal)?;
                        Ok((
                            SubjectKeyKind::parse(kind).ok_or(ErrorCode::Internal)?,
                            value.to_owned(),
                        ))
                    })
                    .collect::<Result<Vec<_>, ErrorCode>>()?,
                roles: roles
                    .iter()
                    .map(|r| SubjectRole::parse(r).ok_or(ErrorCode::Internal))
                    .collect::<Result<Vec<_>, ErrorCode>>()?,
            })
        })
        .collect()
}

/// `memory.enumerate {subjects:true}`: the caller's tenant's live (unmerged) subjects with their
/// keys and roles, newest first, under RLS (another tenant's registry is simply absent).
pub async fn list_subjects(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    limit: i64,
) -> Result<Vec<SubjectListing>, ErrorCode> {
    // dep: PostgreSQL(role_gateway) — transaction entry for `list_subjects`
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    confirm_token_repo::set_authorization_local(&mut txn, auth).await?;
    let rows = select_subjects_in_txn(&mut txn, auth.tenant_id().0, None, limit).await?;
    txn.rollback().await.map_err(db_error)?;
    Ok(rows)
}

/// `memory.subject_register` (ADR-0028 D-F): one `private.subjects` row (+ one
/// `subject_roles` row per role) under the request's authenticated tenant, through
/// `role_gateway`'s own INSERT grant and RLS `WITH CHECK`. Returns the registered subject as
/// the listing shape. Not idempotent by design: two registrations are two subjects (a later
/// merge card owns dedup; an external key is what dedups today — see [`link_key`]).
pub async fn register_subject(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    kind: SubjectKind,
    display_name: &str,
    roles: &[SubjectRole],
) -> Result<SubjectListing, ErrorCode> {
    let tenant_id = auth.tenant_id().0;
    // dep: PostgreSQL(role_gateway) — transaction entry for `register_subject`
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    // ADR-0054 D-B: the write-scope recheck (principal + its one workspace) inside this txn.
    confirm_token_repo::set_write_authorization_local(&mut txn, auth).await?;
    let subject_id: Uuid = sqlx::query_scalar(
        "INSERT INTO private.subjects (tenant_id, kind, display_name) \
         VALUES ($1, $2, $3) RETURNING subject_id",
    )
    .bind(tenant_id)
    .bind(kind.as_str())
    .bind(display_name)
    .fetch_one(&mut *txn)
    .await
    .map_err(db_error)?;
    for role in roles {
        sqlx::query(
            "INSERT INTO private.subject_roles (tenant_id, subject_id, role) VALUES ($1, $2, $3)",
        )
        .bind(tenant_id)
        .bind(subject_id)
        .bind(role.as_str())
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;
    }
    let row = select_subjects_in_txn(&mut txn, tenant_id, Some(subject_id), 1)
        .await?
        .pop()
        .ok_or(ErrorCode::Internal)?;
    txn.commit().await.map_err(db_error)?;
    Ok(row)
}

/// `memory.subject_link_key` (ADR-0028 D-F): attaches an exact external key to a registered
/// subject of the caller's tenant. The subject is resolved through the same rule-1 path a
/// declaration uses (unknown / merged / another tenant's ⇒ `INVALID_INPUT`, no existence
/// oracle); a key already registered for the tenant is `CONFLICT` (0153 `UNIQUE(tenant_id,
/// key_kind, key_value)`). Returns the subject with its keys.
pub async fn link_key(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    subject_id: SubjectId,
    key: &SubjectKey,
) -> Result<SubjectListing, ErrorCode> {
    let tenant_id = auth.tenant_id().0;
    // dep: PostgreSQL(role_gateway) — transaction entry for `link_key`
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    // ADR-0054 D-B: the write-scope recheck (principal + its one workspace) inside this txn.
    confirm_token_repo::set_write_authorization_local(&mut txn, auth).await?;
    let target = SubjectDeclaration::new(vec![subject_id], Vec::new())?;
    resolve_declaration_in_txn(&mut txn, tenant_id, &target).await?;
    sqlx::query(
        "INSERT INTO private.subject_keys (tenant_id, subject_id, key_kind, key_value) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(tenant_id)
    .bind(subject_id.0)
    .bind(key.kind.as_str())
    .bind(&key.value)
    .execute(&mut *txn)
    .await
    .map_err(db_error)?;
    let row = select_subjects_in_txn(&mut txn, tenant_id, Some(subject_id.0), 1)
        .await?
        .pop()
        .ok_or(ErrorCode::Internal)?;
    txn.commit().await.map_err(db_error)?;
    Ok(row)
}

/// Wire-side parse of the optional `subject_ids` / `subject_keys` arguments shared by
/// `remember.put`, `memory.confirm` and `memory.correct` (contracts/mcp/*.schema.json). Absent
/// ⇒ an empty declaration; a malformed id, unknown key kind, blank value, duplicate or over-long
/// list ⇒ `INVALID_INPUT`. Kept here (not in the gateway) so every op parses one way.
pub fn parse_declaration(value: &serde_json::Value) -> Result<SubjectDeclaration, ErrorCode> {
    let ids = match value.get("subject_ids") {
        None => Vec::new(),
        Some(list) => list
            .as_array()
            .ok_or(ErrorCode::InvalidInput)?
            .iter()
            .map(|id| SubjectId::parse(id.as_str().ok_or(ErrorCode::InvalidInput)?))
            .collect::<Result<Vec<_>, _>>()?,
    };
    let keys = match value.get("subject_keys") {
        None => Vec::new(),
        Some(list) => list
            .as_array()
            .ok_or(ErrorCode::InvalidInput)?
            .iter()
            .map(|key| {
                let kind = key
                    .get("kind")
                    .and_then(serde_json::Value::as_str)
                    .and_then(SubjectKeyKind::parse)
                    .ok_or(ErrorCode::InvalidInput)?;
                let value = key
                    .get("value")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(ErrorCode::InvalidInput)?;
                humaux_domain::subject::SubjectKey::new(kind, value)
            })
            .collect::<Result<Vec<_>, _>>()?,
    };
    SubjectDeclaration::new(ids, keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_declaration_accepts_ids_and_keys_and_rejects_garbage() {
        let id = SubjectId::new();
        let ok = parse_declaration(&json!({
            "subject_ids": [id.0.to_string()],
            "subject_keys": [{"kind": "CRM", "value": "CRM-1001"}],
        }))
        .unwrap();
        assert_eq!(ok.ids, vec![id]);
        assert_eq!(ok.keys[0].value, "CRM-1001");
        assert!(parse_declaration(&json!({})).unwrap().is_empty());
        assert_eq!(
            parse_declaration(&json!({"subject_ids": ["nope"]})),
            Err(ErrorCode::InvalidInput)
        );
        assert_eq!(
            parse_declaration(&json!({"subject_keys": [{"kind": "crm", "value": "x"}]})),
            Err(ErrorCode::InvalidInput)
        );
        assert_eq!(
            parse_declaration(&json!({"subject_keys": [{"kind": "CRM", "value": ""}]})),
            Err(ErrorCode::InvalidInput)
        );
    }

    #[test]
    fn split_keeps_arrays_parallel() {
        let a = Uuid::now_v7();
        let b = Uuid::now_v7();
        let (ids, kinds) = split(&[
            ResolvedSubject {
                subject_id: a,
                source: SubjectLinkSource::Declared,
            },
            ResolvedSubject {
                subject_id: b,
                source: SubjectLinkSource::ExternalKey,
            },
        ]);
        assert_eq!(ids, vec![a, b]);
        assert_eq!(kinds, vec!["DECLARED", "EXTERNAL_KEY"]);
    }
}
