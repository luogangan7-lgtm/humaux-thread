//! §78.2 DB<->Rust contract for the card-22c task authorization (ADR-0046), plus the §25.4.B(1)
//! registry-dependency contract and the DB-side privilege boundary.
//!
//! What each block pins, and the failure mode it exists for:
//!
//! 1. **The authorization table's closed sets round-trip against the Rust enums.** `purpose`,
//!    `issuer_kind`, `scope_kind`, `mode` and `grant_authority` are CHECK-pinned in the
//!    database and enum-pinned in `humaux_domain::context`. Set equality with per-item
//!    round-trip, never `count == N` — a swapped pair keeps the count.
//! 2. **Revoke-only is a mechanism.** The trigger exists and is enabled, and the only
//!    column any runtime role can write is `revoked_at`.
//! 3. **The privilege boundary is real, not a comment.** `role_private_worker` — the
//!    background-distillation role §10.1 rule 2 names — gets `permission denied` on an INSERT
//!    here, while its ordinary read on the binding table still works (a role with no
//!    privileges at all would pass the negative half by accident).
//! 4. **`task_explicit_context_v2`'s registry dependencies name real relations**, include the
//!    authorization table, and still do not probe `memory_records.task_id` (§25.4.A(11)).
//! 5. **The reject-reason wire set is a §78.1 registered GOLDEN**, written out by hand — it is
//!    NOT derived from the production `TaskContextReject::wire`, because a test that calls the
//!    production mapping cannot notice the production mapping changing.
//!
//! Structure-only plus one SET ROLE probe in an aborted sub-transaction: this suite creates no
//! durable row and deletes nothing. Three-state per §79.2.

use humaux_domain::context::{
    BindingPurpose, REGISTRY, SelectorId, TASK_AUTHORIZATION_POLICY_VERSION, TASK_GRANT_AUTHORITY,
    TaskContextReject, TaskGrantIssuer,
};
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use std::collections::BTreeSet;

const NAME: &str = "task_grant_contract";
const TABLE: &str = "private.task_binding_grants";

/// §78.1 registered GOLDEN — the reject-reason wire set of §25.4.B(2), written by hand.
const REJECT_GOLDEN: [(TaskContextReject, &str); 10] = [
    (TaskContextReject::TaskNotCurrent, "TASK_NOT_CURRENT"),
    (
        TaskContextReject::BindingScopeMismatch,
        "BINDING_SCOPE_MISMATCH",
    ),
    (
        TaskContextReject::MissingTaskAuthorization,
        "MISSING_TASK_AUTHORIZATION",
    ),
    (
        TaskContextReject::GrantTargetMismatch,
        "GRANT_TARGET_MISMATCH",
    ),
    (
        TaskContextReject::TaskAuthorizationInactive,
        "TASK_AUTHORIZATION_INACTIVE",
    ),
    (TaskContextReject::TargetMissing, "TARGET_MISSING"),
    (TaskContextReject::TargetNotReadable, "TARGET_NOT_READABLE"),
    (
        TaskContextReject::UntrustedInstruction,
        "UNTRUSTED_INSTRUCTION",
    ),
    (
        TaskContextReject::TargetNotActiveOrGrounded,
        "TARGET_NOT_ACTIVE_OR_GROUNDED",
    ),
    (
        TaskContextReject::TargetRevisionChanged,
        "TARGET_REVISION_CHANGED",
    ),
];

fn client() -> Option<Client> {
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(NAME, "missing object: Postgres DSN", ExternalDep::Postgres);
        return None;
    };
    let Ok(client) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(NAME, "missing object: live Postgres", ExternalDep::Postgres);
        return None;
    };
    Some(client)
}

/// `None` when 0173 has not been applied — a three-state skip, never a silent pass.
fn require_table(client: &mut Client) -> Option<()> {
    let present: Option<String> = client
        .query_one("SELECT to_regclass($1)::text", &[&TABLE])
        .expect("regclass lookup")
        .get(0);
    if present.is_none() {
        skip_or_fail(
            NAME,
            "missing object: private.task_binding_grants — run `cargo xtask migrate` \
             (migrations/0173_task_binding_grants.sql)",
            ExternalDep::Postgres,
        );
        return None;
    }
    Some(())
}

/// The quoted literals of a deparsed CHECK, in order.
///
/// PostgreSQL deparses `CHECK (col IN ('A','B'))` as `col = ANY (ARRAY['A'::text, 'B'::text])`,
/// so a contract test that greps for `IN (` alone is a false red. Splitting on the quote
/// character reads both spellings without caring which one the server chose.
fn quoted_literals(def: &str) -> Vec<String> {
    def.split('\'')
        .skip(1)
        .step_by(2)
        .map(str::to_owned)
        .collect()
}

/// Every CHECK on the table, as deparsed text.
fn check_defs(client: &mut Client) -> Vec<String> {
    client
        .query(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
             WHERE conrelid = $1::text::regclass AND contype = 'c' ORDER BY conname",
            &[&TABLE],
        )
        .expect("check lookup")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

/// The literals of the one CHECK that mentions `column_name`, as a set.
fn closed_set_for(client: &mut Client, column_name: &str) -> BTreeSet<String> {
    check_defs(client)
        .into_iter()
        .filter(|def| def.contains(column_name))
        .flat_map(|def| quoted_literals(&def))
        .collect()
}

/// §25.4.B(2): the authorization's closed sets are the Rust enums, item by item.
#[test]
fn task_explicit_context_v2_positive_control_closed_sets_round_trip() {
    let Some(mut client) = client() else { return };
    let Some(()) = require_table(&mut client) else {
        return;
    };

    // purpose: DB labels == BindingPurpose::wire, both directions.
    let db_purposes = closed_set_for(&mut client, "purpose");
    let rust_purposes: BTreeSet<String> = [
        BindingPurpose::ReferenceOnly,
        BindingPurpose::AdoptTaskInstruction,
    ]
    .into_iter()
    .map(|p| p.wire().to_owned())
    .collect();
    // Only ADOPT_TASK_INSTRUCTION is storable: REFERENCE_ONLY is a wire value that writes NO
    // grant, so the DB set is deliberately the ONE-element subset. Assert that relationship
    // rather than set equality, and assert the missing member is exactly the reference one.
    assert_eq!(
        db_purposes,
        [BindingPurpose::AdoptTaskInstruction.wire().to_owned()]
            .into_iter()
            .collect::<BTreeSet<String>>(),
        "only ADOPT_TASK_INSTRUCTION may be stored as a grant purpose"
    );
    assert!(
        rust_purposes.contains(BindingPurpose::ReferenceOnly.wire()),
        "REFERENCE_ONLY must remain a wire value even though it stores no grant"
    );
    for label in &db_purposes {
        assert!(
            BindingPurpose::parse_wire(label).is_some(),
            "DB purpose label {label} has no Rust variant"
        );
    }

    // issuer_kind: full set equality, both directions.
    let db_issuers = closed_set_for(&mut client, "issuer_kind");
    let rust_issuers: BTreeSet<String> = [
        TaskGrantIssuer::AuthenticatedTaskRequest,
        TaskGrantIssuer::TenantPolicy,
    ]
    .into_iter()
    .map(|i| i.wire().to_owned())
    .collect();
    assert_eq!(db_issuers, rust_issuers, "issuer_kind closed set drifted");
    for label in &db_issuers {
        assert!(
            TaskGrantIssuer::parse_wire(label).is_some(),
            "DB issuer label {label} has no Rust variant"
        );
    }

    // scope_kind / mode are pinned to the single legal value each.
    assert_eq!(
        closed_set_for(&mut client, "scope_kind"),
        ["TASK".to_owned()]
            .into_iter()
            .collect::<BTreeSet<String>>()
    );
    assert_eq!(
        closed_set_for(&mut client, "mode"),
        ["MANDATORY".to_owned()]
            .into_iter()
            .collect::<BTreeSet<String>>()
    );

    // grant_authority is pinned to 6 — the one number this table exists to carry.
    assert!(
        check_defs(&mut client)
            .iter()
            .any(|def| def.contains("grant_authority") && def.contains("6")),
        "grant_authority must be CHECK-pinned to {TASK_GRANT_AUTHORITY}"
    );
    assert_eq!(
        TASK_GRANT_AUTHORITY, 6,
        "the Rust constant must be the same 6 the CHECK names"
    );

    // The frozen policy version is non-empty and the CHECK refuses an empty one.
    assert!(!TASK_AUTHORIZATION_POLICY_VERSION.is_empty());
    assert!(
        check_defs(&mut client)
            .iter()
            .any(|def| def.contains("policy_version")),
        "policy_version must be CHECK-constrained"
    );

    // The reject-reason wire set against the hand-written GOLDEN (§78.1).
    for (reason, wire) in REJECT_GOLDEN {
        assert_eq!(reason.wire(), wire, "reject reason wire drifted");
    }
    let wires: BTreeSet<&str> = REJECT_GOLDEN.iter().map(|(_, w)| *w).collect();
    assert_eq!(
        wires.len(),
        REJECT_GOLDEN.len(),
        "reject wires must be unique"
    );
}

/// §25.4.B: the negative half of the same contract — nothing in the database lets a grant be
/// re-pointed, revived, or written by a background role.
#[test]
fn task_explicit_context_v2_negative_control_authorization_cannot_be_forged() {
    let Some(mut client) = client() else { return };
    let Some(()) = require_table(&mut client) else {
        return;
    };

    // Revoke-only is a trigger, not a review rule.
    let trigger: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_trigger \
             WHERE tgrelid = $1::text::regclass AND tgname = 'task_grant_revoke_only_v2' \
               AND NOT tgisinternal AND tgenabled <> 'D'",
            &[&TABLE],
        )
        .expect("trigger lookup")
        .get(0);
    assert_eq!(
        trigger, 1,
        "task_grant_revoke_only_v2 must exist and be enabled"
    );

    // RLS is ENABLED *and* FORCED — the owner is not exempt.
    let rls = client
        .query_one(
            "SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = $1::text::regclass",
            &[&TABLE],
        )
        .expect("rls lookup");
    assert!(rls.get::<_, bool>(0) && rls.get::<_, bool>(1));

    // The §6.2.2 cell, read from the live ACL rather than from the doc: the only writable
    // column is `revoked_at`, nobody holds DELETE, and the six background/repair roles hold
    // nothing at all. `role_gateway` mints and revokes; `role_retrieval_worker` reads.
    let acl = client
        .query_one(
            "SELECT has_table_privilege('role_gateway',$1,'SELECT') \
                AND has_table_privilege('role_gateway',$1,'INSERT') \
                AND has_column_privilege('role_gateway',$1,'revoked_at','UPDATE') \
                AND NOT has_column_privilege('role_gateway',$1,'task_id','UPDATE') \
                AND NOT has_table_privilege('role_gateway',$1,'DELETE') \
                AND has_table_privilege('role_retrieval_worker',$1,'SELECT') \
                AND NOT has_table_privilege('role_retrieval_worker',$1,'INSERT') \
                AND NOT EXISTS (SELECT 1 FROM unnest(ARRAY['role_private_worker', \
                      'role_consolidation_worker','role_public_worker','role_batch_issuer', \
                      'role_maintenance','role_admin']) AS t(r) \
                    WHERE has_table_privilege(t.r,$1,'SELECT') \
                       OR has_table_privilege(t.r,$1,'INSERT') \
                       OR has_table_privilege(t.r,$1,'UPDATE') \
                       OR has_table_privilege(t.r,$1,'DELETE'))",
            &[&TABLE],
        )
        .expect("acl census")
        .get::<_, bool>(0);
    assert!(acl, "§6.2.2 cell for {TABLE} drifted from the live ACL");

    // The low-privilege boundary, attempted rather than asserted from the catalog. Inside an
    // aborted sub-transaction so nothing durable happens either way.
    let mut txn = client.transaction().expect("txn");
    txn.batch_execute("SET LOCAL ROLE role_private_worker")
        .expect("drop to the distillation role");
    // Positive control FIRST: the role is alive and can do its ordinary read. Without this, a
    // role with no privileges anywhere would pass the negative half by accident.
    let ordinary: i64 = txn
        .query_one("SELECT count(*) FROM private.context_bindings", &[])
        .expect("role_private_worker keeps its ordinary read")
        .get(0);
    assert!(ordinary >= 0);
    let denied = txn.execute(
        "INSERT INTO private.task_binding_grants \
           (tenant_id, context_binding_id, scope_kind, task_id, memory_id, mode, task_epoch, \
            payload_sha256, grant_authority, purpose, issuer_kind, issued_by_principal_id, \
            authorization_evidence_id, operation_id, policy_version) \
         VALUES (gen_random_uuid(), gen_random_uuid(), 'TASK', gen_random_uuid(), \
                 gen_random_uuid(), 'MANDATORY', 0, \
                 sha256(convert_to(gen_random_uuid()::text,'UTF8')), 6, \
                 'ADOPT_TASK_INSTRUCTION', 'AUTHENTICATED_TASK_REQUEST', gen_random_uuid(), \
                 gen_random_uuid(), gen_random_uuid(), 'x')",
        &[],
    );
    let error = denied.expect_err("a background role must not be able to mint an authorization");
    assert_eq!(
        error.code().map(postgres::error::SqlState::code),
        Some("42501"),
        "expected insufficient_privilege, got {error:?}"
    );
    drop(txn); // rolls back: SET LOCAL ROLE and the failed statement both disappear
}

/// §25.4.B(1)/(2) + §25.4.A(11): the registry's declared dependencies for the task selector.
#[test]
fn task_selector_registry_dependencies_name_the_authorization_relation() {
    let Some(mut client) = client() else { return };
    let Some(()) = require_table(&mut client) else {
        return;
    };

    let spec = REGISTRY
        .iter()
        .find(|s| s.id == SelectorId::TaskExplicitContextV1)
        .expect("the task selector is registered");
    assert_eq!(
        spec.registered_name, "task_explicit_context_v2",
        "the retired v1 name must not be reused for the new admission object"
    );

    let declared: BTreeSet<(&str, &str)> = spec
        .required_columns
        .iter()
        .map(|(schema, table, _, _)| (*schema, *table))
        .collect();
    assert!(
        declared.contains(&("private", "task_binding_grants")),
        "v2 admits by a verified authorization, so it must probe the authorization relation"
    );
    assert!(
        declared.contains(&("private", "context_bindings")),
        "the obligation still comes from the binding relation"
    );
    // §25.4.A(11): the forbidden probe, in both spellings that ever existed.
    assert!(
        !spec
            .required_columns
            .iter()
            .any(|(_, table, column, _)| *table == "memory_records" && *column == "task_id"),
        "probing memory_records.task_id is forbidden by §25.4.A(11)"
    );

    // Every declared column really exists, with the declared generated-ness.
    for (schema, table, column, stored_generated) in spec.required_columns {
        let found: Option<String> = client
            .query_opt(
                "SELECT a.attgenerated::text FROM pg_attribute a \
                   JOIN pg_class c ON c.oid = a.attrelid \
                   JOIN pg_namespace n ON n.oid = c.relnamespace \
                  WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3 \
                    AND a.attnum > 0 AND NOT a.attisdropped",
                &[schema, table, column],
            )
            .expect("attribute lookup")
            .map(|row| row.get(0));
        let attgenerated =
            found.unwrap_or_else(|| panic!("{schema}.{table}.{column} is declared but absent"));
        assert_eq!(
            *stored_generated,
            attgenerated == "s",
            "{schema}.{table}.{column} generated-ness disagrees with the registry"
        );
    }

    // The facets selector's own generated-column contract is still asserted elsewhere
    // (`facet_contract.rs`); here we only pin that this card did not quietly change it.
    let facets = REGISTRY
        .iter()
        .find(|s| s.id == SelectorId::RequiredCurrentStateFacetsV1)
        .expect("facets selector");
    assert!(
        facets
            .required_columns
            .iter()
            .any(|(_, table, column, stored)| *table == "memory_records"
                && *column == "facet"
                && *stored),
        "card 22b's stored-generated facet contract must survive card 22c"
    );
}

/// I-STORE, **DB half** (ADR-0046 D-A): `memory_records_stored_authority_v2_check` exists and
/// actually refuses a stored `ExplicitTaskContext`.
///
/// Why this is a separate test from `StoredAuthority::authorize`'s unit gate (ruling §七, last
/// paragraph): deleting the Rust guard while the database still refuses is **not** evidence the
/// Rust guard works, and deleting the DB constraint while Rust still refuses is not evidence the
/// constraint is there. Each half gets a directed test reachable only at its own layer.
///
/// The enforcement half is **attempted**, not read off the catalog: a catalog-only assertion
/// stays green against a constraint PostgreSQL would never actually evaluate. Everything happens
/// inside one sub-transaction that is rolled back, so nothing durable is written; the probe seeds
/// its own tenant rather than borrowing an existing row, so it behaves the same on a database
/// that holds none.
#[test]
fn stored_authority_check_refuses_explicit_task_context_in_the_database() {
    /// The probe's own tenant, addressed by name so neither insert needs a Rust-side uuid.
    const PROBE_TENANT: &str =
        "(SELECT tenant_id FROM control.tenants WHERE name = 'task-grant-contract probe')";

    let Some(mut client) = client() else { return };
    let Some(()) = require_table(&mut client) else {
        return;
    };

    // Present, and — since 0175 (post-delivery housekeeping, 2026-09-26) — VALID: the four
    // legacy fixture rows of ADR-0046 were backed up and deleted with the user's approval,
    // then `VALIDATE CONSTRAINT` ran (delivery report §7.3). Asserting `convalidated = true`
    // keeps "0175 applied" and "someone re-added it NOT VALID" distinguishable; a database
    // migrated only through 0173 fails here by design (the lane migrates to head).
    let constraint = client
        .query_opt(
            "SELECT pg_get_constraintdef(oid), convalidated FROM pg_constraint \
             WHERE conrelid = 'private.memory_records'::regclass \
               AND conname = 'memory_records_stored_authority_v2_check'",
            &[],
        )
        .expect("constraint lookup")
        .expect(
            "I-STORE: private.memory_records must carry memory_records_stored_authority_v2_check \
             (migrations/0173_task_binding_grants.sql)",
        );
    let def: String = constraint.get(0);
    assert!(
        def.contains("ExplicitTaskContext"),
        "the I-STORE constraint must name the class it refuses, got {def}"
    );
    assert!(
        constraint.get::<_, bool>(1),
        "0173 shipped this NOT VALID (four legacy rows, ADR-0046); 0175 validated it once the \
         rows were dispositioned — a NOT VALID constraint here means 0175 has not been applied"
    );

    let insert_at = |class: &str| {
        format!(
            "INSERT INTO private.memory_records \
               (tenant_id, memory_type, content, visibility_class, authority_class, confidence, \
                status, asserted_at) \
             VALUES ({PROBE_TENANT}, 'NOTE', '{{}}'::jsonb, 'TENANT_SHARED', '{class}', 1.0, \
                     'active', now())"
        )
    };

    let mut txn = client.transaction().expect("txn");
    txn.execute(
        "INSERT INTO control.tenants (name) VALUES ('task-grant-contract probe')",
        &[],
    )
    .expect("probe tenant");
    // Positive control FIRST: the very same insert at a legal stored class is accepted, so the
    // negative half below cannot go green because the row shape was wrong.
    match txn.execute(insert_at("ProjectConstraint").as_str(), &[]) {
        Ok(n) => assert_eq!(n, 1, "the control insert must write exactly one row"),
        Err(e) => panic!("a legal stored class must still be insertable, got {e:?}"),
    }
    let error = txn
        .execute(insert_at("ExplicitTaskContext").as_str(), &[])
        .expect_err(
            "I-STORE: the database must refuse a stored ExplicitTaskContext — 6 is the use \
             authority of a current task context instance, not a content property",
        );
    assert_eq!(
        error.code().map(postgres::error::SqlState::code),
        Some("23514"),
        "expected check_violation, got {error:?}"
    );
    drop(txn); // rolls back: the probe tenant and the control insert go with the refusal
}

/// Revoke-only is **attempted**, not read off `pg_trigger` (ruling §七: a mechanism asserted from
/// the catalog stays green against a trigger PostgreSQL would never actually run).
///
/// The grant this needs cannot be forged from thin air — the composite FK makes it agree with a
/// real binding, and the evidence FK with a real Evidence row — so the probe seeds the whole
/// chain (tenant, reasoning domain, user, target memory + its basis Evidence link, the
/// authorization Evidence, the TASK/MANDATORY binding, the grant) inside ONE transaction that is
/// always rolled back. Nothing durable is written and nothing is deleted.
///
/// Four arms, in this order so a false green is impossible:
/// 1. re-point the grant to another task generation  -> 23514
/// 2. extend its expiry                              -> 23514
/// 3. the ONE legal UPDATE (`revoked_at` NULL -> now) -> exactly one row  (positive control:
///    without it, a trigger that refused *every* UPDATE would pass arms 1/2/4 by accident)
/// 4. revoke an already-revoked grant                 -> 23514  (single-shot)
#[test]
fn task_grant_revoke_only_is_attempted_not_read_off_the_catalog() {
    /// The probe's own tenant, addressed by name so no insert needs a Rust-side uuid.
    const TENANT: &str =
        "(SELECT tenant_id FROM control.tenants WHERE name = 'task-grant revoke-only probe')";

    let Some(mut client) = client() else { return };
    let Some(()) = require_table(&mut client) else {
        return;
    };

    let mut txn = client.transaction().expect("txn");
    txn.batch_execute(&format!(
        "INSERT INTO control.tenants (name) VALUES ('task-grant revoke-only probe');
         INSERT INTO control.users (state) VALUES ('ACTIVE');
         INSERT INTO control.private_reasoning_domains (tenant_id, name)
           VALUES ({TENANT}, 'revoke-only probe domain');
         INSERT INTO private.memory_records
           (tenant_id, memory_type, content, visibility_class, authority_class, confidence,
            status, asserted_at)
           VALUES ({TENANT}, 'NOTE', '{{\"fixture\": \"revoke-only probe\"}}'::jsonb,
                   'TENANT_SHARED', 'UserCorrection', 1.0, 'active', now());
         -- two Evidence rows on purpose: the basis one is linked to the memory, the
         -- authorization one never is (ADR-0046 D-E: a task receipt is not an authority basis).
         INSERT INTO private.evidence_objects
           (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, visibility_class,
            reasoning_domain_id)
           SELECT {TENANT}, 'EVENT', sha256(convert_to(k, 'UTF8')), 'INTERNAL', 'UserConfirmed',
                  'TENANT_SHARED',
                  (SELECT reasoning_domain_id FROM control.private_reasoning_domains
                    WHERE tenant_id = {TENANT})
             FROM unnest(ARRAY['revoke-only basis', 'revoke-only authorization']) AS t(k);
         INSERT INTO private.memory_evidence
           (memory_id, evidence_id, role, ordinal, grounding_mode, recorded_version)
           SELECT m.memory_id, e.evidence_id, 'PRIMARY', 0, 'LIVE', 1
             FROM private.memory_records m, private.evidence_objects e
            WHERE m.tenant_id = {TENANT} AND e.tenant_id = {TENANT}
              AND e.payload_sha256 = sha256(convert_to('revoke-only basis', 'UTF8'));
         INSERT INTO private.context_bindings
           (tenant_id, memory_id, scope_kind, scope_id, mode, created_by)
           SELECT {TENANT}, m.memory_id, 'TASK', gen_random_uuid(), 'MANDATORY',
                  (SELECT user_id FROM control.users ORDER BY created_at DESC LIMIT 1)
             FROM private.memory_records m WHERE m.tenant_id = {TENANT};
         INSERT INTO private.task_binding_grants
           (tenant_id, context_binding_id, scope_kind, task_id, memory_id, mode, task_epoch,
            payload_sha256, grant_authority, purpose, issuer_kind, issued_by_principal_id,
            authorization_evidence_id, operation_id, policy_version)
           SELECT cb.tenant_id, cb.context_binding_id, 'TASK', cb.scope_id, cb.memory_id,
                  'MANDATORY', 0, sha256(convert_to('revoke-only payload', 'UTF8')), 6,
                  'ADOPT_TASK_INSTRUCTION', 'AUTHENTICATED_TASK_REQUEST',
                  (SELECT user_id FROM control.users ORDER BY created_at DESC LIMIT 1),
                  e.evidence_id, gen_random_uuid(), $$card22c.revoke-only.probe$$
             FROM private.context_bindings cb, private.evidence_objects e
            WHERE cb.tenant_id = {TENANT} AND e.tenant_id = {TENANT}
              AND e.payload_sha256 = sha256(convert_to('revoke-only authorization', 'UTF8'));"
    ))
    .expect("seed the probe's own authorization chain");

    let check_violation = |sql: &str, txn: &mut postgres::Transaction<'_>, what: &str| {
        let mut sp = txn.savepoint("arm").expect("savepoint");
        let error = sp
            .execute(sql, &[])
            .expect_err("a grant is revoke-only and otherwise immutable");
        assert_eq!(
            error.code().map(postgres::error::SqlState::code),
            Some("23514"),
            "{what}: expected check_violation from task_grant_revoke_only_v2, got {error:?}"
        );
        sp.rollback().expect("release the aborted savepoint");
    };

    // 1/2. Re-pointing and extending are not revocations.
    check_violation(
        "UPDATE private.task_binding_grants SET task_epoch = task_epoch + 1",
        &mut txn,
        "re-point to another task generation",
    );
    check_violation(
        "UPDATE private.task_binding_grants SET expires_at = statement_timestamp() + interval '1 day'",
        &mut txn,
        "extend the expiry",
    );
    // 3. Positive control: the one legal UPDATE really works.
    assert_eq!(
        txn.execute(
            "UPDATE private.task_binding_grants SET revoked_at = statement_timestamp() WHERE revoked_at IS NULL",
            &[]
        )
        .expect("revoking a live grant is the one legal UPDATE"),
        1,
        "the legal revoke must touch exactly one row"
    );
    // 4. And it is single-shot: a revoked grant is frozen, so nobody can revive or re-date it.
    check_violation(
        "UPDATE private.task_binding_grants SET revoked_at = statement_timestamp()",
        &mut txn,
        "re-revoke an already-revoked grant",
    );

    drop(txn); // rolls back: tenant, memory, Evidence, binding and grant all disappear
}
