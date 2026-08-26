//! T-card integration test for `migrations/0100_memory_evidence_grounding.sql` — §8.8's two
//! per-edge columns on `private.memory_evidence`, against a real Postgres.
//!
//! Three things only a live DB can settle, none of which `domain::grounding`'s own
//! `#[cfg(test)]` fixtures A–H can see (they compare compiled Rust against compiled Rust):
//!
//! 1. **The DEFAULT's semantics, not just its value.** Every `INSERT INTO
//!    private.memory_evidence` in this tree still lists only `(memory_id, evidence_id, role)`,
//!    so today's writers get the column's default. That row must read back as a LIVE edge with
//!    no recorded version, and *that pair* must derive `RECHECK_REQUIRED` — the grounding debt
//!    §8.8 wants for a row that cannot prove it is still on the same source version — never a
//!    free `CURRENT`.
//! 2. **§78.2 DB-vs-Rust reconciliation.** The CHECK's literal set vs
//!    [`GroundingMode`]'s variants, pulled from `pg_get_constraintdef` so a migration drifting
//!    the constraint out from under the Rust enum is caught. Same shape as
//!    `disclosure_ledger.rs::wire_strings_match_live_db_check_constraints`.
//! 3. **That the CHECK actually bites**, rather than merely appearing in the catalog.
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or migration 0100 not applied each print
//! a visible SKIP naming the missing object — none of them passes silently.

use humaux_domain::grounding::{
    EdgeOutcome, GroundingEdge, GroundingInputs, GroundingMode, GroundingStateKind,
    GroundingVersionToken, derive_grounding_state,
};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use uuid::Uuid;

/// The DB wire value for one [`GroundingMode`]. Exhaustive `match` on purpose: it is the one
/// place a fourth Rust variant breaks the build, forcing whoever adds it to also extend
/// [`ALL_MODES`] and ship the migration that widens the CHECK.
///
/// SCREAMING_SNAKE per §8.8's own state definitions and §11.10's census sentence ("重新
/// resolve 所有 `LIVE` grounding edge"); migration 0100 carries the same reasoning.
fn wire(mode: GroundingMode) -> &'static str {
    match mode {
        GroundingMode::Live => "LIVE",
        GroundingMode::Snapshot => "SNAPSHOT",
        GroundingMode::Immutable => "IMMUTABLE",
    }
}

/// Every variant of [`GroundingMode`] — the Rust side of the §78.2 comparison below.
const ALL_MODES: [GroundingMode; 3] = [
    GroundingMode::Live,
    GroundingMode::Snapshot,
    GroundingMode::Immutable,
];

/// Inverse of [`wire`]. Panics on an unknown value rather than defaulting: a row carrying a
/// mode this build does not know about is precisely the drift the §78.2 test guards, and
/// silently coercing it to `Live` would hide that at the one place it becomes observable.
fn mode_from_wire(raw: &str) -> GroundingMode {
    ALL_MODES
        .into_iter()
        .find(|m| wire(*m) == raw)
        .unwrap_or_else(|| panic!("unknown grounding_mode wire value in DB: {raw:?}"))
}

struct Handle {
    admin: Client,
    tenant_id: Uuid,
    evidence_id: Uuid,
    memory_id: Uuid,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup (repo CLAUDE.md hard rule ④), same shape as
        // `processing_runs_fingerprint_rerun.rs`'s `Handle::drop`. `memory_evidence` rows go
        // with their Memory (`ON DELETE CASCADE`, 0004③).
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM private.memory_records WHERE tenant_id = '{0}'; \
             DELETE FROM private.events WHERE event_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{0}'); \
             DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
             DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}'; \
             DELETE FROM control.tenants WHERE tenant_id = '{0}';",
            self.tenant_id
        ));
    }
}

struct MemoryEvidenceGroundingFixture;

impl DbIntegrationFixture for MemoryEvidenceGroundingFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let migrated: bool = admin
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                   WHERE table_schema = 'private' AND table_name = 'memory_evidence' \
                     AND column_name = 'grounding_mode')",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "private.memory_evidence.grounding_mode does not exist (migration \
                 0100_memory_evidence_grounding not applied) — run `cargo xtask migrate` \
                 against HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }

        seed(admin).map_err(DbFixtureSkipReason::IsolationSetupFailed)
    }
}

/// One tenant + one EVENT-kind Evidence + one Memory linked to it, the link inserted the way
/// every writer in this tree inserts one today — column list `(memory_id, evidence_id, role)`,
/// nothing about grounding. `TENANT_SHARED` keeps `memory_records_visibility_matches_class`
/// satisfied without seeding `control.users`/`workspaces` (same shortcut
/// `consolidate_snapshot.rs::seed_active_memories` takes).
fn seed(mut admin: Client) -> Result<Handle, String> {
    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
            &[&"memory_evidence_grounding.rs throwaway tenant"],
        )
        .map_err(|e| e.to_string())?
        .get(0);
    let reasoning_domain_id: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
             VALUES ($1, 'memory_evidence_grounding.rs domain') RETURNING reasoning_domain_id",
            &[&tenant_id],
        )
        .map_err(|e| e.to_string())?
        .get(0);
    let evidence_id: Uuid = admin
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', $2, 'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $3) \
             RETURNING evidence_id",
            &[&tenant_id, &vec![0u8; 32], &reasoning_domain_id],
        )
        .map_err(|e| e.to_string())?
        .get(0);
    admin
        .execute(
            "INSERT INTO private.events (event_id, event_kind, payload) \
             VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
            &[&evidence_id],
        )
        .map_err(|e| e.to_string())?;

    // §8.6's orphan-Memory check is DEFERRABLE INITIALLY DEFERRED — the link must land in the
    // *same* transaction as the Memory, which per-statement autocommit would not give.
    let mut txn = admin.transaction().map_err(|e| e.to_string())?;
    let memory_id: Uuid = txn
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id, memory_type, content, visibility_class, \
                authority_class, confidence, status, asserted_at) \
             VALUES ($1, 'NOTE', '{}'::jsonb, 'TENANT_SHARED', \
                     'PrivateKnowledge', 0.9, 'active', now()) \
             RETURNING memory_id",
            &[&tenant_id],
        )
        .map_err(|e| e.to_string())?
        .get(0);
    txn.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) \
         VALUES ($1, $2, 'PRIMARY')",
        &[&memory_id, &evidence_id],
    )
    .map_err(|e| e.to_string())?;
    txn.commit().map_err(|e| e.to_string())?;

    Ok(Handle {
        admin,
        tenant_id,
        evidence_id,
        memory_id,
    })
}

/// Reads one edge back as `domain::grounding` sees it.
fn read_edge(handle: &mut Handle, role: &str, outcome: EdgeOutcome) -> GroundingEdge {
    let row = handle
        .admin
        .query_one(
            "SELECT grounding_mode, recorded_version FROM private.memory_evidence \
             WHERE memory_id = $1 AND evidence_id = $2 AND role = $3",
            &[&handle.memory_id, &handle.evidence_id, &role],
        )
        .expect("read back the seeded grounding edge");
    let mode_raw: String = row.get(0);
    let recorded: Option<String> = row.get(1);
    GroundingEdge {
        mode: mode_from_wire(&mode_raw),
        recorded_version: recorded.map(GroundingVersionToken::new),
        outcome,
    }
}

/// An edge written by today's writers — which name no grounding column at all — must come back
/// LIVE with no recorded version, and derive `RECHECK_REQUIRED`: §8.8 does not let a row that
/// cannot prove it is still on the same source version keep the current-truth assumption.
///
/// The `SNAPSHOT`/`IMMUTABLE` defaults this rules out would have gone the other way — excluded
/// from the derivation entirely, hence vacuously `CURRENT` for every legacy edge.
#[test]
fn default_edge_is_live_without_version_and_derives_recheck_required() {
    run_db_fixture::<MemoryEvidenceGroundingFixture, _>(
        "default_edge_is_live_without_version_and_derives_recheck_required",
        |mut handle| {
            let resolved = EdgeOutcome::Resolved(GroundingVersionToken::new("whatever-head-is"));
            let edge = read_edge(&mut handle, "PRIMARY", resolved.clone());

            assert_eq!(edge.mode, GroundingMode::Live, "0100's DEFAULT is 'LIVE'");
            assert_eq!(
                edge.recorded_version, None,
                "a pre-0100 / grounding-unaware writer records no version token"
            );
            assert_eq!(
                derive_grounding_state(GroundingInputs::Edges(&[edge])).kind(),
                GroundingStateKind::RecheckRequired,
                "LIVE + no recorded version must be grounding debt, not a free CURRENT (§8.8)"
            );

            // Positive control: once a version is recorded and the resolver returns the same
            // token, the very same row derives CURRENT — without this, an implementation that
            // answered RECHECK_REQUIRED unconditionally would pass the assertion above.
            handle
                .admin
                .execute(
                    "UPDATE private.memory_evidence SET recorded_version = 'whatever-head-is' \
                     WHERE memory_id = $1 AND evidence_id = $2 AND role = 'PRIMARY'",
                    &[&handle.memory_id, &handle.evidence_id],
                )
                .expect("record a version token on the seeded edge");
            let rebound = read_edge(&mut handle, "PRIMARY", resolved);
            assert_eq!(
                rebound.recorded_version,
                Some(GroundingVersionToken::new("whatever-head-is")),
                "recorded_version round-trips as an opaque string"
            );
            assert_eq!(
                derive_grounding_state(GroundingInputs::Edges(&[rebound])).kind(),
                GroundingStateKind::Current
            );
        },
    );
}

/// §78.2 DB-vs-Rust contract: the CHECK's literal set is [`GroundingMode`]'s variant set.
/// Order-independent — the clause's literal order is not a frozen contract, only the set is.
#[test]
fn grounding_mode_check_matches_rust_enum_wire_strings() {
    run_db_fixture::<MemoryEvidenceGroundingFixture, _>(
        "grounding_mode_check_matches_rust_enum_wire_strings",
        |mut handle| {
            let row = handle
                .admin
                .query_one(
                    "SELECT pg_get_constraintdef(oid), convalidated FROM pg_constraint \
                     WHERE conrelid = to_regclass('private.memory_evidence') \
                       AND conname = 'memory_evidence_grounding_mode_check'",
                    &[],
                )
                .expect("memory_evidence_grounding_mode_check not found — 0100 dropped or drifted");
            let def: String = row.get(0);
            let convalidated: bool = row.get(1);
            assert!(
                convalidated,
                "0100's VALIDATE CONSTRAINT step did not run — the CHECK is enforced for new \
                 writes but no existing row was ever verified against it: {def}"
            );

            // Quoted literals sit at the odd positions of a split on `'` (the definition always
            // opens with non-literal SQL text before the first literal).
            let mut actual: Vec<&str> = def.split('\'').skip(1).step_by(2).collect();
            actual.sort_unstable();
            let mut expected: Vec<&str> = ALL_MODES.into_iter().map(wire).collect();
            expected.sort_unstable();
            assert_eq!(
                actual, expected,
                "grounding_mode's CHECK literals drifted from GroundingMode — db def: {def}"
            );
        },
    );
}

/// The CHECK bites at write time, and `recorded_version` really is nullable for a non-LIVE
/// edge (§8.8: SNAPSHOT/IMMUTABLE never participate, so they have nothing to record).
#[test]
fn unknown_mode_is_rejected_while_a_known_one_inserts_without_a_version() {
    run_db_fixture::<MemoryEvidenceGroundingFixture, _>(
        "unknown_mode_is_rejected_while_a_known_one_inserts_without_a_version",
        |mut handle| {
            let rejected = handle.admin.execute(
                "INSERT INTO private.memory_evidence \
                   (memory_id, evidence_id, role, grounding_mode) \
                 VALUES ($1, $2, 'SUPPORTING', 'HISTORIC')",
                &[&handle.memory_id, &handle.evidence_id],
            );
            assert!(
                rejected.is_err(),
                "'HISTORIC' is not a GroundingMode — the CHECK must reject it at write time, \
                 not merely exist in the catalog"
            );

            // Control: the same INSERT with a real mode succeeds, so the rejection above is the
            // constraint discriminating on the value, not the statement failing for some other
            // reason (PK, FK, RLS).
            handle
                .admin
                .execute(
                    "INSERT INTO private.memory_evidence \
                       (memory_id, evidence_id, role, grounding_mode) \
                     VALUES ($1, $2, 'SUPPORTING', 'SNAPSHOT')",
                    &[&handle.memory_id, &handle.evidence_id],
                )
                .expect("a known GroundingMode must insert, with recorded_version left NULL");

            let edge = read_edge(
                &mut handle,
                "SUPPORTING",
                EdgeOutcome::Resolved(GroundingVersionToken::new("head-moved-on")),
            );
            assert_eq!(edge.mode, GroundingMode::Snapshot);
            assert_eq!(edge.recorded_version, None);
            assert_eq!(
                derive_grounding_state(GroundingInputs::Edges(&[edge])).kind(),
                GroundingStateKind::Current,
                "a SNAPSHOT edge is excluded from the derivation however far its source moved"
            );
        },
    );
}
