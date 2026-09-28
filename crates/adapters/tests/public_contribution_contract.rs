//! `adapters::tests::public_contribution_contract` — §78.2 DB↔Rust contract tests for the §12 public-contribution
//!   closed sets (migration 0103 vs `domain::public`).
//! Depends-on: crates=[humaux-domain, humaux-testkit, postgres]; services=[PostgreSQL(any)
//!   r=[staging.contribution_releases]]; env=[HUMAUX_TEST_PG_DSN]; modules=[domain::public, humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [reads the live CHECK definitions and asserts every Rust variant appears; ContributionPolicy::Disabled
//!   is a legal policy but must not be a legal release snapshot; a missing DB goes through skip_or_fail]
//! Spec: Baseline §78.2; §12; §12.1; §79.2
//!
//! Same pattern as `tenant_placements_migration.rs`: read the live CHECK
//! constraint definition via `pg_get_constraintdef`, assert every Rust variant's `as_db_str`
//! appears — Rust-side renames or DB-side edits can no longer drift apart silently (the exact
//! gap the wave-1 review named: five new closed sets with consistency held only by a manual
//! grep).
//!
//! One deliberate asymmetry, pinned by its own test below: `ContributionPolicy::Disabled` is a
//! legal POLICY value (the config space is three) but must NOT appear in
//! `contribution_releases_policy_closed` — a release row records a release that happened, and
//! §12.1 forbids releasing under DISABLED, so the legal RELEASE-snapshot space is two (see the
//! 0103 constraint comment; `ContributionRelease::release` rejects it type-side).
//!
//! Three-state discipline (§79.2): `testkit::skip_or_fail`; CI declares `HUMAUX_REQUIRE_DB=1`.

use humaux_domain::public::{ContributionPolicy, ModerationState, PublicSourceType, ScanOutcome};
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};

const NAME: &str = "public_contribution_contract";

fn connect() -> Option<Client> {
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(NAME, "missing object: Postgres DSN", ExternalDep::Postgres);
        return None;
    };
    // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
    let Ok(mut admin) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(NAME, "missing object: live Postgres", ExternalDep::Postgres);
        return None;
    };
    // 0103 is the structure this whole contract sits on (ADR-0006 carve-out: the table the
    // tested judgment lives in, not the judgment itself).
    let applied: bool = admin
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
             WHERE table_schema='staging' AND table_name='contribution_releases' \
               AND column_name='policy_snapshot')",
            &[],
        )
        .ok()?
        .get(0);
    if !applied {
        skip_or_fail(
            NAME,
            "missing object: staging.contribution_releases.policy_snapshot — run \
             `cargo xtask migrate` (migrations/0103)",
            ExternalDep::Postgres,
        );
        return None;
    }
    Some(admin)
}

fn constraint_def(admin: &mut Client, conname: &str) -> String {
    admin
        .query_one(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = $1",
            &[&conname],
        )
        .unwrap_or_else(|_| panic!("constraint {conname} must exist (migrations/0103)"))
        .get(0)
}

/// `public.sources.source_type` CHECK enumerates exactly `PublicSourceType::ALL`.
#[test]
fn source_type_check_matches_rust_enum() {
    let Some(mut admin) = connect() else { return };
    let def = constraint_def(&mut admin, "sources_source_type_closed");
    for v in PublicSourceType::ALL {
        assert!(
            def.contains(v.as_db_str()),
            "CHECK def `{def}` missing Rust variant `{}`",
            v.as_db_str()
        );
    }
}

/// `public.claims.moderation_state` CHECK enumerates exactly `ModerationState::ALL`.
#[test]
fn moderation_state_check_matches_rust_enum() {
    let Some(mut admin) = connect() else { return };
    let def = constraint_def(&mut admin, "claims_moderation_state_check");
    for v in ModerationState::ALL {
        assert!(
            def.contains(v.as_db_str()),
            "CHECK def `{def}` missing Rust variant `{}`",
            v.as_db_str()
        );
    }
}

/// Both scan-outcome CHECKs enumerate exactly `ScanOutcome::ALL`.
#[test]
fn scan_outcome_checks_match_rust_enum() {
    let Some(mut admin) = connect() else { return };
    for conname in [
        "contribution_releases_privacy_scan_outcome_check",
        "contribution_releases_secret_scan_outcome_check",
    ] {
        let def = constraint_def(&mut admin, conname);
        for v in ScanOutcome::ALL {
            assert!(
                def.contains(v.as_db_str()),
                "{conname} def `{def}` missing `{}`",
                v.as_db_str()
            );
        }
    }
}

/// The release-snapshot policy CHECK vs `ContributionPolicy`: `Manual` and
/// `AutoAfterUserDistillation` must appear; `Disabled` must NOT (module-doc asymmetry — the
/// policy CONFIG space is three, the legal RELEASE-snapshot space is two). This is also the
/// regression pin for the wave-1 review blocker: the def must carry the `IS NOT NULL`
/// conjunct, or a `'{}'::jsonb` snapshot slips through CHECK's UNKNOWN-passes semantics
/// (reproduced live before the fix).
#[test]
fn release_policy_check_is_null_safe_and_excludes_disabled() {
    let Some(mut admin) = connect() else { return };
    let def = constraint_def(&mut admin, "contribution_releases_policy_closed");
    assert!(
        def.contains(ContributionPolicy::Manual.as_db_str())
            && def.contains(ContributionPolicy::AutoAfterUserDistillation.as_db_str()),
        "release policy CHECK `{def}` must admit the two releasable policies"
    );
    assert!(
        !def.contains(ContributionPolicy::Disabled.as_db_str()),
        "release policy CHECK `{def}` must NOT admit DISABLED (§12.1: no release under \
         DISABLED — a DISABLED snapshot row is a contradiction)"
    );
    assert!(
        def.contains("IS NOT NULL"),
        "release policy CHECK `{def}` lost its IS NOT NULL conjunct — '{{}}'::jsonb would \
         pass via CHECK's UNKNOWN semantics (wave-1 review blocker)"
    );
}

/// `staging.contribution_releases.state` CHECK enumerates the release lifecycle
/// (`ReleaseState::ALL` on the Rust side).
#[test]
fn release_state_check_matches_rust_enum() {
    let Some(mut admin) = connect() else { return };
    let def = constraint_def(&mut admin, "contribution_releases_state_check");
    for v in humaux_domain::public::ReleaseState::ALL {
        assert!(
            def.contains(v.as_db_str()),
            "state CHECK def `{def}` missing `{}`",
            v.as_db_str()
        );
    }
}
