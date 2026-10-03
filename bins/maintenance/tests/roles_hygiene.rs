//! `maintenance::tests::roles_hygiene` — real-PostgreSQL tests of role rotation and the placeholder probe
//!   (ADR-0059 D-A, D-E, D-F) on throwaway cluster roles.
//! Depends-on: crates=[humaux-adapters, humaux-testkit, postgres, rand, tokio]; services=[PostgreSQL(owner), PostgreSQL(any)];
//!   env=[HUMAUX_TEST_PG_DSN]; modules=[adapters::postgres, adapters::role_hygiene, humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [every role a test touches is named hx33_<pid>_<random>_<suffix>, created by the test and dropped by
//!   the fixture's Drop even on panic; no role_* role, grant or function is altered; every password is generated
//!   at run time and never printed]
//! Spec: Baseline §6.2.0; §79.2; ADR-0059
//!
//! Roles are cluster-global, so these tests never touch the frozen `role_*` set: the injected
//! placeholder list is a generated `P_<role>` set over throwaway role names.

use humaux_adapters::postgres::MigratorDbPool;
use humaux_adapters::role_hygiene::{self, CheckStatus, Placeholders, RolePassword, RotateRequest};
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::error::SqlState;
use postgres::{Client, NoTls};

const OWNER_DSN: &str = "HUMAUX_TEST_PG_DSN";

fn random_token(len: usize) -> String {
    (0..len)
        .map(|_| char::from_digit(rand::random_range(0..36u32), 36).expect("base-36 digit"))
        .collect()
}

fn password() -> RolePassword {
    RolePassword::new(random_token(40))
}

/// Throwaway roles plus the superuser DSN; drops every role it named, even on panic.
struct Fixture {
    dsn: String,
    stem: String,
    rt: tokio::runtime::Runtime,
}

impl Fixture {
    fn new(test: &str) -> Option<Self> {
        let Ok(dsn) = std::env::var(OWNER_DSN) else {
            skip_or_fail(
                test,
                "missing object: HUMAUX_TEST_PG_DSN",
                ExternalDep::Postgres,
            );
            return None;
        };
        Some(Self {
            dsn,
            stem: format!("hx33_{}_{}", std::process::id(), random_token(8)),
            rt: tokio::runtime::Runtime::new().expect("runtime"),
        })
    }

    fn role(&self, suffix: &str) -> String {
        format!("{}_{suffix}", self.stem)
    }

    fn migrator(&self) -> MigratorDbPool {
        // dep: PostgreSQL(owner) — the rotation principal (superuser from HUMAUX_TEST_PG_DSN)
        self.rt
            .block_on(MigratorDbPool::connect(&self.dsn))
            .expect("HUMAUX_TEST_PG_DSN is a superuser outside the §6.2.0 set")
    }

    fn rotate(
        &self,
        pool: &MigratorDbPool,
        targets: &[(String, RolePassword)],
        create_missing: bool,
    ) -> Result<(), String> {
        let owner = self.role("own");
        self.rt
            .block_on(role_hygiene::rotate_in_txn(
                pool,
                &RotateRequest {
                    targets,
                    owner: &owner,
                    create_missing,
                },
            ))
            .map_err(|e| e.to_string())
    }

    /// `Ok(())` on a successful login as `role`, else the SQLSTATE (or a non-server error label).
    fn login(&self, role: &str, password: &RolePassword) -> Result<(), String> {
        let mut config: postgres::Config = self.dsn.parse().expect("postgres URL");
        config.user(role).password(password.expose());
        // dep: PostgreSQL(any) — one login attempt as a throwaway role
        match config.connect(NoTls) {
            Ok(_) => Ok(()),
            Err(e) => Err(e
                .code()
                .map_or_else(|| "no-sqlstate".to_owned(), |c| c.code().to_owned())),
        }
    }

    fn probe_login(&self, roles: &[String], placeholders: &Placeholders) -> (CheckStatus, String) {
        let probe = role_hygiene::probe_options(&self.dsn).expect("probe path");
        let r = self
            .rt
            .block_on(role_hygiene::check_placeholder_login(
                &probe,
                roles,
                placeholders,
            ))
            .expect("server answered every probe");
        (r.status, r.detail)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // dep: PostgreSQL(owner) — drops this test's throwaway roles
        if let Ok(mut admin) = Client::connect(&self.dsn, NoTls) {
            let roles: Vec<String> = admin
                .query(
                    "SELECT rolname::text FROM pg_roles WHERE starts_with(rolname, $1)",
                    &[&format!("{}_", self.stem)],
                )
                .map(|rows| rows.iter().map(|r| r.get(0)).collect())
                .unwrap_or_default();
            for role in roles {
                if let Err(e) = admin.batch_execute(&format!("DROP ROLE IF EXISTS \"{role}\"")) {
                    eprintln!("roles_hygiene cleanup: DROP ROLE {role} failed: {e}");
                }
            }
        }
    }
}

/// The path must verify passwords, or every refusal below would be vacuous (D-F `probe_valid`).
fn assert_password_path(f: &Fixture, role: &str) {
    assert_eq!(
        f.login(role, &password()),
        Err(SqlState::INVALID_PASSWORD.code().to_owned()),
        "the test path must check passwords (28P01 for a random one)"
    );
}

/// T7 (card acceptance): placeholder roles are named, rotation turns the check green, and re-setting
/// one role to its placeholder names exactly that role.
#[test]
fn deploy_check_names_each_placeholder_role_then_green_after_rotate() {
    let Some(f) = Fixture::new("deploy_check_names_each_placeholder_role_then_green_after_rotate")
    else {
        return;
    };
    let (a, b) = (f.role("a"), f.role("b"));
    let prefix = random_token(24);
    let pairs: Vec<(String, RolePassword)> = [&a, &b]
        .iter()
        .map(|r| ((*r).clone(), RolePassword::new(format!("{prefix}_{r}"))))
        .collect();
    let placeholders = role_hygiene::derive_candidates(&pairs).expect("P_<role> convention");
    let pool = f.migrator();
    f.rotate(&pool, &pairs, true)
        .expect("create both with placeholders");
    assert_password_path(&f, &a);
    let roles = vec![a.clone(), b.clone()];

    let (status, detail) = f.probe_login(&roles, &placeholders);
    assert_eq!(status, CheckStatus::Fail, "{detail}");
    assert!(detail.contains(&format!("role {a}:")) && detail.contains(&format!("role {b}:")));

    let fresh = vec![(a.clone(), password()), (b.clone(), password())];
    f.rotate(&pool, &fresh, false).expect("rotate both");
    let (status, detail) = f.probe_login(&roles, &placeholders);
    assert_eq!(status, CheckStatus::Pass, "{detail}");

    f.rotate(&pool, &pairs[1..], false)
        .expect("re-set b to its placeholder");
    let (status, detail) = f.probe_login(&roles, &placeholders);
    assert_eq!(status, CheckStatus::Fail, "{detail}");
    assert_eq!(detail.matches("role ").count(), 1, "{detail}");
    assert!(detail.contains(&format!("role {b}:")), "{detail}");
    assert!(!detail.contains(&prefix));
}

/// T8 (ADR-0059 D-E): a rotation that meets a missing role changes nothing, including the roles
/// before it in the list.
#[test]
fn rotate_is_one_transaction() {
    let Some(f) = Fixture::new("rotate_is_one_transaction") else {
        return;
    };
    let a = f.role("a");
    let old = password();
    let pool = f.migrator();
    f.rotate(&pool, &[(a.clone(), old.clone())], true)
        .expect("create a");
    let err = f
        .rotate(
            &pool,
            &[(a.clone(), password()), (f.role("missing"), password())],
            false,
        )
        .expect_err("a missing role refuses the whole rotation");
    assert!(
        err.contains(&format!("missing object: role {}", f.role("missing"))),
        "{err}"
    );
    assert_eq!(f.login(&a, &old), Ok(()), "a keeps its old password");
}

/// T9 (ADR-0059 D-E): after a rotation the new password logs in and the old one is refused (28P01).
#[test]
fn rotate_new_password_logs_in_old_refused() {
    let Some(f) = Fixture::new("rotate_new_password_logs_in_old_refused") else {
        return;
    };
    let a = f.role("a");
    let (old, new) = (password(), password());
    let pool = f.migrator();
    f.rotate(&pool, &[(a.clone(), old.clone())], true)
        .expect("create a");
    f.rotate(&pool, &[(a.clone(), new.clone())], false)
        .expect("rotate a");
    assert_eq!(f.login(&a, &new), Ok(()));
    assert_eq!(
        f.login(&a, &old),
        Err(SqlState::INVALID_PASSWORD.code().to_owned())
    );
}

/// T28 (ADR-0059 D-A): `--create-missing` makes a LOGIN role that accepts only its new value, and the
/// owner-like role NOLOGIN without a verifier, so any password is refused with 28P01.
#[test]
fn create_missing_makes_login_with_verifier_and_owner_nologin() {
    let Some(f) = Fixture::new("create_missing_makes_login_with_verifier_and_owner_nologin") else {
        return;
    };
    let rt_role = f.role("rt");
    let value = password();
    let pool = f.migrator();
    f.rotate(&pool, &[(rt_role.clone(), value.clone())], true)
        .expect("create-missing");
    assert_eq!(f.login(&rt_role, &value), Ok(()));
    assert_password_path(&f, &rt_role);

    let owner = f.role("own");
    // dep: PostgreSQL(owner) — catalog read of the created owner-like role
    let mut admin = Client::connect(&f.dsn, NoTls).expect("superuser");
    let login: bool = admin
        .query_one(
            "SELECT rolcanlogin FROM pg_roles WHERE rolname = $1",
            &[&owner],
        )
        .expect("owner-like role exists")
        .get(0);
    assert!(!login, "owner-like role is NOLOGIN");
    assert_eq!(
        f.login(&owner, &value),
        Err(SqlState::INVALID_PASSWORD.code().to_owned()),
        "no stored verifier: every password is refused"
    );
}

/// ADR-0059 D-E: the rotating principal never rotates itself, and a LOGIN owner refuses rotation.
#[test]
fn rotate_refuses_the_principal_and_a_login_owner() {
    let Some(f) = Fixture::new("rotate_refuses_the_principal_and_a_login_owner") else {
        return;
    };
    let pool = f.migrator();
    let principal = role_hygiene::dsn_user(&f.dsn).expect("user in HUMAUX_TEST_PG_DSN");
    f.rotate(&pool, &[(f.role("a"), password())], true)
        .expect("create a and the NOLOGIN owner");
    let err = f
        .rotate(&pool, &[(principal.clone(), password())], false)
        .expect_err("principal is never a target");
    assert!(
        err.contains(&format!("role {principal} is not a rotation target")),
        "{err}"
    );

    // dep: PostgreSQL(owner) — make the throwaway owner LOGIN to drive the refusal
    let mut admin = Client::connect(&f.dsn, NoTls).expect("superuser");
    admin
        .batch_execute(&format!("ALTER ROLE \"{}\" LOGIN", f.role("own")))
        .expect("throwaway owner LOGIN");
    let err = f
        .rotate(&pool, &[(f.role("a"), password())], false)
        .expect_err("a LOGIN owner refuses");
    assert!(err.contains("ADR-0059 D-A requires NOLOGIN"), "{err}");
}
