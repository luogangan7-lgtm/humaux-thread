//! `maintenance::roles` — `roles rotate` and `deploy-check` (ADR-0059 D-E, D-F): where role passwords come from, how
//!   generated ones are printed, and which roles and variables each check covers.
//! Depends-on: crates=[humaux-adapters, rand, serde_json]; services=[PostgreSQL(owner), PostgreSQL(role_maintenance),
//!   PostgreSQL(any)]; env=[CARGO_MANIFEST_DIR, HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_MIGRATOR_PG_DSN, HUMAUX_ROLE_PASSWORD_ADMIN,
//!   HUMAUX_ROLE_PASSWORD_BATCH_ISSUER, HUMAUX_ROLE_PASSWORD_CONSOLIDATION_WORKER, HUMAUX_ROLE_PASSWORD_GATEWAY,
//!   HUMAUX_ROLE_PASSWORD_MAINTENANCE, HUMAUX_ROLE_PASSWORD_PRIVATE_WORKER, HUMAUX_ROLE_PASSWORD_PUBLIC_WORKER,
//!   HUMAUX_ROLE_PASSWORD_RETRIEVAL_WORKER]; modules=[adapters::postgres, adapters::role_hygiene, maintenance::main]
//! Called-by: [maintenance::main]
//! Invariants: [a generated password is printed exactly once, as `HUMAUX_ROLE_PASSWORD_<SUFFIX>=<value>` on stdout
//!   before the receipt, never in the receipt or on stderr; an env value is validated and refused by variable name
//!   only, and refused when it is built on the 0011 placeholder prefix; the placeholders of migration 0011 are parsed
//!   at run time and never copied into this crate; the migrator principal is never a rotation target]
//! Spec: Baseline §6.2.0; §57.1; §77; ADR-0059

use humaux_adapters::postgres::{
    MaintenanceDbPool, MigratorDbPool, ROLE_ADMIN, ROLE_BATCH_ISSUER, ROLE_CONSOLIDATION_WORKER,
    ROLE_GATEWAY, ROLE_MAINTENANCE, ROLE_MIGRATION_OWNER, ROLE_PRIVATE_WORKER, ROLE_PUBLIC_WORKER,
    ROLE_RETRIEVAL_WORKER,
};
use humaux_adapters::role_hygiene::{
    self, CheckResult, CheckStatus, Placeholders, RolePassword, RotateRequest,
};
use serde_json::{Value, json};

use crate::{Admin, Args, Failure, Output, Result, env};

/// The LOGIN roles migration 0011 creates; `--create-missing` provisions exactly these (ADR-0059 D-A).
const LOGIN_ROLES_0011: [&str; 7] = [
    ROLE_GATEWAY,
    ROLE_PRIVATE_WORKER,
    ROLE_CONSOLIDATION_WORKER,
    ROLE_PUBLIC_WORKER,
    ROLE_RETRIEVAL_WORKER,
    ROLE_BATCH_ISSUER,
    ROLE_MAINTENANCE,
];

/// The eight password roles (§6.2.0 minus the NOLOGIN owner): the default rotation set and the
/// non-owner set of the `schema_migrations_write` check.
fn password_roles() -> Vec<String> {
    LOGIN_ROLES_0011
        .iter()
        .chain([ROLE_ADMIN].iter())
        .map(|r| (*r).to_owned())
        .collect()
}

/// Minimum length of an operator-supplied role password (ADR-0059 D-E).
const MIN_PASSWORD_LEN: usize = 32;

/// `HUMAUX_ROLE_PASSWORD_<SUFFIX>`, SUFFIX = the role name without `role_`, upper-cased.
fn env_name(role: &str) -> String {
    format!(
        "HUMAUX_ROLE_PASSWORD_{}",
        role.strip_prefix("role_")
            .unwrap_or(role)
            .to_ascii_uppercase()
    )
}

/// Where one rotated password came from (receipt field `source`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Source {
    Env,
    Generated,
}

impl Source {
    fn as_str(self) -> &'static str {
        match self {
            Self::Env => "env",
            Self::Generated => "generated",
        }
    }
}

/// The new password of `role`: `HUMAUX_ROLE_PASSWORD_<SUFFIX>` when set (at least
/// [`MIN_PASSWORD_LEN`] URL-unreserved characters, so a DSN needs no encoding, and not built on the
/// repository placeholder prefix), else 32 random bytes hex-encoded. A refusal names the variable,
/// never the value.
fn password_for(
    role: &str,
    placeholders: &Placeholders,
    lookup: impl Fn(&str) -> Option<String>,
) -> std::result::Result<(RolePassword, Source), String> {
    let name = env_name(role);
    match lookup(&name) {
        Some(value) => {
            let unreserved = value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '~' | '-'));
            if value.chars().count() < MIN_PASSWORD_LEN || !unreserved {
                return Err(format!(
                    "{name}: must be at least {MIN_PASSWORD_LEN} characters from [A-Za-z0-9._~-]"
                ));
            }
            // ADR-0059 D-E: a rotation must never report success while a published value stays live.
            if placeholders.taints(&value) {
                return Err(format!(
                    "{name}: is built on the repository placeholder prefix of migration 0011; \
                     unset it to generate a value"
                ));
            }
            Ok((RolePassword::new(value), Source::Env))
        }
        None => {
            let bytes: [u8; 32] = rand::random();
            let hex = bytes.iter().map(|b| format!("{b:02x}")).collect();
            Ok((RolePassword::new(hex), Source::Generated))
        }
    }
}

/// ADR-0059 D-F: the eight `(role, placeholder)` pairs of migration 0011, parsed at run time so the
/// repository holds no second copy. Exactly eight distinct roles, or `Err` (parse drift).
fn parse_roles_sql(text: &str) -> std::result::Result<Vec<(String, RolePassword)>, String> {
    const CREATE: &str = "CREATE ROLE ";
    const PASSWORD: &str = " LOGIN PASSWORD '";
    let mut pairs: Vec<(String, RolePassword)> = Vec::new();
    for line in text.lines() {
        let Some(rest) = line.trim_start().strip_prefix(CREATE) else {
            continue;
        };
        let Some((role, tail)) = rest.split_once(PASSWORD) else {
            continue;
        };
        let value = tail
            .split_once('\'')
            .map(|(v, _)| v)
            .ok_or_else(|| format!("role {role}: unterminated password literal"))?;
        if pairs.iter().any(|(r, _)| r == role) {
            return Err(format!("role {role} appears twice"));
        }
        pairs.push((role.to_owned(), RolePassword::new(value.to_owned())));
    }
    if pairs.len() != 8 {
        return Err(format!(
            "{} placeholder roles, expected 8 (ADR-0059 D-F)",
            pairs.len()
        ));
    }
    Ok(pairs)
}

/// ADR-0059 D-F: the placeholder set of the `--roles-sql <0011>` file, parsed at run time.
fn placeholders_from(args: &Args) -> Result<Placeholders> {
    let path = args.required("--roles-sql")?;
    let text = std::fs::read_to_string(&path)
        .map_err(|e| Failure::Usage(format!("--roles-sql {path}: {e}")))?;
    let pairs = parse_roles_sql(&text)
        .map_err(|why| Failure::Infra(format!("roles-sql parse drift: {why}")))?;
    role_hygiene::derive_candidates(&pairs).ok_or_else(|| {
        Failure::Infra("roles-sql parse drift: a placeholder is not <P>_<role>".to_owned())
    })
}

/// The print-once lines for generated values (ADR-0059 D-E), in rotation order.
fn generated_lines(rotated: &[(String, RolePassword, Source)]) -> Vec<String> {
    rotated
        .iter()
        .filter(|(_, _, source)| *source == Source::Generated)
        .map(|(role, password, _)| format!("{}={}", env_name(role), password.expose()))
        .collect()
}

/// The rotation receipt: roles and sources, never a value.
// ponytail: the receipt is the only durable record (control.audit_events needs a tenant, roles are
// cluster-level, ADR-0059 L3); add a tenant-less ops audit stream if rotations must be queryable.
fn rotate_receipt(rotated: &[(String, RolePassword, Source)], admin: &Admin) -> Value {
    json!({
        "command": "roles rotate",
        "roles": rotated
            .iter()
            .map(|(role, _, source)| json!({ "role": role, "source": source.as_str() }))
            .collect::<Vec<_>>(),
        "owner_nologin": true,
        "actor": admin.actor,
        "reason": admin.reason,
        "ticket": admin.ticket,
        "trace_id": admin.trace_id,
    })
}

/// `roles rotate --roles-sql <0011> [--role R]... [--create-missing]` (ADR-0059 D-E): one
/// transaction through `HUMAUX_MIGRATOR_PG_DSN`; generated values printed once after COMMIT.
pub(crate) async fn rotate(args: &Args) -> Result<Output> {
    let admin = Admin::from(args)?;
    let create_missing = args.0.iter().any(|a| a == "--create-missing");
    let allowed: Vec<String> = if create_missing {
        LOGIN_ROLES_0011.iter().map(|r| (*r).to_owned()).collect()
    } else {
        password_roles()
    };
    let named: Vec<String> = args
        .0
        .windows(2)
        .filter(|w| w[0] == "--role")
        .map(|w| w[1].clone())
        .collect();
    if let Some(other) = named.iter().find(|r| !allowed.contains(r)) {
        return Err(Failure::Usage(format!(
            "--role {other}: not one of {allowed:?} (role_admin is created by migration 0110, \
             rotate it after migrate)"
        )));
    }
    let targets = if named.is_empty() { allowed } else { named };
    let placeholders = placeholders_from(args)?;
    let mut rotated = Vec::with_capacity(targets.len());
    for role in targets {
        let (password, source) =
            password_for(&role, &placeholders, |name| std::env::var(name).ok())
                .map_err(Failure::Usage)?;
        rotated.push((role, password, source));
    }
    let dsn = env("HUMAUX_MIGRATOR_PG_DSN")?;
    // dep: PostgreSQL(owner) — the migrate principal, the only one that may ALTER ROLE
    let pool = MigratorDbPool::connect(&dsn)
        .await
        .map_err(|e| Failure::Infra(format!("connect HUMAUX_MIGRATOR_PG_DSN: {e}")))?;
    let pairs: Vec<(String, RolePassword)> = rotated
        .iter()
        .map(|(role, password, _)| (role.clone(), password.clone()))
        .collect();
    role_hygiene::rotate_in_txn(
        &pool,
        &RotateRequest {
            targets: &pairs,
            owner: ROLE_MIGRATION_OWNER,
            create_missing,
        },
    )
    .await?;
    let mut output = Output::ok(rotate_receipt(&rotated, &admin));
    output.once = generated_lines(&rotated);
    Ok(output)
}

fn check_json(check: &CheckResult) -> Value {
    json!({ "name": check.name, "status": check.status.as_str(), "detail": check.detail })
}

/// A probe check that ran without the superuser target is at best `not_applicable` (§57.1).
fn without_superuser(check: CheckResult, superuser: Option<&String>) -> CheckResult {
    match (check.status, superuser) {
        (CheckStatus::Pass, None) => {
            CheckResult::not_applicable(check.name, "HUMAUX_MIGRATOR_PG_DSN")
        }
        _ => check,
    }
}

/// `deploy-check --roles-sql <0011>` (ADR-0059 D-F): five named checks, read-only, names only.
pub(crate) async fn deploy_check(args: &Args) -> Result<Output> {
    let placeholders = placeholders_from(args)?;
    let superuser = std::env::var("HUMAUX_MIGRATOR_PG_DSN")
        .ok()
        .and_then(|dsn| role_hygiene::dsn_user(&dsn));
    let mut probed: Vec<String> = password_roles();
    probed.push(ROLE_MIGRATION_OWNER.to_owned());
    probed.extend(superuser.clone());

    const MAINTENANCE_DSN: &str = "HUMAUX_MAINTENANCE_PG_DSN";
    let maintenance = std::env::var(MAINTENANCE_DSN)
        .ok()
        .filter(|v| !v.trim().is_empty());
    let mut checks = Vec::with_capacity(5);
    match maintenance {
        None => {
            for name in [
                "placeholder_login",
                "probe_valid",
                "owner_nologin",
                "schema_migrations_write",
            ] {
                checks.push(CheckResult::not_applicable(name, MAINTENANCE_DSN));
            }
        }
        Some(dsn) => {
            let probe = role_hygiene::probe_options(&dsn).ok_or_else(|| {
                Failure::Usage(format!("{MAINTENANCE_DSN}: not a PostgreSQL URL"))
            })?;
            // dep: PostgreSQL(role_maintenance) — catalog reads of the two grant checks
            let pool = MaintenanceDbPool::connect(&dsn)
                .await
                .map_err(|e| Failure::Infra(format!("connect {MAINTENANCE_DSN}: {e}")))?;
            let login =
                role_hygiene::check_placeholder_login(&probe, &probed, &placeholders).await?;
            checks.push(without_superuser(login, superuser.as_ref()));
            let valid = role_hygiene::check_probe_valid(&probe, &probed).await?;
            checks.push(without_superuser(valid, superuser.as_ref()));
            checks.push(role_hygiene::check_owner_nologin(&pool, ROLE_MIGRATION_OWNER).await?);
            checks
                .push(role_hygiene::check_schema_migrations_write(&pool, &password_roles()).await?);
        }
    }
    let env: Vec<(String, String)> = std::env::vars().collect();
    checks.push(role_hygiene::check_env_dsn_placeholders(
        &env,
        &placeholders,
    ));
    let refused = checks.iter().any(|c| c.status != CheckStatus::Pass);
    let mut output = Output::ok(json!({
        "command": "deploy-check",
        "checks": checks.iter().map(check_json).collect::<Vec<_>>(),
    }));
    output.refused = refused;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn random_token(len: usize) -> String {
        (0..len)
            .map(|_| {
                let b: u8 = rand::random::<u8>() % 36;
                char::from_digit(u32::from(b), 36).expect("base-36 digit")
            })
            .collect()
    }

    fn synthetic_0011(roles: &[&str], prefix: &str) -> String {
        roles
            .iter()
            .map(|r| {
                format!(
                    "  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{r}') THEN\n    \
                     CREATE ROLE {r} LOGIN PASSWORD '{prefix}_{r}'\n      NOSUPERUSER;\n  END IF;\n"
                )
            })
            .collect()
    }

    const EIGHT: [&str; 8] = [
        "role_a", "role_b", "role_c", "role_d", "role_e", "role_f", "role_g", "role_h",
    ];

    /// T11 (ADR-0059 D-F): exactly eight distinct roles parse; seven or a duplicate is drift.
    #[test]
    fn roles_sql_parse_requires_exactly_eight_distinct_roles() {
        let prefix = random_token(20);
        let pairs = parse_roles_sql(&synthetic_0011(&EIGHT, &prefix)).expect("eight roles");
        assert_eq!(pairs.len(), 8);
        assert_eq!(pairs[0].0, "role_a");
        assert_eq!(pairs[0].1.expose(), format!("{prefix}_role_a"));
        assert!(parse_roles_sql(&synthetic_0011(&EIGHT[..7], &prefix)).is_err());
        let mut dup = EIGHT;
        dup[7] = "role_a";
        assert!(parse_roles_sql(&synthetic_0011(&dup, &prefix)).is_err());
    }

    /// The real migration parses into eight pairs that follow the `P_<role>` convention.
    #[test]
    fn the_real_0011_parses_and_follows_the_convention() {
        let text = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../migrations/0011_roles_and_grants.sql"),
        )
        .expect("0011 body");
        let pairs = parse_roles_sql(&text).expect("eight roles");
        assert!(pairs.iter().all(|(r, _)| r.starts_with("role_")));
        assert!(role_hygiene::derive_candidates(&pairs).is_some());
    }

    /// Operator values are validated and refused by variable name; absent ones are generated.
    #[test]
    fn env_values_are_validated_by_name_and_absent_ones_generated() {
        let none = synthetic_placeholders();
        let good = random_token(40);
        let (password, source) = password_for("role_gateway", &none, |n| {
            (n == "HUMAUX_ROLE_PASSWORD_GATEWAY").then(|| good.clone())
        })
        .expect("valid env value");
        assert_eq!((password.expose(), source), (good.as_str(), Source::Env));
        for bad in [random_token(31), format!("{}@", random_token(40))] {
            let err =
                password_for("role_admin", &none, |_| Some(bad.clone())).expect_err("refused");
            assert!(err.starts_with("HUMAUX_ROLE_PASSWORD_ADMIN:"), "{err}");
            assert!(!err.contains(&bad));
        }
        let (a, source) = password_for("role_admin", &none, |_| None).expect("generated");
        let (b, _) = password_for("role_admin", &none, |_| None).expect("generated");
        assert_eq!(source, Source::Generated);
        assert_eq!(a.expose().len(), 64);
        assert_ne!(a, b);
    }

    fn synthetic_placeholders() -> Placeholders {
        let pairs = parse_roles_sql(&synthetic_0011(&EIGHT, &random_token(20))).expect("eight");
        role_hygiene::derive_candidates(&pairs).expect("convention")
    }

    /// ADR-0059 D-E (review P1): an env value equal to a published placeholder, or built on its prefix
    /// `P`, is refused naming the variable, so a rotation can never succeed while `P_<role>` stays live.
    #[test]
    fn env_value_built_on_a_placeholder_is_refused() {
        let prefix = random_token(28);
        let pairs = parse_roles_sql(&synthetic_0011(&EIGHT, &prefix)).expect("eight");
        let placeholders = role_hygiene::derive_candidates(&pairs).expect("convention");
        let published = format!("{prefix}_role_a");
        for value in [
            published.clone(),
            format!("{prefix}_role_gateway"),
            format!("{prefix}{}", random_token(12)),
            format!("{}{prefix}", random_token(12)),
        ] {
            assert!(
                value.chars().count() >= MIN_PASSWORD_LEN,
                "test value passes the length rule"
            );
            let err = password_for("role_a", &placeholders, |_| Some(value.clone()))
                .expect_err("placeholder-derived value refused");
            assert!(err.starts_with("HUMAUX_ROLE_PASSWORD_A:"), "{err}");
            assert!(!err.contains(&prefix), "a refusal never prints the value");
        }
    }

    /// T12 (ADR-0059 D-E): each generated value appears exactly once in stdout (its print-once line)
    /// and never in the receipt; an env-sourced value appears nowhere.
    #[test]
    fn rotate_output_prints_each_generated_secret_once() {
        let rotated = vec![
            (
                "role_gateway".to_owned(),
                RolePassword::new(random_token(64)),
                Source::Generated,
            ),
            (
                "role_admin".to_owned(),
                RolePassword::new(random_token(40)),
                Source::Env,
            ),
        ];
        let admin = Admin {
            actor: "t".to_owned(),
            reason: "t".to_owned(),
            ticket: "t".to_owned(),
            trace_id: "t".to_owned(),
            step_up: "t".to_owned(),
        };
        let mut output = Output::ok(rotate_receipt(&rotated, &admin));
        output.once = generated_lines(&rotated);
        let stdout = output.render();
        let receipt = output.receipt.to_string();
        let generated = rotated[0].1.expose();
        assert_eq!(stdout.matches(generated).count(), 1, "printed once");
        assert_eq!(
            stdout
                .lines()
                .filter(|l| *l == format!("HUMAUX_ROLE_PASSWORD_GATEWAY={generated}"))
                .count(),
            1
        );
        assert_eq!(receipt.matches(generated).count(), 0);
        assert_eq!(stdout.matches(rotated[1].1.expose()).count(), 0);
        assert!(receipt.contains("\"source\":\"generated\""), "{receipt}");
    }
}
