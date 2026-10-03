//! `adapters::role_hygiene` — deploy-time PostgreSQL role credentials: client-side SCRAM verifiers, one-transaction
//!   rotation, the read-only deploy-check probes (ADR-0059 D-A, D-E, D-F), and opening/closing the API-key pepper
//!   rehash window (D-H).
//! Depends-on: crates=[base64, hmac, rand, sha2, sqlx]; services=[PostgreSQL(owner), PostgreSQL(role_maintenance)
//!   r=[ops.schema_migrations] x=[control.credential_pepper_epoch_advance, control.credential_pepper_epoch_close], PostgreSQL(any)]; env=[]; modules=[adapters::postgres, adapters::provisioning]
//! Called-by: [maintenance::main, maintenance::roles, tests]
//! Invariants: [a plaintext password never reaches PostgreSQL, a log or an error: ALTER/CREATE ROLE carry a
//!   SCRAM-SHA-256 verifier computed here; every rotation is one transaction (a crash or a missing role changes
//!   nothing); a probe counts as refused only on SQLSTATE 28P01; check details name roles and variables, never a
//!   value; RolePassword's Debug is redacted]
//! Spec: Baseline §6.2.0; §6.2.2; §45; §57.1; ADR-0059
//!
//! The caller (the `humaux-maintenance` binary) owns where values come from (environment, the
//! generator, the parsed migration 0011) and how they are printed; this module only turns them
//! into verifiers, statements and check results.

use std::collections::BTreeSet;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use sqlx::Connection as _;
use sqlx::postgres::{PgConnectOptions, PgConnection};

use crate::postgres::{MaintenanceDbPool, MigratorDbPool};
use crate::provisioning::ProvisioningError;

type HmacSha256 = Hmac<Sha256>;

/// PostgreSQL's own `scram_iterations` default, the count `psql \password` uses (RFC 7677 §4).
const SCRAM_ITERATIONS: u32 = 4096;

/// A role password held in memory only. `Debug` prints `<redacted>` so a stray `{:?}` cannot leak it.
#[derive(Clone, PartialEq, Eq)]
pub struct RolePassword(String);

impl RolePassword {
    /// Wraps a value read from the environment, the generator, or the parsed migration 0011.
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// The plaintext, for the one place that must use it (a login probe, the print-once line).
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for RolePassword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RolePassword(<redacted>)")
    }
}

fn hmac(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(key).expect("HMAC accepts any key length");
    for part in parts {
        mac.update(part);
    }
    mac.finalize().into_bytes().into()
}

fn verifier_with_salt(password: &[u8], salt: &[u8]) -> String {
    // RFC 5802 Hi(): PBKDF2-HMAC-SHA256 with one output block. The passwords this module sees are
    // URL-unreserved ASCII, for which SASLprep is the identity.
    let mut u = hmac(password, &[salt, &1u32.to_be_bytes()]);
    let mut salted = u;
    for _ in 1..SCRAM_ITERATIONS {
        u = hmac(password, &[&u]);
        salted.iter_mut().zip(u).for_each(|(s, x)| *s ^= x);
    }
    let stored_key = Sha256::digest(hmac(&salted, &[b"Client Key"]));
    let server_key = hmac(&salted, &[b"Server Key"]);
    format!(
        "SCRAM-SHA-256${SCRAM_ITERATIONS}:{}${}:{}",
        BASE64.encode(salt),
        BASE64.encode(stored_key),
        BASE64.encode(server_key)
    )
}

/// ADR-0059 D-E: the `SCRAM-SHA-256$4096:<salt>$<StoredKey>:<ServerKey>` verifier PostgreSQL stores
/// as-is, with a fresh 16-byte random salt per call. Sending this instead of the plaintext keeps the
/// password out of the server log, `pg_stat_statements` and error contexts (what `psql \password`
/// does through libpq).
pub fn scram_verifier(password: &RolePassword) -> String {
    let salt: [u8; 16] = rand::random();
    verifier_with_salt(password.expose().as_bytes(), &salt)
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// ADR-0059 D-E: the one builder of `ALTER ROLE ... PASSWORD`; `verifier` comes from
/// [`scram_verifier`], never a plaintext.
pub fn alter_role_password_sql(role: &str, verifier: &str) -> String {
    format!(
        "ALTER ROLE {} PASSWORD {}",
        quote_ident(role),
        quote_literal(verifier)
    )
}

/// ADR-0059 D-A: `CREATE ROLE` for a fresh cluster. `Some(verifier)` = a LOGIN role with the 0011
/// attribute set; `None` = the migration owner, NOLOGIN and without a password.
pub fn create_role_sql(role: &str, verifier: Option<&str>) -> String {
    let login = match verifier {
        Some(v) => format!("LOGIN PASSWORD {}", quote_literal(v)),
        None => "NOLOGIN".to_owned(),
    };
    format!(
        "CREATE ROLE {} {login} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS",
        quote_ident(role)
    )
}

/// One rotation (ADR-0059 D-E): new passwords for `targets`, the owner role that must stay NOLOGIN,
/// and whether absent roles are created (`--create-missing`, D-A fresh cluster).
pub struct RotateRequest<'a> {
    /// `(role, new password)`, applied in order inside one transaction.
    pub targets: &'a [(String, RolePassword)],
    /// The migration owner: refused if it can log in; created NOLOGIN when missing and `create_missing`.
    pub owner: &'a str,
    /// Create absent roles instead of refusing them.
    pub create_missing: bool,
}

/// ADR-0059 D-E: every `ALTER ROLE` (and, with `create_missing`, `CREATE ROLE`) in ONE transaction:
/// a missing role, a LOGIN owner, or a target equal to the connected principal rolls everything
/// back and is [`ProvisioningError::Refused`] naming the role; a crash before COMMIT changes nothing.
pub async fn rotate_in_txn(
    pool: &MigratorDbPool,
    request: &RotateRequest<'_>,
) -> Result<(), ProvisioningError> {
    let refused = |why: String| Err(ProvisioningError::Refused(why));
    // dep: PostgreSQL(owner) — one transaction for every role change of this rotation
    let mut txn = pool.pool().begin().await.map_err(ProvisioningError::Db)?;
    let principal: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(&mut *txn)
        .await
        .map_err(ProvisioningError::Db)?;
    let can_login = |role: &str| {
        sqlx::query_scalar::<_, bool>("SELECT rolcanlogin FROM pg_roles WHERE rolname = $1")
            .bind(role.to_owned())
    };
    // (role, new password); `None` = the owner, created NOLOGIN without a password (D-A).
    let mut steps: Vec<(&str, Option<&RolePassword>)> =
        Vec::with_capacity(request.targets.len() + 1);
    match can_login(request.owner)
        .fetch_optional(&mut *txn)
        .await
        .map_err(ProvisioningError::Db)?
    {
        Some(false) => {}
        Some(true) => {
            return refused(format!(
                "{} can log in (ADR-0059 D-A requires NOLOGIN; run migrate first)",
                request.owner
            ));
        }
        None if request.create_missing => steps.push((request.owner, None)),
        None => return refused(format!("missing object: role {}", request.owner)),
    }
    for (role, password) in request.targets {
        // ADR-0059 D-E: rotating the connected principal is a lockout path.
        if *role == principal || role == request.owner {
            return refused(format!("role {role} is not a rotation target"));
        }
        steps.push((role, Some(password)));
    }
    for (role, password) in steps {
        let exists = can_login(role)
            .fetch_optional(&mut *txn)
            .await
            .map_err(ProvisioningError::Db)?
            .is_some();
        let verifier = password.map(scram_verifier);
        let statement = match (exists, request.create_missing, verifier.as_deref()) {
            (true, _, Some(verifier)) => alter_role_password_sql(role, verifier),
            (false, true, verifier) => create_role_sql(role, verifier),
            _ => return refused(format!("missing object: role {role}")),
        };
        sqlx::raw_sql(&statement)
            .execute(&mut *txn)
            .await
            .map_err(ProvisioningError::Db)?;
    }
    txn.commit().await.map_err(ProvisioningError::Db)
}

/// One login attempt's outcome (ADR-0059 D-F). A result, not an error type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// SQLSTATE 28P01: the server checked the password and refused it. The only "safe" outcome.
    Refused,
    /// The login succeeded.
    Accepted,
    /// SQLSTATE 28000: pg_hba rejected the path, or the password matched a NOLOGIN role. Either way
    /// the password was not shown refused.
    Unverified,
    /// Anything else (connect refused, TLS, protocol): the probe did not happen.
    Infra,
}

/// ADR-0059 D-F: `Ok(())` = logged in; `Err(Some(sqlstate))` = a server error; `Err(None)` = no server
/// answer. Only 28P01 proves a refusal.
pub fn classify_probe(result: Result<(), Option<&str>>) -> ProbeOutcome {
    match result {
        Ok(()) => ProbeOutcome::Accepted,
        Err(Some("28P01")) => ProbeOutcome::Refused,
        Err(Some("28000")) => ProbeOutcome::Unverified,
        Err(_) => ProbeOutcome::Infra,
    }
}

/// Tri-state result of one deploy-check (§57.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckStatus {
    /// The invariant holds.
    Pass,
    /// The invariant is broken; the detail names the roles or variables.
    Fail,
    /// The check could not run; the detail names the missing object.
    NotApplicable,
}

impl CheckStatus {
    /// The wire spelling in the deploy-check receipt.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::NotApplicable => "not_applicable",
        }
    }
}

/// One named deploy-check result. `detail` holds role or variable names only, never a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckResult {
    /// Stable check name (`placeholder_login`, `probe_valid`, ...).
    pub name: &'static str,
    /// Tri-state outcome.
    pub status: CheckStatus,
    /// Names only.
    pub detail: String,
}

impl CheckResult {
    /// §57.1: a check that cannot run names the missing object.
    pub fn not_applicable(name: &'static str, missing_object: &str) -> Self {
        Self {
            name,
            status: CheckStatus::NotApplicable,
            detail: format!("missing object: {missing_object}"),
        }
    }

    fn from_problems(name: &'static str, problems: Vec<String>, ok: String) -> Self {
        if problems.is_empty() {
            Self {
                name,
                status: CheckStatus::Pass,
                detail: ok,
            }
        } else {
            Self {
                name,
                status: CheckStatus::Fail,
                detail: problems.join("; "),
            }
        }
    }
}

/// The repository placeholder set (ADR-0059 D-F): the passwords migration 0011 publishes, all of the
/// form `P_<role>` for one common prefix `P`.
pub struct Placeholders {
    values: Vec<RolePassword>,
    prefix: String,
}

/// ADR-0059 D-F: checks every `(role, value)` pair has the form `P_<role>` for one common `P` and keeps
/// that `P`. `None` = convention drift (the caller reports `roles-sql parse drift`).
pub fn derive_candidates(pairs: &[(String, RolePassword)]) -> Option<Placeholders> {
    let (first_role, first) = pairs.first()?;
    let prefix = first.expose().strip_suffix(&format!("_{first_role}"))?;
    if prefix.is_empty()
        || pairs
            .iter()
            .any(|(role, v)| v.expose() != format!("{prefix}_{role}"))
    {
        return None;
    }
    Some(Placeholders {
        values: pairs.iter().map(|(_, v)| v.clone()).collect(),
        prefix: prefix.to_owned(),
    })
}

impl Placeholders {
    /// Candidates tried against `role`: the published values, plus `P_<role>` (the same convention
    /// for a role 0011 does not create, e.g. role_admin) and `P` itself (the dev superuser value).
    pub fn for_role(&self, role: &str) -> Vec<RolePassword> {
        let mut seen = BTreeSet::new();
        self.values
            .iter()
            .cloned()
            .chain([
                RolePassword::new(format!("{}_{role}", self.prefix)),
                RolePassword::new(self.prefix.clone()),
            ])
            .filter(|c| seen.insert(c.expose().to_owned()))
            .collect()
    }

    /// ADR-0059 D-E: `true` when `value` contains the prefix `P`, which every published value and
    /// every candidate of [`Self::for_role`] does, so equality with any candidate is covered too.
    pub fn taints(&self, value: &str) -> bool {
        value.contains(&self.prefix)
    }

    fn contains(&self, value: &str) -> bool {
        value == self.prefix || self.values.iter().any(|v| v.expose() == value)
    }
}

/// The deploy-check probe path (host, port, database, TLS mode) taken from a PostgreSQL URL; its user
/// and password are replaced on every probe. `None` = not a PostgreSQL URL.
pub fn probe_options(dsn: &str) -> Option<PgConnectOptions> {
    dsn.parse().ok()
}

/// The user name of a PostgreSQL URL (the superuser probe target); its password is never read.
pub fn dsn_user(dsn: &str) -> Option<String> {
    probe_options(dsn)
        .map(|o| o.get_username().to_owned())
        .filter(|u| !u.is_empty())
}

fn path_of(probe: &PgConnectOptions) -> String {
    format!("{}:{}", probe.get_host(), probe.get_port())
}

/// One login attempt. A server answer is classified; no answer (connect refused, TLS, timeout) is
/// `Err` (exit 1), because then nothing was probed.
async fn try_login(
    probe: &PgConnectOptions,
    role: &str,
    password: &str,
) -> Result<ProbeOutcome, ProvisioningError> {
    let options = probe.clone().username(role).password(password);
    // ponytail: no per-probe timeout (a refused TCP connect fails at once); add a configured one if a
    // black-holed database host ever hangs deploy-check.
    // dep: PostgreSQL(any) — one login attempt as `role`, closed at once
    match PgConnection::connect_with(&options).await {
        Ok(connection) => {
            let _ = connection.close().await;
            Ok(classify_probe(Ok(())))
        }
        Err(e) => {
            let code = e
                .as_database_error()
                .and_then(|d| d.code())
                .map(|c| c.into_owned());
            match classify_probe(Err(code.as_deref())) {
                ProbeOutcome::Infra => Err(ProvisioningError::Db(e)),
                outcome => Ok(outcome),
            }
        }
    }
}

/// Check `placeholder_login` (ADR-0059 D-F): every candidate of every role in `roles` must come back
/// 28P01. An accepted login or a 28000 fails naming the role; no server answer is `Err` (infra).
pub async fn check_placeholder_login(
    probe: &PgConnectOptions,
    roles: &[String],
    placeholders: &Placeholders,
) -> Result<CheckResult, ProvisioningError> {
    let mut problems = Vec::new();
    for role in roles {
        let mut worst = ProbeOutcome::Refused;
        for candidate in placeholders.for_role(role) {
            match try_login(probe, role, candidate.expose()).await? {
                ProbeOutcome::Refused | ProbeOutcome::Infra => {}
                ProbeOutcome::Accepted => worst = ProbeOutcome::Accepted,
                ProbeOutcome::Unverified if worst == ProbeOutcome::Refused => {
                    worst = ProbeOutcome::Unverified;
                }
                ProbeOutcome::Unverified => {}
            }
        }
        match worst {
            ProbeOutcome::Accepted => {
                problems.push(format!("role {role}: accepts a repository placeholder"));
            }
            ProbeOutcome::Unverified => problems.push(format!(
                "role {role}: unverified (28000) via {}",
                path_of(probe)
            )),
            ProbeOutcome::Refused | ProbeOutcome::Infra => {}
        }
    }
    Ok(CheckResult::from_problems(
        "placeholder_login",
        problems,
        format!("{} role(s): every placeholder refused (28P01)", roles.len()),
    ))
}

/// Check `probe_valid` (ADR-0059 D-F): a fresh random password must be refused with 28P01 for every
/// role, otherwise the path does not verify passwords and `placeholder_login` proves nothing.
pub async fn check_probe_valid(
    probe: &PgConnectOptions,
    roles: &[String],
) -> Result<CheckResult, ProvisioningError> {
    let mut problems = Vec::new();
    for role in roles {
        let random: [u8; 24] = rand::random();
        match try_login(probe, role, &BASE64.encode(random)).await? {
            ProbeOutcome::Refused | ProbeOutcome::Infra => {}
            ProbeOutcome::Accepted => problems.push(format!(
                "role {role}: a random password logged in via {} (trust/peer path)",
                path_of(probe)
            )),
            ProbeOutcome::Unverified => problems.push(format!(
                "role {role}: unverified (28000) via {}",
                path_of(probe)
            )),
        }
    }
    Ok(CheckResult::from_problems(
        "probe_valid",
        problems,
        format!(
            "{} role(s): a random password is refused (28P01) via {}",
            roles.len(),
            path_of(probe)
        ),
    ))
}

/// §73.5 / ADR-0059 D-H: opens (`advance`: next epoch, window open) or closes the API-key rehash
/// window through the role_maintenance-only owner definers (migrations 0202, 0204). Returns the epoch.
/// `advance` while the window is already open is `Refused("rehash_window_open")` (0205's 55000), so
/// a re-run never moves the epoch a second time inside one window.
pub async fn set_pepper_window(
    pool: &MaintenanceDbPool,
    open: bool,
) -> Result<i32, ProvisioningError> {
    let sql = if open {
        "SELECT control.credential_pepper_epoch_advance()"
    } else {
        "SELECT control.credential_pepper_epoch_close()"
    };
    // dep: PostgreSQL(role_maintenance) — control.credential_pepper_epoch_advance / _close owner definers
    sqlx::query_scalar(sql)
        .fetch_one(pool.pool())
        .await
        .map_err(ProvisioningError::from)
}

/// Check `owner_nologin` (ADR-0059 D-A): `pg_roles.rolcanlogin = false` for the owner.
pub async fn check_owner_nologin(
    pool: &MaintenanceDbPool,
    owner: &str,
) -> Result<CheckResult, ProvisioningError> {
    // dep: PostgreSQL(role_maintenance) — catalog read of the owner's LOGIN attribute
    let login: Option<bool> =
        sqlx::query_scalar("SELECT rolcanlogin FROM pg_roles WHERE rolname = $1")
            .bind(owner)
            .fetch_optional(pool.pool())
            .await
            .map_err(ProvisioningError::Db)?;
    Ok(match login {
        None => CheckResult::not_applicable("owner_nologin", &format!("role {owner}")),
        Some(login) => CheckResult::from_problems(
            "owner_nologin",
            login
                .then(|| format!("role {owner}: rolcanlogin=true"))
                .into_iter()
                .collect(),
            format!("role {owner}: NOLOGIN"),
        ),
    })
}

/// Check `schema_migrations_write` (SEC-2 / ADR-0059 D-C): no role in `roles` holds INSERT, UPDATE,
/// DELETE or TRUNCATE on `ops.schema_migrations`.
pub async fn check_schema_migrations_write(
    pool: &MaintenanceDbPool,
    roles: &[String],
) -> Result<CheckResult, ProvisioningError> {
    // dep: PostgreSQL(role_maintenance) — has_table_privilege over the non-owner roles
    let writers: Vec<String> = sqlx::query_scalar(
        "SELECT r FROM unnest($1::text[]) AS r \
         WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) \
           AND EXISTS (SELECT 1 FROM unnest(ARRAY['INSERT','UPDATE','DELETE','TRUNCATE']) AS p \
                       WHERE has_table_privilege(r, 'ops.schema_migrations', p)) \
         ORDER BY r",
    )
    .bind(roles)
    .fetch_all(pool.pool())
    .await
    .map_err(ProvisioningError::Db)?;
    Ok(CheckResult::from_problems(
        "schema_migrations_write",
        writers
            .into_iter()
            .map(|r| format!("role {r}: can write ops.schema_migrations"))
            .collect(),
        format!("{} role(s): no write on ops.schema_migrations", roles.len()),
    ))
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = (bytes[i] == b'%')
            .then(|| text.get(i + 1..i + 3))
            .flatten()
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match hex {
            Some(b) => {
                out.push(b);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The percent-decoded password of a `postgres://user:password@host/...` URL, if it has one.
fn dsn_password(dsn: &str) -> Option<String> {
    let authority = dsn.split_once("://")?.1;
    let authority = authority.split(['/', '?']).next()?;
    let userinfo = authority.rsplit_once('@')?.0;
    Some(percent_decode(userinfo.split_once(':')?.1))
}

/// Check `env_dsn_placeholders` (ADR-0059 D-F): no `*_PG_DSN` / `DATABASE_URL` variable in `env`
/// carries a repository placeholder as its password. A finding names the variable only.
pub fn check_env_dsn_placeholders(
    env: &[(String, String)],
    placeholders: &Placeholders,
) -> CheckResult {
    let scanned: Vec<&(String, String)> = env
        .iter()
        .filter(|(name, _)| name.ends_with("_PG_DSN") || name == "DATABASE_URL")
        .collect();
    let problems = scanned
        .iter()
        .filter(|(_, dsn)| dsn_password(dsn).is_some_and(|p| placeholders.contains(&p)))
        .map(|(name, _)| format!("{name}: password is a repository placeholder"))
        .collect();
    CheckResult::from_problems(
        "env_dsn_placeholders",
        problems,
        format!(
            "{} DSN variable(s): no repository placeholder",
            scanned.len()
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generated(tag: &str) -> RolePassword {
        let random: [u8; 12] = rand::random();
        RolePassword::new(format!(
            "{tag}{}",
            BASE64.encode(random).replace(['+', '/'], "x")
        ))
    }

    fn synthetic_placeholders(roles: &[&str]) -> (String, Vec<(String, RolePassword)>) {
        let prefix = generated("p").expose().to_owned();
        let pairs = roles
            .iter()
            .map(|r| (r.to_string(), RolePassword::new(format!("{prefix}_{r}"))))
            .collect();
        (prefix, pairs)
    }

    /// T6 (ADR-0059 D-E): PostgreSQL's stored-verifier shape, no plaintext inside, fresh salt per call.
    #[test]
    fn scram_verifier_has_postgres_shape_and_never_contains_the_password() {
        let password = generated("pw");
        let a = scram_verifier(&password);
        let b = scram_verifier(&password);
        for v in [&a, &b] {
            let rest = v.strip_prefix("SCRAM-SHA-256$4096:").expect("prefix");
            let (salt, keys) = rest.split_once('$').expect("salt$keys");
            let (stored, server) = keys.split_once(':').expect("stored:server");
            assert_eq!((salt.len(), stored.len(), server.len()), (24, 44, 44));
            assert!(!v.contains(password.expose()));
        }
        assert_ne!(a, b, "two calls must use different salts");
    }

    /// The public RFC 7677 §3 example credential (password "pencil", salt "W22ZaJ0SNY7soEsUEjb6gQ==",
    /// 4096 iterations) gives the StoredKey/ServerKey that an independent PBKDF2-HMAC-SHA256
    /// implementation computes, so the loop above is the real Hi().
    #[test]
    fn verifier_matches_rfc7677_salted_password() {
        let salt = BASE64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
        let v = verifier_with_salt(b"pencil", &salt);
        assert_eq!(
            v,
            "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU="
        );
    }

    #[test]
    fn role_password_debug_is_redacted() {
        let password = generated("pw");
        assert!(!format!("{password:?}").contains(password.expose()));
    }

    #[test]
    fn statements_quote_the_role_and_carry_only_the_verifier() {
        assert_eq!(
            alter_role_password_sql("role_x", "SCRAM-SHA-256$4096:a$b:c"),
            "ALTER ROLE \"role_x\" PASSWORD 'SCRAM-SHA-256$4096:a$b:c'"
        );
        assert_eq!(
            create_role_sql("role_o", None),
            "CREATE ROLE \"role_o\" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
        );
        assert!(
            create_role_sql("r\"x", Some("v"))
                .starts_with("CREATE ROLE \"r\"\"x\" LOGIN PASSWORD 'v'")
        );
    }

    /// T10 (ADR-0059 D-F): only 28P01 is a refusal; 28000 is unverified, never safe.
    #[test]
    fn probe_counts_only_28p01_as_refused() {
        assert_eq!(classify_probe(Err(Some("28P01"))), ProbeOutcome::Refused);
        assert_eq!(classify_probe(Ok(())), ProbeOutcome::Accepted);
        assert_eq!(classify_probe(Err(Some("28000"))), ProbeOutcome::Unverified);
        assert_eq!(classify_probe(Err(Some("08001"))), ProbeOutcome::Infra);
        assert_eq!(classify_probe(Err(None)), ProbeOutcome::Infra);
    }

    /// T33 (ADR-0059 D-F): the candidates cover the role_admin convention and the superuser prefix,
    /// and a value outside the convention is drift.
    #[test]
    fn derive_candidates_covers_admin_and_superuser_convention() {
        let (prefix, pairs) = synthetic_placeholders(&["role_a", "role_b"]);
        let placeholders = derive_candidates(&pairs).expect("convention holds");
        let admin: Vec<String> = placeholders
            .for_role("role_admin")
            .iter()
            .map(|c| c.expose().to_owned())
            .collect();
        assert!(admin.contains(&format!("{prefix}_role_admin")));
        assert!(admin.contains(&prefix));
        assert_eq!(admin.len(), 4, "2 published + P_role_admin + P");
        assert_eq!(
            placeholders.for_role("role_a").len(),
            3,
            "P_role_a deduplicated"
        );

        let mut drift = pairs.clone();
        drift[1].1 = generated("other");
        assert!(derive_candidates(&drift).is_none());
    }

    /// T13 (ADR-0059 D-F): a placeholder DSN is named by its variable, and no value appears in the detail.
    #[test]
    fn env_dsn_scan_names_the_variable_not_the_value() {
        let (prefix, pairs) = synthetic_placeholders(&["role_a"]);
        let placeholders = derive_candidates(&pairs).expect("convention holds");
        let leaked = pairs[0].1.expose().to_owned();
        let fresh = generated("fresh").expose().to_owned();
        let env = vec![
            (
                "SERVICE_X_PG_DSN".to_owned(),
                format!("postgres://role_a:{leaked}@127.0.0.1:1/db?sslmode=disable"),
            ),
            (
                "DATABASE_URL".to_owned(),
                format!("postgres://postgres:{}@h/db", prefix.replace('_', "%5F")),
            ),
            (
                "SERVICE_Y_PG_DSN".to_owned(),
                format!("postgres://role_a:{fresh}@h/db"),
            ),
            (
                "UNRELATED".to_owned(),
                format!("postgres://u:{leaked}@h/db"),
            ),
        ];
        let result = check_env_dsn_placeholders(&env, &placeholders);
        assert_eq!(result.status, CheckStatus::Fail);
        assert!(
            result.detail.contains("SERVICE_X_PG_DSN"),
            "{}",
            result.detail
        );
        assert!(result.detail.contains("DATABASE_URL"), "{}", result.detail);
        assert!(!result.detail.contains("SERVICE_Y_PG_DSN"));
        assert!(!result.detail.contains("UNRELATED"));
        assert!(!result.detail.contains(&leaked) && !result.detail.contains(&prefix));
        assert_eq!(
            check_env_dsn_placeholders(&env[2..], &placeholders).status,
            CheckStatus::Pass
        );
    }
}
