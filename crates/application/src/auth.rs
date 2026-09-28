//! `application::auth` — H2: §74 注册/登录/重置/变更状态机（Argon2id+security epoch）（Phase 2 wave 实现；判据出处见 spec 家章）。
//! Depends-on: crates=[argon2, hex, hmac, humaux-domain, rand, sha2]; services=[]; env=[]; modules=[domain::audit,
//!   domain::error, domain::ids]
//! Called-by: []
//! Invariants: []
//! Spec: §74; §3; §78.3; §74.6
//!
//! This module is deliberately pure: `humaux-application`'s own `Cargo.toml` carries no
//! SQL/HTTP driver dependency, so nothing here touches Postgres or the network directly
//! (§3/§78.3 — Domain/Application never import HTTP/SQLx/Provider SDK). Every function takes
//! plain snapshots of already-fetched row state and returns a plain outcome; the caller (a
//! future gateway/adapter wiring task) is the one that reads `migrations/0034_...sql`'s rows
//! into these snapshot types and writes the outcome back. [`EmailNotifier`] is the one
//! injection seam for outbound mail — the real adapter is H3's `EmailProvider` trait +
//! `email_outbox` (§74.6), not yet wired into this crate at Phase 2 wave time.
//!
//! §74's own frozen sentence: "Identity Flow 必须是显式状态机，不让前端决定流程是否合法" —
//! every state transition below is a function the caller cannot skip past; there is no field
//! anywhere a client sets directly to jump to `ACCOUNT_ACTIVE` or "verified".

use humaux_domain::audit::AuditMetadata;
use humaux_domain::error::ErrorCode;
use humaux_domain::ids::UserId;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Algorithm, Argon2, Params, Version};
use hmac::{Hmac, KeyInit, Mac};
use rand::{Rng, RngCore};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

// =============================================================================
// §78.1 Typed Config Registry: Argon2id parameters and every challenge TTL/attempt-limit/
// cooldown live here, one struct, never inline in a handler/query. `Default` carries this
// module's own recommendation; the real deployment value is whatever the caller constructs
// (e.g. from a future Policy/Entitlement DB row) — nothing below reads an env var or a
// literal duration a second time.
// =============================================================================

/// §74.3 "参数由性能/安全基线集中配置...不写死在业务 handler" — every Argon2id knob in one
/// place. OWASP-baseline defaults (owner: humaux-application::auth, §78.1 registered here).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Argon2Config {
    /// Memory cost in KiB.
    pub m_cost: u32,
    /// Iteration count.
    pub t_cost: u32,
    /// Degree of parallelism.
    pub p_cost: u32,
}

impl Default for Argon2Config {
    fn default() -> Self {
        // OWASP Password Storage Cheat Sheet Argon2id baseline (19 MiB / 2 iterations / 1
        // lane) — the config a real deployment overrides, not a value handlers reach past.
        Self {
            m_cost: 19 * 1024,
            t_cost: 2,
            p_cost: 1,
        }
    }
}

impl Argon2Config {
    fn params(self) -> Params {
        Params::new(self.m_cost, self.t_cost, self.p_cost, None)
            .expect("Argon2Config values are caller-controlled and validated at construction")
    }

    fn engine(self) -> Argon2<'static> {
        Argon2::new(Algorithm::Argon2id, Version::V0x13, self.params())
    }
}

/// §74.2/§74.4/§74.5 challenge policy: short TTL, attempt limit, send cooldown — one struct
/// per challenge kind so signup/reset/email-change can be tuned independently without a
/// fourth copy of the same three literals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChallengePolicy {
    pub ttl: Duration,
    pub max_attempts: i32,
    pub send_cooldown: Duration,
}

impl Default for ChallengePolicy {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(15 * 60),
            max_attempts: 5,
            send_cooldown: Duration::from_secs(60),
        }
    }
}

/// Top-level auth config: one instance threaded through every function in this module that
/// needs a knob, instead of each function re-declaring its own default (§78.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthConfig {
    pub argon2: Argon2Config,
    pub signup_challenge: ChallengePolicy,
    pub reset_challenge: ChallengePolicy,
    pub email_change_challenge: ChallengePolicy,
    /// §74.4 default session lifetime for a freshly issued session row.
    pub session_ttl: Duration,
}

impl Default for AuthConfig {
    fn default() -> Self {
        // Hand-written rather than `#[derive(Default)]`: every *other* field here has its own
        // hand-written `Default` (`Argon2Config`/`ChallengePolicy`) precisely because a
        // meaningless zero value is unsafe to hand a caller — `#[derive(Default)]` on this
        // struct would still have produced a correct `argon2`/`*_challenge` (their own
        // `Default` impls run), but `session_ttl: Duration` has no such impl and would have
        // silently fallen through to `Duration::default()` (0ns), minting every session
        // already-expired the moment issuance is wired up.
        Self {
            argon2: Argon2Config::default(),
            signup_challenge: ChallengePolicy::default(),
            reset_challenge: ChallengePolicy::default(),
            email_change_challenge: ChallengePolicy::default(),
            // §74.4 — a real deployment overrides this, same status as every other field's
            // default; 30 days is this module's own baseline recommendation, not a spec-frozen
            // number.
            session_ttl: Duration::from_secs(30 * 24 * 60 * 60),
        }
    }
}

// =============================================================================
// §74.1 email canonicalization — "只规范化 domain case；不要擅自实现 Gmail 去点、加号折叠等
// provider-specific 规则".
// =============================================================================

/// Splits `original` into `(original_email, canonical_email)` per §74.1: canonical only
/// lowercases the domain part (Unicode-aware `to_lowercase`, not an ASCII-only pass — a
/// caller with a non-ASCII domain must still canonicalize correctly), the local part is
/// preserved byte-for-byte. `INVALID_INPUT` if there is no `@`, the local/domain part is
/// empty, or the local part itself still contains an `@` (i.e. the input had more than one —
/// `rsplit_once` alone would silently fold `a@b@c` into local=`a@b`/domain=`c`, this rejects
/// that instead of guessing which `@` was the real separator). This function does not attempt
/// full RFC 5321 validation, only enough shape to split local/domain unambiguously.
pub fn canonicalize_email(original: &str) -> Result<(String, String), ErrorCode> {
    let (local, domain) = original.rsplit_once('@').ok_or(ErrorCode::InvalidInput)?;
    if local.is_empty() || domain.is_empty() || local.contains('@') {
        return Err(ErrorCode::InvalidInput);
    }
    let canonical = format!("{local}@{}", domain.to_lowercase());
    Ok((original.to_string(), canonical))
}

// =============================================================================
// §74.3 Argon2id password hashing + rehash-on-login.
// =============================================================================

/// Counts real Argon2id KDF invocations (hash or verify) on the *current thread*. Not a
/// production metric — its only consumer is
/// `login_missing_and_wrong_password_run_equal_kdf_operations`, which needs a deterministic
/// (non-timing-flaky, and safe under `cargo test`'s default per-test-thread parallelism) way
/// to assert both branches of [`login`] pay the same KDF cost (§74.3/§80.1: an assertion that
/// cannot observably fail is not a gate — see that test's own doc comment for the tautology it
/// replaces).
mod argon2_invocation_counter {
    use std::cell::Cell;

    std::thread_local! {
        static COUNT: Cell<u64> = const { Cell::new(0) };
    }

    pub(super) fn record() {
        COUNT.with(|c| c.set(c.get() + 1));
    }

    #[cfg(test)]
    pub(super) fn get() -> u64 {
        COUNT.with(|c| c.get())
    }
}

/// Opaque PHC-encoded Argon2id hash, as stored in `control.password_credentials.password_hash`.
/// Field is private with a redacting [`Debug`](std::fmt::Debug) impl (a stray `{:?}` in a log
/// statement must not print the PHC string) — see [`EncodedPasswordHash::as_str`] for the one
/// legitimate consumer (a caller persisting the row).
#[derive(Clone, PartialEq, Eq)]
pub struct EncodedPasswordHash(String);

impl EncodedPasswordHash {
    /// The PHC-encoded string itself.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for EncodedPasswordHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EncodedPasswordHash(REDACTED)")
    }
}

/// Hashes `plaintext` under `config`'s current Argon2id parameters. `ErrorCode::Internal` only
/// on an RNG/encoding failure that should never happen in practice — never on bad input, which
/// a password has no shape to reject at this layer.
pub fn hash_password(
    config: AuthConfig,
    plaintext: &str,
) -> Result<EncodedPasswordHash, ErrorCode> {
    // `rand` 0.9's OS-backed generator, not `argon2::password_hash::rand_core::OsRng` — that
    // would require enabling password-hash's separate `getrandom` cargo feature on a shared
    // crate manifest for no real gain, since `SaltString::encode_b64` accepts plain bytes from
    // any RNG this crate already depends on.
    let mut salt_bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut salt_bytes);
    let salt = SaltString::encode_b64(&salt_bytes).map_err(|_| ErrorCode::Internal)?;
    let hash = config
        .argon2
        .engine()
        .hash_password(plaintext.as_bytes(), &salt)
        .map_err(|_| ErrorCode::Internal)?;
    argon2_invocation_counter::record();
    Ok(EncodedPasswordHash(hash.to_string()))
}

/// §74.3 verify outcome: whether the password matched, and — only when it did — whether the
/// stored hash's own embedded parameters are stale against `config`'s current target, i.e.
/// rehash-on-login should fire. A caller must never rehash on a failed verify (that would
/// silently "fix" an attacker-supplied wrong password into a valid-looking stored hash).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PasswordVerifyOutcome {
    Valid { needs_rehash: bool },
    Invalid,
}

/// Verifies `plaintext` against `stored` (PHC string) using the parameters embedded in
/// `stored` itself — that is how `PasswordHash` verification works, the caller's `config` is
/// only consulted afterward to decide `needs_rehash`. A malformed `stored` string (never
/// expected from this module's own [`hash_password`] output, but a defensive path against a
/// corrupted row) verifies as `Invalid`, never a panic or an `Err` that could be confused with
/// "verification did not run" — and does not count as a KDF invocation, since no KDF ran.
pub fn verify_password(
    config: AuthConfig,
    plaintext: &str,
    stored: &EncodedPasswordHash,
) -> PasswordVerifyOutcome {
    let Ok(parsed) = PasswordHash::new(&stored.0) else {
        return PasswordVerifyOutcome::Invalid;
    };
    let verify_result = config
        .argon2
        .engine()
        .verify_password(plaintext.as_bytes(), &parsed);
    argon2_invocation_counter::record();
    if verify_result.is_err() {
        return PasswordVerifyOutcome::Invalid;
    }
    let needs_rehash = match Params::try_from(&parsed) {
        Ok(p) => {
            p.m_cost() != config.argon2.m_cost
                || p.t_cost() != config.argon2.t_cost
                || p.p_cost() != config.argon2.p_cost
        }
        Err(_) => true, // can't read embedded params -> treat as stale, rehash on next login
    };
    PasswordVerifyOutcome::Valid { needs_rehash }
}

// =============================================================================
// §74.2 verification codes — cryptographically random, single-use, keyed-hashed at rest, never
// logged. `PlaintextCode`'s `Debug` is deliberately redacted so a stray `{:?}` in a log
// statement cannot leak it (§74.2 "never logged").
// =============================================================================

/// The plaintext code, held only long enough to hand to [`EmailNotifier`] — never stored,
/// never (successfully) printed.
#[derive(Clone, PartialEq, Eq)]
pub struct PlaintextCode(String);

impl PlaintextCode {
    /// The raw digits, for the one legitimate consumer: composing the outbound email body.
    pub fn reveal(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for PlaintextCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PlaintextCode(REDACTED)")
    }
}

/// HMAC-SHA256(pepper, code) hex digest of a code — what actually lands in `code_hash`
/// columns. Field is private with a redacting [`Debug`](std::fmt::Debug) impl for the same
/// reason [`EncodedPasswordHash`] has one: a printed digest here is equivalent to printing the
/// 6-digit plaintext code, since the code space (10^6) is small enough that possession of the
/// keyed hash plus the pepper inverts it instantly — the pepper is exactly what stands between
/// "read access to this row" and "know the code" (see [`CodeHash::verify`]'s doc for why this
/// module moved off unkeyed SHA-256).
#[derive(Clone, PartialEq, Eq)]
pub struct CodeHash(String);

impl CodeHash {
    /// The hex digest itself.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Hashes `candidate` under the same `pepper` and compares against `self` in constant time
    /// (byte-length-fixed hex digests on both sides, so this is not a variable-length-
    /// comparison side channel — XOR-accumulate rather than `==` purely so a future refactor to
    /// a variable-length representation does not silently reintroduce a short-circuit compare).
    pub fn verify(&self, pepper: &[u8], candidate: &str) -> bool {
        let candidate_hash = hmac_sha256_hex(pepper, candidate);
        constant_time_eq(self.0.as_bytes(), candidate_hash.as_bytes())
    }
}

impl std::fmt::Debug for CodeHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CodeHash(REDACTED)")
    }
}

/// §74.2/§73.5 keyed hash: `HMAC-SHA256(pepper, input)`, hex-encoded for the `text` DB columns
/// this lands in (`control.email_challenges.code_hash` et al., `migrations/
/// 0035_email_auth_identity.sql`) — the same construction `protocol::edge::compute_api_key_hash`
/// already uses for `control.api_keys.key_hash` (§73.5), reused rather than the plain unkeyed
/// SHA-256 this replaced. An unkeyed digest over a 6-digit/10^6-entry code space is invertible
/// by a precomputed table the instant a reader has `SELECT` on the row — every non-owner
/// runtime role does (§6.2.1 domain default) — so "hashed at rest" (§74.2) requires the keyed
/// construction, not merely *a* hash. `pepper` is caller-supplied (an env-sourced secret at the
/// real deployment's edge, never read from an env var inside this module — same discipline
/// every other knob in this file follows via [`AuthConfig`]).
fn hmac_sha256_hex(pepper: &[u8], input: &str) -> String {
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(pepper).expect("HMAC accepts any key length");
    mac.update(input.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Generates a new 6-digit code (cryptographically random via `rand`'s OS-backed generator)
/// and its keyed hash. The plaintext is returned solely for the caller to hand to an
/// [`EmailNotifier`]; the hash is what a caller persists.
pub fn generate_verification_code(pepper: &[u8]) -> (PlaintextCode, CodeHash) {
    let mut rng = rand::rng();
    let code: String = (0..6)
        .map(|_| char::from(b'0' + rng.random_range(0..10)))
        .collect();
    let hash = CodeHash(hmac_sha256_hex(pepper, &code));
    (PlaintextCode(code), hash)
}

// =============================================================================
// §74.2/§74.4/§74.5 shared challenge-row shape — one snapshot type + one usability predicate
// reused by signup email verification, password reset, and email change (all three tables in
// migrations/0035_email_auth_identity.sql share this exact column set: code_hash/attempts/
// max_attempts/expires_at/consumed_at).
// =============================================================================

/// Read-only snapshot of one challenge row's lifecycle fields — the caller fetches this from
/// whichever of `control.email_challenges` / `control.password_reset_challenges` /
/// `control.email_change_requests` applies.
#[derive(Debug, Clone, Copy)]
pub struct ChallengeSnapshot {
    pub attempts: i32,
    pub max_attempts: i32,
    pub expires_at: SystemTime,
    pub consumed_at: Option<SystemTime>,
}

/// §74.2: "single-use / short TTL / attempt limit" — the one place all three conditions are
/// evaluated together, so a caller cannot accidentally check only one of them. `now` is
/// caller-supplied (not `SystemTime::now()` inside this function) so tests can freeze time.
pub fn challenge_is_usable(snapshot: ChallengeSnapshot, now: SystemTime) -> bool {
    snapshot.consumed_at.is_none()
        && snapshot.expires_at > now
        && snapshot.attempts < snapshot.max_attempts
}

/// Outcome of one verification attempt against a challenge row, given the just-computed
/// `code_matched` result of [`CodeHash::verify`]. The caller applies `next_attempts`/
/// `should_consume` to its `UPDATE`; this function never touches a database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChallengeAttemptOutcome {
    pub accepted: bool,
    pub next_attempts: i32,
    /// True exactly when this attempt should set `consumed_at` (a correct code on a still-
    /// usable challenge) — single-use is enforced by the caller writing this back, this
    /// function only decides whether that write should happen.
    pub should_consume: bool,
}

/// Records one verification attempt. Only ever call this after confirming
/// [`challenge_is_usable`] for the *pre*-attempt snapshot — an attempt against an already-
/// unusable challenge (expired/consumed/exhausted) is the caller's own bug, not a case this
/// function silently tolerates, so it always reports `accepted: false` for that case too
/// (fail-closed) without incrementing `attempts` past `max_attempts`.
pub fn record_challenge_attempt(
    snapshot: ChallengeSnapshot,
    now: SystemTime,
    code_matched: bool,
) -> ChallengeAttemptOutcome {
    if !challenge_is_usable(snapshot, now) {
        return ChallengeAttemptOutcome {
            accepted: false,
            next_attempts: snapshot.attempts,
            should_consume: false,
        };
    }
    if code_matched {
        ChallengeAttemptOutcome {
            accepted: true,
            next_attempts: snapshot.attempts + 1,
            should_consume: true,
        }
    } else {
        ChallengeAttemptOutcome {
            accepted: false,
            next_attempts: snapshot.attempts + 1,
            should_consume: false,
        }
    }
}

/// §74.2/§78.1 "send cooldown": whether a new challenge/verification-code send is allowed
/// given when the previous one was sent for the same target. `last_sent_at: None` (no prior
/// send recorded) is always allowed. `now` is caller-supplied, same discipline as
/// [`challenge_is_usable`]. A caller whose clock reads `now` earlier than `last_sent_at` fails
/// closed (send blocked) rather than treating an impossible negative elapsed time as "cooldown
/// satisfied" — this is a rate limiter, and rate limiters fail closed, not open.
pub fn send_allowed(
    last_sent_at: Option<SystemTime>,
    now: SystemTime,
    policy: ChallengePolicy,
) -> bool {
    match last_sent_at {
        None => true,
        Some(last) => now
            .duration_since(last)
            .map(|elapsed| elapsed >= policy.send_cooldown)
            .unwrap_or(false),
    }
}

// =============================================================================
// §74.2 registration five-state machine — START -> SIGNUP_PENDING -> EMAIL_CHALLENGE_SENT ->
// EMAIL_VERIFIED -> ACCOUNT_ACTIVE, server-computed from already-persisted facts (see the
// migration file's own comment for why this is not an eighth `control.users` column).
// =============================================================================

/// The four persisted signup states (`START` has no row yet, so it is not a variant here —
/// see [`signup_state`]'s `None` input case).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignupState {
    SignupPending,
    EmailChallengeSent,
    EmailVerified,
    AccountActive,
}

/// §6.3's `control.users.state` values this module reads (a subset — SUSPENDED/DEACTIVATED/
/// PENDING_DELETE/DELETED are T1.1's lifecycle concern, irrelevant to signup progress).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserLifecycleState {
    PendingVerification,
    Active,
    Other,
}

impl UserLifecycleState {
    /// Maps a raw `control.users.state` value (§6.3, `migrations/0003_control_core.sql`'s
    /// six-value CHECK) onto the subset [`signup_state`] branches on — SUSPENDED/DEACTIVATED/
    /// PENDING_DELETE/DELETED all collapse to `Other` because §74.2 signup-progress logic only
    /// ever distinguishes PENDING_VERIFICATION/ACTIVE from everything else. Panics on a value
    /// outside those six — that would mean the DB CHECK and this mapping have already
    /// diverged, which `user_lifecycle_state_covers_every_migration_check_value` (§78.2)
    /// guards against by comparing this function's accepted set to the migration file itself.
    pub fn from_db_str(s: &str) -> Self {
        match s {
            "PENDING_VERIFICATION" => Self::PendingVerification,
            "ACTIVE" => Self::Active,
            "SUSPENDED" | "DEACTIVATED" | "PENDING_DELETE" | "DELETED" => Self::Other,
            // dep-map: allow table-undeclared — names the table only in a panic message; no DB access here
            other => panic!("unrecognized control.users.state value: {other}"),
        }
    }
}

/// Derives the §74.2 signup state from three already-fetched facts — this is the "服务器判定"
/// half of §74's frozen rule: a client cannot set any of these three facts directly, each one
/// only changes as the side effect of a server-validated action (row insert, challenge send,
/// challenge verify, activation).
///
/// - `user_exists: false` -> `None` (the unpersisted `START` state).
/// - `lifecycle_state: Active` **and** `email_verified` -> `AccountActive`. §74.2's "账户在
///   邮箱验证前不能获得正常 SaaS 权益" is a necessary condition on email verification itself,
///   not on the lifecycle column alone — a row that reached `ACTIVE` through some other path
///   (invitation acceptance, an admin action, a future SSO path) while `email_verified` is
///   still `false` must not short-circuit into `AccountActive` just because the lifecycle
///   column says so; see `account_active_lifecycle_without_email_verified_is_not_entitled`.
/// - Otherwise: no challenge ever sent -> `SignupPending`; a challenge sent but the email not
///   yet verified -> `EmailChallengeSent`; email verified but lifecycle not yet flipped to
///   `Active` -> `EmailVerified` (the caller's activation step is what performs that flip).
pub fn signup_state(
    user_exists: bool,
    lifecycle_state: UserLifecycleState,
    challenge_ever_sent: bool,
    email_verified: bool,
) -> Option<SignupState> {
    if !user_exists {
        return None;
    }
    if matches!(lifecycle_state, UserLifecycleState::Active) && email_verified {
        return Some(SignupState::AccountActive);
    }
    if email_verified {
        return Some(SignupState::EmailVerified);
    }
    if challenge_ever_sent {
        return Some(SignupState::EmailChallengeSent);
    }
    Some(SignupState::SignupPending)
}

/// §74.2 "账户在邮箱验证前不能获得正常 SaaS 权益" — the one predicate every entitlement check
/// must route through for "is this account allowed to use the product at all" (a *separate*
/// question from §76's Entitlement Projector, which decides *which* features once this gate
/// already passed).
pub fn is_entitled_to_saas(state: Option<SignupState>) -> bool {
    matches!(state, Some(SignupState::AccountActive))
}

// =============================================================================
// §74.3 login — unified error response (enumeration resistance).
// =============================================================================

/// A hash of a fixed, non-secret placeholder password — computed once, via [`OnceLock`], and
/// reused for every login attempt against an email that does not exist. A prior version of
/// this function called [`hash_password`] (a full Argon2id hash) on *every* invocation, which
/// meant [`login`]'s "no such account" branch paid for that hash *plus* the dummy
/// [`verify_password`] call below — two KDF operations against the found-but-wrong-password
/// branch's one, a measured ~2x wall-clock gap trivially observable over the network. That
/// gap reopened exactly the enumeration channel this function exists to close, despite the
/// two branches returning byte-identical [`LoginOutcome::InvalidCredentials`] values — §74.3
/// requires uniformity in *time*, not only in the response bytes. Caching via `OnceLock`
/// keeps the "no such account" branch to the single `verify_password` call it needs, matching
/// the found-but-wrong-password branch's cost after the first call in a process's lifetime.
fn dummy_hash(config: AuthConfig) -> EncodedPasswordHash {
    static DUMMY: OnceLock<EncodedPasswordHash> = OnceLock::new();
    DUMMY
        .get_or_init(|| {
            // Deliberately not a secret — this hash never verifies any real credential, it
            // exists purely to keep the CPU-bound KDF path shaped identically for the "no such
            // account" branch. A fixed plaintext is fine because it is never compared against
            // anything a real user typed.
            hash_password(config, "humaux-enumeration-resistance-dummy")
                .expect("hashing a fixed literal cannot fail")
        })
        .clone()
}

/// What a caller needs to know to attempt one login: the stored credential row, if the
/// account+email exists. `None` when the email is unregistered — the whole point of this
/// type is that [`login`] treats `Some`-with-wrong-password and `None` identically at the
/// response level.
pub struct LoginLookup {
    pub user_id: UserId,
    pub password_hash: EncodedPasswordHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoginOutcome {
    Success {
        user_id: UserId,
        needs_rehash: bool,
    },
    /// §74.3: identical for "no such email" and "wrong password" — never branch on which one
    /// it was in any caller-visible way (logging the distinction server-side for abuse
    /// detection is fine; the *response* must not).
    InvalidCredentials,
}

/// §74.3 login attempt. `lookup: None` (email not found) and a `Some` lookup whose password
/// does not match both terminate in the identical [`LoginOutcome::InvalidCredentials`] — and,
/// after [`dummy_hash`]'s cache is warm, both paths run exactly one Argon2id verify (see
/// `login_missing_and_wrong_password_run_equal_kdf_operations` and
/// `login_missing_and_wrong_password_have_comparable_wall_clock_cost`, this module's own
/// enumeration-resistance evidence).
pub fn login(
    config: AuthConfig,
    lookup: Option<LoginLookup>,
    candidate_password: &str,
) -> LoginOutcome {
    match lookup {
        Some(found) => match verify_password(config, candidate_password, &found.password_hash) {
            PasswordVerifyOutcome::Valid { needs_rehash } => LoginOutcome::Success {
                user_id: found.user_id,
                needs_rehash,
            },
            PasswordVerifyOutcome::Invalid => LoginOutcome::InvalidCredentials,
        },
        None => {
            // Burn the same Argon2id verify cost as the found-but-wrong-password path.
            let _ = verify_password(config, candidate_password, &dummy_hash(config));
            LoginOutcome::InvalidCredentials
        }
    }
}

// =============================================================================
// §74.4 password reset — session_epoch invalidation.
// =============================================================================

/// §74.4 "increment session_epoch / revoke sessions" — the entire mechanism is one integer
/// bump; every session issued under the old epoch stops validating without a single UPDATE/
/// DELETE on `control.sessions` (see [`session_still_valid`]).
pub fn bump_security_epoch(current_epoch: i64) -> i64 {
    current_epoch + 1
}

/// Read-only snapshot of one `control.sessions` row's lifecycle fields (§74.4/§73.6) — the
/// same snapshot-type shape [`ChallengeSnapshot`] uses, so session validation gets the same
/// all-conditions-together treatment challenge validation already has, instead of checking
/// only `session_epoch` and silently ignoring `expires_at`/`revoked_at` (both columns
/// `migrations/0035_email_auth_identity.sql` already defines on `control.sessions`).
#[derive(Debug, Clone, Copy)]
pub struct SessionSnapshot {
    pub session_epoch: i64,
    pub expires_at: SystemTime,
    pub revoked_at: Option<SystemTime>,
}

/// A session is valid only when none of three independent conditions disqualifies it: it has
/// not been explicitly revoked, it has not passed its expiry, and its issuance-time epoch
/// snapshot still equals the user's *current* `control.users.security_epoch` (§74.4 — a
/// password reset, or any future "revoke all sessions" action, is exactly one
/// [`bump_security_epoch`] call away from invalidating every outstanding session for that
/// user, with no per-session UPDATE/DELETE sweep needed). All three must hold; a caller that
/// checks only the epoch fails open on an expired or an explicitly revoked session.
pub fn session_still_valid(
    session: SessionSnapshot,
    user_current_epoch: i64,
    now: SystemTime,
) -> bool {
    session.revoked_at.is_none()
        && session.expires_at > now
        && session.session_epoch == user_current_epoch
}

/// §74.4 step "request -> generic response": the caller (the future reset-request handler)
/// must route both the found-email and missing-email cases through this single function
/// rather than branching in the handler — the same enumeration-resistance discipline [`login`]
/// applies (§74.3), extended to the reset-request endpoint (§74.4). There is deliberately no
/// lookup-result parameter: the type signature itself is what makes "no caller can special-
/// case the miss branch" true, rather than relying on an assertion that two branches happen to
/// produce an equal value today (see this module's login enumeration tests for the tautology
/// that exact shape produced when applied to `LoginOutcome`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PasswordResetRequested;

/// Always returns the one generic outcome — see [`PasswordResetRequested`].
pub fn reset_request_response() -> PasswordResetRequested {
    PasswordResetRequested
}

// =============================================================================
// §74.5 email change — reauthenticate -> pending -> verify -> notify old email -> commit.
// =============================================================================

/// §74.6-shaped outbound-notification port: the real implementation is H3's `EmailProvider` +
/// `email_outbox` (adapters crate, not a dependency of this one — §3/§78.3 keeps Application
/// free of any SDK/HTTP import). Until that wiring lands, any caller (tests included) supplies
/// its own implementation.
pub trait EmailNotifier {
    /// §74.5 "-> notify old email": tells the previously-registered address that its account's
    /// email is changing, independent of whether the new address has verified yet.
    fn notify_email_change(&self, old_email: &str, new_email: &str) -> Result<(), ErrorCode>;
    /// §74.2/§74.5 outbound verification code delivery.
    fn send_verification_code(&self, email: &str, code: &PlaintextCode) -> Result<(), ErrorCode>;
    /// §74.4 step "-> notify user": tells the user their password was successfully reset —
    /// the reset flow's own final step, distinct from [`EmailNotifier::notify_email_change`]
    /// (§74.5's old-email notice) and from delivering the reset code itself
    /// ([`EmailNotifier::send_verification_code`]).
    fn notify_password_reset_completed(&self, email: &str) -> Result<(), ErrorCode>;
}

/// §74.5 step 1: an email-change request may only be created once the caller has already
/// reauthenticated the user (password/MFA re-check — a separate concern, out of this module's
/// scope) — this function's `reauthenticated` parameter is the caller's attestation of that,
/// never something this function itself decides. `Forbidden` if it was not reauthenticated:
/// the caller's identity is already known and valid (they hold a live session — that is a
/// prerequisite to even reach this call), what is missing is the specific, fresh attestation
/// this sensitive identity-changing action additionally requires, which is `Forbidden`'s
/// "identity known but lacks permission for this action", not `Unauthorized`'s "identity
/// missing or invalid" (`humaux_domain::error::ErrorCode`). This is the one guard standing
/// between "logged in" and "may change identity-defining data".
pub fn begin_email_change(
    reauthenticated: bool,
    new_email_canonical: &str,
    current_email_canonical: &str,
) -> Result<(), ErrorCode> {
    if !reauthenticated {
        return Err(ErrorCode::Forbidden);
    }
    if new_email_canonical == current_email_canonical {
        return Err(ErrorCode::InvalidInput);
    }
    Ok(())
}

/// §74.5 step "-> notify old email", performed through the injected port so this module never
/// imports an email SDK directly. Returns the notifier's own error unchanged — a failed
/// notification is the caller's problem to retry/alert on, this function does not swallow it.
pub fn notify_old_email(
    notifier: &dyn EmailNotifier,
    old_email: &str,
    new_email: &str,
) -> Result<(), ErrorCode> {
    notifier.notify_email_change(old_email, new_email)
}

/// §74.4 step "-> notify user" (reset completion), performed through the same injected port as
/// [`notify_old_email`], for the same reason.
pub fn notify_password_reset(notifier: &dyn EmailNotifier, email: &str) -> Result<(), ErrorCode> {
    notifier.notify_password_reset_completed(email)
}

// =============================================================================
// §74.1 auth event trail — mirrors `control.auth_events.event_type`'s CHECK constraint 1:1
// (`migrations/0035_email_auth_identity.sql`) so a Rust-side typo can't silently produce a row
// the DB CHECK would have rejected — the enum is the single source, not a second string list.
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthEventType {
    SignupStarted,
    EmailChallengeSent,
    EmailVerified,
    AccountActivated,
    LoginSuccess,
    LoginFailure,
    PasswordResetRequested,
    PasswordResetCompleted,
    EmailChangeRequested,
    EmailChangeCompleted,
}

impl AuthEventType {
    /// All ten variants, declaration order — matches
    /// `migrations/0035_email_auth_identity.sql`'s `event_type` CHECK list order verbatim,
    /// verified against the migration file's actual text by
    /// `auth_event_type_db_strings_match_migration_check_constraint` rather than against a
    /// second hand-copied string list (§78.2 — the failure mode a copy-vs-copy comparison
    /// cannot catch is exactly "someone edited the migration and forgot the copy").
    pub const ALL: [AuthEventType; 10] = [
        Self::SignupStarted,
        Self::EmailChallengeSent,
        Self::EmailVerified,
        Self::AccountActivated,
        Self::LoginSuccess,
        Self::LoginFailure,
        Self::PasswordResetRequested,
        Self::PasswordResetCompleted,
        Self::EmailChangeRequested,
        Self::EmailChangeCompleted,
    ];

    /// SQL CHECK-constraint spelling, verbatim (`migrations/0035_email_auth_identity.sql`).
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::SignupStarted => "SIGNUP_STARTED",
            Self::EmailChallengeSent => "EMAIL_CHALLENGE_SENT",
            Self::EmailVerified => "EMAIL_VERIFIED",
            Self::AccountActivated => "ACCOUNT_ACTIVATED",
            Self::LoginSuccess => "LOGIN_SUCCESS",
            Self::LoginFailure => "LOGIN_FAILURE",
            Self::PasswordResetRequested => "PASSWORD_RESET_REQUESTED",
            Self::PasswordResetCompleted => "PASSWORD_RESET_COMPLETED",
            Self::EmailChangeRequested => "EMAIL_CHANGE_REQUESTED",
            Self::EmailChangeCompleted => "EMAIL_CHANGE_COMPLETED",
        }
    }
}

/// One `control.auth_events` row's application-owned fields (§77): pairs the closed
/// [`AuthEventType`] enum with an allowlisted [`AuditMetadata`], never a raw jsonb value — so
/// no caller in this crate can smuggle a password/code/token into `metadata` past §77's ban.
/// `migrations/0035_email_auth_identity.sql`'s own comment on `control.auth_events.metadata`
/// names this as "application-layer contract...a code-review/test invariant" with nothing in
/// the DB CHECKs able to enforce it; this type is what makes it a compile-time invariant
/// instead of merely a documented one — the allowlist itself lives in
/// `humaux_domain::audit::AuditMetadata::insert` and is not reimplemented here, this module's
/// own crate already depends on `humaux-domain`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthEventRecord {
    pub event_type: AuthEventType,
    pub metadata: AuditMetadata,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn cfg() -> AuthConfig {
        AuthConfig::default()
    }

    /// Not a real secret — this module never reads a pepper from the environment itself (the
    /// real deployment's edge does, then passes it in, same as every other [`AuthConfig`]
    /// knob); tests just need *a* fixed byte string to key the HMAC with.
    const TEST_PEPPER: &[u8] = b"test-only-pepper-not-a-secret";

    // ---- §74.3 口令哈希基线（OWASP Argon2id） ----

    /// **DOD-049 的缺口之一，补上**：`Argon2Config::engine()` 里的 `Algorithm::Argon2id`
    /// 此前没有任何断言钉住——改成 `Argon2i` 或 `Argon2d` 全仓无红。那是实质降级：
    /// OWASP 明确要求 Argon2id（Argon2i 抗 GPU 弱，Argon2d 有侧信道风险），而口令哈希
    /// 的降级不会有任何功能表现，只有被拖库那天才看得出来。
    ///
    /// 断言钉在 **PHC 串**而不是内部字段上：`hash_password` 的输出本身就是可观测产物，
    /// 而 PHC 前缀同时编码了算法、版本与三个代价参数，一条断言把整条 OWASP 基线
    /// （Argon2id / v=19 / 19 MiB / 2 iterations / 1 lane）一起钉住。改任一项都会变形。
    #[test]
    fn password_hash_pins_the_owasp_argon2id_baseline_in_its_phc_prefix() {
        let hash = hash_password(cfg(), "correct horse battery staple")
            .expect("hashing a well-formed password never fails");
        let phc = hash.as_str();

        assert!(
            phc.starts_with("$argon2id$"),
            "口令哈希算法必须是 Argon2id（OWASP §74.3）——Argon2i 抗 GPU 弱、Argon2d 有\
             侧信道风险，两者都能通过除本条之外的每一个测试。实得: {}",
            &phc[..phc.len().min(40)]
        );
        assert!(
            phc.starts_with("$argon2id$v=19$m=19456,t=2,p=1$"),
            "OWASP 基线（v=19 / 19 MiB / 2 iterations / 1 lane）被改动。参数调整是合法的\
             运维决定，但必须是**显式**的：连同本断言一起改，而不是悄悄改掉 Default。\
             实得: {}",
            &phc[..phc.len().min(40)]
        );
    }

    /// 反向对照：本条钉的是**基线**不是「任何 PHC 串都行」。故意用一组非基线参数，
    /// 断言它产出的前缀确实不同——否则上面那条断言可能对任何输入都成立（那就是假绿）。
    #[test]
    fn a_non_baseline_config_produces_a_different_phc_prefix() {
        let tuned = AuthConfig {
            argon2: Argon2Config {
                m_cost: 32 * 1024,
                t_cost: 3,
                p_cost: 2,
            },
            ..cfg()
        };
        let phc = hash_password(tuned, "correct horse battery staple")
            .expect("hashing a well-formed password never fails");
        let phc = phc.as_str();
        assert!(phc.starts_with("$argon2id$"), "算法不该随参数变: {phc}");
        assert!(
            !phc.starts_with("$argon2id$v=19$m=19456,t=2,p=1$"),
            "非基线参数产出了基线前缀——说明上面那条断言其实没在看参数: {phc}"
        );
    }

    // ---- §74.2 unverified account cannot use entitlements ----

    #[test]
    fn unverified_account_is_not_entitled() {
        let state = signup_state(true, UserLifecycleState::PendingVerification, true, false);
        assert_eq!(state, Some(SignupState::EmailChallengeSent));
        assert!(!is_entitled_to_saas(state));
    }

    #[test]
    fn email_verified_but_not_yet_activated_is_still_not_entitled() {
        let state = signup_state(true, UserLifecycleState::PendingVerification, true, true);
        assert_eq!(state, Some(SignupState::EmailVerified));
        assert!(!is_entitled_to_saas(state));
    }

    #[test]
    fn account_active_is_entitled() {
        let state = signup_state(true, UserLifecycleState::Active, true, true);
        assert_eq!(state, Some(SignupState::AccountActive));
        assert!(is_entitled_to_saas(state));
    }

    /// 注错红转绿 for the bypass this module's fixer review flagged: `lifecycle_state: Active`
    /// with `email_verified: false` must NOT reach `AccountActive` — a row can land in
    /// `ACTIVE` through a path this module never validated (invitation acceptance, an admin
    /// action, a future SSO path), and §74.2's email-verification gate is a necessary
    /// condition regardless of how the lifecycle column got there.
    #[test]
    fn account_active_lifecycle_without_email_verified_is_not_entitled() {
        let state = signup_state(true, UserLifecycleState::Active, true, false);
        assert_ne!(state, Some(SignupState::AccountActive));
        assert!(!is_entitled_to_saas(state));
    }

    #[test]
    fn nonexistent_user_has_no_state_and_no_entitlement() {
        let state = signup_state(false, UserLifecycleState::Other, false, false);
        assert_eq!(state, None);
        assert!(!is_entitled_to_saas(state));
    }

    #[test]
    fn signup_state_progression_is_server_derived_not_client_settable() {
        // The five-state ladder, walked in order, purely from facts a client cannot set
        // directly (row existence / challenge-sent / verified-at / lifecycle flip).
        assert_eq!(
            signup_state(true, UserLifecycleState::PendingVerification, false, false),
            Some(SignupState::SignupPending)
        );
        assert_eq!(
            signup_state(true, UserLifecycleState::PendingVerification, true, false),
            Some(SignupState::EmailChallengeSent)
        );
        assert_eq!(
            signup_state(true, UserLifecycleState::PendingVerification, true, true),
            Some(SignupState::EmailVerified)
        );
        assert_eq!(
            signup_state(true, UserLifecycleState::Active, true, true),
            Some(SignupState::AccountActive)
        );
    }

    /// §78.2: `UserLifecycleState::from_db_str` must accept exactly the six values
    /// `migrations/0003_control_core.sql`'s `control.users.state` CHECK allows — parsed out of
    /// the migration file's own text (`include_str!`, no DB connection needed, matching this
    /// crate's zero-SQL-driver-dependency charter), not a second hand-copied list.
    #[test]
    fn user_lifecycle_state_covers_every_migration_check_value() {
        let sql = include_str!("../../../migrations/0003_control_core.sql");
        let ddl = extract_table_ddl(sql, "control.users");
        let db_values = extract_check_in_values(ddl, "state");
        assert_eq!(
            db_values,
            vec![
                "PENDING_VERIFICATION",
                "ACTIVE",
                "SUSPENDED",
                "DEACTIVATED",
                "PENDING_DELETE",
                "DELETED",
            ],
            "control.users.state CHECK drifted from the set UserLifecycleState::from_db_str \
             is written against"
        );
        // Direction two: from_db_str must not panic on any value the CHECK allows (compile-time
        // exhaustive match already guarantees each maps into exactly one of
        // {PendingVerification, Active, Other} — this proves it covers precisely these six,
        // not some other set).
        for v in &db_values {
            let _ = UserLifecycleState::from_db_str(v);
        }
    }

    // ---- §74.2 challenge attempt limit ----

    #[test]
    fn challenge_becomes_unusable_after_max_attempts() {
        let now = SystemTime::now();
        let policy = ChallengePolicy::default();
        let mut snapshot = ChallengeSnapshot {
            attempts: 0,
            max_attempts: policy.max_attempts,
            expires_at: now + policy.ttl,
            consumed_at: None,
        };
        assert!(challenge_is_usable(snapshot, now));

        // Exhaust every attempt with a wrong code.
        for _ in 0..policy.max_attempts {
            let outcome = record_challenge_attempt(snapshot, now, false);
            assert!(!outcome.accepted);
            snapshot.attempts = outcome.next_attempts;
        }
        assert_eq!(snapshot.attempts, policy.max_attempts);
        assert!(!challenge_is_usable(snapshot, now));

        // Even the *correct* code no longer accepts once attempts are exhausted.
        let final_try = record_challenge_attempt(snapshot, now, true);
        assert!(!final_try.accepted);
        assert!(!final_try.should_consume);
        assert_eq!(final_try.next_attempts, snapshot.attempts); // not incremented past the cap
    }

    #[test]
    fn correct_code_consumes_challenge_single_use() {
        let now = SystemTime::now();
        let snapshot = ChallengeSnapshot {
            attempts: 0,
            max_attempts: 5,
            expires_at: now + Duration::from_secs(600),
            consumed_at: None,
        };
        let outcome = record_challenge_attempt(snapshot, now, true);
        assert!(outcome.accepted);
        assert!(outcome.should_consume);

        // Once consumed, a second attempt (even with the right code) is rejected.
        let consumed = ChallengeSnapshot {
            consumed_at: Some(now),
            ..snapshot
        };
        assert!(!challenge_is_usable(consumed, now));
        let replay = record_challenge_attempt(consumed, now, true);
        assert!(!replay.accepted);
    }

    #[test]
    fn expired_challenge_is_unusable_even_with_zero_attempts() {
        let now = SystemTime::now();
        let expired = ChallengeSnapshot {
            attempts: 0,
            max_attempts: 5,
            expires_at: now - Duration::from_secs(1),
            consumed_at: None,
        };
        assert!(!challenge_is_usable(expired, now));
    }

    // ---- §74.2/§78.1 send cooldown ----

    #[test]
    fn send_cooldown_blocks_before_window_elapses_and_allows_after() {
        let policy = ChallengePolicy::default();
        let now = SystemTime::now();
        assert!(send_allowed(None, now, policy), "no prior send -> allowed");
        assert!(
            !send_allowed(Some(now), now, policy),
            "zero elapsed < cooldown -> blocked"
        );
        assert!(
            !send_allowed(
                Some(now),
                now + policy.send_cooldown - Duration::from_secs(1),
                policy
            ),
            "one second short of the cooldown -> still blocked"
        );
        assert!(
            send_allowed(Some(now), now + policy.send_cooldown, policy),
            "exactly the cooldown elapsed -> allowed"
        );
    }

    #[test]
    fn send_cooldown_fails_closed_on_clock_skew() {
        let policy = ChallengePolicy::default();
        let now = SystemTime::now();
        let last_sent_in_the_future = now + Duration::from_secs(3600);
        assert!(!send_allowed(Some(last_sent_in_the_future), now, policy));
    }

    // ---- §74.3 enumeration resistance: identical response, existing vs nonexistent email ----

    /// Replaces a prior version of this test that only asserted
    /// `InvalidCredentials == InvalidCredentials` — true for *any* implementation of `login`,
    /// including one with a 2x timing gap between branches (§80.1: "一道闸没有注错红转绿就不算
    /// 存在" — an assertion that cannot go red is not a gate). This version asserts something
    /// that actually distinguishes the fixed implementation from the bug it fixes: the exact
    /// number of Argon2id KDF invocations each branch performs, via
    /// `argon2_invocation_counter` (thread-local — safe under `cargo test`'s default
    /// per-test-thread parallelism, unlike a global atomic every test thread would contend on).
    #[test]
    fn login_missing_and_wrong_password_run_equal_kdf_operations() {
        let config = cfg();
        let real_hash = hash_password(config, "correct horse battery staple").unwrap();

        // Warm `dummy_hash`'s OnceLock before measuring — matches its documented "computed
        // once" behavior; the enumeration-resistance property holds in steady state, which is
        // what every request after the process's first login attempt experiences.
        let _ = login(config, None, "warmup");

        let before_wrong = argon2_invocation_counter::get();
        let lookup = LoginLookup {
            user_id: UserId::new(),
            password_hash: real_hash,
        };
        let wrong_password_outcome = login(config, Some(lookup), "not the password");
        let wrong_password_kdf_ops = argon2_invocation_counter::get() - before_wrong;

        let before_missing = argon2_invocation_counter::get();
        let no_such_account_outcome = login(config, None, "not the password");
        let missing_account_kdf_ops = argon2_invocation_counter::get() - before_missing;

        assert_eq!(wrong_password_outcome, LoginOutcome::InvalidCredentials);
        assert_eq!(no_such_account_outcome, LoginOutcome::InvalidCredentials);
        assert_eq!(
            wrong_password_kdf_ops, 1,
            "found-but-wrong-password must run exactly one Argon2id verify"
        );
        assert_eq!(
            missing_account_kdf_ops, 1,
            "no-such-account must run exactly one Argon2id verify once dummy_hash is warm \
             (a regression back to calling hash_password per-call would make this 2)"
        );
    }

    /// Real wall-clock measurement, not a proxy — the finding this replaces measured a live
    /// 2.01x ratio (m=19MiB/t=2, 10 iters) between these two branches; this test uses a faster
    /// Argon2id config so it stays quick in CI while still measuring genuine KDF cost, and
    /// asserts the ratio lands in a band that a 2x-class regression would fall well outside of.
    /// n and the band are stated in the assertion message so a failure is legible, not a bare
    /// `assert!(false)`.
    #[test]
    fn login_missing_and_wrong_password_have_comparable_wall_clock_cost() {
        let config = AuthConfig {
            argon2: Argon2Config {
                m_cost: 8 * 1024,
                t_cost: 2,
                p_cost: 1,
            },
            ..AuthConfig::default()
        };
        let real_hash = hash_password(config, "correct horse battery staple").unwrap();

        // Warm the OnceLock before either measured loop.
        let _ = login(config, None, "warmup");

        const ITERS: u32 = 15;
        let t_wrong = {
            let start = Instant::now();
            for _ in 0..ITERS {
                let lookup = LoginLookup {
                    user_id: UserId::new(),
                    password_hash: real_hash.clone(),
                };
                std::hint::black_box(login(config, Some(lookup), "not the password"));
            }
            start.elapsed()
        };
        let t_missing = {
            let start = Instant::now();
            for _ in 0..ITERS {
                std::hint::black_box(login(config, None, "not the password"));
            }
            start.elapsed()
        };

        let ratio = t_missing.as_secs_f64() / t_wrong.as_secs_f64();
        assert!(
            (0.5..=1.8).contains(&ratio),
            "enumeration-resistance timing ratio out of band: missing/wrong = {ratio:.3} \
             (t_wrong={t_wrong:?}, t_missing={t_missing:?}, n={ITERS}); the bug this test \
             replaces measured 2.01x"
        );
    }

    #[test]
    fn login_succeeds_with_correct_password() {
        let config = cfg();
        let hash = hash_password(config, "correct horse battery staple").unwrap();
        let user_id = UserId::new();
        let lookup = LoginLookup {
            user_id,
            password_hash: hash,
        };
        let outcome = login(config, Some(lookup), "correct horse battery staple");
        assert_eq!(
            outcome,
            LoginOutcome::Success {
                user_id,
                needs_rehash: false
            }
        );
    }

    #[test]
    fn rehash_on_login_flags_stale_params() {
        let old_config = AuthConfig {
            argon2: Argon2Config {
                m_cost: 8 * 1024,
                t_cost: 1,
                p_cost: 1,
            },
            ..AuthConfig::default()
        };
        let new_config = AuthConfig::default(); // stronger params than old_config
        let hash = hash_password(old_config, "hunter2").unwrap();

        let outcome = verify_password(new_config, "hunter2", &hash);
        assert_eq!(outcome, PasswordVerifyOutcome::Valid { needs_rehash: true });

        // Verifying under the *original* params: correct, no rehash needed.
        let outcome_same = verify_password(old_config, "hunter2", &hash);
        assert_eq!(
            outcome_same,
            PasswordVerifyOutcome::Valid {
                needs_rehash: false
            }
        );
    }

    #[test]
    fn auth_config_default_session_ttl_is_not_zero() {
        // 注错红转绿: `#[derive(Default)]` on AuthConfig would have silently produced 0ns here
        // (`Duration::default()`) — this is the test that would have caught it.
        assert!(AuthConfig::default().session_ttl > Duration::ZERO);
    }

    // ---- §74.4 reset invalidates old-epoch sessions; fail-closed on revoked/expired ----

    #[test]
    fn session_rejected_after_password_reset_bumps_epoch() {
        let epoch_before_reset = 3i64;
        let now = SystemTime::now();
        let session = SessionSnapshot {
            session_epoch: epoch_before_reset, // issued while epoch was 3
            expires_at: now + Duration::from_secs(3600),
            revoked_at: None,
        };
        assert!(session_still_valid(session, epoch_before_reset, now));

        let epoch_after_reset = bump_security_epoch(epoch_before_reset);
        assert_eq!(epoch_after_reset, 4);
        assert!(!session_still_valid(session, epoch_after_reset, now));

        // A session issued *after* the reset carries the new epoch and stays valid.
        let new_session = SessionSnapshot {
            session_epoch: epoch_after_reset,
            ..session
        };
        assert!(session_still_valid(new_session, epoch_after_reset, now));
    }

    /// 注错红转绿: a session with a current epoch and a future expiry, but an explicit
    /// `revoked_at`, must still be rejected — the fail-open bug this closes ignored this
    /// column entirely.
    #[test]
    fn revoked_session_is_rejected_even_with_matching_epoch_and_future_expiry() {
        let now = SystemTime::now();
        let session = SessionSnapshot {
            session_epoch: 1,
            expires_at: now + Duration::from_secs(3600),
            revoked_at: Some(now),
        };
        assert!(!session_still_valid(session, 1, now));
    }

    /// 注错红转绿: same shape, for `expires_at` — the fail-open bug ignored this column too.
    #[test]
    fn expired_session_is_rejected_even_with_matching_epoch_and_no_revocation() {
        let now = SystemTime::now();
        let session = SessionSnapshot {
            session_epoch: 1,
            expires_at: now - Duration::from_secs(1),
            revoked_at: None,
        };
        assert!(!session_still_valid(session, 1, now));
    }

    #[test]
    fn reset_request_response_has_no_lookup_result_input() {
        // The type itself is the guarantee (see the function's doc comment): there is no
        // boolean/enum parameter here for a caller to accidentally branch on, unlike the
        // tautological shape `login`'s old enumeration test had.
        assert_eq!(reset_request_response(), PasswordResetRequested);
    }

    // ---- verification code never stored/logged in plaintext; hashed with a keyed HMAC ----

    #[test]
    fn plaintext_code_is_redacted_in_debug_output() {
        let (plaintext, hash) = generate_verification_code(TEST_PEPPER);
        let debug_repr = format!("{plaintext:?}");
        assert_eq!(debug_repr, "PlaintextCode(REDACTED)");
        assert!(!debug_repr.contains(plaintext.reveal()));
        // The hash is what would actually be written to `code_hash` — never equal to the
        // plaintext string itself (sanity check that generation actually hashed).
        assert_ne!(hash.as_str(), plaintext.reveal());
        assert_eq!(hash.as_str().len(), 64); // HMAC-SHA256 hex digest length
    }

    #[test]
    fn code_hash_debug_output_is_redacted() {
        let (_plaintext, hash) = generate_verification_code(TEST_PEPPER);
        let debug_repr = format!("{hash:?}");
        assert_eq!(debug_repr, "CodeHash(REDACTED)");
        assert!(!debug_repr.contains(hash.as_str()));
    }

    #[test]
    fn encoded_password_hash_debug_output_is_redacted() {
        let hash = hash_password(cfg(), "hunter2").unwrap();
        let debug_repr = format!("{hash:?}");
        assert_eq!(debug_repr, "EncodedPasswordHash(REDACTED)");
        assert!(!debug_repr.contains(hash.as_str()));
    }

    #[test]
    fn code_hash_verifies_only_the_matching_plaintext_under_the_same_pepper() {
        let (plaintext, hash) = generate_verification_code(TEST_PEPPER);
        assert!(hash.verify(TEST_PEPPER, plaintext.reveal()));
        assert!(!hash.verify(TEST_PEPPER, "000000"));
    }

    /// §74.2: the whole point of the keyed construction — knowing the digest (equivalent to
    /// reading the DB row, which every non-owner runtime role can per §6.2.1's domain default)
    /// is not enough to forge a match without the pepper.
    #[test]
    fn code_hash_does_not_verify_under_a_different_pepper() {
        let (plaintext, hash) = generate_verification_code(TEST_PEPPER);
        assert!(!hash.verify(b"a-different-pepper", plaintext.reveal()));
    }

    // ---- §74.1 canonicalization: domain case only, no Gmail dot/plus folding ----

    #[test]
    fn canonicalize_lowercases_domain_only() {
        let (original, canonical) = canonicalize_email("First.Last+tag@EXAMPLE.COM").unwrap();
        assert_eq!(original, "First.Last+tag@EXAMPLE.COM");
        // Local part untouched (dots/plus preserved) — only the domain is lowercased.
        assert_eq!(canonical, "First.Last+tag@example.com");
    }

    #[test]
    fn canonicalize_rejects_missing_at() {
        assert_eq!(
            canonicalize_email("not-an-email").unwrap_err(),
            ErrorCode::InvalidInput
        );
    }

    /// 注错红转绿: prior to `rsplit_once` + the local-part `@`-reject, `a@b@c` silently
    /// canonicalized to a plausible-looking `a@b@c` (via `split_once`'s first-`@` split,
    /// local=`a`, domain=`b@c`) instead of being rejected — this is the trust-boundary input
    /// that must fail loudly.
    #[test]
    fn canonicalize_rejects_multiple_at() {
        assert_eq!(
            canonicalize_email("a@b@c").unwrap_err(),
            ErrorCode::InvalidInput
        );
    }

    // ---- §74.5 email change requires reauthentication first ----

    #[test]
    fn email_change_rejected_without_reauth() {
        let err = begin_email_change(false, "new@example.com", "old@example.com").unwrap_err();
        assert_eq!(err, ErrorCode::Forbidden);
    }

    #[test]
    fn email_change_rejects_no_op() {
        let err = begin_email_change(true, "same@example.com", "same@example.com").unwrap_err();
        assert_eq!(err, ErrorCode::InvalidInput);
    }

    #[test]
    fn email_change_accepted_with_reauth_and_new_address() {
        assert!(begin_email_change(true, "new@example.com", "old@example.com").is_ok());
    }

    struct RecordingNotifier {
        change_calls: std::cell::RefCell<Vec<(String, String)>>,
        reset_calls: std::cell::RefCell<Vec<String>>,
    }

    impl EmailNotifier for RecordingNotifier {
        fn notify_email_change(&self, old_email: &str, new_email: &str) -> Result<(), ErrorCode> {
            self.change_calls
                .borrow_mut()
                .push((old_email.to_string(), new_email.to_string()));
            Ok(())
        }
        fn send_verification_code(
            &self,
            _email: &str,
            _code: &PlaintextCode,
        ) -> Result<(), ErrorCode> {
            Ok(())
        }
        fn notify_password_reset_completed(&self, email: &str) -> Result<(), ErrorCode> {
            self.reset_calls.borrow_mut().push(email.to_string());
            Ok(())
        }
    }

    #[test]
    fn old_email_is_notified_through_injected_port() {
        let notifier = RecordingNotifier {
            change_calls: std::cell::RefCell::new(Vec::new()),
            reset_calls: std::cell::RefCell::new(Vec::new()),
        };
        notify_old_email(&notifier, "old@example.com", "new@example.com").unwrap();
        assert_eq!(
            notifier.change_calls.borrow().as_slice(),
            &[("old@example.com".to_string(), "new@example.com".to_string())]
        );
    }

    #[test]
    fn password_reset_completion_is_notified_through_injected_port() {
        let notifier = RecordingNotifier {
            change_calls: std::cell::RefCell::new(Vec::new()),
            reset_calls: std::cell::RefCell::new(Vec::new()),
        };
        notify_password_reset(&notifier, "user@example.com").unwrap();
        assert_eq!(
            notifier.reset_calls.borrow().as_slice(),
            &["user@example.com".to_string()]
        );
    }

    // ---- §77 auth_events metadata is typed through the allowlist, never raw jsonb ----

    #[test]
    fn auth_event_record_metadata_is_typed_through_the_allowlist() {
        let mut metadata = AuditMetadata::new();
        // "role" is one of `AuditMetadata`'s current closed-set keys (humaux_domain::audit,
        // owned by a different task, §77) — an auth-specific key this crate eventually needs
        // would be added to that allowlist by whoever owns it, not reimplemented here; this
        // test only proves `AuthEventRecord.metadata`'s type routes through it at all.
        metadata.insert("role", "member").unwrap();
        let record = AuthEventRecord {
            event_type: AuthEventType::LoginSuccess,
            metadata: metadata.clone(),
        };
        assert_eq!(record.metadata.len(), 1);
        // A password/token-shaped key never reaches an `AuthEventRecord` at all — the
        // allowlist rejects it at `AuditMetadata::insert` itself (humaux_domain::audit),
        // not reimplemented here.
        let mut bad = AuditMetadata::new();
        assert!(bad.insert("password", "x").is_err());
    }

    // ---- auth_events enum <-> SQL CHECK spelling, read from the migration file itself ----

    /// Extracts the quoted string literals inside a `CHECK (<column> IN (...))` clause from
    /// raw migration SQL text — enough parsing to catch drift between an enum and the DB
    /// constraint (§78.2), not a general SQL parser. Panics on malformed input; this only ever
    /// runs against this repo's own committed migration files, never untrusted input.
    fn extract_check_in_values(sql: &str, column: &str) -> Vec<String> {
        let marker = format!("CHECK ({column} IN (");
        let start = sql
            .find(&marker)
            .unwrap_or_else(|| panic!("`{marker}` not found in migration text"));
        let after_marker = &sql[start + marker.len()..];
        let end = after_marker
            .find("))")
            .expect("closing `))` of the CHECK (...) clause not found");
        after_marker[..end]
            .split(',')
            .map(|s| s.trim().trim_matches('\'').to_string())
            .collect()
    }

    /// Slices out one `CREATE TABLE <table> ( ... );` block from raw migration SQL text, so a
    /// column-name search (e.g. `state`, which several tables share) is scoped to the right
    /// table instead of matching the first occurrence anywhere in the file.
    fn extract_table_ddl<'a>(sql: &'a str, table: &str) -> &'a str {
        let marker = format!("CREATE TABLE {table} (");
        let start = sql
            .find(&marker)
            .unwrap_or_else(|| panic!("`{marker}` not found in migration text"));
        let rest = &sql[start..];
        let end = rest
            .find(");\n")
            .unwrap_or_else(|| panic!("end of `{table}`'s CREATE TABLE block not found"))
            + 2;
        &rest[..end]
    }

    /// Replaces a prior version of this test that compared `AuthEventType::as_db_str()`
    /// against a string-literal array declared in this same file — a copy of a copy, which
    /// stayed green through any edit to the migration's actual CHECK list (§78.2's exact
    /// failure mode). This version reads `migrations/0035_email_auth_identity.sql` itself
    /// (`include_str!`, compile-time only — no DB connection, matching this crate's zero-SQL-
    /// driver-dependency charter) and compares against that.
    #[test]
    fn auth_event_type_db_strings_match_migration_check_constraint() {
        let migration_sql = include_str!("../../../migrations/0035_email_auth_identity.sql");
        let db_values = extract_check_in_values(migration_sql, "event_type");
        let rust_values: Vec<String> = AuthEventType::ALL
            .iter()
            .map(|v| v.as_db_str().to_string())
            .collect();
        assert_eq!(
            db_values.len(),
            AuthEventType::ALL.len(),
            "count drift between AuthEventType::ALL and the migration's event_type CHECK list"
        );
        assert_eq!(
            db_values, rust_values,
            "AuthEventType variants (declaration order) must match migrations/\
             0035_email_auth_identity.sql's event_type CHECK list verbatim, order included"
        );
    }
}
