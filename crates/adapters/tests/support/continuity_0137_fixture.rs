//! `adapters::tests::support::continuity_0137_fixture` — Shared seed, role-DSN and run-bound diagnostic fixture for the 0137 continuity tests.
//! Depends-on: crates=[hex, humaux-adapters, humaux-application, humaux-domain, postgres, serde_json, sha2, tokio,
//!   uuid]; services=[PostgreSQL(owner) r=[ops.commit_seq_seq] w=[control.memberships,
//!   control.private_reasoning_domains, control.tenants, control.users, control.workspace_memberships,
//!   control.workspaces, ops.outbox, private.evidence_objects, private.memory_evidence, private.memory_records,
//!   projection.stream_log] x=[private.publish_continuity_facet, private.register_continuity_project],
//!   PostgreSQL(role_gateway)]; env=[HUMAUX_REQUIRE_DB, HUMAUX_TEST_PG_DSN, HUMAUX_W2_V4_DIAGNOSTIC,
//!   HUMAUX_W2_V4_DIAGNOSTIC_DIR, HUMAUX_W2_V4_DIAGNOSTIC_RUN_UUID]; modules=[adapters::continuity_read,
//!   adapters::postgres, adapters::tests::support::continuity_0137_cleanup, application::continuity, domain::context,
//!   domain::continuity, domain::error, domain::identity, domain::ids]
//! Called-by: [adapters::tests::project_continuity_read_0137_acceptance]
//! Invariants: [seeds control, source and continuity rows as owner and reads back as role_gateway; every seeded id is
//!   registered for cleanup; diagnostic output is bound to one run UUID; a missing DB fails when HUMAUX_REQUIRE_DB=1]
//! Spec: ADR-0035
//!
use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use humaux_adapters::{continuity_read::PostgresContinuityReadPort, postgres::RuntimeDbPool};
use humaux_application::continuity::read_project_continuity;
use humaux_domain::{
    context::ContextBudget,
    continuity::ProjectId,
    error::ErrorCode,
    identity::{AuthorizationScope, BoundedSet, PrincipalId},
    ids::{TenantId, UserId, WorkspaceId},
};
use postgres::{Client, GenericClient, NoTls};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[path = "continuity_0137_cleanup.rs"]
mod continuity_0137_cleanup;
pub use continuity_0137_cleanup::CleanupOwner;

const NIL: Uuid = Uuid::nil();
const DIAGNOSTIC_ENV: &str = "HUMAUX_W2_V4_DIAGNOSTIC";
const DIAGNOSTIC_UUID_ENV: &str = "HUMAUX_W2_V4_DIAGNOSTIC_RUN_UUID";
const DIAGNOSTIC_DIR_ENV: &str = "HUMAUX_W2_V4_DIAGNOSTIC_DIR";
const TEST_NAMES: [&str; 13] = [
    "live_role_acl_and_read_only_attempt_are_exact",
    "malformed_authority_context_and_workspace_arrays_fail_closed",
    "structural_damage_and_unrepresentable_corruption_fail_closed",
    "raw_parallel_array_mispairing_never_yields_current_or_complete",
    "memory_eligibility_visibility_grounding_and_lifecycle_matrix",
    "direct_evidence_truth_table_is_fail_closed",
    "evidence_association_query_failure_is_dependency_unavailable",
    "directed_fault_guard_panic_cleanup_preserves_global_acl",
    "two_connection_rr_revoke_and_tombstone_are_coherent",
    "terminated_source_revalidation_is_bounded_and_has_no_residue",
    "statement_timeout_is_bounded_rollback_no_partial_no_retry",
    "restart_semantic_projection_probe",
    "adapter_static_external_and_write_plane_is_zero",
];
static DIAGNOSTIC_WRITE_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug)]
struct DiagnosticInput {
    run_simple: String,
    run_hyphenated: String,
    dir: PathBuf,
}

#[derive(Clone, Debug)]
struct Diagnostic {
    input: DiagnosticInput,
    test_name: &'static str,
    test8: String,
    pid6: String,
}

fn decode_run_uuid(value: &str) -> Result<([u8; 16], String), String> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("diagnostic run UUID must be exactly 32 lowercase hex characters".into());
    }
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "diagnostic run UUID contains invalid hex")?;
    }
    let hyphenated = Uuid::from_bytes(bytes).hyphenated().to_string();
    if hyphenated.replace('-', "") != value {
        return Err("diagnostic run UUID did not round-trip canonically".into());
    }
    Ok((bytes, hyphenated))
}

fn parse_diagnostic_values(
    values: [Option<OsString>; 3],
) -> Result<Option<DiagnosticInput>, String> {
    if values.iter().all(Option::is_none) {
        return Ok(None);
    }
    if values.iter().any(Option::is_none) {
        return Err("diagnostic environment triad is partial".into());
    }
    let [flag, run, dir] = values.map(Option::unwrap);
    let flag = flag.to_str().ok_or("diagnostic flag is not Unicode")?;
    let run = run.to_str().ok_or("diagnostic run UUID is not Unicode")?;
    let dir = dir.to_str().ok_or("diagnostic directory is not Unicode")?;
    if flag != "1" {
        return Err("diagnostic flag must be exactly 1".into());
    }
    let (_, run_hyphenated) = decode_run_uuid(run)?;
    if dir.is_empty() {
        return Err("diagnostic directory is empty".into());
    }
    Ok(Some(DiagnosticInput {
        run_simple: run.to_owned(),
        run_hyphenated,
        dir: PathBuf::from(dir),
    }))
}

fn started_binds_run(bytes: &[u8], input: &DiagnosticInput) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
        return false;
    };
    value
        .get("run_uuid")
        .and_then(Value::as_str)
        .is_some_and(|run| {
            Uuid::parse_str(run)
                .map(|uuid| {
                    uuid.simple().to_string() == input.run_simple
                        && uuid.hyphenated().to_string() == input.run_hyphenated
                })
                .unwrap_or(false)
        })
}

fn validate_diagnostic_dir(input: &DiagnosticInput) -> Result<(), String> {
    if !input.dir.is_absolute() {
        return Err("diagnostic directory must be absolute".into());
    }
    let canonical = input
        .dir
        .canonicalize()
        .map_err(|error| format!("diagnostic directory cannot be canonicalized: {error}"))?;
    if canonical.as_os_str() != input.dir.as_os_str() {
        return Err("diagnostic directory path is not byte-canonical".into());
    }
    let mut current = PathBuf::new();
    for component in input.dir.components() {
        current.push(component.as_os_str());
        let metadata = fs::symlink_metadata(&current)
            .map_err(|error| format!("diagnostic path component is unreadable: {error}"))?;
        if metadata.file_type().is_symlink() {
            return Err("diagnostic directory contains a symbolic-link component".into());
        }
    }
    if !input.dir.is_dir() {
        return Err("diagnostic path is not a directory".into());
    }
    let started = input.dir.join("STARTED");
    let metadata = fs::symlink_metadata(&started)
        .map_err(|error| format!("diagnostic STARTED sentinel is missing: {error}"))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err("diagnostic STARTED sentinel must be a regular non-symlink file".into());
    }
    if metadata.len() > 1_048_576 {
        return Err("diagnostic STARTED sentinel is unreasonably large".into());
    }
    let bytes = fs::read(&started)
        .map_err(|error| format!("diagnostic STARTED sentinel is unreadable: {error}"))?;
    if !started_binds_run(&bytes, input) {
        return Err("diagnostic STARTED sentinel run binding mismatch".into());
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn test_tag(test_name: &str) -> String {
    sha256_hex(test_name.as_bytes())[..8].to_owned()
}

fn base36_fixed_6(mut value: u32) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut encoded = [b'0'; 6];
    for slot in encoded.iter_mut().rev() {
        *slot = DIGITS[(value % 36) as usize];
        value /= 36;
    }
    assert_eq!(value, 0, "OS PID exceeds fixed-width base36 pid6");
    String::from_utf8(encoded.to_vec()).unwrap()
}

fn diagnostic_from_values(
    test_name: &'static str,
    values: [Option<OsString>; 3],
    validate_dir: bool,
) -> Result<Option<Diagnostic>, String> {
    let Some(input) = parse_diagnostic_values(values)? else {
        return Ok(None);
    };
    if !TEST_NAMES.contains(&test_name) {
        return Err(format!("unknown diagnostic test identity {test_name}"));
    }
    let mut tags: Vec<_> = TEST_NAMES.iter().map(|name| test_tag(name)).collect();
    tags.sort_unstable();
    tags.dedup();
    if tags.len() != TEST_NAMES.len() {
        return Err("diagnostic test8 mapping is not bijective".into());
    }
    if validate_dir {
        validate_diagnostic_dir(&input)?;
    }
    let diagnostic = Diagnostic {
        input,
        test_name,
        test8: test_tag(test_name),
        pid6: base36_fixed_6(std::process::id()),
    };
    for role in ["ad", "gw", "ob"] {
        diagnostic.application_name(role)?;
    }
    Ok(Some(diagnostic))
}

fn diagnostic_from_env(test_name: &'static str) -> Result<Option<Diagnostic>, String> {
    diagnostic_from_values(
        test_name,
        [
            std::env::var_os(DIAGNOSTIC_ENV),
            std::env::var_os(DIAGNOSTIC_UUID_ENV),
            std::env::var_os(DIAGNOSTIC_DIR_ENV),
        ],
        true,
    )
}

pub fn preflight_test(test_name: &'static str) {
    if let Some(diagnostic) = diagnostic_from_env(test_name)
        .unwrap_or_else(|error| panic!("W2 v4.2 diagnostic preflight failed: {error}"))
    {
        if let Some(thread_name) = std::thread::current().name() {
            assert_eq!(
                thread_name, test_name,
                "diagnostic libtest-name cross-check"
            );
        }
        diagnostic.record_identity();
    }
}

fn bind_application_name(base: &str, application_name: &str) -> String {
    let (prefix, query) = base.split_once('?').unwrap_or((base, ""));
    let retained = query
        .split('&')
        .filter(|item| !item.is_empty() && !item.starts_with("application_name="))
        .collect::<Vec<_>>()
        .join("&");
    if retained.is_empty() {
        format!("{prefix}?application_name={application_name}")
    } else {
        format!("{prefix}?{retained}&application_name={application_name}")
    }
}

impl Diagnostic {
    fn application_name(&self, role: &str) -> Result<String, String> {
        if !matches!(role, "ad" | "gw" | "ob") {
            return Err(format!("invalid diagnostic role code {role}"));
        }
        let name = format!(
            "W2:{}:{}:{}:{}",
            &self.input.run_simple[..16],
            self.pid6,
            self.test8,
            role
        );
        if name.len() > 63 || !name.is_ascii() {
            return Err("diagnostic application_name exceeds 63 UTF-8 bytes".into());
        }
        Ok(name)
    }

    fn event_path(&self) -> PathBuf {
        self.input
            .dir
            .join(format!("adapter-{}-{}.jsonl", self.pid6, self.test8))
    }

    fn append(&self, mut event: Value) {
        let object = event
            .as_object_mut()
            .expect("diagnostic event must be a JSON object");
        object.insert("run_uuid".into(), json!(self.input.run_hyphenated));
        object.insert("os_pid".into(), json!(std::process::id()));
        object.insert("test_name".into(), json!(self.test_name));
        object.insert("test8".into(), json!(self.test8));
        object.insert(
            "unix_nanos".into(),
            json!(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system time before UNIX epoch")
                    .as_nanos()
                    .to_string()
            ),
        );
        let mut bytes = serde_json::to_vec(&event).expect("serialize diagnostic event");
        bytes.push(b'\n');
        let _guard = DIAGNOSTIC_WRITE_LOCK.lock().unwrap();
        let path = self.event_path();
        if fs::symlink_metadata(&path)
            .map(|metadata| metadata.file_type().is_symlink() || !metadata.is_file())
            .unwrap_or(false)
        {
            panic!("diagnostic event path is not a regular file");
        }
        let (mut file, created) = match OpenOptions::new().create_new(true).append(true).open(&path)
        {
            Ok(file) => (file, true),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (
                OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .expect("open diagnostic event stream"),
                false,
            ),
            Err(error) => panic!("create diagnostic event stream: {error}"),
        };
        file.write_all(&bytes).expect("append diagnostic event");
        file.sync_all().expect("sync diagnostic event stream");
        if created {
            File::open(&self.input.dir)
                .and_then(|directory| directory.sync_all())
                .expect("sync diagnostic event directory");
        }
    }

    fn record_identity(&self) {
        self.append(json!({
            "event": "IDENTITY_MAP",
            "full_test_name": self.test_name,
            "short_test_tag": self.test8,
            "pid6": self.pid6,
            "roles": ["ad", "gw", "ob"],
        }));
    }

    fn record_backend(&self, role: &str, application_name: &str, backend_pid: i32) {
        self.append(json!({
            "event": "BACKEND_IDENTITY",
            "role": role,
            "application_name": application_name,
            "backend_pid": backend_pid,
        }));
    }

    fn record_ddl(
        &self,
        phase: &str,
        operation: &str,
        identities: &Value,
        sql: &str,
        result: Option<&Result<(), postgres::Error>>,
    ) {
        let mut event = json!({
            "event": phase,
            "operation": operation,
            "object_identities": identities,
            "sql_sha256": sha256_hex(sql.as_bytes()),
        });
        if phase == "DDL_INTENT" {
            event["sql"] = json!(sql);
        }
        if let Some(result) = result {
            match result {
                Ok(()) => event["outcome"] = json!("ok"),
                Err(error) => {
                    event["outcome"] = json!("error");
                    event["error"] = json!(error.to_string());
                }
            }
        }
        self.append(event);
    }
}

pub fn assert_diagnostic_pure_cases() {
    let run = "00112233445566778899aabbccddeeff";
    let values = |flag: Option<&str>, uuid: Option<&str>, dir: Option<&str>| {
        [
            flag.map(OsString::from),
            uuid.map(OsString::from),
            dir.map(OsString::from),
        ]
    };
    assert!(
        parse_diagnostic_values(values(None, None, None))
            .unwrap()
            .is_none()
    );
    let input = parse_diagnostic_values(values(Some("1"), Some(run), Some("/tmp/w2")))
        .unwrap()
        .unwrap();
    assert_eq!(input.run_hyphenated, "00112233-4455-6677-8899-aabbccddeeff");
    assert_eq!(input.run_hyphenated.replace('-', ""), run);
    assert!(started_binds_run(
        br#"{"run_uuid":"00112233-4455-6677-8899-aabbccddeeff"}"#,
        &input
    ));
    assert!(!started_binds_run(
        br#"{"run_uuid":"00112233-4455-6677-8899-aabbccddee00"}"#,
        &input
    ));
    for partial in [
        values(Some("1"), None, None),
        values(None, Some(run), None),
        values(None, None, Some("/tmp/w2")),
        values(Some("1"), Some(run), None),
        values(Some("1"), None, Some("/tmp/w2")),
        values(None, Some(run), Some("/tmp/w2")),
    ] {
        assert!(parse_diagnostic_values(partial).is_err());
    }
    for malformed in [
        values(Some("0"), Some(run), Some("/tmp/w2")),
        values(Some("1"), Some(""), Some("/tmp/w2")),
        values(
            Some("1"),
            Some("00112233445566778899AABBCCDDEEFF"),
            Some("/tmp/w2"),
        ),
        values(
            Some("1"),
            Some("00112233-4455-6677-8899-aabbccddeeff"),
            Some("/tmp/w2"),
        ),
        values(Some("1"), Some(run), Some("")),
    ] {
        assert!(parse_diagnostic_values(malformed).is_err());
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        for index in 0..3 {
            let mut non_unicode = values(Some("1"), Some(run), Some("/tmp/w2"));
            non_unicode[index] = Some(OsString::from_vec(vec![0xff]));
            assert!(parse_diagnostic_values(non_unicode).is_err());
        }
    }
    let diagnostic = diagnostic_from_values(
        "adapter_static_external_and_write_plane_is_zero",
        values(Some("1"), Some(run), Some("/tmp/w2")),
        false,
    )
    .unwrap()
    .unwrap();
    for role in ["ad", "gw", "ob"] {
        let name = diagnostic.application_name(role).unwrap();
        assert!(name.len() <= 63);
        assert_eq!(name, diagnostic.application_name(role).unwrap());
    }
    assert_eq!(base36_fixed_6(0), "000000");
    assert_eq!(base36_fixed_6(35), "00000z");
    assert_eq!(
        bind_application_name(
            "postgres://host/db?sslmode=disable&application_name=old",
            "W2:new"
        ),
        "postgres://host/db?sslmode=disable&application_name=W2:new"
    );
}

pub fn required_dsn() -> Option<String> {
    match std::env::var("HUMAUX_TEST_PG_DSN") {
        Ok(dsn) => Some(dsn),
        Err(_) if std::env::var("HUMAUX_REQUIRE_DB").as_deref() == Ok("1") => {
            panic!("HUMAUX_REQUIRE_DB=1 requires isolated HUMAUX_TEST_PG_DSN")
        }
        Err(_) => None,
    }
}

pub fn role_dsn(base: &str, role: &str, password: &str, app: &str) -> String {
    // Swap the credential pair of whatever admin DSN the caller supplied; the admin password
    // is not fixed by contract (local dev, CI and compose each use a different one), so a
    // hardcoded `postgres:postgres@` prefix makes this fixture panic everywhere but one
    // machine. Same shape as `dsn_as_role` in tests/mandatory_context_lane.rs.
    let suffix = base
        .strip_prefix("postgres://")
        .or_else(|| base.strip_prefix("postgresql://"))
        .and_then(|rest| rest.split_once('@').map(|(_creds, host)| host))
        .expect("HUMAUX_TEST_PG_DSN must be postgres://<user>:<password>@<host>/<db>");
    let separator = if suffix.contains('?') { '&' } else { '?' };
    format!("postgres://{role}:{password}@{suffix}{separator}application_name={app}")
}

pub fn set_context(
    client: &mut impl GenericClient,
    tenant: Uuid,
    workspace: Uuid,
    principal: Uuid,
    user: Option<Uuid>,
) {
    client
        .query_one(
            "SELECT set_config('humaux.tenant_id',$1,true), \
                    set_config('humaux.workspace_id',$2,true), \
                    set_config('humaux.principal_id',$3,true), \
                    set_config('humaux.user_id',$4,true)",
            &[
                &tenant.to_string(),
                &workspace.to_string(),
                &principal.to_string(),
                &user.unwrap_or(NIL).to_string(),
            ],
        )
        .expect("set continuity context");
}

#[derive(Clone, Debug)]
pub struct Fixture {
    pub admin_dsn: String,
    pub gateway_dsn: String,
    pub application_name: String,
    pub tenant: Uuid,
    pub user: Uuid,
    pub other_user: Uuid,
    pub principal: Uuid,
    pub workspace: Uuid,
    pub other_workspace: Uuid,
    pub project: Uuid,
    pub memory: Uuid,
    pub backing_evidence: Uuid,
    pub goal_v1: Uuid,
    diagnostic: Option<Diagnostic>,
    cleanup: Arc<CleanupOwner>,
}

struct SeedIds {
    tenant: Uuid,
    user: Uuid,
    other_user: Uuid,
    workspace: Uuid,
    other_workspace: Uuid,
    domain: Uuid,
    memory: Uuid,
    backing_evidence: Uuid,
}

fn register_seed_ids(cleanup: &CleanupOwner, ids: &SeedIds, project: Uuid) {
    cleanup.register_tenant(ids.tenant);
    cleanup.register_user(ids.user);
    cleanup.register_user(ids.other_user);
    cleanup.register_workspace(ids.workspace);
    cleanup.register_workspace(ids.other_workspace);
    cleanup.register_domain(ids.domain);
    cleanup.register_project(project);
    cleanup.register_memory(ids.memory);
    cleanup.register_evidence(ids.backing_evidence);
}

fn seed_control(admin: &mut Client, ids: &SeedIds) {
    admin
        .execute(
            "INSERT INTO control.tenants(tenant_id,name,state) VALUES($1,'w2 acceptance','ACTIVE')",
            &[&ids.tenant],
        )
        .unwrap();
    for id in [ids.user, ids.other_user] {
        admin
            .execute(
                "INSERT INTO control.users(user_id,state) VALUES($1,'ACTIVE')",
                &[&id],
            )
            .unwrap();
    }
    admin
        .execute(
            "INSERT INTO control.memberships(tenant_id,user_id,role,state) \
             VALUES($1,$2,'OWNER','ACTIVE')",
            &[&ids.tenant, &ids.user],
        )
        .unwrap();
    for id in [ids.workspace, ids.other_workspace] {
        admin
            .execute(
                "INSERT INTO control.workspaces(workspace_id,tenant_id,name) \
                 VALUES($1,$2,'w2 acceptance')",
                &[&id, &ids.tenant],
            )
            .unwrap();
    }
    // ADR-0035 (card 13): 0163 re-points the WORKSPACE_SHARED visibility arm from
    // control.memberships (tenant membership) to an ACTIVE control.workspace_memberships row
    // for the row's OWN workspace. `ids.user` is made a member of BOTH `ids.workspace` and
    // `ids.other_workspace` here: this is only the DB-row-visibility gate, and
    // `direct_evidence_truth_table_is_fail_closed`'s `VisibilityHidden` case needs the raw
    // `private.evidence_objects` SELECT in `continuity_read::validate_evidence` to still return
    // the row (else it misclassifies as SOURCE_REVALIDATION_FAILED instead of
    // VISIBILITY_REVALIDATION_FAILED). The actual app-level "is `other_workspace` in scope"
    // narrowing is `fixture.read()`'s own `AuthorizationScope`, hardcoded below to
    // `BoundedSet::new([WorkspaceId(self.workspace)])` — never `other_workspace` — so
    // `WorkspaceHidden`/`can_read` still correctly reject it regardless of this DB-level grant.
    for id in [ids.workspace, ids.other_workspace] {
        admin
            .execute(
                "INSERT INTO control.workspace_memberships(tenant_id,workspace_id,user_id,role,state) \
                 VALUES($1,$2,$3,'MEMBER','ACTIVE')",
                &[&ids.tenant, &id, &ids.user],
            )
            .unwrap();
    }
    admin
        .execute(
            "INSERT INTO control.private_reasoning_domains( \
               reasoning_domain_id,tenant_id,name) VALUES($1,$2,'w2 acceptance')",
            &[&ids.domain, &ids.tenant],
        )
        .unwrap();
}

fn seed_sources(admin: &mut Client, ids: &SeedIds) {
    admin.batch_execute("BEGIN").unwrap();
    admin
        .execute(
            "INSERT INTO private.evidence_objects( \
               evidence_id,tenant_id,evidence_kind,payload_sha256,data_class,origin_class, \
               visibility_class,reasoning_domain_id) \
             VALUES($1,$2,'EVENT',$3,'PRIVATE','AuthenticatedAgent','TENANT_SHARED',$4)",
            &[
                &ids.backing_evidence,
                &ids.tenant,
                &vec![0x44_u8; 32],
                &ids.domain,
            ],
        )
        .unwrap();
    admin
        .execute(
            "INSERT INTO private.memory_records( \
               memory_id,tenant_id,memory_type,content,visibility_class,authority_class, \
               confidence,status,asserted_at) \
             VALUES($1,$2,'FACT',$3,'TENANT_SHARED','ProjectDecision',1,'active',now())",
            &[
                &ids.memory,
                &ids.tenant,
                &json!({"w2":"authoritative memory"}),
            ],
        )
        .unwrap();
    admin
        .execute(
            "INSERT INTO private.memory_evidence( \
               memory_id,evidence_id,role,grounding_mode,recorded_version) \
             VALUES($1,$2,'SUPPORTING','IMMUTABLE',NULL)",
            &[&ids.memory, &ids.backing_evidence],
        )
        .unwrap();
    admin.batch_execute("COMMIT").unwrap();
}

impl Fixture {
    fn gateway_identity(
        admin_dsn: &str,
        diagnostic: Option<&Diagnostic>,
        fallback_application_name: String,
    ) -> (String, String) {
        let application_name = diagnostic
            .map(|diagnostic| diagnostic.application_name("gw").unwrap())
            .unwrap_or(fallback_application_name);
        let gateway_dsn = role_dsn(
            admin_dsn,
            "role_gateway",
            "devlocal_role_gateway",
            &application_name,
        );
        let gateway_dsn = if diagnostic.is_some() {
            bind_application_name(&gateway_dsn, &application_name)
        } else {
            gateway_dsn
        };
        (application_name, gateway_dsn)
    }

    pub fn new(admin_dsn: String, test_name: &'static str) -> Self {
        let cleanup = Arc::new(CleanupOwner::new(admin_dsn.clone()));
        let diagnostic = diagnostic_from_env(test_name)
            .unwrap_or_else(|error| panic!("W2 v4.2 diagnostic fixture failed: {error}"));
        let (application_name, gateway_dsn) = Self::gateway_identity(
            &admin_dsn,
            diagnostic.as_ref(),
            format!("continuity_w2_acceptance_{}", Uuid::now_v7().simple()),
        );
        let tenant = Uuid::now_v7();
        let user = Uuid::now_v7();
        let other_user = Uuid::now_v7();
        let principal = Uuid::now_v7();
        let workspace = Uuid::now_v7();
        let other_workspace = Uuid::now_v7();
        let project = Uuid::now_v7();
        let domain = Uuid::now_v7();
        let memory = Uuid::now_v7();
        let backing_evidence = Uuid::now_v7();
        let ids = SeedIds {
            tenant,
            user,
            other_user,
            workspace,
            other_workspace,
            domain,
            memory,
            backing_evidence,
        };
        register_seed_ids(&cleanup, &ids, project);
        let mut admin =
            Self::connect_admin_dsn(&admin_dsn, diagnostic.as_ref()).expect("admin connect");
        seed_control(&mut admin, &ids);
        seed_sources(&mut admin, &ids);
        let memory_hash: Vec<u8> = admin
            .query_one(
                "SELECT sha256(convert_to(content::text,'UTF8')) \
                 FROM private.memory_records WHERE tenant_id=$1 AND memory_id=$2",
                &[&tenant, &memory],
            )
            .unwrap()
            .get(0);
        let mut gateway = Self::connect_observed(
            &gateway_dsn,
            diagnostic.as_ref(),
            "gw",
            Some(&application_name),
        )
        .expect("gateway connect");
        gateway.batch_execute("BEGIN").unwrap();
        set_context(&mut gateway, tenant, workspace, principal, Some(user));
        gateway
            .query_one(
                "SELECT private.register_continuity_project($1,$2,$3,$4,$5,'w2 acceptance')",
                &[&tenant, &workspace, &project, &principal, &Some(user)],
            )
            .unwrap();
        gateway.batch_execute("COMMIT; BEGIN").unwrap();
        set_context(&mut gateway, tenant, workspace, principal, Some(user));
        let goal_v1 = gateway
            .query_one(
                "SELECT facet_version_id FROM private.publish_continuity_facet( \
                   $1,$2,$3,$4,$5,'GOAL',0,'CURRENT',$6,$7,$8,$9,$10)",
                &[
                    &tenant,
                    &workspace,
                    &project,
                    &principal,
                    &Some(user),
                    &json!({"goal":"ship W2"}),
                    &vec![memory],
                    &vec![memory_hash],
                    &Vec::<Uuid>::new(),
                    &Vec::<Vec<u8>>::new(),
                ],
            )
            .unwrap()
            .get(0);
        gateway.batch_execute("COMMIT").unwrap();
        Self {
            admin_dsn,
            gateway_dsn,
            application_name,
            tenant,
            user,
            other_user,
            principal,
            workspace,
            other_workspace,
            project,
            memory,
            backing_evidence,
            goal_v1,
            diagnostic,
            cleanup,
        }
    }

    /// Exercises the constructor failure boundary: the owner is armed and its first exact
    /// tenant ID is registered before the simulated later seed failure unwinds the stack.
    pub fn panic_after_partial_seed(admin_dsn: String, tenant: Uuid) -> ! {
        let cleanup = Arc::new(CleanupOwner::new(admin_dsn.clone()));
        cleanup.register_tenant(tenant);
        // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
        let mut admin = Client::connect(&admin_dsn, NoTls).expect("partial-seed admin connect");
        admin
            .execute(
                "INSERT INTO control.tenants(tenant_id,name,state) VALUES($1,'w2 partial','ACTIVE')",
                &[&tenant],
            )
            .expect("partial-seed tenant");
        panic!("forced partial fixture constructor failure")
    }

    pub fn from_state(admin_dsn: String, state: &Value, test_name: &'static str) -> Self {
        let diagnostic = diagnostic_from_env(test_name)
            .unwrap_or_else(|error| panic!("W2 v4.2 diagnostic fixture failed: {error}"));
        let get = |name: &str| {
            Uuid::parse_str(state[name].as_str().expect("restart UUID string")).unwrap()
        };
        let (application_name, gateway_dsn) = Self::gateway_identity(
            &admin_dsn,
            diagnostic.as_ref(),
            format!("continuity_w2_restart_{}", Uuid::now_v7().simple()),
        );
        let cleanup = Arc::new(
            CleanupOwner::from_state(admin_dsn.clone(), &state["cleanup_ledger"])
                .expect("restart state cleanup ledger"),
        );
        Self {
            admin_dsn,
            gateway_dsn,
            application_name,
            tenant: get("tenant"),
            user: get("user"),
            other_user: get("other_user"),
            principal: get("principal"),
            workspace: get("workspace"),
            other_workspace: get("other_workspace"),
            project: get("project"),
            memory: get("memory"),
            backing_evidence: get("backing_evidence"),
            goal_v1: get("goal_v1"),
            diagnostic,
            cleanup,
        }
    }

    pub fn restart_state(&self) -> Value {
        let mut state = json!({
            "tenant": self.tenant.to_string(),
            "user": self.user.to_string(),
            "other_user": self.other_user.to_string(),
            "principal": self.principal.to_string(),
            "workspace": self.workspace.to_string(),
            "other_workspace": self.other_workspace.to_string(),
            "project": self.project.to_string(),
            "memory": self.memory.to_string(),
            "backing_evidence": self.backing_evidence.to_string(),
            "goal_v1": self.goal_v1.to_string(),
        });
        state["cleanup_ledger"] = self.cleanup.ledger_json();
        state
    }

    pub fn cleanup_now(&self) -> Result<(), String> {
        self.cleanup.cleanup_now()
    }

    pub fn disarm_cleanup(&self) -> Result<(), String> {
        let owners = Arc::strong_count(&self.cleanup);
        if owners != 1 {
            return Err(format!(
                "restart cleanup transfer requires one Arc owner, found {owners}"
            ));
        }
        self.cleanup.disarm();
        Ok(())
    }

    pub fn rearm_cleanup(&self) {
        self.cleanup.rearm();
    }

    fn connect_admin_dsn(
        admin_dsn: &str,
        diagnostic: Option<&Diagnostic>,
    ) -> Result<Client, postgres::Error> {
        let (dsn, application_name) = match diagnostic {
            Some(diagnostic) => {
                let application_name = diagnostic.application_name("ad").unwrap();
                (
                    bind_application_name(admin_dsn, &application_name),
                    Some(application_name),
                )
            }
            None => (admin_dsn.to_owned(), None),
        };
        Self::connect_observed(&dsn, diagnostic, "ad", application_name.as_deref())
    }

    fn connect_observed(
        dsn: &str,
        diagnostic: Option<&Diagnostic>,
        role: &str,
        application_name: Option<&str>,
    ) -> Result<Client, postgres::Error> {
        // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
        let mut client = Client::connect(dsn, NoTls)?;
        if let (Some(diagnostic), Some(application_name)) = (diagnostic, application_name) {
            let backend_pid = client.query_one("SELECT pg_backend_pid()", &[])?.get(0);
            diagnostic.record_backend(role, application_name, backend_pid);
        }
        Ok(client)
    }

    pub fn try_admin(&self) -> Result<Client, postgres::Error> {
        Self::connect_admin_dsn(&self.admin_dsn, self.diagnostic.as_ref())
    }

    pub fn admin(&self) -> Client {
        self.try_admin().expect("admin connect")
    }

    pub fn gateway(&self, app: &str) -> Client {
        self.gateway_as("role_gateway", "devlocal_role_gateway", app)
    }

    pub fn gateway_as(&self, role: &str, password: &str, app: &str) -> Client {
        self.try_gateway_as(role, password, app)
            .expect("gateway connect")
    }

    pub fn try_gateway_as(
        &self,
        role: &str,
        password: &str,
        app: &str,
    ) -> Result<Client, postgres::Error> {
        let application_name = self
            .diagnostic
            .as_ref()
            .map(|diagnostic| diagnostic.application_name("gw").unwrap())
            .unwrap_or_else(|| app.to_owned());
        let dsn = role_dsn(&self.admin_dsn, role, password, &application_name);
        let dsn = if self.diagnostic.is_some() {
            bind_application_name(&dsn, &application_name)
        } else {
            dsn
        };
        Self::connect_observed(
            &dsn,
            self.diagnostic.as_ref(),
            "gw",
            Some(&application_name),
        )
    }

    pub fn execute_ddl_batch(
        &self,
        admin: &mut Client,
        operation: &str,
        identities: &Value,
        sql: &str,
    ) -> Result<(), postgres::Error> {
        let Some(diagnostic) = &self.diagnostic else {
            return admin.batch_execute(sql);
        };
        diagnostic.append(json!({
            "event": "DDL_EXPECTED",
            "operation": operation,
            "object_identities": identities,
        }));
        diagnostic.record_ddl("DDL_INTENT", operation, identities, sql, None);
        let result = admin.batch_execute(sql);
        diagnostic.record_ddl("DDL_RESULT", operation, identities, sql, Some(&result));
        result
    }

    pub fn record_backend(&self, role: &str, application_name: &str, backend_pid: i32) {
        if let Some(diagnostic) = &self.diagnostic {
            diagnostic.record_backend(role, application_name, backend_pid);
        }
    }

    pub fn authorization(&self) -> AuthorizationScope {
        AuthorizationScope::new(
            TenantId(self.tenant),
            PrincipalId(self.principal),
            Some(UserId(self.user)),
            BoundedSet::new([WorkspaceId(self.workspace)]).unwrap(),
        )
    }

    pub fn read(&self) -> Result<Value, ErrorCode> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let pool = runtime.block_on(async {
            Arc::new(
                // dep: PostgreSQL(role_gateway) — test opens a direct PG connection for setup/verification
                RuntimeDbPool::connect(&self.gateway_dsn)
                    .await
                    .expect("checked gateway pool"),
            )
        });
        if self.diagnostic.is_some() {
            let mut admin = self.admin();
            let rows = admin
                .query(
                    "SELECT pid FROM pg_stat_activity WHERE application_name=$1 ORDER BY pid",
                    &[&self.application_name],
                )
                .expect("observe gateway pool backend PID");
            assert!(
                !rows.is_empty(),
                "gateway pool backend identity is observable"
            );
            for row in rows {
                self.record_backend("gw", &self.application_name, row.get(0));
            }
        }
        runtime.block_on(async {
            let adapter = PostgresContinuityReadPort::new(pool);
            read_project_continuity(
                &adapter,
                &self.authorization(),
                ProjectId(self.project),
                None,
                ContextBudget::new(10_000, 10_000).unwrap(),
            )
            .await
            .and_then(|value| serde_json::to_value(value).map_err(|_| ErrorCode::Internal))
        })
    }

    pub fn publish_memory_successor(&self) -> Uuid {
        let mut admin = self.admin();
        let memory_hash: Vec<u8> = admin
            .query_one(
                "SELECT sha256(convert_to(content::text,'UTF8')) FROM private.memory_records \
                 WHERE tenant_id=$1 AND memory_id=$2",
                &[&self.tenant, &self.memory],
            )
            .unwrap()
            .get(0);
        let mut gateway = self.gateway("continuity_acceptance_memory_successor");
        gateway.batch_execute("BEGIN").unwrap();
        set_context(
            &mut gateway,
            self.tenant,
            self.workspace,
            self.principal,
            Some(self.user),
        );
        let id = gateway
            .query_one(
                "SELECT facet_version_id FROM private.publish_continuity_facet( \
                 $1,$2,$3,$4,$5,'GOAL',1,'CURRENT',$6,$7,$8,$9,$10)",
                &[
                    &self.tenant,
                    &self.workspace,
                    &self.project,
                    &self.principal,
                    &Some(self.user),
                    &json!({"goal":"successor"}),
                    &vec![self.memory],
                    &vec![memory_hash],
                    &Vec::<Uuid>::new(),
                    &Vec::<Vec<u8>>::new(),
                ],
            )
            .unwrap()
            .get(0);
        gateway.batch_execute("COMMIT").unwrap();
        id
    }

    pub fn create_memory(&self, content: Value) -> Uuid {
        let memory = Uuid::now_v7();
        self.cleanup.register_memory(memory);
        let mut admin = self.admin();
        admin.batch_execute("BEGIN").unwrap();
        admin
            .execute(
                "INSERT INTO private.memory_records( \
                   memory_id,tenant_id,memory_type,content,visibility_class,authority_class, \
                   confidence,status,asserted_at) \
                 VALUES($1,$2,'FACT',$3,'TENANT_SHARED','ProjectDecision',1,'active',now())",
                &[&memory, &self.tenant, &content],
            )
            .unwrap();
        admin
            .execute(
                "INSERT INTO private.memory_evidence( \
                   memory_id,evidence_id,role,grounding_mode,recorded_version) \
                 VALUES($1,$2,'SUPPORTING','IMMUTABLE',NULL)",
                &[&memory, &self.backing_evidence],
            )
            .unwrap();
        admin.batch_execute("COMMIT").unwrap();
        memory
    }

    pub fn publish_goal_memories(&self, expected_version: i64, memories: &[Uuid]) -> Uuid {
        let mut memories = memories.to_vec();
        memories.sort_unstable();
        let mut admin = self.admin();
        let hashes: Vec<Vec<u8>> = memories
            .iter()
            .map(|memory| {
                admin
                    .query_one(
                        "SELECT sha256(convert_to(content::text,'UTF8')) \
                         FROM private.memory_records WHERE tenant_id=$1 AND memory_id=$2",
                        &[&self.tenant, memory],
                    )
                    .unwrap()
                    .get(0)
            })
            .collect();
        let mut gateway = self.gateway("continuity_acceptance_multi_memory_successor");
        gateway.batch_execute("BEGIN").unwrap();
        set_context(
            &mut gateway,
            self.tenant,
            self.workspace,
            self.principal,
            Some(self.user),
        );
        let id = gateway
            .query_one(
                "SELECT facet_version_id FROM private.publish_continuity_facet( \
                 $1,$2,$3,$4,$5,'GOAL',$6,'CURRENT',$7,$8,$9,$10,$11)",
                &[
                    &self.tenant,
                    &self.workspace,
                    &self.project,
                    &self.principal,
                    &Some(self.user),
                    &expected_version,
                    &json!({"goal":"multi-memory successor"}),
                    &memories,
                    &hashes,
                    &Vec::<Uuid>::new(),
                    &Vec::<Vec<u8>>::new(),
                ],
            )
            .unwrap()
            .get(0);
        gateway.batch_execute("COMMIT").unwrap();
        id
    }

    pub fn publish_evidence_successor(&self, associated: bool) -> Uuid {
        let evidence = Uuid::now_v7();
        self.cleanup.register_evidence(evidence);
        let hash = vec![0x71_u8; 32];
        let mut admin = self.admin();
        let domain: Uuid = admin
            .query_one(
                "SELECT reasoning_domain_id FROM control.private_reasoning_domains \
                 WHERE tenant_id=$1 ORDER BY reasoning_domain_id LIMIT 1",
                &[&self.tenant],
            )
            .unwrap()
            .get(0);
        admin
            .execute(
                "INSERT INTO private.evidence_objects( \
                   evidence_id,tenant_id,evidence_kind,payload_sha256,data_class,origin_class, \
                   visibility_class,reasoning_domain_id) \
                 VALUES($1,$2,'EVENT',$3,'PRIVATE','AuthenticatedAgent','TENANT_SHARED',$4)",
                &[&evidence, &self.tenant, &hash, &domain],
            )
            .unwrap();
        if associated {
            self.associate(evidence, self.tenant, "DONE");
        }
        let mut gateway = self.gateway("continuity_acceptance_evidence_successor");
        gateway.batch_execute("BEGIN").unwrap();
        set_context(
            &mut gateway,
            self.tenant,
            self.workspace,
            self.principal,
            Some(self.user),
        );
        gateway
            .query_one(
                "SELECT facet_version_id FROM private.publish_continuity_facet( \
                 $1,$2,$3,$4,$5,'GOAL',1,'CURRENT',$6,$7,$8,$9,$10)",
                &[
                    &self.tenant,
                    &self.workspace,
                    &self.project,
                    &self.principal,
                    &Some(self.user),
                    &json!({"goal":"evidence successor"}),
                    &Vec::<Uuid>::new(),
                    &Vec::<Vec<u8>>::new(),
                    &vec![evidence],
                    &vec![hash],
                ],
            )
            .unwrap();
        gateway.batch_execute("COMMIT").unwrap();
        evidence
    }

    pub fn associate(&self, evidence: Uuid, tenant: Uuid, state: &str) {
        let mut admin = self.admin();
        let commit_seq: i64 = admin
            .query_one("SELECT nextval('ops.commit_seq_seq')", &[])
            .unwrap()
            .get(0);
        let stream_seq = commit_seq;
        let settled = matches!(
            state,
            "DONE" | "SKIPPED_BY_POLICY" | "FAILED" | "TOMBSTONED"
        );
        admin
            .execute(
                if settled {
                    "INSERT INTO projection.stream_log(tenant_id,scope_kind,scope_id,domain, \
                     projection_kind,projection_version,stream_seq,commit_seq,state,settled_at) \
                     VALUES($1,'tenant',$1,'knowledge','continuity-w2','v1',$2,$2,$3,now())"
                } else {
                    "INSERT INTO projection.stream_log(tenant_id,scope_kind,scope_id,domain, \
                     projection_kind,projection_version,stream_seq,commit_seq,state) \
                     VALUES($1,'tenant',$1,'knowledge','continuity-w2','v1',$2,$2,$3)"
                },
                &[&tenant, &stream_seq, &state],
            )
            .unwrap();
        admin
            .execute(
                "INSERT INTO ops.outbox(tenant_id,commit_seq,stream_seq,event_type,evidence_id) \
                 VALUES($1,$2,$2,'EVIDENCE_ACCEPTED',$3)",
                &[&tenant, &commit_seq, &evidence],
            )
            .unwrap();
    }

    pub fn create_decoy_tenant(&self) -> Uuid {
        let tenant = Uuid::now_v7();
        self.cleanup.register_tenant(tenant);
        self.admin()
            .execute(
                "INSERT INTO control.tenants(tenant_id,name,state) VALUES($1,'w2 decoy','ACTIVE')",
                &[&tenant],
            )
            .unwrap();
        tenant
    }

    pub fn supersede_memory(&self) {
        let successor = Uuid::now_v7();
        self.cleanup.register_memory(successor);
        let mut admin = self.admin();
        admin.batch_execute("BEGIN").unwrap();
        admin
            .execute(
                "INSERT INTO private.memory_records(memory_id,tenant_id,memory_type,content, \
                 visibility_class,authority_class,confidence,status,asserted_at) \
                 VALUES($1,$2,'FACT',$3,'TENANT_SHARED','ProjectDecision',1,'active',now())",
                &[&successor, &self.tenant, &json!({"successor":true})],
            )
            .unwrap();
        admin
            .execute(
                "INSERT INTO private.memory_evidence(memory_id,evidence_id,role,grounding_mode) \
                 VALUES($1,$2,'SUPPORTING','IMMUTABLE')",
                &[&successor, &self.backing_evidence],
            )
            .unwrap();
        admin.batch_execute("COMMIT").unwrap();
        admin
            .execute(
                "UPDATE private.memory_records SET status='superseded',superseded_by=$3 \
                 WHERE tenant_id=$1 AND memory_id=$2",
                &[&self.tenant, &self.memory, &successor],
            )
            .unwrap();
    }
}
