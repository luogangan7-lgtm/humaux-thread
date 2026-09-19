//! Real native MCP -> Gateway -> application -> one PostgreSQL RR continuity.get witness.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::{Ipv6Addr, SocketAddr},
    panic::{self, AssertUnwindSafe},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use humaux_adapters::{postgres::RuntimeDbPool, quota_repo::RatePolicy};
use humaux_domain::{
    context::ContextBudget, dataclass::DataClass, identity::VisibilityClass, ids::TenantId,
};
use humaux_gateway::{
    context::ContextBootstrap,
    guard::{GatewayGuard, GuardRatePolicies, GuardSettings},
    mcp_application::GatewayMcpApplication,
    remember::{RememberEventKind, RememberPolicy},
};
use humaux_projection::stream::StreamKey;
use humaux_protocol::{
    edge::{Cidr, TrustedProxyConfig, compute_api_key_hash},
    mcp::{McpAdapter, McpHttpConfig, ToolName},
    mcp_catalog::CanonicalCatalog,
};
use humaux_testkit::run_db_fixture;
use postgres::{Client, GenericClient, Transaction};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc2822};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use uuid::Uuid;

#[allow(dead_code)]
#[path = "../../../crates/adapters/tests/support/operation_receipt_fixture.rs"]
mod operation_receipt_fixture;

use operation_receipt_fixture::{
    Fixture, Handle, SYNTHETIC_CREDENTIAL_PEPPER, SyntheticCredentialScopes,
};

const HOST: &str = "mcp.test";
const ORIGIN: &str = "https://mcp.test";
const NIL: Uuid = Uuid::nil();
const FORCE_NATIVE_PANIC: &str = "HUMAUX_CONTINUITY_W2_FORCE_NATIVE_PANIC";
const FORCE_NATIVE_CLEANUP_FAILURE: &str = "HUMAUX_CONTINUITY_W2_FORCE_NATIVE_CLEANUP_FAILURE";

fn rate() -> RatePolicy {
    RatePolicy::new(100, 100).expect("explicit fixture rate")
}

fn guard(runtime: RuntimeDbPool) -> Arc<GatewayGuard> {
    Arc::new(
        GatewayGuard::new(
            runtime,
            GuardSettings {
                credential_pepper: SYNTHETIC_CREDENTIAL_PEPPER.to_vec(),
                trusted_proxies: TrustedProxyConfig {
                    trusted_proxy_cidrs: vec![Cidr::from_str("127.0.0.1/32").unwrap()],
                    max_forwarded_hops: 1,
                },
                global_denylist: vec![],
                global_emergency_allowlist: vec![],
                tenant_network: BTreeMap::new(),
                rates: GuardRatePolicies {
                    preauth_ip: rate(),
                    credential: rate(),
                    user: rate(),
                    tenant: rate(),
                    operation: rate(),
                },
                reservation_ttl: Duration::from_secs(30),
                handler_timeout: Duration::from_secs(5),
                finalize_timeout: Duration::from_secs(2),
                replay_ttl: Duration::from_secs(60),
            },
        )
        .expect("explicit guard settings"),
    )
}

fn application(handle: &Handle, runtime: RuntimeDbPool) -> GatewayMcpApplication {
    let policy = RememberPolicy::new(
        StreamKey::new(
            TenantId(handle.tenant_id),
            "workspace",
            handle.workspace_id,
            "knowledge",
            "ingest",
            "v1",
        ),
        handle.reasoning_domain_id,
        Duration::from_secs(60),
        DataClass::Internal,
        VisibilityClass::WorkspaceShared,
    )
    .unwrap();
    let bootstrap = ContextBootstrap::new(
        ContextBudget::new(2_048, 1_024).unwrap(),
        humaux_contracts::retrieval_config::resolve_registered_retrieval_profile(
            &Default::default(),
        )
        .unwrap(),
        &policy,
    )
    .unwrap();
    GatewayMcpApplication::new(
        CanonicalCatalog::load().unwrap(),
        guard(runtime),
        policy,
        RememberEventKind::UserMessage,
        bootstrap,
    )
}

fn set_context(
    client: &mut impl GenericClient,
    tenant: Uuid,
    workspace: Uuid,
    principal: Uuid,
    user: Option<Uuid>,
) {
    client
        .query_one(
            "SELECT set_config('humaux.tenant_id',$1,true),\
                    set_config('humaux.workspace_id',$2,true),\
                    set_config('humaux.principal_id',$3,true),\
                    set_config('humaux.user_id',$4,true)",
            &[
                &tenant.to_string(),
                &workspace.to_string(),
                &principal.to_string(),
                &user.unwrap_or(NIL).to_string(),
            ],
        )
        .unwrap();
}

fn register(client: &mut Client, handle: &Handle, workspace: Uuid, project: Uuid, title: &str) {
    register_for(
        client,
        handle.tenant_id,
        workspace,
        project,
        handle.principal_id,
        handle.user_id,
        title,
    );
}

fn register_for(
    client: &mut Client,
    tenant: Uuid,
    workspace: Uuid,
    project: Uuid,
    principal: Uuid,
    user: Uuid,
    title: &str,
) {
    client.batch_execute("BEGIN").unwrap();
    set_context(client, tenant, workspace, principal, Some(user));
    client
        .query_one(
            "SELECT private.register_continuity_project($1,$2,$3,$4,$5,$6)",
            &[
                &tenant,
                &workspace,
                &project,
                &principal,
                &Some(user),
                &title,
            ],
        )
        .unwrap();
    client.batch_execute("COMMIT").unwrap();
}

fn publish_goal(client: &mut Client, handle: &mut Handle, project: Uuid, memory: Uuid) {
    let memory_hash: Vec<u8> = handle
        .admin
        .query_one(
            "SELECT sha256(convert_to(content::text,'UTF8'))\
             FROM private.memory_records WHERE tenant_id=$1 AND memory_id=$2",
            &[&handle.tenant_id, &memory],
        )
        .unwrap()
        .get(0);
    client.batch_execute("BEGIN").unwrap();
    set_context(
        client,
        handle.tenant_id,
        handle.workspace_id,
        handle.principal_id,
        Some(handle.user_id),
    );
    client
        .query_one(
            "SELECT facet_version_id FROM private.publish_continuity_facet(\
               $1,$2,$3,$4,$5,'GOAL',0,'CURRENT',$6,$7,$8,$9,$10)",
            &[
                &handle.tenant_id,
                &handle.workspace_id,
                &project,
                &handle.principal_id,
                &Some(handle.user_id),
                &json!({"goal":"native continuity"}),
                &vec![memory],
                &vec![memory_hash],
                &Vec::<Uuid>::new(),
                &Vec::<Vec<u8>>::new(),
            ],
        )
        .unwrap();
    client.batch_execute("COMMIT").unwrap();
}

async fn start(application: GatewayMcpApplication) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let catalog = CanonicalCatalog::load().unwrap().trusted_catalog().unwrap();
    let adapter = McpAdapter::new(
        Arc::new(application),
        catalog,
        McpHttpConfig::new(vec![HOST.into()], vec![ORIGIN.into()], 64 * 1024).unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            adapter
                .router()
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (address, server)
}

async fn stop_server(server: tokio::task::JoinHandle<()>) -> Result<(), String> {
    server.abort();
    match server.await {
        Err(error) if error.is_cancelled() => {
            eprintln!(
                "continuity_native_server_join pid={} result=cancelled",
                std::process::id()
            );
            Ok(())
        }
        Ok(()) => Err("continuity native server exited cleanly before cancellation".into()),
        Err(error) => Err(format!("continuity native server failed: {error}")),
    }
}

fn exact_fault_mode(name: &str) -> bool {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => false,
        Ok(value) if value == "1" => true,
        Ok(_) | Err(std::env::VarError::NotUnicode(_)) => {
            panic!("{name} must be unset or exactly 1")
        }
    }
}

fn fixture_forwarded(tenant_id: Uuid) -> Ipv6Addr {
    Ipv6Addr::from((0x2001_0db8_u128 << 96) | (tenant_id.as_u128() & ((1_u128 << 96) - 1)))
}

#[derive(Debug)]
struct HttpResponse {
    status: String,
    headers: Vec<String>,
    body: String,
    json: Value,
}

#[derive(Debug, Eq, PartialEq)]
struct ComparableResponse {
    status: String,
    headers: Vec<String>,
    body: String,
}

fn comparable_response(response: &HttpResponse) -> (ComparableResponse, String) {
    let mut raw_date = None;
    let headers = response
        .headers
        .iter()
        .map(|header| {
            let (name, value) = header
                .split_once(':')
                .unwrap_or_else(|| panic!("malformed response header sha={}", digest(header)));
            if name.eq_ignore_ascii_case("date") {
                assert!(raw_date.is_none(), "response must contain exactly one Date");
                let value = value.trim_matches([' ', '\t']);
                assert!(
                    is_imf_fixdate(value),
                    "response Date is not a valid IMF-fixdate"
                );
                raw_date = Some(value.to_owned());
                let ows_prefix_len = header[name.len() + 1..].len()
                    - header[name.len() + 1..]
                        .trim_start_matches([' ', '\t'])
                        .len();
                let value_start = name.len() + 1 + ows_prefix_len;
                let value_end = value_start + value.len();
                format!(
                    "{}<validated-http-date>{}",
                    &header[..value_start],
                    &header[value_end..]
                )
            } else {
                header.clone()
            }
        })
        .collect();
    let raw_date = raw_date.expect("response must contain exactly one Date");
    (
        ComparableResponse {
            status: response.status.clone(),
            headers,
            body: response.body.clone(),
        },
        raw_date,
    )
}

fn is_imf_fixdate(value: &str) -> bool {
    let bytes = value.as_bytes();
    value.len() == 29
        && bytes[0..3].iter().all(u8::is_ascii_alphabetic)
        && &bytes[3..5] == b", "
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b' '
        && bytes[8..11].iter().all(u8::is_ascii_alphabetic)
        && bytes[11] == b' '
        && bytes[12..16].iter().all(u8::is_ascii_digit)
        && bytes[16] == b' '
        && bytes[17..19].iter().all(u8::is_ascii_digit)
        && bytes[19] == b':'
        && bytes[20..22].iter().all(u8::is_ascii_digit)
        && bytes[22] == b':'
        && bytes[23..25].iter().all(u8::is_ascii_digit)
        && &bytes[25..29] == b" GMT"
        && OffsetDateTime::parse(value, &Rfc2822).is_ok()
}

fn digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn assert_equivalent(left: &ComparableResponse, right: &ComparableResponse, pair: usize) {
    assert_eq!(
        left.status, right.status,
        "failure status differs at pair {pair}"
    );
    assert!(
        left.headers == right.headers,
        "failure headers differ at pair {pair}: left_sha={} right_sha={}",
        digest(&left.headers.join("\n")),
        digest(&right.headers.join("\n"))
    );
    assert!(
        left.body == right.body,
        "failure body differs at pair {pair}: left_sha={} right_sha={}",
        digest(&left.body),
        digest(&right.body)
    );
}

#[derive(Debug)]
struct WrongTenantIds {
    tenant: Uuid,
    workspace: Uuid,
    user: Uuid,
}

#[derive(Debug, Default)]
struct NativeOwnedRows {
    projects: Vec<Uuid>,
    wrong_tenant: Option<WrongTenantIds>,
}

impl NativeOwnedRows {
    fn seed_wrong_tenant(
        &mut self,
        client: &mut Client,
        tenant: Uuid,
        workspace: Uuid,
        user: Uuid,
    ) {
        assert!(self.wrong_tenant.is_none(), "wrong tenant seeded once");
        let mut seed = client.transaction().expect("begin wrong-tenant seed");
        assert_eq!(
            seed.execute(
                "INSERT INTO control.tenants(tenant_id,name,state) VALUES($1,'wrong','ACTIVE')",
                &[&tenant],
            )
            .expect("seed wrong tenant"),
            1
        );
        assert_eq!(
            seed.execute(
                "INSERT INTO control.memberships(tenant_id,user_id,role,state) \
                 VALUES($1,$2,'OWNER','ACTIVE')",
                &[&tenant, &user],
            )
            .expect("seed wrong-tenant membership"),
            1
        );
        assert_eq!(
            seed.execute(
                "INSERT INTO control.workspaces(workspace_id,tenant_id,name) \
                 VALUES($1,$2,'wrong')",
                &[&workspace, &tenant],
            )
            .expect("seed wrong-tenant workspace"),
            1
        );
        seed.commit().expect("commit wrong-tenant seed");
        self.wrong_tenant = Some(WrongTenantIds {
            tenant,
            workspace,
            user,
        });
    }

    fn track_project(&mut self, project: Uuid) {
        assert!(!self.projects.contains(&project), "project tracked once");
        self.projects.push(project);
    }

    fn finish_projects(cleanup: &mut Transaction<'_>, projects: &[Uuid]) -> Result<(), String> {
        cleanup
            .query_one(
                "SELECT pg_advisory_xact_lock(\
                   hashtextextended('operation_receipt_fixture_continuity_cleanup',0))",
                &[],
            )
            .map_err(|error| format!("lock native continuity cleanup: {error:?}"))?;
        cleanup
            .batch_execute(
                "ALTER TABLE private.continuity_facet_memory_links \
                   DISABLE TRIGGER continuity_facet_memory_links_append_only; \
                 ALTER TABLE private.continuity_facet_evidence_links \
                   DISABLE TRIGGER continuity_facet_evidence_links_append_only; \
                 ALTER TABLE private.continuity_facet_versions \
                   DISABLE TRIGGER continuity_facet_versions_append_only; \
                 ALTER TABLE private.continuity_projects \
                   DISABLE TRIGGER continuity_project_mutation_guard",
            )
            .map_err(|error| format!("disable native continuity triggers: {error:?}"))?;
        for statement in [
            "DELETE FROM private.continuity_facet_memory_links WHERE project_id=ANY($1)",
            "DELETE FROM private.continuity_facet_evidence_links WHERE project_id=ANY($1)",
            "DELETE FROM private.continuity_facet_slots WHERE project_id=ANY($1)",
            "DELETE FROM private.continuity_facet_versions WHERE project_id=ANY($1)",
        ] {
            cleanup
                .execute(statement, &[&projects])
                .map_err(|error| format!("delete native project child: {error:?}"))?;
        }
        let deleted = cleanup
            .execute(
                "DELETE FROM private.continuity_projects WHERE project_id=ANY($1)",
                &[&projects],
            )
            .map_err(|error| format!("delete native projects: {error:?}"))?;
        if deleted != projects.len() as u64 {
            return Err(format!(
                "native project cleanup rowcount: expected={} actual={deleted}",
                projects.len()
            ));
        }
        cleanup
            .batch_execute("SET CONSTRAINTS ALL IMMEDIATE")
            .map_err(|error| format!("validate native cleanup constraints: {error:?}"))?;
        cleanup
            .batch_execute(
                "ALTER TABLE private.continuity_facet_memory_links \
                   ENABLE TRIGGER continuity_facet_memory_links_append_only; \
                 ALTER TABLE private.continuity_facet_evidence_links \
                   ENABLE TRIGGER continuity_facet_evidence_links_append_only; \
                 ALTER TABLE private.continuity_facet_versions \
                   ENABLE TRIGGER continuity_facet_versions_append_only; \
                 ALTER TABLE private.continuity_projects \
                   ENABLE TRIGGER continuity_project_mutation_guard",
            )
            .map_err(|error| format!("restore native continuity triggers: {error:?}"))?;
        Ok(())
    }

    fn finish(&mut self, client: &mut Client) -> Result<(), String> {
        let mut cleanup = client
            .transaction()
            .map_err(|error| format!("begin native cleanup: {error:?}"))?;
        if !self.projects.is_empty() {
            Self::finish_projects(&mut cleanup, &self.projects)?;
        }
        if let Some(ids) = &self.wrong_tenant {
            for (label, deleted) in [
                (
                    "membership",
                    cleanup
                        .execute(
                            "DELETE FROM control.memberships WHERE tenant_id=$1 AND user_id=$2",
                            &[&ids.tenant, &ids.user],
                        )
                        .map_err(|error| format!("delete wrong-tenant membership: {error:?}"))?,
                ),
                (
                    "workspace",
                    cleanup
                        .execute(
                            "DELETE FROM control.workspaces WHERE tenant_id=$1 AND workspace_id=$2",
                            &[&ids.tenant, &ids.workspace],
                        )
                        .map_err(|error| format!("delete wrong-tenant workspace: {error:?}"))?,
                ),
                (
                    "tenant",
                    cleanup
                        .execute(
                            "DELETE FROM control.tenants WHERE tenant_id=$1",
                            &[&ids.tenant],
                        )
                        .map_err(|error| format!("delete wrong tenant: {error:?}"))?,
                ),
            ] {
                if deleted != 1 {
                    return Err(format!(
                        "wrong-tenant {label} cleanup rowcount: expected=1 actual={deleted}"
                    ));
                }
            }
        }
        cleanup
            .commit()
            .map_err(|error| format!("commit native cleanup: {error:?}"))?;
        self.projects.clear();
        self.wrong_tenant = None;
        Ok(())
    }
}

impl Drop for NativeOwnedRows {
    fn drop(&mut self) {
        if !self.projects.is_empty() || self.wrong_tenant.is_some() {
            eprintln!(
                "native owned rows left armed: projects={:?} wrong_tenant={:?}",
                self.projects, self.wrong_tenant
            );
        }
    }
}

async fn request(
    address: SocketAddr,
    bearer: &str,
    arguments: Value,
    forwarded: Ipv6Addr,
) -> HttpResponse {
    let body = json!({
        "jsonrpc":"2.0",
        "id":1,
        "method":"tools/call",
        "params":{
            "name":"continuity",
            "arguments":arguments,
            "_meta":{
                "io.modelcontextprotocol/protocolVersion":"2026-07-28",
                "io.modelcontextprotocol/clientInfo":{"name":"continuity-w2","version":"1"},
                "io.modelcontextprotocol/clientCapabilities":{}
            }
        }
    })
    .to_string();
    let headers = [
        ("MCP-Protocol-Version", "2026-07-28"),
        ("Mcp-Method", "tools/call"),
        ("Mcp-Name", "continuity"),
        ("Authorization", bearer),
    ];
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut wire = format!(
        "POST /mcp HTTP/1.1\r\nHost: {HOST}\r\nOrigin: {ORIGIN}\r\nConnection: close\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        wire.push_str(name);
        wire.push_str(": ");
        wire.push_str(value);
        wire.push_str("\r\n");
    }
    wire.push_str("Forwarded: ");
    wire.push_str(&forwarded.to_string());
    wire.push_str("\r\n");
    wire.push_str("\r\n");
    wire.push_str(&body);
    stream.write_all(wire.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let mut head_lines = head.lines();
    let status = head_lines.next().unwrap().to_owned();
    let headers: Vec<String> = head_lines.map(str::to_owned).collect();
    let body = if body.starts_with("event:") {
        body.lines()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap()
    } else {
        body
    };
    HttpResponse {
        status,
        headers,
        body: body.to_owned(),
        json: serde_json::from_str(body).unwrap(),
    }
}

fn assert_not_found(response: &HttpResponse) {
    assert_eq!(response.json["result"]["isError"], true, "{:?}", response);
    assert_eq!(
        response.json["result"]["structuredContent"]["code"], "NOT_FOUND",
        "{:?}",
        response
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn native_gateway_preserves_raw_workspace_non_disclosure_and_validates_output() {
    run_db_fixture::<Fixture, _>(
        "native_gateway_preserves_raw_workspace_non_disclosure_and_validates_output",
        |mut handle| {
            let force_native_panic = exact_fault_mode(FORCE_NATIVE_PANIC);
            let force_cleanup_failure = exact_fault_mode(FORCE_NATIVE_CLEANUP_FAILURE);
            let forwarded = fixture_forwarded(handle.tenant_id);
            let project = Uuid::now_v7();
            let archived = Uuid::now_v7();
            let unauthorized = Uuid::now_v7();
            let wrong_tenant_project = Uuid::now_v7();
            let absent = Uuid::now_v7();
            let wrong_tenant = Uuid::now_v7();
            let wrong_tenant_workspace = Uuid::now_v7();
            let mut owned = NativeOwnedRows::default();
            let run_result = panic::catch_unwind(AssertUnwindSafe(|| {
                let prefix = format!("ct{}", &Uuid::now_v7().simple().to_string()[..12]);
                let wire = format!("{prefix}.{}", "d".repeat(32));
                let credential = handle.seed_synthetic_service_credential_and_window(
                    SyntheticCredentialScopes::ContextRead,
                    &prefix,
                    &wire,
                    &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
                    20,
                );
                let source = handle.seed_workspace_visible_context_record();
                let other_workspace = handle.seed_workspace();
                handle.seed_legacy_system_preauth_bucket(forwarded.to_string());
                owned.seed_wrong_tenant(
                    &mut handle.admin,
                    wrong_tenant,
                    wrong_tenant_workspace,
                    handle.user_id,
                );
                let mut gateway = handle.gateway_client().unwrap();
                register(
                    &mut gateway,
                    &handle,
                    handle.workspace_id,
                    project,
                    "native",
                );
                owned.track_project(project);
                publish_goal(&mut gateway, &mut handle, project, source.memory_id);
                register(
                    &mut gateway,
                    &handle,
                    handle.workspace_id,
                    archived,
                    "archived",
                );
                owned.track_project(archived);
                register(
                    &mut gateway,
                    &handle,
                    other_workspace,
                    unauthorized,
                    "unauthorized",
                );
                owned.track_project(unauthorized);
                register_for(
                    &mut gateway,
                    wrong_tenant,
                    wrong_tenant_workspace,
                    wrong_tenant_project,
                    handle.principal_id,
                    handle.user_id,
                    "wrong tenant",
                );
                owned.track_project(wrong_tenant_project);
                handle
                    .admin
                    .execute(
                        "UPDATE private.continuity_projects SET lifecycle_state='ARCHIVED'\
                         WHERE project_id=$1",
                        &[&archived],
                    )
                    .unwrap();

                let unbound_prefix = format!("cu{}", &Uuid::now_v7().simple().to_string()[..12]);
                let unbound_wire = format!("{unbound_prefix}.{}", "e".repeat(32));
                let unbound = handle.seed_synthetic_service_credential(
                    SyntheticCredentialScopes::ContextRead,
                    &unbound_prefix,
                    &unbound_wire,
                    &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &unbound_wire),
                );
                handle
                    .admin
                    .execute(
                        "UPDATE control.api_keys SET workspace_id=NULL WHERE api_key_id=$1",
                        &[&unbound.api_key_id],
                    )
                    .unwrap();

                let polluted_dsn = format!(
                    "{}?options=-c%20humaux.workspace_id%3D{}%20-c%20humaux.user_id%3D{}",
                    handle.gateway_dsn_for_process(),
                    other_workspace,
                    Uuid::now_v7()
                );
                let runtime_handle = handle.rt.handle().clone();
                let runtime = runtime_handle
                    .block_on(RuntimeDbPool::connect(&polluted_dsn))
                    .expect("checked polluted Gateway pool");
                let app = application(&handle, runtime);
                let credential_bearer = credential.bearer.clone();
                let unbound_bearer = unbound.bearer.clone();
                runtime_handle.block_on(async move {
                    let (address, server) = start(app).await;
                    let requests = tokio::spawn(async move {
                        if force_native_panic {
                            panic!("continuity forced native panic witness");
                        }
                        // ADR-0035 (card 13): an unbound PAT is no longer an empty workspace
                        // set. `allowed_workspace_ids` is derived per request from live ACTIVE
                        // control.workspace_memberships, so this credential reads exactly the
                        // workspaces its user is a member of — here the fixture's own workspace,
                        // which holds `project`. (Baseline §73.5.1 corrected in the same change;
                        // a machine key with no bound workspace still has an empty set.)
                        // (That an unbound PAT now DOES read its member workspaces is asserted by
                        // card 13's own native_mcp_workspace_membership_scope; adding a second
                        // successful read here would change this test's single-success receipt
                        // assertion below, so this test stays focused on non-disclosure.)
                        // Non-disclosure is unchanged where it matters: an unbound PAT must
                        // still not learn anything about a workspace it holds no membership in.
                        // `unauthorized` lives in `other_workspace`, which was seeded WITHOUT a
                        // membership (the freeze rule), so this stays NOT_FOUND and remains the
                        // seventh member of the byte-identical failure set compared below.
                        let unbound_failure = request(
                            address,
                            &unbound_bearer,
                            json!({"project_id":unauthorized}),
                            forwarded,
                        )
                        .await;
                        assert_not_found(&unbound_failure);
                        let success = request(
                            address,
                            &credential_bearer,
                            json!({"project_id":project}),
                            forwarded,
                        )
                        .await;
                        let value = &success.json["result"]["structuredContent"];
                        assert_ne!(success.json["result"]["isError"], true, "{:?}", success);
                        CanonicalCatalog::load()
                            .unwrap()
                            .validate_output(ToolName::Continuity, value)
                            .unwrap();
                        assert_eq!(value["facets"].as_array().unwrap().len(), 17);
                        assert_eq!(value["facets"][0]["status"], "CURRENT");
                        assert_eq!(value["facets"][13]["kind"], "HANDOFF");
                        assert_eq!(value["facets"][16]["kind"], "COVERAGE");

                        // Gate witness: this delay must cross Hyper's cached Date second.
                        tokio::time::sleep(Duration::from_millis(1_100)).await;
                        let mut failures = Vec::new();
                        for arguments in [
                            json!({"project_id":absent}),
                            json!({"project_id":archived}),
                            json!({"project_id":wrong_tenant_project}),
                            json!({"project_id":unauthorized}),
                            json!({"project_id":project,"workspace_id":other_workspace}),
                            json!({"project_id":project,"workspace_id":NIL}),
                        ] {
                            let failure =
                                request(address, &credential_bearer, arguments, forwarded).await;
                            assert_not_found(&failure);
                            failures.push(failure);
                        }
                        failures.push(unbound_failure);
                        let comparable: Vec<_> = failures.iter().map(comparable_response).collect();
                        let raw_dates: BTreeSet<_> = comparable
                            .iter()
                            .map(|(_, raw_date)| raw_date.as_str())
                            .collect();
                        assert!(
                            raw_dates.len() >= 2,
                            "cross-second witness requires at least two raw Date values"
                        );
                        eprintln!(
                            "continuity_native_date_witness distinct={} sha={:?}",
                            raw_dates.len(),
                            raw_dates
                                .iter()
                                .map(|value| digest(value))
                                .collect::<Vec<_>>()
                        );
                        for (pair_index, pair) in comparable.windows(2).enumerate() {
                            assert_equivalent(&pair[0].0, &pair[1].0, pair_index);
                        }
                    });
                    let request_result = requests.await;
                    let stop_result = stop_server(server).await;
                    match (request_result, stop_result) {
                        (Ok(()), Ok(())) => {}
                        (Ok(()), Err(stop_error)) => panic!("{stop_error}"),
                        (Err(request_error), Ok(())) if request_error.is_panic() => {
                            panic::resume_unwind(request_error.into_panic());
                        }
                        (Err(request_error), Ok(())) => {
                            panic!("continuity request task failed: {request_error}");
                        }
                        (Err(request_error), Err(stop_error)) => {
                            eprintln!("continuity server cleanup secondary failure: {stop_error}");
                            if request_error.is_panic() {
                                panic::resume_unwind(request_error.into_panic());
                            }
                            panic!("continuity request task failed: {request_error}; {stop_error}");
                        }
                    }
                });
                let receipt: (i64, i64) = handle
                    .admin
                    .query_one(
                        "SELECT \
                           (SELECT count(*) FROM control.usage_reservations \
                            WHERE tenant_id=$1 AND operation='continuity.get' AND status='CONSUMED'), \
                           (SELECT count(*) FROM control.audit_events \
                            WHERE tenant_id=$1 AND resource_id='continuity.get' \
                              AND action='MCP_REQUEST_FINISHED' AND result='OK')",
                        &[&handle.tenant_id],
                    )
                    .map(|row| (row.get(0), row.get(1)))
                    .unwrap();
                assert_eq!(receipt, (1, 1), "successful continuity receipt must close");
            }));
            let mut cleanup_result = owned.finish(&mut handle.admin);
            if force_cleanup_failure && cleanup_result.is_ok() {
                cleanup_result = Err("continuity forced native cleanup failure witness".into());
            }
            match (run_result, cleanup_result) {
                (Ok(()), Ok(())) => {}
                (Ok(()), Err(cleanup_error)) => panic!("{cleanup_error}"),
                (Err(original_panic), Ok(())) => panic::resume_unwind(original_panic),
                (Err(original_panic), Err(cleanup_error)) => {
                    eprintln!("continuity native cleanup secondary failure: {cleanup_error}");
                    panic::resume_unwind(original_panic);
                }
            }
        },
    );
}

#[test]
fn continuity_guard_and_dispatch_are_not_generic_narrowing() {
    let guard = include_str!("../src/guard.rs");
    let dispatch = include_str!("../src/mcp_application.rs");
    assert!(guard.contains("WorkspaceAdmission::PreserveContinuityFilter"));
    assert!(guard.contains("credential.authorize(required_scope, authorization_filter)"));
    assert!(guard.contains("operation.operation_key() != \"continuity.get\""));
    assert!(dispatch.contains("ToolName::Continuity"));
    assert!(dispatch.contains("catalog.validate_output(ToolName::Continuity, &value)"));
    assert!(dispatch.contains("SUPPORTED_OPERATION_KEYS: [&str; 18]"));
    assert!(!dispatch.contains("Scope::Project"));

    let baseline = HttpResponse {
        status: "HTTP/1.1 200 OK".into(),
        headers: vec![
            "content-type: application/json".into(),
            "date: Sun, 06 Nov 1994 08:49:37 GMT".into(),
            "x-stable: one".into(),
        ],
        body: "{}".into(),
        json: json!({}),
    };
    let date_only_change = HttpResponse {
        status: baseline.status.clone(),
        headers: vec![
            "content-type: application/json".into(),
            "date: Sun, 06 Nov 1994 08:49:38 GMT".into(),
            "x-stable: one".into(),
        ],
        body: baseline.body.clone(),
        json: json!({}),
    };
    let stable_header_change = HttpResponse {
        status: baseline.status.clone(),
        headers: vec![
            "content-type: application/json".into(),
            "date: Sun, 06 Nov 1994 08:49:38 GMT".into(),
            "x-stable: two".into(),
        ],
        body: baseline.body.clone(),
        json: json!({}),
    };
    let baseline = comparable_response(&baseline).0;
    assert_eq!(baseline, comparable_response(&date_only_change).0);
    assert_ne!(baseline, comparable_response(&stable_header_change).0);
}
