use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use humaux_domain::error::ErrorCode;
use humaux_protocol::mcp::{
    McpAdapter, McpApplication, McpHttpConfig, McpHttpContext, McpOperation, McpToolArguments,
    McpToolSchema, ToolName, ToolOutput, TrustedMcpCatalog,
};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Default)]
struct MockApplication {
    denied: Mutex<Option<ErrorCode>>,
    invoke_denied: Mutex<Option<ErrorCode>>,
    operations: Mutex<Vec<McpOperation>>,
    invocations: Mutex<Vec<(ToolName, String, String)>>,
    request_ids: Mutex<Vec<uuid::Uuid>>,
}

#[async_trait]
impl McpApplication for MockApplication {
    async fn check_access(
        &self,
        context: &McpHttpContext,
        operation: McpOperation,
    ) -> Result<(), ErrorCode> {
        self.operations.lock().expect("operations").push(operation);
        self.request_ids
            .lock()
            .expect("request ids")
            .push(context.request_id());
        self.denied
            .lock()
            .expect("denied")
            .as_ref()
            .copied()
            .map_or(Ok(()), Err)
    }

    async fn invoke(
        &self,
        _context: &McpHttpContext,
        tool: ToolName,
        arguments: McpToolArguments<'_>,
    ) -> Result<ToolOutput, ErrorCode> {
        if let Some(code) = *self.invoke_denied.lock().expect("invoke denied") {
            return Err(code);
        }
        self.invocations.lock().expect("invocations").push((
            tool,
            arguments.raw_json().unwrap_or("").to_owned(),
            serde_json::to_string(arguments.decoded()).expect("decoded arguments"),
        ));
        Ok(ToolOutput {
            text: "\u{feff}line\r\n".to_owned(),
            structured_content: json!({"kind":"mock","ok":true}),
        })
    }
}

fn catalog() -> TrustedMcpCatalog {
    TrustedMcpCatalog::new(catalog_items()).expect("closed trusted catalog")
}

fn catalog_items() -> Vec<McpToolSchema> {
    [
        ToolName::Remember,
        ToolName::Recall,
        ToolName::Memory,
        ToolName::Context,
        ToolName::Continuity,
        ToolName::Artifact,
        ToolName::Code,
        ToolName::Coordinate,
    ]
    .into_iter()
    .map(|name| McpToolSchema {
        name,
        title: Some(name.as_str().to_owned()),
        description: format!("{} tool", name.as_str()),
        input_schema: Map::new(),
        output_schema: Some(Map::new()),
    })
    .collect()
}

#[test]
fn trusted_catalog_rejects_duplicate_schema_names() {
    let mut items = catalog_items();
    items.push(items[0].clone());
    assert_eq!(
        TrustedMcpCatalog::new(items).expect_err("duplicate must not overwrite trusted schema"),
        ErrorCode::InvalidInput
    );
}

async fn start(app: Arc<MockApplication>) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let adapter = McpAdapter::new(
        app,
        catalog(),
        McpHttpConfig::new(
            vec!["mcp.test".to_owned()],
            vec!["https://mcp.test".to_owned()],
            64 * 1024,
        )
        .expect("safe config"),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("loopback address");
    let handle = tokio::spawn(async move {
        axum::serve(
            listener,
            adapter
                .router()
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .expect("loopback server");
    });
    (address, handle)
}

async fn request(
    address: SocketAddr,
    method: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String, BTreeMap<String, String>) {
    let mut stream = TcpStream::connect(address).await.expect("connect loopback");
    let mut request = format!(
        "{method} /mcp HTTP/1.1\r\nConnection: close\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        body.len(),
    );
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("host"))
    {
        request.push_str("Host: mcp.test\r\n");
    }
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("origin"))
    {
        request.push_str("Origin: https://mcp.test\r\n");
    }
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read response");
    let response = String::from_utf8(response).expect("HTTP response UTF-8");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("HTTP separator: {response:?}"));
    let mut lines = head.lines();
    let status = lines
        .next()
        .expect("status line")
        .split_whitespace()
        .nth(1)
        .expect("status")
        .parse()
        .expect("numeric status");
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    (status, body.to_owned(), headers)
}

fn modern_headers(method: &str) -> [(&str, &str); 3] {
    [
        ("MCP-Protocol-Version", "2026-07-28"),
        ("Mcp-Method", method),
        ("Authorization", "Bearer accepted"),
    ]
}

fn tool_call_headers() -> [(&'static str, &'static str); 4] {
    [
        ("MCP-Protocol-Version", "2026-07-28"),
        ("Mcp-Method", "tools/call"),
        ("Mcp-Name", "remember"),
        ("Authorization", "Bearer accepted"),
    ]
}

const MODERN_META: &str = r#""_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"test","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}"#;

fn modern_params(prefix: &str) -> String {
    if prefix.is_empty() {
        format!("{{{MODERN_META}}}")
    } else {
        format!("{{{prefix},{MODERN_META}}}")
    }
}

#[tokio::test]
async fn native_list_and_call_keep_dual_output_raw_arguments_and_stateless_transport() {
    let app = Arc::new(MockApplication::default());
    let (address, server) = start(app.clone()).await;

    let list = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}}"#,
        modern_params("")
    );
    let (status, body, headers) =
        request(address, "POST", &modern_headers("tools/list"), &list).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(headers.get("mcp-session-id"), None);
    let listed: Value = serde_json::from_str(&body)
        .unwrap_or_else(|error| panic!("list JSON-RPC response {error}: {body:?}"));
    assert_eq!(
        listed["result"]["tools"].as_array().expect("tools").len(),
        8
    );
    assert!(body.contains("\"remember\""));

    let call = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{}}}"#,
        modern_params(r#""name":"remember","arguments":{ "content" : "a\r\n" }"#)
    );
    let (status, body, _) = request(address, "POST", &tool_call_headers(), &call).await;
    assert_eq!(status, 200, "{body}");
    let called: Value = serde_json::from_str(&body).expect("call JSON-RPC response");
    assert_eq!(
        called["result"]["content"][0]["text"], "\u{feff}line\r\n",
        "{body}"
    );
    assert_eq!(called["result"]["structuredContent"]["kind"], "mock");
    let invocations = app.invocations.lock().expect("invocations");
    assert_eq!(invocations.len(), 1);
    assert_eq!(invocations[0].1, r#"{ "content" : "a\r\n" }"#);
    drop(invocations);
    let request_ids = app.request_ids.lock().expect("request ids");
    let distinct = request_ids
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(distinct.len(), 2, "one server-minted id per HTTP request");
    server.abort();
}

#[tokio::test]
async fn malformed_or_legacy_transport_is_rejected_before_invoke() {
    let app = Arc::new(MockApplication::default());
    let (address, server) = start(app.clone()).await;
    let valid_call = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{}}}"#,
        modern_params(r#""name":"remember","arguments":{}"#)
    );

    let old_headers = [
        ("MCP-Protocol-Version", "2025-11-25"),
        ("Mcp-Method", "tools/call"),
        ("Authorization", "Bearer accepted"),
    ];
    assert_eq!(
        request(address, "POST", &old_headers, &valid_call).await.0,
        400
    );

    let missing_capability = r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"remember","arguments":{},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}"#;
    assert_eq!(
        request(address, "POST", &tool_call_headers(), missing_capability)
            .await
            .0,
        400
    );
    assert_eq!(
        request(address, "POST", &tool_call_headers(), "{").await.0,
        200,
        "rmcp owns JSON-RPC parse errors; this request must still be preflighted"
    );

    let duplicate_auth = [
        ("MCP-Protocol-Version", "2026-07-28"),
        ("Mcp-Method", "tools/call"),
        ("Authorization", "Bearer first"),
        ("Authorization", "Bearer second"),
    ];
    assert_eq!(
        request(address, "POST", &duplicate_auth, &valid_call)
            .await
            .0,
        400
    );
    let invalid_origin = [
        ("MCP-Protocol-Version", "2026-07-28"),
        ("Mcp-Method", "tools/call"),
        ("Authorization", "Bearer accepted"),
        ("Origin", "https://wrong.test"),
    ];
    assert_eq!(
        request(address, "POST", &invalid_origin, &valid_call)
            .await
            .0,
        403
    );
    assert_eq!(request(address, "GET", &[], "").await.0, 405);
    assert_eq!(request(address, "DELETE", &[], "").await.0, 405);
    assert!(app.invocations.lock().expect("invocations").is_empty());
    assert!(
        app.operations
            .lock()
            .expect("operations")
            .contains(&McpOperation::Preflight),
        "invalid JSON must still reach the one pre-auth IP-rate callback"
    );
    server.abort();
}

#[tokio::test]
async fn authorization_errors_change_the_http_status_before_dispatch() {
    let app = Arc::new(MockApplication::default());
    let (address, server) = start(app.clone()).await;
    let body = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{}}}"#,
        modern_params(r#""name":"remember","arguments":{}"#)
    );
    *app.invoke_denied.lock().expect("invoke denied") = Some(ErrorCode::Unauthorized);
    let (status, _body, _) = request(address, "POST", &tool_call_headers(), &body).await;
    assert_eq!(status, 401);
    assert!(app.invocations.lock().expect("invocations").is_empty());
    *app.invoke_denied.lock().expect("invoke denied") = Some(ErrorCode::Forbidden);
    let (status, _body, _) = request(address, "POST", &tool_call_headers(), &body).await;
    assert_eq!(status, 403);
    server.abort();
}
