//! Native, stateless MCP-over-HTTP adapter.
//!
//! This module owns the rmcp/Axum boundary.  It deliberately exposes a
//! small application port instead of routing directly into a domain handler:
//! the gateway supplies the RequestGuard, authorization, billing, and business
//! dispatch later.  Raw request credentials and raw JSON arguments never
//! derive `Debug` or `Serialize` here.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    net::{IpAddr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    },
};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, StatusCode, request::Parts},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::any_service,
};
use humaux_domain::error::ErrorCode;
use rmcp::{
    RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ErrorData,
        InitializeRequestParams, InitializeResult, JsonObject, ListToolsResult, ProtocolVersion,
        ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::never::NeverSessionManager,
    },
};
use serde::Deserialize;
use serde_json::{Map, Value, value::RawValue};
use uuid::Uuid;

use crate::error_map::{self, McpError};

const NATIVE_PROTOCOL: ProtocolVersion = ProtocolVersion::V_2026_07_28;
const TOOL_SET: [ToolName; 8] = [
    ToolName::Remember,
    ToolName::Recall,
    ToolName::Memory,
    ToolName::Context,
    ToolName::Continuity,
    ToolName::Artifact,
    ToolName::Code,
    ToolName::Coordinate,
];

/// The only tool names the native MCP endpoint can expose.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ToolName {
    Remember,
    Recall,
    Memory,
    Context,
    Continuity,
    Artifact,
    Code,
    Coordinate,
}

impl ToolName {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Remember => "remember",
            Self::Recall => "recall",
            Self::Memory => "memory",
            Self::Context => "context",
            Self::Continuity => "continuity",
            Self::Artifact => "artifact",
            Self::Code => "code",
            Self::Coordinate => "coordinate",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        TOOL_SET.into_iter().find(|tool| tool.as_str() == value)
    }
}

/// A trusted registry item.  It is intentionally SDK-neutral; this module
/// converts it to rmcp's wire `Tool` only at the transport boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct McpToolSchema {
    pub name: ToolName,
    pub title: Option<String>,
    pub description: String,
    pub input_schema: Map<String, Value>,
    pub output_schema: Option<Map<String, Value>>,
}

impl McpToolSchema {
    fn as_rmcp_tool(&self) -> Tool {
        let tool = Tool::new(
            Cow::Borrowed(self.name.as_str()),
            Cow::Owned(self.description.clone()),
            Arc::new(self.input_schema.clone()),
        );
        let tool = self
            .title
            .clone()
            .map_or(tool.clone(), |title| tool.with_title(title));
        self.output_schema.clone().map_or(tool.clone(), |schema| {
            tool.with_raw_output_schema(Arc::new(schema))
        })
    }
}

/// The single trusted tool registry supplied by bootstrap.
#[derive(Clone, Debug)]
pub struct TrustedMcpCatalog {
    tools: BTreeMap<ToolName, McpToolSchema>,
}

impl TrustedMcpCatalog {
    /// Rejects partial, duplicate, or non-closed catalogs before the listener
    /// exists, so a caller cannot accidentally publish an implementation-sized
    /// subset of the canonical eight-tool surface.
    pub fn new(items: impl IntoIterator<Item = McpToolSchema>) -> Result<Self, ErrorCode> {
        let mut tools = BTreeMap::new();
        for item in items {
            if tools.insert(item.name, item).is_some() {
                return Err(ErrorCode::InvalidInput);
            }
        }
        let expected: BTreeSet<_> = TOOL_SET.into_iter().collect();
        if tools.len() != TOOL_SET.len()
            || tools.keys().copied().collect::<BTreeSet<_>>() != expected
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self { tools })
    }

    fn tool(&self, name: ToolName) -> Option<Tool> {
        self.tools.get(&name).map(McpToolSchema::as_rmcp_tool)
    }

    fn all_tools(&self) -> Vec<Tool> {
        TOOL_SET
            .into_iter()
            .filter_map(|name| self.tool(name))
            .collect()
    }
}

/// Operation sent to the future gateway guard.  The client never supplies a
/// tenant, user, owner, or request id through this type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum McpOperation {
    /// Unauthenticated IP-abuse accounting after the HTTP peer, host/origin,
    /// and body boundaries but before rmcp parses JSON-RPC. This happens once
    /// per HTTP request, including malformed or unknown JSON-RPC requests.
    Preflight,
    Initialize,
    Discover,
    Catalog,
    Ping,
}

/// Per-request data that the gateway can use to authenticate and audit.
///
/// Its secret-bearing fields are private on purpose. `request_id` is minted by
/// this server for each HTTP request, never copied from a JSON-RPC identifier.
#[derive(Clone)]
pub struct McpHttpContext {
    request_id: Uuid,
    peer_ip: IpAddr,
    authorization: Option<String>,
    forwarded: Option<String>,
    raw_arguments: Option<Arc<RawValue>>,
}

impl McpHttpContext {
    pub fn request_id(&self) -> Uuid {
        self.request_id
    }

    pub fn peer_ip(&self) -> IpAddr {
        self.peer_ip
    }

    pub fn authorization(&self) -> Option<&str> {
        self.authorization.as_deref()
    }

    pub fn forwarded(&self) -> Option<&str> {
        self.forwarded.as_deref()
    }

    /// Lexical JSON for `params.arguments`, preserved from the request body.
    /// It is `None` when a valid tools/call has no `arguments` member.
    pub fn raw_arguments_json(&self) -> Option<&str> {
        self.raw_arguments.as_deref().map(RawValue::get)
    }
}

/// Parsed and raw arguments travel together.  `decoded` is the SDK-validated
/// object; `raw_json` is only the original `params.arguments` JSON slice.
pub struct McpToolArguments<'a> {
    decoded: &'a JsonObject,
    raw_json: Option<&'a RawValue>,
}

impl<'a> McpToolArguments<'a> {
    pub fn decoded(&self) -> &'a JsonObject {
        self.decoded
    }

    pub fn raw_json(&self) -> Option<&'a str> {
        self.raw_json.map(RawValue::get)
    }
}

/// The dual-channel MCP result. `text` is carried directly into a text content
/// block; it is never reconstructed from JSON and therefore retains BOM/CRLF.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolOutput {
    pub text: String,
    pub structured_content: Value,
}

/// SDK-neutral application port.
///
/// `check_access` handles only the non-business protocol lifecycle. Its
/// `Preflight` invocation is the one unauthenticated IP-rate accounting hook
/// per accepted HTTP request; `Initialize`, `Discover`, `Catalog`, and `Ping`
/// are zero-BMO protocol calls. `invoke` is the sole complete business Guard
/// entry: authentication, scope, rate, entitlement, quota, handler, and
/// finalize occur there exactly once.
#[async_trait]
pub trait McpApplication: Send + Sync {
    async fn check_access(
        &self,
        context: &McpHttpContext,
        operation: McpOperation,
    ) -> Result<(), ErrorCode>;

    async fn invoke(
        &self,
        context: &McpHttpContext,
        tool: ToolName,
        arguments: McpToolArguments<'_>,
    ) -> Result<ToolOutput, ErrorCode>;
}

/// Explicit listener configuration.  Empty host/origin lists are rejected:
/// rmcp treats empty origins as disabled validation, which this adapter never
/// permits for a public endpoint.
#[derive(Clone, Debug)]
pub struct McpHttpConfig {
    allowed_hosts: Vec<String>,
    allowed_origins: Vec<String>,
    max_request_body_bytes: usize,
}

impl McpHttpConfig {
    pub fn new(
        allowed_hosts: Vec<String>,
        allowed_origins: Vec<String>,
        max_request_body_bytes: usize,
    ) -> Result<Self, ErrorCode> {
        if allowed_hosts.is_empty() || allowed_origins.is_empty() || max_request_body_bytes == 0 {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            allowed_hosts,
            allowed_origins,
            max_request_body_bytes,
        })
    }

    fn sdk_config(&self) -> StreamableHttpServerConfig {
        StreamableHttpServerConfig::default()
            .with_allowed_hosts(self.allowed_hosts.iter().cloned())
            .with_allowed_origins(self.allowed_origins.iter().cloned())
            .with_legacy_session_mode(false)
            .with_json_response(true)
            .with_max_request_body_bytes(self.max_request_body_bytes)
            .with_stateless_protocol_metadata_required(true)
    }
}

/// Builds the `/mcp` router.  The caller must serve it with
/// `into_make_service_with_connect_info::<SocketAddr>()`; missing peer address
/// is rejected before the application port can be reached.
#[derive(Clone)]
pub struct McpAdapter {
    application: Arc<dyn McpApplication>,
    catalog: TrustedMcpCatalog,
    config: McpHttpConfig,
}

impl McpAdapter {
    pub fn new(
        application: Arc<dyn McpApplication>,
        catalog: TrustedMcpCatalog,
        config: McpHttpConfig,
    ) -> Self {
        Self {
            application,
            catalog,
            config,
        }
    }

    pub fn router(&self) -> Router {
        let state = HandlerState {
            application: self.application.clone(),
            catalog: self.catalog.clone(),
        };
        let service = StreamableHttpService::new(
            move || {
                Ok(NativeMcpHandler {
                    state: state.clone(),
                })
            },
            Arc::new(NeverSessionManager::default()),
            self.config.sdk_config(),
        );
        Router::new()
            .route_service("/mcp", any_service(service))
            .layer(middleware::from_fn_with_state(
                BoundaryConfig {
                    application: self.application.clone(),
                    allowed_hosts: self.config.allowed_hosts.clone(),
                    allowed_origins: self.config.allowed_origins.clone(),
                    max_request_body_bytes: self.config.max_request_body_bytes,
                },
                native_request_boundary,
            ))
    }
}

#[derive(Clone)]
struct HandlerState {
    application: Arc<dyn McpApplication>,
    catalog: TrustedMcpCatalog,
}

#[derive(Clone)]
struct BoundaryConfig {
    application: Arc<dyn McpApplication>,
    allowed_hosts: Vec<String>,
    allowed_origins: Vec<String>,
    max_request_body_bytes: usize,
}

/// State inserted into the HTTP request parts and then picked up from rmcp's
/// `RequestContext::extensions`. It intentionally has no Debug implementation.
#[derive(Clone)]
struct McpRequestState {
    request_id: Uuid,
    raw_arguments: Option<Arc<RawValue>>,
    http_status: Arc<AtomicU16>,
}

#[derive(Deserialize)]
struct RawJsonRpcRequest {
    params: Option<RawJsonRpcParams>,
}

#[derive(Deserialize)]
struct RawJsonRpcParams {
    arguments: Option<Box<RawValue>>,
}

async fn native_request_boundary(
    State(config): State<BoundaryConfig>,
    request: Request,
    next: Next,
) -> Response {
    if request.method() != axum::http::Method::POST {
        return (StatusCode::METHOD_NOT_ALLOWED, [("allow", "POST")]).into_response();
    }
    if has_ambiguous_header(request.headers()) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if request
        .headers()
        .get("mcp-protocol-version")
        .and_then(|version| version.to_str().ok())
        != Some(NATIVE_PROTOCOL.as_str())
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if !trusted_host_origin(&request, &config) {
        return StatusCode::FORBIDDEN.into_response();
    }

    let (mut parts, body) = request.into_parts();
    let bytes = match to_bytes(body, config.max_request_body_bytes).await {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    let raw_arguments = serde_json::from_slice::<RawJsonRpcRequest>(&bytes)
        .ok()
        .and_then(|request| request.params.and_then(|params| params.arguments))
        .map(Arc::from);
    let status = Arc::new(AtomicU16::new(StatusCode::OK.as_u16()));
    let request_state = McpRequestState {
        request_id: Uuid::now_v7(),
        raw_arguments,
        http_status: status.clone(),
    };
    let context = match http_context(&parts, &request_state) {
        Ok(context) => context,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    if let Err(code) = config
        .application
        .check_access(&context, McpOperation::Preflight)
        .await
    {
        let mapping = error_map::lookup(code);
        return (
            StatusCode::from_u16(mapping.http_status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            code.as_str(),
        )
            .into_response();
    }
    if !has_required_native_metadata(&bytes) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    parts.extensions.insert(request_state);
    let mut response = next
        .run(Request::from_parts(parts, Body::from(bytes)))
        .await;
    if let Ok(status) = StatusCode::from_u16(status.load(Ordering::Acquire)) {
        *response.status_mut() = status;
    }
    response
}

fn has_ambiguous_header(headers: &HeaderMap) -> bool {
    ["authorization", "forwarded", "host", "origin"]
        .into_iter()
        .any(|name| headers.get_all(name).iter().count() != 1 && headers.contains_key(name))
        || headers.keys().any(|name| {
            name.as_str().starts_with("mcp-") && headers.get_all(name).iter().count() != 1
        })
}

fn trusted_host_origin(request: &Request, config: &BoundaryConfig) -> bool {
    let Some(host) = request
        .headers()
        .get("host")
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    if !config.allowed_hosts.iter().any(|allowed| allowed == host) {
        return false;
    }
    request
        .headers()
        .get("origin")
        .map(|origin| {
            origin.to_str().is_ok_and(|origin| {
                config
                    .allowed_origins
                    .iter()
                    .any(|allowed| allowed == origin)
            })
        })
        .unwrap_or(true)
}

/// rmcp remains the JSON-RPC parser and metadata validator. This small gate
/// only closes the native requirement that must be known before application
/// dispatch: non-initialize requests carry current metadata and capabilities.
fn has_required_native_metadata(body: &[u8]) -> bool {
    let Ok(message) = serde_json::from_slice::<Value>(body) else {
        return true;
    };
    if message.get("method").and_then(Value::as_str) == Some("initialize") {
        return true;
    }
    let Some(meta) = message
        .get("params")
        .and_then(Value::as_object)
        .and_then(|params| params.get("_meta"))
        .and_then(Value::as_object)
    else {
        return false;
    };
    meta.get("io.modelcontextprotocol/protocolVersion")
        .and_then(Value::as_str)
        == Some(NATIVE_PROTOCOL.as_str())
        && meta.contains_key("io.modelcontextprotocol/clientInfo")
        && meta.contains_key("io.modelcontextprotocol/clientCapabilities")
}

#[derive(Clone)]
struct NativeMcpHandler {
    state: HandlerState,
}

impl NativeMcpHandler {
    fn context(
        &self,
        request: &RequestContext<RoleServer>,
    ) -> Result<(McpHttpContext, McpRequestState), ErrorData> {
        let parts = request.extensions.get::<Parts>().ok_or_else(|| {
            ErrorData::new(
                rmcp::model::ErrorCode::INTERNAL_ERROR,
                "missing HTTP request context",
                None,
            )
        })?;
        let state = parts
            .extensions
            .get::<McpRequestState>()
            .cloned()
            .ok_or_else(|| {
                ErrorData::new(
                    rmcp::model::ErrorCode::INTERNAL_ERROR,
                    "missing MCP request state",
                    None,
                )
            })?;
        let context = http_context(parts, &state).map_err(|_| {
            ErrorData::new(
                rmcp::model::ErrorCode::INTERNAL_ERROR,
                "invalid HTTP request context",
                None,
            )
        })?;
        Ok((context, state))
    }

    async fn authorize(
        &self,
        request: &RequestContext<RoleServer>,
        operation: McpOperation,
    ) -> Result<McpHttpContext, ErrorData> {
        let (context, state) = self.context(request)?;
        if let Err(code) = self
            .state
            .application
            .check_access(&context, operation)
            .await
        {
            return Err(protocol_error(&state, code));
        }
        Ok(context)
    }
}

fn http_context(parts: &Parts, state: &McpRequestState) -> Result<McpHttpContext, ()> {
    let peer_ip = parts
        .extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|peer| peer.0.ip())
        .ok_or(())?;
    Ok(McpHttpContext {
        request_id: state.request_id,
        peer_ip,
        authorization: single_header(&parts.headers, "authorization")?,
        forwarded: single_header(&parts.headers, "forwarded")?,
        raw_arguments: state.raw_arguments.clone(),
    })
}

impl ServerHandler for NativeMcpHandler {
    fn get_info(&self) -> ServerInfo {
        InitializeResult::new(ServerCapabilities::builder().enable_tools().build())
            .with_protocol_version(NATIVE_PROTOCOL)
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[NATIVE_PROTOCOL])
    }

    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, ErrorData> {
        self.authorize(&context, McpOperation::Initialize).await?;
        context.peer.set_peer_info(request);
        Ok(self.get_info())
    }

    async fn discover(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::DiscoverResult, ErrorData> {
        self.authorize(&context, McpOperation::Discover).await?;
        Ok(rmcp::model::DiscoverResult::from_server_info(
            vec![NATIVE_PROTOCOL],
            self.get_info(),
        ))
    }

    async fn ping(&self, context: RequestContext<RoleServer>) -> Result<(), ErrorData> {
        self.authorize(&context, McpOperation::Ping)
            .await
            .map(|_| ())
    }

    async fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        self.authorize(&context, McpOperation::Catalog).await?;
        Ok(ListToolsResult::with_all_items(
            self.state.catalog.all_tools(),
        ))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        ToolName::parse(name).and_then(|name| self.state.catalog.tool(name))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        request_context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let tool = ToolName::parse(&request.name)
            .ok_or_else(ErrorData::method_not_found::<rmcp::model::CallToolRequestMethod>)?;
        let (context, state) = self.context(&request_context)?;
        let empty_arguments = JsonObject::new();
        let decoded = request.arguments.as_ref().unwrap_or(&empty_arguments);
        let arguments = McpToolArguments {
            decoded,
            raw_json: context.raw_arguments.as_deref(),
        };
        match self
            .state
            .application
            .invoke(&context, tool, arguments)
            .await
        {
            Ok(output) => {
                let mut result = CallToolResult::success(vec![ContentBlock::text(output.text)]);
                result.structured_content = Some(output.structured_content);
                Ok(result.into())
            }
            Err(code) => match error_map::lookup(code).mcp_error {
                McpError::Protocol { .. } => Err(protocol_error(&state, code)),
                McpError::ToolError { code } => {
                    let mut result = CallToolResult::error(vec![ContentBlock::text(code)]);
                    result.structured_content = Some(serde_json::json!({ "code": code }));
                    Ok(result.into())
                }
            },
        }
    }
}

fn single_header(headers: &HeaderMap, name: &str) -> Result<Option<String>, ()> {
    let values = headers.get_all(name);
    let mut iter = values.iter();
    let Some(value) = iter.next() else {
        return Ok(None);
    };
    if iter.next().is_some() {
        return Err(());
    }
    value
        .to_str()
        .map(|value| Some(value.to_owned()))
        .map_err(|_| ())
}

fn protocol_error(state: &McpRequestState, code: ErrorCode) -> ErrorData {
    let mapping = error_map::lookup(code);
    state
        .http_status
        .store(mapping.http_status, Ordering::Release);
    let json_rpc_code = match mapping.mcp_error {
        McpError::Protocol { json_rpc_code } => json_rpc_code,
        McpError::ToolError { .. } => rmcp::model::ErrorCode::INTERNAL_ERROR.0,
    };
    ErrorData::new(
        rmcp::model::ErrorCode(json_rpc_code),
        code.as_str(),
        Some(serde_json::json!({ "code": code.as_str() })),
    )
}
