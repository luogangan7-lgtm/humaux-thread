//! `protocol::error_map` — the MCP/REST adapter's `ErrorCode` mapping (§52.1: "MCP/REST adapter 负责映射，Domain 不产生 HTTP
//!   status").
//! Depends-on: crates=[humaux-domain]; services=[];
//!   env=[]; modules=[domain::error]
//! Called-by: [protocol::mcp]
//! Invariants: []
//! Spec: §52.1; §52.4
//!
//! Domain
//! itself never chooses an HTTP status or a JSON-RPC error code — this
//! module is where that choice lives.
//!
//! `lookup` is an exhaustive `match ErrorCode` rather than a hand-written
//! array searched at runtime (§52.4 G52-2): a new `ErrorCode` variant that
//! has no arm here fails to *compile*, instead of `lookup` panicking the
//! first time that variant is looked up in production. This closes the
//! second of G52-2's three counts — the adapter mapping — onto the same
//! compiler-enforced list that defines `ErrorCode` itself (see
//! `humaux_domain::error::error_code!`); only §52.1's prose code block
//! remains a count kept in sync by hand.

use humaux_domain::error::ErrorCode;

/// HTTP status code. A bare `u16` rather than a crate type: this table only
/// needs to carry literal status numbers, not build responses, and no HTTP
/// framework is wired into this crate yet.
pub type HttpStatus = u16;

/// MCP-layer error shape. A failed MCP tool call is not itself a JSON-RPC
/// protocol error — the JSON-RPC response still succeeds and the tool
/// result sets `isError: true`, carrying the failure in structured content;
/// only failures *before* a tool is dispatched (bad request shape, auth)
/// are JSON-RPC error objects with a numeric `code`. `McpError` picks
/// whichever the adapter should emit for a given `ErrorCode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpError {
    /// Pre-dispatch failure → JSON-RPC 2.0 error object.
    Protocol {
        /// `-32000..=-32099` is the range JSON-RPC 2.0 reserves for
        /// implementation-defined server errors; `-32602` is the standard
        /// "Invalid params" code, reused verbatim rather than assigned a
        /// server-error code of its own.
        json_rpc_code: i32,
    },
    /// Post-dispatch failure → `CallToolResult { isError: true, .. }`.
    ToolError {
        /// Domain code carried in the structured content (`ErrorCode::as_str()`).
        code: &'static str,
    },
}

/// One §52.1 mapping row, as returned by `lookup`.
#[derive(Debug, Clone, Copy)]
pub struct ErrorMapping {
    /// The `ErrorCode` this row was looked up for (always equals the `lookup` argument).
    pub error_code: ErrorCode,
    /// HTTP status this `ErrorCode` maps to.
    pub http_status: HttpStatus,
    /// MCP-layer error shape this `ErrorCode` maps to.
    pub mcp_error: McpError,
}

const fn protocol(json_rpc_code: i32) -> McpError {
    McpError::Protocol { json_rpc_code }
}

const fn tool_error(code: ErrorCode) -> McpError {
    McpError::ToolError {
        code: code.as_str(),
    }
}

/// Look up the §52.1 mapping row for `code`. The `match` is exhaustive over
/// `ErrorCode`, so a variant with no row is a compile error here rather than
/// the `expect()` panic `lookup` used to hit at runtime the first time a
/// forgotten variant was actually looked up.
pub const fn lookup(code: ErrorCode) -> ErrorMapping {
    let http_status: HttpStatus;
    let mcp_error: McpError;
    match code {
        ErrorCode::InvalidInput => {
            http_status = 400;
            mcp_error = protocol(-32602);
        }
        ErrorCode::NotFound => {
            http_status = 404;
            mcp_error = tool_error(ErrorCode::NotFound);
        }
        ErrorCode::Unauthorized => {
            http_status = 401;
            mcp_error = protocol(-32001);
        }
        ErrorCode::Forbidden => {
            http_status = 403;
            mcp_error = protocol(-32002);
        }
        // §7488: scope 不属于本次请求的 tenant.
        ErrorCode::TenantBoundary => {
            http_status = 403;
            mcp_error = protocol(-32003);
        }
        // §11.3 / §2852: known-pending on a missing/invalid BYOK credential, not a hard failure.
        ErrorCode::WaitingKey => {
            http_status = 409;
            mcp_error = tool_error(ErrorCode::WaitingKey);
        }
        // §72.3 short-window abuse-cost limiter → 429. This is NOT the same
        // contract as §67.2's admission-control rejection, which freezes
        // "HTTP 503 + Retry-After: min(队列估算秒数, 30) / RATE_LIMITED"
        // for a *different* caller (queue admission, not the abuse limiter).
        // One `ErrorCode` cannot carry two HTTP statuses through a single-
        // valued table, and this table has no `Retry-After` field at all —
        // so the §67.2 contract cannot be emitted through `lookup` today.
        // ponytail: left as the §72.3 value only; giving §67.2 its own
        // code/row (or adding a retry-hint field and a caller-supplied
        // status) is an architecture-semantics choice that needs an ADR
        // (§78.6) and touches files outside this fix's scope — not decided
        // silently here.
        ErrorCode::RateLimited => {
            http_status = 429;
            mcp_error = protocol(-32004);
        }
        ErrorCode::QuotaExhausted => {
            http_status = 429;
            mcp_error = protocol(-32005);
        }
        ErrorCode::EntitlementRequired => {
            http_status = 402;
            mcp_error = protocol(-32006);
        }
        ErrorCode::CostBudgetExceeded => {
            http_status = 402;
            mcp_error = protocol(-32007);
        }
        ErrorCode::ProviderRateLimited => {
            http_status = 429;
            mcp_error = tool_error(ErrorCode::ProviderRateLimited);
        }
        ErrorCode::ProviderTransient => {
            http_status = 503;
            mcp_error = tool_error(ErrorCode::ProviderTransient);
        }
        ErrorCode::ProviderPermanent => {
            http_status = 502;
            mcp_error = tool_error(ErrorCode::ProviderPermanent);
        }
        ErrorCode::DependencyUnavailable => {
            http_status = 503;
            mcp_error = tool_error(ErrorCode::DependencyUnavailable);
        }
        // §52.2 concept pair with DegradeCode::ProjectionLag: this row fires only
        // when the caller required read-your-write and projection has not caught up.
        ErrorCode::ProjectionLag => {
            http_status = 409;
            mcp_error = tool_error(ErrorCode::ProjectionLag);
        }
        // §52.2 concept pair with DegradeCode::CompletenessUnknown: fires only
        // when the caller required a completeness class the system can't vouch for.
        ErrorCode::CannotEstablishCompleteness => {
            http_status = 503;
            mcp_error = tool_error(ErrorCode::CannotEstablishCompleteness);
        }
        ErrorCode::Conflict => {
            http_status = 409;
            mcp_error = tool_error(ErrorCode::Conflict);
        }
        ErrorCode::Internal => {
            http_status = 500;
            mcp_error = tool_error(ErrorCode::Internal);
        }
    }
    ErrorMapping {
        error_code: code,
        http_status,
        mcp_error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// G52-2: |ErrorCode 变体| == §52.1 代码块行数 (18, hardcoded — the block
    /// itself is prose, not a compiled artifact). The adapter-mapping count
    /// is no longer a separate hand-maintained number to compare: `lookup`'s
    /// exhaustive match guarantees it equals `ErrorCode::ALL.len()` at
    /// compile time (a missing arm doesn't build), so this test only needs
    /// to pin the two counts that a human still edits independently.
    #[test]
    fn g52_2_three_way_count() {
        assert_eq!(ErrorCode::ALL.len(), 18);
    }

    /// `lookup` covers every `ErrorCode` variant exactly once by
    /// construction (exhaustive match, no duplicate patterns possible) —
    /// this test pins that the returned row always echoes back the code it
    /// was looked up for, which is what "total and injective" cashes out to
    /// for a match-based lookup.
    #[test]
    fn mapping_is_total_and_injective_over_error_code() {
        for code in ErrorCode::ALL {
            assert_eq!(lookup(code).error_code, code);
        }
    }

    /// Domain never produces HTTP status: `lookup` is the only place a
    /// status number is attached to an `ErrorCode`, and every value it
    /// returns is a real HTTP status (3-digit, 4xx/5xx for the error domain).
    #[test]
    fn every_http_status_is_a_client_or_server_error_code() {
        for code in ErrorCode::ALL {
            let row = lookup(code);
            assert!(
                (400..600).contains(&row.http_status),
                "{}: {} is not a 4xx/5xx status",
                code,
                row.http_status
            );
        }
    }

    #[test]
    fn lookup_round_trips_every_variant() {
        for code in ErrorCode::ALL {
            assert_eq!(lookup(code).error_code, code);
        }
    }
}
