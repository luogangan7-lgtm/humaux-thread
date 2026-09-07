//! Canonical MCP tool schema and operation registry (§33.1 / §33.10).
//!
//! The embedded `contracts/mcp` documents are the only operation/permission/
//! BMO metadata source. This adapter merely turns their validated JSON into
//! protocol types; it never authenticates, checks entitlement, or logs args.

use std::collections::{BTreeMap, BTreeSet};

use humaux_domain::error::ErrorCode;
use jsonschema::Validator;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::mcp::{McpToolSchema, ToolName, TrustedMcpCatalog};

const MANIFEST: &str = include_str!("../../../contracts/mcp/manifest.json");
const OUTPUT_SCHEMA: &str = include_str!("../../../contracts/mcp/output.schema.json");
const FEATURE_KEY: &str = "mcp.billable_operations.per_period";
const SCOPES: [&str; 5] = [
    "context:read",
    "memory:write",
    "artifact:manage",
    "code:manage",
    "coordination:manage",
];

/// The accounting meaning selected by a fully validated canonical operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MeterKind {
    Ordinary,
    Zero,
    BatchReservation,
    BatchConsumption,
}

impl MeterKind {
    fn parse(value: &str) -> Result<Self, ErrorCode> {
        match value {
            "ordinary1" => Ok(Self::Ordinary),
            "zeroControl" => Ok(Self::Zero),
            "batchReservation" => Ok(Self::BatchReservation),
            "batchConsumption" => Ok(Self::BatchConsumption),
            _ => Err(ErrorCode::InvalidInput),
        }
    }
}

/// A catalog-authenticated operation. It cannot be deserialized or hand-built
/// by callers: the only constructor is [`CanonicalCatalog::validate`].
pub struct OperationDescriptor {
    tool: ToolName,
    operation_key: String,
    required_scope: String,
    feature_key: String,
    meter_kind: MeterKind,
}

impl OperationDescriptor {
    pub const fn tool(&self) -> ToolName {
        self.tool
    }

    pub fn operation_key(&self) -> &str {
        &self.operation_key
    }

    pub fn required_scope(&self) -> &str {
        &self.required_scope
    }

    pub fn feature_key(&self) -> &str {
        &self.feature_key
    }

    pub const fn meter_kind(&self) -> MeterKind {
        self.meter_kind
    }
}

/// Loaded once by bootstrap; every schema, action, scope, and meter value is
/// checked before the listener can publish a catalog.
pub struct CanonicalCatalog {
    tools: BTreeMap<ToolName, ToolEntry>,
}

struct ToolEntry {
    advertised: McpToolSchema,
    root_validator: Validator,
    output_validator: Validator,
    operations: Vec<Operation>,
}

struct Operation {
    validator: Validator,
    metadata: OperationMetadata,
}

#[derive(Deserialize)]
struct Manifest {
    contract_version: String,
    protocol_version: String,
    shared_output_schema: String,
    tools: Vec<ManifestTool>,
}

#[derive(Deserialize)]
struct ManifestTool {
    name: String,
    title: String,
    description: String,
    input_schema: String,
    output_schema: Option<String>,
    output_schema_pointer: Option<String>,
}

#[derive(Deserialize)]
struct OperationMetadata {
    operation_key: String,
    required_scope: String,
    feature_key: String,
    meter_kind: String,
}

impl CanonicalCatalog {
    /// Loads the closed catalog from compile-time embedded contract documents.
    /// No filesystem or network lookup is available to schema loading.
    pub fn load() -> Result<Self, ErrorCode> {
        Self::from_manifest(MANIFEST)
    }

    /// Converts the exact same source into the MCP SDK-neutral advertised
    /// catalog. No operation metadata is duplicated in this conversion.
    pub fn trusted_catalog(&self) -> Result<TrustedMcpCatalog, ErrorCode> {
        TrustedMcpCatalog::new(self.tools.values().map(|entry| entry.advertised.clone()))
    }

    /// Validates a tool argument object and returns its only derived operation
    /// descriptor. Gateway code must consume this result, not deserialize an
    /// operation/scope/meter value from client arguments.
    pub fn validate(
        &self,
        tool: ToolName,
        arguments: &Value,
    ) -> Result<OperationDescriptor, ErrorCode> {
        let entry = self.tools.get(&tool).ok_or(ErrorCode::InvalidInput)?;
        if entry.root_validator.validate(arguments).is_err() {
            return Err(ErrorCode::InvalidInput);
        }
        let mut matches = entry
            .operations
            .iter()
            .filter(|operation| operation.validator.is_valid(arguments));
        let operation = matches.next().ok_or(ErrorCode::InvalidInput)?;
        if matches.next().is_some() {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(OperationDescriptor {
            tool,
            operation_key: operation.metadata.operation_key.clone(),
            required_scope: operation.metadata.required_scope.clone(),
            feature_key: operation.metadata.feature_key.clone(),
            meter_kind: MeterKind::parse(&operation.metadata.meter_kind)?,
        })
    }

    /// Checks successful business output against the exact schema advertised by tools/list.
    /// A server/schema disagreement is an internal error, never a client input error.
    pub fn validate_output(&self, tool: ToolName, output: &Value) -> Result<(), ErrorCode> {
        let entry = self.tools.get(&tool).ok_or(ErrorCode::Internal)?;
        entry
            .output_validator
            .validate(output)
            .map_err(|_| ErrorCode::Internal)
    }

    fn from_manifest(raw_manifest: &str) -> Result<Self, ErrorCode> {
        let manifest: Manifest =
            serde_json::from_str(raw_manifest).map_err(|_| ErrorCode::InvalidInput)?;
        if manifest.contract_version != "1"
            || manifest.protocol_version != "2026-07-28"
            || manifest.shared_output_schema != "output.schema.json"
            || manifest.tools.len() != 8
        {
            return Err(ErrorCode::InvalidInput);
        }
        let mut tools = BTreeMap::new();
        for item in manifest.tools {
            let tool = parse_tool(&item.name).ok_or(ErrorCode::InvalidInput)?;
            let source = schema_source(&item.input_schema).ok_or(ErrorCode::InvalidInput)?;
            let schema = json_document(source)?;
            reject_external_references(&schema)?;
            let root_validator = validator_for(&schema)?;
            let operations = operations_from_schema(&schema)?;
            let output_source = match item.output_schema.as_deref() {
                None => OUTPUT_SCHEMA,
                Some(name) => schema_source(name).ok_or(ErrorCode::InvalidInput)?,
            };
            let output_document = json_document(output_source)?;
            let mut output_schema = match item.output_schema_pointer.as_deref() {
                None => output_document,
                Some(pointer) => output_document
                    .pointer(pointer)
                    .cloned()
                    .ok_or(ErrorCode::InvalidInput)?,
            };
            if !output_schema.is_object() {
                return Err(ErrorCode::InvalidInput);
            }
            resolve_canonical_envelope(&mut output_schema)?;
            reject_external_references(&output_schema)?;
            let output_validator = validator_for(&output_schema)?;
            let advertised = McpToolSchema {
                name: tool,
                title: Some(item.title),
                description: item.description,
                input_schema: object_schema(schema)?,
                output_schema: Some(object_schema(output_schema)?),
            };
            if tools
                .insert(
                    tool,
                    ToolEntry {
                        advertised,
                        root_validator,
                        output_validator,
                        operations,
                    },
                )
                .is_some()
            {
                return Err(ErrorCode::InvalidInput);
            }
        }
        if tools.len() != 8 {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self { tools })
    }
}

/// Convenience bootstrap entry point for callers that only publish tools.
pub fn trusted_catalog() -> Result<TrustedMcpCatalog, ErrorCode> {
    CanonicalCatalog::load()?.trusted_catalog()
}

fn parse_tool(value: &str) -> Option<ToolName> {
    match value {
        "remember" => Some(ToolName::Remember),
        "recall" => Some(ToolName::Recall),
        "memory" => Some(ToolName::Memory),
        "context" => Some(ToolName::Context),
        "continuity" => Some(ToolName::Continuity),
        "artifact" => Some(ToolName::Artifact),
        "code" => Some(ToolName::Code),
        "coordinate" => Some(ToolName::Coordinate),
        _ => None,
    }
}

fn schema_source(name: &str) -> Option<&'static str> {
    match name {
        "remember.schema.json" => Some(include_str!("../../../contracts/mcp/remember.schema.json")),
        "recall.schema.json" => Some(include_str!("../../../contracts/mcp/recall.schema.json")),
        "memory.schema.json" => Some(include_str!("../../../contracts/mcp/memory.schema.json")),
        "memory.output.schema.json" => Some(include_str!(
            "../../../contracts/mcp/memory.output.schema.json"
        )),
        "context.schema.json" => Some(include_str!("../../../contracts/mcp/context.schema.json")),
        "context.output.schema.json" => Some(include_str!(
            "../../../contracts/mcp/context.output.schema.json"
        )),
        "continuity.schema.json" => Some(include_str!(
            "../../../contracts/mcp/continuity.schema.json"
        )),
        "continuity.output.schema.json" => Some(include_str!(
            "../../../contracts/mcp/continuity.output.schema.json"
        )),
        "artifact.schema.json" => Some(include_str!("../../../contracts/mcp/artifact.schema.json")),
        "code.schema.json" => Some(include_str!("../../../contracts/mcp/code.schema.json")),
        "coordinate.schema.json" => Some(include_str!(
            "../../../contracts/mcp/coordinate.schema.json"
        )),
        _ => None,
    }
}

fn json_document(source: &str) -> Result<Value, ErrorCode> {
    serde_json::from_str(source).map_err(|_| ErrorCode::InvalidInput)
}

fn object_schema(schema: Value) -> Result<Map<String, Value>, ErrorCode> {
    schema.as_object().cloned().ok_or(ErrorCode::InvalidInput)
}

fn validator_for(schema: &Value) -> Result<Validator, ErrorCode> {
    jsonschema::draft202012::options()
        .should_validate_formats(true)
        .build(schema)
        .map_err(|_| ErrorCode::InvalidInput)
}

fn operations_from_schema(schema: &Value) -> Result<Vec<Operation>, ErrorCode> {
    let branches = schema
        .get("oneOf")
        .and_then(Value::as_array)
        .filter(|branches| !branches.is_empty())
        .ok_or(ErrorCode::InvalidInput)?;
    let mut keys = BTreeSet::new();
    let mut operations = Vec::with_capacity(branches.len());
    for branch in branches {
        let metadata: OperationMetadata = serde_json::from_value(
            branch
                .get("x-humaux-operation")
                .cloned()
                .ok_or(ErrorCode::InvalidInput)?,
        )
        .map_err(|_| ErrorCode::InvalidInput)?;
        validate_metadata(&metadata)?;
        if !keys.insert(metadata.operation_key.clone()) {
            return Err(ErrorCode::InvalidInput);
        }
        operations.push(Operation {
            validator: validator_for(branch)?,
            metadata,
        });
    }
    Ok(operations)
}

fn validate_metadata(metadata: &OperationMetadata) -> Result<(), ErrorCode> {
    if metadata.operation_key.is_empty()
        || !SCOPES.contains(&metadata.required_scope.as_str())
        || metadata.feature_key != FEATURE_KEY
    {
        return Err(ErrorCode::InvalidInput);
    }
    MeterKind::parse(&metadata.meter_kind)?;
    Ok(())
}

fn reject_external_references(value: &Value) -> Result<(), ErrorCode> {
    match value {
        Value::Array(values) => {
            for value in values {
                reject_external_references(value)?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                if matches!(key.as_str(), "$dynamicRef" | "$recursiveRef")
                    || (key == "$ref"
                        && !value
                            .as_str()
                            .is_some_and(|reference| reference.starts_with("#/")))
                {
                    return Err(ErrorCode::InvalidInput);
                }
                reject_external_references(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// The only cross-document references are embedded canonical Context sub-schemas. Do not add
/// a network/file resolver: advertised schemas must be self-contained and match validation.
fn resolve_canonical_envelope(value: &mut Value) -> Result<(), ErrorCode> {
    match value {
        Value::Object(fields) => {
            let canonical_pointer = match fields.get("$ref").and_then(Value::as_str) {
                Some("context.output.schema.json#/properties/content") => {
                    Some("/properties/content")
                }
                Some("context.output.schema.json#/properties/handoff") => {
                    Some("/properties/handoff")
                }
                _ => None,
            };
            if let Some(pointer) = canonical_pointer {
                if fields.len() != 1 {
                    return Err(ErrorCode::InvalidInput);
                }
                let canonical = json_document(
                    schema_source("context.output.schema.json").ok_or(ErrorCode::InvalidInput)?,
                )?;
                let envelope = canonical
                    .pointer(pointer)
                    .ok_or(ErrorCode::InvalidInput)?
                    .clone();
                reject_external_references(&envelope)?;
                *value = envelope;
            } else {
                for value in fields.values_mut() {
                    resolve_canonical_envelope(value)?;
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                resolve_canonical_envelope(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn manifest_with_memory_output_pointer(pointer: &str) -> String {
        let mut manifest: Value = serde_json::from_str(MANIFEST).expect("manifest fixture");
        let memory = manifest["tools"]
            .as_array_mut()
            .expect("tools fixture")
            .iter_mut()
            .find(|tool| tool["name"] == "memory")
            .expect("memory tool fixture");
        memory["output_schema"] = json!("context.output.schema.json");
        memory["output_schema_pointer"] = json!(pointer);
        serde_json::to_string(&manifest).expect("manifest serialization")
    }

    #[test]
    fn canonical_eight_are_unique_and_derived_from_schema_metadata() {
        let catalog = CanonicalCatalog::load().expect("closed canonical catalog");
        assert_eq!(catalog.tools.len(), 8);
        let descriptor = catalog
            .validate(ToolName::Remember, &json!({"operation":"begin_batch","client_batch_id":"018d32a7-0000-7000-8000-000000000001","declared_count":2}))
            .expect("valid begin_batch");
        assert_eq!(descriptor.tool(), ToolName::Remember);
        assert_eq!(descriptor.operation_key(), "remember.begin_batch");
        assert_eq!(descriptor.required_scope(), "memory:write");
        assert_eq!(descriptor.feature_key(), FEATURE_KEY);
        assert_eq!(descriptor.meter_kind(), MeterKind::BatchReservation);
    }

    #[test]
    fn batch_and_zero_control_never_add_a_second_bmo() {
        let catalog = CanonicalCatalog::load().expect("catalog");
        let batch_put = catalog
            .validate(ToolName::Remember, &json!({"operation":"put","content":"x","batch_id":"018d32a7-0000-7000-8000-000000000001"}))
            .expect("batch put");
        assert_eq!(batch_put.meter_kind(), MeterKind::BatchConsumption);
        let ordinary_put = catalog
            .validate(
                ToolName::Remember,
                &json!({"operation":"put","content":"x","idempotency_key":"local-retry-1"}),
            )
            .expect("ordinary put");
        assert_eq!(ordinary_put.meter_kind(), MeterKind::Ordinary);
        let heartbeat = catalog
            .validate(ToolName::Coordinate, &json!({"action":"task_heartbeat","task_id":"018d32a7-0000-7000-8000-000000000003"}))
            .expect("heartbeat");
        assert_eq!(heartbeat.meter_kind(), MeterKind::Zero);
        let release = catalog
            .validate(
                ToolName::Coordinate,
                &json!({"action":"lock_release","resource":"catalog"}),
            )
            .expect("lock release");
        assert_eq!(release.meter_kind(), MeterKind::Zero);
    }

    #[test]
    fn unknown_actions_injections_and_bounds_fail_closed() {
        let catalog = CanonicalCatalog::load().expect("catalog");
        for arguments in [
            json!({"query":"x","action":"unknown"}),
            json!({"query":"x","tenant_id":"018d32a7-0000-7000-8000-000000000001"}),
            json!({"query":"x","owner_user_id":"018d32a7-0000-7000-8000-000000000001"}),
            json!({"query":"x","user_id":"018d32a7-0000-7000-8000-000000000001"}),
            json!({"query":"x","principal":"018d32a7-0000-7000-8000-000000000001"}),
            json!({"query":"x","limit":101}),
            json!({"query":"x".repeat(4097)}),
            json!({"query":"x","workspace_id":"not-a-uuid"}),
        ] {
            assert!(matches!(
                catalog.validate(ToolName::Recall, &arguments),
                Err(ErrorCode::InvalidInput)
            ));
        }
        assert!(matches!(
            catalog.validate(
                ToolName::Remember,
                &json!({"operation":"begin_batch","client_batch_id":"018d32a7-0000-7000-8000-000000000001","declared_count":1001}),
            ),
            Err(ErrorCode::InvalidInput)
        ));
        assert!(matches!(
            catalog.validate(
                ToolName::Remember,
                &json!({"operation":"put","content":"x"})
            ),
            Err(ErrorCode::InvalidInput)
        ));
        for idempotency_key in [
            "contains space",
            "unicode-雪",
            "line\nbreak",
            &"a".repeat(129),
        ] {
            assert!(matches!(
                catalog.validate(
                    ToolName::Remember,
                    &json!({"operation":"put","content":"x","idempotency_key":idempotency_key}),
                ),
                Err(ErrorCode::InvalidInput)
            ));
        }
    }

    #[test]
    fn malformed_manifest_duplicate_and_external_ref_fail_closed() {
        assert!(matches!(
            CanonicalCatalog::from_manifest("{"),
            Err(ErrorCode::InvalidInput)
        ));
        let duplicate = MANIFEST.replacen("\"recall\"", "\"remember\"", 1);
        assert!(matches!(
            CanonicalCatalog::from_manifest(&duplicate),
            Err(ErrorCode::InvalidInput)
        ));
        assert!(matches!(
            reject_external_references(&json!({"$ref":"https://invalid.example/schema"})),
            Err(ErrorCode::InvalidInput)
        ));
        let unknown_output = MANIFEST.replace(
            "context.output.schema.json",
            "https://invalid.example/output",
        );
        assert!(matches!(
            CanonicalCatalog::from_manifest(&unknown_output),
            Err(ErrorCode::InvalidInput)
        ));
        let missing_pointer = manifest_with_memory_output_pointer("/properties/missing");
        assert!(matches!(
            CanonicalCatalog::from_manifest(&missing_pointer),
            Err(ErrorCode::InvalidInput)
        ));
        let non_schema_pointer =
            manifest_with_memory_output_pointer("/properties/handoff/required");
        assert!(matches!(
            CanonicalCatalog::from_manifest(&non_schema_pointer),
            Err(ErrorCode::InvalidInput)
        ));
    }

    #[test]
    fn context_output_cannot_silently_drop_diagnostics_or_body_envelope() {
        let catalog = CanonicalCatalog::load().expect("catalog");
        for old_or_incomplete in [
            json!({"mandatory":[],"pinned":[],"counts":{}}),
            json!({"items":[],"completeness":{"class":"cannot_establish"}}),
            json!({"handoff":{},"content":{}}),
        ] {
            assert!(matches!(
                catalog.validate_output(ToolName::Context, &old_or_incomplete),
                Err(ErrorCode::Internal)
            ));
        }
        // Memory now validates against the selected Context Envelope content schema.
        assert!(matches!(
            catalog.validate_output(ToolName::Memory, &json!({})),
            Err(ErrorCode::Internal)
        ));
        // Other tools retain the existing generic output contract.
        catalog
            .validate_output(ToolName::Remember, &json!({"accepted":true}))
            .expect("existing non-context output");
    }

    fn minimal_handoff() -> Value {
        json!({
            "context_snapshot_seq": 7,
            "snapshot_token_sha256": "ab".repeat(32),
            "mandatory": [],
            "pinned": [],
            "needs_verification": [],
            "not_judged": [],
            "unavailable_selectors": [],
            "overflow_manifest": [],
            "counts": {
                "mandatory_expected": 0,
                "mandatory_returned": 0,
                "mandatory_missing": 0,
                "pinned_expected": 0,
                "pinned_returned": 0,
                "pinned_excluded": 0,
                "overflow": false
            }
        })
    }

    fn minimal_continuity_result() -> Value {
        const SOURCE_KINDS: [&str; 15] = [
            "GOAL",
            "CURRENT_STATE",
            "DECISIONS",
            "REJECTIONS",
            "CONSTRAINTS",
            "KNOWN_ISSUES",
            "NEXT_ACTIONS",
            "ACTIVE_TASKS",
            "RECENT_CHANGES",
            "CODE",
            "TESTS",
            "CONFIG",
            "MIGRATIONS",
            "PROCEDURES",
            "OUTCOMES",
        ];
        let mut source = SOURCE_KINDS
            .into_iter()
            .map(|kind| json!({"kind":kind,"mode":"SOURCE_BACKED","status":"UNAVAILABLE"}))
            .collect::<Vec<_>>();
        source.insert(
            13,
            json!({"kind":"HANDOFF","mode":"SYSTEM_DERIVED","status":"CURRENT","payload_ref":"#/handoff"}),
        );
        source.push(
            json!({"kind":"COVERAGE","mode":"SYSTEM_DERIVED","status":"CURRENT","payload_ref":"#/coverage"}),
        );
        json!({
            "contract_version": "1",
            "project_id": "018d32a7-0000-7000-8000-000000000001",
            "snapshot": {
                "context_snapshot_seq": 7,
                "snapshot_token_sha256": "ab".repeat(32),
                "result_sha256": "cd".repeat(32)
            },
            "facets": source,
            "handoff": minimal_handoff(),
            "coverage": {
                "required": 17,
                "current": 2,
                "missing": 0,
                "unavailable": 15,
                "stale": 0,
                "conflicted": 0,
                "ratio": 2.0 / 17.0,
                "completeness_class": "CANNOT_ESTABLISH"
            }
        })
    }

    #[test]
    fn continuity_contract_is_project_bound_and_fixed_to_fifteen_plus_two() {
        let catalog = CanonicalCatalog::load().expect("catalog");
        let project_id = "018d32a7-0000-7000-8000-000000000001";
        let descriptor = catalog
            .validate(ToolName::Continuity, &json!({"project_id":project_id}))
            .expect("project-bound continuity.get");
        catalog
            .validate(
                ToolName::Continuity,
                &json!({"project_id":"018D32A7-0000-7000-B000-000000000001"}),
            )
            .expect("uppercase UUIDv7 project_id");
        assert_eq!(descriptor.operation_key(), "continuity.get");
        assert_eq!(descriptor.required_scope(), "context:read");
        for invalid in [
            json!({}),
            json!({"project_id":project_id,"limit":1}),
            json!({"project_id":"018d32a7-0000-4000-8000-000000000001"}),
            json!({"project_id":"018d32a7-0000-7000-7000-000000000001"}),
        ] {
            assert!(matches!(
                catalog.validate(ToolName::Continuity, &invalid),
                Err(ErrorCode::InvalidInput)
            ));
        }

        let advertised = Value::Object(
            catalog.tools[&ToolName::Continuity]
                .advertised
                .output_schema
                .clone()
                .expect("continuity output schema"),
        );
        let context =
            json_document(schema_source("context.output.schema.json").expect("embedded context"))
                .expect("canonical schema");
        assert_eq!(
            advertised["$defs"]["HandoffV1"],
            context["properties"]["handoff"]
        );

        let valid = minimal_continuity_result();
        catalog
            .validate_output(ToolName::Continuity, &valid)
            .expect("fixed 17-facet result");
        assert_eq!(valid["facets"].as_array().expect("facets").len(), 17);
        assert_eq!(valid["facets"][13]["kind"], "HANDOFF");
        assert_eq!(valid["facets"][16]["kind"], "COVERAGE");
    }

    #[test]
    fn continuity_output_faults_fail_closed() {
        let catalog = CanonicalCatalog::load().expect("catalog");
        let valid = minimal_continuity_result();
        let mut faults = Vec::new();

        let mut uppercase_project_id = valid.clone();
        uppercase_project_id["project_id"] = json!("018D32A7-0000-7000-B000-000000000001");
        catalog
            .validate_output(ToolName::Continuity, &uppercase_project_id)
            .expect("uppercase UUIDv7 project_id");

        let mut uuidv4_project_id = valid.clone();
        uuidv4_project_id["project_id"] = json!("018d32a7-0000-4000-8000-000000000001");
        faults.push(uuidv4_project_id);

        let mut bad_variant_project_id = valid.clone();
        bad_variant_project_id["project_id"] = json!("018d32a7-0000-7000-7000-000000000001");
        faults.push(bad_variant_project_id);

        let mut wrong_order = valid.clone();
        wrong_order["facets"][0]["kind"] = json!("CURRENT_STATE");
        faults.push(wrong_order);

        let mut writable_handoff = valid.clone();
        writable_handoff["facets"][13]["mode"] = json!("SOURCE_BACKED");
        faults.push(writable_handoff);

        let mut leaked_non_current = valid.clone();
        leaked_non_current["facets"][0]["current"] = json!({
            "facet_version_id":"018d32a7-0000-7000-8000-000000000002",
            "facet_version":1,
            "body":{},
            "body_sha256":"ef".repeat(32),
            "memory_source_ids":[],
            "evidence_source_ids":[]
        });
        faults.push(leaked_non_current);

        let mut dynamic_denominator = valid.clone();
        dynamic_denominator["coverage"]["required"] = json!(15);
        faults.push(dynamic_denominator);

        let mut truncated = valid;
        truncated["facets"].as_array_mut().expect("facets").pop();
        faults.push(truncated);

        for fault in faults {
            assert!(matches!(
                catalog.validate_output(ToolName::Continuity, &fault),
                Err(ErrorCode::Internal)
            ));
        }
    }

    fn minimal_memory_envelope() -> Value {
        json!({
            "items": [],
            "pipeline": {
                "evidence": {"expected": None::<u64>, "expected_source":"none", "persisted":None::<u64>, "count_scope":"authorized_view"},
                "knowledge": {"eligible":0, "processed":0, "waiting_key":0, "failed":0, "count_scope":"authorized_view"},
                "projection": {"expected":0, "done":0, "deleted":0, "skipped":0, "visible":0, "open_gaps":0, "pending":0, "completeness_ratio":1.0, "current":true}
            },
            "completeness": {"class":"cannot_establish", "reason":"count_unknown", "exact":None::<Value>, "known_lower_bound":None::<u64>, "lanes":{}, "candidate_count":0, "reranked_count":0, "returned":0, "truncated":false, "degradations":[]},
            "provenance": {
                "binary_build":"test", "projection_version":{"status":"not_applicable"}, "embedding_model_id":{"status":"not_applicable"}, "rerank_model_id":{"status":"not_applicable"}, "card_builder_version":{"status":"not_applicable"}, "profile_fingerprint":"test", "profile":{"top_k":0,"cand_k":0,"cand_k_formula":"test","lanes":[]}
            },
            "freshness": {"class":"unknown", "latest_evidence_at":None::<String>, "state_age_seconds":None::<u64>},
            "grounding": {"current":0,"recheck_required":0,"unresolved":0,"cannot_establish":0,"not_judged":0,"revokes_current_truth_assumption":0},
            "mandatory":{"state":"not_run"}, "pinned":{"state":"not_run"}
        })
    }

    #[test]
    #[allow(clippy::too_many_lines)] // one exhaustive pin of the memory.output oneOf branch order
    fn memory_output_accepts_bare_and_snapshot_page_shapes() {
        let catalog = CanonicalCatalog::load().expect("catalog");
        let advertised = Value::Object(
            catalog.tools[&ToolName::Memory]
                .advertised
                .output_schema
                .clone()
                .expect("advertised output schema"),
        );
        let context =
            json_document(schema_source("context.output.schema.json").expect("embedded context"))
                .expect("canonical schema");
        assert_eq!(
            advertised["$defs"]["Envelope"],
            context["properties"]["content"]
        );
        assert_eq!(
            advertised["oneOf"]
                .as_array()
                .expect("stable output union")
                .len(),
            15
        );
        assert_eq!(advertised["oneOf"][0]["$ref"], "#/$defs/Envelope");
        // ADR-0018: memory.supersede's two results are the third and fourth branches.
        assert_eq!(
            advertised["oneOf"][2]["$ref"],
            "#/$defs/ConfirmationRequired"
        );
        assert_eq!(advertised["oneOf"][3]["$ref"], "#/$defs/Superseded");
        // ADR-0019: memory.pin / memory.unpin executed result.
        assert_eq!(advertised["oneOf"][4]["$ref"], "#/$defs/BindingWritten");
        // ADR-0020: memory.restore's executed result and its success-shaped CONFLICT-with-reason.
        assert_eq!(advertised["oneOf"][5]["$ref"], "#/$defs/Restored");
        // ADR-0024: memory.archive / memory.unarchive executed result.
        assert_eq!(advertised["oneOf"][6]["$ref"], "#/$defs/ArchiveChanged");
        assert_eq!(advertised["oneOf"][7]["$ref"], "#/$defs/ConflictReason");
        // ADR-0025: memory.correct's executed result (Correction Event + new version).
        assert_eq!(advertised["oneOf"][8]["$ref"], "#/$defs/Corrected");
        // ADR-0026 (Card 6): memory.confirm / memory.reject executed results + the candidates page.
        assert_eq!(advertised["oneOf"][9]["$ref"], "#/$defs/Confirmed");
        assert_eq!(advertised["oneOf"][10]["$ref"], "#/$defs/Rejected");
        assert_eq!(advertised["oneOf"][11]["$ref"], "#/$defs/CandidatesPage");
        // ADR-0028: memory.enumerate {subjects:true} page (memory<->subject linkage read-back)
        // and the single-subject result of memory.subject_register / memory.subject_link_key.
        assert_eq!(advertised["oneOf"][12]["$ref"], "#/$defs/SubjectsPage");
        assert_eq!(advertised["oneOf"][13]["$ref"], "#/$defs/Subject");
        // ADR-0030: memory.annotate_affect's result (affect rows + re-projection ticket).
        assert_eq!(advertised["oneOf"][14]["$ref"], "#/$defs/AffectAnnotated");
        // D-D: the Envelope item may carry its affect annotations with effective_intensity.
        assert_eq!(
            advertised["$defs"]["Envelope"]["properties"]["items"]["items"]["properties"]["affects"]
                ["items"]["required"][3],
            "effective_intensity"
        );
        // D-D: the Envelope item may carry its subject links (memory.get / memory.enumerate).
        assert_eq!(
            advertised["$defs"]["Envelope"]["properties"]["items"]["items"]["properties"]["subjects"]
                ["items"]["format"],
            "uuid"
        );
        // A supersede confirmation must still name its successor; pin/unpin must not. ADR-0026
        // restructured ConfirmationRequired's conditionals into an allOf (candidate-target ops
        // join the memory-target ones), so the supersede branch is now allOf[0].
        assert_eq!(
            advertised["$defs"]["ConfirmationRequired"]["allOf"][0]["if"]["properties"]["operation"]
                ["const"],
            "memory.supersede"
        );
        assert_eq!(
            advertised["$defs"]["ConfirmationRequired"]["properties"]["confirmation_required"]["const"],
            true
        );
        assert_eq!(
            advertised["oneOf"][1]["properties"]["content"]["$ref"],
            "#/$defs/Envelope"
        );
        // D-E (card 13, ADR-0035): BindingWritten reports the binding's scope { kind: WORKSPACE,
        // id }. `scope` is required and closed to a WORKSPACE-kinded uuid — dropping the field or
        // widening `kind` off the const turns this red.
        assert!(
            advertised["$defs"]["BindingWritten"]["required"]
                .as_array()
                .expect("BindingWritten.required")
                .iter()
                .any(|field| field == "scope"),
            "BindingWritten must require scope"
        );
        assert_eq!(
            advertised["$defs"]["BindingWritten"]["properties"]["scope"]["properties"]["kind"]["const"],
            "WORKSPACE"
        );
        assert_eq!(
            advertised["$defs"]["BindingWritten"]["properties"]["scope"]["properties"]["id"]["format"],
            "uuid"
        );
        let advertised_validator =
            validator_for(&advertised).expect("self-contained advertised schema");
        let bare = minimal_memory_envelope();
        catalog
            .validate_output(ToolName::Memory, &bare)
            .expect("bare canonical envelope");
        assert!(advertised_validator.is_valid(&bare));
        let page = json!({
            "content": bare,
            "pagination": {"snapshot_id":"018d32a7-0000-7000-8000-000000000001", "next_cursor":null}
        });
        catalog
            .validate_output(ToolName::Memory, &page)
            .expect("wrapped immutable page");
        assert!(advertised_validator.is_valid(&page));
        let mut missing_diagnostics = page;
        missing_diagnostics["content"]
            .as_object_mut()
            .expect("envelope")
            .remove("grounding");
        assert!(matches!(
            catalog.validate_output(ToolName::Memory, &missing_diagnostics),
            Err(ErrorCode::Internal)
        ));
    }

    #[test]
    fn memory_output_rejects_incomplete_or_external_shapes() {
        let catalog = CanonicalCatalog::load().expect("catalog");
        for value in [
            json!({"pagination":{"snapshot_id":"018d32a7-0000-7000-8000-000000000001","next_cursor":null}}),
            json!({"content": minimal_memory_envelope()}),
            json!({"content": minimal_memory_envelope(), "pagination":{"snapshot_id":"018d32a7-0000-7000-8000-000000000001"}}),
            json!({"content": minimal_memory_envelope(), "pagination":{"snapshot_id":"018d32a7-0000-7000-8000-000000000001","next_cursor":""}}),
            json!({"content": minimal_memory_envelope(), "pagination":{"snapshot_id":"bad","next_cursor":null}}),
            json!({"content": minimal_memory_envelope(), "pagination":{"snapshot_id":"018d32a7-0000-7000-8000-000000000001","next_cursor":null,"extra":true}}),
        ] {
            assert!(matches!(
                catalog.validate_output(ToolName::Memory, &value),
                Err(ErrorCode::Internal)
            ));
        }
        assert!(matches!(
            reject_external_references(&json!({"$ref":"https://invalid.example/schema"})),
            Err(ErrorCode::InvalidInput)
        ));
        for reference in [
            json!({"$ref":"file:///schema"}),
            json!({"$ref":2}),
            json!({"$dynamicRef":"#/$defs/Envelope"}),
            json!({"$recursiveRef":"#"}),
        ] {
            assert!(matches!(
                reject_external_references(&reference),
                Err(ErrorCode::InvalidInput)
            ));
        }
        let mut ambiguous =
            json!({"$ref":"context.output.schema.json#/properties/content", "type":"string"});
        assert!(matches!(
            resolve_canonical_envelope(&mut ambiguous),
            Err(ErrorCode::InvalidInput)
        ));
    }
}
