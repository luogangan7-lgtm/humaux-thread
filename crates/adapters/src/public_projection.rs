//! `adapters::public_projection` — Public Qdrant projection writes (§17.6).
//! Depends-on: crates=[async-trait, hex, humaux-domain, humaux-infra-cell, serde_json, sha2, uuid];
//!   services=[Qdrant(*)]; env=[]; modules=[adapters::public_repo, adapters::qdrant, domain::error,
//!   infra-cell::permit, infra-cell::transport]
//! Called-by: [public-worker::main, tests]
//! Invariants: [writes only the public payload contract through the same-cell transport; no dense vector, private
//!   payload, tenant or database seam; a Qdrant failure is DependencyUnavailable]
//! Spec: none
//!
//! This adapter is deliberately narrow: it writes only the public payload contract through the
//! same-cell HTTP transport. It has no dense vector, private payload, tenant, or database seam.

use humaux_infra_cell::{
    CellAccessPermit, IntraCellHttpTransport, IntraCellMethod, IntraCellRequest,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::num::NonZeroU32;
use uuid::Uuid;

use crate::public_repo::{
    EligibleObject, ProjectionIdentity, ProjectionWriteOutcome, PublicProjectionPort,
};
use crate::qdrant::QdrantTransportError;

/// The six fields persisted on a public projection point (§17.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicProjectionPayload {
    /// Public object kind, such as `CLAIM` or `SYNTHESIS`.
    pub object_kind: String,
    /// Stable public object identifier.
    pub object_id: Uuid,
    /// Exact object revision represented by this point.
    pub object_revision: i64,
    /// Evaluation receipt that authorized this revision.
    pub evaluation_id: Uuid,
    /// SHA-256 of the projected body.
    pub body_sha256: String,
    /// Whether this revision is searchable.
    pub projection_live: bool,
}

impl PublicProjectionPayload {
    fn to_json(&self) -> Value {
        json!({
            "object_kind": self.object_kind,
            "object_id": self.object_id.to_string(),
            "object_revision": self.object_revision,
            "evaluation_id": self.evaluation_id.to_string(),
            "body_sha256": self.body_sha256,
            "projection_live": self.projection_live,
        })
    }
}

/// Readback of the point written by [`PublicProjectionAdapter`].
#[derive(Debug, Clone, PartialEq)]
pub struct PublicProjectionReadback {
    /// Deterministic Qdrant point identifier.
    pub point_id: Uuid,
    /// The returned payload, when Qdrant returned one.
    pub payload: Option<Value>,
    /// The returned vector, when Qdrant returned one.
    pub vector: Option<Value>,
    /// True when an insert-only live write observed a pre-existing tombstone.
    pub superseded: bool,
}
#[derive(Debug, Clone, PartialEq)]
pub struct PublicProjectionCandidate {
    /// Untrusted projection identity only; callers must fresh-hydrate it from PostgreSQL before serving.
    pub point_id: Uuid,
    pub identity: ProjectionIdentity,
    pub score: f64,
}

/// Minimal Qdrant adapter for public projection and permanent revision tombstones.
pub struct PublicProjectionAdapter<'a> {
    transport: &'a dyn IntraCellHttpTransport,
    permit: &'a CellAccessPermit,
    collection: &'a str,
}

impl<'a> PublicProjectionAdapter<'a> {
    pub async fn query_live(
        &self,
        text: &str,
        limit: NonZeroU32,
    ) -> Result<Vec<PublicProjectionCandidate>, QdrantTransportError> {
        if text.trim().is_empty() {
            return Err(QdrantTransportError::UnexpectedResponseShape(
                "empty public query".into(),
            ));
        }
        let live_filter = json!({"must":[{"key":"projection_live","match":{"value":true}}]});
        let body = json!({"query":{"text":text,"model":"qdrant/bm25"},"using":"bm25","limit":limit.get(),"with_payload":true,"with_vector":false,"filter":live_filter.clone(),"params":{"idf":{"corpus":live_filter}}});
        // dep: Qdrant(*) — Qdrant REST call for `query_live`
        let response = self
            .transport
            .execute(
                self.permit,
                IntraCellRequest {
                    method: IntraCellMethod::Post,
                    path: format!("/collections/{}/points/query", self.collection),
                    json_body: Some(body),
                    headers: Vec::new(),
                },
            )
            .await?;
        if !(200..300).contains(&response.status) {
            return Err(QdrantTransportError::NonSuccessStatus {
                status: response.status,
                body: response.json_body,
            });
        }
        let points = response
            .json_body
            .and_then(|b| b.get("result").and_then(|r| r.get("points")).cloned())
            .and_then(|v| v.as_array().cloned())
            .ok_or_else(|| {
                QdrantTransportError::UnexpectedResponseShape("missing query points".into())
            })?;
        points.iter().map(parse_candidate).collect()
    }
    /// Binds one already-authorized same-cell transport to one validated collection name.
    pub fn new(
        transport: &'a dyn IntraCellHttpTransport,
        permit: &'a CellAccessPermit,
        collection: &'a str,
    ) -> Result<Self, QdrantTransportError> {
        if collection.is_empty()
            || collection.contains('/')
            || collection.contains('@')
            || collection
                .chars()
                .any(|c| c.is_control() || c.is_whitespace())
        {
            return Err(QdrantTransportError::InvalidCollectionName(
                collection.to_owned(),
            ));
        }
        Ok(Self {
            transport,
            permit,
            collection,
        })
    }

    /// Projects one exact revision with Qdrant's insert-only mode.
    pub async fn write_live_revision(
        &self,
        object_kind: &str,
        object_id: Uuid,
        object_revision: i64,
        evaluation_id: Uuid,
        body: &str,
    ) -> Result<PublicProjectionReadback, QdrantTransportError> {
        self.write_live_identity(
            &ProjectionIdentity {
                object_kind: object_kind.to_owned(),
                object_id,
                object_revision,
                evaluation_id,
                body_sha256: Sha256::digest(body.as_bytes()).into(),
            },
            body,
        )
        .await
    }

    /// Retires exactly one revision. The tombstone is permanent and carries no vector.
    pub async fn retire_revision_fields(
        &self,
        object_kind: &str,
        object_id: Uuid,
        object_revision: i64,
        evaluation_id: Uuid,
        body_sha256: &str,
    ) -> Result<PublicProjectionReadback, QdrantTransportError> {
        let point_id = point_id(object_kind, object_id, object_revision);
        let payload = PublicProjectionPayload {
            object_kind: object_kind.to_owned(),
            object_id,
            object_revision,
            evaluation_id,
            body_sha256: body_sha256.to_owned(),
            projection_live: false,
        };
        let readback = self.write(point_id, &payload, None, false).await?;
        validate_readback(&readback, point_id, &payload)?;
        Ok(readback)
    }

    async fn write(
        &self,
        point_id: Uuid,
        payload: &PublicProjectionPayload,
        body: Option<&str>,
        insert_only: bool,
    ) -> Result<PublicProjectionReadback, QdrantTransportError> {
        let vector = body
            .map(|text| json!({"bm25": {"text": text, "model": "qdrant/bm25"}}))
            .unwrap_or_else(|| json!({}));
        let point =
            json!({"id": point_id.to_string(), "payload": payload.to_json(), "vector": vector});
        let mut request = json!({"points": [point]});
        if insert_only {
            request["update_mode"] = json!("insert_only");
        }
        // dep: Qdrant(*) — Qdrant REST call for `write`
        let response = self
            .transport
            .execute(
                self.permit,
                IntraCellRequest {
                    method: IntraCellMethod::Put,
                    path: format!("/collections/{}/points?wait=true", self.collection),
                    json_body: Some(request),
                    headers: Vec::new(),
                },
            )
            .await?;
        if !(200..300).contains(&response.status) {
            return Err(QdrantTransportError::NonSuccessStatus {
                status: response.status,
                body: response.json_body,
            });
        }
        // dep: Qdrant(*) — Qdrant REST call for `write`
        let verify = self
            .transport
            .execute(
                self.permit,
                IntraCellRequest {
                    method: IntraCellMethod::Get,
                    path: format!("/collections/{}/points/{}", self.collection, point_id),
                    json_body: None,
                    headers: Vec::new(),
                },
            )
            .await?;
        if !(200..300).contains(&verify.status) {
            return Err(QdrantTransportError::NonSuccessStatus {
                status: verify.status,
                body: verify.json_body,
            });
        }
        let result = verify
            .json_body
            .and_then(|body| body.get("result").cloned())
            .ok_or_else(|| {
                QdrantTransportError::UnexpectedResponseShape("missing result".into())
            })?;
        Ok(PublicProjectionReadback {
            point_id,
            payload: result.get("payload").cloned(),
            vector: result.get("vector").cloned(),
            superseded: false,
        })
    }
}

#[async_trait::async_trait]
impl<'a> PublicProjectionPort for PublicProjectionAdapter<'a> {
    async fn project_live(
        &self,
        object: &EligibleObject,
    ) -> Result<ProjectionWriteOutcome, humaux_domain::error::ErrorCode> {
        let readback = self
            .write_live(object)
            .await
            .map_err(|_| humaux_domain::error::ErrorCode::DependencyUnavailable)?;
        Ok(if readback.superseded {
            ProjectionWriteOutcome::Superseded
        } else {
            ProjectionWriteOutcome::Applied
        })
    }

    async fn retire(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<(), humaux_domain::error::ErrorCode> {
        self.retire_revision_fields(
            &identity.object_kind,
            identity.object_id,
            identity.object_revision,
            identity.evaluation_id,
            &hex_bytes(&identity.body_sha256),
        )
        .await
        .map(|_| ())
        .map_err(|_| humaux_domain::error::ErrorCode::DependencyUnavailable)
    }
}

impl<'a> PublicProjectionAdapter<'a> {
    /// Projects a database-hydrated eligible object using its exact PostgreSQL canonical body and hash.
    pub async fn write_live(
        &self,
        object: &EligibleObject,
    ) -> Result<PublicProjectionReadback, QdrantTransportError> {
        self.write_live_identity(&object.identity(), &object.canonical_body)
            .await
    }

    async fn write_live_identity(
        &self,
        identity: &ProjectionIdentity,
        body: &str,
    ) -> Result<PublicProjectionReadback, QdrantTransportError> {
        if Sha256::digest(body.as_bytes()).as_slice() != identity.body_sha256 {
            return Err(QdrantTransportError::UnexpectedResponseShape(
                "canonical projection body does not match identity hash".into(),
            ));
        }
        let point_id = point_id(
            &identity.object_kind,
            identity.object_id,
            identity.object_revision,
        );
        let mut payload = PublicProjectionPayload {
            object_kind: identity.object_kind.clone(),
            object_id: identity.object_id,
            object_revision: identity.object_revision,
            evaluation_id: identity.evaluation_id,
            body_sha256: hex_bytes(&identity.body_sha256),
            projection_live: true,
        };
        let readback = self.write(point_id, &payload, Some(body), true).await?;
        if readback
            .payload
            .as_ref()
            .and_then(|p| p.get("projection_live"))
            == Some(&json!(false))
        {
            payload.projection_live = false;
            validate_readback(&readback, point_id, &payload)?;
            return Ok(PublicProjectionReadback {
                superseded: true,
                ..readback
            });
        }
        validate_readback(&readback, point_id, &payload)?;
        Ok(readback)
    }
}

fn validate_readback(
    readback: &PublicProjectionReadback,
    point_id: Uuid,
    expected: &PublicProjectionPayload,
) -> Result<(), QdrantTransportError> {
    let payload = readback.payload.as_ref().ok_or_else(|| {
        QdrantTransportError::UnexpectedResponseShape("missing projection payload".into())
    })?;
    let matches = *payload == expected.to_json() && readback.point_id == point_id;
    let vector_empty = readback
        .vector
        .as_ref()
        .is_none_or(|v| v.as_object().is_some_and(|m| m.is_empty()));
    let has_bm25 = readback.vector.as_ref().is_some_and(|vector| {
        let Some(vector) = vector.as_object().and_then(|v| v.get("bm25")) else {
            return false;
        };
        match (
            vector.get("indices").and_then(Value::as_array),
            vector.get("values").and_then(Value::as_array),
        ) {
            (Some(indices), Some(values)) => !indices.is_empty() && indices.len() == values.len(),
            _ => false,
        }
    });
    let vector_matches = if expected.projection_live {
        has_bm25
    } else {
        vector_empty
    };
    if !matches || !vector_matches {
        return Err(QdrantTransportError::UnexpectedResponseShape(
            "projection readback mismatch".into(),
        ));
    }
    Ok(())
}

fn parse_candidate(row: &Value) -> Result<PublicProjectionCandidate, QdrantTransportError> {
    let bad =
        || QdrantTransportError::UnexpectedResponseShape("invalid public query candidate".into());
    let point = row
        .get("id")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
        .filter(|id| !id.is_nil())
        .ok_or_else(bad)?;
    let score = row
        .get("score")
        .and_then(Value::as_f64)
        .filter(|s| s.is_finite())
        .ok_or_else(bad)?;
    let p = row
        .get("payload")
        .and_then(Value::as_object)
        .ok_or_else(bad)?;
    if p.len() != 6 {
        return Err(bad());
    }
    let kind = p
        .get("object_kind")
        .and_then(Value::as_str)
        .filter(|v| matches!(*v, "CLAIM" | "SYNTHESIS"))
        .ok_or_else(bad)?;
    let object_id = p
        .get("object_id")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
        .filter(|id| !id.is_nil())
        .ok_or_else(bad)?;
    let revision = p
        .get("object_revision")
        .and_then(Value::as_i64)
        .filter(|v| *v > 0)
        .ok_or_else(bad)?;
    let evaluation_id = p
        .get("evaluation_id")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
        .filter(|id| !id.is_nil())
        .ok_or_else(bad)?;
    let bytes = p
        .get("body_sha256")
        .and_then(Value::as_str)
        .and_then(|s| hex::decode(s).ok())
        .and_then(|v| v.try_into().ok())
        .ok_or_else(bad)?;
    if p.get("projection_live") != Some(&Value::Bool(true))
        || point != point_id(kind, object_id, revision)
    {
        return Err(bad());
    }
    Ok(PublicProjectionCandidate {
        point_id: point,
        identity: ProjectionIdentity {
            object_kind: kind.into(),
            object_id,
            object_revision: revision,
            evaluation_id,
            body_sha256: bytes,
        },
        score,
    })
}

fn hex_bytes(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn point_id(kind: &str, object_id: Uuid, revision: i64) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(kind.as_bytes());
    digest.update([0]);
    digest.update(object_id.as_bytes());
    digest.update([0]);
    digest.update(revision.to_be_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_id_is_stable_and_revision_scoped() {
        let id = Uuid::now_v7();
        assert_eq!(point_id("CLAIM", id, 5), point_id("CLAIM", id, 5));
        assert_ne!(point_id("CLAIM", id, 5), point_id("CLAIM", id, 6));
        assert_ne!(point_id("CLAIM", id, 5), point_id("SYNTHESIS", id, 5));
    }

    #[test]
    fn body_hash_is_sha256_hex() {
        assert_eq!(
            hex_bytes(&Sha256::digest(b"body").into()),
            "230d8358dc8e8890b4c58deeb62912ee2f20357ae92a5cc861b98e68fe31acb5"
        );
    }

    #[test]
    fn a_tombstone_receipt_must_match_exact_identity_and_have_no_vector() {
        let payload = PublicProjectionPayload {
            object_kind: "CLAIM".into(),
            object_id: Uuid::now_v7(),
            object_revision: 3,
            evaluation_id: Uuid::now_v7(),
            body_sha256: "a".repeat(64),
            projection_live: false,
        };
        let id = point_id("CLAIM", payload.object_id, 3);
        let mut receipt = PublicProjectionReadback {
            point_id: id,
            payload: Some(payload.to_json()),
            vector: Some(json!({})),
            superseded: false,
        };
        assert!(validate_readback(&receipt, id, &payload).is_ok());
        receipt.vector = Some(json!({"bm25": {"indices": [1], "values": [1.0]}}));
        assert!(validate_readback(&receipt, id, &payload).is_err());
        receipt.vector = Some(json!({}));
        receipt.payload.as_mut().unwrap()["body_sha256"] = json!("b".repeat(64));
        assert!(validate_readback(&receipt, id, &payload).is_err());
        receipt.payload = Some(payload.to_json());
        receipt.payload.as_mut().unwrap()["private_metadata"] = json!("unexpected");
        assert!(validate_readback(&receipt, id, &payload).is_err());
    }

    #[test]
    fn a_live_receipt_requires_the_named_bm25_vector() {
        let payload = PublicProjectionPayload {
            object_kind: "CLAIM".into(),
            object_id: Uuid::now_v7(),
            object_revision: 3,
            evaluation_id: Uuid::now_v7(),
            body_sha256: "a".repeat(64),
            projection_live: true,
        };
        let id = point_id("CLAIM", payload.object_id, 3);
        let mut receipt = PublicProjectionReadback {
            point_id: id,
            payload: Some(payload.to_json()),
            vector: Some(json!({"bm25": {"indices": [1], "values": [1.0]}})),
            superseded: false,
        };
        assert!(validate_readback(&receipt, id, &payload).is_ok());
        for vector in [
            json!({}),
            json!({"dense": [1.0]}),
            json!({"bm25": {"indices": [], "values": []}}),
            json!({"bm25": {"indices": [1], "values": []}}),
        ] {
            receipt.vector = Some(vector);
            assert!(validate_readback(&receipt, id, &payload).is_err());
        }
    }

    #[test]
    fn query_candidates_require_exact_live_identity_payload() {
        let object = Uuid::now_v7();
        let evaluation = Uuid::now_v7();
        let id = point_id("CLAIM", object, 1);
        let mut row = json!({"id":id.to_string(),"score":1.0,"payload":{"object_kind":"CLAIM","object_id":object.to_string(),"object_revision":1,"evaluation_id":evaluation.to_string(),"body_sha256":"a".repeat(64),"projection_live":true}});
        assert!(parse_candidate(&row).is_ok());
        row["payload"]["object_kind"] = json!("BAD");
        assert!(parse_candidate(&row).is_err());
        row["payload"]["object_kind"] = json!("CLAIM");
        row["payload"]["projection_live"] = json!(false);
        assert!(parse_candidate(&row).is_err());
        row["payload"]["projection_live"] = json!(true);
        row["score"] = json!("NaN");
        assert!(parse_candidate(&row).is_err());
    }
}
