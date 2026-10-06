//! `projection::embedding_fingerprint` — the identity of one embedding vector space (ADR-0064 D-C).
//! Depends-on: crates=[sha2]; services=[]; env=[]; modules=[]
//! Called-by: [adapters::private_projection_registry, tests]
//! Invariants: [the fingerprint is sha256 over length-prefixed fields in one fixed order, so no two field tuples
//!   collide by concatenation; an empty field is refused, the literal `unknown` revision is hashed like any other
//!   value (never resolved to an alias at runtime); dtype / normalization / distance are build constants, not
//!   configuration]
//! Spec: Baseline §16.1; §17; ADR-0064 D-C
//!
//! Not the distill processing-input fingerprint ([`crate::fingerprint`]): that one proves the *input* of a
//! processing run; this one names the vector space a stored or indexed vector lives in. The database row keyed by
//! it is `projection.embedding_fingerprints` (migration 0232), one label = one fingerprint.

use sha2::{Digest, Sha256};

/// ADR-0064 D-C: the domain tag that starts every encoding; bump it, never reuse it, if the field list changes.
pub const FINGERPRINT_DOMAIN: &str = "humaux.embedding-fingerprint.v1";
/// ADR-0064 D-C / 0232 `CHECK (dtype = 'float32')`: stored vectors are `real[]`.
pub const DTYPE: &str = "float32";
/// ADR-0064 D-C: the provider's raw output is stored; Qdrant normalises on upload for Cosine (D-F E5).
pub const NORMALIZATION: &str = "provider-raw";
/// ADR-0064 D-C / 0232 `CHECK (distance = 'Cosine')`: the one distance every private collection uses (§17).
pub const DISTANCE: &str = "Cosine";

/// The variable fields of one embedding fingerprint (ADR-0064 D-C). `preprocessing_version` is the card template
/// hash and `projection_contract_version` the ticket family's projection version; the caller passes both so this
/// module stays free of the card builder and the domain family table.
#[derive(Debug, Clone, Copy)]
pub struct EmbeddingFingerprintInputs<'a> {
    /// Provider route name (e.g. the retrieval worker's `_EMBEDDING_PROVIDER`).
    pub provider: &'a str,
    /// Provider model id.
    pub model_id: &'a str,
    /// Immutable model revision, or the literal `unknown`; never an alias.
    pub model_revision: &'a str,
    /// Vector dimension; must be > 0.
    pub dimension: u32,
    /// Provider task type (e.g. document vs query embedding).
    pub task_type: &'a str,
    /// Card preprocessing / chunking version (`card::CARD_TEMPLATE_HASH`).
    pub preprocessing_version: &'a str,
    /// Projection contract version (`TicketFamily::projection_version`).
    pub projection_contract_version: &'a str,
}

/// sha256 of one embedding space's canonical encoding; the primary key of `projection.embedding_fingerprints`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EmbeddingFingerprint(pub [u8; 32]);

/// The input field that was empty (or, for `dimension`, zero); a fingerprint over it would name no real space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyFingerprintField(pub &'static str);

impl EmbeddingFingerprint {
    /// ADR-0064 D-C: hashes [`FINGERPRINT_DOMAIN`], the seven inputs and the three build constants, each
    /// length-prefixed (the point-id idiom of `private_projection_registry`). Refuses an empty field or a zero
    /// dimension.
    pub fn compute(inputs: &EmbeddingFingerprintInputs<'_>) -> Result<Self, EmptyFingerprintField> {
        let text = [
            ("provider", inputs.provider),
            ("model_id", inputs.model_id),
            ("model_revision", inputs.model_revision),
            ("task_type", inputs.task_type),
            ("preprocessing_version", inputs.preprocessing_version),
            (
                "projection_contract_version",
                inputs.projection_contract_version,
            ),
        ];
        if let Some((name, _)) = text.iter().find(|(_, v)| v.trim().is_empty()) {
            return Err(EmptyFingerprintField(name));
        }
        if inputs.dimension == 0 {
            return Err(EmptyFingerprintField("dimension"));
        }
        fn field(hasher: &mut Sha256, bytes: &[u8]) {
            hasher.update((bytes.len() as u64).to_be_bytes());
            hasher.update(bytes);
        }
        let mut hasher = Sha256::new();
        field(&mut hasher, FINGERPRINT_DOMAIN.as_bytes());
        field(&mut hasher, inputs.provider.as_bytes());
        field(&mut hasher, inputs.model_id.as_bytes());
        field(&mut hasher, inputs.model_revision.as_bytes());
        field(&mut hasher, &inputs.dimension.to_be_bytes());
        field(&mut hasher, inputs.task_type.as_bytes());
        field(&mut hasher, inputs.preprocessing_version.as_bytes());
        field(&mut hasher, inputs.projection_contract_version.as_bytes());
        field(&mut hasher, DTYPE.as_bytes());
        field(&mut hasher, NORMALIZATION.as_bytes());
        field(&mut hasher, DISTANCE.as_bytes());
        Ok(Self(hasher.finalize().into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> EmbeddingFingerprintInputs<'static> {
        EmbeddingFingerprintInputs {
            provider: "dashscope",
            model_id: "text-embedding-v4",
            model_revision: "2026-08",
            dimension: 1024,
            task_type: "document",
            preprocessing_version: "v1-abc",
            projection_contract_version: "v1",
        }
    }

    /// ADR-0064 D-C: each input field moves the fingerprint (fault: drop `model_revision` from the encoding).
    #[test]
    fn every_field_changes_the_fingerprint() {
        let reference = EmbeddingFingerprint::compute(&base()).expect("base fingerprint");
        let variants: [(&str, EmbeddingFingerprintInputs<'static>); 7] = [
            (
                "provider",
                EmbeddingFingerprintInputs {
                    provider: "other",
                    ..base()
                },
            ),
            (
                "model_id",
                EmbeddingFingerprintInputs {
                    model_id: "other",
                    ..base()
                },
            ),
            (
                "model_revision",
                EmbeddingFingerprintInputs {
                    model_revision: "unknown",
                    ..base()
                },
            ),
            (
                "dimension",
                EmbeddingFingerprintInputs {
                    dimension: 768,
                    ..base()
                },
            ),
            (
                "task_type",
                EmbeddingFingerprintInputs {
                    task_type: "query",
                    ..base()
                },
            ),
            (
                "preprocessing_version",
                EmbeddingFingerprintInputs {
                    preprocessing_version: "v2-def",
                    ..base()
                },
            ),
            (
                "projection_contract_version",
                EmbeddingFingerprintInputs {
                    projection_contract_version: "v2",
                    ..base()
                },
            ),
        ];
        for (name, inputs) in variants {
            let moved = EmbeddingFingerprint::compute(&inputs).expect(name);
            assert_ne!(moved, reference, "{name} must change the fingerprint");
        }
        assert_eq!(
            EmbeddingFingerprint::compute(&base()).expect("again"),
            reference,
            "the encoding is deterministic"
        );
        // Length prefixes: moving a byte across a field boundary is a different space.
        let shifted = EmbeddingFingerprintInputs {
            model_id: "text-embedding-v42",
            model_revision: "026-08",
            ..base()
        };
        assert_ne!(
            EmbeddingFingerprint::compute(&shifted).expect("shifted"),
            reference
        );
    }

    /// An empty field or a zero dimension names no space and is refused, naming the field.
    #[test]
    fn an_empty_field_is_refused_by_name() {
        let empty = EmbeddingFingerprintInputs {
            model_revision: " ",
            ..base()
        };
        assert_eq!(
            EmbeddingFingerprint::compute(&empty),
            Err(EmptyFingerprintField("model_revision"))
        );
        let zero = EmbeddingFingerprintInputs {
            dimension: 0,
            ..base()
        };
        assert_eq!(
            EmbeddingFingerprint::compute(&zero),
            Err(EmptyFingerprintField("dimension"))
        );
    }
}
