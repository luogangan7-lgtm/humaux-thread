//! Deterministic public-root support assessment (§12.6).
//!
//! This module consumes server-validated identity metadata only. It performs no
//! I/O and deliberately returns counts rather than a composite trust score.

use std::collections::HashMap;

use humaux_domain::error::ErrorCode;
use uuid::Uuid;

/// Validated identity metadata for one supporting root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootIdentity {
    pub source_id: Uuid,
    pub content_hash: String,
    pub verified_organization: Option<String>,
    pub canonical_url: Option<String>,
    pub document_fingerprint: Option<String>,
    pub upstream_root: Option<String>,
    pub trusted: Option<bool>,
}

/// Explainable support counts; no score or promotion decision is produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustAssessment {
    pub support_count: i32,
    pub independent_support_count: i32,
    pub trusted_source_count: Option<i32>,
    pub identity_incomplete: bool,
}

/// Groups roots by shared identity dimensions and counts the resulting support.
///
/// A source id may occur once with one metadata record; exact duplicates are
/// harmless, while conflicting duplicates are invalid. Only optional identity
/// dimensions connect roots: a content hash alone cannot establish independence.
pub fn assess_roots(roots: &[RootIdentity]) -> Result<TrustAssessment, ErrorCode> {
    if roots.is_empty() {
        return Err(ErrorCode::InvalidInput);
    }

    let mut unique = Vec::with_capacity(roots.len());
    let mut by_source = HashMap::with_capacity(roots.len());
    for root in roots {
        validate(root)?;
        if let Some(&index) = by_source.get(&root.source_id) {
            if unique[index] != *root {
                return Err(ErrorCode::InvalidInput);
            }
            continue;
        }
        by_source.insert(root.source_id, unique.len());
        unique.push(root.clone());
    }

    let mut parent: Vec<usize> = (0..unique.len()).collect();
    for left in 0..unique.len() {
        for right in (left + 1)..unique.len() {
            if shares_identity(&unique[left], &unique[right]) {
                union(&mut parent, left, right);
            }
        }
    }

    let independent_support_count = (0..unique.len())
        .filter(|&index| find(&mut parent, index) == index)
        .count() as i32;
    let trusted_source_count = if unique.iter().any(|root| root.trusted.is_none()) {
        None
    } else {
        Some(
            unique
                .iter()
                .filter(|root| root.trusted == Some(true))
                .count() as i32,
        )
    };

    Ok(TrustAssessment {
        support_count: unique.len() as i32,
        independent_support_count,
        trusted_source_count,
        // A hash identifies content, not an independent/verifiable source.
        identity_incomplete: unique.iter().any(|root| {
            root.verified_organization.is_none()
                && root.canonical_url.is_none()
                && root.document_fingerprint.is_none()
                && root.upstream_root.is_none()
        }),
    })
}

fn validate(root: &RootIdentity) -> Result<(), ErrorCode> {
    if root.source_id == Uuid::nil() || root.content_hash.trim().is_empty() {
        return Err(ErrorCode::InvalidInput);
    }
    for value in [
        root.verified_organization.as_deref(),
        root.canonical_url.as_deref(),
        root.document_fingerprint.as_deref(),
        root.upstream_root.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if value.trim().is_empty() {
            return Err(ErrorCode::InvalidInput);
        }
    }
    Ok(())
}

fn shares_identity(left: &RootIdentity, right: &RootIdentity) -> bool {
    same_nonempty(&left.verified_organization, &right.verified_organization)
        || same_nonempty(&left.canonical_url, &right.canonical_url)
        || left.content_hash == right.content_hash
        || same_nonempty(&left.document_fingerprint, &right.document_fingerprint)
        || same_nonempty(&left.upstream_root, &right.upstream_root)
}

fn same_nonempty(left: &Option<String>, right: &Option<String>) -> bool {
    matches!((left, right), (Some(left), Some(right)) if !left.is_empty() && left == right)
}

fn find(parent: &mut [usize], index: usize) -> usize {
    if parent[index] != index {
        parent[index] = find(parent, parent[index]);
    }
    parent[index]
}

fn union(parent: &mut [usize], left: usize, right: usize) {
    let left = find(parent, left);
    let right = find(parent, right);
    if left != right {
        parent[right] = left;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(id: u128, hash: &str) -> RootIdentity {
        RootIdentity {
            source_id: Uuid::from_u128(id),
            content_hash: hash.into(),
            verified_organization: None,
            canonical_url: None,
            document_fingerprint: None,
            upstream_root: None,
            trusted: Some(true),
        }
    }

    #[test]
    fn duplicate_copy_collapses_to_one_support() {
        let one = root(1, "h");
        let assessment = assess_roots(&[one.clone(), one]).unwrap();
        assert_eq!(assessment.support_count, 1);
        assert_eq!(assessment.independent_support_count, 1);
    }

    #[test]
    fn transitive_identity_chain_is_one_component() {
        let mut a = root(1, "a");
        a.canonical_url = Some("u".into());
        let mut b = root(2, "b");
        b.canonical_url = Some("u".into());
        b.document_fingerprint = Some("f".into());
        let mut c = root(3, "c");
        c.document_fingerprint = Some("f".into());
        let assessment = assess_roots(&[a, b, c]).unwrap();
        assert_eq!(assessment.support_count, 3);
        assert_eq!(assessment.independent_support_count, 1);
    }

    #[test]
    fn hundred_accounts_do_not_create_independent_support() {
        let mut roots = (1..=100)
            .map(|id| {
                let mut item = root(id, &format!("account-copy-{id}"));
                item.verified_organization = Some("verified-original-publisher".into());
                item
            })
            .collect::<Vec<_>>();
        let copies = assess_roots(&roots).unwrap();
        assert_eq!(copies.support_count, 100);
        assert_eq!(copies.independent_support_count, 1);
        assert!(!copies.identity_incomplete);

        let mut independent = root(101, "independently-authored-document");
        independent.verified_organization = Some("different-verified-publisher".into());
        roots.push(independent);
        let with_independent = assess_roots(&roots).unwrap();
        assert_eq!(with_independent.support_count, 101);
        assert_eq!(with_independent.independent_support_count, 2);
    }

    #[test]
    fn copied_content_does_not_gain_support_from_distinct_publisher_ids() {
        let roots = (1..=100)
            .map(|id| {
                let mut item = root(id, "same-copied-body-hash");
                item.verified_organization = Some(format!("publisher-{id}"));
                item
            })
            .collect::<Vec<_>>();
        let assessment = assess_roots(&roots).unwrap();
        assert_eq!(assessment.support_count, 100);
        assert_eq!(assessment.independent_support_count, 1);
    }

    #[test]
    fn equal_values_in_different_dimensions_do_not_connect() {
        let mut a = root(1, "same");
        a.canonical_url = Some("same".into());
        let mut b = root(2, "other");
        b.verified_organization = Some("same".into());
        let assessment = assess_roots(&[a, b]).unwrap();
        assert_eq!(assessment.independent_support_count, 2);
    }

    #[test]
    fn missing_identity_is_incomplete() {
        let assessment = assess_roots(&[root(1, "hash")]).unwrap();
        assert!(assessment.identity_incomplete);
    }

    #[test]
    fn unknown_trust_is_not_zero() {
        let mut one = root(1, "a");
        one.trusted = None;
        let assessment = assess_roots(&[one]).unwrap();
        assert_eq!(assessment.trusted_source_count, None);
    }

    #[test]
    fn conflicting_duplicate_is_invalid() {
        assert_eq!(
            assess_roots(&[root(1, "a"), root(1, "b")]),
            Err(ErrorCode::InvalidInput)
        );
    }

    #[test]
    fn invalid_and_empty_inputs_are_rejected() {
        assert_eq!(assess_roots(&[]), Err(ErrorCode::InvalidInput));
        assert_eq!(assess_roots(&[root(0, "a")]), Err(ErrorCode::InvalidInput));
        assert_eq!(assess_roots(&[root(1, " ")]), Err(ErrorCode::InvalidInput));
        let mut whitespace = root(2, "a");
        whitespace.canonical_url = Some(" ".into());
        assert_eq!(assess_roots(&[whitespace]), Err(ErrorCode::InvalidInput));
    }
}
