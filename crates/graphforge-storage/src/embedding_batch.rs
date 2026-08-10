//! Complete, canonical UUID/vector batches for embedding generations.

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

use crate::{
    EmbeddingContentDigest, EmbeddingNormalization, SearchArtifactError, VectorStoreLimits,
    validate_vector, vector_schema,
};

/// One caller- or producer-supplied UUID/vector row before validation.
#[derive(Clone, Debug, PartialEq)]
pub struct EmbeddingBatchRow {
    /// Stable graph identity; numeric execution surrogates are never accepted.
    pub node_uuid: [u8; 16],
    /// Fixed-dimension Float32 coordinates.
    pub vector: Vec<f32>,
}

/// A complete validated batch in canonical raw-UUID order.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedEmbeddingBatch {
    rows: Vec<EmbeddingBatchRow>,
    dimension: usize,
    content_digest: EmbeddingContentDigest,
}

impl ValidatedEmbeddingBatch {
    /// Canonical rows sorted by raw UUID bytes.
    #[must_use]
    pub fn rows(&self) -> &[EmbeddingBatchRow] {
        &self.rows
    }

    /// Fixed vector width retained even when the complete projection is empty.
    #[must_use]
    pub const fn dimension(&self) -> usize {
        self.dimension
    }

    /// SHA-256 of canonical UUID and little-endian Float32 bytes.
    #[must_use]
    pub const fn content_digest(&self) -> EmbeddingContentDigest {
        self.content_digest
    }

    /// Consume the validated wrapper and return canonical rows.
    #[must_use]
    pub fn into_rows(self) -> Vec<EmbeddingBatchRow> {
        self.rows
    }
}

/// Validate and canonicalize one complete embedding-space batch.
///
/// `eligible_nodes` is the resolved source projection for this generation. The
/// returned batch covers it exactly: every row must be eligible and every
/// eligible UUID must have one row. `checkpoint` is called before validation,
/// once per row, and once per digest row so cancellation never returns a
/// partial validated value.
///
/// # Errors
/// Rejects invalid dimensions/vectors, duplicate or ineligible UUIDs,
/// incomplete coverage, configured resource exhaustion, and cancellation.
pub fn validate_embedding_batch<C>(
    mut rows: Vec<EmbeddingBatchRow>,
    eligible_nodes: &BTreeSet<[u8; 16]>,
    dimension: usize,
    normalization: EmbeddingNormalization,
    limits: VectorStoreLimits,
    mut checkpoint: C,
) -> Result<ValidatedEmbeddingBatch, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    vector_schema(dimension, limits)?;
    enforce_limit("embedding_rows", rows.len(), limits.stored_vectors)?;
    enforce_limit(
        "embedding_eligible_nodes",
        eligible_nodes.len(),
        limits.eligible_nodes,
    )?;
    let cells = rows
        .len()
        .checked_mul(dimension)
        .ok_or_else(|| exhausted("embedding_vector_cells", limits.vector_cells))?;
    enforce_limit("embedding_vector_cells", cells, limits.vector_cells)?;

    rows.sort_unstable_by_key(|row| row.node_uuid);
    for index in 0..rows.len() {
        checkpoint()?;
        if index != 0 && rows[index - 1].node_uuid == rows[index].node_uuid {
            return Err(invalid("embedding batch", "contains duplicate node_uuid"));
        }
        if !eligible_nodes.contains(&rows[index].node_uuid) {
            return Err(invalid(
                "embedding batch",
                "contains a node_uuid outside the eligible projection",
            ));
        }
        if rows[index].vector.len() != dimension {
            return Err(invalid(
                "embedding batch",
                format!(
                    "node_uuid has dimension {}, expected {dimension}",
                    rows[index].vector.len()
                ),
            ));
        }
        let squared_norm = validate_vector(&rows[index].vector, limits)?;
        if normalization == EmbeddingNormalization::L2 {
            normalize_l2(&mut rows[index].vector, squared_norm);
        }
    }

    if rows.len() != eligible_nodes.len() {
        return Err(invalid(
            "embedding batch",
            format!(
                "missing {} eligible node_uuid rows",
                eligible_nodes.len().saturating_sub(rows.len())
            ),
        ));
    }

    let mut hasher = Sha256::new();
    for row in &rows {
        checkpoint()?;
        hasher.update(row.node_uuid);
        for value in &row.vector {
            hasher.update(value.to_le_bytes());
        }
    }
    let content_digest = EmbeddingContentDigest::from_hex(&hex_lower(&hasher.finalize()))?;
    Ok(ValidatedEmbeddingBatch {
        rows,
        dimension,
        content_digest,
    })
}

#[allow(clippy::cast_possible_truncation)]
fn normalize_l2(vector: &mut [f32], squared_norm: f64) {
    // The compatibility contract deliberately persists Float32 coordinates;
    // accumulate the norm safely in Float64, then round each result to Float32.
    let norm = squared_norm.sqrt();
    for value in vector {
        *value = (f64::from(*value) / norm) as f32;
    }
}

fn enforce_limit(
    resource: &'static str,
    actual: usize,
    limit: usize,
) -> Result<(), SearchArtifactError> {
    if actual > limit {
        return Err(exhausted(resource, limit));
    }
    Ok(())
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

fn exhausted(resource: &'static str, limit: usize) -> SearchArtifactError {
    SearchArtifactError::ResourceExhausted {
        resource,
        limit: u64::try_from(limit).unwrap_or(u64::MAX),
    }
}


fn hex_lower(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    const A: [u8; 16] = [1; 16];
    const B: [u8; 16] = [2; 16];

    fn row(node_uuid: [u8; 16], vector: &[f32]) -> EmbeddingBatchRow {
        EmbeddingBatchRow {
            node_uuid,
            vector: vector.to_vec(),
        }
    }

    fn eligible(values: &[[u8; 16]]) -> BTreeSet<[u8; 16]> {
        values.iter().copied().collect()
    }

    #[test]
    fn input_order_cannot_change_rows_or_little_endian_digest() {
        let limits = VectorStoreLimits::default();
        let expected_nodes = eligible(&[A, B]);
        let left = validate_embedding_batch(
            vec![row(B, &[3.0, 4.0]), row(A, &[1.0, 2.0])],
            &expected_nodes,
            2,
            EmbeddingNormalization::None,
            limits,
            || Ok(()),
        )
        .unwrap();
        let right = validate_embedding_batch(
            vec![row(A, &[1.0, 2.0]), row(B, &[3.0, 4.0])],
            &expected_nodes,
            2,
            EmbeddingNormalization::None,
            limits,
            || Ok(()),
        )
        .unwrap();

        let mut expected_bytes = Vec::new();
        expected_bytes.extend_from_slice(&A);
        expected_bytes.extend_from_slice(&1.0_f32.to_le_bytes());
        expected_bytes.extend_from_slice(&2.0_f32.to_le_bytes());
        expected_bytes.extend_from_slice(&B);
        expected_bytes.extend_from_slice(&3.0_f32.to_le_bytes());
        expected_bytes.extend_from_slice(&4.0_f32.to_le_bytes());
        assert_eq!(left, right);
        assert_eq!(left.rows()[0].node_uuid, A);
        assert_eq!(
            left.content_digest(),
            EmbeddingContentDigest::digest(&expected_bytes)
        );
    }

    #[test]
    fn normalization_contract_is_explicit() {
        let complete = eligible(&[A]);
        let preserved = validate_embedding_batch(
            vec![row(A, &[3.0, 4.0])],
            &complete,
            2,
            EmbeddingNormalization::None,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap();
        let normalized = validate_embedding_batch(
            vec![row(A, &[3.0, 4.0])],
            &complete,
            2,
            EmbeddingNormalization::L2,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(preserved.rows()[0].vector, vec![3.0, 4.0]);
        assert_eq!(normalized.rows()[0].vector, vec![0.6, 0.8]);
        assert_ne!(preserved.content_digest(), normalized.content_digest());
    }

    #[test]
    fn coverage_and_vector_validation_fail_closed() {
        let limits = VectorStoreLimits::default();
        let complete = eligible(&[A, B]);
        let invalid_batches = [
            vec![row(A, &[1.0, 0.0]), row(A, &[0.0, 1.0])],
            vec![row(A, &[1.0, 0.0]), row([3; 16], &[0.0, 1.0])],
            vec![row(A, &[1.0, 0.0])],
            vec![row(A, &[1.0]), row(B, &[0.0, 1.0])],
            vec![row(A, &[f32::NAN, 1.0]), row(B, &[0.0, 1.0])],
            vec![row(A, &[0.0, 0.0]), row(B, &[0.0, 1.0])],
        ];
        for rows in invalid_batches {
            assert!(
                validate_embedding_batch(
                    rows,
                    &complete,
                    2,
                    EmbeddingNormalization::None,
                    limits,
                    || Ok(())
                )
                .is_err()
            );
        }
    }

    #[test]
    fn limits_and_cancellation_return_no_validated_batch() {
        assert!(
            validate_embedding_batch(
                Vec::new(),
                &BTreeSet::new(),
                0,
                EmbeddingNormalization::None,
                VectorStoreLimits::default(),
                || Ok(())
            )
            .is_err()
        );

        let mut limits = VectorStoreLimits::default();
        limits.stored_vectors = 1;
        assert!(matches!(
            validate_embedding_batch(
                vec![row(A, &[1.0]), row(B, &[1.0])],
                &eligible(&[A, B]),
                1,
                EmbeddingNormalization::None,
                limits,
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "embedding_rows",
                ..
            })
        ));

        let mut limits = VectorStoreLimits::default();
        limits.eligible_nodes = 1;
        assert!(matches!(
            validate_embedding_batch(
                vec![row(A, &[1.0]), row(B, &[1.0])],
                &eligible(&[A, B]),
                1,
                EmbeddingNormalization::None,
                limits,
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "embedding_eligible_nodes",
                ..
            })
        ));

        let mut limits = VectorStoreLimits::default();
        limits.vector_cells = 1;
        assert!(matches!(
            validate_embedding_batch(
                vec![row(A, &[1.0]), row(B, &[1.0])],
                &eligible(&[A, B]),
                1,
                EmbeddingNormalization::None,
                limits,
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "embedding_vector_cells",
                ..
            })
        ));

        let checkpoints = AtomicUsize::new(0);
        assert!(matches!(
            validate_embedding_batch(
                vec![row(A, &[1.0]), row(B, &[1.0])],
                &eligible(&[A, B]),
                1,
                EmbeddingNormalization::None,
                VectorStoreLimits::default(),
                || {
                    (checkpoints.fetch_add(1, Ordering::Relaxed) < 2)
                        .then_some(())
                        .ok_or(SearchArtifactError::Cancelled)
                }
            ),
            Err(SearchArtifactError::Cancelled)
        ));
    }

    #[test]
    fn empty_complete_projection_retains_fixed_dimension() {
        let batch = validate_embedding_batch(
            Vec::new(),
            &BTreeSet::new(),
            3,
            EmbeddingNormalization::L2,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert!(batch.rows().is_empty());
        assert_eq!(batch.dimension(), 3);
        assert_eq!(batch.content_digest(), EmbeddingContentDigest::digest(&[]));
    }
}
