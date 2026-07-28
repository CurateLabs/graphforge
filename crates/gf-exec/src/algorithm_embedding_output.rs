//! Canonical runtime-dimensional Arrow shaping for embedding-v1 kernels.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, FixedSizeListBuilder, Float32Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use gf_core::algorithms::AnalyzeAlgorithm;

use crate::algorithm_embedding_control::{
    EmbeddingControl, EmbeddingResourceError, EmbeddingResourceEstimate,
};
use crate::algorithm_embedding_options::NormalizedEmbeddingOptions;

pub(crate) const SCHEMA_VERSION: &str = "1";
pub(crate) const RNG_VERSION: &str = "splitmix64-v1";
pub(crate) const RNG_DERIVATION: &str = "graphforge-embedding-substream-v1";

/// One complete UUID-owned embedding produced by a native kernel.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct EmbeddingOutputRow {
    pub(crate) node_uuid: [u8; 16],
    pub(crate) embedding: Vec<f32>,
}

/// Structured failures raised before any embedding table is published.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum EmbeddingOutputError {
    #[error("embedding dimensions must be greater than zero")]
    InvalidDimensions,
    #[error("embedding dimensions exceed the Arrow i32 range")]
    DimensionsOverflow,
    #[error("{algorithm} is not an embedding algorithm")]
    InvalidAlgorithm { algorithm: AnalyzeAlgorithm },
    #[error("{algorithm} does not match normalized version {version}")]
    AlgorithmVersionMismatch {
        algorithm: AnalyzeAlgorithm,
        version: String,
    },
    #[error("embedding row {row} has dimension {observed}, but the invocation requires {expected}")]
    DimensionMismatch {
        row: usize,
        observed: usize,
        expected: usize,
    },
    #[error("embedding output contains duplicate node UUID {node_uuid:?}")]
    DuplicateNode { node_uuid: [u8; 16] },
    #[error("embedding row {row} contains a non-finite value at coordinate {coordinate}")]
    NonFinite { row: usize, coordinate: usize },
    #[error("embedding output size exceeds UInt64 range")]
    SizeOverflow,
    #[error("embedding Arrow construction failed: {message}")]
    Arrow { message: String },
    #[error(transparent)]
    Resource(#[from] EmbeddingResourceError),
}

/// Validate and atomically shape one complete canonical embedding result.
pub(crate) fn shape_embedding_output(
    algorithm: AnalyzeAlgorithm,
    invocation: &NormalizedEmbeddingOptions,
    rows: &[EmbeddingOutputRow],
    control: &EmbeddingControl<'_>,
) -> Result<RecordBatch, EmbeddingOutputError> {
    let dimensions = invocation.dimensions();
    if dimensions == 0 {
        return Err(EmbeddingOutputError::InvalidDimensions);
    }
    let dimensions_i32 =
        i32::try_from(dimensions).map_err(|_| EmbeddingOutputError::DimensionsOverflow)?;
    validate_algorithm_version(algorithm, invocation.algorithm_version)?;

    for (row, value) in rows.iter().enumerate() {
        if value.embedding.len() != dimensions {
            return Err(EmbeddingOutputError::DimensionMismatch {
                row,
                observed: value.embedding.len(),
                expected: dimensions,
            });
        }
        if let Some(coordinate) = value.embedding.iter().position(|value| !value.is_finite()) {
            return Err(EmbeddingOutputError::NonFinite { row, coordinate });
        }
    }

    let mut ordered: Vec<&EmbeddingOutputRow> = rows.iter().collect();
    ordered.sort_unstable_by_key(|row| row.node_uuid);
    for pair in ordered.windows(2) {
        if pair[0].node_uuid == pair[1].node_uuid {
            return Err(EmbeddingOutputError::DuplicateNode {
                node_uuid: pair[0].node_uuid,
            });
        }
    }

    let row_count = u64::try_from(rows.len()).map_err(|_| EmbeddingOutputError::SizeOverflow)?;
    let width = u64::try_from(dimensions).map_err(|_| EmbeddingOutputError::SizeOverflow)?;
    let output_bytes = row_count
        .checked_mul(width)
        .and_then(|value| value.checked_mul(4))
        .ok_or(EmbeddingOutputError::SizeOverflow)?;
    control.preflight(EmbeddingResourceEstimate {
        output_bytes,
        ..EmbeddingResourceEstimate::default()
    })?;

    let mut uuids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let values = Float32Builder::with_capacity(rows.len().saturating_mul(dimensions));
    let mut embeddings = FixedSizeListBuilder::with_capacity(values, dimensions_i32, rows.len())
        .with_field(Arc::new(Field::new("item", DataType::Float32, false)));
    for row in ordered {
        uuids
            .append_value(row.node_uuid)
            .map_err(|error| EmbeddingOutputError::Arrow {
                message: error.to_string(),
            })?;
        for value in &row.embedding {
            embeddings.values().append_value(*value);
        }
        embeddings.append(true);
    }

    control.before_publish()?;
    let fields = vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                dimensions_i32,
            ),
            false,
        ),
    ];
    let metadata = HashMap::from([
        ("graphforge.algorithm".to_owned(), algorithm.to_string()),
        ("graphforge.verb".to_owned(), "analyze".to_owned()),
        (
            "graphforge.algorithm_version".to_owned(),
            invocation.algorithm_version.to_owned(),
        ),
        (
            "graphforge.algorithm_schema_version".to_owned(),
            SCHEMA_VERSION.to_owned(),
        ),
        ("graphforge.dimensions".to_owned(), dimensions.to_string()),
        ("graphforge.seed".to_owned(), invocation.seed().to_string()),
        ("graphforge.rng_version".to_owned(), RNG_VERSION.to_owned()),
        (
            "graphforge.rng_derivation".to_owned(),
            RNG_DERIVATION.to_owned(),
        ),
    ]);
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, metadata)),
        vec![
            Arc::new(uuids.finish()) as ArrayRef,
            Arc::new(embeddings.finish()) as ArrayRef,
        ],
    )
    .map_err(|error| EmbeddingOutputError::Arrow {
        message: error.to_string(),
    })
}

fn validate_algorithm_version(
    algorithm: AnalyzeAlgorithm,
    version: &str,
) -> Result<(), EmbeddingOutputError> {
    let expected = match algorithm {
        AnalyzeAlgorithm::Node2Vec => "node2vec-v1",
        AnalyzeAlgorithm::GraphSage => "graphsage-unsupervised-v1",
        AnalyzeAlgorithm::FastRandomProjection => "fastrp-v1",
        AnalyzeAlgorithm::HashGnn => "hashgnn-v1",
        _ => return Err(EmbeddingOutputError::InvalidAlgorithm { algorithm }),
    };
    if version == expected {
        Ok(())
    } else {
        Err(EmbeddingOutputError::AlgorithmVersionMismatch {
            algorithm,
            version: version.to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, FixedSizeBinaryArray, FixedSizeListArray, Float32Array};
    use gf_core::embedding_options::{
        EmbeddingAnalyzeOptions, EmbeddingOptions, GraphSageOptions, HashGnnOptions,
        Node2VecOptions,
    };

    use super::*;
    use crate::algorithm_dispatch::{
        AlgorithmCancellation, AlgorithmControl, AlgorithmError, AlgorithmLimits,
    };
    use crate::algorithm_embedding_control::EmbeddingResourceLimits;
    use crate::algorithm_embedding_options::normalize_embedding_options;

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn invocation(by: AnalyzeAlgorithm, options: EmbeddingOptions) -> NormalizedEmbeddingOptions {
        normalize_embedding_options(&EmbeddingAnalyzeOptions {
            by,
            via: None,
            directed: false,
            weight: None,
            options,
        })
        .unwrap()
    }

    fn controls(
        cancellation: AlgorithmCancellation,
        memory_bytes: u64,
    ) -> (AlgorithmControl, EmbeddingResourceLimits) {
        (
            AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            EmbeddingResourceLimits {
                memory_bytes,
                work: u64::MAX,
            },
        )
    }

    #[test]
    fn populated_output_is_exact_sorted_and_replayable() {
        let invocation = invocation(
            AnalyzeAlgorithm::Node2Vec,
            EmbeddingOptions::Node2Vec(Node2VecOptions {
                dimensions: 2,
                seed: 42,
                ..Node2VecOptions::default()
            }),
        );
        let rows = vec![
            EmbeddingOutputRow {
                node_uuid: uuid(2),
                embedding: vec![3.0, 4.0],
            },
            EmbeddingOutputRow {
                node_uuid: uuid(1),
                embedding: vec![1.0, 2.0],
            },
        ];
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), u64::MAX);
        let control = EmbeddingControl::new(&algorithm, limits);
        let batch =
            shape_embedding_output(AnalyzeAlgorithm::Node2Vec, &invocation, &rows, &control)
                .unwrap();
        let replay =
            shape_embedding_output(AnalyzeAlgorithm::Node2Vec, &invocation, &rows, &control)
                .unwrap();
        assert_eq!(batch, replay);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().fields().len(), 2);
        assert_eq!(
            batch.schema().field(0),
            &Field::new("node_uuid", DataType::FixedSizeBinary(16), false)
        );
        assert_eq!(
            batch.schema().field(1),
            &Field::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 2),
                false
            )
        );
        let uuids = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(uuids.value(0), uuid(1));
        assert_eq!(uuids.value(1), uuid(2));
        assert_eq!(uuids.null_count(), 0);
        let embeddings = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(embeddings.null_count(), 0);
        assert_eq!(
            embeddings
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .values(),
            &[1.0, 2.0, 3.0, 4.0]
        );
        assert_eq!(
            batch.schema().metadata(),
            &HashMap::from([
                ("graphforge.algorithm".into(), "node2vec".into()),
                ("graphforge.verb".into(), "analyze".into()),
                ("graphforge.algorithm_version".into(), "node2vec-v1".into()),
                ("graphforge.algorithm_schema_version".into(), "1".into()),
                ("graphforge.dimensions".into(), "2".into()),
                ("graphforge.seed".into(), "42".into()),
                ("graphforge.rng_version".into(), "splitmix64-v1".into()),
                (
                    "graphforge.rng_derivation".into(),
                    "graphforge-embedding-substream-v1".into()
                ),
            ])
        );
        assert_eq!(batch.schema().metadata().len(), 8);
    }

    #[test]
    fn empty_output_preserves_each_runtime_width_and_exact_metadata() {
        let cases = [
            (
                AnalyzeAlgorithm::GraphSage,
                EmbeddingOptions::GraphSage(GraphSageOptions {
                    dimensions: 3,
                    feature_properties: vec!["feature".into()],
                    ..GraphSageOptions::default()
                }),
                "graphsage-unsupervised-v1",
            ),
            (
                AnalyzeAlgorithm::HashGnn,
                EmbeddingOptions::HashGnn(HashGnnOptions {
                    dimensions: 5,
                    ..HashGnnOptions::default()
                }),
                "hashgnn-v1",
            ),
        ];
        for (algorithm_name, options, version) in cases {
            let invocation = invocation(algorithm_name, options);
            let (algorithm, limits) = controls(AlgorithmCancellation::default(), u64::MAX);
            let control = EmbeddingControl::new(&algorithm, limits);
            let batch = shape_embedding_output(algorithm_name, &invocation, &[], &control).unwrap();
            assert_eq!(batch.num_rows(), 0);
            assert_eq!(
                batch.schema().field(1).data_type(),
                &DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    i32::try_from(invocation.dimensions()).unwrap(),
                )
            );
            assert_eq!(
                batch.schema().metadata()["graphforge.algorithm_version"],
                version
            );
            assert_eq!(batch.schema().metadata().len(), 8);
        }
    }

    #[test]
    fn malformed_rows_resources_and_cancellation_fail_atomically() {
        let invocation = invocation(
            AnalyzeAlgorithm::Node2Vec,
            EmbeddingOptions::Node2Vec(Node2VecOptions {
                dimensions: 2,
                ..Node2VecOptions::default()
            }),
        );
        let invalid_rows = [
            vec![EmbeddingOutputRow {
                node_uuid: uuid(1),
                embedding: vec![1.0],
            }],
            vec![EmbeddingOutputRow {
                node_uuid: uuid(1),
                embedding: vec![f32::NAN, 1.0],
            }],
            vec![
                EmbeddingOutputRow {
                    node_uuid: uuid(1),
                    embedding: vec![1.0, 2.0],
                },
                EmbeddingOutputRow {
                    node_uuid: uuid(1),
                    embedding: vec![3.0, 4.0],
                },
            ],
        ];
        for rows in invalid_rows {
            let (algorithm, limits) = controls(AlgorithmCancellation::default(), u64::MAX);
            let control = EmbeddingControl::new(&algorithm, limits);
            assert!(
                shape_embedding_output(AnalyzeAlgorithm::Node2Vec, &invocation, &rows, &control)
                    .is_err()
            );
        }

        let rows = vec![EmbeddingOutputRow {
            node_uuid: uuid(1),
            embedding: vec![1.0, 2.0],
        }];
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), 7);
        let control = EmbeddingControl::new(&algorithm, limits);
        assert!(matches!(
            shape_embedding_output(AnalyzeAlgorithm::Node2Vec, &invocation, &rows, &control),
            Err(EmbeddingOutputError::Resource(
                EmbeddingResourceError::MemoryLimit {
                    observed: 8,
                    limit: 7
                }
            ))
        ));

        let cancellation = AlgorithmCancellation::default();
        let (algorithm, limits) = controls(cancellation.clone(), u64::MAX);
        let control = EmbeddingControl::new(&algorithm, limits);
        cancellation.cancel();
        assert_eq!(
            shape_embedding_output(AnalyzeAlgorithm::Node2Vec, &invocation, &rows, &control),
            Err(EmbeddingOutputError::Resource(
                EmbeddingResourceError::Algorithm(AlgorithmError::Cancelled)
            ))
        );
    }

    #[test]
    fn non_embedding_and_mismatched_versions_fail_closed() {
        let invocation = invocation(
            AnalyzeAlgorithm::Node2Vec,
            EmbeddingOptions::Node2Vec(Node2VecOptions::default()),
        );
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), u64::MAX);
        let control = EmbeddingControl::new(&algorithm, limits);
        assert!(matches!(
            shape_embedding_output(AnalyzeAlgorithm::IsDag, &invocation, &[], &control),
            Err(EmbeddingOutputError::InvalidAlgorithm { .. })
        ));
        assert!(matches!(
            shape_embedding_output(AnalyzeAlgorithm::HashGnn, &invocation, &[], &control),
            Err(EmbeddingOutputError::AlgorithmVersionMismatch { .. })
        ));
    }
}
